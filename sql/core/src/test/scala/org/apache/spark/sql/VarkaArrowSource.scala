/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.util.{Collections, IdentityHashMap}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, Literal,
  NamedExpression, RuntimeReplaceable, TryEval}
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler
import org.apache.spark.sql.catalyst.plans.logical.{Command, Limit, LocalRelation, LogicalPlan,
  OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.MetadataColumnHelper
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.{ArrowCachedBatchSerializer, CachedRDDBuilder,
  InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.storage.StorageLevel

/**
 * A test-only source of Arrow batches for Spark's own SQL suites, so that their queries reach
 * Varka (VARKA-271, `sql/varka/plans/m7/VARKA-271.md` 3.1).
 *
 * Varka serves only batches of the Arrow cache serializer, and Spark's suites read `VALUES`
 * lists and small files, which never reach that cache. The extension puts an Arrow-cached copy
 * under every relation of theirs that has a column Varka can read, so the query above it runs
 * on Arrow batches; [[VarkaArrowSource.conf]] sets what that needs. A listener counts, per test,
 * the query executions and the batches Varka served, and releases each query's caches.
 *
 * The cache changes nothing a query answers - the cache-only arm of [[VarkaSQLQueryTestSuite]]
 * holds that against Spark's golden files - with two exceptions it therefore leaves alone:
 *  - a plan holding a command (`DELETE`, `INSERT`, DDL): a cached copy of a command's target is
 *    not that table, so the command would act on, or fail against, the copy;
 *  - a relation whose output carries a hidden metadata column (`_metadata`): the cache keeps only
 *    the data, and the plan above it still reads the column.
 *
 * A `SELECT` with no `FROM` has no relation to cache, so the extension also lifts its literals
 * into one ([[VarkaArrowSource.LiftRule]], `VARKA-271.md` 3.3), unless
 * `-Dvarka.goldenSuite.lift=false`.
 */
object VarkaArrowSource {

  /** The settings the Arrow source needs, applied to a suite's `SparkConf`. */
  def conf(base: SparkConf, varkaEnabled: Boolean): SparkConf = base
    .set("spark.sql.cache.serializer", classOf[ArrowCachedBatchSerializer].getName)
    .set("spark.sql.extensions", classOf[VarkaArrowSourceExtension].getName)
    .set("spark.sql.queryExecutionListeners", classOf[VarkaArrowSourceListener].getName)
    // Folds a projection or filter over `VALUES` into a new `VALUES` before the cache rule sees
    // it, so the expression under test would never reach a kernel.
    .set("spark.sql.optimizer.excludedRules",
      "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation")
    .set("spark.sql.codegen.varka.enabled", varkaEnabled.toString)
    // The warm-up sends a shape's first batches to the row path, and a golden file's query is
    // usually the shape's only one. Off, the kernels run before C2 compiles them.
    .set("spark.sql.codegen.varka.warmup.enabled", "false")

  /** Whether a relation has a column a Varka kernel can read. */
  def varkaTyped(plan: LogicalPlan): Boolean =
    plan.output.exists(a => VarkaExpressionCompiler.readsColumnOf(a.dataType))

  // Set while the cache rule plans a relation's cached copy: that plan goes through the
  // optimizer too, and the rule must not wrap the relation inside its own copy.
  private val building: ThreadLocal[java.lang.Boolean] = ThreadLocal.withInitial(() => false)

  // Every cache the rule built and has not released: the listener releases a query's own after
  // it runs, and the suite whatever is left after each test. Caches a golden file makes itself
  // (`CACHE TABLE`) are not in it and are left to the file. By identity: a builder is a case
  // class whose equality compares plans, which a user's cache over the same relation can share.
  private val built = Collections.synchronizedSet(
    Collections.newSetFromMap(new IdentityHashMap[CachedRDDBuilder, java.lang.Boolean]()))

  /** Releases a cache the rule built; any other cache is left alone. */
  private[sql] def release(builder: CachedRDDBuilder): Unit =
    if (built.remove(builder)) builder.clearCache()

  private def cached(session: SparkSession, leaf: LogicalPlan): LogicalPlan = {
    building.set(true)
    try {
      val relation =
        InMemoryRelation(StorageLevel.MEMORY_ONLY, session.sessionState.executePlan(leaf), None)
      built.add(relation.cacheBuilder)
      relation
    } finally building.set(false)
  }

  /** The cache rule: an Arrow-cached copy under each Varka-typed relation of a query. */
  class CacheRule(session: SparkSession) extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan =
      if (building.get || plan.exists(_.isInstanceOf[Command])) plan else plan transformUp {
        case l: LocalRelation if l.data.nonEmpty && varkaTyped(l) => cached(session, l)
        case r @ LogicalRelation(_: HadoopFsRelation, _, _, _, _)
            if varkaTyped(r) && !r.output.exists(_.isMetadataCol) =>
          cached(session, r)
      }
  }

  // ----------------------------------------------------------------------------------------------
  // Lifting literals
  // ----------------------------------------------------------------------------------------------

  /** Whether the lift runs: on unless `-Dvarka.goldenSuite.lift=false`. */
  def liftEnabled: Boolean = !sys.props.get("varka.goldenSuite.lift").contains("false")

  /**
   * The rows of the relation a lifted `SELECT` reads: seventeen, so a kernel runs its loop over
   * at least one lane group at every vector width (4, 8 or 16 int lanes at 128, 256 or 512 bits)
   * and then its epilogue, where one row would run the epilogue alone.
   */
  val LiftedRows = 17

  /**
   * Whether a Varka kernel fuses `parent` with its `index`-th child read from a column of that
   * child's type: as a projection, or, for a predicate, as a filter. Asked of the compiler, so the
   * lift cannot reach a position Varka would not fuse, where a column can change what the
   * expression means (`map(1, ...)`'s key, a format string).
   */
  private def fusesAsColumn(parent: Expression, index: Int): Boolean = {
    val child = parent.children(index)
    val column = AttributeReference("lifted", child.dataType, nullable = false)()
    val asColumn = parent.withNewChildren(parent.children.updated(index, column))
    asColumn.resolved && (
      VarkaExpressionCompiler.compile(Seq(Alias(asColumn, "c")()), Seq(column)).isPresent ||
      (asColumn.dataType == BooleanType &&
        VarkaExpressionCompiler.compilePredicate(asColumn, Seq(column)).isPresent))
  }

  /**
   * The lift (`VARKA-271.md` 3.3): in each `SELECT` with no `FROM`, each non-null literal of a type
   * a kernel reads, in a position the compiler fuses as a column, becomes a column of a relation
   * of [[LiftedRows]] equal rows holding its value, and a `LIMIT 1` keeps one row. A plan
   * normalization rule, so it runs on the analyzed plan before the optimizer folds the constants;
   * analysis has fixed every output's name, and the rows are equal, so the query answers its own
   * schema and row, and the golden file still applies.
   */
  object LiftRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case p @ Project(list, _: OneRowRelation) if list.exists(_.exists(liftable)) =>
        val columns = mutable.LinkedHashMap.empty[Literal, AttributeReference]
        val lifted = list.map { entry =>
          // Each entry is lifted on a copy of the columns, kept only if every node of the
          // lifted entry still passes Spark's own type check: a function that needs a constant
          // argument (time_bucket's width) checks it there, and the compiler's answer for the
          // literal's parent says nothing about the ancestors above it.
          val tried = columns.clone()
          val candidate = lift(entry, tried)
          if (candidate.resolved &&
              !candidate.exists(!_.checkInputDataTypes().isSuccess)) {
            columns.clear()
            columns ++= tried
            candidate.asInstanceOf[NamedExpression]
          } else {
            entry
          }
        }
        if (columns.isEmpty) {
          p
        } else {
          // A sequence, not the key set's map: two literals of different types can hold the
          // same value (`INTERVAL '-13' MONTH` and `INTERVAL '-1-1' YEAR TO MONTH`), and a set
          // would keep one of them.
          val row = InternalRow.fromSeq(columns.keys.toSeq.map(_.value))
          Limit(Literal(1),
            Project(lifted, LocalRelation(columns.values.toSeq, Seq.fill(LiftedRows)(row))))
        }
    }

    /**
     * `e` with each liftable literal in a fused position replaced by its column in `columns`.
     * Nothing under a `try_` function is lifted: an error inside one reports the query context
     * of the expression Spark evaluates, which differs between constant folding and running the
     * plan, so a lifted literal would change the error's text and not its answer.
     */
    private def lift(
        e: Expression,
        columns: mutable.LinkedHashMap[Literal, AttributeReference]): Expression = e match {
      case t: TryEval => t
      // try_add, try_multiply and their kin are runtime-replaceable: the analyzed plan holds the
      // function, and its replacement is a TryEval or, over numbers, the operation in TRY mode
      // with no TryEval at all; the SQL name is what they share.
      case r: RuntimeReplaceable if r.prettyName.startsWith("try_") => e
      case _ => e.withNewChildren(e.children.zipWithIndex.map {
        case (l: Literal, i) if liftable(l) && fusesAsColumn(e, i) =>
          columns.getOrElseUpdate(l,
            AttributeReference(s"lifted${columns.size}", l.dataType, nullable = false)())
        case (c, _) => lift(c, columns)
      })
    }

    private def liftable(e: Expression): Boolean = e match {
      case l: Literal => l.value != null && VarkaExpressionCompiler.readsColumnOf(l.dataType)
      case _ => false
    }
  }

  // ----------------------------------------------------------------------------------------------
  // What the listener records
  // ----------------------------------------------------------------------------------------------

  /** One test's counts: its query executions, those Varka served a batch in, and the batches. */
  case class Served(executions: Long, served: Long, batches: Long) {
    def +(batchesOfOne: Long): Served =
      Served(executions + 1, served + (if (batchesOfOne > 0) 1 else 0), batches + batchesOfOne)
  }

  // The listener runs on the listener bus's thread; the suite reads after draining the bus.
  private var counts = Served(0, 0, 0)

  private[sql] def record(batches: Long): Unit = synchronized { counts = counts + batches }

  /**
   * The counts since the last call, and the caches the rule built since then released. The
   * caller drains the listener bus first, so every execution of the test has been recorded.
   */
  def takeCounts(): Served = {
    val taken = synchronized {
      val c = counts
      counts = Served(0, 0, 0)
      c
    }
    val left = built.synchronized(built.asScala.toList)
    left.foreach(release)
    taken
  }
}

/** Registers the cache rule and the lift; `spark.sql.extensions` names it. */
class VarkaArrowSourceExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(session => new VarkaArrowSource.CacheRule(session))
    if (VarkaArrowSource.liftEnabled) {
      extensions.injectPlanNormalizationRule(_ => VarkaArrowSource.LiftRule)
    }
  }
}

/**
 * Records, after each query execution, the batches Varka served in it (the `numVarkaBatches`
 * metric of its Varka nodes, subqueries included), and releases the caches the cache rule built
 * for it, so a golden file of hundreds of queries does not hold them all.
 */
class VarkaArrowSourceListener extends QueryExecutionListener with AdaptiveSparkPlanHelper {
  private def record(qe: QueryExecution): Unit = {
    // A query that failed in planning has no executed plan; it counts as an execution with no
    // batch served, and the rule's own list releases whatever it cached.
    val batches = Try(qe.executedPlan).toOption.map { plan =>
      collectWithSubqueries(plan) { case s: InMemoryTableScanExec => s }
        .foreach(s => VarkaArrowSource.release(s.relation.cacheBuilder))
      collectWithSubqueries(plan) {
        case n if n.nodeName.startsWith("Varka") => n.metrics.get("numVarkaBatches").map(_.value)
      }.flatten.sum
    }.getOrElse(0L)
    VarkaArrowSource.record(batches)
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    record(qe)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
    record(qe)
}
