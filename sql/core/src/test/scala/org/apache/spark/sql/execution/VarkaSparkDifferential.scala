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

package org.apache.spark.sql.execution

import java.util.Locale

import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.{VarkaKernelWarmup,
  VarkaTestWatchdog}
import org.apache.spark.sql.internal.SQLConf

/**
 * The check the random differential and its replay share (VARKA-262): a fixture registered and
 * cached in both sessions, a query run in both, the outcomes compared, and the ternary partition
 * of a filter. `fused` counts the plans that took a Varka node, so that a run can say how much
 * of what it compared the kernels could have answered.
 */
trait VarkaSparkDifferential extends QueryTest with VarkaSharedSessions with VarkaTestWatchdog {
  import VarkaSparkFuzz._

  /** Plans of the Varka session, over the checks run, that took a Varka node. */
  protected var fused = 0
  protected var compared = 0
  protected var bothErrored = 0

  /**
   * What the two engines raised over the checks run (VARKA-319): per condition, engine and mode,
   * how many checks raised it. `unusedExpectedErrors` reads it against the list.
   */
  protected val errorsSeen =
    scala.collection.mutable.TreeMap.empty[(String, String, String), Int]

  private def noteRaises(off: Outcome, on: Outcome, ansi: Boolean): Unit = {
    val mode = if (ansi) "ANSI" else "LEGACY"
    for ((side, o) <- Seq("row" -> off, "varka" -> on); raised <- o.left.toOption) {
      val key = (raised, side, mode)
      errorsSeen(key) = errorsSeen.getOrElse(key, 0) + 1
    }
  }

  /** The census, one line per condition, engine and mode. */
  protected def errorCensus: Seq[String] =
    errorsSeen.toSeq.map { case ((raised, side, mode), n) => s"$n x $raised, $side, $mode" }

  /** The entries of the expected-errors list that no check run raised. */
  protected def unusedExpectedErrors: Seq[String] =
    VarkaExpectedErrors.entries().asScala.toSeq.filterNot { e =>
      errorsSeen.keys.exists { case (raised, side, mode) =>
        raised == e.condition() && e.modes().contains(VarkaExpectedErrors.Mode.valueOf(mode)) &&
          e.side().covers(VarkaExpectedErrors.Side.valueOf(side.toUpperCase(Locale.ROOT)))
      }
    }.map { e =>
      s"${e.condition()} (${e.side().toString.toLowerCase(Locale.ROOT)}, ${e.modes()})"
    }

  /** Filtered queries the row engine answered, and those that returned a row (VARKA-297). */
  protected var filtered = 0
  protected var filteredNonEmpty = 0
  protected var pivotFiltered = 0
  protected var pivotFilteredNonEmpty = 0

  /**
   * Forced runs (VARKA-296): outputs forced to decline, and those whose executed plan reported
   * exactly one more residual entry than the unforced run's, the only ones compared.
   */
  protected var forcedRuns = 0
  protected var forcedApplied = 0

  /**
   * `sql`'s outcome on `session`, as [[VarkaSparkFuzz.outcome]] gives it, and the residual
   * entries the Varka nodes of the plan it ran with reported.
   */
  private def outcomeAndResidual(session: SparkSession, sql: String): (Outcome, Long) = {
    val df = session.sql(sql)
    val out = outcomeOf(df)
    val residual = Try(collect(df.queryExecution.executedPlan) { case v if isVarkaNode(v) => v }
      .flatMap(_.metrics.get("numResidualEntries")).map(_.value).sum).getOrElse(0L)
    (out, residual)
  }

  /**
   * The outcome of `query` with output `k` forced to decline (`forceResidualAt`), compared with
   * the row engine's `off` when the plan proves the force moved execution: exactly one more
   * residual entry than `residualOn`. A run whose plan did not change is counted, never passed.
   */
  private def forcedDifference(
      query: String, k: Int, off: Outcome, residualOn: Long, ansi: Boolean): Option[String] = {
    val previous = VarkaColumnarToRowExec.currentEmitOptions
    VarkaColumnarToRowExec.setEmitOptionsForTesting(previous.withForceResidualAt(k + 1))
    val (forced, residualForced) = try outcomeAndResidual(varkaSpark, query) finally {
      VarkaColumnarToRowExec.setEmitOptionsForTesting(previous)
    }
    forcedRuns += 1
    if (residualForced == residualOn + 1) {
      forcedApplied += 1
      unexpectedRaise(forced, VarkaExpectedErrors.Side.VARKA, ansi)
        .orElse(difference(off, forced)).map(why => s"with an output forced to decline, $why")
    } else {
      None
    }
  }

  /**
   * `c` with its conjuncts rectified against its pivot row (VARKA-297), as Pivoted Query
   * Synthesis does: each conjunct is evaluated on the pivot alone by the row engine, kept as it is
   * if TRUE, negated if FALSE and tested `IS NULL` if NULL, so that the filter selects the pivot
   * whatever the other rows are. A conjunct the row engine raises on is dropped. A case without
   * a pivot is returned as it is.
   */
  protected def rectify(c: Case): Case = c.pivot match {
    case Some(row) if c.conjuncts.nonEmpty =>
      spark.sql(fixtureRows(Seq(rowText(row)))).createOrReplaceTempView("fzp")
      try withAnsi(c.ansi) {
        c.copy(conjuncts = c.conjuncts.flatMap { cj =>
          Try(spark.sql(s"SELECT (${cj.sql}) FROM fzp").collect().head.get(0)).toOption.map {
            case b: java.lang.Boolean => Conjunct(cj.sql, negated = !b.booleanValue())
            case _ => Conjunct(cj.sql, negated = false, isNull = true)
          }
        })
      } finally {
        spark.catalog.dropTempView("fzp")
      }
    case _ => c
  }

  /**
   * The kind of disagreement `select` over `where` shows on `fixture` under `ansi`, or None.
   * `partitions` also checks the ternary partition of the filter on both engines, and each of
   * `forces` is an output run once more on Varka forced to decline (VARKA-296).
   */
  protected def disagreement(
      fixture: String,
      select: String,
      where: Option[String],
      ansi: Boolean,
      partitions: Boolean,
      pivot: Option[String] = None,
      forces: Seq[Int] = Nil): Option[String] = {
    cacheFixture(fixture)
    try withAnsi(ansi) {
      val query = querySql(select, where)
      val off = outcome(spark, query)
      val (on, residualOn) = outcomeAndResidual(varkaSpark, query)
      compared += 1
      if (scala.util.Try(varkaSpark.sql(query).queryExecution.executedPlan).toOption
          .exists(p => find(p)(isVarkaNode).isDefined)) {
        fused += 1
      }
      if (off.isLeft && on.isLeft) bothErrored += 1
      noteRaises(off, on, ansi)
      if (where.isDefined) off.foreach { rows =>
        filtered += 1
        if (rows.nonEmpty) filteredNonEmpty += 1
        if (pivot.isDefined) {
          pivotFiltered += 1
          if (rows.nonEmpty) pivotFilteredNonEmpty += 1
        }
      }
      unexpectedError(off, on, ansi).orElse(difference(off, on)).orElse {
        forces.iterator.flatMap(k => forcedDifference(query, k, off, residualOn, ansi)).nextOption()
      }.orElse {
        pivot.filter(_ => where.isDefined).flatMap { row =>
          // The outputs over the pivot row alone, by the row engine.
          val alone = outcome(spark, s"SELECT $select FROM (${fixtureRows(Seq(row))}) AS fz")
          pivotMissing(off, on, alone)
        }
      }.orElse {
        if (partitions && where.isDefined) {
          Seq(spark -> VarkaExpectedErrors.Side.ROW, varkaSpark -> VarkaExpectedErrors.Side.VARKA)
            .iterator.flatMap { case (s, side) =>
              partitionBroken(select, where.get, outcome(s, _), unexpectedRaise(_, side, ansi))
            }.toSeq.headOption
        } else {
          None
        }
      }
    } finally {
      uncacheFixture()
    }
  }

  /** Registers `fixture` as the view `fz` in both sessions, and caches it. */
  protected def cacheFixture(fixture: String): Unit = for (session <- Seq(spark, varkaSpark)) {
    session.sql(fixture).createOrReplaceTempView("fz")
    session.catalog.cacheTable("fz")
  }

  protected def uncacheFixture(): Unit =
    for (session <- Seq(spark, varkaSpark)) session.catalog.uncacheTable("fz")

  // ---- the warmed arm (VARKA-327) ---------------------------------------------------------

  /** `VarkaKernelWarmup.DEADLINE_PROPERTY`, which is package-private to the warm-up. */
  private val warmupDeadlineProperty = "varka.warmup.deadlineSeconds"

  /** How long the warmed arm waits for the warm-up between runs: the deadline and a margin. */
  private def warmupWaitMillis: Long = {
    val deadline = sys.props.get(warmupDeadlineProperty).flatMap(_.toIntOption)
      .filter(_ > 0).getOrElse(60)
    (deadline + 30) * 1000L
  }

  /**
   * One run on `varkaSpark`: its outcome, how many Varka nodes its plan has, and their input,
   * warm-up, kernel and declined batches.
   */
  private case class WarmedRun(outcome: Outcome, nodes: Int, inputBatches: Long,
      warmupBatches: Long, kernelBatches: Long, declined: Long)

  private def warmedRun(sql: String): WarmedRun = {
    val df = varkaSpark.sql(sql)
    val out = outcomeOf(df)
    val nodes = Try(collect(df.queryExecution.executedPlan) { case v if isVarkaNode(v) => v })
      .getOrElse(Nil)
    def total(name: String): Long = nodes.flatMap(_.metrics.get(name)).map(_.value).sum
    WarmedRun(out, nodes.size, total("numInputBatches"), total("numWarmupBatches"),
      total("numVarkaBatches"), total("numFallbackBatchesDeclined"))
  }

  /**
   * The kind of disagreement `c` shows with the kernel warm-up on (VARKA-327), or None: the path
   * production takes, where a new shape's batches take the row path while a background thread
   * gets its kernel compiled, and later ones the kernel. The same query runs on `varkaSpark` until
   * no Varka node of it waited on a warm-up, waiting for the warm-ups between runs, at most four
   * runs: a filter's warm-up comes first, and a projection over it claims its own only once the
   * filter serves Arrow batches. Every run is compared with the row engine. Then a shape that
   * reached its kernel runs once more over `later`, data with the same nulls and new values, so
   * that code compiled from one batch's profile meets a batch it was not compiled from, as
   * production's later batches do. Nothing here reads the warm-up's verdict or its timing: a
   * kernel released at the deadline serves batches as a compiled one does, and either way the
   * answers must agree. How the runs went is added to `counts`.
   */
  protected def warmedDisagreement(
      c: Case, later: Vector[Vector[String]], counts: WarmedCounts): Option[String] = {
    val query = c.query
    // The outputs over the pivot row alone, by the row engine, asked once for every run.
    lazy val pivotAlone = c.pivot.filter(_ => c.where.isDefined).map { row =>
      outcome(spark, s"SELECT ${c.select} FROM (${fixtureRows(Seq(rowText(row)))}) AS fz")
    }
    def compare(off: Outcome, run: WarmedRun, side: String, pivot: Boolean): Option[String] =
      unexpectedError(off, run.outcome, c.ansi).orElse(difference(off, run.outcome)).orElse {
        if (pivot) pivotAlone.flatMap(pivotMissing(off, run.outcome, _)) else None
      }.map(why => s"$side, $why")
    def await(): Unit = if (!VarkaKernelWarmup.awaitIdle(warmupWaitMillis)) counts.notIdle += 1
    counts.compositions += 1
    cacheFixture(c.fixture)
    try withAnsi(c.ansi) {
      withVarkaWarmup(counts) {
        val off = outcome(spark, query)
        var found: Option[String] = None
        var runs = 0
        var waited = true
        var sawRowPath = false
        var sawKernel = false
        var last: WarmedRun = null
        while (found.isEmpty && waited && runs < 4) {
          val run = warmedRun(query)
          last = run
          runs += 1
          noteRaises(off, run.outcome, c.ansi)
          waited = run.warmupBatches > 0
          found = compare(off, run, sideOf(run), pivot = true)
          if (waited) counts.rowPathRuns += 1
          if (run.kernelBatches > 0) counts.kernelRuns += 1
          sawRowPath ||= waited
          sawKernel ||= run.kernelBatches > 0
          if (waited) await()
        }
        if (sawRowPath && sawKernel) counts.handedOver += 1
        if (!sawKernel && found.isEmpty) {
          if (last.nodes == 0) counts.unfused += 1
          else if (last.outcome.isLeft) counts.raised += 1
          else if (last.inputBatches == 0) counts.noBatches += 1
          else if (last.declined > 0) counts.declined += 1
          else counts.stillWarming += 1
        }
        // A shape that never reached a kernel has nothing compiled to meet other data.
        found.orElse(if (!sawKernel) None else {
          uncacheFixture()
          cacheFixture(fixtureSql(later))
          val laterOff = outcome(spark, query)
          val run = warmedRun(query)
          counts.laterRuns += 1
          if (run.warmupBatches > 0) {
            counts.laterNewShape += 1
            await()
          }
          compare(laterOff, run, s"over other data after the handover, ${sideOf(run)}",
            pivot = false)
        })
      }
    } finally {
      uncacheFixture()
    }
  }

  /**
   * Which side of the handover served a run, as a disagreement's kind names it: the row path while
   * every warming node waited, the kernel once none did, and a run with both - one node on its
   * kernel while another warms, a filter's below a projection's - or with neither, every batch
   * declined or pruned, named as such.
   */
  private def sideOf(run: WarmedRun): String =
    (run.warmupBatches > 0, run.kernelBatches > 0) match {
      case (true, false) => "on the row path while the kernel warms"
      case (false, true) => "after the handover to the kernel"
      case (true, true) => "with one node on its kernel while another warms"
      case (false, false) => "with no batch on a kernel"
    }

  /**
   * Runs `body` with the kernel warm-up on, which the shared sessions turn off (it matters only
   * to `varkaSpark`), and restores the setting after waiting for the warm-ups `body` queued, so
   * that none outlives it; a wait that times out is counted in `counts`.
   */
  protected def withVarkaWarmup[T](counts: WarmedCounts)(body: => T): T =
    withBothConf(SQLConf.VARKA_WARMUP_ENABLED.key, "true") {
      try body finally {
        if (!VarkaKernelWarmup.awaitIdle(warmupWaitMillis)) counts.notIdle += 1
      }
    }

  /**
   * Runs `body` with the warm-up's deadline at `seconds`, restoring what was set before. The
   * warmed arm shortens it: about one small kernel in fifteen never gets its verdict and waits
   * out the whole deadline, a minute in production, and a release at any deadline hands the
   * shape to the kernel just as a verdict does, so the answers compared are the same.
   */
  protected def withWarmupDeadline[T](seconds: Int)(body: => T): T = {
    val previous = sys.props.get(warmupDeadlineProperty)
    sys.props(warmupDeadlineProperty) = seconds.toString
    try body finally previous match {
      case Some(value) => sys.props(warmupDeadlineProperty) = value
      case None => sys.props.remove(warmupDeadlineProperty)
    }
  }
}
