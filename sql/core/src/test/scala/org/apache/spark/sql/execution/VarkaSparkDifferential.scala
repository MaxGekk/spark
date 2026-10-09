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

import scala.util.Try

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaTestWatchdog

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
    val out: Outcome = Try(df.collect().map(_.toString).sorted.toSeq).toEither.left.map { e =>
      val chain = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).toSeq
      chain.collectFirst { case t: SparkThrowable if t.getCondition != null => t.getCondition }
        .getOrElse(e.getClass.getName)
    }
    val residual = Try(collect(df.queryExecution.executedPlan) { case v if isVarkaNode(v) => v }
      .flatMap(_.metrics.get("numResidualEntries")).map(_.value).sum).getOrElse(0L)
    (out, residual)
  }

  /**
   * The outcome of `query` with output `k` forced to decline (`forceResidualAt`), compared with
   * the row engine's `off` when the plan proves the force moved execution: exactly one more
   * residual entry than `residualOn`. A run whose plan did not change is counted, never passed.
   */
  private def forcedDifference(query: String, k: Int, off: Outcome, residualOn: Long)
      : Option[String] = {
    val previous = VarkaColumnarToRowExec.currentEmitOptions
    VarkaColumnarToRowExec.setEmitOptionsForTesting(previous.withForceResidualAt(k + 1))
    val (forced, residualForced) = try outcomeAndResidual(varkaSpark, query) finally {
      VarkaColumnarToRowExec.setEmitOptionsForTesting(previous)
    }
    forcedRuns += 1
    if (residualForced == residualOn + 1) {
      forcedApplied += 1
      difference(off, forced).map(why => s"with an output forced to decline, $why")
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
    for (session <- Seq(spark, varkaSpark)) {
      session.sql(fixture).createOrReplaceTempView("fz")
      session.catalog.cacheTable("fz")
    }
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
      if (where.isDefined) off.foreach { rows =>
        filtered += 1
        if (rows.nonEmpty) filteredNonEmpty += 1
        if (pivot.isDefined) {
          pivotFiltered += 1
          if (rows.nonEmpty) pivotFilteredNonEmpty += 1
        }
      }
      difference(off, on).orElse {
        forces.iterator.flatMap(k => forcedDifference(query, k, off, residualOn)).nextOption()
      }.orElse {
        pivot.filter(_ => where.isDefined).flatMap { row =>
          // The outputs over the pivot row alone, by the row engine.
          val alone = outcome(spark, s"SELECT $select FROM (${fixtureRows(Seq(row))}) AS fz")
          pivotMissing(off, on, alone)
        }
      }.orElse {
        if (partitions && where.isDefined) {
          Seq(spark, varkaSpark).iterator.flatMap(s =>
            partitionBroken(select, where.get, outcome(s, _))).toSeq.headOption
        } else {
          None
        }
      }
    } finally {
      for (session <- Seq(spark, varkaSpark)) session.catalog.uncacheTable("fz")
    }
  }
}
