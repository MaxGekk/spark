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

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.SparkArithmeticException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaChrono
import org.apache.spark.sql.execution.VarkaMaybeThrows.Answered

/**
 * Dates outside the column bound (VARKA-379). The compiler reads a bare date column as
 * 0001-01-01 to 9999-12-31 and fuses a calendar field over it with no per-lane check, because
 * the civil-from-days lowering is exact well past that range. Spark stores any int32 day count,
 * though, and a cached column can hold one: until VARKA-379 a calendar field over a day past
 * 42400-02-29 or before -5394572 was served wrong, with nothing declined. Now a compile that
 * rests on the bound guards the column in the kernel, which checks each batch's live days on the
 * vectors it loads and declines the batch with `STATUS_VALUE_RANGE` when one is outside, so every
 * answer here is Spark's - from the kernel where the batch is inside the bound, from the row
 * engine after a declined batch where it is not. The fixture's views hold one batch, which with
 * the null row's masked body and the dense body without it puts every day through both bodies'
 * tails; `VarkaGuardEdgeSuite` (catalyst) drives the guard in full lane groups, the tail and split
 * methods below the evaluator.
 *
 * The shapes are every row of `sql/varka/coverage.json` over the date, int and year-month
 * columns, so a calendar row added to the table is run here with no change to this file, and the
 * hand-picked shapes the reproduction found: a column plus a literal, a filter, and the removed
 * overflow checks a `datediff`'s bound pays for.
 */
class VarkaFarDaySuite extends QueryTest with VarkaMaybeThrows {

  /** Day counts past the bound: the lowering's last exact days and one past, and int32's ends. */
  private val farDays = Seq(14766813, 14766814, 100000000, -5394572, -5394573, -14766814,
    Int.MaxValue, Int.MinValue)

  /** The bound's own edges, beside two ordinary days. */
  private val edgeDays = Seq(VarkaChrono.CONTRACT_MIN_DAYS, VarkaChrono.CONTRACT_MAX_DAYS, 0,
    19000)

  /** The columns the fixture below has; a coverage row over any other column is not run here. */
  private val fixtureColumns = Set("d", "d2", "i", "ym", "ymm", "ymy")

  /**
   * One row of the table, and whether its domain holds the column bound: the rows whose answer
   * rests on it, which must decline a batch past it, where every other row's lowering is exact
   * for any int day and must decline nothing on the column bound's guard.
   */
  private case class CoverageRow(sql: String, executable: String, form: String, bounded: Boolean)

  private lazy val coverageRows: Seq[CoverageRow] = {
    val file = getWorkspaceFilePath("sql", "varka", "coverage.json").toFile
    val doc = new ObjectMapper().readTree(file)
    val columns = doc.get("columns").fieldNames().asScala.toSet
    doc.get("expressions").elements().asScala.toSeq.map { e =>
      val domain = Option(e.get("domain")).map(_.asText()).getOrElse("")
      CoverageRow(e.get("sql").asText(), e.get("executable").asText(), e.get("form").asText(),
        domain.contains("unix_date(d) BETWEEN"))
    }.filter { r =>
      val used = "[a-z0-9_]+".r.findAllIn(r.executable).toSet.intersect(columns)
      used.contains("d") && used.subsetOf(fixtureColumns)
    }
  }

  /**
   * A cached view over `days` in one batch: `d` the day itself, `d2` an ordinary day so that a
   * two-date shape still runs, `i` in 1..12, and the year-month columns small. A null row puts
   * the batch through the masked body; without it the dense body serves it.
   */
  private def withDays(days: Seq[Int], nulls: Boolean)(body: => Unit): Unit = {
    val values = days.zipWithIndex.map { case (v, k) =>
      s"($v, ${19000 + k}, ${k % 12 + 1}, ${k % 5}, ${k % 3}, ${k % 7})"
    } ++ (if (nulls) Seq("(NULL, NULL, NULL, NULL, NULL, NULL)") else Nil)
    // Every column nullable even where no row is null, through a `nullif` on a value no row
    // holds: a column the optimizer knows is never null folds `d IS NULL` and `coalesce(d, d2)`
    // away before Varka sees them.
    val cols = Seq("v", "w", "i", "a", "b", "c")
      .map(c => s"nullif($c, -999999999) AS $c").mkString(", ")
    val view = "SELECT v, date_from_unix_date(v) AS d, date_from_unix_date(w) AS d2, i, " +
      "make_ym_interval(0, a) AS ym, CAST(b AS INTERVAL MONTH) AS ymm, " +
      s"CAST(c AS INTERVAL YEAR) AS ymy FROM (SELECT $cols FROM VALUES " +
      values.mkString(", ") + " AS t(v, w, i, a, b, c))"
    for (session <- Seq(spark, varkaSpark)) {
      session.sql(view).coalesce(1).createOrReplaceTempView("varka_far_days")
      session.catalog.cacheTable("varka_far_days")
    }
    try body finally {
      Seq(spark, varkaSpark).foreach(_.catalog.uncacheTable("varka_far_days"))
    }
  }

  private def metric(plan: SparkPlan, name: String): Long =
    collect(plan) { case v if isVarkaNode(v) => v.metrics.get(name).map(_.value).getOrElse(0L) }
      .sum

  /**
   * The decline reason of the column bound's guard, a `GuardedRange` in the kernel. The fixture's
   * rows hold no other range guard - the `TIME` family's are on columns this view lacks - so a
   * count under it is the column bound's.
   */
  private val columnBound = VarkaExecMetrics.reasonKey("value-range")

  /**
   * Runs `query` on both engines: the same rows or the same error. Where Spark answered, the
   * query must have reached a Varka node, every batch it read was either served by its kernel or
   * declined - nothing failed - and the batches declined on the column bound are returned; none
   * where either engine raised or no batch was read.
   */
  private def check(query: String): Option[Long] = {
    val onSpark = outcome(spark, query)
    val (onVarka, plan) = runOutcome(varkaSpark, query)
    VarkaMaybeThrows.difference(onSpark, onVarka).foreach(d => fail(s"$query\n$d"))
    onSpark match {
      case Answered(rows) =>
        val p = plan.getOrElse(fail(s"$query: no plan"))
        assertFused(p)
        assert(metric(p, "numFallbackBatchesKernel") === 0L, s"$query: a kernel failed")
        val served = metric(p, "numVarkaBatches")
        val declined = metric(p, "numFallbackBatchesDeclined")
        // A filter the cache's batch statistics prune (`d IS NULL` over a batch with no null)
        // reads no batch at all, and answers no row.
        assert(rows.isEmpty || served + declined > 0L,
          s"$query: no batch reached the kernel:\n${p.treeString}")
        if (served + declined > 0L) Some(metric(p, columnBound)) else None
      case _ => None
    }
  }

  private def query(r: CoverageRow): String =
    if (r.form == "projection") s"SELECT v, ${r.executable} AS r FROM varka_far_days"
    else s"SELECT v FROM varka_far_days WHERE ${r.executable}"

  test("the reproduction: year, month and day of a cached column past the bound") {
    val days = Seq(0, 14766813, 14766814, 100000000, -14766814)
    for (nulls <- Seq(true, false)) {
      withDays(days, nulls) {
        for (e <- Seq("year(d)", "month(d)", "dayofmonth(d)")) {
          withClue(s"$e, nulls=$nulls: ") {
            assert(check(s"SELECT v, $e AS r FROM varka_far_days").exists(_ > 0L),
              "the batch holds days past the bound, so it must decline on the column bound")
          }
        }
      }
    }
  }

  test("every coverage row over a date column answers Spark's over days past the bound") {
    assert(coverageRows.exists(r => r.executable == "year(d)" && r.bounded),
      "the table's rows were not read")
    assert(coverageRows.exists(r => r.executable == "dayofweek(d)" && !r.bounded))
    for (nulls <- Seq(true, false)) {
      withDays(farDays, nulls) {
        for (r <- coverageRows) {
          withClue(s"${r.sql}, nulls=$nulls: ") {
            val declined = check(query(r))
            if (r.bounded) {
              assert(declined.forall(_ > 0L), "an answer resting on the bound must decline")
            } else {
              assert(declined.forall(_ == 0L), "a lowering exact for every day needs no bound")
            }
          }
        }
      }
    }
  }

  test("no batch declines on the column bound at the bound's own edges") {
    for (nulls <- Seq(true, false)) {
      withDays(edgeDays, nulls) {
        for (r <- coverageRows) {
          withClue(s"${r.sql}, nulls=$nulls: ") {
            assert(check(query(r)).forall(_ == 0L))
          }
        }
      }
    }
    // One day past either edge is past the bound.
    for (v <- Seq(VarkaChrono.CONTRACT_MIN_DAYS - 1, VarkaChrono.CONTRACT_MAX_DAYS + 1)) {
      withDays(Seq(0, v), nulls = false) {
        withClue(s"v=$v: ") {
          assert(check("SELECT v, year(d) AS r FROM varka_far_days").exists(_ > 0L))
        }
      }
    }
  }

  test("a column plus a literal, and a filter, over days past the bound") {
    withDays(farDays, nulls = true) {
      for (e <- Seq("year(date_add(d, 1))", "dayofmonth(date_sub(d, 1))",
          "month(d + INTERVAL 1 DAY)")) {
        withClue(e) {
          assert(check(s"SELECT v, $e AS r FROM varka_far_days").exists(_ > 0L))
        }
      }
      // 100000000 is year 275760; a kernel that decomposed it wrong keeps no row or another.
      assert(check("SELECT v FROM varka_far_days WHERE year(d) = 275760").exists(_ > 0L))
      assert(check("SELECT v FROM varka_far_days WHERE dayofmonth(d) = 2").exists(_ > 0L))
    }
  }

  test("a datediff's bound removes no overflow check over days past the bound") {
    // Under ANSI, `datediff(d2, d) * 100` and its negation fuse with no overflow check because
    // the bound puts a difference of two columns within 3.7 million days. With a day past the
    // bound the product overflows, and Spark raises: the kernel must not wrap instead.
    withAnsi(enabled = true) {
      withDays(Seq(0, 100000000), nulls = false) {
        checkSparkRaisesTheSame[SparkArithmeticException](
          "SELECT v, datediff(d, d2) * 100 AS r FROM varka_far_days")
      }
      withDays(Seq(0, Int.MinValue), nulls = false) {
        checkSparkRaisesTheSame[SparkArithmeticException](
          "SELECT v, -datediff(d, date_from_unix_date(0)) AS r FROM varka_far_days")
      }
      // The same shapes inside the bound still fuse and are served.
      withDays(Seq(0, 19000, VarkaChrono.CONTRACT_MAX_DAYS), nulls = false) {
        assert(check("SELECT v, datediff(d, d2) * 100 AS r FROM varka_far_days").contains(0L))
      }
    }
  }
}
