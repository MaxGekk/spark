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

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaShapeCache
import org.apache.spark.sql.internal.SQLConf

/**
 * Differential tests (Task 7): the Varka session must produce results identical to the row-based
 * engine across a query matrix - literal offsets (including extreme values), both `datediff`
 * argument orders, null patterns, foldable offsets, ineligible projections, nested expressions,
 * filters/aggregation, multi-batch caches, multi-task scans, and a non-Arrow columnar source.
 * Where the projection is fused into [[VarkaColumnarToRowExec]], the SIMD kernels must actually
 * process the Arrow batches; where it is not, the plan must be untouched.
 */
class VarkaDifferentialSuite extends QueryTest with VarkaSharedSessions {

  private def metaspaceUsed(): Long = {
    java.lang.management.ManagementFactory.getMemoryPoolMXBeans.asScala.collect {
      case p if p.getName == "Metaspace" || p.getName == "Compressed Class Space" =>
        p.getUsage.getUsed
    }.sum
  }

  /**
   * Runs `query` on the base row-engine session and on the varka session, asserting the results
   * match and that the plan is fused (and the kernels ran) exactly when `expectFused`.
   */
  private def checkDifferential(
      expectedSession: SparkSession,
      actualSession: SparkSession,
      query: String,
      expectFused: Boolean): SparkPlan = {
    val expected = expectedSession.sql(query)
    val actual = actualSession.sql(query)
    val plan = actual.queryExecution.executedPlan
    if (expectFused) {
      assertFused(plan)
      checkAnswer(actual, expected)
      assertKernelsRan(plan)
      // Task 18: execute the query a second time, so the kernel class is served from the warm
      // cross-task cache - a wrong or stale hit would surface as a wrong answer right here,
      // which the ghost fallback could never catch.
      checkAnswer(actualSession.sql(query), expected)
    } else {
      assertNotFused(plan)
      checkAnswer(actual, expected)
    }
    plan
  }

  test("date_add and date_sub match the row engine across literal offsets") {
    cacheDates(spark)
    cacheDates(varkaSpark)
    Seq(0, 3, -5, 100).foreach { off =>
      checkDifferential(spark, varkaSpark,
        s"SELECT date_add(d, $off) AS a, date_sub(d, $off) AS b FROM varka_dates ORDER BY a",
        expectFused = true)
    }
    // Extreme offsets wrap the int32 day arithmetic. Spark's own DateAdd semantics (and the SIMD
    // kernel) are a plain int add that wraps mod 2^32, but the end-to-end row engine applies an
    // extra calendar-day rebase to DATE results outside its representable range, so the row
    // engine is NOT the right oracle at the overflow boundary (and those days cannot be decoded
    // to java.sql.Date). The oracle here is therefore the plain int32 wrap - DateAdd.eval and
    // this kernel agree - computed in Scala over the fixed `cacheDates` input, null-aware, in
    // deterministic input order.
    val inputDays: Seq[java.lang.Integer] = Seq(
      "2024-01-01", "2024-01-02", "2023-12-27", "1969-12-31", null).map { v =>
        if (v == null) null else java.time.LocalDate.parse(v).toEpochDay.toInt
      }
    val addWrap = (d: Int, off: Int) => d + off // Scala Int wraps mod 2^32
    val subWrap = (d: Int, off: Int) => d - off
    Seq(Int.MaxValue - 1, Int.MinValue).foreach { off =>
      val query =
        s"SELECT date_add(d, $off) AS a, date_sub(d, $off) AS b FROM varka_dates"
      val actual = varkaSpark.sql(query)
      val plan = actual.queryExecution.executedPlan
      assertFused(plan)
      // `toRdd` hands back the projection's own row, rewritten per row, so the rows have to be
      // copied before they are collected into an array. This is not a Varka rule: the row engine
      // reuses rows here too, and collecting this query from it without the copy yields two
      // distinct row objects for five rows.
      val rows = actual.queryExecution.toRdd.map(_.copy()).collect()
      assert(rows.length == inputDays.length,
        s"expected ${inputDays.length} rows for offset $off but got ${rows.length}")
      rows.zip(inputDays).foreach { case (a, d) =>
        val expectedAdd = if (d == null) null else addWrap(d, off)
        val expectedSub = if (d == null) null else subWrap(d, off)
        assert(a.isNullAt(0) == (expectedAdd == null),
          s"date_add null mismatch (offset $off, input $d)")
        assert(a.isNullAt(1) == (expectedSub == null),
          s"date_sub null mismatch (offset $off, input $d)")
        if (expectedAdd != null) {
          assert(a.getInt(0) == expectedAdd,
            s"date_add day mismatch (offset $off, input $d)")
        }
        if (expectedSub != null) {
          assert(a.getInt(1) == expectedSub,
            s"date_sub day mismatch (offset $off, input $d)")
        }
      }
      assertKernelsRan(plan)
    }
  }

  test("task 38: date_add/date_sub with a column offset match the row engine") {
    // `varka_date_pairs`'s `i` column is not nullable (it comes from zipWithIndex) - the
    // literal-offset shapes already covered that side; this exercises the new column-offset
    // path over both spellings.
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT date_add(d, i) AS a, d + i AS b, date_sub(d, i) AS c FROM varka_date_pairs " +
        "ORDER BY a, b, c",
      expectFused = true)
  }

  test("task 38: a null offset nulls out its row even when the date beside it is not null") {
    cacheDatesNullableOffset(spark)
    cacheDatesNullableOffset(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT date_add(d, off) AS a, date_sub(d, off) AS b FROM varka_dates_nullable_offset " +
        "ORDER BY d, off",
      expectFused = true)
  }

  test("task 38 declines: an interval column offset still does not fuse") {
    // `d + INTERVAL n DAY` with a foldable `n` already fuses (the analyzer folds it to a
    // DateAdd literal, unaffected by this task). A non-foldable interval *column* does not:
    // BinaryArithmeticWithDatetimeResolver rewrites it to
    // DateAdd(d, ExtractANSIIntervalDays(intervalCol)), and ExtractANSIIntervalDays has no
    // compiler arm, so it declines through the ordinary unsupported-expression path.
    // CAST(i AS INTERVAL DAY) is DayTimeIntervalType(DAY, DAY) - a single-field ANSI interval,
    // not the literal `INTERVAL '3' DAY` syntax the optimizer folds away.
    cacheDates(spark)
    cacheDates(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT d + CAST(i AS INTERVAL DAY) AS a FROM varka_dates ORDER BY a",
      expectFused = false)
  }

  test("task 38: a column-offset date_add fuses inside a filter predicate too") {
    // The projection-side column-offset tests above never exercise the mask kernel - a
    // WHERE clause is the shape VarkaFilterExec/VarkaFilterColumnarToRowExec compile, and
    // compileOffset is shared code, so this proves the column-offset path works there too,
    // not only when the offset column feeds a projected value.
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    try {
      val fused = checkDifferential(spark, varkaSpark,
        "SELECT count(*) AS c FROM varka_date_pairs WHERE date_add(d, i) > d2",
        expectFused = true)
      assert(!fused.toString.contains("Filter (date_add("),
        s"the column-offset predicate should be fused, not residual:\n$fused")
    } finally {
      Seq(spark, varkaSpark).foreach(_.catalog.uncacheTable("varka_date_pairs"))
    }
  }

  test("datediff matches the row engine in both argument orders with nulls") {
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT datediff(d2, d) AS diff FROM varka_date_pairs ORDER BY diff",
      expectFused = true)
    checkDifferential(spark, varkaSpark,
      "SELECT datediff(d, d2) AS diff FROM varka_date_pairs ORDER BY diff",
      expectFused = true)
  }

  test("datediff matches the row engine on null-free and all-null inputs") {
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    spark.sql("SELECT d, d2 FROM varka_date_pairs WHERE d IS NOT NULL AND d2 IS NOT NULL")
      .createOrReplaceTempView("varka_null_free")
    spark.catalog.cacheTable("varka_null_free")
    spark.sql("SELECT CAST(NULL AS DATE) AS d, CAST(NULL AS DATE) AS d2 FROM varka_date_pairs")
      .createOrReplaceTempView("varka_all_null")
    spark.catalog.cacheTable("varka_all_null")
    varkaSpark.sql("SELECT d, d2 FROM varka_date_pairs WHERE d IS NOT NULL AND d2 IS NOT NULL")
      .createOrReplaceTempView("varka_null_free")
    varkaSpark.catalog.cacheTable("varka_null_free")
    varkaSpark.sql("SELECT CAST(NULL AS DATE) AS d, CAST(NULL AS DATE) AS d2 FROM varka_date_pairs")
      .createOrReplaceTempView("varka_all_null")
    varkaSpark.catalog.cacheTable("varka_all_null")
    checkDifferential(spark, varkaSpark,
      "SELECT datediff(d2, d) AS diff FROM varka_null_free ORDER BY diff",
      expectFused = true)
    checkDifferential(spark, varkaSpark,
      "SELECT datediff(d2, d) AS diff FROM varka_all_null ORDER BY diff",
      expectFused = true)
  }

  test("a mixed-eligibility projection fuses partially and matches the row engine") {
    // Pinned as "not fused" until task 12: one ineligible entry used to poison the whole
    // projection. Now the date entry runs on the kernels, the bare `i` forwards zero-copy, and
    // `i + 1` is evaluated per row beside them.
    cacheDates(spark)
    cacheDates(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT date_add(d, 3) AS a, i, i + 1 AS inc FROM varka_dates ORDER BY a",
      expectFused = true)
  }

  test("a projection of forwards and residuals alone stays unfused") {
    // Nothing to fuse means nothing gained: the rule leaves the projection on Janino.
    cacheDates(spark)
    cacheDates(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT i, i + 1 AS inc FROM varka_dates ORDER BY i",
      expectFused = false)
  }

  test("a predicated entry fuses beside residual and forwarded ones") {
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT CASE WHEN d < d2 THEN date_add(d, 1) ELSE d2 END AS a, i, i + 1 AS inc " +
        "FROM varka_date_pairs ORDER BY a, i",
      expectFused = true)
  }

  test("constant-folded offsets are fused and match the row engine") {
    cacheDates(spark)
    cacheDates(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT date_add(d, 1 + 2) AS a FROM varka_dates ORDER BY a",
      expectFused = true)
    checkDifferential(spark, varkaSpark,
      "SELECT date_add(d, CAST(3 AS INT)) AS a FROM varka_dates ORDER BY a",
      expectFused = true)
  }

  test("nested date expressions are fused and match the row engine") {
    // These planned as a plain per-row Project until task 10: the recursive compiler is what
    // makes `expectFused = true` hold here at all.
    cacheDates(spark)
    cacheDates(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT date_add(date_add(d, 1), 2) AS a FROM varka_dates ORDER BY a",
      expectFused = true)
    checkDifferential(spark, varkaSpark,
      "SELECT date_sub(date_add(d, 5), 5) AS a FROM varka_dates ORDER BY a",
      expectFused = true)
    checkDifferential(spark, varkaSpark,
      "SELECT date_sub(date_add(date_sub(date_add(d, 1), 2), 3), 4) AS a " +
        "FROM varka_dates ORDER BY a",
      expectFused = true)
  }

  test("datediff over nested chains is fused in both argument orders with nulls") {
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT datediff(date_add(d, 7), d2) AS diff FROM varka_date_pairs ORDER BY diff",
      expectFused = true)
    checkDifferential(spark, varkaSpark,
      "SELECT datediff(d2, date_sub(d, 7)) AS diff FROM varka_date_pairs ORDER BY diff",
      expectFused = true)
    checkDifferential(spark, varkaSpark,
      "SELECT datediff(date_add(d, 3), date_sub(d2, 3)) AS diff " +
        "FROM varka_date_pairs ORDER BY diff",
      expectFused = true)
  }

  test("a shared subchain across outputs is fused and matches the row engine") {
    // The milestone's DAG example: `date_add(d, 1)` feeds both outputs and the emitted loop
    // computes it once per lane group. Correctness here; the CSE mechanics are pinned in
    // VarkaLoopEmitterSuite and the win is priced in VarkaEmitterParityBenchmark.
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT date_add(d, 1) AS a, datediff(date_add(d, 1), d2) AS b " +
        "FROM varka_date_pairs ORDER BY a, b",
      expectFused = true)
  }

  test("a nested chain wraps int32 day arithmetic exactly like the row engine") {
    // The inner add leaves the representable date range and the outer sub wraps it back, so the
    // end-to-end result is decodable and the row engine is a valid oracle for the round trip -
    // unlike a one-way extreme offset (see the wrap-around block in the date_add test above).
    cacheDates(spark)
    cacheDates(varkaSpark)
    val off = Int.MaxValue - 1
    checkDifferential(spark, varkaSpark,
      s"SELECT date_sub(date_add(d, $off), $off) AS a FROM varka_dates ORDER BY a",
      expectFused = true)
  }

  test("a projection with a bare date column fuses and forwards the column zero-copy") {
    // Pinned as "stays unfused until task 12": a bare column output compiles to nothing on
    // purpose - emitting it would be a copy loop - and now forwards as the input's own vector
    // instead (the `eq` assertion lives in VarkaKernelEvaluatorSuite).
    cacheDates(spark)
    cacheDates(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT date_add(d, 3) AS a, d FROM varka_dates ORDER BY a",
      expectFused = true)
  }

  test("CASE WHEN over dates is fused and matches the row engine, three-valued nulls included") {
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT CASE WHEN d < d2 THEN date_add(d, 3) ELSE date_sub(d2, 1) END AS a " +
        "FROM varka_date_pairs ORDER BY a",
      expectFused = true)
    // Three branches, the first match winning; nulls in either column fall through.
    checkDifferential(spark, varkaSpark,
      "SELECT CASE WHEN d < d2 THEN d WHEN d = d2 THEN date_add(d, 1) " +
        "ELSE date_sub(d2, 2) END AS a FROM varka_date_pairs ORDER BY a",
      expectFused = true)
    // A CASE with no ELSE has a null-literal branch and must stay on the row engine.
    checkDifferential(spark, varkaSpark,
      "SELECT CASE WHEN d < d2 THEN date_add(d, 3) END AS a FROM varka_date_pairs ORDER BY a",
      expectFused = false)
  }

  test("IF with BETWEEN and date literals is fused and matches the row engine") {
    cacheDates(spark)
    cacheDates(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT IF(d BETWEEN DATE'2023-12-01' AND DATE'2024-01-01', date_add(d, 7), d) AS a " +
        "FROM varka_dates ORDER BY a",
      expectFused = true)
  }

  test("task 20: IN over date literals fuses to the cap and declines above it") {
    cacheDates(spark)
    cacheDates(varkaSpark)
    // Base 2023-12-27 with step 3 intersects the table (2023-12-27 itself and 2024-01-02),
    // so the fused EQ path is exercised on true lanes, not only on the all-miss ELSE side -
    // the review caught the original base (2023-12-25) never matching any row.
    def literals(n: Int): String = (0 until n).map { k =>
      s"DATE'${java.time.LocalDate.of(2023, 12, 27).plusDays(k * 3L)}'"
    }.mkString(", ")
    // 5 literals arrive as In, 16 as InSet (the optimizer's inSetConversionThreshold is 10);
    // both lists hit and miss real rows, and the null row's unknown condition falls to ELSE.
    checkDifferential(spark, varkaSpark,
      s"SELECT CASE WHEN d IN (${literals(5)}) THEN date_add(d, 1) ELSE d END AS a " +
        "FROM varka_dates ORDER BY a",
      expectFused = true)
    checkDifferential(spark, varkaSpark,
      s"SELECT CASE WHEN d IN (${literals(16)}) THEN date_add(d, 1) ELSE d END AS a " +
        "FROM varka_dates ORDER BY a",
      expectFused = true)
    // Duplicated literals collapse before the cap is counted, so a doubled list still fuses.
    checkDifferential(spark, varkaSpark,
      s"SELECT CASE WHEN d IN (${literals(5)}, ${literals(5)}) THEN d " +
        "ELSE date_add(d, 2) END AS a FROM varka_dates ORDER BY a",
      expectFused = true)
    // Above the cap the entry declines with a recorded reason and stays on the row engine.
    for (n <- Seq(17, 50)) {
      checkDifferential(spark, varkaSpark,
        s"SELECT CASE WHEN d IN (${literals(n)}) THEN date_add(d, 1) ELSE d END AS a " +
          "FROM varka_dates ORDER BY a",
        expectFused = false)
    }
    // A non-literal element makes the whole list ineligible - correct on the row engine.
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    checkDifferential(spark, varkaSpark,
      s"SELECT CASE WHEN d IN (d2, ${literals(3)}) THEN d ELSE date_add(d, 4) END AS a " +
        "FROM varka_date_pairs ORDER BY a",
      expectFused = false)
  }

  test("task 20: coalesce, nvl, ifnull and nvl2 fuse and match the row engine") {
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    for (q <- Seq(
      "SELECT coalesce(d, d2) AS a FROM varka_date_pairs ORDER BY a",
      "SELECT coalesce(d, d2, DATE'1999-09-09') AS a FROM varka_date_pairs ORDER BY a",
      "SELECT nvl(d, date_add(d2, 1)) AS a FROM varka_date_pairs ORDER BY a",
      "SELECT ifnull(d, d2) AS a FROM varka_date_pairs ORDER BY a",
      "SELECT nvl2(d, date_add(d2, 3), date_sub(d2, 3)) AS a " +
        "FROM varka_date_pairs ORDER BY a")) {
      checkDifferential(spark, varkaSpark, q, expectFused = true)
    }
    // A computed operand before the last cannot be guarded (the validity condition reads a
    // column's word) and declines - correct on the row engine.
    checkDifferential(spark, varkaSpark,
      "SELECT coalesce(date_add(d, 1), d2) AS a FROM varka_date_pairs ORDER BY a",
      expectFused = false)
  }

  test("task 20: coalesce over all-null and null-free inputs") {
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    for (session <- Seq(spark, varkaSpark)) {
      session.sql("SELECT d, d2 FROM varka_date_pairs WHERE d IS NOT NULL AND d2 IS NOT NULL")
        .createOrReplaceTempView("varka_null_free")
      session.catalog.cacheTable("varka_null_free")
      session.sql(
        "SELECT CAST(NULL AS DATE) AS d, CAST(NULL AS DATE) AS d2 FROM varka_date_pairs")
        .createOrReplaceTempView("varka_all_null")
      session.catalog.cacheTable("varka_all_null")
    }
    // All-null exercises the skipping contract: the all-null shortcut must not short-circuit
    // an IfElse over the validity condition, and coalesce(all-null, all-null) is all-null.
    checkDifferential(spark, varkaSpark,
      "SELECT coalesce(d, d2) AS a FROM varka_null_free ORDER BY a", expectFused = true)
    checkDifferential(spark, varkaSpark,
      "SELECT coalesce(d, d2) AS a FROM varka_all_null ORDER BY a", expectFused = true)
  }

  test("task 20: IS NULL and IS NOT NULL fuse as conditions, connectives included") {
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT CASE WHEN d IS NULL THEN d2 ELSE d END AS a FROM varka_date_pairs ORDER BY a",
      expectFused = true)
    checkDifferential(spark, varkaSpark,
      "SELECT CASE WHEN d IS NOT NULL AND d < d2 THEN date_add(d, 1) ELSE d2 END AS a " +
        "FROM varka_date_pairs ORDER BY a",
      expectFused = true)
    checkDifferential(spark, varkaSpark,
      "SELECT IF(d IS NULL OR d2 IS NULL, DATE'1970-01-01', greatest(d, d2)) AS a " +
        "FROM varka_date_pairs ORDER BY a",
      expectFused = true)
  }

  test("task 20: BETWEEN over a computed input fuses through the common-expression hoist") {
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    // A non-cheap BETWEEN input hoists into `_common_expr_0` in its own Project; the hoisted
    // arithmetic and the IF over its ref fuse as stacked Varka nodes.
    checkDifferential(spark, varkaSpark,
      "SELECT IF(date_add(d, 7) BETWEEN d2 AND date_add(d2, 40), d, date_sub(d2, 1)) AS a " +
        "FROM varka_date_pairs ORDER BY a",
      expectFused = true)
  }

  test("task 20: cast-wrapped date expressions fuse, folded or unwrapped before the kernel") {
    cacheDates(spark)
    cacheDates(varkaSpark)
    // The optimizer folds the literal cast and drops the identity cast (SimplifyCasts); the
    // compiler's own unwrap covers hand-built trees. Either layer, the query fuses.
    checkDifferential(spark, varkaSpark,
      "SELECT date_add(CAST(d AS DATE), 2) AS a, " +
        "IF(d < CAST('2024-01-02' AS DATE), d, date_sub(d, 1)) AS b " +
        "FROM varka_dates ORDER BY a",
      expectFused = true)
  }

  test("task 41: unix_date and date_from_unix_date fuse as a pure relabel") {
    cacheDates(spark)
    cacheDates(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT unix_date(d) AS u FROM varka_dates ORDER BY u",
      expectFused = true)
    // date_from_unix_date's child is an integer column, which no leaf can read until task 38
    // opens it - it declines through the ordinary non-date-column path and the projection has
    // nothing left to fuse, exactly like any other read of a bare int column today.
    checkDifferential(spark, varkaSpark,
      "SELECT date_from_unix_date(i) AS x FROM varka_dates",
      expectFused = false)
    // The actual argument for the task: a relabelled entry beside an ordinary one must not
    // demote the whole projection to Janino. Before this task the relabel became a residual
    // (per-row) entry rather than blocking `a` too - task 12's per-entry eligibility already
    // covered that - but it still cost a Janino re-evaluation of every row for `b` instead of
    // riding the same vectorized loop as `a`; see VarkaExpressionCompilerSuite for the
    // compiler-level proof that both entries now fuse rather than one falling to residual.
    checkDifferential(spark, varkaSpark,
      "SELECT date_add(d, 1) AS a, unix_date(d) AS b FROM varka_dates ORDER BY a",
      expectFused = true)
  }

  test("AND, OR and NOT conditions follow three-valued logic like the row engine") {
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT CASE WHEN NOT(d < d2) OR d = d2 THEN date_add(d, 1) ELSE d2 END AS a " +
        "FROM varka_date_pairs ORDER BY a",
      expectFused = true)
    checkDifferential(spark, varkaSpark,
      "SELECT CASE WHEN d <= d2 AND NOT(d = d2) THEN d ELSE date_sub(d2, 3) END AS a " +
        "FROM varka_date_pairs ORDER BY a",
      expectFused = true)
  }

  test("greatest and least skip nulls and fuse, nested chains included") {
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    checkDifferential(spark, varkaSpark,
      "SELECT greatest(d, d2) AS a, least(d, d2) AS b FROM varka_date_pairs ORDER BY a, b",
      expectFused = true)
    // The milestone's irreducible chain, plus a three-arg fold with a date literal.
    checkDifferential(spark, varkaSpark,
      "SELECT greatest(date_add(d, 7), d2) AS a, " +
        "least(d, d2, DATE'2024-01-15') AS b FROM varka_date_pairs ORDER BY a, b",
      expectFused = true)
  }

  test("the calendar extractions match the row engine across the Gregorian range") {
    // Every shape the decomposition could get wrong end to end: leap days of a 400-divisible
    // year (2000) and a 100-divisible one (1900), the century boundary itself, the era
    // boundary at 1600, month-length edges, the first and last dates SQL can write, and a
    // null. The March-based year the lowering works in turns at 1 March, so both sides of
    // that are here too.
    val rows = Seq("2024-01-01", "2024-02-29", "2024-03-01", "2024-12-31", "1969-12-31",
      "1970-01-01", "1900-02-28", "1900-03-01", "2000-02-29", "2000-03-01", "1600-02-29",
      "1600-03-01", "0001-01-01", "9999-12-31", "2025-07-04", null)
    Seq(spark, varkaSpark).foreach { session =>
      import scala.jdk.CollectionConverters._
      val schema = org.apache.spark.sql.types.StructType(Seq(
        org.apache.spark.sql.types.StructField("d", org.apache.spark.sql.types.DateType, true)))
      val data = rows.map(v =>
        org.apache.spark.sql.Row(if (v == null) null else java.sql.Date.valueOf(v)))
      session.createDataFrame(data.asJava, schema).createOrReplaceTempView("varka_cal")
      session.catalog.cacheTable("varka_cal")
    }
    try {
      checkDifferential(spark, varkaSpark,
        "SELECT year(d) AS a, month(d) AS b, dayofmonth(d) AS c, quarter(d) AS e, " +
          "year(date_add(d, 1)) AS f FROM varka_cal ORDER BY a, b, c, e, f",
        expectFused = true)
      // EXTRACT desugars to the same nodes, so it must fuse the same way.
      checkDifferential(spark, varkaSpark,
        "SELECT EXTRACT(YEAR FROM d) AS a, EXTRACT(QUARTER FROM d) AS b " +
          "FROM varka_cal ORDER BY a, b",
        expectFused = true)
      // The TPC-H q7/q8/q9 shape: year(date) beside a filter on the same column.
      checkDifferential(spark, varkaSpark,
        "SELECT year(d) AS a FROM varka_cal WHERE d >= DATE '1900-01-01' ORDER BY a",
        expectFused = true)
    } finally {
      Seq(spark, varkaSpark).foreach(_.catalog.uncacheTable("varka_cal"))
    }
  }

  test("dayofweek and weekday match the row engine across 1970 and nulls") {
    val rows = Seq("2024-01-01", "1969-12-31", "1969-01-05", "1900-02-28", "2100-07-04", null)
    Seq(spark, varkaSpark).foreach { session =>
      import scala.jdk.CollectionConverters._
      val schema = org.apache.spark.sql.types.StructType(Seq(
        org.apache.spark.sql.types.StructField("d", org.apache.spark.sql.types.DateType, true)))
      val data = rows.map(v =>
        org.apache.spark.sql.Row(if (v == null) null else java.sql.Date.valueOf(v)))
      session.createDataFrame(data.asJava, schema).createOrReplaceTempView("varka_dow")
      session.catalog.cacheTable("varka_dow")
    }
    try {
      checkDifferential(spark, varkaSpark,
        "SELECT dayofweek(d) AS a, weekday(d) AS b, dayofweek(date_add(d, 1)) AS c " +
          "FROM varka_dow ORDER BY a, b, c",
        expectFused = true)
    } finally {
      Seq(spark, varkaSpark).foreach(_.catalog.uncacheTable("varka_dow"))
    }
  }

  test("next_day matches the row engine across 1970, pre-1970 and nulls") {
    val rows = Seq("2024-01-01", "1969-12-31", "1970-01-01", "1900-02-28", "2100-07-04", null)
    Seq(spark, varkaSpark).foreach { session =>
      import scala.jdk.CollectionConverters._
      val schema = org.apache.spark.sql.types.StructType(Seq(
        org.apache.spark.sql.types.StructField("d", org.apache.spark.sql.types.DateType, true)))
      val data = rows.map(v =>
        org.apache.spark.sql.Row(if (v == null) null else java.sql.Date.valueOf(v)))
      session.createDataFrame(data.asJava, schema).createOrReplaceTempView("varka_next_day")
      session.catalog.cacheTable("varka_next_day")
    }
    try {
      // THURSDAY maps to k = -1 (DateTimeUtils.getDayOfWeekFromString's [0, 6] range has
      // THURSDAY = 0), the one weekday whose runtime literal is negative - included so the
      // end-to-end path, not only the emitter unit test, covers it.
      checkDifferential(spark, varkaSpark,
        "SELECT next_day(d, 'MO') AS a, next_day(d, 'SUNDAY') AS b, " +
          "next_day(d, 'THURSDAY') AS c FROM varka_next_day ORDER BY a, b, c",
        expectFused = true)
    } finally {
      Seq(spark, varkaSpark).foreach(_.catalog.uncacheTable("varka_next_day"))
    }
  }

  test("the rule fires and the kernels run under AQE") {
    // Every Varka session disables AQE for plan determinism, so this pins the default-config
    // path. With AQE on the fused node sits inside a query stage, which a plain
    // SparkPlan.collect never descends into: the shared assertions are stage-aware since
    // task 17, and these two tests are what keeps them that way.
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    varkaSpark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
    try {
      val query = "SELECT CASE WHEN d < d2 THEN date_add(d, 3) ELSE d2 END AS a " +
        "FROM varka_date_pairs ORDER BY a"
      val expected = spark.sql(query)
      val actual = varkaSpark.sql(query)
      checkAnswer(actual, expected)
      val plan = actual.queryExecution.executedPlan
      assertFused(plan)
      assertKernelsRan(plan)
    } finally {
      varkaSpark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    }
  }

  test("the rule fires and the kernels run under AQE on a mixed projection") {
    cacheDates(spark)
    cacheDates(varkaSpark)
    varkaSpark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
    try {
      val query = "SELECT date_add(d, 3) AS a, i, i + 1 AS inc FROM varka_dates ORDER BY a"
      val expected = spark.sql(query)
      val actual = varkaSpark.sql(query)
      checkAnswer(actual, expected)
      val plan = actual.queryExecution.executedPlan
      assertFused(plan)
      assertKernelsRan(plan)
    } finally {
      varkaSpark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    }
  }

  test("a calendar node inside a fused predicate is computed like any other") {
    // compileCond's compare() puts no type gate on its operands, so a calendar node reaches a
    // filter's mask kernel as readily as a projection's, which was not exercised until this
    // test.
    //
    // The shape has to be calendar-against-calendar. `year(d) = 2020` does NOT fuse: the
    // literal is an IntegerType one and the compiler's literal arm accepts DateType only, so
    // the whole predicate stays on the row path. Comparing two calendar nodes needs no
    // literal, and that is what reaches the mask kernel.
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    try {
      val fused = checkDifferential(spark, varkaSpark,
        "SELECT count(*) AS c FROM varka_date_pairs WHERE year(d) = year(d2)",
        expectFused = true)
      // The predicate is in the kernel, not left above it as a row-level Filter.
      assert(!fused.toString.contains("Filter (year("),
        s"the calendar predicate should be fused, not residual:\n$fused")
      // Two calendar nodes per side, so the mask kernel carries roughly two hundred ops.
      checkDifferential(spark, varkaSpark,
        "SELECT count(*) AS c FROM varka_date_pairs " +
          "WHERE year(d) = year(d2) AND month(d) >= month(d2)",
        expectFused = true)
      // A calendar predicate under a calendar projection over the same columns, which is
      // where the filter's compaction and the projection's kernels meet.
      checkDifferential(spark, varkaSpark,
        "SELECT year(d) AS y FROM varka_date_pairs WHERE month(d) = month(d2) ORDER BY y",
        expectFused = true)
    } finally {
      Seq(spark, varkaSpark).foreach(_.catalog.uncacheTable("varka_date_pairs"))
    }
  }

  // Task 51 removed the calendar range guard these two tests exercised end to end (a date
  // pushed past VarkaChrono.NARROW_MIN_DAYS..NARROW_MAX_DAYS by date_add used to decline the
  // batch to the row engine; the "guard on the filter side" and "falls back rather than
  // answering wrongly" assertions checked exactly that). Removed rather than rewritten to
  // assert the new, weaker behavior: PLAN_TASK_51.md section 4 records the accepted regression
  // window, and PLAN_TASK_52.md tracks the producer-side guard that will need an equivalent
  // differential test once it lands, shaped around the node that manufactures the out-of-range
  // day rather than the calendar extraction that reads it.

  test("a declined batch falls back with the row engine's answers, counted as its own cause") {
    // Task 26: a partial lowering (the narrowed civil-from-days one) reports a batch it cannot
    // compute, and the evaluator recomputes it row by row. The sibling tests above reach that
    // path with real out-of-range dates and no hook; this one uses the hook to make a
    // whole-query fallback cheap to assert without depending on any expression's range. What
    // it proves is the routing - that a declined batch answers correctly, and lands under its
    // own metric rather than the ghost fallback's, which is a defect count and must stay
    // clean.
    cacheDates(spark)
    cacheDates(varkaSpark)
    VarkaColumnarToRowExec.setDeclineKernelForTesting(true)
    try {
      val q = "SELECT year(d) AS a, date_add(d, 3) AS b FROM varka_dates ORDER BY a, b"
      val expected = spark.sql(q)
      val actual = varkaSpark.sql(q)
      val plan = actual.queryExecution.executedPlan
      assertFused(plan)
      checkAnswer(actual, expected)
      def metric(name: String): Long = plan.collectFirst { case v: VarkaColumnarToRowExec => v }
        .flatMap(_.metrics.get(name)).map(_.value).getOrElse(0L)
      assert(metric("numVarkaBatches") === 0L, "no batch should have been served by the kernel")
      assert(metric("numFallbackBatchesDeclined") > 0L, "the declined metric should have fired")
      assert(metric("numFallbackBatchesKernel") === 0L,
        "a declined batch is not a kernel failure and must not be counted as one")
    } finally {
      VarkaColumnarToRowExec.setDeclineKernelForTesting(false)
    }
  }

  test("a kernel failure on a mixed projection falls back whole-batch with correct results") {
    cacheDates(spark)
    cacheDates(varkaSpark)
    VarkaColumnarToRowExec.setFailKernelForTesting(true)
    try {
      val q = "SELECT date_add(d, 3) AS a, i, i + 1 AS inc FROM varka_dates ORDER BY a"
      val expected = spark.sql(q)
      val actual = varkaSpark.sql(q)
      val plan = actual.queryExecution.executedPlan
      assertFused(plan)
      checkAnswer(actual, expected)
      val batches = plan.collectFirst { case v: VarkaColumnarToRowExec => v }
        .flatMap(_.metrics.get("numVarkaBatches")).map(_.value).getOrElse(0L)
      assert(batches === 0L, s"expected the fallback to serve every batch, got $batches")
    } finally {
      VarkaColumnarToRowExec.setFailKernelForTesting(false)
    }
  }

  test("a kernel failure on a predicated plan falls back per batch with correct results") {
    cacheDatePairs(spark)
    cacheDatePairs(varkaSpark)
    VarkaColumnarToRowExec.setFailKernelForTesting(true)
    try {
      val q = "SELECT CASE WHEN d < d2 THEN date_add(d, 1) ELSE d2 END AS a " +
        "FROM varka_date_pairs ORDER BY a"
      val expected = spark.sql(q)
      val actual = varkaSpark.sql(q)
      val plan = actual.queryExecution.executedPlan
      assertFused(plan)
      checkAnswer(actual, expected)
      val batches = plan.collectFirst { case v: VarkaColumnarToRowExec => v }
        .flatMap(_.metrics.get("numVarkaBatches")).map(_.value).getOrElse(0L)
      assert(batches === 0L, s"expected the fallback to serve every batch, got $batches")
    } finally {
      VarkaColumnarToRowExec.setFailKernelForTesting(false)
    }
  }

  test("a kernel failure on a nested plan falls back per batch with correct results") {
    cacheDates(spark)
    cacheDates(varkaSpark)
    VarkaColumnarToRowExec.setFailKernelForTesting(true)
    try {
      val query = "SELECT datediff(date_add(d, 1), d) AS a FROM varka_dates ORDER BY a"
      val expected = spark.sql(query)
      val actual = varkaSpark.sql(query)
      val plan = actual.queryExecution.executedPlan
      assertFused(plan)
      checkAnswer(actual, expected)
      val batches = plan.collectFirst { case v: VarkaColumnarToRowExec => v }
        .flatMap(_.metrics.get("numVarkaBatches")).map(_.value).getOrElse(0L)
      assert(batches === 0L, s"expected the fallback to serve every batch, got $batches")
    } finally {
      VarkaColumnarToRowExec.setFailKernelForTesting(false)
    }
  }

  test("filters and aggregation match the row engine") {
    cacheDatesBig(spark, 1024)
    cacheDatesBig(varkaSpark, 1024)
    // Until task 21 the WHERE below pinned expectFused = false - a filter blocked fusion
    // outright. It now fuses: the filter runs the mask kernel and the projection stacks on
    // the compacted batches.
    checkDifferential(spark, varkaSpark,
      "SELECT date_add(d, 7) AS a FROM varka_dates_big WHERE d IS NOT NULL ORDER BY a",
      expectFused = true)
    checkDifferential(spark, varkaSpark,
      "SELECT max(date_add(d, 1)) AS m, count(*) AS c FROM varka_dates_big",
      expectFused = false)
    checkDifferential(spark, varkaSpark,
      "SELECT d, count(*) AS c FROM varka_dates_big GROUP BY d ORDER BY d",
      expectFused = false)
  }

  test("task 21: the survey's filter shapes match the row engine, warm cache included") {
    cacheDatesBig(spark, 2048)
    cacheDatesBig(varkaSpark, 2048)
    // BETWEEN - the survey's dominant date predicate; the optimizer hands it over as paired
    // comparisons on the AND spine. A bare-column output keeps the plan on the row-boundary
    // filter node (no compaction).
    checkDifferential(spark, varkaSpark,
      "SELECT d FROM varka_dates_big " +
        "WHERE d BETWEEN DATE'2020-02-01' AND DATE'2020-06-01' ORDER BY d",
      expectFused = true)
    // The dominant end-to-end shape: WHERE plus an aggregate.
    checkDifferential(spark, varkaSpark,
      "SELECT count(*) AS c FROM varka_dates_big " +
        "WHERE d BETWEEN DATE'2020-02-01' AND DATE'2020-06-01'",
      expectFused = true)
    // IN - the task-20 lowering, now at a filter root (the benchmark's anchor shape).
    checkDifferential(spark, varkaSpark,
      "SELECT count(*) AS c FROM varka_dates_big " +
        "WHERE d IN (DATE'2020-01-02', DATE'2020-03-04', DATE'2020-11-30')",
      expectFused = true)
    // A projection stacked on the filter: the compacted batch must keep the Arrow
    // invariant, so the projection's kernels run over it rather than falling back.
    val stacked = checkDifferential(spark, varkaSpark,
      "SELECT date_add(d, 7) AS a FROM varka_dates_big " +
        "WHERE d < DATE'2020-06-01' ORDER BY a",
      expectFused = true)
    val filterNode = stacked.collectFirst { case f: VarkaFilterExec => f }
    assert(filterNode.isDefined, s"expected a compacting filter node:\n${stacked.treeString}")
    assert(filterNode.get.metrics("numVarkaBatches").value > 0L)
    assert(filterNode.get.metrics("numFallbackBatchesNonArrow").value === 0L)
  }

  test("task 21: null-as-false, and the boundary selectivities") {
    cacheDatesBig(spark, 1024)
    cacheDatesBig(varkaSpark, 1024)
    // The null rows (one in 17) must be dropped by every compilable predicate - SQL's WHERE
    // treats a null predicate as false, and the mask root's rule is exactly that.
    checkDifferential(spark, varkaSpark,
      "SELECT d FROM varka_dates_big WHERE d >= DATE'1900-01-01' ORDER BY d",
      expectFused = true)
    // None selected - through the kernel. The predicate's interval contains no whole day, so
    // every row fails it, but each conjunct is satisfiable against the cache's min/max stats:
    // a range predicate entirely outside the data (d < 1900) would have the in-memory scan's
    // stat pruning drop every batch before the filter node ever saw one, and there would be
    // nothing to assert kernels ran on.
    checkDifferential(spark, varkaSpark,
      "SELECT d FROM varka_dates_big " +
        "WHERE d > DATE'2020-01-05' AND d < DATE'2020-01-06' ORDER BY d",
      expectFused = true)
    // All selected, null rows included: IS NULL OR IS NOT NULL is total and known
    // everywhere, so every row of the batch survives.
    checkDifferential(spark, varkaSpark,
      "SELECT i FROM varka_dates_big WHERE d IS NULL OR d IS NOT NULL ORDER BY i",
      expectFused = true)
    // IS NULL selects exactly the null rows - the known-false mask read through NOT.
    checkDifferential(spark, varkaSpark,
      "SELECT i FROM varka_dates_big WHERE d IS NULL ORDER BY i",
      expectFused = true)
  }

  test("task 21: a mixed predicate splits - the residual conjunct stays above, correct") {
    cacheDatesBig(spark, 1024)
    cacheDatesBig(varkaSpark, 1024)
    val plan = checkDifferential(spark, varkaSpark,
      "SELECT i FROM varka_dates_big WHERE d < DATE'2020-06-01' AND i % 3 = 0 ORDER BY i",
      expectFused = true)
    // The int conjunct cannot fuse: it must survive as a row FilterExec above the Varka
    // node, seeing only the rows the mask kernel let through.
    assert(collectFirst(plan) { case f: FilterExec => f }.isDefined,
      s"expected the residual conjunct's row filter in the plan:\n${plan.treeString}")
  }

  test("task 21 review: the driver-side residual count reaches the SQL UI store") {
    // The listener aggregates task-end updates and posted driver updates only: a driver-side
    // `+=` that is not posted is visible to plan.metrics (what the exec suites read) but
    // never to the UI. This goes through a real tracked execution on the Arrow-backed varka
    // session - the default cache serializer has no columnar output for DateType at all, so
    // only this harness can plan the Varka node - and asserts on the status store, the same
    // surface the SQL tab renders.
    cacheDates(varkaSpark)
    // One fused entry keeps the projection eligible; the int arithmetic is residual.
    varkaSpark.sql("SELECT date_add(d, 1) AS a, i + 1 AS b FROM varka_dates").collect()
    varkaSpark.sparkContext.listenerBus.waitUntilEmpty()
    val statusStore = varkaSpark.sharedState.statusStore
    val executionId = statusStore.executionsList().reverse
      .find(_.physicalPlanDescription.contains("VarkaColumnarToRow"))
      .map(_.executionId)
      .getOrElse(fail("no tracked execution with a Varka node found"))
    val metricId = statusStore.execution(executionId).get.metrics
      .find(_.name.contains("residual")).map(_.accumulatorId)
      .getOrElse(fail("the residual metric is not registered on the execution"))
    val posted = statusStore.executionMetrics(executionId)
    assert(posted.get(metricId).exists(_.contains("1")),
      s"expected the posted residual count in the store, got: $posted")
  }

  test("task 21: caching a view over fused Varka work keeps the work") {
    // The cache builder strips a topmost columnar-to-row transition to reach the columnar plan
    // underneath - sound for the stock transition, silently wrong for the fused Varka nodes,
    // which carry a projection or filter inside it. The Arrow serializer converts them to
    // their columnar siblings instead; this pins it, because the failure mode is vicious:
    // every direct query is right and only a CACHED view materializes the dropped work.
    cacheDatesBig(spark, 256)
    cacheDatesBig(varkaSpark, 256)
    for (session <- Seq(spark, varkaSpark)) {
      session.sql("SELECT date_add(d, 5) AS a FROM varka_dates_big WHERE d IS NOT NULL")
        .createOrReplaceTempView("varka_cached_fused")
      session.catalog.cacheTable("varka_cached_fused")
    }
    // The query over the cache has no Varka work of its own; what it checks is the cache's
    // content - built through the converted VarkaProjectExec-over-VarkaFilterExec plan on
    // the varka session, and through the row engine on the baseline.
    checkDifferential(spark, varkaSpark,
      "SELECT a FROM varka_cached_fused ORDER BY a", expectFused = false)
    for (session <- Seq(spark, varkaSpark)) {
      session.catalog.uncacheTable("varka_cached_fused")
    }
  }

  test("task 21 review: a nondeterministic conjunct keeps the whole filter unfused") {
    // The conjunct split would hoist the date predicate below rand(), changing which rows
    // the seeded stream sees; the compiler declines the whole predicate instead. Plan-shape
    // assertion only: an always-true rand comparison gets optimized away entirely (leaving a
    // deterministic filter that legitimately fuses - the first version of this test learned
    // that), and a live rand makes answers uncomparable; the reorder semantics themselves
    // are pinned in the compiler suite.
    cacheDatesBig(varkaSpark, 256)
    val plan = varkaSpark.sql(
      "SELECT i FROM varka_dates_big WHERE rand(42) < 0.5 AND d IS NOT NULL ORDER BY i")
      .queryExecution.executedPlan
    assertNotFused(plan)
  }

  test("task 21: filters over multiple batches and tasks share one mask kernel class") {
    val batchSize = "32"
    try {
      spark.conf.set(SQLConf.COLUMN_BATCH_SIZE.key, batchSize)
      varkaSpark.conf.set(SQLConf.COLUMN_BATCH_SIZE.key, batchSize)
      cacheDatesBig(spark, 1024, parts = 4)
      cacheDatesBig(varkaSpark, 1024, parts = 4)
      val plan = checkDifferential(spark, varkaSpark,
        "SELECT count(*) AS c FROM varka_dates_big WHERE d < DATE'2020-06-01'",
        expectFused = true)
      val batches = plan.collectFirst { case v if isVarkaNode(v) => v }
        .flatMap(_.metrics.get("numVarkaBatches")).map(_.value).getOrElse(0L)
      assert(batches > 1L, s"expected more than one kernel batch, got $batches")
    } finally {
      spark.conf.unset(SQLConf.COLUMN_BATCH_SIZE.key)
      varkaSpark.conf.unset(SQLConf.COLUMN_BATCH_SIZE.key)
    }
  }

  test("multi-batch: every cached Arrow batch is processed by the kernels") {
    val batchSize = "32"
    try {
      spark.conf.set(SQLConf.COLUMN_BATCH_SIZE.key, batchSize)
      varkaSpark.conf.set(SQLConf.COLUMN_BATCH_SIZE.key, batchSize)
      cacheDatesBig(spark, 1024)
      cacheDatesBig(varkaSpark, 1024)
      val plan = checkDifferential(spark, varkaSpark,
        "SELECT date_add(d, 1) AS a FROM varka_dates_big ORDER BY a",
        expectFused = true)
      val batches = plan.collectFirst { case v: VarkaColumnarToRowExec => v }
        .flatMap(_.metrics.get("numVarkaBatches")).map(_.value).getOrElse(0L)
      assert(batches > 1L, s"expected more than one kernel batch, got $batches")
    } finally {
      spark.conf.unset(SQLConf.COLUMN_BATCH_SIZE.key)
      varkaSpark.conf.unset(SQLConf.COLUMN_BATCH_SIZE.key)
    }
  }

  test("multi-task: tasks sharing one cached kernel class produce correct results") {
    cacheDatesBig(spark, 1024, parts = 4)
    cacheDatesBig(varkaSpark, 1024, parts = 4)
    checkDifferential(spark, varkaSpark,
      "SELECT date_add(d, 1) AS a FROM varka_dates_big ORDER BY a",
      expectFused = true)
  }

  test("a non-Arrow columnar source never runs the kernels and matches the row engine") {
    withTempPath { dir =>
      val rows = Seq((date("2024-01-01"), 0), (date("2023-12-01"), 1), (null, 2))
      spark.createDataFrame(rows).toDF("d", "i").write.parquet(dir.getCanonicalPath)
      val expected = spark.read.parquet(dir.getCanonicalPath).selectExpr("date_add(d, 3) AS a")
      val actual = varkaSpark.read.parquet(dir.getCanonicalPath).selectExpr("date_add(d, 3) AS a")
      val plan = actual.queryExecution.executedPlan
      checkAnswer(actual, expected)
      plan.collectFirst { case v: VarkaColumnarToRowExec => v }.foreach { v =>
        val batches = v.metrics.get("numVarkaBatches").map(_.value).getOrElse(0L)
        assert(batches === 0L, s"expected no kernel batches on a non-Arrow source, got $batches")
      }
    }
  }

  test("many distinct-literal Varka tasks are one cached shape, Metaspace bounded") {
    cacheDates(spark)
    cacheDates(varkaSpark)
    val before = metaspaceUsed()
    val missesBefore = VarkaShapeCache.missCount
    val hitsBefore = VarkaShapeCache.hitCount
    (0 until 100).foreach { i =>
      checkDifferential(spark, varkaSpark,
        s"SELECT date_add(d, $i) AS a FROM varka_dates ORDER BY a", expectFused = true)
    }
    // Task 18 inverted what this test proves. The hundred queries differ only in their
    // literal, which never enters the shape key - they are one shape, so at most one task
    // emitted a class (zero if an earlier test already cached the shape) and the rest hit the
    // JVM-wide cache. The deterministic eviction guarantee lives in VarkaShapeCacheSuite.
    assert(VarkaShapeCache.missCount - missesBefore <= 1,
      s"expected at most one emission for one shape, got ${VarkaShapeCache.missCount} misses")
    assert(VarkaShapeCache.hitCount - hitsBefore >= 100,
      "the repeated shape must be served from the cache")
    System.gc()
    System.runFinalization()
    System.gc()
    val delta = metaspaceUsed() - before
    // Lenient: one cached kernel class (a few KB) must stay far below this bound.
    assert(delta < 64L * 1024 * 1024, s"Metaspace grew by $delta bytes across 100 Varka tasks")
  }

  test("task 18: near-miss shapes back to back in the warm cache stay distinct") {
    cacheDates(spark)
    cacheDates(varkaSpark)
    // Same operand structure, different op kind: date_add vs date_sub must not share a class.
    checkDifferential(spark, varkaSpark,
      "SELECT date_add(d, 5) AS a FROM varka_dates ORDER BY a", expectFused = true)
    checkDifferential(spark, varkaSpark,
      "SELECT date_sub(d, 5) AS a FROM varka_dates ORDER BY a", expectFused = true)
    // Same shape, different constant: shares the class and must still answer with its own
    // literal, which travels as a runtime argument rather than in the bytes.
    checkDifferential(spark, varkaSpark,
      "SELECT date_add(d, 30) AS a FROM varka_dates ORDER BY a", expectFused = true)
    // Same structure with one more literal slot (two distinct offsets): a different shape,
    // because the slot count changes the emitted bytecode independently of the IR.
    checkDifferential(spark, varkaSpark,
      "SELECT date_add(d, 5) AS a, date_add(d, 6) AS b FROM varka_dates ORDER BY a",
      expectFused = true)
  }

}
