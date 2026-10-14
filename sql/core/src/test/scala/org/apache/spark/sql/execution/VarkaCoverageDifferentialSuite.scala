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
import scala.util.{Failure, Success, Try}

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.{VarkaMatrix, VarkaTestWatchdog}
import org.apache.spark.sql.catalyst.optimizer.{BooleanSimplification, InferFiltersFromConstraints, NullDownPropagation}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DateType, DayTimeIntervalType, IntegerType, LongType, TimeType, YearMonthIntervalType}

/**
 * Every row of the published coverage table, run through both engines (VARKA-120).
 *
 * `VarkaCoverageSuite` proves each row of `sql/varka/coverage.json` compiles to a fused kernel
 * and that the table names every expression the compiler admits. This suite proves the kernel
 * behind each row answers what the row engine answers. It reads the committed JSON and nothing
 * else, so a row added to the coverage suite and regenerated into the file is run here without
 * a change to this file: adding an arm without a row fails the coverage suite, adding a row
 * without correctness fails this one, and the two together are the guarantee the table makes.
 *
 * Each row runs on three fixtures over the table's twelve columns - nulls sprinkled
 * independently per column, no nulls, and every column null in every row - and under both
 * consumers a Varka node can have: rows, the plan every direct query gets, and columns, which
 * the Arrow cache builder takes when a view over the query is cached. The hand-chosen shapes in
 * `VarkaDifferentialSuite` (extreme offsets, overflow, cache conversion) stay there; this is the
 * floor under them.
 *
 * Two stronger forms run over the same rows (VARKA-138):
 *
 *  - **The partition oracle.** A predicate `p` splits a table into the rows where it is true,
 *    false and unknown. Each predicate row is run as `WHERE p` and `WHERE NOT (p)` on Varka,
 *    on every fixture, under the default optimizer and with the three rules that rewrite those
 *    filters before Varka sees them excluded; the two must share no row, and the rows in
 *    neither must be exactly Spark's `WHERE (p) IS NULL`. Comparing `WHERE p` alone, as the
 *    test per row does, cannot see a kernel that answers false where it should answer unknown.
 *  - **The ANSI form.** A fourth fixture holds the types' extremes, and each row is run where
 *    Spark evaluates it and where Spark skips it (an untaken CASE arm, a later COALESCE
 *    argument, the right side of a false AND, of a true OR). Both engines must raise the same
 *    error - condition, SQLSTATE and message parameters, what `checkError` compares - or both
 *    answer the same rows: Varka must neither raise where Spark skips nor miss Spark's raise.
 */
class VarkaCoverageDifferentialSuite extends QueryTest with VarkaSharedSessions
  with VarkaTestWatchdog {

  /** One row of the table: what it prints, what runs, and which form it takes. */
  private case class CoverageRow(sql: String, executable: String, family: String, form: String)

  private lazy val rows: Seq[CoverageRow] = {
    val file = getWorkspaceFilePath("sql", "varka", "coverage.json").toFile
    val doc = new ObjectMapper().readTree(file)
    doc.get("expressions").elements().asScala.toSeq.map { e =>
      CoverageRow(e.get("sql").asText(), e.get("executable").asText(),
        e.get("family").asText(), e.get("form").asText())
    }
  }

  /**
   * The three fixtures, each a view with the table's columns: `d`, `d2` (dates), `i` (int),
   * `ymm`, `ymy`, `ym` (the three year-month interval units), and the long lane's `l`, `l2`
   * (bigint, across its range), `t`, `t2` (`TIME(6)`) and `dt`, `dt2` (day-time intervals of
   * both signs) - built through SQL because an interval column has no plain Scala literal. The
   * dates are the ones the other fixtures use
   * - leap days, a 31st, 1969-12-31, the dates the table's `IN` rows name, 2021 dates for its
   * `year(d) = 2021` rows, a far date inside the contract - `d2` is shifted against `d` or
   * equal to it, `i` stays in 1..12 (inside the day-offset guard, and a valid month for
   * `make_date(2021, i, 1)`), and the month counts stay inside the emitter's guard.
   */
  private val fixtures: Seq[(String, String)] = {
    // `i` stays in 1..12: it doubles as the month of `make_date(2021, i, 1)`, and under the
    // session's default ANSI mode an invalid month is an error on the row engine rather than
    // a null, which is `VarkaDifferentialSuite`'s `varka_date_parts` business, not a shape in
    // the table.
    val dense = Seq(
      "(DATE'2024-01-31', DATE'2024-02-29',  3,   3,  1,  2, CAST('5000000000' AS BIGINT), " +
        "CAST('1' AS BIGINT), TIME'12:34:56.789', TIME'00:00:00', " +
        "INTERVAL '1 02:03:04.5' DAY TO SECOND, INTERVAL '0 00:00:00' DAY TO SECOND)",
      "(DATE'2024-02-29', DATE'2024-02-29',  1, -14, -2,  0, CAST('-5000000000' AS BIGINT), " +
        "CAST('-5000000000' AS BIGINT), TIME'00:00:00', TIME'23:59:59.999999', " +
        "INTERVAL '-3 00:00:00.000001' DAY TO SECOND, INTERVAL '0 00:00:01' DAY TO SECOND)",
      "(DATE'2023-12-27', DATE'2024-01-02', 12,  24,  3,  1, " +
        "CAST('9223372036854775807' AS BIGINT), CAST('-9223372036854775808' AS BIGINT), " +
        "TIME'23:59:59.999999', TIME'12:34:56.789', INTERVAL '100000 00:00:00' DAY TO SECOND, " +
        "INTERVAL '-100000 00:00:00' DAY TO SECOND)",
      "(DATE'2021-01-01', DATE'2020-12-31',  4,   0,  0, 11, CAST('0' AS BIGINT), " +
        "CAST('0' AS BIGINT), TIME'06:00:00', TIME'06:00:00', " +
        "INTERVAL '0 00:00:00' DAY TO SECOND, INTERVAL '0 00:00:00' DAY TO SECOND)",
      "(DATE'2021-06-01', DATE'2021-06-01',  6,   7,  2,  3, CAST('2147483648' AS BIGINT), " +
        "CAST('2147483647' AS BIGINT), TIME'12:00:00', TIME'12:00:00.000001', " +
        "INTERVAL '7 12:00:00' DAY TO SECOND, INTERVAL '7 12:00:00' DAY TO SECOND)",
      "(DATE'2021-03-15', DATE'2021-03-14', 11,   1,  1,  5, CAST('-1' AS BIGINT), " +
        "CAST('1' AS BIGINT), TIME'01:02:03.456', TIME'01:02:03.457', " +
        "INTERVAL '-0 00:00:00.5' DAY TO SECOND, INTERVAL '0 00:00:00.5' DAY TO SECOND)",
      "(DATE'1969-12-31', DATE'1970-01-01',  5,  -1,  0,  0, CAST('42' AS BIGINT), " +
        "CAST('-42' AS BIGINT), TIME'18:00:00', TIME'06:00:00', " +
        "INTERVAL '1 00:00:00' DAY TO SECOND, INTERVAL '-1 00:00:00' DAY TO SECOND)",
      "(DATE'2000-02-29', DATE'1999-12-31',  2,  12,  4,  6, CAST('-2147483649' AS BIGINT), " +
        "CAST('-2147483648' AS BIGINT), TIME'09:30:00', TIME'09:29:59.999', " +
        "INTERVAL '0 00:00:00.000001' DAY TO SECOND, INTERVAL '0 00:00:00' DAY TO SECOND)",
      "(DATE'9999-12-01', DATE'0001-01-15',  7,   5,  1,  1, CAST('12345678901234' AS BIGINT), " +
        "CAST('12345678901234' AS BIGINT), TIME'12:34:56.789', TIME'12:34:56.789', " +
        "INTERVAL '2 00:00:00' DAY TO SECOND, INTERVAL '2 00:00:00' DAY TO SECOND)",
      "(DATE'2021-11-01', DATE'2021-11-01',  9, 100,  2,  4, CAST('7' AS BIGINT), " +
        "CAST('11' AS BIGINT), TIME'00:00:00.000001', TIME'00:00:00', " +
        "INTERVAL '0 00:01:00' DAY TO SECOND, INTERVAL '0 00:00:59' DAY TO SECOND)")
    val allNullRow =
      "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
    val mixed = Seq(
      "(DATE'2024-01-31', DATE'2024-02-29',  3,   3,  1,  2, CAST('5000000000' AS BIGINT), " +
        "CAST('1' AS BIGINT), TIME'12:34:56.789', TIME'00:00:00', " +
        "INTERVAL '1 02:03:04.5' DAY TO SECOND, INTERVAL '0 00:00:00' DAY TO SECOND)",
      "(NULL,             DATE'2024-02-29',  1, -14, -2,  0, NULL, " +
        "CAST('-5000000000' AS BIGINT), TIME'00:00:00', TIME'23:59:59.999999', " +
        "INTERVAL '-3 00:00:00.000001' DAY TO SECOND, INTERVAL '0 00:00:01' DAY TO SECOND)",
      "(DATE'2023-12-27', NULL,             12,  24,  3,  1, " +
        "CAST('9223372036854775807' AS BIGINT), CAST('-9223372036854775808' AS BIGINT), " +
        "TIME'23:59:59.999999', TIME'12:34:56.789', INTERVAL '100000 00:00:00' DAY TO SECOND, " +
        "INTERVAL '-100000 00:00:00' DAY TO SECOND)",
      "(DATE'2021-01-01', DATE'2020-12-31', NULL, 0,  0, 11, CAST('0' AS BIGINT), " +
        "CAST('0' AS BIGINT), NULL, TIME'06:00:00', INTERVAL '0 00:00:00' DAY TO SECOND, " +
        "INTERVAL '0 00:00:00' DAY TO SECOND)",
      "(DATE'2021-06-01', DATE'2021-06-01',  6, NULL, 2,  3, CAST('2147483648' AS BIGINT), " +
        "CAST('2147483647' AS BIGINT), TIME'12:00:00', TIME'12:00:00.000001', " +
        "INTERVAL '7 12:00:00' DAY TO SECOND, INTERVAL '7 12:00:00' DAY TO SECOND)",
      "(DATE'2021-03-15', DATE'2021-03-14', 11,   1, NULL, 5, CAST('-1' AS BIGINT), " +
        "CAST('1' AS BIGINT), TIME'01:02:03.456', TIME'01:02:03.457', NULL, " +
        "INTERVAL '0 00:00:00.5' DAY TO SECOND)",
      "(DATE'1969-12-31', DATE'1970-01-01',  5,  -1,  0, NULL, CAST('42' AS BIGINT), " +
        "CAST('-42' AS BIGINT), TIME'18:00:00', TIME'06:00:00', " +
        "INTERVAL '1 00:00:00' DAY TO SECOND, INTERVAL '-1 00:00:00' DAY TO SECOND)",
      allNullRow,
      "(DATE'9999-12-01', DATE'0001-01-15',  7,   5,  1,  1, CAST('-2147483649' AS BIGINT), " +
        "NULL, TIME'09:30:00', TIME'09:29:59.999', INTERVAL '0 00:00:00.000001' DAY TO SECOND, " +
        "INTERVAL '0 00:00:00' DAY TO SECOND)",
      "(DATE'2021-11-01', DATE'2021-11-01',  9, 100,  2,  4, CAST('12345678901234' AS BIGINT), " +
        "CAST('12345678901234' AS BIGINT), TIME'12:34:56.789', TIME'12:34:56.789', " +
        "INTERVAL '2 00:00:00' DAY TO SECOND, INTERVAL '2 00:00:00' DAY TO SECOND)")
    val allNull = Seq.fill(8)(
      "(CAST(NULL AS DATE), CAST(NULL AS DATE), CAST(NULL AS INT), CAST(NULL AS INT), " +
        "CAST(NULL AS INT), CAST(NULL AS INT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), " +
        "CAST(NULL AS TIME(6)), CAST(NULL AS TIME(6)), " +
        "CAST(NULL AS INTERVAL DAY TO SECOND), CAST(NULL AS INTERVAL DAY TO SECOND))")
    // Each row gets its position as `rid`, so the partition test can tell rows apart by id -
    // the all-null fixture's rows are otherwise identical.
    def view(values: Seq[String], where: String = ""): String =
      VarkaCoverageDifferentialSuite.fixtureView(values, where)
    // The dense fixture carries the all-null row in its VALUES and filters it out again, so
    // its columns stay nullable: over a column Spark knows to be non-nullable, `d IS NULL`
    // folds to false, `coalesce(d, d2)` to `d`, and the optimizer leaves nothing for Varka to
    // fuse - a fact about the optimizer, not about the table.
    Seq("varka_coverage_mixed" -> view(mixed), "varka_coverage_dense" -> view(dense :+ allNullRow,
      "\nWHERE d IS NOT NULL AND d2 IS NOT NULL AND i IS NOT NULL AND m IS NOT NULL AND " +
        "y IS NOT NULL AND mm IS NOT NULL AND l IS NOT NULL AND l2 IS NOT NULL AND " +
        "t IS NOT NULL AND t2 IS NOT NULL AND dt IS NOT NULL AND dt2 IS NOT NULL"),
      "varka_coverage_nulls" -> view(allNull))
  }

  /**
   * The ANSI form's fixture: one row at the extremes of every column's type beside one ordinary
   * row. Under ANSI, the extremes make the rows that can overflow or leave a range raise, so the
   * form can tell a skipped position from an evaluated one. Column `i` at `Int.MaxValue` is not
   * a valid month, so `make_date(2021, i, 1)` raises too, which is a fair cell: both engines
   * must raise it alone and neither where it is skipped.
   *
   * The two rows hold no null, so `coalesce(<column>, e)` never needs `e` on Spark. A third row
   * of nulls is filtered out again by `rid`, which keeps every column nullable: over a column
   * Spark knows to be non-nullable the optimizer would fold that COALESCE to the column, and
   * Varka would never see `e` (the dense fixture's trick above).
   */
  private val extremes: (String, String) = {
    val rows = Seq(
      "(DATE'9999-12-31', DATE'0001-01-01', 2147483647, -2147483648, 178956970, 7, " +
        "CAST('9223372036854775807' AS BIGINT), CAST('-9223372036854775808' AS BIGINT), " +
        "TIME'23:59:59.999999', TIME'00:00:00', " +
        "INTERVAL '106751991 04:00:54.775807' DAY TO SECOND, " +
        "INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND)",
      "(DATE'2024-02-29', DATE'2024-01-31', 3, 3, 1, 2, CAST('5000000000' AS BIGINT), " +
        "CAST('1' AS BIGINT), TIME'12:34:56.789', TIME'00:00:00', " +
        "INTERVAL '1 02:03:04.5' DAY TO SECOND, INTERVAL '0 00:00:00' DAY TO SECOND)",
      "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
    // `lrid` is `rid` as a BIGINT, the guard of a long-lane expression: one kernel holds one
    // lane, so `CASE WHEN rid < 0 THEN greatest(l, l2) ...` mixes the int and long lanes and is
    // never fused, where `CASE WHEN lrid < 0 ...` is.
    val view = VarkaCoverageDifferentialSuite.fixtureView(rows, "\nWHERE rid < 2")
    "varka_coverage_extremes" -> s"SELECT *, CAST(rid AS BIGINT) AS lrid FROM ($view)"
  }

  private def cacheFixtures(session: SparkSession): Unit = {
    fixtures.foreach { case (name, sql) =>
      session.sql(sql).createOrReplaceTempView(name)
      session.catalog.cacheTable(name)
    }
    // One partition, so both rows are one batch and an error is the same row's on both engines
    // rather than whichever failing task reaches the driver first (VARKA-42's discipline).
    session.sql(extremes._2).coalesce(1).createOrReplaceTempView(extremes._1)
    session.catalog.cacheTable(extremes._1)
  }

  private def isColumnarVarkaNode(plan: SparkPlan): Boolean = plan match {
    case _: VarkaProjectExec | _: VarkaFilterExec => true
    case _ => false
  }

  /**
   * Whether Varka's kernels evaluated the whole of a plan that ran: they served a batch, and
   * nothing was left to the row engine - no projection entry declined to the residual, and no
   * row `FilterExec` holding a conjunct the rule split off. A Varka node in the plan is not
   * enough: it can decline every batch, or fuse a guard while Spark evaluates the rest.
   */
  private def kernelServed(plan: SparkPlan): Boolean = {
    val nodes = collect(plan) { case v if isVarkaNode(v) => v }
    def sum(metric: String): Long = nodes.flatMap(_.metrics.get(metric)).map(_.value).sum
    sum("numVarkaBatches") > 0 && sum("numResidualEntries") == 0 &&
      find(plan) { case _: FilterExec => true; case _ => false }.isEmpty
  }

  private def queryFor(row: CoverageRow, fixture: String): String = row.form match {
    case "predicate" => s"SELECT d, d2, i FROM $fixture WHERE ${row.executable}"
    case _ => s"SELECT ${row.executable} AS v FROM $fixture"
  }

  /** The row consumer: the plan a direct query gets, checked warm as well as cold. */
  private def checkRowConsumer(row: CoverageRow, fixture: String): Unit = {
    val query = queryFor(row, fixture)
    val context = s"row consumer, $fixture, ${row.family}: ${row.sql}"
    val expected = spark.sql(query)
    val actual = varkaSpark.sql(query)
    val plan = actual.queryExecution.executedPlan
    withClue(context) {
      assertFused(plan)
      checkAnswer(actual, expected)
      // A predicate that selects nothing on a fixture may never reach the kernel: the in-memory
      // scan prunes a batch its column statistics rule out - an all-null batch under a
      // null-rejecting predicate, a null-free batch under `IS NULL` - before the Varka node
      // sees it. The node is in the plan and the empty answer is checked; only the batch
      // metric has nothing to count.
      if (!(row.form == "predicate" && expected.isEmpty)) {
        assertKernelsRan(plan)
      }
      checkAnswer(varkaSpark.sql(query), expected)
    }
  }

  /**
   * The columnar consumer: a cached view over the query, which the Arrow cache builder feeds
   * from the columnar Varka node rather than through rows.
   */
  private def checkColumnarConsumer(row: CoverageRow, fixture: String): Unit = {
    val query = queryFor(row, fixture)
    val context = s"columnar consumer, $fixture, ${row.family}: ${row.sql}"
    val view = "varka_coverage_out"
    for (session <- Seq(spark, varkaSpark)) {
      session.sql(query).createOrReplaceTempView(view)
      session.catalog.cacheTable(view)
    }
    try {
      withClue(context) {
        val cached = varkaSpark.table(view).queryExecution.executedPlan.collectFirst {
          case scan: InMemoryTableScanExec => scan.relation.cachedPlan
        }
        assert(cached.isDefined, "expected the view to be served from the cache")
        assert(cached.get.find(isColumnarVarkaNode).isDefined,
          s"expected a columnar Varka node under the cache:\n${cached.get.treeString}")
        checkAnswer(varkaSpark.table(view), spark.table(view))
      }
    } finally {
      for (session <- Seq(spark, varkaSpark)) session.catalog.uncacheTable(view)
    }
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    cacheFixtures(spark)
    cacheFixtures(varkaSpark)
  }

  test("the coverage table has rows to run") {
    assert(rows.size >= 57, s"only ${rows.size} rows read from sql/varka/coverage.json")
    assert(rows.forall(_.executable.nonEmpty))
  }

  rows.foreach { row =>
    test(s"${row.family}: ${row.sql} matches the row engine under both consumers") {
      fixtures.foreach { case (fixture, _) =>
        checkRowConsumer(row, fixture)
        checkColumnarConsumer(row, fixture)
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The partition oracle (VARKA-138)
  // ---------------------------------------------------------------------------------------------

  /** Every column of a fixture, `rid` first, as the partition queries select them. */
  private val allColumns = "rid, d, d2, i, ymm, ymy, ym, l, l2, t, t2, dt, dt2"

  /**
   * The optimizer rules that rewrite the partitions before Varka sees them: `BooleanSimplification`
   * and `NullDownPropagation` turn `NOT (d < d2)` and `(d < d2) IS NULL` into tests on the
   * columns, and `InferFiltersFromConstraints` adds `isnotnull` conjuncts beside a predicate that
   * rejects nulls. With them excluded, `NOT (p)` reaches Varka as written.
   */
  private val partitionRewrites = Seq(BooleanSimplification, NullDownPropagation,
    InferFiltersFromConstraints).map(_.ruleName).mkString(",")

  private def isVarkaFilter(plan: SparkPlan): Boolean = plan match {
    case _: VarkaFilterExec | _: VarkaFilterColumnarToRowExec => true
    case _ => false
  }

  /**
   * What is wrong with Varka's partitions of a table under a predicate `p`, or nothing.
   * `varkaTrue` and `varkaFalse` are Varka's answers to `WHERE p` and `WHERE NOT (p)`, the rest
   * Spark's: the same two, `WHERE (p) IS NULL`, and the whole table; every row starts with its
   * `rid`. Rows are compared as rows, never counted - a count misses a wrong row standing in for
   * a right one. The unknown partition is what is left of the table after Varka's two, because
   * Varka fuses no `IS NULL` over a comparison; were it fused, it would be queried instead.
   *
   * With `WHERE p` and `WHERE NOT (p)` each equal to Spark's, the last two checks hold Spark's own
   * three filters to each other. That is the point of them: both engines share the optimizer, so
   * a rewrite of `NOT (p)` that both get wrong agrees with itself in a differential and still
   * breaks the partition.
   */
  private def partitionFindings(
      varkaTrue: Seq[Row],
      varkaFalse: Seq[Row],
      sparkTrue: Seq[Row],
      sparkFalse: Seq[Row],
      sparkUnknown: Seq[Row],
      table: Seq[Row]): Seq[String] = {
    def ids(rows: Seq[Row]): Set[Int] = rows.map(_.getInt(0)).toSet
    val shared = ids(varkaTrue).intersect(ids(varkaFalse))
    val decided = ids(varkaTrue) ++ ids(varkaFalse)
    val unknown = table.filterNot(r => decided(r.getInt(0)))
    Seq(
      QueryTest.sameRows(sparkTrue, varkaTrue).map(d => s"WHERE p differs from Spark's:\n$d"),
      QueryTest.sameRows(sparkFalse, varkaFalse).map(d =>
        s"WHERE NOT (p) differs from Spark's:\n$d"),
      Option.when(shared.nonEmpty)(
        s"rows both true and false: rid ${shared.toSeq.sorted.mkString(", ")}"),
      QueryTest.sameRows(sparkUnknown, unknown).map(d =>
        s"the rows neither true nor false differ from Spark's WHERE (p) IS NULL:\n$d")
    ).flatten
  }

  /** The partition oracle for one predicate row on one fixture, under the session's optimizer. */
  private def partitionCheck(row: CoverageRow, fixture: String): Seq[String] = {
    val p = row.executable
    def query(where: String): String = s"SELECT $allColumns FROM $fixture WHERE $where"
    def onSpark(where: String): Seq[Row] = spark.sql(query(where)).collect().toSeq
    // Varka's answer, from a Varka filter whose kernel served the batches: a filter the rule
    // left to Spark, or one that declined, would make the comparison Spark against itself.
    def onVarka(where: String): Either[String, Seq[Row]] = {
      val df = varkaSpark.sql(query(where))
      val plan = df.queryExecution.executedPlan
      if (find(plan)(isVarkaFilter).isEmpty) {
        Left(s"no Varka filter for WHERE $where:\n${plan.treeString}")
      } else {
        val rows = df.collect().toSeq
        if (kernelServed(plan)) Right(rows)
        else Left(s"no kernel served all of WHERE $where:\n${plan.treeString}")
      }
    }
    (onVarka(p), onVarka(s"NOT ($p)")) match {
      case (Right(t), Right(f)) =>
        partitionFindings(t, f, onSpark(p), onSpark(s"NOT ($p)"), onSpark(s"($p) IS NULL"),
          spark.sql(s"SELECT $allColumns FROM $fixture").collect().toSeq)
      case (t, f) => Seq(t, f).flatMap(_.left.toOption)
    }
  }

  rows.filter(_.form == "predicate").foreach { row =>
    test(s"${row.family}: ${row.sql} splits every fixture into true, false and unknown rows") {
      val settings =
        Seq("default optimizer" -> None, "rewrites excluded" -> Some(partitionRewrites))
      // The in-memory scan prunes a batch its statistics rule out - an all-null batch under a
      // predicate that rejects nulls - and a pruned batch meets no kernel, so pruning is off.
      val findings = withBothConf(SQLConf.IN_MEMORY_PARTITION_PRUNING.key, "false") {
        for {
          (setting, excluded) <- settings
          (fixture, _) <- fixtures
          finding <- excluded match {
            case Some(rules) => withBothConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, rules) {
              partitionCheck(row, fixture)
            }
            case None => partitionCheck(row, fixture)
          }
        } yield s"$fixture, $setting: $finding"
      }
      assert(findings.isEmpty, findings.mkString("\n"))
    }
  }

  test("the partition oracle fails a filter that answers unknown where it should answer false") {
    // No kernel can be broken this way on purpose, so the fault is planted in the answers: a
    // filter that swapped known-false and unknown would return Spark's `IS NULL` rows for
    // `NOT (p)`. The oracle must name both the wrong false rows and the wrong rest.
    val fixture = "varka_coverage_mixed"
    def answer(where: String): Seq[Row] =
      spark.sql(s"SELECT $allColumns FROM $fixture WHERE $where").collect().toSeq
    val (t, f, u) = (answer("d < d2"), answer("NOT (d < d2)"), answer("(d < d2) IS NULL"))
    assert(f.nonEmpty && u.nonEmpty, "the planted fault needs both partitions non-empty")
    val table = spark.sql(s"SELECT $allColumns FROM $fixture").collect().toSeq
    assert(partitionFindings(t, f, t, f, u, table).isEmpty)
    val findings = partitionFindings(t, u, t, f, u, table)
    assert(findings.exists(_.startsWith("WHERE NOT (p) differs")), findings.mkString("\n"))
    assert(findings.exists(_.startsWith("the rows neither true nor false")),
      findings.mkString("\n"))
  }

  // ---------------------------------------------------------------------------------------------
  // The ANSI form (VARKA-138)
  // ---------------------------------------------------------------------------------------------

  /** What a query did: the rows it answered, or the error it raised as `checkError` sees one. */
  private sealed trait Outcome

  private case class Answered(rows: Seq[Row]) extends Outcome

  private case class Raised(condition: String, sqlState: String, parameters: Map[String, String])
    extends Outcome

  /**
   * The outcome of `query` on `session`, and the plan it ran with unless it raised before
   * running. Analysis, planning and execution are all inside, so an error raised at any stage
   * is an outcome on either engine. An error is the first `SparkThrowable` with a condition on
   * its cause chain: a task's error reaches the driver wrapped in the job's failure. An error
   * with no condition anywhere is kept by its root cause's class and message, not the job
   * failure's, whose text names stage and task numbers that differ between any two runs.
   */
  private def run(session: SparkSession, query: String): (Outcome, Option[SparkPlan]) = {
    var plan: Option[SparkPlan] = None
    val result = Try {
      val df = session.sql(query)
      plan = Some(df.queryExecution.executedPlan)
      df.collect().toSeq
    }
    val out = result match {
      case Success(rows) => Answered(rows)
      case Failure(e) =>
        val chain = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).toSeq
        chain.collectFirst { case t: SparkThrowable if t.getCondition != null =>
          Raised(t.getCondition, t.getSqlState, t.getMessageParameters.asScala.toMap)
        }.getOrElse(Raised(chain.last.getClass.getName, null,
          Map("message" -> String.valueOf(chain.last.getMessage))))
    }
    (out, plan)
  }

  private def outcome(session: SparkSession, query: String): Outcome = run(session, query)._1

  /** How Varka's outcome differs from Spark's, or nothing. */
  private def outcomeDifference(onSpark: Outcome, onVarka: Outcome): Option[String] =
    (onSpark, onVarka) match {
      case (Answered(s), Answered(v)) => QueryTest.sameRows(s, v).map(d => s"rows differ:\n$d")
      case (s: Raised, v: Raised) => Option.when(s != v)(s"errors differ: Spark $s, Varka $v")
      case (s: Raised, v: Answered) => Some(s"only Spark raises: Spark $s, Varka $v")
      case (s: Answered, v: Raised) => Some(s"only Varka raises: Spark $s, Varka $v")
    }

  private val extremesTable = extremes._1

  /** The positions of the ANSI form; every one but the first is one Spark skips on both rows. */
  private val Alone = "alone"
  private val CaseArm = "the untaken arm of a CASE"
  private val CoalesceTail = "a later COALESCE argument"
  private val FalseAnd = "the right side of a false AND"
  private val TrueOr = "the right side of a true OR"

  /** Whether values of `t` live on Varka's long lane rather than its int lane. */
  private def onLongLane(t: DataType): Boolean = t match {
    case LongType | _: TimeType | _: DayTimeIntervalType => true
    case _ => false
  }

  /**
   * A value of the extremes fixture of `t`'s type that never raises, to stand beside an
   * expression of that type in a CASE (`caseArm`) or ahead of it in a COALESCE: a column, which
   * is non-null on both rows, so a COALESCE that starts with one never needs its second
   * argument. Where the fixture has the type's family but not its exact fields or precision -
   * `t - t2` is an HOUR TO SECOND interval beside `dt2`'s DAY TO SECOND - Spark widens the
   * expression to the common type with a cast, which is still evaluated only where `e` is.
   * Varka compiles a bare int column as no value, so the int CASE's other arm is `year(d2)`, an
   * int the kernel computes and that cannot raise; the int COALESCE keeps the bare column, and
   * the declined list says why it is not fused. The same column is what the false AND's right
   * side compares `e` with (`caseArm` false), where a bare int column is an operand Varka takes.
   */
  private def standIn(t: DataType, caseArm: Boolean): Option[String] = t match {
    case DateType => Some("d2")
    case IntegerType => Some(if (caseArm) "year(d2)" else "i")
    case LongType => Some("l2")
    case _: TimeType => Some("t2")
    case _: DayTimeIntervalType => Some("dt2")
    case YearMonthIntervalType(start, end) if start == end =>
      Some(if (start == YearMonthIntervalType.YEAR) "ymy" else "ymm")
    case _: YearMonthIntervalType => Some("ym")
    case _ => None
  }

  /**
   * The ANSI form's queries for a row: each position and its query, or None where the position
   * cannot be written for the row. A projection `e` runs alone, in the untaken arm of
   * `CASE WHEN rid < 0`, after a column in a COALESCE, and compared with a column behind
   * `rid < 0`;
   * a predicate `p` behind `rid < 0 AND` and `rid >= 0 OR`. `rid` is 0 and 1, so each guard
   * decides every row, and Spark never evaluates the guarded side. Where the guard and the
   * guarded side form one Varka expression - the CASE, the OR, the AND of a predicate - the
   * guard is `lrid` for a long-lane expression, so the two share a lane.
   */
  private def ansiQueries(row: CoverageRow): Seq[(String, Option[String])] = {
    val e = row.executable
    val from = s"FROM $extremesTable"
    row.form match {
      case "predicate" =>
        // The predicate's own columns, analyzed as a projection so no filter of the fixture's
        // view joins them.
        val read = spark.sql(s"SELECT ($e) AS v $from").queryExecution.analyzed match {
          case Project(list, _) => list.flatMap(_.references.toSeq)
          case other => fail(s"expected a projection, got\n$other")
        }
        val guard = if (read.exists(a => onLongLane(a.dataType))) "lrid" else "rid"
        Seq(
          FalseAnd -> Some(s"SELECT rid $from WHERE $guard < 0 AND ($e)"),
          TrueOr -> Some(s"SELECT rid $from WHERE $guard >= 0 OR ($e)"))
      case _ =>
        val t = spark.sql(s"SELECT $e AS v $from").schema.head.dataType
        val guard = if (onLongLane(t)) "lrid" else "rid"
        Seq(
          Alone -> Some(s"SELECT $e AS v $from"),
          CaseArm -> standIn(t, caseArm = true).map(c =>
            s"SELECT CASE WHEN $guard < 0 THEN $e ELSE $c END AS v $from"),
          CoalesceTail -> standIn(t, caseArm = false).map(c =>
            s"SELECT coalesce($c, $e) AS v $from"),
          // `e = <column>`, not `e IS NOT NULL`: Varka fuses no IS NOT NULL over a computed
          // value, so Spark's own filter would evaluate `e` above the kernel.
          FalseAnd -> standIn(t, caseArm = false).map(c =>
            s"SELECT rid $from WHERE $guard < 0 AND ($e) = $c"))
    }
  }

  /**
   * The positions where Varka does not serve a row's expression, with the reason, keyed by the
   * row's `sql` and the position. A skipped position not served by a kernel is a check that
   * compared Spark with itself, so it must be listed here; a listed one that is served fails as
   * stale. Checked under the defaults only: what a configuration of the option matrix fuses is
   * the matrix's own business, and the outcomes are compared under every configuration.
   */
  private val ansiDeclined: Map[(String, String), String] = {
    val intCoalesce = "Varka's COALESCE takes only a bare date column before its last operand, " +
      "and compiles no bare int column as a value (m9/SCOPE.md item 55)"
    val narrowed = "an int computed on the long lane is narrowed only at a kernel's store, so " +
      "it cannot sit under a CASE, a COALESCE or a comparison with an int (VARKA-28)"
    val intValued = Seq("datediff(d2, d)", "unix_date(d)", "year(d)", "month(d)", "day(d)",
      "quarter(d)", "dayofyear(d)", "dayofweek(d)", "weekday(d)",
      "extract(DAYOFWEEK_ISO FROM d)", "weekofyear(d)", "extract(YEAROFWEEK FROM d)",
      "extract(YEAR FROM ym)", "extract(YEAR FROM ym) - 1", "i + 1", "i - 1", "year(d) * 2",
      "-i", "year(d) + i")
    val timeFields = Seq("hour(t)", "minute(t2)", "second(t)")
    // `CAST(i AS INTERVAL DAY)` throws past `INTERVAL_DAY_LIMIT_DAYS`, so the compiler puts a
    // bound on `i` that the evaluator checks before the kernel runs; the bound belongs to the
    // column, not to the arm reading it, and the extremes row's `Int.MaxValue` declines the
    // batch wherever the expression sits (m9/SCOPE.md item 96).
    val dayCast = "d + CAST(i AS INTERVAL DAY)"
    val columnBound = "the offset column's bound is checked per batch whichever arm reads it, " +
      "and Int.MaxValue is past it, so the batch declines (m9/SCOPE.md item 96)"
    // A check that condemns the batch - an overflow, a field out of range, a date leaving the
    // range the calendar lowering is exact over - is qualified by the condition of a CASE arm
    // it sits under (`guardUnderArm`), but not by the left side of an AND: the kernel computes
    // the right side on every lane, and the extremes row's lane condemns the batch although
    // `rid < 0` already rejected it (m9/SCOPE.md item 96).
    val unqualified = "a check on the right side of an AND condemns the batch from a lane the " +
      "left side rejected (m9/SCOPE.md item 96)"
    val checkedRight = Seq("make_date(2021, i, 1)", "add_months(d, i)", "d + ym", "d - ym",
      "ym - ymm", "i + 1", "year(d) + i")
    intValued.map(sql => (sql, CoalesceTail) -> intCoalesce).toMap ++
      timeFields.flatMap(sql =>
        Seq(CaseArm, CoalesceTail, FalseAnd).map(p => (sql, p) -> narrowed)) ++
      Seq(CaseArm, CoalesceTail, FalseAnd).map(p => (dayCast, p) -> columnBound) ++
      checkedRight.map(sql => (sql, FalseAnd) -> unqualified)
  }

  /** What the ANSI form found for one row, and where a kernel served it or both engines raised. */
  private case class AnsiResult(findings: Seq[String], served: Seq[String], raised: Seq[String])

  /** The ANSI form for one row. */
  private def ansiCheck(row: CoverageRow): AnsiResult = {
    val strict = VarkaMatrix.config.isEmpty
    val results = ansiQueries(row).map { case (position, query) =>
      val declined = ansiDeclined.get((row.sql, position))
      query match {
        case None =>
          val finding = Option.when(strict && declined.isEmpty)(
            s"$position: no column of the expression's type, and not on the declined list")
          (finding.toSeq, None, None)
        case Some(q) =>
          val (varka, plan) = run(varkaSpark, q)
          val served = varka.isInstanceOf[Answered] && plan.exists(kernelServed)
          val difference = outcomeDifference(outcome(spark, q), varka)
          val reach = if (position == Alone || !strict) None
          else if (served && declined.isDefined) {
            Some("served by a kernel, but on the declined list: remove the stale entry")
          } else if (!served && declined.isEmpty) {
            val node = if (plan.exists(find(_)(isVarkaNode).isDefined)) {
              "a Varka node that served no batch, or left part of the query to the row engine"
            } else {
              "no Varka node"
            }
            Some(s"not served by a kernel ($node), and not on the declined list:\n" +
              plan.map(_.treeString).getOrElse("(no plan)"))
          } else {
            None
          }
          ((difference ++ reach).map(f => s"$position: $q\n$f").toSeq,
            Option.when(served)(position),
            Option.when(difference.isEmpty && varka.isInstanceOf[Raised])(position))
      }
    }
    AnsiResult(results.flatMap(_._1), results.flatMap(_._2), results.flatMap(_._3))
  }

  rows.foreach { row =>
    test(s"${row.family}: ${row.sql} raises under ANSI only where the row engine does") {
      // The in-memory scan prunes a batch whose statistics rule a filter out, and `rid < 0`
      // rules out every batch: pruned, the guarded side would never meet a kernel and the
      // check would pass by never running.
      val result = withAnsi(enabled = true) {
        withBothConf(SQLConf.IN_MEMORY_PARTITION_PRUNING.key, "false")(ansiCheck(row))
      }
      def list(positions: Seq[String]): String =
        if (positions.isEmpty) "none" else positions.mkString(", ")
      info(s"served by a kernel in: ${list(result.served)}")
      info(s"raised by both engines in: ${list(result.raised)}")
      assert(result.findings.isEmpty, result.findings.mkString("\n\n"))
    }
  }

  test("the ANSI form's declined list names only positions its reach check reads") {
    // An entry for a row the table lacks, for a position the row's form never runs, or for the
    // lone position, which has no reach check, could never go stale.
    val unread = ansiDeclined.keys.filterNot { case (sql, position) =>
      position != Alone && rows.find(_.sql == sql).exists(row =>
        ansiQueries(row).exists { case (p, q) => p == position && q.isDefined })
    }
    assert(unread.isEmpty, s"declined entries no reach check reads: $unread")
  }

  test("the ANSI form fails a raise where the row engine skips, and a missing one") {
    // No kernel raises on purpose, so the fault is planted in the outcomes, which are real:
    // `i + 1` overflows alone on the extremes row and is skipped in the untaken arm. A kernel
    // that evaluated the arm and raised would give the CASE the lone outcome, and one that
    // wrapped instead of raising would give the lone query rows.
    withAnsi(enabled = true) {
      withBothConf(SQLConf.IN_MEMORY_PARTITION_PRUNING.key, "false") {
        val alone = outcome(spark, s"SELECT i + 1 AS v FROM $extremesTable")
        val skipped = outcome(spark,
          s"SELECT CASE WHEN rid < 0 THEN i + 1 ELSE i END AS v FROM $extremesTable")
        val raised = alone match {
          case r: Raised => r
          case other => fail(s"expected i + 1 to overflow on the extremes row, got $other")
        }
        assert(raised.condition === "ARITHMETIC_OVERFLOW")
        assert(skipped.isInstanceOf[Answered])
        assert(outcomeDifference(skipped, skipped).isEmpty)
        assert(outcomeDifference(skipped, alone).exists(_.startsWith("only Varka raises")))
        assert(outcomeDifference(alone, skipped).exists(_.startsWith("only Spark raises")))
        // The same condition with other parameters is a different error.
        val moved = raised.copy(parameters = raised.parameters + ("planted" -> "1"))
        assert(outcomeDifference(alone, moved).exists(_.startsWith("errors differ")))
      }
    }
  }
}

private object VarkaCoverageDifferentialSuite {

  /**
   * A fixture's view over `values`, rows of the twelve source columns in parentheses, each
   * given its position as an int `rid` that no coverage row names; `where` filters the rows.
   */
  def fixtureView(values: Seq[String], where: String = ""): String = {
    val withIds = values.zipWithIndex.map { case (v, rid) => s"($rid, ${v.stripPrefix("(")}" }
    s"""SELECT rid, d, d2, i,
       |       CAST(m AS INTERVAL MONTH) AS ymm,
       |       CAST(y AS INTERVAL YEAR) AS ymy,
       |       make_ym_interval(y, mm) AS ym,
       |       l, l2, CAST(t AS TIME(6)) AS t, CAST(t2 AS TIME(6)) AS t2, dt, dt2
       |FROM VALUES ${withIds.mkString(",\n  ")}
       |AS v(rid, d, d2, i, m, y, mm, l, l2, t, t2, dt, dt2)$where""".stripMargin
  }
}
