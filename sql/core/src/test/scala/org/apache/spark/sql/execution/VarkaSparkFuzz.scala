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
import scala.util.{Random, Try}

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaShrinker

/**
 * The pure parts of the random differential against vanilla Spark (VARKA-262): a case, its
 * draw, its rendering as SQL, the comparison of two outcomes, the shrinker and the reproducer
 * text. The sessions are the suites'; this holds nothing that needs a `SparkSession` but the
 * `outcome` of a query.
 *
 * A case is a list of outputs and a `WHERE` of conjuncts over twelve columns of random data.
 * The outputs and conjuncts are the `executable` column of `sql/varka/coverage.json`, so a row
 * added to the table is drawn here with no change to this file.
 */
object VarkaSparkFuzz {

  /** One row of the coverage table. */
  final case class CoverageRow(sql: String, executable: String, form: String)

  /** A conjunct of the `WHERE`, possibly negated. */
  final case class Conjunct(sql: String, negated: Boolean) {
    def text: String = if (negated) s"NOT ($sql)" else s"($sql)"
  }

  /**
   * A case. `data` is a list of rows of twelve SQL literals, in the order of `columns`. `op` is
   * `AND` or `OR`, joining the conjuncts.
   */
  final case class Case(
      outputs: Vector[String],
      conjuncts: Vector[Conjunct],
      op: String,
      data: Vector[Vector[String]],
      ansi: Boolean) {

    def select: String = outputs.zipWithIndex.map { case (o, k) => s"$o AS c$k" }.mkString(", ")

    /** The `WHERE` expression, or None when there is no filter. */
    def where: Option[String] =
      if (conjuncts.isEmpty) None else Some(conjuncts.map(_.text).mkString(s" $op "))

    def query: String = querySql(select, where)

    def fixture: String = fixtureSql(data)
  }

  def querySql(select: String, where: Option[String]): String =
    s"SELECT $select FROM fz${where.map(w => s" WHERE $w").getOrElse("")}"

  /** The twelve columns, as the fixtures of `VarkaCoverageDifferentialSuite` build them. */
  def fixtureSql(data: Seq[Seq[String]]): String =
    s"""SELECT d, d2, i,
       |       CAST(m AS INTERVAL MONTH) AS ymm,
       |       CAST(y AS INTERVAL YEAR) AS ymy,
       |       make_ym_interval(y, mm) AS ym,
       |       l, l2, CAST(t AS TIME(6)) AS t, CAST(t2 AS TIME(6)) AS t2, dt, dt2
       |FROM VALUES ${data.map(_.mkString("(", ", ", ")")).mkString(",\n  ")}
       |AS v(d, d2, i, m, y, mm, l, l2, t, t2, dt, dt2)""".stripMargin

  /** The coverage table's rows. */
  def coverageRows(file: java.io.File): Seq[CoverageRow] =
    new ObjectMapper().readTree(file).get("expressions").elements().asScala.toSeq.map { e =>
      CoverageRow(e.get("sql").asText(), e.get("executable").asText(), e.get("form").asText())
    }

  // ---- the draw ---------------------------------------------------------------------------

  private val dates = Seq("2024-01-31", "2024-02-29", "2023-12-27", "2021-01-01", "2021-06-01",
    "2021-03-15", "1969-12-31", "1970-01-01", "2000-02-29", "1999-12-31", "2021-11-01")
  private val farDates = Seq("9999-12-01", "0001-01-15")
  private val longs = Seq("0", "1", "-1", "42", "-42", "5000000000", "-5000000000", "2147483647",
    "2147483648", "-2147483648", "-2147483649", "12345678901234")
  private val farLongs = Seq("9223372036854775807", "-9223372036854775808")
  private val times = Seq("00:00:00", "12:34:56.789", "23:59:59.999999", "06:00:00",
    "00:00:00.000001", "18:00:00", "09:30:00", "01:02:03.456")
  private val intervals = Seq("0 00:00:00", "1 02:03:04.5", "-3 00:00:00.000001",
    "100000 00:00:00", "-100000 00:00:00", "7 12:00:00", "0 00:00:01", "-0 00:00:00.5")

  private def pick[A](rnd: Random, xs: Seq[A]): A = xs(rnd.nextInt(xs.size))

  /**
   * `n` rows of random data. The rows are drawn from a pool that keeps every value inside the
   * guards and the ANSI checks (a valid month, a date near the epoch, magnitudes that do not
   * overflow), so a query over them compares values. A `safe` case is only such rows; a full one
   * adds up to two hostile rows, each with one to three cells at an extreme - a far date, a bound
   * of an int, a bound of a long - where the comparison is of what is raised or declined. A pool
   * that was hostile throughout made 70% of the full cases fail on both engines, which compares
   * the class of an error and nothing about the other rows. The null rate is drawn per case.
   */
  def drawData(rnd: Random, n: Int, safe: Boolean): Vector[Vector[String]] = {
    val nullRate = pick(rnd, Seq(0.0, 0.2, 0.2, 0.5, 1.0))
    // A null is typed, as the fixtures of `VarkaCoverageDifferentialSuite` type theirs: a bare
    // `NULL` in every row of a column makes it `VOID`, a type the relation can carry and the
    // evaluator's row fallback cannot convert (see the reproducer `void-column-fallback.sql`).
    def maybe(sqlType: String)(s: => String): String =
      if (rnd.nextDouble() < nullRate) s"CAST(NULL AS $sqlType)" else s
    def int(lo: Int, hi: Int): String = maybe("INT")((lo + rnd.nextInt(hi - lo + 1)).toString)
    def date(): String = maybe("DATE")(s"DATE'${pick(rnd, dates)}'")
    def long(): String = maybe("BIGINT")(s"CAST('${pick(rnd, longs)}' AS BIGINT)")
    def time(): String = maybe("TIME(6)")(s"TIME'${pick(rnd, times)}'")
    def interval(): String =
      maybe("INTERVAL DAY TO SECOND")(s"INTERVAL '${pick(rnd, intervals)}' DAY TO SECOND")
    // Column by column: the safe draw, and the extreme a hostile row puts there.
    val columns: Vector[(() => String, () => String)] = Vector(
      (() => date(), () => s"DATE'${pick(rnd, farDates)}'"),
      (() => date(), () => s"DATE'${pick(rnd, farDates)}'"),
      (() => int(1, 12),
        () => pick(rnd, Seq(0, -1, 13, 100000, Int.MaxValue, Int.MinValue)).toString),
      (() => int(-14, 100), () => pick(rnd, Seq(Int.MaxValue, Int.MinValue)).toString),
      (() => int(-2, 4), () => pick(rnd, Seq(178956971, Int.MaxValue)).toString),
      (() => int(0, 11), () => pick(rnd, Seq(12, -1)).toString),
      (() => long(), () => s"CAST('${pick(rnd, farLongs)}' AS BIGINT)"),
      (() => long(), () => s"CAST('${pick(rnd, farLongs)}' AS BIGINT)"),
      (() => time(), () => s"TIME'${pick(rnd, times)}'"),
      (() => time(), () => s"TIME'${pick(rnd, times)}'"),
      (() => interval(), () => s"INTERVAL '${pick(rnd, intervals)}' DAY TO SECOND"),
      (() => interval(), () => s"INTERVAL '${pick(rnd, intervals)}' DAY TO SECOND"))
    val hostile =
      if (safe) Set.empty[Int] else rnd.shuffle((0 until n).toList).take(rnd.nextInt(3)).toSet
    Vector.tabulate(n) { row =>
      val cells = columns.map(_._1())
      if (!hostile.contains(row)) cells
      else {
        val spots = rnd.shuffle(columns.indices.toList).take(1 + rnd.nextInt(3)).toSet
        cells.zipWithIndex.map { case (cell, j) =>
          if (spots.contains(j)) columns(j)._2() else cell
        }
      }
    }
  }

  /**
   * Composition `iteration` of the stream for `seed`. The draw is stratified: composition `k`
   * includes row `order(k mod rows)` of a shuffled order of the table, as an output if it is a
   * projection and as a conjunct if it is a predicate, so that every row is drawn in every pass
   * of the table's length - a uniform draw found a bug in one row after 254 of 300 compositions.
   */
  def draw(rows: Seq[CoverageRow], seed: Long, iteration: Int): Case = {
    val projections = rows.filter(_.form == "projection")
    val predicates = rows.filter(_.form == "predicate")
    val order = new Random(seed).shuffle(rows.indices.toVector)
    val forced = rows(order(iteration % rows.size))
    val rnd = new Random(seed * 1000003L + iteration)
    val outs = scala.collection.mutable.ArrayBuffer.empty[String]
    val conj = scala.collection.mutable.ArrayBuffer.empty[Conjunct]
    if (forced.form == "projection") outs += forced.executable
    val k = 1 + rnd.nextInt(4)
    while (outs.size < k) outs += pick(rnd, projections).executable
    val filter = rnd.nextInt(3)
    if (filter > 0 || forced.form == "predicate") {
      if (forced.form == "predicate") conj += Conjunct(forced.executable, rnd.nextInt(4) == 0)
      val more = if (filter == 2) 1 + rnd.nextInt(2) else if (conj.isEmpty) 1 else 0
      (0 until more).foreach(_ =>
        conj += Conjunct(pick(rnd, predicates).executable, rnd.nextInt(4) == 0))
    }
    Case(rnd.shuffle(outs.toVector), rnd.shuffle(conj.toVector),
      if (rnd.nextBoolean()) "AND" else "OR", drawData(rnd, 12, safe = iteration % 2 == 0),
      ansi = rnd.nextBoolean())
  }

  // ---- the comparison ---------------------------------------------------------------------

  /** A query's outcome: its rows sorted as strings, or the class of what it raised. */
  type Outcome = Either[String, Seq[String]]

  def outcome(session: SparkSession, sql: String): Outcome =
    Try(session.sql(sql).collect().map(_.toString).sorted.toSeq).toEither.left.map { e =>
      val chain = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).toSeq
      chain.collectFirst { case t: SparkThrowable if t.getCondition != null => t.getCondition }
        .getOrElse(e.getClass.getName)
    }

  /** How two outcomes differ, or None when they agree. */
  def difference(off: Outcome, on: Outcome): Option[String] = (off, on) match {
    case (Right(a), Right(b)) => if (a == b) None else Some("values differ")
    case (Left(a), Left(b)) => if (a == b) None else Some(s"error classes differ: $a, $b")
    case (Left(a), Right(_)) => Some(s"only the row engine throws: $a")
    case (Right(_), Left(b)) => Some(s"only Varka throws: $b")
  }

  /**
   * The ternary partition of a filter: the rows of `WHERE p`, `WHERE NOT (p)` and
   * `WHERE (p) IS NULL` together are the rows of the unfiltered query. None when it holds or
   * when a side raised (an error says nothing about the rows).
   */
  def partitionBroken(
      select: String, where: String, run: String => Outcome): Option[String] = {
    val whole = run(querySql(select, None))
    val parts = Seq(where, s"NOT ($where)", s"($where) IS NULL").map(w =>
      run(querySql(select, Some(w))))
    if ((whole +: parts).exists(_.isLeft)) None
    else {
      val union = parts.flatMap(_.toOption.get).sorted
      if (union == whole.toOption.get.sorted) None else Some("partition broken")
    }
  }

  // ---- the shrinker -----------------------------------------------------------------------

  /**
   * `c` reduced while `kind` still shows: the outputs, the conjuncts and the rows of data, each by
   * ddmin, repeated until a pass changes nothing, within `maxRuns` checks. `check` returns the
   * kind of disagreement a case shows, or None.
   */
  def shrink(c: Case, kind: String, check: Case => Option[String], maxRuns: Int = 400): Case = {
    var runs = 0
    def still(x: Case): Boolean = {
      if (runs >= maxRuns) false
      else {
        runs += 1
        check(x).contains(kind)
      }
    }
    var best = c
    var changed = true
    while (changed && runs < maxRuns) {
      val before = best
      if (best.outputs.size > 1) {
        best = best.copy(outputs = VarkaShrinker.ddmin(best.outputs)(os =>
          os.nonEmpty && still(best.copy(outputs = os))))
      }
      if (best.conjuncts.nonEmpty) {
        best = best.copy(conjuncts = VarkaShrinker.ddmin(best.conjuncts)(cs =>
          still(best.copy(conjuncts = cs))))
      }
      if (best.data.size > 1) {
        best = best.copy(data = VarkaShrinker.ddmin(best.data)(ds =>
          ds.nonEmpty && still(best.copy(data = ds))))
      }
      changed = best != before
    }
    best
  }

  // ---- the reproducer file ----------------------------------------------------------------

  /** A reproducer: what a file holds, enough to replay without the draw. */
  final case class Reproducer(
      status: String,
      note: String,
      ansi: Boolean,
      kind: String,
      select: String,
      where: Option[String],
      fixture: String)

  def render(r: Reproducer): String =
    s"""-- VARKA-262 reproducer
       |-- status: ${r.status}
       |-- note: ${r.note}
       |-- ansi: ${r.ansi}
       |-- kind: ${r.kind}
       |-- select: ${r.select}
       |-- where: ${r.where.getOrElse("")}
       |-- fixture
       |${r.fixture}
       |-- query
       |${querySql(r.select, r.where)}
       |""".stripMargin

  def parse(text: String): Reproducer = {
    val lines = text.linesIterator.toVector
    def header(key: String): String = lines.collectFirst {
      case l if l.startsWith(s"-- $key: ") => l.substring(key.length + 5)
      case l if l == s"-- $key:" => ""
    }.getOrElse(throw new IllegalArgumentException(s"no '-- $key:' line in the reproducer"))
    val f = lines.indexOf("-- fixture")
    val q = lines.indexOf("-- query")
    require(f >= 0 && q > f, "a reproducer has a '-- fixture' and then a '-- query' section")
    val where = header("where")
    Reproducer(header("status"), header("note"), header("ansi").toBoolean, header("kind"),
      header("select"), if (where.isEmpty) None else Some(where),
      lines.slice(f + 1, q).mkString("\n"))
  }
}
