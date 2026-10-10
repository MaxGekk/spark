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

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.sql.execution.VarkaSparkFuzz._

/**
 * Random compositions of the coverage table's rows over random data, run with Varka off and on,
 * ANSI off and on, the answers compared (VARKA-262). A disagreement is shrunk, written as a
 * reproducer under `target/varka-sparkfuzz/`, and fails the test with the reproducer in its
 * message; `sql/varka/fuzz/spark/` is where a reviewed one is kept.
 *
 * Budget: `-Dvarka.sparkfuzz.iterations` (default 200, about ten seconds) and
 * `-Dvarka.sparkfuzz.seed`; `-Dvarka.sparkfuzz.only=<iteration>` replays one. Every
 * `partitionEvery`th composition with a filter also checks the
 * ternary partition on both engines.
 */
class VarkaSparkFuzzSuite extends VarkaSparkDifferential {

  private lazy val rows: Seq[CoverageRow] =
    coverageRows(getWorkspaceFilePath("sql", "varka", "coverage.json").toFile)

  private val seed = sys.props.get("varka.sparkfuzz.seed").map(_.toLong).getOrElse(20261008L)
  private val iterations = sys.props.get("varka.sparkfuzz.iterations").map(_.toInt).getOrElse(200)
  private val onlyIteration = sys.props.get("varka.sparkfuzz.only").map(_.toInt)
  // Every output of a composition forced in turn, where the default forces one (VARKA-296).
  private val forceAll = sys.props.get("varka.sparkfuzz.forceAll").contains("true")
  private val partitionEvery = 4
  private var firstFailure: Option[Int] = None

  private def kindOf(c: Case, partitions: Boolean, forces: Seq[Int]): Option[String] =
    disagreement(c.fixture, c.select, c.where, c.ansi, partitions, c.pivot.map(rowText), forces)

  /**
   * The outputs a composition forces: one, chosen by the iteration, or every one. A composition of
   * one output forces none: with its only output declined the plan keeps no Varka node, so the
   * plan cannot prove the force moved anything.
   */
  private def forcesOf(c: Case, it: Int): Seq[Int] =
    if (c.outputs.size < 2) Nil
    else if (forceAll) c.outputs.indices
    else Seq(it % c.outputs.size)

  test(s"random compositions agree with the row engine (seed $seed, $iterations of them)") {
    val failures = scala.collection.mutable.ArrayBuffer.empty[String]
    val pool = scala.collection.mutable.TreeMap.empty[String, (Int, Int)]
    val range = onlyIteration.map(k => k to k).getOrElse(0 until iterations)
    for (it <- range if failures.size < 3) {
      val c = rectify(draw(rows, seed, it))
      val partitions = it % partitionEvery == 0
      val errorsBefore = bothErrored
      val result = kindOf(c, partitions, forcesOf(c, it))
      val key = (if (it % 2 == 0) "safe" else "full") + (if (c.ansi) " ANSI on" else " ANSI off")
      pool(key) = pool.getOrElse(key, (0, 0)) match {
        case (n, e) => (n + 1, e + (if (bothErrored > errorsBefore) 1 else 0))
      }
      result.foreach { found =>
        // A forced disagreement is shrunk with the one output that shows it forced, so the
        // reproducer can name it; a forced kind that needs every force names none.
        val forced = if (!found.startsWith("with an output forced")) c
          else c.copy(force = forcesOf(c, it).find(k => kindOf(c, partitions, Seq(k)).isDefined))
        val kind = kindOf(forced, partitions, forced.force.toSeq).getOrElse(found)
        val small = shrink(forced, kind, x => kindOf(x, partitions, x.force.toSeq))
        val text = render(Reproducer("regression", s"seed $seed iteration $it", small.ansi, kind,
          small.select, small.where, small.fixture, small.pivot.map(rowText), small.force))
        val dir = getWorkspaceFilePath("sql", "core", "target", "varka-sparkfuzz")
        Files.createDirectories(dir)
        val file = dir.resolve(s"$seed-$it.sql")
        Files.write(file, text.getBytes(StandardCharsets.UTF_8))
        if (firstFailure.isEmpty) firstFailure = Some(it)
        failures += s"$kind (seed $seed, iteration $it), reproducer written to $file:\n$text"
      }
    }
    info(s"compared $compared, fused $fused, both errored $bothErrored")
    pool.foreach { case (k, (n, e)) => info(s"  $k: $n compositions, $e errored on both sides") }
    info(s"filters returning a row: $filteredNonEmpty of $filtered, with a pivot " +
      s"$pivotFilteredNonEmpty of $pivotFiltered")
    info(s"outputs forced to decline: $forcedRuns, of which $forcedApplied changed the plan")
    // A force that silently stopped moving the plan would compare nothing. One that lands on an
    // output already residual (the one-lane rule) or in a plan with no Varka project node moves
    // nothing, which leaves well over a quarter that do.
    if (forcedRuns >= 50) {
      assert(forcedApplied * 4 >= forcedRuns,
        s"only $forcedApplied of $forcedRuns forced outputs changed the plan")
    }
    info(s"first disagreement at iteration ${firstFailure.getOrElse("none")}")
    assert(failures.isEmpty, failures.mkString("\n"))
  }

  test("the draw includes every row of the table in every pass of the table's length") {
    val seen = scala.collection.mutable.Set.empty[String]
    for (it <- 0 until rows.size) {
      val c = draw(rows, seed, it)
      seen ++= c.outputs
      seen ++= c.conjuncts.map(_.sql)
    }
    assert(rows.map(_.executable).toSet.subsetOf(seen), "a row of the table was not drawn")
  }

  test("the shrinker reduces a planted failure to the output that causes it and one row") {
    val c = draw(rows, seed, 5).copy(
      outputs = Vector("date_add(d, 3)", "datediff(d2, d)", "year(d)", "month(d)"))
    val planted = (x: Case) =>
      if (x.outputs.contains("datediff(d2, d)") && x.data.nonEmpty) Some("values differ") else None
    val small = shrink(c, "values differ", planted)
    assert(small.outputs == Vector("datediff(d2, d)"), small.outputs)
    assert(small.conjuncts.isEmpty && small.data.size == 1, small)
  }

  test("a reproducer parses back to what was rendered") {
    val r = Reproducer("known VARKA-999", "seed 1 iteration 2", ansi = true, "values differ",
      "datediff(d2, d) AS c0", Some("d IS NOT NULL"), fixtureSql(Seq(Seq("1", "2"))))
    assert(parse(render(r)) == r)
    val noFilter = r.copy(where = None)
    assert(parse(render(noFilter)) == noFilter)
  }

  test("a pivot case's rectified filter selects its pivot row on the row engine") {
    // Sixty compositions with a pivot: the filter, rectified, must return the pivot's outputs
    // from the whole table. A conjunct that raised on the pivot is dropped, so a case may
    // end without a filter; that case is not a PQS case and is skipped.
    var checked = 0
    for (it <- 0 until 300 if checked < 60) {
      val drawn = draw(rows, seed, it)
      if (drawn.pivot.isDefined) {
        val c = rectify(drawn)
        if (c.conjuncts.nonEmpty) {
          assert(kindOf(c, partitions = false, Nil).isEmpty, s"iteration $it: ${c.query}")
          checked += 1
        }
      }
    }
    assert(checked == 60, s"only $checked pivot cases with a filter in 300 compositions")
  }

  test("the draw reaches pivot cases and values outside the fixtures' lists") {
    val cases = (0 until 120).map(draw(rows, seed, _))
    val pivots = cases.count(_.pivot.isDefined)
    assert(pivots >= 10, s"$pivots pivot cases in 120")
    assert(cases.filter(_.pivot.isDefined).forall(c => c.data.contains(c.pivot.get)))
    val cells = cases.flatMap(_.data.flatten).toSet
    // A date outside the fixtures' lists, and a time with a non-listed fraction.
    val listed = Set("2024-01-31", "2024-02-29", "2023-12-27", "2021-01-01", "2021-06-01",
      "2021-03-15", "1969-12-31", "1970-01-01", "2000-02-29", "1999-12-31", "2021-11-01",
      "9999-12-01", "0001-01-15")
    val dates = cells.filter(_.startsWith("DATE'")).map(_.stripPrefix("DATE'").stripSuffix("'"))
    assert((dates -- listed).size >= 50, s"${(dates -- listed).size} dates outside the lists")
  }

  test("the wider pool leaves the compositions of the main stream as they were") {
    // Iterations 0 and 1 draw no wide data and no pivot: their data is the old stream's.
    val c = draw(rows, seed, 0)
    assert(c.pivot.isEmpty)
    val again = draw(rows, seed, 0)
    assert(c == again)
    val wideIteration = draw(rows, seed, 2)
    assert(wideIteration.outputs == draw(rows, seed, 2).outputs)
  }

  test("a case that lacks its pivot is reported, and the shrinker keeps the pivot row") {
    assert(pivotMissing(Right(Seq("a", "b")), Right(Seq("a")), Right(Seq("b")))
      .contains("pivot row not selected by Varka"))
    assert(pivotMissing(Right(Seq("a")), Right(Seq("a", "b")), Right(Seq("b")))
      .contains("pivot row not selected by the row engine"))
    assert(pivotMissing(Right(Seq("b")), Right(Seq("b")), Right(Seq("b"))).isEmpty)
    // An error on another row says nothing about the pivot.
    assert(pivotMissing(Left("X"), Right(Seq("b")), Right(Seq("b"))).isEmpty)
    assert(pivotMissing(Right(Seq("a")), Left("X"), Right(Seq("b")))
      .contains("pivot row not selected by the row engine"))
    assert(pivotMissing(Left("X"), Left("Y"), Right(Seq("b"))).isEmpty)
    val c = rectify(draw(rows, seed, 1)).copy(pivot = None)
    val data = Vector.tabulate(8)(k => Vector.fill(12)(k.toString))
    val withPivot = c.copy(data = data, pivot = Some(data(5)))
    val kept = shrink(withPivot, "values differ", _ => Some("values differ"))
    assert(kept.data.contains(data(5)) && kept.data.size == 1, kept.data)
  }

  test("a reproducer with a forced output parses back to what was rendered") {
    val r = Reproducer("regression", "seed 1 iteration 4", ansi = false,
      "with an output forced to decline, values differ", "year(d) AS c0, i + 1 AS c1", None,
      fixtureSql(Seq(Seq("1", "2"))), None, Some(1))
    assert(parse(render(r)) == r)
  }

  test("a forced output finds a rollback that truncates one input too far") {
    // `misdescribeRollback` 3 makes a declining entry's rollback drop the input before its own,
    // which changes answers; the unforced comparison meets that branch too rarely to see it
    // (VARKA-296.md 2), and forcing every output of each composition does. Some compositions
    // fail to emit under the planted rollback instead, and the row path answers them: a failure
    // fallback this test causes, so it declares one (VARKA-275).
    val previous = VarkaColumnarToRowExec.currentEmitOptions
    VarkaColumnarToRowExec.setEmitOptionsForTesting(previous.withMisdescribeRollback(3))
    VarkaColumnarToRowExec.setFailureFallbackExpectedForTesting(true)
    try {
      val found = (0 until 300).iterator.map { it =>
        val c = rectify(draw(rows, seed, it))
        kindOf(c, partitions = false, c.outputs.indices)
      }.collectFirst { case Some(kind) => kind }
      assert(found.exists(_.startsWith("with an output forced to decline")), found)
    } finally {
      VarkaColumnarToRowExec.setFailureFallbackExpectedForTesting(false)
      VarkaColumnarToRowExec.setEmitOptionsForTesting(previous)
    }
  }

  test("a reproducer with a pivot parses back to what was rendered") {
    val r = Reproducer("regression", "seed 1 iteration 4", ansi = false,
      "pivot row not selected by Varka", "d AS c0", Some("(d) IS NULL"),
      fixtureSql(Seq(Seq("1", "2"))), Some("(1, 2)"))
    assert(parse(render(r)) == r)
  }
}
