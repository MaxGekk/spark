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
  private val partitionEvery = 4
  private var firstFailure: Option[Int] = None

  private def kindOf(c: Case, partitions: Boolean): Option[String] =
    disagreement(c.fixture, c.select, c.where, c.ansi, partitions)

  test(s"random compositions agree with the row engine (seed $seed, $iterations of them)") {
    val failures = scala.collection.mutable.ArrayBuffer.empty[String]
    val pool = scala.collection.mutable.TreeMap.empty[String, (Int, Int)]
    val range = onlyIteration.map(k => k to k).getOrElse(0 until iterations)
    for (it <- range if failures.size < 3) {
      val c = draw(rows, seed, it)
      val partitions = it % partitionEvery == 0
      val errorsBefore = bothErrored
      val result = kindOf(c, partitions)
      val key = (if (it % 2 == 0) "safe" else "full") + (if (c.ansi) " ANSI on" else " ANSI off")
      pool(key) = pool.getOrElse(key, (0, 0)) match {
        case (n, e) => (n + 1, e + (if (bothErrored > errorsBefore) 1 else 0))
      }
      result.foreach { kind =>
        val small = shrink(c, kind, kindOf(_, partitions))
        val text = render(Reproducer("regression", s"seed $seed iteration $it", small.ansi, kind,
          small.select, small.where, small.fixture))
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
}
