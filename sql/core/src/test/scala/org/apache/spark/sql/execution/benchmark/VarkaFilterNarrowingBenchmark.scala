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

package org.apache.spark.sql.execution.benchmark

import scala.concurrent.duration._

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.VarkaColumnarRule
import org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

/**
 * What it costs a Varka filter to narrow its output (milestone 5, task 145's baseline).
 *
 * `VarkaFilterColumnarToRowExec` carries `narrowing`: the projection above it, absorbed, and
 * `Some` exactly when the node's output differs from the columns it was given. Task 144's
 * crossed experiment found queries that differ only in that clause running eight to ten times
 * apart, and read it as the cost of *forwarding* a column. This file is the separation that
 * says otherwise, and it is committed rather than run once because every number a plan quotes
 * has to trace to a results file.
 *
 * The cases vary one thing at a time over one cached table, at a fixed predicate and two
 * selectivities:
 *
 *   A  one column, filtered and output       - no narrowing
 *   B  two columns, the other one output     - narrowing
 *   C  two columns, both output              - no narrowing, and the case that decides it
 *   D  two columns, both in the predicate    - narrowing
 *   E  one column, no output at all          - an aggregate above the filter, a third path
 *   F  A at about one per cent selectivity
 *   G  B at about one per cent selectivity
 *
 * C is the control the first reading lacked: it reads and returns two columns and should cost
 * what A costs if the column count is not the driver. F and G are what separate a per-input-row
 * cost from a per-surviving-row one.
 *
 * What this cannot say, and task 145 must: the absorbed projection replaced a `Project` above
 * the node, which pays an operator boundary and converts the discarded column too, so a gap
 * against a query that needs no projection at all is not evidence against absorption. The
 * un-absorbed arm needs a switch the rule does not have, and is that task's first commit.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain
 *     org.apache.spark.sql.execution.benchmark.VarkaFilterNarrowingBenchmark"
 * }}}
 */
object VarkaFilterNarrowingBenchmark extends SqlBasedBenchmark {

  private val numRows = 20000000

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val varka = SparkSession.builder()
      .master("local[1]")
      .appName("varka-filter-narrowing")
      .config(UI_ENABLED.key, false)
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 1)
      .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .config(StaticSQLConf.SPARK_CACHE_SERIALIZER.key,
        classOf[ArrowCachedBatchSerializer].getName)
      .config(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key, "true")
      .config(SQLConf.VARKA_ENABLED.key, "true")
      .withExtensions(_.injectColumnar(_ => VarkaColumnarRule))
      .getOrCreate()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    try {
      varka.sql(
        s"""select cast(id % 100000 as int) as i,
           |       cast((id * 7) % 100000 as int) as i2
           |from range(0, $numRows)""".stripMargin)
        .createOrReplaceTempView("varka_narrowing")
      varka.catalog.cacheTable("varka_narrowing")
      varka.sql("select count(*) from varka_narrowing").collect()

      val cases = Seq(
        "A one column, filtered and output (no narrowing)" ->
          "SELECT i FROM varka_narrowing WHERE i > 50000",
        "B two columns, the other output (narrowing)" ->
          "SELECT i2 FROM varka_narrowing WHERE i > 50000",
        "C two columns, both output (no narrowing)" ->
          "SELECT i, i2 FROM varka_narrowing WHERE i > 50000",
        "D two columns, both in the predicate (narrowing)" ->
          "SELECT i FROM varka_narrowing WHERE i > 50000 AND i2 >= 0",
        "E one column, no output (count)" ->
          "SELECT count(*) FROM varka_narrowing WHERE i > 50000",
        "F A at about 1% selectivity" ->
          "SELECT i FROM varka_narrowing WHERE i > 99000",
        "G B at about 1% selectivity" ->
          "SELECT i2 FROM varka_narrowing WHERE i > 99000")

      cases.foreach { case (name, query) =>
        val fused = varka.sql(query).queryExecution.executedPlan.find {
          case _: org.apache.spark.sql.execution.VarkaColumnarToRowExec
              | _: org.apache.spark.sql.execution.VarkaProjectExec
              | _: org.apache.spark.sql.execution.VarkaFilterExec
              | _: org.apache.spark.sql.execution.VarkaFilterColumnarToRowExec => true
          case _ => false
        }.isDefined
        if (!fused) {
          throw new IllegalStateException(s"$name did not fuse: $query")
        }
      }

      runBenchmark("a filter that narrows its output") {
        val benchmark = new Benchmark(s"over $numRows Arrow-cached rows", numRows,
          minNumIters = 5, warmupTime = 2.seconds, minTime = 2.seconds, output = output)
        cases.foreach { case (name, query) =>
          benchmark.addCase(name) { _ => varka.sql(query).noop() }
        }
        benchmark.run()
      }
    } finally {
      varka.stop()
    }
  }
}
