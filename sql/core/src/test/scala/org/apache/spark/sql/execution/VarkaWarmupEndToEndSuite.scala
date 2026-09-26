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

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.{VarkaKernelWarmth,
  VarkaKernelWarmup, VarkaShapeCache}
import org.apache.spark.sql.internal.SQLConf

/**
 * The kernel warm-up end to end (`spark.sql.codegen.varka.warmup.enabled`, `PLAN_TASK_212.md`
 * 10): a shape's first query serves its batches on the row path while a background thread gets
 * the new kernel compiled, and once the warm-up has its verdict the same shape's next query runs
 * the kernel. Both queries must give the row engine's answers; what the tests pin is which path
 * served each batch, read from the node's metrics.
 *
 * The shared sessions pin the warm-up off, so every test here turns it on for its own queries.
 */
class VarkaWarmupEndToEndSuite extends QueryTest with VarkaSharedSessions {

  private val numRows = 40000

  /** The cache's default batch holds 10,000 rows, so the table is four batches. */
  private val numBatches = 4L

  private def withWarmup[T](body: => T): T = {
    varkaSpark.conf.set(SQLConf.VARKA_WARMUP_ENABLED.key, "true")
    try body finally varkaSpark.conf.set(SQLConf.VARKA_WARMUP_ENABLED.key, "false")
  }

  /**
   * Runs the query once and checks its rows. Not `checkAnswer`, which runs a query twice - the
   * second run would meet a shape the first had already claimed, and the metrics would count
   * both.
   */
  private def runAndCheck(df: DataFrame, expected: Seq[Row]): Unit = {
    QueryTest.getErrorMessageInCheckAnswer(df, expected, checkToRDD = false).foreach(fail(_))
  }

  /** A metric of the query's Varka node, after running it. */
  private def varkaMetric(df: DataFrame, name: String): Long = {
    val node = collectFirst(df.queryExecution.executedPlan) { case v if isVarkaNode(v) => v }
      .getOrElse(fail(s"no Varka node in:\n${df.queryExecution.executedPlan.treeString}"))
    node.metrics(name).value
  }

  /**
   * Runs the query twice on the Varka session with the warm-up on, waiting for the warm-up's
   * verdict in between, and checks both runs against the row engine and which path served them.
   */
  private def checkWarmup(query: String): Unit = {
    cacheDatesBig(spark, numRows)
    val expected = spark.sql(query).collect().toSeq
    cacheDatesBig(varkaSpark, numRows)
    withWarmup {
      // A shape another suite emitted is a class this JVM has already met; start from a new one.
      VarkaShapeCache.invalidateAll()
      val first = varkaSpark.sql(query)
      runAndCheck(first, expected)
      assert(varkaMetric(first, "numWarmupBatches") === numBatches)
      assert(varkaMetric(first, "numVarkaBatches") === 0L)

      assert(VarkaKernelWarmup.awaitIdle(120000), "the warm-up did not finish in two minutes")
      val outcome = VarkaKernelWarmup.recentOutcomes().asScala.last
      logInfo(s"The warm-up of `$query`: $outcome")
      assert(outcome.state() === VarkaKernelWarmth.State.COMPILED, outcome)
      assert(outcome.firstProbeBytes() > outcome.lastProbeBytes(), outcome)

      val second = varkaSpark.sql(query)
      runAndCheck(second, expected)
      assert(varkaMetric(second, "numVarkaBatches") === numBatches)
      assert(varkaMetric(second, "numWarmupBatches") === 0L)
      Seq("numFallbackBatchesNonArrow", "numFallbackBatchesKernel", "numFallbackBatchesRowPath",
        "numFallbackBatchesDeclined").foreach { m =>
        assert(varkaMetric(first, m) === 0L, m)
        assert(varkaMetric(second, m) === 0L, m)
      }
    }
  }

  test("a projection's first query takes the row path and its next one the compiled kernel") {
    checkWarmup(
      "SELECT i, date_add(d, 3) AS a, add_months(d, 2) AS b, last_day(d) AS c " +
        "FROM varka_dates_big")
  }

  test("a filter's first query takes the row path and its next one the compiled kernel") {
    checkWarmup("SELECT i FROM varka_dates_big WHERE date_add(d, 30) > DATE'2020-07-01'")
  }
}
