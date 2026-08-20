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

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.execution.columnar.{ArrowCachedBatchSerializer, InMemoryRelation}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for the Varka columnar execution path (Task 6). Data is cached with the
 * Arrow serializer and the vectorized reader so that `InMemoryTableScanExec` feeds real Arrow
 * `DateDayVector` batches into a columnar-to-row transition, mirroring a production columnar
 * scan. A second session with `VarkaColumnarRule` injected and `spark.sql.codegen.varka.enabled`
 * set must fuse eligible projections into `VarkaColumnarToRowExec` and produce results identical
 * to the row-based engine, while ineligible projections and the disabled config must leave the
 * plan untouched.
 */
class VarkaEndToEndSuite extends QueryTest with SharedSparkSession {

  private var varkaSpark: SparkSession = _
  private var disabledSpark: SparkSession = _

  override protected def sparkConf = {
    super.sparkConf
      .set(StaticSQLConf.SPARK_CACHE_SERIALIZER.key,
        classOf[ArrowCachedBatchSerializer].getName)
      .set(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key, "true")
  }

  // InMemoryRelation caches the serializer instance in a process-wide field that is initialized
  // from spark.sql.cache.serializer only on first use, so reset it here to pick up the Arrow
  // serializer, and reset it again afterwards so we do not leak it to later suites.
  override def beforeAll(): Unit = {
    super.beforeAll()
    InMemoryRelation.clearSerializer()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    disabledSpark = SparkSession.builder()
      .sparkContext(spark.sparkContext)
      .config(StaticSQLConf.SPARK_CACHE_SERIALIZER.key,
        classOf[ArrowCachedBatchSerializer].getName)
      .config(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key, "true")
      .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .config(SQLConf.VARKA_ENABLED.key, "false")
      .withExtensions(_.injectColumnar(_ => VarkaColumnarRule))
      .getOrCreate()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    varkaSpark = SparkSession.builder()
      .sparkContext(spark.sparkContext)
      .config(StaticSQLConf.SPARK_CACHE_SERIALIZER.key,
        classOf[ArrowCachedBatchSerializer].getName)
      .config(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key, "true")
      .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .config(SQLConf.VARKA_ENABLED.key, "true")
      .withExtensions(_.injectColumnar(_ => VarkaColumnarRule))
      .getOrCreate()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  override def afterAll(): Unit = {
    InMemoryRelation.clearSerializer()
    varkaSpark = null
    disabledSpark = null
    super.afterAll()
  }

  private def date(value: String): java.sql.Date = java.sql.Date.valueOf(value)

  private def cacheDates(session: SparkSession): Unit = {
    val dates = Seq(date("2024-01-01"), date("2024-01-02"), date("2023-12-27"),
      date("1969-12-31"), null).zipWithIndex
    session.createDataFrame(dates.map { case (d, i) => (d, i) }).toDF("d", "i")
      .createOrReplaceTempView("varka_dates")
    session.catalog.cacheTable("varka_dates")
  }

  private def cacheDatePairs(session: SparkSession): Unit = {
    val dates = Seq(
      (date("2024-03-01"), date("2024-01-01")),
      (date("2024-01-01"), date("2024-01-01")),
      (date("2023-12-01"), date("2024-01-01")),
      (null, date("2024-01-01")),
      (date("2024-01-01"), null)).zipWithIndex
    session.createDataFrame(dates.map { case ((d, d2), i) => (d, d2, i) })
      .toDF("d", "d2", "i")
      .createOrReplaceTempView("varka_date_pairs")
    session.catalog.cacheTable("varka_date_pairs")
  }

  private def assertKernelsRan(plan: SparkPlan): Unit = {
    val node = plan.collectFirst { case v: VarkaColumnarToRowExec => v }.get
    val varkaBatches = node.metrics.get("numVarkaBatches").map(_.value).getOrElse(0L)
    assert(varkaBatches > 0L,
      s"expected the SIMD kernels to process the cached Arrow batches, got $varkaBatches")
  }

  private def assertFused(plan: SparkPlan): Unit = {
    assert(plan.find(_.isInstanceOf[VarkaColumnarToRowExec]).isDefined,
      s"expected a VarkaColumnarToRowExec in the plan:\n${plan.treeString}")
  }

  private def assertNotFused(plan: SparkPlan): Unit = {
    assert(plan.find(_.isInstanceOf[VarkaColumnarToRowExec]).isEmpty,
      s"expected no VarkaColumnarToRowExec in the plan:\n${plan.treeString}")
  }

  test("date_add and date_sub over a cached Arrow source are fused and match the row engine") {
    cacheDates(spark)
    val query =
      "SELECT date_add(d, 3) AS a, date_sub(d, 5) AS b FROM varka_dates ORDER BY a"
    val expected = spark.sql(query)
    cacheDates(varkaSpark)
    val actual = varkaSpark.sql(query)
    val plan = actual.queryExecution.executedPlan
    assertFused(plan)
    checkAnswer(actual, expected)
    assertKernelsRan(plan)
  }

  test("datediff over a cached Arrow source is fused and matches the row engine") {
    cacheDatePairs(spark)
    val query = "SELECT datediff(d2, d) AS diff FROM varka_date_pairs ORDER BY diff"
    val expected = spark.sql(query)
    cacheDatePairs(varkaSpark)
    val actual = varkaSpark.sql(query)
    val plan = actual.queryExecution.executedPlan
    assertFused(plan)
    checkAnswer(actual, expected)
    assertKernelsRan(plan)
  }

  test("a non-foldable offset is not fused but still returns correct results") {
    cacheDates(spark)
    val query = "SELECT date_add(d, i) AS a FROM varka_dates ORDER BY a"
    val expected = spark.sql(query)
    cacheDates(varkaSpark)
    val actual = varkaSpark.sql(query)
    assertNotFused(actual.queryExecution.executedPlan)
    checkAnswer(actual, expected)
  }

  test("the varka config gate keeps the plan untouched when disabled") {
    cacheDates(spark)
    val query = "SELECT date_add(d, 3) AS a FROM varka_dates ORDER BY a"
    val expected = spark.sql(query)
    cacheDates(disabledSpark)
    val actual = disabledSpark.sql(query)
    assertNotFused(actual.queryExecution.executedPlan)
    checkAnswer(actual, expected)
  }
}