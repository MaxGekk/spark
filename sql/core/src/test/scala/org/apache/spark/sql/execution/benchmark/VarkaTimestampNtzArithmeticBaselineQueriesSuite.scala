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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaChrono
import org.apache.spark.sql.execution.VarkaSharedSessions
import org.apache.spark.sql.types.{DayTimeIntervalType, IntegerType, TimestampNTZType}

/**
 * The TIMESTAMP_NTZ arithmetic baseline's sections, run at a small scale on both sessions in every
 * build, so a section that no longer parses, that raises, or that the two sessions answer
 * differently fails a pull request rather than the runner dispatch that times it (VARKA-339 5,
 * step 1). It checks the answers only: whether a section fuses is the benchmark's own check, made
 * when it is timed, since the row's later steps are meant to start fusing these queries.
 *
 * Two further checks keep the table what the benchmark claims: every column has the declared
 * type, and a section over the columns with no null returns no null. Every section is collected
 * on both sessions, so one whose values left the range Spark computes in would raise here
 * (DATETIME_OVERFLOW or ARITHMETIC_OVERFLOW) rather than in the timed run.
 */
class VarkaTimestampNtzArithmeticBaselineQueriesSuite extends QueryTest with VarkaSharedSessions {

  test("every section of the TIMESTAMP_NTZ arithmetic baseline runs, and both sessions answer " +
    "it alike") {
    for (session <- Seq(spark, varkaSpark)) {
      session.sql(VarkaTimestampNtzArithmeticBaselineBenchmark.tableSql(2000))
        .createOrReplaceTempView("varka_ntz_arith")
      session.catalog.cacheTable("varka_ntz_arith")
    }
    try {
      val dt = DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.SECOND)
      val columns = Seq("ntz" -> TimestampNTZType, "ntz2" -> TimestampNTZType,
        "ntz3" -> TimestampNTZType, "dt" -> dt, "q" -> IntegerType)
      val expected = columns ++ columns.map { case (name, t) => (name + "n") -> t }
      val schema = spark.table("varka_ntz_arith").schema
      assert(schema.map(f => f.name -> f.dataType) === expected)

      val sections = VarkaTimestampNtzArithmeticBaselineBenchmark.sections().asScala
      assert(sections.size === 12)
      // Every shape keeps the timestamp's type: the interval sums, timestampadd and time_bucket
      // all answer a TIMESTAMP_NTZ for a TIMESTAMP_NTZ operand.
      assert(sections.map(s => spark.sql(s.query).schema.head.dataType) ===
        Seq.fill(sections.size)(TimestampNTZType))
      for (s <- sections) {
        withClue(s.title) {
          val answer = spark.sql(s.query)
          checkAnswer(varkaSpark.sql(s.query), answer)
          if (s.title.endsWith("no nulls")) {
            assert(answer.where(answer.columns.head + " IS NULL").count() === 0)
          }
        }
      }
    } finally {
      Seq(spark, varkaSpark).foreach(_.catalog.uncacheTable("varka_ntz_arith"))
    }
  }

  test("no section of the TIMESTAMP_NTZ arithmetic baseline raises at the ends of its values") {
    import VarkaTimestampNtzArithmeticBaselineBenchmark._
    // The month form's day guard admits these days, so the after-run's month section fuses whole.
    assert(-MAX_TIMESTAMP_DAYS > VarkaChrono.NARROW_MIN_DAYS)
    assert(MAX_TIMESTAMP_DAYS < VarkaChrono.NARROW_MAX_DAYS)
    // The hash draws a whole number of days within the bound and a time of day in [0, 1 day), so
    // these two rows hold each column's least and greatest values, paired so the chain's
    // ntz + (ntz2 - ntz3) reaches its own least and greatest sums.
    def dayTime(days: Long, high: Boolean): String =
      s"make_dt_interval($days, 0, 0, ${if (high) "86399.999999" else "0"})"
    def row(sign: Long): String = {
      val high = sign > 0
      val epoch = "timestamp_ntz'1970-01-01 00:00:00' + "
      val ntz = epoch + dayTime(sign * MAX_TIMESTAMP_DAYS, high)
      val far = epoch + dayTime(-sign * MAX_TIMESTAMP_DAYS, !high)
      val dt = dayTime(sign * MAX_INTERVAL_DAYS, high)
      val q = s"cast(${sign * MAX_HOURS} as int)"
      Seq(ntz -> "ntz", ntz -> "ntz2", far -> "ntz3", dt -> "dt", q -> "q")
        .flatMap { case (v, n) => Seq(s"$v as $n", s"$v as ${n}n") }
        .mkString("select ", ", ", "")
    }
    // Cached, as the timed table is: an uncached view of two literal rows is folded at planning,
    // so the extreme values would never reach the cached scan a kernel reads.
    for (session <- Seq(spark, varkaSpark)) {
      session.sql(row(1) + " union all " + row(-1)).createOrReplaceTempView("varka_ntz_arith")
      session.catalog.cacheTable("varka_ntz_arith")
    }
    try {
      for (s <- sections().asScala) {
        withClue(s.title) {
          val answer = spark.sql(s.query)
          checkAnswer(varkaSpark.sql(s.query), answer)
          assert(answer.where(answer.columns.head + " IS NULL").count() === 0)
        }
      }
    } finally {
      Seq(spark, varkaSpark).foreach(_.catalog.uncacheTable("varka_ntz_arith"))
    }
  }
}
