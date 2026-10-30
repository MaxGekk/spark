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
import org.apache.spark.sql.execution.VarkaSharedSessions

/**
 * The floating arithmetic baseline's sections, run at a small scale on both sessions in every
 * build, so a section that no longer parses, raises or that the two sessions answer differently
 * fails a pull request rather than the runner dispatch that times it (VARKA-342 5). Each section
 * runs under its own SQL settings, its mode where it has one. It checks the answers only: whether
 * a section fuses is the benchmark's own check, made when it is timed, since row 342 is meant to
 * start fusing these queries.
 */
class VarkaFloatingArithmeticBaselineQueriesSuite extends QueryTest with VarkaSharedSessions {

  /** Runs `body` with the section's SQL settings on both sessions; a section has at most one. */
  private def withSettings(s: VarkaFloatingArithmeticBaselineBenchmark.Section)(
      body: => Unit): Unit = {
    val settings = s.conf.asScala.toSeq
    assert(settings.size <= 1)
    settings.headOption match {
      case Some((key, value)) => withBothConf(key, value)(body)
      case None => body
    }
  }

  test("every section of the floating arithmetic baseline runs, and both sessions answer alike") {
    val view = VarkaFloatingArithmeticBaselineBenchmark.VIEW
    for (session <- Seq(spark, varkaSpark)) {
      session.sql(VarkaFloatingArithmeticBaselineBenchmark.tableSql(2000))
        .createOrReplaceTempView(view)
      session.catalog.cacheTable(view)
    }
    try {
      val sections = VarkaFloatingArithmeticBaselineBenchmark.sections().asScala
      assert(sections.size === 20)
      for (s <- sections) {
        withClue(s.title) {
          withSettings(s) {
            checkAnswer(varkaSpark.sql(s.query), spark.sql(s.query))
          }
        }
      }
    } finally {
      Seq(spark, varkaSpark).foreach(_.catalog.uncacheTable(view))
    }
  }

  test("no section over the null-free columns answers a null") {
    val view = VarkaFloatingArithmeticBaselineBenchmark.VIEW
    spark.sql(VarkaFloatingArithmeticBaselineBenchmark.tableSql(2000))
      .createOrReplaceTempView(view)
    for (s <- VarkaFloatingArithmeticBaselineBenchmark.sections().asScala
        if s.title.endsWith("no nulls")) {
      withClue(s.title) {
        withSettings(s) {
          assert(spark.sql(s.query).where("a IS NULL").count() === 0)
        }
      }
    }
  }
}
