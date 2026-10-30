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
 * The floating baseline's sections, run at a small scale on both sessions in every build, so a
 * section that no longer parses or that the two sessions answer differently fails a pull request
 * rather than the runner dispatch that times it (VARKA-377 5). It checks the answers only: whether
 * a section fuses is the benchmark's own check, made when it is timed, since a lane row is meant
 * to start fusing these queries.
 */
class VarkaFloatingBaselineQueriesSuite extends QueryTest with VarkaSharedSessions {

  test("every section of the floating baseline runs, and both sessions answer it alike") {
    for (session <- Seq(spark, varkaSpark)) {
      session.sql(VarkaFloatingBaselineBenchmark.tableSql(2000))
        .createOrReplaceTempView("varka_floating")
      session.catalog.cacheTable("varka_floating")
    }
    try {
      val sections = VarkaFloatingBaselineBenchmark.sections().asScala
      assert(sections.size === 16)
      for (s <- sections) {
        withClue(s.title) {
          checkAnswer(varkaSpark.sql(s.query), spark.sql(s.query))
        }
      }
    } finally {
      Seq(spark, varkaSpark).foreach(_.catalog.uncacheTable("varka_floating"))
    }
  }
}
