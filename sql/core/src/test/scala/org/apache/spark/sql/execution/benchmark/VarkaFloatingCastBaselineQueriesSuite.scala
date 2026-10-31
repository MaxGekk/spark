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
import org.apache.spark.sql.types._

/**
 * The floating cast baseline's sections, run at a small scale on both sessions in every build, so
 * a section that no longer parses, raises, changes its result type or that the two sessions
 * answer differently fails a pull request rather than the runner dispatch that times it
 * (VARKA-343 5). Each section runs under its own SQL settings, its mode where it has one. It
 * checks the answers only: whether a section fuses is the benchmark's own check, made when it is
 * timed, since row 343 is meant to start fusing these queries.
 *
 * The fixture is the benchmark's table over its first 2000 ids. Its columns are hashes of the
 * id, so these rows already reach the regions the benchmark's javadoc states, which the suite
 * checks before it runs the sections: `l` past `2^53` on both sides and below `2^24`, `i` over
 * both halves of the int range, `x` inside the int range with its zeros, `w` with a fraction, past
 * `2^53` inside the long range and past the long range, and `s` inside the day. A section over
 * the columns with no null must answer no null, except `try_cast`, which must answer null exactly
 * on the rows of `w` past the long range, so a value that left its type into a null fails here
 * rather than going into the timed run as a cheap null.
 */
class VarkaFloatingCastBaselineQueriesSuite extends QueryTest with VarkaSharedSessions {

  private val rows = 2000

  /** The result type of each cast, in the order `sections()` lists them for one density. */
  private val resultTypes = Seq(FloatType, DoubleType, DoubleType, IntegerType, IntegerType,
    LongType, FloatType, DoubleType, TimeType(TimeType.MICROS_PRECISION))

  /** Runs `body` with the section's SQL settings on both sessions; a section has at most one. */
  private def withSettings(s: VarkaFloatingCastBaselineBenchmark.Section)(
      body: => Unit): Unit = {
    val settings = s.conf.asScala.toSeq
    assert(settings.size <= 1)
    settings.headOption match {
      case Some((key, value)) => withBothConf(key, value)(body)
      case None => body
    }
  }

  test("every section of the floating cast baseline runs, and both sessions answer alike") {
    val view = VarkaFloatingCastBaselineBenchmark.VIEW
    val sessions = Seq(spark, varkaSpark)
    try {
      for (session <- sessions) {
        session.sql(VarkaFloatingCastBaselineBenchmark.tableSql(rows))
          .createOrReplaceTempView(view)
        session.catalog.cacheTable(view)
      }
      val longEdge = "9.223372036854775807E18"
      val r = spark.sql(
        s"""select min(l) < -power(2, 53), max(l) > power(2, 53),
           |       count(l > -power(2, 24) and l < power(2, 24)),
           |       min(i) < -power(2, 30), max(i) > power(2, 30),
           |       max(abs(x)) <= power(2, 31) / 7, count(if(x = 0.0d, 1, null)),
           |       count(if(abs(w) > $longEdge, 1, null)),
           |       count(if(w % 1 <> 0, 1, null)),
           |       count(if(abs(w) > power(2, 53) and abs(w) <= $longEdge, 1, null)),
           |       min(s) >= 0, max(s) < 86400, max(s) > 86000
           |from $view""".stripMargin).head()
      assert(r.getBoolean(0) && r.getBoolean(1))
      assert(r.getLong(2) > 0)
      assert(r.getBoolean(3) && r.getBoolean(4))
      assert(r.getBoolean(5))
      assert(r.getLong(6) === rows / 4)
      val overflows = r.getLong(7)
      assert(overflows > rows / 20 && overflows < rows / 5)
      assert(r.getLong(8) > 0 && r.getLong(9) > 0)
      assert(r.getBoolean(10) && r.getBoolean(11) && r.getBoolean(12))

      val sections = VarkaFloatingCastBaselineBenchmark.sections().asScala.toSeq
      assert(sections.size === 2 * resultTypes.size)
      for ((s, index) <- sections.zipWithIndex) {
        withClue(s.title) {
          withSettings(s) {
            val answer = spark.sql(s.query)
            assert(answer.schema.map(_.name) === Seq("a"))
            assert(answer.schema.head.dataType === resultTypes(index % resultTypes.size))
            checkAnswer(varkaSpark.sql(s.query), answer)
            if (s.title.endsWith("no nulls")) {
              val nulls = if (s.query.contains("try_cast")) overflows else 0L
              assert(answer.where("a IS NULL").count() === nulls)
            }
          }
        }
      }
    } finally {
      for (session <- sessions if session.catalog.tableExists(view)) {
        session.catalog.uncacheTable(view)
        session.catalog.dropTempView(view)
      }
    }
  }
}
