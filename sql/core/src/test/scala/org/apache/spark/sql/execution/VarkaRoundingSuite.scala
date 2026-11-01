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

import scala.util.Try

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaTestWatchdog
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType, StructField,
  StructType}

/**
 * The rounding functions over integral operands (VARKA-344) against the row engine, end to end:
 * `floor`, `ceil` and `ceiling` over `BIGINT` and `INT`, and `round`, `bround` and `truncate`
 * over `TINYINT`, `SMALLINT`, `INT` and `BIGINT`, at the cells of `VARKA-344.md` 3.1 - the ties,
 * the ends of ANSI's windows, the extremes of each type, the `2^52` bound of the long division
 * and the scales past the type. Under LEGACY the whole table fuses and answers as the row engine
 * does; under ANSI every cell is run on its own, and Varka's answer or error is the row engine's,
 * a cell Spark raises on reaching it through the declined batch.
 */
// varka-session-lint: allow the cells of the plan's 3.1 through the cached Arrow path, each
// raising cell its own query, so the error is the row engine's after the kernel declines its batch
class VarkaRoundingSuite extends QueryTest with VarkaSharedSessions with VarkaTestWatchdog {

  private val table = "varka_round_cells"

  private val ints: Seq[java.lang.Integer] = Seq[Int](0, 1, -1, 4, 5, -5, 15, -15, 25, -25,
    35, 149, 150, -150, 250, -250, 1234567895, -1234567895, 2147483640, 2147483644, -2147483644,
    2147483645, -2147483645, 2147483646, Int.MaxValue, Int.MinValue).map(Int.box)

  private val two52 = 1L << 52
  private val longs: Seq[java.lang.Long] = Seq[Long](0, 5, -5, 1500, -1500, 2500, -2500, 3499,
    3500, -3500, two52 - 1, -(two52 - 1), two52, -two52, two52 + 500, 4999999999999999999L,
    -4999999999999999999L, 5000000000000000000L, -5000000000000000000L, 5000000000000000001L,
    -5000000000000000001L, Long.MaxValue, Long.MinValue, 123456789012345678L).map(Long.box)

  private val bytes: Seq[java.lang.Byte] =
    Seq[Int](0, 5, -5, 15, -15, 25, 124, -124, 125, -125, 127, -128).map(v => Byte.box(v.toByte))

  private val shorts: Seq[java.lang.Short] = Seq[Int](0, 50, -50, 150, -150, 250, 32749, -32749,
    32750, -32750, 32767, -32768).map(v => Short.box(v.toShort))

  /** One row per index, each column cycling through its own cells, a null at every seventh. */
  private def rows: Seq[Row] = {
    val n = Seq(ints, longs, bytes, shorts).map(_.size).max * 2
    (0 until n).map { k =>
      def at[T <: AnyRef](cells: Seq[T], shift: Int): T =
        if ((k + shift) % 7 == 6) null.asInstanceOf[T] else cells(k % cells.size)
      Row(k, at(ints, 0), at(longs, 1), at(bytes, 2), at(shorts, 3))
    }
  }

  private val schema = StructType(Seq(StructField("id", IntegerType),
    StructField("i", IntegerType), StructField("l", LongType), StructField("y", ByteType),
    StructField("s", ShortType)))

  private def cacheTable(session: SparkSession): Unit = {
    session.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
      .createOrReplaceTempView(table)
    session.catalog.cacheTable(table)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    cacheTable(spark)
    cacheTable(varkaSpark)
  }

  private val forms = Seq(
    "floor(l)", "ceil(l)", "ceiling(l)", "floor(i)", "ceil(i)",
    "round(i, 0)", "round(i, -1)", "bround(i, -1)", "truncate(i, -1)", "round(i, -2)",
    "bround(i, -2)", "truncate(i, -2)", "round(i, -9)", "bround(i, -9)", "round(i, -10)",
    "round(y, -1)", "bround(y, -1)", "truncate(y, -1)", "round(y, -2)", "round(y, -3)",
    "round(s, -2)", "bround(s, -2)", "truncate(s, -3)", "round(s, -4)",
    "round(l, 0)", "round(l, -1)", "bround(l, -3)", "truncate(l, -3)", "round(l, -3)",
    "round(l, -18)", "round(l, -19)", "bround(l, -19)", "truncate(l, -19)", "round(l, -20)")

  /** The forms ANSI raises on for some cell, each run cell by cell. */
  private val raising = Set("round(i, -1)", "bround(i, -1)", "round(y, -1)", "bround(y, -1)",
    "round(s, -2)", "bround(s, -2)", "round(l, -1)", "round(l, -3)", "bround(l, -3)",
    "round(l, -18)", "round(l, -19)", "bround(l, -19)")

  test("LEGACY: every form fuses and answers as the row engine over every cell") {
    withAnsi(false) {
      forms.foreach { form =>
        val q = s"SELECT id, $form AS v FROM $table"
        val actual = varkaSpark.sql(q)
        withClue(s"$q: ") {
          assertFused(actual.queryExecution.executedPlan)
          checkAnswer(actual, expectedSql(q, actual))
        }
      }
    }
  }

  test("ANSI: a form no cell raises on fuses and answers as the row engine over every cell") {
    withAnsi(true) {
      forms.filterNot(raising.contains).foreach { form =>
        val q = s"SELECT id, $form AS v FROM $table"
        val actual = varkaSpark.sql(q)
        withClue(s"$q: ") {
          assertFused(actual.queryExecution.executedPlan)
          checkAnswer(actual, expectedSql(q, actual))
        }
      }
    }
  }

  test("ANSI: a form that raises answers or raises as the row engine on each cell") {
    withAnsi(true) {
      forms.filter(raising.contains).foreach { form =>
        assertFused(varkaSpark.sql(s"SELECT $form AS v FROM $table").queryExecution.executedPlan)
        rows.indices.foreach { id =>
          val one = s"SELECT $form AS v FROM $table WHERE id = $id"
          def outcome(session: SparkSession): Either[String, Seq[Row]] =
            Try(session.sql(one).collect().toSeq).toEither.left.map {
              case e: SparkThrowable => e.getCondition
              case e => throw e
            }
          withClue(s"$one: ") {
            assert(outcome(varkaSpark) === outcome(spark))
          }
        }
      }
    }
  }

  test("the cells of VARKA-344.md 3.1 answer as the plan states") {
    def one(session: SparkSession, sql: String): Any = session.sql(sql).collect().head.get(0)
    withAnsi(false) {
      assert(one(varkaSpark, s"SELECT round(i, -1) FROM $table WHERE i = 2147483645") ===
        -2147483646)
      assert(one(varkaSpark, s"SELECT bround(i, -1) FROM $table WHERE i = 2147483645") ===
        2147483640)
      assert(one(varkaSpark, s"SELECT round(l, -19) FROM $table WHERE l = 5000000000000000000")
        === -8446744073709551616L)
      assert(one(varkaSpark, s"SELECT bround(l, -19) FROM $table WHERE l = 5000000000000000000")
        === 0L)
      assert(one(varkaSpark, s"SELECT bround(l, -19) FROM $table WHERE l = 5000000000000000001")
        === -8446744073709551616L)
    }
  }

  test("floor and ceil over a double still decline, naming the row that builds them") {
    val q = s"SELECT floor(CAST(i AS DOUBLE) / 3.0D) AS v FROM $table"
    assertNotFused(varkaSpark.sql(q).queryExecution.executedPlan)
  }
}
