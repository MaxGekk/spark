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

package org.apache.spark.sql

import java.time.LocalDate
import scala.concurrent.duration._
import scala.util.Random

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaGatherProbe

/**
 * What a Vector API capability costs, measured rather than assumed. Not a gate and not a kernel:
 * the emitter cannot emit either side of this. It exists because milestone 4's design decisions
 * lean on beliefs about the Vector API's primitives, and a belief with no number behind it is
 * how a plan goes wrong quietly.
 *
 * The first question is the gather. A widely deployed row engine (Impala) reads `year` out of a
 * day-indexed lookup table covering 1950-2049 and computes it only outside that window, on the
 * argument that the memory is warm. Whether that would pay on lanes decides nothing for `year` -
 * `IntVector`'s index-map overload exists only on `fromArray`, never on `fromMemorySegment`, and
 * every Varka input is an off-heap Arrow buffer - but it does bear on item 9's dictionary decode,
 * where the dictionary genuinely is on-heap and small.
 *
 * Two input shapes, because the answer depends on the working set rather than the table: the
 * whole 100-year window, where the touched table is about 143 KB, and a seven-year span, which is
 * what a TPC-H-shaped date column looks like and touches about 10 KB.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "catalyst/Test/runMain org.apache.spark.sql.VarkaVectorApiProbeBenchmark"
 *   2. generate result:
 *        SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt
 *          "catalyst/Test/runMain org.apache.spark.sql.VarkaVectorApiProbeBenchmark"
 *   3. the four-lane shape:
 *        build/sbt "project catalyst" 'set Test/javaOptions += "-XX:MaxVectorSize=16"'
 *          "Test/runMain org.apache.spark.sql.VarkaVectorApiProbeBenchmark"
 * }}}
 */
object VarkaVectorApiProbeBenchmark extends BenchmarkBase {

  private val numRows = 1 << 20

  /** Passes over the buffer per timed case. One pass is under a millisecond, which the results
   * file's Best Time column cannot resolve, so every case walks the buffer this many times and
   * the declared row count is scaled to match - the same shape the parity benchmark uses. */
  private val repeats = 20

  private def passes(body: () => Unit): Unit = {
    var i = 0
    while (i < repeats) {
      body()
      i += 1
    }
  }

  /** Uniform days over `[first, first + span)`, the shape a date column has. */
  private def days(first: Int, span: Int, seed: Long): Array[Int] = {
    val random = new Random(seed)
    Array.fill(numRows)(first + random.nextInt(span))
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("year: a gather against the arithmetic (design input for item 9)") {
      val out = new Array[Int](numRows)
      val indexScratch = new Array[Int](VarkaGatherProbe.lanes())
      val wholeTable = days(VarkaGatherProbe.MIN_DAY_MAPPED,
        VarkaGatherProbe.MAX_DAY_MAPPED - VarkaGatherProbe.MIN_DAY_MAPPED + 1, 9001)
      val sevenYears = days(LocalDate.of(1992, 1, 1).toEpochDay.toInt, 7 * 365, 9002)

      // A benchmark that measures a wrong answer is worse than none, and both paths here are
      // easy to get subtly wrong - the gather by an off-by-one on the table base, the
      // arithmetic by a transposed carry. Both are checked against java.time before timing.
      for ((input, ctx) <- Seq((wholeTable, "whole table"), (sevenYears, "seven years"))) {
        VarkaGatherProbe.yearByLookup(input, out, indexScratch)
        checkYears(input, out, s"gather, $ctx")
        VarkaGatherProbe.yearByArithmetic(input, out)
        checkYears(input, out, s"arithmetic, $ctx")
        VarkaGatherProbe.yearByScalarLookup(input, out)
        checkYears(input, out, s"scalar lookup, $ctx")
      }

      val benchmark = new Benchmark(
        s"${numRows.toLong * repeats} dates, ${VarkaGatherProbe.lanes()} lanes",
        numRows.toLong * repeats,
        minNumIters = 5, warmupTime = 2.seconds, minTime = 2.seconds, output = output)
      benchmark.addCase("gather from a 143 KB table, dates over the whole 100 years") { _ =>
        passes(() => VarkaGatherProbe.yearByLookup(wholeTable, out, indexScratch))
      }
      benchmark.addCase("arithmetic, dates over the whole 100 years") { _ =>
        passes(() => VarkaGatherProbe.yearByArithmetic(wholeTable, out))
      }
      benchmark.addCase("scalar loop over the same table, whole 100 years") { _ =>
        passes(() => VarkaGatherProbe.yearByScalarLookup(wholeTable, out))
      }
      benchmark.addCase("gather from a 143 KB table, dates over seven years") { _ =>
        passes(() => VarkaGatherProbe.yearByLookup(sevenYears, out, indexScratch))
      }
      benchmark.addCase("arithmetic, dates over seven years") { _ =>
        passes(() => VarkaGatherProbe.yearByArithmetic(sevenYears, out))
      }
      benchmark.addCase("scalar loop over the same table, seven years") { _ =>
        passes(() => VarkaGatherProbe.yearByScalarLookup(sevenYears, out))
      }
      benchmark.run()
    }
  }

  private def checkYears(input: Array[Int], out: Array[Int], ctx: String): Unit = {
    var i = 0
    while (i < input.length) {
      val want = LocalDate.ofEpochDay(input(i).toLong).getYear
      require(out(i) == want,
        s"$ctx disagreed with java.time on day ${input(i)}: got ${out(i)}, want $want")
      i += 1
    }
  }
}
