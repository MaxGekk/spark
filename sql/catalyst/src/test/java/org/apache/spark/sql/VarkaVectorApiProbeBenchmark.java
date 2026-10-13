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

package org.apache.spark.sql;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Random;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaGatherProbe;

/**
 * What a Vector API capability costs, measured rather than assumed. Not a gate and not a kernel:
 * the emitter cannot emit either side of this. It exists because milestone 4's design decisions
 * lean on beliefs about the Vector API's primitives, and a belief with no number behind it is how
 * a plan goes wrong quietly.
 *
 * <p>The first question is the gather. A widely deployed row engine (Impala) reads {@code year}
 * out of a day-indexed lookup table covering 1950-2049 and computes it only outside that window,
 * on the argument that the memory is warm. Whether that would pay on lanes decides nothing for
 * {@code year} - {@code IntVector}'s index-map overload exists only on {@code fromArray}, never on
 * {@code fromMemorySegment}, and every Varka input is an off-heap Arrow buffer - but it does bear
 * on item 9's dictionary decode, where the dictionary genuinely is on-heap and small.
 *
 * <p>Two input shapes, because the answer depends on the working set rather than the table: the
 * whole 100-year window, where the touched table is about 143 KB, and a seven-year span, which is
 * what a TPC-H-shaped date column looks like and touches about 10 KB.
 *
 * <p>To run this benchmark:
 * <pre>
 *   1. build/sbt "catalyst/Test/runMain org.apache.spark.sql.VarkaVectorApiProbeBenchmark"
 *   2. generate result:
 *        SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt
 *          "catalyst/Test/runMain org.apache.spark.sql.VarkaVectorApiProbeBenchmark"
 *   3. the four-lane shape:
 *        build/sbt "project catalyst" 'set Test/javaOptions += "-XX:MaxVectorSize=16"'
 *          "Test/runMain org.apache.spark.sql.VarkaVectorApiProbeBenchmark"
 * </pre>
 */
public class VarkaVectorApiProbeBenchmark extends VarkaBenchmarkBase {

  private static final int NUM_ROWS = 1 << 20;

  /**
   * Passes over the buffer per timed case. One pass is under a millisecond, which the results
   * file's Best Time column cannot resolve, so every case walks the buffer this many times and the
   * declared row count is scaled to match - the same shape the parity benchmark uses.
   */
  private static final int REPEATS = 20;

  private static final int TARGET = 1998;

  private static void passes(Runnable body) {
    int i = 0;
    while (i < REPEATS) {
      body.run();
      i += 1;
    }
  }

  /** Uniform days over {@code [first, first + span)}, the shape a date column has. */
  private static int[] days(int first, int span, long seed) {
    var random = new Random(seed);
    int[] out = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      out[i] = first + random.nextInt(span);
    }
    return out;
  }

  private static int sevenYearsStart() {
    return (int) LocalDate.of(1992, 1, 1).toEpochDay();
  }

  private static long countYear(int[] input, int target) {
    long count = 0;
    for (int d : input) {
      if (LocalDate.ofEpochDay(d).getYear() == target) {
        count++;
      }
    }
    return count;
  }

  private static VarkaBenchmark.Builder timed(VarkaBenchmark.Builder builder) {
    return builder.minIterations(5).warmup(Duration.ofSeconds(2)).minTime(Duration.ofSeconds(2));
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    section("year: a gather against the arithmetic (design input for item 9)", () -> {
      int[] out = new int[NUM_ROWS];
      int[] indexScratch = new int[VarkaGatherProbe.lanes()];
      int[] wholeTable = days(VarkaGatherProbe.MIN_DAY_MAPPED,
          VarkaGatherProbe.MAX_DAY_MAPPED - VarkaGatherProbe.MIN_DAY_MAPPED + 1, 9001);
      int[] sevenYears = days(sevenYearsStart(), 7 * 365, 9002);

      // A benchmark that measures a wrong answer is worse than none, and both paths here are
      // easy to get subtly wrong - the gather by an off-by-one on the table base, the
      // arithmetic by a transposed carry. Both are checked against java.time before timing.
      for (int which = 0; which < 2; which++) {
        int[] input = which == 0 ? wholeTable : sevenYears;
        String ctx = which == 0 ? "whole table" : "seven years";
        VarkaGatherProbe.yearByLookup(input, out, indexScratch);
        checkYears(input, out, "gather, " + ctx);
        VarkaGatherProbe.yearByArithmetic(input, out);
        checkYears(input, out, "arithmetic, " + ctx);
        VarkaGatherProbe.yearByScalarLookup(input, out);
        checkYears(input, out, "scalar lookup, " + ctx);
      }

      long values = (long) NUM_ROWS * REPEATS;
      var benchmark = timed(benchmark(
          values + " dates, " + VarkaGatherProbe.lanes() + " lanes", values)).build();
      benchmark.addCase("gather from a 143 KB table, dates over the whole 100 years",
          iteration -> passes(() -> VarkaGatherProbe.yearByLookup(wholeTable, out, indexScratch)));
      benchmark.addCase("arithmetic, dates over the whole 100 years",
          iteration -> passes(() -> VarkaGatherProbe.yearByArithmetic(wholeTable, out)));
      benchmark.addCase("scalar loop over the same table, whole 100 years",
          iteration -> passes(() -> VarkaGatherProbe.yearByScalarLookup(wholeTable, out)));
      benchmark.addCase("gather from a 143 KB table, dates over seven years",
          iteration -> passes(() -> VarkaGatherProbe.yearByLookup(sevenYears, out, indexScratch)));
      benchmark.addCase("arithmetic, dates over seven years",
          iteration -> passes(() -> VarkaGatherProbe.yearByArithmetic(sevenYears, out)));
      benchmark.addCase("scalar loop over the same table, seven years",
          iteration -> passes(() -> VarkaGatherProbe.yearByScalarLookup(sevenYears, out)));
      benchmark.run();
    });

    section("year(d) = 1998: the same three, fused, where lanes are supposed to pay", () -> {
      // The probe above measures one field written to an array - the one shape where Varka's
      // fusion advantage is zero, which is why it cannot settle whether a calendar node should
      // lower to scalar code. This section is the shape that can: a filter, counted, where the
      // vector paths never leave a register and the scalar path has to spill and reload.
      int[] sevenYears = days(sevenYearsStart(), 7 * 365, 9002);
      int[] indexScratch = new int[VarkaGatherProbe.lanes()];
      int[] daysScratch = new int[VarkaGatherProbe.lanes()];
      int[] yearScratch = new int[VarkaGatherProbe.lanes()];

      long expected = countYear(sevenYears, TARGET);
      checkCount(VarkaGatherProbe.fusedArithmetic(sevenYears, TARGET), expected,
          "fused arithmetic", "");
      checkCount(VarkaGatherProbe.fusedGather(sevenYears, TARGET, indexScratch), expected,
          "fused gather", "");
      checkCount(VarkaGatherProbe.fusedScalarLookup(sevenYears, TARGET), expected,
          "fused scalar", "");
      checkCount(VarkaGatherProbe.fusedScalarInsideVector(
          sevenYears, TARGET, daysScratch, yearScratch), expected,
          "scalar inside a vector kernel", "");

      long values = (long) NUM_ROWS * REPEATS;
      var fused = timed(benchmark(
          values + " dates, " + VarkaGatherProbe.lanes() + " lanes", values)).build();
      fused.addCase("arithmetic in lanes, compared in lanes",
          iteration -> passes(() -> VarkaGatherProbe.fusedArithmetic(sevenYears, TARGET)));
      fused.addCase("gathered from the table, compared in lanes", iteration ->
          passes(() -> VarkaGatherProbe.fusedGather(sevenYears, TARGET, indexScratch)));
      fused.addCase("scalar loop end to end, no lanes anywhere",
          iteration -> passes(() -> VarkaGatherProbe.fusedScalarLookup(sevenYears, TARGET)));
      fused.addCase("scalar lookup inside a vector kernel (spill and reload)", iteration ->
          passes(() -> VarkaGatherProbe.fusedScalarInsideVector(
              sevenYears, TARGET, daysScratch, yearScratch)));
      fused.run();
    });

    section("year(d) = 1998 over an off-heap column: an era table against the arithmetic", () -> {
      // The section above gathers from a table sized to an arbitrary window (1950-2049) with
      // the column in an int[]. Neither matches Varka. This one fixes both: the column lives in
      // a MemorySegment, the way every Varka input does, and the table is sized to the
      // calendar's period - 146097 entries, one Gregorian era, ClickHouse's DATE_LUT_SIZE - so
      // it is indexed by day of era and needs no fallback for any int32 date. Varka's prefix
      // already computes that index.
      int[] sevenYears = days(sevenYearsStart(), 7 * 365, 9002);
      int[] indexScratch = new int[VarkaGatherProbe.lanes()];
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment column = arena.allocate(NUM_ROWS * 4L, 64);
        for (int i = 0; i < NUM_ROWS; i++) {
          column.set(ValueLayout.JAVA_INT, i * 4L, sevenYears[i]);
        }
        long expected = countYear(sevenYears, TARGET);
        checkCount(VarkaGatherProbe.fusedArithmeticFromSegment(column, NUM_ROWS, TARGET),
            expected, "arithmetic", " over a segment");
        checkCount(VarkaGatherProbe.fusedEraTableFromSegment(
            column, NUM_ROWS, TARGET, indexScratch), expected, "era table", " over a segment");

        long values = (long) NUM_ROWS * REPEATS;
        var offHeap = timed(benchmark(
            values + " dates off-heap, " + VarkaGatherProbe.lanes() + " lanes", values)).build();
        offHeap.addCase("arithmetic in lanes, off-heap column", iteration ->
            passes(() -> VarkaGatherProbe.fusedArithmeticFromSegment(column, NUM_ROWS, TARGET)));
        offHeap.addCase("era-table gather (571 KB, no fallback), off-heap column", iteration ->
            passes(() ->
                VarkaGatherProbe.fusedEraTableFromSegment(column, NUM_ROWS, TARGET, indexScratch)));
        offHeap.run();
      }
    });
  }

  private static void checkCount(long got, long expected, String ctx, String where) {
    require(got == expected,
        ctx + where + " counted " + got + " rows, java.time says " + expected);
  }

  private static void checkYears(int[] input, int[] out, String ctx) {
    int i = 0;
    while (i < input.length) {
      int want = LocalDate.ofEpochDay(input[i]).getYear();
      if (out[i] != want) {
        throw requirementFailed(ctx + " disagreed with java.time on day " + input[i] + ": got "
            + out[i] + ", want " + want);
      }
      i += 1;
    }
  }
}
