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

package org.apache.spark.sql.execution.benchmark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.VarkaBenchmark;
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator;

/**
 * The size ladder (VARKA-171): one projection widened rung by rung, on stock Spark and on Varka,
 * time per row against the number of entries.
 *
 * <p>Every entry is {@code greatest(add_months(d, k), date_add(d, k), last_day(d))} for its own
 * {@code k}, over one Arrow-cached date column, and the projection ends in {@code noop()}. Vanilla
 * Spark generates the whole projection into one method, the projection's consume function, which
 * grows by about 190 bytes an entry and crosses HotSpot's {@code HugeMethodLimit}, 8000 bytes,
 * between 52 and 54 entries; past it the method is loaded and never compiled, and Spark logs
 * nothing ({@code VarkaSizeLadderJitSuite}). Varka emits the same projection as a kernel whose
 * every method is held under that limit by the byte budget, so no rung crosses anything. The
 * rungs straddle the crossing rather than being round, and stop at a hundred: past it {@code
 * spark.sql.codegen.maxFields} turns vanilla's whole-stage codegen off, which is a different cliff
 * ({@code VARKA-171.md} 2.2).
 *
 * <p>Each rung writes what it is into the results file after its timed cases: vanilla's largest
 * generated method, whether that is past {@code HugeMethodLimit}, and how many entries Varka
 * fused. The x-axis's meaning is then in the same file as its timings. A rung whose Varka arm
 * does not fuse every entry, or falls back at run time, fails the run rather than timing a partly
 * per-row Varka arm.
 *
 * <p>The two arms read the same Arrow cache - the vanilla arm through a column-to-row conversion
 * and whole-stage codegen, the Varka arm through its kernel - so they differ in the engine and
 * nothing else, as {@code VarkaThroughputBenchmark}'s baseline does.
 *
 * <p>{@link VarkaSizeLadderTuningBenchmark} times vanilla Spark's own settings against the same
 * rungs, data and query, and {@link VarkaSizeLadderParallelBenchmark} both arms on every core;
 * both take them from {@link VarkaSizeLadder}.
 *
 * <p>Its dry run, {@code -Dvarka.bench.dryRun=true}, times nothing but still caches the data and
 * plans and runs each rung's query, since the rung notes it prints come from that work.
 *
 * <p>To run this benchmark:
 * <pre>
 *   dev/varka_bench_regen.sh sql VarkaSizeLadderBenchmark
 * </pre>
 */
public class VarkaSizeLadderBenchmark extends VarkaSqlBenchmarkBase {

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    // These arms own their Arrow-backed sessions, as VarkaThroughputBenchmark's do; the default
    // session, with the default cache serializer, is never made.
    stopLeftoverSessions();
    SparkSession baseline = VarkaArrowSessions.createSession(
        "VarkaSizeLadder-vanilla", false, false, "local[1]");
    try {
      clearSessions();
      SparkSession varka = VarkaArrowSessions.createSession(
          "VarkaSizeLadder-varka", true, false, "local[1]");
      try {
        clearSessions();
        require(baseline != varka, "the two sessions must be distinct or there is no baseline");
        ladder(baseline, varka);
      } finally {
        varka.stop();
      }
    } finally {
      baseline.stop();
    }
  }

  private void ladder(SparkSession baseline, SparkSession varka) {
    int numRows = VarkaSizeLadder.NUM_ROWS;
    int hugeMethodLimit = CodeGenerator.DEFAULT_JVM_HUGE_METHOD_LIMIT();
    VarkaSizeLadder.cacheDates(baseline);
    VarkaSizeLadder.cacheDates(varka);
    section("the size ladder: greatest(add_months(d, k), date_add(d, k), last_day(d))", () -> {
      for (int n : VarkaSizeLadder.RUNGS) {
        String query = VarkaSizeLadder.query(n);
        int bytes = VarkaArrowSessions.vanillaMethodBytes(baseline, query);
        int fused = VarkaSizeLadder.varkaFused(varka, query);
        require(fused == n, "the Varka arm fused " + fused + " of " + n + " entries");
        VarkaBenchmark benchmark = benchmark(
            n + " entries over " + numRows + " Arrow-cached rows", numRows).timed().build();
        benchmark.addCase("vanilla Spark (whole-stage codegen)", i -> noop(baseline.sql(query)));
        benchmark.addCase("Varka", i -> noop(varka.sql(query)));
        runCases(benchmark);
        // After the table rather than before it: the 128-bit companion file is cut from the
        // console from the first table on, and a note ahead of the first table was lost.
        note("rung " + n + ": vanilla's largest generated method is " + bytes + " bytes, "
            + (bytes > hugeMethodLimit ? "past" : "under")
            + " HugeMethodLimit " + hugeMethodLimit + "; "
            + "Varka fuses " + fused + " of " + n + " entries");
      }
    });
  }
}
