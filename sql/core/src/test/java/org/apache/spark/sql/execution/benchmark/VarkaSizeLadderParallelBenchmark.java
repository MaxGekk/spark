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

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.VarkaBenchmark;
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator;

/**
 * The size ladder on every core (VARKA-197): {@code VarkaSizeLadderBenchmark}'s rungs, data and
 * query, with both sessions on {@code local[*]} instead of {@code local[1]}, to ask whether the gap
 * the one-core ladder measures survives parallelism.
 *
 * <p>Why it might not. Vanilla Spark's consume method, past {@code HugeMethodLimit}, runs
 * interpreted, and an interpreter costs the same on every core, so its time divides by the cores.
 * Varka's kernels read the Arrow cache and write one column per entry at a rate the cores together
 * may saturate: at a hundred entries a kernel writes four hundred bytes a row, and what one core
 * did at a few gigabytes a second, four cores may find bound by the machine rather than by the
 * kernel. Where that happens the ratio between the arms shrinks, and by how much is the
 * measurement.
 *
 * <p>The cached table comes in as many partitions as the session has cores, since {@code range}
 * follows the default parallelism, and that is what puts a task on every core; each rung's note
 * records the core count and the partition count beside vanilla's method size, so a reader knows
 * what "every core" was on the machine that wrote the file. Run in one dispatch with the one-core
 * ladder, so both files come from the same machine.
 *
 * <p>Its own class and files rather than a switch on the ladder, so the one-core file stays what
 * it is and the two are read side by side ({@code VARKA-197.md}).
 *
 * <p>Its dry run, {@code -Dvarka.bench.dryRun=true}, times nothing but still caches the data and
 * plans and runs each rung's query once, since the rung notes it prints come from that work.
 *
 * <p>To run this benchmark:
 * <pre>
 *   dev/varka_bench_regen.sh sql VarkaSizeLadderParallelBenchmark
 * </pre>
 */
public class VarkaSizeLadderParallelBenchmark extends VarkaSqlBenchmarkBase {

  /**
   * The ladder's rungs, data and query, shared with the one-core ladder. Its members are
   * {@code private[benchmark]}, which scalac gives no static forwarder, so Java reaches them
   * through the Scala object's instance.
   */
  private static final VarkaSizeLadder$ LADDER = VarkaSizeLadder$.MODULE$;

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    // These arms own their Arrow-backed sessions, as the one-core ladder's do; the default
    // session, with the default cache serializer, is never made. Both join one SparkContext, so
    // the master of the first is the master of both.
    stopLeftoverSessions();
    SparkSession baseline = VarkaArrowSessions.createSession(
        "VarkaSizeLadderParallel-vanilla", false, false, "local[*]");
    try {
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
      SparkSession varka = VarkaArrowSessions.createSession(
          "VarkaSizeLadderParallel-varka", true, false, "local[*]");
      try {
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
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
    int cores = baseline.sparkContext().defaultParallelism();
    int numRows = LADDER.numRows();
    int hugeMethodLimit = CodeGenerator.DEFAULT_JVM_HUGE_METHOD_LIMIT();
    LADDER.cacheDates(baseline, numRows);
    LADDER.cacheDates(varka, numRows);
    int partitions = baseline.table("ladder_dates").rdd().getNumPartitions();
    section("the size ladder on local[*], " + cores + " cores: "
        + "greatest(add_months(d, k), date_add(d, k), last_day(d))", () -> {
      for (Object rung : CollectionConverters.asJava(LADDER.rungs())) {
        int n = (Integer) rung;
        String query = "SELECT "
            + IntStream.rangeClosed(1, n).mapToObj(LADDER::entry)
                .collect(Collectors.joining(", "))
            + " FROM ladder_dates";
        int bytes = VarkaArrowSessions.vanillaMethodBytes(baseline, query);
        int fused = LADDER.varkaFused(varka, query);
        require(fused == n, "the Varka arm fused " + fused + " of " + n + " entries");
        VarkaBenchmark benchmark = benchmark(
            n + " entries over " + numRows + " Arrow-cached rows on " + cores + " cores",
            numRows).timed().build();
        benchmark.addCase("vanilla Spark (whole-stage codegen)", i -> noop(baseline.sql(query)));
        benchmark.addCase("Varka", i -> noop(varka.sql(query)));
        runCases(benchmark);
        note("rung " + n + ": vanilla's largest generated method is " + bytes + " bytes, "
            + (bytes > hugeMethodLimit ? "past" : "under")
            + " HugeMethodLimit " + hugeMethodLimit + "; "
            + "Varka fuses " + fused + " of " + n + " entries; " + cores + " cores, "
            + partitions + " cached partitions");
      }
    });
  }
}
