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

import java.util.List;

import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.VarkaBenchmark;
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator;
import org.apache.spark.sql.execution.VarkaQ3Ranges;

/**
 * TPC-DS {@code modified-q3}'s filter on vanilla Spark and on Varka: an Arrow-cached int column
 * of date keys filtered by the first n of the query's 200 {@code between} ranges joined by {@code
 * or}, time per row against n. Vanilla generates the whole chain into the scan's {@code
 * processNext}, which grows with the ranges and, past HotSpot's 8000-byte limit, is never
 * compiled; the census found this the one stage of TPC-DS and TPC-H that crosses it ({@code
 * VARKA-193.md}).
 *
 * <p>Varka compiles the ranges to one range set, evaluated by a loop over a table of bounds, so
 * it fuses the filter at every rung and its kernel stays the same size whatever the number of
 * ranges. Written as the tree of comparisons the query spells, the chain fit Varka's byte budget
 * only up to 48 ranges, which is why the rungs straddle 48 and 49; they reach the query's 200,
 * past vanilla's own crossing, which the notes locate ({@code VARKA-172.md}).
 *
 * <p>A third arm times the general answer to the same limit, {@code splitConditions}: range sets
 * off, so the ranges reach Varka as the comparisons the query writes, and the filter split across
 * several selection outputs, each within the budget, instead of declined ({@code VARKA-172.md}
 * 3.1). It is what any disjunction too large for one method gets, not only one of ranges.
 *
 * <p>Each rung writes, after its table, vanilla's largest generated method, whether it is past
 * {@code HugeMethodLimit}, whether Varka ran the filter as a kernel, and how many rows pass. A
 * rung whose Varka arm does not run the filter as a kernel fails the run rather than timing an
 * arm that is not what its name says.
 *
 * <p>To run this benchmark:
 * <pre>
 *   dev/varka_bench_regen.sh sql VarkaRangeFilterBenchmark
 * </pre>
 */
public class VarkaRangeFilterBenchmark extends VarkaSqlBenchmarkBase {

  private static final int NUM_ROWS = 2_000_000;

  /** Straddling Varka's boundary (48 and 49) and vanilla's crossing. */
  private static final List<Integer> RUNGS = List.of(10, 48, 49, 100, 150, 200);

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    // These arms own their Arrow-backed sessions, as the size ladder's do; the default session,
    // with the default cache serializer, is never made.
    stopLeftoverSessions();
    SparkSession baseline = VarkaArrowSessions.createSession(
        "VarkaRangeFilter-vanilla", false, false, "local[1]");
    clearSessions();
    SparkSession varka = VarkaArrowSessions.createSession(
        "VarkaRangeFilter-varka", true, false, "local[1]");
    clearSessions();
    require(baseline != varka, "the two sessions must be distinct or there is no baseline");
    try {
      VarkaArrowSessions.cacheRangeKeys(baseline, NUM_ROWS);
      VarkaArrowSessions.cacheRangeKeys(varka, NUM_ROWS);
      section("modified-q3's filter: n date ranges joined by or", () -> {
        for (int n : RUNGS) {
          rung(baseline, varka, n);
        }
      });
    } finally {
      baseline.stop();
      varka.stop();
    }
  }

  private void rung(SparkSession baseline, SparkSession varka, int n) {
    int limit = CodeGenerator.DEFAULT_JVM_HUGE_METHOD_LIMIT();
    String query = "SELECT ss_sold_date_sk FROM range_keys WHERE "
        + VarkaQ3Ranges.predicate(n, "ss_sold_date_sk");
    int bytes = VarkaArrowSessions.vanillaMethodBytes(baseline, query);
    Tuple2<Object, String> fused = VarkaArrowSessions.varkaFilters(varka, query);
    require((Boolean) fused._1(),
        "at " + n + " ranges the Varka arm declined the filter:\n" + fused._2());
    Tuple2<Object, String> split = VarkaArrowSessions.withEmitOptions(
        VarkaArrowSessions.splitConditionsDesign(),
        () -> VarkaArrowSessions.varkaFilters(varka, query));
    require((Boolean) split._1(),
        "at " + n + " ranges the split arm declined the filter:\n" + split._2());
    long selected = baseline.sql(query).count();
    VarkaBenchmark benchmark = benchmark(
        n + " ranges over " + NUM_ROWS + " Arrow-cached rows", NUM_ROWS).timed().build();
    benchmark.addCase("vanilla Spark (whole-stage codegen)", i -> noop(baseline.sql(query)));
    benchmark.addCase("Varka", i -> noop(varka.sql(query)));
    benchmark.addCase("Varka, split conditions", i -> withEmitOptions(
        VarkaArrowSessions.splitConditionsDesign(), () -> noop(varka.sql(query))));
    runCases(benchmark);
    note("rung " + n + ": vanilla's largest generated method is " + bytes + " bytes, "
        + (bytes > limit ? "past" : "under") + " HugeMethodLimit " + limit + "; both Varka "
        + "arms run the filter as a kernel; " + selected + " of " + NUM_ROWS + " rows pass");
  }
}
