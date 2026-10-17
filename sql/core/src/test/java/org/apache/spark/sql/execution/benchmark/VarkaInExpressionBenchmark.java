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

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Option;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.VarkaBenchmark;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.VarkaColumnarToRowExec;
import org.apache.spark.sql.execution.VarkaFilterColumnarToRowExec;
import org.apache.spark.sql.execution.VarkaFilterExec;
import org.apache.spark.sql.execution.VarkaProjectExec;

/**
 * The columnar-terminal {@code IN} benchmark (VARKA-20): Spark's own {@code
 * InExpressionBenchmark} measures {@code WHERE id IN (...)} as a row-source filter, where the
 * milestone survey found {@code DateType} the slowest primitive at short lists; this benchmark
 * prices what Varka can fuse today - {@code IN} in condition position, {@code CASE WHEN d IN
 * (...) THEN ... ELSE ... END}, over Arrow-cached dates with a columnar terminal - against the
 * same Janino session, list sizes drawn from the stock benchmark's ladder (5, 50, 200, 500) plus
 * the fused cap (16).
 *
 * <p>What the two files do and do not share: the stock committed results ({@code
 * InExpressionBenchmark-*-results.txt}) are upstream Azure EPYC runs (the JDK-25 file reads 31.2
 * M rows/s at 5 dates decaying to 8.6 at 500; the milestone quoted the JDK-17 file's 27.4 -> 8.3),
 * while every number here is this machine's, so the same-run {@code baseline (Janino)} cases are
 * the real baseline and the stock file is a shape reference only. The fused case also does
 * strictly more work than the stock filter - it computes a branch per row - so the two are not a
 * like-for-like ratio; the anchor cases below tie the shapes together instead:
 *
 * <ul>
 *   <li>Fused pair: {@code CASE WHEN d IN (<n>) ...} at n = 5 (arrives as {@code In}), n = 16 (the
 *       compiler cap, arrives as {@code InSet} past the optimizer's threshold of 10), guarded by
 *       {@link #requireFused}.</li>
 *   <li>Above the cap: n = 50, guarded by {@link #requireDeclined} and still measured - the
 *       no-regression proof that an over-cap list keeps stock performance on the varka
 *       session.</li>
 *   <li>Anchor: the stock-shaped {@code SELECT COUNT(*) WHERE d IN (<n>)} filter at 5/50/200/500
 *       on both sessions, tying this file's list shapes to the upstream benchmark's. The in-cap
 *       anchor runs the mask kernel and is guarded fused (VARKA-21), while 50/200/500 stay
 *       declined over the literal cap.</li>
 * </ul>
 *
 * <p>Sessions, tables and methodology follow {@code VarkaThroughputBenchmark} (Arrow cache
 * serializer, session hygiene around {@code getOrCreate}, 2M rows with a null every 31, five
 * iterations over two-second windows).
 *
 * <p>To run this benchmark:
 * <pre>
 *   dev/varka_bench_regen.sh sql VarkaInExpressionBenchmark
 * </pre>
 */
public class VarkaInExpressionBenchmark extends VarkaSqlBenchmarkBase {

  private static final int NUM_ROWS = 2_000_000;

  private static void cacheDates(SparkSession session) {
    session.sql("""
        select case when id %% 31 = 0 then null
               else date_add(date'2020-01-01', cast(id as int) %% 1460) end as d,
               cast(id as int) as i
        from range(0, %d)""".formatted(NUM_ROWS))
        .createOrReplaceTempView("varka_dates");
    session.catalog().cacheTable("varka_dates");
    session.sql("select count(*) from varka_dates").collect();
  }

  /** {@code n} distinct date literals spread across the data's 4-year range, as SQL text. */
  private static String literals(int n) {
    return IntStream.range(0, n)
        .mapToObj(k -> "date'" + LocalDate.of(2020, 1, 2).plusDays(k * 1460L / n) + "'")
        .collect(Collectors.joining(", "));
  }

  private static String fusedQuery(int n) {
    return "SELECT CASE WHEN d IN (" + literals(n) + ") THEN date_add(d, 1) ELSE d END AS a "
        + "FROM varka_dates";
  }

  private static String anchorQuery(int n) {
    return "SELECT COUNT(*) AS c FROM varka_dates WHERE d IN (" + literals(n) + ")";
  }

  private static boolean isVarkaNode(SparkPlan p) {
    return p instanceof VarkaProjectExec || p instanceof VarkaColumnarToRowExec
        || p instanceof VarkaFilterExec || p instanceof VarkaFilterColumnarToRowExec;
  }

  private static void requireFused(SparkSession varka, String name, String query) {
    Dataset<Row> df = varka.sql(query);
    df.queryExecution().toRdd().count();
    SparkPlan plan = df.queryExecution().executedPlan();
    Option<SparkPlan> node = plan.find(VarkaInExpressionBenchmark::isVarkaNode);
    if (node.isEmpty()) {
      throw new IllegalStateException(
          "case '" + name + "' did not fuse on the varka session:\n" + plan.treeString());
    }
    long batches = node.get().metrics().apply("numVarkaBatches").value();
    require(batches > 0, "case '" + name + "' fused but fell back at run time "
        + "(numVarkaBatches = " + batches + ")");
  }

  /** The over-cap guard: the varka session must have declined, not silently fallen back. */
  private static void requireDeclined(SparkSession varka, String name, String query) {
    SparkPlan plan = varka.sql(query).queryExecution().executedPlan();
    require(plan.find(VarkaInExpressionBenchmark::isVarkaNode).isEmpty(),
        "case '" + name + "' was expected to decline but fused:\n" + plan.treeString());
  }

  private void runPair(SparkSession baseline, SparkSession varka, String name, String query) {
    section(name, () -> {
      VarkaBenchmark benchmark = benchmark(
          name + " over " + NUM_ROWS + " Arrow-cached rows", NUM_ROWS).timed().build();
      benchmark.addCase("baseline (Janino)", i -> noop(baseline.sql(query)));
      benchmark.addCase("varka (SIMD)", i -> noop(varka.sql(query)));
      runCases(benchmark);
    });
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    stopLeftoverSessions();
    // VarkaArrowSessions' sessions are the ones this benchmark built for itself: Arrow cache,
    // and on the Varka arm the warm-up off, so each case is checked on its kernel before timing.
    SparkSession baseline = VarkaArrowSessions.createSession(
        "VarkaIn-baseline", false, false, "local[1]");
    clearSessions();
    SparkSession varka = VarkaArrowSessions.createSession(
        "VarkaIn-varka", true, false, "local[1]");
    clearSessions();
    require(baseline != varka, "the two sessions must be distinct or there is no baseline");
    try {
      cacheDates(baseline);
      cacheDates(varka);

      for (int n : List.of(5, 16)) {
        requireFused(varka, "case-when IN, " + n + " literals", fusedQuery(n));
        runPair(baseline, varka, "case-when IN, " + n + " literals, fused", fusedQuery(n));
      }
      requireDeclined(varka, "case-when IN, 50 literals", fusedQuery(50));
      runPair(baseline, varka, "case-when IN, 50 literals, declined (over the cap)",
          fusedQuery(50));
      // The in-cap anchor: the filter itself runs the mask kernel (VARKA-21).
      requireFused(varka, "filter IN anchor, 5 literals", anchorQuery(5));
      runPair(baseline, varka, "filter IN anchor, 5 literals (fused since VARKA-21)",
          anchorQuery(5));
      for (int n : List.of(50, 200, 500)) {
        requireDeclined(varka, "filter IN anchor, " + n + " literals", anchorQuery(n));
        runPair(baseline, varka, "filter IN anchor, " + n + " literals (unfused on both)",
            anchorQuery(n));
      }
    } finally {
      baseline.stop();
      varka.stop();
    }
  }
}
