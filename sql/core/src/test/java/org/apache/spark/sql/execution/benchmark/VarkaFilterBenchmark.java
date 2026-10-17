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
 * The filter benchmark (VARKA-21): the survey's {@code d_date BETWEEN}-style shape - here {@code
 * d < DATE}, the same paired-comparison lowering with one leg - over Arrow-cached dates, at a
 * selectivity ladder from none-selected to all-selected, against the same Janino session. This is
 * the committed measurement behind the milestone's open question 2 (the selected-batch contract):
 * each selectivity runs in both consumer variants, so the two halves of the v1 contract are
 * priced separately -
 *
 * <ul>
 *   <li><b>columnar terminal</b> ({@code noop}, which accepts batches): the varka side is {@link
 *       VarkaFilterExec} alone - the mask kernel plus the scalar compaction into a fresh batch,
 *       the copy {@code compress(mask)} (milestone 4 item 11) would replace. Compaction cost grows
 *       with selected rows, so the high-selectivity rungs are its worst case.</li>
 *   <li><b>row consumer</b> ({@code toRdd}): the varka side is {@link
 *       VarkaFilterColumnarToRowExec} - the mask kernel plus the bitmap-guided row skip, no
 *       compaction at all. The dominant WHERE-plus-aggregate shape consumes this way.</li>
 * </ul>
 *
 * <p>Two stacked cases price the filter-feeds-projection contract (the compacted batch keeps the
 * Arrow invariant, so {@link VarkaProjectExec} runs its kernels right on top), and two COUNT(*)
 * cases tie the ladder to the survey's end-to-end shape.
 *
 * <p>Sessions, tables and methodology follow {@code VarkaThroughputBenchmark} (Arrow cache
 * serializer, session hygiene around {@code getOrCreate}, 2M rows with a null every 31 - the
 * nulls are dropped by every predicate here, on both engines, which is SQL's WHERE - five
 * iterations over two-second windows). The fused guards run through {@code toRdd} like the IN
 * benchmark's, which proves the row-variant plan fused; the columnar variant of the same
 * predicate fuses by the same rule match.
 *
 * <p>To run this benchmark:
 * <pre>
 *   dev/varka_bench_regen.sh sql VarkaFilterBenchmark
 * </pre>
 */
public class VarkaFilterBenchmark extends VarkaSqlBenchmarkBase {

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

  /** A rung of the ladder: its name and the {@code d < DATE} cutoff in days from 2020-01-01. */
  private record Rung(String name, int days) {}

  /**
   * The ladder. The data cycles over 1460 days, so the fraction of non-null rows selected is
   * {@code days / 1460}; "all" uses a cutoff past the range. The ~15% rung sits on the design
   * note's compaction threshold.
   */
  private static final List<Rung> LADDER = List.of(
      new Rung("0% selected", 0),
      new Rung("1% selected", 15),
      new Rung("15% selected", 219),
      new Rung("50% selected", 730),
      new Rung("85% selected", 1241),
      new Rung("100% selected", 1500));

  private static String cutoff(int days) {
    return "date'" + LocalDate.of(2020, 1, 1).plusDays(days) + "'";
  }

  /**
   * The rung's predicate. The 0% rung cannot be an always-false range ({@code d < <data
   * minimum>}): the in-memory scan's min/max stat pruning would drop every batch before the
   * filter node saw one, and the rung would price the pruning, not the mask. An interval
   * containing no whole day selects nothing while each conjunct stays satisfiable against the
   * stats, so the kernel actually runs - at the cost of one extra compare against the other
   * rungs, which is the honest price of an unprunable empty predicate.
   */
  private static String predicate(int days) {
    return days == 0 ? "d > date'2020-01-05' AND d < date'2020-01-06'" : "d < " + cutoff(days);
  }

  private static String filterQuery(int days) {
    return "SELECT d FROM varka_dates WHERE " + predicate(days);
  }

  private static String stackedQuery(int days) {
    return "SELECT date_add(d, 7) AS a FROM varka_dates WHERE " + predicate(days);
  }

  private static String countQuery(int days) {
    return "SELECT COUNT(*) AS c FROM varka_dates WHERE " + predicate(days);
  }

  private static void requireFused(SparkSession varka, String name, String query) {
    Dataset<Row> df = varka.sql(query);
    df.queryExecution().toRdd().count();
    SparkPlan plan = df.queryExecution().executedPlan();
    Option<SparkPlan> node = plan.find(p -> p instanceof VarkaFilterExec
        || p instanceof VarkaFilterColumnarToRowExec || p instanceof VarkaProjectExec
        || p instanceof VarkaColumnarToRowExec);
    if (node.isEmpty()) {
      throw new IllegalStateException(
          "case '" + name + "' did not fuse on the varka session:\n" + plan.treeString());
    }
    long batches = node.get().metrics().apply("numVarkaBatches").value();
    require(batches > 0, "case '" + name + "' fused but fell back at run time "
        + "(numVarkaBatches = " + batches + ")");
  }

  private void runColumnarPair(
      SparkSession baseline, SparkSession varka, String name, String query) {
    section(name, () -> {
      VarkaBenchmark benchmark = benchmark(
          name + " over " + NUM_ROWS + " Arrow-cached rows", NUM_ROWS).timed().build();
      benchmark.addCase("baseline (Janino)", i -> noop(baseline.sql(query)));
      benchmark.addCase("varka (SIMD)", i -> noop(varka.sql(query)));
      runCases(benchmark);
    });
  }

  private void runRowPair(SparkSession baseline, SparkSession varka, String name, String query) {
    section(name, () -> {
      VarkaBenchmark benchmark = benchmark(
          name + " over " + NUM_ROWS + " Arrow-cached rows", NUM_ROWS).timed().build();
      benchmark.addCase("baseline (Janino)",
          i -> baseline.sql(query).queryExecution().toRdd().count());
      benchmark.addCase("varka (SIMD)", i -> varka.sql(query).queryExecution().toRdd().count());
      runCases(benchmark);
    });
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    stopLeftoverSessions();
    // VarkaArrowSessions' sessions are the ones this benchmark built for itself: Arrow cache,
    // and on the Varka arm the warm-up off, so each case is checked on its kernel before timing.
    SparkSession baseline = VarkaArrowSessions.createSession(
        "VarkaFilter-baseline", false, false, "local[1]");
    clearSessions();
    SparkSession varka = VarkaArrowSessions.createSession(
        "VarkaFilter-varka", true, false, "local[1]");
    clearSessions();
    require(baseline != varka, "the two sessions must be distinct or there is no baseline");
    try {
      cacheDates(baseline);
      cacheDates(varka);

      for (Rung rung : LADDER) {
        String query = filterQuery(rung.days());
        requireFused(varka, "filter, " + rung.name(), query);
        runColumnarPair(baseline, varka, "filter to batches (compacting), " + rung.name(),
            query);
        runRowPair(baseline, varka, "filter to rows (mask skip), " + rung.name(), query);
      }
      for (int days : List.of(219, 1241)) {
        String name = days == 219 ? "15% selected" : "85% selected";
        requireFused(varka, "stacked, " + name, stackedQuery(days));
        runColumnarPair(baseline, varka, "filter then fused projection, " + name,
            stackedQuery(days));
        runRowPair(baseline, varka, "COUNT(*) over the filter, " + name, countQuery(days));
      }
    } finally {
      baseline.stop();
      varka.stop();
    }
  }
}
