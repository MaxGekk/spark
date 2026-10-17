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
import java.util.Locale;

import scala.Option;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.VarkaBenchmark;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.VarkaFilterColumnarToRowExec;

/**
 * VARKA-78's measurement: what a two-column predicate under a one-column consumer costs, which
 * was the one shape in milestone 4's debt register where Varka ran slower than stock Spark.
 *
 * <p>It is a file of its own rather than rungs added to {@link VarkaFilterBenchmark}, for two
 * reasons. That file's fixture has a single date column and every predicate in it reads one
 * column, so the shape priced here cannot be written against it without adding a column to the
 * fixture - which would move every number that file has committed, for a question it was not
 * asked. And the question here is not a filter's, it is a plan's: the same predicate and the same
 * kernel throughout, with only the <i>consumer's</i> column set changing.
 *
 * <p><b>The three shapes, and what each isolates.</b> Spark's column pruning has already run by
 * the time any columnar rule sees the plan, so a projection survives above a filter only when the
 * predicate reads more columns than the consumer wants:
 *
 * <ul>
 *   <li>{@code SELECT d ... WHERE d < d2} - the losing shape. Two columns in the predicate, one
 *       wanted. Without the rule's absorption (VARKA-78) a Janino {@code Project [d]} would sit
 *       above the row-producing Varka filter, and the node would convert {@code d2} to a row as
 *       well, whose value that projection discards.</li>
 *   <li>{@code SELECT d, d2 ... WHERE d < d2} - the same predicate and the same selectivity with
 *       nothing to narrow, so the projection is redundant and Spark removed it long before this
 *       rule ran. This is the control: it never had a residual projection to absorb, so it must
 *       not move.</li>
 *   <li>{@code SELECT COUNT(*) ... WHERE d < d2} - the predicate with nothing crossing the
 *       read-back floor at all, which is what the kernel alone is worth on this data.</li>
 * </ul>
 *
 * <p>The difference between the first two rows is the width of the row that crosses the floor;
 * the distance from either to its Janino baseline is what the plan shape is worth. Read the
 * {@code Relative} column against the baseline case in each block, not across blocks: the three
 * shapes do different amounts of work by construction.
 *
 * <p>Selectivity is the ladder because the floor's cost is per <b>selected</b> row while the
 * kernel's advantage is per <b>input</b> row, so the two move in opposite directions and a single
 * rung would say whatever its rung said.
 *
 * <p>The rung is set by the <b>sign</b> of {@code d2 - d}, one day either way, rather than by
 * shifting {@code d2} around the data's own cycle. The cycling version was written first and was
 * wrong in a way worth recording: with {@code d2} the cycle-shift of {@code d} by {@code s} days,
 * {@code d < d2} holds for {@code (cycle - s) / cycle} of the rows - the inverse of the shift,
 * not the shift - so the ladder ran backwards, and its top rung, {@code s = cycle}, made {@code
 * d2} equal to {@code d} on every row and selected nothing at all while being labelled 100%. It
 * measured a real plan over real data either way, which is exactly why nothing failed; what it
 * did not measure is what its labels said. {@link #requireSelectivity} is here so that cannot
 * happen twice - the rung asserts its own fraction against the engine before anything is timed.
 *
 * <p>To run this benchmark:
 * <pre>
 *   dev/varka_bench_regen.sh sql VarkaNarrowingBenchmark
 * </pre>
 */
public class VarkaNarrowingBenchmark extends VarkaSqlBenchmarkBase {

  private static final int NUM_ROWS = 2_000_000;

  /** The day cycle the fixture repeats over; the shift below is read against it. */
  private static final int CYCLE = 1460;

  /**
   * Two date columns over the same cycle. {@code d2} is one day after {@code d} on {@code pct}
   * percent of the rows and one day before it on the rest, so {@code d < d2} selects exactly
   * {@code pct} percent of the rows where both are non-null, whatever the cycle does. Nulls every
   * 31st row in {@code d} and every 37th in {@code d2}, on different periods so the two null sets
   * do not coincide - a WHERE drops them on both engines, which is SQL's own rule and not a Varka
   * behaviour.
   */
  private static void cacheDates(SparkSession session, int pct) {
    session.sql("""
        select d, case when i %% 37 = 0 then null
                    when i %% 100 < %d then date_add(d, 1)
                    else date_sub(d, 1) end as d2, i
        from (select case when id %% 31 = 0 then null
                     else date_add(date'2020-01-01', cast(id as int) %% %d) end as d,
                     cast(id as int) as i
              from range(0, %d))""".formatted(pct, CYCLE, NUM_ROWS))
        .createOrReplaceTempView("varka_pairs");
    session.catalog().cacheTable("varka_pairs");
    session.sql("select count(*) from varka_pairs").collect();
  }

  /**
   * The rung asserts the selectivity its name claims, before anything is timed. A benchmark
   * whose rungs are mislabelled still measures a real plan over real data, so nothing fails
   * and nothing looks wrong - it just answers a question other than the one on the label, and
   * the label is what gets quoted.
   */
  private static void requireSelectivity(SparkSession session, String name, int pct) {
    double selected = session.sql(COUNTED).collectAsList().getFirst().getLong(0);
    double eligible = session.sql(
        "SELECT COUNT(*) FROM varka_pairs WHERE d IS NOT NULL AND d2 IS NOT NULL")
        .collectAsList().getFirst().getLong(0);
    double actual = 100.0 * selected / eligible;
    require(Math.abs(actual - pct) < 1.0, String.format(Locale.ROOT,
        "rung '%s' selects %.1f%% of the non-null rows, not %d%%", name, actual, pct));
  }

  /** A rung of the ladder: its name and the percentage of non-null rows {@code d < d2} selects. */
  private record Rung(String name, int pct) {}

  private static final List<Rung> LADDER = List.of(
      new Rung("10% selected", 10),
      new Rung("70% selected", 70),
      new Rung("100% selected", 100));

  private static final String NARROWED = "SELECT d FROM varka_pairs WHERE d < d2";
  private static final String UNNARROWED = "SELECT d, d2 FROM varka_pairs WHERE d < d2";
  private static final String COUNTED = "SELECT COUNT(*) AS c FROM varka_pairs WHERE d < d2";

  /**
   * Fails the run rather than quietly pricing a plan nobody meant to measure: the varka side
   * must plan the row-out filter and actually run its kernel, and - for the narrowed shape -
   * must have absorbed the projection, which is the thing being measured. A benchmark that
   * silently prices the unfused plan is the failure mode {@code sql/varka/AGENTS.md} calls a
   * ghost fallback, and it is worth more here than usual because the absorbed and unabsorbed
   * plans differ by one node and answer identically.
   */
  private static void requireShape(SparkSession varka, String name, String query,
      boolean absorbed) {
    Dataset<Row> df = varka.sql(query);
    df.queryExecution().toRdd().count();
    SparkPlan plan = df.queryExecution().executedPlan();
    Option<SparkPlan> found = plan.find(p -> p instanceof VarkaFilterColumnarToRowExec);
    if (found.isEmpty()) {
      throw new IllegalStateException(
          "case '" + name + "' did not plan a row-out Varka filter:\n" + plan.treeString());
    }
    var node = (VarkaFilterColumnarToRowExec) found.get();
    require(node.metrics().apply("numVarkaBatches").value() > 0,
        "case '" + name + "' fused but fell back at run time");
    require(node.narrowing().isDefined() == absorbed, "case '" + name
        + "' expected narrowing absorbed = " + absorbed + ":\n" + plan.treeString());
  }

  private void runPair(SparkSession baseline, SparkSession varka, String name, String query) {
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
        "VarkaNarrowing-baseline", false, false, "local[1]");
    clearSessions();
    SparkSession varka = VarkaArrowSessions.createSession(
        "VarkaNarrowing-varka", true, false, "local[1]");
    clearSessions();
    require(baseline != varka, "the two sessions must be distinct or there is no baseline");
    try {
      for (Rung rung : LADDER) {
        cacheDates(baseline, rung.pct());
        cacheDates(varka, rung.pct());
        try {
          requireSelectivity(baseline, rung.name(), rung.pct());
          requireShape(varka, "narrowed, " + rung.name(), NARROWED, true);
          requireShape(varka, "unnarrowed, " + rung.name(), UNNARROWED, false);
          runPair(baseline, varka, "SELECT d WHERE d < d2 (narrowed), " + rung.name(), NARROWED);
          runPair(baseline, varka, "SELECT d, d2 WHERE d < d2 (control), " + rung.name(),
              UNNARROWED);
          runPair(baseline, varka, "COUNT(*) WHERE d < d2 (no read-back), " + rung.name(),
              COUNTED);
        } finally {
          baseline.catalog().uncacheTable("varka_pairs");
          varka.catalog().uncacheTable("varka_pairs");
        }
      }
    } finally {
      baseline.stop();
      varka.stop();
    }
  }
}
