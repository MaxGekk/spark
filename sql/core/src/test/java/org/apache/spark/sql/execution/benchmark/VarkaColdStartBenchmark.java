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

import static org.apache.spark.sql.execution.benchmark.VarkaSizeLadder.cacheDates;
import static org.apache.spark.sql.execution.benchmark.VarkaSizeLadder.entry;
import static org.apache.spark.sql.execution.benchmark.VarkaSizeLadder.quiesce;
import static org.apache.spark.sql.execution.benchmark.VarkaSizeLadder.quiesceOrFail;
import static org.apache.spark.sql.execution.benchmark.VarkaSizeLadder.varkaFused;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Option;
import scala.Tuple2;
import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.VarkaBenchmark;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaChrono;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelWarmup;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaShapeCache;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.VarkaColumnarToRowExec;
import org.apache.spark.sql.execution.VarkaFilterExecBase;
import org.apache.spark.sql.execution.VarkaProjectExec;
import org.apache.spark.sql.execution.VarkaQ3Ranges;
import org.apache.spark.sql.execution.metric.SQLMetric;

/**
 * What the first query costs (VARKA-195 and VARKA-212): a query's first runs against its steady
 * state, on stock Spark and on Varka with and without the kernel warm-up, at the size ladder's
 * rungs.
 *
 * <p>Every number the ladder publishes is steady state: the harness warms each case for two
 * seconds and reports the best of its iterations. A reader who runs a query once pays something
 * else - planning, the compile of the generated code, and the JVM's own warmup of it - and on
 * Varka the compile is an emission and a class definition per shape, then C2's work on the
 * kernel's methods. This benchmark prices that, so the post can say it beside the headline.
 *
 * <p>Each rung is one projection of {@code n} entries of {@code greatest(add_months(d, k),
 * date_add(d, k), last_day(d))} over an Arrow-cached date column, the ladder's own, over fewer
 * rows than the ladder reads so that the steady state does not drown the first run. Three arms,
 * each with its own session: vanilla Spark; Varka with {@code
 * spark.sql.codegen.varka.warmup.enabled} off, where a new kernel serves every batch from the
 * first and runs interpreted until enough batches have passed through it; and Varka with it on,
 * where a new shape's batches take Spark's row path while a background thread gets the kernel
 * compiled ({@code VARKA-212.md} 10). Cases per arm:
 *
 * <ul>
 *   <li><b>plan only</b>: analysis, optimization and physical planning of a shape this JVM has
 *       not seen, up to the executed plan and without running it. On the Varka arms the planner
 *       asks the compiler, which classifies the projection and emits the kernel, so this case
 *       holds the emission; on the vanilla arm code generation happens at execution and is not
 *       here.</li>
 *   <li><b>first run</b>: the same fresh shape, planned and run. Every iteration takes fresh
 *       offsets, so vanilla's generated source is new and Janino compiles it again; the Varka
 *       arms also clear the shape cache first, because literal values are not part of a shape
 *       and the same tree with new constants would be a hit.</li>
 *   <li><b>second run</b>: the same query again in the same session, at once, which is what the
 *       steady state is on the way to; the difference from the first run is the first run's
 *       price.</li>
 *   <li><b>once compiled</b>, the warm-up arm only: the second run started only after the warm-up
 *       has its verdict, which is the query a shape's kernel serves from then on.</li>
 * </ul>
 *
 * <p>Every timed iteration starts with the JVM quiet: no warm-up queued or running, and no JIT
 * compilation finished for a while ({@link VarkaSizeLadder#quiesce}), so that no case pays for
 * compiles an earlier iteration requested.
 *
 * <p>After each rung's table, a line says how long the warm-up took to its verdict in each
 * iteration of the once-compiled case, and how many kernel calls it made.
 *
 * <p>A second section asks the same first-query question of TPC-DS {@code modified-q3}'s filter,
 * the realistic query of VARKA-172, whose two designs tie at steady state: the range set, one loop
 * method under 2000 bytes over a table of bounds, and the split conditions, several methods of
 * several thousand bytes each. Which costs more on the first query is what decides whether both
 * stay ({@code VARKA-172.md} 9.11). The same cases as the projection's, for each design on each
 * Varka arm, with the designs emitted exclusively - the range set with the split off, the split
 * with the range set off - so that a design that fails to lower declines rather than run as the
 * other. At the query's 200 ranges, and at 48, under vanilla's crossing, where the split design
 * has nothing to split yet (the split begins at 49) and its arm is the plain comparison tree in
 * one method. The bounds shift by a day per iteration so that no iteration's source is one
 * Janino has compiled.
 *
 * <p>A third section runs one shape per chosen rung back to back on each arm and prints every
 * query's time, which is where a kernel's switch from the row path shows. A fourth asks whether
 * the code C2 compiles from the warm-up's short calls is as fast as the code it compiles from
 * real batches: at those rungs, over the ladder's two million rows, the kernel the warm-up
 * compiled - timed straight after its verdict - against the kernel its own batches compiled under
 * the ladder's two-second warmup. Both start from the JVM the two first-query sections leave.
 *
 * <p>A rung whose Varka arms do not fuse every entry fails the run rather than timing a partly
 * per-row Varka arm, as the ladder does.
 *
 * <p>Iterations are printed one by one rather than summarised, because a first run is one event
 * and its spread is the finding. The rungs and data are {@link VarkaSizeLadder}'s.
 *
 * <p>{@code VARKA_COLDSTART_SMOKE=true} in the environment, which the forked benchmark JVM
 * inherits where a {@code -D} on sbt's command line does not, runs one rung over ten thousand
 * rows twice, to check the benchmark itself and not to measure anything. The dry run, {@code
 * -Dvarka.bench.dryRun=true}, times no table, so the verdict lines of the first two sections are
 * empty; the back-to-back section and the warm-up's verdict of the last, which are no tables,
 * still run.
 *
 * <p>To run this benchmark:
 * <pre>
 *   dev/varka_bench_regen.sh sql VarkaColdStartBenchmark
 * </pre>
 */
public class VarkaColdStartBenchmark extends VarkaSqlBenchmarkBase {

  private static final boolean SMOKE = "true".equals(System.getenv("VARKA_COLDSTART_SMOKE"));

  /**
   * Rows per query: enough that a run is a query with batches and a JIT warmup rather than a
   * plan, and a twentieth of the ladder's so the first run is not lost in the steady state.
   */
  private static final int NUM_ROWS = SMOKE ? 10_000 : 100_000;
  private static final int REPETITIONS = SMOKE ? 2 : 5;
  private static final List<Integer> RUNGS = SMOKE ? List.of(16) : VarkaSizeLadder.RUNGS;

  /** The rungs the back-to-back section runs, and how many queries it runs on each arm. */
  private static final List<Integer> SERIES_RUNGS = SMOKE ? List.of(16) : List.of(16, 54, 100);
  private static final int SERIES_LENGTH = SMOKE ? 4 : 15;

  /** Rows for the steady-state section: the ladder's own. */
  private static final int STEADY_ROWS = SMOKE ? 20_000 : VarkaSizeLadder.NUM_ROWS;

  /**
   * The rung's query with offsets no earlier iteration used, so nothing about it is cached. The
   * offsets are the {@code add_months} month counts too, and the Varka kernel runs a batch only
   * while a month count is within {@code VarkaChrono.MONTH_ARITH_MAX_MONTHS}: past it the batch is
   * handed to Spark's row path, and the Varka arm would time vanilla's code. So consecutive
   * iterations are a hundred months apart - more than any rung's width - and the largest one
   * stays under the bound.
   */
  private static String query(int n, int iteration) {
    int base = 100 * (iteration + 1);
    return "SELECT "
        + IntStream.rangeClosed(1, n).mapToObj(k -> entry(base + k))
            .collect(Collectors.joining(", "))
        + " FROM ladder_dates";
  }

  /*
   * The iteration offsets the cases use: FIRST_RUN + t, SECOND_RUN + t, COMPILED + t and
   * PLAN_ONLY + t for iteration t, and SERIES for the back-to-back section. Ten apart, so no two
   * cases share a query while every offset stays far inside the month bound.
   */
  private static final int FIRST_RUN = 0;
  private static final int SECOND_RUN = 10;
  private static final int COMPILED = 20;
  private static final int PLAN_ONLY = 30;
  private static final int SERIES = 40;
  private static final int STEADY = 50;

  /**
   * The range filter's rungs: under vanilla's crossing, and the query's own count. The smoke run
   * takes 49, the smallest count the split design splits at, so that it reaches that design's
   * multi-method kernel.
   */
  private static final List<Integer> RANGE_RUNGS = SMOKE ? List.of(49) : List.of(48, 200);

  /** One design of VARKA-172 for a disjunction of ranges, and the options that emit it alone. */
  private record Design(String name, VarkaEmitOptions options) {}

  /**
   * The two designs, each emitted with the other off: the range set, one loop over a table of
   * bounds, and the split conditions.
   */
  private static final List<Design> DESIGNS = List.of(
      new Design("range set", VarkaEmitOptions.DEFAULTS.withSplitConditions(false)),
      new Design("split conditions", VarkaArrowSessions.splitConditionsDesign()));

  /**
   * The filter of {@code n} of {@code modified-q3}'s ranges over an Arrow-cached key column, with
   * every bound shifted by {@code iteration} days: a different source for each iteration, since a
   * range's literals are in vanilla's generated code and Janino would otherwise serve its cache. A
   * uniform shift leaves the ranges as they are to each other; what it can change is the
   * selectivity at the top of the key domain, where the last range ends, by a fraction of one
   * range in two hundred at these offsets.
   */
  private static String rangeQuery(int n, int iteration) {
    var clauses = new ArrayList<String>(n);
    for (Tuple2<Object, Object> range
        : CollectionConverters.asJava(VarkaQ3Ranges.ranges()).subList(0, n)) {
      int a = (Integer) range._1();
      int b = (Integer) range._2();
      clauses.add("ss_sold_date_sk between " + (a + iteration) + " and " + (b + iteration));
    }
    return "SELECT ss_sold_date_sk FROM range_keys WHERE " + String.join(" or ", clauses);
  }

  /** How long to wait for a warm-up's verdict: a hundred-entry kernel takes seconds. */
  private static final long WARMUP_TIMEOUT_MILLIS = 120_000L;

  /**
   * One Varka arm of the first-query cases: its label, its session, the emit options its
   * queries plan under, and the design's name where a section compares designs, which joins
   * the case names and the verdict lines; empty where none does.
   */
  private record Arm(String label, SparkSession session, VarkaEmitOptions options,
      String design) {
    Arm(String label, SparkSession session) {
      this(label, session, VarkaEmitOptions.DEFAULTS, "");
    }

    String name(String tail) {
      return design.isEmpty() ? label + ", " + tail : label + ", " + design + ", " + tail;
    }
  }

  /**
   * The first-query cases of one rung, {@code q} the query of an iteration's offset: vanilla's
   * plan only, first run and second run, then for each Varka arm the same three under its options
   * and, on the warm-up session, once compiled, whose verdict per iteration goes to {@code
   * verdicts} and whose last iteration checks the kernel serves the query - there, while its
   * shape is still cached, since a later arm's cases empty the cache.
   */
  private static void addFirstQueryCases(VarkaBenchmark benchmark, SparkSession baseline,
      SparkSession warmup, List<String> verdicts, IntFunction<String> q, List<Arm> arms) {
    benchmark.addTimerCase("vanilla Spark, plan only", timer -> {
      String query = q.apply(PLAN_ONLY + timer.iteration());
      quiesce();
      timer.startTiming();
      baseline.sql(query).queryExecution().executedPlan();
      timer.stopTiming();
    });
    benchmark.addTimerCase("vanilla Spark, first run", timer -> {
      String query = q.apply(FIRST_RUN + timer.iteration());
      quiesce();
      timer.startTiming();
      noop(baseline.sql(query));
      timer.stopTiming();
    });
    benchmark.addTimerCase("vanilla Spark, second run", timer -> {
      String query = q.apply(SECOND_RUN + timer.iteration());
      quiesce();
      noop(baseline.sql(query));
      timer.startTiming();
      noop(baseline.sql(query));
      timer.stopTiming();
    });
    for (Arm arm : arms) {
      benchmark.addTimerCase(arm.name("plan only"), timer -> {
        String query = q.apply(PLAN_ONLY + timer.iteration());
        VarkaShapeCache.invalidateAll();
        quiesce();
        timer.startTiming();
        withEmitOptions(arm.options(),
            () -> arm.session().sql(query).queryExecution().executedPlan());
        timer.stopTiming();
      });
      benchmark.addTimerCase(arm.name("first run"), timer -> {
        String query = q.apply(FIRST_RUN + timer.iteration());
        VarkaShapeCache.invalidateAll();
        quiesce();
        timer.startTiming();
        withEmitOptions(arm.options(), () -> noop(arm.session().sql(query)));
        timer.stopTiming();
      });
      benchmark.addTimerCase(arm.name("second run"), timer -> {
        String query = q.apply(SECOND_RUN + timer.iteration());
        VarkaShapeCache.invalidateAll();
        quiesce();
        withEmitOptions(arm.options(), () -> noop(arm.session().sql(query)));
        timer.startTiming();
        withEmitOptions(arm.options(), () -> noop(arm.session().sql(query)));
        timer.stopTiming();
      });
      if (arm.session() == warmup) {
        benchmark.addTimerCase(arm.name("once compiled"), timer -> {
          String query = q.apply(COMPILED + timer.iteration());
          VarkaShapeCache.invalidateAll();
          quiesce();
          withEmitOptions(arm.options(), () -> noop(warmup.sql(query)));
          String verdict = describe(awaitVerdict());
          verdicts.add(arm.design().isEmpty() ? verdict : arm.design() + ": " + verdict);
          quiesce();
          timer.startTiming();
          withEmitOptions(arm.options(), () -> noop(warmup.sql(query)));
          timer.stopTiming();
          if (timer.iteration() == REPETITIONS - 1) {
            withEmitOptions(arm.options(), () -> checkKernelServed(warmup, query));
          }
        });
      }
    }
  }

  /** Waits for the warm-up's verdict and returns it; fails the run if it never came. */
  private static VarkaKernelWarmup.Outcome awaitVerdict() throws InterruptedException {
    require(VarkaKernelWarmup.awaitIdle(WARMUP_TIMEOUT_MILLIS),
        "no warm-up verdict in " + WARMUP_TIMEOUT_MILLIS / 1000 + " seconds");
    return VarkaKernelWarmup.recentOutcomes().getLast();
  }

  private static String describe(VarkaKernelWarmup.Outcome o) {
    return o.state() + " after " + o.runNanos() / 1_000_000 + " ms and " + o.calls() + " calls";
  }

  /**
   * Runs {@code q} on {@code session} once more and checks that its kernel served every batch:
   * none on the row path, warming or falling back. The ladder's own check ({@link
   * VarkaSizeLadder#varkaFused}) runs its query outside an SQL execution, and so under the
   * default class loader rather than the session's own, which the timed queries run under - a
   * different entry of the shape cache, whose kernel no warm-up has touched. This one runs as the
   * timed queries do.
   */
  private static void checkKernelServed(SparkSession session, String q) {
    QueryExecution qe = session.sql(q).queryExecution();
    SQLExecution.withNewExecutionId(qe, Option.apply("check"), () -> qe.toRdd().count());
    Option<SparkPlan> node = qe.executedPlan().find(p -> p instanceof VarkaColumnarToRowExec
        || p instanceof VarkaProjectExec || p instanceof VarkaFilterExecBase);
    if (node.isEmpty()) {
      throw new IllegalStateException("no Varka node:\n" + qe.executedPlan().treeString());
    }
    var counts = new TreeMap<String, Long>();
    long fallbacks = 0;
    for (Map.Entry<String, SQLMetric> metric
        : CollectionConverters.asJava(node.get().metrics()).entrySet()) {
      String k = metric.getKey();
      if (k.endsWith("Batches") || k.startsWith("numFallbackBatches")) {
        counts.put(k, metric.getValue().value());
      }
      if (k.startsWith("numFallbackBatches")) {
        fallbacks += metric.getValue().value();
      }
    }
    require(count(counts, "numVarkaBatches") > 0 && count(counts, "numWarmupBatches") == 0
        && fallbacks == 0, "the compiled kernel did not serve every batch of " + q + ": " + counts);
  }

  /** A metric's value, refusing a name the node does not report. */
  private static long count(Map<String, Long> counts, String name) {
    Long value = counts.get(name);
    if (value == null) {
      throw new IllegalStateException("no metric " + name + " in " + counts);
    }
    return value;
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    // These arms own their Arrow-backed sessions, as the ladder's do; the default session, with
    // the default cache serializer, is never made.
    stopLeftoverSessions();
    SparkSession baseline = VarkaArrowSessions.createSession(
        "VarkaColdStart-vanilla", false, false, "local[1]");
    clearSessions();
    SparkSession varka = VarkaArrowSessions.createSession(
        "VarkaColdStart-varka", true, false, "local[1]");
    clearSessions();
    SparkSession warmup = VarkaArrowSessions.createSession(
        "VarkaColdStart-warmup", true, true, "local[1]");
    clearSessions();
    require(baseline != varka && varka != warmup && baseline != warmup,
        "the three sessions must be distinct or there is nothing to compare");
    require(REPETITIONS <= SECOND_RUN - FIRST_RUN, "the offset families would overlap");
    int largest = 100 * (STEADY + 1) + Collections.max(RUNGS);
    require(largest <= VarkaChrono.MONTH_ARITH_MAX_MONTHS, "offsets up to " + largest
        + " pass the kernel's month bound " + VarkaChrono.MONTH_ARITH_MAX_MONTHS);
    try {
      cacheDates(baseline, NUM_ROWS);
      cacheDates(varka, NUM_ROWS);
      cacheDates(warmup, NUM_ROWS);
      section("the first query: greatest(add_months(d, k), date_add(d, k), last_day(d))",
          () -> firstQuery(baseline, varka, warmup));
      for (SparkSession session : List.of(baseline, varka, warmup)) {
        VarkaArrowSessions.cacheRangeKeys(session, NUM_ROWS);
      }
      section("the first query of modified-q3's filter: the range set against the split "
          + "conditions", () -> firstRangeQuery(baseline, varka, warmup));
      section("the same shape back to back: ms per query",
          () -> backToBack(baseline, varka, warmup));
      for (SparkSession session : List.of(varka, warmup)) {
        session.catalog().uncacheTable("ladder_dates");
        cacheDates(session, STEADY_ROWS);
      }
      section("steady state: the kernel the warm-up compiled against the batches' own",
          () -> steadyState(varka, warmup));
    } finally {
      baseline.stop();
      varka.stop();
      warmup.stop();
    }
  }

  /** The first section: the projection's first-query cases at each rung. */
  private void firstQuery(SparkSession baseline, SparkSession varka, SparkSession warmup) {
    for (int n : RUNGS) {
      // Every entry fuses and the kernel runs, at the largest offset each executed case uses:
      // the offsets grow with the iteration, so the last one is the one that could fall back.
      for (int family : List.of(FIRST_RUN, SECOND_RUN, COMPILED)) {
        int last = family + REPETITIONS - 1;
        int fused = varkaFused(varka, query(n, last));
        require(fused == n,
            "the Varka arm fused " + fused + " of " + n + " entries at iteration " + last);
      }
      VarkaBenchmark benchmark = firstQueryBenchmark(
          n + " entries over " + NUM_ROWS + " Arrow-cached rows");
      var verdicts = new ArrayList<String>();
      addFirstQueryCases(benchmark, baseline, warmup, verdicts, t -> query(n, t),
          List.of(new Arm("Varka", varka), new Arm("Varka with warm-up", warmup)));
      runCases(benchmark);
      note("rung " + n + ": the warm-up's verdicts, one per once-compiled iteration: "
          + String.join("; ", verdicts));
    }
  }

  /** The second section: the range filter's first-query cases, each design on each Varka arm. */
  private void firstRangeQuery(SparkSession baseline, SparkSession varka, SparkSession warmup) {
    for (int n : RANGE_RUNGS) {
      // Each design fuses and its kernel runs. One query per design: a range's literals are no
      // part of the shape, so what fuses at one offset fuses at every one, where the projection
      // above checks three offsets for its month bound.
      for (Design design : DESIGNS) {
        String q = rangeQuery(n, COMPILED + REPETITIONS - 1);
        Tuple2<Object, String> filtered = VarkaArrowSessions.withEmitOptions(design.options(),
            () -> VarkaArrowSessions.varkaFilters(varka, q));
        require((Boolean) filtered._1(), "the " + design.name()
            + " arm declined the filter of " + n + " ranges:\n" + filtered._2());
      }
      VarkaBenchmark benchmark = firstQueryBenchmark(
          n + " ranges over " + NUM_ROWS + " Arrow-cached rows");
      var verdicts = new ArrayList<String>();
      var arms = new ArrayList<Arm>();
      for (Design design : DESIGNS) {
        arms.add(new Arm("Varka", varka, design.options(), design.name()));
        arms.add(new Arm("Varka with warm-up", warmup, design.options(), design.name()));
      }
      addFirstQueryCases(benchmark, baseline, warmup, verdicts, t -> rangeQuery(n, t), arms);
      runCases(benchmark);
      note("rung " + n + " ranges: the warm-up's verdicts, one per once-compiled iteration: "
          + String.join("; ", verdicts));
    }
  }

  /** A first-query table: each iteration one event, printed, with no warm-up and no minimum. */
  private VarkaBenchmark firstQueryBenchmark(String name) {
    return benchmark(name, NUM_ROWS).minIterations(REPETITIONS).warmup(Duration.ZERO)
        .minTime(Duration.ZERO).outputPerIteration(true).build();
  }

  /** The third section: one shape run back to back on each arm, every query's time printed. */
  private void backToBack(SparkSession baseline, SparkSession varka, SparkSession warmup) {
    record Series(String arm, SparkSession session) {}
    for (int n : SERIES_RUNGS) {
      note(n + " entries over " + NUM_ROWS + " Arrow-cached rows, " + SERIES_LENGTH
          + " queries of one shape back to back, in ms:");
      for (Series series : List.of(new Series("vanilla Spark", baseline),
          new Series("Varka", varka), new Series("Varka with warm-up", warmup))) {
        VarkaShapeCache.invalidateAll();
        quiesceOrFail();
        String q = query(n, SERIES);
        var times = new ArrayList<String>(SERIES_LENGTH);
        for (int i = 0; i < SERIES_LENGTH; i++) {
          long start = System.nanoTime();
          noop(series.session().sql(q));
          times.add(Long.toString((System.nanoTime() - start) / 1_000_000));
        }
        note("  %-20s %s".formatted(series.arm(), String.join(" ", times)));
        if (series.session() == warmup) {
          note("  the warm-up's verdict: " + describe(verdict()));
        }
      }
    }
  }

  /**
   * The fourth section: the kernel its own batches compiled, timed under the ladder's warmup,
   * against the kernel the warm-up compiled, timed straight after its verdict.
   */
  private void steadyState(SparkSession varka, SparkSession warmup) {
    for (int n : SERIES_RUNGS) {
      String q = query(n, STEADY);
      VarkaShapeCache.invalidateAll();
      quiesceOrFail();
      VarkaBenchmark byBatches = benchmark(n + " entries over " + STEADY_ROWS
          + " Arrow-cached rows, compiled from its batches", STEADY_ROWS)
          .minIterations(REPETITIONS).warmup(Duration.ofSeconds(2)).build();
      byBatches.addCase("Varka, after two seconds of its own batches", i -> noop(varka.sql(q)));
      runCases(byBatches);
      VarkaShapeCache.invalidateAll();
      quiesceOrFail();
      noop(warmup.sql(q));
      VarkaKernelWarmup.Outcome verdict = verdict();
      quiesceOrFail();
      VarkaBenchmark byWarmup = benchmark(n + " entries over " + STEADY_ROWS
          + " Arrow-cached rows, compiled from the warm-up", STEADY_ROWS)
          .minIterations(REPETITIONS).warmup(Duration.ZERO).minTime(Duration.ZERO).build();
      byWarmup.addCase("Varka with warm-up, straight after its verdict",
          i -> noop(warmup.sql(q)));
      runCases(byWarmup);
      note("rung " + n + ": the warm-up's verdict: " + describe(verdict));
      checkKernelServed(warmup, q);
    }
  }

  /** {@link #awaitVerdict} outside a timer case, which declares its interrupt. */
  private static VarkaKernelWarmup.Outcome verdict() {
    try {
      return awaitVerdict();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while waiting for a warm-up's verdict", e);
    }
  }
}
