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

import static org.apache.spark.sql.execution.benchmark.VarkaColdPath.STEADY;
import static org.apache.spark.sql.execution.benchmark.VarkaColdPath.noKernel;
import static org.apache.spark.sql.execution.benchmark.VarkaColdPath.query;
import static org.apache.spark.sql.execution.benchmark.VarkaColdPath.rowByRow;
import static org.apache.spark.sql.execution.benchmark.VarkaColdPath.rowsPlan;
import static org.apache.spark.sql.execution.benchmark.VarkaColdPath.toNoop;
import static org.apache.spark.sql.execution.benchmark.VarkaColdPath.toRows;
import static org.apache.spark.sql.execution.benchmark.VarkaSizeLadder.cacheDates;
import static org.apache.spark.sql.execution.benchmark.VarkaSizeLadder.quiesce;
import static org.apache.spark.sql.execution.benchmark.VarkaSizeLadder.quiesceOrFail;
import static org.apache.spark.sql.execution.benchmark.VarkaSizeLadder.varkaFused;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import scala.Option;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.BoxedUnit;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.VarkaBenchmark;
import org.apache.spark.sql.catalyst.expressions.codegen.ByteCodeStats;
import org.apache.spark.sql.catalyst.expressions.codegen.CodeAndComment;
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaChrono;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelWarmup;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaShapeCache;
import org.apache.spark.sql.execution.ColumnarToRowExec;
import org.apache.spark.sql.execution.ProjectExec;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.VarkaColumnarToRowExec;
import org.apache.spark.sql.execution.VarkaProjectExec;
import org.apache.spark.sql.execution.WholeStageCodegenExec;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.util.QueryExecutionListener;

/**
 * What a new shape's batches cost on each path they can take before its kernel is compiled,
 * the admission check of VARKA-228 (see row 228 of {@code m6/PLAN.md}).
 *
 * <p>While a shape's kernel warms, and whenever a batch falls back, a Varka node evaluates its
 * projection row by row, with Spark's {@code UnsafeProjection} over each batch's {@code
 * ColumnarBatchRow}. Which node that is depends on the consumer. Under a consumer of rows the
 * planner puts {@code VarkaColumnarToRowExec}, which hands the projected rows on; under a consumer
 * of batches it keeps {@code VarkaProjectExec}, which writes them back into column vectors.
 * Vanilla Spark scans the same Arrow cache row by row - the cache's row reader writes each row's
 * fields into an {@code UnsafeRow} - and projects in one whole-stage method, which HotSpot
 * compiles only while the method is at most 8000 bytes of bytecode ({@code -XX:HugeMethodLimit}).
 *
 * <p>At the size ladder's rungs, over the cold-start benchmark's hundred thousand Arrow-cached
 * rows:
 *
 * <ul>
 *   <li><b>rows</b> ({@code toRdd} in an SQL execution, every row consumed as a row, the control
 *       the other Varka benchmarks use to force the to-row node): first run and steady state of
 *       vanilla's whole-stage code, of vanilla with whole-stage codegen off, and of {@code
 *       VarkaColumnarToRowExec} with no kernel; the steady state also times the node with its
 *       compiled kernel.</li>
 *   <li><b>a columnar sink</b> ({@code noop}, which takes batches, the sink the other Varka
 *       benchmarks write to, the cold-start one included): the steady state of the same vanilla
 *       arms, and of {@code VarkaProjectExec} with no kernel and with its compiled kernel.</li>
 *   <li><b>the whole-stage class's compile</b>: Janino's compile of vanilla's generated class for
 *       the rung's projection, from new source every iteration, which vanilla's first run pays,
 *       and the size of the class's largest method, which decides whether HotSpot compiles it at
 *       all.</li>
 * </ul>
 *
 * <p>"No kernel" is the evaluator's emission-failure test hook: the node cannot obtain a kernel,
 * every batch takes its row path, and no warm-up runs beside it. {@code VarkaColdStartBenchmark}
 * times the same paths while a warm-up competes with them for the compiler. Before each rung the
 * checks run the plans the timed cases run and read their nodes' metrics, so a case that timed
 * another node or another path fails the run.
 *
 * <p>A first run takes offsets no earlier query used, so its generated source is new and Janino
 * compiles it, and each arm has offsets of its own, so that no arm finds another arm's class in
 * the code cache. Every first run starts with the JIT quiet ({@link VarkaSizeLadder#quiesce}).
 *
 * <p>{@code VARKA_COLDPATH_SMOKE=true} in the environment, which the forked benchmark JVM
 * inherits where a {@code -D} on sbt's command line does not, runs one rung over ten thousand
 * rows twice, to check the benchmark itself and not to measure anything. The dry run, {@code
 * -Dvarka.bench.dryRun=true}, makes the checks before each rung and times nothing; the kernel is
 * never warmed then, so the check that it served the steady state is skipped and the verdict line
 * says so.
 *
 * <p>To run this benchmark:
 * <pre>
 *   dev/varka_bench_regen.sh sql VarkaColdPathBenchmark --no-narrow
 * </pre>
 */
public class VarkaColdPathBenchmark extends VarkaSqlBenchmarkBase {

  private static final boolean SMOKE = "true".equals(System.getenv("VARKA_COLDPATH_SMOKE"));

  private static final int NUM_ROWS = SMOKE ? 10_000 : 100_000;
  private static final int REPETITIONS = SMOKE ? 2 : 5;
  private static final List<Integer> RUNGS = SMOKE ? List.of(16) : VarkaSizeLadder.RUNGS;
  private static final Duration STEADY_WARMUP =
      SMOKE ? Duration.ofMillis(500) : Duration.ofSeconds(2);

  /*
   * The offset families: one per arm's first run, so each compiles its own source; the steady
   * state's, which every arm shares, is VarkaColdPath.STEADY. The compile section's queries are
   * generated and never run, so they count up from COMPILES without regard to the month bound.
   */
  private static final int VANILLA_FIRST = 0;
  private static final int ROWWISE_FIRST = 10;
  private static final int ROW_PATH_FIRST = 20;
  private static final int COMPILES = 1000;

  /** How long to wait for a warm-up's verdict: a hundred-entry kernel takes seconds. */
  private static final long WARMUP_TIMEOUT_MILLIS = 120_000L;

  /** A sink of the steady-state tables: its label, how a query runs into it, its Varka node. */
  private record Sink(String label, BiConsumer<SparkSession, String> run, String node) {}

  private static Optional<WholeStageCodegenExec> wholeStage(SparkSession session, String q) {
    Option<SparkPlan> stage = session.sql(q).queryExecution().executedPlan()
        .find(p -> p instanceof WholeStageCodegenExec);
    return stage.isDefined()
        ? Optional.of((WholeStageCodegenExec) stage.get()) : Optional.empty();
  }

  /** Runs {@code q} as {@code toNoop} does and returns the write's plan, from its listener. */
  private static SparkPlan noopPlan(SparkSession session, String q) {
    var captured = new AtomicReference<SparkPlan>();
    QueryExecutionListener listener = new QueryExecutionListener() {
      @Override
      public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
        captured.set(qe.executedPlan());
      }

      @Override
      public void onFailure(String funcName, QueryExecution qe, Exception error) {}
    };
    session.listenerManager().register(listener);
    try {
      toNoop(session, q);
      session.sparkContext().listenerBus().waitUntilEmpty();
    } catch (TimeoutException e) {
      throw new IllegalStateException("the listener bus did not drain after the write: " + q, e);
    } finally {
      session.listenerManager().unregister(listener);
    }
    SparkPlan plan = captured.get();
    if (plan == null) {
      throw new IllegalStateException("no plan for the write: " + q);
    }
    return plan;
  }

  /** The metrics of the plan's one Varka node, which must be of the class the sink plans. */
  private static Map<String, Long> nodeMetrics(
      SparkPlan plan, Class<? extends SparkPlan> expected) {
    var nodes = new ArrayList<SparkPlan>();
    plan.foreach(p -> {
      if (p instanceof VarkaColumnarToRowExec || p instanceof VarkaProjectExec) {
        nodes.add(p);
      }
      return BoxedUnit.UNIT;
    });
    require(nodes.size() == 1 && expected.isInstance(nodes.getFirst()),
        "expected one " + expected.getSimpleName() + ":\n" + plan.treeString());
    var counts = new TreeMap<String, Long>();
    for (Map.Entry<String, SQLMetric> metric
        : CollectionConverters.asJava(nodes.getFirst().metrics()).entrySet()) {
      counts.put(metric.getKey(), metric.getValue().value());
    }
    return Collections.unmodifiableMap(counts);
  }

  /** A metric's value, refusing a name the node does not report. */
  private static long count(Map<String, Long> counts, String name) {
    Long value = counts.get(name);
    if (value == null) {
      throw new IllegalStateException("no metric " + name + " in " + counts);
    }
    return value;
  }

  private static long fallbacks(Map<String, Long> counts) {
    long sum = 0;
    for (Map.Entry<String, Long> c : counts.entrySet()) {
      if (c.getKey().startsWith("numFallbackBatches")) {
        sum += c.getValue();
      }
    }
    return sum;
  }

  /**
   * The whole-stage arm compiles the projection over the scan's rows, with no columnar
   * transition between them; the row-by-row arm generates no stage.
   */
  private static void checkVanilla(SparkSession baseline, String q) {
    SparkPlan plan = baseline.sql(q).queryExecution().executedPlan();
    Option<SparkPlan> stage = plan.find(p -> p instanceof WholeStageCodegenExec);
    require(stage.isDefined()
        && ((WholeStageCodegenExec) stage.get()).child() instanceof ProjectExec
        && !plan.exists(p -> p instanceof ColumnarToRowExec),
        "vanilla's plan is not a whole-stage projection over the scan's rows:\n"
            + plan.treeString());
    var rowwise = new AtomicReference<Optional<WholeStageCodegenExec>>();
    rowByRow(baseline, () -> rowwise.set(wholeStage(baseline, q)));
    require(rowwise.get().isEmpty(), "whole-stage codegen off still generated a stage: " + q);
  }

  /** A sink's executed plan, and the class of the one Varka node it must hold. */
  private record Planned(SparkPlan plan, Class<? extends SparkPlan> node) {}

  /** Under each sink, the no-kernel arm's node took its row path for every batch. */
  private static void checkNoKernel(SparkSession varka, String q) {
    var planned = new ArrayList<Planned>();
    noKernel(() -> planned.add(new Planned(rowsPlan(varka, q), VarkaColumnarToRowExec.class)));
    noKernel(() -> planned.add(new Planned(noopPlan(varka, q), VarkaProjectExec.class)));
    for (Planned p : planned) {
      Class<? extends SparkPlan> node = p.node();
      Map<String, Long> counts = nodeMetrics(p.plan(), node);
      require(count(counts, "numInputBatches") > 0 && count(counts, "numVarkaBatches") == 0
          && count(counts, "numWarmupBatches") == 0 && count(counts, "numEmissionFailures") > 0,
          node.getSimpleName() + " with no kernel did not take its row path for every batch: "
              + counts);
    }
  }

  /** Under each sink, the compiled kernel served every batch: none warming or falling back. */
  private static void checkKernelServed(SparkSession warmup, String q) {
    List<Planned> planned = List.of(
        new Planned(rowsPlan(warmup, q), VarkaColumnarToRowExec.class),
        new Planned(noopPlan(warmup, q), VarkaProjectExec.class));
    for (Planned p : planned) {
      Class<? extends SparkPlan> node = p.node();
      Map<String, Long> counts = nodeMetrics(p.plan(), node);
      require(count(counts, "numVarkaBatches") > 0 && count(counts, "numWarmupBatches") == 0
          && fallbacks(counts) == 0,
          "the compiled kernel did not serve every batch of " + node.getSimpleName() + ": "
              + counts);
    }
  }

  /** Runs {@code start} and waits for the warm-up it queued: its verdict, or none if it queued
   *  none. */
  private static Optional<VarkaKernelWarmup.Outcome> warmUp(Runnable start)
      throws InterruptedException {
    VarkaKernelWarmup.Outcome before = last(VarkaKernelWarmup.recentOutcomes());
    start.run();
    require(VarkaKernelWarmup.awaitIdle(WARMUP_TIMEOUT_MILLIS),
        "no warm-up verdict in " + WARMUP_TIMEOUT_MILLIS / 1000 + " seconds");
    VarkaKernelWarmup.Outcome after = last(VarkaKernelWarmup.recentOutcomes());
    // By identity, as the outcomes are kept: an equal verdict of a later warm-up is a new one.
    return after == null || after == before ? Optional.empty() : Optional.of(after);
  }

  private static VarkaKernelWarmup.Outcome last(List<VarkaKernelWarmup.Outcome> outcomes) {
    return outcomes.isEmpty() ? null : outcomes.getLast();
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    // These arms own their Arrow-backed sessions, as the ladder's do; the default session, with
    // the default cache serializer, is never made.
    stopLeftoverSessions();
    SparkSession baseline = VarkaArrowSessions.createSession(
        "VarkaColdPath-vanilla", false, false, "local[1]");
    clearSessions();
    SparkSession varka = VarkaArrowSessions.createSession(
        "VarkaColdPath-varka", true, false, "local[1]");
    clearSessions();
    SparkSession warmup = VarkaArrowSessions.createSession(
        "VarkaColdPath-warmup", true, true, "local[1]");
    clearSessions();
    require(baseline != varka && varka != warmup && baseline != warmup,
        "the three sessions must be distinct or there is nothing to compare");
    require(REPETITIONS <= ROWWISE_FIRST - VANILLA_FIRST, "the offset families would overlap");
    int largest = 100 * (STEADY + 1) + Collections.max(RUNGS);
    require(largest <= VarkaChrono.MONTH_ARITH_MAX_MONTHS, "offsets up to " + largest
        + " pass the kernel's month bound " + VarkaChrono.MONTH_ARITH_MAX_MONTHS);
    Runnable restoreEvaluator = VarkaColdPath.silenceEvaluator();
    try {
      cacheDates(baseline, NUM_ROWS);
      cacheDates(varka, NUM_ROWS);
      cacheDates(warmup, NUM_ROWS);
      section("the cold path: greatest(add_months(d, k), date_add(d, k), last_day(d))",
          () -> coldPath(baseline, varka, warmup));
      section("the whole-stage class's compile: Janino, from new source every iteration",
          () -> compiles(baseline));
    } finally {
      restoreEvaluator.run();
      baseline.stop();
      varka.stop();
      warmup.stop();
    }
  }

  /** The first section: each rung's first runs, then its steady state under each sink. */
  private void coldPath(SparkSession baseline, SparkSession varka, SparkSession warmup) {
    for (int n : RUNGS) {
      String steadyQuery = query(n, STEADY);
      for (int family : List.of(ROW_PATH_FIRST + REPETITIONS - 1, STEADY)) {
        String q = query(n, family);
        int fused = varkaFused(varka, q);
        require(fused == n, "the Varka arm fused " + fused + " of " + n + " entries: " + q);
      }
      checkVanilla(baseline, steadyQuery);
      checkNoKernel(varka, steadyQuery);

      VarkaBenchmark first = benchmark(
          n + " entries over " + NUM_ROWS + " Arrow-cached rows, rows, first run", NUM_ROWS)
          .minIterations(REPETITIONS).warmup(Duration.ZERO).minTime(Duration.ZERO)
          .outputPerIteration(true).build();
      first.addTimerCase("vanilla Spark, whole-stage code", timer -> {
        String q = query(n, VANILLA_FIRST + timer.iteration());
        quiesce(WARMUP_TIMEOUT_MILLIS);
        timer.startTiming();
        toRows(baseline, q);
        timer.stopTiming();
      });
      first.addTimerCase("vanilla Spark, whole-stage codegen off", timer -> {
        String q = query(n, ROWWISE_FIRST + timer.iteration());
        quiesce(WARMUP_TIMEOUT_MILLIS);
        rowByRow(baseline, () -> {
          timer.startTiming();
          toRows(baseline, q);
          timer.stopTiming();
        });
      });
      first.addTimerCase("Varka to rows, no kernel", timer -> {
        String q = query(n, ROW_PATH_FIRST + timer.iteration());
        quiesce(WARMUP_TIMEOUT_MILLIS);
        noKernel(() -> {
          timer.startTiming();
          toRows(varka, q);
          timer.stopTiming();
        });
      });
      runCases(first);

      // The kernel is warmed on the first call of its case, inside the harness's warmup, so that
      // no case before it runs beside the warm-up's compiles; the columnar table's kernel case
      // then finds the same kernel compiled, since the two nodes share it. Null until it ran.
      var verdict = new AtomicReference<Optional<VarkaKernelWarmup.Outcome>>();
      List<Sink> sinks = List.of(
          new Sink("rows", VarkaColdPath::toRows, "Varka to rows"),
          new Sink("a columnar sink", VarkaColdPath::toNoop, "Varka columnar"));
      for (Sink sink : sinks) {
        quiesceOrFail();
        VarkaBenchmark table = benchmark(n + " entries over " + NUM_ROWS
            + " Arrow-cached rows, " + sink.label() + ", steady state", NUM_ROWS)
            .minIterations(REPETITIONS).warmup(STEADY_WARMUP).build();
        table.addTimerCase("vanilla Spark, whole-stage code", timer -> {
          timer.startTiming();
          sink.run().accept(baseline, steadyQuery);
          timer.stopTiming();
        });
        table.addTimerCase("vanilla Spark, whole-stage codegen off", timer ->
            rowByRow(baseline, () -> {
              timer.startTiming();
              sink.run().accept(baseline, steadyQuery);
              timer.stopTiming();
            }));
        table.addTimerCase(sink.node() + ", no kernel", timer ->
            noKernel(() -> {
              timer.startTiming();
              sink.run().accept(varka, steadyQuery);
              timer.stopTiming();
            }));
        table.addTimerCase(sink.node() + ", the kernel once compiled", timer -> {
          if (verdict.get() == null) {
            VarkaShapeCache.invalidateAll();
            verdict.set(warmUp(() -> sink.run().accept(warmup, steadyQuery)));
            quiesce(WARMUP_TIMEOUT_MILLIS);
          }
          timer.startTiming();
          sink.run().accept(warmup, steadyQuery);
          timer.stopTiming();
        });
        runCases(table);
      }
      String said;
      if (verdict.get() == null) {
        // No kernel case ran: a dry run times none, and -Dvarka.bench.only may have dropped
        // both, so there is no warmed kernel to check.
        said = dryRun() ? "not taken, a dry run times nothing"
            : "not taken, -Dvarka.bench.only ran no kernel case";
      } else {
        checkKernelServed(warmup, steadyQuery);
        said = verdict.get().map(o -> o.state() + " after " + o.runNanos() / 1_000_000
            + " ms and " + o.calls() + " calls").orElse("none, the kernel was compiled already");
      }
      note("rung " + n + ": the kernel's warm-up verdict: " + said);
    }
  }

  /** The second section: Janino's compile of vanilla's class for each rung, and its size. */
  private void compiles(SparkSession baseline) {
    for (int n : RUNGS) {
      var fresh = new AtomicInteger(COMPILES);
      var stats = new AtomicReference<ByteCodeStats>();
      VarkaBenchmark benchmark = benchmark(n + " entries: vanilla's whole-stage class", 1)
          .minIterations(REPETITIONS).warmup(STEADY_WARMUP).build();
      benchmark.addTimerCase("Janino compile of the generated class", timer -> {
        CodeAndComment code = source(baseline, query(n, fresh.incrementAndGet()));
        timer.startTiming();
        ByteCodeStats compiled = CodeGenerator.compile(code)._2();
        timer.stopTiming();
        stats.set(compiled);
      });
      runCases(benchmark);
      if (stats.get() == null) {
        // The case did not run, in a dry run or a filtered one; one compile of a fresh source
        // reads the same method size.
        stats.set(CodeGenerator.compile(source(baseline, query(n, fresh.incrementAndGet())))._2());
      }
      int bytes = stats.get().maxMethodCodeSize();
      int limit = CodeGenerator.DEFAULT_JVM_HUGE_METHOD_LIMIT();
      note("rung " + n + ": the whole-stage class's largest method is " + bytes + " bytes, "
          + (bytes > limit ? "past" : "under") + " HugeMethodLimit " + limit);
    }
  }

  /** Vanilla's generated source for {@code q}'s whole-stage class. */
  private static CodeAndComment source(SparkSession baseline, String q) {
    return wholeStage(baseline, q)
        .orElseThrow(() -> new IllegalStateException("no whole-stage class for " + q))
        .doCodeGen()._2();
  }
}
