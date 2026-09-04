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

package org.apache.spark.sql.varka.bench;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.util.QueryExecutionListener;
import scala.jdk.javaapi.CollectionConverters;

/**
 * The date-surface benchmark (task 62): every entry of {@link Surface}, in the projection shape
 * (written to the {@code noop} sink, which takes columnar batches) and the filter shape
 * (counted), over one cached table, timed by wall clock and by executor time, written in
 * Spark's harness format with a provenance block on top. Submitted with {@code spark-submit}
 * to any distribution; whether Varka is on is the session's configuration, and the plan of
 * every query is read back through {@code EXPLAIN} so the file says which entries the kernel
 * served.
 *
 * <pre>
 *   spark-submit --master local[1] --driver-memory 8g --class ...DateSurfaceBenchmark \
 *     varka-bench.jar --label spark-4.2.0 --rows 200000000 --out FILE [--iters 5]
 *     [--warmup-seconds 2] [--min-seconds 2] [--only REGEX] [--provenance key=value]...
 *     [--expect-fused] [--max-fixed-share PERCENT]
 * </pre>
 *
 * {@code --expect-fused} (the fork with Varka on) fails the run, after writing the file, when an
 * entry the surface marks as fused planned without a Varka node; {@code --max-fixed-share}
 * fails it when a Varka-planned row's fixed share, {@code (wall - executor) / wall}, is over the
 * given percent - the job-size rule, checked from the numbers the file carries.
 *
 * The row count is the job-size rule of PLAN_MILESTONE_4.md 2.29: enough rows that the job's
 * fixed cost is under 5% of every Varka row's wall time, which the file makes checkable by
 * printing executor time beside wall time.
 */
public final class DateSurfaceBenchmark {

  /** Sums {@code executorRunTime} over every task that ends; read around each iteration. */
  static final class ExecutorTime extends SparkListener {
    private final AtomicLong runMillis = new AtomicLong();

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
      TaskMetrics m = taskEnd.taskMetrics();
      if (m != null) {
        runMillis.addAndGet(m.executorRunTime());
      }
    }

    long millis() {
      return runMillis.get();
    }
  }

  /**
   * Sums the fork's batch metrics over every finished execution: {@code numVarkaBatches} (the
   * kernel served the batch) and every {@code numFallbackBatches*} (it did not). On stock Spark
   * no plan carries them and both stay 0. This is what tells a kernel run from a row-engine run
   * under a Varka node, which the plan alone cannot: a distribution without the engine jar
   * plans every entry through a Varka node and falls back on every batch.
   */
  static final class KernelBatches implements QueryExecutionListener {
    private final AtomicLong kernel = new AtomicLong();
    private final AtomicLong fallback = new AtomicLong();

    @Override
    public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
      walk(qe.executedPlan());
    }

    @Override
    public void onFailure(String funcName, QueryExecution qe, Exception exception) {
    }

    private void walk(SparkPlan plan) {
      Map<String, SQLMetric> metrics = CollectionConverters.asJava(plan.metrics());
      for (Map.Entry<String, SQLMetric> e : metrics.entrySet()) {
        if (e.getKey().equals("numVarkaBatches")) {
          kernel.addAndGet(e.getValue().value());
        } else if (e.getKey().startsWith("numFallbackBatches")) {
          fallback.addAndGet(e.getValue().value());
        }
      }
      for (SparkPlan child : CollectionConverters.asJava(plan.children())) {
        walk(child);
      }
    }

    long kernel() {
      return kernel.get();
    }

    long fallback() {
      return fallback.get();
    }
  }

  static final class Args {
    String label = "unnamed";
    long rows = 200_000_000L;
    Path out = null;
    int iters = 5;
    double warmupSeconds = 2.0;
    double minSeconds = 2.0;
    Pattern only = null;
    boolean expectFused = false;
    double maxFixedShare = Double.NaN;
    final Map<String, String> provenance = new LinkedHashMap<>();

    static Args parse(String[] argv) {
      Args a = new Args();
      for (int i = 0; i < argv.length; i++) {
        String k = argv[i];
        String v = i + 1 < argv.length ? argv[i + 1] : null;
        switch (k) {
          case "--label" -> a.label = need(k, v);
          case "--rows" -> a.rows = Long.parseLong(need(k, v));
          case "--out" -> a.out = Path.of(need(k, v));
          case "--iters" -> a.iters = Integer.parseInt(need(k, v));
          case "--warmup-seconds" -> a.warmupSeconds = Double.parseDouble(need(k, v));
          case "--min-seconds" -> a.minSeconds = Double.parseDouble(need(k, v));
          case "--only" -> a.only = Pattern.compile(need(k, v));
          case "--expect-fused" -> {
            a.expectFused = true;
            i--;
          }
          case "--max-fixed-share" -> a.maxFixedShare = Double.parseDouble(need(k, v));
          case "--provenance" -> {
            String kv = need(k, v);
            int eq = kv.indexOf('=');
            if (eq <= 0) {
              throw new IllegalArgumentException("--provenance wants key=value, got " + kv);
            }
            a.provenance.put(kv.substring(0, eq), kv.substring(eq + 1));
          }
          default -> throw new IllegalArgumentException("unknown argument " + k);
        }
        i++;
      }
      if (a.out == null) {
        throw new IllegalArgumentException("--out FILE is required");
      }
      return a;
    }

    private static String need(String k, String v) {
      if (v == null) {
        throw new IllegalArgumentException(k + " wants a value");
      }
      return v;
    }
  }

  private DateSurfaceBenchmark() {}

  public static void main(String[] argv) throws IOException {
    Args args = Args.parse(argv);
    double load = Provenance.loadAverage();
    SparkSession spark = SparkSession.builder().appName("VarkaDateSurface").getOrCreate();
    ExecutorTime executor = new ExecutorTime();
    spark.sparkContext().addSparkListener(executor);
    KernelBatches batches = new KernelBatches();
    spark.listenerManager().register(batches);
    Runnable drain = () -> {
      try {
        spark.sparkContext().listenerBus().waitUntilEmpty();
      } catch (java.util.concurrent.TimeoutException e) {
        throw new IllegalStateException("the listener bus did not drain", e);
      }
    };
    PrintStream log = System.out;
    try {
      buildTable(spark, args.rows);
      StringBuilder file = new StringBuilder();
      Map<String, String> prov =
          Provenance.collect(args.label, spark.version(), load, args.provenance);
      prov.put("rows", Long.toString(args.rows));
      prov.put("methodology", String.format(Locale.ROOT,
          "%d+ iterations over %.0fs windows after %.0fs warm-up; wall time by nanoTime, "
              + "executor time as the sum of TaskMetrics.executorRunTime over the iteration",
          args.iters, args.minSeconds, args.warmupSeconds));
      file.append(Provenance.format(prov)).append(System.lineSeparator());
      log.print(file);
      List<String> violations = new ArrayList<>();
      for (Surface.Entry entry : Surface.ENTRIES) {
        if (args.only != null && !args.only.matcher(entry.label()).find()) {
          continue;
        }
        String block = runEntry(spark, executor, batches, drain, entry, args, log, violations);
        file.append(block);
        log.print(block);
        log.flush();
      }
      Files.writeString(args.out, file.toString(), StandardCharsets.UTF_8);
      log.println("wrote " + args.out);
      if (!violations.isEmpty()) {
        violations.forEach(v -> log.println("VIOLATION: " + v));
        System.exit(3);
      }
    } finally {
      spark.stop();
    }
  }

  /**
   * The table: the generator {@code VarkaThroughputBenchmark} uses, in partitions of 4M rows so
   * a run is one job of many tasks rather than one task (the per-job cost is paid once either
   * way; the per-task cost stays in the executor's share where the file shows it).
   */
  static void buildTable(SparkSession spark, long rows) {
    long partitions = Math.max(1L, (rows + 4_000_000L - 1) / 4_000_000L);
    spark.sql(String.format(Locale.ROOT,
        "SELECT CASE WHEN id %% 31 = 0 THEN NULL"
            + " ELSE date_add(DATE'2020-01-01', CAST(id %% 1460 AS INT)) END AS d,"
            + " date_add(DATE'2021-01-01', CAST(id %% 1500 AS INT)) AS d2,"
            + " CAST(id %% 3650 AS INT) AS i"
            + " FROM range(0, %d, 1, %d)", rows, partitions))
        .createOrReplaceTempView("varka_dates");
    spark.catalog().cacheTable("varka_dates");
    spark.sql("SELECT count(*) FROM varka_dates").collect();
  }

  static String projectionQuery(Surface.Entry e) {
    return "SELECT " + e.projection() + " AS a FROM varka_dates";
  }

  /** The filter with a columnar consumer: the selected dates written to the noop sink. */
  static String filterColumnarQuery(Surface.Entry e) {
    return "SELECT d FROM varka_dates WHERE " + e.filter() + "";
  }

  static String filterQuery(Surface.Entry e) {
    return "SELECT count(*) FROM varka_dates WHERE " + e.filter();
  }

  /** Whether the physical plan of {@code query} has a Varka node, read through EXPLAIN. */
  static boolean plansVarka(SparkSession spark, String query) {
    List<Row> rows = spark.sql("EXPLAIN " + query).collectAsList();
    return !rows.isEmpty() && rows.get(0).getString(0).contains("Varka");
  }

  private static String runEntry(
      SparkSession spark, ExecutorTime executor, KernelBatches batches, Runnable drain,
      Surface.Entry entry, Args args, PrintStream log, List<String> violations) {
    long warmup = (long) (args.warmupSeconds * 1e9);
    long min = (long) (args.minSeconds * 1e9);
    List<Harness.Case> wall = new ArrayList<>();
    List<Harness.Case> exec = new ArrayList<>();
    List<String> plans = new ArrayList<>();
    List<String> shares = new ArrayList<>();
    if (entry.projection() != null) {
      String q = projectionQuery(entry);
      Dataset<Row> df = spark.sql(q);
      Runnable body = () -> df.write().format("noop").mode("overwrite").save();
      measureShape(spark, executor, batches, drain, "projection, columnar consumer", q, body,
          args, warmup, min, wall, exec, plans, shares, log, entry, violations);
    }
    if (entry.filter() != null) {
      String qc = filterColumnarQuery(entry);
      Dataset<Row> dfc = spark.sql(qc);
      Runnable bodyc = () -> dfc.write().format("noop").mode("overwrite").save();
      measureShape(spark, executor, batches, drain, "filter, columnar consumer", qc, bodyc,
          args, warmup, min, wall, exec, plans, shares, log, entry, violations);
      String q = filterQuery(entry);
      Dataset<Row> df = spark.sql(q);
      Runnable body = df::collect;
      measureShape(spark, executor, batches, drain, "filter, counted", q, body, args,
          warmup, min, wall, exec, plans, shares, log, entry, violations);
    }
    String name = entry.label() + " over " + args.rows + " rows";
    StringBuilder sb = new StringBuilder();
    sb.append(Harness.table(name, args.rows, wall)).append(System.lineSeparator());
    sb.append(Harness.table(name + ", executor time", args.rows, exec));
    sb.append("# plan: ").append(String.join(", ", plans)).append(System.lineSeparator());
    sb.append("# fixed share (wall - executor) / wall: ").append(String.join(", ", shares))
        .append(System.lineSeparator()).append(System.lineSeparator());
    return sb.toString();
  }

  private static void measureShape(
      SparkSession spark, ExecutorTime executor, KernelBatches batches, Runnable drain,
      String caseName, String query, Runnable body, Args args, long warmup, long min,
      List<Harness.Case> wall, List<Harness.Case> exec, List<String> plans, List<String> shares,
      PrintStream log, Surface.Entry entry, List<String> violations) {
    boolean varka = plansVarka(spark, query);
    if (args.expectFused && entry.expectFused() && !varka) {
      violations.add("expected fused, planned without a Varka node: " + query);
    }
    log.println("Running: " + query + (varka ? "  [Varka]" : "  [plain]"));
    long kernelBefore = batches.kernel();
    long fallbackBefore = batches.fallback();
    Harness.Samples s = Harness.measure(body, System::nanoTime, executor::millis, drain,
        args.iters, warmup, min);
    long kernelBatches = batches.kernel() - kernelBefore;
    long fallbackBatches = batches.fallback() - fallbackBefore;
    if (args.expectFused && entry.expectFused() && varka && kernelBatches == 0) {
      violations.add("planned a Varka node but the kernel served no batch ("
          + fallbackBatches + " fell back): " + query);
    }
    Harness.Stats w = Harness.stats(s.wallMs());
    Harness.Stats x = Harness.stats(s.executorMs());
    wall.add(new Harness.Case(caseName, w));
    exec.add(new Harness.Case(caseName, x));
    String shape = caseName;
    plans.add(shape + (varka
        ? String.format(Locale.ROOT, " Varka (kernel %d batches, fallback %d)", kernelBatches,
            fallbackBatches)
        : " plain"));
    double share = 100.0 * (w.bestMs() - x.bestMs()) / w.bestMs();
    shares.add(String.format(Locale.ROOT, "%s %.1f%%", shape, share));
    if (varka && !Double.isNaN(args.maxFixedShare) && share > args.maxFixedShare) {
      violations.add(String.format(Locale.ROOT,
          "fixed share %.1f%% over %.1f%% on a Varka row: %s", share, args.maxFixedShare,
          query));
    }
  }
}
