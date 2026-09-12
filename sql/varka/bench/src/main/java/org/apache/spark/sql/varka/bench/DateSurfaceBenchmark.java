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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.apache.spark.storage.RDDInfo;
import org.apache.spark.storage.StorageLevel;
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
 *     varka-bench.jar --label spark-4.2.0 --rows 500000000 --out FILE [--partitions 1] [--iters 5]
 *     [--warmup-seconds 2] [--min-seconds 2] [--only REGEX] [--shard I/N]
 *     [--provenance key=value]... [--expect-fused] [--max-fixed-share PERCENT]
 *     [--allow-nonresident-cache] [--storage-level MEMORY_ONLY]
 * </pre>
 *
 * {@code --expect-fused} (the fork with Varka on) fails the run, after writing the file, when an
 * entry the surface marks as fused planned without a Varka node; {@code --max-fixed-share}
 * fails it when a Varka-planned row's fixed share, {@code (wall - executor) / wall}, is over the
 * given percent - the job-size rule, checked from the numbers the file carries.
 *
 * The row count and the partition count are the job-size rule of PLAN_MILESTONE_4.md 2.29:
 * enough rows in few enough tasks that the job's fixed cost is under 5% of every Varka row's
 * wall time, which the file makes checkable by printing executor time beside wall time. One
 * partition is the default because on {@code local[1]} every task costs about two
 * milliseconds of scheduling and commit round trip, which fifty tasks turn into a tenth of
 * a second - invisible behind stock Spark's seconds, a third of a Varka row's.
 *
 * <p><b>{@code --shard I/N}</b> runs entries {@code I}, {@code I+N}, {@code I+2N} ... of
 * {@link Surface#ENTRIES}, so N dispatches between them cover the surface exactly once. It
 * exists because that job-size rule and the six-hour limit of a GitHub Actions job pull in
 * opposite directions: the rule wants enough rows that fixed cost is under 5%, and at that row
 * count one dispatch of the whole surface across four distributions does not fit
 * (PLAN_TASK_62.md 11.11). Sharding divides the queries, not the table, so each shard still
 * builds the full cached table in each distribution - which is why the split is worth taking
 * only as far as the per-shard table build, and no further.
 *
 * <p>The index is taken over the entry list rather than matched against entry names, which
 * {@code --only} would do. Names would have to be written out by hand, a shard would silently
 * cover the wrong entries when the surface gains a line, and nothing would notice; an index
 * modulo the list length cannot drift from the list it is an index into. Every shard of one
 * run must therefore be built from the same commit, which {@code dev/varka_bench_merge.py}
 * checks rather than assumes.
 *
 * <p>Striding rather than slicing into contiguous blocks is deliberate: the surface is ordered
 * by expression family, so a contiguous block would give one shard every calendar extraction
 * and another every comparison, and the shards' run times would differ several-fold. A stride
 * mixes the families, so the shards finish together and the slowest one - which is what the
 * wall-clock actually costs - is close to the mean.
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
    long rows = 500_000_000L;
    int partitions = 1;
    Path out = null;
    int iters = 5;
    double warmupSeconds = 2.0;
    double minSeconds = 2.0;
    Pattern only = null;
    int shardIndex = 0;
    int shardCount = 1;
    boolean expectFused = false;
    boolean allowNonresidentCache = false;
    StorageLevel storageLevel = StorageLevel.MEMORY_ONLY();
    double maxFixedShare = Double.NaN;
    final Map<String, String> provenance = new LinkedHashMap<>();

    static Args parse(String[] argv, int entryCount) {
      Args a = new Args();
      for (int i = 0; i < argv.length; i++) {
        String k = argv[i];
        String v = i + 1 < argv.length ? argv[i + 1] : null;
        switch (k) {
          case "--label" -> a.label = need(k, v);
          case "--rows" -> a.rows = Long.parseLong(need(k, v));
          case "--partitions" -> a.partitions = Integer.parseInt(need(k, v));
          case "--out" -> a.out = Path.of(need(k, v));
          case "--iters" -> a.iters = Integer.parseInt(need(k, v));
          case "--warmup-seconds" -> a.warmupSeconds = Double.parseDouble(need(k, v));
          case "--min-seconds" -> a.minSeconds = Double.parseDouble(need(k, v));
          case "--only" -> a.only = Pattern.compile(need(k, v));
          case "--shard" -> {
            String spec = need(k, v);
            int slash = spec.indexOf('/');
            if (slash <= 0) {
              throw new IllegalArgumentException("--shard wants I/N, got " + spec);
            }
            a.shardIndex = Integer.parseInt(spec.substring(0, slash));
            a.shardCount = Integer.parseInt(spec.substring(slash + 1));
          }
          case "--expect-fused" -> {
            a.expectFused = true;
            i--;
          }
          case "--storage-level" -> a.storageLevel = StorageLevel.fromString(need(k, v));
          case "--allow-nonresident-cache" -> {
            a.allowNonresidentCache = true;
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
      if (a.shardCount < 1 || a.shardIndex < 0 || a.shardIndex >= a.shardCount) {
        throw new IllegalArgumentException(
            "--shard wants 0 <= I < N with N >= 1, got " + a.shardIndex + "/" + a.shardCount);
      }
      if (a.shardCount > entryCount) {
        // Not an error worth failing a dispatch over, but a shard with no entries writes a
        // file with no rows, and a merge would then quietly be missing nothing at all.
        throw new IllegalArgumentException("--shard N is over the benchmark's " + entryCount
            + " entries, so some shard would be empty: " + a.shardCount);
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
    run(argv, Surface.ENTRIES, "VarkaDateSurface");
  }

  /**
   * The whole driver, over whatever list of entries it is given.
   *
   * <p>Parameterised rather than copied because {@link DateChainBenchmark} measures a
   * different question over the same machinery, and everything here that is easy to get
   * wrong - the residency guard, the fixed-share rule, the {@code EXPLAIN} check, the
   * provenance block, the shard arithmetic - should be got right once. The entry list and
   * the application name are the whole of the difference.
   */
  static void run(String[] argv, List<Surface.Entry> entries, String appName) throws IOException {
    Args args = Args.parse(argv, entries.size());
    double load = Provenance.loadAverage();
    SparkSession spark = SparkSession.builder().appName(appName).getOrCreate();
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
      buildTable(spark, args.rows, args.partitions, args.storageLevel);
      String cache = cacheState(spark, args.partitions);
      if (!cacheResident(spark, args.partitions) && !args.allowNonresidentCache) {
        throw new IllegalStateException("the cached table is not resident: " + cache
            + ". Every timing below it would be a recompute rate, not a kernel rate. Raise"
            + " --partitions so no single block has to fit, or lower --rows; raising"
            + " --driver-memory does not help, because the limit is per block.");
      }
      StringBuilder file = new StringBuilder();
      Map<String, String> prov =
          Provenance.collect(args.label, spark.version(), load, args.provenance);
      prov.put("rows", Long.toString(args.rows));
      prov.put("partitions", Integer.toString(args.partitions));
      // Always written, so a whole-surface file says "0/1" rather than being silent about it
      // and leaving a reader to wonder whether it is complete. The merge reads this.
      prov.put("cache", cache + ", " + args.storageLevel.description());
      prov.put("shard", args.shardIndex + "/" + args.shardCount);
      prov.put("surface entries", Integer.toString(entries.size()));
      prov.put("methodology", String.format(Locale.ROOT,
          "%d+ iterations over %.0fs windows after %.0fs warm-up; wall time by nanoTime, "
              + "executor time as the sum of TaskMetrics.executorRunTime over the iteration",
          args.iters, args.minSeconds, args.warmupSeconds));
      file.append(Provenance.format(prov)).append(System.lineSeparator());
      log.print(file);
      List<String> violations = new ArrayList<>();
      for (int idx = 0; idx < entries.size(); idx++) {
        Surface.Entry entry = entries.get(idx);
        if (idx % args.shardCount != args.shardIndex) {
          continue;
        }
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
   * The table: the generator {@code VarkaThroughputBenchmark} uses, in the given number of
   * partitions (one by default; see the class comment on what a task costs).
   */
  static void buildTable(SparkSession spark, long rows, int partitions, StorageLevel level) {
    spark.sql(String.format(Locale.ROOT,
        "SELECT CASE WHEN id %% 31 = 0 THEN NULL"
            + " ELSE date_add(DATE'2020-01-01', CAST(id %% 1460 AS INT)) END AS d,"
            + " date_add(DATE'2021-01-01', CAST(id %% 1500 AS INT)) AS d2,"
            + " CAST(id %% 3650 AS INT) AS i,"
            // Task 67: one year-month interval column per unit, over counts inside the
            // emitter's MONTH_ARITH range so every row fuses and the rows measure the kernel
            // rather than the guard's decline path.
            + " CAST(CAST(id %% 240 AS INT) AS INTERVAL MONTH) AS ymm,"
            + " CAST(CAST(id %% 20 AS INT) AS INTERVAL YEAR) AS ymy,"
            + " make_ym_interval(CAST(id %% 20 AS INT), CAST(id %% 12 AS INT)) AS ym"
            + " FROM range(0, %d, 1, %d)", rows, partitions))
        .createOrReplaceTempView("varka_dates");
    // MEMORY_ONLY, not Spark's MEMORY_AND_DISK default, and the difference is not academic:
    // the default is right for a workload, which should finish rather than fail, and exactly
    // wrong for a benchmark, which should fail rather than measure something else. Three runs
    // in September 2026 logged "Persisting block rdd_4_0 to disk instead" and then timed the
    // runner's SSD - 72.6 M rows/s against 854 M/s for the same entry at half the rows - and
    // reported success, because a disk-backed cache is still a cache as far as everything
    // downstream is concerned. Removing the disk path makes "cached" mean one thing, so the
    // partition count alone is a complete residency statement.
    spark.catalog().cacheTable("varka_dates", level);
    spark.sql("SELECT count(*) FROM varka_dates").collect();
  }

  /**
   * What the cache actually holds, as a one-line summary, or {@code null} when the table is
   * entirely resident in memory.
   *
   * <p><b>Why this exists.</b> {@code --max-fixed-share} fails a job too *small* to amortise
   * its driver overhead. Nothing failed a job so *large* that the cached table did not fit,
   * and that omission is worse than it sounds, because such a run does not merely go
   * unnoticed - it *passes* the fixed-share rule, and passes it easily. Every iteration
   * recomputes the table, executor time balloons, and the constant driver cost becomes a
   * negligible fraction of it. Three runs on GitHub runners in September 2026 reported
   * success this way while timing the runner's SSD - Spark's default {@code MEMORY_AND_DISK}
   * had persisted the block to disk, so the read still came from a "cache" - and at 2e8 rows
   * {@code date_add(d, 3)} read
   * 72.6 M rows/s against 854 M/s at 1e8, with a *better* fixed share (1.9% against 15.4%),
   * and the only symptom was 389 {@code MemoryStore} warnings buried in the log.
   *
   * <p>So the two rules bracket the row count from opposite sides, and a file can only be
   * written when it is between them. This one is checked once, immediately after the table
   * is materialised, so a misconfigured run costs a minute rather than three hours.
   *
   * <p>The failure is per *block*, not per byte: with {@code --partitions 1} the whole table
   * is one block and Spark will not cache a single block larger than the unrolling memory it
   * has, however large the heap. Raising {@code --driver-memory} therefore does not help -
   * measured, 6g, 8g and 11g all failed identically - and more partitions is the fix.
   *
   * <p>This check is kept even though {@link #buildTable} now asks for {@code MEMORY_ONLY},
   * which removes the disk path that caused the original silence. The two are complementary:
   * the storage level rules out one substitution, and this rules out the rest - a partially
   * cached table, or one evicted later under execution pressure.
   */
  static String cacheState(SparkSession spark, int partitions) {
    int cached = 0;
    long mem = 0;
    long disk = 0;
    for (RDDInfo info : spark.sparkContext().getRDDStorageInfo()) {
      cached += info.numCachedPartitions();
      mem += info.memSize();
      disk += info.diskSize();
    }
    return String.format(Locale.ROOT,
        "%d of %d partitions cached, %.1f GiB in memory, %.1f GiB on disk", cached, partitions,
        mem / (double) (1L << 30), disk / (double) (1L << 30));
  }

  /** True when every partition is in memory and none spilled. */
  static boolean cacheResident(SparkSession spark, int partitions) {
    int cached = 0;
    long disk = 0;
    for (RDDInfo info : spark.sparkContext().getRDDStorageInfo()) {
      cached += info.numCachedPartitions();
      disk += info.diskSize();
    }
    return cached >= partitions && disk == 0;
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

  /** What the physical plan says about a shape: see {@link #classifyPlan}. */
  enum Fusion { FUSED, PARTIAL, PLAIN }

  /** The plan's verdict for {@code query}, read through EXPLAIN. */
  static Fusion plansVarka(SparkSession spark, String query) {
    List<Row> rows = spark.sql("EXPLAIN " + query).collectAsList();
    return rows.isEmpty() ? Fusion.PLAIN : classifyPlan(rows.get(0).getString(0));
  }

  private static final Pattern RESIDUAL_ABOVE = Pattern.compile(
      "^[\\s+:|-]*(?:\\*\\(\\d+\\) )?(?:Filter |Project \\[[^\\]]+\\])");

  /**
   * {@code PLAIN} when no Varka node planned; {@code PARTIAL} when one did but a row-engine
   * {@code Filter} or a non-empty {@code Project} sits above it, which is what the laptop's
   * first full run measured for {@code year(d) = 2021} (the predicate declined at its literal
   * and Janino's {@code Filter} ran over every row) and for {@code SELECT d ... WHERE d < d2}
   * (a column-narrowing projection the rule does not take, so the filter served rows to a
   * Janino {@code Project}); {@code FUSED} otherwise. An empty {@code Project} above a counted
   * filter is the aggregate's and is not residual. The plan text alone said "Varka" in all
   * three cases, which is why a boolean was not enough.
   */
  static Fusion classifyPlan(String explain) {
    String[] lines = explain.split("\\n");
    int varkaAt = -1;
    for (int i = 0; i < lines.length; i++) {
      if (lines[i].contains("Varka")) {
        varkaAt = i;
        break;
      }
    }
    if (varkaAt < 0) {
      return Fusion.PLAIN;
    }
    for (int i = 0; i < varkaAt; i++) {
      if (RESIDUAL_ABOVE.matcher(lines[i]).find()) {
        return Fusion.PARTIAL;
      }
    }
    return Fusion.FUSED;
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
    // Every iteration plans its query afresh. A Dataset reused across iterations keeps its
    // RDD lineage, and the counted filter's final aggregate sits behind a shuffle, so Spark
    // reuses the map stage's output on the second run and only the one-partition result stage
    // executes: 4 ms and no executor time for 200M rows, which the first laptop run measured
    // before this comment existed. A fresh plan is a fresh lineage and nothing is reused.
    if (entry.projection() != null) {
      String q = projectionQuery(entry);
      Runnable body = () -> spark.sql(q).write().format("noop").mode("overwrite").save();
      measureShape(spark, executor, batches, drain, "projection, columnar consumer", q, body,
          args, warmup, min, wall, exec, plans, shares, log, entry, violations);
    }
    Long selected = null;
    if (entry.filter() != null) {
      selected = spark.sql(filterQuery(entry)).collectAsList().get(0).getLong(0);
      String qc = filterColumnarQuery(entry);
      Runnable bodyc = () -> spark.sql(qc).write().format("noop").mode("overwrite").save();
      measureShape(spark, executor, batches, drain, "filter, columnar consumer", qc, bodyc,
          args, warmup, min, wall, exec, plans, shares, log, entry, violations);
      String q = filterQuery(entry);
      Runnable body = () -> spark.sql(q).collect();
      measureShape(spark, executor, batches, drain, "filter, counted", q, body, args,
          warmup, min, wall, exec, plans, shares, log, entry, violations);
    }
    String name = entry.label() + " over " + args.rows + " rows";
    StringBuilder sb = new StringBuilder();
    sb.append(Harness.table(name, args.rows, wall)).append(System.lineSeparator());
    sb.append(Harness.table(name + ", executor time", args.rows, exec));
    sb.append("# plan: ").append(String.join(", ", plans)).append(System.lineSeparator());
    if (selected != null) {
      sb.append(String.format(Locale.ROOT, "# selectivity: %d of %d rows, %.1f%%%n", selected,
          args.rows, 100.0 * selected / args.rows));
    }
    sb.append("# fixed share (wall - executor) / wall: ").append(String.join(", ", shares))
        .append(System.lineSeparator()).append(System.lineSeparator());
    return sb.toString();
  }

  private static void measureShape(
      SparkSession spark, ExecutorTime executor, KernelBatches batches, Runnable drain,
      String caseName, String query, Runnable body, Args args, long warmup, long min,
      List<Harness.Case> wall, List<Harness.Case> exec, List<String> plans, List<String> shares,
      PrintStream log, Surface.Entry entry, List<String> violations) {
    Fusion fusion = plansVarka(spark, query);
    boolean varka = fusion != Fusion.PLAIN;
    if (args.expectFused && entry.expectFused() && fusion == Fusion.PLAIN) {
      violations.add("expected fused, planned without a Varka node: " + query);
    }
    if (args.expectFused && entry.expectFused() && fusion == Fusion.PARTIAL) {
      violations.add("expected fused, but a row-engine Filter or Project sits above the Varka "
          + "node: " + query);
    }
    log.println("Running: " + query + "  [" + fusion.name().toLowerCase(Locale.ROOT) + "]");
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
    if (x.bestMs() <= 0.0) {
      // No task ran in some iteration: a reused stage, a folded query, or a listener that did
      // not see the tasks. Whatever it is, the row would be a number about nothing.
      violations.add("an iteration ran no executor work (a reused shuffle stage, or a query "
          + "the optimizer answered without a scan): " + query);
    }
    wall.add(new Harness.Case(caseName, w));
    exec.add(new Harness.Case(caseName, x));
    String shape = caseName;
    plans.add(shape + (varka
        ? String.format(Locale.ROOT, " Varka%s (kernel %d batches, fallback %d)",
            fusion == Fusion.PARTIAL ? ", residual Filter/Project above" : "",
            kernelBatches, fallbackBatches)
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
