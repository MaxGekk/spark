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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import scala.Option;

import org.apache.spark.internal.config.UI;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.VarkaBenchmark;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.VarkaColumnarToRowExec;
import org.apache.spark.sql.execution.VarkaFilterExecBase;
import org.apache.spark.sql.execution.VarkaProjectExec;
import org.apache.spark.sql.execution.columnar.InMemoryRelation$;
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec;
import org.apache.spark.sql.internal.SQLConf;

/**
 * The schema-width cliff at the cache ({@code VARKA-185.md}): a query that reads one column of a
 * cached table, over tables of 100 and 101 fields. {@code InMemoryTableScanExec} produces columnar
 * batches only while the whole cached schema has at most {@code spark.sql.codegen.maxFields} (100)
 * fields, so the two tables, one column apart, are read by two different paths.
 *
 * <p>Two questions, in two phases, since the cache serializer is fixed for the process:
 *
 * <ul>
 *   <li>What the whole-schema count costs vanilla Spark, with its default cache serializer, over
 *       tables of int columns alone, the only kind that serializer produces batches for: a sum of
 *       one int column over 100 fields (columnar), over 101 (rows), and over 101 with {@code
 *       maxFields} raised past the width (columnar again), which is what counting the scan's own
 *       output instead of the whole schema would give.</li>
 *   <li>Whether Varka costs the same at both widths, over the Arrow cache, where with Varka on the
 *       scan counts the limit over the columns it reads and so reads the 101-field table as
 *       batches too: {@code date_add} of the one date column, on vanilla Spark and on Varka.</li>
 * </ul>
 *
 * <p>Each table writes, after it, how every arm's plan read the cache. The dry run, {@code
 * -Dvarka.bench.dryRun=true}, caches the tables and writes those lines; it times nothing.
 *
 * <p>To run this benchmark:
 * <pre>
 *   dev/varka_bench_regen.sh sql VarkaSchemaWidthBenchmark
 * </pre>
 */
public class VarkaSchemaWidthBenchmark extends VarkaSqlBenchmarkBase {

  private static final int NUM_ROWS = 1_000_000;

  /** An arm of the first phase: its case name, its table and the settings its query runs under. */
  private record SumArm(String label, String table, Map<String, String> settings) {
    String query() {
      return "SELECT sum(i1) FROM " + table;
    }
  }

  /** An arm of the second phase: its case name, whether Varka is on, and its table. */
  private record DateArm(String label, boolean varka, String table) {
    String query() {
      return "SELECT date_add(d, 1) AS e FROM " + table;
    }
  }

  /**
   * Caches {@code wide_<n>} with {@code n} fields, one null in 31 of each: {@code n} int columns,
   * or with {@code date} a date {@code d} and {@code n - 1} int columns. Spark's default cache
   * serializer produces batches only for primitive numeric types, so its phase caches ints alone.
   */
  private static void cacheWide(SparkSession session, int n, boolean date) {
    String name = "wide_" + n;
    int first = date ? 1 : 0;
    var columns = new ArrayList<String>(n);
    if (date) {
      columns.add(
          "if(id % 31 = 0, null, date_add(date'2000-01-01', cast(id % 20000 as int))) AS d");
    }
    for (int k = first; k < n; k++) {
      columns.add("cast(if(id % 31 = " + (k + 1) + " % 31, null, id + " + k + ") as int) AS i"
          + (k + 1 - first));
    }
    session.range(0, NUM_ROWS).selectExpr(columns.toArray(String[]::new))
        .createOrReplaceTempView(name);
    session.catalog().cacheTable(name);
    session.sql("select count(*) from " + name).collect();
  }

  /** How {@code query}'s plan reads the cache, for the note after each table. */
  private static String reads(SparkSession session, String query) {
    SparkPlan plan = session.sql(query).queryExecution().executedPlan();
    Option<SparkPlan> scan = plan.find(p -> p instanceof InMemoryTableScanExec);
    boolean varka = plan.find(p -> p instanceof VarkaProjectExec
        || p instanceof VarkaColumnarToRowExec || p instanceof VarkaFilterExecBase).isDefined();
    String columnar = scan.isDefined() && scan.get().supportsColumnar() ? "columnar" : "rows";
    return (varka ? "Varka" : "Spark") + ", cache scan " + columnar;
  }

  private static SparkSession session(String name, boolean varka, boolean arrow) {
    if (arrow) {
      return VarkaArrowSessions.createSession(name, varka, false, "local[1]");
    }
    return SparkSession.builder().master("local[1]").appName(name)
        .config(UI.UI_ENABLED().key(), false)
        .config(SQLConf.SHUFFLE_PARTITIONS().key(), 1L)
        .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false")
        .getOrCreate();
  }

  /** The process-wide cache serializer forgotten, so the next session's setting is read. */
  private static void clearSerializer() {
    // `clearSerializer` is private[sql], which scalac gives no static forwarder.
    InMemoryRelation$.MODULE$.clearSerializer();
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    stopLeftoverSessions();
    defaultSerializer();
    arrowCache();
  }

  /** Phase 1: vanilla Spark and its default cache serializer. */
  private void defaultSerializer() {
    clearSerializer();
    SparkSession vanilla = session("VarkaSchemaWidth-default", false, false);
    clearSessions();
    try {
      cacheWide(vanilla, 100, false);
      cacheWide(vanilla, 101, false);
      section("vanilla Spark, default cache serializer: sum of one column", () -> {
        VarkaBenchmark benchmark = benchmark(
            "sum(i1) over " + NUM_ROWS + " cached rows", NUM_ROWS).timed().build();
        String maxFields = SQLConf.WHOLESTAGE_MAX_NUM_FIELDS().key();
        List<SumArm> arms = List.of(
            new SumArm("100 fields", "wide_100", Map.of()),
            new SumArm("101 fields", "wide_101", Map.of()),
            new SumArm("101 fields, maxFields=1000", "wide_101", Map.of(maxFields, "1000")));
        for (SumArm arm : arms) {
          benchmark.addCase(arm.label(),
              i -> withConf(vanilla, arm.settings(), () -> noop(vanilla.sql(arm.query()))));
        }
        runCases(benchmark);
        for (SumArm arm : arms) {
          withConf(vanilla, arm.settings(),
              () -> note(arm.label() + ": " + reads(vanilla, arm.query())));
        }
      });
    } finally {
      vanilla.stop();
      clearSessions();
      clearSerializer();
    }
  }

  /**
   * Phase 2: the Arrow cache, vanilla Spark against Varka, in one session whose Varka switch is
   * set per arm, so the two tables are cached once. Each timed iteration sets the switch before
   * its query and leaves it for the next arm to set, rather than scoping it with {@link
   * #withConf}, which would add a restore to every timed iteration.
   */
  private void arrowCache() {
    SparkSession arrow = session("VarkaSchemaWidth-arrow", true, true);
    clearSessions();
    try {
      cacheWide(arrow, 100, true);
      cacheWide(arrow, 101, true);
      section("Arrow cache: date_add of one date column", () -> {
        VarkaBenchmark benchmark = benchmark(
            "date_add(d, 1) over " + NUM_ROWS + " cached rows", NUM_ROWS).timed().build();
        List<DateArm> arms = List.of(
            new DateArm("vanilla Spark, 100 fields", false, "wide_100"),
            new DateArm("vanilla Spark, 101 fields", false, "wide_101"),
            new DateArm("Varka, 100 fields", true, "wide_100"),
            new DateArm("Varka, 101 fields", true, "wide_101"));
        for (DateArm arm : arms) {
          benchmark.addCase(arm.label(), i -> {
            varkaOn(arrow, arm.varka());
            noop(arrow.sql(arm.query()));
          });
        }
        runCases(benchmark);
        for (DateArm arm : arms) {
          varkaOn(arrow, arm.varka());
          note(arm.label() + ": " + reads(arrow, arm.query()));
        }
      });
    } finally {
      arrow.stop();
      clearSerializer();
    }
  }

  private static void varkaOn(SparkSession session, boolean enabled) {
    session.conf().set(SQLConf.VARKA_ENABLED().key(), Boolean.toString(enabled));
  }
}
