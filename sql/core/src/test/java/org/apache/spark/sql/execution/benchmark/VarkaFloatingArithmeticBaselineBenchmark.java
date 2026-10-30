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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.VarkaBenchmark;
import org.apache.spark.sql.internal.SQLConf;

/**
 * The baseline for floating arithmetic before Varka fuses it (VARKA-342): Spark's row path over
 * an Arrow-cached table of {@code DOUBLE} and interval columns, one section per form and null
 * density, so that the row reports what it changes against a committed file.
 * {@link VarkaFloatingBaselineBenchmark} already times the chain {@code (x + y) * z - x}; this
 * class times the forms it does not.
 *
 * <p>The forms are a column division {@code x / y} under LEGACY and under ANSI, the two modes
 * whose zero divisor Spark disposes of differently, a division by a literal {@code x / 3.0D},
 * {@code try_divide(x, y)}, {@code sqrt(abs(x))}, {@code -x}, {@code signum(x)},
 * {@code degrees(x)}, and the interval forms {@code ym * x} and {@code dt / x}, each with no nulls
 * and with one row in ten null. The forms other than the division have one answer in every mode,
 * and run in the session's default one.
 *
 * <p>Each section has two arms, named so the labels survive the row landing:
 * {@code spark row path (varka off)}, a session without the Varka rule, and {@code varka on}, a
 * session with it. Today both are the row path, so they should tie.
 *
 * <p>A baseline is only one while nothing fuses: before any section is timed, the run checks that
 * the Varka session's plan for each holds no Varka node, and fails when a row has started to fuse
 * one, unless {@link VarkaSqlBenchmarkBase#FUSE_CHECK_PROPERTY} lifts the check for the run that
 * measures that row.
 *
 * <p>To run this benchmark, on a GitHub runner (the results come back as an artifact):
 * <pre>
 *   gh workflow run benchmark.yml -f class=VarkaFloatingArithmeticBaselineBenchmark -f jdk=25 \
 *     -f expected-cpu="AMD EPYC 7763"
 *   (and again with -f extra-java-options=-XX:MaxVectorSize=16 for the 128-bit companion)
 * </pre>
 */
public class VarkaFloatingArithmeticBaselineBenchmark extends VarkaSqlBenchmarkBase {

  static final int NUM_ROWS = 2_000_000;

  /** The table's view name. */
  static final String VIEW = "varka_floating_arithmetic";

  /**
   * A section: its title, its query, and the SQL settings it runs under on both sessions, empty
   * for a form whose answer does not depend on the mode.
   */
  record Section(String title, String query, Map<String, String> conf) {}

  /**
   * The table: {@code x} and {@code y} as doubles, {@code ym} a year-month and {@code dt} a
   * day-time interval, with no null, and {@code xn} to {@code dtn} the same values with one row
   * in ten null, at a different row in each column.
   *
   * <p>The doubles are spread over a few thousand either side of zero and are never zero: each is
   * a multiple of a seventh or an eleventh offset by a quarter, so no division raises under ANSI,
   * and every interval result stays in its type. {@code ym} is up to a hundred years of months,
   * which times at most about 7300 stays far inside an int; {@code dt} is up to a day in seconds,
   * whose microseconds over the smallest {@code |x|}, a 28th, stay far inside a long.
   */
  static String tableSql(int rows) {
    return """
        select x, y, ym, dt,
               case when id %% 10 = 1 then null else x end as xn,
               case when id %% 10 = 4 then null else y end as yn,
               case when id %% 10 = 7 then null else ym end as ymn,
               case when id %% 10 = 8 then null else dt end as dtn
        from (select id,
                     (id %% 100003) / 7.0d - 7000.25d as x,
                     ((id * 7) %% 100019) / 11.0d - 4500.25d as y,
                     interval '1' month * cast((id * 13) %% 1200 as int) as ym,
                     interval '1' second * ((id * 17) %% 86400) as dt
              from range(0, %d))""".formatted(rows);
  }

  /** Every section, in the order they run: each null density, then each form. */
  static List<Section> sections() {
    List<Section> out = new ArrayList<>();
    Map<String, String> legacy = Map.of(SQLConf.ANSI_ENABLED().key(), "false");
    Map<String, String> ansi = Map.of(SQLConf.ANSI_ENABLED().key(), "true");
    for (String nulls : new String[] {"", "n"}) {
      String x = "x" + nulls;
      String y = "y" + nulls;
      String ym = "ym" + nulls;
      String dt = "dt" + nulls;
      String density = ", " + (nulls.isEmpty() ? "no nulls" : "one row in ten null");
      String from = " AS a FROM " + VIEW;
      out.add(new Section("DOUBLE x / y, LEGACY" + density,
          "SELECT " + x + " / " + y + from, legacy));
      out.add(new Section("DOUBLE x / y, ANSI" + density,
          "SELECT " + x + " / " + y + from, ansi));
      out.add(new Section("DOUBLE x / 3.0D" + density,
          "SELECT " + x + " / 3.0D" + from, Map.of()));
      out.add(new Section("DOUBLE try_divide(x, y)" + density,
          "SELECT try_divide(" + x + ", " + y + ")" + from, Map.of()));
      out.add(new Section("DOUBLE sqrt(abs(x))" + density,
          "SELECT sqrt(abs(" + x + "))" + from, Map.of()));
      out.add(new Section("DOUBLE -x" + density, "SELECT -" + x + from, Map.of()));
      out.add(new Section("DOUBLE signum(x)" + density,
          "SELECT signum(" + x + ")" + from, Map.of()));
      out.add(new Section("DOUBLE degrees(x)" + density,
          "SELECT degrees(" + x + ")" + from, Map.of()));
      out.add(new Section("year-month interval ym * x" + density,
          "SELECT " + ym + " * " + x + from, Map.of()));
      out.add(new Section("day-time interval dt / x" + density,
          "SELECT " + dt + " / " + x + from, Map.of()));
    }
    return out;
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    stopLeftoverSessions();
    SparkSession baseline = VarkaArrowSessions.createSession(
        "varka-floating-arithmetic-baseline-off", false, false, "local[1]");
    clearSessions();
    SparkSession varka = VarkaArrowSessions.createSession(
        "varka-floating-arithmetic-baseline-on", true, false, "local[1]");
    clearSessions();
    try {
      for (SparkSession session : List.of(baseline, varka)) {
        session.sql(tableSql(NUM_ROWS)).createOrReplaceTempView(VIEW);
        VarkaArrowSessions.cache(session, VIEW);
      }
      for (Section s : sections()) {
        withConf(varka, s.conf(), () -> requireBaseline(varka, s.title(), s.query()));
      }
      for (Section s : sections()) {
        section(s.title(), () -> withConf(baseline, s.conf(), () -> withConf(varka, s.conf(),
            () -> {
              VarkaBenchmark benchmark = benchmark(
                  s.title() + " over " + NUM_ROWS + " Arrow-cached rows", NUM_ROWS)
                  .timed().build();
              benchmark.addCase("spark row path (varka off)", i -> noop(baseline.sql(s.query())));
              benchmark.addCase("varka on", i -> noop(varka.sql(s.query())));
              runCases(benchmark);
            })));
      }
    } finally {
      baseline.stop();
      varka.stop();
    }
  }
}
