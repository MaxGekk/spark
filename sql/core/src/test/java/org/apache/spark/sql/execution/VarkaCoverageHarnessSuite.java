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

package org.apache.spark.sql.execution;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import scala.Tuple2;
import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaMatrixRuns;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSuite;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * Every row of the published coverage table held to Spark's answer with no session (VARKA-509,
 * {@code m8/VARKA-509.md}): the answer form of {@code VarkaCoverageDifferentialSuite}, on its
 * three answer fixtures, through {@link VarkaExpressionHarness}. A projection row runs through
 * the projection evaluator, a predicate row through the filter evaluator, as the exec nodes run
 * them; each row is one test, so the option matrix's record of what a configuration moves keeps
 * a row's granularity.
 *
 * <p>The kernel must answer some batch of a row on some fixture: a row whose kernel declined
 * everything would otherwise pass on the row path's answers alone.
 */
@VarkaSuite
public final class VarkaCoverageHarnessSuite {

  /** One row of {@code sql/varka/coverage.json}. */
  record Row(String sql, String executable, String family, String form) {}

  private static final List<Row> ROWS = readRows();

  /** The table's rows, which {@link VarkaHarnessParitySuite} plans in a session too. */
  static List<Row> rows() {
    return ROWS;
  }

  private static final Map<String, VarkaHarnessPlans.Fixture> FIXTURES = readFixtures();

  private static List<Row> readRows() {
    String home = System.getProperty("spark.test.home", System.getenv("SPARK_HOME"));
    if (home == null) {
      throw new IllegalStateException("spark.test.home or SPARK_HOME is not set");
    }
    try {
      JsonNode doc = new ObjectMapper().readTree(
          Path.of(home, "sql", "varka", "coverage.json").toFile());
      var rows = new ArrayList<Row>();
      for (JsonNode e : doc.get("expressions")) {
        rows.add(new Row(e.get("sql").asText(), e.get("executable").asText(),
            e.get("family").asText(), e.get("form").asText()));
      }
      return rows;
    } catch (IOException e) {
      throw new IllegalStateException("cannot read sql/varka/coverage.json", e);
    }
  }

  private static Map<String, VarkaHarnessPlans.Fixture> readFixtures() {
    var out = new LinkedHashMap<String, VarkaHarnessPlans.Fixture>();
    for (Tuple2<String, String> f :
        CollectionConverters.asJava(VarkaCoverageDifferentialSuite$.MODULE$.fixtures())) {
      out.put(f._1(), VarkaHarnessPlans.fixture(f._2()));
    }
    return out;
  }

  @BeforeAll
  static void matrixOptions() {
    VarkaColumnarToRowExec$.MODULE$.setEmitOptionsForTesting(VarkaMatrixRuns.BASE);
  }

  @AfterAll
  static void defaultOptions() {
    VarkaColumnarToRowExec$.MODULE$.setEmitOptionsForTesting(VarkaEmitOptions.DEFAULTS);
  }

  /**
   * The conf the harness's batches and Spark's answers read: the thread's, with the matrix's
   * rows per Arrow batch laid over it where the configuration sets one. The sessions of the
   * end-to-end suites take it from {@code VarkaSharedSessions}; here the harness's own batching
   * stands in for the cache's, so without it {@code arrowBatchSize=1}, {@code 3} and {@code 17}
   * would run each fixture as one batch, as the defaults do.
   */
  static SQLConf conf(boolean ansi) {
    SQLConf conf = SQLConf.get().clone();
    VarkaMatrixRuns.ARROW_BATCH_SIZE_ROWS.ifPresent(rows -> conf.setConfString(
        SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH().key(), Integer.toString(rows)));
    conf.setConfString(SQLConf.ANSI_ENABLED().key(), Boolean.toString(ansi));
    return conf;
  }

  /** {@code body} with {@code conf} as the thread's conf, which the analyzer reads ANSI from. */
  static <T> T withConf(SQLConf conf, java.util.function.Supplier<T> body) {
    return SQLConf.withExistingConf(conf, body::get);
  }

  /** {@code row} on every answer fixture: what the checks found, and whether a kernel answered. */
  private static VarkaExpressionHarness.Outcome check(Row row) {
    SQLConf conf = conf(SQLConf.get().ansiEnabled());
    return withConf(conf, () -> checkUnder(row, conf));
  }

  private static VarkaExpressionHarness.Outcome checkUnder(Row row, SQLConf conf) {
    var all = new VarkaExpressionHarness.Outcome();
    for (Map.Entry<String, VarkaHarnessPlans.Fixture> f : FIXTURES.entrySet()) {
      String context = f.getKey() + ", " + row.family() + ": " + row.sql();
      VarkaHarnessPlans.Fixture fixture = f.getValue();
      VarkaExpressionHarness.Outcome one = row.form().equals("predicate")
          ? VarkaExpressionHarness.filter(context,
              VarkaHarnessPlans.condition(row.executable(), fixture.columns()), fixture, conf)
          : VarkaExpressionHarness.projection(context,
              VarkaHarnessPlans.projection(row.executable(), fixture.columns()), fixture, conf);
      all.findings.addAll(one.findings);
      all.answered += one.answered;
      all.declined += one.declined;
    }
    return all;
  }

  @Test
  @DisplayName("the coverage table has rows to run")
  void tableHasRows() {
    assertTrue(ROWS.size() >= 57, "only " + ROWS.size() + " rows read from coverage.json");
  }

  @TestFactory
  Stream<DynamicTest> everyRow() {
    return ROWS.stream().map(row -> DynamicTest.dynamicTest(
        row.family() + ": " + row.sql() + " matches Spark's answer below the session", () -> {
          VarkaExpressionHarness.Outcome outcome = check(row);
          assertTrue(outcome.ok(), String.join("\n", outcome.findings));
          assertTrue(outcome.answered > 0, "the kernel answered no batch of " + row.sql()
              + " on any fixture (" + outcome.declined + " declined)");
        }));
  }

  @Test
  @DisplayName("a kernel planted to answer an overflowing row fails the harness")
  void plantedKernelFails() {
    // The fault the end-to-end suite's ANSI form plants (VARKA-317): with checkIntOverflow off,
    // a checked int add emits as LEGACY and wraps, so the kernel answers Int.MaxValue + 1 where
    // Spark raises ARITHMETIC_OVERFLOW. Checked first with the add checked, which must pass.
    SQLConf conf = conf(true);
    withConf(conf, () -> {
      plantUnder(conf);
      return null;
    });
  }

  private static void plantUnder(SQLConf conf) {
    VarkaHarnessPlans.Fixture fixture = VarkaHarnessPlans.fixture(
        "SELECT rid, i FROM VALUES (0, 2147483647), (1, 1) AS v(rid, i)");
    var list = VarkaHarnessPlans.projection("i + 1", fixture.columns());
    // Both halves set the check themselves, so the test means the same under the matrix's
    // checkIntOverflow=false configuration as at the defaults.
    try {
      VarkaColumnarToRowExec$.MODULE$.setEmitOptionsForTesting(
          VarkaMatrixRuns.BASE.withCheckIntOverflow(true));
      VarkaExpressionHarness.Outcome checked =
          VarkaExpressionHarness.projection("checked", list, fixture, conf);
      assertTrue(checked.ok(), String.join("\n", checked.findings));
      VarkaColumnarToRowExec$.MODULE$.setEmitOptionsForTesting(
          VarkaMatrixRuns.BASE.withCheckIntOverflow(false));
      VarkaExpressionHarness.Outcome planted =
          VarkaExpressionHarness.projection("planted", list, fixture, conf);
      assertTrue(planted.answered > 0, "the planted kernel answered no batch");
      assertTrue(planted.findings.stream().anyMatch(f -> f.contains("Spark raises on")
          && f.contains("ARITHMETIC_OVERFLOW")), "the harness missed the planted answer: "
          + planted.findings);
    } finally {
      VarkaColumnarToRowExec$.MODULE$.setEmitOptionsForTesting(VarkaMatrixRuns.BASE);
    }
  }
}
