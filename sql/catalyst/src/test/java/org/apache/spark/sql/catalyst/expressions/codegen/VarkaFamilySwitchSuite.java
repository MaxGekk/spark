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
package org.apache.spark.sql.catalyst.expressions.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.SparkIllegalArgumentException;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.RuntimeReplaceable;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaErrors;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaFamily;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaMatrixRuns;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSqlResolve;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSuite;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.SQLConf$;

/**
 * The kill switch of each expression family (VARKA-318): with one family switched off, every
 * coverage-table row that holds a node of that family declines with the reason naming the
 * family's switch, and every other row compiles exactly as it does with every family on. A
 * row's families are read independently of the compiler's own record of them - from which
 * families of {@code VarkaNodeCompiler.families} claim its non-leaf nodes, plus the condition
 * family for a filter's conjunct - and the compiler's record ({@code CompiledVarkaProjection
 * .families}) is held to that reading too, since a kernel failure's warning names it.
 */
@VarkaSuite
public final class VarkaFamilySwitchSuite {

  /** One coverage row: its SQL, whether it is a filter's conjunct, and its resolved tree. */
  private record Row(String sql, boolean predicate, Expression expr) {}

  /**
   * What the compiler made of a row: present IR and families when it fused, else the reason.
   * Two outcomes are equal exactly when the row compiled to the same kernel or declined alike.
   */
  private record Outcome(
      Optional<String> kernel, List<VarkaFamily> families, Optional<String> reason) {}

  private static final VarkaEmitOptions OPTIONS = VarkaMatrixRuns.BASE;

  /**
   * The rows holding a node one family claims that another family's lowering builds directly,
   * without the chain, so the claimant's switch does not reach it (`VARKA-318.md` 3.2): each
   * with the family whose switch it escapes. In `d - ym` the calendar lowers the negated interval
   * operand through `VarkaIntervalCompiler`'s helpers. A new such row fails the test until it is
   * listed here, and a listed row that stops being one fails it too.
   */
  private static final Map<String, VarkaFamily> LOWERED_BY_CALLER =
      Map.of("d - ym", VarkaFamily.INTERVAL);

  // Read once for the class: JUnit builds an instance per test, and the rows' resolution through
  // the analyzer is most of the suite's time.
  private static Seq<Attribute> columns;
  private static List<Row> rows;

  @BeforeAll
  static void readCoverage() throws IOException {
    String home = System.getProperty("spark.test.home", System.getenv("SPARK_HOME"));
    assertTrue(home != null, "spark.test.home or SPARK_HOME is not set.");
    JsonNode json = new ObjectMapper().readTree(
        Files.readString(Path.of(home, "sql", "varka", "coverage.json")));
    var attrs = new ArrayList<Attribute>();
    for (var c : json.get("columns").properties()) {
      attrs.add(VarkaSqlResolve.column(
          c.getKey(), CatalystSqlParser$.MODULE$.parseDataType(c.getValue().asText())));
    }
    columns = CollectionConverters.asScala(List.<Attribute>copyOf(attrs)).toSeq();
    var read = new ArrayList<Row>();
    for (JsonNode row : json.get("expressions")) {
      String sql = row.get("executable").asText();
      if (sql.isEmpty()) {
        sql = row.get("sql").asText();
      }
      read.add(new Row(sql, "predicate".equals(row.get("form").asText()),
          VarkaSqlResolve.parse(sql, columns)));
    }
    rows = List.copyOf(read);
    assertTrue(rows.size() > 80, rows.size() + " coverage rows read");
  }

  private Outcome compile(Row row, Set<VarkaFamily> off) {
    if (row.predicate()) {
      var explained =
          VarkaExpressionCompiler.explainAndCompilePredicate(row.expr(), columns, OPTIONS, off);
      if (explained.compiled().isPresent()
          && explained.compiled().get().residualConjuncts().isEmpty()) {
        CompiledVarkaProjection fused = explained.compiled().get().fused();
        return new Outcome(Optional.of(fused.outputs().toString()), fused.families(),
            Optional.empty());
      }
      return new Outcome(Optional.empty(), List.of(), explained.specs().stream()
          .flatMap(s -> s.decline().stream()).map(VarkaDecline::reason).findFirst());
    }
    Seq<NamedExpression> entries = CollectionConverters.asScala(
        List.of(VarkaSqlResolve.named(row.expr(), "o"))).toSeq();
    Optional<CompiledVarkaProjection> fused =
        VarkaExpressionCompiler.compilePartial(entries, columns, OPTIONS, off)
            .filter(p -> p.specs().get(0) instanceof VarkaOutputSpec.FusedOutput)
            .map(PartialVarkaProjection::fused);
    if (fused.isPresent()) {
      return new Outcome(Optional.of(fused.get().outputs().toString()), fused.get().families(),
          Optional.empty());
    }
    return new Outcome(Optional.empty(), List.of(), Optional.ofNullable(
        VarkaExpressionCompiler.declines(entries, columns, OPTIONS, off).get(0))
        .map(VarkaDecline::reason));
  }

  /**
   * The families whose nodes {@code row} holds, read off the chain's claims rather than the
   * compiler's record: every non-leaf node of the bound tree (and of what a replaceable node
   * compiles as) that a switchable family claims, and the condition family for a conjunct.
   */
  private List<VarkaFamily> claimedFamilies(Row row) {
    Expression bound = VarkaSqlResolve.bind(row.expr(), columns);
    var found = EnumSet.noneOf(VarkaFamily.class);
    if (row.predicate()) {
      found.add(VarkaFamily.CONDITION);
    }
    var sink = new DeclineSink(columns, true);
    var stack = new ArrayList<Expression>(List.of(bound));
    while (!stack.isEmpty()) {
      Expression e = stack.remove(stack.size() - 1);
      if (e instanceof RuntimeReplaceable r) {
        stack.add(r.replacement());
      }
      stack.addAll(CollectionConverters.asJava(e.children()));
      if (e instanceof BoundReference || e instanceof Literal) {
        continue;
      }
      for (VarkaNodeCompiler.Family family : VarkaNodeCompiler.families()) {
        if (family.family() != null && family.claim().of(
            e, new VarkaSlotTable(), new VarkaSlotTable(), sink) != null) {
          found.add(family.family());
        }
      }
    }
    return List.copyOf(found);
  }

  @Test
  @DisplayName("a family switched off declines exactly the coverage rows that hold its nodes")
  void eachSwitchDeclinesItsFamily() {
    var declined = new EnumMap<VarkaFamily, Integer>(VarkaFamily.class);
    var failures = new ArrayList<String>();
    for (Row row : rows) {
      Outcome on = compile(row, Set.of());
      List<VarkaFamily> holds = claimedFamilies(row);
      if (on.kernel().isEmpty()) {
        failures.add("`" + row.sql() + "` does not fuse with every family on: " + on.reason());
        continue;
      }
      VarkaFamily escapes = LOWERED_BY_CALLER.get(row.sql());
      if (escapes != null) {
        if (!holds.contains(escapes) || on.families().contains(escapes)) {
          failures.add("`" + row.sql() + "` is listed as lowering " + escapes.label()
              + " in its caller, and no longer does");
        }
        holds = holds.stream().filter(f -> f != escapes).toList();
      }
      if (!on.families().equals(holds)) {
        failures.add("`" + row.sql() + "` records families " + on.families() + ", its nodes are "
            + holds);
      }
      for (VarkaFamily family : VarkaFamily.values()) {
        Outcome off = compile(row, Set.of(family));
        if (holds.contains(family)) {
          declined.merge(family, 1, Integer::sum);
          if (!off.reason().equals(Optional.of(family.offReason()))) {
            failures.add("`" + row.sql() + "` with " + family.label() + " off: " + off);
          }
        } else if (!off.equals(on)) {
          failures.add("`" + row.sql() + "` changed with " + family.label() + " off: " + off);
        }
      }
    }
    assertEquals(List.of(), failures);
    // Each switch is seen to work on something, or this test proves nothing about it.
    assertEquals(EnumSet.allOf(VarkaFamily.class), declined.keySet(), declined.toString());
  }

  @Test
  @DisplayName("every family's switch is a session setting of SQLConf, on by default")
  void switchesAreSqlConfs() {
    for (VarkaFamily family : VarkaFamily.values()) {
      String key = family.switchKey();
      assertTrue(SQLConf$.MODULE$.containsConfigKey(key), key + " is not declared in SQLConf");
      assertEquals("true", SQLConf.get().getConfString(key), key);
      assertTrue(family.offReason().contains(key + "=false"), family.offReason());
      assertTrue(family.retry().contains("set " + key + " to false and retry"), family.retry());
    }
  }

  @Test
  @DisplayName("a failure inside a family's lowering names the family's switch in the error")
  void failureNamesTheSwitch() {
    for (VarkaFamily family : VarkaFamily.values()) {
      Row row = rows.stream()
          .filter(r -> !LOWERED_BY_CALLER.containsKey(r.sql()))
          .filter(r -> claimedFamilies(r).contains(family)).findFirst().orElseThrow();
      VarkaNodeCompiler.setFailInFamilyForTesting(family);
      try {
        RuntimeException e = assertThrows(RuntimeException.class, () -> compile(row, Set.of()));
        // The site raised an argument check, and the planning error keeps its type.
        var error = assertInstanceOf(SparkIllegalArgumentException.class, e, row.sql());
        assertEquals(VarkaErrors.INTERNAL_ERROR, error.getCondition());
        String message = error.getMessage();
        assertTrue(message.contains(family.retry()), message);
        assertTrue(message.contains("\n  families: " + family.label() + " ("
            + family.switchKey() + ")"), message);
        assertTrue(message.contains("injected failure in Varka's " + family.label()), message);
      } finally {
        VarkaNodeCompiler.setFailInFamilyForTesting(null);
      }
    }
  }

  @Test
  @DisplayName("a switch keeps the columns its family claims readable by the other families")
  void readsAreNotSwitched() {
    // The time family claims every 64-bit column; with it off, a comparison of two bigint or two
    // TIME columns is still the condition family's alone and fuses.
    for (String sql : List.of("l > l2", "t < t2")) {
      Outcome off = compile(new Row(sql, true, VarkaSqlResolve.parse(sql, columns)),
          Set.of(VarkaFamily.TIME));
      assertTrue(off.kernel().isPresent(), sql + ": " + off);
      assertEquals(List.of(VarkaFamily.CONDITION), off.families(), sql);
    }
  }
}
