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
package org.apache.spark.sql.catalyst.expressions.codegen.varka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import org.apache.spark.SparkThrowable;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.GuardedRange;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.errors.QueryExecutionErrors;

/**
 * Varka's invariant sites raise Spark's {@code INTERNAL_ERROR} (VARKA-314): every site in Varka's
 * main code goes through {@link VarkaErrors}, a site of each family raises the condition with its
 * own text and the request to report it while keeping the Java type it threw before, and the
 * planning catch puts the context record around a site's error rather than passing it through
 * without one. The sql/core side - the query that fails on the driver, the fallback warning - is
 * {@code VarkaErrorContextSuite}'s.
 */
@VarkaSuite
public final class VarkaInternalErrorSuite {

  /**
   * The raw throws of a JDK invariant type left in Varka's main code on purpose, each by its
   * file and a fragment of its line: the two test-injection throws, which simulate a failure of
   * that type on purpose, and the reproduction file's parser, whose errors say that a file
   * handed to the replay tool is malformed - a reader's mistake, not a bug in Varka.
   */
  private static final List<String> LEFT_RAW = List.of(
      "VarkaExpressionCompiler.java: \"injected Varka compiler failure\"",
      "VarkaEvaluatorBase.java: \"injected Varka emission failure\"",
      "VarkaReproduction.java: \"expected '\" + key",
      "VarkaReproduction.java: \"not a Varka reproduction file",
      "VarkaReproduction.java: \"line \" + (i + 1)",
      "VarkaReproduction.java: \"unexpected '\"");

  private static final Pattern RAW = Pattern.compile(
      "throw new (?:java\\.lang\\.)?"
          + "(IllegalArgumentException|IllegalStateException|UnsupportedOperationException)"
          + "\\((.*)");

  /** The directories of 2's admission check in {@code VARKA-314.md}, from the checkout's root. */
  private static final List<String> ROOTS = List.of(
      "sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen",
      "sql/core/src/main/java/org/apache/spark/sql/execution",
      "sql/core/src/main/scala/org/apache/spark/sql/execution");

  @Test
  @DisplayName("no invariant site in Varka's main code throws a bare JDK exception")
  void everySiteConverted() throws IOException {
    String home = System.getProperty("spark.test.home", System.getenv("SPARK_HOME"));
    assertTrue(home != null, "spark.test.home or SPARK_HOME is not set.");
    var found = new ArrayList<String>();
    int files = 0;
    for (String root : ROOTS) {
      Path dir = Path.of(home, root);
      List<Path> sources;
      try (Stream<Path> walk = Files.walk(dir)) {
        sources = walk.filter(p -> isVarkaSource(dir, p)).sorted().toList();
      }
      for (Path source : sources) {
        files++;
        for (String line : Files.readAllLines(source, StandardCharsets.UTF_8)) {
          String code = line.strip();
          if (code.startsWith("*") || code.startsWith("//")) {
            continue;
          }
          var m = RAW.matcher(code);
          if (m.find()) {
            String site = source.getFileName() + ": " + m.group(m.groupCount());
            found.add(LEFT_RAW.stream().filter(site::contains).findFirst().orElse(site));
          }
        }
      }
    }
    // The walk saw Varka's code at all: a moved directory would otherwise pass on nothing.
    assertTrue(files > 90, "only " + files + " Varka source files under " + ROOTS);
    assertEquals(LEFT_RAW, found.stream().sorted(Comparator.comparingInt(LEFT_RAW::indexOf))
        .toList(),
        "a raw throw of a JDK invariant type: raise it through VarkaErrors, or, if it is not a "
            + "Varka invariant, list it in LEFT_RAW with the reason");
  }

  /**
   * Varka's own files under {@code root}: a file in it whose name holds {@code Varka}
   * ({@code CompiledVarkaProjection} and its siblings included), or any in its varka/.
   */
  private static boolean isVarkaSource(Path root, Path p) {
    String name = p.getFileName().toString();
    if (!name.endsWith(".java") && !name.endsWith(".scala")) {
      return false;
    }
    Path parent = p.getParent();
    return parent.equals(root) ? name.contains("Varka") : parent.equals(root.resolve("varka"));
  }

  /**
   * Checks that {@code site} raises {@code INTERNAL_ERROR} of {@code type} with {@code text} as
   * its own text and the request to report it, no record or cause, and a stack trace that starts
   * at the site, as a site deep in the compiler raises it.
   */
  private static <T extends RuntimeException> void raisesInternalError(
      Class<T> type, String text, Executable site) {
    T e = assertThrows(type, site);
    var spark = assertInstanceOf(SparkThrowable.class, e);
    assertEquals(VarkaErrors.INTERNAL_ERROR, spark.getCondition());
    assertEquals("XX000", spark.getSqlState());
    assertEquals(text + "\n" + VarkaErrors.REPORT, spark.getMessageParameters().get("message"));
    assertEquals(text, VarkaErrors.siteText(e));
    assertNull(e.getCause());
    // The trace starts at the site, as a throw there did: VarkaSizeAdmission keys its once-per-site
    // warning on the top frame.
    assertTrue(!e.getStackTrace()[0].getClassName().equals(VarkaErrors.class.getName()),
        e.getStackTrace()[0].toString());
  }

  @Test
  @DisplayName("a site of each family raises INTERNAL_ERROR with its own text, in its Java type")
  void eachFamily() {
    // The IR: a node's constructor rejects a tree the compiler should never build.
    raisesInternalError(IllegalArgumentException.class, "an empty range guards nothing: [5, 1]",
        () -> new GuardedRange(new ColumnRef(0, LaneType.INT), 5, 1));
    // The range analysis the compiler admits entries by.
    raisesInternalError(IllegalArgumentException.class, "empty range [5, 1]",
        () -> new VarkaValueRange.Bounded(5, 1));
    // The emitter, handed nothing to emit.
    raisesInternalError(IllegalArgumentException.class, "no output chains to emit",
        () -> VarkaLoopEmitter.emitTraced("VarkaInternalErrorSuite_none", List.of(), 1, 0,
            VarkaEmitOptions.DEFAULTS, null));
    // Building options: the record's constructor and the option table's lookup by name.
    raisesInternalError(IllegalArgumentException.class, "groupBudget must be positive: 0",
        () -> VarkaEmitOptions.DEFAULTS.toBuilder().groupBudget(0).build());
    raisesInternalError(IllegalArgumentException.class, "no emit option noSuchOption",
        () -> VarkaEmitOption.named("noSuchOption"));
    // A kernel called through the other lane width's entry point: Spark's carrier of
    // UnsupportedOperationException.
    VarkaFusedKernel kernel = new VarkaFusedKernel() { };
    raisesInternalError(UnsupportedOperationException.class,
        kernel.getClass().getName() + " is a 64-bit-lane kernel; call the eight-argument run",
        () -> kernel.run(new long[0], new long[0], new int[0], new long[0], new long[0],
            new int[0], 0));
    // A state invariant: a class loader used after the shape cache released it.
    var loader = new VarkaGeneratedClassLoader(getClass().getClassLoader());
    loader.release();
    raisesInternalError(IllegalStateException.class,
        "VarkaGeneratedClassLoader has already been released",
        () -> loader.defineGeneratedClass("VarkaInternalErrorSuite_released", new byte[0]));
  }

  @Test
  @DisplayName("the planning catch puts the record around a site's error, keeping its type, and "
      + "asks for the report once")
  void planningWrapsASite() {
    var site = assertThrows(IllegalArgumentException.class,
        () -> new VarkaValueRange.Bounded(5, 1));
    RuntimeException wrapped = VarkaErrors.planningFailure(site, () -> "projection [x#1]",
        VarkaEmitOptions.DEFAULTS);
    assertInstanceOf(IllegalArgumentException.class, wrapped);
    assertSame(site, wrapped.getCause());
    var spark = assertInstanceOf(SparkThrowable.class, wrapped);
    assertEquals(VarkaErrors.INTERNAL_ERROR, spark.getCondition());
    assertEquals("""
        Varka's compiler failed while planning.
        Varka context:
          kind: planning
          identity: projection [x#1]
          shape hash: none
          options: the defaults
          cause: org.apache.spark.SparkIllegalArgumentException: empty range [5, 1]
        """ + VarkaErrors.REPORT, spark.getMessageParameters().get("message"));
    assertEquals(1, wrapped.getMessage().split(Pattern.quote(VarkaErrors.REPORT), -1).length - 1,
        wrapped.getMessage());
    // A state site stays a state error.
    RuntimeException state = VarkaErrors.planningFailure(VarkaErrors.illegalState("x"),
        () -> "projection [x#1]", VarkaEmitOptions.DEFAULTS);
    assertInstanceOf(IllegalStateException.class, state);
    assertNull(VarkaErrors.siteText(state), "a wrapped error has its record");
  }

  @Test
  @DisplayName("the planning catch passes on a Varka error that has its record, a user's "
      + "condition and a designed decline")
  void planningPassesTheRest() {
    var recorded = VarkaErrors.illegalState("x", new VarkaErrorContext("kernel-failure", "k",
        "h", "", null, null));
    assertSame(recorded, VarkaErrors.planningFailure(recorded, () -> "p",
        VarkaEmitOptions.DEFAULTS));
    // A constant the compiler folded raised the user's own error, not a bug in Varka.
    var user = QueryExecutionErrors.divideByZeroError(null);
    assertNull(VarkaErrors.siteText(user));
    assertSame(user, VarkaErrors.planningFailure(user, () -> "p", VarkaEmitOptions.DEFAULTS));
    var declined = new VarkaEmitDeclined("over the budget", List.of(0));
    assertSame(declined, VarkaErrors.planningFailure(declined, () -> "p",
        VarkaEmitOptions.DEFAULTS));
  }

  @Test
  @DisplayName("a record renders a site's error as its cause by the site's text")
  void recordRendersASite() {
    var site = VarkaErrors.illegalState("word slot 3 is stored but never loaded");
    String block = new VarkaErrorContext("emission-failure", "k", "h", "", null, site).render();
    assertTrue(block.endsWith("\n  cause: org.apache.spark.SparkIllegalStateException: word slot"
        + " 3 is stored but never loaded"), block);
    assertTrue(!block.contains(VarkaErrors.REPORT), block);
  }

  @Test
  @DisplayName("a site's error signs as the site's JDK type and text, as before the carriers")
  void signatureReadsTheSite() {
    var site = assertThrows(IllegalArgumentException.class,
        () -> new VarkaValueRange.Bounded(5, 1));
    assertEquals(new VarkaFailureSignature("IllegalArgumentException", "empty range [#, #]"),
        VarkaFailureSignature.of(site, ""));
  }
}
