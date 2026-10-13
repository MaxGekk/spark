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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSuiteReport.Failed;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSuiteReport.Passed;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSuiteReport.Result;

/**
 * Runs one suite in a JVM of its own and returns each test's result (VARKA-246, {@code m8/SCOPE.md}
 * item 69): the machinery behind a suite marked to run alone, free of any test framework, whose
 * face ({@code VarkaOwnJvm} for ScalaTest) names the main class that runs the suite and the
 * arguments it takes (VARKA-312).
 *
 * <p>The child gets the parent's command-line arguments, classpath and {@code -D} properties (the
 * matrix configuration, the sanitizer, a fuzzer's budget and seed), and
 * {@code -Dvarka.ownJvm=true}, which runs the suite's tests directly. It writes a JUnit report
 * to a temporary directory that the parent reads. The child's output is copied to the parent's,
 * so the matrix's markers are where they were.
 *
 * <p>The child is ended from outside when it outlasts its budget, which is the shortest of the
 * suites' per-test caps ({@code varka.test.watchdog.minutes}, {@code spark.test.timeout}) less a
 * minute, so the parent reports the child's hang before a cap halts the JVM;
 * {@code -Dvarka.ownJvm.timeoutMinutes} overrides it.
 */
public final class VarkaChildJvm {

  private VarkaChildJvm() {}

  /** Set for the child, and by hand to run such a suite's tests in the JVM at hand. */
  public static final String PROPERTY = VarkaSpeciesGuard.OWN_JVM_PROPERTY;

  /** Whether this JVM is a child, or was told to run such suites directly. */
  public static boolean inChild() {
    return "true".equals(System.getProperty(PROPERTY));
  }

  /** The minutes a child may run: the shortest per-test cap less one, or the override. */
  static long budgetMinutes() {
    String override = System.getProperty("varka.ownJvm.timeoutMinutes");
    if (override != null) {
      return Long.parseLong(override);
    }
    long watchdog = VarkaWatchdog.defaultMinutes();
    long sparkCap = Long.parseLong(System.getProperty("spark.test.timeout", "20"));
    return Math.max(1L, Math.min(watchdog, sparkCap) - 1L);
  }

  /**
   * The parent's classpath. {@code java.class.path} is what the parent runs with; the
   * {@code SPARK_DIST_CLASSPATH} environment variable is what the other forking suites prefer,
   * for a launcher whose own classpath is a stub, so it is taken only when the parent's does not
   * hold ScalaTest.
   */
  private static String classpath() {
    String own = System.getProperty("java.class.path");
    if (own.contains("scalatest")) {
      return own;
    }
    String dist = System.getenv("SPARK_DIST_CLASSPATH");
    return dist == null || dist.isEmpty() ? own : dist;
  }

  /**
   * Each test's result from a child: its own where the report holds it, else {@code otherwise},
   * the failure every test asked about takes when the child timed out or left no report.
   */
  public record Results(Map<String, Result> byName, Optional<Failed> otherwise) {
    public Results {
      byName = Map.copyOf(byName);
    }

    /** The result of the test named {@code name}, empty when the child reported none. */
    public Optional<Result> of(String name) {
      Result own = byName.get(name);
      return own != null ? Optional.of(own) : otherwise.map(f -> f);
    }
  }

  /**
   * Runs {@code suite} in a child JVM through {@code mainClass}, given the report directory's
   * path as {@code reportFlag}'s value and then {@code arguments}, and returns each test's result
   * by name. Every test the report holds as passed fails instead when the child timed out, and a
   * child that left no report fails every test asked about, with its last lines.
   */
  public static Results run(String suite, String mainClass, List<String> arguments,
      String reportFlag) throws Exception {
    File report = Files.createTempDirectory("varka-own-jvm").toFile();
    File javaBin = new File(new File(System.getProperty("java.home"), "bin"), "java");
    var command = new ArrayList<String>();
    command.add(javaBin.getAbsolutePath());
    // The parent's own command-line arguments: heap, the incubator module, native access, every
    // -D the run was given. A -javaagent or -agentlib would start twice, and the child needs
    // neither - except JaCoCo's, which dev/varka_gen_coverage.sh puts on every test JVM and which
    // must reach the suites run here too, or their kernels go unmeasured (VARKA-285); it appends
    // to its file under a lock, so a second JVM writing it is what it is built for. Properties
    // the parent set programmatically are not copied.
    for (String a : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      boolean agent = a.startsWith("-javaagent") && !a.contains("org.jacoco.agent");
      if (!agent && !a.startsWith("-agentlib")) {
        command.add(a);
      }
    }
    command.add("-D" + PROPERTY + "=true");
    command.add("-cp");
    command.add(classpath());
    command.add(mainClass);
    command.add(reportFlag);
    command.add(report.toString());
    command.addAll(arguments);
    var builder = new ProcessBuilder(command);
    builder.redirectErrorStream(true);
    builder.environment().put("SPARK_TESTING", "1");
    Process process = builder.start();
    long budget = budgetMinutes();
    // The read below ends only when the child exits, so a hung child is ended from here.
    var timedOut = new AtomicBoolean(false);
    var killer = new Thread(() -> {
      try {
        if (!process.waitFor(budget, TimeUnit.MINUTES)) {
          timedOut.set(true);
          process.destroyForcibly();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    killer.setDaemon(true);
    killer.start();
    var tail = new ArrayDeque<String>();
    try (var reader = new BufferedReader(
        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        tail.addLast(line);
        if (tail.size() > 30) {
          tail.removeFirst();
        }
      }
      process.waitFor();
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
      }
    }
    Map<String, Result> parsed = VarkaSuiteReport.read(report);
    if (timedOut.get()) {
      return failAll(parsed,
          suite + " did not finish in " + budget + " minutes in its own JVM");
    } else if (parsed.isEmpty()) {
      return failAll(parsed, suite + " left no report in its own JVM (exit "
          + process.exitValue() + "):\n" + String.join("\n", tail));
    }
    return new Results(parsed, Optional.empty());
  }

  /** {@code parsed} with every pass turned into a failure with {@code message}, which also
   *  answers for any test it does not hold. */
  private static Results failAll(Map<String, Result> parsed, String message) {
    var failure = new Failed(message);
    var failed = new LinkedHashMap<String, Result>();
    parsed.forEach((name, result) -> failed.put(name, result instanceof Passed ? failure : result));
    return new Results(failed, Optional.of(failure));
  }
}
