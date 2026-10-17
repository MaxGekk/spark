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

import static org.apache.spark.sql.VarkaBenchmarkBase.require;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.management.ObjectName;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelCompileDirective;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelWarmup;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaShapeCache;

/**
 * Why Varka keeps C1 off its kernel classes ({@code VarkaKernelCompileDirective}, {@code
 * VARKA-212.md} 10): the warm-up's verdict on a 54-entry kernel with the directive and without
 * it, each in fresh JVMs.
 *
 * <p>Every child JVM runs the sequence that exposed the trap: a Varka session with the warm-up
 * on, the ladder's hundred thousand cached rows, then twelve queries of a 16-entry shape and
 * twelve of a 54-entry one, back to back, each rung waiting for its warm-up's verdict. The
 * queries' own compiles keep C2's queue long while the new kernel crosses its first threshold,
 * which is when HotSpot compiles a method at tier 2 instead of tier 3. Without the directive a
 * kernel method compiled that way is stranded on boxing C1 code and the verdict never comes; with
 * it, every kernel method goes from the interpreter to C2. "Without" is the directive the
 * production code installs, overridden by one pushed on top of it that enables C1 again for the
 * same classes, so the production path needs no switch for this.
 *
 * <p>Fresh JVMs because the trap depends on what else the JIT is doing, which one JVM's history
 * would carry from one run into the next. Each child prints one line per rung. The children are
 * minutes of work, so the dry run, {@code -Dvarka.bench.dryRun=true}, which must take about a
 * JVM's start, writes the section and starts none.
 *
 * <p>To run this benchmark:
 * <pre>
 *   dev/varka_bench_regen.sh sql VarkaWarmupDirectiveBenchmark
 * </pre>
 */
public class VarkaWarmupDirectiveBenchmark extends VarkaSqlBenchmarkBase {

  private static final boolean SMOKE = "true".equals(System.getenv("VARKA_COLDSTART_SMOKE"));

  /** Fresh JVMs per mode. */
  private static final int RUNS = SMOKE ? 1 : 3;

  private static final long CHILD_TIMEOUT_SECONDS = 600L;

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    // The children make their own sessions; this JVM makes none, and stops one a failed class
    // left running under Spark's Benchmarks runner.
    stopLeftoverSessions();
    section("the warm-up's verdict with and without C1 kept off the kernel classes", () -> {
      if (dryRun()) {
        return;
      }
      for (String mode : List.of("without", "with")) {
        for (int run = 1; run <= RUNS; run++) {
          for (String line : child(mode)) {
            note(mode + " the directive, JVM " + run + ": " + line);
          }
        }
      }
    });
  }

  /**
   * Runs one child JVM in {@code mode} and returns its result lines. Its output goes to a file
   * rather than a pipe, so that a child which stalls cannot hold the parent past the timeout.
   */
  private static List<String> child(String mode) {
    var command = new ArrayList<String>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    // The benchmark JVM's own options - the module opens Spark needs, the heap, the vector
    // module - so the child is the same JVM but for its history.
    for (String option : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      if (!option.startsWith("-agentlib")) {
        command.add(option);
      }
    }
    command.add("-cp");
    command.add(childClassPath());
    command.add(Child.class.getName());
    command.add(mode);
    try {
      Path log = Files.createTempFile("varka-directive-child", ".log");
      try {
        Process process = new ProcessBuilder(command).redirectErrorStream(true)
            .redirectOutput(log.toFile()).start();
        boolean finished = process.waitFor(CHILD_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!finished) {
          process.destroyForcibly();
          process.waitFor(30, TimeUnit.SECONDS);
        }
        List<String> lines = Files.readAllLines(log, StandardCharsets.UTF_8);
        String tail = String.join("\n", lines.subList(Math.max(0, lines.size() - 40),
            lines.size()));
        if (!finished) {
          throw new IllegalStateException("the child did not finish in " + CHILD_TIMEOUT_SECONDS
              + " s; its last lines:\n" + tail);
        }
        List<String> results = lines.stream().filter(l -> l.startsWith(Child.RESULT))
            .map(l -> l.substring(Child.RESULT.length())).toList();
        require(process.exitValue() == 0 && !results.isEmpty(), "the child failed (exit "
            + process.exitValue() + "); its last lines:\n" + tail);
        return results;
      } finally {
        Files.deleteIfExists(log);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while waiting for the child", e);
    }
  }

  /**
   * The child's class path: this JVM's own, plus every local jar and directory its context
   * class loaders were given. Under {@code spark-submit}, which is how the benchmark workflow runs
   * benchmarks, the test jars that hold the child come in through {@code --jars} and are only
   * there.
   */
  private static String childClassPath() {
    var entries = new LinkedHashSet<String>();
    for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
      if (!entry.isEmpty()) {
        entries.add(entry);
      }
    }
    for (ClassLoader loader = Thread.currentThread().getContextClassLoader(); loader != null;
        loader = loader.getParent()) {
      if (loader instanceof URLClassLoader urls) {
        for (URL url : urls.getURLs()) {
          if (url.getProtocol().equals("file")) {
            try {
              entries.add(new File(url.toURI()).getPath());
            } catch (URISyntaxException e) {
              throw new IllegalArgumentException("not a file URL: " + url, e);
            }
          }
        }
      }
    }
    return String.join(File.pathSeparator, entries);
  }

  /**
   * One child JVM: the sequence, in the mode it is given. A nested class, since the benchmark's
   * own class inherits Spark's instance {@code main}, which a static one beside it cannot hide.
   */
  public static final class Child {

    static final String RESULT = "VARKA-DIRECTIVE-RESULT ";

    private Child() {}

    public static void main(String[] args) throws Exception {
      String mode = args[0];
      require(VarkaKernelCompileDirective.readyForWarmup()
          && VarkaKernelCompileDirective.installed(), "this JVM did not add the directive");
      if (mode.equals("without")) {
        // A directive pushed later is matched first, so this one wins for the kernel classes.
        Path file = Files.createTempFile("varka-c1-enabled", ".json");
        try {
          Files.writeString(file, "[{ match: \"" + VarkaKernelCompileDirective.METHOD_PATTERN
              + "\", c1: { Exclude: false } }]");
          ManagementFactory.getPlatformMBeanServer().invoke(
              new ObjectName("com.sun.management:type=DiagnosticCommand"),
              "compilerDirectivesAdd", new Object[] {new String[] {file.toString()}},
              new String[] {String[].class.getName()});
        } finally {
          Files.deleteIfExists(file);
        }
      }
      SparkSession session = VarkaArrowSessions.createSession(
          "VarkaWarmupDirective", true, true, "local[1]");
      try {
        VarkaSizeLadder.cacheDates(session, 100_000);
        for (int n : List.of(16, 54)) {
          VarkaShapeCache.invalidateAll();
          String q = "SELECT "
              + IntStream.rangeClosed(1, n).mapToObj(k -> VarkaSizeLadder.entry(100 + k))
                  .collect(Collectors.joining(", "))
              + " FROM ladder_dates";
          List<VarkaKernelWarmup.Outcome> before = VarkaKernelWarmup.recentOutcomes();
          VarkaKernelWarmup.Outcome previous = before.isEmpty() ? null : before.getLast();
          for (int i = 0; i < 12; i++) {
            session.sql(q).write().format("noop").mode("overwrite").save();
          }
          require(VarkaKernelWarmup.awaitIdle(300_000), "the " + n + "-entry warm-up did not "
              + "finish");
          VarkaKernelWarmup.Outcome o = VarkaKernelWarmup.recentOutcomes().getLast();
          // By identity, as the outcomes are kept: an equal verdict is still a new warm-up's.
          require(o != previous, "the " + n + "-entry shape had no warm-up of its own");
          System.out.println(RESULT + n + " entries: " + o.state() + " after "
              + o.runNanos() / 1_000_000 + " ms and " + o.calls() + " calls; the first probe "
              + "allocated " + o.firstProbeBytes() + " bytes, the last " + o.lastProbeBytes());
        }
      } finally {
        session.stop();
      }
    }
  }
}
