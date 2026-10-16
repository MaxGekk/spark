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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import scala.runtime.BoxedUnit;

import com.sun.management.HotSpotDiagnosticMXBean;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.VarkaBenchmark;
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator;
import org.apache.spark.sql.execution.WholeStageCodegenExec;
import org.apache.spark.sql.internal.SQLConf;

/**
 * Can vanilla Spark tune its way off the size ladder's cliff? {@link VarkaSizeLadderBenchmark}
 * shows stock Spark's time per row stepping up several times over where its generated consume
 * method passes HotSpot's 8000-byte {@code HugeMethodLimit} and is never compiled. This benchmark
 * runs the same rungs, data and query on vanilla Spark only, under the settings a reader would
 * name as the remedy, so the size of what they buy, and what they cost below the cliff, is
 * measured rather than argued:
 *
 * <ul>
 *   <li>{@code defaults}, the ladder's own vanilla arm;</li>
 *   <li>{@code spark.sql.codegen.hugeMethodLimit=8000}: an internal setting, default 65535, whose
 *       own doc advises 8000 on HotSpot. Past it Spark keeps the whole-stage codegen node in the
 *       plan but executes its child with the per-operator code paths instead;</li>
 *   <li>{@code spark.sql.codegen.wholeStage=false}: whole-stage codegen off for every stage.</li>
 * </ul>
 *
 * <p>The fourth remedy, {@code -XX:-DontCompileHugeMethods}, is a JVM flag rather than a session
 * setting, so it is a second run of this class under the flag rather than an arm. The class reads
 * the flag back from the running JVM, names its results file after it ({@code
 * -dontcompilehugemethods-off-results.txt}) and states it in the file's header, so a file can
 * never claim a flag its JVM did not have.
 *
 * <p>Before timing a rung, each arm runs the query once and the benchmark checks that the plan did
 * what the setting promises, failing the run otherwise: under the defaults whole-stage codegen ran
 * at every rung, under {@code wholeStage=false} the plan has no whole-stage stage, and under
 * {@code hugeMethodLimit=8000} it ran exactly at the rungs whose method is within 8000 bytes.
 * Whether a stage ran is read from its {@code pipelineTime} metric, which only the compiled
 * pipeline updates: the node is in the plan either way. After each table a note records vanilla's
 * largest method and what each arm executed. The dry run, {@code -Dvarka.bench.dryRun=true}, makes
 * those checks and prints those notes; it times nothing.
 *
 * <p>Each timed iteration puts the session in its arm's settings before its query and leaves them
 * there for the next arm's {@link #use} to clear: scoping them with {@link #withConf} would add a
 * restore to every timed iteration, work the measured query does not do.
 *
 * <p>To run this benchmark (the flag run takes the same command with the option added to the sbt
 * test JVM, as {@code dev/varka_bench_regen.sh}'s narrow run adds {@code -XX:MaxVectorSize}):
 * <pre>
 *   dev/varka_bench_regen.sh sql VarkaSizeLadderTuningBenchmark --no-narrow
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "project sql" \
 *     'set Test/javaOptions += "-XX:-DontCompileHugeMethods"' \
 *     "Test/runMain org.apache.spark.sql.execution.benchmark.VarkaSizeLadderTuningBenchmark"
 * </pre>
 * Vanilla arms only, so the vector width does not matter and there is no narrow companion.
 */
public class VarkaSizeLadderTuningBenchmark extends VarkaSqlBenchmarkBase {

  /** The {@code hugeMethodLimit} arm's value: HotSpot's {@code HugeMethodLimit}, as Spark's doc
   *  advises. */
  private static final int TUNED_LIMIT = CodeGenerator.DEFAULT_JVM_HUGE_METHOD_LIMIT();

  /**
   * One setting under test: the name the results file shows, what it sets, and how it must
   * execute a rung whose vanilla method is a given number of bytes long.
   */
  private record Arm(String name, Map<String, String> settings, IntFunction<String> expected) {}

  /** The settings under test, in the order the tables show them. */
  private static final List<Arm> ARMS = List.of(
      new Arm("defaults", Map.of(), bytes -> "whole-stage"),
      new Arm("hugeMethodLimit=" + TUNED_LIMIT,
          Map.of(SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT().key(), Integer.toString(TUNED_LIMIT)),
          bytes -> bytes > TUNED_LIMIT ? "per-operator" : "whole-stage"),
      new Arm("wholeStage=false", Map.of(SQLConf.WHOLESTAGE_CODEGEN_ENABLED().key(), "false"),
          bytes -> "no whole-stage"));

  /** Every key an arm sets, each once, which {@link #use} clears before setting an arm's. */
  private static final List<String> ARM_KEYS = ARMS.stream()
      .flatMap(arm -> arm.settings().keySet().stream()).distinct().toList();

  /** HotSpot's own answer, not the command line's: whether huge methods are left interpreted. */
  private final boolean dontCompileHugeMethods = Boolean.parseBoolean(
      ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
          .getVMOption("DontCompileHugeMethods").getValue());

  @Override
  public String suffix() {
    return dontCompileHugeMethods ? "" : "-dontcompilehugemethods-off";
  }

  /** Puts the session in one arm's settings, clearing whatever the previous arm set. */
  private static void use(SparkSession session, Map<String, String> settings) {
    ARM_KEYS.forEach(session.conf()::unset);
    settings.forEach(session.conf()::set);
  }

  /**
   * Runs the query once under the session's current settings and says how it executed: "whole-
   * stage" if a whole-stage codegen stage ran its compiled pipeline, "per-operator" if the plan
   * has a stage that fell back to its child, "no whole-stage" if the plan has none.
   */
  private static String executedAs(SparkSession session, String query) {
    Dataset<Row> df = session.sql(query);
    df.queryExecution().toRdd().count();
    var stages = new ArrayList<WholeStageCodegenExec>();
    df.queryExecution().executedPlan().foreach(p -> {
      if (p instanceof WholeStageCodegenExec w) {
        stages.add(w);
      }
      return BoxedUnit.UNIT;
    });
    if (stages.isEmpty()) {
      return "no whole-stage";
    } else if (stages.stream().allMatch(w -> w.metrics().apply("pipelineTime").value() > 0)) {
      return "whole-stage";
    } else {
      return "per-operator";
    }
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    // The ladder's session reads the Arrow cache, as its vanilla arm does; the default session,
    // with the default cache serializer, is never made.
    stopLeftoverSessions();
    SparkSession session = VarkaArrowSessions.createSession(
        "VarkaSizeLadderTuning", false, false, "local[1]");
    try {
      clearSessions();
      ladder(session);
    } finally {
      session.stop();
    }
  }

  private void ladder(SparkSession session) {
    int numRows = VarkaSizeLadder.NUM_ROWS;
    VarkaSizeLadder.cacheDates(session);
    section("vanilla Spark's settings on the size ladder "
        + "(DontCompileHugeMethods=" + dontCompileHugeMethods + ")", () -> {
      for (int n : VarkaSizeLadder.RUNGS) {
        String query = VarkaSizeLadder.query(n);
        use(session, Map.of());
        int bytes = VarkaArrowSessions.vanillaMethodBytes(session, query);
        var executed = new ArrayList<String>(ARMS.size());
        for (Arm arm : ARMS) {
          use(session, arm.settings());
          String as = executedAs(session, query);
          String expected = arm.expected().apply(bytes);
          require(as.equals(expected),
              "rung " + n + ", " + arm.name() + ": expected " + expected
                  + " execution, the plan ran " + as);
          executed.add(arm.name() + " " + as);
        }
        VarkaBenchmark benchmark = benchmark(
            n + " entries over " + numRows + " Arrow-cached rows", numRows).timed().build();
        for (Arm arm : ARMS) {
          benchmark.addCase("vanilla Spark, " + arm.name(), i -> {
            use(session, arm.settings());
            noop(session.sql(query));
          });
        }
        runCases(benchmark);
        note("rung " + n + ": vanilla's largest generated method is " + bytes + " bytes, "
            + (bytes > TUNED_LIMIT ? "past" : "under") + " HugeMethodLimit " + TUNED_LIMIT + "; "
            + String.join(", ", executed));
      }
    });
  }
}
