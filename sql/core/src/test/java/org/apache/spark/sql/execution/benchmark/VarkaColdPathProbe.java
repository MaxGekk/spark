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

import java.util.Set;

import org.apache.spark.sql.SparkSession;

/**
 * One arm of {@link VarkaColdPathBenchmark} in a loop, for a profiler or the JIT's own logs to
 * watch: the rung's steady-state query into a sink, by one path, for a number of seconds.
 *
 * <pre>
 *   build/sbt "project sql" \
 *     'set Test/javaOptions += "-XX:StartFlightRecording=filename=arm.jfr,settings=profile"' \
 *     "Test/runMain org.apache.spark.sql.execution.benchmark.VarkaColdPathProbe \
 *       columnar varka 16 30"
 * </pre>
 *
 * <p>The sink is {@code rows} or {@code columnar}, as in the benchmark. The arm is {@code
 * vanilla} (whole-stage code), {@code vanilla-rowwise} (whole-stage codegen off) or {@code varka}
 * (the Varka node with no kernel, so every batch takes its row path). The data is the
 * benchmark's, a hundred thousand Arrow-cached rows. The last line of output gives the number of
 * queries and their mean time, so that a profile can be matched to the benchmark's steady state.
 */
public final class VarkaColdPathProbe {

  private VarkaColdPathProbe() {}

  public static void main(String[] args) {
    require(args.length == 4, "usage: VarkaColdPathProbe <rows|columnar> "
        + "<vanilla|vanilla-rowwise|varka> <entries> <seconds>");
    String sink = args[0];
    String arm = args[1];
    String entries = args[2];
    String seconds = args[3];
    require(Set.of("vanilla", "vanilla-rowwise", "varka").contains(arm), "unknown arm " + arm);
    SparkSession session = VarkaArrowSessions.createSession(
        "VarkaColdPathProbe-" + arm, arm.equals("varka"), false, "local[1]");
    try {
      VarkaSizeLadder.cacheDates(session, 100_000);
      String q = VarkaColdPath.query(Integer.parseInt(entries), VarkaColdPath.STEADY);
      Runnable toSink = switch (sink) {
        case "rows" -> () -> VarkaColdPath.toRows(session, q);
        case "columnar" -> () -> VarkaColdPath.toNoop(session, q);
        default -> throw new IllegalArgumentException("unknown sink " + sink);
      };
      Runnable once = switch (arm) {
        case "vanilla" -> toSink;
        case "vanilla-rowwise" -> () -> VarkaColdPath.rowByRow(session, toSink);
        default -> () -> VarkaColdPath.noKernel(toSink);
      };
      Runnable restoreEvaluator = VarkaColdPath.silenceEvaluator();
      try {
        long start = System.nanoTime();
        long deadline = start + Long.parseLong(seconds) * 1_000_000_000L;
        int queries = 0;
        while (System.nanoTime() < deadline) {
          once.run();
          queries++;
        }
        double meanMs = (System.nanoTime() - start) / 1e6 / queries;
        System.out.println("VARKA_COLDPATH_PROBE sink=%s arm=%s entries=%s queries=%d mean=%.1f ms"
            .formatted(sink, arm, entries, queries, meanMs));
      } finally {
        restoreEvaluator.run();
      }
    } finally {
      session.stop();
    }
  }
}
