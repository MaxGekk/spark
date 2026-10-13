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

import java.time.Duration;
import java.util.List;

import scala.collection.immutable.Seq;

import org.apache.spark.sql.VarkaBenchmarkBase;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.DateAdd;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaLoopEmitter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSqlResolve;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR;
import org.apache.spark.sql.types.DataTypes;

/**
 * Class-generation time benchmark (VARKA-7): Gen-time for the Janino string path
 * ({@code GenerateUnsafeProjection.generate}, i.e. string generation + {@code
 * CodeGenerator.compile}) vs the Varka Class-File path - {@code VarkaLoopEmitter.emit} of a
 * representative fused kernel (two outputs sharing a subchain, the shape the throughput
 * benchmark's DAG-CSE case runs) plus define, load and instantiate through a fresh {@link
 * VarkaGeneratedClassLoader}, which is the exact per-task work {@code VarkaKernelEvaluator}'s
 * runner does. A fresh literal each Janino iteration defeats the global compiler cache so every
 * iteration is a cold compile, matching the first-query cost.
 *
 * <p>VARKA-14 added the fused case beside milestone 1's per-op dispatcher case; VARKA-17 retired
 * the dispatchers with the rest of that layer, so what remains measures only machinery that runs.
 *
 * <p>To run this benchmark:
 * <pre>
 *   dev/varka_bench_regen.sh core VarkaCodegenBenchmark
 * </pre>
 * or, for the table alone, {@code build/sbt "sql/Test/runMain
 * org.apache.spark.sql.execution.benchmark.VarkaCodegenBenchmark"}; the results file is
 * "benchmarks/VarkaCodegenBenchmark-jdk&lt;NN&gt;-results.txt".
 */
public class VarkaCodegenBenchmark extends VarkaBenchmarkBase {

  private static final Attribute START_ATTR = VarkaSqlResolve.column("d", DataTypes.DateType);
  private static final Seq<Attribute> FIELDS = seq(List.of(START_ATTR));
  // The representative fused-kernel IR: `date_add(d, 1) AS a, datediff(date_add(d, 1), d2) AS b`
  // - two outputs, a shared subchain, two inputs, one literal slot. The same shape as the
  // throughput benchmark's DAG-CSE case, so the two committed files describe one kernel.
  private static final VarkaVectorIR SHARED_CHAIN = new VarkaVectorIR.AddDays(
      new VarkaVectorIR.ColumnRef(0), new VarkaVectorIR.LiteralSlot(0));
  private static final List<VarkaVectorIR> FUSED_OUTPUTS = List.of(
      SHARED_CHAIN, new VarkaVectorIR.DateDiff(SHARED_CHAIN, new VarkaVectorIR.ColumnRef(1)));

  /**
   * Keeps the compiled result alive so the compile work cannot be elided: a plain field written
   * each iteration, as the Scala form's captured {@code var} was, and published once to {@link
   * #blackhole} after the run.
   */
  private Object sink;

  private volatile Object blackhole;

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    section("Varka per-op class generation (Gen-time)", () -> {
      var benchmark = benchmark("class generation time", 1)
          .minIterations(2).warmup(Duration.ofSeconds(1)).minTime(Duration.ofSeconds(1)).build();

      benchmark.addCase("janino: GenerateUnsafeProjection.generate", 5, i -> {
        // A fresh literal per iteration defeats the CodeGenerator cache so each iteration is a
        // cold string-generate + Janino compile.
        Expression add = new DateAdd(START_ATTR, Literal.create(i + 1, DataTypes.IntegerType));
        sink = GenerateUnsafeProjection.generate(seq(List.of(add)), FIELDS);
      });

      benchmark.addCase("varka: fused emit + define + load + instantiate", 5, i -> {
        // The per-task sequence VarkaKernelEvaluator's runner performs for the fused loop.
        String className = "org.apache.spark.sql.varka.execution.VarkaFusedProjectionBench" + i;
        byte[] bytes = VarkaLoopEmitter.emit(className, FUSED_OUTPUTS, 2, 1);
        var loader = new VarkaGeneratedClassLoader(Thread.currentThread().getContextClassLoader());
        sink = defineKernel(loader, className, bytes);
        loader.release();
      });

      benchmark.run();
      blackhole = sink;
    });
  }
}
