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

package org.apache.spark.sql;

import java.util.List;

import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaLoopEmitter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.AddDays;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.AddMonths;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.And;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Compare;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.CompareOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfMonth;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Greatest;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntArith;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IsNotNull;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LastDay;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.MakeDate;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Month;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ThursdayOf;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.WeekOfYear;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Year;

/**
 * What it costs to <i>build</i> a fused kernel, as opposed to running one: the time {@link
 * VarkaLoopEmitter} spends turning an IR tree into class bytes, and the time the JVM spends
 * defining the class those bytes describe.
 *
 * <p>This is the cost a query pays once per distinct shape, before the first batch and behind the
 * shape cache, so it never appears in a throughput figure. It is worth its own file for two
 * reasons. It is the number a reader asks for when told that a query compiles itself - "how long
 * does that take" has had no committed answer, only an estimate in a plan. And it is the number
 * the lane refactor moves: parameterising the emitter on a lane descriptor builds a {@code
 * MethodTypeDesc} per lane member and consults the descriptor at every one of the forty per-lane
 * sites, which is work done while emitting. A baseline measured before that change is what makes
 * "the refactor costs nothing at emit time" a claim that can be checked rather than asserted.
 *
 * <p>Two sections, and two for wide kernels: the VARKA-190 ladder, and the grouping priced by the
 * emit cost model (see {@code VARKA-199.md}).
 *
 * <p><b>Emission alone</b> calls {@code VarkaLoopEmitter.emit} and throws the bytes away. It
 * isolates the emitter: the IR walk, the analysis pass, the loop and epilogue bodies, the constant
 * pool. The shapes rise from one leaf to a four-output projection over calendar nodes, so a reader
 * can see how the cost scales with what is being compiled rather than only its value for one
 * shape.
 *
 * <p><b>Emission and definition</b> adds {@code VarkaGeneratedClassLoader.defineKernelClass},
 * which is what a cold query actually pays: the JVM verifies the bytes and links the class. The
 * pair prices the emitter against the platform, and the split matters because only the first half
 * is Varka's to improve.
 *
 * <p>Every case emits a fresh class each iteration - the shape cache is deliberately not involved,
 * since its hit path is VARKA-18's subject and costs a map lookup.
 *
 * <pre>
 *   build/sbt "catalyst/Test/runMain org.apache.spark.sql.VarkaEmissionBenchmark"
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt \
 *     "catalyst/Test/runMain org.apache.spark.sql.VarkaEmissionBenchmark"
 * </pre>
 */
public class VarkaEmissionBenchmark extends VarkaBenchmarkBase {

  /** Emissions per measured iteration: enough that one iteration is milliseconds, not micros. */
  private static final int EMISSIONS_PER_ITERATION = 200;

  private static final VarkaVectorIR COL = new ColumnRef(0);
  private static final VarkaVectorIR COL1 = new ColumnRef(1);
  private static final VarkaVectorIR LIT = new LiteralSlot(0);

  /** A shape: its name, its roots, and the number of inputs and literals it needs. */
  private record Shape(String name, List<VarkaVectorIR> roots, int inputs, int literals) { }

  /** A case of the prediction section: its name, roots and base options. */
  private record Predicted(String name, List<VarkaVectorIR> roots, VarkaEmitOptions base) { }

  /** The shapes, smallest first, with the number of inputs and literals each needs. */
  private static final List<Shape> SHAPES = List.of(
      new Shape("a column, copied", List.of(COL), 1, 0),
      new Shape("d + 3", List.of(new AddDays(COL, LIT)), 1, 1),
      new Shape("i + 1, ANSI checked", List.of(new IntArith(IntOp.ADD, Overflow.FAIL, COL, LIT)),
          1, 1),
      new Shape("year(d)", List.of(new Year(COL)), 1, 0),
      new Shape("a predicate: d < d2 AND d IS NOT NULL",
          List.of(new And(new Compare(CompareOp.LT, COL, COL1), new IsNotNull(COL))), 2, 0),
      new Shape("make_date(year(d), month(d), 1)",
          List.of(new MakeDate(new Year(COL), new Month(COL), LIT, true)), 1, 1),
      new Shape("four calendar outputs over one date",
          List.of(new Year(COL), new Month(COL), new DayOfMonth(COL),
              new WeekOfYear(new ThursdayOf(COL))), 1, 0));

  /** Keeps the emitted bytes and the defined classes live to the end of the run. */
  private int sink = 0;

  /**
   * In a dry run, emits a case's shape once before the case is added: every emission here is
   * the timed work itself, which a dry run skips, so without this a dry run would check none of
   * the shapes builds. A timed run does nothing here, so its cases are as they were.
   */
  private void checkInDryRun(List<VarkaVectorIR> roots, int inputs, int literals,
      VarkaEmitOptions options) {
    if (dryRun()) {
      sink += VarkaLoopEmitter.emit("VarkaEmissionBenchmarkCheck", List.copyOf(roots), inputs,
          literals, null, null, options).length;
    }
  }

  private static VarkaVectorIR entry(int k) {
    return new Greatest(new Greatest(
        new AddMonths(COL, new LiteralSlot(k)), new AddDays(COL, new LiteralSlot(k))),
        new LastDay(COL));
  }

  private static VarkaVectorIR tail(int k) {
    return new IntArith(IntOp.ADD, Overflow.WRAP, new Year(COL), new LiteralSlot(k));
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    VarkaEmitOptions options = VarkaEmitOptions.DEFAULTS;

    section("emitting a fused kernel: bytes only", () -> {
      var benchmark = benchmark("one emission", EMISSIONS_PER_ITERATION).timed().build();
      for (Shape shape : SHAPES) {
        VarkaVectorIR[] roots = shape.roots().toArray(new VarkaVectorIR[0]);
        checkInDryRun(shape.roots(), shape.inputs(), shape.literals(), options);
        benchmark.addCase(shape.name(), iteration -> {
          int i = 0;
          while (i < EMISSIONS_PER_ITERATION) {
            byte[] bytes = VarkaLoopEmitter.emit("VarkaEmissionBenchmarkKernel",
                List.of(roots), shape.inputs(), shape.literals(), null, null, options);
            // Read one byte so the emission cannot be optimized away as dead.
            sink += bytes.length;
            i += 1;
          }
        });
      }
      runCases(benchmark);
    });

    section("emitting a fused kernel: bytes and a defined class", () -> {
      var benchmark = benchmark("one emission and definition", EMISSIONS_PER_ITERATION)
          .timed().build();
      for (Shape shape : SHAPES) {
        VarkaVectorIR[] roots = shape.roots().toArray(new VarkaVectorIR[0]);
        checkInDryRun(shape.roots(), shape.inputs(), shape.literals(), options);
        benchmark.addCase(shape.name(), iteration -> {
          int i = 0;
          while (i < EMISSIONS_PER_ITERATION) {
            // A loader per class, as the shape cache gives each entry, so a defined class can be
            // collected rather than accumulating for the length of the run.
            var loader = new VarkaGeneratedClassLoader(getClass().getClassLoader());
            String className = "VarkaEmissionBenchmarkKernel" + i;
            byte[] bytes = VarkaLoopEmitter.emit(className, List.of(roots), shape.inputs(),
                shape.literals(), null, null, options);
            sink += loader.defineGeneratedClass(className, bytes).getSimpleName().length();
            loader.release();
            i += 1;
          }
        });
      }
      runCases(benchmark);
    });

    // VARKA-190: kernels as wide as a real projection gets once the op cap no longer bounds
    // them. One emission per iteration - a four-hundred-output emission is not microseconds -
    // and the byte budget set out of reach at 200 and 400 outputs, where the default would
    // decline the driver: the rungs price emission itself, which the designs past the driver's
    // ceiling (VARKA-190.md 3.2) both pay, and prediction 2 there reads them. VARKA-191 adds the
    // second arm at every rung: the frames planned over the whole kernel, the form before it,
    // against frames planned over each group's own nodes; both named, so the labels survive
    // the default changing.
    section("emitting a wide kernel: four-op outputs (VARKA-190)", () -> {
      var benchmark = benchmark("one emission", 1).timed().build();
      for (int n : new int[] {25, 50, 100, 200, 400}) {
        List<VarkaVectorIR> roots = List.copyOf(roots(n, VarkaEmissionBenchmark::entry));
        VarkaEmitOptions wide = n <= 100 ? options : options.withMethodByteBudget(1 << 20);
        String budget = n <= 100 ? "" : ", budget out of reach";
        for (boolean groupLocal : new boolean[] {false, true}) {
          String frames = groupLocal ? "group-local frames" : "kernel-wide frames";
          VarkaEmitOptions arm = wide.withGroupLocalSlots(groupLocal);
          checkInDryRun(roots, 1, n, arm);
          benchmark.addCase(n + " outputs, " + frames + budget, iteration -> {
            sink += VarkaLoopEmitter.emit("VarkaEmissionBenchmarkWide", roots, 1, n, null,
                null, arm).length;
          });
        }
      }
      runCases(benchmark);
    });

    // The first grouping priced by the emit cost model against the weights alone. The
    // four-hundred-entry ladder with the budget out of reach asks the model at every output and
    // never acts on it, which is the model's own cost; the cheap tails are the shape the
    // call-site budget rebuilds under the weights and the prediction builds once.
    section("grouping by prediction", () -> {
      var benchmark = benchmark("one emission", 1).timed().build();
      var cases = List.of(
          new Predicted("400-entry ladder, budget out of reach",
              roots(400, VarkaEmissionBenchmark::entry), options.withMethodByteBudget(1 << 20)),
          new Predicted("64 cheap tails", roots(64, VarkaEmissionBenchmark::tail), options));
      for (Predicted c : cases) {
        for (boolean predict : new boolean[] {false, true}) {
          VarkaEmitOptions arm = c.base().withPredictGrouping(predict);
          List<VarkaVectorIR> rootList = List.copyOf(c.roots());
          int size = c.roots().size();
          checkInDryRun(rootList, 1, size, arm);
          benchmark.addCase(c.name() + ", " + (predict ? "predicted" : "weights"), iteration -> {
            sink += VarkaLoopEmitter.emit("VarkaEmissionBenchmarkPredicted", rootList, 1,
                size, null, null, arm).length;
          });
        }
      }
      runCases(benchmark);
    });

    // Keeps the emitted bytes and the defined classes live to the end of the run.
    if (sink == Integer.MIN_VALUE) {
      System.out.println(sink);
    }
  }
}
