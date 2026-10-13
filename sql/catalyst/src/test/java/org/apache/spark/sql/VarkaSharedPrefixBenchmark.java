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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import scala.collection.immutable.Seq;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitterTestSupport;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaFusedKernel;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaLoopEmitter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaMethodNames;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSqlResolve;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR;
import org.apache.spark.sql.types.DataTypes;

/**
 * What recomputing the civil-from-days prefix costs, per group that recomputes it, against
 * computing it once per batch (VARKA-198, {@code sql/varka/plans/m6/VARKA-198.md}).
 *
 * <p>A calendar output - {@code year(d)}, {@code month(d)}, {@code make_date(year(d), month(d),
 * k)} - decomposes its date through a prefix of about forty vector operations, and the outputs of
 * one loop method that decompose the same date share it. A kernel whose outputs are split across
 * several loop methods recomputes it in each - or, under {@code materializeChronoPrefix}, the
 * first method stores the prefix's vectors into a scratch region the caller passes and the later
 * methods load them. Each table takes the same outputs at the fused ceiling lowered step by step,
 * so the emitter splits them into more groups, and at each ceiling emits them both ways: the
 * recomputed arm and the materialized arm at the same group count, so their difference is what
 * the materialization wins at that many groups, and the recomputed arms' differences price the
 * recomputations the way the admission check did before the mechanism was built.
 *
 * <p>Two shapes. Cheap tails over one date, {@code year(d) + k}, where the prefix is most of each
 * group's work and the ceiling is widest; and the make_date ladder's top rung, 60 {@code
 * make_date(year(d), month(d), k)} outputs, the shape {@code VARKA-87.md} 3.3 counted eleven
 * repeated prefixes in. Each arm's table row names its group count, read from the emitted class,
 * so the x-axis is in the file. Null-free batches of 1024 rows, which every lane count divides:
 * the loop methods are what is measured, and the epilogue returns at once.
 *
 * <p>Two more arms at the default ceiling price the call-site budget (VARKA-209), which splits a
 * wide group whose loop method C1 refuses and leaves a narrow one: the budget off, so the cheap
 * tails are the one loop method past C1 the census measured; and the budget with its heavy-group
 * exemption off, so the make_date outputs split to one a method as the cheap tails do. The
 * default arms carry the budget as production does.
 *
 * <p>To run this benchmark:
 * <pre>
 *   dev/varka_bench_regen.sh catalyst VarkaSharedPrefixBenchmark
 * </pre>
 */
public class VarkaSharedPrefixBenchmark extends VarkaBenchmarkBase {

  private static final int NUM_ROWS = 1_000_000;
  private static final int CHUNK = 1024;

  private static final Attribute D = VarkaSqlResolve.column("d", DataTypes.DateType);
  private static final Seq<Attribute> COLUMNS = seq(List.of(D));

  /** Fused ceilings from the default down, each splitting the outputs into more groups. */
  private static final List<Integer> CEILINGS = List.of(400, 200, 100, 50);

  private final Set<String> usedIds = new HashSet<>();

  /** A shape: its title and its outputs as SQL. */
  private record Shape(String title, List<String> sqls) { }

  /** An emitted kernel and the loop methods it has: its groups. */
  private record Emitted(VarkaFusedKernel kernel, int groups) { }

  /** One arm of a shape: its label, whether the prefix is materialized, and its kernel. */
  private record Arm(
      String label, boolean materialized, int groups, VarkaFusedKernel kernel, long scratch) { }

  private static CompiledVarkaProjection compile(List<String> sqls) {
    var exprs = new ArrayList<NamedExpression>();
    for (int k = 0; k < sqls.size(); k++) {
      exprs.add(VarkaSqlResolve.named(VarkaSqlResolve.parse(sqls.get(k), COLUMNS), "c" + k));
    }
    return VarkaExpressionCompiler.compile(seq(exprs), COLUMNS).orElseThrow(() ->
        new IllegalStateException("the projection did not fuse: " + sqls.get(0) + ", ..."));
  }

  /** The two shapes, by title. */
  private static List<Shape> shapes() {
    var tails = new ArrayList<String>();
    for (int k = 1; k <= 64; k++) {
      tails.add("year(d) + " + k);
    }
    var makeDates = new ArrayList<String>();
    for (int k = 1; k <= 60; k++) {
      int day = (k - 1) % 28 + 1;
      int yearOffset = (k - 1) / 28;
      String year = yearOffset == 0 ? "year(d)" : "year(d) + " + yearOffset;
      makeDates.add("make_date(" + year + ", month(d), " + day + ")");
    }
    return List.of(
        new Shape("64 cheap tails over one date: year(d) + k", tails),
        new Shape("60 make_date(year(d), month(d), k) outputs", makeDates));
  }

  private Emitted emit(CompiledVarkaProjection fused, VarkaGeneratedClassLoader loader,
      String id, VarkaEmitOptions options) {
    require(usedIds.add(id), "case id " + id + " is already in use by another emit in this "
        + "benchmark");
    String name = "org.apache.spark.sql.varka.execution.VarkaSharedPrefixBench" + id;
    var roots = new ArrayList<VarkaVectorIR>(fused.outputs());
    byte[] bytes = VarkaLoopEmitter.emit(name, roots, fused.inputOrdinals().size(),
        fused.numLiterals(), null, null, options);
    int groups = (int) VarkaEmitterTestSupport.methodNames(bytes).stream()
        .filter(e -> VarkaMethodNames.isLoop(e, true)).count();
    return new Emitted(defineKernel(loader, name, bytes), groups);
  }

  private static int drive(VarkaFusedKernel kernel, long scratch, MemorySegment data,
      long[] dst, long[] dstValidity, int[] literals) {
    int status = 0;
    int done = 0;
    while (done < NUM_ROWS) {
      int len = Math.min(CHUNK, NUM_ROWS - done);
      status |= kernel.run(new long[] {data.address()}, new long[] {0L}, new int[] {0}, dst,
          dstValidity, literals, len, scratch);
      done += len;
    }
    return status;
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    var loader = new VarkaGeneratedClassLoader(getClass().getClassLoader());
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment data = arena.allocate(CHUNK * 4L, 64);
      for (int i = 0; i < CHUNK; i++) {
        data.set(ValueLayout.JAVA_INT, i * 4L, i * 7 % 20000 - 10000);
      }
      section("the civil-from-days prefix, recomputed per group or computed once", () -> {
        var shapes = shapes();
        for (int s = 0; s < shapes.size(); s++) {
          String title = shapes.get(s).title();
          List<String> sqls = shapes.get(s).sqls();
          CompiledVarkaProjection fused = compile(sqls);
          int n = sqls.size();
          int[] literals = fused.literals().stream().mapToInt(Integer::intValue).toArray();
          long[] dst = new long[n];
          for (int k = 0; k < n; k++) {
            dst[k] = arena.allocate(CHUNK * 4L, 64).address();
          }
          long[] dstValidity = new long[n];
          for (int k = 0; k < n; k++) {
            dstValidity[k] = arena.allocate(CHUNK / 8L, 64).address();
          }
          // Per ceiling, the recomputed arm and the materialized one; the latter takes the
          // scratch its kernel asks for, sized by the batch. Then the two call-site budget arms
          // at the default ceiling, materialized: the budget off, and every group split.
          var arms = new ArrayList<Arm>();
          for (int c : CEILINGS) {
            for (boolean materialized : new boolean[] {false, true}) {
              Emitted e = emit(fused, loader, s + "_" + c + "_" + materialized,
                  VarkaEmitOptions.DEFAULTS.withFusedCeiling(c)
                      .withMaterializeChronoPrefix(materialized));
              long scratch = VarkaEmitterTestSupport.scratch(e.kernel(), CHUNK);
              arms.add(new Arm("fused ceiling " + c, materialized, e.groups(), e.kernel(),
                  scratch));
            }
          }
          addBudgetArm(arms, fused, loader, s, "call-site budget off",
              VarkaEmitOptions.DEFAULTS.withCallSiteBudget(0));
          addBudgetArm(arms, fused, loader, s, "every group split",
              VarkaEmitOptions.DEFAULTS.withHeavyGroupOutputs(0));
          for (Arm arm : arms) {
            require(drive(arm.kernel(), arm.scratch(), data, dst, dstValidity, literals) == 0,
                title + " at " + arm.label() + ", materialized " + arm.materialized()
                    + ", declined a batch");
          }
          var benchmark = benchmark(title + " over " + NUM_ROWS + " rows", NUM_ROWS)
              .timed().build();
          for (Arm arm : arms) {
            String name = arm.label() + ": " + arm.groups() + " group"
                + (arm.groups() == 1 ? "" : "s")
                + (arm.materialized() ? ", prefix computed once" : ", prefix per group");
            benchmark.addCase(name, iteration ->
                drive(arm.kernel(), arm.scratch(), data, dst, dstValidity, literals));
          }
          benchmark.run();
        }
      });
    }
  }

  private void addBudgetArm(List<Arm> arms, CompiledVarkaProjection fused,
      VarkaGeneratedClassLoader loader, int s, String label, VarkaEmitOptions options) {
    Emitted e = emit(fused, loader, s + "_" + label, options);
    long scratch = VarkaEmitterTestSupport.scratch(e.kernel(), CHUNK);
    arms.add(new Arm("fused ceiling " + CEILINGS.get(0) + ", " + label, true, e.groups(),
        e.kernel(), scratch));
  }
}
