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
import java.util.List;
import java.util.function.IntPredicate;

import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaFusedKernel;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaLoopEmitter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DateDiff;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntArith;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntNeg;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Month;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Year;

/**
 * What int32 arithmetic costs in the fused loop, and what Spark's ANSI overflow check costs on top
 * of it ({@code sql/varka/plans/m4/VARKA-63.md}).
 *
 * <p>A file of its own rather than another section in {@code VarkaEmitterParityBenchmark}, whose
 * subject is a lowering priced against its hand-written kernel or against a second lowering of the
 * same shape. The question here is a different one - what a <i>semantic</i> obligation costs, when
 * the evaluation mode is the only thing that changed - and its rows can then be regenerated,
 * reviewed and quoted on their own.
 *
 * <p>Three things are measured.
 *
 * <p><b>The check's price, as an A/B on one node.</b> Every checked case appears twice, as the
 * same IR emitted with {@code checkIntOverflow} on and off. Off is not a setting any query may run
 * under - it drops an obligation ANSI mode imposes - it is the reference arm, the same device
 * VARKA-52 uses for {@code guardDayProducers}. The difference between the two rows is the sign
 * test: four lanewise ops and a compare for {@code +} and {@code -}, one compare for unary minus,
 * which reads the operand rather than the result. The wrapping form of the same operation is
 * beside them as a control, so the arithmetic's own cost can be read before any check is added.
 *
 * <p><b>Where the mask goes.</b> A {@code FAIL} node ORs the overflowing lanes into the batch's
 * condemning accumulator; a {@code NULL} node - {@code try_add} - clears them from its own
 * validity word instead. The masks are identical and their disposal is not, so {@code try_add} is
 * priced beside the checked add rather than assumed to match it. Neither is measured with lanes
 * that actually overflow: the columns stay far from the extremes, so both rows price the check
 * doing its work rather than short-circuiting a batch.
 *
 * <p><b>What the compile-time bound removes.</b> {@code year(d) * 100 + month(d)} is the shape
 * {@code VARKA-63.md} 6 is about. Its operands are bounded by the calendar, the compiler proves the
 * result cannot leave the int range, and every node in it is emitted {@code WRAP} - so under ANSI
 * this shape carries no check at all. The section prices that against the same expression with its
 * outer add checked, which is what the emitter would produce without the analysis. There is no
 * third arm with the multiply checked, because there is no such kernel: an int-lane multiply has
 * no cheap overflow test, the compiler declines a checked one outright, and without the bound the
 * whole expression would be residual rather than slower.
 *
 * <p>To run this benchmark:
 * <pre>
 *   1. build/sbt "catalyst/Test/runMain org.apache.spark.sql.VarkaArithmeticBenchmark"
 *   2. generate result:
 *        SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt
 *          "catalyst/Test/runMain org.apache.spark.sql.VarkaArithmeticBenchmark"
 *      Results will be written to
 *      "sql/catalyst/benchmarks/VarkaArithmeticBenchmark-jdk&lt;NN&gt;-results.txt".
 *   3. both widths, with provenance:
 *        dev/varka_bench_regen.sh catalyst VarkaArithmeticBenchmark
 * </pre>
 */
public class VarkaArithmeticBenchmark extends VarkaBenchmarkBase {

  private static final int NUM_ROWS = 1_000_000;

  /** The A/B's reference arm: the same IR with VARKA-63's overflow check switched off. */
  private static final VarkaEmitOptions UNCHECKED_ARM =
      VarkaEmitOptions.DEFAULTS.withCheckIntOverflow(false);

  private static final int[] NO_LITERALS = new int[0];

  /** A column: its data, its validity and its null count. */
  private record Column(MemorySegment data, MemorySegment validity, int nulls) { }

  // Each kernel is named by its case id n, claimed so that a reused one fails at once. A dry run
  // (-Dvarka.bench.dryRun=true, VarkaBenchmarkBase.dryRun) emits every case and claims its id but
  // times none, so the whole file's ids are checked in about as long as it takes to start a JVM
  // instead of the twenty minutes a full run takes to reach the case that collides. Picking a
  // free id by reading the file is unreliable: not every id here is a literal, so a grep answers
  // a question the dry run answers exactly. dev/varka_bench_ids.sh is its front end, and prints
  // the next free id.
  private VarkaFusedKernel emit(List<VarkaVectorIR> roots, int numInputs, int numLiterals,
      VarkaGeneratedClassLoader loader, int n) {
    return emit(roots, numInputs, numLiterals, loader, n, VarkaEmitOptions.DEFAULTS);
  }

  private VarkaFusedKernel emit(List<VarkaVectorIR> roots, int numInputs, int numLiterals,
      VarkaGeneratedClassLoader loader, int n, VarkaEmitOptions options) {
    claimCaseId(n);
    String name = "org.apache.spark.sql.varka.execution.VarkaArithBench" + n;
    return defineKernel(loader, name,
        VarkaLoopEmitter.emit(name, roots, numInputs, numLiterals, null, null, options));
  }

  /**
   * One int32 column, the same shape the parity benchmark fills: values within +-10000 of {@code
   * shift}, so they are dates when a calendar node reads them and ordinary integers when the
   * arithmetic does, and nowhere near the extremes either way. No case here can therefore
   * overflow, which is what makes the checked and unchecked arms comparable - both run every lane
   * to the end. {@code shift} is what makes a second column genuinely a second column: two calls
   * with {@code shift = 0} produce bit-identical data, which made every two-input case here a
   * disguised {@code datediff(x, x)} until this was added - a benchmark whose own column was a
   * copy of the first still measures real bytes and real throughput, but it could never have been
   * extended into a value check, since nothing it computed could ever be wrong.
   */
  private static Column fill(Arena arena, IntPredicate isNull, int shift) {
    MemorySegment data = arena.allocate(NUM_ROWS * 4L, 8);
    MemorySegment validity = arena.allocate((NUM_ROWS + 7) / 8L, 8);
    validity.fill((byte) 0);
    int nulls = 0;
    for (int i = 0; i < NUM_ROWS; i++) {
      data.set(ValueLayout.JAVA_INT, i * 4L, i % 20000 - 10000 + shift);
      if (isNull.test(i)) {
        nulls += 1;
      } else {
        long off = i / 8L;
        byte old = validity.get(ValueLayout.JAVA_BYTE, off);
        validity.set(ValueLayout.JAVA_BYTE, off, (byte) (old | (1 << (i % 8))));
      }
    }
    return new Column(data, validity, nulls);
  }

  /** The columns and destinations every case runs over. */
  private record Columns(Column nf, Column mx, Column nf2, Column mx2, MemorySegment dst,
      MemorySegment dstValidity, MemorySegment dst2, MemorySegment dst2Validity) {

    /** One input, null-free or mixed, with the literal values a case's slots expect. */
    void run1(VarkaFusedKernel kernel, boolean mixed, int[] literals) {
      int status = mixed
          ? kernel.run(new long[] {mx.data().address()}, new long[] {mx.validity().address()},
              new int[] {mx.nulls()}, new long[] {dst.address()},
              new long[] {dstValidity.address()}, literals, NUM_ROWS)
          : kernel.run(new long[] {nf.data().address()}, new long[] {0L}, new int[] {0},
              new long[] {dst.address()}, new long[] {dstValidity.address()}, literals,
              NUM_ROWS);
      // A declined batch would mean a lane overflowed, which no case here is built to do -
      // and the checked arm would then be measured short-circuiting rather than checking.
      if (status != 0) {
        throw requirementFailed("the kernel declined a batch: status " + status);
      }
    }

    /** One input, two outputs: the control that computes both calendar fields separately. */
    void runTwoOutputs(VarkaFusedKernel kernel) {
      int status = kernel.run(new long[] {nf.data().address()}, new long[] {0L}, new int[] {0},
          new long[] {dst.address(), dst2.address()},
          new long[] {dstValidity.address(), dst2Validity.address()}, NO_LITERALS, NUM_ROWS);
      if (status != 0) {
        throw requirementFailed("the kernel declined a batch: status " + status);
      }
    }

    /** Two inputs, for the {@code datediff} shape whose operands must be different columns. */
    void run2(VarkaFusedKernel kernel, boolean mixed, int[] literals) {
      int status = mixed
          ? kernel.run(new long[] {mx.data().address(), mx2.data().address()},
              new long[] {mx.validity().address(), mx2.validity().address()},
              new int[] {mx.nulls(), mx2.nulls()}, new long[] {dst.address()},
              new long[] {dstValidity.address()}, literals, NUM_ROWS)
          : kernel.run(new long[] {nf.data().address(), nf2.data().address()},
              new long[] {0L, 0L}, new int[] {0, 0}, new long[] {dst.address()},
              new long[] {dstValidity.address()}, literals, NUM_ROWS);
      if (status != 0) {
        throw requirementFailed("the kernel declined a batch: status " + status);
      }
    }
  }

  private static List<VarkaVectorIR> binary(IntOp op, Overflow mode, VarkaVectorIR col,
      VarkaVectorIR lit) {
    return List.of(new IntArith(op, mode, col, lit));
  }

  /**
   * year(d) * 100 + month(d) as the compiler emits it under ANSI: the calendar bounds both
   * operands, the product and the sum are proved to stay in range, and every node is WRAP. The
   * paired arm checks the outer add - the one check the analysis removes, and the only one that
   * could be emitted at all, since a checked multiply has no int-lane test and is declined
   * outright.
   */
  private static List<VarkaVectorIR> compositeKey(Overflow outer, VarkaVectorIR col,
      VarkaVectorIR lit) {
    return List.of(new IntArith(IntOp.ADD, outer,
        new IntArith(IntOp.MUL, Overflow.WRAP, new Year(col), lit), new Month(col)));
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    var loader = new VarkaGeneratedClassLoader(getClass().getClassLoader());
    try (Arena arena = Arena.ofConfined()) {
      Column nf = fill(arena, i -> false, 0);
      Column mx = fill(arena, i -> i % 7 == 0, 0);
      // Shifted, not repeated: the two-input cases below are `datediff`, and a second column
      // bit-identical to the first would price the emitted bytes correctly but make every
      // difference zero, which is not the shape a differential over this file would want.
      Column nf2 = fill(arena, i -> false, 137);
      Column mx2 = fill(arena, i -> i % 11 == 0, 137);
      var c = new Columns(nf, mx, nf2, mx2,
          arena.allocate(NUM_ROWS * 4L, 8), arena.allocate((NUM_ROWS + 7) / 8L, 8),
          arena.allocate(NUM_ROWS * 4L, 8), arena.allocate((NUM_ROWS + 7) / 8L, 8));

      VarkaVectorIR col = new ColumnRef(0);
      VarkaVectorIR col1 = new ColumnRef(1);
      VarkaVectorIR lit = new LiteralSlot(0);
      int[] one = {1};

      section("int arithmetic over a column, and what the ANSI check adds", () -> {
        var benchmark = benchmark("one output over " + NUM_ROWS + " rows", NUM_ROWS).timed()
            .build();
        var addChecked = emit(binary(IntOp.ADD, Overflow.FAIL, col, lit), 1, 1, loader, 960);
        var addUnchecked =
            emit(binary(IntOp.ADD, Overflow.FAIL, col, lit), 1, 1, loader, 961, UNCHECKED_ARM);
        var addWrap = emit(binary(IntOp.ADD, Overflow.WRAP, col, lit), 1, 1, loader, 962);
        var subChecked = emit(binary(IntOp.SUB, Overflow.FAIL, col, lit), 1, 1, loader, 963);
        var subUnchecked =
            emit(binary(IntOp.SUB, Overflow.FAIL, col, lit), 1, 1, loader, 964, UNCHECKED_ARM);
        var negChecked = emit(List.of(new IntNeg(Overflow.FAIL, col)), 1, 0, loader, 965);
        var negUnchecked =
            emit(List.of(new IntNeg(Overflow.FAIL, col)), 1, 0, loader, 966, UNCHECKED_ARM);

        benchmark.addCase("i + 1, ANSI, checked", i -> c.run1(addChecked, false, one));
        benchmark.addCase("i + 1, ANSI, check off", i -> c.run1(addUnchecked, false, one));
        benchmark.addCase("i + 1, LEGACY wrapping", i -> c.run1(addWrap, false, one));
        benchmark.addCase("i + 1, ANSI, checked, mixed nulls",
            i -> c.run1(addChecked, true, one));
        benchmark.addCase("i + 1, ANSI, check off, mixed nulls",
            i -> c.run1(addUnchecked, true, one));
        benchmark.addCase("i - 1, ANSI, checked", i -> c.run1(subChecked, false, one));
        benchmark.addCase("i - 1, ANSI, check off", i -> c.run1(subUnchecked, false, one));
        benchmark.addCase("-i, ANSI, checked", i -> c.run1(negChecked, false, NO_LITERALS));
        benchmark.addCase("-i, ANSI, check off", i -> c.run1(negUnchecked, false, NO_LITERALS));
        runCases(benchmark);
      });

      section("where the overflow mask goes: condemn the batch, or null the lane", () -> {
        var benchmark = benchmark("one output over " + NUM_ROWS + " rows", NUM_ROWS).timed()
            .build();
        var failAdd = emit(binary(IntOp.ADD, Overflow.FAIL, col, lit), 1, 1, loader, 970);
        var tryAdd = emit(binary(IntOp.ADD, Overflow.NULL, col, lit), 1, 1, loader, 971);
        var wrapAdd = emit(binary(IntOp.ADD, Overflow.WRAP, col, lit), 1, 1, loader, 972);

        // Mixed nulls only, for both checked arms. A NULL node nulls lanes whose inputs were
        // valid, so the dispatcher never hands it a dense batch, and its dense body would drop
        // the mask - a null-free row here would price a kernel production never runs.
        benchmark.addCase("i + 1, ANSI, checked (mask condemns the batch)",
            i -> c.run1(failAdd, true, one));
        benchmark.addCase("try_add(i, 1) (mask clears the lane's validity)",
            i -> c.run1(tryAdd, true, one));
        benchmark.addCase("i + 1, LEGACY wrapping (no mask)", i -> c.run1(wrapAdd, true, one));
        runCases(benchmark);
      });

      section("arithmetic over calendar fields, where the bound removes the check", () -> {
        var benchmark = benchmark("one output over " + NUM_ROWS + " rows", NUM_ROWS).timed()
            .build();
        int[] hundred = {100};

        var keyBounded = emit(compositeKey(Overflow.WRAP, col, lit), 1, 1, loader, 980);
        var keyCheckedAdd = emit(compositeKey(Overflow.FAIL, col, lit), 1, 1, loader, 981);
        // The fields alone, so the rows above can be read against what they are built on.
        var yearOnly = emit(List.of(new Year(col)), 1, 0, loader, 982);
        var bothFields = emit(List.of(new Year(col), new Month(col)), 1, 0, loader, 983);
        // The other bounded shape: datediff's own range bounds what the arithmetic extends.
        var diffWrap = emit(List.of(new IntArith(IntOp.ADD, Overflow.WRAP,
            new DateDiff(col, col1), lit)), 2, 1, loader, 984);
        var diffChecked = emit(List.of(new IntArith(IntOp.ADD, Overflow.FAIL,
            new DateDiff(col, col1), lit)), 2, 1, loader, 985);

        benchmark.addCase("year(d) * 100 + month(d), bound proved, no check",
            i -> c.run1(keyBounded, false, hundred));
        benchmark.addCase("year(d) * 100 + month(d), add checked",
            i -> c.run1(keyCheckedAdd, false, hundred));
        benchmark.addCase("year(d) * 100 + month(d), no check, mixed nulls",
            i -> c.run1(keyBounded, true, hundred));
        benchmark.addCase("year(d) alone", i -> c.run1(yearOnly, false, NO_LITERALS));
        benchmark.addCase("year(d), month(d), no arithmetic", i -> c.runTwoOutputs(bothFields));
        benchmark.addCase("datediff(d, d2) + 1, bound proved, no check",
            i -> c.run2(diffWrap, false, one));
        benchmark.addCase("datediff(d, d2) + 1, checked", i -> c.run2(diffChecked, false, one));
        runCases(benchmark);
      });
    } finally {
      loader.release();
    }
    reportCaseIds();
  }
}
