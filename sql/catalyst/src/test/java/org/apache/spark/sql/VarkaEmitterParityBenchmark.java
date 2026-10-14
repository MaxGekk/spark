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
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntPredicate;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;

import org.apache.spark.sql.catalyst.expressions.DateTimeExpressionUtils;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.TruncLevelLeaf;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitterTestSupport;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaFusedKernel;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaLoopEmitter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaMethodNames;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.AddDays;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.AddMonths;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Compare;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.CompareOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ConstDivide;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DateDiff;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfMonth;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfWeek;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfWeekIso;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfYear;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Greatest;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IfElse;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntArith;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Least;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.MakeDate;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Month;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.NextDay;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Quarter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.SubDays;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ThursdayOf;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.TruncDate;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.TruncDateDynamic;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.TruncLevel;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.WeekOfYear;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Year;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.WeekdayLeaf;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.DateTimeUtils$;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.varka.vector.ChronoScalarOps;
import org.apache.spark.sql.varka.vector.ChronoVectorOps;
import org.apache.spark.sql.varka.vector.DateVectorOps;
import org.apache.spark.sql.vectorized.ArrowColumnVector;

/**
 * The emitter's gates as a benchmark (see {@code sql/varka/plans/m2/VARKA-9.md},
 * {@code VARKA-10.md} and {@code VARKA-11.md}).
 *
 * <p>The parity gates: an emitted single-op loop must reach the hand-written kernel within noise
 * (acceptance: at least 0.9x its best-time throughput) - anything worse means C2 did not
 * intrinsify the emitted Vector API calls and the emitter is wrong. VARKA-9 measured
 * {@code date_add}; VARKA-10 adds the two-input {@code datediff}. The chain cases are the fusion
 * gate and the data behind {@code MAX_CHAIN_DEPTH}: a fused depth-N chain against N sequential
 * kernel passes over the same buffers. VARKA-10 adds the DAG cases: a subchain shared by two
 * outputs with CSE on, with the memo disabled (pricing CSE itself), and as sequential kernel
 * passes; and the widest shape the emitter accepts ({@code MAX_FUSED_NODES} ops), which must
 * scale with its op count rather than fall off a cliff at the cap. VARKA-11 adds the predication
 * cases - a CASE WHEN blend against the same-depth plain arithmetic - and the {@code dayofweek}
 * mod-7 comparison, whose shipped lowering is the two-fold magic multiply since the VARKA-14
 * follow-up, priced against the VARKA-11 digit sum, the lanewise-DIV reference and the per-row
 * {@code LocalDate} path.
 *
 * <p>VARKA-24 adds the batch-length alignment ladder, which is the only case here that exercises
 * the loop's remainder handling at all: every other case runs one call over the whole
 * lane-aligned buffer, so {@code loopBound == length} and the tail has never processed a row
 * under measurement. It drives the same total row count through aligned and unaligned chunks, at
 * two chunk sizes, so the tail's marginal cost and the per-call prologue can be read separately.
 *
 * <p>Every emitted kernel is named by its case id, claimed so that a reused one fails at once
 * ({@link #claimCaseId}). A dry run ({@code -Dvarka.bench.dryRun=true}) emits every case and
 * claims its id but times none, so the whole file's ids are checked in about as long as it takes
 * to start a JVM instead of the twenty minutes a full run takes to reach the case that collides.
 * Picking a free id by reading the file is unreliable: not every id here is a literal - the trunc
 * block computes {@code id} and {@code id + 1} from a list - so a grep answers a question the dry
 * run answers exactly. {@code dev/varka_bench_ids.sh} is its front end, and prints the next free
 * id. A dry run measures nothing and is never a substitute for a regeneration.
 *
 * <p>To run this benchmark:
 * <pre>
 *   1. build/sbt "catalyst/Test/runMain org.apache.spark.sql.VarkaEmitterParityBenchmark"
 *   2. generate result:
 *        SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt
 *          "catalyst/Test/runMain org.apache.spark.sql.VarkaEmitterParityBenchmark"
 *      Results will be written to
 *      "sql/catalyst/benchmarks/VarkaEmitterParityBenchmark-jdk&lt;NN&gt;-results.txt".
 *   3. both widths, with provenance:
 *        dev/varka_bench_regen.sh catalyst VarkaEmitterParityBenchmark
 * </pre>
 */
public class VarkaEmitterParityBenchmark extends VarkaBenchmarkBase {

  // The row count, the passes and the chunk are read at run time, as the Scala object's field
  // and local vals were, never folded by javac: a static final int would be a compile-time
  // constant, inlined into every timed loop as an immediate, and C2 would then compile each
  // walk and each per-row anchor with a constant trip count the Scala form never gave it.
  // Nothing assigns them after class initialisation.

  /** The rows of every column. */
  private static int numRows = 1_000_000;

  /** The passes over the whole buffer the chunked sections make per iteration. */
  private static int repeats = 20;

  /** The chunk the chunked sections drive a kernel in: Spark's COLUMN_BATCH_SIZE. */
  private static int chunk = 4096;

  private static final int[] NO_LITERALS = new int[0];

  /**
   * VARKA-70's reference arm, "words per group": the masked loop reads the input words and ORs
   * a served root's validity in per lane group, which is what shipped until VARKA-70's driver
   * pass took both over (VARKA-70.md 3.1). Not to be read as VARKA-45's "validity OR-ed per
   * group", which is the dense body's arm. Every VARKA-70 pair below is the shipped kernel
   * beside this one, adjacent, on the mixed-null arm the pass changes; the null-free arm is
   * VARKA-45's fill either way and is not paired.
   */
  private static final VarkaEmitOptions PER_GROUP_WRITE =
      VarkaEmitOptions.DEFAULTS.withValidityByBitmap(false);

  // ---------------------------------------------------------------------------------------
  // Data, kernels and the shapes they are built from
  // ---------------------------------------------------------------------------------------

  /** A filled column: its data, its validity and its null count. */
  private record Column(MemorySegment data, MemorySegment validity, int nulls) { }

  /** One chunk of a pass: the data offset, the validity offset and the row count. */
  @FunctionalInterface
  private interface ChunkBody {
    void accept(long dataOff, long validityOff, int n);
  }

  /** A scalar year kernel of {@code ChronoScalarOps}, over one chunk. */
  @FunctionalInterface
  private interface ScalarKernel {
    void run(long srcData, long dstData, long dstValidity, int length);
  }

  /** A section's own runner of a kernel over one null pattern. */
  @FunctionalInterface
  private interface KernelArm {
    void run(VarkaFusedKernel kernel, boolean mixed);
  }

  /** A section's own runner of a kernel at a chunk size or an output count. */
  @FunctionalInterface
  private interface KernelAt {
    void run(VarkaFusedKernel kernel, int n);
  }

  /**
   * A section's own runner of its one kernel over one null pattern.
   *
   * <p>The runners, which the Scala wrote as local functions, are lambdas through these
   * interfaces, methods of the year section's record or static methods. Bringing them to one
   * style would change how a timed body reaches its kernel, which this port keeps as it was
   * first written and measured against the Scala.
   */
  @FunctionalInterface
  private interface MixedArm {
    void run(boolean mixed);
  }

  /** A chunk size of the alignment ladders and what it leaves to the tail. */
  private record Rung(int chunk, String note) { }

  /** A trunc level of the VARKA-35 A/B and the first of its two case ids. */
  private record TruncLevelCase(String name, TruncLevel level, int id) { }

  /** The two VARKA-35 lowerings of one trunc level. */
  private record TruncArms(String name, VarkaFusedKernel subtract, VarkaFusedKernel recomposed) {
  }

  /** A setting of the VARKA-32 B2 ladder. */
  private record Setting(String label, VarkaEmitOptions options) { }

  /** A point of the VARKA-43 width ladder and the op count it must emit. */
  private record LadderPoint(int subtrees, int expectedOps) { }

  /** A shape of the VARKA-88 division A/B. */
  private record DivisionShape(String label, List<VarkaVectorIR> roots, int lits) { }

  /** The make_date inputs: the year, month and day of each of nfData's dates. */
  private record DateParts(MemorySegment year, MemorySegment month, MemorySegment day) { }

  private static final List<Rung> LADDER = List.of(new Rung(4096, "aligned"),
      new Rung(4095, "lanes-1 tail rows"), new Rung(64, "aligned"),
      new Rung(63, "lanes-1 tail rows"));

  private static VarkaVectorIR chain(int depth) {
    return chain(depth, 0);
  }

  private static VarkaVectorIR chain(int depth, int slotBase) {
    return chainOver(new ColumnRef(0), depth, slotBase);
  }

  private static VarkaVectorIR chainOver(VarkaVectorIR base, int depth, int slotBase) {
    VarkaVectorIR node = base;
    for (int level = 0; level < depth; level++) {
      node = level % 2 == 0 ? new AddDays(node, new LiteralSlot(slotBase + level))
          : new SubDays(node, new LiteralSlot(slotBase + level));
    }
    return node;
  }

  /** {@code level * step + 1} for every level below {@code n}: a chain's literal offsets. */
  private static int[] offsets(int n, int step) {
    int[] out = new int[n];
    for (int level = 0; level < n; level++) {
      out[level] = level * step + 1;
    }
    return out;
  }

  private VarkaFusedKernel emit(List<VarkaVectorIR> roots, int numInputs, int numLiterals,
      VarkaGeneratedClassLoader loader, int n) {
    return emit(roots, numInputs, numLiterals, loader, n, VarkaEmitOptions.DEFAULTS);
  }

  private VarkaFusedKernel emit(List<VarkaVectorIR> roots, int numInputs, int numLiterals,
      VarkaGeneratedClassLoader loader, int n, VarkaEmitOptions options) {
    // A duplicate id is otherwise a LinkageError from the class loader, twenty minutes into a
    // regeneration and pointing at the loader rather than at the two cases that chose the same
    // number.
    claimCaseId(n);
    String name = "org.apache.spark.sql.varka.execution.VarkaFusedBench" + n;
    return defineKernel(loader, name,
        VarkaLoopEmitter.emit(name, roots, numInputs, numLiterals, null, null, options));
  }

  private static Column fill(Arena arena, IntPredicate isNull) {
    MemorySegment data = arena.allocate(numRows * 4L, 8);
    MemorySegment validity = arena.allocate((numRows + 7) / 8L, 8);
    validity.fill((byte) 0);
    int nulls = 0;
    for (int i = 0; i < numRows; i++) {
      data.set(ValueLayout.JAVA_INT, i * 4L, i % 20000 - 10000);
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

  private static MemorySegment[] allocateAll(Arena arena, int count, long bytes) {
    MemorySegment[] out = new MemorySegment[count];
    for (int k = 0; k < count; k++) {
      out[k] = arena.allocate(bytes, 8);
    }
    return out;
  }

  private static long[] addresses(MemorySegment[] segments) {
    long[] out = new long[segments.length];
    for (int k = 0; k < segments.length; k++) {
      out[k] = segments[k].address();
    }
    return out;
  }

  // ---------------------------------------------------------------------------------------
  // Per-batch address arrays, built in the timed bodies as the Scala built them
  // ---------------------------------------------------------------------------------------

  /** Every address in {@code base} plus {@code off}: Scala's {@code base.map(_ + off)}. */
  private static long[] shifted(long[] base, long off) {
    long[] out = new long[base.length];
    for (int k = 0; k < base.length; k++) {
      out[k] = base[k] + off;
    }
    return out;
  }

  /**
   * The first {@code count} addresses of {@code base} plus {@code off}, as Scala's
   * {@code base.take(count).map(_ + off)}: the copy, then the shifted array.
   */
  private static long[] takeShifted(long[] base, int count, long off) {
    long[] taken = Arrays.copyOf(base, count);
    long[] out = new long[count];
    for (int k = 0; k < count; k++) {
      out[k] = taken[k] + off;
    }
    return out;
  }

  /** Every segment's address plus {@code off}: Scala's {@code segs.map(_.address() + off)}. */
  private static long[] addressesShifted(MemorySegment[] segments, long off) {
    long[] out = new long[segments.length];
    for (int k = 0; k < segments.length; k++) {
      out[k] = segments[k].address() + off;
    }
    return out;
  }

  /**
   * The first {@code count} segments' addresses plus {@code off}, as Scala's
   * {@code segs.take(count).map(_.address() + off)}: the copy, then the address array.
   */
  private static long[] takeAddressesShifted(MemorySegment[] segments, int count, long off) {
    MemorySegment[] taken = Arrays.copyOf(segments, count);
    long[] out = new long[count];
    for (int k = 0; k < count; k++) {
      out[k] = taken[k].address() + off;
    }
    return out;
  }

  // ---------------------------------------------------------------------------------------
  // The buffers every section runs over
  // ---------------------------------------------------------------------------------------

  /**
   * The filled columns and the destinations. {@code nf2Data} is filled exactly as
   * {@code nfData} is, so the two hold the same values.
   */
  private record Buffers(Arena arena, VarkaGeneratedClassLoader loader,
      MemorySegment nfData, MemorySegment mxData, MemorySegment mxValidity, int mxNulls,
      MemorySegment nf2Data, MemorySegment mx2Data, MemorySegment mx2Validity, int mx2Nulls,
      MemorySegment dst, MemorySegment dstValidity, MemorySegment dst2,
      MemorySegment dst2Validity, MemorySegment[] tmpData, MemorySegment[] tmpValidity,
      MemorySegment[] wideDst, MemorySegment[] wideDstValidity) {

    /** The chain as {@code depth} hand-written kernel passes, src to dst through the temps. */
    void sequentialChain(int depth, int[] offsets, long srcData, long srcValidity,
        int srcNulls, long dstData, long dstVal) {
      long srcD = srcData;
      long srcV = srcValidity;
      for (int level = 0; level < depth; level++) {
        long outD;
        long outV;
        if (level == depth - 1) {
          outD = dstData;
          outV = dstVal;
        } else {
          outD = tmpData[level].address();
          outV = tmpValidity[level].address();
        }
        if (level % 2 == 0) {
          DateVectorOps.vectorAddDays(srcD, srcV, srcNulls, outD, outV, numRows,
              offsets[level]);
        } else {
          DateVectorOps.vectorSubDays(srcD, srcV, srcNulls, outD, outV, numRows,
              offsets[level]);
        }
        srcD = outD;
        srcV = outV;
      }
    }
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    Arena arena = Arena.ofConfined();
    var loader = new VarkaGeneratedClassLoader(getClass().getClassLoader());
    try {
      Column nf = fill(arena, i -> false);
      Column mx = fill(arena, i -> i % 7 == 0);
      Column nf2 = fill(arena, i -> false);
      Column mx2 = fill(arena, i -> i % 11 == 0);
      MemorySegment dst = arena.allocate(numRows * 4L, 8);
      MemorySegment dstValidity = arena.allocate((numRows + 7) / 8L, 8);
      MemorySegment dst2 = arena.allocate(numRows * 4L, 8);
      MemorySegment dst2Validity = arena.allocate((numRows + 7) / 8L, 8);
      // Sequential-pass temporaries, one pair per intermediate of the deepest chain, and the
      // per-output destinations of the widest shape.
      int maxDepth = 16;
      MemorySegment[] tmpData = allocateAll(arena, maxDepth, numRows * 4L);
      MemorySegment[] tmpValidity = allocateAll(arena, maxDepth, (numRows + 7) / 8L);
      MemorySegment[] wideDst = allocateAll(arena, 4, numRows * 4L);
      MemorySegment[] wideDstValidity = allocateAll(arena, 4, (numRows + 7) / 8L);
      var b = new Buffers(arena, loader, nf.data(), mx.data(), mx.validity(), mx.nulls(),
          nf2.data(), mx2.data(), mx2.validity(), mx2.nulls(), dst, dstValidity, dst2,
          dst2Validity, tmpData, tmpValidity, wideDst, wideDstValidity);

      section("single op: emitted loop vs hand-written kernel", () -> singleOp(b));
      section("datediff: emitted loop vs hand-written kernel", () -> dateDiff(b));
      section("fused chain vs sequential kernel passes", () -> fusedChain(b));
      section("shared subchain across two outputs (DAG-CSE)", () -> sharedSubchain(b));
      section("predication: CASE WHEN blend vs plain arithmetic (VARKA-11)",
          () -> predication(b));
      section("dayofweek: magic multiply vs digit sum vs DIV vs LocalDate (VARKA-11, VARKA-14)",
          () -> dayOfWeek(b));
      section("year: the calendar extractions against LocalDate (VARKA-26)", () -> year(b));
      section("GROUP_BUDGET: two outputs over one shared chain, split vs kept together",
          () -> groupBudget(b));
      section("VARKA-32 B2: the fused-method ladder past four outputs (VARKA-32.md 10.4)",
          () -> fusedMethodLadder(b));
      section("widest shape: MAX_FUSED_NODES ops in one kernel", () -> widestShape(b));
      section("next_day: the literal kernel, the column kernel, the derived leaf and "
          + "the row engine's own path (VARKA-59)", () -> nextDay(b));
      section("trunc with a format column: the dynamic kernel, the literal controls, the "
          + "level leaf and the row engine's own path (VARKA-61)", () -> truncFormat(b));
      section("batch-length alignment: what the scalar tail actually costs (VARKA-24)",
          () -> batchAlignment(b));
      section("VARKA-44: the epilogue's HugeMethodLimit crossing (VARKA-32.md 7.1)",
          () -> epilogueCrossing(b));
      section("VARKA-43: one output, widening - where a single loop method stops scaling",
          () -> widening(b));
      section("VARKA-88 step 4: the constant division, magic against the double lane",
          () -> constantDivision(b));
      section("VARKA-88 step 4: extract(YEAR FROM ym), the division with no magic form",
          () -> extractYear(b));
      section(EXACT_STATUS_SECTION, () -> exactRefusalStatus(b));
    } finally {
      loader.release();
      arena.close();
    }
    reportCaseIds();
  }

  // ---------------------------------------------------------------------------------------
  // The parity, fusion and predication gates
  // ---------------------------------------------------------------------------------------

  private void singleOp(Buffers b) {
    VarkaBenchmark benchmark =
        benchmark("date_add over " + numRows + " rows", numRows).timed().build();
    VarkaFusedKernel depth1 = emit(List.of(chain(1)), 1, 1, b.loader(), 0);
    benchmark.addCase("hand-written kernel, null-free", it ->
        DateVectorOps.vectorAddDays(b.nfData().address(), 0L, 0,
            b.dst().address(), b.dstValidity().address(), numRows, 3));
    benchmark.addCase("emitted loop, null-free", it ->
        depth1.run(new long[] {b.nfData().address()}, new long[] {0L}, new int[] {0},
            new long[] {b.dst().address()}, new long[] {b.dstValidity().address()},
            new int[] {3}, numRows));
    benchmark.addCase("hand-written kernel, mixed nulls", it ->
        DateVectorOps.vectorAddDays(b.mxData().address(), b.mxValidity().address(),
            b.mxNulls(), b.dst().address(), b.dstValidity().address(), numRows, 3));
    benchmark.addCase("emitted loop, mixed nulls", it ->
        depth1.run(new long[] {b.mxData().address()}, new long[] {b.mxValidity().address()},
            new int[] {b.mxNulls()}, new long[] {b.dst().address()},
            new long[] {b.dstValidity().address()}, new int[] {3}, numRows));
    runCases(benchmark);
  }

  private void dateDiff(Buffers b) {
    VarkaBenchmark benchmark =
        benchmark("datediff over " + numRows + " rows", numRows).timed().build();
    VarkaFusedKernel diff = emit(List.of(new DateDiff(new ColumnRef(0), new ColumnRef(1))), 2, 0,
        b.loader(), 100);
    benchmark.addCase("hand-written kernel, null-free", it ->
        DateVectorOps.vectorDateDiff(b.nfData().address(), 0L, 0, b.nf2Data().address(), 0L,
            0, b.dst().address(), b.dstValidity().address(), numRows));
    benchmark.addCase("emitted loop, null-free", it ->
        diff.run(new long[] {b.nfData().address(), b.nf2Data().address()},
            new long[] {0L, 0L}, new int[] {0, 0}, new long[] {b.dst().address()},
            new long[] {b.dstValidity().address()}, NO_LITERALS, numRows));
    benchmark.addCase("hand-written kernel, mixed nulls", it ->
        DateVectorOps.vectorDateDiff(b.mxData().address(), b.mxValidity().address(),
            b.mxNulls(), b.mx2Data().address(), b.mx2Validity().address(), b.mx2Nulls(),
            b.dst().address(), b.dstValidity().address(), numRows));
    benchmark.addCase("emitted loop, mixed nulls", it ->
        diff.run(new long[] {b.mxData().address(), b.mx2Data().address()},
            new long[] {b.mxValidity().address(), b.mx2Validity().address()},
            new int[] {b.mxNulls(), b.mx2Nulls()}, new long[] {b.dst().address()},
            new long[] {b.dstValidity().address()}, NO_LITERALS, numRows));
    // VARKA-70's baselines, committed before that task so its pass has numbers to move
    // (VARKA-70.md 6). The OR root is the one shape where the pass computes an OR
    // rather than a copy or an AND, and it had no committed row at all.
    VarkaFusedKernel greatest =
        emit(List.of(new Greatest(new ColumnRef(0), new ColumnRef(1))), 2, 0,
            b.loader(), 930);
    benchmark.addCase("greatest(d, d2), emitted loop, null-free", it ->
        greatest.run(new long[] {b.nfData().address(), b.nf2Data().address()},
            new long[] {0L, 0L}, new int[] {0, 0}, new long[] {b.dst().address()},
            new long[] {b.dstValidity().address()}, NO_LITERALS, numRows));
    benchmark.addCase("greatest(d, d2), emitted loop, mixed nulls", it ->
        greatest.run(new long[] {b.mxData().address(), b.mx2Data().address()},
            new long[] {b.mxValidity().address(), b.mx2Validity().address()},
            new int[] {b.mxNulls(), b.mx2Nulls()}, new long[] {b.dst().address()},
            new long[] {b.dstValidity().address()}, NO_LITERALS, numRows));
    // The third baseline: an input all-null - validity address 0L and a null count of every
    // row, the morsel contract's spelling - which is the operand 2.3 folds away without
    // dereferencing anything. It hangs off the OR root, not the AND one, on purpose. Under
    // an AND root `DateDiff` satisfies the masked driver's all-null shortcut, so the batch
    // returns straight after `zero(dstValidity)` and never reaches a loop: the row would
    // time a 125 KB memset, could not move whatever the pass does, and would publish a
    // meaningless Relative against the hand-written kernel above it. `Greatest` is
    // null-skipping, so the shortcut declines it and the loop runs - and the OR of an
    // all-null operand is exactly 2.3's degenerate case, where the pass copies the other
    // input's bitmap rather than reading a bitmap that was never materialised.
    benchmark.addCase("greatest(d, d2), emitted loop, first input all-null", it ->
        greatest.run(new long[] {b.mxData().address(), b.mx2Data().address()},
            new long[] {0L, b.mx2Validity().address()}, new int[] {numRows, b.mx2Nulls()},
            new long[] {b.dst().address()}, new long[] {b.dstValidity().address()},
            NO_LITERALS, numRows));
    // VARKA-70's A/B on the OR root: the pass writes the OR of two whole bitmaps once and
    // the loop drops the root's write - but keeps both reads, since the pick's null
    // substitution blends by the operand words for the value (VARKA-70.md 3.3).
    VarkaFusedKernel greatestPerGroup =
        emit(List.of(new Greatest(new ColumnRef(0), new ColumnRef(1))), 2,
            0, b.loader(), 945, PER_GROUP_WRITE);
    benchmark.addCase("greatest(d, d2), words per group (VARKA-70 A/B), mixed nulls", it ->
        greatestPerGroup.run(new long[] {b.mxData().address(), b.mx2Data().address()},
            new long[] {b.mxValidity().address(), b.mx2Validity().address()},
            new int[] {b.mxNulls(), b.mx2Nulls()}, new long[] {b.dst().address()},
            new long[] {b.dstValidity().address()}, NO_LITERALS, numRows));
    benchmark.addCase(
        "greatest(d, d2), words per group (VARKA-70 A/B), first input all-null", it ->
        greatestPerGroup.run(new long[] {b.mxData().address(), b.mx2Data().address()},
            new long[] {0L, b.mx2Validity().address()}, new int[] {numRows, b.mx2Nulls()},
            new long[] {b.dst().address()}, new long[] {b.dstValidity().address()},
            NO_LITERALS, numRows));
    runCases(benchmark);
  }

  private void fusedChain(Buffers b) {
    VarkaBenchmark benchmark = benchmark("chain over " + numRows + " rows, mixed nulls", numRows)
        .timed().build();
    for (int depth : List.of(1, 2, 4, 8, 16)) {
      int[] offsets = offsets(depth, 13);
      VarkaFusedKernel fused = emit(List.of(chain(depth)), 1, depth, b.loader(), depth);
      benchmark.addCase("fused, depth " + depth, it ->
          fused.run(new long[] {b.mxData().address()}, new long[] {b.mxValidity().address()},
              new int[] {b.mxNulls()}, new long[] {b.dst().address()},
              new long[] {b.dstValidity().address()}, offsets, numRows));
      benchmark.addCase("sequential kernels, depth " + depth, it ->
          b.sequentialChain(depth, offsets, b.mxData().address(), b.mxValidity().address(),
              b.mxNulls(), b.dst().address(), b.dstValidity().address()));
    }
    runCases(benchmark);
  }

  private void sharedSubchain(Buffers b) {
    // a = chain8(d); b = datediff(chain8(d), d2): 9 distinct ops. With the memo disabled
    // the same trees emit 17 op calls - the delta prices CSE itself, separately from
    // fusion. The sequential oracle is 9 kernel passes.
    VarkaBenchmark benchmark =
        benchmark("two outputs over " + numRows + " rows, mixed nulls", numRows)
            .timed().build();
    VarkaVectorIR shared = chain(8);
    List<VarkaVectorIR> roots = List.of(shared, new DateDiff(shared, new ColumnRef(1)));
    int[] offsets = offsets(8, 13);
    VarkaFusedKernel fused = emit(roots, 2, 8, b.loader(), 200);
    VarkaFusedKernel fusedNoCse =
        emit(roots, 2, 8, b.loader(), 201, VarkaEmitOptions.DEFAULTS.withCse(false));
    Consumer<VarkaFusedKernel> runFused = kernel ->
        kernel.run(new long[] {b.mxData().address(), b.mx2Data().address()},
            new long[] {b.mxValidity().address(), b.mx2Validity().address()},
            new int[] {b.mxNulls(), b.mx2Nulls()},
            new long[] {b.dst().address(), b.dst2().address()},
            new long[] {b.dstValidity().address(), b.dst2Validity().address()}, offsets,
            numRows);
    benchmark.addCase("fused, CSE", it -> runFused.accept(fused));
    benchmark.addCase("fused, memo disabled", it -> runFused.accept(fusedNoCse));
    benchmark.addCase("sequential kernels (9 passes)", it -> {
      b.sequentialChain(8, offsets, b.mxData().address(), b.mxValidity().address(),
          b.mxNulls(), b.dst().address(), b.dstValidity().address());
      DateVectorOps.vectorDateDiff(b.dst().address(), b.dstValidity().address(), b.mxNulls(),
          b.mx2Data().address(), b.mx2Validity().address(), b.mx2Nulls(),
          b.dst2().address(), b.dst2Validity().address(), numRows);
    });
    runCases(benchmark);
  }

  /** The predication section's one call shape: two inputs, one output. */
  private static void runPredicated(Buffers b, int[] offsets, VarkaFusedKernel kernel, long d1,
      long v1, int n1, long d2v, long v2, int n2) {
    kernel.run(new long[] {d1, d2v}, new long[] {v1, v2}, new int[] {n1, n2},
        new long[] {b.dst().address()}, new long[] {b.dstValidity().address()}, offsets,
        numRows);
  }

  private void predication(Buffers b) {
    // The same depth-4 arithmetic with and without a comparison + blend wrapped around
    // it - pricing predication itself, in both bodies. The arms are disjoint chains over
    // each input so the predicated case does strictly more work.
    VarkaBenchmark benchmark = benchmark("depth-4 arms over " + numRows + " rows", numRows).timed()
        .build();
    int[] offsets = offsets(8, 13);
    VarkaFusedKernel plain =
        emit(List.of(chainOver(new ColumnRef(0), 4, 0)), 2, 8, b.loader(), 400);
    VarkaFusedKernel blended = emit(List.of(new IfElse(
        new Compare(CompareOp.LT, new ColumnRef(0), new ColumnRef(1)),
        chainOver(new ColumnRef(0), 4, 0),
        chainOver(new ColumnRef(1), 4, 4))), 2, 8, b.loader(), 401);
    benchmark.addCase("arithmetic depth 4, null-free", it ->
        runPredicated(b, offsets, plain, b.nfData().address(), 0L, 0, b.nf2Data().address(),
            0L, 0));
    benchmark.addCase("CASE WHEN, depth-4 arms, null-free", it ->
        runPredicated(b, offsets, blended, b.nfData().address(), 0L, 0,
            b.nf2Data().address(), 0L, 0));
    benchmark.addCase("arithmetic depth 4, mixed nulls", it ->
        runPredicated(b, offsets, plain, b.mxData().address(), b.mxValidity().address(),
            b.mxNulls(), b.mx2Data().address(), b.mx2Validity().address(), b.mx2Nulls()));
    benchmark.addCase("CASE WHEN, depth-4 arms, mixed nulls", it ->
        runPredicated(b, offsets, blended, b.mxData().address(), b.mxValidity().address(),
            b.mxNulls(), b.mx2Data().address(), b.mx2Validity().address(), b.mx2Nulls()));
    runCases(benchmark);
  }

  private void dayOfWeek(Buffers b) {
    VarkaBenchmark benchmark = benchmark("dayofweek over " + numRows + " rows", numRows).timed()
        .build();
    VarkaFusedKernel dow = emit(List.of(new DayOfWeek(new ColumnRef(0))), 1, 0, b.loader(), 500);
    VarkaFusedKernel dowDiv = emit(List.of(new DayOfWeek(new ColumnRef(0))), 1, 0, b.loader(), 501,
        VarkaEmitOptions.DEFAULTS.withFloorMod7(VarkaEmitOptions.FloorMod7.DIV));
    VarkaFusedKernel dowDigitSum =
        emit(List.of(new DayOfWeek(new ColumnRef(0))), 1, 0, b.loader(), 502,
            VarkaEmitOptions.DEFAULTS.withFloorMod7(VarkaEmitOptions.FloorMod7.DIGIT_SUM));
    // VARKA-57: extract(DAYOFWEEK_ISO), dayofweek's tail with the other offset and the same
    // add; priced beside the shipped dayofweek row, which it should match within noise.
    VarkaFusedKernel dowIso =
        emit(List.of(new DayOfWeekIso(new ColumnRef(0))), 1, 0, b.loader(), 507);
    benchmark.addCase("dayofweek_iso (VARKA-57), null-free", it ->
        dowIso.run(new long[] {b.nfData().address()}, new long[] {0L}, new int[] {0},
            new long[] {b.dst().address()}, new long[] {b.dstValidity().address()},
            NO_LITERALS, numRows));
    benchmark.addCase("magic multiply (shipped), null-free", it ->
        dow.run(new long[] {b.nfData().address()}, new long[] {0L}, new int[] {0},
            new long[] {b.dst().address()}, new long[] {b.dstValidity().address()},
            NO_LITERALS, numRows));
    benchmark.addCase("magic multiply (shipped), mixed nulls", it ->
        dow.run(new long[] {b.mxData().address()}, new long[] {b.mxValidity().address()},
            new int[] {b.mxNulls()}, new long[] {b.dst().address()},
            new long[] {b.dstValidity().address()}, NO_LITERALS, numRows));
    benchmark.addCase("digit sum (VARKA-11 reference), null-free", it ->
        dowDigitSum.run(new long[] {b.nfData().address()}, new long[] {0L}, new int[] {0},
            new long[] {b.dst().address()}, new long[] {b.dstValidity().address()},
            NO_LITERALS, numRows));
    benchmark.addCase("digit sum (VARKA-11 reference), mixed nulls", it ->
        dowDigitSum.run(new long[] {b.mxData().address()},
            new long[] {b.mxValidity().address()}, new int[] {b.mxNulls()},
            new long[] {b.dst().address()}, new long[] {b.dstValidity().address()},
            NO_LITERALS, numRows));
    benchmark.addCase("lanewise DIV, null-free", it ->
        dowDiv.run(new long[] {b.nfData().address()}, new long[] {0L}, new int[] {0},
            new long[] {b.dst().address()}, new long[] {b.dstValidity().address()},
            NO_LITERALS, numRows));
    benchmark.addCase("per-row LocalDate (the path Spark uses today)", it -> {
      int i = 0;
      while (i < numRows) {
        int days = b.nfData().get(ValueLayout.JAVA_INT, i * 4L);
        b.dst().set(ValueLayout.JAVA_INT, i * 4L,
            LocalDate.ofEpochDay(days).getDayOfWeek().plus(1).getValue());
        i += 1;
      }
    });
    runCases(benchmark);
  }

  // ---------------------------------------------------------------------------------------
  // The calendar extractions, in 4096-row chunks
  // ---------------------------------------------------------------------------------------

  /** {@code repeats} passes over the whole buffer in {@code chunk}-row chunks. */
  private static void eachChunk(int repeats, int chunk, ChunkBody body) {
    int pass = 0;
    while (pass < repeats) {
      int done = 0;
      while (done < numRows) {
        int n = Math.min(chunk, numRows - done);
        body.accept(done * 4L, done / 8L, n);
        done += n;
      }
      pass += 1;
    }
  }

  /**
   * The null count a chunked case hands the kernel for an n-row chunk of the first mixed
   * column, which fill() nulls every seventh row. A chunk read from offset 0, as the alignment
   * ladder reads every chunk, holds exactly this many. A chunk that walks the buffer starts at
   * a multiple of 4096, which is not one of 7, so its real count can be one fewer. That does not
   * matter: the kernel reads the count only to choose its body - the dense one at 0, the
   * all-null shortcut at n, and the masked one, which reads the validity bitmap itself,
   * anywhere between ({@code VarkaFusedKernel}) - and a chunk of thousands of rows is between
   * whichever count it is given. What must not be passed is the whole buffer's count, a count
   * for a batch the kernel is not running.
   */
  private static int nullsIn(int n) {
    return (n + 6) / 7;
  }

  /**
   * The same for the second mixed column, which fill() nulls every eleventh row; exact from
   * offset 0, possibly one more than the real count for a chunk that walks the buffer, and
   * either way strictly between 0 and n, so the kernel's choice of body is the same.
   */
  private static int nulls2In(int n) {
    return (n + 10) / 11;
  }

  /** The year section's runners over its 4096-row chunks. */
  private record YearRunners(Buffers b, long[] dstData4, long[] dstValidity4) {

    /**
     * The chunk walk itself, shared by every case in this section: {@code repeats} passes over
     * the whole buffer in 4096-row chunks, handing the body each chunk's source data offset,
     * validity offset and row count. Written once because two cases measured against each
     * other must not differ in their addressing - VARKA-32's first pass hand-copied this loop
     * for its own case, which is exactly the drift this prevents.
     */
    void eachChunk(ChunkBody body) {
      VarkaEmitterParityBenchmark.eachChunk(repeats, chunk, body);
    }

    void chunked(VarkaFusedKernel kernel, boolean mixed) {
      chunked(kernel, mixed, 1, NO_LITERALS);
    }

    void chunked(VarkaFusedKernel kernel, boolean mixed, int outputs) {
      chunked(kernel, mixed, outputs, NO_LITERALS);
    }

    void chunked(VarkaFusedKernel kernel, boolean mixed, int outputs, int[] lits) {
      eachChunk((dataOff, validityOff, n) -> {
        long[] dstData = outputs == 1 ? new long[] {b.dst().address() + dataOff}
            : takeShifted(dstData4, outputs, dataOff);
        long[] dstValid = outputs == 1 ? new long[] {b.dstValidity().address() + validityOff}
            : takeShifted(dstValidity4, outputs, validityOff);
        // A declined batch does the same vector work and reports the same time, so a
        // discarded status would let this file commit a rate production never sees -
        // it pays the kernel and then the whole row path. Same reason checkMatrix
        // asserts it.
        int status;
        if (mixed) {
          status = kernel.run(new long[] {b.mxData().address() + dataOff},
              new long[] {b.mxValidity().address() + validityOff},
              new int[] {nullsIn(n)}, dstData, dstValid, lits, n);
        } else {
          status = kernel.run(new long[] {b.nfData().address() + dataOff}, new long[] {0L},
              new int[] {0}, dstData, dstValid, lits, n);
        }
        if (status != 0) {
          throw requirementFailed("the kernel declined a batch: status " + status);
        }
      });
    }

    /**
     * add_months is the widest calendar node the emitter has - it decomposes and recomposes in
     * one loop method. One scalar arg, the month count, so this cannot share {@code chunked}.
     */
    void chunkedAddMonths(VarkaFusedKernel kernel, boolean mixed) {
      eachChunk((dataOff, validityOff, n) -> {
        int status;
        if (mixed) {
          status = kernel.run(new long[] {b.mxData().address() + dataOff},
              new long[] {b.mxValidity().address() + validityOff}, new int[] {nullsIn(n)},
              new long[] {b.dst().address() + dataOff},
              new long[] {b.dstValidity().address() + validityOff}, new int[] {13}, n);
        } else {
          status = kernel.run(new long[] {b.nfData().address() + dataOff}, new long[] {0L},
              new int[] {0}, new long[] {b.dst().address() + dataOff},
              new long[] {b.dstValidity().address() + validityOff}, new int[] {13}, n);
        }
        if (status != 0) {
          throw requirementFailed("the kernel declined a batch: status " + status);
        }
      });
    }

    /**
     * A Cond root writes no data at all, so its data address is 0L by the interface contract -
     * the dst slot must not be materialized - and the day it compares against rides the
     * scalar-args array. Zero selects about half of {@code fill}'s values, which run from
     * -10000 to 9999, so the bitmap carries mixed bits rather than a constant.
     */
    void chunkedFilter(VarkaFusedKernel kernel, boolean mixed) {
      eachChunk((dataOff, validityOff, n) -> {
        int status;
        if (mixed) {
          status = kernel.run(new long[] {b.mxData().address() + dataOff},
              new long[] {b.mxValidity().address() + validityOff}, new int[] {nullsIn(n)},
              new long[] {0L}, new long[] {b.dstValidity().address() + validityOff},
              new int[] {0}, n);
        } else {
          status = kernel.run(new long[] {b.nfData().address() + dataOff}, new long[] {0L},
              new int[] {0}, new long[] {0L},
              new long[] {b.dstValidity().address() + validityOff}, new int[] {0}, n);
        }
        if (status != 0) {
          throw requirementFailed("the kernel declined a batch: status " + status);
        }
      });
    }

    void chunkedTwo(VarkaFusedKernel kernel, boolean mixed) {
      chunkedTwo(kernel, mixed, NO_LITERALS);
    }

    /**
     * {@code lits} is empty for every two-column shape here; VARKA-60's literal-count control is
     * the one case that needs a slot, and it runs on this same runner so that the control and
     * the shape it controls for differ in the kernel alone.
     */
    void chunkedTwo(VarkaFusedKernel kernel, boolean mixed, int[] lits) {
      eachChunk((dataOff, validityOff, n) -> {
        int status;
        if (mixed) {
          status = kernel.run(
              new long[] {b.mxData().address() + dataOff, b.mx2Data().address() + dataOff},
              new long[] {b.mxValidity().address() + validityOff,
                  b.mx2Validity().address() + validityOff},
              new int[] {nullsIn(n), nulls2In(n)},
              new long[] {b.dst().address() + dataOff},
              new long[] {b.dstValidity().address() + validityOff}, lits, n);
        } else {
          status = kernel.run(
              new long[] {b.nfData().address() + dataOff, b.nf2Data().address() + dataOff},
              new long[] {0L, 0L}, new int[] {0, 0},
              new long[] {b.dst().address() + dataOff},
              new long[] {b.dstValidity().address() + validityOff}, lits, n);
        }
        if (status != 0) {
          throw requirementFailed("the kernel declined a batch: status " + status);
        }
      });
    }

    void chunkedThree(VarkaFusedKernel kernel, boolean mixed, DateParts parts) {
      eachChunk((dataOff, validityOff, n) -> {
        int status;
        if (mixed) {
          status = kernel.run(
              new long[] {parts.year().address() + dataOff, parts.month().address() + dataOff,
                  parts.day().address() + dataOff},
              new long[] {b.mxValidity().address() + validityOff, 0L, 0L},
              new int[] {nullsIn(n), 0, 0},
              new long[] {b.dst().address() + dataOff},
              new long[] {b.dstValidity().address() + validityOff}, NO_LITERALS, n);
        } else {
          status = kernel.run(
              new long[] {parts.year().address() + dataOff, parts.month().address() + dataOff,
                  parts.day().address() + dataOff},
              new long[] {0L, 0L, 0L}, new int[] {0, 0, 0},
              new long[] {b.dst().address() + dataOff},
              new long[] {b.dstValidity().address() + validityOff}, NO_LITERALS, n);
        }
        if (status != 0) {
          throw requirementFailed("the kernel declined a batch: status " + status);
        }
      });
    }

    /**
     * The scalar baseline's chunk walk: the same passes and chunks over the same buffer as
     * {@link #eachChunk}, handing the scalar kernel each chunk's absolute source, destination
     * and validity addresses. It is a second walk because the Scala form's was one, a loop of
     * its own around a four-argument function rather than a body handed to {@code eachChunk};
     * routing it through {@code eachChunk} would add a call layer to these two timed cases that
     * the Scala did not have. Its addressing is {@code eachChunk}'s: chunk k starts at row
     * {@code k * chunk} of every buffer.
     */
    void scalarChunked(ScalarKernel kernel) {
      int pass = 0;
      while (pass < repeats) {
        int done = 0;
        while (done < numRows) {
          int n = Math.min(chunk, numRows - done);
          kernel.run(b.nfData().address() + done * 4L, b.dst().address() + done * 4L,
              b.dstValidity().address() + done / 8L, n);
          done += n;
        }
        pass += 1;
      }
    }
  }

  private void year(Buffers b) {
    // The emitted civil-from-days decomposition against the scalar path Spark runs today.
    // A second lowering, which split the dividend to cover the whole int day range without
    // a guard, was measured here before being dropped for costing 14-24%; see
    // VARKA-26.md section 11.2 for that comparison.
    //
    // Driven in 4096-row chunks - Spark's COLUMN_BATCH_SIZE - rather than one
    // million-row call, so the per-call prologue is paid at the rate production pays it,
    // and walking the buffer rather than a warm prefix so the kernel and the scalar
    // anchor below are measured in the same memory regime. The multi-field cases are here
    // because VARKA-26 gave calendar nodes a weight that kept each in its own loop method
    // and VARKA-32 step B2 lets siblings over one date share one; the separate/shared pairs
    // below are where that is measured rather than assumed.
    VarkaBenchmark benchmark = benchmark((long) numRows * repeats + " rows in 4096-row chunks",
        (long) numRows * repeats).timed().build();
    // Four outputs need four destinations; one shared buffer would have the kernels
    // overwrite each other and alias four stores onto one cache line. wideDst/wideDstValidity
    // are the same four buffers the "widest shape" section below uses - VARKA-32's own cases
    // reuse them rather than allocating a second set, since nothing in either section holds
    // a result across sections.
    long[] dstData4 = addresses(b.wideDst());
    long[] dstValidity4 = addresses(b.wideDstValidity());
    // Each chunk advances the source and destination addresses, so a pass walks the whole
    // buffer rather than re-reading a cache-warm prefix 245 times. That matters here and
    // not in the task-24 ladder below: this section's scalar anchor walks all one million
    // rows, so a cache-resident kernel measured against a streaming scalar loop would put
    // the two sides of the headline ratio in different regimes. Measured: the same
    // dayofweek kernel reads 7746.1 M rows/s over the whole buffer against 8058.8 over a
    // warm prefix, so the regime is worth about 4% here - small, but it is the difference
    // between a measured ratio and a nearly-measured one.
    var r = new YearRunners(b, dstData4, dstValidity4);
    VarkaGeneratedClassLoader loader = b.loader();
    VarkaFusedKernel year = emit(List.of(new Year(new ColumnRef(0))), 1, 0, loader, 801);
    List<VarkaVectorIR> fourFields = List.of(new Year(new ColumnRef(0)),
        new Month(new ColumnRef(0)), new DayOfMonth(new ColumnRef(0)),
        new Quarter(new ColumnRef(0)));
    VarkaFusedKernel four = emit(fourFields, 1, 0, loader, 803);
    VarkaFusedKernel dow = emit(List.of(new DayOfWeek(new ColumnRef(0))), 1, 0, loader, 804);
    // VARKA-45's A/B, on shapes that already exist rather than new ones: the point is what
    // the dense validity fill does to kernels that ship today. Each pair is adjacent, so
    // both sides run back to back under one JIT and thermal state, and the mixed-null rows
    // are the control - the masked bodies are asserted byte for byte identical under the
    // option, so movement there is run noise rather than an effect.
    VarkaEmitOptions perGroup = VarkaEmitOptions.DEFAULTS.withDenseValidityOnce(false);
    VarkaFusedKernel yearPerGroup =
        emit(List.of(new Year(new ColumnRef(0))), 1, 0, loader, 830, perGroup);
    VarkaFusedKernel yearWordsPerGroup =
        emit(List.of(new Year(new ColumnRef(0))), 1, 0, loader, 940, PER_GROUP_WRITE);
    VarkaFusedKernel dowPerGroup =
        emit(List.of(new DayOfWeek(new ColumnRef(0))), 1, 0, loader, 832, perGroup);
    // VARKA-48's A/B. The shipped year kernel skips the prefix's March-month step - four
    // lane ops a year tail never reads, since it takes the January turn off the day of
    // year instead (VARKA-48.md section 2) - and this is the same kernel with the
    // step put back. The two are adjacent, and the pairs interleaved by null pattern,
    // because that is the interleaving: one regeneration runs both sides back to back
    // under the same JIT and thermal state, and the file is compared by minimums across
    // regenerations rather than row against row. Four ops on a 43-op body is under half a
    // millisecond at this section's 11 ms best time, so "inside noise" is a legitimate
    // outcome here and the default does not rest on this number - it rests on the step
    // being provably dead work where it is elided.
    VarkaFusedKernel yearMonthKept = emit(List.of(new Year(new ColumnRef(0))), 1, 0, loader, 802,
        VarkaEmitOptions.DEFAULTS.withElideChronoMonth(false));
    // add_months is the widest calendar node the emitter has - it decomposes and
    // recomposes in one loop method - and until now it had no committed number at all,
    // which is why the leap flag's cost was argued rather than measured. One scalar arg,
    // the month count, so this cannot share `chunked`.
    // VARKA-53's A/B, built the same way and for the same reason: adjacent cases, both
    // null patterns, so the two axes are measured back to back under one JIT and thermal
    // state. `dayofmonth` is the one with the largest op-count win (-4 of 43, the tail
    // stops running emitMonthStart forwards entirely) and `month` the smallest (-2 of 40),
    // so the pair brackets what the numerator can be worth; the four-field shape is here
    // because it pays the block once and the tails three times, which is where a win
    // should compound if it is real. `year` is deliberately absent: it reads neither axis,
    // and VARKA-53.md 6.1 prediction 3 is that it does not move - a case that cannot
    // move is a case that only adds runtime to this section.
    VarkaFusedKernel monthNeri = emit(List.of(new Month(new ColumnRef(0))), 1, 0, loader, 820);
    VarkaFusedKernel monthOld = emit(List.of(new Month(new ColumnRef(0))), 1, 0, loader, 821,
        VarkaEmitOptions.DEFAULTS.withNeriSchneiderMonth(false));
    VarkaFusedKernel domNeri = emit(List.of(new DayOfMonth(new ColumnRef(0))), 1, 0, loader, 822);
    VarkaFusedKernel domOld = emit(List.of(new DayOfMonth(new ColumnRef(0))), 1, 0, loader, 823,
        VarkaEmitOptions.DEFAULTS.withNeriSchneiderMonth(false));
    VarkaFusedKernel fourOld = emit(fourFields, 1, 0, loader, 824,
        VarkaEmitOptions.DEFAULTS.withNeriSchneiderMonth(false));
    VarkaFusedKernel addMonths = emit(List.of(new AddMonths(new ColumnRef(0), new LiteralSlot(0))),
        1, 1, loader, 811);
    // VARKA-54's A/B, both prefix forms named explicitly rather than one of them as "the
    // default", so the labels survive the default changing. `year` is the shape that pays
    // the prefix and nothing else, so it brackets the top of what the map is worth (five
    // ops off a body of about forty, one carry stage off the dependent chain); the
    // four-field shared shape pays the prefix once and the tails thrice, so it brackets the
    // bottom; `add_months` is the widest node and the one whose year assembly runs inside a
    // recomposition. Adjacent cases, both null patterns, the same interleaving discipline
    // as VARKA-45, VARKA-48 and VARKA-53.
    VarkaEmitOptions julian = VarkaEmitOptions.DEFAULTS.withJulianMap(true);
    VarkaEmitOptions centuryYear = VarkaEmitOptions.DEFAULTS.withJulianMap(false);
    VarkaFusedKernel yearJulian =
        emit(List.of(new Year(new ColumnRef(0))), 1, 0, loader, 840, julian);
    VarkaFusedKernel yearCenturyYear =
        emit(List.of(new Year(new ColumnRef(0))), 1, 0, loader, 841, centuryYear);
    VarkaFusedKernel fourJulian = emit(fourFields, 1, 0, loader, 842, julian);
    VarkaFusedKernel fourCenturyYear = emit(fourFields, 1, 0, loader, 843, centuryYear);
    VarkaFusedKernel addMonthsJulian =
        emit(List.of(new AddMonths(new ColumnRef(0), new LiteralSlot(0))),
            1, 1, loader, 844, julian);
    VarkaFusedKernel addMonthsCenturyYear = emit(
        List.of(new AddMonths(new ColumnRef(0), new LiteralSlot(0))), 1, 1, loader, 845,
        centuryYear);
    // VARKA-46's A/B: the same kernels emitted against the general validity helpers - the
    // pair that takes the lane count as an argument and carries a four-arm switch on it -
    // rather than the sibling named for the emitted width. Measured through VARKA-45's
    // A/B, one refused `orValidityBitsAt` costs 1.87 to 3.24 ns per lane group at either
    // width, so the shapes here are the ones that still make the call: a masked
    // projection, the shared four-field masked shape with four writes and one read per
    // group, and the selection kernel below, which makes it in both bodies. The
    // null-free-with-per-group-OR pair isolates the write with no masked machinery around
    // it, and is directly comparable to the VARKA-45 row beside it.
    // Both VARKA-46 arms ride VARKA-70's per-group reference variant. Under the shipped
    // default a served root makes no per-group validity call at all, so on `year`, the
    // four fields and `dayofweek` the width-named helper and the OR's position have
    // nothing left to change and each pair would time one kernel against itself. On the
    // reference arm the call is back and the pairs price what they are named for; their
    // comparand is the "words per group (VARKA-70 A/B)" row beside them, not the shipped
    // one. The dense (null-free) arms and the filter kernel are unaffected either way -
    // the pass rewrites only the masked body, and a `Cond` root is never served - so the
    // same options serve them unchanged.
    VarkaEmitOptions generalHelpers = PER_GROUP_WRITE.withValidityByWidth(false);
    VarkaFusedKernel yearGeneral =
        emit(List.of(new Year(new ColumnRef(0))), 1, 0, loader, 892, generalHelpers);
    VarkaFusedKernel yearPerGroupGeneral =
        emit(List.of(new Year(new ColumnRef(0))), 1, 0, loader, 893,
            generalHelpers.withDenseValidityOnce(false));
    VarkaFusedKernel dowGeneral = emit(List.of(new DayOfWeek(new ColumnRef(0))), 1, 0, loader, 882,
        generalHelpers);
    VarkaFusedKernel fourSharedGeneral = emit(fourFields, 1, 0, loader, 883, generalHelpers);
    // The selection kernel: a Cond root's slot holds a selection bitmap, which is computed
    // rather than known, so VARKA-45's driver fill cannot serve it and the per-group OR
    // stays in the dense body too. It is the shape the columnar filter runs on every batch
    // and the one shape with no committed parity case before VARKA-46.
    // The second half of VARKA-46: the same kernels with the validity OR emitted after the
    // store, which is where it was until the compiled loop showed it as a real call in every
    // arm. Both sides carry the width-named helpers, so this pair prices the order alone.
    VarkaEmitOptions orAfter = PER_GROUP_WRITE.withValidityOrFirst(false);
    VarkaFusedKernel yearOrAfter =
        emit(List.of(new Year(new ColumnRef(0))), 1, 0, loader, 887, orAfter);
    VarkaFusedKernel fourSharedOrAfter = emit(fourFields, 1, 0, loader, 888, orAfter);
    // VARKA-76's ladder in the number of per-group validity writes, which is the quantity
    // 2.38 proposes keying the helper choice on. The two shapes that raised the question -
    // single-field `year` at one write, the four shared fields at four - differ in method
    // size and op count as well as in writes, so a threshold fitted to them alone could be
    // fitted to the wrong quantity. These four rungs hold the shape family constant and
    // vary only the count: an `IfElse` blend is unserved by construction (its word is
    // computed per lane group, VARKA-70.md 2.1), so k blends over one date are k
    // writes in one loop method, and each rung adds the same 145 bytes and the same ops.
    // Both arms ride VARKA-70's per-group reference variant, as every VARKA-46 pair must.
    var blends = new ArrayList<List<VarkaVectorIR>>();
    for (int k = 1; k <= 4; k++) {
      var rung = new ArrayList<VarkaVectorIR>();
      for (int j = 0; j < k; j++) {
        rung.add(new IfElse(new Compare(CompareOp.LT, new ColumnRef(0), new LiteralSlot(j)),
            new AddDays(new ColumnRef(0), new LiteralSlot(j)),
            new SubDays(new ColumnRef(0), new LiteralSlot(j))));
      }
      blends.add(List.copyOf(rung));
    }
    int[] blendLits = {1, 2, 3, 4};
    var blendSpecialised = new ArrayList<VarkaFusedKernel>();
    for (int i = 0; i < blends.size(); i++) {
      blendSpecialised.add(emit(blends.get(i), 1, 4, loader, 962 + i, PER_GROUP_WRITE));
    }
    var blendGeneral = new ArrayList<VarkaFusedKernel>();
    for (int i = 0; i < blends.size(); i++) {
      blendGeneral.add(emit(blends.get(i), 1, 4, loader, 966 + i, generalHelpers));
    }
    // VARKA-47's arm over the same four rungs: the shapes are 76's, the question is not.
    // 76 asked which *helper* a per-group read-modify-write should call; this asks whether
    // the read-modify-write should happen at all. So the arms are `validityByWord` on and
    // off, with the width-named helpers on both sides, and the ids are a new block - 962
    // to 969 are 76's pair and reusing them would put two questions in one row.
    var blendByWord = new ArrayList<VarkaFusedKernel>();
    for (int i = 0; i < blends.size(); i++) {
      blendByWord.add(emit(blends.get(i), 1, 4, loader, 970 + i,
          PER_GROUP_WRITE.withValidityByWord(true)));
    }
    var selectionRoot = new Compare(CompareOp.LT, new ColumnRef(0), new LiteralSlot(0));
    VarkaFusedKernel filterKernel = emit(List.of(selectionRoot), 1, 1, loader, 884);
    VarkaFusedKernel filterGeneral =
        emit(List.of(selectionRoot), 1, 1, loader, 885, generalHelpers);
    benchmark.addCase("year, null-free", it -> r.chunked(year, false));
    benchmark.addCase("year, validity OR-ed per group (VARKA-45 A/B), null-free",
        it -> r.chunked(yearPerGroup, false));
    benchmark.addCase(
        "year, validity OR-ed per group, general helpers (VARKA-46 A/B), null-free",
        it -> r.chunked(yearPerGroupGeneral, false));
    benchmark.addCase("year, validity OR-ed per group (VARKA-45 A/B), mixed nulls",
        it -> r.chunked(yearPerGroup, true));
    benchmark.addCase("year, month step kept (VARKA-48 A/B), null-free",
        it -> r.chunked(yearMonthKept, false));
    benchmark.addCase("year, mixed nulls", it -> r.chunked(year, true));
    // VARKA-70's A/B: the one read and one write per group gone, the bitmap copied once.
    benchmark.addCase("year, words per group (VARKA-70 A/B), mixed nulls",
        it -> r.chunked(yearWordsPerGroup, true));
    benchmark.addCase(
        "year, per group + general validity helpers (VARKA-46 A/B), mixed nulls",
        it -> r.chunked(yearGeneral, true));
    benchmark.addCase(
        "year, per group + OR after the store (VARKA-46 A/B), mixed nulls",
        it -> r.chunked(yearOrAfter, true));
    benchmark.addCase("dayofweek, mixed nulls", it -> r.chunked(dow, true));
    benchmark.addCase(
        "dayofweek, per group + general validity helpers (VARKA-46 A/B), mixed nulls",
        it -> r.chunked(dowGeneral, true));
    // VARKA-76's rungs, adjacent so each pair shares a JIT and thermal state.
    for (int k = 1; k <= 4; k++) {
      int outputs = k;
      VarkaFusedKernel specialised = blendSpecialised.get(k - 1);
      VarkaFusedKernel general = blendGeneral.get(k - 1);
      VarkaFusedKernel byWord = blendByWord.get(k - 1);
      benchmark.addCase(
          k + " validity write(s) per body, width-named helpers (VARKA-76), mixed nulls",
          it -> r.chunked(specialised, true, outputs, blendLits));
      benchmark.addCase(
          k + " validity write(s) per body, general helpers (VARKA-76), mixed nulls",
          it -> r.chunked(general, true, outputs, blendLits));
      benchmark.addCase(
          k + " validity write(s) per body, one write per word (VARKA-47), mixed nulls",
          it -> r.chunked(byWord, true, outputs, blendLits));
    }
    benchmark.addCase("filter d < literal, null-free",
        it -> r.chunkedFilter(filterKernel, false));
    benchmark.addCase(
        "filter d < literal, general validity helpers (VARKA-46 A/B), null-free",
        it -> r.chunkedFilter(filterGeneral, false));
    benchmark.addCase("filter d < literal, mixed nulls",
        it -> r.chunkedFilter(filterKernel, true));
    benchmark.addCase(
        "filter d < literal, general validity helpers (VARKA-46 A/B), mixed nulls",
        it -> r.chunkedFilter(filterGeneral, true));
    benchmark.addCase("year, month step kept (VARKA-48 A/B), mixed nulls",
        it -> r.chunked(yearMonthKept, true));
    benchmark.addCase("add_months(d, 13), null-free",
        it -> r.chunkedAddMonths(addMonths, false));
    benchmark.addCase("add_months(d, 13), mixed nulls",
        it -> r.chunkedAddMonths(addMonths, true));
    benchmark.addCase("year, Julian map (VARKA-54 A/B), null-free",
        it -> r.chunked(yearJulian, false));
    benchmark.addCase("year, century-then-year (VARKA-54 A/B), null-free",
        it -> r.chunked(yearCenturyYear, false));
    benchmark.addCase("year, Julian map (VARKA-54 A/B), mixed nulls",
        it -> r.chunked(yearJulian, true));
    benchmark.addCase("year, century-then-year (VARKA-54 A/B), mixed nulls",
        it -> r.chunked(yearCenturyYear, true));
    benchmark.addCase("year+month+day+quarter, Julian map (VARKA-54 A/B), null-free",
        it -> r.chunked(fourJulian, false, 4));
    benchmark.addCase("year+month+day+quarter, century-then-year (VARKA-54 A/B), null-free",
        it -> r.chunked(fourCenturyYear, false, 4));
    benchmark.addCase("add_months(d, 13), Julian map (VARKA-54 A/B), null-free",
        it -> r.chunkedAddMonths(addMonthsJulian, false));
    benchmark.addCase("add_months(d, 13), century-then-year (VARKA-54 A/B), null-free",
        it -> r.chunkedAddMonths(addMonthsCenturyYear, false));
    benchmark.addCase("month, Neri-Schneider (VARKA-53 A/B), null-free",
        it -> r.chunked(monthNeri, false));
    benchmark.addCase("month, 0-based axis (VARKA-53 A/B), null-free",
        it -> r.chunked(monthOld, false));
    benchmark.addCase("month, Neri-Schneider (VARKA-53 A/B), mixed nulls",
        it -> r.chunked(monthNeri, true));
    benchmark.addCase("month, 0-based axis (VARKA-53 A/B), mixed nulls",
        it -> r.chunked(monthOld, true));
    benchmark.addCase("dayofmonth, Neri-Schneider (VARKA-53 A/B), null-free",
        it -> r.chunked(domNeri, false));
    benchmark.addCase("dayofmonth, 0-based axis (VARKA-53 A/B), null-free",
        it -> r.chunked(domOld, false));
    benchmark.addCase("dayofmonth, Neri-Schneider (VARKA-53 A/B), mixed nulls",
        it -> r.chunked(domNeri, true));
    benchmark.addCase("dayofmonth, 0-based axis (VARKA-53 A/B), mixed nulls",
        it -> r.chunked(domOld, true));
    benchmark.addCase("year+month+day+quarter, 0-based axis (VARKA-53 A/B), null-free",
        it -> r.chunked(fourOld, false, 4));
    benchmark.addCase("year+month+day+quarter, null-free", it -> r.chunked(four, false, 4));
    // VARKA-32 step B2 (VARKA-32.md sections 7.2 and 10): siblings over one date in one
    // loop method, where shareChronoPrefix runs the decomposition once, against each in
    // its own method. Since B2 the defaults *are* the shared shape - clause 2 of
    // groupOutputs admits an output that reuses the prefix, up to FUSED_CEILING - so
    // "shared" is emitted with the defaults and "separate" with sharing off, which turns
    // clause 2 off along with the fragment and reproduces the pre-B2 grouping exactly
    // (VarkaEmitterBudgetSuite pins both layouts). Before B2 the shared arm was forced with
    // withGroupBudget(200), the measurement rig 7.2 cleared the gate with; the bytes are
    // the same. Neither the option nor the grouping affects results, only which bytes
    // compute them.
    VarkaEmitOptions separate = VarkaEmitOptions.DEFAULTS.withShareChronoPrefix(false);
    var col0 = new ColumnRef(0);
    List<VarkaVectorIR> yearMonth = List.of(new Year(col0), new Month(col0));
    var yearMonthDayList = new ArrayList<VarkaVectorIR>(yearMonth);
    yearMonthDayList.add(new DayOfMonth(col0));
    List<VarkaVectorIR> yearMonthDay = List.copyOf(yearMonthDayList);
    VarkaFusedKernel yearMonthSeparate = emit(yearMonth, 1, 0, loader, 805, separate);
    VarkaFusedKernel yearMonthShared = emit(yearMonth, 1, 0, loader, 806);
    VarkaFusedKernel yearMonthDaySeparate = emit(yearMonthDay, 1, 0, loader, 807, separate);
    VarkaFusedKernel yearMonthDayShared = emit(yearMonthDay, 1, 0, loader, 808);
    VarkaFusedKernel fourSeparate = emit(fourFields, 1, 0, loader, 809, separate);
    // `four` above is the defaults, which is the shared four-field kernel now.
    VarkaFusedKernel fourShared = four;
    VarkaFusedKernel fourSharedPerGroup = emit(fourFields, 1, 0, loader, 831,
        VarkaEmitOptions.DEFAULTS.withDenseValidityOnce(false));
    VarkaFusedKernel fourSharedWordsPerGroup = emit(fourFields, 1, 0, loader, 941, PER_GROUP_WRITE);
    benchmark.addCase("year+month, separate (2 loop methods), null-free",
        it -> r.chunked(yearMonthSeparate, false, 2));
    benchmark.addCase("year+month, shared (1 loop method), null-free",
        it -> r.chunked(yearMonthShared, false, 2));
    benchmark.addCase("year+month+day, separate (3 loop methods), null-free",
        it -> r.chunked(yearMonthDaySeparate, false, 3));
    benchmark.addCase("year+month+day, shared (1 loop method), null-free",
        it -> r.chunked(yearMonthDayShared, false, 3));
    benchmark.addCase("dayofweek, validity OR-ed per group (VARKA-45 A/B), null-free",
        it -> r.chunked(dowPerGroup, false));
    benchmark.addCase(
        "year+month+day+quarter, shared, validity OR-ed per group (VARKA-45 A/B), null-free",
        it -> r.chunked(fourSharedPerGroup, false, 4));
    benchmark.addCase(
        "year+month+day+quarter, shared, validity OR-ed per group (VARKA-45 A/B), mixed nulls",
        it -> r.chunked(fourSharedPerGroup, true, 4));
    benchmark.addCase("year+month+day+quarter, separate (4 loop methods), null-free",
        it -> r.chunked(fourSeparate, false, 4));
    benchmark.addCase("year+month+day+quarter, shared (1 loop method), null-free",
        it -> r.chunked(fourShared, false, 4));
    benchmark.addCase("year+month+day+quarter, shared (1 loop method), mixed nulls",
        it -> r.chunked(fourShared, true, 4));
    // VARKA-70's A/B on the shape B2 emits by default: four writes and one read per group
    // gone, four bitmap copies per batch - the largest predicted mover (VARKA-70.md 6.1).
    benchmark.addCase(
        "year+month+day+quarter, shared, words per group (VARKA-70 A/B), mixed nulls",
        it -> r.chunked(fourSharedWordsPerGroup, true, 4));
    benchmark.addCase(
        "year+month+day+quarter, shared, per group + general helpers (VARKA-46 A/B), "
            + "mixed nulls",
        it -> r.chunked(fourSharedGeneral, true, 4));
    benchmark.addCase(
        "year+month+day+quarter, shared, per group + OR after the store (VARKA-46 A/B), "
            + "mixed nulls",
        it -> r.chunked(fourSharedOrAfter, true, 4));
    // The regression guard section 5.2 asks for: two chrono nodes over different dates
    // must not be pushed into one method by clause 2, since there is nothing between them
    // to share. VarkaEmitterBudgetSuite pins the two-method layout; this prices what the
    // right non-merge costs against the single-date case above. Before B2 this kernel was
    // emitted under withGroupBudget(200), which put the two in one method by clause 1 with
    // two prefixes in it; under the defaults they are two methods, so this row's number is
    // not comparable to the one committed before B2.
    VarkaFusedKernel twoDates =
        emit(List.of(new Year(col0), new Year(new ColumnRef(1))), 2, 0, loader, 810);
    benchmark.addCase("year(d1), year(d2), two dates, shared option, null-free",
        it -> r.eachChunk((dataOff, validityOff, n) -> {
          int status = twoDates.run(
              new long[] {b.nfData().address() + dataOff, b.nf2Data().address() + dataOff},
              new long[] {0L, 0L}, new int[] {0, 0},
              new long[] {b.dst().address() + dataOff, b.dst2().address() + dataOff},
              new long[] {b.dstValidity().address() + validityOff,
                  b.dst2Validity().address() + validityOff},
              NO_LITERALS, n);
          if (status != 0) {
            throw requirementFailed("the kernel declined a batch: status " + status);
          }
        }));
    // VARKA-52's A/B: the range guard, moved from every calendar extraction (VARKA-26, removed
    // by VARKA-51) to the one producer the compiler cannot bound - a date_add whose offset is a
    // column - and only under a calendar node. `year(date_add(d, off))` is the one shape that
    // pays it, so that is what is priced, guard on against guard off, adjacent and on both null
    // patterns like the VARKA-48 and VARKA-53 pairs; `date_add(d, off)` alone is the control,
    // byte-identical under both settings (VarkaEmitterBudgetSuite asserts it) and measured here
    // so a difference in that row is run noise, not the guard. The second date buffer stands in
    // for the offset column: its values are days in [-10000, 10000), so every sum stays in
    // range and the status must read zero.
    VarkaEmitOptions guardOff = VarkaEmitOptions.DEFAULTS.withGuardDayProducers(false);
    var offsetAdd = new AddDays(col0, new ColumnRef(1));
    VarkaFusedKernel yearOfAddGuarded = emit(List.of(new Year(offsetAdd)), 2, 0, loader, 850);
    VarkaFusedKernel yearOfAddUnguarded =
        emit(List.of(new Year(offsetAdd)), 2, 0, loader, 851, guardOff);
    VarkaFusedKernel addAloneGuardOn = emit(List.of(offsetAdd), 2, 0, loader, 852);
    VarkaFusedKernel addAloneGuardOff = emit(List.of(offsetAdd), 2, 0, loader, 853, guardOff);
    benchmark.addCase("year(date_add(d, off)), producer guard on (VARKA-52 A/B), null-free",
        it -> r.chunkedTwo(yearOfAddGuarded, false));
    benchmark.addCase("year(date_add(d, off)), producer guard off (VARKA-52 A/B), null-free",
        it -> r.chunkedTwo(yearOfAddUnguarded, false));
    benchmark.addCase("year(date_add(d, off)), producer guard on (VARKA-52 A/B), mixed nulls",
        it -> r.chunkedTwo(yearOfAddGuarded, true));
    benchmark.addCase(
        "year(date_add(d, off)), producer guard off (VARKA-52 A/B), mixed nulls",
        it -> r.chunkedTwo(yearOfAddUnguarded, true));
    benchmark.addCase("date_add(d, off) alone, guard option on (VARKA-52 control), null-free",
        it -> r.chunkedTwo(addAloneGuardOn, false));
    benchmark.addCase(
        "date_add(d, off) alone, guard option off (VARKA-52 control), null-free",
        it -> r.chunkedTwo(addAloneGuardOff, false));
    // VARKA-79's pair, on the same producer as VARKA-52's above and with the same two
    // streams: the guarded `year(date_add(d, off))` wrapped in a CASE whose condition is a
    // comparison of the two columns, priced with the arm context on and off. What the
    // delta contains is one mask AND per guarded node per lane group - the context - and
    // nothing else: both arms, both loads and the blend are in each case. `off`'s values
    // keep every sum in range, so no batch declines under either setting and this prices
    // the qualification rather than the declines it prevents, which the differential
    // counts instead.
    VarkaEmitOptions armOff = VarkaEmitOptions.DEFAULTS.withGuardUnderArm(false);
    var guardedUnderArm = new IfElse(
        new Compare(CompareOp.LT, col0, new ColumnRef(1)),
        new Year(offsetAdd),
        new Year(col0));
    VarkaFusedKernel armOn = emit(List.of(guardedUnderArm), 2, 0, loader, 960);
    VarkaFusedKernel armless = emit(List.of(guardedUnderArm), 2, 0, loader, 961, armOff);
    benchmark.addCase(
        "CASE over year(date_add(d, off)), arm context on (VARKA-79 A/B), null-free",
        it -> r.chunkedTwo(armOn, false));
    benchmark.addCase(
        "CASE over year(date_add(d, off)), arm context off (VARKA-79 A/B), null-free",
        it -> r.chunkedTwo(armless, false));
    benchmark.addCase(
        "CASE over year(date_add(d, off)), arm context on (VARKA-79 A/B), mixed nulls",
        it -> r.chunkedTwo(armOn, true));
    benchmark.addCase(
        "CASE over year(date_add(d, off)), arm context off (VARKA-79 A/B), mixed nulls",
        it -> r.chunkedTwo(armless, true));
    // VARKA-60's pair, the same guard block on a heavier producer: add_months' own month
    // count, widened from a literal to a column, with a runtime guard against
    // MONTH_ARITH_MIN/MAX_MONTHS in place of VARKA-40's compile-time bound. nf2Data/mx2Data
    // double as the count column - their values are days in [-10000, 10000), comfortably
    // inside the guard's range - so the status must read zero and this prices the shape
    // alone, not a decline.
    //
    // The control is the literal count on this same runner, emitted adjacent and with the
    // same numInputs, rather than id 811 far above: that one runs chunkedAddMonths and sits
    // dozens of cases away, so its delta carried a different runner and a different JIT and
    // thermal state as well as the change being measured. What the delta here contains is
    // the guard plus one column load in place of a broadcast - the control is passed both
    // stream addresses but its IR reads only col0, so the second stream's traffic is still
    // on the column side of the comparison and the difference is not the guard alone.
    //
    // There is deliberately no guard-off variant. The count check is self-guarding and
    // unconditional (VarkaEmitOptions.guardDayProducers does not reach it), because the
    // compiler's dayRange bounds a column count on the strength of it firing and cannot see
    // the option; an A/B against it would emit identical bytes. Isolating the guard's own
    // cost would take a measurement-only option, which is not worth reintroducing the
    // coupling this task's review removed - see VARKA-60.md 9.
    var addMonthsCol = new AddMonths(col0, new ColumnRef(1));
    var addMonthsLit = new AddMonths(col0, new LiteralSlot(0));
    VarkaFusedKernel addMonthsColGuarded = emit(List.of(addMonthsCol), 2, 0, loader, 950);
    VarkaFusedKernel addMonthsLitControl = emit(List.of(addMonthsLit), 2, 1, loader, 855);
    VarkaFusedKernel addMonthsColPerGroup =
        emit(List.of(addMonthsCol), 2, 0, loader, 943, PER_GROUP_WRITE);
    VarkaFusedKernel addMonthsLitPerGroup =
        emit(List.of(addMonthsLit), 2, 1, loader, 944, PER_GROUP_WRITE);
    benchmark.addCase("add_months(d, m), column count (VARKA-60), null-free",
        it -> r.chunkedTwo(addMonthsColGuarded, false));
    benchmark.addCase("add_months(d, 13), literal count (VARKA-60 control), null-free",
        it -> r.chunkedTwo(addMonthsLitControl, false, new int[] {13}));
    benchmark.addCase("add_months(d, m), column count (VARKA-60), mixed nulls",
        it -> r.chunkedTwo(addMonthsColGuarded, true));
    benchmark.addCase("add_months(d, 13), literal count (VARKA-60 control), mixed nulls",
        it -> r.chunkedTwo(addMonthsLitControl, true, new int[] {13}));
    // VARKA-70's A/B, and its control. The column count's guard keeps both reads, so this
    // row moves by the write alone; the literal count's 81-op tail hides one write, so its
    // row is predicted flat (VARKA-70.md 6.1, prediction 3).
    benchmark.addCase(
        "add_months(d, m), column count, words per group (VARKA-70 A/B), mixed nulls",
        it -> r.chunkedTwo(addMonthsColPerGroup, true));
    benchmark.addCase(
        "add_months(d, 13), literal count, words per group (VARKA-70 control), mixed nulls",
        it -> r.chunkedTwo(addMonthsLitPerGroup, true, new int[] {13}));
    // VARKA-35's A/B: trunc(date, ...) under its two lowerings, SUBTRACT (the day of year or
    // day of month taken off the date) against RECOMPOSE (the period's first day rebuilt
    // through emitDaysFromCivil), adjacent per level like the VARKA-48, VARKA-53 and VARKA-54
    // pairs so both sides run under one JIT and thermal state. MONTH follows the switch too,
    // though only its subtract form is expected to ship: it is the day-of-month tail with one
    // op changed, and it prices the recomposition on the shape where it has the least to hide
    // behind. The per-row DateTimeUtils.truncDate loop is what Spark runs today.
    VarkaEmitOptions recompose = VarkaEmitOptions.DEFAULTS
        .withTruncDate(VarkaEmitOptions.TruncDateForm.RECOMPOSE);
    var truncCases = new ArrayList<TruncArms>();
    for (TruncLevelCase c : List.of(new TruncLevelCase("YEAR", TruncLevel.YEAR, 860),
        new TruncLevelCase("MONTH", TruncLevel.MONTH, 862),
        new TruncLevelCase("QUARTER", TruncLevel.QUARTER, 864))) {
      truncCases.add(new TruncArms(c.name(),
          emit(List.of(new TruncDate(col0, c.level())), 1, 0, loader, c.id()),
          emit(List.of(new TruncDate(col0, c.level())), 1, 0, loader, c.id() + 1, recompose)));
    }
    for (TruncArms arms : truncCases) {
      benchmark.addCase("trunc " + arms.name() + ", subtract (VARKA-35 A/B), null-free",
          it -> r.chunked(arms.subtract(), false));
      benchmark.addCase("trunc " + arms.name() + ", recompose (VARKA-35 A/B), null-free",
          it -> r.chunked(arms.recomposed(), false));
    }
    for (TruncArms arms : truncCases) {
      if (!arms.name().equals("MONTH")) {
        benchmark.addCase("trunc " + arms.name() + ", subtract (VARKA-35 A/B), mixed nulls",
            it -> r.chunked(arms.subtract(), true));
        benchmark.addCase("trunc " + arms.name() + ", recompose (VARKA-35 A/B), mixed nulls",
            it -> r.chunked(arms.recomposed(), true));
      }
    }
    // VARKA-42: make_date over three int columns - the year, month and day of nfData's own
    // dates, so every triple is valid and in range - the NULL and ANSI forms as an adjacent
    // A/B (they differ by a word store and the mask the guard receives), a mixed-null run
    // with mxData's validity on the year, and the per-row path Spark uses today.
    MemorySegment yData = b.arena().allocate(numRows * 4L, 8);
    MemorySegment mData = b.arena().allocate(numRows * 4L, 8);
    MemorySegment dData = b.arena().allocate(numRows * 4L, 8);
    for (int i = 0; i < numRows; i++) {
      LocalDate date = LocalDate.ofEpochDay(b.nfData().get(ValueLayout.JAVA_INT, i * 4L));
      yData.set(ValueLayout.JAVA_INT, i * 4L, date.getYear());
      mData.set(ValueLayout.JAVA_INT, i * 4L, date.getMonthValue());
      dData.set(ValueLayout.JAVA_INT, i * 4L, date.getDayOfMonth());
    }
    var parts = new DateParts(yData, mData, dData);
    var col1 = new ColumnRef(1);
    var col2 = new ColumnRef(2);
    VarkaFusedKernel makeDateNull =
        emit(List.of(new MakeDate(col0, col1, col2, false)), 3, 0, loader, 880);
    VarkaFusedKernel makeDateAnsi =
        emit(List.of(new MakeDate(col0, col1, col2, true)), 3, 0, loader, 881);
    benchmark.addCase("make_date, NULL form (VARKA-42 A/B), null-free",
        it -> r.chunkedThree(makeDateNull, false, parts));
    benchmark.addCase("make_date, ANSI form (VARKA-42 A/B), null-free",
        it -> r.chunkedThree(makeDateAnsi, false, parts));
    benchmark.addCase("make_date, NULL form (VARKA-42 A/B), mixed nulls",
        it -> r.chunkedThree(makeDateNull, true, parts));
    benchmark.addCase("make_date, ANSI form (VARKA-42 A/B), mixed nulls",
        it -> r.chunkedThree(makeDateAnsi, true, parts));
    benchmark.addCase("per-row LocalDate.of make_date (the path Spark uses today)", it -> {
      int pass = 0;
      while (pass < repeats) {
        int i = 0;
        while (i < numRows) {
          LocalDate date = LocalDate.of(yData.get(ValueLayout.JAVA_INT, i * 4L),
              mData.get(ValueLayout.JAVA_INT, i * 4L), dData.get(ValueLayout.JAVA_INT, i * 4L));
          b.dst().set(ValueLayout.JAVA_INT, i * 4L, DateTimeUtils.localDateToDays(date));
          i += 1;
        }
        pass += 1;
      }
    });
    // VARKA-37: weekofyear as the compiler builds it, the week tail over the Thursday shift,
    // beside the dayofyear rows above as the sibling control (the tail is dayofyear's plus
    // four ops, the shift another seventeen), the shift alone, and the per-row path.
    VarkaFusedKernel weekOfYear =
        emit(List.of(new WeekOfYear(new ThursdayOf(col0))), 1, 0, loader, 870);
    VarkaFusedKernel thursdayOf = emit(List.of(new ThursdayOf(col0)), 1, 0, loader, 872);
    // VARKA-58: yearofweek is Year over the same shift; the pair is the sharing row, one
    // ThursdayOf and one prefix for both fields under CSE.
    VarkaFusedKernel yearOfWeek = emit(List.of(new Year(new ThursdayOf(col0))), 1, 0, loader, 873);
    VarkaFusedKernel isoPair = emit(List.of(new WeekOfYear(new ThursdayOf(col0)),
        new Year(new ThursdayOf(col0))), 1, 0, loader, 874);
    benchmark.addCase("weekofyear (VARKA-37), null-free", it -> r.chunked(weekOfYear, false));
    benchmark.addCase("yearofweek (VARKA-58), null-free", it -> r.chunked(yearOfWeek, false));
    benchmark.addCase("weekofyear + yearofweek, one shift (VARKA-58), null-free",
        it -> r.chunked(isoPair, false, 2));
    benchmark.addCase("weekofyear (VARKA-37), null-free", it -> r.chunked(weekOfYear, false));
    benchmark.addCase("weekofyear (VARKA-37), mixed nulls", it -> r.chunked(weekOfYear, true));
    benchmark.addCase("ThursdayOf alone (VARKA-37), null-free",
        it -> r.chunked(thursdayOf, false));
    benchmark.addCase("per-row DateTimeUtils.getWeekOfYear (the path Spark uses today)", it -> {
      int pass = 0;
      while (pass < repeats) {
        int i = 0;
        while (i < numRows) {
          int days = b.nfData().get(ValueLayout.JAVA_INT, i * 4L);
          b.dst().set(ValueLayout.JAVA_INT, i * 4L, DateTimeUtils.getWeekOfYear(days));
          i += 1;
        }
        pass += 1;
      }
    });
    benchmark.addCase("per-row DateTimeUtils.truncDate YEAR (the path Spark uses today)", it -> {
      int pass = 0;
      while (pass < repeats) {
        int i = 0;
        while (i < numRows) {
          int days = b.nfData().get(ValueLayout.JAVA_INT, i * 4L);
          // TRUNC_TO_YEAR is private[sql], which gets no static forwarder: read it off the
          // module, as the Scala call compiles to.
          b.dst().set(ValueLayout.JAVA_INT, i * 4L,
              DateTimeUtils.truncDate(days, DateTimeUtils$.MODULE$.TRUNC_TO_YEAR()));
          i += 1;
        }
        pass += 1;
      }
    });
    // VARKA-32's ceiling (VARKA-32.md): the same four fields from one shared
    // decomposition, computed by hand outside the emitter (ChronoVectorOps), against the four
    // independently emitted nodes above. It runs through the same eachChunk walk, over the
    // same buffers, writing the same four data and four validity destinations, and pays the
    // same narrow-range guard - once, where the four-node case pays it four times, which is
    // the saving being measured rather than an omission. The first version of this case
    // omitted the guard, wrote one shared validity buffer and hand-copied the chunk loop;
    // the number it produced is why VARKA-32 was first declined.
    benchmark.addCase("year+month+day+quarter, shared decomposition (hand-written ceiling)",
        it -> r.eachChunk((dataOff, validityOff, n) -> {
          int status = ChronoVectorOps.vectorFourFields(
              b.nfData().address() + dataOff, 0L, 0,
              shifted(dstData4, dataOff), shifted(dstValidity4, validityOff), n);
          if (status != 0) {
            throw requirementFailed("the ceiling kernel declined a batch: status " + status);
          }
        }));
    // The same arithmetic and the same op count, scheduled to keep fewer values live: the
    // year assembly hoisted so era/century/yoc die early, and each output stored as soon as
    // it exists rather than all four at the end (which is also what emitLaneGroup does). The
    // pair prices the schedule alone, which is what decides whether the 128-bit width can
    // reach the win the native width gets - see VARKA-32.md section 7.
    benchmark.addCase("year+month+day+quarter, shared decomposition (short live ranges)",
        it -> r.eachChunk((dataOff, validityOff, n) -> {
          int status = ChronoVectorOps.vectorFourFieldsShortLive(
              b.nfData().address() + dataOff, 0L, 0,
              shifted(dstData4, dataOff), shifted(dstValidity4, validityOff), n);
          if (status != 0) {
            throw requirementFailed("the short-live kernel declined a batch: status " + status);
          }
        }));
    // Section 2.17's bound: the same kernel, the same guard, the same op count, with every
    // destination validity buffer and every orValidityBitsAt/orPartialValidityBitsAt call
    // removed. The gap between this and the ceiling above is what the validity write costs,
    // separated from the arithmetic without touching a single line the two kernels share -
    // ChronoVectorOpsTest checks that byte for byte.
    benchmark.addCase("year+month+day+quarter, shared decomposition (no validity write)",
        it -> r.eachChunk((dataOff, validityOff, n) -> {
          int status = ChronoVectorOps.vectorFourFieldsNoValidity(
              b.nfData().address() + dataOff, shifted(dstData4, dataOff), n);
          if (status != 0) {
            throw requirementFailed("the no-validity kernel declined a batch: status " + status);
          }
        }));
    // The scalar baseline this file never had. The LocalDate anchor below is not one: it
    // allocates a LocalDate per row, so it prices allocation rather than arithmetic.
    // ChronoScalarOps is the same civil-from-days as an ordinary Java loop over the same
    // buffer, in the same chunks, writing the same outputs - so the gap between these and
    // the emitted "year, null-free" case above is what the Vector API is actually worth
    // here. The two spellings differ only in how each division is written, and that is the
    // second question: `/` becomes a high-half multiply, which has no vector node and so
    // can never auto-vectorize, while `(x * M) >>> k` is a plain 64-bit multiply and shift,
    // which SuperWord may vectorize on its own. Running the magic case again under
    // -XX:-UseSuperWord says whether it did.
    benchmark.addCase("scalar year, division (cannot auto-vectorize), null-free",
        it -> r.scalarChunked(ChronoScalarOps::yearByDivision));
    benchmark.addCase("scalar year, magic multiply (may auto-vectorize), null-free",
        it -> r.scalarChunked(ChronoScalarOps::yearByMagic));
    // The 2x2 that says *why* SuperWord declined the case above: array against
    // MemorySegment, trivial body against the full decomposition. Re-run the section under
    // -XX:-UseSuperWord; a case SuperWord vectorized slows down, one it never touched does
    // not move. See ChronoScalarOps's probe block. These are diagnostics, not kernels, and
    // they are here rather than in a scratch program so the answer stays reproducible.
    int[] probeSrc = new int[chunk];
    int[] probeDst = new int[chunk];
    int pi = 0;
    while (pi < chunk) {
      probeSrc[pi] = b.nfData().get(ValueLayout.JAVA_INT, pi * 4L);
      pi += 1;
    }
    int probePasses = repeats * (numRows / chunk);
    benchmark.addCase("probe: trivial body, int[] (control)", it -> {
      int p = 0;
      while (p < probePasses) {
        ChronoScalarOps.probeArrayTrivial(probeSrc, probeDst, chunk);
        p += 1;
      }
    });
    benchmark.addCase("probe: trivial body, MemorySegment", it -> {
      int p = 0;
      while (p < probePasses) {
        ChronoScalarOps.probeSegmentTrivial(b.nfData().address(), b.dst().address(), chunk);
        p += 1;
      }
    });
    benchmark.addCase("probe: full year body, int[]", it -> {
      int p = 0;
      while (p < probePasses) {
        ChronoScalarOps.probeArrayYear(probeSrc, probeDst, chunk);
        p += 1;
      }
    });
    // The in-harness anchors: dayofweek is the cheapest emitted date node (a 20-op vector
    // body against year's ~50), and the per-row LocalDate loop is what Spark runs today.
    benchmark.addCase("dayofweek, for scale, null-free", it -> r.chunked(dow, false));
    benchmark.addCase("per-row LocalDate year (the path Spark uses today)", it -> {
      int pass = 0;
      while (pass < repeats) {
        int i = 0;
        while (i < numRows) {
          int days = b.nfData().get(ValueLayout.JAVA_INT, i * 4L);
          b.dst().set(ValueLayout.JAVA_INT, i * 4L, LocalDate.ofEpochDay(days).getYear());
          i += 1;
        }
        pass += 1;
      }
    });
    runCases(benchmark);
  }

  // ---------------------------------------------------------------------------------------
  // Grouping, the fused-method ladder and the widest shape
  // ---------------------------------------------------------------------------------------

  private void groupBudget(Buffers b) {
    // VARKA-17's pair, and since VARKA-71 the A/B is the rule rather than the budget. A
    // shared depth-8 chain with six more ops on each of two outputs is 20 distinct nodes
    // against a budget of 16, so clause 1 splits them and the second method recomputes the
    // eight shared ops per lane group - 60 vector ops across two methods against 43 in
    // one. VARKA-71 measured that the merge is strictly less work and that clause 1 rejects
    // it anyway, because the budget bounds the method while the marginal cost already
    // excludes what the group holds; the fix is clause 2 counting whole-node reuse
    // (`shareWholeNodes`), not a wider budget, because the budget is what keeps compile
    // time in hand. So the arms are the rule off and on, both at the shipped budget: off
    // is what every regeneration through VARKA-70 measured, on is what ships.
    VarkaBenchmark benchmark = benchmark(
        "two outputs over a shared chain, " + numRows + " rows, mixed nulls", numRows)
        .timed().build();
    VarkaVectorIR shared = chain(8);
    List<VarkaVectorIR> roots = List.of(chainOver(shared, 6, 8), chainOver(shared, 6, 14));
    int[] offsets = offsets(20, 13);
    VarkaFusedKernel split = emit(roots, 2, 20, b.loader(), 600,
        VarkaEmitOptions.DEFAULTS.withShareWholeNodes(false));
    VarkaFusedKernel together = emit(roots, 2, 20, b.loader(), 601, VarkaEmitOptions.DEFAULTS);
    Consumer<VarkaFusedKernel> run = kernel ->
        kernel.run(new long[] {b.mxData().address(), b.mx2Data().address()},
            new long[] {b.mxValidity().address(), b.mx2Validity().address()},
            new int[] {b.mxNulls(), b.mx2Nulls()},
            new long[] {b.dst().address(), b.dst2().address()},
            new long[] {b.dstValidity().address(), b.dst2Validity().address()}, offsets,
            numRows);
    benchmark.addCase("whole-node reuse off: two loop methods, shared chain recomputed",
        it -> run.accept(split));
    benchmark.addCase("whole-node reuse on (shipped): one loop method, CSE kept",
        it -> run.accept(together));
    runCases(benchmark);
  }

  private void fusedMethodLadder(Buffers b) {
    // Clause 2 of groupOutputs merges every calendar output that reuses the prefix, up to
    // FUSED_CEILING, and a projection can carry more of them than the task-26 quartet. The
    // two-, three- and four-field rows are in the "year" section; this one goes past four
    // with add_months over the same date under distinct literals - the heaviest tail in
    // the register (81 ops against a field's 5), so it is the node that decides how many
    // outputs a ceiling admits: six outputs weigh 214, eight 376, twelve 700. Each row at
    // four settings: unshared (sharing off, every output its own method); one method (a
    // ceiling nothing here reaches); and the two candidate ceilings, which split the row
    // wherever it exceeds them. Null-free data, the "year" section's 4096-row chunks, one
    // destination per output. The ceiling ships at the widest row whose one-method arm
    // still beats its unshared arm at both vector widths and whose methods reach tier 4
    // inside a second (-XX:+PrintCompilation, read in a separate run); the case names
    // carry each arm's loop-method count, so the file says which split it measured.
    VarkaBenchmark benchmark = benchmark((long) numRows * repeats + " rows in 4096-row chunks",
        (long) numRows * repeats).timed().build();
    var col = new ColumnRef(0);
    List<VarkaVectorIR> fields =
        List.of(new Year(col), new Month(col), new DayOfMonth(col), new Quarter(col));
    int[] months = {1, 2, 3, 4, 5, 6, 7, 8};
    MemorySegment[] dsts = allocateAll(b.arena(), 12, numRows * 4L);
    MemorySegment[] dstValidities = allocateAll(b.arena(), 12, (numRows + 7) / 8L);
    KernelAt chunkedWide = (kernel, outputs) -> {
      int[] lits = Arrays.copyOf(months, outputs - 4);
      int pass = 0;
      while (pass < repeats) {
        int done = 0;
        while (done < numRows) {
          int n = Math.min(chunk, numRows - done);
          long dataOff = done * 4L;
          long validityOff = done / 8L;
          int status = kernel.run(new long[] {b.nfData().address() + dataOff},
              new long[] {0L}, new int[] {0},
              takeAddressesShifted(dsts, outputs, dataOff),
              takeAddressesShifted(dstValidities, outputs, validityOff), lits, n);
          if (status != 0) {
            throw requirementFailed("the kernel declined a batch: status " + status);
          }
          done += n;
        }
        pass += 1;
      }
    };
    List<Setting> settings = List.of(
        new Setting("unshared", VarkaEmitOptions.DEFAULTS.withShareChronoPrefix(false)),
        new Setting("one method", VarkaEmitOptions.DEFAULTS.withFusedCeiling(1000)),
        new Setting("fusedCeiling 200", VarkaEmitOptions.DEFAULTS.withFusedCeiling(200)),
        new Setting("fusedCeiling 400", VarkaEmitOptions.DEFAULTS.withFusedCeiling(400)));
    int id = 910;
    for (int outputs : List.of(6, 8, 12)) {
      for (Setting setting : settings) {
        // add_months(d, k), k = 1, 2, ...: one literal slot each.
        var roots = new ArrayList<VarkaVectorIR>(fields);
        for (int k = 0; k < outputs - 4; k++) {
          roots.add(new AddMonths(col, new LiteralSlot(k)));
        }
        VarkaFusedKernel kernel =
            emit(List.copyOf(roots), 1, outputs - 4, b.loader(), id, setting.options());
        id += 1;
        int methods = 0;
        for (Method m : kernel.getClass().getDeclaredMethods()) {
          if (VarkaMethodNames.isLoop(m.getName(), true)) {
            methods += 1;
          }
        }
        benchmark.addCase(outputs + " outputs over one date, " + setting.label() + " ("
            + methods + " loop methods), null-free", it -> chunkedWide.run(kernel, outputs));
      }
    }
    runCases(benchmark);
  }

  private void widestShape(Buffers b) {
    // Four disjoint depth-16 chains: 64 distinct ops, the cap exactly - emitted as four
    // GROUP_BUDGET-sized loop methods since VARKA-11, which is what keeps this case honest
    // in a JVM that has compiled every other kernel in this file first (see
    // VARKA-11.md section 6). The sequential version is the same 64 kernel passes.
    VarkaBenchmark benchmark = benchmark("4 outputs x depth 16 over " + numRows + " rows", numRows)
        .timed().build();
    List<VarkaVectorIR> roots = roots(4, k -> chain(16, k * 16));
    int[] offsets = offsets(64, 7);
    VarkaFusedKernel fused = emit(roots, 1, 64, b.loader(), 300);
    benchmark.addCase("fused, 64 ops", it ->
        fused.run(new long[] {b.mxData().address()}, new long[] {b.mxValidity().address()},
            new int[] {b.mxNulls()}, addresses(b.wideDst()), addresses(b.wideDstValidity()),
            offsets, numRows));
    benchmark.addCase("sequential kernels, 64 passes", it -> {
      for (int k = 0; k < 4; k++) {
        b.sequentialChain(16, Arrays.copyOfRange(offsets, k * 16, k * 16 + 16),
            b.mxData().address(), b.mxValidity().address(), b.mxNulls(),
            b.wideDst()[k].address(), b.wideDstValidity()[k].address());
      }
    });
    runCases(benchmark);
  }

  // ---------------------------------------------------------------------------------------
  // The derived leaves: next_day and trunc over a string column
  // ---------------------------------------------------------------------------------------

  /** Scala's {@code s.head.toString + s.tail.toLowerCase}: the first letter kept. */
  private static String capitalised(String s) {
    return s.substring(0, 1) + s.substring(1).toLowerCase(Locale.ROOT);
  }

  /**
   * Closes every non-null vector in {@code vs}, guarding each so one failure cannot strand the
   * rest. {@code failing}, when non-null, is an exception already on its way out: a close
   * failure is attached to it rather than replacing it, since that one is the reason the run is
   * ending. What it catches is what Scala's {@code NonFatal} catches: anything but a virtual
   * machine error, a linkage error or an interrupt. ({@code NonFatal} also excludes
   * {@code ThreadDeath}, which nothing can throw since {@code Thread.stop} was removed.)
   */
  private static void closeNames(VarCharVector[] vs, Throwable failing) {
    if (vs != null) {
      int i = 0;
      while (i < vs.length) {
        VarCharVector v = vs[i];
        vs[i] = null;
        if (v != null) {
          try {
            v.close();
          } catch (Throwable e) {
            if (e instanceof VirtualMachineError || e instanceof LinkageError
                || e instanceof InterruptedException) {
              throw e;
            }
            if (failing != null) {
              failing.addSuppressed(e);
            } else {
              System.out.println("closing a benchmark string vector failed: " + e);
            }
          }
        }
        i += 1;
      }
    }
  }

  private static final List<String> WEEKDAY_SPELLINGS = List.of("SU", "SUN", "SUNDAY", "MO",
      "MON", "MONDAY", "TU", "TUE", "TUESDAY", "WE", "WED", "WEDNESDAY", "TH", "THU", "THURSDAY",
      "FR", "FRI", "FRIDAY", "SA", "SAT", "SATURDAY");

  private static String styledWeekday(int i) {
    String s = WEEKDAY_SPELLINGS.get(i % WEEKDAY_SPELLINGS.size());
    return switch ((i / WEEKDAY_SPELLINGS.size()) % 3) {
      case 0 -> s;
      case 1 -> s.toLowerCase(Locale.ROOT);
      default -> capitalised(s);
    };
  }

  /**
   * Builds every chunk's vector, closing the ones already built if a later allocation fails.
   * Building the array in one expression would drop the partial array on the way out, leaking
   * each vector it had filled - and {@code allocateNew} is exactly the call that can fail here.
   */
  private static VarCharVector[] names(BufferAllocator allocator, int chunks, int chunk,
      IntPredicate bad) {
    VarCharVector[] built = new VarCharVector[chunks];
    try {
      int c = 0;
      while (c < chunks) {
        int n = Math.min(chunk, numRows - c * chunk);
        VarCharVector v = new VarCharVector("s", allocator);
        built[c] = v;
        v.allocateNew(n * 10L, n);
        for (int i = 0; i < n; i++) {
          int row = c * chunk + i;
          String s = bad.test(row) ? "xyz" : styledWeekday(row);
          v.setSafe(i, s.getBytes(StandardCharsets.UTF_8));
        }
        v.setValueCount(n);
        c += 1;
      }
      return built;
    } catch (RuntimeException | Error e) {
      closeNames(built, e);
      throw e;
    }
  }

  private static ArrowColumnVector[] columns(VarCharVector[] vectors) {
    ArrowColumnVector[] out = new ArrowColumnVector[vectors.length];
    for (int k = 0; k < vectors.length; k++) {
      out[k] = new ArrowColumnVector(vectors[k]);
    }
    return out;
  }

  private void nextDay(Buffers b) {
    // VARKA-59's measurement (VARKA-59.md 6). next_day with a weekday column runs as
    // the two-input kernel over an int32 column the evaluator derives per batch from the
    // string column through the row engine's own parser; the fused form is the column
    // kernel plus the leaf, and the anchor it is held to is getNextDateExact per row, the
    // path the row engine runs: the same parse and the arithmetic, per row. The literal
    // kernel is the control that must not move, and the column kernel is priced beside it
    // (one load in place of a broadcast). The leaf is priced alone under both parsers,
    // over valid names and over names of which a tenth are unrecognised, where the
    // row-engine parser constructs an exception per bad row. Driven in 4096-row chunks
    // over the whole buffer like the year section, for the same reason: the anchor walks
    // every row, so the kernel must too.
    VarkaBenchmark benchmark = benchmark((long) numRows * repeats + " rows in 4096-row chunks",
        (long) numRows * repeats).timed().build();
    // The weekday column: the 21 spellings in three case styles, cycling; and the same
    // with every tenth row unrecognised. One Arrow vector per 4096-row chunk, which is
    // what the evaluator hands the leaf - a cached batch is its own vector - read through
    // the same ArrowColumnVector accessor it reads a cached string column by.
    BufferAllocator allocator = ArrowUtils.rootAllocator().newChildAllocator("next-day-bench", 0,
        Long.MAX_VALUE);
    int chunks = (numRows + chunk - 1) / chunk;
    // The two arrays are built *inside* the try, and assigned to variables declared outside
    // it, so that the finally owns them from the first vector onward. Built before it - as
    // they were - a failure in the second `names` call left the first array and the
    // allocator unreachable for the rest of the benchmark JVM.
    VarCharVector[] validNames = null;
    VarCharVector[] tenthBad = null;
    try {
      validNames = names(allocator, chunks, chunk, row -> false);
      tenthBad = names(allocator, chunks, chunk, row -> row % 10 == 9);
      ArrowColumnVector[] valid = columns(validNames);
      ArrowColumnVector[] mixedNames = columns(tenthBad);
      // k as the kernel reads it after the leaf: -1 .. 5, cycling; the leaf's own output
      // buffers, overwritten per chunk.
      MemorySegment kData = b.arena().allocate(numRows * 4L, 8);
      for (int i = 0; i < numRows; i++) {
        kData.set(ValueLayout.JAVA_INT, i * 4L, i % 7 - 1);
      }
      MemorySegment kValidity = b.arena().allocate((numRows + 7) / 8L, 8);
      VarkaFusedKernel literal =
          emit(List.of(new NextDay(new ColumnRef(0), new LiteralSlot(0))), 1, 1,
              b.loader(), 890);
      VarkaFusedKernel column = emit(List.of(new NextDay(new ColumnRef(0), new ColumnRef(1))), 2, 0,
          b.loader(), 891);
      VarkaFusedKernel columnPerGroup =
          emit(List.of(new NextDay(new ColumnRef(0), new ColumnRef(1))), 2, 0,
              b.loader(), 942, PER_GROUP_WRITE);
      Runnable chunkedLiteral = () -> eachChunk(repeats, chunk, (dataOff, validityOff, n) -> {
        int status = literal.run(new long[] {b.nfData().address() + dataOff},
            new long[] {0L}, new int[] {0}, new long[] {b.dst().address() + dataOff},
            new long[] {b.dstValidity().address() + validityOff}, new int[] {3}, n);
        if (status != 0) {
          throw requirementFailed("the kernel declined a batch: status " + status);
        }
      });
      // The column kernel over the k column as the leaf leaves it: null-free, and with the
      // date's mixed-null pattern on the date side (the leaf's own nulls are the parity of
      // the mixed-names case above, not of this kernel).
      KernelArm chunkedColumn = (kernel, mixed) ->
          eachChunk(repeats, chunk, (dataOff, validityOff, n) -> {
            int status;
            if (mixed) {
              status = kernel.run(
                  new long[] {b.mxData().address() + dataOff, kData.address() + dataOff},
                  new long[] {b.mxValidity().address() + validityOff, 0L},
                  new int[] {(n + 6) / 7, 0},
                  new long[] {b.dst().address() + dataOff},
                  new long[] {b.dstValidity().address() + validityOff}, NO_LITERALS, n);
            } else {
              status = kernel.run(
                  new long[] {b.nfData().address() + dataOff, kData.address() + dataOff},
                  new long[] {0L, 0L}, new int[] {0, 0},
                  new long[] {b.dst().address() + dataOff},
                  new long[] {b.dstValidity().address() + validityOff}, NO_LITERALS, n);
            }
            if (status != 0) {
              throw requirementFailed("the kernel declined a batch: status " + status);
            }
          });
      BiConsumer<ArrowColumnVector[], WeekdayLeaf.Parser> chunkedLeaf = (cols, parser) ->
          eachChunk(repeats, chunk, (dataOff, validityOff, n) -> {
            int nulls = WeekdayLeaf.fill(cols[(int) (dataOff / 4L) / chunk], n, false,
                parser, kData.address() + dataOff, kValidity.address() + validityOff);
            if (nulls < 0) {
              throw new IllegalArgumentException("requirement failed");
            }
          });
      benchmark.addCase("next_day(d, 'MON'), literal kernel (control), null-free",
          it -> chunkedLiteral.run());
      benchmark.addCase("next_day(d, k), column kernel, null-free",
          it -> chunkedColumn.run(column, false));
      benchmark.addCase("next_day(d, k), column kernel, mixed nulls on the date",
          it -> chunkedColumn.run(column, true));
      // VARKA-70's A/B on the two-input AND: both reads and the write gone, one bitmap AND.
      benchmark.addCase(
          "next_day(d, k), column kernel, words per group (VARKA-70 A/B), mixed nulls on the "
              + "date", it -> chunkedColumn.run(columnPerGroup, true));
      benchmark.addCase("weekday leaf, row-engine parser, valid names",
          it -> chunkedLeaf.accept(valid, WeekdayLeaf.Parser.ROW_ENGINE));
      benchmark.addCase("weekday leaf, ascii parser, valid names",
          it -> chunkedLeaf.accept(valid, WeekdayLeaf.Parser.ASCII));
      benchmark.addCase("weekday leaf, row-engine parser, a tenth unrecognised",
          it -> chunkedLeaf.accept(mixedNames, WeekdayLeaf.Parser.ROW_ENGINE));
      benchmark.addCase("weekday leaf, ascii parser, a tenth unrecognised",
          it -> chunkedLeaf.accept(mixedNames, WeekdayLeaf.Parser.ASCII));
      benchmark.addCase("next_day(d, s) per row, getNextDateExact (the row engine)",
          it -> eachChunk(repeats, chunk, (dataOff, validityOff, n) -> {
            ArrowColumnVector weekdays = valid[(int) (dataOff / 4L) / chunk];
            int i = 0;
            while (i < n) {
              int days = b.nfData().get(ValueLayout.JAVA_INT, dataOff + i * 4L);
              b.dst().set(ValueLayout.JAVA_INT, dataOff + i * 4L,
                  DateTimeExpressionUtils.getNextDateExact(days, weekdays.getUTF8String(i)));
              i += 1;
            }
          }));
      runCases(benchmark);
    } finally {
      // Each vector close is guarded and the allocator close sits in its own finally, so
      // one failure cannot skip the rest: the allocator is a child of the process-lifetime
      // root, and skipping its close charges these bytes against the root for the whole run.
      try {
        closeNames(validNames, null);
        closeNames(tenthBad, null);
      } finally {
        allocator.close();
      }
    }
  }

  private static final List<String> TRUNC_SPELLINGS =
      List.of("YEAR", "YYYY", "YY", "MON", "MONTH", "MM", "QUARTER", "WEEK");

  private static String styledFormat(int i) {
    String f = TRUNC_SPELLINGS.get(i % TRUNC_SPELLINGS.size());
    return switch ((i / TRUNC_SPELLINGS.size()) % 3) {
      case 0 -> f;
      case 1 -> f.toLowerCase(Locale.ROOT);
      default -> capitalised(f);
    };
  }

  /** Builds every chunk's format vector, closing the ones already built if one fails, as names. */
  private static VarCharVector[] formats(BufferAllocator allocator, int chunks, int chunk,
      IntPredicate bad) {
    VarCharVector[] built = new VarCharVector[chunks];
    try {
      for (int c = 0; c < chunks; c++) {
        int n = Math.min(chunk, numRows - c * chunk);
        VarCharVector v = new VarCharVector("fmt", allocator);
        built[c] = v;
        v.allocateNew(n * 8L, n);
        for (int i = 0; i < n; i++) {
          int row = c * chunk + i;
          String f = bad.test(row) ? (row % 20 == 9 ? "DAY" : "QTR") : styledFormat(row);
          v.setSafe(i, f.getBytes(StandardCharsets.UTF_8));
        }
        v.setValueCount(n);
      }
      return built;
    } catch (RuntimeException | Error e) {
      closeNames(built, e);
      throw e;
    }
  }

  private void truncFormat(Buffers b) {
    // VARKA-61's measurement (VARKA-61.md 6). trunc(d, fmt) with a format column runs
    // as the two-input kernel over an int32 level column the evaluator derives per batch
    // through the row engine's own parseTruncLevel; the kernel computes all four periods
    // and blends on the level, so it is priced against the widest and the narrowest
    // literal tails (QUARTER, MONTH), which must not move. The leaf is priced alone over
    // valid spellings and over spellings of which a tenth are unrecognised (no error path:
    // an unrecognised format is a null lane), and the anchor is parseTruncLevel then
    // truncDate per row, which is what the row engine runs. 4096-row chunks like the year
    // and next_day sections.
    VarkaBenchmark benchmark = benchmark((long) numRows * repeats + " rows in 4096-row chunks",
        (long) numRows * repeats).timed().build();
    // The format column: the eight accepted spellings in three case styles, cycling; and
    // the same with every tenth row a sub-day or unrecognised format.
    BufferAllocator allocator = ArrowUtils.rootAllocator().newChildAllocator("trunc-bench", 0,
        Long.MAX_VALUE);
    int chunks = (numRows + chunk - 1) / chunk;
    // Built inside the try and closed in a guarded finally, as the next_day section's names
    // are, so that a failure in the second call or in one close cannot strand the first array
    // or the allocator. (The Scala form built both before its try and closed them unguarded.)
    VarCharVector[] validFormats = null;
    VarCharVector[] tenthBad = null;
    try {
      validFormats = formats(allocator, chunks, chunk, row -> false);
      tenthBad = formats(allocator, chunks, chunk, row -> row % 10 == 9);
      ArrowColumnVector[] valid = columns(validFormats);
      ArrowColumnVector[] mixedFormats = columns(tenthBad);
      // The level column as the kernel reads it after the leaf: the four codes cycling;
      // also the leaf's own output buffers, overwritten per chunk.
      MemorySegment levelData = b.arena().allocate(numRows * 4L, 8);
      for (int i = 0; i < numRows; i++) {
        levelData.set(ValueLayout.JAVA_INT, i * 4L, TruncLevelLeaf.WEEK + i % 4);
      }
      MemorySegment levelValidity = b.arena().allocate((numRows + 7) / 8L, 8);
      VarkaFusedKernel dynamic =
          emit(List.of(new TruncDateDynamic(new ColumnRef(0), new ColumnRef(1))),
              2, 0, b.loader(), 902);
      VarkaFusedKernel quarter =
          emit(List.of(new TruncDate(new ColumnRef(0), TruncLevel.QUARTER)), 1, 0,
              b.loader(), 903);
      VarkaFusedKernel month =
          emit(List.of(new TruncDate(new ColumnRef(0), TruncLevel.MONTH)), 1, 0,
              b.loader(), 904);
      KernelArm chunkedLiteral = (kernel, mixed) ->
          eachChunk(repeats, chunk, (dataOff, validityOff, n) -> {
            int status;
            if (mixed) {
              status = kernel.run(new long[] {b.mxData().address() + dataOff},
                  new long[] {b.mxValidity().address() + validityOff}, new int[] {(n + 6) / 7},
                  new long[] {b.dst().address() + dataOff},
                  new long[] {b.dstValidity().address() + validityOff}, NO_LITERALS, n);
            } else {
              status = kernel.run(new long[] {b.nfData().address() + dataOff},
                  new long[] {0L}, new int[] {0}, new long[] {b.dst().address() + dataOff},
                  new long[] {b.dstValidity().address() + validityOff}, NO_LITERALS, n);
            }
            if (status != 0) {
              throw requirementFailed("the kernel declined a batch: status " + status);
            }
          });
      // The dynamic kernel over the level column as the leaf leaves it: null-free, and
      // with the date's mixed-null pattern on the date side.
      MixedArm chunkedDynamic = mixed ->
          eachChunk(repeats, chunk, (dataOff, validityOff, n) -> {
            int status;
            if (mixed) {
              status = dynamic.run(
                  new long[] {b.mxData().address() + dataOff, levelData.address() + dataOff},
                  new long[] {b.mxValidity().address() + validityOff, 0L},
                  new int[] {(n + 6) / 7, 0},
                  new long[] {b.dst().address() + dataOff},
                  new long[] {b.dstValidity().address() + validityOff}, NO_LITERALS, n);
            } else {
              status = dynamic.run(
                  new long[] {b.nfData().address() + dataOff, levelData.address() + dataOff},
                  new long[] {0L, 0L}, new int[] {0, 0},
                  new long[] {b.dst().address() + dataOff},
                  new long[] {b.dstValidity().address() + validityOff}, NO_LITERALS, n);
            }
            if (status != 0) {
              throw requirementFailed("the kernel declined a batch: status " + status);
            }
          });
      Consumer<ArrowColumnVector[]> chunkedLeaf = cols ->
          eachChunk(repeats, chunk, (dataOff, validityOff, n) -> {
            int nulls = TruncLevelLeaf.fill(cols[(int) (dataOff / 4L) / chunk], n,
                levelData.address() + dataOff, levelValidity.address() + validityOff);
            if (nulls < 0) {
              throw new IllegalArgumentException("requirement failed");
            }
          });
      benchmark.addCase("trunc(d, 'QUARTER'), literal kernel (control), null-free",
          it -> chunkedLiteral.run(quarter, false));
      benchmark.addCase("trunc(d, 'MONTH'), literal kernel (control), null-free",
          it -> chunkedLiteral.run(month, false));
      benchmark.addCase("trunc(d, level), dynamic kernel, null-free",
          it -> chunkedDynamic.run(false));
      benchmark.addCase("trunc(d, 'QUARTER'), literal kernel (control), mixed nulls",
          it -> chunkedLiteral.run(quarter, true));
      benchmark.addCase("trunc(d, level), dynamic kernel, mixed nulls on the date",
          it -> chunkedDynamic.run(true));
      benchmark.addCase("trunc-level leaf, valid formats", it -> chunkedLeaf.accept(valid));
      benchmark.addCase("trunc-level leaf, a tenth sub-day or unrecognised",
          it -> chunkedLeaf.accept(mixedFormats));
      benchmark.addCase("trunc(d, fmt) per row, parseTruncLevel then truncDate (the row "
          + "engine)", it -> eachChunk(repeats, chunk, (dataOff, validityOff, n) -> {
            ArrowColumnVector formatColumn = valid[(int) (dataOff / 4L) / chunk];
            int i = 0;
            while (i < n) {
              int days = b.nfData().get(ValueLayout.JAVA_INT, dataOff + i * 4L);
              int level = DateTimeUtils.parseTruncLevel(formatColumn.getUTF8String(i));
              b.dst().set(ValueLayout.JAVA_INT, dataOff + i * 4L,
                  DateTimeUtils.truncDate(days, level));
              i += 1;
            }
          }));
      runCases(benchmark);
    } finally {
      try {
        closeNames(validFormats, null);
        closeNames(tenthBad, null);
      } finally {
        allocator.close();
      }
    }
  }

  // ---------------------------------------------------------------------------------------
  // Batch lengths, the epilogue and the width of one loop method
  // ---------------------------------------------------------------------------------------

  private void batchAlignment(Buffers b) {
    // Milestone 4 open question 3, answered before the masked epilogue replaces the tail.
    // Every committed harness in this project happens to be lane-aligned - this file runs
    // one call over 1,000,000 rows, DateVectorOpsBenchmark's sizes are 32 / 10000 /
    // 1000000, and Spark's default COLUMN_BATCH_SIZE is 4096, every one a multiple of 4, 8
    // and 16 - so loopBound == length throughout and the scalar tail has never executed a
    // single row under measurement. Two chunk ladders, because the production shape and
    // the marginal cost are different questions. 4096 against 4095 is the shape a query
    // actually runs: the unaligned arm leaves lanes-1 rows to the tail, which is 0.4% of a
    // batch at 16 lanes and may well sit inside this harness's noise - that being itself
    // the finding. 64 against 63 leaves the same lanes-1 rows in a batch a sixty-fourth
    // the size, magnifying them to a quarter of the work so the marginal cost of a scalar
    // row is measured rather than inferred.
    //
    // Two comparisons, read differently. Within a pair the call counts match to within
    // one, so the difference is the tail and nothing else. Between the pairs the call
    // count rises 64-fold, so the difference prices the per-call prologue - which matters
    // here because every loop and epilogue method emits that prologue, so the tail method
    // re-wraps every segment, re-reads the species and recomputes loopBound before
    // discovering it has no rows to process.
    //
    // Every case processes the same row count, so the columns compare directly across
    // chunk sizes. Each call re-reads the same cache-warm prefix rather than walking the
    // buffer, which holds memory traffic constant and leaves the loop shape as the only
    // variable - the same cache-resident regime the rest of this file runs in, so the
    // rates here are comparable to the chain section above.
    //
    // Read the result as an absolute cost, not a percentage: divide a pair's per-call
    // difference by lanes-1 to get the price of one scalar tail row. The percentage
    // depends entirely on the denominator, and this file's denominator is a bare kernel
    // call. An end-to-end query's is ten to twenty times larger - VarkaFilterBenchmark
    // and VarkaThroughputBenchmark measure 5-25 ns/row once Arrow access and the row
    // boundary are in it - so the same absolute cost is a far smaller share there.
    VarkaBenchmark benchmark = benchmark((long) numRows * repeats + " rows in chunks",
        (long) numRows * repeats).timed().build();
    int[] chainOffsets = offsets(4, 13);
    VarkaFusedKernel chain4 = emit(List.of(chain(4)), 1, 4, b.loader(), 700);
    VarkaFusedKernel dow = emit(List.of(new DayOfWeek(new ColumnRef(0))), 1, 0, b.loader(), 701);
    for (Rung rung : LADDER) {
      benchmark.addCase("depth-4 chain, chunk " + rung.chunk() + " (" + rung.note()
          + "), null-free", it -> alignedChunks(b, chain4, rung.chunk(), chainOffsets, false));
      benchmark.addCase("depth-4 chain, chunk " + rung.chunk() + " (" + rung.note()
          + "), mixed nulls", it -> alignedChunks(b, chain4, rung.chunk(), chainOffsets, true));
    }
    // dayofweek as the second shape: its vector body is 20 ops but its scalar tail is one
    // Math.floorMod and two fixups per row, so the tail's share of the work is larger here
    // than on the chain - the shape most likely to make the tail visible at all.
    for (Rung rung : LADDER.subList(2, LADDER.size())) {
      benchmark.addCase("dayofweek, chunk " + rung.chunk() + " (" + rung.note()
          + "), null-free", it -> alignedChunks(b, dow, rung.chunk(), NO_LITERALS, false));
      benchmark.addCase("dayofweek, chunk " + rung.chunk() + " (" + rung.note()
          + "), mixed nulls", it -> alignedChunks(b, dow, rung.chunk(), NO_LITERALS, true));
    }
    // VARKA-32 step B1's own case for this ladder (VARKA-32.md section 7.1). Until B2
    // the two settings could differ only in the epilogue: the shipped grouping kept each
    // field in its own loop method whether or not shareChronoPrefix was set, and the
    // epilogue is the one method every output shares (VARKA-24) - invisible at chunk 4096,
    // which divides evenly at every lane count, so the unaligned arms here were the only
    // place in the file that timed it. Since B2 the shared arm holds the four fields in one
    // loop method as well, so the aligned rows show the loop's gain and the unaligned rows
    // add the epilogue's on top of it.
    List<VarkaVectorIR> fourFieldsCol = List.of(new Year(new ColumnRef(0)),
        new Month(new ColumnRef(0)), new DayOfMonth(new ColumnRef(0)),
        new Quarter(new ColumnRef(0)));
    VarkaFusedKernel fourFieldsUnshared = emit(fourFieldsCol, 1, 0, b.loader(), 710,
        VarkaEmitOptions.DEFAULTS.withShareChronoPrefix(false));
    VarkaFusedKernel fourFieldsShared = emit(fourFieldsCol, 1, 0, b.loader(), 711);
    VarkaFusedKernel fourFieldsPerGroup =
        emit(fourFieldsCol, 1, 0, b.loader(), 946, PER_GROUP_WRITE);
    long[] calDstData = addresses(b.wideDst());
    long[] calDstValidity = addresses(b.wideDstValidity());
    for (Rung rung : LADDER) {
      int chunk = rung.chunk();
      String note = rung.note();
      benchmark.addCase(
          "year+month+day+quarter, unshared, chunk " + chunk + " (" + note + "), null-free",
          it -> calendarChunks(b, calDstData, calDstValidity, fourFieldsUnshared, chunk,
              false));
      benchmark.addCase(
          "year+month+day+quarter, shared, chunk " + chunk + " (" + note + "), null-free",
          it -> calendarChunks(b, calDstData, calDstValidity, fourFieldsShared, chunk, false));
      // The masked arm, added as VARKA-70's baseline (VARKA-70.md 6, risk 2): the
      // short-batch rows are where a per-batch bitmap pass could cost more than the
      // per-group calls it replaces, and until now every four-field row here was null-free.
      benchmark.addCase(
          "year+month+day+quarter, shared, chunk " + chunk + " (" + note + "), mixed nulls",
          it -> calendarChunks(b, calDstData, calDstValidity, fourFieldsShared, chunk, true));
      // VARKA-70's A/B on the short batches risk 2 is about: at 64 rows the pass is an
      // 8-byte bitmap against four lane groups' calls.
      benchmark.addCase(
          "year+month+day+quarter, shared, words per group (VARKA-70 A/B), chunk " + chunk
              + " (" + note + "), mixed nulls",
          it -> calendarChunks(b, calDstData, calDstValidity, fourFieldsPerGroup, chunk,
              true));
    }
    runCases(benchmark);
  }

  /** The alignment ladder's walk: every call re-reads the same cache-warm prefix. */
  private static void alignedChunks(Buffers b, VarkaFusedKernel kernel, int chunk,
      int[] offsets, boolean mixed) {
    int pass = 0;
    while (pass < repeats) {
      int done = 0;
      while (done < numRows) {
        int n = Math.min(chunk, numRows - done);
        if (mixed) {
          kernel.run(new long[] {b.mxData().address()}, new long[] {b.mxValidity().address()},
              new int[] {nullsIn(n)}, new long[] {b.dst().address()},
              new long[] {b.dstValidity().address()}, offsets, n);
        } else {
          kernel.run(new long[] {b.nfData().address()}, new long[] {0L}, new int[] {0},
              new long[] {b.dst().address()}, new long[] {b.dstValidity().address()}, offsets,
              n);
        }
        done += n;
      }
      pass += 1;
    }
  }

  /** The four-field walk of the alignment ladder, into the four calendar destinations. */
  private static void calendarChunks(Buffers b, long[] calDstData, long[] calDstValidity,
      VarkaFusedKernel kernel, int chunk, boolean mixed) {
    int pass = 0;
    while (pass < repeats) {
      int done = 0;
      while (done < numRows) {
        int n = Math.min(chunk, numRows - done);
        int status;
        if (mixed) {
          status = kernel.run(new long[] {b.mxData().address()},
              new long[] {b.mxValidity().address()}, new int[] {nullsIn(n)}, calDstData,
              calDstValidity, NO_LITERALS, n);
        } else {
          status = kernel.run(new long[] {b.nfData().address()}, new long[] {0L},
              new int[] {0}, calDstData, calDstValidity, NO_LITERALS, n);
        }
        if (status != 0) {
          throw requirementFailed("the kernel declined a batch: status " + status);
        }
        done += n;
      }
      pass += 1;
    }
  }

  private void epilogueCrossing(Buffers b) {
    // The four-field ladder above never shows the crossing this task is actually about:
    // sixteen calendar outputs (four date columns) sit at 7531 bytes unshared - already
    // under the 8000-byte HugeMethodLimit, so sharing there has nothing to cross, and the
    // near-identical numbers above are the honest result of that. Five date columns of
    // four fields is twenty outputs, which VARKA-32.md's ladder measures at 9436 bytes
    // unshared - past the limit, so HotSpot compiles epilogueMasked at no tier at all and it
    // runs interpreted with boxed vectors on every batch whose length is not a lane
    // multiple - and 4048 bytes shared, comfortably under it. This section is where that
    // crossing is priced rather than inferred from bytecode size alone.
    int cols = 5;
    VarkaBenchmark benchmark = benchmark(
        (long) numRows * repeats + " rows, 20 calendar outputs over 5 dates",
        (long) numRows * repeats).timed().build();
    MemorySegment[] srcCols = new MemorySegment[cols];
    for (int c = 0; c < cols; c++) {
      srcCols[c] = fill(b.arena(), i -> false).data();
    }
    MemorySegment[] dstCols = allocateAll(b.arena(), 4 * cols, numRows * 4L);
    MemorySegment[] dstValCols = allocateAll(b.arena(), 4 * cols, (numRows + 7) / 8L);
    var roots20 = new ArrayList<VarkaVectorIR>();
    for (int c = 0; c < cols; c++) {
      var col = new ColumnRef(c);
      roots20.addAll(List.of(new Year(col), new Month(col), new DayOfMonth(col),
          new Quarter(col)));
    }
    // Both arms in the single-epilogue form (methodByteBudget 0): VARKA-87's default splits
    // the epilogue per group, under which neither arm crosses the limit and this section
    // would price nothing. VarkaMethodSizeBenchmark measures the split form against this one.
    VarkaEmitOptions single = VarkaEmitOptions.DEFAULTS.withMethodByteBudget(0);
    VarkaFusedKernel unshared20 = emit(List.copyOf(roots20), cols, 0, b.loader(), 900,
        single.withShareChronoPrefix(false));
    VarkaFusedKernel shared20 = emit(List.copyOf(roots20), cols, 0, b.loader(), 901, single);
    long[] srcValidityZero = new long[cols];
    int[] srcNullsZero = new int[cols];
    KernelAt chunked20 = (kernel, chunk) -> {
      int pass = 0;
      while (pass < repeats) {
        int done = 0;
        while (done < numRows) {
          int n = Math.min(chunk, numRows - done);
          long dataOff = done * 4L;
          long validityOff = done / 8L;
          int status = kernel.run(
              addressesShifted(srcCols, dataOff), srcValidityZero, srcNullsZero,
              addressesShifted(dstCols, dataOff), addressesShifted(dstValCols, validityOff),
              NO_LITERALS, n);
          if (status != 0) {
            throw requirementFailed("the kernel declined a batch: status " + status);
          }
          done += n;
        }
        pass += 1;
      }
    };
    // 4096/64 never reach the epilogue at all (every supported lane count divides them),
    // so they are the control: whatever the two settings cost there is the loop, not the
    // crossing. 4095/63 are where the crossing has to show up if it is real.
    for (Rung rung : LADDER) {
      benchmark.addCase("unshared (9436B epilogue, past HugeMethodLimit), chunk " + rung.chunk()
          + " (" + rung.note() + ")", it -> chunked20.run(unshared20, rung.chunk()));
      benchmark.addCase("shared (4048B epilogue, under HugeMethodLimit), chunk " + rung.chunk()
          + " (" + rung.note() + ")", it -> chunked20.run(shared20, rung.chunk()));
    }
    runCases(benchmark);
  }

  private static VarkaVectorIR dowAt(int k) {
    return new DayOfWeek(new AddDays(new ColumnRef(0), new LiteralSlot(k)));
  }

  private static VarkaVectorIR wideTree(int subtrees) {
    VarkaVectorIR node = dowAt(0);
    for (int i = 1; i < subtrees; i++) {
      node = i % 2 == 0 ? new Greatest(node, dowAt(i)) : new Least(node, dowAt(i));
    }
    return node;
  }

  private void widening(Buffers b) {
    // GROUP_BUDGET bounds ops *between* outputs and never inside one, so a single root can
    // emit an arbitrarily wide loop method. The budget's javadoc calls single-output loops
    // healthy "at every width tried" and the width tried was 59 ops; this is the ladder that
    // finds out where that stops being true (VARKA-43.md).
    //
    // The shape has to vary op count and nothing else, and the three obvious constructions
    // all fail: an AddDays chain varies dependency depth (VARKA-25's axis), repeated calendar
    // nodes get their prefixes shared by VARKA-32 step B1 - which is why section 2.16's own
    // example is 61 ops today rather than the ~190 it records - and repeated identical
    // subtrees are CSE'd away by emitValue. A greatest/least tree over independent
    // dayofweek(d + k) subtrees with distinct literal slots avoids all three, and measures
    // exactly linear at 19 ops per subtree.
    VarkaBenchmark benchmark = benchmark("one output over " + numRows + " rows, null-free", numRows)
        .timed().build();
    for (LadderPoint point : List.of(new LadderPoint(1, 20), new LadderPoint(3, 58),
        new LadderPoint(5, 96), new LadderPoint(8, 153), new LadderPoint(10, 191),
        new LadderPoint(13, 248))) {
      int subtrees = point.subtrees();
      VarkaVectorIR root = wideTree(subtrees);
      String name = "org.apache.spark.sql.varka.execution.VarkaFusedLadder" + subtrees;
      byte[] bytes = VarkaLoopEmitter.emit(name, List.of(root), 1, subtrees, null, null,
          VarkaEmitOptions.DEFAULTS);
      // Asserted rather than assumed: if a lowering change moves these, the x-axis of this
      // ladder has silently changed and every number below is about a different shape.
      int ops = VarkaEmitterTestSupport.invocationCount(bytes, "loopDense0",
          "jdk.incubator.vector.IntVector");
      require(ops == point.expectedOps(), "ladder point " + subtrees + " should emit "
          + point.expectedOps() + " IntVector ops, got " + ops);
      VarkaFusedKernel kernel = defineKernel(b.loader(), name, bytes);
      int[] offsets = offsets(subtrees, 13);
      int loopBytes = VarkaEmitterTestSupport.codeSize(bytes, "loopDense0");
      benchmark.addCase(ops + " ops in one loop method (" + loopBytes + " bytecode bytes)",
          it -> {
            int status = kernel.run(new long[] {b.nfData().address()}, new long[] {0L},
                new int[] {0}, new long[] {b.dst().address()},
                new long[] {b.dstValidity().address()}, offsets, numRows);
            if (status != 0) {
              throw requirementFailed("the kernel declined a batch: status " + status);
            }
          });
    }
    runCases(benchmark);
  }

  // ---------------------------------------------------------------------------------------
  // The constant division
  // ---------------------------------------------------------------------------------------

  private void constantDivision(Buffers b) {
    // The A/B `VARKA-88.md` 6 exists for. Every calendar division is a range-narrowed
    // magic multiply today, exact only because the emitter proves the dividend bounded; the
    // two double forms convert into double lanes instead, divide there and convert back,
    // which is exact over the whole lane and needs neither the bound nor the round-down
    // carry that follows a magic quotient. Seven operations against two, and the seven move
    // off the 32-bit multiplier - so whether the trade wins is a property of the machine's
    // vector divider, which is what this measures rather than argues.
    //
    // Three shapes, chosen to bracket what a division is worth rather than to enumerate:
    // `year` pays the prefix and nothing else, so its three divisions are the largest share
    // of a body any extraction has; the four-field shared shape pays the prefix once and
    // three tails after it, so the same three divisions are the smallest share; and
    // `add_months` reaches the four division sites no extraction does, inside a
    // recomposition. Adjacent cases, one form after another on the same shape, the same
    // interleaving discipline as VARKA-45, VARKA-48 and VARKA-53.
    //
    // Null-free only, deliberately. The division sits in the arithmetic and the masked body
    // divides identically, so a mixed-null arm would price VARKA-45's validity machinery a
    // second time under a name that says "division"; the shipped `year` rows above already
    // carry both patterns for the shape.
    //
    // DOUBLE_RECIP is not uniformly reachable: `verify_double_division.py` proves the
    // reciprocal inexact for /146097 over the era step's dividend, so that one site falls
    // back to the magic under this setting while the other two divisions convert. The row
    // is named for what it is rather than for what it asks for.
    VarkaBenchmark benchmark = benchmark("constant division over " + numRows + " rows, null-free",
        numRows).timed().build();
    VarkaEmitOptions magic = VarkaEmitOptions.DEFAULTS;
    VarkaEmitOptions trueDiv =
        VarkaEmitOptions.DEFAULTS.withDivision(VarkaEmitOptions.Division.DOUBLE_DIV);
    VarkaEmitOptions recip =
        VarkaEmitOptions.DEFAULTS.withDivision(VarkaEmitOptions.Division.DOUBLE_RECIP);
    List<DivisionShape> divisionShapes = List.of(
        new DivisionShape("year (three divisions, the whole body)",
            List.of(new Year(new ColumnRef(0))), 0),
        new DivisionShape("four fields shared (three divisions, one prefix, three tails)",
            List.of(new Year(new ColumnRef(0)), new Month(new ColumnRef(0)),
                new DayOfMonth(new ColumnRef(0)), new DayOfYear(new ColumnRef(0))), 0),
        new DivisionShape("add_months (the recomposing shape, four division sites)",
            List.of(new AddMonths(new ColumnRef(0), new LiteralSlot(0))), 1));
    for (int shape = 0; shape < divisionShapes.size(); shape++) {
      DivisionShape s = divisionShapes.get(shape);
      List<Setting> forms = List.of(new Setting("magic multiply (shipped)", magic),
          new Setting("double lane, true divide", trueDiv),
          new Setting("double lane, reciprocal where admitted", recip));
      for (int variant = 0; variant < forms.size(); variant++) {
        Setting form = forms.get(variant);
        VarkaFusedKernel kernel =
            emit(s.roots(), 1, s.lits(), b.loader(), 974 + shape * 3 + variant,
                form.options());
        int[] offsets = s.lits() == 0 ? NO_LITERALS : new int[] {13};
        // One destination per root, taken from the shape rather than assumed: the
        // four-field shape writes four columns, and a single-destination call reaches the
        // kernel as an index out of bounds rather than as a wrong answer.
        long[] dstAddrs = addresses(Arrays.copyOf(b.wideDst(), s.roots().size()));
        long[] dstValAddrs = addresses(Arrays.copyOf(b.wideDstValidity(), s.roots().size()));
        benchmark.addCase(s.label() + ": " + form.label(), it -> {
          int status = kernel.run(new long[] {b.nfData().address()}, new long[] {0L},
              new int[] {0}, dstAddrs, dstValAddrs, offsets, numRows);
          if (status != 0) {
            throw requirementFailed("the kernel declined a batch: status " + status);
          }
        });
      }
    }
    runCases(benchmark);
  }

  private void extractYear(Buffers b) {
    // `extract(YEAR FROM ym)` is a month count divided by twelve over a column nothing
    // bounds, and the calendar's range-narrowed magic is exact over about one
    // forty-thousandth of int32, so the calendar's lowering is not available and the
    // comparand is the scalar division the row engine performs (prediction 3 of
    // `VARKA-88.md` 6.1). VARKA-88 gave it the conversion through double lanes; task
    // 149 the multiply-high through 64-bit lanes, which is what a scalar compiler emits
    // for the same division and what the scalar loop below is in fact running.
    //
    // The column spans signed int32 rather than the epoch days the other blocks use,
    // because that is the range the shape actually sees and the reason the magic declines
    // it.
    //
    // The scalar case is a floor on the row engine's cost, not the row engine: it is the
    // arithmetic alone, with none of the per-row dispatch around it, and this file's
    // sibling case is named "cannot auto-vectorize" precisely because a tight loop like
    // this one may be vectorised by C2 - which is not established here, and would be the
    // JVM's own output to establish rather than a ratio's to imply. So the row bounds the
    // advantage from below and `VARKA-88.md` 6.1's prediction 3, which is stated
    // against the row engine, needs a row-engine comparand to be scored.
    MemorySegment months = b.arena().allocate(numRows * 4L, 8);
    int step = (int) ((1L << 32) / numRows);
    for (int i = 0; i < numRows; i++) {
      months.set(ValueLayout.JAVA_INT, i * 4L, (int) ((long) Integer.MIN_VALUE + (long) i * step));
    }
    VarkaBenchmark benchmark =
        benchmark("extract(YEAR FROM ym) over " + numRows + " rows, null-free",
            numRows).timed().build();
    // VARKA-149 gave this division a second lowering, the multiply-high through 64-bit
    // lanes, and made it the default: the conversion form stays as the reference arm
    // behind `mulHiDivide`, so the two are adjacent here and the scalar loop is the
    // comparand for both. The shipped case keeps its id; its bytes changed, which is the
    // point of the row.
    List<VarkaVectorIR> divide = List.of(new ConstDivide(new ColumnRef(0), 12));
    VarkaFusedKernel mulHi = emit(divide, 1, 0, b.loader(), 983);
    VarkaFusedKernel converting = emit(divide, 1, 0, b.loader(), 984,
        VarkaEmitOptions.DEFAULTS.withMulHiDivide(false));
    benchmark.addCase("multiply-high through 64-bit lanes (shipped)", it -> {
      int status = mulHi.run(new long[] {months.address()}, new long[] {0L}, new int[] {0},
          new long[] {b.dst().address()}, new long[] {b.dstValidity().address()},
          NO_LITERALS, numRows);
      if (status != 0) {
        throw requirementFailed("the kernel declined a batch: status " + status);
      }
    });
    benchmark.addCase("double lane, true divide: the conversion form it replaced", it -> {
      int status = converting.run(new long[] {months.address()}, new long[] {0L},
          new int[] {0}, new long[] {b.dst().address()},
          new long[] {b.dstValidity().address()}, NO_LITERALS, numRows);
      if (status != 0) {
        throw requirementFailed("the kernel declined a batch: status " + status);
      }
    });
    benchmark.addCase("scalar loop, one division per row: a floor on the row engine", it -> {
      int i = 0;
      while (i < numRows) {
        b.dst().set(ValueLayout.JAVA_INT, i * 4L,
            months.get(ValueLayout.JAVA_INT, i * 4L) / 12);
        i += 1;
      }
    });
    runCases(benchmark);
  }

  // ---------------------------------------------------------------------------------------
  // VARKA-313: what exact refusal status costs
  // ---------------------------------------------------------------------------------------

  /** The section's title, which {@code dev/varka_bench_regen.sh --sections} selects it by. */
  static final String EXACT_STATUS_SECTION =
      "VARKA-313: exact refusal status, a mask per reason against one folded mask";

  /**
   * The cost of {@code exactRefusalStatus}, which decides whether it becomes the default
   * ({@code VARKA-313.md} 3.2). {@code year(date_add(d, i + i))} under ANSI holds two guards of
   * two reasons in one loop method - the sum's overflow and the day {@code year} reads - so the
   * default folds them into one mask and the option keeps a mask per reason: two more
   * {@code VectorMask} calls and 18 more bytes in each loop method, by {@code dev/varka_emit.sh}.
   * The data keeps every lane in range, so no batch declines and the rows time the extra mask
   * alone; the setup checks that, since a declining kernel would time a different path.
   */
  private void exactRefusalStatus(Buffers b) {
    VarkaBenchmark benchmark = benchmark("year(date_add(d, i + i)) under ANSI over " + numRows
        + " rows", numRows).timed().build();
    List<VarkaVectorIR> roots = List.of(new Year(new AddDays(new ColumnRef(0),
        new IntArith(IntOp.ADD, Overflow.FAIL, new ColumnRef(1), new ColumnRef(1)))));
    VarkaFusedKernel folded = emit(roots, 2, 0, b.loader(), 985);
    VarkaFusedKernel exact = emit(roots, 2, 0, b.loader(), 986,
        VarkaEmitOptions.DEFAULTS.withExactRefusalStatus(true));
    long[] nfData = {b.nfData().address(), b.nf2Data().address()};
    long[] mxData = {b.mxData().address(), b.mx2Data().address()};
    long[] mxValidity = {b.mxValidity().address(), b.mx2Validity().address()};
    int[] mxNulls = {b.mxNulls(), b.mx2Nulls()};
    long[] dst = {b.dst().address()};
    long[] dstValidity = {b.dstValidity().address()};
    for (VarkaFusedKernel kernel : List.of(folded, exact)) {
      int dense = kernel.run(nfData, new long[] {0L, 0L}, new int[] {0, 0}, dst, dstValidity,
          NO_LITERALS, numRows);
      int masked = kernel.run(mxData, mxValidity, mxNulls, dst, dstValidity, NO_LITERALS,
          numRows);
      require(dense == 0 && masked == 0, "the exact-status section's data declined (status "
          + dense + ", " + masked + "); its rows would time the decline, not the masks");
    }
    benchmark.addCase("one folded mask (the default), null-free", it ->
        folded.run(nfData, new long[] {0L, 0L}, new int[] {0, 0}, dst, dstValidity,
            NO_LITERALS, numRows));
    benchmark.addCase("a mask per reason (exactRefusalStatus), null-free", it ->
        exact.run(nfData, new long[] {0L, 0L}, new int[] {0, 0}, dst, dstValidity,
            NO_LITERALS, numRows));
    benchmark.addCase("one folded mask (the default), mixed nulls", it ->
        folded.run(mxData, mxValidity, mxNulls, dst, dstValidity, NO_LITERALS, numRows));
    benchmark.addCase("a mask per reason (exactRefusalStatus), mixed nulls", it ->
        exact.run(mxData, mxValidity, mxNulls, dst, dstValidity, NO_LITERALS, numRows));
    runCases(benchmark);
  }
}
