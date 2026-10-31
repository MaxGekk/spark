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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;

import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitterTestSupport;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaFusedKernel;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaLoopEmitter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSegments;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaTierWitness;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ConstDivide;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntArith;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.ArrowColumnVector;

import static org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_DAY;
import static org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MICROS;
import static org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND;
import static org.apache.spark.sql.catalyst.util.DateTimeConstants.SECONDS_PER_MINUTE;

/**
 * What a decimal output costs to write before the kernel can store one itself: the baseline of
 * row 157's widening store ({@code VARKA-157.md} 4), on the code as it is.
 *
 * <p>An Arrow decimal column holds sixteen bytes a row at every precision, the unscaled value's
 * low word {@code u} and then its sign extension {@code u >> 63} ({@code VARKA-157.md} 0, item 1).
 * A kernel today stores a long-lane value as a {@code BIGINT}, eight bytes a row, so the form that
 * needs no emitter change is that store followed by a scalar loop that widens each long into the
 * decimal vector's slots, as an evaluator would per batch ({@code m5/PLAN.md} 2.93). Two arms per
 * shape, each its own kernel class:
 * <ul>
 *   <li><b>the {@code BIGINT} store</b>, the control: the value's eight bytes a row, which is what
 *     the long lane writes for any long output;
 *   <li><b>the {@code BIGINT} store, then the scalar widening</b>: the same kernel, then {@link
 *     #widen} writing {@code u} and {@code u >> 63} into a {@link DecimalVector}'s data buffer,
 *     the kernel having written the validity into the vector's own validity buffer. This is the
 *     baseline the store has to beat.
 * </ul>
 * The store's own arms, one per arm of its {@code decimalStore} option, join these with 157b.
 *
 * <p><b>The shapes.</b> The compiler declines both {@code TIME} producers today
 * ({@code VarkaTimeCompiler}, {@code getSecondsOfTimeWithFraction} and {@code timeToSeconds}), so
 * their long-lane values are built here as IR, as {@code VARKA-157.md} 2.3 writes the lowerings,
 * without the {@code WidenDecimal} root that does not exist yet. The constants are long literal
 * slots ({@link #LONG_LITS}), as the compiler passes them:
 * <ul>
 *   <li>{@code time_to_seconds(t)}, a {@code DECIMAL(14, 6)}: {@code ConstDivide(t + 500, 1000)},
 *     the dividend under {@code NANOS_PER_DAY + 500};
 *   <li>{@code extract(SECOND FROM t)} at {@code TIME(6)}, a {@code DECIMAL(8, 6)}: {@code s *
 *     10^6 + g}, with {@code q = ConstDivide(t, 10^9)} under a day, {@code s = q - (q / 60) * 60}
 *     the compiler's {@code second(t)}, {@code r = t - q * 10^9} sharing {@code q}, and {@code g =
 *     ConstDivide(r, 1000)} under a second;
 *   <li>the store alone: the long column itself as the root, stored as a {@code DECIMAL(18, 0)}.
 * </ul>
 * Each kernel's values are checked against Spark's own row functions ({@code
 * DateTimeUtils.timeToSeconds} and {@code getSecondsOfTimeWithFraction}) on every row of a rung,
 * on its first call and again once it is compiled, and the widened vector is read back through
 * {@link ArrowColumnVector}, whose small-decimal reader rejects a wrong high word.
 *
 * <p><b>The column</b> is a {@code TIME(6)}, nanoseconds of the day at whole microseconds, spread
 * over the whole day by a multiplicative hash as {@code VarkaTimeBenchmark}'s are, the first row
 * midnight and the last the day's last microsecond; no nulls.
 *
 * <p><b>The rungs</b> are three of {@code VarkaTimeBenchmark}'s: in L2, in L3 and past L3. The
 * widening arm reads eight bytes and writes eight and then reads eight and writes sixteen a row,
 * against the control's eight and eight.
 *
 * <p><b>Compiled code first.</b> Every arm is driven at its own turn, just before it is timed, at
 * least eight times with C2 idle after each drive, until two drives take the same time, every
 * dense loop method of its kernel is C2 code and, for the widening arm, {@link #widen} is level-4
 * code in {@code Compiler.codelist} (row 407's rule, {@code VARKA-347.md} 0, item 13). The loops
 * are read through {@link VarkaTierWitness}, whose JFR recording runs from before the first
 * kernel call to the end of the run, since a loop C2 inlined into its caller has no code of its
 * own in the list. Its tier state is written as a line beside its table; an arm that does not
 * settle in 60 seconds says so there and is timed as it is.
 *
 * <pre>
 *   build/sbt "catalyst/Test/runMain org.apache.spark.sql.VarkaDecimalStoreBenchmark"
 *   dev/varka_bench_regen.sh catalyst VarkaDecimalStoreBenchmark
 * </pre>
 */
public class VarkaDecimalStoreBenchmark extends VarkaBenchmarkBase {

  /** A rung of the ladder: a row count and what it is for. */
  private record Rung(int numRows, String level) { }

  /**
   * A shape priced: its name, its long-lane value, the decimal type it is stored as, and Spark's
   * unscaled answer for a nanosecond of the day.
   */
  record Shape(String name, VarkaVectorIR root, int numLiterals, int precision, int scale,
      LongUnaryOperator spark) { }

  /** One timed arm: its label, its kernel, and whether it widens into the decimal vector. */
  private record Arm(String label, VarkaFusedKernel kernel, boolean widens) { }

  /**
   * How an arm's warm-up ended, written beside its number: the dense loops' levels, which decide
   * it, and every compiled method of the kernel class with its level, which shows a loop C2
   * inlined into its caller (no version of its own, the caller at level 4).
   */
  private record TierState(boolean settled, int drives, Map<String, Integer> loopLevels,
      Map<String, Integer> kernelLevels, int widenLevel) { }

  private static final List<Rung> LADDER = List.of(
      new Rung(16384, "every arm in L2"),
      new Rung(262144, "every arm in L3"),
      new Rung(8388608, "every arm past L3"));

  /** Rows a timed iteration covers, so that the L2 rung's iteration is long enough to time. */
  private static final long ROWS_PER_ITERATION = 1L << 24;

  private static final int MIN_WARM_DRIVES = 8;
  private static final double STEADY_FRACTION = 0.3;
  private static final long WARM_WAIT_MILLIS = 60_000;
  private static final long IDLE_WAIT_MILLIS = 10_000;

  // ---------------------------------------------------------------------------------------
  // The shapes, as VARKA-157.md 2.3 lowers them
  // ---------------------------------------------------------------------------------------

  /** The long literal slots every TIME shape reads: 500, 60, 10^9 and 10^6. */
  static final long[] LONG_LITS = {500L, SECONDS_PER_MINUTE, NANOS_PER_SECOND, 1_000_000L};

  private static final int[] NO_INT_LITS = new int[0];

  private static LiteralSlot slot(int index) {
    return new LiteralSlot(index, LaneType.LONG);
  }

  private static VarkaVectorIR add(VarkaVectorIR a, VarkaVectorIR b) {
    return new IntArith(IntOp.ADD, Overflow.WRAP, a, b);
  }

  private static VarkaVectorIR sub(VarkaVectorIR a, VarkaVectorIR b) {
    return new IntArith(IntOp.SUB, Overflow.WRAP, a, b);
  }

  private static VarkaVectorIR mul(VarkaVectorIR a, VarkaVectorIR b) {
    return new IntArith(IntOp.MUL, Overflow.WRAP, a, b);
  }

  /** {@code time_to_seconds(t)}'s unscaled value: {@code (t + 500) / 1000}. */
  static VarkaVectorIR timeToSeconds() {
    var t = new ColumnRef(0, LaneType.LONG);
    return new ConstDivide(add(t, slot(0)), 1000L, NANOS_PER_DAY + 500L);
  }

  /**
   * {@code extract(SECOND FROM t)}'s unscaled value at {@code TIME(6)}: {@code s * 10^6 + g}, the
   * whole seconds within the minute and the microseconds within the second.
   */
  static VarkaVectorIR secondWithFraction() {
    var t = new ColumnRef(0, LaneType.LONG);
    var q = new ConstDivide(t, NANOS_PER_SECOND, NANOS_PER_DAY);
    var s = sub(q, mul(new ConstDivide(q, SECONDS_PER_MINUTE, q.quotientBound()), slot(1)));
    var r = sub(t, mul(q, slot(2)));
    var g = new ConstDivide(r, 1000L, NANOS_PER_SECOND);
    return add(mul(s, slot(3)), g);
  }

  /** The three shapes, each with Spark's answer as its oracle. */
  static List<Shape> shapes() {
    return List.of(
        new Shape("time_to_seconds(t), DECIMAL(14, 6)", timeToSeconds(), LONG_LITS.length, 14,
            6, n -> DateTimeUtils.timeToSeconds(n).toUnscaledLong()),
        new Shape("extract(SECOND FROM t) at TIME(6), DECIMAL(8, 6)", secondWithFraction(),
            LONG_LITS.length, 8, 6,
            n -> DateTimeUtils.getSecondsOfTimeWithFraction(n, 6).toUnscaledLong()),
        new Shape("the store alone over a long column, DECIMAL(18, 0)",
            new ColumnRef(0, LaneType.LONG), 0, 18, 0, n -> n));
  }

  /**
   * The {@code TIME(6)} value of row {@code i} of {@code numRows}: whole microseconds of the day,
   * hashed over the day, midnight first and the day's last microsecond last.
   */
  static long timeOfRow(int i, int numRows) {
    if (i == numRows - 1) {
      return NANOS_PER_DAY - NANOS_PER_MICROS;
    }
    long micros = Long.remainderUnsigned(i * 0x9E3779B97F4A7C15L, NANOS_PER_DAY / NANOS_PER_MICROS);
    return micros * NANOS_PER_MICROS;
  }

  // ---------------------------------------------------------------------------------------
  // The scalar widening, the baseline arm's second pass
  // ---------------------------------------------------------------------------------------

  /**
   * Writes each of {@code numRows} longs of {@code src} into the sixteen-byte slots of {@code
   * slots}: the value at {@code 16 i} and its sign extension at {@code 16 i + 8}, little-endian,
   * which is what {@code DecimalVector.set(int, long)} writes into a slot.
   */
  static void widen(MemorySegment src, MemorySegment slots, int numRows) {
    for (int i = 0; i < numRows; i++) {
      long u = src.get(ValueLayout.JAVA_LONG_UNALIGNED, i * 8L);
      slots.set(ValueLayout.JAVA_LONG_UNALIGNED, i * 16L, u);
      slots.set(ValueLayout.JAVA_LONG_UNALIGNED, i * 16L + 8, u >> 63);
    }
  }

  // ---------------------------------------------------------------------------------------
  // Emitting the kernels
  // ---------------------------------------------------------------------------------------

  private int nextId = 0;

  /** Emits {@code shape}'s kernel under the next case id, claimed so a reused one fails. */
  private VarkaFusedKernel emit(Shape shape, VarkaGeneratedClassLoader loader) {
    int n = nextId;
    nextId += 1;
    claimCaseId(n);
    String name = "org.apache.spark.sql.varka.execution.VarkaDecimalStoreBench" + n;
    return defineKernel(loader, name, VarkaLoopEmitter.emit(name, List.of(shape.root()), 1,
        shape.numLiterals(), null, null, VarkaEmitOptions.DEFAULTS));
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    var loader = new VarkaGeneratedClassLoader(getClass().getClassLoader());
    try {
      // Every kernel once, before the ladder: a kernel does not depend on the row count.
      var shapes = shapes();
      var arms = new ArrayList<List<Arm>>();
      for (Shape shape : shapes) {
        arms.add(List.of(
            new Arm("BIGINT store, 8 bytes a row (the control)", emit(shape, loader), false),
            new Arm("BIGINT store, then a scalar loop widening it into a DecimalVector",
                emit(shape, loader), true)));
      }
      reportCaseIds();
      if (dryRun()) {
        return;
      }
      // The witness records from before any kernel runs, so it knows every compile of each
      // kernel class: which are on-stack replacements, and which loops C2 inlined into a caller.
      var witness = VarkaTierWitness.get();
      for (List<Arm> shapeArms : arms) {
        for (Arm arm : shapeArms) {
          witness.watch(arm.kernel().getClass().getName());
        }
      }
      section("decimal outputs before the widening store", () -> {
        for (Rung rung : LADDER) {
          runRung(rung, shapes, arms);
        }
      });
    } finally {
      VarkaTierWitness.close();
      loader.release();
    }
  }

  // ---------------------------------------------------------------------------------------
  // One rung
  // ---------------------------------------------------------------------------------------

  /** A rung's buffers for one arm, and the call that runs it over them. */
  private record Buffers(VarkaFusedKernel kernel, long[] src, long[] dsts, long[] validity,
      MemorySegment longs, MemorySegment slots, int numRows) {

    private static final long[] NO_VALIDITY = {0L};
    private static final int[] NO_NULLS = {0};

    /** One batch: the kernel, then the widening if the arm has one. */
    void runOnce() {
      int status = kernel.run(src, NO_VALIDITY, NO_NULLS, dsts, validity, NO_INT_LITS,
          LONG_LITS, numRows);
      if (status != 0) {
        throw requirementFailed("the kernel declined a batch: status " + status);
      }
      if (slots != null) {
        widen(longs, slots, numRows);
      }
    }

    void drive(int reps) {
      for (int r = 0; r < reps; r++) {
        runOnce();
      }
    }
  }

  private void runRung(Rung rung, List<Shape> shapes, List<List<Arm>> arms) {
    int n = rung.numRows();
    int reps = (int) Math.max(1, ROWS_PER_ITERATION / n);
    try (Arena arena = Arena.ofConfined();
         BufferAllocator allocator = ArrowUtils.rootAllocator()
             .newChildAllocator("varka-decimal-store-bench", 0, Long.MAX_VALUE)) {
      MemorySegment column = arena.allocate(n * 8L, 64);
      for (int i = 0; i < n; i++) {
        column.set(ValueLayout.JAVA_LONG, i * 8L, timeOfRow(i, n));
      }
      var benchmark = benchmark("decimal outputs over " + n + " rows - " + rung.level(),
          (long) n * reps).timed().build();
      var vectors = new ArrayList<DecimalVector>();
      var states = new ArrayList<String[]>();
      try {
        for (int s = 0; s < shapes.size(); s++) {
          Shape shape = shapes.get(s);
          long[] expected = new long[n];
          for (int i = 0; i < n; i++) {
            expected[i] = shape.spark().applyAsLong(timeOfRow(i, n));
          }
          for (Arm arm : arms.get(s)) {
            String label = shape.name() + ": " + arm.label();
            MemorySegment longs = arena.allocate(n * 8L, 64);
            DecimalVector vector = null;
            MemorySegment slots = null;
            long validity;
            if (arm.widens()) {
              vector = new DecimalVector("d", allocator, shape.precision(), shape.scale());
              vectors.add(vector);
              vector.allocateNew(n);
              require(vector.getDataBuffer().capacity() >= n * 16L,
                  "the decimal vector holds fewer than " + n + " slots");
              slots = VarkaSegments.map(vector.getDataBuffer().memoryAddress(), n * 16L);
              validity = vector.getValidityBuffer().memoryAddress();
            } else {
              validity = arena.allocate((n + 63) / 64 * 8L, 64).address();
            }
            var buffers = new Buffers(arm.kernel(), new long[] {column.address()},
                new long[] {longs.address()}, new long[] {validity}, longs, slots, n);
            buffers.runOnce();
            check(label + " (first call)", buffers, expected, vector, shape);
            DecimalVector checked = vector;
            String[] state = {label, null};
            states.add(state);
            boolean[] settled = new boolean[1];
            benchmark.addTimerCase(label, timer -> {
              if (!settled[0]) {
                settled[0] = true;
                TierState t = driveToCompiledCode(label, buffers, reps);
                state[1] = describe(t, arm.widens());
                check(label + " (compiled)", buffers, expected, checked, shape);
              }
              timer.startTiming();
              buffers.drive(reps);
              timer.stopTiming();
            });
          }
        }
        runCases(benchmark);
        for (String[] state : states) {
          if (state[1] != null) {
            note("tier at " + n + " rows, " + state[0] + ": " + state[1]);
          }
        }
      } finally {
        for (DecimalVector v : vectors) {
          v.close();
        }
      }
    }
  }

  /**
   * Checks an arm's output against Spark's answer: every row's long, and for the widening arm
   * every slot's high word and, on up to 4096 rows spread over the rung, the value Spark's column
   * reader gives for the slot.
   */
  private static void check(String label, Buffers b, long[] expected, DecimalVector vector,
      Shape shape) {
    int n = b.numRows();
    for (int i = 0; i < n; i++) {
      long got = b.longs().get(ValueLayout.JAVA_LONG, i * 8L);
      if (got != expected[i]) {
        throw requirementFailed(label + ": row " + i + " (t = " + timeOfRow(i, n) + ") is "
            + got + ", Spark's unscaled value is " + expected[i]);
      }
    }
    if (vector == null) {
      return;
    }
    for (int i = 0; i < n; i++) {
      long lo = b.slots().get(ValueLayout.JAVA_LONG_UNALIGNED, i * 16L);
      long hi = b.slots().get(ValueLayout.JAVA_LONG_UNALIGNED, i * 16L + 8);
      if (lo != expected[i] || hi != expected[i] >> 63) {
        throw requirementFailed(label + ": slot " + i + " holds (" + lo + ", " + hi + ")");
      }
    }
    vector.setValueCount(n);
    var reader = new ArrowColumnVector(vector);
    int stride = Math.max(1, n / 4096);
    for (int i = 0; i < n; i += stride) {
      require(!reader.isNullAt(i), label + ": slot " + i + " reads as null");
      long got = reader.getDecimal(i, shape.precision(), shape.scale()).toUnscaledLong();
      require(got == expected[i], label + ": Spark's reader gives " + got + " for slot " + i);
    }
  }

  // ---------------------------------------------------------------------------------------
  // Driving an arm to compiled code
  // ---------------------------------------------------------------------------------------

  /**
   * Each method of {@code className} that has compiled code in use, with its highest level, as
   * the code list shows it now. An on-stack-replacement version is not told from a standard one
   * here, and a method C2 inlined into its caller has no version of its own, so a missing method
   * can mean inlined.
   */
  private static Map<String, Integer> codeLevels(String className) {
    var levels = new TreeMap<String, Integer>();
    for (VarkaTierWitness.CodeEntry e : VarkaTierWitness.parse(
        VarkaTierWitness.readCodeList(), className)) {
      if (e.inUse()) {
        levels.merge(e.method(), e.level(), Math::max);
      }
    }
    return levels;
  }

  /**
   * Drives an arm until the timed run will measure C2's code: at least {@link #MIN_WARM_DRIVES}
   * drives, C2 idle after the last, {@link #widen} at level 4 for the widening arm, the last two
   * drives within {@link #STEADY_FRACTION} of each other, and then, read by the tier witness once
   * its recording has caught up, every dense loop method of the kernel C2 code: a level-4
   * version of its own that is not an on-stack replacement, or inlined into a level-4 caller. The
   * code list alone cannot tell the second: a loop whose code was thrown away and that C2 then
   * compiled only inside its caller has no version of its own, which the past-L3 rung showed on
   * five of six arms in a first laptop run. Bounded by {@link #WARM_WAIT_MILLIS}; an arm that
   * does not settle says so and is timed as it is.
   */
  private static TierState driveToCompiledCode(String label, Buffers b, int reps) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WARM_WAIT_MILLIS);
    Class<?> kernelClass = b.kernel().getClass();
    Set<String> loops = VarkaEmitterTestSupport.loopLevels(kernelClass).keySet();
    var witness = VarkaTierWitness.get();
    boolean widens = b.slots() != null;
    long previous = 0;
    int drives = 0;
    while (true) {
      long start = System.nanoTime();
      b.drive(reps);
      long took = System.nanoTime() - start;
      drives++;
      boolean idle = VarkaEmitterTestSupport.awaitC2Idle(IDLE_WAIT_MILLIS);
      int widenLevel = widens
          ? codeLevels(VarkaDecimalStoreBenchmark.class.getName()).getOrDefault("widen", 0)
          : 4;
      boolean steady = previous > 0 && Math.abs(took - previous) <= previous * STEADY_FRACTION;
      Map<String, Integer> levels = Map.of();
      if (drives >= MIN_WARM_DRIVES && idle && widenLevel == 4 && steady) {
        // A second or so for the recording to deliver what the drives compiled, paid only once
        // the cheap conditions hold.
        witness.settle();
        levels = witness.levelsNow(kernelClass.getName(), loops);
        if (VarkaEmitterTestSupport.allLoopsC2(levels)) {
          return new TierState(true, drives, levels, codeLevels(kernelClass.getName()),
              widenLevel);
        }
      }
      if (System.nanoTime() > deadline) {
        System.out.println("VarkaDecimalStoreBenchmark: " + label + " at " + b.numRows()
            + " rows did not settle in " + WARM_WAIT_MILLIS + " ms (the last drive "
            + TimeUnit.NANOSECONDS.toMicros(took) + " us after "
            + TimeUnit.NANOSECONDS.toMicros(previous) + " us, C2 " + (idle ? "idle" : "busy")
            + "); it is timed as it is");
        witness.settle();
        return new TierState(false, drives, witness.levelsNow(kernelClass.getName(), loops),
            codeLevels(kernelClass.getName()), widenLevel);
      }
      previous = took;
    }
  }

  private static String describe(TierState t, boolean widens) {
    return (t.settled() ? "settled" : "NOT settled") + " after " + t.drives() + " drives, loop "
        + "levels " + t.loopLevels() + (widens ? ", widen level " + t.widenLevel() : "")
        + ", kernel code " + t.kernelLevels();
  }
}
