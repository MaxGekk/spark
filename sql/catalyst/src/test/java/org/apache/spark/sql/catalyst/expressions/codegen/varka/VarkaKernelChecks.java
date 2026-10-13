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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Cond;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.NarrowLane;

/**
 * The fuzzers' row-by-row check of one emitted kernel against {@link VarkaReference}, at the int
 * lane and at the long lane, shared by the IR fuzzer's drawn, long and wide shapes, the IR series,
 * the guard edge suite, the variants and the composition fuzzer's compiled kernels (VARKA-238,
 * VARKA-289). {@code VarkaKernelCheck} is its Scala face (VARKA-311).
 *
 * <p>A check that fails throws an {@link IllegalStateException} whose message starts with the
 * batch's context and then names the mismatch - "output 2 row 7 differs (want 12, got 13)" -
 * which is the order {@code VarkaFailureSignature} classifies by. A second line carries the row's
 * inputs and the body that ran, so a failure can often be read from its message alone; it is a
 * line of its own because a signature reads only the first, and a null pattern in it would split
 * one bug into many and stop the shrinker changing the batch. The ScalaTest assertions this
 * replaced put their own comparison text first, so every such failure fell through to the
 * exception's class.
 */
public final class VarkaKernelChecks {

  private VarkaKernelChecks() {
  }

  /**
   * One batch's columns: {@code length} rows, each column's null flags ({@code nulls[c][i]}) and
   * drawn values, and whether the masked body is forced by reporting a null over a full bitmap.
   */
  public record Batch(int length, boolean[][] nulls, int[][] data, boolean forceMasked) {
    Batch withNulls(boolean[][] nulls, boolean forceMasked) {
      return new Batch(length, nulls, data, forceMasked);
    }
  }

  /** {@link Batch} at the long lane: the same, with 64-bit column values. */
  public record LongBatch(int length, boolean[][] nulls, long[][] data, boolean forceMasked) {
    LongBatch withNulls(boolean[][] nulls, boolean forceMasked) {
      return new LongBatch(length, nulls, data, forceMasked);
    }
  }

  /** The body a batch did not take: its null flags and whether the masked body is forced. */
  public record OtherBody(boolean[][] nulls, boolean forceMasked) { }

  /** Spark's answer for one output on one row (VARKA-276): a value, a null, or what it raised. */
  public sealed interface Answer {
    /** A value, a predicate's {@code true} and {@code false} as 1 and 0. */
    record Value(long v) implements Answer { }

    record Null() implements Answer { }

    /** The error Spark raised on the row, where a kernel declines the batch instead. */
    record Raised(Throwable e) implements Answer { }
  }

  /** Spark's answers for a kernel's outputs, as {@code VarkaSparkOracle} gives them. */
  public interface SparkAnswers {
    Answer answer(int output, long[] values, boolean[] isNull);

    /** Output {@code output}'s expression, for a failure message. */
    String sql(int output);
  }

  /** Answers the reference evaluator has been held to Spark on, so a suite can see it was. */
  public static final AtomicLong SPARK_COMPARISONS = new AtomicLong();

  /**
   * A validity bitmap's size as Arrow Java's {@code allocateNew} gives it, in whole 64-bit words:
   * the kernels write and read a word at a time, so a buffer of the nominal
   * {@code (length + 7) / 8} bytes would be one a kernel is not entitled to, and the memory
   * sanitizer (VARKA-263) says so.
   */
  private static long wordBytes(int length) {
    return ((length + 63) / 64) * 8L;
  }

  /** What a kernel's {@code run} is handed besides its buffers: the scratch address. */
  private interface Run {
    int apply(long scratch);
  }

  /**
   * Runs {@code run} with the scratch address the kernel takes, inside the memory sanitizer's
   * window over the buffers this harness allocated, so that every mapping the kernel makes is
   * checked against them when the sanitizer is on. {@code stride} is a value's bytes in the lane.
   * Nothing is registered, and nothing changes, when it is off.
   */
  private static int sanitized(VarkaFusedKernel kernel, int length, long stride, long[] srcData,
      long[] srcValidity, long[] outData, long[] outValidity, Run run) {
    long scratch = VarkaEmitterTestSupport.scratch(kernel, length);
    VarkaMemorySanitizer.begin();
    try {
      if (VarkaMemorySanitizer.ENABLED) {
        for (int c = 0; c < srcData.length; c++) {
          VarkaMemorySanitizer.register("input data", c, srcData[c], length * stride);
          if (srcValidity[c] != 0L) {
            VarkaMemorySanitizer.register("input validity", c, srcValidity[c], wordBytes(length));
          }
        }
        for (int o = 0; o < outData.length; o++) {
          if (outData[o] != 0L) {
            VarkaMemorySanitizer.register("output data", o, outData[o], length * stride);
          }
          VarkaMemorySanitizer.register("output validity", o, outValidity[o], wordBytes(length));
        }
        if (scratch != 0L) {
          VarkaMemorySanitizer.register("kernel scratch", 0, scratch,
              (long) kernel.scratchBytesPerRow() * length);
        }
      }
      return run.apply(scratch);
    } finally {
      VarkaMemorySanitizer.end();
    }
  }

  /** A value or a null as the Scala checks printed one, so a message reads as it did. */
  private static String shown(OptionalLong v) {
    return v.isPresent() ? "Some(" + v.getAsLong() + ")" : "None";
  }

  private static String shown(Answer a) {
    return switch (a) {
      case Answer.Value v -> "Value(" + v.v() + ")";
      case Answer.Null n -> "Null";
      case Answer.Raised r -> "Raised(" + r.e() + ")";
    };
  }

  private static String row(long[] values, boolean[] isNull) {
    var parts = new ArrayList<String>();
    for (int c = 0; c < values.length; c++) {
      parts.add(isNull[c] ? "None" : "Some(" + values[c] + ")");
    }
    return "(" + String.join(", ", parts) + ")";
  }

  /** Which of the kernel's bodies a batch ran: the masked one wherever a column reports a null. */
  private static String body(int[] nullCounts) {
    for (int n : nullCounts) {
      if (n > 0) {
        return "masked body";
      }
    }
    return "dense body";
  }

  /**
   * A failure's second line: the row's inputs and the body that ran. Built only when a check
   * fails, and kept off the first line, which is the only one a signature reads.
   */
  private static String evidence(long[] values, boolean[] isNull, int[] nullCounts) {
    return "\n  inputs " + row(values, isNull) + "; " + body(nullCounts);
  }

  /**
   * The reference evaluator's answer for output {@code o} on row {@code i} against Spark's
   * (VARKA-276): equal values or both null, a predicate's answers being 1, 0 or null. A difference
   * is the reference's to fix, or a named delta (row 244), before the kernel is held to it. Spark
   * raising is a row the kernel must decline the batch over, and this is called only for a batch
   * the kernel answered. The expression and the row are on the message's second line.
   */
  private static void agreeWithSpark(String context, SparkAnswers spark, int o, int i,
      long[] values, boolean[] isNull, OptionalLong reference) {
    SPARK_COMPARISONS.incrementAndGet();
    Answer answer = spark.answer(o, values, isNull);
    String where = "\n  " + spark.sql(o) + ", inputs " + row(values, isNull);
    boolean agree = switch (answer) {
      case Answer.Raised raised -> throw new IllegalStateException(context + ": Spark raises on "
          + "output " + o + " at row " + i + ", and the kernel answered the batch instead of "
          + "declining it" + where + ": " + raised.e().getMessage());
      case Answer.Value v -> reference.isPresent() && reference.getAsLong() == v.v();
      case Answer.Null n -> reference.isEmpty();
    };
    if (!agree) {
      throw new IllegalStateException(context + ": the reference evaluator and Spark disagree on "
          + "output " + o + " at row " + i + ": reference " + shown(reference) + ", Spark "
          + shown(answer) + where);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The entry points.
  // ---------------------------------------------------------------------------------------------

  /** One batch through one body: the status, 0 once compared. */
  private interface BodyRun {
    int apply(String context, boolean[][] nulls, boolean forceMasked);
  }

  /**
   * Runs one emitted int-lane kernel over the drawn columns and checks every row and validity bit
   * against {@link VarkaReference}. Null lanes are poisoned; {@code forceMasked} reports a null
   * over a full bitmap so the masked body runs. A batch that is compared is then run again through
   * the body it did not take ({@link #otherBody}), so both of the kernel's bodies meet the oracle.
   * Returns whether the rows were compared: false only where {@code declineAllowed} and the kernel
   * declined the batch, which a kernel over columns drawn without its domains may do.
   * {@code spark} is null where the suite holds no Spark answers.
   */
  public static boolean runAndCompare(String context, String className, byte[] bytes,
      List<VarkaVectorIR> roots, int numInputs, int[] lits, Batch batch, boolean declineAllowed,
      SparkAnswers spark) {
    return bodies(context, numInputs, batch.length(), batch.nulls(), batch.forceMasked(),
        (label, nulls, forced) -> runOne(label, className, bytes, roots, numInputs,
            new IntLane(batch.withNulls(nulls, forced), lits), declineAllowed, spark))[0] == 0;
  }

  /**
   * {@link #runAndCompare} for a caller that must know why a batch declined (VARKA-278's
   * variants): the status of the first body that declined, or 0 where every body run was compared.
   * A decline fails the check unless {@code declineAllowed}, as there; the bits are each reason's
   * own only for a kernel emitted under {@code exactRefusalStatus}.
   */
  public static int runAndCompareStatus(String context, String className, byte[] bytes,
      List<VarkaVectorIR> roots, int numInputs, int[] lits, Batch batch,
      boolean declineAllowed) {
    int[] statuses = bodies(context, numInputs, batch.length(), batch.nulls(),
        batch.forceMasked(), (label, nulls, forced) -> runOne(label, className, bytes, roots,
            numInputs, new IntLane(batch.withNulls(nulls, forced), lits), declineAllowed, null));
    return statuses[0] != 0 ? statuses[0] : statuses[1];
  }

  /**
   * The batch through each of the kernel's bodies, its own and then {@link #otherBody}'s, whatever
   * the first returned: each body's status, its rows compared where the status is 0. For a caller
   * that holds each body to its own expectation, as the guard verdict does (VARKA-279): a status
   * from {@link #runAndCompareStatus} does not say which body returned it.
   */
  public static List<Integer> runEachBodyStatus(String context, String className, byte[] bytes,
      List<VarkaVectorIR> roots, int numInputs, int[] lits, Batch batch) {
    return each(context, numInputs, batch.length(), batch.nulls(), batch.forceMasked(),
        (label, nulls, forced) -> runOne(label, className, bytes, roots, numInputs,
            new IntLane(batch.withNulls(nulls, forced), lits), true, null));
  }

  /**
   * {@link #runAndCompare} at the long lane: 64-bit buffers, the eight-argument {@code run} with
   * {@code lits} as the long literal table, and the long-lane reference. A narrowing root stores
   * four bytes a row, so its output is read at that width and sign-extended: a value that did not
   * fit 32 bits then differs from the evaluator's 64-bit answer instead of being truncated on both
   * sides.
   */
  public static boolean runAndCompareLong(String context, String className, byte[] bytes,
      List<VarkaVectorIR> roots, int numInputs, long[] lits, LongBatch batch,
      boolean declineAllowed, SparkAnswers spark) {
    return bodies(context, numInputs, batch.length(), batch.nulls(), batch.forceMasked(),
        (label, nulls, forced) -> runOne(label, className, bytes, roots, numInputs,
            new LongLane(batch.withNulls(nulls, forced), lits), declineAllowed, spark))[0] == 0;
  }

  /** {@link #runAndCompareStatus} at the long lane. */
  public static int runAndCompareLongStatus(String context, String className, byte[] bytes,
      List<VarkaVectorIR> roots, int numInputs, long[] lits, LongBatch batch,
      boolean declineAllowed) {
    int[] statuses = bodies(context, numInputs, batch.length(), batch.nulls(),
        batch.forceMasked(), (label, nulls, forced) -> runOne(label, className, bytes, roots,
            numInputs, new LongLane(batch.withNulls(nulls, forced), lits), declineAllowed, null));
    return statuses[0] != 0 ? statuses[0] : statuses[1];
  }

  /** {@link #runEachBodyStatus} at the long lane. */
  public static List<Integer> runEachBodyLongStatus(String context, String className,
      byte[] bytes, List<VarkaVectorIR> roots, int numInputs, long[] lits, LongBatch batch) {
    return each(context, numInputs, batch.length(), batch.nulls(), batch.forceMasked(),
        (label, nulls, forced) -> runOne(label, className, bytes, roots, numInputs,
            new LongLane(batch.withNulls(nulls, forced), lits), true, null));
  }

  /**
   * What a caller does around the one kernel call of a compared batch (VARKA-274). The tier
   * witness reads the code cache and opens its JFR event in {@code before} and closes both in
   * {@code after}, so that its window holds the kernel's call and not the buffers' setup or the
   * row-by-row comparison after it.
   */
  public interface CallHook {
    void before();

    void after(int status);

    /** A hook that does nothing, for the entry points that load a class per batch. */
    CallHook NONE = new CallHook() {
      @Override public void before() {
      }

      @Override public void after(int status) {
      }
    };
  }

  /**
   * One batch, through the body its null counts select, on a kernel the caller has loaded and
   * keeps (VARKA-274's histories, which run many batches on one class): the status, 0 once every
   * row and validity bit is compared with {@link VarkaReference}. A decline fails the check unless
   * {@code declineAllowed}. Unlike {@link #runAndCompare} it runs one body only; the caller asks
   * {@link #otherBody} for the other.
   */
  public static int runLoaded(String context, VarkaFusedKernel kernel,
      List<VarkaVectorIR> roots, int numInputs, int[] lits, Batch batch, boolean declineAllowed,
      CallHook hook) {
    return runLoaded(context, kernel, roots, numInputs, new IntLane(batch, lits), declineAllowed,
        null, hook);
  }

  /** {@link #runLoaded} at the long lane. */
  public static int runLoadedLong(String context, VarkaFusedKernel kernel,
      List<VarkaVectorIR> roots, int numInputs, long[] lits, LongBatch batch,
      boolean declineAllowed, CallHook hook) {
    return runLoaded(context, kernel, roots, numInputs, new LongLane(batch, lits),
        declineAllowed, null, hook);
  }

  /**
   * A planted wrong answer, for the test that a check which compares nothing cannot pass
   * (VARKA-274's test 5): while set, output 0's reference value on row 0 is one more than the
   * evaluator says. Set only by that test, in a JVM of its own.
   */
  static volatile boolean plantWrongAnswer;

  private static OptionalLong planted(OptionalLong want, int output, int row) {
    return plantWrongAnswer && output == 0 && row == 0 && want.isPresent()
        ? OptionalLong.of(want.getAsLong() + 1) : want;
  }

  /** {@code run} over the batch's own body and then {@link #otherBody}'s, each status in order. */
  private static List<Integer> each(String context, int numInputs, int length,
      boolean[][] nulls, boolean forceMasked, BodyRun run) {
    var statuses = new ArrayList<Integer>();
    statuses.add(run.apply(context, nulls, forceMasked));
    otherBody(numInputs, length, nulls, forceMasked).ifPresent(other ->
        statuses.add(run.apply(context + " (the other body)", other.nulls(), other.forceMasked())));
    return statuses;
  }

  /**
   * The batch through {@code run} - its own body - and, once that compared, the same values
   * through {@link #otherBody}, so both of a kernel's bodies meet the oracle: each run's status,
   * the second 0 where it did not run.
   */
  private static int[] bodies(String context, int numInputs, int length, boolean[][] nulls,
      boolean forceMasked, BodyRun run) {
    int first = run.apply(context, nulls, forceMasked);
    if (first != 0) {
      return new int[] {first, 0};
    }
    var other = otherBody(numInputs, length, nulls, forceMasked);
    int second = other.isPresent()
        ? run.apply(context + " (the other body)", other.get().nulls(), other.get().forceMasked())
        : 0;
    return new int[] {first, second};
  }

  /**
   * The same values through the body the drawn batch did not take (VARKA-284): a kernel has a
   * dense body for a batch with no nulls and a masked one for every other, and a shape run on one
   * kind of batch has only that body compared. A batch that took the masked body goes again with
   * no nulls; one that took the dense body goes again with the masked one forced - a null count
   * over a full bitmap, or for one row every column null, a forced batch of one row being all-null
   * by contract and a null in a column the kernel does not read selecting nothing. Empty for a
   * kernel that reads no column, which has no masked body to choose.
   */
  public static Optional<OtherBody> otherBody(int numInputs, int length, boolean[][] nulls,
      boolean forceMasked) {
    if (numInputs == 0 || length == 0) {
      return Optional.empty();
    }
    boolean masked = forceMasked;
    for (int c = 0; c < nulls.length && !masked; c++) {
      for (int i = 0; i < length && !masked; i++) {
        masked = nulls[c][i];
      }
    }
    if (masked) {
      return Optional.of(new OtherBody(filled(numInputs, length, false), false));
    }
    return length > 1 ? Optional.of(new OtherBody(nulls, true))
        : Optional.of(new OtherBody(filled(numInputs, length, true), false));
  }

  private static boolean[][] filled(int numInputs, int length, boolean value) {
    var out = new boolean[numInputs][length];
    for (var column : out) {
      Arrays.fill(column, value);
    }
    return out;
  }

  // ---------------------------------------------------------------------------------------------
  // One batch through one body, at either lane.
  // ---------------------------------------------------------------------------------------------

  /**
   * What differs between the two lanes in one batch's run: the value's width, its poison and the
   * sentinel an unwritten output keeps, the {@code run} overload, the reference's overload, and
   * how an output is read back. {@link #runOne} is the one harness both lanes go through.
   */
  private sealed interface Lane permits IntLane, LongLane {
    int length();

    boolean[][] nulls();

    boolean forceMasked();

    /** A value's bytes in the lane. */
    long stride();

    /** Column {@code c}'s drawn value at row {@code i}, widened. */
    long value(int c, int i);

    /** Writes {@code v} at row {@code i} of a column or output buffer at the lane's width. */
    void put(MemorySegment d, int i, long v);

    /**
     * Poison for the {@code nullOrdinal}-th null of a column (VARKA-70's harness rule): the lane's
     * extremes in turn, so each sits on both sides of every bound whatever the null pattern.
     */
    long poison(int nullOrdinal);

    /** What an output buffer holds before the kernel writes, so an unwritten lane shows. */
    long sentinel();

    int run(VarkaFusedKernel kernel, long[] srcData, long[] srcValidity, int[] nullCounts,
        long[] outData, long[] outValidity, long scratch);

    /** The reference's value of {@code root} on the row {@code values}, {@code isNull}. */
    OptionalLong reference(VarkaVectorIR root, long[] values, boolean[] isNull);

    VarkaReference.Truth reference(Cond cond, long[] values, boolean[] isNull);

    /** Output {@code o}'s stored value at row {@code i}. */
    long got(MemorySegment out, int i, VarkaVectorIR root);
  }

  /** The 32-bit lane; {@code rowInts} is its reusable row for the reference. */
  private record IntLane(Batch batch, int[] lits, int[] rowInts) implements Lane {
    IntLane(Batch batch, int[] lits) {
      this(batch, lits, new int[batch.data().length]);
    }

    @Override public int length() {
      return batch.length();
    }

    @Override public boolean[][] nulls() {
      return batch.nulls();
    }

    @Override public boolean forceMasked() {
      return batch.forceMasked();
    }

    @Override public long stride() {
      return 4L;
    }

    @Override public long value(int c, int i) {
      return batch.data()[c][i];
    }

    @Override public void put(MemorySegment d, int i, long v) {
      d.set(ValueLayout.JAVA_INT, i * 4L, (int) v);
    }

    @Override public long poison(int nullOrdinal) {
      return VarkaEmitterFixtures.poison(nullOrdinal);
    }

    @Override public long sentinel() {
      return 0xDEADBEEF;
    }

    @Override public int run(VarkaFusedKernel kernel, long[] srcData, long[] srcValidity,
        int[] nullCounts, long[] outData, long[] outValidity, long scratch) {
      return kernel.run(srcData, srcValidity, nullCounts, outData, outValidity, lits, length(),
          scratch);
    }

    @Override public OptionalLong reference(VarkaVectorIR root, long[] values, boolean[] isNull) {
      for (int c = 0; c < values.length; c++) {
        rowInts[c] = (int) values[c];
      }
      OptionalInt v = VarkaReference.value(root, rowInts, isNull, lits);
      return v.isPresent() ? OptionalLong.of(v.getAsInt()) : OptionalLong.empty();
    }

    @Override public VarkaReference.Truth reference(Cond cond, long[] values, boolean[] isNull) {
      for (int c = 0; c < values.length; c++) {
        rowInts[c] = (int) values[c];
      }
      return VarkaReference.cond(cond, rowInts, isNull, lits);
    }

    @Override public long got(MemorySegment out, int i, VarkaVectorIR root) {
      return out.get(ValueLayout.JAVA_INT, i * 4L);
    }
  }

  /** The 64-bit lane, whose narrowing roots store four bytes a row. */
  private record LongLane(LongBatch batch, long[] lits) implements Lane {
    @Override public int length() {
      return batch.length();
    }

    @Override public boolean[][] nulls() {
      return batch.nulls();
    }

    @Override public boolean forceMasked() {
      return batch.forceMasked();
    }

    @Override public long stride() {
      return 8L;
    }

    @Override public long value(int c, int i) {
      return batch.data()[c][i];
    }

    @Override public void put(MemorySegment d, int i, long v) {
      d.set(ValueLayout.JAVA_LONG, i * 8L, v);
    }

    @Override public long poison(int nullOrdinal) {
      return (nullOrdinal & 1) == 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
    }

    @Override public long sentinel() {
      return 0xDEADBEEFCAFEBABEL;
    }

    @Override public int run(VarkaFusedKernel kernel, long[] srcData, long[] srcValidity,
        int[] nullCounts, long[] outData, long[] outValidity, long scratch) {
      return kernel.run(srcData, srcValidity, nullCounts, outData, outValidity, new int[0], lits,
          length(), scratch);
    }

    @Override public OptionalLong reference(VarkaVectorIR root, long[] values, boolean[] isNull) {
      return VarkaReference.longValue(root, values, isNull, lits);
    }

    @Override public VarkaReference.Truth reference(Cond cond, long[] values, boolean[] isNull) {
      return VarkaReference.longCond(cond, values, isNull, lits);
    }

    @Override public long got(MemorySegment out, int i, VarkaVectorIR root) {
      return root instanceof NarrowLane ? out.get(ValueLayout.JAVA_INT, i * 4L)
          : out.get(ValueLayout.JAVA_LONG, i * 8L);
    }
  }

  /**
   * Loads the generated class. A failure the class's own construction throws is rethrown as it is
   * - a {@code NoSuchMethodError} from a misdescribed helper call stays one - so the failure
   * classifier names it, instead of reading the reflection that carried it.
   */
  static VarkaFusedKernel load(VarkaGeneratedClassLoader loader, String className,
      byte[] bytes) {
    VarkaSpeciesGuard.check(className, bytes);
    loader.defineGeneratedClass(className, bytes);
    try {
      return (VarkaFusedKernel) loader.loadClass(className).getConstructor().newInstance();
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof Error error) {
        throw error;
      }
      if (e.getCause() instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw new IllegalStateException(e.getCause());
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * One batch through the check, whichever body its null counts select: the kernel's status, 0
   * once the rows are compared. Null lanes are poisoned; every output buffer starts at the lane's
   * sentinel and every validity bit set, so a lane or bit the kernel fails to write shows.
   */
  private static int runOne(String context, String className, byte[] bytes,
      List<VarkaVectorIR> rootList, int numInputs, Lane lane, boolean declineAllowed,
      SparkAnswers spark) {
    var loader = new VarkaGeneratedClassLoader(VarkaKernelChecks.class.getClassLoader());
    var kernel = load(loader, className, bytes);
    try {
      return runLoaded(context, kernel, rootList, numInputs, lane, declineAllowed, spark,
          CallHook.NONE);
    } finally {
      loader.release();
    }
  }

  /**
   * {@link #runOne} on a kernel already loaded, so that a caller can run many batches on one
   * class (VARKA-274): {@code hook} is told just before and just after the kernel's one call.
   */
  private static int runLoaded(String context, VarkaFusedKernel kernel,
      List<VarkaVectorIR> rootList, int numInputs, Lane lane, boolean declineAllowed,
      SparkAnswers spark, CallHook hook) {
    int length = lane.length();
    var roots = rootList.toArray(new VarkaVectorIR[0]);
    try (var arena = Arena.ofConfined()) {
      long stride = lane.stride();
      long[] srcData = new long[numInputs];
      long[] srcValidity = new long[numInputs];
      int[] nullCounts = new int[numInputs];
      for (int c = 0; c < numInputs; c++) {
        var d = VarkaEmitterFixtures.alloc(arena, length * stride);
        var v = VarkaEmitterFixtures.alloc(arena, wordBytes(length));
        v.fill((byte) 0);
        int nulls = 0;
        for (int i = 0; i < length; i++) {
          if (lane.nulls()[c][i]) {
            // Poisoned, not left at the drawn value. The data is drawn inside `columnBound` and
            // `MONTH_ARITH_MAX_MONTHS`, so a null lane holding its drawn value is in range by
            // construction and can never reach a guard's condemning comparison - which is the
            // one thing the fuzzer is here to reach.
            lane.put(d, i, lane.poison(nulls));
            nulls++;
          } else {
            lane.put(d, i, lane.value(c, i));
            long off = i / 8L;
            v.set(ValueLayout.JAVA_BYTE, off,
                (byte) (v.get(ValueLayout.JAVA_BYTE, off) | (1 << (i % 8))));
          }
        }
        srcData[c] = d.address();
        nullCounts[c] = lane.forceMasked() && nulls == 0 ? 1 : nulls;
        srcValidity[c] = nullCounts[c] == 0 || nulls == length ? 0L : v.address();
      }
      int n = roots.length;
      var outValues = new MemorySegment[n];
      var outBits = new MemorySegment[n];
      long[] outData = new long[n];
      long[] outValidity = new long[n];
      for (int o = 0; o < n; o++) {
        outValues[o] = VarkaEmitterFixtures.alloc(arena, length * stride);
        for (int i = 0; i < length; i++) {
          lane.put(outValues[o], i, lane.sentinel());
        }
        outBits[o] = VarkaEmitterFixtures.alloc(arena, wordBytes(length));
        outBits[o].fill((byte) 0xFF);
        outData[o] = roots[o] instanceof Cond ? 0L : outValues[o].address();
        outValidity[o] = outBits[o].address();
      }
      hook.before();
      int status = sanitized(kernel, length, stride, srcData, srcValidity, outData, outValidity,
          scratch -> lane.run(kernel, srcData, srcValidity, nullCounts, outData, outValidity,
              scratch));
      hook.after(status);
      if (status != 0 && declineAllowed) {
        return status;
      }
      if (status != 0) {
        throw new IllegalStateException(
            context + ": the kernel declined the batch (status " + status + ")");
      }
      long[] values = new long[numInputs];
      boolean[] isNull = new boolean[numInputs];
      for (int i = 0; i < length; i++) {
        for (int c = 0; c < numInputs; c++) {
          isNull[c] = lane.nulls()[c][i];
          values[c] = lane.value(c, i);
        }
        for (int o = 0; o < n; o++) {
          var root = roots[o];
          boolean bit = (outBits[o].get(ValueLayout.JAVA_BYTE, i / 8L) & (1 << (i % 8))) != 0;
          if (root instanceof Cond c) {
            var truth = lane.reference(c, values, isNull);
            if (spark != null) {
              agreeWithSpark(context, spark, o, i, values, isNull,
                  truth == VarkaReference.Truth.UNKNOWN ? OptionalLong.empty()
                      : OptionalLong.of(truth.isTrue() ? 1L : 0L));
            }
            if (bit != truth.isTrue()) {
              throw new IllegalStateException(context + ": selection row " + i + " differs (want "
                  + truth.isTrue() + ", got " + bit + ")" + evidence(values, isNull, nullCounts));
            }
          } else {
            OptionalLong want = planted(lane.reference(root, values, isNull), o, i);
            if (spark != null) {
              agreeWithSpark(context, spark, o, i, values, isNull, want);
            }
            long got = lane.got(outValues[o], i, root);
            if (bit != want.isPresent()) {
              throw new IllegalStateException(context + ": validity of output " + o + " row " + i
                  + " differs (want " + shown(want) + ", got "
                  + (bit ? "Some(" + got + ")" : "None") + ")"
                  + evidence(values, isNull, nullCounts));
            }
            if (want.isPresent() && got != want.getAsLong()) {
              throw new IllegalStateException(context + ": output " + o + " row " + i
                  + " differs (want " + want.getAsLong() + ", got " + got + ")"
                  + evidence(values, isNull, nullCounts));
            }
          }
        }
      }
      return 0;
    }
  }
}
