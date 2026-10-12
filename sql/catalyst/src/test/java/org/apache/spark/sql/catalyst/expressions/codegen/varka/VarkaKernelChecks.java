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
   * Outputs compared on rows the kernel answered: one per output per row of every body run, so two
   * runs of one suite at one seed that read the same count compared the same things (VARKA-311).
   */
  public static final AtomicLong ROW_COMPARISONS = new AtomicLong();

  private static MemorySegment alloc(Arena arena, long bytes) {
    return arena.allocate(Math.max(bytes, 1L), 8);
  }

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

  /** Which of the kernel's bodies a batch ran: the masked one wherever a column reports a null. */
  private static String body(int[] nullCounts) {
    for (int n : nullCounts) {
      if (n > 0) {
        return "masked body";
      }
    }
    return "dense body";
  }

  private static String row(long[] values, boolean[] isNull) {
    var parts = new ArrayList<String>();
    for (int c = 0; c < values.length; c++) {
      parts.add(isNull[c] ? "None" : "Some(" + values[c] + ")");
    }
    return "(" + String.join(", ", parts) + ")";
  }

  /**
   * The reference evaluator's answer for output {@code o} on row {@code i} against Spark's
   * (VARKA-276): equal values or both null, a predicate's answers being 1, 0 or null. A difference
   * is the reference's to fix, or a named delta (row 244), before the kernel is held to it. Spark
   * raising is a row the kernel must decline the batch over, and this is called only for a batch
   * the kernel answered.
   */
  private static void agreeWithSpark(String context, SparkAnswers spark, int o, int i,
      long[] values, boolean[] isNull, OptionalLong reference) {
    SPARK_COMPARISONS.incrementAndGet();
    Answer answer = spark.answer(o, values, isNull);
    if (answer instanceof Answer.Raised raised) {
      throw new IllegalStateException(context + ": Spark raises on output " + o + " ("
          + spark.sql(o) + ") at row " + i + ", inputs " + row(values, isNull) + ", and the "
          + "kernel answered the batch instead of declining it: " + raised.e().getMessage());
    }
    boolean agree = switch (answer) {
      case Answer.Value v -> reference.isPresent() && reference.getAsLong() == v.v();
      case Answer.Null n -> reference.isEmpty();
      case Answer.Raised r -> false;
    };
    if (!agree) {
      throw new IllegalStateException(context + ": the reference evaluator and Spark disagree on "
          + "output " + o + " (" + spark.sql(o) + ") at row " + i + ", inputs "
          + row(values, isNull) + ": reference " + shown(reference) + ", Spark " + shown(answer));
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
        (label, nulls, forced) -> runBatch(label, className, bytes, roots, numInputs, lits,
            batch.withNulls(nulls, forced), declineAllowed, spark))[0] == 0;
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
        batch.forceMasked(), (label, nulls, forced) -> runBatch(label, className, bytes, roots,
            numInputs, lits, batch.withNulls(nulls, forced), declineAllowed, null));
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
        (label, nulls, forced) -> runBatch(label, className, bytes, roots, numInputs, lits,
            batch.withNulls(nulls, forced), true, null));
  }

  /** {@link #runAndCompare} at the long lane, with {@code lits} as the long literal table. */
  public static boolean runAndCompareLong(String context, String className, byte[] bytes,
      List<VarkaVectorIR> roots, int numInputs, long[] lits, LongBatch batch,
      boolean declineAllowed, SparkAnswers spark) {
    return bodies(context, numInputs, batch.length(), batch.nulls(), batch.forceMasked(),
        (label, nulls, forced) -> runLongBatch(label, className, bytes, roots, numInputs, lits,
            batch.withNulls(nulls, forced), declineAllowed, spark))[0] == 0;
  }

  /** {@link #runAndCompareStatus} at the long lane. */
  public static int runAndCompareLongStatus(String context, String className, byte[] bytes,
      List<VarkaVectorIR> roots, int numInputs, long[] lits, LongBatch batch,
      boolean declineAllowed) {
    int[] statuses = bodies(context, numInputs, batch.length(), batch.nulls(),
        batch.forceMasked(), (label, nulls, forced) -> runLongBatch(label, className, bytes,
            roots, numInputs, lits, batch.withNulls(nulls, forced), declineAllowed, null));
    return statuses[0] != 0 ? statuses[0] : statuses[1];
  }

  /** {@link #runEachBodyStatus} at the long lane. */
  public static List<Integer> runEachBodyLongStatus(String context, String className,
      byte[] bytes, List<VarkaVectorIR> roots, int numInputs, long[] lits, LongBatch batch) {
    return each(context, numInputs, batch.length(), batch.nulls(), batch.forceMasked(),
        (label, nulls, forced) -> runLongBatch(label, className, bytes, roots, numInputs, lits,
            batch.withNulls(nulls, forced), true, null));
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
  // One batch through one body.
  // ---------------------------------------------------------------------------------------------

  private static VarkaFusedKernel load(VarkaGeneratedClassLoader loader, String className,
      byte[] bytes) {
    VarkaSpeciesGuard.check(className, bytes);
    loader.defineGeneratedClass(className, bytes);
    try {
      return (VarkaFusedKernel) loader.loadClass(className).getConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * One batch through {@link #runAndCompare}'s check, whichever body its null counts select: the
   * kernel's status, 0 once the rows are compared.
   */
  private static int runBatch(String context, String className, byte[] bytes,
      List<VarkaVectorIR> roots, int numInputs, int[] lits, Batch batch, boolean declineAllowed,
      SparkAnswers spark) {
    int length = batch.length();
    var loader = new VarkaGeneratedClassLoader(VarkaKernelChecks.class.getClassLoader());
    var kernel = load(loader, className, bytes);
    try (var arena = Arena.ofConfined()) {
      long[] srcData = new long[numInputs];
      long[] srcValidity = new long[numInputs];
      int[] nullCounts = new int[numInputs];
      for (int c = 0; c < numInputs; c++) {
        var d = alloc(arena, length * 4L);
        var v = alloc(arena, wordBytes(length));
        v.fill((byte) 0);
        int nulls = 0;
        for (int i = 0; i < length; i++) {
          if (batch.nulls()[c][i]) {
            // Poisoned, not left at the drawn value (VARKA-70's harness rule, the same one
            // VarkaEmitterFixtures.poison states). The data is drawn inside `columnBound` and
            // `MONTH_ARITH_MAX_MONTHS`, so a null lane holding its drawn value is in range by
            // construction and can never reach a guard's condemning comparison - which is the
            // one thing the fuzzer is here to reach. Alternating on the null ordinal puts each
            // extreme on both sides of every bound whatever the null pattern is.
            d.set(ValueLayout.JAVA_INT, i * 4L,
                (nulls & 1) == 0 ? Integer.MIN_VALUE : Integer.MAX_VALUE);
            nulls++;
          } else {
            d.set(ValueLayout.JAVA_INT, i * 4L, batch.data()[c][i]);
            long off = i / 8L;
            v.set(ValueLayout.JAVA_BYTE, off,
                (byte) (v.get(ValueLayout.JAVA_BYTE, off) | (1 << (i % 8))));
          }
        }
        srcData[c] = d.address();
        nullCounts[c] = batch.forceMasked() && nulls == 0 ? 1 : nulls;
        srcValidity[c] = nullCounts[c] == 0 || nulls == length ? 0L : v.address();
      }
      int n = roots.size();
      var outValues = new MemorySegment[n];
      var outBits = new MemorySegment[n];
      long[] outData = new long[n];
      long[] outValidity = new long[n];
      for (int o = 0; o < n; o++) {
        outValues[o] = alloc(arena, length * 4L);
        for (int i = 0; i < length; i++) {
          outValues[o].set(ValueLayout.JAVA_INT, i * 4L, 0xDEADBEEF);
        }
        outBits[o] = alloc(arena, wordBytes(length));
        outBits[o].fill((byte) 0xFF);
        outData[o] = roots.get(o) instanceof Cond ? 0L : outValues[o].address();
        outValidity[o] = outBits[o].address();
      }
      int status = sanitized(kernel, length, 4L, srcData, srcValidity, outData, outValidity,
          scratch -> kernel.run(srcData, srcValidity, nullCounts, outData, outValidity, lits,
              length, scratch));
      if (status != 0 && declineAllowed) {
        return status;
      }
      if (status != 0) {
        throw new IllegalStateException(
            context + ": the kernel declined the batch (status " + status + ")");
      }
      int[] values = new int[numInputs];
      long[] wide = new long[numInputs];
      boolean[] isNull = new boolean[numInputs];
      for (int i = 0; i < length; i++) {
        for (int c = 0; c < numInputs; c++) {
          isNull[c] = batch.nulls()[c][i];
          values[c] = batch.data()[c][i];
          wide[c] = values[c];
        }
        for (int o = 0; o < n; o++) {
          ROW_COMPARISONS.incrementAndGet();
          var root = roots.get(o);
          boolean bit = (outBits[o].get(ValueLayout.JAVA_BYTE, i / 8L) & (1 << (i % 8))) != 0;
          if (spark != null) {
            OptionalLong reference;
            if (root instanceof Cond c) {
              var t = VarkaReference.cond(c, values, isNull, lits);
              reference = t == VarkaReference.Truth.UNKNOWN ? OptionalLong.empty()
                  : OptionalLong.of(t.isTrue() ? 1L : 0L);
            } else {
              var v = VarkaReference.value(root, values, isNull, lits);
              reference = v.isPresent() ? OptionalLong.of(v.getAsInt()) : OptionalLong.empty();
            }
            agreeWithSpark(context, spark, o, i, wide, isNull, reference);
          }
          String evidence = "inputs " + row(wide, isNull) + "; " + body(nullCounts);
          if (root instanceof Cond c) {
            boolean want = VarkaReference.cond(c, values, isNull, lits).isTrue();
            if (bit != want) {
              throw new IllegalStateException(context + ": selection row " + i + " differs (want "
                  + want + ", got " + bit + ")\n  " + evidence);
            }
          } else {
            OptionalInt want = VarkaReference.value(root, values, isNull, lits);
            int got = outValues[o].get(ValueLayout.JAVA_INT, i * 4L);
            if (bit != want.isPresent()) {
              throw new IllegalStateException(context + ": validity of output " + o + " row " + i
                  + " differs (want " + (want.isPresent() ? "Some(" + want.getAsInt() + ")"
                  : "None") + ", got " + (bit ? "Some(" + got + ")" : "None") + ")\n  "
                  + evidence);
            }
            if (want.isPresent() && got != want.getAsInt()) {
              throw new IllegalStateException(context + ": output " + o + " row " + i
                  + " differs (want " + want.getAsInt() + ", got " + got + ")\n  " + evidence);
            }
          }
        }
      }
      return 0;
    } finally {
      loader.release();
    }
  }

  /**
   * One batch through {@link #runAndCompareLong}'s check: 64-bit buffers, the eight-argument
   * {@code run} with {@code lits} as the long literal table, and the long-lane reference. Null
   * lanes are poisoned with the lane's own extremes. A narrowing root stores four bytes a row, so
   * its output is read at that width and sign-extended: a value that did not fit 32 bits then
   * differs from the evaluator's 64-bit answer instead of being truncated on both sides.
   */
  private static int runLongBatch(String context, String className, byte[] bytes,
      List<VarkaVectorIR> roots, int numInputs, long[] lits, LongBatch batch,
      boolean declineAllowed, SparkAnswers spark) {
    int length = batch.length();
    var loader = new VarkaGeneratedClassLoader(VarkaKernelChecks.class.getClassLoader());
    var kernel = load(loader, className, bytes);
    try (var arena = Arena.ofConfined()) {
      long[] srcData = new long[numInputs];
      long[] srcValidity = new long[numInputs];
      int[] nullCounts = new int[numInputs];
      for (int c = 0; c < numInputs; c++) {
        var d = alloc(arena, length * 8L);
        var v = alloc(arena, wordBytes(length));
        v.fill((byte) 0);
        int nulls = 0;
        for (int i = 0; i < length; i++) {
          if (batch.nulls()[c][i]) {
            d.set(ValueLayout.JAVA_LONG, i * 8L,
                (nulls & 1) == 0 ? Long.MIN_VALUE : Long.MAX_VALUE);
            nulls++;
          } else {
            d.set(ValueLayout.JAVA_LONG, i * 8L, batch.data()[c][i]);
            long off = i / 8L;
            v.set(ValueLayout.JAVA_BYTE, off,
                (byte) (v.get(ValueLayout.JAVA_BYTE, off) | (1 << (i % 8))));
          }
        }
        srcData[c] = d.address();
        nullCounts[c] = batch.forceMasked() && nulls == 0 ? 1 : nulls;
        srcValidity[c] = nullCounts[c] == 0 || nulls == length ? 0L : v.address();
      }
      int n = roots.size();
      var outValues = new MemorySegment[n];
      var outBits = new MemorySegment[n];
      long[] outData = new long[n];
      long[] outValidity = new long[n];
      for (int o = 0; o < n; o++) {
        outValues[o] = alloc(arena, length * 8L);
        for (int i = 0; i < length; i++) {
          outValues[o].set(ValueLayout.JAVA_LONG, i * 8L, 0xDEADBEEFCAFEBABEL);
        }
        outBits[o] = alloc(arena, wordBytes(length));
        outBits[o].fill((byte) 0xFF);
        outData[o] = roots.get(o) instanceof Cond ? 0L : outValues[o].address();
        outValidity[o] = outBits[o].address();
      }
      int status = sanitized(kernel, length, 8L, srcData, srcValidity, outData, outValidity,
          scratch -> kernel.run(srcData, srcValidity, nullCounts, outData, outValidity,
              new int[0], lits, length, scratch));
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
          isNull[c] = batch.nulls()[c][i];
          values[c] = batch.data()[c][i];
        }
        for (int o = 0; o < n; o++) {
          ROW_COMPARISONS.incrementAndGet();
          var root = roots.get(o);
          boolean bit = (outBits[o].get(ValueLayout.JAVA_BYTE, i / 8L) & (1 << (i % 8))) != 0;
          if (spark != null) {
            OptionalLong reference;
            if (root instanceof Cond c) {
              var t = VarkaReference.longCond(c, values, isNull, lits);
              reference = t == VarkaReference.Truth.UNKNOWN ? OptionalLong.empty()
                  : OptionalLong.of(t.isTrue() ? 1L : 0L);
            } else {
              reference = VarkaReference.longValue(root, values, isNull, lits);
            }
            agreeWithSpark(context, spark, o, i, values, isNull, reference);
          }
          String evidence = "inputs " + row(values, isNull) + "; " + body(nullCounts);
          if (root instanceof Cond c) {
            boolean want = VarkaReference.longCond(c, values, isNull, lits).isTrue();
            if (bit != want) {
              throw new IllegalStateException(context + ": selection row " + i + " differs (want "
                  + want + ", got " + bit + ")\n  " + evidence);
            }
          } else {
            OptionalLong want = VarkaReference.longValue(root, values, isNull, lits);
            long got = root instanceof NarrowLane
                ? outValues[o].get(ValueLayout.JAVA_INT, i * 4L)
                : outValues[o].get(ValueLayout.JAVA_LONG, i * 8L);
            if (bit != want.isPresent()) {
              throw new IllegalStateException(context + ": validity of output " + o + " row " + i
                  + " differs (want " + shown(want) + ", got "
                  + (bit ? "Some(" + got + ")" : "None") + ")\n  " + evidence);
            }
            if (want.isPresent() && got != want.getAsLong()) {
              throw new IllegalStateException(context + ": output " + o + " row " + i
                  + " differs (want " + want.getAsLong() + ", got " + got + ")\n  " + evidence);
            }
          }
        }
      }
      return 0;
    } finally {
      loader.release();
    }
  }
}
