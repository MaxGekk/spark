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

import static org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaDecimalStoreTables.label;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions.DecimalFault;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.CompareOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Cond;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DecimalColumn;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DecimalWord;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IfElse;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IsNotNull;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Not;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Word;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.ArrowColumnVector;

/**
 * The 128-bit decimal's word leaves and comparisons (`m8/VARKA-359.md` 3 items 1 and 2, 11.7),
 * run over Arrow decimal vectors whose sixteen-byte slots are written word by word, so that a
 * slot can hold a value with more digits than its precision, which Spark's writer never writes.
 *
 * <p>The oracle is Spark's column reader, the same vector wrapped in {@link ArrowColumnVector},
 * whose decimal accessor raises on a valid slot outside the precision and never reads a null one,
 * and {@link BigInteger} for the comparisons. Every kernel runs at two, four and eight long lanes,
 * baked, and at the preferred species unbaked ({@code lanesOverride=1}), in the dense body and the
 * masked one. In a JVM of its own: the widths are several species of the long lane, which the
 * shared JVM refuses (VARKA-246).
 */
@VarkaSuite(ownJvm = true)
public final class VarkaDecimalPairSuite {

  private static final int[] WIDTHS = {1, 2, 4, 8};
  private static final int[] PRECISIONS = {19, 20, 30, 38};
  /**
   * Shorter than one lane group at every width but the narrowest, one whose last row is in the
   * epilogue at every baked width, and one with no epilogue.
   */
  private static final int[] LENGTHS = {1, 3, 37, 64};
  private static final int SCALE = 2;
  private static final BigInteger TWO_63 = BigInteger.ONE.shiftLeft(63);
  private static final BigInteger TWO_64 = BigInteger.ONE.shiftLeft(64);
  private static final BigInteger TWO_127 = BigInteger.ONE.shiftLeft(127);

  /** A column's rows: each row's value (any 128-bit integer) and whether it is null. */
  private record Column(BigInteger[] values, boolean[] nulls, int precision) {
    int length() {
      return values.length;
    }
  }

  private static BigInteger bound(int precision) {
    return BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
  }

  /**
   * A column of in-precision values whose words both carry bits, with nulls on every third row
   * when {@code masked}, each null holding garbage, out-of-precision values included.
   */
  private static Column column(int length, int precision, boolean masked, long seed) {
    var random = new Random(seed);
    BigInteger b = bound(precision);
    var values = new BigInteger[length];
    var nulls = new boolean[length];
    for (int i = 0; i < length; i++) {
      nulls[i] = masked && i % 3 == 1;
      if (nulls[i]) {
        values[i] = new BigInteger(127, random).subtract(TWO_127.shiftRight(1));
      } else {
        BigInteger v = new BigInteger(b.bitLength() + 8, random).mod(b.shiftLeft(1).add(
            BigInteger.ONE)).subtract(b);
        values[i] = v;
      }
    }
    return new Column(values, nulls, precision);
  }

  /** The rows a planted slot is tried at: the first group, a middle one and the last row. */
  private static int[] positions(int length) {
    return IntStream.of(0, length / 2, length - 1).distinct().toArray();
  }

  /** The values each of which must decline the batch where it is valid. */
  private static List<BigInteger> declining(int precision) {
    BigInteger p = BigInteger.TEN.pow(precision);
    return List.of(p, p.negate(), TWO_127.subtract(BigInteger.ONE), TWO_127.negate());
  }

  /**
   * The values that pass: the bounds, zero and minus one, and a low word with its sign bit set
   * under a high word of 0 and of -1 (every precision from 19 holds {@code 2^63}).
   */
  private static List<BigInteger> passing(int precision) {
    BigInteger b = bound(precision);
    return List.of(b, b.negate(), BigInteger.ZERO, BigInteger.ONE.negate(), TWO_63,
        TWO_63.negate().subtract(BigInteger.ONE));
  }

  /** The vector of {@code c}, its slots written word by word and its nulls marked. */
  private static DecimalVector vector(Column c, BufferAllocator allocator) {
    var v = new DecimalVector("d", allocator, c.precision(), SCALE);
    v.allocateNew(Math.max(1, c.length()));
    for (int i = 0; i < c.length(); i++) {
      DecimalWords w = DecimalWords.of(c.values()[i]);
      v.getDataBuffer().setLong(i * 16L, w.lo());
      v.getDataBuffer().setLong(i * 16L + 8, w.hi());
      if (c.nulls()[i]) {
        v.setNull(i);
      } else {
        v.setIndexDefined(i);
      }
    }
    v.setValueCount(c.length());
    return v;
  }

  /** Spark's reading of a column: each valid row's unscaled value, or null if some row raised. */
  private static BigInteger[] spark(DecimalVector v, int precision) {
    var column = new ArrowColumnVector(v);
    var unscaled = new BigInteger[v.getValueCount()];
    for (int i = 0; i < unscaled.length; i++) {
      if (column.isNullAt(i)) {
        continue;
      }
      try {
        Decimal d = column.getDecimal(i, precision, SCALE);
        unscaled[i] = d.toJavaBigDecimal().setScale(SCALE).unscaledValue();
      } catch (ArithmeticException e) {
        return null;
      }
    }
    return unscaled;
  }

  /** The kernel's answer: its status, then each row's value and validity, per output. */
  private record Answer(int status, long[][] values, boolean[][] valid) { }

  /** Runs {@code kernel} over decimal vectors at ordinals 0, 1, ... with long outputs. */
  private static Answer run(VarkaFusedKernel kernel, List<DecimalVector> columns, int outputs,
      long[] literals) {
    int length = columns.get(0).getValueCount();
    int inputs = columns.size();
    long wordBytes = ((length + 63) / 64) * 8L;
    try (var arena = Arena.ofConfined()) {
      long[] srcData = new long[inputs];
      long[] srcValidity = new long[inputs];
      int[] nullCounts = new int[inputs];
      long[] inStride = new long[inputs];
      for (int c = 0; c < inputs; c++) {
        DecimalVector v = columns.get(c);
        srcData[c] = v.getDataBuffer().memoryAddress();
        srcValidity[c] = v.getNullCount() == length ? 0L : v.getValidityBuffer().memoryAddress();
        nullCounts[c] = v.getNullCount();
        inStride[c] = 16L;
      }
      long[] outData = new long[outputs];
      long[] outValidity = new long[outputs];
      long[] outStride = new long[outputs];
      MemorySegment[] out = new MemorySegment[outputs];
      MemorySegment[] outValid = new MemorySegment[outputs];
      for (int o = 0; o < outputs; o++) {
        out[o] = arena.allocate(Math.max(1, length) * 8L, 64);
        outValid[o] = arena.allocate(Math.max(8, wordBytes), 64);
        outData[o] = out[o].address();
        outValidity[o] = outValid[o].address();
        outStride[o] = 8L;
      }
      int status = VarkaKernelChecks.sanitized(kernel, length, inStride, outStride, srcData,
          srcValidity, outData, outValidity,
          scratch -> kernel.run(srcData, srcValidity, nullCounts, outData, outValidity,
              new int[0], literals, length, scratch));
      long[][] values = new long[outputs][length];
      boolean[][] validRows = new boolean[outputs][length];
      for (int o = 0; o < outputs; o++) {
        for (int i = 0; i < length; i++) {
          values[o][i] = out[o].get(ValueLayout.JAVA_LONG, i * 8L);
          validRows[o][i] =
              (outValid[o].get(ValueLayout.JAVA_LONG, (i >>> 6) * 8L) >>> (i & 63) & 1L) != 0;
        }
      }
      return new Answer(status, values, validRows);
    }
  }

  private static VarkaEmitterFixtures.Loaded kernel(List<VarkaVectorIR> roots, int inputs,
      int literals, VarkaEmitOptions options) {
    var emitted = VarkaEmitterFixtures.emit(roots, inputs, literals, options);
    List<String> errors = VarkaEmitterTestSupport.verify(emitted.bytes());
    assertTrue(errors.isEmpty(), () -> "the class does not verify: " + errors);
    return VarkaEmitterFixtures.load(emitted);
  }

  private static VarkaEmitOptions at(int lanes) {
    return VarkaMatrixRuns.BASE.withLanesOverride(lanes);
  }

  private static DecimalWord lo(int ordinal, int precision) {
    return new DecimalWord(ordinal, Word.LOW, precision);
  }

  private static DecimalWord hi(int ordinal, int precision) {
    return new DecimalWord(ordinal, Word.HIGH, precision);
  }

  // ------------------------------------------------------------------------------------------
  // The slot table: the load, its de-interleave and its 10^p check
  // ------------------------------------------------------------------------------------------

  /**
   * Runs the kernel of the column's two words as two outputs over {@code c}; returns a mismatch,
   * or null where the kernel declines exactly where Spark's reader raises and answers both
   * words of every valid row otherwise. {@code declined[0]} says whether it declined.
   */
  private static String checkWords(String at, VarkaFusedKernel k, Column c, boolean[] declined) {
    try (BufferAllocator allocator =
        ArrowUtils.rootAllocator().newChildAllocator("varka-decimal-pair", 0, Long.MAX_VALUE);
        DecimalVector v = vector(c, allocator)) {
      BigInteger[] expected = spark(v, c.precision());
      Answer got = run(k, List.of(v), 2, new long[0]);
      declined[0] = got.status() != 0;
      if (expected == null) {
        return got.status() == VarkaFusedKernel.STATUS_DECIMAL_SLOT ? null
            : at + ": Spark's reader raises, the kernel answers status " + got.status();
      }
      if (got.status() != 0) {
        return at + ": Spark's reader reads every row, the kernel declines " + got.status();
      }
      for (int i = 0; i < c.length(); i++) {
        if (got.valid()[0][i] == c.nulls()[i] || got.valid()[1][i] == c.nulls()[i]) {
          return at + ": validity of row " + i;
        }
        if (!c.nulls()[i]) {
          DecimalWords w = DecimalWords.of(expected[i]);
          if (got.values()[0][i] != w.lo() || got.values()[1][i] != w.hi()) {
            return at + ": row " + i + " holds " + expected[i] + ", the kernel's words are ("
                + got.values()[0][i] + ", " + got.values()[1][i] + ")";
          }
        }
      }
      return null;
    }
  }

  /** The slot table at {@code widths}: the first mismatch, or null. */
  private static String slotTable(int[] widths, int[] precisions) {
    return slotTable(widths, precisions, DecimalFault.NONE);
  }

  /** The slot table at {@code widths}, emitted with {@code fault} planted. */
  private static String slotTable(int[] widths, int[] precisions, DecimalFault fault) {
    boolean[] declined = new boolean[1];
    int planted = 0;
    for (int lanes : widths) {
      for (int precision : precisions) {
        VarkaFusedKernel k = kernel(List.of(lo(0, precision), hi(0, precision)), 1, 0,
            at(lanes).withDecimalFault(fault)).kernel();
        for (int length : LENGTHS) {
          for (boolean masked : new boolean[] {false, true}) {
            String where = label(lanes) + ", precision " + precision + ", length " + length
                + (masked ? ", with nulls" : ", no null");
            long seed = 31L * lanes + 7L * precision + length + (masked ? 1 : 0);
            String m = checkWords(where, k, column(length, precision, masked, seed), declined);
            if (m != null) {
              return m;
            }
            for (int row : positions(length)) {
              for (BigInteger value : passing(precision)) {
                Column c = column(length, precision, masked, seed);
                c.values()[row] = value;
                c.nulls()[row] = false;
                m = checkWords(where + ", " + value + " at row " + row, k, c, declined);
                if (m != null || declined[0]) {
                  return m != null ? m : where + ": " + value + " passes, yet declined";
                }
              }
              for (BigInteger value : declining(precision)) {
                Column c = column(length, precision, masked, seed);
                c.values()[row] = value;
                c.nulls()[row] = false;
                String context = where + ", " + value + " at row " + row;
                m = checkWords(context, k, c, declined);
                if (m != null || !declined[0]) {
                  return m != null ? m : context + ": Spark raises, yet the kernel answered";
                }
                planted++;
                c.nulls()[row] = true;
                m = checkWords(context + " under a null", k, c, declined);
                if (m != null || declined[0]) {
                  return m != null ? m : context + ": a null slot declined";
                }
              }
            }
          }
        }
      }
    }
    return planted > 0 ? null : "the table planted no declining slot";
  }

  @Test
  @DisplayName("a 128-bit decimal column's slots load as their two words, and a valid slot "
      + "outside the precision declines the batch, a null one never")
  void slotTableOfThePair() {
    assertNull(slotTable(WIDTHS, PRECISIONS));
  }

  @Test
  @DisplayName("a body that reads both words of a column loads its slots once and checks them "
      + "once")
  void bothWordsShareOneLoad() {
    for (int lanes : WIDTHS) {
      var roots = List.<VarkaVectorIR>of(hi(0, 38), lo(0, 38),
          new IfElse(new IsNotNull(lo(0, 38)), hi(0, 38), lo(0, 38)));
      var emitted = VarkaEmitterFixtures.emit(roots, 1, 0, at(lanes));
      List<String> loops = VarkaEmitterTestSupport.methodNames(emitted.bytes()).stream()
          .filter(m -> m.startsWith("loop")).toList();
      assertTrue(!loops.isEmpty(), "no loop body");
      for (String loop : loops) {
        assertEquals(2, count(emitted.bytes(), loop, "rearrange"),
            () -> loop + " at " + label(lanes) + " takes the two rearranges once");
        assertEquals(2, count(emitted.bytes(), loop, "fromMemorySegment"),
            () -> loop + " at " + label(lanes) + " loads the two vectors of slots once");
      }
    }
  }

  /** How often {@code method} invokes a method named {@code name}. */
  private static int count(byte[] bytes, String method, String name) {
    int n = 0;
    for (var m : java.lang.classfile.ClassFile.of().parse(bytes).methods()) {
      if (!m.methodName().equalsString(method) || m.code().isEmpty()) {
        continue;
      }
      for (var element : m.code().get()) {
        if (element instanceof java.lang.classfile.instruction.InvokeInstruction invoke
            && invoke.name().equalsString(name)) {
          n++;
        }
      }
    }
    return n;
  }

  // ------------------------------------------------------------------------------------------
  // The word edges: each comparison against BigInteger, three-valued
  // ------------------------------------------------------------------------------------------

  /** The values the word edges pair: each word's sign bit, zero low words, both high signs. */
  private static List<BigInteger> edges() {
    var out = new ArrayList<BigInteger>();
    for (BigInteger v : List.of(BigInteger.ZERO, BigInteger.ONE, TWO_63.subtract(BigInteger.ONE),
        TWO_63, TWO_63.add(BigInteger.ONE), TWO_64.subtract(BigInteger.ONE), TWO_64,
        TWO_64.add(TWO_63), bound(38))) {
      out.add(v);
      if (v.signum() != 0) {
        out.add(v.negate());
      }
    }
    return out;
  }

  /** A comparison's answer for one row: true, false or unknown (null). */
  private interface Truth {
    Boolean of(BigInteger x, BigInteger y);
  }

  private static Truth order(CompareOp op) {
    return (x, y) -> {
      if (x == null || y == null) {
        return null;
      }
      int c = x.compareTo(y);
      return switch (op) {
        case LT -> c < 0;
        case LE -> c <= 0;
        case GT -> c > 0;
        case GE -> c >= 0;
        case EQ -> c == 0;
        case ULT, ULE, UGT, UGE -> throw new IllegalArgumentException(op.name());
      };
    };
  }

  /**
   * The kernel of {@code IfElse(cond, 1, 0)} and {@code IfElse(NOT cond, 1, 0)}, over two
   * 38-digit columns {@code x} at 0 and {@code y} at 1 (or {@code y} at the precision
   * {@code yPrecision} on the long lane, widened, where it is 18 or less), run over every pair of
   * {@link #edges()} with every null pattern: the first mismatch with {@code truth}, or null.
   */
  private static String edgeTable(String name, Function<int[], Cond> cond, int yPrecision,
      Truth truth, int[] widths) {
    return edgeTable(name, cond, yPrecision, truth, widths, DecimalFault.NONE);
  }

  /** {@link #edgeTable} emitted with {@code fault} planted. */
  private static String edgeTable(String name, Function<int[], Cond> cond, int yPrecision,
      Truth truth, int[] widths, DecimalFault fault) {
    List<BigInteger> xs = edges();
    List<BigInteger> ys = yPrecision > DecimalColumn.MAX_LONG_PRECISION ? edges()
        : List.of(BigInteger.ZERO, BigInteger.ONE, BigInteger.ONE.negate(),
            BigInteger.TEN.pow(yPrecision).subtract(BigInteger.ONE),
            BigInteger.TEN.pow(yPrecision).subtract(BigInteger.ONE).negate());
    int n = xs.size() * ys.size() + 3;
    var x = new BigInteger[n];
    var y = new BigInteger[n];
    var xNull = new boolean[n];
    var yNull = new boolean[n];
    int r = 0;
    for (BigInteger a : xs) {
      for (BigInteger b : ys) {
        x[r] = a;
        y[r] = b;
        r++;
      }
    }
    // The null patterns: x null, y null, both, each over garbage.
    for (int k = 0; k < 3; k++) {
      x[r] = TWO_64.add(BigInteger.valueOf(k));
      y[r] = BigInteger.valueOf(-k);
      xNull[r] = k != 1;
      yNull[r] = k != 0;
      r++;
    }
    Cond c = cond.apply(new int[] {38, yPrecision});
    var roots = List.<VarkaVectorIR>of(
        new IfElse(c, new LiteralSlot(0, LaneType.LONG), new LiteralSlot(1, LaneType.LONG)),
        new IfElse(new Not(c), new LiteralSlot(0, LaneType.LONG),
            new LiteralSlot(1, LaneType.LONG)));
    for (int lanes : widths) {
      VarkaFusedKernel k =
          kernel(roots, 2, EDGE_LITERALS.length, at(lanes).withDecimalFault(fault)).kernel();
      // Without nulls too, so the dense body runs.
      for (boolean withNulls : new boolean[] {false, true}) {
        int rows = withNulls ? n : n - 3;
        String where = name + " at " + label(lanes) + (withNulls ? ", with nulls" : "");
        try (BufferAllocator allocator =
            ArrowUtils.rootAllocator().newChildAllocator("varka-pair-edges", 0, Long.MAX_VALUE);
            DecimalVector vx = vector(new Column(java.util.Arrays.copyOf(x, rows),
                java.util.Arrays.copyOf(xNull, rows), 38), allocator);
            DecimalVector vy = vector(new Column(java.util.Arrays.copyOf(y, rows),
                java.util.Arrays.copyOf(yNull, rows), yPrecision), allocator)) {
          Answer got = run(k, List.of(vx, vy), 2, EDGE_LITERALS);
          if (got.status() != 0) {
            return where + ": declined with status " + got.status();
          }
          for (int i = 0; i < rows; i++) {
            Boolean want = truth.of(xNull[i] ? null : x[i], yNull[i] ? null : y[i]);
            long t = Boolean.TRUE.equals(want) ? 1L : 0L;
            long f = Boolean.FALSE.equals(want) ? 1L : 0L;
            if (got.values()[0][i] != t || got.values()[1][i] != f) {
              return where + ": row " + i + " (" + (xNull[i] ? "null" : x[i]) + ", "
                  + (yNull[i] ? "null" : y[i]) + ") is " + want + ", the kernel answers ("
                  + got.values()[0][i] + ", " + got.values()[1][i] + ")";
            }
            // Each root picks one of two literals, so its answer is never null, a null
            // operand included.
            if (!got.valid()[0][i] || !got.valid()[1][i]) {
              return where + ": row " + i + " (" + (xNull[i] ? "null" : x[i]) + ", "
                  + (yNull[i] ? "null" : y[i]) + ") picks a literal, the kernel answers null ("
                  + got.valid()[0][i] + ", " + got.valid()[1][i] + " valid)";
            }
          }
        }
      }
    }
    return null;
  }

  /** The edge tables' literals: the true and false answers, and the sign shift's count. */
  private static final long[] EDGE_LITERALS = {1L, 0L, 63L};

  private static Cond compare(CompareOp op, int[] p) {
    return DecimalPairLowering.compare(op, lo(0, p[0]), hi(0, p[0]), lo(1, p[1]), hi(1, p[1]));
  }

  /** Every order and the equality, over two 38-digit columns: the first mismatch, or null. */
  private static String everyComparison(int[] widths) {
    return everyComparison(widths, DecimalFault.NONE);
  }

  /** {@link #everyComparison(int[])} emitted with {@code fault} planted. */
  private static String everyComparison(int[] widths, DecimalFault fault) {
    for (CompareOp op : CompareOp.signed()) {
      String m =
          edgeTable("x " + op + " y", p -> compare(op, p), 38, order(op), widths, fault);
      if (m != null) {
        return m;
      }
    }
    return null;
  }

  @Test
  @DisplayName("each comparison of two 128-bit decimals answers as BigInteger's, unknown where "
      + "either is null, at the words' every edge")
  void wordEdgesOfEachComparison() {
    assertNull(everyComparison(WIDTHS));
  }

  @Test
  @DisplayName("x <> y is NOT(x = y), unknown where either is null")
  void wordEdgesOfNotEqual() {
    Truth eq = order(CompareOp.EQ);
    assertNull(edgeTable("x <> y", p -> new Not(compare(CompareOp.EQ, p)), 38,
        (x, y) -> eq.of(x, y) == null ? null : !eq.of(x, y), WIDTHS));
  }

  @Test
  @DisplayName("x <=> y is true where both are null or both are equal, false otherwise, "
      + "never unknown")
  void wordEdgesOfNullSafeEqual() {
    assertNull(edgeTable("x <=> y", p -> DecimalPairLowering.nullSafeEqual(lo(0, p[0]),
        hi(0, p[0]), lo(1, p[1]), hi(1, p[1])), 38,
        (x, y) -> x == null || y == null ? x == y : x.equals(y), WIDTHS));
  }

  @Test
  @DisplayName("a decimal of 18 digits compared with a 38-digit one is its long sign-extended")
  void wordEdgesOfAWidenedOperand() {
    for (CompareOp op : CompareOp.signed()) {
      assertNull(edgeTable("x " + op + " d18", p -> {
        var y = new DecimalColumn(1, LaneType.LONG, p[1]);
        return DecimalPairLowering.compare(op, lo(0, p[0]), hi(0, p[0]), y,
            new VarkaVectorIR.BitOp(VarkaVectorIR.BitOpKind.SHR, y,
                new LiteralSlot(2, LaneType.LONG)));
      }, 18, order(op), WIDTHS));
    }
  }

  @Test
  @DisplayName("a null test over a word reads the column's validity and still checks the slots")
  void nullTestChecksTheSlots() {
    for (int lanes : WIDTHS) {
      Cond c = new IsNotNull(lo(0, 38));
      var roots = List.<VarkaVectorIR>of(
          new IfElse(c, new LiteralSlot(0, LaneType.LONG), new LiteralSlot(1, LaneType.LONG)));
      VarkaFusedKernel k = kernel(roots, 1, 2, at(lanes)).kernel();
      for (int length : LENGTHS) {
        for (boolean masked : new boolean[] {false, true}) {
          Column col = column(length, 38, masked, 5L * lanes + length);
          try (BufferAllocator allocator =
              ArrowUtils.rootAllocator().newChildAllocator("varka-pair-null", 0, Long.MAX_VALUE);
              DecimalVector v = vector(col, allocator)) {
            Answer got = run(k, List.of(v), 1, new long[] {1L, 0L});
            assertEquals(0, got.status());
            for (int i = 0; i < length; i++) {
              assertEquals(col.nulls()[i] ? 0L : 1L, got.values()[0][i], "row " + i);
            }
          }
          col.values()[length - 1] = BigInteger.TEN.pow(38);
          col.nulls()[length - 1] = false;
          try (BufferAllocator allocator =
              ArrowUtils.rootAllocator().newChildAllocator("varka-pair-null", 0, Long.MAX_VALUE);
              DecimalVector v = vector(col, allocator)) {
            assertEquals(VarkaFusedKernel.STATUS_DECIMAL_SLOT,
                run(k, List.of(v), 1, new long[] {1L, 0L}).status(),
                label(lanes) + ", length " + length + ": a bad slot under IS NOT NULL");
          }
        }
      }
    }
  }

  // ------------------------------------------------------------------------------------------
  // The leaf's record, and the planted faults (row 504)
  // ------------------------------------------------------------------------------------------

  @Test
  @DisplayName("a decimal word renders apart from the 64-bit leaf and is refused at 18 digits "
      + "or fewer")
  void wordRendersAndRefuses() {
    assertEquals("decw:0:lo:p38", VarkaVectorIR.canonical(lo(0, 38)));
    assertEquals("decw:3:hi:p19", VarkaVectorIR.canonical(hi(3, 19)));
    assertEquals(lo(0, 38), hi(0, 38).low());
    assertEquals(hi(0, 38), lo(0, 38).sibling());
    assertThrows(IllegalArgumentException.class, () -> lo(0, 18));
    assertThrows(IllegalArgumentException.class, () -> lo(0, 39));
    assertThrows(IllegalArgumentException.class, () -> lo(-1, 38));
    assertEquals(new BigDecimal(bound(38)).toBigInteger(), lo(0, 38).bound().value());
  }

  @Test
  @DisplayName("planted: the low words compared signed is caught by the word edges, under a key "
      + "of its own")
  void plantedSignedLowWord() {
    assertNotNull(everyComparison(new int[] {4}, DecimalFault.LOW_WORD_SIGNED),
        "a signed low-word compare must disagree at a low word's sign bit");
    // The fault is the emission's option, not a JVM-wide switch: the IR is the clean one, the
    // key and the bytes are not, and the clean emission is unchanged.
    var roots = List.<VarkaVectorIR>of(new IfElse(compare(CompareOp.LT, new int[] {38, 38}),
        new LiteralSlot(0, LaneType.LONG), new LiteralSlot(1, LaneType.LONG)));
    VarkaEmitOptions clean = at(4);
    VarkaEmitOptions faulty = clean.withDecimalFault(DecimalFault.LOW_WORD_SIGNED);
    assertNotEquals(VarkaShapeCacheImpl.shapeHash(new VarkaShapeKey(roots, 2, 2, clean)),
        VarkaShapeCacheImpl.shapeHash(new VarkaShapeKey(roots, 2, 2, faulty)));
    String name = "org.apache.spark.sql.varka.execution.VarkaFusedSignedLowWord";
    byte[] before = VarkaLoopEmitter.emit(name, roots, 2, 2, null, null, clean);
    assertTrue(!java.util.Arrays.equals(before,
        VarkaLoopEmitter.emit(name, roots, 2, 2, null, null, faulty)), "the fault moved no byte");
    assertArrayEquals(before, VarkaLoopEmitter.emit(name, roots, 2, 2, null, null, clean));
  }

  @Test
  @DisplayName("planted: the check's bound at 10^p is caught by the slot table's 10^p row")
  void plantedBoundAtThePower() {
    assertNotNull(slotTable(new int[] {4}, new int[] {38}, DecimalFault.BOUND_AT_POWER),
        "a check at 10^p must pass the slot Spark raises on");
  }

  @Test
  @DisplayName("planted: the sibling word parked from the wrong rearrange is caught by every row")
  void plantedSiblingFromTheWrongRearrange() {
    assertNotNull(slotTable(new int[] {4}, new int[] {38},
        DecimalFault.SIBLING_FROM_WRONG_REARRANGE), "both words read from one half must disagree");
    assertNotNull(everyComparison(new int[] {4}, DecimalFault.SIBLING_FROM_WRONG_REARRANGE),
        "the comparisons over one half must disagree");
  }

  @Test
  @DisplayName("planted: a load's fault is cached under a key of its own, so the clean shape "
      + "emitted after it is clean")
  void plantedFaultNeverServesTheCleanShape() {
    var roots = List.<VarkaVectorIR>of(lo(0, 38), hi(0, 38));
    VarkaEmitOptions clean = at(4);
    VarkaShapeKey cleanKey = new VarkaShapeKey(roots, 1, 0, clean);
    String name = "org.apache.spark.sql.varka.execution.VarkaFusedFaultKey";
    byte[] before = VarkaLoopEmitter.emit(name, roots, 1, 0, null, null, clean);
    // A slot holding 10^38, which Spark's reader raises on, so the clean kernel declines where
    // the check at 10^p answers; and a column of valid slots, whose words the wrong sibling
    // reads from one half.
    Column outside = column(8, 38, false, 3L);
    outside.values()[5] = BigInteger.TEN.pow(38);
    Column valid = column(8, 38, false, 4L);
    boolean[] declined = new boolean[1];
    var cache = new VarkaShapeCacheImpl(8);
    ClassLoader parent = VarkaDecimalPairSuite.class.getClassLoader();
    for (DecimalFault fault : List.of(DecimalFault.BOUND_AT_POWER,
        DecimalFault.SIBLING_FROM_WRONG_REARRANGE)) {
      VarkaShapeKey faultyKey =
          new VarkaShapeKey(roots, 1, 0, clean.withDecimalFault(fault));
      assertNotEquals(VarkaShapeCacheImpl.shapeHash(cleanKey),
          VarkaShapeCacheImpl.shapeHash(faultyKey), fault + " shares the clean shape's hash");
      VarkaShapeLookup faulty = cache.getOrEmit(parent, faultyKey, "faulty");
      Column caught = fault == DecimalFault.BOUND_AT_POWER ? outside : valid;
      assertNotNull(checkWords(fault.name(), faulty.entry().newKernel(), caught, declined),
          fault + " must answer wrongly through the cache");
      VarkaShapeLookup fresh = cache.getOrEmit(parent, cleanKey, "clean");
      assertNull(checkWords("clean after " + fault, fresh.entry().newKernel(), outside,
          declined));
      assertTrue(declined[0], "the clean kernel declines 10^38");
      assertNull(checkWords("clean after " + fault, fresh.entry().newKernel(), valid,
          declined));
      assertNotEquals(faulty.entry().className(), fresh.entry().className());
      assertArrayEquals(before, VarkaLoopEmitter.emit(name, roots, 1, 0, null, null, clean),
          "the clean bytes moved after " + fault);
    }
  }
}
