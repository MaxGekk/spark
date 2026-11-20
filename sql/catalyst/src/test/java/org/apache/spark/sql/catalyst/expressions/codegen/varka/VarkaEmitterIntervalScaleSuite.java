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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.IntPredicate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.apache.spark.sql.catalyst.expressions.DivideDTInterval;
import org.apache.spark.sql.catalyst.expressions.DivideYMInterval;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.catalyst.expressions.MultiplyDTInterval;
import org.apache.spark.sql.catalyst.expressions.MultiplyYMInterval;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaIntervalCompiler;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.YearMonthIntervalType;

/**
 * An interval times or over a floating factor (VARKA-342, `VARKA-342.md` 2.4 and 3.7): Guava's
 * HALF_UP of the double product or quotient, built by {@link VarkaIntervalCompiler#scaledHalfUp}
 * over the conversions to and from the double lane and a floating guard. Each kernel is held to
 * Spark's own evaluation of {@code MultiplyYMInterval} and {@code DivideYMInterval} (and their
 * day-time twins) through the reference: over 3.7's pinned cells - the ties at {@code +-0.5},
 * {@code +-2.5} and {@code 2^31 - 1.5}, each guard's ends and their outer neighbours, a NaN and
 * an infinite factor, a zero divisor of either sign - and the cross product of edge counts and
 * factors, at every width, length and null pattern, both bodies. A cell Spark raises on is a
 * batch the kernel declines; a null one is not. The division is built here though the compiler
 * keeps it declined until its error keeps the query context (VARKA-414).
 */
@VarkaSuite(ownJvm = true)
public final class VarkaEmitterIntervalScaleSuite {

  private static final ColumnRef MONTHS = new ColumnRef(0, LaneType.INT);
  private static final ColumnRef MICROS = new ColumnRef(0, LaneType.LONG);
  private static final ColumnRef X = new ColumnRef(1, LaneType.DOUBLE);
  private static final ColumnRef F = new ColumnRef(1, LaneType.FLOAT);

  private static final int[] WIDE_LANES = {2, 4, 8};
  private static final int[] LENGTHS = {1, 2, 7, 8, 9, 17, 33};
  private static final List<IntPredicate> PATTERNS = List.of(
      i -> false, i -> i % 5 == 0, i -> i % 2 == 1);

  /** The level whose x86 host has AVX512DQ, where the long lane's conversions are one call. */
  private static final VarkaEmitOptions DQ =
      VarkaMatrixRuns.BASE.withUseAVX(VarkaEmitOptions.AVX512_LEVEL);

  private static final long[] MONTH_COUNTS = {0, 1, -1, 2, -2, 3, 5, -5, 7, 1000, -1000,
      123456789, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE - 1,
      Integer.MIN_VALUE + 1};

  private static final long[] MICRO_COUNTS = {0, 1, -1, 2, -2, 3, 5, -5, 7, 86_400_000_000L,
      -86_400_000_000L, 1L << 53, (1L << 53) + 1, -(1L << 53) - 1, 1L << 51, (1L << 51) + 1,
      Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE - 511, Long.MAX_VALUE - 512};

  private static final double[] FACTORS = {0.5, -0.5, 2.5, -2.5, 1.0, -1.0, 0.25, 1.5, -1.5,
      0.3, 2.0, -2.0, 3.0, 4.0, 1e-9, -1e-9, 0.1, 1e10, -1e10, 0.0, -0.0, Double.NaN,
      Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.MIN_VALUE, Double.MAX_VALUE,
      0.49999999999999994, 0.5000000000000001, 1.0000000000000002};

  /**
   * The values a product takes at a count of one: 3.7's ties and the guards' ends with their
   * outer neighbours, so that {@code 1 * x} lands on each exactly.
   */
  private static final double[] PINNED = {0.5, -0.5, 1.5, -1.5, 2.5, -2.5, 0x1p31 - 1.5,
      -0x1p31 + 1.5, 0x1p31 - 0.5, -0x1p31 - 0.5, Math.nextDown(0x1p31 - 0.5),
      Math.nextUp(-0x1p31 - 0.5), 0x1p31 - 1.0, -0x1p31, 0x1p52 + 0.5, 0x1p52 + 1.0,
      -0x1p52 - 1.0, 0x1p53, Math.nextDown(0x1p63), -0x1p63, 0x1p63, Math.nextDown(-0x1p63),
      VarkaIntervalCompiler.HALF_UP_INT_LO, VarkaIntervalCompiler.HALF_UP_INT_HI,
      VarkaIntervalCompiler.HALF_UP_LONG_LO, VarkaIntervalCompiler.HALF_UP_LONG_HI};

  /** The two literal tables a hand-built tree indexes, as the compiler's slot tables do. */
  private static final class Lits {
    final List<Integer> ints = new ArrayList<>();
    final List<Long> longs = new ArrayList<>();

    VarkaVectorIR dbl(double v) {
      longs.add(Double.doubleToRawLongBits(v));
      return new LiteralSlot(longs.size() - 1, LaneType.DOUBLE);
    }

    VarkaVectorIR count(long v, LaneType lane) {
      if (lane == LaneType.INT) {
        ints.add((int) v);
        return new LiteralSlot(ints.size() - 1, LaneType.INT);
      }
      longs.add(v);
      return new LiteralSlot(longs.size() - 1, LaneType.LONG);
    }

    int[] intArray() {
      return ints.stream().mapToInt(Integer::intValue).toArray();
    }

    long[] longArray() {
      return longs.stream().mapToLong(Long::longValue).toArray();
    }
  }

  /** One form: the operation, the count's lane, the factor's, and the tree with its literals. */
  private record Form(IntOp op, LaneType lane, LaneType factor, VarkaVectorIR root, Lits lits) {
    static Form of(IntOp op, LaneType lane, LaneType factor) {
      var lits = new Lits();
      VarkaVectorIR root = VarkaIntervalCompiler.scaledHalfUp(op,
          lane == LaneType.INT ? MONTHS : MICROS, factor == LaneType.FLOAT ? F : X, lits::dbl,
          v -> lits.count(v, lane));
      return new Form(op, lane, factor, root, lits);
    }

    String name() {
      return (lane == LaneType.INT ? "ym" : "dt") + (op == IntOp.MUL ? " * " : " / ")
          + (factor == LaneType.FLOAT ? "f" : "x");
    }
  }

  /** A cell: the count, the factor's lane bits, and Spark's answer (empty where it raises). */
  private record Cell(long count, long factorBits, Optional<Long> spark) { }

  private static long factorBits(LaneType factor, double v) {
    return factor == LaneType.FLOAT ? Float.floatToRawIntBits((float) v)
        : Double.doubleToRawLongBits(v);
  }

  /**
   * Spark's answer for one cell: the expression's own {@code eval} over two literals, the
   * months or microseconds it returns, or empty where it raises.
   */
  private static Optional<Long> spark(Form form, long count, long bits) {
    Literal interval = form.lane() == LaneType.INT
        ? Literal$.MODULE$.create((int) count, new YearMonthIntervalType(
            YearMonthIntervalType.YEAR(), YearMonthIntervalType.MONTH()))
        : Literal$.MODULE$.create(count, new DayTimeIntervalType(DayTimeIntervalType.DAY(),
            DayTimeIntervalType.SECOND()));
    Literal factor = form.factor() == LaneType.FLOAT
        ? Literal$.MODULE$.create(Float.intBitsToFloat((int) bits), DataTypes.FloatType)
        : Literal$.MODULE$.create(Double.longBitsToDouble(bits), DataTypes.DoubleType);
    boolean months = form.lane() == LaneType.INT;
    Expression e = form.op() == IntOp.MUL
        ? (months ? new MultiplyYMInterval(interval, factor)
            : new MultiplyDTInterval(interval, factor))
        : (months ? new DivideYMInterval(interval, factor)
            : new DivideDTInterval(interval, factor));
    try {
      Object v = e.eval(null);
      return Optional.of(v instanceof Integer i ? (long) i : (Long) v);
    } catch (RuntimeException raised) {
      return Optional.empty();
    }
  }

  private static List<Cell> cells(Form form) {
    long[] counts = form.lane() == LaneType.INT ? MONTH_COUNTS : MICRO_COUNTS;
    var cells = new ArrayList<Cell>();
    for (long m : counts) {
      for (double x : FACTORS) {
        long bits = factorBits(form.factor(), x);
        cells.add(new Cell(m, bits, spark(form, m, bits)));
      }
    }
    for (double x : PINNED) {
      for (long m : new long[] {1, -1}) {
        long bits = factorBits(form.factor(), x);
        cells.add(new Cell(m, bits, spark(form, m, bits)));
      }
    }
    return cells;
  }

  private static VarkaKernelChecks.LongBatch batch(int length, IntPredicate isNull,
      List<Cell> cells) {
    var nulls = new boolean[2][length];
    var values = new long[2][length];
    for (int i = 0; i < length; i++) {
      Cell c = cells.get((i * 7) % cells.size());
      nulls[0][i] = isNull.test(i);
      nulls[1][i] = isNull.test(i + 3);
      values[0][i] = c.count();
      values[1][i] = c.factorBits();
    }
    return new VarkaKernelChecks.LongBatch(length, nulls, values, false);
  }

  /**
   * The reference answers Spark's value at every cell Spark answers: the lowering is Guava's
   * HALF_UP there, and not only the kernel's own model of it.
   */
  private static void referenceIsSpark(Form form, List<Cell> answered) {
    int[] ints = form.lits().intArray();
    long[] longs = form.lits().longArray();
    for (Cell c : answered) {
      OptionalLong got = VarkaReference.mixedValue(form.root(), new long[] {c.count(),
          c.factorBits()}, new boolean[2], ints, longs);
      String ctx = form.name() + " at " + c.count() + ", " + Long.toHexString(c.factorBits());
      assertTrue(got.isPresent(), ctx);
      assertEquals(c.spark().orElseThrow(), got.getAsLong(), ctx);
    }
  }

  /**
   * {@code form} at every width under {@code options}: over the cells Spark answers, no batch
   * declines and every row is the reference's; each cell Spark raises on, put among them, declines
   * both bodies, and the same cell null declines neither the masked body nor the answer.
   */
  private static void check(Form form, VarkaEmitOptions options) {
    List<Cell> all = cells(form);
    List<Cell> answered = all.stream().filter(c -> c.spark().isPresent()).toList();
    List<Cell> raised = all.stream().filter(c -> c.spark().isEmpty()).toList();
    assertTrue(answered.size() > 100 && raised.size() > 10, form.name() + ": " + answered.size()
        + " answered, " + raised.size() + " raised");
    referenceIsSpark(form, answered);
    List<VarkaVectorIR> roots = List.of(form.root());
    int[] ints = form.lits().intArray();
    long[] longs = form.lits().longArray();
    int numLits = ints.length + longs.length;
    for (int lanes : WIDE_LANES) {
      var emitted = VarkaEmitterFixtures.emit(roots, 2, numLits, options.withLanesOverride(lanes));
      for (int length : LENGTHS) {
        for (int p = 0; p < PATTERNS.size(); p++) {
          String ctx = form.name() + " at " + lanes + " lanes, length " + length + ", pattern " + p;
          assertTrue(VarkaKernelChecks.runAndCompareMixed(ctx, emitted.className(),
              emitted.bytes(), roots, 2, ints, longs, batch(length, PATTERNS.get(p), answered),
              false, null), ctx);
        }
      }
      String whole = form.name() + " at " + lanes + " lanes over every answered cell";
      assertTrue(VarkaKernelChecks.runAndCompareMixed(whole, emitted.className(),
          emitted.bytes(), roots, 2, ints, longs, batch(answered.size(), i -> false, answered),
          false, null), whole);
      for (Cell bad : raised) {
        var cells = new ArrayList<>(answered.subList(0, 16));
        cells.add(3, bad);
        var nulls = new boolean[2][17];
        var values = new long[2][17];
        for (int i = 0; i < 17; i++) {
          values[0][i] = cells.get(i).count();
          values[1][i] = cells.get(i).factorBits();
        }
        var b = new VarkaKernelChecks.LongBatch(17, nulls, values, false);
        String ctx = form.name() + " at " + lanes + " lanes, " + bad.count() + " and "
            + Long.toHexString(bad.factorBits());
        List<Integer> live = VarkaKernelChecks.runEachBodyMixedStatus(ctx, emitted.className(),
            emitted.bytes(), roots, 2, ints, longs, b);
        assertNotEquals(0, live.get(0), ctx);
        assertNotEquals(0, live.get(1), ctx);
        nulls[1][3] = true;
        List<Integer> masked = VarkaKernelChecks.runEachBodyMixedStatus(ctx + ", null",
            emitted.className(), emitted.bytes(), roots, 2, ints, longs, b);
        assertEquals(0, masked.get(0), ctx + ", null");
      }
    }
  }

  @Test
  @DisplayName("ym * x and ym * f round the double product to the months as Spark does, "
      + "and a product Spark raises on declines its batch")
  void yearMonthProduct() {
    check(Form.of(IntOp.MUL, LaneType.INT, LaneType.DOUBLE), VarkaMatrixRuns.BASE);
    check(Form.of(IntOp.MUL, LaneType.INT, LaneType.FLOAT), VarkaMatrixRuns.BASE);
  }

  @Test
  @DisplayName("ym / x and ym / f round the double quotient to the months as Spark does, "
      + "and a zero divisor of either sign declines its batch")
  void yearMonthQuotient() {
    check(Form.of(IntOp.DIV, LaneType.INT, LaneType.DOUBLE), VarkaMatrixRuns.BASE);
    check(Form.of(IntOp.DIV, LaneType.INT, LaneType.FLOAT), VarkaMatrixRuns.BASE);
  }

  @Test
  @DisplayName("the HALF_UP lowering takes an int or long count and a MUL or DIV only")
  void refusals() {
    var lits = new Lits();
    assertThrows(IllegalArgumentException.class, () -> VarkaIntervalCompiler.scaledHalfUp(
        IntOp.ADD, MONTHS, X, lits::dbl, v -> lits.count(v, LaneType.INT)));
    assertThrows(IllegalArgumentException.class, () -> VarkaIntervalCompiler.scaledHalfUp(
        IntOp.MUL, X, X, lits::dbl, v -> lits.count(v, LaneType.INT)));
    assertThrows(IllegalArgumentException.class, () -> VarkaIntervalCompiler.scaledHalfUp(
        IntOp.MUL, MONTHS, MICROS, lits::dbl, v -> lits.count(v, LaneType.INT)));
  }

  /** The day-time forms over both factor lanes under {@code options}. */
  static void checkDayTime(VarkaEmitOptions options) {
    for (IntOp op : List.of(IntOp.MUL, IntOp.DIV)) {
      for (LaneType factor : List.of(LaneType.DOUBLE, LaneType.FLOAT)) {
        check(Form.of(op, LaneType.LONG, factor), options);
      }
    }
  }

  @Test
  @DisplayName("dt * x and dt / x round to the microseconds as Spark does where the level "
      + "assures AVX512DQ")
  void dayTimeAtLevelThree() {
    checkDayTime(DQ);
  }

  @Test
  @DisplayName("dt * x and dt / x round to the microseconds as Spark does at AVX2, under both "
      + "whole-range conversion forms")
  void dayTimeAtAvx2() {
    var avx2 = VarkaMatrixRuns.BASE.withUseAVX(2);
    checkDayTime(avx2.withWideConversionForm(VarkaEmitOptions.WideConversionForm.XOR_SPLIT));
    checkDayTime(avx2.withWideConversionForm(VarkaEmitOptions.WideConversionForm.SPLIT));
  }
}
