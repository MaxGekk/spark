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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoublePredicate;
import java.util.function.IntPredicate;
import java.util.function.IntToLongFunction;
import java.util.stream.LongStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.apache.spark.sql.catalyst.expressions.CastUtils;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions.WideConversionForm;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ConvertLane;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.GuardedRange;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntArith;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.NarrowLane;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;
import org.apache.spark.sql.types.DoubleExactNumeric$;
import org.apache.spark.sql.types.FloatExactNumeric$;

/**
 * The conversions from a floating lane to an integral one (VARKA-343, `VARKA-343.md` 2.4):
 * {@code F2I} and {@code D2I}, one {@code convertShape} at every level, and {@code F2L} and
 * {@code D2L} where the level assures AVX512DQ; a narrow target as the int answer wrapped; and
 * the three modes, LEGACY the bare conversion, ANSI and TRY the conversion over a floating
 * {@link GuardedRange} of {@link VarkaTypes#castBounds}, failing and nulling. Each kernel is held
 * to the reference, Java's cast, over every width, length, null pattern and both bodies, at the
 * cells of 2.4: each bound and its outer neighbour, NaN, the infinities, both zeros, the
 * truncations that a rounding form would get wrong, and LEGACY's wrap.
 */
@VarkaSuite(ownJvm = true)
public final class VarkaEmitterFloatToIntegralSuite {

  private static final ColumnRef F = new ColumnRef(0, LaneType.FLOAT);
  private static final ColumnRef X = new ColumnRef(0, LaneType.DOUBLE);

  private static final int[] WIDE_LANES = {2, 4, 8};
  private static final int[] NARROW_LANES = {4, 8, 16};
  private static final int[] LENGTHS = {1, 2, 7, 8, 9, 16, 17, 33, 129};
  private static final List<IntPredicate> PATTERNS = List.of(
      i -> false, i -> i % 5 == 0, i -> i % 2 == 1, i -> true);

  /** The level whose x86 host has AVX512DQ, where {@code F2L} and {@code D2L} are admitted. */
  private static final VarkaEmitOptions DQ =
      VarkaMatrixRuns.BASE.withUseAVX(VarkaEmitOptions.AVX512_LEVEL);

  /** The cells of 2.4 common to both sources, as values. */
  private static final double[] CELLS = {Double.NaN, Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY, 0.0, -0.0, 0.7, -0.7, 1.5, -1.5, 0.5, 128.0, 300.7, -129.5,
      1e10, -1e10, 0x1p31, -0x1p31, 0x1p31 - 0.5, 0x1p63, -0x1p63, 3e18, 32767.9, -32768.9,
      2147483520.0, 1e30, -1e30, 0x1p51, -0x1p51, 0x1p51 - 0.5, -(0x1p51 - 0.5), 0x1p62,
      Math.nextDown(0x1p63), -Math.nextDown(0x1p63)};

  /** The cells, a source's bounds for every target and their outer neighbours, as bits. */
  private static long[] cells(LaneType source) {
    var values = new java.util.ArrayList<Double>();
    for (double v : CELLS) {
      values.add(v);
    }
    for (LaneType target : List.of(LaneType.BYTE, LaneType.SHORT, LaneType.INT, LaneType.LONG)) {
      double[] b = VarkaTypes.castBounds(source, target);
      values.add(b[0]);
      values.add(b[1]);
      values.add(outward(source, b[0], false));
      values.add(outward(source, b[1], true));
    }
    var bits = new long[values.size()];
    for (int k = 0; k < bits.length; k++) {
      double v = values.get(k);
      bits[k] = source == LaneType.FLOAT ? Float.floatToRawIntBits((float) v)
          : Double.doubleToRawLongBits(v);
    }
    return bits;
  }

  /** {@link #cells} inside the bounds of {@code target}, where an ANSI cast never declines. */
  private static long[] inside(LaneType source, LaneType target) {
    double[] b = VarkaTypes.castBounds(source, target);
    return LongStream.of(cells(source)).filter(bits -> {
      double v = source == LaneType.FLOAT ? Float.intBitsToFloat((int) bits)
          : Double.longBitsToDouble(bits);
      return v >= b[0] && v <= b[1];
    }).toArray();
  }

  private static double outward(LaneType source, double v, boolean up) {
    if (source == LaneType.FLOAT) {
      return up ? Math.nextUp((float) v) : Math.nextDown((float) v);
    }
    return up ? Math.nextUp(v) : Math.nextDown(v);
  }

  private static VarkaKernelChecks.LongBatch batch(int length, int pattern, long[] cells) {
    var nulls = new boolean[1][length];
    var values = new long[1][length];
    IntPredicate p = PATTERNS.get(pattern);
    for (int i = 0; i < length; i++) {
      nulls[0][i] = p.test(i);
      values[0][i] = cells[(i * 7) % cells.length];
    }
    return new VarkaKernelChecks.LongBatch(length, nulls, values, length > 1 && pattern == 0);
  }

  /**
   * {@code root} at every width of its drive under {@code base}, held to the reference over every
   * length and null pattern, both bodies; a batch may decline only where {@code declines}, and
   * then some batch of every width must not.
   */
  private static void check(String name, VarkaVectorIR root, long[] cells, VarkaEmitOptions base,
      boolean declines) {
    List<VarkaVectorIR> roots = List.of(root);
    Optional<LaneType> planned = VarkaKernelPlan.driveOf(roots, base.withLanesOverride(4));
    assertTrue(planned.isPresent(), () -> name + ": "
        + VarkaKernelPlan.refusalOf(roots, base.withLanesOverride(4)));
    int[] widths = planned.get().bits() == Long.SIZE ? WIDE_LANES : NARROW_LANES;
    for (int lanes : widths) {
      var emitted = VarkaEmitterFixtures.emit(roots, 1, 0, base.withLanesOverride(lanes));
      int compared = 0;
      for (int length : LENGTHS) {
        for (int pattern = 0; pattern < PATTERNS.size(); pattern++) {
          String context = name + " at " + lanes + " lanes, length " + length + ", pattern "
              + pattern;
          boolean ran = VarkaKernelChecks.runAndCompareMixed(context, emitted.className(),
              emitted.bytes(), roots, 1, new int[0], new long[0],
              batch(length, pattern, cells), declines, null);
          assertTrue(ran || declines, context);
          compared += ran ? 1 : 0;
        }
      }
      assertTrue(compared > 0, name + " at " + lanes + " lanes: every batch declined");
    }
  }

  private static ConvertLane convert(VarkaVectorIR child, LaneType target) {
    return new ConvertLane(child, target);
  }

  private static GuardedRange guard(VarkaVectorIR child, LaneType target, Overflow mode) {
    double[] b = VarkaTypes.castBounds(child.laneType(), target);
    return GuardedRange.floating(child, b[0], b[1], mode);
  }

  /** Spark's cast in {@code mode} as the IR the compiler builds for it. */
  private static VarkaVectorIR cast(VarkaVectorIR child, LaneType target, Overflow mode) {
    VarkaVectorIR value = mode == Overflow.WRAP ? child : guard(child, target, mode);
    var converted = convert(value, target == LaneType.LONG ? LaneType.LONG : LaneType.INT);
    return target.isNarrow() ? new NarrowLane(converted, target) : converted;
  }

  // ---------------------------------------------------------------------------------------------
  // The bounds and the guard
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("the bounds are VARKA-343 2.4's table and the edges of Spark's own ANSI casts")
  void bounds() {
    assertEquals(List.of(Math.nextUp(-0x1p31 - 1), Math.nextDown(0x1p31)),
        boundsOf(LaneType.DOUBLE, LaneType.INT));
    assertEquals(List.of(-0x1p31, 2147483520.0), boundsOf(LaneType.FLOAT, LaneType.INT));
    assertEquals(List.of(-0x1p63, 0x1p63), boundsOf(LaneType.DOUBLE, LaneType.LONG));
    assertEquals(List.of(-0x1p63, 0x1p63), boundsOf(LaneType.FLOAT, LaneType.LONG));
    assertEquals(List.of(Math.nextUp(-32769.0), Math.nextDown(32768.0)),
        boundsOf(LaneType.DOUBLE, LaneType.SHORT));
    assertEquals(List.of(Math.nextUp(-129.0), Math.nextDown(128.0)),
        boundsOf(LaneType.DOUBLE, LaneType.BYTE));
    assertEquals(List.of((double) Math.nextUp(-129.0f), (double) Math.nextDown(128.0f)),
        boundsOf(LaneType.FLOAT, LaneType.BYTE));
    for (LaneType source : List.of(LaneType.FLOAT, LaneType.DOUBLE)) {
      for (LaneType target : List.of(LaneType.BYTE, LaneType.SHORT, LaneType.INT,
          LaneType.LONG)) {
        double[] b = VarkaTypes.castBounds(source, target);
        DoublePredicate spark = sparkAdmits(source, target);
        String ctx = source + " to " + target;
        assertTrue(spark.test(b[0]) && spark.test(b[1]), ctx);
        assertFalse(spark.test(outward(source, b[0], false)), ctx);
        assertFalse(spark.test(outward(source, b[1], true)), ctx);
        assertFalse(spark.test(Double.NaN), ctx);
      }
    }
  }

  private static List<Double> boundsOf(LaneType source, LaneType target) {
    double[] b = VarkaTypes.castBounds(source, target);
    return List.of(b[0], b[1]);
  }

  /** Whether Spark's ANSI cast of {@code source} to {@code target} answers {@code v}. */
  private static DoublePredicate sparkAdmits(LaneType source, LaneType target) {
    return v -> {
      try {
        float f = (float) v;
        switch (target) {
          case BYTE -> {
            if (source == LaneType.FLOAT) {
              CastUtils.floatToByteExact(f);
            } else {
              CastUtils.doubleToByteExact(v);
            }
          }
          case SHORT -> {
            if (source == LaneType.FLOAT) {
              CastUtils.floatToShortExact(f);
            } else {
              CastUtils.doubleToShortExact(v);
            }
          }
          case INT -> {
            if (source == LaneType.FLOAT) {
              FloatExactNumeric$.MODULE$.toInt(f);
            } else {
              DoubleExactNumeric$.MODULE$.toInt(v);
            }
          }
          case LONG -> {
            if (source == LaneType.FLOAT) {
              FloatExactNumeric$.MODULE$.toLong(f);
            } else {
              DoubleExactNumeric$.MODULE$.toLong(v);
            }
          }
          case FLOAT, DOUBLE -> throw new IllegalArgumentException("a floating target");
        }
        return true;
      } catch (ArithmeticException e) {
        return false;
      }
    };
  }

  @Test
  @DisplayName("a floating guard refuses a NaN bound, unordered bounds and a bound no float holds")
  void guardRefusals() {
    assertThrows(IllegalArgumentException.class,
        () -> GuardedRange.floating(X, Double.NaN, 1.0, Overflow.FAIL));
    assertThrows(IllegalArgumentException.class,
        () -> GuardedRange.floating(X, 2.0, 1.0, Overflow.NULL));
    assertThrows(IllegalArgumentException.class,
        () -> GuardedRange.floating(F, 0.0, 0.1, Overflow.FAIL));
    var g = GuardedRange.floating(F, -1.5, 2147483520.0, Overflow.FAIL);
    assertEquals(-1.5, g.loValue());
    assertEquals(2147483520.0, g.hiValue());
    assertEquals(VarkaValueRange.UNKNOWN, VarkaRangeAnalysis.range(g,
        VarkaRangeAnalysis.Kind.INT, VarkaRangeAnalysis.GuardPolicy.NONE, k -> 0, c -> { }));
  }

  // ---------------------------------------------------------------------------------------------
  // The kernels, in the three modes
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("LEGACY: F2I and D2I truncate, saturate and send NaN to 0, at every level")
  void legacyToInt() {
    for (var options : List.of(VarkaMatrixRuns.BASE, DQ)) {
      check("cast(f AS int)", cast(F, LaneType.INT, Overflow.WRAP), cells(LaneType.FLOAT),
          options, false);
      check("cast(x AS int)", cast(X, LaneType.INT, Overflow.WRAP), cells(LaneType.DOUBLE),
          options, false);
    }
  }

  @Test
  @DisplayName("LEGACY: a narrow target wraps the int answer, (byte) 300.7 is 44 and 1e10 is -1")
  void legacyToNarrow() {
    for (LaneType target : List.of(LaneType.BYTE, LaneType.SHORT)) {
      check("cast(f AS " + target + ")", cast(F, target, Overflow.WRAP), cells(LaneType.FLOAT),
          VarkaMatrixRuns.BASE, false);
      check("cast(x AS " + target + ")", cast(X, target, Overflow.WRAP), cells(LaneType.DOUBLE),
          VarkaMatrixRuns.BASE, false);
    }
    assertEquals(44, (byte) (int) 300.7);
    assertEquals(-1, (byte) (int) 1e10);
    assertEquals(-128, (byte) (int) 128.0);
  }

  @Test
  @DisplayName("TRY: a lane outside the bounds or NaN is null, the rest Java's cast")
  void tryNulls() {
    for (LaneType target : List.of(LaneType.BYTE, LaneType.SHORT, LaneType.INT)) {
      check("try_cast(f AS " + target + ")", cast(F, target, Overflow.NULL),
          cells(LaneType.FLOAT), VarkaMatrixRuns.BASE, false);
      check("try_cast(x AS " + target + ")", cast(X, target, Overflow.NULL),
          cells(LaneType.DOUBLE), VarkaMatrixRuns.BASE, false);
    }
    // A float guard in a long kernel: the guard's mask has twice the drive's lanes.
    check("try_cast(f AS bigint)", cast(F, LaneType.LONG, Overflow.NULL), cells(LaneType.FLOAT),
        DQ, false);
    check("try_cast(x AS bigint)", cast(X, LaneType.LONG, Overflow.NULL),
        cells(LaneType.DOUBLE), DQ, false);
  }

  @Test
  @DisplayName("ANSI: the answers inside the bounds are LEGACY's; only a lane outside declines")
  void ansiAnswers() {
    for (LaneType target : List.of(LaneType.BYTE, LaneType.INT)) {
      check("cast(f AS " + target + ") under ANSI", cast(F, target, Overflow.FAIL),
          inside(LaneType.FLOAT, target), VarkaMatrixRuns.BASE, false);
      check("cast(x AS " + target + ") under ANSI", cast(X, target, Overflow.FAIL),
          inside(LaneType.DOUBLE, target), VarkaMatrixRuns.BASE, false);
      // Over every cell, a batch with a live lane outside declines and the others answer.
      check("cast(x AS " + target + ") under ANSI, every cell", cast(X, target, Overflow.FAIL),
          cells(LaneType.DOUBLE), VarkaMatrixRuns.BASE, true);
    }
    check("cast(x AS bigint) under ANSI", cast(X, LaneType.LONG, Overflow.FAIL),
        inside(LaneType.DOUBLE, LaneType.LONG), DQ, false);
  }

  @Test
  @DisplayName("ANSI: a live lane past a bound or NaN declines the batch, a null one does not")
  void ansiDeclines() {
    int range = VarkaFusedKernel.STATUS_VALUE_RANGE;
    for (LaneType source : List.of(LaneType.FLOAT, LaneType.DOUBLE)) {
      var column = new ColumnRef(0, source);
      for (LaneType target : List.of(LaneType.INT, LaneType.LONG)) {
        var roots = List.<VarkaVectorIR>of(cast(column, target, Overflow.FAIL));
        var options = (target == LaneType.LONG ? DQ : VarkaMatrixRuns.BASE).withLanesOverride(4);
        var emitted = VarkaEmitterFixtures.emit(roots, 1, 0, options);
        double[] b = VarkaTypes.castBounds(source, target);
        double[] outside = {outward(source, b[0], false), outward(source, b[1], true),
            Double.NaN, Double.POSITIVE_INFINITY};
        for (double bad : outside) {
          for (int at : new int[] {2, 16}) {
            var values = new long[1][17];
            for (int i = 0; i < 17; i++) {
              double v = i == at ? bad : (i % 2 == 0 ? b[0] : b[1]);
              values[0][i] = source == LaneType.FLOAT ? Float.floatToRawIntBits((float) v)
                  : Double.doubleToRawLongBits(v);
            }
            var nulls = new boolean[1][17];
            var batch = new VarkaKernelChecks.LongBatch(17, nulls, values, false);
            String ctx = source + " to " + target + ", " + bad + " at row " + at;
            assertEquals(List.of(range, range), VarkaKernelChecks.runEachBodyMixedStatus(ctx,
                emitted.className(), emitted.bytes(), roots, 1, new int[0], new long[0], batch),
                ctx);
            nulls[0][at] = true;
            assertEquals(List.of(0, range), VarkaKernelChecks.runEachBodyMixedStatus(
                ctx + ", null", emitted.className(), emitted.bytes(), roots, 1, new int[0],
                new long[0], batch), ctx + ", null");
          }
        }
      }
    }
  }

  @Test
  @DisplayName("LEGACY at level 3: F2L and D2L saturate at the long range and send NaN to 0")
  void legacyToLong() {
    check("cast(f AS bigint)", cast(F, LaneType.LONG, Overflow.WRAP), cells(LaneType.FLOAT), DQ,
        false);
    check("cast(x AS bigint)", cast(X, LaneType.LONG, Overflow.WRAP), cells(LaneType.DOUBLE), DQ,
        false);
    // Beside a long column and its arithmetic, in one kernel of the long lane.
    var l = new ColumnRef(1, LaneType.LONG);
    var roots = List.<VarkaVectorIR>of(new IntArith(IntOp.ADD, Overflow.WRAP,
        cast(X, LaneType.LONG, Overflow.WRAP), l));
    assertTrue(VarkaKernelPlan.refusalOf(roots, DQ).isEmpty());
  }

  @Test
  @DisplayName("the plan admits F2I and D2I at every level, F2L and D2L at every named one")
  void levels() {
    for (VarkaEmitOptions options : List.of(VarkaEmitOptions.DEFAULTS, VarkaMatrixRuns.BASE,
        VarkaMatrixRuns.BASE.withUseAVX(2), DQ)) {
      for (var source : List.of(F, X)) {
        assertTrue(VarkaKernelPlan.refusalOf(List.of(convert(source, LaneType.INT)), options)
            .isEmpty(), source + " at " + options.useAVX());
        var toLong = VarkaKernelPlan.of(List.of(convert(source, LaneType.LONG)), options)
            .refusal();
        assertEquals(options.levelUnknown(), toLong.isPresent(), source + " at "
            + options.useAVX());
        toLong.ifPresent(r -> assertEquals(VarkaKernelPlan.Kind.CONVERSION, r.kind()));
      }
    }
    // A narrow target from a double is stored from the low half of the int species, as a long
    // kernel stores it; a narrowing another node reads still declines at the double lane.
    var narrowed = cast(X, LaneType.BYTE, Overflow.WRAP);
    assertTrue(VarkaKernelPlan.refusalOf(List.of(narrowed), VarkaMatrixRuns.BASE).isEmpty());
    var read = new IntArith(IntOp.ADD, Overflow.WRAP, narrowed, narrowed);
    assertTrue(VarkaKernelPlan.refusalOf(List.of(read), VarkaMatrixRuns.BASE).isPresent());
  }

  // ---------------------------------------------------------------------------------------------
  // The AVX2 forms of D2L and F2L
  // ---------------------------------------------------------------------------------------------

  /** Level 2, an AVX2 host's, under each arm of {@code wideConversionForm}. */
  private static List<VarkaEmitOptions> avx2Arms() {
    var avx2 = VarkaMatrixRuns.BASE.withUseAVX(2);
    return List.of(avx2.withWideConversionForm(WideConversionForm.XOR_SPLIT),
        avx2.withWideConversionForm(WideConversionForm.SPLIT),
        avx2.withWideConversionForm(WideConversionForm.MAGIC));
  }

  @Test
  @DisplayName("AVX2: the assembled D2L and F2L are Java's cast at every cell, the magic within "
      + "2^51")
  void avx2ToLong() {
    for (VarkaEmitOptions options : avx2Arms()) {
      boolean magic = options.wideConversionForm() == WideConversionForm.MAGIC;
      String arm = " under " + options.wideConversionForm();
      for (LaneType source : List.of(LaneType.FLOAT, LaneType.DOUBLE)) {
        var column = new ColumnRef(0, source);
        long[] cells = magic ? window(source) : cells(source);
        check("cast(" + source + " AS bigint)" + arm, cast(column, LaneType.LONG, Overflow.WRAP),
            cells, options, false);
        check("try_cast(" + source + " AS bigint)" + arm,
            cast(column, LaneType.LONG, Overflow.NULL), cells, options, false);
        check("cast(" + source + " AS bigint) under ANSI" + arm,
            cast(column, LaneType.LONG, Overflow.FAIL), inside(source, LaneType.LONG), options,
            magic);
      }
    }
  }

  /** {@link #cells} within the magic's window, {@code |x| <= 2^51}, NaN left out. */
  private static long[] window(LaneType source) {
    return LongStream.of(cells(source)).filter(bits -> {
      double v = source == LaneType.FLOAT ? Float.intBitsToFloat((int) bits)
          : Double.longBitsToDouble(bits);
      return Math.abs(v) <= 0x1p51;
    }).toArray();
  }

  @Test
  @DisplayName("AVX2 MAGIC: a live lane past 2^51, NaN or an infinity declines, a null one does "
      + "not")
  void magicDeclines() {
    var options = avx2Arms().get(2).withLanesOverride(4);
    int range = VarkaFusedKernel.STATUS_VALUE_RANGE;
    var roots = List.<VarkaVectorIR>of(cast(X, LaneType.LONG, Overflow.WRAP));
    var emitted = VarkaEmitterFixtures.emit(roots, 1, 0, options);
    for (double bad : new double[] {Math.nextUp(0x1p51), -Math.nextUp(0x1p51), Double.NaN,
        Double.NEGATIVE_INFINITY}) {
      var values = new long[1][17];
      for (int i = 0; i < 17; i++) {
        values[0][i] = Double.doubleToRawLongBits(i == 9 ? bad : (i % 2 == 0 ? 0x1p51 : -1.5));
      }
      var nulls = new boolean[1][17];
      var batch = new VarkaKernelChecks.LongBatch(17, nulls, values, false);
      assertEquals(List.of(range, range), VarkaKernelChecks.runEachBodyMixedStatus("" + bad,
          emitted.className(), emitted.bytes(), roots, 1, new int[0], new long[0], batch));
      nulls[0][9] = true;
      assertEquals(List.of(0, range), VarkaKernelChecks.runEachBodyMixedStatus(bad + ", null",
          emitted.className(), emitted.bytes(), roots, 1, new int[0], new long[0], batch));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The reduced-width twins of 3.2: each AVX2 form at 32 bits, scalar, over every float
  // ---------------------------------------------------------------------------------------------

  /**
   * The assembly's twin, {@code float} to {@code int} from the exponent with the same steps at
   * half the width: {@code e = ((b >>> 23) & 0xFF) - 150}, {@code m} with its implicit bit, the
   * shifts, the right count raised to 31, the sign and the saturation past {@code e = 7}.
   */
  static int assemblyTwin(float x) {
    int b = Float.floatToRawIntBits(x);
    int e = ((b >>> 23) & 0xFF) - 150;
    int m = (b & 0x7FFFFF) | (1 << 23);
    int neg = -(e >>> 31);
    int left = m << ((e ^ (e & neg)) & 31);
    int c = (-e) & neg;
    c |= (-((31 - c) >>> 31)) & 31;
    int mag = left ^ ((left ^ (m >>> c)) & neg);
    int sign = -(b >>> 31);
    int r = (mag ^ sign) - sign;
    int big = -((7 - e) >>> 31);
    int notNan = ((0x7F800000 - (b & Integer.MAX_VALUE)) >>> 31) - 1;
    int sat = (Integer.MAX_VALUE ^ sign) & notNan;
    return r ^ ((r ^ sat) & big);
  }

  /** The truncated magic's twin, {@code float} to {@code int} with {@code 0x1.8p23}. */
  static int magicTwin(float x) {
    float magic = 0x1.8p23f;
    float r = (x + magic) - magic;
    int l = Float.floatToRawIntBits(r + magic) - Float.floatToRawIntBits(magic);
    int xb = Float.floatToRawIntBits(x);
    int d = Float.floatToRawIntBits(x - r);
    int nz = -(((d & Integer.MAX_VALUE) + Integer.MAX_VALUE) >>> 31);
    int differ = -((d ^ xb) >>> 31);
    int sign = -(xb >>> 31);
    return l - ((sign | 1) & nz & differ);
  }

  /** Every {@code every}-th float pattern through both twins, against Java's cast. */
  private static void twins(int every) {
    String bad = LongStream.range(0, (1L << 32) / every).parallel()
        .mapToInt(k -> (int) (k * every + Integer.MIN_VALUE))
        .filter(b -> {
          float x = Float.intBitsToFloat(b);
          boolean window = Math.abs(x) <= 0x1p22f;
          return assemblyTwin(x) != (int) x || window && magicTwin(x) != (int) x;
        })
        .mapToObj(b -> String.format("%08x", b)).findFirst().orElse(null);
    if (bad != null) {
      fail("a twin differs from Java's cast over the float bits " + bad);
    }
  }

  @Test
  @DisplayName("the assembly's and the truncated magic's float-to-int twins over every 97th "
      + "float are Java's cast")
  void twinSlices() {
    twins(97);
  }

  @Test
  @DisplayName("the float-to-int twins over every float are Java's cast (opt-in: "
      + "-Dvarka.sweep=true)")
  void twinsWhole() {
    assumeTrue("true".equals(System.getProperty("varka.sweep")),
        "set -Dvarka.sweep=true to run the twins over every float");
    twins(1);
  }

  // ---------------------------------------------------------------------------------------------
  // The sweeps of 3.1: every FLOAT pattern
  // ---------------------------------------------------------------------------------------------

  private static final int CHUNK = 1 << 16;
  /** The answer that marks a null row in a sweep's expectations. */
  private static final long NULL = Long.MIN_VALUE + 1;

  /**
   * Every {@code every}-th chunk of the 2^32 float patterns through {@code root}, a kernel over
   * one float column, each row held to {@code want} bit for bit, or to a null where it answers
   * {@link #NULL}. A 64-bit result is read eight bytes a row, a 32-bit or narrow one widened.
   */
  private static void sweep(String ctx, VarkaVectorIR root, VarkaEmitOptions options, int every,
      IntToLongFunction want) {
    boolean wide = root.laneType() == LaneType.LONG;
    int width = switch (root.laneType()) {
      case BYTE -> 1;
      case SHORT -> 2;
      case INT -> 4;
      case LONG -> 8;
      case FLOAT, DOUBLE -> throw new IllegalArgumentException("a floating root: " + root);
    };
    var emitted = VarkaEmitterFixtures.emit(List.of(root), 1, 0, options);
    var loaders = new ConcurrentLinkedQueue<VarkaGeneratedClassLoader>();
    ThreadLocal<VarkaFusedKernel> kernels = ThreadLocal.withInitial(() -> {
      var loaded = VarkaEmitterFixtures.load(emitted);
      loaders.add(loaded.loader());
      return loaded.kernel();
    });
    var failure = new AtomicReference<String>();
    try {
      LongStream.range(0, (1L << 32) / CHUNK).filter(k -> k % every == 0).parallel().forEach(k -> {
        if (failure.get() != null) {
          return;
        }
        long first = k * CHUNK;
        VarkaFusedKernel kernel = kernels.get();
        try (Arena arena = Arena.ofConfined()) {
          MemorySegment in = VarkaEmitterFixtures.alloc(arena, CHUNK * 4L);
          for (int i = 0; i < CHUNK; i++) {
            in.set(ValueLayout.JAVA_INT, i * 4L, (int) (first + i));
          }
          var out = wide ? VarkaEmitterFixtures.makeLongOutput(arena, CHUNK)
              : VarkaEmitterFixtures.makeOutput(arena, CHUNK, -1L);
          long[] src = {in.address()};
          long[] dst = {out.data().address()};
          long[] validity = {out.validity().address()};
          long scratch = VarkaEmitterTestSupport.scratch(kernel, CHUNK);
          int status = wide
              ? kernel.run(src, new long[] {0L}, new int[] {0}, dst, validity, new int[0],
                  new long[0], CHUNK, scratch)
              : kernel.run(src, new long[] {0L}, new int[] {0}, dst, validity, new int[0], CHUNK,
                  scratch);
          if (status != 0) {
            failure.compareAndSet(null, ctx + ": status " + status + " at " + first);
          }
          for (int i = 0; i < CHUNK && failure.get() == null; i++) {
            int value = (int) (first + i);
            boolean valid = (out.validity().get(ValueLayout.JAVA_BYTE, i >> 3) >> (i & 7) & 1) != 0;
            long got = switch (width) {
              case 1 -> out.data().get(ValueLayout.JAVA_BYTE, i);
              case 2 -> out.data().get(ValueLayout.JAVA_SHORT_UNALIGNED, i * 2L);
              case 4 -> out.data().get(ValueLayout.JAVA_INT, i * 4L);
              default -> out.data().get(ValueLayout.JAVA_LONG, i * 8L);
            };
            long expected = want.applyAsLong(value);
            boolean wrong = expected == NULL ? valid : !valid || got != expected;
            if (wrong) {
              failure.compareAndSet(null, String.format("%s: over %08x the kernel gave %x (valid"
                  + " %b) and Java's cast %x", ctx, value, got, valid, expected));
            }
          }
        }
      });
    } finally {
      for (VarkaGeneratedClassLoader loader : loaders) {
        loader.release();
      }
    }
    if (failure.get() != null) {
      fail(failure.get());
    }
  }

  /**
   * The LEGACY and TRY sweeps of a float source to the four integral types; {@code BIGINT} at
   * level 3 and under each AVX2 arm that converts the whole range.
   */
  private static void sweeps(int every) {
    var targets = List.of(LaneType.BYTE, LaneType.SHORT, LaneType.INT, LaneType.LONG,
        LaneType.LONG, LaneType.LONG);
    var levels = List.of(VarkaMatrixRuns.BASE, VarkaMatrixRuns.BASE, VarkaMatrixRuns.BASE, DQ,
        avx2Arms().get(0), avx2Arms().get(1));
    for (int t = 0; t < targets.size(); t++) {
      LaneType target = targets.get(t);
      var options = levels.get(t);
      double[] b = VarkaTypes.castBounds(LaneType.FLOAT, target);
      IntToLongFunction legacy = v -> {
        float f = Float.intBitsToFloat(v);
        return switch (target) {
          case BYTE -> (byte) (int) f;
          case SHORT -> (short) (int) f;
          case INT -> (int) f;
          default -> (long) f;
        };
      };
      String at = " at " + options.useAVX() + " " + options.wideConversionForm();
      sweep("cast(f AS " + target + ")" + at, cast(F, target, Overflow.WRAP), options, every,
          legacy);
      sweep("try_cast(f AS " + target + ")" + at, cast(F, target, Overflow.NULL), options, every,
          v -> {
            float f = Float.intBitsToFloat(v);
            return f >= b[0] && f <= b[1] ? legacy.applyAsLong(v) : NULL;
          });
    }
  }

  @Test
  @DisplayName("every 257th chunk of the FLOAT patterns to the four integral types, in LEGACY "
      + "and TRY, is Java's cast or Spark's null")
  void sweepSlices() {
    // 257, coprime to the 256 chunks of an exponent, so the slice meets every exponent's low bit.
    sweeps(257);
  }

  @Test
  @DisplayName("every FLOAT pattern to the four integral types, in LEGACY and TRY, is Java's cast "
      + "or Spark's null (opt-in: -Dvarka.sweep=true)")
  void sweepsWhole() {
    assumeTrue("true".equals(System.getProperty("varka.sweep")),
        "set -Dvarka.sweep=true to sweep the emitted kernels");
    sweeps(1);
  }
}
