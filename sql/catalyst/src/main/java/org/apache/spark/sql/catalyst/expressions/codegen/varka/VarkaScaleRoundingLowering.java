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

import static org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaDescriptors.*;
import static org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorWalk.*;

import java.lang.classfile.CodeBuilder;
import java.lang.constant.MethodTypeDesc;
import java.math.BigDecimal;
import java.util.Objects;
import java.util.Set;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.RoundToScale;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ScaleMode;

/**
 * The lowering of {@link RoundToScale}: Spark's {@code round}, {@code bround} and
 * {@code truncate} of a double at a scale {@code s} from 0 to {@link #MAX_SCALE}, which round the
 * double's shortest decimal ({@code BigDecimal.valueOf(d).setScale(s, mode).doubleValue()}),
 * not its binary value, computed in lanes by IEEE operations alone (VARKA-346 2.2 and 2.3).
 *
 * <p><b>Three regions.</b> With {@code a = |d|}: below {@link #lowerBound} {@code L_s} every ulp
 * is under {@code 10^-(s + 1)}, and the tie test below computes the answer; from
 * {@link #keepBound} {@code K_s} up, and for NaN and the infinities, the answer is {@code d}
 * itself; between them, an integral {@code a} is its own shortest decimal and answers {@code d},
 * and a non-integral one is the declined band, whose batch the row engine answers
 * ({@code Analysis.Refusal.ROUNDING_BAND}). At scale 0 the two bounds are both {@code 2^52} and
 * there is no band.
 *
 * <p><b>The tie test.</b> With {@code P = 10^s}, exact as a double for {@code s <= 22}, and
 * {@code y = RN(a * P)}:
 *
 * <pre>
 *   HALF_UP:    k = floor(y);  t = (2k + 1) / (2P);  n = k + [a &gt;= t]
 *   HALF_EVEN:  k = floor(y);  t = (2k + 1) / (2P);  n = k + [a &gt; t or (a == t and k odd)]
 *   DOWN:       m = rint(y);   t = m / P;            n = m - [a &lt; t]
 *   answer:     (bits(n / P) | sign(d)) + 0.0
 * </pre>
 *
 * Below {@code L_s} at most one boundary of {@code s + 1} digits lies among the reals that round
 * to {@code a}, and when one does it is the shortest decimal and {@code t == a} exactly (Clinger's
 * fast path); otherwise one compare says on which side of it the shortest decimal lies.
 * {@code + 0.0} turns {@code -0.0} into {@code +0.0}, since a {@code BigDecimal} has no negative
 * zero. {@code VARKA-346.md} 2.3 gives the argument in full.
 *
 * <p>{@code floor(y)} is {@code r = rint(y)} stepped down where {@code r > y}, and {@code rint}
 * the {@code 2^52} construction ({@link VarkaRoundingLowering#emitRoundToNearest}), exact here
 * since {@code y < 2^53 / 10}. A {@code k} is odd exactly when {@code k / 2} is not an integer,
 * so the oddness is one multiply, one {@code rint} and one compare, with no mask between lane
 * kinds.
 */
final class VarkaScaleRoundingLowering {

  /** The largest scale fused, {@link RoundToScale#MAX_SCALE}. */
  static final int MAX_SCALE = RoundToScale.MAX_SCALE;

  /** The scratch locals the lowering parks: the operand, its magnitude, and two values. */
  static final int SCRATCH = 4;

  /**
   * {@link #lowerBound} and {@link #keepBound} by scale, 0 to {@link #MAX_SCALE}, computed once
   * here: the guard, the weight and the suites read them per node and per value.
   */
  private static final double[] LOWER_BOUNDS = new double[MAX_SCALE + 1];
  private static final double[] KEEP_BOUNDS = new double[MAX_SCALE + 1];

  static {
    for (int scale = 0; scale <= MAX_SCALE; scale++) {
      LOWER_BOUNDS[scale] = computeLowerBound(scale);
      KEEP_BOUNDS[scale] = computeKeepBound(scale);
    }
  }

  private VarkaScaleRoundingLowering() {
  }

  /** {@code 10^s}, exact as a double for {@code 0 <= s <= 22}. */
  static double power(int scale) {
    requireScale(scale);
    return Double.parseDouble("1e" + scale);
  }

  /**
   * {@code L_s}: below it the tie test is exact. {@code 2^E} for the largest {@code E} with
   * {@code 2^(E - 53) < 10^-(s + 1)}, so that every ulp below it is under {@code 10^-(s + 1)};
   * {@code 2^52} at scale 0, where every boundary {@code j + 1/2} is a multiple of the ulp up to
   * there ({@code VARKA-346.md} 2.3).
   */
  static double lowerBound(int scale) {
    requireScale(scale);
    return LOWER_BOUNDS[scale];
  }

  /** {@link #lowerBound} computed, once per scale, into {@link #LOWER_BOUNDS}. */
  private static double computeLowerBound(int scale) {
    if (scale == 0) {
      return 0x1.0p52;
    }
    BigDecimal limit = BigDecimal.ONE.movePointLeft(scale + 1);
    int e = 52;
    while (new BigDecimal(Math.scalb(1.0, e - 53)).compareTo(limit) >= 0) {
      e--;
    }
    return Math.scalb(1.0, e);
  }

  /**
   * {@code K_s}: from it up every double is its own answer. {@code 10^(16 - s)} rounded up to a
   * double for {@code s >= 1}, since a shortest decimal of at most 17 digits whose leading digit
   * is at {@code 10^(16 - s)} or above has no digit below {@code 10^-s}; {@code 2^52} at scale 0,
   * where every double from there up is an integer.
   */
  static double keepBound(int scale) {
    requireScale(scale);
    return KEEP_BOUNDS[scale];
  }

  /** {@link #keepBound} computed, once per scale, into {@link #KEEP_BOUNDS}. */
  private static double computeKeepBound(int scale) {
    if (scale == 0) {
      return 0x1.0p52;
    }
    BigDecimal exact = BigDecimal.ONE.movePointRight(16 - scale);
    double d = exact.doubleValue();
    return new BigDecimal(d).compareTo(exact) < 0 ? Math.nextUp(d) : d;
  }

  /**
   * Whether the magnitude {@code a} is in the declined band at {@code scale}: in
   * {@code [L_s, K_s)} and not an integer. The predicate the emitted guard computes, for the
   * suites that predict a kernel's declines.
   */
  static boolean inBand(int scale, double a) {
    return a >= lowerBound(scale) && a < keepBound(scale) && Math.rint(a) != a;
  }

  /** Whether {@code scale} has a declined band, which every scale but 0 has. */
  static boolean hasBand(int scale) {
    return lowerBound(scale) < keepBound(scale);
  }

  /**
   * The lane calls the node emits, which the budget weighs it by: the magnitude; the tie test of
   * its mode (the floor's multiply, {@code rint} and step, the boundary's three operations, its
   * compares and the step's add and blend; {@code HALF_EVEN} adds the oddness's multiply,
   * {@code rint} and compare, and a second compare; {@code DOWN} has no boundary); the answer's
   * division, sign, {@code + 0.0} and blend under {@code L_s}; and the band's three compares and
   * {@code rint}. The masks' {@code and} and {@code or} are mask calls, which no weight counts.
   */
  static int weight(RoundToScale n) {
    int test = switch (n.mode()) {
      case HALF_UP -> 12;
      case HALF_EVEN -> 17;
      case DOWN -> 7;
    };
    return 1 + test + 9 + (hasBand(n.scale()) ? 5 : 0);
  }

  private static void requireScale(int scale) {
    if (scale < 0 || scale > MAX_SCALE) {
      throw VarkaErrors.illegalArgument("a rounding scale outside [0, " + MAX_SCALE + "]: "
          + scale);
    }
  }

  /**
   * Spark's rounding of the child to the node's scale per lane, the band's lanes folded into the
   * batch's accumulator. The operand, its magnitude and two intermediate values are parked in
   * the node's scratch locals, since each is read several times. The validity is the child's.
   */
  static void emit(CodeBuilder cb, RoundToScale n, boolean dense, Analysis analysis, Slots s,
      Set<VarkaVectorIR> computed) {
    int[] tmp = Objects.requireNonNull(s.roundTmp.get(n), "the node's scratch locals");
    int x = tmp[0];
    int a = tmp[1];
    int y = tmp[2];
    int k = tmp[3];
    int scale = n.scale();
    double p = power(scale);
    emitValue(cb, n.child(), dense, analysis, s, computed);
    line(cb, analysis, n);
    cb.astore(x);
    cb.aload(x);
    cb.getstatic(VECTOR_OPERATORS, "ABS", VO_UNARY);
    cb.invokevirtual(DOUBLE_VECTOR, "lanewise", MethodTypeDesc.of(DOUBLE_VECTOR, VO_UNARY));
    cb.astore(a);
    switch (n.mode()) {
      case HALF_UP, HALF_EVEN -> emitHalf(cb, n.mode(), p, a, y, k);
      case DOWN -> emitDown(cb, p, a, y, k);
    }
    // [n] -> [(bits(n / P) | sign(x)) + 0.0], kept where a < L_s and x elsewhere.
    cb.loadConstant(p);
    cb.invokevirtual(DOUBLE_VECTOR, "div", LANEWISE_VD);
    cb.invokevirtual(VECTOR, "reinterpretAsLongs", REINTERPRET_L);
    cb.aload(x);
    cb.invokevirtual(VECTOR, "reinterpretAsLongs", REINTERPRET_L);
    cb.loadConstant(Long.MIN_VALUE);
    cb.invokevirtual(LONG_VECTOR, "and", Lane.LONG.lanewiseVI);
    cb.invokevirtual(LONG_VECTOR, "or", Lane.LONG.lanewiseVV);
    cb.invokevirtual(VECTOR, "reinterpretAsDoubles", REINTERPRET_D);
    cb.loadConstant(0.0);
    cb.invokevirtual(DOUBLE_VECTOR, "add", LANEWISE_VD);
    cb.astore(y);
    cb.aload(x);
    cb.aload(y);
    compareScalar(cb, a, "LT", lowerBound(scale));
    cb.invokevirtual(DOUBLE_VECTOR, "blend", Lane.DOUBLE.blend);   // [r]
    if (hasBand(scale)) {
      // [r] -> [r, a >= L_s and a < K_s and rint(a) != a], folded into the accumulator.
      compareScalar(cb, a, "GE", lowerBound(scale));
      compareScalar(cb, a, "LT", keepBound(scale));
      cb.invokevirtual(VECTOR_MASK, "and", MASK_BINARY);
      cb.aload(a);
      cb.getstatic(VECTOR_OPERATORS, "NE", VO_COMPARISON);
      cb.aload(a);
      VarkaRoundingLowering.emitRoundToNearest(cb);
      cb.invokevirtual(DOUBLE_VECTOR, "compare", Lane.DOUBLE.compareVV);
      cb.invokevirtual(VECTOR_MASK, "and", MASK_BINARY);
      emitGuardCollect(cb, n, dense ? null : s.wordRef.get(n), dense, analysis, s);
    }
  }

  /**
   * {@code [] -> [n]} for the two half modes: {@code k = floor(a * P)}, the boundary
   * {@code t = (2k + 1) / (2P)}, and {@code k} stepped up where {@code a} is past it, or at it
   * under {@code HALF_UP} or at it with an odd {@code k} under {@code HALF_EVEN}.
   */
  private static void emitHalf(CodeBuilder cb, ScaleMode mode, double p, int a, int y, int k) {
    cb.aload(a);
    cb.loadConstant(p);
    cb.invokevirtual(DOUBLE_VECTOR, "mul", LANEWISE_VD);
    cb.astore(y);                                                  // y = RN(a * P)
    cb.aload(y);
    VarkaRoundingLowering.emitRoundToNearest(cb);
    cb.astore(k);                                                  // r = rint(y)
    cb.aload(k);
    cb.aload(k);
    cb.loadConstant(1.0);
    cb.invokevirtual(DOUBLE_VECTOR, "sub", LANEWISE_VD);
    cb.aload(k);
    cb.getstatic(VECTOR_OPERATORS, "GT", VO_COMPARISON);
    cb.aload(y);
    cb.invokevirtual(DOUBLE_VECTOR, "compare", Lane.DOUBLE.compareVV);
    cb.invokevirtual(DOUBLE_VECTOR, "blend", Lane.DOUBLE.blend);
    cb.astore(k);                                                  // k = floor(y)
    if (mode == ScaleMode.HALF_EVEN) {
      // k odd: k / 2 is not an integer.
      cb.aload(k);
      cb.loadConstant(0.5);
      cb.invokevirtual(DOUBLE_VECTOR, "mul", LANEWISE_VD);
      cb.astore(y);
      cb.aload(y);
      cb.getstatic(VECTOR_OPERATORS, "NE", VO_COMPARISON);
      cb.aload(y);
      VarkaRoundingLowering.emitRoundToNearest(cb);
      cb.invokevirtual(DOUBLE_VECTOR, "compare", Lane.DOUBLE.compareVV);   // [odd]
    }
    cb.aload(k);
    cb.loadConstant(2.0);
    cb.invokevirtual(DOUBLE_VECTOR, "mul", LANEWISE_VD);
    cb.loadConstant(1.0);
    cb.invokevirtual(DOUBLE_VECTOR, "add", LANEWISE_VD);
    cb.loadConstant(2.0 * p);
    cb.invokevirtual(DOUBLE_VECTOR, "div", LANEWISE_VD);
    cb.astore(y);                                                  // t
    if (mode == ScaleMode.HALF_EVEN) {
      compareVector(cb, a, "EQ", y);
      cb.invokevirtual(VECTOR_MASK, "and", MASK_BINARY);
      compareVector(cb, a, "GT", y);
      cb.invokevirtual(VECTOR_MASK, "or", MASK_BINARY);            // [up]
    } else {
      compareVector(cb, a, "GE", y);                               // [up]
    }
    cb.aload(k);
    cb.swap();                                                     // [k, up]
    cb.aload(k);
    cb.loadConstant(1.0);
    cb.invokevirtual(DOUBLE_VECTOR, "add", LANEWISE_VD);
    cb.swap();                                                     // [k, k + 1, up]
    cb.invokevirtual(DOUBLE_VECTOR, "blend", Lane.DOUBLE.blend);   // [n]
  }

  /**
   * {@code [] -> [n]} for {@code DOWN}: {@code m = rint(a * P)}, {@code t = m / P}, and
   * {@code m} stepped down where {@code a} is below {@code t}.
   */
  private static void emitDown(CodeBuilder cb, double p, int a, int y, int k) {
    cb.aload(a);
    cb.loadConstant(p);
    cb.invokevirtual(DOUBLE_VECTOR, "mul", LANEWISE_VD);
    VarkaRoundingLowering.emitRoundToNearest(cb);
    cb.astore(k);                                                  // m
    cb.aload(k);
    cb.loadConstant(p);
    cb.invokevirtual(DOUBLE_VECTOR, "div", LANEWISE_VD);
    cb.astore(y);                                                  // t
    cb.aload(k);
    cb.aload(k);
    cb.loadConstant(1.0);
    cb.invokevirtual(DOUBLE_VECTOR, "sub", LANEWISE_VD);
    compareVector(cb, a, "LT", y);
    cb.invokevirtual(DOUBLE_VECTOR, "blend", Lane.DOUBLE.blend);   // [n]
  }

  /** {@code [] -> [local OP c]}, a mask. */
  private static void compareScalar(CodeBuilder cb, int local, String op, double c) {
    cb.aload(local);
    cb.getstatic(VECTOR_OPERATORS, op, VO_COMPARISON);
    cb.loadConstant(c);
    cb.invokevirtual(DOUBLE_VECTOR, "compare", Lane.DOUBLE.compareVI);
  }

  /** {@code [] -> [left OP right]}, a mask of two locals. */
  private static void compareVector(CodeBuilder cb, int left, String op, int right) {
    cb.aload(left);
    cb.getstatic(VECTOR_OPERATORS, op, VO_COMPARISON);
    cb.aload(right);
    cb.invokevirtual(DOUBLE_VECTOR, "compare", Lane.DOUBLE.compareVV);
  }
}
