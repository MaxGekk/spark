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
package org.apache.spark.sql.catalyst.expressions.codegen;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.spark.sql.catalyst.expressions.BRound;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Ceil;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Floor;
import org.apache.spark.sql.catalyst.expressions.Round;
import org.apache.spark.sql.catalyst.expressions.RoundBase;
import org.apache.spark.sql.catalyst.expressions.Truncate;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaErrors;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaTypes;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.BitOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.BitOpKind;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Compare;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.CompareOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Cond;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.CondValue;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ConstDivide;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.GuardedRange;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntArith;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.NarrowLane;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.WidenLane;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

/**
 * The rounding family (VARKA-344): {@code floor}, {@code ceil} and {@code ceiling} over
 * {@code BIGINT} and {@code INT}, and {@code round}, {@code bround} and {@code truncate} over
 * {@code TINYINT}, {@code SMALLINT}, {@code INT} and {@code BIGINT} at a constant scale.
 *
 * <p>{@code floor} and {@code ceil} of a {@code BIGINT} are the operand, Spark's own identity.
 * An {@code INT} operand reaches them as {@code FLOOR(CAST(i AS DOUBLE))}, the first member of
 * their input types the int casts to; an int converts to a double exactly, is its own floor and
 * ceiling, and lies inside the long range {@code MathUtils.doubleToLong} checks under ANSI, so
 * Spark's answer in both modes is the int widened to the long, which is what is built, without
 * the cast. A double operand is row 344b's, a narrow or {@code FLOAT} one waits for the narrow
 * load in a {@code BIGINT} kernel (row 332) and the floating casts (row 343).
 *
 * <p>The integral rounding at a negative scale {@code -k} rounds to a multiple of
 * {@code d = 10^k} by the signed remainder of {@link ConstDivide}'s truncating quotient:
 * {@code q0 = x / d}, {@code r = x - q0 * d}, which has {@code x}'s sign, and {@code q} is
 * {@code q0} moved one step away from zero where {@code r} passes the half - at it too for
 * {@code HALF_UP}, at it only for an odd {@code q0} for {@code HALF_EVEN}, never for
 * {@code DOWN}. The answer is {@code q * d} wrapped, the low bits of the exact rounded integer,
 * which is what Spark's {@code BigDecimal.setScale(-k, mode).intValue()} (or {@code longValue},
 * {@code byteValue}, {@code shortValue}) gives under LEGACY. Under ANSI Spark raises where the
 * rounded value leaves the type; the rounding is monotone in {@code x}, so those inputs are two
 * tails, found here at compile time by bisection over {@code BigDecimal}'s own answer, and a
 * {@link GuardedRange} declines the batch holding one, so the row engine raises. A scale
 * {@code >= 0} is the identity in every mode.
 *
 * <p>Public for {@code VarkaProofFiles}, which renders {@code integral_round.smt2} from
 * {@link #preShiftLimit}, {@link #preShiftBound} and {@link #window}, and for
 * {@code VarkaDomains}, whose edges fixture straddles {@link #preShiftLimit}; the family is
 * reached through {@link #arm} alone.
 */
public final class VarkaRoundingCompiler {

  /**
   * The deepest negative scale admitted. Every integral value rounds to 0 long before it, while
   * {@code BigDecimal.setScale} raises at scales near {@code Integer.MIN_VALUE}, an error the
   * kernel would not reproduce.
   */
  static final int MAX_NEGATIVE_SCALE = 100;

  /** What {@link #window} answers when every input of the type rounds inside it. */
  private static final long[] NO_WINDOW = new long[0];

  private VarkaRoundingCompiler() {
  }

  /** The family's claim, or null. */
  static @Nullable VarkaFamilyArm arm(
      Expression e,
      VarkaSlotTable inputs,
      VarkaSlotTable literals,
      DeclineSink sink) {
    return switch (e) {
      case Floor f -> () -> floorOrCeil(f, f.child(), inputs, literals, sink);
      case Ceil c -> () -> floorOrCeil(c, c.child(), inputs, literals, sink);
      case Round r -> () -> integral(r, RoundingMode.HALF_UP, r.ansiEnabled(), inputs,
          literals, sink);
      case BRound r -> () -> integral(r, RoundingMode.HALF_EVEN, r.ansiEnabled(), inputs,
          literals, sink);
      case Truncate t -> () -> integral(t, RoundingMode.DOWN, t.ansiEnabled(), inputs,
          literals, sink);
      default -> null;
    };
  }

  private static Optional<VarkaVectorIR> floorOrCeil(
      Expression whole, Expression child, VarkaSlotTable inputs, VarkaSlotTable literals,
      DeclineSink sink) {
    DataType t = child.dataType();
    if (DataTypes.LongType.equals(t)) {
      return operand(child, LaneType.LONG, inputs, literals, sink);
    }
    if (child instanceof Cast c && DataTypes.DoubleType.equals(t)) {
      DataType from = c.child().dataType();
      if (DataTypes.IntegerType.equals(from)) {
        return operand(c.child(), LaneType.INT, inputs, literals, sink)
            .map(x -> (VarkaVectorIR) new WidenLane(x, LaneType.LONG));
      }
      if (DataTypes.ByteType.equals(from) || DataTypes.ShortType.equals(from)) {
        return sink.decline("floor or ceil over a narrow integral operand, which needs the "
            + "narrow load in a BIGINT kernel (row 332)", whole);
      }
    }
    if (DataTypes.DoubleType.equals(t)) {
      return sink.decline("floor or ceil over a double (row 344b)", whole);
    }
    return sink.decline("floor or ceil over " + t.simpleString(), whole);
  }

  /** An operand compiled to {@code lane}, or empty with the lane named. */
  private static Optional<VarkaVectorIR> operand(
      Expression e, LaneType lane, VarkaSlotTable inputs, VarkaSlotTable literals,
      DeclineSink sink) {
    // An int column or literal is no family's node, so compileNode leaves it to the int
    // operand rule, which reads it as the int leaf.
    Optional<VarkaVectorIR> x = lane == LaneType.INT
        ? VarkaNodeCompiler.intOperand(e, inputs, literals, sink)
        : VarkaNodeCompiler.compileNode(e, inputs, literals, sink);
    if (x.isPresent() && x.get().laneType() != lane) {
      return sink.decline("a rounding operand computed on the " + x.get().laneType()
          + " lane, not the " + lane + " one", e);
    }
    return x;
  }

  private static Optional<VarkaVectorIR> integral(
      RoundBase whole, RoundingMode mode, boolean ansi, VarkaSlotTable inputs,
      VarkaSlotTable literals, DeclineSink sink) {
    Expression child = whole.left();
    // A decimal is declined by its type, before the lane: a DECIMAL of precision 18 or less is
    // read on the long lane as its unscaled value, and the integer rounding of that value would
    // ignore the scale and the result's type, at a non-negative scale as much as a negative one.
    if (child.dataType() instanceof DecimalType) {
      return sink.decline("round, bround or truncate over " + child.dataType().simpleString()
          + " (rows 351, 363)", whole);
    }
    // A floating operand never reaches here: VarkaNodeCompiler.floatingDecline declines it first.
    Optional<LaneType> typeLane = VarkaTypes.lane(child.dataType()).filter(l -> !l.isFloating());
    if (typeLane.isEmpty()) {
      return sink.decline("round, bround or truncate over " + child.dataType().simpleString(),
          whole);
    }
    Expression scaleExpr = whole.right();
    if (!scaleExpr.foldable()) {
      return sink.decline("a scale that is not a constant", whole);
    }
    Object scaleValue = scaleExpr.eval(null);
    if (!(scaleValue instanceof Integer scale)) {
      return sink.decline("a null scale", whole);
    }
    LaneType lane = typeLane.get();
    if (scale >= 0) {
      return operand(child, lane, inputs, literals, sink);
    }
    if (scale < -MAX_NEGATIVE_SCALE) {
      return sink.decline("a scale below -" + MAX_NEGATIVE_SCALE, whole);
    }
    int k = -scale;
    Optional<VarkaVectorIR> read = operand(child, lane, inputs, literals, sink);
    if (read.isEmpty()) {
      return read;
    }
    return lane == LaneType.LONG
        ? Optional.of(roundLong(read.get(), k, mode, ansi, sink))
        : Optional.of(roundInt(read.get(), lane, k, mode, ansi, literals));
  }

  /** The rounding at the int lane, a narrow operand widened to it and the answer narrowed. */
  private static VarkaVectorIR roundInt(VarkaVectorIR read, LaneType lane, int k,
      RoundingMode mode, boolean ansi, VarkaSlotTable literals) {
    VarkaVectorIR x = lane == LaneType.INT ? read : new WidenLane(read, LaneType.INT);
    long[] w = ansi ? window(lane, k, mode) : NO_WINDOW;
    if (w.length > 0) {
      x = new GuardedRange(x, w[0], w[1], Overflow.FAIL);
    }
    VarkaVectorIR out;
    if (k > digits(lane)) {
      // 10^k exceeds every magnitude of the type by more than twice, so every value rounds to
      // 0, which keeps the operand's validity as a multiply by zero.
      out = new IntArith(IntOp.MUL, Overflow.WRAP, x,
          VarkaNodeCompiler.slot(0, LaneType.INT, literals));
    } else {
      int d = BigInteger.TEN.pow(k).intValueExact();
      out = remainderForm(x, new ConstDivide(x, d),
          v -> VarkaNodeCompiler.slot((int) v, LaneType.INT, literals), d, mode);
    }
    return lane == LaneType.INT ? out : new NarrowLane(out, lane);
  }

  /**
   * The rounding at the long lane. {@code 10^k} is a long up to {@code k = 18}, and the
   * conversion route of {@link ConstDivide} is exact only for a dividend under
   * {@link ConstDivide#EXACT_DIVIDEND_BOUND}, so the rounding is {@link #preShiftForm}'s, over
   * the magnitude, whose dividend is {@code |x| >>> k}: the operand is
   * guarded to {@link #preShiftLimit}, {@code |x| < 2^(52 + k)}, and a batch with a lane past it
   * declines; from {@code k = 12} every long is inside. ANSI's tails narrow the guard where they
   * reach into it. At {@code k = 19} the quotient is 0 for every long, so the remainder is the
   * operand and needs no division, and from {@code k = 20} every long rounds to 0.
   */
  private static VarkaVectorIR roundLong(VarkaVectorIR x, int k, RoundingMode mode,
      boolean ansi, DeclineSink sink) {
    long[] w = ansi ? window(LaneType.LONG, k, mode) : NO_WINDOW;
    if (k <= 18) {
      long[] ends = preShiftGuard(k);
      long lo = ends[0];
      long hi = ends[1];
      if (w.length > 0) {
        lo = Math.max(lo, w[0]);
        hi = Math.min(hi, w[1]);
      }
      VarkaVectorIR g = lo == Long.MIN_VALUE && hi == Long.MAX_VALUE ? x
          : new GuardedRange(x, lo, hi, Overflow.FAIL);
      return preShiftForm(g, k, lo, hi, mode, sink);
    }
    VarkaVectorIR g = w.length == 0 ? x : new GuardedRange(x, w[0], w[1], Overflow.FAIL);
    if (k >= 20) {
      return new IntArith(IntOp.MUL, Overflow.WRAP, g, sink.longSlot(0L));
    }
    // k = 19: q0 = 0 and r = x; the half is 5 * 10^18 and 10^19 wraps to its low 64 bits.
    long h = 5_000_000_000_000_000_000L;
    long dWrapped = BigInteger.TEN.pow(19).longValue();
    if (mode == RoundingMode.DOWN) {
      return new IntArith(IntOp.MUL, Overflow.WRAP, g, sink.longSlot(0L));
    }
    boolean halfUp = mode == RoundingMode.HALF_UP;
    Cond up = new Compare(halfUp ? CompareOp.GE : CompareOp.GT, g, sink.longSlot(h));
    Cond down = new Compare(halfUp ? CompareOp.LE : CompareOp.LT, g, sink.longSlot(-h));
    // CondValue, not IfElse over literals: a null operand leaves both conditions unknown, and
    // the answer must stay null rather than take a literal arm.
    VarkaVectorIR q = new IntArith(IntOp.SUB, Overflow.WRAP, new CondValue(up, LaneType.LONG),
        new CondValue(down, LaneType.LONG));
    return new IntArith(IntOp.MUL, Overflow.WRAP, q, sink.longSlot(dWrapped));
  }

  /**
   * The largest magnitude the pre-shift at scale {@code -k} divides exactly, so that the guard
   * is {@code [-limit, limit]}: {@code 2^(52 + k) - 1} up to {@code k = 10}, every long but
   * {@code Long.MIN_VALUE} at {@code k = 11}, and every long from {@code k = 12}, answered as
   * {@code Long.MAX_VALUE}, where {@link #preShiftGuard} admits {@code Long.MIN_VALUE} too. At
   * {@code k = 11} the magnitude of {@code Long.MIN_VALUE} is {@code 2^63}, and
   * {@code 2^63 >>> 11} is {@code 2^52}, one past the division's bound.
   */
  public static long preShiftLimit(int k) {
    if (k < 1 || k > 18) {
      throw VarkaErrors.illegalArgument("the pre-shift serves scales -1 to -18, not -" + k);
    }
    return k >= 11 ? Long.MAX_VALUE : (1L << (52 + k)) - 1;
  }

  /**
   * The pre-shift's own guard at scale {@code -k}, as {@code [lo, hi]}: {@code [-limit, limit]}
   * of {@link #preShiftLimit}, and every long from {@code k = 12}.
   */
  public static long[] preShiftGuard(int k) {
    long limit = preShiftLimit(k);
    return new long[] {k >= 12 ? Long.MIN_VALUE : -limit, limit};
  }

  /**
   * The rounding of {@code x} inside {@code [lo, hi]} at scale {@code -k}, over its magnitude:
   * {@code s = x >> 63} is 0 or -1, {@code a = (x ^ s) - s} is {@code |x|} (read unsigned, so
   * {@code Long.MIN_VALUE} gives {@code 2^63}), {@code a >>> k} is {@code floor(|x| / 2^k)}, and
   * its quotient {@code Q} by {@code 5^k} is {@code floor(|x| / 10^k)}, since nested floor
   * divisions by positive integers compose. The remainder {@code R = a - Q * 10^k} is not
   * negative, so {@code Q} steps up by one past the half - at it too for {@code HALF_UP}
   * ({@code R >= h}), at it only for an odd {@code Q} for {@code HALF_EVEN}
   * ({@code R + (Q & 1) > h}, one compare where the tie test would be three nodes), never for
   * {@code DOWN} - and the magnitude {@code M} times {@code 10^k} gets its sign back as a
   * multiply by {@code s | 1}, which is 1 or -1, every step wrapping: the rounding is symmetric
   * in the sign, and its low 64 bits are {@code longValue()}'s. Both choices keep the chain
   * shallow: this form is 14 deep for {@code HALF_EVEN}, under the emitter's
   * {@code MAX_CHAIN_DEPTH} of 16, leaving room for a CASE or a comparison above it. The
   * dividend {@code a >>> k} is under the bound stated to the division, derived from the
   * guard's ends; {@code integral_round.smt2} proves each step.
   */
  private static VarkaVectorIR preShiftForm(VarkaVectorIR x, int k, long lo, long hi,
      RoundingMode mode, DeclineSink sink) {
    long bound = preShiftBound(k, lo, hi);
    long d = BigInteger.TEN.pow(k).longValueExact();
    VarkaVectorIR s = new BitOp(BitOpKind.SHR, x, sink.longSlot(63L));
    VarkaVectorIR a = new IntArith(IntOp.SUB, Overflow.WRAP,
        new BitOp(BitOpKind.XOR, x, s), s);
    VarkaVectorIR t = new BitOp(BitOpKind.USHR, a, sink.longSlot(k));
    long five = BigInteger.valueOf(5).pow(k).longValueExact();
    VarkaVectorIR q = new ConstDivide(t, five, bound);
    VarkaVectorIR dSlot = sink.longSlot(d);
    VarkaVectorIR qd = new IntArith(IntOp.MUL, Overflow.WRAP, q, dSlot);
    VarkaVectorIR m;
    if (mode == RoundingMode.DOWN) {
      m = qd;
    } else {
      VarkaVectorIR r = new IntArith(IntOp.SUB, Overflow.WRAP, a, qd);
      VarkaVectorIR half = sink.longSlot(d / 2);
      Cond up = mode == RoundingMode.HALF_UP
          ? new Compare(CompareOp.GE, r, half)
          : new Compare(CompareOp.GT, new IntArith(IntOp.ADD, Overflow.WRAP, r,
              new BitOp(BitOpKind.AND, q, sink.longSlot(1L))), half);
      // CondValue, not IfElse: see remainderForm.
      m = new IntArith(IntOp.MUL, Overflow.WRAP,
          new IntArith(IntOp.ADD, Overflow.WRAP, q, new CondValue(up, LaneType.LONG)), dSlot);
    }
    VarkaVectorIR sign = new BitOp(BitOpKind.OR, s, sink.longSlot(1L));
    return new IntArith(IntOp.MUL, Overflow.WRAP, m, sign);
  }

  /**
   * The exclusive bound on the pre-shift's dividend {@code |x| >>> k} over {@code x} in
   * {@code [lo, hi]}, which is what the division is told: the larger end's magnitude, read
   * unsigned, shifted, plus one. It refuses a range whose dividend passes
   * {@link ConstDivide#EXACT_DIVIDEND_BOUND}.
   */
  public static long preShiftBound(int k, long lo, long hi) {
    long magLo = lo == Long.MIN_VALUE ? Long.MIN_VALUE : -Math.min(lo, 0L);
    long magHi = Math.max(hi, 0L);
    long mag = Long.compareUnsigned(magLo, magHi) >= 0 ? magLo : magHi;
    long bound = (mag >>> k) + 1;
    if (bound > ConstDivide.EXACT_DIVIDEND_BOUND) {
      throw VarkaErrors.illegalState("the pre-shift at scale -" + k + " over [" + lo + ", " + hi
          + "] gives a dividend past the division's bound");
    }
    return bound;
  }

  /** A literal on the computing lane. */
  @FunctionalInterface
  private interface Slot {
    VarkaVectorIR of(long v);
  }

  /**
   * {@code q * d} with {@code q} the truncating quotient {@code q0} stepped away from zero by
   * the remainder's position against the half, per {@code mode}; every value is on {@code x}'s
   * lane and wraps.
   */
  private static VarkaVectorIR remainderForm(VarkaVectorIR x, VarkaVectorIR q0, Slot slot,
      long d, RoundingMode mode) {
    VarkaVectorIR dSlot = slot.of(d);
    if (mode == RoundingMode.DOWN) {
      return new IntArith(IntOp.MUL, Overflow.WRAP, q0, dSlot);
    }
    VarkaVectorIR r = new IntArith(IntOp.SUB, Overflow.WRAP, x,
        new IntArith(IntOp.MUL, Overflow.WRAP, q0, dSlot));
    long h = d / 2;
    VarkaVectorIR half = slot.of(h);
    VarkaVectorIR negHalf = slot.of(-h);
    VarkaVectorIR one = slot.of(1);
    Cond up;
    Cond down;
    if (mode == RoundingMode.HALF_UP) {
      up = new Compare(CompareOp.GE, r, half);
      down = new Compare(CompareOp.LE, r, negHalf);
    } else {
      // The tie test folded into one compare per side, as preShiftForm does: r and h are
      // integers and q0 & 1 is 0 or 1 (1 for a negative odd q0 too, in two's complement), so
      // r + (q0 & 1) > h is r > h, or r == h with q0 odd, and r - (q0 & 1) < -h its mirror.
      // |r| < d <= 10^9, so neither wraps. The chain is a node shallower than the Or of an
      // And would be, room for a CASE or a comparison above a narrow operand under ANSI.
      VarkaVectorIR odd = new BitOp(BitOpKind.AND, q0, one);
      up = new Compare(CompareOp.GT, new IntArith(IntOp.ADD, Overflow.WRAP, r, odd), half);
      down = new Compare(CompareOp.LT, new IntArith(IntOp.SUB, Overflow.WRAP, r, odd), negHalf);
    }
    // The steps as values, q0 + [up] - [down], rather than as IfElse arms: a guard below a
    // conditional's arms is qualified by their conditions, and an arm-free tree keeps the ANSI
    // guard qualified by an enclosing CASE alone. CondValue is null where its condition is
    // unknown, which is where the operand is null.
    LaneType lane = q0.laneType();
    VarkaVectorIR q = new IntArith(IntOp.SUB, Overflow.WRAP,
        new IntArith(IntOp.ADD, Overflow.WRAP, q0, new CondValue(up, lane)),
        new CondValue(down, lane));
    return new IntArith(IntOp.MUL, Overflow.WRAP, q, dSlot);
  }

  /** The most decimal digits {@code 10^k} may have before every value rounds to zero. */
  private static int digits(LaneType lane) {
    return switch (lane) {
      case BYTE -> 2;
      case SHORT -> 4;
      case INT -> 9;
      case LONG -> 18;
      case FLOAT, DOUBLE -> throw VarkaErrors.illegalArgument("a floating lane: " + lane);
    };
  }

  /**
   * The inputs of {@code lane}'s type whose rounding at scale {@code -k} fits the type, as
   * {@code [lo, hi]}, or {@link #NO_WINDOW} when every input fits. Bisected over
   * {@code BigDecimal}'s exact rounding, which is monotone in the input, so the bounds are
   * Spark's own.
   */
  public static long[] window(LaneType lane, int k, RoundingMode mode) {
    long min = minOf(lane);
    long max = maxOf(lane);
    long lo = min;
    long hi = max;
    if (!fits(max, k, mode, min, max)) {
      long a = 0;
      long b = max;
      while (a < b) {
        // The difference read unsigned: over a long's range it passes 2^63.
        long m = a + ((b - a + 1) >>> 1);
        if (fits(m, k, mode, min, max)) {
          a = m;
        } else {
          b = m - 1;
        }
      }
      hi = a;
    }
    if (!fits(min, k, mode, min, max)) {
      long a = min;
      long b = 0;
      while (a < b) {
        long m = a + ((b - a) >>> 1);
        if (fits(m, k, mode, min, max)) {
          b = m;
        } else {
          a = m + 1;
        }
      }
      lo = a;
    }
    return lo == min && hi == max ? NO_WINDOW : new long[] {lo, hi};
  }

  private static boolean fits(long v, int k, RoundingMode mode, long min, long max) {
    BigInteger r = BigDecimal.valueOf(v).setScale(-k, mode).toBigInteger();
    return r.compareTo(BigInteger.valueOf(min)) >= 0 && r.compareTo(BigInteger.valueOf(max)) <= 0;
  }

  private static long minOf(LaneType lane) {
    return switch (lane) {
      case BYTE -> Byte.MIN_VALUE;
      case SHORT -> Short.MIN_VALUE;
      case INT -> Integer.MIN_VALUE;
      case LONG -> Long.MIN_VALUE;
      case FLOAT, DOUBLE -> throw VarkaErrors.illegalArgument("a floating lane: " + lane);
    };
  }

  private static long maxOf(LaneType lane) {
    return switch (lane) {
      case BYTE -> Byte.MAX_VALUE;
      case SHORT -> Short.MAX_VALUE;
      case INT -> Integer.MAX_VALUE;
      case LONG -> Long.MAX_VALUE;
      case FLOAT, DOUBLE -> throw VarkaErrors.illegalArgument("a floating lane: " + lane);
    };
  }
}
