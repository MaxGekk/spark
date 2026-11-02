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
import java.util.List;
import java.util.Optional;

import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.DecimalPairLowering;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.DecimalWords;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaErrors;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaTypes;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.BitOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.BitOpKind;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Compare;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.CompareOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Cond;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DecimalColumn;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DecimalWord;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.NullSafeEqual;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Word;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.WidenDecimal;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;

/**
 * The operands of the decimal predicates a kernel computes today (VARKA-347, its plan's 2.5): the
 * comparisons of two decimals of one scale, and the null tests. Each operand is an unscaled long
 * on the long lane, a decimal column's {@link DecimalColumn} leaf or a literal's slot.
 *
 * <p>Why the long compare is Spark's answer. Two values of {@code DECIMAL(p, s)} at the same
 * scale {@code s} are {@code u1 / 10^s} and {@code u2 / 10^s}, so they order and compare equal
 * exactly as their unscaled values {@code u1} and {@code u2} do, and {@code Decimal.compare}
 * compares by value (the long fields where both are compact, {@code BigDecimal.compareTo}
 * otherwise, which ignores the representation). Where the operands' precisions differ, Spark's
 * type coercion ({@code DecimalPrecisionTypeCoercion.decimalAndDecimal}) casts both to the wider
 * type, {@code DECIMAL(max(p1 - s, p2 - s) + s, s)} for one scale: a cast to the same scale and a
 * larger precision, which changes no unscaled value and cannot fail, so it is its child here, as
 * {@code TIME}'s widening precision cast is in {@code VarkaTimeCompiler}.
 *
 * <p>A comparison whose common type is wider than 18 digits compares 128-bit lane pairs
 * ({@link Pair}, VARKA-359): a column of precision 19 to 38 is its two {@link DecimalWord}
 * leaves, a column of 18 digits or fewer its long sign-extended, and a literal its two words in
 * the long table; {@link DecimalPairLowering} composes the comparison over the words.
 *
 * <p>What stays declined, each naming its row: operands of two scales, whose comparison needs a
 * rescale (row 348, and above 18 digits rows 360 and 363); and every other decimal operand, a
 * cast that changes the scale or an arithmetic result (rows 348 to 350). A decimal in a value
 * position - a choice, an arithmetic operand, an output - is declined by
 * {@code VarkaTimeCompiler}'s decimal arm, and a decimal-valued output root, once a lowering
 * builds one, is admitted by {@link #outputRoot}.
 */
final class VarkaDecimalCompiler {

  /** The largest unscaled value of 18 digits, {@code 10^18 - 1}. */
  private static final long MAX_UNSCALED = new DecimalColumn(0, LaneType.LONG,
      DecimalColumn.MAX_LONG_PRECISION).bound();

  private VarkaDecimalCompiler() {
  }

  /** Whether {@code e} is a decimal, which only this class compiles as an operand. */
  static boolean isDecimal(Expression e) {
    return e.dataType() instanceof DecimalType;
  }

  /**
   * The two operands of a comparison of decimals, as unscaled longs of one scale, or empty with
   * the reason noted. Type coercion gives both operands one type; the check is kept so that a
   * hand-built tree of two scales declines rather than comparing unscaled values of two scales.
   */
  private static Optional<List<VarkaVectorIR>> operands(Expression whole, Expression left,
      Expression right, VarkaSlotTable inputs, DeclineSink sink) {
    DecimalType lt = (DecimalType) left.dataType();
    boolean wide = widerThanLong(left, right);
    if (!(right.dataType() instanceof DecimalType rt) || lt.scale() != rt.scale()) {
      return sink.decline(wide
          ? "a comparison of decimals of two scales above " + DecimalColumn.MAX_LONG_PRECISION
              + " digits needs row 360's rescale up and row 363's capped rescale down"
          : "a comparison of decimals of two scales, which needs a rescale, is row 348's", whole);
    }
    if (wide) {
      // Unreachable: a comparison of one scale above 18 digits is wideType's, compiled as pairs.
      throw VarkaErrors.illegalState("a comparison wider than "
          + DecimalColumn.MAX_LONG_PRECISION + " digits is compiled over pairs: " + whole);
    }
    Optional<VarkaVectorIR> l = operand(left, lt.scale(), inputs, sink);
    if (l.isEmpty()) {
      return Optional.empty();
    }
    Optional<VarkaVectorIR> r = operand(right, lt.scale(), inputs, sink);
    if (r.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(List.of(l.get(), r.get()));
  }

  /**
   * {@code left op right} over two decimals: the unscaled longs' compare up to 18 digits, the
   * pairs' composed compare above ({@link DecimalPairLowering#compare}); or empty with the
   * reason noted.
   */
  static Optional<Cond> compare(CompareOp op, Expression whole, Expression left,
      Expression right, VarkaSlotTable inputs, DeclineSink sink) {
    Optional<DecimalType> wide = wideType(left, right);
    if (wide.isEmpty()) {
      return operands(whole, left, right, inputs, sink)
          .map(both -> new Compare(op, both.get(0), both.get(1)));
    }
    return pairs(left, right, wide.get(), inputs, sink)
        .map(p -> DecimalPairLowering.compare(op, p.get(0).lo(), p.get(0).hi(), p.get(1).lo(),
            p.get(1).hi()));
  }

  /** {@code left <=> right} over two decimals, as {@link #compare} takes them. */
  static Optional<Cond> nullSafeEqual(Expression whole, Expression left, Expression right,
      VarkaSlotTable inputs, DeclineSink sink) {
    Optional<DecimalType> wide = wideType(left, right);
    if (wide.isEmpty()) {
      return operands(whole, left, right, inputs, sink)
          .map(both -> new NullSafeEqual(both.get(0), both.get(1)));
    }
    return pairs(left, right, wide.get(), inputs, sink)
        .map(p -> DecimalPairLowering.nullSafeEqual(p.get(0).lo(), p.get(0).hi(),
            p.get(1).lo(), p.get(1).hi()));
  }

  /**
   * The common type of a comparison whose operands are two decimals of one scale, one of them
   * wider than 18 digits: the wider of the two, which type coercion has already cast both to.
   * Empty for any other comparison, which {@link #operands} takes or declines.
   */
  private static Optional<DecimalType> wideType(Expression left, Expression right) {
    if (left.dataType() instanceof DecimalType lt && right.dataType() instanceof DecimalType rt
        && lt.scale() == rt.scale() && widerThanLong(left, right)) {
      return Optional.of(lt.precision() >= rt.precision() ? lt : rt);
    }
    return Optional.empty();
  }

  /**
   * Whether either operand is a decimal of more than 18 digits, whatever the scales: the
   * comparison is a pair's, compiled by {@link #wideType}'s path at one scale and declined
   * naming the rescales at two.
   */
  private static boolean widerThanLong(Expression left, Expression right) {
    return isWide(left) || isWide(right);
  }

  private static boolean isWide(Expression e) {
    return e.dataType() instanceof DecimalType t
        && t.precision() > DecimalColumn.MAX_LONG_PRECISION;
  }

  /** Both operands of a wide comparison as pairs of {@code type}, or empty with the reason. */
  private static Optional<List<Pair>> pairs(Expression left, Expression right, DecimalType type,
      VarkaSlotTable inputs, DeclineSink sink) {
    Optional<Pair> l = pairOperand(left, type, inputs, sink);
    if (l.isEmpty()) {
      return Optional.empty();
    }
    Optional<Pair> r = pairOperand(right, type, inputs, sink);
    if (r.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(List.of(l.get(), r.get()));
  }

  /**
   * One operand of a wide comparison as a pair of {@code type}: a column of precision 19 to 38
   * as its two word leaves, a column of 18 digits or fewer as its long sign-extended
   * ({@link #widened}), each through any widening casts, which change no unscaled value; or a
   * literal's two words ({@link #literalPair}).
   */
  private static Optional<Pair> pairOperand(Expression e, DecimalType type,
      VarkaSlotTable inputs, DeclineSink sink) {
    Expression stripped = stripWidening(e);
    if (stripped instanceof BoundReference br) {
      if (VarkaTypes.read(br.dataType()).isEmpty()) {
        return sink.decline(VarkaTypes.declineReason(br.dataType())
            .orElse("a decimal column on no lane"), e);
      }
      int precision = ((DecimalType) br.dataType()).precision();
      if (precision <= DecimalColumn.MAX_LONG_PRECISION) {
        return Optional.of(widened(column(br, inputs), sink));
      }
      int ordinal = inputs.intern(br.ordinal());
      return Optional.of(new Pair(new DecimalWord(ordinal, Word.LOW, precision),
          new DecimalWord(ordinal, Word.HIGH, precision)));
    }
    if (isLiteralTree(e)) {
      Object value;
      try {
        value = e.eval(null);
      } catch (RuntimeException failed) {
        return sink.decline("a decimal literal whose cast fails", e);
      }
      if (!(value instanceof Decimal d)) {
        return sink.decline("a null decimal literal", e);
      }
      return literalPair(e, d, type, sink);
    }
    return sink.decline("a decimal operand that is not a column or a literal: its operators "
        + "are rows 348 to 350's", e);
  }

  /**
   * The output root of {@code value}, the unscaled long-lane value of the decimal-valued
   * {@code whole}, or empty with the reason noted: {@code WidenDecimal(value)}, whose store writes
   * each row's word and its sign extension into the output's sixteen-byte slot, where the
   * output's type admits that store ({@code VarkaTypes.decimalOutputDeclineReason}), and a
   * decline naming row 359 for a precision above 18, so such a shape never fuses rather than
   * reaching the evaluator's allocator, which refuses it too. The binding that knows an output's
   * type calls it, and only for an output root (`VARKA-157.md` 2.1); no lowering builds a
   * decimal-valued root before 157b's producers, so its tests are its only callers until then.
   */
  static Optional<VarkaVectorIR> outputRoot(Expression whole, VarkaVectorIR value,
      DeclineSink sink) {
    if (!(whole.dataType() instanceof DecimalType type)) {
      throw VarkaErrors.illegalArgument("a decimal output root over a "
          + whole.dataType().simpleString() + ": " + whole);
    }
    Optional<String> refused = VarkaTypes.decimalOutputDeclineReason(type);
    if (refused.isPresent()) {
      return sink.decline(refused.get(), whole);
    }
    return Optional.of(new WidenDecimal(value));
  }

  /**
   * The column a null test over a decimal reads, or empty with the reason noted. A widening cast
   * of the column is null exactly where the column is, so it is the column here too. The test
   * reads the column's validity word, and the leaf beneath it still loads and checks the slots:
   * Spark's {@code IS NULL} evaluates its child, which reads a valid row's value through
   * {@code ArrowColumnVector.getDecimal} and raises on a slot outside the precision, so the
   * kernel declines that batch as every other reader of the leaf does.
   */
  static Optional<VarkaVectorIR> nullTested(Expression whole, Expression child,
      VarkaSlotTable inputs, DeclineSink sink) {
    Expression column = stripWidening(child);
    if (column instanceof BoundReference br) {
      if (VarkaTypes.read(br.dataType()).isEmpty()) {
        return sink.decline(VarkaTypes.declineReason(br.dataType())
            .orElse("a decimal column on no lane"), whole);
      }
      int precision = ((DecimalType) br.dataType()).precision();
      if (precision > DecimalColumn.MAX_LONG_PRECISION) {
        // Either word's validity is the column's; the low one's leaf loads and checks the slots.
        return Optional.of(new DecimalWord(inputs.intern(br.ordinal()), Word.LOW, precision));
      }
      return Optional.of(column(br, inputs));
    }
    return sink.decline("a null test over a decimal that is not a column", whole);
  }

  /**
   * One operand of scale {@code scale}: a decimal column the registry admits, through any
   * widening casts, or a literal of the scale, whose unscaled value takes a slot in the long
   * table. A literal arrives folded in a real query; in a tree the analyzer alone resolved it is
   * the literal under the casts type coercion put over it, and is folded here by evaluating
   * them, which Spark's constant folding does the same way.
   */
  private static Optional<VarkaVectorIR> operand(Expression e, int scale, VarkaSlotTable inputs,
      DeclineSink sink) {
    Expression stripped = stripWidening(e);
    if (stripped instanceof BoundReference br) {
      if (VarkaTypes.read(br.dataType()).isEmpty()) {
        return sink.decline(VarkaTypes.declineReason(br.dataType())
            .orElse("a decimal column on no lane"), e);
      }
      return Optional.of(column(br, inputs));
    }
    if (isLiteralTree(e)) {
      return literal(e, scale, sink);
    }
    return sink.decline("a decimal operand that is not a column or a literal: its operators "
        + "are rows 348 to 350's", e);
  }

  /** The leaf of a decimal column on the long lane, its precision the column's. */
  private static DecimalColumn column(BoundReference br, VarkaSlotTable inputs) {
    int precision = ((DecimalType) br.dataType()).precision();
    return new DecimalColumn(inputs.intern(br.ordinal()), LaneType.LONG, precision);
  }

  /**
   * {@code e} without the casts over it that keep its scale and widen its precision: each is the
   * identity on the unscaled value, and none can fail, in any mode.
   */
  private static Expression stripWidening(Expression e) {
    Expression at = e;
    while (at instanceof Cast c && c.dataType() instanceof DecimalType to
        && c.child().dataType() instanceof DecimalType from
        && to.scale() == from.scale() && to.precision() >= from.precision()) {
      at = c.child();
    }
    return at;
  }

  /** Whether {@code e} is a literal under zero or more casts, so evaluating it reads nothing. */
  private static boolean isLiteralTree(Expression e) {
    Expression at = e;
    while (at instanceof Cast c) {
      at = c.child();
    }
    return at instanceof Literal;
  }

  /**
   * A literal operand's unscaled value at {@code scale}, in a slot of the long table. A null
   * literal declines: the optimizer has folded the comparison to null before a real query gets
   * here. A cast that fails, or a value that does not fit 18 digits, declines too, and leaves the
   * row engine to raise or answer.
   */
  private static Optional<VarkaVectorIR> literal(Expression e, int scale, DeclineSink sink) {
    Object value;
    try {
      value = e.eval(null);
    } catch (RuntimeException failed) {
      return sink.decline("a decimal literal whose cast fails", e);
    }
    if (!(value instanceof Decimal d)) {
      return sink.decline("a null decimal literal", e);
    }
    BigDecimal exact = d.toJavaBigDecimal();
    long unscaled;
    try {
      unscaled = exact.setScale(scale).unscaledValue().longValueExact();
    } catch (ArithmeticException notExact) {
      return sink.decline("a decimal literal with no exact long at the scale", e);
    }
    if (unscaled > MAX_UNSCALED || unscaled < -MAX_UNSCALED) {
      return sink.decline("a decimal literal wider than " + DecimalColumn.MAX_LONG_PRECISION
          + " digits is a 128-bit lane pair, row 359's", e);
    }
    return Optional.of(sink.longSlot(unscaled));
  }

  // -----------------------------------------------------------------------------------------
  // The 128-bit lane pair (VARKA-359): its compile-time value and the operands built into it.
  // -----------------------------------------------------------------------------------------

  /**
   * A decimal of precision 19 to 38 as its two words, both long-lane nodes: {@code lo} the
   * unscaled value's low 64 bits, read unsigned by the pair's comparisons, and {@code hi} its
   * high 64 bits, signed. A compile-time value, never an IR node (`m8/VARKA-359.md` 2.2): every
   * operator over a pair is composed from long-lane nodes over the two words.
   */
  record Pair(VarkaVectorIR lo, VarkaVectorIR hi) {
    Pair {
      if (lo.laneType() != LaneType.LONG || hi.laneType() != LaneType.LONG) {
        throw new IllegalArgumentException("a decimal pair's words are on the long lane, not "
            + lo.laneType() + " and " + hi.laneType());
      }
    }
  }

  /**
   * A 64-bit decimal operand {@code x}, an unscaled long, as a pair of a wider type at the same
   * scale: its low word is {@code x} and its high word {@code x >> 63}, the sign
   * extension. Type coercion casts {@code d18 < d38} to the wider type at one scale, which
   * changes no unscaled value, so the extension is the whole of the cast.
   */
  static Pair widened(VarkaVectorIR x, DeclineSink sink) {
    return new Pair(x, new BitOp(BitOpKind.SHR, x, sink.longSlot(Long.SIZE - 1)));
  }

  /**
   * A literal's unscaled value at {@code type}'s scale as a pair: two slots of the long table,
   * its low and high words ({@link DecimalWords}). Empty, with the reason noted, for a value
   * with no exact unscaled integer at the scale or one with more digits than the type has.
   */
  static Optional<Pair> literalPair(Expression e, Decimal value, DecimalType type,
      DeclineSink sink) {
    BigInteger unscaled;
    try {
      unscaled = value.toJavaBigDecimal().setScale(type.scale()).unscaledValue();
    } catch (ArithmeticException notExact) {
      return sink.decline("a decimal literal with no exact unscaled value at the scale", e);
    }
    if (unscaled.abs().compareTo(BigInteger.TEN.pow(type.precision())) >= 0) {
      return sink.decline("a decimal literal with more than " + type.precision() + " digits",
          e);
    }
    DecimalWords words = DecimalWords.of(unscaled);
    return Optional.of(new Pair(sink.longSlot(words.lo()), sink.longSlot(words.hi())));
  }
}
