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

import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaTypes;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Cond;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.CondValue;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ConvertLane;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.GuardedRange;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.NarrowLane;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.WidenLane;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * The floating casts family (VARKA-343): Spark's casts to {@code FLOAT} and {@code DOUBLE} from
 * {@code TINYINT}, {@code SMALLINT}, {@code INT} and {@code BOOLEAN}, and between {@code FLOAT}
 * and {@code DOUBLE}. A 32-bit value beside a 64-bit one fills the low half of its species, so
 * one kernel holds lanes of two counts (`VARKA-343.md` 2.2).
 *
 * <p>Spark computes each with Java's primitive cast, {@code (float) c} or {@code (double) c}, and
 * a boolean as {@code 1.0} or {@code 0.0}, in every mode: a conversion to a floating type never
 * raises, so TRY's answer is LEGACY's and ANSI's ({@code Cast.castToFloatCode},
 * {@code castToDoubleCode}). So each is one {@link ConvertLane}, exact by construction: the vector
 * conversion is the JLS's on every lane. A narrow source is the {@code INT} value its load widens
 * to, and a boolean is the {@code INT} one or zero its condition gives as a {@link CondValue},
 * narrowed from the long lane where its condition compares longs, then converted, which is exact.
 *
 * <p>From a floating type to an integral one ({@link #toIntegral}), Spark's LEGACY answer is
 * Java's cast, JLS 5.1.3: NaN to 0, saturation at the int or long range, and a {@code TINYINT}
 * or {@code SMALLINT} as the int conversion wrapped. ANSI raises {@code CAST_OVERFLOW} unless
 * {@code Math.floor(v) <= MAX && Math.ceil(v) >= MIN} in double arithmetic, NaN failing both
 * ({@code FloatExactNumeric}, {@code DoubleExactNumeric}, {@code CastUtils.floatToByteExact}
 * and its kin), and TRY answers null where ANSI raises. So each is the same conversion over a
 * {@link GuardedRange} of the source's values that pass that test
 * ({@link VarkaTypes#castBounds}), failing under ANSI and nulling under TRY (`VARKA-343.md` 2.4).
 *
 * <p>The {@code DATE} pairs (always null under LEGACY, the floating lane's typed null) and every
 * other source decline by {@link VarkaNodeCompiler}'s {@code floatingDecline}, which admits
 * exactly the pairs {@link #admits} names.
 */
final class VarkaFloatingCastCompiler {

  private VarkaFloatingCastCompiler() {
  }

  /** The family's claim: a cast {@link #admits} names, or null. */
  static @Nullable VarkaFamilyArm arm(
      Expression e,
      VarkaSlotTable inputs,
      VarkaSlotTable literals,
      DeclineSink sink) {
    if (!(e instanceof Cast c) || !admits(c)) {
      return null;
    }
    if (integralTarget(c.dataType()).isPresent()) {
      return () -> toIntegral(c, inputs, literals, sink);
    }
    LaneType target = DataTypes.DoubleType.equals(c.dataType()) ? LaneType.DOUBLE : LaneType.FLOAT;
    return () -> convert(c, target, inputs, literals, sink);
  }

  /**
   * Whether {@code c} is one of this family's pairs: an integral or boolean source to a floating
   * target, or one floating type to the other. A {@code BIGINT} source is admitted here and
   * refused by the kernel plan at the unknown emit level, which may be an AVX2 host's, where
   * {@code convertShape} of a long is the Vector API's Java loop; the level is an emit
   * option the compiler's arms do not read ({@code VarkaKernelPlan}). The identities never reach
   * here, as the optimizer removes them, and are its child where one does.
   */
  static boolean admits(Cast c) {
    DataType to = c.dataType();
    DataType from = c.child().dataType();
    if (integralTarget(to).isPresent()) {
      return DataTypes.FloatType.equals(from) || DataTypes.DoubleType.equals(from);
    }
    if (!DataTypes.FloatType.equals(to) && !DataTypes.DoubleType.equals(to)) {
      return false;
    }
    return DataTypes.ByteType.equals(from) || DataTypes.ShortType.equals(from)
        || DataTypes.IntegerType.equals(from) || DataTypes.LongType.equals(from)
        || DataTypes.BooleanType.equals(from)
        || DataTypes.FloatType.equals(from) || DataTypes.DoubleType.equals(from);
  }

  /** The lane of an integral cast target, {@code TINYINT} to {@code BIGINT}, or empty. */
  private static Optional<LaneType> integralTarget(DataType to) {
    if (DataTypes.ByteType.equals(to)) {
      return Optional.of(LaneType.BYTE);
    }
    if (DataTypes.ShortType.equals(to)) {
      return Optional.of(LaneType.SHORT);
    }
    if (DataTypes.IntegerType.equals(to)) {
      return Optional.of(LaneType.INT);
    }
    return DataTypes.LongType.equals(to) ? Optional.of(LaneType.LONG) : Optional.empty();
  }

  /**
   * A {@code FLOAT} or {@code DOUBLE} value cast to an integral type: {@code ConvertLane} to the
   * int lane, or the long lane for {@code BIGINT}, which is Java's cast; a narrow target is the
   * int answer wrapped by a {@link NarrowLane}, which is JLS 5.1.3's {@code (byte) (int) v}. Under
   * ANSI and TRY the conversion reads a {@link GuardedRange} of {@link VarkaTypes#castBounds}, in
   * the mode {@code FAIL} or {@code NULL}: inside the bounds Spark's answer is the LEGACY one.
   */
  private static Optional<VarkaVectorIR> toIntegral(
      Cast c, VarkaSlotTable inputs, VarkaSlotTable literals, DeclineSink sink) {
    LaneType target = integralTarget(c.dataType()).orElseThrow();
    LaneType source = DataTypes.FloatType.equals(c.child().dataType())
        ? LaneType.FLOAT : LaneType.DOUBLE;
    Overflow mode = VarkaNodeCompiler.overflowOf(c.evalMode());
    Optional<VarkaVectorIR> x = VarkaNodeCompiler.compileNode(c.child(), inputs, literals, sink);
    if (x.isEmpty()) {
      return x;
    }
    VarkaVectorIR value = x.get();
    if (value.laneType() != source) {
      return sink.decline("a " + c.child().dataType().simpleString() + " operand on the "
          + value.laneType() + " lane", c);
    }
    if (mode != Overflow.WRAP) {
      double[] b = VarkaTypes.castBounds(source, target);
      value = GuardedRange.floating(value, b[0], b[1], mode);
    }
    VarkaVectorIR converted =
        new ConvertLane(value, target == LaneType.LONG ? LaneType.LONG : LaneType.INT);
    return Optional.of(target.isNarrow() ? new NarrowLane(converted, target) : converted);
  }

  private static Optional<VarkaVectorIR> convert(
      Cast c, LaneType target, VarkaSlotTable inputs, VarkaSlotTable literals,
      DeclineSink sink) {
    Expression child = c.child();
    if (DataTypes.BooleanType.equals(child.dataType())) {
      return booleanValue(c, inputs, literals, sink).flatMap(x -> converted(x, target));
    }
    // An int is read through the int operand rule, the one place a bare INT column or literal
    // becomes its leaf; every other source is a node compileNode lowers.
    Optional<VarkaVectorIR> x = DataTypes.IntegerType.equals(child.dataType())
        ? VarkaNodeCompiler.intOperand(child, inputs, literals, sink)
        : VarkaNodeCompiler.compileNode(child, inputs, literals, sink);
    if (x.isEmpty()) {
      return x;
    }
    VarkaVectorIR value = x.get();
    if (value.laneType().isNarrow()) {
      // A TINYINT or SMALLINT value: the int its load widens it to, exact, then the conversion.
      value = new WidenLane(value, LaneType.INT);
    }
    return converted(value, target);
  }

  /**
   * {@code value} as {@code target}: itself where it is already there (an identity cast that
   * reached the compiler), or one {@link ConvertLane}.
   */
  private static Optional<VarkaVectorIR> converted(VarkaVectorIR value, LaneType target) {
    if (value.laneType() == target) {
      return Optional.of(value);
    }
    return Optional.of(new ConvertLane(value, target));
  }

  /**
   * A boolean as the int one or zero of its condition, Spark's {@code b ? 1 : 0} with a null
   * where the condition is unknown: a {@link CondValue} at the condition's lane, the int lane
   * for a narrow condition, and on the long lane narrowed to the int one, exactly. So every
   * boolean converts through {@code I2F} or {@code I2D}, which are intrinsic on every x86 host,
   * and never through {@code L2F} or {@code L2D}, which the kernel plan refuses where
   * the level is unknown. A condition on a
   * floating lane has no integral value here and declines: its conversion would cross the kinds
   * twice.
   */
  private static Optional<VarkaVectorIR> booleanValue(
      Cast c, VarkaSlotTable inputs, VarkaSlotTable literals, DeclineSink sink) {
    Optional<Cond> cond = VarkaConditionCompiler.compileCond(c.child(), inputs, literals, sink);
    if (cond.isEmpty()) {
      return Optional.empty();
    }
    Cond k = VarkaConditionCompiler.onIntLane(cond.get());
    if (k.laneType().isFloating()) {
      return sink.decline("a condition on the " + k.laneType() + " lane as a floating value", c);
    }
    CondValue value = new CondValue(k, k.laneType());
    return Optional.of(
        k.laneType() == LaneType.LONG ? new NarrowLane(value, LaneType.INT) : value);
  }
}
