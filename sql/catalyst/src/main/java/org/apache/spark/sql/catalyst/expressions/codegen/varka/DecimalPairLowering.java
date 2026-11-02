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

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.And;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Compare;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.CompareOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Cond;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.NullSafeEqual;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Or;

/**
 * The comparisons of two 128-bit decimals of one scale over their words (VARKA-359, its plan's
 * 2.5), each operand a low word {@code lo} and a high word {@code hi} with
 * {@code v = hi * 2^64 + lo}: the values order as the high words do read signed and, where those
 * are equal, as the low words do read unsigned. Each is composed of long-lane conditions, so the
 * three-valued rules hold word by word: a null row makes every word's compare unknown, and
 * {@code unknown OR (unknown AND unknown)} is unknown. {@code decimal128.smt2} proves each form
 * equal to the comparison of the two values.
 *
 * <p>{@code x <> y} is Spark's {@code NOT(x = y)}, which the condition compiler builds over the
 * equality here.
 */
public final class DecimalPairLowering {

  private DecimalPairLowering() {
  }

  /**
   * {@code x op y} for a signed {@code op}: {@code EQ(x.hi, y.hi) & EQ(x.lo, y.lo)} for
   * {@code =}, and for an order {@code op(x.hi, y.hi) | (EQ(x.hi, y.hi) & uop(x.lo, y.lo))}
   * with {@code op}'s strict form on the high words and its unsigned form on the low ones.
   */
  public static Cond compare(CompareOp op, VarkaVectorIR xLo, VarkaVectorIR xHi,
      VarkaVectorIR yLo, VarkaVectorIR yHi) {
    if (op.isUnsigned()) {
      throw VarkaErrors.illegalArgument("a decimal comparison is signed, not " + op);
    }
    if (op == CompareOp.EQ) {
      return new And(new Compare(CompareOp.EQ, xHi, yHi), new Compare(CompareOp.EQ, xLo, yLo));
    }
    CompareOp strict = switch (op) {
      case LT, LE -> CompareOp.LT;
      case GT, GE -> CompareOp.GT;
      case EQ, ULT, ULE, UGT, UGE -> throw VarkaErrors.illegalState("unreachable: " + op);
    };
    CompareOp low = lowWord(op);
    return new Or(new Compare(strict, xHi, yHi),
        new And(new Compare(CompareOp.EQ, xHi, yHi), new Compare(low, xLo, yLo)));
  }

  /**
   * {@code x <=> y}: {@code NullSafeEqual} on each word, joined by {@code AND}. Both words of a
   * row are null together, so each word's answer is the pair's on a null row - true where both
   * are null, false where one is - and on a valid row the AND is the equality of the values.
   */
  public static Cond nullSafeEqual(VarkaVectorIR xLo, VarkaVectorIR xHi, VarkaVectorIR yLo,
      VarkaVectorIR yHi) {
    return new And(new NullSafeEqual(xHi, yHi), new NullSafeEqual(xLo, yLo));
  }

  /**
   * The low words' compare of an order {@code op}: its unsigned form. A test plants the signed
   * form at emit time, from the emission's options
   * ({@link VarkaEmitOptions.DecimalFault#LOW_WORD_SIGNED}), and never here.
   */
  private static CompareOp lowWord(CompareOp op) {
    return switch (op) {
      case LT -> CompareOp.ULT;
      case LE -> CompareOp.ULE;
      case GT -> CompareOp.UGT;
      case GE -> CompareOp.UGE;
      case EQ, ULT, ULE, UGT, UGE -> throw VarkaErrors.illegalState("unreachable: " + op);
    };
  }
}
