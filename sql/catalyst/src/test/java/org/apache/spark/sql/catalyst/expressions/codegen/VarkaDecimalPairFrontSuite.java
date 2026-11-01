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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaDecimalCompiler.Pair;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.DecimalWords;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSuite;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.BitOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.BitOpKind;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;

/**
 * The compile-time half of the 128-bit lane pair (VARKA-359, its plan's 11.5 item 4): the
 * {@link Pair} of two long-lane words, a literal's two slots and a 64-bit operand's sign
 * extension. Nothing emits a pair yet, so these are checked here and not through a kernel.
 */
@VarkaSuite
public final class VarkaDecimalPairFrontSuite {

  private static DeclineSink sink() {
    return new DeclineSink(CollectionConverters.asScala(List.<Attribute>of()).toSeq(), false);
  }

  private static final DecimalType D38 = DataTypes.createDecimalType(38, 2);

  @Test
  @DisplayName("a literal pair holds its unscaled value's low and high words in two long slots")
  void literalPairWords() {
    for (String text : new String[] {"0.00", "-0.01", "184467440737095516.16",
        "-184467440737095516.17", "999999999999999999999999999999999999.99",
        "-999999999999999999999999999999999999.99", "92233720368547758.08"}) {
      DeclineSink sink = sink();
      Decimal value = Decimal.apply(new java.math.BigDecimal(text));
      Pair pair = VarkaDecimalCompiler.literalPair(Literal.create(value, D38), value, D38, sink)
          .orElseThrow();
      List<Long> table = sink.longLiteralValues();
      long lo = table.get(((LiteralSlot) pair.lo()).index());
      long hi = table.get(((LiteralSlot) pair.hi()).index());
      BigInteger unscaled = new java.math.BigDecimal(text).unscaledValue();
      assertEquals(unscaled, new DecimalWords(lo, hi).value(), text);
      assertEquals(D38, pair.type());
    }
  }

  @Test
  @DisplayName("a literal pair declines a value past the type's digits or with no exact unscaled "
      + "value at its scale")
  void literalPairDeclines() {
    DecimalType d20 = DataTypes.createDecimalType(20, 2);
    Decimal tooWide = Decimal.apply(new java.math.BigDecimal("1000000000000000000.00"));
    assertTrue(VarkaDecimalCompiler.literalPair(Literal.create(tooWide,
        DataTypes.createDecimalType(21, 2)), tooWide, d20, sink()).isEmpty());
    Decimal finer = Decimal.apply(new java.math.BigDecimal("1.005"));
    assertTrue(VarkaDecimalCompiler.literalPair(Literal.create(finer,
        DataTypes.createDecimalType(4, 3)), finer, d20, sink()).isEmpty());
  }

  @Test
  @DisplayName("a 64-bit operand widens to the pair by its sign: the low word itself and the "
      + "high word its arithmetic shift by 63")
  void widenedIsTheSignExtension() {
    DeclineSink sink = sink();
    VarkaVectorIR x = new ColumnRef(0, LaneType.LONG);
    Pair pair = VarkaDecimalCompiler.widened(x, D38, sink);
    assertEquals(x, pair.lo());
    BitOp hi = (BitOp) pair.hi();
    assertEquals(BitOpKind.SHR, hi.op());
    assertEquals(x, hi.left());
    assertEquals(63L, sink.longLiteralValues().get(((LiteralSlot) hi.right()).index()));
    // The extension is the value: -1 >> 63 is -1, so (lo, hi) = (-1, -1) is -1 as a pair.
    assertEquals(BigInteger.valueOf(-1), new DecimalWords(-1L, -1L >> 63).value());
    assertEquals(BigInteger.valueOf(Long.MAX_VALUE),
        new DecimalWords(Long.MAX_VALUE, Long.MAX_VALUE >> 63).value());
  }

  @Test
  @DisplayName("a pair's words are long-lane nodes")
  void pairWordsAreLong() {
    assertThrows(IllegalArgumentException.class,
        () -> new Pair(new ColumnRef(0), new ColumnRef(1), D38));
  }

  @Test
  @DisplayName("the word pair of 10^p - 1 is the bound at every pair precision, and its "
      + "negation the lower bound")
  void boundWords() {
    for (int p = 19; p <= 38; p++) {
      BigInteger b = BigInteger.TEN.pow(p).subtract(BigInteger.ONE);
      assertEquals(b, DecimalWords.bound(p).value());
      assertEquals(b.negate(), DecimalWords.bound(p).negate().value());
    }
    assertThrows(IllegalArgumentException.class, () -> DecimalWords.bound(18));
    assertThrows(IllegalArgumentException.class, () -> DecimalWords.bound(39));
  }
}
