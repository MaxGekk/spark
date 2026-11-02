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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Set;
import java.util.stream.LongStream;

import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.expressions.BinaryComparison;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

/**
 * `DECIMAL` above 18 digits held to the conformance contract (`m8/VARKA-359.md` 11.8 point 6),
 * at the type registry's {@code DECIMAL(30, 4)}: two columns of 128-bit lane pairs, their
 * sixteen-byte slots written word by word ({@link #decimalSlots}), so that both words of a value
 * carry bits and both signs occur.
 *
 * <p>The values are the precision's bounds and their neighbours, a low word with its sign bit set
 * under a high word of 0 ({@code 2^63}) and its mirror under -1 ({@code -2^63 - 1}), a value
 * whose low word is 0 ({@code -2^64}), one with both words set ({@code 2^64 + 5}), and zero and
 * one each way. The census marks the comparisons fused in part for {@code dec} - at one scale
 * they fuse, across two they wait for rows 360 and 363 - so the load, null and ordering checks
 * hold every comparison, {@code <>}, {@code <=>} and the null tests to the reference and to
 * Spark; the forms the census owes ({@code IN}, the choices and {@code greatest}, owed to 359b;
 * the casts, 364; the arithmetic, 360 to 363) are required to decline, and cancel their checks
 * naming the rows.
 *
 * <p>In its own JVM, as the long-lane suites are ({@link VarkaSpeciesGuard}).
 */
@VarkaSuite(ownJvm = true)
public final class VarkaDecimalConformanceSuite extends VarkaTypeConformance {

  private static final DecimalType TYPE = DataTypes.createDecimalType(30, 4);
  private static final BigInteger BOUND = BigInteger.TEN.pow(30).subtract(BigInteger.ONE);
  private static final BigInteger TWO_63 = BigInteger.ONE.shiftLeft(63);
  private static final BigInteger TWO_64 = BigInteger.ONE.shiftLeft(64);

  /** The unscaled values the fixture's indices name. */
  private static final BigInteger[] VALUES = {
    BOUND.negate(), BOUND.negate().add(BigInteger.ONE), TWO_64.negate(),
    TWO_63.negate().subtract(BigInteger.ONE), BigInteger.ONE.negate(), BigInteger.ZERO,
    BigInteger.ONE, TWO_63, TWO_64.add(BigInteger.valueOf(5)), BOUND.subtract(BigInteger.ONE),
    BOUND};

  /** The indices of {@code 2^64 + 5} and of -1, the forms' literals. */
  private static final long[] PICKS = {8, 4};

  @Override
  protected ConformanceType type() {
    // An index is spelled as its value's exact literal at the scale, which type coercion widens
    // to the column's type at the same scale, so a comparison reads it as a literal pair.
    return new ConformanceType("dec", TYPE, "DECIMAL(30, 4)",
        i -> new BigDecimal(VALUES[(int) i], TYPE.scale()).toPlainString() + "BD",
        LongStream.range(0, VALUES.length).toArray(), PICKS,
        // Null-intolerant and fused over two pair columns; `x + y` is row 360's.
        new PairForm("x < y", "<"));
  }

  @Override
  protected BigInteger[] decimalSlots() {
    return VALUES;
  }

  /** The census entries that mark {@code dec} fused in part: the comparisons. */
  private static final Set<String> COMPARISONS = Set.of("<=>", "=", "==", ">", ">=", "<", "<=");

  /**
   * The band {@code census_curated.txt} states for the comparisons: two decimals of one scale, to
   * 38 digits. A comparison across two scales needs rows 360 and 363's rescale and is owed.
   */
  @Override
  protected boolean inFusedBand(String register, Expression expr) {
    return COMPARISONS.contains(register) && oneScale(expr);
  }

  /** Whether every comparison in {@code e} compares two decimals of one scale. */
  private static boolean oneScale(Expression e) {
    if (e instanceof BinaryComparison c && !(c.left().dataType() instanceof DecimalType l
        && c.right().dataType() instanceof DecimalType r && l.scale() == r.scale())) {
      return false;
    }
    return CollectionConverters.asJava(e.children()).stream()
        .allMatch(VarkaDecimalConformanceSuite::oneScale);
  }
}
