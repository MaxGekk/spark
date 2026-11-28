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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;

/**
 * {@code RoundToScale} with a {@code DOUBLE} output (VARKA-346, part 346b): {@code round},
 * {@code bround} and {@code truncate} over a double at every scale from 0 to 22, built as part
 * 346b1b's compiler arm builds it, from Spark's own expression, and held bit for bit to its
 * {@code eval}, which rounds the double's shortest decimal. The cells of {@code VARKA-346.md}
 * 3.1, and below each scale's {@code L_s} its powers of two, its decimals of {@code s + 1} digits
 * near a tie or a multiple and a seeded draw, answer as Spark does; a batch with a non-integral
 * band value declines with the band's bit; a band value on a null row declines nothing, and a
 * null row stays null.
 */
@VarkaSuite(ownJvm = true)
public final class VarkaEmitterScaleRoundingSuite {

  private static final String[] FUNCTIONS = {"round", "bround", "truncate"};

  private static final long[] NANS = {0x7ff8000000000000L, 0xfff8000000000000L,
      0x7ff8000000000001L, 0xfff0000000000001L, 0x7ff4000000000000L, 0xffffffffffffffffL};

  /** A compiled {@code fn(x, scale)}, its kernel and Spark's bound expression. */
  private record Case(String sql, VarkaEmitterFixtures.Loaded loaded, Expression spark) {

    static Case of(String fn, int scale) {
      var x = VarkaSqlResolve.column("x", DataTypes.DoubleType);
      var cols = CollectionConverters.asScala(List.<Attribute>of(x)).toSeq();
      String sql = fn + "(x, " + scale + ")";
      Expression e = VarkaSqlResolve.parse(sql, cols);
      // Built by hand: no compiler arm admits the node before part 346b1b.
      var node = new VarkaVectorIR.RoundToScale(modeOf(fn), scale,
          new VarkaVectorIR.ColumnRef(0, VarkaVectorIR.LaneType.DOUBLE),
          VarkaVectorIR.ScaleOut.DOUBLE);
      var emitted = VarkaEmitterFixtures.emit(List.of(node), 1, 0, VarkaMatrixRuns.BASE);
      return new Case(sql, VarkaEmitterFixtures.load(emitted), VarkaSqlResolve.bind(e, cols));
    }

    /** Spark's function to the node's mode: VarkaRoundingCompiler.toScale's map (346b1b). */
    private static VarkaVectorIR.ScaleMode modeOf(String fn) {
      return switch (fn) {
        case "round" -> VarkaVectorIR.ScaleMode.HALF_UP;
        case "bround" -> VarkaVectorIR.ScaleMode.HALF_EVEN;
        case "truncate" -> VarkaVectorIR.ScaleMode.DOWN;
        default -> throw new IllegalArgumentException("no rounding function " + fn);
      };
    }

    /** Spark's answer over {@code d}, as its bits. */
    long spark(double d) {
      var row = new GenericInternalRow(new Object[] {d});
      return Double.doubleToRawLongBits((Double) spark.eval(row));
    }

    /** Runs the kernel over {@code values}, every row valid; the status, the outputs in out. */
    int run(double[] values, long[] out) {
      int n = values.length;
      try (Arena arena = Arena.ofConfined()) {
        var col = VarkaEmitterFixtures.makeLongInput(arena, n, i -> false,
            i -> Double.doubleToRawLongBits(values[i]), true);
        var o = VarkaEmitterFixtures.makeLongOutput(arena, n);
        int status = loaded.kernel().run(new long[] {col.data().address()},
            new long[] {col.validityAddress(n)}, new int[] {col.nullCount()},
            new long[] {o.data().address()}, new long[] {o.validity().address()}, new int[0],
            new long[0], n, VarkaEmitterTestSupport.scratch(loaded.kernel(), n));
        for (int i = 0; i < n; i++) {
          out[i] = o.data().get(ValueLayout.JAVA_LONG, i * 8L);
        }
        return status;
      }
    }

    void release() {
      loaded.loader().release();
    }
  }

  /** The cells of 3.1 that every scale is fed, beside each scale's own values. */
  private static double[] cells() {
    return new double[] {9.95, 3527.61953125, 10.3, 2.675, 1.005, 0.125, -0.0, 0.0, -0.004,
        -0.5, 0.7, 2.5, -2.5, 0.5, 1.5, -1.5, 0.49999999999999994, 1.0, 123456.789,
        Double.MIN_VALUE, 2 * Double.MIN_VALUE, 3 * Double.MIN_VALUE, Double.MIN_NORMAL,
        Double.MAX_VALUE, -Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
        6.1317116247284E18, 1.0E15, 0x1.0p52, Math.nextDown(0x1.0p52), 0x1.0p53};
  }

  /**
   * The values at {@code scale} outside the band: the cells, then below {@code L_s} each power of
   * two with its neighbours, the tie and multiple decimals of {@code s + 1} digits with three
   * doubles either side, a seeded draw, an integral value inside the band and values from
   * {@code K_s} up, each in both signs, and the NaNs.
   */
  private static double[] values(int scale) {
    var out = new ArrayList<Double>();
    double lower = VarkaScaleRoundingLowering.lowerBound(scale);
    double keep = VarkaScaleRoundingLowering.keepBound(scale);
    for (double c : cells()) {
      out.add(c);
    }
    for (int e = -1074; Math.scalb(1.0, e) < lower; e++) {
      double p = Math.scalb(1.0, e);
      for (double v : new double[] {p, Math.nextUp(p), Math.nextDown(p), 1.5 * p}) {
        out.add(v);
      }
    }
    var rnd = new SplittableRandom(346L + scale);
    BigDecimal step = BigDecimal.ONE.movePointLeft(scale + 1);
    for (int i = 0; i < 600; i++) {
      // A decimal of s + 1 fractional digits below L_s: a tie when its last digit is 5.
      double t = lower * rnd.nextDouble();
      BigDecimal dec = new BigDecimal(t).divideToIntegralValue(step).multiply(step)
          .add(i % 2 == 0 ? step.multiply(BigDecimal.valueOf(5)).movePointLeft(1)
              : BigDecimal.ZERO);
      double d = dec.doubleValue();
      double up = d;
      double down = d;
      for (int k = 0; k < 4; k++) {
        out.add(up);
        out.add(down);
        up = Math.nextUp(up);
        down = Math.nextDown(down);
      }
      out.add(Math.scalb(1.0 + rnd.nextDouble(), -rnd.nextInt(60)) * lower * 0.5);
    }
    if (VarkaScaleRoundingLowering.hasBand(scale) && Math.floor(keep * 0.5) >= lower) {
      out.add(Math.floor(keep * 0.5));
      out.add(Math.floor(Math.sqrt(lower * keep)));
    }
    for (double v : new double[] {keep, Math.nextUp(keep), keep * 7.5, 1.0E300}) {
      out.add(v);
    }
    var all = new ArrayList<Double>();
    for (double v : out) {
      double a = Math.abs(v);
      if (!VarkaScaleRoundingLowering.inBand(scale, a)) {
        all.add(v);
        all.add(-v);
      }
    }
    for (long nan : NANS) {
      all.add(Double.longBitsToDouble(nan));
    }
    return all.stream().mapToDouble(Double::doubleValue).toArray();
  }

  /** Non-integral values of the band at {@code scale}: its ends' neighbours and inner points. */
  private static double[] band(int scale) {
    double lower = VarkaScaleRoundingLowering.lowerBound(scale);
    double keep = VarkaScaleRoundingLowering.keepBound(scale);
    var out = new ArrayList<Double>();
    for (double v : new double[] {lower, Math.nextUp(lower), lower * 1.5 + 0.25,
        Math.nextDown(keep), Math.sqrt(lower * keep) + 0.125, keep * 0.5 + 0.5}) {
      if (VarkaScaleRoundingLowering.inBand(scale, v)) {
        out.add(v);
        out.add(-v);
      }
    }
    return out.stream().mapToDouble(Double::doubleValue).toArray();
  }

  @Test
  @DisplayName("round, bround and truncate of a double are Spark's at every scale, below the "
      + "band and from its keep bound up")
  void matchesSparkOutsideTheBand() {
    for (int scale = 0; scale <= VarkaScaleRoundingLowering.MAX_SCALE; scale++) {
      double[] values = values(scale);
      for (String fn : FUNCTIONS) {
        Case c = Case.of(fn, scale);
        try {
          long[] got = new long[values.length];
          assertEquals(0, c.run(values, got), c.sql() + " declined a batch outside its band");
          for (int i = 0; i < values.length; i++) {
            long want = c.spark(values[i]);
            assertEquals(Long.toHexString(want), Long.toHexString(got[i]),
                c.sql() + " over " + values[i] + " (0x"
                    + Long.toHexString(Double.doubleToRawLongBits(values[i])) + ")");
          }
        } finally {
          c.release();
        }
      }
    }
  }

  @Test
  @DisplayName("a batch holding a non-integral value of the band declines with the band's bit")
  void declinesTheBand() {
    for (int scale = 1; scale <= VarkaScaleRoundingLowering.MAX_SCALE; scale++) {
      double[] band = band(scale);
      assertTrue(band.length > 0, "scale " + scale + " has a band to test");
      for (String fn : FUNCTIONS) {
        Case c = Case.of(fn, scale);
        try {
          for (double v : band) {
            double[] batch = {1.0, v, 0.5, -0.0};
            int status = c.run(batch, new long[batch.length]);
            assertEquals(VarkaFusedKernel.STATUS_ROUNDING_BAND, status,
                c.sql() + " over the band's " + v);
          }
        } finally {
          c.release();
        }
      }
    }
  }

  @Test
  @DisplayName("a null row's band value declines nothing, null rows stay null and the rest are "
      + "Spark's")
  void nullRowsNeitherDeclineNorAnswer() {
    double[] band = band(2);
    double[] values = values(2);
    int n = values.length + band.length;
    double[] batch = new double[n];
    System.arraycopy(values, 0, batch, 0, values.length);
    System.arraycopy(band, 0, batch, values.length, band.length);
    // Every band value is on a null row, and so is every seventh of the others.
    java.util.function.IntPredicate isNull = i -> i >= values.length || i % 7 == 3;
    for (String fn : FUNCTIONS) {
      Case c = Case.of(fn, 2);
      try (Arena arena = Arena.ofConfined()) {
        var col = VarkaEmitterFixtures.makeLongInput(arena, n, isNull,
            i -> Double.doubleToRawLongBits(batch[i]), true);
        var o = VarkaEmitterFixtures.makeLongOutput(arena, n);
        int status = c.loaded().kernel().run(new long[] {col.data().address()},
            new long[] {col.validityAddress(n)}, new int[] {col.nullCount()},
            new long[] {o.data().address()}, new long[] {o.validity().address()}, new int[0],
            new long[0], n, VarkaEmitterTestSupport.scratch(c.loaded().kernel(), n));
        assertEquals(0, status, c.sql() + " declined over a band value on a null row");
        for (int i = 0; i < n; i++) {
          boolean valid = (o.validity().get(ValueLayout.JAVA_BYTE, i / 8L) & (1 << (i % 8))) != 0;
          assertEquals(!isNull.test(i), valid, c.sql() + ": the validity of row " + i);
          if (valid) {
            assertEquals(Long.toHexString(c.spark(batch[i])),
                Long.toHexString(o.data().get(ValueLayout.JAVA_LONG, i * 8L)),
                c.sql() + " over " + batch[i]);
          }
        }
      } finally {
        c.release();
      }
    }
  }
}
