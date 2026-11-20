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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.DateTimeException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.TimeBucket;
import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaDecline;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelChecks.LongBatch;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelChecks.SparkAnswers;
import org.apache.spark.sql.catalyst.util.DateTimeConstants;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.YearMonthIntervalType;

/**
 * {@code time_bucket} over a {@code TIMESTAMP_NTZ} value by a day-time width (VARKA-339,
 * `VARKA-339.md` 2.5 and 3.6). Spark buckets an NTZ value at UTC, so the start of its bucket is
 * {@code o + floorDiv(t - o, b) * b}, raising where the difference, the product or the sum leaves
 * a long and, for a width of a day or more, where the next bucket's start does. The kernel takes
 * the remainder {@code floorMod(t - o, b)} in one of three forms: through the time of day for a
 * width that divides a day, through the day for a width of whole days, and by one division under
 * a window of differences for any other width.
 *
 * <p>The first test holds the three forms, in scalar form, to Spark's
 * {@code timeBucketDTInterval} for every one of the 504 divisors of a day, five whole-day widths
 * and five windowed ones, at five origins, over the long range's ends, the ends of the buckets
 * around them and a seeded draw: equal where both answer, and declining exactly where Spark
 * raises, the window apart. The second runs the emitted kernels over the same values, for a
 * sample of the widths; the third pins what declines.
 */
@VarkaSuite
public final class VarkaTimestampNtzBucketSuite {

  private static final long DAY = DateTimeConstants.MICROS_PER_DAY;

  /**
   * The widest width the arm for any other width takes, {@code VarkaTimeCompiler}'s
   * {@code BUCKET_WINDOW_MAX_WIDTH}; the declines test holds the compiler to it.
   */
  private static final long WIDEST_WINDOWED = 1L << 51;

  /** The window the arm for any other width guards the difference to, per side. */
  private static long window(long b) {
    return VarkaVectorIR.ConstDivide.EXACT_DIVIDEND_BOUND - b;
  }

  /** The divisors of a day's microseconds, {@code 2^13 * 3^3 * 5^8}, in increasing order. */
  private static long[] dayDivisors() {
    var out = new ArrayList<Long>();
    for (long p2 = 1; DAY % p2 == 0; p2 *= 2) {
      for (long p3 = 1; DAY % (p2 * p3) == 0; p3 *= 3) {
        for (long p5 = 1; DAY % (p2 * p3 * p5) == 0; p5 *= 5) {
          out.add(p2 * p3 * p5);
        }
      }
    }
    return out.stream().mapToLong(Long::longValue).sorted().toArray();
  }

  /** The widths of whole days, from one to the widest whose start can be a long. */
  private static final long[] WHOLE_DAYS = {DAY, 7 * DAY, 30 * DAY, 365 * DAY,
      106_751_991L * DAY};

  /** Widths that neither divide a day nor are whole days: seven hours, 36 hours, odd ones. */
  private static final long[] WINDOWED = {7 * DateTimeConstants.MICROS_PER_HOUR,
      36 * DateTimeConstants.MICROS_PER_HOUR, 1_000_003L, DAY + 1,
      WIDEST_WINDOWED};

  /** The origins: the epoch, a held NTZ value, one before the epoch, and the long's ends. */
  private static final long[] ORIGINS = {0, 1_622_548_800_000_000L, -1, Long.MIN_VALUE + 5,
      Long.MAX_VALUE - 5};

  /** Spark's start of the bucket, or empty where it raises. */
  private static Optional<Long> spark(long b, long t, long o) {
    try {
      return Optional.of(DateTimeUtils.timeBucketDTInterval(b, t, o, ZoneOffset.UTC));
    } catch (ArithmeticException | DateTimeException e) {
      return Optional.empty();
    }
  }

  /**
   * The kernel's start in scalar form, each step as {@code VarkaTimeCompiler.bucketStart} builds
   * it in the long lane's wrapping arithmetic, or empty where a checked step or a guard declines.
   */
  static Optional<Long> kernel(long b, long t, long o) {
    long diff;
    try {
      diff = Math.subtractExact(t, o);
    } catch (ArithmeticException e) {
      return Optional.empty();
    }
    long r;
    long r1 = diff - Math.floorDiv(diff, DAY) * DAY;
    if (b == 1) {
      r = 0;
    } else if (b < DAY && DAY % b == 0) {
      r = r1 - r1 / b * b;
    } else if (b % DAY == 0) {
      long n = b / DAY;
      long q1 = Math.floorDiv(diff, DAY);
      long k = (q1 + ((q1 >> 63) & (1 - n))) / n;
      r = n == 1 ? r1 : diff - k * b;
    } else {
      if (diff < -window(b) || diff > window(b)) {
        return Optional.empty();
      }
      long k = (diff + ((diff >> 63) & (1 - b))) / b;
      r = diff - k * b;
    }
    long kb;
    long start;
    try {
      kb = Math.subtractExact(diff, r);
      start = Math.addExact(o, kb);
    } catch (ArithmeticException e) {
      return Optional.empty();
    }
    if (b >= DAY && (kb > Long.MAX_VALUE - b || start > Long.MAX_VALUE - b)) {
      return Optional.empty();
    }
    return Optional.of(start);
  }

  /** Whether the kernel declines at {@code t} by the window alone, where Spark answers. */
  private static boolean outsideWindow(long b, long t, long o) {
    if (DAY % b == 0 || b % DAY == 0) {
      return false;
    }
    try {
      long diff = Math.subtractExact(t, o);
      return diff < -window(b) || diff > window(b);
    } catch (ArithmeticException e) {
      return false;
    }
  }

  /**
   * The counts a width is checked at: the long range's ends and their neighbours, the ends of
   * the buckets at the origin, at the long's ends and at the window's ends, and a seeded draw
   * over the whole range, the window and a thousand days of the origin.
   */
  private static long[] values(long b, long o, int draws) {
    var out = new ArrayList<Long>();
    for (long v : new long[] {Long.MIN_VALUE, Long.MIN_VALUE + 1, -DAY, -1, 0, 1, DAY,
        Long.MAX_VALUE - 1, Long.MAX_VALUE}) {
      out.add(v);
    }
    // The buckets' ends: the first and last count of a bucket, and one past each.
    for (long anchor : new long[] {o, Long.MIN_VALUE, Long.MAX_VALUE, o - window(b),
        o + window(b), Long.MAX_VALUE - b, Long.MIN_VALUE + b}) {
      Optional<Long> start = spark(b, anchor, o);
      if (start.isPresent()) {
        long s = start.get();
        for (long v : new long[] {s - 1, s, s + 1, s + b - 1, s + b, s - b}) {
          out.add(v);
        }
      }
      out.add(anchor);
    }
    var random = new SplittableRandom(20261120L ^ b ^ (o * 31));
    for (int i = 0; i < draws; i++) {
      out.add(random.nextLong());
      out.add(o + random.nextLong(-window(1), window(1)));
      out.add(o + random.nextLong(-1000 * DAY, 1000 * DAY));
    }
    return out.stream().mapToLong(Long::longValue).toArray();
  }

  private static long[] widths() {
    return LongStream.concat(LongStream.of(dayDivisors()),
        LongStream.concat(LongStream.of(WHOLE_DAYS), LongStream.of(WINDOWED))).toArray();
  }

  @Test
  @DisplayName("the bucket's start in its three forms is Spark's timeBucketDTInterval for every "
      + "divisor of a day, whole days and other widths, declining exactly where Spark raises "
      + "and, for the other widths, outside the window")
  void scalarForms() {
    long[] widths = widths();
    assertEquals(504 + WHOLE_DAYS.length + WINDOWED.length, widths.length);
    long compared = 0;
    long declined = 0;
    for (long b : widths) {
      for (long o : ORIGINS) {
        for (long t : values(b, o, 40)) {
          Optional<Long> spark = spark(b, t, o);
          Optional<Long> ours = kernel(b, t, o);
          String at = "time_bucket(" + b + ", " + t + ", " + o + ")";
          if (ours.isPresent()) {
            assertEquals(spark, ours, at);
            compared++;
          } else {
            assertTrue(spark.isEmpty() || outsideWindow(b, t, o),
                at + " declines where Spark answers " + spark);
            declined++;
          }
        }
      }
    }
    assertTrue(compared > 100_000 && declined > 1000,
        "compared " + compared + ", declined " + declined);
  }

  private static final AtomicInteger CLASS_COUNTER = new AtomicInteger();

  private final Attribute ntz = VarkaSqlResolve.column("ntz", DataTypes.TimestampNTZType);

  private scala.collection.immutable.Seq<Attribute> columns() {
    return CollectionConverters.asScala(List.of(ntz)).toList();
  }

  private Expression bucket(long b, long o) {
    return new TimeBucket(new Literal(b, DayTimeIntervalType.apply()), ntz,
        new Literal(o, DataTypes.TimestampNTZType), Option.empty());
  }

  private Optional<CompiledVarkaProjection> compile(Expression e) {
    return VarkaExpressionCompiler.compile(
        CollectionConverters.asScala(List.of(VarkaSqlResolve.named(e, "c"))).toList(), columns(),
        VarkaMatrixRuns.BASE);
  }

  private String declineOf(Expression e) {
    var entry = CollectionConverters.asScala(List.of(VarkaSqlResolve.named(e, "c"))).toList();
    return VarkaExpressionCompiler.declines(entry, columns(), VarkaMatrixRuns.BASE).values()
        .stream().findFirst().map(VarkaDecline::reason).orElse("nothing declined");
  }

  /** Whether the emitted kernel of {@code plan} answers {@code values} as Spark does. */
  private static boolean answers(CompiledVarkaProjection plan, String className, byte[] bytes,
      long b, long o, long[] values) {
    int n = values.length * 2;
    var data = new long[1][n];
    var nulls = new boolean[1][n];
    for (int i = 0; i < n; i++) {
      data[0][i] = values[i / 2];
      nulls[0][i] = i % 2 == 1;
    }
    String sql = "time_bucket(" + b + ", ntz, " + o + ")";
    SparkAnswers oracle = new SparkAnswers() {
      @Override
      public VarkaKernelChecks.Answer answer(int output, long[] row, boolean[] isNull) {
        if (isNull[0]) {
          return new VarkaKernelChecks.Answer.Null();
        }
        return new VarkaKernelChecks.Answer.Value(spark(b, row[0], o).orElseThrow());
      }

      @Override
      public String sql(int output) {
        return sql;
      }
    };
    return VarkaKernelChecks.runAndCompareLong(className + " " + sql, className, bytes,
        plan.outputs(), plan.inputOrdinals().size(),
        plan.longLiterals().stream().mapToLong(Long::longValue).toArray(),
        plan.literals().stream().mapToInt(Integer::intValue).toArray(),
        new LongBatch(n, nulls, data, false), true, oracle);
  }

  @Test
  @DisplayName("the emitted bucket answers every count Spark answers inside the window, and "
      + "declines a batch holding one Spark raises on or one outside the window")
  void emittedForms() {
    long[] divisors = dayDivisors();
    var sample = new ArrayList<Long>();
    for (int i = 0; i < divisors.length; i += 23) {
      sample.add(divisors[i]);
    }
    sample.addAll(List.of(divisors[divisors.length - 1], 900_000_000L,
        DateTimeConstants.MICROS_PER_HOUR));
    LongStream.of(WHOLE_DAYS).forEach(sample::add);
    LongStream.of(WINDOWED).forEach(sample::add);
    int kernels = 0;
    for (long b : sample) {
      for (long o : ORIGINS) {
        Expression e = bucket(b, o);
        CompiledVarkaProjection plan = compile(e).orElseThrow(
            () -> new AssertionError(e + " declines: " + declineOf(e)));
        String className = "org.apache.spark.sql.varka.execution.VarkaNtzBucket"
            + CLASS_COUNTER.incrementAndGet();
        byte[] bytes = VarkaLoopEmitter.emit(className, plan.outputs(),
            plan.inputOrdinals().size(), plan.numLiterals(), null, null, VarkaMatrixRuns.BASE);
        long[] values = values(b, o, 8);
        long[] answered = LongStream.of(values)
            .filter(t -> spark(b, t, o).isPresent() && !outsideWindow(b, t, o)).toArray();
        assertTrue(answers(plan, className, bytes, b, o, answered),
            "time_bucket(" + b + ", ntz, " + o + ") declined a batch Spark answers");
        for (long t : values) {
          if (spark(b, t, o).isEmpty() || outsideWindow(b, t, o)) {
            assertFalse(answers(plan, className, bytes, b, o, new long[] {0, t, 1}),
                "time_bucket(" + b + ", ntz, " + o + ") answered a batch holding " + t);
          }
        }
        kernels++;
      }
    }
    assertEquals(sample.size() * ORIGINS.length, kernels);
  }

  @Test
  @DisplayName("time_bucket declines a year-month width, a null width or origin, a windowed width "
      + "past 2^51 and a zoned TIMESTAMP, and fuses the windowed arm's widest width")
  void declines() {
    Expression ym = new TimeBucket(new Literal(1, YearMonthIntervalType.apply()), ntz,
        new Literal(0L, DataTypes.TimestampNTZType), Option.empty());
    assertTrue(compile(ym).isEmpty(), "a year-month width fused");
    assertTrue(declineOf(ym).contains("m9/SCOPE.md item 117"), declineOf(ym));
    Expression nullWidth = new TimeBucket(new Literal(null, DayTimeIntervalType.apply()), ntz,
        new Literal(0L, DataTypes.TimestampNTZType), Option.empty());
    assertTrue(compile(nullWidth).isEmpty(), "a null width fused");
    Expression nullOrigin = new TimeBucket(new Literal(DAY, DayTimeIntervalType.apply()), ntz,
        new Literal(null, DataTypes.TimestampNTZType), Option.empty());
    assertTrue(compile(nullOrigin).isEmpty(), "a null origin fused");
    assertTrue(declineOf(nullOrigin).contains("folds to null"), declineOf(nullOrigin));
    long widest = WIDEST_WINDOWED;
    assertTrue(compile(bucket(widest, 0)).isPresent(), "the widest windowed width declined");
    Expression past = bucket(widest + 1, 0);
    assertTrue(compile(past).isEmpty(), "a windowed width past 2^51 fused");
    assertTrue(declineOf(past).contains("past 2^51"), declineOf(past));
    // A width of whole days past 2^51 takes the day's arm, which needs no window.
    assertTrue(compile(bucket(106_751_991L * DAY, 0)).isPresent());
    Attribute ltz = VarkaSqlResolve.column("ltz", DataTypes.TimestampType);
    Expression zoned = new TimeBucket(new Literal(DAY, DayTimeIntervalType.apply()), ltz,
        new Literal(0L, DataTypes.TimestampType), Option.apply("UTC"));
    var cols = CollectionConverters.asScala(List.of(ltz)).toList();
    assertTrue(VarkaExpressionCompiler.compile(
        CollectionConverters.asScala(List.of(VarkaSqlResolve.named(zoned, "c"))).toList(), cols,
        VarkaMatrixRuns.BASE).isEmpty(), "time_bucket over a zoned TIMESTAMP fused");
  }
}
