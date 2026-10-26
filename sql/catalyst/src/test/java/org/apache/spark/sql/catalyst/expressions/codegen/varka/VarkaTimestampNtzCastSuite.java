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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaDecline;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelChecks.LongBatch;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelChecks.SparkAnswers;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.GuardedRange;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntArith;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.WidenLane;
import org.apache.spark.sql.catalyst.util.DateTimeConstants;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.SQLConf$;
import org.apache.spark.sql.types.DataTypes;

/**
 * The casts between {@code DATE} and {@code TIMESTAMP_NTZ} at their edges (VARKA-338,
 * `VARKA-338.md` 2.3 and 3.3). {@code CAST(d AS TIMESTAMP_NTZ)} is a guarded wrapping multiply:
 * its lowering is {@code Math.multiplyExact} over every int day, which is what Spark computes in
 * every mode, and a batch with a day past the guard declines in LEGACY, ANSI and TRY alike, so
 * that the row engine raises or nulls on the row. {@code CAST(ntz AS DATE)} is the floor division
 * of {@code long_divide.smt2}, run here at the long range's extremes and at the day's boundaries,
 * against Spark and against {@code java.time}. And a date field of an NTZ value still declines,
 * which row 340 changes.
 */
@VarkaSuite
public final class VarkaTimestampNtzCastSuite {

  private static final long DAY = DateTimeConstants.MICROS_PER_DAY;

  /** The last day whose midnight is a long count of microseconds. */
  private static final long LAST_DAY = Long.MAX_VALUE / DAY;

  private static final AtomicInteger CLASS_COUNTER = new AtomicInteger();

  private final Attribute d = VarkaSqlResolve.column("d", DataTypes.DateType);
  private final Attribute ntz = VarkaSqlResolve.column("ntz", DataTypes.TimestampNTZType);
  private final List<Attribute> columns = List.of(d, ntz);

  /** {@code sql} resolved over the two columns, ANSI on or off. */
  private Expression resolve(String sql, boolean ansi) {
    SQLConf conf = SQLConf.get().clone();
    conf.setConfString(SQLConf.ANSI_ENABLED().key(), Boolean.toString(ansi));
    var cols = CollectionConverters.asScala(columns).toSeq();
    return SQLConf$.MODULE$.withExistingConf(conf, () -> VarkaSqlResolve.parse(sql, cols));
  }

  private Optional<CompiledVarkaProjection> compile(Expression e) {
    return VarkaExpressionCompiler.compile(
        CollectionConverters.asScala(List.of(VarkaSqlResolve.named(e, "c"))).toSeq(),
        CollectionConverters.asScala(columns).toSeq(), VarkaMatrixRuns.BASE);
  }

  /** The three spellings of a cast, one per evaluation mode, resolved. */
  private List<Expression> modes(String operand, String target) {
    return List.of(resolve("CAST(" + operand + " AS " + target + ")", false),
        resolve("CAST(" + operand + " AS " + target + ")", true),
        resolve("try_cast(" + operand + " AS " + target + ")", true));
  }

  // -------------------------------------------------------------------------------------------
  // DATE to TIMESTAMP_NTZ
  // -------------------------------------------------------------------------------------------

  @Test
  @DisplayName("the guard of DATE to NTZ admits exactly the days multiplyExact converts")
  void dateToNtzExhausted() {
    for (Expression cast : modes("d", "TIMESTAMP_NTZ")) {
      CompiledVarkaProjection plan = compile(cast).orElseThrow(() ->
          new AssertionError(cast.sql() + " declines"));
      // The tree the compiler builds, read rather than restated: the multiply, its mode, the
      // guard's bounds and the multiplier from the long literal table.
      if (!(plan.outputs().get(0) instanceof IntArith mul) || mul.op() != IntOp.MUL
          || mul.mode() != Overflow.WRAP || !(mul.left() instanceof GuardedRange guard)
          || !(guard.child() instanceof WidenLane) || !(mul.right() instanceof LiteralSlot slot)) {
        fail(cast.sql() + " compiles to " + plan.outputs().get(0)
            + ", not a wrapping multiply of the guarded day");
        return;
      }
      long lo = guard.lo();
      long hi = guard.hi();
      long factor = plan.longLiterals().get(slot.index());
      assertEquals(DAY, factor, cast.sql());
      // Every day: the guard admits exactly the days whose product does not overflow, by the
      // full 128-bit product's high half, which is the condition multiplyExact throws on.
      var bad = new AtomicLong();
      var first = new AtomicLong();
      int chunks = 256;
      long span = (1L << 32) / chunks;
      IntStream.range(0, chunks).parallel().forEach(c -> {
        long from = Integer.MIN_VALUE + c * span;
        for (long day = from; day < from + span; day++) {
          boolean admitted = lo <= day && day <= hi;
          long product = day * factor;
          boolean overflows = Math.multiplyHigh(day, factor) != (product >> 63);
          if (admitted == overflows && bad.getAndIncrement() == 0) {
            first.set(day);
          }
        }
      });
      assertEquals(0, bad.get(), cast.sql() + ": the guard [" + lo + ", " + hi + "] admits "
          + "a day whose product overflows, or refuses one whose product does not, at "
          + bad.get() + " days, first " + first.get());
      // And multiplyExact itself, which Spark calls, over a million days at each end of the
      // guard and in the middle: the product where the guard admits, a throw where it refuses.
      for (long centre : new long[] {lo, 0, hi}) {
        for (long day = centre - 500_000; day <= centre + 500_000; day++) {
          boolean admitted = lo <= day && day <= hi;
          try {
            long exact = Math.multiplyExact(day, factor);
            assertTrue(admitted && exact == day * factor, cast.sql() + " at day " + day);
          } catch (ArithmeticException e) {
            assertTrue(!admitted, cast.sql() + " admits day " + day + ", which overflows");
          }
        }
      }
    }
  }

  @Test
  @DisplayName("a kernel converts the days at the guard's ends, and declines a batch holding one "
      + "past them, in LEGACY, ANSI and TRY")
  void dateToNtzAtTheEdges() {
    long[] inside = {-LAST_DAY, -LAST_DAY + 1, -1, 0, 1, LAST_DAY - 1, LAST_DAY};
    for (Expression cast : modes("d", "TIMESTAMP_NTZ")) {
      Kernel k = kernel(cast);
      assertTrue(k.answers(inside), cast.sql() + " declined the days inside the guard");
      for (long past : new long[] {-LAST_DAY - 1, LAST_DAY + 1}) {
        long[] values = {0, 1, past, -1};
        assertNotEquals(0, k.status(values), cast.sql() + " answered a batch holding day "
            + past + ", which Spark raises on or nulls");
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // TIMESTAMP_NTZ to DATE
  // -------------------------------------------------------------------------------------------

  @Test
  @DisplayName("NTZ to DATE answers the floor division at the long range's ends and at the day's "
      + "boundaries, as Spark and java.time do")
  void ntzToDateAtTheEdges() {
    var values = new ArrayList<Long>();
    for (long v : new long[] {Long.MIN_VALUE, Long.MAX_VALUE, -1, 0, DAY, -DAY,
        LAST_DAY * DAY, -LAST_DAY * DAY}) {
      for (long delta = -2; delta <= 2; delta++) {
        long w = v + delta;
        // Only the neighbours on the range's own side: past an end is the other end.
        if ((v == Long.MIN_VALUE && delta < 0) || (v == Long.MAX_VALUE && delta > 0)) {
          continue;
        }
        values.add(w);
      }
    }
    long[] all = values.stream().mapToLong(Long::longValue).toArray();
    for (long v : all) {
      long seconds = Math.floorDiv(v, 1_000_000L);
      int nanos = (int) Math.floorMod(v, 1_000_000L) * 1000;
      long epochDay =
          LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC).toLocalDate().toEpochDay();
      assertEquals(Math.floorDiv(v, DAY), epochDay, "java.time and floorDiv at " + v);
    }
    for (Expression cast : modes("ntz", "DATE")) {
      Kernel k = kernel(cast);
      assertTrue(k.answers(all), cast.sql() + " declined a batch; it cannot fail");
      for (long v : all) {
        var spark = k.oracle.answer(0, rowOf(v));
        long seconds = Math.floorDiv(v, 1_000_000L);
        long epochDay = LocalDateTime.ofEpochSecond(seconds,
            (int) Math.floorMod(v, 1_000_000L) * 1000, ZoneOffset.UTC).toLocalDate().toEpochDay();
        assertEquals(new VarkaSparkOracle.Value(epochDay), spark, cast.sql() + " at " + v);
      }
    }
  }

  @Test
  @DisplayName("a date field of an NTZ value declines: the cast to DATE has no day range for the "
      + "calendar, which row 340 gives it")
  void dateFieldDeclines() {
    Expression year = resolve("year(ntz)", true);
    var declines = VarkaExpressionCompiler.declines(
        CollectionConverters.asScala(List.of(VarkaSqlResolve.named(year, "c"))).toSeq(),
        CollectionConverters.asScala(columns).toSeq(), VarkaMatrixRuns.BASE);
    VarkaDecline decline = declines.get(0);
    assertTrue(decline != null, "year(ntz) fuses");
    assertEquals(YEAR_OF_NTZ_DECLINE, decline.reason(), year.sql());
  }

  /** Why {@code year(ntz)} declines today; row 340 makes it fuse. */
  private static final String YEAR_OF_NTZ_DECLINE =
      "day producer the calendar range analysis does not bound";

  // -------------------------------------------------------------------------------------------
  // Running a kernel
  // -------------------------------------------------------------------------------------------

  /** One compiled cast, emitted, with Spark's evaluation of it for the answers. */
  private final class Kernel {
    final Expression expr;
    final CompiledVarkaProjection plan;
    final String className;
    final byte[] bytes;
    final VarkaSparkOracle oracle;

    Kernel(Expression expr, CompiledVarkaProjection plan) {
      this.expr = expr;
      this.plan = plan;
      this.className = "org.apache.spark.sql.varka.execution.VarkaNtzCast"
          + CLASS_COUNTER.incrementAndGet();
      this.bytes = VarkaLoopEmitter.emit(className, plan.outputs(), plan.inputOrdinals().size(),
          plan.numLiterals(), null, null, VarkaMatrixRuns.BASE);
      var input = columns.get(plan.inputOrdinals().get(0));
      this.oracle = new VarkaSparkOracle(CollectionConverters.asScala(List.of(expr)).toSeq(),
          CollectionConverters.asScala(List.of(input)).toSeq());
    }

    private LongBatch batch(long[] values) {
      // Each value once valid and once null, so both bodies and a null lane run.
      int n = values.length * 2;
      var data = new long[1][n];
      var nulls = new boolean[1][n];
      for (int i = 0; i < n; i++) {
        data[0][i] = values[i / 2];
        nulls[0][i] = i % 2 == 1;
      }
      return new LongBatch(n, nulls, data, false);
    }

    private long[] longLits() {
      return plan.longLiterals().stream().mapToLong(Long::longValue).toArray();
    }

    private int[] intLits() {
      return plan.literals().stream().mapToInt(Integer::intValue).toArray();
    }

    /** Whether the kernel answers {@code values}, every row compared with Spark. */
    boolean answers(long[] values) {
      return VarkaKernelChecks.runAndCompareLong(className + " " + expr.sql(), className, bytes,
          plan.outputs(), 1, longLits(), intLits(), batch(values), false, sparkAnswers(oracle));
    }

    /** The kernel's status over {@code values}: non-zero when it declines the batch. */
    int status(long[] values) {
      return VarkaKernelChecks.runAndCompareLongStatus(className + " " + expr.sql(), className,
          bytes, plan.outputs(), 1, longLits(), batch(values), true);
    }
  }

  private Kernel kernel(Expression cast) {
    return new Kernel(cast, compile(cast).orElseThrow(() ->
        new AssertionError(cast.sql() + " declines")));
  }

  private static scala.collection.immutable.Seq<Option<Object>> rowOf(long v) {
    return CollectionConverters.asScala(List.of(Option.apply((Object) v))).toList();
  }

  private static SparkAnswers sparkAnswers(VarkaSparkOracle oracle) {
    return new SparkAnswers() {
      @Override
      public VarkaKernelChecks.Answer answer(int output, long[] values, boolean[] isNull) {
        var row = new ArrayList<Option<Object>>();
        for (int c = 0; c < values.length; c++) {
          row.add(isNull[c] ? Option.empty() : Option.apply((Object) values[c]));
        }
        VarkaSparkOracle.Answer a = oracle.answer(output,
            CollectionConverters.asScala(row).toList());
        if (a instanceof VarkaSparkOracle.Value v) {
          return new VarkaKernelChecks.Answer.Value(v.v());
        } else if (a instanceof VarkaSparkOracle.Raised r) {
          return new VarkaKernelChecks.Answer.Raised(r.e());
        }
        return new VarkaKernelChecks.Answer.Null();
      }

      @Override
      public String sql(int output) {
        return oracle.sql(output);
      }
    };
  }
}
