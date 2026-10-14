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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.IsoFields;
import java.util.OptionalInt;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaRangeAnalysis.Shift;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.DateTimeUtils$;

/**
 * The calendar rules of {@code VarkaRangeAnalysis} - how far a calendar function moves a day, and
 * the range of each field - over every epoch day the compiler admits a calendar node for,
 * {@code [NARROW_MIN_DAYS, NARROW_DECOMPOSE_MAX_DAYS]}, computed as the reference evaluator
 * computes them, by {@code java.time} and {@code DateTimeUtils} (VARKA-281).
 *
 * <p>This is the leg independent of {@code range_calendar.smt2}, which proves the same rules over
 * a transcription of {@code java.time}'s closed forms: the sweep reads no closed form, so a rule
 * narrowed by one fails here in the JVM whatever a solver says of the transcription, as the
 * division sweeps stand beside the division proofs. Every bound is the analysis's own named
 * constant, so a value proven and swept is the value applied.
 */
@VarkaSuite
public final class VarkaRangeRulesSuite {

  /** The days the compiler admits a calendar node for. */
  private static final int FIRST = VarkaChrono.NARROW_MIN_DAYS;
  private static final int LAST = VarkaChrono.NARROW_DECOMPOSE_MAX_DAYS;

  private static final int WEEK = DateTimeUtils$.MODULE$.TRUNC_TO_WEEK();
  private static final int MONTH = DateTimeUtils$.MODULE$.TRUNC_TO_MONTH();
  private static final int QUARTER = DateTimeUtils$.MODULE$.TRUNC_TO_QUARTER();
  private static final int YEAR = DateTimeUtils$.MODULE$.TRUNC_TO_YEAR();

  private static boolean within(long v, Shift s) {
    return s.lo() <= v && v <= s.hi();
  }

  private static boolean within(long v, long lo, long hi) {
    return lo <= v && v <= hi;
  }

  /** Every admitted day holds {@code rule}, or the first that does not is named. */
  private static void sweep(String what, IntPredicate rule) {
    OptionalInt failing =
        IntStream.rangeClosed(FIRST, LAST).parallel().filter(d -> !rule.test(d)).findFirst();
    assertTrue(failing.isEmpty(), () -> what + " fails at epoch day " + failing.getAsInt()
        + ", " + LocalDate.ofEpochDay(failing.getAsInt()));
  }

  @Test
  @DisplayName("last_day moves every admitted day by LAST_DAY_SHIFT")
  void lastDay() {
    sweep("last_day", d -> within(
        DateTimeUtils.getLastDayOfMonth(d) - (long) d, VarkaRangeAnalysis.LAST_DAY_SHIFT));
  }

  @Test
  @DisplayName("trunc at each of its four levels moves every admitted day by TRUNC_SHIFT")
  void trunc() {
    for (int level : new int[] {WEEK, MONTH, QUARTER, YEAR}) {
      sweep("trunc at level " + level, d -> within(
          DateTimeUtils.truncDate(d, level) - (long) d, VarkaRangeAnalysis.TRUNC_SHIFT));
    }
  }

  @Test
  @DisplayName("next_day for each weekday moves every admitted day by NEXT_DAY_SHIFT")
  void nextDay() {
    sweep("next_day", d -> {
      for (int dayOfWeek = 0; dayOfWeek < 7; dayOfWeek++) {
        long moved = DateTimeUtils.getNextDateForDayOfWeek(d, dayOfWeek) - (long) d;
        if (!within(moved, VarkaRangeAnalysis.NEXT_DAY_SHIFT)) {
          return false;
        }
      }
      return true;
    });
  }

  @Test
  @DisplayName("the Thursday of every admitted day's week lies within THURSDAY_SHIFT of it")
  void thursday() {
    sweep("the Thursday", d -> within(
        LocalDate.ofEpochDay(d).with(DayOfWeek.THURSDAY).toEpochDay() - d,
        VarkaRangeAnalysis.THURSDAY_SHIFT));
  }

  @Test
  @DisplayName("add_months by counts at the guard's ends and around zero moves every admitted "
      + "day by MONTH_DAYS_MIN to MONTH_DAYS_MAX days a month")
  void addMonths() {
    int min = VarkaChrono.MONTH_ARITH_MIN_MONTHS;
    int max = VarkaChrono.MONTH_ARITH_MAX_MONTHS;
    for (int months : new int[] {min, min + 1, -13, -12, -1, 0, 1, 11, 12, 13, max - 1, max}) {
      long a = (long) VarkaRangeAnalysis.MONTH_DAYS_MIN * months;
      long b = (long) VarkaRangeAnalysis.MONTH_DAYS_MAX * months;
      sweep("add_months by " + months, d -> within(
          DateTimeUtils.dateAddMonths(d, months) - (long) d, Math.min(a, b), Math.max(a, b)));
    }
  }

  @Test
  @DisplayName("the year of every admitted day has magnitude at most YEAR_FIELD_MAGNITUDE")
  void year() {
    int magnitude = VarkaChrono.YEAR_FIELD_MAGNITUDE;
    sweep("year", d -> within(LocalDate.ofEpochDay(d).getYear(), -magnitude, magnitude));
  }

  @Test
  @DisplayName("month, day, quarter, day_of_year and weekofyear of every admitted day lie "
      + "under their magnitudes")
  void fields() {
    sweep("the fields", d -> {
      LocalDate date = LocalDate.ofEpochDay(d);
      return within(date.getMonthValue(), 1, VarkaRangeAnalysis.MONTH_MAGNITUDE)
          && within(date.getDayOfMonth(), 1, VarkaRangeAnalysis.DAY_OF_MONTH_MAGNITUDE)
          && within(date.get(IsoFields.QUARTER_OF_YEAR), 1, VarkaRangeAnalysis.QUARTER_MAGNITUDE)
          && within(date.getDayOfYear(), 1, VarkaRangeAnalysis.DAY_OF_YEAR_MAGNITUDE)
          && within(date.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR), 1,
              VarkaRangeAnalysis.WEEK_OF_YEAR_MAGNITUDE);
    });
  }

  @Test
  @DisplayName("dayofweek, the ISO weekday and weekday of every admitted day lie under their "
      + "magnitudes")
  void weekdays() {
    // Spark's own definitions, through java.time: dayofweek is Sunday 1 to Saturday 7, the ISO
    // weekday Monday 1 to Sunday 7, and weekday Monday 0 to Sunday 6 - not the lanes' floorMod
    // forms, which range_calendar.smt2 proves and which would hold by construction here.
    sweep("the weekdays", d -> within(DateTimeUtils.getDayOfWeek(d), 1,
            VarkaRangeAnalysis.DAY_OF_WEEK_MAGNITUDE)
        && within(DateTimeUtils.getWeekDay(d) + 1, 1, VarkaRangeAnalysis.DAY_OF_WEEK_MAGNITUDE)
        && within(DateTimeUtils.getWeekDay(d), 0, VarkaRangeAnalysis.WEEK_DAY_MAGNITUDE));
  }

  @Test
  @DisplayName("IntArith SUB over asymmetric operands answers the differences, not the sums")
  void subtractionOverAsymmetricOperands() {
    // Found by VARKA-281's review: `arith` answered the sum's interval for SUB, which is the
    // difference's only over symmetric operands. Over two BoundedDivide answers, [0, 52] and
    // [0, 3], the sum's [0, 55] misses day_of_year 1 of a December: 0 - 3.
    var c0 = new VarkaVectorIR.ColumnRef(0);
    var dayOfYearWeeks = VarkaVectorIR.BoundedDivide.of(new VarkaVectorIR.DayOfYear(c0), 7, 367);
    var monthQuarters = VarkaVectorIR.BoundedDivide.of(new VarkaVectorIR.Month(c0), 4, 13);
    var difference = new VarkaVectorIR.IntArith(VarkaVectorIR.IntOp.SUB,
        VarkaVectorIR.Overflow.WRAP, dayOfYearWeeks, monthQuarters);
    VarkaValueRange.Range answer = VarkaRangeAnalysis.range(difference,
        VarkaRangeAnalysis.Kind.INT, VarkaRangeAnalysis.GuardPolicy.NONE, i -> {
          throw new IllegalStateException("no literal slots");
        });
    assertTrue(answer.equals(VarkaValueRange.of(-3, 52)), () -> "SUB answered " + answer);
    assertTrue(answer.within(-3, 52) && !answer.within(0, 55));
  }
}
