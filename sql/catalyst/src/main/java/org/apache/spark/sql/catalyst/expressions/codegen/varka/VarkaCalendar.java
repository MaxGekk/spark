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

/**
 * The civil-from-days decomposition {@code year}, {@code month}, {@code dayofmonth} and
 * {@code quarter} are lowered from (task 26), as scalar Java, plus every magic constant the
 * emitter loads. This class is three things at once and is written to be all three:
 *
 * <ol>
 *   <li><b>The derivation record.</b> Each constant is named, and the comment beside it says
 *       which division it stands for and why that division admits the form it does. The whole
 *       argument is in {@code sql/varka/plans/PLAN_TASK_26.md} section 1.</li>
 *   <li><b>The single source of truth for the emitter.</b> {@link VarkaLoopEmitter} loads these
 *       fields rather than repeating their values, so a constant cannot drift between the
 *       emitted bytecode and the model that is swept against {@code java.time}.</li>
 *   <li><b>The test oracle's twin.</b> The methods below are the exact lane arithmetic the
 *       emitter emits, one lane at a time, so a sweep over them is a sweep over the algorithm
 *       and any disagreement with the emitted kernel is an emission bug rather than an
 *       arithmetic one.</li>
 * </ol>
 *
 * <p><b>Why magic multiplies and not division.</b> {@code VectorOperators} has no multiply-high
 * on any lane type, so full-range Granlund-Montgomery division is inexpressible on int lanes;
 * only a range-narrowed magic works, where the value is shrunk until the correctness condition
 * {@code v * e < 2^k} and the no-overflow condition {@code v * M < 2^31} both hold in the low 32
 * bits {@code mul} returns (task 14's follow-up; see the {@code SKILLS.md} entry). Worst-case
 * {@code e ~ d} forces {@code 2^k > d * v}, hence {@code M ~ v}, hence {@code v < 46341}: an
 * <i>exact</i> magic exists on int lanes only for dividends under roughly 46000. The two large
 * divisors here are past that, so they use a round-down magic - which never overestimates the
 * quotient - followed by a bounded number of correction steps, each one compare and two
 * adjustments on a remainder the decomposition wants anyway.
 *
 * <p><b>Two variants, one tail.</b> {@link #narrowed} reaches the day-of-era with one division
 * and one correction but is valid only over {@link #NARROW_MIN_DAYS}..{@link #NARROW_MAX_DAYS};
 * {@link #total} is correct for every {@code int} day and pays a two-step division for it.
 * Everything from the day of era onward is shared - {@link #fromEra} - and is where the
 * exhaustive verification concentrates, since its input domain is only 146097 wide.
 *
 * <p><b>The calendar this decomposes into.</b> All of it works in a March-based year, where the
 * leap day is the last day rather than an interior one, so a year's length is a function of its
 * index alone. {@link #fromEra} converts back to January-based years at the end, which is the
 * {@code mp >= 10} adjustment.
 */
public final class VarkaCalendar {

  private VarkaCalendar() {}

  /** Days in a 400-year Gregorian era: 400 * 365 + 97 leap days. */
  public static final int ERA_DAYS = 146097;

  /** Days in the first three centuries of an era; the fourth has one more. */
  public static final int CENTURY_DAYS = 36524;

  /** Days from 0000-03-01 to 1970-01-01, the shift into March-based years. */
  public static final int MARCH_EPOCH_SHIFT = 719468;

  // --- The narrowed variant: one division, one correction, a bounded input range -------------

  /** How many eras {@link #NARROW_BIAS} adds; subtracted again when the year is assembled. */
  public static final int NARROW_ERA_BIAS = 32;

  /**
   * {@link #MARCH_EPOCH_SHIFT} plus {@link #NARROW_ERA_BIAS} whole eras. The extra eras are what
   * let the range reach back past year zero while the value stays non-negative, so the division
   * never has to round toward negative infinity.
   */
  public static final int NARROW_BIAS = MARCH_EPOCH_SHIFT + NARROW_ERA_BIAS * ERA_DAYS;

  /**
   * {@code floor(2^24 / 146097)}, rounded down so the quotient is never overestimated. The
   * shortfall is {@code w * (2^24 - M * d) / (d * 2^24) < 1} for every {@code w < 2^24}, so one
   * correction step recovers the exact quotient - and {@code M * w <= 114 * (2^24 - 1)} is
   * comfortably inside {@code 2^31}.
   */
  public static final int NARROW_ERA_M = 114;

  /** The shift paired with {@link #NARROW_ERA_M}; {@code w < 2^24} is what bounds the range. */
  public static final int NARROW_ERA_K = 24;

  /** The first day {@link #narrowed} is defined for: {@code -NARROW_BIAS}, i.e. 0000-03-01 less
   * {@link #NARROW_ERA_BIAS} eras. In calendar terms, 1 March -12800. */
  public static final int NARROW_MIN_DAYS = -NARROW_BIAS;

  /** The last day {@link #narrowed} is defined for: the largest day with {@code w < 2^24}. In
   * calendar terms, 15 August 33134. */
  public static final int NARROW_MAX_DAYS = (1 << NARROW_ERA_K) - 1 - NARROW_BIAS;

  // --- The total variant: a two-step division, correct for every int day --------------------

  /**
   * Where the dividend is split. The high half drives a magic multiply and the low half is
   * dropped, which can only make the quotient too small - never too large - so the corrections
   * stay one-sided.
   */
  public static final int TOTAL_SPLIT_SHIFT = 16;

  /** Added to the arithmetic-shifted high half to make it non-negative. */
  public static final int TOTAL_HI_BIAS = 1 << (31 - TOTAL_SPLIT_SHIFT);

  /**
   * The magic for {@code h * 2^16 / 146097}, and {@link #TOTAL_ERA_OFFSET} the quotient of
   * {@link #TOTAL_HI_BIAS} eras that the bias introduced. Chosen by exhaustive search over all
   * 65536 high halves at both low-half endpoints - a proof rather than a sample, because the
   * error is monotone in the low half - to satisfy {@code 0 <= q - q0 <= 2}, i.e. never an
   * overestimate and at most two corrections.
   */
  public static final int TOTAL_ERA_M = 14699;

  /** The shift paired with {@link #TOTAL_ERA_M}. */
  public static final int TOTAL_ERA_K = 15;

  /** Subtracted after the magic, to undo {@link #TOTAL_HI_BIAS}. */
  public static final int TOTAL_ERA_OFFSET = 14700;

  /** How many correction steps {@link #TOTAL_ERA_M}'s bound requires. */
  public static final int TOTAL_CORRECTIONS = 2;

  /** {@code MARCH_EPOCH_SHIFT / ERA_DAYS}: the epoch shift, folded past the division. */
  public static final int EPOCH_ERA_QUOTIENT = MARCH_EPOCH_SHIFT / ERA_DAYS;

  /** {@code MARCH_EPOCH_SHIFT % ERA_DAYS}: what is left to add to the remainder, where it costs
   * one compare instead of an addition that would overflow near {@code Integer.MAX_VALUE}. */
  public static final int EPOCH_ERA_REMAINDER = MARCH_EPOCH_SHIFT % ERA_DAYS;

  // --- The shared tail: day of era to the four fields ---------------------------------------

  /** {@code floor(2^28 / 36524)}, round-down; one correction, dividend at most 146096. */
  public static final int CENTURY_M = 7349;

  /** The shift paired with {@link #CENTURY_M}. */
  public static final int CENTURY_K = 28;

  /**
   * {@code ceil(2^24 / 365) = 45966}, with {@code e = 374}, so the identity holds exactly for
   * every dividend under {@code 2^24 / 374 = 44859}. The dividend here is the day of century,
   * at most 36524 - the era's spilling last day, one past a plain century's 36523 - so this
   * division needs no correction at all. It is the split into centuries that buys this: on the
   * day of era, 146096 wide, no exact magic for 365 exists.
   */
  public static final int YEAR_M = 45966;

  /** The shift paired with {@link #YEAR_M}. */
  public static final int YEAR_K = 24;

  /** Exact magic for {@code / 153} over a dividend of at most {@code 5 * 365 + 2}. */
  public static final int MONTH_M = 877241;

  /** The shift paired with {@link #MONTH_M}. */
  public static final int MONTH_K = 27;

  /** Exact magic for {@code / 5} over a dividend of at most {@code 153 * 11 + 2}. */
  public static final int DAY_M = 838861;

  /** The shift paired with {@link #DAY_M}. */
  public static final int DAY_K = 22;

  /** Exact magic for {@code / 3} over a dividend of at most 14. */
  public static final int QUARTER_M = 89478486;

  /** The shift paired with {@link #QUARTER_M}. */
  public static final int QUARTER_K = 28;

  /** The day of the March-based year on which January arrives, and the year number turns. */
  public static final int MARCH_YEAR_JANUARY = 10;

  /**
   * The four calendar fields one decomposition yields, in the order the emitter's per-field
   * tails branch off the shared work.
   *
   * @param year the proleptic Gregorian year, as {@code java.time.LocalDate#getYear} gives it.
   * @param month 1-12.
   * @param dayOfMonth 1-31.
   * @param quarter 1-4.
   */
  public record Fields(int year, int month, int dayOfMonth, int quarter) {}

  /** Whether {@link #narrowed} is defined for {@code days} - the guard the emitted kernel
   * evaluates per lane when the narrowed lowering is in use. */
  public static boolean inNarrowRange(int days) {
    return days >= NARROW_MIN_DAYS && days <= NARROW_MAX_DAYS;
  }

  /**
   * The narrowed decomposition. Undefined - not merely inaccurate - outside
   * {@link #inNarrowRange}, which is why the emitted form carries a guard and the batch falls
   * back to the row engine rather than publishing whatever this returns.
   */
  public static Fields narrowed(int days) {
    int w = days + NARROW_BIAS;
    int era = (w * NARROW_ERA_M) >>> NARROW_ERA_K;
    int rem = w - era * ERA_DAYS;
    if (rem >= ERA_DAYS) {
      era++;
      rem -= ERA_DAYS;
    }
    return fromEra(era - NARROW_ERA_BIAS, rem);
  }

  /**
   * The total decomposition, correct for every {@code int} day. It never forms
   * {@code days + MARCH_EPOCH_SHIFT}, which overflows in the top {@link #MARCH_EPOCH_SHIFT}
   * days; the shift is folded past the division instead, where it is one compare.
   */
  public static Fields total(int days) {
    int h = (days >> TOTAL_SPLIT_SHIFT) + TOTAL_HI_BIAS;
    int quotient = ((h * TOTAL_ERA_M) >>> TOTAL_ERA_K) - TOTAL_ERA_OFFSET;
    // The product below overflows int for the largest quotients, deliberately: the true
    // difference lies in [0, 3 * ERA_DAYS), so the low 32 bits carry it exactly. Do not
    // "fix" this by widening - the whole point of the split is to stay in 32-bit lanes.
    int rem = days - quotient * ERA_DAYS;
    for (int i = 0; i < TOTAL_CORRECTIONS; i++) {
      if (rem >= ERA_DAYS) {
        quotient++;
        rem -= ERA_DAYS;
      }
    }
    int shifted = rem + EPOCH_ERA_REMAINDER;
    int era = quotient + EPOCH_ERA_QUOTIENT;
    if (shifted >= ERA_DAYS) {
      era++;
      shifted -= ERA_DAYS;
    }
    return fromEra(era, shifted);
  }

  /**
   * Day of era to the four fields - the half both variants share, and the half whose input
   * domain ({@code [0, 146096]}) is small enough to verify exhaustively on its own.
   *
   * <p>Two overshoot fixes earn their place here. The century magic can land on century 4,
   * which exists for exactly one day of each era (the fourth century holds the era's extra leap
   * day); it is folded back into century 3. And the exact {@code / 365} ignores leap days, so
   * it can name the next year when the day of century falls in one - detected by a negative day
   * of year, and undone by giving the day back, one more when the year we step back into is a
   * leap year.
   */
  private static Fields fromEra(int era, int dayOfEra) {
    int century = (dayOfEra * CENTURY_M) >>> CENTURY_K;
    int dayOfCentury = dayOfEra - century * CENTURY_DAYS;
    if (dayOfCentury >= CENTURY_DAYS) {
      century++;
      dayOfCentury -= CENTURY_DAYS;
    }
    if (century == 4) {
      century = 3;
      dayOfCentury += CENTURY_DAYS;
    }
    int yearOfCentury = (dayOfCentury * YEAR_M) >>> YEAR_K;
    int dayOfYear = dayOfCentury - (365 * yearOfCentury + (yearOfCentury >>> 2));
    if (dayOfYear < 0) {
      dayOfYear += 365 + ((yearOfCentury & 3) == 0 ? 1 : 0);
      yearOfCentury--;
    }
    int marchMonth = ((5 * dayOfYear + 2) * MONTH_M) >>> MONTH_K;
    int dayOfMonth = dayOfYear - (((153 * marchMonth + 2) * DAY_M) >>> DAY_K) + 1;
    int month = marchMonth < MARCH_YEAR_JANUARY ? marchMonth + 3 : marchMonth - 9;
    int year = 400 * era + 100 * century + yearOfCentury
        + (marchMonth >= MARCH_YEAR_JANUARY ? 1 : 0);
    int quarter = ((month + 2) * QUARTER_M) >>> QUARTER_K;
    return new Fields(year, month, dayOfMonth, quarter);
  }
}
