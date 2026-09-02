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
 * <p><b>The range, and why it is bounded.</b> {@link #narrowed} reaches the day of era with one
 * division and one correction, and is valid only over
 * {@link #NARROW_MIN_DAYS}..{@link #NARROW_MAX_DAYS} - years -12800 to 33134, which contains
 * every date SQL can write but is reachable past by {@code date_add}. The emitted form therefore
 * carries a guard and declines a batch it cannot compute. A variant that split the dividend and
 * so covered the whole {@code int} range without a guard was built and measured against this one
 * before being dropped: it cost 14 to 24%, and the numbers are in {@code PLAN_TASK_26.md}
 * section 11.2.
 *
 * <p><b>The calendar this decomposes into.</b> All of it works in a March-based year, where the
 * leap day is the last day rather than an interior one, so a year's length is a function of its
 * index alone. {@link #fromEra} converts back to January-based years at the end, which is the
 * {@code mp >= 10} adjustment.
 */
public final class VarkaChrono {

  private VarkaChrono() {}

  /** Days in a 400-year Gregorian era: 400 * 365 + 97 leap days. */
  public static final int ERA_DAYS = 146097;

  /** Days in the first three centuries of an era; the fourth has one more. */
  public static final int CENTURY_DAYS = 36524;

  /** Days from 0000-03-01 to 1970-01-01, the shift into March-based years. */
  public static final int MARCH_EPOCH_SHIFT = 719468;

  // --- The day-of-era step: one division, one correction, a bounded input range -------------

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

  // --- Day of era to the four fields ---------------------------------------

  /** {@code floor(2^28 / 36524)}, round-down; one correction, dividend at most 146096. */
  public static final int CENTURY_M = 7349;

  /** The shift paired with {@link #CENTURY_M}. */
  public static final int CENTURY_K = 28;

  /**
   * The magic for {@code / 365}, one above the round-up form: {@code ceil(2^24 / 365)} is
   * 45965 (with {@code e = 9}), and this is 45966 (with {@code e = 374}). Both are exact over
   * the dividend this sees; 45965 would be exact far further, to 1864135 against 45966's
   * 44858, and either would do. The number is what it is because that is what was derived,
   * swept and committed - do not "fix" it to the tighter ceil without re-running the sweep,
   * and do not copy 45966 as though it were {@code ceil}, because for another divisor the
   * extra one may put {@code e} past the bound.
   *
   * <p>What matters is the bound: {@code v * e < 2^k} is strict, so this is exact for every
   * dividend up to 44858. The dividend here is the day of century, at most 36524 - the era's
   * spilling last day, one past a plain century's 36523 - so this division needs no correction
   * at all. It is the split into centuries that buys that: on the day of era, 146096 wide, no
   * exact magic for 365 exists at any k.
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

  // --- Task 40: the inverse direction, and the month arithmetic built on it ------------------

  /**
   * A year bias making a reported year non-negative over the range {@link #emitDaysFromCivil}
   * actually has to cover, so a division by 100 or 400 can use a magic multiply. That range is
   * wider than task 26's narrow day range: {@code add_months}/{@code date +- INTERVAL n
   * MONTH/YEAR} can push a year up to {@code MONTH_ARITH_MAX_MONTHS}/12 (about 2047 years)
   * past either end of it, so the covered year range is roughly -14848..35181, and 15200 - a
   * multiple of 400, so it changes neither leapness nor which 400-year cycle a year falls in -
   * is the smallest such multiple that keeps the biased value non-negative throughout.
   */
  public static final int YEAR_BIAS = 15200;

  /**
   * {@code floor(2^24 / 400)}, which is also {@code floor(2^22 / 100)} - the two divisors are in
   * a 1:4 ratio matching the shift, so one magic constant serves both {@link #YEAR_CENTURY_K}
   * and {@link #YEAR_QUATERCENTENNIAL_K}.
   *
   * <p><b>This is a round-down magic, not an exact one - unlike task 34's leap flag, which
   * covers only task 26's narrow day range and needs no correction.</b> The first version of
   * this class claimed exactness "to 199728" and was wrong: that bound came from checking
   * {@code (v * M) >> k == v / d} with arbitrary-precision arithmetic, which is the right check
   * for the shift but silently assumes the multiply itself does not overflow. The emitter's
   * lanes are 32-bit and {@code LSHR} is unsigned, so the multiply is safe up to {@code v * M <
   * 2^32}, not {@code 2^31} - but the biased year here reaches about 50381, and {@code 50381 *
   * 167773} is over four billion either way. The wrong constant produced a silently wrong
   * {@code era} for exactly the inputs task 40's own tests reached during development (a
   * four-digit year plus a multi-century month offset) and nothing smaller - the failure was
   * findable only by testing the actual range this class has to cover, not a plausible-looking
   * subrange of it. One correction step (the same shape {@link #CENTURY_M} already uses) fixes
   * it: {@code floor(v / d)} from this magic is short by at most one for both divisors over
   * every dividend the callers below feed it.
   */
  public static final int YEAR_CENTURY_M = 41943;

  /** The shift for {@code / 100} paired with {@link #YEAR_CENTURY_M}; one correction needed. */
  public static final int YEAR_CENTURY_K = 22;

  /** The shift for {@code / 400} paired with {@link #YEAR_CENTURY_M}; one correction needed. */
  public static final int YEAR_QUATERCENTENNIAL_K = 24;

  /**
   * Exact magic for {@code / 12} (task 40's month arithmetic), over the dividend
   * {@code (month - 1) + monthsOffset + MONTH_ARITH_BIAS} - kept small by construction rather
   * than folding the year in, which would put the dividend near 400,000: past the ~46341 bound
   * an exact magic needs, and past the ~160,000 a round-down-plus-one-correction reaches.
   */
  public static final int MONTH_ARITH_M = 43691;

  /** The shift paired with {@link #MONTH_ARITH_M}; exact far past what {@link #MONTH_ARITH_M}
   * needs to stay inside {@code 2^31} for, which is the tighter of the two bounds. */
  public static final int MONTH_ARITH_K = 19;

  /** Whole years of headroom the month-arithmetic dividend is biased by, so it stays
   * non-negative for the most negative literal {@link #MONTH_ARITH_MIN_MONTHS} allows. */
  public static final int MONTH_ARITH_BIAS = 12 * 2048;

  /**
   * The largest {@code numMonths}/{@code interval} literal the emitter's magic multiply covers,
   * derived from {@code v * MONTH_ARITH_M < 2^31}: the dividend is
   * {@code (month - 1) + months + MONTH_ARITH_BIAS} with {@code month - 1} up to 11, so
   * {@code months} up to {@code floor((2^31 - 1) / MONTH_ARITH_M) - MONTH_ARITH_BIAS - 11}.
   * About 2000 years; a literal past this is declined rather than computed wrongly.
   */
  public static final int MONTH_ARITH_MAX_MONTHS = 24564;

  /** The smallest {@code numMonths}/{@code interval} literal covered - the negative mirror of
   * {@link #MONTH_ARITH_MAX_MONTHS}, bound only by {@link #MONTH_ARITH_BIAS} itself since
   * {@code month - 1} is never negative. */
  public static final int MONTH_ARITH_MIN_MONTHS = -MONTH_ARITH_BIAS;

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
   * Day of era to the four fields - the half whose input domain ({@code [0, 146096]}) is small
   * enough to verify exhaustively on its own.
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

  /**
   * Task 40: Hinnant's {@code days_from_civil}, the exact inverse of {@link #narrowed}, over a
   * biased (non-negative) March-based year. {@code / 4} is a shift and {@code / 5} (inside
   * {@code dayOfYear}) is an exact magic multiply over its small dividend, the same one
   * {@link #narrowed}'s day tail uses; {@code / 400} and {@code / 100} are round-down magics
   * with one correction each, {@link #YEAR_CENTURY_M}'s javadoc records why an exact one does
   * not reach far enough here even though the dividend (up to about 50381) is smaller than
   * task 26's forward-direction ones.
   *
   * <p>{@code month} must be 1-12 and {@code dayOfMonth} the already-clamped day; this method
   * does no clamping itself; {@link VarkaLoopEmitter}'s {@code emitAddMonths} does the clamp
   * before calling the equivalent lane-wise sequence. Round-trips with {@link #narrowed} over
   * every day from year 1 to year 9999, and over the wider year range {@code emitAddMonths}'s
   * month arithmetic can reach - see {@code verify_days_from_civil.py} and
   * {@code PLAN_TASK_40.md}.
   */
  public static int daysFromCivil(int year, int month, int dayOfMonth) {
    int marchYear = year - (month <= 2 ? 1 : 0);
    int biased = marchYear + YEAR_BIAS;
    int era = (biased * YEAR_CENTURY_M) >>> YEAR_QUATERCENTENNIAL_K;
    int yearOfEra = biased - era * 400;
    if (yearOfEra >= 400) {
      era++;
      yearOfEra -= 400;
    }
    int marchMonth = month + (month <= 2 ? 9 : -3);
    int dayOfYear = (((153 * marchMonth + 2) * DAY_M) >>> DAY_K) + dayOfMonth - 1;
    int centuryOfEra = (yearOfEra * YEAR_CENTURY_M) >>> YEAR_CENTURY_K;
    if (yearOfEra - centuryOfEra * 100 >= 100) {
      centuryOfEra++;
    }
    int dayOfEra = yearOfEra * 365 + (yearOfEra >>> 2) - centuryOfEra + dayOfYear;
    return (era - YEAR_BIAS / 400) * ERA_DAYS + dayOfEra - MARCH_EPOCH_SHIFT;
  }
}
