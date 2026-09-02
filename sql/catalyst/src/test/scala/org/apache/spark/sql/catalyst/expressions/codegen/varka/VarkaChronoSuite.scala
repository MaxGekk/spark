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


package org.apache.spark.sql.catalyst.expressions.codegen.varka

import java.time.LocalDate
import java.time.temporal.IsoFields

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaChrono.Fields

/**
 * The scalar half of task 26: `VarkaChrono`'s civil-from-days model, checked against
 * `java.time` before any of it is emitted as bytecode. The emitter loads the same constants
 * these methods use, so a disagreement between an emitted kernel and this model is an emission
 * bug, while a disagreement between this model and `LocalDate` is an arithmetic one - keeping
 * the two failures apart is the reason this suite exists separately from the emitter's.
 *
 * The everyday tests run a curated boundary set. The exhaustive sweep that actually justifies
 * the constants - all 16777216 days the lowering is defined over, against `LocalDate` - is
 * gated behind `-Dvarka.sweep=true`, and its result is recorded in
 * `sql/varka/plans/PLAN_TASK_26.md`. The nearest precedent for the gate is the engine module's
 * `varka.jmh` JUnit gate - no other catalyst Varka test is property-gated.
 *
 *   build/sbt 'catalyst/testOnly *VarkaChronoSuite'
 *   build/sbt "project catalyst" 'set Test/javaOptions += "-Dvarka.sweep=true"' \
 *     'testOnly *VarkaChronoSuite'
 */
class VarkaChronoSuite extends SparkFunSuite {

  /** `java.time`'s answer, which is exactly what Spark's `DateTimeUtils.getYear` and its three
   * siblings return for the same day. */
  private def reference(days: Int): Fields = {
    val date = LocalDate.ofEpochDay(days.toLong)
    // IsoFields.QUARTER_OF_YEAR, not (month + 2) / 3: the second is what the emitter computes,
    // and an oracle that restates the implementation checks nothing. DateTimeUtils.getQuarter
    // is the first form, so it is the definition this must be held to.
    new Fields(date.getYear, date.getMonthValue, date.getDayOfMonth,
      date.get(IsoFields.QUARTER_OF_YEAR), date.getDayOfYear)
  }

  /**
   * Every day the decomposition could plausibly get wrong: era and century starts, the
   * 400-year cycle's leap-day edges, every month-length boundary of a leap
   * and a common year, the March-based year's own turn, pre-1970, year 1, and the narrowed
   * range's endpoints. Days outside the narrowed range are marked so the narrowed model, which
   * is undefined there, is not asked about them.
   */
  private def boundaryDays: Seq[Int] = {
    val eras = (-13 to 13).flatMap { era =>
      val start = era * 146097 - 719468
      Seq(start - 1, start, start + 1)
    }
    val centuries = (-1 to 4).flatMap { century =>
      val start = century * 36524 - 719468
      Seq(start - 1, start, start + 1)
    }
    val chrono = Seq(
      LocalDate.of(1, 1, 1), LocalDate.of(1, 12, 31),
      LocalDate.of(1600, 2, 28), LocalDate.of(1600, 2, 29), LocalDate.of(1600, 3, 1),
      LocalDate.of(1700, 2, 28), LocalDate.of(1700, 3, 1),
      LocalDate.of(1900, 2, 28), LocalDate.of(1900, 3, 1),
      LocalDate.of(1969, 12, 31), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 2),
      LocalDate.of(2000, 2, 28), LocalDate.of(2000, 2, 29), LocalDate.of(2000, 3, 1),
      LocalDate.of(2024, 12, 31), LocalDate.of(2025, 1, 1),
      LocalDate.of(9999, 12, 31)).map(_.toEpochDay.toInt)
    val monthEnds = for {
      year <- Seq(2023, 2024)
      month <- 1 to 12
      day <- Seq(1, LocalDate.of(year, month, 1).lengthOfMonth)
    } yield LocalDate.of(year, month, day).toEpochDay.toInt
    val edges = Seq(
      VarkaChrono.NARROW_MIN_DAYS, VarkaChrono.NARROW_MIN_DAYS + 1,
      VarkaChrono.NARROW_MAX_DAYS - 1, VarkaChrono.NARROW_MAX_DAYS)
    eras ++ centuries ++ chrono ++ monthEnds ++ edges
  }

  /** The structural boundaries that fall inside the narrowed range, plus a uniform sample of
   * that range - which is where the narrowed model is defined and nowhere else. */
  private def narrowDays: Seq[Int] = {
    val random = new Random(2600)
    val span = VarkaChrono.NARROW_MAX_DAYS.toLong - VarkaChrono.NARROW_MIN_DAYS + 1
    val sampled = Seq.fill(20000)(
      (VarkaChrono.NARROW_MIN_DAYS + (random.nextDouble() * span).toLong).toInt)
    boundaryDays.filter(VarkaChrono.inNarrowRange) ++ sampled
  }

  test("the narrowed model matches LocalDate over every calendar boundary in its range") {
    val days = narrowDays
    assert(days.forall(VarkaChrono.inNarrowRange), "the set must stay inside the range")
    for (day <- days) {
      assert(VarkaChrono.narrowed(day) === reference(day), s"disagreed on day $day")
    }
  }

  test("the narrowed range's bounds are the ones the constants imply") {
    assert(VarkaChrono.NARROW_MIN_DAYS === -VarkaChrono.NARROW_BIAS)
    assert(VarkaChrono.NARROW_MAX_DAYS ===
      (1 << VarkaChrono.NARROW_ERA_K) - 1 - VarkaChrono.NARROW_BIAS)
    assert(!VarkaChrono.inNarrowRange(VarkaChrono.NARROW_MIN_DAYS - 1))
    assert(VarkaChrono.inNarrowRange(VarkaChrono.NARROW_MIN_DAYS))
    assert(VarkaChrono.inNarrowRange(VarkaChrono.NARROW_MAX_DAYS))
    assert(!VarkaChrono.inNarrowRange(VarkaChrono.NARROW_MAX_DAYS + 1))
    // The range must contain every date SQL can write, which is what makes the guard's
    // fallback a corner case rather than a common path.
    assert(VarkaChrono.inNarrowRange(LocalDate.of(1, 1, 1).toEpochDay.toInt))
    assert(VarkaChrono.inNarrowRange(LocalDate.of(9999, 12, 31).toEpochDay.toInt))
  }

  test("the leap-year hash is exact over its whole domain, and only there") {
    // A perfect hash is exact inside its domain and arbitrary one step past it, so the domain
    // is the whole contract and the only honest test of it is all of it: 102,500 years, which
    // costs milliseconds. Both ends are asserted, because the bound being tight is what makes
    // "a caller outside this range needs a different bias, not a correction" true.
    val lo = -VarkaChrono.YEAR_BIAS
    val hi = VarkaChrono.LEAP_HASH_MAX_BIASED_YEAR - VarkaChrono.YEAR_BIAS
    def reference(year: Int): Boolean =
      Math.floorMod(year, 4) == 0 &&
        (Math.floorMod(year, 100) != 0 || Math.floorMod(year, 400) == 0)
    var mismatches = 0
    var year = lo
    while (year <= hi) {
      if (VarkaChrono.isLeapYear(year) != reference(year)) {
        mismatches += 1
      }
      year += 1
    }
    assert(mismatches === 0, s"the hash disagreed with the Gregorian rule on $mismatches years")
    // The first year past the domain, where it is allowed to be - and in fact is - wrong. If
    // this ever starts agreeing, the constants moved and the domain must be re-derived.
    assert(VarkaChrono.isLeapYear(hi + 1) !== reference(hi + 1),
      "the hash is now correct one year past its stated domain, so the domain is stale")
    // The range the emitter actually needs, called out so a future widening of month
    // arithmetic trips here rather than in a differential.
    assert(lo <= -14848 && hi >= 35181,
      "the covered range no longer contains what add_months and the interval arithmetic reach")
  }

  test("the leap flag agrees with java.time over the calendar boundaries") {
    for (day <- boundaryDays) {
      val year = LocalDate.ofEpochDay(day.toLong).getYear
      assert(VarkaChrono.isLeapYear(year) === LocalDate.of(year, 1, 1).isLeapYear,
        s"disagreed with java.time on year $year")
    }
  }

  test("the exhaustive sweep (opt-in: -Dvarka.sweep=true)") {
    assume(System.getProperty("varka.sweep") == "true",
      "set -Dvarka.sweep=true to run the exhaustive sweep")

    var mismatches = 0
    var day = VarkaChrono.NARROW_MIN_DAYS
    while (day <= VarkaChrono.NARROW_MAX_DAYS) {
      if (VarkaChrono.narrowed(day) != reference(day)) {
        mismatches += 1
      }
      day += 1
    }
    assert(mismatches === 0, s"the model disagreed with LocalDate on $mismatches days")
  }
}
