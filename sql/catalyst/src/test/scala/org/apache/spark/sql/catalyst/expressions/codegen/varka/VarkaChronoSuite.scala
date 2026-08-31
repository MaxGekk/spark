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

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaChrono.Fields

/**
 * The scalar half of task 26: `VarkaChrono`'s two civil-from-days models, checked against
 * `java.time` before any of it is emitted as bytecode. The emitter loads the same constants
 * these methods use, so a disagreement between an emitted kernel and this model is an emission
 * bug, while a disagreement between this model and `LocalDate` is an arithmetic one - keeping
 * the two failures apart is the reason this suite exists separately from the emitter's.
 *
 * The everyday tests run a curated boundary set. The exhaustive sweeps that actually justify
 * the constants are gated behind `-Dvarka.sweep=true` because the total one walks all 2^32
 * days; their results are recorded in `sql/varka/plans/PLAN_TASK_26.md`. The nearest precedent
 * for the gate is the engine module's `varka.jmh` JUnit gate - no other catalyst Varka test is
 * property-gated.
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
    new Fields(date.getYear, date.getMonthValue, date.getDayOfMonth, (date.getMonthValue + 2) / 3)
  }

  /**
   * Hinnant's civil-from-days in long arithmetic - an independent oracle for the sweep that
   * cannot walk `LocalDate` 2^32 times in a test's patience. It is itself checked against
   * `LocalDate` by the third sweep, so the chain is closed rather than assumed.
   */
  private def longReference(days: Int): Fields = {
    val z = days.toLong + 719468L
    val era = Math.floorDiv(z, 146097L)
    val doe = z - era * 146097L
    val yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365
    val doy = doe - (365 * yoe + yoe / 4 - yoe / 100)
    val mp = (5 * doy + 2) / 153
    val day = doy - (153 * mp + 2) / 5 + 1
    val month = mp + (if (mp < 10) 3 else -9)
    val year = yoe + era * 400 + (if (month <= 2) 1 else 0)
    new Fields(year.toInt, month.toInt, day.toInt, ((month + 2) / 3).toInt)
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

  /**
   * The structural boundaries plus a uniform sample of the whole int range. Only about one
   * random int in 256 lands inside the narrowed range, which is the point: this set is for the
   * total model, and `narrowDays` is the one the narrowed model is asked about.
   */
  private def wideDays: Seq[Int] = {
    val random = new Random(26)
    boundaryDays ++ Seq.fill(20000)(random.nextInt())
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
      assert(VarkaChrono.narrowed(day) === reference(day), s"narrowed disagreed on day $day")
    }
  }

  test("the total model matches LocalDate over every calendar boundary and the int extremes") {
    val days = wideDays ++ Seq(
      Int.MinValue, Int.MinValue + 1, Int.MaxValue - 1, Int.MaxValue,
      // The top of the range is where `days + MARCH_EPOCH_SHIFT` would overflow, which is the
      // whole reason the total variant folds the shift past the division.
      Int.MaxValue - VarkaChrono.MARCH_EPOCH_SHIFT,
      Int.MaxValue - VarkaChrono.MARCH_EPOCH_SHIFT + 1)
    for (day <- days) {
      assert(VarkaChrono.total(day) === reference(day), s"total disagreed on day $day")
    }
  }

  test("the two models agree wherever both are defined") {
    for (day <- narrowDays) {
      assert(VarkaChrono.narrowed(day) === VarkaChrono.total(day), s"disagreed on day $day")
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

  test("the long reference agrees with LocalDate, which is what lets the sweep use it") {
    for (day <- wideDays) {
      assert(longReference(day) === reference(day), s"the long reference disagreed on day $day")
    }
  }

  test("the exhaustive sweeps (opt-in: -Dvarka.sweep=true)") {
    assume(System.getProperty("varka.sweep") == "true",
      "set -Dvarka.sweep=true to run the exhaustive sweeps")

    var mismatches = 0
    var day = VarkaChrono.NARROW_MIN_DAYS
    while (day <= VarkaChrono.NARROW_MAX_DAYS) {
      if (VarkaChrono.narrowed(day) != reference(day)) {
        mismatches += 1
      }
      day += 1
    }
    assert(mismatches === 0, s"the narrowed model disagreed with LocalDate on $mismatches days")

    mismatches = 0
    var wide = Int.MinValue.toLong
    while (wide <= Int.MaxValue.toLong) {
      val d = wide.toInt
      if (VarkaChrono.total(d) != longReference(d)) {
        mismatches += 1
      }
      wide += 1
    }
    assert(mismatches === 0, s"the total model disagreed with the reference on $mismatches days")

    mismatches = 0
    day = VarkaChrono.NARROW_MIN_DAYS
    while (day <= VarkaChrono.NARROW_MAX_DAYS) {
      if (longReference(day) != reference(day)) {
        mismatches += 1
      }
      day += 1
    }
    assert(mismatches === 0, s"the long reference disagreed with LocalDate on $mismatches days")
  }
}
