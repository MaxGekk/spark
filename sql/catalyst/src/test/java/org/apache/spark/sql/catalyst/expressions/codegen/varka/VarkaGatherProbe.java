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

import java.time.LocalDate;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * Two ways to get a year from an epoch day on int lanes, so the parity benchmark can price a
 * <b>gather</b> against arithmetic. Written because a widely deployed row engine (Impala) reads
 * the year out of a day-indexed lookup table for dates in 1950-2049 and computes it only outside
 * that window, and the milestone's item 9 rests on an assumption about what a gather costs.
 *
 * <p>This is a capability probe, not a kernel. Varka cannot emit either of these: the emitter
 * has no gather, and - the reason that is not merely an omission - {@code IntVector} offers an
 * index-map overload only on {@code fromArray}, never on {@code fromMemorySegment}, while every
 * Varka input is an off-heap Arrow buffer. See {@code PLAN_MILESTONE_4.md} item 9.
 */
public final class VarkaGatherProbe {

  private VarkaGatherProbe() {
  }

  private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

  /** 1 January 1950, the first day Impala's table covers. */
  public static final int MIN_DAY_MAPPED = -7305;

  /** 31 December 2049, the last. */
  public static final int MAX_DAY_MAPPED = 29219;

  /** The year of every day in that window: 36525 ints, about 143 KB. */
  public static final int[] YEAR_BY_DAY = buildTable();

  private static int[] buildTable() {
    int[] table = new int[MAX_DAY_MAPPED - MIN_DAY_MAPPED + 1];
    for (int i = 0; i < table.length; i++) {
      table[i] = LocalDate.ofEpochDay(MIN_DAY_MAPPED + i).getYear();
    }
    return table;
  }

  /**
   * The year of each day, read from {@link #YEAR_BY_DAY}. One index vector per lane group, spilled
   * to {@code indexScratch} because the gather takes its index map as an {@code int[]}, and one
   * {@code fromArray} with that map. Defined only over the mapped window.
   */
  public static void yearByLookup(int[] days, int[] out, int[] indexScratch) {
    for (int i = 0; i < days.length; i += SPECIES.length()) {
      IntVector.fromArray(SPECIES, days, i).sub(MIN_DAY_MAPPED).intoArray(indexScratch, 0);
      IntVector.fromArray(SPECIES, YEAR_BY_DAY, 0, indexScratch, 0).intoArray(out, i);
    }
  }

  /**
   * The year of each day by the civil-from-days arithmetic Varka emits: the narrowed era step,
   * the century and year-of-century divisions with their correction carries, and the March-based
   * month whose only use here is the January bit. Kept close to the emitted lowering so the
   * comparison is fair, and held to {@code LocalDate} by {@code VarkaGatherProbeSuite}.
   *
   * <p>This mirrors the lowering on master. Task 48 removes the month step from the year tail,
   * reading the January bit off the day of year instead, which takes four lane ops off this
   * body; the gather it is measured against is unaffected, so the ratio moves in arithmetic's
   * favour by that much once it lands.
   */
  public static void yearByArithmetic(int[] days, int[] out) {
    for (int i = 0; i < days.length; i += SPECIES.length()) {
      IntVector w = IntVector.fromArray(SPECIES, days, i).add(VarkaChrono.NARROW_BIAS);
      IntVector era = w.mul(VarkaChrono.NARROW_ERA_M)
          .lanewise(VectorOperators.LSHR, VarkaChrono.NARROW_ERA_K);
      IntVector rem = w.sub(era.mul(VarkaChrono.ERA_DAYS));
      VectorMask<Integer> carry = rem.compare(VectorOperators.GE, VarkaChrono.ERA_DAYS);
      era = era.add(1, carry);
      rem = rem.sub(VarkaChrono.ERA_DAYS, carry);

      IntVector century = rem.mul(VarkaChrono.CENTURY_M)
          .lanewise(VectorOperators.LSHR, VarkaChrono.CENTURY_K);
      IntVector doc = rem.sub(century.mul(VarkaChrono.CENTURY_DAYS));
      VectorMask<Integer> carry2 = doc.compare(VectorOperators.GE, VarkaChrono.CENTURY_DAYS);
      century = century.add(1, carry2);
      doc = doc.sub(VarkaChrono.CENTURY_DAYS, carry2);
      VectorMask<Integer> spill = century.compare(VectorOperators.EQ, 4);
      century = century.sub(1, spill);
      doc = doc.add(VarkaChrono.CENTURY_DAYS, spill);

      IntVector yoc = doc.mul(VarkaChrono.YEAR_M)
          .lanewise(VectorOperators.LSHR, VarkaChrono.YEAR_K);
      IntVector doy = doc.sub(yoc.mul(365).add(yoc.lanewise(VectorOperators.LSHR, 2)));
      VectorMask<Integer> over = doy.compare(VectorOperators.LT, 0);
      VectorMask<Integer> leap = yoc.and(3).compare(VectorOperators.EQ, 0).and(over);
      doy = doy.add(365, over).add(1, leap);
      yoc = yoc.sub(1, over);

      IntVector marchMonth = doy.mul(5).add(2).mul(VarkaChrono.MONTH_M)
          .lanewise(VectorOperators.LSHR, VarkaChrono.MONTH_K);
      era.mul(400).add(century.mul(100)).add(yoc).sub(VarkaChrono.NARROW_ERA_BIAS * 400)
          .add(1, marchMonth.compare(VectorOperators.GE, VarkaChrono.MARCH_YEAR_JANUARY))
          .intoArray(out, i);
    }
  }

  /**
   * The same table read by a plain scalar loop - no lanes, no index map, no spill. This is the
   * shape a row engine runs, and the one an emitter would produce if a calendar node lowered to
   * scalar code inside an otherwise vector kernel. It is here because the vector gather has to
   * pay for its index map twice, storing the index vector to an array and reading it back, and
   * that cost is an artifact of the API rather than of the hardware.
   */
  public static void yearByScalarLookup(int[] days, int[] out) {
    for (int i = 0; i < days.length; i++) {
      out[i] = YEAR_BY_DAY[days[i] - MIN_DAY_MAPPED];
    }
  }

  /** Lanes this JVM runs at, for the benchmark's header. */
  public static int lanes() {
    return SPECIES.length();
  }
}
