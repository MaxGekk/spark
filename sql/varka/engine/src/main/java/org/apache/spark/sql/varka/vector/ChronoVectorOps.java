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

package org.apache.spark.sql.varka.vector;

import static org.apache.spark.sql.varka.vector.VarkaVectorSupport.ofAddress;
import static org.apache.spark.sql.varka.vector.VarkaVectorSupport.orPartialValidityBitsAt;
import static org.apache.spark.sql.varka.vector.VarkaVectorSupport.orValidityBitsAt;
import static org.apache.spark.sql.varka.vector.VarkaVectorSupport.partialValidityBitsAt;
import static org.apache.spark.sql.varka.vector.VarkaVectorSupport.validityBitsAt;
import static org.apache.spark.sql.varka.vector.VarkaVectorSupport.zero;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * A hand-written measurement kernel for task 32 ({@code PLAN_MILESTONE_4.md} section 2.9): the
 * ceiling on computing {@code year}, {@code month}, {@code dayofmonth} and {@code quarter} from
 * ONE civil-from-days decomposition per row, against the 441.2 M rows/s four independently
 * emitted nodes reach today ({@code VarkaEmitterParityBenchmark-jdk25-results.txt}).
 *
 * <p>This is deliberately not production code and is not wired into {@link
 * org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaFusedKernel} - the engine module
 * cannot depend on catalyst, which owns that interface and the narrow-range guard
 * ({@code VarkaChrono.inNarrowRange}). The guard is intentionally omitted here: this kernel
 * exists only to measure the ceiling on sharing the decomposition, using the same in-range,
 * null-aware data the "year+month+day+quarter, null-free" benchmark case already drives, and the
 * decision this measures is whether task 32 proceeds at all - not to ship a second, guardless
 * lowering. If the task proceeds, the guard and the fallback status belong to whatever mechanism
 * gets built, not to this file.
 *
 * <p>The arithmetic below is a lane-for-lane transcription of {@code VarkaChrono.narrowed} /
 * {@code VarkaChrono.fromEra} and of the bytecode {@code VarkaLoopEmitter.emitChrono} emits for
 * each field today - same magic constants, same correction steps, same order of operations - so
 * that the only thing this measures is sharing the decomposition, not a different algorithm.
 */
public final class ChronoVectorOps {

  private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;
  private static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;

  // The narrowed civil-from-days constants, copied from VarkaChrono (catalyst) rather than
  // referenced - the engine module cannot depend on catalyst. Keep these in lockstep with
  // VarkaChrono by hand; ChronoVectorOpsTest sweeps this kernel against java.time directly,
  // so a drift here fails a test rather than silently mismeasuring the ceiling.
  private static final int ERA_DAYS = 146097;
  private static final int CENTURY_DAYS = 36524;
  private static final int MARCH_EPOCH_SHIFT = 719468;
  private static final int NARROW_ERA_BIAS = 32;
  private static final int NARROW_BIAS = MARCH_EPOCH_SHIFT + NARROW_ERA_BIAS * ERA_DAYS;
  private static final int NARROW_ERA_M = 114;
  private static final int NARROW_ERA_K = 24;
  private static final int CENTURY_M = 7349;
  private static final int CENTURY_K = 28;
  private static final int YEAR_M = 45966;
  private static final int YEAR_K = 24;
  private static final int MONTH_M = 877241;
  private static final int MONTH_K = 27;
  private static final int DAY_M = 838861;
  private static final int DAY_K = 22;
  private static final int QUARTER_M = 89478486;
  private static final int QUARTER_K = 28;
  private static final int MARCH_YEAR_JANUARY = 10;

  private ChronoVectorOps() {}

  /** The four fields one decomposition yields, one lane group at a time. */
  private record Fields(IntVector year, IntVector month, IntVector dayOfMonth,
      IntVector quarter) {}

  private static IntVector magic(IntVector v, int m, int k) {
    return v.mul(m).lanewise(VectorOperators.LSHR, k);
  }

  /**
   * The shared civil-from-days decomposition, computed once and read four ways - the thing
   * task 32 measures the value of. Mirrors {@code VarkaLoopEmitter.emitChrono}'s bytecode
   * exactly, including both round-down-magic correction steps and both overshoot fixes (the
   * era's spilling fourth century, and the exact {@code / 365} naming the following year).
   */
  private static Fields computeFields(IntVector days) {
    IntVector w = days.add(NARROW_BIAS);
    IntVector era = magic(w, NARROW_ERA_M, NARROW_ERA_K);
    IntVector rem = w.sub(era.mul(ERA_DAYS));
    VectorMask<Integer> eraCarry = rem.compare(VectorOperators.GE, ERA_DAYS);
    era = era.add(1, eraCarry);
    rem = rem.sub(ERA_DAYS, eraCarry);
    era = era.sub(NARROW_ERA_BIAS);

    IntVector century = magic(rem, CENTURY_M, CENTURY_K);
    rem = rem.sub(century.mul(CENTURY_DAYS));
    VectorMask<Integer> centuryCarry = rem.compare(VectorOperators.GE, CENTURY_DAYS);
    century = century.add(1, centuryCarry);
    rem = rem.sub(CENTURY_DAYS, centuryCarry);

    // The era's fourth century holds one extra day (its leap day); fold it back into century 3.
    VectorMask<Integer> century4 = century.compare(VectorOperators.EQ, 4);
    century = century.sub(1, century4);
    rem = rem.add(CENTURY_DAYS, century4);

    IntVector yearOfCentury = magic(rem, YEAR_M, YEAR_K);
    IntVector dayOfYear =
        rem.sub(yearOfCentury.mul(365).add(yearOfCentury.lanewise(VectorOperators.LSHR, 2)));

    // The exact /365 ignores leap days, so it can name the following year; step back where it
    // did, giving one more day back if the year stepped into is a leap year.
    VectorMask<Integer> negDoy = dayOfYear.compare(VectorOperators.LT, 0);
    VectorMask<Integer> leap =
        yearOfCentury.and(3).compare(VectorOperators.EQ, 0).and(negDoy);
    dayOfYear = dayOfYear.add(365, negDoy).add(1, leap);
    yearOfCentury = yearOfCentury.sub(1, negDoy);

    IntVector marchMonth = magic(dayOfYear.mul(5).add(2), MONTH_M, MONTH_K);
    VectorMask<Integer> januaryTurned = marchMonth.compare(VectorOperators.GE, MARCH_YEAR_JANUARY);

    IntVector year =
        era.mul(400).add(century.mul(100)).add(yearOfCentury).add(1, januaryTurned);
    IntVector month = marchMonth.add(3).sub(12, januaryTurned);
    IntVector dayOfMonth =
        dayOfYear.sub(magic(marchMonth.mul(153).add(2), DAY_M, DAY_K)).add(1);
    IntVector quarter = magic(month.add(2), QUARTER_M, QUARTER_K);

    return new Fields(year, month, dayOfMonth, quarter);
  }

  /**
   * dst[i] = the four fields of {@code src[i]}'s civil-from-days decomposition; every dst null
   * iff src is null. {@code src} must lie in {@code VarkaChrono.NARROW_MIN_DAYS..NARROW_MAX_DAYS}
   * - this kernel has no guard and does not check, per the class doc.
   *
   * @param srcData address of the source int32 day values.
   * @param srcValidity address of the source bit-packed validity; ignored (may be 0L) when
   *        {@code srcNullCount == 0} or {@code srcNullCount == length}.
   * @param srcNullCount number of null rows in the source.
   * @param dstYear address of the destination year int32 values (length * 4 bytes).
   * @param dstMonth address of the destination month int32 values (length * 4 bytes).
   * @param dstDayOfMonth address of the destination day-of-month int32 values (length * 4 bytes).
   * @param dstQuarter address of the destination quarter int32 values (length * 4 bytes).
   * @param dstValidity address of the shared destination bit-packed validity
   *        ((length + 7) / 8 bytes); always required. All four outputs share one validity
   *        buffer because all four come from the same single source column.
   * @param length number of rows.
   */
  public static void vectorFourFields(
      long srcData, long srcValidity, int srcNullCount,
      long dstYear, long dstMonth, long dstDayOfMonth, long dstQuarter,
      long dstValidity, int length) {
    if (length <= 0) {
      return;
    }
    MemorySegment src = ofAddress(srcData, length * 4L);
    MemorySegment yearSeg = ofAddress(dstYear, length * 4L);
    MemorySegment monthSeg = ofAddress(dstMonth, length * 4L);
    MemorySegment dayOfMonthSeg = ofAddress(dstDayOfMonth, length * 4L);
    MemorySegment quarterSeg = ofAddress(dstQuarter, length * 4L);
    MemorySegment dstValiditySeg = ofAddress(dstValidity, (length + 7) / 8L);
    zero(dstValiditySeg);
    if (srcNullCount == length) {
      return;
    }
    boolean hasNulls = srcNullCount > 0;
    MemorySegment validity = hasNulls ? ofAddress(srcValidity, (length + 7) / 8L) : null;

    long loopBound = SPECIES.loopBound(length);
    int lanes = SPECIES.length();
    long i = 0;
    for (; i < loopBound; i += lanes) {
      VectorMask<Integer> mask = hasNulls
          ? VectorMask.fromLong(SPECIES, validityBitsAt(validity, i, lanes))
          : VectorMask.fromLong(SPECIES, -1L);
      long byteOffset = i * 4L;
      IntVector days = IntVector.fromMemorySegment(SPECIES, src, byteOffset, ORDER, mask);
      Fields f = computeFields(days);
      f.year().intoMemorySegment(yearSeg, byteOffset, ORDER, mask);
      f.month().intoMemorySegment(monthSeg, byteOffset, ORDER, mask);
      f.dayOfMonth().intoMemorySegment(dayOfMonthSeg, byteOffset, ORDER, mask);
      f.quarter().intoMemorySegment(quarterSeg, byteOffset, ORDER, mask);
      orValidityBitsAt(dstValiditySeg, i, mask.toLong(), lanes);
    }
    if (i < length) {
      fourFieldsEpilogue(src, yearSeg, monthSeg, dayOfMonthSeg, quarterSeg, dstValiditySeg,
          validity, hasNulls, i, length);
    }
  }

  /** {@link #vectorFourFields}'s final partial lane group; see {@code DateVectorOps}'s class
   * doc, step 5, for why this is its own method rather than inlined into the loop's. */
  private static void fourFieldsEpilogue(MemorySegment src, MemorySegment yearSeg,
      MemorySegment monthSeg, MemorySegment dayOfMonthSeg, MemorySegment quarterSeg,
      MemorySegment dstValiditySeg, MemorySegment validity, boolean hasNulls,
      long i, int length) {
    int rows = (int) (length - i);
    VectorMask<Integer> bounds = SPECIES.indexInRange((int) i, length);
    VectorMask<Integer> mask = hasNulls
        ? VectorMask.fromLong(SPECIES, partialValidityBitsAt(validity, i, rows)).and(bounds)
        : bounds;
    long byteOffset = i * 4L;
    IntVector days = IntVector.fromMemorySegment(SPECIES, src, byteOffset, ORDER, mask);
    Fields f = computeFields(days);
    f.year().intoMemorySegment(yearSeg, byteOffset, ORDER, mask);
    f.month().intoMemorySegment(monthSeg, byteOffset, ORDER, mask);
    f.dayOfMonth().intoMemorySegment(dayOfMonthSeg, byteOffset, ORDER, mask);
    f.quarter().intoMemorySegment(quarterSeg, byteOffset, ORDER, mask);
    orPartialValidityBitsAt(dstValiditySeg, i, mask.toLong(), rows);
  }
}
