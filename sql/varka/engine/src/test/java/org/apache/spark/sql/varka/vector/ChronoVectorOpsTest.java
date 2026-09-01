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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.varka.memory.VarkaMorsel;
import org.apache.spark.sql.varka.memory.VarkaMorsel.DateMorsel;

/**
 * Differential tests for {@link ChronoVectorOps}, task 32's ceiling-measurement kernel: every
 * result is asserted against {@code java.time.LocalDate}, the same oracle {@code VarkaChrono}
 * and the emitted {@code year}/{@code month}/{@code dayofmonth}/{@code quarter} nodes are swept
 * against in {@code PLAN_TASK_26.md}. This kernel has no range guard (see its class doc), so
 * every value used here is kept inside the narrowed range it silently assumes:
 * {@code NARROW_MIN_DAYS = -5394572} to {@code NARROW_MAX_DAYS = 11382643} (years -12800 to
 * 33134), copied from {@code VarkaChrono} since the engine module cannot depend on catalyst.
 */
public class ChronoVectorOpsTest {

  private static final long NARROW_MIN_DAYS = -5394572L;
  private static final long NARROW_MAX_DAYS = 11382643L;
  private static final long NARROW_RANGE = NARROW_MAX_DAYS - NARROW_MIN_DAYS + 1; // 2^24

  private static final int[] SIZES = {1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 1000, 100000};

  private RootAllocator allocator;
  private final List<DateDayVector> vectors = new ArrayList<>();

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  void tearDown() {
    for (DateDayVector v : vectors) {
      v.close();
    }
    allocator.close();
  }

  @Test
  void fourFieldsMatchesJavaTimeReference() {
    IntPredicate[] patterns = {
        i -> true,                       // no nulls
        i -> i % 2 == 0,                 // alternating
        i -> i % 7 == 0,                 // sparse
        i -> i > 3 && i < 40,            // dense middle
        i -> i == 0 || i == 63,          // first and last
    };
    for (int n : SIZES) {
      for (IntPredicate pattern : patterns) {
        fourFieldsForPattern(n, pattern);
      }
    }
  }

  @Test
  void fourFieldsBoundaryValuesMatchJavaTimeReference() {
    long[] days = {
        NARROW_MIN_DAYS, NARROW_MIN_DAYS + 1, NARROW_MAX_DAYS - 1, NARROW_MAX_DAYS,
        -1, 0, 1,                                       // the epoch
        LocalDate.of(1, 1, 1).toEpochDay(),
        LocalDate.of(1, 3, 1).toEpochDay(),
        LocalDate.of(400, 1, 1).toEpochDay(),
        LocalDate.of(401, 1, 1).toEpochDay(),
        LocalDate.of(1900, 2, 28).toEpochDay(),         // not a leap year (div 100, not 400)
        LocalDate.of(1900, 3, 1).toEpochDay(),
        LocalDate.of(2000, 2, 29).toEpochDay(),         // leap year (div 400)
        LocalDate.of(2000, 3, 1).toEpochDay(),
        LocalDate.of(2100, 2, 28).toEpochDay(),         // not a leap year
        LocalDate.of(2400, 2, 29).toEpochDay(),         // leap year (div 400)
        LocalDate.of(2023, 12, 31).toEpochDay(),
        LocalDate.of(2024, 1, 1).toEpochDay(),
        LocalDate.of(9999, 12, 31).toEpochDay(),
        LocalDate.of(33134, 1, 1).toEpochDay(),
        LocalDate.of(-12800, 3, 1).toEpochDay(),
    };
    int n = days.length;
    DateDayVector v = newVector(n);
    for (int i = 0; i < n; i++) {
      v.set(i, (int) days[i]);
    }
    v.setValueCount(n);
    assertFourFields(v, n, 0);
  }

  private void fourFieldsForPattern(int n, IntPredicate validRows) {
    DateDayVector v = newVector(n);
    int nulls = 0;
    for (int i = 0; i < n; i++) {
      if (validRows.test(i)) {
        v.set(i, (int) value(i));
      } else {
        v.setNull(i);
        nulls++;
      }
    }
    v.setValueCount(n);
    assertFourFields(v, n, nulls);
  }

  /** A value spread pseudo-randomly across the whole narrowed range, deterministically. */
  private long value(int i) {
    return NARROW_MIN_DAYS + Math.floorMod((long) i * 500009L, NARROW_RANGE);
  }

  private void assertFourFields(DateDayVector v, int n, int nulls) {
    DateMorsel m = VarkaMorsel.extractDate(v, n);
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment dstYear = arena.allocate(n * 4L);
      MemorySegment dstMonth = arena.allocate(n * 4L);
      MemorySegment dstDay = arena.allocate(n * 4L);
      MemorySegment dstQuarter = arena.allocate(n * 4L);
      MemorySegment dstValidity = arena.allocate((n + 7) / 8L);
      long srcValidity = (nulls == 0 || nulls == n) ? 0L : m.validity().address();
      ChronoVectorOps.vectorFourFields(m.data().address(), srcValidity, nulls,
          dstYear.address(), dstMonth.address(), dstDay.address(), dstQuarter.address(),
          dstValidity.address(), n);
      for (int i = 0; i < n; i++) {
        boolean valid = !v.isNull(i);
        assertEquals(valid, isBitSet(dstValidity, i), "validity mismatch at row " + i);
        if (valid) {
          LocalDate expected = LocalDate.ofEpochDay(v.get(i));
          int expectedQuarter = (expected.getMonthValue() + 2) / 3;
          assertEquals(expected.getYear(), dstYear.get(ValueLayout.JAVA_INT, i * 4L),
              "year mismatch at row " + i + " for day " + v.get(i));
          assertEquals(expected.getMonthValue(), dstMonth.get(ValueLayout.JAVA_INT, i * 4L),
              "month mismatch at row " + i + " for day " + v.get(i));
          assertEquals(expected.getDayOfMonth(), dstDay.get(ValueLayout.JAVA_INT, i * 4L),
              "day-of-month mismatch at row " + i + " for day " + v.get(i));
          assertEquals(expectedQuarter, dstQuarter.get(ValueLayout.JAVA_INT, i * 4L),
              "quarter mismatch at row " + i + " for day " + v.get(i));
        }
      }
    }
  }

  private static boolean isBitSet(MemorySegment validity, int i) {
    return (validity.get(ValueLayout.JAVA_BYTE, i / 8L) & (1 << (i % 8))) != 0;
  }

  private DateDayVector newVector(int rowCount) {
    DateDayVector v = new DateDayVector("date", allocator);
    v.allocateNew(rowCount);
    vectors.add(v);
    return v;
  }
}
