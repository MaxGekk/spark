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

import org.junit.jupiter.api.Test;

/**
 * Differential tests for {@link ChronoScalarOps} against {@code java.time.LocalDate}.
 *
 * <p>Both spellings of the algorithm are checked, and checked against each other: they exist to
 * be compared as a performance A/B, so a divergence between them would silently make that
 * comparison meaningless rather than fail anything.
 *
 * <p>Unlike the emitted lowering, this one claims exactness over <b>every</b> int32 day, so the
 * boundary set includes both ends of the int32 range - where {@code LocalDate} still has an
 * answer, since its own year range is far wider than anything an int32 epoch day can reach.
 */
public class ChronoScalarOpsTest {

  /** A stride that is coprime with the era length, so the walk does not land in phase with it. */
  private static final int STRIDE = 500009;

  @FunctionalInterface
  private interface ScalarYear {
    void run(long srcData, long dstData, long dstValidity, int length);
  }

  private static final ScalarYear[] KERNELS = {
      ChronoScalarOps::yearByDivision,
      ChronoScalarOps::yearByMagic,
  };

  private static final String[] NAMES = {"yearByDivision", "yearByMagic"};

  @Test
  void matchesJavaTimeOverTheSqlRange() {
    // Every day of years 1 to 9999 would be 3.65 million rows per kernel; a strided walk over
    // the same span with a coprime stride covers every era, century and leap pattern without
    // making the suite slow. The exhaustive sweep lives in the Python model this was derived
    // from (see ChronoScalarOps's class doc).
    long lo = LocalDate.of(1, 1, 1).toEpochDay();
    long hi = LocalDate.of(9999, 12, 31).toEpochDay();
    int n = 200000;
    long[] days = new long[n];
    for (int i = 0; i < n; i++) {
      days[i] = lo + Math.floorMod((long) i * STRIDE, hi - lo + 1);
    }
    assertYears(days);
  }

  @Test
  void matchesJavaTimeAtBoundaries() {
    long[] days = {
        Integer.MIN_VALUE, Integer.MIN_VALUE + 1, Integer.MAX_VALUE, Integer.MAX_VALUE - 1,
        -1, 0, 1,
        LocalDate.of(1, 1, 1).toEpochDay(),
        LocalDate.of(1, 3, 1).toEpochDay(),
        LocalDate.of(400, 1, 1).toEpochDay(),
        LocalDate.of(401, 1, 1).toEpochDay(),
        LocalDate.of(1900, 2, 28).toEpochDay(),
        LocalDate.of(1900, 3, 1).toEpochDay(),
        LocalDate.of(2000, 2, 29).toEpochDay(),
        LocalDate.of(2000, 3, 1).toEpochDay(),
        LocalDate.of(2100, 2, 28).toEpochDay(),
        LocalDate.of(2400, 2, 29).toEpochDay(),
        LocalDate.of(2023, 12, 31).toEpochDay(),
        LocalDate.of(2024, 1, 1).toEpochDay(),
        LocalDate.of(9999, 12, 31).toEpochDay(),
    };
    assertYears(days);
  }

  /** Every row length from 0 to 33, so the loop's entry and exit are exercised at every phase. */
  @Test
  void handlesEveryShortLength() {
    for (int n = 0; n <= 33; n++) {
      long[] days = new long[n];
      for (int i = 0; i < n; i++) {
        days[i] = LocalDate.of(2024, 1, 1).toEpochDay() + i * 37L;
      }
      assertYears(days);
    }
  }

  private void assertYears(long[] days) {
    int n = days.length;
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment src = arena.allocate(Math.max(1, n) * 4L);
      for (int i = 0; i < n; i++) {
        src.set(ValueLayout.JAVA_INT, i * 4L, (int) days[i]);
      }
      int[][] results = new int[KERNELS.length][n];
      for (int k = 0; k < KERNELS.length; k++) {
        MemorySegment dst = arena.allocate(Math.max(1, n) * 4L);
        MemorySegment validity = arena.allocate(Math.max(1, (n + 7) / 8L));
        KERNELS[k].run(src.address(), dst.address(), validity.address(), n);
        for (int i = 0; i < n; i++) {
          int actual = dst.get(ValueLayout.JAVA_INT, i * 4L);
          results[k][i] = actual;
          assertEquals(LocalDate.ofEpochDay(days[i]).getYear(), actual,
              NAMES[k] + ": year mismatch at row " + i + " for day " + days[i]);
        }
        for (int i = 0; i < n; i++) {
          assertEquals(true, VarkaVectorSupport.isBitSet(validity, i),
              NAMES[k] + ": row " + i + " should be valid");
        }
      }
      for (int i = 0; i < n; i++) {
        assertEquals(results[0][i], results[1][i],
            "the two spellings disagreed at row " + i + " for day " + days[i]
                + ", which makes the performance A/B meaningless");
      }
    }
  }
}
