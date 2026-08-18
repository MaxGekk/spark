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

package org.apache.spark.sql.varka.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.IntVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.varka.memory.VarkaMorsel.DateMorsel;

/**
 * Validates {@link VarkaMorsel}: every segment read-back is asserted against Arrow's own
 * {@code DateDayVector.get(i)} / {@code isNull(i)} accessors as the oracle.
 */
public class VarkaMorselTest {

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
  void alternatingNulls() {
    int n = 1000;
    DateDayVector v = newDateVector(n);
    for (int i = 0; i < n; i++) {
      if (i % 2 == 0) {
        v.set(i, i * 3 - 1000);
      } else {
        v.setNull(i);
      }
    }
    v.setValueCount(n);

    DateMorsel m = VarkaMorsel.extractDate(v, n);
    assertEquals(n, m.rowCount());
    assertEquals(500, m.nullCount());
    assertFalse(m.allNull());
    assertFalse(m.noNulls());
    assertNotNull(m.validity());

    assertReadback(m, v);
    VarkaMorsel.reportAlignment(m);
  }

  @Test
  void noNulls() {
    int n = 100;
    DateDayVector v = newDateVector(n);
    for (int i = 0; i < n; i++) {
      v.set(i, i + 42);
    }
    v.setValueCount(n);

    DateMorsel m = VarkaMorsel.extractDate(v, n);
    assertEquals(0, m.nullCount());
    assertTrue(m.noNulls());
    assertFalse(m.allNull());
    assertNotNull(m.validity());

    assertReadback(m, v);
  }

  @Test
  void allNull() {
    int n = 64;
    DateDayVector v = newDateVector(n);
    for (int i = 0; i < n; i++) {
      v.setNull(i);
    }
    v.setValueCount(n);

    DateMorsel m = VarkaMorsel.extractDate(v, n);
    assertEquals(n, m.nullCount());
    assertTrue(m.allNull());
    assertNull(m.validity());
  }

  @Test
  void empty() {
    DateDayVector v = newDateVector(0);
    v.setValueCount(0);

    DateMorsel m = VarkaMorsel.extractDate(v, 0);
    assertEquals(0, m.rowCount());
    assertEquals(0, m.nullCount());
    assertTrue(m.allNull());
    assertNull(m.validity());
    assertNotNull(m.data());
  }

  @Test
  void boundaryRowCounts() {
    int[] sizes = {1, 7, 8, 9, 13, 17, 64, 100, 1000};
    for (int n : sizes) {
      DateDayVector v = newDateVector(n);
      for (int i = 0; i < n; i++) {
        if (i % 3 == 0) {
          v.setNull(i);
        } else {
          v.set(i, -1000000 + i);
        }
      }
      v.setValueCount(n);

      DateMorsel m = VarkaMorsel.extractDate(v, n);
      long expectedNulls = (n + 2) / 3;
      assertEquals(expectedNulls, m.nullCount(), "nullCount mismatch for n=" + n);
      if (expectedNulls < n) {
        assertNotNull(m.validity(), "validity must be mapped for n=" + n);
      } else {
        assertNull(m.validity(), "validity must be null for all-null column n=" + n);
      }

      assertReadback(m, v);
    }
  }

  @Test
  void segmentSizing() {
    int n = 1000;
    DateDayVector v = newDateVector(n);
    for (int i = 0; i < n; i++) {
      if (i % 7 == 0) {
        v.setNull(i);
      } else {
        v.set(i, i);
      }
    }
    v.setValueCount(n);

    DateMorsel m = VarkaMorsel.extractDate(v, n);
    assertTrue(m.data().byteSize() >= n * 4L,
        "data segment must cover n*4 bytes");
    assertTrue(m.validity().byteSize() >= (n + 7) / 8L,
        "validity segment must cover (n+7)/8 bytes");
  }

  @Test
  void rowCountExceedsValueCount() {
    DateDayVector v = newDateVector(10);
    for (int i = 0; i < 10; i++) {
      v.set(i, i);
    }
    v.setValueCount(10);

    assertThrows(IllegalArgumentException.class, () -> VarkaMorsel.extractDate(v, 11));
  }

  @Test
  void negativeRowCountRejected() {
    DateDayVector v = newDateVector(5);
    v.setValueCount(5);

    assertThrows(IllegalArgumentException.class, () -> VarkaMorsel.extractDate(v, -1));
  }

  @Test
  void nonDateVectorRejected() {
    try (IntVector v = new IntVector("ints", allocator)) {
      v.allocateNew(1);
      v.setValueCount(1);

      assertThrows(IllegalArgumentException.class, () -> VarkaMorsel.extractDate(v, 1));
    }
  }

  /**
   * Asserts that the morsel segments agree with the Arrow vector for every row.
   */
  private static void assertReadback(DateMorsel m, DateDayVector v) {
    for (int i = 0; i < m.rowCount(); i++) {
      if (m.validity() == null) {
        assertTrue(v.isNull(i), "expected row " + i + " to be null in an all-null column");
      } else {
        assertEquals(v.isNull(i), VarkaMorsel.isNull(m.validity(), i),
            "validity mismatch at row " + i);
        if (!v.isNull(i)) {
          assertEquals(v.get(i), m.data().get(ValueLayout.JAVA_INT, (long) i * 4),
              "data mismatch at row " + i);
        }
      }
    }
  }

  private DateDayVector newDateVector(int rowCount) {
    DateDayVector v = new DateDayVector("date", allocator);
    v.allocateNew(rowCount);
    vectors.add(v);
    return v;
  }
}
