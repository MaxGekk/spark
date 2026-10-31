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

package org.apache.spark.sql;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaFusedKernel;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaLoopEmitter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSegments;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSuite;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.ArrowColumnVector;

import static org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_DAY;
import static org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MICROS;
import static org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND;
import static org.apache.spark.sql.catalyst.util.DateTimeConstants.SECONDS_PER_DAY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * What {@link VarkaDecimalStoreBenchmark} times, checked in every build at a small scale: its
 * hand-built {@code TIME} lowerings ({@code VARKA-157.md} 2.3) against Spark's row functions over
 * every second of the day, and its scalar widening against the bytes Arrow's own writer puts in a
 * slot. The benchmark checks the same on its rungs before it times them; this fails a pull request
 * rather than a runner dispatch.
 *
 * <p>Its subject is the benchmark's kernels, emitted at the defaults, so it pins the defaults. In
 * its own JVM, as the long-lane emitter suites are ({@code VarkaSpeciesGuard}).
 */
@VarkaSuite(ownJvm = true)
@Tag(VarkaSuite.PINS_DEFAULTS)
public final class VarkaDecimalStoreBenchmarkSuite {

  /**
   * Five rows a second of the day - its first microsecond, its last, one hashed between, a
   * hashed nanosecond, and the half microsecond before the next second, where {@code
   * time_to_seconds} rounds up - and the day's last microsecond once more, so the batch ends in a
   * partial lane group. The benchmark's column is a {@code TIME(6)}, at whole microseconds; the
   * two rows off the microsecond hold the lowerings to Spark at every {@code TIME} precision, the
   * {@code + 500} of {@code time_to_seconds} included.
   */
  private static long[] times() {
    int seconds = (int) SECONDS_PER_DAY;
    int perSecond = 5;
    long[] out = new long[perSecond * seconds + 1];
    for (int s = 0; s < seconds; s++) {
      long base = s * NANOS_PER_SECOND;
      long hash = s * 0x9E3779B97F4A7C15L;
      int at = perSecond * s;
      out[at] = base;
      out[at + 1] = base + NANOS_PER_SECOND - NANOS_PER_MICROS;
      out[at + 2] = base + Long.remainderUnsigned(hash, 1_000_000L) * NANOS_PER_MICROS;
      out[at + 3] = base + Long.remainderUnsigned(hash >>> 7, NANOS_PER_SECOND);
      out[at + 4] = base + NANOS_PER_SECOND - NANOS_PER_MICROS / 2;
    }
    out[perSecond * seconds] = NANOS_PER_DAY - NANOS_PER_MICROS;
    return out;
  }

  @Test
  @DisplayName("the benchmark's TIME lowerings give Spark's unscaled values over every second "
      + "of the day")
  void loweringsAgreeWithSpark() {
    long[] times = times();
    int n = times.length;
    var loader = new VarkaGeneratedClassLoader(getClass().getClassLoader());
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment column = arena.allocate(n * 8L, 64);
      MemorySegment.copy(times, 0, column, ValueLayout.JAVA_LONG, 0, n);
      MemorySegment dst = arena.allocate(n * 8L, 64);
      MemorySegment validity = arena.allocate((n + 63) / 64 * 8L, 64);
      var shapes = VarkaDecimalStoreBenchmark.shapes();
      for (int k = 0; k < shapes.size(); k++) {
        var shape = shapes.get(k);
        String name = "org.apache.spark.sql.varka.execution.VarkaDecimalStoreCheck" + k;
        VarkaFusedKernel kernel = VarkaBenchmarkBase.defineKernel(loader, name,
            VarkaLoopEmitter.emit(name, List.of(shape.root()), 1, shape.numLiterals(), null,
                null, VarkaEmitOptions.DEFAULTS));
        int status = kernel.run(new long[] {column.address()}, new long[] {0L}, new int[] {0},
            new long[] {dst.address()}, new long[] {validity.address()}, new int[0],
            VarkaDecimalStoreBenchmark.LONG_LITS, n);
        assertEquals(0, status, shape.name() + " declined the batch");
        long[] want = new long[n];
        for (int i = 0; i < n; i++) {
          want[i] = shape.spark().applyAsLong(times[i]);
        }
        assertArrayEquals(want, dst.toArray(ValueLayout.JAVA_LONG), shape.name());
      }
    } finally {
      loader.release();
    }
  }

  @Test
  @DisplayName("the benchmark's scalar widening writes the slots DecimalVector.set writes")
  void wideningWritesArrowsSlots() {
    var values = new ArrayList<Long>(List.of(0L, 1L, -1L, 999_999_999_999_999_999L,
        -999_999_999_999_999_999L, Long.MAX_VALUE, Long.MIN_VALUE));
    var random = new Random(157L);
    for (int k = 0; k < 57; k++) {
      values.add(random.nextLong() % 1_000_000_000_000_000_000L);
    }
    int n = values.size();
    try (Arena arena = Arena.ofConfined();
         BufferAllocator allocator = ArrowUtils.rootAllocator()
             .newChildAllocator("varka-decimal-store-check", 0, Long.MAX_VALUE);
         DecimalVector widened = new DecimalVector("w", allocator, 18, 0);
         DecimalVector arrow = new DecimalVector("a", allocator, 18, 0)) {
      MemorySegment longs = arena.allocate(n * 8L, 64);
      widened.allocateNew(n);
      arrow.allocateNew(n);
      for (int i = 0; i < n; i++) {
        longs.set(ValueLayout.JAVA_LONG, i * 8L, values.get(i));
        arrow.set(i, values.get(i));
        widened.setIndexDefined(i);
      }
      widened.setValueCount(n);
      arrow.setValueCount(n);
      VarkaDecimalStoreBenchmark.widen(longs,
          VarkaSegments.map(widened.getDataBuffer().memoryAddress(), n * 16L), n);
      byte[] got = new byte[n * 16];
      byte[] want = new byte[n * 16];
      widened.getDataBuffer().getBytes(0, got);
      arrow.getDataBuffer().getBytes(0, want);
      assertArrayEquals(want, got);
      var reader = new ArrowColumnVector(widened);
      for (int i = 0; i < n; i++) {
        long v = values.get(i);
        // The reader reads only values within the vector's 18 digits; the two long edges are
        // checked by their bytes above.
        if (v > -1_000_000_000_000_000_000L && v < 1_000_000_000_000_000_000L) {
          assertEquals(v, reader.getDecimal(i, 18, 0).toUnscaledLong(), "slot " + i);
        }
      }
    }
  }
}
