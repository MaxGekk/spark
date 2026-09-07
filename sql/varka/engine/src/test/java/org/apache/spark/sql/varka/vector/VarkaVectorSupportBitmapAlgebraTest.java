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
import java.util.Random;

import org.junit.jupiter.api.Test;

/**
 * {@link VarkaVectorSupport#copyValidity}, {@link VarkaVectorSupport#andValidity} and
 * {@link VarkaVectorSupport#orValidity} (task 70), whose whole value is bit-exactness: each
 * produces exactly what the emitted loop produces when it ORs lane-masked words into a zeroed
 * bitmap, which is what {@code VarkaLoopEmitterSuite.assertSameOutput} compares byte for byte.
 * So every test here is about three things - the bits below {@code rows}, the bits past them in
 * the final byte, and the byte after the bitmap - and one more the loop never had to think about:
 * the destination aliasing an operand, which is how a nested word expression is evaluated into
 * the destination without a scratch buffer.
 */
public class VarkaVectorSupportBitmapAlgebraTest {

  private static final int[] ROWS = {0, 1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 1000, 4095, 4096};

  /** A bitmap with pseudo-random bits everywhere, including past {@code rows} in its last
   *  byte and in a guard byte after it, so garbage there is visible if it leaks. */
  private static MemorySegment random(Arena arena, int bitmapBytes, long seed) {
    MemorySegment seg = arena.allocate(bitmapBytes + 1L, 8);
    Random r = new Random(seed);
    for (int i = 0; i <= bitmapBytes; i++) {
      seg.set(ValueLayout.JAVA_BYTE, i, (byte) r.nextInt(256));
    }
    return seg;
  }

  private static boolean bit(MemorySegment seg, int i) {
    return (seg.get(ValueLayout.JAVA_BYTE, i / 8L) & (1 << (i % 8))) != 0;
  }

  private interface Op {
    void apply(MemorySegment dst, MemorySegment a, MemorySegment b, int rows);
  }

  private interface Expected {
    boolean of(boolean a, boolean b);
  }

  private static void check(String name, Op op, Expected expectedBit) {
    try (Arena arena = Arena.ofConfined()) {
      for (int rows : ROWS) {
        int bitmapBytes = (rows + 7) / 8;
        MemorySegment a = random(arena, bitmapBytes, rows * 3L + 1);
        MemorySegment b = random(arena, bitmapBytes, rows * 5L + 2);
        MemorySegment dst = arena.allocate(bitmapBytes + 1L, 8);
        dst.fill((byte) 0xFF);
        op.apply(dst.asSlice(0L, Math.max(bitmapBytes, 1)), a, b, rows);
        for (int i = 0; i < bitmapBytes * 8; i++) {
          boolean expected = i < rows && expectedBit.of(bit(a, i), bit(b, i));
          assertEquals(expected, bit(dst, i), name + "(" + rows + "): bit " + i);
        }
        assertEquals((byte) 0xFF, dst.get(ValueLayout.JAVA_BYTE, bitmapBytes),
            name + "(" + rows + ") wrote past the bitmap");
      }
    }
  }

  @Test
  public void andIsBitExactAndStopsAtRows() {
    check("andValidity", VarkaVectorSupport::andValidity, (a, b) -> a && b);
  }

  @Test
  public void orIsBitExactAndStopsAtRows() {
    check("orValidity", VarkaVectorSupport::orValidity, (a, b) -> a || b);
  }

  @Test
  public void copyIsBitExactAndStopsAtRows() {
    // The second operand is generated and ignored, so the same harness applies.
    check("copyValidity", (dst, a, b, rows) -> VarkaVectorSupport.copyValidity(dst, a, rows),
        (a, b) -> a);
  }

  /** The destination may be either operand: an expression such as OR(AND(d, m), d2) is evaluated
   *  inner-first into the destination, so the second step reads what the first wrote. */
  @Test
  public void aliasingTheDestinationWithAnOperandIsSafe() {
    try (Arena arena = Arena.ofConfined()) {
      for (int rows : ROWS) {
        int bitmapBytes = Math.max((rows + 7) / 8, 1);
        MemorySegment a = random(arena, bitmapBytes, rows * 7L + 3);
        MemorySegment b = random(arena, bitmapBytes, rows * 11L + 4);
        MemorySegment reference = arena.allocate(bitmapBytes, 8);
        VarkaVectorSupport.andValidity(reference, a, b, rows);
        MemorySegment aliased = arena.allocate(bitmapBytes, 8);
        VarkaVectorSupport.copyValidity(aliased, a, rows);
        VarkaVectorSupport.andValidity(aliased, aliased, b, rows);
        for (int i = 0; i < bitmapBytes; i++) {
          assertEquals(reference.get(ValueLayout.JAVA_BYTE, i),
              aliased.get(ValueLayout.JAVA_BYTE, i), "byte " + i + " at rows=" + rows);
        }
        MemorySegment aliasedRight = arena.allocate(bitmapBytes, 8);
        VarkaVectorSupport.copyValidity(aliasedRight, b, rows);
        VarkaVectorSupport.orValidity(aliasedRight, a, aliasedRight, rows);
        MemorySegment referenceOr = arena.allocate(bitmapBytes, 8);
        VarkaVectorSupport.orValidity(referenceOr, a, b, rows);
        for (int i = 0; i < bitmapBytes; i++) {
          assertEquals(referenceOr.get(ValueLayout.JAVA_BYTE, i),
              aliasedRight.get(ValueLayout.JAVA_BYTE, i), "byte " + i + " at rows=" + rows);
        }
      }
    }
  }

  /** What the emitted loop produces today for an AND root: zero, then the OR of the two inputs'
   *  words ANDed, group by group, truncated to the batch. The pass must be indistinguishable. */
  @Test
  public void matchesTheLoopsOwnForm() {
    try (Arena arena = Arena.ofConfined()) {
      for (int rows : ROWS) {
        int bitmapBytes = Math.max((rows + 7) / 8, 1);
        MemorySegment a = random(arena, bitmapBytes, rows * 13L + 5);
        MemorySegment b = random(arena, bitmapBytes, rows * 17L + 6);
        MemorySegment loop = arena.allocate(bitmapBytes, 8);
        VarkaVectorSupport.zero(loop);
        for (int i = 0; i < rows; i++) {
          if (bit(a, i) && bit(b, i)) {
            VarkaVectorSupport.setBit(loop, i);
          }
        }
        MemorySegment pass = arena.allocate(bitmapBytes, 8);
        VarkaVectorSupport.andValidity(pass, a, b, rows);
        for (int i = 0; i < bitmapBytes; i++) {
          assertEquals(loop.get(ValueLayout.JAVA_BYTE, i), pass.get(ValueLayout.JAVA_BYTE, i),
              "byte " + i + " differs at rows=" + rows);
        }
      }
    }
  }
}
