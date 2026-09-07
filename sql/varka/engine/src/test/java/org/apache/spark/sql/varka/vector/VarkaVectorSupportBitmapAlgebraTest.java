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
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.Test;

/**
 * {@link VarkaVectorSupport#copyValidity}, {@link VarkaVectorSupport#andValidity} and
 * {@link VarkaVectorSupport#orValidity} (task 70), whose whole value is bit-exactness: each
 * produces exactly what the emitted loop produces when it ORs lane-masked words into a zeroed
 * bitmap, which is what {@code VarkaLoopEmitterSuite.assertSameOutput} compares byte for byte.
 * So every test here is about four things - the bits below {@code rows}, the bits past them in
 * the final byte, the byte after the bitmap, and that the operands come back unchanged - and one
 * more the loop never had to think about: the destination aliasing an operand, which is how a
 * nested word expression is evaluated into the destination without a scratch buffer.
 *
 * <p><b>The operands are sliced to exactly the bitmap.</b> Production maps every validity
 * operand at {@code (length + 7) / 8} bytes and nothing more, through
 * {@code VarkaVectorSupport.ofAddress}, so the {@link MemorySegment} bounds check is what stands
 * between a lowering that reads one byte too far and a neighbouring Arrow buffer. A test that
 * handed these a segment with slack would not fail on the over-read it exists to catch -
 * milestone 1's finding 1, in the one file where the reads are hand-written.
 */
public class VarkaVectorSupportBitmapAlgebraTest {

  private static final int[] ROWS = {0, 1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 1000, 4095, 4096};

  /** Independent operand contents per (rows, trial), so no length is judged on one draw. */
  private static final int TRIALS = 4;

  /** The byte written after the bitmap and into the destination before the call: any of it that
   *  survives where it should not, or is disturbed where it should not be, is visible. */
  private static final byte GUARD = (byte) 0xC3;

  private interface Op {
    void apply(MemorySegment dst, MemorySegment a, MemorySegment b, int rows);
  }

  private interface Expected {
    boolean of(boolean a, boolean b);
  }

  /**
   * A buffer of {@code bitmapBytes + 1} pseudo-random bytes: the bitmap, whose last byte has
   * bits set past {@code rows}, plus a guard byte after it.
   *
   * <p>The seed goes through splitmix64's finaliser first. {@code new Random(n)} for two small
   * {@code n} can yield the same first {@code nextInt(256)} - {@code new Random(4)} and
   * {@code new Random(7)} both start 187 - which at {@code rows <= 8} would make AND, OR and
   * copy indistinguishable on the one byte that matters.
   */
  private static MemorySegment random(Arena arena, int bitmapBytes, long seed) {
    MemorySegment seg = arena.allocate(bitmapBytes + 1L, 8);
    Random r = new Random(mix(seed));
    for (int i = 0; i < bitmapBytes; i++) {
      seg.set(ValueLayout.JAVA_BYTE, i, (byte) r.nextInt(256));
    }
    seg.set(ValueLayout.JAVA_BYTE, bitmapBytes, GUARD);
    return seg;
  }

  /** A destination of exactly the bitmap's size, so it is bounds-checked like production's. */
  private static MemorySegment bitmap(Arena arena, int bitmapBytes) {
    return arena.allocate(Math.max(bitmapBytes, 1L), 8).asSlice(0L, bitmapBytes);
  }

  private static long mix(long z) {
    z = (z + 0x9E3779B97F4A7C15L) * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
    return z ^ (z >>> 31);
  }

  private static boolean bit(MemorySegment seg, int i) {
    return (seg.get(ValueLayout.JAVA_BYTE, i / 8L) & (1 << (i % 8))) != 0;
  }

  private static byte[] bytes(MemorySegment seg, int n) {
    byte[] out = new byte[n];
    MemorySegment.copy(seg, ValueLayout.JAVA_BYTE, 0L, out, 0, n);
    return out;
  }

  private static void assertUnchanged(String what, byte[] before, MemorySegment seg, String at) {
    byte[] after = bytes(seg, before.length);
    for (int i = 0; i < before.length; i++) {
      assertEquals(before[i], after[i], what + " byte " + i + " was modified " + at);
    }
  }

  /**
   * The bit-exactness body: {@code dst} carries garbage going in, the operands are sliced to the
   * bitmap, and afterwards every bit below {@code rows} is the operation, every bit above it in
   * the final byte is zero, the guard byte past the bitmap is untouched, and both operands read
   * back exactly as they were written.
   */
  private static void check(String name, Op op, Expected expectedBit) {
    try (Arena arena = Arena.ofConfined()) {
      for (int rows : ROWS) {
        int bitmapBytes = (rows + 7) / 8;
        for (int trial = 0; trial < TRIALS; trial++) {
          long salt = rows * 64L + trial;
          MemorySegment aBuf = random(arena, bitmapBytes, salt * 3 + 1);
          MemorySegment bBuf = random(arena, bitmapBytes, salt * 5 + 2);
          String at = "at rows=" + rows + " trial=" + trial;
          if (bitmapBytes > 0) {
            assertFalse(Arrays.equals(bytes(aBuf, bitmapBytes), bytes(bBuf, bitmapBytes)),
                "the two operands drew identical bytes " + at + ", so this length proves nothing");
          }
          byte[] aBefore = bytes(aBuf, bitmapBytes + 1);
          byte[] bBefore = bytes(bBuf, bitmapBytes + 1);

          MemorySegment dstBuf = arena.allocate(bitmapBytes + 1L, 8);
          dstBuf.fill(GUARD);
          op.apply(dstBuf.asSlice(0L, bitmapBytes), aBuf.asSlice(0L, bitmapBytes),
              bBuf.asSlice(0L, bitmapBytes), rows);

          for (int i = 0; i < bitmapBytes * 8; i++) {
            boolean expected = i < rows && expectedBit.of(bit(aBuf, i), bit(bBuf, i));
            assertEquals(expected, bit(dstBuf, i), name + "(" + rows + "): bit " + i);
          }
          assertEquals(GUARD, dstBuf.get(ValueLayout.JAVA_BYTE, bitmapBytes),
              name + "(" + rows + ") wrote past the bitmap");
          assertUnchanged(name + " operand a", aBefore, aBuf, at);
          assertUnchanged(name + " operand b", bBefore, bBuf, at);
        }
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
    // The second operand is generated and ignored, so the same harness applies - and it doubles
    // as the assertion that copyValidity leaves the bitmap it did not read alone.
    check("copyValidity", (dst, a, b, rows) -> VarkaVectorSupport.copyValidity(dst, a, rows),
        (a, b) -> a);
  }

  /** The destination may be either operand: an expression such as OR(AND(d, m), d2) is evaluated
   *  inner-first into the destination, so the second step reads what the first wrote. */
  @Test
  public void aliasingTheDestinationWithAnOperandIsSafe() {
    try (Arena arena = Arena.ofConfined()) {
      for (int rows : ROWS) {
        int bitmapBytes = (rows + 7) / 8;
        MemorySegment a = random(arena, bitmapBytes, rows * 7L + 3).asSlice(0L, bitmapBytes);
        MemorySegment b = random(arena, bitmapBytes, rows * 11L + 4).asSlice(0L, bitmapBytes);
        MemorySegment reference = bitmap(arena, bitmapBytes);
        VarkaVectorSupport.andValidity(reference, a, b, rows);
        MemorySegment aliased = bitmap(arena, bitmapBytes);
        VarkaVectorSupport.copyValidity(aliased, a, rows);
        VarkaVectorSupport.andValidity(aliased, aliased, b, rows);
        for (int i = 0; i < bitmapBytes; i++) {
          assertEquals(reference.get(ValueLayout.JAVA_BYTE, i),
              aliased.get(ValueLayout.JAVA_BYTE, i), "byte " + i + " at rows=" + rows);
        }
        MemorySegment aliasedRight = bitmap(arena, bitmapBytes);
        VarkaVectorSupport.copyValidity(aliasedRight, b, rows);
        VarkaVectorSupport.orValidity(aliasedRight, a, aliasedRight, rows);
        MemorySegment referenceOr = bitmap(arena, bitmapBytes);
        VarkaVectorSupport.orValidity(referenceOr, a, b, rows);
        for (int i = 0; i < bitmapBytes; i++) {
          assertEquals(referenceOr.get(ValueLayout.JAVA_BYTE, i),
              aliasedRight.get(ValueLayout.JAVA_BYTE, i), "byte " + i + " at rows=" + rows);
        }
      }
    }
  }

  /**
   * Self-aliasing is the one case where an operand is written: {@code copyValidity(seg, seg,
   * rows)} keeps every bit below {@code rows} and clears the bits above them in the final byte.
   * The javadoc says so because a caller that expected a no-op over a sliced Arrow buffer would
   * be clearing the next slice's leading rows.
   */
  @Test
  public void copyingASegmentOntoItselfClearsOnlyTheBitsPastRows() {
    try (Arena arena = Arena.ofConfined()) {
      for (int rows : ROWS) {
        int bitmapBytes = (rows + 7) / 8;
        MemorySegment buf = random(arena, bitmapBytes, rows * 19L + 8);
        byte[] before = bytes(buf, bitmapBytes + 1);
        VarkaVectorSupport.copyValidity(buf.asSlice(0L, bitmapBytes),
            buf.asSlice(0L, bitmapBytes), rows);
        for (int i = 0; i < bitmapBytes * 8; i++) {
          boolean expected = i < rows && (before[i / 8] & (1 << (i % 8))) != 0;
          assertEquals(expected, bit(buf, i), "bit " + i + " at rows=" + rows);
        }
        assertEquals(GUARD, buf.get(ValueLayout.JAVA_BYTE, bitmapBytes),
            "self-copy wrote past the bitmap at rows=" + rows);
      }
    }
  }

  /**
   * What the emitted loop produces today: zero, then the lane-masked word of the root's validity
   * ORed in group by group, truncated to the batch. All three helpers must be indistinguishable
   * from it, not just the AND - the copy is the shape the pass will emit most.
   */
  @Test
  public void matchesTheLoopsOwnForm() {
    assertMatchesTheLoop("andValidity", VarkaVectorSupport::andValidity, (a, b) -> a && b);
    assertMatchesTheLoop("orValidity", VarkaVectorSupport::orValidity, (a, b) -> a || b);
    assertMatchesTheLoop("copyValidity",
        (dst, a, b, rows) -> VarkaVectorSupport.copyValidity(dst, a, rows), (a, b) -> a);
  }

  private static void assertMatchesTheLoop(String name, Op op, Expected expectedBit) {
    try (Arena arena = Arena.ofConfined()) {
      for (int rows : ROWS) {
        int bitmapBytes = (rows + 7) / 8;
        MemorySegment a = random(arena, bitmapBytes, rows * 13L + 5).asSlice(0L, bitmapBytes);
        MemorySegment b = random(arena, bitmapBytes, rows * 17L + 6).asSlice(0L, bitmapBytes);
        MemorySegment loop = bitmap(arena, bitmapBytes);
        VarkaVectorSupport.zero(loop);
        for (int i = 0; i < rows; i++) {
          if (expectedBit.of(bit(a, i), bit(b, i))) {
            VarkaVectorSupport.setBit(loop, i);
          }
        }
        MemorySegment pass = bitmap(arena, bitmapBytes);
        op.apply(pass, a, b, rows);
        for (int i = 0; i < bitmapBytes; i++) {
          assertEquals(loop.get(ValueLayout.JAVA_BYTE, i), pass.get(ValueLayout.JAVA_BYTE, i),
              name + ": byte " + i + " differs at rows=" + rows);
        }
      }
    }
  }
}
