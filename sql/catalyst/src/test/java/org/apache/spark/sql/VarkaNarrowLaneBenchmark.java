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
import java.lang.management.ManagementFactory;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.ObjectName;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitterTestSupport;

/**
 * What a {@code TINYINT} or {@code SMALLINT} column costs to compare in a Vector API loop under
 * the two ways a narrow lane can be carried, written by hand with no emitter (VARKA-329's
 * spike, which chooses the emitter's shape on a measurement before the emitter is touched).
 *
 * <p>The two arms of {@code b1 < b2}, written to a selection word, one bit a row, 64 rows to a
 * {@code long}:
 * <ul>
 *   <li>WIDENED: the column is read through a byte (short) species a quarter (half) as wide as
 *       the int species and converted to the int species, and the compare is an int compare. At
 *       128 bits the matching byte species would be 32 bits, which the Vector API does not have,
 *       so the load is 64 bits wide and converted in two parts; a second arm, WIDENED128, reads
 *       128 bits and converts in four.</li>
 *   <li>NATIVE: the column is read at the preferred byte (short) species, the compare is a byte
 *       (short) compare, and the mask is one {@code toLong} per group of rows.</li>
 * </ul>
 * Beside them an INT arm of the same predicate over {@code INT} columns (what a narrow column
 * costs against the int the engine has) and a SCALAR loop that packs the same word. Every arm's
 * words are checked against the scalar loop's before it is timed.
 *
 * <p>Two mixed shapes are int-driven, a narrow column against an int column. For {@code b < i}
 * the two arms are the same operations by construction (the column is widened at its load or at
 * the compare, and nothing is computed in the byte lane between), so only WIDENED is timed, and
 * the shape stands for both. For {@code greatest(b1, b2) < i} the byte maximum is computed in the
 * part-width byte species before the widening under NATIVE and after it under WIDENED, which is
 * where the two differ.
 *
 * <p>Every arm is driven to compiled code at its own turn, just before it is timed: at least
 * eight drives, then two consecutive drives within 30 percent, C2 idle after each, and a loop
 * method that {@code Compiler.codelist} shows at level 4 (VARKA-407's rule). An arm that does not
 * settle in 30 seconds says so on the console and is timed as it is. Row counts are multiples of
 * 64, so no loop has a tail: this measures the loops, not the epilogue.
 *
 * <p>Run it with {@code -XX:CompileCommand=dontinline,<this class>::loop*} so that each loop is
 * a compile unit of its own, at {@code -XX:MaxVectorSize=64}, {@code 32} and {@code 16} for the
 * three widths. {@code -Dvarka.narrow.arms=widened,native,int,scalar} (a subset)
 * keeps the second byte species of one arm out of the JVM of the other.
 *
 * <pre>
 *   build/sbt catalyst/Test/runMain org.apache.spark.sql.VarkaNarrowLaneBenchmark
 * </pre>
 */
public class VarkaNarrowLaneBenchmark extends VarkaBenchmarkBase {

  /** A rung of the ladder: a row count and what it is for. */
  private record Rung(int numRows, String level) { }

  /** The rungs, named by the bytes of a pair of columns, which is what a cache level holds. */
  private static final List<Rung> LADDER = List.of(
      new Rung(16384, "pairs of 32 KB to 128 KB"),
      new Rung(262144, "pairs of 512 KB to 2 MB"),
      new Rung(1000000, "pairs of 2 MB to 8 MB"),
      new Rung(8388608, "pairs of 16 MB to 64 MB"));

  /** Rows a timed iteration covers, so that one is long enough for a millisecond clock. */
  private static final long ROWS_PER_ITERATION = 1L << 27;

  private static final int MIN_WARM_DRIVES = 8;
  private static final double STEADY_FRACTION = 0.3;
  private static final long WARM_WAIT_MILLIS = 30_000;
  private static final long IDLE_WAIT_MILLIS = 10_000;

  private static final String ARMS_PROPERTY = "varka.narrow.arms";

  private static final ByteOrder ORDER = ByteOrder.nativeOrder();

  private static final VectorSpecies<Integer> IS = IntVector.SPECIES_PREFERRED;
  private static final int IL = IS.length();

  // ---------------------------------------------------------------------------------------
  // The species each arm uses. Each sits in a holder so that a JVM that runs only some arms
  // defines only their species.
  // ---------------------------------------------------------------------------------------

  /** NATIVE: the preferred byte and short species. */
  private static final class Nat {
    static final VectorSpecies<Byte> B = ByteVector.SPECIES_PREFERRED;
    static final VectorSpecies<Short> S = ShortVector.SPECIES_PREFERRED;
    static final int LB = B.length();
    static final int LS = S.length();
    static final int GB = 64 / LB;
    static final int GS = 64 / LS;
  }

  /** WIDENED: a byte (short) species a quarter (half) of the int species' width, 64 bits least. */
  private static final class Wid {
    static final VectorSpecies<Byte> B = VectorSpecies.of(byte.class,
        VectorShape.forBitSize(Math.max(64, IS.vectorBitSize() / 4)));
    static final VectorSpecies<Short> S = VectorSpecies.of(short.class,
        VectorShape.forBitSize(Math.max(64, IS.vectorBitSize() / 2)));
    static final int RB = B.length();
    static final int RS = S.length();
    static final int PB = RB / IL;
    static final int PS = RS / IL;
    static final int GB = 64 / RB;
    static final int GS = 64 / RS;
  }

  /** WIDENED128: a 128-bit byte species converted in {@code 16 / IL} parts. */
  private static final class Wid128 {
    static final VectorSpecies<Byte> B = ByteVector.SPECIES_128;
    static final int RB = B.length();
    static final int PB = RB / IL;
    static final int GB = 64 / RB;
  }

  private static final int GI = 64 / IL;

  // ---------------------------------------------------------------------------------------
  // The loops. Each has the signature of Loop: a column, a second column, a third, the output
  // words, and the row count (a multiple of 64).
  // ---------------------------------------------------------------------------------------

  /** A loop over columns {@code a}, {@code b} and {@code c} into {@code out}. */
  @FunctionalInterface
  private interface Loop {
    void apply(MemorySegment a, MemorySegment b, MemorySegment c, long[] out, int n);
  }

  static void loopNativeByte(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int p = 0; p < Nat.GB; p++) {
        int at = i + p * Nat.LB;
        word |= ByteVector.fromMemorySegment(Nat.B, a, at, ORDER)
            .compare(VectorOperators.LT, ByteVector.fromMemorySegment(Nat.B, b, at, ORDER))
            .toLong() << (p * Nat.LB);
      }
      out[w] = word;
    }
  }

  static void loopNativeShort(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int p = 0; p < Nat.GS; p++) {
        int at = i + p * Nat.LS;
        word |= ShortVector.fromMemorySegment(Nat.S, a, at * 2L, ORDER)
            .compare(VectorOperators.LT, ShortVector.fromMemorySegment(Nat.S, b, at * 2L, ORDER))
            .toLong() << (p * Nat.LS);
      }
      out[w] = word;
    }
  }

  static void loopWidenedByte(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int g = 0; g < Wid.GB; g++) {
        int at = i + g * Wid.RB;
        var x = ByteVector.fromMemorySegment(Wid.B, a, at, ORDER);
        var y = ByteVector.fromMemorySegment(Wid.B, b, at, ORDER);
        for (int p = 0; p < Wid.PB; p++) {
          var xi = (IntVector) x.convertShape(VectorOperators.B2I, IS, p);
          var yi = (IntVector) y.convertShape(VectorOperators.B2I, IS, p);
          word |= xi.compare(VectorOperators.LT, yi).toLong() << (g * Wid.RB + p * IL);
        }
      }
      out[w] = word;
    }
  }

  static void loopWidenedByte128(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int g = 0; g < Wid128.GB; g++) {
        int at = i + g * Wid128.RB;
        var x = ByteVector.fromMemorySegment(Wid128.B, a, at, ORDER);
        var y = ByteVector.fromMemorySegment(Wid128.B, b, at, ORDER);
        for (int p = 0; p < Wid128.PB; p++) {
          var xi = (IntVector) x.convertShape(VectorOperators.B2I, IS, p);
          var yi = (IntVector) y.convertShape(VectorOperators.B2I, IS, p);
          word |= xi.compare(VectorOperators.LT, yi).toLong() << (g * Wid128.RB + p * IL);
        }
      }
      out[w] = word;
    }
  }

  static void loopWidenedShort(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int g = 0; g < Wid.GS; g++) {
        int at = i + g * Wid.RS;
        var x = ShortVector.fromMemorySegment(Wid.S, a, at * 2L, ORDER);
        var y = ShortVector.fromMemorySegment(Wid.S, b, at * 2L, ORDER);
        for (int p = 0; p < Wid.PS; p++) {
          var xi = (IntVector) x.convertShape(VectorOperators.S2I, IS, p);
          var yi = (IntVector) y.convertShape(VectorOperators.S2I, IS, p);
          word |= xi.compare(VectorOperators.LT, yi).toLong() << (g * Wid.RS + p * IL);
        }
      }
      out[w] = word;
    }
  }

  static void loopInt(MemorySegment a, MemorySegment b, MemorySegment c, long[] out, int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int g = 0; g < GI; g++) {
        long at = (long) (i + g * IL) * 4L;
        word |= IntVector.fromMemorySegment(IS, a, at, ORDER)
            .compare(VectorOperators.LT, IntVector.fromMemorySegment(IS, b, at, ORDER))
            .toLong() << (g * IL);
      }
      out[w] = word;
    }
  }

  /** {@code b < i}: the byte column widened by the part-width species, compared to the int one. */
  static void loopMixedByte(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int g = 0; g < Wid.GB; g++) {
        int at = i + g * Wid.RB;
        var x = ByteVector.fromMemorySegment(Wid.B, a, at, ORDER);
        for (int p = 0; p < Wid.PB; p++) {
          var xi = (IntVector) x.convertShape(VectorOperators.B2I, IS, p);
          var iv = IntVector.fromMemorySegment(IS, c, (long) (at + p * IL) * 4L, ORDER);
          word |= xi.compare(VectorOperators.LT, iv).toLong() << (g * Wid.RB + p * IL);
        }
      }
      out[w] = word;
    }
  }

  static void loopMixedShort(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int g = 0; g < Wid.GS; g++) {
        int at = i + g * Wid.RS;
        var x = ShortVector.fromMemorySegment(Wid.S, a, at * 2L, ORDER);
        for (int p = 0; p < Wid.PS; p++) {
          var xi = (IntVector) x.convertShape(VectorOperators.S2I, IS, p);
          var iv = IntVector.fromMemorySegment(IS, c, (long) (at + p * IL) * 4L, ORDER);
          word |= xi.compare(VectorOperators.LT, iv).toLong() << (g * Wid.RS + p * IL);
        }
      }
      out[w] = word;
    }
  }

  /** {@code greatest(b1, b2) < i}, widened first and the maximum taken on ints. */
  static void loopMaxWidenedByte(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int g = 0; g < Wid.GB; g++) {
        int at = i + g * Wid.RB;
        var x = ByteVector.fromMemorySegment(Wid.B, a, at, ORDER);
        var y = ByteVector.fromMemorySegment(Wid.B, b, at, ORDER);
        for (int p = 0; p < Wid.PB; p++) {
          var xi = (IntVector) x.convertShape(VectorOperators.B2I, IS, p);
          var yi = (IntVector) y.convertShape(VectorOperators.B2I, IS, p);
          var iv = IntVector.fromMemorySegment(IS, c, (long) (at + p * IL) * 4L, ORDER);
          word |= xi.lanewise(VectorOperators.MAX, yi).compare(VectorOperators.LT, iv).toLong()
              << (g * Wid.RB + p * IL);
        }
      }
      out[w] = word;
    }
  }

  /** {@code greatest(b1, b2) < i}, the maximum taken in the byte species, then widened. */
  static void loopMaxNativeByte(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int g = 0; g < Wid.GB; g++) {
        int at = i + g * Wid.RB;
        var m = ByteVector.fromMemorySegment(Wid.B, a, at, ORDER).lanewise(VectorOperators.MAX,
            ByteVector.fromMemorySegment(Wid.B, b, at, ORDER));
        for (int p = 0; p < Wid.PB; p++) {
          var mi = (IntVector) m.convertShape(VectorOperators.B2I, IS, p);
          var iv = IntVector.fromMemorySegment(IS, c, (long) (at + p * IL) * 4L, ORDER);
          word |= mi.compare(VectorOperators.LT, iv).toLong() << (g * Wid.RB + p * IL);
        }
      }
      out[w] = word;
    }
  }

  static void loopMaxWidenedShort(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int g = 0; g < Wid.GS; g++) {
        int at = i + g * Wid.RS;
        var x = ShortVector.fromMemorySegment(Wid.S, a, at * 2L, ORDER);
        var y = ShortVector.fromMemorySegment(Wid.S, b, at * 2L, ORDER);
        for (int p = 0; p < Wid.PS; p++) {
          var xi = (IntVector) x.convertShape(VectorOperators.S2I, IS, p);
          var yi = (IntVector) y.convertShape(VectorOperators.S2I, IS, p);
          var iv = IntVector.fromMemorySegment(IS, c, (long) (at + p * IL) * 4L, ORDER);
          word |= xi.lanewise(VectorOperators.MAX, yi).compare(VectorOperators.LT, iv).toLong()
              << (g * Wid.RS + p * IL);
        }
      }
      out[w] = word;
    }
  }

  static void loopMaxNativeShort(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int g = 0; g < Wid.GS; g++) {
        int at = i + g * Wid.RS;
        var m = ShortVector.fromMemorySegment(Wid.S, a, at * 2L, ORDER).lanewise(
            VectorOperators.MAX, ShortVector.fromMemorySegment(Wid.S, b, at * 2L, ORDER));
        for (int p = 0; p < Wid.PS; p++) {
          var mi = (IntVector) m.convertShape(VectorOperators.S2I, IS, p);
          var iv = IntVector.fromMemorySegment(IS, c, (long) (at + p * IL) * 4L, ORDER);
          word |= mi.compare(VectorOperators.LT, iv).toLong() << (g * Wid.RS + p * IL);
        }
      }
      out[w] = word;
    }
  }

  // The scalar loops: what a row engine does per row, packing the same word.

  static void loopScalarByte(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int k = 0; k < 64; k++) {
        word |= (a.get(ValueLayout.JAVA_BYTE, i + k) < b.get(ValueLayout.JAVA_BYTE, i + k)
            ? 1L : 0L) << k;
      }
      out[w] = word;
    }
  }

  static void loopScalarShort(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int k = 0; k < 64; k++) {
        long at = (i + k) * 2L;
        word |= (a.get(ValueLayout.JAVA_SHORT, at) < b.get(ValueLayout.JAVA_SHORT, at)
            ? 1L : 0L) << k;
      }
      out[w] = word;
    }
  }

  static void loopScalarInt(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int k = 0; k < 64; k++) {
        long at = (i + k) * 4L;
        word |= (a.get(ValueLayout.JAVA_INT, at) < b.get(ValueLayout.JAVA_INT, at)
            ? 1L : 0L) << k;
      }
      out[w] = word;
    }
  }

  static void loopScalarMixedByte(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int k = 0; k < 64; k++) {
        word |= (a.get(ValueLayout.JAVA_BYTE, i + k) < c.get(ValueLayout.JAVA_INT, (i + k) * 4L)
            ? 1L : 0L) << k;
      }
      out[w] = word;
    }
  }

  static void loopScalarMixedShort(MemorySegment a, MemorySegment b, MemorySegment c,
      long[] out, int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int k = 0; k < 64; k++) {
        word |= (a.get(ValueLayout.JAVA_SHORT, (i + k) * 2L)
            < c.get(ValueLayout.JAVA_INT, (i + k) * 4L) ? 1L : 0L) << k;
      }
      out[w] = word;
    }
  }

  static void loopScalarMaxByte(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int k = 0; k < 64; k++) {
        int m = Math.max(a.get(ValueLayout.JAVA_BYTE, i + k), b.get(ValueLayout.JAVA_BYTE, i + k));
        word |= (m < c.get(ValueLayout.JAVA_INT, (i + k) * 4L) ? 1L : 0L) << k;
      }
      out[w] = word;
    }
  }

  static void loopScalarMaxShort(MemorySegment a, MemorySegment b, MemorySegment c, long[] out,
      int n) {
    for (int i = 0, w = 0; i < n; i += 64, w++) {
      long word = 0;
      for (int k = 0; k < 64; k++) {
        long at = (i + k) * 2L;
        int m = Math.max(a.get(ValueLayout.JAVA_SHORT, at), b.get(ValueLayout.JAVA_SHORT, at));
        word |= (m < c.get(ValueLayout.JAVA_INT, (i + k) * 4L) ? 1L : 0L) << k;
      }
      out[w] = word;
    }
  }

  // ---------------------------------------------------------------------------------------
  // The arms and the data
  // ---------------------------------------------------------------------------------------

  /** Which columns of the rung an arm reads. */
  private enum Columns { BYTES, SHORTS, INTS }

  /**
   * One arm: the group it is selected by ({@code widened}, {@code native}, {@code int} or {@code
   * scalar}), its case name, the loop method's name (for the code list), the loop, and the
   * columns it reads. {@code reference} is the scalar arm of the same shape that checks it.
   */
  private record Arm(String group, String label, String method, Loop loop, Columns columns,
      String reference) { }

  /** The columns of one rung. */
  private record Data(int numRows, MemorySegment bytes1, MemorySegment bytes2,
      MemorySegment shorts1, MemorySegment shorts2, MemorySegment ints1, MemorySegment ints2,
      MemorySegment smallInts) {

    MemorySegment[] of(Columns c) {
      return switch (c) {
        case BYTES -> new MemorySegment[] {bytes1, bytes2, smallInts};
        case SHORTS -> new MemorySegment[] {shorts1, shorts2, smallInts};
        case INTS -> new MemorySegment[] {ints1, ints2, smallInts};
      };
    }
  }

  private static MemorySegment random(Arena arena, long bytes, long seed) {
    MemorySegment seg = arena.allocate(bytes, 64);
    var r = new SplittableRandom(seed);
    for (long off = 0; off < bytes; off += 8) {
      seg.set(ValueLayout.JAVA_LONG, off, r.nextLong());
    }
    return seg;
  }

  private static MemorySegment smallInts(Arena arena, int n, long seed) {
    MemorySegment seg = arena.allocate(n * 4L, 64);
    var r = new SplittableRandom(seed);
    for (int i = 0; i < n; i++) {
      seg.set(ValueLayout.JAVA_INT, i * 4L, r.nextInt(-200, 200));
    }
    return seg;
  }

  private static Data data(Arena arena, int n) {
    return new Data(n, random(arena, n, 1), random(arena, n, 2), random(arena, n * 2L, 3),
        random(arena, n * 2L, 4), random(arena, n * 4L, 5), random(arena, n * 4L, 6),
        smallInts(arena, n, 7));
  }

  private static List<Arm> arms() {
    var arms = new ArrayList<Arm>();
    // Shape 1: b1 < b2.
    arms.add(new Arm("widened", "TINYINT b1 < b2, WIDENED", "loopWidenedByte",
        VarkaNarrowLaneBenchmark::loopWidenedByte, Columns.BYTES, "t1"));
    if (Math.max(64, IS.vectorBitSize() / 4) != 128) {
      arms.add(new Arm("widened", "TINYINT b1 < b2, WIDENED 128-bit loads", "loopWidenedByte128",
          VarkaNarrowLaneBenchmark::loopWidenedByte128, Columns.BYTES, "t1"));
    }
    arms.add(new Arm("native", "TINYINT b1 < b2, NATIVE", "loopNativeByte",
        VarkaNarrowLaneBenchmark::loopNativeByte, Columns.BYTES, "t1"));
    arms.add(new Arm("scalar", "TINYINT b1 < b2, scalar", "loopScalarByte",
        VarkaNarrowLaneBenchmark::loopScalarByte, Columns.BYTES, "t1"));
    arms.add(new Arm("widened", "SMALLINT s1 < s2, WIDENED", "loopWidenedShort",
        VarkaNarrowLaneBenchmark::loopWidenedShort, Columns.SHORTS, "s1"));
    arms.add(new Arm("native", "SMALLINT s1 < s2, NATIVE", "loopNativeShort",
        VarkaNarrowLaneBenchmark::loopNativeShort, Columns.SHORTS, "s1"));
    arms.add(new Arm("scalar", "SMALLINT s1 < s2, scalar", "loopScalarShort",
        VarkaNarrowLaneBenchmark::loopScalarShort, Columns.SHORTS, "s1"));
    arms.add(new Arm("int", "INT i1 < i2", "loopInt", VarkaNarrowLaneBenchmark::loopInt,
        Columns.INTS, "i1"));
    arms.add(new Arm("scalar", "INT i1 < i2, scalar", "loopScalarInt",
        VarkaNarrowLaneBenchmark::loopScalarInt, Columns.INTS, "i1"));
    // Shape 2: b < i, int-driven; both arms are these operations.
    arms.add(new Arm("widened", "TINYINT b < i (WIDENED, and NATIVE: the same operations)",
        "loopMixedByte", VarkaNarrowLaneBenchmark::loopMixedByte, Columns.BYTES, "tm"));
    arms.add(new Arm("scalar", "TINYINT b < i, scalar", "loopScalarMixedByte",
        VarkaNarrowLaneBenchmark::loopScalarMixedByte, Columns.BYTES, "tm"));
    arms.add(new Arm("widened", "SMALLINT s < i (WIDENED, and NATIVE: the same operations)",
        "loopMixedShort", VarkaNarrowLaneBenchmark::loopMixedShort, Columns.SHORTS, "sm"));
    arms.add(new Arm("scalar", "SMALLINT s < i, scalar", "loopScalarMixedShort",
        VarkaNarrowLaneBenchmark::loopScalarMixedShort, Columns.SHORTS, "sm"));
    // Shape 3: greatest(b1, b2) < i.
    arms.add(new Arm("widened", "TINYINT greatest(b1, b2) < i, WIDENED", "loopMaxWidenedByte",
        VarkaNarrowLaneBenchmark::loopMaxWidenedByte, Columns.BYTES, "tx"));
    arms.add(new Arm("native", "TINYINT greatest(b1, b2) < i, NATIVE", "loopMaxNativeByte",
        VarkaNarrowLaneBenchmark::loopMaxNativeByte, Columns.BYTES, "tx"));
    arms.add(new Arm("scalar", "TINYINT greatest(b1, b2) < i, scalar", "loopScalarMaxByte",
        VarkaNarrowLaneBenchmark::loopScalarMaxByte, Columns.BYTES, "tx"));
    arms.add(new Arm("widened", "SMALLINT greatest(s1, s2) < i, WIDENED", "loopMaxWidenedShort",
        VarkaNarrowLaneBenchmark::loopMaxWidenedShort, Columns.SHORTS, "sx"));
    arms.add(new Arm("native", "SMALLINT greatest(s1, s2) < i, NATIVE", "loopMaxNativeShort",
        VarkaNarrowLaneBenchmark::loopMaxNativeShort, Columns.SHORTS, "sx"));
    arms.add(new Arm("scalar", "SMALLINT greatest(s1, s2) < i, scalar", "loopScalarMaxShort",
        VarkaNarrowLaneBenchmark::loopScalarMaxShort, Columns.SHORTS, "sx"));
    return arms;
  }

  /** The scalar loop whose words check every arm of one reference shape. */
  private static Loop referenceLoop(String reference) {
    return switch (reference) {
      case "t1" -> VarkaNarrowLaneBenchmark::loopScalarByte;
      case "s1" -> VarkaNarrowLaneBenchmark::loopScalarShort;
      case "i1" -> VarkaNarrowLaneBenchmark::loopScalarInt;
      case "tm" -> VarkaNarrowLaneBenchmark::loopScalarMixedByte;
      case "sm" -> VarkaNarrowLaneBenchmark::loopScalarMixedShort;
      case "tx" -> VarkaNarrowLaneBenchmark::loopScalarMaxByte;
      case "sx" -> VarkaNarrowLaneBenchmark::loopScalarMaxShort;
      default -> throw new IllegalArgumentException("no reference named " + reference);
    };
  }

  private static Set<String> selectedGroups() {
    return Set.of(System.getProperty(ARMS_PROPERTY, "widened,native,int,scalar").split(","));
  }

  // ---------------------------------------------------------------------------------------
  // Driving an arm to compiled code
  // ---------------------------------------------------------------------------------------

  /**
   * The highest tier of {@code method}'s code in use according to {@code Compiler.codelist}, 0 if
   * it has none, -1 if the list cannot be read. An on-stack-replacement version is not marked as
   * one in the list, and a loop method is shared by every rung, so from the second rung on this
   * is true on arrival: the steady drive times are what show a rung's loop is settled.
   */
  private static int levelInUse(String method) {
    try {
      Object list = ManagementFactory.getPlatformMBeanServer().invoke(
          new ObjectName("com.sun.management:type=DiagnosticCommand"), "compilerCodelist",
          new Object[] {null}, new String[] {String[].class.getName()});
      String prefix = VarkaNarrowLaneBenchmark.class.getName() + "." + method + "(";
      int level = 0;
      for (String line : String.valueOf(list).split("\\R")) {
        // "<id> <tier> <state> <class>.<method><descriptor> [<addresses>]"; state 0 is in use.
        String[] fields = line.strip().split(" ", 5);
        if (fields.length >= 4 && fields[3].startsWith(prefix) && "0".equals(fields[2])) {
          level = Math.max(level, Integer.parseInt(fields[1]));
        }
      }
      return level;
    } catch (Exception | LinkageError e) {
      return -1;
    }
  }

  private static void drive(Arm arm, MemorySegment[] cols, long[] out, int numRows, int reps) {
    for (int r = 0; r < reps; r++) {
      arm.loop().apply(cols[0], cols[1], cols[2], out, numRows);
    }
  }

  /** Drives {@code arm} until its loop method is level-4 code and its drive time is steady. */
  private static void driveToCompiledCode(Arm arm, MemorySegment[] cols, long[] out,
      int numRows, int reps) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WARM_WAIT_MILLIS);
    long previous = 0;
    int drives = 0;
    while (true) {
      long start = System.nanoTime();
      drive(arm, cols, out, numRows, reps);
      long took = System.nanoTime() - start;
      drives++;
      boolean idle = VarkaEmitterTestSupport.awaitC2Idle(IDLE_WAIT_MILLIS);
      int level = levelInUse(arm.method());
      boolean steady = previous > 0 && Math.abs(took - previous) <= previous * STEADY_FRACTION;
      if (drives >= MIN_WARM_DRIVES && idle && steady && (level == 4 || level == -1)) {
        System.out.println("VarkaNarrowLaneBenchmark: " + arm.label() + " at " + numRows
            + " rows settled after " + drives + " drives, level " + level);
        return;
      }
      if (System.nanoTime() > deadline) {
        System.out.println("VarkaNarrowLaneBenchmark: " + arm.label() + " at " + numRows
            + " rows did not settle in " + WARM_WAIT_MILLIS + " ms (" + drives + " drives, last "
            + TimeUnit.NANOSECONDS.toMicros(took) + " us after "
            + TimeUnit.NANOSECONDS.toMicros(previous) + " us, level " + level + ", C2 "
            + (idle ? "idle" : "busy") + "); it is timed as it is");
        return;
      }
      previous = took;
    }
  }

  // ---------------------------------------------------------------------------------------
  // The benchmark
  // ---------------------------------------------------------------------------------------

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    var groups = selectedGroups();
    var arms = arms().stream().filter(a -> groups.contains(a.group())).toList();
    section("narrow-lane hand loops at " + IS.vectorBitSize() + " bits, arms " + groups
        .stream().sorted().toList(), () -> {
      for (Rung rung : LADDER) {
        runRung(rung, arms);
      }
    });
  }

  private void runRung(Rung rung, List<Arm> arms) {
    int n = rung.numRows();
    require(n % 64 == 0, n + " rows is not a multiple of 64");
    int reps = (int) Math.max(1, ROWS_PER_ITERATION / n);
    try (Arena arena = Arena.ofConfined()) {
      Data d = data(arena, n);
      var benchmark = benchmark(n + " rows - " + rung.level(), (long) n * reps)
          .minIterations(3).warmup(Duration.ofSeconds(1)).minTime(Duration.ofSeconds(1)).build();
      for (Arm arm : arms) {
        MemorySegment[] cols = d.of(arm.columns());
        long[] out = new long[n / 64];
        long[] expected = new long[n / 64];
        referenceLoop(arm.reference()).apply(cols[0], cols[1], cols[2], expected, n);
        arm.loop().apply(cols[0], cols[1], cols[2], out, n);
        require(Arrays.equals(out, expected), arm.label() + " disagrees with the scalar loop");
        var settled = new AtomicBoolean();
        benchmark.addTimerCase(arm.label(), timer -> {
          if (settled.compareAndSet(false, true)) {
            driveToCompiledCode(arm, cols, out, n, reps);
            // The words the compiled code wrote, not the first call's: the check before the
            // timing ran the loop in the interpreter, where the Vector API is its Java code.
            require(Arrays.equals(out, expected),
                arm.label() + " disagrees with the scalar loop in compiled code");
          }
          timer.startTiming();
          drive(arm, cols, out, n, reps);
          timer.stopTiming();
        });
      }
      runCases(benchmark);
    }
  }
}
