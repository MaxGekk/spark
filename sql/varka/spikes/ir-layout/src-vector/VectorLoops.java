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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * Which layout lets the JVM vectorize a pass over every row: the same per-row hash of four int
 * fields (kind and three child ids) over heap columns, off-heap columns, and off-heap rows 32
 * bytes apart (layout A), by C2's auto-vectorizer and by the Vector API. The hash is independent
 * for each row and uses 32-bit arithmetic only, so a vector unit can do it. Run it with and
 * without {@code -XX:-UseSuperWord}: a loop that is no slower without it was not vectorized.
 */
public final class VectorLoops {

  private static final int ROWS = 1 << 16;
  private static final int STRIDE = 32;
  private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;
  private static volatile int sink;

  private VectorLoops() {}

  static int mix(int h, int v) {
    h = (h ^ v) * 0x9E3779B1;
    return h ^ (h >>> 15);
  }

  public static void main(String[] args) {
    String mode = args[0];
    double seconds = args.length > 1 ? Double.parseDouble(args[1]) : 2;
    int[] kind = new int[ROWS];
    int[] c0 = new int[ROWS];
    int[] c1 = new int[ROWS];
    int[] c2 = new int[ROWS];
    int[] out = new int[ROWS];
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment sKind = arena.allocate(4L * ROWS, 64);
      MemorySegment sC0 = arena.allocate(4L * ROWS, 64);
      MemorySegment sC1 = arena.allocate(4L * ROWS, 64);
      MemorySegment sC2 = arena.allocate(4L * ROWS, 64);
      MemorySegment rows = arena.allocate((long) STRIDE * ROWS, 64);
      for (int i = 0; i < ROWS; i++) {
        kind[i] = i % 37;
        c0[i] = i - 1;
        c1[i] = i / 2;
        c2[i] = i % 5 - 1;
        sKind.setAtIndex(ValueLayout.JAVA_INT, i, kind[i]);
        sC0.setAtIndex(ValueLayout.JAVA_INT, i, c0[i]);
        sC1.setAtIndex(ValueLayout.JAVA_INT, i, c1[i]);
        sC2.setAtIndex(ValueLayout.JAVA_INT, i, c2[i]);
        rows.set(ValueLayout.JAVA_INT, (long) i * STRIDE, kind[i]);
        rows.set(ValueLayout.JAVA_INT, (long) i * STRIDE + 4, c0[i]);
        rows.set(ValueLayout.JAVA_INT, (long) i * STRIDE + 8, c1[i]);
        rows.set(ValueLayout.JAVA_INT, (long) i * STRIDE + 12, c2[i]);
      }
      long window = (long) (seconds * 1e9);
      double best = Double.MAX_VALUE;
      for (int it = 0; it < 8; it++) {
        long start = System.nanoTime();
        long end = start + window;
        long passes = 0;
        long now;
        do {
          switch (mode) {
            case "heap-columns" -> heapColumns(kind, c0, c1, c2, out);
            case "segment-columns" -> segmentColumns(sKind, sC0, sC1, sC2, out);
            case "segment-rows" -> segmentRows(rows, out);
            case "vector-heap" -> vectorHeap(kind, c0, c1, c2, out);
            case "vector-segment" -> vectorSegment(sKind, sC0, sC1, sC2, out);
            default -> throw new IllegalArgumentException(mode);
          }
          passes++;
          now = System.nanoTime();
        } while (now < end);
        if (it >= 3) {
          best = Math.min(best, (now - start) / (double) passes / ROWS);
        }
      }
      sink = out[7];
      System.out.printf("VLOOP %s superword %s ns_per_row %.3f%n", mode,
          System.getProperty("vloops.superword", "on"), best);
    }
  }

  static void heapColumns(int[] kind, int[] c0, int[] c1, int[] c2, int[] out) {
    for (int i = 0; i < ROWS; i++) {
      out[i] = mix(mix(mix(kind[i], c0[i]), c1[i]), c2[i]);
    }
  }

  static void segmentColumns(MemorySegment kind, MemorySegment c0, MemorySegment c1,
      MemorySegment c2, int[] out) {
    for (int i = 0; i < ROWS; i++) {
      out[i] = mix(mix(mix(kind.getAtIndex(ValueLayout.JAVA_INT, i),
          c0.getAtIndex(ValueLayout.JAVA_INT, i)), c1.getAtIndex(ValueLayout.JAVA_INT, i)),
          c2.getAtIndex(ValueLayout.JAVA_INT, i));
    }
  }

  static void segmentRows(MemorySegment rows, int[] out) {
    for (int i = 0; i < ROWS; i++) {
      long at = (long) i * STRIDE;
      out[i] = mix(mix(mix(rows.get(ValueLayout.JAVA_INT, at),
          rows.get(ValueLayout.JAVA_INT, at + 4)), rows.get(ValueLayout.JAVA_INT, at + 8)),
          rows.get(ValueLayout.JAVA_INT, at + 12));
    }
  }

  private static IntVector vmix(IntVector h, IntVector v) {
    h = h.lanewise(VectorOperators.XOR, v).mul(0x9E3779B1);
    return h.lanewise(VectorOperators.XOR, h.lanewise(VectorOperators.LSHR, 15));
  }

  static void vectorHeap(int[] kind, int[] c0, int[] c1, int[] c2, int[] out) {
    int lanes = SPECIES.length();
    for (int i = 0; i < ROWS; i += lanes) {
      var h = IntVector.fromArray(SPECIES, kind, i);
      h = vmix(h, IntVector.fromArray(SPECIES, c0, i));
      h = vmix(h, IntVector.fromArray(SPECIES, c1, i));
      h = vmix(h, IntVector.fromArray(SPECIES, c2, i));
      h.intoArray(out, i);
    }
  }

  static void vectorSegment(MemorySegment kind, MemorySegment c0, MemorySegment c1,
      MemorySegment c2, int[] out) {
    int lanes = SPECIES.length();
    ByteOrder order = ByteOrder.nativeOrder();
    for (int i = 0; i < ROWS; i += lanes) {
      long at = 4L * i;
      var h = IntVector.fromMemorySegment(SPECIES, kind, at, order);
      h = vmix(h, IntVector.fromMemorySegment(SPECIES, c0, at, order));
      h = vmix(h, IntVector.fromMemorySegment(SPECIES, c1, at, order));
      h = vmix(h, IntVector.fromMemorySegment(SPECIES, c2, at, order));
      h.intoArray(out, i);
    }
  }
}
