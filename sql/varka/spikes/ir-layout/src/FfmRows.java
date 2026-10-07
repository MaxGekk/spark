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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.Intervals.Facts;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.KindTable.Kind;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.KindTable.Scalar;

/**
 * Arms 2 and 3 of the spike, one class for each layout so that none pays for a switch on the layout
 * at every access: the IR as rows of a flat store in an FFM {@link Arena} ({@link FfmRowsA} and
 * {@link FfmRowsB}, arm 3), or as heap columns ({@link ColumnRows}, arm 2). What they share is
 * here: the pool of ints that holds what a row cannot, interned so that equal content has one
 * offset and equal rows stay equal; the hash and the decoding of a row's scalars back to text, for
 * the round trip through {@link VarkaIrDescription}; and the walk that builds a graph.
 *
 * <p>A row is hash-consed on build: its words are hashed, an open-addressing table of row ids is
 * probed, and a row equal to an existing one is not added. The store is sized for the graph before
 * it is built, which a store that grows would not need; growing is not what is measured.
 */
abstract class FfmRows implements AutoCloseable {

  static final long LOW32 = 0xFFFFFFFFL;

  final KindTable table = KindNames.TABLE;
  final Arena arena = Arena.ofConfined();
  final MemorySegment pool;
  final int[] poolSlots;
  final int poolMask;
  int poolInts;
  int count;

  FfmRows(int poolIntCapacity, int poolEntryCapacity) {
    pool = arena.allocate(Math.max(1L, poolIntCapacity) * Integer.BYTES, 64);
    int slots = Integer.highestOneBit(Math.max(4, poolEntryCapacity * 2 - 1)) << 1;
    poolSlots = new int[slots];
    Arrays.fill(poolSlots, -1);
    poolMask = slots - 1;
  }

  /** The layout's name, A, B or C. */
  abstract String layout();

  /** Bytes a row takes in the layout. */
  abstract int rowBytes();

  /**
   * Adds node {@code i} of {@code g}, whose children are the rows in {@code idMap}, and returns
   * its row.
   */
  abstract int add(LoadedGraph g, int i, int[] idMap);

  /** The interval fact of every row, bottom-up in one pass over the rows. */
  abstract Facts analyze(int[] roots);

  /** The rows as a description, to rebuild records from. */
  abstract VarkaIrDescription.Graph toGraph(LoadedGraph g, int[] roots);

  /**
   * The rebuild an e-graph does after a merge: every row's children are remapped (a reference to
   * {@code from} becomes one to {@code to}, and every child to the row it was rebuilt as), the row
   * is rehashed and interned into a fresh table, and rows that became equal are one. Returns the
   * number of rows after. Requires {@code to < from}, as {@link LoadedGraph#substitution} gives.
   */
  abstract int rebuild(int from, int to);

  static int remap(int child, int from, int to, int[] rebuilt) {
    return child < 0 ? -1 : rebuilt[child == from ? to : child];
  }

  /** Bytes of the hash-consing tables, which a graph that is not being built does not need. */
  abstract long tableBytes();

  /** Bytes the rows and the pool hold. */
  final long bytes() {
    return (long) count * rowBytes() + (long) poolInts * Integer.BYTES;
  }

  @Override
  public void close() {
    arena.close();
  }

  /** Builds {@code g}, returning each output's row. */
  final int[] build(LoadedGraph g) {
    var idMap = new int[g.size()];
    for (int i = 0; i < g.size(); i++) {
      idMap[i] = add(g, i, idMap);
    }
    var roots = new int[g.roots().length];
    for (int r = 0; r < roots.length; r++) {
      roots[r] = idMap[g.roots()[r]];
    }
    return roots;
  }

  /** A layout the early-access JDK adds: src-ea registers it, so src builds on JDK 25 alone. */
  interface Factory {
    FfmRows create(int nodes, int poolIntCapacity, int poolEntryCapacity);
  }

  static final java.util.Map<String, Factory> EXTRA = new java.util.TreeMap<>();

  /** Whether the layout's pool also holds wide scalars, so that it is sized like B's. */
  static boolean spillsWide(String layout) {
    return layout.equals("B") || layout.startsWith("V");
  }

  /** Whether the layout packs scalars into the row's head, and so refuses one that is too big. */
  static boolean packsInHead(String layout) {
    return layout.equals("B") || layout.startsWith("V");
  }

  static FfmRows create(String layout, int nodes, int poolIntCapacity, int poolEntryCapacity) {
    Factory extra = EXTRA.get(layout);
    if (extra != null) {
      return extra.create(nodes, poolIntCapacity, poolEntryCapacity);
    }
    return switch (layout) {
      case "A" -> new FfmRowsA(nodes, poolIntCapacity, poolEntryCapacity);
      case "B" -> new FfmRowsB(nodes, poolIntCapacity, poolEntryCapacity);
      case "C" -> new ColumnRows(nodes, poolIntCapacity, poolEntryCapacity);
      case "D" -> new SegmentColumns(nodes, poolIntCapacity, poolEntryCapacity);
      default -> throw new IllegalArgumentException("no layout " + layout);
    };
  }

  // ---------------------------------------------------------------------------------------------
  // The pool.
  // ---------------------------------------------------------------------------------------------

  /** The offset, in ints, of {@code entry} in the pool, adding it if no equal entry is there. */
  final int poolIntern(int[] entry) {
    int slot = hashInts(entry) & poolMask;
    while (true) {
      int offset = poolSlots[slot];
      if (offset < 0) {
        break;
      }
      if (poolEquals(offset, entry)) {
        return offset;
      }
      slot = (slot + 1) & poolMask;
    }
    int offset = poolInts;
    for (int value : entry) {
      pool.setAtIndex(ValueLayout.JAVA_INT, poolInts++, value);
    }
    poolSlots[slot] = offset;
    return offset;
  }

  private boolean poolEquals(int offset, int[] entry) {
    for (int i = 0; i < entry.length; i++) {
      if (pool.getAtIndex(ValueLayout.JAVA_INT, offset + i) != entry[i]) {
        return false;
      }
    }
    return true;
  }

  static int hashInts(int[] values) {
    long h = values.length * 0x9E3779B97F4A7C15L;
    for (int value : values) {
      h = (h ^ (h >>> 29) ^ value) * 0xBF58476D1CE4E5B9L;
    }
    return (int) (h ^ (h >>> 32));
  }

  static int hashRow(int a, int b, int c, int d, long e, long f) {
    long h = a * 0x9E3779B97F4A7C15L;
    h = (h ^ (h >>> 29) ^ b) * 0xBF58476D1CE4E5B9L;
    h = (h ^ (h >>> 29) ^ c) * 0x94D049BB133111EBL;
    h = (h ^ (h >>> 29) ^ d) * 0x9E3779B97F4A7C15L;
    h = (h ^ (h >>> 29) ^ e) * 0xBF58476D1CE4E5B9L;
    h = (h ^ (h >>> 29) ^ f) * 0x94D049BB133111EBL;
    return (int) (h ^ (h >>> 32));
  }

  static int[] tableOf(int nodes) {
    int slots = Integer.highestOneBit(Math.max(4, nodes * 2 - 1)) << 1;
    var table = new int[slots];
    Arrays.fill(table, -1);
    return table;
  }

  // ---------------------------------------------------------------------------------------------
  // Scalars as ints, for the pool, and back.
  // ---------------------------------------------------------------------------------------------

  /** A node's plain scalars as ints, a {@code long} as its low and high words, then its list. */
  static int[] scalarInts(Kind kind, long[] scalars, int[] list) {
    int n = 0;
    for (Scalar type : kind.scalars()) {
      n += switch (type) {
        case LONG -> 2;
        case LIST -> 0;
        default -> 1;
      };
    }
    int total = n + (list == null ? 0 : 1 + list.length);
    var out = new int[total];
    int at = 0;
    int next = 0;
    for (Scalar type : kind.scalars()) {
      switch (type) {
        case LONG -> {
          out[at++] = (int) scalars[next];
          out[at++] = (int) (scalars[next++] >> 32);
        }
        case LIST -> { }
        default -> out[at++] = (int) scalars[next++];
      }
    }
    if (list != null) {
      out[at++] = list.length;
      for (int value : list) {
        out[at++] = value;
      }
    }
    return out;
  }

  /** The ints {@link #scalarInts} wrote at {@code offset}, as scalar tokens, in component order. */
  final List<String> poolTokens(Kind kind, int offset) {
    var tokens = new ArrayList<String>();
    int at = offset;
    for (int s = 0; s < kind.scalars().size(); s++) {
      switch (kind.scalars().get(s)) {
        case LONG -> {
          long low = pool.getAtIndex(ValueLayout.JAVA_INT, at++) & LOW32;
          long high = pool.getAtIndex(ValueLayout.JAVA_INT, at++);
          tokens.add(Long.toString(low | (high << 32)));
        }
        case LIST -> {
          int n = pool.getAtIndex(ValueLayout.JAVA_INT, at++);
          var text = new StringBuilder("[");
          for (int i = 0; i < n; i++) {
            text.append(i == 0 ? "" : ",").append(pool.getAtIndex(ValueLayout.JAVA_INT, at++));
          }
          tokens.add(text.append(']').toString());
        }
        default -> tokens.add(token(kind, s, pool.getAtIndex(ValueLayout.JAVA_INT, at++)));
      }
    }
    return tokens;
  }

  /** One scalar's text, from its value: an enum by constant name, a boolean as true or false. */
  static String token(Kind kind, int s, long value) {
    return switch (kind.scalars().get(s)) {
      case ENUM -> ((Enum<?>) kind.enums().get(s).getEnumConstants()[(int) value]).name();
      case BOOL -> Boolean.toString(value != 0);
      default -> Long.toString(value);
    };
  }

  static boolean isBounded(int kindIndex) {
    return BOUNDED[kindIndex];
  }

  private static final boolean[] BOUNDED = new boolean[KindNames.TABLE.size()];

  static {
    BOUNDED[KindNames.TABLE.kind("BoundedDivide").index()] = true;
  }
}
