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

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.Intervals.Facts;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.KindTable.Kind;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.KindTable.Scalar;

/**
 * Layout B: a 16-byte row with all three children in the row and only wide scalars spilled.
 *
 * <pre>
 * byte   0       4       8       12      16
 *        +-------+-------+-------+-------+
 *        | head  |  c0   |  c1   |  c2   |
 *        +-------+-------+-------+-------+
 * </pre>
 * {@code head} holds the {@link KindTable} index in bits 0 to 5. For a kind whose scalars are small
 * it holds them in bits 6 to 31: the enums and booleans from bit 6 in component order, each as
 * wide as its constants need, then the one {@code int} a kind has, in the bits that remain. For a
 * kind with a {@code long}, more than one {@code int}, or a list it holds, from bit 6, the offset
 * of an entry in the pool of ints, which never holds a child id and so never changes when children
 * are rewritten to canonical ids.
 */
final class FfmRowsB extends FfmRows {

  private static final StructLayout ROW = MemoryLayout.structLayout(
      ValueLayout.JAVA_INT.withName("head"), ValueLayout.JAVA_INT.withName("c0"),
      ValueLayout.JAVA_INT.withName("c1"), ValueLayout.JAVA_INT.withName("c2"));

  private static final VarHandle HEAD = ROW.varHandle(PathElement.groupElement("head"));
  private static final VarHandle C0 = ROW.varHandle(PathElement.groupElement("c0"));
  private static final VarHandle C1 = ROW.varHandle(PathElement.groupElement("c1"));
  private static final VarHandle C2 = ROW.varHandle(PathElement.groupElement("c2"));

  static final int KIND_BITS = 6;
  static final int KIND_MASK = (1 << KIND_BITS) - 1;
  static final int MAX_POOL_OFFSET = (1 << (32 - KIND_BITS)) - 1;

  /** Where, in bits, and how wide a kind's plain scalars are in {@code head}, by kind index. */
  static final int[][] SHIFT;
  static final int[][] MASK;

  static {
    KindTable table = KindNames.TABLE;
    if (table.size() > (1 << KIND_BITS)) {
      throw new IllegalStateException(table.size() + " kinds do not fit " + KIND_BITS + " bits");
    }
    SHIFT = new int[table.size()][];
    MASK = new int[table.size()][];
    for (int k = 0; k < table.size(); k++) {
      Kind kind = table.kind(k);
      int plain = kind.plainScalars();
      SHIFT[k] = new int[plain];
      MASK[k] = new int[plain];
      if (kind.wide()) {
        continue;
      }
      int cursor = KIND_BITS;
      int index = 0;
      for (int s = 0; s < kind.scalars().size(); s++) {
        Scalar type = kind.scalars().get(s);
        if (type == Scalar.ENUM || type == Scalar.BOOL) {
          int constants = type == Scalar.BOOL ? 2 : kind.enums().get(s).getEnumConstants().length;
          int width = 32 - Integer.numberOfLeadingZeros(constants - 1);
          SHIFT[k][index] = cursor;
          MASK[k][index] = (1 << width) - 1;
          cursor += width;
        }
        index++;
      }
      index = 0;
      for (int s = 0; s < kind.scalars().size(); s++) {
        if (kind.scalars().get(s) == Scalar.INT) {
          SHIFT[k][index] = cursor;
          MASK[k][index] = (int) ((1L << (32 - cursor)) - 1);
        }
        index++;
      }
    }
  }

  private final MemorySegment rows;
  private final int[] slots;
  private final int mask;

  FfmRowsB(int nodes, int poolIntCapacity, int poolEntryCapacity) {
    super(poolIntCapacity, poolEntryCapacity);
    rows = arena.allocate(ROW.byteSize() * Math.max(1, nodes), 64);
    slots = tableOf(nodes);
    mask = slots.length - 1;
  }

  @Override
  String layout() {
    return "B";
  }

  @Override
  int rowBytes() {
    return (int) ROW.byteSize();
  }

  @Override
  long tableBytes() {
    return (slots.length + poolSlots.length) * (long) Integer.BYTES;
  }

  @Override
  int add(LoadedGraph g, int i, int[] idMap) {
    int kindIndex = g.kind()[i];
    Kind kind = table.kind(kindIndex);
    int[] ch = g.children()[i];
    int c0 = ch.length > 0 ? idMap[ch[0]] : -1;
    int c1 = ch.length > 1 ? idMap[ch[1]] : -1;
    int c2 = ch.length > 2 ? idMap[ch[2]] : -1;
    int head = kindIndex;
    long[] s = g.scalars()[i];
    if (kind.wide()) {
      int offset = poolIntern(scalarInts(kind, s, g.lists()[i]));
      if (offset > MAX_POOL_OFFSET) {
        throw new IllegalArgumentException(
            "pool offset " + offset + " does not fit " + (32 - KIND_BITS) + " bits");
      }
      head |= offset << KIND_BITS;
    } else {
      for (int j = 0; j < s.length; j++) {
        if (s[j] < 0 || s[j] > MASK[kindIndex][j]) {
          throw new IllegalArgumentException(g.name() + " node " + i + ": " + kind.name()
              + " scalar " + s[j] + " does not fit " + Integer.bitCount(MASK[kindIndex][j])
              + " bits");
        }
        head |= (int) s[j] << SHIFT[kindIndex][j];
      }
    }
    int slot = hashRow(head, c0, c1, c2, 0, 0) & mask;
    while (true) {
      int id = slots[slot];
      if (id < 0) {
        break;
      }
      long at = (long) id << 4;
      if ((int) HEAD.get(rows, at) == head && (int) C0.get(rows, at) == c0
          && (int) C1.get(rows, at) == c1 && (int) C2.get(rows, at) == c2) {
        return id;
      }
      slot = (slot + 1) & mask;
    }
    int id = count++;
    long at = (long) id << 4;
    HEAD.set(rows, at, head);
    C0.set(rows, at, c0);
    C1.set(rows, at, c1);
    C2.set(rows, at, c2);
    slots[slot] = id;
    return id;
  }

  private long poolLong(int offset) {
    return (pool.getAtIndex(ValueLayout.JAVA_INT, offset) & LOW32)
        | ((long) pool.getAtIndex(ValueLayout.JAVA_INT, offset + 1) << 32);
  }

  @Override
  Facts analyze(int[] roots) {
    var lo = new long[count];
    var hi = new long[count];
    long checksum = 0;
    for (int id = 0; id < count; id++) {
      long at = (long) id << 4;
      int head = (int) HEAD.get(rows, at);
      int kind = head & KIND_MASK;
      long s0 = 0;
      long s1 = 0;
      switch (RowTransfer.CATEGORY[kind]) {
        case RowTransfer.LEAF -> s1 = (head >>> SHIFT[kind][1]) & MASK[kind][1];
        case RowTransfer.IARITH -> s0 = (head >>> SHIFT[kind][0]) & MASK[kind][0];
        case RowTransfer.GUARD -> {
          int offset = head >>> KIND_BITS;
          s0 = poolLong(offset);
          s1 = poolLong(offset + 2);
        }
        case RowTransfer.DIV -> {
          int offset = head >>> KIND_BITS;
          s0 = isBounded(kind) ? pool.getAtIndex(ValueLayout.JAVA_INT, offset) : poolLong(offset);
        }
        default -> { }
      }
      RowTransfer.apply(kind, id, (int) C0.get(rows, at), (int) C1.get(rows, at),
          (int) C2.get(rows, at), s0, s1, lo, hi);
      checksum += Intervals.mix(lo[id], hi[id]);
    }
    var rootLo = new long[roots.length];
    var rootHi = new long[roots.length];
    for (int r = 0; r < roots.length; r++) {
      rootLo[r] = lo[roots[r]];
      rootHi[r] = hi[roots[r]];
    }
    return new Facts(count, checksum, rootLo, rootHi);
  }

  @Override
  VarkaIrDescription.Graph toGraph(LoadedGraph g, int[] roots) {
    var nodes = new ArrayList<VarkaIrDescription.Node>(count);
    for (int id = 0; id < count; id++) {
      long at = (long) id << 4;
      int head = (int) HEAD.get(rows, at);
      int kindIndex = head & KIND_MASK;
      Kind kind = table.kind(kindIndex);
      List<String> tokens;
      if (kind.wide()) {
        tokens = poolTokens(kind, head >>> KIND_BITS);
      } else {
        tokens = new ArrayList<>();
        for (int j = 0; j < kind.plainScalars(); j++) {
          tokens.add(token(kind, j, (head >>> SHIFT[kindIndex][j]) & MASK[kindIndex][j]));
        }
      }
      var children = new ArrayList<Integer>();
      int[] ids = {(int) C0.get(rows, at), (int) C1.get(rows, at), (int) C2.get(rows, at)};
      for (int c = 0; c < kind.children(); c++) {
        children.add(ids[c]);
      }
      nodes.add(new VarkaIrDescription.Node(kind.name(), tokens, children));
    }
    var rootIds = new ArrayList<Integer>();
    for (int root : roots) {
      rootIds.add(root);
    }
    return new VarkaIrDescription.Graph(g.name(), g.numInputs(), g.numLiterals(), nodes, rootIds);
  }
}
