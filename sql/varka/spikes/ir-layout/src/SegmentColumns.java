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
import java.util.List;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.Intervals.Facts;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.KindTable.Kind;

/**
 * Layout D: layout A's six fields as off-heap columns, one {@link MemorySegment} for each field in
 * the arena, so the unit-stride loops that auto-vectorize over heap columns (layout C) also do
 * here, with the lifetime, alignment and native hand-off of an arena (layout A). The same packing
 * and the same 32 bytes a node as A and C; what differs is only where the six fields live and
 * how they are read (a segment's {@code getAtIndex}, no {@code VarHandle}).
 */
final class SegmentColumns extends FfmRows {

  private final MemorySegment kindCol;
  private final MemorySegment c0Col;
  private final MemorySegment c1Col;
  private final MemorySegment c2Col;
  private final MemorySegment p0Col;
  private final MemorySegment p1Col;
  private final int[] slots;
  private final int mask;

  SegmentColumns(int nodes, int poolIntCapacity, int poolEntryCapacity) {
    super(poolIntCapacity, poolEntryCapacity);
    int n = Math.max(1, nodes);
    kindCol = arena.allocate(4L * n, 64);
    c0Col = arena.allocate(4L * n, 64);
    c1Col = arena.allocate(4L * n, 64);
    c2Col = arena.allocate(4L * n, 64);
    p0Col = arena.allocate(8L * n, 64);
    p1Col = arena.allocate(8L * n, 64);
    slots = tableOf(nodes);
    mask = slots.length - 1;
  }

  private static int getInt(MemorySegment column, int index) {
    return column.getAtIndex(ValueLayout.JAVA_INT, index);
  }

  private static long getLong(MemorySegment column, int index) {
    return column.getAtIndex(ValueLayout.JAVA_LONG, index);
  }

  private static void putInt(MemorySegment column, int index, int value) {
    column.setAtIndex(ValueLayout.JAVA_INT, index, value);
  }

  private static void putLong(MemorySegment column, int index, long value) {
    column.setAtIndex(ValueLayout.JAVA_LONG, index, value);
  }

  /** The column for field {@code field}: kind, c0, c1, c2 (ints), then p0, p1 (longs). */
  MemorySegment column(int field) {
    return switch (field) {
      case 0 -> kindCol;
      case 1 -> c0Col;
      case 2 -> c1Col;
      case 3 -> c2Col;
      case 4 -> p0Col;
      case 5 -> p1Col;
      default -> throw new IllegalArgumentException("no field " + field);
    };
  }

  @Override
  String layout() {
    return "D";
  }

  @Override
  int rowBytes() {
    return 4 * Integer.BYTES + 2 * Long.BYTES;
  }

  @Override
  long tableBytes() {
    return (slots.length + poolSlots.length) * (long) Integer.BYTES;
  }

  @Override
  int add(LoadedGraph g, int i, int[] idMap) {
    int kind = g.kind()[i];
    int[] ch = g.children()[i];
    long[] s = g.scalars()[i];
    int c0 = ch.length > 0 ? idMap[ch[0]] : -1;
    int c1 = ch.length > 1 ? idMap[ch[1]] : -1;
    int c2 = ch.length > 2 ? idMap[ch[2]] : -1;
    long p0 = 0;
    long p1 = 0;
    int[] list = g.lists()[i];
    if (list != null) {
      var entry = new int[1 + list.length];
      entry[0] = list.length;
      System.arraycopy(list, 0, entry, 1, list.length);
      p0 = poolIntern(entry);
      p1 = list.length;
    } else if (s.length <= 2) {
      p0 = s.length > 0 ? s[0] : 0;
      p1 = s.length > 1 ? s[1] : 0;
    } else if (s.length <= 4) {
      p0 = (s[0] & LOW32) | (s[1] << 32);
      p1 = (s[2] & LOW32) | ((s.length > 3 ? s[3] : 0) << 32);
    } else {
      throw new IllegalArgumentException(g.name() + " node " + i + " has " + s.length + " scalars");
    }
    int slot = hashRow(kind, c0, c1, c2, p0, p1) & mask;
    while (true) {
      int id = slots[slot];
      if (id < 0) {
        break;
      }
      if (getInt(kindCol, id) == kind && getInt(c0Col, id) == c0
          && getInt(c1Col, id) == c1 && getInt(c2Col, id) == c2
          && getLong(p0Col, id) == p0 && getLong(p1Col, id) == p1) {
        return id;
      }
      slot = (slot + 1) & mask;
    }
    int id = count++;
    putInt(kindCol, id, kind);
    putInt(c0Col, id, c0);
    putInt(c1Col, id, c1);
    putInt(c2Col, id, c2);
    putLong(p0Col, id, p0);
    putLong(p1Col, id, p1);
    slots[slot] = id;
    return id;
  }

  @Override
  int rebuild(int from, int to) {
    int[] dslots = tableOf(count);
    int dmask = dslots.length - 1;
    int[] rebuilt = new int[count];
    int n = 0;
    try (Arena tmp = Arena.ofConfined()) {
      long rows = Math.max(1, count);
      MemorySegment dkind = tmp.allocate(4L * rows, 64);
      MemorySegment dc0 = tmp.allocate(4L * rows, 64);
      MemorySegment dc1 = tmp.allocate(4L * rows, 64);
      MemorySegment dc2 = tmp.allocate(4L * rows, 64);
      MemorySegment dp0 = tmp.allocate(8L * rows, 64);
      MemorySegment dp1 = tmp.allocate(8L * rows, 64);
      for (int id = 0; id < count; id++) {
        int kind = getInt(kindCol, id);
        int c0 = remap(getInt(c0Col, id), from, to, rebuilt);
        int c1 = remap(getInt(c1Col, id), from, to, rebuilt);
        int c2 = remap(getInt(c2Col, id), from, to, rebuilt);
        long p0 = getLong(p0Col, id);
        long p1 = getLong(p1Col, id);
        int slot = hashRow(kind, c0, c1, c2, p0, p1) & dmask;
        int found = -1;
        while (true) {
          int e = dslots[slot];
          if (e < 0) {
            break;
          }
          if (getInt(dkind, e) == kind
              && getInt(dc0, e) == c0
              && getInt(dc1, e) == c1
              && getInt(dc2, e) == c2
              && getLong(dp0, e) == p0
              && getLong(dp1, e) == p1) {
            found = e;
            break;
          }
          slot = (slot + 1) & dmask;
        }
        if (found < 0) {
          found = n++;
          putInt(dkind, found, kind);
          putInt(dc0, found, c0);
          putInt(dc1, found, c1);
          putInt(dc2, found, c2);
          putLong(dp0, found, p0);
          putLong(dp1, found, p1);
          dslots[slot] = found;
        }
        rebuilt[id] = found;
      }
    }
    return n;
  }

  @Override
  Facts analyze(int[] roots) {
    var lo = new long[count];
    var hi = new long[count];
    long checksum = 0;
    for (int id = 0; id < count; id++) {
      int kind = getInt(kindCol, id);
      long s0 = 0;
      long s1 = 0;
      switch (RowTransfer.CATEGORY[kind]) {
        case RowTransfer.LEAF -> s1 = getLong(p1Col, id);
        case RowTransfer.IARITH -> s0 = getLong(p0Col, id);
        case RowTransfer.GUARD -> {
          s0 = getLong(p0Col, id);
          s1 = getLong(p1Col, id);
        }
        case RowTransfer.DIV -> {
          long p0 = getLong(p0Col, id);
          s0 = isBounded(kind) ? (int) p0 : p0;
        }
        default -> { }
      }
      RowTransfer.apply(kind, id, getInt(c0Col, id), getInt(c1Col, id),
          getInt(c2Col, id), s0, s1, lo, hi);
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
      Kind kind = table.kind(getInt(kindCol, id));
      long p0 = getLong(p0Col, id);
      long p1 = getLong(p1Col, id);
      List<String> tokens;
      if (kind.hasList()) {
        tokens = poolTokens(kind, (int) p0);
      } else {
        int plain = kind.plainScalars();
        var values = new long[plain];
        if (plain <= 2) {
          if (plain > 0) {
            values[0] = p0;
          }
          if (plain > 1) {
            values[1] = p1;
          }
        } else {
          values[0] = (int) p0;
          values[1] = p0 >> 32;
          values[2] = (int) p1;
          if (plain > 3) {
            values[3] = p1 >> 32;
          }
        }
        tokens = new ArrayList<>();
        for (int s = 0; s < plain; s++) {
          tokens.add(token(kind, s, values[s]));
        }
      }
      var children = new ArrayList<Integer>();
      int[] ids = {getInt(c0Col, id), getInt(c1Col, id), getInt(c2Col, id)};
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
