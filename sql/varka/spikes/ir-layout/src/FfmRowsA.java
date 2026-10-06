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

/**
 * Layout A: a fixed 32-byte row, the row of eight ints jegg issue #92 proposes.
 *
 * <pre>
 * byte   0       4       8       12      16              24              32
 *        +-------+-------+-------+-------+---------------+---------------+
 *        | kind  |  c0   |  c1   |  c2   |      p0       |      p1       |
 *        +-------+-------+-------+-------+---------------+---------------+
 * </pre>
 * {@code kind} is the {@link KindTable} index; {@code c0} to {@code c2} are child row ids, -1 when
 * absent; {@code p0} and {@code p1} are the scalars in component order, each widened to 64 bits, or
 * for a kind with more than two the ints packed two to a long. A list is the one use of the pool:
 * {@code p0} is its offset and {@code p1} its length.
 */
final class FfmRowsA extends FfmRows {

  private static final StructLayout ROW = MemoryLayout.structLayout(
      ValueLayout.JAVA_INT.withName("kind"), ValueLayout.JAVA_INT.withName("c0"),
      ValueLayout.JAVA_INT.withName("c1"), ValueLayout.JAVA_INT.withName("c2"),
      ValueLayout.JAVA_LONG.withName("p0"), ValueLayout.JAVA_LONG.withName("p1"));

  // static final, so that the JIT folds the handles into plain loads and stores.
  private static final VarHandle KIND = ROW.varHandle(PathElement.groupElement("kind"));
  private static final VarHandle C0 = ROW.varHandle(PathElement.groupElement("c0"));
  private static final VarHandle C1 = ROW.varHandle(PathElement.groupElement("c1"));
  private static final VarHandle C2 = ROW.varHandle(PathElement.groupElement("c2"));
  private static final VarHandle P0 = ROW.varHandle(PathElement.groupElement("p0"));
  private static final VarHandle P1 = ROW.varHandle(PathElement.groupElement("p1"));

  private final MemorySegment rows;
  private final int[] slots;
  private final int mask;

  FfmRowsA(int nodes, int poolIntCapacity, int poolEntryCapacity) {
    super(poolIntCapacity, poolEntryCapacity);
    rows = arena.allocate(ROW.byteSize() * Math.max(1, nodes), 64);
    slots = tableOf(nodes);
    mask = slots.length - 1;
  }

  @Override
  String layout() {
    return "A";
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
      long at = (long) id << 5;
      if ((int) KIND.get(rows, at) == kind && (int) C0.get(rows, at) == c0
          && (int) C1.get(rows, at) == c1 && (int) C2.get(rows, at) == c2
          && (long) P0.get(rows, at) == p0 && (long) P1.get(rows, at) == p1) {
        return id;
      }
      slot = (slot + 1) & mask;
    }
    int id = count++;
    long at = (long) id << 5;
    KIND.set(rows, at, kind);
    C0.set(rows, at, c0);
    C1.set(rows, at, c1);
    C2.set(rows, at, c2);
    P0.set(rows, at, p0);
    P1.set(rows, at, p1);
    slots[slot] = id;
    return id;
  }

  @Override
  Facts analyze(int[] roots) {
    var lo = new long[count];
    var hi = new long[count];
    long checksum = 0;
    for (int id = 0; id < count; id++) {
      long at = (long) id << 5;
      int kind = (int) KIND.get(rows, at);
      long s0 = 0;
      long s1 = 0;
      switch (RowTransfer.CATEGORY[kind]) {
        case RowTransfer.LEAF -> s1 = (long) P1.get(rows, at);
        case RowTransfer.IARITH -> s0 = (long) P0.get(rows, at);
        case RowTransfer.GUARD -> {
          s0 = (long) P0.get(rows, at);
          s1 = (long) P1.get(rows, at);
        }
        case RowTransfer.DIV -> {
          long p0 = (long) P0.get(rows, at);
          s0 = isBounded(kind) ? (int) p0 : p0;
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
      long at = (long) id << 5;
      Kind kind = table.kind((int) KIND.get(rows, at));
      long p0 = (long) P0.get(rows, at);
      long p1 = (long) P1.get(rows, at);
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
