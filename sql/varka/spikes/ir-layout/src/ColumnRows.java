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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.Intervals.Facts;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.KindTable.Kind;

/**
 * Arm 2 of the spike: layout A's fields as plain heap columns, the baseline the FFM rows are read
 * against. The same six fields and the same packing as {@link FfmRowsA}, so a row is the same 32
 * bytes; what differs is where they live (six {@code int[]} and {@code long[]} columns, one array
 * for each field, instead of one off-heap row) and how they are read (array loads with the JIT's
 * bounds checks, no {@code MemorySegment}). The pool of lists is the shared off-heap one, since
 * one kind has a list and the rest never touch it.
 */
final class ColumnRows extends FfmRows {

  private final int[] kindCol;
  private final int[] c0Col;
  private final int[] c1Col;
  private final int[] c2Col;
  private final long[] p0Col;
  private final long[] p1Col;
  private final int[] slots;
  private final int mask;

  ColumnRows(int nodes, int poolIntCapacity, int poolEntryCapacity) {
    super(poolIntCapacity, poolEntryCapacity);
    int n = Math.max(1, nodes);
    kindCol = new int[n];
    c0Col = new int[n];
    c1Col = new int[n];
    c2Col = new int[n];
    p0Col = new long[n];
    p1Col = new long[n];
    slots = tableOf(nodes);
    mask = slots.length - 1;
  }

  @Override
  String layout() {
    return "C";
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
      if (kindCol[id] == kind && c0Col[id] == c0
          && c1Col[id] == c1 && c2Col[id] == c2
          && p0Col[id] == p0 && p1Col[id] == p1) {
        return id;
      }
      slot = (slot + 1) & mask;
    }
    int id = count++;
    kindCol[id] = kind;
    c0Col[id] = c0;
    c1Col[id] = c1;
    c2Col[id] = c2;
    p0Col[id] = p0;
    p1Col[id] = p1;
    slots[slot] = id;
    return id;
  }

  @Override
  Facts analyze(int[] roots) {
    var lo = new long[count];
    var hi = new long[count];
    long checksum = 0;
    for (int id = 0; id < count; id++) {
      int kind = kindCol[id];
      long s0 = 0;
      long s1 = 0;
      switch (RowTransfer.CATEGORY[kind]) {
        case RowTransfer.LEAF -> s1 = p1Col[id];
        case RowTransfer.IARITH -> s0 = p0Col[id];
        case RowTransfer.GUARD -> {
          s0 = p0Col[id];
          s1 = p1Col[id];
        }
        case RowTransfer.DIV -> {
          long p0 = p0Col[id];
          s0 = isBounded(kind) ? (int) p0 : p0;
        }
        default -> { }
      }
      RowTransfer.apply(kind, id, c0Col[id], c1Col[id],
          c2Col[id], s0, s1, lo, hi);
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
      Kind kind = table.kind(kindCol[id]);
      long p0 = p0Col[id];
      long p1 = p1Col[id];
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
      int[] ids = {c0Col[id], c1Col[id], c2Col[id]};
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
