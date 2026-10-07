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

import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;

import jdk.internal.value.ValueClass;
import jdk.internal.vm.annotation.LooselyConsistentValue;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.Intervals.Facts;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.KindTable.Kind;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.KindTable.Scalar;

/**
 * Arm 4, the packed row: one {@code long} a node, so a value record of one 8-byte field, flat as a
 * null-restricted atomic array through the internal API.
 *
 * <pre>
 * bit  63 62                 44 43                25 24                6 5       0
 *      +--+---------------------+--------------------+--------------------+--------+
 *      | 0|     c2 + 1 (19)     |     c1 + 1 (19)    |     c0 + 1 (19)    | kind(6)|
 *      +--+---------------------+--------------------+--------------------+--------+
 * </pre>
 * A child id is stored plus one, so that 0 is "no child" and a graph holds up to 524,286 nodes; a
 * larger one is refused. The scalars are not in the word: a parallel {@code int[]}, {@code aux},
 * holds what layout B holds in bits 6 to 31 of its head (small scalars, or an offset into the
 * pool), so a node is 12 bytes, and the arm isolates the row container from the packing of
 * scalars. The constructor requires the row array to be flat.
 */
final class RowsV63 extends FfmRows {

  /** The row: the word above. */
  value record Row(long word) {}

  private static final int ID_BITS = 19;
  private static final int ID_MASK = (1 << ID_BITS) - 1;

  private final Row[] rows;
  private final int[] aux;
  private final int[] slots;
  private final int mask;

  RowsV63(int nodes, int poolIntCapacity, int poolEntryCapacity) {
    super(poolIntCapacity, poolEntryCapacity);
    rows = (Row[]) ValueClass.newNullRestrictedAtomicArray(
        Row.class, Math.max(1, nodes), new Row(0L));
    if (!ValueClass.isFlatArray(rows)) {
      throw new IllegalStateException("the packed row array is not flat on this JVM");
    }
    aux = new int[Math.max(1, nodes)];
    slots = tableOf(nodes);
    mask = slots.length - 1;
  }

  private static long word(int kind, int c0, int c1, int c2) {
    return kind | (long) (c0 + 1) << 6 | (long) (c1 + 1) << (6 + ID_BITS)
        | (long) (c2 + 1) << (6 + 2 * ID_BITS);
  }

  private int kindOf(int id) {
    return (int) rows[id].word() & FfmRowsB.KIND_MASK;
  }

  /** Child {@code n} of row {@code id}, or -1. */
  private int child(int id, int n) {
    return ((int) (rows[id].word() >>> (6 + n * ID_BITS)) & ID_MASK) - 1;
  }

  /** What layout B calls the head: the kind with the scalars above it. */
  private int head(int id) {
    return kindOf(id) | aux[id] << FfmRowsB.KIND_BITS;
  }

  @Override
  String layout() {
    return "V63";
  }

  @Override
  int rowBytes() {
    return Long.BYTES + Integer.BYTES;
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
      if (offset > FfmRowsB.MAX_POOL_OFFSET) {
        throw new IllegalArgumentException(
            "pool offset " + offset + " does not fit " + (32 - FfmRowsB.KIND_BITS) + " bits");
      }
      head |= offset << FfmRowsB.KIND_BITS;
    } else {
      for (int j = 0; j < s.length; j++) {
        if (s[j] < 0 || s[j] > FfmRowsB.MASK[kindIndex][j]) {
          throw new IllegalArgumentException(g.name() + " node " + i + ": " + kind.name()
              + " scalar " + s[j] + " does not fit " + Integer.bitCount(FfmRowsB.MASK[kindIndex][j])
              + " bits");
        }
        head |= (int) s[j] << FfmRowsB.SHIFT[kindIndex][j];
      }
    }
    long word = word(kindIndex, c0, c1, c2);
    int payload = head >>> FfmRowsB.KIND_BITS;
    int slot = hashRow((int) word, (int) (word >>> 32), payload, 0, 0, 0) & mask;
    while (true) {
      int id = slots[slot];
      if (id < 0) {
        break;
      }
      if (rows[id].word() == word && aux[id] == payload) {
        return id;
      }
      slot = (slot + 1) & mask;
    }
    if (count > ID_MASK - 1) {
      throw new IllegalArgumentException("row " + count + " does not fit " + ID_BITS + " bits");
    }
    int id = count++;
    rows[id] = new Row(word);
    aux[id] = payload;
    slots[slot] = id;
    return id;
  }

  private long poolLong(int offset) {
    return (pool.getAtIndex(ValueLayout.JAVA_INT, offset) & LOW32)
        | ((long) pool.getAtIndex(ValueLayout.JAVA_INT, offset + 1) << 32);
  }

  @Override
  int rebuild(int from, int to) {
    int[] dslots = tableOf(count);
    int dmask = dslots.length - 1;
    int[] rebuilt = new int[count];
    Row[] dst = (Row[]) ValueClass.newNullRestrictedAtomicArray(
        Row.class, Math.max(1, count), new Row(0L));
    int[] daux = new int[Math.max(1, count)];
    int n = 0;
    for (int id = 0; id < count; id++) {
      int c0 = remap(child(id, 0), from, to, rebuilt);
      int c1 = remap(child(id, 1), from, to, rebuilt);
      int c2 = remap(child(id, 2), from, to, rebuilt);
      long word = word(kindOf(id), c0, c1, c2);
      int payload = aux[id];
      int slot = hashRow((int) word, (int) (word >>> 32), payload, 0, 0, 0) & dmask;
      int found = -1;
      while (true) {
        int e = dslots[slot];
        if (e < 0) {
          break;
        }
        if (dst[e].word() == word && daux[e] == payload) {
          found = e;
          break;
        }
        slot = (slot + 1) & dmask;
      }
      if (found < 0) {
        found = n++;
        dst[found] = new Row(word);
        daux[found] = payload;
        dslots[slot] = found;
      }
      rebuilt[id] = found;
    }
    return n;
  }

  @Override
  Facts analyze(int[] roots) {
    var lo = new long[count];
    var hi = new long[count];
    long checksum = 0;
    for (int id = 0; id < count; id++) {
      int head = head(id);
      int kind = head & FfmRowsB.KIND_MASK;
      long s0 = 0;
      long s1 = 0;
      switch (RowTransfer.CATEGORY[kind]) {
        case RowTransfer.LEAF -> s1 = (head >>> FfmRowsB.SHIFT[kind][1]) & FfmRowsB.MASK[kind][1];
        case RowTransfer.IARITH -> s0 = (head >>> FfmRowsB.SHIFT[kind][0]) & FfmRowsB.MASK[kind][0];
        case RowTransfer.GUARD -> {
          int offset = head >>> FfmRowsB.KIND_BITS;
          s0 = poolLong(offset);
          s1 = poolLong(offset + 2);
        }
        case RowTransfer.DIV -> {
          int offset = head >>> FfmRowsB.KIND_BITS;
          s0 = isBounded(kind) ? pool.getAtIndex(ValueLayout.JAVA_INT, offset) : poolLong(offset);
        }
        default -> { }
      }
      RowTransfer.apply(kind, id, child(id, 0), child(id, 1),
          child(id, 2), s0, s1, lo, hi);
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
      int head = head(id);
      int kindIndex = head & FfmRowsB.KIND_MASK;
      Kind kind = table.kind(kindIndex);
      List<String> tokens;
      if (kind.wide()) {
        tokens = poolTokens(kind, head >>> FfmRowsB.KIND_BITS);
      } else {
        tokens = new ArrayList<>();
        for (int j = 0; j < kind.plainScalars(); j++) {
          tokens.add(token(kind, j,
              (head >>> FfmRowsB.SHIFT[kindIndex][j]) & FfmRowsB.MASK[kindIndex][j]));
        }
      }
      var children = new ArrayList<Integer>();
      int[] ids = {child(id, 0), child(id, 1), child(id, 2)};
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
