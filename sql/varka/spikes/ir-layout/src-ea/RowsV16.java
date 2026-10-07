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
 * Arm 4, the 16-byte reference: layout B's row and packing, with the rows in a flat array of value
 * records instead of an FFM segment. Flat only through the internal API (a null-restricted,
 * non-atomic array of a {@code @LooselyConsistentValue} class), so it is a reference for what the
 * VM can do, not something Varka could ship on a public API. The constructor requires the array
 * to be flat, from {@code ValueClass.isFlatArray}, so a layout the VM did not flatten fails
 * instead of being measured as if it were.
 */
final class RowsV16 extends FfmRows {

  /** The row: the same four ints as layout B's. */
  @LooselyConsistentValue
  value record Row(int head, int c0, int c1, int c2) {}

  private final Row[] rows;
  private final int[] slots;
  private final int mask;

  RowsV16(int nodes, int poolIntCapacity, int poolEntryCapacity) {
    super(poolIntCapacity, poolEntryCapacity);
    rows = (Row[]) ValueClass.newNullRestrictedNonAtomicArray(
        Row.class, Math.max(1, nodes), new Row(0, 0, 0, 0));
    if (!ValueClass.isFlatArray(rows)) {
      throw new IllegalStateException("the 16-byte row array is not flat on this JVM");
    }
    slots = tableOf(nodes);
    mask = slots.length - 1;
  }

  @Override
  String layout() {
    return "V16";
  }

  @Override
  int rowBytes() {
    return 4 * Integer.BYTES;
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
    int slot = hashRow(head, c0, c1, c2, 0, 0) & mask;
    while (true) {
      int id = slots[slot];
      if (id < 0) {
        break;
      }
      if (rows[id].head() == head && rows[id].c0() == c0
          && rows[id].c1() == c1 && rows[id].c2() == c2) {
        return id;
      }
      slot = (slot + 1) & mask;
    }
    int id = count++;
    rows[id] = new Row(head, c0, c1, c2);
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
    Row[] dst = (Row[]) ValueClass.newNullRestrictedNonAtomicArray(
        Row.class, Math.max(1, count), new Row(0, 0, 0, 0));
    int n = 0;
    for (int id = 0; id < count; id++) {
      Row row = rows[id];
      int c0 = remap(row.c0(), from, to, rebuilt);
      int c1 = remap(row.c1(), from, to, rebuilt);
      int c2 = remap(row.c2(), from, to, rebuilt);
      int slot = hashRow(row.head(), c0, c1, c2, 0, 0) & dmask;
      int found = -1;
      while (true) {
        int e = dslots[slot];
        if (e < 0) {
          break;
        }
        Row other = dst[e];
        if (other.head() == row.head() && other.c0() == c0 && other.c1() == c1
            && other.c2() == c2) {
          found = e;
          break;
        }
        slot = (slot + 1) & dmask;
      }
      if (found < 0) {
        found = n++;
        dst[found] = new Row(row.head(), c0, c1, c2);
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
      int head = rows[id].head();
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
      RowTransfer.apply(kind, id, rows[id].c0(), rows[id].c1(),
          rows[id].c2(), s0, s1, lo, hi);
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
      int head = rows[id].head();
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
      int[] ids = {rows[id].c0(), rows[id].c1(), rows[id].c2()};
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
