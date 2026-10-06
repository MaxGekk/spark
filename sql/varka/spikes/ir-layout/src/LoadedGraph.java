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

import java.util.List;

/**
 * A {@link VarkaIrDescription.Graph} with its scalars read as numbers once, outside any timing, so
 * that each flat arm builds from arrays and not from text: a node's kind as its {@link KindTable}
 * index, its children as ids, its scalars each as a {@code long} (an enum's ordinal, an
 * {@code int}, a {@code long}, a {@code boolean} as 0 or 1), and the one list of ints apart.
 */
record LoadedGraph(
    String name,
    int numInputs,
    int numLiterals,
    int size,
    int[] kind,
    int[][] children,
    long[][] scalars,
    int[][] lists,
    int[] roots,
    VarkaIrDescription.Graph source) {

  /** How many ints a pool that holds only this graph's lists needs, layout A's. */
  int listInts() {
    int total = 0;
    for (int[] list : lists) {
      total += list == null ? 0 : 1 + list.length;
    }
    return total;
  }

  /** How many nodes have a list, the most entries layout A's pool can hold. */
  int listNodes() {
    int total = 0;
    for (int[] list : lists) {
      total += list == null ? 0 : 1;
    }
    return total;
  }

  /** How many nodes have wide scalars, the most entries layout B's pool can hold. */
  int wideNodes(KindTable table) {
    int total = 0;
    for (int i = 0; i < size; i++) {
      total += table.kind(kind[i]).wide() ? 1 : 0;
    }
    return total;
  }

  /**
   * How many ints a pool that holds every wide kind's scalars needs, layout B's: the most it can
   * hold, since an entry equal to an earlier one is not added again.
   */
  int wideInts(KindTable table) {
    int total = 0;
    for (int i = 0; i < size; i++) {
      KindTable.Kind kind = table.kind(this.kind[i]);
      if (kind.wide()) {
        total += FfmRows.scalarInts(kind, scalars[i], lists[i]).length;
      }
    }
    return total;
  }

  static LoadedGraph load(KindTable table, VarkaIrDescription.Graph graph) {
    int n = graph.nodes().size();
    var kind = new int[n];
    var children = new int[n][];
    var scalars = new long[n][];
    var lists = new int[n][];
    for (int i = 0; i < n; i++) {
      VarkaIrDescription.Node node = graph.nodes().get(i);
      KindTable.Kind info = table.kind(node.kind());
      kind[i] = info.index();
      children[i] = node.children().stream().mapToInt(Integer::intValue).toArray();
      var plain = new long[info.plainScalars()];
      int next = 0;
      for (int s = 0; s < info.scalars().size(); s++) {
        String text = node.scalars().get(s);
        switch (info.scalars().get(s)) {
          case INT -> plain[next++] = Integer.parseInt(text);
          case LONG -> plain[next++] = Long.parseLong(text);
          case BOOL -> plain[next++] = Boolean.parseBoolean(text) ? 1 : 0;
          case ENUM -> plain[next++] = ordinal(info.enums().get(s), text);
          case LIST -> lists[i] = parseList(text);
        }
      }
      scalars[i] = plain;
    }
    return new LoadedGraph(graph.name(), graph.numInputs(), graph.numLiterals(), n, kind,
        children, scalars, lists, graph.roots().stream().mapToInt(Integer::intValue).toArray(),
        graph);
  }

  private static long ordinal(Class<?> type, String name) {
    for (Object constant : type.getEnumConstants()) {
      if (((Enum<?>) constant).name().equals(name)) {
        return ((Enum<?>) constant).ordinal();
      }
    }
    throw new IllegalArgumentException(name + " is not a constant of " + type.getSimpleName());
  }

  private static int[] parseList(String text) {
    String inner = text.substring(1, text.length() - 1);
    if (inner.isEmpty()) {
      return new int[0];
    }
    String[] parts = inner.split(",");
    var values = new int[parts.length];
    for (int i = 0; i < parts.length; i++) {
      values[i] = Integer.parseInt(parts[i]);
    }
    return values;
  }

  /** The graphs of a description file, loaded. */
  static List<LoadedGraph> loadAll(KindTable table, String text) {
    return VarkaIrDescription.parse(text).stream().map(g -> load(table, g)).toList();
  }
}
