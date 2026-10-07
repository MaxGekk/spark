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

import org.apache.spark.sql.catalyst.expressions.codegen.varka.Intervals.Facts;

/**
 * Graphs at and past the limits of what layout B packs into a row's head, which the corpus does not
 * reach: the largest column ordinal, the largest literal index that fits 25 bits and the first one
 * that does not, a long lane, the extremes of the wide scalars that go to the pool, a list of one
 * full-range pair and a long one, and the three-child kinds with their flag both ways.
 *
 * <p>Layout B must refuse a literal index that does not fit, with an error, and never truncate it:
 * a row that read back as a different node would be the one failure the agreement check could not
 * see, since the round trip would be built from the same wrong row.
 */
final class EdgeCases {

  private EdgeCases() {}

  private record Case(String name, String text, boolean bRefuses) {}

  private static final List<Case> CASES = List.of(
      new Case("max_index", """
          graph edge_max_index inputs 64 literals 33554432 roots 2
          0 ColumnRef 63 INT |
          1 LiteralSlot 33554431 INT |
          2 IntArith ADD WRAP | 0 1
          """, false),
      new Case("past_max_index", """
          graph edge_past_max_index inputs 1 literals 33554433 roots 2
          0 ColumnRef 0 INT |
          1 LiteralSlot 33554432 INT |
          2 IntArith ADD WRAP | 0 1
          """, true),
      new Case("long_lane", """
          graph edge_long_lane inputs 1 literals 8 roots 3
          0 ColumnRef 0 LONG |
          1 LiteralSlot 7 LONG |
          2 IntArith MUL NULL | 0 1
          3 IntNeg FAIL | 2
          """, false),
      new Case("wide_scalars", """
          graph edge_wide_scalars inputs 1 literals 0 roots 1,2,3
          0 ColumnRef 0 LONG |
          1 ConstDivide 9223372036854775807 4503599627370496 | 0
          2 ConstDivide -9223372036854775807 1 | 0
          3 GuardedRange -9223372036854775808 9223372036854775807 | 0
          """, false),
      new Case("lists", """
          graph edge_lists inputs 1 literals 0 roots 1,2,3
          0 ColumnRef 0 INT |
          1 InRanges [-2147483648,2147483647] | 0
          2 InRanges [1,2,4,5,10,10] | 0
          3 InRanges [0,0,2,2,4,4,6,6,8,8,10,10,12,12,14,14,16,16,18,18,20,20,22,22] | 0
          """, false),
      new Case("three_children", """
          graph edge_three_children inputs 1 literals 2 roots 3,4,5
          0 ColumnRef 0 INT |
          1 LiteralSlot 0 INT |
          2 Compare LT | 0 1
          3 IfElse | 2 0 1
          4 MakeDate true | 0 0 1
          5 MakeDate false | 0 0 1
          """, false));

  /** Runs every case, failing with the case's name if an arm disagrees or B truncates. */
  static void run(KindTable table) {
    for (Case c : CASES) {
      LoadedGraph g = LoadedGraph.loadAll(table, c.text()).get(0);
      List<VarkaVectorIR> records = RecordsArm.build(g);
      Facts expected = RecordsArm.analyze(records);
      for (String layout : new String[] {"A", "B"}) {
        boolean wide = layout.equals("A");
        try (FfmRows rows = FfmRows.create(layout, g.size(),
            wide ? g.listInts() : g.wideInts(table), wide ? g.listNodes() : g.wideNodes(table))) {
          int[] roots;
          try {
            roots = rows.build(g);
          } catch (IllegalArgumentException e) {
            if (layout.equals("B") && c.bRefuses() && e.getMessage().contains("does not fit")) {
              continue;
            }
            throw new IllegalStateException("edge case " + c.name() + ", layout " + layout
                + ": " + e.getMessage(), e);
          }
          if (layout.equals("B") && c.bRefuses()) {
            throw new IllegalStateException("edge case " + c.name()
                + ": layout B took a literal index that does not fit, which it must refuse");
          }
          Facts facts = rows.analyze(roots);
          if (!facts.sameAs(expected)) {
            throw new IllegalStateException("edge case " + c.name() + ", layout " + layout
                + " computes " + facts + " where the records compute " + expected);
          }
          if (!VarkaIrDescription.rebuild(rows.toGraph(g, roots)).equals(records)) {
            throw new IllegalStateException(
                "edge case " + c.name() + ", layout " + layout + " does not round-trip");
          }
        }
      }
    }
    System.out.println("edge cases: " + CASES.size() + " graphs at the packing limits agree,"
        + " and layout B refuses an index that does not fit");
  }
}
