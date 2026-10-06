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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.Intervals.Facts;

/**
 * The IR-storage spike's harness (VARKA-291): reads graph descriptions, builds each graph in every
 * arm, and checks that the arms agree before anyone looks at a time. Plain Java, outside the
 * build; see the README beside it for how to run it.
 *
 * <p>For every graph, the arms must agree on three things with the records: how many distinct
 * nodes it has, the interval fact of every node (summed as an order-independent checksum, and
 * exactly for each output), and a round trip: the rows decoded back to a description and rebuilt as
 * records equal the records built directly. The records built by direct constructors must in turn
 * equal the ones the reflective description rebuilds. A disagreement stops the run, naming the
 * graph, because a benchmark of an arm that computes something else measures nothing.
 *
 * <p>The times it prints are one cold pass over every graph, to see that nothing is pathological;
 * they are not measurements.
 */
public final class IrLayoutHarness {

  private IrLayoutHarness() {}

  public static void main(String[] args) throws IOException {
    Path graphs = Path.of("graphs");
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--graphs")) {
        graphs = Path.of(args[++i]);
      } else {
        throw new IllegalArgumentException("usage: IrLayoutHarness [--graphs DIR]");
      }
    }
    KindTable table = KindNames.TABLE;
    var loaded = new ArrayList<LoadedGraph>();
    try (Stream<Path> files = Files.list(graphs)) {
      for (Path file : files.filter(f -> f.toString().endsWith(".graphs")).sorted().toList()) {
        loaded.addAll(
            LoadedGraph.loadAll(table, Files.readString(file, StandardCharsets.UTF_8)));
      }
    }
    if (loaded.isEmpty()) {
      throw new IllegalArgumentException("no .graphs files in " + graphs);
    }
    EdgeCases.run(table);
    run(table, loaded);
  }

  static void run(KindTable table, List<LoadedGraph> loaded) {
    long nodes = 0;
    var seen = new TreeSet<String>();
    long recordsBuild = 0;
    long recordsAnalyze = 0;
    long[] build = new long[2];
    long[] analyze = new long[2];
    long[] rowBytes = new long[2];
    long[] tableBytes = new long[2];
    String[] layouts = {"A", "B"};
    for (LoadedGraph g : loaded) {
      nodes += g.size();
      for (int k : g.kind()) {
        seen.add(table.kind(k).name());
      }
      long t = System.nanoTime();
      List<VarkaVectorIR> records = RecordsArm.build(g);
      recordsBuild += System.nanoTime() - t;
      require(records.equals(VarkaIrDescription.rebuild(g.source())), g,
          "the records built by constructors differ from the ones the description rebuilds");
      t = System.nanoTime();
      Facts expected = RecordsArm.analyze(records);
      recordsAnalyze += System.nanoTime() - t;
      require(expected.distinct() == g.size(), g,
          "the records hold " + expected.distinct() + " distinct nodes, the description "
              + g.size());
      for (int l = 0; l < layouts.length; l++) {
        boolean a = layouts[l].equals("A");
        try (FfmRows rows = FfmRows.create(layouts[l], g.size(),
            a ? g.listInts() : g.wideInts(table), a ? g.listNodes() : g.wideNodes(table))) {
          t = System.nanoTime();
          int[] roots = rows.build(g);
          build[l] += System.nanoTime() - t;
          require(rows.count == g.size(), g, "layout " + layouts[l] + " holds " + rows.count
              + " rows for " + g.size() + " nodes");
          t = System.nanoTime();
          Facts facts = rows.analyze(roots);
          analyze[l] += System.nanoTime() - t;
          require(facts.sameAs(expected), g, "layout " + layouts[l] + " computes " + facts
              + " where the records compute " + expected);
          List<VarkaVectorIR> back =
              VarkaIrDescription.rebuild(rows.toGraph(g, roots));
          require(back.equals(records), g,
              "layout " + layouts[l] + " does not round-trip to the same records");
          rowBytes[l] += rows.bytes();
          tableBytes[l] += rows.tableBytes();
        }
      }
    }
    System.out.printf("graphs %d, nodes %d, kinds %d of %d%n", loaded.size(), nodes, seen.size(),
        table.size());
    System.out.println("agreement: records, layout A and layout B agree on every graph "
        + "(distinct nodes, interval facts, round trip)");
    for (int l = 0; l < layouts.length; l++) {
      System.out.printf("layout %s: %d bytes in rows and pool, %.2f bytes a node; hash-consing"
          + " tables %.2f bytes a node%n", layouts[l], rowBytes[l], rowBytes[l] / (double) nodes,
          tableBytes[l] / (double) nodes);
    }
    System.out.printf("one cold pass, ms (not a measurement): records build %d analyze %d;"
        + " A build %d analyze %d; B build %d analyze %d%n", recordsBuild / 1_000_000,
        recordsAnalyze / 1_000_000, build[0] / 1_000_000, analyze[0] / 1_000_000,
        build[1] / 1_000_000, analyze[1] / 1_000_000);
  }

  private static void require(boolean ok, LoadedGraph g, String message) {
    if (!ok) {
      throw new IllegalStateException("graph " + g.name() + ": " + message);
    }
  }
}
