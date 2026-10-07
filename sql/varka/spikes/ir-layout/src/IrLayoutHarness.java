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
    String[] layouts = {"A", "B", "C"};
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--graphs")) {
        graphs = Path.of(args[++i]);
      } else if (args[i].equals("--layouts")) {
        // A subset, to run one arm in its own JVM; "none" runs the records alone.
        String list = args[++i];
        layouts = list.equals("none") ? new String[0] : list.split(",");
      } else {
        throw new IllegalArgumentException(
            "usage: IrLayoutHarness [--graphs DIR] [--layouts A,B,C|none]");
      }
    }
    KindTable table = KindNames.TABLE;
    checkVariant();
    for (String layout : layouts) {
      if (layout.startsWith("V")) {
        registerEarlyAccessLayouts();
        break;
      }
    }
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
    EdgeCases.run(table, layouts);
    run(table, loaded, layouts);
  }

  /**
   * Prints the JDK and which kind of node classes this run holds, and fails if the run script's
   * claim (the {@code ir.variant} property, "plain" or "value") is not what the classes are: a
   * variant that silently ran the other one would be read as a result.
   */
  private static void checkVariant() {
    String claim = System.getProperty("ir.variant", "plain");
    boolean expectValue = claim.equals("value");
    int records = 0;
    int values = 0;
    for (Class<?> c : VarkaVectorIR.class.getDeclaredClasses()) {
      if (c.isRecord()) {
        records++;
        if (isValueClass(c)) {
          values++;
        }
      }
    }
    if (records == 0 || values != (expectValue ? records : 0)) {
      throw new IllegalStateException("the run claims " + claim + " node classes, but "
          + values + " of " + records + " are value classes");
    }
    System.out.println("jdk " + Runtime.version() + ", node classes: " + claim + " records ("
        + records + ")");
  }

  /** The layouts of src-ea, which only the early-access JDK compiles, register themselves. */
  static void registerEarlyAccessLayouts() {
    try {
      Class.forName("org.apache.spark.sql.catalyst.expressions.codegen.varka.EaLayouts");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("the V layouts need run-ea.sh, which compiles src-ea", e);
    }
  }

  /** {@code Class.isValue} exists only on the early-access JDK, so it is read by reflection. */
  private static boolean isValueClass(Class<?> c) {
    try {
      return (boolean) Class.class.getMethod("isValue").invoke(c);
    } catch (NoSuchMethodException e) {
      return false;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  static void run(KindTable table, List<LoadedGraph> loaded, String[] layouts) {
    long nodes = 0;
    var seen = new TreeSet<String>();
    int substituted = 0;
    long recordsBuild = 0;
    long recordsAnalyze = 0;
    long[] build = new long[layouts.length];
    long[] analyze = new long[layouts.length];
    long[] rowBytes = new long[layouts.length];
    long[] tableBytes = new long[layouts.length];
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
      if (!"value".equals(System.getProperty("ir.variant"))) {
        require(RecordsArm.analyzeByIdentity(records).sameAs(expected), g,
            "the identity-memo analysis differs from the structural one");
      }
      require(expected.distinct() == g.size(), g,
          "the records hold " + expected.distinct() + " distinct nodes, the description "
              + g.size());
      for (int l = 0; l < layouts.length; l++) {
        boolean a = !FfmRows.spillsWide(layouts[l]);
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
          int[] swap = g.substitution();
          if (l == 0 && swap[0] != swap[1]) {
            substituted++;
          }
          int rebuilt = rows.rebuild(swap[0], swap[1]);
          require(rebuilt == distinctRows(g.substitute(swap[0], swap[1])), g,
              "layout " + layouts[l] + " rebuilt to " + rebuilt + " rows, not the "
                  + distinctRows(g.substitute(swap[0], swap[1])) + " a build of the substituted"
                  + " graph holds");
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
    if (seen.size() != table.size()) {
      throw new IllegalStateException("the graphs cover " + seen.size() + " of " + table.size()
          + " kinds, so the agreement above is incomplete");
    }
    if (layouts.length > 0) {
      System.out.println("rebuild: " + substituted + " of " + loaded.size()
          + " graphs have a real substitution, and every layout rebuilt each to the rows a build"
          + " of the substituted graph holds");
    }
    System.out.println("agreement: records and layouts " + String.join(", ", layouts)
        + " agree on every graph (distinct nodes, interval facts, round trip)");
    for (int l = 0; l < layouts.length; l++) {
      System.out.printf("layout %s: %d bytes in rows and pool, %.2f bytes a node; hash-consing"
          + " tables %.2f bytes a node%n", layouts[l], rowBytes[l], rowBytes[l] / (double) nodes,
          tableBytes[l] / (double) nodes);
    }
    var cold = new StringBuilder(String.format("records build %d analyze %d",
        recordsBuild / 1_000_000, recordsAnalyze / 1_000_000));
    for (int l = 0; l < layouts.length; l++) {
      cold.append(String.format("; %s build %d analyze %d", layouts[l], build[l] / 1_000_000,
          analyze[l] / 1_000_000));
    }
    System.out.println("one cold pass, ms (not a measurement): " + cold);
  }

  /** The rows a hash-consed build of {@code g} holds, by layout C, which agreement has checked. */
  private static int distinctRows(LoadedGraph g) {
    try (FfmRows rows = FfmRows.create("C", g.size(), g.listInts(), g.listNodes())) {
      rows.build(g);
      return rows.count;
    }
  }

  private static void require(boolean ok, LoadedGraph g, String message) {
    if (!ok) {
      throw new IllegalStateException("graph " + g.name() + ": " + message);
    }
  }
}
