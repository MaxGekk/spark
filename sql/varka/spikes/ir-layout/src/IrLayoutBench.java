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
import java.lang.instrument.Instrumentation;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Step 5 of the spike: the measures of plan section 3.3, one arm, one graph and one measure in each
 * JVM, so that no arm warms another's code and each cold sample is a first run. Modes:
 *
 * <ul>
 *   <li>{@code cold}: a fresh JVM; each measure called once, timed, and printed as one line.
 *   <li>{@code warm}: one measure ({@code --op}) in warm-up iterations then measured iterations of
 *       a fixed time each, printing ns an operation for every measured iteration.
 *   <li>{@code bytes}: bytes a node, which needs {@code -javaagent} with {@link SizeAgent}.
 * </ul>
 *
 * The measures: {@code build} (from the description to the arm's store, hash-consing included for
 * the flat arms), {@code intern} (hashing and equality of every node: the flat arms build the same
 * graph onto the populated store, so every probe hits; records call {@code hashCode} and
 * {@code equals} on every node against a second copy, which walk the subtree), and
 * {@code analyze} (the interval pass of {@link RowTransfer}).
 */
public final class IrLayoutBench {

  /** Keeps results live so the JIT cannot drop the work. */
  static volatile long sink;

  private IrLayoutBench() {}

  /** One arm over one graph. */
  abstract static class Case {
    final LoadedGraph g;

    Case(LoadedGraph g) {
      this.g = g;
    }

    abstract void build();

    /** What {@link #intern} needs before it is timed: not part of any measure. */
    abstract void prepareIntern();

    abstract void intern();

    abstract void analyze();

    /** Bytes of the arm's store for the graph, and whether the figure is measured or computed. */
    abstract long bytes();

    abstract boolean bytesMeasured();

    void run(String op) {
      switch (op) {
        case "build" -> build();
        case "intern" -> intern();
        case "analyze" -> analyze();
        default -> throw new IllegalArgumentException("no measure " + op);
      }
    }
  }

  static final class RecordsCase extends Case {
    private VarkaVectorIR[] all;
    private List<VarkaVectorIR> roots;
    private VarkaVectorIR[] first;
    private VarkaVectorIR[] second;

    private final boolean identity;

    RecordsCase(LoadedGraph g, boolean identity) {
      super(g);
      this.identity = identity;
    }

    @Override
    void build() {
      all = RecordsArm.buildAll(g);
      roots = RecordsArm.roots(g, all);
    }

    @Override
    void prepareIntern() {
      first = RecordsArm.buildAll(g);
      second = RecordsArm.buildAll(g);
    }

    @Override
    void intern() {
      long h = 0;
      boolean equal = true;
      for (int i = 0; i < first.length; i++) {
        h += first[i].hashCode();
        equal &= first[i].equals(second[i]);
      }
      if (!equal) {
        throw new IllegalStateException("equal graphs compare unequal");
      }
      sink += h;
    }

    @Override
    void analyze() {
      sink += (identity ? RecordsArm.analyzeByIdentity(roots) : RecordsArm.analyze(roots))
          .checksum();
    }

    @Override
    long bytes() {
      Instrumentation inst = SizeAgent.instrumentation();
      long total = 0;
      for (VarkaVectorIR node : all) {
        total += inst.getObjectSize(node);
        if (node instanceof VarkaVectorIR.InRanges r) {
          // The bounds are a list of boxed ints: the list, its array, and an Integer (16 bytes)
          // for each value outside the cache of -128 to 127.
          total += inst.getObjectSize(r.bounds()) + inst.getObjectSize(r.bounds().toArray());
          for (int bound : r.bounds()) {
            total += bound >= -128 && bound <= 127 ? 0 : 16;
          }
        }
      }
      return total;
    }

    @Override
    boolean bytesMeasured() {
      return true;
    }
  }

  static final class RowsCase extends Case {
    private final String layout;
    private final KindTable table = KindNames.TABLE;
    private FfmRows rows;
    private int[] roots;

    RowsCase(LoadedGraph g, String layout) {
      super(g);
      this.layout = layout;
    }

    @Override
    void build() {
      if (rows != null) {
        rows.close();
      }
      boolean listOnly = !FfmRows.spillsWide(layout);
      rows = FfmRows.create(layout, g.size(), listOnly ? g.listInts() : g.wideInts(table),
          listOnly ? g.listNodes() : g.wideNodes(table));
      roots = rows.build(g);
    }

    @Override
    void prepareIntern() {
      if (rows == null) {
        build();
      }
    }

    @Override
    void intern() {
      sink += rows.build(g).length + rows.count;
    }

    @Override
    void analyze() {
      sink += rows.analyze(roots).checksum();
    }

    @Override
    long bytes() {
      return rows.bytes();
    }

    @Override
    boolean bytesMeasured() {
      return false;
    }
  }

  public static void main(String[] args) throws IOException {
    String arm = null;
    String graph = null;
    String mode = "cold";
    String op = "build";
    Path graphs = Path.of("graphs");
    double seconds = 2;
    int warmup = 3;
    int iterations = 5;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--arm" -> arm = args[++i];
        case "--graph" -> graph = args[++i];
        case "--mode" -> mode = args[++i];
        case "--op" -> op = args[++i];
        case "--graphs" -> graphs = Path.of(args[++i]);
        case "--seconds" -> seconds = Double.parseDouble(args[++i]);
        case "--warmup" -> warmup = Integer.parseInt(args[++i]);
        case "--iterations" -> iterations = Integer.parseInt(args[++i]);
        default -> throw new IllegalArgumentException("unknown option " + args[i]);
      }
    }
    if (arm == null || graph == null) {
      throw new IllegalArgumentException("usage: IrLayoutBench --arm ARM --graph NAME"
          + " [--mode cold|warm|bytes] [--op build|intern|analyze] [--graphs DIR]");
    }
    if (arm.startsWith("V")) {
      IrLayoutHarness.registerEarlyAccessLayouts();
    }
    LoadedGraph g = find(graphs, graph);
    Case c = switch (arm) {
      case "records" -> new RecordsCase(g, false);
      case "records-id" -> new RecordsCase(g, true);
      default -> new RowsCase(g, arm);
    };
    switch (mode) {
      case "cold" -> cold(c, arm);
      case "warm" -> warm(c, arm, op, seconds, warmup, iterations);
      case "bytes" -> bytes(c, arm);
      default -> throw new IllegalArgumentException("no mode " + mode);
    }
  }

  private static LoadedGraph find(Path dir, String name) throws IOException {
    KindTable table = KindNames.TABLE;
    try (Stream<Path> files = Files.list(dir)) {
      for (Path file : files.filter(f -> f.toString().endsWith(".graphs")).sorted().toList()) {
        for (LoadedGraph g : LoadedGraph.loadAll(table,
            Files.readString(file, StandardCharsets.UTF_8))) {
          if (g.name().equals(name)) {
            return g;
          }
        }
      }
    }
    throw new IllegalArgumentException("no graph " + name + " in " + dir);
  }

  private static void cold(Case c, String arm) {
    long t = System.nanoTime();
    c.build();
    long build = System.nanoTime() - t;
    t = System.nanoTime();
    c.analyze();
    long analyze = System.nanoTime() - t;
    c.prepareIntern();
    t = System.nanoTime();
    c.intern();
    long intern = System.nanoTime() - t;
    System.out.printf("COLD %s %s nodes %d build_ns %d analyze_ns %d intern_ns %d%n", arm,
        c.g.name(), c.g.size(), build, analyze, intern);
  }

  private static void warm(Case c, String arm, String op, double seconds, int warmup,
      int iterations) {
    c.build();
    c.prepareIntern();
    long window = (long) (seconds * 1e9);
    for (int it = 0; it < warmup + iterations; it++) {
      long start = System.nanoTime();
      long end = start + window;
      long n = 0;
      long now;
      do {
        c.run(op);
        n++;
        now = System.nanoTime();
      } while (now < end);
      if (it >= warmup) {
        System.out.printf("WARM %s %s nodes %d op %s iteration %d ns_per_op %.1f%n", arm,
            c.g.name(), c.g.size(), op, it - warmup, (now - start) / (double) n);
      }
    }
  }

  private static void bytes(Case c, String arm) {
    c.build();
    long total = c.bytes();
    System.out.printf("BYTES %s %s nodes %d %s %d bytes_per_node %.2f%n", arm, c.g.name(),
        c.g.size(), c.bytesMeasured() ? "measured" : "computed", total,
        total / (double) c.g.size());
  }
}
