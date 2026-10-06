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
import java.util.stream.IntStream;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitCostCorpus.Shape;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.AddDays;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;

/**
 * Writes the graphs the IR-storage spike measures (VARKA-291) as descriptions
 * ({@link VarkaIrDescription}): the shapes of {@link VarkaEmitCostCorpus}, one file for each of
 * its families, every graph checked on the way out to rebuild into records equal to the ones it
 * was read from.
 *
 * <pre>
 * build/sbt "catalyst/Test/runMain
 *   org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaIrLayoutExport OUTDIR"
 * </pre>
 */
public final class VarkaIrLayoutExport {

  private VarkaIrLayoutExport() {}

  /** The corpus's shapes by family, in the order the corpus draws them. */
  static List<Shape> corpus() {
    var shapes = new ArrayList<Shape>();
    shapes.addAll(VarkaEmitCostCorpus.ladders());
    shapes.addAll(VarkaEmitCostCorpus.fuzz());
    shapes.addAll(VarkaEmitCostCorpus.wide());
    shapes.addAll(VarkaEmitCostCorpus.pastCeiling());
    shapes.addAll(grown());
    return shapes;
  }

  /**
   * The graphs the corpus does not have: wide ones of the size the spike's gate is read at, and
   * deep ones. The corpus's widest shapes are about six thousand nodes and every entry in it is a
   * tree four deep, so a layout's cost in graph size and in depth is seen here and not there.
   *
   * <p>"grown ladder" is the size ladder's entry repeated, five nodes each besides the two it
   * shares: 200 entries are about a thousand nodes, where Varka's projections are, and 2000 are
   * ten thousand, the benchmark's size. "deep chain" is one output, a date shifted by a literal a
   * given number of times, two nodes a level: the depth at which a record's hash walks the whole
   * chain on every lookup.
   */
  static List<Shape> grown() {
    var shapes = new ArrayList<Shape>();
    for (int n : new int[] {200, 2000}) {
      List<VarkaVectorIR> roots =
          IntStream.range(0, n).mapToObj(VarkaEmitCostCorpus::ladderEntry).toList();
      shapes.add(new Shape("grown ladder", n, roots, 1, n));
    }
    for (int depth : new int[] {16, 128, 1024}) {
      VarkaVectorIR date = new ColumnRef(0);
      for (int level = 0; level < depth; level++) {
        date = new AddDays(date, new LiteralSlot(level));
      }
      shapes.add(new Shape("deep chain", depth, List.of(date), 1, depth));
    }
    return shapes;
  }

  /** A file-name-safe name for a shape: its family and its index. */
  static String nameOf(Shape shape) {
    return shape.family().replaceAll("[^A-Za-z0-9]+", "_") + "-" + shape.index();
  }

  /**
   * The description of {@code roots}, after checking that it rebuilds into equal records, both
   * from the graph and from its text.
   */
  static VarkaIrDescription.Graph describeChecked(
      String name, List<VarkaVectorIR> roots, int numInputs, int numLiterals) {
    var graph = VarkaIrDescription.describe(name, roots, numInputs, numLiterals);
    check(name + " from the graph", roots, VarkaIrDescription.rebuild(graph));
    var parsed = VarkaIrDescription.parse(VarkaIrDescription.toText(graph));
    if (parsed.size() != 1 || !parsed.get(0).equals(graph)) {
      throw new IllegalStateException(name + ": its text does not parse back to the same graph");
    }
    return graph;
  }

  private static void check(String what, List<VarkaVectorIR> expected, List<VarkaVectorIR> actual) {
    if (!expected.equals(actual)) {
      for (int i = 0; i < expected.size(); i++) {
        if (!expected.get(i).equals(actual.get(i))) {
          throw new IllegalStateException(what + ": output " + i + " rebuilds as "
              + VarkaVectorIR.canonical(actual.get(i)) + " and not as "
              + VarkaVectorIR.canonical(expected.get(i)));
        }
      }
      throw new IllegalStateException(what + ": " + expected.size() + " outputs rebuild as "
          + actual.size());
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      throw new IllegalArgumentException("usage: VarkaIrLayoutExport OUTDIR");
    }
    Path out = Path.of(args[0]);
    Files.createDirectories(out);
    var byFamily = new java.util.LinkedHashMap<String, StringBuilder>();
    var sizes = new java.util.LinkedHashMap<String, long[]>();
    for (Shape shape : corpus()) {
      var graph = describeChecked(
          nameOf(shape), shape.roots(), shape.numInputs(), shape.numLiterals());
      String file = shape.family().replaceAll("[^A-Za-z0-9]+", "_") + ".graphs";
      byFamily.computeIfAbsent(file, f -> new StringBuilder())
          .append(VarkaIrDescription.toText(graph));
      long[] total = sizes.computeIfAbsent(file, f -> new long[3]);
      total[0]++;
      total[1] += graph.nodes().size();
      total[2] = Math.max(total[2], graph.nodes().size());
    }
    for (var entry : byFamily.entrySet()) {
      Files.writeString(out.resolve(entry.getKey()), entry.getValue(), StandardCharsets.UTF_8);
      long[] total = sizes.get(entry.getKey());
      System.out.printf("%-28s %5d graphs, %8d nodes, largest %6d%n",
          entry.getKey(), total[0], total[1], total[2]);
    }
  }
}
