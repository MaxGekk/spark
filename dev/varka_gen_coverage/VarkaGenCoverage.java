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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaDebugInfo;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaDebugInfoReader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaMethodNames;
import org.jacoco.core.analysis.Analyzer;
import org.jacoco.core.analysis.CoverageBuilder;
import org.jacoco.core.analysis.IClassCoverage;
import org.jacoco.core.analysis.ICounter;
import org.jacoco.core.analysis.ILine;
import org.jacoco.core.analysis.IMethodCoverage;
import org.jacoco.core.data.ExecutionDataStore;
import org.jacoco.core.tools.ExecFileLoader;

/**
 * The coverage of the classes Varka emits (VARKA-285), from a JaCoCo execution file and the
 * classes the agent dumped: run by {@code dev/varka_gen_coverage.sh}, which puts
 * {@code org.jacoco.core}, ASM and catalyst's classes on the class path, so that no build gains a
 * dependency.
 *
 * <p>JaCoCo's own report cannot read this data: the suites emit one class name with different
 * bytes, and a {@link CoverageBuilder} holds one class per name. So each dumped class is analysed
 * with a builder of its own, against the execution data of its own id. Only classes carrying a
 * Varka line map are kernels; a class ran when one of its kernel methods - the dispatch, a
 * driver, a stage, a loop or an epilogue - was entered, not merely its constructor. A class's
 * coverage sums per method kind ({@link VarkaMethodNames}) and per IR operation, each line being
 * the node the class's line map names.
 *
 * <pre>
 *   java -cp &lt;jacoco core, asm, catalyst classes&gt; VarkaGenCoverage.java EXEC DUMPDIR REPORT
 * </pre>
 */
public final class VarkaGenCoverage {

  /** Instructions and branches, covered and missed. */
  static final class Counts {
    long ci;
    long mi;
    long cb;
    long mb;

    void add(ICounter instructions, ICounter branches) {
      ci += instructions.getCoveredCount();
      mi += instructions.getMissedCount();
      cb += branches.getCoveredCount();
      mb += branches.getMissedCount();
    }

    static String pct(long covered, long missed) {
      long total = covered + missed;
      return total == 0 ? "-" : String.format("%.1f%%", 100.0 * covered / total);
    }
  }

  /** Methods of one kind in the classes that ran: how many, how many entered, and their code. */
  static final class Kind {
    long methods;
    long entered;
    final Counts counts = new Counts();
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 3) {
      throw new IllegalArgumentException("usage: VarkaGenCoverage EXEC DUMPDIR REPORT");
    }
    ExecFileLoader loader = new ExecFileLoader();
    loader.load(new File(args[0]));
    ExecutionDataStore store = loader.getExecutionDataStore();
    List<Path> dumped;
    try (Stream<Path> files = Files.walk(Path.of(args[1]))) {
      dumped = files.filter(f -> f.toString().endsWith(".class")).sorted().toList();
    }

    long emitted = 0;
    long ran = 0;
    long cut = 0;
    long bothDrivers = 0;
    long oneDriverOnly = 0;
    long onlyDense = 0;
    long onlyMasked = 0;
    Map<String, long[]> families = new TreeMap<>();
    Map<String, Kind> kinds = new TreeMap<>();
    Map<String, Counts> operations = new TreeMap<>();
    Map<String, Long> missedInRunLoops = new TreeMap<>();

    for (Path file : dumped) {
      byte[] bytes = Files.readAllBytes(file);
      String lineMap = VarkaDebugInfoReader.lineMap(bytes);
      if (lineMap == null) {
        continue;
      }
      emitted++;
      CoverageBuilder builder = new CoverageBuilder();
      new Analyzer(store, builder).analyzeClass(bytes, file.toString());
      IClassCoverage cls = builder.getClasses().iterator().next();
      // Ran: a kernel method was entered. A class only constructed - a test that builds a kernel
      // and calls the wrong overload, which the interface's default refuses - did not run.
      if (cls.getMethods().stream().noneMatch(m -> m.getInstructionCounter().getCoveredCount() > 0
          && !kind(m.getName()).startsWith("other"))) {
        continue;
      }
      ran++;
      // A wide kernel's line map is cut to fit one constant (VarkaDebugInfo.TRUNCATED): its last
      // entry is a prefix, and the lines after it name no node.
      boolean truncated = lineMap.endsWith(VarkaDebugInfo.TRUNCATED);
      if (truncated) {
        cut++;
        lineMap = lineMap.substring(0, lineMap.lastIndexOf('\n') + 1);
      }
      String unmapped = truncated ? "(line map cut)" : "(no node)";
      Map<Integer, String> nodes = new HashMap<>();
      for (String entry : lineMap.split("\n")) {
        int eq = entry.indexOf('=');
        if (eq > 0) {
          nodes.put(Integer.parseInt(entry.substring(0, eq)), operation(entry.substring(eq + 1)));
        }
      }
      boolean dense = false;
      boolean masked = false;
      for (IMethodCoverage m : cls.getMethods()) {
        String kind = kind(m.getName());
        boolean entered = m.getInstructionCounter().getCoveredCount() > 0;
        if (entered && m.getName().equals(VarkaMethodNames.driver(true))) {
          dense = true;
        }
        if (entered && m.getName().equals(VarkaMethodNames.driver(false))) {
          masked = true;
        }
        Kind k = kinds.computeIfAbsent(kind, x -> new Kind());
        k.methods++;
        if (entered) {
          k.entered++;
        }
        k.counts.add(m.getInstructionCounter(), m.getBranchCounter());
        boolean runLoop = entered && VarkaMethodNames.isLoop(m.getName());
        for (int line = m.getFirstLine(); line >= 0 && line <= m.getLastLine(); line++) {
          ILine l = m.getLine(line);
          if (l.getInstructionCounter().getTotalCount() == 0) {
            continue;
          }
          String op = nodes.getOrDefault(line, unmapped);
          operations.computeIfAbsent(op, x -> new Counts())
              .add(l.getInstructionCounter(), l.getBranchCounter());
          long missed = l.getBranchCounter().getMissedCount();
          if (runLoop && missed > 0) {
            missedInRunLoops.merge(op, missed, Long::sum);
          }
        }
      }
      // A class may have one driver only: a kernel whose nulls come from valid inputs has no
      // dense body, and one that reads no column no masked body. One body is all it has to enter.
      boolean hasDense = cls.getMethods().stream()
          .anyMatch(m -> m.getName().equals(VarkaMethodNames.driver(true)));
      boolean hasMasked = cls.getMethods().stream()
          .anyMatch(m -> m.getName().equals(VarkaMethodNames.driver(false)));
      long[] family = families.computeIfAbsent(family(cls.getName()), x -> new long[5]);
      if (dense && masked) {
        bothDrivers++;
        family[0]++;
      } else if ((dense && !hasMasked) || (masked && !hasDense)) {
        oneDriverOnly++;
        family[1]++;
      } else if (dense) {
        onlyDense++;
        family[2]++;
      } else if (masked) {
        onlyMasked++;
        family[3]++;
      } else {
        family[4]++;
      }
    }
    Files.writeString(Path.of(args[2]), render(emitted, ran, cut, bothDrivers, oneDriverOnly,
        onlyDense, onlyMasked, families, kinds, operations, missedInRunLoops),
        StandardCharsets.UTF_8);
  }

  /**
   * A class's family: its simple name without the counter or the shape hash that makes it unique,
   * which says who emitted it - {@code VarkaFusedProjection} the evaluator, {@code VarkaFusedTest}
   * the emitter suites, {@code VarkaFusedFuzz} the IR fuzzer, {@code VarkaCompositionWide} the
   * composition fuzzer.
   */
  static String family(String internalName) {
    String simple = internalName.substring(internalName.lastIndexOf('/') + 1);
    return simple.replaceAll("_[0-9a-f]+$", "").replaceAll("[0-9]+$", "");
  }

  /** Why a family's classes may run one body only, for the families that may (VARKA-284). */
  static final Map<String, String> ONE_BODY_REASONS = new TreeMap<>(Map.of(
      "VarkaFusedProjection", "the evaluator in the end-to-end suites, which hand it the batches "
          + "their tables hold: which body a batch takes is the evaluator's decision on the data, "
          + "and a test there is about a query, not a body",
      "VarkaFusedTest", "the emitter suites' tests that call a kernel directly for one property - "
          + "a status, the scratch contract, one body's behaviour - rather than through "
          + "checkMatrix, which runs whichever body its cases miss",
      "VarkaFusedFuzz", "a case whose first comparison throws stops there, and the IR fuzzer's "
          + "planted-failure tests and their shrinking throw on purpose, each failing candidate "
          + "a class of its own; these classes are attributed to them, not traced",
      "VarkaFusedFuzzLong", "as for VarkaFusedFuzz, at the long lane"));

  /** A line map node's operation: the word after its parenthesis, or a leaf's kind. */
  static String operation(String node) {
    if (node.startsWith("(")) {
      int end = 1;
      while (end < node.length() && " )".indexOf(node.charAt(end)) < 0) {
        end++;
      }
      return node.substring(1, end);
    }
    int colon = node.indexOf(':');
    return colon > 0 ? node.substring(0, colon) : node;
  }

  /** A method's kind, through {@link VarkaMethodNames}. */
  static String kind(String method) {
    for (boolean dense : new boolean[] {true, false}) {
      String side = dense ? " (dense)" : " (masked)";
      if (method.equals(VarkaMethodNames.driver(dense))) {
        return "driver" + side;
      }
      if (VarkaMethodNames.isStage(method, dense)) {
        return "stage" + side;
      }
      if (VarkaMethodNames.isLoop(method, dense)) {
        return "loop" + side;
      }
      if (VarkaMethodNames.isEpilogue(method, dense)) {
        return "epilogue" + side;
      }
    }
    return method.equals(VarkaMethodNames.DISPATCH) ? "dispatch" : "other (" + method + ")";
  }

  static String render(long emitted, long ran, long cut, long both, long oneDriver,
      long onlyDense, long onlyMasked, Map<String, long[]> families,
      Map<String, Kind> kinds, Map<String, Counts> operations, Map<String, Long> missed) {
    List<String> out = new ArrayList<>();
    out.add("# Coverage of the generated code");
    out.add("");
    out.add("Generated by `dev/varka_gen_coverage.sh` (VARKA-285): every Varka suite of "
        + "`catalyst` and `sql/core`, as the gate's wide step runs them, under the JaCoCo agent, "
        + "each emitted class analysed on its own. Do not edit; regenerate.");
    out.add("");
    out.add("## Classes");
    out.add("");
    out.add("| emitted | ran | both drivers | its only driver | only the dense one "
        + "| only the masked one | neither |");
    out.add("| ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    out.add("| " + emitted + " | " + ran + " | " + both + " | " + oneDriver + " | " + onlyDense
        + " | " + onlyMasked + " | " + (ran - both - oneDriver - onlyDense - onlyMasked) + " |");
    out.add("");
    out.add("A class with one driver - a kernel whose nulls come from valid inputs has no dense "
        + "body, one that reads no column no masked body - entered all it has. A class ran when "
        + "a kernel method was entered, not only its constructor. One that "
        + "entered the dispatch and neither driver left it before choosing a side: an empty "
        + "batch returns there, and a zero scratch address is refused there (VARKA-198).");
    out.add("");
    out.add("By family - who emitted the class (VARKA-284: every class that ran enters both "
        + "drivers, or its family is named below with the reason it may not):");
    out.add("");
    out.add("| family | both drivers | its only driver | only the dense one "
        + "| only the masked one | neither |");
    out.add("| :--- | ---: | ---: | ---: | ---: | ---: |");
    families.forEach((f, v) -> out.add("| " + f + " | " + v[0] + " | " + v[1] + " | " + v[2]
        + " | " + v[3] + " | " + v[4] + " |"));
    out.add("");
    ONE_BODY_REASONS.forEach((f, why) -> out.add("* `" + f + "`: " + why + "."));
    out.add("");
    out.add("## Methods of the classes that ran, by kind");
    out.add("");
    out.add("| kind | methods | entered | instructions | branches |");
    out.add("| :--- | ---: | ---: | ---: | ---: |");
    kinds.forEach((k, v) -> out.add("| " + k + " | " + v.methods + " | " + v.entered + " | "
        + Counts.pct(v.counts.ci, v.counts.mi) + " | " + Counts.pct(v.counts.cb, v.counts.mb)
        + " |"));
    out.add("");
    out.add("## The classes that ran, by IR operation");
    out.add("");
    out.add("A line number marks where the emitter began a node, and the instructions after it "
        + "count to that node until the next mark, so a node owns the stores and loop overhead "
        + "that follow it as well - which is why the leaves (`lit`, `col`) own so much. "
        + "`(no node)` is the code before any mark - dispatch, drivers, segment setup - and "
        + "`(line map cut)` the lines past the end of a line map cut to fit one constant, in "
        + cut + " of the classes that ran.");
    out.add("");
    out.add("| operation | instructions | covered | branches | covered |");
    out.add("| :--- | ---: | ---: | ---: | ---: |");
    operations.forEach((op, c) -> out.add("| " + op + " | " + (c.ci + c.mi) + " | "
        + Counts.pct(c.ci, c.mi) + " | " + (c.cb + c.mb) + " | " + Counts.pct(c.cb, c.mb) + " |"));
    out.add("");
    out.add("## Missed branches inside loops that ran, by operation");
    out.add("");
    out.add("A loop method a test entered, with a branch on one of its lines that no row took: "
        + "the shapes reach the code and do not exercise it.");
    out.add("");
    out.add("| operation | missed branches |");
    out.add("| :--- | ---: |");
    missed.entrySet().stream()
        .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
        .forEach(e -> out.add("| " + e.getKey() + " | " + e.getValue() + " |"));
    out.add("");
    return String.join("\n", out);
  }

  private VarkaGenCoverage() {}
}
