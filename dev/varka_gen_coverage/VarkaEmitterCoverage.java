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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.jacoco.core.analysis.Analyzer;
import org.jacoco.core.analysis.CoverageBuilder;
import org.jacoco.core.analysis.IClassCoverage;
import org.jacoco.core.analysis.ICounter;
import org.jacoco.core.analysis.IMethodCoverage;
import org.jacoco.core.analysis.ISourceFileCoverage;
import org.jacoco.core.data.ExecutionData;
import org.jacoco.core.data.ExecutionDataStore;
import org.jacoco.core.tools.ExecFileLoader;

/**
 * The branch coverage of Varka's emitter and compiler (VARKA-265), from the execution file
 * {@code dev/varka_gen_coverage.sh} writes while every Varka suite runs under the JaCoCo agent -
 * the same run whose emitted classes {@link VarkaGenCoverage} reads.
 *
 * <p>The emitter is {@code codegen.varka}, the compiler the {@code Varka*},
 * {@code CompiledVarka*} and {@code PartialVarka*} classes of {@code codegen}; their classes are
 * catalyst's built ones, which must be the bytes the suites ran, so a class whose name has
 * execution data under another id is reported as stale rather than counted as missed. The
 * report gives per source file the branches the suites took, the methods they never entered,
 * each line of {@code Slots} and {@code Analysis} with a branch never taken - the classes the
 * mutation run mutates - and, for each mechanism of {@code VarkaEmitMechanism}, whether the line
 * that records it ran under the suites, beside whether VARKA-280's reach reports saw it.
 *
 * <pre>
 *   java -cp &lt;jacoco core, asm&gt; VarkaEmitterCoverage.java EXEC CLASSES SOURCES REACH REPORT
 * </pre>
 */
public final class VarkaEmitterCoverage {

  static final String CODEGEN = "org/apache/spark/sql/catalyst/expressions/codegen/";
  static final String EMITTER = CODEGEN + "varka/";
  static final Pattern COMPILER = Pattern.compile("(Varka|CompiledVarka|PartialVarka)[^/]*");
  /** The classes whose every missed branch is listed: the mutation run's targets. */
  static final List<String> DETAILED = List.of("Slots.java", "Analysis.java");
  static final Pattern MECHANISM = Pattern.compile("^  ([A-Z_]+)\\(Family\\.");
  static final Pattern SITE = Pattern.compile("VarkaEmitMechanism\\.([A-Z_]+)\\b");
  static final Pattern NEVER = Pattern.compile("^\\| [a-z]+ \\| `([A-Z_]+)`.*\\| never \\|$");

  /** Instructions, branches and methods of a set of classes. */
  static final class Counts {
    long ci;
    long mi;
    long cb;
    long mb;
    long methods;
    long entered;

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

  public static void main(String[] args) throws IOException {
    if (args.length != 5) {
      throw new IllegalArgumentException(
          "usage: VarkaEmitterCoverage EXEC CLASSES SOURCES REACH REPORT");
    }
    ExecFileLoader loader = new ExecFileLoader();
    loader.load(new File(args[0]));
    ExecutionDataStore store = loader.getExecutionDataStore();
    Path classes = Path.of(args[1]);
    Path sources = Path.of(args[2]);
    Path reach = Path.of(args[3]);

    CoverageBuilder builder = new CoverageBuilder();
    Analyzer analyzer = new Analyzer(store, builder);
    List<Path> files;
    try (Stream<Path> walk = Files.walk(classes.resolve(CODEGEN))) {
      files = walk.filter(VarkaEmitterCoverage::inScope).sorted().toList();
    }
    for (Path f : files) {
      analyzer.analyzeClass(Files.readAllBytes(f), f.toString());
    }
    // A class the suites ran under other bytes: its name has data, but not under this id.
    Set<String> ids = new HashSet<>();
    Map<String, Boolean> matched = new TreeMap<>();
    for (IClassCoverage c : builder.getClasses()) {
      ids.add(c.getName());
      matched.put(c.getName(), store.get(c.getId()) != null);
    }
    List<String> stale = new ArrayList<>();
    for (ExecutionData d : store.getContents()) {
      if (ids.contains(d.getName()) && !matched.get(d.getName())) {
        stale.add(d.getName());
      }
    }

    Map<String, Counts> areas = new TreeMap<>();
    Map<String, Counts> byFile = new TreeMap<>();
    Map<String, List<String>> neverEntered = new TreeMap<>();
    for (IClassCoverage c : builder.getClasses()) {
      String file = c.getPackageName() + "/" + c.getSourceFileName();
      Counts area = areas.computeIfAbsent(area(c.getPackageName()), x -> new Counts());
      Counts fc = byFile.computeIfAbsent(file, x -> new Counts());
      for (IMethodCoverage m : c.getMethods()) {
        // Synthetic lambdas and bridges count with their code, but a method is listed by name.
        boolean entered = m.getInstructionCounter().getCoveredCount() > 0;
        for (Counts k : List.of(area, fc)) {
          k.add(m.getInstructionCounter(), m.getBranchCounter());
          k.methods++;
          if (entered) {
            k.entered++;
          }
        }
        if (!entered && !m.getName().startsWith("lambda$")) {
          String simple = c.getName().substring(c.getName().lastIndexOf('/') + 1);
          neverEntered.computeIfAbsent(file, x -> new ArrayList<>())
              .add(simple + "." + m.getName() + (m.getFirstLine() > 0
                  ? " (line " + m.getFirstLine() + ")" : ""));
        }
      }
    }

    Map<String, ISourceFileCoverage> sourceFiles = new TreeMap<>();
    for (ISourceFileCoverage s : builder.getSourceFiles()) {
      sourceFiles.put(s.getPackageName() + "/" + s.getName(), s);
    }
    Map<String, List<String>> detail = new LinkedHashMap<>();
    for (String name : DETAILED) {
      String key = EMITTER.substring(0, EMITTER.length() - 1) + "/" + name;
      ISourceFileCoverage s = sourceFiles.get(key);
      List<String> text = Files.readAllLines(sources.resolve(key));
      List<String> rows = new ArrayList<>();
      if (s != null) {
        for (int line = s.getFirstLine(); line > 0 && line <= s.getLastLine(); line++) {
          ICounter b = s.getLine(line).getBranchCounter();
          if (b.getMissedCount() > 0) {
            rows.add("| " + line + " | " + b.getMissedCount() + " of " + b.getTotalCount()
                + " | " + (b.getCoveredCount() == 0 && s.getLine(line).getInstructionCounter()
                    .getCoveredCount() == 0 ? "never ran" : "ran")
                + " | `" + text.get(line - 1).strip().replace("|", "\\|").replace("`", "'")
                + "` |");
          }
        }
      }
      detail.put(name, rows);
    }

    Files.writeString(Path.of(args[4]), render(areas, byFile, neverEntered, detail,
        mechanisms(sources, sourceFiles, reach), stale), StandardCharsets.UTF_8);
  }

  static boolean inScope(Path f) {
    String s = f.toString().replace(File.separatorChar, '/');
    if (!s.endsWith(".class")) {
      return false;
    }
    int at = s.indexOf(CODEGEN);
    String rest = s.substring(at + CODEGEN.length(), s.length() - ".class".length());
    return rest.startsWith("varka/") ? rest.indexOf('/', 6) < 0
        : COMPILER.matcher(rest).matches();
  }

  static String area(String pkg) {
    return pkg.equals(EMITTER.substring(0, EMITTER.length() - 1))
        ? "emitter (codegen.varka)" : "compiler (codegen.Varka*)";
  }

  /**
   * Per mechanism: the lines that record it and whether they ran under the suites, beside whether
   * the reach reports list it as reached by none. A mechanism is recorded in one of three ways,
   * and each is read where it really happens: a {@code reach(VarkaEmitMechanism.X)} call, where
   * a call choosing between two mechanisms ran both arms only if its condition took both
   * branches; a size counter of {@code VarkaEmitTrace}, paired with its mechanism by the order of
   * {@code counters()} and {@code COUNTED}, at the lines that increment it; and a guard's reason,
   * through {@code VarkaEmitMechanism.guard}, where the line says the guards ran and not which.
   */
  static List<String> mechanisms(Path sources, Map<String, ISourceFileCoverage> files, Path reach)
      throws IOException {
    String pkg = EMITTER.substring(0, EMITTER.length() - 1);
    Path dir = sources.resolve(pkg);
    Map<String, List<String>> sites = new LinkedHashMap<>();
    Map<String, String> status = new LinkedHashMap<>();
    for (String line : Files.readAllLines(dir.resolve("VarkaEmitMechanism.java"))) {
      Matcher m = MECHANISM.matcher(line);
      if (m.find()) {
        sites.put(m.group(1), new ArrayList<>());
      }
    }
    List<String> guardSites = new ArrayList<>();
    Map<String, List<String>> text = new TreeMap<>();
    try (Stream<Path> walk = Files.list(dir)) {
      for (Path f : walk.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
        text.put(f.getFileName().toString().replace(".java", ""), Files.readAllLines(f));
      }
    }
    // The size counters: field i of counters() counts mechanism i of COUNTED.
    String trace = String.join("\n", text.get("VarkaEmitTrace"));
    List<String> fields = tokens(trace, "return new int[] {", "}");
    List<String> counted = tokens(trace, "COUNTED = {", "}").stream()
        .map(t -> t.replace("VarkaEmitMechanism.", "")).toList();
    if (fields.size() != counted.size()) {
      throw new IllegalStateException("VarkaEmitTrace's counters() and COUNTED differ in length");
    }
    Map<String, String> counter = new LinkedHashMap<>();
    for (int i = 0; i < fields.size(); i++) {
      String f = fields.get(i);
      counter.put(f.endsWith(".size()") ? f.replace(".size()", ".add(") : f + "++",
          counted.get(i));
    }
    for (Map.Entry<String, List<String>> e : text.entrySet()) {
      String name = e.getKey();
      if (name.equals("VarkaEmitMechanism") || name.equals("VarkaEmitTrace")) {
        continue;
      }
      List<String> lines = e.getValue();
      ISourceFileCoverage cov = files.get(pkg + "/" + name + ".java");
      for (int i = 0; i < lines.size(); i++) {
        String code = lines.get(i).strip();
        if (code.startsWith("*") || code.startsWith("//") || code.startsWith("/*")) {
          continue;
        }
        int line = i + 1;
        Matcher m = SITE.matcher(code);
        List<String> here = new ArrayList<>();
        while (m.find()) {
          if (sites.containsKey(m.group(1))) {
            here.add(m.group(1));
          }
        }
        // A choice between two mechanisms: its condition is on this line or the one before.
        boolean choice = here.size() == 2 || (here.size() == 1 && code.startsWith("?"));
        for (String mech : here) {
          sites.get(mech).add(name + ":" + line);
          String verdict;
          if (choice) {
            ICounter b = branches(cov, line);
            verdict = b == null || b.getTotalCount() == 0 ? (ran(cov, line) ? "yes" : "no")
                : b.getMissedCount() == 0 ? "yes"
                : b.getCoveredCount() > 0 ? "one arm of " + b.getTotalCount() : "no";
          } else {
            verdict = ran(cov, line) ? "yes" : "no";
          }
          status.merge(mech, verdict, VarkaEmitterCoverage::better);
        }
        for (Map.Entry<String, String> c : counter.entrySet()) {
          if (code.contains("." + c.getKey())) {
            sites.get(c.getValue()).add(name + ":" + line);
            status.merge(c.getValue(), ran(cov, line) ? "yes" : "no", VarkaEmitterCoverage::better);
          }
        }
        if (code.contains("VarkaEmitMechanism.guard(")) {
          guardSites.add((ran(cov, line) ? "the guards' line ran" : "no") + " (" + name + ":"
              + line + "); which reason, unread");
        }
      }
    }
    // The guards reached only through a reason: no line names them, the reasons' line speaks.
    for (String mech : sites.keySet()) {
      if (mech.startsWith("GUARD_") && sites.get(mech).isEmpty() && !guardSites.isEmpty()) {
        status.put(mech, String.join("; ", guardSites));
      }
    }
    Set<String> never = neverReached(reach);
    List<String> rows = new ArrayList<>();
    sites.forEach((mech, where) -> {
      String st = status.getOrDefault(mech, "no site found");
      rows.add("| `" + mech + "` | " + (where.isEmpty() ? "-" : String.join(", ", where))
          + " | " + (st.equals("no") ? "**no**" : st) + " | "
          + (never.contains(mech) ? "never" : "") + " |");
    });
    return rows;
  }

  /** The comma-separated tokens between an opening text and the next closing one. */
  static List<String> tokens(String text, String open, String close) {
    int from = text.indexOf(open);
    if (from < 0) {
      throw new IllegalStateException("no '" + open + "' in VarkaEmitTrace");
    }
    from += open.length();
    int to = text.indexOf(close, from);
    List<String> out = new ArrayList<>();
    for (String t : text.substring(from, to).split(",")) {
      if (!t.isBlank()) {
        out.add(t.strip());
      }
    }
    return out;
  }

  /** Of two verdicts on one mechanism's sites, the one that says more of it ran. */
  static String better(String a, String b) {
    if (a.equals("yes") || b.equals("yes")) {
      return "yes";
    }
    return a.equals("no") ? b : a;
  }

  /** Mechanisms every reach report lists as reached by none. */
  static Set<String> neverReached(Path reach) throws IOException {
    Set<String> never = null;
    try (Stream<Path> walk = Files.list(reach)) {
      for (Path f : walk.filter(p -> p.toString().endsWith(".md")).sorted().toList()) {
        Set<String> here = new HashSet<>();
        for (String line : Files.readAllLines(f)) {
          Matcher m = NEVER.matcher(line);
          if (m.find()) {
            here.add(m.group(1));
          }
        }
        if (never == null) {
          never = here;
        } else {
          never.retainAll(here);
        }
      }
    }
    return never == null ? Set.of() : never;
  }

  /** The branches of a line, or of the line before when it holds the condition. */
  static ICounter branches(ISourceFileCoverage cov, int line) {
    if (cov == null) {
      return null;
    }
    ICounter here = cov.getLine(line).getBranchCounter();
    return here.getTotalCount() > 0 ? here : cov.getLine(line - 1).getBranchCounter();
  }

  static boolean ran(ISourceFileCoverage cov, int line) {
    return cov != null && cov.getLine(line).getInstructionCounter().getCoveredCount() > 0;
  }

  static String render(Map<String, Counts> areas, Map<String, Counts> byFile,
      Map<String, List<String>> neverEntered, Map<String, List<String>> detail,
      List<String> mechanisms, List<String> stale) {
    List<String> out = new ArrayList<>();
    out.add("# Coverage of the emitter and the compiler");
    out.add("");
    out.add("Generated by `dev/varka_gen_coverage.sh` (VARKA-265): every Varka suite of "
        + "`catalyst` and `sql/core`, as the gate's wide step runs them, under the JaCoCo agent - "
        + "the run that also writes `generated.md` - over catalyst's built classes. Do not edit; "
        + "regenerate.");
    out.add("");
    if (!stale.isEmpty()) {
      out.add("**Stale**: these classes ran under other bytes than the built ones, and count as "
          + "missed: " + String.join(", ", stale) + ".");
      out.add("");
    }
    out.add("| area | methods | entered | instructions | branches | branches missed |");
    out.add("| :--- | ---: | ---: | ---: | ---: | ---: |");
    areas.forEach((a, c) -> out.add("| " + a + " | " + c.methods + " | " + c.entered + " | "
        + Counts.pct(c.ci, c.mi) + " | " + Counts.pct(c.cb, c.mb) + " | " + c.mb + " |"));
    out.add("");
    out.add("## By source file");
    out.add("");
    out.add("Nested classes and lambdas count with the file that holds them.");
    out.add("");
    out.add("| file | methods | entered | instructions | branches | branches missed |");
    out.add("| :--- | ---: | ---: | ---: | ---: | ---: |");
    byFile.forEach((f, c) -> out.add("| " + f.substring(f.lastIndexOf('/') + 1) + " | "
        + c.methods + " | " + c.entered + " | " + Counts.pct(c.ci, c.mi) + " | "
        + Counts.pct(c.cb, c.mb) + " | " + c.mb + " |"));
    out.add("");
    out.add("## The mechanisms, by the line that records each");
    out.add("");
    out.add("Whether a line recording the mechanism ran under the suites, beside whether "
        + "VARKA-280's reach reports (`reach/`, the IR fuzzer's and the coverage suite's "
        + "emissions) saw it: `never` there and `yes` here is a mechanism some other suite "
        + "reaches; `**no**` here is one no suite reaches.");
    out.add("");
    out.add("| mechanism | recorded at | ran under the suites | reach reports |");
    out.add("| :--- | :--- | :---: | :---: |");
    out.addAll(mechanisms);
    out.add("");
    for (Map.Entry<String, List<String>> e : detail.entrySet()) {
      out.add("## Branches never taken in " + e.getKey().replace(".java", ""));
      out.add("");
      out.add("Every line with a branch no suite took: the mutation run's survivors on these "
          + "lines are expected; a survivor on a line not listed here had a test reach it.");
      out.add("");
      out.add("| line | branches missed | the line | source |");
      out.add("| ---: | ---: | :--- | :--- |");
      out.addAll(e.getValue());
      out.add("");
    }
    out.add("## Methods never entered");
    out.add("");
    neverEntered.forEach((f, ms) -> {
      out.add("* " + f.substring(f.lastIndexOf('/') + 1) + ": " + String.join(", ", ms));
    });
    out.add("");
    return String.join("\n", out);
  }

  private VarkaEmitterCoverage() {}
}
