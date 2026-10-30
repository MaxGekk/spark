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

import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaPredicate;
import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.PartialVarkaProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler;

/**
 * The option matrix's reach by test (VARKA-507): which configurations change anything a test
 * compiled or emitted, recorded in a run of the defaults and read by a configuration's run, which
 * then runs only the tests it changes.
 *
 * <p>A test's answers follow from what Varka compiled and emitted for it and from the rows it ran
 * over. With {@code -Dvarka.matrix.reachRecord=<file>}, a run of the defaults hears every compile
 * and every kernel emitted or looked up ({@link VarkaCompileObserver}) and, for each
 * configuration, compiles and emits it again under the configuration's values laid over the
 * options it was given: a different compiled form, a decline where there was a kernel or the
 * other way round, or different bytes, moves it. A kernel looked up whose IR an earlier compile
 * produced, in a suite's set-up or an earlier test, moves where that compile moved. Each test's
 * line names the configurations that moved anything it did. With
 * {@code -Dvarka.matrix.reachFile=<file>}, a configuration's run skips a test whose line does not
 * name it: the test passes without running and records no fused count, so the declining check
 * leaves it alone, and the JVM prints how many it skipped.
 *
 * <p>Some configurations run every test, each for the reason {@link #FULL} gives, and so does a
 * test the file has no line for and a test the skip list names under the configuration, whose
 * line must stay held to account.
 */
public final class VarkaTestReach {

  /** The file a run of the defaults appends a line a test to. */
  public static final String RECORD_PROPERTY = "varka.matrix.reachRecord";

  /** The recorded file a configuration's run reads. */
  public static final String FILE_PROPERTY = "varka.matrix.reachFile";

  /**
   * The configurations that run every test, by the option they set or, for one that sets several,
   * by the whole configuration, each with its reason: what they change is not, or not only, what a
   * test compiles and emits.
   */
  public static final Map<String, String> FULL = fullRun();

  private static Map<String, String> fullRun() {
    var full = new LinkedHashMap<String, String>();
    String plan = "changes which kernels a plan has, which the plan's own tests measure";
    for (String option : List.of("fusedCeiling", "splitDriver", "severalKernels",
        "splitConditions", "rangeSets", "materializeChronoPrefix")) {
      full.put(option, plan);
    }
    full.put("lanesOverride", "a second vector species in the JVM, which every later kernel's C2 "
        + "code meets");
    full.put(VarkaMatrixRuns.ARROW_BATCH_SIZE,
        "changes the sessions' cached batches, not an option");
    full.put("exactRefusalStatus", "read by the evaluator as it runs a kernel, not only emitted");
    full.put("denseValidityOnce=false,validityByBitmap=false,validityOrFirst=false",
        "sets three options at once, so a test's pin of one could hide the others' effect");
    return Map.copyOf(full);
  }

  /**
   * The suites the recording leaves out, each with its reason: their kernels are so large that
   * emitting each again under every configuration costs the pull request's job more than running
   * the suite in full costs the matrix. A suite left out runs every test under every
   * configuration, as a test the record has no line for does.
   */
  public static final Map<String, String> UNRECORDED = Map.of(
      "VarkaCodegenGiveUpSuite", "kernels of thousands of nodes: 16 s plain, 225 s recorded",
      "VarkaSeveralKernelsSuite", "plans split into large kernels: 47 s plain, 192 s recorded",
      "VarkaSplitConditionSuite", "conditions split into many kernels: 3 s plain, 33 s recorded",
      "VarkaSplitConditionFusionSuite", "the same over fused filters: 1 s plain, 29 s recorded");

  /** Whether {@code config} runs every test: it, or the one option it sets, is in {@link #FULL}. */
  public static boolean runsEveryTest(String config) {
    return FULL.containsKey(config)
        || !config.contains(",") && FULL.containsKey(config.split("=", 2)[0].trim());
  }

  private VarkaTestReach() {}

  // ---------------------------------------------------------------------------------------------
  // Recording, in a run of the defaults
  // ---------------------------------------------------------------------------------------------

  private static final Optional<String> RECORD =
      Optional.ofNullable(System.getProperty(RECORD_PROPERTY));
  private static volatile Recorder recorder;

  /**
   * Starts the recording in a run of the defaults that asks for one; a suite calls it as it is
   * built, before its set-up compiles anything. Idempotent.
   */
  public static synchronized void install() {
    if (recorder == null && RECORD.isPresent() && VarkaMatrixRuns.CONFIG.isEmpty()) {
      List<String> configs = VarkaMatrixRuns.configurations().stream()
          .filter(c -> !runsEveryTest(c))
          .toList();
      Recorder r = new Recorder(configs);
      recorder = r;
      VarkaCompileObserver.set(new Guarded(r));
      // The configurations the record covers, which the planner reads (dev/varka_matrix_bins.py):
      // one not named here runs every test.
      append("#\tcovers\t" + String.join(",", configs));
      // What the JVM compiled outside every suite, which every test may have read.
      Runtime.getRuntime().addShutdownHook(new Thread(() -> append("*\t*\t" + names(r.outside))));
    }
  }

  /**
   * Opens a suite's window, before its set-up: what it compiles outside a test is the suite's. A
   * suite in {@link #UNRECORDED} pauses the listening instead, and writes no line.
   */
  public static void suiteBegin(String suite) {
    Recorder r = recorder;
    if (r != null) {
      r.paused = UNRECORDED.containsKey(suite);
      r.suite = ConcurrentHashMap.newKeySet();
    }
  }

  /** Closes the suite's window with a {@code suite<TAB>*} line, which joins each of its tests'. */
  public static void suiteEnd(String suite) {
    Recorder r = recorder;
    if (r != null && r.suite != null) {
      Set<String> moved = r.suite;
      r.suite = null;
      boolean paused = r.paused;
      r.paused = false;
      if (!paused) {
        append(suite + "\t*\t" + names(moved));
      }
    }
  }

  /** Begins a test's line, where the run records. */
  static void begin() {
    Recorder r = recorder;
    if (r != null) {
      r.current = ConcurrentHashMap.newKeySet();
    }
  }

  /**
   * Ends the test's line and appends it to the file, where the run records. A test that cancelled
   * at the defaults did not show what it does, so its line is {@code *}: it runs under every
   * configuration, as does every test once the recording broke.
   */
  static void end(String suite, String test, boolean cancelled) {
    Recorder r = recorder;
    if (r == null) {
      return;
    }
    Set<String> moved = r.current;
    r.current = null;
    if (!r.paused) {
      append(suite + "\t" + test + "\t" + (cancelled ? EVERY : names(moved)));
    }
  }

  /** The line's word for a test every configuration runs. */
  static final String EVERY = "*";

  private static String names(Set<String> moved) {
    return moved == null || moved.isEmpty() ? "-" : String.join(",", new TreeSet<>(moved));
  }

  private static synchronized void append(String line) {
    try (var w = new FileWriter(RECORD.get(), StandardCharsets.UTF_8, true)) {
      w.write(line + "\n");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The listener as the compiler hears it: a recording that fails must not fail the compile it
   * listens to, which is a pull request's test. The first failure marks the file broken, which
   * its readers take as no record at all, and stops the listening.
   */
  private record Guarded(Recorder r) implements VarkaCompileObserver.Listener {
    @Override
    public void compiled(Object inputs, VarkaEmitOptions options, Object result,
        Function<VarkaEmitOptions, Object> recompile) {
      try {
        r.compiled(inputs, options, result, recompile);
      } catch (VirtualMachineError e) {
        throw e;
      } catch (Throwable t) {
        broke(t);
      }
    }

    @Override
    public void emitted(List<VarkaVectorIR> outputs, int numInputs, int numLiterals,
        VarkaEmitOptions options) {
      try {
        r.emitted(outputs, numInputs, numLiterals, options);
      } catch (VirtualMachineError e) {
        throw e;
      } catch (Throwable t) {
        broke(t);
      }
    }

    private static void broke(Throwable t) {
      VarkaCompileObserver.set(null);
      append("#\tbroken\t" + t.getClass().getName() + ": " + t.getMessage());
    }
  }

  /** The listener: each compile and kernel's moving configurations, memoized for the JVM. */
  static final class Recorder implements VarkaCompileObserver.Listener {
    private static final String NAME = "org.apache.spark.sql.varka.execution.VarkaReachProbe";

    private final List<String> configs;
    private final Map<List<Object>, Set<String>> byCompile = new ConcurrentHashMap<>();
    private final Map<List<Object>, Set<String>> byKernel = new ConcurrentHashMap<>();
    // A kernel's IR to the configurations that moved the compile producing it.
    private final Map<List<VarkaVectorIR>, Set<String>> producedBy = new ConcurrentHashMap<>();
    volatile Set<String> current;
    volatile Set<String> suite;
    volatile boolean paused;
    final Set<String> outside = ConcurrentHashMap.newKeySet();

    Recorder(List<String> configs) {
      this.configs = configs;
    }

    @Override
    public void compiled(Object inputs, VarkaEmitOptions options, Object result,
        Function<VarkaEmitOptions, Object> recompile) {
      if (paused) {
        return;
      }
      Set<String> moved = byCompile.computeIfAbsent(List.of(inputs, options, Optional.of(result)),
          k -> VarkaCompileObserver.unobserved(() -> movedBy(options, result, recompile)));
      for (CompiledVarkaProjection kernel : kernels(result)) {
        producedBy.merge(kernel.outputs(), moved, VarkaTestReach::union);
      }
      note(moved);
    }

    @Override
    public void emitted(List<VarkaVectorIR> outputs, int numInputs, int numLiterals,
        VarkaEmitOptions options) {
      if (paused) {
        return;
      }
      Set<String> moved = byKernel.computeIfAbsent(
          List.of(outputs, numInputs, numLiterals, options),
          k -> VarkaCompileObserver.unobserved(() -> {
            var out = new TreeSet<String>();
            Emission mine = Emission.of(outputs, numInputs, numLiterals, options);
            for (String config : configs) {
              VarkaEmitOptions theirs = VarkaMatrixRuns.parseOver(options, config);
              if (!theirs.equals(options)
                  && !mine.equals(Emission.of(outputs, numInputs, numLiterals, theirs))) {
                out.add(config);
              }
            }
            return Set.copyOf(out);
          }));
      note(moved);
      note(producedBy.getOrDefault(outputs, Set.of()));
    }

    /** Notes what moved for the test running, else the suite setting up, else the JVM. */
    private void note(Set<String> moved) {
      Set<String> c = current;
      Set<String> w = suite;
      (c != null ? c : w != null ? w : outside).addAll(moved);
    }

    private Set<String> movedBy(VarkaEmitOptions options, Object result,
        Function<VarkaEmitOptions, Object> recompile) {
      var out = new TreeSet<String>();
      List<Emission> mine = emissions(result, options);
      for (String config : configs) {
        VarkaEmitOptions theirs = VarkaMatrixRuns.parseOver(options, config);
        if (theirs.equals(options)) {
          continue;
        }
        Object again;
        try {
          again = recompile.apply(theirs);
        } catch (RuntimeException e) {
          out.add(config);
          continue;
        }
        if (!Objects.equals(again, result) || !mine.equals(emissions(again, theirs))) {
          out.add(config);
        }
      }
      return Set.copyOf(out);
    }

    private static List<Emission> emissions(Object result, VarkaEmitOptions options) {
      var out = new ArrayList<Emission>();
      for (CompiledVarkaProjection k : kernels(result)) {
        out.add(Emission.of(k.outputs(), k.inputOrdinals().size(), k.numLiterals(), options));
      }
      return out;
    }

    /** What emitting one kernel gave: its bytes, or the refusal's kind and message. */
    private record Emission(byte[] bytes, String refusal) {
      static Emission of(List<VarkaVectorIR> outputs, int numInputs, int numLiterals,
          VarkaEmitOptions options) {
        try {
          return new Emission(VarkaLoopEmitter.emit(NAME, outputs, numInputs, numLiterals, null,
              null, options), null);
        } catch (RuntimeException e) {
          return new Emission(null, e.getClass().getName() + ": " + e.getMessage());
        }
      }

      @Override
      public boolean equals(Object o) {
        return o instanceof Emission e && Arrays.equals(bytes, e.bytes)
            && Objects.equals(refusal, e.refusal);
      }

      @Override
      public int hashCode() {
        return 31 * Arrays.hashCode(bytes) + Objects.hashCode(refusal);
      }
    }
  }

  /** The kernels a compile's result runs, none for a compile that declined. */
  static List<CompiledVarkaProjection> kernels(Object result) {
    Object r = result instanceof Optional<?> o ? o.orElse(null) : result;
    return switch (r) {
      case PartialVarkaProjection p -> p.kernels();
      case CompiledVarkaPredicate p -> List.of(p.fused());
      case VarkaExpressionCompiler.Explained e -> kernels(e.compiled());
      case null, default -> List.of();
    };
  }

  private static Set<String> union(Set<String> a, Set<String> b) {
    var out = new TreeSet<>(a);
    out.addAll(b);
    return Set.copyOf(out);
  }

  // ---------------------------------------------------------------------------------------------
  // Reading, in a configuration's run
  // ---------------------------------------------------------------------------------------------

  /**
   * A record as a configuration's run reads it: each test's line, each suite's set-up line and
   * the JVM-wide line, the last two joining every test they cover.
   */
  record Record(Map<String, Set<String>> tests, Map<String, Set<String>> suites,
      Set<String> everywhere) {
    /** The configurations that move the test, or null for a test the record has no line for. */
    Set<String> moving(String suite, String test) {
      Set<String> own = tests.get(suite + "\t" + test);
      if (own == null) {
        return null;
      }
      var all = new TreeSet<>(own);
      all.addAll(suites.getOrDefault(suite, Set.of()));
      all.addAll(everywhere);
      return all;
    }
  }

  private static final Optional<Record> REACH = Optional
      .ofNullable(System.getProperty(FILE_PROPERTY))
      .flatMap(f -> read(Path.of(f)));
  private static final AtomicLong SKIPPED = new AtomicLong();
  private static final AtomicLong RAN = new AtomicLong();

  static {
    if (REACH.isPresent() && !VarkaMatrixRuns.CONFIG.isEmpty()) {
      Runtime.getRuntime().addShutdownHook(new Thread(() -> System.out.println(
          "VARKA_REACH_TESTS\t" + VarkaMatrixRuns.CONFIG + "\tskipped " + SKIPPED.get()
              + "\tran " + RAN.get())));
    }
  }

  /**
   * {@code suite<TAB>test<TAB>configs} lines, a test's last line winning, {@code #} ones aside;
   * empty for a record its recording marked broken.
   */
  static Optional<Record> read(Path file) {
    var tests = new HashMap<String, Set<String>>();
    var suites = new HashMap<String, Set<String>>();
    var everywhere = new TreeSet<String>();
    try {
      for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
        if (line.startsWith("#\tbroken")) {
          return Optional.empty();
        }
        int last = line.lastIndexOf('\t');
        if (last <= 0 || line.startsWith("#")) {
          continue;
        }
        String names = line.substring(last + 1);
        Set<String> moved = names.equals("-") ? Set.of() : Set.of(names.split(","));
        String key = line.substring(0, last);
        if (key.equals("*\t*")) {
          everywhere.addAll(moved);
        } else if (key.endsWith("\t*")) {
          suites.merge(key.substring(0, key.length() - 2), moved, VarkaTestReach::union);
        } else {
          tests.put(key, moved);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return Optional.of(new Record(tests, suites, everywhere));
  }

  /**
   * Whether a run of {@code config} skips the test: the recorded file has a line for it that,
   * with its suite's and the JVM's, does not name the configuration or {@code *}, the
   * configuration does not run every test, and the skip list does not name the test under it
   * ({@code listed}).
   */
  static boolean skips(String config, String suite, String test, boolean listed) {
    return skips(REACH, config, suite, test, listed, true);
  }

  /** {@link #skips} with the record and the configuration named: the seam its test uses. */
  static boolean skips(Optional<Record> reach, String config, String suite, String test,
      boolean listed, boolean count) {
    if (reach.isEmpty() || config.isEmpty() || runsEveryTest(config) || listed) {
      return false;
    }
    Set<String> moved = reach.get().moving(suite, test);
    boolean skip = moved != null && !moved.contains(config) && !moved.contains(EVERY);
    if (count) {
      (skip ? SKIPPED : RAN).incrementAndGet();
    }
    return skip;
  }
}
