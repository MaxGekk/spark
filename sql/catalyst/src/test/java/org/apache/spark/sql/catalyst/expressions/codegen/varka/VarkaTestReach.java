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
import java.util.TreeMap;
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
   * the suite in full costs the matrix; they count the shape cache's builds, which the recording's
   * own emissions would add to; they time themselves against a watchdog the recording outlasts;
   * they filter their own cases under a configuration ({@link VarkaReach}); or, in catalyst, their
   * recording costs the job more than the matrix time it saves. A suite left out
   * runs every test under every configuration, as a test the record has no line for does.
   */
  public static final Map<String, String> UNRECORDED = Map.ofEntries(
      Map.entry("VarkaCodegenGiveUpSuite",
          "kernels of thousands of nodes: 16 s plain, 225 s recorded"),
      Map.entry("VarkaSeveralKernelsSuite",
          "plans split into large kernels: 47 s plain, 192 s recorded"),
      Map.entry("VarkaSplitConditionSuite",
          "conditions split into many kernels: 3 s plain, 33 s recorded"),
      Map.entry("VarkaSplitConditionFusionSuite",
          "the same over fused filters: 1 s plain, 29 s recorded"),
      Map.entry("VarkaExpressionCompilerSuite",
          "counts the shape cache's builds; 63 s plain, 778 s recorded"),
      Map.entry("VarkaShapeCacheSuite", "counts the shape cache's builds and hits"),
      Map.entry("VarkaEmitCostSuite", "prices every emission: 17 s plain, 409 s recorded"),
      Map.entry("VarkaEmitCostAuditSuite",
          "audits the emitter's prices under a watchdog the recording outlasts"),
      Map.entry("VarkaSmallTreeSuite", "filters its own trees by their bytes (VarkaReach)"),
      Map.entry("VarkaIrFuzzSuite", "filters its own draws by their options (VarkaReach)"),
      // Catalyst suites whose recording costs a pull request's job more than a tenth of what it
      // saves the matrix, on the laptop on 28 October 2026 (VARKA-507): plain and recorded
      // seconds at the defaults, against the matrix's seconds over the covered configurations.
      Map.entry("VarkaCoverageCompositionFuzzSuite",
          "72 s plain, 247 s recorded, for 1603 s the matrix would skip"),
      Map.entry("VarkaCoverageSuite",
          "3 s plain, 24 s recorded, for 103 s the matrix would skip"),
      Map.entry("VarkaEmittedBytesSuite",
          "65 s plain, 611 s recorded, for 291 s the matrix would skip"),
      Map.entry("VarkaEmitterBudgetSuite",
          "6 s plain, 34 s recorded, for 137 s the matrix would skip"),
      Map.entry("VarkaEmitterDriverTableSuite",
          "5 s plain, 26 s recorded, for 223 s the matrix would skip"),
      Map.entry("VarkaEmitterFramesSuite",
          "1 s plain, 15 s recorded, for 34 s the matrix would skip"),
      Map.entry("VarkaEmitterSplitDriverSuite",
          "3 s plain, 29 s recorded, for 74 s the matrix would skip"),
      Map.entry("VarkaExactGroupingSuite",
          "3 s plain, 18 s recorded, for 63 s the matrix would skip"),
      Map.entry("VarkaFamilySwitchSuite",
          "1 s plain, 11 s recorded, for 34 s the matrix would skip"),
      Map.entry("VarkaGroupingBoundSuite",
          "57 s plain, 818 s recorded, for 1054 s the matrix would skip"),
      Map.entry("VarkaKernelPlanSuite",
          "11 s plain, 428 s recorded, for 127 s the matrix would skip"),
      Map.entry("VarkaMaskOfWordSuite",
          "4 s plain, 59 s recorded, for 190 s the matrix would skip"),
      Map.entry("VarkaUnreadLocalsSuite",
          "19 s plain, 508 s recorded, for 227 s the matrix would skip"),
      Map.entry("VarkaVariantSuite",
          "26 s plain, 135 s recorded, for 1444 s the matrix would skip"),
      Map.entry("VarkaWarmupReachSuite",
          "18 s plain, 237 s recorded, for 625 s the matrix would skip"));

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
      r.keyed.clear();
    }
  }

  /**
   * Runs a piece of a suite's set-up whose work only some tests read, under {@code key}: what it
   * compiles joins the tests that name the key through {@link #reads}, not every test of the
   * suite. A test that reads the piece without naming it would be skipped where the piece moved,
   * so a suite keys only set-up whose readers it names exactly.
   */
  public static void setUp(String key, Runnable body) {
    Recorder r = recorder;
    if (r == null) {
      body.run();
    } else {
      r.during(key, body);
    }
  }

  /** Names the keyed set-up ({@link #setUp}) the running test reads. */
  public static void reads(String... keys) {
    Recorder r = recorder;
    Set<String> reading = r == null ? null : r.reading;
    if (reading != null) {
      reading.addAll(List.of(keys));
    }
  }

  /**
   * Closes the suite's window with a {@code suite<TAB>*} line, which joins each of its tests': what
   * the suite compiled outside its tests, and what this JVM compiled outside every suite so far,
   * which a test of this suite may have read. What the JVM compiles later cannot have changed an
   * answer this suite gave, and another JVM's work is not this one's, so no line joins every test
   * of every module.
   */
  public static void suiteEnd(String suite) {
    Recorder r = recorder;
    if (r != null && r.suite != null) {
      Set<String> moved = r.suite;
      r.suite = null;
      boolean paused = r.paused;
      r.paused = false;
      if (!paused) {
        var all = new TreeSet<String>(moved);
        all.addAll(r.outside);
        append(suite + "\t*\t" + names(all));
        // What each keyed piece of the set-up moved, for a reader of the record; readers skip it.
        new TreeMap<>(r.keyed).forEach((key, keyMoved) ->
            append("#\tsetup\t" + suite + "\t" + key + "\t" + names(keyMoved)));
      }
      r.keyed.clear();
    }
  }

  /** Begins a test's line, where the run records. */
  static void begin() {
    Recorder r = recorder;
    if (r != null) {
      r.current = ConcurrentHashMap.newKeySet();
      r.reading = ConcurrentHashMap.newKeySet();
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
    String names = r.testNames(cancelled);
    r.current = null;
    r.reading = null;
    if (!r.paused) {
      append(suite + "\t" + test + "\t" + names);
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
      // The first frames say where the recording broke, which the message alone may not.
      var frames = new StringBuilder();
      StackTraceElement[] stack = t.getStackTrace();
      for (int i = 0; i < Math.min(6, stack.length); i++) {
        frames.append(" at ").append(stack[i]);
      }
      append("#\tbroken\t" + t.getClass().getName() + ": " + t.getMessage() + frames);
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
    // The keyed set-up running, each piece's moved configurations, and the pieces the running
    // test reads (setUp and reads).
    volatile Set<String> setUp;
    final Map<String, Set<String>> keyed = new ConcurrentHashMap<>();
    volatile Set<String> reading;
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

    /** Runs {@code body} as the keyed set-up piece {@code key}: {@link VarkaTestReach#setUp}. */
    void during(String key, Runnable body) {
      if (paused) {
        body.run();
        return;
      }
      Set<String> outer = setUp;
      setUp = keyed.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet());
      try {
        body.run();
      } finally {
        setUp = outer;
      }
    }

    /**
     * The running test's line's configurations: what it moved itself and what the keyed set-up it
     * reads moved, or {@code *} for a test that cancelled or that reads a piece the recording
     * never heard, which it cannot vouch for.
     */
    String testNames(boolean cancelled) {
      Set<String> moved = current;
      Set<String> read = reading;
      var all = new TreeSet<String>(moved == null ? Set.of() : moved);
      boolean every = cancelled;
      for (String key : read == null ? Set.<String>of() : read) {
        Set<String> keyMoved = keyed.get(key);
        every |= keyMoved == null;
        if (keyMoved != null) {
          all.addAll(keyMoved);
        }
      }
      return every ? EVERY : names(all);
    }

    /**
     * Notes what moved for the keyed set-up running, else the test running, else the suite
     * setting up, else the JVM.
     */
    private void note(Set<String> moved) {
      Set<String> k = setUp;
      Set<String> c = current;
      Set<String> w = suite;
      (k != null ? k : c != null ? c : w != null ? w : outside).addAll(moved);
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
