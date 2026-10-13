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

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Cond;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.util.DateTimeUtils$;

/**
 * Checks a drawn kernel's answers in the code users run, not only in the interpreter (VARKA-274).
 *
 * <p>The fuzzers' check ({@link VarkaKernelChecks}) loads a kernel, runs it once and compares
 * every row with {@link VarkaReference}. A method run once is never compiled, so that verdict is
 * the interpreter's. A user's kernel serves batches as C2 code once a warm-up has compiled it, or
 * moves through the interpreter, C1 and C2 while its batches run when the warm-up is off, and on
 * the way it can be compiled at one tier, thrown away by a deoptimization, recompiled, and entered
 * in the middle of a loop by an on-stack replacement (OSR). Validating JIT compilers, Li et al.
 * (SOSP 2023) found nine in ten of their miscompilations only on such mixed histories.
 *
 * <p>So a drawn case is run over eight <b>histories</b>, each on a class of its own, since a
 * history is exactly what ran on the class before its compared batches:
 *
 * <ul>
 *   <li>{@code cold}: the drawn batch on a fresh class, the fuzzers' check as it was;</li>
 *   <li>{@code warm-same}, {@code warm-other}: warmed until every loop and epilogue method is C2
 *       code, on the drawn values or on another draw of the same columns, then the drawn batch;
 *   </li>
 *   <li>{@code quiet-remainder}, {@code quiet-null}, {@code quiet-guard}: warmed on calls that
 *       never leave a remainder past the last whole vector, never hold an all-null column, or
 *       never come near a guard's bound, then a batch that does - the case C2's code was never
 *       profiled for, which it answers by trapping back to the interpreter;</li>
 *   <li>{@code recompiled}: {@code quiet-null}'s trap, then calls until the code is C2 again, then
 *       the trapping batch again;</li>
 *   <li>{@code released}: C1 left on and no warm-up, as for a kernel run with the warm-up off: a
 *       series of large batches, during which the loops are entered by OSR at C1's tier and then
 *       at C2's.</li>
 * </ul>
 *
 * <p>Every compared batch is compared in full whatever served it. Which tier served it is a
 * second fact, read by {@link VarkaTierWitness} around each call: it says whether the history
 * happened, not whether the answer is right. The warmed histories name their class with the
 * warmed mark, so production's compiler directive keeps C1 off them as it does off a user's warmed
 * kernel ({@link VarkaKernelCompileDirective}); the others keep a name the directive does not
 * match. The warm-up is this check's own, on {@link VarkaKernelWarmup}'s call shape - 32 rows
 * plus the batch's remainder a call, the drivers alternating, from a 1,024-row copy - and it stops
 * when the code list shows every loop and epilogue method at level 4. A kernel that has not got
 * there by the deadline is <b>unsettled</b>: its batches are still compared, and it is counted,
 * not failed.
 */
public final class VarkaCompiledCheck {

  private VarkaCompiledCheck() {
  }

  /** The histories, in the order a case runs them. */
  public enum History {
    COLD("cold", false),
    WARM_SAME("warm-same", true),
    WARM_OTHER("warm-other", true),
    QUIET_REMAINDER("quiet-remainder", true),
    QUIET_NULL("quiet-null", true),
    QUIET_GUARD("quiet-guard", true),
    RECOMPILED("recompiled", true),
    RELEASED("released", false);

    public final String label;
    /** Whether the class is warmed under production's directive, C1 kept off it. */
    public final boolean warmed;

    History(String label, boolean warmed) {
      this.label = label;
      this.warmed = warmed;
    }

    /** The history called {@code label}, as {@code -Dvarka.fuzz.history} names one. */
    public static History named(String label) {
      for (History h : values()) {
        if (h.label.equals(label)) {
          return h;
        }
      }
      throw new IllegalArgumentException("no history named " + label + "; the histories are "
          + Arrays.stream(values()).map(h -> h.label).toList());
    }
  }

  /**
   * How a run is sized: the warm-up's deadline, the {@code released} history's batches and rows,
   * whether the tier witness reads each compared call (off for the A/B that prices it; the
   * recording itself stays on, since the warm-up reads it), and one history alone.
   */
  public record Settings(long settleMillis, int releasedBatches, int releasedRows,
      boolean witnessed, Optional<History> only) {

    /**
     * From {@code -Dvarka.fuzz.*}: {@code settleMillis} (default 10,000), {@code releasedBatches}
     * (200), {@code releasedRows} (10,000), {@code witness} (true) and {@code history} (all).
     */
    public static Settings fromProperties() {
      String only = System.getProperty("varka.fuzz.history");
      return new Settings(Long.getLong("varka.fuzz.settleMillis", 10_000L),
          Integer.getInteger("varka.fuzz.releasedBatches", 200),
          Integer.getInteger("varka.fuzz.releasedRows", 10_000),
          !"false".equals(System.getProperty("varka.fuzz.witness")),
          only == null ? Optional.empty() : Optional.of(History.named(only)));
    }

    public Settings withSettleMillis(long millis) {
      return new Settings(millis, releasedBatches, releasedRows, witnessed, only);
    }

    public Settings withReleased(int batches, int rows) {
      return new Settings(settleMillis, batches, rows, witnessed, only);
    }

    public Settings withOnly(History history) {
      return new Settings(settleMillis, releasedBatches, releasedRows, witnessed,
          Optional.of(history));
    }

    public boolean runs(History h) {
      return only.isEmpty() || only.get() == h;
    }
  }

  /**
   * What one history did on one case: why it was skipped (null when it ran), whether its warm-up
   * settled and how long that took, its wall time, and its compared calls, whose verdicts are read
   * once the witness has settled ({@link #verdicts}).
   */
  public record Run(History history, String skipped, String className, boolean warmed,
      boolean settled, long settleMillis, long millis, List<VarkaTierWitness.Call> calls,
      String unsettled) {
    public Run {
      calls = List.copyOf(calls);
    }

    static Run skipped(History h, String why) {
      return new Run(h, why, null, false, false, 0L, 0L, List.of(), "");
    }

    /** Each compared call's verdict, in order; empty when the witness was off. */
    public List<VarkaTierWitness.Verdict> verdicts() {
      return calls.stream().map(VarkaTierWitness.Call::verdict).toList();
    }

    /**
     * The verdicts of the calls through a body with loop and epilogue methods: a kernel whose
     * outputs read no column has none on its masked side, and a call through it has no tier to
     * show, so the counts leave it out.
     */
    public List<VarkaTierWitness.Verdict> countedVerdicts() {
      return calls.stream().filter(k -> !k.methods().isEmpty())
          .map(VarkaTierWitness.Call::verdict).toList();
    }
  }

  /** A case's runs, one per history run, in history order. */
  public record Result(String label, LaneType lane, List<Run> runs) {
    public Result {
      runs = List.copyOf(runs);
    }

    /** The case's wall time over its compiled histories, every one but {@code cold}. */
    public long compiledMillis() {
      return runs.stream().filter(r -> r.history() != History.COLD).mapToLong(Run::millis).sum();
    }
  }

  private static final AtomicInteger CLASSES = new AtomicInteger();

  /** The warmed histories' class names: production's prefix and warmed mark, then a fuzz tag. */
  static final String WARMED_PREFIX =
      VarkaShapeCacheImpl.CLASS_NAME_PREFIX + VarkaShapeCacheImpl.WARMED_MARK + "fuzz";

  /** The other histories' class names, which the directive does not match. */
  static final String PLAIN_PREFIX = "org.apache.spark.sql.varka.execution.VarkaFusedFuzzTier";

  /**
   * Runs {@code c} over every history {@code s} selects. {@code emit} emits the case's kernel
   * under a class name; {@code replay} is the system properties that rerun the case, which a
   * failure's message carries with the history's own. Throws the first wrong answer, its message
   * starting with the case, the history and the batch.
   */
  public static Result run(VarkaFuzzCase c, Function<String, byte[]> emit, Settings s,
      String replay) {
    var runs = new ArrayList<Run>();
    for (History h : History.values()) {
      if (!s.runs(h)) {
        continue;
      }
      try {
        runs.add(new Check(c, emit, s, h).run());
      } catch (RuntimeException | AssertionError e) {
        throw new IllegalStateException(firstLine(e) + "\n  in history " + h.label
            + "; replay with " + replay + " -Dvarka.fuzz.history=" + h.label, e);
      }
    }
    return new Result(c.label(), c.lane(), runs);
  }

  private static String firstLine(Throwable e) {
    return e.getMessage() == null ? e.getClass().getName() : e.getMessage();
  }

  // ---------------------------------------------------------------------------------------------
  // Columns: a batch's values and null flags, at either lane, before they become buffers.
  // ---------------------------------------------------------------------------------------------

  /** A batch as values widened to 64 bits: {@code data[c][i]}, {@code nulls[c][i]}. */
  record Cols(int length, long[][] data, boolean[][] nulls, boolean forceMasked) {

    /** {@code rows} rows taken from this batch's in turn, its null flags with them. */
    Cols cycled(int rows) {
      int n = data.length;
      var d = new long[n][rows];
      var z = new boolean[n][rows];
      for (int c = 0; c < n; c++) {
        for (int i = 0; i < rows; i++) {
          d[c][i] = data[c][i % length];
          z[c][i] = nulls[c][i % length];
        }
      }
      return new Cols(rows, d, z, forceMasked && rows > 1);
    }

    Cols withNulls(boolean[][] z, boolean forced) {
      return new Cols(length, data, z, forced);
    }

    /** Whether the kernel's masked body takes this batch: a null, or a forced null count. */
    boolean masked() {
      if (data.length == 0) {
        return false;
      }
      if (forceMasked) {
        return true;
      }
      for (boolean[] column : nulls) {
        for (boolean z : column) {
          if (z) {
            return true;
          }
        }
      }
      return false;
    }

    VarkaKernelChecks.Batch ints() {
      var d = new int[data.length][];
      for (int c = 0; c < data.length; c++) {
        d[c] = new int[length];
        for (int i = 0; i < length; i++) {
          d[c][i] = (int) data[c][i];
        }
      }
      return new VarkaKernelChecks.Batch(length, nulls, d, forceMasked);
    }

    VarkaKernelChecks.LongBatch longs() {
      return new VarkaKernelChecks.LongBatch(length, nulls, data, forceMasked);
    }
  }

  /** Every row null in no column. */
  private static boolean[][] noNulls(int columns, int rows) {
    return new boolean[columns][rows];
  }

  /**
   * Partial nulls in every column of every slice of at least five rows: row {@code i} of column
   * {@code c} is null where {@code i % 5 == c % 5}, so no slice is all-null (which would take the
   * masked driver's all-null shortcut and run no loop) and none is null-free.
   */
  private static boolean[][] everyFifth(int columns, int rows) {
    var z = new boolean[columns][rows];
    for (int c = 0; c < columns; c++) {
      for (int i = 0; i < rows; i++) {
        z[c][i] = i % 5 == c % 5;
      }
    }
    return z;
  }

  // ---------------------------------------------------------------------------------------------
  // One history on one case.
  // ---------------------------------------------------------------------------------------------

  /** Rows of the warm-up's copy, as {@link VarkaKernelWarmup#SNAPSHOT_ROWS}. */
  private static final int SNAPSHOT_ROWS = 1024;

  /** Rows a warm-up call runs before the batch's remainder, as the production warm-up's. */
  private static final int SLICE_ROWS = 32;

  /** Calls between two reads of the code list while warming. */
  private static final int CALLS_PER_LOOK = 256;

  /** The quiet-remainder batch's rows: odd, so it leaves a remainder at every lane count. */
  private static final int REMAINDER_ROWS = 1007;

  /** The guard edge batch's rows: a full vector lane at row 0 and the epilogue's at row 16. */
  private static final int EDGE_ROWS = 17;

  private static final class Check {
    private final VarkaFuzzCase c;
    private final Function<String, byte[]> emit;
    private final Settings s;
    private final History h;
    private final boolean isLong;
    private final int numInputs;
    private final Cols drawn;
    private final List<VarkaTierWitness.Call> calls = new ArrayList<>();
    private final VarkaTierWitness witness;
    private String className;
    private VarkaFusedKernel kernel;
    private boolean settled = true;
    private long settleMillis;
    private boolean everWarmed;
    private String unsettledLevels = "";

    Check(VarkaFuzzCase c, Function<String, byte[]> emit, Settings s, History h) {
      this.c = c;
      this.emit = emit;
      this.s = s;
      this.h = h;
      this.isLong = c.lane() == LaneType.LONG;
      this.numInputs = c.numInputs();
      this.drawn = new Cols(c.length(), c.data(), c.nulls(), c.forceMasked());
      this.witness = s.witnessed() ? VarkaTierWitness.get() : null;
    }

    Run run() {
      if (h.warmed && !VarkaKernelCompileDirective.readyForWarmup()) {
        return Run.skipped(h, "this JVM cannot warm a kernel to C2");
      }
      Optional<Edges> edges = Optional.empty();
      if (h == History.QUIET_GUARD) {
        edges = guardEdges();
        if (edges.isEmpty()) {
          return Run.skipped(h, "no guard whose bound a column reaches");
        }
      }
      if ((h == History.QUIET_NULL || h == History.RECOMPILED) && numInputs == 0) {
        return Run.skipped(h, "no input to null");
      }
      long start = System.nanoTime();
      className = (h.warmed ? WARMED_PREFIX : PLAIN_PREFIX) + CLASSES.incrementAndGet();
      byte[] bytes = emit.apply(className);
      VarkaTierWitness.get().watch(className);
      var loader = new VarkaGeneratedClassLoader(VarkaCompiledCheck.class.getClassLoader());
      kernel = VarkaKernelChecks.load(loader, className, bytes);
      try {
        switch (h) {
          case COLD -> compareBoth("the drawn batch", drawn);
          case WARM_SAME -> {
            warm(drawn.cycled(SNAPSHOT_ROWS), ownNulls(), sliceRows(), allMethods());
            compareBoth("the drawn batch", drawn);
          }
          case WARM_OTHER -> {
            var other = otherDraw();
            warm(other, other.nulls(), sliceRows(), allMethods());
            compareBoth("the drawn batch", drawn);
          }
          case QUIET_REMAINDER -> {
            warm(drawn.cycled(SNAPSHOT_ROWS), everyFifth(numInputs, SNAPSHOT_ROWS), SLICE_ROWS,
                allMethods());
            // A remainder-free batch like the warm-up's calls first, then the one with a
            // remainder: a trap is then seen to fall in its own call's window.
            compare("a batch of " + SLICE_ROWS + " rows", drawn.cycled(SLICE_ROWS), false);
            compareBoth("a batch of " + REMAINDER_ROWS + " rows", drawn.cycled(REMAINDER_ROWS));
          }
          case QUIET_NULL -> {
            warm(drawn.cycled(SNAPSHOT_ROWS), everyFifth(numInputs, SNAPSHOT_ROWS), sliceRows(),
                allMethods());
            // First a batch shaped like the warm-up's calls, which should not trap, then the
            // one that should: a trap is then seen to fall in its own call's window.
            Cols quiet = drawn.cycled(sliceRows());
            compare("a batch like the warm-up's", quiet.withNulls(
                everyFifth(numInputs, quiet.length()), true), false);
            compareBoth("the drawn batch with column 0 all-null", firstAllNull());
          }
          case QUIET_GUARD -> {
            warm(drawn.cycled(SNAPSHOT_ROWS), everyFifth(numInputs, SNAPSHOT_ROWS), sliceRows(),
                allMethods());
            edges.get().run(this);
          }
          case RECOMPILED -> recompiled();
          case RELEASED -> released();
        }
      } finally {
        loader.release();
      }
      long millis = (System.nanoTime() - start) / 1_000_000L;
      return new Run(h, null, className, everWarmed, settled, settleMillis, millis, calls,
          unsettledLevels);
    }

    /** Rows per warm-up call: 32 and the drawn batch's remainder past them. */
    private int sliceRows() {
      return SLICE_ROWS + c.length() % SLICE_ROWS;
    }

    /** The warm-up's masked calls on the drawn null flags, where they are partial in a slice. */
    private boolean[][] ownNulls() {
      boolean[][] own = drawn.cycled(SNAPSHOT_ROWS).nulls();
      boolean[][] fallback = everyFifth(numInputs, SNAPSHOT_ROWS);
      int rows = sliceRows();
      for (int col = 0; col < numInputs; col++) {
        for (int from = 0; from + rows <= SNAPSHOT_ROWS; from += rows) {
          boolean all = true;
          for (int i = from; i < from + rows; i++) {
            all &= own[col][i];
          }
          if (all) {
            System.arraycopy(fallback[col], from, own[col], from, rows);
          }
        }
      }
      return own;
    }

    /**
     * Another draw of the case's columns, each within the bound the fuzzer draws it in, with one
     * row in three null: what {@code warm-other} warms on.
     */
    private Cols otherDraw() {
      var rnd = new Random(c.label().hashCode() * 31L + 0x57415243L);
      var d = new long[numInputs][SNAPSHOT_ROWS];
      var z = new boolean[numInputs][SNAPSHOT_ROWS];
      for (int col = 0; col < numInputs; col++) {
        for (int i = 0; i < SNAPSHOT_ROWS; i++) {
          d[col][i] = drawValue(rnd, col);
          z[col][i] = i % 5 != 4 && rnd.nextInt(3) == 0;
        }
      }
      return new Cols(SNAPSHOT_ROWS, d, z, false);
    }

    /** One value of column {@code col}, drawn as the fuzzer draws that column. */
    private long drawValue(Random rnd, int col) {
      if (isLong) {
        return VarkaIrGrammar.within(rnd, VarkaIrGrammar.LONG_COLUMN_BOUND);
      }
      if (col == c.levelOrdinal()) {
        // Exactly the codes a truncation level takes; see VarkaIrFuzzSuite's draw.
        int week = DateTimeUtils$.MODULE$.TRUNC_TO_WEEK();
        return week + rnd.nextInt(DateTimeUtils$.MODULE$.TRUNC_TO_YEAR() - week + 1);
      }
      return VarkaIrGrammar.within(rnd, columnBound(col));
    }

    /** The bound column {@code col}'s values are drawn within, plus or minus. */
    private long columnBound(int col) {
      if (isLong) {
        return VarkaIrGrammar.LONG_COLUMN_BOUND;
      }
      return col == c.smallOrdinal() ? VarkaChrono.MONTH_ARITH_MAX_MONTHS
          : VarkaIrGrammar.COLUMN_BOUND;
    }

    /** The drawn batch with every row of column 0 null. */
    private Cols firstAllNull() {
      var z = new boolean[numInputs][];
      for (int col = 0; col < numInputs; col++) {
        z[col] = col == 0 ? filled(c.length()) : c.nulls()[col].clone();
      }
      return drawn.withNulls(z, false);
    }

    private static boolean[] filled(int rows) {
      var out = new boolean[rows];
      Arrays.fill(out, true);
      return out;
    }

    // -------------------------------------------------------------------------------------------
    // The kernel's methods.
    // -------------------------------------------------------------------------------------------

    /** The loop and epilogue methods of one side, read off the loaded class. */
    private TreeSet<String> groupMethods(boolean dense) {
      var out = new TreeSet<String>();
      for (Method m : kernel.getClass().getDeclaredMethods()) {
        String name = m.getName();
        if (VarkaMethodNames.isLoop(name, dense) || VarkaMethodNames.isEpilogue(name, dense)) {
          out.add(name);
        }
      }
      return out;
    }

    /** Both sides' loop and epilogue methods, or the dense side's for a kernel with no input. */
    private TreeSet<String> allMethods() {
      var out = groupMethods(true);
      if (numInputs > 0) {
        out.addAll(groupMethods(false));
      }
      return out;
    }

    // -------------------------------------------------------------------------------------------
    // Compared calls.
    // -------------------------------------------------------------------------------------------

    /**
     * One batch through the body it selects, compared row by row: the status, 0 once compared.
     * The witness reads the code list and opens its window around the kernel's call alone.
     */
    private int compare(String what, Cols b, boolean declineAllowed) {
      String context = c.label() + " history=" + h.label + " batch=" + what;
      VarkaKernelChecks.CallHook hook = VarkaKernelChecks.CallHook.NONE;
      if (witness != null) {
        var call = witness.call(className, groupMethods(!b.masked() || numInputs == 0));
        calls.add(call);
        hook = call;
      }
      int[] intLits = new int[c.lits().length];
      for (int k = 0; k < intLits.length; k++) {
        intLits[k] = (int) c.lits()[k];
      }
      return isLong
          ? VarkaKernelChecks.runLoadedLong(context, kernel, c.roots(), numInputs, c.lits(),
              b.longs(), declineAllowed, hook)
          : VarkaKernelChecks.runLoaded(context, kernel, c.roots(), numInputs, intLits, b.ints(),
              declineAllowed, hook);
    }

    /** {@link #compare} through the batch's own body, then through the other one. */
    private List<Integer> compareBoth(String what, Cols b, boolean declineAllowed) {
      var statuses = new ArrayList<Integer>();
      statuses.add(compare(what, b, declineAllowed));
      VarkaKernelChecks.otherBody(numInputs, b.length(), b.nulls(), b.forceMasked())
          .ifPresent(o -> statuses.add(compare(what + " (the other body)",
              b.withNulls(o.nulls(), o.forceMasked()), declineAllowed)));
      return statuses;
    }

    private void compareBoth(String what, Cols b) {
      compareBoth(what, b, false);
    }

    // -------------------------------------------------------------------------------------------
    // The warm-up.
    // -------------------------------------------------------------------------------------------

    /**
     * Calls the kernel on slices of {@code snapshot} until the code list shows every one of
     * {@code required} as C2 code in use, or the deadline passes. Each slice is called once
     * through the dense driver, with no nulls, and once through the masked one, with
     * {@code maskedNulls}' flags; null lanes hold the harness's poison. The buffers are built once,
     * so a call allocates nothing. Nothing is compared: what the warm-up computes is thrown away.
     */
    private void warm(Cols snapshot, boolean[][] maskedNulls, int rows, TreeSet<String> required) {
      everWarmed = true;
      VarkaTierWitness.get().touched(className);
      long start = System.nanoTime();
      long deadline = start + s.settleMillis() * 1_000_000L;
      try (var arena = Arena.ofConfined()) {
        var sets = new ArrayList<Args>();
        for (int from = 0; from + rows <= snapshot.length(); from += rows) {
          sets.add(args(arena, snapshot, noNulls(numInputs, snapshot.length()), from, rows,
              false));
          if (numInputs > 0) {
            sets.add(args(arena, snapshot, maskedNulls, from, rows, true));
          }
        }
        boolean done = false;
        for (long k = 0; !done; k++) {
          sets.get((int) (k % sets.size())).call(kernel, isLong);
          if (k % CALLS_PER_LOOK == CALLS_PER_LOOK - 1) {
            done = allC2(required) || System.nanoTime() > deadline;
          }
        }
      }
      boolean ok = allC2(required);
      if (!ok && unsettledLevels.isEmpty()) {
        unsettledLevels = VarkaTierWitness.get().levelsNow(className, required).toString();
      }
      settled &= ok;
      settleMillis += (System.nanoTime() - start) / 1_000_000L;
    }

    /**
     * Whether the warm-up is done, read from the code list and from what the recording has said
     * about on-stack replacements and inlining - which the warm-up needs whether or not the
     * compared calls are witnessed.
     */
    private boolean allC2(TreeSet<String> required) {
      return VarkaTierWitness.get().allC2Now(className, required);
    }

    /**
     * One warm-up call's arguments over rows {@code from} to {@code from + rows} of
     * {@code snapshot}, with {@code nulls}' flags. For a {@code masked} call a column with no null
     * in the slice gets a forced null count over a full bitmap, as the harness forces the masked
     * body, so the masked driver runs whatever the flags.
     */
    private Args args(Arena arena, Cols snapshot, boolean[][] nulls, int from, int rows,
        boolean masked) {
      long stride = isLong ? 8L : 4L;
      long words = ((rows + 63) / 64) * 8L;
      var srcData = new long[numInputs];
      var srcValidity = new long[numInputs];
      var nullCounts = new int[numInputs];
      for (int col = 0; col < numInputs; col++) {
        var d = VarkaEmitterFixtures.alloc(arena, rows * stride);
        var v = VarkaEmitterFixtures.alloc(arena, words);
        v.fill((byte) 0);
        int count = 0;
        for (int i = 0; i < rows; i++) {
          boolean z = nulls[col][from + i];
          long value = z ? poison(count) : snapshot.data()[col][from + i];
          if (isLong) {
            d.set(ValueLayout.JAVA_LONG, i * 8L, value);
          } else {
            d.set(ValueLayout.JAVA_INT, i * 4L, (int) value);
          }
          if (z) {
            count++;
          } else {
            long off = i / 8L;
            v.set(ValueLayout.JAVA_BYTE, off,
                (byte) (v.get(ValueLayout.JAVA_BYTE, off) | (1 << (i % 8))));
          }
        }
        srcData[col] = d.address();
        nullCounts[col] = count;
        srcValidity[col] = count == 0 || count == rows ? 0L : v.address();
      }
      if (masked) {
        for (int col = 0; col < numInputs; col++) {
          if (nullCounts[col] == 0) {
            var v = VarkaEmitterFixtures.alloc(arena, words);
            v.fill((byte) 0xFF);
            nullCounts[col] = 1;
            srcValidity[col] = v.address();
          }
        }
      }
      int n = c.roots().size();
      var outData = new long[n];
      var outValidity = new long[n];
      for (int o = 0; o < n; o++) {
        outData[o] = c.roots().get(o) instanceof Cond ? 0L
            : VarkaEmitterFixtures.alloc(arena, rows * stride).address();
        outValidity[o] = VarkaEmitterFixtures.alloc(arena, words).address();
      }
      int[] intLits = new int[c.lits().length];
      for (int k = 0; k < intLits.length; k++) {
        intLits[k] = (int) c.lits()[k];
      }
      return new Args(srcData, srcValidity, nullCounts, outData, outValidity, intLits, c.lits(),
          rows, VarkaEmitterTestSupport.scratch(kernel, rows));
    }

    /** The harness's poison for the {@code k}-th null of a column, at the case's lane. */
    private long poison(int k) {
      return isLong ? ((k & 1) == 0 ? Long.MIN_VALUE : Long.MAX_VALUE)
          : VarkaEmitterFixtures.poison(k);
    }

    // -------------------------------------------------------------------------------------------
    // recompiled and released.
    // -------------------------------------------------------------------------------------------

    /**
     * {@code quiet-null}'s warm-up and all-null batch; then, if that threw code away, calls that
     * mix all-null slices into the warm-up until every method is C2 code again, and the all-null
     * batch once more. A kernel the all-null batch did not trap has nothing to recover from, and
     * its run shows only the first batch's call.
     */
    private void recompiled() {
      var required = allMethods();
      var snapshot = drawn.cycled(SNAPSHOT_ROWS);
      warm(snapshot, everyFifth(numInputs, SNAPSHOT_ROWS), sliceRows(), required);
      Cols trapping = firstAllNull();
      compareBoth("the drawn batch with column 0 all-null", trapping);
      if (!allC2(required)) {
        var mixed = everyFifth(numInputs, SNAPSHOT_ROWS);
        int rows = sliceRows();
        for (int from = 0; from + rows <= SNAPSHOT_ROWS; from += 2 * rows) {
          Arrays.fill(mixed[0], from, from + rows, true);
        }
        warm(snapshot, mixed, rows, required);
        compareBoth("the drawn batch with column 0 all-null, after the recompile", trapping);
      }
    }

    /**
     * No warm-up and C1 on: {@code releasedBatches} batches of {@code releasedRows} rows, the
     * drawn rows in turn, each compared through its own body. What the JIT does on the way is the
     * history: the interpreter, an OSR compile at C1's profiling tier, C1's code, an OSR compile
     * at C2's, C2's code.
     */
    private void released() {
      Cols batch = drawn.cycled(s.releasedRows());
      for (int k = 0; k < s.releasedBatches(); k++) {
        compare("released batch " + k, batch, false);
      }
    }

    // -------------------------------------------------------------------------------------------
    // quiet-guard.
    // -------------------------------------------------------------------------------------------

    /**
     * A guard's edge reached through one column: on the drawn first row, column {@code column}
     * at {@code answer} must be answered and at {@code decline} must be declined (or, by an
     * unqualified guard under an untaken arm, may be).
     */
    private Optional<Edges> guardEdges() {
      var guards = new VarkaGuardSpecs.Guards(c.roots(), numInputs, c.lits().length, c.options(),
          isLong);
      if (guards.guards().isEmpty() || c.length() == 0) {
        return Optional.empty();
      }
      long[] template = new long[numInputs];
      for (int col = 0; col < numInputs; col++) {
        template[col] = c.data()[col][0];
      }
      if (!verdict(guards, template).answers()) {
        return Optional.empty();
      }
      for (int col = 0; col < numInputs; col++) {
        if (!isLong && col == c.levelOrdinal()) {
          continue;
        }
        long bound = columnBound(col);
        for (long dir : new long[] {1L, -1L}) {
          long inside = dir * bound;
          long[] row = template.clone();
          row[col] = inside;
          long answer;
          long decline;
          if (!verdict(guards, row).answers()) {
            // The guard's bound is inside the drawn range: bisect for the last answered value.
            long lo = template[col];
            long hi = inside;
            while (Math.abs(hi - lo) > 1) {
              long mid = lo + (hi - lo) / 2;
              row[col] = mid;
              if (verdict(guards, row).answers()) {
                lo = mid;
              } else {
                hi = mid;
              }
            }
            answer = lo;
            decline = hi;
          } else {
            // At the drawn range's end: one past it, the decline side alone leaving the range.
            row[col] = inside + dir;
            if (verdict(guards, row).answers()) {
              continue;
            }
            answer = inside;
            decline = inside + dir;
          }
          row[col] = decline;
          return Optional.of(new Edges(guards, template, col, answer, decline,
              verdict(guards, row)));
        }
      }
      return Optional.empty();
    }

    private VarkaGuardSpecs.Verdict verdict(VarkaGuardSpecs.Guards guards, long[] row) {
      return VarkaGuardSpecs.verdict(guards, c.lits(), new long[][] {row},
          new boolean[][] {new boolean[numInputs]}, List.of());
    }

    /** A batch of {@link #EDGE_ROWS} rows of {@code template}, {@code column} at {@code value}
     * on the first and last rows. */
    private Cols edgeBatch(long[] template, int column, long value) {
      var d = new long[numInputs][EDGE_ROWS];
      for (int col = 0; col < numInputs; col++) {
        Arrays.fill(d[col], template[col]);
      }
      d[column][0] = value;
      d[column][EDGE_ROWS - 1] = value;
      return new Cols(EDGE_ROWS, d, noNulls(numInputs, EDGE_ROWS), false);
    }

    /** What quiet-guard compares after its warm-up: the answer side, then the decline side. */
    private record Edges(VarkaGuardSpecs.Guards guards, long[] template, int column, long answer,
        long decline, VarkaGuardSpecs.Verdict declineVerdict) {

      void run(Check k) {
        String at = "column " + column + " at ";
        k.compareBoth(at + answer + ", inside its guard", k.edgeBatch(template, column, answer));
        String what = at + decline + ", past its guard";
        for (int status : k.compareBoth(what, k.edgeBatch(template, column, decline), true)) {
          boolean exact = k.c.options().exactRefusalStatus();
          if (declineVerdict.must() != 0 && status == 0
              || exact && !declineVerdict.admits(status)) {
            throw new IllegalStateException(k.c.label() + " history=" + k.h.label + " batch="
                + what + ": status " + status + " where the guards' verdict is "
                + declineVerdict);
          }
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The counts a run reports.
  // ---------------------------------------------------------------------------------------------

  /**
   * What a run of many cases came to, per lane and history, read once the witness has settled:
   * the runs and skips, the warm-ups that settled and their times, how many cases' compared calls
   * were all C2 code, trapped, or were entered by an on-stack replacement, the unsettled cases,
   * and each case's time over its compiled histories.
   */
  public static final class Tally {
    private final List<Result> results = new ArrayList<>();
    private int coldOnly;

    public void add(Result r) {
      results.add(r);
    }

    /** A case run {@code cold} only: its options put a second vector species in the JVM. */
    public void addColdOnly() {
      coldOnly++;
    }

    public List<Result> results() {
      return List.copyOf(results);
    }

    /** The runs of one lane and history, skipped ones included. */
    public List<Run> runs(LaneType lane, History h) {
      var out = new ArrayList<Run>();
      for (Result r : results) {
        if (r.lane() == lane) {
          for (Run run : r.runs()) {
            if (run.history() == h) {
              out.add(run);
            }
          }
        }
      }
      return out;
    }

    /** Whether every counted call of {@code run} has a verdict labelled {@code label}. */
    public static boolean all(Run run, String label) {
      var verdicts = run.countedVerdicts();
      return !verdicts.isEmpty() && verdicts.stream().allMatch(v -> v.label().equals(label));
    }

    /** Whether some counted call of {@code run} has a verdict labelled {@code label}. */
    public static boolean any(Run run, String label) {
      return run.countedVerdicts().stream().anyMatch(v -> v.label().equals(label));
    }

    /** The first compared call of {@code run} with a verdict labelled {@code label}, or -1. */
    public static int first(Run run, String label) {
      var verdicts = run.verdicts();
      for (int k = 0; k < verdicts.size(); k++) {
        if (verdicts.get(k).label().equals(label)) {
          return k;
        }
      }
      return -1;
    }

    private static String percentiles(List<Long> values) {
      if (values.isEmpty()) {
        return "-";
      }
      var sorted = new ArrayList<>(values);
      sorted.sort(null);
      return "median " + sorted.get(sorted.size() / 2) + ", p90 "
          + sorted.get(Math.min(sorted.size() - 1, (int) Math.ceil(sorted.size() * 0.9) - 1))
          + ", max " + sorted.get(sorted.size() - 1);
    }

    /** The settle times of one lane's settled warm-ups, every warmed history's, in ms. */
    public List<Long> settleTimes(LaneType lane) {
      var out = new ArrayList<Long>();
      for (History h : History.values()) {
        for (Run run : runs(lane, h)) {
          if (run.skipped() == null && run.warmed() && run.settled()) {
            out.add(run.settleMillis());
          }
        }
      }
      return out;
    }

    /** The report, a line per lane and history and a few after them. */
    public List<String> report(boolean witnessed) {
      var lines = new ArrayList<String>();
      for (LaneType lane : List.of(LaneType.INT, LaneType.LONG)) {
        String name = lane.toString().toLowerCase(java.util.Locale.ROOT);
        var compiled = new ArrayList<Long>();
        for (Result r : results) {
          if (r.lane() == lane && r.runs().size() > 1) {
            compiled.add(r.compiledMillis());
          }
        }
        if (compiled.isEmpty() && runs(lane, History.COLD).isEmpty()) {
          continue;
        }
        lines.add(name + " lane: " + compiled.size() + " cases over the compiled histories, "
            + "ms a case " + percentiles(compiled) + "; warm-ups settled in ms "
            + percentiles(settleTimes(lane)));
        for (History h : History.values()) {
          var runs = runs(lane, h);
          if (runs.isEmpty()) {
            continue;
          }
          var ran = runs.stream().filter(r -> r.skipped() == null).toList();
          var line = new StringBuilder("  " + h.label + ": " + ran.size() + " run");
          if (ran.size() < runs.size()) {
            var why = new java.util.TreeMap<String, Integer>();
            runs.stream().filter(r -> r.skipped() != null)
                .forEach(r -> why.merge(r.skipped(), 1, Integer::sum));
            line.append(", skipped ").append(why);
          }
          if (h.warmed) {
            long settled = ran.stream().filter(Run::settled).count();
            line.append(", settled ").append(settled).append('/').append(ran.size());
          }
          line.append(", ms ").append(percentiles(ran.stream().map(Run::millis).toList()));
          if (witnessed) {
            var counts = new java.util.TreeMap<String, Integer>();
            for (Run run : ran) {
              String kind = all(run, "C2") ? "all C2" : any(run, "trapped") ? "trapped"
                  : any(run, "osr4") ? "osr4" : any(run, "osr3") ? "osr3"
                  : all(run, "mixed") ? "all mixed" : "some mixed";
              counts.merge(kind, 1, Integer::sum);
            }
            line.append("; cases ").append(counts);
            if (h == History.RECOMPILED) {
              line.append("; trapped and then C2 on the last batch ").append(ran.stream()
                  .filter(r -> any(r, "trapped") && !r.countedVerdicts().isEmpty()
                      && r.countedVerdicts().getLast().label().equals("C2"))
                  .count());
            }
            if (h == History.RELEASED) {
              line.append("; first osr3 at batch ").append(percentiles(firsts(ran, "osr3")))
                  .append(", first osr4 at batch ").append(percentiles(firsts(ran, "osr4")))
                  .append(", cases with osr4 ")
                  .append(ran.stream().filter(r -> any(r, "osr4")).count());

            }
          }
          lines.add(line.toString());
        }
      }
      if (coldOnly > 0) {
        lines.add(coldOnly + " case(s) with a lanes override, run cold only after the others");
      }
      for (Result r : results) {
        for (Run run : r.runs()) {
          if (run.skipped() == null && run.warmed() && !run.settled()) {
            lines.add("unsettled: " + run.history().label + " " + r.label()
                + (witnessed ? " traps " + VarkaTierWitness.get().trapReasons(run.className())
                    + " levels " + run.unsettled() : ""));
          }
        }
      }
      return lines;
    }

    private static List<Long> firsts(List<Run> runs, String label) {
      var out = new ArrayList<Long>();
      for (Run run : runs) {
        int k = first(run, label);
        if (k >= 0) {
          out.add((long) k);
        }
      }
      return out;
    }
  }

  /**
   * The histories a run shows did not happen, each as a failure message: a {@code cold} call that
   * was not interpreted at both edges, a lane whose settled {@code warm-same} or {@code warm-other}
   * runs never once showed every call as C2 code, and a lane whose {@code released} runs never
   * showed an on-stack replacement. A history not reached is a fact about the JIT on this machine
   * and its load, never a wrong answer, and the message carries the code list to read it by.
   */
  public static List<String> notReached(Tally tally) {
    var out = new ArrayList<String>();
    for (LaneType lane : List.of(LaneType.INT, LaneType.LONG)) {
      for (Run run : tally.runs(lane, History.COLD)) {
        for (var v : run.verdicts()) {
          if (!(v instanceof VarkaTierWitness.Mixed m && m.interpreted())) {
            out.add("history not reached: cold, a call on " + run.className()
                + " was not interpreted: " + v);
          }
        }
      }
      for (History h : List.of(History.WARM_SAME, History.WARM_OTHER)) {
        var settled = tally.runs(lane, h).stream()
            .filter(r -> r.skipped() == null && r.settled()).toList();
        if (!settled.isEmpty() && settled.stream().noneMatch(r -> Tally.all(r, "C2"))) {
          var sample = settled.get(0);
          out.add("history not reached: " + h.label + ", no settled case of the " + lane
              + " lane ran every compared call as C2 code; first case's verdicts "
              + sample.verdicts() + ", its code list before the first call "
              + sample.calls().get(0).codeBefore());
        }
      }
      var released = tally.runs(lane, History.RELEASED).stream()
          .filter(r -> r.skipped() == null).toList();
      if (!released.isEmpty()
          && released.stream().noneMatch(r -> Tally.any(r, "osr3") || Tally.any(r, "osr4"))) {
        out.add("history not reached: released, no case of the " + lane + " lane was entered by "
            + "an on-stack replacement in " + released.get(0).calls().size() + " batches");
      }
    }
    return out;
  }

  /**
   * One warm-up call's arguments, built once: the kernel's {@code run} takes them as they are,
   * the int lane's literals in {@code intLits}, the long lane's in {@code longLits}.
   */
  private record Args(long[] srcData, long[] srcValidity, int[] nullCounts, long[] outData,
      long[] outValidity, int[] intLits, long[] longLits, int rows, long scratch) {

    void call(VarkaFusedKernel kernel, boolean isLong) {
      if (isLong) {
        kernel.run(srcData, srcValidity, nullCounts, outData, outValidity, new int[0], longLits,
            rows, scratch);
      } else {
        kernel.run(srcData, srcValidity, nullCounts, outData, outValidity, intLits, rows, scratch);
      }
    }
  }
}
