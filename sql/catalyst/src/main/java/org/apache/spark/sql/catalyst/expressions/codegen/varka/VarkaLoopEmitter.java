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

// Only four Class-File API imports appear here: importing several others (CustomAttribute,
// AttributedElement, ClassElement...) makes scalac - and so every scaladoc pass over the module -
// fail with an "illegal cyclic reference" while completing the API's sealed hierarchy. Task-13
// additions use fully-qualified names inside method bodies instead, which scalac's Java parser
// never reads; see VarkaDebugInfo's class doc.
import static org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaDescriptors.*;
import static org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitBudget.*;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.Analysis.BitmapPass;

import java.lang.classfile.ClassBuilder;
import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.classfile.Label;
import java.lang.constant.ClassDesc;
import java.lang.constant.ConstantDescs;
import java.lang.constant.MethodTypeDesc;
import java.lang.reflect.AccessFlag;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.AddDays;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.AddMonths;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.And;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Chrono;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Compare;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Cond;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DateDiff;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfMonth;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfWeek;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfWeekIso;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfYear;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Greatest;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.GuardedDay;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.GuardedRange;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IfElse;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntArith;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.BoundedDivide;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ConstDivide;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntNeg;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IsNotNull;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LastDay;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Least;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.MakeDate;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Month;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.NarrowLane;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.NextDay;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Not;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Or;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Quarter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.SubDays;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ThursdayOf;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.TruncDate;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.TruncDateDynamic;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.TruncLevel;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.WeekDay;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.WeekOfYear;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Year;

/**
 * Emits a fused vector loop for a {@link VarkaVectorIR} DAG using the Class-File API: a class
 * implementing {@link VarkaFusedKernel} whose {@code run} <i>is</i> the loop - loads, the op
 * DAG on the operand stack, one store per output. It generalizes the six-step shape of the
 * hand-written {@code DateVectorOps} kernels, which remain the reference semantics for the
 * arithmetic; this class exists so that a whole projection, predication included, runs in one
 * pass with its intermediates in vector registers rather than in memory.
 *
 * <p>This is the largest file in the engine. The sections below are its design in the order
 * the emitted code executes.
 *
 * <p><b>Method layout.</b> {@code run} dispatches per batch on one loop-invariant test - are
 * all referenced inputs null-free? - to a dense or masked <i>driver</i>, which zeroes the
 * output validity, takes the all-null shortcut, then calls one sibling <i>loop</i> method per
 * output group (at most {@code GROUP_BUDGET} ops each; see that constant for the measured
 * reason) and finally the sibling <i>epilogue</i> method. The dense side runs with no validity
 * bookkeeping at all, which is sound because every node maps valid inputs to valid outputs -
 * there is no null-literal node - so null-free in means all-valid out. Separate methods rather
 * than one large one: each gets its own C2 compilation, so no method's node and inlining
 * budgets can starve another's intrinsics.
 *
 * <p><b>Unmasked compute.</b> Both bodies run unmasked loads, lanewise ops and stores. Inside
 * {@code loopBound} every access is in bounds, an all-null column still has an allocated data
 * buffer, and the engine contract declares invalid destination lanes undefined - so masks
 * carry no correctness inside the loop, and masked ops measure 2.3x-2.9x slower even with an
 * all-true mask (see {@code PLAN_TASK_10.md}). Truth lives in the <i>validity words</i>
 * instead: per lane group each referenced input contributes one long ({@code 0L} all-null,
 * {@code -1L} null-free, {@code validityBitsAt} otherwise), and each node's validity is
 * computed from its children's words by the mask algebra - AND for the null-intolerant ops, OR
 * for {@code greatest}/{@code least}, a word blend for {@code IfElse}. A {@code VectorMask} is
 * materialized only where a blend semantically needs one.
 *
 * <p><b>Conditions.</b> A {@link Cond} node evaluates to a known-true and a known-false word
 * pair - three-valued logic, where an unknown lane (a null below the comparison) is neither,
 * and {@code IfElse} takes its ELSE branch there. In the dense body every input lane is valid,
 * so the pair degenerates to the comparison mask itself and the connectives run in mask space.
 * {@code IfElse} validity is {@code (kT & validThen) | (~kT & validElse)}: the chosen branch's
 * validity, lane-wise, nothing ANDed globally.
 *
 * <p>{@code dayofweek}/{@code weekday} lower to a full-range mod-7 by base-8 digit sum, which
 * measures 8x the lanewise-DIV variant that x86 scalarizes (see {@code PLAN_TASK_11.md}): fold
 * 15-, 6- and 3-bit chunks ({@code 2^(3k) = 1 mod 7}), correct by {@code +3} where the input is
 * negative ({@code 2^32 = 4 mod 7}), one compare-subtract fixup, then the constant offset
 * applied after the mod so it cannot overflow.
 *
 * <p><b>Selection outputs.</b> A {@link Cond} may itself be an output root, and such an output
 * is a <i>selection bitmap</i> rather than a column - the root's known-true word OR-ed into
 * {@code dstValidity} exactly where a value root ORs its validity word, with the
 * {@code dstData} slot unused (callers pass {@code 0L}; the body never materializes it). The
 * bitmap's semantics are SQL's {@code WHERE}: a set bit means known true, so an unknown lane
 * reads as false - free by construction, because {@code kT} is a subset of the operands'
 * validity. This is the filter kernel: one Cond root per predicate, with no value outputs
 * beside it.
 *
 * <p><b>The epilogue, not a scalar tail.</b> The rows past {@code loopBound} are one more
 * iteration of the same lane-group body, under the mask {@code indexInRange} builds for a
 * partial group - {@code i} is {@code loopBound}, {@code lanes} becomes the remainder so every
 * validity helper stays bounded by the group, and only the loads and the stores take their
 * masked overloads. The masked load is required rather than preferred: the data segment is
 * sized to {@code length} times the lane's width in bytes, so an unmasked load of the last
 * partial group would run off its end. Lanes outside the mask are neither read nor faulted on,
 * which is what lets one masked iteration cover a partial group at all. The obvious
 * alternative - a per-row topological pass lowering every node type a second
 * time into int locals - is rejected because it is a complete second walk of the IR whose
 * every arm would have to grow with each new node type.
 *
 * <p><b>Inactive lanes read {@code 0}, so no operation in the walk may trap on {@code 0}.</b>
 * That is the invariant the epilogue rests on, and today it holds for free: the mod-7
 * lowerings divide by the constant 7, and add, sub, compare, blend, max, min and the shifts
 * are total. The first trapping operation to enter the IR - ANSI division above all - has to
 * blend a safe value into the inactive lanes or use a masked lanewise form, because the
 * epilogue computes them and only declines to store them.
 *
 * <p>Every call the loop makes is declared once, in one of two places. The calls that name a
 * lane's width - the loads and stores, the broadcast, the arithmetic, the comparisons, the
 * blend - come from the emission's {@link Lane}; the calls that do not, the mask algebra and
 * the validity helpers, are the constants below. Erasure is a live hazard in both -
 * {@code add}, {@code compare}, {@code blend} and {@code max} all take the <i>erased</i>
 * {@code Vector} - and a wrong descriptor should be found by pointing at one line rather than
 * by disassembling the output.
 *
 * <p>Out-of-shape IR - unknown lane types, a condition in a value position, out-of-range
 * ordinals or slots, a day offset that is neither a literal slot nor a column, trees past
 * {@link #MAX_CHAIN_DEPTH} or {@link #MAX_FUSED_NODES} - is rejected with
 * {@link IllegalArgumentException}, which the evaluator wiring treats as "fall back". Refusing
 * to emit is a normal outcome here, not an error path: the query still runs, on stock Spark.
 *
 * <p><b>Telemetry.</b> Every emitted class carries a {@code SourceFile} attribute - the
 * caller-supplied name, meant to identify the operator and stage
 * ({@code Varka_Project_Stage3.java}), so a stack frame in the generated {@code run} names the
 * plan node it came from without any mapping table - and a {@link VarkaDebugInfo} custom
 * attribute holding the IR and the caller's plan fragment, so a captured class is
 * self-describing. Both are metadata the JVM ignores; neither costs anything at runtime.
 */
public final class VarkaLoopEmitter {

  /**
   * The deepest op path (root to leaf, per output) the emitter accepts, fixed by measurement
   * (VarkaEmitterParityBenchmark; details in PLAN_TASK_9.md): fused throughput declines only
   * gently with depth while sequential passes collapse linearly, so the cap bounds emitted
   * method size and register pressure by policy, well past any depth a real projection
   * produces, rather than marking a measured performance edge. Condition nodes count.
   */
  public static final int MAX_CHAIN_DEPTH = 16;

  /**
   * The most distinct op nodes one emitted kernel may hold, across all outputs after CSE. Depth
   * alone no longer bounds method size once outputs multiply, so this is the total-size counterpart
   * of {@link #MAX_CHAIN_DEPTH}: a policy bound far past any real projection, kept honest by the
   * widest-shape case in the parity benchmark. The ops are spread over loop methods of at most
   * {@code GROUP_BUDGET} ops each, so this caps the kernel, not any one compiled method.
   */
  public static final int MAX_FUSED_NODES = 64;


  /**
   * The most input columns one emitted loop may read. A node's referenced-column set is a long
   * bitset, which fixes the representation limit at 64; real projections reference a handful.
   */
  public static final int MAX_INPUTS = 64;

  /**
   * one step of an arm chain - an {@code IfElse} and which of its two arms. The condition it names
   * is what the guard is qualified by, taken as-is for the then arm and complemented for the else
   * arm; see {@link #emitArmContext} for why the complement and not the known-false word.
   */
  record ArmStep(IfElse node, boolean thenBranch) { }


  private VarkaLoopEmitter() {
  }

  // The word-reference value meaning "constant all-true" (a literal-only subtree).
  static final int WORD_ALL_TRUE = -1;
  /**
   * The word-reference value meaning "this word is dead in this body": no consumer
   * left in the method reads it, so it is neither allocated nor computed. Only an own word
   * takes this value - an input's word keeps its slot for parity with the dense body's layout
   * and is marked dead in {@link Slots#deadRefs} instead. {@link #loadWord} refuses both.
   */
  static final int WORD_DEAD = -2;


  /**
   * The lane every output root agrees on. The roots are the emission's outputs, and a class
   * holds one species: its loop, its epilogue and its stores are all that species, so two roots
   * on different lanes are two kernels rather than one. `analyze` re-checks every node below
   * them against this, which is where a mixed *tree* is caught.
   */
  private static Lane laneOf(List<VarkaVectorIR> outputs) {
    if (outputs.isEmpty()) {
      throw new IllegalArgumentException("no output chains to emit");
    }
    // The emission lane, not the root's: a NarrowLane root is a 32-bit column computed in the
    // 64-bit lane its child is on, and the loop runs at the child's species.
    Lane lane = Lane.of(VarkaVectorIR.emissionLane(outputs.get(0)));
    for (VarkaVectorIR output : outputs) {
      if (VarkaVectorIR.emissionLane(output) != lane.laneType) {
        throw new IllegalArgumentException("outputs mix lanes: " + lane.laneType + " and "
            + VarkaVectorIR.emissionLane(output));
      }
    }
    return lane;
  }

  /** {@link #emitLanes}, for the suite that checks a baked width against what the JVM has. */
  static int emitLanesForTest(VarkaEmitOptions options, Lane lane) {
    return emitLanes(options, lane);
  }

  /**
   * The lane count to bake into the emitted class, or 0 for "do not bake one" - which is what
   * {@link VarkaEmitOptions#validityByWidth} off means, and what a width the class cannot both
   * name and serve means.
   *
   * <p>A baked width needs two things that a lane count alone does not guarantee. It needs a
   * named species constant, which is a question about the width in bits: {@code SPECIES_64}
   * through {@code SPECIES_512} exist, and the shapes SVE reaches above 512 bits have no name.
   * And it needs the width-specialised validity helpers in {@link VarkaVectorSupport}, which
   * exist per lane *count*: 2, 4, 8 and 16. At the int lane the two sets coincide; at the long
   * lane they do not, because a single 64-bit lane is a species that exists and a helper that
   * does not. Anything the pair of checks rejects runs on {@code SPECIES_PREFERRED} and the
   * general helpers, which is correct at every width and no slower than before task 92.
   */
  static int emitLanes(VarkaEmitOptions options, Lane lane) {
    if (!options.validityByWidth()) {
      return 0;
    }
    int lanes = options.lanesOverride() != 0 ? options.lanesOverride() : lane.preferredLanes;
    // Both checks, not either: a width the class can name but not serve emits a call to a
    // validity helper that does not exist, which verifies and throws NoSuchMethodError on the
    // first masked batch. One long lane is that width, reachable with no override at all on a
    // JVM whose widest vector is 64 bits.
    return lane.hasSpecies(lanes) && hasValidityHelpers(lanes) ? lanes : 0;
  }

  /**
   * Whether {@link VarkaVectorSupport} carries a width-specialised validity pair for this many
   * lanes. A width without one is emitted against {@code SPECIES_PREFERRED} and the general
   * helpers, which is correct at any width and no slower than before task 92 existed.
   */
  private static boolean hasValidityHelpers(int lanes) {
    return lanes == 2 || lanes == 4 || lanes == 8 || lanes == 16;
  }


  /**
   * The telemetry-defaulted form of
   * {@link #emit(String, List, int, int, String, String, VarkaEmitOptions)}: the
   * {@code SourceFile} name falls back to the class's own simple name, the plan fragment to
   * empty, and the options to {@link VarkaEmitOptions#DEFAULTS}. For callers that hold no plan -
   * tests and benchmarks building IR by hand.
   */
  public static byte[] emit(
      String className, List<VarkaVectorIR> outputs, int numInputs, int numLiterals) {
    return emit(className, outputs, numInputs, numLiterals, null, null,
        VarkaEmitOptions.DEFAULTS);
  }

  /** As above, with telemetry strings and default options. */
  public static byte[] emit(
      String className, List<VarkaVectorIR> outputs, int numInputs, int numLiterals,
      String sourceFile, String planFragment) {
    return emit(className, outputs, numInputs, numLiterals, sourceFile, planFragment,
        VarkaEmitOptions.DEFAULTS);
  }

  /**
   * Assembles the fused-kernel class for the given output trees over {@code numInputs} columns
   * and {@code numLiterals} scalar-argument slots. Output {@code o} writes
   * {@code dstData[o]}/{@code dstValidity[o]}; a {@link ColumnRef} ordinal indexes the
   * {@code src*} arrays.
   *
   * <p>{@code sourceFile} becomes the class's {@code SourceFile} attribute - callers name the
   * operator and stage there so stack traces name the plan node - and {@code planFragment} is
   * carried verbatim in the {@link VarkaDebugInfo} attribute beside the IR (the telemetry note
   * in the class doc). Either may be null; see the four-argument form for the defaults. Neither
   * belongs in the shape key: each is already a function of the shape hash the cache computes.
   *
   * <p>{@code options} carries every other byte-affecting input - the group budget, CSE, the
   * mod-7 lowering, the descriptor fault injector. Unlike the two strings it <i>does</i> ride the
   * cache key, because it changes the loop rather than the labels on it; see
   * {@link VarkaEmitOptions}.
   *
   * @throws IllegalArgumentException if the IR is outside what this emitter serves - the
   *         caller is expected to fall back to the per-row projection, exactly as a kernel
   *         failure does.
   */
  public static byte[] emit(
      String className, List<VarkaVectorIR> outputs, int numInputs, int numLiterals,
      String sourceFile, String planFragment, VarkaEmitOptions options) {
    if (outputs.isEmpty()) {
      throw new IllegalArgumentException("no output chains to emit");
    }
    if (numInputs < 1 || numInputs > MAX_INPUTS) {
      throw new IllegalArgumentException(
          "numInputs " + numInputs + " outside [1, " + MAX_INPUTS + "]");
    }
    if (options == null) {
      // Checked beside the others rather than left to fail as a bare NPE deep in the walk;
      // VarkaShapeKey rejects a null the same way, so this closes the other door in.
      throw new IllegalArgumentException("emit options must not be null");
    }
    Analysis analysis = new Analysis(numInputs, numLiterals, options, laneOf(outputs));
    for (VarkaVectorIR root : outputs) {
      analysis.analyzeRoot(root);
    }
    analysis.collectArmContexts(outputs);
    analysis.collectGuardedProducers();
    analysis.planWordAlgebra();
    analysis.planBitmapPass(outputs);

    // Method layout, all sharing the seven-parameter shape so slots line up everywhere: `run`
    // dispatches per batch to a dense or masked *driver*; the driver zeroes the output validity,
    // takes the all-null shortcut, then calls one sibling *loop* method per output group (within
    // GROUP_BUDGET, or FUSED_CEILING where the group's outputs share a calendar prefix - see
    // groupOutputs) and finally the sibling *epilogue* method. Separate methods, not one big one:
    // each gets its own C2 compilation, so no method's node and inlining budgets can starve
    // another's intrinsics (measured 3x to 4x; see `PLAN_TASK_10.md`).
    ClassDesc classDesc = ClassDesc.of(className);
    boolean anyColumns = analysis.referencedColumns != 0;
    List<List<Integer>> groups = groupOutputs(outputs, options);
    String source = sourceFile != null
        ? sourceFile : className.substring(className.lastIndexOf('.') + 1) + ".java";
    VarkaDebugInfo debugInfo = new VarkaDebugInfo(
        "outputs=" + VarkaBodyEmitter.renderOutputs(outputs) + ", numInputs=" + numInputs
            + ", numLiterals=" + numLiterals,
        planFragment != null ? planFragment : "",
        VarkaBodyEmitter.renderLineMap(analysis));
    return ClassFile.of().build(classDesc, (ClassBuilder b) -> {
      b.withFlags(AccessFlag.PUBLIC, AccessFlag.FINAL)
          .withInterfaceSymbols(FUSED_KERNEL)
          .with(java.lang.classfile.attribute.SourceFileAttribute.of(source))
          .with((java.lang.classfile.ClassElement) debugElement(debugInfo))
          .withMethodBody("<init>", INIT, AccessFlag.PUBLIC.mask(), (CodeBuilder cb) -> {
            cb.aload(0);
            cb.invokespecial(ConstantDescs.CD_Object, "<init>", INIT);
            cb.return_();
          })
          .withMethodBody("run", analysis.lane.runDesc, AccessFlag.PUBLIC.mask(),
              (CodeBuilder cb) -> emitDispatch(cb, classDesc, analysis));
      // A kernel that nulls a valid input (non-ANSI make_date) has no dense methods: the dense body
      // writes no per-lane validity, so the dispatch takes the masked methods for every batch, and
      // the masked body treats a null-free input as a constant word.
      if (!analysis.nullsFromValidInputs) {
        b.withMethodBody("runDense", analysis.lane.runDesc, AccessFlag.PRIVATE.mask(),
            (CodeBuilder cb) -> VarkaBodyEmitter.emitBody(cb, true, BodyMode.DRIVER, -1,
                classDesc, outputs, analysis, numLiterals, groups))
            .withMethodBody("epilogueDense", analysis.lane.runDesc, AccessFlag.PRIVATE.mask(),
                (CodeBuilder cb) -> VarkaBodyEmitter.emitBody(cb, true, BodyMode.EPILOGUE, -1,
                    classDesc, outputs, analysis, numLiterals, groups));
        for (int g = 0; g < groups.size(); g++) {
          final int group = g;
          b.withMethodBody("loopDense" + g, analysis.lane.runDesc, AccessFlag.PRIVATE.mask(),
              (CodeBuilder cb) -> VarkaBodyEmitter.emitBody(cb, true, BodyMode.LOOP, group,
                  classDesc, outputs, analysis, numLiterals, groups));
        }
      }
      if (anyColumns || analysis.nullsFromValidInputs) {
        b.withMethodBody("runMasked", analysis.lane.runDesc, AccessFlag.PRIVATE.mask(),
            (CodeBuilder cb) -> VarkaBodyEmitter.emitBody(cb, false, BodyMode.DRIVER, -1,
                classDesc, outputs, analysis, numLiterals, groups))
            .withMethodBody("epilogueMasked", analysis.lane.runDesc, AccessFlag.PRIVATE.mask(),
                (CodeBuilder cb) -> VarkaBodyEmitter.emitBody(cb, false, BodyMode.EPILOGUE, -1,
                    classDesc, outputs, analysis, numLiterals, groups));
        for (int g = 0; g < groups.size(); g++) {
          final int group = g;
          b.withMethodBody("loopMasked" + g, analysis.lane.runDesc, AccessFlag.PRIVATE.mask(),
              (CodeBuilder cb) -> VarkaBodyEmitter.emitBody(cb, false, BodyMode.LOOP, group,
                  classDesc, outputs, analysis, numLiterals, groups));
        }
      }
    });
  }

  /**
   * The write side of {@link VarkaDebugInfo}: its payload as a class element for the build
   * above. Lives here, private, beside its only call site, with the attribute subclass and
   * its write-only mapper as fully-qualified local classes in the method body - the regime
   * {@link VarkaDebugInfo}'s class doc explains (scalac cannot complete much of the
   * Class-File API, so its types stay out of every import and every non-private signature).
   * That class doc also fixes the byte format this writer and {@code read}'s mapper must
   * agree on: the writer emits the whole attribute structure, six-byte name-and-length
   * header included (the built-in mappers do the same), with the two u2 constant-pool
   * indices as the payload.
   *
   * <p>Declared to return {@code Object} - the caller casts to {@code ClassElement} inside
   * its own body - because scalac completes even a private method's signature types, and
   * {@code ClassElement} is one of the types it cannot complete.
   */
  private static Object debugElement(VarkaDebugInfo info) {
    final class Attr extends java.lang.classfile.CustomAttribute<Attr> {
      Attr(java.lang.classfile.AttributeMapper<Attr> mapper) {
        super(mapper);
      }
    }
    final class WriteMapper implements java.lang.classfile.AttributeMapper<Attr> {
      @Override
      public String name() {
        return VarkaDebugInfo.NAME;
      }

      @Override
      public Attr readAttribute(java.lang.classfile.AttributedElement enclosing,
          java.lang.classfile.ClassReader cf, int pos) {
        throw new UnsupportedOperationException(
            "write-side mapper; parsing uses VarkaDebugInfo.read()");
      }

      @Override
      public void writeAttribute(java.lang.classfile.BufWriter buf, Attr attr) {
        buf.writeIndex(buf.constantPool().utf8Entry(VarkaDebugInfo.NAME));
        buf.writeInt(6);
        buf.writeIndex(buf.constantPool().utf8Entry(info.ir()));
        buf.writeIndex(buf.constantPool().utf8Entry(info.planFragment()));
        buf.writeIndex(buf.constantPool().utf8Entry(info.lineMap()));
      }

      @Override
      public AttributeStability stability() {
        return AttributeStability.CP_REFS;
      }
    }
    return new Attr(new WriteMapper());
  }

  /** The three body-method roles; see the method-layout note in {@link #emit}. */
  enum BodyMode { DRIVER, LOOP, EPILOGUE }

  /**
   * Partitions the outputs into loop-method groups, greedily in output order, counting only
   * ops new to the group so shared subtrees keep their outputs together (and their
   * cross-output CSE). Two clauses admit the next output into the group being built:
   *
   * <ol> <li>its marginal ops keep the group within {@code groupBudget} (normally
   * {@code GROUP_BUDGET} ) - the ordinary rule;</li> <li>joining lets it skip a civil-from-days
   * prefix the group already computes, and the group stays within {@code fusedCeiling} (normally
   * {@code FUSED_CEILING} ). {@link GroupOps#saved} is what opens the wider bound, and it counts
   * prefix reuse only: a whole node the group already holds is not reason enough. An output that
   * skips a prefix makes the method strictly less work rather than a trade, which is a property of
   * the shape and holds whatever the register pressure of the day; whether a merely-shared subchain
   * pays is an empirical question that has already answered both ways (the merge measured a 1.4x
   * loss before the validity OR moved ahead of the vector work the same committed rows show it
   * winning by 1.3x - see {@code PLAN_TASK_32.md} 7.6), so it stays {@code GROUP_BUDGET} 's own
   * retuning question rather than riding on this clause. With
   * {@link VarkaEmitOptions#shareChronoPrefix} off no prefix is ever shared, so the clause never
   * fires and the weights count whole.</li> </ol>
   *
   * <p>An output wider than either bound on its own still forms a group: splitting inside one
   * output would forfeit the register residency that is the point. Greedy in output order is
   * a known limitation: in {@code year(d), year(d2), month(d)} the month is offered to the
   * group holding {@code year(d2)}, whose prefix it cannot reuse, so it forms a third group and
   * recomputes a prefix it would have shared had it been adjacent to {@code year(d)}. The suite
   * pins that as a limitation; reordering outputs for prefix affinity is in the milestone's
   * debt register, because the evaluator's per-output vectors and the debug line map key on
   * the projection's order.
   */
  private static List<List<Integer>> groupOutputs(List<VarkaVectorIR> outputs,
      VarkaEmitOptions options) {
    List<List<Integer>> groups = new ArrayList<>();
    List<Integer> current = new ArrayList<>();
    GroupOps group = new GroupOps(options.shareChronoPrefix());
    for (int o = 0; o < outputs.size(); o++) {
      GroupOps withNext = group.copy();
      int marginal = withNext.add(outputs.get(o));
      // What clause 2 counts as reuse. By default only a civil-from-days prefix the group already
      // computes. Under `shareWholeNodes` any node the group already holds counts too, measured as
      // what this output would cost on its own less what it actually adds - which is the prefix
      // accounting generalised, since a reused prefix is reused nodes. The gate stays `> 0`: reuse
      // opens the wider bound, its size does not.
      int reuse = withNext.saved;
      if (options.shareWholeNodes()) {
        GroupOps alone = new GroupOps(options.shareChronoPrefix());
        reuse = alone.add(outputs.get(o)) - marginal;
      }
      boolean fits = group.ops + marginal <= options.groupBudget()
          || (reuse > 0 && group.ops + marginal <= options.fusedCeiling());
      // marginal == 0 means this output adds no node the group does not already have - it
      // is structurally the same tree - so splitting it off cannot reduce the method's op
      // count and only costs it the CSE. That matters once a node can outweigh the budget on
      // its own: after one calendar output `ops` already exceeds it, so without this test
      // `SELECT year(d) AS a, year(d) AS b` would emit the decomposition twice.
      if (!current.isEmpty() && marginal > 0 && !fits) {
        groups.add(current);
        current = new ArrayList<>();
        withNext = new GroupOps(options.shareChronoPrefix());
        withNext.add(outputs.get(o));
      }
      current.add(o);
      group = withNext;
    }
    groups.add(current);
    return groups;
  }

  /**
   * What one loop-method group costs so far, for {@link #groupOutputs}: the distinct nodes it
   * holds, the dates whose civil-from-days prefix it computes, and the op total under the
   * split {@code CHRONO_PREFIX_WEIGHT} describes - a calendar node whose prefix the group
   * already computes adds only its tail.
   *
   * <p>The prefix is identified by the date it decomposes ({@link #chronoChild}), which is the
   * dense body's {@link FragmentKey}; the masked body's key also carries the node's validity word,
   * so a group may hold two calendar outputs whose masked bodies do not share (the column-count
   * {@code add_months} beside {@code month(d)}) while the dense body and the epilogue do. Grouping
   * on the child alone is the conservative side of that: the shape is correct either way, and a
   * share the masked body misses is a missed win, never a wrong grouping.
   */
  private static final class GroupOps {
    private final boolean sharePrefix;
    private final Set<VarkaVectorIR> nodes;
    private final Set<VarkaVectorIR> prefixes;
    /** The group's op total. */
    int ops;
    /** How many ops the last {@link #add} skipped by reusing prefixes the group already
     * computed; zero for an output that reuses none. */
    int saved;

    GroupOps(boolean sharePrefix) {
      this(sharePrefix, new HashSet<>(), new HashSet<>(), 0);
    }

    private GroupOps(boolean sharePrefix, Set<VarkaVectorIR> nodes,
        Set<VarkaVectorIR> prefixes, int ops) {
      this.sharePrefix = sharePrefix;
      this.nodes = nodes;
      this.prefixes = prefixes;
      this.ops = ops;
    }

    GroupOps copy() {
      return new GroupOps(sharePrefix, new HashSet<>(nodes), new HashSet<>(prefixes), ops);
    }

    /** Adds the output's distinct nodes; returns how many ops were new, and leaves in
     * {@link #saved} how many the output skipped by reusing a prefix already here. */
    int add(VarkaVectorIR root) {
      saved = 0;
      int before = ops;
      walk(root);
      return ops - before;
    }

    private void walk(VarkaVectorIR node) {
      if (!nodes.add(node)) {
        return;
      }
      int weight = weightOf(node);
      if (sharePrefix && isChrono(node) && !prefixes.add(chronoChild(node))) {
        weight -= CHRONO_PREFIX_WEIGHT;
        saved += CHRONO_PREFIX_WEIGHT;
      }
      ops += weight;
      for (VarkaVectorIR child : childrenOf(node)) {
        walk(child);
      }
    }
  }

  /** Whether {@code root}'s subtree contains a member of {@code nodes} (structural equality). */
  static boolean reaches(VarkaVectorIR root, Set<VarkaVectorIR> nodes) {
    if (nodes.contains(root)) {
      return true;
    }
    for (VarkaVectorIR child : childrenOf(root)) {
      if (reaches(child, nodes)) {
        return true;
      }
    }
    return false;
  }

  /** The date a calendar node decomposes - the one child its shared prefix depends on. */
  static VarkaVectorIR chronoChild(VarkaVectorIR node) {
    return switch (node) {
      case Year n -> n.days();
      case Month n -> n.days();
      case DayOfMonth n -> n.days();
      case Quarter n -> n.days();
      case DayOfYear n -> n.days();
      case AddMonths n -> n.days();
      case LastDay n -> n.days();
      case TruncDate n -> n.days();
      case TruncDateDynamic n -> n.days();
      case WeekOfYear n -> n.days();
      default -> throw new IllegalStateException("not a calendar node: " + node);
    };
  }

  /**
   * Whether {@code node}'s tail reads the March-based month the prefix would otherwise leave in
   * {@code t[5]} - an exhaustive switch over the same family {@link #chronoChild} covers, so a
   * new calendar node is a compile error here rather than a silent "yes" that quietly costs
   * five ops, or a silent "no" that reads an uninitialised local.
   *
   * <p>Only {@link Year} answers no today: it takes the January turn off the day of year, which
   * is the same test one step earlier in the chain ({@link VarkaChrono#MARCH_TO_JANUARY_DAYS}).
   * {@link Month} and {@link Quarter} go through {@code emitChronoMonth}, {@link DayOfMonth}
   * through {@code emitMonthStart}, and {@link AddMonths} needs both.
   */
  static boolean tailReadsMarchMonth(VarkaVectorIR node) {
    return switch (node) {
      case Year n -> false;
      case Month n -> true;
      case DayOfMonth n -> true;
      case Quarter n -> true;
      case DayOfYear n -> false;
      case AddMonths n -> true;
      case LastDay n -> true;
      // MONTH reads the numerator for the zero-based day of month, QUARTER goes through
      // emitChronoMonth for the quarter; YEAR takes the January turn off the day of year like
      // Year and DayOfYear do, under either lowering (the recompose form's January month is a
      // constant).
      case TruncDate n -> n.level() != TruncLevel.YEAR;
      // Its MONTH and QUARTER results are the literal tails', so it always reads the month.
      case TruncDateDynamic n -> true;
      // The week tail is the day-of-year tail plus a division: no month.
      case WeekOfYear n -> false;
      default -> throw new IllegalStateException("not a calendar node: " + node);
    };
  }

  /**
   * A run of emitted lane ops that several nodes need, that depends on one shared child, and that
   * leaves its results in scratch locals rather than on the operand stack. It is the sub-node
   * counterpart of the CSE {@link #emitValue} already does between whole nodes: what is worth
   * sharing between {@code year(d)} and {@code month(d)} is not a node - the IR has none for it -
   * but the forty-odd ops in the middle of both their emissions.
   *
   * <p>One kind so far. The key carries it so that a second one is additive rather than a
   * rewrite of everything keyed on it.
   */
  enum FragmentKind { CHRONO_PREFIX }

  /**
   * What makes two emissions of a fragment interchangeable: the kind, the child they decompose,
   * and the reference the node's validity word resolves to.
   *
   * <p>The word's presence in the key is now conservative rather than load-bearing, and the reason
   * recorded here no longer applies: {@code emitChronoPrefix} once carried the narrow-range guard,
   * which read the node's validity word, so two nodes with different words could not share a
   * prefix. The prefix reads no word at all today, so keying on the word cannot make a shared
   * fragment wrong - it can only miss a share that would have been sound.
   *
   * <p>It costs one, and that starts to show where {@code planWordRef} aliases every {@link Chrono}
   * extraction's word to its child's, so {@code year(d)} and {@code month(d)} agree and share, but
   * {@link AddMonths} 's word is the AND of the date's and the month count's, so a column-count
   * {@code add_months(d, m)} is the first chrono node whose word is its own - and it no longer
   * shares the forty-odd-op decomposition of {@code d} with {@code month(d)}. Only a masked body
   * pays: in a dense body no word is planned at all and the child alone decides. Dropping
   * {@code word} from the key would recover the share, and is safe as far as this analysis goes,
   * but it changes emitted bytes and so wants its own measurement.
   *
   * @param word the node's validity-word reference, or null in a dense body.
   */
  record FragmentKey(FragmentKind kind, VarkaVectorIR child, Integer word) {}

  /**
   * Which of this lane group's prefix fragments a tail in it reads the March-based month out
   * of, over the union of the group's outputs' subtrees. The walk is the group's own
   * because {@link Slots#fragmentsReadingMonth} is the group's own - see its doc for why the
   * body's whole output list would be too wide - and it precedes every emission in the group,
   * so no sibling's order can change what it decides.
   */
  static void planFragmentsReadingMonth(List<VarkaVectorIR> outputs,
      List<Integer> outputIdx, boolean dense, Slots s) {
    s.fragmentsReadingMonth.clear();
    Set<VarkaVectorIR> seen = new HashSet<>();
    List<VarkaVectorIR> pending = new ArrayList<>();
    for (int o : outputIdx) {
      pending.add(outputs.get(o));
    }
    while (!pending.isEmpty()) {
      VarkaVectorIR node = pending.remove(pending.size() - 1);
      if (!seen.add(node)) {
        continue;
      }
      if (isChrono(node) && tailReadsMarchMonth(node)) {
        s.fragmentsReadingMonth.add(fragmentKey(node, dense, s));
      }
      for (VarkaVectorIR child : childrenOf(node)) {
        pending.add(child);
      }
    }
  }

  /** {@link FragmentKey} for {@code node}'s civil-from-days prefix; see that record's doc. */
  static FragmentKey fragmentKey(VarkaVectorIR node, boolean dense, Slots s) {
    return new FragmentKey(FragmentKind.CHRONO_PREFIX, chronoChild(node),
        dense ? null : s.wordRef.get(node));
  }

  static VarkaVectorIR[] childrenOf(VarkaVectorIR node) {
    return switch (node) {
      case ColumnRef c -> new VarkaVectorIR[0];
      case LiteralSlot l -> new VarkaVectorIR[0];
      case AddDays n -> new VarkaVectorIR[] {n.days(), n.offset()};
      case SubDays n -> new VarkaVectorIR[] {n.days(), n.offset()};
      case GuardedDay n -> new VarkaVectorIR[] {n.days()};
      case GuardedRange n -> new VarkaVectorIR[] {n.child()};
      case NarrowLane n -> new VarkaVectorIR[] {n.child()};
      case DateDiff n -> new VarkaVectorIR[] {n.end(), n.start()};
      case DayOfWeek n -> new VarkaVectorIR[] {n.days()};
      case WeekDay n -> new VarkaVectorIR[] {n.days()};
      case DayOfWeekIso n -> new VarkaVectorIR[] {n.days()};
      case NextDay n -> new VarkaVectorIR[] {n.days(), n.offset()};
      case ThursdayOf n -> new VarkaVectorIR[] {n.days()};
      case Year n -> new VarkaVectorIR[] {n.days()};
      case Month n -> new VarkaVectorIR[] {n.days()};
      case DayOfMonth n -> new VarkaVectorIR[] {n.days()};
      case Quarter n -> new VarkaVectorIR[] {n.days()};
      case DayOfYear n -> new VarkaVectorIR[] {n.days()};
      case LastDay n -> new VarkaVectorIR[] {n.days()};
      case TruncDate n -> new VarkaVectorIR[] {n.days()};
      case TruncDateDynamic n -> new VarkaVectorIR[] {n.days(), n.level()};
      case WeekOfYear n -> new VarkaVectorIR[] {n.days()};
      case AddMonths n -> new VarkaVectorIR[] {n.days(), n.months()};
      case MakeDate n -> new VarkaVectorIR[] {n.year(), n.month(), n.day()};
      case Greatest n -> new VarkaVectorIR[] {n.left(), n.right()};
      case Least n -> new VarkaVectorIR[] {n.left(), n.right()};
      case IfElse n -> new VarkaVectorIR[] {n.cond(), n.thenNode(), n.elseNode()};
      case Compare n -> new VarkaVectorIR[] {n.left(), n.right()};
      case And n -> new VarkaVectorIR[] {n.left(), n.right()};
      case Or n -> new VarkaVectorIR[] {n.left(), n.right()};
      case Not n -> new VarkaVectorIR[] {n.child()};
      case IsNotNull n -> new VarkaVectorIR[] {n.child()};
      case IntArith n -> new VarkaVectorIR[] {n.left(), n.right()};
      case IntNeg n -> new VarkaVectorIR[] {n.child()};
      case ConstDivide n -> new VarkaVectorIR[] {n.child()};
      case BoundedDivide n -> new VarkaVectorIR[] {n.child()};
    };
  }

  /**
   * Whether {@code outputs} over {@code numInputs} kernel columns fit this emitter's
   * structural budgets ({@link #MAX_FUSED_NODES} distinct ops across all outputs,
   * {@link #MAX_CHAIN_DEPTH} height per output, {@link #MAX_INPUTS} columns), counted
   * exactly as {@link Analysis} and {@link #emit} count them. The compiler mirrors the
   * budgets with this before accepting an entry: an over-budget shape that reaches
   * {@link #emit} fails there with an {@code IllegalArgumentException} the evaluator can
   * only turn into a silent per-batch fallback - no task-16 decline reason, and EXPLAIN
   * still claims fusion. Checked here instead, the offending entry is demoted to residual
   * with a recorded reason.
   *
   * <p>The lane agreement {@link #laneOf} demands is checked on the same terms and for the
   * same reason: one class holds one species, so outputs on different lanes are two kernels,
   * and a caller that learned that from an exception at {@code emit} would have learned it too
   * late to record why.
   */
  public static boolean fitsBudgets(java.util.List<VarkaVectorIR> outputs, int numInputs) {
    if (numInputs > MAX_INPUTS || outputs.isEmpty()) {
      return false;
    }
    for (VarkaVectorIR output : outputs) {
      if (VarkaVectorIR.emissionLane(output) != VarkaVectorIR.emissionLane(outputs.get(0))) {
        return false;
      }
    }
    java.util.HashMap<VarkaVectorIR, Integer> heights = new java.util.HashMap<>();
    int[] opNodes = {0};
    for (VarkaVectorIR root : outputs) {
      if (budgetWalk(root, heights, opNodes) > MAX_CHAIN_DEPTH
          || opNodes[0] > MAX_FUSED_NODES) {
        return false;
      }
    }
    return true;
  }

  /**
   * How the bitmap pass classified the value roots of this shape under
   * {@code options}: {@code [served, declined]}, where a declined root is one whose word is a
   * pure expression that mixes AND and OR, which no chain of one operator can fold into the
   * destination. Every other unserved root - a computed word, a {@code Cond}, the whole shape
   * with {@link VarkaEmitOptions#validityByBitmap} off - is in neither count.
   *
   * <p>This is the safety net PLAN_TASK_70.md 3.1 and 5 promise and the counter alone did not
   * provide: with only an increment inside a private class, a regression that stopped serving
   * every root would revert the whole lowering to the per-group path and pass every test, since
   * the byte-identity tests compare the two settings (equal when nothing is served), the
   * differential compares against a reference evaluator (the per-group path is correct), and
   * the size assertions are upper bounds. The suite pins both numbers per shape instead.
   *
   * <p>Runs the analysis and nothing else - no bytes are emitted, so it is safe to call on a
   * shape whose emission would exceed a budget.
   */
  public static int[] bitmapPassCounts(List<VarkaVectorIR> outputs, int numInputs,
      int numLiterals, VarkaEmitOptions options) {
    Analysis analysis = new Analysis(numInputs, numLiterals, options, laneOf(outputs));
    for (VarkaVectorIR root : outputs) {
      analysis.analyzeRoot(root);
    }
    analysis.collectArmContexts(outputs);
    analysis.collectGuardedProducers();
    analysis.planWordAlgebra();
    analysis.planBitmapPass(outputs);
    int served = 0;
    for (BitmapPass pass : analysis.served) {
      if (pass != null) {
        served++;
      }
    }
    return new int[] {served, analysis.declinedBitmapRoots};
  }

  /** The height of {@code node}, memoized per distinct node like {@code Analysis.height}. */
  private static int budgetWalk(VarkaVectorIR node,
      java.util.HashMap<VarkaVectorIR, Integer> heights, int[] opNodes) {
    Integer memo = heights.get(node);
    if (memo != null) {
      return memo;
    }
    int height;
    if (node instanceof ColumnRef || node instanceof LiteralSlot) {
      height = 0;
    } else {
      opNodes[0]++;
      int maxChild = 0;
      for (VarkaVectorIR child : childrenOf(node)) {
        maxChild = Math.max(maxChild, budgetWalk(child, heights, opNodes));
      }
      height = 1 + maxChild;
    }
    heights.put(node, height);
    return height;
  }

  /**
   * The public {@code run}: one loop-invariant test per batch - are all referenced inputs
   * null-free? - selecting {@code runDense} or {@code runMasked} (`PLAN_TASK_10.md` 2.5).
   */
  private static void emitDispatch(CodeBuilder cb, ClassDesc classDesc, Analysis analysis) {
    if (analysis.nullsFromValidInputs) {
      // No dense path for this kernel (see emit): every batch is served by the masked methods.
      invokeBody(cb, classDesc, "runMasked", analysis.lane);
      return;
    }
    Label masked = cb.newLabel();
    boolean anyColumns = analysis.referencedColumns != 0;
    for (int i = 0; i < analysis.numInputs; i++) {
      if (referenced(analysis, i)) {
        cb.aload(P_NULL_COUNT);
        cb.loadConstant(i);
        cb.iaload();
        cb.ifne(masked);
      }
    }
    invokeBody(cb, classDesc, "runDense", analysis.lane);
    if (anyColumns) {
      cb.labelBinding(masked);
      invokeBody(cb, classDesc, "runMasked", analysis.lane);
    }
    // With no referenced columns the masked label is never targeted and must not be bound:
    // unreachable code has no stack frame to compute.
  }

  /** {@code this.<name>(srcData, ..., length)} - all seven parameters forwarded. */
  static void invokeCall(CodeBuilder cb, ClassDesc classDesc, String name, Lane lane) {
    cb.aload(0);
    cb.aload(P_SRC_DATA);
    cb.aload(P_SRC_VALIDITY);
    cb.aload(P_NULL_COUNT);
    cb.aload(P_DST_DATA);
    cb.aload(P_DST_VALIDITY);
    cb.aload(P_SCALAR_ARGS);
    if (lane.pLongArgs >= 0) {
      cb.aload(lane.pLongArgs);
    }
    cb.iload(lane.pLength);
    cb.invokespecial(classDesc, name, lane.runDesc);
  }

  /** {@link #invokeCall} whose status becomes this method's own - a tail call in effect. */
  private static void invokeBody(CodeBuilder cb, ClassDesc classDesc, String name, Lane lane) {
    invokeCall(cb, classDesc, name, lane);
    cb.ireturn();
  }

  static boolean referenced(Analysis analysis, int ordinal) {
    return (analysis.referencedColumns >>> ordinal & 1L) != 0;
  }

  /**
   * The node kinds a {@code date_add}/{@code date_sub} day offset may be. Public because
   * `VarkaExpressionCompiler` gates its offset arm on exactly this: the compiler deciding what
   * to build and the emitter deciding what to accept are one rule, and stating it twice is how
   * `date_add(d, weekday(d2) + 1)` came to be fused in EXPLAIN and refused at emit time, which
   * the evaluator turns into a silent per-batch fallback. Widen this and both move together.
   */
  public static boolean isDayOffsetShape(VarkaVectorIR offset) {
    return offset instanceof LiteralSlot || offset instanceof ColumnRef
        || offset instanceof IntArith || offset instanceof IntNeg;
  }

  /**
   * Attributes the instructions emitted next to the node's own line of the notional source
   * file - its 1-based topological index. Called immediately before each node's
   * defining instruction, so a stack trace through the generated loop names the IR node that
   * threw rather than only the method; {@link VarkaDebugInfo} carries the decoding key.
   */
  static void line(CodeBuilder cb, Analysis analysis, VarkaVectorIR node) {
    Integer number = analysis.lineNumbers.get(node);
    if (number != null) {
      cb.lineNumber(number);
    }
  }

  /**
   * Pushes a validity word: a long local, or the all-true constant. The one call every consumer
   * of a word goes through, which is what makes {@link Slots#wordUses} a complete count.
   */
  static void loadWord(CodeBuilder cb, Slots s, int ref) {
    if (ref == WORD_ALL_TRUE) {
      cb.loadConstant(-1L);
    } else if (ref == WORD_DEAD || s.deadRefs.contains(ref)) {
      throw new IllegalStateException("a word the liveness pass declared dead is loaded: slot "
          + ref + " - a consumer the inventory in liveWords does not list");
    } else {
      cb.lload(ref);
      s.wordUses.merge(ref, 1, Integer::sum);
    }
  }

  /** Stores the word on the stack into its slot, recording the definition. */
  static void storeWord(CodeBuilder cb, Slots s, int ref) {
    cb.lstore(ref);
    s.wordDefs.add(ref);
  }

  // ---------------------------------------------------------------------------------------------
  // The vector walk.
  // ---------------------------------------------------------------------------------------------

  /**
   * Post-order walk leaving the node's {@code IntVector} on the operand stack. A node used
   * more than once is computed at its first (textual) use, duplicated into its local, and
   * later uses load the local - across outputs too, since the loop body is one straight line.
   * In the masked body the node's validity word is stored as a side effect of the first visit.
   */
  static void emitValue(CodeBuilder cb, VarkaVectorIR node, boolean dense,
      Analysis analysis, Slots s, Set<VarkaVectorIR> computed) {
    Integer shared = s.sharedSlot.get(node);
    if (shared != null && computed.contains(node)) {
      cb.aload(shared);
      return;
    }
    switch (node) {
      case ColumnRef c -> {
        line(cb, analysis, node);
        cb.aload(s.species);
        cb.aload(s.srcSeg[c.ordinal()]);
        cb.lload(s.byteOffset);
        cb.getstatic(BYTE_ORDER, "LITTLE_ENDIAN", BYTE_ORDER);
        if (s.epilogueMask != null) {
          cb.aload(s.epilogueMask);
          cb.invokestatic(analysis.lane.vector, "fromMemorySegment",
              analysis.lane.fromMemorySegmentMasked);
        } else {
          cb.invokestatic(analysis.lane.vector, "fromMemorySegment",
              analysis.lane.fromMemorySegmentDense);
        }
      }
      case LiteralSlot l -> {
        line(cb, analysis, node);
        if (s.broadcastSlot != null) {
          cb.aload(s.broadcastSlot[l.index()]);
        } else {
          cb.aload(s.species);
          analysis.lane.loadScalar(cb, s.scalarArg[l.index()]);
          cb.invokestatic(analysis.lane.vector, "broadcast", analysis.lane.broadcast);
        }
      }
      case AddDays n -> {
        analysis.lane.requireInt(n);
        // The misdescribe hook: whichever body executes first must fail naming the call.
        MethodTypeDesc desc =
            analysis.options.misdescribeAdd()
                ? analysis.lane.lanewiseVVWrong : analysis.lane.lanewiseVV;
        emitAndValidatedOp(cb, node, n.days(), n.offset(), "add", desc, dense, analysis, s,
            computed);
      }
      case SubDays n -> {
        analysis.lane.requireInt(n);
        emitAndValidatedOp(cb, node, n.days(), n.offset(), "sub", LANEWISE_VV,
            dense, analysis, s, computed);
      }
      case IntArith n -> emitIntArith(cb, n, dense, analysis, s, computed);
      case IntNeg n -> emitIntNeg(cb, n, dense, analysis, s, computed);
      case ConstDivide n ->
          VarkaDivisionLowering.emitConstDivide(cb, n, dense, analysis, s, computed);
      case BoundedDivide n -> {
        // `(x * M) >>> k`: the product is under 2^32 for every dividend under the bound, so
        // the int multiply's wrap is the unsigned product the logical shift reads, and the
        // constructor proved the quotient exact there. Two lane operations, no correction.
        emitValue(cb, n.child(), dense, analysis, s, computed);
        line(cb, analysis, node);
        cb.loadConstant(n.multiplier());
        cb.invokevirtual(INT_VECTOR, "mul", LANEWISE_VI);
        emitShift(cb, "LSHR", n.shift());
      }
      case DateDiff n -> {
        analysis.lane.requireInt(n);
        emitAndValidatedOp(cb, node, n.end(), n.start(), "sub", LANEWISE_VV,
            dense, analysis, s, computed);
      }
      case DayOfWeek n -> {
        analysis.lane.requireInt(n);
        emitValue(cb, n.days(), dense, analysis, s, computed);
        line(cb, analysis, node);
        VarkaChronoLowering.emitFloorMod7(cb, node, analysis, s);
        VarkaChronoLowering.emitModOffset(cb, s, 4);
        cb.loadConstant(1);
        cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI);
      }
      case WeekDay n -> {
        analysis.lane.requireInt(n);
        emitValue(cb, n.days(), dense, analysis, s, computed);
        line(cb, analysis, node);
        VarkaChronoLowering.emitFloorMod7(cb, node, analysis, s);
        VarkaChronoLowering.emitModOffset(cb, s, 3);
      }
      case GuardedDay n -> {
        analysis.lane.requireInt(n);
        // The value passes through untouched; what this node adds is two compares beside it.
        // The word is the child's, because a range check does not change validity -
        // it decides whether the batch is answered at all, not which lanes are null.
        emitValue(cb, n.days(), dense, analysis, s, computed);
        line(cb, analysis, node);
        Integer guardTmp = s.guardTmp.get(node);
        if (guardTmp != null) {
          emitRangeGuard(cb, node, dense ? null : s.wordRef.get(n.days()), guardTmp, dense,
              analysis, s, VarkaChrono.NARROW_MIN_DAYS, VarkaChrono.NARROW_MAX_DAYS);
        }
      }
      case NarrowLane n -> {
        // The value stays in the long lane here; the narrowing happens at the root's store,
        // which is the only place this node can be (see `emitNarrowStore`). The word is the
        // child's.
        emitValue(cb, n.child(), dense, analysis, s, computed);
        line(cb, analysis, node);
      }
      case GuardedRange n -> {
        // The day guard's twin at whichever lane the child is on, with the bounds the node
        // carries. The value passes through; the word is the child's.
        emitValue(cb, n.child(), dense, analysis, s, computed);
        line(cb, analysis, node);
        Integer guardTmp = s.guardTmp.get(node);
        if (guardTmp != null) {
          emitRangeGuard(cb, node, dense ? null : s.wordRef.get(n.child()), guardTmp, dense,
              analysis, s, n.lo(), n.hi());
        }
      }
      case ThursdayOf n -> {
        analysis.lane.requireInt(n);
        // t = d + 3 - weekday0(d), the Thursday of d's Monday-based week, on
        // NextDay's pattern: the date's second copy rides the operand stack across
        // emitFloorMod7, whose two dowTmp slots it would otherwise have to share.
        emitValue(cb, n.days(), dense, analysis, s, computed);   // [d]
        cb.dup();                                                // [d, d]
        line(cb, analysis, node);
        // [d, floorMod(d, 7)]
        VarkaChronoLowering.emitFloorMod7(cb, node, analysis, s);
        // [d, weekday0]
        VarkaChronoLowering.emitModOffset(cb, s, 3);
        cb.swap();                                               // [weekday0, d]
        cb.loadConstant(3);
        cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI);        // [weekday0, d + 3]
        cb.swap();                                               // [d + 3, weekday0]
        cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VV);        // [d + 3 - weekday0]
      }
      case DayOfWeekIso n -> {
        analysis.lane.requireInt(n);
        // WeekDay's tail plus one: Monday 1 to Sunday 7.
        emitValue(cb, n.days(), dense, analysis, s, computed);
        line(cb, analysis, node);
        VarkaChronoLowering.emitFloorMod7(cb, node, analysis, s);
        VarkaChronoLowering.emitModOffset(cb, s, 3);
        cb.loadConstant(1);
        cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI);
      }
      case NextDay n -> {
        analysis.lane.requireInt(n);
        // date is needed twice - once inside w = k - d, once again for the final d + r - and
        // both children must be emitted before line() re-tags the node's own instructions
        // (matching AddDays/SubDays/DateDiff), so it rides the operand stack via dup/swap
        // rather than a dedicated local: [date] -dup-> [date, date] -offset-> [date, date, k]
        // -swap-> [date, k, date], leaving exactly k.sub(date)'s [receiver, arg] shape on top
        // with the reserved date copy underneath for the later d.add(r).
        emitValue(cb, n.days(), dense, analysis, s, computed);
        cb.dup();
        emitValue(cb, n.offset(), dense, analysis, s, computed);
        cb.swap();
        line(cb, analysis, node);
        // w = k - d, wrapping on purpose: next_day's oracle is Spark's own
        // getNextDateForDayOfWeek, which computes this in plain int arithmetic, so
        // byte-exactness with the row engine means reproducing the wrap, not avoiding it.
        cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VV);
        VarkaChronoLowering.emitFloorMod7(cb, node, analysis, s);
        // result = d + r + 1, wrapping again.
        cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VV);
        cb.loadConstant(1);
        cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI);
        // A column weekday can be null on its own, so the node's word is the AND of
        // both inputs' words, stored here as AddMonths does by hand; a literal weekday is the
        // all-true word and planWordRef aliases the date's, so nothing is stored.
        if (!dense && s.ownWord.contains(node)) {
          emitAndWord(cb, s, s.wordRef.get(node), s.wordRef.get(n.days()),
              s.wordRef.get(n.offset()));
        }
      }
      case Year n -> VarkaChronoLowering.emitChrono(cb, node, dense, analysis, s, computed);
      case Month n -> VarkaChronoLowering.emitChrono(cb, node, dense, analysis, s, computed);
      case DayOfMonth n -> VarkaChronoLowering.emitChrono(cb, node, dense, analysis, s, computed);
      case Quarter n -> VarkaChronoLowering.emitChrono(cb, node, dense, analysis, s, computed);
      case DayOfYear n -> VarkaChronoLowering.emitChrono(cb, node, dense, analysis, s, computed);
      case AddMonths n -> VarkaChronoLowering.emitAddMonths(cb, n, dense, analysis, s, computed);
      case LastDay n -> VarkaChronoLowering.emitChrono(cb, node, dense, analysis, s, computed);
      case TruncDate n -> VarkaChronoLowering.emitChrono(cb, node, dense, analysis, s, computed);
      case TruncDateDynamic n -> {
        VarkaChronoLowering.emitChrono(cb, node, dense, analysis, s, computed);
        // A column level can be null on its own, so the node's word is the AND of
        // both inputs' words - NextDay's rule for its column weekday.
        if (!dense && s.ownWord.contains(node)) {
          emitAndWord(cb, s, s.wordRef.get(node), s.wordRef.get(n.days()),
              s.wordRef.get(n.level()));
        }
      }
      case MakeDate n -> VarkaChronoLowering.emitMakeDate(cb, n, dense, analysis, s, computed);
      case WeekOfYear n -> VarkaChronoLowering.emitChrono(cb, node, dense, analysis, s, computed);
      case Greatest n -> emitPick(cb, n, n.left(), n.right(), "max", dense, analysis, s,
          computed);
      case Least n -> emitPick(cb, n, n.left(), n.right(), "min", dense, analysis, s,
          computed);
      case IfElse n -> {
        emitCond(cb, n.cond(), dense, analysis, s, computed);
        emitValue(cb, n.elseNode(), dense, analysis, s, computed);
        emitValue(cb, n.thenNode(), dense, analysis, s, computed);
        line(cb, analysis, node);
        if (dense) {
          cb.aload(s.condMask.get(n.cond()));
        } else {
          cb.aload(s.species);
          cb.lload(s.kt.get(n.cond()));
          cb.invokestatic(VECTOR_MASK, "fromLong", FROM_LONG);
        }
        cb.invokevirtual(analysis.lane.vector, "blend", analysis.lane.blend);
        if (!dense && s.ownWord.contains(node)) {
          // valid = (kT & validThen) | (~kT & validElse), the chosen branch's validity.
          cb.lload(s.kt.get(n.cond()));
          loadWord(cb, s, s.wordRef.get(n.thenNode()));
          cb.land();
          cb.lload(s.kt.get(n.cond()));
          cb.loadConstant(-1L);
          cb.lxor();
          loadWord(cb, s, s.wordRef.get(n.elseNode()));
          cb.land();
          cb.lor();
          storeWord(cb, s, s.wordRef.get(node));
        }
      }
      case Cond c -> throw new IllegalStateException(
          "condition node in a value position survived validation: " + c);
    }
    if (shared != null) {
      cb.dup();
      cb.astore(shared);
      computed.add(node);
    }
  }

  /**
   * {@code IntArith}: the lanewise op, then - in `FAIL` and `NULL` - the overflow check. The check
   * is the standard sign-based test, which needs both operands and the result, and the lanewise
   * call has consumed the operands off the stack by the time the result exists, so all three are
   * parked in {@link Slots#intArithTmp} first. `emitRangeGuard` parks its one value for the same
   * reason.
   *
   * <p>{@code ADD} overflows exactly where the operands share a sign that the result does not:
   * {@code ((a ^ r) & (b ^ r)) < 0}. {@code SUB} overflows where the operands differ in sign
   * and the result differs from the left: {@code ((a ^ b) & (a ^ r)) < 0}. Both are four
   * lanewise ops and a compare, and neither branches.
   *
   * <p>{@code MUL} has no such test in int lanes - the honest check needs the 64-bit product,
   * or a division the lane loop must not do (PLAN_TASK_11.md priced lanewise DIV at 8x) - so
   * the compiler declines a checked multiply outright and only {@code WRAP} reaches here.
   * That is wider than PLAN_TASK_63.md 3.3 assumed; see the correction there.
   *
   * <p>Where the mask goes is what separates the two checked modes. {@code FAIL} folds it into
   * the batch's condemning accumulator through {@link #emitGuardCollect}, so the batch declines
   * and the row engine raises Spark's own error. {@code NULL} clears those lanes from the
   * node's own validity word instead, so the row is null and the batch runs on - which is why
   * `analyze` marks a `NULL` node as one that nulls valid inputs.
   *
   * <p>The {@code FAIL} route inherits {@link #emitGuardCollect}'s untaken-arm cliff, which is the
   * untaken-arm case: the mask is ANDed with the node's word and the epilogue mask but not with an
   * enclosing {@code IfElse}'s condition, and a vector body computes both arms, so a checked node
   * under a {@code CASE} arm condemns the batch from a lane the condition would have sent the other
   * way. Answers stay right - the row engine recomputes the batch - and only the fusion is lost, on
   * exactly the data the check exists for.
   */
  private static void emitIntArith(CodeBuilder cb, IntArith n, boolean dense, Analysis analysis,
      Slots s, Set<VarkaVectorIR> computed) {
    String op = switch (n.op()) {
      case ADD -> "add";
      case SUB -> "sub";
      case MUL -> "mul";
    };
    // Before the scratch-slot test, because the refusal is about the node and not about how
    // this body was configured: with `checkIntOverflow` off there is no scratch, and a checked
    // multiply would otherwise slip through as a plain wrapping one.
    //
    // Note what the argument is not. That switch *does* change meaning for the other checked
    // nodes, deliberately: with it off a `FAIL` add wraps and a `NULL` add answers the wrapped
    // number where Spark returns null, which is what its own javadoc promises and what makes
    // it a reference arm rather than a setting. `MUL` is different in kind - there is no
    // correct checked emission for it at all, so "off" cannot mean "the same node, cheaper".
    // A reader who generalises this into the `NULL` and `IntNeg` arms breaks the benchmark.
    if (n.op() == IntOp.MUL && n.mode() != Overflow.WRAP) {
      throw new IllegalArgumentException("a checked multiply has no int-lane overflow test: " + n);
    }
    int[] tmp = s.intArithTmp.get(n);
    if (tmp == null) {
      // WRAP, or the check switched off for the A/B: the plain lanewise op, whose word is the
      // operands' AND like every other null-intolerant binary node.
      emitAndValidatedOp(cb, n, n.left(), n.right(), op, analysis.lane.lanewiseVV, dense,
          analysis, s, computed);
      return;
    }
    int a = tmp[0];
    int b = tmp[1];
    int r = tmp[2];
    emitValue(cb, n.left(), dense, analysis, s, computed);
    emitValue(cb, n.right(), dense, analysis, s, computed);
    line(cb, analysis, n);
    cb.dup2();
    cb.invokevirtual(analysis.lane.vector, op, analysis.lane.lanewiseVV);
    cb.astore(r);
    cb.astore(b);
    cb.astore(a);
    if (!dense && s.ownWord.contains(n)) {
      emitAndWord(cb, s, s.wordRef.get(n), s.wordRef.get(n.left()), s.wordRef.get(n.right()));
    }
    // The two XORs, whose operands differ between ADD and SUB, then the AND and the sign test.
    cb.aload(a);
    cb.getstatic(VECTOR_OPERATORS, "XOR", VO_ASSOCIATIVE);
    cb.aload(n.op() == IntOp.ADD ? r : b);
    cb.invokevirtual(analysis.lane.vector, "lanewise", analysis.lane.lanewiseBinaryV);
    cb.getstatic(VECTOR_OPERATORS, "AND", VO_ASSOCIATIVE);
    cb.aload(n.op() == IntOp.ADD ? b : a);
    cb.getstatic(VECTOR_OPERATORS, "XOR", VO_ASSOCIATIVE);
    cb.aload(r);
    cb.invokevirtual(analysis.lane.vector, "lanewise", analysis.lane.lanewiseBinaryV);
    cb.invokevirtual(analysis.lane.vector, "lanewise", analysis.lane.lanewiseBinaryV);
    cb.getstatic(VECTOR_OPERATORS, "LT", VO_COMPARISON);
    analysis.lane.pushScalar(cb, 0);
    cb.invokevirtual(analysis.lane.vector, "compare", analysis.lane.compareVI);
    emitOverflowMask(cb, n.mode(), n, dense, analysis, s);
    cb.aload(r);
  }

  /**
   * {@code IntNeg}, emitted as a multiply by -1 so it needs no unary descriptor: the two agree on
   * every lane, {@link Integer#MIN_VALUE} included, where both return the input. That single value
   * is the whole of the overflow test, so the check is one compare rather than the four ops
   * {@link #emitIntArith} needs, and it reads the operand rather than the result - no scratch slot
   * at all.
   */
  private static void emitIntNeg(CodeBuilder cb, IntNeg n, boolean dense, Analysis analysis,
      Slots s, Set<VarkaVectorIR> computed) {
    emitValue(cb, n.child(), dense, analysis, s, computed);
    line(cb, analysis, n);
    boolean checked = n.mode() == Overflow.FAIL && analysis.options.checkIntOverflow();
    if (checked) {
      cb.dup();
      cb.getstatic(VECTOR_OPERATORS, "EQ", VO_COMPARISON);
      analysis.lane.pushScalar(cb, analysis.lane.mostNegative());
      cb.invokevirtual(analysis.lane.vector, "compare", analysis.lane.compareVI);
      emitOverflowMask(cb, n.mode(), n, dense, analysis, s);
    }
    analysis.lane.pushScalar(cb, -1);
    cb.invokevirtual(analysis.lane.vector, "mul", analysis.lane.lanewiseVI);
  }

  /**
   * Consumes a {@code VectorMask} of overflowing lanes and disposes of it as the mode says:
   * {@code FAIL} condemns the batch through the shared accumulator, {@code NULL} clears those
   * lanes from the node's own validity word. Narrowing a word after it was stored is what makes
   * such a node able to null a lane whose inputs were both valid, and there is one sibling that
   * does the same thing: {@code make_date}'s non-ANSI tail in {@code emitMakeDate}, which ANDs
   * the node's word with its validity mask so an invalid date is a null rather than an error.
   * The two are deliberately not one helper. They narrow by opposite polarities - this arm by
   * the complement of its mask, {@code emitMakeDate} by the mask itself - and take the mask from
   * different places, this one off the operand stack and that one out of a local, so a shared
   * helper would have to reorder the {@code land} operands at one of the two sites. That is a
   * bytecode change to a shape whose committed benchmark numbers and pinned op counts describe
   * the bytes as they are, which is a real cost for three lines. What the pair does need is to
   * stay findable from each other, which is what this paragraph and its twin there are for.
   */
  private static void emitOverflowMask(CodeBuilder cb, Overflow mode, VarkaVectorIR node,
      boolean dense, Analysis analysis, Slots s) {
    if (mode == Overflow.FAIL) {
      emitGuardCollect(cb, node, dense ? null : s.wordRef.get(node), dense, analysis, s);
      return;
    }
    // NULL: word &= ~overflow. A dense body has no word to narrow, and `emit` builds none at
    // all for a kernel whose analysis set nullsFromValidInputs - which every NULL node does -
    // so reaching here dense means that invariant broke. Refused rather than papered over with
    // a pop, which would silently drop the check and answer where Spark returns null.
    //
    // The two sibling impossibilities are refused earlier, not by a throw at this same site: a
    // NULL IntNeg never reaches emission at all, because analyze() throws on it while walking
    // every root (Spark has no try_negative, so this can only mean a bug upstream); a checked
    // MUL is declined by the compiler and, failing that, throws inside emitIntArith itself
    // before this method would ever see it. Three refusals, three places, one invariant - a
    // future refactor that unifies the overflow dispatch across IntArith and IntNeg has to
    // keep all three in view, not assume the shape of one implies the others.
    //
    // "Refused" is a claim about this class, not about a running query: `VarkaKernelEvaluator`
    // catches an IllegalArgumentException out of `emit` as an emission failure and drops the
    // whole task to the row path, so in production a broken invariant is a warned
    // de-optimisation and not a crash. What makes these refusals load-bearing is that the
    // emitter suite asserts each of them; the throw is how the assertion has something to
    // catch, not a runtime guarantee.
    if (dense) {
      throw new IllegalArgumentException(
          "a NULL-mode overflow mask reached a dense body, which has no word to narrow: " + node);
    }
    // Not ANDed with the epilogue mask, unlike the FAIL arm's collect: a tail lane above the
    // row count can test as overflowing (a literal operand is broadcast to every lane while a
    // column's is zero-filled by the masked load), and clearing its validity bit is harmless
    // because no consumer reads a bit past the row count - the store is masked too. The FAIL
    // arm cannot be so relaxed: its mask leaves the lane group in the accumulator and would
    // condemn the whole batch from a row that does not exist.
    cb.invokevirtual(VECTOR_MASK, "toLong", TO_LONG);
    cb.loadConstant(-1L);
    cb.lxor();
    loadWord(cb, s, s.wordRef.get(node));
    cb.land();
    storeWord(cb, s, s.wordRef.get(node));
  }

  /** {@code lstore(own, ref(a) & ref(b))} - the null-intolerant word rule. */
  static void emitAndWord(CodeBuilder cb, Slots s, int own, int a, int b) {
    loadWord(cb, s, a);
    loadWord(cb, s, b);
    cb.land();
    storeWord(cb, s, own);
  }

  /**
   * The shape shared by {@code AddDays}, {@code SubDays} and {@code DateDiff}: two children,
   * one lanewise binary op, and - in the masked body, when the node needs its own word - the
   * null-intolerant AND-of-validity-words rule ({@link #emitAndWord}). Factored so the AND
   * cannot be dropped on one arm and not another the way it was once, silently, before a
   * dedicated test caught it.
   */
  private static void emitAndValidatedOp(CodeBuilder cb, VarkaVectorIR node, VarkaVectorIR left,
      VarkaVectorIR right, String op, MethodTypeDesc desc, boolean dense, Analysis analysis,
      Slots s, Set<VarkaVectorIR> computed) {
    emitValue(cb, left, dense, analysis, s, computed);
    emitValue(cb, right, dense, analysis, s, computed);
    line(cb, analysis, node);
    cb.invokevirtual(analysis.lane.vector, op, desc);
    if (!dense && s.ownWord.contains(node)) {
      emitAndWord(cb, s, s.wordRef.get(node), s.wordRef.get(left), s.wordRef.get(right));
    }
    Integer guardTmp = s.guardTmp.get(node);
    if (guardTmp != null) {
      // The producer guard covers this node's own result, so the word that qualifies it is this
      // node's.
      emitRangeGuard(cb, node, dense ? null : s.wordRef.get(node), guardTmp, dense, analysis, s,
          VarkaChrono.NARROW_MIN_DAYS, VarkaChrono.NARROW_MAX_DAYS);
    }
  }

  /**
   * The runtime range guard on a column-driven producer's own result: lanes outside
   * {@code [lo, hi]} are ORed into {@link Slots#guardAcc}, and {@code emitStatusReturn} turns a
   * non-empty accumulator into {@code STATUS_CHRONO_RANGE}, which the evaluator answers by
   * recomputing the batch on the row engine. The producer guard calls this on the result of a
   * column-offset {@code AddDays} / {@code SubDays} some calendar node reads, with {@code lo} /
   * {@code hi} = {@link VarkaChrono#NARROW_MIN_DAYS} / {@link VarkaChrono#NARROW_MAX_DAYS}; task
   * 60 calls it on {@code AddMonths} ' own month count, with {@code lo} / {@code hi} =
   * {@link VarkaChrono#MONTH_ARITH_MIN_MONTHS} / {@link VarkaChrono#MONTH_ARITH_MAX_MONTHS} - two
   * compares are two compares regardless of what they bound. The guarded value stays on the operand
   * stack for the caller; it is parked in {@code guardTmp} only for the compares.
   *
   * <p>{@code word} is the validity word to AND the out-of-range mask with in a masked body, or
   * null in a dense one. The caller passes it rather than this method looking it up from a node,
   * because the value being guarded and the word that qualifies it are not always the same node's:
   * a producer guard covers a node's own result under that node's own word, while the month-count
   * guard covers an operand under the {@code AddMonths} node's word. Resolving it here from one
   * node reference made those two cases indistinguishable, and reading the word slot before the arm
   * that fills it is what produced this task's VerifyError; making the caller state it keeps the
   * two facts together at the site that knows both.
   *
   * <p>This is the old per-extraction guard block, retargeted from the extraction's input to the
   * producer's output - so it runs once per distinct producer rather than once per calendar node
   * reading it, and not at all for the shapes the compiler bounds. The set is keyed on the guarded
   * node, not on the operand it checks, so two {@code AddMonths} over one count column each emit
   * their own guard over that column - redundant, not wrong, and the price of keying on the node
   * that owns the validity word the guard has to AND with. Two ANDs carry over unchanged and for
   * the same reasons: with the node's validity word in the masked body, because a null row's lanes
   * are undefined and must not condemn the batch (the node's word is the AND of every input, so a
   * null offset or a null count is covered), and with the epilogue's bounds mask, because a partial
   * group's padding lanes hold whatever the masked load left. The dense body skips the word AND:
   * every lane is valid there. A producer used more than once is emitted once per lane group under
   * CSE, guard included; with CSE off it is re-emitted per use, which repeats the guard - correct,
   * merely redundant, and not a shape production emits.
   */
  static void emitRangeGuard(CodeBuilder cb, VarkaVectorIR node, Integer word,
      int guardTmp, boolean dense, Analysis analysis, Slots s, long lo, long hi) {
    cb.dup();
    cb.astore(guardTmp);
    cb.aload(guardTmp);
    cb.getstatic(VECTOR_OPERATORS, "LT", VO_COMPARISON);
    analysis.lane.pushScalar(cb, lo);
    cb.invokevirtual(analysis.lane.vector, "compare", analysis.lane.compareVI);
    cb.aload(guardTmp);
    cb.getstatic(VECTOR_OPERATORS, "GT", VO_COMPARISON);
    analysis.lane.pushScalar(cb, hi);
    cb.invokevirtual(analysis.lane.vector, "compare", analysis.lane.compareVI);
    cb.invokevirtual(VECTOR_MASK, "or", MASK_BINARY);
    emitGuardCollect(cb, node, word, dense, analysis, s);
  }

  /**
   * Consumes a {@code VectorMask} of lanes that condemn the batch and folds it into the body's
   * accumulator: ANDed with {@code word}, the lanes' validity, in a masked body (a null lane is not
   * out of range; {@code null} or the all-true constant skips the AND), ANDed with the epilogue's
   * bounds mask when there is one, ORed into {@code guardAcc}. The tail of the producer guard,
   * shared with the self-guarding nodes.
   *
   * <p>It is also ANDed with the enclosing {@code IfElse} arms' condition, where {@code node}'s
   * uses all sit under the same chain of them ({@link #emitArmContext}). A vector body computes
   * both arms and a guard condemns rather than producing a value the blend can discard, so without
   * that a lane the condition sends to the other arm declines the batch it is in. The batch fell
   * back and the answers stayed right; the fusion was what was lost.
   */
  static void emitGuardCollect(CodeBuilder cb, VarkaVectorIR node, Integer word,
      boolean dense, Analysis analysis, Slots s) {
    // WORD_DEAD is deliberately not screened here beside the all-true constant. A guarded node
    // whose word the liveness pass killed is a bug in that pass, not a case to emit around: the AND
    // is what keeps a null lane from condemning the batch, so skipping it quietly would turn a
    // liveness error into spurious batch declines on nullable data. `loadWord` refuses instead, and
    // that refusal is what {@code misdescribeWordLiveness} arms in the "loaded" direction. What
    // makes the case unreachable is {@link #guardedWord}, which the liveness pass reads and which
    // contains {@link #guardScratch}, the slot planner's predicate, by construction - so the
    // planner cannot decide this node is guarded while the liveness pass decides its word is dead.
    // It was one predicate, but its two readers ask different questions; the containment is what
    // preserves the property.
    if (!dense && word != null && word != WORD_ALL_TRUE) {
      cb.aload(s.species);
      loadWord(cb, s, word);
      cb.invokestatic(VECTOR_MASK, "fromLong", FROM_LONG);
      cb.invokevirtual(VECTOR_MASK, "and", MASK_BINARY);
    }
    emitArmContext(cb, node, dense, analysis, s);
    if (s.epilogueMask != null) {
      cb.aload(s.epilogueMask);
      cb.invokevirtual(VECTOR_MASK, "and", MASK_BINARY);
    }
    cb.aload(s.guardAcc);
    cb.invokevirtual(VECTOR_MASK, "or", MASK_BINARY);
    cb.astore(s.guardAcc);
  }

  /**
   * ANDs the condemning mask on the stack with the arms {@code node} sits under, where its uses
   * agree on one chain of them. Nothing is emitted for the empty chain, which is what every shape
   * had before this task and what {@link VarkaEmitOptions#guardUnderArm} off restores.
   *
   * <p>Polarity follows SQL's {@code CASE}, in which an <em>unknown</em> condition falls to
   * {@code ELSE}: the then arm's mask is the condition's known-true set and the else arm's is
   * its complement - known-false <em>plus unknown</em> - never the known-false word. Taking
   * {@code kF} there would drop the unknown-condition lanes from the else arm's guard and stop
   * it condemning a batch it must condemn.
   *
   * <p>The condition is read from the slot it already owns, per body: a word in the masked body
   * ({@code kt}, complemented by XOR), a {@code VectorMask} in the dense one ({@code condMask},
   * complemented by {@code not()}). Both are per-condition-node maps, so a nested arm reads its own
   * and cannot clobber the enclosing one, and both are set by {@code emitCond} before either arm's
   * values are emitted. Neither goes through {@link #loadWord}, so the liveness bookkeeping is
   * untouched.
   */
  private static void emitArmContext(CodeBuilder cb, VarkaVectorIR node, boolean dense,
      Analysis analysis, Slots s) {
    for (ArmStep step : analysis.armChainOf(node)) {
      VarkaVectorIR cond = step.node().cond();
      if (dense) {
        cb.aload(s.condMask.get(cond));
        if (!step.thenBranch()) {
          cb.invokevirtual(VECTOR_MASK, "not", MASK_UNARY);
        }
      } else {
        cb.aload(s.species);
        cb.lload(s.kt.get(cond));
        if (!step.thenBranch()) {
          cb.loadConstant(-1L);
          cb.lxor();
        }
        cb.invokestatic(VECTOR_MASK, "fromLong", FROM_LONG);
      }
      cb.invokevirtual(VECTOR_MASK, "and", MASK_BINARY);
    }
  }

  /**
   * The null-skipping {@code greatest}/{@code least}: in the dense body a plain lanewise
   * {@code max}/{@code min}; in the masked body each operand substitutes the other where it is
   * null - {@code aSel = a.blend(b, ~validA)} - which reduces every case (both valid, only A,
   * only B) to the plain op, and {@code valid = validA | validB}.
   */
  private static void emitPick(CodeBuilder cb, VarkaVectorIR node, VarkaVectorIR left,
      VarkaVectorIR right, String op, boolean dense, Analysis analysis, Slots s,
      Set<VarkaVectorIR> computed) {
    if (dense) {
      emitValue(cb, left, dense, analysis, s, computed);
      emitValue(cb, right, dense, analysis, s, computed);
      line(cb, analysis, node);
      cb.invokevirtual(analysis.lane.vector, op, analysis.lane.lanewiseVV);
      return;
    }
    int[] tmp = s.pairTmp.get(node);
    emitValue(cb, left, dense, analysis, s, computed);
    cb.astore(tmp[0]);
    emitValue(cb, right, dense, analysis, s, computed);
    cb.astore(tmp[1]);
    line(cb, analysis, node);
    cb.aload(tmp[0]);
    cb.aload(tmp[1]);
    cb.aload(s.species);
    loadWord(cb, s, s.wordRef.get(left));
    cb.loadConstant(-1L);
    cb.lxor();
    cb.invokestatic(VECTOR_MASK, "fromLong", FROM_LONG);
    cb.invokevirtual(analysis.lane.vector, "blend", analysis.lane.blend);
    cb.aload(tmp[1]);
    cb.aload(tmp[0]);
    cb.aload(s.species);
    loadWord(cb, s, s.wordRef.get(right));
    cb.loadConstant(-1L);
    cb.lxor();
    cb.invokestatic(VECTOR_MASK, "fromLong", FROM_LONG);
    cb.invokevirtual(analysis.lane.vector, "blend", analysis.lane.blend);
    cb.invokevirtual(analysis.lane.vector, op, analysis.lane.lanewiseVV);
    if (s.ownWord.contains(node)) {
      loadWord(cb, s, s.wordRef.get(left));
      loadWord(cb, s, s.wordRef.get(right));
      cb.lor();
      storeWord(cb, s, s.wordRef.get(node));
    }
  }

  /** {@code [v] -> [v shifted]} by a constant, for either shift direction. */
  static void emitShift(CodeBuilder cb, String op, int bits) {
    cb.getstatic(VECTOR_OPERATORS, op, VO_BINARY);
    cb.loadConstant(bits);
    cb.invokevirtual(INT_VECTOR, "lanewise", LANEWISE_BINARY_I);
  }


  /**
   * Emits a condition node: in the dense body a single {@code VectorMask} local (every input
   * lane is valid, so known-true is the comparison itself and known-false its complement); in
   * the masked body the known-true / known-false word pair of plan 2.6.
   */
  static void emitCond(CodeBuilder cb, Cond node, boolean dense, Analysis analysis,
      Slots s, Set<VarkaVectorIR> computed) {
    if (computed.contains(node)) {
      return;
    }
    computed.add(node);
    switch (node) {
      case Compare n -> {
        emitValue(cb, n.left(), dense, analysis, s, computed);
        cb.getstatic(VECTOR_OPERATORS, n.op().name(), VO_COMPARISON);
        emitValue(cb, n.right(), dense, analysis, s, computed);
        line(cb, analysis, node);
        cb.invokevirtual(analysis.lane.vector, "compare", analysis.lane.compareVV);
        if (dense) {
          cb.astore(s.condMask.get(node));
        } else {
          cb.invokevirtual(VECTOR_MASK, "toLong", TO_LONG);
          cb.lstore(s.cmpTmp);
          // kT = cmp & validL & validR; kF = ~cmp & validL & validR.
          cb.lload(s.cmpTmp);
          loadWord(cb, s, s.wordRef.get(n.left()));
          cb.land();
          loadWord(cb, s, s.wordRef.get(n.right()));
          cb.land();
          cb.lstore(s.kt.get(node));
          cb.lload(s.cmpTmp);
          cb.loadConstant(-1L);
          cb.lxor();
          loadWord(cb, s, s.wordRef.get(n.left()));
          cb.land();
          loadWord(cb, s, s.wordRef.get(n.right()));
          cb.land();
          cb.lstore(s.kf.get(node));
        }
      }
      case And n -> {
        emitCond(cb, n.left(), dense, analysis, s, computed);
        emitCond(cb, n.right(), dense, analysis, s, computed);
        line(cb, analysis, node);
        if (dense) {
          cb.aload(s.condMask.get(n.left()));
          cb.aload(s.condMask.get(n.right()));
          cb.invokevirtual(VECTOR_MASK, "and", MASK_BINARY);
          cb.astore(s.condMask.get(node));
        } else {
          cb.lload(s.kt.get(n.left()));
          cb.lload(s.kt.get(n.right()));
          cb.land();
          cb.lstore(s.kt.get(node));
          cb.lload(s.kf.get(n.left()));
          cb.lload(s.kf.get(n.right()));
          cb.lor();
          cb.lstore(s.kf.get(node));
        }
      }
      case Or n -> {
        emitCond(cb, n.left(), dense, analysis, s, computed);
        emitCond(cb, n.right(), dense, analysis, s, computed);
        line(cb, analysis, node);
        if (dense) {
          cb.aload(s.condMask.get(n.left()));
          cb.aload(s.condMask.get(n.right()));
          cb.invokevirtual(VECTOR_MASK, "or", MASK_BINARY);
          cb.astore(s.condMask.get(node));
        } else {
          cb.lload(s.kt.get(n.left()));
          cb.lload(s.kt.get(n.right()));
          cb.lor();
          cb.lstore(s.kt.get(node));
          cb.lload(s.kf.get(n.left()));
          cb.lload(s.kf.get(n.right()));
          cb.land();
          cb.lstore(s.kf.get(node));
        }
      }
      case Not n -> {
        emitCond(cb, n.child(), dense, analysis, s, computed);
        line(cb, analysis, node);
        if (dense) {
          cb.aload(s.condMask.get(n.child()));
          cb.invokevirtual(VECTOR_MASK, "not", MASK_UNARY);
          cb.astore(s.condMask.get(node));
        }
        // Masked: kT/kF are the child's, swapped - pure slot aliasing, planned, no code.
      }
      case IsNotNull n -> {
        line(cb, analysis, node);
        if (dense) {
          // The dense body ran because every referenced input is null-free, so the
          // predicate is constant true.
          cb.aload(s.species);
          cb.loadConstant(-1L);
          cb.invokestatic(VECTOR_MASK, "fromLong", FROM_LONG);
          cb.astore(s.condMask.get(node));
        } else {
          // kT = word(child); kF = ~word(child) - total: both masks cover every lane. The
          // ~ also inverts a word's undefined bits above `lanes`; that is safe because
          // every consumer truncates (`fromLong` reads species-length bits,
          // `orValidityBitsAt` applies its lane mask) - the same invariant IfElse's ~kT
          // already relies on.
          loadWord(cb, s, s.wordRef.get(n.child()));
          cb.lstore(s.kt.get(node));
          loadWord(cb, s, s.wordRef.get(n.child()));
          cb.loadConstant(-1L);
          cb.lxor();
          cb.lstore(s.kf.get(node));
        }
      }
    }
  }
}
