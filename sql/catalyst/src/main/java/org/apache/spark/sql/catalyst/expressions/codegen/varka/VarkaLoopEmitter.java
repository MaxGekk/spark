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

// Only the four Class-File API imports that predate task 13 appear here: importing several
// others (CustomAttribute, AttributedElement, ClassElement...) makes scalac - and so every
// scaladoc pass over the module - fail with an "illegal cyclic reference" while completing
// the API's sealed hierarchy. Task-13 additions use fully-qualified names inside method
// bodies instead, which scalac's Java parser never reads; see VarkaDebugInfo's class doc.
import java.lang.classfile.ClassBuilder;
import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.classfile.Label;
import java.lang.constant.ClassDesc;
import java.lang.constant.ConstantDescs;
import java.lang.constant.MethodTypeDesc;
import java.lang.reflect.AccessFlag;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.AddDays;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.And;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Chrono;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Compare;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Cond;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DateDiff;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfMonth;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfWeek;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfYear;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Greatest;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IfElse;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IsNotNull;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Least;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Month;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Not;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Or;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Quarter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.SubDays;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.WeekDay;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Year;

/**
 * Emits a fused vector loop for a {@link VarkaVectorIR} DAG with the Class-File API
 * (milestone 2, tasks 9-11): a class implementing {@link VarkaFusedKernel} whose {@code run}
 * is the loop itself - loads, the op DAG on the operand stack, one store per output -
 * mirroring the hand-written {@code DateVectorOps} kernels' six-step shape, generalized. The
 * kernels remain the reference semantics for the arithmetic ops; this class exists so a whole
 * projection - predication included - runs in one pass with its intermediates in vector
 * registers.
 *
 * <p><b>Method layout</b> (task 10's twin bodies, split further in task 11): {@code run}
 * dispatches per batch on one loop-invariant test - are all referenced inputs null-free? - to
 * a dense or masked <i>driver</i>, which zeroes the output validity, takes the all-null
 * shortcut, then calls one sibling <i>loop</i> method per output group (at most
 * {@link #GROUP_BUDGET} ops each; see that constant for the measured reason) and finally the
 * sibling <i>epilogue</i> method. The dense side runs with no validity bookkeeping at all, which
 * task 11's invariant keeps sound: every node maps valid inputs to valid outputs (there is no
 * null-literal node), so null-free in means all-valid out. Separate methods, not one big one:
 * each gets its own C2 compilation, so no method's node and inlining budgets can starve
 * another's intrinsics.
 *
 * <p><b>Unmasked compute</b> (task 11, plan 2.4): both bodies run unmasked loads, lanewise ops
 * and stores. Inside {@code loopBound} every access is in bounds, an all-null column still has
 * an allocated data buffer, and the engine contract declares invalid destination lanes
 * undefined - so masks carry no correctness inside the loop, and task 10 measured masked ops
 * at 2.3x-2.9x slower even with an all-true mask. Truth lives in the <i>validity words</i>:
 * per lane group each referenced input contributes one long ({@code 0L} all-null, {@code -1L}
 * null-free, {@code validityBitsAt} otherwise), and each node's validity is computed from its
 * children's words by the task-11 mask algebra - AND for the null-intolerant ops, OR for
 * {@code greatest}/{@code least}, a word blend for {@code IfElse}. A {@code VectorMask} is
 * materialized only where a blend semantically needs one.
 *
 * <p><b>Conditions</b> (task 11, plan 2.6): a {@link Cond} node evaluates to a known-true and
 * a known-false word pair - three-valued logic, where an unknown lane (a null below the
 * comparison) is neither, and {@code IfElse} takes its ELSE branch there. In the dense body
 * every input lane is valid, so the pair degenerates to the comparison mask itself and the
 * connectives run in mask space. {@code IfElse} validity is
 * {@code (kT & validThen) | (~kT & validElse)}: the chosen branch's validity, lane-wise,
 * nothing ANDed globally.
 *
 * <p>{@code dayofweek}/{@code weekday} lower to a full-range mod-7 by base-8 digit sum
 * (pre-measured in PLAN_TASK_11.md: 8x the lanewise-DIV variant, which x86 scalarizes): fold
 * 15-, 6- and 3-bit chunks ({@code 2^(3k) = 1 mod 7}), correct by {@code +3} where the input
 * is negative ({@code 2^32 = 4 mod 7}), one compare-subtract fixup, then the constant offset
 * applied after the mod so it cannot overflow.
 *
 * <p><b>Selection outputs</b> (task 21): a {@link Cond} may itself be an output root, and such
 * an output is a <i>selection bitmap</i> rather than a column - the root's known-true word
 * OR-ed into {@code dstValidity} exactly where a value root ORs its validity word, with the
 * {@code dstData} slot unused (callers pass {@code 0L}; the body never materializes it). The
 * bitmap's semantics are SQL's {@code WHERE}: a set bit means known true, so an unknown lane
 * (a null below the comparison) reads as false - free by construction, because {@code kT} is
 * a subset of the operands' validity. This is the filter kernel: one Cond root per predicate,
 * no value outputs beside it in milestone 3.
 *
 * <p><b>The epilogue, not a scalar tail</b> (task 24): the rows past {@code loopBound} are one
 * more iteration of the same lane-group body, under the mask {@code indexInRange} builds for a
 * partial group - {@code i} is {@code loopBound}, {@code lanes} becomes the remainder so every
 * validity helper stays bounded by the group, and only the loads and the stores take their
 * masked overloads. The masked load is required rather than preferred: the data segment is
 * sized to {@code length * 4}, so an unmasked load of the last partial group would run off its
 * end. This replaced a per-row topological pass that lowered every node type a second time
 * into int locals - a complete second walk of the IR, and the half that would have had to grow
 * with every node type added after this.
 *
 * <p><b>Inactive lanes read {@code 0}, so no operation in the walk may trap on {@code 0}.</b>
 * That is the invariant the epilogue rests on, and today it holds for free: the mod-7
 * lowerings divide by the constant 7, and add, sub, compare, blend, max, min and the shifts
 * are total. The first trapping operation to enter the IR - ANSI division above all - has to
 * blend a safe value into the inactive lanes or use a masked lanewise form, because the
 * epilogue computes them and only declines to store them.
 *
 * <p>Every call the loop makes is declared once in the descriptor table below - erasure is
 * this milestone's named risk ({@code IntVector.add}, {@code compare}, {@code blend},
 * {@code max} all take the <i>erased</i> {@code Vector}), and a wrong descriptor must be found
 * by pointing at one line, not by disassembling the output.
 *
 * <p>Out-of-shape IR - unknown lane types, a condition in a value position, out-of-range
 * ordinals or slots, non-literal day offsets, trees past {@link #MAX_CHAIN_DEPTH} or
 * {@link #MAX_FUSED_NODES} - is rejected with {@link IllegalArgumentException}, which the
 * evaluator wiring treats as "fall back".
 *
 * <p><b>Telemetry</b> (task 13): every emitted class carries a {@code SourceFile} attribute -
 * the caller-supplied name, meant to identify the operator and stage
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
   * The most distinct op nodes one emitted kernel may hold, across all outputs after CSE
   * (task 10). Depth alone no longer bounds method size once outputs multiply, so this is the
   * total-size counterpart of {@link #MAX_CHAIN_DEPTH}: a policy bound far past any real
   * projection, kept honest by the widest-shape case in the parity benchmark. Since task 11
   * the ops are spread over loop methods of at most {@link #GROUP_BUDGET} ops each, so this
   * caps the kernel, not any one compiled method.
   */
  public static final int MAX_FUSED_NODES = 64;

  /**
   * The most op nodes one emitted <i>loop method</i> carries; outputs are partitioned into
   * sibling loop methods within this budget (task 11). Measured reason: each Vector API call
   * site expands into a large intrinsic graph, so C2's compile time grows steeply with op
   * count - the tier-4 compile of a single 64-op loop took ~10 seconds, during which the
   * loop ran the C1 version with boxed vectors at ~1% speed ({@code -XX:+PrintCompilation}
   * shows the OSR task pending; whether a run sees the cliff depends only on when that
   * compile lands relative to it). A 16-op loop method compiles promptly under every load
   * tried, so every hot loop stays at or under it by construction. Grouping is greedy over
   * the output order and counts only nodes new to the group, so outputs sharing subtrees
   * tend to land together and keep their cross-output CSE; a single output wider than the
   * budget gets its own group untouched - splitting inside an output would forfeit the
   * register residency that is the point, and single-output loops measured healthy at every
   * width tried (59 ops: 80% of peak within 400 ms, throughput proportional to op count) -
   * the slow compiles were specific to multi-output loops. Numbers in PLAN_TASK_11.md
   * section 6.
   *
   * <p>Task 17 priced the one candidate the debt register left open - raising the budget so
   * two outputs sharing a deep chain keep their cross-output CSE in one method - and closed
   * it against the change: on 20 distinct ops split across two outputs, the shipped 16 runs
   * 4471.9 M rows/s (two loop methods, the shared chain recomputed per lane group) against
   * 3110.5 M at 24 (one method, CSE kept) - the committed parity file, requoted whenever it
   * is regenerated, which task 26 had to learn twice. Recomputing eight ops in registers is
   * cheaper than the wider method's register pressure, the same effect that made sibling methods
   * the rule in the first place. The parity benchmark keeps both cases so a future retune is
   * measured rather than argued.
   */
  public static final int GROUP_BUDGET = 16;

  /**
   * The most input columns one emitted loop may read. A node's referenced-column set is a long
   * bitset, which fixes the representation limit at 64; real projections reference a handful.
   */
  public static final int MAX_INPUTS = 64;

  /**
   * What a calendar node weighs against {@link #GROUP_BUDGET}: the vector ops
   * {@link #emitChrono} emits for one, counted and rounded to the nearest ten. It only has to
   * exceed the budget for each calendar output to get its own loop method; the real figure is
   * used rather than a flag so that a future node of intermediate width sorts sensibly beside
   * it, which is the only reason the exact value matters - re-count it if the lowering
   * changes shape rather than leaving it to drift. Covers {@code Year}/{@code Month}/
   * {@code DayOfMonth}/{@code Quarter} (task 26), the shared prefix (~40 ops) plus each
   * field's own 3-6 op tail; {@code DayOfYear} is heavier and weighs
   * {@link #DAY_OF_YEAR_WEIGHT} instead.
   */
  private static final int CHRONO_WEIGHT = 50;

  /**
   * What {@code DayOfYear} (task 34) weighs against {@link #GROUP_BUDGET}, counted the same
   * way as {@link #CHRONO_WEIGHT}: the shared ~40-op prefix, plus {@code emitYearValue} (6),
   * plus {@code emitLeapFlag} (19: the by-4 test, two {@code emitDivisibleBy} calls at 7 each,
   * a {@code not} and the final combine), plus the January-based blend (5) - about 70 total,
   * counted by reading every emitted instruction rather than estimated. Both this and
   * {@link #CHRONO_WEIGHT} already exceed {@link #GROUP_BUDGET}, so the exact value does not
   * change today's grouping decision (a lone {@code DayOfYear} output already forms its own
   * loop method either way) - but 70 real ops in one single-output method is past the 59-op
   * width this file's own single-output measurements call healthy and close to the 64-op loop
   * task 26 measured triggering a ~10 second tier-4 compile stall. Whether a lone
   * {@code SELECT dayofyear(d)} actually reaches that stall is exactly task 43's question
   * ("what bounds a loop method inside one output"), not re-measured here - this weight only
   * keeps the accounting honest until task 43 answers it.
   */
  private static final int DAY_OF_YEAR_WEIGHT = 70;

  private VarkaLoopEmitter() {
  }

  // ---------------------------------------------------------------------------------------------
  // Descriptor table: the single source of truth for everything the emitted code calls.
  // ---------------------------------------------------------------------------------------------

  private static final ClassDesc MEMORY_SEGMENT =
      ClassDesc.of("java.lang.foreign.MemorySegment");
  private static final ClassDesc BYTE_ORDER = ClassDesc.of("java.nio.ByteOrder");
  private static final ClassDesc INT_VECTOR = ClassDesc.of("jdk.incubator.vector.IntVector");
  private static final ClassDesc VECTOR = ClassDesc.of("jdk.incubator.vector.Vector");
  private static final ClassDesc VECTOR_MASK = ClassDesc.of("jdk.incubator.vector.VectorMask");
  private static final ClassDesc VECTOR_SPECIES =
      ClassDesc.of("jdk.incubator.vector.VectorSpecies");
  private static final ClassDesc VECTOR_OPERATORS =
      ClassDesc.of("jdk.incubator.vector.VectorOperators");
  private static final ClassDesc VO_COMPARISON =
      ClassDesc.ofDescriptor("Ljdk/incubator/vector/VectorOperators$Comparison;");
  private static final ClassDesc VO_BINARY =
      ClassDesc.ofDescriptor("Ljdk/incubator/vector/VectorOperators$Binary;");
  private static final ClassDesc SUPPORT =
      ClassDesc.of("org.apache.spark.sql.varka.vector.VarkaVectorSupport");
  private static final ClassDesc FUSED_KERNEL = ClassDesc.of(VarkaFusedKernel.class.getName());

  private static final ClassDesc LONG_ARRAY = ConstantDescs.CD_long.arrayType();
  private static final ClassDesc INT_ARRAY = ConstantDescs.CD_int.arrayType();

  /**
   * {@code int run(long[], long[], int[], long[], long[], int[], int)} - every body method
   * shares it, so slots line up everywhere and the driver can forward a callee's status
   * without repacking. The int is the batch status; see {@link VarkaFusedKernel#run}.
   */
  private static final MethodTypeDesc RUN = MethodTypeDesc.of(ConstantDescs.CD_int,
      LONG_ARRAY, LONG_ARRAY, INT_ARRAY, LONG_ARRAY, LONG_ARRAY, INT_ARRAY,
      ConstantDescs.CD_int);
  private static final MethodTypeDesc INIT = MethodTypeDesc.of(ConstantDescs.CD_void);

  /** {@code MemorySegment VarkaVectorSupport.ofAddress(long, long)}. */
  private static final MethodTypeDesc OF_ADDRESS =
      MethodTypeDesc.of(MEMORY_SEGMENT, ConstantDescs.CD_long, ConstantDescs.CD_long);
  /** {@code void VarkaVectorSupport.zero(MemorySegment)}. */
  private static final MethodTypeDesc ZERO =
      MethodTypeDesc.of(ConstantDescs.CD_void, MEMORY_SEGMENT);
  /** {@code long VarkaVectorSupport.validityBitsAt(MemorySegment, long, int)}. */
  private static final MethodTypeDesc VALIDITY_BITS_AT = MethodTypeDesc.of(
      ConstantDescs.CD_long, MEMORY_SEGMENT, ConstantDescs.CD_long, ConstantDescs.CD_int);
  /** {@code void VarkaVectorSupport.orValidityBitsAt(MemorySegment, long, long, int)}. */
  private static final MethodTypeDesc OR_VALIDITY_BITS_AT = MethodTypeDesc.of(
      ConstantDescs.CD_void, MEMORY_SEGMENT, ConstantDescs.CD_long, ConstantDescs.CD_long,
      ConstantDescs.CD_int);

  /** {@code int VectorSpecies.length()} / {@code int VectorSpecies.loopBound(int)}. */
  private static final MethodTypeDesc SPECIES_LENGTH = MethodTypeDesc.of(ConstantDescs.CD_int);
  private static final MethodTypeDesc LOOP_BOUND =
      MethodTypeDesc.of(ConstantDescs.CD_int, ConstantDescs.CD_int);
  /**
   * {@code VectorMask VectorSpecies.indexInRange(int, int)} - the partial lane group's mask,
   * and the whole reason the epilogue can replace a scalar walk (task 24).
   */
  private static final MethodTypeDesc INDEX_IN_RANGE =
      MethodTypeDesc.of(VECTOR_MASK, ConstantDescs.CD_int, ConstantDescs.CD_int);
  /** {@code IntVector IntVector.broadcast(VectorSpecies, int)} (static). */
  private static final MethodTypeDesc BROADCAST =
      MethodTypeDesc.of(INT_VECTOR, VECTOR_SPECIES, ConstantDescs.CD_int);
  /** {@code VectorMask VectorMask.fromLong(VectorSpecies, long)} (static). */
  private static final MethodTypeDesc FROM_LONG =
      MethodTypeDesc.of(VECTOR_MASK, VECTOR_SPECIES, ConstantDescs.CD_long);
  /** {@code long VectorMask.toLong()}. */
  private static final MethodTypeDesc TO_LONG = MethodTypeDesc.of(ConstantDescs.CD_long);
  /**
   * {@code IntVector.fromMemorySegment(VectorSpecies, MemorySegment, long, ByteOrder)}
   * (static, unmasked - see the class doc; every load is inside {@code loopBound}).
   */
  private static final MethodTypeDesc FROM_MEMORY_SEGMENT_DENSE = MethodTypeDesc.of(INT_VECTOR,
      VECTOR_SPECIES, MEMORY_SEGMENT, ConstantDescs.CD_long, BYTE_ORDER);
  /**
   * The same load with a mask (task 24): the epilogue's only reason to differ from the loop.
   * Lanes outside the mask are neither read nor faulted on, which is what lets one masked
   * iteration cover a partial lane group whose data segment ends at {@code length * 4}.
   */
  private static final MethodTypeDesc FROM_MEMORY_SEGMENT_MASKED = MethodTypeDesc.of(INT_VECTOR,
      VECTOR_SPECIES, MEMORY_SEGMENT, ConstantDescs.CD_long, BYTE_ORDER, VECTOR_MASK);
  /**
   * {@code IntVector IntVector.add/sub/max/min(Vector)} - the parameter is the *erased*
   * {@code Vector}, not {@code IntVector}; the covariant return stays {@code IntVector}.
   */
  private static final MethodTypeDesc LANEWISE_VV =
      MethodTypeDesc.of(INT_VECTOR, VECTOR);
  /** The deliberately wrong shape behind {@link VarkaEmitOptions#misdescribeAdd()}. */
  private static final MethodTypeDesc LANEWISE_VV_WRONG =
      MethodTypeDesc.of(INT_VECTOR, INT_VECTOR);
  /** {@code IntVector IntVector.add/sub/and/mul/div(int)} - broadcast-scalar convenience. */
  private static final MethodTypeDesc LANEWISE_VI =
      MethodTypeDesc.of(INT_VECTOR, ConstantDescs.CD_int);
  /** {@code IntVector IntVector.add/sub(int, VectorMask)}. */
  private static final MethodTypeDesc LANEWISE_VI_MASKED =
      MethodTypeDesc.of(INT_VECTOR, ConstantDescs.CD_int, VECTOR_MASK);
  /** {@code IntVector IntVector.lanewise(VectorOperators.Binary, int)} - the shifts. */
  private static final MethodTypeDesc LANEWISE_BINARY_I =
      MethodTypeDesc.of(INT_VECTOR, VO_BINARY, ConstantDescs.CD_int);
  /** {@code VectorMask IntVector.compare(VectorOperators.Comparison, Vector)} - erased. */
  private static final MethodTypeDesc COMPARE_VV =
      MethodTypeDesc.of(VECTOR_MASK, VO_COMPARISON, VECTOR);
  /** {@code VectorMask IntVector.compare(VectorOperators.Comparison, int)}. */
  private static final MethodTypeDesc COMPARE_VI =
      MethodTypeDesc.of(VECTOR_MASK, VO_COMPARISON, ConstantDescs.CD_int);
  /** {@code IntVector IntVector.blend(Vector, VectorMask)} - erased {@code Vector}. */
  private static final MethodTypeDesc BLEND =
      MethodTypeDesc.of(INT_VECTOR, VECTOR, VECTOR_MASK);
  /** {@code VectorMask VectorMask.and/or(VectorMask)} and {@code VectorMask.not()}. */
  private static final MethodTypeDesc MASK_BINARY = MethodTypeDesc.of(VECTOR_MASK, VECTOR_MASK);
  private static final MethodTypeDesc ANY_TRUE = MethodTypeDesc.of(ConstantDescs.CD_boolean);
  private static final MethodTypeDesc MASK_UNARY = MethodTypeDesc.of(VECTOR_MASK);
  /** {@code void IntVector.intoMemorySegment(MemorySegment, long, ByteOrder)} - unmasked. */
  private static final MethodTypeDesc INTO_MEMORY_SEGMENT_DENSE = MethodTypeDesc.of(
      ConstantDescs.CD_void, MEMORY_SEGMENT, ConstantDescs.CD_long, BYTE_ORDER);
  /** {@code void IntVector.intoMemorySegment(MemorySegment, long, ByteOrder, VectorMask)}. */
  private static final MethodTypeDesc INTO_MEMORY_SEGMENT_MASKED = MethodTypeDesc.of(
      ConstantDescs.CD_void, MEMORY_SEGMENT, ConstantDescs.CD_long, BYTE_ORDER, VECTOR_MASK);

  // Parameter slots of `run` (instance method: `this` is slot 0, finding 11's lesson).
  private static final int P_SRC_DATA = 1;
  private static final int P_SRC_VALIDITY = 2;
  private static final int P_NULL_COUNT = 3;
  private static final int P_DST_DATA = 4;
  private static final int P_DST_VALIDITY = 5;
  private static final int P_SCALAR_ARGS = 6;
  private static final int P_LENGTH = 7;

  // The word-reference value meaning "constant all-true" (a literal-only subtree).
  private static final int WORD_ALL_TRUE = -1;

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
    Analysis analysis = new Analysis(numInputs, numLiterals, options);
    for (VarkaVectorIR root : outputs) {
      analysis.analyzeRoot(root);
    }

    // Method layout, all sharing the seven-parameter shape so slots line up everywhere:
    // `run` dispatches per batch to a dense or masked *driver*; the driver zeroes the output
    // validity, takes the all-null shortcut, then calls one sibling *loop* method per output
    // group (each at most GROUP_BUDGET ops - see that constant for the measured reason) and
    // finally the sibling *epilogue* method. Separate methods, not one big one: each gets its own
    // C2 compilation, so no method's node and inlining budgets can starve another's
    // intrinsics (task 10 measured 3x to 4x on exactly that).
    ClassDesc classDesc = ClassDesc.of(className);
    boolean anyColumns = analysis.referencedColumns != 0;
    List<List<Integer>> groups = groupOutputs(outputs, options.groupBudget());
    String source = sourceFile != null
        ? sourceFile : className.substring(className.lastIndexOf('.') + 1) + ".java";
    VarkaDebugInfo debugInfo = new VarkaDebugInfo(
        "outputs=" + renderOutputs(outputs) + ", numInputs=" + numInputs
            + ", numLiterals=" + numLiterals,
        planFragment != null ? planFragment : "",
        renderLineMap(analysis));
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
          .withMethodBody("run", RUN, AccessFlag.PUBLIC.mask(),
              (CodeBuilder cb) -> emitDispatch(cb, classDesc, analysis))
          .withMethodBody("runDense", RUN, AccessFlag.PRIVATE.mask(),
              (CodeBuilder cb) -> emitBody(cb, true, BodyMode.DRIVER, -1, classDesc, outputs,
                  analysis, numLiterals, groups))
          .withMethodBody("epilogueDense", RUN, AccessFlag.PRIVATE.mask(),
              (CodeBuilder cb) -> emitBody(cb, true, BodyMode.EPILOGUE, -1, classDesc, outputs,
                  analysis, numLiterals, groups));
      for (int g = 0; g < groups.size(); g++) {
        final int group = g;
        b.withMethodBody("loopDense" + g, RUN, AccessFlag.PRIVATE.mask(),
            (CodeBuilder cb) -> emitBody(cb, true, BodyMode.LOOP, group, classDesc, outputs,
                analysis, numLiterals, groups));
      }
      if (anyColumns) {
        b.withMethodBody("runMasked", RUN, AccessFlag.PRIVATE.mask(),
            (CodeBuilder cb) -> emitBody(cb, false, BodyMode.DRIVER, -1, classDesc, outputs,
                analysis, numLiterals, groups))
            .withMethodBody("epilogueMasked", RUN, AccessFlag.PRIVATE.mask(),
                (CodeBuilder cb) -> emitBody(cb, false, BodyMode.EPILOGUE, -1, classDesc,
                    outputs, analysis, numLiterals, groups));
        for (int g = 0; g < groups.size(); g++) {
          final int group = g;
          b.withMethodBody("loopMasked" + g, RUN, AccessFlag.PRIVATE.mask(),
              (CodeBuilder cb) -> emitBody(cb, false, BodyMode.LOOP, group, classDesc, outputs,
                  analysis, numLiterals, groups));
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
  private enum BodyMode { DRIVER, LOOP, EPILOGUE }

  /**
   * Partitions the outputs into loop-method groups of at most {@code budget} ops (normally
   * {@link #GROUP_BUDGET}), greedily in output order, counting only ops new to the group so
   * shared subtrees keep their outputs together (and their cross-output CSE). An output wider
   * than the budget on its own still forms a group: splitting inside one output would forfeit
   * the register residency that is the point.
   */
  private static List<List<Integer>> groupOutputs(List<VarkaVectorIR> outputs, int budget) {
    List<List<Integer>> groups = new ArrayList<>();
    List<Integer> current = new ArrayList<>();
    Set<VarkaVectorIR> seen = new HashSet<>();
    int ops = 0;
    for (int o = 0; o < outputs.size(); o++) {
      Set<VarkaVectorIR> withNext = new HashSet<>(seen);
      int marginal = addOps(outputs.get(o), withNext);
      // marginal == 0 means this output adds no node the group does not already have - it
      // is structurally the same tree - so splitting it off cannot reduce the method's op
      // count and only costs it the CSE. That matters once a node can outweigh the budget on
      // its own: after one calendar output `ops` already exceeds it, so without this test
      // `SELECT year(d) AS a, year(d) AS b` would emit the decomposition twice.
      if (!current.isEmpty() && marginal > 0 && ops + marginal > budget) {
        groups.add(current);
        current = new ArrayList<>();
        withNext = new HashSet<>();
        marginal = addOps(outputs.get(o), withNext);
        ops = 0;
      }
      current.add(o);
      seen = withNext;
      ops += marginal;
    }
    groups.add(current);
    return groups;
  }

  /** Adds the subtree's distinct nodes to {@code seen}; returns how many op nodes were new. */
  private static int addOps(VarkaVectorIR node, Set<VarkaVectorIR> seen) {
    if (!seen.add(node)) {
      return 0;
    }
    int count = weightOf(node);
    for (VarkaVectorIR child : childrenOf(node)) {
      count += addOps(child, seen);
    }
    return count;
  }

  /**
   * What one node costs against {@link #GROUP_BUDGET}. Every node has weighed 1 since task 10,
   * because every node was one or two lane ops; task 26's calendar nodes are not - each expands
   * to roughly forty, since a civil-from-days decomposition is mostly division and there is no
   * vector divide. Counting them as 1 would let four calendar outputs share a loop method of
   * ~180 vector ops, which is the compile cliff {@link #GROUP_BUDGET} exists to avoid (see its
   * javadoc: a 64-op loop took a ~10 s tier-4 compile). Weighing them by what they emit puts
   * each in its own sibling method instead, which is the shape the budget's own doc blesses -
   * an output wider than the budget forms its own group, and single-output loops measured
   * healthy at 59 ops.
   *
   * <p>This is deliberately only about <i>grouping</i>. {@link #MAX_FUSED_NODES} still counts
   * nodes, so a projection may fuse as many calendar fields as it likes; they simply do not
   * share a method.
   */
  private static int weightOf(VarkaVectorIR node) {
    if (node instanceof ColumnRef || node instanceof LiteralSlot) {
      return 0;
    }
    if (node instanceof DayOfYear) {
      return DAY_OF_YEAR_WEIGHT;
    }
    return isChrono(node) ? CHRONO_WEIGHT : 1;
  }

  /** Whether {@code node}'s subtree contains a calendar extraction, which is what decides
   * whether a body needs a guard accumulator at all. */
  private static boolean hasChrono(VarkaVectorIR node) {
    if (isChrono(node)) {
      return true;
    }
    for (VarkaVectorIR child : childrenOf(node)) {
      if (hasChrono(child)) {
        return true;
      }
    }
    return false;
  }

  /** Whether {@code node} is one of the civil-from-days extractions. The IR's sealed
   * {@link Chrono} interface is what makes this total: a new extraction joins the family and
   * is weighed and guarded without touching this method. */
  private static boolean isChrono(VarkaVectorIR node) {
    return node instanceof Chrono;
  }

  private static VarkaVectorIR[] childrenOf(VarkaVectorIR node) {
    return switch (node) {
      case ColumnRef c -> new VarkaVectorIR[0];
      case LiteralSlot l -> new VarkaVectorIR[0];
      case AddDays n -> new VarkaVectorIR[] {n.days(), n.offset()};
      case SubDays n -> new VarkaVectorIR[] {n.days(), n.offset()};
      case DateDiff n -> new VarkaVectorIR[] {n.end(), n.start()};
      case DayOfWeek n -> new VarkaVectorIR[] {n.days()};
      case WeekDay n -> new VarkaVectorIR[] {n.days()};
      case Year n -> new VarkaVectorIR[] {n.days()};
      case Month n -> new VarkaVectorIR[] {n.days()};
      case DayOfMonth n -> new VarkaVectorIR[] {n.days()};
      case Quarter n -> new VarkaVectorIR[] {n.days()};
      case DayOfYear n -> new VarkaVectorIR[] {n.days()};
      case Greatest n -> new VarkaVectorIR[] {n.left(), n.right()};
      case Least n -> new VarkaVectorIR[] {n.left(), n.right()};
      case IfElse n -> new VarkaVectorIR[] {n.cond(), n.thenNode(), n.elseNode()};
      case Compare n -> new VarkaVectorIR[] {n.left(), n.right()};
      case And n -> new VarkaVectorIR[] {n.left(), n.right()};
      case Or n -> new VarkaVectorIR[] {n.left(), n.right()};
      case Not n -> new VarkaVectorIR[] {n.child()};
      case IsNotNull n -> new VarkaVectorIR[] {n.child()};
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
   */
  public static boolean fitsBudgets(java.util.List<VarkaVectorIR> outputs, int numInputs) {
    if (numInputs > MAX_INPUTS) {
      return false;
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
   * null-free? - selecting {@code runDense} or {@code runMasked} (plan 2.5 of task 10).
   */
  private static void emitDispatch(CodeBuilder cb, ClassDesc classDesc, Analysis analysis) {
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
    invokeBody(cb, classDesc, "runDense");
    if (anyColumns) {
      cb.labelBinding(masked);
      invokeBody(cb, classDesc, "runMasked");
    }
    // With no referenced columns the masked label is never targeted and must not be bound:
    // unreachable code has no stack frame to compute.
  }

  /** {@code this.<name>(srcData, ..., length)} - all seven parameters forwarded. */
  private static void invokeCall(CodeBuilder cb, ClassDesc classDesc, String name) {
    cb.aload(0);
    cb.aload(P_SRC_DATA);
    cb.aload(P_SRC_VALIDITY);
    cb.aload(P_NULL_COUNT);
    cb.aload(P_DST_DATA);
    cb.aload(P_DST_VALIDITY);
    cb.aload(P_SCALAR_ARGS);
    cb.iload(P_LENGTH);
    cb.invokespecial(classDesc, name, RUN);
  }

  /** {@link #invokeCall} whose status becomes this method's own - a tail call in effect. */
  private static void invokeBody(CodeBuilder cb, ClassDesc classDesc, String name) {
    invokeCall(cb, classDesc, name);
    cb.ireturn();
  }

  // ---------------------------------------------------------------------------------------------
  // Validation and DAG analysis.
  // ---------------------------------------------------------------------------------------------

  /**
   * One walk over the output trees, before any bytecode exists: validates every node, counts
   * uses on structural equality (the DAG view of trees the caller may have built
   * independently), computes per node the referenced-column bitset and its height, collects a
   * post-order (children-first) topological order - the line map's numbering, and the
   * schedule planSlots' validity aliasing depends on - and marks the null-skipping subtrees
   * the all-null shortcut must not reason about.
   */
  private static final class Analysis {
    final int numInputs;
    final int numLiterals;
    /** The emission's options, carried here because Analysis already reaches every body. */
    final VarkaEmitOptions options;
    /** Distinct nodes in first-visit order, with how often each is used. */
    final Map<VarkaVectorIR, Integer> useCount = new LinkedHashMap<>();
    /** Per distinct node, the bitset of input ordinals its subtree references. */
    final Map<VarkaVectorIR, Long> columns = new HashMap<>();
    /** Distinct nodes, children strictly before parents - the line map's numbering and
     * planSlots' schedule: a word reference planned here always sees concrete child
     * references, which the validity aliasing depends on. */
    final List<VarkaVectorIR> topoOrder = new ArrayList<>();
    /**
     * Each distinct node's 1-based position in {@link #topoOrder}, which is the line number
     * the emitted {@code LineNumberTable} attributes its instructions to (task 16). The
     * mapping from those lines back to nodes is recorded in the class's
     * {@link VarkaDebugInfo}, so a stack frame or profile sample naming
     * {@code Varka_Project_Stage3.java:7} resolves to an IR node without a live session.
     */
    final Map<VarkaVectorIR, Integer> lineNumbers = new HashMap<>();
    /** Whether the subtree holds a null-skipping node (IfElse, Greatest, Least). */
    final Map<VarkaVectorIR, Boolean> skipping = new HashMap<>();
    private final Map<VarkaVectorIR, Integer> height = new HashMap<>();
    /** The union of every node's columns: unreferenced inputs get no locals and no state. */
    long referencedColumns = 0L;
    private int opNodes = 0;

    Analysis(int numInputs, int numLiterals, VarkaEmitOptions options) {
      this.numInputs = numInputs;
      this.numLiterals = numLiterals;
      this.options = options;
    }

    void analyzeRoot(VarkaVectorIR root) {
      // A Cond root is legal since task 21: it emits this output's selection bitmap into
      // dstValidity, with the dstData slot unused (see the class doc). Value positions
      // below a root still reject conditions via requireValue.
      analyze(root);
      if (height.get(root) > MAX_CHAIN_DEPTH) {
        throw new IllegalArgumentException(
            "chain deeper than MAX_CHAIN_DEPTH=" + MAX_CHAIN_DEPTH);
      }
    }

    private static void requireValue(VarkaVectorIR node, String position) {
      if (node instanceof Cond) {
        throw new IllegalArgumentException(
            "condition node " + node + " in a value position (" + position + ")");
      }
    }

    private void analyze(VarkaVectorIR node) {
      if (node.laneType() != VarkaVectorIR.LaneType.INT) {
        throw new IllegalArgumentException("unsupported lane type " + node.laneType());
      }
      Integer seen = useCount.get(node);
      if (seen != null) {
        // A repeated node: its subtree is already analyzed, only the use count grows.
        useCount.put(node, seen + 1);
        return;
      }
      useCount.put(node, 1);
      switch (node) {
        case ColumnRef c -> {
          if (c.ordinal() < 0 || c.ordinal() >= numInputs) {
            throw new IllegalArgumentException(
                "column ordinal " + c.ordinal() + " outside [0, " + numInputs + ")");
          }
          long set = 1L << c.ordinal();
          columns.put(node, set);
          height.put(node, 0);
          skipping.put(node, false);
          referencedColumns |= set;
        }
        case LiteralSlot l -> {
          if (l.index() < 0 || l.index() >= numLiterals) {
            throw new IllegalArgumentException(
                "literal slot " + l.index() + " outside [0, " + numLiterals + ")");
          }
          columns.put(node, 0L);
          height.put(node, 0);
          skipping.put(node, false);
        }
        case AddDays n -> {
          requireLiteralOffset(n.offset());
          analyzeOp(node, false, n.days(), n.offset());
        }
        case SubDays n -> {
          requireLiteralOffset(n.offset());
          analyzeOp(node, false, n.days(), n.offset());
        }
        case DateDiff n -> analyzeOp(node, false, n.end(), n.start());
        case DayOfWeek n -> analyzeOp(node, false, n.days());
        case WeekDay n -> analyzeOp(node, false, n.days());
        case Year n -> analyzeOp(node, false, n.days());
        case Month n -> analyzeOp(node, false, n.days());
        case DayOfMonth n -> analyzeOp(node, false, n.days());
        case Quarter n -> analyzeOp(node, false, n.days());
        case DayOfYear n -> analyzeOp(node, false, n.days());
        case Greatest n -> analyzeOp(node, true, n.left(), n.right());
        case Least n -> analyzeOp(node, true, n.left(), n.right());
        case IfElse n -> analyzeOp(node, true, n.cond(), n.thenNode(), n.elseNode());
        case Compare n -> analyzeOp(node, false, n.left(), n.right());
        case And n -> analyzeOp(node, false, n.left(), n.right());
        case Or n -> analyzeOp(node, false, n.left(), n.right());
        case Not n -> analyzeOp(node, false, n.child());
        case IsNotNull n -> {
          // The compiler enforces this too; re-checked here because emitCond reads the
          // child's per-input validity word, which only a column has before any value walk.
          if (!(n.child() instanceof ColumnRef)) {
            throw new IllegalArgumentException(
                "IsNotNull child must be a ColumnRef, got " + n.child());
          }
          // skips = true states the semantics - known output from a null input - though a
          // Cond only reaches a root through IfElse, which already marks skipping.
          analyzeOp(node, true, n.child());
        }
      }
      topoOrder.add(node);
      lineNumbers.put(node, topoOrder.size());
    }

    /**
     * Common op bookkeeping. Value-typed children are checked against condition nodes here;
     * condition-typed children ({@code IfElse.cond}, the connectives') are enforced by the
     * record types themselves.
     */
    private void analyzeOp(VarkaVectorIR node, boolean skips, VarkaVectorIR... children) {
      opNodes++;
      if (opNodes > MAX_FUSED_NODES) {
        throw new IllegalArgumentException(
            "more than MAX_FUSED_NODES=" + MAX_FUSED_NODES + " distinct ops");
      }
      long set = 0L;
      int maxChildHeight = 0;
      boolean childSkips = false;
      for (VarkaVectorIR child : children) {
        // Value children of value ops and of Compare must not be conditions; the ops whose
        // condition children are legal carry them in Cond-typed record fields already.
        if (child instanceof Cond && !(node instanceof IfElse) && !(node instanceof And)
            && !(node instanceof Or) && !(node instanceof Not)) {
          requireValue(child, "child of " + node.getClass().getSimpleName());
        }
        analyze(child);
        set |= columns.get(child);
        maxChildHeight = Math.max(maxChildHeight, height.get(child));
        childSkips |= skipping.get(child);
      }
      columns.put(node, set);
      height.put(node, 1 + maxChildHeight);
      skipping.put(node, skips || childSkips);
    }

    private static void requireLiteralOffset(VarkaVectorIR offset) {
      if (!(offset instanceof LiteralSlot)) {
        throw new IllegalArgumentException("day offsets must be literal slots, got " + offset);
      }
    }
  }

  private static boolean referenced(Analysis analysis, int ordinal) {
    return (analysis.referencedColumns >>> ordinal & 1L) != 0;
  }

  // ---------------------------------------------------------------------------------------------
  // Slot planning.
  // ---------------------------------------------------------------------------------------------

  /** The local-variable slots one emitted body uses, threaded to the emitters. */
  private static final class Slots {
    /** The nominal data / validity segment sizes in bytes (long slots). */
    int dataBytes;
    int validityBytes;
    final int[] srcSeg;
    final int[] srcValSeg;
    final int[] dead;
    final int[] hasNulls;
    final int[] word;
    final int[] dstSeg;
    final int[] dstValSeg;
    int ncTmp;
    int species;
    int lanes;
    int loopBound;
    int[] scalarArg;
    int[] broadcastSlot;
    int iVar;
    int byteOffset;
    int cmpTmp;
    int maskTmp;
    /** Per distinct value node: its validity-word reference (a long slot, an input's word
     * slot, or {@link #WORD_ALL_TRUE}); aliased where the algebra makes it a copy. */
    final Map<VarkaVectorIR, Integer> wordRef = new HashMap<>();
    /** The value nodes whose word is computed into their own slot (not an alias). */
    final Set<VarkaVectorIR> ownWord = new HashSet<>();
    /** Per condition node, masked body: the known-true / known-false word slots. */
    final Map<VarkaVectorIR, Integer> kt = new HashMap<>();
    final Map<VarkaVectorIR, Integer> kf = new HashMap<>();
    /** The conditions whose kt/kf are computed (Not aliases its child's, swapped). */
    final Set<VarkaVectorIR> ownCond = new HashSet<>();
    /** Per condition node, dense body: the single mask local. */
    final Map<VarkaVectorIR, Integer> condMask = new HashMap<>();
    /** Per node used more than once: the local its first vector lands in (DAG-CSE). */
    final Map<VarkaVectorIR, Integer> sharedSlot = new HashMap<>();
    /** Per Greatest/Least (masked): the two operand temporaries the substitution needs. */
    final Map<VarkaVectorIR, int[]> pairTmp = new HashMap<>();
    /** Per DayOfWeek/WeekDay: the original-value and fold temporaries. */
    final Map<VarkaVectorIR, int[]> dowTmp = new HashMap<>();
    /** Per calendar node: the civil-from-days temporaries (task 26, extended by task 34's
     * leap-flag tail), eight vectors and two masks - the decomposition is too long to keep
     * on the operand stack. */
    final Map<VarkaVectorIR, int[]> chronoTmp = new HashMap<>();
    /**
     * The epilogue's bounds mask (task 24), or null in every other body role. Non-null is
     * exactly the signal that loads and stores take their masked overloads: the value is a
     * {@code VectorMask} local, live for the whole single pass.
     */
    Integer epilogueMask;
    /** The driver's status accumulator (an int slot), where its callees' returns are ORed. */
    int status;
    /**
     * The guard's accumulated out-of-range mask (task 26), or null when this body has no
     * chrono node at all. Non-null is exactly the signal that the method returns something
     * other than a constant zero.
     */
    Integer guardAcc;

    Slots(int numInputs, int numOutputs) {
      srcSeg = new int[numInputs];
      srcValSeg = new int[numInputs];
      dead = new int[numInputs];
      hasNulls = new int[numInputs];
      word = new int[numInputs];
      dstSeg = new int[numOutputs];
      dstValSeg = new int[numOutputs];
    }
  }

  /**
   * Assigns every local slot the body needs, including the per-node word and condition slots,
   * with word aliasing: a node whose validity equals one child's (a literal offset, a unary
   * op) shares that child's reference instead of recomputing it. Per-node slots are planned
   * only for the body roles that emit them - the vector-walk slots for a loop or epilogue
   * method, neither for the driver, which runs only the shared prologue.
   */
  private static Slots planSlots(boolean dense, BodyMode mode, List<VarkaVectorIR> outputs,
      Analysis analysis, int numLiterals) {
    int numInputs = analysis.numInputs;
    Slots s = new Slots(numInputs, outputs.size());
    int slot = 8;
    s.dataBytes = slot;
    slot += 2;
    s.validityBytes = slot;
    slot += 2;
    for (int o = 0; o < outputs.size(); o++) {
      s.dstSeg[o] = slot++;
      s.dstValSeg[o] = slot++;
    }
    for (int i = 0; i < numInputs; i++) {
      if (referenced(analysis, i)) {
        s.srcSeg[i] = slot++;
        s.srcValSeg[i] = slot++;
        s.dead[i] = slot++;
        s.hasNulls[i] = slot++;
        s.word[i] = slot;
        slot += 2;
      }
    }
    s.ncTmp = slot++;
    s.species = slot++;
    s.lanes = slot++;
    s.loopBound = slot++;
    s.scalarArg = new int[numLiterals];
    for (int j = 0; j < numLiterals; j++) {
      s.scalarArg[j] = slot++;
    }
    // Broadcasts are hoisted into vector locals only where they are used - the loop methods -
    // and only in the regime task 9 measured the hoist as a win: one output, at most a chain's
    // worth of literals. Any wider body inlines them at each use and lets C2 rematerialize
    // under register pressure (PLAN_TASK_10.md).
    s.broadcastSlot = mode == BodyMode.LOOP
        && outputs.size() == 1 && numLiterals <= MAX_CHAIN_DEPTH ? new int[numLiterals] : null;
    if (s.broadcastSlot != null) {
      for (int j = 0; j < numLiterals; j++) {
        s.broadcastSlot[j] = slot++;
      }
    }
    s.iVar = slot++;
    s.byteOffset = slot;
    slot += 2;
    s.cmpTmp = slot;
    slot += 2;
    s.maskTmp = slot++;
    s.status = slot++;
    // The guard exists only where a lowering is partial - today, the narrowed calendar one.
    // Allocated for the whole body rather than per node: one accumulator carries every guarded
    // node's verdict, since the caller acts on the batch, not on the lane.
    boolean guarded = mode != BodyMode.DRIVER
        && outputs.stream().anyMatch(VarkaLoopEmitter::hasChrono);
    if (guarded) {
      s.guardAcc = slot++;
    }

    if (mode == BodyMode.EPILOGUE) {
      s.epilogueMask = slot++;
    }

    // The epilogue is the loop body run once over a partial lane group, so it needs exactly
    // the loop's slots - the word, condition, CSE and temporary locals - and none of its own.
    boolean vectorWalk = mode == BodyMode.LOOP || mode == BodyMode.EPILOGUE;
    boolean cse = analysis.options.cse();
    for (VarkaVectorIR node : analysis.topoOrder) {
      if (vectorWalk) {
        // Vector-walk slots. Children precede parents in the topo order, so a word reference
        // computed here always sees concrete child references - the aliasing depends on it.
        if (!(node instanceof Cond)) {
          if (!dense) {
            int ref = planWordRef(node, s);
            if (ref == Integer.MIN_VALUE) {
              ref = slot;
              slot += 2;
              s.ownWord.add(node);
            }
            s.wordRef.put(node, ref);
          }
          if (cse && analysis.useCount.get(node) > 1 && !(node instanceof LiteralSlot)) {
            s.sharedSlot.put(node, slot++);
          }
          if (!dense && (node instanceof Greatest || node instanceof Least)) {
            s.pairTmp.put(node, new int[] {slot++, slot++});
          }
          if (node instanceof DayOfWeek || node instanceof WeekDay) {
            s.dowTmp.put(node, new int[] {slot++, slot++});
          }
          if (isChrono(node)) {
            // Eight int-vector temporaries and two masks; see emitChrono for what stays live.
            // The last two (the biased year and a remainder scratch) are only used by
            // DayOfYear's leap-flag tail, but every chrono node gets them so the slot layout
            // stays uniform.
            s.chronoTmp.put(node, new int[] {
                slot++, slot++, slot++, slot++, slot++, slot++, slot++, slot++, slot++, slot++});
          }
        } else if (dense) {
          s.condMask.put(node, slot++);
        } else {
          if (node instanceof Not n) {
            // NOT swaps the pair: pure slot aliasing, no code emitted for it.
            s.kt.put(node, s.kf.get(n.child()));
            s.kf.put(node, s.kt.get(n.child()));
          } else {
            s.kt.put(node, slot);
            slot += 2;
            s.kf.put(node, slot);
            slot += 2;
            s.ownCond.add(node);
          }
        }
      }
    }
    return s;
  }

  /**
   * The validity-word reference for a value node, or {@code Integer.MIN_VALUE} when the node
   * needs its own slot (assigned in a second pass). AND-nodes over a single non-constant
   * child alias that child; literal-only subtrees are the all-true constant.
   */
  private static int planWordRef(VarkaVectorIR node, Slots s) {
    return switch (node) {
      case ColumnRef c -> s.word[c.ordinal()];
      case LiteralSlot l -> WORD_ALL_TRUE;
      case AddDays n -> s.wordRef.get(n.days());
      case SubDays n -> s.wordRef.get(n.days());
      case DayOfWeek n -> s.wordRef.get(n.days());
      case WeekDay n -> s.wordRef.get(n.days());
      case Year n -> s.wordRef.get(n.days());
      case Month n -> s.wordRef.get(n.days());
      case DayOfMonth n -> s.wordRef.get(n.days());
      case Quarter n -> s.wordRef.get(n.days());
      case DayOfYear n -> s.wordRef.get(n.days());
      case DateDiff n -> andRef(s.wordRef.get(n.end()), s.wordRef.get(n.start()));
      // Greatest/Least (OR) and IfElse (blend) always compute their own word.
      default -> Integer.MIN_VALUE;
    };
  }

  private static int andRef(int a, int b) {
    if (a == WORD_ALL_TRUE) {
      return b;
    }
    if (b == WORD_ALL_TRUE || a == b) {
      return a;
    }
    return Integer.MIN_VALUE;
  }

  // ---------------------------------------------------------------------------------------------
  // The emitted body methods.
  // ---------------------------------------------------------------------------------------------

  /**
   * One body method in one of the three roles of the method layout (see {@link #emit}). The
   * dense variants run only when the dispatcher has proven every referenced input null-free,
   * so they emit no all-null shortcut and no validity words; the masked variants are the
   * general ones, and the pairs must agree wherever both could run. Every method re-derives
   * the prologue state from the same seven parameters; only the driver zeroes the destination
   * validity (the loop and epilogue methods run after bits were written and must not), and
   * the epilogue starts its single pass at {@code loopBound}.
   */
  private static void emitBody(CodeBuilder cb, boolean dense, BodyMode mode, int group,
      ClassDesc classDesc, List<VarkaVectorIR> outputs, Analysis analysis, int numLiterals,
      List<List<Integer>> groups) {
    int numInputs = analysis.numInputs;
    int numOutputs = outputs.size();
    Slots s = planSlots(dense, mode, outputs, analysis, numLiterals);

    // (1) if (length <= 0) return 0 - nothing ran, so there is nothing to report.
    Label nonEmpty = cb.newLabel();
    cb.iload(P_LENGTH);
    cb.ifgt(nonEmpty);
    cb.loadConstant(0);
    cb.ireturn();
    cb.labelBinding(nonEmpty);

    // (2) Nominal sizes: dataBytes = (long) length * 4; validityBytes = (length + 7) / 8L.
    cb.iload(P_LENGTH);
    cb.i2l();
    cb.loadConstant(4L);
    cb.lmul();
    cb.lstore(s.dataBytes);
    cb.iload(P_LENGTH);
    cb.loadConstant(7);
    cb.iadd();
    cb.i2l();
    cb.loadConstant(8L);
    cb.ldiv();
    cb.lstore(s.validityBytes);

    // (3) Per output: segments, and - in the driver only - zero(dstValidity) before any
    // return below, the emitter invariant: an output nothing writes must still read as
    // all-null. The loop and epilogue methods run after bits were written and must not. A
    // Cond root's data address is 0L by the interface contract and must not be materialized
    // (the same rule as an all-null input's validity address); zeroing its bitmap doubles
    // as the selection invariant - an unwritten row reads as unselected.
    for (int o = 0; o < numOutputs; o++) {
      if (!(outputs.get(o) instanceof Cond)) {
        loadSegment(cb, P_DST_DATA, o, s.dataBytes, s.dstSeg[o]);
      }
      loadSegment(cb, P_DST_VALIDITY, o, s.validityBytes, s.dstValSeg[o]);
      if (mode == BodyMode.DRIVER) {
        cb.aload(s.dstValSeg[o]);
        cb.invokestatic(SUPPORT, "zero", ZERO);
      }
    }

    // (4) Per referenced input: null state (masked body only - the dispatcher has proven a
    // dense batch null-free) and the data segment. An all-null input's validity address is 0L
    // by the morsel contract, so its segment must not be materialized; its validity word is 0L
    // in every group instead, which nulls everything computed from it.
    for (int i = 0; i < numInputs; i++) {
      if (!referenced(analysis, i)) {
        continue;
      }
      if (dense) {
        loadSegment(cb, P_SRC_DATA, i, s.dataBytes, s.srcSeg[i]);
        continue;
      }
      cb.aload(P_NULL_COUNT);
      cb.loadConstant(i);
      cb.iaload();
      cb.istore(s.ncTmp);
      Label notDead = cb.newLabel();
      Label stateDone = cb.newLabel();
      cb.iload(s.ncTmp);
      cb.iload(P_LENGTH);
      cb.if_icmpne(notDead);
      cb.loadConstant(1);
      cb.istore(s.dead[i]);
      cb.loadConstant(0);
      cb.istore(s.hasNulls[i]);
      cb.aconst_null();
      cb.astore(s.srcValSeg[i]);
      cb.goto_(stateDone);
      cb.labelBinding(notDead);
      cb.loadConstant(0);
      cb.istore(s.dead[i]);
      Label noNulls = cb.newLabel();
      cb.iload(s.ncTmp);
      cb.ifle(noNulls);
      cb.loadConstant(1);
      cb.istore(s.hasNulls[i]);
      cb.aload(P_SRC_VALIDITY);
      cb.loadConstant(i);
      cb.laload();
      cb.lload(s.validityBytes);
      cb.invokestatic(SUPPORT, "ofAddress", OF_ADDRESS);
      cb.astore(s.srcValSeg[i]);
      cb.goto_(stateDone);
      cb.labelBinding(noNulls);
      cb.loadConstant(0);
      cb.istore(s.hasNulls[i]);
      cb.aconst_null();
      cb.astore(s.srcValSeg[i]);
      cb.labelBinding(stateDone);
      loadSegment(cb, P_SRC_DATA, i, s.dataBytes, s.srcSeg[i]);
    }

    // (5) All-null shortcut: return iff every output reads at least one all-null column.
    // Sound only for null-intolerant outputs - a null-skipping subtree (greatest, IfElse) can
    // be valid over an all-null column - and emitted in the masked driver only (the dense
    // body has nothing null; the loop and epilogue methods never run when it fires), and
    // only when every output references a column. A Cond root (task 21) is excluded outright
    // rather than reasoned about: Or(unknown, known-true) is known true, so an OR over one
    // all-null column and one live one still selects rows, which the zeroed bitmap the
    // shortcut leaves behind would deny. The loop needs no shortcut to be correct there -
    // an all-null input's word is 0L, so its side contributes no known-true bits.
    boolean shortcutApplies = !dense && mode == BodyMode.DRIVER;
    for (VarkaVectorIR root : outputs) {
      shortcutApplies &= analysis.columns.get(root) != 0L && !analysis.skipping.get(root)
          && !(root instanceof Cond);
    }
    if (shortcutApplies) {
      Label live = cb.newLabel();
      boolean firstOutput = true;
      for (VarkaVectorIR root : outputs) {
        long set = analysis.columns.get(root);
        boolean firstColumn = true;
        for (int i = 0; i < numInputs; i++) {
          if ((set >>> i & 1L) != 0) {
            cb.iload(s.dead[i]);
            if (!firstColumn) {
              cb.ior();
            }
            firstColumn = false;
          }
        }
        if (!firstOutput) {
          cb.iand();
        }
        firstOutput = false;
      }
      cb.ifeq(live);
      cb.loadConstant(0);
      cb.ireturn();
      cb.labelBinding(live);
    }

    // Species, lane count, loop bound, and the hoisted scalar arguments (LICM). The species is
    // read with getstatic so it stays a JIT constant - what lets C2 intrinsify the calls.
    cb.getstatic(INT_VECTOR, "SPECIES_PREFERRED", VECTOR_SPECIES);
    cb.astore(s.species);
    cb.aload(s.species);
    cb.invokeinterface(VECTOR_SPECIES, "length", SPECIES_LENGTH);
    cb.istore(s.lanes);
    cb.aload(s.species);
    cb.iload(P_LENGTH);
    cb.invokeinterface(VECTOR_SPECIES, "loopBound", LOOP_BOUND);
    cb.istore(s.loopBound);
    for (int j = 0; j < numLiterals; j++) {
      cb.aload(P_SCALAR_ARGS);
      cb.loadConstant(j);
      cb.iaload();
      cb.istore(s.scalarArg[j]);
      if (s.broadcastSlot != null) {
        cb.aload(s.species);
        cb.iload(s.scalarArg[j]);
        cb.invokestatic(INT_VECTOR, "broadcast", BROADCAST);
        cb.astore(s.broadcastSlot[j]);
      }
    }

    if (s.guardAcc != null) {
      // An empty mask: no lane has been found out of range yet.
      cb.aload(s.species);
      cb.loadConstant(0L);
      cb.invokestatic(VECTOR_MASK, "fromLong", FROM_LONG);
      cb.astore(s.guardAcc);
    }

    switch (mode) {
      case DRIVER -> {
        // Every callee returns a status; the batch's is their union, so one out-of-range lane
        // anywhere condemns the whole batch - which is what the caller acts on.
        cb.loadConstant(0);
        cb.istore(s.status);
        for (int g = 0; g < groups.size(); g++) {
          cb.iload(s.status);
          invokeCall(cb, classDesc, (dense ? "loopDense" : "loopMasked") + g);
          cb.ior();
          cb.istore(s.status);
        }
        // The rows past loopBound belong to the sibling epilogue method.
        cb.iload(s.status);
        invokeCall(cb, classDesc, dense ? "epilogueDense" : "epilogueMasked");
        cb.ior();
        cb.ireturn();
      }
      case LOOP -> {
        emitVectorLoop(cb, dense, outputs, groups.get(group), analysis, s);
        emitStatusReturn(cb, s);
      }
      case EPILOGUE -> {
        // One method for every output, not one per group: the epilogue runs a single pass per
        // batch, so GROUP_BUDGET - which exists to keep a *hot* method's C2 compile cheap -
        // has nothing to bound here. This is the same shape the scalar tail it replaces had.
        List<Integer> all = new java.util.ArrayList<>();
        for (int o = 0; o < numOutputs; o++) {
          all.add(o);
        }
        emitEpilogue(cb, dense, outputs, all, analysis, s);
        emitStatusReturn(cb, s);
      }
    }
  }

  /**
   * Ends a loop or epilogue method with its status: a constant zero where nothing is guarded,
   * and otherwise whether any lane the body saw fell outside the lowering's range. The
   * reduction is once per method, not once per lane group - the accumulator is a mask OR in
   * the loop, which is one op.
   */
  private static void emitStatusReturn(CodeBuilder cb, Slots s) {
    if (s.guardAcc == null) {
      cb.loadConstant(0);
      cb.ireturn();
      return;
    }
    Label clean = cb.newLabel();
    cb.aload(s.guardAcc);
    cb.invokevirtual(VECTOR_MASK, "anyTrue", ANY_TRUE);
    cb.ifeq(clean);
    cb.loadConstant(VarkaFusedKernel.STATUS_CHRONO_RANGE);
    cb.ireturn();
    cb.labelBinding(clean);
    cb.loadConstant(0);
    cb.ireturn();
  }

  private static void emitVectorLoop(CodeBuilder cb, boolean dense,
      List<VarkaVectorIR> outputs, List<Integer> outputIdx, Analysis analysis, Slots s) {
    // (6) The lane-group loop: for (i = 0; i < loopBound; i += lanes).
    cb.loadConstant(0);
    cb.istore(s.iVar);
    Label loopTop = cb.newLabel();
    Label loopEnd = cb.newLabel();
    cb.labelBinding(loopTop);
    cb.iload(s.iVar);
    cb.iload(s.loopBound);
    cb.if_icmpge(loopEnd);

    emitLaneGroup(cb, dense, outputs, outputIdx, analysis, s);

    cb.iload(s.iVar);
    cb.iload(s.lanes);
    cb.iadd();
    cb.istore(s.iVar);
    cb.goto_(loopTop);
    cb.labelBinding(loopEnd);
  }

  /**
   * (7) The masked epilogue, as its own method body (task 24): the rows past
   * {@code loopBound}, done as one more iteration of the very same lane-group body rather
   * than as a second, scalar walk of the IR. Three substitutions make it so - {@code i} is
   * {@code loopBound} with no back edge, {@code lanes} becomes the remainder so every
   * validity helper is bounded by it, and {@code indexInRange} supplies the mask the loads
   * and the stores take. Nothing between a load and a store is masked, exactly as in the
   * loop.
   *
   * <p>The masked load is not an optimization here: the data segment is sized to
   * {@code length * 4}, so an unmasked load of the last partial group would run off the end
   * of the segment. Its other consequence is the invariant recorded in the class doc - lanes
   * outside the mask read {@code 0}, so no operation in the walk may trap on {@code 0}.
   *
   * <p>What this replaces: a per-row topological pass that computed every distinct node's
   * value (and, masked, its validity bit and a condition's kT/kF bits) into int locals - a
   * complete second lowering of the IR, roughly 330 lines and a second {@code switch} over
   * every node type, which every node type added after task 24 would have had to extend
   * twice and keep in agreement row for row.
   */
  private static void emitEpilogue(CodeBuilder cb, boolean dense,
      List<VarkaVectorIR> outputs, List<Integer> outputIdx, Analysis analysis, Slots s) {
    // Nothing to do when the batch divides evenly - the common case, since the default
    // COLUMN_BATCH_SIZE is 4096 and every lane count this runs at divides it.
    Label remainder = cb.newLabel();
    cb.iload(s.loopBound);
    cb.iload(P_LENGTH);
    cb.if_icmplt(remainder);
    cb.loadConstant(0);
    cb.ireturn();
    cb.labelBinding(remainder);

    cb.iload(s.loopBound);
    cb.istore(s.iVar);
    // `lanes` means "how many rows this group covers" everywhere below, which for the last
    // group is the remainder - not a lane width, which is why the validity helpers switch to
    // their partial-group forms (see validityBits / orValidityBits). This one store is what
    // keeps the partial group's validity from reading or writing past the batch.
    cb.iload(P_LENGTH);
    cb.iload(s.loopBound);
    cb.isub();
    cb.istore(s.lanes);
    cb.aload(s.species);
    cb.iload(s.loopBound);
    cb.iload(P_LENGTH);
    cb.invokeinterface(VECTOR_SPECIES, "indexInRange", INDEX_IN_RANGE);
    cb.astore(s.epilogueMask);

    emitLaneGroup(cb, dense, outputs, outputIdx, analysis, s);
  }

  /**
   * The two validity helpers, named per group shape. A whole lane group spans a power-of-two
   * number of bytes and is read or written in one access; the epilogue's partial group is not
   * a lane width at all, so it takes the {@code partial} pair, which walks the bytes it spans
   * and cannot run off a nominally sized bitmap. The descriptors are identical, so the body
   * emitters differ only in the name they pass. Getting this wrong is silent, not loud: a
   * nine-row group handed to the whole-group form reads one byte and calls its ninth row null.
   */
  private static String validityBits(Slots s) {
    return s.epilogueMask != null ? "partialValidityBitsAt" : "validityBitsAt";
  }

  private static String orValidityBits(Slots s) {
    return s.epilogueMask != null ? "orPartialValidityBitsAt" : "orValidityBitsAt";
  }

  /**
   * One lane group: this group's validity words, then each output's vector walk and store.
   * Shared by the loop, which calls it per iteration, and the epilogue, which calls it once
   * with {@code s.epilogueMask} set - the only difference between them inside here.
   */
  private static void emitLaneGroup(CodeBuilder cb, boolean dense,
      List<VarkaVectorIR> outputs, List<Integer> outputIdx, Analysis analysis, Slots s) {
    int numInputs = analysis.numInputs;

    // byteOffset = (long) i * 4.
    cb.iload(s.iVar);
    cb.i2l();
    cb.loadConstant(4L);
    cb.lmul();
    cb.lstore(s.byteOffset);

    // The columns this loop method can read: the union over its own outputs' subtrees. The
    // kernel-wide referenced set would also be sound but wasteful - the word computation below
    // runs per lane group, and an input only other groups reference has no reader here.
    long groupColumns = 0L;
    for (int o : outputIdx) {
      groupColumns |= analysis.columns.get(outputs.get(o));
    }

    if (!dense) {
      // Each group-referenced input's validity word for this lane group: 0L when all-null, the
      // bitmap bits when it has nulls, -1L when null-free. All three branches leave one long.
      for (int i = 0; i < numInputs; i++) {
        if ((groupColumns >>> i & 1L) == 0) {
          continue;
        }
        Label wNotDead = cb.newLabel();
        Label wNoNulls = cb.newLabel();
        Label wDone = cb.newLabel();
        cb.iload(s.dead[i]);
        cb.ifeq(wNotDead);
        cb.loadConstant(0L);
        cb.goto_(wDone);
        cb.labelBinding(wNotDead);
        cb.iload(s.hasNulls[i]);
        cb.ifeq(wNoNulls);
        cb.aload(s.srcValSeg[i]);
        cb.iload(s.iVar);
        cb.i2l();
        cb.iload(s.lanes);
        cb.invokestatic(SUPPORT, validityBits(s), VALIDITY_BITS_AT);
        cb.goto_(wDone);
        cb.labelBinding(wNoNulls);
        cb.loadConstant(-1L);
        cb.labelBinding(wDone);
        cb.lstore(s.word[i]);
      }
    }

    // Each output of this group: the DAG post-order with intermediates on the operand stack
    // (or in a shared node's local), one unmasked store, and this lane group's validity bits -
    // the root's word (all-true when dense), which orValidityBitsAt truncates itself.
    // A Cond root (task 21) writes no data at all: its output is the selection bitmap - the
    // known-true word, which is unknown-as-false by construction (kT is a subset of valid) -
    // OR-ed into dstValidity exactly where a value root ORs its validity word; the dstData
    // slot stays untouched, per the interface contract.
    Set<VarkaVectorIR> computed = new HashSet<>();
    for (int o : outputIdx) {
      VarkaVectorIR root = outputs.get(o);
      if (root instanceof Cond cond) {
        emitCond(cb, cond, dense, analysis, s, computed);
        cb.aload(s.dstValSeg[o]);
        cb.iload(s.iVar);
        cb.i2l();
        if (dense) {
          cb.aload(s.condMask.get(cond));
          cb.invokevirtual(VECTOR_MASK, "toLong", TO_LONG);
        } else {
          cb.lload(s.kt.get(cond));
        }
        cb.iload(s.lanes);
        cb.invokestatic(SUPPORT, orValidityBits(s), OR_VALIDITY_BITS_AT);
        continue;
      }
      emitValue(cb, root, dense, analysis, s, computed);
      cb.aload(s.dstSeg[o]);
      cb.lload(s.byteOffset);
      cb.getstatic(BYTE_ORDER, "LITTLE_ENDIAN", BYTE_ORDER);
      if (s.epilogueMask != null) {
        cb.aload(s.epilogueMask);
        cb.invokevirtual(INT_VECTOR, "intoMemorySegment", INTO_MEMORY_SEGMENT_MASKED);
      } else {
        cb.invokevirtual(INT_VECTOR, "intoMemorySegment", INTO_MEMORY_SEGMENT_DENSE);
      }
      cb.aload(s.dstValSeg[o]);
      cb.iload(s.iVar);
      cb.i2l();
      if (dense) {
        cb.loadConstant(-1L);
      } else {
        loadWord(cb, s.wordRef.get(root));
      }
      cb.iload(s.lanes);
      cb.invokestatic(SUPPORT, orValidityBits(s), OR_VALIDITY_BITS_AT);
    }
  }

  /** {@code local = VarkaVectorSupport.ofAddress(param[index], lload(bytes))}. */
  private static void loadSegment(
      CodeBuilder cb, int arrayParam, int index, int bytesSlot, int destSlot) {
    cb.aload(arrayParam);
    cb.loadConstant(index);
    cb.laload();
    cb.lload(bytesSlot);
    cb.invokestatic(SUPPORT, "ofAddress", OF_ADDRESS);
    cb.astore(destSlot);
  }

  /**
   * The {@code LineNumberTable}'s decoding key: one {@code <line>=<node>} entry per distinct
   * IR node, newline separated, in the topological order the line numbers index (task 16).
   * Recorded in {@link VarkaDebugInfo} so the mapping travels inside the class bytes.
   *
   * <p>Nodes render through {@link VarkaVectorIR#canonicalShallow}, which task 23 added for
   * this: the key used to be built from {@link Record#toString}, whose format no JDK promises,
   * and which inlined each node's whole subtree - so a shared subexpression was repeated once
   * per parent and the key grew quadratically in the sharing the emitter is built to exploit.
   * Children are their own line numbers here, so the key reconstructs the DAG and each node is
   * written once.
   */
  private static String renderLineMap(Analysis analysis) {
    StringBuilder key = new StringBuilder();
    for (int i = 0; i < analysis.topoOrder.size(); i++) {
      if (i > 0) {
        key.append('\n');
      }
      VarkaVectorIR node = analysis.topoOrder.get(i);
      key.append(i + 1).append('=')
          .append(VarkaVectorIR.canonicalShallow(node, analysis.lineNumbers::get));
    }
    return key.toString();
  }

  /**
   * The whole IR as one line for {@link VarkaDebugInfo}'s summary field - the full recursive
   * {@link VarkaVectorIR#canonical} rendering per output, for the same reason the line map uses
   * the shallow one: {@code Record.toString} is not a format anything may depend on.
   */
  private static String renderOutputs(List<VarkaVectorIR> outputs) {
    StringBuilder rendered = new StringBuilder("[");
    for (int i = 0; i < outputs.size(); i++) {
      if (i > 0) {
        rendered.append(", ");
      }
      rendered.append(VarkaVectorIR.canonical(outputs.get(i)));
    }
    return rendered.append(']').toString();
  }

  /**
   * Attributes the instructions emitted next to the node's own line of the notional source
   * file - its 1-based topological index (task 16). Called immediately before each node's
   * defining instruction, so a stack trace through the generated loop names the IR node that
   * threw rather than only the method; {@link VarkaDebugInfo} carries the decoding key.
   */
  private static void line(CodeBuilder cb, Analysis analysis, VarkaVectorIR node) {
    Integer number = analysis.lineNumbers.get(node);
    if (number != null) {
      cb.lineNumber(number);
    }
  }

  /** Pushes a validity word: a long local, or the all-true constant. */
  private static void loadWord(CodeBuilder cb, int ref) {
    if (ref == WORD_ALL_TRUE) {
      cb.loadConstant(-1L);
    } else {
      cb.lload(ref);
    }
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
  private static void emitValue(CodeBuilder cb, VarkaVectorIR node, boolean dense,
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
          cb.invokestatic(INT_VECTOR, "fromMemorySegment", FROM_MEMORY_SEGMENT_MASKED);
        } else {
          cb.invokestatic(INT_VECTOR, "fromMemorySegment", FROM_MEMORY_SEGMENT_DENSE);
        }
      }
      case LiteralSlot l -> {
        line(cb, analysis, node);
        if (s.broadcastSlot != null) {
          cb.aload(s.broadcastSlot[l.index()]);
        } else {
          cb.aload(s.species);
          cb.iload(s.scalarArg[l.index()]);
          cb.invokestatic(INT_VECTOR, "broadcast", BROADCAST);
        }
      }
      case AddDays n -> {
        emitValue(cb, n.days(), dense, analysis, s, computed);
        emitValue(cb, n.offset(), dense, analysis, s, computed);
        // The misdescribe hook: whichever body executes first must fail naming the call.
        MethodTypeDesc desc =
            analysis.options.misdescribeAdd() ? LANEWISE_VV_WRONG : LANEWISE_VV;
        line(cb, analysis, node);
        cb.invokevirtual(INT_VECTOR, "add", desc);
      }
      case SubDays n -> {
        emitValue(cb, n.days(), dense, analysis, s, computed);
        emitValue(cb, n.offset(), dense, analysis, s, computed);
        line(cb, analysis, node);
        cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VV);
      }
      case DateDiff n -> {
        emitValue(cb, n.end(), dense, analysis, s, computed);
        emitValue(cb, n.start(), dense, analysis, s, computed);
        line(cb, analysis, node);
        cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VV);
        if (!dense && s.ownWord.contains(node)) {
          emitAndWord(cb, s.wordRef.get(node),
              s.wordRef.get(n.end()), s.wordRef.get(n.start()));
        }
      }
      case DayOfWeek n -> {
        emitValue(cb, n.days(), dense, analysis, s, computed);
        line(cb, analysis, node);
        emitFloorMod7(cb, node, analysis, s);
        emitModOffset(cb, s, 4);
        cb.loadConstant(1);
        cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI);
      }
      case WeekDay n -> {
        emitValue(cb, n.days(), dense, analysis, s, computed);
        line(cb, analysis, node);
        emitFloorMod7(cb, node, analysis, s);
        emitModOffset(cb, s, 3);
      }
      case Year n -> {
        emitValue(cb, n.days(), dense, analysis, s, computed);
        line(cb, analysis, node);
        emitChrono(cb, node, dense, analysis, s);
      }
      case Month n -> {
        emitValue(cb, n.days(), dense, analysis, s, computed);
        line(cb, analysis, node);
        emitChrono(cb, node, dense, analysis, s);
      }
      case DayOfMonth n -> {
        emitValue(cb, n.days(), dense, analysis, s, computed);
        line(cb, analysis, node);
        emitChrono(cb, node, dense, analysis, s);
      }
      case Quarter n -> {
        emitValue(cb, n.days(), dense, analysis, s, computed);
        line(cb, analysis, node);
        emitChrono(cb, node, dense, analysis, s);
      }
      case DayOfYear n -> {
        emitValue(cb, n.days(), dense, analysis, s, computed);
        line(cb, analysis, node);
        emitChrono(cb, node, dense, analysis, s);
      }
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
        cb.invokevirtual(INT_VECTOR, "blend", BLEND);
        if (!dense) {
          // valid = (kT & validThen) | (~kT & validElse), the chosen branch's validity.
          cb.lload(s.kt.get(n.cond()));
          loadWord(cb, s.wordRef.get(n.thenNode()));
          cb.land();
          cb.lload(s.kt.get(n.cond()));
          cb.loadConstant(-1L);
          cb.lxor();
          loadWord(cb, s.wordRef.get(n.elseNode()));
          cb.land();
          cb.lor();
          cb.lstore(s.wordRef.get(node));
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

  /** {@code lstore(own, ref(a) & ref(b))} - the null-intolerant word rule. */
  private static void emitAndWord(CodeBuilder cb, int own, int a, int b) {
    loadWord(cb, a);
    loadWord(cb, b);
    cb.land();
    cb.lstore(own);
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
      cb.invokevirtual(INT_VECTOR, op, LANEWISE_VV);
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
    loadWord(cb, s.wordRef.get(left));
    cb.loadConstant(-1L);
    cb.lxor();
    cb.invokestatic(VECTOR_MASK, "fromLong", FROM_LONG);
    cb.invokevirtual(INT_VECTOR, "blend", BLEND);
    cb.aload(tmp[1]);
    cb.aload(tmp[0]);
    cb.aload(s.species);
    loadWord(cb, s.wordRef.get(right));
    cb.loadConstant(-1L);
    cb.lxor();
    cb.invokestatic(VECTOR_MASK, "fromLong", FROM_LONG);
    cb.invokevirtual(INT_VECTOR, "blend", BLEND);
    cb.invokevirtual(INT_VECTOR, op, LANEWISE_VV);
    loadWord(cb, s.wordRef.get(left));
    loadWord(cb, s.wordRef.get(right));
    cb.lor();
    cb.lstore(s.wordRef.get(node));
  }

  /**
   * Consumes the child's {@code IntVector} on the stack and leaves {@code floorMod(v, 7)},
   * full range. The shipped variant (the task 14 follow-up) is two 15-bit digit-sum folds
   * ({@code 2^15 = 1 mod 7}) followed by Granlund-Montgomery magic division: the folds
   * leave {@code v <= 32771} (unsigned reading), the +3-where-negative fixup
   * ({@code 2^32 = 4 mod 7}) raises that to at most 32774, and in that range the magic is
   * exact in the <i>low</i> 32 bits - with {@code M = ceil(2^18 / 7) = 37450} and
   * {@code e = 7 * M - 2^18 = 6}, {@code v * e < 2^18} makes {@code q = (v * M) >>> 18}
   * exactly {@code v / 7}, and {@code v * M < 2^31} keeps the low-half multiply from
   * overflowing, so {@code r = v - q * 7} needs no final fixup at all. The multiply-high
   * the classic trick wants is not expressible in the Vector API; pre-folding makes the
   * low half sufficient. Measured 1.6-1.8x the task 11 digit sum at buffer level and a
   * ~10-op-smaller loop method, which also shortens the per-task JIT warm-up
   * (PLAN_TASK_14.md 7.5). The full digit sum behind
   * {@link VarkaEmitOptions.FloorMod7#DIGIT_SUM} and the lanewise DIV behind
   * {@link VarkaEmitOptions.FloorMod7#DIV} are the reference variants the parity benchmark
   * prices this one against.
   */
  private static void emitFloorMod7(
      CodeBuilder cb, VarkaVectorIR node, Analysis analysis, Slots s) {
    int[] tmp = s.dowTmp.get(node);
    int orig = tmp[0];
    int fold = tmp[1];
    cb.astore(orig);
    if (analysis.options.floorMod7() == VarkaEmitOptions.FloorMod7.DIV) {
      // r = v - (v / 7) * 7; r += 7 where r < 0.
      cb.aload(orig);
      cb.aload(orig);
      cb.loadConstant(7);
      cb.invokevirtual(INT_VECTOR, "div", LANEWISE_VI);
      cb.loadConstant(7);
      cb.invokevirtual(INT_VECTOR, "mul", LANEWISE_VI);
      cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VV);
      cb.astore(fold);
      cb.aload(fold);
      cb.loadConstant(7);
      cb.aload(fold);
      cb.getstatic(VECTOR_OPERATORS, "LT", VO_COMPARISON);
      cb.loadConstant(0);
      cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VI);
      cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI_MASKED);
      return;
    }
    if (analysis.options.floorMod7() == VarkaEmitOptions.FloorMod7.DIGIT_SUM) {
      // The task 11 shipped variant: folds of two 15-bit halves, one 6-bit, three 3-bit.
      emitFold(cb, orig, fold, 0x7FFF, 15);
      emitFold(cb, fold, fold, 0x7FFF, 15);
      emitFold(cb, fold, fold, 63, 6);
      emitFold(cb, fold, fold, 7, 3);
      emitFold(cb, fold, fold, 7, 3);
      emitFold(cb, fold, fold, 7, 3);
      // s += 3 where the original value was negative.
      cb.aload(fold);
      cb.loadConstant(3);
      cb.aload(orig);
      cb.getstatic(VECTOR_OPERATORS, "LT", VO_COMPARISON);
      cb.loadConstant(0);
      cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VI);
      cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI_MASKED);
      // One conditional subtract lands [0, 12] in [0, 6].
      emitSubSevenWhereGe(cb, s);
      return;
    }
    // Two folds, the sign fixup, then the exact magic (the method comment has the bounds).
    emitFold(cb, orig, fold, 0x7FFF, 15);
    emitFold(cb, fold, fold, 0x7FFF, 15);
    cb.aload(fold);
    cb.loadConstant(3);
    cb.aload(orig);
    cb.getstatic(VECTOR_OPERATORS, "LT", VO_COMPARISON);
    cb.loadConstant(0);
    cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VI);
    cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI_MASKED);
    cb.astore(fold);
    // r = v - ((v * 37450) >>> 18) * 7.
    cb.aload(fold);
    cb.aload(fold);
    cb.loadConstant(37450);
    cb.invokevirtual(INT_VECTOR, "mul", LANEWISE_VI);
    cb.getstatic(VECTOR_OPERATORS, "LSHR", VO_BINARY);
    cb.loadConstant(18);
    cb.invokevirtual(INT_VECTOR, "lanewise", LANEWISE_BINARY_I);
    cb.loadConstant(7);
    cb.invokevirtual(INT_VECTOR, "mul", LANEWISE_VI);
    cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VV);
  }

  /** {@code dst = src.and(mask).add(src >>> shift)}, all through locals. */
  private static void emitFold(CodeBuilder cb, int src, int dst, int mask, int shift) {
    cb.aload(src);
    cb.loadConstant(mask);
    cb.invokevirtual(INT_VECTOR, "and", LANEWISE_VI);
    cb.aload(src);
    cb.getstatic(VECTOR_OPERATORS, "LSHR", VO_BINARY);
    cb.loadConstant(shift);
    cb.invokevirtual(INT_VECTOR, "lanewise", LANEWISE_BINARY_I);
    cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VV);
    cb.astore(dst);
  }

  /** Consumes nothing: {@code [s] -> [s - 7 where s >= 7]} via one masked subtract. */
  private static void emitSubSevenWhereGe(CodeBuilder cb, Slots s) {
    cb.dup();
    cb.getstatic(VECTOR_OPERATORS, "GE", VO_COMPARISON);
    cb.loadConstant(7);
    cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VI);
    cb.astore(s.maskTmp);
    cb.loadConstant(7);
    cb.aload(s.maskTmp);
    cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VI_MASKED);
  }

  /** {@code [r] -> [(r + k) mod 7]} for {@code r} in {@code [0, 6]}, {@code k} in 3..4. */
  private static void emitModOffset(CodeBuilder cb, Slots s, int k) {
    cb.loadConstant(k);
    cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI);
    emitSubSevenWhereGe(cb, s);
  }

  /**
   * Consumes the child's {@code IntVector} of epoch days and leaves one of the five calendar
   * fields (task 26, plus {@code dayOfYear} from task 34). {@link VarkaChrono} is the scalar
   * twin of everything below - it holds every constant this method loads, and its own javadoc
   * carries the derivation - so the two cannot drift and a disagreement between them is an
   * emission bug rather than an arithmetic one.
   *
   * <p>The shape is a civil-from-days decomposition in a March-based year, where the leap day
   * is a year's last day rather than an interior one. There is no vector divide, so every
   * division is a magic multiply: the three small ones are exact, and the two large ones
   * ({@code / 146097} and {@code / 36524}) use a round-down magic that never overestimates,
   * followed by carries that are one compare and two masked adjustments each. That is the
   * whole reason this node weighs {@link #CHRONO_WEIGHT} rather than 1.
   *
   * <p>The temporaries are locals rather than operand-stack juggling because six values stay
   * live across the tail - era, century, year of century, day of year, the March month, and
   * two masks - which is past what the stack can hold legibly.
   */
  private static void emitChrono(CodeBuilder cb, VarkaVectorIR node, boolean dense,
      Analysis analysis, Slots s) {
    int[] t = s.chronoTmp.get(node);
    int days = t[0];
    int era = t[1];
    int rem = t[2];
    int century = t[3];
    int yearOfCentury = t[4];
    int marchMonth = t[5];
    int mask = t[6];
    int leap = t[7];
    int biasedYear = t[8];
    int remScratch = t[9];

    cb.astore(days);

    emitEra(cb, node, dense, analysis, s, days, era, rem, mask);

    // rem is now the day of era, in [0, 146096]. Everything below works on that.
    // century = (doe * M) >>> K, then doc = doe - century * 36524, with one carry.
    cb.aload(rem);
    emitMagic(cb, VarkaChrono.CENTURY_M, VarkaChrono.CENTURY_K);
    cb.astore(century);
    cb.aload(rem);
    cb.aload(century);
    cb.loadConstant(VarkaChrono.CENTURY_DAYS);
    cb.invokevirtual(INT_VECTOR, "mul", LANEWISE_VI);
    cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VV);
    cb.astore(rem);
    emitCarry(cb, century, rem, VarkaChrono.CENTURY_DAYS, mask);

    // An era's fourth century holds one extra day - its leap day - so the quotient can land on
    // 4 for exactly one day of each era. Fold that back into century 3.
    cb.aload(century);
    cb.getstatic(VECTOR_OPERATORS, "EQ", VO_COMPARISON);
    cb.loadConstant(4);
    cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VI);
    cb.astore(mask);
    cb.aload(century);
    cb.loadConstant(1);
    cb.aload(mask);
    cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VI_MASKED);
    cb.astore(century);
    cb.aload(rem);
    cb.loadConstant(VarkaChrono.CENTURY_DAYS);
    cb.aload(mask);
    cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI_MASKED);
    cb.astore(rem);

    // yoc = doc / 365 - exact here, because the split into centuries left a dividend under
    // 44859. It ignores leap days, so it can name the following year; the fix is below.
    cb.aload(rem);
    emitMagic(cb, VarkaChrono.YEAR_M, VarkaChrono.YEAR_K);
    cb.astore(yearOfCentury);

    // doy = doc - (365 * yoc + yoc / 4). Negative exactly where yoc overshot.
    cb.aload(rem);
    cb.aload(yearOfCentury);
    cb.loadConstant(365);
    cb.invokevirtual(INT_VECTOR, "mul", LANEWISE_VI);
    cb.aload(yearOfCentury);
    emitShift(cb, "LSHR", 2);
    cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VV);
    cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VV);
    cb.astore(rem);

    // Where it overshot, step back a year and give the days back - one more when the year we
    // step into is a leap year, which in a March-based year is simply yoc divisible by four.
    cb.aload(rem);
    cb.getstatic(VECTOR_OPERATORS, "LT", VO_COMPARISON);
    cb.loadConstant(0);
    cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VI);
    cb.astore(mask);
    cb.aload(yearOfCentury);
    cb.loadConstant(3);
    cb.invokevirtual(INT_VECTOR, "and", LANEWISE_VI);
    cb.getstatic(VECTOR_OPERATORS, "EQ", VO_COMPARISON);
    cb.loadConstant(0);
    cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VI);
    cb.aload(mask);
    cb.invokevirtual(VECTOR_MASK, "and", MASK_BINARY);
    cb.astore(leap);
    cb.aload(rem);
    cb.loadConstant(365);
    cb.aload(mask);
    cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI_MASKED);
    cb.loadConstant(1);
    cb.aload(leap);
    cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI_MASKED);
    cb.astore(rem);
    cb.aload(yearOfCentury);
    cb.loadConstant(1);
    cb.aload(mask);
    cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VI_MASKED);
    cb.astore(yearOfCentury);

    // mp = (5 * doy + 2) / 153: the March-based month, 0 for March through 11 for February.
    cb.aload(rem);
    cb.loadConstant(5);
    cb.invokevirtual(INT_VECTOR, "mul", LANEWISE_VI);
    cb.loadConstant(2);
    cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI);
    emitMagic(cb, VarkaChrono.MONTH_M, VarkaChrono.MONTH_K);
    cb.astore(marchMonth);

    switch (node) {
      case Year n -> emitYearValue(cb, era, century, yearOfCentury, marchMonth);
      case Month n -> emitChronoMonth(cb, marchMonth);
      case DayOfMonth n -> {
        // doy - (153 * mp + 2) / 5 + 1, the inverse of the month's own linear form.
        cb.aload(rem);
        cb.aload(marchMonth);
        cb.loadConstant(153);
        cb.invokevirtual(INT_VECTOR, "mul", LANEWISE_VI);
        cb.loadConstant(2);
        cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI);
        emitMagic(cb, VarkaChrono.DAY_M, VarkaChrono.DAY_K);
        cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VV);
        cb.loadConstant(1);
        cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI);
      }
      case Quarter n -> {
        emitChronoMonth(cb, marchMonth);
        cb.loadConstant(2);
        cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI);
        emitMagic(cb, VarkaChrono.QUARTER_M, VarkaChrono.QUARTER_K);
      }
      case DayOfYear n -> {
        // year - Year's own formula, recomputed here because the leap flag needs a plain
        // year and nothing upstream keeps one around.
        emitYearValue(cb, era, century, yearOfCentury, marchMonth);
        cb.astore(biasedYear);
        emitLeapFlag(cb, biasedYear, remScratch);
        cb.astore(leap);

        // dayofyear = doy >= 306 ? doy - 305 : doy + 60 + L
        cb.aload(rem);
        cb.getstatic(VECTOR_OPERATORS, "GE", VO_COMPARISON);
        cb.loadConstant(VarkaChrono.MARCH_TO_JANUARY_DAYS);
        cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VI);
        cb.astore(mask);

        cb.aload(rem);
        cb.loadConstant(VarkaChrono.MARCH_DAY_OF_YEAR);
        cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI);
        cb.loadConstant(1);
        cb.aload(leap);
        cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI_MASKED);

        cb.aload(rem);
        cb.loadConstant(VarkaChrono.MARCH_TO_JANUARY_DAYS - 1);
        cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VI);
        cb.aload(mask);
        cb.invokevirtual(INT_VECTOR, "blend", BLEND);
      }
      default -> throw new IllegalStateException("not a calendar node: " + node);
    }
  }

  /**
   * The day-of-era step: one round-down division and one carry over a biased day, which is
   * defined only over {@link VarkaChrono#NARROW_MIN_DAYS}..{@link VarkaChrono#NARROW_MAX_DAYS} -
   * so it also emits the guard, which is what makes the cheaper arithmetic safe to publish.
   *
   * <p>A variant that split the dividend instead, and so needed no guard at all over the whole
   * int range, was built and measured against this one before being dropped: it cost 14 to 24%
   * depending on width and null pattern, to buy a range no SQL date literal can reach. The
   * numbers are in {@code PLAN_TASK_26.md} section 11.2.
   *
   * <p>The guard is two compares ORed together, then narrowed twice before it is ORed into
   * the body's accumulator, and both narrowings are load-bearing:
   *
   * <ul>
   *   <li><b>The row's validity</b>, taken from the node's own word reference. A null row's
   *       data bytes are undefined, so an out-of-range value under one must not condemn the
   *       batch. {@code planWordRef} aliases a chrono node's word to its child's, and the
   *       child's word is live by the time this runs, so this covers a computed child as
   *       well as a bare column - which an earlier version did not, and which is the shape
   *       {@code year(date_add(d, n))} takes.</li>
   *   <li><b>The epilogue's bounds mask</b>, where there is one. A masked load fills the
   *       lanes past {@code length} with 0, and 0 is in range - but the guard runs on this
   *       node's <i>input</i>, and a computed child maps 0 wherever it likes. Without this,
   *       {@code year(date_sub(d, 5400000))} declines every batch whose length is not a lane
   *       multiple while every real row is in range: correct answers, silent total loss of
   *       fusion, and nothing above debug logging to say so.</li>
   * </ul>
   */
  private static void emitEra(CodeBuilder cb, VarkaVectorIR node, boolean dense,
      Analysis analysis, Slots s, int days, int era, int rem, int mask) {
    cb.aload(days);
    cb.getstatic(VECTOR_OPERATORS, "LT", VO_COMPARISON);
    cb.loadConstant(VarkaChrono.NARROW_MIN_DAYS);
    cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VI);
    cb.aload(days);
    cb.getstatic(VECTOR_OPERATORS, "GT", VO_COMPARISON);
    cb.loadConstant(VarkaChrono.NARROW_MAX_DAYS);
    cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VI);
    cb.invokevirtual(VECTOR_MASK, "or", MASK_BINARY);
    if (!dense) {
      // The node's own word, which planWordRef has aliased to its child's - so this is the
      // child's validity whatever shape the child has.
      Integer word = s.wordRef.get(node);
      if (word != null && word != WORD_ALL_TRUE) {
        cb.aload(s.species);
        loadWord(cb, word);
        cb.invokestatic(VECTOR_MASK, "fromLong", FROM_LONG);
        cb.invokevirtual(VECTOR_MASK, "and", MASK_BINARY);
      }
    }
    if (s.epilogueMask != null) {
      cb.aload(s.epilogueMask);
      cb.invokevirtual(VECTOR_MASK, "and", MASK_BINARY);
    }
    cb.aload(s.guardAcc);
    cb.invokevirtual(VECTOR_MASK, "or", MASK_BINARY);
    cb.astore(s.guardAcc);

    // w = days + BIAS, non-negative throughout the range, so one round-down magic and one
    // carry give the era - and the bias's whole eras come back off in the year assembly.
    cb.aload(days);
    cb.loadConstant(VarkaChrono.NARROW_BIAS);
    cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI);
    cb.astore(rem);
    cb.aload(rem);
    emitMagic(cb, VarkaChrono.NARROW_ERA_M, VarkaChrono.NARROW_ERA_K);
    cb.astore(era);
    cb.aload(rem);
    cb.aload(era);
    cb.loadConstant(VarkaChrono.ERA_DAYS);
    cb.invokevirtual(INT_VECTOR, "mul", LANEWISE_VI);
    cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VV);
    cb.astore(rem);
    emitCarry(cb, era, rem, VarkaChrono.ERA_DAYS, mask);
    cb.aload(era);
    cb.loadConstant(VarkaChrono.NARROW_ERA_BIAS);
    cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VI);
    cb.astore(era);
  }

  /** {@code [v] -> [(v * m) >>> k]}, the shape every division in {@link #emitChrono} takes. */
  private static void emitMagic(CodeBuilder cb, int m, int k) {
    cb.loadConstant(m);
    cb.invokevirtual(INT_VECTOR, "mul", LANEWISE_VI);
    emitShift(cb, "LSHR", k);
  }

  /** {@code [v] -> [v shifted]} by a constant, for either shift direction. */
  private static void emitShift(CodeBuilder cb, String op, int bits) {
    cb.getstatic(VECTOR_OPERATORS, op, VO_BINARY);
    cb.loadConstant(bits);
    cb.invokevirtual(INT_VECTOR, "lanewise", LANEWISE_BINARY_I);
  }

  /**
   * One correction step of a round-down magic division: where the remainder still reaches the
   * divisor, the quotient was one short. Consumes nothing and leaves nothing on the stack -
   * both operands are locals, because the pair is applied up to twice in a row.
   */
  private static void emitCarry(CodeBuilder cb, int quotient, int remainder, int divisor,
      int mask) {
    cb.aload(remainder);
    cb.getstatic(VECTOR_OPERATORS, "GE", VO_COMPARISON);
    cb.loadConstant(divisor);
    cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VI);
    cb.astore(mask);
    cb.aload(quotient);
    cb.loadConstant(1);
    cb.aload(mask);
    cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI_MASKED);
    cb.astore(quotient);
    cb.aload(remainder);
    cb.loadConstant(divisor);
    cb.aload(mask);
    cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VI_MASKED);
    cb.astore(remainder);
  }

  /** Leaves the mask of lanes whose March-based year has already turned into January. */
  private static void emitJanuaryMask(CodeBuilder cb, int marchMonth) {
    cb.aload(marchMonth);
    cb.getstatic(VECTOR_OPERATORS, "GE", VO_COMPARISON);
    cb.loadConstant(VarkaChrono.MARCH_YEAR_JANUARY);
    cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VI);
  }

  /** Leaves the January-based month: {@code mp + 3}, less 12 once the year has turned. */
  private static void emitChronoMonth(CodeBuilder cb, int marchMonth) {
    cb.aload(marchMonth);
    cb.loadConstant(3);
    cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI);
    cb.loadConstant(12);
    emitJanuaryMask(cb, marchMonth);
    cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VI_MASKED);
  }

  /** Leaves the proleptic Gregorian year: {@code 400 * era + 100 * century + yearOfCentury},
   * plus one where the March-based year has already turned January. Shared by {@code Year}
   * and {@code DayOfYear}, which both need the plain year value. */
  private static void emitYearValue(CodeBuilder cb, int era, int century, int yearOfCentury,
      int marchMonth) {
    cb.aload(era);
    cb.loadConstant(400);
    cb.invokevirtual(INT_VECTOR, "mul", LANEWISE_VI);
    cb.aload(century);
    cb.loadConstant(100);
    cb.invokevirtual(INT_VECTOR, "mul", LANEWISE_VI);
    cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VV);
    cb.aload(yearOfCentury);
    cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VV);
    cb.loadConstant(1);
    emitJanuaryMask(cb, marchMonth);
    cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI_MASKED);
  }

  /**
   * Leaves the mask of lanes whose reported (January-based) year is a leap year -
   * {@link VarkaChrono#isLeapYear}'s lane-wise twin, two round-down magic-multiply modulo
   * tests over a year biased non-negative, rather than a shortcut off
   * {@code yearOfCentury}/{@code century}: that shortcut is tempting but wrong at the century
   * and era boundaries, where the reported year has already rolled over relative to those
   * intermediates. Tasks 35 and 36 call this too (`PLAN_TASK_35.md`, `PLAN_TASK_36.md`), so it
   * is written to be called rather than inlined - task 37's `weekofyear` does not call this
   * method; its own `weeksIn` needs a structurally different helper (`PLAN_TASK_37.md` section
   * 3) even though it shares this method's {@link VarkaChrono#LEAP_YEAR_BIAS} bias.
   *
   * <p>Both magics are round-down, so the quotient can undershoot by one and the remainder
   * they leave is either the true one or the true one plus the divisor - which is why each
   * test below is "remainder is 0 or the divisor" rather than a single {@code == 0}.
   *
   * @param biasedYear a slot holding the plain reported year on entry; overwritten with the
   *     biased year, since the biased value is read five times below.
   * @param remScratch a scratch slot for a remainder that is read twice.
   */
  private static void emitLeapFlag(CodeBuilder cb, int biasedYear, int remScratch) {
    cb.aload(biasedYear);
    cb.loadConstant(VarkaChrono.LEAP_YEAR_BIAS);
    cb.invokevirtual(INT_VECTOR, "add", LANEWISE_VI);
    cb.astore(biasedYear);

    // by4 = (biasedYear & 3) == 0
    cb.aload(biasedYear);
    cb.loadConstant(3);
    cb.invokevirtual(INT_VECTOR, "and", LANEWISE_VI);
    cb.getstatic(VECTOR_OPERATORS, "EQ", VO_COMPARISON);
    cb.loadConstant(0);
    cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VI);

    // not(by100)
    emitDivisibleBy(cb, biasedYear, remScratch, VarkaChrono.LEAP_CENTURY_M,
        VarkaChrono.LEAP_CENTURY_K, 100);
    cb.invokevirtual(VECTOR_MASK, "not", MASK_UNARY);

    // by400
    emitDivisibleBy(cb, biasedYear, remScratch, VarkaChrono.LEAP_ERA_M, VarkaChrono.LEAP_ERA_K,
        400);

    // leap = by4 & (not(by100) | by400)
    cb.invokevirtual(VECTOR_MASK, "or", MASK_BINARY);
    cb.invokevirtual(VECTOR_MASK, "and", MASK_BINARY);
  }

  /**
   * Leaves the mask of lanes where {@code biasedYear} is divisible by {@code divisor}: a
   * round-down magic-multiply quotient ({@code m}, {@code k}) can undershoot the true quotient
   * by one, so the remainder is tested against both {@code 0} and {@code divisor} rather than a
   * single {@code == 0} - the same "round-down plus one correction" idiom {@link #emitCarry}
   * uses for a quotient, adapted here to a modulo test. {@code remScratch} holds the remainder
   * across its two-way equality check, since it is read twice.
   */
  private static void emitDivisibleBy(CodeBuilder cb, int biasedYear, int remScratch, int m,
      int k, int divisor) {
    cb.aload(biasedYear);
    cb.aload(biasedYear);
    emitMagic(cb, m, k);
    cb.loadConstant(divisor);
    cb.invokevirtual(INT_VECTOR, "mul", LANEWISE_VI);
    cb.invokevirtual(INT_VECTOR, "sub", LANEWISE_VV);
    cb.astore(remScratch);
    cb.aload(remScratch);
    cb.getstatic(VECTOR_OPERATORS, "EQ", VO_COMPARISON);
    cb.loadConstant(0);
    cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VI);
    cb.aload(remScratch);
    cb.getstatic(VECTOR_OPERATORS, "EQ", VO_COMPARISON);
    cb.loadConstant(divisor);
    cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VI);
    cb.invokevirtual(VECTOR_MASK, "or", MASK_BINARY);
  }

  /**
   * Emits a condition node: in the dense body a single {@code VectorMask} local (every input
   * lane is valid, so known-true is the comparison itself and known-false its complement); in
   * the masked body the known-true / known-false word pair of plan 2.6.
   */
  private static void emitCond(CodeBuilder cb, Cond node, boolean dense, Analysis analysis,
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
        cb.invokevirtual(INT_VECTOR, "compare", COMPARE_VV);
        if (dense) {
          cb.astore(s.condMask.get(node));
        } else {
          cb.invokevirtual(VECTOR_MASK, "toLong", TO_LONG);
          cb.lstore(s.cmpTmp);
          // kT = cmp & validL & validR; kF = ~cmp & validL & validR.
          cb.lload(s.cmpTmp);
          loadWord(cb, s.wordRef.get(n.left()));
          cb.land();
          loadWord(cb, s.wordRef.get(n.right()));
          cb.land();
          cb.lstore(s.kt.get(node));
          cb.lload(s.cmpTmp);
          cb.loadConstant(-1L);
          cb.lxor();
          loadWord(cb, s.wordRef.get(n.left()));
          cb.land();
          loadWord(cb, s.wordRef.get(n.right()));
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
          loadWord(cb, s.wordRef.get(n.child()));
          cb.lstore(s.kt.get(node));
          loadWord(cb, s.wordRef.get(n.child()));
          cb.loadConstant(-1L);
          cb.lxor();
          cb.lstore(s.kf.get(node));
        }
      }
    }
  }
}
