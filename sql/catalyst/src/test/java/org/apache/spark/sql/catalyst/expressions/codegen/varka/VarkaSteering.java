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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.function.IntFunction;

/**
 * Generation steered toward mechanisms the emitter has not reached yet, after QPG (VARKA-280):
 * a draw whose emission reached a set of mechanisms ({@link VarkaEmitMechanism}) no earlier
 * emission had is kept, and later steps mutate a kept draw instead of drawing afresh. The
 * mutations are the ones the suites already have, so a mutant is a case the fuzzer can check:
 * one option of {@code VarkaEmitOption.TABLE} redrawn as the IR fuzzer draws it, or one of the
 * draw's {@link VarkaVariants}, built as {@link VarkaVariantChecks#variants} builds them.
 *
 * <p>The steering is emission only - the mechanisms are compile-time facts - so a sequence is
 * decided before any kernel runs, and a case the fuzzer then checks is one of a sequence that a
 * seed reproduces in seconds. The unsteered arm of the A/B is the fresh draws alone.
 */
final class VarkaSteering {

  private VarkaSteering() {
  }

  /** The options a mutation redraws: every option but the fault injectors, in a fixed order. */
  private static final List<VarkaEmitOption> FUZZED = VarkaEmitOption.TABLE.stream()
      .filter(o -> o.reason() != VarkaEmitOption.Reason.FAULT_INJECTOR)
      .sorted(Comparator.comparing(VarkaEmitOption::name))
      .toList();

  /**
   * What makes a steered step keep its case: a set of mechanisms no earlier emission reached, or
   * a mechanism fewer than {@link #RARE} earlier emissions reached. Random draws reach a new set
   * at most steps, so the first keeps nearly everything; the second keeps what is rare, which is
   * closer to a coverage-guided fuzzer's favoured inputs. The A/B runs both.
   */
  enum Keep { NEW_SET, RARE_MECHANISM }

  /** Below how many earlier emissions a mechanism is rare, for {@link Keep#RARE_MECHANISM}. */
  static final int RARE = 4;

  /** The class name every steering emission builds under: nothing loads the class. */
  private static final String CLASS_NAME = "org.apache.spark.sql.varka.execution.VarkaSteered";

  /**
   * The first {@code budget} fresh draws, each emitted once into {@code trace}: the unsteered
   * arm, and the sequence the IR fuzzer's main tests run.
   */
  static List<VarkaFuzzCase> unsteered(int budget, IntFunction<VarkaFuzzCase> fresh,
      VarkaEmitTrace trace) {
    List<VarkaFuzzCase> cases = new ArrayList<>();
    for (int k = 0; k < budget; k++) {
      VarkaFuzzCase c = fresh.apply(k);
      emit(c, trace);
      cases.add(c);
    }
    return cases;
  }

  /**
   * {@code budget} steps of steered generation, each emitted once into {@code trace}, whose
   * emissions {@code reach} must be attached to: at each step, with even odds or where nothing is
   * kept yet, the next fresh draw, else a mutant of a kept case; a case is kept as {@code keep}
   * says. {@code seed} decides the coin and the mutations, so one seed gives one sequence.
   */
  static List<VarkaFuzzCase> steered(int budget, long seed, IntFunction<VarkaFuzzCase> fresh,
      VarkaEmitTrace trace, VarkaEmitReach reach, Keep keep) {
    Random rnd = new Random(seed ^ 0x53544545524cL);
    List<VarkaFuzzCase> kept = new ArrayList<>();
    List<VarkaFuzzCase> cases = new ArrayList<>();
    int nextFresh = 0;
    for (int k = 0; k < budget; k++) {
      VarkaFuzzCase c = kept.isEmpty() || rnd.nextBoolean()
          ? fresh.apply(nextFresh++)
          : mutate(kept.get(rnd.nextInt(kept.size())), rnd, k);
      int before = reach.emissions();
      emit(c, trace);
      boolean keeps = switch (keep) {
        case NEW_SET -> reach.lastReachedNewSet();
        case RARE_MECHANISM -> reach.lastReachedRare(RARE);
      };
      if (reach.emissions() > before && keeps) {
        kept.add(c);
      }
      cases.add(c);
    }
    return cases;
  }

  /**
   * One mutation of {@code c}: with even odds one option redrawn, else one of its variants that
   * the emitter's form without a byte budget holds, chosen at random among those that apply; the
   * option is redrawn where no variant applies.
   */
  static VarkaFuzzCase mutate(VarkaFuzzCase c, Random rnd, int step) {
    if (rnd.nextBoolean()) {
      List<VarkaVariants.Variant> fitting = new ArrayList<>();
      List<VarkaVectorIR> strangers = new ArrayList<>();
      for (int i = 0; i < c.roots().size(); i++) {
        strangers.add(c.roots().get((step + 1 + i) % c.roots().size()));
      }
      for (VarkaVariantChecks.KindVariant kv : VarkaVariantChecks.variants(c, strangers,
          VarkaVariants.valueSubtrees(c.roots()), step, VarkaSteering::fits)) {
        kv.variant().filter(VarkaSteering::fits).ifPresent(fitting::add);
      }
      if (!fitting.isEmpty()) {
        VarkaVariants.Variant v = fitting.get(rnd.nextInt(fitting.size()));
        return VarkaVariantChecks.caseOf(c, v).withLabel(c.label() + " -> " + v.kind());
      }
    }
    VarkaEmitOption option = FUZZED.get(rnd.nextInt(FUZZED.size()));
    VarkaEmitOptions options = switch (option) {
      case VarkaEmitOption.Flag flag -> flag.with(c.options(), !flag.value(c.options()));
      case VarkaEmitOption.Choice<?> choice ->
          choice.withIndex(c.options(), rnd.nextInt(choice.constants().size()));
      case VarkaEmitOption.Count count ->
          count.with(c.options(), count.fuzzDraws().get(rnd.nextInt(count.fuzzDraws().size())));
    };
    return c.withOptions(options).withLabel(c.label() + " -> " + option.name() + " redrawn");
  }

  /** Whether the emitter's form without a byte budget holds a variant, as it holds every draw. */
  private static boolean fits(VarkaVariants.Variant v) {
    return VarkaLoopEmitter.fitsBudgets(v.roots(), v.numInputs(),
        VarkaEmitOptions.DEFAULTS.withMethodByteBudget(0));
  }

  /**
   * Emits {@code c} once under its options into {@code trace}. A decline is an emission the
   * trace records; any other refusal - a variant past a cap - is a step that emitted nothing.
   */
  static void emit(VarkaFuzzCase c, VarkaEmitTrace trace) {
    try {
      VarkaLoopEmitter.emitTraced(CLASS_NAME, c.roots(), c.numInputs(), c.lits().length,
          c.options(), trace);
    } catch (IllegalArgumentException e) {
      // VarkaEmitDeclined among them, which the trace has already noted.
    }
  }
}
