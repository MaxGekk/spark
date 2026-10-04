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
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ObjIntConsumer;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.function.UnaryOperator;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions.Builder;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions.Division;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions.FloorMod7;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions.TruncDateForm;

/**
 * One option of {@link VarkaEmitOptions} as the options table holds it: its name, why it exists,
 * its default, how to read it off a value and set it on a builder, how it reaches
 * {@link VarkaEmitOptions#canonical()}, and - for an int - the values the IR fuzzer draws and the
 * bytes suite's inventory audits.
 *
 * <p>{@link #TABLE} lists every option once, in the record's declaration order, and everything
 * that treats the options as a set reads it rather than its own list: the defaults are built from
 * it by name, {@code canonical()} renders from it, and the bytes suite's inventory, the IR
 * fuzzer's draws and {@code VarkaEmitDump}'s parser walk it. Adding an option is the record
 * component, its builder setter and {@code with*} method, its javadoc, and one entry here.
 */
public sealed interface VarkaEmitOption
    permits VarkaEmitOption.Flag, VarkaEmitOption.Count, VarkaEmitOption.Choice {

  /** Why an option exists: what keeps it in the record rather than deleted. */
  enum Reason {
    /**
     * An alternative form kept as the reference the shipped one is checked against: the A/B that
     * priced the choice stays re-runnable, and the differential suites compare the two.
     */
    REFERENCE,
    /**
     * An alternative kept because which form wins depends on the machine - its vector width or
     * its AVX level - so no single form can be shipped for every host.
     */
    MACHINE,
    /**
     * A size limit that tests and benchmarks vary: a small value shows a mechanism on a shape of
     * a few outputs, a large one prices a retuning.
     */
    KNOB,
    /**
     * A correctness check kept switchable so its cost can be priced. Off computes wrong answers,
     * so only the A/B and the differential run it so.
     */
    PRICED_CHECK,
    /**
     * A fault injector: it feeds the emitter a wrong input so a test can show that the emitter's
     * own self-checks fire. The fuzzer never draws one.
     */
    FAULT_INJECTOR
  }

  /** How an option reaches {@code canonical()}. */
  sealed interface Rendering permits Positional, FlagTag, CountTag {}

  /** Rendered by position, always: the first 26 options, which predate the tags. */
  record Positional() implements Rendering {}

  /** A flag shown as {@code |token} when its value is {@code when}, and not at all otherwise. */
  record FlagTag(String token, boolean when) implements Rendering {}

  /** A count shown as {@code |prefix<value>} when off its default, and not at all otherwise. */
  record CountTag(String prefix) implements Rendering {}

  /** One arm of the bytes suite's inventory: its label and the change it makes to a value. */
  record Arm(String name, UnaryOperator<VarkaEmitOptions> apply) {}

  Rendering POSITIONAL = new Positional();

  String name();

  Reason reason();

  Rendering rendering();

  /** Sets this option's default on a builder. */
  void applyDefault(Builder builder);

  /** This option's value in {@code options}, as {@code canonical()} prints it. */
  String text(VarkaEmitOptions options);

  /** This option's tag in {@code options}: {@code ""} unless its rendering is a tag that shows. */
  String tag(VarkaEmitOptions options);

  /** The inventory's arms for this option, each labelled {@code name=value}. */
  List<Arm> arms();

  /** A boolean option. */
  record Flag(String name, Reason reason, boolean defaultValue, Predicate<VarkaEmitOptions> get,
      BiConsumer<Builder, Boolean> set, Rendering rendering) implements VarkaEmitOption {

    public Flag {
      if (rendering instanceof CountTag) {
        throw new IllegalArgumentException(name + ": a flag renders by position or as a flag tag");
      }
    }

    public boolean value(VarkaEmitOptions options) {
      return get.test(options);
    }

    public VarkaEmitOptions with(VarkaEmitOptions options, boolean value) {
      Builder builder = options.toBuilder();
      set.accept(builder, value);
      return builder.build();
    }

    @Override
    public void applyDefault(Builder builder) {
      set.accept(builder, defaultValue);
    }

    @Override
    public String text(VarkaEmitOptions options) {
      return String.valueOf(value(options));
    }

    @Override
    public String tag(VarkaEmitOptions options) {
      return rendering instanceof FlagTag(String token, boolean when) && value(options) == when
          ? "|" + token : "";
    }

    @Override
    public List<Arm> arms() {
      return List.of(new Arm(name + "=true", o -> with(o, true)),
          new Arm(name + "=false", o -> with(o, false)));
    }
  }

  /**
   * An int option. {@code fuzzDraws} are the values the IR fuzzer draws from, and
   * {@code auditValues} the inventory's arms; either may be empty.
   */
  record Count(String name, Reason reason, int defaultValue, ToIntFunction<VarkaEmitOptions> get,
      ObjIntConsumer<Builder> set, Rendering rendering, List<Integer> fuzzDraws,
      List<Integer> auditValues) implements VarkaEmitOption {

    public Count {
      if (rendering instanceof FlagTag) {
        throw new IllegalArgumentException(
            name + ": a count renders by position or as a count tag");
      }
      fuzzDraws = List.copyOf(fuzzDraws);
      auditValues = List.copyOf(auditValues);
    }

    public int value(VarkaEmitOptions options) {
      return get.applyAsInt(options);
    }

    public VarkaEmitOptions with(VarkaEmitOptions options, int value) {
      Builder builder = options.toBuilder();
      set.accept(builder, value);
      return builder.build();
    }

    @Override
    public void applyDefault(Builder builder) {
      set.accept(builder, defaultValue);
    }

    @Override
    public String text(VarkaEmitOptions options) {
      return String.valueOf(value(options));
    }

    @Override
    public String tag(VarkaEmitOptions options) {
      return rendering instanceof CountTag(String prefix) && value(options) != defaultValue
          ? "|" + prefix + value(options) : "";
    }

    @Override
    public List<Arm> arms() {
      List<Arm> arms = new ArrayList<>();
      for (int v : auditValues) {
        arms.add(new Arm(name + "=" + v, o -> with(o, v)));
      }
      return arms;
    }
  }

  /** An enum option: one of {@code type}'s constants. Always rendered by position. */
  record Choice<E extends Enum<E>>(String name, Reason reason, Class<E> type, E defaultValue,
      Function<VarkaEmitOptions, E> get, BiConsumer<Builder, E> set) implements VarkaEmitOption {

    public E value(VarkaEmitOptions options) {
      return get.apply(options);
    }

    public VarkaEmitOptions with(VarkaEmitOptions options, E value) {
      Builder builder = options.toBuilder();
      set.accept(builder, value);
      return builder.build();
    }

    public List<E> constants() {
      return List.of(type.getEnumConstants());
    }

    /** {@code options} with the {@code index}-th constant: how a random draw picks one. */
    public VarkaEmitOptions withIndex(VarkaEmitOptions options, int index) {
      return with(options, constants().get(index));
    }

    /**
     * {@code options} with the constant named {@code constant}; an unknown name throws
     * {@link IllegalArgumentException}, naming the constants there are.
     */
    public VarkaEmitOptions withNamed(VarkaEmitOptions options, String constant) {
      for (E e : constants()) {
        if (e.name().equals(constant)) {
          return with(options, e);
        }
      }
      throw new IllegalArgumentException(
          name + " has no constant " + constant + "; it has " + constants());
    }

    @Override
    public Rendering rendering() {
      return POSITIONAL;
    }

    @Override
    public void applyDefault(Builder builder) {
      set.accept(builder, defaultValue);
    }

    @Override
    public String text(VarkaEmitOptions options) {
      return value(options).toString();
    }

    @Override
    public String tag(VarkaEmitOptions options) {
      return "";
    }

    @Override
    public List<Arm> arms() {
      List<Arm> arms = new ArrayList<>();
      for (E constant : constants()) {
        arms.add(new Arm(name + "=" + constant, o -> with(o, constant)));
      }
      return arms;
    }
  }

  /** The values the fuzzer draws a budget from: small enough to bring its mechanism into view. */
  List<Integer> SMALL_BUDGETS = List.of(8, 24, 32);

  /**
   * Every option, once, in the record's declaration order - which is also the order
   * {@code canonical()} renders them in, the positional ones first. See each option's
   * {@code @param} on {@link VarkaEmitOptions} for what it does and the measurement behind its
   * default.
   */
  List<VarkaEmitOption> TABLE = List.of(
      new Count("groupBudget", Reason.KNOB, VarkaEmitBudget.GROUP_BUDGET,
          VarkaEmitOptions::groupBudget, Builder::groupBudget, POSITIONAL,
          SMALL_BUDGETS, List.of(64, 800)),
      new Count("fusedCeiling", Reason.KNOB, VarkaEmitBudget.FUSED_CEILING,
          VarkaEmitOptions::fusedCeiling, Builder::fusedCeiling, POSITIONAL,
          SMALL_BUDGETS, List.of(200, 800)),
      new Flag("cse", Reason.REFERENCE, true,
          VarkaEmitOptions::cse, Builder::cse, POSITIONAL),
      new Flag("shareChronoPrefix", Reason.REFERENCE, true,
          VarkaEmitOptions::shareChronoPrefix, Builder::shareChronoPrefix, POSITIONAL),
      new Flag("denseValidityOnce", Reason.REFERENCE, true,
          VarkaEmitOptions::denseValidityOnce, Builder::denseValidityOnce, POSITIONAL),
      new Flag("elideChronoMonth", Reason.REFERENCE, true,
          VarkaEmitOptions::elideChronoMonth, Builder::elideChronoMonth, POSITIONAL),
      new Flag("neriSchneiderMonth", Reason.REFERENCE, true,
          VarkaEmitOptions::neriSchneiderMonth, Builder::neriSchneiderMonth, POSITIONAL),
      new Flag("julianMap", Reason.REFERENCE, true,
          VarkaEmitOptions::julianMap, Builder::julianMap, POSITIONAL),
      new Flag("guardDayProducers", Reason.PRICED_CHECK, true,
          VarkaEmitOptions::guardDayProducers, Builder::guardDayProducers, POSITIONAL),
      new Flag("validityByWidth", Reason.REFERENCE, true,
          VarkaEmitOptions::validityByWidth, Builder::validityByWidth, POSITIONAL),
      new Flag("validityOrFirst", Reason.REFERENCE, true,
          VarkaEmitOptions::validityOrFirst, Builder::validityOrFirst, POSITIONAL),
      new Flag("validityByBitmap", Reason.REFERENCE, true,
          VarkaEmitOptions::validityByBitmap, Builder::validityByBitmap, POSITIONAL),
      new Flag("checkIntOverflow", Reason.PRICED_CHECK, true,
          VarkaEmitOptions::checkIntOverflow, Builder::checkIntOverflow, POSITIONAL),
      new Count("lanesOverride", Reason.MACHINE, 0,
          VarkaEmitOptions::lanesOverride, Builder::lanesOverride, POSITIONAL,
          List.of(2, 4, 8, 16, 32), List.of()),
      new Choice<>("truncDate", Reason.REFERENCE, TruncDateForm.class, TruncDateForm.SUBTRACT,
          VarkaEmitOptions::truncDate, Builder::truncDate),
      new Choice<>("floorMod7", Reason.REFERENCE, FloorMod7.class, FloorMod7.MAGIC,
          VarkaEmitOptions::floorMod7, Builder::floorMod7),
      new Choice<>("division", Reason.MACHINE, Division.class, Division.MAGIC,
          VarkaEmitOptions::division, Builder::division),
      new Count("useAVX", Reason.MACHINE, VarkaEmitOptions.USE_AVX_UNKNOWN,
          VarkaEmitOptions::useAVX, Builder::useAVX, POSITIONAL,
          List.of(VarkaEmitOptions.USE_AVX_UNKNOWN, 0, 2, 3),
          List.of(VarkaEmitOptions.USE_AVX_UNKNOWN, 0, 1, 2, 3)),
      new Flag("misdescribeAdd", Reason.FAULT_INJECTOR, false,
          VarkaEmitOptions::misdescribeAdd, Builder::misdescribeAdd, POSITIONAL),
      new Flag("misdescribeWordLiveness", Reason.FAULT_INJECTOR, false,
          VarkaEmitOptions::misdescribeWordLiveness, Builder::misdescribeWordLiveness,
          POSITIONAL),
      new Flag("guardUnderArm", Reason.REFERENCE, true,
          VarkaEmitOptions::guardUnderArm, Builder::guardUnderArm, POSITIONAL),
      new Flag("shareWholeNodes", Reason.REFERENCE, true,
          VarkaEmitOptions::shareWholeNodes, Builder::shareWholeNodes, POSITIONAL),
      new Flag("validityByWord", Reason.REFERENCE, false,
          VarkaEmitOptions::validityByWord, Builder::validityByWord, POSITIONAL),
      new Flag("mulHiDivide", Reason.REFERENCE, true,
          VarkaEmitOptions::mulHiDivide, Builder::mulHiDivide, POSITIONAL),
      new Flag("narrowHalfSpecies", Reason.REFERENCE, false,
          VarkaEmitOptions::narrowHalfSpecies, Builder::narrowHalfSpecies, POSITIONAL),
      new Count("methodByteBudget", Reason.KNOB, VarkaEmitBudget.HUGE_METHOD_LIMIT,
          VarkaEmitOptions::methodByteBudget, Builder::methodByteBudget, POSITIONAL,
          List.of(0, 8000, 1000, 2000, 4000), List.of(0, 8000)),
      new Flag("rangeSets", Reason.REFERENCE, true,
          VarkaEmitOptions::rangeSets, Builder::rangeSets, new FlagTag("noRangeSets", false)),
      new Flag("splitConditions", Reason.REFERENCE, true,
          VarkaEmitOptions::splitConditions, Builder::splitConditions,
          new FlagTag("noSplitConditions", false)),
      new Flag("groupLocalSlots", Reason.REFERENCE, true,
          VarkaEmitOptions::groupLocalSlots, Builder::groupLocalSlots,
          new FlagTag("kernelWideSlots", false)),
      new Flag("materializeChronoPrefix", Reason.REFERENCE, true,
          VarkaEmitOptions::materializeChronoPrefix, Builder::materializeChronoPrefix,
          new FlagTag("recomputePrefix", false)),
      new Count("callSiteBudget", Reason.KNOB, VarkaEmitBudget.CALL_SITE_BUDGET,
          VarkaEmitOptions::callSiteBudget, Builder::callSiteBudget, new CountTag("callSites="),
          SMALL_BUDGETS, List.of(0, VarkaEmitBudget.CALL_SITE_BUDGET)),
      new Count("heavyGroupOutputs", Reason.KNOB, VarkaEmitBudget.HEAVY_GROUP_OUTPUTS,
          VarkaEmitOptions::heavyGroupOutputs, Builder::heavyGroupOutputs, new CountTag("heavy="),
          SMALL_BUDGETS, List.of(0, VarkaEmitBudget.HEAVY_GROUP_OUTPUTS)),
      new Flag("predictGrouping", Reason.REFERENCE, true,
          VarkaEmitOptions::predictGrouping, Builder::predictGrouping,
          new FlagTag("predictGrouping", true)),
      new Flag("driverOutputTable", Reason.REFERENCE, true,
          VarkaEmitOptions::driverOutputTable, Builder::driverOutputTable,
          new FlagTag("unrolledDriver", false)),
      new Flag("exactGrouping", Reason.REFERENCE, true,
          VarkaEmitOptions::exactGrouping, Builder::exactGrouping,
          new FlagTag("greedyGrouping", false)),
      new Flag("splitDriver", Reason.REFERENCE, true,
          VarkaEmitOptions::splitDriver, Builder::splitDriver, new FlagTag("wholeDriver", false)),
      new Flag("severalKernels", Reason.REFERENCE, true,
          VarkaEmitOptions::severalKernels, Builder::severalKernels,
          new FlagTag("oneKernel", false)),
      new Flag("elideUnreadLocals", Reason.REFERENCE, true,
          VarkaEmitOptions::elideUnreadLocals, Builder::elideUnreadLocals,
          new FlagTag("elideUnreadLocals", true)),
      new Flag("planSize", Reason.REFERENCE, true,
          VarkaEmitOptions::planSize, Builder::planSize, new FlagTag("planSize", true)),
      new Count("misdescribeDriverBytes", Reason.FAULT_INJECTOR, 0,
          VarkaEmitOptions::misdescribeDriverBytes, Builder::misdescribeDriverBytes,
          new CountTag("misdescribeDriverBytes="), List.of(), List.of()));

  /** The option with this name. */
  static VarkaEmitOption named(String name) {
    for (VarkaEmitOption option : TABLE) {
      if (option.name().equals(name)) {
        return option;
      }
    }
    throw new IllegalArgumentException("no emit option " + name);
  }
}
