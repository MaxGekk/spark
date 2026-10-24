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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;

/**
 * {@link VarkaKernelPlan} against the derivations it replaces (VARKA-488), for as long as both
 * exist: the refactor moves every consumer onto the plan commit by commit, and the emitted-bytes,
 * coverage and width-audit oracles pin only what the compiler accepts, so a plan that stopped
 * refusing a kernel {@code narrowRefusal} refuses would move no byte. This suite pins the
 * refusal side and the lanes: over random kernels of every grammar, pairs of them, and every
 * coverage row, the plan
 * answers what {@code kernelLane} and {@code narrowRefusal} answer, and the
 * compiler's admission of one entry after another decides the same with either. It runs under the
 * matrix's configuration, since the lanes turn on {@code narrowLanes}, {@code validityByWidth}
 * and {@code lanesOverride}. It is deleted with {@code kernelLane} and {@code narrowRefusal}.
 */
@VarkaSuite
public final class VarkaKernelPlanEquivalenceSuite {

  private static final VarkaEmitOptions OPTIONS = VarkaMatrixRuns.BASE;

  /** Draws per grammar. */
  private static final int DRAWS = 400;

  /** One kernel of the corpus: its roots and the input and literal counts that bound them. */
  private record Kernel(String name, List<VarkaVectorIR> roots, int numInputs, int numLiterals) {}

  private static List<Kernel> drawn() {
    var out = new ArrayList<Kernel>();
    List<Function<Random, Kernel>> grammars = List.of(
        r -> of("int", VarkaIrGrammar.drawShape(r)),
        r -> of("narrow", VarkaIrGrammar.drawNarrowShape(r)),
        r -> of("wide", VarkaIrGrammar.drawWideShape(r)),
        r -> of("long", VarkaIrGrammar.drawLongShape(r)),
        r -> of("wide long", VarkaIrGrammar.drawWideLongShape(r)),
        r -> {
          var m = VarkaIrGrammar.drawMixedShape(r);
          return new Kernel("mixed", m.roots(), m.numInputs(), m.intLiterals() + m.longLiterals());
        });
    for (int g = 0; g < grammars.size(); g++) {
      Random rnd = new Random(20261024L + g);
      for (int i = 0; i < DRAWS; i++) {
        out.add(grammars.get(g).apply(rnd));
      }
    }
    return out;
  }

  private static Kernel of(String name, VarkaIrGrammar.Drawn d) {
    return new Kernel(name, d.roots(), d.numInputs(), d.numLiterals());
  }

  private static Kernel of(String name, VarkaIrGrammar.DrawnLong d) {
    return new Kernel(name, d.roots(), d.numInputs(), d.numLiterals());
  }

  /** Every coverage row the compiler fuses, and the width audit's hand-built constructions. */
  private static List<Kernel> coverage() {
    String home = System.getProperty("spark.test.home", System.getenv("SPARK_HOME"));
    assertTrue(home != null, "spark.test.home or SPARK_HOME is not set.");
    File file = new File(home, "sql/varka/coverage.json");
    var out = new ArrayList<Kernel>();
    for (var e : CollectionConverters.asJava(VarkaWidthAuditShapes.all(file))) {
      if (e instanceof VarkaWidthAuditShapes.Shape s) {
        out.add(new Kernel(s.name(), CollectionConverters.asJava(s.roots()), s.numInputs(),
            s.numLiterals()));
      }
    }
    return out;
  }

  /** The kernel of two others' roots side by side, bounded by the larger of each count. */
  private static Kernel join(Kernel a, Kernel b) {
    var roots = new ArrayList<>(a.roots());
    roots.addAll(b.roots());
    return new Kernel(a.name() + " + " + b.name(), roots,
        Math.max(a.numInputs(), b.numInputs()), Math.max(a.numLiterals(), b.numLiterals()));
  }

  private static List<Kernel> corpus() {
    List<Kernel> single = new ArrayList<>(drawn());
    single.addAll(coverage());
    var out = new ArrayList<>(single);
    Random rnd = new Random(488L);
    for (int i = 0; i < 2000; i++) {
      out.add(join(single.get(rnd.nextInt(single.size())),
          single.get(rnd.nextInt(single.size()))));
    }
    return out;
  }

  @Test
  @DisplayName("the plan answers what kernelLane and narrowRefusal answer")
  void lanes() {
    var wrong = new ArrayList<String>();
    int planned = 0;
    for (Kernel k : corpus()) {
      String disagreement = compare(k);
      if (disagreement == null) {
        continue;
      }
      if (disagreement.isEmpty()) {
        planned++;
      } else if (wrong.size() < 20) {
        wrong.add(k.name() + ": " + disagreement);
      }
    }
    assertTrue(wrong.isEmpty(), "the plan disagrees:\n  " + String.join("\n  ", wrong));
    assertTrue(planned > 1000, "only " + planned + " kernels were emittable");
  }

  /**
   * The first way the plan and the derivations it replaces differ on {@code k}, empty when
   * they agree on an emittable kernel, or null when they agree on a refusal. The node and input
   * lanes are not compared: {@code Analysis} reads them from the plan, and the emitted-bytes
   * oracle holds what it emits with them.
   */
  private static String compare(Kernel k) {
    VarkaKernelPlan plan = VarkaKernelPlan.of(k.roots(), OPTIONS);
    Optional<String> narrow = VarkaLoopEmitter.narrowRefusal(k.roots(), OPTIONS);
    Optional<LaneType> kernel = VarkaVectorIR.kernelLane(k.roots(), OPTIONS);
    if (narrow.isPresent()) {
      return plan.refusal().map(VarkaKernelPlan.Refusal::reason).equals(narrow) ? null
          : "narrowRefusal " + narrow + ", the plan " + plan.refusal();
    }
    if (kernel.isEmpty()) {
      return plan.refusal().isPresent() ? null : "no kernel lane, and the plan has none refused";
    }
    if (plan.refusal().isPresent()) {
      return "kernel lane " + kernel.get() + ", and the plan refuses: " + plan.refusal();
    }
    if (plan.drive() != kernel.get()) {
      return "kernel lane " + kernel.get() + ", the plan's " + plan.drive();
    }
    return "";
  }

  @Test
  @DisplayName("the compiler's admission of one entry after another decides the same with either")
  void admission() {
    Random rnd = new Random(20261025L);
    List<Kernel> single = new ArrayList<>(drawn());
    single.addAll(coverage());
    var wrong = new ArrayList<String>();
    int refused = 0;
    for (int run = 0; run < 400 && wrong.size() < 20; run++) {
      List<VarkaVectorIR> outputs = new ArrayList<>();
      for (int step = 0; step < 6; step++) {
        Kernel entry = single.get(rnd.nextInt(single.size()));
        for (VarkaVectorIR ir : entry.roots()) {
          Optional<String> old = oldAdmission(outputs, ir);
          Optional<String> fresh = planAdmission(outputs, ir);
          if (!old.equals(fresh)) {
            wrong.add(entry.name() + " after " + outputs.size() + " entries: before " + old
                + ", with the plan " + fresh);
          }
          if (old.isEmpty()) {
            outputs.add(ir);
          } else {
            refused++;
          }
        }
      }
    }
    assertTrue(wrong.isEmpty(), "the admissions differ:\n  " + String.join("\n  ", wrong));
    assertTrue(refused > 50, "only " + refused + " entries were refused");
  }

  /** {@code VarkaExpressionCompiler}'s lane checks of one entry as they stand before the plan. */
  private static Optional<String> oldAdmission(List<VarkaVectorIR> outputs, VarkaVectorIR ir) {
    Optional<String> narrow = VarkaLoopEmitter.narrowRefusal(List.of(ir), OPTIONS);
    if (narrow.isPresent()) {
      return narrow;
    }
    List<VarkaVectorIR> with = new ArrayList<>(outputs);
    with.add(ir);
    if (!outputs.isEmpty() && VarkaVectorIR.kernelLane(with, OPTIONS).isEmpty()) {
      return Optional.of("the " + VarkaVectorIR.emissionLane(ir, OPTIONS)
          + " lane in a kernel on the " + VarkaVectorIR.kernelLane(outputs, OPTIONS).orElseThrow()
          + " lane: one kernel holds one lane");
    }
    Optional<String> joined = VarkaLoopEmitter.narrowRefusal(with, OPTIONS);
    return joined.map(j -> "joining drives the kernel's narrow entries at the int lane, where "
        + j);
  }

  /** The same checks read from the plan of the entry and the plan of the kernel with it. */
  private static Optional<String> planAdmission(List<VarkaVectorIR> outputs, VarkaVectorIR ir) {
    Optional<VarkaKernelPlan.Refusal> alone = VarkaKernelPlan.of(List.of(ir), OPTIONS).refusal();
    if (alone.isPresent()) {
      return Optional.of(alone.get().reason());
    }
    List<VarkaVectorIR> with = new ArrayList<>(outputs);
    with.add(ir);
    return VarkaKernelPlan.of(with, OPTIONS).refusal().map(r ->
        r.kind() == VarkaKernelPlan.Kind.LANE_MISMATCH ? r.reason()
        : "joining drives the kernel's narrow entries at the int lane, where " + r.reason());
  }
}
