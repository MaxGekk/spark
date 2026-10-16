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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * What one run of a suite's emissions reached, read off the emitter's own record of each
 * emission (VARKA-280): per mechanism ({@link VarkaEmitMechanism}) how many emissions reached it
 * and at which emission first, the mechanisms never reached, and how many distinct plans and
 * distinct mechanism sets the run produced.
 *
 * <p>A plan, after QPG's (Ba and Rigger, ICSE 2023), is what the emitter did with a shape: the
 * canonical IR of its outputs over literal slots, how many loop-method groups it formed, and the
 * set of mechanisms it reached. Two draws with different literal values and the same trees share
 * a plan. A mechanism set is the plan without its trees: random trees are nearly all distinct,
 * so the set is the coarser count, and the one a steered fuzzer keeps a draw for.
 *
 * <p>Attach one to a trace for the stretch a report covers ({@link #attach}) and detach it after
 * ({@link #detach}); the emitter then hands it every emission that ends through that trace, built
 * or declined. The report is lines for the test's output ({@link #lines}) and a markdown table
 * over several reports ({@link #markdown}) for the file row 265 reads, written where
 * {@code VARKA_REACH_DUMP} names a directory ({@link #dump}).
 */
final class VarkaEmitReach implements Consumer<VarkaEmitTrace.Emission> {

  final String name;
  private int emissions;
  private int declined;
  private final Map<VarkaEmitMechanism, Integer> reachedBy =
      new EnumMap<>(VarkaEmitMechanism.class);
  private final Map<VarkaEmitMechanism, Integer> firstAt = new EnumMap<>(VarkaEmitMechanism.class);
  private final Set<List<Object>> plans = new HashSet<>();
  private final Set<Set<VarkaEmitMechanism>> sets = new HashSet<>();
  private boolean lastNewSet;
  private int lastRarest;

  VarkaEmitReach(String name) {
    this.name = name;
  }

  /** A new report named {@code name}, receiving every emission that ends through {@code trace}. */
  static VarkaEmitReach attach(VarkaEmitTrace trace, String name) {
    VarkaEmitReach reach = new VarkaEmitReach(name);
    trace.onEmission = reach;
    return reach;
  }

  /** Stops {@code trace} handing its emissions to any report. */
  static void detach(VarkaEmitTrace trace) {
    trace.onEmission = null;
  }

  @Override
  public void accept(VarkaEmitTrace.Emission e) {
    emissions++;
    if (e.declined()) {
      declined++;
    }
    lastRarest = Integer.MAX_VALUE;
    for (VarkaEmitMechanism m : e.reached()) {
      lastRarest = Math.min(lastRarest, reachedBy(m));
      reachedBy.merge(m, 1, Integer::sum);
      firstAt.putIfAbsent(m, emissions);
    }
    lastNewSet = sets.add(e.reached());
    // The IR nodes are records, so the outputs compare by structure: the canonical IR's identity
    // without rendering it, and literal slots rather than values, since values are not in the IR.
    plans.add(List.of(e.outputs(), e.groups(), e.reached()));
  }

  /** Whether the last emission reached a set of mechanisms no earlier one had. */
  boolean lastReachedNewSet() {
    return lastNewSet;
  }

  /**
   * Whether the last emission reached a mechanism fewer than {@code below} earlier emissions
   * had: a new one at {@code below} 1, a rare one above.
   */
  boolean lastReachedRare(int below) {
    return lastRarest < below;
  }

  int emissions() {
    return emissions;
  }

  int distinctPlans() {
    return plans.size();
  }

  int distinctSets() {
    return sets.size();
  }

  /** How many emissions reached {@code m}; 0 for one never reached. */
  int reachedBy(VarkaEmitMechanism m) {
    return reachedBy.getOrDefault(m, 0);
  }

  /** The 1-based emission that first reached {@code m}, or -1. */
  int firstAt(VarkaEmitMechanism m) {
    return firstAt.getOrDefault(m, -1);
  }

  Set<VarkaEmitMechanism> neverReached() {
    Set<VarkaEmitMechanism> never = EnumSet.allOf(VarkaEmitMechanism.class);
    never.removeAll(reachedBy.keySet());
    return never;
  }

  /** The report as the test's output prints it: totals, then the mechanisms never reached. */
  List<String> lines() {
    List<String> out = new ArrayList<>();
    Set<VarkaEmitMechanism> never = neverReached();
    out.add(name + ": " + emissions + " emissions (" + declined + " declined), "
        + plans.size() + " distinct plans, " + sets.size() + " distinct mechanism sets, "
        + (VarkaEmitMechanism.values().length - never.size()) + " of "
        + VarkaEmitMechanism.values().length + " mechanisms reached");
    if (!never.isEmpty()) {
      List<String> names = new ArrayList<>();
      for (VarkaEmitMechanism m : never) {
        names.add(m.name());
      }
      out.add(name + ": never reached: " + String.join(", ", names));
    }
    return out;
  }

  /**
   * The reports side by side, as markdown: a totals table, then per mechanism how many emissions
   * of each report reached it, a dash for none, and a last column saying where none did.
   */
  static String markdown(String title, String provenance, List<VarkaEmitReach> reports) {
    StringBuilder md = new StringBuilder();
    md.append("# ").append(title).append("\n\n").append(provenance).append("\n\n");
    md.append("| report | emissions | declined | distinct plans | distinct mechanism sets |"
        + " mechanisms reached |\n| :--- | ---: | ---: | ---: | ---: | ---: |\n");
    int all = VarkaEmitMechanism.values().length;
    for (VarkaEmitReach r : reports) {
      md.append("| ").append(r.name).append(" | ").append(r.emissions).append(" | ")
          .append(r.declined).append(" | ").append(r.plans.size()).append(" | ")
          .append(r.sets.size()).append(" | ").append(all - r.neverReached().size())
          .append(" of ").append(all).append(" |\n");
    }
    md.append("\nEmissions reaching each mechanism; a dash where a report reached none.\n\n");
    md.append("| family | mechanism |");
    StringBuilder rule = new StringBuilder("| :--- | :--- |");
    for (VarkaEmitReach r : reports) {
      md.append(' ').append(r.name).append(" |");
      rule.append(" ---: |");
    }
    md.append(" reached by none |\n").append(rule).append(" :---: |\n");
    for (VarkaEmitMechanism m : VarkaEmitMechanism.values()) {
      md.append("| ").append(m.family.name().toLowerCase(java.util.Locale.ROOT)).append(" | `")
          .append(m.name()).append("`: ").append(m.sentence).append(" |");
      boolean any = false;
      for (VarkaEmitReach r : reports) {
        int n = r.reachedBy(m);
        any |= n > 0;
        md.append(' ').append(n > 0 ? Integer.toString(n) : "-").append(" |");
      }
      md.append(any ? " |" : " never |").append('\n');
    }
    return md.toString();
  }

  /**
   * Writes {@link #markdown} to {@code <dir>/<file>} where the environment variable
   * {@code VARKA_REACH_DUMP} names a directory, and says where; empty where it is unset.
   */
  static java.util.Optional<Path> dump(String file, String title, String provenance,
      List<VarkaEmitReach> reports) throws IOException {
    String dir = System.getenv("VARKA_REACH_DUMP");
    if (dir == null || dir.isEmpty()) {
      return java.util.Optional.empty();
    }
    Path target = Path.of(dir).resolve(file);
    Files.createDirectories(target.getParent());
    Files.writeString(target, markdown(title, provenance, reports));
    return java.util.Optional.of(target);
  }
}
