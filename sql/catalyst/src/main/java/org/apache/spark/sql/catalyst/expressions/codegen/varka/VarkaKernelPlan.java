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

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.NarrowLane;

/**
 * The lanes of one kernel, decided once from its roots and the emit options (VARKA-488,
 * {@code VARKA-475.md} 3.3): the lane its loop drives at, the lane each node is emitted at, each
 * input's load and the int groups one iteration computes - or why no kernel of these roots can be
 * emitted. The compiler's admission, the budget mirror, the emitter and the compiled projection
 * each worked these out for themselves, and agreed only because each was fixed to agree; the plan
 * is the one place they are decided.
 *
 * <p>A plan with a {@link #refusal()} has no lanes: asking one throws, so no consumer can emit
 * past a refusal. The refusal texts are the compiler's decline reasons, word for word.
 */
public final class VarkaKernelPlan {

  /** Why no kernel of these roots can be emitted, and the root that cannot join the others. */
  public record Refusal(Kind kind, String reason, VarkaVectorIR root) {}

  /** What refused a kernel. */
  public enum Kind {
    /** A narrow value the drive lane cannot lower or this width cannot widen. */
    NARROW,
    /** Two roots no one lane serves: a long root beside a narrow or int one. */
    LANE_MISMATCH
  }

  private final Optional<Refusal> refusal;
  private final LaneType drive;
  private final Lane lane;
  private final int lanes;
  private final boolean mixed;
  private final Set<VarkaVectorIR> storeNarrowings;
  private final Map<Integer, LaneType> inputLanes;
  private final int narrowGroups;

  private VarkaKernelPlan(Optional<Refusal> refusal, LaneType drive, int lanes, boolean mixed,
      Set<VarkaVectorIR> storeNarrowings, Map<Integer, LaneType> inputLanes, int narrowGroups) {
    this.refusal = refusal;
    this.drive = drive;
    this.lane = drive == null ? null : Lane.of(drive);
    this.lanes = lanes;
    this.mixed = mixed;
    this.storeNarrowings = storeNarrowings;
    this.inputLanes = inputLanes;
    this.narrowGroups = narrowGroups;
  }

  /** The plan of a kernel of {@code roots} under {@code options}. */
  public static VarkaKernelPlan of(List<VarkaVectorIR> roots, VarkaEmitOptions options) {
    if (roots.isEmpty()) {
      throw VarkaErrors.illegalArgument("no output chains to plan");
    }
    Optional<LaneType> kernel = VarkaVectorIR.kernelLane(roots, options);
    Optional<Refusal> narrow = narrowRefusal(roots, kernel, options);
    if (narrow.isPresent()) {
      return refused(narrow.get());
    }
    if (kernel.isEmpty()) {
      return refused(laneMismatch(roots, options));
    }
    LaneType drive = kernel.get();
    Lane lane = Lane.of(drive);
    int lanes = Lane.emitLanes(options, lane);

    // Every distinct node, and every node some other node reads: a narrowing root that nothing
    // else reads narrows at its store and stays in the long lane until then.
    Set<VarkaVectorIR> nodes = new HashSet<>();
    Set<VarkaVectorIR> operands = new HashSet<>();
    Map<Integer, LaneType> inputLanes = new HashMap<>();
    var todo = new ArrayDeque<VarkaVectorIR>(roots);
    while (!todo.isEmpty()) {
      VarkaVectorIR node = todo.pop();
      if (!nodes.add(node)) {
        continue;
      }
      if (node instanceof ColumnRef c) {
        inputLanes.putIfAbsent(c.ordinal(), c.lane());
      }
      for (VarkaVectorIR child : VarkaVectorIR.childrenOf(node)) {
        operands.add(child);
        todo.push(child);
      }
    }
    Set<VarkaVectorIR> storeNarrowings = new HashSet<>();
    for (VarkaVectorIR root : roots) {
      if (root instanceof NarrowLane && !operands.contains(root)) {
        storeNarrowings.add(root);
      }
    }
    boolean mixed = false;
    if (lane == Lane.LONG) {
      for (VarkaVectorIR node : nodes) {
        mixed |= node.laneType() == LaneType.INT && !storeNarrowings.contains(node);
      }
    }
    return new VarkaKernelPlan(Optional.empty(), drive, lanes, mixed, Set.copyOf(storeNarrowings),
        Map.copyOf(inputLanes), narrowGroups(roots, lane, lanes));
  }

  private static VarkaKernelPlan refused(Refusal refusal) {
    return new VarkaKernelPlan(Optional.of(refusal), null, 0, false, Set.of(), Map.of(), 0);
  }

  // -------------------------------------------------------------------------------------------
  // The refusals
  // -------------------------------------------------------------------------------------------

  /**
   * Why this kernel cannot be emitted over the narrow values it holds, judged at the kernel's
   * lane, or at each root's own where no lane serves them all (the lane check refuses that on
   * its own).
   *
   * <p>Under {@link VarkaEmitOptions#narrowLanes()} a kernel whose every value is narrow computes
   * in the narrow species itself, so nothing has to be converted. Otherwise a narrow column is
   * read with one widening load per iteration, and that load needs a narrow species wider than
   * or as wide as the int group: the narrow species of the int species' width where it exists,
   * and otherwise the next width up, whose parts each become one int group
   * ({@link Lane#widenLanes}). A width with no baked lane count - {@code validityByWidth} off -
   * cannot name the species at all, and the entry declines: the row engine answers it.
   */
  private static Optional<Refusal> narrowRefusal(List<VarkaVectorIR> roots,
      Optional<LaneType> kernel, VarkaEmitOptions options) {
    for (VarkaVectorIR root : roots) {
      Set<LaneType> lanes = VarkaVectorIR.lanesIn(root);
      boolean narrow = lanes.contains(LaneType.BYTE) || lanes.contains(LaneType.SHORT);
      if (!narrow) {
        continue;
      }
      LaneType drive = kernel.orElseGet(() -> VarkaVectorIR.emissionLane(root, options));
      if (drive.isNarrow()) {
        continue;
      }
      if (drive == LaneType.LONG) {
        // Analysis admits no narrow node in a long-lane emission, not even one read only for
        // its validity word, so the entry declines here rather than failing at emit.
        return Optional.of(new Refusal(Kind.NARROW,
            "a narrow value under a bigint kernel has no lowering", root));
      }
      if (!VarkaVectorIR.needsNarrowSpecies(root)) {
        // A narrow column whose only use is a validity word - `y1 IS NOT NULL` - reads no
        // value and needs no narrow species, so it emits at this int lane count. Refusing it
        // would name a load the tree never emits.
        continue;
      }
      int intLanes = Lane.emitLanes(options, Lane.INT);
      if (intLanes == 0) {
        return Optional.of(new Refusal(Kind.NARROW,
            "a narrow column needs a baked int lane count to be widened", root));
      }
      for (LaneType type : lanes) {
        if (type.isNarrow() && Lane.widenLanes(intLanes, Lane.of(type)) == 0) {
          return Optional.of(new Refusal(Kind.NARROW,
              "a " + type + " column has no widening load at " + intLanes + " int lanes", root));
        }
      }
    }
    return Optional.empty();
  }

  /**
   * The refusal of roots no one lane serves: the first root that no lane serves beside the roots
   * before it, which is the entry the compiler is admitting when it asks, named with its own lane
   * and the lane of the kernel it would join.
   */
  private static Refusal laneMismatch(List<VarkaVectorIR> roots, VarkaEmitOptions options) {
    for (int k = 1; k < roots.size(); k++) {
      if (VarkaVectorIR.kernelLane(roots.subList(0, k + 1), options).isEmpty()) {
        VarkaVectorIR entry = roots.get(k);
        LaneType kernel = VarkaVectorIR.kernelLane(roots.subList(0, k), options).orElseThrow();
        return new Refusal(Kind.LANE_MISMATCH, "the " + VarkaVectorIR.emissionLane(entry, options)
            + " lane in a kernel on the " + kernel + " lane: one kernel holds one lane",
            entry);
      }
    }
    throw VarkaErrors.illegalState("no lane serves " + roots + ", and every prefix has one");
  }

  /**
   * The lanes roots no one lane serves would each drive at, for the emitter's refusal of a
   * caller that hands it such roots: the compiler declines them before they reach it.
   */
  static String mixedLanes(List<VarkaVectorIR> roots, VarkaEmitOptions options) {
    return "outputs mix lanes: " + roots.stream()
        .map(o -> VarkaVectorIR.emissionLane(o, options).toString()).distinct()
        .collect(java.util.stream.Collectors.joining(" and "));
  }

  // -------------------------------------------------------------------------------------------
  // The lanes
  // -------------------------------------------------------------------------------------------

  /**
   * How many int lane groups one iteration computes: the widest part count any narrow value the
   * roots read loads at, so that a kernel driven at four int lanes reads a byte column as an
   * eight-lane 64-bit species and converts it in two parts. One for a kernel not driven at the
   * int lane, or with no baked lane count.
   */
  private static int narrowGroups(List<VarkaVectorIR> roots, Lane lane, int lanes) {
    if (lane != Lane.INT || lanes == 0) {
      return 1;
    }
    int groups = 1;
    for (VarkaVectorIR root : roots) {
      for (LaneType type : VarkaVectorIR.narrowValuesIn(root)) {
        groups = Math.max(groups, Lane.widenLanes(lanes, Lane.of(type)) / lanes);
      }
    }
    return groups;
  }

  /** Why no kernel of these roots can be emitted, or empty when one can. */
  public Optional<Refusal> refusal() {
    return refusal;
  }

  /** The lane the kernel's loop drives at, which picks the {@code run} overload. */
  public LaneType drive() {
    requireEmittable();
    return drive;
  }

  /** {@link #drive()} as the emitter's lane: the species, descriptors and helpers it names. */
  Lane lane() {
    requireEmittable();
    return lane;
  }

  /** The lane count baked for the drive lane's species, or 0 to read it at run time. */
  int lanes() {
    requireEmittable();
    return lanes;
  }

  /** Whether the kernel converts between lanes: a long-driven kernel holding int values. */
  boolean mixed() {
    requireEmittable();
    return mixed;
  }

  /**
   * Whether {@code node} is a narrowing that stays in the long lane until its root's store: an
   * output root that no other node reads.
   */
  boolean narrowsAtStore(VarkaVectorIR node) {
    requireEmittable();
    return storeNarrowings.contains(node);
  }

  /**
   * The lane {@code node}'s value is emitted in: the drive lane for every node of a single-lane
   * kernel, and in a kernel that converts between lanes the node's own, a narrowing at its store
   * excepted.
   */
  Lane laneOf(VarkaVectorIR node) {
    requireEmittable();
    if (!mixed || storeNarrowings.contains(node)) {
      return lane;
    }
    return Lane.of(node.laneType());
  }

  /** The lane input {@code ordinal} is read at, which sizes its data segment. */
  Lane inputLane(int ordinal) {
    requireEmittable();
    LaneType laneType = inputLanes.get(ordinal);
    return laneType == null ? lane : Lane.of(laneType);
  }

  /**
   * The lane count baked for {@code of}'s species, or 0 where none is: {@link #lanes()} for the
   * drive lane, and for the int lane of a kernel driven at the long one the int species of the
   * same width, twice the long lanes.
   */
  int lanesOf(Lane of) {
    requireEmittable();
    if (of == lane) {
      return lanes;
    }
    return lanes == 0 ? 0 : lanes * 2;
  }

  /** The int groups one iteration computes; see {@link #narrowGroups(List, Lane, int)}. */
  int narrowGroups() {
    requireEmittable();
    return narrowGroups;
  }

  private void requireEmittable() {
    if (refusal.isPresent()) {
      throw VarkaErrors.illegalState("a refused kernel has no lanes: " + refusal.get().reason());
    }
  }
}
