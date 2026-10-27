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

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;

/**
 * The small facts about one IR node that the compiler's passes ask, answered by the node itself:
 * every record of {@link VarkaVectorIR} implements {@link VarkaVectorIR#props()}, which has no
 * default, so a new node cannot compile until it decides each of them. A pass reads the one it
 * needs instead of keeping a switch over every node type of its own.
 *
 * <p>Each answer reads the node's own fields and lane and nothing else. What depends on the
 * emission's options or on the node's place in a tree is the pass's to work out from these
 * answers: {@link Ring.Arith} says a node is ring arithmetic of a given overflow mode, and the
 * analysis decides from the options whether that mode's check is emitted.
 *
 * @param ring whether reduction modulo a narrow lane's width commutes with the node, which is
 *     what {@code Analysis.isNarrowRing} asks
 * @param validity how the node's validity word follows from its operands' words: whether the
 *     word is a pure function of input bitmaps, and whether a null operand is skipped rather
 *     than propagated
 * @param weight what the node costs against the grouping budgets, in lane operations
 *     ({@code VarkaEmitBudget.weightOf})
 * @param costKind the feature the emit cost model counts the node under
 *     ({@code VarkaEmitCost.kindOf}): the record name and the parts of the node that choose
 *     between lowerings
 * @param fills the restricted operand positions this node may stand in
 *     ({@link VarkaOperandPosition#admits})
 * @param restricted the node's own operands that sit in a restricted position
 *     ({@link VarkaOperandPosition#restrictedOperands})
 */
public record VarkaNodeProps(Ring ring, Validity validity, int weight, String costKind,
    Set<VarkaOperandPosition> fills, List<VarkaOperandPosition.Restricted> restricted) {

  /** For a node that may stand in no restricted position. */
  public static final Set<VarkaOperandPosition> FILLS_NOTHING = Set.of();

  /** For a node none of whose operands is in a restricted position. */
  public static final List<VarkaOperandPosition.Restricted> RESTRICTS_NOTHING = List.of();

  public VarkaNodeProps {
    Objects.requireNonNull(ring, "ring");
    Objects.requireNonNull(validity, "validity");
    Objects.requireNonNull(costKind, "costKind");
    if (weight < 0) {
      throw VarkaErrors.illegalArgument("a node's weight is never negative, got " + weight);
    }
    fills = Set.copyOf(fills);
    restricted = List.copyOf(restricted);
  }

  /**
   * Whether reduction modulo a narrow width commutes with a node. Over sign-extended
   * {@code TINYINT} or {@code SMALLINT} values an int chain of such nodes needs one reduction, at
   * its first reader that is not one ({@code VARKA-329.md} 4.2).
   */
  public sealed interface Ring permits Ring.Never, Ring.Pick, Ring.Arith {
    /** Never a ring node, whatever its lane: a compare, a shift, a conversion, a leaf. */
    Ring NEVER = new Never();
    /** A ring node whenever it is on a narrow lane: a blend only picks lanes. */
    Ring PICK = new Pick();

    /** See {@link #NEVER}. */
    record Never() implements Ring {}

    /** See {@link #PICK}. */
    record Pick() implements Ring {}

    /**
     * Narrow add, subtract, multiply or negate declared {@code mode}: a ring node where the
     * emission does not check its overflow, since a checked node's range compare reads the exact
     * narrow value.
     */
    record Arith(Overflow mode) implements Ring {
      public Arith {
        Objects.requireNonNull(mode, "mode");
      }
    }
  }

  /**
   * How a node's validity word is made. The analysis's word owners
   * ({@code Analysis.ownerOf}), its pure word algebra ({@code Analysis.pureOf}) and the slot
   * planner's word references ({@code Slots.planWordRef}) all read this one answer, so the three
   * agree by construction.
   */
  public sealed interface Validity
      permits Validity.Input, Validity.AllTrue, Validity.Strict, Validity.SkipsNull,
          Validity.Own, Validity.NoWord {
    /** A literal: valid in every lane. */
    Validity ALL_TRUE = new AllTrue();
    /** Null where any operand is null: the AND of its operands' words, or a unary alias. */
    Validity STRICT = new Strict();
    /** Null only where every operand is null: the OR of its operands' words. */
    Validity SKIPS_NULL = new SkipsNull();
    /** A word the node computes from values, not from its operands' words alone. */
    Validity OWN = new Own();
    /** A condition, whose known-true and known-false words stand where a validity word would. */
    Validity NO_WORD = new NoWord();

    /**
     * An input leaf: the validity word of the column at {@code ordinal}. The ordinal is not
     * checked here: the emitter's admission refuses one outside the kernel's inputs with the
     * reason a caller is shown, and a descriptor read first must not pre-empt it.
     */
    record Input(int ordinal) implements Validity {}

    /** See {@link #ALL_TRUE}. */
    record AllTrue() implements Validity {}

    /** See {@link #STRICT}. */
    record Strict() implements Validity {}

    /** See {@link #SKIPS_NULL}. */
    record SkipsNull() implements Validity {}

    /** See {@link #OWN}. */
    record Own() implements Validity {}

    /** See {@link #NO_WORD}. */
    record NoWord() implements Validity {}
  }
}
