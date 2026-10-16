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
package org.apache.spark.sql.catalyst.expressions.codegen;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;

import org.apache.spark.sql.catalyst.expressions.Add;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.EvalMode$;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Greatest;
import org.apache.spark.sql.catalyst.expressions.Least;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Multiply;
import org.apache.spark.sql.catalyst.expressions.RuntimeReplaceable;
import org.apache.spark.sql.catalyst.expressions.Subtract;
import org.apache.spark.sql.catalyst.expressions.UnaryMinus;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaDerivedKind;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaErrors;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaRangeAnalysis;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaOperandPosition;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntArith;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntNeg;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.TimeType;
import org.apache.spark.sql.types.YearMonthIntervalType;

/**
 * The recursive node compiler of the Varka expression compiler: the chain of families that
 * lowers one Catalyst expression to the vector IR, and the operand helpers the families share.
 * The classifier in {@code VarkaExpressionCompiler} decides which projection entries to compile
 * and under what budgets; this class compiles one tree.
 *
 * <p>{@link #compileNode} asks each family of {@link #FAMILIES}, in order, whether it claims the
 * node, and runs the first claimant's lowering. A claim has no side effect (a family's
 * {@code arm} tests and returns a deferred call), so the order is not what decides a node's
 * compiler: no expression is claimed by two families, every arm being gated by the expression
 * class or its data type, and {@code VarkaFamilyChainSuite} holds that as a fact over the
 * coverage table, so a family added here or a widened guard that broke it fails there rather than
 * winning silently by coming first. An empty result anywhere fails the enclosing entry, whose
 * caller rolls the tables back to their pre-entry state.
 *
 * <p>The input and literal tables are the classifier's {@link VarkaSlotTable}s: the key is a
 * child ordinal (or a literal value), the slot its dense index.
 */
final class VarkaNodeCompiler {

  /**
   * The most literals an {@code IN} list may hold and still fuse, counted after dedup. The basis,
   * recorded in {@code VARKA-20.md}: 16 is depth-safe under any fold shape
   * ({@code MAX_CHAIN_DEPTH} = 16 while the balanced chain here is {@code ceil(log2 16) + 1} = 5
   * levels), and its 31 op nodes left half the emitter's {@code MAX_FUSED_NODES} = 64 budget to
   * the rest of the projection when that cap bounded every kernel; under the byte budget it
   * bounds only the reference form, and the choice of 16 stands on the depth argument. (The
   * emitter's broadcast hoist is NOT part of the basis: its gate counts the kernel's total
   * literal slots, so a capped IN plus any other literal already re-broadcasts inline - the
   * review pass corrected an earlier claim here.) Above the cap the entry declines with a reason
   * instead of silently losing the whole kernel at emission.
   */
  static final int MAX_IN_LITERALS = 16;

  /** Tests whether a family claims a node; the claim returns its lowering, deferred. */
  @FunctionalInterface
  interface Claim {
    VarkaFamilyArm of(
        Expression e,
        VarkaSlotTable inputs,
        VarkaSlotTable literals,
        DeclineSink sink);
  }

  /** A named link of the chain. */
  record Family(String name, Claim claim) {
  }

  /**
   * The chain, in order: the date leaves, the four families, then the int arithmetic and the
   * picks. The int arithmetic comes after the calendar family so that the {@code Add(WeekDay, 1)}
   * shape keeps its dedicated node, and its guard says so as well.
   */
  private static final List<Family> FAMILIES = List.of(
      new Family("date leaves", VarkaNodeCompiler::leafArm),
      new Family("calendar", VarkaChronoCompiler::arm),
      new Family("interval", VarkaIntervalCompiler::arm),
      new Family("time", VarkaTimeCompiler::arm),
      new Family("condition", VarkaConditionCompiler::arm),
      new Family("int arithmetic", VarkaNodeCompiler::arithmeticArm));

  private VarkaNodeCompiler() {
  }

  /**
   * Compiles {@code expr}. Shapes that cannot be served stay unclaimed by construction: a
   * {@code date_add} over a {@code datediff} result only type-checks through a {@code Cast},
   * which compiles to nothing here. Integer arithmetic over a {@code datediff} result is
   * admitted by the int32 lowering; a SIMD lane still cannot throw row-accurately, so an ANSI
   * overflow condemns the batch and the row engine raises it instead.
   */
  static Optional<VarkaVectorIR> compileNode(
      Expression expr,
      VarkaSlotTable inputs,
      VarkaSlotTable literals,
      DeclineSink sink) {
    for (Family family : FAMILIES) {
      VarkaFamilyArm arm = family.claim().of(expr, inputs, literals, sink);
      if (arm != null) {
        return arm.compile();
      }
    }
    return fallback(expr, inputs, literals, sink);
  }

  /** The chain, for the suite that holds no node is claimed twice. */
  static List<Family> families() {
    return FAMILIES;
  }

  /** The names of the families of {@code chain} that claim {@code expr}, in chain order. */
  static List<String> claimants(
      List<Family> chain,
      Expression expr,
      VarkaSlotTable inputs,
      VarkaSlotTable literals,
      DeclineSink sink) {
    var names = new ArrayList<String>();
    for (Family family : chain) {
      if (family.claim().of(expr, inputs, literals, sink) != null) {
        names.add(family.name());
      }
    }
    return names;
  }

  /**
   * The date leaves: a date column, a date literal and the identity date cast. First in the
   * chain, ahead of every family.
   */
  private static VarkaFamilyArm leafArm(
      Expression e,
      VarkaSlotTable inputs,
      VarkaSlotTable literals,
      DeclineSink sink) {
    return switch (e) {
      case BoundReference br when DataTypes.DateType.equals(br.dataType()) ->
          () -> Optional.of(columnRef(br, inputs));
      // A date literal's value is already an epoch-day int, so it takes a slot in the shared
      // per-distinct-value table like a folded day offset does - what makes
      // `d < DATE'...'` and `greatest(d, DATE'...')` reachable at all. A null-valued Literal is
      // not claimed; that is a safe blind spot, not a bug, since ConstantFolding removes a null
      // date literal from any real query before it can reach here (unix_date/date_from_unix_date
      // add two more recursive paths into this same match, both equally covered by that
      // guarantee).
      case Literal l when l.value() instanceof Integer days
          && DataTypes.DateType.equals(l.dataType()) ->
          () -> Optional.of(intSlot(days, literals));
      // The identity cast: the corpus wraps date expressions in `CAST(... AS DATE)`
      // 85 times, and after optimization the wrapper is a no-op over an already-date child -
      // unwrap it. A `cast(<string literal> AS DATE)` never reaches here (constant-folded to a
      // date literal by the optimizer); a string *column* cast is a per-row parse with no
      // string lane and stays declined.
      case Cast c when DataTypes.DateType.equals(c.dataType())
          && DataTypes.DateType.equals(c.child().dataType()) ->
          () -> compileNode(c.child(), inputs, literals, sink);
      default -> null;
    };
  }

  /**
   * The int32 arithmetic over int-valued operands and the null-skipping picks, after the
   * families: the {@code Add(WeekDay, 1)} shape keeps its dedicated calendar node because the
   * calendar arms come first in the chain.
   */
  private static VarkaFamilyArm arithmeticArm(
      Expression e,
      VarkaSlotTable inputs,
      VarkaSlotTable literals,
      DeclineSink sink) {
    return switch (e) {
      // Spark's greatest/least are n-ary; the null-skipping algebra is associative, so a left
      // fold into the binary IR nodes is exact.
      case Greatest g -> () -> VarkaConditionCompiler.foldPick(
          g.children(), inputs, literals, sink, VarkaNodeCompiler::greatest);
      case Least l -> () -> VarkaConditionCompiler.foldPick(
          l.children(), inputs, literals, sink, VarkaNodeCompiler::least);
      // extract(DAYOFWEEK_ISO) / date_part('DOW_ISO'): the analyzer spells them Add(WeekDay(d), 1),
      // and so does a hand-written weekday(d) + 1. One narrow arm, either operand order, and
      // nothing else: integer arithmetic over an output is out of scope for this compiler. Int32
      // arithmetic over int-valued operands - a fused field, an IntegerType column, an int
      // literal, or nested arithmetic. Placed after the `Add(WeekDay, 1)` arm of the calendar
      // family so that shape keeps its cheaper dedicated node.
      case Add a when isInt(a) && !VarkaChronoCompiler.isDayOfWeekIso(a) ->
          () -> intArith(IntOp.ADD, a.evalMode(), a.left(), a.right(), a, inputs, literals, sink);
      case Subtract a when isInt(a) ->
          () -> intArith(IntOp.SUB, a.evalMode(), a.left(), a.right(), a, inputs, literals, sink);
      case Multiply a when isInt(a) ->
          () -> intArith(IntOp.MUL, a.evalMode(), a.left(), a.right(), a, inputs, literals, sink);
      case UnaryMinus n when isInt(n) ->
          // Spark has no try_negative, so the mode is only ever WRAP or FAIL here. Negation
          // overflows on exactly one value, `Int.MinValue`, so any bound at all rules it out and
          // the check comes off - the same reasoning the binary arms use, on a narrower fact.
          () -> intOperand(n.child(), inputs, literals, sink).map(x -> {
            boolean checked = n.failOnError() && !boundedByIntMax(x, literals);
            return (VarkaVectorIR) new IntNeg(checked ? Overflow.FAIL : Overflow.WRAP, x);
          });
      default -> null;
    };
  }

  private static VarkaVectorIR greatest(VarkaVectorIR left, VarkaVectorIR right) {
    return new VarkaVectorIR.Greatest(left, right);
  }

  private static VarkaVectorIR least(VarkaVectorIR left, VarkaVectorIR right) {
    return new VarkaVectorIR.Least(left, right);
  }

  private static boolean isInt(Expression e) {
    return DataTypes.IntegerType.equals(e.dataType());
  }

  /**
   * What no arm claimed: a column of another type declines by its type, a
   * {@code RuntimeReplaceable} compiles what would run, and anything else declines as
   * unsupported.
   */
  private static Optional<VarkaVectorIR> fallback(
      Expression e,
      VarkaSlotTable inputs,
      VarkaSlotTable literals,
      DeclineSink sink) {
    // A column of any other type: eligible to be forwarded as a whole entry, never to be read by
    // the int32 lanes of a kernel.
    if (e instanceof BoundReference br) {
      return sink.decline("non-date column of type " + br.dataType().simpleString(), br);
    }
    // Defensive: a real query never carries an unreplaced RuntimeReplaceable this far (the
    // optimizer's ReplaceExpressions runs long before physical planning), but hand-built
    // expressions in tests and the plan-side fusion report can - compile what would run.
    if (e instanceof RuntimeReplaceable r) {
      return compileNode(r.replacement(), inputs, literals, sink);
    }
    return sink.decline("unsupported expression", e);
  }

  /**
   * The lane a Spark type's values occupy in a kernel, or empty for a type no kernel reads. The
   * int side is what the leaf arms admit - a date, an int and a year-month interval are all one
   * 32-bit lane - and the long side is milestone 5's: {@code bigint}, {@code TIME} (nanoseconds
   * of day) and a day-time interval (microseconds) are one 64-bit lane ({@code VARKA-29.md} 3.1).
   * The two timestamp types are that lane physically and are deliberately absent.
   */
  static Optional<LaneType> laneOf(DataType dataType) {
    if (DataTypes.IntegerType.equals(dataType) || DataTypes.DateType.equals(dataType)
        || dataType instanceof YearMonthIntervalType) {
      return Optional.of(LaneType.INT);
    }
    if (DataTypes.LongType.equals(dataType) || dataType instanceof TimeType
        || dataType instanceof DayTimeIntervalType) {
      return Optional.of(LaneType.LONG);
    }
    return Optional.empty();
  }

  /** Whether {@code dataType} is one of the types the 64-bit lane carries. */
  static boolean onLongLane(DataType dataType) {
    return laneOf(dataType).filter(lane -> lane == LaneType.LONG).isPresent();
  }

  /**
   * Spark's evaluation mode ({@code EvalMode.Value}, typed {@code Object} because scaladoc, which
   * Maven runs over the Java sources, cannot resolve a type nested in a Scala object) as the IR
   * spells it.
   */
  static Overflow overflowOf(Object mode) {
    var modes = EvalMode$.MODULE$;
    if (modes.LEGACY().equals(mode)) {
      return Overflow.WRAP;
    }
    if (modes.ANSI().equals(mode)) {
      return Overflow.FAIL;
    }
    if (modes.TRY().equals(mode)) {
      return Overflow.NULL;
    }
    throw VarkaErrors.illegalArgument("unknown evaluation mode " + mode);
  }

  /**
   * An operand of int arithmetic: an {@code IntegerType} column becomes the int column leaf, an
   * int literal a slot, and everything else goes through {@link #compileNode} - which yields the
   * fused int fields ({@code datediff}, the extractions, the ISO weekday) and nested arithmetic.
   * A {@code DateType} operand is refused here rather than silently treated as a day count:
   * {@code date + 1} is {@code DateAdd} and has its own arm.
   */
  static Optional<VarkaVectorIR> intOperand(
      Expression e,
      VarkaSlotTable inputs,
      VarkaSlotTable literals,
      DeclineSink sink) {
    return intOperand(e, () -> "int arithmetic operand of type " + e.dataType().simpleString(),
        inputs, literals, sink);
  }

  /**
   * An int operand of a node that is not a day: a foldable int literal as a slot, a bare
   * {@code IntegerType} column as a column ref, and any other {@code IntegerType} expression
   * through {@link #compileNode}, which is where the fused int fields and the arithmetic arms
   * live. So {@code make_date(y + 1, m, d)} fuses, and an operand of the wrong type still
   * declines here with {@code position} in the reason rather than reaching an arm that would read
   * it as an int. The date column leaf of {@link #compileNode} is {@code DateType}-only, which is
   * why the two leaves here cannot be left to it.
   */
  static Optional<VarkaVectorIR> compileIntOperand(
      Expression e,
      String position,
      VarkaSlotTable inputs,
      VarkaSlotTable literals,
      DeclineSink sink) {
    return intOperand(e, () -> position + " is not an int column or literal", inputs, literals,
        sink);
  }

  /**
   * The rule both int operand forms share (VARKA-86): the int leaf, and any other
   * {@code IntegerType} expression through {@link #compileNode}; {@code notInt} is the position's
   * own reason for an operand of another type, built only when it declines.
   */
  private static Optional<VarkaVectorIR> intOperand(
      Expression e,
      Supplier<String> notInt,
      VarkaSlotTable inputs,
      VarkaSlotTable literals,
      DeclineSink sink) {
    Optional<VarkaVectorIR> leaf = intLeaf(e, inputs, literals);
    if (leaf.isPresent()) {
      return leaf;
    }
    if (!DataTypes.IntegerType.equals(e.dataType())) {
      return sink.decline(notInt.get(), e);
    }
    return compileNode(e, inputs, literals, sink);
  }

  /**
   * The int leaf, the one rule every int operand position starts from: an int literal as a slot,
   * a bare {@code IntegerType} column as an int column ref; empty for anything else. It is not
   * {@link #compileNode}'s leaf, which is {@code DateType}: a bare int has no meaning as a date
   * operand, and widening that leaf would admit {@code date_add(d, i)}'s offset as a date.
   */
  static Optional<VarkaVectorIR> intLeaf(
      Expression e, VarkaSlotTable inputs, VarkaSlotTable literals) {
    if (e instanceof Literal l && l.value() instanceof Integer v
        && DataTypes.IntegerType.equals(l.dataType())) {
      return Optional.of(intSlot(v, literals));
    }
    if (e instanceof BoundReference br && DataTypes.IntegerType.equals(br.dataType())) {
      return Optional.of(columnRef(br, inputs));
    }
    return Optional.empty();
  }

  /**
   * {@code built} where {@code position} takes it, and otherwise a decline with {@code reason}:
   * the compiler's half of the rule the emitter's {@code Analysis.requireOperand} checks, so an
   * operand the emitter would refuse never leaves the compiler as fused (VARKA-86).
   */
  static Optional<VarkaVectorIR> admitted(
      VarkaOperandPosition position, VarkaVectorIR built, Expression e, String reason,
      DeclineSink sink) {
    return position.admits(built) ? Optional.of(built) : sink.decline(reason, e);
  }

  /**
   * The literal table as {@link VarkaRangeAnalysis} reads it: slot index to value. The table is
   * keyed by value in slot order, so a slot's value is its key's position; and it is read at call
   * time, never snapshotted, because the table grows as compilation proceeds and is truncated on
   * every decline.
   */
  static IntUnaryOperator literalAt(VarkaSlotTable literals) {
    return literals::keyAt;
  }

  /**
   * How large an int-valued node's result can be in absolute value, or empty where nothing
   * bounds it: {@link VarkaRangeAnalysis}'s {@code INT} query. This exists so a checked operation
   * that provably cannot overflow needs no check - which is what makes
   * {@code year(d) * 100 + month(d)} fuse under ANSI, the shape {@code VARKA-63.md} 6 measures.
   * The compiler can do this and the emitter cannot: a {@code LiteralSlot} carries a slot index,
   * and the value behind it only arrives in {@code scalarArgs} at run time. Conservative by
   * construction: an empty answer costs a check or a decline and never a wrong answer.
   */
  static OptionalLong magnitude(VarkaVectorIR node, VarkaSlotTable literals) {
    return VarkaRangeAnalysis.magnitude(node, literalAt(literals));
  }

  /** Whether {@code node}'s magnitude is known and at most {@code Int.MaxValue}. */
  static boolean boundedByIntMax(VarkaVectorIR node, VarkaSlotTable literals) {
    OptionalLong m = magnitude(node, literals);
    return m.isPresent() && m.getAsLong() <= Integer.MAX_VALUE;
  }

  /**
   * Whether the operation on operands of these bounds cannot leave the int32 range. Read through
   * the analysis's own {@code IntArith} transfer function rather than re-dispatched here: the
   * candidate node is never emitted, so building one to ask the question is free, and the two
   * answers cannot drift apart the way two copies of "MUL multiplies, else adds" once could. A
   * magnitude is non-negative by construction, so the only thing left to ask is whether it stays
   * at or under {@code Int.MaxValue} - one past it, {@code 2^31}, is the first magnitude that
   * overflows.
   */
  private static boolean cannotOverflow(
      IntOp op, VarkaVectorIR l, VarkaVectorIR r, VarkaSlotTable literals) {
    return boundedByIntMax(new IntArith(op, Overflow.WRAP, l, r), literals);
  }

  /**
   * The shared body of the three binary arithmetic arms. A checked multiply declines unless the
   * operands' bounds prove it cannot overflow: the overflow test for {@code *} needs the 64-bit
   * product or a lane division, and the emitter has neither in int lanes, so an unprovable
   * {@code ANSI} or {@code TRY} multiply stays on the row engine until milestone 5's long lanes
   * arrive ({@code VARKA-63.md} 3.4).
   */
  private static Optional<VarkaVectorIR> intArith(
      IntOp op,
      Object mode,
      Expression l,
      Expression r,
      Expression whole,
      VarkaSlotTable inputs,
      VarkaSlotTable literals,
      DeclineSink sink) {
    int mark = literals.size();
    Optional<VarkaVectorIR> x = intOperand(l, inputs, literals, sink);
    if (x.isEmpty()) {
      return x;
    }
    Optional<VarkaVectorIR> y = intOperand(r, inputs, literals, sink);
    if (y.isEmpty()) {
      return y;
    }
    return arithOver(op, overflowOf(mode), x.get(), y.get(), whole, literals, mark, sink);
  }

  /**
   * The overflow decision, shared by the int arithmetic arms and the interval ones so that the
   * bound rule and the checked-multiply refusal are stated once rather than in two places that
   * can drift. A checked operation whose operands' bounds rule out overflow needs no check at all
   * and emits as {@code WRAP} - fewer ops, and the only way a checked multiply fuses; an int lane
   * has no cheap overflow test for {@code *}, so one that keeps its check declines and
   * {@code literals} is rolled back to {@code mark} so a declining entry leaves no slot behind.
   */
  static Optional<VarkaVectorIR> arithOver(
      IntOp op,
      Overflow declared,
      VarkaVectorIR x,
      VarkaVectorIR y,
      Expression whole,
      VarkaSlotTable literals,
      int mark,
      DeclineSink sink) {
    Overflow overflow = declared != Overflow.WRAP && cannotOverflow(op, x, y, literals)
        ? Overflow.WRAP
        : declared;
    if (op == IntOp.MUL && overflow != Overflow.WRAP) {
      literals.truncate(mark);
      return sink.decline("checked int multiply whose operands do not rule out overflow", whole);
    }
    return Optional.of(new IntArith(op, overflow, x, y));
  }

  /**
   * Interns {@code value} into the per-distinct-value literal table and wraps it as a
   * {@code LiteralSlot} on the int lane. Every folded constant the compiler admits is an int - a
   * day count, a month count, a date's epoch day - so this is the one place a literal's lane is
   * chosen, as {@link #columnRef} is for a column's.
   */
  static LiteralSlot intSlot(int value, VarkaSlotTable literals) {
    return new LiteralSlot(literals.intern(value), LaneType.INT);
  }

  /**
   * Interns {@code br}'s ordinal into {@code inputs} and wraps it as a {@code ColumnRef} on the
   * int lane; shared by the date, interval and long leaves and the offset's {@code IntegerType}
   * one.
   */
  static ColumnRef columnRef(BoundReference br, VarkaSlotTable inputs) {
    return columnRef(br, inputs, LaneType.INT);
  }

  /** {@link #columnRef(BoundReference, VarkaSlotTable)} on {@code lane}. */
  static ColumnRef columnRef(
      BoundReference br, VarkaSlotTable inputs, LaneType lane) {
    return new ColumnRef(inputs.intern(br.ordinal()), lane);
  }

  /**
   * {@code columnRef}'s twin for an input the evaluator derives from {@code br}: interned under
   * {@code VarkaDerivedInput.key} beside the child ordinals, so it takes the next kernel input
   * index and shares the table's rollback.
   */
  static ColumnRef derivedRef(
      BoundReference br, VarkaDerivedKind kind, VarkaSlotTable inputs) {
    return new ColumnRef(inputs.intern(VarkaDerivedInput.key(br.ordinal(), kind)), LaneType.INT);
  }
}
