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

package org.apache.spark.sql.catalyst.expressions.codegen

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Abs, Add, BoundReference, Cast, Expression,
  ExtractANSIIntervalMonths, ExtractANSIIntervalYears, Literal, MakeYMInterval, MultiplyYMInterval,
  Subtract, UnaryMinus}
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler._
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.{Compare, CompareOp,
  ConstDivide, IfElse, IntNeg, IntOp, LiteralSlot, Overflow}
import org.apache.spark.sql.types.{IntegerType, YearMonthIntervalType}

/**
 * The year-month interval family of the compiler: the interval column and literal leaves, the casts
 * between an interval and an int and between two interval units, and the interval algebra - add,
 * subtract, negate, `abs`, `make_ym_interval`, the extracts and the multiply - which are the int32
 * arithmetic nodes with an interval-typed output and every one of them checked, since Spark
 * computes them with the exact methods in every evaluation mode.
 *
 * The arms are a partial function `VarkaExpressionCompiler.compileNode` chains after the calendar
 * family's; the shared operand helpers and the recursion come from `VarkaExpressionCompiler`
 * through its import.
 */
private[codegen] object VarkaIntervalCompiler {

  /**
   * The year-month interval arms of `compileNode`, in their original order.
   */
  private[codegen] def arms(
      inputs: mutable.LinkedHashMap[Int, Int],
      literals: mutable.LinkedHashMap[Int, Int],
      sink: DeclineSink): PartialFunction[Expression, Option[VarkaVectorIR]] = {
    // A year-month interval column, on the same lane. Its value is a count of months in every unit,
    // so nothing about the lowering changes; what makes widening the leaf safe rather than "do not
    // open it wider" is that Spark's own typing decides where the value may appear. An interval
    // only type-checks into DateAddYMInterval, the ordered comparisons and IN, the same-typed
    // Least/Greatest/Coalesce/If/CaseWhen, and Cast - never into date_add's offset, datediff, a
    // calendar extraction or AddMonths' date operand, all of which are typed DateType or
    // IntegerType. So an interval in a date position is a type error the analyzer rejected before
    // the compiler ran, and the leaf cannot put one there.
    case br: BoundReference if br.dataType.isInstanceOf[YearMonthIntervalType] =>
      Some(columnRef(br, inputs))
    // The interval literal, beside the date literal and for the same reason: the value is
    // already the int the lane holds, so `ym > INTERVAL '6' MONTH` and
    // `coalesce(ym, INTERVAL '0' MONTH)` become a slot rather than a decline.
    case Literal(months: Int, _: YearMonthIntervalType) =>
      Some(intSlot(months, literals))
    // The interval relabels, on `unix_date`'s pattern above: a cast that returns its operand
    // unchanged is the child alone, with no node emitted. `intToYearMonthInterval` returns `v` for
    // a MONTH end field and `yearMonthIntervalToInt` returns `v` for a MONTH-ended interval, so
    // both directions of the MONTH unit are the identity on the lane; only the Spark type on the
    // outside differs, and that rides on `outputTypes`. The YEAR unit is neither direction's
    // identity - it multiplies or divides by twelve. Its outbound half is the arm below; its
    // inbound half, `CAST(ym AS INT)` over a YEAR-ended interval, is a division by twelve, which is
    // not supported yet - it belongs with the year-month extracts.
    case Cast(child, YearMonthIntervalType(_, YearMonthIntervalType.MONTH), _, _)
        if child.dataType == IntegerType =>
      compileIntOperand(child, "the month count", inputs, literals, sink)
    // The unit relabel between two year-month intervals, which is not a cast a user writes but the
    // one type coercion inserts whenever two units meet - `ymm + ymy` widens both operands to YEAR
    // TO MONTH before the add. `Cast.castToYearMonthInterval` computes
    // `periodToMonths(monthsToPeriod(v), endField)`, which splits the count into whole years and a
    // remainder and puts it back together: exactly `v` again for a MONTH end field, at every int
    // including `Int.MinValue`, since the reassembly's `multiplyExact` is over `v / 12`. So this
    // direction emits nothing and only `outputTypes` moves. The YEAR-ended direction drops the
    // remainder, which is a division by twelve, and declines below.
    case Cast(child, YearMonthIntervalType(_, YearMonthIntervalType.MONTH), _, _)
        if child.dataType.isInstanceOf[YearMonthIntervalType] =>
      compileNode(child, inputs, literals, sink)
    // `CAST(i AS INTERVAL YEAR)` in a value position, which is `12 * i` with an interval output.
    // `IntervalUtils.intToYearMonthInterval` computes it with `Math.multiplyExact` whatever the
    // session's ANSI mode, so the multiply is checked and the bound is the only thing that removes
    // it. This is the same expression `compileMonths` admits in `add_months`' month-count position;
    // the difference is that there the emitter has a shape check to satisfy and here it has none,
    // which is why `PLAN_TASK_67.md` 2.1 - written about `compileMonths` - reads as if the whole
    // cast were blocked when only that position was.
    case c @ Cast(child, YearMonthIntervalType(YearMonthIntervalType.YEAR,
        YearMonthIntervalType.YEAR), _, _) if child.dataType == IntegerType =>
      val mark = literals.size
      val built = for {
        x <- intOperand(child, inputs, literals, sink)
        r <- arithOver(IntOp.MUL, Overflow.FAIL, x, twelve(literals), c, literals, mark, sink)
      } yield r
      if (built.isEmpty) truncate(literals, mark)
      built
    // The truncating half of the pair above, named rather than left to the generic decline:
    // narrowing a year-month interval to a YEAR-ended unit keeps only the whole years, which is `v
    // - v % 12` and so a division. Type coercion never produces this - it widens the end field - so
    // it reaches here only from a cast the user wrote, and it is not supported yet, belonging with
    // `extract(YEAR FROM ym)` and `ym / k`.
    case c @ Cast(child, YearMonthIntervalType(_, YearMonthIntervalType.YEAR), _, _)
        if child.dataType.isInstanceOf[YearMonthIntervalType] =>
      sink.note("year-month interval narrowed to a YEAR-ended unit, which divides by twelve", c)
      None
    case Cast(child, IntegerType, _, _)
        if child.dataType == YearMonthIntervalType(YearMonthIntervalType.MONTH,
          YearMonthIntervalType.MONTH) =>
      compileNode(child, inputs, literals, sink)
    // Group A: the year-month interval algebra, on the int32 arithmetic nodes with an
    // interval-typed output. The int arms above keep their `IntegerType` gate and these are
    // siblings rather than a widening of it, because int arithmetic is an int-typed concept and a
    // widened gate is how an interval reaches a position that reads it as a day count. Every one of
    // them is checked in every evaluation mode - Spark computes them with `addExact`,
    // `subtractExact`, `negateExact` and `multiplyExact`, and there is no `LEGACY` or `try_`
    // spelling for an interval - so the mode is `FAIL` and the bound is the only thing that takes
    // the check off.
    case a: Add if a.dataType.isInstanceOf[YearMonthIntervalType] =>
      intervalArith(IntOp.ADD, a.left, a.right, a, inputs, literals, sink)
    case a: Subtract if a.dataType.isInstanceOf[YearMonthIntervalType] =>
      intervalArith(IntOp.SUB, a.left, a.right, a, inputs, literals, sink)
    case n @ UnaryMinus(c, _) if n.dataType.isInstanceOf[YearMonthIntervalType] =>
      // `IntervalMathUtils.negateExact`, which throws on `Int.MinValue` alone, so any bound at
      // all rules it out - `IntNeg`'s reasoning over an interval operand.
      intervalOperand(c, "the negated interval", inputs, literals, sink).map { x =>
        val checked = !magnitude(x, literals).exists(_ <= Int.MaxValue.toLong)
        new IntNeg(if (checked) Overflow.FAIL else Overflow.WRAP, x)
      }
    case n @ Abs(c, _) if n.dataType.isInstanceOf[YearMonthIntervalType] =>
      // There is no abs op in the IR, and none is needed: `abs(x)` is the blend `if (x < 0) -x else
      // x`, which is the int negate node under `IfElse`. The only input that overflows a negation
      // is `Int.MinValue`, and it is negative, so it takes the `IntNeg` arm - the check fires
      // exactly where `IntegerExactNumeric` throws. That puts a checked node under a `CASE` arm,
      // and that is deliberate: the guard is qualified by the arm, so only the lanes that actually
      // negate can condemn the batch.
      intervalOperand(c, "the absolute interval", inputs, literals, sink).map { x =>
        val zero = intSlot(0, literals)
        val checked = !magnitude(x, literals).exists(_ <= Int.MaxValue.toLong)
        new IfElse(new Compare(CompareOp.LT, x, zero),
          new IntNeg(if (checked) Overflow.FAIL else Overflow.WRAP, x), x)
      }
    case m @ MakeYMInterval(y, mo) =>
      // `toIntExact(addExact(months, multiplyExact(years, 12)))` - two of the int32 arithmetic
      // nodes composed, both checked. Over bounded operands the bound removes both checks, which is
      // what makes `make_ym_interval(year(d), month(d))` fuse with none; over an unbounded int
      // column the multiply keeps its check and declines, as every checked multiply does.
      val mark = literals.size
      val built = for {
        years <- intOperand(y, inputs, literals, sink)
        months <- intOperand(mo, inputs, literals, sink)
        scaled <- arithOver(IntOp.MUL, Overflow.FAIL, years, twelve(literals), m,
          literals, mark, sink)
        total <- arithOver(IntOp.ADD, Overflow.FAIL, months, scaled, m, literals, mark, sink)
      } yield total
      if (built.isEmpty) truncate(literals, mark)
      built
    case e @ ExtractANSIIntervalYears(iv) =>
      // `IntervalUtils.getYears(months)` is `months / 12` - Java's `/`, truncating toward zero -
      // over a stored month count that nothing bounds. The int-lane magic multiply the calendar
      // uses is exact over 0..49,151, about one forty-thousandth of the type, so this is the
      // first division Varka emits through the double lane instead: exact for every int32, and
      // truncating already, so it needs neither a range guard nor a correction step.
      // `sql/varka/plans/verify_ym_division.py` checks both claims over all 2^32 month counts.
      intervalOperand(iv, "the interval", inputs, literals, sink).map(new ConstDivide(_, 12))
    case e @ ExtractANSIIntervalMonths(iv) =>
      // `(months % 12).toByte`. The remainder is one multiply and one subtract away from the
      // quotient above, so the division is not what blocks this: the result is a `ByteType`, and
      // Varka has neither a byte lane nor an Arrow vector to store one into. It declines until
      // a narrowing store exists, which is its own question (PLAN_MILESTONE_5.md 2.20).
      sink.note("extract(MONTH FROM ym) returns a byte, which has no lane", e)
      None
    case m @ MultiplyYMInterval(iv, num) =>
      // `Math.multiplyExact(months, num)` for the int-family arms. Both operands have to be
      // bounded before the check comes off, and a stored interval column never is, so a
      // literal multiplier alone does not buy it: `ym * 2` declines, while
      // `make_ym_interval(year(d), month(d)) * 2` fuses over a bounded interval. The `Long`,
      // `Decimal` and `Double` arms are not int32 lanes and decline by type rather than
      // reaching `intOperand`, which would report them as
      // "not an int column or literal" and hide which of the two is wrong.
      num.dataType match {
        case IntegerType =>
          val mark = literals.size
          val built = for {
            x <- intervalOperand(iv, "the multiplied interval", inputs, literals, sink)
            k <- intOperand(num, inputs, literals, sink)
            r <- arithOver(IntOp.MUL, Overflow.FAIL, x, k, m, literals, mark, sink)
          } yield r
          if (built.isEmpty) truncate(literals, mark)
          built
        case other =>
          sink.note(s"interval multiplier of type ${other.simpleString} is not an int32 lane", m)
          None
      }
  }

  /**
   * An operand of the year-month interval algebra: a value whose lane is a count of months. The
   * interval column and the interval literal are `compileNode`'s own leaves, and nested interval
   * arithmetic is the arms below, so this is a type gate over the same walk - the interval
   * counterpart of `intOperand`, and separate for the same reason the arms are separate rather than
   * the int arms' type gate being widened.
   */
  private[codegen] def intervalOperand(
      e: Expression,
      position: String,
      inputs: mutable.LinkedHashMap[Int, Int],
      literals: mutable.LinkedHashMap[Int, Int],
      sink: DeclineSink): Option[VarkaVectorIR] = e match {
    case _ if !e.dataType.isInstanceOf[YearMonthIntervalType] =>
      sink.note(s"$position of type ${e.dataType.simpleString} is not a year-month interval", e)
      None
    case _ => compileNode(e, inputs, literals, sink)
  }

  /**
   * The shared body of the binary interval arms. Spark computes `ym + ym` and `ym - ym` with
   * `IntervalMathUtils.addExact`/`subtractExact`, which throw in every evaluation mode - there is
   * no `LEGACY` wrapping form for an interval and no `try_` spelling - so the declared mode is
   * `FAIL` unconditionally rather than read off `evalMode`, and `arithOver`'s bound is the only
   * thing that takes the check off.
   */
  private def intervalArith(
      op: IntOp,
      l: Expression,
      r: Expression,
      whole: Expression,
      inputs: mutable.LinkedHashMap[Int, Int],
      literals: mutable.LinkedHashMap[Int, Int],
      sink: DeclineSink): Option[VarkaVectorIR] = {
    val mark = literals.size
    val operands = for {
      x <- intervalOperand(l, "the left interval operand", inputs, literals, sink)
      y <- intervalOperand(r, "the right interval operand", inputs, literals, sink)
    } yield (x, y)
    operands.flatMap { case (x, y) =>
      arithOver(op, Overflow.FAIL, x, y, whole, literals, mark, sink)
    }
  }

  /** The slot holding `12`, the months in a year - `make_ym_interval` and the YEAR casts. */
  private[codegen] def twelve(literals: mutable.LinkedHashMap[Int, Int]): LiteralSlot =
    intSlot(12, literals)
}
