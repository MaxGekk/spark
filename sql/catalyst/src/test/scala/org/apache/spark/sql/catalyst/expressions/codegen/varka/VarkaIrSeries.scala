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

package org.apache.spark.sql.catalyst.expressions.codegen.varka

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaIrGrammar._
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR._

/**
 * Every well-typed IR tree up to a size, by sort (VARKA-269): the series `VarkaSmallTreeSuite`
 * runs through the emitter and compares with the reference evaluator, where
 * [[VarkaIrGrammar]] draws trees at random. Random draws miss corners; a series of every tree of
 * a size has none to miss.
 *
 * The series is the grammar's, arm by arm: the same nodes, the same structural rules (a day
 * offset is a leaf, `IsNotNull` is over a column, a month count is a literal or the small
 * column, a trunc level the level column) and the same magnitude gates (a calendar node over a
 * subtree that stays inside the narrowed range, a checked mode where the bounds rule overflow
 * out, a range guard whose bounds contain its child's), through the grammar's own `boundsOf`.
 * So a tree here is one the grammar could draw, and the series answers what the grammar's
 * distribution never promised: whether every such tree of a size was checked. `gated = false`
 * drops the magnitude gates, which is how the suite asks what they keep out.
 *
 * A tree's size is the number of grammar productions in it: a leaf is one - a column or a
 * literal slot, and for a condition `IsNotNull` over a column - and each arm is one, however
 * many IR nodes it builds. `make_date` over a date's fields is the grammar's one arm, though its
 * IR repeats the date three times; `weekofyear` is one arm over `ThursdayOf`.
 *
 * The environment is fixed, so a size always gives the same series, in the same order: the day
 * columns and the literal slots are the leaves, and the special columns appear only where an arm
 * reads them. Where the grammar draws `literal()` - a literal slot, or a column when its
 * iteration has none - the series takes both, the union over the grammar's environments. Two day
 * columns rather than one, because a node over one column twice and over two columns are
 * different kernels.
 *
 * Every size below the one asked for is held; the size asked for is generated as it is read,
 * which is what lets the largest run at all.
 */
object VarkaIrSeries {

  /** The int lane's day columns, the generic leaves with the literal slot. */
  val DayColumns: Seq[Int] = Seq(0, 1)
  /** The column whose values stay inside the month-count range: `add_months`' count only. */
  val SmallColumn = 2
  /** The column whose values are trunc-level codes: `TruncDateDynamic`'s level only. */
  val LevelColumn = 3
  val NumInputs = 4
  /** The literal slots, the generic leaves with the columns. */
  val Literals = 1

  /** The long lane's columns, all generic leaves. */
  val LongColumns: Seq[Int] = Seq(0, 1)
  val LongNumInputs = 2

  /** The divisors of the grammar's `ConstDivide` arm, for the reasons it gives. */
  private val ConstDivideDivisors = Seq(2, 3, 7, 12, 100, -3, -12)

  /** The long lane's divisors, `LongShapes`' set. */
  private val LongDivisors = Seq(2L, 3L, 7L, 60L, -60L, 1000L, 1000000L, 1000000000L,
    60000000000L, 3600000000000L, 86400000000L, -12L)

  private def satAdd(a: Long, b: Long): Long =
    try Math.addExact(a, b) catch { case _: ArithmeticException => Long.MaxValue }
  private def satMul(a: Long, b: Long): Long =
    try Math.multiplyExact(a, math.max(1L, b)) catch {
      case _: ArithmeticException => Long.MaxValue
    }

  /** Every split of `n` into `k` positive parts, in order. */
  private def splits(n: Int, k: Int): Seq[Seq[Int]] =
    if (k == 1) { if (n >= 1) Seq(Seq(n)) else Seq.empty }
    else (1 to n - k + 1).flatMap(first => splits(n - first, k - 1).map(first +: _))

  /**
   * The range sets `InRanges` takes, laid over the child's own bound as the grammar places its
   * random one: one range across the middle, and two either side of zero with a gap, so that
   * lanes land inside, between and outside.
   */
  private def rangeSets(bound: Long): Seq[Seq[Int]] = {
    val span = math.max(4L, math.min(bound, Int.MaxValue / 4L)).toInt
    Seq(Seq(-span / 2, span / 2), Seq(-span, -span / 4, span / 4, span))
  }

  /**
   * The series of one lane: value and condition trees by size, the smaller sizes held, and the
   * roots a kernel is made of.
   */
  abstract class Series {
    private val heldValues = mutable.Map.empty[Int, IndexedSeq[Gen]]
    private val heldConds = mutable.Map.empty[Int, IndexedSeq[Cond]]

    /** Every value tree of exactly `n` productions, held. */
    def value(n: Int): IndexedSeq[Gen] = heldValues.getOrElseUpdate(n, values(n).toIndexedSeq)

    /** Every condition tree of exactly `n` productions, held. */
    def cond(n: Int): IndexedSeq[Cond] = heldConds.getOrElseUpdate(n, conds(n).toIndexedSeq)

    /** Every value tree of exactly `n` productions, generated as it is read. */
    def values(n: Int): Iterator[Gen]

    /** Every condition tree of exactly `n` productions, generated as it is read. */
    def conds(n: Int): Iterator[Cond]

    /** The roots of size `n`: value roots, then condition roots. */
    def roots(n: Int): Iterator[VarkaVectorIR] = values(n).map(_.node) ++ conds(n)

    /** Every root up to size `n`, smallest first, with its size: what the suite runs. */
    def upTo(n: Int): Iterator[(Int, VarkaVectorIR)] =
      (1 to n).iterator.flatMap(k => roots(k).map(k -> _))

    /** The comparisons and the connectives of size `n`, which both lanes have. */
    protected def compareAndConnectives(n: Int): Iterator[Cond] = {
      val compares = for {
        Seq(i, j) <- splits(n - 1, 2).iterator
        a <- value(i).iterator
        b <- value(j).iterator
        op <- CompareOp.values().iterator
      } yield new Compare(op, a.node, b.node): Cond
      val connectives = for {
        Seq(i, j) <- splits(n - 1, 2).iterator
        a <- cond(i).iterator
        b <- cond(j).iterator
        c <- Iterator[Cond](new And(a, b), new Or(a, b))
      } yield c
      compares ++ connectives ++ cond(n - 1).iterator.map(a => new Not(a): Cond)
    }
  }

  /** The int lane, `VarkaIrGrammar.Shapes`' arms. */
  final class IntSeries(gated: Boolean = true) extends Series {

    private val leaves: IndexedSeq[Gen] =
      DayColumns.map(c => Gen(new ColumnRef(c), columnBound)).toIndexedSeq ++
        (0 until Literals).map(j => Gen(new LiteralSlot(j), literalBound))

    private def fitsUnderChrono(a: Gen): Boolean =
      !gated || (a.bound <= chronoBound && boundsOf(a.node)._2 <= chronoBound)

    /** The arms over one subtree. */
    private def unary(a: Gen): Seq[Gen] = {
      val out = mutable.ArrayBuffer.empty[Gen]
      out += Gen(new DayOfWeek(a.node), 7)
      out += Gen(new WeekDay(a.node), 6)
      out += Gen(new ThursdayOf(a.node), satAdd(a.bound, 3))
      out += Gen(new DayOfWeekIso(a.node), 7)
      out += Gen(new IntNeg(Overflow.WRAP, a.node), a.bound)
      // Negation overflows on one value, which a bound inside the int range rules out.
      if (!gated || a.bound <= Int.MaxValue.toLong) {
        out += Gen(new IntNeg(Overflow.FAIL, a.node), a.bound)
      }
      for (d <- ConstDivideDivisors) {
        out += Gen(new ConstDivide(a.node, d), a.bound / math.abs(d.toLong))
      }
      // A range guard whose bounds contain the child's, the whole int range past it.
      val bound = if (a.bound > Int.MaxValue.toLong) Int.MaxValue.toLong else a.bound
      val lo = if (a.bound > Int.MaxValue.toLong) Int.MinValue.toLong else -bound
      out += Gen(new GuardedRange(a.node, lo, bound), a.bound)
      if (fitsUnderChrono(a)) {
        out += Gen(new Year(a.node), 40000)
        out += Gen(new Month(a.node), 12)
        out += Gen(new DayOfMonth(a.node), 31)
        out += Gen(new Quarter(a.node), 4)
        out += Gen(new DayOfYear(a.node), 366)
        out += Gen(new LastDay(a.node), satAdd(a.bound, 31))
        out += Gen(new GuardedDay(a.node), a.bound)
        out += Gen(new WeekOfYear(new ThursdayOf(a.node)), 53)
        for (failOnError <- Seq(false, true)) {
          out += Gen(new MakeDate(new Year(a.node), new Month(a.node), new DayOfMonth(a.node),
            failOnError), a.bound)
        }
        out += Gen(BoundedDivide.of(new DayOfYear(a.node), 7, 367), 53)
        out += Gen(BoundedDivide.of(new Month(a.node), 4, 13), 4)
        out += Gen(BoundedDivide.of(new DayOfMonth(a.node), 7, 32), 5)
        for (level <- TruncLevel.values()) {
          out += Gen(new TruncDate(a.node, level), satAdd(a.bound, 366))
        }
      }
      out.toSeq
    }

    /** The arms over a subtree and a leaf. */
    private def withLeaf(a: Gen, b: Gen): Seq[Gen] = {
      val out = mutable.ArrayBuffer.empty[Gen]
      out += Gen(new AddDays(a.node, b.node), satAdd(a.bound, b.bound))
      out += Gen(new SubDays(a.node, b.node), satAdd(a.bound, b.bound))
      out += Gen(new NextDay(a.node, b.node), satAdd(a.bound, 8))
      val sumBound = satAdd(a.bound, b.bound)
      // A checked mode only where the bounds rule overflow out: the kernel has to answer.
      val modes = if (!gated || sumBound <= Int.MaxValue.toLong) {
        Seq(Overflow.WRAP, Overflow.FAIL, Overflow.NULL)
      } else {
        Seq(Overflow.WRAP)
      }
      for (mode <- modes; op <- Seq(IntOp.ADD, IntOp.SUB)) {
        out += Gen(new IntArith(op, mode, a.node, b.node), sumBound)
      }
      out += Gen(new IntArith(IntOp.MUL, Overflow.WRAP, a.node, b.node), satMul(a.bound, b.bound))
      out.toSeq
    }

    /** The arms over a subtree and a special column or a literal slot. */
    private def withSpecial(a: Gen): Seq[Gen] = {
      if (!fitsUnderChrono(a)) return Seq.empty
      val out = mutable.ArrayBuffer.empty[Gen]
      val count = Gen(new ColumnRef(SmallColumn), VarkaChrono.MONTH_ARITH_MAX_MONTHS.toLong)
      out += Gen(new AddMonths(a.node, count.node), satAdd(a.bound, satMul(count.bound, 31)))
      out += Gen(new TruncDateDynamic(a.node, new ColumnRef(LevelColumn)), satAdd(a.bound, 366))
      for (j <- 0 until Literals) {
        val m = Gen(new LiteralSlot(j), literalBound)
        out += Gen(new AddMonths(a.node, m.node), satAdd(a.bound, satMul(m.bound, 31)))
        // make_date's NULL form with a literal day: an invalid day is a null output.
        out += Gen(new MakeDate(new Year(a.node), new Month(a.node), m.node, false),
          satAdd(a.bound, 31))
      }
      out.toSeq
    }

    def values(n: Int): Iterator[Gen] = {
      if (n <= 0) return Iterator.empty
      if (n == 1) return leaves.iterator
      val binary = for {
        Seq(i, j) <- splits(n - 1, 2).iterator
        a <- value(i).iterator
        b <- value(j).iterator
        g <- Iterator(
          Gen(new DateDiff(a.node, b.node), satAdd(a.bound, b.bound)),
          Gen(new Greatest(a.node, b.node), math.max(a.bound, b.bound)),
          Gen(new Least(a.node, b.node), math.max(a.bound, b.bound)))
      } yield g
      val ifElse = for {
        Seq(i, j, k) <- splits(n - 1, 3).iterator
        c <- cond(i).iterator
        a <- value(j).iterator
        b <- value(k).iterator
      } yield Gen(new IfElse(c, a.node, b.node), math.max(a.bound, b.bound))
      value(n - 1).iterator.flatMap(unary) ++
        value(n - 2).iterator.flatMap(a => leaves.iterator.flatMap(b => withLeaf(a, b))) ++
        value(n - 2).iterator.flatMap(withSpecial) ++
        binary ++ ifElse
    }

    def conds(n: Int): Iterator[Cond] = {
      if (n <= 0) return Iterator.empty
      // IsNotNull reads a column's validity word directly, so its child is a column.
      if (n == 1) return (0 until NumInputs).iterator.map(c => new IsNotNull(new ColumnRef(c)))
      compareAndConnectives(n) ++ (for {
        a <- value(n - 1).iterator
        bounds <- rangeSets(a.bound).iterator
      } yield new InRanges(a.node, bounds.map(Int.box).asJava): Cond)
    }
  }

  /** The long lane, `VarkaIrGrammar.LongShapes`' arms, with `NarrowLane` at a root. */
  final class LongSeries extends Series {

    private val leaves: IndexedSeq[Gen] =
      LongColumns.map(c => Gen(new ColumnRef(c, LaneType.LONG), longColumnBound)).toIndexedSeq ++
        (0 until Literals).map(j => Gen(new LiteralSlot(j, LaneType.LONG), longLiteralBound))

    private def saturated(bound: Long): Boolean = bound == Long.MaxValue

    private def unary(a: Gen): Seq[Gen] = {
      val out = mutable.ArrayBuffer.empty[Gen]
      out += Gen(new IntNeg(Overflow.WRAP, a.node), a.bound)
      if (!saturated(a.bound)) out += Gen(new IntNeg(Overflow.FAIL, a.node), a.bound)
      // The constant division, over a dividend the lowering is exact for.
      if (a.bound < ConstDivide.EXACT_DIVIDEND_BOUND) {
        for (d <- LongDivisors) {
          out += Gen(new ConstDivide(a.node, d, a.bound + 1), a.bound / math.abs(d))
        }
      }
      out += (if (saturated(a.bound)) {
        Gen(new GuardedRange(a.node, Long.MinValue, Long.MaxValue), a.bound)
      } else {
        Gen(new GuardedRange(a.node, -a.bound, a.bound), a.bound)
      })
      out.toSeq
    }

    /** The arms over two subtrees: add and subtract, multiply, greatest and least. */
    private def binary(a: Gen, b: Gen): Seq[Gen] = {
      val out = mutable.ArrayBuffer.empty[Gen]
      val sumBound = satAdd(a.bound, b.bound)
      val modes = if (saturated(sumBound)) {
        Seq(Overflow.WRAP)
      } else {
        Seq(Overflow.WRAP, Overflow.FAIL, Overflow.NULL)
      }
      for (mode <- modes; op <- Seq(IntOp.ADD, IntOp.SUB)) {
        out += Gen(new IntArith(op, mode, a.node, b.node), sumBound)
      }
      out += Gen(new IntArith(IntOp.MUL, Overflow.WRAP, a.node, b.node), satMul(a.bound, b.bound))
      out += Gen(new Greatest(a.node, b.node), math.max(a.bound, b.bound))
      out += Gen(new Least(a.node, b.node), math.max(a.bound, b.bound))
      out.toSeq
    }

    def values(n: Int): Iterator[Gen] = {
      if (n <= 0) return Iterator.empty
      if (n == 1) return leaves.iterator
      val pairs = for {
        Seq(i, j) <- splits(n - 1, 2).iterator
        a <- value(i).iterator
        b <- value(j).iterator
        g <- binary(a, b).iterator
      } yield g
      val ifElse = for {
        Seq(i, j, k) <- splits(n - 1, 3).iterator
        c <- cond(i).iterator
        a <- value(j).iterator
        b <- value(k).iterator
      } yield Gen(new IfElse(c, a.node, b.node), math.max(a.bound, b.bound))
      value(n - 1).iterator.flatMap(unary) ++ pairs ++ ifElse
    }

    def conds(n: Int): Iterator[Cond] = {
      if (n <= 0) return Iterator.empty
      if (n == 1) {
        return LongColumns.iterator.map(c => new IsNotNull(new ColumnRef(c, LaneType.LONG)))
      }
      compareAndConnectives(n)
    }

    /**
     * A value tree at a root is also narrowed to a 32-bit column where its bound fits an int, as
     * the compiler builds every `TIME` kernel's root; `NarrowLane` is admitted at an output root
     * alone, so it is added here and never inside a tree.
     */
    override def roots(n: Int): Iterator[VarkaVectorIR] =
      values(n).flatMap { g =>
        if (g.bound <= Int.MaxValue) Iterator(g.node, new NarrowLane(g.node)) else Iterator(g.node)
      } ++ conds(n)
  }
}
