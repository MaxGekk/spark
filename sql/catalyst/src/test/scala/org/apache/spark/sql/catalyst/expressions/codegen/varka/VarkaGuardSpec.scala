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

import java.time.{DateTimeException, LocalDate}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR._

/**
 * Which batches a kernel must decline, may decline, or must answer, decided from the inputs and
 * the guards' bounds before the kernel runs (VARKA-279). A kernel may refuse a batch - set a
 * status bit, and Spark's row engine answers it instead - and the guards that refuse are where a
 * lowering's correctness has a boundary: inside the range a guard admits the kernel is exact,
 * outside it the batch must go to Spark. The reference evaluator has no notion of a declined
 * batch; this is its counterpart, and the suites hold a kernel's status to it.
 *
 * Per row, for every node the emitter guards (`VarkaLoopEmitter.guardsForTest`, under the options
 * the kernel is emitted with), the guard fires when the node's operands are valid and its checked
 * value is outside the bound. A fired guard is then read by the rule `Analysis` applies:
 *
 *  - qualified by an arm chain (non-empty): it condemns the row only where
 *    the chain selects it, and then the row **must** be declined; elsewhere the arm masks it and
 *    the row must be answered;
 *  - unqualified: where the row reaches the node through arms it takes, it **must** be declined;
 *    where only through arms it does not take, it **may** be declined, by that guard's bit.
 *
 * A null is never a reason to decline. A batch's verdict joins its rows'. The bounds are the
 * constants the emitter reads (`VarkaChrono`'s narrow range, month-arithmetic and `make_date`
 * limits, the lane's own range for a checked overflow, a `GuardedRange`'s own `lo` and `hi`),
 * not copies, so the verdict cannot drift from the code by someone editing one side. The values
 * are the reference evaluator's, which shares no lowering with the emitter.
 *
 * Scala, beside the reference evaluator it drives, for the reason [[VarkaKernelCheck]] gives. The
 * plan's fourth verdict, an input outside the column contract, is not computed: the IR does not
 * say which columns are dates, so the edge suite keeps every input inside the contract by
 * construction instead.
 */
object VarkaGuardSpec {

  /**
   * The bits a batch must decline with and the further bits it may; both zero for a batch the
   * kernel must answer.
   */
  final case class Verdict(must: Int, may: Int) {
    def |(other: Verdict): Verdict = Verdict(must | other.must, may | other.may)

    def answers: Boolean = must == 0 && may == 0

    /**
     * Whether a kernel status, read under `exactRefusalStatus`, agrees: every must bit and
     * nothing outside must and may; any subset of may where there is no must; 0 for an answer.
     */
    def admits(status: Int): Boolean =
      if (must != 0) (status & must) == must && (status & ~(must | may)) == 0
      else (status & ~may) == 0
  }

  val Answer: Verdict = Verdict(0, 0)

  /**
   * One end of one guard's bound moved, for the test that the edge suite cannot pass vacuously:
   * `lo` and `hi` are added to the bound the guard has.
   */
  final case class Shift(reason: Analysis.Refusal, lo: Long, hi: Long)

  /**
   * A kernel's guards, read once: `roots` emitted under `options`, at the long lane where `long`,
   * and each guarded node's reason and arm chain.
   */
  final class Guards(val roots: Seq[VarkaVectorIR], numInputs: Int, numLiterals: Int,
      options: VarkaEmitOptions, val long: Boolean) {
    val guards: Seq[(VarkaVectorIR, Analysis.Refusal, Seq[Analysis.ArmStep])] =
      VarkaLoopEmitter.guardsForTest(roots.asJava, numInputs, numLiterals, options).asScala.toSeq
        .map { case (node, g) => (node, g.reason(), g.arms().asScala.toSeq) }
  }

  /** The verdict on a batch of `rows` (each a value or None per column) for the kernel `g`. */
  def verdict(g: Guards, lits: Array[Long], rows: Seq[Seq[Option[Long]]],
      shifts: Seq[Shift]): Verdict =
    rows.foldLeft(Answer) { (v, row) =>
      v | rowVerdict(g, new Values(row, lits, g.long), shifts)
    }

  /** [[verdict]] for a kernel whose guards are read for this one call. */
  def verdict(roots: Seq[VarkaVectorIR], numInputs: Int, lits: Array[Long],
      options: VarkaEmitOptions, long: Boolean, rows: Seq[Seq[Option[Long]]],
      shifts: Seq[Shift] = Nil): Verdict =
    verdict(new Guards(roots, numInputs, lits.length, options, long), lits, rows, shifts)

  /** The reference evaluator's values and conditions on one row, at one lane. */
  private final class Values(row: Seq[Option[Long]], lits: Array[Long], long: Boolean) {
    private val ints = row.map(_.map(_.toInt))
    private val intLits = lits.map(_.toInt)

    def value(node: VarkaVectorIR): Option[BigInt] =
      if (long) VarkaReferenceEvaluator.evalLong(node, row, lits).map(BigInt(_))
      else VarkaReferenceEvaluator.evalValue(node, ints, intLits).map(BigInt(_))

    def holds(cond: Cond): Boolean =
      if (long) VarkaReferenceEvaluator.evalCondLong(cond, row, lits).contains(true)
      else VarkaReferenceEvaluator.evalCond(cond, ints, intLits).contains(true)
  }

  private def rowVerdict(g: Guards, values: Values, shifts: Seq[Shift]): Verdict = {
    lazy val live = reached(g.roots, values)
    g.guards.foldLeft(Answer) { case (v, (node, reason, chain)) =>
      if (!fires(node, reason, values, g.long, shifts)) {
        v
      } else {
        if (chain.nonEmpty) {
          val selected = chain.forall(s => values.holds(s.node().cond()) == s.thenBranch())
          if (selected) v | Verdict(reason.status, 0) else v
        } else if (live.contains(node)) {
          v | Verdict(reason.status, 0)
        } else {
          v | Verdict(0, reason.status)
        }
      }
    }
  }

  /**
   * The nodes the row computes for an output it answers: those reached from a root through arms
   * the row takes. A condition is always reached where its `IfElse` is; an unknown condition
   * takes the else arm, as the reference evaluator and the emitter both read it.
   */
  private def reached(roots: Seq[VarkaVectorIR], values: Values): Set[VarkaVectorIR] = {
    val seen = mutable.HashSet.empty[VarkaVectorIR]
    def mark(node: VarkaVectorIR): Unit = if (seen.add(node)) node match {
      case n: IfElse =>
        mark(n.cond())
        if (values.holds(n.cond())) mark(n.thenNode()) else mark(n.elseNode())
      case other => VarkaVectorIR.childrenOf(other).foreach(mark)
    }
    roots.foreach(mark)
    seen.toSet
  }

  /** The bound `reason`'s guard on `node` admits, inclusive, with any shift applied. */
  private def bound(node: VarkaVectorIR, reason: Analysis.Refusal, long: Boolean,
      shifts: Seq[Shift]): (BigInt, BigInt) = {
    val (lo, hi): (BigInt, BigInt) = reason match {
      case Analysis.Refusal.DAY_PRODUCER | Analysis.Refusal.DAY_RANGE =>
        (VarkaChrono.NARROW_MIN_DAYS, VarkaChrono.NARROW_MAX_DAYS)
      case Analysis.Refusal.MONTH_COUNT =>
        (VarkaChrono.MONTH_ARITH_MIN_MONTHS, VarkaChrono.MONTH_ARITH_MAX_MONTHS)
      case Analysis.Refusal.MAKE_DATE =>
        (VarkaChrono.MAKE_DATE_MIN_YEAR, VarkaChrono.MAKE_DATE_MAX_YEAR)
      case Analysis.Refusal.INT_OVERFLOW =>
        if (long) (BigInt(Long.MinValue), BigInt(Long.MaxValue))
        else (BigInt(Int.MinValue), BigInt(Int.MaxValue))
      case Analysis.Refusal.VALUE_RANGE => node match {
        case g: GuardedRange => (BigInt(g.lo()), BigInt(g.hi()))
        case other => throw new IllegalArgumentException(s"a value range on $other")
      }
    }
    shifts.filter(_.reason == reason).foldLeft((lo, hi)) { case ((l, h), s) =>
      (l + s.lo, h + s.hi)
    }
  }

  /**
   * Whether `reason`'s guard on `node` fires on the row: its operands valid, and its checked
   * value - the producer's result, the month count, the year (and under ANSI the date), the
   * exact sum or negation, the guarded child - outside the bound.
   */
  private def fires(node: VarkaVectorIR, reason: Analysis.Refusal, values: Values, long: Boolean,
      shifts: Seq[Shift]): Boolean = {
    val (lo, hi) = bound(node, reason, long, shifts)
    def outside(v: BigInt): Boolean = v < lo || v > hi
    node match {
      case n: AddDays =>
        (for (d <- values.value(n.days()); o <- values.value(n.offset())) yield d + o)
          .exists(outside)
      case n: SubDays =>
        (for (d <- values.value(n.days()); o <- values.value(n.offset())) yield d - o)
          .exists(outside)
      case n: AddMonths =>
        values.value(n.days()).isDefined && values.value(n.months()).exists(outside)
      case n: MakeDate =>
        (for (y <- values.value(n.year()); m <- values.value(n.month());
            d <- values.value(n.day())) yield {
          outside(y) || (n.failOnError() && !isDate(y, m, d))
        }).contains(true)
      case n: IntArith =>
        (for (l <- values.value(n.left()); r <- values.value(n.right())) yield n.op() match {
          case IntOp.ADD => l + r
          case IntOp.SUB => l - r
          case IntOp.MUL => l * r
        }).exists(outside)
      case n: IntNeg => values.value(n.child()).map(c => -c).exists(outside)
      case n: GuardedDay => values.value(n.days()).exists(outside)
      case n: GuardedRange => values.value(n.child()).exists(outside)
      case other => throw new IllegalArgumentException(s"no guard spec for $reason on $other")
    }
  }

  /** Whether `(y, m, d)` names a day of the calendar, as `LocalDate.of` decides it. */
  private def isDate(y: BigInt, m: BigInt, d: BigInt): Boolean = {
    if (!y.isValidInt || !m.isValidInt || !d.isValidInt) return false
    try {
      LocalDate.of(y.toInt, m.toInt, d.toInt)
      true
    } catch {
      case _: DateTimeException => false
    }
  }
}
