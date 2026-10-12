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

import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import org.scalatest.Assertions._

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVariants.{Kind, Variant}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR._

/**
 * Runs the variants of a passing case ([[VarkaVariants]], VARKA-278) through the emitter and
 * [[VarkaKernelCheck]], for `VarkaVariantSuite` over the small-tree series and `VarkaIrFuzzSuite`
 * over its draws. A case is a [[VarkaFuzzCase]] - roots, columns, literals and one batch - and a
 * variant becomes a case of its own, its columns built from the original's by the variant's
 * plan. Scala because the check and the reference evaluator it drives are.
 *
 * Every output of a variant is compared with the reference evaluator, row for row. A variant may
 * decline only where [[VarkaVariants.Kind.mayDecline]] says, and then the kernel is emitted again
 * under `exactRefusalStatus` and run on the same batch: the bits it declines with must be bits of
 * guards the variant adds ([[VarkaVariants.addedRefusalBits]]). Anywhere else a decline fails.
 */
object VarkaVariantCheck {

  /** What one variant's emission and check came to. */
  sealed trait Outcome
  /** Every row of every output agreed with the reference evaluator. */
  case object Compared extends Outcome
  /** A decline the variant may make, by the bits it was allowed: `status` under exact status. */
  final case class Declined(status: Int) extends Outcome
  /** No form of the emitter holds the shape: past the class-file cap. */
  case object Skipped extends Outcome

  /**
   * How a suite emits: the roots, the column and literal counts, the options and the class name
   * to give the class; None for a shape past the class-file cap. The series passes
   * [[retryWithoutBudget]]; the fuzzer passes its own, so its checks on a decline's reason, its
   * count of shapes past the cap and its size trace apply to its variants as to its draws.
   */
  type Emit = (Seq[VarkaVectorIR], Int, Int, VarkaEmitOptions, String) => Option[Array[Byte]]

  /**
   * The series' way of emitting: a size decline under the byte budget is retried without it, as
   * the fuzzer does, and a decline without one is the class-file cap's.
   */
  val retryWithoutBudget: Emit = (roots, numInputs, numLiterals, options, className) => {
    def emitWith(o: VarkaEmitOptions): Array[Byte] =
      VarkaLoopEmitter.emit(className, roots.asJava, numInputs, numLiterals, null, null, o)
    try Some(emitWith(options)) catch {
      case _: VarkaEmitDeclined if options.methodByteBudget() > 0 =>
        try Some(emitWith(options.withMethodByteBudget(0))) catch {
          case _: VarkaEmitDeclined => None
        }
      case _: VarkaEmitDeclined => None
    }
  }

  private val classCounter = new AtomicInteger(0)

  private def className(c: VarkaFuzzCase): String = "org.apache.spark.sql.varka.execution." +
    s"VarkaVariant${if (c.lane == LaneType.LONG) "Long" else ""}${classCounter.addAndGet(1)}"

  /**
   * The variants of `c`, in [[Kind]]'s order, each None where it does not apply to `c`.
   * `stranger` is a tree that passes over `c`'s columns, and `pool` the trees an untaken arm may
   * be replaced by, scanned from `rotation`.
   */
  def variants(c: VarkaFuzzCase, stranger: VarkaVectorIR, pool: Seq[VarkaVectorIR],
      rotation: Int): Seq[(Kind, Option[Variant])] = {
    val roots = c.roots.asJava
    val n = c.numInputs
    Seq(
      Kind.BESIDE_A_CHILD -> VarkaVariants.besideAChild(roots, n).toScala,
      Kind.PERMUTED -> VarkaVariants.permuted(roots, n).toScala,
      Kind.DUPLICATED -> Some(VarkaVariants.duplicated(roots, n)),
      Kind.COPIED -> Some(VarkaVariants.copied(roots, n)),
      Kind.BESIDE_A_STRANGER -> Some(VarkaVariants.besideAStranger(roots, n, stranger)),
      Kind.LITERAL_TO_COLUMN -> VarkaVariants.literalToColumn(roots, n).toScala,
      Kind.COLUMNS_RENUMBERED -> VarkaVariants.columnsRenumbered(roots, n).toScala,
      Kind.UNTAKEN_ARM ->
        VarkaVariants.untakenArm(roots, n, armOracle(c), pool.asJava, rotation).toScala)
  }

  /**
   * Which arm of `cond` the rows of `c` take, by the reference evaluator: the then arm where the
   * condition is true, the else arm where it is false or null. Read over both batches the check
   * runs - `c`'s own nulls and its other body's ([[VarkaKernelCheck.otherBody]]: no nulls, or at
   * one row every column null) - since a condition over a column's validity can turn there, and
   * an arm untaken on one is taken on the other.
   */
  def armOracle(c: VarkaFuzzCase): VarkaVariants.ArmOracle = cond => {
    val own = (0 until c.numInputs).map(col => (i: Int) => c.nulls(col)(i))
    val other = VarkaKernelCheck.otherBody(c.numInputs, c.length, own, c.forceMasked).map(_._1)
    var anyThen = false
    var anyElse = false
    for (patterns <- own +: other.toSeq; i <- 0 until c.length) {
      val row = (0 until c.numInputs).map { col =>
        if (patterns(col)(i)) None else Some(c.data(col)(i))
      }
      val taken = if (c.lane == LaneType.LONG) {
        VarkaReferenceEvaluator.evalCondLong(cond, row, c.lits).contains(true)
      } else {
        VarkaReferenceEvaluator.evalCond(cond, row.map(_.map(_.toInt)), c.lits.map(_.toInt))
          .contains(true)
      }
      if (taken) anyThen = true else anyElse = true
    }
    if (!anyThen) VarkaVariants.Taken.ELSE_ONLY
    else if (!anyElse) VarkaVariants.Taken.THEN_ONLY
    else VarkaVariants.Taken.BOTH
  }

  /** `c` as `v` sees it: `v`'s roots over the columns its plan builds from `c`'s. */
  def caseOf(c: VarkaFuzzCase, v: Variant): VarkaFuzzCase = {
    val sources = v.columns.asScala.toSeq
    val data = sources.map {
      case f: VarkaVariants.FromColumn => c.data(f.ordinal)
      case l: VarkaVariants.FromLiteral => Array.fill(c.length)(c.lits(l.slot))
    }.toArray
    val nulls = sources.map {
      case f: VarkaVariants.FromColumn => c.nulls(f.ordinal)
      case _: VarkaVariants.FromLiteral => Array.fill(c.length)(false)
    }.toArray
    def moved(ordinal: Int): Int =
      if (ordinal < 0) -1 else sources.indexOf(new VarkaVariants.FromColumn(ordinal))
    val roots = v.roots.asScala.toSeq
    c.copy(roots = roots, numInputs = sources.size, nulls = nulls, data = data,
      smallOrdinal = moved(c.smallOrdinal), levelOrdinal = moved(c.levelOrdinal),
      label = s"${c.label} variant=${v.kind.label} " +
        s"roots=${roots.map(r => VarkaVectorIR.canonical(r)).mkString("[", ", ", "]")}")
  }

  /** `c` itself, emitted and compared; a decline fails. */
  def checkPlain(c: VarkaFuzzCase, emit: Emit): Outcome = {
    val name = className(c)
    emit(c.roots, c.numInputs, c.lits.length, c.options, name) match {
      case None => Skipped
      case Some(bytes) =>
        status(c, name, bytes, declineAllowed = false)
        Compared
    }
  }

  /**
   * Variant `v` of `c`, emitted and compared; throws on a mismatch, on a decline the variant may
   * not make, and on one whose exact bits are not all the guards' it adds.
   */
  def check(c: VarkaFuzzCase, v: Variant, emit: Emit): Outcome = {
    val vc = caseOf(c, v)
    def run(options: VarkaEmitOptions): Option[Int] = {
      val name = className(vc)
      emit(vc.roots, vc.numInputs, vc.lits.length, options, name)
        .map(bytes => status(vc, name, bytes, v.kind.mayDecline))
    }
    run(vc.options) match {
      case None => Skipped
      case Some(0) => Compared
      case Some(first) =>
        val exactOptions = vc.options.withExactRefusalStatus(true)
        val allowed = VarkaVariants.addedRefusalBits(c.roots.asJava, c.numInputs, v.roots,
          vc.numInputs, vc.lits.length, exactOptions)
        val exact = run(exactOptions).getOrElse(
          fail(s"${vc.label}: declined (status $first), then past the class-file cap under " +
            "exactRefusalStatus"))
        assert(exact != 0, s"${vc.label}: declined (status $first) under its options and " +
          "answered under exactRefusalStatus")
        assert((exact & ~allowed) == 0, s"${vc.label}: declined with status $exact; the guards " +
          s"the variant adds give only $allowed")
        Declined(exact)
    }
  }

  /** `c`'s kernel run over `c`'s batch: the status, 0 once every row is compared. */
  private def status(c: VarkaFuzzCase, name: String, bytes: Array[Byte],
      declineAllowed: Boolean): Int = {
    val patterns = (0 until c.numInputs).map(col => (i: Int) => c.nulls(col)(i))
    if (c.lane == LaneType.LONG) {
      VarkaKernelCheck.runAndCompareLongStatus(c.label, name, bytes, c.roots, c.numInputs,
        c.lits, VarkaKernelCheck.LongBatch(c.length, patterns, c.data, c.forceMasked),
        declineAllowed)
    } else {
      VarkaKernelCheck.runAndCompareStatus(c.label, name, bytes, c.roots, c.numInputs,
        c.lits.map(_.toInt), VarkaKernelCheck.Batch(c.length, patterns,
          c.data.map(_.map(_.toInt)), c.forceMasked), declineAllowed)
    }
  }
}
