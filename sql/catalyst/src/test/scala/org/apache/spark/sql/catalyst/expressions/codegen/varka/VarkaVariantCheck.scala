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

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVariants.{Kind, Variant}

/**
 * The variant check's Scala face: [[VarkaVariantChecks]], the Java class that runs a case's
 * variants (VARKA-278) through the emitter and the kernel check (VARKA-311), over the suites'
 * [[VarkaFuzzCase]]. Each method converts the case, the suite's emit function and the outcome;
 * nothing is checked here.
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

  /** How a suite emits, as [[VarkaVariantChecks.Emit]]. */
  type Emit = (Seq[VarkaVectorIR], Int, Int, VarkaEmitOptions, String) => Option[Array[Byte]]

  /** [[VarkaVariantChecks.RETRY_WITHOUT_BUDGET]]. */
  val retryWithoutBudget: Emit = (roots, numInputs, numLiterals, options, className) =>
    VarkaVariantChecks.RETRY_WITHOUT_BUDGET.emit(roots.asJava, numInputs, numLiterals, options,
      className).toScala

  private def toJava(c: VarkaFuzzCase): VarkaVariantChecks.Case =
    new VarkaVariantChecks.Case(c.lane, c.roots.asJava, c.numInputs, c.lits, c.length, c.nulls,
      c.data, c.forceMasked, c.options, c.smallOrdinal, c.levelOrdinal, c.label)

  private def toJava(emit: Emit): VarkaVariantChecks.Emit =
    (roots, numInputs, numLiterals, options, className) =>
      emit(roots.asScala.toSeq, numInputs, numLiterals, options, className).toJava

  private def outcome(o: VarkaVariantChecks.Outcome): Outcome = o match {
    case _: VarkaVariantChecks.Outcome.Compared => Compared
    case d: VarkaVariantChecks.Outcome.Declined => Declined(d.status())
    case _: VarkaVariantChecks.Outcome.Skipped => Skipped
  }

  /** [[VarkaVariantChecks.variants]]: each kind, and its variant where it applies to `c`. */
  def variants(c: VarkaFuzzCase, stranger: VarkaVectorIR, pool: Seq[VarkaVectorIR],
      rotation: Int): Seq[(Kind, Option[Variant])] =
    VarkaVariantChecks.variants(toJava(c), stranger, pool.asJava, rotation).asScala.toSeq
      .map(kv => kv.kind() -> kv.variant().toScala)

  /** [[VarkaVariantChecks.variants]] with the strangers in order and a predicate (VARKA-323). */
  def variants(c: VarkaFuzzCase, strangers: Seq[VarkaVectorIR], pool: Seq[VarkaVectorIR],
      rotation: Int, fits: Variant => Boolean): Seq[(Kind, Option[Variant])] =
    VarkaVariantChecks.variants(toJava(c), strangers.asJava, pool.asJava, rotation,
      (v: Variant) => fits(v)).asScala.toSeq.map(kv => kv.kind() -> kv.variant().toScala)

  /** [[VarkaVariantChecks.armOracle]]. */
  def armOracle(c: VarkaFuzzCase): VarkaVariants.ArmOracle = VarkaVariantChecks.armOracle(toJava(c))

  /** [[VarkaVariantChecks.caseOf]], as a [[VarkaFuzzCase]]. */
  def caseOf(c: VarkaFuzzCase, v: Variant): VarkaFuzzCase = {
    val vc = VarkaVariantChecks.caseOf(toJava(c), v)
    c.copy(roots = vc.roots().asScala.toSeq, numInputs = vc.numInputs(), nulls = vc.nulls(),
      data = vc.data(), smallOrdinal = vc.smallOrdinal(), levelOrdinal = vc.levelOrdinal(),
      label = vc.label())
  }

  /** [[VarkaVariantChecks.checkPlain]]. */
  def checkPlain(c: VarkaFuzzCase, emit: Emit): Outcome =
    outcome(VarkaVariantChecks.checkPlain(toJava(c), toJava(emit)))

  /** [[VarkaVariantChecks.check]]. */
  def check(c: VarkaFuzzCase, v: Variant, emit: Emit): Outcome =
    outcome(VarkaVariantChecks.check(toJava(c), v, toJava(emit)))
}
