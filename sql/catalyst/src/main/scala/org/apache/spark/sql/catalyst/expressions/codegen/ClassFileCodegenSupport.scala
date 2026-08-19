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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.types.DateType

/**
 * The kind of Varka batch kernel backing an expression (Task 4).
 */
object ClassFileGenOpKind extends Enumeration {
  val DateAdd, DateSub, DateDiff = Value
}

/**
 * The declarative `invokestatic` contract of a Varka batch kernel (Task 4). The argument
 * order of [[DateVectorOpsOwner]] methods IS the JVM stack order, so the descriptor fully
 * pins the bytecode emission. This is a pure-data value consumed by the engine-side
 * Class-File assembler (Task 5); the engine module is referenced only by name.
 *
 * @param ownerClassName the binary name of the class owning the kernel.
 * @param methodName the static method name.
 * @param methodDescriptor the JVM method descriptor, e.g. `(JJIJJII)V`.
 * @param kind which expression this op backs.
 */
case class ClassFileGenOp(
    ownerClassName: String,
    methodName: String,
    methodDescriptor: String,
    kind: ClassFileGenOpKind.Value)

/**
 * Marker trait for expressions that can be compiled to a Varka batch-kernel call instead of
 * per-row string codegen (Task 4). Mixing expressions register themselves into the
 * [[CodegenContext]] registry via `genCode` and keep their existing string codegen path
 * unchanged; routing to the Class-File assembler is a later task.
 */
trait ClassFileCodegenSupport extends Expression {

  /** The `invokestatic` contract for the Varka batch kernel backing this expression. */
  def classFileGenOp: ClassFileGenOp

  /** Whether this expression is eligible for the Varka batch-kernel path (MVP rules). */
  def isClassFileGenEligible: Boolean

  /**
   * Folded `days` offset for DateAdd/DateSub. `Some(intValue)` when `days` is a non-null
   * integral literal, `None` otherwise (runtime or null days keep the string path).
   */
  def daysOffsetConstant: Option[Int] = None

  override def genCode(ctx: CodegenContext): ExprCode = {
    ctx.registerClassFileGenExpression(this)
    super.genCode(ctx)
  }
}

/**
 * Plan-level helpers for the Varka Class-File path (Task 4). The constants are the
 * compile-time contract with the standalone engine module: they must match
 * `DateVectorOps` exactly, which the engine-side emission test enforces by deriving the
 * descriptor from the actual method via reflection.
 */
object VarkaClassFileGen {

  /** Binary name of the engine class owning the batch kernels. */
  val DateVectorOpsClassName = "org.apache.spark.sql.varka.vector.DateVectorOps"

  val AddDaysMethodDescriptor = "(JJIJJII)V"
  val SubDaysMethodDescriptor = "(JJIJJII)V"
  val DateDiffMethodDescriptor = "(JJIJJIJJI)V"

  /** The Varka-eligible ops of a projection's expression list, in order. */
  def eligibleOps(projectList: Seq[Expression]): Seq[ClassFileGenOp] = {
    projectList.collect {
      case e: ClassFileCodegenSupport if e.isClassFileGenEligible => e.classFileGenOp
    }
  }

  /**
   * A plain date attribute: an [[Attribute]] of [[DateType]]. The batch kernels read a
   * whole Arrow column's buffers, so only direct column references are MVP-eligible.
   */
  private[expressions] def isDateAttribute(e: Expression): Boolean = {
    e.isInstanceOf[Attribute] && e.dataType == DateType
  }

  /**
   * Folds a literal integer/short/byte `days` argument to an int offset.
   */
  private[expressions] def foldDaysOffset(days: Expression): Option[Int] = days match {
    case Literal(value: Number, _) => Some(value.intValue())
    case _ => None
  }
}
