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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, DateAdd, DateDiff, DateSub, Literal}
import org.apache.spark.sql.types.{DateType, IntegerType}

/**
 * Task 4: declarative Varka Class-File codegen support for DateAdd/DateSub/DateDiff.
 * Asserts the emission contract (owner/name/descriptor), the eligibility matrix, and that
 * genCode registers into the [[CodegenContext]] while keeping the string path intact.
 */
class ClassFileCodegenSupportSuite extends SparkFunSuite {

  private val startAttr = AttributeReference("start", DateType)()
  private val endAttr = AttributeReference("end", DateType)()
  private val otherDateAttr = AttributeReference("other", DateType)()

  test("DateAdd emission contract") {
    val op = DateAdd(startAttr, Literal(3)).classFileGenOp
    assert(op.ownerClassName == VarkaClassFileGen.DateVectorOpsClassName)
    assert(op.methodName == "vectorAddDays")
    assert(op.methodDescriptor == "(JJIJJII)V")
    assert(op.kind == ClassFileGenOpKind.DateAdd)
  }

  test("DateSub emission contract") {
    val op = DateSub(startAttr, Literal(3)).classFileGenOp
    assert(op.ownerClassName == VarkaClassFileGen.DateVectorOpsClassName)
    assert(op.methodName == "vectorSubDays")
    assert(op.methodDescriptor == "(JJIJJII)V")
    assert(op.kind == ClassFileGenOpKind.DateSub)
  }

  test("DateDiff emission contract") {
    val op = DateDiff(endAttr, startAttr).classFileGenOp
    assert(op.ownerClassName == VarkaClassFileGen.DateVectorOpsClassName)
    assert(op.methodName == "vectorDateDiff")
    assert(op.methodDescriptor == "(JJIJJIJJI)V")
    assert(op.kind == ClassFileGenOpKind.DateDiff)
  }

  test("daysOffsetConstant folds integral literals") {
    assert(DateAdd(startAttr, Literal(3)).daysOffsetConstant == Some(3))
    assert(DateAdd(startAttr, Literal(3: Short)).daysOffsetConstant == Some(3))
    assert(DateAdd(startAttr, Literal(3.toByte)).daysOffsetConstant == Some(3))
    assert(DateAdd(startAttr, Literal(-7)).daysOffsetConstant == Some(-7))
    assert(DateAdd(startAttr, Literal(null, IntegerType)).daysOffsetConstant.isEmpty)
    assert(DateSub(startAttr, Literal(2)).daysOffsetConstant == Some(2))
  }

  test("DateAdd/DateSub eligibility requires a plain date attribute and foldable days") {
    assert(DateAdd(startAttr, Literal(3)).isClassFileGenEligible)
    assert(DateSub(startAttr, Literal(3)).isClassFileGenEligible)
    assert(!DateAdd(startAttr, AttributeReference("d", IntegerType)()).isClassFileGenEligible)
    assert(!DateAdd(startAttr, Literal(null, IntegerType)).isClassFileGenEligible)
    assert(!DateAdd(Literal(19244, DateType), Literal(3)).isClassFileGenEligible)
    assert(!DateSub(startAttr, AttributeReference("d", IntegerType)()).isClassFileGenEligible)
  }

  test("DateDiff eligibility requires two plain date attributes") {
    assert(DateDiff(endAttr, startAttr).isClassFileGenEligible)
    assert(!DateDiff(endAttr, Literal(19244, DateType)).isClassFileGenEligible)
    assert(!DateDiff(Literal(19244, DateType), startAttr).isClassFileGenEligible)
  }

  test("VarkaClassFileGen.eligibleOps collects eligible ops in order") {
    val ineligible = AttributeReference("d", IntegerType)()
    val projectList = Seq(
      DateAdd(startAttr, Literal(3)),
      Literal(1),
      DateDiff(endAttr, otherDateAttr),
      DateSub(startAttr, Literal(1)),
      DateAdd(startAttr, ineligible))
    val ops = VarkaClassFileGen.eligibleOps(projectList)
    assert(ops.map(_.methodName) == Seq("vectorAddDays", "vectorDateDiff", "vectorSubDays"))
    assert(ops.forall(_.ownerClassName == VarkaClassFileGen.DateVectorOpsClassName))
  }

  test("genCode registers into the CodegenContext and keeps the string path") {
    val ctx = new CodegenContext
    val add = DateAdd(BoundReference(0, DateType, nullable = true), Literal(3))
    val code = add.genCode(ctx)
    assert(ctx.isClassFileGenEligible)
    assert(ctx.classFileGenExpressions.toSeq == Seq(add))
    assert(code.code.toString.nonEmpty)
    assert(code.code.toString.contains("+ 3"))
  }

  test("non-Varka expressions do not register") {
    val ctx = new CodegenContext
    Literal(1).genCode(ctx)
    assert(!ctx.isClassFileGenEligible)
    assert(ctx.classFileGenExpressions.isEmpty)
  }
}