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

import java.lang.classfile.{ClassBuilder, ClassFile, CodeBuilder, TypeKind}
import java.lang.constant.{ClassDesc, ConstantDescs, MethodTypeDesc}
import java.lang.reflect.AccessFlag

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}

/**
 * The declarative `invokestatic` contract of a Varka batch kernel (Task 4). The argument
 * order of the DateVectorOps methods IS the JVM stack order, so the descriptor fully pins
 * the bytecode emission. Catalyst owns the Class-File assembly on the Java 25+ baseline;
 * the engine module is referenced only by name.
 *
 * @param ownerClassName the binary name of the class owning the kernel.
 * @param methodName the static method name.
 * @param methodDescriptor the JVM method descriptor, e.g. `(JJIJJII)V`.
 */
case class ClassFileGenOp(
    ownerClassName: String,
    methodName: String,
    methodDescriptor: String)

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

  override def genCode(ctx: CodegenContext): ExprCode = {
    ctx.registerClassFileGenExpression(this)
    super.genCode(ctx)
  }
}

/**
 * Plan-level helpers for the Varka Class-File path (Task 4). Catalyst owns the Class-File
 * assembly (Java 25+ baseline), so the kernel contract below is both declared and assembled
 * here; the engine-side integration test independently cross-checks the descriptors against
 * `DateVectorOps` via reflection.
 */
object VarkaClassFileGen {

  /** The Varka-eligible ops of a projection's expression list, in order. */
  def eligibleOps(projectList: Seq[Expression]): Seq[ClassFileGenOp] = {
    projectList.collect {
      case Alias(e: ClassFileCodegenSupport, _) if e.isClassFileGenEligible => e.classFileGenOp
      case e: ClassFileCodegenSupport if e.isClassFileGenEligible => e.classFileGenOp
    }
  }

  /**
   * Assembles the class bytes of a probe that invokes the op's kernel: a public class with a
   * default constructor and a static `run` method that loads the kernel parameters in order
   * and invokes them with a single `invokestatic`. A later task routes eligible expressions
   * to this and defines the result via the engine's VarkaClassLoader.
   */
  def assembleKernelClass(className: String, op: ClassFileGenOp): Array[Byte] = {
    val classDesc = ClassDesc.of(className)
    val kernelDesc = MethodTypeDesc.ofDescriptor(op.methodDescriptor)
    ClassFile.of().build(classDesc, (b: ClassBuilder) => b
      .withFlags(AccessFlag.PUBLIC)
      .withMethodBody("<init>", MethodTypeDesc.of(ConstantDescs.CD_void),
        AccessFlag.PUBLIC.mask(),
        (cb: CodeBuilder) => {
          cb.aload(0)
          cb.invokespecial(ConstantDescs.CD_Object, "<init>",
            MethodTypeDesc.of(ConstantDescs.CD_void))
          cb.return_()
          ()
        })
      .withMethodBody("run", kernelDesc, AccessFlag.PUBLIC.mask() | AccessFlag.STATIC.mask(),
        (cb: CodeBuilder) => {
          var slot = 0
          var i = 0
          while (i < kernelDesc.parameterCount()) {
            val pDesc = kernelDesc.parameterList().get(i).descriptorString()
            val kind = TypeKind.fromDescriptor(pDesc.substring(0, 1))
            if (kind == TypeKind.LONG) {
              cb.lload(slot)
              slot += 2
            } else {
              cb.iload(slot)
              slot += 1
            }
            i += 1
          }
          cb.invokestatic(ClassDesc.of(op.ownerClassName), op.methodName, kernelDesc)
          cb.return_()
          ()
        }))
  }
}
