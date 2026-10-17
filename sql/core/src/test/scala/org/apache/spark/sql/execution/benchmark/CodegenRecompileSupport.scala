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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.LeafExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodegenContext, CodeGenerator, EmptyBlock, ExprCode, FalseLiteral, JavaCode}
import org.apache.spark.sql.classic.ExpressionUtils
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
 * An integer constant whose generated code reads it from a field of the generated class, set
 * from the class's references when the class is initialized, instead of writing the value into
 * the source as `Literal` does. Two queries that differ only in the value therefore generate the
 * same source and share one compiled class, as two queries that differ only in a string literal
 * already do. It exists for `CodegenRecompileBenchmark`, which measures whether passing integer
 * literals this way would pay; it is not foldable, so the optimizer leaves it in place.
 */
case class IntSlot(value: Int) extends LeafExpression {
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = false
  override def foldable: Boolean = false
  override def eval(input: InternalRow): Any = value

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val ref = ctx.addReferenceObj("slot", Integer.valueOf(value), "java.lang.Integer")
    val field = ctx.addMutableState(CodeGenerator.JAVA_INT, "slot", v => s"$v = $ref.intValue();")
    ev.copy(code = EmptyBlock, isNull = FalseLiteral, value = JavaCode.global(field, IntegerType))
  }
}

/** The Scala side of `CodegenRecompileBenchmark`: what a Java body cannot reach by itself. */
object CodegenRecompileSupport {

  /** `IntSlot(value)` as a column. */
  def intSlot(value: Int): Column = ExpressionUtils.column(IntSlot(value))

  /**
   * The generated source of the largest whole stage codegen stage of `df`'s plan, the one that
   * holds its expressions, planned without adaptive execution, which otherwise creates the stages
   * of a plan with an exchange only when it runs.
   */
  def largestStageSource(df: DataFrame): CodeAndComment = {
    val conf = df.sparkSession.conf
    val key = SQLConf.ADAPTIVE_EXECUTION_ENABLED.key
    val adaptive = conf.get(key)
    conf.set(key, "false")
    try {
      val sources = df.queryExecution.executedPlan.collect {
        case w: WholeStageCodegenExec => w.doCodeGen()._2
      }
      require(sources.nonEmpty, "no whole stage codegen stage")
      sources.maxBy(_.body.length)
    } finally {
      conf.set(key, adaptive)
    }
  }
}
