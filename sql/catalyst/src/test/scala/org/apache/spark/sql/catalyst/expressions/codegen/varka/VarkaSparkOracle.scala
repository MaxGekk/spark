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

import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, GenericInternalRow, RuntimeReplaceable}
import org.apache.spark.sql.types.{DataType, DateType, DayTimeIntervalType, IntegerType, LongType, TimeType, YearMonthIntervalType}

/**
 * Spark's own answer for each output of a compiled kernel (VARKA-276): the Catalyst expression the
 * output was compiled from, evaluated by its interpreted `eval` on the row the kernel saw, with no
 * session. [[VarkaKernelCheck]] holds [[VarkaReferenceEvaluator]] to it on every row before it
 * holds the kernel to the reference, so the oracle of both fuzzers is itself checked against
 * Spark (PQS tests its interpreter against the engine the same way, Rigger and Su, pp. 12-13).
 *
 * `outputs(o)` is output `o`'s expression and `inputs(i)` the attribute kernel input `i` reads.
 * A lane value becomes Catalyst's internal value of the input's type, which for every type a
 * kernel reads is the lane value itself: days for a date, months for a year-month interval,
 * nanoseconds for a `TIME`, microseconds for a day-time interval.
 */
class VarkaSparkOracle(outputs: Seq[Expression], inputs: Seq[Attribute]) {

  private val bound: Seq[Expression] =
    outputs.map(e => BindReferences.bindReference(replaced(e), inputs))

  /**
   * `e` with each `RuntimeReplaceable` swapped for its replacement, as the optimizer's
   * `ReplaceExpressions` does before anything evaluates it: `extract` and its kin have no `eval`.
   */
  private def replaced(e: Expression): Expression = {
    val next = e.transformDown { case r: RuntimeReplaceable => r.replacement }
    if (next.fastEquals(e)) e else replaced(next)
  }

  private val types: Seq[DataType] = inputs.map(_.dataType)

  /** Spark's answer for output `o` on a row of lane values, `None` for a null input. */
  def answer(o: Int, row: Seq[Option[Long]]): VarkaSparkOracle.Answer = {
    val values = row.zip(types).map {
      case (None, _) => null
      case (Some(v), DateType | IntegerType | _: YearMonthIntervalType) => v.toInt
      case (Some(v), LongType | _: TimeType | _: DayTimeIntervalType) => v
      case (_, t) => throw new IllegalArgumentException(s"no kernel input of type $t")
    }
    try {
      bound(o).eval(new GenericInternalRow(values.toArray[Any])) match {
        case null => VarkaSparkOracle.Null
        case b: Boolean => VarkaSparkOracle.Value(if (b) 1L else 0L)
        case i: Int => VarkaSparkOracle.Value(i.toLong)
        case l: Long => VarkaSparkOracle.Value(l)
        case other => throw new IllegalArgumentException(
          s"output $o evaluated to a ${other.getClass.getSimpleName}, which no kernel stores")
      }
    } catch {
      case e: ArithmeticException => VarkaSparkOracle.Raised(e)
      case e: org.apache.spark.SparkThrowable => VarkaSparkOracle.Raised(e.asInstanceOf[Throwable])
      case e: java.time.DateTimeException => VarkaSparkOracle.Raised(e)
    }
  }

  /** Output `o`'s expression, for a failure message. */
  def sql(o: Int): String = outputs(o).sql
}

object VarkaSparkOracle {
  sealed trait Answer
  /** A value, a predicate's `true` and `false` as 1 and 0. */
  case class Value(v: Long) extends Answer
  case object Null extends Answer
  /** The error Spark raised on the row, where a kernel declines the batch instead. */
  case class Raised(e: Throwable) extends Answer
}
