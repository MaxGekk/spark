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

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Cond

/**
 * The reference evaluator's Scala face: [[VarkaReference]], the Java class that holds the
 * semantics (VARKA-310), over the `Seq[Option[Int]]` rows its Scala callers build. Each method
 * converts the row once and the answer back; nothing is decided here.
 */
object VarkaReferenceEvaluator {

  def evalValue(node: VarkaVectorIR, row: Seq[Option[Int]], lits: Array[Int]): Option[Int] = {
    val v = VarkaReference.value(node, row.map(_.getOrElse(0)).toArray, row.map(_.isEmpty).toArray,
      lits)
    if (v.isPresent) Some(v.getAsInt) else None
  }

  def evalCond(cond: Cond, row: Seq[Option[Int]], lits: Array[Int]): Option[Boolean] =
    truth(VarkaReference.cond(cond, row.map(_.getOrElse(0)).toArray, row.map(_.isEmpty).toArray,
      lits))

  def evalLong(node: VarkaVectorIR, row: Seq[Option[Long]], lits: Array[Long]): Option[Long] = {
    val v = VarkaReference.longValue(node, row.map(_.getOrElse(0L)).toArray,
      row.map(_.isEmpty).toArray, lits)
    if (v.isPresent) Some(v.getAsLong) else None
  }

  def evalCondLong(cond: Cond, row: Seq[Option[Long]], lits: Array[Long]): Option[Boolean] =
    truth(VarkaReference.longCond(cond, row.map(_.getOrElse(0L)).toArray,
      row.map(_.isEmpty).toArray, lits))

  private def truth(t: VarkaReference.Truth): Option[Boolean] = t match {
    case VarkaReference.Truth.TRUE => Some(true)
    case VarkaReference.Truth.FALSE => Some(false)
    case VarkaReference.Truth.UNKNOWN => None
  }
}
