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

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType

/**
 * Everything one fuzz run reads, as a value: the trees, the options and the batch. A fuzzer draws
 * one and runs it; keeping the two apart is what lets a shrinker run the same case again with a
 * part of it changed (VARKA-277).
 *
 * Both lanes share the shape: `lits` and `data` are longs, which an int-lane case holds as the
 * ints it drew and the run narrows back. A null bitmap is materialised, one array per input,
 * where the draw used a function of the row, so that a case can be cut by length or have a null
 * cleared. `smallOrdinal` and `levelOrdinal` name the two inputs whose data the grammar
 * constrains (a month-arithmetic bound and the codes of a truncation level), which a change to
 * the data must leave alone; -1 where the shape has none. `label` is the context a failure
 * message starts with: the seed, the iteration and the parts of the case, written once at the
 * draw.
 */
final case class VarkaFuzzCase(
    lane: LaneType,
    roots: Seq[VarkaVectorIR],
    numInputs: Int,
    lits: Array[Long],
    length: Int,
    nulls: Array[Array[Boolean]],
    data: Array[Array[Long]],
    forceMasked: Boolean,
    options: VarkaEmitOptions,
    smallOrdinal: Int,
    levelOrdinal: Int,
    label: String)

object VarkaFuzzCase {

  /** The options that differ from the defaults, as `name=value`; empty for the defaults. */
  def optionDelta(options: VarkaEmitOptions): Seq[String] = {
    import scala.jdk.CollectionConverters._
    VarkaEmitOption.TABLE.asScala.toSeq
      .filter(o => o.text(options) != o.text(VarkaEmitOptions.DEFAULTS))
      .map(o => s"${o.name}=${o.text(options)}")
  }

  /** The case on one line for a person: the trees, the delta from the defaults and the batch. */
  def describe(c: VarkaFuzzCase): String = {
    def rows(col: Int): String = {
      val shown = (0 until c.length.min(8)).map { i =>
        if (c.nulls(col)(i)) "null" else c.data(col)(i).toString
      }
      shown.mkString("[", ",", if (c.length > 8) ",...]" else "]")
    }
    s"lane=${c.lane.toString.toLowerCase(Locale.ROOT)} " +
      s"roots=${c.roots.map(r => VarkaVectorIR.canonical(r)).mkString("[", ", ", "]")} " +
      s"options=${optionDelta(c.options).mkString("{", ", ", "}")} " +
      s"length=${c.length} forceMasked=${c.forceMasked} " +
      s"literals=${c.lits.mkString("[", ",", "]")} " +
      s"columns=${(0 until c.numInputs).map(rows).mkString(" ")}"
  }
}

