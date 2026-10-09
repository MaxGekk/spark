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

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Everything one composition fuzz run reads, as a value (VARKA-294): the picked entries, the
 * emit options, and the seed the wide check draws its batches from. A fuzzer draws one and runs
 * it; keeping the two apart is what lets a shrinker run the same case again with a part of it
 * changed, as [[VarkaFuzzCase]] does for the IR fuzzer.
 *
 * `kind` is `projection`, `predicate` or `wide`, the three tests of
 * `VarkaCoverageCompositionFuzzSuite`; an entry is a projection's output, a filter's conjunct, or
 * a wide projection's output already moved onto its copy of the table's columns. `label` is the
 * context a failure starts with, written once at the draw.
 */
final case class VarkaCompositionCase(
    kind: String,
    entries: Vector[VarkaCompositionCase.Entry],
    options: VarkaEmitOptions,
    checkSeed: Long,
    label: String)

object VarkaCompositionCase {

  /** A picked row's SQL and its resolved expression. */
  final case class Entry(sql: String, expr: Expression)

  /** The case for a person: kind, the delta from the defaults, one entry per line. */
  def describe(c: VarkaCompositionCase): String =
    s"kind=${c.kind} options=${VarkaFuzzCase.optionDelta(c.options).mkString("{", ", ", "}")} " +
      s"entries=${c.entries.size}:" +
      c.entries.map(e => s"\n    ${e.expr.sql}").mkString
}
