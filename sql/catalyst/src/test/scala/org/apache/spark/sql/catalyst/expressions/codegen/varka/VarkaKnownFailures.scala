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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

/**
 * The fuzz failures that are known (VARKA-277), `sql/varka/fuzz/known_failures.tsv`: tab
 * separated lane (`int` or `long`, or `projection`, `predicate` or `wide` for the composition
 * fuzzer), seed, iteration, the signature's kind, its text, and the
 * reason, which names the plan row or ticket that owns the bug; `#` starts a comment line. A
 * fuzzer's failure with a listed signature is reported as known and does not fail the run, and a
 * listed entry that its (lane, seed, iteration) no longer reproduces is stale and fails, so the
 * list can only shrink once a bug is fixed - as `matrix/skips.tsv` does for the option matrix.
 */
object VarkaKnownFailures {

  /** The fuzzers a lane names: the IR fuzzer's two lanes and the composition fuzzer's three. */
  val lanes = Set("int", "long", "projection", "predicate", "wide")

  /** The committed list, relative to `spark.test.home`. */
  val PATH = "sql/varka/fuzz/known_failures.tsv"

  final case class Entry(
      lane: String, seed: Long, iteration: Int, signature: VarkaFailureSignature, reason: String)

  def parse(text: String): Seq[Entry] = text.linesIterator
    .map(_.stripTrailing()).filterNot(l => l.isBlank || l.startsWith("#")).map { line =>
      line.split("\t", -1) match {
        case Array(lane, seed, iteration, kind, message, reason)
            if lanes.contains(lane) && reason.nonEmpty =>
          Entry(lane, seed.toLong, iteration.toInt, VarkaFailureSignature(kind, message), reason)
        case _ => throw new IllegalArgumentException(
          s"$PATH: expected lane<TAB>seed<TAB>iteration<TAB>kind<TAB>text<TAB>reason, got '$line'")
      }
    }.toSeq

  /** The committed list. */
  lazy val entries: Seq[Entry] = {
    val home = sys.props.getOrElse("spark.test.home", ".")
    val file = new File(home, PATH)
    if (!file.exists()) Seq.empty
    else parse(new String(Files.readAllBytes(file.toPath), StandardCharsets.UTF_8))
  }

  def isKnown(list: Seq[Entry], signature: VarkaFailureSignature): Boolean =
    list.exists(_.signature == signature)

  /** The entries whose case, replayed, no longer fails with the signature they list. */
  def stale(list: Seq[Entry], replay: Entry => Option[VarkaFailureSignature]): Seq[Entry] =
    list.filterNot(e => replay(e).contains(e.signature))
}
