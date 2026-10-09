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
 * Reduces a failing composition case to a small one that fails the same way (VARKA-294): the
 * entries by delta debugging, each entry's expression by replacing a node with a child of its
 * own data type, then the emit options, repeated until a pass changes nothing. The core is
 * [[VarkaShrinker]]'s: `ddmin`, the failure signature, and the options reduction.
 *
 * A candidate is kept only if it fails with the original's [[VarkaFailureSignature]]; a run that
 * passes, is skipped or fails differently is "not smaller".
 */
object VarkaCompositionShrinker {

  /** What a shrink produced and what it cost. */
  final case class Shrunk(
      small: VarkaCompositionCase, runs: Int, millis: Long, stoppedEarly: Boolean, stable: Boolean)

  /** The number of nodes of an expression tree. */
  def size(e: Expression): Int = 1 + e.children.map(size).sum

  def shrink(
      original: VarkaCompositionCase,
      signature: VarkaFailureSignature,
      outcome: VarkaCompositionCase => Option[VarkaFailureSignature],
      maxRuns: Int = 600,
      maxMillis: Long = 60000L): Shrunk = {
    val start = System.nanoTime()
    var runs = 0
    var stopped = false
    def elapsed: Long = (System.nanoTime() - start) / 1000000L
    def stillFails(c: VarkaCompositionCase): Boolean = {
      if (runs >= maxRuns || elapsed >= maxMillis) {
        stopped = true
        false
      } else {
        runs += 1
        outcome(c).contains(signature)
      }
    }

    var best = original
    var changed = true
    while (changed && !stopped) {
      val before = best
      best = shrinkEntries(best, stillFails)
      best = shrinkTrees(best, stillFails)
      best = best.copy(options = VarkaShrinker.shrinkOptionsOf(best.options)(o =>
        stillFails(best.copy(options = o))))
      changed = before.entries != best.entries || before.options != best.options
    }
    // A failure that depends on the JIT may not repeat: the result is run three times more, and
    // is reported as unstable unless every run fails the same way.
    val stable = (0 until 3).forall(_ => outcome(best).contains(signature))
    Shrunk(best, runs, elapsed, stopped, stable)
  }

  private def shrinkEntries(
      c: VarkaCompositionCase, fails: VarkaCompositionCase => Boolean): VarkaCompositionCase = {
    if (c.entries.size < 2) c
    else {
      val kept = VarkaShrinker.ddmin(c.entries)(es => es.nonEmpty && fails(c.copy(entries = es)))
      c.copy(entries = kept)
    }
  }

  private type Path = Vector[Int]

  private def nodeAt(root: Expression, path: Path): Expression =
    path.foldLeft(root)((n, i) => n.children(i))

  /** The positions of a tree, root first and then level by level. */
  private def levelOrder(root: Expression): Seq[Path] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[Path]
    var level: Seq[Path] = Seq(Vector.empty)
    while (level.nonEmpty) {
      out ++= level
      level = level.flatMap(p => nodeAt(root, p).children.indices.map(i => p :+ i))
    }
    out.toSeq
  }

  /** `root` with the node at `path` replaced by `repl`. */
  private def replaceAt(root: Expression, path: Path, repl: Expression): Expression =
    if (path.isEmpty) repl
    else {
      val kids = root.children.toVector
      root.withNewChildren(kids.updated(path.head, replaceAt(kids(path.head), path.tail, repl)))
    }

  /**
   * Each entry's expression, root first: a node is replaced by one of its children of the same
   * data type, which only builds shapes the analyzer already typed. The first replacement that
   * still fails is kept and the entry is walked again.
   */
  private def shrinkTrees(
      c: VarkaCompositionCase, fails: VarkaCompositionCase => Boolean): VarkaCompositionCase = {
    var current = c
    for (i <- c.entries.indices if i < current.entries.size) {
      var progress = true
      while (progress) {
        progress = false
        val root = current.entries(i).expr
        val replacements = levelOrder(root).iterator.flatMap { path =>
          val node = nodeAt(root, path)
          node.children.iterator.filter(_.dataType == node.dataType)
            .map(child => replaceAt(root, path, child))
        }
        replacements.find { smaller =>
          fails(current.copy(entries = current.entries.updated(
            i, current.entries(i).copy(expr = smaller))))
        }.foreach { smaller =>
          current = current.copy(entries = current.entries.updated(
            i, current.entries(i).copy(expr = smaller)))
          progress = true
        }
      }
    }
    current
  }
}
