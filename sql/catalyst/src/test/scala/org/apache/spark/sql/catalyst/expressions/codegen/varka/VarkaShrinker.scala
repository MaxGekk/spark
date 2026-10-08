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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR._

/**
 * Reduces a failing fuzz case to a small one that fails the same way (VARKA-277): delta
 * debugging, Zeller and Hildebrandt's ddmin, whose result is 1-minimal, over the case's parts in
 * the order the plan gives - the roots, each tree, the options, the batch - repeated until a
 * pass changes nothing.
 *
 * A candidate is kept only if it fails with the original's [[VarkaFailureSignature]], so a
 * reduction cannot slide from one bug to another. A run that passes, is skipped or fails
 * differently is "not smaller".
 */
object VarkaShrinker {

  /** What a shrink produced and what it cost. */
  final case class Shrunk(
      small: VarkaFuzzCase, runs: Int, millis: Long, stoppedEarly: Boolean)

  /** The number of IR nodes of a tree. */
  def size(node: VarkaVectorIR): Int = 1 + VarkaVectorIR.childrenOf(node).map(size).sum

  /**
   * ddmin: the smallest sublist of `items`, in order, that still makes `fails` true, 1-minimal
   * (removing any one item stops it failing). `fails` is asked about the whole list first by the
   * caller's contract and is not asked again here; an empty result is possible when `fails`
   * holds for the empty list.
   */
  def ddmin[A](items: Vector[A])(fails: Vector[A] => Boolean): Vector[A] = {
    var current = items
    var n = 2
    while (current.size >= 2) {
      val chunk = math.max(1, (current.size + n - 1) / n)
      val parts = current.grouped(chunk).toVector
      parts.find(fails) match {
        case Some(part) =>
          current = part
          n = 2
        case None =>
          // At n = 2 a complement is the other part, which was just tried.
          val complements = if (n == 2) Nil else parts.indices.map { i =>
            parts.patch(i, Nil, 1).flatten
          }
          complements.find(c => c.nonEmpty && fails(c)) match {
            case Some(complement) =>
              current = complement
              n = math.max(n - 1, 2)
            case None =>
              if (n >= current.size) return current
              n = math.min(current.size, 2 * n)
          }
      }
    }
    if (current.size == 1 && fails(Vector.empty)) Vector.empty else current
  }

  /**
   * Shrinks `original`, which fails with `signature` under `outcome`. At most `maxRuns` runs and
   * `maxMillis` of wall time are spent; a shrink that hits either returns what it has.
   */
  def shrink(
      original: VarkaFuzzCase,
      signature: VarkaFailureSignature,
      outcome: VarkaFuzzCase => Option[VarkaFailureSignature],
      maxRuns: Int = 600,
      maxMillis: Long = 60000L): Shrunk = {
    val start = System.nanoTime()
    var runs = 0
    var stopped = false
    def elapsed: Long = (System.nanoTime() - start) / 1000000L
    def stillFails(c: VarkaFuzzCase): Boolean = {
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
      best = shrinkRoots(best, stillFails)
      best = shrinkTrees(best, stillFails)
      best = shrinkOptions(best, stillFails)
      best = shrinkBatch(best, stillFails)
      changed = !sameCase(before, best)
    }
    Shrunk(best, runs, elapsed, stopped)
  }

  private def sameCase(a: VarkaFuzzCase, b: VarkaFuzzCase): Boolean =
    a.roots == b.roots && a.options == b.options && a.length == b.length &&
      a.forceMasked == b.forceMasked && a.nulls.map(_.toSeq).toSeq == b.nulls.map(_.toSeq).toSeq &&
      a.data.map(_.toSeq).toSeq == b.data.map(_.toSeq).toSeq

  private def shrinkRoots(c: VarkaFuzzCase, fails: VarkaFuzzCase => Boolean): VarkaFuzzCase = {
    if (c.roots.size < 2) c
    else {
      val kept = ddmin(c.roots.toVector)(rs => rs.nonEmpty && fails(c.copy(roots = rs)))
      c.copy(roots = kept)
    }
  }

  // ---- trees ----------------------------------------------------------------------------

  private type Path = Vector[Int]

  private def nodeAt(root: VarkaVectorIR, path: Path): VarkaVectorIR =
    path.foldLeft(root)((n, i) => VarkaVectorIR.childrenOf(n)(i))

  /** The positions of a tree, root first and then level by level. */
  private def levelOrder(root: VarkaVectorIR): Seq[Path] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[Path]
    var level: Seq[Path] = Seq(Vector.empty)
    while (level.nonEmpty) {
      out ++= level
      level = level.flatMap { p =>
        VarkaVectorIR.childrenOf(nodeAt(root, p)).indices.map(i => p :+ i)
      }
    }
    out.toSeq
  }

  /**
   * `root` with the node at `path` replaced, rebuilt up the spine by `withChildren`; None when a
   * constructor refuses the result, which means the replacement does not type there.
   */
  private def replaceAt(root: VarkaVectorIR, path: Path, repl: VarkaVectorIR)
      : Option[VarkaVectorIR] = {
    if (path.isEmpty) Some(repl)
    else {
      val children = VarkaVectorIR.childrenOf(root)
      replaceAt(children(path.head), path.tail, repl).flatMap { child =>
        try {
          Some(VarkaVectorIR.withChildren(root, children.updated(path.head, child): _*))
        } catch {
          case _: IllegalArgumentException => None
        }
      }
    }
  }

  /**
   * The operand positions whose value domain the grammar constrains, which a move must leave
   * alone: the month count of `add_months` and the level of a dynamic truncation read the two
   * special columns, whose data is drawn to fit them.
   */
  private def isProtected(root: VarkaVectorIR, path: Path): Boolean = {
    path.indices.exists { depth =>
      val parent = nodeAt(root, path.take(depth))
      val index = path(depth)
      parent match {
        case _: AddMonths => index == 1
        case _: TruncDateDynamic => index == 1
        case _ => false
      }
    }
  }

  /** The smaller trees to try in `node`'s place: a leaf first, then each same-kind child. */
  private def replacements(node: VarkaVectorIR, c: VarkaFuzzCase): Seq[VarkaVectorIR] = {
    val leaf: Seq[VarkaVectorIR] = node match {
      case _: ColumnRef | _: LiteralSlot => Nil
      case cond: Cond =>
        val columns = usableColumns(c, node.laneType())
        columns.take(1).map(o => new IsNotNull(new ColumnRef(o, node.laneType())): VarkaVectorIR)
          .filterNot(_ == cond)
      case _ =>
        val lane = node.laneType()
        usableColumns(c, lane).map(o => new ColumnRef(o, lane): VarkaVectorIR) ++
          (0 until c.lits.length).map(j => new LiteralSlot(j, lane): VarkaVectorIR)
    }
    val hoisted = VarkaVectorIR.childrenOf(node).toSeq.filter { ch =>
      ch.isInstanceOf[Cond] == node.isInstanceOf[Cond] && ch.laneType() == node.laneType()
    }
    leaf ++ hoisted
  }

  /** The columns a leaf may read: all but the two whose data the grammar constrains. */
  private def usableColumns(c: VarkaFuzzCase, lane: LaneType): Seq[Int] =
    (0 until c.numInputs).filterNot(o => o == c.smallOrdinal || o == c.levelOrdinal)

  private def shrinkTrees(c: VarkaFuzzCase, fails: VarkaFuzzCase => Boolean): VarkaFuzzCase = {
    var current = c
    for (i <- current.roots.indices) {
      var improved = true
      while (improved) {
        improved = false
        val root = current.roots(i)
        val candidates = levelOrder(root).iterator
          .filterNot(p => isProtected(root, p))
          .flatMap { p =>
            replacements(nodeAt(root, p), current).iterator.flatMap(r => replaceAt(root, p, r))
          }
          .filter(t => size(t) < size(root))
        val found = candidates.find { t =>
          fails(current.copy(roots = current.roots.updated(i, t)))
        }
        found.foreach { t =>
          current = current.copy(roots = current.roots.updated(i, t))
          improved = true
        }
      }
    }
    current
  }

  // ---- options --------------------------------------------------------------------------

  private def shrinkOptions(c: VarkaFuzzCase, fails: VarkaFuzzCase => Boolean): VarkaFuzzCase = {
    val changed = VarkaEmitOption.TABLE.asScala.toVector
      .filter(o => o.text(c.options) != o.text(VarkaEmitOptions.DEFAULTS))
    def with_(kept: Vector[VarkaEmitOption]): VarkaEmitOptions = {
      val builder = c.options.toBuilder
      changed.filterNot(kept.contains).foreach(_.applyDefault(builder))
      builder.build()
    }
    if (changed.isEmpty) c
    else {
      val kept = ddmin(changed) { subset => fails(c.copy(options = with_(subset))) }
      c.copy(options = with_(kept))
    }
  }

  // ---- the batch ------------------------------------------------------------------------

  /** `c` over only the rows `rows`, in order. */
  private def onlyRows(c: VarkaFuzzCase, rows: Vector[Int]): VarkaFuzzCase = c.copy(
    length = rows.size,
    nulls = c.nulls.map(col => rows.map(col(_)).toArray),
    data = c.data.map(col => rows.map(col(_)).toArray),
    // The harness never forces the masked path at length 1 (a null count equal to the length
    // is the contract's all-null column).
    forceMasked = c.forceMasked && rows.size > 1)

  private def shrinkBatch(c: VarkaFuzzCase, fails: VarkaFuzzCase => Boolean): VarkaFuzzCase = {
    var current = c
    if (current.length > 1) {
      val rows = ddmin((0 until current.length).toVector) { rs =>
        rs.nonEmpty && fails(onlyRows(current, rs))
      }
      current = onlyRows(current, rows)
    }
    if (current.forceMasked) {
      val plain = current.copy(forceMasked = false)
      if (fails(plain)) current = plain
    }
    // Nulls: all valid if the failure survives it, else column by column.
    if (current.nulls.exists(_.exists(identity))) {
      val clear = current.copy(nulls = current.nulls.map(col => Array.fill(col.length)(false)))
      if (fails(clear)) current = clear
      else {
        for (col <- 0 until current.numInputs if current.nulls(col).exists(identity)) {
          val one = current.copy(nulls = current.nulls.updated(
            col, Array.fill(current.length)(false)))
          if (fails(one)) current = one
        }
      }
    }
    // Data: all zero, else all one, for each column the grammar does not constrain; a column
    // already simpler than a value is not moved to it.
    def rank(column: Array[Long]): Int =
      if (column.forall(_ == 0L)) 0 else if (column.forall(_ == 1L)) 1 else 2
    for (col <- 0 until current.numInputs
         if col != current.smallOrdinal && col != current.levelOrdinal) {
      val filled = Seq(0L, 1L).iterator
        .filter(value => value < rank(current.data(col)))
        .map(value => current.copy(data = current.data.updated(
          col, Array.fill(current.length)(value))))
        .find(fails)
      filled.foreach(current = _)
    }
    current
  }
}
