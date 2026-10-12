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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR._

/**
 * Every small IR tree through the emitter, compared with the reference evaluator (VARKA-269).
 * [[VarkaIrFuzzSuite]] draws trees at random and so cannot say it met every corner of a size;
 * [[VarkaIrSeries]] enumerates every tree the grammar could draw up to a size, and this suite
 * runs each one, at the int lane and the long, through `VarkaKernelCheck`, which compares every
 * row with `VarkaReferenceEvaluator` and runs the body the batch did not take as well.
 *
 * The size is `-Dvarka.series.size`, 3 by default, which is the pull request's: about three
 * thousand int trees, every one meeting all four batches, in seconds. Size 4 cost the gate about
 * 400 seconds at each width, so it runs at night instead: inside the nightly fuzz job's size 5,
 * which holds every smaller tree, at the narrow width in the same job, and under every emitter
 * configuration in the weekly option matrix. An idle window runs size 6 (`dev/varka_series.sh`).
 * The larger sizes are spread across JVMs with `-Dvarka.series.slice=k/n`, a JVM taking every
 * n-th tree from the k-th - across JVMs and not threads, because the emitter is not shown safe
 * to call from several threads (row 307). Options come from `VarkaMatrix.base`, so the option
 * matrix's configurations reach this suite as they reach every other.
 *
 * A tree up to size 3 meets all four batches of [[VarkaIrSeries.intBatches]]; a larger one meets
 * the one its index picks, so each batch meets a quarter of them, unless
 * `-Dvarka.series.allBatches=true`.
 * The suite runs the whole series and then fails with the first twenty failures; the series is
 * ordered by size, so the first is a smallest one. A failure on the committed known-failures
 * list (`VarkaKnownFailures`) is logged and not counted, as the fuzzer treats it.
 */
class VarkaSmallTreeSuite extends SparkFunSuite with VarkaMatrixTests with VarkaOwnJvm {

  private val size = sys.props.get("varka.series.size").map(_.toInt).getOrElse(3)
  private val (slice, slices) = sys.props.get("varka.series.slice").map { s =>
    val Array(k, n) = s.split("/").map(_.toInt)
    (k, n)
  }.getOrElse((0, 1))
  private val allBatches = sys.props.get("varka.series.allBatches").contains("true")
  /** The largest size whose trees meet every batch. */
  private val fullBelow = 3
  private val classCounter = new AtomicInteger(0)

  // ---------------------------------------------------------------------------------------------
  // The batches
  // ---------------------------------------------------------------------------------------------

  /** The batches a tree meets: all four up to [[fullBelow]], else the one its index picks. */
  private def pick[B](batches: Seq[B], index: Int, treeSize: Int): Seq[B] =
    if (allBatches || treeSize <= fullBelow) batches else Seq(batches(index % batches.size))

  // ---------------------------------------------------------------------------------------------
  // Checking one shape
  // ---------------------------------------------------------------------------------------------

  /** The failures of one run: the first twenty, and how many there were. */
  private final class Failures {
    val first = mutable.ArrayBuffer.empty[String]
    var count = 0
    var checked = 0
    var skipped = 0

    def add(message: String): Unit = {
      count += 1
      if (first.size < 20) first += message
    }

    def assertNone(what: String): Unit = {
      logInfo(s"$what: $checked shapes checked, $skipped past the class-file cap, " +
        s"$count failed")
      assert(count == 0, s"$count of $checked $what failed; the first ${first.size}:\n" +
        first.mkString("\n"))
    }
  }

  /**
   * The shape's class under `options`, or None for a shape no form of the emitter holds: a size
   * decline under the byte budget is retried without it, as the fuzzer does, and a decline
   * without one is the class-file cap's.
   */
  private def emit(roots: Seq[VarkaVectorIR], numInputs: Int, options: VarkaEmitOptions,
      className: String): Option[Array[Byte]] = {
    def emitWith(o: VarkaEmitOptions): Array[Byte] = VarkaLoopEmitter.emit(className,
      roots.asJava, numInputs, VarkaIrSeries.Literals, null, null, o)
    try Some(emitWith(options)) catch {
      case _: VarkaEmitDeclined if options.methodByteBudget() > 0 =>
        try Some(emitWith(options.withMethodByteBudget(0))) catch {
          case _: VarkaEmitDeclined => None
        }
      case _: VarkaEmitDeclined => None
    }
  }

  /**
   * Emits the shape and compares it with the reference evaluator over `batches`, recording a
   * failure in `failures` - anything thrown, a mismatch, a non-zero status, a refusal.
   */
  private def check(roots: Seq[VarkaVectorIR], long: Boolean, index: Int, treeSize: Int,
      options: VarkaEmitOptions, failures: Failures): Unit = {
    val className = "org.apache.spark.sql.varka.execution.VarkaSmallTree" +
      s"${if (long) "Long" else ""}${classCounter.addAndGet(1)}"
    val numInputs = if (long) VarkaIrSeries.LongNumInputs else VarkaIrSeries.NumInputs
    val label = s"lane=${if (long) "long" else "int"} size=$treeSize index=$index " +
      s"roots=${roots.map(r => VarkaVectorIR.canonical(r)).mkString("[", ", ", "]")} " +
      s"options=${if (options.isDefault) "(defaults)" else options.canonical()}"
    try {
      emit(roots, numInputs, options, className) match {
        case None => failures.skipped += 1
        case Some(bytes) =>
          failures.checked += 1
          if (long) {
            for (b <- pick(VarkaIrSeries.longBatches, index, treeSize)) {
              VarkaKernelCheck.runAndCompareLong(s"$label batch=${b.name}", className, bytes,
                roots, numInputs, b.lits, b.batch)
            }
          } else {
            for (b <- pick(VarkaIrSeries.intBatches, index, treeSize)) {
              VarkaKernelCheck.runAndCompare(s"$label batch=${b.name}", className, bytes, roots,
                numInputs, b.lits, b.batch)
            }
          }
      }
    } catch {
      case e: VirtualMachineError => throw e
      case e: InterruptedException => throw e
      case t: Throwable =>
        val signature = VarkaFailureSignature.of(t, label)
        if (VarkaKnownFailures.isKnown(VarkaKnownFailures.entries, signature)) {
          logWarning(s"known failure, not counted: $signature")
        } else {
          failures.add(s"$label: ${Option(t.getMessage).getOrElse(t.getClass.getName)}")
        }
    }
  }

  /** Every root of `series` up to `upTo`, this JVM's slice of them, each checked alone. */
  private def runSeries(series: VarkaIrSeries.Series, long: Boolean, upTo: Int,
      options: VarkaEmitOptions): Failures = {
    val failures = new Failures
    var index = 0
    for ((treeSize, root) <- series.upTo(upTo)) {
      if (index % slices == slice) check(Seq(root), long, index, treeSize, options, failures)
      index += 1
    }
    failures
  }

  // ---------------------------------------------------------------------------------------------
  // The series
  // ---------------------------------------------------------------------------------------------

  test(s"every int tree to size $size matches the reference evaluator " +
      s"(slice $slice of $slices)") {
    runSeries(new VarkaIrSeries.IntSeries, long = false, size, VarkaMatrix.base)
      .assertNone(s"int trees to size $size")
  }

  test(s"every long tree to size $size matches the reference evaluator " +
      s"(slice $slice of $slices)") {
    runSeries(new VarkaIrSeries.LongSeries, long = true, size, VarkaMatrix.base)
      .assertNone(s"long trees to size $size")
  }

  test("a tree and its first value child as two outputs match the reference evaluator") {
    // The child is computed once and read by both outputs, which is what sharing and the slot
    // planner act on; one output alone never shows a slot the two disagree on.
    val failures = new Failures
    val series = new VarkaIrSeries.IntSeries
    var index = 0
    for (treeSize <- 2 to math.min(size, fullBelow); g <- series.value(treeSize)) {
      VarkaVectorIR.childrenOf(g.node).find(!_.isInstanceOf[Cond]).foreach { child =>
        if (index % slices == slice) {
          check(Seq(g.node, child), long = false, index, treeSize, VarkaMatrix.base, failures)
        }
        index += 1
      }
    }
    failures.assertNone("trees with their first value child")
  }

  test("a planted liveness bug fails the series by size 3, with a tree in the message") {
    // The series is only worth what it catches: the fuzzer's planted bug, which keeps a validity
    // word live that the liveness pass should have killed, has to fail it.
    val planted = VarkaEmitOption.named("misdescribeWordLiveness") match {
      case flag: VarkaEmitOption.Flag => flag.`with`(VarkaMatrix.base, true)
      case other => fail(s"not a flag: $other")
    }
    val failures = runSeries(new VarkaIrSeries.IntSeries, long = false, 3, planted)
    assert(failures.count > 0, "the planted bug failed no tree to size 3")
    assert(failures.first.head.contains("roots=["), failures.first.head)
  }

  // ---------------------------------------------------------------------------------------------
  // What the series leaves out, and what it reaches
  // ---------------------------------------------------------------------------------------------

  test("the trees the magnitude gates leave out are guarded or outside the contract, never " +
      "refused") {
    // The ungated series has the grammar's arms without its gates. Each tree it has and the
    // series does not is one of three: refused by the emitter, which would be a tree the gates
    // hide that the emitter cannot hold, and fails here; guarded by `Analysis`, so a batch may
    // decline and the reference evaluator has no spelling for it; or a calendar node over a
    // subtree that leaves the narrowed range with no guard to report it, which the compiler
    // never builds and only the gate keeps out.
    // Size 4 at any series size: the test generates and analyses, emitting nothing, so it costs
    // little, and the nightly series jobs filter it out, so the pull request's run is its only one.
    val upTo = 4
    def canonical(s: VarkaIrSeries.Series): Iterator[(VarkaVectorIR, String)] =
      s.upTo(upTo).map { case (_, root) => root -> VarkaVectorIR.canonical(root) }
    val gated = canonical(new VarkaIrSeries.IntSeries).map(_._2).toSet
    var refused = 0
    var outside = 0
    val guardedBy = mutable.Map.empty[String, Int].withDefaultValue(0)
    var omitted = 0
    for ((root, text) <- canonical(new VarkaIrSeries.IntSeries(gated = false))
        if !gated.contains(text)) {
      omitted += 1
      try {
        val refusals = VarkaLoopEmitter.refusalsForTest(Seq(root).asJava,
          VarkaIrSeries.NumInputs, VarkaIrSeries.Literals, VarkaMatrix.base).values().asScala
        if (refusals.isEmpty) {
          outside += 1
          if (outside <= 5) logInfo(s"outside the contract: $text")
        } else {
          refusals.toSet.foreach((r: Analysis.Refusal) => guardedBy(r.name) += 1)
        }
      } catch {
        case e @ (_: IllegalArgumentException | _: IllegalStateException) =>
          refused += 1
          logWarning(s"refused: $text: ${e.getMessage}")
      }
    }
    val guarded = omitted - refused - outside
    logInfo(s"the gates leave out $omitted trees to size $upTo: $guarded guarded " +
      s"(${guardedBy.toSeq.sorted.mkString(", ")}), $outside outside the contract")
    assert(refused == 0, s"$refused trees the gates leave out are refused by the emitter")
  }

  /** The (parent, child position, child type) triples under `node`, and its node types. */
  private def walk(node: VarkaVectorIR, triples: mutable.Set[String],
      types: mutable.Set[String]): Unit = {
    types += node.getClass.getSimpleName
    val children = VarkaVectorIR.childrenOf(node)
    for (i <- children.indices) {
      triples += s"${node.getClass.getSimpleName}/$i/${children(i).getClass.getSimpleName}"
      walk(children(i), triples, types)
    }
  }

  /** The size the reach test enumerates to: generation only, so never below `IfElse`'s 4. */
  private val reachSize = math.max(size, 4)

  test(s"the series reaches by size $reachSize every node type the grammar draws, counted by " +
      "position") {
    // Generation only, nothing emitted. The fuzzer's reach test counts node types; the series
    // also counts the positions it reaches each type in, and logs the triples each size adds.
    def reach(roots: Iterator[VarkaVectorIR]): (Set[String], Set[String]) = {
      val triples = mutable.Set.empty[String]
      val types = mutable.Set.empty[String]
      roots.foreach(walk(_, triples, types))
      (triples.toSet, types.toSet)
    }
    for (long <- Seq(false, true)) {
      val lane = if (long) "long" else "int"
      val series = if (long) new VarkaIrSeries.LongSeries else new VarkaIrSeries.IntSeries
      var seen = Set.empty[String]
      var types = Set.empty[String]
      for (k <- 1 to reachSize) {
        val (t, ty) = reach(series.roots(k))
        logInfo(s"$lane size $k reaches ${(t -- seen).size} new triples, ${t.size} in all")
        seen ++= t
        types ++= ty
      }
      val drawn = reach((0 until 20000).iterator.flatMap { k =>
        val rnd = VarkaIrGrammar.shapeRandom(VarkaIrGrammar.fuzzSeed, k)
        if (long) VarkaIrGrammar.drawLongShape(rnd).roots else VarkaIrGrammar.drawShape(rnd).roots
      })
      logInfo(s"$lane: the grammar's draws hold ${drawn._1.size} triples, " +
        s"${(drawn._1 -- seen).size} past size $reachSize")
      val missing = drawn._2 -- types
      assert(missing.isEmpty, s"$lane: the series to size $reachSize never builds " +
        missing.toSeq.sorted.mkString(", "))
    }
  }
}
