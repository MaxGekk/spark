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
import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CompiledVarkaProjection, FusedOutput, KernelOutput, VarkaExpressionCompiler}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaCompositionCase.Entry
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType
import org.apache.spark.sql.catalyst.util.{DateTimeConstants, DateTimeUtils}
import org.apache.spark.sql.types.{DataType, DayTimeIntervalType, TimeType}

/**
 * Random compositions of the coverage table, through the compiler to the emitter.
 *
 * `VarkaIrFuzzSuite` draws IR directly and so never sees the compiler admit anything; the
 * coverage suite compiles every documented expression alone and requires it to fuse. Between
 * the two is the path a wide query takes: many admitted entries in one projection, or many
 * admitted conjuncts in one filter, which the compiler groups, budgets in bytes, regroups and
 * partly declines. That path produced the epilogue past 64KB (`VARKA-87.md`) and the
 * `CASE WHEN` that failed to emit (`VARKA-169.md`), and nothing drew it at random.
 *
 * Each iteration composes a projection of one to three hundred entries drawn from the table's
 * projection rows, or a filter of one to sixty-four of its predicate rows, resolves them against
 * the table's own columns, and asks the compiler what the planner asks. The property is the
 * milestone's (`m6/PLAN.md` 1.3): every entry is fused or declined with a reason, the
 * compiler throws nothing, and a decline of an entry the table says fuses alone is one of the
 * two the record knows, a size decline naming the budget or the one-lane rule. Emit options
 * alternate between the default width and four lanes, the two the emitted-bytes oracle pins,
 * and the exact grouping (`VARKA-200.md`) is on or off at random, since the wide projections
 * drawn here are where it changes the partition.
 *
 * Past the columns one kernel reads (VARKA-238): a third test spreads each of 150 to 300 rows over
 * eighty renamed copies of the table's columns, so the compiler serves the projection with
 * several kernels (`VarkaEmitOptions.severalKernels`), holds the same property, and runs every
 * kernel, at either lane, against the reference evaluator (`VarkaKernelCheck`). A batch a guard
 * declines, over columns drawn without all of their domains, is drawn again nearer zero, so every
 * kernel is compared and each lane that has kernels has comparisons (VARKA-289).
 * `-Dvarka.fuzz.wideCompositions` sets its count (default 20).
 *
 * Budget: `-Dvarka.fuzz.compositions` (default 40, under a minute); `-Dvarka.fuzz.seed` (default
 * fixed, shared with the IR fuzzer so a nightly varies both with one property). A failure names
 * the seed, the iteration and the rows, and `-Dvarka.fuzz.only=<iteration>` replays one.
 */
class VarkaCoverageCompositionFuzzSuite
    extends SparkFunSuite with VarkaMatrixTests with VarkaOwnJvm {

  private val seed = sys.props.get("varka.fuzz.seed").map(_.toLong).getOrElse(20260925L)
  private val iterations = sys.props.get("varka.fuzz.compositions").map(_.toInt).getOrElse(40)
  private val only = sys.props.get("varka.fuzz.only").map(_.toInt)

  /** The coverage table's columns and rows, from the committed file the coverage suite keeps. */
  private lazy val table =
    VarkaCoverageRows.read(getWorkspaceFilePath("sql", "varka", "coverage.json"))
  private def columns = table.columns
  private def projections = table.projections
  private def predicates = table.predicates

  private def resolve(sql: String): Expression = VarkaCoverageRows.resolve(sql, columns)

  /** One to `max`, log-uniform, so most compositions are small and some are very wide. */
  private def width(rnd: Random, max: Int): Int =
    math.max(1, math.exp(rnd.nextDouble() * math.log(max)).toInt)

  /**
   * The emit options of an iteration. The exact grouping is drawn from a stream of its own, so
   * adding it left every composition the main stream draws, and the seeds that found past bugs,
   * as they were.
   */
  private def options(rnd: Random, seed: Long, iteration: Int): VarkaEmitOptions = {
    val lanes = if (rnd.nextBoolean()) VarkaMatrix.base
      else VarkaMatrix.base.withLanesOverride(4)
    lanes.withExactGrouping(new Random(~(seed * 1000003L + iteration)).nextBoolean())
  }

  /**
   * The two reasons a composition may decline an entry that fuses alone: a size decline, whose
   * reason names a budget, and the one-lane rule - a kernel holds one lane, so a projection
   * that mixes the int and the long lane fuses the first lane it meets and leaves the other,
   * which `VARKA-29.md` pins and VARKA-28's width conversion is to lift. Any other reason on
   * an admitted row is a finding.
   */
  private def isCompositionDecline(reason: String): Boolean =
    reason.contains("budget") || reason.contains("one kernel holds one lane")

  /**
   * The finding of a failed check on a line of its own, and the case after it: the signature of a
   * failure (VARKA-294) is that first line with its numbers taken out, so the case, whose size and
   * options change as it shrinks, must not be on it.
   */
  private def finding(what: String, c: VarkaCompositionCase): String =
    s"$what\n  on ${c.label}"

  private def whereOf(seed: Long, iteration: Int, c: VarkaCompositionCase, noun: String,
      picked: Seq[String]): String =
    s"seed $seed iteration $iteration, ${picked.size} $noun, options " +
      s"${c.options.canonical}:\n  ${picked.mkString("\n  ")}"

  /** The case drawn for `iteration`, with its label naming the seed, the iteration and the rows. */
  private def drawProjection(seed: Long, iteration: Int): VarkaCompositionCase = {
    val rnd = new Random(seed * 1000003L + iteration)
    val picked = Seq.fill(width(rnd, 300))(projections(rnd.nextInt(projections.size)))
    val entries = picked.map(row => Entry(row.executable, resolve(row.executable))).toVector
    val opts = options(rnd, seed, iteration)
    val c = VarkaCompositionCase("projection", entries, opts, 0L, "")
    c.copy(label = whereOf(seed, iteration, c, "entries", picked.map(_.executable)))
  }

  private def checkProjection(c: VarkaCompositionCase): Unit = {
    val list: Seq[NamedExpression] = c.entries.zipWithIndex.map { case (e, i) =>
      Alias(e.expr, s"c$i")()
    }
    val (fused, declined) = try {
      // A further kernel's entry is fused too (VARKA-190's `severalKernels`, on by default).
      val fused = VarkaExpressionCompiler.compilePartial(list, columns, c.options)
        .map(_.specs.zipWithIndex.collect {
          case (_: FusedOutput, i) => i
          case (_: KernelOutput, i) => i
        }.toSet)
        .getOrElse(Set.empty[Int])
      (fused, VarkaExpressionCompiler.declines(list, columns, c.options))
    } catch {
      case e: Exception => fail(finding(s"the compiler threw ${threw(e)}", c), e)
    }
    assert(fused.size + declined.size == list.size && (fused & declined.keySet).isEmpty,
      finding("entries neither fused nor declined, or both", c))
    declined.foreach { case (i, d) =>
      assert(d.reason.nonEmpty, finding(s"entry $i declined without a reason", c))
      assert(isCompositionDecline(d.reason),
        finding(s"entry $i, which fuses alone, declined for '${d.reason}'", c))
    }
  }

  /** The exception's class and first line, for the first line of a finding. */
  private def threw(e: Throwable): String =
    s"${e.getClass.getSimpleName}: ${Option(e.getMessage).flatMap(_.linesIterator.nextOption())
      .getOrElse("")}"

  private def drawPredicate(seed: Long, iteration: Int): VarkaCompositionCase = {
    val rnd = new Random(seed * 1000003L + 500000L + iteration)
    val picked = Seq.fill(width(rnd, 64))(predicates(rnd.nextInt(predicates.size)))
    val entries = picked.map(row => Entry(row.executable, resolve(row.executable))).toVector
    val opts = options(rnd, seed, iteration)
    val c = VarkaCompositionCase("predicate", entries, opts, 0L, "")
    c.copy(label = whereOf(seed, iteration, c, "conjuncts", picked.map(_.executable)))
  }

  private def checkPredicate(c: VarkaCompositionCase): Unit = {
    val condition = c.entries.map(_.expr).reduceLeft(And)
    val specs = try {
      VarkaExpressionCompiler.explainPredicate(condition, columns, c.options)
    } catch {
      case e: Exception => fail(finding(s"the compiler threw ${threw(e)}", c), e)
    }
    // A row of the table may itself be a conjunction, which the compiler splits, so the specs
    // are at least as many as the rows picked.
    assert(specs.size >= c.entries.size, finding(s"${specs.size} conjunct specs", c))
    specs.zipWithIndex.foreach { case (spec, i) =>
      assert(spec.fused || spec.decline.exists(_.reason.nonEmpty),
        finding(s"conjunct $i neither fused nor declined with a reason", c))
      spec.decline.filterNot(_ => spec.fused).foreach { d =>
        assert(isCompositionDecline(d.reason),
          finding(s"conjunct $i, which fuses alone, declined for '${d.reason}'", c))
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Past the columns one kernel reads (VARKA-238).
  // ---------------------------------------------------------------------------------------------

  /**
   * The table's columns eighty times over, renamed. Most rows read the one date column, so a
   * projection reaches past the 64 columns a kernel reads only when that column alone has more
   * copies than a kernel holds.
   */
  private lazy val copies: Seq[Seq[Attribute]] = (0 until 80).map { k =>
    columns.map(a => AttributeReference(s"${a.name}_$k", a.dataType, a.nullable)())
  }

  /** `e`, resolved against the table's columns, moved onto copy `k`. */
  private def onCopy(e: Expression, k: Int): Expression = {
    val byId = columns.map(_.exprId).zip(copies(k)).toMap
    e.transform { case a: AttributeReference if byId.contains(a.exprId) => byId(a.exprId) }
  }

  private var kernelCounter = 0

  private val wideIterations =
    sys.props.get("varka.fuzz.wideCompositions").map(_.toInt).getOrElse(20)

  /**
   * The value of a long-lane input of `dataType` at `scale`, the attempt's step towards zero: a
   * `TIME` inside the day in nanoseconds, the domain every long-lane guard assumes; a day-time
   * interval and a `BIGINT` within 2^45 and 2^40 of zero, then nearer at each step.
   */
  private def drawLong(rnd: Random, dataType: DataType, scale: Int): Long = {
    def within(bound: Long): Long = Math.floorMod(rnd.nextLong(), 2 * bound + 1) - bound
    (dataType, scale) match {
      case (_, 2) => 1 + rnd.nextInt(3)
      case (_: TimeType, 0) => Math.floorMod(rnd.nextLong(), DateTimeConstants.NANOS_PER_DAY)
      case (_: TimeType, _) => Math.floorMod(rnd.nextLong(), 1000000L)
      case (_: DayTimeIntervalType, 0) => within(1L << 45)
      case (_, 0) => within(1L << 40)
      case _ => within(30000)
    }
  }

  /** Kernels compared row by row and batches drawn again after a decline, per lane. */
  private val comparedByLane = scala.collection.mutable.Map.empty[LaneType, Int]
  private val kernelsByLane = scala.collection.mutable.Map.empty[LaneType, Int]
  private var redrawn = 0

  /**
   * Runs one compiled kernel, at its lane, against the reference evaluator over `inputs`, the
   * attributes its input ordinals index. Each input is drawn from its own domain: a derived input
   * from its kind's codes, a bounded input across its bound, an int-lane input within thirty
   * thousand either side of zero, a long-lane one by its type (`drawLong`). A batch a guard
   * declines is drawn again twice, nearer zero each time and a bounded input clamped into its
   * bound, the last within one to three, which no guard declines; each attempt keeps the length
   * and null patterns. The test then asserts that
   * every kernel was compared.
   */
  private def checkKernel(plan: CompiledVarkaProjection, inputs: Seq[Attribute],
      opts: VarkaEmitOptions, rnd: Random): Unit = {
    val numInputs = plan.inputOrdinals.size
    kernelCounter += 1
    kernelsByLane(plan.lane) = kernelsByLane.getOrElse(plan.lane, 0) + 1
    val className = s"org.apache.spark.sql.varka.execution.VarkaCompositionWide$kernelCounter"
    val bytes = VarkaLoopEmitter.emit(className, plan.outputs.asJava, numInputs,
      plan.numLiterals, null, null, opts)
    val length = Seq(1, 7, 64, 100, 257, 1000)(rnd.nextInt(6))
    val patterns: Seq[Int => Boolean] = Seq.fill(numInputs) {
      rnd.nextInt(3) match {
        case 0 => (_: Int) => false
        case 1 => (i: Int) => i % 5 == 0
        case _ =>
          val bits = Array.fill(length)(rnd.nextInt(3) == 0)
          (i: Int) => bits(i)
      }
    }
    val forceMasked = length > 1 && rnd.nextBoolean()
    val context = "kernel"
    def intValue(i: Int, scale: Int): Int = plan.derivedAt(i).map(_.kind) match {
      case Some(VarkaDerivedKind.TRUNC_LEVEL) =>
        DateTimeUtils.TRUNC_TO_WEEK +
          rnd.nextInt(DateTimeUtils.TRUNC_TO_YEAR - DateTimeUtils.TRUNC_TO_WEEK + 1)
      case Some(_) => rnd.nextInt(7)
      case None =>
        def unbounded: Int = scale match {
          case 0 => rnd.nextInt(60001) - 30000
          case 1 => rnd.nextInt(201) - 100
          case _ => 1 + rnd.nextInt(3)
        }
        plan.inputBounds.find(_.inputIndex == i) match {
          case Some(b) if scale == 0 =>
            (b.lo + (rnd.nextLong() & Long.MaxValue) % (b.hi.toLong - b.lo + 1)).toInt
          // Nearer zero as the unbounded draw, inside the bound: a value across the whole of
          // it, added to a date, passes the date guard at every attempt.
          case Some(b) => unbounded.max(b.lo).min(b.hi)
          case None => unbounded
        }
    }
    def attempt(scale: Int): Boolean = if (plan.lane == LaneType.LONG) {
      val data = Array.tabulate(numInputs) { i =>
        val dataType = inputs(plan.inputOrdinals(i)).dataType
        Array.fill(length)(drawLong(rnd, dataType, scale))
      }
      VarkaKernelCheck.runAndCompareLong(context, className, bytes, plan.outputs, numInputs,
        plan.longLiterals.toArray,
        VarkaKernelCheck.LongBatch(length, patterns, data, forceMasked), declineAllowed = true)
    } else {
      val data = Array.tabulate(numInputs)(i => Array.fill(length)(intValue(i, scale)))
      VarkaKernelCheck.runAndCompare(context, className, bytes, plan.outputs, numInputs,
        plan.literals.toArray, VarkaKernelCheck.Batch(length, patterns, data, forceMasked),
        declineAllowed = true)
    }
    val compared = (0 until 3).exists { scale =>
      if (scale > 0) redrawn += 1
      attempt(scale)
    }
    if (compared) comparedByLane(plan.lane) = comparedByLane.getOrElse(plan.lane, 0) + 1
  }

  private def drawWide(seed: Long, iteration: Int): VarkaCompositionCase = {
    val rnd = new Random(seed * 1000003L + 900000L + iteration)
    val picked = Seq.fill(150 + rnd.nextInt(151))(projections(rnd.nextInt(projections.size)))
    val entries = picked.map { row =>
      Entry(row.executable, onCopy(resolve(row.executable), rnd.nextInt(copies.size)))
    }.toVector
    val opts = options(rnd, seed, iteration)
    // The batches the kernels are checked on come from a stream of their own, so a shrunk case
    // is checked on the batches the original was.
    val c = VarkaCompositionCase("wide", entries, opts, rnd.nextLong(), "")
    c.copy(label = whereOf(seed, iteration, c, "entries", picked.map(_.executable))
      .replace(", options", " (wide), options"))
  }

  /** Whether the projection reached several kernels, and how many columns its first read. */
  private def checkWide(c: VarkaCompositionCase): (Boolean, Int) = {
    val list: Seq[NamedExpression] = c.entries.zipWithIndex.map { case (e, i) =>
      Alias(e.expr, s"c$i")()
    }
    val wide = copies.flatten
    val rnd = new Random(c.checkSeed)
    val partial = try {
      VarkaExpressionCompiler.compilePartial(list, wide, c.options)
    } catch {
      case e: Exception => fail(finding(s"the compiler threw ${threw(e)}", c), e)
    }
    val declined = VarkaExpressionCompiler.declines(list, wide, c.options)
    val fused = partial.toSeq.flatMap(_.specs.zipWithIndex.collect {
      case (_: FusedOutput, i) => i
      case (_: KernelOutput, i) => i
    }).toSet
    assert(fused.size + declined.size == list.size && (fused & declined.keySet).isEmpty,
      finding("entries neither fused nor declined, or both", c))
    declined.foreach { case (i, d) =>
      assert(isCompositionDecline(d.reason),
        finding(s"entry $i, which fuses alone, declined for '${d.reason}'", c))
    }
    partial.foreach(p => p.kernels.foreach(checkKernel(_, wide, c.options, rnd)))
    (partial.exists(_.kernels.size > 1), partial.map(_.fused.inputOrdinals.size).getOrElse(0))
  }

  // ---------------------------------------------------------------------------------------------
  // Failures, shrunk (VARKA-294).
  // ---------------------------------------------------------------------------------------------

  /** Off with `-Dvarka.fuzz.shrink=false`, which leaves a failure as the generator drew it. */
  private val shrinkFailures = !sys.props.get("varka.fuzz.shrink").contains("false")

  /** What `check(c)` threw as a signature; None when the case passes. */
  private def outcomeOf(check: VarkaCompositionCase => Any)(
      c: VarkaCompositionCase): Option[VarkaFailureSignature] =
    try {
      check(c)
      None
    } catch {
      case e: VirtualMachineError => throw e
      case e: InterruptedException => throw e
      case t: Throwable => Some(VarkaFailureSignature.of(t, "kernel"))
    }

  /**
   * Runs the case. A failure - any `Throwable`, a finding or a mismatch out of a kernel - is
   * shrunk and rethrown as a test failure whose message has the original and the smaller case,
   * unless its signature is on the known list, which is reported and returns None.
   */
  private def runChecked[T](c: VarkaCompositionCase,
      known: Seq[VarkaKnownFailures.Entry] = VarkaKnownFailures.entries)(
      check: VarkaCompositionCase => T): Option[T] = {
    try Some(check(c)) catch {
      case e: VirtualMachineError => throw e
      case e: InterruptedException => throw e
      case t: Throwable if shrinkFailures =>
        val signature = VarkaFailureSignature.of(t, "kernel")
        if (VarkaKnownFailures.isKnown(known, signature)) {
          logWarning(s"known fuzz failure, not shrunk: $signature")
          return None
        }
        val shrunk = VarkaCompositionShrinker.shrink(c, signature, outcomeOf(check))
        val early = (if (shrunk.stoppedEarly) ", budget reached" else "") +
          (if (shrunk.stable) "" else ", UNSTABLE: it did not fail the same way three times")
        fail(Option(t.getMessage).getOrElse(t.getClass.getName) +
          s"\n  shrunk to ${VarkaCompositionCase.describe(shrunk.small)}" +
          s"\n  (${shrunk.runs} runs, ${shrunk.millis} ms$early); signature: $signature", t)
    }
  }

  /** `c` with the planted-bug option `name` on. */
  private def planted(c: VarkaCompositionCase, name: String): VarkaCompositionCase =
    c.copy(options = VarkaEmitOption.named(name) match {
      case flag: VarkaEmitOption.Flag => flag.`with`(c.options, true)
      case count: VarkaEmitOption.Count => count.`with`(c.options, 1)
      case other => fail(s"$name is not a planted-bug option: $other")
    })

  test("random projections over more columns than a kernel reads are fused or declined, and " +
      "their kernels answer as the reference evaluator does") {
    var severalKernels = 0
    var widest = 0
    comparedByLane.clear()
    kernelsByLane.clear()
    redrawn = 0
    for (iteration <- 0 until wideIterations) {
      runChecked(drawWide(seed, iteration))(checkWide).foreach { case (several, first) =>
        widest = math.max(widest, first)
        if (several) severalKernels += 1
      }
    }
    def perLane(m: scala.collection.Map[LaneType, Int]): String =
      LaneType.values.toSeq.map(l => s"${m.getOrElse(l, 0)} $l").mkString(", ")
    info(s"$severalKernels of $wideIterations projections served by several kernels, " +
      s"kernels compared row by row: ${perLane(comparedByLane)} of ${perLane(kernelsByLane)}, " +
      s"$redrawn batches drawn again after a decline, the widest first kernel reading $widest " +
      "columns")
    // Every lane that has kernels has comparisons, since a declined batch is drawn again until
    // no guard declines it; the draw decides only how many kernels each lane gets.
    assert(severalKernels > 0, s"no projection reached several kernels; the widest first " +
      s"kernel read $widest columns")
    kernelsByLane.keys.foreach { lane =>
      assert(comparedByLane.getOrElse(lane, 0) == kernelsByLane(lane),
        s"${comparedByLane.getOrElse(lane, 0)} of ${kernelsByLane(lane)} $lane kernels were " +
          "compared row by row")
    }
  }

  test("random projections of coverage rows are fused or declined in bytes, never thrown") {
    only match {
      case Some(i) => runChecked(drawProjection(seed, i))(checkProjection)
      case None => (0 until iterations).foreach(i =>
        runChecked(drawProjection(seed, i))(checkProjection))
    }
  }

  test("random conjunctions of coverage predicates are fused or declined in bytes, never thrown") {
    only match {
      case Some(i) => runChecked(drawPredicate(seed, i))(checkPredicate)
      case None => (0 until iterations).foreach(i =>
        runChecked(drawPredicate(seed, i))(checkPredicate))
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The shrinker on this fuzzer's cases (VARKA-294).
  // ---------------------------------------------------------------------------------------------

  /** A planted-bug wide case's failure and its shrink: the first of ten draws that fails. */
  private def shrunkPlanted(name: String): (VarkaCompositionCase, VarkaFailureSignature,
      VarkaCompositionShrinker.Shrunk) = {
    val found = (0 until 10).iterator.map(k => planted(drawWide(seed, k), name))
      .map(c => (c, outcomeOf(checkWide)(c))).collectFirst { case (c, Some(sig)) => (c, sig) }
    val (c, sig) = found.getOrElse(fail(s"$name: no failing wide case in 10 draws"))
    (c, sig, VarkaCompositionShrinker.shrink(c, sig, outcomeOf(checkWide)))
  }

  test("a planted bug in a wide projection shrinks from hundreds of entries to a few") {
    val (c, sig, shrunk) = shrunkPlanted("misdescribeAdd")
    val small = shrunk.small
    info(s"${c.entries.size} entries shrunk to ${small.entries.size} in ${shrunk.runs} runs, " +
      s"${shrunk.millis} ms: ${VarkaCompositionCase.describe(small)}")
    assert(c.entries.size >= 150)
    assert(small.entries.size <= 3, VarkaCompositionCase.describe(small))
    assert(small.entries.map(e => VarkaCompositionShrinker.size(e.expr)).sum <=
      small.entries.map(e => VarkaCompositionShrinker.size(e.expr)).size * 8,
      VarkaCompositionCase.describe(small))
    assert(VarkaFuzzCase.optionDelta(small.options).contains("misdescribeAdd=true"))
    assert(VarkaFuzzCase.optionDelta(small.options).size <= 3)
    assert(sig.kind == "generated class: NoSuchMethodError", sig)
    assert(shrunk.runs <= 300 && shrunk.millis < 30000L && !shrunk.stoppedEarly, shrunk.toString)
    assert(shrunk.stable)
  }

  test("ddmin over the entries finds the pair a failure needs") {
    val c = drawProjection(seed, 3)
    val sixty = (0 until 60).map(i => c.entries(i % c.entries.size).copy(sql = s"row$i")).toVector
    val base = c.copy(entries = sixty)
    val sig = VarkaFailureSignature("test", "needs a pair")
    def outcome(x: VarkaCompositionCase): Option[VarkaFailureSignature] =
      if (Seq("row7", "row41").forall(r => x.entries.exists(_.sql == r))) Some(sig) else None
    val shrunk = VarkaCompositionShrinker.shrink(base, sig, outcome)
    assert(shrunk.small.entries.map(_.sql) == Vector("row7", "row41"), shrunk.small.entries)
    assert(shrunk.stable && !shrunk.stoppedEarly)
  }

  test("an entry's expression shrinks to the node a failure needs") {
    val nested = Entry("date_add(last_day(d), 3)", resolve("date_add(last_day(d), 3)"))
    val base = VarkaCompositionCase("projection", Vector(nested), VarkaMatrix.base, 0L, "")
    val sig = VarkaFailureSignature("test", "needs last_day")
    def outcome(x: VarkaCompositionCase): Option[VarkaFailureSignature] =
      if (x.entries.exists(_.expr.sql.contains("last_day"))) Some(sig) else None
    val shrunk = VarkaCompositionShrinker.shrink(base, sig, outcome)
    assert(VarkaCompositionShrinker.size(nested.expr) > 3)
    assert(shrunk.small.entries.head.expr.sql.startsWith("last_day("), shrunk.small.entries)
    assert(VarkaCompositionShrinker.size(shrunk.small.entries.head.expr) == 2)
  }

  test("a failure fails its test with the smaller case in the message") {
    val (c, _, _) = shrunkPlanted("misdescribeAdd")
    val e = intercept[org.scalatest.exceptions.TestFailedException](runChecked(c)(checkWide))
    assert(e.getMessage.contains("shrunk to kind=wide"), e.getMessage)
    assert(e.getMessage.contains("signature: generated class: NoSuchMethodError"), e.getMessage)
  }

  test("a failure with a listed signature is reported as known, not failed") {
    val (c, sig, _) = shrunkPlanted("misdescribeAdd")
    val known = Seq(VarkaKnownFailures.Entry("wide", seed, 0, sig, "test"))
    assert(runChecked(c, known)(checkWide).isEmpty)
  }

  test("every known composition failure still reproduces") {
    val replay = (e: VarkaKnownFailures.Entry) => e.lane match {
      case "projection" => outcomeOf(checkProjection)(drawProjection(e.seed, e.iteration))
      case "predicate" => outcomeOf(checkPredicate)(drawPredicate(e.seed, e.iteration))
      case "wide" => outcomeOf(checkWide)(drawWide(e.seed, e.iteration))
      // The IR fuzzer's entries are replayed by its own suite.
      case _ => Some(e.signature)
    }
    val stale = VarkaKnownFailures.stale(VarkaKnownFailures.entries, replay)
    assert(stale.isEmpty, "these known fuzz failures no longer reproduce; delete them from " +
      s"${VarkaKnownFailures.PATH}: ${stale.mkString(", ")}")
  }
}
