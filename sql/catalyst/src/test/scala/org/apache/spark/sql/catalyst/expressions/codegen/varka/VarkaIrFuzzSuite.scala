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

import scala.jdk.CollectionConverters._
import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaIrGrammar._
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR._
import org.apache.spark.sql.catalyst.util.DateTimeUtils

/**
 * Random IR trees against the reference evaluator.
 *
 * The emitter suite's matrices are exhaustive over the shapes someone thought to write. This
 * suite writes the others: for each iteration a fresh `Random` seeded from the run seed and the
 * iteration number builds one to three roots out of every supported node type, over one to
 * three int32 columns and up to two literal slots, picks a length, a null pattern per column
 * and a random `VarkaEmitOptions` variant, emits and loads the kernel, runs it once and checks
 * every row and validity bit against [[VarkaReferenceEvaluator]]. A failure names the seed and
 * the iteration, the roots in canonical form, the options, the length and the patterns, and
 * replays with `-Dvarka.fuzz.seed=<seed> -Dvarka.fuzz.only=<iteration>`.
 *
 * What it is for: the class of bug where a slot or a local means two things under two option
 * settings, or a lane-group tail or a validity word is right for every curated shape and wrong
 * for one nobody curated. Random option combinations are the point - every boolean `with*` on
 * the options record is toggled at random (the fault injector excepted), the mod-7 lowering is
 * drawn from all three, and `groupBudget` is sometimes narrowed - so a new option is fuzzed the
 * day it lands without anyone touching this file.
 *
 * The shapes respect the emitter's structural rules, which are the compiler's: a day offset is a
 * literal slot or a column, `next_day`'s weekday and `add_months`' month count are a literal
 * slot or a column, `IsNotNull` is over a column, a selection kernel has one condition root.
 *
 * Ranges: calendar nodes are defined over `VarkaChrono`'s narrowed day range, so every tree
 * carries a bound on the magnitude of its value and a calendar node is only put over a subtree
 * whose bound fits inside that range with slack. Columns hold days within plus or minus 2.5
 * million (about six thousand years either side of 1970) and literals within plus or minus
 * 4000, which keeps sums of two columns and chains of offsets inside the range too.
 *
 * The long lane has a corpus of its own, drawn from `VarkaIrGrammar.LongShapes` over 64-bit
 * columns and literals and run through the kernel's eight-argument entry point against
 * `evalLong`. It is a second sequence with a second seed rather than long shapes mixed into the
 * first, because the first is also the emitted-bytes oracle's committed corpus. Its value roots
 * are narrowed to an int column where their bound fits (`LongShapes.root`, VARKA-235), and a
 * narrowing root's output is read at the store's four bytes a row. The same
 * option draws apply, which is what puts `useAVX` under the constant division and so fuzzes
 * both of its lowerings on one machine.
 *
 * Past the ceilings (VARKA-238): the byte budget is drawn small as well as off and 8000, so the
 * regroup and the declines happen at the widths drawn here, and a third test composes
 * `drawWideShape`'s roots into kernels of at least 250 outputs, past the driver's ceiling of about
 * 180 groups, under option variants that reach every size mechanism - the regroup, the call-site
 * splits and their rollback, the split driver's stages, both grouping switches dropped, and the
 * decline. Every composition is checked row by row, under the defaults where its variant
 * declines. A mechanism the drawn compositions miss is drawn for, up to six more cycles of the
 * variants, and a run that still reaches no instance of it fails (`VarkaEmitTrace`, VARKA-289).
 *
 * Budget: `-Dvarka.fuzz.iterations` (default 300, a few seconds); `-Dvarka.fuzz.wide` (default
 * 10 compositions, a few seconds); `-Dvarka.fuzz.seed` (default fixed, so the committed run is
 * reproducible and a nightly can vary it). The iterations and the seed apply to both lanes.
 */
class VarkaIrFuzzSuite extends SparkFunSuite with VarkaMatrixTests with VarkaOwnJvm {

  private val seed = sys.props.get("varka.fuzz.seed").map(_.toLong).getOrElse(fuzzSeed)
  private val longSeed = sys.props.get("varka.fuzz.seed").map(_.toLong).getOrElse(longFuzzSeed)
  private val iterations = sys.props.get("varka.fuzz.iterations").map(_.toInt).getOrElse(300)
  private val only = sys.props.get("varka.fuzz.only").map(_.toInt)
  private val classCounter = new AtomicInteger(0)
  private val skippedPastTheCap = new AtomicInteger(0)
  // What the random shapes' emissions did about size, reported after each run (VARKA-238).
  private val randomTrace = new VarkaEmitTrace
  private val lengths = Seq(1, 3, 7, 15, 16, 17, 33, 64, 65, 100, 257, 1000)

  /** The options `randomOptions` draws, in the order it draws them. */
  private val fuzzedOptions = VarkaEmitOption.TABLE.asScala.toSeq
    .filter(_.reason != VarkaEmitOption.Reason.FAULT_INJECTOR)
    .sortBy(o => "with" + o.name.head.toUpper + o.name.tail)

  /**
   * A random variant of the options record, drawn from the options table: every option but the
   * fault injectors, in the order of their `with*` setters' names, a boolean as a coin, an enum
   * as one of its constants, and an int one time in five from the values its table entry lists -
   * a lanes override from the powers of two, the AVX level from the levels either side of 3
   * where the 64-bit division changes lowering, the method byte budget from off, the HotSpot
   * limit and the smaller budgets that bring the size machinery down to the widths drawn here
   * (VARKA-238), and the other budgets from small values. The order is fixed so that a seed
   * recorded in a failure keeps drawing the same options.
   */
  private def randomOptions(rnd: Random): VarkaEmitOptions = {
    var opts = VarkaMatrix.base
    for (option <- fuzzedOptions) {
      option match {
        case flag: VarkaEmitOption.Flag => opts = flag.`with`(opts, rnd.nextBoolean())
        case choice: VarkaEmitOption.Choice[_] =>
          opts = choice.withIndex(opts, rnd.nextInt(choice.constants.size))
        case count: VarkaEmitOption.Count =>
          if (rnd.nextInt(5) == 0) {
            opts = count.`with`(opts, count.fuzzDraws.get(rnd.nextInt(count.fuzzDraws.size)))
          }
      }
    }
    opts
  }

  private val patternNames = Seq("null-free", "every-5th", "alternating", "all-null", "random")

  private def pattern(rnd: Random, which: Int, length: Int): Int => Boolean = which match {
    case 0 => _ => false
    case 1 => i => i % 5 == 0
    case 2 => i => i % 2 == 1
    case 3 => _ => true
    case _ =>
      val bits = Array.fill(length)(rnd.nextInt(3) == 0)
      i => bits(i)
  }

  /**
   * The shape's class, or None for a shape no form of the emitter holds. Under the byte budget
   * a shape whose single output is over the budget declines with a reason, by design (VARKA-87);
   * the heaviest trees are the ones most worth checking, so such a shape is run in the form
   * without the budget rather than skipped. When that form declines too, or the budget was off
   * and it declined at once, the reason is the class-file cap on a method's code, the one limit
   * the legacy form has: the JVM holds no method the emitter could make of the shape, the
   * decline is the emitter's answer (VARKA-219), and the shape is counted and skipped. Anything
   * else the emitter throws is a failure - it rejected a shape the grammar builds.
   */
  private def emitOrSkip(context: String, options: VarkaEmitOptions)(
      emitWith: VarkaEmitOptions => Array[Byte]): Option[Array[Byte]] = {
    def pastTheCap(d: VarkaEmitDeclined): Option[Array[Byte]] = {
      assert(d.getMessage.contains("over the class-file cap of"),
        s"$context: declined with no budget to decline on: ${d.getMessage}")
      skippedPastTheCap.incrementAndGet()
      None
    }
    try {
      Some(emitWith(options))
    } catch {
      case d: VarkaEmitDeclined if options.methodByteBudget() > 0 =>
        assert(d.getMessage.contains("bytes"), s"$context: a size decline without a size")
        try {
          Some(emitWith(options.withMethodByteBudget(0)))
        } catch {
          case again: VarkaEmitDeclined => pastTheCap(again)
          case e: IllegalArgumentException =>
            fail(s"$context: the emitter rejected the shape without the budget: " +
              e.getMessage, e)
          case e: IllegalStateException =>
            fail(s"$context: the emitter failed its own check without the budget: " +
              e.getMessage, e)
        }
      case d: VarkaEmitDeclined => pastTheCap(d)
      case e: IllegalArgumentException =>
        fail(s"$context: the emitter rejected the shape: ${e.getMessage}", e)
      // One of the emitter's own invariants, such as the word check: named with the shape, so
      // a failure found at a high iteration count says which iteration to replay.
      case e: IllegalStateException =>
        fail(s"$context: the emitter failed its own check: ${e.getMessage}", e)
    }
  }

  private def drawInt(iteration: Int, drawSeed: Long = seed): VarkaFuzzCase = {
    val rnd = shapeRandom(drawSeed, iteration)
    // The shape itself comes from the shared draw, so this suite and the emitted-bytes oracle
    // run over one corpus; `rnd` is left where the lane values and null patterns below pick up.
    val Drawn(roots, numInputs, numLiterals, smallOrdinal, levelOrdinal) = drawShape(rnd)
    val lits = Array.fill(numLiterals)(rnd.nextInt(2 * literalBound + 1) - literalBound)
    val length = lengths(rnd.nextInt(lengths.length))
    val patternIds = Seq.fill(numInputs)(rnd.nextInt(patternNames.length))
    val patterns = patternIds.map(pattern(rnd, _, length))
    // Forcing the masked path reports one null over a full bitmap, which the dispatcher reads
    // as "has nulls". Never at length 1: a null count equal to the length is the contract's
    // all-null column, and the fuzzer's first run found exactly that contradiction (44 cases,
    // every one at length 1) before it found anything about the kernel.
    val forceMasked = length > 1 && rnd.nextInt(4) == 0
    val options = randomOptions(rnd)
    def draw(bound: Long): Int =
      (rnd.nextLong() % (2 * bound + 1) - bound).toInt.max(-bound.toInt).min(bound.toInt)
    val data = Array.tabulate(numInputs, length) { (c, _) =>
      if (c == levelOrdinal) {
        // Exactly the codes TruncLevelLeaf produces; a value outside them is a lane the
        // kernel's contract does not define, so the fuzzer must not invent one.
        DateTimeUtils.TRUNC_TO_WEEK + rnd.nextInt(
          DateTimeUtils.TRUNC_TO_YEAR - DateTimeUtils.TRUNC_TO_WEEK + 1)
      } else {
        draw(if (c == smallOrdinal) VarkaChrono.MONTH_ARITH_MAX_MONTHS.toLong else columnBound)
      }
    }

    val context = s"seed=$drawSeed iteration=$iteration " +
      s"roots=${roots.map(r => VarkaVectorIR.canonical(r)).mkString("[", ", ", "]")} " +
      s"options=${if (options.isDefault) "(defaults)" else options.canonical()} " +
      s"length=$length patterns=${patternIds.map(patternNames).mkString(",")} " +
      s"literals=${lits.mkString(",")} forceMasked=$forceMasked"

    VarkaFuzzCase(LaneType.INT, roots, numInputs, lits.map(_.toLong), length,
      Array.tabulate(numInputs, length)((c, i) => patterns(c)(i)), data.map(_.map(_.toLong)),
      forceMasked, options, smallOrdinal, levelOrdinal, context)
  }

  /**
   * `runOne` at the long lane: the same draw of length, null patterns, masking and options
   * over a long-lane shape, 64-bit buffers, the eight-argument `run`, and `evalLong` as the
   * oracle. Null lanes are poisoned with the lane's own extremes, for the reason `runOne`
   * gives: every drawn value is inside the guards and the checked modes by construction, so
   * only a poisoned null lane can reach a condemning comparison, and a kernel that reads one
   * has to be caught reading it.
   */
  private def drawLong(iteration: Int, drawSeed: Long = longSeed): VarkaFuzzCase = {
    val rnd = shapeRandom(drawSeed, iteration)
    val DrawnLong(roots, numInputs, numLiterals) = drawLongShape(rnd)
    // A floor modulus, so a negative draw lands inside the bound too: a signed `%` would put
    // it as far as three bounds below zero, outside every guard the grammar drew.
    def draw(bound: Long): Long = Math.floorMod(rnd.nextLong(), 2 * bound + 1) - bound
    val lits = Array.fill(numLiterals)(draw(longLiteralBound))
    val length = lengths(rnd.nextInt(lengths.length))
    val patternIds = Seq.fill(numInputs)(rnd.nextInt(patternNames.length))
    val patterns = patternIds.map(pattern(rnd, _, length))
    val forceMasked = length > 1 && rnd.nextInt(4) == 0
    val options = randomOptions(rnd)
    val data = Array.tabulate(numInputs, length)((_, _) => draw(longColumnBound))

    val context = s"lane=long seed=$drawSeed iteration=$iteration " +
      s"roots=${roots.map(r => VarkaVectorIR.canonical(r)).mkString("[", ", ", "]")} " +
      s"options=${if (options.isDefault) "(defaults)" else options.canonical()} " +
      s"length=$length patterns=${patternIds.map(patternNames).mkString(",")} " +
      s"literals=${lits.mkString(",")} forceMasked=$forceMasked"

    VarkaFuzzCase(LaneType.LONG, roots, numInputs, lits, length,
      Array.tabulate(numInputs, length)((c, i) => patterns(c)(i)), data, forceMasked, options,
      -1, -1, context)
  }

  /**
   * Emits the case's kernel and compares it with the reference evaluator, row by row. Throws
   * what the check throws; returns quietly for a shape past the class-file cap, which
   * `emitOrSkip` counts and skips.
   */
  private def run(c: VarkaFuzzCase): Unit = {
    val long = c.lane == LaneType.LONG
    val className = "org.apache.spark.sql.varka.execution.VarkaFusedFuzz" +
      s"${if (long) "Long" else ""}${classCounter.addAndGet(1)}"
    def emitWith(o: VarkaEmitOptions): Array[Byte] = VarkaLoopEmitter.emitTraced(
      className, c.roots.asJava, c.numInputs, c.lits.length, o, randomTrace)
    val bytes = emitOrSkip(c.label, c.options)(emitWith) match {
      case Some(b) => b
      case None => return
    }
    val patterns = (0 until c.numInputs).map(col => (i: Int) => c.nulls(col)(i))
    if (long) {
      VarkaKernelCheck.runAndCompareLong(c.label, className, bytes, c.roots, c.numInputs, c.lits,
        VarkaKernelCheck.LongBatch(c.length, patterns, c.data, c.forceMasked))
    } else {
      VarkaKernelCheck.runAndCompare(c.label, className, bytes, c.roots, c.numInputs,
        c.lits.map(_.toInt),
        VarkaKernelCheck.Batch(c.length, patterns, c.data.map(_.map(_.toInt)), c.forceMasked))
    }
  }

  /** What `run(c)` threw as a signature; None when the case passes or is skipped. */
  private def outcomeOf(c: VarkaFuzzCase): Option[VarkaFailureSignature] =
    try {
      run(c)
      None
    } catch {
      case e: VirtualMachineError => throw e
      case e: InterruptedException => throw e
      case t: Throwable => Some(VarkaFailureSignature.of(t, c.label))
    }

  /** Off with `-Dvarka.fuzz.shrink=false`, which leaves a failure as the generator drew it. */
  private val shrinkFailures = !sys.props.get("varka.fuzz.shrink").contains("false")

  /**
   * Runs the case. A failure - any `Throwable`, a mismatch or an error out of the generated
   * class - is shrunk (VARKA-277) and rethrown as a test failure whose message has the original
   * and the smaller case.
   */
  private def runChecked(c: VarkaFuzzCase): Unit = {
    try run(c) catch {
      case e: VirtualMachineError => throw e
      case e: InterruptedException => throw e
      case t: Throwable if shrinkFailures =>
        val signature = VarkaFailureSignature.of(t, c.label)
        if (VarkaKnownFailures.isKnown(VarkaKnownFailures.entries, signature)) {
          logWarning(s"known fuzz failure, not shrunk: $signature")
          return
        }
        val shrunk = VarkaShrinker.shrink(c, signature, x => outcomeOf(x.copy(label = "case")))
        val early = (if (shrunk.stoppedEarly) ", budget reached" else "") +
          (if (shrunk.stable) "" else ", UNSTABLE: it did not fail the same way three times")
        fail(Option(t.getMessage).getOrElse(t.getClass.getName) +
          s"\n  shrunk to ${VarkaFuzzCase.describe(shrunk.small)}" +
          s"\n  (${shrunk.runs} runs, ${shrunk.millis} ms$early); signature: $signature", t)
    }
  }

  private def runOne(iteration: Int): Unit = runChecked(drawInt(iteration))

  private def runOneLong(iteration: Int): Unit = runChecked(drawLong(iteration))

  /** `c` with the planted-bug option `name` on. */
  private def planted(c: VarkaFuzzCase, name: String): VarkaFuzzCase =
    c.copy(options = VarkaEmitOption.named(name) match {
      case flag: VarkaEmitOption.Flag => flag.`with`(c.options, true)
      case count: VarkaEmitOption.Count => count.`with`(c.options, 1)
      case other => fail(s"$name is not a planted-bug option: $other")
    })

  /**
   * The first of the first 200 draws that fails with the planted-bug option `name` on, and its
   * signature. Which draws a planted bug reaches depends on the seed, so a test that needs a
   * failing case searches for one rather than naming a draw (VARKA-302).
   */
  private def firstPlantedFailure(name: String): (VarkaFuzzCase, VarkaFailureSignature) =
    (0 until 200).iterator.map(k => planted(drawInt(k), name))
      .map(c => (c, outcomeOf(c))).collectFirst { case (c, Some(sig)) => (c, sig) }
      .getOrElse(fail(s"$name: no failing case in 200 draws (seed $seed)"))

  /** A planted-bug case's failure, shrunk: the first of the first 200 draws that fails. */
  private def shrunkPlanted(name: String): (VarkaFuzzCase, VarkaFailureSignature,
      VarkaShrinker.Shrunk) = {
    val (c, sig) = firstPlantedFailure(name)
    (c, sig, VarkaShrinker.shrink(c, sig, x => outcomeOf(x.copy(label = "case"))))
  }

  private def nodes(c: VarkaFuzzCase): Int = c.roots.map(VarkaShrinker.size).sum

  private def contains(node: VarkaVectorIR, cls: Class[_]): Boolean =
    cls.isInstance(node) || VarkaVectorIR.childrenOf(node).exists(contains(_, cls))

  test("a planted liveness bug shrinks to one small root, the option alone and one row") {
    val (c, sig, shrunk) = shrunkPlanted("misdescribeWordLiveness")
    val small = shrunk.small
    assert(small.roots.size == 1 && nodes(small) <= 5, VarkaFuzzCase.describe(small))
    assert(VarkaFuzzCase.optionDelta(small.options).contains("misdescribeWordLiveness=true"))
    assert(VarkaFuzzCase.optionDelta(small.options).size <= 3)
    assert(small.length <= 17)
    assert(sig.kind == "emitter rejection", sig)
    assert(shrunk.runs <= 600 && shrunk.millis < 60000L && !shrunk.stoppedEarly)
    assert(shrunk.stable)
    assert(nodes(small) < nodes(c))
  }

  test("a planted bug in the generated class is classified and shrunk, not left to escape") {
    val (_, sig, shrunk) = shrunkPlanted("misdescribeAdd")
    val small = shrunk.small
    assert(sig.kind == "generated class: NoSuchMethodError", sig)
    assert(small.roots.exists(contains(_, classOf[AddDays])), VarkaFuzzCase.describe(small))
    assert(VarkaFuzzCase.optionDelta(small.options).contains("misdescribeAdd=true"))
    assert(shrunk.runs <= 600 && shrunk.millis < 60000L && shrunk.stable)
  }

  test("a failure of the generated class fails its test with the smaller case in the message") {
    val (c, _) = firstPlantedFailure("misdescribeWordLiveness")
    val e = intercept[org.scalatest.exceptions.TestFailedException](runChecked(c))
    assert(e.getMessage.contains("shrunk to lane=int"), e.getMessage)
    assert(e.getMessage.contains("signature: emitter rejection"), e.getMessage)
  }

  test("every known failure still reproduces") {
    val replay = (e: VarkaKnownFailures.Entry) => e.lane match {
      case "int" => outcomeOf(drawInt(e.iteration, e.seed))
      case "long" => outcomeOf(drawLong(e.iteration, e.seed))
      // A composition fuzzer's entry (VARKA-294) is replayed by its own suite.
      case _ => Some(e.signature)
    }
    val stale = VarkaKnownFailures.stale(VarkaKnownFailures.entries, replay)
    assert(stale.isEmpty, "these known fuzz failures no longer reproduce; delete them from " +
      s"${VarkaKnownFailures.PATH}: ${stale.mkString(", ")}")
  }

  /** The record node types of the sealed IR, by simple name. */
  private def recordNodeTypes: Set[Class[_]] = {
    def walk(c: Class[_]): Set[Class[_]] = {
      val subs = Option(c.getPermittedSubclasses).map(_.toSet).getOrElse(Set.empty[Class[_]])
      if (subs.isEmpty) Set(c) else subs.flatMap(walk)
    }
    walk(classOf[VarkaVectorIR]).filter(_.isRecord)
  }

  /** Every node type reachable from `node`, by simple name, added to `seen`. */
  private def collectNodeTypes(node: AnyRef, seen: scala.collection.mutable.Set[String]): Unit = {
    seen += node.getClass.getSimpleName
    node.getClass.getRecordComponents.foreach { rc =>
      val v = rc.getAccessor.invoke(node)
      if (v != null && classOf[VarkaVectorIR].isInstance(v)) {
        collectNodeTypes(v.asInstanceOf[AnyRef], seen)
      }
    }
  }

  /**
   * Whether a node type admits 64-bit lanes, asked of the type itself: its canonical
   * constructor is called once over long leaves, and a type that decomposes epoch days refuses
   * there (`requireInt`), while a lane-generic one constructs. Read off the constructors rather
   * than written as a list, so a node type added to the IR is classified by what it does and
   * the long-lane reach test below sees it without anyone editing this file.
   */
  private def admitsLongLanes(cls: Class[_]): Boolean = {
    val longCol = new ColumnRef(0, LaneType.LONG)
    val longCond = new Compare(CompareOp.LT, longCol, longCol)
    val components = cls.getRecordComponents
    val args: Array[AnyRef] = components.map { rc =>
      val t = rc.getType
      if (t == classOf[Cond]) longCond
      else if (classOf[VarkaVectorIR].isAssignableFrom(t)) longCol
      else if (t == java.lang.Long.TYPE) java.lang.Long.valueOf(1L)
      else if (t == java.lang.Integer.TYPE) Integer.valueOf(1)
      else if (t == java.lang.Boolean.TYPE) java.lang.Boolean.FALSE
      else if (t.isEnum) t.getEnumConstants.head.asInstanceOf[AnyRef]
      else if (t == classOf[java.util.List[_]]) {
        java.util.List.of(Integer.valueOf(1), Integer.valueOf(2))
      }
      else fail(s"${cls.getSimpleName}.${rc.getName} has a component type this probe cannot " +
        s"build: ${t.getName}")
    }
    try {
      cls.getDeclaredConstructor(components.map(_.getType): _*).newInstance(args: _*)
      true
    } catch {
      case e: java.lang.reflect.InvocationTargetException
          if e.getCause.isInstanceOf[IllegalArgumentException] => false
    }
  }

  test("the generator reaches every IR node type") {
    // A green fuzz run says nothing about a node type the generator cannot build: the shapes
    // that would have exercised it are simply never drawn, and the suite reports success for
    // the ones it did draw. That is not hypothetical - `TruncDateDynamic` was outside this
    // generator from VARKA-61 until this test was written, and the gap was found by reading the
    // arms rather than by anything failing.
    //
    // So the reachable set is asserted rather than assumed, against the sealed hierarchy itself
    // so a node type added to the IR fails here until the generator can build it. Generation
    // only: no bytes are emitted and nothing runs, so this is cheap enough to draw far more
    // shapes than the differential test does.
    val permitted = recordNodeTypes.map(_.getSimpleName)
    val seen = scala.collection.mutable.Set.empty[String]
    val rnd = new Random(seed)
    for (_ <- 0 until 20000) {
      val numInputs = 1 + rnd.nextInt(3)
      val numLiterals = rnd.nextInt(3)
      val smallOrdinal = if (numInputs > 1) numInputs - 1 else -1
      val levelOrdinal = if (numInputs > 2) numInputs - 2 else -1
      val shapes = new Shapes(rnd, numInputs, numLiterals, smallOrdinal, levelOrdinal)
      val depth = 1 + rnd.nextInt(4)
      val root: AnyRef = if (rnd.nextInt(5) == 0) shapes.cond(depth) else shapes.value(depth).node
      collectNodeTypes(root, seen)
    }
    // Deliberately out of this generator's reach: `NarrowLane` takes a long child, nothing in
    // the IR widens an int lane, and this grammar builds int trees only, so no int shape can
    // hold one. The long-lane generator draws it at a root, and the test below asserts that.
    val missing = permitted -- seen - "NarrowLane"
    assert(missing.isEmpty,
      s"the generator never built: ${missing.toSeq.sorted.mkString(", ")} - add an arm, or " +
        "state here why the node type is deliberately out of the fuzzer's reach")
  }

  test("the long-lane generator reaches every node type that admits 64-bit lanes") {
    // The same assertion for the second corpus, against the set the IR itself defines: a node
    // type is in the long lane's reach exactly when its constructor accepts long leaves. So a
    // lane-generic node added to the IR fails here until `LongShapes` builds it, and a
    // calendar node - which refuses a 64-bit child where it is built - is not asked for.
    val (laneGeneric, intOnly) = recordNodeTypes.partition(admitsLongLanes)
    // The split has to be the one the emitter's javadoc describes, or the probe is not asking
    // the constructors what it thinks it is: every calendar node refuses, and the leaves and
    // the arithmetic accept.
    assert(intOnly.map(_.getSimpleName).contains("Year"))
    assert(laneGeneric.map(_.getSimpleName).contains("ConstDivide"))
    val seen = scala.collection.mutable.Set.empty[String]
    val rnd = new Random(longSeed)
    for (_ <- 0 until 20000) {
      val numInputs = 1 + rnd.nextInt(3)
      val numLiterals = rnd.nextInt(3)
      val shapes = new LongShapes(rnd, numInputs, numLiterals)
      val depth = 1 + rnd.nextInt(4)
      // Through `root`, as `drawLongShape` draws, so the narrowing an output root may carry is
      // asked for with the rest: it constructs over long leaves, so it counts as lane-generic.
      val root: AnyRef = if (rnd.nextInt(5) == 0) shapes.cond(depth) else shapes.root(depth)
      collectNodeTypes(root, seen)
    }
    val missing = laneGeneric.map(_.getSimpleName) -- seen
    assert(missing.isEmpty,
      s"the long-lane generator never built: ${missing.toSeq.sorted.mkString(", ")} - add an " +
        "arm to LongShapes, or state here why the node type is deliberately out of its reach")
  }

  // ---------------------------------------------------------------------------------------------
  // Wide compositions: past the ceilings the one-to-three-root draw never reaches (VARKA-238).
  // ---------------------------------------------------------------------------------------------

  private val wideIterations = sys.props.get("varka.fuzz.wide").map(_.toInt).getOrElse(10)

  /**
   * The options the wide compositions cycle through, each aimed at mechanisms that only a kernel
   * past the driver's ceiling of about 180 groups reaches: the stages, the grouping switches
   * dropped before a decline, the call-site splits and their rollback, the regroup under a small
   * budget, and the declines themselves.
   */
  private val wideVariants: Seq[(String, VarkaEmitOptions)] = {
    // The variants are the size loop's, with the plan and the prediction off (VARKA-236): under
    // both the first build is the last and no mechanism is reached. The defaults run beside
    // them, planned, so every composition is also checked as production emits it.
    val d = VarkaMatrix.base.withPlanSize(false).withPredictGrouping(false)
    Seq(
      "the defaults, planned" -> VarkaMatrix.base,
      "split driver" -> d.withSplitDriver(true),
      "whole driver" -> d.withSplitDriver(false),
      "whole driver, predicted grouping" -> d.withSplitDriver(false).withPredictGrouping(true),
      "whole driver, call sites split" ->
        d.withSplitDriver(false).withCallSiteBudget(8).withHeavyGroupOutputs(2),
      "split driver, 2000-byte budget" -> d.withSplitDriver(true).withMethodByteBudget(2000))
  }

  private var wideDeclines = 0
  private var wideChecked = 0

  /** `drawWideShape`'s draws, until they hold at least 250 value roots or there are twelve. */
  private def wideDraws(rnd: Random): Seq[Drawn] = {
    val draws = scala.collection.mutable.ArrayBuffer.empty[Drawn]
    while (draws.map(_.roots.size).sum < 250 && draws.size < 12) {
      draws += drawWideShape(rnd)
    }
    draws.toSeq
  }

  /**
   * One wide composition: `drawWideShape`'s value roots, drawn until there are at least 250, over
   * the draws' shared column layout. A column keeps the narrowest domain any draw gives it - trunc
   * levels, then month counts, then days - since each is inside the next. The kernel it emits is
   * checked row by row like a drawn shape's; a decline must name a size.
   */
  private def runWide(iteration: Int, trace: VarkaEmitTrace): Unit = {
    val rnd = shapeRandom(seed ^ 0x57494445L, iteration)
    val (label, options) = wideVariants(iteration % wideVariants.size)
    val draws = wideDraws(rnd)
    val roots = draws.flatMap(_.roots).distinct.toSeq
    val numInputs = draws.map(_.numInputs).max
    val numLiterals = draws.map(_.numLiterals).max
    val levels = draws.map(_.levelOrdinal).filter(_ >= 0).toSet
    val small = draws.map(_.smallOrdinal).filter(_ >= 0).toSet
    val lits = Array.fill(numLiterals)(rnd.nextInt(2 * literalBound + 1) - literalBound)
    val length = lengths(rnd.nextInt(lengths.length))
    val patternIds = Seq.fill(numInputs)(rnd.nextInt(patternNames.length))
    val patterns = patternIds.map(pattern(rnd, _, length))
    val forceMasked = length > 1 && rnd.nextInt(4) == 0
    def draw(bound: Long): Int =
      (rnd.nextLong() % (2 * bound + 1) - bound).toInt.max(-bound.toInt).min(bound.toInt)
    val data = Array.tabulate(numInputs, length) { (c, _) =>
      if (levels(c)) {
        DateTimeUtils.TRUNC_TO_WEEK + rnd.nextInt(
          DateTimeUtils.TRUNC_TO_YEAR - DateTimeUtils.TRUNC_TO_WEEK + 1)
      } else {
        draw(if (small(c)) VarkaChrono.MONTH_ARITH_MAX_MONTHS.toLong else columnBound)
      }
    }
    val context = s"wide seed=$seed iteration=$iteration ($label) roots=${roots.size} " +
      s"inputs=$numInputs length=$length patterns=${patternIds.map(patternNames).mkString(",")} " +
      s"literals=${lits.mkString(",")} forceMasked=$forceMasked"
    val className =
      s"org.apache.spark.sql.varka.execution.VarkaFusedFuzzWide${classCounter.addAndGet(1)}"
    def emitWith(o: VarkaEmitOptions, into: VarkaEmitTrace): Option[Array[Byte]] =
      try {
        Some(VarkaLoopEmitter.emitTraced(className, roots.asJava, numInputs, numLiterals, o,
          into))
      } catch {
        case d: VarkaEmitDeclined =>
          assert(d.getMessage.contains("bytes"), s"$context: a decline names no size: " +
            d.getMessage)
          None
      }
    // A variant that declines is the answer it gives, counted; the composition's answers are
    // then checked under the defaults, where the split driver serves it, on a trace of its own
    // so the retry does not count as the variant reaching a mechanism.
    val bytes = emitWith(options, trace).orElse {
      wideDeclines += 1
      emitWith(VarkaMatrix.base, new VarkaEmitTrace)
    }
    bytes.foreach { b =>
      VarkaKernelCheck.runAndCompare(context, className, b, roots, numInputs, lits,
        VarkaKernelCheck.Batch(length, patterns, data, forceMasked))
      wideChecked += 1
    }
  }

  test(s"wide compositions past the driver's ceiling match the reference evaluator, and reach " +
      s"every size mechanism (seed $seed, $wideIterations compositions)") {
    val trace = new VarkaEmitTrace
    wideDeclines = 0
    wideChecked = 0
    (0 until wideIterations).foreach(runWide(_, trace))
    // Under the defaults nothing of this width declines: the split driver serves every one.
    assert(wideChecked === wideIterations,
      s"$wideChecked of $wideIterations compositions ran against the reference evaluator")
    def reached: Seq[(String, Int)] = Seq(
      "a group halved on bytes" -> trace.byteRegroups,
      "a group split on call sites" -> trace.siteSplits,
      "the call-site splits rolled back" -> trace.siteRollbacks,
      "a driver split into stages" -> trace.stageSplits,
      "the exact grouping dropped" -> trace.exactFallbacks,
      "the prediction dropped" -> trace.predictFallbacks,
      "a decline" -> wideDeclines)
    def missed: Seq[String] = reached.collect { case (mechanism, 0) => mechanism }
    // Every variant runs at least once only from a full cycle up. Which mechanisms a width
    // reaches is the defaults' structure, so the option matrix checks only the answers. A
    // mechanism the drawn compositions missed is drawn for: further compositions, cycling the
    // variants and each checked row by row, up to six cycles of them (VARKA-289), so the run
    // fails only where a mechanism has become unreachable, not where ten draws were unlucky.
    if (wideIterations >= wideVariants.size && VarkaMatrix.config.isEmpty) {
      val cap = wideIterations + 6 * wideVariants.size
      var k = wideIterations
      while (missed.nonEmpty && k < cap) {
        runWide(k, trace)
        k += 1
      }
      info(s"${k - wideIterations} further compositions drawn for a mechanism the first " +
        s"$wideIterations missed")
      reached.foreach { case (mechanism, n) => info(s"$mechanism: $n") }
      assert(missed.isEmpty, s"no wide composition reached, in $k: ${missed.mkString(", ")}")
    } else {
      reached.foreach { case (mechanism, n) => info(s"$mechanism: $n") }
    }
  }

  /**
   * What the size control decided for every emission of a fixed corpus, one line each, for a
   * refactor of the size loop to diff against its base (VARKA-290), like the coverage suite's
   * fusion dump: set `VARKA_SIZE_TRACE_DUMP` to a file and run this test on both sides. A line
   * holds the emission's outcome - the class's SHA-256, or the decline's reason, outputs and
   * planned cut - every `VarkaEmitTrace` counter, and the plan's corrections, so a changed
   * decision shows even where it builds the same bytes. The corpus is the first 1500 drawn shapes
   * of each lane under their drawn options, and 24 wide compositions under the wide variants and
   * planned variants that reach the plan's declines, stages and corrections.
   */
  test("the size control's decisions over a fixed corpus, dumped for a refactor to diff " +
      "(opt-in: VARKA_SIZE_TRACE_DUMP=<file>; VARKA-290)") {
    val target = sys.env.get("VARKA_SIZE_TRACE_DUMP")
    assume(target.isDefined, "opt-in: set VARKA_SIZE_TRACE_DUMP to a file")
    val className = "org.apache.spark.sql.varka.execution.VarkaFusedSizeTraceDump"
    val sha = java.security.MessageDigest.getInstance("SHA-256")
    def line(label: String, roots: Seq[VarkaVectorIR], numInputs: Int, numLiterals: Int,
        o: VarkaEmitOptions): String = {
      val trace = new VarkaEmitTrace
      val outcome = try {
        val bytes = VarkaLoopEmitter.emitTraced(className, roots.asJava, numInputs,
          numLiterals, o, trace)
        "class " + sha.digest(bytes).map("%02x".format(_)).mkString
      } catch {
        case d: VarkaEmitDeclined =>
          s"declined ${d.getMessage} outputs=${d.outputs} cut=${d.plannedCut}"
      }
      s"$label | $outcome | builds=${trace.builds} plannedDeclines=${trace.plannedDeclines} " +
        s"plannedStages=${trace.plannedStages} ${trace.reactions.asScala.mkString(" ")} " +
        s"corrections=${trace.corrections.asScala.mkString("; ")}"
    }
    val lines = scala.collection.mutable.ArrayBuffer.empty[String]
    for (k <- 0 until 1500; c <- Seq(drawInt(k), drawLong(k))) {
      lines += line(s"${c.lane} $k ${c.options.canonical()}", c.roots, c.numInputs,
        c.lits.length, c.options)
    }
    val d = VarkaMatrix.base
    val planned = Seq(
      "planned, whole driver" -> d.withSplitDriver(false),
      "planned, driver misread" -> d.withMisdescribeDriverBytes(4000),
      "planned, 2000-byte budget" -> d.withMethodByteBudget(2000),
      "planned, call sites split" -> d.withCallSiteBudget(8).withHeavyGroupOutputs(2),
      "planned, no exact grouping" -> d.withExactGrouping(false),
      "planned, no prediction" -> d.withPredictGrouping(false),
      "planned, whole driver misread" ->
        d.withSplitDriver(false).withMisdescribeDriverBytes(4000))
    for (k <- 0 until 24; (label, o) <- wideVariants ++ planned) {
      val draws = wideDraws(shapeRandom(seed ^ 0x57494445L, k))
      lines += line(s"wide $k ($label)", draws.flatMap(_.roots).distinct,
        draws.map(_.numInputs).max, draws.map(_.numLiterals).max, o)
    }
    java.nio.file.Files.write(java.nio.file.Paths.get(target.get), lines.asJava)
    info(s"${lines.size} emissions to ${target.get}")
  }

  test(s"random IR trees match the reference evaluator (seed $seed, $iterations iterations)") {
    only match {
      case Some(k) => runOne(k)
      case None => (0 until iterations).foreach(runOne)
    }
    reportSkipped()
  }

  test(s"random long-lane IR trees match the reference evaluator (seed $longSeed, " +
      s"$iterations iterations)") {
    only match {
      case Some(k) => runOneLong(k)
      case None => (0 until iterations).foreach(runOneLong)
    }
    reportSkipped()
  }

  /**
   * The shapes skipped as past the class-file cap, reported and bounded: the night that found
   * them saw one in three hundred thousand trees per lane, so a run that skips more than a few
   * per thousand has either a heavier grammar or an emitter that declines where it should
   * build, and neither may pass quietly.
   */
  private def reportSkipped(): Unit = {
    info(s"size control over the run: ${randomTrace.builds} builds, " +
      s"${randomTrace.byteRegroups} regroups on bytes, ${randomTrace.siteSplits} call-site " +
      s"splits, ${randomTrace.siteRollbacks} rolled back, ${randomTrace.stageSplits} stage " +
      s"splits, ${randomTrace.fallbacks()} grouping switches dropped")
    val skipped = skippedPastTheCap.getAndSet(0)
    if (skipped > 0) {
      info(s"$skipped shape(s) past the class-file cap on a method in every form the emitter " +
        "has: declined, and skipped (VARKA-219)")
    }
    assert(skipped <= 2 + iterations / 1000,
      s"$skipped shapes skipped as past the class-file cap in $iterations iterations")
  }
}
