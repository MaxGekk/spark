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

import java.lang.foreign.{Arena, MemorySegment, ValueLayout}
import java.time.LocalDate
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader
import org.apache.spark.sql.catalyst.expressions.codegen.varka.{VarkaEmitterFixtures => F}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR._

/**
 * The fixtures every emitter suite shares, as the suites see them. The code is
 * [[VarkaEmitterFixtures]] (VARKA-268): the emitted class built through [[emitMulti]] and loaded
 * through [[load]], the input and output columns over Arrow-shaped memory, the data, and the three
 * sweeps of the narrow range. This trait keeps the names and Scala signatures the suites were
 * written against - tuples, Scala functions, default arguments - and forwards each to the Java
 * class. The two matrices, [[checkMatrix]] at the int lane and [[checkLongMatrix]] at the long
 * lane, run a shape over every (length, null pattern) case against [[VarkaReferenceEvaluator]],
 * the independent Scala implementation of the semantics, and stay here with it, as
 * `VarkaKernelCheck` does.
 *
 * The suites are split by the family of the emitter they test: `VarkaEmitterArithmeticSuite`,
 * `VarkaEmitterChronoSuite`, `VarkaEmitterDivisionSuite`, `VarkaEmitterLongLaneSuite`,
 * `VarkaEmitterValiditySuite`, `VarkaEmitterBudgetSuite` and `VarkaEmitterContractSuite`. Each
 * must also run green under `-XX:MaxVectorSize=16`, the four-lane shape where width bugs hide:
 *
 * {{{
 *   build/sbt "project catalyst" 'set Test/javaOptions += "-XX:MaxVectorSize=16"' \
 *     "testOnly *VarkaEmitter*Suite"
 * }}}
 */
trait VarkaEmitterTestBase extends SparkFunSuite with VarkaTestWatchdog {

  protected val classCounter = new AtomicInteger(0)

  protected val lengths: Seq[Int] = F.LENGTHS.toSeq

  protected val offsets: Seq[Int] = F.OFFSETS.toSeq

  /** Null pattern: name -> which rows are null. */
  protected val nullPatterns: Seq[(String, Int => Boolean)] = Seq(
    ("null-free", _ => false),
    ("mixed", i => i % 5 == 0),
    ("alternating", i => i % 2 == 1),
    ("all-null", _ => true))

  protected def addDays(offsetSlot: Int): VarkaVectorIR = F.addDays(offsetSlot)

  /** An `AddDays`/`SubDays` chain of the given depth, alternating so C2 cannot reassociate it. */
  protected def chain(depth: Int, slotBase: Int = 0): VarkaVectorIR = F.chain(depth, slotBase)

  // -------------------------------------------------------------------------------------------
  // The emitted class
  // -------------------------------------------------------------------------------------------

  /** Emits the chain into a uniquely named class; returns the name with the bytes. */
  protected def emit(
      root: VarkaVectorIR,
      numLiterals: Int,
      options: VarkaEmitOptions = VarkaMatrix.base): (String, Array[Byte]) =
    emitMulti(Seq(root), 1, numLiterals, options)

  /** The multi-output, multi-input version of [[emit]] (VARKA-10). */
  protected def emitMulti(
      roots: Seq[VarkaVectorIR],
      numInputs: Int,
      numLiterals: Int,
      options: VarkaEmitOptions = VarkaMatrix.base): (String, Array[Byte]) = {
    val emitted = F.emit(roots.asJava, numInputs, numLiterals, options)
    (emitted.className, emitted.bytes)
  }

  /** Loads an emitted class through the per-task loader and instantiates it. */
  protected def load(
      named: (String, Array[Byte])): (VarkaFusedKernel, VarkaGeneratedClassLoader) = {
    val loaded = F.load(new F.Emitted(named._1, named._2))
    (loaded.kernel, loaded.loader)
  }

  /** The declared method names of an emitted class - how the method layout is asserted. */
  protected def methodNames(named: (String, Array[Byte])): Seq[String] =
    F.methodNames(new F.Emitted(named._1, named._2)).asScala.toSeq

  // -------------------------------------------------------------------------------------------
  // Columns
  // -------------------------------------------------------------------------------------------

  /** One column's worth of buffers: data, validity bitmap and its null count. */
  protected type Col = F.Col

  protected def Col(data: MemorySegment, validity: MemorySegment, nullCount: Int): Col =
    new F.Col(data, validity, nullCount)

  protected def alloc(arena: Arena, bytes: Long): MemorySegment = F.alloc(arena, bytes)

  protected def makeInput(arena: Arena, length: Int, isNull: Int => Boolean): Col =
    F.makeInput(arena, length, i => isNull(i))

  /** The value a null slot holds; see [[VarkaEmitterFixtures.poison]] for why it alternates. */
  protected def poison(nullOrdinal: Int): Int = F.poison(nullOrdinal)

  /** See [[VarkaEmitterFixtures.makeInputData]], and when `poisonNulls` is off. */
  protected def makeInputData(
      arena: Arena,
      length: Int,
      isNull: Int => Boolean,
      value: Int => Int,
      poisonNulls: Boolean = true): Col =
    F.makeInputData(arena, length, i => isNull(i), i => value(i), poisonNulls)

  /** See [[VarkaEmitterFixtures.makeOutput]]: validity starts at `fill`, all ones by default. */
  protected def makeOutput(
      arena: Arena,
      length: Int,
      validityBytes: Long = -1L,
      fill: VarkaKernelChecks.ValidityFill = VarkaKernelChecks.ValidityFill.ONES)
      : (MemorySegment, MemorySegment) = {
    val out = F.makeOutput(arena, length, validityBytes, fill)
    (out.data, out.validity)
  }

  /** The destination validity bytes an emission under `options` needs; see [[makeOutput]]. */
  protected def outputValidityBytes(options: VarkaEmitOptions, length: Int): Long =
    F.outputValidityBytes(options, length)

  /** Asserts two (data, validity) outputs agree bit for bit and, where valid, value for value. */
  protected def assertSameOutput(
      length: Int,
      expected: (MemorySegment, MemorySegment),
      actual: (MemorySegment, MemorySegment),
      context: String): Unit =
    F.assertSameOutput(length, new F.Output(expected._1, expected._2),
      new F.Output(actual._1, actual._2), context)

  /** Runs a two-input kernel with one output, returning the batch status. */
  protected def runKernel2(kernel: VarkaFusedKernel, a: Col, b: Col,
      out: (MemorySegment, MemorySegment), length: Int): Int =
    F.runKernel2(kernel, a, b, new F.Output(out._1, out._2), length)

  // -------------------------------------------------------------------------------------------
  // VARKA-11: the reference evaluator - an independent Scala implementation of the milestone's
  // 2.6 semantics (three-valued conditions, blend, null-skipping greatest/least, floorMod)
  // that every predication test runs the emitted loop against, row for row and bit for bit.
  // It lives in VarkaReferenceEvaluator, shared with the IR fuzzer; these are the suite's
  // names for it.
  // -------------------------------------------------------------------------------------------

  protected def evalValue(
      node: VarkaVectorIR, row: Seq[Option[Int]], lits: Array[Int]): Option[Int] =
    VarkaReferenceEvaluator.evalValue(node, row, lits)

  protected def evalCond(
      cond: Cond, row: Seq[Option[Int]], lits: Array[Int]): Option[Boolean] =
    VarkaReferenceEvaluator.evalCond(cond, row, lits)

  protected def evalLong(
      node: VarkaVectorIR, row: Seq[Option[Long]], lits: Array[Long]): Option[Long] =
    VarkaReferenceEvaluator.evalLong(node, row, lits)

  protected def evalCondLong(
      cond: Cond, row: Seq[Option[Long]], lits: Array[Long]): Option[Boolean] =
    VarkaReferenceEvaluator.evalCondLong(cond, row, lits)

  protected def defaultData(col: Int, i: Int): Int = F.defaultData(col, i)

  /**
   * Emits the outputs once, then runs every (length, per-column null pattern) case against the
   * reference evaluator. With `forceMasked` a null-free column reports one null over a
   * full-set bitmap, which sends the batch down `runMasked` - the dispatcher tests only
   * `nullCount != 0` - so the masked body is exercised on the same data the dense body serves.
   *
   * Both bodies meet the oracle whatever the cases select (VARKA-284): if no case took one of
   * them, it runs once more - the masked body forced on the first case longer than a row (a forced
   * batch of one row is all-null by contract), or the dense one on the first length with no nulls.
   */
  protected def checkMatrix(
      roots: Seq[VarkaVectorIR],
      numInputs: Int,
      lits: Array[Int],
      caseLengths: Seq[Int],
      patternCombos: Seq[Seq[Int => Boolean]],
      data: (Int, Int) => Int = defaultData,
      forceMasked: Boolean = false,
      ctx: String = "",
      options: VarkaEmitOptions = VarkaMatrix.base): Unit = {
    val (kernel, loader) = load(emitMulti(roots, numInputs, lits.length, options))
    var tookDense = false
    var tookMasked = false
    // One case; returns whether it took the masked body.
    def runCase(length: Int, combo: Seq[Int => Boolean], comboId: Any,
        forceMasked: Boolean): Boolean = {
        val arena = Arena.ofConfined()
        try {
          val cols = (0 until numInputs).map { c =>
            makeInputData(arena, length, combo(c), i => data(c, i))
          }
          val nullCounts = cols.map { col =>
            if (forceMasked && col.nullCount == 0) 1 else col.nullCount
          }
          val masked = nullCounts.exists(_ != 0)
          val validityAddrs = cols.zip(nullCounts).map { case (col, nc) =>
            if (nc == 0 || nc == length) col.validityAddress(length) else col.validity.address()
          }
          // A Cond root is a selection output (VARKA-21): its data address is 0L per the
          // kernel contract - exactly what the filter evaluator passes - so a regression
          // that touches it faults instead of writing somewhere silently.
          // Each case at both validity fills (VARKA-138): ones shows a bit the kernel fails to
          // clear, zero - what production's Arrow buffers hold - one it fails to set.
          for (fill <- VarkaKernelCheck.bothFills) {
            val outs = roots.map(_ =>
              makeOutput(arena, length, outputValidityBytes(options, length), fill))
            val dstData = roots.zip(outs).map { case (root, out) =>
              if (root.isInstanceOf[Cond]) 0L else out._1.address()
            }
            // The status is asserted, not discarded: a guard that declines every batch
            // leaves the destination values correct - the arithmetic does not depend on it -
            // so without this the matrix stays green while the kernel computes nothing in
            // production. Every shape this harness drives is one the kernel must answer.
            val status = kernel.run(cols.map(_.data.address()).toArray, validityAddrs.toArray,
              nullCounts.toArray, dstData.toArray,
              outs.map(_._2.address()).toArray, lits, length,
              VarkaEmitterTestSupport.scratch(kernel, length))
            assert(status === 0,
              s"$ctx: the kernel declined a batch it should have computed " +
                s"(length $length, combo $comboId, $fill, status $status)")
            for (i <- 0 until length) {
              val row = (0 until numInputs).map { c =>
                if (combo(c)(i)) None else Some(data(c, i))
              }
              for ((root, o) <- roots.zipWithIndex) {
                val bit = (outs(o)._2.get(ValueLayout.JAVA_BYTE, i / 8L) & (1 << (i % 8))) != 0
                val where = s"$ctx len=$length combo=$comboId out=$o row=$i fill=$fill"
                root match {
                  case c: Cond =>
                    // The selection rule: a bit is set exactly where the condition is known
                    // true - unknown reads as false (the mask-root null rule).
                    val expected = evalCond(c, row, lits).contains(true)
                    assert(bit === expected, s"$where: selection differs (want $expected)")
                  case _ =>
                    val expected = evalValue(root, row, lits)
                    assert(bit === expected.isDefined,
                      s"$where: validity differs (want $expected)")
                    expected.foreach { v =>
                      assert(outs(o)._1.get(ValueLayout.JAVA_INT, i * 4L) === v, s"$where: value")
                    }
                }
              }
            }
          }
          masked
        } finally {
          arena.close()
        }
    }
    try {
      for (length <- caseLengths; (combo, comboId) <- patternCombos.zipWithIndex) {
        if (runCase(length, combo, comboId, forceMasked)) tookMasked = true else tookDense = true
      }
      if (numInputs > 0 && caseLengths.nonEmpty) {
        if (!tookMasked) {
          runCase(caseLengths.find(_ > 1).getOrElse(17), patternCombos.head, "masked (forced)",
            forceMasked = true)
        }
        if (!tookDense) {
          runCase(caseLengths.head, Seq.fill(numInputs)((_: Int) => false), "dense (no nulls)",
            forceMasked = false)
        }
      }
    } finally {
      loader.release()
    }
  }

  // -------------------------------------------------------------------------------------------
  // The long lane (VARKA-85, step 4)
  // -------------------------------------------------------------------------------------------

  /** One 64-bit column; see [[VarkaEmitterFixtures.makeLongInput]]. */
  protected def makeLongInput(
      arena: Arena,
      length: Int,
      isNull: Int => Boolean,
      value: Int => Long,
      poisonNulls: Boolean = true): Col =
    F.makeLongInput(arena, length, i => isNull(i), i => value(i), poisonNulls)

  /** `makeOutput`'s twin at 64-bit lanes. */
  protected def makeLongOutput(
      arena: Arena,
      length: Int,
      fill: VarkaKernelChecks.ValidityFill = VarkaKernelChecks.ValidityFill.ONES)
      : (MemorySegment, MemorySegment) = {
    val out = F.makeLongOutput(arena, length, fill)
    (out.data, out.validity)
  }

  /**
   * The int matrix's twin at 64-bit lanes, over the subset VARKA-85 ships there: the leaves, the
   * arithmetic and its modes, the negate, the comparisons, the hull ops and the conditional.
   * It drives the kernel through the eight-argument `run` - the long lane's own entry point,
   * whose second scalar array is what a 64-bit literal needs - and compares against
   * `evalLong`, which is VARKA-119's first part.
   */
  protected def checkLongMatrix(
      roots: Seq[VarkaVectorIR],
      numInputs: Int,
      lits: Array[Long],
      caseLengths: Seq[Int],
      patternCombos: Seq[Seq[Int => Boolean]],
      data: (Int, Int) => Long,
      ctx: String,
      lanes: Option[Int] = None,
      base: VarkaEmitOptions = VarkaMatrix.base): Unit = {
    // Unless a test names a width of its own, as a sweep of widths does, the matrix's wins: a
    // fixed one would put a second species beside a configuration's (VARKA-246).
    val laneCount = lanes.getOrElse(base.lanesOverride())
    val options = base.withLanesOverride(laneCount)
    val (kernel, loader) = load(emitMulti(roots, numInputs, lits.length, options))
    try {
      for (length <- caseLengths; (combo, comboId) <- patternCombos.zipWithIndex) {
        val arena = Arena.ofConfined()
        try {
          val cols = (0 until numInputs).map { c =>
            makeLongInput(arena, length, combo(c), i => data(c, i))
          }
          // Each case at both validity fills (VARKA-138), as the int matrix runs them.
          for (fill <- VarkaKernelCheck.bothFills) {
            val outs = roots.map(_ => makeLongOutput(arena, length, fill))
            val dstData = roots.zip(outs).map { case (root, out) =>
              if (root.isInstanceOf[Cond]) 0L else out._1.address()
            }
            val status = kernel.run(cols.map(_.data.address()).toArray,
              cols.map(_.validityAddress(length)).toArray, cols.map(_.nullCount).toArray,
              dstData.toArray, outs.map(_._2.address()).toArray,
              Array.empty[Int], lits, length, VarkaEmitterTestSupport.scratch(kernel, length))
            assert(status === 0,
              s"$ctx at $laneCount lanes: the kernel declined a batch it should have computed " +
                s"(length $length, combo $comboId, $fill, status $status)")
            for (i <- 0 until length) {
              val row = (0 until numInputs).map { c =>
                if (combo(c)(i)) None else Some(data(c, i))
              }
              for ((root, o) <- roots.zipWithIndex) {
                val bit = (outs(o)._2.get(ValueLayout.JAVA_BYTE, i / 8L) & (1 << (i % 8))) != 0
                val where =
                  s"$ctx at $laneCount lanes, len=$length combo=$comboId out=$o row=$i fill=$fill"
                root match {
                  case c: Cond =>
                    val expected = evalCondLong(c, row, lits).contains(true)
                    assert(bit === expected, s"$where: selection differs (want $expected)")
                  case _ =>
                    val expected = evalLong(root, row, lits)
                    assert(bit === expected.isDefined,
                      s"$where: validity differs (want $expected)")
                    expected.foreach { v =>
                      assert(outs(o)._1.get(ValueLayout.JAVA_LONG, i * 8L) === v, s"$where: value")
                    }
                }
              }
            }
          }
        } finally {
          arena.close()
        }
      }
    } finally {
      loader.release()
    }
  }

  /** Every pair (or triple) of the four null patterns, as per-column combinations. */
  protected def combos(numInputs: Int): Seq[Seq[Int => Boolean]] = {
    val ps = nullPatterns.map(_._2)
    if (numInputs == 1) ps.map(Seq(_))
    else if (numInputs == 2) for (a <- ps; b <- ps) yield Seq(a, b)
    else for (a <- ps; b <- ps; c <- ps) yield Seq(a, b, c)
  }

  // -------------------------------------------------------------------------------------------
  // The data
  // -------------------------------------------------------------------------------------------

  /** The calendar boundary set every bounded calendar test walks. */
  protected val calendarBoundaryDays: Array[Int] = F.CALENDAR_BOUNDARY_DAYS.clone()

  protected def calendarBoundaryDay(c: Int, i: Int): Int = F.calendarBoundaryDay(c, i)

  // VARKA-35: trunc(date, YEAR | MONTH | QUARTER), two lowerings behind VarkaEmitOptions.truncDate.
  protected val truncRoots: Seq[VarkaVectorIR] = F.TRUNC_ROOTS.asScala.toSeq

  // VARKA-42: make_date over three int columns; see VarkaEmitterFixtures.MAKE_DATE_ALL.
  protected val makeDateAll: Array[(Int, Int, Int)] =
    F.MAKE_DATE_ALL.map(t => (t.year, t.month, t.day))

  protected val makeDateValid: Array[(Int, Int, Int)] =
    F.MAKE_DATE_VALID.map(t => (t.year, t.month, t.day))

  /** Column `c` of the `i`-th triple, cycling: 0 the year, 1 the month, 2 the day. */
  protected def tripleData(triples: Array[(Int, Int, Int)])(c: Int, i: Int): Int = {
    val t = triples(i % triples.length)
    if (c == 0) t._1 else if (c == 1) t._2 else t._3
  }

  protected val makeDateNull: MakeDate = F.MAKE_DATE_NULL

  // VARKA-37's ISO-week days; see VarkaEmitterFixtures.ISO_WEEK_DAYS.
  protected val isoWeekDays: Array[Int] = F.ISO_WEEK_DAYS.clone()

  protected def isoWeekDay(c: Int, i: Int): Int = F.isoWeekDay(c, i)

  /** The days the calendar differentials drive: the range's edges, then a strided walk. */
  protected def calendarDays(c: Int, i: Int): Int = F.calendarDays(c, i)

  /** Odd or prime lengths, so every case exercises the epilogue; see the Java constant. */
  protected val remainderLengths: Seq[Int] = F.REMAINDER_LENGTHS.toSeq

  /** Values that put the sign test under load: both extremes, their neighbours, and zero. */
  protected def extreme(col: Int, i: Int): Int = F.extreme(col, i)

  /** The same spread, kept small enough that no op over two of them can overflow. */
  protected def small(col: Int, i: Int): Int = F.small(col, i)

  // -------------------------------------------------------------------------------------------
  // The sweeps of the narrow range
  // -------------------------------------------------------------------------------------------

  protected def sweepTrunc(options: VarkaEmitOptions): Unit = F.sweepTrunc(options)

  /** What `LocalDate` says the five extractions are, in the order they are emitted. */
  protected val allCalendarFields: LocalDate => Seq[Int] =
    date => F.ALL_CALENDAR_FIELDS.apply(date).toSeq

  protected def sweepCalendar(
      roots: Seq[VarkaVectorIR],
      options: VarkaEmitOptions,
      expected: LocalDate => Seq[Int] = allCalendarFields): Unit =
    F.sweepCalendar(roots.asJava, options, date => expected(date).toArray)

  protected def sweepLastDay(options: VarkaEmitOptions): Unit = F.sweepLastDay(options)

  // -------------------------------------------------------------------------------------------
  // VARKA-32 step B and after: options and what an emitted class costs
  // -------------------------------------------------------------------------------------------

  protected val sharing = VarkaMatrix.base.withShareChronoPrefix(true)

  protected val unshared = VarkaMatrix.base.withShareChronoPrefix(false)

  // VARKA-63's int arithmetic. `checkOff` is the A/B arm the benchmark prices and the flag the
  // emitter reads to drop the sign test; it is never a correct setting for an ANSI query.
  protected val checkOff = VarkaMatrix.base.withCheckIntOverflow(false)

  /** The lane ops one emitted body method runs: its `IntVector` invocations. */
  protected def laneOps(bytes: Array[Byte], method: String): Int = F.laneOps(bytes, method)

  /** What the prefix's month step costs on the axis `options` picks. */
  protected def monthStepOps(options: VarkaEmitOptions): Int = F.monthStepOps(options)

  /** The single masked epilogue's bytecode size in the single-epilogue form (budget 0). */
  protected def singleEpilogueSize(
      roots: Seq[VarkaVectorIR], numInputs: Int, options: VarkaEmitOptions): Int =
    F.singleEpilogueSize(roots.asJava, numInputs, options)
}
