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
import java.lang.ref.{ReferenceQueue, WeakReference}
import java.time.LocalDate
import java.time.temporal.IsoFields
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.varka.vector.DateVectorOps

/**
 * Unit tests for [[VarkaLoopEmitter]] (milestone 2, tasks 9-11): the emitted fused loop must
 * match the hand-written `DateVectorOps` kernels - the reference semantics for the arithmetic
 * ops - row for row and bit for bit, across lengths that straddle every lane and byte boundary
 * of the 4-, 8- and 16-lane species, every null pattern (applied independently per column for
 * the multi-input shapes), and offsets including int wrap-around. The predication ops (task 11)
 * run against an in-suite reference evaluator implementing the milestone's 2.6 semantics
 * independently - Kleene three-valued conditions, blend, null-skipping greatest/least,
 * full-range floorMod - across the same matrices.
 *
 * The suite must also run green under `-XX:MaxVectorSize=16` (the four-lane shape; milestone 1's
 * finding 1 is why that width is where bugs hide):
 * {{{
 *   build/sbt "project catalyst" 'set Test/javaOptions += "-XX:MaxVectorSize=16"' \
 *     "testOnly *VarkaLoopEmitterSuite"
 * }}}
 */
class VarkaLoopEmitterSuite extends SparkFunSuite {

  private val classCounter = new AtomicInteger(0)

  // Boundary-straddling lengths for every species this can run at (4, 8 or 16 lanes), plus the
  // byte boundaries of the bit-packed validity, plus batch-sized ones.
  private val lengths = Seq(0, 1, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65,
    1000, 4096, 4097)

  private val offsets = Seq(0, 1, -1, 3, Int.MaxValue - 1)

  /** Null pattern: name -> which rows are null. */
  private val nullPatterns: Seq[(String, Int => Boolean)] = Seq(
    ("null-free", _ => false),
    ("mixed", i => i % 5 == 0),
    ("alternating", i => i % 2 == 1),
    ("all-null", _ => true))

  private def addDays(offsetSlot: Int): VarkaVectorIR =
    new AddDays(new ColumnRef(0), new LiteralSlot(offsetSlot))

  /** An `AddDays`/`SubDays` chain of the given depth, alternating so C2 cannot reassociate it. */
  private def chain(depth: Int, slotBase: Int = 0): VarkaVectorIR = {
    var node: VarkaVectorIR = new ColumnRef(0)
    for (level <- 0 until depth) {
      node = if (level % 2 == 0) new AddDays(node, new LiteralSlot(slotBase + level))
      else new SubDays(node, new LiteralSlot(slotBase + level))
    }
    node
  }

  /** Emits the chain into a uniquely named class; returns the name with the bytes. */
  private def emit(
      root: VarkaVectorIR,
      numLiterals: Int,
      options: VarkaEmitOptions = VarkaEmitOptions.DEFAULTS): (String, Array[Byte]) =
    emitMulti(Seq(root), 1, numLiterals, options)

  /**
   * The multi-output, multi-input version of [[emit]] (task 10). Since task 23 the emitter's
   * non-shape inputs travel as a [[VarkaEmitOptions]] value on the call rather than as static
   * hooks a test had to set and reset, so a variant is just a different argument here.
   */
  private def emitMulti(
      roots: Seq[VarkaVectorIR],
      numInputs: Int,
      numLiterals: Int,
      options: VarkaEmitOptions = VarkaEmitOptions.DEFAULTS): (String, Array[Byte]) = {
    val name = s"org.apache.spark.sql.varka.execution.VarkaFusedTest${classCounter.addAndGet(1)}"
    (name, VarkaLoopEmitter.emit(name, roots.asJava, numInputs, numLiterals, null, null, options))
  }

  /** Loads an emitted class through the per-task loader and instantiates it. */
  private def load(named: (String, Array[Byte])): (VarkaFusedKernel, VarkaGeneratedClassLoader) = {
    val (className, bytes) = named
    val loader = new VarkaGeneratedClassLoader(getClass.getClassLoader)
    loader.defineGeneratedClass(className, bytes)
    val kernel = loader.loadClass(className).getConstructor()
      .newInstance().asInstanceOf[VarkaFusedKernel]
    (kernel, loader)
  }

  /** Runs one kernel over one input column, returning the batch status it reports. */
  private def runKernel(
      kernel: VarkaFusedKernel,
      input: Col,
      out: (MemorySegment, MemorySegment),
      length: Int): Int =
    kernel.run(
      Array(input.data.address()), Array(input.validity.address()), Array(input.nullCount),
      Array(out._1.address()), Array(out._2.address()), Array.empty[Int], length)

  /** The declared method names of an emitted class - how the method layout is asserted. */
  private def methodNames(named: (String, Array[Byte])): Seq[String] = {
    val (className, bytes) = named
    val loader = new VarkaGeneratedClassLoader(getClass.getClassLoader)
    loader.defineGeneratedClass(className, bytes)
    loader.loadClass(className).getDeclaredMethods.map(_.getName).toSeq
  }

  /** One column's worth of buffers: data, validity bitmap and its null count. */
  private case class Col(data: MemorySegment, validity: MemorySegment, nullCount: Int) {
    // Per the kernel contract a null-free or all-null column may pass 0L for its validity.
    def validityAddress(length: Int): Long =
      if (nullCount == 0 || nullCount == length) 0L else validity.address()
  }

  private def alloc(arena: Arena, bytes: Long): MemorySegment =
    arena.allocate(math.max(bytes, 1L), 8)

  private def makeInput(arena: Arena, length: Int, isNull: Int => Boolean): Col =
    makeInputData(arena, length, isNull, i => i * 31 - 7000)

  private def makeInputData(
      arena: Arena, length: Int, isNull: Int => Boolean, value: Int => Int): Col = {
    val data = alloc(arena, length * 4L)
    val validity = alloc(arena, (length + 7) / 8L)
    validity.fill(0.toByte)
    var nulls = 0
    for (i <- 0 until length) {
      data.set(ValueLayout.JAVA_INT, i * 4L, value(i))
      if (isNull(i)) {
        nulls += 1
      } else {
        val off = i / 8L
        val old = validity.get(ValueLayout.JAVA_BYTE, off)
        validity.set(ValueLayout.JAVA_BYTE, off, (old | (1 << (i % 8))).toByte)
      }
    }
    Col(data, validity, nulls)
  }

  private def makeOutput(arena: Arena, length: Int): (MemorySegment, MemorySegment) = {
    val data = alloc(arena, length * 4L)
    // A sentinel no chain produces from the inputs above, so an unwritten valid row shows.
    for (i <- 0 until length) data.set(ValueLayout.JAVA_INT, i * 4L, 0xDEADBEEF)
    val validity = alloc(arena, (length + 7) / 8L)
    validity.fill(0xFF.toByte) // the loop must zero it; stale bits must not leak through
    (data, validity)
  }

  /** Asserts two (data, validity) outputs agree bit for bit and, where valid, value for value. */
  private def assertSameOutput(
      length: Int,
      expected: (MemorySegment, MemorySegment),
      actual: (MemorySegment, MemorySegment),
      context: String): Unit = {
    for (b <- 0L until (length + 7) / 8L) {
      assert(actual._2.get(ValueLayout.JAVA_BYTE, b) === expected._2.get(ValueLayout.JAVA_BYTE, b),
        s"$context: validity byte $b differs")
    }
    for (i <- 0 until length) {
      val valid = (expected._2.get(ValueLayout.JAVA_BYTE, i / 8L) & (1 << (i % 8))) != 0
      if (valid) {
        assert(actual._1.get(ValueLayout.JAVA_INT, i * 4L) ===
          expected._1.get(ValueLayout.JAVA_INT, i * 4L), s"$context: row $i differs")
      }
    }
  }

  // -----------------------------------------------------------------------------------------
  // Task 11: the reference evaluator - an independent Scala implementation of the milestone's
  // 2.6 semantics (three-valued conditions, blend, null-skipping greatest/least, floorMod)
  // that every predication test runs the emitted loop against, row for row and bit for bit.
  // -----------------------------------------------------------------------------------------

  private def evalValue(
      node: VarkaVectorIR, row: Seq[Option[Int]], lits: Array[Int]): Option[Int] = node match {
    case c: ColumnRef => row(c.ordinal())
    case l: LiteralSlot => Some(lits(l.index()))
    case n: AddDays =>
      for (d <- evalValue(n.days(), row, lits); o <- evalValue(n.offset(), row, lits))
        yield d + o
    case n: SubDays =>
      for (d <- evalValue(n.days(), row, lits); o <- evalValue(n.offset(), row, lits))
        yield d - o
    case n: DateDiff =>
      for (e <- evalValue(n.end(), row, lits); s <- evalValue(n.start(), row, lits)) yield e - s
    case n: DayOfWeek =>
      evalValue(n.days(), row, lits).map(v => (Math.floorMod(v, 7) + 4) % 7 + 1)
    case n: WeekDay =>
      evalValue(n.days(), row, lits).map(v => (Math.floorMod(v, 7) + 3) % 7)
    // The oracle is Spark's own getNextDateForDayOfWeek, quoted directly, not the lowering:
    // Scala's Int arithmetic wraps exactly as the lanes do, so this is exact even at
    // Int.MinValue, and it is byte-for-byte what the row engine evaluates.
    case n: NextDay =>
      for (d <- evalValue(n.days(), row, lits); k <- evalValue(n.offset(), row, lits))
        yield d + 1 + Math.floorMod(k - d, 7)
    // The calendar oracle is java.time, which is what DateTimeUtils.getYear and its three
    // siblings call - not VarkaChrono, so the emitted bytes are checked against the
    // definition rather than against the model they were derived from.
    case n: Year =>
      evalValue(n.days(), row, lits).map(v => LocalDate.ofEpochDay(v.toLong).getYear)
    case n: Month =>
      evalValue(n.days(), row, lits).map(v => LocalDate.ofEpochDay(v.toLong).getMonthValue)
    case n: DayOfMonth =>
      evalValue(n.days(), row, lits).map(v => LocalDate.ofEpochDay(v.toLong).getDayOfMonth)
    case n: Quarter =>
      // IsoFields.QUARTER_OF_YEAR, which is what DateTimeUtils.getQuarter calls - not
      // (month + 2) / 3, which is what the emitter computes. An oracle that restates the
      // implementation is not an oracle.
      evalValue(n.days(), row, lits)
        .map(v => LocalDate.ofEpochDay(v.toLong).get(IsoFields.QUARTER_OF_YEAR))
    case n: DayOfYear =>
      evalValue(n.days(), row, lits).map(v => LocalDate.ofEpochDay(v.toLong).getDayOfYear)
    // The oracle is DateTimeUtils.dateAddMonths - the definition AddMonthsBase's nullSafeEval
    // calls - not VarkaChrono.daysFromCivil, which is the model this node's own arithmetic was
    // derived from and checked against; using it here would test the lowering against itself.
    case n: AddMonths =>
      for (d <- evalValue(n.days(), row, lits); m <- evalValue(n.months(), row, lits))
        yield DateTimeUtils.dateAddMonths(d, m)
    case n: Greatest =>
      pick(evalValue(n.left(), row, lits), evalValue(n.right(), row, lits), math.max)
    case n: Least =>
      pick(evalValue(n.left(), row, lits), evalValue(n.right(), row, lits), math.min)
    case n: IfElse =>
      if (evalCond(n.cond(), row, lits).contains(true)) evalValue(n.thenNode(), row, lits)
      else evalValue(n.elseNode(), row, lits)
    case c: Cond => fail(s"condition $c evaluated as a value")
  }

  private def pick(a: Option[Int], b: Option[Int], op: (Int, Int) => Int): Option[Int] =
    (a, b) match {
      case (Some(x), Some(y)) => Some(op(x, y))
      case (Some(x), None) => Some(x)
      case (None, y) => y
    }

  /** Kleene three-valued logic; `None` is unknown, and only known-true selects THEN. */
  private def evalCond(
      cond: Cond, row: Seq[Option[Int]], lits: Array[Int]): Option[Boolean] = cond match {
    case n: Compare =>
      for (l <- evalValue(n.left(), row, lits); r <- evalValue(n.right(), row, lits)) yield {
        n.op() match {
          case CompareOp.LT => l < r
          case CompareOp.LE => l <= r
          case CompareOp.GT => l > r
          case CompareOp.GE => l >= r
          case CompareOp.EQ => l == r
        }
      }
    case n: And =>
      (evalCond(n.left(), row, lits), evalCond(n.right(), row, lits)) match {
        case (Some(false), _) | (_, Some(false)) => Some(false)
        case (Some(true), Some(true)) => Some(true)
        case _ => None
      }
    case n: Or =>
      (evalCond(n.left(), row, lits), evalCond(n.right(), row, lits)) match {
        case (Some(true), _) | (_, Some(true)) => Some(true)
        case (Some(false), Some(false)) => Some(false)
        case _ => None
      }
    case n: Not => evalCond(n.child(), row, lits).map(!_)
    // The first total condition (task 20): IS NOT NULL never returns unknown - a null
    // operand is a definite false, not a missing answer.
    case n: IsNotNull => Some(evalValue(n.child(), row, lits).isDefined)
  }

  private def defaultData(col: Int, i: Int): Int = (i * (col + 3)) % 23 - 11

  /**
   * Emits the outputs once, then runs every (length, per-column null pattern) case against the
   * reference evaluator. With `forceMasked` a null-free column reports one null over a
   * full-set bitmap, which sends the batch down `runMasked` - the dispatcher tests only
   * `nullCount != 0` - so the masked body is exercised on the same data the dense body serves.
   */
  private def checkMatrix(
      roots: Seq[VarkaVectorIR],
      numInputs: Int,
      lits: Array[Int],
      caseLengths: Seq[Int],
      patternCombos: Seq[Seq[Int => Boolean]],
      data: (Int, Int) => Int = defaultData,
      forceMasked: Boolean = false,
      ctx: String = "",
      options: VarkaEmitOptions = VarkaEmitOptions.DEFAULTS): Unit = {
    val (kernel, loader) = load(emitMulti(roots, numInputs, lits.length, options))
    try {
      for (length <- caseLengths; (combo, comboId) <- patternCombos.zipWithIndex) {
        val arena = Arena.ofConfined()
        try {
          val cols = (0 until numInputs).map { c =>
            makeInputData(arena, length, combo(c), i => data(c, i))
          }
          val outs = roots.map(_ => makeOutput(arena, length))
          val nullCounts = cols.map { col =>
            if (forceMasked && col.nullCount == 0) 1 else col.nullCount
          }
          val validityAddrs = cols.zip(nullCounts).map { case (col, nc) =>
            if (nc == 0 || nc == length) col.validityAddress(length) else col.validity.address()
          }
          // A Cond root is a selection output (task 21): its data address is 0L per the
          // kernel contract - exactly what the filter evaluator passes - so a regression
          // that touches it faults instead of writing somewhere silently.
          val dstData = roots.zip(outs).map { case (root, out) =>
            if (root.isInstanceOf[Cond]) 0L else out._1.address()
          }
          // The status is asserted, not discarded: a guard that declines every batch
          // leaves the destination values correct - the arithmetic does not depend on it -
          // so without this the matrix stays green while the kernel computes nothing in
          // production. Every shape this harness drives is one the kernel must answer.
          val status = kernel.run(cols.map(_.data.address()).toArray, validityAddrs.toArray,
            nullCounts.toArray, dstData.toArray,
            outs.map(_._2.address()).toArray, lits, length)
          assert(status === 0,
            s"$ctx: the kernel declined a batch it should have computed " +
              s"(length $length, combo $comboId, status $status)")
          for (i <- 0 until length) {
            val row = (0 until numInputs).map { c =>
              if (combo(c)(i)) None else Some(data(c, i))
            }
            for ((root, o) <- roots.zipWithIndex) {
              val bit = (outs(o)._2.get(ValueLayout.JAVA_BYTE, i / 8L) & (1 << (i % 8))) != 0
              val where = s"$ctx len=$length combo=$comboId out=$o row=$i"
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
        } finally {
          arena.close()
        }
      }
    } finally {
      loader.release()
    }
  }

  /** Every pair (or triple) of the four null patterns, as per-column combinations. */
  private def combos(numInputs: Int): Seq[Seq[Int => Boolean]] = {
    val ps = nullPatterns.map(_._2)
    if (numInputs == 2) for (a <- ps; b <- ps) yield Seq(a, b)
    else for (a <- ps; b <- ps; c <- ps) yield Seq(a, b, c)
  }

  test("the emitted class passes class-file verification before it is ever loaded") {
    val errors = VarkaEmitterTestSupport.verify(emit(addDays(0), 1)._2).asScala
    assert(errors.isEmpty, s"verifier errors: ${errors.mkString("; ")}")
  }

  test("a single AddDays matches vectorAddDays across lengths, null patterns and offsets") {
    val (kernel, loader) = load(emit(addDays(0), 1))
    try {
      for {
        length <- lengths
        (patternName, isNull) <- nullPatterns
        offset <- offsets
      } {
        val arena = Arena.ofConfined()
        try {
          val input = makeInput(arena, length, isNull)
          val expected = makeOutput(arena, length)
          val actual = makeOutput(arena, length)
          DateVectorOps.vectorAddDays(
            input.data.address(), input.validityAddress(length), input.nullCount,
            expected._1.address(), expected._2.address(), length, offset)
          kernel.run(
            Array(input.data.address()), Array(input.validityAddress(length)),
            Array(input.nullCount),
            Array(actual._1.address()), Array(actual._2.address()), Array(offset), length)
          assertSameOutput(length, expected, actual,
            s"length=$length pattern=$patternName offset=$offset")
        } finally {
          arena.close()
        }
      }
    } finally {
      loader.release()
    }
  }

  test("a chain of depth N matches N sequential kernel passes") {
    for (depth <- Seq(2, 3, 5, 8, 16)) {
      val chainOffsets = (0 until depth).map(level => level * 13 + 1).toArray
      val (kernel, loader) = load(emit(chain(depth), depth))
      try {
        val arena = Arena.ofConfined()
        try {
          val length = 1000
          val input = makeInput(arena, length, i => i % 7 == 0)
          val actual = makeOutput(arena, length)
          kernel.run(
            Array(input.data.address()), Array(input.validityAddress(length)),
            Array(input.nullCount),
            Array(actual._1.address()), Array(actual._2.address()), chainOffsets, length)

          // Oracle: the same chain as `depth` single-op kernel passes through temp buffers.
          var current = input
          for (level <- 0 until depth) {
            val out = makeOutput(arena, length)
            if (level % 2 == 0) {
              DateVectorOps.vectorAddDays(
                current.data.address(), current.validityAddress(length), current.nullCount,
                out._1.address(), out._2.address(), length, chainOffsets(level))
            } else {
              DateVectorOps.vectorSubDays(
                current.data.address(), current.validityAddress(length), current.nullCount,
                out._1.address(), out._2.address(), length, chainOffsets(level))
            }
            current = Col(out._1, out._2, current.nullCount)
          }
          assertSameOutput(length, (current.data, current.validity), actual, s"depth=$depth")
        } finally {
          arena.close()
        }
      } finally {
        loader.release()
      }
    }
  }

  test("DateDiff matches vectorDateDiff across lengths and per-column null patterns") {
    val root = new DateDiff(new ColumnRef(0), new ColumnRef(1))
    val (kernel, loader) = load(emitMulti(Seq(root), 2, 0))
    try {
      for {
        length <- lengths
        (endName, endNull) <- nullPatterns
        (startName, startNull) <- nullPatterns
      } {
        val arena = Arena.ofConfined()
        try {
          val end = makeInput(arena, length, endNull)
          val start = makeInput(arena, length, startNull)
          val expected = makeOutput(arena, length)
          val actual = makeOutput(arena, length)
          DateVectorOps.vectorDateDiff(
            end.data.address(), end.validityAddress(length), end.nullCount,
            start.data.address(), start.validityAddress(length), start.nullCount,
            expected._1.address(), expected._2.address(), length)
          kernel.run(
            Array(end.data.address(), start.data.address()),
            Array(end.validityAddress(length), start.validityAddress(length)),
            Array(end.nullCount, start.nullCount),
            Array(actual._1.address()), Array(actual._2.address()), Array.empty[Int], length)
          assertSameOutput(length, expected, actual,
            s"length=$length end=$endName start=$startName")
        } finally {
          arena.close()
        }
      }
    } finally {
      loader.release()
    }
  }

  test("two outputs sharing a subchain match sequential kernel passes, types independent") {
    // a = date_add(d, off); b = datediff(date_add(d, off), d2) - the milestone's DAG example:
    // the shared subchain is computed once per lane group and stored into both outputs' math.
    val shared = new AddDays(new ColumnRef(0), new LiteralSlot(0))
    val roots = Seq[VarkaVectorIR](shared, new DateDiff(shared, new ColumnRef(1)))
    val (kernel, loader) = load(emitMulti(roots, 2, 1))
    try {
      for ((patternName, isNull) <- nullPatterns) {
        val arena = Arena.ofConfined()
        try {
          val length = 1000
          val offset = 11
          val d = makeInput(arena, length, isNull)
          val d2 = makeInput(arena, length, i => i % 3 == 0)
          val actualA = makeOutput(arena, length)
          val actualB = makeOutput(arena, length)
          kernel.run(
            Array(d.data.address(), d2.data.address()),
            Array(d.validityAddress(length), d2.validityAddress(length)),
            Array(d.nullCount, d2.nullCount),
            Array(actualA._1.address(), actualB._1.address()),
            Array(actualA._2.address(), actualB._2.address()),
            Array(offset), length)

          // Oracle: the same DAG as two hand-written kernel passes through a temp buffer.
          val expectedA = makeOutput(arena, length)
          val expectedB = makeOutput(arena, length)
          DateVectorOps.vectorAddDays(
            d.data.address(), d.validityAddress(length), d.nullCount,
            expectedA._1.address(), expectedA._2.address(), length, offset)
          DateVectorOps.vectorDateDiff(
            expectedA._1.address(), if (d.nullCount == 0) 0L else expectedA._2.address(),
            d.nullCount,
            d2.data.address(), d2.validityAddress(length), d2.nullCount,
            expectedB._1.address(), expectedB._2.address(), length)
          assertSameOutput(length, expectedA, actualA, s"pattern=$patternName output a")
          assertSameOutput(length, expectedB, actualB, s"pattern=$patternName output b")
        } finally {
          arena.close()
        }
      }
    } finally {
      loader.release()
    }
  }

  test("an all-null input kills only the outputs that read it") {
    // a reads column 0 only; b reads both. With column 1 all-null, a is served and b reads
    // back all-null - through the mask algebra alone, with no dedicated dead-output code.
    val roots = Seq[VarkaVectorIR](
      new AddDays(new ColumnRef(0), new LiteralSlot(0)),
      new DateDiff(new ColumnRef(0), new ColumnRef(1)))
    val (kernel, loader) = load(emitMulti(roots, 2, 1))
    try {
      val arena = Arena.ofConfined()
      try {
        val length = 1000
        val offset = 5
        val d = makeInput(arena, length, i => i % 5 == 0)
        val allNull = makeInput(arena, length, _ => true)
        val actualA = makeOutput(arena, length)
        val actualB = makeOutput(arena, length)
        kernel.run(
          Array(d.data.address(), allNull.data.address()),
          Array(d.validityAddress(length), allNull.validityAddress(length)),
          Array(d.nullCount, allNull.nullCount),
          Array(actualA._1.address(), actualB._1.address()),
          Array(actualA._2.address(), actualB._2.address()),
          Array(offset), length)
        val expectedA = makeOutput(arena, length)
        DateVectorOps.vectorAddDays(
          d.data.address(), d.validityAddress(length), d.nullCount,
          expectedA._1.address(), expectedA._2.address(), length, offset)
        assertSameOutput(length, expectedA, actualA, "the live output")
        for (b <- 0L until (length + 7) / 8L) {
          assert(actualB._2.get(ValueLayout.JAVA_BYTE, b) === 0.toByte,
            s"dead output validity byte $b not zero")
        }

        // Both inputs all-null: the generalized all-null shortcut returns early, and both
        // outputs must still read as all-null (their validity was pre-filled with stale bits).
        val actualC = makeOutput(arena, length)
        val actualD = makeOutput(arena, length)
        kernel.run(
          Array(allNull.data.address(), allNull.data.address()),
          Array(0L, 0L), Array(length, length),
          Array(actualC._1.address(), actualD._1.address()),
          Array(actualC._2.address(), actualD._2.address()),
          Array(offset), length)
        for (b <- 0L until (length + 7) / 8L) {
          assert(actualC._2.get(ValueLayout.JAVA_BYTE, b) === 0.toByte)
          assert(actualD._2.get(ValueLayout.JAVA_BYTE, b) === 0.toByte)
        }
      } finally {
        arena.close()
      }
    } finally {
      loader.release()
    }
  }

  test("disabling CSE changes the bytecode but never the results") {
    val shared = new AddDays(new ColumnRef(0), new LiteralSlot(0))
    val roots = Seq[VarkaVectorIR](shared, new DateDiff(shared, new ColumnRef(1)))
    val withCse = emitMulti(roots, 2, 1)
    val withoutCse = emitMulti(roots, 2, 1, VarkaEmitOptions.DEFAULTS.withCse(false))
    assert(!java.util.Arrays.equals(withCse._2, withoutCse._2),
      "disabling the memo left the bytecode unchanged - CSE was not exercised")
    val (kernelCse, loaderCse) = load(withCse)
    val (kernelNoCse, loaderNoCse) = load(withoutCse)
    try {
      val arena = Arena.ofConfined()
      try {
        val length = 1000
        val d = makeInput(arena, length, i => i % 5 == 0)
        val d2 = makeInput(arena, length, i => i % 3 == 0)
        def run(kernel: VarkaFusedKernel): ((MemorySegment, MemorySegment),
            (MemorySegment, MemorySegment)) = {
          val a = makeOutput(arena, length)
          val b = makeOutput(arena, length)
          kernel.run(
            Array(d.data.address(), d2.data.address()),
            Array(d.validityAddress(length), d2.validityAddress(length)),
            Array(d.nullCount, d2.nullCount),
            Array(a._1.address(), b._1.address()),
            Array(a._2.address(), b._2.address()),
            Array(7), length)
          (a, b)
        }
        val (cseA, cseB) = run(kernelCse)
        val (plainA, plainB) = run(kernelNoCse)
        assertSameOutput(length, plainA, cseA, "output a")
        assertSameOutput(length, plainB, cseB, "output b")
      } finally {
        arena.close()
      }
    } finally {
      loaderCse.release()
      loaderNoCse.release()
    }
  }

  test("IfElse over every comparison matches the reference across per-column null patterns") {
    for (op <- CompareOp.values.toSeq) {
      val root = new IfElse(new Compare(op, new ColumnRef(0), new ColumnRef(1)),
        new AddDays(new ColumnRef(0), new LiteralSlot(0)),
        new SubDays(new ColumnRef(1), new LiteralSlot(1)))
      checkMatrix(Seq(root), 2, Array(7, 3), Seq(0, 1, 5, 17, 64, 65, 1000), combos(2),
        ctx = s"op=$op")
    }
  }

  test("three-valued connectives: unknowns propagate by Kleene's rules") {
    // NOT(a < b) OR (a = c AND b <= c): known-false must survive NOT, an unknown falls
    // through to ELSE, and the reference evaluator implements Kleene logic independently.
    val a = new ColumnRef(0)
    val b = new ColumnRef(1)
    val c = new ColumnRef(2)
    val cond = new Or(
      new Not(new Compare(CompareOp.LT, a, b)),
      new And(new Compare(CompareOp.EQ, a, c), new Compare(CompareOp.LE, b, c)))
    val root = new IfElse(cond, new AddDays(a, new LiteralSlot(0)), b)
    checkMatrix(Seq(root), 3, Array(11), Seq(17, 64, 1000), combos(3), ctx = "kleene")
  }

  test("task 20: coalesce lowers to IfElse over IsNotNull and matches the reference") {
    val a = new ColumnRef(0)
    val b = new ColumnRef(1)
    val c = new ColumnRef(2)
    // coalesce(a, b) and coalesce(a, b, c) exactly as the compiler lowers them, plus a
    // computed last operand - only the guarded operands are restricted to columns.
    val roots = Seq[VarkaVectorIR](
      new IfElse(new IsNotNull(a), a, b),
      new IfElse(new IsNotNull(a), a, new IfElse(new IsNotNull(b), b, c)),
      new IfElse(new IsNotNull(a), a, new AddDays(b, new LiteralSlot(0))))
    checkMatrix(roots, 3, Array(9), Seq(1, 17, 64, 65, 1000), combos(3), ctx = "coalesce")
  }

  test("task 20: a validity predicate among the connectives keeps Kleene's rules") {
    // IsNotNull is the first *total* condition - never unknown - and the pair algebra must
    // absorb it unchanged: AND/OR against an unknown comparison, and IS NULL as NOT over it
    // (a slot swap in the masked body).
    val a = new ColumnRef(0)
    val b = new ColumnRef(1)
    val cond = new Or(
      new And(new IsNotNull(a), new Compare(CompareOp.LT, a, b)),
      new Not(new IsNotNull(b)))
    val root = new IfElse(cond, new Greatest(a, b), new SubDays(b, new LiteralSlot(0)))
    checkMatrix(Seq(root), 2, Array(5), Seq(1, 17, 64, 65, 1000), combos(2), ctx = "validity")
    // Dense/masked agreement on null-free data, where the predicate is constant true.
    val nullFree = Seq(Seq[Int => Boolean](_ => false, _ => false))
    checkMatrix(Seq(root), 2, Array(5), Seq(17, 65), nullFree, forceMasked = true,
      ctx = "validity-forced-masked")
  }

  test("task 20: IsNotNull over a computed operand is rejected at analysis") {
    // The compiler already declines this shape; the emitter re-checks because its emission
    // reads the child's per-input validity word, which only a column has before value walks.
    val bad = new IfElse(new IsNotNull(new AddDays(new ColumnRef(0), new LiteralSlot(0))),
      new ColumnRef(0), new ColumnRef(0))
    val e = intercept[IllegalArgumentException](emitMulti(Seq(bad), 1, 1))
    assert(e.getMessage.contains("IsNotNull child must be a ColumnRef"))
  }

  test("task 20: fitsBudgets mirrors the analysis caps, distinct ops across outputs") {
    def chain(base: Int, depth: Int): VarkaVectorIR =
      (0 until depth).foldLeft[VarkaVectorIR](new ColumnRef(base)) { (n, _) =>
        new AddDays(n, new LiteralSlot(0))
      }
    assert(VarkaLoopEmitter.fitsBudgets(java.util.List.of[VarkaVectorIR](chain(0, 16)), 1))
    assert(!VarkaLoopEmitter.fitsBudgets(java.util.List.of[VarkaVectorIR](chain(0, 17)), 1))
    // Five disjoint depth-13 chains are 65 distinct ops - the same shape the emitter's own
    // rejection test uses against MAX_FUSED_NODES.
    val five: Seq[VarkaVectorIR] = (0 until 5).map(k => chain(k, 13))
    assert(!VarkaLoopEmitter.fitsBudgets(java.util.List.of[VarkaVectorIR](five: _*), 5))
    // A shared subtree is one node, exactly as Analysis counts it.
    val shared = chain(0, 13)
    val sharedFive: Seq[VarkaVectorIR] = Seq.fill(5)(shared)
    assert(VarkaLoopEmitter.fitsBudgets(java.util.List.of[VarkaVectorIR](sharedFive: _*), 1))
    // The input-column cap is mirrored too (the review found it missing): the emitter's
    // emit() rejects numInputs > 64, so the compiler must never accept such a projection.
    val one = java.util.List.of[VarkaVectorIR](chain(0, 1))
    assert(VarkaLoopEmitter.fitsBudgets(one, 64))
    assert(!VarkaLoopEmitter.fitsBudgets(one, 65))
  }

  test("greatest and least skip nulls, nested to the n-ary fold shape") {
    val g2 = new Greatest(new ColumnRef(0), new ColumnRef(1))
    val roots = Seq[VarkaVectorIR](
      new Greatest(g2, new ColumnRef(2)),
      new Least(new Least(new ColumnRef(0), new ColumnRef(1)), new ColumnRef(2)),
      // The milestone's irreducible chain: greatest over a nested arithmetic chain.
      new Greatest(new AddDays(new ColumnRef(0), new LiteralSlot(0)), new ColumnRef(2)))
    checkMatrix(roots, 3, Array(7), Seq(1, 17, 64, 65, 1000), combos(3), ctx = "pick")
  }

  test("dayofweek and weekday match floorMod and LocalDate across extreme and negative days") {
    val roots = Seq[VarkaVectorIR](
      new DayOfWeek(new ColumnRef(0)), new WeekDay(new ColumnRef(0)))
    // The 15-bit fold boundaries are edges of the shipped magic-multiply lowering.
    val extremes = Array(Int.MinValue, Int.MaxValue, Int.MinValue + 1, Int.MaxValue - 1,
      -1, 0, 1, -7, 7, -8, 8, Int.MaxValue - 3, Int.MinValue + 3,
      32767, 32768, -32768, -32769)
    def days(c: Int, i: Int): Int =
      if (i < extremes.length) extremes(i) else i * 997 - 300000
    checkMatrix(roots, 1, Array.empty[Int], Seq(1, 13, 17, 64, 1000),
      nullPatterns.map(p => Seq(p._2)), data = days, ctx = "dow")
    // The independent oracle behind the reference: Spark's DateTimeUtils formula through
    // LocalDate, valid for every int epoch day.
    for (v <- extremes) {
      val viaLocalDate = java.time.LocalDate.ofEpochDay(v).getDayOfWeek.plus(1).getValue
      assert((Math.floorMod(v, 7) + 4) % 7 + 1 === viaLocalDate, s"oracle self-check v=$v")
    }
  }

  test("next_day matches Spark's own wrapping formula for every weekday, at the extremes") {
    // One root per weekday offset (k = dayOfWeek - 1). DateTimeUtils.getDayOfWeekFromString
    // returns [0, 6] with THURSDAY = 0 .. WEDNESDAY = 6, so k itself ranges over [-1, 5], not
    // [0, 6] - THURSDAY's k = -1 is the one value a naive 0-to-6 sweep would miss (caught by
    // this task's code review). All seven share one emitted class and one literal-slot array
    // - the point of "k is a runtime literal" (section 2).
    val roots = (0 to 6).map(slot => new NextDay(new ColumnRef(0), new LiteralSlot(slot)))
    val lits = Array(-1, 0, 1, 2, 3, 4, 5)
    // The 15-bit fold boundaries are edges of the shared floorMod7 lowering; the rest probe
    // the deliberate k - d overflow (section 2) near both ends of the int range.
    val extremes = Array(Int.MinValue, Int.MaxValue, Int.MinValue + 1, Int.MaxValue - 1,
      -1, 0, 1, -7, 7, -8, 8, Int.MaxValue - 3, Int.MinValue + 3,
      32767, 32768, -32768, -32769)
    def days(c: Int, i: Int): Int =
      if (i < extremes.length) extremes(i) else i * 997 - 300000
    checkMatrix(roots, 1, lits, Seq(1, 13, 17, 64, 1000),
      nullPatterns.map(p => Seq(p._2)), data = days, ctx = "next_day")
    // The independent oracle behind the reference: Spark's own getNextDateForDayOfWeek,
    // which wraps in plain int arithmetic - checked against the reduce-first form the recipe
    // warns is wrong, to confirm the two really do disagree at the boundary it names.
    def spark(startDay: Int, dayOfWeek: Int): Int =
      startDay + 1 + ((dayOfWeek - 1 - startDay) % 7 + 7) % 7
    def reduceFirst(startDay: Int, k: Int): Int =
      startDay + 1 + Math.floorMod(k - Math.floorMod(startDay, 7), 7)
    assert(spark(Int.MinValue, 3) === -2147483647, "oracle self-check")
    assert(reduceFirst(Int.MinValue, 2) === -2147483643, "reduce-first disagrees as documented")
    assert(spark(Int.MinValue, 3) !== reduceFirst(Int.MinValue, 2))
  }

  test("the calendar extractions match LocalDate over the range they cover") {
    val roots = Seq[VarkaVectorIR](
      new Year(new ColumnRef(0)), new Month(new ColumnRef(0)),
      new DayOfMonth(new ColumnRef(0)), new Quarter(new ColumnRef(0)),
      new DayOfYear(new ColumnRef(0)))
    val inRange = Array(
      VarkaChrono.NARROW_MIN_DAYS, VarkaChrono.NARROW_MIN_DAYS + 1,
      VarkaChrono.NARROW_MAX_DAYS, VarkaChrono.NARROW_MAX_DAYS - 1,
      -1, 0, 1, -719468, -719162,
      LocalDate.of(1600, 2, 29).toEpochDay.toInt, LocalDate.of(1900, 3, 1).toEpochDay.toInt,
      LocalDate.of(2000, 2, 29).toEpochDay.toInt, LocalDate.of(1, 1, 1).toEpochDay.toInt,
      LocalDate.of(9999, 12, 31).toEpochDay.toInt
    ) ++ Array(
      // dayofyear's own boundary set (task 34): every year-end/year-start pair a leap flag
      // could get wrong, plus February's own boundary in a leap and a century-non-leap year.
      LocalDate.of(2000, 1, 1), LocalDate.of(2000, 12, 31), // leap
      LocalDate.of(2024, 1, 1), LocalDate.of(2024, 12, 31), // leap
      LocalDate.of(2023, 1, 1), LocalDate.of(2023, 12, 31), // common
      LocalDate.of(1900, 1, 1), LocalDate.of(1900, 12, 31), // century, not leap
      LocalDate.of(2000, 2, 28), LocalDate.of(1900, 2, 28)
    ).map(_.toEpochDay.toInt)
    def days(c: Int, i: Int): Int =
      if (i < inRange.length) inRange(i) else i * 9973 - 400000
    checkMatrix(roots, 1, Array.empty[Int], Seq(1, 13, 17, 64, 1000),
      nullPatterns.map(p => Seq(p._2)), data = days, ctx = "narrowed")
  }

  test("the emitted kernel agrees with VarkaChrono's scalar twin, not only with LocalDate") {
    // Every other test in this file checks the emitted kernel against LocalDate and
    // VarkaChronoSuite checks VarkaChrono against LocalDate separately - each a genuine
    // definition-level oracle, deliberately not each other (see evalValue's comment above).
    // That leaves a gap this test closes: nothing committed (the direct comparison only runs
    // opt-in, in the exhaustive sweep below) ever compares the emitted bytecode against
    // VarkaChrono directly, so a future edit that moved both the same wrong way could agree
    // with LocalDate on every curated/pseudo-random day above and still have silently
    // diverged from VarkaChrono - contradicting VarkaChrono's own class-doc promise that "any
    // disagreement with the emitted kernel is an emission bug". A committed, non-exhaustive
    // sample is enough to catch that: it does not need to be exhaustive, since the exhaustive
    // sweep already exists for the LocalDate side and opting into it is what full coverage
    // means here.
    val roots = Seq[VarkaVectorIR](
      new Year(new ColumnRef(0)), new Month(new ColumnRef(0)),
      new DayOfMonth(new ColumnRef(0)), new Quarter(new ColumnRef(0)),
      new DayOfYear(new ColumnRef(0)))
    val days = Array(
      VarkaChrono.NARROW_MIN_DAYS, VarkaChrono.NARROW_MIN_DAYS + 1,
      VarkaChrono.NARROW_MAX_DAYS, VarkaChrono.NARROW_MAX_DAYS - 1, -1, 0, 1,
      LocalDate.of(1600, 2, 29).toEpochDay.toInt, LocalDate.of(1900, 3, 1).toEpochDay.toInt,
      LocalDate.of(2000, 2, 29).toEpochDay.toInt
    ) ++ Array.tabulate(2000)(i => i * 9973 - 400000)
      .filter(VarkaChrono.inNarrowRange)
    val (kernel, loader) = load(emitMulti(roots, 1, 0))
    try {
      val arena = Arena.ofConfined()
      try {
        val data = alloc(arena, days.length * 4L)
        val validity = alloc(arena, (days.length + 7) / 8L)
        validity.fill(0xFF.toByte)
        days.zipWithIndex.foreach { case (d, i) => data.set(ValueLayout.JAVA_INT, i * 4L, d) }
        val outs = roots.map(_ => makeOutput(arena, days.length))
        val status = kernel.run(Array(data.address()), Array(validity.address()), Array(0),
          outs.map(_._1.address()).toArray, outs.map(_._2.address()).toArray,
          Array.empty[Int], days.length)
        assert(status === 0, "the kernel declined an in-range batch")
        days.indices.foreach { i =>
          val fields = VarkaChrono.narrowed(days(i))
          val want = Seq(fields.year, fields.month, fields.dayOfMonth, fields.quarter,
            fields.dayOfYear)
          val got = outs.map(_._1.get(ValueLayout.JAVA_INT, i * 4L))
          assert(got === want, s"day ${days(i)}: emitted $got, VarkaChrono $want")
        }
      } finally {
        arena.close()
      }
    } finally {
      loader.release()
    }
  }

  test("task 40: add_months matches DateTimeUtils across clamp boundaries and month offsets") {
    val root = new AddMonths(new ColumnRef(0), new LiteralSlot(0))
    // Every one of these has a different day-of-month than the month it lands in, at both
    // ends of the year and across a common/leap February - the clamp is where a wrong
    // implementation fails, per PLAN_TASK_40.md section 4.
    val clampDays = Array(
      LocalDate.of(2023, 1, 31).toEpochDay.toInt, LocalDate.of(2023, 3, 31).toEpochDay.toInt,
      LocalDate.of(2020, 2, 29).toEpochDay.toInt, LocalDate.of(2024, 2, 28).toEpochDay.toInt,
      LocalDate.of(1900, 1, 31).toEpochDay.toInt, LocalDate.of(2000, 1, 31).toEpochDay.toInt,
      LocalDate.of(2023, 12, 31).toEpochDay.toInt, 0, -1, 1,
      // A four-digit year plus a multi-century month offset overflows the 32-bit lane
      // multiply behind the /400 and /100 magic (VarkaChrono.YEAR_CENTURY_M's javadoc) -
      // the exact shape that found the bug during development. Near-epoch dates alone do
      // not reach it.
      VarkaChrono.NARROW_MIN_DAYS, VarkaChrono.NARROW_MIN_DAYS + 1,
      VarkaChrono.NARROW_MAX_DAYS, VarkaChrono.NARROW_MAX_DAYS - 1, 3818579, 3811279)
    def days(c: Int, i: Int): Int =
      if (i < clampDays.length) clampDays(i) else i * 9973 - 400000
    // Offsets of 0, +-1, +-11, +-12, +-13, +-1200 cross a multiple of 12 both ways, which is
    // where the month-arithmetic dividend's own bias could be off by one.
    for (offset <- Seq(0, 1, -1, 11, -11, 12, -12, 13, -13, 1200, -1200,
        VarkaChrono.MONTH_ARITH_MAX_MONTHS, VarkaChrono.MONTH_ARITH_MIN_MONTHS)) {
      checkMatrix(Seq(root), 1, Array(offset), Seq(1, 13, 17, 64, 1000),
        nullPatterns.map(p => Seq(p._2)), data = days, ctx = s"add_months offset=$offset")
    }
  }

  test("a chained calendar computation matches across every lane-group tail length") {
    // Historically an epilogue-mask/guard interaction bug: a masked load fills the lanes past
    // `length` with 0, and the now-removed guard ran on the node's *input*, which here is a
    // computed value (0 - 5400000, well outside the guard's range) - so an unmasked check
    // declined every batch whose length was not a lane multiple, even though every real row,
    // near 2022, was in range. Task 51 removed the guard entirely; this case is kept as a
    // general correctness check on a chained node across non-lane-multiple lengths.
    val root = new Year(new SubDays(new ColumnRef(0), new LiteralSlot(0)))
    def days(c: Int, i: Int): Int = 19000 + i
    checkMatrix(Seq(root), 1, Array(5400000), Seq(16, 17, 31, 64, 1000, 4095, 4096),
      nullPatterns.map(p => Seq(p._2)), data = days, ctx = "epilogue-guard")
  }

  test("the emitted calendar kernel matches LocalDate over its whole range " +
      "(opt-in: -Dvarka.sweep=true)") {
    // VarkaChrono's own suite sweeps the scalar model over all 16,777,216 days, and the
    // emitter loads the same constants - but it re-expresses the algorithm as bytecode, with
    // its own op order, carry steps and mask polarity. Only this sweep holds the *emitted*
    // form to the same standard; without it the class doc's "cannot drift" covers the
    // constants and not the code, and a transposed slot would survive every other test.
    assume(System.getProperty("varka.sweep") == "true",
      "set -Dvarka.sweep=true to sweep the emitted kernel")
    val roots = Seq[VarkaVectorIR](
      new Year(new ColumnRef(0)), new Month(new ColumnRef(0)),
      new DayOfMonth(new ColumnRef(0)), new Quarter(new ColumnRef(0)),
      new DayOfYear(new ColumnRef(0)))
    // Both lowerings, because task 32 step B's shared prefix re-orders nothing but does make
    // four of these five outputs read locals a fifth wrote. A transposed slot there would
    // survive every bounded test in this suite and fail here.
    for (options <- Seq(unshared, sharing)) {
      sweepCalendar(roots, options)
    }
  }

  private def sweepCalendar(roots: Seq[VarkaVectorIR], options: VarkaEmitOptions): Unit = {
    val (kernel, loader) = load(emitMulti(roots, 1, 0, options))
    try {
      val arena = Arena.ofConfined()
      try {
        val chunk = 1 << 16
        val data = alloc(arena, chunk * 4L)
        val validity = alloc(arena, (chunk + 7) / 8L)
        validity.fill(0xFF.toByte)
        val outs = roots.map(_ => makeOutput(arena, chunk))
        var day = VarkaChrono.NARROW_MIN_DAYS
        var mismatches = 0
        while (day <= VarkaChrono.NARROW_MAX_DAYS) {
          val n = math.min(chunk, VarkaChrono.NARROW_MAX_DAYS - day + 1)
          var i = 0
          while (i < n) {
            data.set(ValueLayout.JAVA_INT, i * 4L, day + i)
            i += 1
          }
          val status = kernel.run(Array(data.address()), Array(validity.address()), Array(0),
            outs.map(_._1.address()).toArray, outs.map(_._2.address()).toArray,
            Array.empty[Int], n)
          assert(status === 0, s"the kernel declined an in-range batch at day $day")
          i = 0
          while (i < n) {
            val date = LocalDate.ofEpochDay((day + i).toLong)
            val got = outs.map(_._1.get(ValueLayout.JAVA_INT, i * 4L))
            val want = Seq(date.getYear, date.getMonthValue, date.getDayOfMonth,
              date.get(IsoFields.QUARTER_OF_YEAR), date.getDayOfYear)
            if (got != want) {
              mismatches += 1
              if (mismatches < 4) {
                fail(s"day ${day + i} ($date), shared=${options.shareChronoPrefix()}: " +
                  s"emitted $got, LocalDate $want")
              }
            }
            i += 1
          }
          day += n
        }
        assert(mismatches === 0, s"the emitted kernel disagreed on $mismatches days, " +
          s"shared=${options.shareChronoPrefix()}")
      } finally {
        arena.close()
      }
    } finally {
      loader.release()
    }
  }

  test("a day outside the covered range is no longer declined (task 51)") {
    // Tasks 26 through 40 guarded every calendar extraction against a day outside
    // VarkaChrono.NARROW_MIN_DAYS..NARROW_MAX_DAYS, declining the whole batch to the row
    // engine. Task 51 removed that guard: the arithmetic is still only proven exact inside
    // the narrowed range (VarkaChronoSuite's exhaustive sweep is over exactly that range), but
    // nothing checks it at run time anymore, so a day outside it is now computed silently
    // rather than declined. PLAN_TASK_51.md records why the owner accepted that trade, and
    // PLAN_TASK_52.md tracks moving the check to the nodes that can actually manufacture such
    // a day - unbounded runtime arithmetic, not a bare column.
    val root = new Year(new ColumnRef(0))
    val (kernel, loader) = load(emitMulti(Seq(root), 1, 0, VarkaEmitOptions.DEFAULTS))
    try {
      val arena = Arena.ofConfined()
      try {
        val length = 64
        // One day past the range, in a lane the vector loop covers.
        val bad = makeInputData(arena, length, _ => false,
          i => if (i == 3) VarkaChrono.NARROW_MAX_DAYS + 1 else i * 97)
        val out = makeOutput(arena, length)
        assert(runKernel(kernel, bad, out, length) === 0)
        // And in a lane only the epilogue covers, whatever the host's lane count.
        val tail = makeInputData(arena, 17, _ => false,
          i => if (i == 16) VarkaChrono.NARROW_MIN_DAYS - 1 else i * 97)
        val tailOut = makeOutput(arena, 17)
        assert(runKernel(kernel, tail, tailOut, 17) === 0)
      } finally {
        arena.close()
      }
    } finally {
      loader.release()
    }
  }

  test("each calendar output gets its own loop method, whatever GROUP_BUDGET would say") {
    // Four calendar outputs weigh far more than GROUP_BUDGET, so they must not share a loop
    // method: one method of ~180 vector ops is the C2 compile cliff the budget exists for.
    val roots = Seq[VarkaVectorIR](
      new Year(new ColumnRef(0)), new Month(new ColumnRef(0)),
      new DayOfMonth(new ColumnRef(0)), new Quarter(new ColumnRef(0)))
    val names = methodNames(emitMulti(roots, 1, 0, VarkaEmitOptions.DEFAULTS))
    assert(names.count(_.startsWith("loopDense")) === 4,
      s"expected one dense loop method per calendar output, got ${names.mkString(", ")}")
    // A plain chain is unaffected: the weight applies to calendar nodes only.
    val plain = Seq[VarkaVectorIR](
      new AddDays(new ColumnRef(0), new LiteralSlot(0)),
      new SubDays(new ColumnRef(0), new LiteralSlot(0)))
    assert(methodNames(emitMulti(plain, 1, 1, VarkaEmitOptions.DEFAULTS))
      .count(_.startsWith("loopDense")) === 1)
  }

  // -------------------------------------------------------------------------------------------
  // Task 32 step B: sharing the civil-from-days prefix between calendar nodes over one date.
  // -------------------------------------------------------------------------------------------

  /** The days the calendar differentials drive: the range's edges, then a strided walk. */
  private def calendarDays(c: Int, i: Int): Int = {
    val edges = Array(
      VarkaChrono.NARROW_MIN_DAYS, VarkaChrono.NARROW_MIN_DAYS + 1,
      VarkaChrono.NARROW_MAX_DAYS, VarkaChrono.NARROW_MAX_DAYS - 1,
      -1, 0, 1, -719468,
      LocalDate.of(1600, 2, 29).toEpochDay.toInt, LocalDate.of(1900, 3, 1).toEpochDay.toInt,
      LocalDate.of(2000, 2, 29).toEpochDay.toInt, LocalDate.of(2023, 12, 31).toEpochDay.toInt)
    if (i < edges.length) edges(i) else i * 9973 - 400000
  }

  // Lengths deliberately chosen odd or prime: a lane count divides 64 and 1000 but none of
  // these, so every case leaves a remainder and so exercises the epilogue - which under
  // today's grouping is the only body that holds two calendar outputs at once, and therefore
  // the only body where sharing does anything at all.
  private val remainderLengths = Seq(1, 13, 17, 63, 1001)

  private val sharing = VarkaEmitOptions.DEFAULTS.withShareChronoPrefix(true)
  private val unshared = VarkaEmitOptions.DEFAULTS.withShareChronoPrefix(false)

  /** The masked epilogue's bytecode size - the one method every output shares (task 24). */
  private def epilogueSize(
      roots: Seq[VarkaVectorIR], numInputs: Int, options: VarkaEmitOptions): Int =
    VarkaEmitterTestSupport.codeSize(
      emitMulti(roots, numInputs, 0, options)._2, "epilogueMasked")

  test("sharing the calendar prefix changes the bytecode but never the results") {
    val roots = Seq[VarkaVectorIR](
      new Year(new ColumnRef(0)), new Month(new ColumnRef(0)),
      new DayOfMonth(new ColumnRef(0)), new Quarter(new ColumnRef(0)))
    assert(VarkaEmitOptions.DEFAULTS.shareChronoPrefix(),
      "the shared prefix is no longer the default - the epilogue-size case for it is in " +
        "PLAN_TASK_32.md section 7.1, so say why here if it was deliberately turned off")
    assert(epilogueSize(roots, 1, sharing) < epilogueSize(roots, 1, unshared),
      "the shared epilogue is no smaller, so the prefix is still being emitted four times")
    // Both settings over the same matrix and the same java.time oracle. Running the unshared
    // one here too is what makes this a differential rather than a second correctness test:
    // a harness case that the shared lowering fails and the unshared one also fails is a
    // problem with the case, and this says so in the same run.
    for ((options, ctx) <- Seq((unshared, "unshared"), (sharing, "shared"))) {
      checkMatrix(roots, 1, Array.empty[Int], remainderLengths,
        nullPatterns.map(p => Seq(p._2)), data = calendarDays, ctx = s"$ctx prefix",
        options = options)
      // forceMasked reports one null, so a length of 1 would report the column all-null and
      // take the kernel's all-null shortcut instead of the masked body this is here to drive.
      checkMatrix(roots, 1, Array.empty[Int], remainderLengths.filter(_ > 1),
        nullPatterns.map(p => Seq(p._2)), data = calendarDays, forceMasked = true,
        ctx = s"$ctx prefix, masked", options = options)
    }
  }

  test("a shared prefix serves add_months and a plain extraction over the same date") {
    // add_months writes the prefix's carry mask as its own scratch after the prefix is done
    // (emitChronoPrefix's javadoc says why that is sound). Ordering it *before* the three
    // extractions is what would catch it if it were not: they read the shared slots after it
    // has finished with them.
    val col = new ColumnRef(0)
    val roots = Seq[VarkaVectorIR](
      new AddMonths(col, new LiteralSlot(0)), new Year(col), new Month(col), new DayOfMonth(col))
    for (offset <- Seq(0, 1, -13, VarkaChrono.MONTH_ARITH_MAX_MONTHS)) {
      checkMatrix(roots, 1, Array(offset), remainderLengths,
        nullPatterns.map(p => Seq(p._2)), data = calendarDays,
        ctx = s"shared with add_months offset=$offset", options = sharing)
    }
  }

  test("the guard's removal reaches the shared prefix too (task 51)") {
    // This PR predates task 51 and originally asserted the opposite: that the guard, sharing
    // the prefix across the three outputs below, still fired and declined the batch. Task 51
    // removed the guard from emitEra, which emitChronoPrefixOnce - the fragment-sharing entry
    // point this PR added - calls exactly like the unshared path does. That is why removal
    // needed no change here: there was never a second, sharing-specific copy of the guard to
    // find and delete. This test now exists to keep it that way - if a future change gives
    // the shared path its own inlined guard logic instead of routing through emitEra, this is
    // where that would first show up as a mistaken STATUS_CHRONO_RANGE.
    val col = new ColumnRef(0)
    val roots = Seq[VarkaVectorIR](new Year(col), new Month(col), new Quarter(col))
    val (kernel, loader) = load(emitMulti(roots, 1, 0, sharing))
    try {
      val arena = Arena.ofConfined()
      try {
        def status(length: Int, isNull: Int => Boolean, day: Int => Int): Int = {
          val in = makeInputData(arena, length, isNull, day)
          val outs = roots.map(_ => makeOutput(arena, length))
          kernel.run(
            Array(in.data.address()), Array(in.validity.address()), Array(in.nullCount),
            outs.map(_._1.address()).toArray, outs.map(_._2.address()).toArray,
            Array.empty[Int], length)
        }
        assert(status(64, _ => false, i => i * 97) === 0, "an in-range batch was declined")
        assert(status(64, _ => false, i => if (i == 3) VarkaChrono.NARROW_MAX_DAYS + 1
          else i * 97) === 0, "a day past the range was declined through the shared prefix")
        assert(status(17, _ => false, i => if (i == 16) VarkaChrono.NARROW_MIN_DAYS - 1
          else i * 97) === 0,
          "a day past the range was declined in the epilogue, where sharing happens today")
        assert(status(64, i => i == 3, i => if (i == 3) VarkaChrono.NARROW_MAX_DAYS + 1
          else i * 97) === 0, "an out-of-range value under a null row condemned the batch")
      } finally {
        arena.close()
      }
    } finally {
      loader.release()
    }
  }

  test("the shared prefix survives two calendar outputs in one loop method") {
    // Today's GROUP_BUDGET puts every calendar output in its own loop method, so only the
    // epilogue ever holds two - which means nothing in the default configuration exercises
    // sharing inside a loop body. A budget wide enough to hold all four does, and that is
    // the shape step B2 would ship, measured here for correctness before it is measured for
    // throughput.
    val col = new ColumnRef(0)
    val roots = Seq[VarkaVectorIR](
      new Year(col), new Month(col), new DayOfMonth(col), new Quarter(col))
    val wide = sharing.withGroupBudget(200)
    assert(methodNames(emitMulti(roots, 1, 0, wide)).count(_.startsWith("loopDense")) === 1,
      "the wide budget did not put the four outputs in one loop method")
    checkMatrix(roots, 1, Array.empty[Int], remainderLengths ++ Seq(64, 1000),
      nullPatterns.map(p => Seq(p._2)), data = calendarDays, ctx = "one wide loop method",
      options = wide)
  }

  test("two calendar outputs over different dates share nothing") {
    // The fragment is keyed on the child, so year(d1) and year(d2) must each emit their own
    // prefix. A key that collapsed to the node type would silently answer d2 from d1's
    // decomposition - right-looking numbers, wrong rows, and no status to say so.
    val roots = Seq[VarkaVectorIR](new Year(new ColumnRef(0)), new Year(new ColumnRef(1)))
    // Not the whole class: emitMulti gives every emission a fresh name, so the constant pool
    // differs whatever the body does. The epilogue is where two outputs meet, so its size is
    // the thing that would have moved had the two prefixes collapsed into one.
    assert(epilogueSize(roots, 2, sharing) === epilogueSize(roots, 2, unshared),
      "the epilogue moved for two outputs that have nothing to share")
    checkMatrix(roots, 2, Array.empty[Int], remainderLengths,
      // The second date is the first walked from a different index rather than shifted by a
      // constant: adding to a day that is already at the range's edge would push it out and
      // make the kernel decline, which is a guard result, not a sharing one.
      nullPatterns.map(p => Seq(p._2, p._2)), data = (c, i) => calendarDays(c, i + c * 3),
      ctx = "two dates", options = sharing)
  }

  test("sharing the prefix leaves every loop method byte for byte as it was") {
    // Why no benchmark number moves, established by construction rather than by re-running a
    // noisy measurement. Today's GROUP_BUDGET gives every calendar output its own loop method,
    // so no loop body holds two chrono nodes and there is nothing in one for the fragment to
    // share; the epilogue is the only body that holds them all. The parity benchmark drives
    // 4096-row chunks, which every lane count divides, so its epilogue returns at the length
    // check and is never timed - and with the loop methods identical, no committed figure in
    // VarkaEmitterParityBenchmark-jdk25-results.txt can be affected by this change.
    //
    // If a future task relaxes the budget so a loop method does hold two (step B2), this test
    // fails and that is the signal that the parity file has to be regenerated.
    val col = new ColumnRef(0)
    for (roots <- Seq(
        Seq[VarkaVectorIR](new Year(col)),
        Seq[VarkaVectorIR](new Year(col), new Month(col)),
        Seq[VarkaVectorIR](
          new Year(col), new Month(col), new DayOfMonth(col), new Quarter(col)))) {
      val plainBytes = emitMulti(roots, 1, 0, unshared)._2
      val sharedBytes = emitMulti(roots, 1, 0, sharing)._2
      val loops = methodNames(emitMulti(roots, 1, 0, unshared))
        .filter(n => n.startsWith("loopDense") || n.startsWith("loopMasked"))
      assert(loops.size === roots.size * 2,
        s"expected one dense and one masked loop method per output, got $loops")
      for (name <- loops) {
        assert(VarkaEmitterTestSupport.codeSize(sharedBytes, name)
          === VarkaEmitterTestSupport.codeSize(plainBytes, name),
          s"$name changed size under sharing, so a loop body now holds two calendar nodes " +
            "and the parity results file needs regenerating")
      }
    }
  }

  test("sharing the prefix moves the epilogue's HugeMethodLimit crossing from 19 outputs to 44") {
    // This is what step B1 is for, and the only thing it is for under today's grouping. The
    // epilogue is one method over *every* output by task 24's deliberate decision, so its size
    // grows with the whole projection rather than with a group. Four fields over one date
    // repeat the decomposition four times; sharing it is most of the method.
    //
    // The outputs must be distinct nodes to count: the IR's records compare by value, so
    // year(d) twice is one node and the emitter already emits it once. Four fields per date
    // over as many dates as the width needs is the shape task 44 measured.
    //
    // Both boundaries below moved by task 51: removing the per-extraction range guard shrank
    // every emitted calendar prefix, shared or not, so more outputs now fit under the 8000-byte
    // HugeMethodLimit before HotSpot gives up on compiling the method (interpreted, boxed
    // vectors, on every batch whose length is not a lane multiple). Unshared moved from 16
    // fits/17 crosses (task 44's original number) to 18 fits/19 crosses; shared moved from the
    // 40-output boundary PLAN_TASK_32.md section 7.1 recorded to 44. Re-measured directly
    // rather than estimated - see PLAN_TASK_51.md section 4.1 for the numbers this replaced.
    def fields(dates: Int): Seq[VarkaVectorIR] = (0 until dates).flatMap { c =>
      val col = new ColumnRef(c)
      Seq[VarkaVectorIR](new Year(col), new Month(col), new DayOfMonth(col), new Quarter(col))
    }
    val limit = 8000
    // Unshared, 18 outputs fit and 19 do not.
    assert(epilogueSize(fields(5).take(18), 12, unshared) < limit)
    assert(epilogueSize(fields(5).take(19), 12, unshared) > limit)
    // Shared, the same 19 fit with room to spare, and the boundary moves out to 44 outputs
    // over eleven dates.
    assert(epilogueSize(fields(5).take(19), 12, sharing) < limit)
    assert(epilogueSize(fields(8), 12, sharing) < limit)
    val past = epilogueSize(fields(11), 12, sharing)
    assert(past > limit,
      s"forty-four shared calendar outputs now fit in $past bytes - sharing reaches further " +
        "than this test records, so the ladder in PLAN_TASK_32.md section 7.1 is stale again")
  }

  test("the masked body agrees with the dense body on null-free data") {
    // forceMasked reports one null over a full-set bitmap, which the dispatcher sends down
    // runMasked; the reference expectations are identical to the dense run's.
    val root = new IfElse(new Compare(CompareOp.LT, new ColumnRef(0), new ColumnRef(1)),
      new Greatest(new DayOfWeek(new ColumnRef(0)), new ColumnRef(1)),
      new SubDays(new ColumnRef(0), new LiteralSlot(0)))
    val nullFree = Seq(Seq[Int => Boolean](_ => false, _ => false))
    checkMatrix(Seq(root), 2, Array(3), Seq(17, 64, 65, 1000), nullFree, ctx = "dense")
    checkMatrix(Seq(root), 2, Array(3), Seq(17, 64, 65, 1000), nullFree,
      forceMasked = true, ctx = "forced-masked")
  }

  test("the lanewise-DIV floorMod reference variant agrees with the shipped magic multiply") {
    val roots = Seq[VarkaVectorIR](new DayOfWeek(new ColumnRef(0)))
    val extremes = Array(Int.MinValue, Int.MaxValue, -1, 0, -7, 7)
    def days(c: Int, i: Int): Int =
      if (i < extremes.length) extremes(i) else i * 31 - 7000
    checkMatrix(roots, 1, Array.empty[Int], Seq(64, 1000),
      nullPatterns.map(p => Seq(p._2)), data = days, ctx = "div-variant",
      options = VarkaEmitOptions.DEFAULTS.withFloorMod7(VarkaEmitOptions.FloorMod7.DIV))
  }

  test("the digit-sum floorMod reference variant agrees with the shipped magic multiply") {
    // The task 11 lowering, kept as a reference: same matrix as the shipped path's own test,
    // with the 15-bit fold boundaries among the extremes.
    val roots = Seq[VarkaVectorIR](
      new DayOfWeek(new ColumnRef(0)), new WeekDay(new ColumnRef(0)))
    val extremes = Array(Int.MinValue, Int.MaxValue, Int.MinValue + 1, Int.MaxValue - 1,
      -1, 0, 1, -7, 7, -8, 8, 32767, 32768, -32768, -32769)
    def days(c: Int, i: Int): Int =
      if (i < extremes.length) extremes(i) else i * 997 - 300000
    checkMatrix(roots, 1, Array.empty[Int], Seq(1, 13, 17, 64, 1000),
      nullPatterns.map(p => Seq(p._2)), data = days, ctx = "digit-sum-variant",
      options = VarkaEmitOptions.DEFAULTS.withFloorMod7(VarkaEmitOptions.FloorMod7.DIGIT_SUM))
  }

  test("task 21: a comparison root emits the selection bitmap with null-as-false") {
    // The simplest filter kernel: one Compare root, its bitmap checked against the Kleene
    // reference with unknown collapsed to false at the root - across lengths (partial lane
    // groups included) and every pair of null patterns, all-null included (the all-null
    // shortcut must leave a correct all-clear bitmap for a null-intolerant root).
    val root = new Compare(CompareOp.LT, new ColumnRef(0), new ColumnRef(1))
    checkMatrix(Seq(root), 2, Array.emptyIntArray, Seq(0, 5, 16, 17, 65, 1000), combos(2),
      ctx = "cmp-root")
  }

  test("task 21: BETWEEN- and IN-shaped roots match the reference") {
    // The survey's two dominant filter shapes: BETWEEN as And over paired comparisons
    // against literals, and IN as the balanced OR chain of EQ leaves (task 20's lowering,
    // now at a root). Data cycles a small range so both selects and rejects occur.
    val d = new ColumnRef(0)
    val between = new And(
      new Compare(CompareOp.GE, d, new LiteralSlot(0)),
      new Compare(CompareOp.LE, d, new LiteralSlot(1)))
    val inChain = new Or(
      new Or(new Compare(CompareOp.EQ, d, new LiteralSlot(0)),
        new Compare(CompareOp.EQ, d, new LiteralSlot(1))),
      new Compare(CompareOp.EQ, d, new LiteralSlot(2)))
    checkMatrix(Seq(between), 1, Array(-3, 4), Seq(5, 64, 65, 1000),
      nullPatterns.map(p => Seq(p._2)), ctx = "between-root")
    checkMatrix(Seq(inChain), 1, Array(-4, 0, 5), Seq(5, 64, 65, 1000),
      nullPatterns.map(p => Seq(p._2)), ctx = "in-root")
  }

  test("task 21: an Or root over one all-null column still selects on the live column") {
    // The all-null-shortcut counterexample, pinned: Or(unknown, known-true) is known true,
    // so with column 0 all-null and column 1 live the rows where column 1 matches must
    // still select. A shortcut that fired on "some referenced column is all-null" would
    // zero this bitmap - which is why Cond roots are excluded from it.
    val root = new Or(
      new Compare(CompareOp.EQ, new ColumnRef(0), new LiteralSlot(0)),
      new Compare(CompareOp.EQ, new ColumnRef(1), new LiteralSlot(0)))
    val allNullFirst = Seq(Seq[Int => Boolean](_ => true, _ => false))
    checkMatrix(Seq(root), 2, Array(4), Seq(5, 64, 65, 1000), allNullFirst,
      ctx = "or-allnull")
    // And the full matrix for completeness: every pair of patterns.
    checkMatrix(Seq(root), 2, Array(4), Seq(65), combos(2), ctx = "or-matrix")
  }

  test("task 21: validity-predicate roots - IS NOT NULL, and IS NULL as its NOT") {
    val isNotNull = new IsNotNull(new ColumnRef(0))
    checkMatrix(Seq(isNotNull), 1, Array.emptyIntArray, Seq(5, 64, 65, 1000),
      nullPatterns.map(p => Seq(p._2)), ctx = "isnotnull-root")
    checkMatrix(Seq[VarkaVectorIR](new Not(isNotNull)), 1, Array.emptyIntArray,
      Seq(5, 64, 65, 1000), nullPatterns.map(p => Seq(p._2)), ctx = "isnull-root")
  }

  test("task 21: a mask root beside a value root shares the kernel and its subtrees") {
    // The emitter serves mixed outputs even though milestone 3's filter kernels are
    // single-root: the mask and the value share one CSE'd subtree, and each output keeps
    // its own contract (bitmap with no data store; value with data plus validity).
    val add = new AddDays(new ColumnRef(0), new LiteralSlot(0))
    val roots = Seq[VarkaVectorIR](
      new Compare(CompareOp.GT, add, new ColumnRef(1)),
      add)
    checkMatrix(roots, 2, Array(7), Seq(5, 64, 65, 1000), combos(2), ctx = "mixed-roots")
  }

  test("task 21: the masked body agrees with the dense body on a null-free mask root") {
    val root = new And(
      new Compare(CompareOp.GE, new ColumnRef(0), new LiteralSlot(0)),
      new Compare(CompareOp.LE, new ColumnRef(0), new LiteralSlot(1)))
    val nullFree = Seq(Seq[Int => Boolean](_ => false))
    checkMatrix(Seq(root), 1, Array(-3, 4), Seq(64, 65, 1000), nullFree, ctx = "mask-dense")
    checkMatrix(Seq(root), 1, Array(-3, 4), Seq(64, 65, 1000), nullFree,
      forceMasked = true, ctx = "mask-forced")
  }

  test("a shared subchain feeds a condition and both branches across outputs") {
    // CSE across the value/condition boundary: `add = date_add(d, 7)` is compared against,
    // blended over, and emitted as its own output - one computation per lane group.
    val add = new AddDays(new ColumnRef(0), new LiteralSlot(0))
    val cond = new Compare(CompareOp.GT, add, new ColumnRef(1))
    val roots = Seq[VarkaVectorIR](
      add,
      new IfElse(cond, add, new ColumnRef(1)),
      new IfElse(new Not(cond), new DateDiff(add, new ColumnRef(1)), new LiteralSlot(1)))
    checkMatrix(roots, 2, Array(7, 42), Seq(5, 64, 65, 1000), combos(2), ctx = "shared")
  }

  test("IR outside the emitter's shape is rejected with a reason, not emitted wrong") {
    def rejects(body: => Unit, fragment: String): Unit = {
      val e = intercept[IllegalArgumentException](body)
      assert(e.getMessage.contains(fragment), s"message was: ${e.getMessage}")
    }
    rejects(emit(chain(VarkaLoopEmitter.MAX_CHAIN_DEPTH + 1),
      VarkaLoopEmitter.MAX_CHAIN_DEPTH + 1), "MAX_CHAIN_DEPTH")
    rejects(emit(new AddDays(new ColumnRef(1), new LiteralSlot(0)), 1), "column ordinal")
    rejects(emit(new AddDays(new ColumnRef(0), new LiteralSlot(1)), 1), "literal slot")
    rejects(emit(new AddDays(new ColumnRef(0), new ColumnRef(0)), 1), "literal slots")
    rejects(VarkaLoopEmitter.emit("t", java.util.List.of[VarkaVectorIR](), 1, 0),
      "no output chains")
    rejects(VarkaLoopEmitter.emit("t", java.util.List.of(addDays(0)), 0, 1), "numInputs")
    rejects(VarkaLoopEmitter.emit("t", java.util.List.of(addDays(0)),
      VarkaLoopEmitter.MAX_INPUTS + 1, 1), "numInputs")
    // 5 disjoint depth-13 chains hold 65 distinct ops, one past the total-size cap. The cap
    // counts nodes after CSE: the same 4 chains repeated as 8 outputs stay within it.
    val disjointChains = (0 until 5).map(k => chain(13, slotBase = k * 13))
    rejects(emitMulti(disjointChains, 1, 65), "MAX_FUSED_NODES")
    val (_, sharedOk) = emitMulti(
      disjointChains.take(4) ++ disjointChains.take(4), 1, 52)
    assert(sharedOk.nonEmpty)
    // Task 11: conditions are never values. (A condition as an output ROOT became legal in
    // task 21 - it emits a selection bitmap - so only the value positions reject now.)
    val cmp = new Compare(CompareOp.LT, new ColumnRef(0), new ColumnRef(0))
    rejects(emitMulti(Seq(new AddDays(cmp, new LiteralSlot(0))), 1, 1), "value position")
    rejects(emitMulti(Seq(new Greatest(new ColumnRef(0), cmp)), 1, 0), "value position")
  }

  test("a wrong descriptor fails naming the call, not as an anonymous VerifyError") {
    val named = emit(addDays(0), 1, VarkaEmitOptions.DEFAULTS.withMisdescribeAdd(true))
    // Member resolution is link-time work, so the class still verifies...
    assert(VarkaEmitterTestSupport.verify(named._2).isEmpty)
    val (kernel, loader) = load(named)
    try {
      val arena = Arena.ofConfined()
      try {
        // Long enough that the vector loop (where the wrong call sits) runs at any width.
        val length = 64
        val input = makeInput(arena, length, _ => false)
        val out = makeOutput(arena, length)
        val e = intercept[LinkageError] {
          kernel.run(
            Array(input.data.address()), Array(0L), Array(0),
            Array(out._1.address()), Array(out._2.address()), Array(1), length)
        }
        // ...and the first execution names the exact call the descriptor table got wrong.
        assert(e.isInstanceOf[NoSuchMethodError], s"got ${e.getClass}: ${e.getMessage}")
        assert(e.getMessage.contains("IntVector.add"), s"message was: ${e.getMessage}")
      } finally {
        arena.close()
      }
    } finally {
      loader.release()
    }
  }

  test("the emitted class unloads once the loader is released") {
    val queue = new ReferenceQueue[ClassLoader]()
    val (className, bytes) = emit(addDays(0), 1)
    var loader = new VarkaGeneratedClassLoader(getClass.getClassLoader)
    val ref = new WeakReference[ClassLoader](loader, queue)
    loader.defineGeneratedClass(className, bytes)
    loader.loadClass(className)
    loader.release()
    // Drop the only strong reference; the frame slot must not pin the loader (the reason the
    // existing loader suite uses a var too).
    loader = null
    var collected = false
    var attempts = 0
    while (!collected && attempts < 50) {
      System.gc()
      collected = queue.remove(100) != null
      attempts += 1
    }
    assert(collected, "the loader (and with it the emitted class) was not collected")
    assert(ref.get() == null)
  }

  /** The committed line map of the every-node-type key; see the test that pins it. */
  private val pinnedLineMap = Seq(
    "1=col:0",
    "2=lit:0",
    "3=(cmp:LT 1 2)",
    "4=(cmp:EQ 1 2)",
    "5=(not 4)",
    "6=(or 3 5)",
    "7=(cmp:GE 1 2)",
    "8=(isNotNull 1)",
    "9=(and 7 8)",
    "10=(and 6 9)",
    "11=(addDays 1 2)",
    "12=(subDays 1 2)",
    "13=(greatest 11 12)",
    "14=(year 1)",
    "15=(month 1)",
    "16=(greatest 14 15)",
    "17=(dayOfMonth 1)",
    "18=(quarter 1)",
    "19=(greatest 17 18)",
    "20=(least 16 19)",
    "21=(dayOfYear 1)",
    "22=(greatest 20 21)",
    "23=(dayOfWeek 1)",
    "24=(dateDiff 22 23)",
    "25=(weekDay 1)",
    "26=(nextDay 1 2)",
    "27=(addMonths 1 2)",
    "28=(least 26 27)",
    "29=(least 25 28)",
    "30=(least 24 29)",
    "31=(if 10 13 30)").mkString("\n")

  /** The class's own LineNumberTable key, parsed back into line -> rendered IR node. */
  private def lineKey(bytes: Array[Byte]): Map[Int, String] = {
    val recorded = VarkaDebugInfoReader.lineMap(bytes)
    assert(recorded != null && recorded.nonEmpty, "the class recorded no line map")
    recorded.linesIterator.map { entry =>
      val parts = entry.split("=", 2)
      parts(0).toInt -> parts(1)
    }.toMap
  }

  test("emit rejects null options the way it rejects its other arguments") {
    // The other two argument checks throw IllegalArgumentException with a message; options
    // would otherwise have failed as a bare NPE partway through the analysis walk.
    val e = intercept[IllegalArgumentException] {
      VarkaLoopEmitter.emit("X", Seq[VarkaVectorIR](addDays(0)).asJava, 1, 1, null, null, null)
    }
    assert(e.getMessage.contains("options"), e.getMessage)
  }

  test("task 23: the shallow rendering of every node type is pinned, like the shape hash") {
    // The line map travels inside the class bytes and is read back by tooling with no live
    // session, so its rendering is a contract, not an implementation detail - and it used to
    // ride Record.toString, whose format no JDK promises. One key using all 22 node types (and
    // three CompareOps), so a change to any rendering, to the operand order, or to the
    // topological schedule fails here. If it does: make sure the change is intended, then
    // update the literal and say so in the task plan - the same rule as the pinned shape
    // hashes in VarkaShapeCacheSuite. Task 26 added the four calendar extractions and
    // re-pinned it (PLAN_TASK_26.md); task 33 added NextDay, task 34 added DayOfYear and
    // task 40 added AddMonths, each re-pinning it again (PLAN_TASK_33.md, PLAN_TASK_34.md,
    // PLAN_TASK_40.md).
    val col = new ColumnRef(0)
    val lit = new LiteralSlot(0)
    val cond = new And(
      new Or(
        new Compare(CompareOp.LT, col, lit),
        new Not(new Compare(CompareOp.EQ, col, lit))),
      new And(new Compare(CompareOp.GE, col, lit), new IsNotNull(col)))
    val chrono = new Greatest(
      new Least(
        new Greatest(new Year(col), new Month(col)),
        new Greatest(new DayOfMonth(col), new Quarter(col))),
      new DayOfYear(col))
    val everyNode = new IfElse(
      cond,
      new Greatest(new AddDays(col, lit), new SubDays(col, lit)),
      new Least(new DateDiff(chrono, new DayOfWeek(col)),
        new Least(new WeekDay(col),
          new Least(new NextDay(col, lit), new AddMonths(col, lit)))))
    val (_, bytes) = emitMulti(Seq(everyNode), 1, 1)
    assert(VarkaDebugInfoReader.lineMap(bytes) === pinnedLineMap)
    // The DAG, not a tree: col:0 is written once as line 1 and pointed at fifteen times. The
    // Record.toString rendering this replaced inlined every subtree, so line 25 alone carried
    // the whole IR and the key grew quadratically in exactly the sharing the emitter exploits.
    assert(pinnedLineMap.linesIterator.count(_.contains("col:0")) === 1)
  }

  test("telemetry: the emitted lines index the IR nodes the debug attribute records") {
    // datediff(date_add(d, 1), d2): five distinct nodes, so the loop and the epilogue
    // attribute their instructions to lines 1..5 and the key decodes every one of them.
    val add = new AddDays(new ColumnRef(0), new LiteralSlot(0))
    val root = new DateDiff(add, new ColumnRef(1))
    val (_, bytes) = emitMulti(Seq(root), 2, 1)
    val key = lineKey(bytes)
    assert(key.keys.toSeq.sorted === (1 to key.size).toSeq,
      "the key must number the nodes 1..N with no gaps")
    // Children strictly before parents, which is what makes a line number a schedule position.
    assert(key(key.size).startsWith("(dateDiff"), s"the root should be last: ${key(key.size)}")
    assert(key.values.exists(_.startsWith("col:")))
    assert(key.values.count(_.startsWith("(addDays")) === 1)
    for (method <- Seq("loopMasked0", "epilogueMasked", "loopDense0", "epilogueDense")) {
      val lines = VarkaEmitterTestSupport.lineNumbers(bytes, method)
      assert(lines.asScala.nonEmpty, s"$method carries no LineNumberTable")
      assert(lines.asScala.forall(line => key.contains(line)),
        s"$method has lines outside the key: ${lines.asScala.mkString(", ")}")
    }
  }

  test("a kernel failure's stack frame resolves to the IR node that threw") {
    // The misdescribe option fails the AddDays call site at link time, inside the loop - the
    // shape a real kernel failure takes. The frame through the generated class must name the
    // SourceFile and a line, and the class's own key must decode that line to the node.
    val named = emit(addDays(0), 1, VarkaEmitOptions.DEFAULTS.withMisdescribeAdd(true))
    val (className, bytes) = named
    val (kernel, loader) = load(named)
    try {
      val arena = Arena.ofConfined()
      try {
        val length = 64
        val input = makeInput(arena, length, _ => false)
        val out = makeOutput(arena, length)
        val e = intercept[LinkageError] {
          kernel.run(
            Array(input.data.address()), Array(0L), Array(0),
            Array(out._1.address()), Array(out._2.address()), Array(1), length)
        }
        val frame = e.getStackTrace.find(_.getClassName == className).getOrElse(
          fail(s"no frame in the generated class:\n${e.getStackTrace.mkString("\n")}"))
        val simpleName = className.substring(className.lastIndexOf('.') + 1)
        assert(frame.getFileName === s"$simpleName.java")
        assert(frame.getLineNumber > 0, "the frame carries no line number")
        val node = lineKey(bytes).getOrElse(frame.getLineNumber,
          fail(s"line ${frame.getLineNumber} is not in the recorded key"))
        assert(node.startsWith("(addDays"), s"the failing line decoded to $node")
      } finally {
        arena.close()
      }
    } finally {
      loader.release()
    }
  }

  test("telemetry: the SourceFile and VarkaDebugInfo attributes round-trip off the bytes") {
    val name = s"org.apache.spark.sql.varka.execution.VarkaFusedTest${classCounter.addAndGet(1)}"
    val bytes = VarkaLoopEmitter.emit(name, Seq(addDays(0)).asJava, 1, 1,
      "Varka_Project_Stage3.java", "date_add(d#1, 3) AS a#2")
    // The attributes are metadata: the class must verify exactly as it did without them.
    assert(VarkaEmitterTestSupport.verify(bytes).isEmpty)
    // A reader without the mapper sees an opaque attribute under the right name - the shape
    // any third-party class-file tool gets - while the diagnostics reader registers the
    // mapper and recovers the payload: the rendered IR and the caller's plan fragment.
    assert(VarkaEmitterTestSupport.hasAttributeNamed(bytes, "VarkaDebugInfo"))
    assert(VarkaDebugInfoReader.sourceFile(bytes) === "Varka_Project_Stage3.java")
    val ir = VarkaDebugInfoReader.ir(bytes)
    assert(ir.contains("outputs=[(addDays col:0 lit:0)]"))
    assert(ir.contains("numInputs=1"))
    assert(VarkaDebugInfoReader.planFragment(bytes) === "date_add(d#1, 3) AS a#2")
    // Task 16: the same attribute carries the LineNumberTable's decoding key.
    assert(VarkaDebugInfoReader.lineMap(bytes).startsWith("1="))
  }

  test("the telemetry-defaulted emit derives the SourceFile and records no plan fragment") {
    val (className, bytes) = emit(addDays(0), 1)
    val simpleName = className.substring(className.lastIndexOf('.') + 1)
    assert(VarkaDebugInfoReader.sourceFile(bytes) === s"$simpleName.java")
    assert(VarkaDebugInfoReader.ir(bytes).contains("(addDays col:0 lit:0)"))
    assert(VarkaDebugInfoReader.planFragment(bytes) === "")
  }
}
