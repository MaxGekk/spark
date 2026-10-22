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

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelCheck.Batch
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR._
import org.apache.spark.sql.types.{ByteType, ShortType}

/**
 * TINYINT and SMALLINT columns, stored narrow and computed either on the int lane (the widened
 * arm, the default) or on the narrow lane itself (the native arm, `narrowLanes`), VARKA-329.
 *
 * <p>Every value of a byte column is written and read back at every tail length and on a metre
 * batch that puts each value on every lane position; every short value is sampled across the
 * domain with its two extremes; every ordered byte pair runs through the comparisons, a range set,
 * `greatest`, `least` and a choice, 2^16 rows in one batch; a narrow column read beside an int
 * column is widened on load in an int-lane kernel; and the two arms answer the same, since each
 * is checked against [[VarkaReference]] on the same batch.
 *
 * <p>In its own JVM: the suite sweeps `lanesOverride` and holds narrow and int species at once.
 */
class VarkaEmitterNarrowSuite extends VarkaEmitterTestBase with VarkaOwnJvm {

  /**
   * The default arm: narrow values stored narrow, computed on the int lane. The option is set
   * explicitly rather than left to the matrix's base, which is this suite's own subject: under
   * `narrowLanes=true` the base has the other arm, and the pair must still be the pair.
   */
  private val widened: VarkaEmitOptions =
    VarkaMatrix.base.toBuilder.narrowLanes(false).build()

  /** The native arm: a tree whose every value is narrow stays on the narrow lane. */
  private val native: VarkaEmitOptions =
    VarkaMatrix.base.toBuilder.narrowLanes(true).build()

  private val arms: Seq[(String, VarkaEmitOptions)] = Seq(
    "the widened arm" -> widened,
    "the native arm" -> native)

  private val byteCol: VarkaVectorIR = new ColumnRef(0, LaneType.BYTE)
  private val byteCol2: VarkaVectorIR = new ColumnRef(1, LaneType.BYTE)
  private val shortCol: VarkaVectorIR = new ColumnRef(0, LaneType.SHORT)
  private val shortCol2: VarkaVectorIR = new ColumnRef(1, LaneType.SHORT)

  private val roundTripLengths: Seq[Int] =
    Seq(1, 2, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257)

  /**
   * The null patterns a narrow column runs at: none, every seventh row, 32-row groups, and the
   * same all-valid batch with a mask forced. The last one makes the masked body run over no null
   * lane, and the first the dense body, which is the pair of bodies the kernel must keep apart.
   */
  private def patterns(length: Int): Seq[(String, Int => Boolean, Boolean)] = Seq(
    ("with no nulls", (_: Int) => false, false),
    ("with every seventh row null", (i: Int) => i % 7 == 3, false),
    ("with null 32-row groups", (i: Int) => (i / 32) % 3 == 0, false)) ++
    // A forced mask reports one null over a full bitmap, which at a single row is
    // indistinguishable from an all-null column: the kernel's all-null shortcut would return
    // before writing anything, as the morsel contract lets it, so one-row batches skip it.
    (if (length > 1) Seq(("with a forced mask", (_: Int) => false, true)) else Seq.empty)

  /** {@code v} as {@code lane} stores it, sign-extended - what the reference reads back. */
  private def narrow(lane: LaneType, v: Int): Int = lane match {
    case LaneType.BYTE => v.toByte.toInt
    case LaneType.SHORT => v.toShort.toInt
    case _ => v
  }

  private def width(lane: LaneType): Long = lane match {
    case LaneType.BYTE => 1L
    case LaneType.SHORT => 2L
    case LaneType.INT => 4L
    case LaneType.LONG => 8L
  }

  /**
   * Whether the compiler refuses this shape under `options`, because the narrow species of the
   * int species' width does not exist: `lanesOverride=4` asks for a 32-bit byte species, which
   * the Vector API does not have, and `validityByWidth=false` bakes no lane count for the
   * conversion to read. The emitter would name a constant the class lacks
   * (`ByteVector.SPECIES_32`), so the refusal is the answer, and the suite pins it here instead
   * of emitting; the compiler asks the same question, and an option matrix configuration that
   * declines a narrow kernel answers it with the row engine (VARKA-329).
   */
  private def refusedForNoSpecies(
      roots: Seq[VarkaVectorIR], options: VarkaEmitOptions): Boolean =
    VarkaLoopEmitter.narrowRefusal(roots.asJava, options).toScala match {
      case Some(reason) =>
        assert(
          reason == "a narrow column needs a baked int lane count to be widened" ||
            reason.matches("a (BYTE|SHORT) column has no widening load at \\d+ int lanes"),
          s"unexpected refusal: $reason")
        true
      case None => false
    }

  private def put(lane: LaneType, seg: MemorySegment, row: Int, v: Int): Unit = lane match {
    case LaneType.BYTE => seg.set(ValueLayout.JAVA_BYTE, row.toLong, v.toByte)
    case LaneType.SHORT => seg.set(ValueLayout.JAVA_SHORT, row.toLong * 2, v.toShort)
    case LaneType.INT => seg.set(ValueLayout.JAVA_INT, row.toLong * 4, v)
    case LaneType.LONG => seg.set(ValueLayout.JAVA_LONG, row.toLong * 8, v.toLong)
  }

  /**
   * Runs the condition {@code roots} over columns of the given lanes and compares every row's
   * selection bit with [[VarkaReference]]. A condition's output is its validity bitmap alone, so
   * the data address is 0, as the kernel contract says; the columns are stored at their lanes'
   * widths, which is how a narrow column sits beside an int one.
   */
  private def checkConds(
      ctx: String,
      roots: Seq[Cond],
      lanes: Seq[LaneType],
      length: Int,
      value: (Int, Int) => Int,
      isNull: (Int, Int) => Boolean,
      options: VarkaEmitOptions): Unit = {
    if (refusedForNoSpecies(roots, options)) {
      return
    }
    val (kernel, _) = load(emitMulti(roots, lanes.size, 0, options))
    val bytes = (length + 7) / 8
    val arena = Arena.ofConfined()
    try {
      val data = lanes.map(lane => alloc(arena, length * width(lane))).toArray
      val valid = lanes.indices.map(_ => alloc(arena, bytes)).toArray
      valid.foreach(_.fill(0.toByte))
      val nullCounts = new Array[Int](lanes.size)
      for (c <- lanes.indices; row <- 0 until length) {
        if (isNull(c, row)) {
          nullCounts(c) += 1
        } else {
          put(lanes(c), data(c), row, value(c, row))
          val bit = 1 << (row % 8)
          val off = (row / 8).toLong
          valid(c).set(ValueLayout.JAVA_BYTE, off,
            (valid(c).get(ValueLayout.JAVA_BYTE, off) | bit).toByte)
        }
      }
      val outBytes = outputValidityBytes(options, length)
      for (fill <- VarkaKernelCheck.bothFills) {
        val outs = roots.map(_ => makeOutput(arena, length, outBytes, fill))
        val status = kernel.run(data.map(_.address), valid.map(_.address), nullCounts,
          roots.map(_ => 0L).toArray, outs.map(_._2.address).toArray, Array.empty[Int], length,
          VarkaEmitterTestSupport.scratch(kernel, length))
        assert(status === 0, s"$ctx: declined (length $length, $fill)")
        for (row <- 0 until length) {
          val ints = lanes.indices.map { c =>
            if (isNull(c, row)) 0 else narrow(lanes(c), value(c, row))
          }.toArray
          val nulls = lanes.indices.map(c => isNull(c, row)).toArray
          for (o <- roots.indices) {
            val want = VarkaReference.cond(roots(o), ints, nulls, Array.empty[Int]).isTrue
            val got =
              (outs(o)._2.get(ValueLayout.JAVA_BYTE, (row / 8).toLong) & (1 << (row % 8))) != 0
            assert(got === want, s"$ctx: cond $o ${VarkaVectorIR.canonical(roots(o))} row " +
              s"$row of $length $fill: want $want, got $got")
          }
        }
      }
    } finally {
      arena.close()
    }
  }

  /** Runs the value {@code roots} over one column of the drawn values, as the arms compare it. */
  private def checkValues(
      ctx: String,
      roots: Seq[VarkaVectorIR],
      numInputs: Int,
      lits: Array[Int],
      length: Int,
      data: Array[Array[Int]],
      pattern: Int => Boolean,
      forceMasked: Boolean,
      options: VarkaEmitOptions): Unit = {
    if (refusedForNoSpecies(roots, options)) {
      return
    }
    val (className, bytes) = emitMulti(roots, numInputs, lits.length, options)
    val batch = Batch(length, Seq.fill(numInputs)(pattern), data, forceMasked)
    assert(VarkaKernelCheck.runAndCompareNarrow(
      s"$ctx (length $length)", className, bytes, roots, numInputs, lits, batch))
  }

  test("the option picks the narrow lane only where every value is narrow") {
    assert(VarkaVectorIR.emissionLane(byteCol, widened) === LaneType.INT)
    assert(VarkaVectorIR.emissionLane(byteCol, native) === LaneType.BYTE)
    assert(VarkaVectorIR.emissionLane(shortCol, widened) === LaneType.INT)
    assert(VarkaVectorIR.emissionLane(shortCol, native) === LaneType.SHORT)
    // A comparison of two narrow columns is itself narrow - its lane is its left operand's -
    // so the native arm takes it to the narrow lane too, and only the widened arm's int lane
    // stands between the leaves and the mask.
    val cmp: Cond = new Compare(CompareOp.LT, byteCol, byteCol2)
    assert(VarkaVectorIR.emissionLane(cmp, widened) === LaneType.INT)
    assert(VarkaVectorIR.emissionLane(cmp, native) === LaneType.BYTE)
  }

  test("a narrow value lifted to the long lane is refused, not emitted") {
    // The third refusal: a bigint guard makes the kernel long-lane, and no long-lane form of a
    // narrow value exists in either arm. The compiler asks the same question, so a query that
    // would build this - `CASE WHEN l < 0 THEN y1 ELSE y2 END` - answers from the row engine.
    val longCond: Cond =
      new Compare(CompareOp.LT, new ColumnRef(1, LaneType.LONG), new ColumnRef(2, LaneType.LONG))
    val longNarrow: VarkaVectorIR = new IfElse(longCond,
      new WidenLane(byteCol, LaneType.LONG), new WidenLane(byteCol, LaneType.LONG))
    for ((arm, options) <- arms) {
      assert(VarkaLoopEmitter.narrowRefusal(List[VarkaVectorIR](longNarrow).asJava, options)
        .toScala === Some("a narrow value under a bigint kernel has no lowering"), arm)
    }
    // The same choice on the int lane is the widened arm's ordinary shape: where a configuration
    // has no species for the widening load it is refused, but for that width and never for the
    // long lane, which is the distinction this test pins.
    val intNarrow: VarkaVectorIR = new IfElse(
      new Compare(CompareOp.LT, new ColumnRef(1, LaneType.INT), new ColumnRef(2, LaneType.INT)),
      new WidenLane(byteCol, LaneType.INT), new WidenLane(byteCol, LaneType.INT))
    val intRefusal =
      VarkaLoopEmitter.narrowRefusal(List[VarkaVectorIR](intNarrow).asJava, widened).toScala
    assert(intRefusal.forall(reason =>
      reason == "a narrow column needs a baked int lane count to be widened" ||
        reason.matches("a (BYTE|SHORT) column has no widening load at \\d+ int lanes")),
      intRefusal)
  }

  test("a byte column round-trips every value at every length and tail, in both arms") {
    val lits = Array(-56)
    val roots = Seq(byteCol, new LiteralSlot(0, LaneType.BYTE))
    for ((arm, options) <- arms; length <- roundTripLengths;
         (nulls, pattern, force) <- patterns(length)) {
      val data = Array(Array.tabulate(length)(i => ((i * 97 + 13) % 256) - 128))
      checkValues(s"a byte column $arm $nulls", roots, 1, lits, length, data, pattern, force,
        options)
    }
    // The metre batch: every byte value once, and again at every lane position of a 64-lane
    // group, so a species that reads the wrong part of the word cannot hide behind a tail.
    val data = Array(Array.tabulate(1024)(i => ((i + 1024 * (i / 256)) % 256) - 128))
    for ((arm, options) <- arms) {
      checkValues(s"every byte value $arm", roots, 1, lits, 1024, data, _ => false, false,
        options)
      checkValues(s"every byte value masked $arm", roots, 1, lits, 1024, data, _ => false, true,
        options)
    }
  }

  test("a short column round-trips its extremes and a spread of its domain, in both arms") {
    val lits = Array(12345)
    val roots = Seq(shortCol, new LiteralSlot(0, LaneType.SHORT))
    for ((arm, options) <- arms; length <- roundTripLengths;
         (nulls, pattern, force) <- patterns(length)) {
      val data = Array(Array.tabulate(length)(i => ((i * 4051 + 17) % 65536) - 32768))
      if (length > 2) {
        data(0)(0) = Short.MinValue.toInt
        data(0)(1) = Short.MaxValue.toInt
        data(0)(2) = -1
      }
      checkValues(s"a short column $arm $nulls", roots, 1, lits, length, data, pattern, force,
        options)
    }
  }

  test("every byte pair gives the reference's answer through every comparison and choice") {
    val inBounds = java.util.List.of[Integer](
      Int.box(-100), Int.box(-50), Int.box(-1), Int.box(1), Int.box(100), Int.box(120))
    val conds: Seq[Cond] = Seq(
      new Compare(CompareOp.LT, byteCol, byteCol2),
      new Compare(CompareOp.LE, byteCol, byteCol2),
      new Compare(CompareOp.EQ, byteCol, byteCol2),
      new Compare(CompareOp.GT, byteCol, byteCol2),
      new Compare(CompareOp.GE, byteCol, byteCol2),
      new InRanges(byteCol, inBounds))
    val values: Seq[VarkaVectorIR] = Seq(
      new Greatest(byteCol, byteCol2),
      new Least(byteCol, byteCol2),
      new IfElse(new Compare(CompareOp.GT, byteCol, byteCol2), byteCol, byteCol2),
      new IfElse(new Compare(CompareOp.LT, byteCol, byteCol2), byteCol2, byteCol))
    // Row `a * 256 + b` holds the pair `(a, b)` over the whole byte square, once each.
    val length = 65536
    def data(c: Int, row: Int): Int =
      if (c == 0) (row / 256) - 128 else (row % 256) - 128
    for ((arm, options) <- arms) {
      checkConds(s"the byte square's comparisons $arm", conds,
        Seq(LaneType.BYTE, LaneType.BYTE), length, data, (_, _) => false, options)
      checkValues(s"the byte square's choices $arm", values, 2, Array.empty[Int], length,
        Array.tabulate(2)(c => Array.tabulate(length)(row => data(c, row))), _ => false, false,
        options)
    }
  }

  test("a narrow column read beside an int column is widened on load") {
    // The shape the compiler builds for an int-lane kernel that also reads a byte column: the
    // byte leaf widens into the int lane inside an int-lane tree, so its load is the part-width
    // species' and the widening itself is an identity.
    val mixedCmp: Cond = new Compare(CompareOp.LT,
      new WidenLane(byteCol, LaneType.INT), new ColumnRef(1, LaneType.INT))
    val mixedValue: VarkaVectorIR = new IfElse(new Compare(CompareOp.GT,
      new WidenLane(byteCol, LaneType.INT), new ColumnRef(1, LaneType.INT)),
      new WidenLane(byteCol, LaneType.INT), new ColumnRef(1, LaneType.INT))
    val length = 300
    def byteValue(row: Int): Int = ((row * 7) % 256) - 128
    def intValue(row: Int): Int = ((row * 13) % 256) - 128
    def isNull(c: Int, row: Int): Boolean = c == 0 && row % 11 == 5
    for ((arm, options) <- arms) {
      checkConds(s"a widened byte beside an int column $arm", Seq(mixedCmp),
        Seq(LaneType.BYTE, LaneType.INT), length, (c, row) =>
          if (c == 0) byteValue(row) else intValue(row), isNull, options)
      checkValues(s"a widened byte in a choice $arm", Seq(mixedValue), 2, Array.empty[Int],
        length, Array(Array.tabulate(length)(byteValue), Array.tabulate(length)(intValue)),
        row => isNull(0, row), false, options)
    }
  }

  test("a narrow-typed value computed on the int lane is stored narrow") {
    // The shape `storedNarrow` builds for a narrow-typed choice whose guard is int-typed: the
    // branches widen into the int lane, the root narrows once at its store, and one byte a row
    // comes out. Storing four bytes there is the answer a batch of these rows got wrong before
    // the store knew the root's target (VARKA-329); the reference narrows with the store, so a
    // wrong width shows row by row.
    val guard: Cond = new Compare(CompareOp.LT,
      new ColumnRef(2, LaneType.INT), new ColumnRef(3, LaneType.INT))
    val stored: VarkaVectorIR = new NarrowLane(new IfElse(guard,
      new WidenLane(byteCol, LaneType.INT), new WidenLane(byteCol2, LaneType.INT)),
      LaneType.BYTE)
    val length = 300
    def first(row: Int): Int = ((row * 7) % 256) - 128
    def second(row: Int): Int = 127 - ((row * 11) % 256)
    def left(row: Int): Int = (row % 5) - 2
    def right(row: Int): Int = ((row * 3) % 7) - 3
    val data = Array(
      Array.tabulate(length)(first), Array.tabulate(length)(second),
      Array.tabulate(length)(left), Array.tabulate(length)(right))
    for ((arm, options) <- arms) {
      // Both arms: an int node in the tree drives this kernel at the int lane either way, so the
      // shape is the same one and the option only decides how the leaves are read.
      checkValues(s"a choice stored narrow $arm", Seq(stored), 4, Array.empty[Int], length,
        data, row => row % 13 == 4, false, options)
    }
  }

  test("the reference narrows a stored byte and short as the lane does") {
    // The oracle itself: a value written at a narrow width reads back sign-extended, which is
    // what the suites above rest on.
    assert(narrow(LaneType.BYTE, 200) === -56)
    assert(narrow(LaneType.SHORT, 40000) === -25536)
    assert(ByteType.defaultSize.toLong === width(LaneType.BYTE))
    assert(ShortType.defaultSize.toLong === width(LaneType.SHORT))
  }

}
