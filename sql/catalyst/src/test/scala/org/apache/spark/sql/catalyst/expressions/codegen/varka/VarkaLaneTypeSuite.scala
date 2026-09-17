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

import java.lang.constant.{ClassDesc, ConstantDescs, MethodTypeDesc}
import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR._

/**
 * The lane a node's value occupies: that the leaves carry it, that every other node derives it,
 * and that a tree whose lanes do not fit cannot be built.
 *
 * The lane is the width of the vector a node is emitted into, not the Spark type above it - a
 * date, an int and a year-month interval are all the same 32-bit lane. Until the emitter is
 * parameterised on it (task 85, steps 3 and 4) the int lane is the only one it can emit, so a
 * well-formed 64-bit tree is built here and refused there, which is what the last test pins.
 */
class VarkaLaneTypeSuite extends SparkFunSuite {

  private val intCol = new ColumnRef(0)
  private val intLit = new LiteralSlot(0)
  private val longCol = new ColumnRef(0, LaneType.LONG)
  private val longLit = new LiteralSlot(0, LaneType.LONG)

  /** One node of every type in the sealed hierarchy, over int leaves. */
  private val everyIntNode: Seq[VarkaVectorIR] = Seq(
    intCol,
    intLit,
    new GuardedDay(intCol),
    new AddDays(intCol, intLit),
    new SubDays(intCol, intLit),
    new DateDiff(intCol, intCol),
    new IntArith(IntOp.ADD, Overflow.WRAP, intCol, intLit),
    new IntNeg(Overflow.WRAP, intCol),
    new Compare(CompareOp.LT, intCol, intLit),
    new And(new IsNotNull(intCol), new IsNotNull(intCol)),
    new Or(new IsNotNull(intCol), new IsNotNull(intCol)),
    new Not(new IsNotNull(intCol)),
    new IsNotNull(intCol),
    new IfElse(new IsNotNull(intCol), intCol, intLit),
    new Greatest(intCol, intLit),
    new Least(intCol, intLit),
    new DayOfWeek(intCol),
    new WeekDay(intCol),
    new DayOfWeekIso(intCol),
    new NextDay(intCol, intLit),
    new ThursdayOf(intCol),
    new AddMonths(intCol, intLit),
    new MakeDate(intCol, intLit, intLit, true),
    new Year(intCol),
    new Month(intCol),
    new DayOfMonth(intCol),
    new Quarter(intCol),
    new DayOfYear(intCol),
    new LastDay(intCol),
    new TruncDate(intCol, TruncLevel.YEAR),
    new TruncDateDynamic(intCol, intCol),
    new WeekOfYear(intCol))

  /** The node types whose lane is their operands' rather than INT by construction. */
  private val derivesItsLane: Set[Class[_]] = Set(
    classOf[IntArith], classOf[IntNeg], classOf[Greatest], classOf[Least], classOf[IfElse],
    classOf[Compare], classOf[And], classOf[Or], classOf[Not], classOf[IsNotNull])

  /** Every concrete node type the sealed hierarchy permits, nested interfaces expanded. */
  private def concreteNodeTypes(root: Class[_]): Set[Class[_]] =
    root.getPermittedSubclasses.toSeq.flatMap { c =>
      if (c.isRecord) Seq(c) else concreteNodeTypes(c)
    }.toSet

  test("the lane of an int tree is INT at every node type") {
    // The list above is the specification of what "derived" means per node, so it has to name
    // every type: a node type added without a lane rule would otherwise go unchecked here even
    // though the emitter walks it.
    assert(everyIntNode.map(_.getClass).toSet === concreteNodeTypes(classOf[VarkaVectorIR]))
    everyIntNode.foreach { node =>
      assert(node.laneType() === LaneType.INT, s"${VarkaVectorIR.canonical(node)}")
    }
  }

  test("a leaf carries its lane, and the short form is the int lane") {
    assert(new ColumnRef(3) === new ColumnRef(3, LaneType.INT))
    assert(new LiteralSlot(2) === new LiteralSlot(2, LaneType.INT))
    assert(longCol.lane() === LaneType.LONG)
    assert(longLit.lane() === LaneType.LONG)
    assert(new ColumnRef(3) !== new ColumnRef(3, LaneType.LONG))
  }

  test("a value node over long leaves is on the long lane") {
    // The lane-generic nodes: the ones task 85 ships at 64 bits. Each derives from its
    // operands, so a whole subtree answers LONG without anything below it being asked twice.
    val nodes = Seq(
      new IntArith(IntOp.MUL, Overflow.WRAP, longCol, longLit),
      new IntNeg(Overflow.FAIL, longCol),
      new Greatest(longCol, longLit),
      new Least(longCol, longLit),
      new IfElse(new Compare(CompareOp.GT, longCol, longLit), longCol, longLit),
      new Compare(CompareOp.EQ, longCol, longLit),
      new IsNotNull(longCol),
      new Not(new IsNotNull(longCol)),
      new And(new IsNotNull(longCol), new Compare(CompareOp.LT, longCol, longLit)),
      new Or(new IsNotNull(longCol), new Compare(CompareOp.LT, longCol, longLit)))
    nodes.foreach { node =>
      assert(node.laneType() === LaneType.LONG, s"${VarkaVectorIR.canonical(node)}")
    }
    // Nesting: a long subtree under a long node stays long.
    val nested = new IntArith(IntOp.ADD, Overflow.WRAP,
      new IntArith(IntOp.SUB, Overflow.WRAP, longCol, longLit), longCol)
    assert(nested.laneType() === LaneType.LONG)
  }

  test("a calendar node over a wider child is refused when it is built") {
    // The calendar lowerings decompose a 32-bit epoch day. A 64-bit child would be
    // reinterpreted rather than converted, so the constructor refuses it and names the lane.
    //
    // Every node that takes epoch days appears here, and every operand position of each: the
    // twenty checks are near-identical lines, which is exactly the shape a copy-paste slip
    // survives in, and a node left unchecked would report the int lane over a 64-bit subtree.
    // The completeness assertion below is what keeps the list honest as nodes are added.
    val refusals: Seq[(String, Int, () => Any)] = Seq(
      ("guardedDay", 0, () => new GuardedDay(longCol)),
      ("addDays", 0, () => new AddDays(longCol, intLit)),
      ("addDays", 1, () => new AddDays(intCol, longLit)),
      ("subDays", 0, () => new SubDays(longCol, intLit)),
      ("subDays", 1, () => new SubDays(intCol, longLit)),
      ("dateDiff", 0, () => new DateDiff(longCol, intCol)),
      ("dateDiff", 1, () => new DateDiff(intCol, longCol)),
      ("dayOfWeek", 0, () => new DayOfWeek(longCol)),
      ("weekDay", 0, () => new WeekDay(longCol)),
      ("dayOfWeekIso", 0, () => new DayOfWeekIso(longCol)),
      ("nextDay", 0, () => new NextDay(longCol, intLit)),
      ("nextDay", 1, () => new NextDay(intCol, longLit)),
      ("thursdayOf", 0, () => new ThursdayOf(longCol)),
      ("addMonths", 0, () => new AddMonths(longCol, intLit)),
      ("addMonths", 1, () => new AddMonths(intCol, longLit)),
      ("makeDate", 0, () => new MakeDate(longCol, intLit, intLit, true)),
      ("makeDate", 1, () => new MakeDate(intCol, longLit, intLit, false)),
      ("makeDate", 2, () => new MakeDate(intCol, intLit, longLit, true)),
      ("year", 0, () => new Year(longCol)),
      ("month", 0, () => new Month(longCol)),
      ("dayOfMonth", 0, () => new DayOfMonth(longCol)),
      ("quarter", 0, () => new Quarter(longCol)),
      ("dayOfYear", 0, () => new DayOfYear(longCol)),
      ("lastDay", 0, () => new LastDay(longCol)),
      ("truncDate", 0, () => new TruncDate(longCol, TruncLevel.MONTH)),
      ("truncDateDynamic", 0, () => new TruncDateDynamic(longCol, intCol)),
      ("truncDateDynamic", 1, () => new TruncDateDynamic(intCol, longCol)),
      ("weekOfYear", 0, () => new WeekOfYear(longCol)))
    refusals.foreach { case (what, position, build) =>
      val e = intercept[IllegalArgumentException](build())
      assert(e.getMessage.contains(what), s"$what operand $position: ${e.getMessage}")
      assert(e.getMessage.contains("LONG"), s"$what operand $position: ${e.getMessage}")
    }
    // Every node whose lane is INT by construction rather than derived from its operands takes
    // epoch days, so every one of them has to appear above.
    val covered = refusals.map(_._1.toLowerCase(Locale.ROOT)).toSet
    val calendarNodes = everyIntNode
      .filterNot(n => n.isInstanceOf[ColumnRef] || n.isInstanceOf[LiteralSlot])
      .filterNot(n => derivesItsLane.contains(n.getClass))
      .map(_.getClass.getSimpleName.toLowerCase(Locale.ROOT))
      .toSet
    assert(calendarNodes -- covered === Set.empty[String],
      "a node that takes epoch days has no refusal case")
  }

  test("a node whose operands disagree on their lane is refused when it is built") {
    // One node is emitted over one species: an add whose operands are different widths, or a
    // blend whose mask and values are, has no lowering. Widening is a conversion node's job.
    val refusals: Seq[(String, () => Any)] = Seq(
      "int arithmetic" -> (() => new IntArith(IntOp.ADD, Overflow.WRAP, intCol, longLit)),
      "int arithmetic" -> (() => new IntArith(IntOp.MUL, Overflow.NULL, longCol, intLit)),
      "a comparison" -> (() => new Compare(CompareOp.LT, longCol, intLit)),
      "greatest" -> (() => new Greatest(longCol, intCol)),
      "least" -> (() => new Least(intCol, longLit)),
      "and" -> (() => new And(new IsNotNull(intCol), new IsNotNull(longCol))),
      "or" -> (() => new Or(new IsNotNull(longCol), new IsNotNull(intCol))),
      "if" -> (() => new IfElse(new IsNotNull(intCol), longCol, longLit)),
      "if" -> (() => new IfElse(new IsNotNull(longCol), longCol, intLit)))
    refusals.foreach { case (what, build) =>
      val e = intercept[IllegalArgumentException](build())
      assert(e.getMessage.contains(what), e.getMessage)
      assert(e.getMessage.contains("mixes lanes"), e.getMessage)
    }
  }

  test("the int lane renders as nothing, so no shape hash committed before it moved") {
    // The rendering drives the emitted class's name and the committed hashes in
    // VarkaShapeCacheSuite, so the int lane has to be invisible in it - the same elision
    // VarkaEmitOptions makes for its defaults. A wider lane renders, and so gets its own name.
    assert(VarkaVectorIR.canonical(intCol) === "col:0")
    assert(VarkaVectorIR.canonical(intLit) === "lit:0")
    assert(VarkaVectorIR.canonical(longCol) === "col:0:long")
    assert(VarkaVectorIR.canonical(longLit) === "lit:0:long")
    val intKey = new VarkaShapeKey(Seq[VarkaVectorIR](intCol).asJava, 1, 0)
    val longKey = new VarkaShapeKey(Seq[VarkaVectorIR](longCol).asJava, 1, 0)
    assert(intKey !== longKey, "the lane is part of a shape's identity")
    assert(VarkaShapeCacheImpl.shapeHash(intKey) !== VarkaShapeCacheImpl.shapeHash(longKey))
  }

  test("every descriptor the lane derives is the one the Vector API declares") {
    // The emitter builds its descriptors from two facts - the vector class and the scalar type -
    // instead of writing one table per lane, so nothing but this test stands between a wrong
    // derivation and a class that fails verification at a lane the oracle cannot reach. The
    // expectations are written out by hand from the Vector API's own signatures, which is the
    // point: a derivation checked against itself would check nothing.
    val v = ClassDesc.of("jdk.incubator.vector.IntVector")
    val vector = ClassDesc.of("jdk.incubator.vector.Vector")
    val mask = ClassDesc.of("jdk.incubator.vector.VectorMask")
    val species = ClassDesc.of("jdk.incubator.vector.VectorSpecies")
    val segment = ClassDesc.of("java.lang.foreign.MemorySegment")
    val order = ClassDesc.of("java.nio.ByteOrder")
    val binary = ClassDesc.of("jdk.incubator.vector.VectorOperators$Binary")
    val comparison = ClassDesc.of("jdk.incubator.vector.VectorOperators$Comparison")
    val int = ConstantDescs.CD_int
    val lane = VarkaLoopEmitter.Lane.INT

    assert(lane.vector === v)
    assert(lane.bits === 32)
    assert(lane.byteStride === 4L, "the int lane is four bytes wide")
    assert(lane.broadcast === MethodTypeDesc.of(v, species, int))
    assert(lane.fromMemorySegmentDense ===
      MethodTypeDesc.of(v, species, segment, ConstantDescs.CD_long, order))
    assert(lane.fromMemorySegmentMasked ===
      MethodTypeDesc.of(v, species, segment, ConstantDescs.CD_long, order, mask))
    assert(lane.intoMemorySegmentDense ===
      MethodTypeDesc.of(ConstantDescs.CD_void, segment, ConstantDescs.CD_long, order))
    assert(lane.intoMemorySegmentMasked ===
      MethodTypeDesc.of(ConstantDescs.CD_void, segment, ConstantDescs.CD_long, order, mask))
    assert(lane.lanewiseVV === MethodTypeDesc.of(v, vector), "the parameter is the erased Vector")
    assert(lane.lanewiseVVWrong === MethodTypeDesc.of(v, v), "the misdescribe hook's wrong shape")
    assert(lane.lanewiseVI === MethodTypeDesc.of(v, int))
    assert(lane.lanewiseVIMasked === MethodTypeDesc.of(v, int, mask))
    assert(lane.lanewiseBinaryV === MethodTypeDesc.of(v, binary, vector))
    assert(lane.lanewiseBinaryI === MethodTypeDesc.of(v, binary, int))
    assert(lane.compareVV === MethodTypeDesc.of(mask, comparison, vector))
    assert(lane.compareVI === MethodTypeDesc.of(mask, comparison, int))
    assert(lane.blend === MethodTypeDesc.of(v, vector, mask))
  }

  test("the species constant a lane names follows its own width") {
    // Sixteen int lanes is 512 bits; the same count at a wider lane would name a wider species,
    // which is why the name is the lane's business and not a shared helper's.
    val lane = VarkaLoopEmitter.Lane.INT
    assert(lane.speciesField(0) === "SPECIES_PREFERRED")
    assert(lane.speciesField(2) === "SPECIES_64")
    assert(lane.speciesField(4) === "SPECIES_128")
    assert(lane.speciesField(8) === "SPECIES_256")
    assert(lane.speciesField(16) === "SPECIES_512")
  }

  test("the emitter refuses a lane it cannot emit, naming it") {
    // Steps 3 and 4 of task 85 give the emitter a lane descriptor; until then the analysis
    // pass is where a long tree stops, and this test is what tells the difference between
    // "refused" and "emitted as int by accident".
    val e = intercept[IllegalArgumentException] {
      VarkaLoopEmitter.emit("VarkaLaneTypeSuiteKernel", Seq[VarkaVectorIR](longCol).asJava,
        1, 0, null, null, VarkaEmitOptions.DEFAULTS)
    }
    assert(e.getMessage === "unsupported lane type LONG", e.getMessage)
  }
}
