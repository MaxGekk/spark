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
    val refusals: Seq[(String, () => Any)] = Seq(
      "year" -> (() => new Year(longCol)),
      "month" -> (() => new Month(longCol)),
      "truncDate" -> (() => new TruncDate(longCol, TruncLevel.MONTH)),
      "truncDateDynamic" -> (() => new TruncDateDynamic(longCol, intCol)),
      "addDays" -> (() => new AddDays(longCol, intLit)),
      "addDays" -> (() => new AddDays(intCol, longLit)),
      "subDays" -> (() => new SubDays(longCol, intLit)),
      "dateDiff" -> (() => new DateDiff(longCol, intCol)),
      "nextDay" -> (() => new NextDay(longCol, intLit)),
      "addMonths" -> (() => new AddMonths(intCol, longLit)),
      "makeDate" -> (() => new MakeDate(intCol, longLit, intLit, false)),
      "guardedDay" -> (() => new GuardedDay(longCol)),
      "dayOfWeek" -> (() => new DayOfWeek(longCol)),
      "thursdayOf" -> (() => new ThursdayOf(longCol)),
      "weekOfYear" -> (() => new WeekOfYear(longCol)))
    refusals.foreach { case (what, build) =>
      val e = intercept[IllegalArgumentException](build())
      assert(e.getMessage.contains(what), e.getMessage)
      assert(e.getMessage.contains("LONG"), e.getMessage)
    }
  }

  test("a node whose operands disagree on their lane is refused when it is built") {
    // One node is emitted over one species: an add whose operands are different widths, or a
    // blend whose mask and values are, has no lowering. Widening is a conversion node's job.
    val refusals: Seq[(String, () => Any)] = Seq(
      "int:ADD" -> (() => new IntArith(IntOp.ADD, Overflow.WRAP, intCol, longLit)),
      "int:MUL" -> (() => new IntArith(IntOp.MUL, Overflow.NULL, longCol, intLit)),
      "cmp:LT" -> (() => new Compare(CompareOp.LT, longCol, intLit)),
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
