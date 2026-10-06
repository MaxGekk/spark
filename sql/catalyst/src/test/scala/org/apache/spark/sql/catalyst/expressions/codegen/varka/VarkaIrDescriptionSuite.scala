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
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaIrGrammar._
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.{AddDays, ColumnRef, LiteralSlot}

/**
 * The description of an IR graph (`VarkaIrDescription`, VARKA-291): that it rebuilds into records
 * equal to the ones it was read from, through its text as well, for every shape of the emit-cost
 * corpus and every draw of the grammar, and that together they reach all of the IR's kinds, so
 * that the spike's arms are built from a description that loses none of them.
 */
class VarkaIrDescriptionSuite extends SparkFunSuite {

  private def kindsOf(graph: VarkaIrDescription.Graph): Set[String] =
    graph.nodes.asScala.map(_.kind).toSet

  test("every shape of the emit-cost corpus rebuilds into equal records, through its text") {
    // Catches: a component the reflection reads wrongly (a lane, a long, a list), and a text form
    // that loses a scalar, which the spike's arms would then build their graphs without.
    val shapes = VarkaIrLayoutExport.corpus().asScala
    assert(shapes.nonEmpty)
    shapes.foreach { s =>
      VarkaIrLayoutExport.describeChecked(
        VarkaIrLayoutExport.nameOf(s), s.roots, s.numInputs, s.numLiterals)
    }
  }

  test("the grammar's draws rebuild into equal records and, with the corpus, reach every kind") {
    // Catches: an IR kind the description cannot hold - one added without a scalar type the
    // reflection handles - and a kind no graph the spike reads contains, which it would never see.
    // The corpus alone reaches every kind today; the draws round-trip more shapes besides, and
    // stand in for the corpus if it is ever narrowed.
    val reached = scala.collection.mutable.Set.empty[String]
    def add(name: String, roots: Seq[VarkaVectorIR], inputs: Int, literals: Int): Unit =
      reached ++= kindsOf(
        VarkaIrLayoutExport.describeChecked(name, roots.asJava, inputs, literals))
    VarkaIrLayoutExport.corpus().asScala.foreach { s =>
      reached ++= kindsOf(VarkaIrDescription.describe(VarkaIrLayoutExport.nameOf(s), s.roots,
        s.numInputs, s.numLiterals))
    }
    (0 until 400).foreach { k =>
      val d = drawShape(shapeRandom(fuzzSeed, k))
      add(s"draw-$k", d.roots, d.numInputs, d.numLiterals)
      val l = drawLongShape(shapeRandom(longFuzzSeed, k))
      add(s"long-$k", l.roots, l.numInputs, l.numLiterals)
    }
    (0 until 20).foreach { k =>
      val d = drawWideShape(shapeRandom(fuzzSeed, k))
      add(s"wide-$k", d.roots, d.numInputs, d.numLiterals)
      val l = drawWideLongShape(shapeRandom(longFuzzSeed, k))
      add(s"widelong-$k", l.roots, l.numInputs, l.numLiterals)
    }
    val missing = VarkaIrDescription.allKinds().asScala.filterNot(reached)
    assert(missing.isEmpty, s"no graph contains: ${missing.mkString(", ")}")
    assert(VarkaIrDescription.allKinds().size === 37)
  }

  test("equal subtrees are one node, each before its parents") {
    // Catches: a description that repeats a shared subtree, which the flat arms would then count
    // as separate nodes against the records' shared ones.
    val col = new ColumnRef(0)
    val shifted = new AddDays(col, new LiteralSlot(0))
    val graph = VarkaIrDescription.describe("shared", java.util.List.of(
      new AddDays(shifted, new LiteralSlot(1)), new AddDays(shifted, new LiteralSlot(2))), 1, 3)
    // The column, three literals, the shifted date and the two sums: seven, not eleven.
    assert(graph.nodes.size === 7)
    assert(graph.roots.size === 2)
    graph.nodes.asScala.zipWithIndex.foreach { case (node, id) =>
      assert(node.children.asScala.forall(_ < id))
    }
  }

  test("a description that cannot be built is refused, naming why") {
    // Catches: a rebuild that guesses at a kind it does not know, or at a child that is not built.
    val unknown = intercept[IllegalArgumentException] {
      VarkaIrDescription.parse("graph g inputs 1 literals 0 roots 0\n0 NoSuchKind |\n")
        .forEach(g => VarkaIrDescription.rebuild(g))
    }
    assert(unknown.getMessage.contains("NoSuchKind"))
    val forward = intercept[IllegalArgumentException] {
      VarkaIrDescription.parse(
        "graph g inputs 1 literals 0 roots 0\n0 AddDays | 1 1\n1 ColumnRef 0 INT |\n")
    }
    assert(forward.getMessage.contains("does not come before"))
  }
}
