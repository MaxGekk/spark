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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR._

/**
 * The shrinker over synthetic failures (VARKA-277): a predicate on the case stands in for the
 * kernel, so nothing is emitted and the suite checks the reduction alone.
 */
class VarkaShrinkerSuite extends SparkFunSuite {

  private val sigA = VarkaFailureSignature("output mismatch", "output # row # differs (want #)")
  private val sigB = VarkaFailureSignature("declined batch", "the kernel declined the batch")

  private def contains(node: VarkaVectorIR, cls: Class[_]): Boolean =
    cls.isInstance(node) || VarkaVectorIR.childrenOf(node).exists(contains(_, cls))

  private def makeCase(
      roots: Seq[VarkaVectorIR],
      length: Int = 20,
      options: VarkaEmitOptions = VarkaEmitOptions.DEFAULTS,
      numInputs: Int = 2,
      smallOrdinal: Int = -1): VarkaFuzzCase = VarkaFuzzCase(
    LaneType.INT, roots, numInputs, Array(3L, 4L), length,
    Array.tabulate(numInputs, length)((c, i) => i % (c + 3) == 0),
    Array.tabulate(numInputs, length)((c, i) => (i * 7 + c).toLong),
    forceMasked = true, options, smallOrdinal, -1, "case")

  test("ddmin finds the culprits and no more, in about 2 log2 n runs for one") {
    val items = (0 until 100).toVector
    for (culprits <- Seq(Set(17), Set(3, 80), Set(5, 6, 7), Set(0), Set(99))) {
      var runs = 0
      val found = VarkaShrinker.ddmin(items) { xs =>
        runs += 1
        culprits.forall(xs.contains)
      }
      assert(found.toSet == culprits, s"culprits $culprits, found $found")
      if (culprits.size == 1) {
        assert(runs <= 2 * 7 + 6, s"one culprit among 100 took $runs runs")
      }
    }
  }

  test("a tree shrinks to the node that matters, with the batch and options reduced too") {
    val col0 = new ColumnRef(0)
    val col1 = new ColumnRef(1)
    val tree = new AddDays(new LastDay(new Greatest(col0, col1)),
      new IntNeg(Overflow.WRAP, new LiteralSlot(0)))
    val opts = VarkaEmitOptions.DEFAULTS.toBuilder.cse(false).groupBudget(7).build()
    val c = makeCase(Seq(tree, new Year(col1)), length = 40, options = opts)
    val fails = (x: VarkaFuzzCase) =>
      if (x.roots.exists(contains(_, classOf[LastDay])) && !x.options.cse()) Some(sigA) else None
    val shrunk = VarkaShrinker.shrink(c, sigA, fails)
    assert(shrunk.small.roots.size == 1)
    assert(shrunk.small.roots.head == new LastDay(col0), shrunk.small.roots.head)
    assert(VarkaFuzzCase.optionDelta(shrunk.small.options) == Seq("cse=false"),
      VarkaFuzzCase.optionDelta(shrunk.small.options))
    assert(shrunk.small.length == 1)
    assert(!shrunk.small.forceMasked)
    assert(shrunk.small.nulls.forall(_.forall(!_)))
    assert(shrunk.small.data.forall(_.forall(_ == 0L)), VarkaFuzzCase.describe(shrunk.small))
    assert(!shrunk.stoppedEarly)
  }

  test("a reduction that fails differently is not smaller") {
    val col0 = new ColumnRef(0)
    val tree = new LastDay(new AddDays(col0, new IntNeg(Overflow.WRAP, new LiteralSlot(0))))
    // Below four nodes the case fails as a different bug: the shrinker has to stop at four.
    val fails = (x: VarkaFuzzCase) => x.roots.headOption.map(VarkaShrinker.size) match {
      case Some(n) if n >= 4 && contains(x.roots.head, classOf[LastDay]) => Some(sigA)
      case Some(_) if contains(x.roots.head, classOf[LastDay]) => Some(sigB)
      case _ => None
    }
    val shrunk = VarkaShrinker.shrink(makeCase(Seq(tree)), sigA, fails)
    assert(VarkaShrinker.size(shrunk.small.roots.head) == 4, shrunk.small.roots.head)
  }

  test("an operand whose domain the grammar constrains is left alone") {
    val months = new ColumnRef(1)
    val tree = new AddMonths(new ColumnRef(0), months)
    val fails = (x: VarkaFuzzCase) =>
      if (x.roots.exists(contains(_, classOf[AddMonths]))) Some(sigA) else None
    val shrunk = VarkaShrinker.shrink(makeCase(Seq(tree), smallOrdinal = 1), sigA, fails)
    assert(shrunk.small.roots.head == tree, shrunk.small.roots.head)
  }

  test("the run and time budgets stop a shrink and it returns what it has") {
    val tree = new LastDay(new AddDays(new ColumnRef(0), new LiteralSlot(0)))
    val fails = (x: VarkaFuzzCase) =>
      if (x.roots.exists(contains(_, classOf[LastDay]))) Some(sigA) else None
    val shrunk = VarkaShrinker.shrink(makeCase(Seq(tree)), sigA, fails, maxRuns = 2)
    assert(shrunk.stoppedEarly)
    assert(shrunk.runs <= 2)
  }

  test("a failure is grouped by kind and message with the numbers taken out") {
    val label = "seed=1 iteration=2 roots=[x]"
    def sig(m: String, t: Throwable = null): VarkaFailureSignature =
      VarkaFailureSignature.of(if (t != null) t else new RuntimeException(s"$label: $m"), label)
    assert(sig("output 0 row 7 differs (want 12)") == sig("output 1 row 3 differs (want -4)"))
    assert(sig("output 0 row 7 differs (want 12)").kind == "output mismatch")
    assert(sig("validity of output 0 row 7 differs (want None)").kind == "validity mismatch")
    assert(sig("selection row 7 differs (want true)").kind == "selection mismatch")
    assert(sig("the kernel declined the batch (status 2)").kind == "declined batch")
    assert(sig("x") != sig("output 0 row 7 differs (want 12)"))
    val linkage = new NoSuchMethodError("IntVector.add(IntVector)")
    assert(VarkaFailureSignature.of(linkage, label).kind == "generated class: NoSuchMethodError")
  }
}
