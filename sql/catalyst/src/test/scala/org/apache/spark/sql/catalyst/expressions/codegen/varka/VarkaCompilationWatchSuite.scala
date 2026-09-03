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

/**
 * Task 50: the compiled-size watch.
 *
 * Everything here except the last case runs without JFR compiling anything, by driving
 * `VarkaCompilationWatch.record` directly - the seam the JFR handler calls once it has read the
 * event's fields. That split is deliberate: the parts that can be wrong in an interesting way
 * are the key and the threshold, and neither needs a running compiler to check, while a test
 * that waits for C2 is exactly the kind that goes flaky on a loaded runner.
 */
class VarkaCompilationWatchSuite extends SparkFunSuite {

  private val prefix = VarkaShapeCacheImpl.CLASS_NAME_PREFIX
  private val shapeA = "0123456789abcdef"
  private val shapeB = "fedcba9876543210"
  private def classOf(shape: String): String = prefix + shape

  test("the key names the method and the tier, not just the shape") {
    // The correction in PLAN_TASK_50.md 2.1. A shape emits run, runDense, loopDense0,
    // epilogueMasked and more, whose compiled sizes differ by an order of magnitude - measured
    // on a probe at 576 bytes against 10552 for two methods of one class. Keyed on the shape
    // alone, the second method compiled would be reported as a divergence and the detector
    // would fire constantly on a healthy JVM.
    val a = VarkaCompilationWatch.keyFor(classOf(shapeA), "loopDense0", 4)
    assert(a !== VarkaCompilationWatch.keyFor(classOf(shapeA), "epilogueMasked", 4),
      "two methods of one shape must not share a key")
    assert(a !== VarkaCompilationWatch.keyFor(classOf(shapeA), "loopDense0", 3),
      "two tiers of one method must not share a key - the probe measured 576 bytes at tier 3 " +
        "against 696 at tier 4 for the same method")
    assert(a !== VarkaCompilationWatch.keyFor(classOf(shapeB), "loopDense0", 4),
      "two shapes must not share a key")
    assert(a === VarkaCompilationWatch.keyFor(classOf(shapeA), "loopDense0", 4),
      "the same method, shape and tier must key the same")
  }

  test("a class that is not a generated kernel is not keyed") {
    // The filter that keeps jdk.Compilation's firehose out. Every method the JVM compiles
    // reaches the handler; only Varka's generated classes may get past this.
    assert(VarkaCompilationWatch.keyFor("java.util.HashMap", "put", 4) === null)
    assert(VarkaCompilationWatch.keyFor(
      "org.apache.spark.sql.varka.vector.ChronoVectorOps", "vectorFourFields", 4) === null,
      "the hand-written kernels are not shape-keyed, so they have no baseline to compare against")
    assert(VarkaCompilationWatch.keyFor(prefix, "run", 4) === null,
      "the prefix with no shape hash after it is not a kernel class")
    assert(VarkaCompilationWatch.keyFor(null, "run", 4) === null)
    assert(VarkaCompilationWatch.keyFor(classOf(shapeA), null, 4) === null)
  }

  test("the first compilation is a baseline, not a divergence") {
    val watch = VarkaCompilationWatch.inert()
    watch.record(classOf(shapeA), "loopDense0", 4, 1600L)
    assert(watch.divergenceCount() === 0, "the first size seen has nothing to be compared with")
    assert(watch.observedCount() === 1)
  }

  test("a size within the threshold is not reported, one beyond it is") {
    val watch = VarkaCompilationWatch.inert()
    watch.record(classOf(shapeA), "loopDense0", 4, 1600L)
    watch.record(classOf(shapeA), "loopDense0", 4, 1700L)
    assert(watch.divergenceCount() === 0,
      s"a 6% difference is inside the ${VarkaCompilationWatch.DIVERGENCE_RATIO} threshold")
    // Task 32's actual failure: 1581 instructions against 3000, about 2x, and worth 30-40%.
    watch.record(classOf(shapeA), "loopDense0", 4, 3000L)
    assert(watch.divergenceCount() === 1, "a 2x difference is the case this exists to catch")
  }

  test("a smaller later compilation counts too") {
    // The baseline is whichever came first, and the bad allocation is not guaranteed to be it -
    // task 32 saw the fast outcome 4 times in 21 runs, so the first compilation of a JVM is more
    // likely to be the slow one. The comparison is on absolute difference for that reason.
    val watch = VarkaCompilationWatch.inert()
    watch.record(classOf(shapeA), "loopDense0", 4, 3000L)
    watch.record(classOf(shapeA), "loopDense0", 4, 1581L)
    assert(watch.divergenceCount() === 1)
  }

  test("a second method of the same shape establishes its own baseline") {
    // The regression PLAN_TASK_50.md 2.1 exists to prevent, asserted rather than argued.
    val watch = VarkaCompilationWatch.inert()
    watch.record(classOf(shapeA), "run", 4, 271L)
    watch.record(classOf(shapeA), "loopDense0", 4, 3000L)
    watch.record(classOf(shapeA), "epilogueMasked", 4, 9000L)
    assert(watch.divergenceCount() === 0,
      "three methods of one shape, all first compilations - nothing has diverged")
    assert(watch.observedCount() === 3)
  }

  test("a repeatedly diverging method is counted every time and logged once") {
    val watch = VarkaCompilationWatch.inert()
    watch.record(classOf(shapeA), "loopDense0", 4, 1600L)
    (1 to 5).foreach(_ => watch.record(classOf(shapeA), "loopDense0", 4, 3000L))
    assert(watch.divergenceCount() === 5,
      "the counter keeps counting; it is the log that speaks once, so a recompiling loop " +
        "cannot flood it")
  }

  test("a nonsense size is ignored rather than treated as a baseline") {
    val watch = VarkaCompilationWatch.inert()
    watch.record(classOf(shapeA), "loopDense0", 4, 0L)
    watch.record(classOf(shapeA), "loopDense0", 4, 1600L)
    watch.record(classOf(shapeA), "loopDense0", 4, 1650L)
    assert(watch.divergenceCount() === 0,
      "a zero size must not become the baseline every later compilation is measured against")
  }

  test("the watch JFR could not start is inert and safe") {
    // What VarkaCompilationWatch.start() returns when the stream cannot be opened - JFR
    // disabled, or a stripped runtime. A diagnostic must never be why a JVM fails to start, so
    // every method has to stay callable.
    val watch = VarkaCompilationWatch.inert()
    assert(!watch.isRunning())
    watch.record(classOf(shapeA), "loopDense0", 4, 1600L)
    watch.record(classOf(shapeA), "loopDense0", 4, 3000L)
    assert(watch.divergenceCount() === 1, "the decision path works with no stream behind it")
    watch.close()
  }

  test("the watch is off unless it is asked for") {
    // The default, and the whole cost of this feature to anyone who has not enabled it: no
    // stream, no thread, no map. Read through the singleton rather than the class, because
    // "off" is a decision VarkaShapeCache makes, not one the watch makes about itself.
    assert(!VarkaShapeCache.compilationWatchRunning(),
      "spark.sql.codegen.varka.compilationWatch.enabled defaults to false")
    assert(VarkaShapeCache.compilationDivergences() === 0)
  }
}
