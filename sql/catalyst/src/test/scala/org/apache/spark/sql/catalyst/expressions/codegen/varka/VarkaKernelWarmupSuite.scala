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
import java.lang.management.ManagementFactory
import javax.management.ObjectName

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelWarmth.State
import org.apache.spark.util.Utils

/**
 * The kernel warm-up (`PLAN_TASK_212.md` 10): the copy of a batch it runs on, its verdict on a
 * real emitted kernel, its release when the shape leaves the cache, and the compiler directive
 * that keeps C1 off the kernel classes so the verdict can arrive at all.
 */
class VarkaKernelWarmupSuite extends SparkFunSuite {

  /** A shape with enough calendar work that its uncompiled calls allocate unmistakably. */
  private def shape: VarkaShapeKey = {
    val d = new VarkaVectorIR.ColumnRef(0)
    val k = new VarkaVectorIR.LiteralSlot(0)
    val roots = java.util.List.of[VarkaVectorIR](
      new VarkaVectorIR.AddMonths(d, k), new VarkaVectorIR.LastDay(d),
      new VarkaVectorIR.AddDays(d, k))
    new VarkaShapeKey(roots, 1, 1)
  }

  /** `rows` dates around 2020, every seventh one null, in memory the arena owns. */
  private def dates(arena: Arena, rows: Int): (MemorySegment, MemorySegment, Int) = {
    val data = arena.allocate(rows * 4L, 64)
    val validity = arena.allocate(((rows + 63) / 64) * 8L, 64)
    var nulls = 0
    (0 until rows).foreach { r =>
      data.setAtIndex(ValueLayout.JAVA_INT, r, 18262 + r % 1460)
      if (r % 7 == 3) {
        nulls += 1
      } else {
        val b = r / 8
        validity.set(ValueLayout.JAVA_BYTE, b,
          (validity.get(ValueLayout.JAVA_BYTE, b) | (1 << (r % 8))).toByte)
      }
    }
    (data, validity, nulls)
  }

  /** Queues a warm-up of the cache's kernel for `shape` on a batch of `rows` dates. */
  private def warm(cache: VarkaShapeCacheImpl, rows: Int): VarkaShapeEntry = {
    val entry = cache.getOrEmit(Utils.getContextOrSparkClassLoader, shape, "warmup-suite").entry
    assert(entry.warmth().tryClaim())
    val arena = Arena.ofConfined()
    try {
      val (data, validity, nulls) = dates(arena, rows)
      val queued = VarkaKernelWarmup.start(entry.warmth(), entry.shapeHash(), entry.newKernel(),
        false, Array(data.address()), Array(validity.address()), Array(nulls), Array(4), rows,
        3, Array(2), Array.emptyLongArray)
      assert(queued, "the warm-up was not queued")
    } finally {
      // The warm-up copies the batch before start returns, so the batch can go at once.
      arena.close()
    }
    entry
  }

  test("the copy repeats a short batch's rows and validity bits, bit offsets included") {
    val arena = Arena.ofConfined()
    try {
      val rows = 5
      val (data, validity, _) = dates(arena, rows)
      val dataCopy = arena.allocate(VarkaKernelWarmup.SNAPSHOT_ROWS * 4L)
      val validityCopy = arena.allocate(VarkaKernelWarmup.SNAPSHOT_ROWS / 8L)
      VarkaKernelWarmup.tileData(data.address(), 4, rows, dataCopy)
      VarkaKernelWarmup.tileValidity(validity.address(), rows, validityCopy)
      (0 until VarkaKernelWarmup.SNAPSHOT_ROWS).foreach { r =>
        val s = r % rows
        assert(dataCopy.getAtIndex(ValueLayout.JAVA_INT, r) ===
          data.getAtIndex(ValueLayout.JAVA_INT, s), s"row $r")
        val bit = (validityCopy.get(ValueLayout.JAVA_BYTE, r / 8) >> (r % 8)) & 1
        assert(bit === (if (s % 7 == 3) 0 else 1), s"validity of row $r")
      }
    } finally {
      arena.close()
    }
  }

  test("a warm-up runs a new kernel until it no longer allocates, and says it is compiled") {
    assume(VarkaAllocationSampler.supported(), "thread allocation accounting unavailable")
    val cache = new VarkaShapeCacheImpl(8)
    val entry = warm(cache, 10000)
    assert(VarkaKernelWarmup.awaitIdle(120000), "the warm-up did not finish in two minutes")
    val outcome = VarkaKernelWarmup.recentOutcomes().asScala.last
    assert(outcome.shapeHash() === entry.shapeHash())
    assert(outcome.state() === State.COMPILED, outcome)
    assert(entry.warmth().state() === State.COMPILED)
    assert(entry.warmth().ready())
    // The verdict's evidence: the first probe boxed, the last one did not.
    assert(outcome.lastProbeBytes() * VarkaKernelWarmup.COMPILED_DROP <= outcome.firstProbeBytes(),
      outcome)
  }

  test("a shape that leaves the cache stops its warm-up and is ready for its tasks") {
    assume(VarkaAllocationSampler.supported(), "thread allocation accounting unavailable")
    val cache = new VarkaShapeCacheImpl(8)
    val entry = warm(cache, 1000)
    cache.invalidateAll()
    assert(VarkaKernelWarmup.awaitIdle(120000), "the warm-up did not finish in two minutes")
    val outcome = VarkaKernelWarmup.recentOutcomes().asScala.last
    assert(outcome.shapeHash() === entry.shapeHash())
    // Released, unless the compile won the race with the invalidation - both leave the tasks
    // running the kernel, which is the property that matters.
    assert(entry.warmth().ready())
    assert(outcome.state() === entry.warmth().state())
  }

  test("a claim goes to exactly one caller, and handing it back lets another take it") {
    val warmth = new VarkaKernelWarmth
    assert(!warmth.ready())
    assert(warmth.tryClaim())
    assert(!warmth.tryClaim(), "a second claim while the first is warming")
    warmth.unclaim()
    assert(warmth.state() === State.COLD)
    assert(warmth.tryClaim())
    warmth.release()
    assert(warmth.ready() && warmth.state() === State.RELEASED)
    warmth.unclaim()
    assert(warmth.state() === State.RELEASED, "a released shape does not go back to cold")
  }

  test("emitting a kernel installs the directive that keeps C1 off the kernel classes") {
    new VarkaShapeCacheImpl(8).getOrEmit(Utils.getContextOrSparkClassLoader, shape, "directive")
    assume(VarkaKernelCompileDirective.installed(), "C2 is not this JVM's top tier")
    val directives = ManagementFactory.getPlatformMBeanServer.invoke(
      new ObjectName("com.sun.management:type=DiagnosticCommand"), "compilerDirectivesPrint",
      Array[AnyRef](Array.empty[String]), Array(classOf[Array[String]].getName)).toString
    assert(directives.contains(VarkaKernelCompileDirective.METHOD_PATTERN), directives)
  }
}
