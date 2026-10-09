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

import org.apache.arrow.memory.RootAllocator

import org.apache.spark.SparkFunSuite

/**
 * The sanitizer's window, driven directly so that the suite does not need the system property
 * that turns the sanitizer on. Each test names the mistake in the sanitizer it would catch.
 */
class VarkaMemorySanitizerSuite extends SparkFunSuite {

  private val Base = 0x10000L

  /** A window with one 64-byte "input data" buffer at [[Base]] and a second at Base + 256. */
  private def window(): VarkaMemorySanitizer.Window = {
    val w = new VarkaMemorySanitizer.Window
    w.add("input data", 0, Base, 64L)
    w.add("output data", 1, Base + 256L, 32L)
    w
  }

  test("a mapping inside a buffer passes, from its first byte to its last") {
    // Catches: an off-by-one that rejects a mapping ending exactly at the buffer's end, which is
    // what every kernel's last lane group does.
    val w = window()
    w.check(Base, 64L)
    w.check(Base + 60L, 4L)
    w.check(Base + 256L, 32L)
  }

  test("a mapping one byte past a buffer fails, naming the nearest buffer") {
    // Catches: comparing against the size the kernel claims instead of the capacity registered,
    // which is the whole point of the check.
    val e = intercept[VarkaMemoryViolation] { window().check(Base, 65L) }
    assert(e.getMessage.contains("input data 0"), e.getMessage)
    assert(e.getMessage.contains("65 bytes"), e.getMessage)
  }

  test("a mapping that starts before a buffer, or beyond every one, fails") {
    // Catches: checking only the end of the mapping.
    intercept[VarkaMemoryViolation] { window().check(Base - 4L, 8L) }
    val e = intercept[VarkaMemoryViolation] { window().check(Base + 4096L, 4L) }
    assert(e.getMessage.contains("output data 1"), "the nearest buffer is named: " + e.getMessage)
  }

  test("a mapping that spans two adjacent buffers fails: one buffer must cover all of it") {
    // Catches: treating the registered ranges as one merged region, which would let a kernel read
    // across the end of one buffer into whatever the allocator put next.
    val w = new VarkaMemorySanitizer.Window
    w.add("input data", 0, Base, 64L)
    w.add("input data", 1, Base + 64L, 64L)
    intercept[VarkaMemoryViolation] { w.check(Base + 32L, 64L) }
  }

  test("an empty mapping passes anywhere and a non-empty one at the null address fails") {
    // Catches: failing the all-null column's validity, which the runner passes as address 0.
    val w = window()
    w.check(0L, 0L)
    w.check(Base + 1000L, 0L)
    val e = intercept[VarkaMemoryViolation] { w.check(0L, 8L) }
    assert(e.getMessage.contains("null address"), e.getMessage)
  }

  test("a negative size and a mapping that wraps the address space fail") {
    // Catches: an overflow in address + bytes that makes the end smaller than the start and so
    // passes the containment test.
    intercept[VarkaMemoryViolation] { window().check(Base, -1L) }
    intercept[VarkaMemoryViolation] { window().check(Long.MaxValue - 3L, 8L) }
  }

  test("an empty window fails every non-empty mapping, saying nothing is registered") {
    // Catches: a window opened and never registered into, which would otherwise pass everything.
    val e = intercept[VarkaMemoryViolation] { new VarkaMemorySanitizer.Window().check(Base, 4L) }
    assert(e.getMessage.contains("0 buffers are registered"), e.getMessage)
  }

  test("a canary past a guarded buffer is intact until something writes past the nominal end") {
    // Catches: a canary that is written and never read back, and one written at the wrong offset,
    // which would pass a kernel that overran by exactly the bytes it covers.
    val allocator = new RootAllocator()
    val buf = allocator.buffer(64L)
    try {
      val w = new VarkaMemorySanitizer.Window
      w.add("output data", 2, buf.memoryAddress(), 48L)
      w.guard("output data", 2, buf, 48L)
      w.verifyCanaries()
      buf.setByte(47L, 1)
      w.verifyCanaries()
      buf.setByte(48L + VarkaMemorySanitizer.CANARY_BYTES - 1, 0)
      val e = intercept[VarkaMemoryViolation] { w.verifyCanaries() }
      assert(e.getMessage.contains("output data 2"), e.getMessage)
      assert(e.getMessage.contains("48 bytes"), e.getMessage)
    } finally {
      buf.close()
      allocator.close()
    }
  }

  test("a window with no guarded buffer verifies nothing and passes") {
    new VarkaMemorySanitizer.Window().verifyCanaries()
  }

  test("the catalyst harness runs its kernels inside the sanitizer's window") {
    // Catches: the fuzzers' kernel runs, which have no evaluator to open a window, being checked
    // against nothing, which a suite that passes under the sanitizer cannot tell from one that is
    // checked: the count of mappings checked has to rise.
    assume(VarkaMemorySanitizer.ENABLED, "run with -Dvarka.sanitizeMemory=true")
    val roots = Seq[VarkaVectorIR](new VarkaVectorIR.IntArith(VarkaVectorIR.IntOp.ADD,
      VarkaVectorIR.Overflow.WRAP, new VarkaVectorIR.ColumnRef(0, VarkaVectorIR.LaneType.INT),
      new VarkaVectorIR.LiteralSlot(0, VarkaVectorIR.LaneType.INT)))
    val className = "org.apache.spark.sql.varka.execution.VarkaSanitizerHarnessProbe"
    // Under the option matrix's options, as every other kernel of the JVM is: a configuration
    // that pins the lane count would otherwise mix the preferred width into the JVM (VARKA-246).
    val bytes = VarkaLoopEmitter.emit(className, roots.asJava, 1, 1, null, null, VarkaMatrix.base)
    val length = 100
    val before = VarkaMemorySanitizer.checked()
    VarkaKernelCheck.runAndCompare("sanitizer harness probe", className, bytes, roots, 1,
      Array(5), VarkaKernelCheck.Batch(length, Seq(_ => false),
        Array(Array.tabulate(length)(identity)), forceMasked = false))
    assert(VarkaMemorySanitizer.checked() > before,
      "the harness's kernel mapped nothing in a window: the sanitizer did not reach it")
  }

  test("the facade checks inside a window only when the sanitizer is on, never outside one") {
    // Catches: a check that runs without the flag, or one that fails a thread nothing began a
    // window on (the warm-up's thread, a unit test calling a kernel directly).
    VarkaMemorySanitizer.begin()
    VarkaMemorySanitizer.register("input data", 0, Base, 64L)
    if (VarkaMemorySanitizer.ENABLED) {
      intercept[VarkaMemoryViolation] { VarkaMemorySanitizer.check(Base + 4096L, 4L) }
    } else {
      VarkaMemorySanitizer.check(Base + 4096L, 4L)
    }
    VarkaMemorySanitizer.end()
    VarkaMemorySanitizer.check(Base + 4096L, 4L)
  }
}
