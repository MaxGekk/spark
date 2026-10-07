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

package org.apache.spark.sql.execution

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.codegen.varka.{VarkaMemorySanitizer,
  VarkaMemoryViolation, VarkaTestWatchdog}
import org.apache.spark.sql.varka.vector.VarkaVectorSupport

/**
 * The memory sanitizer (VARKA-263) on the real evaluator path. These tests need the sanitizer on
 * (`-Dvarka.sanitizeMemory=true`) and are cancelled without it; the one that does not need it,
 * "the suites run with the sanitizer on", is what fails when a suite JVM was meant to have it and
 * does not, so that "the suites and the fuzzers run with it" is checked and not claimed.
 */
class VarkaMemorySanitizerEndToEndSuite
    extends QueryTest with VarkaSharedSessions with VarkaTestWatchdog {

  private def withSanitizer(body: => Unit): Unit = {
    assume(VarkaMemorySanitizer.ENABLED, "run with -Dvarka.sanitizeMemory=true")
    body
  }

  test("a Varka projection's kernel maps its buffers through the sanitizer") {
    withSanitizer {
      cacheDates(spark)
      val query = "SELECT date_add(d, 3) AS a, date_sub(d, 5) AS b FROM varka_dates ORDER BY a"
      val expected = spark.sql(query)
      cacheDates(varkaSpark)
      val before = VarkaMemorySanitizer.checked()
      val actual = varkaSpark.sql(query)
      assertFused(actual.queryExecution.executedPlan)
      checkAnswer(actual, expected)
      assertKernelsRan(actual.queryExecution.executedPlan)
      assert(VarkaMemorySanitizer.checked() > before,
        "the projection's kernel mapped nothing in a window: the sanitizer did not reach it")
    }
  }

  test("a mapping one byte past a registered buffer fails through the engine's ofAddress") {
    withSanitizer {
      val buf = org.apache.spark.sql.util.ArrowUtils.rootAllocator.buffer(64L)
      try {
        VarkaMemorySanitizer.begin()
        try {
          VarkaMemorySanitizer.register("input data", 7, buf)
          VarkaVectorSupport.ofAddress(buf.memoryAddress(), buf.capacity())
          val e = intercept[VarkaMemoryViolation] {
            VarkaVectorSupport.ofAddress(buf.memoryAddress(), buf.capacity() + 1)
          }
          assert(e.getMessage.contains("input data 7"), e.getMessage)
        } finally {
          VarkaMemorySanitizer.end()
        }
      } finally {
        buf.close()
      }
    }
  }

  test("a violation is not a catchable kernel failure") {
    // Catches: the ghost fallback turning a memory violation into a row-engine answer.
    val violation = new VarkaMemoryViolation("test")
    assert(!org.apache.spark.sql.execution.varka.VarkaKernelRunner.isCatchable(violation))
    assert(org.apache.spark.sql.execution.varka.VarkaKernelRunner.isCatchable(
      new IllegalStateException("an ordinary kernel failure")))
  }
}
