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

import org.scalactic.source.Position
import org.scalatest.Tag

/**
 * A cap on each test of a Varka suite that ends a hung test by name: the ScalaTest face of
 * [[VarkaWatchdog]], whose doc says how it works and why (`m6/PLAN.md` row 226, VARKA-312).
 * A suite whose tests are meant to run longer overrides `watchdogMinutes`.
 *
 * It extends [[VarkaMatrixTests]], so every test also runs under the option matrix's skip list.
 */
trait VarkaTestWatchdog extends VarkaMatrixTests {

  /** Minutes a test may run before the watchdog acts. */
  protected def watchdogMinutes: Long = VarkaWatchdog.defaultMinutes()

  override protected def test(testName: String, testTags: Tag*)(testBody: => Any)
      (implicit pos: Position): Unit = {
    super.test(testName, testTags: _*)(
      VarkaTestWatchdog.guard(suiteName, testName, watchdogMinutes)(testBody))
  }
}

object VarkaTestWatchdog {

  /** [[VarkaWatchdog.HALT_STATUS]]: the exit status of a JVM the watchdog halted. */
  val HALT_STATUS: Int = VarkaWatchdog.HALT_STATUS

  /** [[VarkaWatchdog.guard]]: `body` under a watchdog of `minutes`, and thirty seconds of grace. */
  def guard[T](suite: String, test: String, minutes: Long)(body: => T): T =
    VarkaWatchdog.guard(suite, test, minutes, () => body)

  /** [[VarkaWatchdog.guard]] with its clock and effects as parameters, for its tests. */
  private[varka] def guard[T](
      suite: String,
      test: String,
      millis: Long,
      graceMillis: Long,
      report: String => Unit,
      halt: Int => Unit)(body: => T): T =
    VarkaWatchdog.guard(suite, test, millis, graceMillis, (m: String) => report(m),
      (status: Int) => halt(status), () => body)
}
