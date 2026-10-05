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

import org.apache.spark.SparkFunSuite

/**
 * Runs each test of a Varka suite through [[VarkaMatrix.around]]: under the option matrix, a test
 * the skip list expects a configuration to break is cancelled with the entry's reason, or fails
 * as a stale entry when it passes, and each test's fused batches are recorded for the runner.
 * A test tagged `VarkaMatrix.PinsDefaults` is cancelled under any configuration. Outside the
 * matrix it changes nothing. `VarkaTestWatchdog` extends it; a suite that runs
 * without the watchdog, such as a fuzzer whose tests outlast its cap, mixes in this alone.
 */
trait VarkaMatrixTests extends SparkFunSuite {

  override protected def test(testName: String, testTags: Tag*)(testBody: => Any)
      (implicit pos: Position): Unit = {
    if (VarkaMatrix.config.nonEmpty && testTags.contains(VarkaMatrix.PinsDefaults)) {
      super.test(testName, testTags: _*)(
        cancel(s"pins the defaults' structure; not run under ${VarkaMatrix.config}"))
    } else {
      super.test(testName, testTags: _*)(VarkaMatrix.around(suiteName, testName)(testBody))
    }
  }
}
