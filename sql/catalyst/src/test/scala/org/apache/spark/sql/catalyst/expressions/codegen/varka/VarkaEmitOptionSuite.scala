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
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOption.Reason

/**
 * The options table (`VarkaEmitOption.TABLE`) against the record it describes: one entry per
 * component in declaration order, the defaults it builds, the renderings `canonical()` makes from
 * it, and the reasons it records.
 */
class VarkaEmitOptionSuite extends SparkFunSuite with VarkaTestWatchdog {

  private val table = VarkaEmitOption.TABLE.asScala.toSeq

  test("the table names every option of the record once, in declaration order") {
    val components = classOf[VarkaEmitOptions].getRecordComponents.map(_.getName).toSeq
    assert(table.map(_.name) === components)
  }

  test("the defaults hold every option at its table default") {
    val d = VarkaEmitOptions.DEFAULTS
    for (option <- table) {
      option match {
        case f: VarkaEmitOption.Flag => assert(f.value(d) === f.defaultValue, f.name)
        case c: VarkaEmitOption.Count => assert(c.value(d) === c.defaultValue, c.name)
        case ch: VarkaEmitOption.Choice[_] => assert(ch.value(d) === ch.defaultValue, ch.name)
      }
    }
    assert(d.canonical() === "" && d.isDefault)
  }

  test("canonical renders the positional options, then the tags that show") {
    // The renderings variants have always had: the 26 options that predate the tags by
    // position, then a tag for each later option on the values its entry names - so a flag on
    // by default still shows (`predictGrouping`, `planSize`, `elideUnreadLocals`), and one off
    // its default shows its opposite (`noRangeSets`).
    val positional = "opts(4|400|false|true|true|true|true|true|true|true|true|true|true|0|" +
      "SUBTRACT|MAGIC|MAGIC|-1|false|false|true|true|false|true|false|8000"
    val onByDefault = "|predictGrouping|elideUnreadLocals|planSize"
    val noCse = VarkaEmitOptions.DEFAULTS.withCse(false)
      .withGroupBudget(4).withFusedCeiling(400)
    assert(noCse.canonical() === positional + onByDefault + ")")
    assert(noCse.withRangeSets(false).withCallSiteBudget(7).canonical() ===
      positional + "|noRangeSets|callSites=7" + onByDefault + ")")
  }

  test("the reasons the table records") {
    def reasonOf(name: String): Reason = VarkaEmitOption.named(name).reason
    for (name <- Seq("misdescribeAdd", "misdescribeWordLiveness", "misdescribeDriverBytes")) {
      assert(reasonOf(name) === Reason.FAULT_INJECTOR, name)
    }
    for (name <- Seq("division", "useAVX", "lanesOverride")) {
      assert(reasonOf(name) === Reason.MACHINE, name)
    }
    for (name <- Seq("checkIntOverflow", "guardDayProducers")) {
      assert(reasonOf(name) === Reason.PRICED_CHECK, name)
    }
    assert(table.filter(_.reason == Reason.KNOB).map(_.name) ===
      Seq("groupBudget", "fusedCeiling", "methodByteBudget", "callSiteBudget",
        "heavyGroupOutputs"))
  }

  test("an option is found by name, and an enum constant by its name or not at all") {
    def withDivision(constant: String): VarkaEmitOptions =
      VarkaEmitOption.named("division") match {
        case choice: VarkaEmitOption.Choice[_] =>
          choice.withNamed(VarkaEmitOptions.DEFAULTS, constant)
        case other => fail(s"division is not a choice: $other")
      }
    assert(withDivision("DOUBLE_DIV").division === VarkaEmitOptions.Division.DOUBLE_DIV)
    val unknown = intercept[IllegalArgumentException](withDivision("FAST"))
    assert(unknown.getMessage.contains("division has no constant FAST"))
    intercept[IllegalArgumentException](VarkaEmitOption.named("noSuchOption"))
  }

  test("every option the fuzzer may draw has values to draw, and no fault injector does") {
    for (option <- table) {
      option match {
        case c: VarkaEmitOption.Count if c.reason != Reason.FAULT_INJECTOR =>
          assert(!c.fuzzDraws.isEmpty, c.name)
        case c: VarkaEmitOption.Count => assert(c.fuzzDraws.isEmpty, c.name)
        case _ =>
      }
    }
  }
}
