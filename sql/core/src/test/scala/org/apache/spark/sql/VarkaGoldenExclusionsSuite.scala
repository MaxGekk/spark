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

package org.apache.spark.sql

import java.util.Optional

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.VarkaGoldenExclusions.{Arm, Verdict}

/**
 * The golden-file suite's exclusion list - its parse, and what it makes of each outcome - and its
 * served report, which the suite reads back as a floor.
 */
class VarkaGoldenExclusionsSuite extends SparkFunSuite {

  private def parse(lines: String*): VarkaGoldenExclusions =
    VarkaGoldenExclusions.parse(lines.asJava)

  private val list = parse(
    "# a comment, and a blank line after it",
    "",
    "printed.sql\tboth\tharness\tprints a plan",
    "python.sql\tvarka\tenvironment\tneeds Python")

  private val failure: Optional[Throwable] = Optional.of(new RuntimeException("wrong answer"))
  private val passed: Optional[Throwable] = Optional.empty()

  test("the committed list parses, and lists only harness and environment tests") {
    val committed = VarkaGoldenExclusions.read(
      getWorkspaceFilePath("sql", "varka", "spark_suites", "excluded.tsv"))
    assert(committed.entries.size == 6)
    assert(committed.lookup("explain.sql", Arm.VARKA).isPresent)
    assert(committed.lookup("explain.sql", Arm.CACHE_ONLY).isPresent)
  }

  test("an entry applies only to the arms it names") {
    assert(list.lookup("printed.sql", Arm.CACHE_ONLY).isPresent)
    assert(list.lookup("python.sql", Arm.VARKA).isPresent)
    assert(list.lookup("python.sql", Arm.CACHE_ONLY).isEmpty)
    assert(list.lookup("other.sql", Arm.VARKA).isEmpty)
  }

  test("a malformed line fails with its line number") {
    def rejected(line: String): String =
      intercept[IllegalArgumentException](parse("# header", line)).getMessage
    assert(rejected("a.sql\tboth\tharness").contains("line 2: expected 4"))
    assert(rejected("a.sql\tall\tharness\twhy").contains("unknown arm 'all'"))
    assert(rejected("a.sql\tboth\tflaky\twhy").contains("unknown kind 'flaky'"))
    assert(rejected("a.sql\tboth\tharness\t ").contains("reason are required"))
    val twice = intercept[IllegalArgumentException](parse(
      "a.sql\tvarka\tharness\twhy", "a.sql\tboth\tharness\twhy")).getMessage
    assert(twice.contains("line 2: 'a.sql' is listed twice for the varka arm"))
  }

  test("a listed failure is cancelled with its reason, an unlisted one keeps its cause") {
    VarkaGoldenExclusions.decide(list.lookup("printed.sql", Arm.VARKA), failure) match {
      case c: Verdict.Cancel =>
        assert(c.reason == "listed in excluded.tsv (harness): prints a plan")
      case other => fail(s"expected a cancel, got $other")
    }
    VarkaGoldenExclusions.decide(list.lookup("other.sql", Arm.VARKA), failure) match {
      case f: Verdict.Fail => assert(f.cause.get eq failure.get)
      case other => fail(s"expected a failure, got $other")
    }
    assert(VarkaGoldenExclusions.decide(list.lookup("other.sql", Arm.VARKA), passed)
      .isInstanceOf[Verdict.Pass])
  }

  test("a listed harness test that passes is a stale entry; an environment one is not") {
    VarkaGoldenExclusions.decide(list.lookup("printed.sql", Arm.CACHE_ONLY), passed) match {
      case f: Verdict.Fail =>
        assert(f.message.startsWith("stale entry: 'printed.sql' passes"))
        assert(f.cause.isEmpty)
      case other => fail(s"expected a stale entry, got $other")
    }
    assert(VarkaGoldenExclusions.decide(list.lookup("python.sql", Arm.VARKA), passed)
      .isInstanceOf[Verdict.Pass])
  }

  test("the served report reads back what it writes, odd test names included") {
    val tests = Seq(
      "a.sql" -> VarkaArrowSource.Served(10, 3, 7),
      "udf/b.sql - Scala UDF" -> VarkaArrowSource.Served(4, 0, 0),
      "quote\"and\\slash.sql" -> VarkaArrowSource.Served(2, 1, 1))
    val path = java.nio.file.Files.createTempFile("served", ".json")
    try {
      java.nio.file.Files.writeString(path, VarkaSQLQueryTestSuite.renderServed(Arm.VARKA, tests))
      assert(VarkaSQLQueryTestSuite.readServed(path) ==
        tests.map { case (name, s) => name -> s.served }.toMap)
    } finally java.nio.file.Files.delete(path)
    val committed = VarkaSQLQueryTestSuite.readServed(
      getWorkspaceFilePath("sql", "varka", "spark_suites", "served.json"))
    assert(committed.size > 700 && committed.values.count(_ > 0) > 100)
  }
}
