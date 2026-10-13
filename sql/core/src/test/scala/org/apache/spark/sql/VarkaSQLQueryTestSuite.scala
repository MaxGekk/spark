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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import org.scalactic.source.Position
import org.scalatest.Tag
import org.scalatest.exceptions.TestCanceledException

import org.apache.spark.SparkConf
import org.apache.spark.sql.VarkaGoldenExclusions.{Arm, Verdict}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.{VarkaEmitOption,
  VarkaEmitOptions}
import org.apache.spark.sql.execution.VarkaColumnarToRowExec

/**
 * Spark's golden-file suite with its queries fed to Varka, Spark's committed `.sql.out` files
 * the oracle (VARKA-271, `sql/varka/plans/m7/VARKA-271.md` 3.2).
 *
 * Every golden file runs as [[SQLQueryTestSuite]] runs it, through [[VarkaArrowSource]], in one
 * of two arms the property `varka.goldenSuite.arm` selects:
 *  - `varka`: Varka on. Its failures are Varka's or the harness's;
 *  - `cacheOnly`: the same Arrow source with Varka off. Its failures are the harness's alone.
 * A test that fails with Varka on and passes with it off is a Varka failure. Without the
 * property the suite runs nothing and starts no session: the cache serializer it sets is one
 * process-wide field, fixed at first use, so the suite must never share a JVM with suites that
 * cache under another serializer (`dev/varka_golden_suite.sh` gives it its own).
 *
 * A test listed in `sql/varka/spark_suites/excluded.tsv` for the arm is cancelled when it fails
 * and, for a harness entry, fails as stale when it passes ([[VarkaGoldenExclusions]]).
 *
 * After each test the suite records how many of its query executions Varka served a batch in,
 * the served report, so a test that passes without reaching Varka is visible. The report goes
 * to `varka.goldenSuite.servedOut` when that is set, and `VARKA_GOLDEN_SUITE_REGEN=true`
 * rewrites the committed `sql/varka/spark_suites/served.json` from a full run.
 *
 * `varka.goldenSuite.fault=misdescribeAdd` plants the emitter fault of that name in every kernel,
 * so `date_add` throws when it runs: the check that the suite does reach Varka (test 1 of the
 * plan), to be run over a few files only.
 */
class VarkaSQLQueryTestSuite extends SQLQueryTestSuite {

  // These read the system properties on each call rather than holding a val: the parent
  // registers its tests from its constructor, before this class's fields exist.
  private def armProperty: Option[Arm] = sys.props.get("varka.goldenSuite.arm").map(Arm.named)
  private def arm: Arm = armProperty.get

  private lazy val exclusions =
    VarkaGoldenExclusions.read(VarkaSQLQueryTestSuite.workspaceFile("excluded.tsv"))

  // The committed served report, held as a floor rather than compared exactly: two full runs
  // agree on all but three tests, and those differ by one served execution even run alone
  // (`VARKA-271.md` 9). A test that served a batch when the report was written must still serve
  // one, or it has silently stopped testing Varka. Checked in the Varka arm without a planted
  // fault, and not while the report is being rewritten.
  private lazy val servedFloor: Map[String, Long] =
    if (arm != Arm.VARKA || sys.props.contains("varka.goldenSuite.fault") ||
        VarkaSQLQueryTestSuite.regenerating) {
      Map.empty
    } else {
      VarkaSQLQueryTestSuite.readServed(VarkaSQLQueryTestSuite.workspaceFile("served.json"))
    }

  // The served report: each test's counts, in the order the tests ran.
  private val served = mutable.LinkedHashMap.empty[String, VarkaArrowSource.Served]

  protected override def sparkConf: SparkConf =
    VarkaArrowSource.conf(super.sparkConf, varkaEnabled = arm == Arm.VARKA)

  private val optIn = "runs only in its own JVM, with -Dvarka.goldenSuite.arm=varka or cacheOnly"

  // Without the opt-in, the parent's tests - the golden files and its own few that need a
  // session - are not registered at all, and one test says how to run the suite.
  protected override def createScalaTestCase(testCase: TestCase): Unit =
    if (armProperty.isDefined) super.createScalaTestCase(testCase)

  override protected def test(testName: String, testTags: Tag*)(testBody: => Any)
      (implicit pos: Position): Unit =
    if (armProperty.isDefined || testName == optIn) super.test(testName, testTags: _*)(testBody)

  if (armProperty.isEmpty) {
    test(optIn) {
      cancel("opt-in: dev/varka_golden_suite.sh runs this suite")
    }
  }

  override def beforeAll(): Unit = if (armProperty.isDefined) {
    super.beforeAll()
    sys.props.get("varka.goldenSuite.fault").foreach { name =>
      val planted = VarkaEmitOption.named(name) match {
        case flag: VarkaEmitOption.Flag => flag.`with`(VarkaEmitOptions.DEFAULTS, true)
        case other => throw new IllegalArgumentException(s"$name is not a flag: $other")
      }
      VarkaColumnarToRowExec.setEmitOptionsForTesting(planted)
    }
  }

  // The parent's per-test hooks read the session; without the opt-in there is none.
  protected override def beforeEach(): Unit = if (armProperty.isDefined) super.beforeEach()

  protected override def afterEach(): Unit = if (armProperty.isDefined) super.afterEach()

  override def afterAll(): Unit = if (armProperty.isDefined) {
    try {
      VarkaColumnarToRowExec.setEmitOptionsForTesting(VarkaEmitOptions.DEFAULTS)
      writeServedReport()
    } finally super.afterAll()
  }

  protected override def runSqlTestCase(testCase: TestCase, listTestCases: Seq[TestCase]): Unit = {
    VarkaArrowSource.takeCounts()
    val failure =
      try {
        super.runSqlTestCase(testCase, listTestCases)
        None
      } catch {
        case e: TestCanceledException => throw e
        case e: Throwable => Some(e)
      } finally {
        // The listener runs on the listener bus's thread: drain it, so every execution of this
        // test is counted for this test and none for the next.
        spark.sparkContext.listenerBus.waitUntilEmpty()
        served(testCase.name) = VarkaArrowSource.takeCounts()
      }
    val floor = servedFloor.getOrElse(testCase.name, 0L)
    if (failure.isEmpty && floor > 0 && served(testCase.name).served == 0) {
      fail(s"${testCase.name} passed, but Varka served none of its query executions where " +
        s"sql/varka/spark_suites/served.json records $floor: the suite no longer reaches Varka " +
        "for it. Find what changed, or regenerate the report if the change is intended.")
    }
    VarkaGoldenExclusions.decide(exclusions.lookup(testCase.name, arm), failure.toJava) match {
      case _: Verdict.Pass =>
      case c: Verdict.Cancel => cancel(c.reason)
      case f: Verdict.Fail => f.cause.toScala match {
        case Some(cause) => throw cause
        case None => fail(f.message)
      }
    }
  }

  private def writeServedReport(): Unit = {
    val rendered = VarkaSQLQueryTestSuite.renderServed(arm, served.toSeq)
    sys.props.get("varka.goldenSuite.servedOut").foreach { out =>
      Files.writeString(Path.of(out), rendered, StandardCharsets.UTF_8)
    }
    if (VarkaSQLQueryTestSuite.regenerating) {
      Files.writeString(VarkaSQLQueryTestSuite.workspaceFile("served.json"), rendered,
        StandardCharsets.UTF_8)
    }
  }
}

object VarkaSQLQueryTestSuite {
  private def regenerating: Boolean = sys.env.get("VARKA_GOLDEN_SUITE_REGEN").contains("true")

  private val servedLine = """^ {4}"((?:[^"\\]|\\.)*)": \{"executions": \d+, "served": (\d+),.*""".r

  /** Each test's served executions from a served report [[renderServed]] wrote. */
  def readServed(path: Path): Map[String, Long] =
    Files.readAllLines(path, StandardCharsets.UTF_8).asScala.collect {
      case servedLine(name, served) =>
        name.replace("\\\"", "\"").replace("\\\\", "\\") -> served.toLong
    }.toMap

  private def workspaceFile(name: String): Path =
    Path.of(sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME")),
      "sql", "varka", "spark_suites", name)

  /**
   * The served report as JSON, one test per line in name order so that two runs diff line by
   * line: the query executions, those with a batch Varka served, and the batches.
   */
  def renderServed(arm: Arm, tests: Seq[(String, VarkaArrowSource.Served)]): String = {
    def quoted(s: String): String =
      "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    val lines = tests.sortBy(_._1).map { case (name, s) =>
      s"    ${quoted(name)}: {\"executions\": ${s.executions}, \"served\": ${s.served}, " +
        s"\"batches\": ${s.batches}}"
    }
    val total = tests.map(_._2).foldLeft(VarkaArrowSource.Served(0, 0, 0)) { (a, s) =>
      VarkaArrowSource.Served(a.executions + s.executions, a.served + s.served,
        a.batches + s.batches)
    }
    val generatedBy = "VarkaSQLQueryTestSuite (VARKA-271); regenerate with " +
      "VARKA_GOLDEN_SUITE_REGEN=true dev/varka_golden_suite.sh"
    Seq(
      "{",
      s"  \"generated_by\": ${quoted(generatedBy)},",
      s"  \"arm\": ${quoted(arm.spelling)},",
      s"  \"total\": {\"tests\": ${tests.size}, \"executions\": ${total.executions}, " +
        s"\"served\": ${total.served}, \"batches\": ${total.batches}},",
      "  \"tests\": {",
      lines.mkString(",\n"),
      "  }",
      "}").mkString("", "\n", "\n")
  }
}
