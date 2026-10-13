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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.scalactic.source.Position
import org.scalatest.{Args, Status, Tag}
import org.scalatest.exceptions.TestCanceledException

import org.apache.spark.SparkConf
import org.apache.spark.sql.VarkaGoldenExclusions.{Arm, Kind, Verdict}
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
 * cache under another serializer (`dev/varka_golden_suite.sh` gives it its own). It never
 * writes golden files: an answer that went through Varka is not an oracle.
 *
 * `sql/varka/spark_suites/excluded.tsv` excuses what the Arrow source itself changes
 * ([[VarkaGoldenExclusions]]): a `plans` entry leaves its file's `EXPLAIN` statements uncompared
 * and compares every other statement, and a `harness` or `environment` entry cancels its test
 * when it fails.
 *
 * After each test the suite records how many of its query executions Varka served a batch in,
 * the served report, so a test that passes without reaching Varka is visible. The report goes
 * to `varka.goldenSuite.servedOut` when that is set, and `VARKA_GOLDEN_SUITE_REGEN=true`
 * rewrites the committed `sql/varka/spark_suites/served.json` from a full run of the Varka arm.
 *
 * `varka.goldenSuite.fault=<flag>` plants one of the emitter's fault injectors in every kernel:
 * the check that the suite does reach Varka (test 1 of the plan), to be run over a few files
 * only: `misdescribeAdd` fails the date files, whose lifted statements call `date_add`, and
 * `misdescribeWordLiveness`, under which every emission raises, fails every test Varka serves.
 */
class VarkaSQLQueryTestSuite extends SQLQueryTestSuite {

  // These read the system properties on each call rather than holding a val: the parent
  // registers its tests from its constructor, before this class's fields exist.
  private def armProperty: Option[Arm] = sys.props.get("varka.goldenSuite.arm").map(Arm.named)
  private def arm: Arm = armProperty.get
  private def fault: Option[String] = sys.props.get("varka.goldenSuite.fault")

  private lazy val exclusions = VarkaGoldenExclusions.read(spanFile("excluded.tsv"))

  // The committed served report, held as a floor rather than compared exactly: two full runs
  // agree on all but three tests, and those differ by one served execution even run alone
  // (`VARKA-271.md` 9). A test that served a batch when the report was written must still serve
  // one, or it has silently stopped testing Varka. Checked in the Varka arm without a planted
  // fault, and not while the report is being rewritten.
  private lazy val servedFloor: Map[String, Long] =
    if (arm != Arm.VARKA || fault.isDefined || VarkaSQLQueryTestSuite.regenerating) Map.empty
    else VarkaSQLQueryTestSuite.readServed(spanFile("served.json"))

  // The served report: each test's counts, in the order the tests ran.
  private val served = mutable.LinkedHashMap.empty[String, VarkaArrowSource.Served]

  // The tests ScalaTest selected to run, so a rewrite of the report can tell a full run from a
  // filtered one.
  private val ran = mutable.Set.empty[String]

  // The test running now and the EXPLAIN statements of it that differed and went uncompared.
  private var current: Option[TestCase] = None
  private var differingPlans = 0

  private def spanFile(name: String): Path =
    getWorkspaceFilePath("sql", "varka", "spark_suites", name)

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
    // The parent rewrites every golden file from the answers it gets when this is set; through
    // the Arrow source those answers are the harness's and Varka's, never Spark's.
    require(!regenerateGoldenFiles, "SPARK_GENERATE_GOLDEN_FILES=1 is set: " +
      "VarkaSQLQueryTestSuite never writes golden files, since its answers go through Varka. " +
      "Regenerate them with SQLQueryTestSuite, from a shell without the variable for this one.")
    super.beforeAll()
    fault.foreach { name =>
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

  protected override def runTest(testName: String, args: Args): Status = {
    ran += testName
    super.runTest(testName, args)
  }

  protected override def runSqlTestCase(testCase: TestCase, listTestCases: Seq[TestCase]): Unit = {
    VarkaArrowSource.takeCounts()
    current = Some(testCase)
    differingPlans = 0
    var failure =
      try {
        super.runSqlTestCase(testCase, listTestCases)
        None
      } catch {
        case e: TestCanceledException => throw e
        case NonFatal(e) => Some(e)
      } finally current = None
    // The listener runs on the listener bus's thread: drain it, so every execution of this
    // test is counted for this test and none for the next. A file of thousands of statements
    // beside a night load can take longer than the bus's default ten seconds; past two minutes
    // the drain's own failure is reported, unless the test had already failed.
    try spark.sparkContext.listenerBus.waitUntilEmpty(TimeUnit.MINUTES.toMillis(2))
    catch { case NonFatal(e) => if (failure.isEmpty) failure = Some(e) }
    served(testCase.name) = VarkaArrowSource.takeCounts()
    val floor = servedFloor.getOrElse(testCase.name, 0L)
    if (failure.isEmpty && floor > 0 && served(testCase.name).served == 0) {
      fail(s"${testCase.name} passed, but Varka served none of its query executions where " +
        s"sql/varka/spark_suites/served.json records $floor: the suite no longer reaches Varka " +
        "for it. Find what changed, or regenerate the report if the change is intended.")
    }
    val entry = exclusions.lookup(testCase.name, arm)
    VarkaGoldenExclusions.decide(entry, failure.toJava, differingPlans) match {
      case _: Verdict.Pass =>
      case c: Verdict.Cancel => cancel(c.reason)
      case f: Verdict.Fail => f.cause.toScala match {
        case Some(cause) => throw cause
        case None => fail(f.message)
      }
    }
  }

  /**
   * For a file with a `plans` entry, each `EXPLAIN` statement whose output differs from the
   * golden file is compared as if it matched, and counted; every other statement is compared as
   * the parent compares it. The golden file is split as the parent splits it.
   */
  override def readGoldenFileAndCompareResults(
      resultFile: String,
      outputs: Seq[QueryTestOutput],
      makeOutput: (String, Option[String], String) => QueryTestOutput): Unit = {
    val plans = current.flatMap(t => exclusions.lookup(t.name, arm).toScala)
      .exists(_.kind == Kind.PLANS)
    if (!plans) {
      super.readGoldenFileAndCompareResults(resultFile, outputs, makeOutput)
    } else {
      val segments = Files.readString(new File(resultFile).toPath).split("-- !query.*\n")
      var at = 0
      val compared = outputs.map { output =>
        val expected = if (output.numSegments == 3) {
          makeOutput(segments(at + 1).trim, Some(segments(at + 2).trim),
            normalizeTestResults(segments(at + 3)))
        } else {
          makeOutput(segments(at + 1).trim, None, normalizeTestResults(segments(at + 2)))
        }
        at += output.numSegments
        val explain = expected.sql.trim.toUpperCase(Locale.ROOT).startsWith("EXPLAIN")
        if (explain && expected.sql == output.sql && expected != output) {
          differingPlans += 1
          expected
        } else {
          output
        }
      }
      super.readGoldenFileAndCompareResults(resultFile, compared, makeOutput)
    }
  }

  private def writeServedReport(): Unit = {
    val rendered = VarkaSQLQueryTestSuite.renderServed(arm, served.toSeq)
    sys.props.get("varka.goldenSuite.servedOut").foreach { out =>
      Files.writeString(Path.of(out), rendered, StandardCharsets.UTF_8)
    }
    if (VarkaSQLQueryTestSuite.regenerating) {
      // The committed report is the floor every later run is held to, so only a run that can
      // set it rewrites it: the Varka arm, no fault planted, and every registered test run.
      val ignored = tags.collect { case (name, t) if t.contains(VarkaSQLQueryTestSuite.Ignore) =>
        name }.toSet
      val skipped = testNames.toSet -- ignored -- ran
      require(arm == Arm.VARKA && fault.isEmpty && skipped.isEmpty,
        "VARKA_GOLDEN_SUITE_REGEN=true rewrites sql/varka/spark_suites/served.json only from a " +
          s"full run of the Varka arm with no fault planted; this run is the ${arm.spelling} " +
          s"arm${fault.map(f => s" with $f planted").getOrElse("")} and left " +
          s"${skipped.size} tests out")
      Files.writeString(spanFile("served.json"), rendered, StandardCharsets.UTF_8)
    }
  }
}

object VarkaSQLQueryTestSuite {
  private val Ignore = "org.scalatest.Ignore"

  private def regenerating: Boolean = sys.env.get("VARKA_GOLDEN_SUITE_REGEN").contains("true")

  private val mapper = new ObjectMapper()

  /** Each test's served executions from a served report [[renderServed]] wrote. */
  def readServed(path: Path): Map[String, Long] =
    mapper.readTree(path.toFile).get("tests").properties().asScala
      .map(e => e.getKey -> e.getValue.get("served").asLong()).toMap

  /**
   * The served report as JSON, tests in name order so that two runs diff test by test: the query
   * executions, those with a batch Varka served, and the batches.
   */
  def renderServed(arm: Arm, tests: Seq[(String, VarkaArrowSource.Served)]): String = {
    def counts(node: ObjectNode, s: VarkaArrowSource.Served): ObjectNode =
      node.put("executions", s.executions).put("served", s.served).put("batches", s.batches)
    val root = mapper.createObjectNode()
    root.put("generated_by", "VarkaSQLQueryTestSuite (VARKA-271); regenerate with " +
      "VARKA_GOLDEN_SUITE_REGEN=true dev/varka_golden_suite.sh")
    root.put("arm", arm.spelling)
    val total = tests.map(_._2).foldLeft(VarkaArrowSource.Served(0, 0, 0)) { (a, s) =>
      VarkaArrowSource.Served(a.executions + s.executions, a.served + s.served,
        a.batches + s.batches)
    }
    counts(root.putObject("total").put("tests", tests.size), total)
    val byTest = root.putObject("tests")
    tests.sortBy(_._1).foreach { case (name, s) => counts(byTest.putObject(name), s) }
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n"
  }
}
