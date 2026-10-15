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

import java.io.File
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import org.apache.logging.log4j.Level

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler
import org.apache.spark.sql.catalyst.expressions.codegen.varka.{VarkaEmitOptions, VarkaReproduction, VarkaTestWatchdog}
import org.apache.spark.sql.internal.SQLConf

/**
 * The three branches of the troubleshooting runbook in `docs/sql-varka.md`, each walked on a
 * planted fault of its kind, so that the page's steps are checked against the code and not only
 * against a run once (VARKA-316). Every test prints what each step shows (lines starting
 * `RUNBOOK`); the page quotes them. The tests that write files leave them under
 * `target/varka-runbook` (the working directory of the test JVM), where the page's command-line
 * steps (`dev/varka_emit.sh --replay`, `javap`) read them.
 *
 *  - a wrong answer: `guardDayProducers=false` removes the guard that declines a day outside the
 *    calendar lowering's range, so the kernel answers a far offset with a wrapped year;
 *  - a slow query: the same query with the guard on declines the batch, so a user sees the cost
 *    as metrics and as `EXPLAIN` lines, with no error;
 *  - a crash: `misdescribeDataBytes=-4` makes the kernel throw inside the vector loop.
 */
class VarkaRunbookSuite extends QueryTest with VarkaSharedSessions with VarkaTestWatchdog {

  private val out = new File("target/varka-runbook")

  private val farOffset = "SELECT year(date_add(d, off)) AS y FROM varka_dates_far_offset"

  private def show(step: String, text: String): Unit = {
    // scalastyle:off println
    text.linesIterator.foreach(line => println(s"RUNBOOK [$step] $line"))
    // scalastyle:on println
  }

  private def withOptions[T](change: VarkaEmitOptions => VarkaEmitOptions)(body: => T): T = {
    val previous = VarkaColumnarToRowExec.currentEmitOptions
    VarkaColumnarToRowExec.setEmitOptionsForTesting(change(previous))
    try body finally VarkaColumnarToRowExec.setEmitOptionsForTesting(previous)
  }

  private def varkaNode(df: org.apache.spark.sql.DataFrame): SparkPlan =
    collectFirst(df.queryExecution.executedPlan) { case v if isVarkaNode(v) => v }
      .getOrElse(fail(s"no Varka node in:\n${df.queryExecution.executedPlan.treeString}"))

  /**
   * The finished reproduction files in `dir`, waiting up to 30 s for the first: the file is
   * written by the failing task, which can still be moving it into place when the query's
   * failure reaches the test.
   */
  private def awaitFiles(dir: File): Seq[Path] = {
    def finished: Seq[Path] = if (!dir.exists()) Seq.empty else {
      val listing = Files.list(dir.toPath)
      try listing.iterator().asScala.toSeq.filterNot(_.getFileName.toString.startsWith("."))
      finally listing.close()
    }
    val deadline = System.nanoTime() + 30L * 1000 * 1000 * 1000
    while (finished.isEmpty && System.nanoTime() < deadline) Thread.sleep(10)
    finished
  }

  private def years(df: org.apache.spark.sql.DataFrame): String =
    df.collect().map(_.get(0)).mkString("[", ", ", "]")

  test("wrong answer: turn Varka off and compare, then read the plan and the emitted kernel") {
    cacheDatesFarOffset(spark)
    cacheDatesFarOffset(varkaSpark)
    val expected = years(spark.sql(farOffset))
    show("wrong 1 Spark's own answer", expected)
    withOptions(_.withGuardDayProducers(false)) {
      val df = varkaSpark.sql(farOffset)
      val got = years(df)
      show("wrong 1 Varka on", got)
      assert(got !== expected, "the planted fault must change the answer")
      val node = varkaNode(df)
      show("wrong 2 plan", node.treeString.linesIterator.take(2).mkString("\n"))
      show("wrong 2 numVarkaBatches / numFallbackBatchesDeclined",
        s"${node.metrics("numVarkaBatches").value} / " +
          s"${node.metrics("numFallbackBatchesDeclined").value}")
      // The page: nothing declined, so only the comparison can find this fault.
      assert(node.metrics("numFallbackBatchesDeclined").value === 0L)
      val key = SQLConf.VARKA_ENABLED.key
      val previous = varkaSpark.conf.getOption(key)
      varkaSpark.conf.set(key, "false")
      try {
        val off = years(varkaSpark.sql(farOffset))
        show("wrong 1 Varka off", off)
        assert(off === expected)
      } finally previous.fold(varkaSpark.conf.unset(key))(varkaSpark.conf.set(key, _))
    }
  }

  test("slow query: a query that declines a batch shows it in the metrics and EXPLAIN") {
    cacheDatesFarOffset(spark)
    cacheDatesFarOffset(varkaSpark)
    // The guard under test is an emit option the option matrix may turn off.
    withOptions(_.withGuardDayProducers(true))(slowQuery())
  }

  private def slowQuery(): Unit = {
    val df = varkaSpark.sql(farOffset)
    checkAnswer(df, spark.sql(farOffset))
    val node = varkaNode(df)
    for (m <- Seq("numVarkaBatches", "numFallbackBatchesDeclined", "numVarkaRows",
        "numFallbackRows")) {
      show("slow 1 metric", s"$m = ${node.metrics(m).value}")
    }
    show("slow 1 metric", s"fallbackTime registered = ${node.metrics.contains("fallbackTime")}")
    VarkaFallbackReasons.rendered(varkaSpark, node).asScala.toSeq.sortBy(_._1)
      .foreach { case (reason, shown) => show("slow 2 reason", s"$reason = $shown") }
    val declined = node.metrics("numFallbackBatchesDeclined").value
    assert(declined > 0L)
    assert(node.metrics.contains("varkaKernelTime") && !node.metrics.contains("fallbackTime"))
    val shown = VarkaFallbackReasons.rendered(varkaSpark, node).asScala
    assert(shown("day-producer") === declined.toString, shown)
    assert(shown.filterNot(_._1 == "day-producer").values.forall(_ == "0"), shown)
    val explained = df.queryExecution.explainString(ExplainMode.fromString("formatted"))
    show("slow 3 EXPLAIN", explained)
    // A decline at run time is not in EXPLAIN: the entry still reads fused.
    assert(explained.contains("y: fused"), explained)
    cacheDates(spark)
    cacheDates(varkaSpark)
    val residual = varkaSpark.sql(
      "SELECT year(d) AS a, length(CAST(i AS STRING)) AS b FROM varka_dates")
    val residualLines = residual.queryExecution.explainString(ExplainMode.fromString("formatted"))
      .linesIterator.filter(_.startsWith("Varka")).mkString("\n")
    show("slow 3 EXPLAIN of a projection with a residual entry", residualLines)
    assert(residualLines.contains("b: residual (unsupported expression"), residualLines)
    val mixed = varkaSpark.sql(
      "SELECT d FROM varka_dates_far_offset WHERE d < date'2024-01-02' AND off % 7 = 1")
    val mixedText = mixed.queryExecution.explainString(ExplainMode.fromString("formatted"))
    show("slow 3 EXPLAIN of a filter with a residual conjunct", mixedText)
    assert(mixedText.contains(": fused") && mixedText.contains(": residual (unsupported"),
      mixedText)
  }

  test("crash: a kernel that throws writes a reproduction file, the warning names it") {
    cacheDatesFarOffset(spark)
    cacheDatesFarOffset(varkaSpark)
    val query = "SELECT date_add(d, 3) AS a FROM varka_dates_far_offset"
    val expected = spark.sql(query).collect().toSeq
    org.apache.spark.util.Utils.deleteRecursively(out)
    val repro = new File(out, "repro")
    val dump = new File(out, "classes")
    VarkaReproduction.resetForTesting()
    varkaSpark.conf.set(SQLConf.VARKA_REPRODUCTION_DIRECTORY.key, repro.getPath)
    varkaSpark.conf.set(SQLConf.VARKA_CLASS_DUMP_DIRECTORY.key, dump.getPath)
    val appender = new LogAppender("runbook crash")
    appender.setThreshold(Level.WARN)
    try {
      withLogAppender(appender) {
        withOptions(_.withMisdescribeDataBytes(-4)) {
          VarkaColumnarToRowExec.setFailureFallbackExpectedForTesting(true)
          try checkAnswer(varkaSpark.sql(query), expected)
          finally VarkaColumnarToRowExec.setFailureFallbackExpectedForTesting(false)
        }
      }
    } finally {
      varkaSpark.conf.unset(SQLConf.VARKA_REPRODUCTION_DIRECTORY.key)
      varkaSpark.conf.unset(SQLConf.VARKA_CLASS_DUMP_DIRECTORY.key)
    }
    show("crash 1 answer", "the query still answers, with the row engine's rows")
    val warnings = appender.loggingEvents.map(_.getMessage.getFormattedMessage).distinct
    warnings.foreach(show("crash 1 warning", _))
    assert(warnings.exists(w => w.contains("failed on this batch; falling back to the per-row " +
      "path.") && w.contains("kind: kernel-failure") && w.contains("shape hash: ")), warnings)
    assert(warnings.exists(_.contains("Wrote a reproduction of this Varka failure")), warnings)
    val files = awaitFiles(repro)
    assert(files.size === 1, files)
    show("crash 2 file", repro.getPath + "/" + files.head.getFileName)
    show("crash 2 classes", Option(dump.list()).map(_.mkString(", ")).getOrElse("none"))
  }

  test("crash at planning: a class the verifier rejects writes a file and replays") {
    cacheDatesFarOffset(varkaSpark)
    val repro = new File(out, "repro-planning")
    VarkaReproduction.resetForTesting()
    varkaSpark.conf.set(SQLConf.VARKA_REPRODUCTION_DIRECTORY.key, repro.getPath)
    try {
      withOptions(_.withDropStackMaps(true)) {
        val e = intercept[Throwable](
          varkaSpark.sql("SELECT date_add(d, 3) AS a FROM varka_dates_far_offset").collect())
        show("crash 4 error", e.toString.linesIterator.take(12).mkString("\n"))
        // What the page says today (row 314b is open): a VerifyError somewhere in the chain.
        assert(Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(20)
          .exists(_.isInstanceOf[VerifyError]), e)
      }
    } finally varkaSpark.conf.unset(SQLConf.VARKA_REPRODUCTION_DIRECTORY.key)
    val files = awaitFiles(repro)
    show("crash 4 file", files.map(_.getFileName.toString).mkString(", "))
    assert(files.size === 1, files)
  }

  test("crash at planning: a compiler bug raises INTERNAL_ERROR with the context record") {
    cacheDatesFarOffset(varkaSpark)
    VarkaExpressionCompiler.setFailForTesting(true)
    try {
      val e = intercept[Throwable](
        varkaSpark.sql("SELECT date_add(d, 3) AS a FROM varka_dates_far_offset").collect())
      show("crash 3 INTERNAL_ERROR", e.getMessage.linesIterator.mkString("\n"))
      show("crash 3 cause", String.valueOf(e.getCause))
      assert(e.getMessage.contains("[INTERNAL_ERROR]") && e.getMessage.contains("kind: planning"))
    } finally VarkaExpressionCompiler.setFailForTesting(false)
  }
}
