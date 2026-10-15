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
      val key = SQLConf.VARKA_ENABLED.key
      varkaSpark.conf.set(key, "false")
      try {
        val off = years(varkaSpark.sql(farOffset))
        show("wrong 1 Varka off", off)
        assert(off === expected)
      } finally varkaSpark.conf.set(key, "true")
    }
  }

  test("slow query: a query that declines a batch shows it in the metrics and EXPLAIN") {
    cacheDatesFarOffset(spark)
    cacheDatesFarOffset(varkaSpark)
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
    assert(node.metrics("numFallbackBatchesDeclined").value > 0L)
    show("slow 3 EXPLAIN",
      df.queryExecution.explainString(ExplainMode.fromString("formatted")))
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
    show("slow 3 EXPLAIN of a filter with a residual conjunct",
      mixed.queryExecution.explainString(ExplainMode.fromString("formatted")))
  }

  test("crash: a kernel that throws writes a reproduction file, the warning names it") {
    cacheDatesFarOffset(spark)
    cacheDatesFarOffset(varkaSpark)
    val query = "SELECT date_add(d, 3) AS a FROM varka_dates_far_offset"
    val expected = spark.sql(query).collect().toSeq
    Option(out.listFiles()).foreach(_.foreach(f =>
      Option(f.listFiles()).foreach(_.foreach(_.delete()))))
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
    appender.loggingEvents.map(_.getMessage.getFormattedMessage).distinct
      .foreach(show("crash 1 warning", _))
    val deadline = System.nanoTime() + 30L * 1000 * 1000 * 1000
    def finished: Seq[Path] = if (!repro.exists()) Seq.empty else {
      val listing = Files.list(repro.toPath)
      try listing.iterator().asScala.toSeq.filterNot(_.getFileName.toString.startsWith("."))
      finally listing.close()
    }
    while (finished.isEmpty && System.nanoTime() < deadline) Thread.sleep(10)
    assert(finished.size === 1, finished)
    show("crash 2 file", repro.getPath + "/" + finished.head.getFileName)
    show("crash 2 classes", Option(dump.list()).map(_.mkString(", ")).getOrElse("none"))
  }

  test("crash at planning: a class the verifier rejects is still a bare VerifyError") {
    cacheDatesFarOffset(varkaSpark)
    val repro = new File(out, "repro-planning")
    VarkaReproduction.resetForTesting()
    varkaSpark.conf.set(SQLConf.VARKA_REPRODUCTION_DIRECTORY.key, repro.getPath)
    try {
      withOptions(_.withDropStackMaps(true)) {
        val e = intercept[Throwable](
          varkaSpark.sql("SELECT date_add(d, 3) AS a FROM varka_dates_far_offset").collect())
        show("crash 4 error", e.toString.linesIterator.take(12).mkString("\n"))
        // Open work: the page says this is not yet an INTERNAL_ERROR with a context block.
        assert(e.isInstanceOf[VerifyError] && !e.getMessage.contains("INTERNAL_ERROR"), e)
      }
    } finally varkaSpark.conf.unset(SQLConf.VARKA_REPRODUCTION_DIRECTORY.key)
    Thread.sleep(500)
    show("crash 4 file", Option(repro.list()).map(_.mkString(", ")).getOrElse("none"))
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
