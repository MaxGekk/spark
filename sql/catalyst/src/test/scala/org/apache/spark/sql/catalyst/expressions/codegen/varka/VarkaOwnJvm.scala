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

import java.io.{BufferedReader, File, InputStreamReader}
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import javax.xml.parsers.DocumentBuilderFactory

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkFunSuite

/**
 * A suite that runs its tests in a JVM of its own when it is run in a shared one (VARKA-246,
 * `m8/SCOPE.md` item 69). Mix it in last, after the suite's other traits.
 *
 * The tests of such a suite emit and run kernels at a lanes override other than the JVM's
 * preferred width, which puts a second vector species of one lane type into the JVM. That makes
 * the Vector API's shared templates inline bimorphically, so that every kernel compiled in the
 * JVM afterwards can box its vectors, and any test whose verdict is a JIT outcome fails by the
 * suites' order (`sql/varka/skills/vector-api-and-width.md`). [[VarkaSpeciesGuard]] fails any
 * suite that does it in the shared JVM; this trait is how a suite that must keeps its coverage.
 *
 * In the shared JVM the suite keeps its tests, by name and tags, but their bodies do not run
 * there: the first of them to run starts a child JVM with the parent's command-line arguments,
 * classpath and `-D` properties (the matrix configuration, the sanitizer, a fuzzer's budget and
 * seed) running this suite alone and writing a JUnit report, and each test then takes its result
 * from that report - passed, cancelled with the child's reason, or failed with the child's
 * message. So a name filter (`-z`, `-t`) selects the tests it names, the counts mean what they
 * did, and a failure is attributed to its test; what the filter cannot do is shorten the child,
 * which runs the whole suite. The child's output is copied to the parent's, so the matrix's markers
 * are where they were. `-Dvarka.ownJvm=true` runs the tests directly in the JVM at hand with the
 * guard off, which is how the child runs.
 *
 * The child is ended from outside when it outlasts its budget, which is the shortest of the
 * suites' per-test caps (`varka.test.watchdog.minutes`, `spark.test.timeout`) less a minute, so the
 * parent reports the child's hang before a cap halts the JVM; `-Dvarka.ownJvm.timeoutMinutes`
 * overrides it. The first test carries the suite's whole time against that cap.
 */
trait VarkaOwnJvm extends SparkFunSuite {

  override protected def test(testName: String, testTags: Tag*)(testBody: => Any)
      (implicit pos: Position): Unit = {
    if (VarkaOwnJvm.inChild) {
      super.test(testName, testTags: _*)(testBody)
    } else {
      super.test(testName, testTags: _*) {
        VarkaOwnJvm.resultsOf(getClass.getName).get(testName) match {
          case Some(VarkaOwnJvm.Passed) =>
          case Some(VarkaOwnJvm.Skipped(reason)) => cancel(reason)
          case Some(VarkaOwnJvm.Failed(message)) => fail(message)
          case None => fail(s"the child JVM reported no result for '$testName'")
        }
      }
    }
  }
}

object VarkaOwnJvm {

  /** Set for the child, and by hand to run such a suite's tests in the JVM at hand. */
  val PROPERTY: String = VarkaSpeciesGuard.OWN_JVM_PROPERTY

  sealed trait Result
  case object Passed extends Result
  final case class Skipped(reason: String) extends Result
  final case class Failed(message: String) extends Result

  def inChild: Boolean = sys.props.get(PROPERTY).contains("true")

  // One child per suite per parent JVM; the first test to ask runs it.
  private val children = new ConcurrentHashMap[String, Map[String, Result]]()

  private def resultsOf(suite: String): Map[String, Result] =
    children.computeIfAbsent(suite, runInChild)

  /** The minutes a child may run: the shortest per-test cap less one, or the override. */
  private def budgetMinutes: Long = sys.props.get("varka.ownJvm.timeoutMinutes").map(_.toLong)
    .getOrElse {
      val watchdog = sys.props.get("varka.test.watchdog.minutes").map(_.toLong).getOrElse(10L)
      val sparkCap = sys.props.get("spark.test.timeout").map(_.toLong).getOrElse(20L)
      math.max(1L, math.min(watchdog, sparkCap) - 1L)
    }

  /**
   * The parent's classpath. `java.class.path` is what the parent runs with; the
   * `SPARK_DIST_CLASSPATH` environment variable is what the other forking suites prefer, for a
   * launcher whose own classpath is a stub, so it is taken only when the parent's does not hold
   * ScalaTest.
   */
  private def classpath: String = {
    val own = System.getProperty("java.class.path")
    if (own.contains("scalatest")) own
    else Option(System.getenv("SPARK_DIST_CLASSPATH")).filter(_.nonEmpty).getOrElse(own)
  }

  private def runInChild(suite: String): Map[String, Result] = {
    val report = Files.createTempDirectory("varka-own-jvm")
    val javaBin = new File(new File(System.getProperty("java.home"), "bin"), "java")
    val command = new java.util.ArrayList[String]()
    command.add(javaBin.getAbsolutePath)
    // The parent's own command-line arguments: heap, the incubator module, native access, every
    // -D the run was given. A -javaagent or -agentlib would start twice, and the child needs
    // neither. Properties the parent set programmatically are not copied.
    ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
      .filterNot(a => a.startsWith("-javaagent") || a.startsWith("-agentlib")).foreach(command.add)
    command.add(s"-D$PROPERTY=true")
    command.add("-cp")
    command.add(classpath)
    command.add("org.scalatest.tools.Runner")
    command.addAll(java.util.List.of("-oW", "-u", report.toString, "-s", suite))
    val builder = new ProcessBuilder(command)
    builder.redirectErrorStream(true)
    builder.environment().put("SPARK_TESTING", "1")
    val process = builder.start()
    // The read below ends only when the child exits, so a hung child is ended from here.
    var timedOut = false
    val killer = new Thread(() => {
      if (!process.waitFor(budgetMinutes, TimeUnit.MINUTES)) {
        timedOut = true
        process.destroyForcibly()
      }
    })
    killer.setDaemon(true)
    killer.start()
    val tail = mutable.Queue.empty[String]
    try {
      val reader = new BufferedReader(
        new InputStreamReader(process.getInputStream, StandardCharsets.UTF_8))
      try {
        var line = reader.readLine()
        while (line != null) {
          // scalastyle:off println
          println(line)
          // scalastyle:on println
          tail.enqueue(line)
          if (tail.size > 30) tail.dequeue()
          line = reader.readLine()
        }
      } finally {
        reader.close()
      }
      process.waitFor()
    } finally {
      if (process.isAlive) process.destroyForcibly()
    }
    val parsed = parseReport(report.toFile, suite)
    val exit = process.exitValue()
    if (timedOut) {
      failAll(parsed, s"$suite did not finish in $budgetMinutes minutes in its own JVM")
    } else if (parsed.isEmpty) {
      failAll(parsed, s"$suite left no report in its own JVM (exit $exit):\n${tail.mkString("\n")}")
    } else {
      parsed
    }
  }

  private def failAll(parsed: Map[String, Result], message: String): Map[String, Result] =
    parsed.map { case (name, result) => name -> (result match {
      case Passed => Failed(message)
      case other => other
    }) }.withDefaultValue(Failed(message))

  /** The child's JUnit report: each test case passed, skipped (cancelled) or failed. */
  private def parseReport(dir: File, suite: String): Map[String, Result] = {
    val files = Option(dir.listFiles()).getOrElse(Array.empty[File])
      .filter(_.getName.endsWith(".xml"))
    val results = mutable.LinkedHashMap.empty[String, Result]
    val builder = DocumentBuilderFactory.newInstance().newDocumentBuilder()
    files.foreach { file =>
      val cases = builder.parse(file).getElementsByTagName("testcase")
      for (i <- 0 until cases.getLength) {
        val node = cases.item(i).asInstanceOf[org.w3c.dom.Element]
        val name = node.getAttribute("name")
        def child(tag: String) = node.getElementsByTagName(tag)
        def messageOf(tag: String) = child(tag).item(0).asInstanceOf[org.w3c.dom.Element]
          .getAttribute("message")
        results(name) =
          if (child("failure").getLength > 0) Failed(s"in its own JVM: ${messageOf("failure")}")
          else if (child("error").getLength > 0) Failed(s"in its own JVM: ${messageOf("error")}")
          else if (child("skipped").getLength > 0) Skipped(s"cancelled in its own JVM")
          else Passed
      }
    }
    results.toMap
  }
}
