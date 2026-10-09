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
import java.util.concurrent.TimeUnit

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
 * In the shared JVM the suite registers one test, which starts a child JVM with the parent's
 * arguments, classpath and system properties (the matrix configuration, the sanitizer, a fuzzer's
 * budget and seed) running this suite alone, and passes when the child passes. The suite's own
 * tests are not registered in the parent. The child's output is copied to the parent's, so the
 * matrix's markers and a failure's message are where they were, and a failing test's lines are in
 * the parent's failure message. `-Dvarka.ownJvm=true` runs the tests directly with the guard off,
 * which is how the child runs, and how to run one test of such a suite from sbt:
 *
 * {{{
 *   build/sbt 'set Test/javaOptions += "-Dvarka.ownJvm=true"' \
 *     'catalyst/testOnly *VarkaIrFuzzSuite -- -z "planted"'
 * }}}
 */
trait VarkaOwnJvm extends SparkFunSuite {

  override protected def test(testName: String, testTags: Tag*)(testBody: => Any)
      (implicit pos: Position): Unit = {
    if (VarkaOwnJvm.inChild) {
      super.test(testName, testTags: _*)(testBody)
    }
  }

  if (!VarkaOwnJvm.inChild) {
    super.test(s"$suiteName runs in a JVM of its own, which keeps one vector species per lane " +
        "type in the shared one") {
      VarkaOwnJvm.runInChild(getClass.getName)
    }
  }
}

object VarkaOwnJvm {

  /** Set for the child, and by hand to run such a suite's tests in the JVM at hand. */
  val PROPERTY = "varka.ownJvm"

  def inChild: Boolean = sys.props.get(PROPERTY).contains("true")

  private val timeoutMinutes = 15L

  private def classpath: String =
    Option(System.getenv("SPARK_DIST_CLASSPATH")).filter(_.nonEmpty)
      .getOrElse(System.getProperty("java.class.path"))

  /** Runs `suite` alone in a child JVM and fails with the child's failing lines if it fails. */
  def runInChild(suite: String): Unit = {
    val javaBin = new File(new File(System.getProperty("java.home"), "bin"), "java")
    val command = new java.util.ArrayList[String]()
    command.add(javaBin.getAbsolutePath)
    // The parent's own arguments: heap, the incubator module, native access, every -D the run
    // was given. A -javaagent or -agentlib would start twice, and the child needs neither.
    ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
      .filterNot(a => a.startsWith("-javaagent") || a.startsWith("-agentlib")).foreach(command.add)
    command.add(s"-D$PROPERTY=true")
    command.add("-cp")
    command.add(classpath)
    command.add("org.scalatest.tools.Runner")
    command.add("-oW")
    command.add("-s")
    command.add(suite)
    val builder = new ProcessBuilder(command)
    builder.redirectErrorStream(true)
    builder.environment().put("SPARK_TESTING", "1")
    val process = builder.start()
    val reader = new BufferedReader(
      new InputStreamReader(process.getInputStream, StandardCharsets.UTF_8))
    val failures = mutable.ArrayBuffer.empty[String]
    val tail = mutable.Queue.empty[String]
    var line = reader.readLine()
    while (line != null) {
      // scalastyle:off println
      println(line)
      // scalastyle:on println
      if (line.contains("*** FAILED ***") || line.contains("*** ABORTED ***")) {
        failures += line.trim
      }
      tail.enqueue(line)
      if (tail.size > 30) tail.dequeue()
      line = reader.readLine()
    }
    if (!process.waitFor(timeoutMinutes, TimeUnit.MINUTES)) {
      process.destroyForcibly()
      throw new IllegalStateException(s"$suite did not finish in $timeoutMinutes minutes in its " +
        "own JVM")
    }
    val exit = process.exitValue()
    assert(exit == 0,
      s"$suite failed in its own JVM (exit $exit):\n" +
        (if (failures.nonEmpty) failures.mkString("\n") else tail.mkString("\n")))
  }
}
