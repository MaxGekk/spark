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

package org.apache.spark.sql.catalyst.expressions.codegen

import java.util.concurrent.atomic.AtomicInteger

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * The Varka Class-File assembly engine (Task 5). It routes Varka-eligible codegen units
 * through the [[CodeGenerator.compile]] funnel (see [[CodeGenerator.compile]]): when a
 * [[CodeAndComment]] carries Class-File ops and routing is enabled, the [[GeneratedClass]]
 * shell is assembled with the Class-File API (see [[ClassFileAssembler]]), loaded via a
 * [[VarkaGeneratedClassLoader]] and cached under the same key as the string backend's. Any
 * failure (or the explicit test injection) falls back to the string backend, so a Varka
 * assembly problem can never break codegen.
 *
 * The assembled evaluator body is a stub for now: `apply` throws
 * `UnsupportedOperationException` because the actual batch execution is wired in Task 6.
 */
private[expressions] object JavaClassFileEngine extends Logging {

  /** The binary name of the assembled wrapper class. */
  private val WrapperClassName = "org.apache.spark.sql.varka.execution.GeneratedClass"

  /** The binary name of the assembled projection class. */
  private val SpecClassName = "org.apache.spark.sql.varka.execution.SpecificVarkaProjection"

  /**
   * Whether the `CodeGenerator.compile` funnel routes Class-File-eligible units through
   * this engine. Off by default so the Varka machinery is fully inert until a later task
   * wires the actual batch execution; tests enable it explicitly.
   *
   * Thread-scoped: sbt runs test suites in parallel within one JVM (`Test / parallelExecution`
   * is true by default), and the funnel reads this flag on the compiling thread. Scoping it
   * to the thread keeps the test knob from ever leaking into a concurrently-running suite.
   */
  private val routingEnabledForTestingLocal = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }
  private[expressions] def routingEnabledForTesting: Boolean =
    routingEnabledForTestingLocal.get()
  private[expressions] def routingEnabledForTesting_=(value: Boolean): Unit =
    routingEnabledForTestingLocal.set(value)

  /**
   * Test injection: when true, `assembleOrFallback` skips assembly and falls back to the
   * string backend. Exercises the ghost-fallback path of the compile funnel. Thread-scoped
   * like [[routingEnabledForTesting]].
   */
  private val failAssemblyForTestingLocal = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }
  private[expressions] def failAssemblyForTesting: Boolean = failAssemblyForTestingLocal.get()
  private[expressions] def failAssemblyForTesting_=(value: Boolean): Unit =
    failAssemblyForTestingLocal.set(value)

  /**
   * Test injection: when true, `assembleGeneratedClass` flips the wrapper class's magic
   * number so the JVM rejects the bytes at definition time with a `ClassFormatError` (a
   * [[LinkageError]]). Exercises the real assembly/load catch path of the funnel, as opposed
   * to [[failAssemblyForTesting]], which short-circuits before assembly. Only reachable when
   * routing is enabled on the same thread, so it cannot affect concurrent suites.
   */
  @volatile private[expressions] var corruptAssemblyForTesting: Boolean = false

  /** Number of times `assembleGeneratedClass` has run; lets tests assert no re-assembly. */
  private[expressions] val assemblyAttempts = new AtomicInteger(0)

  /**
   * Assembles the full [[GeneratedClass]] shell as two classes: a public wrapper `className`
   * extending [[GeneratedClass]] with `generate(Object[])`, and `SpecificVarkaProjection`
   * extending [[UnsafeProjection]]. Returns each class's binary name and bytes.
   */
  def assembleGeneratedClass(className: String): Seq[(String, Array[Byte])] = {
    assemblyAttempts.incrementAndGet()
    val bytes = ClassFileAssembler.assembleGeneratedClass(className, SpecClassName)
    if (corruptAssemblyForTesting) {
      val corruptedWrapper = bytes(0).clone()
      corruptedWrapper(0) = (corruptedWrapper(0) ^ 0xff.toByte).toByte
      Seq((className, corruptedWrapper), (SpecClassName, bytes(1)))
    } else {
      Seq((className, bytes(0)), (SpecClassName, bytes(1)))
    }
  }

  /** Fallback-catchable failures: [[NonFatal]] plus [[LinkageError]] (bad bytecode surfaces
   * as `VerifyError`/`ClassFormatError`, a missing class as `NoClassDefFoundError`). */
  private def isCatchable(e: Throwable): Boolean = NonFatal(e) || e.isInstanceOf[LinkageError]

  /** Assembles, loads and instantiates the [[GeneratedClass]] for `code`. */
  def assembleAndLoad(code: CodeAndComment): (GeneratedClass, ByteCodeStats) = {
    val classes = assembleGeneratedClass(WrapperClassName)
    val loader = new VarkaGeneratedClassLoader(Utils.getContextOrSparkClassLoader)
    try {
      classes.foreach { case (name, bytes) => loader.defineGeneratedClass(name, bytes) }
      val clazz = loader.loadClass(WrapperClassName)
      val generated = clazz.getConstructor().newInstance().asInstanceOf[GeneratedClass]
      (generated, CodeCompiler.computeByteCodeStats(classes))
    } catch {
      case e: Throwable if isCatchable(e) =>
        loader.release()
        throw e
    }
  }

  /**
   * The `CodeGenerator.compile` funnel entry point. Assembles and loads when the unit is
   * Varka-eligible; on any failure falls back to `fallback.compile(code)`. Never throws.
   */
  def assembleOrFallback(
      code: CodeAndComment,
      fallback: CodeCompiler): (GeneratedClass, ByteCodeStats) = {
    if (failAssemblyForTesting) {
      logWarning("Varka Class-File assembly is disabled for testing; " +
        "falling back to the string codegen backend.")
      fallback.compile(code)
    } else {
      try {
        assembleAndLoad(code)
      } catch {
        case e: Throwable if isCatchable(e) =>
          logWarning("Varka Class-File assembly failed; " +
            "falling back to the string codegen backend.", e)
          fallback.compile(code)
      }
    }
  }
}
