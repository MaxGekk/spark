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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BaseFixedWidthVector, BigIntVector, DateDayVector, DurationVector,
  IntervalYearVector, IntVector, TimeNanoVector, VarCharVector}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaProjection
import org.apache.spark.sql.catalyst.expressions.codegen.varka.{VarkaAllocationSampler,
  VarkaEmitDeclined, VarkaEmitOptions, VarkaFallbackEvent, VarkaKernelWarmup,
  VarkaMemorySanitizer, VarkaShapeCache, VarkaShapeKey, VarkaVectorIR}
import org.apache.spark.sql.execution.varka.{VarkaBatchDeclined, VarkaBatchLedger, VarkaClassDump,
  VarkaFallbackAccounting, VarkaKernelFailure, VarkaKernelRunner, VarkaKernelScratch,
  VarkaWarmupGate}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

/**
 * The task-lifetime machinery shared by every Varka evaluator (split out of
 * [[VarkaKernelEvaluator]] when the filter evaluator became its second user), as a composition of
 * the Java components that do the work (VARKA-251):
 *
 *  - [[VarkaBatchLedger]]: the task's Arrow allocator, the open-batch ledger and its
 *    task-completion safety net;
 *  - [[VarkaKernelScratch]]: the derived inputs' buffers and the kernel's prefix scratch;
 *  - [[VarkaKernelRunner]]: the shape-cached kernel, its argument arrays, and filling and running
 *    them per batch;
 *  - [[VarkaWarmupGate]]: whether a batch waits on the row path while the shape's kernel warms;
 *  - [[VarkaFallbackAccounting]]: each fallback counted and evented under its actual cause;
 *  - [[VarkaClassDump]]: the emitted class written for `javap`.
 *
 * What stays here is what the evaluators share and Scala expresses best: the Arrow-backed
 * `canRun` test, the telemetry names, and [[serveBatch]], whose kernel and fallback paths are
 * by-name. A concrete evaluator supplies the compiled fused sub-plan the kernel follows and what
 * its identity reads as in the shape cache's side table; the projection and filter evaluators
 * own everything specific to their output shape - vector allocation and batch assembly there,
 * the selection bitmap here.
 *
 * The ownership and ordering contracts documented on [[VarkaKernelEvaluator]] are implemented
 * here and in the components, and hold for every subclass.
 *
 * @param warmupEnabled whether a shape's batches wait on the row path while its new kernel
 *                      compiles (`spark.sql.codegen.varka.warmup.enabled`; see [[serveBatch]]).
 *                      Off unless the exec node passes the session's setting, so an evaluator
 *                      built directly by a suite runs its kernel on the first batch.
 */
private[sql] abstract class VarkaEvaluatorBase(
    childOutput: Seq[Attribute],
    operatorName: String,
    classDumpDirectory: Option[String],
    metrics: VarkaExecMetrics,
    emitUseAVX: Int = VarkaEmitOptions.USE_AVX_UNKNOWN,
    warmupEnabled: Boolean = false)
    extends Logging {

  /** The fused sub-plan the kernel computes; None when nothing is Varka-eligible. */
  protected def fusedPlan: Option[CompiledVarkaProjection]

  /**
   * The entries rendered into the shape cache's side-table identity, in order - a projection's
   * entries, a filter's condition. Consumed lazily so a wide list is rendered only up to the
   * table's length cap.
   */
  protected def identityEntries: Iterator[String]

  private val ledger = new VarkaBatchLedger

  // Built on first use, since its size is the fused plan's, which a subclass defines after this
  // constructor has run. It grows through `taskAllocator`, which a suite may override to cap.
  private var scratchOrNull: VarkaKernelScratch = null

  private def scratch: VarkaKernelScratch = {
    if (scratchOrNull == null) {
      val numInputs = fusedPlan.map(_.inputOrdinals.size).getOrElse(0)
      scratchOrNull = new VarkaKernelScratch(() => taskAllocator(), numInputs)
    }
    scratchOrNull
  }

  // The task-completion listener closes the open batches, then runs these in order, each guarded
  // on its own, then closes the allocator.
  ledger.onTaskCompletion(() => releaseTaskScratch())
  ledger.onTaskCompletion(() => onTaskCleanup())

  private lazy val accounting = new VarkaFallbackAccounting(
    new VarkaFallbackAccounting.Counters(
      metrics.fallbackBatchesKernel.orNull,
      metrics.fallbackBatchesRowPath.orNull,
      metrics.fallbackBatchesDeclined.orNull,
      metrics.fallbackBatchesNonArrow.orNull,
      metrics.suspectAllocationSamples.orNull),
    () => kernelIdentity)

  // The task-lifetime emitted fused loop and its reused argument arrays. None when emission
  // failed - an IR shape past the emitter's caps, or any linkage problem - in which case every
  // batch takes the caller's fallback path.
  protected lazy val fusedRunner: Option[VarkaKernelRunner] = {
    fusedPlan.flatMap { plan =>
      try {
        Some(newRunner(plan))
      } catch {
        case d: VarkaEmitDeclined =>
          // A shape over the emitter's method budget: the same reason on every task, so it is
          // logged once per JVM, and as a reason rather than a stack trace. The compiler asks the
          // emitter at plan time and demotes what it declines, so this is the last resort: a
          // shape the planning JVM's vector width admitted within the byte or so another width
          // adds (VARKA-169.md 2.2).
          if (VarkaKernelEvaluator.loggedDeclines.add(d.getMessage)) {
            logWarning(s"The Varka emitter declined $kernelIdentity: ${d.getMessage}; " +
              "falling back to the per-row path.")
          }
          metrics.emissionFailures.foreach(_ += 1)
          VarkaFallbackAccounting.fallbackEvent(VarkaFallbackEvent.EMISSION_FAILURE,
            () => kernelIdentity, d.getClass.getName)
          None
        case e if isCatchable(e) =>
          logWarning(s"Failed to emit the Varka fused kernel $kernelIdentity; falling back " +
            "to the per-row path.", e)
          metrics.emissionFailures.foreach(_ += 1)
          VarkaFallbackAccounting.fallbackEvent(VarkaFallbackEvent.EMISSION_FAILURE,
            () => kernelIdentity, e.getClass.getName)
          None
      }
    }
  }

  /**
   * The runner for `plan`: the shape-named class from [[VarkaShapeCache]], whose lookup records
   * this execution (operator, stage, the evaluator's leading entries) in the cache's side table
   * so the class joins back to the plan nodes that ran it; the class dumped where a directory is
   * configured, on hit and miss alike, so a session that configured it after the shape was cached
   * still gets its file; and the plan handed over as arrays, read per batch without boxing.
   */
  private def newRunner(plan: CompiledVarkaProjection): VarkaKernelRunner = {
    if (VarkaColumnarToRowExec.isFailEmissionForTesting) {
      throw new IllegalStateException("injected Varka emission failure")
    }
    val lookup = VarkaShapeCache.getOrEmit(shapeKey(plan), executionIdentity())
    (if (lookup.hit) metrics.cacheHits else metrics.cacheMisses).foreach(_ += 1)
    val entry = lookup.entry
    VarkaClassDump.dump(classDumpDirectory.orNull, entry.sourceFile, entry.classBytes)
    val n = plan.inputOrdinals.size
    new VarkaKernelRunner(entry, plan.lane, plan.inputOrdinals.toArray,
      Array.tabulate(n)(i => plan.derivedAt(i).map(_.kind).orNull),
      plan.inputBounds.map(b => new VarkaKernelRunner.Bound(b.inputIndex, b.lo, b.hi)).toArray,
      plan.outputs.size, plan.literals.toArray, plan.longLiterals.toArray, scratch, accounting,
      VarkaEvaluatorBase.runnerHooks)
  }

  /**
   * Whether this task tried and failed to obtain its kernel class: the plan
   * compiled but the runner could not be built. The exec nodes use it to keep the per-batch
   * fallback cause honest - after an emission failure every batch fails `canRun`, which
   * without this test would count as "input not Arrow-backed".
   */
  private[execution] def emissionFailed: Boolean = fusedPlan.nonEmpty && fusedRunner.isEmpty

  /**
   * This execution's identity - the operator and this task's stage - which goes
   * to [[VarkaShapeCache]]'s side table rather than into the shared class bytes. Outside a
   * task (diagnostics, tests) the stage reads as -1 rather than throwing.
   */
  private def executionName: String = {
    val stage = Option(TaskContext.get()).map(_.stageId()).getOrElse(-1)
    s"Varka_${operatorName}_Stage$stage"
  }

  /**
   * The identity recorded in the cache's side table: the execution name, then as much of the
   * evaluator's entries as the table keeps
   * ([[VarkaShapeCache.MAX_EXECUTION_IDENTITY_LENGTH]]). Bounded while building: rendering all
   * of a wide projection on every task's setup path would be paid only to be truncated on
   * arrival, or discarded outright when the cache is disabled.
   */
  private def executionIdentity(): String = {
    val sb = new StringBuilder(executionName).append(": ")
    val it = identityEntries
    while (it.hasNext && sb.length <= VarkaShapeCache.MAX_EXECUTION_IDENTITY_LENGTH) {
      sb.append(it.next())
      if (it.hasNext) sb.append(", ")
    }
    sb.toString
  }

  /**
   * The cache key of the fused sub-plan: exactly the emitter inputs the bytes follow. The
   * session's `spark.sql.codegen.varka.emit.useAVX`, read on the driver and carried here,
   * is the one production knob on the options; it is applied over the test hook's options
   * rather than instead of them, so a suite that drives a variant and sets the level gets
   * both, and it is left alone at the default so that the hook's own level survives.
   */
  protected def shapeKey(plan: CompiledVarkaProjection): VarkaShapeKey =
    new VarkaShapeKey(plan.outputs.asJava, plan.inputOrdinals.size, plan.numLiterals, emitOptions,
      warmed)

  /**
   * Whether this evaluator's kernels are warmed before they serve batches: the warm-up is on and
   * this JVM can warm ([[VarkaKernelWarmup.canWarm]]). A warmed kernel is its own class, under the
   * name the C1-exclusion directive matches, so a session with the warm-up off - or a JVM that
   * cannot warm - emits and compiles its kernels exactly as it would without the warm-up.
   */
  private lazy val warmed: Boolean = VarkaKernelWarmup.warms(warmupEnabled)

  /** The options this evaluator emits with, which its compiler call must use too. */
  protected def emitOptions: VarkaEmitOptions = VarkaColumnarToRowExec.emitOptions(emitUseAVX)

  /**
   * The kernel named the way its telemetry names it: the
   * `SourceFile` of the shared class, the IR it computes, and this execution's operator and
   * stage. Every fallback warning - here and in the exec nodes - says which kernel it gave up
   * on, so a log line identifies both the class and the plan node without correlation.
   * Reading it forces no emission: the shape hash is computed from the IR, not the bytes.
   * A lazy val (VARKA-21 review): the rendering hashes the canonical IR, and it is constant
   * per evaluator, so per-batch fallback paths must not recompute it.
   *
   * The IR renders through `VarkaVectorIR.canonical` rather than `Record.toString` (with the line
   * map): the same rendering the class's own `VarkaDebugInfo` carries, so a log
   * line and the bytes it names describe the shape the same way - and neither depends on a
   * format no JDK promises.
   */
  private[execution] lazy val kernelIdentity: String = {
    fusedPlan match {
      case Some(plan) =>
        val ir = plan.outputs.map(VarkaVectorIR.canonical).mkString(", ")
        val hash = VarkaShapeCache.shapeHash(shapeKey(plan))
        s"${VarkaShapeCache.sourceFileFor(hash)} [$ir] ($executionName)"
      case None => s"[no compiled projection] ($executionName)"
    }
  }

  /**
   * The emitted fused-kernel class's bytes, exactly as defined - the diagnostics hook behind
   * the telemetry note in [[VarkaKernelEvaluator]]'s class doc: `VarkaDebugInfo.read` and
   * `ClassFile.parse` recover the IR, the plan fragment and the `SourceFile` name from them.
   * Forces emission if no batch has done so yet; None when the plan is ineligible or emission
   * failed.
   */
  private[execution] def emittedClassBytes: Option[Array[Byte]] = fusedRunner.map(_.classBytes)

  /**
   * Runs this evaluator's kernel over the input batch into vectors `allocate` makes from
   * `allocator`, appending each to `owned` as it is created (the caller closes `owned` on
   * failure), and returns them by the kernel's output index. The projection evaluator calls it
   * on itself and on each further kernel of a projection several kernels serve
   * (`VarkaEmitOptions.severalKernels`), so every kernel's columns come from the one allocator
   * and join the one output batch. Callers must have asked [[canRun]] first.
   */
  private[execution] def runKernel(
      input: ColumnarBatch,
      len: Int,
      owned: mutable.ArrayBuffer[ColumnVector],
      allocator: BufferAllocator,
      allocate: (DataType, Int, Int, BufferAllocator) => BaseFixedWidthVector)
      : Array[ColumnVector] = {
    val plan = fusedPlan.get
    val runner = fusedRunner.get
    // Under the memory sanitizer (VARKA-263) every buffer the kernel is handed is registered in
    // this window, and a mapping outside them fails; off, `begin` and `end` do nothing.
    VarkaMemorySanitizer.begin()
    try {
      runner.fill(input, len)
      val fixed = new Array[BaseFixedWidthVector](plan.outputs.size)
      val columns = new Array[ColumnVector](plan.outputs.size)
      var o = 0
      // Under the sanitizer an output has a tail of rows past `len` for its canary to sit in; the
      // vector's value count is still `len`, so nothing downstream sees them.
      val rows = if (VarkaMemorySanitizer.ENABLED) len + VarkaMemorySanitizer.CANARY_ROWS else len
      plan.outputTypes.foreach { dataType =>
        val vector = allocate(dataType, o, rows, allocator)
        fixed(o) = vector
        columns(o) = new VarkaOwnedArrowColumnVector(vector)
        owned += columns(o)
        runner.dstData(o) = vector.getDataBuffer().memoryAddress()
        runner.dstValidity(o) = vector.getValidityBuffer().memoryAddress()
        if (VarkaMemorySanitizer.ENABLED) {
          // The kernel's own bytes: `len` values, and the validity bitmap to its last whole word.
          VarkaMemorySanitizer.guard("output data", o, vector.getDataBuffer(),
            len.toLong * vector.getTypeWidth)
          VarkaMemorySanitizer.guard("output validity", o, vector.getValidityBuffer(),
            ((len + 63) / 64) * 8L)
        }
        o += 1
      }
      runner.invoke(len)
      fixed.foreach(_.setValueCount(len))
      columns
    } finally {
      VarkaMemorySanitizer.end()
    }
  }

  /**
   * Whether the kernel can serve this batch, or the caller has to fall back. The Arrow check
   * covers only the columns the fused sub-plan references: other entries put no constraint on
   * the input format beyond what `rowIterator` needs.
   */
  def canRun(input: ColumnarBatch): Boolean = {
    (fusedPlan, fusedRunner) match {
      case (Some(plan), Some(_)) => input.numRows() > 0 && isArrowBacked(plan, input)
      case _ => false
    }
  }

  /**
   * The per-batch dispatch every exec node runs (VARKA-21 review, second pass: the
   * canRun/catch/refuse skeleton had grown into four identical copies): the kernel path under the
   * shared cause accounting ([[VarkaFallbackAccounting]]), with every degradation routed to the
   * caller's fallback.
   *
   * With the warm-up on, a batch the kernel could serve still takes the row path while the
   * shape's kernel is not compiled yet ([[VarkaWarmupGate]]). That is not a fallback and is
   * counted apart from them: the row path is the faster of the two until C2 has the kernel. A
   * batch the evaluator declines while it is copied for the warm-up is counted as the declined
   * batch it is, as it would be on the kernel path. A batch that is not Arrow because a Varka
   * node below sent it down its own row path while its kernel warmed is a warm-up batch here too,
   * and so is this node's output for it, for the node above.
   */
  private[execution] def serveBatch[T](input: ColumnarBatch)(kernelPath: => T)(
      fallbackPath: => T): T = {
    if (canRun(input)) {
      val ready =
        try {
          Right(kernelReady(input))
        } catch {
          case e: VarkaBatchDeclined => Left(e)
        }
      ready match {
        case Left(declined) =>
          accounting.declinedBatch(declined.status, declined.kernel)
          fallbackPath
        case Right(false) =>
          metrics.warmupBatches.foreach(_ += 1)
          VarkaKernelEvaluator.markWarmupBatch(fallbackPath)
        case Right(true) =>
          try {
            kernelPath
          } catch {
            // Not a failure: the kernel ran and said it could not answer for this batch.
            case e: VarkaBatchDeclined =>
              accounting.declinedBatch(e.status, e.kernel)
              fallbackPath
            // A genuine kernel error is told apart from a failure in the per-row machinery
            // sharing the try by the marker the runner wraps it in.
            case e: VarkaKernelFailure =>
              accounting.kernelFailure(e.getCause, e.kernel)
              fallbackPath
            case e if isCatchable(e) =>
              accounting.rowPathFailure(e)
              fallbackPath
          }
      }
    } else if (recordRefusedBatch(input)) {
      VarkaKernelEvaluator.markWarmupBatch(fallbackPath)
    } else {
      fallbackPath
    }
  }

  private lazy val warmupGate = new VarkaWarmupGate(fusedRunner.get, anyNullableInput,
    (input: ColumnarBatch) => inputWidths(input), () => kernelIdentity)

  /** Whether this batch goes to the kernel; see [[VarkaWarmupGate]]. */
  private[execution] def kernelReady(input: ColumnarBatch): Boolean =
    !warmed || warmupGate.kernelReady(input)

  /**
   * Whether any kernel input can hold a null, so that a batch can reach the kernel's masked
   * driver: a column the child declares nullable, or a derived input, whose derivation may
   * produce one. The warm-up compiles the masked driver only then.
   */
  private def anyNullableInput: Boolean = {
    val plan = fusedPlan.get
    plan.inputOrdinals.indices.exists { i =>
      plan.derivedAt(i).isDefined || childOutput(plan.inputOrdinals(i)).nullable
    }
  }

  /** Each kernel input's bytes per row: its Arrow vector's width, four for a derived input. */
  private def inputWidths(input: ColumnarBatch): Array[Int] = {
    val plan = fusedPlan.get
    plan.inputOrdinals.indices.map { i =>
      if (plan.derivedAt(i).isDefined) {
        4
      } else {
        input.column(plan.inputOrdinals(i)).asInstanceOf[ArrowColumnVector].getValueVector()
          .asInstanceOf[BaseFixedWidthVector].getTypeWidth()
      }
    }.toArray
  }

  /**
   * A batch [[canRun]] refused, counted under its actual cause (VARKA-21 review: the nodes
   * used to label every refusal "input not Arrow-backed"): an emission failure was already
   * counted once per task by the emission catch; an empty batch is served trivially and is
   * no fallback at all; an ineligible plan (defensive - the rule should not have fused it)
   * is not a data-format property. Only a non-empty batch whose referenced columns fail the
   * Arrow check is the non-Arrow cause the metric names - unless a Varka node below produced it
   * on its row path while its kernel warmed, which is a warm-up batch here as well. Returns
   * whether it was that.
   */
  private def recordRefusedBatch(input: ColumnarBatch): Boolean = {
    if (!emissionFailed && fusedPlan.nonEmpty && input.numRows() > 0) {
      if (VarkaKernelEvaluator.isWarmupBatch(input)) {
        metrics.warmupBatches.foreach(_ += 1)
        true
      } else {
        accounting.nonArrowBatch()
        false
      }
    } else {
      false
    }
  }

  /**
   * Takes ownership of a batch the caller built itself; see [[VarkaBatchLedger.track]].
   */
  def track(batch: ColumnarBatch): ColumnarBatch = ledger.track(batch)

  /**
   * The output batch for a projection that only forwards columns of its input; see
   * [[VarkaBatchLedger.forwardColumns]].
   */
  def forwardColumns(input: ColumnarBatch, ordinals: Array[Int]): ColumnarBatch =
    ledger.forwardColumns(input, ordinals)

  protected def trackOwned(batch: ColumnarBatch, owned: Array[ColumnVector]): Unit =
    ledger.trackOwned(batch, owned)

  /**
   * Releases a batch obtained from this evaluator or handed to [[track]]; see
   * [[VarkaBatchLedger.release]].
   */
  def release(batch: ColumnarBatch): Unit = ledger.release(batch)

  /** A kernel failure worth falling back on, rather than one that has to fail the task. */
  def isCatchable(e: Throwable): Boolean = VarkaKernelRunner.isCatchable(e)

  /**
   * Whether the kernel can run over this batch: every referenced column must be an Arrow
   * vector of a class the kernels read - four bytes wide (`DateDayVector`, `IntVector`,
   * `IntervalYearVector`) or eight (`BigIntVector`, `TimeNanoVector`, `DurationVector`) -
   * holding exactly the batch's
   * rows, no more - or, for an input the evaluator derives, an Arrow `VarCharVector`
   * of the same row count, the one string vector the Arrow cache produces and the derived
   * leaf reads; the large and view string vectors refuse the batch like any other column type.
   *
   * The row count matters because the kernel takes a null count for the rows it is given,
   * while a vector's null count covers all `valueCount` of its rows. A vector longer than the
   * batch would hand it a count for rows that are not in it - and a vector whose extra rows
   * happen to hold every null would make that count equal the batch's row count, tripping the
   * all-null shortcut over rows that are not null at all. Such a batch takes the caller's
   * fallback; serving it from the kernels would mean counting nulls over `[0, len)` here
   * instead.
   */
  private def isArrowBacked(plan: CompiledVarkaProjection, input: ColumnarBatch): Boolean = {
    // Indexed rather than `zipWithIndex.forall`: this runs once per batch for every Varka
    // query, and zipping allocates a tuple per input column each time on a gate that was
    // otherwise allocation-free.
    val ordinals = plan.inputOrdinals
    val rows = input.numRows()
    var i = 0
    while (i < ordinals.length) {
      val ok = input.column(ordinals(i)) match {
        case acv: ArrowColumnVector =>
          (acv.getValueVector(), plan.derivedAt(i)) match {
            case (v: DateDayVector, None) => v.getValueCount() == rows
            case (v: IntVector, None) => v.getValueCount() == rows
            // a year-month interval is a count of months in an int32 buffer whatever
            // its unit, and `IntervalYearVector` is a BaseFixedWidthVector of width four - the
            // same buffer layout the kernels already read. The list is by vector class rather
            // than by Spark type, so admitting the type is exactly this line: the serializer
            // already stores such a column and `ArrowColumnVector` already reads it back.
            case (v: IntervalYearVector, None) => v.getValueCount() == rows
            // The long lane's three (VARKA-29): a `bigint`, a `TIME(p)` - nanoseconds of day at
            // every precision - and a day-time interval in microseconds. All three are
            // `BaseFixedWidthVector`s of width eight, and VARKA-116 proved the two datetime ones
            // map through the runner's `fill` exactly as the int vectors do. The timestamp
            // vectors are deliberately not here: the compiler never builds a leaf for them, so
            // admitting them would only decline the batch one layer later.
            case (v: BigIntVector, None) => v.getValueCount() == rows
            case (v: TimeNanoVector, None) => v.getValueCount() == rows
            case (v: DurationVector, None) => v.getValueCount() == rows
            case (v: VarCharVector, Some(_)) => v.getValueCount() == rows
            case _ => false
          }
        case _ => false
      }
      if (!ok) {
        return false
      }
      i += 1
    }
    true
  }

  /** A subclass's extra cleanup, run by the task-completion listener before the allocator
   * closes - the filter evaluator releases its selection buffer here. */
  protected def onTaskCleanup(): Unit = {}

  /**
   * Releases this evaluator's task-lifetime scratch - the derived inputs' buffers and the
   * kernel's prefix scratch - for an owner whose cleanup does it: a further kernel of a
   * projection allocates from the projection's allocator and registers no listener of its own,
   * so the projection's evaluator releases its scratch before it closes that allocator.
   */
  private[execution] def releaseTaskScratch(): Unit = {
    if (scratchOrNull != null) {
      scratchOrNull.release()
    }
  }

  /** See [[VarkaBatchLedger.closeQuietly]]. */
  protected def closeQuietly(c: AutoCloseable, what: String): Unit =
    VarkaBatchLedger.closeQuietly(c, what)

  /** [[closeQuietly]] over a collection, guarding each element separately. */
  protected def closeAllQuietly(cs: Iterable[_ <: AutoCloseable], what: String): Unit =
    cs.foreach(closeQuietly(_, what))

  /** Registers the single task-completion listener; see [[VarkaBatchLedger.ensureCleanup]]. */
  protected def ensureCleanup(): Unit = ledger.ensureCleanup()

  /** Returns the task's Arrow child allocator, creating it on first use. */
  protected def taskAllocator(): BufferAllocator = ledger.allocator()
}

private object VarkaEvaluatorBase {

  /** The runner's test hooks, read per batch from the exec nodes' test switches. */
  val runnerHooks: VarkaKernelRunner.Hooks = new VarkaKernelRunner.Hooks {
    override def failKernel(): Boolean = VarkaColumnarToRowExec.isFailKernelForTesting
    override def declineKernel(): Boolean = VarkaColumnarToRowExec.isDeclineKernelForTesting
    override def allocationSchedule(): VarkaAllocationSampler.Schedule =
      VarkaKernelEvaluator.allocationSchedule
  }
}
