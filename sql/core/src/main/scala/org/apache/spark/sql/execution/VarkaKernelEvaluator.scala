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
import java.lang.foreign.MemorySegment
import java.nio.file.Files

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.{BaseFixedWidthVector, DateDayVector, IntVector, ValueVector}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.{CompiledVarkaProjection, ForwardedOutput, FusedOutput, PartialVarkaProjection, ResidualOutput, VarkaExpressionCompiler}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.{VarkaFallbackEvent, VarkaFusedKernel, VarkaSelectionBitmap, VarkaShapeCache, VarkaShapeKey}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{DateType, IntegerType, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

/**
 * The task-lifetime machinery shared by every Varka evaluator (split out of
 * [[VarkaKernelEvaluator]] in task 21, when the filter evaluator became its second user): the
 * shape-cached kernel runner and its argument arrays, the task's Arrow allocator, the
 * open-batch ledger with its task-completion safety net, the Arrow-backed `canRun` test, and
 * the telemetry names. A concrete evaluator supplies the compiled fused sub-plan the kernel
 * follows and what its identity reads as in the shape cache's side table; the projection and
 * filter evaluators below own everything specific to their output shape - vector allocation
 * and batch assembly there, the selection bitmap here.
 *
 * The ownership and ordering contracts documented on [[VarkaKernelEvaluator]] are implemented
 * here and hold for every subclass.
 */
private[sql] abstract class VarkaEvaluatorBase(
    childOutput: Seq[Attribute],
    operatorName: String,
    classDumpDirectory: Option[String],
    metrics: VarkaExecMetrics)
    extends Logging {

  /** The fused sub-plan the kernel computes; None when nothing is Varka-eligible. */
  protected def fusedPlan: Option[CompiledVarkaProjection]

  /**
   * The entries rendered into the shape cache's side-table identity, in order - a projection's
   * entries, a filter's condition. Consumed lazily so a wide list is rendered only up to the
   * table's length cap.
   */
  protected def identityEntries: Iterator[String]

  // One Arrow child allocator for the whole task, created on the first kernel batch. Allocating
  // one per batch - and registering a task-completion listener per batch to close it - would
  // hold every result batch off-heap until the task ended, which is exactly what the streaming
  // iterator model exists to avoid.
  private var kernelAllocator: BufferAllocator = null

  // Batches handed out and not released yet, each mapped to the vectors this evaluator owns in
  // it - never the forwarded input vectors (see the ownership note in the class doc). A batch
  // is normally released by the caller as soon as it is done with it; the map is the safety net
  // for a task that stops early (a LIMIT, a failure) and is drained by the task-completion
  // listener.
  private val openBatches = mutable.Map.empty[ColumnarBatch, Seq[ColumnVector]]

  private var cleanupRegistered = false

  // The task-lifetime emitted fused loop and its reused argument arrays. None when emission
  // failed - an IR shape past the emitter's caps, or any linkage problem - in which case every
  // batch takes the caller's fallback path.
  protected lazy val fusedRunner: Option[FusedRunner] = {
    fusedPlan.flatMap { plan =>
      try {
        Some(new FusedRunner(plan))
      } catch {
        case e if isCatchable(e) =>
          logWarning(s"Failed to emit the Varka fused kernel $kernelIdentity; falling back " +
            "to the per-row path.", e)
          metrics.emissionFailures.foreach(_ += 1)
          VarkaKernelEvaluator.emitFallbackEvent("emission-failure", kernelIdentity,
            e.getClass.getName)
          None
      }
    }
  }

  /**
   * Whether this task tried and failed to obtain its kernel class (task 22): the plan
   * compiled but the runner could not be built. The exec nodes use it to keep the per-batch
   * fallback cause honest - after an emission failure every batch fails `canRun`, which
   * without this test would count as "input not Arrow-backed".
   */
  private[execution] def emissionFailed: Boolean = fusedPlan.nonEmpty && fusedRunner.isEmpty

  /**
   * This execution's identity - the operator and this task's stage - which since task 18 goes
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
   * ([[VarkaShapeCache.maxExecutionIdentityLength]]). Bounded while building: rendering all
   * of a wide projection on every task's setup path would be paid only to be truncated on
   * arrival, or discarded outright when the cache is disabled.
   */
  private def executionIdentity(): String = {
    val sb = new StringBuilder(executionName).append(": ")
    val it = identityEntries
    while (it.hasNext && sb.length <= VarkaShapeCache.maxExecutionIdentityLength) {
      sb.append(it.next())
      if (it.hasNext) sb.append(", ")
    }
    sb.toString
  }

  /** The cache key of the fused sub-plan: exactly the emitter inputs the bytes follow. */
  protected def shapeKey(plan: CompiledVarkaProjection): VarkaShapeKey =
    VarkaShapeKey(plan.outputs, plan.inputOrdinals.size, plan.literals.size)

  /**
   * The kernel named the way its telemetry names it (task 16, shape-based since task 18): the
   * `SourceFile` of the shared class, the IR it computes, and this execution's operator and
   * stage. Every fallback warning - here and in the exec nodes - says which kernel it gave up
   * on, so a log line identifies both the class and the plan node without correlation.
   * Reading it forces no emission: the shape hash is computed from the IR, not the bytes.
   */
  private[execution] def kernelIdentity: String = {
    fusedPlan match {
      case Some(plan) =>
        val ir = plan.outputs.mkString(", ")
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
   * Takes ownership of a batch the caller built itself - a fallback batch, every column the
   * caller's own - so that the same task-completion listener closes it if the task stops before
   * the caller releases it.
   */
  def track(batch: ColumnarBatch): ColumnarBatch = {
    trackOwned(batch, (0 until batch.numCols()).map(batch.column))
    batch
  }

  protected def trackOwned(batch: ColumnarBatch, owned: Seq[ColumnVector]): Unit = {
    ensureCleanup()
    openBatches(batch) = owned
  }

  /**
   * Releases a batch obtained from this evaluator or handed to [[track]]: closes exactly the
   * vectors this evaluator owns in it, so a forwarded input vector is left to its input batch.
   */
  def release(batch: ColumnarBatch): Unit = {
    openBatches.remove(batch) match {
      case Some(owned) => owned.foreach(_.close())
      // Not one of ours - nothing borrowed can be inside, so closing it whole is safe.
      case None => batch.close()
    }
  }

  /** A kernel failure worth falling back on, rather than one that has to fail the task. */
  def isCatchable(e: Throwable): Boolean = {
    NonFatal(e) || e.isInstanceOf[LinkageError]
  }

  /**
   * Whether the kernel can run over this batch: every referenced column must be an Arrow
   * `DateDayVector` holding exactly the batch's rows, no more.
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
    plan.inputOrdinals.forall { ordinal =>
      input.column(ordinal) match {
        case acv: ArrowColumnVector =>
          acv.getValueVector() match {
            case ddv: DateDayVector => ddv.getValueCount() == input.numRows()
            case _ => false
          }
        case _ => false
      }
    }
  }

  /** A subclass's extra cleanup, run by the task-completion listener before the allocator
   * closes - the filter evaluator releases its selection buffer here. */
  protected def onTaskCleanup(): Unit = {}

  /**
   * Registers the single task-completion listener that closes any batch still open and then the
   * allocator. Both this and [[taskAllocator]] are called from the task thread only.
   */
  protected def ensureCleanup(): Unit = {
    if (!cleanupRegistered) {
      cleanupRegistered = true
      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        openBatches.foreach { case (_, owned) => owned.foreach(_.close()) }
        openBatches.clear()
        onTaskCleanup()
        if (kernelAllocator != null) {
          kernelAllocator.close()
          kernelAllocator = null
        }
      }
    }
  }

  /** Returns the task's Arrow child allocator, creating it on first use. */
  protected def taskAllocator(): BufferAllocator = {
    ensureCleanup()
    if (kernelAllocator == null) {
      kernelAllocator =
        ArrowUtils.rootAllocator.newChildAllocator("varka-kernels", 0, Long.MaxValue)
    }
    kernelAllocator
  }

  /**
   * Writes the emitted class to the configured dump directory under its `SourceFile` name
   * (task 16), so `javap -c -p` reaches a generated loop with no debugger. Diagnostics only:
   * every failure is logged and swallowed, because a query must not fail over a debug write.
   * Every task of a shape holds identical bytes (task 18), so a per-JVM memo makes the
   * shape's first task with the directory configured write the file once, instead of every
   * task re-writing it on the task-setup path. The memo is per-process on purpose: the file
   * name derives from the shape, not the bytes, so a file left by an *older* emitter must be
   * overwritten, not trusted - each JVM's first write refreshes it. (Two first tasks can
   * still race past the memo; they write the same bytes, so the race is benign.)
   */
  private def dumpClass(sourceFile: String, bytes: Array[Byte]): Unit = {
    classDumpDirectory.foreach { directory =>
      val memoKey = s"$directory|$sourceFile"
      if (VarkaKernelEvaluator.dumpedClassFiles.add(memoKey)) {
        try {
          val target = new File(directory, sourceFile.stripSuffix(".java") + ".class")
          Files.createDirectories(target.toPath.getParent)
          Files.write(target.toPath, bytes)
          logInfo(s"Wrote the Varka kernel class to ${target.getAbsolutePath}")
        } catch {
          case NonFatal(e) =>
            VarkaKernelEvaluator.dumpedClassFiles.remove(memoKey)
            logWarning(s"Could not dump the Varka kernel class to $directory; " +
              "execution is unaffected.", e)
        }
      }
    }
  }

  /**
   * Fills the runner's source-side argument arrays from the input batch - one morsel per
   * referenced input column, in dense kernel-input order. `canRun` has vouched for every
   * column this reads.
   */
  protected def fillSources(runner: FusedRunner, input: ColumnarBatch, len: Int): Unit = {
    val plan = fusedPlan.get
    var i = 0
    plan.inputOrdinals.foreach { ordinal =>
      val acv = input.column(ordinal).asInstanceOf[ArrowColumnVector]
      val morsel = extractMorsel(acv.getValueVector().asInstanceOf[DateDayVector], len)
      runner.srcData(i) = morsel.data.address()
      runner.srcValidity(i) = morsel.validityAddress
      runner.srcNullCount(i) = morsel.nullCount.toInt
      i += 1
    }
  }

  protected def invokeFused(runner: FusedRunner, len: Int): Unit = {
    if (VarkaColumnarToRowExec.isFailKernelForTesting) {
      // scalastyle:off throwerror
      throw new NoClassDefFoundError("injected Varka kernel failure")
      // scalastyle:on throwerror
    }
    runner.kernel.run(runner.srcData, runner.srcValidity, runner.srcNullCount,
      runner.dstData, runner.dstValidity, runner.scalarArgs, len)
  }

  /**
   * Maps a `DateDayVector` to its data and validity segments (zero-copy), mirroring the
   * engine's `VarkaMorsel.extractDate` contract: the validity segment is null for an all-null
   * column, and callers pass a `0L` address in that case because the kernels never
   * dereference it then.
   *
   * The vector must hold exactly the batch's rows, which `isArrowBacked` has already checked -
   * that is what makes the vector's null count the batch's null count, and so what makes the
   * all-null test below sound.
   */
  private def extractMorsel(ddv: DateDayVector, len: Int): Morsel = {
    require(len == ddv.getValueCount(),
      s"rowCount $len does not match the vector value count ${ddv.getValueCount()}")
    val data = ofAddress(ddv.getDataBuffer())
    val nullCount = ddv.getNullCount()
    val validity = if (nullCount == len) null else ofAddress(ddv.getValidityBuffer())
    Morsel(data, validity, nullCount)
  }

  private def ofAddress(buf: ArrowBuf): MemorySegment = {
    MemorySegment.ofAddress(buf.memoryAddress()).reinterpret(buf.capacity())
  }

  /**
   * The fused loop serving one task, plus the `run` argument arrays, allocated once here and
   * refilled per batch - nothing is allocated per call. Since task 18 the class comes from
   * [[VarkaShapeCache]] - shared across tasks and released on cache eviction, so its C2 code
   * survives the task boundary - and only the kernel instance and these arrays are the
   * task's own. The cache owns the loader in every configuration: with `maxEntries` = 0 it
   * evicts (and releases) each entry as it is loaded, and this task's strong references
   * carry the class to task end - the pre-task-18 lifecycle through the same path.
   */
  protected class FusedRunner(plan: CompiledVarkaProjection) {
    // The lookup records this execution (operator, stage, the evaluator's leading entries)
    // in the cache's side table, so the shape-named class joins back to the plan nodes that
    // ran it.
    private val lookup = VarkaShapeCache.getOrEmit(shapeKey(plan), executionIdentity())
    private val entry = lookup.entry
    (if (lookup.hit) metrics.cacheHits else metrics.cacheMisses).foreach(_ += 1)

    val sourceFile: String = entry.sourceFile

    val classBytes: Array[Byte] = {
      // dumpClass writes once per shape and directory (an existing file is left alone), and
      // runs on hit and miss alike so a session that configured the dump directory after the
      // shape was cached still gets its file.
      dumpClass(sourceFile, entry.classBytes)
      entry.classBytes
    }

    val kernel: VarkaFusedKernel = entry.newKernel()

    val srcData = new Array[Long](plan.inputOrdinals.size)
    val srcValidity = new Array[Long](plan.inputOrdinals.size)
    val srcNullCount = new Array[Int](plan.inputOrdinals.size)
    val dstData = new Array[Long](plan.outputs.size)
    val dstValidity = new Array[Long](plan.outputs.size)
    val scalarArgs: Array[Int] = plan.literals.toArray
  }
}

/**
 * The kernel half of the Varka projection, for one partition: it turns an input `ColumnarBatch`
 * into a batch of the projection's output, and owns everything that costs a task to set up - the
 * compiled IR, the fused-loop kernel instance, the Arrow allocator and the batches handed out.
 *
 * Since task 10 the compute is one [[VarkaFusedKernel]] emitted by
 * `VarkaLoopEmitter` for the whole projection - every output computed in a single pass with
 * intermediates in vector registers - instead of one dispatcher call per output op. The
 * projection is compiled to IR by [[VarkaExpressionCompiler]], the same call
 * `VarkaColumnarRule` decided eligibility with, so the plan the rule fused is by construction a
 * plan this evaluator serves. Since task 18 the emitted ''class'' is not per-task state: it
 * comes from [[VarkaShapeCache]], the JVM-wide cache keyed on the kernel's structural shape,
 * so tasks (and sessions) computing the same shape share one loaded class and skip its
 * per-task JIT warm-up - the fixed 13-50 ms `PLAN_TASK_14.md` 7.5 diagnosed. Only the kernel
 * ''instance'' and its argument arrays stay per-task.
 *
 * Since task 12 eligibility is partial and the output batch is assembled column by column in
 * projection order: fused entries come from the kernel's freshly allocated Arrow vectors,
 * bare-column entries are '''forwarded''' - the output batch references `input.column(ordinal)`
 * itself, zero copy - and the remaining ('''residual''') entries are evaluated in one per-row
 * pass over the input into writable vectors.
 *
 * '''Ownership.''' The evaluator owns the vectors it allocated - kernel outputs and residual
 * columns - and never the forwarded ones, which belong to whoever owns the input batch. Every
 * release path (the caller's [[release]], and the task-completion listener that drains
 * abandoned batches) closes exactly the owned vectors of a batch and never calls
 * `ColumnarBatch.close()`, which would close every column unconditionally, forwarded ones
 * included. This follows Spark's own two-tier convention (`closeIfFreeable` and its no-op
 * overrides) rather than a wrapper class: the borrowed vector simply stays off the owned list.
 *
 * '''Ordering contract.''' Forwarded vectors make the output batch valid only as long as its
 * input batch: both exec nodes therefore release the output batch '''before''' requesting the
 * next input batch from the child, so a forwarded vector can never outlive its input. The
 * nodes' iterators already obeyed this order for memory reasons; with forwarding it is
 * load-bearing for correctness.
 *
 * '''Telemetry''' (tasks 13 and 16, reconciled with the shared class in task 18). The emitted
 * class is named by its shape (`VarkaFusedProjection_<hash>`, `SourceFile` to match), and its
 * `VarkaDebugInfo` attribute and `LineNumberTable` describe the shape - the vector IR, the
 * line-to-node map - because the bytes are shared and must not replay one query's identity for
 * another. The per-execution identity that used to ride the bytes (operator, stage, this
 * projection's expression list) is recorded in [[VarkaShapeCache]]'s side table on every
 * lookup, keyed by the shape hash, and every fallback this class logs still names both halves
 * ([[kernelIdentity]]: the shape name, the IR, and the operator/stage). The bytes are kept
 * behind [[emittedClassBytes]] so diagnostics read the attributes off exactly what ran, and
 * `spark.sql.codegen.varka.classDumpDirectory` writes them to disk under the `SourceFile`
 * name, so `javap` reaches a generated loop with no debugger attached.
 *
 * One instance per partition, created inside the task: it registers a task-completion listener
 * on first use, and its state must not be shared across partitions (see [[SafeForKWayMerge]]).
 *
 * @param operatorName the exec node this evaluator serves, for the telemetry names above.
 * @param classDumpDirectory where to write each emitted class, or None to write none.
 * @param metrics the exec node's Varka metric set (task 22); every field is optional, and
 *                suites or diagnostics that construct the evaluator directly pass none.
 */
private[sql] class VarkaKernelEvaluator(
    projectList: Seq[NamedExpression],
    childOutput: Seq[Attribute],
    offHeapColumnVectorEnabled: Boolean,
    operatorName: String,
    classDumpDirectory: Option[String] = None,
    metrics: VarkaExecMetrics = VarkaExecMetrics())
    extends VarkaEvaluatorBase(childOutput, operatorName, classDumpDirectory, metrics) {

  // The projection classified entry by entry and its fused sub-projection compiled to vector
  // IR; None when no entry is Varka-eligible (should not happen given [[VarkaColumnarRule]],
  // but be safe).
  private lazy val compiled: Option[PartialVarkaProjection] = {
    val partial = VarkaExpressionCompiler.compilePartial(projectList, childOutput)
    // Task 16: the same per-entry account verbose EXPLAIN prints, once per task at debug level.
    partial.foreach { plan =>
      logDebug(s"Varka $operatorName fusion: " +
        VarkaFusionReport.lines(plan, projectList, childOutput).mkString("; "))
    }
    partial
  }

  override protected def fusedPlan: Option[CompiledVarkaProjection] = compiled.map(_.fused)

  override protected def identityEntries: Iterator[String] = projectList.iterator.map(_.toString)

  // The residual entries and their per-row machinery. All lazy (task 15's discipline): a
  // kernel-only projection has no residual entries, and even a mixed one pays the Janino
  // compile only when the first batch actually reaches [[project]].
  private lazy val residualExprs: Seq[NamedExpression] =
    compiled.toSeq.flatMap(_.specs.zip(projectList).collect {
      case (ResidualOutput, named) => named
    })
  private lazy val residualSchema: StructType =
    DataTypeUtils.fromAttributes(residualExprs.map(_.toAttribute))
  private lazy val residualProjection = UnsafeProjection.create(residualExprs, childOutput)
  private lazy val residualConverter = new RowToColumnConverter(residualSchema)

  /** The classified projection, for the row node's merge-at-row read-back (see 2.3). */
  private[execution] def partialPlan: Option[PartialVarkaProjection] = compiled

  /**
   * Runs the fused kernel over the input batch, evaluates the residual entries per row,
   * forwards the bare-column entries, and returns the assembled output batch, tracked here
   * until the caller [[release]]s it. Callers must have asked [[canRun]] first, and must treat
   * a throw as "this batch could not be served": nothing is left allocated by a failed call.
   */
  def project(input: ColumnarBatch): ColumnarBatch = {
    val partial = compiled.get
    val len = input.numRows()
    // Everything allocated for this batch - kernel outputs, then residual columns - closed on
    // any failure here, and by release()/the listener once the batch is handed out. Forwarded
    // input vectors never join this list: they stay owned by the input batch.
    val owned = mutable.ArrayBuffer.empty[ColumnVector]
    try {
      val fusedColumns = computeFused(input, len, owned)
      val residualColumns = projectResiduals(input, len)
      owned ++= residualColumns
      var residual = 0
      val columns = partial.specs.map {
        case FusedOutput(index) => fusedColumns(index)
        case ForwardedOutput(ordinal) => input.column(ordinal)
        case ResidualOutput =>
          residual += 1
          residualColumns(residual - 1)
      }.toArray
      val batch = new ColumnarBatch(columns)
      batch.setNumRows(len)
      trackOwned(batch, owned.toSeq)
      batch
    } catch {
      case e: Throwable =>
        owned.foreach(_.close())
        throw e
    }
  }

  /**
   * Runs only the fused kernel and returns a batch of just its columns, tracked like
   * [[project]]'s. This is the row node's entry point (merge-at-row, `PLAN_TASK_12.md` 2.3):
   * it reads fused values from this batch and evaluates residual entries during its own row
   * pass, so materialising them into vectors here would be pure waste. Nothing in it is
   * borrowed - fused columns are always freshly allocated.
   */
  def projectFused(input: ColumnarBatch): ColumnarBatch = {
    val len = input.numRows()
    val owned = mutable.ArrayBuffer.empty[ColumnVector]
    try {
      val fusedColumns = computeFused(input, len, owned)
      val batch = new ColumnarBatch(fusedColumns)
      batch.setNumRows(len)
      trackOwned(batch, owned.toSeq)
      batch
    } catch {
      case e: Throwable =>
        owned.foreach(_.close())
        throw e
    }
  }

  /**
   * Runs the fused kernel over the input batch into freshly allocated Arrow vectors, appending
   * them to `owned` as they are created (the caller closes `owned` on failure). Returns the
   * fused columns by fused index.
   */
  private def computeFused(
      input: ColumnarBatch,
      len: Int,
      owned: mutable.ArrayBuffer[ColumnVector]): Array[ColumnVector] = {
    val plan = compiled.get.fused
    val runner = fusedRunner.get
    val alloc = taskAllocator()
    fillSources(runner, input, len)
    val fixed = new Array[BaseFixedWidthVector](plan.outputs.size)
    val fusedColumns = new Array[ColumnVector](plan.outputs.size)
    var o = 0
    plan.outputTypes.foreach { dataType =>
      val vector = allocateVector(dataType, o, len, alloc)
      fixed(o) = vector
      fusedColumns(o) = new VarkaOwnedArrowColumnVector(vector)
      owned += fusedColumns(o)
      runner.dstData(o) = vector.getDataBuffer().memoryAddress()
      runner.dstValidity(o) = vector.getValidityBuffer().memoryAddress()
      o += 1
    }
    invokeFused(runner, len)
    fixed.foreach(_.setValueCount(len))
    fusedColumns
  }

  /**
   * Evaluates all residual entries in one per-row pass over the input, into writable vectors
   * sized to the batch. Returns the columns in residual-entry order; empty when the projection
   * has no residual entries.
   */
  private def projectResiduals(input: ColumnarBatch, len: Int): Seq[ColumnVector] = {
    if (residualExprs.isEmpty) {
      Seq.empty
    } else {
      val vectors: Array[WritableColumnVector] = if (offHeapColumnVectorEnabled) {
        OffHeapColumnVector.allocateColumns(len, residualSchema).toArray[WritableColumnVector]
      } else {
        OnHeapColumnVector.allocateColumns(len, residualSchema).toArray[WritableColumnVector]
      }
      try {
        val rows = input.rowIterator()
        while (rows.hasNext) {
          residualConverter.convert(residualProjection(rows.next()), vectors)
        }
      } catch {
        case e: Throwable =>
          vectors.foreach(_.close())
          throw e
      }
      vectors.toSeq
    }
  }

  /**
   * Allocates one destination Arrow vector: a `DateDayVector` for a date output, an `IntVector`
   * for a `datediff` day count. The fused loop writes its validity and data buffers directly
   * (zero-copy); it zeroes every destination validity first, so only the valid rows get set
   * bits, and null lanes of the data buffer are undefined, matching the engine contract.
   */
  private def allocateVector(
      dataType: org.apache.spark.sql.types.DataType,
      ordinal: Int,
      len: Int,
      allocator: BufferAllocator): BaseFixedWidthVector = {
    val vector: ValueVector = dataType match {
      case DateType => new DateDayVector(s"varka$ordinal", allocator)
      case IntegerType => new IntVector(s"varka$ordinal", allocator)
    }
    val fixed = vector.asInstanceOf[BaseFixedWidthVector]
    try {
      fixed.allocateNew(len)
    } catch {
      case e: Throwable =>
        vector.close()
        throw e
    }
    fixed
  }
}

/**
 * An Arrow-backed column vector the Varka evaluator owns (task 21): `closeIfFreeable` is a
 * no-op, per Spark's two-tier close convention, because the vector's lifecycle belongs to the
 * evaluator's release paths - and a consumer that frees the batches it drains (the Arrow
 * cache writer calls `ColumnarBatch.closeIfFreeable()` per batch) must not close what it
 * does not own: it would free the buffers under the evaluator's own later release, the
 * double-close the ownership doc forbids. `WritableColumnVector` makes exactly this override
 * for the same reason; plain `ArrowColumnVector` does not, because a scan's vectors really
 * are freed that way.
 */
private[execution] class VarkaOwnedArrowColumnVector(vector: ValueVector)
    extends ArrowColumnVector(vector) {
  override def closeIfFreeable(): Unit = {}
}

/**
 * One batch's selection (task 21): the bitmap the filter kernel wrote - valid until the
 * evaluator's next [[VarkaFilterEvaluator.filterMask]] call, since the buffer is reused - and
 * the number of selected rows. Read through `VarkaSelectionBitmap`.
 */
private[sql] case class VarkaSelection(mask: MemorySegment, count: Int)

/**
 * The kernel half of the Varka filter, for one partition (task 21): it runs the mask kernel -
 * a fused loop whose single output root is the predicate's condition - over an Arrow-backed
 * batch and hands back the selection bitmap, leaving what to do with it (compact a fresh
 * batch, or skip rows at the row boundary) to the exec node. Shares every task-lifetime
 * mechanism with the projection evaluator through [[VarkaEvaluatorBase]].
 *
 * The condition must be fully fused: [[VarkaColumnarRule]] splits a mixed predicate and keeps
 * the residual conjuncts in a row `FilterExec` above, so a condition with residual conjuncts
 * reaching this evaluator would mean silently dropping them - the compile is therefore
 * accepted only when every conjunct fused, and anything else makes every batch take the
 * caller's row fallback.
 */
private[sql] class VarkaFilterEvaluator(
    condition: Expression,
    childOutput: Seq[Attribute],
    offHeapColumnVectorEnabled: Boolean,
    operatorName: String,
    classDumpDirectory: Option[String] = None,
    metrics: VarkaExecMetrics = VarkaExecMetrics())
    extends VarkaEvaluatorBase(childOutput, operatorName, classDumpDirectory, metrics) {

  private lazy val compiled =
    VarkaExpressionCompiler.compilePredicate(condition, childOutput)
      .filter(_.residualConjuncts.isEmpty)

  override protected def fusedPlan: Option[CompiledVarkaProjection] = compiled.map(_.fused)

  override protected def identityEntries: Iterator[String] = Iterator(condition.toString)

  // The selection buffer, reused across batches and grown on demand; released by the
  // task-completion listener before the allocator closes. The kernel zeroes the leading
  // (len + 7) / 8 bytes itself (its driver zeroes every dstValidity segment), so a stale
  // tail from a longer earlier batch is never read - the bitmap readers stop at `len` bits.
  private var maskBuf: ArrowBuf = null

  override protected def onTaskCleanup(): Unit = {
    if (maskBuf != null) {
      maskBuf.close()
      maskBuf = null
    }
  }

  private def maskBuffer(len: Int): ArrowBuf = {
    val needed = ((len + 63) / 64) * 8L
    if (maskBuf == null || maskBuf.capacity() < needed) {
      val alloc = taskAllocator()
      if (maskBuf != null) {
        maskBuf.close()
      }
      maskBuf = alloc.buffer(needed)
    }
    maskBuf
  }

  /**
   * Runs the mask kernel over the input batch and returns its selection. Callers must have
   * asked [[canRun]] first, must treat a throw as "this batch could not be served", and must
   * finish reading the bitmap before the next call - the buffer is task state, not batch
   * state, which is safe under the nodes' one-batch-at-a-time iteration and allocates nothing
   * per batch.
   */
  def filterMask(input: ColumnarBatch): VarkaSelection = {
    val len = input.numRows()
    val runner = fusedRunner.get
    fillSources(runner, input, len)
    val buf = maskBuffer(len)
    // The mask output's data slot is unused by contract (the emitted body never touches it);
    // its validity slot receives the selection bitmap.
    runner.dstData(0) = 0L
    runner.dstValidity(0) = buf.memoryAddress()
    invokeFused(runner, len)
    val mask = MemorySegment.ofAddress(buf.memoryAddress()).reinterpret((len + 7) / 8)
    VarkaSelection(mask, VarkaSelectionBitmap.countSet(mask, len))
  }

  // The generic-column compaction machinery, rebuilt only when the set of generic positions
  // changes (in practice once per partition: which columns take the fixed-width Arrow copy is
  // a property of the child's batch layout, not of the row values). Building an
  // UnsafeProjection per batch would put a Janino compile on the per-batch path.
  private var genericPositions: Seq[Int] = null
  private var genericSchema: StructType = null
  private var genericProjection: UnsafeProjection = null
  private var genericConverter: RowToColumnConverter = null

  private def genericMachinery(positions: Seq[Int]): Unit = {
    if (positions != genericPositions) {
      genericPositions = positions
      val attrs = positions.map(childOutput)
      genericSchema = DataTypeUtils.fromAttributes(attrs)
      genericProjection = UnsafeProjection.create(attrs, childOutput)
      genericConverter = new RowToColumnConverter(genericSchema)
    }
  }

  /**
   * Runs the mask kernel and compacts the selected rows into a fresh output batch - the
   * columnar filter's whole batch path, and the v1 selected-batch contract (milestone open
   * question 2): the batch that leaves a Varka filter is an ordinary dense batch, so every
   * consumer's invariants hold unchanged - `canRun`'s valueCount-equals-numRows check
   * included, which is what lets a Varka projection stack right on top.
   *
   * Columns whose input is an Arrow `DateDayVector` or `IntVector` compact by a typed scalar
   * copy into a fresh Arrow vector of the same type (keeping them kernel-servable upstream of
   * the next Varka node); every other column goes through one per-row pass with the standard
   * row-to-column converter, the same machinery as the projection residuals. Both loops are
   * the scalar compaction milestone 4 item 11 is expected to replace with `compress(mask)` -
   * measured first here. Every output column is owned: forwarding ends at a compacting
   * filter, because a forwarded vector cannot be shortened.
   */
  def filterCompact(input: ColumnarBatch): ColumnarBatch = {
    val selection = filterMask(input)
    val len = input.numRows()
    val count = selection.count
    val owned = mutable.ArrayBuffer.empty[ColumnVector]
    try {
      val columns = new Array[ColumnVector](childOutput.length)
      val generic = mutable.ArrayBuffer.empty[Int]
      var j = 0
      while (j < childOutput.length) {
        input.column(j) match {
          case acv: ArrowColumnVector =>
            acv.getValueVector() match {
              case src: DateDayVector if src.getValueCount() == len =>
                val dst = new DateDayVector(s"varka$j", taskAllocator())
                columns(j) = compactFixed(dst, selection, len, count, owned) { (pos, i) =>
                  if (src.isNull(i)) dst.setNull(pos) else dst.set(pos, src.get(i))
                }
              case src: IntVector if src.getValueCount() == len =>
                val dst = new IntVector(s"varka$j", taskAllocator())
                columns(j) = compactFixed(dst, selection, len, count, owned) { (pos, i) =>
                  if (src.isNull(i)) dst.setNull(pos) else dst.set(pos, src.get(i))
                }
              case _ => generic += j
            }
          case _ => generic += j
        }
        j += 1
      }
      if (generic.nonEmpty) {
        genericMachinery(generic.toSeq)
        val vectors: Array[WritableColumnVector] = if (offHeapColumnVectorEnabled) {
          OffHeapColumnVector.allocateColumns(math.max(count, 1), genericSchema)
            .toArray[WritableColumnVector]
        } else {
          OnHeapColumnVector.allocateColumns(math.max(count, 1), genericSchema)
            .toArray[WritableColumnVector]
        }
        owned ++= vectors
        val rows = input.rowIterator()
        var i = 0
        while (rows.hasNext) {
          val row = rows.next()
          if (VarkaSelectionBitmap.isSet(selection.mask, i)) {
            genericConverter.convert(genericProjection(row), vectors)
          }
          i += 1
        }
        generic.zipWithIndex.foreach { case (position, k) => columns(position) = vectors(k) }
      }
      val batch = new ColumnarBatch(columns)
      batch.setNumRows(count)
      trackOwned(batch, owned.toSeq)
      batch
    } catch {
      case e: Throwable =>
        owned.foreach(_.close())
        throw e
    }
  }

  /** Allocates `dst` for `count` rows, copies the selected rows via `copyRow(pos, i)`, and
   * wraps it; the vector joins `owned` as soon as it can leak. */
  private def compactFixed(dst: BaseFixedWidthVector, selection: VarkaSelection, len: Int,
      count: Int, owned: mutable.ArrayBuffer[ColumnVector])(
      copyRow: (Int, Int) => Unit): ColumnVector = {
    try {
      dst.allocateNew(math.max(count, 1))
    } catch {
      case e: Throwable =>
        dst.close()
        throw e
    }
    val wrapped = new VarkaOwnedArrowColumnVector(dst)
    owned += wrapped
    var i = 0
    var pos = 0
    while (i < len) {
      if (VarkaSelectionBitmap.isSet(selection.mask, i)) {
        copyRow(pos, i)
        pos += 1
      }
      i += 1
    }
    dst.setValueCount(count)
    wrapped
  }
}

/**
 * The Varka-specific SQL metrics one exec node threads to its factory and evaluator (task 22),
 * bundled so the parameter lists stop growing metric by metric (task 18 threaded two options;
 * this task would have made it five). Every field is optional: suites and diagnostics
 * construct evaluators with none.
 */
private[sql] case class VarkaExecMetrics(
    varkaBatches: Option[SQLMetric] = None,
    cacheHits: Option[SQLMetric] = None,
    cacheMisses: Option[SQLMetric] = None,
    fallbackBatchesNonArrow: Option[SQLMetric] = None,
    fallbackBatchesKernel: Option[SQLMetric] = None,
    emissionFailures: Option[SQLMetric] = None)

private[execution] object VarkaKernelEvaluator {

  /**
   * Emits the task-22 fallback JFR event; shared by the evaluator's emission-failure path and
   * the exec nodes' per-batch fallback branches. Populates only while a recording has the
   * event enabled; `exceptionClass` is empty for the non-Arrow cause, a data property rather
   * than an error.
   */
  private[execution] def emitFallbackEvent(
      cause: String,
      kernelIdentity: String,
      exceptionClass: String): Unit = {
    val event = new VarkaFallbackEvent
    if (event.isEnabled()) {
      event.cause = cause
      event.kernelIdentity = kernelIdentity
      event.exceptionClass = exceptionClass
      event.commit()
    }
  }
  // The (directory, SourceFile) pairs this JVM has dumped, so a shape's class file is
  // written once per process rather than once per task - and exactly once per process,
  // because a file left by an older emitter under the same shape name must be refreshed.
  private[execution] val dumpedClassFiles =
    java.util.concurrent.ConcurrentHashMap.newKeySet[String]()
}

private case class Morsel(data: MemorySegment, validity: MemorySegment, nullCount: Long) {
  def validityAddress: Long = if (validity == null) 0L else validity.address()
}
