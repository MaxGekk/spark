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
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.{CompiledVarkaProjection, ForwardedOutput, FusedOutput, PartialVarkaProjection, ResidualOutput, VarkaExpressionCompiler}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.{VarkaFusedKernel, VarkaShapeCache, VarkaShapeKey}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{DateType, IntegerType, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

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
 * @param cacheHitMetric incremented once per task whose kernel class came from the cache,
 *                       when the exec node passes its metric down; None outside an exec node.
 * @param cacheMissMetric the same for a task that had to emit and define the class.
 */
private[sql] class VarkaKernelEvaluator(
    projectList: Seq[NamedExpression],
    childOutput: Seq[Attribute],
    offHeapColumnVectorEnabled: Boolean,
    operatorName: String,
    classDumpDirectory: Option[String] = None,
    cacheHitMetric: Option[SQLMetric] = None,
    cacheMissMetric: Option[SQLMetric] = None)
    extends Logging {

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
  private lazy val fusedRunner: Option[FusedRunner] = {
    compiled.flatMap { partial =>
      try {
        Some(new FusedRunner(partial.fused))
      } catch {
        case e if isCatchable(e) =>
          logWarning(s"Failed to emit the Varka fused kernel $kernelIdentity; falling back " +
            "to the per-row projection.", e)
          None
      }
    }
  }

  /** The classified projection, for the row node's merge-at-row read-back (see 2.3). */
  private[execution] def partialPlan: Option[PartialVarkaProjection] = compiled

  /**
   * This execution's identity - the operator and this task's stage - which since task 18 goes
   * to [[VarkaShapeCache]]'s side table rather than into the shared class bytes. Outside a
   * task (diagnostics, tests) the stage reads as -1 rather than throwing.
   */
  private def executionName: String = {
    val stage = Option(TaskContext.get()).map(_.stageId()).getOrElse(-1)
    s"Varka_${operatorName}_Stage$stage"
  }

  /** The cache key of the fused sub-projection: exactly the emitter inputs the bytes follow. */
  private def shapeKey(plan: CompiledVarkaProjection): VarkaShapeKey =
    VarkaShapeKey(plan.outputs, plan.inputOrdinals.size, plan.literals.size)

  /**
   * The kernel named the way its telemetry names it (task 16, shape-based since task 18): the
   * `SourceFile` of the shared class, the IR it computes, and this execution's operator and
   * stage. Every fallback warning - here and in both exec nodes - says which kernel it gave up
   * on, so a log line identifies both the class and the plan node without correlation.
   * Reading it forces no emission: the shape hash is computed from the IR, not the bytes.
   */
  private[execution] def kernelIdentity: String = {
    compiled match {
      case Some(partial) =>
        val ir = partial.fused.outputs.mkString(", ")
        val hash = VarkaShapeCache.shapeHash(shapeKey(partial.fused))
        s"VarkaFusedProjection_$hash.java [$ir] ($executionName)"
      case None => s"[no compiled projection] ($executionName)"
    }
  }

  /**
   * The emitted fused-kernel class's bytes, exactly as defined - the diagnostics hook behind
   * the telemetry note in the class doc: `VarkaDebugInfo.read` and `ClassFile.parse` recover
   * the IR, the plan fragment and the `SourceFile` name from them. Forces emission if no batch
   * has done so yet; None when the projection is ineligible or emission failed.
   */
  private[execution] def emittedClassBytes: Option[Array[Byte]] = fusedRunner.map(_.classBytes)

  /**
   * Whether [[project]] (or [[projectFused]]) can serve this batch, or the caller has to fall
   * back. The Arrow check
   * covers only the columns the fused entries reference: forwarded and residual entries put no
   * constraint on the input format beyond what `rowIterator` needs.
   */
  def canRun(input: ColumnarBatch): Boolean = {
    (compiled, fusedRunner) match {
      case (Some(partial), Some(_)) => input.numRows() > 0 && isArrowBacked(partial.fused, input)
      case _ => false
    }
  }

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
    var i = 0
    plan.inputOrdinals.foreach { ordinal =>
      val acv = input.column(ordinal).asInstanceOf[ArrowColumnVector]
      val morsel = extractMorsel(acv.getValueVector().asInstanceOf[DateDayVector], len)
      runner.srcData(i) = morsel.data.address()
      runner.srcValidity(i) = morsel.validityAddress
      runner.srcNullCount(i) = morsel.nullCount.toInt
      i += 1
    }
    val fixed = new Array[BaseFixedWidthVector](plan.outputs.size)
    val fusedColumns = new Array[ColumnVector](plan.outputs.size)
    var o = 0
    plan.outputTypes.foreach { dataType =>
      val vector = allocateVector(dataType, o, len, alloc)
      fixed(o) = vector
      fusedColumns(o) = new ArrowColumnVector(vector)
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
   * Takes ownership of a batch the caller built itself - a fallback batch, every column the
   * caller's own - so that the same task-completion listener closes it if the task stops before
   * the caller releases it.
   */
  def track(batch: ColumnarBatch): ColumnarBatch = {
    trackOwned(batch, (0 until batch.numCols()).map(batch.column))
    batch
  }

  private def trackOwned(batch: ColumnarBatch, owned: Seq[ColumnVector]): Unit = {
    ensureCleanup()
    openBatches(batch) = owned
  }

  /**
   * Releases a batch obtained from [[project]] or handed to [[track]]: closes exactly the
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
   * Whether the kernels can run over this batch: every referenced column must be an Arrow
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

  /**
   * Registers the single task-completion listener that closes any batch still open and then the
   * allocator. Both this and [[taskAllocator]] are called from the task thread only.
   */
  private def ensureCleanup(): Unit = {
    if (!cleanupRegistered) {
      cleanupRegistered = true
      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        openBatches.foreach { case (_, owned) => owned.foreach(_.close()) }
        openBatches.clear()
        if (kernelAllocator != null) {
          kernelAllocator.close()
          kernelAllocator = null
        }
      }
    }
  }

  /** Returns the task's Arrow child allocator, creating it on first use. */
  private def taskAllocator(): BufferAllocator = {
    ensureCleanup()
    if (kernelAllocator == null) {
      kernelAllocator =
        ArrowUtils.rootAllocator.newChildAllocator("varka-kernels", 0, Long.MaxValue)
    }
    kernelAllocator
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

  /**
   * Writes the emitted class to the configured dump directory under its `SourceFile` name
   * (task 16), so `javap -c -p` reaches a generated loop with no debugger. Diagnostics only:
   * every failure is logged and swallowed, because a query must not fail over a debug write.
   * Every task of a shape holds identical bytes (task 18), so they overwrite one file.
   */
  private def dumpClass(sourceFile: String, bytes: Array[Byte]): Unit = {
    classDumpDirectory.foreach { directory =>
      try {
        val target = new File(directory, sourceFile.stripSuffix(".java") + ".class")
        Files.createDirectories(target.toPath.getParent)
        Files.write(target.toPath, bytes)
        logInfo(s"Wrote the Varka kernel class to ${target.getAbsolutePath}")
      } catch {
        case NonFatal(e) =>
          logWarning(s"Could not dump the Varka kernel class to $directory; " +
            "execution is unaffected.", e)
      }
    }
  }

  private def invokeFused(runner: FusedRunner, len: Int): Unit = {
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
   * task's own. With the cache disabled (`maxEntries` = 0) the entry is unshared and the
   * pre-task-18 lifecycle applies: this runner's task releases the loader on completion.
   */
  private class FusedRunner(plan: CompiledVarkaProjection) {
    // The lookup records this execution (operator, stage, the whole projection - forwarded
    // and residual entries included) in the cache's side table, so the shape-named class
    // joins back to the plan nodes that ran it.
    private val lookup =
      VarkaShapeCache.getOrEmit(shapeKey(plan), s"$executionName: ${projectList.mkString(", ")}")
    private val entry = lookup.entry
    (if (lookup.hit) cacheHitMetric else cacheMissMetric).foreach(_ += 1)

    val sourceFile: String = entry.sourceFile

    val classBytes: Array[Byte] = {
      // Every task of a shape dumps identical bytes, so they overwrite one file - and a hit
      // in a session that configured the dump directory after the miss still gets its file.
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

    if (!entry.shared) {
      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        entry.loader.release()
      }
    }
  }
}

private case class Morsel(data: MemorySegment, validity: MemorySegment, nullCount: Long) {
  def validityAddress: Long = if (validity == null) 0L else validity.address()
}
