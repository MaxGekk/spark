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

import scala.jdk.CollectionConverters._

import org.apache.spark.TaskContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, DateAdd, GreaterThan, IsNotNull, LessThan, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitterTestSupport
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Unit tests for the two Varka filter nodes (task 21): [[VarkaFilterExec]] - the mask kernel
 * plus compaction into a fresh dense batch - and [[VarkaFilterColumnarToRowExec]], which
 * consumes the selection bitmap at the row boundary with no compaction. Plus the
 * `VarkaColumnarRule` filter rewrites, the conjunct split included.
 *
 * The batch scaffolding - `BatchSpec`, `TestColumnarBatchPlan`, `buildBatch` - is shared with
 * [[VarkaColumnarToRowExecSuite]].
 */
class VarkaFilterExecSuite extends QueryTest with SharedSparkSession {

  private val attrD = AttributeReference("d", DateType)()
  private val intAttr = AttributeReference("i", IntegerType)()

  /** `d < DATE(epoch day 10)`: the workhorse predicate of these tests. */
  private def dLess10 = LessThan(attrD, Literal(10, DateType))

  private def columnarNode(condition: org.apache.spark.sql.catalyst.expressions.Expression,
      specs: Seq[BatchSpec], output: Seq[Attribute]): VarkaFilterExec = {
    VarkaFilterExec(condition, TestColumnarBatchPlan(specs, output))
  }

  /** Runs the columnar node and reads every output batch into per-row tuples of columns. */
  private def batchRows(node: VarkaFilterExec): Seq[Seq[Any]] = {
    node.executeColumnar().mapPartitions { batches =>
      batches.flatMap { batch =>
        (0 until batch.numRows()).map { r =>
          (0 until batch.numCols()).map { c =>
            if (batch.column(c).isNullAt(r)) null else Int.box(batch.column(c).getInt(r))
          }.toList
        }.toList.iterator
        // Materialised inside the partition: a batch belongs to its producer.
      }
    }.collect().toSeq
  }

  /** Runs the row node and reads its rows' first column. */
  private def rowValues(node: VarkaFilterColumnarToRowExec): Seq[Any] = {
    node.execute().map { row =>
      if (row.isNullAt(0)) null else Int.box(row.getInt(0))
    }.collect().toSeq
  }

  test("the columnar node compacts to exactly the selected rows, null-as-false") {
    val dates = Seq(Int.box(1), null, Int.box(9), Int.box(10), Int.box(42))
    val plan = columnarNode(dLess10, Seq(BatchSpec("arrow", Seq(dates))), Seq(attrD))
    // Row 1 is null: SQL's WHERE drops it, exactly as a false does.
    assert(batchRows(plan) === Seq(Seq(1), Seq(9)))
    assert(plan.metrics("numOutputRows").value === 2)
    assert(plan.metrics("numVarkaBatches").value === 1)
  }

  test("all-selected and none-selected batches compact to full and empty batches") {
    val dates = Seq(Int.box(1), Int.box(2), Int.box(3))
    val all = columnarNode(dLess10, Seq(BatchSpec("arrow", Seq(dates))), Seq(attrD))
    assert(batchRows(all) === Seq(Seq(1), Seq(2), Seq(3)))
    val none = columnarNode(
      GreaterThan(attrD, Literal(100, DateType)),
      Seq(BatchSpec("arrow", Seq(dates))), Seq(attrD))
    assert(batchRows(none) === Seq.empty)
    assert(none.metrics("numOutputRows").value === 0)
    assert(none.metrics("numVarkaBatches").value === 1)
  }

  test("an IS NOT NULL conjunction drops exactly the null rows") {
    // The view-caching shape that first exercised filters under a cache build: a two-column
    // validity conjunction, total (never unknown) - the mask must clear exactly the rows
    // where either side is null.
    val d = Seq(Int.box(1), null, Int.box(3), Int.box(4), null)
    val d2 = Seq(Int.box(10), Int.box(20), null, Int.box(40), Int.box(50))
    val attrD2 = AttributeReference("d2", DateType)()
    val plan = columnarNode(
      And(IsNotNull(attrD), IsNotNull(attrD2)),
      Seq(BatchSpec("arrow", Seq(d, d2))), Seq(attrD, attrD2))
    assert(batchRows(plan) === Seq(Seq(1, 10), Seq(4, 40)))
  }

  test("non-predicate columns ride the compaction: dates typed, ints typed") {
    // Both columns are Arrow fixed-width, so both take the typed compaction path; the int
    // column has a null on a surviving row, which must survive as a null.
    val dates = Seq(Int.box(1), Int.box(50), Int.box(3), Int.box(60))
    val ints = Seq(Int.box(7), Int.box(8), null, Int.box(10))
    val plan = columnarNode(
      dLess10, Seq(BatchSpec("arrow", Seq(dates, ints))), Seq(attrD, intAttr))
    assert(batchRows(plan) === Seq(Seq(1, 7), Seq(3, null)))
  }

  test("a column outside the typed compaction goes through the generic row pass") {
    // A batch whose predicate column is Arrow but whose second column is an on-heap vector:
    // canRun holds (only referenced columns must be Arrow) and the on-heap column compacts
    // through the generic converter pass. Built by hand - BatchSpec describes whole batches.
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("varka-test", 0, Long.MaxValue)
    val context = TaskContext.empty()
    TaskContext.setTaskContext(context)
    try {
      val arrowBatch = VarkaColumnarToRowExecSuite.buildBatch(
        BatchSpec("arrow", Seq(Seq(Int.box(1), Int.box(50), Int.box(3)))),
        Seq(attrD), allocator)
      val onheap = new OnHeapColumnVector(3, IntegerType)
      onheap.putInt(0, 7)
      onheap.putNull(1)
      onheap.putInt(2, 9)
      val batch = new ColumnarBatch(Array(arrowBatch.column(0), onheap))
      batch.setNumRows(3)
      val kernels = new VarkaFilterEvaluator(
        dLess10, Seq(attrD, intAttr), offHeapColumnVectorEnabled = false,
        operatorName = "Filter")
      assert(kernels.canRun(batch))
      val out = kernels.filterCompact(batch)
      assert(out.numRows() === 2)
      assert(Seq(out.column(0).getInt(0), out.column(0).getInt(1)) === Seq(1, 3))
      assert(out.column(1).getInt(0) === 7)
      assert(out.column(1).getInt(1) === 9)
      kernels.release(out)
      arrowBatch.close()
      onheap.close()
      context.markTaskCompleted(None)
    } finally {
      TaskContext.unset()
      allocator.close()
    }
  }

  test("a batch the kernel cannot serve falls back to the per-row filter, counted") {
    val plan = columnarNode(dLess10, Seq(
      BatchSpec("arrow", Seq(Seq(Int.box(1), Int.box(42)))),
      BatchSpec("onheap", Seq(Seq(Int.box(2), null, Int.box(77))))),
      Seq(attrD))
    assert(batchRows(plan) === Seq(Seq(1), Seq(2)))
    assert(plan.metrics("numInputBatches").value === 2)
    assert(plan.metrics("numVarkaBatches").value === 1)
    assert(plan.metrics("numFallbackBatchesNonArrow").value === 1)
    assert(plan.metrics("numFallbackBatchesKernel").value === 0)
    assert(plan.metrics("numOutputRows").value === 2)
  }

  test("the ghost fallback: a kernel failure filters the batch per row, counted") {
    VarkaColumnarToRowExec.setFailKernelForTesting(true)
    try {
      val plan = columnarNode(dLess10,
        Seq(BatchSpec("arrow", Seq(Seq(Int.box(1), null, Int.box(42))))), Seq(attrD))
      assert(batchRows(plan) === Seq(Seq(1)))
      assert(plan.metrics("numVarkaBatches").value === 0)
      assert(plan.metrics("numFallbackBatchesKernel").value === 1)
    } finally {
      VarkaColumnarToRowExec.setFailKernelForTesting(false)
    }
  }

  test("an emission failure counts once per task, evented, not mislabeled") {
    val recording = new jdk.jfr.Recording()
    recording.enable("org.apache.spark.sql.varka.Fallback")
    recording.start()
    VarkaEmitterTestSupport.setDisableCse(true)
    try {
      val plan = columnarNode(dLess10,
        Seq(BatchSpec("arrow", Seq(Seq(Int.box(1), null, Int.box(42))))), Seq(attrD))
      assert(batchRows(plan) === Seq(Seq(1)))
      assert(plan.metrics("numVarkaBatches").value === 0)
      assert(plan.metrics("numEmissionFailures").value === 1)
      assert(plan.metrics("numFallbackBatchesNonArrow").value === 0)
      assert(plan.metrics("numFallbackBatchesKernel").value === 0)
    } finally {
      VarkaEmitterTestSupport.setDisableCse(false)
      recording.stop()
    }
    withTempDir { dir =>
      val dump = new java.io.File(dir, "varka-filter-fallback.jfr").toPath
      recording.dump(dump)
      recording.close()
      val causes = jdk.jfr.consumer.RecordingFile.readAllEvents(dump).asScala
        .filter(_.getEventType.getName == "org.apache.spark.sql.varka.Fallback")
        .filter(_.getString("kernelIdentity").contains("Varka_Filter_"))
        .map(_.getString("cause"))
      assert(causes.contains("emission-failure"), causes.mkString("; "))
    }
  }

  test("each compacted batch is released when the next one is requested") {
    val numBatches = 8
    val rowsPerBatch = 512
    val specs = (0 until numBatches).map { b =>
      BatchSpec("arrow", Seq((0 until rowsPerBatch).map(i => Int.box(b * rowsPerBatch + i))))
    }
    val initial = ArrowUtils.rootAllocator.getAllocatedMemory
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("varka-test", 0, Long.MaxValue)
    val context = TaskContext.empty()
    TaskContext.setTaskContext(context)
    try {
      val inputs = specs.map(VarkaColumnarToRowExecSuite.buildBatch(_, Seq(attrD), allocator))
      val baseline = ArrowUtils.rootAllocator.getAllocatedMemory
      // Everything selects, so each compacted batch is as large as its input - the worst
      // case for a leak to show.
      val factory = new VarkaFilterEvaluatorFactory(
        GreaterThan(attrD, Literal(-1, DateType)),
        Seq(attrD),
        offHeapColumnVectorEnabled = false,
        classDumpDirectory = None,
        numOutputRows = org.apache.spark.sql.execution.metric.SQLMetrics
          .createMetric(sparkContext, "rows"),
        numInputBatches = org.apache.spark.sql.execution.metric.SQLMetrics
          .createMetric(sparkContext, "batches"),
        varkaMetrics = VarkaExecMetrics())
      val batches = factory.createEvaluator().eval(0, inputs.iterator)
      var seen = 0
      var peak = 0L
      batches.foreach { batch =>
        assert(batch.numRows() === rowsPerBatch)
        assert(batch.column(0).getInt(0) === seen * rowsPerBatch)
        seen += 1
        peak = math.max(peak, ArrowUtils.rootAllocator.getAllocatedMemory - baseline)
      }
      assert(seen === numBatches)
      // Unlike the projection's ledger, task state legitimately survives the drain here: the
      // reused selection buffer lives until task completion. Everything batch-shaped must be
      // gone, so the residue is bounded by that one small buffer.
      val residue = ArrowUtils.rootAllocator.getAllocatedMemory - baseline
      assert(residue >= 0 && residue <= 4096,
        s"the compacted batches were not released as the iterator advanced ($residue bytes)")
      val oneBatch = 4L * rowsPerBatch
      assert(peak < 4 * oneBatch,
        s"peak Varka off-heap use was $peak bytes for a $oneBatch-byte batch")
      inputs.foreach(_.close())
      context.markTaskCompleted(None)
      assert(ArrowUtils.rootAllocator.getAllocatedMemory === initial,
        "the task-completion listener did not release the Varka state")
    } finally {
      TaskContext.unset()
      allocator.close()
    }
  }

  test("the row node emits exactly the selected rows, null-as-false, no compaction") {
    val dates = Seq(Int.box(1), null, Int.box(9), Int.box(10), Int.box(42))
    val plan = VarkaFilterColumnarToRowExec(
      dLess10, TestColumnarBatchPlan(Seq(BatchSpec("arrow", Seq(dates))), Seq(attrD)))
    assert(rowValues(plan) === Seq(1, 9))
    assert(plan.metrics("numOutputRows").value === 2)
    assert(plan.metrics("numVarkaBatches").value === 1)
  }

  test("the row node falls back per row on a batch the kernel cannot serve") {
    val plan = VarkaFilterColumnarToRowExec(
      dLess10,
      TestColumnarBatchPlan(Seq(
        BatchSpec("arrow", Seq(Seq(Int.box(1), Int.box(42)))),
        BatchSpec("onheap", Seq(Seq(Int.box(2), null, Int.box(77))))), Seq(attrD)))
    assert(rowValues(plan) === Seq(1, 2))
    assert(plan.metrics("numFallbackBatchesNonArrow").value === 1)
    assert(plan.metrics("numOutputRows").value === 2)
  }

  test("both filter nodes tighten output nullability like FilterExec") {
    val child = TestColumnarBatchPlan(Nil, Seq(attrD, intAttr))
    val condition = And(IsNotNull(attrD), dLess10)
    val columnar = VarkaFilterExec(condition, child)
    val row = VarkaFilterColumnarToRowExec(condition, child)
    val filter = FilterExec(condition, child)
    assert(columnar.output.map(_.nullable) === filter.output.map(_.nullable))
    assert(row.output.map(_.nullable) === filter.output.map(_.nullable))
    assert(!columnar.output.head.nullable, "the IsNotNull-guarded column must read non-null")
    assert(columnar.output(1).nullable)
  }

  test("EXPLAIN reports the predicate's conjuncts through the fusion report") {
    val fused = VarkaFusionReport.predicateLines(And(dLess10, IsNotNull(attrD)),
      Seq(attrD, intAttr))
    assert(fused === Seq("(d < DATE '1970-01-11'): fused", "(d IS NOT NULL): fused"))
    val mixed = VarkaFusionReport.predicateLines(
      And(dLess10, GreaterThan(intAttr, Literal(5))), Seq(attrD, intAttr))
    assert(mixed.head === "(d < DATE '1970-01-11'): fused")
    assert(mixed(1).startsWith("(i > 5): residual (non-date column of type int"))
    assert(VarkaFusionReport.predicateLines(GreaterThan(intAttr, Literal(5)),
      Seq(attrD, intAttr)) === Seq("no conjunct is Varka-eligible"))
  }

  test("VarkaColumnarRule: the pre stage rewrites an eligible filter, splitting conjuncts") {
    val child = TestColumnarBatchPlan(Nil, Seq(attrD, intAttr))
    val eligible = FilterExec(dLess10, child)
    val mixed = FilterExec(And(dLess10, GreaterThan(intAttr, Literal(5))), child)
    val ineligible = FilterExec(GreaterThan(intAttr, Literal(5)), child)
    val rowChild = FilterExec(dLess10, ColumnarToRowExec(child))

    withSQLConf(SQLConf.VARKA_ENABLED.key -> "true") {
      assert(VarkaColumnarRule.preColumnarTransitions(eligible) ===
        VarkaFilterExec(dLess10, child))
      // The mixed predicate splits: the fused conjunct in the Varka node, the int conjunct
      // in a row FilterExec above it - which then sees only the surviving rows.
      assert(VarkaColumnarRule.preColumnarTransitions(mixed) ===
        FilterExec(GreaterThan(intAttr, Literal(5)), VarkaFilterExec(dLess10, child)))
      assert(VarkaColumnarRule.preColumnarTransitions(ineligible) === ineligible)
      // A row child is left for the post stage, which absorbs the transition.
      assert(VarkaColumnarRule.preColumnarTransitions(rowChild) === rowChild)
    }
    withSQLConf(SQLConf.VARKA_ENABLED.key -> "false") {
      assert(VarkaColumnarRule.preColumnarTransitions(eligible) === eligible)
    }
  }

  test("VarkaColumnarRule: the post stage fuses the transition, or absorbs one") {
    val child = TestColumnarBatchPlan(Nil, Seq(attrD, intAttr))
    withSQLConf(SQLConf.VARKA_ENABLED.key -> "true") {
      // The transition the pre-stage node received is fused into the row-out filter.
      assert(VarkaColumnarRule.postColumnarTransitions(
        ColumnarToRowExec(VarkaFilterExec(dLess10, child))) ===
        VarkaFilterColumnarToRowExec(dLess10, child))
      // A filter the pre stage did not see absorbs its to-row transition.
      assert(VarkaColumnarRule.postColumnarTransitions(
        FilterExec(dLess10, ColumnarToRowExec(child))) ===
        VarkaFilterColumnarToRowExec(dLess10, child))
      // The split applies there too.
      assert(VarkaColumnarRule.postColumnarTransitions(
        FilterExec(And(dLess10, GreaterThan(intAttr, Literal(5))),
          ColumnarToRowExec(child))) ===
        FilterExec(GreaterThan(intAttr, Literal(5)),
          VarkaFilterColumnarToRowExec(dLess10, child)))
      // The residual filter the pre stage left above a (now fused) Varka filter is not
      // touched again: its child is a row node.
      val residualOverFused = FilterExec(GreaterThan(intAttr, Literal(5)),
        VarkaFilterColumnarToRowExec(dLess10, child))
      assert(VarkaColumnarRule.postColumnarTransitions(residualOverFused)
        === residualOverFused)
    }
    withSQLConf(SQLConf.VARKA_ENABLED.key -> "false") {
      val transition = ColumnarToRowExec(VarkaFilterExec(dLess10, child))
      assert(VarkaColumnarRule.postColumnarTransitions(transition) === transition)
    }
  }

  test("VarkaColumnarRule: a Varka projection stacks on a Varka filter in one pre pass") {
    val child = TestColumnarBatchPlan(Nil, Seq(attrD, intAttr))
    val plan = ProjectExec(
      Seq(Alias(DateAdd(attrD, Literal(3)), "add")()),
      FilterExec(dLess10, child))
    withSQLConf(SQLConf.VARKA_ENABLED.key -> "true") {
      val rewritten = VarkaColumnarRule.preColumnarTransitions(plan)
      assert(rewritten.isInstanceOf[VarkaProjectExec],
        s"expected a stacked Varka projection, got:\n$rewritten")
      assert(rewritten.children.head.isInstanceOf[VarkaFilterExec])
    }
    // With a residual conjunct the stack breaks by design: the row FilterExec sits between.
    val mixedPlan = ProjectExec(
      Seq(Alias(DateAdd(attrD, Literal(3)), "add")()),
      FilterExec(And(dLess10, GreaterThan(intAttr, Literal(5))), child))
    withSQLConf(SQLConf.VARKA_ENABLED.key -> "true") {
      val rewritten = VarkaColumnarRule.preColumnarTransitions(mixedPlan)
      assert(rewritten.isInstanceOf[ProjectExec])
      assert(rewritten.children.head.isInstanceOf[FilterExec])
    }
  }

  test("stacked filter and projection execute end to end on the kernels") {
    val dates = Seq(Int.box(1), null, Int.box(9), Int.box(42))
    val child = TestColumnarBatchPlan(Seq(BatchSpec("arrow", Seq(dates))), Seq(attrD))
    val stacked = VarkaProjectExec(
      Seq(Alias(DateAdd(attrD, Literal(100)), "add")()),
      VarkaFilterExec(dLess10, child))
    val rows = stacked.executeColumnar().mapPartitions { batches =>
      batches.flatMap { batch =>
        (0 until batch.numRows()).map { r =>
          if (batch.column(0).isNullAt(r)) null else Int.box(batch.column(0).getInt(r))
        }.toList.iterator
      }
    }.collect().toSeq
    assert(rows === Seq(101, 109))
    // The compacted batch keeps the Arrow invariant, so the projection above ran on the
    // kernels rather than falling back.
    assert(stacked.metrics("numVarkaBatches").value === 1)
    assert(stacked.metrics("numFallbackBatchesNonArrow").value === 0)
  }
}
