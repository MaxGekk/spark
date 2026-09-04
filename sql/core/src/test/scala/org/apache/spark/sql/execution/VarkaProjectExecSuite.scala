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

import org.apache.spark.TaskContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, Attribute, AttributeReference, DateAdd, DateDiff, DateSub, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.{VarkaAllocationSampler,
  VarkaFallbackEvent, VarkaJfrTestSupport, VarkaKernelAllocationEvent}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Unit tests for [[VarkaProjectExec]] (the columnar-out half of the Varka projection): the SIMD
 * kernels over Arrow `DateDayVector` batches, the materialising fallback that a columnar-out node
 * needs where [[VarkaColumnarToRowExec]] can project rows one by one, and the batch ownership the
 * node owes its consumer.
 *
 * The batch scaffolding - `BatchSpec`, `TestColumnarBatchPlan`, `buildBatch` - is shared with
 * [[VarkaColumnarToRowExecSuite]].
 */
class VarkaProjectExecSuite extends QueryTest with SharedSparkSession {

  private val attrD = AttributeReference("d", DateType)()
  private val attrD2 = AttributeReference("d2", DateType)()
  private val intAttr = AttributeReference("i", IntegerType)()

  private def project(exprs: NamedExpression*): Seq[NamedExpression] = exprs

  /** Runs the node and reads every output batch into plain values, one column. */
  private def values(node: VarkaProjectExec): Seq[Any] = {
    node.executeColumnar().mapPartitions { batches =>
      batches.flatMap { batch =>
        val column = batch.column(0)
        (0 until batch.numRows()).map { i =>
          if (column.isNullAt(i)) null else Int.box(column.getInt(i))
        }.toList.iterator
      }
      // The rows are materialised into a List above: a batch belongs to the node that produced
      // it, so nothing may read it after the next batch is asked for.
    }.collect().toSeq
  }

  private def node(projectList: Seq[NamedExpression], specs: Seq[BatchSpec],
      output: Seq[Attribute]): VarkaProjectExec = {
    VarkaProjectExec(projectList, TestColumnarBatchPlan(specs, output))
  }

  test("the node is columnar and never asked for rows") {
    val plan = node(
      project(Alias(DateAdd(attrD, Literal(3)), "add")()),
      Seq(BatchSpec("arrow", Seq(Seq(Int.box(1))))),
      Seq(attrD))
    assert(plan.supportsColumnar)
    assert(!plan.supportsRowBased)
    intercept[Exception](plan.execute())
  }

  test("date_add, date_sub and date_diff over Arrow batches") {
    val days = Seq(0, 1, 100).map(Int.box)
    assert(values(node(
      project(Alias(DateAdd(attrD, Literal(3)), "add")()),
      Seq(BatchSpec("arrow", Seq(days))), Seq(attrD))) === Seq(3, 4, 103))
    assert(values(node(
      project(Alias(DateSub(attrD, Literal(2)), "sub")()),
      Seq(BatchSpec("arrow", Seq(days))), Seq(attrD))) === Seq(-2, -1, 98))
    assert(values(node(
      project(Alias(DateDiff(attrD2, attrD), "diff")()),
      Seq(BatchSpec("arrow", Seq(days, Seq(10, 10, 10).map(Int.box)))),
      Seq(attrD2, attrD))) === Seq(-10, -9, 90))
  }

  test("null patterns: mixed, all-null and null-free columns") {
    val mixed = Seq(Int.box(1), null, Int.box(3))
    assert(values(node(
      project(Alias(DateAdd(attrD, Literal(1)), "add")()),
      Seq(BatchSpec("arrow", Seq(mixed))), Seq(attrD))) === Seq(2, null, 4))

    val allNull = Seq(null, null, null)
    assert(values(node(
      project(Alias(DateAdd(attrD, Literal(1)), "add")()),
      Seq(BatchSpec("arrow", Seq(allNull))), Seq(attrD))) === Seq(null, null, null))
  }

  test("a mixed projection produces fused, forwarded and residual columns in one batch") {
    val dates = Seq(Int.box(0), null, Int.box(20000))
    val ints = Seq(Int.box(7), Int.box(8), null)
    val plan = node(
      project(
        Alias(DateAdd(attrD, Literal(3)), "a")(),
        intAttr,
        Alias(Add(intAttr, Literal(1)), "inc")()),
      Seq(BatchSpec("arrow", Seq(dates, ints))),
      Seq(attrD, intAttr))
    val rows = plan.executeColumnar().mapPartitions { batches =>
      batches.flatMap { batch =>
        (0 until batch.numRows()).map { r =>
          (0 until batch.numCols()).map { c =>
            if (batch.column(c).isNullAt(r)) null else Int.box(batch.column(c).getInt(r))
          }.toList
        }.toList.iterator
      }
    }.collect().toSeq
    assert(rows === Seq(List(3, 7, 8), List(null, 8, 9), List(20003, null, null)))
    assert(plan.metrics("numVarkaBatches").value === 1)
    // Task 22: the residual entry (`inc`) is counted once, driver-side - a static plan
    // property, not multiplied by task count.
    assert(plan.metrics("numResidualEntries").value === 1)
  }

  test("a non-Arrow batch is materialised by the fallback, not dropped") {
    val plan = node(
      project(Alias(DateAdd(attrD, Literal(3)), "add")()),
      Seq(BatchSpec("onheap", Seq(Seq(Int.box(1), null, Int.box(5))))),
      Seq(attrD))
    assert(values(plan) === Seq(4, null, 8))
  }

  test("an empty batch produces an empty batch") {
    val plan = node(
      project(Alias(DateAdd(attrD, Literal(3)), "add")()),
      Seq(BatchSpec("arrow", Seq(Seq.empty[java.lang.Integer]))),
      Seq(attrD))
    assert(values(plan) === Seq.empty)
  }

  test("an ineligible projection still produces the right batches") {
    // `i + 1` is not a kernel op, so every batch goes through the fallback - the node is only
    // ever planned for eligible projections, but it must not produce wrong data if it is not.
    val plan = node(
      project(Alias(Add(intAttr, Literal(1)), "add")()),
      Seq(BatchSpec("onheap", Seq(Seq(Int.box(100), Int.box(101))))),
      Seq(intAttr))
    assert(values(plan) === Seq(101, 102))
    assert(plan.metrics("numVarkaBatches").value === 0)
  }

  test("an injected kernel failure falls back per batch without crashing") {
    VarkaColumnarToRowExec.setFailKernelForTesting(true)
    try {
      val plan = node(
        project(Alias(DateAdd(attrD, Literal(3)), "add")()),
        Seq(BatchSpec("arrow", Seq(Seq(Int.box(1), null, Int.box(5))))),
        Seq(attrD))
      assert(values(plan) === Seq(4, null, 8))
      // Task 22: the ghost fallback is counted under its own cause.
      assert(plan.metrics("numFallbackBatchesKernel").value === 1)
      assert(plan.metrics("numFallbackBatchesNonArrow").value === 0)
    } finally {
      VarkaColumnarToRowExec.setFailKernelForTesting(false)
    }
  }

  test("task 16: the fallback warning names the kernel it gave up on") {
    // Before task 16 this line carried only the exception, so a log could not say which plan
    // node or projection had fallen back.
    VarkaColumnarToRowExec.setFailKernelForTesting(true)
    try {
      val appender = new LogAppender("varka fallback")
      withLogAppender(appender) {
        val plan = node(
          project(Alias(DateAdd(attrD, Literal(3)), "add")()),
          Seq(BatchSpec("arrow", Seq(Seq(Int.box(1), null, Int.box(5))))),
          Seq(attrD))
        assert(values(plan) === Seq(4, null, 8))
      }
      val warning = appender.loggingEvents
        .map(_.getMessage.getFormattedMessage)
        .find(_.contains("failed on this batch"))
        .getOrElse(fail("no fallback warning was logged"))
      // The kernel's own telemetry name, plus the IR it computes.
      assert(warning.contains("Varka_Project_Stage"), warning)
      assert(warning.contains("(addDays "), warning)
    } finally {
      VarkaColumnarToRowExec.setFailKernelForTesting(false)
    }
  }

  test("task 16: verbose EXPLAIN accounts for every entry, with the residual entry's reason") {
    val plan = node(
      project(
        Alias(DateAdd(attrD, Literal(3)), "a")(),
        intAttr,
        Alias(Add(intAttr, Literal(1)), "inc")()),
      Seq(BatchSpec("arrow", Seq(Seq(Int.box(0)), Seq(Int.box(7))))),
      Seq(attrD, intAttr))
    val explained = plan.verboseStringWithOperatorId()
    assert(explained.contains("Varka"), explained)
    assert(explained.contains("a: fused"), explained)
    assert(explained.contains("i: forwarded from i"), explained)
    assert(explained.contains("inc: residual (unsupported expression:"), explained)
  }

  test("the fallback projection is compiled lazily, only when a batch falls back") {
    // Same construction as the VarkaColumnarToRowExecSuite counterpart: under CODEGEN_ONLY,
    // [[ExplodingCodegenExpression]] makes building the fallback projection throw, so the
    // evaluator constructor succeeding proves the compile is deferred (task 15), and the
    // failure surfacing on an ineligible batch proves it is deferred exactly to the fallback.
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY") {
      val factory = new VarkaProjectEvaluatorFactory(
        project(Alias(ExplodingCodegenExpression(), "boom")()), Seq(intAttr),
        offHeapColumnVectorEnabled = false,
        classDumpDirectory = None,
        SQLMetrics.createMetric(sparkContext, "rows"),
        SQLMetrics.createMetric(sparkContext, "batches"),
        VarkaExecMetrics())
      // Before task 15 this constructor compiled the fallback eagerly and threw.
      val evaluator = factory.createEvaluator()
      val column = new OnHeapColumnVector(1, IntegerType)
      column.putInt(0, 7)
      val batch = new ColumnarBatch(Array(column), 1)
      val e = intercept[Throwable] {
        evaluator.eval(0, Iterator(batch)).next()
      }
      val chain = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(10).toSeq
      assert(chain.exists(_.getMessage.contains("exploding-codegen")),
        s"expected the codegen failure to surface on the fallback path, got: $e")
    }
  }

  test("each output batch is released when the next one is requested") {
    val numBatches = 16
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
      // Everything allocated from here on is the kernel path's own output.
      val baseline = ArrowUtils.rootAllocator.getAllocatedMemory
      val batches = evaluate(inputs.iterator)

      var seen = 0
      var peak = 0L
      batches.foreach { batch =>
        assert(batch.numRows() === rowsPerBatch)
        assert(batch.column(0).getInt(0) === seen * rowsPerBatch + 3)
        seen += 1
        peak = math.max(peak, ArrowUtils.rootAllocator.getAllocatedMemory - baseline)
      }
      assert(seen === numBatches)
      // The iterator is drained, so the last batch was released too - without waiting for the
      // task to complete.
      assert(ArrowUtils.rootAllocator.getAllocatedMemory === baseline,
        "the output batches were not released as the iterator advanced")
      // Only one output batch is live at a time; allow slack for Arrow's power-of-two buffer
      // rounding, but stay far below the numBatches-times figure a leak would reach.
      val oneBatch = 4L * rowsPerBatch
      assert(peak < 4 * oneBatch,
        s"peak Varka off-heap use was $peak bytes for a ${oneBatch}-byte batch")

      inputs.foreach(_.close())
      context.markTaskCompleted(None)
      assert(ArrowUtils.rootAllocator.getAllocatedMemory === initial,
        "the task-completion listener did not release the Varka child allocator")
    } finally {
      TaskContext.unset()
      allocator.close()
    }
  }

  test("a consumer that stops early leaves nothing open after the task completes") {
    val specs = Seq(
      BatchSpec("arrow", Seq((0 until 128).map(Int.box))),
      BatchSpec("arrow", Seq((128 until 256).map(Int.box))))
    val initial = ArrowUtils.rootAllocator.getAllocatedMemory
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("varka-test", 0, Long.MaxValue)
    val context = TaskContext.empty()
    TaskContext.setTaskContext(context)
    try {
      val inputs = specs.map(VarkaColumnarToRowExecSuite.buildBatch(_, Seq(attrD), allocator))
      val baseline = ArrowUtils.rootAllocator.getAllocatedMemory
      // Take one batch and walk away, like a LIMIT would: it stays open until the task ends.
      val batches = evaluate(inputs.iterator)
      assert(batches.next().numRows() === 128)
      assert(ArrowUtils.rootAllocator.getAllocatedMemory > baseline)

      inputs.foreach(_.close())
      context.markTaskCompleted(None)
      assert(ArrowUtils.rootAllocator.getAllocatedMemory === initial,
        "the task-completion listener did not release the open Varka batch")
    } finally {
      TaskContext.unset()
      allocator.close()
    }
  }

  test("metrics count rows and the batches the kernels served") {
    val plan = node(
      project(Alias(DateAdd(attrD, Literal(3)), "add")()),
      Seq(
        BatchSpec("arrow", Seq(Seq(Int.box(1), Int.box(2)))),
        BatchSpec("onheap", Seq(Seq(Int.box(3)))),
        BatchSpec("arrow", Seq(Seq.empty))),
      Seq(attrD))
    plan.executeColumnar().foreach(_ => ())
    assert(plan.metrics("numOutputRows").value === 3)
    assert(plan.metrics("numInputBatches").value === 3)
    // Only the non-empty Arrow batch reaches the kernels; the on-heap one takes the fallback.
    assert(plan.metrics("numVarkaBatches").value === 1)
    // Task 18: each spec is its own partition, so three tasks looked the shape up - canRun
    // forces the runner on the fallback tasks too, which before the cache emitted a class it
    // never ran and now costs a hit. Hit or miss per task depends on what ran in this JVM.
    assert(plan.metrics("numVarkaCacheHits").value +
      plan.metrics("numVarkaCacheMisses").value === 3)
    // Task 22: the on-heap fallback batch is counted under its cause; nothing else fired -
    // in particular the EMPTY Arrow batch, which canRun also refuses, is served trivially
    // and must not read as "input not Arrow-backed" (the task-21 review's cause fix).
    assert(plan.metrics("numFallbackBatchesNonArrow").value === 1)
    assert(plan.metrics("numFallbackBatchesKernel").value === 0)
    assert(plan.metrics("numEmissionFailures").value === 0)
    assert(plan.metrics("numResidualEntries").value === 0)
  }


  test("task 21 review: a residual-machinery failure is counted under its own cause") {
    // Under CODEGEN_ONLY the residual projection's Janino compile throws inside the kernel
    // try; the VarkaKernelFailure marker keeps it out of the kernel-failure metric and it
    // lands under row-path-failure - once - before the fallback re-throws the same failure.
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY") {
      val rowPath = SQLMetrics.createMetric(sparkContext, "rowPath")
      val kernelFailures = SQLMetrics.createMetric(sparkContext, "kernel")
      val factory = new VarkaProjectEvaluatorFactory(
        project(
          Alias(DateAdd(attrD, Literal(3)), "a")(),
          Alias(ExplodingCodegenExpression(), "boom")()),
        Seq(attrD),
        offHeapColumnVectorEnabled = false,
        classDumpDirectory = None,
        SQLMetrics.createMetric(sparkContext, "rows"),
        SQLMetrics.createMetric(sparkContext, "batches"),
        VarkaExecMetrics(
          fallbackBatchesKernel = Some(kernelFailures),
          fallbackBatchesRowPath = Some(rowPath)))
      val allocator = ArrowUtils.rootAllocator.newChildAllocator("varka-test", 0, Long.MaxValue)
      val context = TaskContext.empty()
      TaskContext.setTaskContext(context)
      try {
        val batch = VarkaColumnarToRowExecSuite.buildBatch(
          BatchSpec("arrow", Seq(Seq(Int.box(1)))), Seq(attrD), allocator)
        try {
          intercept[Throwable] {
            factory.createEvaluator().eval(0, Iterator(batch)).next()
          }
          assert(rowPath.value === 1)
          assert(kernelFailures.value === 0)
        } finally {
          batch.close()
        }
      } finally {
        context.markTaskCompleted(None)
        TaskContext.unset()
        allocator.close()
      }
    }
  }

  test("task 22: an emission failure counts once per task, evented, not mislabeled") {
    // The injected emission failure makes the class lookup throw, so the runner cannot be
    // built: the evaluator counts one emission failure and emits the JFR fallback event, and
    // the per-batch fallbacks are NOT counted as non-Arrow (the carve-out under test).
    val (_, recorded) = VarkaJfrTestSupport.withJfrRecording(classOf[VarkaFallbackEvent]) {
      VarkaColumnarToRowExec.setFailEmissionForTesting(true)
      try {
        val plan = node(
          project(Alias(DateAdd(attrD, Literal(3)), "add")()),
          Seq(BatchSpec("arrow", Seq(Seq(Int.box(1), null, Int.box(5))))),
          Seq(attrD))
        assert(values(plan) === Seq(4, null, 8))
        assert(plan.metrics("numVarkaBatches").value === 0)
        assert(plan.metrics("numEmissionFailures").value === 1)
        assert(plan.metrics("numFallbackBatchesNonArrow").value === 0)
        assert(plan.metrics("numFallbackBatchesKernel").value === 0)
      } finally {
        VarkaColumnarToRowExec.setFailEmissionForTesting(false)
      }
    }
    val causes = recorded
      .filter(VarkaJfrTestSupport.isEvent(_, classOf[VarkaFallbackEvent]))
      .filter(_.getString("kernelIdentity").contains("Varka_Project_"))
      .map(_.getString("cause"))
    assert(causes.contains(VarkaFallbackEvent.EMISSION_FAILURE), causes.mkString("; "))
  }

  test("the allocation sampler events every sampled batch, and the samples go clean once C2 " +
      "has the loop") {
    // The species-pollution check, wired: under a schedule that samples every batch, each
    // batch produces one event, and the suspect metric agrees with the events' verdicts. The
    // verdicts themselves show why the production schedule does not start at batch 1: the
    // first batches run the kernel interpreted, where every vector is a heap object, and only
    // once C2 has compiled the loop do the samples read clean. How many batches that takes is
    // the machine's business - this laptop compiled the loop inside 300 batches of 1024 rows,
    // GitHub's runner took 1112 - so the test runs rounds of batches until a whole round is
    // clean, and asserts only that this happens within a cap. The positive case - a compiled
    // loop that still boxes - cannot run here without making this JVM box (see
    // VarkaAllocationSamplerSuite). The test plan puts each batch in its own partition, so
    // every batch is its own evaluator's first: the samples are ordered by time, not by the
    // per-evaluator batch index.
    val batchesPerRound = 1200
    val maxRounds = 8
    val column = (0 until 1024).map(Int.box)
    val specs = Seq.fill(batchesPerRound)(BatchSpec("arrow", Seq(column)))
    VarkaKernelEvaluator.allocationSchedule = new VarkaAllocationSampler.Schedule(1, 1)
    val (plans, recorded) = try {
      VarkaJfrTestSupport.withJfrRecording(classOf[VarkaKernelAllocationEvent]) {
        val plans = Seq.newBuilder[VarkaProjectExec]
        var cleanRound = false
        var rounds = 0
        while (!cleanRound && rounds < maxRounds) {
          val plan = node(project(Alias(DateAdd(attrD, Literal(3)), "add")()), specs, Seq(attrD))
          assert(values(plan).length === batchesPerRound * column.length)
          assert(plan.metrics("numVarkaBatches").value === batchesPerRound)
          plans += plan
          rounds += 1
          cleanRound = plan.metrics("numSuspectAllocationSamples").value === 0
        }
        plans.result()
      }
    } finally {
      VarkaKernelEvaluator.allocationSchedule = VarkaAllocationSampler.Schedule.DEFAULT
    }
    val samples = recorded
      .filter(VarkaJfrTestSupport.isEvent(_, classOf[VarkaKernelAllocationEvent]))
      .filter(_.getString("kernelIdentity").contains("Varka_Project_"))
      .sortBy(_.getStartTime)
    val batches = plans.length * batchesPerRound
    assert(samples.length === batches)
    assert(samples.forall(_.getInt("rows") === column.length))
    assert(samples.forall(_.getLong("batchIndex") === 1L))
    val verdicts = samples.map(_.getBoolean("suspect"))
    val suspectMetric = plans.map(_.metrics("numSuspectAllocationSamples").value).sum
    assert(suspectMetric === verdicts.count(identity))
    val lastSuspect = verdicts.lastIndexOf(true)
    val bytes = samples.map(_.getLong("allocatedBytes"))
    assert(lastSuspect < batches - batchesPerRound,
      s"no clean round within $maxRounds rounds of $batchesPerRound batches; last suspect at " +
        s"sample $lastSuspect; tail bytes ${bytes.takeRight(20).mkString(" ")}")
    logInfo(s"allocation samples: ${verdicts.count(identity)} suspect, last at $lastSuspect " +
      s"of $batches, tail ${bytes.takeRight(5).mkString(" ")}")
  }

  /** Drives the evaluator directly, so a test can control when the next batch is requested. */
  private def evaluate(inputs: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    val factory = new VarkaProjectEvaluatorFactory(
      project(Alias(DateAdd(attrD, Literal(3)), "add")()),
      Seq(attrD),
      offHeapColumnVectorEnabled = false,
      classDumpDirectory = None,
      SQLMetrics.createMetric(sparkContext, "rows"),
      SQLMetrics.createMetric(sparkContext, "batches"),
      VarkaExecMetrics())
    factory.createEvaluator().eval(0, inputs)
  }
}
