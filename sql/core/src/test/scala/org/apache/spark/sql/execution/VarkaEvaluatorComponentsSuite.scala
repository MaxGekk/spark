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
import java.nio.file.Files

import scala.collection.mutable

import org.mockito.Mockito.{doAnswer, doThrow, mock, never, verify}

import org.apache.spark.{SparkFunSuite, TaskContext}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.varka.{VarkaBatchLedger, VarkaClassDump,
  VarkaFallbackAccounting}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * The guard paths of the Java components `VarkaEvaluatorBase` is composed of (VARKA-251), which the
 * evaluator suites reach only with healthy batches: the order and the guards of the task-end
 * listener, a close that re-enters the ledger, the accounting's null counters, and the class dump's
 * memo. Each test names the edit it would catch.
 */
class VarkaEvaluatorComponentsSuite extends SparkFunSuite {

  /** Runs `body` in a task context, then completes the task, as the ledger's listener expects. */
  private def inTask[T](body: => T): T = {
    val context = TaskContext.empty()
    TaskContext.setTaskContext(context)
    try {
      val result = body
      context.markTaskCompleted(None)
      result
    } finally {
      TaskContext.unset()
    }
  }

  private def vector(onClose: => Unit): ColumnVector = {
    val v = mock(classOf[ColumnVector])
    doAnswer { _ => onClose; null }.when(v).close()
    v
  }

  private def batchOf(vectors: ColumnVector*): ColumnarBatch =
    new ColumnarBatch(vectors.toArray, 1)

  test("the task end closes the open batches, runs every cleanup in order, then the allocator") {
    // Catches: folding the loops into one try (a throwing close would skip the cleanups), moving
    // the allocator's close ahead of the cleanups (the scratch release would meet a closed
    // allocator), and dropping a guard (a throwing hook would skip the ones after it).
    val initial = ArrowUtils.rootAllocator.getAllocatedMemory
    val events = mutable.ArrayBuffer.empty[String]
    inTask {
      val ledger = new VarkaBatchLedger
      val buffer = ledger.allocator().buffer(1024)
      val throwing = mock(classOf[ColumnVector])
      doThrow(new IllegalStateException("close")).when(throwing).close()
      val owned = vector(events += "close")
      ledger.trackOwned(batchOf(throwing, owned), Array(throwing, owned))
      ledger.onTaskCompletion(() => { events += "first"; throw new IllegalStateException("hook") })
      ledger.onTaskCompletion(() => { events += "second"; buffer.close() })
    }
    // The batch's second vector closed although the first one threw, then the cleanups ran in the
    // order they were registered although the first threw, and the allocator closed last: it
    // would have raised with the buffer still open had it closed before the second cleanup.
    assert(events.toSeq === Seq("close", "first", "second"))
    assert(ArrowUtils.rootAllocator.getAllocatedMemory === initial)
  }

  test("a close that re-enters the ledger does not skip the rest of the task end") {
    // Catches: iterating the live map rather than a snapshot, which a re-entrant `release` turns
    // into a ConcurrentModificationException that skips the cleanups and the allocator's close.
    val initial = ArrowUtils.rootAllocator.getAllocatedMemory
    var ran = false
    inTask {
      val ledger = new VarkaBatchLedger
      val buffer = ledger.allocator().buffer(1024)
      // Two batches whose vectors each release the other batch, once, so that whichever the
      // task end closes first removes the other from the map it is walking.
      var batchA: ColumnarBatch = null
      var batchB: ColumnarBatch = null
      var releasedB, releasedA = false
      val a = vector(if (!releasedB) { releasedB = true; ledger.release(batchB) })
      val b = vector(if (!releasedA) { releasedA = true; ledger.release(batchA) })
      batchA = batchOf(a)
      batchB = batchOf(b)
      ledger.trackOwned(batchA, Array(a))
      ledger.trackOwned(batchB, Array(b))
      ledger.onTaskCompletion(() => { ran = true; buffer.close() })
    }
    assert(ran, "the cleanups were skipped")
    assert(ArrowUtils.rootAllocator.getAllocatedMemory === initial)
  }

  test("releasing a forwarded batch closes none of the input's vectors") {
    // Catches: a forwarded batch reaching `release`'s "not one of ours" arm, which closes the
    // batch whole and takes the input's vectors with it.
    inTask {
      val ledger = new VarkaBatchLedger
      val column = mock(classOf[ColumnVector])
      val input = new ColumnarBatch(Array(column), 1)
      val forwarded = ledger.forwardColumns(input, Array(0))
      ledger.release(forwarded)
      verify(column, never()).close()
    }
  }

  test("the fallback accounting counts under its cause and tolerates a node with no counters") {
    // Catches: dropping the null check in `add`, which would be an NPE inside `serveBatch`'s catch
    // arm for a node that registered a partial `VarkaExecMetrics`.
    val none = new VarkaFallbackAccounting(
      new VarkaFallbackAccounting.Counters(null, null, null, null, null), () => "kernel")
    none.kernelFailure(new RuntimeException("boom"), null)
    none.rowPathFailure(new RuntimeException("boom"))
    none.declinedBatch(1, null)
    none.nonArrowBatch()
    none.allocationSample(1L << 30, 1)

    def metric(): SQLMetric = new SQLMetric("sum")
    val Seq(kernel, rowPath, declined, nonArrow, suspect) = Seq.fill(5)(metric())
    val counted = new VarkaFallbackAccounting(
      new VarkaFallbackAccounting.Counters(kernel, rowPath, declined, nonArrow, suspect),
      () => "kernel")
    counted.kernelFailure(new RuntimeException("boom"), "further kernel")
    counted.rowPathFailure(new RuntimeException("boom"))
    counted.declinedBatch(1, "further kernel")
    counted.declinedBatch(1, null)
    counted.nonArrowBatch()
    counted.allocationSample(1L << 30, 1)
    assert(Seq(kernel, rowPath, declined, nonArrow, suspect).map(_.value) === Seq(1, 1, 2, 1, 1))
  }

  test("the class dump writes a shape once, and a failed write is neither thrown nor remembered") {
    // Catches: a broken memo (a disk write per task), a throw out of a diagnostics write, and a
    // failed write left in the memo (the shape never dumped again).
    withTempDir { dir =>
      val first = Array[Byte](1, 2, 3)
      VarkaClassDump.dump(dir.getAbsolutePath, "VarkaFusedProjection_a.java", first)
      VarkaClassDump.dump(dir.getAbsolutePath, "VarkaFusedProjection_a.java", Array[Byte](9))
      val file = new File(dir, "VarkaFusedProjection_a.class")
      assert(Files.readAllBytes(file.toPath).toSeq === first.toSeq, "the memo dumped it twice")

      // A directory that is a file: creating it fails, nothing throws, and the shape is dumped
      // once the obstacle is gone.
      val blocked = new File(dir, "blocked")
      Files.write(blocked.toPath, Array[Byte](0))
      VarkaClassDump.dump(blocked.getAbsolutePath, "VarkaFusedProjection_b.java", first)
      assert(blocked.isFile)
      assert(blocked.delete())
      VarkaClassDump.dump(blocked.getAbsolutePath, "VarkaFusedProjection_b.java", first)
      assert(Files.readAllBytes(new File(blocked, "VarkaFusedProjection_b.class").toPath).toSeq ===
        first.toSeq)
    }
  }
}
