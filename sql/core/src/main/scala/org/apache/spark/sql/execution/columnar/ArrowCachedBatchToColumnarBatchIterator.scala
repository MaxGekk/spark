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

package org.apache.spark.sql.execution.columnar

import java.io.{ByteArrayInputStream}
import java.nio.channels.Channels

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.{ReadChannel}
import org.apache.arrow.vector.ipc.message.{MessageSerializer}

import org.apache.spark.{TaskContext}
import org.apache.spark.sql.columnar.{CachedBatch}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

/**
 * Iterator that converts ArrowCachedBatch to ColumnarBatch.
 */
private class ArrowCachedBatchToColumnarBatchIterator(
    batchIter: Iterator[CachedBatch],
    cacheSchema: StructType,
    selectedSchema: StructType,
    columnIndices: Array[Int],
    timeZoneId: String,
    prefetchEnabled: Boolean = false) extends Iterator[ColumnarBatch] {

  import java.util.concurrent.{Callable, ExecutionException, Executors, ExecutorService, Future}

  private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
    s"ArrowCachedBatchToColumnarBatchIterator-${TaskContext.get().taskAttemptId()}",
    0,
    Long.MaxValue)

  private val arrowSchema = ArrowUtils.toArrowSchema(
    cacheSchema, timeZoneId, false, false, losslessInternalTypes = true)

  // Projection pushdown: the cached batch stores all cache columns, but only the selected ones
  // are needed. When every selected column maps to a distinct cached column, read a batch holding
  // only the selected columns' buffers (in columnIndices order) so unselected columns are never
  // copied off-heap, loaded, or decompressed. The projected schema's field order matches
  // columnIndices, so the loaded root's vectors are already in output order. If any selected
  // attribute is absent from the cache schema (index -1), fall back to reading the full batch.
  private val cacheFields = arrowSchema.getFields.asScala.toSeq
  private val canProjectOnLoad = columnIndices.forall(_ >= 0)
  private val projectedSchema =
    if (canProjectOnLoad) {
      new org.apache.arrow.vector.types.pojo.Schema(columnIndices.map(cacheFields).toList.asJava)
    } else {
      arrowSchema
    }

  // Track only the previous root to close it when next batch is produced
  private var previousRoot: VectorSchemaRoot = null

  // Prefetch support: deserialize the next batch into its own root in a background thread while
  // the current batch is being consumed. Only the deserialization (IPC read + decompression +
  // loading into a fresh root) happens off-thread; closing the previous root stays on the
  // consumer thread in next(), so the vectors backing a returned ColumnarBatch are never released
  // while the consumer may still read them.
  private val prefetchExecutor: ExecutorService = if (prefetchEnabled) {
    Executors.newSingleThreadExecutor(r => {
      val t = new Thread(r, "arrow-cache-prefetch")
      t.setDaemon(true)
      t
    })
  } else {
    null
  }
  private var prefetchFuture: Future[VectorSchemaRoot] = _

  // Register cleanup - close remaining root and allocator when task completes
  Option(TaskContext.get()).foreach { tc =>
    tc.addTaskCompletionListener[Unit] { _ =>
      // Stop the worker and close any root it already produced before closing the allocator.
      // A short-circuiting consumer (e.g. LIMIT) can trigger task completion while a prefetched
      // root is in flight; simply cancelling the future would drop that root and allocator.close()
      // would then fail with "Memory was leaked by query".
      prefetchFuture = ArrowCachedBatchSerializer.drainAndClosePrefetch(
        prefetchExecutor, prefetchFuture)
      if (previousRoot != null) {
        previousRoot.close()
        previousRoot = null
      }
      allocator.close()
    }
  }

  override def hasNext: Boolean = prefetchFuture != null || batchIter.hasNext

  override def next(): ColumnarBatch = {
    // Close the previous root since the consumer has moved on from the batch it backed.
    if (previousRoot != null) {
      previousRoot.close()
      previousRoot = null
    }

    val root = if (prefetchFuture != null) {
      val r = try {
        prefetchFuture.get()
      } catch {
        case e: ExecutionException => throw e.getCause
      }
      prefetchFuture = null
      r
    } else {
      deserializeToRoot(batchIter.next().asInstanceOf[ArrowCachedBatch])
    }

    previousRoot = root

    // When projected on load, the root already holds only the selected columns in output order,
    // so wrap its vectors directly. Otherwise it holds all cache columns and must be selected.
    val selectedColumns = if (canProjectOnLoad) {
      root.getFieldVectors.asScala.map(v => new ArrowColumnVector(v)).toArray[ColumnVector]
    } else {
      val allColumns = root.getFieldVectors.asScala.map { vector =>
        new ArrowColumnVector(vector)
      }.toArray[ColumnVector]
      columnIndices.map(allColumns(_))
    }
    val batch = new ColumnarBatch(selectedColumns, root.getRowCount)

    // Start prefetching the next batch while this one is being consumed.
    submitPrefetch()

    batch
  }

  /** Deserialize a cached batch into its own freshly-created root. Does not touch other roots. */
  private def deserializeToRoot(cachedBatch: ArrowCachedBatch): VectorSchemaRoot = {
    // Projection pushdown: read only the selected columns' buffers out of the cached bytes, so
    // unselected columns are never copied off-heap, loaded, or decompressed.
    val recordBatch = if (canProjectOnLoad) {
      ArrowCachedBatchSerializer.readProjectedRecordBatch(
        cachedBatch.arrowData, cacheFields, columnIndices, allocator)
    } else {
      val in = new ByteArrayInputStream(cachedBatch.arrowData)
      val readChannel = new ReadChannel(Channels.newChannel(in))
      MessageSerializer.deserializeRecordBatch(readChannel, allocator)
    }
    Utils.tryWithSafeFinally {
      val root = VectorSchemaRoot.create(projectedSchema, allocator)
      // VectorLoader.load fills vectors incrementally, so a failure (malformed data, decompression
      // error, OOM) can occur after earlier vectors have allocated buffers. Close the partially
      // loaded root on failure, otherwise it becomes unreachable and the later allocator.close()
      // fails with a leak error that masks the original exception.
      try {
        val loader = new VectorLoader(root)
        loader.load(recordBatch)
        root
      } catch {
        case t: Throwable =>
          root.close()
          throw t
      }
    } {
      recordBatch.close()
    }
  }

  /** Submit deserialization of the next batch to the background thread, if prefetch is enabled. */
  private def submitPrefetch(): Unit = {
    if (prefetchEnabled && batchIter.hasNext) {
      val nextCachedBatch = batchIter.next().asInstanceOf[ArrowCachedBatch]
      prefetchFuture = prefetchExecutor.submit(new Callable[VectorSchemaRoot] {
        override def call(): VectorSchemaRoot = deserializeToRoot(nextCachedBatch)
      })
    }
  }
}
