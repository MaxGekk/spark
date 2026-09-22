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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.columnar.{CachedBatch}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils

/**
 * Fast-path iterator that converts ArrowCachedBatch to InternalRow.
 * Uses pre-built typed column readers to avoid per-row pattern matching,
 * and writes directly to UnsafeRowWriter to avoid intermediate SpecificInternalRow.
 * Only used for schemas without complex types (Array/Struct/Map).
 */
private class ArrowCachedBatchToInternalRowIterator(
    batchIter: Iterator[CachedBatch],
    cacheSchema: StructType,
    selectedSchema: StructType,
    columnIndices: Array[Int],
    timeZoneId: String,
    prefetchEnabled: Boolean = false) extends Iterator[InternalRow] {

  import java.util.concurrent.{Callable, ExecutionException, Future, Executors,
    ExecutorService}

  private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
    s"ArrowCachedBatchToInternalRowIterator-${TaskContext.get().taskAttemptId()}",
    0,
    Long.MaxValue)

  private var currentRoot: VectorSchemaRoot = null
  private var currentRowIndex: Int = 0
  private var currentRowCount: Int = 0

  private val numFields = selectedSchema.length
  private val arrowSchema = ArrowUtils.toArrowSchema(
    cacheSchema, timeZoneId, false, false, losslessInternalTypes = true)

  // Projection pushdown: see ArrowCachedBatchToColumnarBatchIterator. When every selected column
  // maps to a distinct cached column, read a batch holding only the selected columns' buffers so
  // unselected columns are never copied off-heap, loaded, or decompressed, and readers bind
  // positionally. If any selected attribute is absent from the cache (index -1), fall back to the
  // full batch and bind readers via columnIndices.
  private val cacheFields = arrowSchema.getFields.asScala.toSeq
  private val canProjectOnLoad = columnIndices.forall(_ >= 0)
  private val projectedSchema =
    if (canProjectOnLoad) {
      new org.apache.arrow.vector.types.pojo.Schema(columnIndices.map(cacheFields).toList.asJava)
    } else {
      arrowSchema
    }

  // Pre-build typed readers per column at init time -- no per-row pattern match
  private val columnReaders: Array[ArrowColumnReader] =
    selectedSchema.fields.map(f => ArrowColumnReader.create(f.dataType))

  // Write directly to UnsafeRow -- no intermediate SpecificInternalRow + UnsafeProjection
  private val rowWriter = new UnsafeRowWriter(numFields)

  // Prefetch support: deserialize the next batch in background while current batch is consumed
  private val prefetchExecutor: ExecutorService = if (prefetchEnabled) {
    Executors.newSingleThreadExecutor(r => {
      val t = new Thread(r, "arrow-cache-row-prefetch")
      t.setDaemon(true)
      t
    })
  } else {
    null
  }
  private var prefetchFuture: Future[VectorSchemaRoot] = _

  // Register cleanup
  Option(TaskContext.get()).foreach { tc =>
    tc.addTaskCompletionListener[Unit] { _ =>
      // Stop the worker and close any root it already produced before closing the allocator;
      // otherwise a prefetched root produced after a short-circuiting consumer (e.g. LIMIT) stops
      // reading would leak and allocator.close() would fail with "Memory was leaked by query".
      prefetchFuture = ArrowCachedBatchSerializer.drainAndClosePrefetch(
        prefetchExecutor, prefetchFuture)
      if (currentRoot != null) {
        currentRoot.close()
        currentRoot = null
      }
      allocator.close()
    }
  }

  override def hasNext: Boolean = {
    // Keep loading batches until the current one has rows or the input is exhausted. A cached
    // batch can legitimately have zero rows (e.g. an empty ColumnarBatch from a columnar source);
    // without this loop an empty batch would make hasNext return false and silently drop all
    // remaining, non-empty batches.
    while (currentRowIndex >= currentRowCount && (prefetchFuture != null || batchIter.hasNext)) {
      loadNextBatch()
    }
    if (currentRowIndex < currentRowCount) {
      true
    } else {
      if (currentRoot != null) {
        currentRoot.close()
        currentRoot = null
      }
      false
    }
  }

  override def next(): InternalRow = {
    if (!hasNext) {
      throw new NoSuchElementException("No more rows")
    }

    rowWriter.reset()
    rowWriter.zeroOutNullBytes()

    val rowIdx = currentRowIndex
    var i = 0
    while (i < numFields) {
      val reader = columnReaders(i)
      if (reader.vector.isNull(rowIdx)) {
        rowWriter.setNullAt(i)
      } else {
        reader.read(rowIdx, i, rowWriter)
      }
      i += 1
    }

    currentRowIndex += 1
    rowWriter.getRow()
  }

  /** Deserialize a cached batch into a VectorSchemaRoot. */
  private def deserializeBatch(cachedBatch: ArrowCachedBatch): VectorSchemaRoot = {
    // Projection pushdown: read only the selected columns' buffers out of the cached bytes.
    val recordBatch = if (canProjectOnLoad) {
      ArrowCachedBatchSerializer.readProjectedRecordBatch(
        cachedBatch.arrowData, cacheFields, columnIndices, allocator)
    } else {
      val in = new ByteArrayInputStream(cachedBatch.arrowData)
      val readChannel = new ReadChannel(Channels.newChannel(in))
      MessageSerializer.deserializeRecordBatch(readChannel, allocator)
    }
    try {
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
    } finally {
      recordBatch.close()
    }
  }

  /** Submit prefetch for the next batch if available. */
  private def submitPrefetch(): Unit = {
    if (prefetchEnabled && batchIter.hasNext) {
      val nextCachedBatch = batchIter.next().asInstanceOf[ArrowCachedBatch]
      prefetchFuture = prefetchExecutor.submit(new Callable[VectorSchemaRoot] {
        override def call(): VectorSchemaRoot = deserializeBatch(nextCachedBatch)
      })
    }
  }

  private def loadNextBatch(): Unit = {
    if (currentRoot != null) {
      currentRoot.close()
      currentRoot = null
    }

    val root = if (prefetchFuture != null) {
      // Use the prefetched result
      val r = try {
        prefetchFuture.get()
      } catch {
        case e: ExecutionException => throw e.getCause
      }
      prefetchFuture = null
      r
    } else {
      // No prefetch available, deserialize synchronously
      val cachedBatch = batchIter.next().asInstanceOf[ArrowCachedBatch]
      deserializeBatch(cachedBatch)
    }

    currentRoot = root

    // Update pre-built readers with new vectors. When projected on load, the root holds the
    // selected columns positionally; otherwise it holds all cache columns, selected via
    // columnIndices.
    var i = 0
    while (i < numFields) {
      val vectorIndex = if (canProjectOnLoad) i else columnIndices(i)
      columnReaders(i).setVector(root.getVector(vectorIndex))
      i += 1
    }

    currentRowIndex = 0
    currentRowCount = root.getRowCount

    // Start prefetching the next batch while this one is being consumed
    submitPrefetch()
  }
}
