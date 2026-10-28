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

package org.apache.spark.sql.execution.columnar;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import scala.Option;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.TaskContext$;
import org.apache.spark.TaskContextImpl;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.columnar.CachedBatch;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Rows as the Arrow cache serves them, with no session (VARKA-509): the two iterators
 * {@code ArrowCachedBatchSerializer} runs in a task - the one that writes rows into cached Arrow
 * batches and the one that reads them back as columnar batches - driven over a local list. A
 * suite that hands these batches to a Varka evaluator feeds it the vectors the cache would, by
 * construction.
 *
 * <p>The reading iterator closes a batch's vectors when it moves to the next, so {@link #each}
 * hands the batches over one at a time and the consumer must be done with each before it
 * returns. Both iterators name an allocator after the running task and release it when the task
 * completes, so {@code each} runs inside {@link #inTask}, as the evaluators do.
 */
public final class VarkaCacheBatches {

  private VarkaCacheBatches() {}

  /**
   * Runs {@code body} as a task of its own: a {@code TaskContext} on this thread, completed
   * afterwards so that every listener the iterators and the evaluators registered releases what
   * it holds.
   */
  public static <T> T inTask(Supplier<T> body) {
    TaskContextImpl context = TaskContext$.MODULE$.empty();
    TaskContext$.MODULE$.setTaskContext(context);
    try {
      return body.get();
    } finally {
      try {
        context.markTaskCompleted(Option.empty());
      } finally {
        TaskContext$.MODULE$.unset();
      }
    }
  }

  /** {@code rows} with {@code schema}'s columns, batch by batch, as the cache reads them back. */
  public static void each(List<InternalRow> rows, Seq<Attribute> schema, SQLConf conf,
      Consumer<ColumnarBatch> consumer) {
    StructType sparkSchema = DataTypeUtils.fromAttributes(schema);
    var cached = new InternalRowToArrowCachedBatchIterator(
        CollectionConverters.asScala(rows.iterator()),
        schema,
        sparkSchema,
        conf.arrowMaxRecordsPerBatch(),
        conf.arrowMaxBytesPerBatch(),
        conf.sessionLocalTimeZone(),
        conf.arrowCompressionCodec(),
        conf.arrowZstdCompressionLevel());
    // Scala's Iterator is covariant, which Java's generics cannot say.
    @SuppressWarnings("unchecked")
    var asCached = (scala.collection.Iterator<CachedBatch>) (scala.collection.Iterator<?>) cached;
    var batches = new ArrowCachedBatchToColumnarBatchIterator(
        asCached,
        sparkSchema,
        sparkSchema,
        CachedColumnIndices$.MODULE$.apply(schema, schema),
        conf.sessionLocalTimeZone(),
        false);
    while (batches.hasNext()) {
      consumer.accept(batches.next());
    }
  }
}
