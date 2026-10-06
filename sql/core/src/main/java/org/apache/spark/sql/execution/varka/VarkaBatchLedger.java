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


package org.apache.spark.sql.execution.varka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.util.control.NonFatal;

import org.apache.arrow.memory.BufferAllocator;

import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskCompletionListener;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A Varka evaluator's task-lifetime memory: the one Arrow child allocator for the task, the
 * batches handed out and not released yet, and the one task-completion listener that closes what
 * is still open, runs the evaluator's registered cleanups, and closes the allocator.
 *
 * <p>One allocator for the whole task, created on first use: allocating one per batch - and
 * registering a listener per batch to close it - would hold every result batch off-heap until
 * the task ended, which is exactly what the streaming iterator model exists to avoid. Each open
 * batch maps to the vectors the evaluator owns in it, never the forwarded input vectors; a batch
 * is normally released by the caller as soon as it is done with it, and the map is the safety
 * net for a task that stops early (a LIMIT, a failure). Called from the task thread only.
 */
public final class VarkaBatchLedger {

  private static final SparkLogger LOG = SparkLoggerFactory.getLogger(VarkaBatchLedger.class);

  /** The owned vectors of a batch that owns none: a forwarded batch. */
  public static final ColumnVector[] NO_VECTORS = new ColumnVector[0];

  private final Map<ColumnarBatch, ColumnVector[]> openBatches = new HashMap<>();
  private final List<Runnable> cleanups = new ArrayList<>();
  private BufferAllocator allocator;
  private boolean cleanupRegistered;

  /**
   * Adds a cleanup the task-completion listener runs after closing the open batches and before
   * closing the allocator, each guarded on its own: the evaluator's scratch release, a subclass's
   * hook. Registered when the evaluator is built, not per batch.
   */
  public void onTaskCompletion(Runnable cleanup) {
    cleanups.add(cleanup);
  }

  /** Returns the task's Arrow child allocator, creating it on first use. */
  public BufferAllocator allocator() {
    ensureCleanup();
    if (allocator == null) {
      allocator = ArrowUtils.rootAllocator().newChildAllocator("varka-kernels", 0, Long.MAX_VALUE);
    }
    return allocator;
  }

  /**
   * Registers the single task-completion listener. The flag records a registration that
   * happened, so it is set after the call and not before it: setting it first meant a throw from
   * {@code addTaskCompletionListener} - outside a task {@code TaskContext.get()} is null - left
   * the evaluator believing it had a listener, and the next {@link #allocator} would then create a
   * child allocator that nothing ever closes. Not reachable from a query, where every evaluator is
   * built inside a {@code PartitionEvaluator}, but reachable from a harness that drives one
   * directly.
   */
  public void ensureCleanup() {
    if (!cleanupRegistered) {
      TaskCompletionListener listener = context -> completeTask();
      TaskContext.get().addTaskCompletionListener(listener);
      cleanupRegistered = true;
    }
  }

  /**
   * Every stage here frees task-lifetime Arrow memory, and each is guarded separately so that one
   * failure cannot skip the others: a throw must not skip the allocator close below, or the child
   * allocator's accounting leaks against the shared root for the JVM's lifetime and the task's
   * real error is masked (VARKA-21 review, second pass). One try around the whole prologue would
   * satisfy the letter of that and not its point: a throwing batch close would still cost the
   * scratch release and the hook.
   */
  private void completeTask() {
    // A snapshot, closed and then cleared: a close that re-entered the ledger would otherwise
    // modify the map under the iteration and skip the cleanups and the allocator close below.
    for (ColumnVector[] owned : new ArrayList<>(openBatches.values())) {
      closeAll(owned, "a Varka batch left open at task completion");
    }
    openBatches.clear();
    for (Runnable cleanup : cleanups) {
      try {
        cleanup.run();
      } catch (Throwable e) {
        rethrowIfFatal(e);
        LOG.warn("Varka task-cleanup hook failed.", e);
      }
    }
    if (allocator != null) {
      allocator.close();
      allocator = null;
    }
  }

  /**
   * Takes ownership of a batch the caller built itself - a fallback batch, every column the
   * caller's own - so that the task-completion listener closes it if the task stops before the
   * caller releases it.
   */
  public ColumnarBatch track(ColumnarBatch batch) {
    var owned = new ColumnVector[batch.numCols()];
    for (int c = 0; c < owned.length; c++) {
      owned[c] = batch.column(c);
    }
    trackOwned(batch, owned);
    return batch;
  }

  /**
   * The output batch for a projection that only forwards columns of its input: the input's own
   * vectors, selected and reordered by {@code ordinals}, with nothing copied and no kernel run.
   *
   * <p>It is tracked owning nothing, so {@link #release} unregisters it and closes none of its
   * columns - they belong to the input batch, exactly as a forwarded entry's column does on the
   * kernel path. A batch built with {@code new ColumnarBatch(...)} and not tracked would instead
   * reach {@code release}'s "not one of ours" arm and be closed whole, taking the input's vectors
   * with it.
   */
  public ColumnarBatch forwardColumns(ColumnarBatch input, int[] ordinals) {
    var columns = new ColumnVector[ordinals.length];
    for (int i = 0; i < ordinals.length; i++) {
      columns[i] = input.column(ordinals[i]);
    }
    var batch = new ColumnarBatch(columns, input.numRows());
    trackOwned(batch, NO_VECTORS);
    return batch;
  }

  /** Registers {@code batch} as open, owning exactly the vectors in {@code owned}. */
  public void trackOwned(ColumnarBatch batch, ColumnVector[] owned) {
    ensureCleanup();
    openBatches.put(batch, owned);
  }

  /**
   * Releases a batch obtained from the evaluator or handed to {@link #track}: closes exactly the
   * vectors the evaluator owns in it, so a forwarded input vector is left to its input batch.
   *
   * <p>Each close is guarded, even though this is the ordinary path with a caller above it that
   * could handle a throw. The registry entry is removed first, so by the time anything closes,
   * this call is the only route to those vectors: a throw part-way would strand the rest where
   * nothing - not a later {@code release}, not the task-completion listener - can reach them. The
   * task's allocator close then finds outstanding bytes and raises "Memory was leaked by query"
   * <i>instead of</i> completing, so the child allocator's accounting stays charged against the
   * shared root for the JVM's lifetime. Closing everything and logging what failed is strictly
   * better here than handing the caller an exception it can do nothing useful with.
   */
  public void release(ColumnarBatch batch) {
    ColumnVector[] owned = openBatches.remove(batch);
    if (owned != null) {
      closeAll(owned, "a Varka output vector on release");
    } else {
      // Not one of ours - nothing borrowed can be inside, so closing it whole is safe.
      batch.close();
    }
  }

  /**
   * Closes one resource on a path that must not be derailed by the close itself: whatever it
   * throws is logged and swallowed.
   *
   * <p>Used where something has already gone wrong, or where the caller is on its way out. On a
   * failure path the original exception is the one worth keeping - closing a list with a plain
   * loop there both strands every resource after the one that threw and replaces the error being
   * reported with a cleanup error, which is the same objection the task-completion listener's own
   * guard was written for.
   */
  public static void closeQuietly(AutoCloseable resource, String what) {
    try {
      resource.close();
    } catch (Throwable e) {
      rethrowIfFatal(e);
      LOG.warn("Closing " + what + " failed.", e);
    }
  }

  /** {@link #closeQuietly} over an array, guarding each element separately. */
  private static void closeAll(ColumnVector[] resources, String what) {
    for (ColumnVector resource : resources) {
      closeQuietly(resource, what);
    }
  }

  /**
   * Rethrows {@code e} unless it is non-fatal in Scala's sense ({@code NonFatal}): the
   * evaluators catch what {@code NonFatal} admits and let the rest - a VM error, an interrupt, a
   * linkage error - end the task, and these Java components keep that line where it was.
   */
  static void rethrowIfFatal(Throwable e) {
    if (!NonFatal.apply(e)) {
      throw VarkaBatchLedger.<RuntimeException>sneaky(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> E sneaky(Throwable e) throws E {
    throw (E) e;
  }
}
