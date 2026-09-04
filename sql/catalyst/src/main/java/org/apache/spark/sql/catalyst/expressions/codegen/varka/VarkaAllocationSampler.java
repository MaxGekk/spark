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

package org.apache.spark.sql.catalyst.expressions.codegen.varka;

import java.lang.management.ManagementFactory;

/**
 * The allocation-rate check the evaluator runs on a sample of kernel batches: the runtime half
 * of the species-pollution finding that {@code VarkaAssemblySuite} asserts at test time. The
 * two halves measure the same thing the same way - bytes the current thread allocated across
 * a kernel call, from {@link com.sun.management.ThreadMXBean} - because the failure is
 * environmental rather than a property of the emitted bytes: the same kernel class boxes or
 * does not depending on what else ran hot in the JVM before it, so a clean test JVM proves
 * nothing about a production executor that also ran a 128-bit species.
 *
 * <p>Everything here is a pure decision so it can be unit-tested without a kernel:
 * <ul>
 *   <li>{@link Schedule#due} says which batches are sampled. Not the first ones: an
 *   interpreted or C1-compiled Vector API loop allocates every vector it touches, and only C2's
 *   escape analysis scalarizes them, so a sample before the loop reached tier 4 would report
 *   boxing that is about to stop. The schedule starts at batch 512 - two million rows at the
 *   default batch size, long past C2's back-edge threshold - then samples at each power of
 *   two and every 4096th batch after that, so a long task keeps being checked without paying
 *   the two management calls per batch.</li>
 *   <li>{@link #suspect} is the verdict on one sample: more than a fixed allowance plus one
 *   byte per row. The allowance covers what a healthy call allocates regardless of length
 *   (the emitted class allocates nothing, but a caller's segment views and the management
 *   call's own bookkeeping are a few hundred bytes); the per-row term is the task-55 ceiling.
 *   A boxing loop allocates a vector object per operator per lane group - hundreds of bytes
 *   per row - so the margin between healthy and suspect is two orders of magnitude and the
 *   exact constants do not matter.</li>
 *   <li>{@link Tracker} turns samples into one warning: two suspect samples in a row, once
 *   per evaluator. One sample can land on a deoptimization or a GC safepoint; two consecutive
 *   ones, at least a batch's worth of rows apart, cannot both be that.</li>
 * </ul>
 */
public final class VarkaAllocationSampler {

  private VarkaAllocationSampler() {}

  /** Bytes a healthy call may allocate regardless of its length. */
  public static final long FIXED_ALLOWANCE_BYTES = 4096;

  /** Bytes a healthy call may allocate per row on top of the fixed allowance. */
  public static final long BYTES_PER_ROW_ALLOWANCE = 1;

  /**
   * Which batches are sampled: none before {@code first}, then every power of two and every
   * multiple of {@code every}. {@link #DEFAULT} is the production schedule; suites use a
   * denser one to reach a sample in a short query.
   */
  public record Schedule(long first, long every) {
    public static final Schedule DEFAULT = new Schedule(512, 4096);

    public Schedule {
      if (first < 1 || every < 1) {
        throw new IllegalArgumentException("first and every must be positive: " + this);
      }
    }

    /** Whether the batch with this one-based index is sampled. */
    public boolean due(long batchIndex) {
      return batchIndex >= first
          && ((batchIndex & (batchIndex - 1)) == 0 || batchIndex % every == 0);
    }
  }

  private static final com.sun.management.ThreadMXBean THREADS = threadBean();

  private static com.sun.management.ThreadMXBean threadBean() {
    var bean = ManagementFactory.getThreadMXBean();
    if (bean instanceof com.sun.management.ThreadMXBean sun
        && sun.isThreadAllocatedMemorySupported()) {
      if (!sun.isThreadAllocatedMemoryEnabled()) {
        sun.setThreadAllocatedMemoryEnabled(true);
      }
      return sun;
    }
    return null;
  }

  /** Whether this JVM can report per-thread allocation; false disables sampling entirely. */
  public static boolean supported() {
    return THREADS != null;
  }

  /**
   * Bytes the current thread has allocated so far; the difference of two readings around a
   * call is what the call allocated. Not the read-and-store of a Java local: the call is
   * about two microseconds of management overhead, which is why it is sampled, not run on
   * every batch.
   */
  public static long allocatedBytes() {
    return THREADS.getCurrentThreadAllocatedBytes();
  }

  /** The verdict on one sample. */
  public static boolean suspect(long allocatedBytes, int rows) {
    return allocatedBytes > FIXED_ALLOWANCE_BYTES + BYTES_PER_ROW_ALLOWANCE * rows;
  }

  /**
   * Per-evaluator state: consecutive suspect samples, and whether the warning went out. Not
   * thread-safe, like the evaluator that owns it - one task, one thread.
   */
  public static final class Tracker {
    private int consecutiveSuspects;
    private boolean warned;

    /**
     * Records one sample's verdict; true exactly once, on the second consecutive suspect
     * sample, when the caller should warn.
     */
    public boolean record(boolean suspect) {
      if (!suspect) {
        consecutiveSuspects = 0;
        return false;
      }
      consecutiveSuspects++;
      if (consecutiveSuspects >= 2 && !warned) {
        warned = true;
        return true;
      }
      return false;
    }

    public boolean warned() {
      return warned;
    }
  }
}
