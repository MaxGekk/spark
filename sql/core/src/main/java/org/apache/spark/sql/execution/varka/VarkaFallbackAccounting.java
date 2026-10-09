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

import java.util.function.Supplier;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaAllocationSampler;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaFallbackEvent;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelAllocationEvent;
import org.apache.spark.sql.execution.metric.SQLMetric;

/**
 * Per-batch fallback accounting, shared by every Varka evaluator (VARKA-21 review: the nodes
 * carried byte-identical copies of these blocks, which had already begun to drift). Each method
 * counts and events one batch under its actual cause; the caller then takes its own fallback
 * path. Also the species-pollution check: a kernel that boxes still answers correctly, so no
 * fallback path and no differential test can see it - only its allocation rate can, sampled on
 * the schedule {@link VarkaAllocationSampler} explains, never on every batch.
 *
 * <p>The kernel's identity is rendered only when a line or an event needs it: rendering hashes the
 * canonical IR, which the metered-but-uneventful path must not pay. So it arrives as a supplier,
 * built once per evaluator.
 */
public final class VarkaFallbackAccounting {

  private static final SparkLogger LOG =
      SparkLoggerFactory.getLogger(VarkaFallbackAccounting.class);

  /** The counters each cause adds to, each null where the node registered none. */
  public record Counters(
      SQLMetric kernelFailures,
      SQLMetric rowPathFailures,
      SQLMetric declined,
      SQLMetric nonArrow,
      SQLMetric suspectAllocationSamples) {}

  private final Counters counters;
  private final Supplier<String> kernelIdentity;
  private final VarkaAllocationSampler.Tracker allocationTracker =
      new VarkaAllocationSampler.Tracker();
  private final boolean allocationSampling = VarkaAllocationSampler.supported();
  private long kernelBatches;

  public VarkaFallbackAccounting(Counters counters, Supplier<String> kernelIdentity) {
    this.counters = counters;
    this.kernelIdentity = kernelIdentity;
  }

  private static void add(SQLMetric metric) {
    if (metric != null) {
      metric.add(1L);
    }
  }

  /**
   * The ghost fallback's bookkeeping: an error from the emitted kernel itself. {@code kernel}
   * names a further kernel of the projection when that is the one that failed, and is null for
   * the evaluator's own.
   */
  public void kernelFailure(Throwable e, String kernel) {
    String identity = kernel != null ? kernel : kernelIdentity.get();
    LOG.warn("The Varka SIMD kernels " + identity
        + " failed on this batch; falling back to the per-row path.", e);
    add(counters.kernelFailures());
    fallbackEvent(VarkaFallbackEvent.KERNEL_FAILURE, () -> identity, e.getClass().getName());
  }

  /**
   * A catchable failure from the per-row machinery running beside the kernel - the residual or
   * merge projection's compile or evaluation - which the VARKA-21 review split out of the kernel
   * metric: it is not the kernel's failure, and a throwing lazy re-runs its initializer, so
   * counting it there would inflate the ghost-fallback metric on every batch. Counted under its
   * own bounded cause metric, evented and logged.
   */
  public void rowPathFailure(Throwable e) {
    LOG.warn("The per-row machinery beside the Varka kernel " + kernelIdentity.get()
        + " failed on this batch; falling back to the per-row path.", e);
    add(counters.rowPathFailures());
    fallbackEvent(VarkaFallbackEvent.ROW_PATH_FAILURE, kernelIdentity, e.getClass().getName());
  }

  /**
   * A batch the kernel itself declined: a lowering that is correct only over part of its input
   * domain met a value outside it and reported it rather than publishing an answer it does not
   * have. Logged at debug rather than warning: unlike the ghost fallback this is a designed
   * outcome, not a defect, and a batch of far-future dates would otherwise fill the log. The
   * event's third field is its exceptionClass, and a declined batch has no exception: the status
   * is in the log line, where it belongs.
   */
  public void declinedBatch(int status, String kernel) {
    if (LOG.isDebugEnabled()) {
      String identity = kernel != null ? kernel : kernelIdentity.get();
      LOG.debug("The Varka SIMD kernels " + identity + " declined this batch (status " + status
          + "); falling back to the per-row path.");
    }
    add(counters.declined());
    if (kernel != null) {
      fallbackEvent(VarkaFallbackEvent.RANGE_DECLINED, () -> kernel, "");
    } else {
      fallbackEvent(VarkaFallbackEvent.RANGE_DECLINED, kernelIdentity, "");
    }
  }

  /** A non-empty batch whose referenced columns are not Arrow vectors the kernels read. */
  public void nonArrowBatch() {
    add(counters.nonArrow());
    fallbackEvent(VarkaFallbackEvent.NON_ARROW_BATCH, kernelIdentity, "");
  }

  /**
   * Counts one kernel batch and says whether the allocation sampler measures it, on the schedule
   * suites can set ({@code VarkaEvaluatorBase.allocationSchedule}).
   */
  public boolean sampleDue(VarkaAllocationSampler.Schedule schedule) {
    kernelBatches++;
    return allocationSampling && schedule.due(kernelBatches);
  }

  /** One allocation sample of the kernel call: evented always, counted and warned when suspect. */
  public void allocationSample(long allocatedBytes, int rows) {
    boolean suspect = VarkaAllocationSampler.suspect(allocatedBytes, rows);
    allocationEvent(kernelIdentity, kernelBatches, rows, allocatedBytes, suspect);
    if (suspect) {
      add(counters.suspectAllocationSamples());
      if (allocationTracker.record(true)) {
        LOG.warn("The Varka SIMD kernels " + kernelIdentity.get() + " allocated " + allocatedBytes
            + " bytes over a " + rows + "-row batch (batch " + kernelBatches + "), and did so on"
            + " the previous sample too. A kernel that runs as emitted allocates nothing per row;"
            + " this rate means the Vector API is boxing its vectors, most likely because two"
            + " vector species of one lane type ran hot in this JVM (SKILLS.md, the"
            + " species-pollution section). Results are still correct; the kernel is several"
            + " times slower than it should be.");
      }
    } else {
      allocationTracker.record(false);
    }
  }

  /**
   * Emits the VARKA-22 fallback JFR event, populated only while a recording has it enabled;
   * {@code exceptionClass} is empty for the non-Arrow and declined causes, which are not errors.
   */
  public static void fallbackEvent(String cause, Supplier<String> kernelIdentity,
      String exceptionClass) {
    var event = new VarkaFallbackEvent();
    if (event.isEnabled()) {
      event.cause = cause;
      event.kernelIdentity = kernelIdentity.get();
      event.exceptionClass = exceptionClass;
      event.commit();
    }
  }

  /** The allocation-sample event; populated only while a recording has it enabled. */
  private static void allocationEvent(Supplier<String> kernelIdentity, long batchIndex, int rows,
      long allocatedBytes, boolean suspect) {
    var event = new VarkaKernelAllocationEvent();
    if (event.isEnabled()) {
      event.kernelIdentity = kernelIdentity.get();
      event.batchIndex = batchIndex;
      event.rows = rows;
      event.allocatedBytes = allocatedBytes;
      event.suspect = suspect;
      event.commit();
    }
  }
}
