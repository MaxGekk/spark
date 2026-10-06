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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

/**
 * A Varka evaluator's task-lifetime scratch: one data and one validity buffer per kernel input
 * the evaluator derives, and the scratch a kernel with a materialized calendar prefix takes
 * (VARKA-198). Every buffer is reused across batches and grown on demand from the task's
 * allocator, and all of them are released before the allocator closes. A derived input's buffers
 * are read only inside {@code kernel.run}, so a batch never sees another batch's fill.
 */
public final class VarkaKernelScratch {

  private final Supplier<BufferAllocator> allocator;
  private final int numInputs;
  private ArrowBuf[] derivedData;
  private ArrowBuf[] derivedValidity;
  private ArrowBuf kernelScratch;

  /**
   * @param allocator the task's allocator, asked on each grow; the evaluator's own
   *                  {@code taskAllocator}, which a suite may override to cap
   */
  public VarkaKernelScratch(Supplier<BufferAllocator> allocator, int numInputs) {
    this.allocator = allocator;
    this.numInputs = numInputs;
  }

  /**
   * The address of {@code bytesPerRow * len} bytes of kernel scratch, grown to the largest
   * batch's need. A kernel without scratch asks for zero bytes per row and is passed a zero
   * address, which its {@code run} ignores.
   */
  public long kernelScratchAddress(int bytesPerRow, int len) {
    if (bytesPerRow == 0 || len <= 0) {
      return 0L;
    }
    long needed = (long) bytesPerRow * len;
    if (kernelScratch == null || kernelScratch.capacity() < needed) {
      // Allocate, store, then release, for the reasons `growSlot` gives.
      ArrowBuf fresh = allocator.get().buffer(needed);
      ArrowBuf old = kernelScratch;
      kernelScratch = fresh;
      if (old != null) {
        old.close();
      }
    }
    return kernelScratch.memoryAddress();
  }

  /** Makes derived input {@code i}'s buffers hold {@code len} rows. */
  public void ensureDerived(int i, int len) {
    if (derivedData == null) {
      derivedData = new ArrowBuf[numInputs];
      derivedValidity = new ArrowBuf[numInputs];
    }
    long dataNeeded = Math.max(len * 4L, 8L);
    long validityNeeded = ((len + 63) / 64) * 8L;
    if (derivedData[i] == null || derivedData[i].capacity() < dataNeeded) {
      growSlot(derivedData, i, dataNeeded);
    }
    if (derivedValidity[i] == null || derivedValidity[i].capacity() < validityNeeded) {
      growSlot(derivedValidity, i, validityNeeded);
    }
  }

  public ArrowBuf derivedData(int i) {
    return derivedData[i];
  }

  public ArrowBuf derivedValidity(int i) {
    return derivedValidity[i];
  }

  /**
   * Replaces {@code slots[i]} with a fresh buffer of {@code needed} bytes: allocate, store, and
   * only then release what was there, so that no step can leave a released buffer referenced.
   *
   * <p>All three parts of that order matter, and each was got wrong in turn. Closing before
   * allocating was a use-after-free: {@code buffer} throws {@code OutOfMemoryException} when the
   * allocator cannot satisfy the request, a plain {@code RuntimeException}, so {@code serveBatch}
   * catches it as a per-batch failure and <i>the task keeps running</i> - with the slot holding a
   * buffer that had already been released, because the assignment that would have replaced it
   * never ran.
   *
   * <p>Storing through the caller narrowed that window without closing it: {@code close()} can
   * throw too - {@code BufferLedger.release} raises on reference-count underflow, and with
   * assertions on it checks the allocator is open - and a throw there again unwinds before the
   * caller's store, stranding the released buffer in the slot and leaking the fresh one. Doing the
   * store here is what makes the release the last thing that can fail, and makes this identical to
   * the filter evaluator's {@code maskBuffer} rather than merely similar to it.
   *
   * <p>Why a stranded slot is worse than it sounds: Arrow's {@code close()} only releases the
   * reference, while {@code capacity()} and {@code memoryAddress()} stay plain field reads it does
   * not touch. So the next, smaller batch finds the stale capacity still large enough, skips the
   * regrow, and has the leaf write through an address the allocator has already freed; the task's
   * cleanup then closes the same buffer a second time and the reference count goes negative.
   */
  private void growSlot(ArrowBuf[] slots, int i, long needed) {
    ArrowBuf fresh = allocator.get().buffer(needed);
    ArrowBuf old = slots[i];
    slots[i] = fresh;
    if (old != null) {
      old.close();
    }
  }

  /**
   * Closes every buffer and drops the arrays.
   *
   * <p>Each slot is cleared before its buffer is closed and each close is guarded on its own, so
   * that one throwing {@code close()} cannot leave the rest unclosed or leave a closed buffer
   * referenced. The arrays go first, so even a failure part-way leaves the evaluator with no
   * scratch rather than with half-released scratch: the next batch reallocates, which is correct
   * if wasteful, where reusing a partly-closed array is not. Whatever a close throws is logged and
   * swallowed; this runs from the task-completion listener, where the allocator close after it
   * matters more than any one buffer, and a throw here would mask the task's real error.
   */
  public void release() {
    ArrowBuf[] data = derivedData;
    ArrowBuf[] validity = derivedValidity;
    derivedData = null;
    derivedValidity = null;
    closeAll(data);
    closeAll(validity);
    ArrowBuf b = kernelScratch;
    kernelScratch = null;
    if (b != null) {
      VarkaBatchLedger.closeQuietly(b, "a Varka kernel scratch buffer");
    }
  }

  private static void closeAll(ArrowBuf[] buffers) {
    if (buffers != null) {
      for (int i = 0; i < buffers.length; i++) {
        ArrowBuf b = buffers[i];
        buffers[i] = null;
        if (b != null) {
          VarkaBatchLedger.closeQuietly(b, "a Varka derived-input scratch buffer");
        }
      }
    }
  }
}
