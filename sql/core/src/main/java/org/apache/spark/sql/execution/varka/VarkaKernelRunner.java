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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseFixedWidthVector;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.IntRangeOps;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.TruncLevelLeaf;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaAllocationSampler;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaDerivedKind;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaFusedKernel;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelWarmth;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaMemorySanitizer;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaMemoryViolation;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaShapeEntry;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.WeekdayLeaf;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * The fused loop serving one task, plus the {@code run} argument arrays, allocated once here and
 * refilled per batch - nothing is allocated per call. The class comes from the shape cache -
 * shared across tasks and released on cache eviction, so its C2 code survives the task boundary -
 * and only the kernel instance and these arrays are the task's own.
 *
 * <p>The plan arrives as plain arrays (the caller converts the compiled plan once), so the
 * per-batch {@link #fill} and {@link #invoke} touch no Scala collection and box nothing.
 */
public final class VarkaKernelRunner {

  /**
   * The decline status the evaluator itself reports when an input lane lies outside a bound the
   * compiler recorded - bit 1, beside the kernels' {@code STATUS_CHRONO_RANGE} (bit 0), so a log
   * line tells the two apart. Never returned by an emitted kernel.
   */
  public static final int STATUS_INPUT_BOUND = 2;

  /**
   * The decline status the evaluator reports when a derived input met a value its row-engine
   * definition raises on under ANSI - an unrecognised weekday name - bit 2. The row engine
   * recomputes the batch and raises where a non-null date sits beside the name.
   */
  public static final int STATUS_DERIVED_INPUT = 4;

  /**
   * A kernel input the compiler bounded: its position among the kernel's inputs and the closed
   * interval every live value must lie in.
   */
  public record Bound(int input, int lo, int hi) {}

  /** Test hooks, read per batch: a kernel failure injected, a decline forced. */
  public interface Hooks {
    boolean failKernel();

    boolean declineKernel();

    VarkaAllocationSampler.Schedule allocationSchedule();
  }

  /** The shape's warm state, shared with every task that runs the shape. */
  public final VarkaKernelWarmth warmth;
  public final String shapeHash;
  public final byte[] classBytes;
  public final LaneType lane;
  public final long[] srcData;
  public final long[] srcValidity;
  public final int[] srcNullCount;
  public final long[] dstData;
  public final long[] dstValidity;
  public final int[] scalarArgs;
  public final long[] longArgs;

  private final VarkaShapeEntry entry;
  private final VarkaFusedKernel kernel;
  /** Bytes of scratch per row the kernel's {@code run} takes; zero for most kernels. */
  private final int scratchBytesPerRow;
  private final int[] inputOrdinals;
  private final VarkaDerivedKind[] derived;
  private final Bound[] bounds;
  private final VarkaKernelScratch scratch;
  private final VarkaFallbackAccounting accounting;
  private final Hooks hooks;

  /**
   * @param derived each kernel input's derivation, or null for a column read as it is
   * @param bounds the kernel inputs the compiler bounded
   */
  public VarkaKernelRunner(VarkaShapeEntry entry, LaneType lane, int[] inputOrdinals,
      VarkaDerivedKind[] derived, Bound[] bounds, int numOutputs, int[] scalarArgs,
      long[] longArgs, VarkaKernelScratch scratch, VarkaFallbackAccounting accounting,
      Hooks hooks) {
    this.entry = entry;
    this.kernel = entry.newKernel();
    this.scratchBytesPerRow = kernel.scratchBytesPerRow();
    this.warmth = entry.warmth();
    this.shapeHash = entry.shapeHash();
    this.classBytes = entry.classBytes();
    this.lane = lane;
    this.inputOrdinals = inputOrdinals;
    this.derived = derived;
    this.bounds = bounds;
    this.srcData = new long[inputOrdinals.length];
    this.srcValidity = new long[inputOrdinals.length];
    this.srcNullCount = new int[inputOrdinals.length];
    this.dstData = new long[numOutputs];
    this.dstValidity = new long[numOutputs];
    this.scalarArgs = scalarArgs;
    this.longArgs = longArgs;
    this.scratch = scratch;
    this.accounting = accounting;
    this.hooks = hooks;
  }

  /** Another instance of the shape's class, which no task runs: the warm-up's own. */
  public VarkaFusedKernel newKernel() {
    return entry.newKernel();
  }

  /**
   * Fills the source-side argument arrays from the input batch - one column per referenced input,
   * in dense kernel-input order. {@code canRun} has vouched for every column this reads: each is an
   * Arrow vector holding exactly the batch's rows, which is what makes the vector's null count the
   * batch's null count, and so what makes the all-null test sound. A column's validity address is
   * zero when every row is null; the kernels never dereference it then.
   *
   * <p>A derived input is computed here, before the kernel runs, into the task's scratch: the
   * string column goes through the row engine's own parser and the kernel reads the int32 result
   * like any other input. The leaves never throw; under ANSI an unrecognised weekday name declines
   * the batch, and the row engine - which parses a name only beside a non-null date - raises its
   * own error where one is due. The trunc level has no such route: an unrecognised format is a
   * null lane in every mode, as it is a NULL result on the row engine.
   *
   * <p>An input the compiler bounded - today a day offset that came from {@code CAST(i AS INTERVAL
   * DAY)}, which Spark's cast throws on past the bound - is checked before the kernel runs, over
   * its live lanes only. A lane outside declines the batch the same way a kernel status does: the
   * row engine recomputes it and raises the error the kernel cannot.
   */
  public void fill(ColumnarBatch input, int len) {
    for (int i = 0; i < inputOrdinals.length; i++) {
      var acv = (ArrowColumnVector) input.column(inputOrdinals[i]);
      VarkaDerivedKind kind = derived[i];
      if (kind == null) {
        var v = (BaseFixedWidthVector) acv.getValueVector();
        if (len != v.getValueCount()) {
          throw new IllegalArgumentException("requirement failed: rowCount " + len
              + " does not match the vector value count " + v.getValueCount());
        }
        int nullCount = v.getNullCount();
        srcData[i] = v.getDataBuffer().memoryAddress();
        srcValidity[i] = nullCount == len ? 0L : v.getValidityBuffer().memoryAddress();
        srcNullCount[i] = nullCount;
        if (VarkaMemorySanitizer.ENABLED) {
          VarkaMemorySanitizer.register("input data", i, v.getDataBuffer());
          if (nullCount != len) {
            VarkaMemorySanitizer.register("input validity", i, v.getValidityBuffer());
          }
        }
      } else {
        scratch.ensureDerived(i, len);
        ArrowBuf data = scratch.derivedData(i);
        ArrowBuf validity = scratch.derivedValidity(i);
        if (VarkaMemorySanitizer.ENABLED) {
          VarkaMemorySanitizer.register("derived data", i, data);
          VarkaMemorySanitizer.register("derived validity", i, validity);
        }
        // No default: a derived kind added to the enum is a compile error here, not a silent
        // trip down the weekday path.
        int nulls = switch (kind) {
          case TRUNC_LEVEL ->
              TruncLevelLeaf.fill(acv, len, data.memoryAddress(), validity.memoryAddress());
          case WEEKDAY, WEEKDAY_ANSI ->
              WeekdayLeaf.fill(acv, len, kind.failOnError, WeekdayLeaf.DEFAULT_PARSER,
                  data.memoryAddress(), validity.memoryAddress());
        };
        if (nulls == WeekdayLeaf.DECLINED) {
          throw new VarkaBatchDeclined(STATUS_DERIVED_INPUT);
        }
        // The leaf writes (len + 7) / 8 validity bytes; the rest of the words the kernel reads is
        // zeroed so a longer earlier batch's bits cannot read as lanes past `len`. The bound is
        // what the scratch was sized to need, not its capacity: the scratch grows and is never
        // shrunk, so zeroing to capacity would memset bytes nothing reads.
        int written = (len + 7) / 8;
        long readable = ((len + 63) / 64) * 8L;
        validity.setZero(written, readable - written);
        srcData[i] = data.memoryAddress();
        srcValidity[i] = nulls == len ? 0L : validity.memoryAddress();
        srcNullCount[i] = nulls;
      }
    }
    for (Bound b : bounds) {
      int k = b.input();
      if (!IntRangeOps.allWithin(srcData[k], srcValidity[k], srcNullCount[k], len, b.lo(),
          b.hi())) {
        throw new VarkaBatchDeclined(STATUS_INPUT_BOUND);
      }
    }
  }

  /**
   * Invokes the emitted loop, marking any catchable throw as {@link VarkaKernelFailure} so the
   * exec nodes' catch can tell a genuine kernel error from a failure in the per-row machinery
   * that shares the same try (VARKA-21 review). A fatal error, and a memory violation of the
   * sanitizer, pass unmarked. A non-zero status means the kernel met a value its lowering is not
   * defined over and declined the batch: the outputs it wrote are not answers, and the batch
   * takes the caller's fallback path, signalled by a throw because that is the one path every
   * caller already routes to the fallback.
   */
  public void invoke(int len) {
    boolean sampled = accounting.sampleDue(hooks.allocationSchedule());
    long before = sampled ? VarkaAllocationSampler.allocatedBytes() : 0L;
    // Grown outside the try below, as the derived inputs' buffers are: an allocator's failure is
    // the per-batch machinery's, not the kernel's, and must not be marked as the kernel's.
    long scratchAddress = scratch.kernelScratchAddress(scratchBytesPerRow, len);
    if (VarkaMemorySanitizer.ENABLED && scratchAddress != 0L) {
      VarkaMemorySanitizer.register("kernel scratch", 0, scratch.kernelScratchBuffer());
    }
    int status;
    try {
      if (hooks.failKernel()) {
        // checkstyle.off: RegexpSinglelineJava
        throw new NoClassDefFoundError("injected Varka kernel failure");
        // checkstyle.on: RegexpSinglelineJava
      }
      // One emitted class is one lane, and each lane has its own `run`: the seven-argument form
      // reads the int literal table, the eight-argument one adds the long table. The plan's lane
      // is fixed at compile time, so this is a branch on a final field. The wrong overload would
      // not run a wrong kernel - each default throws naming the lane - but that throw would be a
      // fallback with a misleading cause.
      if (lane == LaneType.LONG) {
        status = kernel.run(srcData, srcValidity, srcNullCount, dstData, dstValidity, scalarArgs,
            longArgs, len, scratchAddress);
      } else {
        status = kernel.run(srcData, srcValidity, srcNullCount, dstData, dstValidity, scalarArgs,
            len, scratchAddress);
      }
    } catch (Throwable e) {
      if (!isCatchable(e)) {
        throw e;
      }
      throw new VarkaKernelFailure(e);
    }
    if (sampled) {
      accounting.allocationSample(VarkaAllocationSampler.allocatedBytes() - before, len);
    }
    if (status != 0 || hooks.declineKernel()) {
      throw new VarkaBatchDeclined(status != 0 ? status : 1);
    }
  }

  /**
   * A kernel failure worth falling back on, rather than one that has to fail the task: what
   * Scala's {@code NonFatal} admits, and a linkage error, which a kernel class that cannot link
   * raises.
   */
  public static boolean isCatchable(Throwable e) {
    // A memory violation is the sanitizer's finding, never a kernel failure: falling back on it
    // would let the row engine answer for a kernel that read outside its buffers.
    return !(e instanceof VarkaMemoryViolation)
        && (scala.util.control.NonFatal.apply(e) || e instanceof LinkageError);
  }
}
