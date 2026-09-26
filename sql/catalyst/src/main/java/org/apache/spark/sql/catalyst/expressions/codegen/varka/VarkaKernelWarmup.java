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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelWarmth.State;

/**
 * Gets a newly emitted kernel compiled before it serves batches: one JVM-wide daemon thread that
 * runs each queued kernel on a copy of a real batch until HotSpot has compiled it, while the
 * shape's own batches take Spark's row path ({@link VarkaKernelWarmth}).
 *
 * <p><b>Why a kernel needs this.</b> A kernel's methods are called once per batch and loop a few
 * hundred times per call, so a new class reaches none of HotSpot's compile thresholds over a query
 * of ten batches, and until C2 compiles it every Vector API operation is a library call that
 * allocates its result - slower than Spark's own row code. Here the calls are short,
 * {@link #SLICE_ROWS} rows, so the invocation counters the thresholds read advance hundreds of
 * times faster per row than on real batches, and the compile is requested after a few thousand
 * calls rather than after hundreds of batches.
 *
 * <p><b>How it knows the compile landed.</b> By allocation, not by a count. A kernel that is not
 * compiled yet allocates a vector box per operation; a compiled one allocates almost nothing -
 * only the memory segments its driver makes for each column it reads or writes, a few hundred
 * bytes per column per call on a wide kernel, where C2 does not inline every call they are
 * passed to. The warm-up measures its own thread's allocation over a probe block of calls, and a
 * block is clean when it is within {@link #SEGMENT_BYTES_PER_COLUMN} per column per call over the
 * species-pollution check's allowance ({@link VarkaAllocationSampler}) and allocates at most a
 * quarter ({@link #COMPILED_DROP}) of what the first block did; {@link #CLEAN_PROBES} clean blocks
 * in a row are the verdict. The per-column term keeps a wide kernel's segments from reading as
 * boxing, and the drop keeps a narrow kernel's boxing from reading as segments. A count cannot
 * say any of this: crossing a threshold only queues a compile, which lands whenever a compiler
 * thread reaches it. A kernel whose first block is already clean has nothing to wait for - its
 * driver returns before any loop runs, as it does over an all-null input - and is released at
 * once.
 *
 * <p><b>What it runs on.</b> A copy of a real batch of the shape, taken on the task thread before
 * that batch is released: its kernel inputs, tiled to {@link #SNAPSHOT_ROWS} rows. C2 then
 * compiles from the real profile - the same dense or masked driver, the same values, the same
 * guard outcomes - rather than a synthetic batch's. A slice passes its input's null class rather
 * than its own count: an input with some nulls passes one, which keeps the masked driver the real
 * batches take even for a slice whose rows are all valid. That is sound because the kernel tests
 * the count only against zero and the length and otherwise reads the slice's real validity bits.
 * A call runs {@link #SLICE_ROWS} rows plus the batch's own length modulo that, which leaves the
 * batch's remainder past the last whole lane group at every lane count that divides it: the
 * epilogues then run, or return at once, as they do on the real batches, and C2 compiles them
 * from that profile rather than one the warm-up made up.
 *
 * <p><b>What it costs.</b> One thread's CPU while a kernel warms, plus the C2 compiles the kernel
 * needs in any case before it can run fast. After {@link #SPIN_CALLS} calls, past every threshold
 * at its default, the warm-up only probes, every {@link #PACE_MILLIS} milliseconds, while the
 * compile queue works. A warm-up without a verdict after {@link #DEADLINE_SECONDS} seconds
 * releases the shape, and its tasks run the kernel as they would with no warm-up at all.
 */
public final class VarkaKernelWarmup {

  private static final SparkLogger LOG = SparkLoggerFactory.getLogger(VarkaKernelWarmup.class);

  /** Rows of the copied batch; a shorter batch is repeated to fill them. */
  static final int SNAPSHOT_ROWS = 1024;

  /**
   * Rows per call before the batch's remainder: two lane groups at the widest int species, so
   * every loop's back edge is taken in the profile C2 reads, and short enough that a call costs
   * little at interpreted speed.
   */
  static final int SLICE_ROWS = 32;

  /** Slices start on 64-row boundaries, so a slice's validity address is word-aligned. */
  private static final int SLICE_STRIDE = 64;

  private static final int MAX_CALL_ROWS = 2 * SLICE_ROWS - 1;

  private static final int NUM_SLICES = (SNAPSHOT_ROWS - MAX_CALL_ROWS) / SLICE_STRIDE + 1;

  /** Calls between probes while the warm-up spins. */
  private static final int BLOCK_CALLS = 64;

  /** Calls one allocation probe measures. */
  private static final int PROBE_CALLS = 16;

  /** Clean probes in a row that make the verdict. */
  static final int CLEAN_PROBES = 2;

  /**
   * What a compiled kernel may allocate per column it reads or writes, per call: its driver's
   * memory segments, which escape into the calls C2 leaves out of line on a wide kernel. Measured
   * at about 130 bytes a column on a 54-output kernel; a boxing kernel allocates thousands.
   */
  static final int SEGMENT_BYTES_PER_COLUMN = 256;

  /** How far below the first probe block a clean block must be. */
  static final int COMPILED_DROP = 4;

  /**
   * Calls after which the warm-up stops spinning: well past JDK 25's tier-4 invocation threshold
   * (5000 calls) at its default scale, so the compile has been requested and only a busy compile
   * queue stands between the kernel and its verdict.
   */
  static final int SPIN_CALLS = 12000;

  /** The pause between probes once the warm-up has stopped spinning. */
  static final int PACE_MILLIS = 5;

  /**
   * How long a warm-up waits for its verdict before it releases the shape. A hundred-entry
   * kernel is some fifty methods that C2 compiles one after another on a four-core machine's two
   * compiler threads, which takes seconds; the deadline is for a compile that never comes.
   */
  static final int DEADLINE_SECONDS = 60;

  /** Warm-ups queued or running at most; a shape arriving past it is released at once. */
  static final int QUEUE_CAPACITY = 16;

  private static final int RECENT_OUTCOMES = 64;

  private static final ArrayBlockingQueue<Job> QUEUE = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

  // Warm-ups queued or running, for awaitIdle; incremented before a job is offered and decremented
  // after it has finished and freed its copy.
  private static final AtomicInteger PENDING = new AtomicInteger();

  private static final ArrayDeque<Outcome> OUTCOMES = new ArrayDeque<>();

  private static Thread worker;

  private VarkaKernelWarmup() {
  }

  /**
   * One finished warm-up: how it ended, what it ran, how long the job waited in the queue and
   * then ran, and the evidence - what the first probe block allocated, with the kernel surely not
   * compiled yet, against what the last one did. For the cold-start benchmark and diagnostics.
   */
  public record Outcome(String shapeHash, State state, int calls, long rows, long queuedNanos,
      long runNanos, long firstProbeBytes, long lastProbeBytes) {
  }

  /**
   * Copies one batch's kernel inputs and queues a warm-up of {@code kernel} on the copy. Called on
   * the task thread, which must keep the batch alive until this returns; the arrays are the
   * evaluator's argument arrays, already filled for the batch, and are copied rather than kept.
   * Returns false, having released {@code warmth}, when this JVM cannot measure a thread's
   * allocation or the queue is full. A throw leaves {@code warmth} to the caller.
   *
   * @param srcWidths the bytes per row of each input's data, four or eight.
   * @param kernel an instance of the shape's class for the warm-up's own use; kernels keep no
   *        state between calls, so it only has to be a different object from the tasks' ones.
   */
  public static boolean start(VarkaKernelWarmth warmth, String shapeHash, VarkaFusedKernel kernel,
      boolean longLane, long[] srcData, long[] srcValidity, int[] srcNullCount, int[] srcWidths,
      int length, int numOutputs, int[] scalarArgs, long[] longArgs) {
    if (!VarkaAllocationSampler.supported() || length <= 0) {
      warmth.release();
      return false;
    }
    Job job = new Job(warmth, shapeHash, kernel, longLane, srcData, srcValidity, srcNullCount,
        srcWidths, length, numOutputs, scalarArgs, longArgs);
    ensureWorker();
    PENDING.incrementAndGet();
    if (!QUEUE.offer(job)) {
      PENDING.decrementAndGet();
      job.close();
      warmth.release();
      LOG.info("Varka kernel warm-up queue is full; " + VarkaShapeCacheImpl.sourceFileFor(shapeHash)
          + " serves batches without one.");
      return false;
    }
    return true;
  }

  /**
   * Waits until no warm-up is queued or running, or the timeout passes, and says which. For
   * benchmarks and tests that need the JVM quiet, or a shape's verdict, before they go on.
   */
  public static boolean awaitIdle(long timeoutMillis) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    while (PENDING.get() > 0) {
      if (System.nanoTime() - deadline > 0) {
        return false;
      }
      Thread.sleep(5);
    }
    return true;
  }

  /** The most recent warm-ups' outcomes, oldest first. */
  public static List<Outcome> recentOutcomes() {
    synchronized (OUTCOMES) {
      return List.copyOf(OUTCOMES);
    }
  }

  private static synchronized void ensureWorker() {
    if (worker == null) {
      worker = Thread.ofPlatform().daemon().name("varka-kernel-warmup")
          .unstarted(VarkaKernelWarmup::work);
      worker.start();
    }
  }

  private static void work() {
    while (true) {
      Job job;
      try {
        job = QUEUE.take();
      } catch (InterruptedException e) {
        // Nothing interrupts this thread on purpose; keep serving the queue.
        continue;
      }
      try {
        job.run();
      } catch (Throwable t) {
        // Job.run handles the kernel's own failures; this is what escaped it, and a thread that
        // serves every shape in the JVM must outlive one job whatever that was.
        job.warmth.release();
        LOG.warn("Varka kernel warm-up of " + VarkaShapeCacheImpl.sourceFileFor(job.shapeHash)
            + " failed; the shape serves batches without one.", t);
      } finally {
        job.close();
        PENDING.decrementAndGet();
      }
    }
  }

  private static void record(Outcome outcome) {
    synchronized (OUTCOMES) {
      if (OUTCOMES.size() == RECENT_OUTCOMES) {
        OUTCOMES.removeFirst();
      }
      OUTCOMES.addLast(outcome);
    }
  }

  /** One queued warm-up: the copied batch in off-heap memory it owns, and the calls it makes. */
  static final class Job {

    final VarkaKernelWarmth warmth;
    final String shapeHash;
    private final VarkaFusedKernel kernel;
    private final boolean longLane;
    private final Arena arena;
    private final long queuedAt = System.nanoTime();

    // One set of source arguments per slice, built once, so a call does nothing but invoke the
    // kernel; every call runs the same number of rows.
    private final long[][] srcData;
    private final long[][] srcValidity;
    private final int[][] srcNullCount;
    private final int rows;
    private final long[] dstData;
    private final long[] dstValidity;
    private final int[] scalarArgs;
    private final long[] longArgs;
    private final int columns;

    Job(VarkaKernelWarmth warmth, String shapeHash, VarkaFusedKernel kernel, boolean longLane,
        long[] batchData, long[] batchValidity, int[] batchNullCount, int[] widths, int length,
        int numOutputs, int[] scalarArgs, long[] longArgs) {
      this.warmth = warmth;
      this.shapeHash = shapeHash;
      this.kernel = kernel;
      this.longLane = longLane;
      this.scalarArgs = scalarArgs.clone();
      this.longArgs = longArgs.clone();
      this.arena = Arena.ofShared();
      this.columns = batchData.length + numOutputs;
      try {
        int numInputs = batchData.length;
        long[] dataBase = new long[numInputs];
        long[] validityBase = new long[numInputs];
        for (int i = 0; i < numInputs; i++) {
          MemorySegment data =
              arena.allocate((long) (SNAPSHOT_ROWS + SLICE_STRIDE) * widths[i], SLICE_STRIDE);
          tileData(batchData[i], widths[i], length, data);
          dataBase[i] = data.address();
          if (partlyNull(batchNullCount[i], length)) {
            MemorySegment validity = arena.allocate(validityBytes(SNAPSHOT_ROWS), SLICE_STRIDE);
            tileValidity(batchValidity[i], length, validity);
            validityBase[i] = validity.address();
          }
        }
        this.rows = SLICE_ROWS + length % SLICE_ROWS;
        this.srcData = new long[NUM_SLICES][numInputs];
        this.srcValidity = new long[NUM_SLICES][numInputs];
        this.srcNullCount = new int[NUM_SLICES][numInputs];
        for (int v = 0; v < NUM_SLICES; v++) {
          int start = v * SLICE_STRIDE;
          for (int i = 0; i < numInputs; i++) {
            srcData[v][i] = dataBase[i] + (long) start * widths[i];
            if (batchNullCount[i] == 0) {
              srcNullCount[v][i] = 0;
            } else if (batchNullCount[i] >= length) {
              srcNullCount[v][i] = rows;
            } else {
              srcValidity[v][i] = validityBase[i] + start / 8;
              srcNullCount[v][i] = 1;
            }
          }
        }
        this.dstData = new long[numOutputs];
        this.dstValidity = new long[numOutputs];
        for (int o = 0; o < numOutputs; o++) {
          dstData[o] = arena.allocate((long) (MAX_CALL_ROWS + SLICE_STRIDE) * 8, SLICE_STRIDE)
              .address();
          dstValidity[o] = arena.allocate(validityBytes(MAX_CALL_ROWS), SLICE_STRIDE).address();
        }
      } catch (Throwable t) {
        arena.close();
        throw t;
      }
    }

    /** Runs the warm-up to its verdict; see the class doc. */
    void run() {
      long started = System.nanoTime();
      long deadline = started + TimeUnit.SECONDS.toNanos(DEADLINE_SECONDS);
      VarkaKernelWarmupEvent event = new VarkaKernelWarmupEvent();
      event.begin();
      int calls = 0;
      long rowsRun = 0;
      int clean = 0;
      long firstProbeBytes = -1;
      long lastProbeBytes = -1;
      State outcome = State.RELEASED;
      String why = null;
      try {
        while (true) {
          if (warmth.state() != State.WARMING) {
            why = "the shape left the cache";
            break;
          }
          if (calls < SPIN_CALLS) {
            for (int k = 0; k < BLOCK_CALLS; k++) {
              rowsRun += call(calls++);
            }
          } else {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(PACE_MILLIS));
          }
          long before = VarkaAllocationSampler.allocatedBytes();
          int probeRows = 0;
          for (int k = 0; k < PROBE_CALLS; k++) {
            probeRows += call(calls++);
          }
          long allocated = VarkaAllocationSampler.allocatedBytes() - before;
          rowsRun += probeRows;
          lastProbeBytes = allocated;
          long allowance = VarkaAllocationSampler.FIXED_ALLOWANCE_BYTES
              + VarkaAllocationSampler.BYTES_PER_ROW_ALLOWANCE * probeRows
              + (long) PROBE_CALLS * SEGMENT_BYTES_PER_COLUMN * columns;
          if (firstProbeBytes < 0) {
            // The first block runs long before any threshold, so it is the uncompiled rate.
            firstProbeBytes = allocated;
            if (allocated <= allowance) {
              warmth.release();
              why = "it allocates nothing to wait for";
              break;
            }
          } else if (allocated > allowance || allocated * COMPILED_DROP > firstProbeBytes) {
            clean = 0;
          } else if (++clean >= CLEAN_PROBES) {
            if (warmth.markCompiled()) {
              outcome = State.COMPILED;
            } else {
              why = "the shape left the cache";
            }
            break;
          }
          if (System.nanoTime() - deadline > 0) {
            warmth.release();
            why = "no compile after " + DEADLINE_SECONDS + " seconds";
            break;
          }
        }
      } catch (Throwable t) {
        warmth.release();
        LOG.warn("The Varka kernel " + VarkaShapeCacheImpl.sourceFileFor(shapeHash)
            + " failed during its warm-up; the shape serves batches without one.", t);
        if (!(t instanceof Exception) && !(t instanceof LinkageError)) {
          throw t;
        }
        why = "the kernel failed";
      }
      long finished = System.nanoTime();
      record(new Outcome(shapeHash, outcome, calls, rowsRun, started - queuedAt,
          finished - started, firstProbeBytes, lastProbeBytes));
      event.end();
      if (event.shouldCommit()) {
        event.shapeHash = shapeHash;
        event.outcome = outcome.name();
        event.calls = calls;
        event.rows = rowsRun;
        event.commit();
      }
      String kernelName = VarkaShapeCacheImpl.sourceFileFor(shapeHash);
      long millis = TimeUnit.NANOSECONDS.toMillis(finished - started);
      if (outcome == State.COMPILED) {
        LOG.info("Varka kernel " + kernelName + " is compiled after a warm-up of " + millis
            + " ms and " + calls + " calls; its batches run the kernel from now on.");
      } else {
        LOG.info("Varka kernel " + kernelName + " stopped its warm-up after " + millis + " ms and "
            + calls + " calls (" + why + "); its batches run the kernel from now on.");
      }
    }

    private int call(int n) {
      int v = n % NUM_SLICES;
      if (longLane) {
        kernel.run(srcData[v], srcValidity[v], srcNullCount[v], dstData, dstValidity, scalarArgs,
            longArgs, rows);
      } else {
        kernel.run(srcData[v], srcValidity[v], srcNullCount[v], dstData, dstValidity, scalarArgs,
            rows);
      }
      return rows;
    }

    void close() {
      arena.close();
    }
  }

  private static boolean partlyNull(int nullCount, int length) {
    return nullCount > 0 && nullCount < length;
  }

  /** Whole 64-bit words covering {@code rows} bits, plus one spare word. */
  private static long validityBytes(int rows) {
    return ((rows + 63L) / 64 + 1) * 8;
  }

  /** Fills {@code dst}'s first {@link #SNAPSHOT_ROWS} rows with the batch's, repeated. */
  static void tileData(long address, int width, int length, MemorySegment dst) {
    if (address == 0L) {
      return;
    }
    MemorySegment src = MemorySegment.ofAddress(address).reinterpret((long) length * width);
    long done = 0;
    while (done < SNAPSHOT_ROWS) {
      long chunk = Math.min(length, SNAPSHOT_ROWS - done);
      MemorySegment.copy(src, 0, dst, done * width, chunk * width);
      done += chunk;
    }
  }

  /**
   * Sets bit {@code r} of {@code dst} to the batch's bit {@code r % length}, for every snapshot
   * row; {@code dst} arrives zeroed. Bit by bit, because a batch whose length is not a multiple of
   * eight repeats at a bit offset; it runs once per shape over a thousand rows.
   */
  static void tileValidity(long address, int length, MemorySegment dst) {
    MemorySegment src = MemorySegment.ofAddress(address).reinterpret((length + 7) / 8);
    for (int r = 0; r < SNAPSHOT_ROWS; r++) {
      int s = r % length;
      if (((src.get(ValueLayout.JAVA_BYTE, s >>> 3) >>> (s & 7)) & 1) != 0) {
        long b = r >>> 3;
        byte bits = dst.get(ValueLayout.JAVA_BYTE, b);
        dst.set(ValueLayout.JAVA_BYTE, b, (byte) (bits | (1 << (r & 7))));
      }
    }
  }
}
