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

import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;

import org.apache.arrow.memory.ArrowBuf;

/**
 * A test-only check of every address a kernel maps against the buffers the evaluator handed it.
 *
 * <p>A kernel gets raw addresses, and the size it maps each at is computed inside the kernel from
 * the batch length. The {@link java.lang.foreign.MemorySegment} bounds check enforces that size,
 * not the buffer's: a size that is wrong, or a buffer that is shorter than the kernel assumes (an
 * IPC-read buffer is sliced to its exact length, and Arrow only recommends padding), reads what
 * lies beyond it. Under {@code -Dvarka.sanitizeMemory=true} the evaluator opens a window on the
 * task thread for a batch ({@link #begin}), registers each buffer it hands out with its real
 * capacity ({@link #register}), and every mapping made on that thread ({@link
 * VarkaSegments#map}, and the engine's {@code VarkaVectorSupport.ofAddress}, which reaches
 * {@link #check} through a method handle) must lie inside one of them or fails with a {@link
 * VarkaMemoryViolation} that names the nearest.
 *
 * <p>Off, which is the default and the only mode of a benchmark, every method returns at once on
 * a {@code static final} and the JIT removes it. A thread with no window, the warm-up's thread and
 * a unit test that calls a kernel directly, is not checked. A window is per thread and per batch,
 * so a buffer's address reused by a later batch cannot hide a stale one.
 *
 * <p>{@code -Dvarka.sanitizeMemory.origins=true} also records where each buffer was registered, at
 * the cost of a stack trace each, and attaches it to a violation.
 */
public final class VarkaMemorySanitizer {

  /** The system property that turns the sanitizer on. */
  public static final String PROPERTY = "varka.sanitizeMemory";

  public static final boolean ENABLED = Boolean.getBoolean(PROPERTY);

  private static final boolean ORIGINS = Boolean.getBoolean(PROPERTY + ".origins");

  private static final ThreadLocal<Window> ACTIVE = new ThreadLocal<>();

  private static final LongAdder CHECKED = new LongAdder();

  private VarkaMemorySanitizer() {}

  /** Opens a window on this thread; windows nest, and the outermost one owns the ranges. */
  public static void begin() {
    if (ENABLED) {
      Window window = ACTIVE.get();
      if (window == null) {
        window = new Window();
        ACTIVE.set(window);
      }
      window.depth++;
    }
  }

  /** Closes the window {@link #begin} opened; the outermost close drops its ranges. */
  public static void end() {
    if (ENABLED) {
      Window window = ACTIVE.get();
      if (window != null && --window.depth == 0) {
        ACTIVE.remove();
      }
    }
  }

  /** Registers {@code buf} at its real capacity, as buffer {@code index} of {@code role}. */
  public static void register(String role, int index, ArrowBuf buf) {
    if (ENABLED) {
      register(role, index, buf.memoryAddress(), buf.capacity());
    }
  }

  /** Registers {@code capacity} bytes at {@code address}, buffer {@code index} of {@code role}. */
  public static void register(String role, int index, long address, long capacity) {
    if (ENABLED) {
      Window window = ACTIVE.get();
      if (window != null) {
        window.add(role, index, address, capacity);
      }
    }
  }

  /** Fails with a {@link VarkaMemoryViolation} when the mapping leaves every registered buffer. */
  public static void check(long address, long bytes) {
    if (ENABLED) {
      Window window = ACTIVE.get();
      if (window != null) {
        CHECKED.increment();
        window.check(address, bytes);
      }
    }
  }

  /**
   * How many mappings have been checked in a window since the JVM started: what a test reads to
   * prove the sanitizer was on and reached the code it meant to, since a suite that passes with it
   * silently off says nothing.
   */
  public static long checked() {
    return CHECKED.sum();
  }

  /**
   * The ranges of one batch. Its own type, with no static state, so that a test can drive it
   * without the system property.
   */
  static final class Window {
    int depth;
    private int count;
    private long[] starts = new long[16];
    private long[] capacities = new long[16];
    private String[] roles = new String[16];
    private int[] indexes = new int[16];
    private Throwable[] origins = new Throwable[16];

    void add(String role, int index, long address, long capacity) {
      if (count == starts.length) {
        int grown = count * 2;
        starts = Arrays.copyOf(starts, grown);
        capacities = Arrays.copyOf(capacities, grown);
        roles = Arrays.copyOf(roles, grown);
        indexes = Arrays.copyOf(indexes, grown);
        origins = Arrays.copyOf(origins, grown);
      }
      starts[count] = address;
      capacities[count] = capacity;
      roles[count] = role;
      indexes[count] = index;
      origins[count] = ORIGINS ? new Throwable("registered here") : null;
      count++;
    }

    void check(long address, long bytes) {
      if (bytes < 0) {
        throw violation("a mapping of " + bytes + " bytes", -1);
      }
      if (bytes == 0) {
        return;
      }
      if (address == 0L) {
        throw violation("a mapping of " + bytes + " bytes at the null address", -1);
      }
      long end = address + bytes;
      if (end < address) {
        throw violation("a mapping of " + bytes + " bytes at " + hex(address)
            + " that wraps around the address space", -1);
      }
      int nearest = -1;
      long nearestGap = Long.MAX_VALUE;
      for (int i = 0; i < count; i++) {
        long bufferEnd = starts[i] + capacities[i];
        if (address >= starts[i] && end <= bufferEnd) {
          return;
        }
        long gap = address < starts[i] ? starts[i] - address : Math.max(0L, address - bufferEnd);
        if (gap < nearestGap) {
          nearestGap = gap;
          nearest = i;
        }
      }
      throw violation("a mapping of " + bytes + " bytes at " + hex(address) + ", "
          + hex(end) + " exclusive, leaves every registered buffer", nearest);
    }

    private VarkaMemoryViolation violation(String what, int nearest) {
      var message = new StringBuilder(what);
      if (nearest < 0) {
        message.append("; ").append(count).append(" buffers are registered");
      } else {
        message.append("; the nearest is ").append(roles[nearest]).append(' ')
            .append(indexes[nearest]).append(", ").append(hex(starts[nearest])).append(" for ")
            .append(capacities[nearest]).append(" bytes");
      }
      var violation = new VarkaMemoryViolation(message.toString());
      if (nearest >= 0 && origins[nearest] != null) {
        violation.addSuppressed(origins[nearest]);
      }
      return violation;
    }

    private static String hex(long value) {
      return "0x" + Long.toHexString(value);
    }
  }
}
