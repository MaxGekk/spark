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

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;

/**
 * The machine canary behind {@code dev/varka_bench_canary.sh}: three fixed loops whose rates
 * say what state the machine is in before a benchmark is trusted. Run as a single-file source
 * program, no build needed:
 *
 * <pre>java --add-modules jdk.incubator.vector dev/varka_canary/Canary.java</pre>
 *
 * <ul>
 *   <li>{@code compute}: a scalar multiply-add recurrence. Frequency-bound and touches no
 *       memory, so it moves only if the clock does. This is the control.</li>
 *   <li>{@code cache}: an {@code IntVector} add over three 4 MB arrays - the working set of
 *       the parity benchmark's million-row kernels, which live in L2/L3.</li>
 *   <li>{@code memory}: the same loop over three 96 MB arrays, past every cache.</li>
 * </ul>
 *
 * Why three: the same code on the same machine measured its memory-bound kernels 20-27%
 * apart on different days with every compute-bound control flat (task 54's regeneration
 * against master's committed parity file, and a same-day run of master that reproduced the
 * gap). A control that only checks the clock cannot see that; these two loops can. Each rate
 * is the best of five two-second windows after a warm-up, printed as {@code name=M/s} lines
 * the shell wrapper compares against a committed baseline.
 */
public final class Canary {
  private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;
  private static final int CACHE_ELEMENTS = 1 << 20;   // 4 MB per array
  private static final int MEMORY_ELEMENTS = 24 << 20; // 96 MB per array
  private static final long WINDOW_NANOS = 2_000_000_000L;
  private static final int WINDOWS = 5;

  private Canary() {
  }

  public static void main(String[] args) {
    int[] a = fill(new int[MEMORY_ELEMENTS], 1);
    int[] b = fill(new int[MEMORY_ELEMENTS], 3);
    int[] o = new int[MEMORY_ELEMENTS];
    long sink = 0;
    // Warm-up: enough calls for C2 on all three shapes.
    for (int i = 0; i < 2000; i++) {
      sink += compute(1 << 16, i);
    }
    for (int i = 0; i < 200; i++) {
      sink += vectorAdd(a, b, o, CACHE_ELEMENTS);
    }
    for (int i = 0; i < 8; i++) {
      sink += vectorAdd(a, b, o, MEMORY_ELEMENTS);
    }
    System.out.printf("preferred_bits=%d%n", SPECIES.vectorBitSize());
    long[] seed = {sink};
    System.out.printf("compute=%.1f%n", best(() -> compute(1 << 16, ++seed[0]), 1 << 16));
    System.out.printf("cache=%.1f%n",
        best(() -> vectorAdd(a, b, o, CACHE_ELEMENTS), CACHE_ELEMENTS));
    System.out.printf("memory=%.1f%n",
        best(() -> vectorAdd(a, b, o, MEMORY_ELEMENTS), MEMORY_ELEMENTS));
    System.out.printf("sink=%d%n", sink);
  }

  private interface Body {
    long run();
  }

  /** Best of {@link #WINDOWS} windows, in millions of elements per second. */
  private static double best(Body body, int elementsPerCall) {
    double best = 0;
    for (int w = 0; w < WINDOWS; w++) {
      long calls = 0;
      long sink = 0;
      long start = System.nanoTime();
      long now;
      do {
        sink += body.run();
        calls++;
        now = System.nanoTime();
      } while (now - start < WINDOW_NANOS);
      double rate = calls * (double) elementsPerCall / (now - start) * 1e3;
      if (sink == 42) {
        System.out.println();
      }
      best = Math.max(best, rate);
    }
    return best;
  }

  private static int[] fill(int[] x, int seed) {
    for (int i = 0; i < x.length; i++) {
      x[i] = i * seed;
    }
    return x;
  }

  /**
   * The compute control: an xorshift recurrence from a seed that changes per call. A linear
   * recurrence would not do - C2 reassociates {@code acc * a + c} across its unrolled
   * iterations and the chain shrinks by the unroll factor, which is how the first version of
   * this loop reported 76 billion steps a second. Xorshift is nonlinear, so every step waits
   * on the last and the rate is the clock's.
   */
  private static long compute(int n, long seed) {
    long acc = seed | 1;
    for (int i = 0; i < n; i++) {
      acc ^= acc << 13;
      acc ^= acc >>> 7;
      acc ^= acc << 17;
    }
    return acc;
  }

  private static long vectorAdd(int[] a, int[] b, int[] o, int n) {
    int i = 0;
    for (; i < n; i += SPECIES.length()) {
      IntVector va = IntVector.fromArray(SPECIES, a, i);
      IntVector vb = IntVector.fromArray(SPECIES, b, i);
      va.add(vb).intoArray(o, i);
    }
    return o[n - 1];
  }
}
