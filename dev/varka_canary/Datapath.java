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
 * How wide the vector execution units actually are, as opposed to how wide the instruction set
 * and {@code MaxVectorSize} say they are. Run as a single-file source program at two widths and
 * compare the rates:
 *
 * <pre>
 * java --add-modules jdk.incubator.vector -XX:MaxVectorSize=32 dev/varka_canary/Datapath.java
 * java --add-modules jdk.incubator.vector -XX:MaxVectorSize=64 dev/varka_canary/Datapath.java
 * </pre>
 *
 * <p><b>What the number means.</b> The loop below is deliberately ALU-throughput-bound: eight
 * independent {@code IntVector} accumulators, register-resident, no loads, no stores, no
 * dependency between iterations of one accumulator that a second accumulator cannot hide. It
 * reports <b>lanes per nanosecond</b> - operations times {@link VectorSpecies#length} - and not
 * operations per second, and that is the whole design. A machine whose units are genuinely 512
 * bits wide retires a 512-bit operation in the same time as a 256-bit one, so doubling the
 * species doubles the lanes per nanosecond. A machine that issues a 512-bit operation as two
 * 256-bit halves takes twice as long for twice the lanes, so the rate is flat. The ratio of the
 * two runs is therefore about 2 on a full-width unit and about 1 on a double-pumped one.
 *
 * <p><b>Why this file exists.</b> Task 62's shell driver and its results files carried a
 * `datapath` line built from {@code Canary.compute} at those two flag settings. That loop is a
 * scalar xorshift over a single {@code long} with a self-dependency - {@code Canary}'s own
 * javadoc calls it "a scalar multiply-add recurrence [...] This is the control" - so
 * {@code MaxVectorSize} cannot affect it and the ratio is 1.00 on every machine ever measured,
 * including hardware that is full width. Eight GitHub runner dispatches on 11 September 2026
 * made that concrete: an AMD EPYC 7763 with no AVX-512 at all, an EPYC 9V74, and an Intel Xeon
 * 6973P-C with {@code UseAVX=3} and {@code MaxVectorSize=64} all read exactly 1.00, as does the
 * development laptop. A probe that cannot distinguish those machines is not measuring the
 * datapath.
 *
 * <p>{@code Canary.compute} is left alone rather than fixed: it is the frequency control
 * {@code dev/varka_bench_canary.sh} compares a machine's state against, and every committed
 * {@code dev/varka_canary/baseline-<host>.txt} is calibrated to it.
 *
 * <p><b>The negative control this ships with.</b> On the development machine (AMD Ryzen AI 9 HX
 * PRO 370, Zen 5 mobile) the answer must be about 1, because {@code SKILLS.md}'s "This machine's
 * AVX-512 is 256 bits wide" establishes that independently, from task 43's op-count ladder where
 * 256 to 512 buys 0.95x. A positive control - a machine where this reads about 2 - is what
 * validates the probe in the other direction, and until one is observed the honest reading of a
 * 1.00 is "this machine is not full width, or this probe cannot tell", not the first alone.
 */
public final class Datapath {

  private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;
  private static final int WINDOWS = 5;
  private static final long WINDOW_NANOS = 1_000_000_000L;
  /** Independent accumulators, to keep the units fed rather than waiting on add latency. */
  private static final int CHAINS = 8;
  /** Operations per call, spread over the chains. */
  private static final int OPS = 1 << 14;

  public static void main(String[] args) {
    // Warm up until C2 has the loop, then measure.
    long sink = 0;
    for (int i = 0; i < 200; i++) {
      sink += laneOps(i);
    }
    double best = 0;
    for (int w = 0; w < WINDOWS; w++) {
      long calls = 0;
      long acc = 0;
      long start = System.nanoTime();
      long now;
      do {
        acc += laneOps((int) calls);
        calls++;
        now = System.nanoTime();
      } while ((now = System.nanoTime()) - start < WINDOW_NANOS);
      // lanes per nanosecond: the quantity that doubles only if the datapath does.
      double rate = calls * (double) OPS * SPECIES.length() / (now - start);
      if (acc == 42) {
        System.out.println();
      }
      best = Math.max(best, rate);
    }
    System.out.printf("preferred_bits=%d%n", SPECIES.vectorBitSize());
    System.out.printf("lanes=%d%n", SPECIES.length());
    System.out.printf("lane_ops_per_ns=%.1f%n", best);
    System.out.printf("sink=%d%n", sink);
  }

  /**
   * {@code OPS} vector adds over {@link #CHAINS} independent accumulators. Everything stays in
   * registers: at 512 bits this is eight {@code zmm} accumulators plus a constant, well inside
   * the 32 the ISA has, so nothing spills and the loop measures issue throughput and nothing
   * else.
   */
  private static long laneOps(int seed) {
    IntVector step = IntVector.broadcast(SPECIES, seed | 1);
    IntVector a0 = IntVector.broadcast(SPECIES, seed);
    IntVector a1 = a0, a2 = a0, a3 = a0, a4 = a0, a5 = a0, a6 = a0, a7 = a0;
    for (int i = 0; i < OPS / CHAINS; i++) {
      a0 = a0.add(step);
      a1 = a1.add(step);
      a2 = a2.add(step);
      a3 = a3.add(step);
      a4 = a4.add(step);
      a5 = a5.add(step);
      a6 = a6.add(step);
      a7 = a7.add(step);
    }
    return a0.add(a1).add(a2).add(a3).add(a4).add(a5).add(a6).add(a7)
        .reduceLanes(jdk.incubator.vector.VectorOperators.ADD);
  }

  private Datapath() {
  }
}
