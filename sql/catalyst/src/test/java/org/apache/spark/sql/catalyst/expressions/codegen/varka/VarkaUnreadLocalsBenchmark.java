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
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.apache.spark.sql.VarkaBenchmarkBase;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;

/**
 * VARKA-239's admission measurement ({@code VARKA-239.md} 2.2 and 2.3): what the reference locals
 * a loop or epilogue method stores and never reads cost a kernel at run time, measured before the
 * emitter changes by stripping them from the built class ({@code VarkaUnreadLocalsTrim}). The size
 * ladder and the mixed family as {@code VarkaWideKernelBenchmark} runs them, a million rows in
 * 4096-row batches, null-free and with every seventh row null; each kernel as emitted, stripped of
 * every kind, of the segments alone and of the prefix reloads alone at the widest size, and as
 * emitted again last, a control for the order of the cases. Then what one batch allocates, as
 * emitted and stripped, from the JVM's per-thread counter after the timed runs have warmed the
 * kernels.
 *
 * <pre>
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/Test/runMain \
 *     org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaUnreadLocalsBenchmark"
 * </pre>
 */
public class VarkaUnreadLocalsBenchmark extends VarkaBenchmarkBase {

  private static final int NUM_ROWS = 1_000_000;
  private static final int CHUNK = 4096;
  private static final List<String> ALL_KINDS =
      List.of("segment", "vector", "mask", "shared slot", "other");

  /**
   * How every arm is emitted: with the unread locals built, as the emitter built them when this
   * benchmark was written. Elision has been the default since, and a class emitted with it has
   * nothing left for {@code VarkaUnreadLocalsTrim} to strip, so under the defaults "as emitted"
   * and "stripped" would time the same class. Pinned, "as emitted" is the class with the locals
   * and "stripped" the same class with them removed, which is the difference this prices.
   */
  private static final VarkaEmitOptions LOCALS_BUILT =
      VarkaEmitOptions.DEFAULTS.withElideUnreadLocals(false);

  private int nextId = 0;

  /** The arguments a batch passes for one output count: literals and destinations. */
  private record Args(int[] literals, long[] dstData, long[] dstValidity) { }

  /** A family of shapes: a title, its sizes, and its roots at a size. */
  private record Family(String title, List<Integer> sizes, IntFunction<List<VarkaVectorIR>> roots) {
    int widest() {
      return sizes.stream().mapToInt(Integer::intValue).max().orElseThrow();
    }
  }

  /** One arm of a size: its label and the kinds of unread locals it strips. */
  private record Arm(String label, List<String> kinds) { }

  /** A kernel whose allocation is reported after the timed runs. */
  private record Reported(String label, VarkaFusedKernel kernel, int outputs) { }

  /** The column, its batches' pointers and the destinations, made once for the run. */
  private static final class Column {
    final long[][] srcData = new long[NUM_ROWS / CHUNK + 1][];
    final long[][] srcValidity = new long[NUM_ROWS / CHUNK + 1][];
    final int[][] nullCounts;
    final long[] noValidity = {0L};
    final int[] noNulls = {0};
    final MemorySegment[] dsts = new MemorySegment[400];
    final MemorySegment[] dstValidities = new MemorySegment[400];
    // Every argument array a batch passes, made once per output count, so that what a batch
    // allocates is the kernel's alone.
    final Map<Integer, Args> argsByOutputs = new HashMap<>();

    Column(Arena arena) {
      MemorySegment data = arena.allocate(NUM_ROWS * 4L, 8);
      MemorySegment validity = arena.allocate((NUM_ROWS + 7) / 8L, 8);
      for (int i = 0; i < NUM_ROWS; i++) {
        data.setAtIndex(ValueLayout.JAVA_INT, i, (i * 37) % 22000);
        if (i % 7 != 0) {
          long off = i / 8L;
          validity.set(ValueLayout.JAVA_BYTE, off,
              (byte) (validity.get(ValueLayout.JAVA_BYTE, off) | (1 << (i % 8))));
        }
      }
      var batchNulls = new ArrayList<Integer>();
      for (int start = 0; start < NUM_ROWS; start += CHUNK) {
        int count = 0;
        for (int r = start; r < Math.min(start + CHUNK, NUM_ROWS); r++) {
          if (r % 7 == 0) {
            count++;
          }
        }
        batchNulls.add(count);
      }
      for (int k = 0; k < dsts.length; k++) {
        dsts[k] = arena.allocate(CHUNK * 4L, 8);
      }
      for (int k = 0; k < dstValidities.length; k++) {
        dstValidities[k] = arena.allocate(CHUNK / 8L, 8);
      }
      for (int b = 0; b < srcData.length; b++) {
        srcData[b] = new long[] {data.address() + b * CHUNK * 4L};
        srcValidity[b] = new long[] {validity.address() + b * CHUNK / 8L};
      }
      nullCounts = new int[batchNulls.size()][];
      for (int b = 0; b < nullCounts.length; b++) {
        nullCounts[b] = new int[] {batchNulls.get(b)};
      }
    }

    Args args(int outputs) {
      return argsByOutputs.computeIfAbsent(outputs, n -> {
        int[] lits = new int[n];
        long[] dstData = new long[n];
        long[] dstValidity = new long[n];
        for (int i = 0; i < n; i++) {
          lits[i] = i % 12 + 1;
          dstData[i] = dsts[i].address();
          dstValidity[i] = dstValidities[i].address();
        }
        return new Args(lits, dstData, dstValidity);
      });
    }

    /** Batch {@code batch} of the column through {@code k}. */
    void runBatch(VarkaFusedKernel k, Args a, boolean withNulls, int batch) {
      int n = Math.min(CHUNK, NUM_ROWS - batch * CHUNK);
      int status = k.run(srcData[batch], withNulls ? srcValidity[batch] : noValidity,
          withNulls ? nullCounts[batch] : noNulls, a.dstData(), a.dstValidity(), a.literals(), n);
      if (status != 0) {
        throw requirementFailed("the kernel declined a batch: status " + status);
      }
    }

    void scan(VarkaFusedKernel k, int outputs, boolean withNulls) {
      Args a = args(outputs);
      int batch = 0;
      while (batch * CHUNK < NUM_ROWS) {
        runBatch(k, a, withNulls, batch);
        batch += 1;
      }
    }
  }

  private VarkaFusedKernel kernel(
      VarkaGeneratedClassLoader loader, List<VarkaVectorIR> roots, List<String> kinds) {
    nextId += 1;
    String name = "org.apache.spark.sql.varka.execution.VarkaUnreadLocalsBench" + nextId;
    byte[] emitted = VarkaLoopEmitter.emit(name, roots, 1, roots.size(), null, null,
        LOCALS_BUILT);
    byte[] bytes = kinds.isEmpty() ? emitted : VarkaUnreadLocalsTrim.trim(emitted, kinds);
    return defineKernel(loader, name, bytes);
  }

  private static List<VarkaVectorIR> ladder(int n) {
    return roots(n, VarkaEmitCostCorpus::ladderEntry);
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    try (Arena arena = Arena.ofConfined()) {
      var loader = new VarkaGeneratedClassLoader(getClass().getClassLoader());
      var column = new Column(arena);
      var families = List.of(
          new Family("the size ladder", List.of(100, 400), VarkaUnreadLocalsBenchmark::ladder),
          new Family("the mixed family", List.of(100, 200), VarkaGroupingBound::mixed));
      var mx = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
      long tid = Thread.currentThread().threadId();
      for (Family family : families) {
        var reported = new ArrayList<Reported>();
        section(family.title() + ", as emitted and stripped of the locals nothing reads", () -> {
          var benchmark = benchmark(NUM_ROWS + " rows in " + CHUNK + "-row batches", NUM_ROWS)
              .timed().build();
          for (boolean withNulls : new boolean[] {false, true}) {
            for (int n : family.sizes()) {
              String label = withNulls ? "every seventh row null" : "null-free";
              var arms = new ArrayList<Arm>();
              arms.add(new Arm("as emitted", List.of()));
              arms.add(new Arm("stripped", ALL_KINDS));
              if (n == family.widest()) {
                arms.add(new Arm("segments stripped", List.of("segment")));
                arms.add(new Arm("reloads stripped", List.of("vector")));
              }
              arms.add(new Arm("as emitted, again", List.of()));
              for (Arm arm : arms) {
                VarkaFusedKernel k = kernel(loader, family.roots().apply(n), arm.kinds());
                String caseName = n + " entries, " + arm.label() + ", " + label;
                if (n == family.widest()
                    && (arm.label().equals("as emitted") || arm.label().equals("stripped"))) {
                  reported.add(new Reported(caseName, k, n));
                }
                benchmark.addCase(caseName, iteration -> column.scan(k, n, withNulls));
              }
            }
          }
          benchmark.run();
          // What one batch allocates, after the timed runs have compiled the kernels.
          var text = new ArrayList<String>();
          text.add("");
          text.add("Allocation, one 4096-row batch:");
          for (Reported r : reported) {
            boolean withNulls = r.label().endsWith("every seventh row null");
            Args a = column.args(r.outputs());
            for (int i = 0; i < 1000; i++) {
              column.runBatch(r.kernel(), a, withNulls, 0);
            }
            long before = mx.getThreadAllocatedBytes(tid);
            for (int i = 0; i < 1000; i++) {
              column.runBatch(r.kernel(), a, withNulls, 0);
            }
            long after = mx.getThreadAllocatedBytes(tid);
            text.add(String.format("%-60s %10.1f bytes allocated a batch", r.label(),
                (after - before) / 1000.0));
          }
          text.add("");
          String joined = String.join("\n", text);
          if (output().isDefined()) {
            try {
              output().get().write((joined + "\n").getBytes(StandardCharsets.UTF_8));
            } catch (java.io.IOException e) {
              throw new java.io.UncheckedIOException(e);
            }
          }
          System.out.println(joined);
        });
      }
    }
  }
}
