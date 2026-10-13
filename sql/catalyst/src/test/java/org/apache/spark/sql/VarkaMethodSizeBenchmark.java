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

package org.apache.spark.sql;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import scala.collection.immutable.Seq;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator;
import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitterTestSupport;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaFusedKernel;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaLoopEmitter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSqlResolve;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR;
import org.apache.spark.sql.types.DataTypes;

/**
 * What a kernel costs when one of its methods is too large for the JIT
 * ({@code sql/varka/plans/m6/VARKA-87.md} section 6).
 *
 * <p>HotSpot never compiles a method whose bytecode exceeds 8000 bytes ({@code
 * DontCompileHugeMethods}, on by default, with {@code HugeMethodLimit} fixed at 8000 in a product
 * build). Varka's emitter bounds its loop methods by grouping outputs, and did not bound the
 * epilogue - the one masked pass that handles the rows past the last full lane group - which
 * therefore holds every output of the kernel. A projection of {@code make_date} outputs pushes it
 * past 8000 bytes at thirteen outputs (masked form) and fourteen (dense form), from which point
 * that epilogue runs interpreted for the life of the JVM, and a batch whose length is not a
 * multiple of the lane count pays for it on every call.
 *
 * <p>The ladder here is the plan's: {@code make_date(year(d), month(d), k)} for {@code k} in
 * {@code 1..n}, at 4, 8, 12, 13, 14, 16, 32 and 60 outputs. Two batch lengths, because the cost is
 * a property of the tail: 1024 rows divide by every lane count, so the epilogue returns before any
 * vector work; 1031 leave a tail on every call. Two null arms, because a null-free batch runs the
 * dense driver and its epilogue and only a batch with nulls reaches the masked pair - the one that
 * crosses 8000 bytes first. Every case drives the same total row count through its chunks,
 * reading the same cache-warm prefix, so the columns compare directly and the tail is the only
 * thing that differs between the two lengths of one rung.
 *
 * <p>Two forms, named explicitly so the labels survive the default changing: {@code single
 * epilogue}, the legacy emission ({@code methodByteBudget} 0), and {@code epilogue per group}, the
 * emission under the budget at HugeMethodLimit - a group's methods set up only their group, the
 * epilogue is one method per group, and the class is measured and regrouped ({@code VARKA-87.md}
 * 3.1). The file was first committed with the legacy form alone, so the baseline existed before
 * the change measured against it; both forms are in one table per rung so the columns compare
 * directly.
 *
 * <p>The null-free rows from 12 outputs on are a different cliff, and the file says so rather than
 * hiding it ({@code m6/PLAN.md} section 2.12, VARKA-189). In some JVM forks the second group's
 * dense loop method enters a C2 deoptimization cycle - a new tier-4 compile installed about every
 * 250 milliseconds, the method's own compile time, and made not entrant on first execution at a
 * {@code profile_predicate} trap on the loop's back-edge - and runs interpreted for the fork's
 * life, a hundred times slower at both batch lengths alike; in other forks the same method
 * compiles once. {@code -XX:-UseProfiledLoopPredicate} removes the cycle. The band therefore puts
 * those rows in tier 3, unreadable from a diff, and a committed file shows whichever mode its fork
 * landed in. The masked arm never enters the cycle, so the masked rows carry the epilogue
 * comparison this file exists for.
 *
 * <p>Outputs past the 28th add a year offset - {@code make_date(year(d) + 1, month(d), k)} -
 * because a day literal above 28 is not a valid date in every month and the emitted kernel would
 * decline the batch. The first 28 outputs are exactly the plan's shape; the 32- and 60-output
 * rungs carry one more operation per output beyond it, which is past the cliff either way.
 *
 * <p>To run this benchmark:
 * <pre>
 *   1. build/sbt "catalyst/Test/runMain org.apache.spark.sql.VarkaMethodSizeBenchmark"
 *   2. generate result:
 *        SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt
 *          "catalyst/Test/runMain org.apache.spark.sql.VarkaMethodSizeBenchmark"
 *      Results will be written to "benchmarks/VarkaMethodSizeBenchmark-results.txt".
 *   3. both widths with provenance, the way every committed file here is made:
 *        dev/varka_bench_regen.sh catalyst VarkaMethodSizeBenchmark
 * </pre>
 */
public class VarkaMethodSizeBenchmark extends VarkaBenchmarkBase {

  private static final int NUM_ROWS = 1_000_000;

  /** A batch length and what it is: even, which every lane count divides, or ragged. */
  private record Chunk(int rows, String note) { }

  /** A null arm: whether the batches have nulls, and its label. */
  private record NullArm(boolean mixed, String label) { }

  /** One of the two forms: its label, also the class name's suffix, and its options. */
  private record Form(String label, VarkaEmitOptions options) { }

  /** A form's kernel at one rung. */
  private record Kernel(String label, VarkaFusedKernel kernel) { }

  /**
   * The rungs of the ladder, chosen to straddle the 8000-byte crossing rather than round. {@code
   * -Dvarka.bench.rungs=12,16} runs a subset, which is how one rung is put under a JVM diagnostic
   * flag without waiting for the others; a committed file always runs them all.
   */
  private static final List<Integer> RUNGS = property("varka.bench.rungs")
      .map(list -> Arrays.stream(list.split(",")).map(s -> Integer.parseInt(s.trim())).toList())
      .orElse(List.of(4, 8, 12, 13, 14, 16, 32, 60));

  /**
   * An even length, which every lane count divides, and a ragged one that none does. {@code
   * -Dvarka.bench.chunks=1024} runs one of them alone, so a kernel can be driven at a single
   * length under a diagnostic flag; a committed file always runs both.
   */
  private static final List<Chunk> CHUNKS = chunks();

  /** The largest chunk decides the buffers: every call reads and writes the prefix [0, n). */
  private static final int BUFFER_ROWS = 4096;

  /**
   * The null arms, {@code null-free} and {@code mixed nulls}; {@code
   * -Dvarka.bench.nulls=null-free} runs one alone. The dense and masked bodies are different
   * methods, so one arm under a diagnostic flag says something the pair cannot; a committed file
   * always runs both.
   */
  private static final List<NullArm> NULL_ARMS = nullArms();

  private static final Attribute D = VarkaSqlResolve.column("d", DataTypes.DateType);
  private static final Seq<Attribute> COLUMNS = seq(List.of(D));

  /** The two forms, by label; the label is also the class name's suffix. */
  private static final List<Form> FORMS = List.of(
      new Form("single epilogue", VarkaEmitOptions.DEFAULTS.withMethodByteBudget(0)),
      new Form("epilogue per group", VarkaEmitOptions.DEFAULTS.withMethodByteBudget(
          CodeGenerator.DEFAULT_JVM_HUGE_METHOD_LIMIT())));

  private final Set<String> usedIds = new HashSet<>();

  private static java.util.Optional<String> property(String key) {
    return java.util.Optional.ofNullable(System.getProperty(key)).filter(s -> !s.isEmpty());
  }

  private static List<Chunk> chunks() {
    var all = List.of(new Chunk(1024, "even"), new Chunk(1031, "ragged"));
    return property("varka.bench.chunks").map(list -> {
      Set<Integer> keep = new HashSet<>();
      for (String s : list.split(",")) {
        keep.add(Integer.parseInt(s.trim()));
      }
      return all.stream().filter(c -> keep.contains(c.rows())).toList();
    }).orElse(all);
  }

  private static List<NullArm> nullArms() {
    var all = List.of(new NullArm(false, "null-free"), new NullArm(true, "mixed nulls"));
    return property("varka.bench.nulls")
        .map(name -> all.stream().filter(a -> a.label().equals(name)).toList())
        .orElse(all);
  }

  /** The {@code n}-output projection, compiled the way production compiles it. */
  private static CompiledVarkaProjection shape(int n) {
    var exprs = new ArrayList<NamedExpression>();
    for (int k = 1; k <= n; k++) {
      int day = (k - 1) % 28 + 1;
      int yearOffset = (k - 1) / 28;
      String year = yearOffset == 0 ? "year(d)" : "year(d) + " + yearOffset;
      String sql = "make_date(" + year + ", month(d), " + day + ")";
      exprs.add(VarkaSqlResolve.named(VarkaSqlResolve.parse(sql, COLUMNS), "c" + k));
    }
    return VarkaExpressionCompiler.compile(seq(exprs), COLUMNS)
        .orElseThrow(() -> new IllegalStateException(
            "the " + n + "-output make_date projection did not fuse"));
  }

  private VarkaFusedKernel emit(CompiledVarkaProjection fused, VarkaGeneratedClassLoader loader,
      int n, Form form) {
    String id = n + "_" + form.label().replace(' ', '_');
    // A duplicate id is a LinkageError from the class loader, so it is refused here by name.
    require(usedIds.add(id), "case id " + id + " is already in use by another emit in this "
        + "benchmark");
    String name = "org.apache.spark.sql.varka.execution.VarkaFusedSizeBench" + id;
    var javaRoots = new ArrayList<VarkaVectorIR>(fused.outputs());
    return defineKernel(loader, name, VarkaLoopEmitter.emit(name, javaRoots,
        fused.inputOrdinals().size(), fused.numLiterals(), null, null, form.options()));
  }

  /** A column: its data and its validity bitmap, a set bit for a valid row. */
  private record Column(MemorySegment data, MemorySegment validity) { }

  /**
   * A date column of {@code BUFFER_ROWS} days in [-10000, 10000), every seventh row null when
   * {@code nulls} is set. Every year and month it yields makes a valid date with any day up to 28.
   */
  private static Column fill(Arena arena, boolean nulls) {
    MemorySegment data = arena.allocate(BUFFER_ROWS * 4L, 64);
    MemorySegment validity = arena.allocate(BUFFER_ROWS / 8L, 64);
    validity.fill((byte) 0);
    for (int i = 0; i < BUFFER_ROWS; i++) {
      data.set(ValueLayout.JAVA_INT, i * 4L, i % 20000 - 10000);
      if (!(nulls && i % 7 == 0)) {
        long off = i / 8L;
        byte old = validity.get(ValueLayout.JAVA_BYTE, off);
        validity.set(ValueLayout.JAVA_BYTE, off, (byte) (old | (1 << (i % 8))));
      }
    }
    return new Column(data, validity);
  }

  /** Nulls in the prefix [0, n) of the mixed arm, where rows 0, 7, 14, ... are null. */
  private static int nullsIn(int n) {
    return (n + 6) / 7;
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    var loader = new VarkaGeneratedClassLoader(getClass().getClassLoader());
    try (Arena arena = Arena.ofConfined()) {
      // The null-free arm passes no bitmap (the kernel's dense body reads none), so only its
      // data is used.
      MemorySegment nfData = fill(arena, false).data();
      Column mx = fill(arena, true);
      MemorySegment mxData = mx.data();
      MemorySegment mxValidity = mx.validity();

      section("the epilogue past HugeMethodLimit: a make_date ladder (VARKA-87)", () -> {
        for (int n : RUNGS) {
          CompiledVarkaProjection fused = shape(n);
          var kernels = new ArrayList<Kernel>();
          for (Form form : FORMS) {
            kernels.add(new Kernel(form.label(), emit(fused, loader, n, form)));
          }
          int[] literals = fused.literals().stream().mapToInt(Integer::intValue).toArray();
          // Every output is a date, four bytes a row; the validity segment is sized so a
          // word-writing kernel owns the whole last word at every chunk length.
          long[] dst = new long[n];
          for (int k = 0; k < n; k++) {
            dst[k] = arena.allocate(BUFFER_ROWS * 4L, 64).address();
          }
          long[] dstValidity = new long[n];
          for (int k = 0; k < n; k++) {
            dstValidity[k] = arena.allocate(BUFFER_ROWS / 8L, 64).address();
          }
          var driver = new Driver(nfData, mxData, mxValidity, dst, dstValidity, literals);

          // A guard that trips would send the batch to the row engine in a query; here it
          // would silently time a kernel that did less than the whole batch, so it is refused.
          for (Kernel k : kernels) {
            for (Chunk chunk : CHUNKS) {
              for (NullArm arm : NULL_ARMS) {
                int status = driver.drive(k.kernel(), chunk.rows(), arm.mixed());
                require(status == 0, n + " outputs, " + k.label() + ", at chunk " + chunk.rows()
                    + " (mixed=" + arm.mixed() + ") declined with status " + status);
              }
            }
          }

          var benchmark = benchmark(n + " make_date outputs over " + NUM_ROWS + " rows in chunks",
              NUM_ROWS).timed().build();
          for (Kernel k : kernels) {
            for (Chunk chunk : CHUNKS) {
              for (NullArm arm : NULL_ARMS) {
                benchmark.addCase(
                    k.label() + ", chunk " + chunk.rows() + " (" + chunk.note() + "), "
                        + arm.label(),
                    iteration -> driver.drive(k.kernel(), chunk.rows(), arm.mixed()));
              }
            }
          }
          benchmark.run();
        }
      });
    }
  }

  /** Drives a rung's kernels over the column in chunks: the columns and the rung's buffers. */
  private record Driver(MemorySegment nfData, MemorySegment mxData, MemorySegment mxValidity,
      long[] dst, long[] dstValidity, int[] literals) {

    int drive(VarkaFusedKernel kernel, int chunk, boolean mixed) {
      int status = 0;
      int done = 0;
      long scratch = VarkaEmitterTestSupport.scratch(kernel, chunk);
      while (done < NUM_ROWS) {
        int len = Math.min(chunk, NUM_ROWS - done);
        status |= mixed
            ? kernel.run(new long[] {mxData.address()}, new long[] {mxValidity.address()},
                new int[] {nullsIn(len)}, dst, dstValidity, literals, len, scratch)
            : kernel.run(new long[] {nfData.address()}, new long[] {0L}, new int[] {0},
                dst, dstValidity, literals, len, scratch);
        done += len;
      }
      return status;
    }
  }
}
