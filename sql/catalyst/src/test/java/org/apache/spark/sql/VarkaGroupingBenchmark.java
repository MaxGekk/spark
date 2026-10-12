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
import java.lang.foreign.ValueLayout;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitterTestSupport;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaFusedKernel;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaLoopEmitter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaMethodNames;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSqlResolve;
import org.apache.spark.sql.types.DataTypes;

/**
 * What it costs to split a projection's outputs into more loop methods, on the shapes where the
 * grouping's first rule decides the split.
 *
 * <p>The emitter puts a projection's outputs into groups, one loop method each. A group closes
 * when the weights of its nodes - a hand-kept register of the vector operations each node emits,
 * in {@code VarkaEmitBudget.weightOf} - would pass the group budget of 16, unless the next output
 * reuses what the group already computes (a calendar prefix, or whole nodes under the default
 * {@code shareWholeNodes}), which lets it join up to the fused ceiling of 400. The cost model's
 * prediction and the measured byte and call-site limits can close a group earlier, never later.
 * Each extra loop method costs a call per batch and reloads the input columns it reads; a wider
 * one holds more live vectors, which can spill registers. This benchmark prices that trade on
 * four shapes, at the default budget and with the budget raised to 400, where the prediction and
 * the measured limits alone bound a group. Each row names its loop-method count, read from the
 * emitted class, and arms that build the same class share one row.
 *
 * <p>The shapes. {@code dayofweek} and {@code weekday} over two dates beside cheap date outputs,
 * whose weights (1 each) are far under what they emit (about 16), and a mixed projection of forty
 * outputs over two dates and two integers: the two where the weights and the budget decide the
 * split. And two controls, which the budget does not split today: 24 {@code date_add} outputs,
 * which the cost model's plan splits by call sites before the budget would, and eight checked
 * sums {@code i + j + k}, whose shared {@code i + j} lets each join its group up to the fused
 * ceiling. Null-free batches of 1024 rows, which every lane count divides, so the loop methods are
 * what is measured.
 *
 * <p>Committed before VARKA-254 recounts the weights, so that its results file shows the change
 * as a diff against these rows.
 *
 * <p>To run this benchmark:
 * <pre>
 *   dev/varka_bench_regen.sh catalyst VarkaGroupingBenchmark
 * </pre>
 */
public class VarkaGroupingBenchmark extends VarkaBenchmarkBase {

  private static final int NUM_ROWS = 10_000_000;
  private static final int CHUNK = 1024;

  private static final Attribute D = VarkaSqlResolve.column("d", DataTypes.DateType);
  private static final Attribute D2 = VarkaSqlResolve.column("d2", DataTypes.DateType);
  private static final Attribute I = VarkaSqlResolve.column("i", DataTypes.IntegerType);
  private static final Attribute J = VarkaSqlResolve.column("j", DataTypes.IntegerType);
  private static final Seq<Attribute> COLUMNS = seq(List.of(D, D2, I, J));

  /** A shape: its title and its outputs as SQL over {@code d}, {@code d2}, {@code i}, {@code j}. */
  private record Shape(String title, List<String> sqls) { }

  /** An arm: its label and the options it emits under. */
  private record Arm(String label, VarkaEmitOptions options) { }

  /**
   * One emitted kernel, with the loop-method count the row's name reports and the class emitted
   * again under one fixed name, so that two arms' classes can be compared byte for byte.
   */
  private record Emitted(VarkaFusedKernel kernel, int loops, byte[] probe) { }

  /** One timed row: the labels of the arms that build its class, and the kernel they build. */
  private record Row(List<String> labels, Emitted emitted) { }

  private final Set<String> usedIds = new HashSet<>();
  private volatile int blackhole;

  private static <T> Seq<T> seq(List<T> list) {
    return CollectionConverters.asScala(list).toSeq();
  }

  private static List<Shape> shapes() {
    var weekdays = new ArrayList<String>(
        List.of("dayofweek(d)", "weekday(d)", "dayofweek(d2)", "weekday(d2)"));
    for (int k = 1; k <= 4; k++) {
      weekdays.add("date_add(d, " + k + ")");
    }
    var dateAdds = new ArrayList<String>();
    for (int k = 1; k <= 24; k++) {
      dateAdds.add("date_add(d, " + k + ")");
    }
    var sums = new ArrayList<String>();
    for (int k = 1; k <= 8; k++) {
      sums.add("i + j + " + k);
    }
    var mixed = new ArrayList<String>();
    for (int k = 1; k <= 40; k++) {
      mixed.add(switch (k % 5) {
        case 0 -> "dayofweek(d" + (k % 2 == 0 ? "" : "2") + ")";
        case 1 -> "date_add(d, " + k + ")";
        case 2 -> "i + j + " + k;
        case 3 -> "IF(d < d2, date_add(d2, " + k + "), d)";
        default -> "datediff(d, d2) + " + k;
      });
    }
    return List.of(
        new Shape("dayofweek and weekday over two dates, beside four date_add", weekdays),
        new Shape("24 date_add outputs over one date", dateAdds),
        new Shape("8 checked sums i + j + k", sums),
        new Shape("40 mixed outputs over two dates and two integers", mixed));
  }

  private static List<Arm> arms() {
    return List.of(
        new Arm("group budget 16 (the default)", VarkaEmitOptions.DEFAULTS),
        new Arm("group budget 400", VarkaEmitOptions.DEFAULTS.withGroupBudget(400)));
  }

  private static CompiledVarkaProjection compile(Shape shape) {
    var named = new ArrayList<NamedExpression>();
    for (int k = 0; k < shape.sqls().size(); k++) {
      named.add(VarkaSqlResolve.named(VarkaSqlResolve.parse(shape.sqls().get(k), COLUMNS),
          "c" + k));
    }
    var fused = VarkaExpressionCompiler.compile(seq(named), COLUMNS).orElseThrow(
        () -> new IllegalStateException("the projection did not fuse: " + shape.title()));
    if (!fused.derivedInputs().isEmpty() || !fused.longLiterals().isEmpty()) {
      throw new IllegalStateException(
          "the projection is not a plain 32-bit-lane kernel: " + shape.title());
    }
    return fused;
  }

  private Emitted emit(CompiledVarkaProjection fused, VarkaGeneratedClassLoader loader, String id,
      VarkaEmitOptions options) throws ReflectiveOperationException {
    if (!usedIds.add(id)) {
      throw new IllegalStateException("case id " + id + " is already in use");
    }
    String name = "org.apache.spark.sql.varka.execution.VarkaGroupingBench" + id;
    byte[] bytes = VarkaLoopEmitter.emit(name, fused.outputs(), fused.inputOrdinals().size(),
        fused.numLiterals(), null, null, options);
    int loops = 0;
    for (String method : VarkaEmitterTestSupport.methodNames(bytes)) {
      if (VarkaMethodNames.isLoop(method, true)) {
        loops++;
      }
    }
    byte[] probe = VarkaLoopEmitter.emit("org.apache.spark.sql.varka.execution.VarkaGroupingProbe",
        fused.outputs(), fused.inputOrdinals().size(), fused.numLiterals(), null, null, options);
    loader.defineGeneratedClass(name, bytes);
    var kernel = (VarkaFusedKernel) loader.loadClass(name).getConstructor().newInstance();
    return new Emitted(kernel, loops, probe);
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    section("how many loop methods a projection's outputs are split into", () -> {
      var loader = new VarkaGeneratedClassLoader(getClass().getClassLoader());
      try (var arena = Arena.ofConfined()) {
        var dates = arena.allocate(CHUNK * 4L, 64);
        var dates2 = arena.allocate(CHUNK * 4L, 64);
        var ints = arena.allocate(CHUNK * 4L, 64);
        var ints2 = arena.allocate(CHUNK * 4L, 64);
        for (int r = 0; r < CHUNK; r++) {
          dates.set(ValueLayout.JAVA_INT, r * 4L, r * 7 % 20000 - 10000);
          dates2.set(ValueLayout.JAVA_INT, r * 4L, r * 13 % 20000 - 10000);
          // Small enough that no checked sum overflows, so no batch declines.
          ints.set(ValueLayout.JAVA_INT, r * 4L, r % 1000 - 500);
          ints2.set(ValueLayout.JAVA_INT, r * 4L, r * 3 % 1000 - 500);
        }
        long[] columns = {dates.address(), dates2.address(), ints.address(), ints2.address()};
        var shapes = shapes();
        for (int s = 0; s < shapes.size(); s++) {
          var shape = shapes.get(s);
          var fused = compile(shape);
          int n = fused.outputs().size();
          int numInputs = fused.inputOrdinals().size();
          long[] src = new long[numInputs];
          for (int c = 0; c < numInputs; c++) {
            src[c] = columns[fused.inputOrdinals().get(c)];
          }
          long[] srcValidity = new long[numInputs];
          int[] srcNullCount = new int[numInputs];
          int[] literals = fused.literals().stream().mapToInt(Integer::intValue).toArray();
          long[] dst = new long[n];
          long[] dstValidity = new long[n];
          for (int o = 0; o < n; o++) {
            dst[o] = arena.allocate(CHUNK * 4L, 64).address();
            dstValidity[o] = arena.allocate(CHUNK / 8L, 64).address();
          }
          var benchmark = benchmark(shape.title() + " over " + NUM_ROWS + " rows", NUM_ROWS)
              .minIterations(5).warmup(Duration.ofSeconds(2)).minTime(Duration.ofSeconds(2))
              .build();
          // An arm that builds the class an earlier arm built is not timed again: its row would
          // measure the same bytes, and two timings of one class differ by where the JIT and the
          // allocator happen to put it - up to 17% on this file's first run, enough to read as an
          // effect of the option. Its label joins the earlier arm's row instead.
          var rows = new ArrayList<Row>();
          var armList = arms();
          for (int a = 0; a < armList.size(); a++) {
            var arm = armList.get(a);
            Emitted emitted;
            try {
              emitted = emit(fused, loader, s + "_" + a, arm.options());
            } catch (ReflectiveOperationException e) {
              throw new IllegalStateException(e);
            }
            Row same = null;
            for (var row : rows) {
              if (Arrays.equals(row.emitted().probe(), emitted.probe())) {
                same = row;
              }
            }
            if (same != null) {
              same.labels().add(arm.label());
            } else {
              rows.add(new Row(new ArrayList<>(List.of(arm.label())), emitted));
            }
          }
          for (var row : rows) {
            var kernel = row.emitted().kernel();
            long scratch = VarkaEmitterTestSupport.scratch(kernel, CHUNK);
            Runnable drive = () -> {
              int status = 0;
              for (int done = 0; done < NUM_ROWS; done += CHUNK) {
                status |= kernel.run(src, srcValidity, srcNullCount, dst, dstValidity, literals,
                    Math.min(CHUNK, NUM_ROWS - done), scratch);
              }
              blackhole = status;
            };
            String labels = String.join(" and ", row.labels());
            drive.run();
            if (blackhole != 0) {
              throw new IllegalStateException(shape.title() + " under " + labels
                  + " declined a batch");
            }
            int count = row.emitted().loops();
            String loops = count + " loop method" + (count == 1 ? "" : "s");
            String same = row.labels().size() > 1 ? ", one class" : "";
            benchmark.addCase(labels + ": " + loops + same, iteration -> drive.run());
          }
          benchmark.run();
        }
      } finally {
        loader.release();
      }
    });
  }
}
