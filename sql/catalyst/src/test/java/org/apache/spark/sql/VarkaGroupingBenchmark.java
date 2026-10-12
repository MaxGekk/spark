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
import java.util.List;

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
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaGroupingBound;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaLoopEmitter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaMethodNames;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSqlResolve;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR;
import org.apache.spark.sql.types.DataTypes;

/**
 * What it costs to split a projection's outputs into more loop methods.
 *
 * <p>The emitter puts a projection's outputs into groups, one loop method each. Under the
 * budget's rule a group closes when the weights of its nodes - the vector operations each node
 * emits, in {@code VarkaEmitBudget.weightOf} - would pass the group budget of 16, unless the next
 * output reuses what the group already computes (a calendar prefix, or whole nodes under the
 * default {@code shareWholeNodes}), which lets it join up to the fused ceiling of 400. Since
 * VARKA-254 the cost model's prediction bounds a group in the budget's place by default, up to
 * the ceiling; under either rule the measured byte and call-site limits split a group after the
 * build.
 * Each extra loop method costs a call per batch and reloads the input columns it reads; a wider
 * one holds more live vectors, which can spill registers. This benchmark prices that trade at the
 * default budget, with the prediction bounding groups in the budget's place (VARKA-254), and with
 * the budget raised to 400, where the prediction and the measured limits alone bound a group.
 * Each row names its loop-method count, read from the emitted class.
 *
 * <p>Each table has a row per arm and the default timed again last. An arm whose class is byte
 * for byte the default's is labelled a control: the gap between two copies of one class is where
 * the JIT and the allocator put them, and the gap between the two default rows is what position
 * in the run costs. A difference between arms counts only where it clears both, and against the
 * committed band.
 *
 * <p>The shapes. {@code dayofweek} and {@code weekday} over two dates beside four
 * {@code date_add}, which the budget splits into five loop methods since VARKA-254 weighed the
 * two at what they emit, and the prediction keeps in two; forty mixed outputs over two dates and
 * two integers, which the budget splits into sixteen; VARKA-200's mixed family over one date, the
 * one corpus family where the prediction builds more loop methods than the budget; and two
 * controls that build one class under every arm: 24 {@code date_add}, which the cost model's plan
 * splits by call sites, and eight checked sums {@code i + j + k}, whose shared {@code i + j} lets
 * each join its group up to the ceiling. Null-free batches of 1024 rows, which every lane count
 * divides, so the loop methods are what is measured.
 *
 * <p>Committed before VARKA-254 recounted the weights, so that its results show the change as a
 * diff against the rows measured on the code before it.
 *
 * <p>To run this benchmark:
 * <pre>
 *   dev/varka_bench_regen.sh catalyst VarkaGroupingBenchmark
 * </pre>
 */
public class VarkaGroupingBenchmark extends VarkaBenchmarkBase {

  private static final int NUM_ROWS = 10_240_000;
  private static final int CHUNK = 1024;

  private static final Attribute D = VarkaSqlResolve.column("d", DataTypes.DateType);
  private static final Attribute D2 = VarkaSqlResolve.column("d2", DataTypes.DateType);
  private static final Attribute I = VarkaSqlResolve.column("i", DataTypes.IntegerType);
  private static final Attribute J = VarkaSqlResolve.column("j", DataTypes.IntegerType);
  private static final Seq<Attribute> COLUMNS = seq(List.of(D, D2, I, J));

  /** A shape: its title and its outputs as SQL over {@code d}, {@code d2}, {@code i}, {@code j}. */
  private record Shape(String title, List<String> sqls) { }

  /**
   * A shape ready to emit: its outputs, the column each kernel input reads (0 and 1 the dates, 2
   * and 3 the integers) and the literals' values.
   */
  private record Prepared(String title, List<VarkaVectorIR> outputs, int[] inputColumns,
      int[] literals) { }

  /** An arm: its label and the options it emits under. */
  private record Arm(String label, VarkaEmitOptions options) { }

  /**
   * One emitted kernel, with the loop-method count the row's name reports and the class as emitted
   * under one fixed name, so that two arms' classes can be compared byte for byte.
   */
  private record Emitted(VarkaFusedKernel kernel, int loops, byte[] probe) { }

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

  /**
   * Every shape, prepared: the SQL ones compiled, then VARKA-200's mixed family over one date at
   * forty entries ({@code VarkaGroupingBound.mixed}) - a size-ladder entry, a {@code make_date},
   * a cheap tail and a {@code date_add} in rotation, each reading the literal of its index - where
   * the prediction bounding groups builds more loop methods than the budget. Literal {@code k} is
   * {@code k % 28 + 1}, a day every month has, so no {@code make_date} declines.
   */
  private static List<Prepared> prepared() {
    var out = new ArrayList<Prepared>();
    for (var shape : shapes()) {
      var fused = compile(shape);
      out.add(new Prepared(shape.title(), fused.outputs(),
          fused.inputOrdinals().stream().mapToInt(Integer::intValue).toArray(),
          fused.literals().stream().mapToInt(Integer::intValue).toArray()));
    }
    int n = 40;
    int[] literals = new int[n];
    for (int k = 0; k < n; k++) {
      literals[k] = k % 28 + 1;
    }
    out.add(new Prepared("40 mixed families over one date (VarkaGroupingBound)",
        VarkaGroupingBound.mixed(n), new int[] {0}, literals));
    return out;
  }

  /**
   * The default first - the prediction bounding groups in the budget's place, since VARKA-254 -
   * then the group budget binding as it did before ({@code groupBudgetUnderPrediction} on), and
   * the budget raised to 400, kept from the file's first rows so the three stay comparable.
   */
  private static List<Arm> arms() {
    return List.of(
        new Arm("the prediction bounds groups (the default)", VarkaEmitOptions.DEFAULTS),
        new Arm("group budget 16 binds",
            VarkaEmitOptions.DEFAULTS.withGroupBudgetUnderPrediction(true)),
        new Arm("group budget 400",
            VarkaEmitOptions.DEFAULTS.withGroupBudgetUnderPrediction(true).withGroupBudget(400)));
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

  /**
   * Emits the shape under {@code options}: first under one fixed name, which is what two arms'
   * classes are compared by and what the loop methods are counted in, then under the arm's own
   * name, which is what is loaded and timed.
   */
  private static Emitted emit(Prepared shape, VarkaGeneratedClassLoader loader, String id,
      VarkaEmitOptions options) throws ReflectiveOperationException {
    int numInputs = shape.inputColumns().length;
    int numLiterals = shape.literals().length;
    byte[] probe = VarkaLoopEmitter.emit("org.apache.spark.sql.varka.execution.VarkaGroupingProbe",
        shape.outputs(), numInputs, numLiterals, null, null, options);
    // The dense and the masked side group alike; the larger count is the class's, as the
    // grouping suites count it.
    int dense = 0;
    int masked = 0;
    for (String method : VarkaEmitterTestSupport.methodNames(probe)) {
      dense += VarkaMethodNames.isLoop(method, true) ? 1 : 0;
      masked += VarkaMethodNames.isLoop(method, false) ? 1 : 0;
    }
    String name = "org.apache.spark.sql.varka.execution.VarkaGroupingBench" + id;
    byte[] bytes = VarkaLoopEmitter.emit(name, shape.outputs(), numInputs, numLiterals, null,
        null, options);
    loader.defineGeneratedClass(name, bytes);
    var kernel = (VarkaFusedKernel) loader.loadClass(name).getConstructor().newInstance();
    return new Emitted(kernel, Math.max(dense, masked), probe);
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
        var shapes = prepared();
        for (int s = 0; s < shapes.size(); s++) {
          var shape = shapes.get(s);
          int n = shape.outputs().size();
          int numInputs = shape.inputColumns().length;
          long[] src = new long[numInputs];
          for (int c = 0; c < numInputs; c++) {
            src[c] = columns[shape.inputColumns()[c]];
          }
          long[] srcValidity = new long[numInputs];
          int[] srcNullCount = new int[numInputs];
          int[] literals = shape.literals();
          long[] dst = new long[n];
          long[] dstValidity = new long[n];
          for (int o = 0; o < n; o++) {
            dst[o] = arena.allocate(CHUNK * 4L, 64).address();
            dstValidity[o] = arena.allocate(CHUNK / 8L, 64).address();
          }
          var benchmark = benchmark(shape.title() + " over " + NUM_ROWS + " rows", NUM_ROWS)
              .minIterations(5).warmup(Duration.ofSeconds(2)).minTime(Duration.ofSeconds(2))
              .build();
          var armList = arms();
          var emitted = new ArrayList<Emitted>();
          for (int a = 0; a < armList.size(); a++) {
            try {
              emitted.add(emit(shape, loader, s + "_" + a, armList.get(a).options()));
            } catch (ReflectiveOperationException e) {
              throw new IllegalStateException(e);
            }
          }
          // The rows, in order: each arm, then the default again. An arm that builds the
          // default's class is still timed, as a control: its gap from the default is where the
          // JIT and the allocator placed two copies of one class, which every other gap in the
          // table has to clear. The default timed last shows what position in the run costs.
          var order = new ArrayList<Integer>();
          for (int a = 0; a < armList.size(); a++) {
            order.add(a);
          }
          order.add(0);
          for (int k = 0; k < order.size(); k++) {
            int a = order.get(k);
            var arm = armList.get(a);
            var kernel = emitted.get(a).kernel();
            long scratch = VarkaEmitterTestSupport.scratch(kernel, CHUNK);
            Runnable drive = () -> {
              int status = 0;
              for (int done = 0; done < NUM_ROWS; done += CHUNK) {
                status |= kernel.run(src, srcValidity, srcNullCount, dst, dstValidity, literals,
                    CHUNK, scratch);
              }
              blackhole = status;
            };
            drive.run();
            if (blackhole != 0) {
              throw new IllegalStateException(
                  shape.title() + " under " + arm.label() + " declined a batch");
            }
            int count = emitted.get(a).loops();
            String label = arm.label() + ": " + count + " loop method" + (count == 1 ? "" : "s");
            if (k == order.size() - 1) {
              label += ", timed again last";
            } else if (a > 0 && Arrays.equals(emitted.get(a).probe(), emitted.get(0).probe())) {
              label += ", the default's class (a control)";
            }
            benchmark.addCase(label, iteration -> drive.run());
          }
          benchmark.run();
        }
      } finally {
        loader.release();
      }
    });
  }
}
