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

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongToIntFunction;
import java.util.stream.LongStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.BRound;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.Round;
import org.apache.spark.sql.catalyst.expressions.Truncate;
import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitterFixtures.Output;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.SQLConf$;
import org.apache.spark.sql.types.DataTypes;

/**
 * The integral rounding of the rounding family (VARKA-344) through its emitted kernels against
 * Spark's own {@code Round}, {@code BRound} and {@code Truncate}: every {@code INT} through each
 * function at each scale from -1 to -10, LEGACY's value and ANSI's raise ({@code VARKA-344.md}
 * 3.5), behind {@code -Dvarka.sweep=true}; a strided slice of the ints, the windows' ends and the
 * extremes among them, on every build. Under ANSI a chunk the kernel answers holds no int Spark
 * raises on, and a chunk holding one is declined.
 */
@VarkaSuite(ownJvm = true)
public final class VarkaEmitterRoundingSuite {

  private static final int CHUNK = 4096;
  private static final long SLICE_ROWS = 1L << 20;
  private static final List<String> FUNCTIONS = List.of("round", "bround", "truncate");
  private static final List<Attribute> COLS =
      List.of(VarkaSqlResolve.column("i", DataTypes.IntegerType));

  /** The ints every slice starts with: the extremes and the ends of ANSI's windows at -1. */
  private static final int[] EDGES = {Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1, 1, 5, -5,
      2147483644, -2147483644, 2147483645, -2147483645, 2147483646, -2147483646, 1999999999,
      -1999999999, 2147483640, -2147483640};

  private static boolean sweepsEnabled() {
    return "true".equals(System.getProperty("varka.sweep"));
  }

  /** The slice's row {@code r}: the edges, then a stride whose every byte varies. */
  private static int slice(long r) {
    return r < EDGES.length ? EDGES[(int) r] : (int) (r * 0x9E3779B1L);
  }

  private static Expression spark(String fn, int scale, boolean ansi) {
    var child = new BoundReference(0, DataTypes.IntegerType, false);
    var s = Literal$.MODULE$.apply(scale);
    return switch (fn) {
      case "round" -> new Round(child, s, ansi);
      case "bround" -> new BRound(child, s, ansi);
      case "truncate" -> new Truncate(child, s, ansi);
      default -> throw new IllegalArgumentException(fn);
    };
  }

  private static CompiledVarkaProjection compile(String sql, boolean ansi) {
    SQLConf conf = SQLConf.get().clone();
    conf.setConfString(SQLConf.ANSI_ENABLED().key(), Boolean.toString(ansi));
    Expression e = SQLConf$.MODULE$.withExistingConf(conf,
        () -> VarkaSqlResolve.parse(sql, CollectionConverters.asScala(COLS).toSeq()));
    scala.collection.immutable.Seq<NamedExpression> entry =
        CollectionConverters.asScala(List.of(VarkaSqlResolve.named(e, "c"))).toSeq();
    var cols = CollectionConverters.asScala(COLS).toSeq();
    Optional<CompiledVarkaProjection> plan =
        VarkaExpressionCompiler.compile(entry, cols, VarkaMatrixRuns.BASE);
    if (plan.isEmpty()) {
      fail(sql + " declines: " + VarkaExpressionCompiler.declines(entry, cols,
          VarkaMatrixRuns.BASE));
    }
    return plan.get();
  }

  /**
   * {@code fn(i, scale)} over {@code rows} ints, {@code input(r)} the int of row {@code r}, in
   * chunks run in parallel, each worker on a kernel of its own; the first difference fails.
   */
  private static void sweep(String fn, int scale, boolean ansi, long rows,
      LongToIntFunction input) {
    String sql = fn + "(i, " + scale + ")";
    String ctx = sql + (ansi ? " under ANSI" : " under LEGACY");
    CompiledVarkaProjection plan = compile(sql, ansi);
    Expression expr = spark(fn, scale, ansi);
    var emitted = VarkaEmitterFixtures.emit(plan.outputs(), plan.inputOrdinals().size(),
        plan.numLiterals(), VarkaMatrixRuns.BASE);
    int[] lits = plan.literals().stream().mapToInt(Integer::intValue).toArray();
    var loaders = new ConcurrentLinkedQueue<VarkaGeneratedClassLoader>();
    var kernels = ThreadLocal.withInitial(() -> {
      var loaded = VarkaEmitterFixtures.load(emitted);
      loaders.add(loaded.loader());
      return loaded.kernel();
    });
    var failure = new AtomicReference<String>();
    long chunks = (rows + CHUNK - 1) / CHUNK;
    try {
      LongStream.range(0, chunks).parallel().forEach(k -> {
        if (failure.get() != null) {
          return;
        }
        long first = k * CHUNK;
        int n = (int) Math.min(CHUNK, rows - first);
        var row = new GenericInternalRow(new Object[1]);
        int[] want = new int[n];
        boolean raises = false;
        for (int i = 0; i < n; i++) {
          row.update(0, input.applyAsInt(first + i));
          try {
            want[i] = (Integer) expr.eval(row);
          } catch (ArithmeticException e) {
            raises = true;
          }
        }
        var kernel = kernels.get();
        try (Arena arena = Arena.ofConfined()) {
          MemorySegment data = VarkaEmitterFixtures.alloc(arena, n * 4L);
          for (int i = 0; i < n; i++) {
            data.set(ValueLayout.JAVA_INT, i * 4L, input.applyAsInt(first + i));
          }
          Output out = VarkaEmitterFixtures.makeOutput(arena, n, -1L);
          int status = kernel.run(new long[] {data.address()}, new long[] {0L}, new int[] {0},
              new long[] {out.data().address()}, new long[] {out.validity().address()}, lits, n,
              VarkaEmitterTestSupport.scratch(kernel, n));
          if (raises) {
            if (status == 0) {
              failure.compareAndSet(null, ctx + ": the chunk at row " + first
                  + " holds an int Spark raises on and was answered");
            }
            return;
          }
          if (status != 0) {
            failure.compareAndSet(null, ctx + ": status " + status + " at row " + first
                + ", where Spark answers every int");
            return;
          }
          for (int i = 0; i < n; i++) {
            int got = out.data().get(ValueLayout.JAVA_INT, i * 4L);
            if (got != want[i]) {
              failure.compareAndSet(null, ctx + ": over " + input.applyAsInt(first + i)
                  + " the kernel gave " + got + " and Spark " + want[i]);
              return;
            }
          }
        }
      });
    } finally {
      loaders.forEach(VarkaGeneratedClassLoader::release);
    }
    if (failure.get() != null) {
      fail(failure.get());
    }
  }

  private static void sweepAll(long rows, LongToIntFunction input) {
    for (String fn : FUNCTIONS) {
      for (int k = 1; k <= 10; k++) {
        sweep(fn, -k, false, rows, input);
        sweep(fn, -k, true, rows, input);
      }
    }
  }

  @Test
  @DisplayName("round, bround and truncate of an int at scales -1 to -10 match Spark over a "
      + "strided slice of the ints, in LEGACY and ANSI")
  void slice() {
    sweepAll(SLICE_ROWS, VarkaEmitterRoundingSuite::slice);
  }

  @Test
  @DisplayName("round, bround and truncate of an int at scales -1 to -10 match Spark over every "
      + "int, in LEGACY and ANSI (opt-in: -Dvarka.sweep=true; VARKA-344)")
  void everyInt() {
    assumeTrue(sweepsEnabled(), "set -Dvarka.sweep=true to sweep the emitted kernels");
    sweepAll(1L << 32, r -> (int) r);
  }
}
