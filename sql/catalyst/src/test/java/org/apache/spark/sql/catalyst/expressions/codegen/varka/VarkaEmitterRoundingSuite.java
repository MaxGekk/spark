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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * The integral rounding of the rounding family (VARKA-344) through its emitted kernels against
 * Spark's own {@code Round}, {@code BRound} and {@code Truncate}: every {@code INT} through each
 * function at each scale from -1 to -10, LEGACY's value and ANSI's raise ({@code VARKA-344.md}
 * 3.5), behind {@code -Dvarka.sweep=true}; a strided slice of the ints, the windows' ends and the
 * extremes among them, on every build. Under ANSI a chunk the kernel answers holds no int Spark
 * raises on, and a chunk holding one is declined.
 *
 * <p>A {@code BIGINT} at each scale from -1 to -20, through the pre-shift's window
 * ({@code VarkaRoundingCompiler.preShiftForm}): the longs near the window's edge, near
 * {@code 2^52}, {@code 2^55}, {@code 5 * 10^18} and the extremes, the ties beside each, and a
 * seeded draw over every bit length, answered exactly where the long lies inside the window and
 * Spark answers, and declined everywhere else, so the window is asserted, not only the answers.
 */
@VarkaSuite(ownJvm = true)
public final class VarkaEmitterRoundingSuite {

  private static final int CHUNK = 4096;
  private static final long SLICE_ROWS = 1L << 20;
  private static final List<String> FUNCTIONS = List.of("round", "bround", "truncate");
  private static final List<Attribute> COLS =
      List.of(VarkaSqlResolve.column("i", DataTypes.IntegerType));
  private static final List<Attribute> LONG_COLS =
      List.of(VarkaSqlResolve.column("l", DataTypes.LongType));

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
    return spark(fn, scale, ansi, DataTypes.IntegerType);
  }

  private static Expression spark(String fn, int scale, boolean ansi, DataType type) {
    var child = new BoundReference(0, type, false);
    var s = Literal$.MODULE$.apply(scale);
    return switch (fn) {
      case "round" -> new Round(child, s, ansi);
      case "bround" -> new BRound(child, s, ansi);
      case "truncate" -> new Truncate(child, s, ansi);
      default -> throw new IllegalArgumentException(fn);
    };
  }

  private static CompiledVarkaProjection compile(String sql, boolean ansi) {
    return compile(sql, ansi, COLS);
  }

  private static CompiledVarkaProjection compile(String sql, boolean ansi,
      List<Attribute> columns) {
    SQLConf conf = SQLConf.get().clone();
    conf.setConfString(SQLConf.ANSI_ENABLED().key(), Boolean.toString(ansi));
    Expression e = SQLConf$.MODULE$.withExistingConf(conf,
        () -> VarkaSqlResolve.parse(sql, CollectionConverters.asScala(columns).toSeq()));
    scala.collection.immutable.Seq<NamedExpression> entry =
        CollectionConverters.asScala(List.of(VarkaSqlResolve.named(e, "c"))).toSeq();
    var cols = CollectionConverters.asScala(columns).toSeq();
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

  // -----------------------------------------------------------------------------------------
  // BIGINT: the pre-shift's window.
  // -----------------------------------------------------------------------------------------

  /**
   * Whether the pre-shift at scale {@code -k} admits {@code v}, restated from the plan rather
   * than read from the compiler: {@code |v| < 2^(52 + k)} up to {@code k = 10}, every long but
   * {@code Long.MIN_VALUE} at {@code k = 11}, and every long from {@code k = 12} (where the
   * scales past 18 have no division at all).
   */
  private static boolean inWindow(long v, int k) {
    if (k >= 12) {
      return true;
    }
    if (k == 11) {
      return v != Long.MIN_VALUE;
    }
    return v != Long.MIN_VALUE && Math.abs(v) < 1L << (52 + k);
  }

  /** The longs that probe scale {@code -k}: the window's edges and the named points, and ties. */
  private static List<Long> longCells(int k) {
    var points = new LinkedHashSet<Long>();
    BigInteger d = BigInteger.TEN.pow(k);
    long edge = k <= 10 ? 1L << (52 + k) : Long.MAX_VALUE;
    for (long c : new long[] {0, 1, edge - 1, edge, 1L << 52, 1L << 55,
        5_000_000_000_000_000_000L, Long.MAX_VALUE}) {
      for (long off = -2; off <= 2; off++) {
        long v = c + off;
        if ((off > 0 && v < c) || (off < 0 && v > c)) {
          continue;
        }
        points.add(v);
        points.add(-v);
        // The tie nearest v, and its neighbours: the multiple of 10^k at or below |v| plus half.
        if (k <= 18) {
          BigInteger m = BigInteger.valueOf(v).abs().divide(d).multiply(d)
              .add(d.shiftRight(1));
          for (int t = -1; t <= 1; t++) {
            BigInteger tie = m.add(BigInteger.valueOf(t));
            if (tie.bitLength() < 64) {
              points.add(tie.longValue());
              points.add(-tie.longValue());
            }
          }
        }
      }
    }
    points.add(Long.MIN_VALUE);
    points.add(Long.MIN_VALUE + 1);
    return List.copyOf(points);
  }

  /** One long chunk through {@code kernel}: the status, and the answers into {@code got}. */
  private static int runLong(VarkaFusedKernel kernel, int[] ints, long[] longs, long[] in,
      long[] got) {
    int n = in.length;
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment data = VarkaEmitterFixtures.alloc(arena, n * 8L);
      for (int i = 0; i < n; i++) {
        data.set(ValueLayout.JAVA_LONG, i * 8L, in[i]);
      }
      Output out = VarkaEmitterFixtures.makeLongOutput(arena, n);
      int status = kernel.run(new long[] {data.address()}, new long[] {0L}, new int[] {0},
          new long[] {out.data().address()}, new long[] {out.validity().address()}, ints, longs,
          n, VarkaEmitterTestSupport.scratch(kernel, n));
      for (int i = 0; i < n; i++) {
        got[i] = out.data().get(ValueLayout.JAVA_LONG, i * 8L);
      }
      return status;
    }
  }

  /**
   * {@code fn(l, -k)} in one mode: every cell inside the window that Spark answers, with a
   * seeded draw inside the window beside them, in one chunk the kernel must answer exactly; and
   * every other cell in a chunk of its own beside two answerable longs, which the kernel must
   * decline.
   */
  private static void checkLong(String fn, int k, boolean ansi, Random rnd) {
    String sql = fn + "(l, " + (-k) + ")";
    String ctx = sql + (ansi ? " under ANSI" : " under LEGACY");
    CompiledVarkaProjection plan = compile(sql, ansi, LONG_COLS);
    Expression expr = spark(fn, -k, ansi, DataTypes.LongType);
    var loaded = VarkaEmitterFixtures.load(VarkaEmitterFixtures.emit(plan.outputs(),
        plan.inputOrdinals().size(), plan.numLiterals(), VarkaMatrixRuns.BASE));
    int[] ints = plan.literals().stream().mapToInt(Integer::intValue).toArray();
    long[] longs = plan.longLiterals().stream().mapToLong(Long::longValue).toArray();
    var row = new GenericInternalRow(new Object[1]);
    try {
      var answered = new ArrayList<Long>();
      var declined = new ArrayList<Long>();
      for (long v : longCells(k)) {
        boolean raises = false;
        row.update(0, v);
        try {
          expr.eval(row);
        } catch (ArithmeticException e) {
          raises = true;
        }
        (inWindow(v, k) && !raises ? answered : declined).add(v);
      }
      while (answered.size() < 1024) {
        long v = rnd.nextLong() >> rnd.nextInt(64);
        row.update(0, v);
        try {
          expr.eval(row);
          if (inWindow(v, k)) {
            answered.add(v);
          }
        } catch (ArithmeticException e) {
          // A long Spark raises on is a declined cell, which the named ones cover.
        }
      }
      long[] in = answered.stream().mapToLong(Long::longValue).toArray();
      long[] got = new long[in.length];
      int status = runLong(loaded.kernel(), ints, longs, in, got);
      assertEquals(0, status, ctx + ": a chunk inside the window was declined");
      for (int i = 0; i < in.length; i++) {
        row.update(0, in[i]);
        long want = (Long) expr.eval(row);
        if (got[i] != want) {
          fail(ctx + ": over " + in[i] + " the kernel gave " + got[i] + " and Spark " + want);
        }
      }
      for (long v : declined) {
        long[] one = {0L, v, 1L};
        int st = runLong(loaded.kernel(), ints, longs, one, new long[3]);
        assertNotEquals(0, st, ctx + ": a chunk holding " + v + ", outside the window or "
            + "raising, was answered");
      }
    } finally {
      loaded.loader().release();
    }
  }

  @Test
  @DisplayName("round, bround and truncate of a bigint at scales -1 to -20 answer exactly inside "
      + "the pre-shift's window and decline past it, in LEGACY and ANSI")
  void bigintWindow() {
    var rnd = new Random(344L);
    for (String fn : FUNCTIONS) {
      for (int k = 1; k <= 20; k++) {
        checkLong(fn, k, false, rnd);
        checkLong(fn, k, true, rnd);
      }
    }
  }
}
