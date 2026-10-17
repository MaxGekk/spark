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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaFusedKernel;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaLoopEmitter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Compare;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.CompareOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Cond;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Greatest;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IfElse;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntArith;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;

/**
 * What a 64-bit lane costs against a 32-bit one, for the same expression over the same rows.
 *
 * <p>The long lane is milestone 5's subject: {@code bigint}, {@code TIME}, the timestamps and
 * day-time intervals are all built on it, and VARKA-85 made the emitter produce it before any of
 * those exist. This file is that lane's baseline, committed before the compiler arms that let
 * SQL reach it (VARKA-104) - the project's rule that a measurement precedes the change it
 * is meant to judge.
 *
 * <p><b>The arithmetic the numbers are read against.</b> A 512-bit register holds sixteen int
 * lanes and eight long ones, so the same row count is twice the lane groups. It is also twice the
 * bytes. So a compute-bound shape should cost about twice as much per row at the wider lane, and a
 * memory-bound one should too, for a different reason - and a shape that costs <i>less</i> than
 * twice would mean neither bound was reached. The interesting number is not the ratio itself but
 * where each shape falls against 2.
 *
 * <p><b>Why this is a ladder and not one row count.</b> Doubling the width doubles the working
 * set, so at some row counts the two arms are not measured in the same place: the int arm still
 * fits a cache level that the long arm has just fallen out of, and the ratio then prices that
 * boundary rather than the lane. The rungs below bracket it - two where both arms share a level,
 * one where the split is expected, and one where both are past the last level - so a ratio near 2
 * and a ratio far above it can be told apart instead of averaged. A rung's working set is three
 * columns wide (two inputs and an output), which is 12 bytes a row at the int lane and 24 at the
 * long one.
 *
 * <p><b>What is deliberately not here.</b> No SQL, no evaluator, no cache: these are the emitted
 * kernels driven straight over memory segments, as {@code VarkaArithmeticBenchmark} drives them,
 * so what moves between the two arms is the lane and nothing else. The shapes are the subset task
 * 85 ships at the long lane; the calendar family has no 64-bit form to price.
 *
 * <pre>
 *   build/sbt "catalyst/Test/runMain org.apache.spark.sql.VarkaLongLaneBenchmark"
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt \
 *     "catalyst/Test/runMain org.apache.spark.sql.VarkaLongLaneBenchmark"
 * </pre>
 */
public class VarkaLongLaneBenchmark extends VarkaBenchmarkBase {

  /** A rung of the ladder: a row count and what it is for. */
  private record Rung(int numRows, String level) { }

  /** A shape: its name and its roots at one lane. */
  private record Shape(String name, List<VarkaVectorIR> roots) { }

  /**
   * The rungs, by what each one is for. The sizes are the two-column working sets they imply per
   * arm, int lane then long lane, against this class of machine: a private L2 of about a megabyte
   * a core and a shared L3 of tens of megabytes.
   */
  private static final List<Rung> LADDER = List.of(
      new Rung(16384, "both arms in L2"),            // 192 KB and 384 KB
      new Rung(262144, "both arms in L3"),           // 3 MB and 6 MB
      new Rung(1000000, "the long arm leaves L3"),   // 12 MB and 24 MB
      new Rung(8388608, "both arms past L3"));       // 96 MB and 192 MB

  private final AtomicInteger classCounter = new AtomicInteger(0);

  private VarkaFusedKernel kernel(List<VarkaVectorIR> roots, int numInputs, int numLiterals,
      VarkaGeneratedClassLoader loader) {
    String name = "org.apache.spark.sql.varka.bench.LongLane" + classCounter.incrementAndGet();
    byte[] bytes = VarkaLoopEmitter.emit(name, roots, numInputs, numLiterals, null, null,
        VarkaEmitOptions.DEFAULTS);
    return defineKernel(loader, name, bytes);
  }

  /** One null-free column of the given width, values chosen to exercise the whole range. */
  private static MemorySegment column(Arena arena, int numRows, int bytesPerRow, int shift) {
    MemorySegment data = arena.allocate((long) numRows * bytesPerRow, 8);
    for (int i = 0; i < numRows; i++) {
      if (bytesPerRow == 4) {
        data.set(ValueLayout.JAVA_INT, i * 4L, (i * 7 + shift) % 100000 - 50000);
      } else {
        // Past the int range on purpose: a long lane that was quietly an int one would be
        // measured on values it could not hold.
        data.set(ValueLayout.JAVA_LONG, i * 8L, (1L << 40) + (i * 7L + shift) % 100000L - 50000L);
      }
    }
    return data;
  }

  private static List<Shape> shapes(LaneType lane) {
    var c = new ColumnRef(0, lane);
    var c1 = new ColumnRef(1, lane);
    var l = new LiteralSlot(0, lane);
    return List.of(
        new Shape("a column, copied", List.of(c)),
        new Shape("column + literal, wrapping",
            List.of(new IntArith(IntOp.ADD, Overflow.WRAP, c, l))),
        new Shape("column + column, wrapping",
            List.of(new IntArith(IntOp.ADD, Overflow.WRAP, c, c1))),
        new Shape("column + literal, ANSI checked",
            List.of(new IntArith(IntOp.ADD, Overflow.FAIL, c, l))),
        new Shape("column * column, wrapping",
            List.of(new IntArith(IntOp.MUL, Overflow.WRAP, c, c1))),
        new Shape("greatest(column, column)", List.of(new Greatest(c, c1))),
        new Shape("a comparison, as a selection", List.of(new Compare(CompareOp.LT, c, c1))),
        new Shape("CASE WHEN c < c2 THEN c ELSE literal END",
            List.of(new IfElse(new Compare(CompareOp.LT, c, c1), c, l))));
  }

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    var loader = new VarkaGeneratedClassLoader(getClass().getClassLoader());
    try {
      section("the same expression at 32-bit and 64-bit lanes", () -> {
        for (Rung rung : LADDER) {
          runRung(rung.numRows(), rung.level(), loader);
        }
      });
    } finally {
      loader.release();
    }
  }

  /**
   * One rung: fresh buffers, both lanes, every shape. Each rung owns its arena and closes it, so
   * a later rung never measures a cache the earlier one happened to leave warm.
   */
  private void runRung(int numRows, String level, VarkaGeneratedClassLoader loader) {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment int1 = column(arena, numRows, 4, 0);
      MemorySegment int2 = column(arena, numRows, 4, 137);
      MemorySegment long1 = column(arena, numRows, 8, 0);
      MemorySegment long2 = column(arena, numRows, 8, 137);
      MemorySegment dstInt = arena.allocate(numRows * 4L, 8);
      MemorySegment dstLong = arena.allocate(numRows * 8L, 8);
      MemorySegment dstValidity = arena.allocate((numRows + 7) / 8L, 8);
      int[] intLits = {3};
      long[] longLits = {3L << 32};

      var benchmark = benchmark(numRows + " rows - " + level, numRows).timed().build();
      List<Shape> intShapes = shapes(LaneType.INT);
      List<Shape> longShapes = shapes(LaneType.LONG);
      for (int s = 0; s < intShapes.size(); s++) {
        Shape intShape = intShapes.get(s);
        Shape longShape = longShapes.get(s);
        VarkaFusedKernel ik = kernel(intShape.roots(), 2, intLits.length, loader);
        VarkaFusedKernel lk = kernel(longShape.roots(), 2, longLits.length, loader);
        benchmark.addCase(intShape.name() + ", int32 lanes", iteration -> {
          long dst = intShape.roots().get(0) instanceof Cond ? 0L : dstInt.address();
          int status = ik.run(new long[] {int1.address(), int2.address()}, new long[] {0L, 0L},
              new int[] {0, 0}, new long[] {dst}, new long[] {dstValidity.address()}, intLits,
              numRows);
          if (status != 0) {
            throw new IllegalStateException(
                "assertion failed: the kernel declined a batch: " + status);
          }
        });
        benchmark.addCase(intShape.name() + ", int64 lanes", iteration -> {
          long dst = longShape.roots().get(0) instanceof Cond ? 0L : dstLong.address();
          int status = lk.run(new long[] {long1.address(), long2.address()},
              new long[] {0L, 0L}, new int[] {0, 0}, new long[] {dst},
              new long[] {dstValidity.address()}, new int[0], longLits, numRows);
          if (status != 0) {
            throw new IllegalStateException(
                "assertion failed: the kernel declined a batch: " + status);
          }
        });
      }
      benchmark.run();
    }
  }
}
