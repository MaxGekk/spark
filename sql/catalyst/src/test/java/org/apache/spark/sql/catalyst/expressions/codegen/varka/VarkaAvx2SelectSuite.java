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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Locale;

import com.sun.management.HotSpotDiagnosticMXBean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Compare;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.CompareOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IfElse;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntArith;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;

/**
 * Row 396's shrunk case as a regression test, under AVX2 (VARKA-491). JDK 25's C2 miscompiles
 * {@code VectorMask.fromLong(s, m.toLong())} on x86 without AVX-512 mask registers. The masked
 * body of {@code if(c0 > c1, c0, try_add(c1, c2))}, over a lane group whose columns have no nulls,
 * is that round trip under {@code fromLongMasks}: C2-compiled, its select takes the ELSE branch on
 * every true lane. At the defaults the emitter builds the mask through {@code VarkaMaskOps}. This
 * suite runs the case through every compiled history of {@link VarkaCompiledCheck} in a JVM of its
 * own started with {@code -XX:UseAVX=2}, so an AVX-512 host checks it too; a JVM that is not x86
 * ignores the flag. The int lane takes the same shape, since the rule matches every lane type.
 *
 * <p>Under {@code fromLongMasks} both cases fail in {@code warm-same}. The failure is a matter of
 * the JIT's timing, so a pass proves less than the byte check of {@code VarkaMaskOfWordSuite},
 * which holds the emitter to the form whatever the JIT does.
 */
@VarkaSuite(ownJvm = true, jvmFlags = {"-XX:+IgnoreUnrecognizedVMOptions", "-XX:UseAVX=2"})
public final class VarkaAvx2SelectSuite {

  /** The shrunk batch: seven rows, a lane group of four and a three-row masked epilogue. */
  private static final Long[][] COLUMNS = {
    {4460076517051L, null, 18898746763276L, null, -50266100486726L, null, -49607829850524L},
    {4421922265370L, 35258963307884L, -59385115539699L, null, 38382192361867L, 5079289584248L,
        null},
    {-36599559927214L, null, -25562645782204L, null, -41690266522730L, null, 41053864906336L}};

  /** The options the shrink kept off; the rest are the defaults. */
  private static final String SHRUNK_OPTIONS = "rangeSets=false,splitConditions=false,"
      + "groupLocalSlots=false,materializeChronoPrefix=false,predictGrouping=false,"
      + "driverOutputTable=false,splitDriver=false,severalKernels=false,"
      + "elideUnreadLocals=false,planSize=false,groupBudgetUnderPrediction=true";

  /** The tries a case gets to reach {@code warm-same}, the history row 396's case failed in. */
  private static final int TRIES = 3;

  /**
   * Runs the shrunk shape at {@code lane} through every history, failing on a wrong answer. A
   * run whose {@code warm-same} kernel was not C2 code throughout checked nothing the bug needs,
   * so the case runs again, and is cancelled, not passed, when no try reached the history.
   */
  private static void check(LaneType lane, String label) {
    VarkaVectorIR c0 = new ColumnRef(0, lane);
    VarkaVectorIR c1 = new ColumnRef(1, lane);
    VarkaVectorIR c2 = new ColumnRef(2, lane);
    VarkaVectorIR root = new IfElse(new Compare(CompareOp.GT, c0, c1), c0,
        new IntArith(IntOp.ADD, Overflow.NULL, c1, c2));
    VarkaEmitOptions options = VarkaMatrixRuns.parse(SHRUNK_OPTIONS);
    int rows = COLUMNS[0].length;
    boolean[][] nulls = new boolean[COLUMNS.length][rows];
    long[][] data = new long[COLUMNS.length][rows];
    for (int c = 0; c < COLUMNS.length; c++) {
      for (int i = 0; i < rows; i++) {
        nulls[c][i] = COLUMNS[c][i] == null;
        // The int lane keeps each value's low half, which is as random as the whole.
        long v = COLUMNS[c][i] == null ? 0 : COLUMNS[c][i];
        data[c][i] = lane == LaneType.INT ? (int) v : v;
      }
    }
    var fuzzCase = new VarkaFuzzCase(lane, List.of(root), COLUMNS.length, new long[0], rows,
        nulls, data, true, options, 0, 0, label);
    var settings = VarkaCompiledCheck.Settings.fromProperties();
    List<String> missed = List.of();
    for (int i = 0; i < TRIES; i++) {
      var tally = new VarkaCompiledCheck.Tally();
      tally.add(VarkaCompiledCheck.run(fuzzCase,
          name -> VarkaLoopEmitter.emit(name, List.of(root), COLUMNS.length, 0, null, null,
              options),
          settings, label));
      if (!settings.witnessed()) {
        return;
      }
      // The verdicts are read once the witness has seen every compile and trap of the run.
      VarkaTierWitness.get().settle();
      missed = VarkaCompiledCheck.notReached(tally).stream()
          .filter(m -> m.startsWith("history not reached: warm-same,"))
          .toList();
      if (missed.isEmpty()) {
        return;
      }
    }
    assumeTrue(false, TRIES + " tries did not reach warm-same: " + missed);
  }

  @Test
  @DisplayName("the child runs under AVX2 where it is x86")
  void runsUnderAvx2() {
    String arch = System.getProperty("os.arch").toLowerCase(Locale.ROOT);
    if (arch.equals("amd64") || arch.equals("x86_64")) {
      var hotspot = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
      assertEquals("2", hotspot.getVMOption("UseAVX").getValue());
    }
  }

  @Test
  @DisplayName("row 396's long-lane select answers right in every compiled history")
  void longSelect() {
    check(LaneType.LONG, "row396-long");
  }

  @Test
  @DisplayName("the same select at the int lane answers right in every compiled history")
  void intSelect() {
    check(LaneType.INT, "row396-int");
  }
}
