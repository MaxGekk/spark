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
import java.util.List;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;

import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.varka.vector.ChronoVectorOps;
import org.apache.spark.sql.varka.vector.DateVectorOps;

/**
 * The child process behind {@code VarkaAssemblySuite} (task 31). It is launched in a forked JVM
 * carrying {@code -XX:+UnlockDiagnosticVMOptions -XX:CompileCommand=print,...}, runs one named
 * case hot enough for C2 to compile the case's method, and exits; the disassembly HotSpot prints
 * on the way is what the parent reads.
 *
 * <p><b>Why the driver calls a method rather than looping inside one.</b> A single call with a
 * long loop reaches C2 through on-stack replacement, and an OSR nmethod is not the compilation
 * that runs in production - {@code SKILLS.md}'s bimodality investigation had to compare standard
 * nmethods for the same reason. So every case is a short method called {@link #ROUNDS} times,
 * which produces a standard compilation the parent can find by the absence of {@code %} in
 * HotSpot's {@code Compiled method} line.
 *
 * <p><b>Every case prints its preferred vector width</b> ({@link #PREFERRED_BITS_PREFIX}) before
 * doing any work. The expected register class - {@code zmm}, {@code ymm} or {@code xmm} - is a
 * property of the host and of the flags this child was given, not of the machine the suite was
 * written on, so the parent derives it from this line rather than assuming.
 */
public final class VarkaAssemblyProbe {

  /** The line the parent parses the host's preferred vector width out of. */
  public static final String PREFERRED_BITS_PREFIX = "VARKA_PROBE_PREFERRED_BITS=";

  /** The line the parent uses to confirm the case ran to completion rather than dying early. */
  public static final String DONE_PREFIX = "VARKA_PROBE_DONE=";

  private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

  /** Elements per call. Small enough that a call is cheap, large enough to be worth vectorizing. */
  private static final int LENGTH = 1024;

  /**
   * Calls per case. Well past {@code Tier4InvocationThreshold} (15000 by default) with room for a
   * loaded machine, and cheap: the whole run is a few hundred million integer ops.
   */
  private static final int ROUNDS = 200_000;

  private VarkaAssemblyProbe() {
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("usage: VarkaAssemblyProbe <case>");
      System.exit(2);
    }
    String name = args[0];
    System.out.println(PREFERRED_BITS_PREFIX + SPECIES.vectorBitSize());

    int[] a = new int[LENGTH];
    int[] b = new int[LENGTH];
    int[] o = new int[LENGTH];
    for (int i = 0; i < LENGTH; i++) {
      a[i] = i;
      b[i] = LENGTH - i;
    }

    int sink = 0;
    switch (name) {
      case "scalarChain" -> {
        for (int r = 0; r < ROUNDS; r++) {
          sink += scalarChain(b);
        }
      }
      case "vectorAdd" -> {
        for (int r = 0; r < ROUNDS; r++) {
          sink += vectorAdd(a, b, o);
        }
      }
      case "chronoFourFields" -> sink += chronoFourFields();
      case "dateAddDays" -> sink += dateAddDays();
      case "emittedYear" -> sink += emittedProjection(
          "emittedYear", List.of(new VarkaVectorIR.Year(COLUMN_0)), 1, 0);
      case "emittedDayOfWeek" -> sink += emittedProjection(
          "emittedDayOfWeek", List.of(new VarkaVectorIR.DayOfWeek(COLUMN_0)), 1, 0);
      case "emittedCompare" -> sink += emittedProjection(
          "emittedCompare",
          List.of(new VarkaVectorIR.IfElse(
              new VarkaVectorIR.Compare(
                  VarkaVectorIR.CompareOp.GT, COLUMN_0, new VarkaVectorIR.LiteralSlot(0)),
              COLUMN_0,
              new VarkaVectorIR.LiteralSlot(0))),
          1, 1);
      default -> {
        System.err.println("unknown case: " + name);
        System.exit(2);
      }
    }
    System.out.println(DONE_PREFIX + name + " sink=" + sink);
  }

  /**
   * The negative half of the self-test: a body that cannot vectorize, whatever C2 does.
   *
   * <p>The recurrence is the point. {@code acc} is read and written every iteration and the
   * update is not one of the reduction forms SuperWord recognises, so no auto-vectorizer can
   * turn this into packed arithmetic. That is deliberately a *structural* guarantee rather than
   * {@code -XX:-UseSuperWord}: a flag would hide it if this loop turned out to be vectorizable
   * after all, and the whole value of this case is that the detector reports "no packed add" on
   * a body that genuinely has none.
   */
  public static int scalarChain(int[] b) {
    int acc = 1;
    for (int i = 0; i < b.length; i++) {
      acc = acc * 31 + b[i];
    }
    return acc;
  }

  /**
   * The positive half: an explicit {@link IntVector} loop, which is what the emitter produces.
   * Auto-vectorization of a plain {@code o[i] = a[i] + b[i]} would also do, but this exercises
   * the Vector API intrinsic path the real kernels depend on, so a failure here means the same
   * thing a failure over a Varka kernel would.
   */
  public static int vectorAdd(int[] a, int[] b, int[] o) {
    int i = 0;
    int bound = SPECIES.loopBound(a.length);
    for (; i < bound; i += SPECIES.length()) {
      IntVector va = IntVector.fromArray(SPECIES, a, i);
      IntVector vb = IntVector.fromArray(SPECIES, b, i);
      va.add(vb).intoArray(o, i);
    }
    for (; i < a.length; i++) {
      o[i] = a[i] + b[i];
    }
    return o[0];
  }

  // --- The kernels and the emitted loops --------------------------------------------------------

  private static final VarkaVectorIR COLUMN_0 = new VarkaVectorIR.ColumnRef(0);

  /**
   * The prefix every emitted case's class name carries. Production names a generated class
   * {@code VarkaFusedProjection_<shape hash>} ({@code VarkaShapeCacheImpl.classNameFor}), so the
   * probe's classes are named the same way with a readable suffix in place of the hash: the
   * suite's {@code CompileCommand} pattern then wildcards exactly where it would in production,
   * rather than exercising a pattern shape that never occurs.
   */
  private static final String EMITTED_PREFIX =
      "org.apache.spark.sql.varka.execution.VarkaFusedProjection_";

  /** Rows per call. A multiple of every supported lane count, so the dense loop does the work
   *  and the epilogue handles nothing. */
  private static final int ROWS = 1024;

  /** Calls for the off-heap cases. Lower than {@link #ROUNDS} because each call does 1024 rows
   *  of real work rather than one add, and still far past the tier-4 threshold. */
  private static final int KERNEL_ROUNDS = 50_000;

  /** Days around 2020, so the calendar lowering runs on realistic values. */
  private static void fillDays(MemorySegment data, int rows) {
    for (int i = 0; i < rows; i++) {
      data.set(ValueLayout.JAVA_INT, i * 4L, 18000 + i);
    }
  }

  private static MemorySegment allValid(Arena arena, int rows) {
    MemorySegment validity = arena.allocate((rows + 7) / 8L, 8);
    validity.fill((byte) 0xFF);
    return validity;
  }

  /** {@code ChronoVectorOps.vectorFourFields} - the hand-written reference the emitted calendar
   *  lowering is measured against, and the first thing this suite should be able to vouch for. */
  private static int chronoFourFields() {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment src = arena.allocate(ROWS * 4L, 64);
      fillDays(src, ROWS);
      MemorySegment srcValidity = allValid(arena, ROWS);
      long[] dstData = new long[4];
      long[] dstValidity = new long[4];
      for (int f = 0; f < 4; f++) {
        dstData[f] = arena.allocate(ROWS * 4L, 64).address();
        dstValidity[f] = arena.allocate((ROWS + 7) / 8L, 8).address();
      }
      int status = 0;
      for (int r = 0; r < KERNEL_ROUNDS; r++) {
        status |= ChronoVectorOps.vectorFourFields(
            src.address(), srcValidity.address(), 0, dstData, dstValidity, ROWS);
      }
      return status;
    }
  }

  /** {@code DateVectorOps.vectorAddDays} - the simplest kernel, and the one whose body should be
   *  packed loads, one packed add and packed stores with nothing else in it. */
  private static int dateAddDays() {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment src = arena.allocate(ROWS * 4L, 64);
      fillDays(src, ROWS);
      MemorySegment srcValidity = allValid(arena, ROWS);
      MemorySegment dst = arena.allocate(ROWS * 4L, 64);
      MemorySegment dstValidity = arena.allocate((ROWS + 7) / 8L, 8);
      for (int r = 0; r < KERNEL_ROUNDS; r++) {
        DateVectorOps.vectorAddDays(src.address(), srcValidity.address(), 0,
            dst.address(), dstValidity.address(), ROWS, 7);
      }
      return dst.get(ValueLayout.JAVA_INT, 0);
    }
  }

  /**
   * Emits one projection, loads it, and runs it hot.
   *
   * <p>The driver calls {@code loopDense0} once per {@code run}, so the loop method's own
   * invocation counter trips at the same rate the kernel's does and HotSpot compiles it
   * standalone - which is what lets the assertion name a loop method rather than the whole
   * kernel, and is why {@code KERNEL_ROUNDS} calls rather than one long loop.
   */
  private static int emittedProjection(
      String suffix, List<VarkaVectorIR> outputs, int numInputs, int numLiterals) {
    String className = EMITTED_PREFIX + suffix;
    byte[] bytes = VarkaLoopEmitter.emit(
        className, outputs, numInputs, numLiterals, null, null, VarkaEmitOptions.DEFAULTS);
    VarkaFusedKernel kernel;
    try {
      VarkaGeneratedClassLoader loader =
          new VarkaGeneratedClassLoader(VarkaAssemblyProbe.class.getClassLoader());
      loader.defineGeneratedClass(className, bytes);
      kernel = (VarkaFusedKernel) loader.loadClass(className).getConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("could not load the emitted kernel " + className, e);
    }
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment src = arena.allocate(ROWS * 4L, 64);
      fillDays(src, ROWS);
      MemorySegment srcValidity = allValid(arena, ROWS);
      long[] dstData = new long[outputs.size()];
      long[] dstValidity = new long[outputs.size()];
      for (int i = 0; i < outputs.size(); i++) {
        dstData[i] = arena.allocate(ROWS * 4L, 64).address();
        dstValidity[i] = arena.allocate((ROWS + 7) / 8L, 8).address();
      }
      int[] scalarArgs = new int[numLiterals];
      for (int i = 0; i < numLiterals; i++) {
        scalarArgs[i] = 18500;
      }
      int status = 0;
      for (int r = 0; r < KERNEL_ROUNDS; r++) {
        status |= kernel.run(new long[] {src.address()}, new long[] {srcValidity.address()},
            new int[] {0}, dstData, dstValidity, scalarArgs, ROWS);
      }
      return status;
    }
  }
}
