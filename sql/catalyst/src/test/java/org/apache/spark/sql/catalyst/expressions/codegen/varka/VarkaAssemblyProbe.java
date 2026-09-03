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

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;

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
}
