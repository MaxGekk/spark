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
import java.lang.management.ManagementFactory;
import java.lang.reflect.Executable;
import java.util.List;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;

/**
 * Standalone probe: puts class-check traps into the Vector API's shared IntVector.add by
 * feeding one compiled caller receivers of four species, then resets the counts two ways
 * (WhiteBox clearMethodState; redefinition with the class's own bytes through an agent
 * loaded in-process), reading the counts through WhiteBox after each step. It also checks
 * that a vector loop is still allocation-free after the redefinition.
 */
public final class Probe370 {
  static int sink;

  static IntVector f(IntVector a, IntVector b) {
    return a.add(b);
  }

  static void drive(VectorSpecies<Integer> sp, int n) {
    int[] x = new int[sp.length()];
    for (int i = 0; i < x.length; i++) x[i] = i;
    IntVector a = IntVector.fromArray(sp, x, 0);
    IntVector b = a;
    for (int i = 0; i < n; i++) a = f(a, b);
    sink += a.lane(0);
  }

  static final VectorSpecies<Integer> S = IntVector.SPECIES_PREFERRED;

  static void kernel(int[] a, int[] b, int[] c) {
    for (int i = 0; i < a.length; i += S.length()) {
      IntVector.fromArray(S, a, i).add(IntVector.fromArray(S, b, i)).intoArray(c, i);
    }
  }

  static int kernelB(int[] a) {
    IntVector acc = IntVector.zero(S);
    for (int i = 0; i < a.length; i += S.length()) acc = f(acc, IntVector.fromArray(S, a, i));
    return acc.lane(0);
  }

  static Executable KA, KB, F;

  static String bytesPerKernelCall() throws Exception {
    if (KA == null) {
      KA = Probe370.class.getDeclaredMethod("kernel", int[].class, int[].class, int[].class);
      KB = Probe370.class.getDeclaredMethod("kernelB", int[].class);
      F = Probe370.class.getDeclaredMethod("f", IntVector.class, IntVector.class);
    }
    Counts370.WB.deoptimizeMethod(KA);
    Counts370.WB.deoptimizeMethod(KB);
    Counts370.WB.deoptimizeMethod(F);
    return "A=" + bytesA() + " B=" + bytesB();
  }

  static long bytesB() {
    int[] a = new int[1024];
    for (int i = 0; i < 200_000; i++) sink += kernelB(a);
    com.sun.management.ThreadMXBean t =
        (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    long before = t.getCurrentThreadAllocatedBytes();
    for (int i = 0; i < 10_000; i++) sink += kernelB(a);
    return (t.getCurrentThreadAllocatedBytes() - before) / 10_000;
  }

  static long bytesA() {
    int[] a = new int[1024], b = new int[1024], c = new int[1024];
    for (int i = 0; i < 200_000; i++) kernel(a, b, c);
    com.sun.management.ThreadMXBean t =
        (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    long before = t.getCurrentThreadAllocatedBytes();
    for (int i = 0; i < 10_000; i++) kernel(a, b, c);
    return (t.getCurrentThreadAllocatedBytes() - before) / 10_000;
  }

  static void pollute() {
    for (int round = 0; round < 4; round++) {
      drive(IntVector.SPECIES_512, 300_000);
      drive(IntVector.SPECIES_128, 20_000);
      drive(IntVector.SPECIES_256, 20_000);
      drive(IntVector.SPECIES_64, 20_000);
    }
  }

  static void show(String label, List<Executable> all, Executable add) {
    System.out.println(label + ": IntVector.add traps="
        + Counts370.WB.getMethodTrapCount(add) + " decompiles="
        + Counts370.WB.getMethodDecompileCount(add) + "; all vector methods "
        + Counts370.fmt(Counts370.sums(all)));
  }

  public static void main(String[] args) throws Exception {
    String jar = args[0];
    String mode = args[1];
    System.out.println("mode " + mode + "; agent load: " + Agent370.loadSelf(jar) + " inst="
        + (Agent370.inst != null));
    Executable add = IntVector.class.getMethod("add", jdk.incubator.vector.Vector.class);
    System.out.println("kernel bytes/call, fresh: " + bytesPerKernelCall());
    for (int cycle = 1; cycle <= 3; cycle++) {
      pollute();
      List<Executable> all = Counts370.methods(Agent370.vectorClasses());
      show("cycle " + cycle + " polluted", all, add);
      System.out.println("kernel bytes/call, polluted: " + bytesPerKernelCall());
      long t0 = System.nanoTime();
      if (mode.equals("clear")) {
        for (Executable m : all) Counts370.WB.clearMethodState(m);
      } else if (mode.equals("redefine")) {
        Agent370.redefine(Agent370.vectorClasses());
      }
      System.out.println(mode + " took " + (System.nanoTime() - t0) / 1_000_000 + " ms");
      all = Counts370.methods(Agent370.vectorClasses());
      show("cycle " + cycle + " after " + mode, all, add);
      System.out.println("kernel bytes/call, after " + mode + ": " + bytesPerKernelCall());
    }
    System.out.println("sink " + sink);
  }
}
