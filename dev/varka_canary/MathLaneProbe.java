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
import jdk.incubator.vector.*;

/**
 * Do the Vector API's math operators compute the same bits as the row engine would?
 *
 * Spark's math expressions call one of two scalar libraries: {@code java.lang.Math} for
 * sin, cos, tan, the inverse and hyperbolic trig, cbrt, sqrt, atan2 and hypot, and
 * {@code StrictMath} - fdlibm - for exp, expm1, log, log10, log1p and pow. A Varka kernel
 * would compute the same function with {@code DoubleVector.lanewise(VectorOperators.SIN)}
 * and its siblings, which C2 lowers to a vector math library: Intel's SVML on x86
 * ({@code libjsvml.so}), a SLEEF derivative on aarch64. Whether the lanes agree with the
 * scalar call bit for bit decides whether such a kernel can hold Varka's contract that a
 * fused result equals the row engine's, or needs a ULP contract instead
 * (SCOPE_FUNCTIONS.md section 3).
 *
 * Each operator runs over the same inputs enough times to reach C2, and every lane is then
 * compared against both scalar libraries. The count of differing lanes and the largest
 * difference in units in the last place are printed per operator. The inputs are positive and
 * moderate so every operator is defined on all of them; the point is agreement on ordinary
 * arguments, not edge behaviour.
 *
 * How to run:
 *   java --add-modules jdk.incubator.vector dev/varka_canary/MathLaneProbe.java
 * To see which library the lanes reached - the diagnosis, not a guess:
 *   java --add-modules jdk.incubator.vector -Xlog:library=info \
 *     dev/varka_canary/MathLaneProbe.java 2>&1 | grep -E 'jsvml|sleef'
 * and -XX:+UnlockDiagnosticVMOptions -XX:+PrintIntrinsics shows any operator C2 refused.
 *
 * On 19 September 2026, JDK 25, Zen 5 (this repo's laptop): every operator resolved an
 * {@code __jsvml_*_ha_z0} symbol, none was refused, and all eleven agreed with
 * {@code java.lang.Math} on every one of 262144 inputs. Against {@code StrictMath} eight of
 * them differed by one or two ULP on between two and ten percent of inputs; expm1, log1p
 * and atan agreed with both. Of the eight, only exp, log and log10 are functions Spark
 * computes with {@code StrictMath}, so those three are where a lane and the row engine
 * part. The consequence for Spark's function set is drawn in
 * SCOPE_FUNCTIONS.md. The result is a property of the host's two libraries, so it has to be
 * re-read on aarch64 before it is relied on there.
 */
public class MathLaneProbe {
  static final VectorSpecies<Double> S = DoubleVector.SPECIES_PREFERRED;

  static void run(VectorOperators.Unary op, double[] x, double[] v) {
    for (int i = 0; i < x.length; i += S.length()) {
      DoubleVector.fromArray(S, x, i).lanewise(op).intoArray(v, i);
    }
  }

  static double math(String name, double x) {
    return switch (name) {
      case "SIN" -> Math.sin(x); case "COS" -> Math.cos(x); case "TAN" -> Math.tan(x);
      case "EXP" -> Math.exp(x); case "LOG" -> Math.log(x); case "LOG10" -> Math.log10(x);
      case "EXPM1" -> Math.expm1(x); case "LOG1P" -> Math.log1p(x); case "ATAN" -> Math.atan(x);
      case "TANH" -> Math.tanh(x); default -> Math.cbrt(x);
    };
  }

  static double strict(String name, double x) {
    return switch (name) {
      case "SIN" -> StrictMath.sin(x); case "COS" -> StrictMath.cos(x);
      case "TAN" -> StrictMath.tan(x); case "EXP" -> StrictMath.exp(x);
      case "LOG" -> StrictMath.log(x); case "LOG10" -> StrictMath.log10(x);
      case "EXPM1" -> StrictMath.expm1(x); case "LOG1P" -> StrictMath.log1p(x);
      case "ATAN" -> StrictMath.atan(x); case "TANH" -> StrictMath.tanh(x);
      default -> StrictMath.cbrt(x);
    };
  }

  /** The binary operators Spark reaches: pow through StrictMath, atan2 and hypot through Math. */
  static double math2(String name, double x, double y) {
    return switch (name) {
      case "POW" -> Math.pow(x, y); case "ATAN2" -> Math.atan2(x, y); default -> Math.hypot(x, y);
    };
  }

  static double strict2(String name, double x, double y) {
    return switch (name) {
      case "POW" -> StrictMath.pow(x, y); case "ATAN2" -> StrictMath.atan2(x, y);
      default -> StrictMath.hypot(x, y);
    };
  }

  static void run2(VectorOperators.Binary op, double[] x, double[] y, double[] v) {
    for (int i = 0; i < x.length; i += S.length()) {
      DoubleVector.fromArray(S, x, i).lanewise(op, DoubleVector.fromArray(S, y, i))
          .intoArray(v, i);
    }
  }

  /**
   * What machine this ran on, printed first so a result can be attributed: the JVM's own view
   * of the architecture and vector width, and the AVX level where the flag exists. A row of
   * results without this line is the thing task 150 exists to prevent.
   */
  static String machine() {
    String avx;
    try {
      avx = java.lang.management.ManagementFactory
          .getPlatformMXBean(com.sun.management.HotSpotDiagnosticMXBean.class)
          .getVMOption("UseAVX").getValue();
    } catch (RuntimeException e) {
      avx = "n/a";
    }
    String mvs;
    try {
      mvs = java.lang.management.ManagementFactory
          .getPlatformMXBean(com.sun.management.HotSpotDiagnosticMXBean.class)
          .getVMOption("MaxVectorSize").getValue();
    } catch (RuntimeException e) {
      mvs = "n/a";
    }
    return "arch=" + System.getProperty("os.arch") + " jvm=" + System.getProperty("java.vm.version")
        + " UseAVX=" + avx + " MaxVectorSize=" + mvs + " species double=" + S.length();
  }

  public static void main(String[] a) {
    int n = 1 << 18;
    int rounds = a.length > 0 ? Integer.parseInt(a[0]) : 200;
    double[] x = new double[n];
    double[] v = new double[n];
    java.util.Random r = new java.util.Random(7);
    for (int i = 0; i < n; i++) {
      x[i] = Math.abs((r.nextDouble() - 0.5) * 60);
    }
    String[] names = {"SIN", "COS", "TAN", "EXP", "LOG", "LOG10", "EXPM1", "LOG1P", "ATAN",
        "TANH", "CBRT"};
    VectorOperators.Unary[] ops = {VectorOperators.SIN, VectorOperators.COS,
        VectorOperators.TAN, VectorOperators.EXP, VectorOperators.LOG, VectorOperators.LOG10,
        VectorOperators.EXPM1, VectorOperators.LOG1P, VectorOperators.ATAN,
        VectorOperators.TANH, VectorOperators.CBRT};
    System.out.println(machine() + "; inputs=" + n + "; rounds before measuring=" + rounds);
    for (int k = 0; k < ops.length; k++) {
      for (int w = 0; w < rounds; w++) {
        run(ops[k], x, v);
      }
      long diffMath = 0, diffStrict = 0, ulpMath = 0, ulpStrict = 0;
      for (int i = 0; i < n; i++) {
        long b = Double.doubleToLongBits(v[i]);
        long em = Math.abs(b - Double.doubleToLongBits(math(names[k], x[i])));
        long es = Math.abs(b - Double.doubleToLongBits(strict(names[k], x[i])));
        if (em != 0) { diffMath++; ulpMath = Math.max(ulpMath, em); }
        if (es != 0) { diffStrict++; ulpStrict = Math.max(ulpStrict, es); }
      }
      System.out.printf("%-6s vs Math: %6d lanes differ (max %d ulp)   vs StrictMath: %6d"
          + " lanes differ (max %d ulp)%n", names[k], diffMath, ulpMath, diffStrict, ulpStrict);
    }
    // The binary operators, over a second positive operand in a moderate range.
    double[] y = new double[n];
    for (int i = 0; i < n; i++) {
      y[i] = 0.5 + r.nextDouble() * 4;
    }
    String[] names2 = {"POW", "ATAN2", "HYPOT"};
    VectorOperators.Binary[] ops2 = {VectorOperators.POW, VectorOperators.ATAN2,
        VectorOperators.HYPOT};
    for (int k = 0; k < ops2.length; k++) {
      for (int w = 0; w < rounds; w++) {
        run2(ops2[k], x, y, v);
      }
      long diffMath = 0, diffStrict = 0, ulpMath = 0, ulpStrict = 0;
      for (int i = 0; i < n; i++) {
        long b = Double.doubleToLongBits(v[i]);
        long em = Math.abs(b - Double.doubleToLongBits(math2(names2[k], x[i], y[i])));
        long es = Math.abs(b - Double.doubleToLongBits(strict2(names2[k], x[i], y[i])));
        if (em != 0) { diffMath++; ulpMath = Math.max(ulpMath, em); }
        if (es != 0) { diffStrict++; ulpStrict = Math.max(ulpStrict, es); }
      }
      System.out.printf("%-6s vs Math: %6d lanes differ (max %d ulp)   vs StrictMath: %6d"
          + " lanes differ (max %d ulp)%n", names2[k], diffMath, ulpMath, diffStrict, ulpStrict);
    }
  }
}
