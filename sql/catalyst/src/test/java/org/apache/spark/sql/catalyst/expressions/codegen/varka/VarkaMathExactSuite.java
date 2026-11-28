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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.OptionalLong;
import java.util.SplittableRandom;
import java.util.function.IntPredicate;

import javax.annotation.Nullable;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions.FloatingFault;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitterFixtures.Col;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitterFixtures.Output;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntArith;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.MathBinary;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.MathFn;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.MathUnary;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;

/**
 * The math functions' exact forms against Spark's generated code (VARKA-356, {@code VARKA-345.md}
 * 2.3): each function's kernel, a per-lane call of the method Spark's code calls, compared bit for
 * bit with that code evaluated by scalar Java on the same JVM - {@code Math.sin(c)} for
 * {@code sin}, {@code 1 / Math.cos(c)} for {@code sec} - by {@code doubleToRawLongBits}.
 *
 * <p>Three checks. The sampled differential draws {@value #SAMPLES} inputs a function (uniform bit
 * patterns, the function's own domain, {@code [-2, 2]}, magnitudes down to {@code 2^-60}, and the
 * function's own hard inputs: for the trigonometric functions arguments from {@code 2^20} up to
 * {@code 1e300}, where the reduction is hardest; for the exponentials the band from underflow to
 * overflow; for the log family subnormals, {@code [0.5, 2]}, the non-positive half and both
 * zeros) at the preferred width. The fixed vectors - the special values and their 64 neighbours
 * each way, Spark's own edges and NaN payloads of both signs, quiet and signalling - run through
 * every null pattern and several lengths at two and eight lanes. And two planted faults must fail:
 * the other class's method fails the differential where the classes differ, and the log family's
 * null at {@code <} rather than {@code <=} fails the fixed vectors at the asymptote.
 *
 * <p>The log family's domain null is checked as a null: a lane Spark's code nulls must come back
 * null, and a lane it answers must come back valid with Spark's bits.
 *
 * <p>Every answer is compared by its raw bits, a NaN's too: a NaN passed through from the input
 * keeps its payload, and one the function makes from a number ({@code asin(2.0)}) is the bits the
 * one method computes on this JVM for both sides. The one exception is a two-operand function
 * over two NaNs, whose answer the same method gave as either operand's payload on one JVM; there
 * two NaNs agree, by {@link VarkaSameAnswer}'s row level.
 */
@VarkaSuite(ownJvm = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public final class VarkaMathExactSuite {

  /** The sampled inputs a function, about 2^24, over the draws below. */
  private static final int SAMPLES = Integer.getInteger("varka.math.samples", 1 << 24);

  /** The rows of one sampled batch. */
  private static final int BATCH = 1 << 14;

  /**
   * The sampled inputs of a two-operand function under the planted other class, about 2^20.
   * {@code Math.pow} is the host's stub on x86 and leaves {@code StrictMath.pow}'s bits on a large
   * share of the inputs, so a sixteenth of the draws sees it; its two columns and the call make
   * its draw the suite's dearest. The one-operand functions keep {@link #SAMPLES}, the
   * differential's own count, since their classes differ on a few inputs in a thousand.
   */
  private static final int PLANT_SAMPLES = SAMPLES >> 4;

  private static final ColumnRef X = new ColumnRef(0, LaneType.DOUBLE);

  /** The second column: the base of {@code log(b, x)}, or the second operand. */
  private static final ColumnRef B = new ColumnRef(1, LaneType.DOUBLE);

  /** The inputs a function's own draw takes, and its fixed vectors' extra edges. */
  private enum Domain { TRIG, UNIT, PLAIN, EXP, LOG, LOG1P, POW }

  /**
   * Spark's generated code for a function, evaluated by scalar Java: its answer for the operand
   * {@code x} and, for a two-operand function, the second operand {@code b}, or null where the
   * code sets {@code isNull}.
   */
  @FunctionalInterface
  private interface Code {
    @Nullable Double eval(double x, double b);
  }

  /**
   * A function as the kernel computes it and as Spark's generated code does. {@code columns} is 1,
   * or 2 for {@code log(b, x)} and {@code pow}; {@code classesDiffer} says whether
   * a call of the other class's method answers otherwise on x86 ({@code VARKA-356.md} item 0.3);
   * {@code logNull} whether the function carries the log family's domain null.
   */
  private record Fn(String name, VarkaVectorIR root, long[] lits, int columns, Code spark,
      Domain domain, boolean classesDiffer, boolean logNull) {}

  private static long[] bits(double... values) {
    long[] out = new long[values.length];
    for (int i = 0; i < values.length; i++) {
      out[i] = Double.doubleToRawLongBits(values[i]);
    }
    return out;
  }

  private static LiteralSlot lit(int index) {
    return new LiteralSlot(index, LaneType.DOUBLE);
  }

  private static Fn call(String name, MathFn fn, Code spark, Domain domain,
      boolean classesDiffer) {
    return new Fn(name, new MathUnary(fn, X), new long[0], 1, spark, domain, classesDiffer,
        false);
  }

  /** {@code 1 / fn(c)}, Spark's {@code sec}, {@code csc} and {@code cot}, the 1.0 a literal. */
  private static Fn reciprocal(String name, MathFn fn, Code spark) {
    var root = new IntArith(IntOp.DIV, Overflow.WRAP, lit(0), new MathUnary(fn, X));
    return new Fn(name, root, bits(1.0), 1, spark, Domain.TRIG, true, false);
  }

  /** {@code fn(c)} under the domain null at {@code asymptote}, the asymptote literal 0. */
  private static Fn logCall(String name, MathFn fn, double asymptote, Code spark, Domain domain,
      boolean classesDiffer, FloatingFault fault) {
    return new Fn(name, VarkaMathDomain.nullAtOrBelow(fn, X, lit(0), fault), bits(asymptote), 1,
        spark, domain, classesDiffer, true);
  }

  /** {@code ln(c) / divisor}, the divisor literal 1: {@code log2} and a constant base. */
  private static Fn logOver(String name, double divisor, Code spark, FloatingFault fault) {
    var root = new IntArith(IntOp.DIV, Overflow.WRAP,
        VarkaMathDomain.nullAtOrBelow(MathFn.LOG, X, lit(0), fault), lit(1));
    return new Fn(name, root, bits(0.0, divisor), 1, spark, Domain.LOG, true, true);
  }

  /** Spark's {@code if (c <= t) null else v}, Java's {@code <=}. */
  private static @Nullable Double nullAtOrBelow(double c, double t, double v) {
    return c <= t ? null : v;
  }

  /**
   * Spark's generated code for each function ({@code mathExpressions.scala}), with the domain
   * null built under {@code fault}, which the planted {@link FloatingFault#MATH_DOMAIN_LT} reads.
   */
  private static List<Fn> functions(FloatingFault fault) {
    double log2 = StrictMath.log(2);
    var logOfBase = new IntArith(IntOp.DIV, Overflow.WRAP,
        VarkaMathDomain.nullAtOrBelow(MathFn.LOG, X, lit(0), fault),
        VarkaMathDomain.nullAtOrBelow(MathFn.LOG, B, lit(0), fault));
    return List.of(
        call("sin", MathFn.SIN, (c, b) -> Math.sin(c), Domain.TRIG, true),
        call("cos", MathFn.COS, (c, b) -> Math.cos(c), Domain.TRIG, true),
        call("tan", MathFn.TAN, (c, b) -> Math.tan(c), Domain.TRIG, true),
        call("asin", MathFn.ASIN, (c, b) -> Math.asin(c), Domain.UNIT, false),
        call("acos", MathFn.ACOS, (c, b) -> Math.acos(c), Domain.UNIT, false),
        call("atan", MathFn.ATAN, (c, b) -> Math.atan(c), Domain.PLAIN, false),
        reciprocal("sec", MathFn.COS, (c, b) -> 1 / Math.cos(c)),
        reciprocal("csc", MathFn.SIN, (c, b) -> 1 / Math.sin(c)),
        reciprocal("cot", MathFn.TAN, (c, b) -> 1 / Math.tan(c)),
        call("exp", MathFn.EXP, (c, b) -> StrictMath.exp(c), Domain.EXP, true),
        call("expm1", MathFn.EXPM1, (c, b) -> StrictMath.expm1(c), Domain.EXP, false),
        call("cbrt", MathFn.CBRT, (c, b) -> Math.cbrt(c), Domain.PLAIN, true),
        logCall("ln", MathFn.LOG, 0.0,
            (c, b) -> nullAtOrBelow(c, 0.0, StrictMath.log(c)), Domain.LOG, true, fault),
        logCall("log10", MathFn.LOG10, 0.0,
            (c, b) -> nullAtOrBelow(c, 0.0, StrictMath.log10(c)), Domain.LOG, true, fault),
        logCall("log1p", MathFn.LOG1P, -1.0,
            (c, b) -> nullAtOrBelow(c, -1.0, StrictMath.log1p(c)), Domain.LOG1P, false, fault),
        logOver("log2", log2,
            (c, b) -> nullAtOrBelow(c, 0.0, StrictMath.log(c) / StrictMath.log(2)), fault),
        logOver("log(2.5, x)", StrictMath.log(2.5),
            (c, b) -> nullAtOrBelow(c, 0.0, StrictMath.log(c) / StrictMath.log(2.5)), fault),
        new Fn("log(b, x)", logOfBase, bits(0.0), 2,
            (c, b) -> c <= 0.0 || b <= 0.0 ? null : StrictMath.log(c) / StrictMath.log(b),
            Domain.LOG, true, true),
        new Fn("pow", new MathBinary(MathFn.POW, X, B), new long[0], 2,
            (c, y) -> StrictMath.pow(c, y), Domain.POW, true, false));
  }

  private static final List<Fn> FUNCTIONS = functions(FloatingFault.NONE);

  // -------------------------------------------------------------------------------------------
  // The comparison
  // -------------------------------------------------------------------------------------------

  /**
   * The mismatch, or null where the kernel's answer - {@code valid}, and the bits {@code got} - is
   * Spark's for {@code x} and {@code b} at {@link VarkaSameAnswer.Level#EXACT}, a NaN's raw bits
   * too (the class comment says why), and a null where Spark's code nulls.
   */
  private static @Nullable String mismatch(Fn f, double x, double b, boolean valid, long got) {
    Double want = f.spark().eval(x, b);
    String at = f.name() + "(" + hex(x) + (f.columns() == 2 ? ", " + hex(b) : "") + ")";
    if (want == null) {
      return valid ? at + ": want null, got " + hex(Double.longBitsToDouble(got)) : null;
    }
    if (!valid) {
      return at + ": want " + hex(want) + ", got null";
    }
    if (f.root() instanceof MathBinary && VarkaSameAnswer.sameAnswer(f.root(),
        Double.doubleToRawLongBits(want), got, operands(f.root(), want, x, b))) {
      // Two NaN operands: fdlibm's pow answers a sum of the two, and a method of that form
      // answered x's payload in the kernel and y's in this scalar call on one JVM (VARKA-358),
      // so Spark's own answer is either NaN: the open NaN of VarkaSameAnswer's row level.
      return null;
    }
    return Double.doubleToRawLongBits(want) == got ? null
        : at + ": want " + hex(want) + ", got " + hex(Double.longBitsToDouble(got));
  }

  /**
   * The row {@link VarkaSameAnswer}'s rule reads: Spark's answer {@code want} at {@code root},
   * the two columns' values below it, and no condition.
   */
  private static VarkaSameAnswer.IrRow operands(VarkaVectorIR root, double want, double x,
      double b) {
    return new VarkaSameAnswer.IrRow() {
      @Override public OptionalLong value(VarkaVectorIR node) {
        return node.equals(root) ? OptionalLong.of(Double.doubleToRawLongBits(want))
            : node.equals(X) ? OptionalLong.of(Double.doubleToRawLongBits(x))
            : node.equals(B) ? OptionalLong.of(Double.doubleToRawLongBits(b))
            : OptionalLong.empty();
      }

      @Override public boolean holds(VarkaVectorIR.Cond cond) {
        return false;
      }
    };
  }

  private static boolean validAt(Output out, int i) {
    return (out.validity().get(ValueLayout.JAVA_BYTE, i / 8) & (1 << (i % 8))) != 0;
  }

  private static String hex(double v) {
    return String.format(Locale.ROOT, "%s (0x%016x)", v, Double.doubleToRawLongBits(v));
  }

  // -------------------------------------------------------------------------------------------
  // The draws
  // -------------------------------------------------------------------------------------------

  /** Draw {@code d} of {@code f}'s sampled inputs, as raw bits. */
  private static long draw(Fn f, int d, SplittableRandom rnd) {
    return switch (d) {
      case 0 -> rnd.nextLong();
      case 1 -> Double.doubleToRawLongBits(f.domain() == Domain.UNIT ? rnd.nextDouble(-1.0, 1.0)
          : rnd.nextDouble(-1e6, 1e6));
      case 2 -> Double.doubleToRawLongBits(rnd.nextDouble(-2.0, 2.0));
      case 3 -> Double.doubleToRawLongBits((rnd.nextBoolean() ? 1 : -1)
          * Math.scalb(1.0 + rnd.nextDouble(), -rnd.nextInt(61)));
      case 4 -> Double.doubleToRawLongBits(ownDraw(f.domain(), rnd));
      default -> throw new IllegalArgumentException("draw " + d);
    };
  }

  /**
   * {@code pow}'s own hard pairs: {@code x} within {@code 2^-20} of 1 with {@code |y|} past
   * {@code 2^31}, fdlibm's weak branch; a negative {@code x} with an integral {@code y} and with a
   * non-integral one; and an integral {@code y} over any {@code x}.
   */
  private static double[] powDraw(SplittableRandom rnd) {
    double sign = rnd.nextBoolean() ? 1.0 : -1.0;
    return switch (rnd.nextInt(4)) {
      case 0 -> new double[] {1.0 + sign * Math.scalb(rnd.nextDouble(), -20 - rnd.nextInt(33)),
          (rnd.nextBoolean() ? 1.0 : -1.0) * Math.scalb(1.0 + rnd.nextDouble(),
              31 + rnd.nextInt(30))};
      case 1 -> new double[] {-rnd.nextDouble(0.0, 1e3), rnd.nextInt(-64, 65)};
      case 2 -> new double[] {-rnd.nextDouble(0.0, 1e3), rnd.nextDouble(-64.0, 64.0)};
      default -> new double[] {Double.longBitsToDouble(rnd.nextLong()), rnd.nextInt(-1100, 1100)};
    };
  }

  /** The function's own hard inputs. */
  private static double ownDraw(Domain domain, SplittableRandom rnd) {
    return switch (domain) {
      // The trigonometric functions' hard reductions.
      case TRIG -> (rnd.nextBoolean() ? 1 : -1)
          * Math.pow(10.0, rnd.nextDouble(Math.log10(0x1p20), 300.0));
      // The inverse functions' edges near +-1.
      case UNIT, PLAIN -> Math.copySign(1.0 - Math.scalb(rnd.nextDouble(), -rnd.nextInt(53)),
          rnd.nextBoolean() ? 1.0 : -1.0);
      // pow's own draw is a pair, powDraw's, which sampledMismatches takes in place of this.
      case POW -> throw new IllegalArgumentException("pow's own draw is a pair: powDraw");
      // From below exp's subnormal edge, -708.05, and its underflow to zero, to past its
      // overflow at 709.78.
      case EXP -> rnd.nextDouble(-750.0, 712.0);
      // Subnormals, [0.5, 2], the non-positive half and both zeros.
      case LOG -> switch (rnd.nextInt(4)) {
        case 0 -> Double.longBitsToDouble(rnd.nextLong() & 0x000fffffffffffffL);
        case 1 -> rnd.nextDouble(0.5, 2.0);
        case 2 -> -rnd.nextDouble(0.0, 1e6);
        default -> rnd.nextBoolean() ? 0.0 : -0.0;
      };
      // The same about log1p's asymptote, -1.
      case LOG1P -> switch (rnd.nextInt(4)) {
        case 0 -> -1.0 + Math.scalb(rnd.nextDouble(), -rnd.nextInt(53));
        case 1 -> rnd.nextDouble(-0.5, 1.0);
        case 2 -> -1.0 - rnd.nextDouble(0.0, 1e6);
        default -> rnd.nextBoolean() ? -1.0 : Math.nextDown(-1.0);
      };
    };
  }

  private static final int DRAWS = 5;

  /**
   * The number of inputs of {@code f}'s sampled differential its kernel answers otherwise than
   * Spark's code, the first few named in {@code firstFew}.
   */
  private static long sampledMismatches(Fn f, VarkaEmitOptions options, int samples,
      List<String> firstFew) {
    assumeTrue(Lane.emitLanes(options, Lane.DOUBLE) != 0, "no baked lane count for the call");
    var loaded = VarkaEmitterFixtures.load(
        VarkaEmitterFixtures.emit(List.of(f.root()), f.columns(), f.lits().length, options));
    long mismatches = 0;
    var rnd = new SplittableRandom(0x356L * 31 + f.name().hashCode());
    try (Arena arena = Arena.ofConfined()) {
      var cols = new Col[f.columns()];
      for (int c = 0; c < cols.length; c++) {
        cols[c] = new Col(VarkaEmitterFixtures.alloc(arena, BATCH * 8L),
            VarkaEmitterFixtures.alloc(arena, BATCH / 8), 0);
      }
      Output out = VarkaEmitterFixtures.makeLongOutput(arena, BATCH);
      int perDraw = Math.max(1, samples / DRAWS / BATCH);
      for (int d = 0; d < DRAWS; d++) {
        for (int b = 0; b < perDraw; b++) {
          for (int i = 0; i < BATCH; i++) {
            if (f.domain() == Domain.POW && d == 4) {
              double[] xy = powDraw(rnd);
              cols[0].data().set(ValueLayout.JAVA_DOUBLE, i * 8L, xy[0]);
              cols[1].data().set(ValueLayout.JAVA_DOUBLE, i * 8L, xy[1]);
            } else {
              for (Col col : cols) {
                col.data().set(ValueLayout.JAVA_LONG, i * 8L, draw(f, d, rnd));
              }
            }
          }
          run(loaded.kernel(), f, cols, out, BATCH);
          for (int i = 0; i < BATCH; i++) {
            String why = mismatch(f, valueAt(cols[0], i),
                cols.length == 2 ? valueAt(cols[1], i) : 0.0, validAt(out, i),
                out.data().get(ValueLayout.JAVA_LONG, i * 8L));
            if (why != null && mismatches++ < 5) {
              firstFew.add(why);
            }
          }
        }
      }
    } finally {
      loaded.loader().release();
    }
    return mismatches;
  }

  private static double valueAt(Col col, int i) {
    return col.data().get(ValueLayout.JAVA_DOUBLE, i * 8L);
  }

  private static void run(VarkaFusedKernel kernel, Fn f, Col[] cols, Output out, int length) {
    long[] data = new long[cols.length];
    long[] validity = new long[cols.length];
    int[] nulls = new int[cols.length];
    for (int c = 0; c < cols.length; c++) {
      data[c] = cols[c].data().address();
      validity[c] = cols[c].validityAddress(length);
      nulls[c] = cols[c].nullCount();
    }
    int status = kernel.run(data, validity, nulls, new long[] {out.data().address()},
        new long[] {out.validity().address()}, new int[0], f.lits(), length,
        VarkaEmitterTestSupport.scratch(kernel, length));
    if (status != 0) {
      fail(f.name() + ": the kernel declined a batch, status " + status);
    }
  }

  @Test
  @DisplayName("each function's kernel answers Spark's bits on every sampled input")
  void sampledDifferential() {
    var failures = new ArrayList<String>();
    for (Fn f : FUNCTIONS) {
      var firstFew = new ArrayList<String>();
      long n = sampledMismatches(f, VarkaMatrixRuns.BASE, SAMPLES, firstFew);
      if (n != 0) {
        failures.add(n + " mismatches, " + firstFew);
      }
    }
    assertEquals(List.of(), failures);
  }

  // -------------------------------------------------------------------------------------------
  // The fixed vectors
  // -------------------------------------------------------------------------------------------

  /**
   * The fixed vectors of every function: SLEEF's special values ({@code READING_SLEEF.md} 6.3),
   * each with its 64 neighbours each way, Spark's edges for these functions (the zeros of
   * {@code csc} and {@code cot}, {@code +-1} of {@code asin} and {@code acos}, the multiples of
   * {@code pi / 2} near which {@code tan} and {@code sec} blow up, {@code 2^20} where the
   * reduction changes, the log family's asymptotes {@code 0.0}, {@code -0.0} and {@code -1.0}, and
   * {@code exp}'s overflow threshold, its subnormal edge and its underflow to zero), and NaN
   * payloads of both signs, quiet and signalling.
   */
  static long[] fixedVectors() {
    double[] edges = {0.0, -0.0, 1.0, -1.0, Double.MAX_VALUE, -Double.MAX_VALUE,
        Double.MIN_NORMAL, -Double.MIN_NORMAL, Double.MIN_VALUE, -Double.MIN_VALUE,
        Math.PI / 2, -Math.PI / 2, Math.PI, -Math.PI, 3 * Math.PI / 2, 0x1p20, -0x1p20,
        Math.PI / 4, 0x1p-28, 1e300, -1e300, 709.782712893384, -708.0498449419841,
        -745.1332191019411, 2.0, 0.5, 0x1p31, -0x1p31};
    var bits = new ArrayList<Long>();
    for (double e : edges) {
      long b = Double.doubleToRawLongBits(e);
      // A neighbour past a zero or an infinity steps into the other sign's or the NaNs'
      // patterns, which are inputs too.
      for (int k = -64; k <= 64; k++) {
        bits.add(b + k);
      }
    }
    bits.add(Double.doubleToRawLongBits(Double.POSITIVE_INFINITY));
    bits.add(Double.doubleToRawLongBits(Double.NEGATIVE_INFINITY));
    for (long nan : new long[] {0x7ff8000000000000L, 0xfff8000000000000L, 0x7ff8000000000001L,
        0xfff800000000beefL, 0x7ff0000000000001L, 0xfff0000000000001L, 0x7ff4000000000000L,
        0x7fffffffffffffffL, 0xffffffffffffffffL}) {
      bits.add(nan);
    }
    return bits.stream().mapToLong(Long::longValue).toArray();
  }

  /** The null patterns the emitter suites share: none, every fifth, every odd row, all. */
  private static final List<IntPredicate> NULL_PATTERNS = List.of(
      i -> false, i -> i % 5 == 0, i -> i % 2 == 1, i -> true);

  @Test
  @DisplayName("each function's kernel answers Spark's bits on the fixed vectors, over nulls, "
      + "lengths and both widths")
  void fixedVectorsOverNullsAndWidths() {
    assumeTrue(Lane.emitLanes(VarkaMatrixRuns.BASE, Lane.DOUBLE) != 0, "no baked lane count");
    var failures = new ArrayList<String>();
    for (Fn f : FUNCTIONS) {
      failures.addAll(fixedVectorFailures(f));
    }
    assertEquals(List.of(), failures.subList(0, Math.min(10, failures.size())));
  }

  /**
   * {@code f}'s mismatches on the fixed vectors, over every null pattern, three lengths and two
   * and eight lanes. A second column, the base of {@code log(b, x)} or {@code y} of {@code pow},
   * holds the vectors in reverse, nulled by the same pattern one row on, so each edge meets the
   * others as the second operand.
   */
  private static List<String> fixedVectorFailures(Fn f) {
    long[] vectors = fixedVectors();
    var failures = new ArrayList<String>();
    for (int lanes : new int[] {2, 8}) {
      var loaded = VarkaEmitterFixtures.load(VarkaEmitterFixtures.emit(List.of(f.root()),
          f.columns(), f.lits().length, VarkaMatrixRuns.BASE.withLanesOverride(lanes)));
      try {
        for (int length : new int[] {1, 7, vectors.length}) {
          for (int p = 0; p < NULL_PATTERNS.size(); p++) {
            IntPredicate isNull = NULL_PATTERNS.get(p);
            IntPredicate baseNull = i -> f.columns() == 2 && isNull.test(i + 1);
            int n = length;
            try (Arena arena = Arena.ofConfined()) {
              Col x = VarkaEmitterFixtures.makeLongInput(arena, length, isNull,
                  i -> vectors[i], true);
              Col[] cols = f.columns() == 1 ? new Col[] {x} : new Col[] {x,
                  VarkaEmitterFixtures.makeLongInput(arena, length, baseNull,
                      i -> vectors[n - 1 - i], true)};
              Output out = VarkaEmitterFixtures.makeLongOutput(arena, length);
              run(loaded.kernel(), f, cols, out, length);
              for (int i = 0; i < length; i++) {
                boolean valid = validAt(out, i);
                String where = f.name() + " at " + lanes + " lanes, length " + length
                    + ", pattern " + p + ", row " + i;
                if (isNull.test(i) || baseNull.test(i)) {
                  if (valid) {
                    failures.add(where + ": a null operand answered valid");
                  }
                  continue;
                }
                String why = mismatch(f, Double.longBitsToDouble(vectors[i]),
                    f.columns() == 2 ? Double.longBitsToDouble(vectors[n - 1 - i]) : 0.0, valid,
                    out.data().get(ValueLayout.JAVA_LONG, i * 8L));
                if (why != null) {
                  failures.add(where + ": " + why);
                }
              }
            }
          }
        }
      } finally {
        loaded.loader().release();
      }
    }
    return failures;
  }

  /**
   * fdlibm's special pairs of {@code pow}, one or more for each of the nineteen cases of the
   * table in JDK 25's {@code FdLibm.Pow}, and its fast paths at {@code y = 2} and {@code 0.5}:
   * the cases the reversed second column of the fixed vectors pairs only by chance.
   */
  private static final double[][] POW_SPECIAL = {
      // 1. anything ** 0 is 1; 2. anything ** 1 is itself.
      {Double.NaN, 0.0}, {Double.POSITIVE_INFINITY, -0.0}, {-0.0, 0.0}, {-5.0, -0.0},
      {-2.5, 1.0}, {-0.0, 1.0}, {Double.NEGATIVE_INFINITY, 1.0}, {Double.MIN_VALUE, 1.0},
      // 3. anything ** NaN is NaN, 1 and -1 included (Java's, not C99's 1); 4. NaN ** y.
      {2.0, Double.NaN}, {1.0, Double.NaN}, {-1.0, Double.NaN}, {0.0, Double.NaN},
      {Double.NaN, 2.0}, {Double.NaN, -0.5}, {Double.NaN, Double.POSITIVE_INFINITY},
      // 5 to 8: |x| > 1 and |x| < 1 against the infinities.
      {1.5, Double.POSITIVE_INFINITY}, {-1.5, Double.POSITIVE_INFINITY},
      {Double.MAX_VALUE, Double.POSITIVE_INFINITY}, {1.5, Double.NEGATIVE_INFINITY},
      {-3.0, Double.NEGATIVE_INFINITY}, {0.5, Double.POSITIVE_INFINITY},
      {-0.5, Double.POSITIVE_INFINITY}, {Double.MIN_VALUE, Double.POSITIVE_INFINITY},
      {0.5, Double.NEGATIVE_INFINITY}, {-0.25, Double.NEGATIVE_INFINITY},
      {Math.nextUp(1.0), Double.POSITIVE_INFINITY}, {Math.nextDown(1.0), Double.NEGATIVE_INFINITY},
      // 9. +-1 ** +-inf is NaN.
      {1.0, Double.POSITIVE_INFINITY}, {1.0, Double.NEGATIVE_INFINITY},
      {-1.0, Double.POSITIVE_INFINITY}, {-1.0, Double.NEGATIVE_INFINITY},
      // 10 to 14: the zeros, against positive, negative, odd and even integers and the
      // infinities.
      {0.0, 3.0}, {0.0, 0.5}, {0.0, Double.POSITIVE_INFINITY}, {0.0, Double.MIN_VALUE},
      {-0.0, 2.0}, {-0.0, 0.5}, {-0.0, Double.POSITIVE_INFINITY}, {-0.0, 2.5},
      {0.0, -3.0}, {0.0, -0.5}, {0.0, Double.NEGATIVE_INFINITY}, {-0.0, -2.0}, {-0.0, -0.5},
      {-0.0, Double.NEGATIVE_INFINITY}, {-0.0, 3.0}, {-0.0, -3.0}, {-0.0, -1.0},
      // 15 to 17: the infinities as the base.
      {Double.POSITIVE_INFINITY, 2.0}, {Double.POSITIVE_INFINITY, 0.5},
      {Double.POSITIVE_INFINITY, Double.MIN_VALUE}, {Double.POSITIVE_INFINITY, -2.0},
      {Double.POSITIVE_INFINITY, -0.5}, {Double.NEGATIVE_INFINITY, 3.0},
      {Double.NEGATIVE_INFINITY, 2.0}, {Double.NEGATIVE_INFINITY, -3.0},
      {Double.NEGATIVE_INFINITY, -2.0}, {Double.NEGATIVE_INFINITY, 0.5},
      {Double.NEGATIVE_INFINITY, -0.5},
      // 18. a negative base to an integer: odd, even, past 2^53 (even) and overflowing.
      {-2.0, 3.0}, {-2.0, 4.0}, {-3.0, -3.0}, {-1.5, 0x1p53}, {-2.0, 1075.0}, {-2.0, -1075.0},
      {-1.0, 0x1p60}, {-1.0, 0x1p52 + 1},
      // 19. a negative finite base to a non-integer is NaN.
      {-2.0, 0.5}, {-2.0, 2.5}, {-0.5, 1.5}, {-Double.MIN_VALUE, 0.5},
      // The fast paths: y = 2 is x * x, y = 0.5 a square root but at -inf and -0.0.
      {-3.0, 2.0}, {Double.MAX_VALUE, 2.0}, {2.0, 0.5}, {Double.MIN_VALUE, 0.5},
      // An integral result that must be exact, and the edges of overflow and underflow.
      {3.0, 20.0}, {2.0, 1023.0}, {2.0, 1024.0}, {2.0, -1074.0}, {2.0, -1075.0}};

  @Test
  @DisplayName("pow answers StrictMath.pow's bits on fdlibm's special pairs at two and eight "
      + "lanes")
  void powSpecialPairs() {
    assumeTrue(Lane.emitLanes(VarkaMatrixRuns.BASE, Lane.DOUBLE) != 0, "no baked lane count");
    Fn pow = FUNCTIONS.stream().filter(f -> f.name().equals("pow")).findFirst().orElseThrow();
    var failures = new ArrayList<String>();
    int n = POW_SPECIAL.length;
    for (int lanes : new int[] {2, 8}) {
      var loaded = VarkaEmitterFixtures.load(VarkaEmitterFixtures.emit(List.of(pow.root()), 2, 0,
          VarkaMatrixRuns.BASE.withLanesOverride(lanes)));
      try (Arena arena = Arena.ofConfined()) {
        Col[] cols = {
            VarkaEmitterFixtures.makeLongInput(arena, n, i -> false,
                i -> Double.doubleToRawLongBits(POW_SPECIAL[i][0]), true),
            VarkaEmitterFixtures.makeLongInput(arena, n, i -> false,
                i -> Double.doubleToRawLongBits(POW_SPECIAL[i][1]), true)};
        Output out = VarkaEmitterFixtures.makeLongOutput(arena, n);
        run(loaded.kernel(), pow, cols, out, n);
        for (int i = 0; i < n; i++) {
          String why = mismatch(pow, POW_SPECIAL[i][0], POW_SPECIAL[i][1], validAt(out, i),
              out.data().get(ValueLayout.JAVA_LONG, i * 8L));
          if (why != null) {
            failures.add(lanes + " lanes: " + why);
          }
        }
      } finally {
        loaded.loader().release();
      }
    }
    assertEquals(List.of(), failures);
  }

  // -------------------------------------------------------------------------------------------
  // The planted faults
  // -------------------------------------------------------------------------------------------

  /**
   * The differential's power, by planting the other class's method in the per-lane call
   * (row 504's rule). On x86 {@code Math.sin}, {@code cos} and {@code tan} are the host's
   * intrinsics and differ from {@code StrictMath}'s on thousands of inputs in a million
   * ({@code VARKA-356.md} item 0.3), and so are {@code Math.exp}, {@code log}, {@code log10}
   * and {@code cbrt} against Spark's {@code StrictMath.exp}, {@code log} and {@code log10} and
   * {@code Math.cbrt}, and {@code Math.pow} against {@code StrictMath.pow}, so the differential
   * must fail for those and the compositions over them; {@code asin}, {@code acos}, {@code atan},
   * {@code expm1} and {@code log1p} are {@code StrictMath}'s whichever class is called, so the
   * planted call changes nothing for them.
   * Asserted on x86 only: on another host the classes may agree.
   */
  @Test
  @DisplayName("a call of the other class's method fails the differential where the classes "
      + "differ")
  void otherClassIsCaught() {
    assumeTrue(VarkaHostIsa.isX86(System.getProperty("os.arch")), "the classes differ on x86");
    VarkaEmitOptions planted =
        VarkaMatrixRuns.BASE.withFloatingFault(VarkaEmitOptions.FloatingFault.MATH_OTHER_CLASS);
    for (Fn f : FUNCTIONS) {
      int samples = f.root() instanceof MathBinary ? PLANT_SAMPLES : SAMPLES;
      long n = sampledMismatches(f, planted, samples, new ArrayList<>());
      assertTrue(f.classesDiffer() ? n > 0 : n == 0, f.name() + " under the planted call: " + n
          + " mismatches");
    }
  }

  /**
   * The log family's null at {@code x < t} instead of Spark's {@code x <= t}: at the asymptote
   * itself, {@code ln(0.0)}, {@code ln(-0.0)} and {@code log1p(-1.0)}, the call answers an
   * infinity where Spark answers null, which the fixed vectors hold. Every function with the
   * domain null must fail them, and every other function, built without one, must not move.
   */
  @Test
  @DisplayName("the log family's null at < instead of <= fails the fixed vectors")
  void domainAtLessThanIsCaught() {
    assumeTrue(Lane.emitLanes(VarkaMatrixRuns.BASE, Lane.DOUBLE) != 0, "no baked lane count");
    for (Fn f : functions(FloatingFault.MATH_DOMAIN_LT)) {
      int n = fixedVectorFailures(f).size();
      assertTrue(f.logNull() ? n > 0 : n == 0, f.name() + " under the planted null: " + n
          + " mismatches");
    }
  }

  // -------------------------------------------------------------------------------------------
  // No allocation a batch
  // -------------------------------------------------------------------------------------------

  /**
   * The per-lane call keeps the vector in registers: a kernel compiled by C2 allocates nothing a
   * batch, where a vector boxed around the scalar call would allocate one object a lane. The
   * kernel is warmed until a measured window stays under the sampler's allowance, a few times at
   * most.
   */
  @Test
  @Order(1)
  @DisplayName("a warmed per-lane call kernel allocates nothing a batch")
  void noAllocationPerBatch() {
    assumeTrue(VarkaAllocationSampler.supported(), "thread allocation accounting unavailable");
    // First, at the preferred species alone: a second species in the JVM boxes every vector.
    assumeTrue(VarkaMatrixRuns.BASE.lanesOverride() == 0, "a lane count other than the host's");
    Fn f = FUNCTIONS.get(0);
    var loaded = VarkaEmitterFixtures.load(
        VarkaEmitterFixtures.emit(List.of(f.root()), 1, 0, VarkaMatrixRuns.BASE));
    try (Arena arena = Arena.ofConfined()) {
      int rows = 1024;
      MemorySegment in = VarkaEmitterFixtures.alloc(arena, rows * 8L);
      for (int i = 0; i < rows; i++) {
        in.set(ValueLayout.JAVA_DOUBLE, i * 8L, i * 0.001);
      }
      Output out = VarkaEmitterFixtures.makeLongOutput(arena, rows);
      VarkaFusedKernel kernel = loaded.kernel();
      // The arguments built once, so that the window measures the kernel alone.
      long[] data = {in.address()};
      long[] validity = {0L};
      int[] nulls = {0};
      long[] outData = {out.data().address()};
      long[] outValidity = {out.validity().address()};
      int[] ints = new int[0];
      long[] longs = new long[0];
      long scratch = VarkaEmitterTestSupport.scratch(kernel, rows);
      long allocated = Long.MAX_VALUE;
      int batches = 2000;
      for (int round = 0; round < 6 && VarkaAllocationSampler.suspect(allocated,
          rows * batches); round++) {
        for (int b = 0; b < 20000; b++) {
          kernel.run(data, validity, nulls, outData, outValidity, ints, longs, rows, scratch);
        }
        long before = VarkaAllocationSampler.allocatedBytes();
        for (int b = 0; b < batches; b++) {
          kernel.run(data, validity, nulls, outData, outValidity, ints, longs, rows, scratch);
        }
        allocated = VarkaAllocationSampler.allocatedBytes() - before;
      }
      assertTrue(!VarkaAllocationSampler.suspect(allocated, rows * batches),
          allocated + " bytes over " + batches + " batches");
    } finally {
      loaded.loader().release();
    }
  }
}
