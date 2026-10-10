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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntBinaryOperator;
import java.util.function.LongBinaryOperator;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaDivisionLowering.MulHiMagic;

/**
 * The rendered files under {@code sql/varka/proofs/} (VARKA-240): the proofs whose constants come
 * from the code, and the check of the prelude against the JVM. {@code VarkaProofFilesSuite} fails
 * when a committed file differs from what this renders, so a proof cannot go on proving a constant
 * the code no longer has. {@code java.smt2}, the prelude, is written by hand and is not here.
 *
 * <p>Every check is an {@code (echo "<what it shows>: expect sat|unsat")} followed by its
 * {@code (check-sat)} between {@code push} and {@code pop}; {@code dev/varka_prove.sh} pairs each
 * verdict with the expectation before it.
 */
final class VarkaProofFiles {

  private VarkaProofFiles() {
  }

  /**
   * The int-lane constant divisors the multiply-high proof and {@code VarkaEmitterDivisionSuite}'s
   * sweep and matrix cover: {@code extract(YEAR FROM ym)}'s 12, the fuzz grammar's list
   * ({@code VarkaIrGrammar.ConstDivideDivisors}), the {@code TIME} split form's 60 and 3600, and
   * two edges of {@link VarkaDivisionLowering#signedMagic}: 196611, the first divisor whose shift
   * Theorem 5.1 raises past the book's, and {@code Integer.MIN_VALUE}, the magnitude 2^31.
   */
  static final List<Integer> INT_DIVISORS =
      List.of(12, 2, 3, 7, 100, -3, -12, 60, 3600, 196611, Integer.MIN_VALUE);

  /** Each rendered file's name under {@code sql/varka/proofs/}, and its text. */
  static Map<String, String> render() {
    var files = new LinkedHashMap<String, String>();
    files.put("java_check.smt2", javaCheck());
    files.put("int_mulhi_divide.smt2", intMulHiDivide());
    return files;
  }

  private static final String LICENSE = """
      ; Licensed to the Apache Software Foundation (ASF) under one or more
      ; contributor license agreements.  See the NOTICE file distributed with
      ; this work for additional information regarding copyright ownership.
      ; The ASF licenses this file to You under the Apache License, Version 2.0
      ; (the "License"); you may not use this file except in compliance with
      ; the License.  You may obtain a copy of the License at
      ;
      ;    http://www.apache.org/licenses/LICENSE-2.0
      ;
      ; Unless required by applicable law or agreed to in writing, software
      ; distributed under the License is distributed on an "AS IS" BASIS,
      ; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
      ; See the License for the specific language governing permissions and
      ; limitations under the License.

      """;

  private static final String RENDERED = """
      ;
      ; Rendered by VarkaProofFiles. VarkaProofFilesSuite fails when this file differs from its
      ; rendering, and VARKA_PROOFS_REGEN=true build/sbt 'catalyst/testOnly *VarkaProofFilesSuite'
      ; rewrites it. Run by dev/varka_prove.sh, which inserts java.smt2 after the set-logic line.

      (set-logic QF_NIA)
      """;

  // -----------------------------------------------------------------------------------------
  // The multiply-high division (VARKA-149), proven per divisor.
  // -----------------------------------------------------------------------------------------

  /**
   * {@code int_mulhi_divide.smt2}: for every divisor in {@link #INT_DIVISORS}, the form
   * {@code emitMulHiDivide} emits, with the pair {@code signedMagic} derives, is Java's
   * {@code n / d} for every int {@code n} - and is not with the multiplier lowered by one or the
   * shift raised by one, which shows the statement tells the real constants from their
   * neighbours.
   */
  static String intMulHiDivide() {
    var out = new StringBuilder(LICENSE);
    out.append("""
        ; The int lane's multiply-high division (VARKA-149) is exact: for each divisor d below and
        ; every int n, what the lanes of VarkaDivisionLowering.emitMulHiDivide compute is Java's
        ; n / d. Per lane, with (Mu, shift) = VarkaDivisionLowering.signedMagic(|d|):
        ;
        ;   long p = (long) n * Mu;        I2L, then LongVector.mul by the multiplier
        ;   int q = (int) (p >> shift);    ASHR by the shift, then L2I
        ;   q = q + (n >>> 31);            the dividend's sign bit, LSHR 31, added
        ;   if (d < 0) q = q * -1;         IntVector.mul(-1) for a negative divisor
        ;
        ; Each divisor has three checks: the form is n / d for every int n (unsat: no n where it
        ; is not), and it is not with the multiplier lowered by one, or with the shift raised by
        ; one (sat: a counterexample exists), which shows the statement can tell the derived
        ; constants from their neighbours.
        """);
    out.append(RENDERED);
    out.append("""

        (define-fun mulhi.divide ((n Int) (mu Int) (shift Int) (negate Bool)) Int
          (let ((q (jint.add (l2i (jlong.shr (jlong.mul (i2l n) mu) shift)) (jint.ushr n 31))))
            (ite negate (jint.mul q (- 1)) q)))

        (declare-const n Int)
        (assert (jint.in n))
        """);
    check(out, "the dividend's domain admits Integer.MIN_VALUE", "sat",
        List.of("(assert (= n jint.MIN))"));
    check(out, "the dividend's domain admits Integer.MAX_VALUE", "sat",
        List.of("(assert (= n jint.MAX))"));
    for (int d : INT_DIVISORS) {
      MulHiMagic magic = VarkaDivisionLowering.signedMagicForTest(Math.abs((long) d));
      String negate = d < 0 ? "true" : "false";
      out.append("\n; d = ").append(d).append(": signedMagic(").append(magic.divisor())
          .append(") = (").append(magic.multiplier()).append(", ").append(magic.shift())
          .append(")\n");
      check(out, "d = " + d + ": the form is n / d for every int n", "unsat",
          List.of(notDivides(magic.multiplier(), magic.shift(), negate, d)));
      check(out, "d = " + d + ": with the multiplier lowered by one it is not", "sat",
          List.of(notDivides(magic.multiplier() - 1, magic.shift(), negate, d)));
      check(out, "d = " + d + ": with the shift raised by one it is not", "sat",
          List.of(notDivides(magic.multiplier(), magic.shift() + 1, negate, d)));
    }
    return out.toString();
  }

  private static String notDivides(long mu, int shift, String negate, int d) {
    return "(assert (not (= (mulhi.divide n " + mu + " " + shift + " " + negate + ") (jint.div n "
        + lit(d) + "))))";
  }

  // -----------------------------------------------------------------------------------------
  // The prelude against the JVM.
  // -----------------------------------------------------------------------------------------

  private static final int[] INTS =
      {Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -7, -1, 0, 1, 7, Integer.MAX_VALUE};

  private static final int[] INT_DIVISORS_CHECKED =
      {Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -7, -1, 1, 7, Integer.MAX_VALUE};

  private static final long[] LONGS = {Long.MIN_VALUE, Long.MIN_VALUE + 1, -(1L << 32), -7, -1, 0,
      1, 7, 1L << 32, Long.MAX_VALUE};

  private static final long[] LONG_DIVISORS_CHECKED = {Long.MIN_VALUE, Long.MIN_VALUE + 1,
      -(1L << 32), -7, -1, 1, 7, 1L << 32, Long.MAX_VALUE};

  /** Shift counts: in range, at and past the mask, and negative, whose low bits are all set. */
  private static final int[] COUNTS = {-1, 0, 1, 31, 32, 33, 63, 64};

  private static final long[] LONG_COUNTS = {-1, 0, 1, 31, 32, 33, 63, 64};

  /**
   * {@code java_check.smt2}: each of the prelude's definitions gives what the JVM gives, over
   * boundary operands - each type's extremes and the values next to them, small values of both
   * signs, and shift counts whose masked-off bits are set. The expected values are computed here,
   * by Java's own operators, while rendering. One check per operator, which names it when it
   * fails.
   */
  static String javaCheck() {
    var out = new StringBuilder(LICENSE);
    out.append("""
        ; The prelude, java.smt2, against the JVM: for each operator it defines, a check that its
        ; definition gives what Java gives over boundary operands - each type's extremes and the
        ; values next to them, small values of both signs, and shift counts whose masked-off bits
        ; are set. The expected values were computed by Java's own operators while rendering, so a
        ; definition with the right name and the wrong meaning fails here before a proof can rest
        ; on it. Each check is unsat when every equation in it holds.
        """);
    out.append(RENDERED);
    range(out, "jint.in", "int", Integer.MIN_VALUE, Integer.MAX_VALUE);
    range(out, "jlong.in", "long", Long.MIN_VALUE, Long.MAX_VALUE);
    ints(out, "jint.add", "int +", INTS, (a, b) -> a + b);
    ints(out, "jint.sub", "int -", INTS, (a, b) -> a - b);
    ints(out, "jint.mul", "int *", INTS, (a, b) -> a * b);
    ints(out, "jint.div", "int /", INT_DIVISORS_CHECKED, (a, b) -> a / b);
    ints(out, "jint.rem", "int %", INT_DIVISORS_CHECKED, (a, b) -> a % b);
    ints(out, "jint.floorDiv", "Math.floorDiv on ints", INT_DIVISORS_CHECKED, Math::floorDiv);
    ints(out, "jint.floorMod", "Math.floorMod on ints", INT_DIVISORS_CHECKED, Math::floorMod);
    ints(out, "jint.shl", "int <<, its count masked", COUNTS, (a, s) -> a << s);
    ints(out, "jint.shr", "int >>, its count masked", COUNTS, (a, s) -> a >> s);
    ints(out, "jint.ushr", "int >>>, its count masked", COUNTS, (a, s) -> a >>> s);
    var intUnary = new ArrayList<String>();
    for (int a : INTS) {
      intUnary.add(eq("jint.neg", lit(a), lit(-a)));
      intUnary.add(eq("jint.not", lit(a), lit(~a)));
      intUnary.add(eq("i2l", lit(a), lit((long) a)));
    }
    equations(out, "jint.neg, jint.not and i2l are Java's int -, int ~ and (long)", intUnary);
    longs(out, "jlong.add", "long +", LONGS, (a, b) -> a + b);
    longs(out, "jlong.sub", "long -", LONGS, (a, b) -> a - b);
    longs(out, "jlong.mul", "long *", LONGS, (a, b) -> a * b);
    longs(out, "jlong.div", "long /", LONG_DIVISORS_CHECKED, (a, b) -> a / b);
    longs(out, "jlong.rem", "long %", LONG_DIVISORS_CHECKED, (a, b) -> a % b);
    longs(out, "jlong.floorDiv", "Math.floorDiv on longs", LONG_DIVISORS_CHECKED, Math::floorDiv);
    longs(out, "jlong.floorMod", "Math.floorMod on longs", LONG_DIVISORS_CHECKED, Math::floorMod);
    longs(out, "jlong.shl", "long <<, its count masked", LONG_COUNTS, (a, s) -> a << s);
    longs(out, "jlong.shr", "long >>, its count masked", LONG_COUNTS, (a, s) -> a >> s);
    longs(out, "jlong.ushr", "long >>>, its count masked", LONG_COUNTS, (a, s) -> a >>> s);
    var longUnary = new ArrayList<String>();
    for (long a : LONGS) {
      longUnary.add(eq("jlong.neg", lit(a), lit(-a)));
      longUnary.add(eq("jlong.not", lit(a), lit(~a)));
    }
    for (long a : new long[] {Long.MIN_VALUE, -(1L << 32) - 5, -(1L << 31) - 1, -(1L << 31), -1,
        0, (1L << 31) - 1, 1L << 31, (1L << 32) + 5, Long.MAX_VALUE}) {
      longUnary.add(eq("l2i", lit(a), lit((int) a)));
    }
    equations(out, "jlong.neg, jlong.not and l2i are Java's long -, long ~ and (int)", longUnary);
    return out.toString();
  }

  private static void range(StringBuilder out, String in, String type, long min, long max) {
    BigInteger below = BigInteger.valueOf(min).subtract(BigInteger.ONE);
    BigInteger above = BigInteger.valueOf(max).add(BigInteger.ONE);
    var eqs = List.of("(" + in + " " + lit(min) + ")", "(" + in + " " + lit(max) + ")",
        "(not (" + in + " " + lit(below) + "))", "(not (" + in + " " + lit(above) + "))");
    equations(out, in + " holds at the " + type + " range's ends and not one past them", eqs);
  }

  private static void ints(StringBuilder out, String name, String java, int[] rights,
      IntBinaryOperator op) {
    var eqs = new ArrayList<String>();
    for (int a : INTS) {
      for (int b : rights) {
        eqs.add(eq(name, lit(a) + " " + lit(b), lit(op.applyAsInt(a, b))));
      }
    }
    equations(out, name + " is Java's " + java, eqs);
  }

  private static void longs(StringBuilder out, String name, String java, long[] rights,
      LongBinaryOperator op) {
    var eqs = new ArrayList<String>();
    for (long a : LONGS) {
      for (long b : rights) {
        eqs.add(eq(name, lit(a) + " " + lit(b), lit(op.applyAsLong(a, b))));
      }
    }
    equations(out, name + " is Java's " + java, eqs);
  }

  private static String eq(String name, String args, String expected) {
    return "(= (" + name + " " + args + ") " + expected + ")";
  }

  /** One check that every equation holds: unsat for the negation of their conjunction. */
  private static void equations(StringBuilder out, String what, List<String> eqs) {
    var lines = new ArrayList<String>();
    lines.add("(assert (not (and");
    for (String e : eqs) {
      lines.add("  " + e);
    }
    lines.add(")))");
    check(out, what + ", on " + eqs.size() + " operand sets", "unsat", lines);
  }

  // -----------------------------------------------------------------------------------------
  // Shared.
  // -----------------------------------------------------------------------------------------

  private static void check(StringBuilder out, String what, String expect, List<String> body) {
    out.append("\n(echo \"").append(what).append(": expect ").append(expect).append("\")\n");
    out.append("(push 1)\n");
    for (String line : body) {
      out.append(line).append('\n');
    }
    out.append("(check-sat)\n(pop 1)\n");
  }

  /** An SMT-LIB integer literal: a negative one is {@code (- n)}. */
  static String lit(long v) {
    return v >= 0 ? Long.toString(v) : "(- " + Long.toString(v).substring(1) + ")";
  }

  private static String lit(BigInteger v) {
    return v.signum() >= 0 ? v.toString() : "(- " + v.negate() + ")";
  }
}
