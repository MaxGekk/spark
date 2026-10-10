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
import org.apache.spark.sql.catalyst.util.DateTimeConstants;

/**
 * The rendered files under {@code sql/varka/proofs/} (VARKA-240, VARKA-241): the proofs whose
 * constants come from the code, and the check of the prelude against the JVM.
 * {@code VarkaProofFilesSuite} fails when a committed file differs from what this renders, so a
 * proof cannot go on proving a constant the code no longer has. {@code java.smt2}, the prelude, is
 * written by hand and is not here.
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
    files.put("long_divide.smt2", longDivide());
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
  // The long lane's two division forms (VARKA-241), proven per divisor.
  // -----------------------------------------------------------------------------------------

  /**
   * The long-lane constant divisors: every one the {@code TIME} compiler divides by
   * ({@code VarkaTimeCompiler}'s {@code subtractTimes}, {@code timeDiff}, {@code timeTrunc}, the
   * conversions to milliseconds and microseconds, the field extracts and
   * {@code remainderOfSixty}), which are all a long-lane {@code ConstDivide} the compiler builds,
   * and -60, which takes the sign branch a positive divisor does not.
   */
  static final List<Long> LONG_DIVISORS = List.of(
      DateTimeConstants.NANOS_PER_MICROS,
      DateTimeConstants.NANOS_PER_MILLIS,
      DateTimeConstants.NANOS_PER_SECOND,
      DateTimeConstants.NANOS_PER_SECOND * DateTimeConstants.SECONDS_PER_MINUTE,
      DateTimeConstants.NANOS_PER_SECOND * DateTimeConstants.SECONDS_PER_MINUTE
          * DateTimeConstants.MINUTES_PER_HOUR,
      (long) DateTimeConstants.SECONDS_PER_MINUTE,
      -60L);

  private static final long P52 = 1L << 52;
  private static final long P53 = 1L << 53;

  /**
   * {@code long_divide.smt2}: for every divisor in {@link #LONG_DIVISORS}, both forms
   * {@code VarkaDivisionLowering.emitConstDivide} takes at the long lane are Java's {@code v / d}
   * for every dividend under {@code ConstDivide.EXACT_DIVIDEND_BOUND}; the conversion form is
   * exact to one below the first multiple of {@code d} past 2^53 and wrong there.
   *
   * <p>Every rounding is the prelude's {@code jdouble.rne} in a named binade, so each check is
   * linear: one check per binade the quotient can fall in, and a check that those binades cover
   * every quotient the dividend's range gives, without which the checks could pass by leaving a
   * quotient out.
   */
  static String longDivide() {
    if (VarkaVectorIR.ConstDivide.EXACT_DIVIDEND_BOUND != P52
        || VarkaDivisionLowering.MANTISSA_52 != P52 - 1
        || VarkaDivisionLowering.TWO_52_BITS != 0x433L << 52) {
      throw new IllegalStateException("long_divide.smt2 is stated for a dividend bound of 2^52 "
          + "and the magic form's constants 0x4330000000000000 and 2^52 - 1; restate it");
    }
    var out = new StringBuilder(LICENSE);
    out.append("""
        ; The long lane's constant division (VarkaVectorIR.ConstDivide) is exact under its
        ; dividend bound, ConstDivide.EXACT_DIVIDEND_BOUND = 2^52, in both of the forms
        ; VarkaDivisionLowering.emitConstDivide emits (VARKA-241). Per lane:
        ;
        ;   the conversion form, emitDoubleDivide:
        ;     double x = (double) v;          L2D
        ;     double q = x / d;               DoubleVector.div
        ;     long r = (long) q;              D2L
        ;
        ;   the magic form, emitMagicDivide, where L2D and D2L do not intrinsify:
        ;     long a = v < 0 ? -v : v;                                masked NEG
        ;     double x = longBitsToDouble(a | 0x4330000000000000L);   or, reinterpretAsDoubles
        ;     double q = (x - 2^52) / |d|;                            sub, div
        ;     double r1 = (q + 2^52) - 2^52;                          add, sub: q to nearest
        ;     double fl = r1 > q ? r1 - 1 : r1;                       compare, masked SUB
        ;     long y = doubleToRawLongBits(fl + 2^52) & (2^52 - 1);   add, reinterpretAsLongs, and
        ;     long r = (v < 0) != (d < 0) ? -y : y;                   masked NEG
        ;
        ; Every rounding is jdouble.rne in a named binade, so each check is linear: one check per
        ; binade a quotient can fall in, and one that the binades listed cover every quotient the
        ; dividend's range gives. The conversion form is also exact past the bound, to one below
        ; the first multiple of d above 2^53, and wrong there: the solver confirms the bound both
        ; ways, so the rule that renders it is checked too.
        """);
    out.append(RENDERED);
    out.append("""

        ; Lemma: an integer n with 0 < n <= 2^53 rounds to itself - L2D of it, and any sum or
        ; difference whose exact value it is. The checks below write such a value without its
        ; rounding.
        """);
    for (int j = 0; j <= 53; j++) {
      long[] s = {j >= 52 ? 1L << (j - 52) : 1, j >= 52 ? 1 : 1L << (52 - j)};
      check(out, "an integer in [2^" + j + ", 2^" + (j + 1) + ") rounds to itself", "unsat",
          List.of("(declare-const n Int)", "(declare-const M Int)",
              "(assert (and (<= " + (1L << j) + " n) (<= n " + Math.min((1L << (j + 1)) - 1, P53)
                  + ")))",
              "(assert (jdouble.rne n 1 M " + s[0] + " " + s[1] + "))",
              "(assert (not (= (* M " + s[0] + ") (* n " + s[1] + "))))"));
    }
    out.append("""

        ; Lemma: the magic form's OR is disjoint below 2^52, so the bits read back are 2^52 + a
        ; in 2^52's binade.
        """);
    check(out, "a | 0x4330000000000000L read as a double is 2^52 + a for 0 <= a < 2^52", "unsat",
        List.of("(declare-const a Int)", "(assert (and (<= 0 a) (< a " + P52 + ")))",
            "(define-fun xb () Int (jlong.or.disjoint a " + VarkaDivisionLowering.TWO_52_BITS
                + "))",
            "(assert (not (and (= (jdouble.fromBits.E xb) 0) (= (jdouble.fromBits.M xb) (+ "
                + P52 + " a)))))"));
    for (long d : LONG_DIVISORS) {
      long m = Math.abs(d);
      out.append("\n; d = ").append(d).append("\n");
      conversionExact(out, d, m);
      conversionBound(out, d, m);
      magicExact(out, d, m);
    }
    return out.toString();
  }

  /** The conversion form, for every {@code 0 < |v| < 2^53}: one check per quotient binade. */
  private static void conversionExact(StringBuilder out, long d, long m) {
    int kLo = binade(BigInteger.valueOf(P53 - 1), BigInteger.valueOf(m)) - 1;
    int kHi = binade(BigInteger.ONE, BigInteger.valueOf(m));
    check(out, "d = " + d + ": the conversion form's quotients for 0 < v < 2^53 lie in binades 2^-"
        + (kLo + 1) + " to 2^-" + kHi, "unsat", List.of("(declare-const v Int)",
        "(assert (and (<= 1 v) (< v " + P53 + ")))",
        "(assert (not " + inBinades("v", m, kLo + 1, kHi) + "))"));
    for (int k = kLo; k <= kHi; k++) {
      String[] s = scale(-k);
      check(out, "d = " + d + ": the conversion form is v / d for 0 < |v| < 2^53, quotient in "
          + "binade 2^-" + k, "unsat", List.of(
          "(declare-const v Int)", "(declare-const M Int)", "(declare-const r Int)",
          "(assert (and (< 0 (abs v)) (< (abs v) " + P53 + ")))",
          "; L2D leaves v as it is (the lemma); |v| / |d| rounds to M * 2^-" + k
              + "; D2L truncates it",
          "(assert (jdouble.rne (abs v) " + m + " M " + s[0] + " " + s[1] + "))",
          "(assert (jdouble.d2l.mag r M " + s[0] + " " + s[1] + "))",
          "(assert (not (= " + signed("r", d) + " (jlong.div v " + lit(d) + "))))"));
    }
    check(out, "d = " + d + ": both forms take 0 to 0", "unsat",
        List.of("(assert (not (= 0 (jlong.div 0 " + lit(d) + "))))"));
  }

  /**
   * The conversion form past the bound: exact for {@code 2^53 <= |v| < B} and wrong at
   * {@code |v| = B}, where B is one less than the first multiple of {@code |d|} above 2^53. L2D
   * rounds there, into 2^53's binade or onto 2^54.
   */
  private static void conversionBound(StringBuilder out, long d, long m) {
    long b = (P53 / m + 1) * m - 1;
    int kLo = binade(BigInteger.valueOf(P53).shiftLeft(1), BigInteger.valueOf(m)) - 1;
    int kHi = binade(BigInteger.valueOf(P53), BigInteger.valueOf(m));
    String l2d = "(assert (or (and (jdouble.rne (abs v) 1 Mx 2 1) (= X (* 2 Mx))) "
        + "(and (jdouble.rne (abs v) 1 Mx 4 1) (= X (* 4 Mx)))))";
    var branches = new ArrayList<String>();
    for (int k = kLo; k <= kHi; k++) {
      String[] s = scale(-k);
      branches.add("(and (jdouble.rne X " + m + " M " + s[0] + " " + s[1]
          + ") (jdouble.d2l.mag r M " + s[0] + " " + s[1] + "))");
    }
    String rounds = "(assert (or " + String.join("\n  ", branches) + "))";
    var decl = List.of("(declare-const v Int)", "(declare-const Mx Int)", "(declare-const X Int)",
        "(declare-const M Int)", "(declare-const r Int)");
    check(out, "d = " + d + ": the conversion form's quotients for 2^53 <= |v| <= 2^54 lie in "
        + "binades 2^-" + (kLo + 1) + " to 2^-" + kHi, "unsat", List.of("(declare-const X Int)",
        "(assert (and (<= " + P53 + " X) (<= X " + 2 * P53 + ")))",
        "(assert (not " + inBinades("X", m, kLo + 1, kHi) + "))"));
    var exact = new ArrayList<>(decl);
    exact.add("(assert (and (<= " + P53 + " (abs v)) (< (abs v) " + b + ")))");
    exact.add(l2d);
    exact.add(rounds);
    exact.add("(assert (not (= " + signed("r", d) + " (jlong.div v " + lit(d) + "))))");
    check(out, "d = " + d + ": the conversion form is v / d for 2^53 <= |v| < " + b
        + ", one less than the first multiple of |d| past 2^53", "unsat", exact);
    for (long v : new long[] {b, -b}) {
      var wrong = new ArrayList<>(decl);
      wrong.add("(assert (= v " + lit(v) + "))");
      wrong.add(l2d);
      wrong.add(rounds);
      wrong.add("(assert (not (= " + signed("r", d) + " (jlong.div v " + lit(d) + "))))");
      check(out, "d = " + d + ": and it is not at v = " + v, "sat", wrong);
    }
  }

  /** The magic form, for every {@code 0 < |v| < 2^52}: one check per quotient binade. */
  private static void magicExact(StringBuilder out, long d, long m) {
    int kLo = binade(BigInteger.valueOf(P52 - 1), BigInteger.valueOf(m)) - 1;
    int kHi = binade(BigInteger.ONE, BigInteger.valueOf(m));
    check(out, "d = " + d + ": the magic form's quotients for 0 < a < 2^52 lie in binades 2^-"
        + (kLo + 1) + " to 2^-" + kHi, "unsat", List.of("(declare-const a Int)",
        "(assert (and (<= 1 a) (< a " + P52 + ")))",
        "(assert (not " + inBinades("a", m, kLo + 1, kHi) + "))"));
    String flip = d < 0 ? "(not (< v 0))" : "(< v 0)";
    for (int k = kLo; k <= kHi; k++) {
      String[] s = scale(-k);
      BigInteger p = BigInteger.ONE.shiftLeft(k);
      String sum = "(+ Mq " + BigInteger.valueOf(P52).shiftLeft(k) + ")";
      check(out, "d = " + d + ": the magic form is v / d for 0 < |v| < 2^52, quotient in binade 2^-"
          + k, "unsat", List.of(
          "(declare-const v Int)", "(declare-const Mq Int)", "(declare-const Ms Int)",
          "(declare-const s Int)",
          "(assert (and (< 0 (abs v)) (< (abs v) " + P52 + ")))",
          "; a = |v|; x = 2^52 + a (the second lemma); x - 2^52 = a (the first); a / |d| rounds to "
              + "Mq * 2^-" + k,
          "(define-fun a () Int (ite (< v 0) (jlong.neg v) v))",
          "(assert (jdouble.rne a " + m + " Mq " + s[0] + " " + s[1] + "))",
          "; q + 2^52 rounds into 2^52's binade or onto 2^53",
          "(assert (or (and (jdouble.rne " + sum + " " + p + " Ms 1 1) (= s Ms))",
          "            (and (jdouble.rne " + sum + " " + p + " Ms 2 1) (= s (* 2 Ms)))))",
          "; - 2^52 is exact, as are r1 - 1 and fl + 2^52 (the first lemma); y is fl + 2^52's bits",
          "; masked to the low 52",
          "(define-fun r1 () Int (- s " + P52 + "))",
          "(define-fun fl () Int (ite (> (* r1 " + p + ") Mq) (- r1 1) r1))",
          "(define-fun y () Int (jlong.and.low (jdouble.bits (+ fl " + P52 + ") 0) "
              + (VarkaDivisionLowering.MANTISSA_52 + 1) + "))",
          "(define-fun r () Int (ite " + flip + " (jlong.neg y) y))",
          "(assert (not (and (<= 0 r1) (<= r1 " + P52 + ") (<= 0 fl) (< fl " + P52 + ")",
          "  (= r (jlong.div v " + lit(d) + ")))))"));
    }
  }

  /** {@code |r|} with the quotient's sign: negative where exactly one of v and d is. */
  private static String signed(String r, long d) {
    return "(ite (= (< v 0) " + (d < 0) + ") " + r + " (- " + r + "))";
  }

  /** {@code 2^e} as the prelude's {@code sn} and {@code sd}. */
  private static String[] scale(int e) {
    return e >= 0 ? new String[] {BigInteger.ONE.shiftLeft(e).toString(), "1"}
        : new String[] {"1", BigInteger.ONE.shiftLeft(-e).toString()};
  }

  /** The binade k of {@code a / b > 0}: {@code 2^52 <= (a / b) * 2^k < 2^53}. */
  static int binade(BigInteger a, BigInteger b) {
    int k = 52 - (a.bitLength() - b.bitLength());
    // Now 2^51 < (a / b) * 2^k < 2^54; step k until the scaled quotient is in [2^52, 2^53).
    while (scaledBelow(a, b, k, 52)) {
      k++;
    }
    while (!scaledBelow(a, b, k, 53)) {
      k--;
    }
    return k;
  }

  /** Whether {@code (a / b) * 2^k < 2^e}. */
  private static boolean scaledBelow(BigInteger a, BigInteger b, int k, int e) {
    BigInteger left = k >= 0 ? a.shiftLeft(k) : a;
    BigInteger right = k >= 0 ? b.shiftLeft(e) : b.shiftLeft(e - k);
    return left.compareTo(right) < 0;
  }

  /** That {@code x / m} lies in binades {@code kLo..kHi}: {@code [2^(52-kHi), 2^(53-kLo))}. */
  private static String inBinades(String x, long m, int kLo, int kHi) {
    return "(and " + scaledAtLeast(x, m, kHi, 52) + " (not " + scaledAtLeast(x, m, kLo, 53) + "))";
  }

  /** {@code (x / m) * 2^k >= 2^e}, as a linear inequality. */
  private static String scaledAtLeast(String x, long m, int k, int e) {
    BigInteger left = BigInteger.ONE.shiftLeft(Math.max(k, 0));
    BigInteger right = BigInteger.valueOf(m).shiftLeft(k >= 0 ? e : e - k);
    return "(>= (* " + x + " " + left + ") " + right + ")";
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
    doubles(out);
    return out.toString();
  }

  /**
   * The doubles (VARKA-241): {@code jdouble.rne} against Java's {@code (double)} of a long and
   * {@code /} of two exact doubles, and against the sum the magic form rounds at its ties; then
   * {@code jdouble.d2l.mag}, the bit layout, and the two bitwise operations the magic form uses.
   * Each rounding is checked both ways: the double Java computed satisfies the definition, and its
   * two neighbours in the same binade do not, so the definition pins one double rather than
   * admitting several.
   */
  private static void doubles(StringBuilder out) {
    var l2d = new ArrayList<String>();
    // 2^54 - 2 is a double, the last of its binade, and 2^54 - 1 is the tie between it and 2^54:
    // the two points where the bottom of 2^54's binade decides.
    for (long v : new long[] {1, 7, P52 - 1, P52, P52 + 1, P53 - 1, P53, P53 + 1, P53 + 2,
        P53 + 3, P53 + 5, 2 * P53 - 2, 2 * P53 - 1, 2 * P53 + 2, 2 * P53 + 6, 2 * P53 + 7,
        3 * P53 + 1, Long.MAX_VALUE}) {
      rounded(l2d, BigInteger.valueOf(v), BigInteger.ONE, (double) v);
    }
    equations(out, "jdouble.rne is Java's (double) of a long, ties included", l2d);
    var div = new ArrayList<String>();
    for (long b : new long[] {3, 49, 60, 1000, 1_000_000, 1_000_000_000, 60_000_000_000L,
        3_600_000_000_000L, 146097}) {
      for (long a : new long[] {1, 2, 7, b - 1, b + 1, 12_345_678_901L, P52 - 1, P52, P52 + 1,
          P53 - 1, P53}) {
        rounded(div, BigInteger.valueOf(a), BigInteger.valueOf(b), (double) a / (double) b);
      }
    }
    equations(out, "jdouble.rne is Java's / of two exact doubles", div);
    // The magic form's q + 2^52 at a quotient ending in a half: the sum is a tie, and Java rounds
    // it to the even neighbour. As a rational, (2n + 1 + 2^53) / 2.
    var ties = new ArrayList<String>();
    for (long n : new long[] {0, 1, 2, 3, 1_000_000, (1L << 51) - 1}) {
      double sum = (n + 0.5) + 4503599627370496.0;
      rounded(ties, BigInteger.valueOf(2 * n + 1 + P53), BigInteger.TWO, sum);
    }
    equations(out, "jdouble.rne is Java's + at a tie, to the even neighbour", ties);
    var d2l = new ArrayList<String>();
    for (double q : new double[] {0.25, 0.5, 1.0, 1.5, 2.5, 7.999999999999999, 12345.678,
        4503599627370495.5, 4503599627370496.0, 9007199254740991.0, 9.2e18}) {
      long bits = Double.doubleToRawLongBits(q);
      int e = Math.getExponent(q) - 52;
      long mant = (bits & VarkaDivisionLowering.MANTISSA_52) | P52;
      String[] sc = scale(e);
      d2l.add("(jdouble.d2l.mag " + (long) q + " " + mant + " " + sc[0] + " " + sc[1] + ")");
      for (long wrong : new long[] {(long) q - 1, (long) q + 1}) {
        d2l.add("(not (jdouble.d2l.mag " + wrong + " " + mant + " " + sc[0] + " " + sc[1] + "))");
      }
    }
    equations(out, "jdouble.d2l.mag is Java's (long) of a double, toward zero", d2l);
    var bits = new ArrayList<String>();
    for (double q : new double[] {Double.MIN_NORMAL, 0.1, 1.0, 1.5, 4503599627370496.0,
        4503599627370497.0, 9007199254740991.0, 1e300}) {
      long raw = Double.doubleToRawLongBits(q);
      int e = Math.getExponent(q) - 52;
      long mant = (raw & VarkaDivisionLowering.MANTISSA_52) | P52;
      bits.add("(= (jdouble.bits " + mant + " " + lit(e) + ") " + raw + ")");
      bits.add("(= (jdouble.fromBits.M " + raw + ") " + mant + ")");
      bits.add("(= (jdouble.fromBits.E " + raw + ") " + lit(e) + ")");
    }
    equations(out, "jdouble.bits and jdouble.fromBits are Double.doubleToRawLongBits and "
        + "longBitsToDouble", bits);
    var bitwise = new ArrayList<String>();
    for (long x : LONGS) {
      bitwise.add("(= (jlong.and.low " + lit(x) + " " + P52 + ") "
          + lit(x & VarkaDivisionLowering.MANTISSA_52) + ")");
    }
    for (long a : new long[] {0, 1, 12_345_678_901L, P52 - 1}) {
      bitwise.add("(= (jlong.or.disjoint " + a + " " + VarkaDivisionLowering.TWO_52_BITS + ") "
          + (a | VarkaDivisionLowering.TWO_52_BITS) + ")");
    }
    equations(out, "jlong.and.low and jlong.or.disjoint are Java's & and |, where the magic form "
        + "uses them", bitwise);
  }

  /**
   * That {@code jdouble.rne} takes {@code a / b} to {@code q}, which Java computed, and to neither
   * neighbour of {@code q}: the next double up and the next down, each written in its own binade,
   * so a neighbour across a binade's edge is refused as well as one inside it.
   */
  private static void rounded(List<String> eqs, BigInteger a, BigInteger b, double q) {
    eqs.add(rne(a, b, q));
    eqs.add("(not " + rne(a, b, Math.nextUp(q)) + ")");
    eqs.add("(not " + rne(a, b, Math.nextDown(q)) + ")");
  }

  /** {@code (jdouble.rne a b M sn sd)} with the positive normal {@code q} as M and its binade. */
  private static String rne(BigInteger a, BigInteger b, double q) {
    long raw = Double.doubleToRawLongBits(q);
    long mant = (raw & VarkaDivisionLowering.MANTISSA_52) | P52;
    String[] sc = scale(Math.getExponent(q) - 52);
    return "(jdouble.rne " + a + " " + b + " " + mant + " " + sc[0] + " " + sc[1] + ")";
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
