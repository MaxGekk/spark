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
    files.put("chrono_divide.smt2", chronoDivide());
    files.put("floor_mod7.smt2", floorMod7());
    files.put("leap_hash.smt2", leapHash());
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

  private static final String RENDERED = rendered("QF_NIA");

  /**
   * The lines every rendered file shares after its own header, ending in its logic. A file
   * declares {@code ALL} where cvc5 decides its checks only under it (VARKA-242.md 2.2) or where
   * it reasons over bit-vectors, which {@code QF_NIA} does not admit.
   */
  private static String rendered(String logic) {
    return """
        ;
        ; Rendered by VarkaProofFiles. VarkaProofFilesSuite fails when this file differs from its
        ; rendering, and VARKA_PROOFS_REGEN=true build/sbt 'catalyst/testOnly *VarkaProofFilesSuite'
        ; rewrites it. Run by dev/varka_prove.sh, which inserts java.smt2 after the set-logic line.

        (set-logic %s)
        """.formatted(logic);
  }

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
  // The calendar's constant divisions (VARKA-242), proven per site.
  // -----------------------------------------------------------------------------------------

  /**
   * {@code chrono_divide.smt2}: for every {@link VarkaChronoLowering.ChronoDivide} site, the
   * magic form with its carry where the table says so is {@code v / d} over the site's dividends
   * up to one below the first that fails, and fails there; without its carry it fails inside the
   * site's range; and the two double forms, {@code DOUBLE_DIV} and {@code DOUBLE_RECIP}, are exact
   * over the range, or for a reciprocal the table marks inexact, wrong inside it.
   */
  static String chronoDivide() {
    var out = new StringBuilder(LICENSE);
    out.append("""
        ; The calendar's constant divisions (VarkaChronoLowering.ChronoDivide) are exact over the
        ; dividends each site sees (VARKA-242). Per lane, at the int lane, with (m, k) the site's
        ; magic pair and d its divisor:
        ;
        ;   the magic form, emitDivide and emitCarry:
        ;     int q = (v * m) >>> k;           IntVector.mul, LSHR
        ;     if (v - q * d >= d) q = q + 1;   the carry, at the sites the table marks carried
        ;
        ;   the double forms, emitDoubleDivide under DOUBLE_DIV and DOUBLE_RECIP:
        ;     int q = (int) ((double) v / d);           I2D, DoubleVector.div, D2I
        ;     int q = (int) ((double) v * (1.0 / d));   I2D, DoubleVector.mul, D2I
        ;
        ; A site's dividends are stride * x + residue, non-negative, up to its maxDividend: the
        ; Julian sites divide 4 * dayOfEra + 3 and what the map adds to it in fours, every other
        ; site an interval from 0. For each site the magic form is shown exact to one below the
        ; first dividend of that shape where it fails, which VarkaProofFiles found by scanning
        ; while rendering, and wrong there, so the solver confirms the scan; the rendering refuses
        ; a maxDividend at or past it. A carried site is shown wrong without its carry inside its
        ; range, so no site is marked carried for nothing. The double forms are stated per
        ; quotient binade, as in long_divide.smt2, with a check that the binades cover the range.
        """);
    out.append(RENDERED);
    out.append("""

        ; Lemma: an integer n with 0 < n < 2^31 rounds to itself, so I2D of a dividend is exact and
        ; the checks below write it without its rounding.
        """);
    for (int j = 0; j <= 30; j++) {
      long[] s = {1, 1L << (52 - j)};
      check(out, "an integer in [2^" + j + ", 2^" + (j + 1) + ") rounds to itself", "unsat",
          List.of("(declare-const n Int)", "(declare-const M Int)",
              "(assert (and (<= " + (1L << j) + " n) (< n " + (1L << (j + 1)) + ")))",
              "(assert (jdouble.rne n 1 M " + s[0] + " " + s[1] + "))",
              "(assert (not (= (* M " + s[0] + ") (* n " + s[1] + "))))"));
    }
    out.append("""

        (define-fun chrono.magic ((v Int) (m Int) (k Int) (d Int) (carry Bool)) Int
          (let ((q (jint.ushr (jint.mul v m) k)))
            (ite (and carry (>= (jint.sub v (jint.mul q d)) d)) (jint.add q 1) q)))
        """);
    for (VarkaChronoLowering.ChronoDivide site : VarkaChronoLowering.ChronoDivide.values()) {
      chronoMagic(out, site);
      chronoDouble(out, site, false);
      chronoDouble(out, site, true);
    }
    return out.toString();
  }

  /**
   * The first dividend of the site's shape where the scalar magic form, with its carry where the
   * table says so, is not {@code v / d}: {@code (v * m) >>> k} in Java's own int arithmetic.
   */
  static int chronoFirstWrong(VarkaChronoLowering.ChronoDivide site) {
    for (int v = site.residue; v >= 0; v += site.stride) {
      if (chronoMagicScalar(site, v, site.carried) != v / site.divisor) {
        return v;
      }
    }
    throw new IllegalStateException(site + "'s magic form is exact over every int of its shape");
  }

  private static int chronoMagicScalar(VarkaChronoLowering.ChronoDivide site, int v,
      boolean carry) {
    int q = (v * site.m) >>> site.k;
    if (carry && v - q * site.divisor >= site.divisor) {
      q++;
    }
    return q;
  }

  /** The first dividend of the site's shape where the reciprocal form is wrong, or -1. */
  private static int chronoRecipFirstWrong(VarkaChronoLowering.ChronoDivide site) {
    double recip = 1.0 / site.divisor;
    for (int v = site.residue; v >= 0 && v <= site.maxDividend; v += site.stride) {
      if ((int) ((double) v * recip) != v / site.divisor) {
        return v;
      }
    }
    return -1;
  }

  /** That {@code v} is of the site's shape: {@code stride * x + residue}, from 0 to {@code max}. */
  private static String chronoShape(VarkaChronoLowering.ChronoDivide site, long lo, long max) {
    String range = "(<= " + lo + " v) (<= v " + max + ")";
    return site.stride == 1 ? "(assert (and " + range + "))"
        : "(assert (and " + range + " (= (mod v " + site.stride + ") " + site.residue + ")))";
  }

  private static void chronoMagic(StringBuilder out, VarkaChronoLowering.ChronoDivide site) {
    int d = site.divisor;
    int first = chronoFirstWrong(site);
    if (site.maxDividend > first - site.stride) {
      throw new IllegalStateException(site + "'s dividends reach " + site.maxDividend
          + ", and its magic form first fails at " + first);
    }
    out.append("\n; ").append(site).append(": d = ").append(d).append(", (m, k) = (")
        .append(site.m).append(", ").append(site.k).append(")")
        .append(site.carried ? " and a carry" : "").append(", dividends ")
        .append(site.stride == 1 ? "0" : site.stride + "x + " + site.residue).append(" to ")
        .append(site.maxDividend).append(", first wrong ").append(first).append("\n");
    String form = "(chrono.magic v " + site.m + " " + site.k + " " + d + " " + site.carried + ")";
    check(out, site + ": the magic form is v / " + d + " for every dividend of its shape to "
        + (first - site.stride) + ", past its " + site.maxDividend, "unsat", List.of(
        "(declare-const v Int)", chronoShape(site, 0, first - site.stride),
        "(assert (not (= " + form + " (div v " + d + "))))"));
    check(out, site + ": and it is not at " + first, "sat", List.of("(declare-const v Int)",
        "(assert (= v " + first + "))", "(assert (not (= " + form + " (div v " + d + "))))"));
    if (site.carried) {
      check(out, site + ": without the carry it is not, inside the range", "sat", List.of(
          "(declare-const v Int)", chronoShape(site, 0, site.maxDividend),
          "(assert (not (= (chrono.magic v " + site.m + " " + site.k + " " + d + " false) (div v "
              + d + "))))"));
    }
  }

  /**
   * {@code DOUBLE_DIV} or {@code DOUBLE_RECIP} over the site's range: the exact quotient is
   * {@code (v * num) / den}, with {@code num / den} either {@code 1 / d} or the double nearest
   * {@code 1 / d}, and it rounds in a named binade.
   */
  private static void chronoDouble(StringBuilder out, VarkaChronoLowering.ChronoDivide site,
      boolean recip) {
    int d = site.divisor;
    String name = recip ? "DOUBLE_RECIP" : "DOUBLE_DIV";
    BigInteger num = BigInteger.ONE;
    BigInteger den = BigInteger.valueOf(d);
    String a = "v";
    if (recip) {
      double r = 1.0 / d;
      long raw = Double.doubleToRawLongBits(r);
      num = BigInteger.valueOf((raw & VarkaDivisionLowering.MANTISSA_52) | P52);
      den = BigInteger.ONE.shiftLeft(52 - Math.getExponent(r));
      a = "(* v " + num + ")";
      out.append("; ").append(site).append(": 1.0 / ").append(d).append(" is ").append(num)
          .append(" * 2^-").append(52 - Math.getExponent(r)).append("\n");
    }
    BigInteger lowest = BigInteger.valueOf(site.residue == 0 ? site.stride : site.residue);
    BigInteger max = BigInteger.valueOf(site.maxDividend);
    // The exact quotients lie in binades kLo to kHi; a rounding at the top of one lands on the
    // bottom of the next one up, so the rounded quotient can also be in kLo - 1.
    int kLo = binade(max.multiply(num), den);
    int kHi = binade(lowest.multiply(num), den);
    if (!recip || site.recipExact) {
      check(out, site + ": " + name + "'s quotients over its range lie in binades 2^-" + kLo
          + " to 2^-" + kHi, "unsat", List.of("(declare-const v Int)",
          chronoShape(site, lowest.longValue(), site.maxDividend),
          "(assert (not (and " + scaledAtLeast(a, den, kHi, 52) + " (not "
              + scaledAtLeast(a, den, kLo, 53) + "))))"));
      for (int k = kLo - 1; k <= kHi; k++) {
        String[] s = scale(-k);
        check(out, site + ": " + name + " is v / " + d + " over its range, quotient in binade 2^-"
            + k, "unsat", List.of("(declare-const v Int)", "(declare-const M Int)",
            "(declare-const r Int)", chronoShape(site, lowest.longValue(), site.maxDividend),
            "(assert (jdouble.rne " + a + " " + den + " M " + s[0] + " " + s[1] + "))",
            "(assert (jdouble.d2l.mag r M " + s[0] + " " + s[1] + "))",
            "(assert (not (= r (div v " + d + "))))"));
      }
      if (site.residue == 0) {
        check(out, site + ": " + name + " takes 0 to 0", "unsat",
            List.of("(assert (not (= 0 (div 0 " + d + "))))"));
      }
      return;
    }
    int wrong = chronoRecipFirstWrong(site);
    if (wrong < 0) {
      throw new IllegalStateException(site + " is marked recipExact = false, and the reciprocal "
          + "is exact over its range");
    }
    // The product rounds in the exact quotient's binade or, at its top, onto the next one up.
    int k = binade(BigInteger.valueOf(wrong).multiply(num), den);
    String[] s = scale(-k);
    String[] up = scale(-(k - 1));
    check(out, site + ": recipExact is false: " + name + " is not v / " + d + " at " + wrong
        + ", inside the range", "sat", List.of("(declare-const v Int)", "(declare-const M Int)",
        "(declare-const r Int)", "(assert (= v " + wrong + "))",
        "(assert (or (and (jdouble.rne " + a + " " + den + " M " + s[0] + " " + s[1]
            + ") (jdouble.d2l.mag r M " + s[0] + " " + s[1] + "))",
        "            (and (jdouble.rne " + a + " " + den + " M " + up[0] + " " + up[1]
            + ") (jdouble.d2l.mag r M " + up[0] + " " + up[1] + "))))",
        "(assert (not (= r (div v " + d + "))))"));
  }

  /** {@code (a / b) * 2^k >= 2^e} for an expression {@code a}, as a linear inequality. */
  private static String scaledAtLeast(String a, BigInteger b, int k, int e) {
    BigInteger left = BigInteger.ONE.shiftLeft(Math.max(k, 0));
    BigInteger right = b.shiftLeft(k >= 0 ? e : e - k);
    return "(>= (* " + a + " " + left + ") " + right + ")";
  }

  // -----------------------------------------------------------------------------------------
  // floorMod(v, 7) (VARKA-242), its three forms.
  // -----------------------------------------------------------------------------------------

  private static final long P32 = 1L << 32;

  /**
   * {@code floor_mod7.smt2}: each of {@code emitFloorMod7}'s three forms is
   * {@code Math.floorMod(v, 7)} for every int {@code v}, stated as lemmas over the digits each
   * fold splits its input into (VARKA-242.md 2.2), and a check per form that the lemmas compose.
   */
  static String floorMod7() {
    var folds = new ArrayList<VarkaChronoLowering.Fold>(VarkaChronoLowering.FLOOR_MOD7_FOLDS);
    folds.addAll(VarkaChronoLowering.DIGIT_SUM_FOLDS);
    for (VarkaChronoLowering.Fold f : folds) {
      if (f.mask() != (1 << f.shift()) - 1) {
        throw new IllegalStateException("floor_mod7.smt2 states a fold whose mask is its shift's "
            + "low bits, and " + f + " is not one");
      }
    }
    int fix = VarkaChronoLowering.FLOOR_MOD7_SIGN_FIX;
    var out = new StringBuilder(LICENSE);
    out.append("""
        ; floorMod(v, 7) is what VarkaChronoLowering.emitFloorMod7 computes, in each of its three
        ; forms, for every int v (VARKA-242). Per lane:
        ;
        ;   a fold, VarkaChronoLowering.Fold:     y = (x & (2^k - 1)) + (x >>> k)
        ;   the shipped form (FloorMod7.MAGIC):   two folds of 15 bits; + 3 where v < 0;
        ;                                         r = g - ((g * 37450) >>> 18) * 7
        ;   FloorMod7.DIGIT_SUM:                  folds of 15, 15, 6, 3, 3 and 3 bits; + 3 where
        ;                                         v < 0; - 7 where that is at least 7
        ;   FloorMod7.DIV:                        r = v - (v / 7) * 7; + 7 where r < 0
        ;
        ; The folds read v unsigned, as u = v or v + 2^32. Each is stated by the digits of its
        ; input, u = 2^k * a + b with 0 <= b < 2^k, so every check is linear arithmetic over a few
        ; small variables: a lemma that the prelude's & and >>> give b and a, at both signs; a
        ; lemma per fold that a + b keeps the residue mod 7, since 2^k is 1 mod 7 for k = 15, 6
        ; and 3, and is at most the bound written; that + 3 restores a negative v's residue, since
        ; 2^32 is 4 mod 7; and the last step over the small value the folds leave. A check per
        ; form then takes the lemmas' conclusions as its premises and shows the result is v's
        ; residue. A residue kept is written as a difference that is a multiple of 7, and in the
        ; composition as 7 times a declared integer, with no mod at all: cvc5 decides each of
        ; these checks alone in milliseconds, but within the file its earlier checks left the mod
        ; forms of two of them unknown. The file declares ALL: under QF_NIA cvc5 leaves six of
        ; these checks unknown, under ALL it decides every one (VARKA-242.md 2.2).
        """);
    out.append(rendered("ALL"));
    out.append("""

        (define-fun fm7.unsigned ((x Int)) Int (ite (< x 0) (+ x 4294967296) x))
        """);
    out.append("\n; The prelude's & and >>> give a fold's digits.\n");
    var seen = new java.util.LinkedHashSet<VarkaChronoLowering.Fold>(folds);
    for (VarkaChronoLowering.Fold f : seen) {
      long base = 1L << f.shift();
      for (boolean negative : new boolean[] {false, true}) {
        String sign = negative ? "(< x 0)" : "(>= x 0)";
        String digits = "(assert (and (= (fm7.unsigned x) (+ (* " + base + " a) b)) (<= 0 b) (< b "
            + base + ")))";
        var decl = List.of("(declare-const x Int)", "(declare-const a Int)",
            "(declare-const b Int)", "(assert (and (jint.in x) " + sign + "))", digits);
        String which = negative ? "a negative" : "a non-negative";
        var mask = new ArrayList<>(decl);
        mask.add("(assert (not (= (jint.and.low x " + base + ") b)))");
        check(out, "x & " + f.mask() + " is the low digit b of " + which + " int's unsigned "
            + "reading", "unsat", mask);
        var shift = new ArrayList<>(decl);
        shift.add("(assert (not (= (jint.ushr x " + f.shift() + ") a)))");
        check(out, "x >>> " + f.shift() + " is the high digit a of " + which + " int's unsigned "
            + "reading", "unsat", shift);
      }
    }
    long shippedMax = foldResidues(out, "the shipped form", VarkaChronoLowering.FLOOR_MOD7_FOLDS);
    long digitMax = foldResidues(out, "DIGIT_SUM", VarkaChronoLowering.DIGIT_SUM_FOLDS);
    out.append("\n; The sign fixup, and the last steps.\n");
    check(out, "+ " + fix + " restores a negative int's residue from its unsigned reading", "unsat",
        List.of("(declare-const x Int)", "(assert (and (jint.in x) (< x 0)))",
            "(assert (not (= (mod (- (+ (fm7.unsigned x) " + fix + ") x) 7) 0)))"));
    for (long max : new long[] {shippedMax, digitMax}) {
      check(out, "+ " + fix + " does not wrap over [0, " + max + "]", "unsat",
          List.of("(declare-const y Int)", "(assert (and (<= 0 y) (<= y " + max + ")))",
              "(assert (not (= (jint.add y " + fix + ") (+ y " + fix + "))))"));
    }
    long g = shippedMax + fix;
    int m = VarkaChronoLowering.FLOOR_MOD7_M;
    int k = VarkaChronoLowering.FLOOR_MOD7_K;
    check(out, "the shipped form: (g * " + m + ") >>> " + k + " is g / 7 over [0, " + g + "]",
        "unsat", List.of("(declare-const g Int)", "(assert (and (<= 0 g) (<= g " + g + ")))",
            "(assert (not (= (jint.ushr (jint.mul g " + m + ") " + k + ") (div g 7))))"));
    check(out, "the shipped form: g - (g / 7) * 7 is g's residue over [0, " + g + "]", "unsat",
        List.of("(declare-const g Int)", "(assert (and (<= 0 g) (<= g " + g + ")))",
            "(assert (not (= (jint.sub g (jint.mul (div g 7) 7)) (mod g 7))))"));
    long h = digitMax + fix;
    check(out, "DIGIT_SUM: one subtract of 7 where at least 7 is the residue over [0, " + h + "]",
        "unsat", List.of("(declare-const g Int)", "(assert (and (<= 0 g) (<= g " + h + ")))",
            "(assert (not (= (ite (>= g 7) (jint.sub g 7) g) (mod g 7))))"));
    out.append("\n; Each form composed: the lemmas' conclusions as premises.\n");
    foldComposition(out, "the shipped form", VarkaChronoLowering.FLOOR_MOD7_FOLDS, fix);
    foldComposition(out, "DIGIT_SUM", VarkaChronoLowering.DIGIT_SUM_FOLDS, fix);
    // DIV through the truncated quotient q, named: stated in one check, cvc5 cannot decide it.
    for (boolean negative : new boolean[] {false, true}) {
      String which = negative ? "a negative" : "a non-negative";
      var decl = List.of("(declare-const x Int) (declare-const q Int) (declare-const t Int)",
          "(assert (and (jint.in x) " + (negative ? "(< x 0)" : "(>= x 0)") + "))",
          "(assert (and (= x (+ (* 7 q) t)) "
              + (negative ? "(< (- 7) t) (<= t 0)" : "(<= 0 t) (< t 7)") + "))");
      var quotient = new ArrayList<>(decl);
      quotient.add("(assert (not (= (jint.div x 7) q)))");
      check(out, "DIV: v / 7 is the quotient truncated toward zero, for " + which + " int",
          "unsat", quotient);
      var rem = new ArrayList<>(decl);
      rem.add("(assert (not (= (jint.sub x (jint.mul q 7)) t)))");
      check(out, "DIV: v - (v / 7) * 7 is the remainder t, without wrapping, for " + which
          + " int", "unsat", rem);
      var rest = new ArrayList<>(decl);
      rest.add(RESIDUE_OF_X);
      rest.add("(assert (not (= (ite (< t 0) (jint.add t 7) t) sx)))");
      check(out, "DIV: t, + 7 where negative, is the residue, for " + which + " int", "unsat",
          rest);
    }
    check(out, "Math.floorMod(v, 7) is v's non-negative residue for every int", "unsat",
        List.of("(declare-const x Int)", "(assert (jint.in x))", RESIDUE_OF_X,
            "(assert (not (= (jint.floorMod x 7) sx)))"));
    return out.toString();
  }

  /** {@code sx}, the residue of {@code x} mod 7, by its own quotient rather than by {@code mod}. */
  private static final String RESIDUE_OF_X = "(declare-const qx Int) (declare-const sx Int)\n"
      + "(assert (and (= x (+ (* 7 qx) sx)) (<= 0 sx) (< sx 7)))";

  /** The largest {@code (y >>> shift) + (y & (2^shift - 1))} over {@code 0 <= y <= max}. */
  private static long foldMax(long max, int shift) {
    long mask = (1L << shift) - 1;
    long q = max >>> shift;
    return Math.max(q + (max & mask), q >= 1 ? q - 1 + mask : 0);
  }

  /** A lemma per fold of the chain: the residue kept, the sum bounded. Returns the last bound. */
  private static long foldResidues(StringBuilder out, String form,
      List<VarkaChronoLowering.Fold> chain) {
    out.append("\n; ").append(form).append("'s folds keep the residue mod 7.\n");
    long max = P32 - 1;
    for (VarkaChronoLowering.Fold f : chain) {
      long next = foldMax(max, f.shift());
      long base = 1L << f.shift();
      check(out, form + ": a fold of " + f.shift() + " bits over [0, " + max + "] is at most "
          + next + " and keeps the residue", "unsat", List.of("(declare-const y Int)",
          "(declare-const a Int)", "(declare-const b Int)",
          "(assert (and (<= 0 y) (<= y " + max + ") (= y (+ (* " + base + " a) b)) (<= 0 b) (< b "
              + base + ")))",
          "(assert (not (and (<= (+ a b) " + next + ") (= (mod (- y (+ a b)) 7) 0))))"));
      max = next;
    }
    return max;
  }

  /**
   * That the lemmas compose: from each fold's residue and bound, the sign fixup and the last
   * step's equation, the form's result is {@code mod(v, 7)}.
   */
  private static void foldComposition(StringBuilder out, String form,
      List<VarkaChronoLowering.Fold> chain, int fix) {
    var body = new ArrayList<String>();
    body.add("(declare-const x Int)");
    body.add("(assert (jint.in x))");
    String prev = "(fm7.unsigned x)";
    long max = P32 - 1;
    for (int i = 0; i < chain.size(); i++) {
      max = foldMax(max, chain.get(i).shift());
      body.add("(declare-const y" + i + " Int) (declare-const c" + i + " Int)");
      body.add("(assert (and (<= 0 y" + i + ") (<= y" + i + " " + max + ") (= (- " + prev + " y"
          + i + ") (* 7 c" + i + "))))");
      prev = "y" + i;
    }
    body.add("(define-fun g () Int (ite (< x 0) (+ " + prev + " " + fix + ") " + prev + "))");
    body.add("(declare-const cf Int)");
    body.add("(assert (=> (< x 0) (= (- (+ (fm7.unsigned x) " + fix + ") x) (* 7 cf))))");
    body.add("; mod g 7 and mod x 7, as the residues sg and sx of their own quotients");
    body.add("(declare-const qg Int) (declare-const sg Int)");
    body.add("(assert (and (= g (+ (* 7 qg) sg)) (<= 0 sg) (< sg 7)))");
    body.add(RESIDUE_OF_X);
    body.add("(assert (not (= sg sx)))");
    check(out, form + ": the folds, the fixup and the last step give v's residue", "unsat", body);
  }

  // -----------------------------------------------------------------------------------------
  // The leap hash's unsigned compare (VARKA-242).
  // -----------------------------------------------------------------------------------------

  /**
   * {@code leap_hash.smt2}: {@link VarkaChrono#isLeapYear}'s hash, which
   * {@code VarkaChronoLowering}'s leap flag emits, is the Gregorian rule over every biased year to
   * {@link VarkaChrono#LEAP_HASH_MAX_BIASED_YEAR}, is not one year past it, and would not be with a
   * signed compare.
   */
  static String leapHash() {
    int max = VarkaChrono.LEAP_HASH_MAX_BIASED_YEAR;
    var out = new StringBuilder(LICENSE);
    out.append("""
        ; The leap flag's perfect hash (VarkaChrono.isLeapYear, VarkaChronoLowering's leap flag) is
        ; the Gregorian rule over its whole domain (VARKA-242). Per lane, y the year biased by
        ; YEAR_BIAS, a multiple of 400, so leapness is unchanged and y is non-negative:
        ;
        ;   leap = Integer.compareUnsigned((y * LEAP_HASH_M) & LEAP_HASH_MASK, LEAP_HASH_MAX) <= 0
        ;                                   IntVector.mul, and, compare ULE
        ;
        ; Over 32-bit vectors, whose bvmul, bvand and bvule are Java's int *, & and the unsigned
        ; compare exactly: the hash is the rule (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) for
        ; every biased year up to LEAP_HASH_MAX_BIASED_YEAR, is not one year past it, so the bound
        ; is exact, and a signed compare would be wrong inside the range. Over integers this ran
        ; past ten minutes under Z3 (VARKA-240); over bit-vectors it takes under a second. The file
        ; declares ALL so that the prelude, which it does not use, can be inserted.
        """);
    out.append(rendered("ALL"));
    out.append("\n(define-fun leap.hash ((y (_ BitVec 32))) (_ BitVec 32)\n  (bvand (bvmul y ")
        .append(bv(VarkaChrono.LEAP_HASH_M)).append(") ").append(bv(VarkaChrono.LEAP_HASH_MASK))
        .append("))\n");
    out.append("""
        (define-fun leap.gregorian ((y (_ BitVec 32))) Bool
          (and (= (bvurem y #x00000004) #x00000000)
               (or (not (= (bvurem y #x00000064) #x00000000))
                   (= (bvurem y #x00000190) #x00000000))))
        """);
    String unsigned = "(bvule (leap.hash y) " + bv(VarkaChrono.LEAP_HASH_MAX) + ")";
    String signed = "(bvsle (leap.hash y) " + bv(VarkaChrono.LEAP_HASH_MAX) + ")";
    check(out, "the hash, compared unsigned, is the Gregorian rule for every biased year to " + max,
        "unsat", List.of("(declare-const y (_ BitVec 32))", "(assert (bvule y " + bv(max) + "))",
            "(assert (not (= " + unsigned + " (leap.gregorian y))))"));
    check(out, "and it is not at " + (max + 1), "sat", List.of("(declare-const y (_ BitVec 32))",
        "(assert (= y " + bv(max + 1) + "))",
        "(assert (not (= " + unsigned + " (leap.gregorian y))))"));
    check(out, "compared signed, it is not the rule inside the range", "sat", List.of(
        "(declare-const y (_ BitVec 32))", "(assert (bvule y " + bv(max) + "))",
        "(assert (not (= " + signed + " (leap.gregorian y))))"));
    return out.toString();
  }

  /** An int as a 32-bit SMT-LIB bit-vector literal. */
  private static String bv(int v) {
    return String.format("#x%08x", v);
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
    var intMask = new ArrayList<String>();
    for (int a : INTS) {
      for (int k : new int[] {3, 6, 15, 16}) {
        intMask.add(eq("jint.and.low", lit(a) + " " + (1 << k), lit(a & ((1 << k) - 1))));
      }
    }
    equations(out, "jint.and.low is Java's int & with a low mask, at both signs", intMask);
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
    // The calendar's reciprocal form (VARKA-242): an int dividend times the double nearest 1 / d.
    var mul = new ArrayList<String>();
    for (long d : new long[] {3, 7, 12, 100, 365, 1461, 146097}) {
      double r = 1.0 / d;
      long raw = Double.doubleToRawLongBits(r);
      BigInteger mr = BigInteger.valueOf((raw & VarkaDivisionLowering.MANTISSA_52) | P52);
      BigInteger den = BigInteger.ONE.shiftLeft(52 - Math.getExponent(r));
      for (long v : new long[] {1, d - 1, d, d + 1, 3 * d, 584399, 20161385}) {
        rounded(mul, BigInteger.valueOf(v).multiply(mr), den, (double) v * r);
      }
    }
    equations(out, "jdouble.rne is Java's * of an int and the double nearest 1 / d", mul);
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
