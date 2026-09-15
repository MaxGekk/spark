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
 * The AVX2 fallback for a long->double->long floor division: no conversion instruction at
 * all. For 0 <= v < 2^52, (v | 0x4330000000000000L) reinterpreted as a double is exactly
 * 2^52 + v, so subtracting 2^52 yields v as a double with or, reinterpret and sub. Back:
 * an integer-valued double q in [0, 2^52) plus 2^52 has q in its low 52 mantissa bits, so
 * reinterpret and mask recover it. There is no lanewise floor in the Vector API, so the
 * quotient is rounded to nearest with the same 2^52 trick and stepped down where rounding
 * went up - a compare and a blend.
 *
 * On 15 September 2026 under -XX:UseAVX=2 (Zen 5), C2's compilation of magic() is 18 vsubpd,
 * 12 vaddpd, 6 vpor, 6 vpand, 6 vmulpd, 6 vcmpgtpd and no conversion, extraction or call at
 * all; 0 wrong quotients over 65536 nanos-of-day values at 4 and at 8 lanes.
 */
public class MagicProbe {
  static final VectorSpecies<Long> LS = LongVector.SPECIES_PREFERRED;
  static final VectorSpecies<Double> DS = DoubleVector.SPECIES_PREFERRED;
  static final long D = 3_600_000_000_000L;
  static final double RD = 1.0 / D;
  static final long MAGIC_BITS = 0x4330000000000000L;      // 2^52 as a double, bit pattern
  static final double TWO52 = 4503599627370496.0;
  static final long MANTISSA = 0x000FFFFFFFFFFFFFL;

  static long magic(long[] in, long[] out) {
    long acc = 0;
    LongVector mb = LongVector.broadcast(LS, MAGIC_BITS);
    DoubleVector two52 = DoubleVector.broadcast(DS, TWO52);
    DoubleVector rd = DoubleVector.broadcast(DS, RD);
    LongVector mant = LongVector.broadcast(LS, MANTISSA);
    for (int i = 0; i < in.length; i += LS.length()) {
      LongVector v = LongVector.fromArray(LS, in, i);
      DoubleVector d = v.or(mb).reinterpretAsDoubles().sub(two52);   // exact: v < 2^52
      DoubleVector q = d.mul(rd);
      // No lanewise floor in the Vector API: round to nearest with the 2^52 trick (exact for
      // q < 2^51), then step down where rounding went up. vaddpd, vsubpd, vcmppd, a blend.
      DoubleVector qr = q.add(two52).sub(two52);
      VectorMask<Double> up = qr.compare(VectorOperators.GT, q);
      DoubleVector fl = qr.lanewise(VectorOperators.SUB, 1.0, up);
      LongVector h = fl.add(two52).reinterpretAsLongs().and(mant);   // exact: fl < 2^52
      h.intoArray(out, i);
      acc += out[i];
    }
    return acc;
  }
  public static void main(String[] a) {
    int n = 1 << 16;
    long[] in = new long[n], out = new long[n];
    java.util.Random r = new java.util.Random(7);
    for (int i = 0; i < n; i++) in[i] = (long) (r.nextDouble() * 86_399_999_999_999L);
    long s = 0;
    int rounds = a.length > 0 ? Integer.parseInt(a[0]) : 3000;
    for (int k = 0; k < rounds; k++) s += magic(in, out);
    magic(in, out);
    long bad = 0;
    for (int i = 0; i < n; i++) if (out[i] != in[i] / D) bad++;
    System.out.println("species long=" + LS.length() + "; checksum=" + s
        + "; wrong quotients over " + n + ": " + bad);
  }
}
