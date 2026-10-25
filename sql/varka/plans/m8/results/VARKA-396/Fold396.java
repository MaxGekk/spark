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

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * A select whose mask goes through a long and back: blend(fromLong(toLong(a > b) & w)), with w
 * -1 on the branch the loop takes (a column with no nulls), so that C2 folds the AND away and
 * compiles blend(fromLong(toLong(compare))). Each variant is checked against scalar arithmetic
 * on every call; the first wrong call is printed.
 */
public class Fold396 {
  static final VectorSpecies<Long> S = LongVector.SPECIES_256;

  /** The validity word Varka reads for a column: all ones where the column has no nulls. */
  static long validity(int nullCount, long[] bits, int i) {
    return nullCount == 0 ? -1L : bits[i >> 2] >>> (i & 63) & 0xF;
  }

  /** The masked body as Varka emits it, its validity words read per group. */
  static void viaValidity(long[] a, long[] b, long[] c, long[] out, int nullCount, long[] bits) {
    for (int i = 0; i < S.loopBound(a.length); i += S.length()) {
      LongVector va = LongVector.fromArray(S, a, i);
      LongVector vb = LongVector.fromArray(S, b, i);
      long w0 = validity(nullCount, bits, i);
      long w1 = validity(nullCount, bits, i);
      long m = va.compare(VectorOperators.GT, vb).toLong();
      long kT = m & w0 & w1;
      LongVector s = vb.add(LongVector.fromArray(S, c, i));
      s.blend(va, VectorMask.fromLong(S, kT)).intoArray(out, i);
    }
  }

  /** The folded form written out: the mask to a long and straight back. */
  static void roundTrip(long[] a, long[] b, long[] c, long[] out) {
    for (int i = 0; i < S.loopBound(a.length); i += S.length()) {
      LongVector va = LongVector.fromArray(S, a, i);
      LongVector vb = LongVector.fromArray(S, b, i);
      long m = va.compare(VectorOperators.GT, vb).toLong();
      LongVector s = vb.add(LongVector.fromArray(S, c, i));
      s.blend(va, VectorMask.fromLong(S, m)).intoArray(out, i);
    }
  }

  interface Body {
    void run(long[] a, long[] b, long[] c, long[] out);
  }

  static void check(String name, Body body, long[] a, long[] b, long[] c, int calls) {
    long[] out = new long[a.length];
    for (int call = 0; call < calls; call++) {
      body.run(a, b, c, out);
      for (int i = 0; i < a.length; i++) {
        long want = a[i] > b[i] ? a[i] : b[i] + c[i];
        if (out[i] != want) {
          System.out.printf("%s: first wrong at call %d, row %d: want %d got %d%n", name, call,
              i, want, out[i]);
          return;
        }
      }
    }
    System.out.printf("%s: no difference in %d calls%n", name, calls);
  }

  public static void main(String[] args) {
    int calls = args.length > 0 ? Integer.parseInt(args[0]) : 200_000;
    // Row 396's first lane group, and a second group: rows 0 and 2 have a > b.
    long[] a = {4460076517051L, 0, 18898746763276L, 0, -50266100486726L, 7, 9, -49607829850524L};
    long[] b = {4421922265370L, 35258963307884L, -59385115539699L, 0, 38382192361867L, 5, 1, 0};
    long[] c = {-36599559927214L, 0, -25562645782204L, 0, -41690266522730L, 3, 4, 41053864906336L};
    long[] bits = new long[1];
    String which = args.length > 1 ? args[1] : "both";
    if (!which.equals("roundTrip")) {
      check("viaValidity", (x, y, z, o) -> viaValidity(x, y, z, o, 0, bits), a, b, c, calls);
    }
    if (!which.equals("viaValidity")) {
      check("roundTrip", Fold396::roundTrip, a, b, c, calls);
    }
  }
}
