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

/**
 * The interval arithmetic of the spike's bottom-up analysis, shared by every arm so that they
 * differ only in how they walk and store the graph. A fact is a closed interval of {@code long}s,
 * saturating at {@link #TOP_LO} and {@link #TOP_HI}, which sit far enough inside the {@code long}
 * range that adding two bounds never wraps.
 *
 * <p>It is a stand-in for {@code VarkaRangeAnalysis}, which is Scala in the main build and cannot
 * be compiled beside the IR on its own: it answers the same kind of question, bottom-up and per
 * node, with a transfer function for each of the 37 kinds, and nothing here claims to be sound for
 * the compiler. What the arms must agree on is its answer.
 */
final class Intervals {

  private Intervals() {}

  static final long TOP_HI = Long.MAX_VALUE / 2;
  static final long TOP_LO = -TOP_HI;
  static final long INT_LO = Integer.MIN_VALUE;
  static final long INT_HI = Integer.MAX_VALUE;

  static long sat(long v) {
    return v < TOP_LO ? TOP_LO : Math.min(v, TOP_HI);
  }

  static long add(long a, long b) {
    return sat(a + b);
  }

  static long sub(long a, long b) {
    return sat(a - b);
  }

  /** {@code a * b}, saturating. */
  static long mul(long a, long b) {
    long high = Math.multiplyHigh(a, b);
    long low = a * b;
    if ((high == 0 && low >= 0) || (high == -1 && low < 0)) {
      return sat(low);
    }
    return high < 0 ? TOP_LO : TOP_HI;
  }

  /** The lower bound of {@code [aLo, aHi] * [bLo, bHi]}. */
  static long mulLo(long aLo, long aHi, long bLo, long bHi) {
    return Math.min(Math.min(mul(aLo, bLo), mul(aLo, bHi)), Math.min(mul(aHi, bLo), mul(aHi, bHi)));
  }

  /** The upper bound of {@code [aLo, aHi] * [bLo, bHi]}. */
  static long mulHi(long aLo, long aHi, long bLo, long bHi) {
    return Math.max(Math.max(mul(aLo, bLo), mul(aLo, bHi)), Math.max(mul(aHi, bLo), mul(aHi, bHi)));
  }

  /** An order-independent mix of one node's fact, summed over a graph's distinct nodes. */
  static long mix(long lo, long hi) {
    return lo * 0x9E3779B97F4A7C15L + hi;
  }

  /**
   * What an analysis answers for a graph: how many distinct nodes, the sum of their facts' mix,
   * and each output's fact.
   */
  record Facts(int distinct, long checksum, long[] rootLo, long[] rootHi) {
    boolean sameAs(Facts other) {
      return distinct == other.distinct && checksum == other.checksum
          && java.util.Arrays.equals(rootLo, other.rootLo)
          && java.util.Arrays.equals(rootHi, other.rootHi);
    }

    @Override
    public String toString() {
      return "Facts[distinct=" + distinct + ", checksum=" + checksum + ", roots="
          + java.util.Arrays.toString(rootLo) + ".." + java.util.Arrays.toString(rootHi) + "]";
    }
  }
}
