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
 * The transfer functions of {@link Intervals}' analysis for the flat arms, as a table: a kind's
 * category, and for the kinds whose answer is a constant range, the range. {@link RecordsArm}
 * writes the same functions as a sealed {@code switch} over the records; the two must agree, and
 * the harness checks that they do on every graph.
 */
final class RowTransfer {

  private RowTransfer() {}

  static final int LEAF = 0;
  static final int IARITH = 1;
  static final int NEG = 2;
  static final int ADDK = 3;
  static final int SUBK = 4;
  static final int GREATEST = 5;
  static final int LEAST = 6;
  static final int HULL = 7;
  static final int BOOL = 8;
  static final int FIXED = 9;
  static final int PASS0 = 10;
  static final int GUARD = 11;
  static final int DIV = 12;

  /** The category of every kind, by kind index. */
  static final int[] CATEGORY;
  /** The range of a {@link #FIXED} kind, by kind index. */
  static final long[] FIXED_LO;
  static final long[] FIXED_HI;

  static {
    KindTable table = KindNames.TABLE;
    CATEGORY = new int[table.size()];
    FIXED_LO = new long[table.size()];
    FIXED_HI = new long[table.size()];
    for (int k = 0; k < table.size(); k++) {
      String name = table.kind(k).name();
      long lo = Intervals.INT_LO;
      long hi = Intervals.INT_HI;
      int category;
      switch (name) {
        case "ColumnRef", "LiteralSlot" -> category = LEAF;
        case "IntArith" -> category = IARITH;
        case "IntNeg" -> category = NEG;
        case "AddDays" -> category = ADDK;
        case "SubDays" -> category = SUBK;
        case "Greatest" -> category = GREATEST;
        case "Least" -> category = LEAST;
        case "IfElse" -> category = HULL;
        case "Compare", "And", "Or", "Not", "IsNotNull", "InRanges" -> category = BOOL;
        case "GuardedDay", "NarrowLane" -> category = PASS0;
        case "GuardedRange" -> category = GUARD;
        case "ConstDivide", "BoundedDivide" -> category = DIV;
        default -> {
          category = FIXED;
          switch (name) {
            case "Month" -> { lo = 1; hi = 12; }
            case "DayOfMonth" -> { lo = 1; hi = 31; }
            case "DayOfYear" -> { lo = 1; hi = 366; }
            case "Quarter" -> { lo = 1; hi = 4; }
            case "DayOfWeek", "DayOfWeekIso" -> { lo = 1; hi = 7; }
            case "WeekDay" -> { lo = 0; hi = 6; }
            case "WeekOfYear" -> { lo = 1; hi = 53; }
            case "Year", "LastDay", "ThursdayOf", "TruncDate", "AddMonths", "DateDiff", "NextDay",
                 "TruncDateDynamic", "MakeDate" -> { }
            default -> throw new IllegalStateException("no transfer function for " + name);
          }
        }
      }
      CATEGORY[k] = category;
      FIXED_LO[k] = lo;
      FIXED_HI[k] = hi;
    }
  }

  /**
   * Computes row {@code id}'s fact into {@code lo[id]} and {@code hi[id]} from its children's, and
   * from up to two scalars the category reads: {@code s0} is {@code IntArith}'s op, a guard's lower
   * bound or a divisor, and {@code s1} a leaf's lane or a guard's upper bound.
   */
  static void apply(
      int kind, int id, int c0, int c1, int c2, long s0, long s1, long[] lo, long[] hi) {
    switch (CATEGORY[kind]) {
      case LEAF -> {
        if (s1 == 0) {
          lo[id] = Intervals.INT_LO;
          hi[id] = Intervals.INT_HI;
        } else {
          lo[id] = Intervals.TOP_LO;
          hi[id] = Intervals.TOP_HI;
        }
      }
      case IARITH -> {
        if (s0 == 0) {
          lo[id] = Intervals.add(lo[c0], lo[c1]);
          hi[id] = Intervals.add(hi[c0], hi[c1]);
        } else if (s0 == 1) {
          lo[id] = Intervals.sub(lo[c0], hi[c1]);
          hi[id] = Intervals.sub(hi[c0], lo[c1]);
        } else {
          lo[id] = Intervals.mulLo(lo[c0], hi[c0], lo[c1], hi[c1]);
          hi[id] = Intervals.mulHi(lo[c0], hi[c0], lo[c1], hi[c1]);
        }
      }
      case NEG -> {
        lo[id] = -hi[c0];
        hi[id] = -lo[c0];
      }
      case ADDK -> {
        lo[id] = Intervals.add(lo[c0], lo[c1]);
        hi[id] = Intervals.add(hi[c0], hi[c1]);
      }
      case SUBK -> {
        lo[id] = Intervals.sub(lo[c0], hi[c1]);
        hi[id] = Intervals.sub(hi[c0], lo[c1]);
      }
      case GREATEST -> {
        lo[id] = Math.max(lo[c0], lo[c1]);
        hi[id] = Math.max(hi[c0], hi[c1]);
      }
      case LEAST -> {
        lo[id] = Math.min(lo[c0], lo[c1]);
        hi[id] = Math.min(hi[c0], hi[c1]);
      }
      case HULL -> {
        lo[id] = Math.min(lo[c1], lo[c2]);
        hi[id] = Math.max(hi[c1], hi[c2]);
      }
      case BOOL -> {
        lo[id] = 0;
        hi[id] = 1;
      }
      case FIXED -> {
        lo[id] = FIXED_LO[kind];
        hi[id] = FIXED_HI[kind];
      }
      case PASS0 -> {
        lo[id] = lo[c0];
        hi[id] = hi[c0];
      }
      case GUARD -> {
        long l = Math.max(lo[c0], s0);
        long h = Math.min(hi[c0], s1);
        lo[id] = l > h ? s0 : l;
        hi[id] = l > h ? s1 : h;
      }
      case DIV -> {
        if (s0 > 0) {
          lo[id] = lo[c0] / s0;
          hi[id] = hi[c0] / s0;
        } else {
          lo[id] = Intervals.TOP_LO;
          hi[id] = Intervals.TOP_HI;
        }
      }
      default -> throw new IllegalStateException("category " + CATEGORY[kind]);
    }
  }
}
