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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.AddDays;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.AddMonths;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.And;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.BoundedDivide;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Compare;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ConstDivide;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DateDiff;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfMonth;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfWeek;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfWeekIso;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DayOfYear;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Greatest;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.GuardedDay;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.GuardedRange;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IfElse;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.InRanges;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntArith;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntNeg;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IsNotNull;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LastDay;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Least;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.MakeDate;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Month;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.NarrowLane;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.NextDay;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Not;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Or;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Quarter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.SubDays;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ThursdayOf;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.TruncDate;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.TruncDateDynamic;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.WeekDay;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.WeekOfYear;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Year;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.CompareOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Cond;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.TruncLevel;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.Intervals.Facts;

/**
 * Arm 1 of the spike: the IR as it is, a graph of {@link VarkaVectorIR} records.
 *
 * <p>{@link #build} makes the records the way the compiler does, by a direct constructor call for
 * each kind and no reflection, so its time is the cost of building the IR; {@link #analyze} walks
 * them with the sealed type's pattern {@code switch}, memoising by the records' own structural
 * {@code hashCode} and {@code equals}, which is how an analysis over the IR is written today.
 */
final class RecordsArm {

  private RecordsArm() {}

  /** The graph's outputs as records, built bottom-up by direct constructors. */
  static List<VarkaVectorIR> build(LoadedGraph g) {
    var nodes = new VarkaVectorIR[g.size()];
    for (int i = 0; i < g.size(); i++) {
      nodes[i] = make(g, i, nodes);
    }
    var roots = new ArrayList<VarkaVectorIR>(g.roots().length);
    for (int root : g.roots()) {
      roots.add(nodes[root]);
    }
    return roots;
  }

  private static VarkaVectorIR make(LoadedGraph g, int i, VarkaVectorIR[] n) {
    int[] c = g.children()[i];
    long[] s = g.scalars()[i];
    return switch (KindNames.of(g.kind()[i])) {
      case "AddDays" -> new AddDays(n[c[0]], n[c[1]]);
      case "AddMonths" -> new AddMonths(n[c[0]], n[c[1]]);
      case "And" -> new And((Cond) n[c[0]], (Cond) n[c[1]]);
      case "BoundedDivide" ->
          new BoundedDivide(n[c[0]], (int) s[0], (int) s[1], (int) s[2], (int) s[3]);
      case "ColumnRef" -> new ColumnRef((int) s[0], LaneType.values()[(int) s[1]]);
      case "Compare" -> new Compare(CompareOp.values()[(int) s[0]], n[c[0]], n[c[1]]);
      case "ConstDivide" -> new ConstDivide(n[c[0]], s[0], s[1]);
      case "DateDiff" -> new DateDiff(n[c[0]], n[c[1]]);
      case "DayOfMonth" -> new DayOfMonth(n[c[0]]);
      case "DayOfWeek" -> new DayOfWeek(n[c[0]]);
      case "DayOfWeekIso" -> new DayOfWeekIso(n[c[0]]);
      case "DayOfYear" -> new DayOfYear(n[c[0]]);
      case "Greatest" -> new Greatest(n[c[0]], n[c[1]]);
      case "GuardedDay" -> new GuardedDay(n[c[0]]);
      case "GuardedRange" -> new GuardedRange(n[c[0]], s[0], s[1]);
      case "IfElse" -> new IfElse((Cond) n[c[0]], n[c[1]], n[c[2]]);
      case "InRanges" -> new InRanges(n[c[0]], boxed(g.lists()[i]));
      case "IntArith" ->
          new IntArith(IntOp.values()[(int) s[0]], Overflow.values()[(int) s[1]], n[c[0]], n[c[1]]);
      case "IntNeg" -> new IntNeg(Overflow.values()[(int) s[0]], n[c[0]]);
      case "IsNotNull" -> new IsNotNull(n[c[0]]);
      case "LastDay" -> new LastDay(n[c[0]]);
      case "Least" -> new Least(n[c[0]], n[c[1]]);
      case "LiteralSlot" -> new LiteralSlot((int) s[0], LaneType.values()[(int) s[1]]);
      case "MakeDate" -> new MakeDate(n[c[0]], n[c[1]], n[c[2]], s[0] != 0);
      case "Month" -> new Month(n[c[0]]);
      case "NarrowLane" -> new NarrowLane(n[c[0]]);
      case "NextDay" -> new NextDay(n[c[0]], n[c[1]]);
      case "Not" -> new Not((Cond) n[c[0]]);
      case "Or" -> new Or((Cond) n[c[0]], (Cond) n[c[1]]);
      case "Quarter" -> new Quarter(n[c[0]]);
      case "SubDays" -> new SubDays(n[c[0]], n[c[1]]);
      case "ThursdayOf" -> new ThursdayOf(n[c[0]]);
      case "TruncDate" -> new TruncDate(n[c[0]], TruncLevel.values()[(int) s[0]]);
      case "TruncDateDynamic" -> new TruncDateDynamic(n[c[0]], n[c[1]]);
      case "WeekDay" -> new WeekDay(n[c[0]]);
      case "WeekOfYear" -> new WeekOfYear(n[c[0]]);
      case "Year" -> new Year(n[c[0]]);
      default -> throw new IllegalStateException(
          "no constructor call for " + KindNames.of(g.kind()[i]));
    };
  }

  private static List<Integer> boxed(int[] values) {
    var list = new ArrayList<Integer>(values.length);
    for (int v : values) {
      list.add(v);
    }
    return List.copyOf(list);
  }

  /**
   * The interval fact of every distinct node under the sealed switch, which has no {@code default}:
   * a kind added to the IR is a compile error here, which is what a flat layout gives up.
   */
  static Facts analyze(List<VarkaVectorIR> roots) {
    var memo = new HashMap<VarkaVectorIR, long[]>();
    var rootLo = new long[roots.size()];
    var rootHi = new long[roots.size()];
    for (int r = 0; r < roots.size(); r++) {
      long[] fact = fact(roots.get(r), memo);
      rootLo[r] = fact[0];
      rootHi[r] = fact[1];
    }
    long checksum = 0;
    for (long[] fact : memo.values()) {
      checksum += Intervals.mix(fact[0], fact[1]);
    }
    return new Facts(memo.size(), checksum, rootLo, rootHi);
  }

  private static long[] fact(VarkaVectorIR node, Map<VarkaVectorIR, long[]> memo) {
    long[] known = memo.get(node);
    if (known != null) {
      return known;
    }
    long[] fact = compute(node, memo);
    memo.put(node, fact);
    return fact;
  }

  private static long[] compute(VarkaVectorIR node, Map<VarkaVectorIR, long[]> memo) {
    return switch (node) {
      case ColumnRef r -> lane(r.lane());
      case LiteralSlot r -> lane(r.lane());
      case IntArith r -> {
        long[] a = fact(r.left(), memo);
        long[] b = fact(r.right(), memo);
        yield switch (r.op()) {
          case ADD -> new long[] {Intervals.add(a[0], b[0]), Intervals.add(a[1], b[1])};
          case SUB -> new long[] {Intervals.sub(a[0], b[1]), Intervals.sub(a[1], b[0])};
          case MUL -> new long[] {Intervals.mulLo(a[0], a[1], b[0], b[1]),
              Intervals.mulHi(a[0], a[1], b[0], b[1])};
        };
      }
      case IntNeg r -> {
        long[] a = fact(r.child(), memo);
        yield new long[] {-a[1], -a[0]};
      }
      case AddDays r -> {
        long[] a = fact(r.days(), memo);
        long[] b = fact(r.offset(), memo);
        yield new long[] {Intervals.add(a[0], b[0]), Intervals.add(a[1], b[1])};
      }
      case SubDays r -> {
        long[] a = fact(r.days(), memo);
        long[] b = fact(r.offset(), memo);
        yield new long[] {Intervals.sub(a[0], b[1]), Intervals.sub(a[1], b[0])};
      }
      case Greatest r -> {
        long[] a = fact(r.left(), memo);
        long[] b = fact(r.right(), memo);
        yield new long[] {Math.max(a[0], b[0]), Math.max(a[1], b[1])};
      }
      case Least r -> {
        long[] a = fact(r.left(), memo);
        long[] b = fact(r.right(), memo);
        yield new long[] {Math.min(a[0], b[0]), Math.min(a[1], b[1])};
      }
      case IfElse r -> {
        fact(r.cond(), memo);
        long[] a = fact(r.thenNode(), memo);
        long[] b = fact(r.elseNode(), memo);
        yield new long[] {Math.min(a[0], b[0]), Math.max(a[1], b[1])};
      }
      case GuardedRange r -> {
        long[] a = fact(r.child(), memo);
        long lo = Math.max(a[0], r.lo());
        long hi = Math.min(a[1], r.hi());
        yield lo > hi ? new long[] {r.lo(), r.hi()} : new long[] {lo, hi};
      }
      case ConstDivide r -> divided(fact(r.child(), memo), r.divisor());
      case BoundedDivide r -> divided(fact(r.child(), memo), r.divisor());
      case GuardedDay r -> fact(r.days(), memo);
      case NarrowLane r -> fact(r.child(), memo);
      case Compare r -> {
        fact(r.left(), memo);
        fact(r.right(), memo);
        yield new long[] {0, 1};
      }
      case And r -> {
        fact(r.left(), memo);
        fact(r.right(), memo);
        yield new long[] {0, 1};
      }
      case Or r -> {
        fact(r.left(), memo);
        fact(r.right(), memo);
        yield new long[] {0, 1};
      }
      case Not r -> {
        fact(r.child(), memo);
        yield new long[] {0, 1};
      }
      case IsNotNull r -> {
        fact(r.child(), memo);
        yield new long[] {0, 1};
      }
      case InRanges r -> {
        fact(r.child(), memo);
        yield new long[] {0, 1};
      }
      case Month r -> fixed(r.days(), memo, 1, 12);
      case DayOfMonth r -> fixed(r.days(), memo, 1, 31);
      case DayOfYear r -> fixed(r.days(), memo, 1, 366);
      case Quarter r -> fixed(r.days(), memo, 1, 4);
      case DayOfWeek r -> fixed(r.days(), memo, 1, 7);
      case DayOfWeekIso r -> fixed(r.days(), memo, 1, 7);
      case WeekDay r -> fixed(r.days(), memo, 0, 6);
      case WeekOfYear r -> fixed(r.days(), memo, 1, 53);
      case Year r -> fixed(r.days(), memo, Intervals.INT_LO, Intervals.INT_HI);
      case LastDay r -> fixed(r.days(), memo, Intervals.INT_LO, Intervals.INT_HI);
      case ThursdayOf r -> fixed(r.days(), memo, Intervals.INT_LO, Intervals.INT_HI);
      case TruncDate r -> fixed(r.days(), memo, Intervals.INT_LO, Intervals.INT_HI);
      case AddMonths r -> {
        fact(r.days(), memo);
        fact(r.months(), memo);
        yield new long[] {Intervals.INT_LO, Intervals.INT_HI};
      }
      case DateDiff r -> {
        fact(r.end(), memo);
        fact(r.start(), memo);
        yield new long[] {Intervals.INT_LO, Intervals.INT_HI};
      }
      case NextDay r -> {
        fact(r.days(), memo);
        fact(r.offset(), memo);
        yield new long[] {Intervals.INT_LO, Intervals.INT_HI};
      }
      case TruncDateDynamic r -> {
        fact(r.days(), memo);
        fact(r.level(), memo);
        yield new long[] {Intervals.INT_LO, Intervals.INT_HI};
      }
      case MakeDate r -> {
        fact(r.year(), memo);
        fact(r.month(), memo);
        fact(r.day(), memo);
        yield new long[] {Intervals.INT_LO, Intervals.INT_HI};
      }
    };
  }

  private static long[] lane(LaneType lane) {
    return lane == LaneType.INT
        ? new long[] {Intervals.INT_LO, Intervals.INT_HI}
        : new long[] {Intervals.TOP_LO, Intervals.TOP_HI};
  }

  private static long[] fixed(
      VarkaVectorIR child, Map<VarkaVectorIR, long[]> memo, long lo, long hi) {
    fact(child, memo);
    return new long[] {lo, hi};
  }

  private static long[] divided(long[] a, long divisor) {
    return divisor > 0
        ? new long[] {a[0] / divisor, a[1] / divisor}
        : new long[] {Intervals.TOP_LO, Intervals.TOP_HI};
  }
}
