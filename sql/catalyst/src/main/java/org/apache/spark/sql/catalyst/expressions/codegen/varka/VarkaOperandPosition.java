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

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.*;

/**
 * The operand positions whose node kinds the emitter restricts, and which kinds each admits - one
 * rule that the compiler asks when it builds an operand and the emitter asks when it accepts one
 * (VARKA-86). Stated twice, the rule drifted: {@code date_add(d, weekday(d2) + 1)} was fused in
 * EXPLAIN and refused at emit time, which the evaluator turns into a silent per-batch fallback.
 *
 * <p>{@link #admits} is a switch over every node kind with no default, so a new kind does not
 * compile until each position says whether it takes it: widening a position widens the compiler
 * and the emitter together. The positions not listed here - an int arithmetic operand, an int
 * argument, a comparison operand - take any int-valued node, and the emitter checks nothing there.
 */
public enum VarkaOperandPosition {
  /**
   * The day count of {@code AddDays}/{@code SubDays}: a literal slot, a column, or int arithmetic
   * over those, {@code date_add(d, i * 7)}. Not every int node: a date-valued subtree here would
   * be read as a day count and give a plausible wrong date.
   */
  DAY_OFFSET,

  /**
   * The month count of {@code AddMonths}: a literal slot, a column, or int arithmetic -
   * {@code d - ym_col} is an {@code IntNeg} over the interval column, {@code CAST(i AS INTERVAL
   * YEAR)} that column times twelve. A derived count is safe here because {@code AddMonths}
   * guards the count's own value at run time ({@code Analysis.Refusal.MONTH_COUNT}), whatever
   * produced it.
   */
  MONTH_COUNT,

  /**
   * {@code NextDay}'s weekday: a literal slot or a column, the strictest position. Its range
   * comes from a compile-time fold or from the derived weekday leaf, and no runtime guard stands
   * behind it, so a derived value here would reach {@code emitFloorMod7} unchecked. The compiler
   * builds only these two kinds in this position; the check is the emitter's defence against a
   * future producer.
   */
  WEEKDAY;

  /** Whether this position takes {@code node}. */
  public boolean admits(VarkaVectorIR node) {
    return switch (node) {
      case LiteralSlot _, ColumnRef _ -> true;
      case IntArith _, IntNeg _ -> this != WEEKDAY;
      case AddDays _, SubDays _, DateDiff _, IfElse _, Greatest _, Least _, DayOfWeek _,
           WeekDay _, DayOfWeekIso _, NextDay _, ThursdayOf _, AddMonths _, MakeDate _,
           ConstDivide _, BoundedDivide _, GuardedDay _, GuardedRange _, NarrowLane _,
           Year _, Month _, DayOfMonth _, Quarter _, DayOfYear _, LastDay _, TruncDate _,
           TruncDateDynamic _, WeekOfYear _,
           Compare _, And _, Or _, Not _, IsNotNull _, InRanges _ -> false;
    };
  }

  /** What this position takes, for a message. */
  public String takes() {
    return this == WEEKDAY ? "a literal slot or a column"
        : "a literal slot, a column or int arithmetic";
  }
}
