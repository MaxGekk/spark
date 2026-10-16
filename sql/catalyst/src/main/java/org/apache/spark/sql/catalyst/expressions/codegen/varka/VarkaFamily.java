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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The expression families of the compiler that a session can switch off one at a time
 * (VARKA-318): the families of the chain {@code VarkaNodeCompiler} dispatches every node
 * through, less the date leaves, which read columns and fuse nothing of their own. A family that
 * is off declines every node it would claim, with {@link #offReason()} as the reason, so the
 * entry around that node runs on Spark's row engine and the rest of the projection still fuses.
 *
 * <p>The switch is {@link #switchKey()}, a session configuration declared in {@code SQLConf}, and
 * every message about a family names it, as StarRocks's "set jit_level = 0 to disable jit and
 * retry" names its own: the decline reason, the planning error a failure inside the family raises
 * ({@link #retry()}), and the {@code families:} line of the context record a kernel failure's
 * warning carries ({@link VarkaErrorContext}).
 *
 * <p>The switch covers the nodes the family <em>claims</em>. Code of one family that another
 * family's lowering calls directly - the calendar's year-month interval operand, the int
 * arithmetic's {@code greatest} and {@code least} folding through the condition family's pick -
 * runs under the claiming family's switch.
 */
public enum VarkaFamily {
  /**
   * {@code VarkaChronoCompiler}: day arithmetic ({@code date_add}, {@code datediff},
   * {@code unix_date}), the calendar fields ({@code year} to {@code weekofyear}),
   * {@code make_date}, {@code last_day}, {@code next_day}, {@code trunc}, {@code add_months} and a
   * date plus a year-month interval.
   */
  CALENDAR("calendar", "calendar"),
  /**
   * {@code VarkaIntervalCompiler}: year-month interval arithmetic, negation and {@code abs},
   * {@code make_ym_interval}, the year and month extractions, the multiply by an int and the
   * casts between intervals and ints.
   */
  INTERVAL("interval", "interval"),
  /**
   * {@code VarkaTimeCompiler}: the {@code TIME} functions ({@code hour} to {@code time_trunc},
   * {@code time_diff}, a time plus a day-time interval) and the casts to {@code TIME} and to a
   * day-time interval.
   */
  TIME("time", "time"),
  /**
   * {@code VarkaConditionCompiler}: a filter's predicates (comparisons, {@code IN}, the validity
   * tests and the connectives) and the value-side conditionals {@code IF}, {@code CASE WHEN} and
   * {@code coalesce}. Off, no filter fuses.
   */
  CONDITION("condition", "condition"),
  /**
   * The int arithmetic of {@code VarkaNodeCompiler}: {@code +}, {@code -}, {@code *} and unary
   * minus over int operands, checked under ANSI where the operands' bounds do not rule out
   * overflow, and {@code greatest} and {@code least}.
   */
  INT_ARITHMETIC("int arithmetic", "intArithmetic");

  /** The prefix every family's switch shares. */
  public static final String SWITCH_PREFIX = "spark.sql.codegen.varka.family.";

  private final String label;
  private final String key;

  VarkaFamily(String label, String key) {
    this.label = label;
    this.key = key;
  }

  /** The family's name in prose and in the compiler's chain, e.g. {@code int arithmetic}. */
  public String label() {
    return label;
  }

  /** The configuration that switches the family off when false. */
  public String switchKey() {
    return SWITCH_PREFIX + key + ".enabled";
  }

  /** The reason every node of the family declines with while its switch is off. */
  public String offReason() {
    return "Varka's " + label + " family is switched off (" + switchKey() + "=false)";
  }

  /** What a failure inside the family asks of the user who wants the query to run meanwhile. */
  public String retry() {
    return "To plan the query with Varka's " + label + " family on Spark's row engine, set "
        + switchKey() + " to false and retry.";
  }

  /**
   * {@code families} as the context record's {@code families:} line renders them: each family's
   * label and its switch, in declaration order, comma-separated; empty for none.
   */
  public static String render(Collection<VarkaFamily> families) {
    return List.of(values()).stream()
        .filter(families::contains)
        .map(f -> f.label + " (" + f.switchKey() + ")")
        .collect(Collectors.joining(", "));
  }
}
