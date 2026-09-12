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

package org.apache.spark.sql.varka.bench;

import java.util.List;

/**
 * Chained date expressions, for the question {@link Surface} cannot answer: how much does the
 * width of the vector datapath buy?
 *
 * <p><b>Why a second list.</b> The surface is one entry per expression, in the spelling a
 * reader would write, and that is what makes it a coverage document. It is also what makes it
 * the wrong instrument for a datapath measurement. Measured on the development laptop at 1e9
 * rows, {@code date_add(d, 3)} runs at 0.5 ns/row - that one *is* committed, in
 * {@code benchmarks/DateSurface-varka-jdk25-results.txt} - while reading four bytes and
 * writing four, which is about 15 GB/s and so single-core DRAM bandwidth on that machine.
 * The kernel is waiting for memory, not for the vector unit, and no width of datapath moves
 * bytes faster. Roughly a third of the surface is in that regime.
 *
 * <p><b>What these are instead.</b> The same operations composed until the arithmetic per byte
 * read rises by an order of magnitude and the kernel is bound by what it computes rather than
 * by what it loads. Op counts from {@code dev/varka_emit.sh --table}, against 34 for
 * {@code year(d)} and 64 for {@code weekofyear(d)}.
 *
 * <p><b>And they mix the types on purpose.</b> Varka covers three - DATE, INT and the
 * year-month interval, all int32 in one lane - and a benchmark of dates alone understates that
 * to a reader and exercises less of the compiler. Seven of the twelve entries carry a date, an
 * int and an interval in one expression; one produces an interval rather than consuming one.
 * That is closer to real SQL than a chain of one type, and it is the claim the milestone
 * actually wants to make.
 *
 * <p><b>Two other things follow from the arithmetic, and they are why this list exists at the
 * size it does.</b> The job-size rule of {@code DateSurfaceBenchmark} fails a row whose fixed
 * share is over 5%, so an entry has to be slow enough per row that the job's constant cost
 * disappears behind it. These entries are chosen to clear that at **1e8 rows**, where the
 * cached table is 2.4 GB and resident, rather than at the 5e8 the surface needs and that no
 * runner in the pool can hold. More work per row and more rows are two ways to buy executor
 * time, and only one of them collides with memory.
 *
 * <p><b>The sizing is a prediction, and is registered as one.</b> {@code PLAN_TASK_62.md}
 * 11.13 records it with its arithmetic and will score it against the first committed chain
 * file. The numbers behind it - a per-iteration fixed cost near 18 ms, roughly 0.02 ns per
 * emitter op over a memory floor near 0.8 ns, and so 3.9 to 5.2 ns/row for the entries here -
 * come from a GitHub dispatch on 11 September 2026 whose results were never committed,
 * because that run failed the fixed-share rule and predates the change that made a failing
 * run upload its evidence. They are therefore scratch figures in the sense
 * {@code dev/varka_quote_allowlist.txt} means it, and they are written here as the reasoning
 * that chose the list rather than as measurements anyone should quote.
 *
 * <p>Every entry is checked to fuse before it is added - the driver's {@code --expect-fused}
 * fails the run otherwise - and the op count is the reason each was chosen over a lighter
 * spelling of the same shape. A chain that folds is worth knowing about but not worth timing:
 * {@code datediff(date_from_unix_date(unix_date(d) + i), d)} collapses to 9 ops, because
 * {@code date_from_unix_date(unix_date(d))} is the identity and the compiler knows it.
 *
 * <p><b>Shapes that decline, found while choosing these and worth recording.</b>
 * {@code datediff(d2, d) * i} and {@code i % 20} both decline - an int multiply by a column
 * and an int remainder - although multiplying by a literal is fine, as
 * {@code CAST(month(d) AS INTERVAL YEAR) * 3} in {@link Surface} shows. So does
 * {@code make_ym_interval(i, i)}, where the surface's
 * {@code make_ym_interval(year(d), month(d))} fuses. These are coverage gaps in the int32
 * arm rather than anything wrong here, and they are why several otherwise natural mixed-type
 * spellings are absent.
 */
public final class Chains {

  /**
   * One chain: the expression, and the emitter op count that is the reason it is here.
   *
   * <p>The count is data rather than a comment because it is the entry's whole justification,
   * and a justification a test cannot read is one that goes stale silently. Nesting depth is
   * not a usable proxy for it - {@code weekofyear(add_months(d, i))} is two calls and 176 ops,
   * while {@code month(next_day(date_add(d, i), 'MONDAY'))} is three calls and 55 - because
   * what costs is the arithmetic inside each operation, not how many there are.
   *
   * @param expr the projection, in the spelling a reader would write
   * @param emitterOps IntVector and VectorMask invocations in the dense loop, from
   *                   {@code dev/varka_emit.sh --table "<expr>"}, which is the source of truth
   *                   and regenerates every number here
   */
  record Chain(String expr, int emitterOps) {}

  /**
   * Below this an entry does not earn its place: it would not clear the 5% fixed-share rule at
   * 1e8 rows, which is what the whole list is arranged around. See the class comment for the
   * arithmetic - roughly 0.02 ns per op over a 0.8 ns memory floor, against a 3.6 ns/row
   * threshold, so about 140 ops is the break-even and 150 is the margin.
   */
  static final int MIN_OPS = 150;

  private static final List<Chain> CHAINS = List.of(
      // Eight of the twelve carry a date, an int and an interval column at once. That is the
      // claim the milestone wants to make and the reason this list is not about dates.
      new Chain("(year(d + ym) - year(d2)) * 12 + month(d + ymm) + i", 304),
      new Chain("dayofweek(last_day(d + ym) + CAST(i AS INTERVAL MONTH))", 303),
      new Chain("weekofyear(add_months(d, i) + ymm)", 288),
      new Chain("year(d - ymm) * 100 + dayofyear(d + ymy) + i", 280),
      // An interval *output*, built from two decomposed dates and an int: the third type as a
      // result and not only as an input.
      new Chain("make_ym_interval(year(d + ym), month(d + ymm) + i)", 274),
      new Chain("quarter(d + ymm + CAST(i AS INTERVAL MONTH))", 262),
      new Chain("year(add_months(d + ym, i))", 258),
      new Chain("datediff(last_day(d + ym), date_add(d, i))", 178),
      // Four without an interval, for contrast with the surface's own date/int rows and
      // because the last of them is the control described below.
      new Chain("dayofyear(add_months(last_day(date_add(d, i)), 1))", 218),
      new Chain("quarter(last_day(add_months(d, i)))", 211),
      // Two independent chains feeding one subtract: the only entry with instruction-level
      // parallelism of its own, and so the control for whether the others are latency-bound.
      new Chain("datediff(last_day(d), last_day(add_months(d, i)))", 207),
      new Chain("extract(YEAROFWEEK FROM add_months(d, i))", 163));

  /**
   * The chains, ordered by family rather than by cost, the way {@link Surface#ENTRIES} is: a
   * shard is a stride over this list, so families must be interleaved by the stride rather
   * than pre-sorted, or the shards' run times diverge.
   */
  public static final List<Surface.Entry> ENTRIES =
      CHAINS.stream().map(c -> Surface.Entry.projection(c.expr())).toList();

  /** The recorded op count for an entry's label, for the test that checks it earns its place. */
  static int emitterOps(String label) {
    return CHAINS.stream().filter(c -> c.expr().equals(label)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException("no chain " + label))
        .emitterOps();
  }

  private Chains() {}
}
