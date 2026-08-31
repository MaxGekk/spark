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
 * The byte-affecting emit inputs that are not the shape: how wide a loop method may be, whether
 * common subexpressions are shared, which of the three mod-7 lowerings to emit, and one pure
 * fault injector. Everything here changes the bytes {@link VarkaLoopEmitter#emit} produces for a
 * given {@link VarkaShapeKey}, so it is part of that key rather than beside it.
 *
 * <p>Task 23 introduced this record to replace five {@code private static volatile} hook fields
 * on the emitter, an {@code AtomicLong} write generation, five package-private setters, two
 * package-private queries, a re-export shim in the catalyst test jar, a reflection-based
 * completeness test, and three reads in the shape cache: a JVM-wide gate that refused every
 * lookup while any hook was set, a snapshot of the generation before the emit walk and a re-check
 * after it. That machinery existed because the hooks were global mutable state the key could not
 * see. Options travel as a value on the call instead, so the three races it was guarding against
 * cannot be expressed:
 *
 * <ol>
 *   <li>a hook set between the cache's gate and the emit walk's snapshot - an unbounded window,
 *       since the caller may block on another task's in-flight load - was already set when the
 *       snapshot was taken, so the re-check passed and the poisoned bytes were cached under the
 *       plain key;</li>
 *   <li>the gate was JVM-wide, so while any suite held a hook every unrelated concurrent query
 *       threw instead of simply emitting uncached;</li>
 *   <li>every write bumped the generation, resets included, so one suite <i>clearing</i> its hook
 *       spuriously failed an unrelated thread's in-flight emit.</li>
 * </ol>
 *
 * <p>The record also removes an illegal state by construction. The two mod-7 reference variants
 * used to be independent booleans that could both be set at once, where the emitter silently
 * preferred one; {@link FloorMod7} makes the choice exclusive.
 *
 * <p><b>Defaults hash to what they always hashed.</b> {@link VarkaShapeCacheImpl#shapeHash}
 * renders these into the hash only when they differ from {@link #DEFAULTS}, so production hashes,
 * class names and telemetry are unchanged bit for bit and only the variants a suite asks for get
 * their own identity. They have to reach the hash at all because the cache's execution side table
 * is keyed on the hash alone while the map is keyed on the full key - options in one but not the
 * other would merge two variants' execution identities.
 *
 * @param groupBudget the most vector ops one emitted loop method may carry; see
 *                    {@link VarkaLoopEmitter#GROUP_BUDGET} for the measured reason it is 16, and
 *                    for the retuning question the parity benchmark prices by varying it.
 * @param cse whether shared subtrees are computed once and reused. Results must not change - CSE
 *            is an optimization, never a semantics change - and the emitter suite pins exactly
 *            that; the parity benchmark uses it to price CSE itself.
 * @param floorMod7 which lowering {@code dayofweek}/{@code weekday} use for their mod-7.
 * @param civilFromDays which lowering the four calendar extractions use for their day-of-era
 *                      step. Both are shipped and differentially tested against each other;
 *                      see the enum for the trade between them.
 * @param misdescribeAdd emits {@code AddDays} against a deliberately wrong descriptor (an unerased
 *                       {@code IntVector} parameter instead of {@code Vector}). The class still
 *                       passes bytecode verification - member resolution happens at link time - so
 *                       the failure surfaces on first execution as a {@code NoSuchMethodError}
 *                       naming {@code IntVector.add}. The suite pins that, so a future descriptor
 *                       regression is diagnosable from the error alone.
 */
public record VarkaEmitOptions(
    int groupBudget,
    boolean cse,
    FloorMod7 floorMod7,
    CivilFromDays civilFromDays,
    boolean misdescribeAdd) {

  /**
   * The three mod-7 lowerings. {@link #MAGIC} is what ships: two 15-bit digit-sum folds followed
   * by an exact Granlund-Montgomery magic division (task 14's follow-up). The other two are the
   * reference variants the parity benchmark and the differential suite check it against -
   * {@link #DIGIT_SUM} is the full base-8 digit sum that shipped with task 11, and {@link #DIV} is
   * the certainly-correct lanewise divide, which scalarizes on every lane type this JVM has.
   */
  public enum FloorMod7 { MAGIC, DIV, DIGIT_SUM }

  /**
   * The two civil-from-days lowerings (task 26), differing only in how they reach the day of
   * era. {@link #TOTAL} splits the dividend so the arithmetic is correct for every {@code int}
   * day and needs no runtime check. {@link #NARROWED} divides once, which is about five vector
   * ops cheaper, but is defined only over {@link VarkaChrono#NARROW_MIN_DAYS}..
   * {@link VarkaChrono#NARROW_MAX_DAYS} - years -12800 to 33134, which contains every date
   * SQL can write but is reachable past by {@code date_add} - so it also emits a per-lane guard
   * and reports {@link VarkaFusedKernel#STATUS_CHRONO_RANGE} for a batch it cannot compute,
   * which the caller then recomputes on the row engine.
   *
   * <p>Both ship: whichever is not the default stays a live reference variant the differential
   * suite checks the other against, the way {@link FloorMod7} keeps its two.
   */
  public enum CivilFromDays { TOTAL, NARROWED }

  /** What production always emits with; see the hashing note in the class doc. */
  public static final VarkaEmitOptions DEFAULTS = new VarkaEmitOptions(
      VarkaLoopEmitter.GROUP_BUDGET, true, FloorMod7.MAGIC, CivilFromDays.TOTAL, false);

  public VarkaEmitOptions {
    if (groupBudget < 1) {
      throw new IllegalArgumentException("groupBudget must be positive: " + groupBudget);
    }
    if (floorMod7 == null) {
      throw new IllegalArgumentException("floorMod7 must not be null");
    }
    if (civilFromDays == null) {
      throw new IllegalArgumentException("civilFromDays must not be null");
    }
  }

  /** {@link #DEFAULTS} with one field changed, for the suites and benchmarks that vary one. */
  public VarkaEmitOptions withGroupBudget(int budget) {
    return new VarkaEmitOptions(budget, cse, floorMod7, civilFromDays, misdescribeAdd);
  }

  public VarkaEmitOptions withCse(boolean enabled) {
    return new VarkaEmitOptions(groupBudget, enabled, floorMod7, civilFromDays, misdescribeAdd);
  }

  public VarkaEmitOptions withFloorMod7(FloorMod7 lowering) {
    return new VarkaEmitOptions(groupBudget, cse, lowering, civilFromDays, misdescribeAdd);
  }

  public VarkaEmitOptions withCivilFromDays(CivilFromDays lowering) {
    return new VarkaEmitOptions(groupBudget, cse, floorMod7, lowering, misdescribeAdd);
  }

  public VarkaEmitOptions withMisdescribeAdd(boolean misdescribe) {
    return new VarkaEmitOptions(groupBudget, cse, floorMod7, civilFromDays, misdescribe);
  }

  public boolean isDefault() {
    return DEFAULTS.equals(this);
  }

  /**
   * The hand-pinned rendering that reaches the shape hash - never {@code Record.toString}, whose
   * format no JDK promises, for the same reason {@code VarkaVectorIR.canonical} exists. Empty for
   * {@link #DEFAULTS}, so a production hash is byte-identical to what it was before options
   * existed; otherwise every field, in declaration order, so two variants can never collide.
   */
  public String canonical() {
    if (isDefault()) {
      return "";
    }
    return "opts(" + groupBudget + '|' + cse + '|' + floorMod7 + '|' + civilFromDays + '|'
        + misdescribeAdd + ')';
  }
}
