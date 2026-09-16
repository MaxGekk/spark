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

import java.util.OptionalLong;

/**
 * The value-range lattice the compiler reasons in: what a lane can hold, as a closed interval of
 * {@code long} or as {@link #UNKNOWN}. {@link VarkaRangeAnalysis} computes one per IR node; the
 * compiler asks it two questions - can a calendar node's input leave the range the calendar
 * lowering is exact in, and can a checked int operation overflow - and takes a runtime check off
 * only when the interval says it cannot.
 *
 * <p>Every operation is total and <b>saturates to {@link #UNKNOWN} instead of wrapping</b>. Two
 * nested bounds whose product passes {@code 2^63} once came back as a small positive number and
 * "proved" a checked multiply safe; here that arithmetic is a property of the type, so no caller
 * can forget it. An unknown range is the conservative answer everywhere: it admits no calendar
 * node and removes no check, and it absorbs every operation.
 *
 * <p>Pure data: nothing here knows about the IR or about Spark types. The interval is over
 * {@code long} although the lanes are int32, so that two literals of two billion cannot wrap
 * their sum and so the same type serves the wider lanes later milestones add.
 */
public final class VarkaValueRange {

  private VarkaValueRange() {}

  /** A range: {@link Bounded} or {@link Unknown}. */
  public sealed interface Range permits Bounded, Unknown {

    /** This interval moved by {@code [lo, hi]}: its low end by {@code lo}, its high end by
     * {@code hi}. */
    Range shift(long lo, long hi);

    /** The smallest interval containing both. */
    Range hull(Range other);

    /** The interval of sums of a member of this and a member of {@code other}. */
    Range add(Range other);

    /** The interval of differences, a member of this minus a member of {@code other}. */
    Range sub(Range other);

    /** The interval of products, from the four corner products. */
    Range mul(Range other);

    /** The interval of negations. */
    Range neg();

    /** The interval of absolute values. */
    Range abs();

    /** The largest absolute value a member can have, or empty when unknown. */
    OptionalLong magnitude();

    /** Whether every member fits an int32 lane. */
    boolean fitsInt();

    /** Whether every member lies in {@code [lo, hi]}. */
    boolean within(long lo, long hi);
  }

  /**
   * The unknown range: absorbs every operation, proves nothing. A record with no components
   * rather than an enum: scalac models an enum constant as a child of the enum type when it
   * populates a sealed hierarchy for exhaustiveness, and a Scala match over {@link Range} then
   * sends scaladoc into a cycle. Every instance is equal to every other; use {@link #UNKNOWN}.
   */
  public record Unknown() implements Range {
    @Override public Range shift(long lo, long hi) { return this; }
    @Override public Range hull(Range other) { return this; }
    @Override public Range add(Range other) { return this; }
    @Override public Range sub(Range other) { return this; }
    @Override public Range mul(Range other) { return this; }
    @Override public Range neg() { return this; }
    @Override public Range abs() { return this; }
    @Override public OptionalLong magnitude() { return OptionalLong.empty(); }
    @Override public boolean fitsInt() { return false; }
    @Override public boolean within(long lo, long hi) { return false; }
    @Override public String toString() { return "Unknown"; }
  }

  /** A closed interval {@code [lo, hi]} of {@code long}, never empty. */
  public record Bounded(long lo, long hi) implements Range {

    public Bounded {
      if (lo > hi) {
        throw new IllegalArgumentException("empty range [" + lo + ", " + hi + "]");
      }
    }

    @Override
    public Range shift(long lo, long hi) {
      try {
        return new Bounded(Math.addExact(this.lo, lo), Math.addExact(this.hi, hi));
      } catch (ArithmeticException overflow) {
        return UNKNOWN;
      }
    }

    @Override
    public Range hull(Range other) {
      if (other instanceof Bounded b) {
        return new Bounded(Math.min(lo, b.lo), Math.max(hi, b.hi));
      }
      return UNKNOWN;
    }

    @Override
    public Range add(Range other) {
      if (other instanceof Bounded b) {
        return shift(b.lo, b.hi);
      }
      return UNKNOWN;
    }

    @Override
    public Range sub(Range other) {
      if (other instanceof Bounded b) {
        try {
          return new Bounded(Math.subtractExact(lo, b.hi), Math.subtractExact(hi, b.lo));
        } catch (ArithmeticException overflow) {
          return UNKNOWN;
        }
      }
      return UNKNOWN;
    }

    @Override
    public Range mul(Range other) {
      if (other instanceof Bounded b) {
        try {
          long p1 = Math.multiplyExact(lo, b.lo);
          long p2 = Math.multiplyExact(lo, b.hi);
          long p3 = Math.multiplyExact(hi, b.lo);
          long p4 = Math.multiplyExact(hi, b.hi);
          return new Bounded(
              Math.min(Math.min(p1, p2), Math.min(p3, p4)),
              Math.max(Math.max(p1, p2), Math.max(p3, p4)));
        } catch (ArithmeticException overflow) {
          return UNKNOWN;
        }
      }
      return UNKNOWN;
    }

    @Override
    public Range neg() {
      try {
        return new Bounded(Math.negateExact(hi), Math.negateExact(lo));
      } catch (ArithmeticException overflow) {
        return UNKNOWN;
      }
    }

    @Override
    public Range abs() {
      if (lo >= 0) {
        return this;
      }
      if (hi <= 0) {
        return neg();
      }
      OptionalLong m = magnitude();
      return m.isPresent() ? new Bounded(0, m.getAsLong()) : UNKNOWN;
    }

    @Override
    public OptionalLong magnitude() {
      try {
        return OptionalLong.of(Math.max(Math.absExact(lo), Math.absExact(hi)));
      } catch (ArithmeticException overflow) {
        return OptionalLong.empty();
      }
    }

    @Override
    public boolean fitsInt() {
      return lo >= Integer.MIN_VALUE && hi <= Integer.MAX_VALUE;
    }

    @Override
    public boolean within(long lo, long hi) {
      return this.lo >= lo && this.hi <= hi;
    }

    @Override
    public String toString() {
      return "[" + lo + ", " + hi + "]";
    }
  }

  /** The unknown range. */
  public static final Range UNKNOWN = new Unknown();

  /** The interval {@code [lo, hi]}. */
  public static Range of(long lo, long hi) {
    return new Bounded(lo, hi);
  }

  /** The one-point interval {@code [v, v]}. */
  public static Range point(long v) {
    return new Bounded(v, v);
  }

  /**
   * The interval {@code [-m, m]}: what a magnitude bound says about a value. This is how the
   * compiler's int-lane question is asked of the lattice - a magnitude {@code m} is the interval
   * symmetric about zero of half-width {@code m}, and interval arithmetic over symmetric
   * intervals stays symmetric, so sums bound as sums of magnitudes and products as products.
   */
  public static Range symmetric(long m) {
    if (m < 0) {
      throw new IllegalArgumentException("negative magnitude " + m);
    }
    return new Bounded(-m, m);
  }
}
