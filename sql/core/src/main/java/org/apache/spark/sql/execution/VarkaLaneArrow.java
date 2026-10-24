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
package org.apache.spark.sql.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.TimeType;
import org.apache.spark.sql.types.YearMonthIntervalType;
import org.apache.spark.sql.util.ArrowUtils;

/**
 * The Arrow vectors each lane's columns arrive in and its outputs are written to: the evaluator's
 * one table of them (VARKA-474 2.2), read by {@code VarkaEvaluatorBase.isArrowBacked} to admit a
 * batch and by {@code VarkaKernelEvaluator.allocateVector} to build a destination.
 *
 * <p>The table is one switch over {@code LaneType} with no {@code default}, so a new lane does not
 * compile until it names its vectors here. Before it, the two functions held the list twice, the
 * admission in a switch over vector classes whose {@code default -> false} declined a lane nobody
 * had wired on every batch, which reads as a slow query rather than an error. That switch keeps a
 * {@code default}, since Arrow's vector classes are not a sealed set, but what it admits is now
 * this table's.
 *
 * <p>Two vectors the evaluator admits belong to no lane, and stay named in {@code isArrowBacked}:
 * a boolean column's {@code BitVector}, read as two bitmaps rather than lane values, and the
 * {@code VarCharVector} a derived input arrives in. Which lane a Spark type is on is also known to
 * {@code VarkaNodeCompiler.laneOf} in catalyst; the two agree by the suites, and row 475's type
 * registry is where they become one.
 */
final class VarkaLaneArrow {

  private VarkaLaneArrow() {
  }

  /** Builds an empty destination vector of one Arrow class for a Spark type stored in it. */
  @FunctionalInterface
  interface Allocate {
    ValueVector create(String name, DataType dataType, BufferAllocator allocator);
  }

  /**
   * One Arrow vector class a lane reads and writes: the class, which Spark types are stored in
   * it, and how a destination of it is built.
   */
  record Vector(Class<? extends ValueVector> vectorClass, Predicate<DataType> stores,
      Allocate allocate) {
  }

  /**
   * The vectors of one lane. A lane's Spark types share its width and its buffer layout, so each
   * vector is a fixed-width one the kernels address directly.
   */
  static List<Vector> of(LaneType lane) {
    return switch (lane) {
      // A narrow column's one or two bytes a row (VARKA-329); the kernel stores into it at the
      // type's own width.
      case BYTE -> List.of(new Vector(TinyIntVector.class, DataTypes.ByteType::equals,
          (name, type, allocator) -> new TinyIntVector(name, allocator)));
      case SHORT -> List.of(new Vector(SmallIntVector.class, DataTypes.ShortType::equals,
          (name, type, allocator) -> new SmallIntVector(name, allocator)));
      // A year-month interval is a count of months in an int32 buffer whatever its unit, which
      // rides on the Spark type and never on the buffer, so every unit is one vector class.
      case INT -> List.of(
          new Vector(DateDayVector.class, DataTypes.DateType::equals,
              (name, type, allocator) -> new DateDayVector(name, allocator)),
          new Vector(IntVector.class, DataTypes.IntegerType::equals,
              (name, type, allocator) -> new IntVector(name, allocator)),
          new Vector(IntervalYearVector.class, type -> type instanceof YearMonthIntervalType,
              (name, type, allocator) -> new IntervalYearVector(name, allocator)));
      // The long lane's three (VARKA-29): a bigint, a TIME(p) - nanoseconds of day at every
      // precision - and a day-time interval in microseconds, each built from the Arrow field
      // Spark's own writer builds for the type, so TimeNanoVector carries the precision in its
      // field metadata and the row path reads all three back through ArrowColumnVector. The
      // timestamp vectors are deliberately not here: the compiler builds no leaf for them.
      case LONG -> List.of(
          new Vector(BigIntVector.class, DataTypes.LongType::equals, VarkaLaneArrow::fromField),
          new Vector(TimeNanoVector.class, type -> type instanceof TimeType,
              VarkaLaneArrow::fromField),
          new Vector(DurationVector.class, type -> type instanceof DayTimeIntervalType,
              VarkaLaneArrow::fromField));
    };
  }

  private static ValueVector fromField(String name, DataType type, BufferAllocator allocator) {
    return ArrowUtils.toArrowField(name, type, true, null, false, Metadata.empty(), false)
        .createVector(allocator);
  }

  /** Every lane's vectors, in lane order, built once: the admission reads it on every batch. */
  private static final Vector[] ALL = all();

  private static Vector[] all() {
    List<Vector> out = new ArrayList<>();
    for (LaneType lane : LaneType.values()) {
      out.addAll(of(lane));
    }
    return out.toArray(new Vector[0]);
  }

  /**
   * Whether a lane reads {@code vector}: an instance of one of the table's classes, as the type
   * patterns this replaced matched. A loop over a few entries, so a batch's admission allocates
   * nothing.
   */
  static boolean reads(ValueVector vector) {
    for (Vector v : ALL) {
      if (v.vectorClass().isInstance(vector)) {
        return true;
      }
    }
    return false;
  }

  /**
   * An empty destination vector for an output of {@code dataType}, or null where no lane stores
   * the type - a boolean output, which the kernels do not write yet (row 27).
   */
  static @Nullable ValueVector allocate(String name, DataType dataType,
      BufferAllocator allocator) {
    for (Vector v : ALL) {
      if (v.stores().test(dataType)) {
        return v.allocate().create(name, dataType, allocator);
      }
    }
    return null;
  }
}
