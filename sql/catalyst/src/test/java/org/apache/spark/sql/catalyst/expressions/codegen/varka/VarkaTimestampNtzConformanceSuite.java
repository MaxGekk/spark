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

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.stream.LongStream;

import org.apache.spark.sql.types.DataTypes;

/**
 * `TIMESTAMP_NTZ` held to the conformance contract (VARKA-484, VARKA-338): the checks of
 * {@link VarkaTypeConformance} over microseconds since the epoch at the long lane. The values are
 * the long range's extremes and their neighbours, zero and one each way, and the edges of the
 * days a {@code DATE} converts to without overflow, {@code +-106,751,991} days, with the first
 * microsecond past each.
 */
@VarkaSuite
public final class VarkaTimestampNtzConformanceSuite extends VarkaTypeConformance {

  private static final long MICROS_PER_DAY = 86_400_000_000L;

  /** The last day whose midnight is a long count of microseconds (`VARKA-338.md` 0.3). */
  private static final long LAST_DAY = 106_751_991L;

  private static final DateTimeFormatter LITERAL =
      DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS");

  @Override
  protected ConformanceType type() {
    long edge = LAST_DAY * MICROS_PER_DAY;
    long[] values = LongStream.concat(
        LongStream.of(integralValues(Long.MIN_VALUE, Long.MAX_VALUE)),
        LongStream.of(-edge, -edge - 1, edge, edge + 1)).toArray();
    return new ConformanceType("ntz", DataTypes.TimestampNTZType, "TIMESTAMP_NTZ",
        VarkaTimestampNtzConformanceSuite::literal, values);
  }

  /** An NTZ literal for {@code micros}: the UTC wall clock it counts to, to the microsecond. */
  static String literal(long micros) {
    LocalDateTime t = LocalDateTime.ofEpochSecond(Math.floorDiv(micros, 1_000_000L),
        (int) Math.floorMod(micros, 1_000_000L) * 1000, ZoneOffset.UTC);
    return "TIMESTAMP_NTZ '" + LITERAL.format(t) + "'";
  }
}
