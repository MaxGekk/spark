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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaTypes.Admission;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaTypes.Conformance;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaTypes.Info;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.ArrowUtils;

/**
 * The type registry ({@link VarkaTypes}, VARKA-494) held to Spark and written for the tools: each
 * form maps back to its own id, each entry's storage is the width of the Arrow type Spark writes
 * for it, and {@code sql/varka/types.tsv} - which {@code dev/varka_census.py} and
 * {@code dev/varka_new_lane.py} read in place of lists of their own - is the table as it is.
 */
@VarkaSuite
public final class VarkaTypesSuite {

  private static final String REGEN =
      "VARKA_TYPES_REGEN=true build/sbt 'catalyst/testOnly *VarkaTypesSuite'";

  private static final String HEADER = "id\tspark\tcensus\tsql\tadmission\tlane\trow"
      + "\tstorage_bits\tlanes\tconformance\tconformance_row";

  private static Path file() {
    String home = System.getProperty("spark.test.home", System.getenv("SPARK_HOME"));
    assertTrue(home != null, "spark.test.home or SPARK_HOME is not set.");
    return Path.of(home, "sql", "varka", "types.tsv");
  }

  /** A Spark type's class as the tools name it: {@code IntegerType}, {@code TimeType}. */
  static String sparkName(DataType t) {
    return t.getClass().getSimpleName().replace("$", "");
  }

  private static String orDash(String s) {
    return s == null || s.isEmpty() ? "-" : s;
  }

  /** The table's lines, one per id in the id's order, after the generated-by comment. */
  static List<String> lines() {
    var out = new ArrayList<String>();
    out.add("# Written by VarkaTypesSuite from VarkaTypes (VARKA-494); regenerate with " + REGEN);
    out.add(HEADER);
    for (Info i : VarkaTypes.all()) {
      String admission = switch (i.admission()) {
        case Admission.OnLane on -> "on_lane";
        case Admission.AsCondition c -> "as_condition";
        case Admission.Owed o -> "owed";
        case Admission.Declined d -> "declined";
      };
      String row = switch (i.admission()) {
        case Admission.OnLane on -> null;
        case Admission.AsCondition c -> c.valuesRow();
        case Admission.Owed o -> o.row();
        case Admission.Declined d -> null;
      };
      String suite = switch (i.conformance()) {
        case Conformance.Suite s -> s.name();
        case Conformance.Owed o -> o.name();
        case Conformance.NotLanded n -> null;
      };
      String suiteRow = i.conformance() instanceof Conformance.Owed o ? o.row() : null;
      var spark = new LinkedHashSet<String>();
      i.forms().forEach(f -> spark.add(sparkName(f)));
      out.add(String.join("\t", i.id().name(), String.join(",", spark), orDash(i.census()),
          i.sql(), admission, orDash(i.lane().map(Enum::name).orElse(null)), orDash(row),
          Integer.toString(i.storageBits()),
          orDash(i.lanes().stream().map(Enum::name).collect(Collectors.joining(","))),
          orDash(suite), orDash(suiteRow)));
    }
    return out;
  }

  @Test
  @DisplayName("sql/varka/types.tsv is the registry as it is")
  void committedFile() throws IOException {
    List<String> fresh = lines();
    if ("true".equals(System.getenv("VARKA_TYPES_REGEN"))) {
      Files.write(file(), fresh, StandardCharsets.UTF_8);
      return;
    }
    List<String> committed = Files.exists(file())
        ? Files.readAllLines(file(), StandardCharsets.UTF_8) : List.of();
    if (!committed.equals(fresh)) {
      fail("sql/varka/types.tsv is not the registry; regenerate it with\n  " + REGEN
          + "\nThe registry today:\n  " + String.join("\n  ", fresh));
    }
  }

  @Test
  @DisplayName("every form maps back to its own id, and a type outside the table to none")
  void formsMapToTheirId() {
    for (Info i : VarkaTypes.all()) {
      for (DataType form : i.forms()) {
        assertEquals(i.id(), VarkaTypes.of(form).orElse(null), form.sql());
      }
    }
    for (DataType other : List.of(DataTypes.StringType, DataTypes.BinaryType,
        DataTypes.NullType, DataTypes.CalendarIntervalType,
        DataTypes.createArrayType(DataTypes.IntegerType),
        DataTypes.createMapType(DataTypes.IntegerType, DataTypes.IntegerType))) {
      assertTrue(VarkaTypes.of(other).isEmpty(), other.sql());
    }
  }

  @Test
  @DisplayName("each entry's storage is the width of the Arrow type Spark writes for every form")
  void storageIsSparksArrowWidth() {
    for (Info i : VarkaTypes.all()) {
      for (DataType form : i.forms()) {
        ArrowType arrow = ArrowUtils.toArrowType(form, "UTC", false);
        assertEquals(i.storageBits(), arrowBits(arrow), form.sql() + " is " + arrow);
      }
    }
  }

  @Test
  @DisplayName("a type read on a lane names that lane among the lanes its kernels reach")
  void laneIsReached() {
    for (Info i : VarkaTypes.all()) {
      i.lane().ifPresent(lane -> assertTrue(i.lanes().contains(lane), i.id().name()));
      if (i.lane().isEmpty() && !(i.admission() instanceof Admission.AsCondition)) {
        assertTrue(i.lanes().isEmpty(), i.id() + " is read on no lane and reaches some");
      }
    }
  }

  @Test
  @DisplayName("a column of an owed, declined or condition-only type declines naming its type")
  void declineNamesTheType() {
    assertEquals("a double column: the DOUBLE lane is row 341's",
        VarkaTypes.declineReason(DataTypes.DoubleType).orElseThrow());
    assertEquals("a timestamp column: its arithmetic is in the session time zone, which is not "
        + "lane arithmetic", VarkaTypes.declineReason(DataTypes.TimestampType).orElseThrow());
    assertEquals("a boolean column read as a value, which row 27 builds; it is read as a "
        + "condition", VarkaTypes.declineReason(DataTypes.BooleanType).orElseThrow());
    assertEquals("a float column: the FLOAT lane is row 341's",
        VarkaTypes.declineReason(DataTypes.FloatType).orElseThrow());
    assertTrue(VarkaTypes.declineReason(DataTypes.IntegerType).isEmpty());
    assertTrue(VarkaTypes.declineReason(DataTypes.TimestampNTZType).isEmpty());
    assertTrue(VarkaTypes.declineReason(DataTypes.StringType).isEmpty());
  }

  /** The bits a row of a fixed-width Arrow type, as Spark's writer lays it out. */
  private static int arrowBits(ArrowType t) {
    if (t instanceof ArrowType.Bool) {
      return 1;
    }
    if (t instanceof ArrowType.Int i) {
      return i.getBitWidth();
    }
    if (t instanceof ArrowType.FloatingPoint f) {
      return f.getPrecision() == FloatingPointPrecision.SINGLE ? 32 : 64;
    }
    if (t instanceof ArrowType.Decimal d) {
      return d.getBitWidth();
    }
    if (t instanceof ArrowType.Date) {
      return 32;
    }
    if (t instanceof ArrowType.Time time) {
      return time.getBitWidth();
    }
    if (t instanceof ArrowType.Timestamp || t instanceof ArrowType.Duration) {
      return 64;
    }
    if (t instanceof ArrowType.Interval) {
      return 32;
    }
    return fail("no fixed width for " + t);
  }
}
