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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SurfaceTest}'s twin for the {@code TIME} surface: every entry parses and runs on the
 * stock release the module compiles against, over a thousand rows of {@code varka_times}, in
 * both shapes - so a spelling the stock arm rejects, or a table the entries do not match, is
 * found here and not by the first multi-hour run. The stock session also pins that the type
 * and every function the list uses exist in the release the stock arm downloads.
 */
public class TimesTest {
  private static SparkSession spark;
  private static final DateSurfaceBenchmark.TableShape TIMES =
      DateSurfaceBenchmark.TableShape.TIMES;

  @BeforeAll
  public static void start() {
    spark = BenchSession.startWithTimes("TimesTest");
  }

  @AfterAll
  public static void stop() {
    // The session is shared across the module's suites through getOrCreate, and the date
    // suites' residency check counts every cached partition in it; the times table leaves.
    spark.catalog().uncacheTable("varka_times");
    spark.catalog().dropTempView("varka_times");
    BenchSession.stop(spark);
  }

  @Test
  public void everyEntryRunsInBothShapes() {
    for (Surface.Entry e : Times.ENTRIES) {
      if (e.projection() != null) {
        String q = DateSurfaceBenchmark.projectionQuery(e, TIMES);
        var df = spark.sql(q);
        assertEquals(1, df.schema().fields().length, q);
        assertEquals(1_000L, df.count(), q);
        assertEquals(DateSurfaceBenchmark.Fusion.PLAIN,
            DateSurfaceBenchmark.plansVarka(spark, q), q);
      }
      if (e.filter() != null) {
        String qc = DateSurfaceBenchmark.filterColumnarQuery(e, TIMES);
        assertTrue(spark.sql(qc).count() <= 1_000L, qc);
        assertEquals(DateSurfaceBenchmark.Fusion.PLAIN,
            DateSurfaceBenchmark.plansVarka(spark, qc), qc);
        String q = DateSurfaceBenchmark.filterQuery(e, TIMES);
        List<Row> rows = spark.sql(q).collectAsList();
        assertEquals(1, rows.size(), q);
        assertEquals(DataTypes.LongType, spark.sql(q).schema().fields()[0].dataType(), q);
      }
    }
  }

  /**
   * The table is what the entries assume: the null patterns, the spread of {@code t} over the
   * whole day, and {@code t + dt} inside the day on every row - the property the interval add's
   * row rests on, since a crossing row would be a declined batch timed as the kernel.
   */
  @Test
  public void theTableHasTheShapeTheEntriesAssume() {
    Row r = spark.sql("SELECT count(*), count(t), count(t2), count(dt), count(dt2), count(l), "
        + "count(l2), min(hour(t)), max(hour(t)), min(l), max(l) FROM varka_times").first();
    assertEquals(1_000L, r.getLong(0));
    assertEquals(967L, r.getLong(1), "every 31st t is null: ids 0, 31, ..., 992");
    assertEquals(1_000L, r.getLong(2));
    assertEquals(979L, r.getLong(3), "every 47th dt is null: ids 46, 93, ..., 986");
    assertEquals(1_000L, r.getLong(4));
    assertEquals(1_000L, r.getLong(5));
    assertEquals(982L, r.getLong(6), "every 53rd l2 is null: ids 52, 105, ..., 998");
    assertEquals(0, r.getInt(7));
    assertEquals(23, r.getInt(8));
    assertTrue(r.getLong(9) >= 0L && r.getLong(10) < 10_000_000_000L);
    // The sum exists on every row the row engine can compute it for: a crossing row would
    // raise here rather than produce a value.
    Row sums = spark.sql("SELECT count(t + dt), count(*) FROM varka_times "
        + "WHERE t IS NOT NULL AND dt IS NOT NULL").first();
    assertEquals(sums.getLong(1), sums.getLong(0));
    // And the sign rule that keeps it inside the day: forward before noon, backward after.
    Row signs = spark.sql("SELECT count(*) FROM varka_times WHERE t IS NOT NULL "
        + "AND dt IS NOT NULL AND (hour(t) < 12) = (dt >= INTERVAL '0' SECOND)").first();
    assertEquals(sums.getLong(1), signs.getLong(0), "dt's sign follows t's half of the day");
  }

  @Test
  public void labelsAreUniqueSoTablesAreTooAcrossFiles() {
    long distinct = Times.ENTRIES.stream().map(Surface.Entry::label).distinct().count();
    assertEquals(Times.ENTRIES.size(), distinct);
  }

  /** The entries read the times table's columns and nothing of the date table's. */
  @Test
  public void everyEntryIsSatisfiedByTheTimesTableAndRefusedByTheDateOne() {
    for (Surface.Entry e : Times.ENTRIES) {
      DateSurfaceBenchmark.requireColumns(e, TIMES);
      assertThrows(IllegalArgumentException.class,
          () -> DateSurfaceBenchmark.requireColumns(e, DateSurfaceBenchmark.TableShape.ALL),
          e.label());
    }
  }

  /** The two surfaces never share a label, so their files cannot be confused in a merge. */
  @Test
  public void noLabelIsSharedWithTheDateSurface() {
    for (Surface.Entry e : Times.ENTRIES) {
      assertTrue(Surface.ENTRIES.stream().noneMatch(d -> d.label().equals(e.label())),
          e.label());
    }
  }

  @Test
  public void theTimesShapeBuildsTheColumnsItClaims() {
    List<String> built = List.of(spark.table("varka_times").schema().fieldNames());
    assertEquals(TIMES.columns(), built);
    assertEquals("varka_times", TIMES.tableName());
    assertEquals("t", TIMES.filterColumn());
  }
}
