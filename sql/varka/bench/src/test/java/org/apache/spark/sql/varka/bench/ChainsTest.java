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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Chains}, checked the way {@link SurfaceTest} checks the surface: every entry has to
 * run, produce one column, and plan without a Varka node on stock Spark, which is the negative
 * side of the {@code EXPLAIN} check the driver relies on.
 *
 * <p>What this file cannot check is the property the list exists for - that each chain is
 * heavy enough to leave the memory-bandwidth regime. That is an op count, and op counts come
 * from the emitter in catalyst test scope rather than from here, so {@link Chains}'s javadoc
 * records the number for each entry and {@code dev/varka_emit.sh --table} reproduces them. The
 * cheap half of the property is checkable, though, and is: a chain must be a composition, not
 * a single call, or it belongs in {@link Surface} instead.
 */
public class ChainsTest {
  private static SparkSession spark;

  @BeforeAll
  public static void start() {
    spark = SparkSession.builder().master("local[1]").appName("ChainsTest")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate();
    DateSurfaceBenchmark.buildTable(spark, 1_000L, 2, StorageLevel.MEMORY_ONLY());
  }

  @AfterAll
  public static void stop() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void everyChainRuns() {
    for (Surface.Entry e : Chains.ENTRIES) {
      String q = DateSurfaceBenchmark.projectionQuery(e);
      var df = spark.sql(q);
      assertEquals(1, df.schema().fields().length, q);
      assertEquals(1_000L, df.count(), q);
      assertEquals(DateSurfaceBenchmark.Fusion.PLAIN, DateSurfaceBenchmark.plansVarka(spark, q), q);
    }
  }

  /**
   * The reason each entry is here, asserted rather than asserted-in-a-comment. An entry below
   * {@link Chains#MIN_OPS} would not clear the 5% fixed-share rule at 1e8 rows and so would
   * not do the job the list exists for.
   *
   * <p>This replaced a nesting-depth check, which was the obvious proxy and the wrong one:
   * {@code weekofyear(add_months(d, i))} is two calls and 176 ops while
   * {@code month(next_day(date_add(d, i), 'MONDAY'))} is three calls and 55, because what
   * costs is the arithmetic inside each operation and not how many are stacked.
   */
  @Test
  public void everyChainIsHeavyEnoughToEarnItsPlace() {
    for (Surface.Entry e : Chains.ENTRIES) {
      int ops = Chains.emitterOps(e.label());
      assertTrue(ops >= Chains.MIN_OPS,
          e.label() + " is " + ops + " ops, under the " + Chains.MIN_OPS + " this list needs");
    }
  }

  @Test
  public void chainsAreExpectedToFuseAndAreNotInTheSurface() {
    List<String> surface = Surface.ENTRIES.stream().map(Surface.Entry::label).toList();
    for (Surface.Entry e : Chains.ENTRIES) {
      assertTrue(e.expectFused(), e.label() + " must be expected to fuse or it times a fallback");
      assertTrue(!surface.contains(e.label()), e.label() + " is already in the surface");
    }
  }

  @Test
  public void labelsAreUniqueSoTablesAreTooAcrossFiles() {
    long distinct = Chains.ENTRIES.stream().map(Surface.Entry::label).distinct().count();
    assertEquals(Chains.ENTRIES.size(), distinct);
  }
}
