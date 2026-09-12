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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

/**
 * The session the entry-list suites run against: stock Spark, one core, and the shared
 * {@code varka_dates} table at a thousand rows in two partitions.
 *
 * <p>Shared because {@link SurfaceTest} and {@link ChainsTest} had it byte for byte, and a
 * copied fixture is the thing that drifts: the two would have disagreed about the row count,
 * the partition count or the storage level the moment one of them was edited, and the entry
 * counts each asserts are taken against it. Stock, not Varka-enabled, on purpose - the
 * {@code plansVarka} assertions in both suites are the *negative* side of the driver's
 * {@code EXPLAIN} check, so they need a session where nothing fuses.
 */
final class BenchSession {

  static SparkSession start(String appName) {
    SparkSession spark = SparkSession.builder().master("local[1]").appName(appName)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate();
    DateSurfaceBenchmark.buildTable(spark, 1_000L, 2, StorageLevel.MEMORY_ONLY());
    return spark;
  }

  static void stop(SparkSession spark) {
    if (spark != null) {
      spark.stop();
    }
  }

  private BenchSession() {}
}
