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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import scala.collection.immutable.Seq;

import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaMatrixRuns;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSuite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The harness's store ({@link VarkaHarnessStore}) as the defaults' run and a configuration's run
 * use it, both played in this JVM: a writing store stands for the defaults' run, a store reading
 * its entries under a named configuration for the configuration's, over a second making of the
 * fixture, whose columns have other expression ids, as another JVM's would.
 */
@VarkaSuite
public final class VarkaHarnessStoreSuite {

  private static final String FIXTURE = "SELECT rid, i, d FROM VALUES "
      + "(0, 1, DATE'2021-03-04'), (1, 2, DATE'2022-05-06'), "
      + "(2, CAST(NULL AS INT), CAST(NULL AS DATE)) AS v(rid, i, d)";

  @AfterEach
  void matrixOptions() {
    VarkaColumnarToRowExec$.MODULE$.setEmitOptionsForTesting(VarkaMatrixRuns.BASE);
  }

  @Test
  @DisplayName("a configuration's run plans only what the defaults' run did not store")
  void plansOnlyWhatTheStoreLacks() {
    VarkaHarnessPlans.Fixture fixture = VarkaHarnessPlans.fixture(FIXTURE);
    VarkaHarnessStore defaults = VarkaHarnessStore.writing();
    VarkaHarnessStore.with(defaults, () -> {
      VarkaHarnessPlans.projection("i + 1", fixture.columns());
      VarkaHarnessPlans.projection("year(d)", fixture.columns());
      VarkaHarnessPlans.condition("i > 1", fixture.columns());
      return null;
    });
    assertEquals(3, defaults.planMisses.get());
    assertEquals(0, defaults.planUnstorable.get());

    VarkaHarnessPlans.Fixture again = VarkaHarnessPlans.fixture(FIXTURE);
    VarkaHarnessStore config = defaults.readingAs("cse=false");
    config.remove(key -> key.contains("year(d)"));
    Seq<NamedExpression> taken = VarkaHarnessStore.with(config, () -> {
      VarkaHarnessPlans.projection("year(d)", again.columns());
      VarkaHarnessPlans.condition("i > 1", again.columns());
      return VarkaHarnessPlans.projection("i + 1", again.columns());
    });
    assertEquals(2, config.planHits.get(), config.line());
    assertEquals(1, config.planMisses.get(), config.line());
    // What the configuration took reads the second fixture's columns, as its own plan does.
    Seq<NamedExpression> planned = VarkaHarnessPlans.projection("i + 1", again.columns());
    assertEquals(VarkaHarnessStore.describe(planned, true),
        VarkaHarnessStore.describe(taken, true));
  }

  @Test
  @DisplayName("the store's entries come back from its files as they were written")
  void filesRoundTrip() throws IOException {
    Path dir = Files.createTempDirectory("varka-harness-store");
    try {
      VarkaHarnessPlans.Fixture fixture = VarkaHarnessPlans.fixture(FIXTURE);
      VarkaHarnessStore defaults = VarkaHarnessStore.writing();
      VarkaHarnessStore.with(defaults, () ->
          VarkaHarnessPlans.projection("i + 1", fixture.columns()));
      VarkaHarnessStore.write(dir, defaults.entries());
      VarkaHarnessStore.write(dir, Map.of("only in the second file", new byte[] {1, 2}));
      Map<String, byte[]> read = VarkaHarnessStore.read(dir);
      assertEquals(defaults.entries().size() + 1, read.size());
      for (Map.Entry<String, byte[]> e : defaults.entries().entrySet()) {
        assertTrue(java.util.Arrays.equals(e.getValue(), read.get(e.getKey())), e.getKey());
      }
    } finally {
      try (var files = Files.list(dir)) {
        for (Path f : files.toList()) {
          Files.delete(f);
        }
      }
      Files.delete(dir);
    }
  }
}
