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

package org.apache.spark.sql.varka.vector;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Runs {@link VarkaUnrollFactorBenchmark} in-process, same mechanism as
 * {@link DateVectorOpsBenchmarkTest} and for the same reason (maven-jmh-plugin does not resolve
 * on this environment's Maven mirror).
 *
 * <p>Gated behind {@code -Dvarka.jmh=true}: {@code ./build/mvn -f sql/varka/engine/pom.xml test
 * -Dvarka.jmh=true} runs this alongside every other gated benchmark; a plain {@code mvn test}
 * skips it.
 */
public class VarkaUnrollFactorBenchmarkTest {

  @Test
  void runBenchmarkWhenRequested() throws Exception {
    if (!"true".equals(System.getProperty("varka.jmh"))) {
      return; // Keeps normal `mvn test` free of benchmark runs.
    }
    String pkg = "org\\.apache\\.spark\\.sql\\.varka\\.vector\\.";
    Options options = new OptionsBuilder()
        .include(pkg + "VarkaUnrollFactorBenchmark")
        .mode(Mode.Throughput)
        .timeUnit(TimeUnit.MILLISECONDS)
        .forks(0)
        .warmupIterations(3)
        .warmupTime(TimeValue.seconds(2))
        .measurementIterations(5)
        .measurementTime(TimeValue.seconds(2))
        .shouldFailOnError(true)
        .build();
    Collection<RunResult> results = new Runner(options).run();
    assertNotNull(results);
    assertFalse(results.isEmpty(), "expected at least one benchmark result");
  }
}
