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
 * Runs the {@link DateVectorOpsBenchmark} JMH harness from a test, because the maven-jmh-plugin
 * is not resolvable on this environment's Maven mirror. JMH forks one JVM per benchmark
 * ({@code forks(1)}); the child inherits the surefire JVM's argLine (the Vector API and
 * native-access flags, and {@code -XX:MaxVectorSize=16} in the narrow execution), so each
 * benchmark is measured on a JVM whose profiles nothing else has touched - the JUnit suites that
 * ran before it, and the other benchmarks, included.
 *
 * <p>Benchmarks are gated behind {@code -Dvarka.jmh=true}: a plain {@code mvn test} executes this
 * as an instant no-op (benchmarks never run), while
 * {@code ./build/mvn -f sql/varka/engine/pom.xml test -Dvarka.jmh=true} runs them.
 */
public class DateVectorOpsBenchmarkTest {

  @Test
  void runBenchmarkWhenRequested() throws Exception {
    if (!"true".equals(System.getProperty("varka.jmh"))) {
      return; // Keeps normal `mvn test` free of benchmark runs.
    }
    Options options = new OptionsBuilder()
        .include("org\\.apache\\.spark\\.sql\\.varka\\.vector\\.DateVectorOpsBenchmark")
        .mode(Mode.Throughput)
        .timeUnit(TimeUnit.MILLISECONDS)
        .forks(1)
        .warmupIterations(2)
        .warmupTime(TimeValue.milliseconds(500))
        .measurementIterations(3)
        .measurementTime(TimeValue.milliseconds(500))
        .shouldFailOnError(true)
        .build();
    Collection<RunResult> results = new Runner(options).run();
    assertNotNull(results);
    assertFalse(results.isEmpty(), "expected at least one DateVectorOpsBenchmark result");
  }
}
