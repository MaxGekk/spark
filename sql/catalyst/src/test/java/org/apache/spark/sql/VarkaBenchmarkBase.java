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

package org.apache.spark.sql;

import scala.runtime.BoxedUnit;

import org.apache.spark.benchmark.BenchmarkBase;

/**
 * The base of a Varka benchmark written in Java: Spark's {@link BenchmarkBase}, with sections and
 * benchmarks a Java body can make without touching a Scala type ({@link VarkaBenchmark}).
 *
 * <p>A subclass implements {@code runBenchmarkSuite(String[])} and declares no {@code main}.
 * {@code BenchmarkBase.main} is an instance method, which Java will not let a static one hide; JDK
 * 25's launcher calls an inherited instance {@code main} on a class with none of its own (JEP
 * 512), and {@code build/sbt "<module>/Test/runMain <class>"} forks through that launcher. So
 * Spark's own {@code main} runs, and names and writes the results file as it does for a Scala
 * benchmark: {@code dev/varka_bench_regen.sh catalyst <class>} works unchanged. An unforked sbt
 * {@code run} would not find the method; the test configurations all fork.
 */
public abstract class VarkaBenchmarkBase extends BenchmarkBase {

  /** A titled section of the results file, holding what {@code body} runs. */
  protected final void section(String title, Runnable body) {
    runBenchmark(title, () -> {
      body.run();
      return BoxedUnit.UNIT;
    });
  }

  /** A benchmark writing to this run's results file; {@code valuesPerIteration} sets the rate. */
  protected final VarkaBenchmark.Builder benchmark(String name, long valuesPerIteration) {
    return new VarkaBenchmark.Builder(name, valuesPerIteration, output());
  }
}
