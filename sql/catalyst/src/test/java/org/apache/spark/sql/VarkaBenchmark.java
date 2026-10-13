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

import java.io.OutputStream;
import java.time.Duration;
import java.util.function.IntConsumer;

import scala.Option;
import scala.jdk.javaapi.DurationConverters;
import scala.runtime.BoxedUnit;

import org.apache.spark.benchmark.Benchmark;

/**
 * One of Spark's {@link Benchmark}s as Java writes it: a builder in place of the constructor's
 * seven arguments, and cases that are an {@link IntConsumer} in place of a Scala function that
 * returns {@code BoxedUnit}. Spark's class still times the cases and writes the table, so the
 * results files keep the one format {@code dev/varka_bench_regen.sh}, {@code
 * dev/varka_bench_diff.py} and the quote check read.
 *
 * <p>Made by {@link VarkaBenchmarkBase#benchmark}, which hands it the run's results file.
 */
public final class VarkaBenchmark {
  private final Benchmark benchmark;

  private VarkaBenchmark(Benchmark benchmark) {
    this.benchmark = benchmark;
  }

  /** A case run for as many iterations as the benchmark's minimum time asks; gets the iteration. */
  public void addCase(String name, IntConsumer body) {
    addCase(name, 0, body);
  }

  /** A case run for exactly {@code iterations} iterations, or as {@link #addCase(String,
   *  IntConsumer)} when that is 0. */
  public void addCase(String name, int iterations, IntConsumer body) {
    benchmark.addCase(name, iterations, iteration -> {
      body.accept((Integer) iteration);
      return BoxedUnit.UNIT;
    });
  }

  /**
   * Keeps only the cases whose name contains {@code substring}: how a benchmark serves {@code
   * -Dvarka.bench.only}, a diagnostic run of one block that {@link VarkaBenchmarkBase#main}
   * refuses to write to a results file.
   */
  public void retainCases(String substring) {
    benchmark.benchmarks().filterInPlace(c -> c.name().contains(substring));
  }

  /** Whether any case is left to run; Spark's {@code run} refuses a benchmark with none. */
  public boolean hasCases() {
    return benchmark.benchmarks().nonEmpty();
  }

  /** Runs every case and writes the table, relative speeds against the first case. */
  public void run() {
    benchmark.run(false);
  }

  /** The settings of one benchmark, with Spark's defaults. */
  public static final class Builder {
    private final String name;
    private final long valuesPerIteration;
    private final Option<OutputStream> output;
    // Spark's defaults for the constructor's parameters of the same names.
    private int minIterations = 2;
    private Duration warmup = Duration.ofSeconds(2);
    private Duration minTime = Duration.ofSeconds(2);

    Builder(String name, long valuesPerIteration, Option<OutputStream> output) {
      this.name = name;
      this.valuesPerIteration = valuesPerIteration;
      this.output = output;
    }

    /**
     * The settings most Varka benchmarks time with: at least five iterations, each case warmed
     * for two seconds and timed for at least two, so a row is a best of several runs long enough
     * for the JIT to settle.
     */
    public Builder timed() {
      return minIterations(5).warmup(Duration.ofSeconds(2)).minTime(Duration.ofSeconds(2));
    }

    /** The fewest iterations per case, after the warm-up. */
    public Builder minIterations(int minIterations) {
      this.minIterations = minIterations;
      return this;
    }

    /** How long each case runs before its iterations are timed, for the JIT. */
    public Builder warmup(Duration warmup) {
      this.warmup = warmup;
      return this;
    }

    /** How long each case keeps iterating past its minimum count. */
    public Builder minTime(Duration minTime) {
      this.minTime = minTime;
      return this;
    }

    public VarkaBenchmark build() {
      return new VarkaBenchmark(new Benchmark(name, valuesPerIteration, minIterations,
          DurationConverters.toScala(warmup), DurationConverters.toScala(minTime), false, output));
    }
  }
}
