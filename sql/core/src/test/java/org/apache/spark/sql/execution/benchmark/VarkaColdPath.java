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

package org.apache.spark.sql.execution.benchmark;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import scala.Option;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.VarkaColumnarToRowExec$;
import org.apache.spark.sql.internal.SQLConf;

/**
 * The queries, sinks and switches of {@link VarkaColdPathBenchmark}, shared with {@link
 * VarkaColdPathProbe}, which runs one of the benchmark's arms in a loop for a profiler. A class
 * of static helpers, as {@link VarkaSizeLadder} is, since the probe is no benchmark.
 */
public final class VarkaColdPath {

  private VarkaColdPath() {}

  /**
   * The offset family of the steady-state query, which every arm of the benchmark shares and
   * the probe runs.
   */
  public static final int STEADY = 30;

  /**
   * The rung's query at an offset family's iteration, as {@code VarkaColdStartBenchmark} builds
   * it: a hundred months between iterations, so no two share a query, and every executed offset
   * within the kernel's month bound.
   */
  public static String query(int n, int iteration) {
    int base = 100 * (iteration + 1);
    return "SELECT "
        + IntStream.rangeClosed(1, n).mapToObj(k -> VarkaSizeLadder.entry(base + k))
            .collect(Collectors.joining(", "))
        + " FROM ladder_dates";
  }

  /**
   * Every row consumed as a row: Varka plans {@code VarkaColumnarToRowExec}. Inside an SQL
   * execution of its own, as a Dataset action runs and as the noop write runs: a query run
   * outside one gets another class loader, and the loader is part of the shape cache's key, so
   * the two sinks would warm two kernels for one shape.
   */
  public static void toRows(SparkSession session, String q) {
    rowsPlan(session, q);
  }

  /** Runs {@code q} as {@link #toRows} does and returns the executed plan, its metrics filled. */
  public static SparkPlan rowsPlan(SparkSession session, String q) {
    QueryExecution qe = session.sql(q).queryExecution();
    SQLExecution.withNewExecutionId(qe, Option.apply("rows"), () -> qe.toRdd().count());
    return qe.executedPlan();
  }

  /** Into the noop sink, which takes batches: Varka plans {@code VarkaProjectExec}. */
  public static void toNoop(SparkSession session, String q) {
    session.sql(q).write().format("noop").mode(SaveMode.Overwrite).save();
  }

  /**
   * Runs {@code body} with the Varka node unable to obtain a kernel, so that every batch takes
   * its row path and no warm-up starts.
   */
  public static void noKernel(Runnable body) {
    // `setFailEmissionForTesting` is private[sql], which scalac gives no static forwarder.
    VarkaColumnarToRowExec$.MODULE$.setFailEmissionForTesting(true);
    try {
      body.run();
    } finally {
      VarkaColumnarToRowExec$.MODULE$.setFailEmissionForTesting(false);
    }
  }

  /** Runs {@code body} with whole-stage codegen off in {@code session}. */
  public static void rowByRow(SparkSession session, Runnable body) {
    String key = SQLConf.WHOLESTAGE_CODEGEN_ENABLED().key();
    session.conf().set(key, false);
    try {
      body.run();
    } finally {
      session.conf().unset(key);
    }
  }

  /**
   * Turns the evaluator's warnings off and returns what turns them back on. {@link #noKernel}'s
   * emissions fail on purpose, and each task would log the failure with its stack trace: a cost
   * of the injection, not of the path. Callers read the nodes' metrics instead.
   */
  public static Runnable silenceEvaluator() {
    // The emission warning is logged under the evaluators' base class; the fallback, decline
    // and warm-up messages come from the components in the `varka` package (VARKA-251).
    List<String> loggers = List.of("org.apache.spark.sql.execution.VarkaEvaluatorBase",
        "org.apache.spark.sql.execution.VarkaKernelEvaluator",
        "org.apache.spark.sql.execution.varka");
    var levels = new LinkedHashMap<String, Level>();
    for (String logger : loggers) {
      levels.put(logger, LogManager.getLogger(logger).getLevel());
    }
    for (String logger : loggers) {
      Configurator.setLevel(logger, Level.ERROR);
    }
    return () -> levels.forEach(Configurator::setLevel);
  }
}
