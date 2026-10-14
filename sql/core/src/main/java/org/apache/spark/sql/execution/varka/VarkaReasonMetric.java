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
package org.apache.spark.sql.execution.varka;

import java.text.NumberFormat;
import java.util.Locale;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.execution.metric.CustomMetrics;
import org.apache.spark.sql.execution.metric.SQLMetric;

/**
 * The SQL metric type of a declined-batch count by refusal reason, which can say "not reported".
 *
 * <p>A plain sum metric reads 0 when nothing added to it, so it cannot tell "no batch declined for
 * this reason" from "a batch may have, and nobody could say". The second happens under the default
 * emit options: a kernel loop method that holds guards of several reasons folds them into one mask
 * and reports all of them for a batch any one declined, a candidate set rather than a reason
 * ({@code docs/sql-varka.md}, "A declined batch names its guard"). Reading that as a count would
 * be wrong; reading it as 0 would be worse. So each task reports a reason only when it can: its
 * count, a 0 included, when every decline that named the reason was attributed exactly, and
 * nothing - the metric left at its initial -1 - when one was not ({@link
 * VarkaFallbackAccounting#flushReasons}).
 *
 * <p>The SQL UI renders a metric whose type names a {@link CustomMetric} class by loading that
 * class and calling {@link #aggregateTaskMetrics} over every task's raw value, -1 included; that
 * is the one way Spark gives a metric cell text rather than a number, and why this is a custom
 * metric rather than a sum. The class needs the no-argument constructor the UI calls.
 */
public final class VarkaReasonMetric implements CustomMetric {

  /** What a reason reads as when no task could attribute it. */
  public static final String NOT_REPORTED = "not reported";

  @Override
  public String name() {
    return "varkaDeclinedByReason";
  }

  @Override
  public String description() {
    return "batches the Varka kernels declined for one refusal reason";
  }

  /**
   * The sum over the tasks that reported; {@link #NOT_REPORTED} when none did, and both when only
   * some did. A task that never sent an update reads 0 in the UI's array and is not told apart
   * from one that reported 0; a finished task always sends one.
   */
  @Override
  public String aggregateTaskMetrics(long[] taskMetrics) {
    long sum = 0;
    int unreported = 0;
    for (long v : taskMetrics) {
      if (v < 0) {
        unreported++;
      } else {
        sum += v;
      }
    }
    if (unreported == 0) {
      return format(sum);
    }
    if (unreported == taskMetrics.length) {
      return NOT_REPORTED;
    }
    return format(sum) + " attributed, " + NOT_REPORTED + " in " + unreported + " of "
        + taskMetrics.length + " tasks";
  }

  private static String format(long value) {
    return NumberFormat.getIntegerInstance(Locale.US).format(value);
  }

  /** A reason's metric on a node: of this type, at -1 until a task reports it. */
  public static SQLMetric create(SparkContext sc, String description) {
    var metric = new SQLMetric(CustomMetrics.buildV2CustomMetricTypeName(new VarkaReasonMetric()),
        -1L);
    metric.register(sc, scala.Option.apply(description), false);
    return metric;
  }
}
