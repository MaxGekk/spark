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
 * <p>A plain count cannot tell "no batch declined for this reason" from "a batch may have, and
 * nobody could say". The second happens under the default emit options: a kernel loop method that
 * holds guards of several reasons folds them into one mask and reports all of them for a batch any
 * one declined, a candidate set rather than a reason ({@code docs/sql-varka.md}, "A declined batch
 * names its guard"). Counting it against each would overcount; leaving it out would show a 0 that
 * may be false.
 *
 * <p>So one metric carries two counts: the batches that named the reason exactly in its low 32
 * bits, and the batches whose candidate set included it in the bits above, added as
 * {@link #UNATTRIBUTED} each. Both are sums, so the encoding survives everything Spark does to a
 * metric - the merge of tasks, of several evaluators in one task (a coalesce runs one per parent
 * partition), the driver's accumulator and the event log a history server replays - where a
 * sentinel value would not. A task would need four billion declined batches to carry over.
 *
 * <p>The SQL UI renders a metric whose type names a {@link CustomMetric} class by loading that
 * class and calling {@link #aggregateTaskMetrics} over the tasks' values; that is the one way
 * Spark gives a metric cell text rather than a number. The class needs the no-argument
 * constructor the UI calls.
 */
public final class VarkaReasonMetric implements CustomMetric {

  /** What one unattributed declined batch adds to a reason's metric. */
  public static final long UNATTRIBUTED = 1L << 32;

  /** What a reason reads as when every decline that named it was a candidate set. */
  public static final String NOT_REPORTED = "not reported";

  @Override
  public String name() {
    return "varkaDeclinedByReason";
  }

  @Override
  public String description() {
    return "batches the Varka kernels declined for one refusal reason";
  }

  /** The batches that named the reason exactly, from a metric value or a sum of them. */
  public static long exact(long value) {
    return value & (UNATTRIBUTED - 1);
  }

  /** The batches whose candidate set included the reason, from a value or a sum of them. */
  public static long unattributed(long value) {
    return value >>> 32;
  }

  /** The tasks' values summed, then {@link #render}ed. */
  @Override
  public String aggregateTaskMetrics(long[] taskMetrics) {
    long sum = 0;
    for (long v : taskMetrics) {
      if (v > 0) {
        sum += v;
      }
    }
    return render(sum);
  }

  /**
   * The exact count where nothing was unattributed, a 0 included; {@link #NOT_REPORTED} where
   * every decline that named the reason was a candidate set; both where some were.
   */
  public static String render(long value) {
    long exact = exact(value);
    long unattributed = unattributed(value);
    if (unattributed == 0) {
      return format(exact);
    }
    if (exact == 0) {
      return NOT_REPORTED + " (" + format(unattributed) + " batches named it among others)";
    }
    return format(exact) + ", and " + NOT_REPORTED + " in " + format(unattributed)
        + " batches that named it among others";
  }

  private static String format(long value) {
    return NumberFormat.getIntegerInstance(Locale.US).format(value);
  }

  /** A reason's metric on a node, of this type. */
  public static SQLMetric create(SparkContext sc, String description) {
    var metric = new SQLMetric(CustomMetrics.buildV2CustomMetricTypeName(new VarkaReasonMetric()),
        0L);
    metric.register(sc, scala.Option.apply(description), false);
    return metric;
  }
}
