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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.execution.ui.SQLAppStatusStore;
import org.apache.spark.sql.execution.ui.SQLExecutionUIData;
import org.apache.spark.sql.execution.varka.VarkaFallbackAccounting;
import org.apache.spark.sql.execution.varka.VarkaReasonMetric;

/**
 * Reads a Varka node's declined batches by refusal reason, the way a suite asserts which reason a
 * query fell back for (VARKA-313; Comet's {@code checkSparkAnswerAndFallbackReason} is the model).
 * Two readings, because they can differ: the driver-side accumulators, which sum only the tasks
 * that reported a reason, and the strings the SQL UI shows, which also say where a reason was not
 * reported.
 */
public final class VarkaFallbackReasons {

  private VarkaFallbackReasons() {
  }

  /**
   * Each reason's driver-side count, or null for a reason no task reported - one that a folded
   * decline left unattributed in every task ({@link VarkaFallbackAccounting#flushReasons}).
   */
  public static Map<String, Long> counts(SparkPlan node) {
    var out = new LinkedHashMap<String, Long>();
    for (String reason : VarkaFallbackAccounting.REASONS) {
      SQLMetric metric = metric(node, reason);
      out.put(reason, metric.isZero() ? null : metric.value());
    }
    return out;
  }

  /**
   * Each reason as the SQL UI renders it for the session's latest finished execution, which the
   * caller has just run: a count, or {@link VarkaReasonMetric#NOT_REPORTED}. Waits for the
   * listener bus first, since the UI's store is filled from it.
   */
  public static Map<String, String> rendered(SparkSession session, SparkPlan node)
      throws TimeoutException {
    session.sparkContext().listenerBus().waitUntilEmpty();
    SQLAppStatusStore store = session.sharedState().statusStore();
    long latest = -1;
    for (SQLExecutionUIData execution : CollectionConverters.asJava(store.executionsList())) {
      latest = Math.max(latest, execution.executionId());
    }
    if (latest < 0) {
      throw new IllegalStateException("the session has run no SQL execution");
    }
    scala.collection.immutable.Map<Object, String> values = store.executionMetrics(latest);
    var out = new LinkedHashMap<String, String>();
    for (String reason : VarkaFallbackAccounting.REASONS) {
      scala.Option<String> value = values.get(metric(node, reason).id());
      out.put(reason, value.isDefined() ? value.get() : null);
    }
    return out;
  }

  private static SQLMetric metric(SparkPlan node, String reason) {
    String key = VarkaExecMetrics.reasonKey(reason);
    scala.Option<SQLMetric> metric = node.metrics().get(key);
    if (metric.isEmpty()) {
      throw new IllegalArgumentException(node.nodeName() + " has no metric " + key);
    }
    return metric.get();
  }
}
