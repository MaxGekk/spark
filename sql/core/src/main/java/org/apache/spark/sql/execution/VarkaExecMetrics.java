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

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.execution.metric.SQLMetrics;

/**
 * The Varka-specific SQL metrics one exec node threads to its factory and evaluator, bundled so
 * the parameter lists stop growing metric by metric. A field is {@code null} where a suite or a
 * diagnostic constructs an evaluator without that metric, and {@link #inc} is the null-safe
 * increment; {@link #NONE} is the bundle with none. It is {@code Serializable}, as the Scala case
 * class it replaces was: the exec nodes' evaluator factories carry it to the executors. A suite
 * that sets one or two builds it with {@link #builder}, since ten same-typed positional arguments
 * are a silent-swap hazard (the reason the Scala case class this replaces used named arguments).
 *
 * @param varkaBatches batches served by the kernels
 * @param cacheHits tasks served a kernel class by the shape cache
 * @param cacheMisses tasks that emitted and defined the kernel class
 * @param fallbackBatchesNonArrow batches falling back: input not Arrow-backed
 * @param fallbackBatchesKernel batches falling back: kernel failure
 * @param fallbackBatchesRowPath batches falling back: per-row machinery failure
 * @param fallbackBatchesDeclined batches falling back: a value outside a lowering's range
 * @param emissionFailures tasks that could not emit or define the kernel class
 * @param suspectAllocationSamples sampled kernel batches that allocated like a boxing loop
 * @param warmupBatches batches on the per-row path while a new kernel was compiled
 */
public record VarkaExecMetrics(
    SQLMetric varkaBatches,
    SQLMetric cacheHits,
    SQLMetric cacheMisses,
    SQLMetric fallbackBatchesNonArrow,
    SQLMetric fallbackBatchesKernel,
    SQLMetric fallbackBatchesRowPath,
    SQLMetric fallbackBatchesDeclined,
    SQLMetric emissionFailures,
    SQLMetric suspectAllocationSamples,
    SQLMetric warmupBatches) implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The bundle with no metric: what a suite that builds an evaluator directly passes. */
  public static final VarkaExecMetrics NONE = new VarkaExecMetrics(
      null, null, null, null, null, null, null, null, null, null);

  /** Adds one to {@code metric}, or does nothing for an absent one. */
  public static void inc(SQLMetric metric) {
    if (metric != null) {
      metric.add(1L);
    }
  }

  /**
   * The metric set every Varka node registers, defined once (the four nodes once carried
   * byte-identical copies, where a changed key or description would compile clean and fork the UI
   * vocabularies). {@code numOutputRows} semantics stay per node: a projection counts input rows,
   * a filter counts selected rows.
   */
  public static Map<String, SQLMetric> nodeMetrics(SparkContext sc) {
    var metrics = new LinkedHashMap<String, SQLMetric>();
    metrics.put("numOutputRows", SQLMetrics.createMetric(sc, "number of output rows"));
    metrics.put("numInputBatches", SQLMetrics.createMetric(sc, "number of input batches"));
    metrics.put("numVarkaBatches", SQLMetrics.createMetric(
        sc, "number of input batches processed by the Varka SIMD kernels"));
    metrics.put("numVarkaCacheHits", SQLMetrics.createMetric(
        sc, "number of tasks served a kernel class by the Varka shape cache"));
    metrics.put("numVarkaCacheMisses", SQLMetrics.createMetric(
        sc, "number of tasks that emitted and defined the Varka kernel class"));
    metrics.put("numFallbackBatchesNonArrow", SQLMetrics.createMetric(
        sc, "batches falling back: input not Arrow-backed"));
    metrics.put("numFallbackBatchesKernel", SQLMetrics.createMetric(
        sc, "batches falling back: kernel failure (the ghost fallback)"));
    metrics.put("numFallbackBatchesRowPath", SQLMetrics.createMetric(
        sc, "batches falling back: per-row machinery failure beside the kernel"));
    metrics.put("numFallbackBatchesDeclined", SQLMetrics.createMetric(
        sc, "batches falling back: a value outside a lowering's range"));
    metrics.put("numEmissionFailures", SQLMetrics.createMetric(
        sc, "tasks that could not emit or define the kernel class"));
    metrics.put("numSuspectAllocationSamples", SQLMetrics.createMetric(
        sc, "sampled kernel batches that allocated like a boxing Vector API loop"));
    metrics.put("numWarmupBatches", SQLMetrics.createMetric(
        sc, "batches on the per-row path while a new kernel was compiled"));
    return metrics;
  }

  /**
   * {@link #nodeMetrics} plus the projection nodes' static residual-entry count; a filter's
   * residual is a visible row {@code FilterExec} above it rather than a number.
   */
  public static Map<String, SQLMetric> projectionMetrics(SparkContext sc) {
    var metrics = nodeMetrics(sc);
    metrics.put("numResidualEntries", SQLMetrics.createMetric(
        sc, "projection entries declined to the per-row residual (reasons in EXPLAIN)"));
    return metrics;
  }

  /** The evaluator-facing bundle built from a node's registered metrics. */
  public static VarkaExecMetrics fromNode(Function<String, SQLMetric> metric) {
    return new VarkaExecMetrics(
        metric.apply("numVarkaBatches"),
        metric.apply("numVarkaCacheHits"),
        metric.apply("numVarkaCacheMisses"),
        metric.apply("numFallbackBatchesNonArrow"),
        metric.apply("numFallbackBatchesKernel"),
        metric.apply("numFallbackBatchesRowPath"),
        metric.apply("numFallbackBatchesDeclined"),
        metric.apply("numEmissionFailures"),
        metric.apply("numSuspectAllocationSamples"),
        metric.apply("numWarmupBatches"));
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Sets the metrics a suite cares about; every other stays absent. */
  public static final class Builder {
    private SQLMetric varkaBatches;
    private SQLMetric cacheHits;
    private SQLMetric cacheMisses;
    private SQLMetric fallbackBatchesNonArrow;
    private SQLMetric fallbackBatchesKernel;
    private SQLMetric fallbackBatchesRowPath;
    private SQLMetric fallbackBatchesDeclined;
    private SQLMetric emissionFailures;
    private SQLMetric suspectAllocationSamples;
    private SQLMetric warmupBatches;

    private Builder() {
    }

    public Builder varkaBatches(SQLMetric m) {
      varkaBatches = m;
      return this;
    }

    public Builder cacheHits(SQLMetric m) {
      cacheHits = m;
      return this;
    }

    public Builder cacheMisses(SQLMetric m) {
      cacheMisses = m;
      return this;
    }

    public Builder fallbackBatchesNonArrow(SQLMetric m) {
      fallbackBatchesNonArrow = m;
      return this;
    }

    public Builder fallbackBatchesKernel(SQLMetric m) {
      fallbackBatchesKernel = m;
      return this;
    }

    public Builder fallbackBatchesRowPath(SQLMetric m) {
      fallbackBatchesRowPath = m;
      return this;
    }

    public Builder fallbackBatchesDeclined(SQLMetric m) {
      fallbackBatchesDeclined = m;
      return this;
    }

    public Builder emissionFailures(SQLMetric m) {
      emissionFailures = m;
      return this;
    }

    public Builder suspectAllocationSamples(SQLMetric m) {
      suspectAllocationSamples = m;
      return this;
    }

    public Builder warmupBatches(SQLMetric m) {
      warmupBatches = m;
      return this;
    }

    public VarkaExecMetrics build() {
      return new VarkaExecMetrics(varkaBatches, cacheHits, cacheMisses, fallbackBatchesNonArrow,
          fallbackBatchesKernel, fallbackBatchesRowPath, fallbackBatchesDeclined, emissionFailures,
          suspectAllocationSamples, warmupBatches);
    }
  }
}
