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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.execution.metric.SQLMetrics;
import org.apache.spark.sql.execution.varka.VarkaFallbackAccounting;
import org.apache.spark.sql.execution.varka.VarkaReasonMetric;

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
 * @param paths the rows and the time through the kernel path and the fallback
 * @param declinedByReason declined batches by refusal reason, in
 *        {@link VarkaFallbackAccounting#REASONS}' order, or empty where none is registered; each
 *        a {@link VarkaReasonMetric}, which keeps exact and unattributed declines apart
 */
public record VarkaExecMetrics(
    @Nullable SQLMetric varkaBatches,
    @Nullable SQLMetric cacheHits,
    @Nullable SQLMetric cacheMisses,
    @Nullable SQLMetric fallbackBatchesNonArrow,
    @Nullable SQLMetric fallbackBatchesKernel,
    @Nullable SQLMetric fallbackBatchesRowPath,
    @Nullable SQLMetric fallbackBatchesDeclined,
    @Nullable SQLMetric emissionFailures,
    @Nullable SQLMetric suspectAllocationSamples,
    @Nullable SQLMetric warmupBatches,
    Paths paths,
    List<SQLMetric> declinedByReason) implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The bundle with no metric: what a suite that builds an evaluator directly passes. */
  public static final VarkaExecMetrics NONE = new VarkaExecMetrics(
      null, null, null, null, null, null, null, null, null, null, Paths.NONE, List.of());

  /**
   * The rows and the time through each path. A batch's rows count once, under the path that
   * answered it, warm-up batches with the fallback's. The fallback's time is registered only on
   * the nodes whose fallback is eager - the columnar-out ones; a row-out node's fallback is a
   * lazy row iterator that a bracket around the call does not time, so it has no such metric
   * rather than one that reads 0.
   *
   * @param kernelRows input rows of the batches Varka answered without the per-row path: the
   *        kernels', and a projection's that only forwards columns and runs no kernel
   * @param fallbackRows input rows of the batches the per-row path answered, whatever the cause
   * @param kernelTime nanoseconds in the kernel path of the batches it served, until its output
   *        batch: on a row-out node the residual entries and the row conversion are evaluated
   *        lazily after it, and are not in it; the kernel's emission is not in it either
   * @param fallbackTime nanoseconds in an eager fallback, the per-row machinery's one-time
   *        compile included; null on a row-out node
   */
  public record Paths(
      @Nullable SQLMetric kernelRows,
      @Nullable SQLMetric fallbackRows,
      @Nullable SQLMetric kernelTime,
      @Nullable SQLMetric fallbackTime) implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final Paths NONE = new Paths(null, null, null, null);
  }

  public VarkaExecMetrics {
    paths = paths == null ? Paths.NONE : paths;
    declinedByReason = declinedByReason == null ? List.of() : List.copyOf(declinedByReason);
    if (!declinedByReason.isEmpty()
        && declinedByReason.size() != VarkaFallbackAccounting.REASONS.size()) {
      throw new IllegalArgumentException("declinedByReason holds " + declinedByReason.size()
          + " metrics for " + VarkaFallbackAccounting.REASONS.size() + " reasons");
    }
  }

  /** Adds {@code n} to {@code metric}, or does nothing for an absent one. */
  public static void add(@Nullable SQLMetric metric, long n) {
    if (metric != null) {
      metric.add(n);
    }
  }

  /** The key a reason's metric is registered under on a node. */
  public static String reasonKey(String reason) {
    return "numDeclinedBatches[" + reason + "]";
  }

  /** Adds one to {@code metric}, or does nothing for an absent one. */
  public static void inc(@Nullable SQLMetric metric) {
    if (metric != null) {
      metric.add(1L);
    }
  }

  /**
   * The metric set every Varka node registers, defined once (the four nodes once carried
   * byte-identical copies, where a changed key or description would compile clean and fork the UI
   * vocabularies). {@code numOutputRows} semantics stay per node: a projection counts input rows,
   * a filter counts selected rows. {@code eagerFallback} says whether the node's fallback builds
   * its output before it returns, so that its time can be measured ({@link Paths}).
   */
  public static Map<String, SQLMetric> nodeMetrics(SparkContext sc, boolean eagerFallback) {
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
    metrics.put("numVarkaRows", SQLMetrics.createMetric(
        sc, "input rows of the batches Varka answered without the per-row path"));
    metrics.put("numFallbackRows", SQLMetrics.createMetric(
        sc, "input rows of the batches the per-row path answered"));
    metrics.put("varkaKernelTime", SQLMetrics.createNanoTimingMetric(
        sc, "time in the Varka kernels, until their output batch", -1L));
    if (eagerFallback) {
      metrics.put("fallbackTime", SQLMetrics.createNanoTimingMetric(
          sc, "time in the per-row fallback, its compile included", -1L));
    }
    for (String reason : VarkaFallbackAccounting.REASONS) {
      metrics.put(reasonKey(reason), VarkaReasonMetric.create(
          sc, "batches declined, reason " + reason));
    }
    return metrics;
  }

  /**
   * {@link #nodeMetrics} plus the projection nodes' static residual-entry count; a filter's
   * residual is a visible row {@code FilterExec} above it rather than a number.
   */
  public static Map<String, SQLMetric> projectionMetrics(SparkContext sc, boolean eagerFallback) {
    var metrics = nodeMetrics(sc, eagerFallback);
    metrics.put("numResidualEntries", SQLMetrics.createMetric(
        sc, "projection entries declined to the per-row residual (reasons in EXPLAIN)"));
    return metrics;
  }

  /**
   * The evaluator-facing bundle built from a node's registered metrics; {@code metric} returns
   * null for a key the node did not register. Only {@code fallbackTime} may be missing: a
   * missing key of the {@link #nodeMetrics} set throws, so a key renamed on one side only fails
   * the plan rather than counting nothing.
   */
  public static VarkaExecMetrics fromNode(Function<String, SQLMetric> metric) {
    Function<String, SQLMetric> required = key -> {
      SQLMetric m = metric.apply(key);
      if (m == null) {
        throw new IllegalStateException("the node registers no metric " + key);
      }
      return m;
    };
    var reasons = new ArrayList<SQLMetric>();
    for (String reason : VarkaFallbackAccounting.REASONS) {
      reasons.add(required.apply(reasonKey(reason)));
    }
    return new VarkaExecMetrics(
        required.apply("numVarkaBatches"),
        required.apply("numVarkaCacheHits"),
        required.apply("numVarkaCacheMisses"),
        required.apply("numFallbackBatchesNonArrow"),
        required.apply("numFallbackBatchesKernel"),
        required.apply("numFallbackBatchesRowPath"),
        required.apply("numFallbackBatchesDeclined"),
        required.apply("numEmissionFailures"),
        required.apply("numSuspectAllocationSamples"),
        required.apply("numWarmupBatches"),
        new Paths(required.apply("numVarkaRows"), required.apply("numFallbackRows"),
            required.apply("varkaKernelTime"), metric.apply("fallbackTime")),
        reasons);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Sets the metrics a suite cares about; every other stays absent. */
  public static final class Builder {
    private @Nullable SQLMetric varkaBatches;
    private @Nullable SQLMetric cacheHits;
    private @Nullable SQLMetric cacheMisses;
    private @Nullable SQLMetric fallbackBatchesNonArrow;
    private @Nullable SQLMetric fallbackBatchesKernel;
    private @Nullable SQLMetric fallbackBatchesRowPath;
    private @Nullable SQLMetric fallbackBatchesDeclined;
    private @Nullable SQLMetric emissionFailures;
    private @Nullable SQLMetric suspectAllocationSamples;
    private @Nullable SQLMetric warmupBatches;
    private Paths paths = Paths.NONE;
    private List<SQLMetric> declinedByReason = List.of();

    private Builder() {
    }

    public Builder varkaBatches(@Nullable SQLMetric m) {
      varkaBatches = m;
      return this;
    }

    public Builder cacheHits(@Nullable SQLMetric m) {
      cacheHits = m;
      return this;
    }

    public Builder cacheMisses(@Nullable SQLMetric m) {
      cacheMisses = m;
      return this;
    }

    public Builder fallbackBatchesNonArrow(@Nullable SQLMetric m) {
      fallbackBatchesNonArrow = m;
      return this;
    }

    public Builder fallbackBatchesKernel(@Nullable SQLMetric m) {
      fallbackBatchesKernel = m;
      return this;
    }

    public Builder fallbackBatchesRowPath(@Nullable SQLMetric m) {
      fallbackBatchesRowPath = m;
      return this;
    }

    public Builder fallbackBatchesDeclined(@Nullable SQLMetric m) {
      fallbackBatchesDeclined = m;
      return this;
    }

    public Builder emissionFailures(@Nullable SQLMetric m) {
      emissionFailures = m;
      return this;
    }

    public Builder suspectAllocationSamples(@Nullable SQLMetric m) {
      suspectAllocationSamples = m;
      return this;
    }

    public Builder warmupBatches(@Nullable SQLMetric m) {
      warmupBatches = m;
      return this;
    }

    public Builder paths(Paths p) {
      paths = p;
      return this;
    }

    /** One metric per {@link VarkaFallbackAccounting#REASONS} entry, in its order. */
    public Builder declinedByReason(List<SQLMetric> metrics) {
      declinedByReason = metrics;
      return this;
    }

    public VarkaExecMetrics build() {
      return new VarkaExecMetrics(varkaBatches, cacheHits, cacheMisses, fallbackBatchesNonArrow,
          fallbackBatchesKernel, fallbackBatchesRowPath, fallbackBatchesDeclined, emissionFailures,
          suspectAllocationSamples, warmupBatches, paths, declinedByReason);
    }
  }
}
