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

import java.util.function.Supplier;

import scala.Option;
import scala.collection.immutable.Seq;

import org.apache.arrow.memory.BufferAllocator;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaProjection;

/**
 * One further kernel of a projection several kernels serve
 * ({@code VarkaEmitOptions.severalKernels}, {@code VARKA-190.md} 11): the evaluator machinery of
 * {@link VarkaEvaluatorBase} - the shape-cached runner, its warm-up, its scratch and its argument
 * arrays - for that kernel alone. It serves no batch itself: the projection's evaluator asks it
 * whether it can run and whether it is
 * ready, and runs it into the one output batch ({@code runKernel}), so every fallback is still
 * decided, counted and taken once per batch, by the projection's evaluator.
 *
 * <p>It allocates from the projection's allocator and registers no task listener of its own: the
 * projection's evaluator releases its scratch at task end, before closing that allocator, so a task
 * has one allocator however many kernels its projection has.
 */
final class VarkaKernelPart extends VarkaEvaluatorBase {

  private final CompiledVarkaProjection plan;
  private final Seq<NamedExpression> entries;
  private final Supplier<BufferAllocator> allocator;

  VarkaKernelPart(
      CompiledVarkaProjection plan,
      Seq<NamedExpression> entries,
      Seq<Attribute> childOutput,
      String operatorName,
      Option<String> classDumpDirectory,
      VarkaExecMetrics metrics,
      int emitUseAVX,
      boolean warmupEnabled,
      Supplier<BufferAllocator> allocator) {
    super(childOutput, operatorName, classDumpDirectory, metrics, emitUseAVX, warmupEnabled);
    this.plan = plan;
    this.entries = entries;
    this.allocator = allocator;
  }

  @Override
  protected Option<CompiledVarkaProjection> fusedPlan() {
    return Option.apply(plan);
  }

  /** The entries this kernel computes, so its side-table identity names them. */
  @Override
  protected java.util.Iterator<String> identityEntries() {
    return render(entries);
  }

  @Override
  protected BufferAllocator taskAllocator() {
    return allocator.get();
  }
}
