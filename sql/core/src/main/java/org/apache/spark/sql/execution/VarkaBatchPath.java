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

/**
 * One of the two paths {@link VarkaEvaluatorBase#serveBatch} runs a batch down: the kernel's or
 * the caller's fallback. The Scala exec nodes pass their closures as these; the evaluator calls
 * them and writes no lambda of its own on the batch path (VARKA-267). The closures themselves are
 * the nodes': each batch builds the two, as the Scala by-name thunks it replaces were built, so the
 * allocation the rule names is moved to the call site and not removed (a per-node serve method
 * would remove it, and is a later step).
 *
 * @param <T> what a path produces: an output batch, or a row iterator
 */
@FunctionalInterface
public interface VarkaBatchPath<T> {
  T run();
}
