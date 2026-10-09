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

import org.apache.arrow.vector.ValueVector;

import org.apache.spark.sql.vectorized.ArrowColumnVector;

/**
 * An Arrow-backed column vector the Varka evaluator owns: {@code closeIfFreeable} is a no-op, per
 * Spark's two-tier close convention, because the vector's lifecycle belongs to the evaluator's
 * release paths. A consumer that frees the batches it drains (the Arrow cache writer calls
 * {@code ColumnarBatch.closeIfFreeable()} per batch) must not close what it does not own: it would
 * free the buffers under the evaluator's own later release, the double close the ownership rules of
 * {@link VarkaKernelEvaluator} forbid. {@code WritableColumnVector} makes exactly this override for
 * the same reason; a plain {@code ArrowColumnVector} does not, because a scan's vectors really are
 * freed that way.
 */
final class VarkaOwnedArrowColumnVector extends ArrowColumnVector {

  VarkaOwnedArrowColumnVector(ValueVector vector) {
    super(vector);
  }

  @Override
  public void closeIfFreeable() {
  }
}
