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

import java.util.List;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaFamily;

/**
 * Marks a throwable as coming from the emitted kernel invocation itself (VARKA-21 review): the
 * exec nodes label their per-batch catch by it, so a catchable failure in the per-row machinery
 * running beside the kernel - a residual or merge projection's compile or evaluation - is not
 * metered as a kernel failure.
 */
public final class VarkaKernelFailure extends RuntimeException {
  /** The further kernel of a projection that failed; null for the evaluator's own. */
  public final String kernel;

  /**
   * The shape hash of the kernel that failed, which the failure's context record names
   * (VARKA-314).
   */
  public final String shapeHash;

  /**
   * The families whose nodes the further kernel {@code kernel} holds, which the failure's context
   * record names with their switches (VARKA-318); empty for the evaluator's own kernel, whose
   * evaluator knows its families.
   */
  public final List<VarkaFamily> families;

  public VarkaKernelFailure(Throwable cause, String kernel, String shapeHash) {
    this(cause, kernel, shapeHash, List.of());
  }

  public VarkaKernelFailure(
      Throwable cause, String kernel, String shapeHash, List<VarkaFamily> families) {
    super(cause);
    this.kernel = kernel;
    this.shapeHash = shapeHash;
    this.families = List.copyOf(families);
  }
}
