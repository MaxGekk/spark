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

/**
 * Marks a throwable as coming from the emitted kernel invocation itself (task-21 review): the
 * exec nodes label their per-batch catch by it, so a catchable failure in the per-row machinery
 * running beside the kernel - a residual or merge projection's compile or evaluation - is not
 * metered as a kernel failure.
 */
public final class VarkaKernelFailure extends RuntimeException {
  /** The further kernel of a projection that failed; null for the evaluator's own. */
  public final String kernel;

  public VarkaKernelFailure(Throwable cause, String kernel) {
    super(cause);
    this.kernel = kernel;
  }

  public VarkaKernelFailure(Throwable cause) {
    this(cause, null);
  }
}
