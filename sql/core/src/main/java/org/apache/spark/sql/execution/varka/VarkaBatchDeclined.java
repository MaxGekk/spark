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
 * The batch was declined to the row engine: the kernel ran and returned a non-zero status,
 * meaning some lane lay outside the range a partial lowering is defined over, or the evaluator's
 * own pre-check found an input lane outside a bound the compiler recorded
 * ({@link VarkaKernelRunner#STATUS_INPUT_BOUND}). Not an error - it carries no cause and no stack
 * trace, because it is control flow on a designed path, and the evaluator's {@code serveBatch}
 * turns it into the row-engine fallback.
 */
public final class VarkaBatchDeclined extends RuntimeException {
  public final int status;
  /** The further kernel of a projection that declined; null for the evaluator's own. */
  public final String kernel;

  public VarkaBatchDeclined(int status, String kernel) {
    super(null, null, false, false);
    this.status = status;
    this.kernel = kernel;
  }

  public VarkaBatchDeclined(int status) {
    this(status, null);
  }
}
