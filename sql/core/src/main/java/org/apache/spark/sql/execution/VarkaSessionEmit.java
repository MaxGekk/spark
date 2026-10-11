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

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions;
import org.apache.spark.sql.internal.SQLConf;

/**
 * The emit settings a session chooses, resolved from its {@code SQLConf} once, where an exec node
 * is planned, and carried to the factories and evaluators that emit its kernels (VARKA-308). The
 * executors cannot read the session's configuration, so the settings travel with the plan, and a
 * kernel's options are derived from them in one place, {@code VarkaColumnarToRowExec.emitOptions},
 * which the compiler asks at planning and the evaluator asks on the executor: the two must agree,
 * or the plan and the task could classify one projection differently.
 *
 * @param useAVX the {@code -XX:UseAVX} level a lowering is chosen for
 *        ({@code spark.sql.codegen.varka.emit.useAVX}), {@link VarkaEmitOptions#USE_AVX_UNKNOWN}
 *        for the emitter's own default
 * @param exactRefusalStatus whether a method of several refusal reasons gives each its own mask
 *        ({@code spark.sql.codegen.varka.emit.exactRefusalStatus}, VARKA-304)
 */
public record VarkaSessionEmit(int useAVX, boolean exactRefusalStatus) implements Serializable {

  private static final long serialVersionUID = 1L;

  /** A session that sets nothing: what a suite that builds an evaluator directly passes. */
  public static final VarkaSessionEmit DEFAULTS =
      new VarkaSessionEmit(VarkaEmitOptions.USE_AVX_UNKNOWN, false);

  /** The session's settings, read from {@code conf}: the one place they are bound. */
  public static VarkaSessionEmit of(SQLConf conf) {
    return new VarkaSessionEmit(conf.varkaEmitUseAVX(), conf.varkaEmitExactRefusalStatus());
  }

  /** {@code base} with the session's settings laid over it; {@code base} itself at the defaults. */
  public VarkaEmitOptions apply(VarkaEmitOptions base) {
    VarkaEmitOptions options = base;
    if (useAVX != VarkaEmitOptions.USE_AVX_UNKNOWN) {
      options = options.withUseAVX(useAVX);
    }
    if (exactRefusalStatus) {
      options = options.withExactRefusalStatus(true);
    }
    return options;
  }
}
