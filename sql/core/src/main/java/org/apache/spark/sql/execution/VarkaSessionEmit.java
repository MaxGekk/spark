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

import javax.annotation.Nullable;

import scala.Option;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions;
import org.apache.spark.sql.internal.SQLConf;

/**
 * The emit settings a session chooses, resolved from its {@code SQLConf} once, when the columnar
 * rule builds an exec node - the node holds them, so a plan executed again after the conf changed
 * still emits what it was planned with - and carried to the factories and evaluators that emit
 * its kernels (VARKA-308). Each setting either overrides the base options or, unset, leaves them:
 * a session that says nothing must not undo a variant a suite drives through the test hook, and a
 * session that sets a value, {@code false} included, must get it. The
 * executors cannot read the session's configuration, so the settings travel with the plan, and a
 * kernel's options are derived from them in one place, {@code VarkaColumnarToRowExec.emitOptions},
 * which the compiler asks at planning and the evaluator asks on the executor: the two must agree,
 * or the plan and the task could classify one projection differently.
 *
 * @param useAVX the {@code -XX:UseAVX} level a lowering is chosen for
 *        ({@code spark.sql.codegen.varka.emit.useAVX}), {@link VarkaEmitOptions#USE_AVX_UNKNOWN}
 *        to leave the base options' level
 * @param exactRefusalStatus whether a method of several refusal reasons gives each its own mask
 *        ({@code spark.sql.codegen.varka.emit.exactRefusalStatus}, VARKA-304), null to leave the
 *        base options' setting
 */
public record VarkaSessionEmit(int useAVX, @Nullable Boolean exactRefusalStatus)
    implements Serializable {

  private static final long serialVersionUID = 1L;

  /** A session that sets nothing: what a suite that builds an evaluator directly passes. */
  public static final VarkaSessionEmit DEFAULTS =
      new VarkaSessionEmit(VarkaEmitOptions.USE_AVX_UNKNOWN, null);

  public VarkaSessionEmit {
    // SQLConf checks the same on the conf; a record built another way is held to it too.
    if (useAVX < VarkaEmitOptions.USE_AVX_UNKNOWN) {
      throw new IllegalArgumentException("useAVX is a level from 0 or -1, got " + useAVX);
    }
  }

  /** The session's settings, read from {@code conf}: the one place they are bound. */
  public static VarkaSessionEmit of(SQLConf conf) {
    Option<Object> exact = conf.varkaEmitExactRefusalStatus();
    return new VarkaSessionEmit(conf.varkaEmitUseAVX(),
        exact.isDefined() ? (Boolean) exact.get() : null);
  }

  /** {@code base} with the session's settings laid over it, or {@code base} when none is set. */
  public VarkaEmitOptions apply(VarkaEmitOptions base) {
    if (useAVX == VarkaEmitOptions.USE_AVX_UNKNOWN && exactRefusalStatus == null) {
      return base;
    }
    var options = base.toBuilder();
    if (useAVX != VarkaEmitOptions.USE_AVX_UNKNOWN) {
      options.useAVX(useAVX);
    }
    if (exactRefusalStatus != null) {
      options.exactRefusalStatus(exactRefusalStatus);
    }
    return options.build();
  }
}
