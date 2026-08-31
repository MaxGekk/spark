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

package org.apache.spark.sql.catalyst.expressions.codegen.varka;

import jdk.jfr.Category;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

/**
 * JFR event (task 22) for one degradation of the Varka fast path, the event form of the
 * fallback-cause metrics: {@code cause} is one of the constants below - {@link #NON_ARROW_BATCH}
 * (a non-empty batch whose referenced columns are not Arrow-backed, per batch; an empty batch
 * is served trivially and carries no cause at all), {@link #KERNEL_FAILURE} (the ghost fallback
 * caught an error from the emitted kernel itself, per batch), {@link #ROW_PATH_FAILURE} (the
 * task-21 review split: an error from the per-row machinery beside the kernel - the residual or
 * merge projection - which is not the kernel's failure and must not inflate its metric) or
 * {@link #RANGE_DECLINED} (a partial lowering met a value outside its range and declined the
 * batch rather than answering wrongly, per batch), and
 * {@link #EMISSION_FAILURE} (the task could not emit or define its kernel class, once per
 * task). The kernel identity is the shape-named class plus the operator/stage, the same string
 * the fallback warnings log; {@code exceptionClass} is empty for the non-Arrow cause, which is
 * a plan/data property rather than an error. Every emission site is on a fallback path, never
 * in the fused loop; fires only while a recording has the event enabled.
 */
@Name("org.apache.spark.sql.varka.Fallback")
@Label("Varka Fallback")
@Category("Varka")
public final class VarkaFallbackEvent extends Event {

  // The cause vocabulary, in one place: recordings, JMC rules and dashboards filter on these
  // strings, so a call-site literal that drifted would fork the vocabulary silently. Static
  // fields are not event payload; JFR ignores them.
  public static final String NON_ARROW_BATCH = "non-arrow-batch";
  public static final String KERNEL_FAILURE = "kernel-failure";
  public static final String ROW_PATH_FAILURE = "row-path-failure";
  public static final String EMISSION_FAILURE = "emission-failure";

  /** A lowering declined the batch: some lane fell outside the range it is defined over. */
  public static final String RANGE_DECLINED = "range-declined";

  @Label("Cause")
  public String cause;

  @Label("Kernel Identity")
  public String kernelIdentity;

  @Label("Exception Class")
  public String exceptionClass;
}
