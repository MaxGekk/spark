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
import jdk.jfr.Enabled;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.Threshold;

/**
 * JFR event (task 22) for one degradation of the Varka fast path, the event form of the
 * fallback-cause metrics: {@code cause} is one of {@code non-arrow-batch} (a batch
 * {@code canRun} refused, per batch), {@code kernel-failure} (the ghost fallback caught a
 * kernel error, per batch) or {@code emission-failure} (the task could not emit or define its
 * kernel class, once per task). The kernel identity is the shape-named class plus the
 * operator/stage, the same string the fallback warnings log; {@code exceptionClass} is empty
 * for the non-Arrow cause, which is a plan/data property rather than an error. Every emission
 * site is on a fallback path, never in the fused loop; fires only while a recording has the
 * event enabled.
 */
@Name("org.apache.spark.sql.varka.Fallback")
@Label("Varka Fallback")
@Category("Varka")
@Enabled(true)
@Threshold("0 ms")
public final class VarkaFallbackEvent extends Event {

  @Label("Cause")
  public String cause;

  @Label("Kernel Identity")
  public String kernelIdentity;

  @Label("Exception Class")
  public String exceptionClass;
}
