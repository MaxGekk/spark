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
import jdk.jfr.Description;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * A test-only JFR event around one compared kernel call (VARKA-274). Its start and end are the
 * window {@link VarkaTierWitness} reads a call's tier in: a deoptimization or an on-stack
 * replacement whose JFR timestamp falls between them happened during this call. JFR stamps every
 * event from one clock, so the window can be compared with the JVM's own compilation and
 * deoptimization events, which {@code System.nanoTime} could not.
 *
 * <p>The witness also commits one with a negative {@code call} after a history's last call, as a
 * marker: once the recording stream has delivered it, every event committed before it has been
 * delivered too.
 */
@Name("varka.CheckedCall")
@Label("Varka checked kernel call")
@Category({"Varka", "Test"})
@Description("One kernel call whose answers a fuzz history compares with the reference")
@StackTrace(false)
public final class VarkaCheckedCallEvent extends jdk.jfr.Event {

  /** The witness's number for the call, or minus a settle marker's number. */
  @Label("Call")
  public long call;

  /** The kernel class the call ran. */
  @Label("Kernel")
  public String kernel;
}
