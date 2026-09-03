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
 * JFR event (task 50) reporting that C2 compiled the same generated method to a materially
 * different size than it did earlier in this JVM. Since the bytecode behind a shape hash is
 * byte-identical by construction, two compilations of the same method at the same tier are
 * compiling exactly the same input, so a size difference is the register allocator's doing -
 * task 32 measured a 2x spread (1581 instructions against 3000) between a clean allocation and
 * a spilling one, worth 30-40% of throughput, with nothing anywhere reporting it.
 *
 * <p>Fires only while a JFR recording has the event enabled <i>and</i>
 * {@code spark.sql.codegen.varka.compilationWatch.enabled} is on; the watch that produces it is
 * not running otherwise. Sits in the same {@code Varka} category as the emission, cache-lookup
 * and fallback events, so a recording already capturing those picks this up with no extra
 * wiring.
 */
@Name("org.apache.spark.sql.varka.CompilationDivergence")
@Label("Varka Compilation Divergence")
@Category("Varka")
public final class VarkaCompilationDivergenceEvent extends Event {

  @Label("Shape Hash")
  public String shapeHash;

  @Label("Method Name")
  public String methodName;

  @Label("Compile Level")
  public int compileLevel;

  @Label("Baseline Code Size")
  public long baselineCodeSize;

  @Label("Observed Code Size")
  public long observedCodeSize;
}
