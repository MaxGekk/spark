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
 * JFR event (task 22) for one shape-cache lookup: the per-task resolution of a kernel class,
 * hit or miss - the event form of the counters task 18 left for exactly this. The execution
 * field carries the caller's truncated identity ({@code Varka_<operator>_Stage<n>: ...}), so a
 * recording joins shape-named classes back to the operators that ran them without the identity
 * riding the shared bytes. Fires only while a recording has the event enabled.
 */
@Name("org.apache.spark.sql.varka.ShapeCacheLookup")
@Label("Varka Shape-Cache Lookup")
@Category("Varka")
@Enabled(true)
@Threshold("0 ms")
public final class VarkaCacheLookupEvent extends Event {

  @Label("Shape Hash")
  public String shapeHash;

  @Label("Cache Hit")
  public boolean hit;

  @Label("Execution")
  public String execution;
}
