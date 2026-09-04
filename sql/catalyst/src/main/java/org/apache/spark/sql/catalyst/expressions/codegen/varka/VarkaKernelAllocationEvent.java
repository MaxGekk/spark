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
 * One allocation sample of a fused kernel's {@code run} (the species-pollution check): the
 * bytes the calling thread allocated across the call, and the sampler's verdict on them. A
 * kernel that runs as emitted allocates nothing per row - its vectors live in registers - so a
 * sample in the hundreds of bytes per row means the Vector API is boxing, which on HotSpot has
 * one known cause in this code base: two vector species of one lane type ran hot in the same
 * JVM, and the shared Vector API templates went bimorphic ({@code SKILLS.md}, "Every operator
 * the plans rely on ..."). The kernel still answers correctly, several times slower; nothing
 * but this sample, the metric beside it and the one-time warning would say so.
 *
 * <p>Emitted on every sample, suspect or not, so a recording of a healthy run shows the
 * sampler ran and what it saw. {@link VarkaAllocationSampler} decides when a sample is taken
 * and what counts as suspect.
 */
@Name("org.apache.spark.sql.varka.KernelAllocation")
@Label("Varka Kernel Allocation")
@Category("Varka")
public final class VarkaKernelAllocationEvent extends Event {

  @Label("Kernel Identity")
  public String kernelIdentity;

  @Label("Batch Index")
  public long batchIndex;

  @Label("Rows")
  public int rows;

  @Label("Allocated Bytes")
  public long allocatedBytes;

  @Label("Suspect")
  public boolean suspect;
}
