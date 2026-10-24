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

/**
 * A zero the JIT cannot see is zero, which an emitted kernel XORs into a word before
 * {@code VectorMask.fromLong} (VARKA-491). JDK 25's C2 miscompiles {@code fromLong(s, m.toLong())}
 * on x86 without AVX-512 mask registers: it replaces the pair by the mask with its lanes cast to
 * booleans, which a select then reads as false on every true lane (VARKA-396). A masked body's
 * known-true word becomes that pair once its columns have no nulls, which C2 learns from the
 * profile. HotSpot never folds a static field that is not final into a constant, so the XOR stays
 * in the compiled code and the pair never forms; it costs a load C2 lifts out of the loop and one
 * XOR a site.
 *
 * <p>Nothing may write the field: a kernel that read anything but zero would compute wrong masks.
 */
public final class VarkaOpaqueZero {

  private VarkaOpaqueZero() {}

  /** Zero, and never written. */
  // Not final on purpose: a final field is a constant to C2, and the XOR would fold away. Public,
  // since the emitted kernels live in another runtime package.
  @SuppressWarnings("NonFinalStaticField")
  public static long zero = 0L;
}
