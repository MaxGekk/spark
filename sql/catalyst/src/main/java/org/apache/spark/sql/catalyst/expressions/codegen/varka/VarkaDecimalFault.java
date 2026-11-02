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
 * The fault a test plants in the 128-bit decimal's comparisons to prove that the checks over them
 * can fail (milestone 8 row 504, `m8/VARKA-359.md` 11.7): a plausible wrong lowering that agrees
 * with Spark on most inputs. {@link #NONE} in production. The lowering reads it while it builds
 * the IR, so a kernel lowered under it is a different IR, and so a different shape key, from the
 * clean one: a faulty class can never be cached under a clean key. The load's faults change the
 * emission rather than the IR, and are emit options instead
 * ({@link VarkaEmitOptions.DecimalFault}).
 */
public enum VarkaDecimalFault {
  /** No fault: the lowering as built. */
  NONE,
  /**
   * The low words compared signed ({@code LT} for {@code ULT}, and so on), which is wrong
   * exactly where one low word has its sign bit set and the high words are equal.
   */
  LOW_WORD_SIGNED;

  private static volatile VarkaDecimalFault planted = NONE;

  /** The fault planted now; {@link #NONE} unless a test planted one. */
  public static VarkaDecimalFault planted() {
    return planted;
  }

  /** Plants {@code fault} until the next call; {@link #NONE} clears it. */
  public static void plantForTesting(VarkaDecimalFault fault) {
    planted = fault;
  }
}
