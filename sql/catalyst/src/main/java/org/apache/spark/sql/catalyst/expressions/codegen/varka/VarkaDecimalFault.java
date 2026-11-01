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
 * The faults a test plants in the 128-bit decimal's load and comparisons to prove that the checks
 * over them can fail (milestone 8 row 504, `m8/VARKA-359.md` 11.7): each is a plausible wrong
 * lowering that agrees with Spark on most inputs. {@link #NONE} in production; a test plants one
 * around an emission and clears it after, and emits the kernel itself rather than through the
 * class cache, so that no faulty class outlives the test.
 */
public enum VarkaDecimalFault {
  /** No fault: the lowering as built. */
  NONE,
  /**
   * The low words compared signed ({@code LT} for {@code ULT}, and so on), which is wrong
   * exactly where one low word has its sign bit set and the high words are equal.
   */
  LOW_WORD_SIGNED,
  /**
   * The load's check against {@code 10^p} rather than {@code 10^p - 1}, which passes a slot
   * holding {@code 10^p} or {@code -10^p}, one past the precision, where Spark's reader raises.
   */
  BOUND_AT_POWER,
  /**
   * The sibling word parked from the visited word's own rearrange, so that both words of a
   * column read the same half of its slots.
   */
  SIBLING_FROM_WRONG_REARRANGE;

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
