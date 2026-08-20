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

package org.apache.spark.sql.varka.vector;

/**
 * Test-only stand-in for the engine module's {@code DateVectorOps} kernel, at the same
 * fully qualified name and method contract, so the catalyst classpath can resolve the
 * kernel owner referenced by {@code ClassFileGenOp} descriptors while the engine module
 * is not on the classpath. Task 6 replaces this with the real engine's kernel.
 */
public final class DateVectorOps {

  private DateVectorOps() {}

  /** Stub for {@code vectorAddDays}: no-op. */
  public static void vectorAddDays(
      long srcData, long srcValidity, int srcNullCount,
      long dstData, long dstValidity, int length, int daysOffset) {
  }

  /** Stub for {@code vectorSubDays}: no-op. */
  public static void vectorSubDays(
      long srcData, long srcValidity, int srcNullCount,
      long dstData, long dstValidity, int length, int daysOffset) {
  }

  /** Stub for {@code vectorDateDiff}: no-op. */
  public static void vectorDateDiff(
      long dataA, long validityA, int nullCountA,
      long dataB, long validityB, int nullCountB,
      long dstData, long dstValidity, int length) {
  }
}
