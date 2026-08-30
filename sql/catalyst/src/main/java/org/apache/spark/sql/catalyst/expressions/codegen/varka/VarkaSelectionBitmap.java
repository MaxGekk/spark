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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * The read side of a selection bitmap (task 21): the bit-packed mask a filter kernel writes
 * into its {@code dstValidity} slot, one bit per row, set exactly where the predicate is known
 * true (see {@link VarkaFusedKernel}'s selection-output contract). The layout is Arrow's
 * validity layout - LSB of byte 0 is row 0 - because the kernel writes it with the same
 * {@code orValidityBitsAt}/{@code setBit} helpers that write output validity, so the two
 * bitmaps cannot drift apart in format.
 *
 * <p>These helpers read byte-wise so they hold for any segment of at least
 * {@code (length + 7) / 8} bytes, the size the kernel zeroes; bits at and above {@code length}
 * are never read, so a reused buffer's stale tail cannot leak into a count.
 */
public final class VarkaSelectionBitmap {

  private VarkaSelectionBitmap() {
  }

  /** Whether {@code row}'s bit is set. The caller keeps {@code row} within {@code [0, length)}. */
  public static boolean isSet(MemorySegment mask, int row) {
    byte b = mask.get(ValueLayout.JAVA_BYTE, row >>> 3);
    return (b >>> (row & 7) & 1) != 0;
  }

  /** The number of set bits among the first {@code length} rows - the selected-row count. */
  public static int countSet(MemorySegment mask, int length) {
    int count = 0;
    int fullBytes = length >>> 3;
    for (long i = 0; i < fullBytes; i++) {
      count += Integer.bitCount(mask.get(ValueLayout.JAVA_BYTE, i) & 0xFF);
    }
    int rest = length & 7;
    if (rest != 0) {
      int lastByte = mask.get(ValueLayout.JAVA_BYTE, fullBytes) & 0xFF;
      count += Integer.bitCount(lastByte & ((1 << rest) - 1));
    }
    return count;
  }
}
