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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

/**
 * A word's bits as a mask, without {@code VectorMask.fromLong} (VARKA-491): lane {@code i} is set
 * where bit {@code i} of {@code word} is, for each lane type a kernel computes in.
 *
 * <p>JDK 25's C2 miscompiles {@code VectorMask.fromLong(s, m.toLong())} on x86 without AVX-512
 * mask registers (VARKA-396): {@code VectorLongToMaskNode::Ideal} folds the round trip into a cast
 * that keeps the mask encoding where canonical booleans are needed, and a blend by the result takes
 * its other input on every true lane. A masked body's known-true word is exactly that round trip
 * when its columns have no nulls. These methods never reach {@code VectorLongToMask}: they
 * broadcast the word, AND it with each lane's bit and compare against zero, so the rule has nothing
 * to fold. A byte or short species with more lanes than its element has bits first spreads the
 * word's bytes or shorts across the lanes, through a long broadcast reinterpreted and rearranged.
 *
 * <p>Each takes the arguments {@code fromLong} takes, so an emitter site swaps one static call
 * for the other. The species is a constant at every call site the emitter writes, so the
 * lane-bit and shuffle vectors are loop invariants C2 hoists out of the kernel's loop.
 */
public final class VarkaMaskOps {

  private VarkaMaskOps() {}

  /** {@code word}'s low {@code s.length()} bits as a mask of the long species {@code s}. */
  public static VectorMask<Long> longMask(VectorSpecies<Long> s, long word) {
    LongVector bits = LongVector.broadcast(s, 1L)
        .lanewise(VectorOperators.LSHL, LongVector.zero(s).addIndex(1));
    return LongVector.broadcast(s, word).and(bits).compare(VectorOperators.NE, 0L);
  }

  /** {@code word}'s low {@code s.length()} bits (at most 16) as a mask of the int species. */
  public static VectorMask<Integer> intMask(VectorSpecies<Integer> s, long word) {
    IntVector bits = IntVector.broadcast(s, 1)
        .lanewise(VectorOperators.LSHL, IntVector.zero(s).addIndex(1));
    return IntVector.broadcast(s, (int) word).and(bits).compare(VectorOperators.NE, 0);
  }

  /** {@code word}'s low {@code s.length()} bits as a mask of the short species {@code s}. */
  public static VectorMask<Short> shortMask(VectorSpecies<Short> s, long word) {
    ShortVector lane = ShortVector.zero(s).addIndex(1);
    if (s.length() <= Short.SIZE) {
      ShortVector bits = ShortVector.broadcast(s, (short) 1).lanewise(VectorOperators.LSHL, lane);
      return ShortVector.broadcast(s, (short) word).and(bits).compare(VectorOperators.NE, 0);
    }
    // More lanes than a short has bits: lane j reads short j / 16 of the word, bit j % 16.
    ShortVector chunks = LongVector.broadcast(longSpecies(s.vectorShape()), word)
        .reinterpretAsShorts();
    ShortVector spread = chunks.rearrange(lane.lanewise(VectorOperators.LSHR, 4).toShuffle());
    ShortVector bits = ShortVector.broadcast(s, (short) 1)
        .lanewise(VectorOperators.LSHL, lane.lanewise(VectorOperators.AND, (short) 15));
    return spread.and(bits).compare(VectorOperators.NE, 0);
  }

  /** {@code word}'s low {@code s.length()} bits as a mask of the byte species {@code s}. */
  public static VectorMask<Byte> byteMask(VectorSpecies<Byte> s, long word) {
    ByteVector lane = ByteVector.zero(s).addIndex(1);
    if (s.length() <= Byte.SIZE) {
      ByteVector bits = ByteVector.broadcast(s, (byte) 1).lanewise(VectorOperators.LSHL, lane);
      return ByteVector.broadcast(s, (byte) word).and(bits).compare(VectorOperators.NE, 0);
    }
    // More lanes than a byte has bits: lane j reads byte j / 8 of the word, bit j % 8.
    ByteVector chunks = LongVector.broadcast(longSpecies(s.vectorShape()), word)
        .reinterpretAsBytes();
    ByteVector spread = chunks.rearrange(lane.lanewise(VectorOperators.LSHR, 3).toShuffle());
    ByteVector bits = ByteVector.broadcast(s, (byte) 1)
        .lanewise(VectorOperators.LSHL, lane.lanewise(VectorOperators.AND, (byte) 7));
    return spread.and(bits).compare(VectorOperators.NE, 0);
  }

  /** The long species of {@code shape}, which a narrow species' word is broadcast through. */
  private static VectorSpecies<Long> longSpecies(VectorShape shape) {
    return LongVector.SPECIES_MAX.vectorShape() == shape ? LongVector.SPECIES_MAX
        : VectorSpecies.of(long.class, shape);
  }
}
