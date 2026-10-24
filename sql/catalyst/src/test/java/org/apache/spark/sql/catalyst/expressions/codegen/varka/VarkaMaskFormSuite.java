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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;
import java.util.function.BiFunction;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The word-to-mask form the emitter writes out at each masked-body site, in its Java form ({@code
 * VarkaVectorTemplateWarmup.Rounds.longMask} and its siblings, VARKA-491), sets lane {@code i}
 * exactly where bit {@code i} of the word is, for every lane type at every shape the platform has,
 * over the words that matter (all ones, none, each bit alone, and random words with bits above the
 * lane count), each compared lane by lane with the word's own bits. It runs in a JVM of its own,
 * since a species other than the preferred one would leave the shared Vector API templates profiled
 * for it in the JVM's later suites.
 */
@VarkaSuite(ownJvm = true)
public final class VarkaMaskFormSuite {

  private static <E> void check(String what, VectorSpecies<E> s,
      BiFunction<VectorSpecies<E>, Long, VectorMask<E>> maskOf) {
    Random rnd = new Random(491);
    long[] words = new long[200];
    words[0] = -1L;
    words[1] = 0L;
    for (int i = 2; i < 66; i++) {
      words[i] = 1L << (i - 2);
    }
    for (int i = 66; i < words.length; i++) {
      words[i] = rnd.nextLong();
    }
    for (long word : words) {
      VectorMask<E> m = maskOf.apply(s, word);
      for (int lane = 0; lane < s.length(); lane++) {
        assertEquals((word >>> lane & 1) != 0, m.laneIsSet(lane),
            what + " " + s + " word " + Long.toHexString(word) + " lane " + lane);
      }
    }
  }

  @Test
  @DisplayName("every lane type at every shape sets the lanes the word's bits name")
  void everyLaneTypeAndShapeSetsTheWordsBits() {
    for (VectorSpecies<Long> s : new VectorSpecies[] {LongVector.SPECIES_64,
        LongVector.SPECIES_128, LongVector.SPECIES_256, LongVector.SPECIES_512}) {
      check("long", s, VarkaVectorTemplateWarmup.Rounds::longMask);
    }
    for (VectorSpecies<Integer> s : new VectorSpecies[] {IntVector.SPECIES_64,
        IntVector.SPECIES_128, IntVector.SPECIES_256, IntVector.SPECIES_512}) {
      check("int", s, VarkaVectorTemplateWarmup.Rounds::intMask);
    }
    for (VectorSpecies<Short> s : new VectorSpecies[] {ShortVector.SPECIES_64,
        ShortVector.SPECIES_128, ShortVector.SPECIES_256, ShortVector.SPECIES_512}) {
      if (s.length() <= Short.SIZE) {
        check("short", s, VarkaVectorTemplateWarmup.Rounds::shortMask);
      }
      check("short spread", s, VarkaVectorTemplateWarmup.Rounds::shortMaskSpread);
    }
    for (VectorSpecies<Byte> s : new VectorSpecies[] {ByteVector.SPECIES_64,
        ByteVector.SPECIES_128, ByteVector.SPECIES_256, ByteVector.SPECIES_512}) {
      if (s.length() <= Byte.SIZE) {
        check("byte", s, VarkaVectorTemplateWarmup.Rounds::byteMask);
      }
      check("byte spread", s, VarkaVectorTemplateWarmup.Rounds::byteMaskSpread);
    }
  }
}
