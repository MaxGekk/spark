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

package org.apache.spark.sql.execution;

import java.util.Optional;

/**
 * How the Spark-level differential compares again a check in which a query raised (VARKA-381).
 * The fuzzer's view has two partitions, and when rows in both raise, whichever task fails first
 * decides what the query raises, on either engine and from one run to the next
 * ({@code VARKA-298.md} 9.3). So such a check runs again where no task races another.
 */
public enum VarkaErrorRerun {
  /**
   * Over the same rows as one partition, where a query raises the first raising row's condition
   * in the rows' order: the default, at one check more per check that raised.
   */
  ONE_PARTITION,
  /**
   * Over the rows as one partition and then over every row alone, which also compares the values
   * of the rows that do not raise and the condition each row raises, at a check per row more.
   */
  EACH_ROW;

  /** The property that selects the arm: {@code onePartition} (the default) or {@code eachRow}. */
  public static final String PROPERTY = "varka.sparkfuzz.errorRerun";

  /** The arm a value of {@link #PROPERTY} names; null is the default. */
  public static VarkaErrorRerun fromProperty(String value) {
    if (value == null || value.equals("onePartition")) {
      return ONE_PARTITION;
    }
    if (value.equals("eachRow")) {
      return EACH_ROW;
    }
    throw new IllegalArgumentException(
        "-D" + PROPERTY + " is onePartition or eachRow, not " + value);
  }

  /** The text a reproducer's note carries when it was found under this arm, or empty. */
  public String noteSuffix() {
    return this == EACH_ROW ? ", -D" + PROPERTY + "=eachRow" : "";
  }

  /** The arm a reproducer's note says it was found under. */
  public static VarkaErrorRerun ofNote(String note) {
    return note.contains("-D" + PROPERTY + "=eachRow") ? EACH_ROW : ONE_PARTITION;
  }

  /**
   * What is left of {@code kind}, found over two partitions, once a condition a race may have
   * chosen is taken out, or empty when nothing is. A kind that compares two conditions keeps
   * nothing; one that names one side's raise keeps which side raised and drops the condition. An
   * unexpected condition is kept whole: whichever task raised it, some row raises a condition the
   * expected-errors list does not allow. Over the same partitions, which side raises does not
   * depend on which task fails first, only which condition it names does; so a kind left here
   * that the rerun does not show is a difference the two partitions' batches made. The batches
   * decide more than Varka's kernels: the cached scan skips a batch whose statistics fail a
   * conjunct, so a row that raises in a batch of six may never be evaluated in a batch of its own
   * ({@code VARKA-381.md} 9).
   */
  public static Optional<String> withoutConditions(String kind) {
    if (kind.contains("error classes differ: ")) {
      return Optional.empty();
    }
    int at = kind.indexOf(" throws: ");
    return Optional.of(at < 0 ? kind : kind.substring(0, at + " throws".length()));
  }
}
