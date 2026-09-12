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

package org.apache.spark.sql.varka.bench;

import java.io.IOException;

/**
 * {@link Chains} through {@link DateSurfaceBenchmark}'s driver: the same table, the same
 * harness, the same guards, a different list of expressions and its own results files.
 *
 * <pre>
 *   spark-submit --master local[1] --driver-memory 8g --class ...DateChainBenchmark \
 *     varka-bench.jar --label varka-jdk25 --rows 100000000 --out FILE
 * </pre>
 *
 * <p><b>What this measures that the surface does not.</b> The surface times one expression per
 * entry, and its lightest entries are bound by memory bandwidth rather than by arithmetic -
 * {@code date_add(d, 3)} moves eight bytes per row and does one add, and on the development
 * laptop it runs at 15.2 GB/s, which is that machine's single-core DRAM speed. Widening the
 * vector datapath cannot make such a kernel faster, so a 512-bit machine will show nothing on
 * those rows however genuine its datapath. {@link Chains} composes the same operations three
 * and four deep, which raises the arithmetic per byte by an order of magnitude and puts the
 * kernel where the datapath width is what it is waiting on.
 *
 * <p><b>Why it is a separate class and separate files rather than a section of the surface.</b>
 * The surface is a coverage document - one entry per expression, in the spelling a reader
 * would write - and the ratios a reader wants from it are Varka against stock Spark, which
 * these chains do not improve on. Folding two questions into one file would leave neither
 * answerable on its own, and the project's habit is that a new question gets its own
 * benchmark class and its own committed results files.
 */
public final class DateChainBenchmark {

  public static void main(String[] argv) throws IOException {
    DateSurfaceBenchmark.run(argv, Chains.ENTRIES, "chains", "VarkaDateChain");
  }

  private DateChainBenchmark() {}
}
