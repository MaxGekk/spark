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
 * {@link TimeChains} through the surface driver over the {@code varka_times} table: the
 * {@code TIME} twin of {@link DateChainBenchmark}, and the benchmark that carries the long
 * lane's full-width number, which the {@code TIME} surface cannot because its entries are
 * bound by memory rather than by arithmetic. Results land in
 * {@code sql/varka/bench/benchmarks/TimeChain-<label>-results.txt}; the driver's arguments,
 * guards and provenance block are {@link DateSurfaceBenchmark}'s.
 */
public final class TimeChainBenchmark {
  public static void main(String[] argv) throws IOException {
    DateSurfaceBenchmark.run(argv, TimeChains.ENTRIES, "timechains", "VarkaTimeChain",
        DateSurfaceBenchmark.TableShape.TIMES);
  }

  private TimeChainBenchmark() {}
}
