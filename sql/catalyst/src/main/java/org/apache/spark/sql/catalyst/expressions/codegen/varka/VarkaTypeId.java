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
 * The Spark types Varka reads or plans to read, and the zoned {@code TIMESTAMP}, which it does
 * not read by decision: one id per type, whatever its forms (every precision of a decimal or a
 * {@code TIME}, every field range of an interval). {@link VarkaTypes#of} maps a Spark type to
 * its id, and {@link VarkaTypes#info} holds the facts about it.
 *
 * <p>The order is the census's ({@code dev/varka_census.py}), which {@code sql/varka/types.tsv}
 * keeps.
 *
 * <p>The set is closed and every switch over it has no {@code default}, so adding a type is a
 * compile error at each place that must decide what the type does there.
 */
public enum VarkaTypeId {
  BOOLEAN,
  BYTE,
  SHORT,
  INT,
  LONG,
  FLOAT,
  DOUBLE,
  DECIMAL,
  DATE,
  TIMESTAMP_NTZ,
  TIME,
  YM_INTERVAL,
  DT_INTERVAL,
  TIMESTAMP
}
