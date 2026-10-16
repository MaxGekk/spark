// Licensed to the Apache Software Foundation (ASF) under one or more contributor license
// agreements. See the NOTICE file distributed with this work for additional information regarding
// copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0. Unless required by
// applicable law or agreed to in writing, software distributed under the License is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the
// License.

// What Spark's class cache keeps when a session runs more distinct queries than it holds.
//
// Each JVM keeps the classes it compiled in a cache of spark.sql.codegen.cache.maxEntries entries,
// 100 by default, a static setting read when the JVM starts. This script runs a loop of n queries
// that differ only in an integer, then the same loop again, and counts how many queries of the
// second loop compiled anything: with every class still cached, none would. The first loop's
// classes are the newest in the cache when the second loop starts, so only the cache's size
// decides. Run it again with a larger cache to see the setting work:
//
//   bin/spark-shell --master local[1] -i cache.scala
//   bin/spark-shell --master local[1] --conf spark.sql.codegen.cache.maxEntries=1000 -i cache.scala

import org.apache.spark.metrics.source.CodegenMetrics

spark.sql("CACHE TABLE t AS SELECT CAST(id AS INT) AS i FROM RANGE(100000)")
spark.sql("SELECT count(*) FROM t WHERE i = -1").collect()

/** Runs `query` and returns how many classes it compiled. */
def compiles(query: String): Long = {
  val before = CodegenMetrics.METRIC_COMPILATION_TIME.getCount
  spark.sql(query).collect()
  CodegenMetrics.METRIC_COMPILATION_TIME.getCount - before
}

/** The first value of the next loop, so that no loop repeats an earlier loop's queries. */
var next = 0

/** Runs n distinct queries twice; prints each loop's compiles and the second's queries that did. */
def loops(n: Int): Unit = {
  val values = next until next + n
  next += n
  val first = values.map(v => compiles(s"SELECT count(*) FROM t WHERE i = $v"))
  val second = values.map(v => compiles(s"SELECT count(*) FROM t WHERE i = $v"))
  println(f"$n%9d ${first.sum}%17d ${second.sum}%18d ${second.count(_ > 0)}%14d of $n")
}

val maxEntries = spark.conf.get("spark.sql.codegen.cache.maxEntries")
println(s"### n distinct queries, then the same n again, spark.sql.codegen.cache.maxEntries = " +
  maxEntries)
println("  queries  classes, 1st loop  classes, 2nd loop  2nd loop's queries that compiled")
Seq(10, 20, 25, 30, 33, 40, 50, 100, 150).foreach(loops)
println(s"### Spark ${spark.version}, Java ${System.getProperty("java.version")}")
System.exit(0)
