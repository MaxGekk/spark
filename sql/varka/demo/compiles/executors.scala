// Licensed to the Apache Software Foundation (ASF) under one or more contributor license
// agreements. See the NOTICE file distributed with this work for additional information regarding
// copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0. Unless required by
// applicable law or agreed to in writing, software distributed under the License is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the
// License.

// Where a query is compiled: on the driver, and again on every executor that runs its tasks.
//
// The driver compiles each stage while it plans, to read its methods' sizes, and ships the stage
// to the executors as Java source. Every executor JVM compiles that source again, into a cache of
// its own. This script runs on a cluster of two executors and reads each JVM's compile counter
// (CodegenMetrics) after each query: on the driver directly, on the executors through a small job
// that reads it where it runs. A new integer compiles on all three JVMs; a new string on none.
//
//   bin/spark-shell --master 'local-cluster[2,1,1024]' -i executors.scala

import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.CodegenMetrics

/** Each executor's compile counter, from a job that reads it where its tasks run. */
def probe(): Map[String, Long] = sc.parallelize(0 until 64, 64)
  .map(_ => (SparkEnv.get.executorId, CodegenMetrics.METRIC_COMPILATION_TIME.getCount))
  .collect().groupBy(_._1).map { case (id, cs) => (s"executor $id", cs.map(_._2).max) }

/** The compile counter of the driver and of both executors, by executor id. */
def counts(): Map[String, Long] = {
  // The probe compiles nothing, so it can run again until its tasks have reached both executors.
  var executors = probe()
  var tries = 1
  while (executors.size < 2 && tries < 20) {
    executors = executors ++ probe()
    tries += 1
  }
  require(executors.size == 2, s"the probe reached ${executors.keys.mkString(", ")} only")
  executors + ("driver" -> CodegenMetrics.METRIC_COMPILATION_TIME.getCount)
}

// Four partitions, so that each executor holds some of the cached table and scans it, and eight
// partitions after the exchange, so that each executor also runs the final aggregate and has
// compiled its class before the counts start.
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
spark.sql("""CACHE TABLE t AS SELECT CAST(id AS INT) AS i, CAST(id % 100 AS STRING) AS s
  FROM RANGE(0, 100000, 1, 4)""")
spark.sql("SELECT count(*) FROM t WHERE i = 1").collect()
spark.sql("SELECT count(*) FROM t WHERE s = '1'").collect()

val jvms = counts().keys.toSeq.sorted

/** Runs `query` and prints how many classes each JVM compiled for it. */
def run(label: String, query: String, args: Map[String, Any] = Map.empty): Unit = {
  val before = counts()
  spark.sql(query, args).collect()
  val after = counts()
  println(f"$label%-24s " + jvms.map(j => f"${after(j) - before(j)}%12d").mkString)
}

println("### Classes compiled per query, on each JVM of local-cluster[2,1,1024]")
println(f"${"query"}%-24s " + jvms.map(j => f"$j%12s").mkString)
run("int, first", "SELECT count(*) FROM t WHERE i = 5")
run("int, the same again", "SELECT count(*) FROM t WHERE i = 5")
run("int, new value", "SELECT count(*) FROM t WHERE i = 6")
run("string, new value", "SELECT count(*) FROM t WHERE s = '6'")
run("int parameter, new", "SELECT count(*) FROM t WHERE i = :p", Map("p" -> 8))
run("string parameter, new", "SELECT count(*) FROM t WHERE s = :p", Map("p" -> "8"))
println(s"### Spark ${spark.version}, Java ${System.getProperty("java.version")}")
System.exit(0)
