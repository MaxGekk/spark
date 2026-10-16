// Licensed to the Apache Software Foundation (ASF) under one or more contributor license
// agreements. See the NOTICE file distributed with this work for additional information regarding
// copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0. Unless required by
// applicable law or agreed to in writing, software distributed under the License is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the
// License.

// What a generated aggregate does when its distinct keys pass the fast hash map's capacity.
//
// A grouped aggregate whose keys and buffers are primitive (or decimal, string, interval) gets,
// in its partial half, a small generated hash map in front of the regular one. Each row first
// probes that fast map; a key it cannot place falls through to the regular map, in the same
// generated code. The fast map holds 1 << spark.sql.codegen.aggregate.fastHashMap.capacityBit
// keys, 65,536 by default. A key that found room stays in the fast map; a key that did not finds
// none later either, since the map only fills, so all of its rows take the regular map, after a
// failed probe of the fast one.
//
// The query is SELECT k, sum(v) FROM t GROUP BY k over a fixed number of rows, k = id % K so that
// every one of the K keys appears, for K stepping across 65,536, in three arms: Spark's default,
// capacityBit=20 (1,048,576 keys), and the fast map off (map.twolevel.enabled=false). A single
// long key hashes to the key plus a constant in the fast map, so keys 0 to K - 1 take consecutive
// buckets and never collide below 2^17: capacity is the only reason a key falls through here,
// and keys in the fast map are min(K, capacity). That count is derived from the source, not
// observed; Spark has no counter for it. What is observed: whether the stage's generated source
// holds the fast map and its capacity, and the partial aggregate's metrics, which must show no
// spill and no sort fallback. Its peak memory is the regular map's alone (finishAggregate), and
// the regular map takes its first data page, one page in size, only when a key reaches it: a
// peak of a page or more says that at least one key fell through, a fraction of a megabyte that
// none did.
//
// The timed statement is also the answer check: it wraps the aggregate in a count of its groups
// and a sum of hash(k, sum(v)), the same in all three arms; that outer global aggregate over K
// rows is the same work in every arm. Each statement is planned anew, so its metrics are its own.
//
// Times depend on the machine, so the committed output comes from a GitHub-hosted runner, through
// run.sh and .github/workflows/varka-demo.yml. The fast map's page must hold capacity x 40 bytes
// (an 8-byte key and buffer, each an UnsafeRow of 16 bytes, and 8 bytes of record header) for
// capacityBit=20 to mean what it says, so run.sh gives the driver 4g:
//
//   bin/spark-shell --master local[1] --driver-memory 4g -i capacity.scala
//
// CAPACITY_SMOKE=1 in the environment runs a small grid, to check the script end to end; its
// output says so in its first line, and is not a measurement.

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

val smoke = sys.env.contains("CAPACITY_SMOKE")
/**
 * Rows per query: 2^26, about 64 rows a key at a million keys and so many at 65,536 that the
 * fixed costs of a query (planning, the shuffle and final aggregate of K groups, the regular
 * map's first allocation) stay a small part of a run of a second or more.
 */
val rows = if (smoke) 1L << 21 else 1L << 26
val keyCounts =
  if (smoke) Seq(16384, 65536, 65537, 131072)
  else Seq(16384, 32768, 61440, 65536, 65537, 69632, 131072, 262144, 1048576)
val warmups = if (smoke) 1 else 2
val timed = if (smoke) 1 else 5

/** One way to run the aggregate: its label and the settings it changes from Spark's defaults. */
case class Arm(label: String, capacityBit: Option[Int], fastMap: Boolean)

val arms = Seq(
  Arm("default", None, true),
  Arm("capacityBit=20", Some(20), true),
  Arm("fast map off", None, false))

val capacityKey = "spark.sql.codegen.aggregate.fastHashMap.capacityBit"
val twoLevelKey = "spark.sql.codegen.aggregate.map.twolevel.enabled"
val defaultBit = spark.conf.get(capacityKey).toInt

def use(arm: Arm): Unit = {
  arm.capacityBit match {
    case Some(bit) => spark.conf.set(capacityKey, bit.toString)
    case None => spark.conf.unset(capacityKey)
  }
  spark.conf.set(twoLevelKey, arm.fastMap.toString)
}

spark.conf.set("spark.sql.shuffle.partitions", "1")
// With adaptive execution each stage of the plan is a leaf QueryStageExec, and the partial
// aggregate's metrics and generated source are out of reach of a walk over the plan.
spark.conf.set("spark.sql.adaptive.enabled", "false")

def query(keys: Int): String =
  "SELECT count(*), sum(hash(k, s)) FROM (SELECT k, sum(v) AS s FROM " +
    s"(SELECT id % $keys AS k, id AS v FROM RANGE(0, $rows, 1, 1)) GROUP BY k)"

/** What one run gives back: the answer, the time, and the partial aggregate's metrics. */
case class Run(answer: (Long, Long), ns: Long, peakMemory: Long, spill: Long, fallbacks: Long)

def partialAggregate(plan: org.apache.spark.sql.execution.SparkPlan): HashAggregateExec = {
  val found = plan.collect {
    case h: HashAggregateExec
      if h.groupingExpressions.nonEmpty && h.aggregateExpressions.forall(_.mode == Partial) => h
  }
  require(found.size == 1, s"expected one grouped partial aggregate, found ${found.size}")
  found.head
}

def run(keys: Int): Run = {
  val start = System.nanoTime()
  val df = spark.sql(query(keys))
  val row = df.collect().head
  val ns = System.nanoTime() - start
  val metrics = partialAggregate(df.queryExecution.executedPlan).metrics
  Run((row.getLong(0), row.getLong(1)), ns, metrics("peakMemory").value,
    metrics("spillSize").value, metrics("numTasksFallBacked").value)
}

/** The fast map's capacity in the arm's generated stages, or None when no stage has one. */
def fastMapCapacity(keys: Int): Option[Int] = {
  val sources = spark.sql(query(keys)).queryExecution.executedPlan.collect {
    case w: WholeStageCodegenExec => w.doCodeGen()._2.body
  }.filter(_.contains("FastHashMap"))
  require(sources.size <= 1, s"${sources.size} stages hold a fast hash map")
  val capacity = """private int capacity = 1 << (\d+);""".r
  sources.headOption.map { s =>
    1 << capacity.findFirstMatchIn(s).getOrElse(sys.error("no capacity in the fast map")).group(1)
      .toInt
  }
}

def table(): Unit = {
  println(f"${"keys"}%9s ${"arm"}%-15s ${"fast map"}%9s ${"in fast map"}%12s " +
    f"${"best ns/row"}%12s ${"median ns/row"}%14s ${"peak memory"}%12s ${"groups"}%9s " +
    f"${"checksum"}%14s")
  keyCounts.foreach { keys =>
    // The arms take turns, so that drift in the machine falls on all three alike.
    (1 to warmups).foreach(_ => arms.foreach { arm => use(arm); run(keys) })
    val runs = arms.map(_ -> scala.collection.mutable.ArrayBuffer.empty[Run]).toMap
    (1 to timed).foreach(_ => arms.foreach { arm => use(arm); runs(arm) += run(keys) })
    val answers = runs.values.flatten.map(_.answer).toSet
    require(answers.size == 1, s"the arms disagree at $keys keys: $answers")
    require(answers.head._1 == keys, s"$keys keys gave ${answers.head._1} groups")
    arms.foreach { arm =>
      use(arm)
      val capacity = fastMapCapacity(keys)
      require(capacity.isDefined == arm.fastMap, s"${arm.label}: fast map $capacity")
      require(capacity.forall(_ == 1 << arm.capacityBit.getOrElse(defaultBit)),
        s"${arm.label}: capacity $capacity")
      val rs = runs(arm)
      require(rs.forall(r => r.spill == 0 && r.fallbacks == 0),
        s"${arm.label} at $keys keys spilled or fell back to sorting")
      val ns = rs.map(_.ns.toDouble / rows).sorted
      val inFast = capacity.map(c => math.min(keys, c).toString).getOrElse("-")
      println(f"$keys%9d ${arm.label}%-15s ${capacity.map(_.toString).getOrElse("none")}%9s " +
        f"$inFast%12s ${ns.head}%12.2f ${ns(ns.size / 2)}%14.2f " +
        f"${rs.last.peakMemory / 1048576.0}%10.1fMB ${answers.head._1}%9d ${answers.head._2}%14d")
    }
  }
  println()
}

// One statement, so that the end marker is printed only if every part ran: spark-shell -i goes
// on to the next statement after one that fails, and run.sh takes the marker as the sign that the
// script finished.
try {
  val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
  // The fast map's records live in one page; one that cannot hold them all fills before the map.
  require(pageSize >= (1L << 20) * 40,
    s"page size $pageSize is too small for capacityBit=20; give the driver more memory")
  if (smoke) println("### SMOKE RUN: a small grid to check the script, not a measurement")
  println("### The fast hash map's capacity: SELECT k, sum(v) FROM t GROUP BY k, k = id % <keys>")
  println(s"$rows rows in one partition; warm-up and timed runs per arm: $warmups and $timed, " +
    s"the arms taking turns; driver memory ${spark.conf.get("spark.driver.memory", "default")}, " +
    s"page size $pageSize bytes; $capacityKey default $defaultBit")
  println("in fast map: keys the fast map holds, min(keys, capacity), derived from the source; " +
    "peak memory: the partial aggregate's regular map, a page or more once any key reaches it")
  table()
  println(s"### Spark ${spark.version}, Java ${System.getProperty("java.version")}")
  System.exit(0)
} catch {
  case e: Throwable =>
    e.printStackTrace()
    System.exit(1)
}
