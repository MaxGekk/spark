// Licensed to the Apache Software Foundation (ASF) under one or more contributor license
// agreements. See the NOTICE file distributed with this work for additional information regarding
// copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0. Unless required by
// applicable law or agreed to in writing, software distributed under the License is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the
// License.

// What compiling a query again costs on stock Spark, in time.
//
// The other scripts here count the classes Spark compiles; this one times them. It has two parts,
// the stock-Spark sections of CodegenRecompileBenchmark (sql/core/benchmarks), with the same
// queries, shapes and table formats, so that the two tables can be compared line by line:
//
// 1. a short query over a small cached table, with the same integer each time, or a new integer,
//    date, string or integer parameter each time: over 40 queries, the classes compiled per
//    query, the wall time per query, and Janino's part of it. The benchmark's harness first times
//    each arm for two seconds after two of warm-up; here the arm runs as long, untimed;
// 2. Janino's time for one compile of a whole stage against the stage's size: projections of
//    1 to 99 entries `id + k` and aggregates of 10 to 60 sums by a key, each compile of a source
//    new to the cache, best and median of nine.
//
// Janino's time is CodeGenerator.compileTime: the nanoseconds of every compile that missed the
// cache, from the same clock that logs "Code generated in ... ms" and feeds the histogram
// CodegenMetrics.METRIC_COMPILATION_TIME. The histogram is not read for it: it keeps whole
// milliseconds, and a sample of them, not their sum.
//
// Times depend on the machine, so run.sh runs this script only when asked, and the committed
// output comes from a GitHub-hosted runner, through .github/workflows/varka-demo.yml:
//
//   bin/spark-shell --master local[1] -i times.scala

import org.apache.spark.metrics.source.CodegenMetrics
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec

spark.conf.set("spark.sql.shuffle.partitions", "1")

/** Rows of the table; the values the counter gives are its row ids. */
val rows = 100000
/** The value of the arm that repeats one value; the counter starts above it. */
val same = 5
/** Queries per arm in the table of means. */
val queries = 40
/** Seconds each arm runs before the table: the benchmark harness's warm-up and timed run. */
val warmupSeconds = 4
/** Compiles per shape in the table of compile times. */
val compilesPerShape = 9

spark.sql("CACHE TABLE t AS SELECT CAST(id AS INT) AS i, " +
  "DATE_ADD(DATE'2000-01-01', CAST(id AS INT)) AS d, CAST(id AS STRING) AS s " +
  s"FROM RANGE(0, $rows, 1, 1)")

var next = 1000

/** A value no earlier query has used: a row id of the table. */
def fresh(): Int = {
  require(next < rows - 1, "the counter ran past the table's row ids")
  next += 1
  next - 1
}

/** One way to run the short query: its label, the query for a value, and whether it is new. */
case class Arm(label: String, query: Int => Unit, freshEach: Boolean) {
  def run(): Unit = query(if (freshEach) fresh() else same)
}

def collect(sql: String, args: Map[String, Any] = Map.empty): Unit = spark.sql(sql, args).collect()

def date(v: Int): String = java.time.LocalDate.of(2000, 1, 1).plusDays(v).toString

val arms = Seq(
  Arm("int, the same value each time", v => collect(s"SELECT count(*) FROM t WHERE i = $v"), false),
  Arm("int, a new value each time", v => collect(s"SELECT count(*) FROM t WHERE i = $v"), true),
  Arm("date, a new value each time",
    v => collect(s"SELECT count(*) FROM t WHERE d = DATE'${date(v)}'"), true),
  Arm("string, a new value each time",
    v => collect(s"SELECT count(*) FROM t WHERE s = '$v'"), true),
  Arm("int parameter, a new value each time",
    v => collect("SELECT count(*) FROM t WHERE i = :p", Map("p" -> v)), true))

/**
 * Each arm for `warmupSeconds`, in order, as the benchmark's timed table runs them before its
 * table of means; then, for each arm, one query unrecorded (the arms before it evicted the class
 * of an arm that repeats a value) and `queries` queries timed.
 */
def means(): Unit = {
  arms.foreach { arm =>
    val deadline = System.nanoTime() + warmupSeconds * 1000000000L
    while (System.nanoTime() < deadline) arm.run()
  }
  println(f"${"mean of " + queries + " queries"}%-40s ${"classes / query"}%16s " +
    f"${"ms / query"}%16s ${"Janino ms / query"}%18s")
  arms.foreach { arm =>
    arm.run()
    // As the benchmark harness does before each case.
    System.gc()
    val classes = CodegenMetrics.METRIC_COMPILATION_TIME.getCount
    val janino = CodeGenerator.compileTime
    val start = System.nanoTime()
    (1 to queries).foreach(_ => arm.run())
    val ms = (System.nanoTime() - start) / 1e6 / queries
    val perQuery = (CodegenMetrics.METRIC_COMPILATION_TIME.getCount - classes).toDouble / queries
    val janinoMs = (CodeGenerator.compileTime - janino) / 1e6 / queries
    println(f"${arm.label}%-40s $perQuery%16.1f $ms%16.2f $janinoMs%18.2f")
  }
  println()
}

/** A shape of growing size whose literals come from an offset, so each offset is a new source. */
case class Shape(label: String, sql: Int => String)

val shapes =
  Seq(1, 10, 25, 50, 99).map { n =>
    Shape(s"projection of $n entries id + k", o => "SELECT " +
      (1 to n).map(k => s"id + ${k + o} AS c$k").mkString(", ") + " FROM RANGE(10)")
  } ++ Seq(10, 20, 40, 60).map { n =>
    Shape(s"aggregate of $n sums by a key", o => "SELECT id % 10 AS g, " +
      (1 to n).map(k => s"sum(id + ${k + o}) AS s$k").mkString(", ") +
      " FROM RANGE(10) GROUP BY g")
  }

/** The generated source of the largest whole-stage codegen stage of a shape's plan. */
def source(shape: Shape, offset: Int): CodeAndComment = {
  val sources = spark.sql(shape.sql(offset)).queryExecution.executedPlan.collect {
    case w: WholeStageCodegenExec => w.doCodeGen()._2
  }
  require(sources.nonEmpty, s"no whole-stage codegen stage in ${shape.label}")
  sources.maxBy(_.body.length)
}

/**
 * Compiles each shape's largest stage `compilesPerShape` times, each from a source new to the
 * cache, and prints the source's length, its largest method's and the best and median compile.
 * The sources are generated before the clock starts; only the compile is timed.
 */
def compileTimes(): Unit = {
  // Adaptive execution would plan the stages of a plan with an exchange only when it runs.
  spark.conf.set("spark.sql.adaptive.enabled", "false")
  // Janino's own code is cold in a new JVM; a dashboard's server has compiled many classes.
  (0 until 50).foreach(w => CodeGenerator.compile(source(shapes(1), 1000000 + w * 1000)))
  println(f"${"shape, each compile a new source"}%-36s ${"source chars"}%12s " +
    f"${"largest method"}%16s ${"best ms"}%12s ${"median ms"}%12s")
  var offset = 2000000
  shapes.foreach { shape =>
    val sources = (1 to compilesPerShape).map { _ =>
      offset += 10000
      source(shape, offset)
    }
    var maxMethod = 0
    val ms = sources.map { s =>
      val start = System.nanoTime()
      maxMethod = CodeGenerator.compile(s)._2.maxMethodCodeSize
      (System.nanoTime() - start) / 1e6
    }.sorted
    println(f"${shape.label}%-36s ${sources.head.body.length}%12d $maxMethod%10d bytes " +
      f"${ms.head}%12.2f ${ms(compilesPerShape / 2)}%12.2f")
  }
  println()
}

// One statement, so that the end marker is printed only if every part ran: spark-shell -i goes
// on to the next statement after one that fails, and run.sh takes the marker as the sign that the
// script finished.
try {
  // Spark's own classes compile on a session's first queries; each shape runs once first.
  arms.foreach(_.query(1))
  println("### A short query over a small cached table: the same value each time, or a new one")
  println(s"SELECT count(*) FROM t WHERE <column> = <value>, $rows rows in one partition; " +
    s"each arm ${warmupSeconds} s first")
  means()
  println("### Janino's compile time against a stage's size")
  compileTimes()
  println(s"### Spark ${spark.version}, Java ${System.getProperty("java.version")}")
  System.exit(0)
} catch {
  case e: Throwable =>
    e.printStackTrace()
    System.exit(1)
}
