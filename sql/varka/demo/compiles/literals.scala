// Licensed to the Apache Software Foundation (ASF) under one or more contributor license
// agreements. See the NOTICE file distributed with this work for additional information regarding
// copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0. Unless required by
// applicable law or agreed to in writing, software distributed under the License is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the
// License.

// Which new values make Spark compile a query again.
//
// Spark keeps the classes it compiles in a cache keyed by the generated Java source. Some
// literals are written into that source as text - integers, longs, doubles, dates, timestamps -
// and some are passed to the class as objects - strings, decimals. A query that differs from an
// earlier one only in a value of the first kind is a new source, so a new compile. This script
// runs one query, then the same query with a new value of each type, and prints how many classes
// each run compiled, from Spark's own counter (CodegenMetrics), and which ones, from the generated
// source that CodeGenerator logs at DEBUG, with the thread that compiled it: the driver while it
// plans, or a task. A named parameter is bound as a literal, so it behaves
// as the literal of its type.
//
//   bin/spark-shell --master local[1] -i literals.scala

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.{LoggerConfig, Property}
import org.apache.spark.metrics.source.CodegenMetrics

// CodeGenerator logs each source it compiles at DEBUG, just before Janino compiles it. The class
// a source makes is named in its first lines: a stage's iterator, a projection, an ordering.
// The cache is also keyed by the compiling thread's class loader (SPARK-44795), and a task runs
// under the executor's loader, not the driver's, so in local mode a stage the driver compiled is
// compiled again by its first task.
val compiled = scala.collection.mutable.ArrayBuffer.empty[String]
val classOf = """(?s).*?new (\w+)\(.*""".r
val appender = new AbstractAppender("compiled-classes", null, null, true, Property.EMPTY_ARRAY) {
  override def append(event: LogEvent): Unit = event.getMessage.getFormattedMessage match {
    case classOf(name) if event.getLevel == Level.DEBUG =>
      val where = if (event.getThreadName.startsWith("Executor task")) "task" else "driver"
      compiled.synchronized(compiled += s"$name ($where)")
    case _ =>
  }
}
appender.start()
val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext]
val codegenLogger = "org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator"
val loggerConfig = new LoggerConfig(codegenLogger, Level.DEBUG, false)
loggerConfig.addAppender(appender, Level.DEBUG, null)
ctx.getConfiguration.addLogger(codegenLogger, loggerConfig)
ctx.updateLoggers()

spark.sql("""CACHE TABLE t AS SELECT
  CAST(id AS INT) AS i, id AS l, CAST(id AS DOUBLE) AS x, CAST(id AS DECIMAL(10, 2)) AS m,
  DATE_ADD(DATE'2020-01-01', CAST(id % 1000 AS INT)) AS d, TIMESTAMP_SECONDS(id) AS ts,
  CAST(id % 100 AS STRING) AS s
  FROM RANGE(100000)""")

/** Runs `query` and prints the classes it compiled. */
def run(label: String, query: String, args: Map[String, Any] = Map.empty): Unit = {
  compiled.synchronized(compiled.clear())
  val before = CodegenMetrics.METRIC_COMPILATION_TIME.getCount
  spark.sql(query, args).collect()
  val count = CodegenMetrics.METRIC_COMPILATION_TIME.getCount - before
  val names = compiled.synchronized(compiled.toList)
  val params = if (args.isEmpty) "" else args.map { case (k, v) => s":$k = $v" }
    .mkString("   (", ", ", ")")
  println(f"$label%-24s $count%2d   ${names.mkString(", ")}".trim)
  println(s"    $query$params")
}

// The first query of a session also compiles what every query shares; run each shape once first,
// with values the runs below do not use.
Seq("i = 1", "l = 1", "x = 1.5", "m = 1.25", "d = DATE'2020-01-02'",
  "ts = TIMESTAMP'1970-01-01 00:00:01'", "s = '1'").foreach { p =>
  spark.sql(s"SELECT count(*) FROM t WHERE $p").collect()
}
spark.sql("SELECT count(*) FROM t WHERE i = :p", Map("p" -> 1)).collect()
spark.sql("SELECT count(*) FROM t WHERE s = :p", Map("p" -> "1")).collect()

println("### Classes compiled per query: a query, then a new value of each type")
println("query                classes  compiled")
run("int, first", "SELECT count(*) FROM t WHERE i = 5")
run("int, the same again", "SELECT count(*) FROM t WHERE i = 5")
run("int, new value", "SELECT count(*) FROM t WHERE i = 6")
run("bigint, new value", "SELECT count(*) FROM t WHERE l = 6")
run("double, new value", "SELECT count(*) FROM t WHERE x = 6.5")
run("decimal, new value", "SELECT count(*) FROM t WHERE m = 6.25")
run("date, new value", "SELECT count(*) FROM t WHERE d = DATE'2020-01-07'")
run("timestamp, new value", "SELECT count(*) FROM t WHERE ts = TIMESTAMP'1970-01-01 00:00:06'")
run("string, new value", "SELECT count(*) FROM t WHERE s = '6'")
run("int parameter, new", "SELECT count(*) FROM t WHERE i = :p", Map("p" -> 8))
run("int parameter, newer", "SELECT count(*) FROM t WHERE i = :p", Map("p" -> 9))
run("string parameter, new", "SELECT count(*) FROM t WHERE s = :p", Map("p" -> "8"))
run("string parameter, newer", "SELECT count(*) FROM t WHERE s = :p", Map("p" -> "9"))
// A string value compared with an integer column is cast to an integer and folded into an
// integer literal before code generation, so it compiles as one.
run("int column, string value", "SELECT count(*) FROM t WHERE i = :p", Map("p" -> "10"))
println(s"### Spark ${spark.version}, Java ${System.getProperty("java.version")}")
System.exit(0)
