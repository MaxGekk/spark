# How Spark compiles a query, and why it compiles it again

A dashboard runs the same query every time someone moves a filter. The query is short: a count
over a cached table. Yet each new date makes it slower than a rerun with the same date, every
time. On a cloud runner, a count over 100,000 cached rows took about 25 ms with the same integer
each time and about 41 ms with a new one. Nothing is wrong with the data. Spark compiled the
query again, because the new value is written into the query's code.

This post is about when Spark compiles your query: how often, where, and which queries it
compiles again. Everything here was measured on stock Spark; how is at the end.

> **In short**
>
> 1. Spark turns each stage of a query into Java source, compiles it with Janino, and caches the
>    class by that source. Integers, longs, doubles, dates and timestamps are written into the
>    source, so a new value is a new class. Strings and decimals are passed to the class instead,
>    so a new one is not.
> 2. A named parameter doesn't help: Spark binds `:p` as a literal before it plans.
> 3. One new integer cost five compiles on a driver and two executors. In local mode the default
>    cache held about thirty such queries; a loop of more than that compiled every query again.

## Why the same query compiles again

Spark's whole-stage code generation turns each stage of the physical plan into one Java source
string. Janino, the in-memory Java compiler Spark embeds, compiles that string into a class, and
Spark caches the class with the source text as the key. So the question "does this query
compile again?" is the question "is its source text new?".

![Two paths for a value](figures/svg/fig38-two-paths-for-a-value.svg)

*Figure 1. How a value in a `WHERE` clause reaches the generated class. The class counts are
Spark's own counter (`CodegenMetrics`) in local mode, from `literals-jdk25-output.txt`.*

How a value gets into the class depends on its type. An integer, long, float, double, date or
timestamp is written into the source as text: `filter_value_0 = inputadapter_value_0 == 6;`.
So `WHERE i = 6` after `WHERE i = 5` is a new source, a new key, and three new classes. A
string, decimal or binary value goes into the class's `references` array, and the source only
reads it from there. So `WHERE s = '6'` after `WHERE s = '5'` is the same source and a cache hit.

A named parameter is no way out. Spark substitutes `:p` with a literal before it plans, so an
integer parameter is an integer literal by the time code is generated. The same happens to a
string compared with an integer column: it is cast and folded into an integer literal, and
compiles three classes like any new integer.

## Where it compiles

![Where one query is compiled](figures/svg/fig36-where-one-query-is-compiled.svg)

*Figure 2. One query with a new integer on a driver and two executors, from
`executors-jdk25-output.txt`: one compile on the driver, two on each executor.*

The driver compiles the stage first, to check that it compiles and to read the size of every
method. That is where the 64 KB fallback and the 8000-byte check of our earlier posts are
decided. Then it ships the stage to the executors, and what it ships is the source, not the
class. Every executor compiles the source again into a cache of its own. Each executor also
compiles the cached table's batch filter, which holds the value too.

Even one JVM compiles twice. The cache is keyed by the source and by the class loader of the
thread that compiles ([SPARK-44795](https://issues.apache.org/jira/browse/SPARK-44795)), and a
task runs under the executor's class loader, not the driver's. So in local mode, where driver
and executor share a JVM, the stage the driver compiled is compiled again by its first task.
That is the three classes of Figure 1: the stage on the driver, the stage in the task, and the
batch filter.

A new class then starts in the JVM's bytecode interpreter, and only later reaches C1's and C2's
machine code. So each compile is paid twice: once in Janino, and again in the time the new
class runs slowly.

## What a click costs

![What a click costs](figures/svg/fig39-what-a-click-costs.svg)

*Figure 3. The same short query, 40 times per row, split into Janino's time and the rest, from
`CodegenRecompileBenchmark-jdk25-results.txt`. The lower rows are the experiment of "What Spark
could do" below.*

A new integer made the query about two thirds slower, and almost all of the difference is
Janino: about 16 ms of the 41. Dates and integer parameters behave the same; a new string costs
nothing. For a query this small, the new class's time in the interpreter hardly shows. "When
Spark stops compiling your query" measured where it does: wide stages that took seconds to
reach their steady speed.

Every executor that runs a task of the stage pays its own compile. On fifty executors over a
cached table, one click would be a hundred and one compiles: one on the driver and two on each
executor.

## Bigger stages cost more

![Bigger stages cost more to compile](figures/svg/fig40-bigger-stages-cost-more.svg)

*Figure 4. Janino's best time for one compile of a source new to the cache, best of nine,
against the length of that source, from `CodegenRecompileBenchmark-jdk25-results.txt`.*

Janino's time grows with the source: from about 4 ms for a one-entry projection to about 18 ms
for an aggregate of 60 sums. A 60-sum aggregate with a new literal pays those 18 ms on the
driver and again on each executor.

## Past the cache's 100 entries

Each JVM's cache holds `spark.sql.codegen.cache.maxEntries` classes, 100 by default. It is an
internal setting, read once when the JVM starts. To see what happens past it, `cache.scala`
runs a loop of n queries that differ only in an integer, then the same loop again, and counts
how many of the second loop's queries compiled.

![What the class cache keeps](figures/svg/fig37-what-the-class-cache-keeps.svg)

*Figure 5. The second loop's queries that compiled again, under the default cache and under a
cache of 1000, from `cache-100-jdk25-output.txt` and `cache-1000-jdk25-output.txt`.*

In local mode each of these queries compiles three classes. Up to 30 queries, 90 classes, the
second loop compiled nothing. At 33 queries, 99 classes, 31 of 33 compiled again. The cache is
Guava's, which evicts within each quarter of it, so it starts evicting a little before it holds
100. From 40 queries on, every query compiled again.

The cache doesn't degrade gently. A loop larger than the cache evicts each class just before it
is needed again, so all 150 queries of the largest loop compiled twice, 450 classes each time.
With `spark.sql.codegen.cache.maxEntries=1000` the second loop compiled nothing at any size up to
150.

## What to do today

- **Compare strings where the column is a string.** A new string value compiles nothing. This
  helps only where the column already is a string: a string compared with an integer column is
  folded into an integer literal and compiled as one.
- **Raise the cache where the driver and executors have the memory.** It is a static setting, so
  it goes on the command line or in `spark-defaults.conf`:
  `--conf spark.sql.codegen.cache.maxEntries=1000`. In the run above it kept 150 queries' 450
  classes. It doesn't stop the first compile of each new value; it stops the second.
- **Have fewer distinct stages.** A filter the dashboard doesn't need in the query, applied
  after it, is one fewer source to vary. Not measured here.

## What Spark could do

Strings already show the way: pass the value to the class and read it from there, and every
value of a query shares one class. To see whether that would pay for integers, the benchmark has
a stand-in expression that passes the integer to the class and reads it from a field. It runs
`SELECT count(*) FROM t WHERE i % 1000000 = <value>` both ways, after checking that both count
the same rows. The lower rows of Figure 3 are the result. With a new value each time, the
literal compiled 2 classes a query and took about 43 ms; passed to the class, it compiled none
and took about 20 ms, the same as repeating one literal. Once warm it costs nothing measurable:
a scan of 20 million cached rows took 12.4 ns a row with the literal and 12.1 with the field, one
run each. On these numbers the change would pay, and Spark already does it for strings.

## Seeing it on your own cluster

Each compile logs `Code generated in ... ms` at INFO from `CodeGenerator`, on the driver and in
each executor's log. Every JVM's metrics system also counts compiles in the histogram
`CodeGenerator.compilationTime`. A dashboard whose executor logs show that line on every click
is compiling on every click.

## How these were measured

The counts come from three scripts in
[`sql/varka/demo/compiles/`](https://github.com/vecbricks/varka/tree/master/sql/varka/demo/compiles),
run on the stock Spark 4.2.0 distribution with JDK 25 through `bin/spark-shell -i`, with their
outputs committed beside them. The times come from `CodegenRecompileBenchmark` on a GitHub-hosted
runner (AMD EPYC 7763, JDK 25), committed as
`sql/core/benchmarks/CodegenRecompileBenchmark-jdk25-results.txt`. [The timings ran on this
repository's Spark, master-based, with Varka off, not on the 4.2.0 distribution; whether that
build stands in for 4.2.0 is the owner's decision before publication.] The figures are drawn
from those files by the scripts beside them.

This post follows [The 8000-byte cliff in Spark
SQL](https://vecbricks.github.io/the-8000-byte-cliff/), [When Spark stops compiling your
query](https://vecbricks.github.io/when-spark-stops-compiling-your-query/) and [A large CASE
WHEN, split only where it has to be](https://vecbricks.github.io/split-only-where-it-has-to-be/),
which were about the code a stage compiles to. If you want to see how a generator that writes
bytecode directly avoids the second compile altogether, the companion post is [link to row
259's post when it is published].
