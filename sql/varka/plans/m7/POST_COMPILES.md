# How Spark compiles a query, and why it compiles it again

A dashboard runs the same short query every time someone moves a filter, yet each new date
makes it slower than a rerun with the same date, every time. On a cloud runner, a count over
100,000 cached rows took about 19 ms with the same integer each time and about 40 ms with a new
one. Spark compiled the query again, because the new value is written into the query's code.
Everything here but one experiment was measured on stock Spark 4.2.0; how is at the end.

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

Spark turns each stage of the physical plan into one Java source string. Janino, the in-memory
Java compiler Spark embeds, compiles it into a class, which Spark caches with the source text as
the key. So "does this query compile again?" is the question "is its source text new?".

![Two paths for a value](figures/svg/fig38-two-paths-for-a-value.svg)

*Figure 1. How a value in a `WHERE` clause reaches the generated class. The class counts are
Spark's own counter (`CodegenMetrics`) in local mode, from `literals-jdk25-output.txt`.*

How a value gets into the class depends on its type. An integer, long, float, double, date or
timestamp is written into the source as text: `filter_value_0 = inputadapter_value_0 == 6;`.
So `WHERE i = 6` after `WHERE i = 5` is a new source, a new key, and three new classes. A
string, decimal or binary value goes into the class's `references` array, and the source only
reads it from there. So `WHERE s = '6'` after `WHERE s = '5'` is the same source and a cache hit.

A named parameter is no way out: Spark substitutes `:p` with a literal before it plans, so an
integer parameter is an integer literal by the time code is generated.

## Where it compiles

![Where one query is compiled](figures/svg/fig36-where-one-query-is-compiled.svg)

*Figure 2. One query with a new integer on a driver and two executors, from
`executors-jdk25-output.txt`: one compile on the driver, two on each executor.*

The driver compiles the stage first, to check that it compiles and to read the size of every
method. That is where the 64 KB fallback and the 8000-byte check of our earlier posts are
decided. Then it ships the source, not the class, to the executors, and every executor compiles
it again into a cache of its own. Each executor also compiles the cached table's batch filter,
which holds the value too.

Even one JVM compiles twice. The cache is keyed by the source and by the class loader of the
thread that compiles ([SPARK-44795](https://issues.apache.org/jira/browse/SPARK-44795)), and a
task runs under the executor's class loader, not the driver's. So in local mode the stage the
driver compiled is compiled again by its first task: with the batch filter, that is the three
classes of Figure 1. And a new class starts in the JVM's bytecode interpreter, so each compile is
paid twice: once in Janino, and again while the class runs slowly.

## What a click costs

![What a click costs](figures/svg/fig39-what-a-click-costs.svg)

*Figure 3. The same short query, 40 times per row, split into Janino's time and the rest. Upper
block: stock Spark 4.2.0, AMD EPYC 9V45, from `times-jdk25-output.txt`. Lower block: the
experiment of "What Spark could do", this repository's Spark on an AMD EPYC 7763, from
`CodegenRecompileBenchmark-jdk25-results.txt`. The two blocks are not comparable.*

A new integer made the query about twice as slow: about 40 ms against about 19. Janino took
about 11 ms of the difference. The rest, about 10 ms, is time outside Janino that the repeated
query does not pay, such as loading the three new classes and running them cold. Dates and
integer parameters behave the same. A new string compiles nothing; its query is slower than the
integer one because it compares a different column. "When Spark stops compiling your query"
measured where the cold run hurts most: wide stages that took seconds to reach their steady
speed.

On fifty executors over a cached table, one click would be a hundred and one compiles: one on
the driver and two on each executor.

## Bigger stages cost more

![Bigger stages cost more to compile](figures/svg/fig40-bigger-stages-cost-more.svg)

*Figure 4. Janino's time for one compile of a new source, best of nine, against the source's
length on a log scale; stock Spark 4.2.0 on Figure 3's upper runner, from
`times-jdk25-output.txt`.*

Janino's time grows with the source: from about 3 ms for a one-entry projection to about 31 ms
for an aggregate of 60 sums. A 60-sum aggregate with a new literal pays those 31 ms on the
driver and again on each executor.

## Past the cache's 100 entries

Each JVM's cache holds `spark.sql.codegen.cache.maxEntries` classes, 100 by default. To see
what happens past it, `cache.scala` runs a loop of n queries that differ only in an integer,
then the same loop again, and counts how many of the second loop's queries compiled.

![What the class cache keeps](figures/svg/fig37-what-the-class-cache-keeps.svg)

*Figure 5. The second loop's queries that compiled again, under the default cache and under a
cache of 1000, from `cache-100-jdk25-output.txt` and `cache-1000-jdk25-output.txt`.*

In local mode each query compiles three classes. Up to 30 queries, 90 classes, the second
loop compiled nothing. At 33 queries, 99 classes, 31 of 33 compiled again. The cache is
Guava's, which evicts within each quarter of it, so it starts evicting a little before it holds
100. From 40 queries on, every query compiled again.

The cache doesn't degrade gently: a loop larger than the cache evicts each class just before
it is needed again, so all 150 queries of the largest loop compiled twice. With
`spark.sql.codegen.cache.maxEntries=1000` the second loop compiled nothing at any size up to
150.

## What to do today

- **Compare strings where the column is a string.** A new string value compiles nothing. A
  string compared with an integer column doesn't help: it is folded into an integer literal.
- **Raise the cache where the driver and executors have the memory.** It is a static setting, so
  it goes on the command line or in `spark-defaults.conf`:
  `--conf spark.sql.codegen.cache.maxEntries=1000`. It doesn't stop the first compile of each
  new value; it stops the second.
- **Have fewer distinct stages.** A filter the dashboard doesn't need in the query, applied
  after it, is one fewer source to vary. Not measured here.

## What Spark could do

Strings already show the way: pass the value to the class, and every value of a query shares
one class. To see whether that would pay for integers, a benchmark has a stand-in expression
that passes the integer to the class and reads it from a field. A stand-in is test code, so this
runs on this repository's Spark, not on 4.2.0: compare it with itself, not with the numbers
above. It runs `SELECT count(*) FROM t WHERE i % 1000000 = <value>` both ways, after checking
that both count the same rows; the lower block of Figure 3 is the result. With a new value each
time, the literal compiled 2 classes a query and took about 43 ms; passed to the class, it
compiled none and took about 20 ms, the same as repeating one literal. Once warm it costs
nothing measurable: a scan of 20 million cached rows took about 12 ns a row either way, one run
each. On these numbers the change would pay.

## Seeing it on your own cluster

Each compile logs `Code generated in ... ms` at INFO from `CodeGenerator`, on the driver and in
each executor's log. Every JVM's metrics system also counts compiles in the histogram
`CodeGenerator.compilationTime`. A dashboard whose executor logs show that line on every click
is compiling on every click.

## How these were measured

Everything but the experiment ran on the stock Spark 4.2.0 distribution with JDK 25, through
`bin/spark-shell -i`, from the scripts in
[`sql/varka/demo/compiles/`](https://github.com/vecbricks/varka/tree/master/sql/varka/demo/compiles),
with their outputs committed beside them:

- **The counts** (Figures 1, 2 and 5) come from `literals.scala`, `executors.scala` and
  `cache.scala`. Counts do not depend on the machine.
- **The times of a query and of a compile** (Figure 3's upper block, Figure 4, and every time
  quoted outside "What Spark could do") come from `times.scala`, run on a GitHub-hosted runner
  with an AMD EPYC 9V45, committed as `times-jdk25-output.txt`.
- **The experiment** of "What Spark could do" (Figure 3's lower block) needs a test-only
  expression, so it ran on this repository's Spark, master-based with Varka off, as
  `CodegenRecompileBenchmark`, on a GitHub-hosted runner with an AMD EPYC 7763, committed as
  `sql/core/benchmarks/CodegenRecompileBenchmark-jdk25-results.txt`.

Times move from one machine to the next. The same `times.scala` run on another runner, an AMD
EPYC 9V74, measured about 15 ms of Janino per new value instead of about 11 (committed as
`times-attempt1-jdk25-output.txt`). That is why the post quotes one run and names its CPU. The
figures are drawn from these files by the scripts beside them.

This post follows [The 8000-byte cliff in Spark
SQL](https://vecbricks.github.io/the-8000-byte-cliff/), [When Spark stops compiling your
query](https://vecbricks.github.io/when-spark-stops-compiling-your-query/) and [A large CASE
WHEN, split only where it has to be](https://vecbricks.github.io/split-only-where-it-has-to-be/).
If you want to see how a generator that writes bytecode directly avoids the second compile
altogether, read the companion post, [The class got cheaper. The first query got
slower.](https://vecbricks.github.io/the-class-got-cheaper/)
