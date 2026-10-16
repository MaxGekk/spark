# How Spark compiles a query, and why it compiles it again

*Draft (row 258, `VARKA-258.md`). Every count below is from `sql/varka/demo/compiles/` on stock
Spark 4.2.0 with JDK 25. The times are owed by `CodegenRecompileBenchmark` on a GitHub-hosted
runner and are in brackets until its results file is committed.*

---

A dashboard runs the same query every time someone moves a filter. The query is short: a count
over a cached table. Yet the first run with each new date is slower than the rerun with the same
date, every time. [Measured: the short query's ms with the same value and with a new one, from
`CodegenRecompileBenchmark`'s first table, and the ratio.] Nothing is wrong with the data. Spark
compiled the query again, because a date it writes into the query's code is new.

The earlier posts here - [The 8000-byte cliff in Spark
SQL](https://vecbricks.github.io/the-8000-byte-cliff/), [When Spark stops compiling your
query](https://vecbricks.github.io/when-spark-stops-compiling-your-query/) and [A large CASE
WHEN, split only where it has to be](https://vecbricks.github.io/split-only-where-it-has-to-be/) - were about the code a stage compiles to. This one is
about when Spark compiles it: how often, where, and which queries it compiles again.

> **In short**
>
> 1. Spark compiles each stage's Java source with Janino and caches the class by that source.
>    Integers, longs, doubles, dates and timestamps are written into the source, so a new value is
>    a new class. Strings and decimals are passed to the class instead, so a new one is not.
> 2. A named parameter doesn't help: Spark binds it as a literal, so `:p` compiles like the value
>    it holds.
> 3. One new integer cost five compiles on a driver and two executors: one on the driver, two on
>    each executor. In local mode the default cache of 100 classes held about thirty such
>    queries; a loop of more than that compiled every query again.

## 1. From a plan to running code

![Where one query is compiled](figures/svg/fig36-where-one-query-is-compiled.svg)

*Figure 1. One query with a new integer on a cluster of a driver and two executors, from
`executors-jdk25-output.txt`: one compile on the driver and two on each executor.*

Whole-stage codegen turns each stage of the physical plan into one Java source string. The driver
compiles it with Janino in `CodeGenerator.compile`, to check that it compiles and to read the
size of every method, which is where the 64 KB fallback and the 8000-byte check of the earlier posts
are decided. Then it ships the stage to the executors, and what it ships is the source, not the
class. Every executor compiles the source again into a cache of its own.

Each JVM's cache holds `spark.sql.codegen.cache.maxEntries` classes, 100 by default, an internal
setting read once when the JVM starts. It is keyed by the source text and by the class loader of
the thread that compiles (since [SPARK-44795](https://issues.apache.org/jira/browse/SPARK-44795)).
A task runs under the executor's class loader, not the driver's, so even in local mode, where
driver and executor share one JVM, a stage the driver compiled is compiled again by its first
task.

A new class starts in the JVM's bytecode interpreter, moves to C1's code once it is warm and to
C2's later. So every compile is paid twice: once in Janino, and again in the time the new class
runs slowly before the JIT has caught up.

## 2. Why the same query compiles again

`Literal`'s code generation writes integer, long, float, double, date and timestamp values into
the source as text: `filter_value_0 = inputadapter_value_0 == 6;`. Strings, decimals and binary
values go into the class's `references` array instead, and the source only reads them from there.
So `WHERE i = 6` after `WHERE i = 5` is a new source, and `WHERE s = '6'` after `WHERE s = '5'` is
not.

The script `literals.scala` runs one query, then the same query with a new value of each type,
and counts the classes each run compiled with Spark's own counter (`CodegenMetrics`). In local
mode:

| the query after `WHERE i = 5` | classes compiled |
|---|---|
| the same query again | 0 |
| a new int, bigint, double, date or timestamp | 3 |
| a new string or decimal | 0 |
| a new value of an int parameter, `WHERE i = :p` | 3 |
| a new value of a string parameter, `WHERE s = :p` | 0 |
| a string value for an int column, `WHERE i = :p` with `:p = '10'` | 3 |

The three classes are the stage, compiled by the driver, the stage again, compiled by the task,
and the filter that decides which of the cached table's batches to read, which holds the value
too. A parameter is no way out: Spark substitutes `:p` with a literal before it plans, so an
integer parameter is an integer literal by the time code is generated.

## 3. What it costs

**Per executor.** On `local-cluster[2,1,1024]`, one driver and two executors, the new integer
compiled one class on the driver, the stage, and two on each executor, the stage and the batch
filter (`executors.scala`). A new string compiled nothing anywhere. Every executor that runs a
task of the stage compiles it, so on fifty executors over a cached table one click is a hundred
and one compiles.

**Per query.** [From `CodegenRecompileBenchmark`'s first section: the short query's ms with the
same int each time and with a new one, and the same for a date, a string and an int parameter;
then, per query, the classes compiled and the ms Janino took, so that the rest - mostly the new
class running in the interpreter - can be read off.]

**Against a stage's size.** [From its second section: Janino's best and median ms for
projections of 1 to 99 entries and aggregates of 10 to 60 sums, against the source's size and the
largest method's bytes. The point to make: what a wide stage costs to compile again, on the driver
and on each executor that runs it.]

**Past the cache's 100 entries.** `cache.scala` runs a loop of n queries that differ only in an
integer, then the same loop again, and counts how many of the second loop's queries compiled.

![What the class cache keeps](figures/svg/fig37-what-the-class-cache-keeps.svg)

*Figure 2. The second loop's queries that compiled again, under the default cache and under a
cache of 1000, from `cache-100-jdk25-output.txt` and `cache-1000-jdk25-output.txt`.*

In local mode each of these queries compiles three classes, so a cache of 100 holds about thirty
of them. Up to 30 queries the second loop compiled nothing. At 33, 31 of 33 compiled again; at 40
and beyond, every one did. The cache doesn't degrade gently: a loop that is larger than the cache
evicts each class just before it is needed again, so all 150 queries of the largest loop compiled
twice, 450 classes each time. With `spark.sql.codegen.cache.maxEntries=1000` the second loop
compiled nothing at any size up to 150.

On a cluster the classes spread out: the driver caches one class per such query and each executor
two, so by the same arithmetic the driver's cache would hold about a hundred of them and each
executor's about fifty. That is inferred from the per-JVM counts above, not measured.

## 4. What to do today

- **Compare strings where the column is a string.** A new string value compiles nothing. This
  helps only where the column already is a string: a string value compared with an integer
  column is cast and folded into an integer literal before code is generated, and compiled as
  one, three classes like any new integer.
- **Raise the cache where the driver and executors have the memory.** It is a static setting, so
  it goes on the command line or in `spark-defaults.conf`:
  `--conf spark.sql.codegen.cache.maxEntries=1000`. In the run above it kept 150 queries' 450
  classes. It doesn't stop the first compile of each new value; it stops the second.
- **Have fewer distinct stages.** A filter the dashboard doesn't need in the query, applied
  after it, is one fewer source to vary. Not measured here.

## 5. What Spark could do

Strings already show the way: pass the value to the class and read it from there, and every
value of a query shares one class. [From `CodegenRecompileBenchmark`'s third section: with an
integer passed this way, the classes compiled per query with a new value (expected 0) and the
short query's ms against the literal's; and the steady-state cost of reading the value from a
field instead of a constant, on a scan of 20 million cached rows. Then say whether that is worth
proposing upstream.]

## 6. Seeing it on your own cluster

Each compile logs `Code generated in ... ms` at INFO from `CodeGenerator`, on the driver and in
each executor's log, and every JVM's metrics system counts them in the histogram
`CodeGenerator.compilationTime`. A dashboard whose
executor logs show that line on every click is compiling every click. The scripts behind this post
are in [`sql/varka/demo/compiles/`](https://github.com/vecbricks/varka/tree/master/sql/varka/demo/compiles)
and run on a stock distribution with `bin/spark-shell -i`.

If you want to see what the generated class looks like as bytecode, and how a generator that
writes bytecode directly can avoid the second compile altogether, the companion post is [link to
row 259's post when it is published].
