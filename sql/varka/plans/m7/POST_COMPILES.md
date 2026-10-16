# How Spark compiles a query, and why it compiles it again

*Draft (row 258, `VARKA-258.md`). Every count below is from `sql/varka/demo/compiles/` on stock
Spark 4.2.0 with JDK 25. The times are from `CodegenRecompileBenchmark` on a GitHub-hosted runner's
AMD EPYC 7763 with JDK 25 (`sql/core/benchmarks/CodegenRecompileBenchmark-jdk25-results.txt`), on this repository's Spark, master-based, with Varka off; the
owner decides whether that build stands in for 4.2.0 at publication.*

---

A dashboard runs the same query every time someone moves a filter. The query is short: a count
over a cached table. Yet the first run with each new date is slower than the rerun with the same
date, every time. On a cloud runner a count over 100,000 cached rows took 24.92 ms on average
with the same integer each time and 41.45 ms with a new one. Nothing is wrong with the data. Spark
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

**Per query.** The same short query, run back to back in local mode on a runner, 40 queries per
row:

| `SELECT count(*) FROM t WHERE ...` | classes a query | ms a query | of which Janino |
|---|---|---|---|
| the same int each time | 0 | 24.92 | 0 |
| a new int each time | 3 | 41.45 | 16.42 |
| a new date each time | 3 | 42.77 | 16.05 |
| a new string each time | 0 | 21.61 | 0 |
| a new int parameter each time | 3 | 42.57 | 15.46 |

A new integer made the query about two thirds slower, and almost all of the difference is Janino,
16.42 ms a query. For a query this small the new class's time in the interpreter
hardly shows; "When Spark stops compiling your query" measured where it does, on wide stages that
took seconds to reach their steady speed.

**Against a stage's size.** Each compile from a source new to the cache, best of nine:

| stage | source | largest method | Janino, best |
|---|---|---|---|
| a projection of 1 entry `id + k` | 4470 chars | 280 bytes | 3.91 ms |
| a projection of 99 entries | 31930 chars | 3672 bytes | 9.07 ms |
| an aggregate of 10 sums by a key | 20456 chars | 486 bytes | 7.97 ms |
| an aggregate of 60 sums by a key | 82037 chars | 2586 bytes | 17.85 ms |

Janino's time grows with the source, and every JVM that runs the stage pays it again: a 60-sum
aggregate with a new literal is about 18 ms on the driver and about 18 ms more on each executor.

**Past the cache's 100 entries.** `cache.scala` runs a loop of n queries that differ only in an
integer, then the same loop again, and counts how many of the second loop's queries compiled.

![What the class cache keeps](figures/svg/fig37-what-the-class-cache-keeps.svg)

*Figure 2. The second loop's queries that compiled again, under the default cache and under a
cache of 1000, from `cache-100-jdk25-output.txt` and `cache-1000-jdk25-output.txt`.*

In local mode each of these queries compiles three classes. Up to 30 queries, 90 classes, the
second loop compiled nothing. At 33, 99 classes, 31 of 33 compiled again: the cache is Guava's,
which evicts within each quarter of it, so it starts evicting a little before it holds 100. At 40
and beyond, every query compiled again. The cache doesn't degrade gently: a loop that is larger than the cache
evicts each class just before it is needed again, so all 150 queries of the largest loop compiled
twice, 450 classes each time. With `spark.sql.codegen.cache.maxEntries=1000` the second loop
compiled nothing at any size up to 150.

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
value of a query shares one class. To see whether that would pay for integers, the benchmark
has a stand-in expression that passes the integer to the class and reads it from a field, and runs
`SELECT count(*) FROM t WHERE i % 1000000 = <value>` both ways, after checking that both count the
same rows. With a new value each time, the literal compiled 2 classes a query and averaged 42.96
ms; passed to the class, it compiled none and averaged 19.92 ms, the same as repeating one literal
(19.30 ms). Once warm it costs nothing: a scan of 20 million cached rows took 12.4 ns a row with
the literal and 12.1 with the field, one run each, a difference too small to call either way.
On these numbers the change would pay, and Spark already does it for strings.

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
