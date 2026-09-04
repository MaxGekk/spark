# Task 62: The closing measurement: every date expression against stock Spark

## 1. Where this came from

`PLAN_MILESTONE_4.md` section 2.29 and row 62, written on 4 September 2026
when the milestone was re-scoped to the date family, from the owner's
directive: "at the end of milestone 4 I would like to have benchmarks for
all added expressions for the DATE data type. And we should run the
benchmark on a CPU with 512bit datapath and compare with vanilla Spark on
JDK 17 (default one) and on JDK 25", run on a GitHub Actions runner picked
by CPU model, with the README rewritten around the result and a
reproduction guide. The audience is outside this repository: the owner
intends to present Varka publicly once the milestone closes. The same day,
task 56's measurement (`PLAN_TASK_56.md` 9.2) showed that the throughput
benchmark's 10 ms Varka rows are mostly the job's fixed cost, and the owner
added the job-size rule to section 2.29: the per-job fixed cost under 5% of
every Varka row's wall time, with executor time recorded beside wall time.
This task is the last row of the milestone; this plan splits it into three
pull requests so the driver and its laptop run land first, as the baseline
the rule needs, while the remaining date-lane tasks are still open.

## 2. The admission check, done

**2.1 The stock release, and whether it runs on JDK 25.** This fork is a
snapshot of Apache Spark's master (`5.0.0-SNAPSHOT`), so no released Spark
carries its code; the baseline a reader has today is the newest release,
**Spark 4.2.0** (the newest directory under `dlcdn.apache.org/spark/` on 4
September 2026). Checked: `spark-4.2.0-bin-hadoop3`'s `spark-sql` answers
`select date_add(date'2020-01-01', 3), version()` under this machine's
OpenJDK 17 and OpenJDK 25 alike, so both baseline rows exist. The table
says "Spark 4.2.0", not "stock master", and the fork-with-Varka-off run is
the row that shows what the fork carries besides the kernel. What the check
would have rejected: a JDK 25 baseline row that does not start, in which
case the second baseline would be 4.2.0 on the newest JDK it supports.

**2.2 The benchmark workflow cannot run a downloaded distribution as it
stands.** `.github/workflows/benchmark.yml` builds this checkout's test jars
and submits `spark-core*-tests.jar` with `--class
org.apache.spark.benchmark.Benchmarks` through the checkout's own
`bin/spark-submit` under one `setup-java`; the class input is an argument to
that dispatcher. Its CPU pin, extra JVM options, `create-commit` and
tar-what-git-sees steps carry over unchanged to any file a driver writes.
So PR (B) is a sibling workflow with the three `SPARK_HOME`s and two JDKs,
not a mode of this one - and the driver is a plain `spark-submit`
application, not a class on the test classpath, or it could not run on
stock Spark at all.

**2.3 The job size.** Task 56's probe (`PLAN_TASK_56.md` 9.2) put the
cheapest Varka shape through the executor near a nanosecond per row, with
the job's fixed cost near 10 ms on that laptop; the section 2.29 rule (fixed
under 5%, so at least 200 ms of wall time per Varka row) therefore means
about 200M rows, whatever the partitioning, because the fixed cost is
per job and the executor cost is per row. At 200M rows the table is three
columns of four bytes plus validity, under 3 GB in either cache, and the
stock rows at their measured 20-odd nanoseconds per row take four to five
seconds per iteration, which puts a full three-distribution run in the
order of an hour. What the check would have rejected: a row count picked
for a short run, which is what the throughput benchmark's 2M rows were.

**2.4 What is covered today.** `compileNode` and `compileCond`, read in
this worktree: the arms in 3.4, and no others. `weekofyear` (task 37),
`make_date` (task 42), the ISO fields (tasks 57 and 58) and the column
forms of `next_day`, `add_months` and `trunc` (tasks 59 to 61) are open
rows, so they are not in this PR's list; the list is data and PR (B)'s
dispatch runs what it holds then.

## 3. The design

### 3.1 A standalone driver, submitted to any Spark

**Three pull requests.** (A) the driver, its module, the shell driver and
the laptop's run - this PR; (B) the workflow that runs the three
distributions on a pinned runner and commits their files; (C) the README's
benchmark section rewritten from the committed files, with the reproduction
guide. Each has its own gate; the milestone row closes with (C).

**The module: `sql/varka/bench`**, Maven coordinates
`org.apache.spark.varka:varka-bench`, on the engine module's precedent
(`sql/varka/engine/pom.xml`: no parent, its own small plugin set) but *not*
in the root reactor, because it compiles against a **released** Spark - the
`spark-sql_2.13` artifact of the stock release under test, `provided` scope -
so that the one jar runs unchanged on every distribution. It uses only the
API that has been stable since Spark 3: `SparkSession.builder`, `sql`,
`Dataset.write.format("noop")`, `count`, `SparkListener` and `TaskMetrics`,
and `EXPLAIN` through SQL. `maven.compiler.release` is **17**, the oldest JDK
it runs on. No dependency on this fork: the fork is a distribution like the
others, and enabling Varka is a `--conf`
(`spark.sql.codegen.varka.enabled=true`, the rule being registered in every
session by `BaseSessionStateBuilder`) plus the static
`spark.sql.cache.serializer` pointing at the Arrow serializer, both passed
by the shell driver, never known to the Java code.

**The driver, `DateSurfaceBenchmark`.** Arguments: `--out FILE`, `--rows N`,
`--label NAME` (the distribution's name, printed in every table), `--iters`
(default 5), `--warmup-seconds` and `--min-seconds` (default 2 and 2, task
14's methodology), `--only REGEX` for a partial run, and `--provenance
KEY=VALUE` repeated, for what only the shell driver knows (commit, the
datapath probe). It builds one table, `varka_dates`, with the generator the
throughput benchmark uses - `d` a date with every 31st row null, `d2` a
second date, `i` an int - over `range(0, N)` in `ceil(N / 4M)` partitions,
caches it through `spark.catalog.cacheTable` and forces it with a count.
Then, for every entry of the **surface** (3.4), in two shapes:

* the projection: `SELECT <expr> AS a FROM varka_dates` written to the
  `noop` sink, which accepts columnar batches, so the fork's kernel output
  is consumed without a row conversion and stock Spark's codegen output is
  consumed the same way;
* the filter: `SELECT count(*) FROM varka_dates WHERE <pred>`, the count
  being the cheapest consumer of a filter that cannot be optimised away.

Each is warmed for `--warmup-seconds`, then run for at least `--iters`
iterations and `--min-seconds`, with wall time per iteration from
`System.nanoTime` and executor time per iteration from a `SparkListener`
summing `TaskMetrics.executorRunTime` over the iteration's tasks (the
listener bus is drained with `waitUntilEmpty` before reading, so no
iteration reads the previous one's tasks; warm-up iterations are discarded
by index). Before timing, on every distribution, the driver runs
`EXPLAIN` on the query and records whether the plan contains a Varka node;
on the fork with Varka on, an entry that does not fuse is reported in the
file as `residual`, never silently timed, and the shell driver fails the
run if any entry the surface marks as expected-fused is residual.

**The results file** is in Spark's harness format exactly - the table
header `<name>:  Best Time(ms)  Avg Time(ms)  Stdev(ms)  Rate(M/s)  Per
Row(ns)  Relative` and its row layout - because `dev/varka_bench_diff.py`
keys on that header and `dev/varka_quote_check.py` reads those numbers.
Two tables per entry: `<entry> over N rows` with two cases, `projection,
columnar consumer` and `filter, counted`, and `<entry> over N rows,
executor time` with the same two cases, so the fixed share of every row is
`(wall - executor) / wall` from the file. The file opens with the
provenance block `dev/varka_bench_regen.sh` writes - commit, date, JDK,
kernel, CPU, power, load at start - extended with what this task needs:
Spark's `version()`, the JVM's `MaxVectorSize` read through
`HotSpotDiagnosticMXBean` (the flag the run actually had, not the one it
was asked for), the `avx512*` flags from `/proc/cpuinfo`, and the datapath
probe (below). Files are named `DateSurface-<label>-jdk<NN>-results.txt`
under `sql/varka/bench/benchmarks/`, and the quote check's `RESULT_GLOBS`
gains that directory.

**The shell driver, `dev/varka_bench_surface.sh`.** Takes the three (or
four) distributions as `LABEL=SPARK_HOME:JAVA_HOME[:extra confs]`, refuses a
busy machine like the regen script, runs the canary, runs the **datapath
probe** - `dev/varka_canary/Canary.java` under JDK 25 at
`-XX:MaxVectorSize=32` and `=64`, whose compute rates' ratio is near 2x on a
full-width unit and near 1x on a double-pumped one (`SKILLS.md`, "This
machine's AVX-512 is 256 bits wide") - and passes `datapath=<r32>/<r64>` to
every run's provenance, then submits the jar to each distribution in turn
with `spark-submit --master local[1] --driver-memory 8g`, and finishes with
the diff script's comparison of each baseline file against the Varka file.
`dev/varka_bench_diff.py` gains `--table`, which prints the README's
markdown table - one row per entry and shape, the query text beside the
number, wall rates and the ratio new/old - from two files.

**The job size.** `--rows` defaults to 200M, from the rule in section 2.29
and the arithmetic in 2.3: the cheapest Varka shape runs near a nanosecond
per row through the executor, so 200 ms of wall time is 200M rows, and the
row count is the same for every entry and distribution so ratios are on
the same data. The driver prints the fixed share beside every Varka row
and the shell driver fails the run when a Varka row is over 5%.

### 3.2 What is deliberately unchanged

* The emitter, the compiler, the evaluator: this task measures them.
* `VarkaThroughputBenchmark` and the other committed benchmarks: they stay
  the project's own A/B instruments at 2M rows; the sizing rule applies to
  the public table, and their methodology is a separate note for milestone
  5's debt register.
* `.github/workflows/benchmark.yml`: it runs this repository's test-jar
  benchmarks through the `Benchmarks` dispatcher and stays so; PR (B) adds
  a sibling workflow rather than a mode.
* The stock distribution's cache serializer: stock Spark caches through its
  own columnar serializer, the fork through the Arrow one when Varka is on.
  That difference is part of what the reader gets, and the fork-with-Varka-
  off run (default serializer) is the row that separates it from the kernel.

### 3.3 Registered op counts

None: no emitted byte changes.

### 3.4 The surface

One entry per covered date expression, from the compiler's arms as of this
PR (`VarkaExpressionCompiler.compileNode` and `compileCond`), with the
spelling a reader would write. Projections: `date_add(d, 3)`,
`date_add(d, i)`, `date_sub(d, 5)`, `datediff(d2, d)`, `year`, `month`,
`day`, `quarter`, `dayofyear`, `dayofweek`, `weekday`, `next_day(d,
'MONDAY')`, `last_day`, `add_months(d, 3)`, `d + INTERVAL 3 MONTH`,
`trunc(d, 'YEAR'|'MONTH'|'QUARTER'|'WEEK')`, `unix_date`,
`date_from_unix_date(unix_date(d))`, `if(d < d2, d, d2)`, `CASE WHEN d <
d2 THEN d ELSE d2 END`, `coalesce(d, d2)`, `greatest(d, d2)`, `least(d,
d2)`, `year(date_add(d, 30))` as the fused chain. Filters: `d < d2`,
`d = d2`, `d BETWEEN DATE'2020-06-01' AND DATE'2021-06-01'`, `d IN
(DATE'2020-01-01', DATE'2020-07-01', DATE'2021-01-01')`, `d IS NULL`, `d IS
NOT NULL`, `year(d) = 2021`, `dayofweek(d) = 1`, `d < d2 AND month(d) = 6`.
The list is data in one Java class; task 56's `d + CAST(i AS INTERVAL
DAY)` (#118, open), `weekofyear`, `make_date` and tasks 57-61's forms are
one line each when they land, and the final dispatch in PR (B) runs
whatever the list holds then.

## 4. Files

| file | what |
|---|---|
| `sql/varka/bench/pom.xml` | the module: release 17, `spark-sql_2.13` of the stock release provided, JUnit 5 |
| `sql/varka/bench/src/main/java/.../bench/DateSurfaceBenchmark.java` | arguments, the table, the loop over the surface, the two shapes |
| `.../bench/Surface.java` | the entry list: label, projection expression, filter predicate, expected fused |
| `.../bench/Harness.java` | warm-up, iterations, wall and executor timing, the harness-format tables |
| `.../bench/Provenance.java` | the block: the regen script's fields plus version, `MaxVectorSize`, CPU flags, probe |
| `sql/varka/bench/src/test/java/...` | section 5's tests |
| `sql/varka/bench/benchmarks/` | the laptop's files, one per distribution, from this PR's run |
| `dev/varka_bench_surface.sh` | the shell driver: gates, canary, datapath probe, the runs, the comparison |
| `dev/varka_bench_diff.py` | `--table` |
| `dev/varka_quote_check.py` | the new results glob |
| `dev/varka_bench_regen.sh` | (folded in, unrelated) a bare class name resolved from the module's sources |
| `PLAN_MILESTONE_4.md`, this file | row 62 as "(A) done, (B) and (C) open", section 9 |

## 5. Tests, and what each is for

* `HarnessFormatTest`: the formatter's output matches the diff script's
  `HEADER` and `ROW` regexes, copied into the test, for a table with two
  cases, so a drift in the layout fails here and not as an empty diff.
* `HarnessTimingTest`: with a fake clock and a fake listener, five
  iterations over two-second windows yield the best, average and standard
  deviation the harness prints, warm-up iterations excluded.
* `SurfaceTest`: every entry parses and runs on a local stock session over
  a thousand rows, both shapes, and every entry's projection is a `DateType`
  or the type the entry declares - the failure it catches is a typo in the
  list, before a two-hour run finds it.
* `ProvenanceTest`: the block has every key, `MaxVectorSize` is an integer,
  and unknown values print as `n/a` rather than failing the run.
* On the fork, the driver's own `EXPLAIN` check is the fusion test: the
  laptop's Varka file must show every expected-fused entry fused, which the
  shell driver enforces.

## 6. The measurement

The laptop, this PR: `dev/varka_bench_surface.sh` over Spark 4.2.0 on JDK
17, Spark 4.2.0 on JDK 25 (if it starts; 2.1), this fork with Varka on JDK
25, and this fork with Varka off on JDK 25, at 200M rows, on an idle
machine, one run, the four files committed with provenance. The control
rows are the two stock runs against each other (the JDK's own effect) and
the fork-with-Varka-off run against stock on JDK 25 (what the fork carries
besides Varka). The 512-bit runner's files are PR (B)'s.

### 6.1 Predictions, registered before the run

1. At 200M rows every Varka projection row runs 180 to 600 ms of wall time
   with a fixed share under 5%; the rule passes without a second sizing.
2. On this laptop (256-bit datapath), Varka against stock Spark on JDK 17,
   wall time: 4x to 10x on the single-expression projections, 2x to 4x on
   the filters, the fused chain at the top of the range. Executor-time
   ratios are higher than wall ratios on every row, because the fixed cost
   is the same on both sides and is a larger fraction of the faster one.
3. Stock Spark on JDK 25 against JDK 17: within 10% either way on every
   row; the JDK is not where the difference comes from.
4. The fork with Varka off against stock Spark on JDK 25: within 20% on
   every row, the residue being the Arrow cache path against stock's
   columnar cache; if a row is further apart, that row is explained before
   the README quotes it.
5. No row is a loss on the projections; on the filters, a predicate that
   selects almost nothing (`d IS NULL`) is where the fork is closest to 1x,
   since the count dominates.

## 7. Risks

1. **Spark 4.2.0 does not run on JDK 25.** Checked in 2.1 before the
   design was fixed; if it does not, the JDK 25 baseline is Spark 4.2.0 on
   the newest JDK it supports, said so in the table.
2. **200M rows do not fit the runner.** PR (B)'s problem, but the row
   count is an argument and the fixed share is in the file, so a smaller
   run is visibly a smaller run, never a silently overhead-dominated one.
3. **The provided API drifts between 4.2.0 and this fork's 5.0.0.** The
   driver uses the stable subset only and `SurfaceTest` runs on the stock
   artifact; the fork run is exercised by the laptop measurement.
4. **The `noop` sink is not columnar on stock Spark.** It is
   (`NoopDataSource.supportsColumnarWrite`), and the driver's `EXPLAIN`
   shows a `ColumnarToRow` if it is not; the file would say so.
5. **A residual entry is timed as if fused.** The `EXPLAIN` check and the
   shell driver's failure on an expected-fused residual.

## 8. Sequencing

1. The regen script's class resolution (the unrelated one-liner), and this
   plan.
2. The module, the driver, the harness and provenance, with the four unit
   tests; green under `build/mvn -f sql/varka/bench/pom.xml verify`.
3. The shell driver, the datapath probe, `--table`, the quote glob.
4. The laptop run: four files, section 9, the milestone row.

## 9. Outcome

Filled in when the measurement lands.
