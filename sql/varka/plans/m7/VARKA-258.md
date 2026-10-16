# VARKA-258: how Spark compiles a query, and why it compiles it again

*Row 258 of `m7/PLAN.md`, `m9/SCOPE.md` item 78. Outlined on 13 October 2026 at the owner's
decision in the halfway review (`m7/PLAN.md` 9.4), so that the post stops losing the comparison
with the next measurable row. Planned in the shape of the earlier posts (`m6/VARKA-210.md`,
`m6/VARKA-233.md`, `m7/VARKA-288.md`): the question, what the record holds, what has to be
measured with its predictions, the outline, and done-when. The measurements wait for their turn
in the review's ordering (9.3, row 8); this file is the skeleton they fill.*

## 1. The question

The reader runs Spark and has read the earlier posts: "The 8000-byte cliff in Spark SQL", "When
Spark stops compiling your query" and "A large CASE WHEN, split only where it has to be". They
know a stage's method can be too large to compile well. They do not know what happens between the
plan and the running code, or that the same query is compiled several times and, with a new
literal, compiled again. The post answers that, and says what it costs them: on a dashboard whose
filter changes, compiling can be most of a short query's time.

Like the earlier Spark posts, it mentions Varka only through a link, here at the end, to the
companion post (row 259, bytecode without source).

## 2. What the record already holds

- **The pipeline in master's source.** Whole-stage codegen writes one Java source string per
  stage. `CodeGenerator.compile` compiles it with Janino and reads the methods' sizes
  (`ByteCodeStats`), which is where the 64 KB fallback and the 8000-byte check of the first post
  are decided. A per-JVM cache of generated classes keyed by the source (`CodeGenerator.cache`)
  holds `spark.sql.codegen.cache.maxEntries` classes, 100 by default, a static setting. The
  source travels to the executors in the stage's task binary; an internal setting since 4.0.0,
  `spark.sql.codegen.broadcastCleanedSourceThreshold`, can broadcast it separately and is off by
  default.
- **Literals in the source.** `Literal`'s code generation writes integer, long, date, timestamp,
  float and double values into the source text, and passes strings, decimals and binaries as
  references (`ctx.addReferenceObj`), so only the first kind changes the cache key.
- **The JIT's wait**, measured by VARKA-233 for "When Spark stops compiling your query": a new
  class runs interpreted, then as C1's code, then C2's.
- **The 3,000-branch stage** of VARKA-233 logged `Broadcasting large task binary with size 1071.4
  KiB`: the source rides in the task binary.
- **The hook, checked by hand on stock 4.2.0 on the laptop and not committed**: `WHERE id = 6`
  after `WHERE id = 5` compiled two new classes, `WHERE s = 'b'` after `WHERE s = 'a'` none, a new
  date two, and a new value of a named parameter `:p` two, since Spark binds a parameter as a
  literal.

## 3. What has to be measured before the draft

Every measurement runs on a stock distribution, Spark 4.2.0, with its output committed under
`sql/varka/demo/` with the release and JDK in its provenance, as the earlier posts' snippets are.

1. **The literal check, committed.** The hand check of section 2 as a snippet with its output: the
   classes compiled per query (Spark's own `CodeGenerator` metrics), for a repeated query, a new
   int, a new date, a new string and a new parameter value.
2. **Compiles per executor.** The same queries on `local-cluster[2,1,1024]`, the classes each
   executor compiles, from the executors' own metrics.
3. **What a new literal costs a short query.** The latency of a short query over a small cached
   table, a repeated literal against a new one each time, on the driver and end to end.
4. **Janino's compile time against a stage's size**, from the codegen benchmark's rungs.
5. **Past the cache's 100 entries.** A loop of 150 distinct queries and the same loop again: how
   many of the second loop's queries compile.
6. **Whether passing such literals as references would pay**, on a patched build: the classes
   compiled and the short query's latency with the int literal passed as a reference, and the
   steady-state cost of reading it from a field instead of a constant.

### 3.1 Predictions, registered before the runs

1. A new int, date or parameter value compiles at least one new class per stage on the driver and
   on each executor; a new string compiles none.
2. On `local-cluster`, each of the two executors compiles the stage's class once per new literal.
3. For a query of under 100 ms over a small cached table, a new literal at least doubles its
   latency against a repeated one.
4. The second loop of 150 distinct queries compiles at least 50 of them again: the cache keeps
   100.
5. Passing the int literal as a reference removes the recompilation, and costs under 5% on a
   steady-state scan of the stage.

## 4. The outline

1. **A dashboard that is slow on the first click of every filter.** The symptom, from section
   3's latency (3): the same query with a new date is slower, every time.
2. **From a plan to running code.** The pipeline in one figure: the plan, one Java source per
   stage, Janino on the driver, the task binary, Janino on each executor, the interpreter, C1, C2.
   Each step with where in Spark it happens (section 2).
3. **Why the same query compiles again.** The cache is keyed by the source, and some literals are
   in the source. The literal check (1) as the post's central table; parameters do not help.
4. **What it costs.** Per executor (2), per query (3), against a stage's size (4), and past the
   cache's 100 entries (5).
5. **What to do today.** What a user can change: a string-typed parameter where the column allows
   it, a larger cache where the driver's memory allows it, fewer distinct stages; each with its
   measured effect or with the reason it was not measured.
6. **What Spark could do.** Literals as references, as strings already are (6), and whether it is
   worth proposing upstream.
7. **A link** to the companion post, for readers who want to see the bytecode itself.

## 5. Done when

The literal check and every other snippet run on stock Spark 4.2.0 with their outputs committed;
the measurements of section 3 are committed with provenance and the predictions of 3.1 scored in
section 9; the post is drafted, kept short (the owner cuts length from Varka's public writing), and
published, with every number tracing under `dev/varka_quote_check.py`.

## 6. Sequencing

1. This outline, and the row marked as outlined.
2. The snippets and measurements of section 3, in one pull request, in the review's order.
3. The draft, then publication, then section 9.

## 7. Explicitly out of this task

- Varka's own compilation, which is the companion post's (row 259) and the closing post's (270).
- Proposing the literals-as-references change upstream: the post says whether it would pay; a
  proposal is its own decision.

## 8. Risks

1. **The cache or the literal handling changes upstream before publication.** The post names its
   release, and the snippets rerun on the release current at publication.
2. **The short-query latency depends on the machine.** The post states the machine and quotes a
   ratio as well as the times.

## 9. Outcome

### 9.1 The measurements, 16 October 2026

The counts ran on the laptop, since they do not depend on the machine: `sql/varka/demo/compiles/
run.sh` runs three `spark-shell` scripts on the stock Spark 4.2.0 distribution under JDK 25 and
writes their outputs beside them, each with its commit. Three runs gave the same counts. The times
are `CodegenRecompileBenchmark` (sql/core, Java on `VarkaBenchmarkBase`), on the fork's Spark with
Varka off, owed by a GitHub-hosted runner through `benchmark.yml`.

**1, the literal check** (`literals.scala`, `literals-jdk25-output.txt`). In local mode a new int,
bigint, double, date or timestamp compiled three classes; a new string or decimal none; a new
value of an int parameter three, of a string parameter none; a string value for an int column
three, since it is cast and folded into an int literal. The three are the stage compiled by the
driver, the stage compiled again by the task, and the cached table's batch filter
(`SpecificPredicate`), which holds the value too. The stage is compiled twice in one JVM because the
cache is keyed by the compiling thread's class loader as well as the source (SPARK-44795), and a
task runs under the executor's loader. The hand check of section 2 counted two classes; its
query is not in the record, and one over a table that was not cached, which has no batch filter,
would give two.

**2, compiles per executor** (`executors.scala`, `executors-jdk25-output.txt`). On
`local-cluster[2,1,1024]` a new int compiled one class on the driver and two on each executor (the
stage and the batch filter); a new string and a new string parameter none anywhere.

**3, a new literal's cost to a short query; 4, Janino against a stage's size; 6, the integer as a
reference.** Owed by the runner: the benchmark's three sections, in that order. Section 4 uses
projections of 1 to 99 entries and aggregates of 10 to 60 sums, not the `CASE WHEN` rungs: on this
build a stage with a `CASE WHEN` is already compiled while its code is generated, to decide whether
to split it (SPARK-33301), so a timed compile of its source is a cache hit. Section 6 uses
`IntSlot`, a test-only expression that reads the value from a field set from the class's
references, over `i % 1000 = <value>` so that neither arm can prune the cached batches.

**5, past the cache's 100 entries** (`cache.scala`, `cache-100-jdk25-output.txt`,
`cache-1000-jdk25-output.txt`). Loops of n queries differing only in an int, run twice: under the
default cache the second loop compiled nothing up to 30 queries (90 classes), 31 of 33 queries
at 33, and every query from 40 on; the 150-query loop compiled 450 classes in each loop. Under
`maxEntries=1000` the second loop compiled nothing at any n up to 150.

**Predictions scored** (3.1).

1. **Held**, with a second class the prediction did not name: one new class a stage on the driver,
   and on each executor the stage and the cached table's batch filter; a new string none.
2. **Held**, with the same batch filter beside the stage on each executor.
3. Owed (the runner).
4. **Held, and stronger than predicted**: all 150 of the second loop's queries compiled again, not
   50. A loop larger than the cache evicts each class just before it is needed, and at three
   classes a query the cache holds about 30 such queries.
5. Owed (the runner).

### 9.2 The draft, 16 October 2026

`POST_COMPILES.md`, from the counts above, with figures 36 (where one query is compiled, read from
`executors-jdk25-output.txt`) and 37 (what the cache keeps, read from the two cache outputs). The
runner's numbers are in brackets in its sections 1, 3 and 5, as is the link to row 259's post.
