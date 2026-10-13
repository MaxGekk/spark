# VARKA-271: Spark's own SQL suites with Varka on, Spark's golden files the oracle

## 1. Where this came from

Every check of Varka's answers today compares Varka with something this project wrote: the
reference evaluator, the coverage table, the grammars' random trees, and row 81's corpus of
rewritten date statements. Row 271 adds the one oracle this project did not write. Spark's own
golden-file suite, `SQLQueryTestSuite`, has 374 input files under
`sql/core/src/test/resources/sql-tests/inputs/`, each with a committed `.sql.out` file that
records the schema and rows Spark answers. If Varka changes any answer to a query it serves, the
committed file shows it.

Two other accelerators do this (`m7/READING.md` 2). Comet patches a Spark checkout per Spark
version and runs Spark's suites with Comet loaded; 47 tests are excluded in its 4.1.3 patch,
each with a reason. Gluten subclasses about 480 of Spark's suites and keeps about 450 per-test
excludes. Its known-failures gate fails on a new failure and also when a listed failure starts
to pass, so the list cannot go stale. The record called this "the widest differential available
and not yet in the record as a row" (`m8/SCOPE.md` item 24, 16 September 2026). The plan review
added row 271 to milestone 7. The owner chose it as the next test-strength row on 13 October
2026, and on the same day proposed the literal-lifting step of 3.3.

## 2. The admission check, done

Probes on 13 October 2026, in a scratch worktree at `de011ea9b38` (master as of row 311). The
probe code was a test-only session extension, a query listener and a subclass of
`SQLQueryTestSuite`, all removed afterwards.

**Varka on alone reaches nothing.** Varka serves only Arrow batches (`docs/sql-varka.md`,
"Execution integration": "non-Arrow batches fall back entirely"). The golden files read
`VALUES` lists, temporary views over them, and Parquet tables. A query with no `FROM` folds to
a literal before any operator exists. Measured on nine test files (`date.sql`, `interval.sql`,
`extract.sql` and `int4.sql`, in their default, `nonansi/` and `postgreSQL/` forms where they
exist): 781 query executions, 4 with a Varka node in the plan, 0 batches served. The four nodes
sat over vectorized Parquet batches, which are not Arrow.

**An Arrow source puts Varka under 1,423 query executions.** The probe placed an Arrow-cached
copy of every relation that has a Varka-typed column (`DATE`, `INT` or a year-month interval)
under the query that reads it. This is the same plan node `.cache()` makes with the Arrow
serializer. The whole suite ran in two arms, each in one JVM. Each arm took 56 minutes: 790 tests,
779 passed, 11 failed, 59 ignored. The listener recorded every query execution, setup statements
and commands included. In the 390 execution tests (the analyzer-only variants aside):

| step | query executions |
| --- | ---: |
| recorded | 32,429 |
| read an Arrow-cached relation | 8,047 |
| had a Varka node in the plan | 1,494 |
| had a batch that Varka served | 1,423 |

Varka served 5,323 batches in all, in 105 of the 390 execution tests. Most of the nodes were
filters: 1,464 row-consuming filter nodes, 106 row-consuming projection nodes and 24 columnar
filter nodes, counted as query executions over both kinds of test.

**Varka changed no golden answer.** The arm with the Arrow source and Varka off (the
**cache-only arm**) failed the same 11 tests as the arm with Varka on (the **Varka arm**). Ten
of the 11 pass on unmodified Spark, so the harness causes them:

| test | why it fails |
| --- | --- |
| `explain.sql`, `explain-aqe.sql`, `explain-cbo.sql`, `hints.sql`, `join-nearest-by.sql` | the expected output is a printed plan, and the plan now shows the cache scan |
| `identifier-clause.sql`, `identifier-clause-legacy.sql`, and their two analyzer tests | the probe cached the target table of a `DELETE`, and Spark raised `INTERNAL_ERROR` where it raises `UNSUPPORTED_FEATURE.TABLE_OPERATION` |
| `metadata_column_resolution.sql` | a cached file relation has no hidden `_metadata` columns, and the query reads them |

The eleventh, `udtf/udtf.sql - Python UDTFs`, also fails on unmodified Spark in this
environment.

**Lifting literals more than triples what Varka serves on those files.** A second probe rule ran
on the analyzed plan, before the optimizer. In each `SELECT` with no `FROM`, it replaced each
`DATE`, `INT` or year-month interval literal with a column of a 17-row relation holding that
value, and kept one row with `LIMIT 1`. On the nine test files, the query executions with a
batch Varka served rose from 26 to 88. The lift broke one query in both arms:
`map(1, interval 1 year, 2, interval 2 month)` failed once its key was a column.

**Three traps the probe found:**

1. Varka's warm-up (`spark.sql.codegen.varka.warmup.enabled`, on by default) sends a new
   kernel's first batches to the row path while C2 compiles it. A golden query has a few rows in
   one batch, so with warm-up on Varka serves nothing. The probe turned it off.
2. Spark's `ConvertToLocalRelation` evaluates a projection or a filter over a `VALUES` list at
   plan time, so no operator is left for Varka. The probe excluded that rule.
3. Building the cached plan runs the optimizer on the relation again, which runs the rule again.
   The rule must not apply while it builds a cached plan.

What the check would have rejected: running the suites with Varka switched on and nothing else,
which tests nothing; and a design that took the harness's 10 failures for Varka's.

## 3. The design

### 3.1 The Arrow source

A test-only session extension in `sql/core`'s test tree, `VarkaArrowSource`, adds two rules.

1. **Cache under queries.** An optimizer rule places an Arrow-cached copy under every relation
   that has a Varka-typed column: a `VALUES` relation and a file relation. It does not apply
   under a command (`DELETE`, `UPDATE`, `MERGE`, `INSERT` and the DDL), or to a relation whose
   query reads hidden metadata columns. These two limits remove five of the probe's harness
   failures (the four `identifier-clause` tests and `metadata_column_resolution.sql`). It does not
   apply while it builds a cached plan.
2. **Lift literals** (3.3).

The extension also sets what the probe found it needs: the Arrow cache serializer,
`ConvertToLocalRelation` excluded, and the warm-up off. It registers a listener. After each
query execution, the listener records the batches Varka served and releases the query's caches.

### 3.2 Two arms over the golden files

`VarkaSQLQueryTestSuite` extends `SQLQueryTestSuite` and runs every golden file through the Arrow
source. Spark's committed `.sql.out` files are the oracle, unchanged. One property selects the
arm:

- the **Varka arm**: the Arrow source and Varka on;
- the **cache-only arm**: the Arrow source and Varka off.

A test that fails in the Varka arm and passes in the cache-only arm is a Varka failure. A test
that fails in both is the harness's.

The suite runs in a JVM of its own. `InMemoryRelation` keeps its cache serializer in one
process-wide field, set at first use, so the suite cannot share a JVM with suites that cache
with another serializer. The nightly runs the Varka arm. The cache-only arm runs only when the
Varka arm fails a test that is not on the exclusion list, and then only over the failing files.
So a normal night costs one arm, and a new failure is attributed the same night.

### 3.3 Lifting literals

A query with no `FROM` reaches no Varka kernel. The lift gives it one. A plan-normalization rule
(`SparkSessionExtensions.injectPlanNormalizationRule`) runs on the analyzed plan, before the
optimizer folds constants. In each `SELECT` with no `FROM`, it replaces each literal that Varka
can read as a column with a column of a relation of 17 equal rows, and keeps one row with
`LIMIT 1`.

- **Which literals.** A literal is lifted only where its parent expression is one the compiler
  fuses with that child as a column. The rule asks `VarkaExpressionCompiler` with the child
  replaced by a column of the same type, so the set cannot drift from what Varka fuses. This
  keeps the `map(...)` key and every other position where a column changes the meaning.
- **Why 17 rows.** One row runs only the kernel's epilogue. Seventeen rows run the dense loop
  over two lane groups of eight and then the epilogue.
- **Why the golden file still applies.** Analysis fixes every output column's name before the
  rule runs, and the 17 rows are equal, so `LIMIT 1` returns the query's own schema and row.

Row 81's corpus stays as it is (`VarkaGoldenCorpusSuite`): the seven date-family files, select
statements rewritten as text, fixtures with null rows, and each statement compared on the row
engine and on Varka. 271 covers every file and whole statements, with the golden files as the
oracle.

### 3.4 The exclusion list

`sql/varka/spark_suites/excluded.tsv` lists each test that may fail, tab separated: test name,
arm, kind (`harness` or `environment`) and reason. The suite wraps each test through
`SQLQueryTestSuite.createScalaTestCase`. A listed test is cancelled with its reason when it
fails, and fails as a **stale entry** when it passes. Its line must then be removed (Gluten's
rule, and the matrix's `skips.tsv` rule). `SQLQueryTestSuite.ignoreList` is not used, because an
ignored test never runs and so can never show that its entry is stale.

The first list has six entries: the five tests whose output is a printed plan, kind `harness`,
and `udtf/udtf.sql - Python UDTFs`, kind `environment`.

### 3.5 The served report

`sql/varka/spark_suites/served.json` records, per test, the query executions and the executions
with a batch Varka served. It is regenerated with a flag, as `golden_corpus.json` is. A test
that passes in the Varka arm with no served batch tests nothing about Varka, and the report shows
it. Whether the counts repeat run to run is not yet measured (prediction 4). If they repeat, the
suite compares the report byte for byte. If not, it holds each test to a floor that
regeneration raises.

### 3.6 Deliberately unchanged

- **Spark's code and golden files.** The suite is a subclass in this repository. Comet patches
  Spark because it lives outside it.
- **Spark's other SQL suites** (`DateFunctionsSuite` and the like). The same extension can serve
  them as subclasses, as Gluten does. That is a second step, after the golden files show what the
  harness costs.
- **The JIT tier.** With the warm-up off, the kernels run before C2 compiles them. That is a
  departure from production, where the warm-up is on. Row 274 checks the compiled code.

### 3.7 Registered op counts

None: no emitted code changes.

## 4. Files

| file | what |
| --- | --- |
| `sql/core/src/test/scala/.../VarkaArrowSource.scala` | the extension: the cache rule, the lift rule, the settings, the listener |
| `sql/core/src/test/scala/.../VarkaSQLQueryTestSuite.scala` | the subclass, its two arms, the exclusion list's wrap, the report |
| `sql/varka/spark_suites/excluded.tsv` | the exclusion list |
| `sql/varka/spark_suites/served.json` | the served report |
| `dev/varka_nightly.sh` | the Varka arm, and the cache-only arm over a new failure's files |
| `docs/sql-varka.md` | the suite in "Testing and benchmarks" |

## 5. Tests, and what each is for

1. **A planted fault fails golden tests.** With the fault injector `misdescribeAdd` on, through
   the emit-options test hook the SQL suites use, the Varka arm fails the golden tests whose
   served queries add, and the cache-only arm does not. Without it, a suite that never fails
   could mean the harness does not reach Varka.
2. **A stale entry fails.** An entry for a test that passes fails that test with "stale entry".
3. **The exclusion list parses**, and a malformed line fails.
4. **The lift keeps the golden answer.** On the nine probe files, the lifted Varka arm passes the
   same tests as the unlifted one, and the served executions rise.
5. **The cache rule leaves commands alone.** The four `identifier-clause` tests and
   `metadata_column_resolution.sql` pass in both arms.

No pinned fixture moves.

## 6. The measurement

The whole Varka arm, once without the lift and once with it, the funnel of 2 and the time of
each. The Varka arm run twice, to compare the served report.

### 6.1 Predictions, registered before the run

1. With the command and metadata limits of 3.1, the cache-only arm fails exactly the six tests on
   the first exclusion list.
2. The Varka arm fails no test that the cache-only arm passes.
3. With the lift, the query executions with a served batch rise from 1,423 to at least 2,000.
   The nine probe files rose from 26 to 88, but most files have few date or int literals.
4. With the warm-up off, the served report is the same in two runs of the Varka arm.
5. The planted fault of test 1 fails at least ten golden tests.
6. The Varka arm takes under 70 minutes in one JVM.

## 7. Risks

1. **The harness can change what Spark answers.** A cached relation can change row order,
   statistics or which errors surface. The cache-only arm separates these effects from Varka's,
   and the list's reasons name them.
2. **The lift can change a query's meaning.** The compiler-derived rule of 3.3 limits it, and
   test 4 holds the lifted and unlifted arms to the same golden answers.
3. **Memory.** Each query builds caches. The listener releases them after each execution. A
   failing query whose listener never runs could keep them, and the suite clears what is left
   after each file.
4. **Time.** One arm is about an hour in one JVM. The suite is one class, so a split runs file
   lists across JVMs with `-z`, in the nightly only.
5. **Spark upgrades.** A new Spark version brings new golden files and changes old ones. The
   suite runs whatever files the checkout has, and the list's stale rule shows when an entry no
   longer applies.

## 8. Sequencing

Two pull requests.

1. **271a:** the extension's cache rule and listener, the suite and its two arms, the exclusion
   list, the served report, and the nightly step. Proved by tests 1, 2, 3 and 5, and predictions
   1, 2, 4, 5 and 6.
2. **271b:** the lift. Proved by test 4 and prediction 3. Then section 9.

## 9. Outcome
