# Task 160: run only the CI a change can reach

*Opened 20 September 2026 on the owner's question whether the build's ninety
minutes were necessary for every Varka pull request. Measured from a full run,
implemented as one change to the precondition and one job, and proved by the
runs the change itself produces.*

## 1. What a Varka pull request paid, measured

Pull request #267's Build on the fork, a change to the emitter and the
compiler: 35 jobs, 1283 job-minutes, about ninety minutes of wall time. The
long poles, in minutes: the three `sql` shards at 94, 94 and 80; the streaming,
Kafka, connect and cloud shard at 93; the Docker integration tests at 84; the
TPC-DS run at 54; `hive - other` at 54; eight PySpark shards between 37 and 61;
the catalyst shard at 51; the Java 25 Maven build at 45; the linters at 24.

The precondition already selects by module - the fork had tuned it further,
turning SparkR, the protobuf and UI jobs off unless their own files change -
and still ran all of that, for one reason: any change under `sql/catalyst`
sets `build=true`, and `build=true` runs the whole module matrix. Spark's rule
is right for Spark, where catalyst is upstream of everything. It is wrong for
Varka, whose code lives almost entirely in files named for it or in `varka`
directories - 30 main and 35 test files in catalyst, 6 and 21 in sql/core, and
`sql/varka` - and whose hooks into Spark are six shared files (`SQLConf`,
`StaticSQLConf`, `BaseSessionStateBuilder`, `datetimeExpressions`,
`CodeGenerator`, the Arrow cache serializer). Spark's own suites run with the
engine off; a change confined to Varka's files cannot change what they see.

## 2. The change

**A scope per change** (`sparktestsupport.modules.varka_change_scope`): from the
changed files, after the ignored ones are dropped, a change is `spark` if any
file is neither Varka's own nor a Varka document, `scoped` if every file is
Varka's and at least one is code, `docs` if the only files are the documents
the docs job already checks, `none` otherwise. "Varka's own" is
`is_varka_scoped`: a basename containing `varka`, a `varka/` path component,
`sql/varka/`, `dev/varka_*`, the `varka-*` workflows. The six shared hooks and
the build's own workflow fail that test by name, which is the point: touching
them is a Spark change. Both functions carry their doctests.

**The precondition** asks the scope once, and for `scoped` sets `build`,
`build-core-utils`, the PySpark flags, TPC-DS, Docker and the Java 25 build to
false and a new `varka-scoped` flag to true. Everything else - the engine,
bench, assembly and docs jobs, the linters - keeps its own gate.

**The job** `varka-scoped` runs two entries in parallel: `catalyst/testOnly
*Varka*` and `sql/testOnly *Varka* *ArrowCachedBatchSerializerSuite`, each on
the same checkout and caches the module matrix uses. These are the suites the
matrix would have run for the change plus the thousands it would have run
around them.

## 3. What this does not do

It does not shorten the matrix for a `spark` change: a Varka pull request that
touches a shared hook, or `build_and_test.yml` itself, runs what it ran. It does
not touch the linters, which take 24 minutes for a documents-only change and
are a question of their own. It does not classify by content, only by path;
a Varka-named file that reaches into Spark would defeat it, and the review is
what catches that, as it does today.

## 4. Predictions, before the runs

1. A change confined to Varka files runs, besides the linters and the four
   Varka jobs, exactly the two `varka-scoped` entries, and finishes in about a
   third of the matrix's wall time: the catalyst entry near the matrix's
   catalyst shard less the non-Varka suites, the sql entry within the hour.
2. A documents-only change runs the docs checks and the linters and nothing
   else.
3. This pull request's own run, which changes the workflow, runs the full
   matrix - `spark` - as it should.

## 5. Outcome

*Filled in from the fork's runs once they have happened.*
