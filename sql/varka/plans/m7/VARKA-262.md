# VARKA-262: Vanilla Spark as the oracle, at random

## 1. Where this came from

Row 262 of `m7/PLAN.md` (2.2), from the plan review and `m7/READING.md` 1: the contract is
Spark's answer, and no randomized test holds Varka to it. `VarkaIrFuzzSuite` and the composition
fuzzer compare kernels with Varka's own reference evaluator; Spark is the oracle only for fixed
shapes (`VarkaDifferentialSuite`'s hand-chosen tests, and `VarkaCoverageDifferentialSuite`, each
coverage row alone over three fixtures). The row asks for seven things; this task builds the
first four and records the rest as rows, because they are different mechanisms and each is a
result by itself:

| part of the row | here |
|---|---|
| random projections and filters, Varka off against on, random data with nulls, ANSI on and off | **built** |
| a failure shrunk to its smallest failing subtree (row 277's shrinker) | **built** |
| the shrunk failure saved, the saved reproducers replayed in PR CI | **built** |
| one arm in the nightly | **built** |
| `NOT p` and `p IS NULL` beside each predicate, projections under the three partitions | **built** (the partition check) |
| each fused output forced to decline in turn, the answer unchanged | row 296 |
| filters that select known rows (PQS), and nested Catalyst trees with type coercions | rows 297 and 298 |

The oracle is Gluten's, Varka off against on, with Comet's error comparison: both engines
throwing the same error class passes; one throwing fails.

## 2. The admission check, done

Two questions decide whether the harness is worth building: is it cheap enough to run, and can
it find a wrong answer. A throwaway suite (not committed) drew random compositions from the 92
rows of `sql/varka/coverage.json` - one to five projections, and a `WHERE` of one to three
predicates joined by `AND` or `OR`, some negated - over twelve rows of random data with nulls
(a null rate of 0, 20, 50 or 100% a draw), and ran each through the two sessions of
`VarkaSharedSessions` under ANSI off and on, on 8 October 2026 at `c6cf8e9831c`:

| run | compositions | comparisons | fused | both errored | disagreed | time |
|---|---:|---:|---:|---:|---:|---:|
| seed 7 | 3000 | 6000 | 3370 (56%) | 1682 (28%) | 0 | 194 s |
| seed 3, with `datediff`'s operands swapped in the calendar compiler | 300 | 600 | 350 | 154 | 4 | 36 s |

Three things follow, and the second and third change the design.

1. **It is cheap**: about 31 comparisons a second, so a PR-CI arm of a few hundred compositions
   costs ten seconds and the nightly can afford tens of thousands.
2. **It finds a wrong answer, but slowly.** The planted bug was found at composition 254 of 300,
   in two compositions: a uniform draw over 92 rows rarely picks a given one, so a bug in one
   row waits a long time. The draw must be stratified: every row appears at least once in every
   pass of about thirty compositions, the rest of each composition being random.
3. **ANSI-on comparisons are mostly error against error.** Every one of the 1682 double errors
   is in the ANSI-on half: more than half of those compositions meet a row that overflows or is
   invalid, and an error says nothing about the other rows' values. Half the compositions must
   draw from a *safe* pool (months 1 to 12, dates and magnitudes inside every guard) so that the
   ANSI-on arm compares values, and half from the full pool so that it compares error classes.
   The unsafe half is also where the interesting disagreements are: it is the half that tests
   whether the kernel declines the batch in the cases Spark raises.

What the check would have rejected: a draw that is uniform over the rows, or data drawn only from
the unsafe pool.

*Correction, made while building the real harness.* The throwaway fixture of this check lacked
the three interval columns the coverage table names (`ymm`, `ymy`, `ym`), so every composition
that read one failed to analyze, identically on both sides, and was counted as a double error.
The claim in 3 above that all 1682 double errors were in the ANSI-on half was an assumption I did
not check, and was wrong; and the harness was blind to a quarter of the table. The numbers that
stand are the real harness's, in 9. The two design conclusions survive them, the stratified draw
(a planted bug waited 254 compositions under a uniform one) and the safe pool, which makes the
ANSI-on arm compare values (it errors on both sides in none of 10,000 cases, where the full pool
does in 16%).

## 3. The design

### 3.1 A case

`Case`: a list of outputs (SQL strings, from the coverage rows' `executable` column), a
list of conjuncts joined by `AND` or `OR` (each possibly negated), the data (twelve columns by
`n` rows, each cell a SQL literal or `NULL`) and the ANSI setting. It renders to one SQL
query and one fixture `SELECT ... FROM VALUES`, so a case is text and needs no serialisation.

### 3.2 The check

For each case the fixture is registered and cached in both sessions (the Arrow cache is what the
Varka nodes read), the query run in both, and the outcomes compared: the rows sorted as strings,
or the error class (`SparkThrowable.getCondition`, else the exception class) when a side throws.
A case counts toward the *fused* total only when the Varka session's plan has a Varka node; a
case that did not fuse still passes, since the interesting question there is whether Varka left
it alone. Two further queries per filter case are the partition check: the rows of `WHERE p`,
`WHERE NOT p` and `WHERE p IS NULL` together are the rows of the unfiltered query, on both
engines (ternary logic partitioning), which catches a three-valued-logic slip no single query
shows.

### 3.3 The draw

A seeded `Random` per composition, as the other fuzzers. Stratified as 2 requires: composition
`k` of a pass includes row `k mod 92` of a shuffled order as one of its outputs or conjuncts, the
rest drawn uniformly. The data is `safe` or `full`, alternating by iteration: a safe case is only
rows inside every guard and ANSI check; a full one adds up to two hostile rows, each with one to
three cells at an extreme (a first draw made every row hostile, and 70% of the full cases failed
on both engines, which compares the class of an error and nothing else). A null is typed, as the
fixtures of `VarkaCoverageDifferentialSuite` type theirs: a bare `NULL` in every row of a column is
`VOID`, which found the bug of 9 and then hid it behind an accident of the fixture.

### 3.4 The shrinker and the reproducers

A disagreement is shrunk by `VarkaShrinker.ddmin` (row 277) over, in turn, the outputs, the
conjuncts and the data rows, each candidate kept only if it still disagrees *in the same way*
(the same kind: values differ, one side throws, the error classes differ). That is delta debugging
over the parts, not over expression subtrees: the coverage rows are the grammar's leaves, so
the outputs *are* the subtrees at this level, and shrinking inside a row's own expression waits
for row 298's nested trees.

The shrunk case is written as `sql/varka/fuzz/spark/<name>.sql`, a file of three parts a reader
can run in `spark-sql`: a header of comments (the seed, the iteration, the ANSI setting and the
kind of disagreement), the fixture, the query. `VarkaSparkReproducerSuite` replays every file:
a file marked `status: regression` must now agree, one marked `status: known <row>` must still
disagree (so the list cannot go stale, as `known_failures.tsv` and the skip list do). The
directory starts empty: no disagreement is known at this master.

### 3.5 The arms

* **PR CI**: `VarkaSparkFuzzSuite` at a few hundred compositions and a fixed seed, so the run is
  reproducible and its time bounded; `-Dvarka.sparkfuzz.iterations` and `.seed` set both.
* **The nightly**: a `spark-fuzz` step in `dev/varka_nightly.sh` at tens of thousands of
  compositions with the day's seed, under `dev/varka_deadline.sh` like the other steps.

### 3.6 What is deliberately unchanged

* `VarkaDifferentialSuite`, `VarkaCoverageDifferentialSuite` and the fixtures they pin: this
  adds randomness over the same sessions and does not replace the fixed shapes.
* The coverage table: it is the grammar's source, read as `VarkaCoverageDifferentialSuite`
  reads it, so a row added to the table is drawn here with no change to this suite.
* The compiler and the emitter. This task changes no product code.

### 3.7 Registered op counts

None move; no emitted code changes.

## 4. Files

| file | what |
|---|---|
| `sql/core/src/test/scala/.../execution/VarkaSparkFuzz.scala` | the case, the draw, the check, the shrink and the reproducer text |
| `sql/core/src/test/scala/.../execution/VarkaSparkFuzzSuite.scala` | the PR-CI arm and the nightly's entry |
| `sql/core/src/test/scala/.../execution/VarkaSparkReproducerSuite.scala` | the replay and its stale rule |
| `sql/varka/fuzz/spark/` | the saved reproducers, empty |
| `dev/varka_nightly.sh` | the `spark-fuzz` step |
| `sql/varka/skills/testing-and-debugging.md` | how to read a failure and add a reproducer |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-262.md` | the row, rows 296 to 298 and this plan |

## 5. Tests, and what each is for

| test | what it catches that no other would |
|---|---|
| the fuzz suite itself, at a fixed seed | a disagreement between Varka on and off over a random composition and random data |
| a planted-bug test: with one coverage row's result corrupted in the *oracle's* copy of the check, the harness reports it and the shrinker reduces it to one output, one conjunct at most and a few rows | the harness being blind. Done on the check, not on the compiler, so it needs no compiler change and runs in CI |
| the stratified draw: every row appears in a pass of the draw | the sensitivity of 2.2 regressing |
| the reproducer round trip: a case rendered to a file, parsed and replayed | the text form drifting from the parser |
| the stale rule: a `known` file that now agrees fails | a list that outlives its bugs |

## 6. The measurement

Not a timing: the harness's yield, recorded in section 9 - compositions a second, the fused and
both-error fractions on the safe and the full pool, and the compositions needed to find the
planted bug of 2.2 once the draw is stratified.

### 6.1 Predictions, registered before the run

1. **Stratified, the planted `datediff` swap is found within one pass** (92 compositions at
   most), against 254 uniform.
2. **On the safe pool the both-error fraction of the ANSI-on arm falls from 56% to under 5%**,
   the fused fraction staying near 56%.
3. **No disagreement at this master** over 20,000 compositions: the row engine and the kernels
   agree on everything the table can compose. A disagreement would be a finding, recorded as a
   row.
4. **The shrinker reduces a planted failure to one output, no conjunct and at most three
   rows.**

## 7. Risks

1. **A bug hidden by the ghost fallback.** A kernel that throws falls back to the row engine
   per batch and the answer is right, so this harness cannot see it; row 275 is the one that
   makes such a fallback fail a test. A kernel that answers wrongly without throwing is what
   this finds.
2. **A false positive from the harness**, such as a comparison that depends on row order or on
   a float's last digit. Rows are sorted as strings and no float is drawn; a disagreement is
   re-run once before it is believed.
3. **The data pool never reaches a guard.** The pools are the fixtures' (`VarkaCoverageDifferentialSuite`),
   which were chosen to; random picks from them reach combinations the fixed fixtures do not,
   but not values outside them. Row 297 and a wider pool are the follow-up.

## 8. Sequencing

1. This plan, with the rows marked.
2. The case, the draw and the check, and the suite at a fixed seed.
3. The shrinker and the reproducer files, with the replay suite.
4. The partition check.
5. The nightly step and the skill, then section 9.

## 9. Outcome

Done on 8 October 2026.

**What was built.** `VarkaSparkFuzz` (the case, the stratified draw, the comparison, the shrinker
and the reproducer text), `VarkaSparkDifferential` (the check both suites share),
`VarkaSparkFuzzSuite` (the PR-CI arm and the entry for long runs), `VarkaSparkReproducerSuite`
(the replay with its stale rule), `sql/varka/fuzz/spark/` (one known reproducer), a `sparkfuzz`
step in `dev/varka_nightly.sh`, and the skill note. No product code changed.

**It found a bug on its first long run.** `sql/varka/fuzz/spark/void-column-fallback.sql`,
shrunk by the harness from iteration 93: `SELECT greatest(l, l2) AS c0, date_add(d, i) AS c1 FROM
fz WHERE NOT (i = 5)` over four rows, ANSI off, where two columns are `VOID` (all `NULL`) and one
row's `i` is 100000, which takes `date_add` past the kernel's guard so that the batch is declined.
Spark answers; Varka raises `UNSUPPORTED_DATATYPE: Unsupported data type "VOID"` out of the
filter's row fallback, `VarkaFilterEvaluatorFactory.PartitionFilterEvaluator.converter`
(`VarkaFilterExec.scala:200`), which builds a `RowToColumnConverter` over the child's schema and
has no converter for `NullType`. A relation can carry a `NullType` column (`SELECT NULL AS x`), so
this is a crash of the fallback the evaluator relies on to be safe, not an artifact. It is row 299,
and the reproducer is `status: known VARKA-299`, so the replay suite fails when it is fixed and the
file must be turned into a regression.

**The harness's yield**, with the typed fixture, at `c6cf8e9831c` (the master with 215):

| run | compositions | fused | errored on both sides | disagreements | time |
|---|---:|---:|---:|---:|---:|
| seed 20261008 | 20000 | 18736 (94%) | 1592 (8%) | 0 | 21 minutes |

By pool: the safe cases (10,000) errored on both sides in none, and the full ones in 16% (803 of
5073 with ANSI off, 789 of 4927 with ANSI on). 200 compositions, the PR-CI arm, take about ten
seconds.

**Predictions scored.**

1. **Refuted.** Stratified, the planted `datediff` swap was found at iteration 102 on the first
   pool and 93 on the second, not within one pass of 92: the first time a pass forces the row the
   data may not show the difference (all `NULL`, or `d` equal to `d2`). It is still two and a half
   times faster than the uniform draw's 254.
2. **Held, on the right numbers.** The safe pool's ANSI-on arm errors on both sides in none of
   5003 cases. The baseline of 56% in 2 was the blind fixture's.
3. **Refuted, usefully.** The first 20,000 found a disagreement at iteration 93 (the bug above).
   With NULLs typed, and the bug recorded, the same run finds none.
4. **Held for the planted failure, not for the real one.** The planted bug shrank to one output,
   no conjunct and one row. The real one shrank to two outputs, a conjunct and four rows: each of
   those is needed (the second output's `date_add` is what declines the batch, the conjunct is what
   makes the plan a filter, and the rows carry the `VOID` columns), so it is 1-minimal, not
   within the three rows predicted.

**What the work taught.** A comparison that cannot fail looks like agreement: the first harness
counted unanalyzable queries as agreeing errors. The safe pool and the typed NULLs are as much
the harness as the draw is, and the suite now reports the both-errored count by pool so that a
pool that has stopped comparing values shows. And the stratified draw needs more than one pass to
be sure of a data-dependent bug; the nightly's 50,000 compositions are about 540 passes.

**Left for later.** Rows 296 to 298, as planned. Row 299, the bug. The shrinker reduces the parts
of a composition and not the expressions inside a coverage row. A composition's data is twelve
rows, so a bug that needs a larger batch or a particular lane position is out of reach; the
nightly's seed varies the draw but not the batch size.
