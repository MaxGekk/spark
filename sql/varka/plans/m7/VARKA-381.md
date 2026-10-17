# VARKA-381: The Spark-level differential compares errors without a race

*Row 381 of `m7/PLAN.md`, added by the owner on 16 October 2026 from `VARKA-298.md` 9.3. Planned
on 16 October 2026 on branch `varka-298` at `c378cd4093d` (pull request #871, not yet merged),
which owns the four files this task changes.*

## 1. Where this came from

The row: "The Spark-level differential's view has two partitions (`local[2]`), so two listed
errors on rows in different partitions race and the query raises either one, on either engine and
from run to run; VARKA-298 absorbs it by rerunning such a disagreement three times. [...] Make the
comparison deterministic - one partition for error cases, or the set of conditions each row raises
compared instead of the first - and drop the rerun." Done when the comparison cannot depend on
which task fails first, the rerun is gone, and the census still raises every expected error and
finds no disagreement.

`VARKA-298.md` 9.3 is the evidence: the reproducer of seed 20261106, iteration 4259 (`d - ym`
overflowing on one row, `make_date(3, 3, unix_date(d))` out of range on another), replayed eight
times in one JVM, agreed five times and disagreed three, with either engine raising either
condition. Four of five 300-composition runs at the default seed met such a race and passed on the
rerun. The rerun is a filter on a verdict, not a comparison: it believes a disagreement only if
three more runs repeat it, so a race that agrees by luck on the first run passes unseen, and a
race that disagrees four times in a row (one chance in about 80 at 9.3's rate) fails a pull
request that changed nothing.

## 2. The admission check, done

Read on `varka-298` at `c378cd4093d`, then probed with a test that stays (5.1):

* **Where the partitions come from.** `VarkaSparkDifferential.cacheFixture` registers the fixture
  (`SELECT ... FROM VALUES <12 rows>`) as `fz` in both sessions and caches it. The fixture plans as
  a `LocalTableScanExec`, whose parallelism is `min(rows, leafNodeDefaultParallelism)`, which on
  `local[2]` is 2, sliced in order: rows 0 to 5 and 6 to 11. The cache keeps the two partitions.
  Probed: the cached view is 2 partitions, 1 under `coalesce(1)`, 1 for a single row taken with
  `offset(k).limit(1)`, and `SELECT year(d) FROM fz` has a Varka node over each.
* **Whether a race is visible before it decides anything.** Whether a row raises does not depend
  on where it sits: every output and conjunct is row-wise, so a query over the two partitions
  raises if and only if it raises over one, and only *which* condition it names can race. So the
  first run of a check tells, deterministically, whether the check is an error case; a check whose
  queries all answered is unaffected by the partitions and stays as it is.
* **How many checks would pay a second run.** The census tables give the share of compositions
  erroring on both sides: 14 to 68 of 5,000 without nested trees (`VARKA-372.md` 9), 48 to 110 of
  5,000 with them on half the compositions (`VARKA-298.md` 9.1). A check in which any query raised
  is a few more (one side only, a forced run, a partition sub-query), so about 0.5 to 3% of
  compositions.
* **How errors are compared.** `VarkaSparkFuzz.difference` compares the condition alone (`error
  classes differ: a, b`); the shared check of VARKA-317 (`VarkaMaybeThrows`) compares condition,
  SQLSTATE, parameters, message and class, and `VARKA-317.md` 9 leaves the fuzzer's own `Outcome`
  to a task that owns these files. This task does not move it (3.3): what races is the choice of
  the row, and a richer comparison of one raise races just the same.
* **What the per-row arm costs.** "The set of conditions each row raises" needs each row run on its
  own in both sessions, through the cache, since an inline `VALUES` row is a `LocalTableScan` the
  Varka session does not fuse over and would compare the row engine with itself. For a 12-row
  composition that is twelve cached views and twelve checks for each error case: some 12 to 25
  times the rerun's cost, on 0.5 to 3% of compositions. It buys more than determinism: the values
  of the rows that do not raise are compared, and which condition each row raises. It also changes
  what Varka sees, one-row batches instead of six-row ones.

What the check would have rejected: forcing one partition on every composition (it would take the
two-task runs out of every check, `VARKA-298.md` 9.3's last sentence), and a rerun triggered only
by a disagreement (two engines can agree at two partitions by luck too).

## 3. The design

### 3.1 A check that met a raise runs again where no task races another

`VarkaSparkDifferential.disagreement` runs its comparison once over the view as cached (two
partitions), recording whether any query over the view raised. If none did, that is the verdict.
If one did, the counters are put back and the whole check (the main pair, the forced runs, the
pivot, the partition check) runs again in one of two arms, `VarkaSparkFuzz.ErrorRerun`:

* **`OnePartition`** (the default): the view cached through `coalesce(1)`, so each query is one
  task and raises the first raising row's condition in the rows' order. Two queries more per error
  case, and more for the forced runs and the partition check that case takes.
* **`EachRow`** (`-Dvarka.sparkfuzz.errorRerun=eachRow`): each row alone as the view, its check
  run in turn up to the first disagreement; the census counts the composition once (as its first
  run counted it) and each condition and side the rows raised once. The pivot is checked by the
  first run only, since a filter rectified to select it says nothing about another row alone.

A difference the first run showed that names no condition (`only the row engine throws`,
`values differ`, `partition broken`) and that the rerun does not show is not dropped: which side
raises does not depend on the partitions, so such a difference is one the two partitions' batches
made, and it is reported as `at two partitions only, <kind without its condition>`
(`VarkaSparkFuzz.withoutConditions`).

The default is the cheaper arm, as the owner's rule for competing variants asks; the per-row arm
stays under the property as the more informative one, for a campaign that wants each row's
condition.

### 3.2 The warmed arm

`warmedDisagreement` runs a sequence of Varka runs against one row-engine answer, with the warm-up
state carried between them, so it cannot rerun a check whole. It asks the row engine first; if that
raised, the view is cached as one partition before any Varka run, and the same over the later data.
Where only Varka raises, its condition may still be a race's, but that is a disagreement whichever
condition it names. It keeps drawing the coverage rows alone (`drawRows`), for 9.2 of VARKA-298
(row 380), not for the race.

### 3.3 What is deliberately unchanged

* The fuzzer's `Outcome` (the condition alone, keyed by the expected-errors list); moving it onto
  `VarkaMaybeThrows.Raised` stays as `VARKA-317.md` 9 recorded it.
* The view's two partitions for every check that raises nothing.
* The nightly's `-Dvarka.sparkfuzz.nested=false`, which row 380 removes.
* Row 380's files (`VarkaExpressionCompiler`, `VarkaConjunctOrder`, `VarkaDifferentialSuite`).

Removed: `racingErrors` and its test, the suite's three-rerun filter and `withoutCounting`, the
shrinker's keep-only-if-twice for racing kinds, and the info line counting absorbed races, which
becomes a line counting the checks compared again.

### 3.4 Registered op counts

None: no kernel changes.

## 4. Files

| file | what |
|---|---|
| `sql/core/src/test/scala/org/apache/spark/sql/execution/VarkaSparkDifferential.scala` | the rerun of 3.1, the counters' snapshot, `cacheFixture`'s view shape, the warmed arm's one-partition row answer |
| `sql/core/src/test/scala/org/apache/spark/sql/execution/VarkaSparkFuzz.scala` | `ErrorRerun`, `errorRerunOf`, `withoutConditions`; `racingErrors` removed |
| `sql/core/src/test/scala/org/apache/spark/sql/execution/VarkaSparkFuzzSuite.scala` | the rerun filter removed, the info line, three tests (5) |
| `sql/core/src/test/scala/org/apache/spark/sql/execution/VarkaSparkReproducerSuite.scala` | unchanged unless the replay needs it: it calls `disagreement` and takes the rerun with it |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-381.md` | row 381's cell; this file |

## 5. Tests, and what each is for

1. **The view is two partitions, one coalesced or a row alone, and fuses in each** (new): the
   admission probe of 2, kept so that a change to the sessions' parallelism or to the fixture's
   plan that made the rerun compare nothing fails here.
2. **Two partitions that raise different listed conditions compare the same on every run** (new).
   Under ANSI, row 0 (partition 0) raises in `make_date(2021, i, 1)` with month 13 and row 6
   (partition 1) overflows `l + 1`. A UDF in the `WHERE` holds one partition's task, chosen per
   engine, until the job kills it, so the test decides which task fails first. The comparison
   before this task (one run per engine over the two partitions) answers for the four choices
   agree, `error classes differ: ARITHMETIC_OVERFLOW, DATETIME...`, the mirror, agree: its verdict
   is the timing's. `disagreement` under each arm, twice for each choice, finds no disagreement,
   and the census records the month condition alone on both engines under `OnePartition` and both
   conditions under `EachRow`. Catches a rerun that stopped running, ran at two partitions, or
   counted the first run's race.
3. **A difference only the two partitions show keeps which side raised and not the condition**
   (new, pure): `withoutConditions` and `errorRerunOf`.
4. **The suites as they are**: `VarkaSparkFuzzSuite` (200 compositions, the warmed arm's 8, the
   planted tests) and `VarkaSparkReproducerSuite`, whose five `known 380` reproducers must keep
   their kinds (`only the row engine throws: <condition>`) through the rerun.
5. **The census at volume** (counts only): the nightly `sparkfuzz` step's size, 50,000
   compositions at three seeds with `-Dvarka.sparkfuzz.maxFailures` lifted, without nested trees
   as the nightly runs today. The expected-errors entry that only nested trees reach
   (`INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION`, `VARKA-298.md` 9.4) and the race itself
   live in the nested compositions, so a second census draws them, on this branch merged with
   row 380's (`varka-380`, in a scratch worktree, never committed), which removes the
   disagreements of 9.2 that would otherwise be all it reported.
6. **The per-row arm**: the suite's 200 compositions and the reproducer suite under
   `-Dvarka.sparkfuzz.errorRerun=eachRow`.

## 6. The measurement

No performance claim; the cost is test time. The 200-composition run at the default seed before
(`varka-298`) and after, alternating, by minimums, and the number of checks compared again from
the census's info line, read against the nightly's `sparkfuzz` step.

### 6.1 Predictions, registered before the run

1. **The census finds no disagreement and raises every listed condition**: without nested trees,
   every entry but `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION`; with them (and row 380), all
   of them.
2. **Checks compared again: 0.5 to 3% of compositions** (2), at most 1.5% without nested trees.
3. **Added time under 3% of a run under `OnePartition`**: about one more check per raising
   composition, and harness overhead ran 1.5 to 2 times the arithmetic in the calibration table
   (`skills/testing-and-debugging.md`); under `EachRow` 15 to 50%.
4. **The new tests add under 20 s** to the suite, most of it the planted test's sixteen checks.

## 7. Risks

1. **One partition shows a deterministic disagreement between the engines**: the row engine raises
   at the first raising row in scan order, Varka evaluates fused outputs batch-wise. If the rerun
   shows a stable `error classes differ` at one partition, it is a Varka finding about raise order
   inside a batch, not a harness race, and gets a row rather than a workaround. The census (5.5)
   would show it.
2. **A difference only the two-partition batches made is hidden by the rerun**: reported as `at
   two partitions only` (3.1), and test 3 holds the rule.
3. **The planted timing does not hold** on a loaded machine: the held task waits for its kill, not
   for a fixed time, so the order does not depend on the load; a ten-second cap bounds a hang.

## 8. Sequencing

1. This plan and the row marked planned.
2. The rerun, the warmed arm's row answer, the removals and the three tests; both suites green.
3. The census and the per-row arm's runs; section 9 and the row's cell.

## 9. Outcome

Done on 17 October 2026, on `varka-298` (merged in up to `509bd978907`).

**What was built.** As 3.1 and 3.2 planned, with four changes from the self-review (below):
`disagreement` reruns a check in which any query raised over the rows as one partition, or under
`-Dvarka.sparkfuzz.errorRerun=eachRow` over one partition and then each row alone; the arm and the
stripping of a race's condition are a Java enum, `VarkaErrorRerun` (sql/core's test tree), not
Scala; the warmed arm asks the row engine over one partition once it raised. The rerun of
VARKA-298 9.3, `racingErrors`, `withoutCounting`'s old form and the shrinker's keep-if-twice are
gone. Three tests: the view's partitions and fusion (5.1), the planted race (5.2), the pure rules
(5.3).

*Corrections to 1-8.*

* **Section 2's "a query over two partitions raises if and only if it raises over one" is false.**
  The cached scan skips a batch whose column statistics fail a conjunct (`InMemoryTableScanExec`'s
  batch pruning), so whether a raising row is evaluated at all depends on the batch it sits in.
  The per-row arm found it on the `known 380` reproducer of seed 20261104, iteration 2199: over its
  three rows as one batch the row engine raises on the row with `i` = 3840, but that row alone has
  `d` = 2190-08-04, outside the `IN` list's two dates, so its batch is skipped on both engines and
  neither raises. The design survives: what the race decides is still only which condition; a
  check whose queries all answered over two partitions is compared there; and a difference only
  the two-partition batches show is reported as `at two partitions only, ...` (risk 2), not
  dropped.
* **The per-row arm runs over one partition first** (the review's finding 4): run alone, it never
  checked the pivot and its one-row batches lost the 2199 reproducer's disagreement. A reproducer
  found under it carries the arm in its note, and the replay takes the arm from the note.
* **The census counts each condition any run of a check raised, once** (finding 5): over one
  partition only the first raising row's condition is raised, so counting the rerun alone would
  have hidden conditions the two-partition run met. Which extra condition the first run adds may
  be a race's choice; the census lines are counts, not verdicts.

**The planted race (5.2).** With the test choosing which task fails first, the comparison before
this task answered, for the four choices, agree, `error classes differ: ARITHMETIC_OVERFLOW,
DATETIME_FIELD_OUT_OF_BOUNDS.WITHOUT_SUGGESTION`, the mirror, agree; `disagreement` under both arms
answered agreement in all sixteen checks. The test takes 9 s on a quiet machine and up to 15 s
beside other builds.

### 9.1 The census

Six JVMs, one per seed and arm, four pinned cores each, 50,000 compositions at seeds 20261017 to
20261019 (the next three nightly seeds), `-Dvarka.sparkfuzz.maxFailures` lifted. The first three
are the nightly's `sparkfuzz` step as it runs today (`-Dvarka.sparkfuzz.nested=false`) on this
branch; the other three draw the nested trees, on this branch merged with `varka-380` in a scratch
worktree that was never committed or pushed, since row 380's fix is what lets the nightly draw
them again.

| | without nested trees (this branch) | with nested trees (this branch and row 380) |
|---|---|---|
| compositions | 150,000 | 150,000, of which 74,983 nested |
| erroring on both sides | 561, 597, 645 | 928, 935, 988 |
| compared again because a query raised | 561, 597, 645 (1.1 to 1.3%) | 935, 941, 993 (1.9 to 2.0%) |
| differed at two partitions only | 0 | 0 |
| disagreements | 0 | 2 (9.2) |
| listed conditions never raised | `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION`, which only nested trees reach (`VARKA-298.md` 9.4) | none |
| unexpected conditions | 0 | 0 |

### 9.2 Two findings the deterministic comparison exposes (Varka's, not this task's)

Both replay at their seed and iteration on this branch alone, without row 380, and every time, so
neither is a race and neither came from row 380. Reproducers are kept in this task's scratch area,
not in `sql/varka/fuzz/spark/`, which this task does not own.

* **Seed 20261017, iteration 22897 (LEGACY): Varka raises where Spark's earlier conjunct rejects the
  row.** `SELECT t + INTERVAL '0 00:00:00' DAY TO SECOND AS c0 FROM fz WHERE NOT (year(d) = 2021
  AND i > 0) AND ((ymy <= (INTERVAL '1' YEAR - ym)))` over two rows; on the row with `d` in 2021,
  `i` NULL and `y` = -178956970 the first conjunct is NULL, so Spark's `Filter` stops there, while
  Varka evaluates the second, whose year-month subtraction overflows in every mode: `only Varka
  throws: INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION`. The direction row 380 did not cover (380
  is Spark raising where Varka's fused conjunct rejects first).
* **Seed 20261018, iteration 47807 (ANSI): the order of raises inside a batch.** `SELECT
  make_date(2021, i, 1) AS c0 FROM fz WHERE (((ymy + ym) >= ym))` over two rows. Over one
  partition Spark evaluates the filter and then the projection row by row and raises the first
  row's month (`i` = -2821); Varka's filter runs over the whole batch first and raises the second
  row's interval overflow (`y` = 178956969). Risk 1 of 7, met: a deterministic `error classes
  differ` at one partition. Over two partitions the two rows sit in different tasks, so before
  this task it was a race the rerun absorbed.

At two in 74,983 nested compositions, a nightly of 50,000 that draws nested trees (once row 380
lands) meets one about two nights in three, so these need rows before the switch is removed.
Whether the second is a bug or an allowed difference (which of two raising rows an engine names
first) is the owner's call; vanilla Spark's answer is the oracle this harness holds both to.

### 9.3 Predictions scored

1. **Partly refuted.** Without nested trees, no disagreement and every listed condition raised but
   the nested-only one, as predicted. With nested trees, every listed condition was raised, but
   two disagreements, both Varka's (9.2).
2. **Held**: 1.1 to 1.3% compared again without nested trees, 1.9 to 2.0% with them.
3. **Held**: the 200-composition run at the default seed, five times each, alternating, on four
   pinned cores of an otherwise idle machine, took 47.9 to 51.9 s before and 48.5 to 56.8 s after
   (whole JVM): 47.9 against 48.5 s by minimums, under the noise; the run meets four raising
   compositions. The census adds one check per raising composition, 1.1 to 1.3% of the nightly
   step's compositions. Ratio under 1 against the estimate of under 3%; the calibration entry is
   owed to `skills/calibration/`, outside this task's files.
4. **Held**: the three tests take about 9 s together, the planted race nearly all of it.

### 9.4 The self-review

`/code-review high` on the diff against `varka-298`: ten findings, nine taken. (1) An unlisted
condition seen only over two partitions was dropped: now kept whole. (2) A forced kind under the
`at two partitions only` prefix lost its force in the reproducer: forced kinds are found anywhere
in the text. (3) A reproducer found under the per-row arm was replayed under the default: the arm
goes in its note. (4) and (5): the corrections above. (6) A failure's shrink counted its checks in
the census: `failureText` runs uncounted. (7) The planted test's wait was per row and its function
stayed registered: once per task on marked rows, five seconds at most, dropped after. (9) The
comments' citation of this section, which now exists, and section 2's claim, corrected above.
(10) The helpers in Scala against the house rule: `VarkaErrorRerun` is Java. Not taken, (8): the
warmed arm's Varka-only raise over two partitions still names a race's condition. Its kinds are
neither shrunk nor replayed by kind (a `warmed` reproducer is refused by the replay), the case is
a disagreement whichever condition it names, and naming a fixed one would mean repeating the
warm-up sequence over one partition.

### 9.5 Left for later

* The two findings of 9.2, for the owner to make rows.
* The calibration entry for 6.1 #3 (`skills/calibration/`).
