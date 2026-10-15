# VARKA-316: A troubleshooting runbook for Varka

## 1. Where this came from

Row 316 of milestone 7, added by the owner on 12 October 2026 with the other errors rows (313 to
319): "A troubleshooting runbook in `docs/sql-varka.md`: a decision tree from the symptom - a wrong
answer, a slow query, a crash - to the steps ... Each error condition's entry names its reader and
what that reader does, after Druid's personas (user, operator, developer). Done when each branch
has been walked once on a planted fault of its kind and the page says what each step showed."
The row was written last of the seven because it points at the others: 313 (the decline metrics and
the filter's `EXPLAIN`) and 315 (the reproduction file and its replay) are merged, so the runbook
can describe features that exist. Today a reader with a symptom has to know which of the page's
sections, a dozen tools and four skill files to open; nothing connects a symptom to a step.

## 2. The admission check, done

What exists today for each branch of the tree, and what does not (read from the code and run, not
from the plans):

| branch | step | exists | note |
|---|---|---|---|
| any | switch Varka off | yes, as a whole: `spark.sql.codegen.varka.enabled=false` | row 318's per-family switch is not built; the runbook names this one and says so |
| wrong answer | compare with Varka off | yes | |
| wrong answer | `dev/varka_emit.sh` on the expression | yes | prints the IR, the shape hash, methods and op counts; with `--options` the variant |
| wrong answer | shrink with the fuzzers' shrinker | **only inside the fuzz suites** | `VarkaShrinker` is called by `VarkaIrFuzzSuite` and the composition fuzzer on a case they drew; there is no command that takes a user's query or data and shrinks it. The runbook says so and sends the developer to write the failing expression as a case |
| slow query | row 313's metrics, per reason | yes | in the SQL UI and on the node's `metrics` |
| slow query | verbose `EXPLAIN` | yes | names compile-time residuals; a batch declined at run time is **not** in `EXPLAIN` (the line reads `fused`), only in the metrics |
| slow query | a setting that cures a decline | **no** | the only setting is all of Varka; `exactRefusalStatus` is diagnostic. The runbook says the cure is the data or the query, or off |
| crash | the context record | yes | in the `INTERNAL_ERROR` message at planning and in the fallback warning |
| crash | reproduction file and `--replay` | yes | on by default; the warning names the file |
| crash | class dump and `javap` | yes | `spark.sql.codegen.varka.classDumpDirectory` |
| crash | VM flags | in `SKILLS.md` / `skills/the-jit.md` | named, not rerun here except `javap` |
| error conditions | one condition per failure | **only `INTERNAL_ERROR`** | row 314b is open: a class the verifier rejects at planning surfaces as a bare `java.lang.VerifyError` (found in the walk, 9) |

Where it belongs: a new `### Troubleshooting` section in `docs/sql-varka.md` between "Telemetry and
debuggability" (which it points into) and "Execution integration", with an entry in the page's
contents if it has one. What the admission check would have rejected: a runbook step naming a
tool that is not there. Three were found (the shrinker for a user's query, a per-family switch, a
setting that cures a decline) and are written as absent.

The planted faults: no fault injector changes an answer (`misdescribeAdd`, `...WordLiveness` and
`misdescribeDataBytes` all throw). The wrong-answer branch uses `guardDayProducers=false`, an
emit option that removes the guard declining a day outside the calendar lowering's range: the
kernel then answers a far `date_add` offset with a wrapped year. It is a test-only hook (the
option is not a SQL configuration), so the walk runs through a suite.

## 3. The design

### 3.1 The runbook

A decision tree from three symptoms, in plain text, and numbered steps under each. Each step is a
command or a setting and says what it shows. A table of the failures a reader meets (the
`INTERNAL_ERROR` at planning, the fallback warning, the reproduction-file warning, the
memory-violation task failure, the metrics) names the reader of each - the user, the operator, the
developer, after Druid's personas - and what that reader does.

### 3.2 What is deliberately unchanged

No product code. Row 318's switches and 314b's remaining conditions are theirs; the runbook names
the gap in each.

### 3.3 Registered op counts

None; no emitter change.

## 4. Files

| file | what |
|---|---|
| `docs/sql-varka.md` | the Troubleshooting section |
| `sql/core/src/test/scala/org/apache/spark/sql/execution/VarkaRunbookSuite.scala` | the planted walks, printing what each step shows |
| `sql/varka/plans/m7/VARKA-316.md`, `PLAN.md` | this plan, row 316 |

## 5. Tests, and what each is for

`VarkaRunbookSuite`, five tests, one per planted fault. It asserts what the page claims: the
planted wrong answer differs from Spark's and Varka off equals it; a declining query counts a
declined batch under `day-producer` and `EXPLAIN` names a residual entry; a throwing kernel writes
exactly one file whose warning names it; a planning bug raises `INTERNAL_ERROR` with the context.
It fails if the page's description of a step stops being true.

## 6. The measurement

None.

### 6.1 Predictions, registered before the run

1. Every step of the tree runs as written on the first try. (Scored in 9.)
2. The suite takes under 30 s beyond sbt's start.

## 7. Risks

1. A step documented elsewhere does not work as documented: found in the walk, fixed if small.
2. The page restates numbers that drift: it quotes none but the walk's counts, which the suite
   asserts.

## 8. Sequencing

Plan; row; the suite and the page; the outcome.

## 9. Outcome

*15 October 2026.* The page is `### Troubleshooting` in `docs/sql-varka.md`; `VarkaRunbookSuite`
(`sql/core`) walks each branch on a planted fault and prints what each step shows. The output below
is trimmed from its run and from the command-line steps run afterwards on the files it left under
`sql/core/target/varka-runbook`.

### 9.1 The tree

```
wrong answer : Varka off = right? -> A1 compare, A2 dev/varka_emit.sh (masks), A3 fuzz case + shrinker
slow query   : Varka off = fast?  -> B1 counters, B2 per-reason metrics, B3 EXPLAIN, B4 change data/query
crash/error  : INTERNAL_ERROR (planning)     -> C1 context, report
               "failed on this batch" warning -> C2 file, C3 --replay, C4 class dump + javap
```

### 9.2 Walk A: a wrong answer (`guardDayProducers=false`, a test-only emit option)

Query `SELECT year(date_add(d, off)) AS y FROM varka_dates_far_offset`, whose second row adds
-20,000,000 days.

* A1, compare. Spark's own: `[56782, -52735, 2023, null, 1970, null]`. Varka on: `[56782, 49698,
  2023, null, 1970, null]`. Varka off (`spark.sql.codegen.varka.enabled=false`): equal to Spark's.
  The node's `numVarkaBatches / numFallbackBatchesDeclined` read `2 / 0`: nothing declined, so the
  metrics cannot find this fault, only the comparison can.
* A2, the compiled form. `dev/varka_emit.sh "year(date_add(d, off))" --columns d:date,off:int`:
  `VectorMask` 5, 7, 4, 6 in `epilogueDense0`, `epilogueMasked0`, `loopDense0`, `loopMasked0`; with
  `--options guardDayProducers=false`: 0 in all four, and 36 to 38 `IntVector` operations in both.
  Shape hashes `aa2d7bfebc76b893` against `3c681b82df6a84ff`. This is the step that points at the
  guard.
* A3, the shrinker. No fault injector changes an answer and the fuzzers do not draw this fault,
  so it was run on the planted emitter bug the repository proves it on (`misdescribeWordLiveness`):
  `shrunk to lane=int roots=[(isNotNull col:0)] options={misdescribeWordLiveness=true} length=1`
  from a drawn tree of three conjuncts, `(14 runs, 5 ms)`. Whether the fuzzers would find
  `guardDayProducers=false` was not run (finding 3).

### 9.3 Walk B: a slow query (the same query, the guard on)

* B1: `numVarkaBatches = 1`, `numFallbackBatchesDeclined = 1`, `numVarkaRows = 3`,
  `numFallbackRows = 3`; `fallbackTime` is not registered on this row-out node.
* B2: `day-producer = 1`; `day-range`, `derived-input`, `forced`, `input-bound`, `int-overflow`,
  `make-date`, `month-count`, `unrecognised`, `value-range` all `0`.
* B3: `EXPLAIN FORMATTED` reads `Varka [1]: [y: fused]` for the query that declines at run time: a
  decline is invisible to `EXPLAIN`. A projection with an unsupported entry reads `Varka [2]: [a:
  fused, b: residual (unsupported expression: length(CAST(varka_dates.i AS STRING)))]`, and a
  filter `... ((varka_dates_far_offset.off % 7) = 1): residual (unsupported expression: ...)`.
* B4: no setting changes a decline; the page says so.

### 9.4 Walk C: a crash

* C1, planning (`VarkaExpressionCompiler.setFailForTesting`): `[INTERNAL_ERROR] Varka's compiler
  failed while planning.` then `kind: planning`, `identity: projection [date_add(d#1107, 3) AS
  a#1115] over [d#1107]`, `shape hash: none`, `options: the defaults`, `cause:
  java.lang.IllegalArgumentException: injected Varka compiler failure`, `SQLSTATE: XX000`.
* C2, run time (`misdescribeDataBytes=-4`, the kernel throws in the vector loop): the query answers
  with the row engine's rows; the warning reads `The Varka SIMD kernels
  VarkaFusedProjection_85b89e300101e4e1.java [(addDays col:0 lit:0)] (Varka_ProjectToRow_Stage7)
  failed on this batch; falling back to the per-row path.`, `kind: kernel-failure`, `shape hash:
  85b89e300101e4e1`, `options: misdescribeDataBytes=-4`, `batch: 3 rows, nulls per input [1]`,
  `cause: java.lang.IndexOutOfBoundsException: Masked range check failed ...`, then `Wrote a
  reproduction of this Varka failure (...) to .../varka-repro-85b89e300101e4e1-
  indexoutofboundsexception-pid179269-f3915c.txt; replay it with dev/varka_emit.sh --replay ...`.
  Four warnings for the two tasks' two batches each; one file.
* C3, `dev/varka_emit.sh --replay <file>`: `rebuilt shape 85b89e300101e4e1 ... the emitter built
  the failing class byte for byte / defined and linked the class / running the recorded batch of 3
  rows / the kernel threw`, `REPRODUCED: the same signature`, exit 0. For the verify failure
  (`dropStackMaps`): `verified the class as the admission does: 7 errors`, `REPRODUCED`.
* C4, `classDumpDirectory`: one file, `VarkaFusedProjection_85b89e300101e4e1.class`; `javap -c -p`
  prints the class (`run` branches on the null count to `runDense` or `runMasked`); `javap -v`
  shows `VarkaDebugInfo` = `outputs=[(addDays col:0 lit:0)], numInputs=1, numLiterals=1`, `shape
  85b89e300101e4e1`, `1=col:0 2=lit:0 3=(addDays 1 2)`.

Not walked: the memory-violation row (needs `-Dvarka.sanitizeMemory=true`; row 315's end-to-end
suite walks it to a replay), and the VM-flag steps of `SKILLS.md` (`-XX:+PrintCompilation`,
`dev/varka_c2_report.py`, `--asm`, which needs hsdis): the page names them and does not use them in
a walk, so they are marked here as not run.

### 9.5 Findings

1. **The shrinker has no entry for a user's query.** `VarkaShrinker` runs inside the fuzz suites
   on cases they drew. The page says so. A row suggestion: a command that takes an expression, its
   columns and a data file and shrinks a disagreement with the row engine as the fuzzers do.
2. **A class the verifier rejects at planning is still a bare `java.lang.VerifyError`**, with a
   reproduction file but no `INTERNAL_ERROR` and no context block. That is row 314b's work; the
   page's table says so and the suite asserts it, so 314b will have to change both.
3. **Whether the fuzzers see `guardDayProducers=false` is not known.** It is the one planted
   fault of this kind, and no walk ran the fuzzers over it. A row suggestion with the shrinker
   one above.
4. **No fault injector changes an answer**; the wrong-answer walk uses an option that removes a
   guard and is reachable only from a test (it is not a SQL configuration).
5. **A runtime decline is invisible to `EXPLAIN`**, documented in the page; the metrics are the
   only record.

### 9.6 Predictions, scored

1. **Held with two corrections.** Every step ran as written; the planning branch needed a second
   planted fault (the verifier's) because a compiler bug and a bad class surface differently
   (finding 2).
2. **Held.** The five tests take under 6 s (3.9 s for the first, which builds the session).

### 9.7 The review's corrections

`/code-review high` on the branch, eight findings, all taken: the slow-query test forces the day
guard on over the option matrix's options (it failed by design under `guardDayProducers=false`;
the suite now passes under that configuration and under `arrowBatchSize=1`, by `dev/varka_matrix.sh
--config`); the page claimed assertions the suite only printed, so it now asserts the counts,
reasons, `EXPLAIN` lines and warning text it quotes; the verifier test polls for its file; the
test pinning the defect now accepts any `VerifyError` in the chain so row 314b's conversion will
not break it; the page names exit 2 of `--replay`; the enabled setting is restored, not set to
true; the cleanup is recursive; the page no longer quotes the shrinker's run count and timing or
points readers at the plan.
