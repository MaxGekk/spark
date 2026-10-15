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
