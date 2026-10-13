# VARKA-314: Varka's errors as Spark error conditions, and one context record for every failure

*Row 314 of `m7/PLAN.md`, added by the owner on 12 October 2026 with rows 313 and 315 to 319
(`m7/PLAN.md` 2.4, "Errors and troubleshooting"). Planned on 13 October 2026. The admission check
changed what the row can deliver; section 3.0's decision, taken by the owner the same day, is (B):
no new error names.*

## 1. Where this came from

The row asks for three things:

1. **Every throw site decided** as a user error or an internal one, as Velox (`VELOX_USER_CHECK`
   against `VELOX_CHECK`), Trino (`ErrorType`), DuckDB (`INTERNAL`) and DataFusion (`Internal`)
   split them: a user error is a named condition that says what to do; a kernel or emitter bug is
   `SparkException.internalError` with a request to report it.
2. **One context record, built only when something throws** - the expression's IR, the shape
   key, the options that differ from the defaults - in the manner of Velox's `ExceptionContext`,
   so the per-batch path allocates nothing. Row 315 writes the same record as its reproduction
   file.
3. **The conditions in Spark's error-class registry**, each listed in `docs/sql-varka.md` with what
   to do, and a test that reaches each one and checks it with `checkError`.

Its text names three example cases that reach a user: "a decline that cannot fall back, a
configuration out of range, a kernel that fails to load".

## 2. The admission check, done

Read on 13 October 2026, in a worktree at master `ad544abe129` (after #797).

**121 throw sites in Varka's main code** (`throw new`, `SparkException.internalError`, `require`
in the 93 Java and Scala files of `codegen/Varka*`, `codegen/varka/`, `execution/Varka*` and
`execution/varka/`, comment lines excluded), by when they fire. The count is a grep's, and a
review's slightly different rules counted 124 sites in 94 files; the buckets, not the exact
number, decide the design:

| when | sites | kinds |
| --- | ---: | --- |
| plan and emission time (the compilers, the IR, `Analysis`, `Slots`, the emitters) | 81 | 53 `IllegalArgumentException`, 20 `IllegalStateException`, 4 `VarkaEmitDeclined`, 4 `UnsupportedOperationException` |
| per batch and run time (the runner, the evaluators, the exec nodes, the sanitizer, the loader) | 23 | 10 `IllegalStateException`, 3 `IllegalArgumentException`, 3 `VarkaBatchDeclined`, 2 `SparkException.internalError`, others |
| building options and session settings (`VarkaEmitOption`, `VarkaEmitOptions`, `VarkaSessionEmit`) | 17 | 17 `IllegalArgumentException` |

**None of them reaches a user's query as free text in production.**

- *Plan and emission time.* `VarkaEvaluatorBase` catches a `VarkaEmitDeclined` as a designed
  decline and any other `Throwable` from emission as an emission failure
  (`VarkaEvaluatorBase.java:348` and 359), counts it, events it and answers the batch on the row
  path.
- *Per batch.* `VarkaEvaluatorBase.run` catches `VarkaBatchDeclined`, `VarkaKernelFailure` and any
  other `Throwable` (lines 616 to 642) and runs the fallback path it was given; the projection and
  the filter both go through it, and the projection's columnar path, which the Arrow cache builder
  takes, passes its own row-by-row fallback (`VarkaProjectExec.scala:257`). The filter evaluator's
  own `catch` blocks (`VarkaFilterEvaluator.java:337`, 383, 429) release the vectors they allocated
  and rethrow to that same handler. The row's "decline that cannot fall back" does not exist today:
  every decline and every failure has a row path.
- *Under test only*, `refuseUndeclaredFallback` raises an `IllegalStateException` over a failure no
  test declared (`VarkaEvaluatorBase.java:659`, VARKA-275), so a test cannot pass over a broken
  kernel; production never raises it.
- *Settings.* The one Varka setting a user can set out of range - declared `.internal()`, so rarely
  set - `spark.sql.codegen.varka.emit.useAVX`, is checked with `checkValue(validator, errorMsg)`
  (`SQLConf.scala`, `VARKA_EMIT_USE_AVX`), and that form already raises Spark's condition
  `INVALID_CONF_VALUE.REQUIREMENT` through `configRequirementError` (defined at
  `ConfigBuilder.scala:132`, called from the `checkValue` form at
line 170).
  `VarkaSessionEmit`'s own check repeats it for a record built another way, and
  `VarkaEmitOption.named`'s parse errors are reached only from tests and `VarkaEmitDump`.
- *A class dump the user configured* (`spark.sql.codegen.varka.classDumpDirectory`) that cannot be
  written is caught and logged as a warning (`VarkaClassDump.java:64`).

**What a user sees of a failure today is a warning and a counter.** A kernel failure logs "The
Varka SIMD kernels `<identity>` failed on this batch; falling back to the per-row path." with the
exception (`VarkaFallbackAccounting.java:101`), adds to the `kernelFailures` metric, and commits a
`VarkaFallbackEvent` with a cause tag (`kernel-failure`, `emission-failure`, `row-path-failure`,
`range-declined`, `non-arrow-batch`), the kernel's identity and the exception's class name. None
of these carries the IR, the options that differ from the defaults, or the batch.

**Spark's registry.** `common/utils/src/main/resources/error/error-conditions.json` holds no
`VARKA` condition. `SparkThrowableSuite` guards the file; section 5 lists what it checks in this
checkout (from its run on 13 October 2026), and any condition added must pass it. The fork's CI
merges upstream Spark before building, and upstream edits that file often, so every added entry is
a likely merge conflict.

## 3. The design

### 3.0 A decision for the owner: what "Spark error conditions" means when nothing surfaces

The row's premise was that Varka raises free-text exceptions a user sees. The check found that it
raises none in production: failures fall back, and the one setting a user can get wrong already
raises Spark's condition. Two ways forward:

- **(A) Conditions for the fallbacks.** Register a small family, `VARKA_FALLBACK` with sub-classes
  `KERNEL_FAILURE`, `EMISSION_FAILURE`, `ROW_PATH_FAILURE`, and use a `SparkException` of that
  condition, carrying the context record as its parameters, as the exception the fallback's warning
  logs and the test-mode refusal raises. The warning then names a documented condition, and
  `docs/sql-varka.md` lists each with what to do. Cost: three or four registry entries that the
  fork's merges with upstream must carry.
- **(B) No new registry entries.** Keep the conditions Spark already has (`INVALID_CONF_VALUE`,
  `INTERNAL_ERROR`); make every internal invariant an internal error with the context record, so
  that a failure that ever escapes - under test, or on a future path with no fallback - is Spark's
  `INTERNAL_ERROR` with a request to report it; and give the fallback's warning and event the
  context record. The "what to do" list moves to row 316's runbook, which reads the same causes.

*Decided by the owner on 13 October 2026: (B), no new error names.* Section 3.4's documentation
and tests 1 to 5 stand; test 6 and option A's family are not built.

**Recommendation: (B).** It delivers all of the row's substance - the user/internal decision for
every site, the context record row 315 needs, `checkError` on every condition a user can reach -
without registry entries for exceptions no user receives, and the runbook is where a user reading
a fallback warning goes. Sections 3.1 to 3.4 are written for (B); under (A), 3.4 adds the family
and its documentation.

### 3.1 Every throw site decided

Each site is decided by the bucket it fires in, and the few exceptions are named:

- **Plan and emission time, per batch: internal.** These are the code's invariants: an IR node of
  the wrong lane, a slot table out of step, a body that declined a batch it should have run. They
  become `SparkException.internalError(message, cause)` raised through one helper,
  `VarkaErrors.internal(String what, VarkaErrorContext ctx)`, which appends the context record and
  the request to report it. The fallback catches them exactly as it catches the
  `IllegalStateException` today, so production behaviour does not change; what changes is what an
  escape looks like and what the warning carries.
- **The designed outcomes stay what they are**: `VarkaEmitDeclined` and `VarkaBatchDeclined` are
  control flow, not errors, and are not converted.
- **Settings: user errors, already conditioned.** `useAVX` keeps `INVALID_CONF_VALUE.REQUIREMENT`;
  a test checks it with `checkError`. `VarkaSessionEmit`'s and `VarkaEmitOption.named`'s checks are
  internal (no user reaches them) and become internal errors.
- **The class dump**: a warning, as today; a directory the user named that cannot be written is the
  user's to fix, so the warning names the setting.

### 3.2 The context record, built only when something fails

`VarkaErrorContext`, a Java record in `codegen/varka/`:

| field | what | built |
| --- | --- | --- |
| `kind` | `VarkaFallbackEvent`'s cause tag (`kernel-failure`, `emission-failure`, ...) | at the catch |
| `operator` | the plan node's name and the stage, as `kernelIdentity()` gives them today | at the catch, from fields the evaluator holds |
| `shapeHash` | the shape cache's hash of the kernel | precomputed once per runner |
| `ir` | the canonical IR of the kernel's roots (`VarkaVectorIR.canonical`) | rendered at the catch from the shape key the runner holds |
| `options` | the emit options that differ from `VarkaEmitOptions.DEFAULTS`, rendered as the shape hash renders them | rendered at the catch |
| `batch` | the failing batch's row count and null count per input; the batch itself only for row 315 | at the catch |
| `cause` | the original exception | at the catch |

Nothing on the per-batch path allocates for it: the runner already holds the shape key and the
options, and every field is read or rendered only inside the `catch` (AGENTS.md's Java rule of no
per-batch allocation). The record renders as one message block for the warning and the internal
error, and as fields of `VarkaFallbackEvent` (new fields `shapeHash` and `options`; the IR stays in
the warning only, since JFR strings are bounded).

**Row 315 writes this record as its file.** Its fields are 315's header and body: `kind` heads the
file as `VarkaFailureSignature`'s kind does in the test tree, then `operator`, `shapeHash`,
`options`, `ir`, and the batch itself where there is one.

### 3.3 Where the record goes

- The fallback's warning: `VarkaFallbackAccounting.kernelFailure` and `rowPathFailure`, and the
  emission failure's warning, which `VarkaEvaluatorBase.fusedRunner` logs inline (about lines 355
  and 363, its event from `emissionFailed`, lines 377 to 381), log the record's block after today's
  sentence.
- The fallback's JFR event: `shapeHash` and `options`, beside today's cause tag and identity.
- The internal error: under test, `refuseUndeclaredFallback` raises `VarkaErrors.internal(...)`
  with the record, so a test failure over a broken kernel shows the IR and options in its message.
  It covers the kernel and row-path failures, the two catches that call it today; an emission
  failure is never refused under test (the emission catch, lines 348 and 359, does not call it), so
  a test can pass over a broken emission. That is a gap in what the tests catch, outside this row:
  a candidate row for milestone 7's point 2, recorded in section 9 when this row closes.

### 3.4 The documentation

`docs/sql-varka.md` gains a short section, "When Varka falls back", listing each cause tag, what
the warning carries, and what a user does: nothing for a designed decline; report it, with the
warning's block, for a failure. Row 316's runbook extends it.

### 3.5 Deliberately unchanged

- **The ghost fallback.** Every failure still falls back in production; this row changes what a
  failure carries, not whether it falls back.
- **The designed declines** (`VarkaEmitDeclined`, `VarkaBatchDeclined`): control flow.
- **Why a query did not use Varka** at all, in SQL metrics: row 313, which reads the same cause
  tags.

### 3.6 Registered op counts

None: no emitted code changes. `emitted_bytes.json` must be unchanged.

## 4. Files

| file | what |
| --- | --- |
| `codegen/varka/VarkaErrorContext.java` (new) | the record and its rendering |
| `codegen/varka/VarkaErrors.java` (new) | `internal(what, ctx)`, the one way an invariant raises |
| the 114 internal sites (the 121 less the 7 designed declines) | `IllegalArgumentException`/`IllegalStateException` to `VarkaErrors.internal` where a context exists, `SparkException.internalError` where none does |
| `execution/varka/VarkaFallbackAccounting.java`, `VarkaFallbackEvent.java` | the record in the kernel and row-path warnings and in the event |
| `execution/VarkaEvaluatorBase.java` | the record built at the catch; the emission failure's warning (`fusedRunner`) and event (`emissionFailed`); the test-mode refusal raises the internal error |
| `docs/sql-varka.md` | "When Varka falls back" |

## 5. Tests, and what each is for

1. **The one user setting**: `spark.sql.codegen.varka.emit.useAVX=-2` raises
   `INVALID_CONF_VALUE.REQUIREMENT`, checked with `checkError`.
2. **An internal error carries the record**: with a kernel failure injected under test and no
   declaration, the query fails with `INTERNAL_ERROR` and its message holds the IR, the shape hash
   and the options that differ; `checkError` on the condition, a match on the parameters.
3. **The warning carries the record**: with the failure declared, the fallback's warning holds the
   same block (`withLogAppender`), and the JFR event holds the shape hash and options.
4. **No per-batch allocation**: `VarkaAllocationSampler` over a kernel's steady state allocates
   nothing more than before.
5. **The bytes oracle and the suites**: `emitted_bytes.json` unchanged; the Varka suites pass at
   the defaults and in the matrix.
6. Under (A) only: `SparkThrowableSuite` passes with the new family (formatting, sorting, a valid
   `sqlState`, documentation parity as this checkout checks it).

## 6. The measurement

The kernel's steady-state allocation (test 4) and one benchmark pair of a kernel-heavy class
against the merge base, to show the failure path costs nothing when nothing fails.

### 6.1 Predictions, registered before the run

Read against the calibration table in `skills/testing-and-debugging.md` (#797): the cost lines
there came in high when a check added work per batch; this change adds none, so the prediction is
zero, not a percentage.

1. No new per-batch allocation (test 4).
2. The benchmark pair reads within the pair runner's own spread on every row.
3. `emitted_bytes.json` is unchanged.
4. Every one of the 114 internal sites (121 less the 7 designed declines) converts without a
   behaviour change visible to the Varka suites.

## 7. Risks

1. **An internal error that a caller expected as `IllegalArgumentException`.** A few sites may be
   caught by type; the conversion checks each catch of those types in Varka's code first.
2. **The record's rendering itself throws** while a failure is handled. It is built defensively: a
   field that cannot render is written as unavailable, and the original cause is always kept.
3. **Under (A), registry conflicts** with upstream on every merge of the fork; the family is kept to
   one prefix and three or four entries.

## 8. Sequencing

1. This plan, and the owner's decision of 3.0.
2. **314a**: `VarkaErrorContext`, `VarkaErrors`, the record in the warning, the event and the
   test-mode refusal; tests 1 to 4.
3. **314b**: the remaining internal sites converted, the docs section; test 5 (and 6 under (A)).
4. Row 313 starts after 314 lands, since both edit `VarkaEvaluatorBase` and
   `VarkaFallbackAccounting`; row 315 follows, writing the record as its file.

## 9. Outcome
