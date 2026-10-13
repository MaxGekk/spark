# VARKA-314: Varka's errors as Spark error conditions, and one context record for every failure

*Row 314 of `m7/PLAN.md`, added by the owner on 12 October 2026 with rows 313 and 315 to 319
(`m7/PLAN.md` 2.4, "Errors and troubleshooting"). Planned on 13 October 2026. The owner chose, the
same day, section 3.0's option (B): no new error names. The plan's own review then found that
section 2's first reading was wrong in one place - a broken invariant at planning time does reach
the user - and this version is corrected; section 2.1 says what changed.*

## 1. Where this came from

The row asks for three things:

1. **Every throw site decided** as a user error or an internal one, as Velox (`VELOX_USER_CHECK`
   against `VELOX_CHECK`), Trino (`ErrorType`), DuckDB (`INTERNAL`) and DataFusion (`Internal`)
   split them: a user error is a named condition that says what to do; a kernel or emitter bug is
   an internal error with a request to report it.
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
`execution/varka/`, comment lines excluded; a review's slightly different rules counted 124 in 94
files, and the buckets, not the exact number, decide the design), by when they fire:

| when | sites | kinds |
| --- | ---: | --- |
| planning and code generation (the compilers, the IR, `Analysis`, `Slots`, the emitters) | 81 | 53 `IllegalArgumentException`, 20 `IllegalStateException`, 4 `VarkaEmitDeclined`, 4 `UnsupportedOperationException` |
| per batch and run time (the runner, the evaluators, the exec nodes, the sanitizer, the loader) | 23 | 10 `IllegalStateException`, 3 `IllegalArgumentException`, 3 `VarkaBatchDeclined`, 2 `SparkException.internalError`, others |
| building options and session settings (`VarkaEmitOption`, `VarkaEmitOptions`, `VarkaSessionEmit`) | 17 | 17 `IllegalArgumentException` |

**Where a failure goes, by when it fires:**

- **Planning reaches the user.** `VarkaColumnarRule` asks the compiler on the driver whether a
  projection or a filter fuses (`VarkaExpressionCompiler.compilePartial` in `isVarkaEligible`,
  `VarkaColumnarRule.scala:199`; `compilePredicate` in the filter's rewrite, line 261), with no
  `catch` around it, and the evaluator's own compile (`VarkaEvaluatorBase.plan()`, line 278,
  reached from `canRun()` outside the batch's `try`) says in its comment that a throw "fails the
  task". So a broken invariant in the compiler, the IR (`VarkaVectorIR`, 18 sites) or `Analysis`
  (16) fails the user's query with a raw `IllegalArgumentException` or `IllegalStateException`,
  free text and no condition. This is the row's case in practice.
- **Code generation falls back.** `VarkaEvaluatorBase.fusedRunner` catches a `VarkaEmitDeclined`
  as a designed decline and any other catchable `Throwable` from building the kernel as an
  emission failure (lines 348 and 359): it logs a warning inline (line 363), events it
  (`emissionFailed`, lines 377 to 381), refuses it under test (`refuseUndeclaredFallback`, line
  366) and answers on the row path.
- **Each batch falls back.** `VarkaEvaluatorBase.run` catches `VarkaBatchDeclined`,
  `VarkaKernelFailure` and any other catchable `Throwable` (lines 616 to 642) and runs the
  fallback path it was given; the projection's columnar path, which the Arrow cache builder
  takes, passes its own (`VarkaProjectExec.scala:257`). The filter evaluator's own `catch` blocks
  (`VarkaFilterEvaluator.java:337`, 383, 429) release the vectors they allocated and rethrow to that
  handler. `isCatchable` excludes a `VarkaMemoryViolation`, so a sanitizer violation is never
  answered by the fallback. The row's "decline that cannot fall back" does not exist: every
  decline has a row path.
- **Under test**, `refuseUndeclaredFallback` fails the task over an emission, kernel or row-path
  failure no test declared (VARKA-275), so a test cannot pass over a broken kernel.
- **Settings.** The one Varka setting a user can set out of range,
  `spark.sql.codegen.varka.emit.useAVX`, declared `.internal()`, is checked with
  `checkValue(validator, errorMsg)`, and that form already raises Spark's condition
  `INVALID_CONF_VALUE.REQUIREMENT` through `configRequirementError` (defined at
  `ConfigBuilder.scala:132`, called at line 170). `VarkaSessionEmit`'s own check repeats it for a
  record built another way, and `VarkaEmitOption.named`'s parse errors are reached only from tests
  and `VarkaEmitDump`.
- **A class dump** the user configured that cannot be written is caught and logged as a warning
  (`VarkaClassDump.java:64`).

**What a fallback leaves today** is a warning, a counter and a JFR event. A kernel failure logs
"The Varka SIMD kernels `<identity>` failed on this batch; falling back to the per-row path." with
the exception (`VarkaFallbackAccounting.java:101`), adds to `kernelFailures`, and commits a
`VarkaFallbackEvent` with a cause tag (`kernel-failure`, `emission-failure`, `row-path-failure`,
`range-declined`, `non-arrow-batch`), the kernel's identity and the exception's class name. None
carries the IR, the options that differ from the defaults, or the batch.

**The carrier must be unchecked.** `SparkException.internalError` returns a `SparkException`,
which extends `java.lang.Exception`: a checked exception, so Java code cannot throw it without a
`throws` clause that would spread through records' constructors, the emitters and lambdas such as
`VarkaBatchPath.run`, and 119 of the sites are Java. Spark has unchecked carriers of a condition:
`SparkIllegalArgumentException` and `SparkIllegalStateException` (`SparkException.scala:525` and
598), subclasses of the two exceptions Varka throws today.

**About 100 test sites intercept those two types** (`VarkaLaneTypeSuite`, `VarkaEmitOptionSuite`,
`VarkaEmitterDivisionSuite` 13, `VarkaIrFuzzSuite` 9, where the type drives how the fuzzer
classifies a case, `VarkaEmitterChronoSuite` 9, `VarkaMatrixSuite` 6, and others). Since the
carriers are subclasses, those intercepts keep passing.

**A task-side internal error is wrapped.** `DAGScheduler.abortStage` passes a task's
`SparkThrowable` through to the driver unwrapped only when its condition is not an internal error
(`DAGScheduler.scala:4963`); an `INTERNAL_ERROR` from a task arrives inside a plain "Job aborted due
to stage failure" `SparkException`. A planning-time error is raised on the driver and is not
wrapped.

**Spark's registry.** `common/utils/src/main/resources/error/error-conditions.json` holds no
`VARKA` condition, and under (B) none is added.

### 2.1 What the plan's review corrected

The first version said no Varka error reaches a user's query in production, and that an emission
failure is never refused under test. Both were wrong: planning-time throws are not caught (above),
and the emission catch does call `refuseUndeclaredFallback` (line 366). It also proposed
`SparkException.internalError`, which Java cannot throw unchecked; counted control-flow throws and
the two existing internal errors among the sites to convert; expected a task-side internal error to
reach the driver unwrapped; said the runner holds the shape key (it holds the `VarkaShapeEntry`; the
key is a local of `newRunner`); and placed `VarkaFallbackEvent` in `sql/core`. Each is corrected
below.

## 3. The design

### 3.0 The owner's decision: no new error names

The question was what "Spark error conditions" means for Varka:

- **(A)** a small `VARKA_FALLBACK` family in Spark's registry, logged by the fallback's warning; or
- **(B)** no new entries: Spark's own `INTERNAL_ERROR` for every Varka invariant, with a context
  record, and `INVALID_CONF_VALUE` for the one user setting.

*Decided by the owner on 13 October 2026: (B).* The correction of section 2 strengthens it: the
errors that do reach a user are Varka's bugs at planning time, and Spark's answer to a bug a user
meets is `INTERNAL_ERROR` with a request to report it. Option A's family is not built.

### 3.1 Every throw site decided

- **Varka's invariants become internal errors**, through one helper class, `VarkaErrors`:
  `illegalArgument(what, ctx)` and `illegalState(what, ctx)` return a
  `SparkIllegalArgumentException` or `SparkIllegalStateException` with condition `INTERNAL_ERROR`,
  its message parameter the original text, the context record when one exists and the request to
  report it. They keep the Java type each site throws today, so every caller and test that catches
  by type is unchanged. They apply to the `IllegalArgumentException`, `IllegalStateException` and
  `UnsupportedOperationException` sites of all three buckets - 53 + 20 + 4 at planning and code
  generation, 10 + 3 per batch, 17 in building options - less the one test-injection
  `IllegalStateException`: 106 sites. The other 15 are below.
- **Not converted**, since they are control flow or already conditioned:
  - the designed declines `VarkaEmitDeclined` (4) and `VarkaBatchDeclined` (3);
  - `VarkaKernelFailure`, which `run`'s `catch (VarkaKernelFailure)` reads, and
    `VarkaMemoryViolation`, which `isCatchable` excludes so a sanitizer violation is never answered
    by the fallback;
  - the test-injection throws (the "injected Varka emission failure", the injected
    `NoClassDefFoundError`), which simulate a kind of failure on purpose;
  - the two sites already `SparkException.internalError` (`VarkaFilterExec.scala:154`,
    `VarkaProjectExec.scala:114`), and the JDK types a loader contract requires
    (`ClassNotFoundException`, `NoSuchElementException`).
- **The one user setting** keeps `INVALID_CONF_VALUE.REQUIREMENT`; a test checks it.
- **The class dump**: a warning, as today, now naming the setting the user can fix.

### 3.2 The context record, built only when something fails

`VarkaErrorContext`, a Java record in `codegen/varka/`:

| field | what | built |
| --- | --- | --- |
| `kind` | `VarkaFallbackEvent`'s cause tag, or `planning` for a compile at planning time | at the catch |
| `operator` | the plan node and the stage, as `kernelIdentity()` gives them | at the catch |
| `shapeHash` | the shape cache's hash of the kernel | from the `VarkaShapeEntry` the runner holds; for an emission failure, which has no runner, computed from the compiled projection at the catch |
| `ir` | the canonical IR of the kernel's roots (`VarkaVectorIR.canonical`) | rendered at the catch from the compiled projection the evaluator holds |
| `options` | the emit options that differ from `VarkaEmitOptions.DEFAULTS`, rendered as the shape hash renders them | rendered at the catch |
| `batch` | the failing batch's row count and null count per input; the batch itself only for row 315 | at the catch |
| `cause` | the original exception | at the catch |

Every field is read or computed inside the `catch`, from what the evaluator and runner already hold;
nothing is added to the per-batch path (AGENTS.md's Java rule of no per-batch allocation). At
planning time the compiler's exception has no evaluator: there the record is built in the compiler's
own catch, around the entry it was compiling, with the IR of that entry and the options it compiled
under. The record renders as one message block for the warning and the internal error, and as
fields of `VarkaFallbackEvent` (new fields `shapeHash` and `options`; the IR stays in the warning
only, since JFR strings are bounded).

**Row 315 writes this record as its file**: `kind` heads it, as `VarkaFailureSignature`'s kind does
in the test tree, then `operator`, `shapeHash`, `options`, `ir`, and the batch where there is one.

### 3.3 Where the record goes

- **Planning:** the compiler's entry points catch a throw from compiling one entry and rethrow it
  as the internal error with the record, so the query fails with `INTERNAL_ERROR` naming the
  expression and the options instead of a bare `IllegalArgumentException`. The query still fails;
  making a planning-time bug decline the projection instead is a design question for row 313.
- **The fallback's warning:** `VarkaFallbackAccounting.kernelFailure` and `rowPathFailure`, and the
  emission failure's warning, which `VarkaEvaluatorBase.fusedRunner` logs inline, log the record's
  block after today's sentence.
- **The fallback's JFR event:** `shapeHash` and `options`, beside today's cause tag and identity.
- **Under test:** the three refusal sites (emission, kernel, row path) raise the internal error with
  the record, so a test failure over a broken kernel shows the IR and options.

### 3.4 The documentation

`docs/sql-varka.md` gains a short section, "When Varka falls back", listing each cause tag, what the
warning carries, and what a user does: nothing for a designed decline; report it, with the
warning's block, for a failure or for an `INTERNAL_ERROR` that names Varka. Row 316's runbook
extends it.

### 3.5 Deliberately unchanged

- **The ghost fallback.** Every code-generation and per-batch failure still falls back.
- **Whether a planning-time bug fails the query.** It still does; this row changes what the error
  says. Declining instead belongs with row 313's "why Varka did not run".
- **The designed declines, `VarkaKernelFailure`, `VarkaMemoryViolation`**: control flow.

### 3.6 Registered op counts

None: no emitted code changes. `emitted_bytes.json` must be unchanged.

## 4. Files

| file | what |
| --- | --- |
| `sql/catalyst/.../codegen/varka/VarkaErrorContext.java` (new) | the record and its rendering |
| `sql/catalyst/.../codegen/varka/VarkaErrors.java` (new) | `illegalArgument` and `illegalState`, the internal-error carriers |
| the invariant sites of 3.1 | rethrown through `VarkaErrors`, the Java type kept |
| `sql/catalyst/.../codegen/VarkaExpressionCompiler.java` | the planning-time catch that adds the record |
| `sql/catalyst/.../codegen/varka/VarkaFallbackEvent.java` | `shapeHash` and `options` |
| `sql/core/.../execution/varka/VarkaFallbackAccounting.java` | the record in the kernel and row-path warnings |
| `sql/core/.../execution/VarkaEvaluatorBase.java` | the record built at the catches; the emission warning and event; the three refusals raise the internal error |
| `docs/sql-varka.md` | "When Varka falls back" |

## 5. Tests, and what each is for

1. **The one user setting:** `spark.sql.codegen.varka.emit.useAVX=-2` raises
   `INVALID_CONF_VALUE.REQUIREMENT`, checked with `checkError`.
2. **A planning-time bug carries the record:** a compiler invariant broken through a test hook
   fails the query on the driver with `INTERNAL_ERROR`, checked with `checkError`, its message
   holding the IR and options.
3. **A task-side refusal carries the record:** a kernel failure injected under test and not
   declared fails the job; the test unwraps the "Job aborted" `SparkException` to its cause, as
   `DAGScheduler` wraps an internal error, and checks `INTERNAL_ERROR` and the record there.
4. **The warning carries the record:** with the failure declared, the fallback's warning holds the
   block (`withLogAppender`), and the JFR event holds the shape hash and options.
5. **No per-batch allocation:** `VarkaAllocationSampler` over a kernel's steady state allocates
   nothing more than before.
6. **The bytes oracle and the suites:** `emitted_bytes.json` unchanged; every Varka suite passes at
   the defaults and in the option matrix, the roughly 100 intercepts of the two JDK types included.

## 6. The measurement

The kernel's steady-state allocation (test 5) and one benchmark pair of a kernel-heavy class
against the merge base, to show the failure path costs nothing when nothing fails.

### 6.1 Predictions, registered before the run

Read against the calibration table in `skills/testing-and-debugging.md` (#797): its cost lines came
in high when a check added work per batch; this change adds none, so the prediction is zero.

1. No new per-batch allocation (test 5).
2. The benchmark pair reads within the pair runner's own spread on every row.
3. `emitted_bytes.json` is unchanged.
4. All 106 invariant sites of 3.1 convert with their Java type kept, and no Varka suite's outcome
   changes.

## 7. Risks

1. **A message a test matches by text.** The internal error's message is Spark's `INTERNAL_ERROR`
   template around the original text; a test that matches the whole message, not a substring,
   changes. The conversion runs the Varka suites after each family of sites.
2. **The record's rendering itself throws** while a failure is handled. It is built defensively: a
   field that cannot render is written as unavailable, and the original cause is always kept.
3. **`SparkIllegalArgumentException` and `SparkIllegalStateException` are `private[spark]`.**
   Varka's code is in `org.apache.spark` packages, and Java sees a Scala `private[spark]` class as
   public; the plan uses them from there only.

## 8. Sequencing

1. This plan and the owner's decision (taken: B).
2. **314a:** `VarkaErrorContext`, `VarkaErrors`, the planning-time catch, the record in the
   warnings, the event and the three refusals; tests 1 to 5.
3. **314b:** the remaining invariant sites converted, the docs section; test 6.
4. Row 313 starts after 314 lands, since both edit `VarkaEvaluatorBase` and
   `VarkaFallbackAccounting`; row 315 follows 314, writing the record as its file.

## 9. Outcome
