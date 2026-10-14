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
- **Each batch falls back.** `VarkaEvaluatorBase.serveBatch` catches `VarkaBatchDeclined`,
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
`range-declined`, `non-arrow-batch`), the kernel's identity and the exception's class name. The
identity is `VarkaEvaluatorBase.kernelIdentity()` (line 494), memoized per evaluator: the kernel's
source file, named by its shape hash, the canonical IR of each output (`VarkaVectorIR.canonical`)
and the operator and stage. Every fallback warning (kernel, row-path and emission failures, and the
emitter's decline) and the event's `kernelIdentity` field carry it. So today's warning and event
already name the shape hash, the IR and the operator; what none carries is the options that differ
from the defaults, and the batch.

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
to stage failure" `SparkException`. Of the two planning-time compiles, only `VarkaColumnarRule`'s
runs on the driver and raises unwrapped; the evaluator's `plan()`, reached from `canRun()`, runs in
the task, so its error arrives wrapped like any task-side one.

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

A second review, of the page that presents this plan, checked it against the code and found four
more:

1. It said no fallback leaves the IR. `kernelIdentity()` already puts the shape hash, the IR and
   the operator in every fallback warning and in the event's `kernelIdentity` field. The record
   now reuses that identity and the runner's own `shapeHash` field (`VarkaKernelRunner.java:99`)
   instead of a lookup through `VarkaShapeEntry`, and the event gains only `options`.
2. It said a planning-time error is raised on the driver unwrapped. That holds for
   `VarkaColumnarRule`'s compile only; the evaluator's `plan()` compile runs in the task (2, last
   paragraphs; 3.3 and test 2 now say which path each covers).
3. It called `NoSuchElementException` a loader's. It is the `Iterator` contract of the filter's
   exhausted selection iterator (`VarkaFilterExec.scala:493`).
4. Smaller: the per-batch catch is in `serveBatch`, not `run`; the injected
   `NoClassDefFoundError`'s message is "injected Varka kernel failure"
   (`VarkaKernelRunner.java:265`); and the injected emission `IllegalStateException`
   (`VarkaEvaluatorBase.java:393`, in `newRunner`) is counted in the per-batch bucket although it
   fires during code generation, which leaves the 106 unchanged.

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
  - `VarkaKernelFailure`, which `serveBatch`'s `catch (VarkaKernelFailure)` reads, and
    `VarkaMemoryViolation`, which `isCatchable` excludes so a sanitizer violation is never answered
    by the fallback;
  - the test-injection throws (the "injected Varka emission failure" `IllegalStateException`,
    and the `NoClassDefFoundError` "injected Varka kernel failure"), which simulate a kind of
    failure on purpose;
  - the two sites already `SparkException.internalError` (`VarkaFilterExec.scala:154`,
    `VarkaProjectExec.scala:114`), and the JDK types a Java contract requires: the class
    loader's `ClassNotFoundException` (`VarkaGeneratedClassLoader.java:95`) and the iterator's
    `NoSuchElementException` (`VarkaFilterExec.scala:493`).
- **The one user setting** keeps `INVALID_CONF_VALUE.REQUIREMENT`; a test checks it.
- **The class dump**: a warning, as today, now naming the setting the user can fix.

### 3.2 The context record, built only when something fails

`VarkaErrorContext`, a Java record in `codegen/varka/`:

| field | what | built |
| --- | --- | --- |
| `kind` | `VarkaFallbackEvent`'s cause tag, or `planning` for a compile at planning time | at the catch |
| `identity` | the kernel's source file (named by its shape hash), the canonical IR of each output and the operator and stage: `kernelIdentity()`, as today's warnings carry it | already memoized per evaluator; read at the catch |
| `shapeHash` | the shape cache's hash of the kernel | the runner's `shapeHash` field; for an emission failure, which has no runner, computed from the compiled projection at the catch, as `kernelIdentity()` does |
| `options` | the emit options that differ from `VarkaEmitOptions.DEFAULTS`, rendered as the shape hash renders them | rendered at the catch |
| `batch` | the failing batch's row count and null count per input; the batch itself only for row 315 | at the catch |
| `cause` | the original exception | at the catch |

Every field is read or computed inside the `catch`, from what the evaluator and runner already hold;
nothing is added to the per-batch path (AGENTS.md's Java rule of no per-batch allocation). At
planning time the compiler's exception has no evaluator: there the record is built in the compiler's
own catch, around the entry it was compiling, with the IR of that entry and the options it compiled
under. The record renders as one message block for the warning and the internal error. On
`VarkaFallbackEvent` the identity is already in `kernelIdentity`, so the event gains one field,
`options`.

**Row 315 writes this record as its file**: `kind` heads it, as `VarkaFailureSignature`'s kind does
in the test tree, then `identity`, `shapeHash`, `options`, and the batch where there is one.

### 3.3 Where the record goes

- **Planning:** the compiler's entry points catch a throw from compiling one entry and rethrow it
  as the internal error with the record, so the query fails with `INTERNAL_ERROR` naming the
  expression and the options instead of a bare `IllegalArgumentException`. The same catch covers
  both callers: from `VarkaColumnarRule` the error reaches the user on the driver as it is; from
  the evaluator's `plan()` in a task it reaches the driver inside "Job aborted". The query still
  fails; making a planning-time bug decline the projection instead is a design question for row
  313.
- **The fallback's warning:** `VarkaFallbackAccounting.kernelFailure` and `rowPathFailure`, and the
  emission failure's warning, which `VarkaEvaluatorBase.fusedRunner` logs inline, log the record's
  block after today's sentence.
- **The fallback's JFR event:** `options`, beside today's cause tag and identity.
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
| `sql/catalyst/.../codegen/varka/VarkaFallbackEvent.java` | `options` |
| `sql/core/.../execution/varka/VarkaFallbackAccounting.java` | the record in the kernel and row-path warnings |
| `sql/core/.../execution/VarkaEvaluatorBase.java` | the record built at the catches; the emission warning and event; the three refusals raise the internal error |
| `docs/sql-varka.md` | "When Varka falls back" |

## 5. Tests, and what each is for

1. **The one user setting:** `spark.sql.codegen.varka.emit.useAVX=-2` raises
   `INVALID_CONF_VALUE.REQUIREMENT`, checked with `checkError`.
2. **A planning-time bug carries the record:** a compiler invariant broken through a test hook,
   reached through `VarkaColumnarRule`, fails the query on the driver with `INTERNAL_ERROR`,
   checked with `checkError`, its message holding the IR and options. (The evaluator's `plan()`
   path raises the same error inside "Job aborted", as test 3 unwraps.)
3. **A task-side refusal carries the record:** a kernel failure injected under test and not
   declared fails the job; the test unwraps the "Job aborted" `SparkException` to its cause, as
   `DAGScheduler` wraps an internal error, and checks `INTERNAL_ERROR` and the record there.
4. **The warning carries the record:** with the failure declared, the fallback's warning holds the
   block (`withLogAppender`), and the JFR event holds the options beside the identity.
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

### 9.1 314a, 13 October 2026

**What was built.** `VarkaErrorContext`, the record of 3.2 with its block rendering and the
helpers that keep building it from throwing (`safely`, a constructor that replaces a null field
instead of rejecting it), and `VarkaErrors`, whose `illegalArgument` and `illegalState` return
`SparkIllegalArgumentException` and `SparkIllegalStateException` with condition `INTERNAL_ERROR`,
the site's text, the record's block and a request to report the bug as the one message parameter,
and the record's cause as the exception's cause. The planning-time catch is in
`VarkaExpressionCompiler`, around the two funnels every caller reaches (`classifyKernels` behind
`compilePartial` and `declines`, `predicate` behind `explainAndCompilePredicate`, which
`compilePredicate` and `explainPredicate` now call), so it covers `VarkaColumnarRule` on the driver,
the evaluators' own compiles in a task, the exec nodes' and EXPLAIN's. `VarkaEvaluatorBase` builds
the record in its three catches (emission, kernel, row path); the emission warning and
`VarkaFallbackAccounting.kernelFailure` and `rowPathFailure` log its block after their sentence; the
three refusals under test raise `VarkaErrors.illegalState` with it; and `VarkaFallbackEvent` has
`options`, which every cause's event fills. A test hook,
`VarkaExpressionCompiler.setFailForTesting`,
throws inside the planning catch. `VarkaErrorContextSuite` holds tests 1 to 5 of section 5, test 4
as two (the kernel warning and event, and the emission warning), and one more for risk 2: a record
whose cause's `toString` throws and whose options could not render still renders, those fields
reading as unavailable.

**Where the code corrected the plan.**

1. **The planning record names Catalyst entries, not IR.** When the compiler throws, the IR is what
   it was building; there is none to render. The record's identity is the projection's entries and
   their input attributes (`projection [date_add(d#1, 3) AS a#4] over [d#1, i#2]`), or the filter's
   condition, and its shape hash reads `none`.
2. **The catch is per call, not per entry.** Entries are classified together - one slot table, one
   size admission over the fused set, rollbacks when one declines - so there is no per-entry seam to
   catch at without restructuring `classify`; the record names the whole projection or condition.
3. **The planning catch keeps the type of an `IllegalArgumentException` and an
   `IllegalStateException` only.** Any other runtime exception from the compiler (a
   `NullPointerException`, a `ClassCastException`, an `UnsupportedOperationException`) has no Spark
   carrier of its type and becomes `SparkIllegalStateException`, its original as the cause. A
   `SparkThrowable` passes unchanged, and so does `VarkaEmitDeclined`, which extends
   `IllegalArgumentException` and is control flow.
4. **Options render by name.** 3.2 said "as the shape hash renders them"; `canonical()` renders
   the first 26 options by position, unreadable without the table. The record writes each differing
   option as `name=value`, comma-separated, through `VarkaEmitOption.Subject.label` - the option
   matrix's own label, which `VarkaMatrix.parse` reads back - and empty (shown as "the defaults")
   at the defaults. The event's `options` is the same string.
5. **A further kernel's shape hash travels on its failure.** In a projection served by several
   kernels the failing one may be a `VarkaKernelPart`, whose runner the evaluator catching the
   failure does not hold. `VarkaKernelFailure` gained a `shapeHash` field, set by the runner that
   threw it and kept when `VarkaKernelPart` names itself on it; its two unused constructors went.
6. **The batch's null counts are read from its columns at the catch** (`ColumnVector.numNulls` over
   the kernel's inputs), not from the runner's `srcNullCount`, which a failure inside `fill` leaves
   holding the previous batch's counts. For a further kernel's failure the record has the row count
   only: the catching evaluator's input ordinals are not that kernel's.
7. **`VarkaFallbackAccounting` takes the options as a second supplier**, memoized per evaluator,
   so the declined and non-Arrow events carry them too, rendered only when a recording has the
   event enabled.
8. **The planning error's sentence does not repeat the original message.** 3.1 put the original
   text first; at planning the original is the record's cause, whose line already prints it, so the
   sentence is "Varka's compiler failed while planning." and the message says it once.
9. **A `${` in the message needs no escape**, though the self-review said it did. The review read
   the template filling as commons-text's `StringSubstitutor`, which substitutes inside a value, so
   that a projection's literal `'${message}'` rendered into the record would loop and throw. Spark
   fills templates with its own `org.apache.spark.StringSubstitutor`, which does not substitute
   inside values and preserves escapes: the first fix, escaping `${` as `$${`, showed in the
   message, and fork CI caught it. The escape is gone, and the test that caught it now checks that
   such a literal renders as written.

**Predictions (6.1) that 314a scores**, on the laptop on 14 October 2026 and on fork CI.

1. **Held.** Test 5 ran one round in which every one of 106 kernel batches failed and built a
   record, then found a clean round (no suspect allocation sample) on its third steady-state round;
   `VarkaProjectExecSuite`'s own sampler test passes unchanged. The success path gained no code: the
   record, the options rendering and the batch facts are reached only from the catches.
2. **Owed.** The pair needs an idle laptop and was not run; the class is
   `VarkaEvaluatorOverheadBenchmark`
   (`dev/varka_bench_pair.sh core VarkaEvaluatorOverheadBenchmark`),
   whose cases are the evaluator's per-batch path that the change leaves untouched outside its
   catches.
3. **Held.** `VarkaEmittedBytesSuite` passes and `emitted_bytes.json` is unchanged; nothing in the
   emitter changed.
4. Belongs to 314b.

**Test time.** `VarkaErrorContextSuite` adds about 4 s to the sql step (its steady-state test 0.8 s
locally); fork CI's scoped sql step read 401 s against 395 s for a neighbouring branch's run of 39
suites, within that step's run-to-run spread.

**Left for 314b.** The remaining invariant sites of 3.1 and the docs section of 3.4, as section 8
says.

