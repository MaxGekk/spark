# VARKA-275: A kernel failure the ghost fallback hides fails its test

## 1. Where this came from

Row 275 of `m7/PLAN.md`, from the mutation-testing reading (`m7/READING.md` 11, Petrovic and
Ivankovic): a Varka kernel that throws gets a warning, a metric and a JFR event, and its batch
reruns on the row path (`VarkaFallbackAccounting.kernelFailure`), so a `sql/core` test that
checks answers passes over a broken kernel - the row engine's answers are right. Spark's own
suites turn the codegen fallback off for the same reason (`SparkSessionBinder` sets
`spark.sql.codegen.fallback` to false). The row asks that any kernel-failure fallback a test did
not expect fail it, designed declines still passing. Its census of 3 October found no hidden one
in the Varka suites of `sql/core`, so the row expected a guard with nothing to fix first.

## 2. The admission check, done

The rule written first (section 3.1), then every Varka suite of `catalyst` and `sql/core` run
under it at the wide width (`dev/varka_gate.sh --only compile,wide`, 465 s) on 10 October 2026,
and the logs read for every failure fallback and every refusal, attributed to the test that was
running. Every suite passed. What fell back:

| cause | test | declared by |
| :--- | :--- | :--- |
| kernel failure, 29 batches | the injected-failure tests of `VarkaDifferentialSuite`, `VarkaProjectExecSuite`, `VarkaFilterExecSuite` and `VarkaColumnarToRowExecSuite` | `setFailKernelForTesting` |
| emission failure, 2 | the emission-failure tests of `VarkaProjectExecSuite` and `VarkaFilterExecSuite` | `setFailEmissionForTesting` |
| emission failure, 2, refused | `VarkaSparkFuzzSuite`, "a forced output finds a rollback that truncates one input too far" | nothing: its planted `misdescribeRollback` makes some compositions fail to emit |
| row-path failure, 1 | `VarkaProjectExecSuite`, "a residual-machinery failure is counted under its own cause" | nothing: its exploding expression throws in the residual projection |
| row-path failure, 12, refused | `VarkaSparkFuzzSuite`'s random compositions | nothing, and nothing to declare: see below |
| kernel and row-path, 4 | `VarkaEvaluatorComponentsSuite`'s accounting test | not a fallback: it calls the accounting directly |

No hidden failure, then, as the 3 October census found; but two tests cause a failure without a
hook, and both passed under the rule only by accident. The planted-rollback test still found the
answers it looks for among the compositions that did emit, and the residual test intercepts any
`Throwable`, so it caught the refusal where it meant the fallback's own rethrow. Both get the
declaration.

The twelve fuzz fallbacks are each a `RemoteClassLoaderError` for `expressions/Object.class`, in
a task whose stage another task's expected error (a `CAST_OVERFLOW` the differential compares)
had cancelled while it compiled its row machinery: the class-loading failure is the kill's. The
same signature is 1,331 of the 1,342 fallbacks in the night's force-every-output run of 10
October (`VarkaSparkFuzzSuite`, 15,000 compositions); the other 11 there are the planted-rollback
test's emission failures. Refusing in a killed task is harmless - its result is discarded - but the
failure is not a kernel's, so the rule leaves a task that is being killed alone
(`TaskContext.isInterrupted`).


## 3. The design

### 3.1 The rule, and where it is read

Three fallbacks answer a failure: a kernel failure and a failure of the per-row machinery beside
the kernel, both caught in `VarkaEvaluatorBase.serveBatch`, and an emission failure, caught where
the evaluator resolves its runner. At each, after the fallback is counted, evented and logged as
it is today, `refuseUndeclaredFallback` asks `VarkaColumnarToRowExec.failureFallbackForbidden`
and, when it is true, throws an `IllegalStateException` naming the failure, VARKA-275 and how to
declare one, with the failure as its cause. The task fails, and so does the test.

`failureFallbackForbidden` is true under test (`Utils.isTesting`, which sbt and Maven set for
every test JVM) unless a test has declared a failure fallback, and never outside tests. A test
declares one by setting a failure hook - `setFailKernelForTesting` or
`setFailEmissionForTesting`, which can only be set to cause one - or the new
`setFailureFallbackExpectedForTesting`, for a failure caused another way: a planted emitter bug,
an expression made to throw in the row machinery. One rule, then: a failure fallback is allowed
only while a test has said it causes one.

Designed outcomes are not failures and need no declaration: a declined batch, a batch that is not
Arrow, a warm-up batch, and the emitter declining a shape over its budget (`VarkaEmitDeclined`,
caught apart from the failures). Nor is a failure in a task that is being killed (section 2).

### 3.2 Why a static switch, not a SQL configuration

The switch sits beside the hooks it extends, in `VarkaColumnarToRowExec`, with their discipline:
static, because Spark runs tasks on other threads, and reset in a finally block. A configuration
entry would add an option to the production surface whose only legal value outside tests is the
default, which is what the hooks' comment already argues against; and `Utils.isTesting` covers the
Varka suites that build evaluator factories directly, outside any session, which a session
setting would not reach.

### 3.3 What is deliberately unchanged

The fallback itself, its metrics, events and log lines: production behaviour is the same, and
under test the fallback is still counted before it is refused, so the existing tests that read
the metrics read the same values. Row 279 (declines as specified behaviour) and row 274 (the code
users run) are separate rows.

## 4. Files

| file | what |
|---|---|
| `VarkaColumnarToRowExec.scala` | `setFailureFallbackExpectedForTesting`, `failureFallbackForbidden` |
| `VarkaEvaluatorBase.java` | `refuseUndeclaredFallback` at the three fallbacks |
| `VarkaProjectExecSuite.scala` | the rule's tests |
| the suites that cause a failure without a hook | the declaration (section 2) |
| `m7/PLAN.md`, `skills/testing-and-debugging.md` | row 275, the rule |

## 5. Tests, and what each is for

* **The rule**: `failureFallbackForbidden` is true under test, and false while each of the three
  declarations is set.
* **End to end**: with `misdescribeAdd` the kernel for `date_add` throws `NoSuchMethodError`
  when it runs, which no hook injects. Undeclared, the query fails with VARKA-275's message and the
  error as a cause; declared, it answers right with one kernel-failure batch counted. This is the
  failure the row is about, and it was green before.
* **Every Varka suite of `sql/core` and `catalyst`** at both widths under the rule: a suite that
  causes a failure on purpose declares it, and nothing else fails.

## 6. The measurement

None; no kernel changes.

## 7. Risks

1. **A failure that only some machines or orders meet** - a kernel failing at the narrow width,
   or after another suite has run - would now fail a test where it used to pass silently. That is
   the point; each is a finding, with its own row.
2. **A test that causes a failure and relies on the fallback answering** goes red. Section 2's run
   lists every one; each gets the declaration, or, if it did not mean to cause one, a row.

## 8. Sequencing

One commit: the plan, the rule, the declarations, the tests and row 275.

## 9. Outcome, 10 October 2026

The rule, the killed-task exception, the two declarations and the rule's two tests. Every Varka
suite of `catalyst` and `sql/core` passes at both widths under the rule
(`dev/varka_gate.sh --only compile,wide,narrow,lint`: wide 563 s, narrow 689 s, lint clean), and
the only refusal either run logs is the end-to-end test's own, which is the refusal it asserts.
Before the rule that test failed nowhere: the `misdescribeAdd` kernel threw `NoSuchMethodError`
on every batch and the row path answered each correctly.

**Correction to section 1's census.** The row's "nothing to fix first" held for hidden failures -
there were none - but not for the tests: two caused a failure fallback without a hook and passed
only by accident, and the random Spark differential, which postdates the 3 October census, meets
a class-loading failure in tasks its own expected errors cancel. Section 2 has the three.
