# VARKA-308: A session conf for exactRefusalStatus

## 1. Where this came from

Row 308 of `m7/PLAN.md`, filed by VARKA-304 (`VARKA-304.md` 3.3). VARKA-304 gave each runtime
refusal reason its own status bit, kept one guard mask per method by default (a method of several
reasons reports all of them), and added the emit option `exactRefusalStatus`, which gives each
reason its own mask. The option reached a kernel only through tests and `dev/varka_emit.sh`: a
production query could not be rerun with exact attribution. The session conf had to thread
through the exec nodes' evaluator factories, which row 305's pull request was changing; that has
merged.

## 2. The admission check, done

Surveyed on 11 October 2026 at `6db56505123`.

**How a session's emit setting reaches a kernel today.** One does: `spark.sql.codegen.varka.emit.
useAVX`. The exec node reads it from the session's `SQLConf` on the driver and passes it as a bare
`int emitUseAVX` through five call sites in the three exec nodes and `VarkaColumnarRule`, three
evaluator factories, four evaluator constructors and `VarkaEvaluatorBase`, which applies it in
`VarkaColumnarToRowExec.emitOptions(int)` - the one place a kernel's options are derived, asked by
the compiler at planning and by the evaluator on the executor, which must agree. One test passes
the `int` (`VarkaKernelEvaluatorSuite`).

**What the check decides.** A second bare parameter beside the first would double those
signatures for one flag, and the next session-level setting would do it again. The project's
config model (resolve the string surface once into typed values, at one named moment) says the
same: the session's emit settings become one small record, bound from `SQLConf` where the exec
node reads it today, and carried in place of the `int`.

**The observable.** VARKA-304 put the status word on the JFR `Fallback` event, and
`VarkaProjectExecSuite` already records that event around a declined batch
(`VarkaJfrTestSupport`), so the conf can be proven end to end: a query whose kernel holds two
reasons, data that fires one, and the event's status with the conf off and on.

What the check would have rejected: a second `int`-like parameter through every signature.

## 3. The design

### 3.1 The conf

`spark.sql.codegen.varka.emit.exactRefusalStatus`, internal, boolean, false by default, binding
policy `NOT_APPLICABLE` (it changes what a declined batch reports, never what a view body
resolves to), version 5.0.0 as `useAVX`. It is part of the shape key through the options, so a
session that sets it emits and caches its own classes.

### 3.2 The record

`VarkaSessionEmit(int useAVX, boolean exactRefusalStatus)`, a Java record in
`org.apache.spark.sql.execution`, `Serializable` because the factories carry it to the
executors; `of(SQLConf)` is the binding, `DEFAULTS` what a suite passes, and
`apply(VarkaEmitOptions)` lays the session's settings over the base options.
`VarkaColumnarToRowExec.emitOptions` takes it in place of the `int`, and every signature that
carried `emitUseAVX` carries it instead.

### 3.3 What is deliberately unchanged

`useAVX`'s meaning and default; the options' rendering and the shape key; every emitted byte
under the defaults.

### 3.4 Registered op counts

None: no emitter change.

## 4. Files

| file | what |
|---|---|
| `SQLConf.scala` | the conf and its accessor |
| `VarkaSessionEmit.java` | new: the record |
| `VarkaColumnarToRowExec.scala`, `VarkaProjectExec.scala`, `VarkaFilterExec.scala`, `VarkaColumnarRule.scala` | bind it, pass it |
| `VarkaEvaluatorBase.java`, `VarkaKernelEvaluator.java`, `VarkaFilterEvaluator.java`, `VarkaKernelPart.java` | carry it |
| `VarkaKernelEvaluatorSuite.scala`, `VarkaProjectExecSuite.scala` | the record; the end-to-end test |
| `docs/sql-varka.md` | the conf, beside `exactRefusalStatus` |

## 5. Tests, and what each is for

* **End to end**: a projection whose kernel holds two refusal reasons, over data that fires one,
  declined and recorded through the JFR `Fallback` event: with the conf off the status names both
  reasons, with it on only the one that fired. A conf that never reached the executor, or reached
  the planner and not the evaluator, fails it.
* **The binding**: `VarkaSessionEmit.of` reads both settings, and `apply` leaves the base options
  untouched at the defaults.
* **`SparkConfigBindingPolicySuite`**: the new conf declares its policy.
* **The gate**, both widths.

## 6. The measurement

None: no emitted byte changes under the defaults.

### 6.1 Predictions, registered before the run

1. `emitted_bytes.json` and every committed benchmark file are untouched.
2. The end-to-end test reads two bits with the conf off and one with it on.

## 7. Risks

1. **A path that bypasses the record.** `VarkaColumnarRule` asks the compiler with the session's
   options at planning; if it bound its own and the evaluator another, the plan and the task
   could classify a projection differently. Both go through `VarkaSessionEmit.of` and
   `emitOptions`, which the end-to-end test exercises from plan to task.

## 8. Sequencing

1. This plan; row 308 marked Planned.
2. The conf, the record, the threading and the tests.
3. The outcome.

## 9. Outcome

Done on 11 October 2026, in one commit after the plan.

**End to end.** `year(date_add(d, i + i))` under ANSI holds an overflow guard on the sum and a
guard on the day `year` reads, in one method. With `i` at `Int.MaxValue` the sum wraps to -2,
which keeps the day in range, so only the overflow guard fires. The declined batch's status, read
off the JFR `Fallback` event, is 9 (`DAY_PRODUCER | INT_OVERFLOW`) with the conf off and 8
(`INT_OVERFLOW`) with it on.

**Predictions scored.**

1. **Held.** No committed bytes or benchmark file moves: at the defaults `VarkaSessionEmit.apply`
   returns the base options themselves, which a test asserts.
2. **Held**, after the test's data was corrected. Its first version overflowed `i + 1`, which
   wraps to `Int.MinValue` and takes the day out of range as well, so both guards fired and the
   option correctly reported both: 9 either way. `i + i` fires the overflow alone.

**The checks.** `SparkConfigBindingPolicySuite` passes with the new conf. The gate passes; its
first lint run caught two imports of `VarkaEmitOptions` the threading left unused in the
evaluators.

**Review (11 October 2026).** A `/code-review` found nine points, all taken:

* The settings were not bound once, as the record's documentation and section 3 said:
  `doExecute` read the session's conf again, so a plan executed again after the conf changed
  could classify the projection with one set of options at planning and emit it with another in
  the task. Each Varka exec node now holds its `VarkaSessionEmit` as a constructor field, read
  from the conf when the columnar rule builds the node and kept by Spark's copies, and both the
  planning-time classification and the tasks read that field. The end-to-end test checks it: a
  node built with the conf on and run with it off still emits the exact masks.
* `apply` could only turn a setting on, so a session's `false` could not override a test hook left
  on by a suite earlier in the same JVM, and the new test failed under the option matrix's
  `exactRefusalStatus=true` configuration depending on suite order. The conf is now optional:
  unset leaves the base options, set (`false` included) overrides them, and the test sets `false`
  explicitly.
* The conf's documentation said the option never changes whether a batch is declined; its extra
  masks add bytes, so near the byte budget an entry can fall back where it fused without them. The
  documentation now says so.
* The record checks `useAVX >= -1` in its compact constructor; the evaluator derives its options
  once in its constructor and `apply` uses one builder; two comments that called `useAVX` the one
  production knob, and the hook the alternative to a SQLConf entry, are corrected.

Corrections to this plan, which is a record and is not rewritten: 5 says the end-to-end test
covers the rule's path from plan to task; it builds the exec node directly, so the rule's own
eligibility and filter rewrite are not run under the conf. 4's files table puts the record test in
`VarkaKernelEvaluatorSuite`; it is in `VarkaEvaluatorComponentsSuite`.
