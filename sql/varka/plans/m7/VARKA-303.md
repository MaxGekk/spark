# VARKA-303: Both bodies in the end-to-end suites

## 1. Where this came from

Row 303 of `m7/PLAN.md`, found on 10 October 2026 by row 284 and made a task by the owner. A
kernel has a dense body, which a batch with no nulls takes, and a masked one, which every other
batch takes. Row 284 made the fuzzers and the emitter suites compare both bodies of every shape
they run; its report (`sql/varka/coverage/generated.md`) left the evaluator's classes, those the
end-to-end suites compile, as the named exception: 111 entered only the dense driver and 306 only
the masked one, because the batches a query's tables hold decide the body, and no harness change
reaches them.

## 2. The admission check, done

Built first as a prototype in `VarkaKernelRunner`, since the question was whether the check could
be stated at all for a query's batches:

* **A null-free batch** goes through the masked body forced - a null count of one over a full
  bitmap per input - and every row of every output must agree with the dense body's. One row is
  the exception: a null count equal to the length is the all-null column by contract.
* **A batch with nulls** goes through the dense body, which reads every lane as valid, and the
  rows where every input is valid must agree. The dense run may decline, since a null lane holds
  whatever value a guard then reads; a declined run compares nothing.
* **A planted disagreement fails the task.** With the comparison made to fail on row 0,
  `VarkaCoverageDifferentialSuite` failed with "dee952db4f2a64e9's dense and masked bodies
  disagree on output 0 at row 0 of a nullable batch of 5 (VARKA-303)", which shows the check runs
  in the end-to-end suites' JVM and is not skipped.
* **Without it planted**, that suite and `VarkaDifferentialSuite` pass with the check on: 183
  tests.

What would have rejected it: a check that could not tell a correct decline from a wrong answer,
or one that disagreed on correct kernels.

## 3. The design

### 3.1 The check

* `VarkaKernelRunner.invoke`, after a batch the kernel served (status zero), runs the same inputs
  through the other body into buffers of its own, registered with the memory sanitizer, and
  compares, as section 2 describes. A disagreement, a throw from the other body, or the masked
  body declining a null-free batch the dense one served raises `IllegalStateException` naming the
  shape, the output, the row and VARKA-303, which fails the task under test.
* The runner learns each output's width (`dstWidth`, set where `dstData` is: the vector's width
  for a projection, zero for a filter's selection bitmap).
* `-Dvarka.checkBothBodies=true` turns it on, read once into `CHECK_BOTH_BODIES`, and
  `dev/varka_matrix.sh` puts it on every test JVM, as it does `-Dvarka.sanitizeMemory`. Off in
  production.

### 3.2 What is deliberately unchanged

The emitter and the kernels; the evaluator's answer, which is the served body's; the fuzzers'
and emitter suites' own both-bodies runs (row 284).

### 3.3 Registered op counts

None move.

## 4. Files

| file | what |
|---|---|
| `VarkaKernelRunner.java` | the check, `dstWidth`, the flag |
| `VarkaEvaluatorBase.java`, `VarkaFilterEvaluator.java` | `dstWidth` set |
| `dev/varka_matrix.sh` | the flag on every test JVM |
| `sql/varka/coverage/generated.md` | regenerated |
| `m7/PLAN.md` | the record |

## 5. Tests, and what each is for

Every Varka suite of `catalyst` and `sql/core` at both widths with the check on: every batch an
end-to-end suite serves meets its other body. The planted disagreement of section 2 is the check
that the check runs.

## 6. The measurement

The coverage report regenerated: the evaluator's family should enter both drivers.

### 6.1 Predictions, registered before the run

1. Every `VarkaFusedProjection` class that ran enters both drivers, but for the classes with one
   driver by construction, the batches of one row, and the classes whose only batches had nulls
   and whose dense run declined.
2. No suite fails: no kernel the suites run answers one batch two ways.

## 7. Risks

1. A batch with nulls whose dense run never completes - every such batch declining - leaves that
   class one-bodied, which the report shows by family.

## 8. Sequencing

1. This plan. 2. The check, the flag, the regenerated report, the records.

## 9. Outcome, 10 October 2026

The check as section 3 describes. `dev/varka_gen_coverage.sh`, which runs every Varka suite
through `dev/varka_matrix.sh` and so with the check on, regenerated the report: of 2,805 classes
that ran, 2,427 enter both drivers (2,029 before). The evaluator's family, `VarkaFusedProjection`:

| | both drivers | its only driver | only the dense one | only the masked one | neither |
| :--- | ---: | ---: | ---: | ---: | ---: |
| before (row 284) | 224 | 11 | 111 | 306 | 1 |
| after | 622 | 11 | 15 | 4 | 1 |

### 9.1 The predictions scored

1. **Holds, by attribution.** 19 evaluator classes still run one body. The report attributes them
   to the two cases the check leaves: a null-free batch of one row, and a batch with nulls whose
   dense run declined. They are not traced class by class.
2. **Holds.** Every suite passed with the check on, 1,029 tests in 543 s: no kernel the suites run
   answers one batch two ways.
