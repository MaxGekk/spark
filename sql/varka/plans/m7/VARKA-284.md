# VARKA-284: Both bodies of every shape compared

## 1. Where this came from

Row 284 of `m7/PLAN.md`: an emitted kernel has two bodies, the dense one a batch with no nulls
takes and the masked one every other batch takes, and a shape a test runs on one kind of batch has
only that body compared with the oracle; the bytes oracle pins the other's bytes, not its answers.
Found by the JaCoCo spike of 4 October 2026 and measured by row 285's report
(`sql/varka/coverage/generated.md`): of 2,803 emitted classes that ran, 733 entered both drivers,
399 only the dense one and 1,607 only the masked one. Done when the report shows every class that
ran entering both drivers, or names each exception with its reason.

## 2. The admission check, done

285's run, grouped by the family a class's name gives (on 285's branch, 10 October 2026):

| family | who emits it | both | dense only | masked only |
| :--- | :--- | ---: | ---: | ---: |
| `VarkaCompositionWide` | the composition fuzzer, and row 276's per-row test | 0 | 148 | 662 |
| `VarkaFusedFuzz`, `...Long`, `...Wide` | the IR fuzzer | 0 | 83 | 559 |
| `VarkaFusedProjection` | the evaluator, in the end-to-end suites | 224 | 111 | 317 |
| `VarkaFusedTest` | the emitter suites' `checkMatrix` and direct calls | 509 | 56 | 65 |

The fuzzers never compare both bodies: `VarkaKernelCheck` runs a kernel on the one batch it drew.
That is 1,452 of the 2,040 one-body classes, from one helper. The emitter suites run the bodies
their null patterns reach. The end-to-end suites run whatever batches their tables give the
evaluator.

## 3. The design

### 3.1 Both bodies, where the harness decides

* **`VarkaKernelCheck`** compares the drawn batch as now, then the same values through the other
  body: a batch that took the masked body again with no nulls, which takes the dense one; a batch
  that took the dense body again with the masked one forced - a null count over a full bitmap,
  or, for one row, that row null, since a forced batch of one row is all-null by contract. No
  random draw is added, so every recorded seed draws what it drew.
* **`checkMatrix`** (`VarkaEmitterTestBase`) runs its combinations as now and, if none took one of
  the bodies, runs that body once more on its first length: forced masked, or null-free.

### 3.2 The exception, named

The end-to-end suites hand the evaluator the batches their tables hold, and which body a batch
takes is the evaluator's decision on its data: a test there is about a query, not a body. They
stay the report's named exception, with that reason, and 285's report gains a table by family, so
the exception is a row of its own rather than a share of a total.

### 3.3 What is deliberately unchanged

The emitter and the evaluator; the end-to-end suites' data.

### 3.4 Registered op counts

None move.

## 4. Files

| file | what |
|---|---|
| `VarkaKernelCheck.scala` | the other body |
| `VarkaEmitterTestBase.scala` | `checkMatrix`'s missing body |
| `dev/varka_gen_coverage/VarkaGenCoverage.java` | the table by family |
| `sql/varka/coverage/generated.md` | regenerated |
| `m7/PLAN.md` | the record |

## 5. Tests, and what each is for

The fuzzers' and the emitter suites' own tests, now comparing both bodies; the regenerated report
is the measure.

## 6. The measurement

`dev/varka_gen_coverage.sh` before (285's report) and after.

### 6.1 Predictions, registered before the run

1. Every class of the fuzzers' families that ran enters both drivers.
2. The suites' time grows by no more than a quarter: the fuzzers run each kernel twice, on the same
   short batches.

## 7. Risks

1. A second body that disagrees with the oracle, which no test saw: a real finding, kept and
   fixed or recorded.

## 8. Sequencing

1. This plan. 2. The harness, the report's table, the run, the records.

## 9. Outcome, 10 October 2026

### 9.1 What was built, and the report after it

As section 3 describes, with two refinements the first run asked for: the report counts a class
that has one driver by construction - a kernel whose nulls come from valid inputs has no dense
body, one that reads no column no masked body - as having entered all it has (213 classes), and
the forced masked run of a one-row batch makes every column null, since a null in a column the
kernel does not read selects nothing.

`sql/varka/coverage/generated.md`, regenerated: of 2,803 classes that ran, 2,029 entered both
drivers (733 before), 213 their only driver, 177 only the dense one, 320 only the masked one and
64 neither. By family, the one-body classes left are:

* `VarkaFusedProjection`, the evaluator in the end-to-end suites: 111 and 306. The named
  exception: the data a query reads decides the body.
* `VarkaFusedTest`, the emitter suites: 50 and 10, the tests that call a kernel directly for one
  property rather than through `checkMatrix`.
* `VarkaFusedFuzz` and `...Long`, the IR fuzzer: 16 and 4. A case whose first comparison throws
  stops there; these are attributed to the planted-failure tests and their shrinking, which throw
  on purpose with a class per candidate, and are not traced to them.
* `VarkaCompositionWide`, the composition fuzzer and row 276's per-row test: none.

Every suite passed with both bodies compared: no second body disagreed with the oracle.

### 9.2 The predictions scored

1. **Holds in part.** Every composition-fuzzer class that ran entered both drivers; 20 IR-fuzzer
   classes did not, for the reason above.
2. **Holds.** The suites took 387 s, against 390 s for row 285's run on the same machine.

### 9.3 What this leaves

The end-to-end suites' one-body classes would need the evaluator to run a batch through both
bodies under test - a null-free batch through the masked body too - which is a change to the
evaluator, not to a harness. Whether that is a row of its own or the named exception stands is
left to the owner.
