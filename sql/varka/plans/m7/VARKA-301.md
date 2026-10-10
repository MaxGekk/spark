# VARKA-301: Batch size as an axis of the suites

## 1. Where this came from

Row 301 of `m7/PLAN.md`, from the reading of Polars (`m7/READING_POLARS.md` 4): Polars's CI reruns
its whole Python suite at a morsel of four rows, because batch boundaries are where an engine's
edge cases live. Varka sweeps batch size for speed only; its correctness suites run on whatever
batches their cached tables make, and the fuzzers never vary it. The row asks for the Spark
differential and the end-to-end suites rerun at 1, 3 and 17 rows per Arrow cached batch, a skip
list with reasons for tests that assert batch counts, a check that the setting really splits the
cached batches, and the two `VarkaDifferentialSuite` tests that set an ignored knob corrected.

## 2. The admission check, done

Recorded with the row on 9 October 2026: no disagreement at batch sizes 1, 3, 5, 7 and 17 over
1,500 compositions each. Two facts the design rests on, checked here:

* **The Arrow cache's knob is `spark.sql.execution.arrow.maxRecordsPerBatch`.**
  `ArrowCachedBatchSerializer` reads `conf.arrowMaxRecordsPerBatch` to cut its batches; it ignores
  `spark.sql.inMemoryColumnarStorage.batchSize`, which two `VarkaDifferentialSuite` tests set to 32
  and whose `batches > 1` held only because the data spanned two partitions.
* **The matrix already runs a JVM per configuration**, named by `-Dvarka.matrix.config`, so an
  axis is a configuration the sessions read rather than a new runner.

## 3. The design

### 3.1 The axis

* `VarkaMatrix` names it: `arrowBatchSize=<rows>`, which `parse` passes over - it is not an emit
  option - and `arrowBatchSize` reads. Its sizes, 1, 3 and 17 (one row; a batch that is all
  epilogue at any width; just past two lane groups of eight), join `configurations`, so
  `dev/varka_matrix.sh --all` runs them.
* `VarkaSharedSessions` sets the knob in every session it builds from that value.
* A test in `VarkaDifferentialSuite` reads the cache back: every cached batch of a 100-row table
  holds at most the axis's rows, and at a size below 100 the table is split, so the axis cannot
  pass vacuously; under the defaults no batch exceeds the default.
* The two tests set the knob the Arrow cache reads, and restore what was there, so the axis's
  setting survives them.
* `sql/varka/matrix/skips.tsv` takes the tests that assert batch counts, each with its reason.

### 3.2 What is deliberately unchanged

The suites that build their own sessions without `VarkaSharedSessions`; the fuzzers' kernels,
which take their batches from the harness, not the cache; production defaults.

### 3.3 Registered op counts

None move.

## 4. Files

| file | what |
|---|---|
| `VarkaMatrix.scala`, `VarkaMatrixSuite.scala` | the axis, its sizes, its tests |
| `VarkaSharedSessions.scala` | the knob in every session |
| `VarkaDifferentialSuite.scala` | the split check; the two tests corrected |
| `sql/varka/matrix/skips.tsv` | the batch-count tests, with reasons |
| `m7/PLAN.md` | the record |

## 5. Tests, and what each is for

The matrix at each of the three sizes over every Varka suite of both modules, with the memory
sanitizer on, as every matrix run has it.

## 6. The measurement

The matrix run's outcome per size.

### 6.1 Predictions, registered before the run

1. No answer differs at any of the three sizes, as the admission check found for compositions.
2. The skip list grows by tests that count batches - the warm-up's and the batch-count
   assertions - and by nothing that compares answers.

## 7. Risks

1. A test that fails at a small size for a reason that is a real bug, mistaken for a batch count;
   each skip line names the assertion it skips.

## 8. Sequencing

1. This plan. 2. The axis, the check, the two tests, the skip list, the records.

## 9. Outcome, 10 October 2026

The axis as section 3 describes, with one fix the first run asked for: `VarkaMatrix.base` is
computed while the object initialises, so the axis's name is declared before it - the first run at
each size aborted on "no emit option arrowBatchSize", having read the name as null.

Every Varka suite of both modules at each size, with the memory sanitizer on (the defaults: 1,022
passed). Row 303's both-bodies check was not yet on master when this ran; once both are in, a
matrix run at these sizes holds every small batch to both bodies too.

| size | passed | failed before the skip list | cancelled |
| ---: | ---: | ---: | ---: |
| 1 | 934 | 4 | 113 |
| 3 | 937 | 1 | 113 |
| 17 | 938 | 0 | 113 |

The five failures are each a metric a test asserts after comparing its answer - a refused batch,
a declined batch, a batch that reached a kernel - which a small batch leaves at zero for a reason of
the batching, not of Varka: with one row a batch the in-memory scan's statistics prune every batch a
predicate rules out (`null-as-false`, and the crossing lanes of `VarkaTimeArithmeticSuite`), and
with one or three rows no batch holds both a selected and a dropped row, so the filter never
compacts and the string column the two "compacted batch" tests wait for stays Arrow.
`sql/varka/matrix/skips.tsv` takes the five, each with that reason, and a rerun of the two suites at
the three sizes cancels exactly those and fails nothing.

### 9.1 The predictions scored

1. **Holds.** No answer differs at any size.
2. **Holds.** Five skip lines, every one a count asserted after the answer is compared.
