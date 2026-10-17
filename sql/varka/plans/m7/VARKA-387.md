# VARKA-387: Deliver the rows before a filter's error, then raise it

## 1. Where this came from

Row 387 of `m7/PLAN.md`, found by VARKA-381's census with nested trees (seed 20261018, iteration
47807; `VARKA-381.md` 9.2) and saved as `sql/varka/fuzz/spark/varka-387-20261018-47807.sql`
(`status: known 387`). Under ANSI, `SELECT make_date(2021, i, 1) AS c0 FROM fz WHERE ymy + ym >=
ym` over two rows raises `DATETIME_FIELD_OUT_OF_BOUNDS` from the first row's month on Spark and
`INTERVAL_ARITHMETIC_OVERFLOW` from the second row's filter on Varka. Spark pushes rows through a
stage one at a time, so for each row the filter and then the operator above it run before the next
row is read; Varka's columnar filter filters a whole batch before any row reaches the operator
above. Row answers follow vanilla Spark, errors included, so this is a wrong answer, and with a
`LIMIT` above it is a failed query that Spark answers.

The owner chose the fix: "deliver the rows before the error, then raise". When Varka's filter
meets an error at row k of a batch, it hands the rows that passed before k to the consumer as a
shorter batch, and raises the error only when the next batch is asked for. It must cost nothing on
batches that do not fail, and no row at or after k may ever be emitted.

## 2. The admission check, done

A throwaway probe suite (ANSI on, one partition, Arrow-cached rows `(x, y, k, d)` =
`(Long.MaxValue, 0, 1, 1)`, `(1, 0, 2, 1)`, `(0, Int.MaxValue, 3, 0)`, `(0, 0, 4, 1)`, so `y + 1 >
0` overflows on row 3) ran on 17 October 2026 on this branch's base, printing each query's
executed Varka plan and both engines' outcomes:

| query | Varka plan (top to bottom) | Spark | Varka |
|---|---|---|---|
| `SELECT k ... WHERE y + 1 > 0` | `VarkaFilterColumnarToRow` | `ARITHMETIC_OVERFLOW` | `ARITHMETIC_OVERFLOW` |
| `SELECT make_date(2021, k + 12, 1) ... WHERE y + 1 > 0` | `VarkaColumnarToRow < VarkaFilter` | `DATETIME_FIELD_OUT_OF_BOUNDS` | `ARITHMETIC_OVERFLOW` |
| `SELECT date_add(DATE'2021-01-01', k) ... WHERE y + 1 > 0 LIMIT 1` (and `LIMIT 2`) | `CollectLimit < VarkaColumnarToRow < VarkaFilter` | the first rows | `ARITHMETIC_OVERFLOW` |
| `SELECT count(c), max(c) FROM (SELECT make_date(2021, k + 12, 1) AS c ... WHERE y + 1 > 0)` | `HashAggregate < VarkaColumnarToRow < VarkaFilter` | `DATETIME_FIELD_OUT_OF_BOUNDS` | `ARITHMETIC_OVERFLOW` |
| `SELECT max(date_add(DATE'2021-01-01', k)) ... WHERE y + 1 > 0` | `HashAggregate < VarkaFilterColumnarToRow` | `ARITHMETIC_OVERFLOW` | `ARITHMETIC_OVERFLOW` |
| `SELECT make_date(2021, k + 9, 1) ... WHERE y + 1 > 0` (projection raises on row 4) | `VarkaColumnarToRow < VarkaFilter` | `ARITHMETIC_OVERFLOW` | `ARITHMETIC_OVERFLOW` |
| `SELECT date_add(DATE'2021-01-01', k) ... WHERE y >= 0` | `VarkaColumnarToRow < VarkaFilter` | four rows | four rows |

What it shows, and what it would have rejected:

* **Every disagreement has the columnar `VarkaFilterExec` under it**, and every query over the
  row-out `VarkaFilterColumnarToRowExec` agrees. The code says why: the row-out node's fallback is
  `rowIterator.filter(predicate).map(toUnsafe)`, which evaluates the predicate one row per pull, so
  a consumer gets each row before the predicate meets the next one, as in Spark. The columnar
  node's fallback loops over the whole batch into a writable batch and throws out of that loop. If
  the row-out node had disagreed too, the fix would have had to reach the lazy iterator as well.
* **The kernel path never raises a user's error.** A lane whose answer would raise declines the
  whole batch (`VarkaBatchDeclined`), and the evaluator's `serveBatch` sends it to the node's
  per-row fallback, which raises Spark's own error from Spark's own expression. So the fallback is
  the one place an error meets row k, and the only place to change.
* `count(make_date(...))` is not a discriminating aggregate under ANSI: `make_date` is non-nullable
  over non-null arguments there, so the optimizer counts rows without evaluating it. The aggregate
  case is `count(c), max(c)` over a subquery's projection, which `CollapseProject` keeps because
  `c` is used twice.

## 3. The design

### 3.1 The columnar filter's fallback stops at the first raising row and defers its error

`VarkaFilterExec`'s per-partition evaluator (`VarkaFilterEvaluatorFactory.PartitionFilterEvaluator`)
gains one field, `pendingError: Throwable`.

* **`fallback(input)`** evaluates the predicate row by row as today. When the predicate throws a
  non-fatal error at row k and at least one earlier row passed, it stops: the output batch is cut
  to the rows that passed (`setNumRows(rowCount)`), tracked like any output batch, and returned,
  and the error is kept in `pendingError`. No row at or after k is converted. When no row before k
  passed, the error is thrown at once, as today: an empty batch followed by the error is the same
  thing observably, and costs an allocation more. A fatal error, or one thrown by the row
  conversion rather than the predicate, is thrown at once as today, closing the batch.
* **The iterator** checks `pendingError` first in both `hasNext` and `next`, before asking the
  child for anything (the child may be exhausted while an error is pending); it releases the
  current batch and throws. The error is not cleared, so a consumer that asks again gets it again
  and never a later batch.

The consumers above pull a batch only when they have consumed the previous one:
`VarkaColumnarToRowExec` through `flatMap`, which asks the outer iterator only once the inner rows
are exhausted, `VarkaProjectExec` through `hasNext`/`next` one batch at a time, and the cache
writer the same. So the operator above sees the rows before k, does with them what Spark's
operator does - a projection raises its own earlier error, a `LIMIT` stops and answers, an
aggregate raises if those rows overflow it - and only then meets the filter's error, at the point
in the row order where Spark meets it. A `LIMIT` that stops early never asks again, and the error
is dropped with the task's iterator, as Spark never evaluates that row.

The shortened batch is an `OnHeap`/`OffHeapColumnVector` batch, not Arrow, so a Varka projection
above it fails `canRun` and takes its own per-row path over those rows. That is why the mirror case
holds by construction: a projection that would raise on row j > k never sees row j, because the
filter stopped at k. It is also why a projection with several kernels above the filter runs no
kernel on the failing batch: its other batches go through its kernels as before.

A batch the fallback filters without an error, and every batch the kernel serves, run exactly the
code they run today; the only added work per batch is one field read in `hasNext` and in `next`.

### 3.2 What is deliberately unchanged

* **The row-out filter `VarkaFilterColumnarToRowExec`**: already lazy per row (section 2), so it
  delivers the rows before k first. It is covered by tests here, not changed.
* **The kernel path** (`filterMask`, `filterCompact`) and the emitter: no user error is raised
  there. No emitted byte changes.
* **The columnar projection's fallback** (`VarkaProjectExec`) has the same eager per-batch shape:
  a projection raising on row 3 of a batch under a consumer that would stop after row 1. It is
  reachable only under a columnar consumer - the rule gives every row consumer the lazy
  `VarkaColumnarToRowExec` instead - and no columnar consumer stops early (the cache writer reads
  everything), so no query can see the difference. Not changed here.
* **Row 380** (a fused conjunct running before a residual conjunct Spark evaluates first) and
  **row 386** (Varka raising past a NULL conjunct Spark stops at) are about the order of the
  conditions within one row; this row is about the order of rows between two operators. Neither is
  touched, and both still hold `-Dvarka.sparkfuzz.nested=false` in the nightly.
* **The fallback-rows metric**: `serveBatch` charges the whole input batch's rows to
  `varkaFallbackRows` for a batch the fallback stopped in. It counts the rows the fallback was
  given, as it does for every fallback batch, so it stays as it is.

### 3.3 Registered op counts

None: no emitter change.

## 4. Files

| file | what |
|---|---|
| `sql/core/.../execution/VarkaFilterExec.scala` | the fallback's stop-and-defer, the iterator's check, class docs |
| `sql/core/src/test/.../execution/VarkaFilterRaiseOrderSuite.scala` | new: the differential cases of section 5 (replaces the throwaway probe suite) |
| `sql/varka/fuzz/spark/varka-387-20261018-47807.sql` | `status: regression` |
| `sql/varka/fuzz/spark/README.txt` | the 387 file is a regression |
| `sql/varka/plans/m7/PLAN.md` | row 387's outcome |
| `dev/varka_nightly.sh` | the nested switch's comment names 380 and 386, the rows that still hold it |
| `sql/varka/plans/m7/VARKA-387.md` | this plan |

## 5. Tests, and what each is for

A new focused suite, `VarkaFilterRaiseOrderSuite` (`QueryTest with VarkaMaybeThrows with
VarkaTestWatchdog`), ANSI on, over its own one-partition Arrow-cached fixture with the filter's
raising row and the projection's raising row placed independently. Each case compares Varka's
outcome (rows or error, condition and parameters) with vanilla Spark's by
`checkSparkAnswerMaybeThrows`, and asserts which Varka filter node the plan holds, so that no case
agrees because no Varka filter was planned. Each runs at three Arrow batch sizes, set around the
`cacheTable` (the serializer reads it at cache time): the whole fixture in one batch (k mid-batch),
and a size that puts the filter's raising row first in its batch (k at a batch boundary, where the
fallback has no earlier row to deliver), and one that puts it last.

1. **The reproducer's shape**: a projection that raises on an earlier row above a fused filter
   that raises on a later one. Spark's projection error; before the fix Varka's filter error.
2. **`LIMIT 1` and `LIMIT 2` above a projection** over that filter: Spark answers; before the fix
   Varka raised.
3. **An aggregate above**: `count(c), max(c)` over the projection; Spark's projection error.
4. **No error from the consumer**: the projection is clean, so the filter's error is the answer on
   both.
5. **A batch with no error**: a filter and projection that raise nowhere; rows on both, and the
   kernels ran (the no-change case).
6. **The mirror case**: the projection raises on a row after the filter's raising row; the filter's
   error on both.
7. **The row-out node**: the same filter under an aggregate with no projection between (`max` of a
   raising expression, which `CollapseProject` folds into the aggregate), and under a bare
   `LIMIT`; pins that the lazy path agrees.
8. **The residual split**: a fused raising conjunct beside a residual conjunct Varka cannot fuse,
   so the fallback filters with the whole condition and a row `FilterExec` sits above.
9. **The split predicate** (`splitConditions`): a predicate past one method's budget compiled into
   several selection outputs, with the raising conjunct in it.
10. **Several kernels above**: a projection past the sixty-four columns one kernel reads, with the
    earlier-raising entry in it, over the raising filter.

The existing gate suites (`VarkaFilterExecSuite`, `VarkaDifferentialSuite`,
`VarkaSparkReproducerSuite`, `VarkaSparkFuzzSuite`'s PR arm) show nothing else moved; the
reproducer suite holds the 387 file to `status: regression`.

## 6. The measurement

No benchmark is regenerated: the kernel path and the emitted code are unchanged, and the
fallback's added work is a `try` around the predicate call, which costs nothing until it throws.
The per-batch iterator gains one field read. The filter benchmark pair that would show a cost, if
one were wanted, is `VarkaFilterBenchmark` against the merge base (`dev/varka_bench_pair.sh`).

### 6.1 Predictions, registered before the run

1. Cases 1, 2, 3 and 10 disagree before the fix and agree after it, at every batch size where the
   filter's raising row has an earlier passing row in its batch; at the boundary size they already
   agree before the fix. Cases 4, 5, 6, 7, 8 and 9 agree before and after.
2. The new suite's tests take under 10 s and the suite under 30 s with its sessions, at VARKA-244's
   and VARKA-318's ratios for a short sql/core suite (0.6 and 0.37) applied to an estimate of 20
   queries at about 0.3 s each.
3. The reproducer flips to agreement, and a census of 15,000 nested compositions at seed 20261018
   shows no disagreement of a kind the base did not show (380 and 386 kinds may appear).

## 7. Risks

1. **A consumer that prefetches the next batch** before it has consumed the current one would meet
   the error before the rows. None of today's consumers does (3.1); case 1 at every batch size
   would show one.
2. **A deferred error outliving a task kill**: a kill interrupts the task; the error is only held
   until the next pull, which a killed task does not make. Nothing to test.
3. **A row at or after k emitted**: the loop stops at k before converting it; cases 2 (`LIMIT 2`,
   where the answer must be exactly the two rows before k) and 6 would show one.
4. **`failureFallbackForbidden` under test**: the decline is a designed decline, not a failure
   fallback, so the suites' refusal does not fire; the probe ran under it.

## 8. Sequencing

1. This plan.
2. The fix in `VarkaFilterExec.scala` with its docs.
3. The suite, the probe deleted.
4. The reproducer's status, its README, row 387 and the nightly's comment.
5. The gate and the census under the shared lock; section 9.
