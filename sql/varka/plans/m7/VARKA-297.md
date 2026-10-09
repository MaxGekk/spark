# VARKA-297: Filters that select known rows, and a wider data pool, in the Spark differential

## 1. Where this came from

Row 297 of `m7/PLAN.md`, left by VARKA-262 (7.3 and `m7/READING.md` 11): the random differential
against Spark draws filters from the coverage table's predicates over twelve rows of random data,
and a random predicate over twelve rows selects nothing a good share of the time, which leaves the
filter kernels compared over empty results. Rigger and Su's Pivoted Query Synthesis (OSDI 2020)
fixes that: pick a pivot row, evaluate each predicate on it with an interpreter, and rectify it to
TRUE (keep it if TRUE, negate it if FALSE, test it `IS NULL` if NULL), so the query must return
the pivot. The second half of the row is the data pool: the fixtures' lists never reach a value
outside them, so a constant folded wrongly or a guard one off has nothing to find.

## 2. The admission check, done

Checked on 9 October 2026 on `master` (`beba41ed989`), seed 20261008, 200 compositions:

* **Filters selecting nothing are common.** Before any change, of the filtered compositions the row
  engine answered, 71% returned at least one row (63 of 89 without a pivot in the run below); the
  other 29% compared two empty results. The prediction below is about that figure.
* **The row engine can be the interpreter.** A conjunct evaluated on a one-row relation by the row
  engine gives TRUE, FALSE or NULL, or raises (ANSI overflow on a hostile cell), in which case
  it is dropped. The harness already runs the row engine as the oracle, so no interpreter is
  written.
* **The pool can widen without moving the main stream.** The wide cells are drawn from a
  `Random` of their own, so a composition's outputs, conjuncts, operator, ANSI mode and safe
  cells are the ones the old draw gave; a test asserts the main stream is unchanged.

What the check would have rejected: a hand-written predicate interpreter (the row engine already is
one) and a wide pool drawn from the main stream (it would have moved every earlier seed).

## 3. The design

### 3.1 The pivot

Every third composition with a filter (iteration mod 3 is 1) draws a pivot, one of its data rows.
The suites rectify it (`rectify` in `VarkaSparkDifferential`, where the row engine is): each
conjunct is evaluated alone on the pivot, kept, negated or tested `IS NULL`, or dropped if the row
engine raises. Whatever the operator, every rectified conjunct is TRUE on the pivot, so the pivot
is selected. The check (`pivotMissing`) then asks that the filtered query return the pivot's
outputs, as the row engine computes them over the pivot alone, on each engine; a miss on Varka is
`pivot row not selected by Varka` and a miss on the row engine is `... by the row engine`, which
is the harness's own consistency check. The ternary partition check is unchanged.

### 3.2 The wider pool

On every other pair of compositions (iteration / 2 odd) about half the safe cells are replaced
from a stream of their own: a date within two centuries of the epoch day, an `i` within 5,000, a
month count within 1,000, a year count within 100, a long within 2^40, a time and a
day-time interval to the microsecond. NULL cells and hostile rows are untouched.

### 3.3 The shrinker and the reproducer

The shrinker removes data rows by ddmin as before but keeps the pivot row, since a filter
rectified against it selects nothing else for certain. A reproducer carries an optional
`-- pivot:` line, so a pivot failure replays with its check.

### 3.4 What is deliberately unchanged

The coverage table, the compiler, the emitter, the ternary partition, the safe/full alternation
and the both-error accounting.

### 3.5 Registered op counts

None.

## 4. Files

| file | what |
|---|---|
| `VarkaSparkFuzz.scala` | the pivot, the wide pool, `pivotMissing`, the reproducer's pivot |
| `VarkaSparkDifferential.scala` | `rectify`, the pivot check, the counters |
| `VarkaSparkFuzzSuite.scala`, `VarkaSparkReproducerSuite.scala` | the call sites, the tests |

## 5. Tests, and what each is for

* **Sixty rectified pivot cases pass on the live engines**: the machinery is right when the filter
  returns the pivot on both. A rectification that disagreed with the multi-row evaluation would
  show here as a pivot miss on the row engine.
* **The draw reaches pivot cases and values outside the lists**, and the main stream is unchanged.
* **`pivotMissing` over synthetic outcomes**, including that an error on one side says nothing,
  and the shrinker keeping the pivot row.
* **A reproducer with a pivot parses back to what was rendered.**

## 6. The measurement

Two numbers from the suite's own counters, over 5,000 compositions of the default seed: the
fraction of filtered compositions returning a row, with a pivot and without; and the new
disagreements, if any, against the baseline of the same seed's 5,000 on `master`, which found
none in the nightly of 8 October.

### 6.1 Predictions, registered before the run

1. Every pivot filter returns a row (100%), against 71% for the rest, so the compositions
   comparing empty filtered results fall from 29% to about 20% of all filtered ones.
2. The wide pool does not raise the both-error fraction of the safe pool above 5%.
3. No new disagreement in 5,000: the pivot and the wider pool are expected to find nothing on a
   build where 50,000 compositions found nothing, and a finding would be a bug in Varka or in the
   harness's rectification, either of which is a result.

## 7. Risks

1. **A rectified conjunct's value differs between one row and many**, from constant folding or
   a plan change; the check on the row engine (`... by the row engine`) shows it.
2. **A wider pool raises ANSI errors on both sides** and so compares fewer values; the both-error
   fraction by pool is in the suite's output.
3. **The wide dates cross the Gregorian cutover (1582)**; Spark is proleptic on both engines.

## 8. Sequencing

1. This plan, then the pivot and the pool with their tests, then the measurement and section 9.

## 9. Outcome

Filled in when the 5,000 have run.
