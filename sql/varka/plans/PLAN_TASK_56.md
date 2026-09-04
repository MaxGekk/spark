# Task 56: `date + INTERVAL n DAY` with a column interval

## 1. Where this came from

`PLAN_MILESTONE_4.md` section 2.23 and row 56, from the coverage survey taken
after the milestone was re-scoped to the date family (4 September 2026): the
analyzer resolves `date + INTERVAL n DAY` with a *column* interval to
`DateAdd(d, ExtractANSIIntervalDays(col))`, and `ExtractANSIIntervalDays` has
no compiler arm, so the entry declines with task 38's "day offset is not a
foldable literal or an integer column". A literal interval never gets here -
it folds to `date_add(d, n)` before planning, which is what the corpus writes.
The owner scoped the task to the date lane alone: a stored `INTERVAL DAY`
column is int64 microseconds and stays declined; the case to cover is the
interval built from an int column, `d + CAST(i AS INTERVAL DAY)` and its
siblings, whose day count is the int itself. The owner added `date - INTERVAL n
DAY` to the scope on the same day.

## 2. The admission check, done

The milestone section says the unwrap is "exact for every int". It is not,
and the check found where it stops.

**The forward direction is exact.** `CAST(i AS INTERVAL DAY)` is
`IntervalUtils.intToDayTimeInterval`: `Math.multiplyExact(i, MICROS_PER_DAY)`.
`ExtractANSIIntervalDays` is `micros / MICROS_PER_DAY`, integer division. For
every `i` the multiply does not overflow, the division returns `i` exactly,
since the product is an exact multiple. So the rewrite `d + i` is Spark's own
value wherever Spark produces one.

**The cast throws past 106751991 days, in every mode.** `multiplyExact`
overflows a long at `|i| > Long.MAX_VALUE / MICROS_PER_DAY = 106751991`, and
`intToDayTimeInterval` turns that into `castingCauseOverflowError` without
consulting the ANSI flag - unlike `date_add(d, i)`, which wraps by spec at any
`i`. A kernel that computed `d + i` for such a lane would answer where Spark
throws. So the rewrite is exact only under a bound on the offset column's
values, and the bound has to be enforced per batch, at run time, because a
column is not bounded at compile time.

**`date - INTERVAL n DAY`** resolves to
`DateAdd(d, UnaryMinus(ExtractANSIIntervalDays(r), failOnError = ANSI))`.
Inside the cast's own bound `UnaryMinus` cannot overflow (`-106751991` is a
valid int), so under the same bound the subtraction is exactly `SubDays(d, i)`
- the negation is absorbed into the node, no `UnaryMinus` arm is needed.

**`d + i * INTERVAL '1' DAY`** resolves to
`DateAdd(d, ExtractANSIIntervalDays(MultiplyDTInterval(INTERVAL '1' DAY, i)))`,
and `MultiplyDTInterval` on an integral `num` is the same `multiplyExact` with
the same bound, so a one-day literal times `i` is `i` under the same rule. A
literal other than one day would need an int multiply in the lanes, which no
node provides, and declines. (The exact trees are pinned by the compiler
suite; if the analyzer's spelling differs from the above, the test says so
before the arm ships.)

**Verified, not asserted:** a unit test (section 5) drives Spark's own
`intToDayTimeInterval` at `LIMIT` and `LIMIT + 1` and `getDays` back, and the
differential suite runs both the in-range and the throwing case through SQL
against the row engine. What the check would have rejected: the plain unwrap
the milestone section described, which would have wrapped silently for
offsets past 292,000 years - a value no query writes, and exactly the kind
that this project's contract says must decline rather than answer.

## 3. The design

### 3.1 The mechanism: a compiler rewrite, and a per-batch bound the evaluator checks

**The compiler** (`compileOffset`, the one place a day offset is resolved)
gains three shapes, all reducing to an `IntegerType` column `i` with a bound:

    ExtractANSIIntervalDays(Cast(i, DayTimeIntervalType(DAY, DAY)))            -> i
    ExtractANSIIntervalDays(MultiplyDTInterval(Literal(1 day), i))            -> i
    UnaryMinus(ExtractANSIIntervalDays(<either of the above>))                 -> i, negated node

The negated form is recognised by the `DateAdd` arm, which builds `SubDays`
instead of `AddDays` (there is no `UnaryMinus` node and none is added). In
every case the arm records an **input bound** for `i`: `[-LIMIT, LIMIT]` with
`LIMIT = Long.MAX_VALUE / MICROS_PER_DAY = 106751991`, a constant in
`VarkaChrono` derived in source, not typed in. The bound rides the compiled
projection as a new field, `inputBounds: Seq[VarkaInputBound]`, keyed by
kernel input index, and is collected through the per-compile `DeclineSink`
(renamed in its doc to what it now is - the per-entry notes the compiler
leaves: a decline, or a bound the evaluator must check) with the same rollback
a declining entry gets for its columns and literals. A plain int column offset
(`date_add(d, i)`) records no bound: Spark's `DateAdd` wraps there too, so the
kernel's wrap is the definition.

**The evaluator**, after it has the batch's morsel addresses and before it
calls `run`: for each bound, a vector range check over the input column -
`IntRangeOps.allWithin(data, validity, nullCount, length, lo, hi)`, a small
hand-written kernel in the engine module beside `DateVectorOps`, null lanes
masked out because their data is undefined - and a `VarkaBatchDeclined` when
any live lane is outside. The existing decline route does the rest: the batch
is recomputed on the row engine, which throws `castingCauseOverflowError` for
the same row Spark would, counted under `numFallbackBatchesDeclined`. The
check lives in the shared batch path, so the filter kernel gets it too.

**Why the evaluator and not the emitter.** Task 52 (#115, open) built a
per-lane guard *inside* the kernel for a different bound; the same block,
parameterised by bound and aimed at an input rather than a producer's result,
would serve here and would cost less than a separate pass. It is not used now
for two reasons: it is on an unmerged branch whose emitter region this task
would otherwise have to merge through, and the evaluator-side check needs no
new IR, no shape-key change and no emitted byte - the reason this task is the
one that can start while two emitter PRs are open. Section 6 measures what the
separate pass costs; if it is more than the emitted form would be, moving it
into the kernel is a follow-up once #115 lands, and section 9 says so with the
number.

### 3.2 What is deliberately unchanged

* The emitter: no node, no option, no byte. Both pinned fixtures stay.
* `date_add(d, i)` and `date_sub(d, i)` with a plain int column (task 38): no
  bound, since Spark wraps there too.
* A stored `INTERVAL DAY` column and every interval not built from an int
  cast or a one-day multiply: declined, with a reason of their own ("day
  interval is not an int column cast to days"), by the owner's decision.
* `date + INTERVAL n HOUR` and every sub-day interval: the analyzer casts the
  date to a timestamp first; milestone 5's lanes.
* Task 52's compile-time range analysis and producer guard: the rewritten
  node *is* `AddDays(d, ColumnRef)`, so when #115 lands it inherits both with
  no edit here.

### 3.3 Registered op counts

None move: no emitted byte changes. The register test in `VarkaLoopEmitterSuite`
is untouched; the compiler suite asserts the rewritten shape equals task 38's.

## 4. Files

| file | what |
|---|---|
| `VarkaChrono.java` | `INTERVAL_DAY_LIMIT_DAYS`, derived from `Long.MAX_VALUE / MICROS_PER_DAY` |
| `VarkaExpressionCompiler.scala` | the three shapes in `compileOffset` and the negated form in the `DateAdd` arm; `VarkaInputBound`; `CompiledVarkaProjection.inputBounds`; the sink's bound notes and their rollback; the new decline reason |
| `sql/varka/engine/.../IntRangeOps.java` (+ test) | `allWithin` over an off-heap int column with its validity, both widths |
| `VarkaKernelEvaluator.scala` | the per-batch check before `run`, declining through the existing route |
| `VarkaExpressionCompilerSuite.scala` | the shapes, the bound, the declines (section 5) |
| `VarkaDifferentialSuite.scala` + `VarkaSharedSessions.scala` | a fixture with an offset past the limit; in-range, out-of-range, subtraction, multiply forms |
| `VarkaKernelEvaluatorSuite.scala` | the check's decline and metric on a hand-built batch |
| `VarkaThroughputBenchmark.scala` + its committed file | section 6 |
| `docs/sql-varka.md` | the surface bullet and the reason string |
| `PLAN_MILESTONE_4.md`, this file | row 56, section 9 |

## 5. Tests, and what each is for

* Compiler: `d + CAST(i AS INTERVAL DAY)` compiles to `AddDays(col, col)` with
  one bound on the offset's input index at `[-LIMIT, LIMIT]`; `d - CAST(i AS
  INTERVAL DAY)` to `SubDays(col, col)` with the same bound; `d + i * INTERVAL
  '1' DAY` to the same node; `d + i * INTERVAL '2' DAY` declines; `d + CAST(s
  AS INTERVAL DAY)` for a short column declines; a stored interval column
  declines with the new reason; `date_add(d, i)` records no bound. The trees
  are built the way the analyzer builds them, so the assertion is on the
  analyzer's spelling as much as on the arm.
* Admission: `IntervalUtils.intToDayTimeInterval(LIMIT)` round-trips through
  `getDays`, `LIMIT + 1` throws - Spark's code as the oracle, so a change
  there fails here first.
* Engine: `IntRangeOps.allWithin` over aligned and unaligned lengths, a
  violation in a loop lane, in a tail lane, under a null lane (must be
  ignored), and none; both widths through the module's narrow profile.
* Evaluator: a batch with one offset past the limit declines and counts under
  `numFallbackBatchesDeclined`; the same batch with that row null does not.
* Differential: in-range column intervals on the projection and filter paths
  match the row engine with nulls on both sides; the out-of-range fixture
  throws the same error through Varka as through the row engine (compared by
  running both); the subtraction and multiply spellings; the stored interval
  column still residual with its reason in `EXPLAIN`.
* Pinned fixtures: none move.

## 6. The measurement

`VarkaThroughputBenchmark` (sql/core), a pair beside the `date_add` rows:
`date_add(d, i)` (no check) against `d + CAST(i AS INTERVAL DAY)` (the check),
columnar consumer, 2M cached rows, both widths, regenerated with
`dev/varka_bench_regen.sh core VarkaThroughputBenchmark` on an idle machine.
The control is the `date_add(d, i)` row itself, which must not move.

### 6.1 Predictions, registered before the run

1. The check costs 10-30% on the bare `date_add` shape through a columnar
   consumer at 256 bits: a vector min/max over 4096 ints is a few hundredths
   of a nanosecond per row, and that shape runs near 0.1 ns/row, so a small
   absolute cost is a visible fraction. Under a row consumer or a calendar
   node it is under 3%.
2. The control row does not move.
3. The out-of-range fixture declines exactly the batches holding a violating
   row, none other; the metric agrees.
   The rule: the check ships regardless - it is what makes the rewrite
   correct - and prediction 1's number decides whether the kernel-side form
   is worth a follow-up after #115.

## 7. Risks

1. **The analyzer's tree differs from section 2's spelling** (a `Cast` that is
   not exactly `DayTimeIntervalType(DAY, DAY)`, a folded multiply). The
   compiler tests build the trees the analyzer's own resolver builds, and the
   differential runs the SQL; a mismatch shows there, not in production, and
   the arm is narrowed to what is real.
2. **A null offset lane's data trips the check.** Masked by validity in
   `allWithin`; the engine test and the evaluator test both hold a violating
   value under a null.
3. **The bound is dropped on a declining entry's rollback** and applied to
   another entry's column. Bounds are keyed by child ordinal until the entry
   is accepted, and dropped with the entry; the compiler suite's two-entry
   case pins it.
4. **The check's cost is larger than predicted.** Measured, and the kernel-side
   form is the recorded follow-up.

## 8. Sequencing

1. This plan and the milestone row.
2. The constant, the engine kernel and its test.
3. The compiler arms, the bound plumbing, the compiler tests.
4. The evaluator check, its test, the differential, the docs.
5. The benchmark pair, one regeneration, section 9.

## 9. Outcome

Filled in when the measurement lands.
