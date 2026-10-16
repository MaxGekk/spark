# VARKA-379: Far-day dates outside 0001-9999 get wrong calendar fields

## 1. Where this came from

Row 245's refinement proofs (`m7/VARKA-245.md` 9.2 item 1, on branch `varka-245`, #868) found
that the range analysis reads a bare date column as 0001-01-01 to 9999-12-31 and nothing checks
it. Spark stores any int32 day count, and a cached column can hold one; a calendar field over such
a column fuses with no per-lane check and is served wrong. That is a wrong answer, not a failure,
so the ghost fallback cannot catch it - the case `sql/varka/AGENTS.md` names ("Anything that can
return a wrong answer rather than fail ... gets its own differential coverage") - and it sits
under milestone 7's done-when list item 2, the tests reaching what they miss, and item 1, whose
proofs are stated over guarded domains: a domain assumed rather than guarded is not one.

## 2. The admission check, done

The reproduction, end to end on master `e0acfd4fa4a` (worktree `varka-repro-farday`,
16 October 2026): a cached view `SELECT v, date_from_unix_date(v) AS d` in one Arrow batch, each
query run on Varka and on the row engine.

- The kernel served the batch (`numVarkaBatches=1`, nothing declined, no fallback) and answered
  wrong: 108 rows differed over the shapes tried.
- The last correct days are 14766813 (42400-02-29) at the top and -5394572 at the bottom; one
  day past either is wrong. At 14766814 `dayofmonth` is 2 against Spark's 1; at 100000000 `year`
  is 69873 against 275760 and `month` is 31848.
- `quarter`, `weekofyear`, `last_day` and `trunc(d, 'MM')` are also wrong past the range;
  `dayofweek`, `weekday`, the ISO day of week and `unix_date` are right for every int.
- A column plus a literal (`year(date_add(d, 1))`) is wrong the same way. A column-offset
  producer (`date_add(d, i)`) is already guarded and declines.

The three places the code decides there is no guard: `VarkaRangeAnalysis.range`'s `ColumnRef`
arm answers `CONTRACT` unconditionally; `VarkaChronoCompiler.admitCalendar`'s first arm admits a
node whose interval decomposes exactly with no `GuardedDay`; and `Analysis.collectRefusals`
refuses only a column-offset `AddDays`/`SubDays`.

A second consumer of the same assumption, found while enumerating them for this plan: the `INT`
query. `datediff(a, b)` over two columns is bounded by the bound's width, 3.65 million, so under
ANSI `datediff(a, b) * 100` and `-datediff(a, b)` fuse with their overflow checks removed. With a
far day Spark raises and the kernel would wrap. Same cause, same fix; `VarkaFarDaySuite` has it.

What the check would have rejected: a design that only covers calendar admission (the `INT`
path stays open), and one that bounds every date column (it would decline batches for
`dayofweek` and the plain reads, whose lowerings are exact for every int).

## 3. The design

Two designs were named. **(b), widening the decomposition so it is exact over every int32 day,
is not built: it was built once and measured.** `VARKA-26.md` 11.2's `TOTAL` lowering covered the
whole int range without a guard and cost 14 to 24% against the narrowed one, depending on width
and null pattern; the owner chose `NARROWED` and `TOTAL` was removed, on the guard's strength.
The guard that decision rested on covered producers and not bare columns, which is this bug;
the price of totality has not changed, so (b) would buy back a range no SQL literal reaches at
the cost the owner already declined. There is therefore no losing arm to keep as an option.

### 3.1 The column bound, checked per batch

(a), and through machinery that already exists rather than a new IR node: the compiler's input
bounds (`DeclineSink.bound`, carried in `CompiledVarkaProjection.inputBounds`, checked in
`VarkaKernelRunner.fill` over each batch's live lanes by `IntRangeOps.allWithin`, declining with
`STATUS_INPUT_BOUND`), which the `CAST(i AS INTERVAL DAY)` day offset already uses. One vectorised
min/max pass over the column before the kernel, and the batch goes to the row engine when a live
day is outside.

- `VarkaRangeAnalysis.range` takes an `IntConsumer` and reports the input slot of every date
  column whose `CONTRACT` its answer rests on. Not reported: a column under a `GuardedDay`, under
  `MakeDate`, or under a column-offset day shift, whose own result the producer guard checks. The
  four-argument form reports nothing, for callers that do not act on the answer.
- `VarkaChronoCompiler.admitCalendar` asks once more on the node it admits (after `rearm`, so a
  column below a placed `GuardedDay` is not bounded) and notes the bound on each reported column.
- `VarkaNodeCompiler.boundedByIntMax`, which removes an overflow check, notes the bound on the
  columns its magnitude rested on - only when it answers yes.
- `DeclineSink` learns a slot-keyed bound, so the analysis's report (slots, as `ColumnRef`
  carries) needs no table; it rolls back with the entry like every other bound.

The bound noted is `CONTRACT` itself, so every compile-time decision is what it was: the same
shapes admit and decline, and the analysis's statement becomes a checked one. A day in
`[NARROW_MIN_DAYS, CONTRACT_MIN_DAYS)` or past 9999 that a kernel served correctly before now
declines too: a slower path for data no SQL literal writes, accepted for a bound that is one
constant. A per-shape bound - the widest interval the admitted shape is still exact over - would
serve those days and is the follow-up if a workload ever holds them.

### 3.2 What is deliberately unchanged

- The IR, the emitter and `Analysis.collectRefusals`: no node, no refusal, no option. The input
  bounds are not in `VarkaShapeKey`, so the emitted bytes do not move (`emitted_bytes.json`
  unchanged, checked by `VarkaEmittedBytesSuite`).
- The proof files: their constants are unchanged; `sql/varka/proofs/README.md` says the leaf
  interval is a premise the evaluator now discharges.
- The fuzzers' value draws: the Spark-level fuzzers (`VarkaSparkFuzz*`) are row 298's; whether
  they draw days past the bound is reported to the owner rather than edited here. The IR fuzzer
  calls kernels below the evaluator, where the bound is a precondition, so it must not draw them.
- The column-offset producer guard and its option `guardDayProducers` (VARKA-52, VARKA-93).

### 3.3 Registered op counts

None move: no kernel changes. `VarkaEmittedBytesSuite` is the check.

## 4. Files

| file | what |
|---|---|
| `VarkaRangeAnalysis.java` | the column report; `CONTRACT`'s doc says it is checked |
| `VarkaChronoCompiler.java` | `boundColumns` on admission |
| `VarkaNodeCompiler.java` | `boundedByIntMax` notes the bound (one signature; row 318 also edits this file) |
| `VarkaIntervalCompiler.java` | `negationMode` passes the sink |
| `DeclineSink.java` | slot-keyed bounds, `dayColumnBound` |
| `VarkaChrono.java`, `proofs/README.md` | the bound's wording |
| `VarkaFarDaySuite.scala` | the differential |

## 5. Tests, and what each is for

`VarkaFarDaySuite` (sql/core), every query on both engines with `VarkaMaybeThrows`' comparison
(same rows or the same error), and each answered query asserting it reached a Varka node, no
kernel failed, and every batch was served or declined:

1. The reproduction: `year`, `month`, `dayofmonth` over the reproduction's days, dense and with
   a null row (both bodies); each must decline on the input bound.
2. Every row of `coverage.json` over the date, int and year-month columns, over days past the
   bound and int32's ends, dense and nulled. The rows whose lowering is exact for every int
   (`dayofweek`, `weekday`, ISO day of week, `unix_date`, `datediff`, the predicates) must not
   decline on the input bound - the check is not over-applied.
3. The bound's edges serve with no input-bound decline; one day past either declines.
4. A column plus a literal, and filters (`WHERE year(d) = 275760`).
5. ANSI `datediff(d, d2) * 100` and `-datediff(d, date_from_unix_date(0))` raise as Spark does past the
   bound, and still fuse and serve inside it.

The suite is the oracle for the wrong answer; it fails on master (section 2's reproduction is its
first test).

## 6. The measurement

The check is one pass per bounded column per batch, so the cost is per batch and shows where
batches are small. A pair on GitHub runners (`dev/varka_bench_pair.sh` through `benchmark.yml`,
base the merge base), two classes, since the catalyst benchmarks call kernels below the evaluator
and never run the check:

- `VarkaEvaluatorOverheadBenchmark`: `trunc(d, fmt)` is bounded now; `date_add(d, 3)`,
  `least(l, ...)` and the filter `d > DATE'2000-01-01'` are not, and are the controls.
- `VarkaThroughputBenchmark`: `year`, `month`, `day`, `weekofyear`, `year(d) * 100 + month(d)`
  are bounded; `dayofweek` is not, the control.

### 6.1 Predictions, registered before the run

Stated at the calibration table's lesson (`skills/testing-and-debugging.md`): a per-batch check
costs more than the kernel arithmetic says, so these allow for the harness.

1. `VarkaEvaluatorOverheadBenchmark`, `trunc(d, fmt)`: slower by 2 to 8% at 1 and 16 rows a batch
   (a pass that maps the segments and broadcasts two vectors, about 20 to 50 ns against 712 and
   1237 ns), and within 3% at 1024 rows, where the pass is under 1% of a 30 us batch. The three
   controls within the runner's band.
2. `VarkaThroughputBenchmark`: every bounded row within 3% (a 4 KB pass beside a decomposition of
   tens of operations a lane, and the query's scan and row conversion); `dayofweek` unmoved.
3. No row fails the pair's 10% threshold.
4. `VarkaFarDaySuite` costs under 60 s including its sessions, its tests under 30 s (about 250
   queries over a one-batch cached view, at VARKA-243's ratio of 2 on 0.05 s a query).

## 7. Risks

1. A consumer of the column's range not found: anything that reads `CONTRACT` without going
   through `range` with a collector. Grepped: `CONTRACT` is read only by the `ColumnRef` arm, and
   the analysis is acted on in `admitCalendar` and `boundedByIntMax` only. Test 2 runs every
   coverage row past the bound.
2. Over-application costing served batches: test 2's no-decline set, and the pair's controls.
3. A slot-keyed bound pointing at the wrong input after a rollback: slots only append and are
   truncated with the bounds at the same marks; the existing differential suites and the option
   matrix run every shape.
4. Tests that pin a compiled plan's `inputBounds` as empty for a calendar shape: the catalyst
   suites show them.

## 8. Sequencing

One commit for the plan, one for the fix with its suite; the pair dispatched by the owner.

## 9. Outcome

Built as planned (16 October 2026), on branch `varka-379`; the pair is owed.

**What was built.** Section 3.1 as written, with one change from the review below: the calendar
admission notes the bound from the query it admits on, and asks again only for a node `rearm`
rewrote. The bound is noted on 24 of the coverage table's 51 rows over `d` - the calendar fields
whose lowering decomposes the day, the month arithmetic over them, and the int arithmetic and
predicates over those fields - and on none of `dayofweek`, `weekday`, the ISO day of week,
`next_day`, `trunc(d, 'WEEK')`, `unix_date`, `datediff`, the day shifts, the choice nodes or the
date comparisons, whose lowerings are exact for every int. `emitted_bytes.json` is unchanged
(`VarkaEmittedBytesSuite` passes without a regeneration): the input bounds are runtime data, not
part of the shape.

**Two consequences the plan did not list.** The coverage table's VARKA-279 rule - a row whose
kernel can decline a batch names its domain - made the 24 rows carry
`unix_date(d) BETWEEN -719162 AND 2932896` (conjoined with the domain four of them had), so
`coverage.json` and `docs/sql-varka.md` were regenerated, and the coverage differential's edges
fixture gained `d` at both ends of the bound and one past each; its domain tests now run every
bounded row over batches wholly inside, outside and null, and the outside ones decline with
`STATUS_INPUT_BOUND`. And the `datediff` path (section 2) is a confirmed second wrong answer, not
only a reading: with the bound's note disabled, `datediff(d, d2) * 100` answers a wrapped value
where Spark raises `ARITHMETIC_OVERFLOW`.

**The suite's teeth.** With `DeclineSink.dayColumnBound` made a no-op, all five
`VarkaFarDaySuite` tests fail: the reproduction's rows differ, the coverage sweep's `year(d)`
differs, the edge test sees no decline one day past the bound, `year(date_add(d, 1))` differs,
and the `datediff` product answers where Spark raises.

**Suites run.** `dev/varka_matrix.sh --defaults --split 4` with the sanitizer: 1,383 passed, one
failure, `VarkaTierWitnessSuite`, which asserts JIT tiers and traps in `VarkaMemorySanitizer.check`
under the sanitizer; the gate runs it in its quiet phase without the sanitizer, where it passes
here (7 of 7). The option matrix over the guard-relevant configurations, `guardDayProducers=false`,
`guardUnderArm=false`, `exactRefusalStatus=true` and `rangeSets=false`, with the defaults: all
five pass, 1,237 to 1,335 tests each, nothing failed or unfused.

**Predictions.** 6.1 #4: `VarkaFarDaySuite` took 25 s with its sessions and 16 s of tests,
against under 60 s and under 30 s - a ratio of about 0.5. 6.1 #1 to #3 wait for the pair.

**Review** (`/code-review high`, eight findings). Fixed: the admission's second traversal on the
plain path; the suite's hand-kept list of unbounded rows, now read from the table's domains so
every other date row is held to declining nothing; the proofs README's wrapping; the range
analysis now says the producer guard's reference configuration (`guardDayProducers=false`) is the
one gap it cannot see, which computes such a lane wrongly by design, as before. Not changed:
a slot-keyed bound past the input table is dropped as an ordinal-keyed one always was, which
happens only when a declined entry's rollback is skipped (the `misdescribeRollback` fault
injector), and then only for the declined entry's own columns; the per-shape widest bound (3.1's
follow-up); and the four-argument `range` defaulting to no report, which the tests and proofs use.

**Left for others.** The Spark-level fuzzer's value draws (`VarkaSparkFuzz`, row 298's file) stop
at 0001-01-15 and 9999-12-01 and a wide draw of 200,000 days either side of the epoch, so it
never draws a day past the bound; `farDates` there wants `date_from_unix_date` values past
42400-02-29 and before day -5394572, and int32's ends.
