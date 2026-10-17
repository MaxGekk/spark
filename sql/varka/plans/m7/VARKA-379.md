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
serve those days, and is `m9/SCOPE.md` item 102 (added with this task's outcome).

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
injector), and then only for the declined entry's own columns; the per-shape widest bound (`m9/SCOPE.md`
item 102); and the four-argument `range` defaulting to no report, which the tests and proofs use.

**Left for others.** The Spark-level fuzzer's value draws (`VarkaSparkFuzz`, row 298's file) stop
at 0001-01-15 and 9999-12-01 and a wide draw of 200,000 days either side of the epoch, so it
never draws a day past the bound; `farDates` there wants `date_from_unix_date` values past
42400-02-29 and before day -5394572, and int32's ends.

## 10. The check folded into the kernel (17 October 2026)

### 10.1 Why

The pair of section 6 (VarkaThroughputBenchmark, ten rounds on GitHub runners, EPYC 7763, AVX2,
run 38959217998) found the separate pass costly on Varka's side only: `datediff + 1` 17% slower
by median and 11% by best, `year * 100 + month` 16% and 10%, `yearofweek` 14% and 10%,
`weekofyear` 15% and 9%, every Janino row within 2%. Prediction 6.1 #2 (every bounded row within
3%) failed: the pass reads every bounded column once more from memory before the kernel reads it
again, which costs as much as a cheap kernel's own work. The owner held the pull request until
the check sits inside the kernel's loop, on the vector the kernel already loads.

### 10.2 The design

The kernel already has an in-loop range check with everything this needs: `GuardedRange`, the IR
node that passes its child through and declines the batch when a live lane is outside
`[lo, hi]` (VARKA-102's `TIME + INTERVAL`). Its emission is two compares on the loaded vector,
ANDed with the lane's validity word in the masked body and with the bounds mask in the epilogue,
ORed into the method's guard mask; the mask is reduced once per method after the loop and the
driver ORs the methods' statuses, so it is right in both bodies, in the tail, at both widths and
across split methods, and it is priced by the size machinery (`GUARD_WEIGHT`, the emit cost
table). So the column bound becomes that node, with no new emitter code:

- The compile of one entry (a projection entry, a predicate conjunct, and `fitsAlone`'s
  trial) ends by wrapping every `ColumnRef` of a slot the entry noted with `dayColumnBound` in
  `GuardedRange(ColumnRef, CONTRACT_MIN_DAYS, CONTRACT_MAX_DAYS)`, before the budgets are asked,
  so the size admission sees what the emitter will emit. Once per entry at its root rather than
  at the three sites that note the bound: the overflow check's site (`boundedByIntMax`) answers a
  question about operands its callers then build the node from, and a wrap that did not flow back
  into that node would lose the guard while the overflow check stays removed.
- The slot bounds the wrap discharges leave the sink, so they no longer reach
  `CompiledVarkaProjection.inputBounds` and the evaluator's pre-pass. The ordinal bounds stay
  there: `CAST(i AS INTERVAL DAY)`'s offset column sits in `date_add`'s day-offset position,
  which admits a bare column and not a guard (`VarkaOperandPosition`), so moving it in-kernel is
  not a one-line change and is not made here. The wrap never reaches into that position nor under
  `IsNotNull` (which reads only validity); a noted slot whose column sits in a restricted
  position keeps its bound for the pre-pass, though no date column can sit in one.
- `VarkaRangeAnalysis` answers a `GuardedRange`'s own bounds for the `DAY` kind too, as it
  already did for `INT`: a wrapped column reads as the range its guard enforces.
- A decline is `STATUS_VALUE_RANGE`, the node's own reason, rather than the evaluator's
  `STATUS_INPUT_BOUND`, which stays the pre-pass's.

What a declined batch's outputs are worth is the same contract every in-kernel guard already
rests on (`VarkaFusedKernel`'s status): the kernel writes its outputs and returns a non-zero
status; `VarkaKernelRunner.invoke` throws `VarkaBatchDeclined` before the both-bodies check; and
`VarkaEvaluatorBase.serveBatch` catches it and runs the caller's fallback for the whole batch,
for every exec node (project, filter, columnar-to-row) and for a projection of several kernels
(`VarkaKernelPart` rethrows naming the kernel). Nothing written before the decline is read.

What changes in which batches decline. Within one entry the wrap covers every use of a noted
column, so `year(date_add(d, i)) + year(d)` checks `d` under the `date_add` too, where the
producer guard alone would have served a far `d` the shift brings back into range; the pre-pass
did the same across the whole kernel, so nothing declines that did not before. A column use
under an `IF`/`CASE` arm is checked only on the lanes the arm takes (`emitArmContext`), which the
pre-pass could not do. The per-shape bound remains `m9/SCOPE.md` item 102.

### 10.3 Predictions, registered before the build

1. `emitted_bytes.json` moves for the shapes that hold a bounded date column and for no other;
   each body method of such a kernel grows by 40 to 90 bytes for its first guarded column (the
   guard, the mask's initialisation and the status reduction) and by 20 to 50 for each further
   one; no entry's method count changes and no coverage row stops fusing.
2. The re-pair of section 6's `VarkaThroughputBenchmark` against the base: no row 10% slower by
   both median and best, and the four rows above within 5% of the base by best - the guard is
   four vector ops a lane group on a vector already in a register, beside a decomposition of tens.
3. The far-date suites pass unchanged in what they assert, except the status a decline carries.

### 10.4 Outcome

Built as 10.2 describes (17 October 2026). Section 6.1's predictions #2 and #3 are scored by the
pair of 10.1, both failed: the bounded rows were 9 to 17% slower, not within 3%, and four rows
crossed the pair's 10% threshold by median (a calibration entry, ratio 3 to 6). #1, the evaluator
overhead benchmark's rows, was not part of the pair reported here.

**What was built.** `DeclineSink.guardDayColumns`, called at the root of each projection entry,
predicate conjunct and `fitsAlone` trial, before the budgets; `VarkaRangeAnalysis.guardColumns`,
the rewrite; and the range analysis's `GuardedRange` arm answering days. No emitter code changed.
The evaluator's pre-pass, `IntRangeOps` and `STATUS_INPUT_BOUND` stay for the
`CAST(i AS INTERVAL DAY)` bound alone.

**Emitted code (10.3 #1).** `emitted_bytes.json` moved for exactly the 24 coverage rows that carry
the bound, at both widths and in the option arms built from them; none of the fuzz blocks moved,
and no row's set of methods changed. Per method, at the laptop's width (`dev/varka_emit.sh`,
before with the wrap disabled):

| shape | dense loop | masked loop |
|---|---|---|
| `year(d)` | 409 to 461 | 409 to 555 |
| `weekofyear(d)` | 622 to 674 | 622 to 768 |
| `extract(YEAROFWEEK FROM d)` | 531 to 583 | 531 to 677 |
| `year(d) * 100 + month(d)` | 472 to 524 | 472 to 618 |
| `datediff(d, d2) + 1` | 167 to 252 | 167 to 440 |

The dense bodies are inside the prediction (52 bytes for one column, 85 for two); the masked
bodies are not: 146 bytes for one column and 273 for two, against at most 90 and 140. What the
prediction missed is that these masked loops read no input validity before - the driver computes
the outputs' validity a word at a time - and the guard has to, so that a null lane's undefined
day cannot condemn the batch: each lane group now loads the column's validity word and makes a
mask of it, per guarded column. The pre-pass did the same per lane group, with a byte loop.
`year(d) * 100 + month(d)` shows the guard shared: both fields read one guarded column, one guard.

**Two tests the guard's ops moved.** `VarkaInliningCliffSuite` pins the census shape of VARKA-209,
twenty-two `year(d) + k` tails with the call-site budget off, at 99 vector call sites a loop; the
guard's two compares make it 101, still past the C1 limit the census found, and the pin says so.
And `VarkaExpressionCompilerSuite`'s op-cap case (VARKA-169) builds a 63-op tree over `d`, which
with its guard is 64, the cap itself: beside `year(d)` it became the overflow entry instead of
`month(d)`, so the legacy half of that test now puts the tree first.

**Tests added.** `VarkaGuardEdgeSuite`: the column bound joins the guards' edge table (both ends
and one past, a full lane group and the epilogue lane, valid and null, through both bodies); and a
test that compiles `year(d)`, `datediff(d, d2) + 1` under ANSI and a kernel split one output a
method with the guarded output last, and runs each guarded column with a far day in a full lane
group and in the tail, in the dense body, the masked body and the masked body forced, and with far
days only under null rows, which its masked body answers and its dense body, the nulls cleared,
declines; the split case checks the guard's constant is in a loop method other than the first.
`VarkaFarDaySuite` reads the value-range reason where it read the input bound; what it asserts is
otherwise unchanged and it passes (10.3 #3 held).

**Suites run.** The gate step by step, since one call cannot hold the whole of it: compile, wide
and narrow (1,389 passed each, 34 cancelled as on every run, after the cliff pin's fix, which the
first wide run failed on), quiet, sweep, doc, bench and quotes passed. Lint: `dev/lint-java` and
`dev/scalastyle` passed, and Error Prone after two `ReferenceEquality` suppressions on the
rewrite's deliberate identity tests; NullAway then stops on `VarkaEmitTrace.onEmission`, which this
change does not touch and master has fixed since this branch's last merge of it (#889, #890). The
option matrix over the guard-relevant configurations: `exactRefusalStatus=true` and
`guardUnderArm=false` pass; `methodByteBudget=0` fails two tests whose IR is built by hand and
never reaches the compiler - `VarkaKernelPlanSuite`'s shared trace over 300 `date_add` nodes, which
that configuration's 64-op cap refuses, and `VarkaEmitterChronoSuite`'s scratch refusal over
`year` and `month` of a bare column - so neither rests on this change.

**Left open.** A far-day decline is counted under the value-range reason, beside the `TIME`
family's range guards, rather than a reason of its own; the kernel cannot tell the two apart
without a status bit for the column bound, which would be a second refusal reason on the same
node. Prediction 10.3 #2 waits for the re-pair.
