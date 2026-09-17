# Task 29: the long lane, reachable from SQL

*Milestone 5, section 2.3. Opened 17 September 2026, after task 85's step 4. The
milestone's centre: 102, 103, 104 and 105 are all expressions over this lane.*

## 1. Where this came from

Milestone row 29, and the spine section 1.1 sorts to: `117 -> 84 -> 85 -> 29,
with 28 and 92 beside it -> 88 -> 102, 103, 104 -> 105`. Task 85 gave the
emitter a second lane and proved it computes what the reference says at both its
widths; nothing can reach it. No compiler arm admits a column of any 64-bit type,
so every `bigint`, `TIMESTAMP_NTZ`, `TIMESTAMP`, `TIME` and `INTERVAL DAY TO
SECOND` query in existence runs on the row engine, and will keep doing so however
much of the milestone is built above it.

Two tasks handed this one its starting line explicitly. `PLAN_TASK_116.md` 2,
having proved a `TIME(p)` column survives Varka's Arrow cache and maps as
eight-byte lanes, names `isArrowBacked`'s vector-class allowlist as "the first
line task 29 changes", and deliberately did not touch it: admitting a batch no
kernel can read would be a slower way to decline than declining at the gate.
`PLAN_TASK_142.md` 9 committed what the lane costs per shape, so the number this
task's admission rule calls for - "the halved-headroom number committed rather
than discovered" - is already on disk before the work starts, at 1.5x to 2.0x in
cache and 2.11x to 2.20x out of it.

## 2. The admission check, done

Read in the tree on 17 September 2026, at master `a6c2c373089` with #236 applied.
The question is which layers already carry a 64-bit column and which refuse it,
because that decides whether this task is a compiler change or an Arrow change.

**The emitter is done, and that is measured rather than assumed.** Twelve node
types build and verify at `LaneType.LONG` - `ColumnRef`, `LiteralSlot`,
`IntArith`, `IntNeg`, `Greatest`, `Least`, `Compare`, `And`, `Or`, `Not`,
`IsNotNull`, `IfElse` - and `VarkaLaneTypeSuite` asserts that set exactly, so a
thirteenth arriving unlowered fails in milliseconds. The long matrix drives all
of them against `evalLong` at 2 and 8 lanes over four batch lengths and every
null-pattern combination, and the overflow matrix drives the checked and nulling
modes over `Long.MinValue`, `Long.MaxValue` and a sum that overflows only at this
width. **So this task should need no emitter change at all.** That is prediction
6.1.1, and it is the cheapest one to falsify.

**The IR already keeps a kernel single-lane, without anyone asking it to.** A
node's constructor refuses operands whose lanes disagree, `laneOf` refuses output
roots that disagree, and `VarkaLoopEmitter.fitsBudgets` - which the compiler
consults before letting an entry join the kernel - refuses a mixed output list.
The compiler's admission loop therefore already does the right thing: the first
fused entry fixes the lane and an entry of the other lane is demoted to residual.
What it does not do is say so. The decline it records is "exceeds the emitter's
fused budget", because that is the branch `fitsBudgets` returning false lands in,
and a user reading EXPLAIN would go looking for a chain-depth problem that is not
there. The fix is small but it is not one string: `fitsBudgets` answers a
boolean, so the compiler cannot tell a lane mismatch from a budget breach, and
four assertions in `VarkaExpressionCompilerSuite` pin the budget reason for
cases that really are budget breaches. The compiler checks the lane itself,
before `fitsBudgets`, and records its own reason. That is the only compiler
behaviour this task changes rather than adds.

**The Arrow path is width-agnostic already.** `extractMorsel` takes a
`BaseFixedWidthVector` and maps its data and validity buffers by address, so an
eight-byte vector maps through the same call as `DateDayVector`; task 116 proved
the composition end to end for `TimeNanoVector` and `DurationVector`, including
the all-null case that passes no validity segment at all. Nothing in the cache,
the serializer or the column vector needs a change for this task.

**Four places refuse a 64-bit column, and all four are lists.**

| where | what it is today | why it refuses |
|---|---|---|
| `VarkaExpressionCompiler` type gates | `dataType == DateType`, `== IntegerType`, `isInstanceOf[YearMonthIntervalType]` | no arm builds a `LONG` leaf |
| `VarkaKernelEvaluator.isArrowBacked` | allowlist of `DateDayVector`, `IntVector`, `IntervalYearVector`, `VarCharVector` | a `BigIntVector` input declines the batch |
| `VarkaKernelEvaluator.allocateVector` | `DateType`, `IntegerType`, `YearMonthIntervalType` | no destination vector for a long output |
| `VarkaKernelEvaluator.invokeFused` | calls the seven-argument `run` with `Array[Int]` literals | a long kernel throws `UnsupportedOperationException` by design |

Each is short and each is a list rather than a computation, which is what makes
this task an afternoon of plumbing rather than a redesign. `PLAN_TASK_141.md`'s
module map and #236's `fitsBudgets` refusal mean the wrong move fails loudly.

**What the check would have rejected.** If `extractMorsel` had baked a four-byte
stride, or the serializer had stored `TIME` as anything but a fixed-width vector,
this task would have started in Arrow rather than in the compiler and would have
been three times the size. Task 116 exists precisely so that this paragraph is a
fact rather than a hope.

**One thing the check found that the milestone row does not mention, and it is
not about this task.** When a Varka filter passes a batch on, every column of
the batch is compacted to the selected rows, and the vectorised `compress` path
(`VarkaKernelEvaluator.scala:1304`) is a width check that serves four-byte
vectors only; an eight-byte column takes `compactFixed`'s per-row
`copyFromSafe`. That is the case *today*, for every `bigint` or timestamp column
that merely sits in a filtered table, whatever the filter is on - task 29 does
not create it and does not widen it. It is a finding for its own row (section
3.2), not a clause of this one.

### 2.1 What review of this plan corrected, 17 September 2026

Reviewed against the tree before any code, and two claims in the first draft
were wrong in the direction that matters - each would have admitted an
expression that computes a wrong answer on some rows and the right one on the
rest. The draft said differences on zoned `TIMESTAMP` were safe because
instants subtract identically in any zone; Spark's `SubtractTimestamps`
subtracts local date-times in the session zone, which differs across a DST
transition. The draft admitted `ts + INTERVAL` without restricting the type;
`timestampAddDayTime` adds calendar days in the zone. Both are now `TIMESTAMP_NTZ`
only, and the zoned forms join the refusal list with a test that straddles the
transitions. Three claims were overstated rather than wrong: boolean projection
outputs, which no lane supports; the lane-mismatch decline as "one string", when
`fitsBudgets` returns a boolean; and the compaction finding, which is every
forwarded eight-byte column today rather than a long filter's output, and which
belongs to a new row rather than task 92. `bigint + literal` moved to task 104,
where the decision about its overflow mode lives.

## 3. The design

### 3.1 The mechanism

Five changes, in the order a batch meets them.

**(a) One mapping from a Spark type to a lane, in one place.** A private
`laneOf(dataType): Option[LaneType]` in `VarkaExpressionCompiler`: `IntegerType`,
`DateType` and `YearMonthIntervalType` to `INT`; `LongType`, `TimestampNTZType`,
`TimestampType`, `TimeType` and `DayTimeIntervalType` to `LONG`; everything else
`None`, which is the existing decline. Every leaf-building arm asks it instead of
testing a type, so a sixth long type later is one line here and nothing else.

**(b) The lane's own literal table.** `CompiledVarkaProjection.literals` is
`Seq[Int]` and the evaluator passes it as `scalarArgs`. A long shape's literals
are 64 bits, so the plan gains `longLiterals: Seq[Long]` beside it and the
evaluator fills `longArgs`. `LiteralSlot.index` addresses the table of its own
lane, which is well defined because a kernel is single-lane: exactly one of the
two tables is ever read by a given kernel. The compiler interns into the table
its leaf's lane names.

**(c) The gate.** `isArrowBacked` admits `BigIntVector`, `TimeStampMicroVector`,
`TimeStampMicroTZVector`, `TimeNanoVector` and `DurationVector` by class, exactly
as it admits `IntervalYearVector` - by class rather than by Spark type, since the
buffer layout is what the kernel reads.

**(d) The destination.** `allocateVector` gains the matching arms:
`BigIntVector` for `LongType`, `TimeStampMicroVector` for `TimestampNTZType`,
`TimeStampMicroTZVector` for `TimestampType`, `TimeNanoVector` for `TimeType`,
`DurationVector` for `DayTimeIntervalType` - the classes `ArrowWriter` already
pairs with those types, so the row path reads them back through the accessors
`ArrowColumnVector` already has.

**(e) The call.** `invokeFused` picks the overload from the compiled plan's lane:
the seven-argument `run` at `INT`, the eight-argument one at `LONG`. The plan
carries the lane rather than the evaluator deriving it per batch.

**The SQL this admits, and why it is this set.** With no new emitter node, the
twelve lane-generic types cover: comparisons on all five types (`=`, `<`, `<=`,
`>`, `>=`) as a filter's conjuncts and inside a `CASE WHEN` - not as a boolean
projection output, which the compiler declines at every lane today (comparisons
are compiled by `compileCond` only, and `allocateVector` has no `BooleanType`
arm); `IS NULL` and `IS NOT NULL`; `AND`, `OR`, `NOT` over those; `greatest` and
`least`; `CASE WHEN` over them; and, for `TIMESTAMP_NTZ` only, the two pieces of
arithmetic whose overflow mode the *type* decides rather than a config:
`ntz - ntz` (a day-time interval) and `ntz + INTERVAL DAY TO SECOND`. Spark
evaluates both in UTC for the NTZ family (`zoneIdForType`) through
`LocalDateTime.until` and `instantToMicros`, whose exact arithmetic raises on
overflow whatever the ANSI setting, so the checked mode is the only correct
lowering and there is no mode to choose. `bigint + literal` is deliberately not
here: its mode *is* a config, and deciding it under the 2.15 lattice is task
104's whole subject. That is the row's "comparisons, differences, literal
arithmetic", read narrowly enough to be right.

**Zoned operations decline, and are shown to.** `TimestampType` is micros in the
same lane as `TimestampNTZType`, so nothing physical stops a leaf being built for
it - which is the danger, and it is wider than the first draft of this plan
said. On a zoned `TIMESTAMP`, Spark's own semantics make *both* differences and
interval addition zone-dependent: `SubtractTimestamps` evaluates
`ChronoUnit.MICROS.between(localStart, localEnd)` on the two instants' local
date-times in the session zone (`DateTimeUtils.subtractTimestamps`), and
`TimestampAddInterval` evaluates `.atZone(zoneId).plusDays(days).plus(micros)`
(`DateTimeUtils.timestampAddDayTime`), calendar days in that zone. Across a DST
transition neither equals the instant arithmetic a long kernel would do, so a
kernel that computed them would be wrong by an hour on the rows that cross it and
right everywhere else - the worst kind of wrong. So for zoned `TIMESTAMP` this
task admits comparisons and nothing else: two instants compare identically in
every zone, and that is the only operation of which that is true. `-`,
`+ INTERVAL`, `date_trunc`, every field extraction and all day or month
arithmetic on `TimestampType` are refused at the compiler with their own reason.
The `TIMESTAMP_NTZ` family is evaluated in UTC (`zoneIdForType`), which is what
makes its two arithmetic forms plain long arithmetic and admissible above.

### 3.2 What is deliberately unchanged

* **The emitter.** If this task changes a byte of `VarkaLoopEmitter`, prediction
  6.1.1 was wrong and the reason belongs in section 9.
* **Mixed-lane trees.** `cast(int AS long) + long` stays declined; the width
  conversion and its loop-shape measurement are task 28. The decline gets an
  honest reason here, which is what makes task 28's arrival visible as a change
  in behaviour rather than a change in speed.
* **`TIME` and interval expressions** beyond comparisons and the differences
  named above - `hour(t)`, `time_trunc`, `make_time`, interval scaling - are
  tasks 102 and 103, and the divisions they need are task 88.
* **All `LongType` arithmetic** - `bi + 1` included - together with `try_*`, the
  error-identity differential, `div`, `%` and `pmod`, is task 104: the overflow
  mode of `bigint` arithmetic is a configuration decision under the 2.15
  lattice, and admitting even the literal form here would make that decision by
  accident.
* **Boolean projection outputs** (`SELECT bi > 5`) are declined at every lane
  today and stay so; they are a destination-vector question, not a lane one.
* **The width-8 compaction arm** (section 2's finding) is a new milestone row,
  opened by this plan's review rather than folded in: it affects every
  eight-byte column forwarded through any Varka filter today, so it is neither
  this task's to fix nor task 92's, whose subject is the validity write at four
  lanes.
* **The calendar family**, which stays `INT` and keeps `Lane.requireInt`.

### 3.3 Registered op counts

Not applicable, and deliberately so: no emitted method changes. The standing
oracle (`VarkaEmittedBytesSuite`, `sql/varka/emitted_bytes.json`) must stay green
without regeneration, which is this task's equivalent of an op count.

## 4. Files

| file | what |
|---|---|
| `VarkaExpressionCompiler.scala` | `laneOf`, the leaf arms, the long literal table, the lane-mismatch decline reason, the zoned refusals |
| `VarkaKernelEvaluator.scala` | `isArrowBacked`, `allocateVector`, `invokeFused`, the `longArgs` array on `FusedRunner` |
| `VarkaLongLaneSuite.scala` (new, `sql/core`) | the end-to-end suite of section 5 |
| `VarkaExpressionCompilerSuite.scala` | the compiler-level admissions and declines |
| `sql/varka/coverage.json` | the new columns and rows, regenerated |
| `sql/varka/plans/PLAN_MILESTONE_5.md` | row 29 |

## 5. Tests, and what each is for

1. **The differential, per type.** For each of the five types, a fixture table
   with the three null patterns the date suites use, cached through the Arrow
   serializer, running each admitted expression with Varka on and off and
   comparing rows. Catches a lane that computes the right bits in the wrong unit
   - a `TIME` read as micros, an interval read as nanos - which no kernel test
   can catch, because the kernel never sees the unit.
2. **The fused-not-declined assertion.** Each of those queries asserts it
   actually fused (the decline map is empty and the plan shows the Varka node),
   because a differential passes trivially when everything falls back.
3. **A mixed-lane projection.** `SELECT d + 1, bi + 1` fuses one lane, leaves the
   other residual, and the decline reason names the lane rather than the budget.
4. **Zoned operations on `TIMESTAMP`** - `ts - ts`, `ts + INTERVAL '1' DAY`,
   `date_trunc`, `hour(ts)` - each declines with its own reason and answers
   what the row engine answers, under a session zone that observes DST
   (`America/Los_Angeles`) and over instants that straddle both transitions of
   one year. The straddling rows are the point: on any other rows a wrongly
   admitted kernel agrees with Spark, so a fixture without them would pass a
   kernel that is wrong. The "demonstrably declined, not wrong" half of the row's
   admission rule.
5. **A long filter**, where the output goes through the selection path, over a
   batch with nulls: proves the compaction fallback of section 2 is correct even
   though it is not fast.
6. **Both vector widths**, by running the new suite under the narrow width the
   gate already uses, since a long lane at 128 bits is two lanes and the epilogue
   does more of the work.
7. **The standing oracles unchanged**: `VarkaEmittedBytesSuite` green without
   regeneration, and `VarkaCoverageSuite` regenerated with the new rows reviewed
   in the diff.

## 6. The measurement

**The per-shape lane cost is already committed** - `PLAN_TASK_142.md` 9 and
`VarkaLongLaneBenchmark-jdk25-results.txt` - so this task does not re-measure it
at the kernel. What it adds is the first end-to-end number on the lane, and that
belongs to task 105's `TimeSurfaceBenchmark`, which is the file the public
message quotes. This task therefore runs no new benchmark and commits no new
results file; it re-runs the parity gates at both widths, which is a correctness
gate rather than a measurement.

### 6.1 Predictions, registered before the work

1. **The emitter diff is zero lines.** The twelve node types cover every
   expression section 3.1 admits.
2. **A `bigint` comparison filter over an Arrow-cached table fuses and declines
   no batch**, and its end-to-end throughput is between 0.45x and 0.60x the same
   filter over an `int` column of the same row count. Task 142's per-shape
   kernel ratio is 2.11x to 2.20x out of cache, and end to end the cached
   column's bytes double as well, while the per-batch fixed costs - the plan
   walk, the batch handling - do not; the band is wide enough to hold both
   effects and narrow enough to catch a lane that costs three times, which is
   what the single-rung measurement first said.
3. **The first failure will be in the literal table**, not in the lane: two
   tables under one index space is the one place in this design where a wrong
   answer is silent rather than loud. Test 1 is what catches it.

## 7. Risks

1. **A literal interned into the wrong table** reads a neighbouring slot's value
   and answers plausibly wrong. Test 1 over a fixture whose literals differ per
   row-group catches it; a `require` that the plan's two tables are never both
   non-empty makes it structural, since a kernel is single-lane.
2. **`TIMESTAMP` computed as if it were local time.** Section 3.1's refusal list
   is the mitigation and test 4 is the proof. The failure mode is quiet and
   user-visible, which is why it gets its own test rather than a line in another.
3. **Admitting at the gate what the compiler will not fuse.** Widening
   `isArrowBacked` before the compiler builds long leaves would decline batches
   one layer later and slower (task 116's warning). Both move in the same commit.
4. **Overflow in NTZ arithmetic.** `ntz - ntz` and `ntz + interval` can overflow
   a long for extreme instants, and the row engine raises there whatever the ANSI
   setting, because `LocalDateTime.until` and `instantToMicros` use exact
   arithmetic. The lowering must be the checked mode - which the emitter has at
   this lane - so the batch declines and the row engine raises, rather than
   wrapping into a plausible interval. Test 1's fixtures carry the extreme
   instants so that the decline is exercised, not assumed.
5. **The epilogue's method size.** A long lane halves the lanes per group, so a
   batch takes twice the groups and the epilogue grows; the 64KB method limit is
   a known open case (milestone 5's register, at the owner's request). A wide
   long projection is the first shape likely to reach it, and the failure is loud.
6. **`TIME(p)` and precision.** Every precision is nanoseconds in the lane and in
   the literal - `TimeType`'s physical value is already nanos, so there is no
   widening to get wrong - but Catalyst inserts casts between precisions, and a
   comparison of a `TIME(3)` column with a `TIME(6)` literal reaches the compiler
   through one; the compiler must see through it or decline it, never drop it.
   Test 1 across `p` in {0, 3, 6, 9} with literals of a different `p` is what
   holds it.
7. **The coverage fixture's ordinals.** Adding the five long columns to the
   shared `childOutput` renumbers every ordinal after them, which task 122's
   review found breaks tests that assert an ordinal; the new columns go at the
   end, and any test that needs its own layout builds its own list.
8. **This plan assumes #236.** The single-lane demotion in section 2 rests on
   `fitsBudgets` refusing a lane mix, which task 85's step 4 added; nothing in
   section 8 starts before it merges.

## 8. Sequencing

After #236 (task 85, step 4) merges, and not before - risk 8. Each commit green
on its own, and each one alone is a working decline:

1. `laneOf` and the long leaves in the compiler, with every long expression still
   declining for want of a gate - the compiler suite's admissions only.
2. The long literal table through the plan and the evaluator, with the
   eight-argument call; still no gate, so nothing runs it yet.
3. The gate and the destination vectors, both in one commit (risk 3): the first
   commit where a `bigint` query fuses end to end.
4. The zoned refusals and the lane-mismatch decline reason, with their tests.
5. The coverage table regenerated, the milestone row, section 9.

## 9. Outcome

*To be written when the work lands, section by section as the plan's own rule
asks. Nothing above is to be rewritten to look prescient; a correction is added
and says what it corrects.*
