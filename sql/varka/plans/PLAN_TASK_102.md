# Task 102: `TIME` expressions over the long lane

*Written 18 September 2026. Section 2.37 of `PLAN_MILESTONE_5.md` opened this
task on 15 September 2026 by the milestone's re-scope, which calls it "the
milestone's subject", and adjusted it on 17 September: comparisons and
`CASE WHEN` over a `TIME` column landed with task 29.*

## 1. Where this sits

This is the next task on the milestone's own spine - `88 -> 102 / 103 -> 105 ->
121 -> 118` - and everything after it waits: the `TIME` benchmark, the AVX2 arm,
and the closing task that writes the public post. The post is about `TIME`; this
task is what it is about.

`TimeType(p)` stores nanoseconds since midnight in a long whatever `p` is, so
every value lies in `[0, 86 399 999 999 999]`, below 2^47 - which is what makes
every division a `TIME` expression needs exact through task 88's double-lane
route with no bound to prove and nothing to decline.

*The lane is **not** task 29's unchanged, which is what an earlier draft of this
section said. Section 2.4 corrects it, and the correction reorders the task.*

## 2. What the section did not know

### 2.1 No TIME expression reaches the compiler under its own name

`HoursOfTime`, `MinutesOfTime`, `SecondsOfTime`, `SecondsOfTimeWithFraction` and
`MakeTime` are **`RuntimeReplaceable`** - and so, the correction at the end of this
section records, are the other four. Each rewrites itself into a
`StaticInvoke` on `DateTimeUtils` - `getHoursOfTime`, `getMinutesOfTime` and so
on - and the optimizer's `ReplaceExpressions` runs long before physical planning.
So a compiler arm matching `case HoursOfTime(child)` would never fire in a real
query. The compiler's existing `RuntimeReplaceable` arm unwraps to `.replacement`
and is documented as defensive, for hand-built test expressions; in production it
is the `StaticInvoke` that arrives.

**The compiler has never matched a `StaticInvoke`** - the count is zero today.
Every arm it has keys on a Catalyst expression class. So this task's first design
decision is how to recognise these five.

**Not by a hardcoded method name.** `StaticInvoke` carries `staticObject: Class[_]`
and `functionName: String`, so `(classOf[DateTimeUtils.type], "getHoursOfTime")`
identifies the target exactly - and writing that pair as a literal binds Varka to
a helper name upstream may rename without ceremony, after which fusion stops
silently and the only symptom is a benchmark that got slower. That is the failure
mode this project keeps finding in its own tooling, most recently in
`dev/is-changed.py`, which answers `false` for a module that does not exist
exactly as it does for one that did not change.

**Derive the table from the Catalyst classes instead.** The replacement is
produced by the expression itself, so the compiler can ask it rather than
guessing:

    HoursOfTime(dummy).replacement  ->  StaticInvoke(DateTimeUtils, "getHoursOfTime", ...)

Building the lookup as `{ (si.staticObject, si.functionName) -> field }` by
constructing each `RuntimeReplaceable` once and reading its own `.replacement`
means the key is generated from the same source that produces the query's
expression. **An upstream rename is then followed automatically**, because both
sides move together, and no string is written down twice.

Two ways it can still break, and both become loud rather than silent: if a
replacement stops being a `StaticInvoke` at all, the table construction has
nowhere to put it and fails at class-initialisation; and if two entries collide on
one key, the map catches it. A test still asserts end to end that the expression
Spark produces for `hour(t)` - built by running the analyzer and optimizer over
SQL, not by constructing the node by hand - is one the compiler matches.

*Corrected 18 September 2026, starting the implementation: there are no such
four. **All nine are `RuntimeReplaceable`** - `TimeTrunc`, `SubtractTimes`,
`TimeAddInterval` and `TimeDiff` too. The first draft's check read only the first
`extends` on each declaration, found `BinaryExpression` and `TernaryExpression`,
and never saw the second trait. So the `StaticInvoke` table above is not group
B's cost: it is the **prerequisite for every expression in this task**, which
makes the task simpler than planned - one mechanism, applied uniformly - and
moves the whole of it behind that one piece of work. Section 6 is re-grouped
accordingly.*

### 2.2 The reference is `LocalTime`, not Spark's own code

`getHoursOfTime` is `nanosToLocalTime(nanos).getHour` - it goes through
`java.time.LocalTime`, not through a division. Section 2.37 describes the
extracts as "a division and a `floorMod`", which is the right *lowering* and is
not what Spark computes.

Two consequences. Varka is **re-deriving** the arithmetic rather than mirroring
an implementation, so the differential oracle must be `LocalTime` itself, the way
the calendar family's oracle is `LocalDate` - a transcription of Spark's helper
would prove only that two transcriptions agree. And the exhaustive sweep is
cheap here in a way the calendar's was not: a `TIME` value has 86.4e12 possible
nanosecond values, far too many, but the *fields* change only at second
boundaries, so sweeping all 86 400 seconds of a day plus the sub-second edges
covers every distinct answer.

### 2.3 Two expressions are blocked on a representation, not on a lane

`SecondsOfTimeWithFraction` returns a `Decimal` and `TimeToSeconds` returns
`DecimalType(14, 6)`, and it is worth being exact about what stops them, because
"Varka has no decimal lane" is the wrong reason and points nowhere.

Spark's own `Decimal` holds a precision of 18 or less as an unscaled **long**, so
the *value* would sit in the lane this task already uses. What does not fit is the
**column**: `ArrowUtils` maps every `DecimalType` to
`new ArrowType.Decimal(precision, scale, 8 * 16)` - a 128-bit Arrow vector,
sixteen bytes per row, whatever the precision. A Varka output column is read and
written through that Arrow buffer, so the blocker is the representation and not
the arithmetic or the lane width.

That makes these two a **roadmap item rather than a dead end**, and it lines up
with the project's stated aim of several representations per logical type: a
Decimal128 representation, or a narrow-decimal one that keeps a long buffer and a
scale, would admit them unchanged. Until then they decline, and the decline names
the Arrow representation - the same care task 89 took over
`extract(MONTH FROM ym)`, where a reader who saw "declined" would otherwise
conclude the division was still missing.

### 2.4 The three headline extracts are mixed-width kernels

Read from `timeExpressions.scala` rather than assumed:

| expression | input | output | lane |
|---|---|---|---|
| `hour(t)`, `minute(t)`, `second(t)` | TIME, int64 | **`IntegerType`** | **mixed** |
| `make_time(h, m, s)` | int32, int32, **`DecimalType(16, 6)`** | TIME, int64 | **mixed**, and a decimal operand |
| `time_trunc(unit, t)` | TIME | `TimeType` | same |
| `t1 - t2` | TIME | `DayTimeIntervalType(HOUR, SECOND)` | same |
| `timediff(...)` | TIME | `LongType` | same |
| `t + dt` | TIME | `TimeType` | same |

So the three expressions this milestone calls its subject produce an int32 from
an int64 lane, and **nothing in Varka emits a mixed-width kernel today**. They
depend on task 28, which is planned and not built. An earlier draft of section 1
asserted the opposite.

### 2.5 A narrower way in than task 28, worth settling first

The extracts may not need task 28's full bi-lane kernel. Their computation stays
entirely in long lanes; only the **store** narrows. Driving the loop at the long
species and emitting a single `L2I` at the store is a much smaller change than
the pair representation - the loop keeps one trip count, no value is held as two
halves, and no slot pressure doubles.

It is not free, and the store path says why. Today one `s.byteOffset` is shared
by every output and the store writes at the lane's element width, so a narrow
output needs its own offset; and `L2I` part 0 leaves the quotient in the low half
of a full-width `IntVector`, so a dense store would write twice the bytes wanted
and the store must be masked to the long species' lane count.

Both are contained, and neither touches the general mixed-lane machinery. **This
is the first thing to settle**, because it decides whether the headline
expressions wait for task 28 or ship before it.

*Added 19 September 2026.* There is a third answer, and it may be the cheapest:
change the representation rather than the store. `SCOPE_MILESTONE_6.md` item 11
now names a `TIME` held as `(seconds of day: int32, nanoseconds within the
second: int32)`, under which `hour`, `minute` and `second` are int-lane magic
divides by 3600 and 60 - the family task 88's A/B measured at 3.7x the double
route - and no kernel is mixed-width at all, because the fields are already in
32-bit lanes. The conversion is one long-lane `ConstDivide` by 10^9 and a
multiply-subtract, an isomorphism on nanoseconds of day, and it is the first
measurement item 11 asks for that needs no engine. 2.5's two options and this
one should be priced together.

### 2.6 What each expression needs, read from the helpers

*Also 18 September 2026. The first draft grouped by lane and by matching; neither
was the operative constraint for four of the nine.*

`DateTimeUtils` says what the lowering has to do, and division is the divider:

    subtractTimes(end, start) = (end - start) / NANOS_PER_MICROS
    timeDiff(unit, start, end) = (end - start) / getNanosPerTimeUnit(unit)
    timeTrunc(level, nanos)    = truncatedTo(level), i.e. (n / u) * u
    timeAddInterval(t, iv)     = addExact(t, multiplyExact(iv, 1000)), then a range check

So three of the four the first draft called "ordinary" need a **long-lane
division** - task 88's step 3, which section 3 already brings into this task -
and the one that needs no division at all is `t + dt`, which the first draft
deferred to group C.

The real dependency map, which is what section 6 now groups by:

| needs | expressions |
|---|---|
| the `StaticInvoke` table | **all nine** |
| long-lane division (88 step 3) | `time_trunc`, `t1 - t2`, `timediff`, `hour`, `minute`, `second` |
| narrowing (2.5's question, or task 28) | `hour`, `minute`, `second` |
| widening (task 28) | `make_time` |
| an Arrow decimal representation | `second_with_fraction`, `time_to_seconds` |
| **nothing beyond the table** | **`t + dt`** |

`make_time` needs no division either - it is two multiplies and two adds - so
what holds it is the widening alone.

## 3. Where task 88 step 3 lands: here

Task 88's step 3 - the long-lane converts, the `useAVX` option field and the AVX2
magic-number form - **has no caller of its own**. The IR has no division node
that reaches the long lane, and step 3's stated test, "the `TIME` divisors'
parity at both AVX levels", needs this task's nodes. Building it separately would
produce unreachable code tested by nothing.

So it lands here, and this plan owns it. What it needs is already established and
needs no new investigation:

* `dev/varka_canary/L2DProbe.java` pins the long round trip: `L2D`/`D2L` at
  `part 0` both ways, `D2L` truncating, and the converts failing to inline under
  `-XX:UseAVX=2`.
* `dev/varka_canary/MagicProbe.java` pins the AVX2 fallback for `/3.6e12`: no
  conversion instruction at all, 0 wrong quotients over 65 536 nanos-of-day at
  4 and 8 lanes.
* `verify_double_division.py` certifies every `TIME` divisor exact under both
  double forms.

What is genuinely open is `useAVX` in the shape key. Task 88's plan noted it must
enter the key so one committed `emitted_bytes.json` can pin both hosts; the
wrinkle is that a per-host default makes `VarkaEmitOptions.DEFAULTS` host-
dependent, and `canonical()` renders empty for the defaults, so two hosts would
render the same key for different bytes - the exact bug the key exists to
prevent. The resolution is a fixed default in `DEFAULTS` with the session setting
it explicitly, so a host that differs renders a non-empty `canonical()` and gets
its own key. That is a decision to make before the AVX2 form is written, not
after.

## 4. The expressions, and what each is in the lane

*Every `TIME` expression Spark has. Section 6 gives the order: group A ships
first, B carries the width change and the `StaticInvoke` table, C waits - `t + dt`
on an upstream question and the two decimal-returning ones on an Arrow
representation.*

| expression | lowering | note | group |
|---|---|---|---|
| `hour(t)` | `/ 3.6e12` | `StaticInvoke`, 2.1 | B |
| `minute(t)` | `/ 6e10` then `floorMod 60` | `StaticInvoke` | B |
| `second(t)` | `/ 1e9` then `floorMod 60` | `StaticInvoke` | B |
| `make_time(h, m, s)` | two multiplies and adds under a range check | `StaticInvoke`; a foldable seconds argument is a constant, a decimal column declines | B |
| `time_trunc(unit, t)` | a division and a multiply, at a foldable level | `trunc(d, fmt)`'s rule for the level | A |
| `t1 - t2`, `timediff(...)` | a day-time interval, `/ 1000` | exact by range | A |
| `t + dt` | `t + micros * 1000` under a range guard | 4.1 | C |
| `time_to_seconds` etc. | multiplies and divisions by powers of ten | cheap after A; `TimeToSeconds` waits on 2.3 | C |
| `second_with_fraction` | a division and a remainder into a decimal | waits on an Arrow decimal representation, 2.3 | C |

### 4.1 `TimeAddInterval` does not wrap, and that decides its lowering

Vanilla's `timeAddInterval` is `addExact` plus a check that the result lies in
`[0, 24h)`, throwing `timeAddIntervalOverflowError` otherwise - it does **not**
take a modulo. A lane cannot throw, so this takes `make_date`'s pattern: the
guard fails the batch into the ghost fallback, which raises the identical error
on the identical row, or returns null under `TRY`.

[SPARK-57853](https://issues.apache.org/jira/browse/SPARK-57853) is open on
whether that becomes ANSI's modulo-24. If it lands, the lowering becomes a
`floorMod` by `NANOS_PER_DAY` and the guard goes away. The plan should not
pre-empt it; it should make the guard easy to delete.

The ticket is unassigned and has no patch, so milestone 5's task 146 takes
reviewing or writing one upstream. This section is where its answer is recorded:
the guard is deleted here, or kept here with the reason.

## 5. Tests

1. **The differential against `LocalTime`**, over all 86 400 seconds of a day
   plus the sub-second edges - which is every distinct answer the field extracts
   have, 2.2.
2. **The expression Spark actually produces is the one the compiler matches**,
   built by running the analyzer and optimizer over SQL rather than by hand. This
   is the guard against 2.1's silent-rename failure and is the most important
   test in the task.
3. **Both AVX levels**, since step 3's converts do not intrinsify under AVX2 and
   the magic form replaces them there.
4. **The declines name their output type**, for the two decimal-returning
   expressions.
5. **`TimeAddInterval` declines the batch** rather than wrapping or throwing.
6. **The oracle and fuzzer at `TIME`** - task 119's `TIME` arms land here, which
   is what its row says.

## 6. What is in this task, and what is not

Everything Spark has, in the end - but not in one step, and two of them wait on a
representation rather than on this task. The groups are an order, not a
shortlist.

*Re-grouped 18 September 2026 by 2.6's map, which is the dependency that
matters. The first draft grouped by lane and by matching; three of the four it
called free of blockers in fact need the long-lane division, and the one that
needs nothing was in its deferred list.*

**The prerequisite, before any expression: the `StaticInvoke` table** of 2.1.
All nine go through it, so it is not a group's cost but the task's entry fee. It
is also self-contained and testable on its own - the table is built from the
Catalyst classes' own `.replacement`, and the test is that what Spark produces
for a SQL query is what the table holds.

**Group A: `t + dt`.** The only expression needing nothing beyond the table - a
multiply by 1000, an add, and the range guard of 4.1. Its semantics may change
under [SPARK-57853](https://issues.apache.org/jira/browse/SPARK-57853), which is
an argument for building the guard so it is easy to delete, not for waiting: a
milestone whose subject is `TIME` should not have its first `TIME` kernel blocked
on an upstream ticket that may sit for a release.

**Group B: the three same-lane divisions** - `time_trunc`, `t1 - t2`,
`timediff`. They need task 88's step 3 and nothing else, so they follow it
directly and are what proves it on real expressions rather than on a probe.

**Group C: the width changes** - `hour`, `minute`, `second` narrowing, and
`make_time` widening. The extracts are the post's subject and depend on 2.5's
answer; `make_time` needs no division at all, only the widening, so it can land
whenever task 28 does.

**Group D: blocked on a representation** - `second_with_fraction` and
`time_to_seconds`, 2.3, admitted unchanged the day an Arrow decimal
representation exists. `TimeFrom*` and the remaining `TimeTo*` are ordinary
long-lane multiplies and divisions and follow group B cheaply.

**Nothing here is declined for want of a mechanism.** Every `TIME` expression
Spark has is vectorizable except group D's two, and those wait on a
representation rather than on anything about the lane.

## 6.1 Sequencing

1. The plan, and this correction.
2. **The `StaticInvoke` table**, with its guard test. Everything waits on it and
   nothing else does, so it goes first and alone.
3. **Group A**, `t + dt` - the first `TIME` kernel, needing nothing further.
4. **Task 88 step 3**'s long-lane converts, with `useAVX` in the shape key
   resolved per section 3.
5. **Group B**, the three same-lane divisions, which prove step 3 on real
   expressions.
6. **2.5's question**, then group C's extracts; `make_time` with task 28.
7. The declines, the coverage rows, and task 119's `TIME` arms.

## 7. Outcome

### 7.1 Groups B and A, 19 September 2026

Built in that order, which reverses 6.1's, and the reason is worth keeping: group
B needed no new machinery once #255 had landed the long-lane divide - three
compiler arms over nodes that already existed - while group A needed a new IR
node. The cheaper commit went first and proved the compiler path with the least
behind it; A followed on a path already known to work.

**Group B is three arms and no emitter change.** `subtractTimes`, `timeDiff` and
`timeTrunc` are matched by the `DateTimeUtils` method their `StaticInvoke` names,
through #254's table, and lower to a wrapping subtraction and a `ConstDivide` (or
a divide and a multiply for `time_trunc`). Every dividend is bounded by the type
- nanoseconds of day are below 2^47, and so is the difference of two - which is
the one place in the lane's arithmetic where `ConstDivide`'s bound is a property
of the type rather than of the data. No per-batch check is registered. A unit or
level that is not a literal declines, since the divisor is part of the kernel's
shape.

**Group A is a wrapping add under two range guards.** Spark's `timeAddInterval`
is `addExact(t, multiplyExact(dt, 1000))`, a throw if the sum leaves the day,
then a truncation to the target precision. The interval is held to a day first,
for two reasons that are one: beyond a day every sum leaves the day and Spark
throws on every row, and inside a day the multiply and the add stay under 2^48,
so wrapping arithmetic is exact. The sum is then held to the day, which is the
throw as a decline. A literal interval's guard is decided at compile time - a
literal beyond a day declines outright, one inside it folds to a slot.

**The precision step is not emitted, and the argument is code.** The time is a
multiple of 10^(9 - p), the interval's nanoseconds a multiple of 10^3 - or of a
whole minute for an end field coarser than SECOND - and the target
`TimeAddInterval.replacement` computes makes the sum already a multiple of what
the truncation would remove, in both cases. `timeAddIntervalTruncates` checks
that against the types at compile time and declines if it ever fails; a test
holds it across precisions in both directions, one case included that does
truncate so the check is not vacuous.

**The guard is `GuardedRange(child, lo, hi)`**, `GuardedDay`'s twin at whichever
lane the child is on, with the bounds inside the node so that two shapes with
different bounds cannot share a kernel. It reaches every site the day guard does.
`emitRangeGuard`'s bounds widened to `long`, which moved no int-lane byte.

**The bytes oracle's fuzz half moved, and the diff says why.** Adding a node type
adds an arm to the grammar's draw, which reshuffles its fixed-seed sample of ten
thousand shapes. Exactly two keys changed in `emitted_bytes.json`, the fuzz-block
digests at lanes 4 and 16, and no coverage row - every named shape's bytes are
identical. That is the check to run whenever a node is added: the coverage half
is the oracle for emission, the fuzz half for the grammar.

**What the coverage fixture could not hold, and what holds it instead.** The
differential's fixture reaches both ends of the day, so no non-zero constant
interval keeps every row of `t + dt` inside it - Spark raises `DATETIME_OVERFLOW`
on the row that crosses, in both engines. The coverage row therefore adds a zero
interval, on purpose and with the reason in its note, and
`VarkaTimeArithmeticSuite` carries the real tests: every second of the day with
a per-row interval that stays inside, the crossing case raising Spark's own error
under Varka rather than a wrapped time, and a `CASE` whose crossing rows sit in
the untaken arm - where the row engine does not throw, the kernel's guard fires,
and the decline shows in `numFallbackBatchesDeclined`. That last one is what
separates "declined" from "wrong" for a guard no value can show.

**Two things the decline test taught about the test, not the kernel.** The
end-to-end test that shows a declined batch in the node's metric is a conjunction
whose left side the row engine short-circuits and whose right side adds a
crossing interval. Its first form selected with `t = TIME'12:00:00'`, and the
optimizer's constant propagation rewrote the sum's `t` to the literal - a literal
noon plus any interval in the fixture stays inside the day, so the guard had
nothing to fire on and the batch was served correctly. A range on `t` is not
propagated, and with it the guard fires, the batch declines, and the row engine
answers the same row. And under `guardUnderArm` a crossing row in an untaken
`CASE` arm neither throws nor declines, which a test now pins: the `TIME` guard
rides the arm qualification the day guard does.

**On [SPARK-57853](https://issues.apache.org/jira/browse/SPARK-57853).** If
upstream adopts ANSI's modulo-24, the change here is the deletion of one
`GuardedRange` wrapper and the substitution of a floor-mod; the interval's guard
stays, since the multiply's exactness rests on it. Task 146 follows the ticket.

**Still owed:** group C's extracts, which wait on 2.5's question or task 28; group
D's decimal pair; task 119's fuzzer at the long lane, since the grammar still
generates no `LONG` node and the new guard is fuzzed only at the int lane.

## 6.2 Sequencing, as it happened

Steps 2, 4, 5 and 3 of 6.1 in that order: the table (#254), task 88 step 3
(#255), group B, group A. Then group C's extracts by route A (section 8.6);
group D and the split form remain.

## 8. Group C: the extracts, the narrowing store and the split form, planned

*20 September 2026, after tasks 152 and 153 priced the split form and found
where masks stop lowering.*

### 8.1 What group C is, read again

`hour(t)`, `minute(t)` and `second(t)` produce an `IntegerType` from a `TIME`
column (2.4): `HoursOfTime`, `MinutesOfTime`, `SecondsOfTime` are
`RuntimeReplaceable` to `StaticInvoke`s of `DateTimeUtils.getHoursOfTime` and
its two siblings, which go through `LocalTime`, and the arithmetic is the
divisions of 2.6. `make_time(h, m, s)` is the other direction - two int
operands and a `DecimalType(16, 6)` seconds operand into a `TIME` - and its
blockers are task 28's widening and 2.3's decimal representation; it stays
with those. So this section is about the three extracts, and the question 2.5
left open: how an int32 result leaves a long-lane kernel.

### 8.2 The three routes, priced from the numbers already committed

`VarkaTimeBenchmark` (task 152, `PLAN_TASK_152.md` 6) put the four arms of
this question on one ladder, and the numbers decide more than 2.5 expected.
Rates in M rows/s at the L2 rung, 512-bit species then 128-bit.

| route | what computes the field | `hour` | all three | where the cost is |
|---|---|---:|---:|---|
| **A. long lanes, narrowing store** | the conversion-form division in 64-bit lanes, the int32 result narrowed at the store | 4204.3 / 2377.2 | 1391.8 / 760.3 | the divider: one `vdivpd` per eight rows per division |
| **B. split per batch, then int lanes** | a derived int32 seconds column, then a single bounded multiply per field | up to 24417.3 / 30855.0 after the split | up to 13298.7 / 8384.9 after the split | the split: 3989.3 / 2258.9 as a long kernel, far less as a scalar loop |
| **C. split stored, then int lanes** | the same int kernel over a seconds column the Arrow cache already holds | 24417.3 / 30855.0 | 13298.7 / 8384.9 | nothing per query |

The `hour` figures under A are the file's `nanoseconds of day, int64 lanes,
conversion form` rows; B and C's upper bounds are its hand-written single
multiply, which the emitter does not have yet and section 8.4 gives it; B's
split cost is the file's `the split itself` row.

Three things follow, and the third is the one that reorders the work.

1. **A is a fixed cost per field and is already fast.** One field at the
   conversion form's rate is 0.24 ns a row at 512 bits, twenty times a row
   engine that goes through `LocalTime`; three fields are 0.72. It needs one
   contained emitter change - the store - and nothing new in the compiler
   beyond the arms.
2. **B pays only when the split is a vector kernel and several fields are
   taken.** The split as a scalar Java loop in the evaluator - the derived-leaf
   pattern of `next_day` and `trunc` - divides a long per row and would cost
   more than A's whole kernel. As a long kernel it costs about one A field, so
   for one field B is A plus a store, and for three fields it is 0.24 plus three
   cheap ones against A's 0.72: better, by under 2x. And a vector split needs
   exactly A's narrowing store to write its int32 seconds.
3. **C is where the split's 5x to 14x lives, and C is a representation held
   by the cache**, not by this task: two int32 columns beside or instead of the
   `TimeNanoVector`, written by the serializer task 116 already taught `TIME`,
   admitted by `isArrowBacked` as a pair, and chosen by the compiler when the
   batch offers them. That is `SCOPE_MILESTONE_6.md` item 11's first concrete
   encoding decision - the form attribute on a value, and a leaf per form - and
   it belongs to that item, with this section as its pricing.

So the order is A, then B's pieces as the thing C reuses, then C under item 11.
2.5's second option, task 28's bi-lane kernel, is not needed by any of the
three: the extracts never hold an int and a long live in one loop, they narrow
once at the end.

### 8.3 Route A: the narrowing store

**The IR.** Task 28's `NarrowLane(child)` node, as `PLAN_TASK_28.md` 3.3
defines it - a `LONG` child, an `INT` value - admitted in this step **as an
output root only**: the emitter's analysis refuses it anywhere else, so no
interior node ever sees a lane change and task 28 later lifts that refusal
rather than adding a node. The compiler builds `NarrowLane` over the same
trees group B built:

    hour(t)   = NarrowLane(ConstDivide(t, 3600000000000))
    minute(t) = NarrowLane(SUB(x, MUL(ConstDivide(x, 60), 60)))  with x = ConstDivide(t, 60000000000)
    second(t) = NarrowLane(SUB(y, MUL(ConstDivide(y, 60), 60)))  with y = ConstDivide(t, 1000000000)

Every dividend is nanoseconds of day or a quotient of it, under the type's
bound and so under `ConstDivide.EXACT_DIVIDEND_BOUND` structurally, which is
what group B's rows already rely on. No overflow arm: the values are under
2^47 and the quotients under 86400.

**The store.** A root whose node is `NarrowLane` is stored at four bytes a row
instead of eight, at its own byte offset (`i * 4`, where the lane's is
`i * 8`), as `convertShape(L2I, INT species of the same width, 0)` followed by a
store under a constant mask of the low half of the int lanes - eight of
sixteen at 512 bits, two of four at 128 - hoisted out of the loop. Two things
this shape is chosen for. It uses **no second int species**: the int vector is
the width's own, and `PLAN_TASK_28.md` 2.2 is the reason that matters - a
second `IntVector` species in the JVM makes the shared templates inline
bimorphically and boxes every other int kernel in the process. And its mask is
an **int mask**, which task 153's census found lowered at every width, where
the long masks the kernel's guards use are per-lane at two lanes. The epilogue
narrows its long tail mask to an int one once per batch, which at two lanes is
a per-lane cast on one lane group and not worth a design.

**The evaluator.** Nothing: an `IntegerType` output already allocates an
`IntVector`, and the row path reads it back through the accessor it has. The
output's byte width follows the Spark type, as it does for every output.

**What it costs in the emitter.** An output stride and offset per root
instead of one per kernel - `s.byteOffset` becomes per output where a root
narrows - and the store's two extra operations for those roots; the validity
write is unchanged, since it is per row and not per byte. No change to any
body that has no narrowed root, which the bytes oracle proves: only the three
new coverage rows appear, and no committed hash moves.

**Tests.** The three extracts against the row engine over every second of the
day and the sub-second edges through both consumers, on
`VarkaTimeArithmeticSuite`'s `varka_time_day` fixture; the optimized shape
reaches the compiler as the `StaticInvoke` the table names (2.1's guard, which
already covers the three); `NarrowLane` refused as an interior node and at the
int lane; the narrowed store at both widths and every null pattern in the
emitter suite, with the masked store's constant mask asserted by count; the
coverage rows; `VarkaIrGrammar` unchanged, since a root-only node the grammar
cannot place under another node is not a shape to fuzz until task 28 makes it
one - the reach test names it as deliberately out of reach.

**Predictions, to register before the build.**

1. `hour(t)` fused runs within 10% of `VarkaTimeBenchmark`'s long conversion
   form on `hour` at both widths: the narrowing changes the store, not the
   divide.
2. `minute(t)` and `second(t)` likewise track the file's `minute` and `second`
   rows; the three together its three-field row.
3. Against the row engine, `hour(t)` over the day fixture is at least 15x
   faster end to end through the columnar consumer, `LocalTime` being the
   comparand.
4. No committed hash in `emitted_bytes.json` moves; three rows are added.

### 8.4 Routes B and C: the pieces, for when the cache can hold them

**`BoundedDivide(child, divisor, bound)`**, an int-lane node for a
non-negative dividend the caller proves under `bound`: one multiply and one
logical shift, exact by construction because the constructor derives `(M, k)`
by the search `VarkaTimeBenchmark.magic` runs - the largest shift whose
unsigned product stays under 2^32 and whose quotient is exact over
`[0, bound)`, proven by exhaustion at construction - and refuses a pair that
does not exist. It is the calendar prefix's magic multiply given a node, and
task 149's multiply-high is its unbounded sibling; the two are the int lane's
division family. The bound is the caller's obligation, as `ConstDivide`'s is,
and here the caller can discharge it structurally: a seconds-of-day column is
under 86400 by the leaf that made it, and `s - hour * 3600` is under 3600 by
arithmetic. No guard rides it, and none should: a guard is a compare and a
mask, and task 153 says what those cost at two lanes.

**The split leaf**, `VarkaDerivedKind.TIME_SECONDS` (and `TIME_NANOS` for the
fraction, when `second_with_fraction` needs it): a derived input on
`next_day`'s and `trunc`'s pattern - the compiler interns it under
`VarkaDerivedInput.key`, the evaluator fills it per batch, the kernel sees an
int column. Filled by a long-lane Varka kernel with route A's narrowing store
(`NarrowLane(ConstDivide(t, 1000000000))`), never by a scalar loop, for the
reason 8.2 gives. This is route B; it is worth building only as the fill step
of route C, and the benchmark row that decides is the three-field shape against
route A's.

**The cache encoding**, route C: the split as a second physical form of a
`TIME` column in `ArrowCachedBatchSerializer`, `isArrowBacked` admitting the
pair, the compiler taking the seconds leaf when the batch carries it and route
A otherwise. `SCOPE_MILESTONE_6.md` item 11 owns it; the numbers here are its
case.

### 8.5 Sequencing

1. Route A - `NarrowLane` as a root, the three arms, the store, the tests, the
   rows - closes group C's extracts and 2.5's question. Size: small to medium,
   one PR.
2. `BoundedDivide` with its search and proof, priced in `VarkaTimeBenchmark`
   as the emitted twin of the hand-written arm. Size: small, one PR, and it
   serves the calendar's own divisions as a node later.
3. The split leaf and the cache encoding under item 11, when milestone 6 takes
   the representation question up; `make_time` with task 28 and 2.3.

### 8.6 Outcome of route A, 20 September 2026

Built as 8.3 planned it, in one PR, and the plan's account of the cost held:
one node, one store, three compiler arms, nothing in the evaluator.

**What landed.** `NarrowLane(child)` is in the IR as an `INT` value over a
`LONG` child, refused over an int child where it is built. The emitter admits
it at an output root and nowhere else, checked before the lane check so the
refusal names the position; its value is the child's, its validity word is the
child's, and its store is `convertShape(L2I)` into the int species of the same
width followed by `intoMemorySegment` under `indexInRange(0, lanes)` at
`i * 4`. No second int species, and an int mask rather than a long one, for the
two reasons 8.3 gave. `hour(t)`, `minute(t)` and `second(t)` compile to the
trees 8.3 wrote down, under a narrowing root, and a `TIME(3)` column reaches
them through the widening cast group B already admitted.

**One thing 8.3 did not name: the lane of a kernel is no longer its root's.**
Every place that chose a species, a `run` overload or a buffer width had read
`roots.head.laneType()`, and a narrowing root is the first node for which that
is the wrong answer. `VarkaVectorIR.emissionLane` is the one accessor now, and
the emitter, the compiler, the bytes oracle and the width audit ask it. The
last two were found by the suites rather than by the search that fixed the
first two, which `sql/varka/skills/emitter-and-ir.md` records as the lesson.

**Three doors, all closed.** The emitter refuses an interior narrowing. The
compiler declines it first: `compileRoot` carries an `atRoot` flag into the
one place the node is made, so `hour(t) + 1` and `hour(t) = 12` decline with
a reason that says the narrowing is the store's and names task 28, while the
entry beside them fuses (`VarkaTimeArithmeticSuite` runs `hour(t) + 1` next to
`minute(t)` and checks both answers). And the fuzzer's two reach tests name the
node as deliberately out of reach, since the grammar composes nodes under nodes.

**Tests, as 8.3 listed them.** Every second of the day at three precisions
through both consumers against `LocalTime`; the narrowed store at two and eight
lanes over every null pattern beside a wide root in the same kernel, with the
store counted from the bytes (one masked `IntVector` store per narrowed root
across the dense bodies, the same in the masked epilogue, nothing else on the
int species); the refusals; the coverage rows; the compiler's trees. The
reference evaluator answers a narrowing root in full and the suite narrows,
so a value that did not fit an int would show as a difference rather than be
truncated on both sides.

**The census says the store is intrinsic at every width.** `width_audit.json`
gained the three shapes at 128, 256 and 512 bits and C2 said nothing about any
of them: the int-masked store lowers at two long lanes, where task 153 found
every long mask refused, which is what the int-mask choice was for. Two
unrelated rows moved in the same regeneration, a `missing constant` line
appearing under `add_months(d, i)` at 128 bits and disappearing under
`greatest(l, l2)` at 256; the census's own description calls that line a
first late-inline attempt and not a verdict, and it flips between runs on this
host, so the local census check is unstable on lines it should not record.
That is a finding for the milestone, not this task's.

**Predictions scored.**

1. *`hour(t)` within 10% of the long conversion form at both widths.* **Held.**
   `VarkaTimeBenchmark`, L2 rung, M rows/s, narrowed store against the wide
   store of the same tree: 4078.7 against 4247.9 at 512 bits (4% under) and
   2255.5 against 2380.7 at 128 bits (5% under). The narrowing changes the
   store and not the divide, and the store is a small part of a divide-bound
   kernel. Past L3 the two widths part: at 512 bits the narrowed `hour` reads
   3277.3 against 2616.5, a quarter faster, since it writes half the bytes into
   a memory-bound rung; at 128 bits it reads 1961.2 against 2280.1, 14% slower.
2. *`minute` and `second` track the file's rows; the three together its
   three-field row.* **Held at 512 bits, missed for the heavier shapes at 128.**
   At 512 bits, L2 rung: `minute` 2171.5 against 2126.7, `second` 1416.0
   against 1419.6, the three 1388.2 against 1407.4 - all within 2%; past L3 the
   three read 1109.3 against 968.3. At 128 bits `minute` tracks (1149.3 against
   1182.5) but `second` reads 678.8 against 773.2, 12% under, and the three
   628.8 against 765.6, 18% under, at every rung. Two long lanes amortise the
   per-group cost of the `L2I` and the masked store over two rows rather than
   eight, which is the direction of the effect but does not by itself say why
   `second` pays more than `hour`; the cause is not established here and is the
   first question for the 128-bit row of this benchmark when `BoundedDivide`
   (8.4) changes the divide's share.
   The 256-bit file sits between the two: `hour` 4397.2 against 4740.7 at the
   L2 rung (7% under) and 3343.4 against 2622.0 past L3; `second` 1418.4
   against 1471.9 and the three 1347.1 against 1485.4 at L2.

4. *No committed hash moves; three rows are added.* **Held exactly**: the
   flattened-key diff of `emitted_bytes.json` shows the three rows at both
   widths and no other key added, removed or changed - including the fuzz
   blocks, which a node the grammar draws would have reshuffled and this one,
   being root-only, does not.

Prediction 3 (at least 15x against the row engine end to end) is not scored
here: no committed benchmark runs a `TIME` expression through the whole
engine against the row path, and this repository adds a baseline benchmark as
its own PR before the change it measures. It stays open as a follow-up row.

