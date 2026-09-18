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

### 2.1 Five of the nine expressions never reach the compiler under their own name

`HoursOfTime`, `MinutesOfTime`, `SecondsOfTime`, `SecondsOfTimeWithFraction` and
`MakeTime` are **`RuntimeReplaceable`**. Each rewrites itself into a
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

The four that *are* ordinary expressions, matchable the usual way:
`TimeTrunc`, `SubtractTimes`, `TimeAddInterval`, `TimeDiff`.

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

**Group A, and the first thing shipped: same lane, ordinary expressions.**
`time_trunc(unit, t)`, `t1 - t2` and `timediff(...)`. They need neither task 28
nor the `StaticInvoke` mechanism - the same-lane set and the
non-`RuntimeReplaceable` set happen to be the same expressions - so this slice
is independent of both blockers and is what makes the task start moving.
`time_trunc` is the one a query actually writes often, for grouping by hour or
by minute.

**Group B, the story: `hour`, `minute`, `second`, and `make_time` beside them.**
These are what the post is about, and they cost the most - the `StaticInvoke`
table of 2.1 and the width change of 2.4. The three extracts narrow (int64 in,
int32 out) and `make_time` widens (int32 in, int64 out), so 2.5's question
decides the first three and task 28's widening decides the fourth; all four share
one matching mechanism, which is why they belong together.

**Nothing here is declined for want of a mechanism.** Every `TIME` expression
Spark has is vectorizable except the two of 2.3, and those two are waiting on an
Arrow representation rather than on anything about the lane.

**Group C, deferred or dropped, with reasons:**

* **`make_time`** - moved up from here on 18 September 2026: it is vectorizable
  and should be vectorized. Its replacement is
  `StaticInvoke(DateTimeUtils, "makeTime", ...)`, so it costs nothing beyond the
  table 2.1 already builds, and with a foldable seconds argument - which is what
  `make_time(h, m, 30)` gives - the `DecimalType(16, 6)` operand is a constant
  and the whole expression is two multiplies and two adds into a long. What it
  still needs is the widening, since its inputs are int32 and its output int64,
  so it belongs beside group B rather than ahead of it. A seconds argument that
  is a decimal *column* declines, and that decline is about the operand, not the
  expression.
* **`t + dt`** - its semantics are in flux.
  [SPARK-57853](https://issues.apache.org/jira/browse/SPARK-57853) may replace
  the throwing range check with ANSI's modulo-24, so the guard built now is work
  to delete. Cheap to add once that settles, and 4.1 stays as the recipe.
* **`TimeFrom*` and `TimeTo*`** - multiplies and divisions by powers of ten,
  ordinary long-lane work and cheap to add once group A's arms exist.
  `TimeToSeconds` alone is held back by its `DecimalType(14, 6)` output.
* **`second_with_fraction` and `TimeToSeconds`** - not declined on principle:
  blocked on the Arrow decimal representation, 2.3, and admitted unchanged the
  day one exists. This is the milestone's clearest pull towards a second
  representation for a logical type.

## 6.1 Sequencing

1. This PR: the plan.
2. **2.5's question**: can the store narrow, or does this wait for task 28?
3. Group A, which needs neither answer.
4. Task 88 step 3's long-lane converts, with `useAVX` in the shape key resolved
   per section 3.
5. Group B: the `StaticInvoke` matching and the three extracts, with test 2
   first.
6. The declines, the coverage rows, and task 119's `TIME` arms.

## 7. Outcome

*To be written when the work lands.*
