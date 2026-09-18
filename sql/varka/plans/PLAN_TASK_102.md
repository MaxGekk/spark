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

The lane is task 29's, unchanged. `TimeType(p)` stores nanoseconds since midnight
in a long whatever `p` is, so every value lies in `[0, 86 399 999 999 999]`,
below 2^47 - which is what makes every division a `TIME` expression needs exact
through task 88's double-lane route with no bound to prove and nothing to
decline.

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
decision is how to recognise these five, and it is not a detail:

* **By target** - `StaticInvoke`'s `staticObject`, `functionName` and argument
  types. Precise, and it binds Varka to a helper *method name* in
  `DateTimeUtils`, which upstream may rename without ceremony. A rename would
  turn fusion off silently, which is the failure mode this project keeps finding
  in its own tooling.
* **By matching before replacement**, which is not available: the physical plan
  is what the compiler is handed.
* **A guard test that fails when the target disappears.** Whatever the match, a
  test must assert that the expression Spark actually produces for
  `hour(t)` is the one the compiler matches - built by running the analyzer and
  optimizer over the SQL, not by constructing the node by hand. Without it the
  arm silently stops matching on the next upstream rename and the only symptom is
  a benchmark that got slower.

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

### 2.3 Two expressions decline on their output type, not their arithmetic

`SecondsOfTimeWithFraction` returns a `Decimal`, and `TimeToSeconds` returns
`DecimalType(14, 6)`. Varka has no decimal lane. They decline, and the decline
must name the output type - the same distinction task 89 had to draw for
`extract(MONTH FROM ym)`, where a reader who saw "declined" would otherwise
conclude the division was still missing.

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

| expression | lowering | note |
|---|---|---|
| `hour(t)` | `/ 3.6e12` | `StaticInvoke`, 2.1 |
| `minute(t)` | `/ 6e10` then `floorMod 60` | `StaticInvoke` |
| `second(t)` | `/ 1e9` then `floorMod 60` | `StaticInvoke` |
| `make_time(h, m, s)` | two multiplies and adds under a range check | `StaticInvoke`; the fractional second is a decimal and declines unless literal |
| `time_trunc(unit, t)` | a division and a multiply, at a foldable level | `trunc(d, fmt)`'s rule for the level |
| `t1 - t2`, `timediff(...)` | a day-time interval, `/ 1000` | exact by range |
| `t + dt` | `t + micros * 1000` under a range guard | 4.1 |
| `time_to_seconds` etc. | multiplies and divisions by powers of ten | `TimeToSeconds` declines, 2.3 |
| `second_with_fraction` | - | declines, 2.3 |

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

## 6. Sequencing

1. This PR: the plan.
2. Task 88 step 3's long-lane converts, with `useAVX` in the shape key resolved
   per section 3.
3. The four ordinary expressions, which need no new matching mechanism.
4. The `StaticInvoke` matching and the five replaceable ones, with test 2 first.
5. The declines, the coverage rows, and task 119's `TIME` arms.

## 7. Outcome

*To be written when the work lands.*
