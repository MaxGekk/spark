# Task 88: an exact division through double lanes

*Milestone 5, section 2.19. Planned 17 September 2026, with the admission check
run rather than argued. Second draft the same day, after a review found that the
first one certified divisions Spark does not perform; section 2.2 records what
moved.*

## 1. Where this came from

Milestone row 88, opened on 9 September 2026 from task 68's admission check, and
the spine section 1.1 sorts to: `... 85 -> 29 -> 88 -> 102, 103, 104 -> 105`.
Every division Varka does today is a range-narrowed magic multiply, because the
Vector API has no integer divide and no multiply-high on any lane. That is
task 26's machinery: round-down constants, correction carries, `VarkaChrono`'s
range guards, and the bound that made task 68 defer `extract(YEAR FROM ym)` - the
`/12` magic is exact over 0..49,151, one forty-thousandth of the type's range.

Section 2.19's observation is that there is a second route nobody had
considered: widen to *double* lanes. `(double) v` is exact for every int32, IEEE
arithmetic is correctly rounded, and `D2I` truncates toward zero, which is
Java's `/`. At the int32 lane this is an A/B against the shipped magic. At the
long lane it is the only route there is - no 64-bit magic exists without a
128-bit product - and 1.1 names it as how `hour(t)`, `minute(t)`, `time_trunc`
and the interval day extract are computed at all. Tasks 102 and 103 wait on it,
which is why it is next after 29.

## 2. The admission check, done

`verify_double_division.py`, beside `verify_long_lane_magic.py`, run on
17 September 2026 (six seconds). It answers three questions section 2.19 left
open: what the constant is, which form may be used where, and - the one the
first draft got wrong - which divisions Varka actually performs.

**The constant.** RECIP (`trunc(v * fl(1/d))`) has relative error at most
`2^-52`; DIV (`trunc(v / d)`) at most `2^-53`. A non-multiple's true quotient
lies at least `1/d` from every integer, so truncation cannot cross one while
`|v| < 2^52` for RECIP and `|v| < 2^53` for DIV. **The bound is on the dividend
and does not depend on the divisor** - which is what 2.19's "divisor below 2^21"
shorthand obscured, and what the earlier "fits a double" shorthand did not state.

**At an exact multiple the bound says nothing, and a closed form does.** DIV
returns `k` exactly. RECIP computes `fl(k * fl(1/d))`, and if `fl(1/d)` rounded
*down*, the product can land on the double just below `k`. The rule:

    fl(1/d) >= 1/d  ->  exact at every multiple below 2^53
    fl(1/d) <  1/d  ->  exact at every multiple iff trunc(d * fl(1/d)) == 1

The script checks that rule against direct evaluation for every divisor in
`[2, 20000)` over every multiple index below `2^14`, and no divisor disagrees.
The textbook failure is `d = 49`; Varka's own `/146097` is another.

**Admissibility is a property of the (divisor, dividend range) pair.** The same
`146097` is inexact under RECIP over the era step's `w`, where `146097` itself is
a reachable dividend, and *exact* over the Julian century's `quadDays = 4*doe+3`,
whose dividends are only the values congruent to 3 mod 4 - and `146097` is
congruent to 1. The first draft of this plan proposed a table keyed on the
divisor alone, which cannot express that.

**The rows, each a division some lowering performs over the range it sees:**

| division | dividend range | RECIP | DIV |
|---|---|---|---|
| era step, narrowed `/146097` | `w < 2^24` | **no**, from 146097 | exact |
| era step, total `/146097` | the biased int32 range | **no**, from 146097 | exact |
| Julian century `/146097` | `quadDays = 4*doe+3`, every one | exact | exact |
| Julian year `/1461` | the mapped count, to 584399 | exact | exact |
| century `/36524`, year `/365`, month `/153`, day `/5`, quarter `/3`, week `/7`, day of month `/2141` | as `VarkaChrono` documents | exact | exact |
| year of era `/100`, `/400` | 0..399, the biased year | exact | exact |
| `extract(YEAR FROM ym)` `/12` | **all of signed int32** | exact | exact |
| `hour(t)` `/3.6e12` | nanoseconds of day | exact | exact |
| `minute(t)`, `second(t)`: `/6e10`, `/1e9` then `/60` | nanos of day, then the minute or second of day | exact | exact |
| `time_trunc` `/1e6`, `/1e3` | nanoseconds of day | exact | exact |
| `extract(DAY FROM dt)` `/8.64e10` | signed micros in `[-2^52, 2^52]` | exact | exact |

Four things follow, and the first two correct section 2.19.

1. **The reciprocal form is out for the era step**, the one divisor the calendar
   most wanted freed from its narrowing. 2.19's sampled check tried 12, 3, 7 and
   100 and generalised.
2. **`/12` is exact over the whole signed int32 range in both forms**, so
   `extract(YEAR FROM ym)` and `CAST(ym AS INTERVAL YEAR)` need no bound and no
   guard: task 68's deferral is removed by either lowering.
   (`extract(QUARTER FROM ym)` is *not* removed, because it does not exist -
   `intervalExpressions.scala` admits only YEAR and MONTH for a year-month
   interval and throws otherwise.)
3. **`TIME`'s minute and second are a division and then a `/60`**, not one
   division: `DateTimeUtils.getHoursOfTime` and its siblings go through
   `LocalTime`, whose fields are `nanos / unit` and then `% 60`. Both steps are
   exact in both forms, so the lowering is two divisions rather than one - which
   the op counts in 3.3 must carry.
4. **The `2^52` bound is not slack.** Searched to `2^53`, the day extract's
   `/8.64e10` fails in neither form, so the binding constraint there is the
   argument's bound rather than a measured failure; a dividend past it is the
   recorded decline 2.38 already anticipates.

**What the check would have rejected**: a DIV failure inside a stated range -
there was none. What it did reject is narrower and more useful: one form, for one
divisor, over one range.

### 2.1 What Varka does *not* divide, which is most of the interval family

`IntervalUtils` (lines 47-70), read rather than assumed:

* `getDays(micros) = (micros / MICROS_PER_DAY).toInt` - a flat division, `Int`
  output. This is the one interval extract this task serves.
* `getHours = ((micros % MICROS_PER_DAY) / MICROS_PER_HOUR).toByte` and
  `getMinutes = ((micros % MICROS_PER_HOUR) / MICROS_PER_MINUTE).toByte` - a
  modulo *then* a divide, with **`ByteType`** results.
* `getSeconds = Decimal(micros % MICROS_PER_MINUTE, 8, 6)` - **no division at
  all**, and a `Decimal` result.

So three of the four interval extracts are not this task's business, and their
blocker is the output type - the same `ByteType` wall that 2.19 already names
against `extract(MONTH FROM ym)` - rather than the division. They belong to task
103, which inherits this paragraph. `SecondsOfTimeWithFraction` is a `Decimal`
for the same reason and 2.37 already declines it.

### 2.2 What the review of the first draft corrected, 17 September 2026

The first draft's table had four interval rows modelling flat divisions of raw
microseconds. Three of them - hours, minutes, seconds - are arithmetic Spark
never performs, so the check certified a primitive those consumers do not use,
for outputs Varka cannot emit. 2.1 replaces them. Six further corrections, all
verified against the source and each folded into the section it belongs to: the
divisor-keyed table (now (divisor, range), with the Julian century as the case
that proves it); "no argument settles it" (there is a closed form, now stated and
self-tested); the `/1461` range, twelve dividends short of what
`JULIAN_YEAR_M`'s javadoc documents; the missing Julian `/146097` row; the
non-existent `extract(QUARTER FROM ym)`; and the script's own exit status, which
ignored a RECIP failure and so would have passed a future regression silently.
The script now pins every row's verdict in `EXPECTED` and fails when one moves in
either direction - which it did, catching a modelling error of the second draft's
own: the Julian domain written as an interval rather than a stride.

## 3. The design

### 3.1 The mechanism

**Double lanes are a conversion target inside one node's lowering, not a lane the
IR knows.** No `VarkaVectorIR` node is on a double lane, nothing is loaded into
one and nothing is stored from one; a division node converts in, does one
arithmetic op, converts out, and every node's `laneType()` stays what it is. That
is what keeps task 85's oracle honest.

It is therefore **not** a member of `VarkaLoopEmitter.Lane`. That enum is built
from a `VarkaVectorIR.LaneType`, which has exactly `INT` and `LONG`; `Lane.of`
reverse-matches on that field, and the constructor derives twenty-odd load,
store, compare and blend descriptors that a lane nothing loads has no answers
for. The double machinery is a separate helper - `DoubleConvert`, holding the
two species and the four descriptors the conversion needs - reached from the
division emission and from nowhere else.

**The conversions, with the JDK's own rules.** Species are named by *total vector
width*: a 512-bit `IntVector` of sixteen lanes converts into two
`DoubleVector.SPECIES_512`s of eight lanes each. Expanding takes `part` in
`[0..M-1]` and contracting takes `part` in `[-M+1..0]`
(`Vector.convertShape`'s own javadoc), so the round trip at the int lane is
`I2D` with parts `0, 1` and `D2I` back with parts `0, -1` - not "the same
parts", which throws `ArrayIndexOutOfBoundsException` on the second half. At the
long lane the conversion is same-width and `part` is 0 both ways.

**Two forms behind one switch, the magic kept as the reference variant.**
`VarkaEmitOptions` gains `division`, an enum `{ MAGIC, DOUBLE_RECIP, DOUBLE_DIV }`
on the `FloorMod7` precedent, so the shipped lowering stays live under the same
tests and the default flips only if the numbers say so. Two things the first
draft left undefined:

* **`MAGIC` has no long-lane meaning**, since no 64-bit magic exists. The default
  is therefore per lane: `MAGIC` at the int lane, `DOUBLE_DIV` at the long lane,
  which is the only correct lowering there. One enum, two defaults, stated here
  and asserted by a test.
* **The table is a deny-list and only a deny-list.** A (divisor, range) pair the
  script proves inexact under RECIP is refused with a reason naming the divisor
  and the range; everything else is emitted in the requested form. A pair absent
  from the table is *not* silently downgraded - absence means "not known to be
  inexact", which is the only reading under which `DOUBLE_RECIP` is reachable at
  all.

**The AVX2 conversion is for non-negative dividends only.** Under
`-XX:UseAVX=2` the long-to-double converts do not intrinsify (milestone section
6, measured), and the fallback is the magic-number form from
`dev/varka_canary/MagicProbe.java`. That probe documents its domain as
`0 <= v < 2^52` and it produces a *floor*: `v | 0x4330000000000000` on a negative
long corrupts the sign and exponent outright, and floor differs from truncation
on every negative non-multiple. So the AVX2 form serves `TIME` and the
non-negative calendar dividends; a signed long-lane division under AVX2 - today
exactly `extract(DAY FROM dt)` - either takes the sign-corrected form (negate,
divide, negate, which is exact because truncation is odd) or declines to the
magic-free path. Which, is the first emitter commit's measurement, and test 4
covers both signs either way.

**`UseAVX` must enter the shape key.** It is not a `VarkaEmitOptions` field
today and nothing under `sql/catalyst` reads it, so two hosts would emit
different bytes under one key and the single committed `emitted_bytes.json`
could not pin both. It becomes an explicit option field, defaulted from the JVM
at session start, so the cache key and the oracle both see it.

**Truncation is not floor**, and the double route truncates toward zero, which
is Java's `/`. That is what every calendar division needs over its non-negative
dividends and what `extract(YEAR FROM ym)` needs over both signs. It is *not*
what the `floorMod7` shapes need, whose result is a remainder; those keep their
own lowering. A remainder at full width is 2.19's stated rejection case and
stays one - which is also why 2.1's mod-then-divide extracts are out of scope.

### 3.2 What is deliberately unchanged

* **The calendar prefix's magics stay the default** at the int lane. This task
  makes the alternative real and measured; it does not flip a default on the
  strength of a simplification.
* **The `floorMod7` lowerings**, and any shape whose result is a remainder.
* **The IR**: no new node and no new `LaneType`.
* **The three interval extracts of 2.1** and `extract(MONTH FROM ym)`, whose
  blocker is `ByteType` and `Decimal` output, not division - task 103's and 89's.
* **The compiler's year-month extract arms** - admitting `extract(YEAR FROM ym)`
  is task 89, which this task unblocks by removing its range bound.
* **`TIME` and interval expressions** - 102 and 103 consume this lowering.
* **The `2^52` bound for intervals**: past it the decline of 2.38 stands.

### 3.3 Registered op counts

Hand-derived here and replaced by `dev/varka_emit.sh --table` in the first
emitter commit, which is what section 5's test 5 asserts.

| lowering | ops |
|---|---|
| shipped magic, era step (`VarkaChrono.eraOf`) | add, mul, shift, mul, sub, compare, add = **7**, plus the guard's compare and OR |
| double route, int lane | 2x `I2D`, 2x (mul or div), 2x `D2I`, 1 join = **7**, no guard |
| double route, long lane | `L2D`, mul or div, `D2L` = **3**, no guard |
| double route, long lane under AVX2 | or, reinterpret, sub (3) + mul or div (1) + add, reinterpret, and (3) + the round-to-floor compare and blend (2) = **9** |

The first draft costed the magic at four ops by counting only the multiply,
shift, compare and blend, and omitted the bias add and the multiply-subtract
that form the remainder the correction compares.

*Measured 18 September 2026, step 2, from `dev/varka_emit.sh --table` - which had
to be corrected first: it reported only `IntVector` invocations, so a body that
moved a division onto the double lane read as **cheaper** than the magic. It now
sums every vector type, and the numbers below are lane ops per `loopDense0`, not
int ops.*

| expression | `MAGIC` | `DOUBLE_DIV` | `DOUBLE_RECIP` |
|---|---|---|---|
| `year(d)` | 34 | 40 | 38 |
| `month(d)` | 35 | 41 | 39 |
| `dayofmonth(d)` | 36 | 47 | 45 |
| `quarter(d)` | 38 | 49 | 47 |
| `dayofyear(d)` | 43 | 49 | 47 |
| `weekofyear(d)` | 64 | 75 | 73 |
| `last_day(d)` | 63 | 74 | 72 |
| `trunc(d, 'MM')` | 36 | 47 | 45 |
| `add_months(d, 1)` | 112 | 147 | 145 |

Every `DOUBLE_RECIP` column is exactly two below its `DOUBLE_DIV` neighbour, and
that gap is the deny-list: the era step's `/146097` falls back to the magic, so
one division's seven ops become two.

The table above also corrects this section's accounting of what the double form
displaces. The seven ops do not replace two - they replace the magic's multiply
and shift **and**, at a site that rounds down, the three-op carry that corrects
it, because an exact quotient leaves a remainder below the divisor and the
correction can never fire. The emitter elides it rather than emitting dead code,
so the trade at a correcting site is seven against five, and at a site that was
already exact it is seven against two. `year(d)` divides three times and corrects
all three: `3 * (7 - 2) - 3 * 3 = +6`, which is what the table reads.

## 4. Files

| file | what |
|---|---|
| `sql/varka/plans/verify_double_division.py` | the admission check (this PR) |
| `sql/varka/skills/vector-api-and-width.md` | the negative result, per `AGENTS.md` (this PR) |
| `VarkaEmitOptions.java` | the `division` switch, its per-lane default, and the `useAVX` field |
| `VarkaLoopEmitter.java` | `DoubleConvert`, the convert helpers, the AVX2 form, the (divisor, range) deny-list, the division emission behind the switch |
| `VarkaLoopEmitterSuite.scala` | the parity matrix under each setting at both widths and both AVX levels, the op counts, the era step's refusal |
| `VarkaEmittedBytesSuite` / `emitted_bytes.json` | unchanged while the int-lane default is `MAGIC`; regenerated if it flips |
| `VarkaEmitterParityBenchmark.scala` and its results | the A/B rows |
| `sql/varka/plans/PLAN_MILESTONE_5.md` | 2.19's corrections, row 88 |

## 5. Tests, and what each is for

1. **The script**, committed and run here; its `EXPECTED` table is what the
   emitter's deny-list is transcribed from, and a verdict that moves fails it.
2. **The parity matrix under each `division` setting**, the calendar shapes at
   both vector widths against the reference evaluator: catches a conversion that
   saturates, a wrong `part`, a join that drops a half.
3. **The era step under `DOUBLE_RECIP` is refused**, with a reason naming the
   divisor *and the range*, while the Julian century's `/146097` under the same
   setting is admitted - the pair that proves the key is (divisor, range). If a
   later edit proves a range admissible by the closed form and removes its entry,
   this test is what says so out loud.
4. **Signs and AVX levels.** The long-lane divisions over negative dividends,
   under the host width and under `-XX:UseAVX=2`, including
   `extract(DAY FROM dt)` at `-1` micro, where floor and truncation differ.
5. **The op counts** of 3.3 asserted from `dev/varka_emit.sh --table`.
6. **The oracle**: `VarkaEmittedBytesSuite` green with no regeneration while the
   int-lane default is `MAGIC`, which is task 85's own admission rule.

## 6. The measurement

The A/B on `VarkaEmitterParityBenchmark`'s calendar shapes at both widths:
`MAGIC` (shipped, the **control row**), `DOUBLE_DIV`, and `DOUBLE_RECIP` where
the table admits it. Plus one row only the double route can serve at all:
`extract(YEAR FROM ym)` over a full-range month count, whose baseline is the row
engine because the magic declines it. The long-lane rows arrive with 102 and
103's benchmarks.

### 6.1 Predictions, registered before the run

1. **The divide loses to the magic on the calendar prefix**, by a factor this
   plan does not predict. The first draft claimed 3x on the strength of the
   parity file's `lanewise DIV` row - which is 0.084 of the magic at AVX-512 and
   0.106 at 128-bit, and measures `FloorMod7.DIV`, an *integer* divide that
   `VarkaEmitOptions` documents as scalarizing on every lane type this JVM has.
   It is not evidence about `vdivpd`, and no committed file in this repository
   measures one. The direction is predicted; the factor is what the A/B is for.
2. **RECIP is within 0.7x to 1.0x of the magic where the table admits it** - the
   two converts and the join cost about what the guard and the correction cost -
   so RECIP is the form that could displace a magic, and only where admitted.
3. **`extract(YEAR FROM ym)` fuses at more than 5x the row engine** under either
   double form, because its baseline is a decline: the number is the lane against
   Spark, not one lowering against another.

## 7. Risks

1. **The half-join.** `D2I` of an eight-lane double back into a sixteen-lane int
   species fills half the lanes; if the parts do not compose without a
   `rearrange`, the seven-op count of 3.3 is optimistic and prediction 2 fails on
   the join rather than the arithmetic. Test 5 is what measures it.
2. **Saturation and negatives.** `D2I` and `D2L` saturate out of range; every
   dividend here is in range by construction, and the signed cases truncate
   toward zero as Java does. Test 4 carries both signs and the extremes.
3. **The AVX2 form's domain.** It is floor-only and non-negative-only (3.1); the
   danger is a later division quietly routed through it. The deny-list carries
   the range, and test 4 drives the signed case at both AVX levels.
4. **A (divisor, range) pair the table does not know** is emitted in the
   requested form. The script is the table's source, so an unknown pair means
   nobody has checked it - test 3 pins the two entries that exist, and a new
   division without a row is a gap the first emitter commit must close.
5. **Two conversions the JIT may not fuse.** If C2 keeps the halves in registers
   the cost is the divide; if it spills, the converts dominate.
   `dev/varka_emit.sh --asm` on the dense loop before any timing.
6. **`useAVX` in the emit options widens the shape key**, so a session that
   changes it re-emits. That is correct and cheap, and it is what lets one
   committed oracle pin both hosts.

## 8. Sequencing

Task 29 merged on 17 September 2026, so both halves are unblocked; the int32 half
is still first because its A/B is what decides the default.

1. **This PR:** the script, the skills entry, 2.19 corrected, the row planned.
   No code.
2. The `division` switch with its per-lane default, `DoubleConvert`, the int32
   converts and the deny-list, with the calendar prefix's steps emitted under
   both double forms behind it; the parity matrix under each setting; the op
   counts measured and asserted. Oracle green, int-lane default `MAGIC`.
3. The long-lane converts, the `useAVX` option field and the AVX2 magic-number
   form, with the `TIME` divisors' parity at both AVX levels and the signed case
   of test 4.
4. The A/B on the parity benchmark, results committed, 6.1 scored, and the
   default per (divisor, range) decided from the numbers; task 89 opens on it.

## 9. Outcome

*To be written when the work lands, section by section as the plan's own rule
asks. Nothing above is to be rewritten to look prescient; a correction is added
and says what it corrects - 2.2 is the first of them.*

### 9.1 Step 2: the int32 converts behind the switch, 18 September 2026

**The switch.** `VarkaEmitOptions.division` is a three-valued enum defaulting to
`MAGIC`, so no production kernel converts anything and the oracle needed no
regeneration. Every calendar division already funnelled through one two-line
helper, `emitMagic`, at fifteen call sites, which is why the whole lowering is a
single branch rather than fifteen edits.

**The table the sites now name.** The call sites passed magic constants, not
divisors, and two of them - the year of era's `/400` and `/100` - share a
multiplier and differ only in the shift, so neither the divisor nor the range was
recoverable from the call site. A `ChronoDivide` enum now carries the divisor,
the pair, and the reciprocal verdict transcribed from `verify_double_division.py`,
one constant per site, and a site names a division instead of two integers.

**Threading.** Only three of the eleven helpers that divide carry an `Analysis`.
Rather than thread one through the other eight, the resolved choice is passed
down as a `Divider`, the way `emitChronoTrunc` already takes a `TruncDateForm` -
and it is computed once, as a field on `Analysis`, so the fifteen sites cannot
disagree about it.

**Risk 1 did not materialise.** `dev/varka_canary/DoubleDivProbe.java` settled the
conversion before any bytecode was written: expanding parts `0, 1` in, contracting
parts `0, -1` out, and the two contracted halves are lane-disjoint - each fills
the lanes the other left at zero - so a plain `or` rejoins them. No `rearrange`,
no blend, no mask. The seven-op count of 3.3 holds, and the probe is committed so
the next reader does not have to re-derive it.

One detail made the emission simpler than the plan expected: the species
constants are named by the vector's **total width**, not by its lane count, so
`IntVector.SPECIES_256` and `DoubleVector.SPECIES_256` name the eight int lanes
and the four double lanes of the same register. One field name therefore serves
both sides of the conversion.

**The deny-list is observable, not just enforced.** Under `DOUBLE_RECIP` the
narrowed prefix lowers two of its three divisions and the Julian prefix lowers two
of its three - in both cases the one held back is the era step, and in the Julian
case the division that *is* lowered divides by the same 146097. That is the
(divisor, range) key proved from the emitted bytes, and it is asserted as such.

**What was checked.** The parity matrix under all three settings, on both prefix
forms, at every vector width from two to sixteen int lanes; `add_months`,
`make_date`, the recomposing `trunc` and `weekofyear`, which reach the four
division sites no extraction does; and - the check that matters at the resolution
the deny-list was decided at - the opt-in sweep of every day the prefix covers
under both double forms and both prefix forms, against `LocalDate` and
`DateTimeUtils`. All green, `VarkaEmittedBytesSuite` unmoved, and
`verify_double_division.py` re-run here with every verdict matching.

**Still owed by step 3:** the long-lane converts, `useAVX`, the AVX2 form, and
test 4's signed dividends - none of which the int lane reaches, because the
calendar prefix is int-lane only by construction.
