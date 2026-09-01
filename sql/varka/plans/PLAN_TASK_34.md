# Task 34: `dayofyear`

One of four small vocabulary tasks (34-37) written as recipes for a cheap
agent, in the shape task 33 established. Read `PLAN_TASK_33.md` section 3
first: the mechanics of adding a node type - the five emitter switches, the
compiler arm, the two pinned fixtures - are the same here and are not repeated
in full.

**Depends on task 26.** Everything below builds on `VarkaChrono` and
`VarkaLoopEmitter.emitChrono`, so this task cannot start until task 26 has
merged.

If you find yourself making a design decision, stop and say so in the pull
request instead of choosing. Everything here has been decided and checked.

**Task 32 may move the plumbing; it will not move the arithmetic.** A separate
task is measuring whether one shared decomposition can feed several fields,
instead of each node carrying its own copy of it. If that goes ahead, the tails
will read `doy` and `dom` from somewhere else - and every formula in section 2
will be unchanged, because what moves is where the intermediates live, not what
they are. So do not restructure `emitChrono`, do not try to share anything with
the other tails, and do not read the duplicated decomposition as a bug to fix.
Write the tail this recipe describes and let task 32 do its own job.

## 1. What you are building

`dayofyear(d)` returns 1 for 1 January and 365 or 366 for 31 December.
Spark's reference is `DateTimeUtils.getDayInYear`, which is
`daysToLocalDate(days).getDayOfYear` - `LocalDate`, which is exact at every
int day and never wraps. So unlike task 33, the ordinary rule applies: **do
not let any intermediate overflow.** Nothing below can.

## 2. The lowering

`emitChrono` already computes, for every calendar node, a day-of-year `doy` in
a **March-based** year: `doy == 0` is 1 March, and the year runs to the
following February. January-based day-of-year is one comparison away:

```
L        = leap(reported year)                 // section 2.1
dayofyear = doy >= 306 ? doy - 305 : doy + 60 + L
```

306 is the number of days from 1 March to 31 December, so `doy >= 306` is
exactly "January has arrived" - the same test the `year` tail already uses,
and task 26 verified that equivalence exhaustively. 60 is the day-of-year of
1 March in a common year (31 + 28 + 1).

**Verified, not assumed**: this formula was checked against `java.time` over
all 3,652,059 days of `0001-01-01..9999-12-31` during planning - zero
mismatches. The first draft used 59 instead of 60 and failed on 84% of days;
if your differential fails everywhere at once, this constant is the first
place to look.

### 2.1 The leap flag, which three of these four tasks need

The reported (January-based) year is a leap year iff the usual rule holds, and
it is computed with two magic multiplies over a **biased** year, because the
magic form `(v * M) >>> k` requires a non-negative `v` and Varka's covered
range starts at year -12800:

```
y   = year + 13200          // 13200 is a multiple of 400, so leapness is unchanged
L   = ((y & 3) == 0) && ((y mod 100) != 0 || (y mod 400) == 0)
```

with `mod` done as `v - ((v * M) >>> k) * divisor`:

| divisor | M | k | exact for dividends up to |
|---|---|---|---|
| 100 | 167773 | 24 | 199728 |
| 400 | 167773 | 26 | 199728 |

`y` reaches at most 46334 over the covered range, well inside both bounds, so
neither division needs a correction step. 13200 rather than 12800 because task
37 needs `year - 1` to stay non-negative too, and one constant for all four
tasks is easier to keep right than two.

Do **not** try to shortcut this from `yoc` and `century` with bit tricks. It
is tempting - `y mod 4 == yoc mod 4` and so on - and it goes wrong at the
century and era boundaries where the reported year has already rolled over.
Ten honest ops beat a clever four that fail two days a century.

## 3. The edits

Follow `PLAN_TASK_33.md` section 3 for the mechanics. The differences:

* **IR** (`VarkaVectorIR.java`): add `DayOfYear(VarkaVectorIR days)` beside
  `Quarter`, rendering as `(dayOfYear <days>)` in both `canonical` and
  `canonicalShallow`. Copy `Year` exactly - it is a unary node with the same
  shape.
* **Emitter**: `DayOfYear` is a *chrono* node, so unlike task 33 it does not
  get its own `emitValue` body. Instead:
  - add it to `isChrono` so it gets the chrono slots and the `CHRONO_WEIGHT`;
  - add the four routine cases (`childrenOf`, `analyze`, `planWordRef`, and an
    `emitValue` arm that calls `emitChrono` exactly as the `Year` arm does);
  - add a `case DayOfYear n -> {...}` to the switch **inside** `emitChrono`
    that picks the per-field tail, emitting section 2's formula from `rem`
    (which holds `doy` at that point) and the leap flag.
* **The leap flag** does not exist in `emitChrono` yet. Add a private helper
  beside the other emit helpers - `emitLeapFlag`, leaving a `VectorMask` -
  and give the chrono temporaries one more slot for the biased year. Task 35
  and task 37 will both call it, so write it to be called, not inlined.
* **Compiler** (`VarkaExpressionCompiler.scala`): one arm beside the other
  four calendar arms, `case DayOfYear(child) => ... .map(new IRDayOfYear(_))`.

## 4. The tests

1. `VarkaLoopEmitterSuite.evalValue` gains a `DayOfYear` arm whose oracle is
   `LocalDate.ofEpochDay(v.toLong).getDayOfYear` - the definition, not your
   formula.
2. A test in the shape of task 26's chrono one, driving the boundary set that
   matters here: 1 January and 31 December of a leap year (2000, 2024), of a
   common year (2023), of a century non-leap year (1900) and a century leap
   year (2000), 28 and 29 February, 1 March, and the ends of the covered range
   (`VarkaChrono.NARROW_MIN_DAYS`, `NARROW_MAX_DAYS`).
3. The two pinned fixtures, extended and re-pinned per task 33 section 3.4.
4. One compiler test and one differential test, both copying the shapes task
   26 added for `year`.

## 5. What to run, and what must pass

The command block and acceptance criteria are task 33's section 4, unchanged.
One addition: a batch containing a day outside `VarkaChrono`'s covered range
must still decline rather than answer, exactly as `year` does - the guard is in
`emitChrono` and you inherit it, but the differential should prove it, so
include the `date_add` push-past-the-range case task 26 added.

## 6. Explicitly out of task 34

* **`extract(DOY from d)`** - it desugars to this same node, so it is covered
  for free; do not add a second path for it.
* **`dayofweek`, `weekofyear`** - already shipped and task 37 respectively.
* **Hoisting the leap flag out of the tails.** Write `emitLeapFlag` so tasks
  35, 36 and 37 *can* call it, which is a shared helper; do not make
  `emitChrono` compute it once for every node, which is a restructure - see the
  note at the top of this file.

## 7. Outcome

Filled in when the work lands, including which steps of this recipe turned out
to be wrong or unclear. That record is worth as much as the feature.
