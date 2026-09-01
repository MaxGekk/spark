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

Built as planned: `VarkaVectorIR.DayOfYear`, wired through the five emitter
switches and `emitChrono`'s tail switch, a `VarkaExpressionCompiler` arm, and
tests in all four files section 4 named. This worktree was branched directly
off `master` (post PR #60, which already carries task 26) rather than off a
merged task 33 - task 33 was still sitting in its own unmerged worktree at the
time, so this task never saw `NextDay` or the `LANEWISE_UNARY` descriptor
`PLAN_TASK_33.md` section 3 mentions. That is not a deviation: the "follow
section 3 for the mechanics" pointer is about the shape of the edit (five
switches, a compiler arm, two pinned fixtures), which does not depend on
`NextDay` existing.

**One step in this recipe was wrong, and it was a real defect, not a
clarity problem.** Section 2.1's leap-flag constants (`M=167773` at `k=24`
for mod 100, `k=26` for mod 400) are exact only under unbounded-precision
arithmetic. They silently violate the `v * M < 2^31` no-overflow bound this
very file's own opening javadoc derives and every other magic in it respects:
at the covered range's top (biased year 46334), `46334 * 167773 ~ 7.77e9` -
over three and a half times past `2^31`. `plans/verify_chrono_tails.py`'s
`mod_magic` helper never caught this because Python integers do not overflow,
so the script confirmed the formula was mathematically exact without ever
computing what a 32-bit lane multiply actually produces. `VarkaChronoSuite`'s
boundary/random sweep did catch it - `Fields[year=14500, ..., dayOfYear=280]`
against `LocalDate`'s `279` on day 4576767 - because that suite's oracle is
`LocalDate` compared against `VarkaChrono`'s own Java arithmetic, which is
genuine `int` arithmetic and genuinely overflows where the constant is wrong.

The fix: round-down magic constants that respect the bound (`M=41943` at
`k=22` for mod 100, `k=24` for mod 400 - equal by construction, since
`400 = 100 * 4` and `k` is two higher, which cancels exactly). Round-down
means the quotient can undershoot by one, so unlike a same-shape `== 0` test
the corrected form checks the remainder against **both** `0` and the divisor
(`r == 0 || r == d`), the same "round-down plus one correction" idiom
`emitCarry`/`CENTURY_M` already use elsewhere in this file, adapted to a
modulo test rather than a quotient one. Verified over the full covered range
(years -12800..33134) against the reference leap rule with a Python
simulation of true 32-bit truncating multiplication, not the unbounded
arithmetic the first pass trusted - zero mismatches.

That fix needed **one more scratch slot than the plan anticipated**: nine
int-vector chrono temporaries, not eight. Section 3 said "one more slot for
the biased year"; the corrected leap test also needs a slot to hold each
remainder across its two-way equality check, since it is read twice.

Everything else held exactly as written: the `doy >= 306 ? doy - 305 :
doy + 60 + L` formula, `MARCH_TO_JANUARY_DAYS = 306` and
`MARCH_DAY_OF_YEAR = 60`, and the "do not hoist the leap flag into
`emitChrono`'s main body" instruction - `emitLeapFlag` is a private helper
called only from `DayOfYear`'s case, ready for tasks 35 and 37 to call but
not forced on `Year`/`Month`/`DayOfMonth`/`Quarter`.

**Pinned values moved**, both re-pinned here as the update rule requires:
`VarkaLoopEmitterSuite`'s line map key (line 21 `(dayOfYear 1)` inserted, the
`if`'s line moving 25 -> 27) and `VarkaShapeCacheSuite`'s shape hash,
`041e35db20d62e91` -> `e8314287849e8cf8`.

**Final state**: 94 catalyst / 127 sql-core Varka tests green at both vector
widths, `catalyst/doc`, `dev/lint-java` and `dev/scalastyle` all pass, no
non-ASCII/no TODO/no line over 100 characters in any changed file. No
committed benchmark number moved (none was expected to: `dayofyear` adds a
node type and shares the existing shape's guard, touching no existing shape).
