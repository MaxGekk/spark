# Task 37: `weekofyear`

The last and much the hardest of the four small vocabulary tasks (34-37), and
the only one where the honest advice is: **read all of section 2 before
writing any code.** ISO week numbering is not "day of year divided by seven",
and every wrong implementation of it is wrong only on a handful of days a year,
which is exactly the failure mode a thin test suite misses.

Read `PLAN_TASK_33.md` section 3 for the mechanics of adding a node type.
**Depends on tasks 26 and 34** - 26 for `emitChrono`, 34 for `emitLeapFlag`
and the January-based day-of-year.

**Task 32 may move the plumbing; it will not move the arithmetic.** A separate
task is measuring whether one shared decomposition can feed several fields,
instead of each node carrying its own copy of it. If that goes ahead, the tails
will read `doy` and `dom` from somewhere else - and every formula in section 2
will be unchanged, because what moves is where the intermediates live, not what
they are. So do not restructure `emitChrono`, do not try to share anything with
the other tails, and do not read the duplicated decomposition as a bug to fix.
Write the tail this recipe describes and let task 32 do its own job.

## 1. What you are building

`weekofyear(d)` returns the ISO-8601 week number, 1 to 53. Spark's reference:

```scala
def getWeekOfYear(days: Int): Int =
  LocalDate.ofEpochDay(days).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)
```

The rule ISO applies: week 1 of a year is the week containing that year's first
Thursday, weeks run Monday to Sunday, and the first days of January can
therefore belong to the *last* week of the previous year - while the last days
of December can belong to week 1 of the next. 2016-01-01 is week 53 of 2015;
2019-12-30 is week 1 of 2020. If your implementation returns 1 for the first
and 52 for the second, it is wrong in the usual way.

`LocalDate` is exact, so no intermediate below may overflow. None can.

## 2. The lowering

Three pieces, in order. `doy` here means the **January-based** day of year that
task 34 built, not `emitChrono`'s March-based one.

```
isodow = floorMod(d + 3, 7) + 1                 // Monday = 1 ... Sunday = 7
w      = (doy - isodow + 10) / 7                // the provisional week number
weekofyear = w < 1              ? weeksIn(year - 1)
           : w > weeksIn(year)  ? 1
           : w
```

`isodow` is Varka's existing `weekday` plus one - reuse `emitFloorMod7`, do not
write a second mod-7.

The provisional `w` divides a value in `[4, 375]`, always non-negative, so the
`/ 7` is a **plain magic multiply with no correction and no floorMod** - see
`SKILLS.md` for the bound: an exact magic exists for any dividend under 46341.
Do not reuse `emitFloorMod7` here; it is the full-range version and you do not
need it.

### 2.1 `weeksIn(y)`, the part that is easy to get wrong

A year has 53 ISO weeks iff it starts on a Thursday, or is a leap year starting
on a Wednesday:

```
p(y)       = (y' + y'/4 - y'/100 + y'/400) mod 7      // y' = y + 13200
weeksIn(y) = 52 + ((p(y) == 4 || p(y - 1) == 3) ? 1 : 0)
```

The bias of 13200 is the one `PLAN_TASK_34.md` section 2.1 introduces: a
multiple of 400 so the leap structure is unchanged, and large enough that
`y - 1` stays non-negative at the bottom of `VarkaChrono`'s covered range,
which is the reason it is 13200 and not 12800.

**Do not use `M=167773` (`k=24`/`26`) for the `/100`/`/400` divisions below.**
An earlier draft of `PLAN_TASK_34.md` used exactly these constants for the
same divisors over the same biased-year range and they are wrong: they are
exact only under unbounded-precision arithmetic and overflow the
`v * M < 2^31` no-overflow bound every magic multiply in `VarkaChrono` must
respect - at the top of the range (biased year 46334), `46334 * 167773` is
over three and a half times past `2^31`. `PLAN_TASK_34.md` section 7's
Outcome records finding this the hard way, with a corrected round-down magic
(`M=41943` at `k=22` for `/100`, `k=24` for `/400`) that `VarkaChrono`
actually ships. Use those corrected constants here too - they cover the same
range with the same bias, so they carry over directly - but note one
difference from `emitLeapFlag`'s use of them: `emitLeapFlag` only needs a
**boolean** ("is the remainder 0 or the divisor"), which a round-down magic
answers directly. `p(y)` here needs the actual **quotient** `y'/100`/`y'/400`
added into a sum, and a round-down magic can undershoot the true quotient by
one - so this table's divisions need an explicit correction step
(`emitCarry`'s round-down-plus-one-correction idiom) before the quotient is
used, which the constants alone do not provide. Re-verify section 2's whole
claim ("checked against `java.time`... zero mismatches") with a simulation of
true 32-bit truncating multiplication once the correction is in place, the
way `PLAN_TASK_34.md`'s fix was - a plain-Python check without overflow
truncation is what let the wrong constants through the first time:

| divisor | M | k |
|---|---|---|
| 4 | shift by 2 | - |
| 100 | 41943 | 22 |
| 400 | 41943 | 24 |
| 7 (the outer mod) | the standard mod-7 magic, dividend is small and non-negative | - |

`weeksIn` is needed for **two** years - `year` and `year - 1` - so emit it as a
helper called twice rather than twice inline. That is about 15 ops each, and it
is why this task is roughly twice the size of the other three.

**Verified, not assumed**: the whole of section 2 was checked against
`java.time`'s `IsoFields.WEEK_OF_WEEK_BASED_YEAR` over all 3,652,059 days of
`0001-01-01..9999-12-31` during planning - zero mismatches, including every
year boundary in both directions.

Expected size: about 60 ops on top of the decomposition, which makes this the
widest node in the family by some margin. Give it a `CHRONO_WEIGHT` of its own
if the existing one looks wrong for it, and say so in the pull request.

## 3. The edits

Mechanics per `PLAN_TASK_33.md` section 3. Specifics:

* **IR**: `WeekOfYear(VarkaVectorIR days)`, rendering as `(weekOfYear <days>)`.
* **Emitter**: a chrono node - `isChrono`, the four routine cases, and a
  `case WeekOfYear n -> {...}` in `emitChrono`'s tail switch. Add
  `emitWeeksInYear` as a private helper beside `emitLeapFlag`.
* **Compiler**: `case WeekOfYear(child) => ... .map(new IRWeekOfYear(_))`.

## 4. The tests

This is the task where the test matters more than the code. In addition to the
usual four:

1. `evalValue`'s oracle must be
   `LocalDate.ofEpochDay(v.toLong).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)` -
   never your own formula, and never a hand-written ISO rule.
2. **A dense sweep, not a boundary list.** Every day from 1990-12-20 to
   2030-01-10 through `checkMatrix`, which crosses forty year boundaries in
   both directions and costs a second. The known-hard dates -
   2015-12-28 (week 53), 2016-01-01 (week 53 of 2015), 2019-12-30 (week 1 of
   2020), 2021-01-01 (week 53 of 2020), 2020-12-31 (week 53) - are all inside
   that span, so listing them individually is belt and braces rather than the
   test itself. Add them anyway; they document the rule.
3. The 53-week years specifically: 2004, 2009, 2015, 2020, 2026 start on a
   Thursday or are leap years starting on a Wednesday. Check 31 December of
   each returns 53.
4. The ends of `VarkaChrono`'s covered range, where `weeksIn(year - 1)` is
   evaluated at the very bottom - the case the 13200 bias exists for.

## 5. What to run, and what must pass

Task 33's section 4, unchanged.

## 6. Explicitly out of task 37

* **`extract(WEEK from d)`** desugars to this node; covered for free.
* **`yearofweek` / the ISO week-based year** (`DateTimeUtils.getWeekBasedYear`).
  It is the same machinery with a different tail and would be a reasonable
  follow-up, but it is not this task and the corpus asks for neither.
* **Any non-ISO week numbering.** Spark has only the ISO one here.

## 7. Outcome

Filled in when the work lands. For this task especially, record which parts of
section 2 were unclear - it is the one recipe here that asks the agent to
implement a rule rather than transcribe a formula.
