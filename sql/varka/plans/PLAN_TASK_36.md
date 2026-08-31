# Task 36: `last_day`

One of four small vocabulary tasks (34-37) written as recipes for a cheap
agent. Read `PLAN_TASK_33.md` section 3 for the mechanics of adding a node
type, and `PLAN_TASK_34.md` section 2.1 for the leap flag.

**Depends on task 26** for `emitChrono`, and on **task 34** only for
`emitLeapFlag`. If 34 has not landed you may write that helper here instead -
it is twelve lines - but write it in the same place and with the same
signature task 34 specifies, so the two do not collide.

**Task 32 may move the plumbing; it will not move the arithmetic.** A separate
task is measuring whether one shared decomposition can feed several fields,
instead of each node carrying its own copy of it. If that goes ahead, the tails
will read `doy` and `dom` from somewhere else - and every formula in section 2
will be unchanged, because what moves is where the intermediates live, not what
they are. So do not restructure `emitChrono`, do not try to share anything with
the other tails, and do not read the duplicated decomposition as a bug to fix.
Write the tail this recipe describes and let task 32 do its own job.

## 1. What you are building

`last_day(d)` returns the last date of the month `d` falls in.
`last_day(DATE '2024-02-05')` is `2024-02-29`.

Spark's reference is `DateTimeUtils.getLastDayOfMonth`:

```scala
def getLastDayOfMonth(days: Int): Int = {
  val localDate = daysToLocalDate(days)
  (days - localDate.getDayOfMonth) + localDate.lengthOfMonth()
}
```

`LocalDate` again, so exact, no wrapping, and no intermediate below may
overflow. The output is a **date**, not a number - `LastDay.dataType` is
`DateType`, so check the output type in the compiler test the way task 35 does.

## 2. The lowering

`emitChrono` already has the March-based month index `mp` (0 = March, 11 =
February) and the day of month `dom`. The month's length comes from the same
linear form the day-of-month tail already uses:

```
cum(m)  = (153 * m + 2) / 5                    // magic: M = 838861, k = 22
length  = mp < 11 ? cum(mp + 1) - cum(mp) : 28 + L
last_day = d + length - dom
```

`cum(m)` is the count of days from 1 March to the first of March-month `m`, and
the difference of two consecutive values is the length of month `mp`. It is
exact for every `mp` in `[0, 10]` and **wrong at 11**, because the linear form
only models March through January; February is the year's last month and
carries the leap day, so it is the special case. That is why the comparison is
there, and it is not optional.

`L` is the leap flag of the **reported** year, which is the right year for
February precisely because a February date's reported year is already the
following one - `emitChrono` adds that 1 when `mp >= 10`.

**Verified, not assumed**: this formula was checked against `java.time` over
all 3,652,059 days of `0001-01-01..9999-12-31` during planning - zero
mismatches, including every 28/29 February and every century boundary.

Expected size: about a dozen ops on top of the decomposition, most of them the
leap flag.

## 3. The edits

Mechanics per `PLAN_TASK_33.md` section 3. Specifics:

* **IR**: `LastDay(VarkaVectorIR days)`, rendering as `(lastDay <days>)`. A
  plain unary chrono node - copy `Quarter`.
* **Emitter**: add to `isChrono`, the four routine cases, and a
  `case LastDay n -> {...}` in `emitChrono`'s tail switch emitting section 2.
* **Compiler**: `case LastDay(child) => ... .map(new IRLastDay(_))` beside the
  other calendar arms. Note that Spark's expression is
  `LastDay(startDate)` - one child, no format argument.

## 4. The tests

1. `evalValue` gains a `LastDay` arm whose oracle is
   `DateTimeUtils.getLastDayOfMonth(v)` - the definition, not your formula.
2. A boundary test over: every month of a leap year and of a common year (so
   all twelve lengths are exercised twice), 28 and 29 February, 1 and 31
   March, a century non-leap year (1900-02-15) and a century leap year
   (2000-02-15), and the ends of `VarkaChrono`'s covered range.
3. The two pinned fixtures, extended and re-pinned.
4. One compiler test, asserting `outputTypes === Seq(DateType)`.
5. A differential test, including a `date_add(last_day(d), 1)` case so the
   `DateType` output is proved to feed further date arithmetic.

## 5. What to run, and what must pass

Task 33's section 4, unchanged.

## 6. Explicitly out of task 36

* **`add_months`, `months_between`, `make_date`.** All three need the *inverse*
  of the decomposition - days-from-civil - which does not exist yet. Its
  divisions are by 4 and 100 on small dividends, so it is expressible, but it
  is its own task and not a tail on `emitChrono`.
* **`day_of_month`** - shipped by task 26.

## 7. Outcome

Filled in when the work lands, including which steps of this recipe misled you.
