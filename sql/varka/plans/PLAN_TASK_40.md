# Task 40: days-from-civil, and `date + INTERVAL n MONTH / YEAR`

A recipe for a cheap agent, in the shape task 33 established. Read
`PLAN_TASK_33.md` section 3 for the mechanics of adding a node type.

**Depends on task 26** for the decomposition. It does **not** depend on tasks
28, 29 or 30: a year-month interval is physically a month count, so all of this
is int32.

The headline deliverable is not the expression. It is **days-from-civil**, the
inverse of task 26's decomposition, which four separate expressions want and
none of them has. The interval arithmetic is what makes it concrete and
testable.

If you find yourself making a design decision, stop and say so in the pull
request instead of choosing.

## 1. What you are building

`d + INTERVAL '1' YEAR`, `d - INTERVAL '3' MONTH`, and `add_months(d, n)` -
three spellings of one node. `DateAddYMInterval` and `AddMonths` both extend
`AddMonthsBase`, whose semantics are:

```scala
def dateAddMonths(days: Int, months: Int): Int =
  localDateToDays(daysToLocalDate(days).plusMonths(months))
```

`LocalDate.plusMonths` does month arithmetic and then **clamps the day to the
new month's length**: 31 January plus one month is 28 or 29 February, not 3
March. That clamp is why no shortcut exists - you cannot know the day of month
without decomposing.

`LocalDate` is exact and never wraps, so the ordinary rule applies: **no
intermediate may overflow.**

Two spellings you get for free and must not write arms for:

* `d - INTERVAL n MONTH` arrives as `DatetimeSub(l, r, DateAddYMInterval(l,
  UnaryMinus(r)))`. `DatetimeSub` is a `RuntimeReplaceable`, and the compiler
  already unwraps those, so the negation is handled before you see it.
* `add_months(d, n)` is `AddMonths`, the same base class and the same lowering.

**Scope**: the month count must be a foldable literal. A year-month interval
*column* is int32 and would work lane-wise, but section 2.2 explains why its
unbounded value breaks the division; decline it.

## 2. The lowering

Three parts. Part 2.3 is the reusable one and the reason for the task.

### 2.1 Decompose

Task 26's `emitChrono`, giving `year`, `month` (1-12) and `dom`.

### 2.2 The month arithmetic, and the trap

**Do not build a total month count.** The natural formulation - `total = year *
12 + (month - 1) + months`, then `floorDiv`/`floorMod` by 12 - puts the
dividend near 400,000. That is far past the ~46341 bound an exact magic
multiply needs (`SKILLS.md`), and past the ~160,000 that round-down plus one
correction reaches, so it would need several carries. The planning pass wrote
it that way first and had to rewrite it.

Keep the dividend small by not folding the year in:

```
k  = (month - 1) + months + 12 * 2048        // small, and non-negative
q  = (k * 43691) >>> 19                      // k / 12, exact: e = 4
nm = k - q * 12                              // 0..11, the new month, March-free
ny = year + q - 2048                         // the new year
```

`M = 43691, k = 19` is exact for dividends up to 131072 and keeps `M * k`
inside `2^31` for dividends up to 49151. With the bias of `12 * 2048 = 24576`
that leaves room for a literal of about `+-24500` months, so **the compiler
must reject a larger literal** - about +-2000 years, which nobody writes, but
decline it rather than compute it wrongly.

### 2.3 Recompose: days-from-civil

Hinnant's `days_from_civil`, the exact inverse of task 26's forward direction,
and the pleasant surprise of this task: **every division in it is an exact
magic multiply with no correction step**, because every dividend is small. Task
26's forward direction needed two round-down magics with carries; this needs
none.

```
yy  = ny - (nm + 1 <= 2 ? 1 : 0)             // March-based year
b   = yy + 13200                             // the same bias tasks 34-37 use
era = (b * 167773) >>> 26                    // b / 400
yoe = b - era * 400                          // 0..399
mp  = (nm + 1) + ((nm + 1) <= 2 ? 9 : -3)    // March-based month, 0..11
doy = (((153 * mp + 2) * 838861) >>> 22) + day - 1
doe = yoe * 365 + (yoe >>> 2) - ((yoe * 167773) >>> 24) + doy
out = (era - 33) * 146097 + doe - 719468
```

| division | M | k | note |
|---|---|---|---|
| `/ 400` | 167773 | 26 | exact to 199728 |
| `/ 100` | 167773 | 24 | exact to 199728 |
| `/ 4` | - | - | a shift; the operand is non-negative |
| `/ 5` | 838861 | 22 | the same constant the day tail already uses |

The day passed in is the **clamped** one: `min(dom, length(ny, nm))`, where
`length` is the twelve-entry month table plus the leap flag for February. Reuse
`emitLeapFlag` from task 34 if it has landed; if not, write it as that recipe's
section 2.1 specifies.

**Verified, not assumed.** `plans/verify_days_from_civil.py`, committed beside
this file, checks both halves: the round trip (decompose then recompose) is the
identity over all 3,652,059 days from year 1 to year 9999, and `add_months`
matches `LocalDate.plusMonths` on 1,083,571 sampled cases - every 37th day of
that range against eleven offsets from -1200 to +1200 months. Zero mismatches
on both. Run it before you start; it is the clearest statement of what you are
building.

Expected size: about 90 vector ops - roughly 45 to decompose, 8 for the month
arithmetic, 10 for the clamp, 25 to recompose. About twice `year`.

## 3. The edits

Mechanics per `PLAN_TASK_33.md` section 3. Specifics:

* **IR**: `AddMonths(VarkaVectorIR days, VarkaVectorIR months)` - a binary node
  with the month count in a `LiteralSlot`, exactly like `AddDays`. Render as
  `(addMonths <days> <months>)`. Its output is a **date**.
* **Emitter**: a chrono node - `isChrono`, the four routine cases, and an
  `emitValue` arm. Validity is `andRef` over both children (the literal's word
  is all-true, so this is the same as aliasing the date child; use `andRef`
  anyway, as `DateDiff` does). Put days-from-civil in its own private helper,
  `emitDaysFromCivil`, taking the year, month and day slots - **not** inlined
  into the arm, because three later expressions want to call it.
* **Compiler**: one arm for `DateAddYMInterval` and one for `AddMonths`, both
  folding the month count to a literal, both declining a non-foldable count and
  a literal outside the bound from section 2.2, each with its own decline
  message.

## 4. The tests

1. `evalValue` gains an `AddMonths` arm whose oracle is
   `DateTimeUtils.dateAddMonths(v, m)` - the definition, not your formula.
2. **The clamp, which is where wrong implementations fail**: 31 January plus 1
   month, 31 March minus 1 month, 29 February plus 12 months (to 28 February),
   28 February plus 12 months in a leap year, 31 December plus 1 month, and the
   same set with negative offsets.
3. **The month boundary in both directions**: offsets of 0, +-1, +-11, +-12,
   +-13, +-1200, so `k` crosses a multiple of 12 both ways.
4. **The round trip as its own test**: for a spread of dates, decompose and
   recompose must give the original day back. That tests
   `emitDaysFromCivil` independently of the month arithmetic, which is worth
   doing because three later expressions will depend on it alone.
5. The two pinned fixtures, extended and re-pinned; one compiler test per
   accepted spelling plus the two declines; a differential over
   `d + INTERVAL '1' YEAR`, `d - INTERVAL '3' MONTH` and `add_months(d, 5)`
   with nulls, plus a `date_add(add_months(d, 1), 1)` case proving the
   `DateType` output feeds further date arithmetic.

Then task 33's section 4 command block, unchanged, at both widths.

## 5. Explicitly out of task 40

* **A year-month interval column.** Section 2.2 - the bound on `k` is what
  makes the division exact, and a column has no bound. It would need a guard
  and a decline path of its own.
* **The `INTERVAL n YEAR` fast path.** When the literal is a multiple of 12 the
  month index does not change, so the `/ 12` disappears entirely and the clamp
  reduces to the single 29 February case - worth perhaps 10 of the 90 ops. It
  is a real optimization and a natural follow-up, but two code paths mean two
  sets of tests, and this task is meant to ship one correct path. Mention in
  the pull request that you left it; do not add it.
* **`months_between`, `make_date`, `date_trunc('QUARTER')`.** All three want
  `emitDaysFromCivil`, which is why it is a helper rather than inline code -
  but each is its own task with its own argument.
* **Timestamp interval arithmetic** - int64 lanes and the timezone question.

## 6. Outcome

Filled in when the work lands, including which steps of this recipe misled you.
Say in particular whether section 2.2's trap was clear enough before you hit
it, since the planning pass hit it too.
