# Task 35: `trunc(date, 'YEAR' | 'MONTH' | 'QUARTER')`

One of four small vocabulary tasks (34-37) written as recipes for a cheap
agent. Read `PLAN_TASK_33.md` section 3 for the mechanics of adding a node
type, and `PLAN_TASK_34.md` section 2.1 for the leap flag this task needs.

**Depends on tasks 26 and 34** - 26 for `emitChrono`, 34 for `emitLeapFlag`
and for the January-based day-of-year the YEAR and QUARTER forms are built on.
If 34 has not landed, do not reimplement its pieces; wait, or say so.

**Task 32 may move the plumbing; it will not move the arithmetic.** A separate
task is measuring whether one shared decomposition can feed several fields,
instead of each node carrying its own copy of it. If that goes ahead, the tails
will read `doy` and `dom` from somewhere else - and every formula in section 2
will be unchanged, because what moves is where the intermediates live, not what
they are. So do not restructure `emitChrono`, do not try to share anything with
the other tails, and do not read the duplicated decomposition as a bug to fix.
Write the tail this recipe describes and let task 32 do its own job.

## 1. What you are building

`trunc(d, fmt)` rounds a date down to the start of a period and returns a
**date**, not a number - the first output in this family whose type is
`DateType`, which matters because it can then feed further date arithmetic in
the same fused chain.

Spark's reference is `DateTimeUtils.truncDate`:

```scala
case TRUNC_TO_MONTH   => days - getDayOfMonth(days) + 1
case TRUNC_TO_YEAR    => days - getDayInYear(days) + 1
case TRUNC_TO_QUARTER => first day of the quarter's first month
```

All three go through `LocalDate`, which is exact and never wraps, so the
ordinary rule applies: **no intermediate may overflow.** None below can.

**Scope**: `YEAR`, `MONTH` and `QUARTER` only, and only when the format
argument is a foldable string literal. `WEEK`, and every timestamp-level
format, decline. `YEAR` also has the spellings `YYYY` and `YY`, `MONTH` has
`MM` and `MON`, `QUARTER` has `QTR`; accept whatever
`DateTimeUtils.parseTruncLevel` accepts for those three levels and decline the
rest.

## 2. The lowerings

`emitChrono` already has `dom` (day of month) and, after task 34, the
January-based `dayofyear` and the leap flag `L`. Each format is a separate
lowering chosen at compile time - there is no runtime branch on the format.

```
MONTH:    d - dom + 1
YEAR:     d - dayofyear + 1
QUARTER:  d - dayofyear + qstart, where qstart is selected by quarter:
              Q1 -> 1        Q2 -> 91 + L      Q3 -> 182 + L     Q4 -> 274 + L
```

The quarter constants are the January-based day-of-year of 1 January, 1 April,
1 July and 1 October. `quarter` is already computed by `emitChrono`; select
with three compares and three masked adds over a starting value of 1, or with
a blend chain - either is fine, they are the same op count.

**Verified, not assumed**: all three were checked against `java.time` over all
3,652,059 days of `0001-01-01..9999-12-31` during planning - zero mismatches.

`MONTH` is much the cheapest: it needs `dom` and nothing else, so it does not
need task 34 at all. If you want to land something early, land `MONTH` first
as its own commit.

## 3. The edits

Mechanics per `PLAN_TASK_33.md` section 3. What is specific here:

* **One IR node, not three.** `TruncDate(VarkaVectorIR days, int level)` with
  the level as a **record field**, not a literal slot - and this is the one
  place in these four tasks where a constant belongs in the IR rather than in
  `scalarArgs`. The reason: the level chooses which *code* is emitted, not
  which value is used, so two levels are two shapes and must hash differently.
  Task 33's rule ("a chain's identity is its shape, not its constants") points
  the same way here, in the opposite direction, for the same reason. Render as
  `(trunc:<LEVEL> <days>)` in both `canonical` and `canonicalShallow`, with the
  level spelled out so the pinned renderings distinguish them.
* **Output type.** `TruncDate.dataType` is `DateType`, so the compiler's
  `outputTypes` picks that up automatically - but check it in the compiler
  test, because every other node this family added is `IntegerType` and a
  wrong type here means the evaluator allocates the wrong Arrow vector.
* **The emitter**: `TruncDate` is a chrono node - add it to `isChrono`, give it
  the four routine cases, and add a `case TruncDate n -> switch (n.level())
  {...}` inside `emitChrono`'s tail switch.
* **The compiler arm** must evaluate the foldable format, map it through
  `DateTimeUtils.parseTruncLevel`, accept exactly the three date-level values,
  and `sink.note` a reason for everything else - a non-foldable format, an
  unrecognized string, and the levels this task does not cover, each with its
  own message.

## 4. The tests

1. `evalValue` gains a `TruncDate` arm whose oracle calls
   `DateTimeUtils.truncDate(v, level)` directly - the definition, not your
   formula.
2. A boundary test per level: 1 January, 31 December, 28/29 February, 1 March,
   the first and last day of each quarter, in a leap year, a common year, a
   century non-leap year (1900) and a century leap year (2000), plus the ends
   of `VarkaChrono`'s covered range.
3. The two pinned fixtures, extended and re-pinned. Put **one** level in the
   fixture, not three - the fixture exists to pin renderings, and one
   `(trunc:YEAR ...)` proves the rendering shape.
4. A compiler test per accepted level plus one decline test per rejected shape.
5. A differential test with all three levels in one query, and one asserting
   that `trunc(d, 'WEEK')` does **not** fuse.

## 5. What to run, and what must pass

Task 33's section 4, unchanged, plus: the differential must show the
`DateType` output surviving a second operation - something like
`date_add(trunc(d, 'MONTH'), 5)` - because a wrong output type will pass a
single-column test and fail here.

## 6. Explicitly out of task 35

* **`trunc(d, 'WEEK')`**, which Spark defines as
  `getNextDateForDayOfWeek(days - 7, MONDAY)` - i.e. exactly task 33's
  machinery over `d - 7`. It is a genuinely easy follow-up **once task 33 has
  landed**, and it is left out here only to keep the two tasks independent.
  Say in the pull request if you want it; do not add it unasked.
* **`date_trunc`** at timestamp level - it needs int64 lanes, task 29.
* **Every other `trunc` level.**

## 7. Outcome

Filled in when the work lands, including which steps of this recipe misled you.
