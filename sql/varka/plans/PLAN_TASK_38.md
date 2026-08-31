# Task 38: a day offset that is a column, not a literal

Another recipe for a cheap agent, in the shape task 33 established - but not a
new expression. Every guard this task moves already exists to enforce
milestone 1's scope ("foldable integer day offsets"), and the lane math it
needs is already emitted. The work is opening a door, not building a room.

Read `PLAN_TASK_33.md` section 3 for the general mechanics. This task depends
on nothing and can start immediately.

If you find yourself making a design decision, stop and say so in the pull
request instead of choosing.

## 1. What you are building

`date_add(d, n)` and `d + n` where `n` is an **`IntegerType` column** rather
than a literal, and the `date_sub`/`d - n` counterparts. Both spellings reach
the compiler as `DateAdd(date, days)` - `d + n` through
`BinaryArithmeticWithDatetimeResolver:104` - so there is one node to teach, not
two.

Today all four of these decline, and the reason worth understanding before you
start is that **Varka cannot read a non-date column at all**. Foldability is
the visible guard; the column type is the real one. That is why this task
touches the evaluator as well as the compiler.

`d + INTERVAL '3' DAY` already fuses and is not part of this task: the analyzer
rewrites it to `DateAdd` with a folded literal, which works today. An interval
*column* is out - see section 6.

## 2. The four guards

Each of these must move, and each declines for a different reason. Find all
four before changing any of them:

| # | where | what it does today |
|---|---|---|
| 1 | `VarkaExpressionCompiler`, the leaf arm | accepts `BoundReference` only when `dataType == DateType` |
| 2 | `VarkaExpressionCompiler.foldOffset` | requires `Literal(value: Number, _)`, notes "day offset is not a foldable literal" |
| 3 | `VarkaLoopEmitter.analyze`, `requireLiteralOffset` | **throws** unless the offset is a `LiteralSlot` |
| 4 | `VarkaKernelEvaluator.isArrowBacked` | requires every referenced column to be an Arrow `DateDayVector` |

Guard 3 throws rather than declines, which tells you what it is: a scope guard,
not a capability guard. `emitValue`'s `AddDays` arm is already
`emitValue(days); emitValue(offset); add(Vector)` - vector-vector lane math
that does not care where the offset came from.

## 3. The trap: validity

**This is the part of the task that can produce wrong answers rather than a
decline, and it is the reason this recipe exists.**

`VarkaLoopEmitter.planWordRef` reads:

```java
case AddDays n -> s.wordRef.get(n.days());
case SubDays n -> s.wordRef.get(n.days());
```

The result's validity is aliased to the *date* child alone. That is correct
today because the offset is always a literal and a literal is always valid. The
moment the offset can be a nullable column it is **wrong**: `date + NULL` must
be NULL, and this would produce a non-null row holding whatever the null lane
contained.

The fix is two characters short of trivial, and it is deliberately the same
shape `DateDiff` already uses:

```java
case AddDays n -> andRef(s.wordRef.get(n.days()), s.wordRef.get(n.offset()));
case SubDays n -> andRef(s.wordRef.get(n.days()), s.wordRef.get(n.offset()));
```

**This is a no-op for every shape that exists today.** A `LiteralSlot`'s word
reference is `WORD_ALL_TRUE`, and `andRef(a, WORD_ALL_TRUE)` returns `a` by its
first two lines. So the emitted bytes for a literal offset are unchanged, which
is what makes section 5's "nothing existing moves" acceptance criterion true.
Make this change first, before anything else, and confirm the suites are still
green with it alone - that isolates it from everything after.

## 4. The edits

### 4.1 The emitter

* `planWordRef`: section 3.
* `Analysis.analyze`: delete the two `requireLiteralOffset` calls, and delete
  the method if nothing else calls it. Keep the `analyzeOp(node, false,
  n.days(), n.offset())` calls exactly as they are - they already register the
  offset as a child, which is what makes column tracking and CSE work.

### 4.2 The compiler

* The leaf arm becomes `case br: BoundReference if br.dataType == DateType ||
  br.dataType == IntegerType`. Nothing downstream needs to know which it was:
  the IR has one lane type, the analyzer has already type-checked the tree, and
  output types are tracked per entry rather than per input.
* `foldOffset` stops being the only path. `DateAdd`/`DateSub` should try the
  folded literal first - keeping today's `LiteralSlot` shape, so existing plans
  and their cached kernels are untouched - and fall back to
  `compileNode(days, ...)` when it is not foldable.
* **Decline `ShortType` and `ByteType` offsets.** `DateAdd.inputTypes` is
  `Seq(DateType, TypeCollection(IntegerType, ShortType, ByteType))`, so a short
  or byte column arrives with **no cast** and its Arrow vector is 2 or 1 bytes
  wide - which an int32 lane load would read as garbage. This is the second way
  this task can produce wrong answers rather than a decline. The leaf arm above
  gets this right by naming `IntegerType` exactly; do not broaden it to
  "any integral type".

### 4.3 The evaluator

* `isArrowBacked`: accept an Arrow `IntVector` beside `DateDayVector`, with the
  same `getValueCount() == input.numRows()` check.
* `extractMorsel(ddv: DateDayVector, len: Int)` takes the concrete type; widen
  it to `BaseFixedWidthVector`. Both vectors are four bytes wide with the same
  buffer layout, so the body does not change. Both types are already imported.
* Check the call site at `:504`, which casts to `DateDayVector`.

### 4.4 Docs

`docs/sql-varka.md` says the surface is over date columns "and foldable integer
day offsets". Both halves of that sentence are now wrong; fix it, and say that
an integer *column* may be an offset while short and byte columns decline.

## 5. The tests, and what must pass

1. **Emitter suite**: a matrix case with a two-column shape - `AddDays(col0,
   col1)` - over every null pattern, including nulls in the offset column and
   nulls in both. The reference evaluator already handles this shape correctly
   (`for (d <- ...; o <- ...) yield d + o` is null-propagating), so it is the
   emitted side being tested. **A case with a null offset and a non-null date
   is the one that fails if section 3 was skipped.**
2. **Compiler suite**: `date_add(d, i)` compiles to `AddDays(ColumnRef, ColumnRef)`
   with two input ordinals; `date_add(d, 3)` still compiles to a `LiteralSlot`
   as it does today; a `ShortType` offset column declines.
3. **Differential**: `cacheDates` already gives you a table with a date column
   `d` and an int column `i`, so `SELECT date_add(d, i), d + i, date_sub(d, i)`
   is a one-line test. `i` there is **not nullable**, so add a second view with
   a nullable int column - the null-offset case is the whole point.
4. **A decline test**: an interval column still does not fuse (section 6).

Then task 33's section 4 command block, unchanged.

**Acceptance is unusually strong for this task, and you should hold yourself to
it**: no new IR node type, so **neither pinned value moves**, and no existing
emitted shape changes, so **no committed benchmark number moves**. If a pinned
hash or a parity number moves, something in section 4 went further than it
should have - find out what before going on.

## 6. Explicitly out of task 38

* **An interval column.** `DayTimeIntervalType(DAY, DAY)` is physically *long*
  microseconds, so extracting days needs int64 lanes (task 29) and a division
  by 86400000000 on long lanes, where there is no multiply-high either. It
  needs its own range-narrowing argument and is not this task.
* **Short and byte offset columns** - section 4.2, and they must decline.
* **Reading an integer column anywhere else.** This task makes `IntegerType`
  loadable, which is a bigger door than it needs; do not open it wider by
  adding integer comparisons, integer arithmetic, or an integer output that
  did not already exist. Those are their own tasks with their own arguments.
* **`datediff` returning an integer column that another kernel reads** - that
  already works inside one chain and is unrelated.

## 7. Outcome

Filled in when the work lands, including which steps of this recipe misled you.
Say in particular whether section 3's trap was clear enough before you hit it,
because that is the step this recipe exists for.
