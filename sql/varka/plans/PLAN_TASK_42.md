# Task 42: `make_date(year, month, day)`

A recipe for a cheap agent, in the shape task 33 established. Read
`PLAN_TASK_33.md` section 3 for the mechanics of adding a node type.

**Depends on task 38** (an integer column has to be readable at all) and on
**task 40** (`emitDaysFromCivil`). It needs none of 28, 29 or 30 - everything
here is int32.

It is the first expression that reads **three** integer columns, and the first
whose result can be **null for a non-null input**. Both of those are why it is
worth writing down carefully.

If you find yourself making a design decision, stop and say so in the pull
request instead of choosing.

## 1. What you are building

`make_date(y, m, d)` builds a date from three integers. Spark's semantics:

```scala
if (failOnError) {
  DateTimeExpressionUtils.makeDateExact(year, month, day)   // throws
} else {
  try { localDateToDays(LocalDate.of(year, month, day)) }
  catch { case _: java.time.DateTimeException => null }
}
```

`LocalDate.of` rejects a month outside 1-12, a day outside 1 to the month's
length (28, 29, 30 or 31 as the case may be), and a year outside its own huge
range. `failOnError` is `SQLConf.get.ansiEnabled`, captured on the expression,
so it is **known at compile time**.

So there are two different behaviours for the same bad input, and the whole
task turns on keeping them apart.

## 2. Three outcomes, not two

This is the part to read twice. A lane can be in one of three states, and they
are not the same:

| state | example | what must happen |
|---|---|---|
| a null input | `make_date(NULL, 1, 1)` | null output, both modes - ordinary validity |
| a **valid** date | `make_date(2024, 2, 29)` | the date |
| an **invalid** date | `make_date(2024, 2, 30)` | **null** in non-ANSI, **throw** in ANSI |
| a year outside what this engine covers | `make_date(500000, 1, 1)` | **decline the batch**, in *both* modes |

The last two rows are the trap. An invalid date is a **semantic** result - SQL
says what it means, and the kernel must produce it. A year past what the
lowering's magic multiplies cover is an **engine limitation** - SQL has an
answer and this kernel cannot compute it, so the batch declines through task
26's status channel and the row engine answers it correctly. Confusing the two
gives wrong answers in one direction and spurious errors in the other.

In ANSI mode the invalid-date case *also* declines, because a lane cannot
throw: the row engine then raises the exception, at the right row, with the
right message, because it is the row engine. That is the same trick task 39
uses, and it means this task needs no error machinery of its own.

## 3. The lowering

`emitDaysFromCivil` from task 40 does the arithmetic. This task adds the
validity around it:

```
mp     = (m + 9) mod 12                    // March-based month; task 40 computes this
length = mp < 11 ? cum(mp + 1) - cum(mp) : 28 + L      // as task 36 does
okM    = m >= 1 && m <= 12
okD    = d >= 1 && d <= length
okY    = y >= YEAR_MIN && y <= YEAR_MAX    // the engine's own limit, section 3.1
valid  = okM && okD
out    = emitDaysFromCivil(y, m, d)        // computed unconditionally
```

`out` is garbage where `valid` is false, and that is fine: the kernel contract
says the data of a null output row is undefined. Do not branch, do not blend a
safe value in.

Then, by mode:

* **non-ANSI**: the output's validity word is the inputs' validity AND `valid`.
  This node therefore **computes its own word** rather than aliasing a child's
  - `Greatest` and `IfElse` already do that, so follow one of them in
  `planWordRef` and the word-emitting code.
* **ANSI**: the output's validity is just the inputs', and `!valid` (where the
  inputs are non-null) is ORed into the guard mask so the batch declines.

`!okY` is ORed into the guard mask in **both** modes.

`length` must be computed from the *given* month even when that month is out of
range, or `okD` reads a nonsense length. Clamping `m` into 1-12 before
computing `length` is the simplest way and is correct, because `okM` has
already recorded that the row is invalid.

### 3.1 The year limits

`emitDaysFromCivil`'s magic multiplies are exact only up to a bound; take the
limits from task 40's constants rather than inventing them, state them as named
constants in `VarkaChrono`, and **prove them in a test** rather than trusting
this file - task 40's own recipe gives the exactness bounds for each division.
If the bound turns out narrower than the range task 26 already covers, say so
in the pull request: that would be worth knowing for tasks 34-37 too.

## 4. The edits

Mechanics per `PLAN_TASK_33.md` section 3. Specifics:

* **IR**: `MakeDate(VarkaVectorIR year, VarkaVectorIR month, VarkaVectorIR day,
  boolean failOnError)` - three children plus a **shape-bearing** flag, the way
  task 35's `TruncDate` carries its level, because the flag chooses which code
  is emitted. Render as `(makeDate:<ANSI|NULL> <y> <m> <d>)`.
* **Emitter**: the four routine cases plus an `emitValue` arm; `planWordRef`
  returns "computes its own word" for the non-ANSI form and the AND of the
  three children for the ANSI form. Three children is a first for a value node
  - `IfElse` is the only other one - so copy its shape rather than a binary
  node's.
* **Compiler**: `case MakeDate(y, m, d, failOnError) =>` compiling all three
  children. Note that the children are `IntegerType` **columns or literals**;
  both must work, and a literal one goes through the same `compileNode` path.

## 5. The tests

1. `evalValue` gains a `MakeDate` arm whose oracle constructs `LocalDate.of`
   inside a `try` and yields `None` on `DateTimeException` - the definition,
   not your predicate.
2. **The validity matrix, which is the heart of the task**: valid dates; month
   0, 13 and negative; day 0, 32, and 29/30/31 in months that do not have them;
   29 February in a leap year (valid) and in a common year (invalid); nulls in
   each of the three inputs separately and together. Every one of these at both
   settings of `failOnError`.
3. **The two modes differ and must be tested apart**: in non-ANSI an invalid
   row is a null output and the batch still runs; in ANSI the batch declines
   and no output is published.
4. **The year limit declines in both modes**, and is not confused with an
   invalid date - a test with a year past the limit must decline, not null.
5. Differential: `SELECT make_date(y, m, d)` over a cached table with three
   int columns including nulls and invalid combinations, run under both
   `spark.sql.ansi.enabled` settings, with the ANSI run asserting the same
   exception as the row engine by running both rather than by naming a class.
6. The two pinned fixtures, extended and re-pinned.

Then task 33's section 4 command block, unchanged, at both widths.

## 6. Explicitly out of task 42

* **`make_timestamp`, `make_interval`, `make_dt_interval`** - int64 lanes and,
  for the timestamp forms, the timezone question.
* **`try_make_date`**, if it exists in this Spark version - it is a different
  expression with a third error behaviour, and one task should not carry three.
* **Widening the year range** beyond what task 40's magics support. Section 3.1
  says to record the limit, not to raise it.

## 7. Outcome

Filled in when the work lands, including which steps misled you. Say in
particular whether section 2's three-way distinction was clear before you hit
it, because that is the distinction this recipe exists to make.
