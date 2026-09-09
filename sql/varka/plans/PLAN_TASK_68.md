# Task 68: year-month interval algebra

## 1. Where this came from

`PLAN_MILESTONE_4.md` row 68 and section 2.33, which scoped the expressions that
*produce or transform* an interval once task 63's int32 arithmetic and task 67's
type admission were in. Both now are: 63 merged with the arithmetic nodes and
their evaluation modes, and 67 admitted `YearMonthIntervalType` end to end as a
value leaf, a literal, an `add_months` month count and two relabel casts.

Task 67 also left two shapes on this task's doorstep and said why
(`PLAN_TASK_67.md` 2.1): `d - ym_col` and the `YEAR`-unit casts, neither blocked
on arithmetic, both blocked on the emitter's month-count position.

## 2. The admission check, done

Read against master (`3934a8764b8`), with the ranges computed rather than
asserted. The check divides the row's list cleanly in two, and the division is
not the one section 2.33 drew.

### 2.1 What Spark actually computes

| expression | definition | mode |
|---|---|---|
| `make_ym_interval(y, m)` | `toIntExact(addExact(m, multiplyExact(y, 12)))` | always checked |
| `ym + ym`, `ym - ym` | `IntervalMathUtils.addExact` / `subtractExact` | always checked |
| `-ym` | `IntervalMathUtils.negateExact` | always checked |
| `abs(ym)` | `IntegerExactNumeric`'s abs | always checked |
| `ym * k` | `Math.multiplyExact(months, k)` | always checked |
| `ym / k` | `IntMath.divide(months, k, HALF_UP)` | rounding, not truncation |
| `extract(YEAR FROM ym)` | `months / 12`, Java division | truncating |
| `extract(MONTH FROM ym)` | `(months % 12).toByte` | sign follows dividend |

"Always checked" is the useful half: none of the first five has a `LEGACY`
wrapping form, so each is task 63's `FAIL` mode unconditionally, and the
compile-time bound is the only thing that can take the check off.

### 2.2 The two divisions are a different task from the rest, and the reason is
### the absence task 26 was built around

`extract` and `ym / k` need division by a constant. `VectorOperators` has no
multiply-high on any lane type - the absence `VarkaChrono`'s header names, that
`PLAN_MILESTONE_5.md` 2.7 re-checked during task 32 and found is not temporary
(no `MUL_HIGH` in JDK 25 or openjdk master; JDK-8219881 Open at P4 since 2019) -
so a full-range Granlund-Montgomery magic is not expressible on int lanes. What
is expressible is a round-up magic whose product must stay inside a signed int
lane, and that bounds the dividend hard.

Computed, not assumed - for each divisor, the best `M`/`k` and the largest
dividend the pair is exact over:

| divisor | M | k | exact for |
|---|---|---|---|
| `/12` | 43691 | 19 | 0..49,151 |
| `/3` | 43691 | 17 | 0..49,151 |
| `/5` | 26215 | 17 | 0..43,690 |
| `/7` | 18725 | 17 | 0..43,690 |
| `/100` | 5243 | 19 | 0..43,690 |
| `/2`, `/4` | 1 | 1, 2 | 0..2,147,483,647 |

The `/12` row is `MONTH_ARITH_M` exactly - the emitter already has this magic,
and task 60 already carries the runtime guard that keeps its dividend inside the
range. So `extract` does not need a new magic; it needs the *same* magic over a
dividend the type does not bound. A year-month interval is a full int32 month
count, and 49,151 months is about 4,096 years: the magic covers roughly one
forty-thousandth of the type's range.

Powers of two are the exception and are exact over all of int32, which matters
for `ym / k`: a literal `k` that is a power of two needs no magic at all.

### 2.3 Truncation is not floor, and the difference is only on negatives

The magic computes a floor. Java's `/` truncates toward zero, and `extract`
is Java's `/`:

| months | Java `/12` | floor | Java `% 12` |
|---|---|---|---|
| -13 | -1 | -2 | -1 |
| -12 | -1 | -1 | 0 |
| -1 | 0 | -1 | -1 |
| 13 | 1 | 1 | 1 |

So an `extract` built on the existing magic needs a correction on the negative
side, and `extract(MONTH ...)` inherits it, being `months - 12 * q` for
whichever
`q` the year extract produced. Task 60's use of this magic does not need the
correction, because `add_months` wants the floor and biases its dividend
non-negative to get it. That bias is why the same constant serves two callers
that disagree about rounding, and it is worth saying where the correction goes
in
rather than discovering it from a differential.

### 2.4 `ym / k` rounds HALF_UP, which a magic does not give

`IntMath.divide(months, k, HALF_UP)` rounds to the nearest, ties away from zero:
`-5 / 2` is `-3`, not `-2`. A quotient from a magic is floor; HALF_UP needs the
remainder as well - `q` adjusted by one where `2 * |r| >= |k|`, in the direction
of the quotient's sign. That is three more lanewise ops on top of a magic that
is
already range-bounded, and the bound is per-`k` (2.2's table), so the guard's
range would have to be computed per literal rather than being one constant.

### 2.5 What task 67 left, and why it is not about arithmetic

`d - ym_col` resolves to `DateAddYMInterval(d, UnaryMinus(ym))` and the
`YEAR`-unit casts to a `12 *` over the operand. Both want an `IntArith` or
`IntNeg` in `add_months`' month-count position, and the emitter's
`requireOffsetShape` admits a literal slot or a column there and nothing else -
so admitting either in the compiler alone would be a ghost fallback.

`PLAN_TASK_67.md` 2.1 also recorded, and this task should act on it, that the
stated reason for that strictness - "a weekday and a month count carry runtime
bounds a derived value cannot declare" - is true of `next_day`'s weekday and
false of the month count. A column-count `AddMonths` is in `selfGuarding` and is
guarded at run time by a lanewise check on the count's *value*, which does not
care what produced it; a derived count is covered by that same guard. The two
positions share a check and do not share the reason, and splitting them is this
task's, not a later one's.

### 2.6 What the check would have rejected

That `extract` is "a literal-divisor magic multiply" and therefore small (it is
the existing magic over a dividend forty thousand times its exact range); that
the magic's rounding matches Java's (it is floor, `extract` truncates); that
`ym / k` is the same shape as `extract` (it rounds HALF_UP and its exact range
depends on `k`); and that the shapes task 67 deferred were waiting on task 63
(they are waiting on an emitter check this task owns).

## 3. The design

### 3.1 The split the check found, and what this task takes

Section 2.33 lists eight things as one task. 2.1 to 2.4 divide them by what they
need from the emitter, and the division is sharp:

**Group A - no new machinery.** `make_ym_interval`, `ym + ym`, `ym - ym`, `-ym`,
`abs(ym)`, `ym * k`. Every one is task 63's `IntArith`/`IntNeg` in `FAIL` mode
with an interval-typed output, over operands task 67 already admits. The
compile-time bound applies unchanged, so `make_ym_interval(year(d), 3)` needs no
check while `ym1 + ym2` over two columns does. No IR node, no emitted byte that
task 63 did not already emit.

**Group B - a bounded division and a guard.** `extract(YEAR|MONTH FROM ym)` and
`ym / k`. Both need a magic whose exact range is a fraction of the type's, both
need a rounding correction the existing magic does not carry, and `ym / k` needs
its range computed per literal.

**Group C - one emitter check.** `d - ym_col` and the `YEAR`-unit casts, blocked
on `requireOffsetShape` (2.5).

**This task takes A and C.** B is its own task, and 3.4 says why that is not
merely a size argument.

### 3.2 Group A, as arms

The compiler's arithmetic arms are gated on `dataType == IntegerType`
(`case a: Add if a.dataType == IntegerType`), which an interval-typed `Add` does
not satisfy. Widening that gate is wrong - int arithmetic is an int-typed
concept, and task 67 declined to widen `UnaryMinus`' for the same reason - so
each arm gains an interval sibling that builds the same node with the same mode
and an interval `outputTypes` entry. `IntervalMathUtils`' `addExact` /
`subtractExact` / `negateExact` are Spark's own definitions and are checked in
every mode, so the mode is `FAIL` unconditionally rather than read from
`evalMode`.

`abs(ym)` is `IntegerExactNumeric`'s abs, which throws only on `Int.MinValue`,
exactly as `IntNeg` does - so it is `IntNeg`'s bound reasoning with a different
op, and `intBound` rules the check off wherever it rules it off for a negation.

`make_ym_interval(y, m)` is `m + 12 * y` with both parts checked, which is two
of
task 63's nodes composed; `intBound` proves the check away over bounded
operands,
which is what makes `make_ym_interval(year(d), month(d))` fuse with no check at
all.

### 3.3 Group C, as one emitter check split in two

`requireOffsetShape` serves two positions with different runtime-guard
situations, and this task separates them: `add_months`' month count keeps a
check
that admits a literal, a column, *or* task 63's arithmetic - because task 60's
lanewise guard covers a derived value - while `next_day`'s weekday keeps the
strict pair, because its bound is a compile-time fold with no runtime guard
behind it. The comment that currently conflates them is corrected with the
split.

With that, `d - ym_col` is a `compileMonths` arm over `IntNeg`, and the
`YEAR`-unit cast is an `IntArith` `MUL` by 12 whose check `intBound` removes
over
a bounded operand and whose unbounded form declines as task 63 declines any
unbounded checked multiply.

### 3.4 Why group B is its own task and not this one's tail

Two reasons, and the second is the one that matters. It is a different kind of
work - a guard, a range table and a rounding correction, against group A's
arms -
and this project's rule is that a new node type or a new guard earns its own
admission check and its own A/B.

But also: **group B is the task that milestone 5 may delete, now by either of
two routes.**
`PLAN_MILESTONE_5.md`
2.7 (task 65) establishes that widening the dividend to int64 lanes makes the
magic exact with a single 64-bit low product, no range restriction and no
correction carries - `LongVector`'s `MUL` provides what int lanes lack. If int64
lanes land first, `extract` is a widen, a multiply, a shift and a narrow, with
no
guard and no range table at all. Building the int-lane version now means
building
the thing task 65 exists to remove, and then owning both.

And since this plan was written a second route appeared, from its own admission
check: `PLAN_MILESTONE_5.md` 2.19 (task 88) is an exact division through
*double*
lanes - `trunc((double) v * (1.0 / d))`, exact for every int32 dividend and any
divisor below about 2^21, with no magic, no correction and no range restriction,
and unlike task 65 no int64 lane and so no lane-width precondition. It was found
while checking 2.2's bound and is verified numerically and through an
`I2D`/`D2I` round trip; whether it is *fast* enough is a three-arm A/B against
today's magic and task 65's widening, which is task 88's own admission check.

So group B is registered as a milestone row with both dependencies stated,
rather than built here on a bound that is one forty-thousandth of its type's
range and that either route removes outright.

## 4. Files

| file | what |
|---|---|
| `VarkaExpressionCompiler.scala` (+ suite) | the interval siblings of the `Add`/`Subtract`/`Multiply`/`UnaryMinus` arms, `Abs`, `make_ym_interval`; `compileMonths` taking `IntNeg` and the `YEAR` cast; the reasons that change |
| `VarkaLoopEmitter.java` (+ suite) | `requireOffsetShape` split into the month-count and weekday positions, with the comment corrected |
| `VarkaDifferentialSuite.scala`, `VarkaSharedSessions.scala` | the algebra over `varka_dates_intervals`, both ANSI modes, with the overflow rows raising the row engine's own error |
| `Surface.java`, `DateSurfaceBenchmark.java` | the new shapes in task 62's surface |
| `docs/sql-varka.md` | the interval surface, which task 67 wrote and this widens |
| `PLAN_MILESTONE_4.md`, this file | row 68, section 2.33 amended for the A/B/C split, group B as a new row, section 9 |

## 5. Tests, and what each is for

* **The compiler**, per expression and per mode: the node and its `outputTypes`,
  the bound removing the check where the operands are bounded, and the unbounded
  multiply declining as task 63's does.
* **`d - ym_col` and the `YEAR` cast fusing**, which are the two shapes task 67
  pinned as declining - so those tests inverting is the evidence group C landed,
  and their decline reasons disappear from the suite.
* **The position split**: `next_day`'s weekday still refuses arithmetic, with
its
  own reason, while `add_months`' count now takes it. The test that fails if the
  split is made in one direction only.
* **The differential**, both ANSI modes over the interval fixture: every shape
  against the row engine, the overflow rows raising `ARITHMETIC_OVERFLOW` from
  the row engine after the batch declines, and `try_*` where Spark spells it.
* **The emitter**, a value matrix over the int32 extremes for the new arms - the
  same shape task 63's matrices take, since these are its nodes.

## 6. The measurement

A parity row per new node beside task 63's arithmetic rows, on
`VarkaArithmeticBenchmark` rather than the parity file: these are the same
kernels as task 63's with a different Spark type on the outside, so the question
is the one task 67 asked and answered for `d + ym` - that the type costs
nothing -
not what a new lowering costs.

`dev/varka_bench_ids.sh` gives the free ids before the cases are written.

### 6.1 Predictions, registered before the run

1. Each interval arm lands within 3% of its int twin at both widths, since they
   are the same emitted kernel; a larger gap is a finding about the Arrow read
   path, not the lane.
2. `make_ym_interval(year(d), month(d))` emits no overflow check, the bound
   proving it away, and is within 3% of `year(d) * 100 + month(d)` - task 63's
   composite key, which is the same two nodes.
3. No committed number moves: no shape that exists today gains or loses a byte.

## 7. Risks

1. **Widening an arm's type gate instead of adding a sibling**, which is how an
   interval would reach an int-typed position. The arms stay gated; the siblings
   are separate.
2. **The position split done on one side** (2.5), which is a ghost fallback in
   whichever direction is left behind. Its test is written first.
3. **`abs(Int.MinValue)`**, which throws in Spark and must decline or condemn
   rather than answer - `IntNeg`'s case exactly, and its bound reasoning.
4. **A `LEGACY`-mode expectation.** None of group A has a wrapping form; a test
   that sets `EvalMode.LEGACY` and expects `WRAP` would be asserting something
   Spark does not do.

## 8. Sequencing

1. This plan, the milestone row, section 2.33 amended for the split, and group B
   opened as its own row with its task 65 dependency.
2. The emitter's position split and its test (group C's blocker), first because
   both of C's shapes wait on it.
3. Group A's arms with the compiler suite, then C's two arms.
4. The differential and the fixture.
5. The parity rows, the docs, section 9, row 68.

## 9. Outcome

<!-- Filled in when the measurement lands: the numbers with the committed file
     they trace to (dev/varka_quote_check.py holds you to this), 6.1's
     predictions scored one by one, what moved that the plan did not list, and
     what the task leaves for later - which goes to the milestone's debt
     register or a scope document, never to a code comment. -->
