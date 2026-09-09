# Task 68: year-month interval algebra

## 1. Where this came from

`PLAN_MILESTONE_4.md` row 68 and section 2.33, which scoped the expressions that
*produce or transform* an interval once task 63's int32 arithmetic and task 67's
type admission were in. Both now are: 63 merged with the arithmetic nodes and
their evaluation modes, and 67 admitted `YearMonthIntervalType` end to end as a
value leaf, a literal, an `add_months` month count and two relabel casts.

Task 67 also left two shapes on this task's doorstep and said why
(`PLAN_TASK_67.md` 2.1): `d - ym_col` and the `YEAR`-unit cast in month-count
position, neither blocked on arithmetic, both blocked on the emitter's
month-count check.

## 2. The admission check, done

Read against master (`3934a8764b8`), with the ranges computed rather than
asserted. The check divides section 2.33's list in three, and the division is
not the one that section drew.

### 2.1 What Spark actually computes

| expression | definition | mode |
|---|---|---|
| `make_ym_interval(y, m)` | `toIntExact(addExact(m, multiplyExact(y, 12)))` | always checked |
| `ym + ym`, `ym - ym` | `IntervalMathUtils.addExact` / `subtractExact` | always checked |
| `-ym` | `IntervalMathUtils.negateExact` | always checked |
| `abs(ym)` | `IntegerExactNumeric`'s abs, whatever `failOnError` says | always checked |
| `ym * num` | `Math.multiplyExact(months, num)` for an int-family `num` | always checked |
| `ym / num` | `IntMath.divide(months, num, HALF_UP)` | rounding, not truncation |
| `extract(YEAR FROM ym)` | `months / 12`, Java division, an `IntegerType` | truncating |
| `extract(MONTH FROM ym)` | `(months % 12).toByte`, a **`ByteType`** | sign follows dividend |

"Always checked" is the useful half: none of the first five has a `LEGACY`
wrapping form, so each is task 63's `FAIL` mode unconditionally, and the
compile-time bound is the only thing that can take the check off.

Three details of that table decide shapes later in the plan:

* **`abs` is not an op the IR has.** Task 63 shipped `IntArith` and `IntNeg`;
  `abs(x)` lowers as `IfElse(Compare(LT, x, 0), IntNeg(x), x)`, a blend over the
  negate. The only input that overflows a negation is `Int.MinValue`, which is
  negative and so takes the `IntNeg` arm - the check fires exactly where Spark
  throws. It is a checked node under a `CASE` arm, which is task 79's shape, and
  it is deliberate.
* **`ym * num` takes any `NumericType`**, not only a literal:
  `MultiplyYMInterval.inputTypes` is `Seq(YearMonthIntervalType, NumericType)`,
  with arms for Byte/Short/Int, Long, Decimal and Double. An int literal is
  bounded and the check comes off; an int *column* is an unbounded checked
  multiply and declines, as task 63 declines every such multiply; Long, Decimal
  and Double are not int32 lanes and decline with a reason naming the type.
* **`extract(MONTH FROM ym)` returns a byte.**
`ExtractIntervalPart[Int](ByteType,
  getMonths, ...)`. Varka has no byte lane and `allocateVector` has no `ByteType`
  arm, so this expression is un-emittable regardless of how the division is
  done - a second blocker beside the one 2.2 finds, and one that a better
  division does not remove.

### 2.2 The two divisions need what task 26 was built without

`extract` and `ym / num` need division by a constant. `VectorOperators` has no
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
range. So `extract(YEAR)` does not need a new magic; it needs the *same* magic
over a dividend the type does not bound. A year-month interval is a full int32
month count, and 49,151 months is about 4,096 years: the magic covers roughly
one forty-thousandth of the type's range.

Powers of two are the exception and are exact over all of int32, which matters
for `ym / num` with a literal power of two: no magic at all.

### 2.3 Truncation is not floor, and the difference is only on negatives

The magic computes a floor. Java's `/` truncates toward zero, and
`extract(YEAR)` is Java's `/`:

| months | Java `/12` | floor | Java `% 12` |
|---|---|---|---|
| -13 | -1 | -2 | -1 |
| -12 | -1 | -1 | 0 |
| -1 | 0 | -1 | -1 |
| 13 | 1 | 1 | 1 |

So an `extract` built on the existing magic needs a correction on the negative
side, and `extract(MONTH)` inherits it, being `months - 12 * q` for whichever
`q` the year extract produced. Task 60's use of this magic does not need the
correction, because `add_months` wants the floor and biases its dividend
non-negative to get it. That bias is why one constant serves two callers that
disagree about rounding, and it is worth saying where the correction goes rather
than discovering it from a differential.

### 2.4 `ym / num` rounds HALF_UP, which a magic does not give

`IntMath.divide(months, num, HALF_UP)` rounds to the nearest, ties away from
zero: `-5 / 2` is `-3`, not `-2`. A quotient from a magic is a floor; HALF_UP
needs the remainder as well - `q` adjusted by one where `2 * |r| >= |num|`, in
the direction of the quotient's sign. That is three more lanewise ops on top of
a magic that is already range-bounded, and the bound is per-divisor (2.2's
table), so a guard's range would have to be computed per literal rather than
being one constant.

### 2.5 What task 67 left, and which half of it is about the emitter

`d - ym_col` resolves to `DateAddYMInterval(d, UnaryMinus(ym))`, so its month
count is an `IntNeg`. The `YEAR`-unit cast is a `12 *` over its operand, an
`IntArith`. Where either sits in `add_months`' **month-count position**, the
emitter's `requireOffsetShape` admits a literal slot or a column and nothing
else, so admitting them in the compiler alone would be a ghost fallback. That is
the blocker, and it is an emitter check, not arithmetic.

The `YEAR`-unit cast in **value position** - `SELECT CAST(i AS INTERVAL YEAR)` -
has no such blocker: it is an `IntArith` `MUL` with an interval output, the
same shape as `ym * num`, and belongs with group A. Task 67's 2.1 was written
about `compileMonths` and did not draw this line; this plan does.

`PLAN_TASK_67.md` 2.1 also recorded, and this task acts on it, that the stated
reason for the month-count strictness - "a weekday and a month count carry
runtime bounds a derived value cannot declare" - is true of `next_day`'s weekday
and false of the month count. A column-count `AddMonths` is in `selfGuarding`
and is guarded at run time by a lanewise check on the count's *value*, which
does
not care what produced it; a derived count is covered by that same guard. The
two positions share a check and do not share the reason, and splitting them is
this task's.

### 2.6 What the check would have rejected

That `extract` is "a literal-divisor magic multiply" and therefore small (it is
the existing magic over a dividend forty thousand times its exact range, and
`extract(MONTH)` is un-emittable regardless); that the magic's rounding matches
Java's (it is floor, `extract` truncates); that `ym / num` is the same shape as
`extract` (it rounds HALF_UP and its exact range depends on the divisor); that
`abs` is an op (it is a blend over `IntNeg`); that `ym * k` takes a literal (it
takes any numeric); and that the shapes task 67 deferred were waiting on task 63
(they are waiting on an emitter check this task owns, and only in one position).

## 3. The design

### 3.1 The split the check found, and what this task takes

Section 2.33 lists eight things as one task. 2.1 to 2.5 divide them by what they
need from the emitter, and the division is sharp:

**Group A - no new machinery.** `make_ym_interval`, `ym + ym`, `ym - ym`, `-ym`,
`abs(ym)`, `ym * num`, and the `YEAR`-unit cast in value position. Every one is
task 63's `IntArith`/`IntNeg` - `abs` as a blend over `IntNeg` - in `FAIL` mode
with an interval-typed output, over operands task 67 already admits. The
compile-time bound applies unchanged, so `make_ym_interval(year(d), 3)` needs no
check while `ym1 + ym2` over two columns does. No IR node, no emitted byte that
task 63 did not already emit.

**Group B - a bounded division and a guard.** `extract(YEAR|MONTH FROM ym)` and
`ym / num`. Both need a magic whose exact range is a fraction of the type's,
both
need a rounding correction the existing magic does not carry, `ym / num` needs
its range computed per literal, and `extract(MONTH)` needs a byte output the
evaluator does not have.

**Group C - one emitter check.** `d - ym_col`, and the `YEAR`-unit cast in
month-count position, blocked on `requireOffsetShape` (2.5).

**This task takes A and C.** B is milestone 5's task 89, and 3.4 says why that
is not merely a size argument.

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

`abs(ym)` is the blend of 2.1: `IfElse(Compare(LT, x, 0), IntNeg(x), x)`, with
`intBound` of the result equal to `intBound` of the operand, so the check comes
off wherever it would for a negation.

`ym * num` takes the int-family `num` arms only. A literal is a slot; an int
column is `intOperand`'s column and declines as an unbounded checked multiply,
exactly as `i * 3` under ANSI does; Long, Decimal and Double `num` decline with
"interval multiplier of type <t> is not an int32 lane".

`make_ym_interval(y, m)` is `m + 12 * y` with both parts checked, which is
two of
task 63's nodes composed; `intBound` proves the check away over bounded
operands,
which is what makes `make_ym_interval(year(d), month(d))` fuse with no check at
all.

The `YEAR`-unit cast in value position is `IntArith(MUL, FAIL, x, 12)` with an
interval output - the arm task 67 wrote, reverted before it compiled, and
recorded in its 2.1 as a ghost fallback. It was a ghost fallback *in month-count
position*; in value position there is no emitter check to drift from, and it
lands here.

### 3.3 Group C, as one emitter check split in two

`requireOffsetShape` serves two positions with different runtime-guard
situations, and this task separates them: `add_months`' month count keeps a
check that admits a literal, a column, *or* task 63's arithmetic - because task
60's lanewise guard covers a derived value - while `next_day`'s weekday keeps
the
strict pair, because its bound is a compile-time fold with no runtime guard
behind it. The comment that currently conflates them is corrected with the
split.

With that, `d - ym_col` is a `compileMonths` arm over `IntNeg`, and the
`YEAR`-unit cast in month-count position is the same `IntArith` as its
value-position twin, admitted by the widened check and guarded on its value by
task 60.

### 3.4 Why group B is its own task and not this one's tail

Two reasons, and the second is the one that matters. It is a different kind of
work - a guard, a range table, a rounding correction and a byte output, against
group A's arms - and this project's rule is that a new node type or a new guard
earns its own admission check and its own A/B.

But also: **group B is the task that milestone 5 may delete, by either of two
routes.** `PLAN_MILESTONE_5.md` 2.7 (task 65) establishes that widening the
dividend to int64 lanes makes the magic exact with a single 64-bit low product,
no range restriction and no correction carries. And this plan's own admission
check found a second route, now `PLAN_MILESTONE_5.md` 2.19 (task 88): an exact
division through *double* lanes, `trunc((double) v * (1.0 / d))`, exact for
every int32 dividend and any divisor below about 2^21, with no magic, no
correction, no range restriction and no int64 lane. Whether it is *fast* is a
three-arm A/B, which is task 88's admission check.

Building the int-lane version of group B now means building the thing either
route exists to remove, and then owning both. So group B is
`PLAN_MILESTONE_5.md` 2.20 (task 89), with tasks 65 and 88 named as the routes
it waits on and the `ByteType` output named as the blocker neither removes.

## 4. Files

| file | what |
|---|---|
| `VarkaExpressionCompiler.scala` (+ suite) | the interval siblings of the `Add`/`Subtract`/`Multiply`/`UnaryMinus` arms, `Abs` as a blend, `make_ym_interval`, the value-position `YEAR` cast; `compileMonths` taking `IntNeg` and the month-count `YEAR` cast; the reasons that change and the new ones for non-int multipliers |
| `VarkaLoopEmitter.java` (+ suite) | `requireOffsetShape` split into the month-count and weekday positions, with the comment corrected |
| `VarkaDifferentialSuite.scala`, `VarkaSharedSessions.scala` | the algebra over `varka_dates_intervals`, both ANSI modes, with the overflow rows raising the row engine's own error |
| `VarkaThroughputBenchmark.scala` + results | section 6's pairs on task 67's `varka_date_interval_counts` fixture |
| `Surface.java`, `DateSurfaceBenchmark.java` | the new shapes in task 62's surface |
| `docs/sql-varka.md` | the interval surface, which task 67 wrote and this widens |
| `PLAN_MILESTONE_4.md`, `PLAN_MILESTONE_5.md`, this file | row 68, section 2.33 amended for the split, task 89 opened, section 9 |

## 5. Tests, and what each is for

* **The compiler**, per expression and per mode: the node and its `outputTypes`,
  the bound removing the check where the operands are bounded, the unbounded
  multiply declining as task 63's does, and the Long/Decimal/Double multipliers
  declining with their reason.
* **`abs` as a blend**: the IR pinned as `IfElse` over `IntNeg`, and the value
  matrix including `Int.MinValue`, which must condemn the batch and not answer.
* **`d - ym_col` and the month-count `YEAR` cast fusing**, which are the two
  shapes task 67 pinned as declining - so those tests inverting is the evidence
  group C landed, and their decline reasons disappear from the suite.
* **The position split**: `next_day`'s weekday still refuses arithmetic, with
  its own reason, while `add_months`' count now takes it. The test that fails if
  the split is made in one direction only.
* **The differential**, both ANSI modes over the interval fixture: every shape
  against the row engine, the overflow rows raising `ARITHMETIC_OVERFLOW` from
  the row engine after the batch declines, and `try_*` where Spark spells it.
* **The emitter**, a value matrix over the int32 extremes for the new arms - the
  same shape task 63's matrices take, since these are its nodes.

## 6. The measurement

The type exists only above the kernel - in `outputTypes` and `allocateVector` -
and not in the IR, so an emitter-level parity row cannot see it: an "interval
arm" and its int twin are the same IR and the same bytes. The instrument is the
one task 67 used, `VarkaThroughputBenchmark` end to end through Spark, on task
67's `varka_date_interval_counts` fixture, which already holds the same count as
an int column and as an interval.

Two pairs, each the same arithmetic spelled over the two types:

| pair | int form | interval form |
|---|---|---|
| addition | `m + m2` (task 63's checked add) | `ym + ym2` |
| the composite | `year(d) * 12 + month(d)` | `make_ym_interval(year(d), month(d))` |

The fixture gains `m2`/`ym2` as a second count of the same generator with a
different shift - built once and reused for both spellings, and asserted
different from `m`/`ym` before anything is timed, per this project's fixture
rule.

`dev/varka_bench_ids.sh` is not needed here; the throughput file has no case
ids.

### 6.1 Predictions, registered before the run

1. Each interval form lands within 3% of its int form at both widths, since the
   kernel is the same and only the output vector's class differs; a larger gap
   is a finding about the Arrow write path, not the lane.
2. `make_ym_interval(year(d), month(d))` emits no overflow check, `intBound`
   proving it away, and the emitter suite's op-count register says so.
3. No shape that exists today changes a byte: every pinned oracle and every
   `codeSize` assertion holds, and the byte-identity form of that claim is what
   the suite checks. The committed *numbers* in the regenerated file will move
   by the run-to-run band this file has shown before, and that is not a
   prediction about this task.

## 7. Risks

1. **Widening an arm's type gate instead of adding a sibling**, which is how an
   interval would reach an int-typed position. The arms stay gated; the siblings
   are separate.
2. **The position split done on one side** (2.5), which is a ghost fallback in
   whichever direction is left behind. Its test is written first.
3. **`abs(Int.MinValue)`**, which throws in Spark and must condemn rather than
   answer - the blend's `IntNeg` arm is taken for it, and the matrix pins it.
4. **A `LEGACY`-mode expectation.** None of group A has a wrapping form; a test
   that sets `EvalMode.LEGACY` and expects `WRAP` would be asserting something
   Spark does not do.
5. **A non-int multiplier admitted by accident** - `ym * 2.5` is legal Spark and
   must decline by type, not compile as if `2.5` were an int.

## 8. Sequencing

1. This plan, the milestone row, section 2.33 amended for the split, and task
   89 opened with tasks 65 and 88 as its routes.
2. The emitter's position split and its test (group C's blocker), first because
   both of C's shapes wait on it.
3. Group A's arms with the compiler suite, then C's two arms.
4. The differential and the fixture.
5. The throughput pairs, the docs, section 9, row 68.

## 9. Outcome

<!-- Filled in when the measurement lands: the numbers with the committed file
     they trace to (dev/varka_quote_check.py holds you to this), 6.1's
     predictions scored one by one, what moved that the plan did not list, and
     what the task leaves for later - which goes to the milestone's debt
     register or a scope document, never to a code comment. -->
