# Task 152: the `TIME` split form, priced before any engine exists

## 1. Where this came from

`SCOPE_MILESTONE_6.md` item 11 argues that which physical form a value lives in
should be a compiler decision, and on 19 September 2026 it named the first case
that needs no engine to test: a `TIME` held as `(seconds of day: int32,
nanoseconds within the second: int32)` instead of Spark's nanoseconds of day in
a 64-bit lane. `PLAN_TASK_102.md` 2.5 asks the same question from the other
side - the `TIME` extracts compute in 64-bit lanes and produce 32-bit results
that no store narrows, so `hour`, `minute` and `second` wait on a narrowing
store or on task 28's mixed-width kernels - and the split form would answer it
by changing the representation instead: the extracts become 32-bit divisions
of a number under 86400, and no kernel is mixed-width at all.

Item 11 is a long argument with no number under it. This task puts the first
number there, as a committed benchmark and nothing else: no conversion node, no
compiler arm, no cache encoding. Per the house rule the baseline lands as its
own PR before anything that would improve on it, so a later split-form kernel
shows up as a diff in this file rather than as a claim.

## 2. What is measured

`VarkaTimeBenchmark`, a file of its own in the catalyst benchmark scope, on the
same rungs as `VarkaLongLaneBenchmark` (16384, 262144, 1000000 and 8388608
rows) because the two forms read and write different bytes per row and a single
row count would price a cache boundary. Per rung, four shapes - `hour`,
`minute`, `second`, and the three together - each in four arms on the same
instants:

1. **nanoseconds of day, int64 lanes, conversion form**: the shipped lowering,
   `ConstDivide` through the double lane, three operations per division;
2. **nanoseconds of day, int64 lanes, magic form**: the same tree emitted with
   `useAVX = 2`, fourteen operations per division, which is what every
   AVX2-only runner in `PLAN_TASK_62.md` 11's census emits;
3. **seconds of day, int32 lanes, emitted**: the split form's extracts as the
   emitter lowers an int-lane `ConstDivide` today - the double route, seven
   operations per division, since `ConstDivide` has no magic multiply;
4. **seconds of day, int32 lanes, hand-written magic multiply**: the split form
   as item 11 imagines it, one multiply and one logical shift per division,
   exact over the bounded dividend. The constants are found by search and
   proven by exhaustion at start-up (`magic(3600, 86400)` and
   `magic(60, 3600)`), and the arm is checked against the definition on every
   rung's rows before it is timed.

The trees are built the way a compiler with common subexpressions would build
them: each field's remainder feeds the next, so the three-field shape divides
three times in the long form and twice in the split form (`second` is a
remainder there), and `minute` alone divides twice in both.

Beside the shapes: **the split itself** - one 64-bit division by 10^9 and a
multiply-subtract, both halves stored as 64-bit outputs because the narrowing
store does not exist, so it is an upper bound on what the conversion costs -
and two **floors**, a copy of each column, which is the memory cost of each
lane with no arithmetic.

## 3. Predictions, registered before the run

Committed with the benchmark and before the first regeneration.

1. **`hour` in the split form beats the long conversion form by 1.5x to 2.5x**
   under today's int lowering, at every rung: twice the lanes per register and
   half the bytes per row, against seven operations where the long form has
   three. At the DRAM rung the ratio settles near the byte ratio, 2x.
2. **The hand-written magic multiply beats the long conversion form by at
   least 2.5x on `hour` at the in-cache rungs**, and converges toward the byte
   ratio at the DRAM rung, where arithmetic stops being the bound.
3. **The magic form costs the long lane at least 2x against the conversion
   form on `hour` in cache** - fourteen operations against three - and the
   gap narrows at the DRAM rung. This is the number the AVX2 half of the
   runner pool pays today.
4. **The split costs between 1.3x and 2x of one long `hour`** - the same
   division plus a multiply, a subtract and a second wide store - so splitting
   once and then extracting pays only when two or more fields are taken from
   the same column, or when the split is stored (the Arrow cache's second
   encoding).
5. **Three fields together: the split emitted form beats the long conversion
   form by at least 2x in cache**, two divisions against three at half the
   width, and the hand-written arm by at least 4x.

## 4. Files

* `sql/catalyst/src/test/scala/org/apache/spark/sql/VarkaTimeBenchmark.scala`
* `sql/catalyst/benchmarks/VarkaTimeBenchmark-jdk25-results.txt`,
  `-128bit-results.txt`, `-provenance.txt` - written by
  `dev/varka_bench_regen.sh catalyst VarkaTimeBenchmark`
* `dev/varka_bench_ids.sh` - the new file in the default list
* `PLAN_MILESTONE_5.md` row 152 and section 2.88; `SCOPE_MILESTONE_6.md`
  item 11's `TIME` row points here once the numbers exist.

## 5. What this task does not do

No engine, no node, no cache encoding: the numbers decide whether those are
worth building. The hand-written arm is a reference, not a lowering - the
emitter's `ConstDivide` at the int lane stays the double route until a task
gives bounded int-lane divisions a magic multiply, and that task's first line
will be this file's rows 3 and 4 side by side.
