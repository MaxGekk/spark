# VARKA-324: The IR fuzzer's int lane draws its column values evenly

## 1. Where this came from

The review of #770 (VARKA-320) on 13 October 2026 found two bugs older than that pull request,
both in the test harness and neither in the emitter. The row was logged the same day (PLAN.md
row 324).

The first is in `VarkaIrFuzzSuite.drawInt`, which draws an int-lane column value as

    (rnd.nextLong() % (2 * bound + 1) - bound).toInt.max(-bound.toInt).min(bound.toInt)

A signed remainder takes the sign of the dividend, so for a negative `nextLong` the remainder lies
in `(-(2 * bound + 1), 0]`, the value lies below `-bound`, and the clamp moves it to `-bound`.
About half of every int-lane column - day columns and the month column alike - is exactly
`-bound`. `drawLong` in the same file already draws with `Math.floorMod`, and its comment says
why.

The second is in `VarkaSmallTreeSuite`'s reach test, which checks that the series of small trees
builds every node type the grammar draws. For the long lane it draws the grammar's comparison
corpus from `VarkaIrGrammar.FUZZ_SEED`, the int lane's seed, rather than `LONG_FUZZ_SEED`, so it
compares the long series with long shapes that neither the long fuzzer nor the emitted-bytes
oracle draws.

## 2. The admission check, done

**The int draw.** A throwaway test, removed before the commit, drew the first N int-lane cases at
the suite's default seed (`FUZZ_SEED` = 20260903) through `drawInt` and counted, over every column
except the trunc-level column (which draws one of five codes by design), how many values are
exactly `-bound` for that column's bound (`COLUMN_BOUND` = 2,500,000 for day columns,
`MONTH_ARITH_MAX_MONTHS` = 24,564 for the month column):

| draws | values | at `-bound` | at `+bound` | negative | positive | month-column values at `-bound` |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 300 | 64,755 | 32,476 (50.2%) | 2 | 48,743 | 16,012 | 13,685 of 27,321 |
| 2000 | 434,669 | 217,652 (50.1%) | 5 | 326,065 | 108,604 | 87,651 of 174,853 |

So the int lane's data is half one value and a quarter each side of zero below it and above it,
as the mechanism in section 1 predicts. A uniform draw over `[-bound, bound]` would put about
one value in 49,129 at `-bound` in the month column and almost none in a day column.

The same expression, character for character, draws the column values of the wide compositions
(`runWide`, the test "wide compositions past the driver's ceiling ..."), so it has the same
skew. The row names only `drawInt`; the wide compositions are the same lane, so the fix covers
both (3.1).

**The reach test.** A second throwaway test collected the node types that 20,000 draws reach from
each seed, and those the series builds up to the reach test's size (4), for each lane:

| lane | series types | types drawn from `FUZZ_SEED` | types drawn from `LONG_FUZZ_SEED` | drawn, not in series |
| --- | ---: | ---: | ---: | --- |
| int | 36 | 36 | 36 | none from either seed |
| long | 15 | 15 | 15 | none from either seed |

The long lane reaches the same 15 node types from both seeds (`And`, `ColumnRef`, `Compare`,
`ConstDivide`, `Greatest`, `GuardedRange`, `IfElse`, `IntArith`, `IntNeg`, `IsNotNull`, `Least`,
`LiteralSlot`, `NarrowLane`, `Not`, `Or`), and the series builds all of them. So the reach test
passes from the right seed, and the wrong seed hid no gap: the fix makes the test check the
corpus it claims to check without changing its verdict.

**What the check would have rejected.** A fix that changed how many random numbers a value takes
(for example `nextInt(2 * bound + 1)`, two calls on a rejection) would shift every later draw of
the case; the floor modulus takes the same one `nextLong` a value. And had the long seed reached
a node type the series does not build, the seed fix would have been a series fix too.

## 3. The design

### 3.1 One floor-modulus draw for an int-lane column value

A private helper in `VarkaIrFuzzSuite`, beside the two draws, draws an int-lane column value:

    Math.floorMod(rnd.nextLong(), 2 * bound + 1) - bound

which lies in `[-bound, bound]` by construction, with the comment `drawLong`'s draw carries. The
bound is at most 2,500,000, so the narrowing to `Int` is exact, and the clamp is dead and goes.
`drawInt` and `runWide` both call it. Each value still takes exactly one `nextLong`, so the
stream after the data is where it was: the shapes, the literals, the lengths, the null patterns,
the masking and the options of every case are as before, and only the int-lane column values
change.

### 3.2 The reach test's long corpus

`VarkaSmallTreeSuite`'s reach test draws the long lane's corpus from `LONG_FUZZ_SEED` and the
int lane's from `FUZZ_SEED`, the seeds the two fuzzers and the emitted-bytes oracle use.

### 3.3 What is deliberately unchanged

- `emitted_bytes.json` pins the emitted classes of the drawn shapes, which depend on the shapes
  and options only, so it does not move.
- The long lane's draw already uses the floor modulus.
- `VarkaRangeAnalysisSuite` draws its `Role.Offset` column values with the same signed remainder,
  without the clamp, so its offsets range over about `[-3 * bound, bound]` rather than
  `[-bound, bound]`. That suite checks the range analysis against values the guards may admit or
  not, and what domain it should draw is its own question; it is reported with this task's
  outcome rather than changed here.
- No emitter code and no op count changes: this task is test-only, with no registered op counts.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/src/test/scala/.../varka/VarkaIrFuzzSuite.scala` | the floor-modulus helper, used by `drawInt` and `runWide`; the spread test |
| `sql/catalyst/src/test/scala/.../varka/VarkaSmallTreeSuite.scala` | the long corpus from `LONG_FUZZ_SEED` |
| `sql/varka/plans/m7/PLAN.md` | row 324's status |
| `sql/varka/plans/m7/VARKA-324.md` | this plan |

## 5. Tests, and what each is for

1. `VarkaIrFuzzSuite`, a new test that the int draw's column values spread over their range:
   over the first 2000 draws at the suite's seed, every value of a day or month column lies in
   `[-bound, bound]`, no single value holds more than 1% of a bound's values, and each half of
   the range holds more than 40% of them. The trunc-level column is left out, since it draws one
   of five codes on purpose. Today the first condition holds and the other two fail (50% at
   `-bound`, 25% positive).
2. `VarkaSmallTreeSuite`'s reach test, unchanged in its assertions, now over the long seed.
3. The suites that draw int-lane data pass: `VarkaIrFuzzSuite` (the two lanes' differential runs,
   the variants, the wide compositions, the planted-bug tests that search the first 200 draws),
   `VarkaShrinkerSuite`, `VarkaVariantSuite`.
4. `VarkaKnownFailuresSuite` and "every known failure still reproduces". The known-failures file
   holds no entries at this master, so both pass without replaying a draw.
5. `VarkaEmittedBytesSuite` passes with no regeneration, and
   `git diff --quiet origin/master -- sql/varka/emitted_bytes.json` holds.

## 6. The measurement

None to benchmark. The measurement is the admission test of section 2 run again after the fix.

### 6.1 Predictions, registered before the run

1. Over the first 2000 int-lane draws at the default seed, the values at `-bound` fall from
   217,652 to under 20 (a uniform draw expects about 4 in the month column and none in a day
   column), and the negative and positive values are each within 2% of 217,300.
2. The reach test passes from `LONG_FUZZ_SEED` with no missing node type; the long lane reaches
   the 15 types of section 2.
3. The planted-bug tests pick the same draw as before, since which draw a planted bug fails on
   depends on the shape and options, which do not change; they pass.
4. `VarkaEmittedBytesSuite` passes with `emitted_bytes.json` unchanged.

## 7. Risks

1. Spreading the data could surface a real emitter or evaluator bug at values the fuzzer rarely
   reached before, mostly the positive half of the range. That would be the point of the fix; it
   would be recorded in section 9 and in a new row, not hidden.
2. The variants' outcomes depend on the data, since an untaken arm is one no row takes, so the
   variant counts per kind move. Nothing asserts them.
3. A planted-bug test could pick a different draw if its failure turns out to depend on the data;
   the test searches 200 draws, so it would still find one, and section 9 would say so.

## 8. Sequencing

One pull request: the plan, then the helper, the seed fix and the spread test, then section 9
with the admission test rerun and the suites.

## 9. Outcome

Done on 13 October 2026, as section 3 designed it. `drawInt` and `runWide` draw an int-lane column
value through one helper, `intColumnValue`, a floor modulus with no clamp; the reach test draws
each lane's corpus from that lane's seed; and a new test holds the int draw's spread.
`emitted_bytes.json` did not move: `VarkaEmittedBytesSuite` passes with no regeneration and
`git diff --quiet origin/master -- sql/varka/emitted_bytes.json` holds.

**The admission test, rerun.** The same throwaway count over the first 2000 int-lane draws at the
default seed, before and after the change (removed again before the commit):

| | values | at `-bound` | at `+bound` | negative | positive | month-column values at `-bound` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| before | 434,669 | 217,652 | 5 | 326,065 | 108,604 | 87,651 |
| after | 434,669 | 5 | 6 | 217,556 | 217,113 | 5 |

The value count is the same, as it must be when the shapes, lengths and column layouts are. The
five values left at `-bound` are all in the month column, where a uniform draw expects about 3.5
over its 174,853 values.

**Predictions.**

1. Right: 5 values at `-bound` (under 20), and 217,556 negative and 217,113 positive, within 0.12%
   and 0.09% of 217,300.
2. Right: the reach test passes from `LONG_FUZZ_SEED` with nothing missing.
3. Right: both planted-bug searches pick draw 0, before and after, and shrink to the same case
   (the liveness bug to 2 nodes at length 1 in 14 runs; the misdescribed add to 5 nodes at
   length 1 in 35 runs).
4. Right: `VarkaEmittedBytesSuite` passes and the file is unchanged.

**What moved that the plan did not list.** At the suite's 300 iterations, the int lane's untaken-arm
variants went from 29 compared to 27, since which arm no row takes depends on the data; every
other variant count is unchanged on both lanes, and the long lane's are identical. A run at 5000
iterations on each lane at the default seeds passes with every variant compared and none declined
or skipped, so the spread data surfaced no emitter or evaluator bug at that volume (section 7's
first risk).

**Suites.** `VarkaIrFuzzSuite` 10 passed and 1 cancelled (the opt-in size-trace dump),
`VarkaSmallTreeSuite` 6, `VarkaVariantSuite` 6, `VarkaShrinkerSuite` 7, `VarkaKnownFailuresSuite` 5,
`VarkaEmittedBytesSuite` 4 passed and 1 cancelled (the opt-in option audit). `dev/lint-java` and
`dev/scalastyle` pass. The known-failures file holds no entries at this master, so "every known
failure still reproduces" passes without replaying a draw: the done-when's replay holds vacuously.

**What this does not settle.** `VarkaRangeAnalysisSuite` draws its `Role.Offset` column values with
a signed remainder and no clamp, over about `[-3 * bound, bound]` (3.3). Whether that suite should
draw over `[-bound, bound]` is left to its owner and reported with this task.
