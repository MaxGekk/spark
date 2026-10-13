# VARKA-323: A variant stays within the op cap its draw was drawn for

## 1. Where this came from

The night of 12 to 13 October 2026 ran `VarkaIrFuzzSuite` on eight slots, 80 chunks of 60,000
int-lane draws each with a fresh seed, while VARKA-320's port waited for review. 75 chunks passed.
The other five failed one way, all in row 278's variants of a draw that had passed:

    the untaken arm replaced variant of a passing draw failed: seed=2026101307001
    iteration=47340 ... (a variant): the emitter rejected the shape without the budget: more
    than MAX_FUSED_NODES=64 distinct ops

The failing iterations were seed 2026101307001 at 47340, 2026101306003 at 44588, 2026101303005 at
29063, 2026101308007 at 26463 and 2026101305008 at 57488. Four failed in the untaken-arm variant
and one, 2026101303005, beside a stranger. Replaying the first with `-Dvarka.fuzz.only=47340`
fails the same way in seconds, on master as on the 320 branch, on the same tree.

The mechanism is in the harness. The grammar keeps a draw well inside the emitter's
`MAX_FUSED_NODES` = 64 distinct ops and `MAX_CHAIN_DEPTH`. Two variants grow the draw: the
untaken-arm variant replaces an arm no row takes with another value subtree of the draw, which
may be larger, and the stranger variant adds a second root over copies of the columns, whose ops
are distinct from the original's because the columns are. Under the byte budget such a variant
may decline on size, which it may. `emitOrSkip` then emits it without the budget. That form is
the one capped by op count, so the emitter throws `IllegalArgumentException`, and `emitOrSkip`
reports that as a shape the emitter rejected. No answer was wrong. The failure stops the chunk's
remaining draws.

The row was logged the same morning (PLAN.md row 323), and the owner said to start it.

## 2. The admission check, done

`VarkaLoopEmitter.fitsBudgets(roots, numInputs, options)` answers, without emitting, whether the
form without a byte budget holds a shape: the op cap where the budget is off and
`MAX_CHAIN_DEPTH` always (`VarkaEmitterBudgetSuite`, "fitsBudgets mirrors the analysis caps").
A throwaway test, removed before the commit, rebuilt each failing draw's variants as
`runVariants` does and asked `fitsBudgets` with the budget off:

| seed, iteration | roots | failing variant | over the cap | strangers that fit | untaken-arm rotations that fit |
| --- | ---: | --- | --- | --- | --- |
| 2026101307001, 47340 | 3 | untaken arm | that variant only | 3 of 3 | 7 of 29 |
| 2026101306003, 44588 | 1 | untaken arm | that variant only | 1 of 1 | 0 of 16 |
| 2026101303005, 29063 | 2 | beside a stranger | that variant only | 1 of 2 | 35 of 35 |
| 2026101308007, 26463 | 2 | untaken arm | that variant only | 2 of 2 | 0 of 29 |
| 2026101305008, 57488 | 3 | untaken arm | that variant only | 3 of 3 | 10 of 28 |

Every draw itself fits. In each failure the predicate rejects exactly the variant that failed and
no other variant of the draw. So `fitsBudgets` with the budget off is the test of whether a
variant is checkable in both forms. A rotation is the untaken-arm variant built from the pool
scanned from another start, which changes every replacement it makes.

What the check says about alternatives: a fitting stranger existed in the one stranger failure,
and a fitting untaken-arm replacement in two of the four untaken-arm failures. In the other two
(16 and 29 rotations, none fitting) every replacement the pool offers puts the tree over the cap,
so no rebuilding of that variant can make it checkable without the budget.

What the check would have rejected: a fix that only picks another variant, which leaves two of
the five failing; and a fix that skips any variant `fitsBudgets` rejects before emitting, which
would also skip the large variants that the budgeted form answers. The budget form splits a large
shape into methods and has no op cap. Those variants are the heaviest the fuzzer makes and are
worth running.

## 3. The design

### 3.1 Prefer a variant that fits; skip by the cap only where the cap is the reason

Two changes, the first in how variants are built and the second in how a variant's emission is
read.

**Building.** `VarkaVariantChecks.variants` gains a form that takes the strangers in preference
order and a predicate on a variant, `fits`. The stranger variant uses the first stranger whose
variant fits, else the first stranger. The untaken-arm variant is built from the pool scanned from
`rotation`, `rotation + 1` and so on, at most once round the pool, and the first variant that fits
is used, else the one from `rotation`. The existing form, one stranger and no predicate, stays and
behaves as today. `VarkaIrFuzzSuite.runVariants` passes the draw's roots starting at
`(iteration + 1) % roots` as the strangers, the first being today's choice, and as `fits`
`fitsBudgets` with the draw's options and the budget off. A draw whose variants all fit, nearly
every draw, meets the same variants as today.

**Reading.** `emitOrSkip` gains one argument, whether the shape is over the cap of the form
without a budget, which `runVariants` computes with the same `fitsBudgets` call. When that form
throws `IllegalArgumentException` for a shape that is over the cap, the shape is counted as
skipped past the op cap, not failed. That happens either after a size decline under the budget or
at once with the budget off. Any other `IllegalArgumentException`, and any from a shape
`fitsBudgets` admits, still fails as a rejected shape. So the skip names its cause. A shape
`fitsBudgets` and the analysis disagree on still fails, and `VarkaEmitterBudgetSuite` holds the
two together. The suite reports the count after a run beside the class-file cap's.

The drawn shapes never take this path. The grammar keeps them inside the cap, and `runOne`
passes false.

### 3.2 What is deliberately unchanged

- The grammar and the drawn shapes: no draw moves, and `emitted_bytes.json` cannot.
- `VarkaVariantChecks.RETRY_WITHOUT_BUDGET`, the series' emit in `VarkaVariantSuite`. The series
  stops at size 4, and no variant of a tree that small comes near 64 ops.
- The variants themselves and what each may decline by (row 278, VARKA-304's exact status).
- The class-file cap's skip and its bound, `2 + iterations / 1000` (VARKA-219).

### 3.3 Registered op counts

None: no emitted code changes.

## 4. Files

| file | what |
|---|---|
| `VarkaVariants.java` | `untakenArm` with a `fits` predicate, scanning rotations once round the pool |
| `VarkaVariantChecks.java` | `variants` with strangers in order and `fits`; the old form delegates |
| `VarkaVariantCheck.scala` | the Scala face of the new form |
| `VarkaIrFuzzSuite.scala` | strangers and `fits` in `runVariants`, the over-the-cap argument in `emitOrSkip`, its count, the regression tests |
| `VarkaVariantSuite.scala` | unit tests of the two choices |
| `sql/varka/plans/m7/PLAN.md` | row 323 |

## 5. Tests, and what each is for

1. `VarkaIrFuzzSuite`, "a variant past the op cap is skipped, not failed": draw 47340 at seed
   2026101307001 and run its variants as the fuzzer does. Today it fails as a rejected shape. The
   test asserts it passes, with at least one variant either rebuilt to fit or skipped past the op
   cap.
2. `VarkaIrFuzzSuite`, "a stranger that fits is preferred": draw 29063 at seed 2026101303005,
   whose next root as a stranger is over the cap and whose other root fits. The stranger variant is
   built over the other root, fits, and is compared.
3. `VarkaVariantSuite`, the untaken-arm choice: a tree with one untaken arm and a pool whose first
   replacement puts it over a predicate's limit and whose second does not; the variant takes the
   second, and with no replacement fitting it takes the first.
4. The five seeds replayed by hand at their iterations, each passing, recorded in section 9.
5. The suites that build variants pass unchanged: `VarkaIrFuzzSuite`, `VarkaVariantSuite`,
   `VarkaSmallTreeSuite`.

No pinned fixture moves.

## 6. The measurement

None to benchmark. The night's runs are the measurement: the variants' counts per kind after a
fuzz run show how often each choice and each skip happens, and the five seeds replay.

### 6.1 Predictions, registered before the run

1. All five seeds replay passing. Two of the untaken-arm cases end skipped past the op cap
   (2026101306003 at 44588, 2026101308007 at 26463), and the other three end compared on a
   fitting variant.
2. At the suite's default seed and 300 iterations, the variant counts are the same as before the
   change, since no variant of those draws is over the cap.
3. The `fitsBudgets` call adds under 1% to a fuzz chunk's time. It walks the variant's tree once,
   and emission walks it many times.

## 7. Risks

1. `fitsBudgets` and the analysis could disagree, so a variant `fitsBudgets` calls over the cap
   would be skipped though the emitter takes it, or the other way round. `VarkaEmitterBudgetSuite`
   holds them to each other on shapes with distinct ops across outputs. A disagreement the other way
   still fails the fuzzer, which is the safe direction.
2. Preferring a fitting variant could hide a bug that only a large variant shows under the budget.
   The large variant is still the fallback whenever nothing fits, and nothing has been seen to
   fail there except the cap itself.
3. A rotation scan could cost time on a draw with a large pool. It is bounded by the pool size,
   and runs only for a draw whose first choice is over the cap, about one draw in a million.

## 8. Sequencing

One pull request: the plan, then the building and reading changes with their tests, then section
9 with the replays and the counts.

## 9. Outcome

Done on 13 October 2026, as section 3 designed it. No pinned fixture moved and no draw changed.

**The five draws replay passing.** Each was run as the night ran it, at its seed with
`-Dvarka.fuzz.only` set to its iteration. The untaken-arm variant is the one over the cap in four
draws and the stranger in one:

| seed, iteration | what the variant came to |
| --- | --- |
| 2026101307001, 47340 | untaken arm rebuilt within the cap, compared |
| 2026101306003, 44588 | untaken arm with no replacement within the cap, skipped past the op cap |
| 2026101303005, 29063 | stranger over the other root, within the cap, compared |
| 2026101308007, 26463 | untaken arm with no replacement within the cap, skipped past the op cap |
| 2026101305008, 57488 | untaken arm rebuilt within the cap, compared |

Without the change, the two new tests in `VarkaIrFuzzSuite` fail with the night's message
("the emitter rejected the shape without the budget: more than MAX_FUSED_NODES=64 distinct ops").
With it they pass. One test covers the skip at 2026101306003, and the other covers both rebuilds at
2026101307001 and 2026101303005. `VarkaVariantSuite`'s new test holds the untaken-arm choice on a
hand-written tree: the first replacement that fits is taken, and the first one when none fits.

**Predictions.**

1. Right: all five pass, the two named skip, and the other three compare.
2. Right: at the suite's default seed and 300 iterations, the variant counts per kind and outcome
   are identical with and without the change on both lanes (for example, untaken arm 67 compared
   on the int lane and 29 on the long).
3. Not decidable at this resolution. Two runs of 10,000 int-lane iterations each way took 181 s
   and 165 s with the change and 180 s and 177 s without. The runs differ among themselves by up
   to 9%, far more than the change could cost. The change is not slower by anything these runs
   can see.

**Suites.** `VarkaIrFuzzSuite`, `VarkaVariantSuite`, `VarkaSmallTreeSuite` and
`VarkaEmitterBudgetSuite` pass. `dev/lint-java` and `dev/scalastyle` pass.

**What this does not settle.** A variant that is skipped past the op cap is checked only in the
budget form, and only when that form answers it rather than declining on size. Both skipped draws
declined. The grammar's draws never take this path.

### 9.1 Review of #774, 13 October 2026

A `/code-review` of the pull request found nine issues in the reading side and the tests. All are
fixed except where noted:

1. **The budget form's rejection was skipped too.** `emitOrSkip` skipped an
   `IllegalArgumentException` from the first emit of an over-the-cap variant, and with a budget
   that emit is the form that has no op cap, so a real bug there would have been counted as a
   skip. Now a rejection is skipped only when it is the cap the shape is over, in a form the cap
   applies to: `MAX_CHAIN_DEPTH` in either form, `MAX_FUSED_NODES` only without a budget.
2. **The message was not read.** `fitsBudgets` is also false for too many inputs or mixed lanes,
   and those rejections were skipped as the op cap's. The skip now requires the cap's own message.
3. **A draw with no budget skipped the variant unchecked**, though section 3.1 said the budget
   form had run it. Such a variant is now emitted once more under the defaults' budget, which
   has no op cap. It is compared if that form holds it, and skipped if that form declines on
   size. A new test runs the 2026101306003 draw with the budget off and sees the second.
4. **Preferring a variant that fits** could replace large variants the budget form compiles fine,
   and so lose coverage of its splitting. Measured before deciding: over one million int-lane and
   one million long-lane draws (20 seeds, 50,000 each), no first-choice variant was over the cap.
   In the five night draws, each slice from iteration 0 to the failing one held exactly one, the
   failing variant, and the budget form declined all five. A variant over the cap that the budget
   form holds was not seen once. The preference stays.
5. **The regression tests leaked counters** into the fuzz tests after them. They now restore
   `skippedPastTheCap`, the op-cap count and the variant counts.
6. **The rebuild test asserted only an absence.** It now asserts that the rebuilt variant was
   compared. Each test also first checks that its draw's first-choice variant is still over the
   cap, so a grammar change that moves these draws fails the test instead of leaving it passing
   on nothing.
7. **The arm oracle was re-evaluated on every rotation.** It is now read once per condition.
8. **The cap check ran on every emit.** It is by-name now, and asked only when the emitter
   rejects the shape.
9. **An unused options parameter and a stranger built twice.** `fitsWithoutBudget` takes only the
   roots and the inputs, since `fitsBudgets` reads only the budget, and the first stranger's
   variant is reused. The size-control trace (`randomTrace`), which the review also named, is
   informational and has no threshold, so it is left.

`VarkaIrFuzzSuite` (12 passed, 1 opt-in cancel), `VarkaVariantSuite` (7) and
`VarkaSmallTreeSuite` (6) pass, and the linters pass.
