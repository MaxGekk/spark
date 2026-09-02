# Task 52: guard at the producer, not the extraction

**Status: planned, not started.** Task 51 (PR #73) removed the per-extraction
range guard task 26 shipped; this task closes the gap that removal opened. Read
`PLAN_TASK_51.md` and `PLAN_MILESTONE_4.md` sections 2.21-2.22 first - they
carry the argument for why the guard moved rather than simply disappearing, and
this file assumes that argument rather than repeating it.

This is the second version of this plan. The first, written in the same PR as
the removal, guarded only `date_add`/`date_sub` with a *column* offset, on the
claim that "a literal offset's magnitude is visible to the compiler". Visible,
yes; bounded, no. `DateVarkaSupport.foldDaysOffset` accepts any `Int` literal,
nothing checks it, and `year(date_add(d, 20000000))` fuses today - the exact
query the removed differential test used - and after task 51 returns a wrong
year. The first version would have left that in place. Section 2 below is the
corrected scope; section 1 is the rule that produces it.

If you find yourself making a design decision beyond what sections 1-3 settle,
stop and say so in the pull request instead of choosing.

## 1. The rule

A calendar node (`Year`, `Month`, `DayOfMonth`, `Quarter`, `DayOfYear`,
`LastDay`, `AddMonths` - everything `isChrono` admits) runs the narrowed
civil-from-days decomposition, which is exact only on
`VarkaChrono.NARROW_MIN_DAYS..NARROW_MAX_DAYS` (-5394572..11382643, i.e.
1 March -12800 to 15 August 33134). The project's contract is that a date
*column* holds `[0001-01-01, 9999-12-31]`, epoch days `[-719162, 2932896]`.
The narrowed range therefore has slack on both sides of the contract:

    -4675410 <= shift <= 8449747

A day value reaching a calendar node is a contract column plus whatever the IR
between them added. The question task 26's guard answered at run time on every
extraction, and this task answers once, is: **how much can the IR between a
column and this calendar node shift the day?**

* **Bounded, and inside the slack** (`year(d)`, `year(date_add(d, 30))`,
  `year(add_months(d, 12))`, `year(next_day(d, 'MON'))`): the value cannot leave
  the range. No guard, at compile time or run time. This is almost every query.
* **Bounded, and outside the slack** (`year(date_add(d, 20000000))`,
  `year(date_sub(d, 5000000))`): the compiler knows the answer before anything
  runs. **Decline the calendar entry at compile time** with a task-16 reason; the
  row engine computes it correctly. Cost: nothing at run time, one residual
  entry in a query nobody writes.
* **Unbounded** (`year(date_add(d, offset_col))`, task 38 / PR #62): the shift
  is a per-row value the compiler cannot see. **A runtime guard at the producer
  node** - the `AddDays`/`SubDays` whose offset is a column - and only when a
  calendar node consumes it, behind an emit option.

That is the whole task: an interval analysis in the compiler, and the old guard's
bytecode re-emitted at one node type instead of six, only where the analysis
says it is needed.

## 2. Scope, settled

* **The compile-time half is not behind the flag.** It is free, it is correct,
  and it is the only half reachable on today's master, where every day offset
  is a literal. It also lands independently of PR #62.
* **The runtime half guards `AddDays`/`SubDays` with a non-literal offset, and
  only those with a calendar node above them.** `date_add(d, offset_col)`
  projected on its own, or compared, or diffed, produces whatever `int` add
  produces - the same as Spark's `DateAdd`, which does not check either - and
  no decomposition ever reads it. Guarding it would decline batches Spark
  answers fine, for nothing. The emitter's analysis decides "has a calendar
  consumer" by walking each calendar node's subtree, not by looking at the
  producer alone.
* **`NextDay` and `AddMonths` need nothing new.** `NextDay`'s shift is `1..7`
  by construction (task 33 accepts only a foldable weekday). `AddMonths`'s
  literal month count is already bounded by task 40's compile-time decline at
  `MONTH_ARITH_MIN/MAX_MONTHS` (`+-24564` months, about 2047 years, so at most
  `+-24564 * 31` days: inside the slack from any contract date, and inside it
  from anything a bounded `AddDays` chain leaves inside it too). Both enter the
  interval analysis as bounded shifts; neither gets a case of its own.
* **A bare `ColumnRef` is the contract, not a check.** No ingestion-time check;
  the contract is what makes the slack computable at all.
* **Every calendar extraction stays guard-free**, per task 51.
* **Pass-through nodes (`Greatest`, `Least`, `IfElse`) take the hull** of their
  date operands' intervals. `DateDiff` produces a day *count*, not a date; a
  calendar node cannot consume it (the compiler's type gate), so it is not in
  the analysis.
* **The runtime guard's flag is `VarkaEmitOptions.guardDayProducers`**, default
  chosen by section 5's measurement, expected on: it costs nothing on any
  query with no column-offset date arithmetic under a calendar function, and
  section 5 prices what it costs on the one shape that has it.

## 3. Mechanics

### 3.1 The compiler: `dayShift`, and where it declines

In `VarkaExpressionCompiler`, beside `foldOffset`/`foldMonths`, a total function
over the IR the calendar arms have already built for their child:

    dayShift(node): Option[(Long, Long)]        // None = unbounded
      ColumnRef            -> Some((0, 0))
      AddDays(x, lit k)    -> dayShift(x) map { (lo, hi) => (lo + k, hi + k) }
      SubDays(x, lit k)    -> dayShift(x) map { (lo, hi) => (lo - k, hi - k) }
      AddDays/SubDays(x, non-literal) -> None
      NextDay(x, _)        -> dayShift(x) map { (lo, hi) => (lo + 1, hi + 7) }
      AddMonths(x, lit m)  -> dayShift(x) map { (lo, hi) => (lo + min(31m, 28m), hi + max(31m, 28m)) }
      Greatest/Least/IfElse over dates -> hull of the date operands' intervals
                                          (None if any operand is None)
      any calendar node    -> Some((0, 0))   // its output is a field or a date it
                                             // recomposed inside the range (AddMonths);
                                             // a date-typed calendar output (LastDay,
                                             // AddMonths) re-enters the contract range
      LiteralSlot as a date operand (IfElse branch) -> Some((0, 0)) if the literal is a
                                             date in [0001, 9999]; the compiler's literal
                                             arm already restricts to DateType

Longs, not ints, so two literals of 2 billion cannot wrap the sum. The literal's
value comes from the compiler's own `literals` table, which the arm building the
`LiteralSlot` just wrote; the analysis runs in the calendar arms (`Year(child)`,
`LastDay(child)`, `AddMonths(startDate, ...)` and the rest), after
`compileNode(child, ...)` returns, on the IR it returned.

The calendar arm then does one of three things:

* `Some((lo, hi))` with `lo >= -4675410 && hi <= 8449747`: build the node as
  today.
* `Some((lo, hi))` otherwise: `sink.note("day shift of [lo, hi] can leave the
  calendar range", expr)` and return `None` - the entry declines, the row engine
  serves it, verbose `EXPLAIN` says why (task 16).
* `None`: build the node as today. The emitter decides the rest.

The two slack constants are named in `VarkaChrono` beside `NARROW_MIN_DAYS`/
`NARROW_MAX_DAYS`, derived from them and from the contract's two epoch days in
the source (not typed in as decimals), with the derivation in their javadoc.

### 3.2 The emitter: which producers, and where the bytes go

**Which.** In `Analysis`, beside `skipping`/`columns`: for each calendar node
(`isChrono`), walk its subtree and collect every `AddDays`/`SubDays` whose
`offset` is not a `LiteralSlot`. The union over all calendar nodes is
`guardedProducers`, a `Set<VarkaVectorIR>` on `Analysis`. Empty on every kernel
with no column-offset date arithmetic under a calendar node - which is what
makes the default cheap. The predicate that replaces task 51's deleted
`hasChrono` in `planSlots` is: this body's outputs' subtrees intersect
`guardedProducers` (and `options.guardDayProducers()`). Only then is
`s.guardAcc` allocated, and `emitStatusReturn` already does the rest.

**Where.** PR #62 routes `AddDays`/`SubDays` through `emitAndValidatedOp`,
which leaves the result vector on the operand stack after `invokevirtual add`/
`sub`. When `node` is in `guardedProducers` and the option is on, immediately
after the op:

    dup                                        // keep the result for the parent
    astore  guardTmp(node)                     // one Object slot per guarded node,
                                               //   planned like dowTmp
    aload   guardTmp; getstatic LT; ldc NARROW_MIN_DAYS; invokevirtual compare
    aload   guardTmp; getstatic GT; ldc NARROW_MAX_DAYS; invokevirtual compare
    invokevirtual VectorMask.or
    [masked body]  aload species; loadWord(wordRef(node)); invokestatic fromLong;
                   invokevirtual VectorMask.and
    [epilogue]     aload epilogueMask; invokevirtual VectorMask.and
    aload guardAcc; invokevirtual VectorMask.or; astore guardAcc

This is task 26's guard - the block task 51 deleted from `emitEra`, at
`git show 35f4000c407:.../VarkaLoopEmitter.java` lines 2593-2619 - retargeted at
the producer's *output*. Two things carry over unchanged and for the same
reasons: the AND with the node's validity word (a null row's lanes are undefined
and must not condemn the batch; under #62 the node's word is the AND of the
date's and the offset's, so a null *offset* is covered too), and the AND with
the epilogue's bounds mask (the padding lanes of a partial group hold whatever
the masked load left, and `0 + Int.MinValue` is out of range). The dense body
skips the word AND: every lane is valid there.

The guarded node is emitted once per lane group whatever its use count -
`emitValue`'s `computed` set already guarantees that - so the guard runs once
per distinct producer, not once per consumer. That is the whole difference from
task 26's placement.

### 3.3 What the flag does and does not gate

`guardDayProducers = false`: no `guardTmp` planned, no guard emitted,
`s.guardAcc` null, `emitStatusReturn` emits a constant zero - byte for byte
task 51's emitter. The compile-time decline in 3.1 still applies; there is no
option that turns a free correctness check off.

## 4. Files

* `VarkaChrono.java` - the two slack constants and their derivation.
* `VarkaExpressionCompiler.scala` - `dayShift`, the check in every calendar arm,
  the decline reason string.
* `VarkaEmitOptions.java` - `guardDayProducers`, `withGuardDayProducers`,
  `canonical()`.
* `VarkaLoopEmitter.java` - `Analysis.guardedProducers`; the `planSlots`
  predicate and `guardTmp`; the block in `emitAndValidatedOp`; `emitEra`'s
  task-51 javadoc, which says the gap is "a column offset, not a literal one" -
  corrected to point here; `emitStatusReturn`'s doc ("a constant zero where
  nothing is guarded") is already right.
* `VarkaExpressionCompilerSuite.scala` - section 5.1.
* `VarkaLoopEmitterSuite.scala` - section 5.2.
* `sql/core/.../VarkaDifferentialSuite.scala` - section 5.3.
* `VarkaEmitterParityBenchmark.scala` and its committed file - section 5.4.
* `PLAN_TASK_51.md` section 4 currently says the gap is "reachable today only
  through `date_add`/`date_sub`/`next_day` with a column offset"; corrected to
  name the literal case too. `PLAN_MILESTONE_4.md` row 52 and section 2.22
  likewise (done in the same commit as this revision).

## 5. Validation

### 5.1 The compiler, by the bound's edges

In `VarkaExpressionCompilerSuite`, with the constants read from `VarkaChrono`
rather than retyped:

* `year(date_add(d, HI))` fuses; `year(date_add(d, HI + 1))` declines, with the
  reason naming the interval. Same at `LO` / `LO - 1` through `date_sub`.
* `date_add(d, HI + 1)` alone fuses - no calendar consumer, no bound.
* Two literals under the bound whose sum is over it decline:
  `year(date_add(date_add(d, 5000000), 5000000))`.
* The hull: `year(greatest(date_add(d, 5000000), d))` fuses (interval
  `[0, 5000000]`); `year(if(c, date_add(d, HI + 1), d))` declines.
* `year(add_months(d, MONTH_ARITH_MAX_MONTHS))` fuses; the composition
  `year(add_months(date_add(d, HI - 100), 12))` declines (12 months of days
  past 100 days of slack).
* `year(date_add(d, offset_col))` fuses (unbounded is not declined; that is the
  emitter's case) - this test needs PR #62 and is written on top of it.

### 5.2 The emitter, by the old guard's own cases

In `VarkaLoopEmitterSuite`, on top of #62:

* `year(date_add(d, off))` with `off` a column: a batch with one lane at
  `NARROW_MAX_DAYS + 1 - d` in the vector loop returns `STATUS_CHRONO_RANGE`;
  one in an epilogue-only lane likewise; the same lane under a null `off` row
  returns 0; an in-range batch returns 0. This is task 51's rewritten
  `"a day outside the covered range is no longer declined"` test, un-rewritten
  and moved to the producer - and under `withGuardDayProducers(false)` every
  case returns 0, which is asserted too.
* `date_add(d, off)` alone, same out-of-range data: returns 0 under both flag
  values, and `codeSize` of its `loopMasked0` is identical under both - no guard
  was emitted for a producer with no calendar consumer.
* `year(date_add(d, off))`'s and `year(d)`'s `loopMasked0` sizes differ by the
  guard and nothing else; `year(d)`'s is identical under both flag values.
* The existing matrix, all null patterns and lengths, for
  `year(date_add(d, off))` under both flag values on in-range offsets: results
  identical, status 0 throughout.
* **Pinned oracles: not expected to move.** The `everyNode` fixture's `AddDays`
  has a `LiteralSlot` offset and so is never a guarded producer; the guard adds
  bytes, not nodes, so the line map's key is unchanged; `DEFAULTS` renders empty
  in the hash whichever way the default goes.

### 5.3 The differential, restoring what task 51 removed

The two tests `PLAN_TASK_51.md` section 3 lists as removed, reshaped:

* `SELECT year(date_add(d, off)) FROM t` where `off` is a column holding
  20,000,000: with the flag on, `checkAnswer` against the row engine passes and
  `numFallbackBatchesDeclined > 0`, `numFallbackBatchesKernel == 0`; with the
  flag off, the batch runs on the kernel (`numFallbackBatchesDeclined == 0`) -
  asserted on the *metric only*, never on the value, for the reason
  `PLAN_TASK_51.md` section 3 gives for not encoding a wrong answer as green.
* The filter path: `WHERE year(date_add(d, off)) = year(d2)` under the same
  data, through `VarkaFilterExec`'s decline route, flag on.
* `SELECT year(date_add(d, 20000000)) FROM t`: the entry is residual, the plan
  shows the reason, and the answer matches the row engine - the compile-time
  half, which needs no flag and no #62.
* In-range `off` values on the same table still fuse and never decline, flag on.

### 5.4 The number that sets the default

`VarkaEmitterParityBenchmark`, a new case beside the "year" section's existing
ones, same 4096-row chunks, both widths, five iterations over two-second windows,
three runs, minimums for anything near 1.3x:

| case | flag off | flag on |
|---|---|---|
| `year(date_add(d, off))`, null-free, `off` in range | new | new |
| `year(date_add(d, off))`, mixed nulls | new | new |
| `date_add(d, off)` alone, null-free | new | must equal flag off exactly |

The guard is two compares, an OR, up to two ANDs and an OR into the
accumulator, once per lane group, on a body of ~50 ops; task 26 measured its
own version of exactly this block at a few percent. If the flag-on column costs
more than 10% on the first row, that is a finding to explain before the default
flips; below it, the default flips on, since the cost is paid only by the shape
that needs it.

## 6. Predictions, registered before the run

1. The guard costs **3% to 8%** on `year(date_add(d, off))` at AVX-512, less
   than 10% at 128-bit (more lane groups, but the same six ops each).
   Confidence: medium-high - this is task 26's guard, whose cost is in the
   committed record from before task 51.
2. `date_add(d, off)` alone is byte-identical under both flag values.
   Confidence: high; asserted, not measured.
3. No committed number for any existing case moves: no existing case has a
   column-offset date under a calendar node. Confidence: high.
4. No pinned oracle moves. Confidence: high (5.2).
5. The compile-time half declines nothing in the milestone's corpus (TPC-H uses
   `year(d)`; TPC-DS pre-materialises fields). Confidence: high - and if it
   does decline something, the query has a shift past 4.6 million days and the
   decline is the right answer.

## 7. Risks

1. **The interval analysis is wrong in a direction that matters.** Too tight
   declines a correct query (slow, visible in `EXPLAIN`, not wrong); too loose
   lets a shifted value into the decomposition (wrong, silent). Every rule in
   3.1 is written to over-approximate the shift, and 5.1's edge tests are at
   `+-1` of the bound on purpose. `AddMonths`'s `31m` is the deliberately loose
   end.
2. **A null offset lane condemns a batch.** Covered by the word AND in 3.2 and
   by the null-offset case in 5.2, which is silent-and-slow if missed, not
   silent-and-wrong - but it would make every batch with a null offset fall
   back, which is the "silent total loss of fusion" task 26's own javadoc warned
   about.
3. **A new date-producing node.** `dayShift` is total over today's IR by
   construction (a `default` arm that returns `None` is the safe failure: an
   unknown node is treated as unbounded, and a future column-driven producer
   would then need to join `guardedProducers` to get a guard). The emitter
   side's `guardedProducers` walk is a hand-picked pair of node types; its
   javadoc says so, and `dayShift`'s `None`-for-unknown is what turns a missed
   node into a decline rather than a wrong answer... only if the compiler is
   also taught that `None` from an unknown *producer* means decline, not
   "runtime guard elsewhere". Section 3.1's third bullet is therefore precise:
   `None` builds the node only when the `None` came from a column-offset
   `AddDays`/`SubDays`; `None` from an unrecognised node declines. Write it that
   way.
4. **Sequencing against #62.** The runtime half is meaningless without column
   offsets. Commit 1 does not depend on #62; commit 2 does (section 8).
5. **Task 49.** An exact long-lane decomposition would make the *range* check
   unnecessary for the extraction, but this task's check is about the query's
   arithmetic leaving the contract's neighbourhood, not about the lowering's
   exactness; 2.19's update note already says so. Nothing here changes for 49.

## 8. Sequencing

Two commits, in two PRs if #62 has not landed when the first is ready:

1. **The compile-time half**, off `master` once #73 has landed: the slack
   constants, `dayShift`, the calendar arms' check, 5.1's tests (minus the
   column-offset row), 5.3's literal-offset differential, the plan-file
   corrections in section 4. No emitter change, no flag, no pinned value, no
   committed number. This alone closes every instance of the task-51 gap that
   exists on master today.
2. **The runtime half**, on top of #62 and commit 1: the option, the analysis
   set, the guard block, 5.2, the rest of 5.3, 5.4's cases and one regeneration
   run, the default chosen and recorded here in a section 9 with predictions
   scored; `PLAN_MILESTONE_4.md` row 52 to DONE and 2.22 swept; the regression
   window `PLAN_TASK_51.md` section 4 records closed in the past tense.

## 9. Explicitly out of task 52

* An ingestion-time check of the column contract itself.
* Guarding a producer with no calendar consumer (section 2, second bullet).
* Any change to the fragment mechanism, `emitEra`, or the calendar tails.
* Output reordering or any grouping change (task 32 B2 owns grouping).
* Task 30's `try_*` and ANSI paths, which will want a status bit of their own -
  `STATUS_CHRONO_RANGE` is bit 0 of a bitmask for exactly that reason, and
  nothing here makes it harder.
