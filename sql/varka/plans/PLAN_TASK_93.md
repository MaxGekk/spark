# Task 93: guard the outermost producer, so guarded shifts compose

## 1. Where this came from

Task 62 (B), choosing the mixed-type entries for `Chains` on 12 September 2026.
`dayofyear(add_months(last_day(date_add(d, i)), 1) + ymy)` declines at compile
time, and the reason names the mechanism exactly:

    day range [-6156428, 12144157] leaves the calendar lowering's range

The shape is ordinary SQL: shift a date by a column of days, take the month
end, add a month, add a year-month interval column, extract the day of year.
Every piece of it fuses on its own.

## 2. The admission check, done

**What declines is not the column day offset.** That is task 38 and task 52 and
it works: `year(date_add(d, i))` fuses, and so does
`dayofyear(add_months(last_day(date_add(d, i)), 1))` at 218 emitter ops, and
`weekofyear(add_months(d, i) + ymm)` at 288 - a column day offset and an
interval column in one expression. What declines is the *composition of two
guarded producers* under one calendar consumer.

**The arithmetic, from `dayRange` in `VarkaExpressionCompiler`:**

| step | contributes | running interval |
|---|---|---|
| `date_add(d, i)`, column offset | task 52's guard promises only *somewhere in the narrow range* | `[-5394572, 11382643]` |
| `last_day` | `+[0, 30]` | `[-5394572, 11382673]` |
| `+ ymy`, column months | task 60's guard bounds the count to `MONTH_ARITH_MIN/MAX_MONTHS`, so `+/- 31 * 24564` | `[-6156428, 12144157]` |

`admitCalendar` then tests `lo >= NARROW_MIN_DAYS && hi <=
NARROW_DECOMPOSE_MAX_DAYS`, and **only the lower bound fails**: 12144157 is
comfortably inside task 69's widened ceiling of 14766813, while -6156428 is
outside the floor of -5394572.

**The floor cannot be widened the way task 69 widened the ceiling.** 69's
ceiling was a precision limit with headroom in it, found by computing where the
magic multiply overflows. The floor is structural: the narrowed decomposition
runs on `days + NARROW_BIAS` and needs it non-negative, so `NARROW_MIN_DAYS =
-NARROW_BIAS` by construction. Raising the bias moves the window down rather
than widening it, and spends the ceiling headroom 69 just bought. That is a
trade, not a fix, and this plan does not take it.

**So the fault is where the guard sits, not how wide the range is.** Task 52
moved the runtime guard from the calendar extraction (task 26's placement) down
to the producer, because that was faster. With one producer that is exactly
right. With two, the inner guard spends the entire narrow range as its promise,
and the outer shift has nothing left to spend - the analysis must then decline,
because a day outside the range would decompose wrongly and the ghost fallback
cannot catch a wrong answer.

**This is worth doing in milestone 4** rather than deferring: it is a fusion gap
in the flagship expression family, it is the shape every mixed-type chain keeps
hitting, and the milestone ends in a public post where a reader can write this
query in one line.

## 3. The design

**Guard the outermost producer instead of the innermost, when the composition
overflows.** The guard machinery from task 52 already exists and already knows
how to clamp a produced day and report `STATUS_CHRONO_RANGE`; what is new is
choosing a different node to attach it to.

`dayRange` gains a third answer beside `Bounded` and `ColumnShifted`:
a range that is out of bounds *only because a guarded producer below it has spent
its budget* is distinguishable from one that is out of bounds because a literal
shift really does leave the range. The first is guardable at the top; the
second must still decline, and `year(date_add(d, 20000000))` must keep
declining exactly as it does today.

Concretely, in `admitCalendar`'s `Bounded(lo, hi)` failing arm: if every
producer in the subtree between the column and the calendar node is one the
emitter can guard - the `AddDays`/`SubDays` column-offset form, `IRAddMonths`
with a column count, and the interval add that lowers to it - then instead of
declining, mark the calendar node's own input as guarded and emit task 52's
block there. The emitted cost is one extra pair of compares per lane group on a
shape that today runs entirely on the row engine.

Where the subtree contains an unguardable shift - a literal offset that leaves
the range on its own - the decline stands, with its existing reason.

## 4. Files

* `VarkaExpressionCompiler.scala`: `dayRange`'s result type and `admitCalendar`'s
  failing arm; the decline reason gains a second spelling for the case that is
  now guarded rather than declined.
* `VarkaLoopEmitter.java`: `Analysis.guardedProducers` gains the consumer-side
  placement; `emitAndValidatedOp`'s guard block is reused unchanged.
* `VarkaEmitOptions.java`: no new option if the placement is chosen by the
  analysis rather than by a flag - decide in 8, and prefer no new option.
* `VarkaExpressionCompilerSuite.scala`, `VarkaLoopEmitterSuite.scala`,
  `VarkaDifferentialSuite.scala`.

## 5. Tests, and what each is for

* The four shapes of section 2's table as compiler-suite assertions: the three
  that fuse today keep fusing with no byte moved, and
  `dayofyear(add_months(last_day(date_add(d, i)), 1) + ymy)` fuses where it
  declined.
* **The decline that must survive**: `year(date_add(d, 20000000))`, a literal
  shift that leaves the range on its own, still declines with its existing
  reason. This is the assertion that stops the change being written too
  aggressively, and it is the one to write first.
* `VarkaLoopEmitterSuite`: a lane whose composed day leaves the narrow range
  returns `STATUS_CHRONO_RANGE` from the consumer-side guard; a lane inside it
  returns 0; the shapes that already guarded at the producer emit identical
  bytes.
* `VarkaDifferentialSuite`: the section 1 query over a fixture whose `i` and
  `ymy` push the composed day out of range, asserting the answers match the row
  engine and the batch is declined at runtime rather than answered wrongly.
* `VarkaIrFuzzSuite` at the scale that finds composition bugs, since this
  changes which shapes reach the emitter at all.

## 6. The measurement

The shape has no committed benchmark case, and per the milestone's habit the
baseline is committed before the improvement: a `DateChainBenchmark` entry for
`dayofyear(add_months(last_day(date_add(d, i)), 1) + ymy)` - which task 62's
`Chains` list currently cannot contain, because it declines - lands with the
row engine's number, and this task moves it.

What the numbers may claim is the difference between the row engine and a
guarded kernel on this shape, which should be large; what they may **not** claim
is a whole-query speed-up for anyone not writing this shape.

## 7. Risks

1. **The guard is placed where it does not cover.** A consumer-side guard
   clamps the day the consumer sees; if a second consumer reads the same
   producer through a different path, it needs its own. The CSE machinery
   shares producers, so this is real. The fuzz suite at scale is what would find
   it, and the differential over a two-consumer shape is the targeted test.
2. **A shape that should decline now fuses and answers wrongly.** This is the
   one that matters, because the ghost fallback cannot catch a wrong answer.
   The literal-overflow assertion in 5 is the guard against it, and the
   distinction the design rests on - guarded producer below versus literal shift
   below - must be exhaustive over the IR rather than a default.
3. **It re-introduces machinery task 51 deliberately removed.** The mitigation
   is that it applies only where the alternative is declining entirely, so no
   shape that fuses today pays for it; asserted by the byte-identity test.

## 8. Sequencing

1. This plan, and the milestone row.
2. The literal-overflow assertion, failing-first against the current
   compiler - it must pass before and after.
3. The compiler half: `dayRange`'s third answer and `admitCalendar`'s arm.
4. The emitter half: the consumer-side placement.
5. The chain entry, the differential, the fuzz run.
6. Outcome here, and the entry added to `Chains`.

## 9. Outcome

To be written.
