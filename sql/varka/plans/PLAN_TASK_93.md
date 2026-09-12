# Task 93: re-arm the guard wherever the range runs out, so guarded shifts compose

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

**The code already says this, and says why the obvious fix is wrong.**
`VarkaExpressionCompiler.scala:1438-1444`, on `columnShifted`:

> Saying so here is what makes the guarantee compose: a further shift widens
> this interval and `admitCalendar` tests the widened one, where treating the
> subtree as unbounded-but-guarded would let a shift above the producer carry
> the day back out of the range with nothing left to catch it.

That is the trap this task must not fall into. Declaring a guarded subtree
"guarded, therefore fine" and stopping the interval arithmetic there would admit
exactly the shapes that are wrong. The design below keeps the interval
arithmetic in full and adds only the power to *re-arm* the guard - which resets
the interval to a known one rather than abandoning it.

**This is worth doing in milestone 4** rather than deferring: it is a fusion gap
in the flagship expression family, it is the shape every mixed-type chain keeps
hitting, and the milestone ends in a public post where a reader can write this
query in one line.

## 3. The design

### 3.1 The rule: re-arm, do not relocate

The interval walk stays exactly as it is. What it gains is one move: **at a node
whose running interval would leave the decomposable range, emit the guard on
that node's value and reset the interval to `[NARROW_MIN_DAYS,
NARROW_MAX_DAYS]`.** Everything above continues from the reset interval.

This is not "guard the outermost producer", which the first draft of this plan
said and which is under-specified: there can be more than one overflow point.
Measured, with `dev/varka_emit.sh --table`:

| shape | today |
|---|---|
| `dayofyear(d + ymy + ymm)` | **fuses**, 267 ops - two interval columns, no column day offset, and the contract range absorbs both |
| `dayofyear(last_day(date_add(d, i) + ymy) + ymm)` | declines |

In the second, one re-arm after `+ ymy` clamps back to the narrow range, then
`last_day` adds `[0, 30]` and `+ ymm` adds another `+/- 761484` - out again, and
a second re-arm is needed. A rule phrased around a single node cannot express
that; a rule phrased as "re-arm whenever the interval runs out" expresses it
without saying how many times.

Because every consumer above a re-arm sees an interval that starts from the
narrow range, the decomposing nodes on the way up - `IRLastDay`, `IRAddMonths`
and every calendar extraction, all of which decompose their own input - are
in range by construction rather than by a separate argument.

### 3.2 Where it re-arms, and where it must still decline

The distinction is **not** whether the emitter *can* guard a value: it can guard
any date-lane vector, since the guard is two compares and a mask. It is whether
guarding would be anything but a slow decline.

* **Overflow contributed by a runtime value** - a column day offset, a column
  month count, an interval column - re-arms. Most batches are in range, so most
  batches fuse, and the ones that are not were going to fall back anyway.
* **Overflow contributed by a literal** - `year(date_add(d, 20000000))` - still
  declines at compile time, with its existing reason. Guarding it would emit a
  kernel that declines every batch: strictly worse than declining once, at
  compile time, for free.

So the failing arm of `admitCalendar` asks which of the two produced the
overflow, and that question is answered by the walk itself, since it already
knows whether each contribution came from a `LiteralSlot` or from a column.

### 3.3 What actually changes, against the code as it is

`DayRange` today is `sealed trait DayRange` with **`Bounded(lo, hi)` and
`Unknown`, and nothing else**. The `ColumnShifted` of `PLAN_TASK_52.md` 3.1 was
design vocabulary that the implementation collapsed: `columnShifted()` is a
local helper returning `Bounded(NARROW_MIN_DAYS, NARROW_MAX_DAYS)`, and the
`guarded` parameter carries what the third case used to. The first draft of this
plan proposed "a third answer beside `Bounded` and `ColumnShifted`" and was
wrong about both the count and the names.

The change builds on the vocabulary that is there rather than adding a parallel
one:

* `dayRange` already threads `guarded`, and `shifted(..., guardsBelow = true)`
  already means "a guard is armed below this node". The walk gains an
  accumulator beside its interval: the set of nodes where it re-armed, and
  whether the overflow that forced each was literal or runtime.
* `admitCalendar`'s failing `Bounded(lo, hi)` arm consults that set: non-empty
  and all-runtime means admit and hand the set to the emitter; anything else
  declines exactly as today.
* `VarkaLoopEmitter`'s `Analysis.guardedProducers` becomes the set the compiler
  computed rather than one the emitter re-derives, which also removes the
  standing risk that the two disagree - the defect `PLAN_TASK_73.md` 3 describes
  in the same family.

### 3.4 No new emit option

`VarkaEmitOptions.guardDayProducers` already gates the whole mechanism, and a
second flag would multiply the shape-cache key for a placement the analysis
decides rather than the caller. Re-arming rides on the existing flag; with it
off, the shapes that need a re-arm decline as they do today.

### 3.5 Its relation to task 84

`PLAN_MILESTONE_5.md` 2.15 rebuilds `dayRange` and `intBound` as one value-range
lattice, and the compiler's own comment at :1387 already sends this seam there:
"question task 84's value-range lattice answers for `dayRange` and `intBound`".
The re-arm set is a property that lattice would carry natively, so if 84 lands
first this task is a query on it, and if 93 lands first, 84 inherits the set
rather than inventing a second copy. `PLAN_TASK_73.md` 4.4 set this convention
for the third analysis in the family; this is the fourth.

## 4. Files

* `VarkaExpressionCompiler.scala`: the walk's re-arm accumulator, and
  `admitCalendar`'s failing arm. The decline reason splits in two - the existing
  wording where a literal shift leaves the range, and no decline at all where a
  runtime one does.
* `VarkaLoopEmitter.java`: `Analysis.guardedProducers` reads the compiler's set
  instead of re-deriving one; `emitAndValidatedOp`'s guard block is reused
  unchanged at the nodes that set names.
* `VarkaEmitOptions.java`: **unchanged**, per 3.4.
* `VarkaExpressionCompilerSuite.scala`, `VarkaLoopEmitterSuite.scala`,
  `VarkaDifferentialSuite.scala`, `VarkaIrFuzzSuite` fixtures.
* `Chains.java`: the entry, once it fuses - see 6.

## 5. Tests, and what each is for

Written against the shapes measured in 2 and 3.1, so each test names a verdict
that exists today rather than one this plan hopes for.

* **The decline that must survive**, written first and passing before any
  change: `year(date_add(d, 20000000))` still declines with its existing reason.
  A literal shift that leaves the range on its own is not re-armable, and this
  is the assertion that keeps 3.2's split honest.
* **The shapes that fuse today keep fusing, byte for byte**:
  `year(date_add(d, i))`, `dayofyear(add_months(last_day(date_add(d, i)), 1))`
  at 218 ops, `weekofyear(add_months(d, i) + ymm)` at 288, and
  `dayofyear(d + ymy + ymm)` at 267. None of these needs a re-arm, and none may
  gain one.
* **One re-arm**: `dayofyear(add_months(last_day(date_add(d, i)), 1) + ymy)`,
  section 1's shape, fuses where it declined.
* **Two re-arms**: `dayofyear(last_day(date_add(d, i) + ymy) + ymm)`, which is
  what makes the rule's plurality testable rather than rhetorical. A design that
  re-arms once admits this shape and computes it wrongly, so this test is the
  difference between the first draft of section 3 and this one.
* **Runtime behaviour** (`VarkaLoopEmitterSuite`): a lane whose composed day
  leaves the range returns `STATUS_CHRONO_RANGE`; a lane inside it returns 0;
  a null in either the offset or the interval column returns 0.
* **Two consumers, one producer** (risk 2): `year(p)` and `datediff(p, d2)` over
  a shared re-armed `p`, asserting one guard is emitted rather than two, and
  recording what the `datediff` side pays for a guard it does not need.
* `VarkaDifferentialSuite`: section 1's query over a fixture whose `i` and `ymy`
  push the composed day out of range, asserting the answers match the row engine
  and that the out-of-range batches are declined rather than answered.
* `VarkaIrFuzzSuite` at the scale that finds composition bugs - this changes
  which shapes reach the emitter at all, which is exactly what the fuzzer is
  for, and task 73 records that the shipped budget of 300 iterations is too
  small to reach shapes of this family.

## 6. The measurement

The shape has no committed benchmark case, and the milestone's habit is that the
baseline is committed before the improvement. The first draft said to add the
entry to `Chains` first, which cannot be done as written: every `Chains` entry is
built by `Surface.Entry.projection`, which sets `expectFused = true`, and the
driver's `--expect-fused` fails the whole run when a plan comes back residual.

`Surface.Entry` is a four-field record and its last field is exactly this, so the
baseline entry is constructed directly rather than through the factory:

    new Surface.Entry(expr, expr, null, false)   // expectFused = false, for now

with a comment naming this task. That run commits the row engine's number for the
shape. When the task lands, the flag flips to `true` and the same row is
re-measured, so the before and after are the same entry in the same file rather
than two files a reader has to align.

What the numbers may claim is the difference between the row engine and a guarded
kernel on this shape, which should be large. What they may **not** claim is a
whole-query speed-up for anyone not writing it.

## 7. Risks

1. **A shape that should decline now fuses and answers wrongly.** The one that
   matters, because the ghost fallback cannot catch a wrong answer. The
   literal-versus-runtime split of 3.2 is the whole of the defence, and it must
   be exhaustive over the IR rather than a `default` arm - a node type added
   later must be a compile error, not a silent admission. The
   `year(date_add(d, 20000000))` assertion is sequenced before any change.
2. **Shared producers under CSE.** The guard attaches to a *node's value*, and
   CSE shares the node, so one re-arm covers every consumer of it - the
   correctness direction is safe by construction. The cost direction is not: a
   node shared between a decomposing consumer, which needs the re-arm, and a
   `datediff`, which does not, makes the second pay for the first. That is a
   measurement to take, not a correctness risk, and it is why 5 includes a
   two-consumer shape.
3. **The re-arm never terminates.** Re-arming resets the interval, so a
   pathological tree could in principle re-arm at every level. It cannot loop -
   the walk is over a finite tree and each node re-arms at most once - but a
   deep chain could emit many guards. The budget (`GROUP_BUDGET`) already bounds
   the method; the plan records the emitted op count for the section 1 shape as
   the number to watch.
4. **It re-introduces machinery task 51 removed.** Mitigated by applying only
   where the alternative is declining entirely, so nothing that fuses today pays
   for it - asserted by the byte-identity test.

## 8. Sequencing

1. This plan, and the milestone row.
2. The `year(date_add(d, 20000000))` assertion, passing against the compiler as
   it is - it must pass before and after, and it is the one that stops the
   change being written too aggressively.
3. The baseline `Chains` entry with `expectFused = false`, and its committed run.
4. The compiler half: the re-arm accumulator and `admitCalendar`'s arm.
5. The emitter half: `guardedProducers` from the compiler's set.
6. The differential, the two-consumer cost measurement, and a fuzz run at the
   scale that finds composition bugs.
7. The entry's flag flipped to `true`, re-measured, and the outcome written here.

## 9. Outcome

To be written.
