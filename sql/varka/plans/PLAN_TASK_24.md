# Task 24: the scalar tail, mask interrogation, compaction

Milestone 4's first task, and the reason it is first (`PLAN_MILESTONE_4.md`
section 2.1, task row 24, catalogue item 11): the emitter's remainder handling
was a *second full walk of the IR*, so every node type tasks 26-30 add would
otherwise have been written twice - once as vector bytecode, once as scalar -
and kept in agreement row for row. The gate is the standing one, with the
pinned oracles doing unusual duty: this task changes how the emitted class is
structured without changing what it computes, so both committed shape hashes
and the committed line map must come out byte-identical (section 5).

## 1. Decisions, and who made them

Settled with the owner during planning, before any code:

1. **One epilogue method per density.** `epilogueDense` and `epilogueMasked`
   replace `tailDense` and `tailMasked` one for one: same method count, same
   driver call sequence, `GROUP_BUDGET` untouched. The epilogue runs one pass
   per batch, so the budget - which exists to keep a *hot* method's C2 compile
   cheap - has nothing to bound there, and the tail it replaces was already one
   method covering every output. Rejected: an epilogue per output group (turns
   one cold method into N and emits the vector walk twice per group), and
   appending the epilogue to each hot loop method (doubles each hot method's op
   count against `GROUP_BUDGET = 16`, the one option that touches the hot path).
2. **The `anyTrue`/`allTrue` deliverable lands at batch level first** - the
   filter's `count == 0` and `count == len` paths, where measurable work
   disappears. The per-lane-group version is measured and then shipped or
   declined with a task-16 reason.
3. **`DateVectorOps` converts too.** The hand-written kernels are the reference
   later lane types are copied from, so the idiom is written there as well as
   generated.

## 2. What the scalar tail actually cost

Milestone 4's open question 3, and the milestone plan required it answered
*before* the emitter work. Answering it meant building the measurement, not
just running one:

**No committed harness in this project had ever executed a scalar tail row.**
`VarkaEmitterParityBenchmark` ran one call over 1,000,000 rows;
`DateVectorOpsBenchmark`'s `@Param` sizes are 32, 10000 and 1000000; Spark's
default `COLUMN_BATCH_SIZE` is 4096. Every one of those is a multiple of 4, 8
and 16, so `loopBound == length` at every lane count this engine runs at and
the tail was dead code under measurement. Being lane-aligned by accident is the
kind of thing a harness does not tell you, which is why it is written down here
and in `SKILLS.md`.

The new `batch-length alignment` section of `VarkaEmitterParityBenchmark`
drives the same total row count through aligned and unaligned chunks at two
chunk sizes. Within a pair the call counts match, so the difference is the tail
and nothing else; between the pairs the call count rises 64-fold, so the
difference prices the per-call prologue. Committed numbers, best times, one run:

| shape | aligned | unaligned (lanes-1 tail rows) | delta per call | per tail row |
|---|---|---|---|---|
| depth-4 chain, 4096/4095, null-free | 199.0 ns/call | 222.7 | 23.6 ns | 1.58 ns |
| depth-4 chain, 4096/4095, mixed nulls | 265.5 | 285.0 | 19.5 ns | 1.30 ns |
| depth-4 chain, 64/63, null-free | 14.7 | 34.0 | 19.3 ns | 1.29 ns |
| depth-4 chain, 64/63, mixed nulls | 17.4 | 37.0 | 19.6 ns | 1.31 ns |
| `dayofweek`, 64/63, null-free | 16.5 | 47.0 | 30.5 ns | 2.03 ns |
| `dayofweek`, 64/63, mixed nulls | 17.7 | 54.7 | 37.0 ns | 2.47 ns |

The cross-check is that the **delta is the same whether the batch is 4096 rows
or 64** - 19-24 ns for the chain either way - which is what a fixed
per-partial-batch cost looks like and what a measurement artefact does not.

So the answer, in three registers:

* **A scalar tail row costs 1.3-1.6 ns** on the depth-4 chain and 2.0-2.5 ns on
  `dayofweek`, where the vector loop costs 0.73 ns per *lane group* of 16 rows.
* **A partial batch pays 19-24 ns for its tail** (30-37 ns for `dayofweek`),
  which is **~11%** of a 4096-row batch's kernel time and **~135%** of a
  64-row batch's - and **0%** of any lane-aligned batch, which the default
  4096 always is.
* **End to end it is nothing.** `VarkaFilterBenchmark` and
  `VarkaThroughputBenchmark` measure 5-25 ns/row once Arrow access and the row
  boundary are included, so a 4096-row batch takes 20-100 microseconds and the
  tail is 0.02-0.1% of it.

The same section prices the per-call fixed cost at **11.8 ns** - segment
wrapping, species read, `loopBound`, the literal hoist, the dispatch, and the
call into a tail method that then re-does all of it before discovering it has
no rows. At 4096 rows that is 0.003 ns/row.

**The honest conclusion, which the milestone plan predicted: this task is not a
speed change.** The tail is free on aligned batches and cheap on unaligned
ones. The case is entirely the emitter's per-node surface, and it is made in
section 4 as a number rather than an adjective.

One caveat on the run, recorded rather than hidden: regenerating this file
moved the pre-existing sections by 10-30% against their previously committed
values in both directions, on a machine that was quiet but not dedicated. The
conclusions above rest on within-run ratios of 2.2x-3.1x, not on cross-run
absolutes, and the two independent chunk ladders agree to within 0.3 ns on the
derived per-row cost.

## 3. Predictions, registered before the measurements

Written into the approved plan before any number was taken; scored in
section 6.

1. The tail's runtime share is ~0 at aligned batch sizes and under ~2-4% at
   unaligned ones; the case for the change is emitter surface, not speed.
2. Emitted class size changes by less than +-20% for most shapes and no shape
   grows more than +50%.
3. `compress` recovers less than the ~1-3 ns/row ceiling task 21 committed,
   because it makes the copy faster rather than removing it: predict 1-2 ns/row
   at 100% selectivity, so the compacting rung moves from 2.7x to at most
   ~3.2x, and possibly not at all outside AVX-512.
4. The `count == len` forwarding path is worth more at that rung than
   `compress` is, because it removes the copy entirely.
5. No engine JMH number moves, every committed size being lane-aligned.

## 4. The masked epilogue

The insight that kept this small: the epilogue is the existing vector body with
three substitutions, not new code.

| | loop method | epilogue method |
|---|---|---|
| `i` | `for (i = 0; i < loopBound; i += lanes)` | `i = loopBound`, one pass, guarded by `loopBound < length` |
| lane count | `species.length()` | `length - loopBound` |
| loads and stores | unmasked | masked with `species.indexInRange(loopBound, length)` |

So `emitVectorLoop`'s body was lifted into `emitLaneGroup`, which the loop
calls per iteration and the epilogue calls once. Everything between a load and
a store stays unmasked, exactly as in the loop, and the masked overloads are
selected by one nullable slot (`Slots.epilogueMask`) rather than by a flag
threaded through the walk.

The masked load is required, not preferred: the data segment is sized to
`length * 4`, so an unmasked load of the last partial group would leave the
segment. Its consequence is now an invariant in the emitter's class doc -
**inactive lanes read `0`, so no operation in the walk may trap on `0`**. It
holds for free today (the mod-7 lowerings divide by the constant 7; add, sub,
compare, blend, max, min and the shifts are total) and task 30's ANSI division
is the first thing that will break it.

### The bug the design missed, and what it cost

Planned as "`lanes` becomes the remainder". That is wrong, and wrong silently.
`VarkaVectorSupport.validityBitsAt` and `orValidityBitsAt` take a lane
*width*, not a row count: the width decides how many bytes the access spans,
through `groupBytes(lanes) = max(1, lanes / 8)` and a switch over 1, 2, 4 and 8
bytes. A nine-row group asks for `groupBytes(9) == 1`, so the helper reads one
byte and reports the ninth row null. `VarkaLoopEmitterSuite` caught it
immediately at `length=9` - the boundary-straddling `lengths` list doing
exactly the job it exists for.

The write side is worse than a wrong answer. A whole-group write at the last
group can run off the bitmap: at `length = 17` the bitmap is three bytes and a
16-lane group at row 16 stores a short across bytes 2 and 3.

So the task added the partial-group pair -
`VarkaVectorSupport.partialValidityBitsAt` and `orPartialValidityBitsAt` -
which walk the `(row % 8 + rows + 7) / 8` bytes the group actually spans, and
the emitter picks between the pairs on `epilogueMask != null`. Keeping them
separate rather than generalizing the originals is deliberate: the whole-group
form is called once per lane group in the hot loop, and a byte loop there would
be paid for a cold path's convenience.

### What it bought

* `VarkaLoopEmitter.java`: **2110 lines to 1914** (+170, -366). One of the five
  per-node switches over the sealed IR hierarchy is gone, which is the number
  that matters for tasks 26-30: a new node type is now written once.
* Emitted class size, seven shapes, before and after:

| shape | before | after | delta |
|---|---|---|---|
| `addDays` | 4292 | 4386 | +2.2% |
| depth-4 chain | 4855 | 4903 | +1.0% |
| depth-16 chain | 6931 | 7027 | +1.4% |
| `dayofweek` | 5171 | 5224 | +1.0% |
| filter `Compare` root | 4341 | 4267 | -1.7% |
| every node type | 8215 | 8031 | -2.2% |
| 4 x depth-16 (64 ops) | 21574 | 21817 | +1.1% |

  Prediction 2 said +-20% with no shape past +50%; the answer is -2.2% to
  +2.2%, and the direction is the predicted one - narrow shapes grow a little,
  node-rich shapes shrink, because the scalar walk cost grew with node count
  while one masked vector body does not.

## 5. What did not move: the milestone plan corrected

`PLAN_MILESTONE_4.md` section 2.1 says task 24 "changes emitted bytes, so the
two pinned shape hashes and the pinned line-map literal move, and are
regenerated under their own update rule - the one task in the spine where that
is expected rather than alarming."

**That is wrong, and the correction matters more than the error.**
`VarkaShapeCacheImpl.shapeHash` is SHA-256 over `VarkaVectorIR.canonical` of
the outputs plus `numInputs|numLiterals` plus `options.canonical()`, which is
empty for `DEFAULTS`. This task adds no IR node and no emit option, so
`586434f9b9739c40` and `612c94d132690dc2` cannot move. The line map renders
`analysis.topoOrder` through `canonicalShallow`, a property of the IR and its
schedule rather than of how many bodies consume it, so `pinnedLineMap` cannot
move either.

Both held, untouched. That makes them this task's **behaviour-preservation
proof** rather than its collateral - the same role they played in task 23. A
change that deletes one of the emitter's two lowerings of every node type and
leaves both committed hashes byte-identical has demonstrated something; if
either had moved, that would have been a bug, not a regeneration.

The milestone file is corrected in place, per the rule that a plan is a record:
the sentence is rewritten with what was actually found, not deleted.
