# Task 47: one validity write per word

## 1. Where this came from

`PLAN_MILESTONE_4.md` row 47 and section 2.17, the last of the three tasks
that section opened after task 32 measured the validity write at 55.6% to
56.7% of a four-field kernel's time. Task 45 took the dense path away with a
driver-side `setValid`; task 46 gave the write a helper that inlines. 47 is
the one 2.17 said to do last, "only if 45 and 46 leave something on the
table", and three later tasks have said what they left.

**Task 76 (`PLAN_TASK_76.md` 10.6, 10.7).** The per-group read-modify-write
is measurably a *four-lane* problem. A validity group is `lanes` bits, so at
4 lanes it is half a byte and two consecutive groups read-modify-write the
same byte and serialise on it; at 8 lanes a group is exactly one byte and at
16 exactly two, and there the chain is absent. Task 76's write-count ladder
puts task 46's width-named helpers ahead at every write count at 8 and 16
lanes and behind by 4 to 7% at 4 lanes with one or two writes - and its root
cause work (10.4) found the difference is register-file traffic around that
serialised chain rather than anything about the helpers themselves. Task 76
closed as a recorded decline for that reason: "the lever is the write
itself", and tuning a helper choice inside a loop shape scheduled for
replacement is the wrong move. This task is that replacement.

**Task 70 (`PLAN_TASK_70.md` 9.5).** The bitmap pass writes whole bitmaps in
the driver for every root whose validity is a pure AND/OR of input bitmaps,
so those roots have no per-group write at all. What it left per lane group is
named there: a `Cond` root's selection OR, an `IfElse`'s blend, `make_date`'s
validity test, a pick's two reads, and the read-and-AND a range guard keeps.
Those are this task's population.

**Also task 70 (9.5, and `VarkaLoopEmitter` at the input-prologue comment).**
A second, unrelated item was assigned here in as many words: the masked
driver's per-batch null-state prologue for every referenced input, most of
which is dead in the driver itself, "which is the whole of the gap left on a
64-row batch at AVX-512 (chunk 64 at 1080.1 against 1549.6)". The liveness
pass that would remove it exists - task 70 built it for the loop bodies - and
the driver is planned with `live = null` deliberately, leaving the change
here. Section 3.4 takes it; it is a separate mechanism from the rest of this
plan and is measured separately.

**Gating.** Row 47 read "gated on task 46 and on task 44's non-aligned
lengths". Task 76 was the task-46 half and closed on 10 September, so that
half is discharged. The task-44 half is not a gate on the design below and
section 2.3 says why: this plan's word writer keeps the loop and the epilogue
as they are and never carries a live accumulator across the boundary between
them, so what task 44 later decides about splitting the epilogue cannot
invalidate it.

## 2. The admission check, to do first

**Do not write the emitter change first.** This task's whole premise is a
64-bit store where today there are byte, short or int stores, and the
repository already carries a recorded reason why that was not done: the
javadoc on `VarkaVectorSupport.validityBitsAt` says that only the
`groupBytes(lanes)` bytes a group occupies are touched, "never a fixed 64-bit
word", because addressing a whole word "would read past the end of the
bitmap near it, which is why this used to need a word-alignment bound and
left every batch under 57 rows to the scalar path". A 64-bit store has the
same hazard as the 64-bit load that rule was written against. If the check
below fails, the honest outcome is to close this task with the finding
recorded, exactly as task 69's section 2 was allowed to.

### 2.1 Is there a word to write into?

`VarkaLoopEmitter` step (2) computes `s.validityBytes = (length + 7) / 8` and
step (3) materialises each destination bitmap as a `MemorySegment` of exactly
that many bytes. A `putLong` at the last word's offset therefore throws
`IndexOutOfBoundsException` on any batch whose length is not a multiple of
64 - the segment bound is the hazard, before the allocation behind it is.

Two things to establish, in this order:

1. **The bound is ours to widen.** The size argument to `loadSegment` is a
   plan-time choice, so rounding `validityBytes` up to a multiple of 8 for
   the *destination* bitmaps is a one-line change. It is sound only if the
   underlying Arrow buffer really has those bytes.
2. **The Arrow buffer really has them.** `VarkaKernelEvaluator.allocateVector`
   builds each destination through `BaseFixedWidthVector.allocateNew(len)`,
   whose validity buffer is `(len + 7) / 8` bytes *requested* and whatever
   the allocator's rounding policy gives back. The number that matters is
   `getValidityBuffer().capacity()`, which the compaction path at
   `VarkaKernelEvaluator.scala:1387` already reads, so it is available and
   not a new dependency. Check it directly, over a ladder of lengths that
   includes the awkward ones - 1, 7, 8, 9, 63, 64, 65, 4095, 4096 - and
   assert `capacity() >= ((len + 63) / 64) * 8` for every one. Do this as a
   committed test in the evaluator's suite, not as a scratch program, because
   the answer is a property of an Arrow version this repository upgrades.

**If capacity falls short at some length**, the fallback is not to abandon
the task: allocate the destination validity buffer explicitly at the rounded
size rather than through `allocateNew`'s implicit sizing. Establish which of
the two routes is needed before designing around either.

**The source side is not in scope and must not move.** Task 46 measured the
read helper as not the cost - the masked rows sit within 0.04 ns/row of their
dense-plus-one-write A/B - and a source bitmap belongs to the input batch,
whose sizing this project does not control. Every `validityBitsAt*` call and
its byte-only addressing stays exactly as it is. This is a write-side task,
as 46 was.

### 2.2 Is the destination word written by exactly one writer?

A plain store, unlike an OR, destroys whatever else is in the word. Three
claims have to hold, and all three are checkable by reading the emitter
rather than by measuring:

1. **The bitmap is zero before the loop runs.** Step (3) of the driver calls
   `VarkaVectorSupport.zero` on every destination bitmap that is neither
   served by task 70's pass nor filled once by task 45's `setValid`, which is
   exactly the population this task writes. Confirm that a root reaching the
   per-group write always takes the `zero` branch.
2. **No two loop methods share a destination.** `groupOutputs` partitions
   *outputs*, and each output owns its own `dstValidity[o]`, so two loop
   methods never write the same bitmap. Confirm against `planSlots` rather
   than assuming it from the partitioner's shape.
3. **The bitmap pass and the per-group write never both write one output.**
   `servedByPass` decides this per output and the driver's step (3) branches
   on the same predicate, so the two are the same decision by construction.
   Confirm that it is read at both sites with the same arguments.

If any of the three fails, the word writer must OR rather than store, which
keeps the read and therefore keeps the four-lane chain - that is, the task
loses its mechanism and should be closed rather than shipped in a form that
cannot deliver what it exists to deliver.

### 2.3 Does the loop/epilogue boundary force a live accumulator across it?

It must not. The loop and the epilogue are separate *methods* (task 24), so
an accumulator live across the boundary would have to travel through a field
or a return value, and task 44 may later split the epilogue again. The design
below avoids it: the loop flushes whatever it has accumulated before it
returns, and the epilogue keeps today's `orPartialValidityBitsAt` - one
read-modify-write per batch, on the one group whose row count is not a lane
width anyway. Confirm that `loopBound` can be a non-multiple of 64 (it is
`length - length % lanes`, so a 1000-row batch at 16 lanes gives 992), since
that is what makes the flush necessary rather than optional.

## 3. The design, if the check passes

### 3.1 The option space

Three ways to spend what the check buys, and they differ in what they remove
rather than in degree. The plan does not pick one by argument; section 6
measures them against each other on the same ladder.

**Option A: store every group, read none.** Keep one store per lane group
where there is one today, and drop the read: the accumulator holds the word
so far, each group ORs its bits into it in register, and the whole 64-bit
word is stored every time. Store count unchanged, read count zero, and the
memory dependency chain that task 76 found becomes a register dependency
chain. Cheapest to build, and it is the variant that most directly tests
10.4's hypothesis, because it changes the traffic without changing the loop's
shape at all.

**Option B: store once per word.** The accumulator is stored only when a word
completes, which the emitter can test as `(i & 63) == 64 - lanes` - a compare
and a well-predicted branch per group, taken one time in four at 16 lanes and
one in sixteen at 4. Removes three stores in four at 16 lanes and fifteen in
sixteen at 4, which is what 2.17 described, at the price of a branch inside
the lane-group loop.

**Option C: an outer stride of 64 rows.** The group count per word is
`64 / lanes`, a compile-time constant, so the loop can be structured as an
outer word loop with the lane-group body emitted `64 / lanes` times inside
it. No branch and no accumulator test, and it hands C2 an unrolled body,
which is the ILP this project prefers to state rather than hope for. It also
multiplies the loop method's size by four at 16 lanes and by sixteen at 4,
against a `GROUP_BUDGET` of 16 that task 71 just re-justified on method size,
so it is very likely to be refused on those grounds - it is in the option
space to be measured and ruled out on a number, not on the prediction.

**Prediction register, before the runs.** A is expected to recover most of
the four-lane loss because the chain is the mechanism 10.4 named; B is
expected to add a little to A at 16 lanes and more at 4, where it removes
fifteen stores in sixteen; C is expected to beat both per row and to lose on
method size, and to be declined for that. If A alone recovers the four-lane
loss and B adds nothing measurable, ship A: it is the smaller change.

### 3.2 What the emitter emits

One new slot per output that keeps a per-group write - `Slots.validityAcc`,
a `long` local, allocated in the same pass that allocates `guardTmp`, only
for outputs the population test admits. `emitRootValidityOr` gains the arm
that ORs into the accumulator instead of calling the helper, and
`emitVectorLoop` gains the flush.

Whichever option wins, three properties are non-negotiable and are asserted
rather than assumed:

* **The emitted bytes for an output that does *not* keep a per-group write
  are identical to today's.** A dense value root (task 45), a root the bitmap
  pass serves (task 70), and a residual output must all produce byte-identical
  loop methods, so the blast radius is visible in a `codeSize` assertion.
* **The epilogue is unchanged.** Its group is not a lane width, its helper is
  the partial pair, and it runs once per batch.
* **`VarkaEmitOptions` gains one boolean**, `validityByWord`, so the A/B has
  two arms in one run and the shipped default is chosen from committed
  numbers rather than from the first shape measured. It follows
  `validityByWidth`'s pattern exactly, including staying out of `canonical()`
  when it holds its default so production shape hashes do not move.

### 3.3 Where the accumulator resets

The word index is `i >>> 6` and the bit position is `i & 63`. A group whose
`(i & 63) == 0` starts a new word and must not inherit the previous one's
bits. Under option A and B alike this is a clear-then-or at the word's first
group, which the emitter can hoist out of the general case only under option
C, where it knows statically which of the unrolled copies is first. Under A
and B it is a test per group; the plan does not assume it is free, and
section 6 measures A against today rather than against an idealised A.

### 3.4 The masked driver's dead null-state prologue

Separate mechanism, same task, per `PLAN_TASK_70.md` 9.5. The masked driver
derives `hasNulls[i]`, `srcValSeg[i]` and `srcSeg[i]` for every referenced
input, and reads them only inside `emitLaneGroup` and `emitValue` - which
only a loop or epilogue body calls. In the driver they are written and never
read. `dead[i]` is read by the all-null shortcut alone and is dead too on any
shape that emits no shortcut. Task 70 built the liveness pass that would
remove all of it and deliberately planned the driver with `live = null`.

The change is to plan the driver with a liveness set of its own rather than
with `null`, computed from what the driver's own body reads. It is worth its
own A/B because its population is different from the rest of this task's: it
pays per *batch*, so it is invisible on a 4096-row benchmark row and is the
whole of the remaining gap on a 64-row one (chunk 64 at 1080.1 against the
dense 1549.6 at AVX-512). Measure it on the short-batch rows task 70 added,
not on the standard ones.

## 4. Tests

* `VarkaLoopEmitterSuite`: for each of 4, 8 and 16 lanes, a masked shape with
  a per-group write, run over batch lengths 1, 7, 8, 63, 64, 65, 127, 128 and
  4096, with the resulting bitmap compared bit for bit against the same shape
  under `validityByWord=false`. The lengths are the point - this is the task
  whose failure mode is a word that runs off the end or a partial word left
  unflushed, and both are length-dependent and silent.
* The byte-identity assertions of 3.2: a dense root, a pass-served root and a
  residual output emit the same `codeSize` under both arms.
* `VarkaVectorSupportTest` (or the width test beside it): the capacity ladder
  of 2.1, as a committed assertion about Arrow's allocation rather than a
  comment about it.
* `VarkaDifferentialSuite`: a masked shape from task 70's leftover
  population - a filter's `Cond` root and an `IfElse` blend - over a batch
  whose length is deliberately not a multiple of 64, checked against the row
  engine. A wrong bit here is a wrong answer, not a slow one.
* The fuzz corpus at both widths, since the population is defined by a
  predicate over shapes and the fuzzer is what finds a shape the predicate
  admits by accident.

## 5. Verification

    build/sbt catalyst/Test/compile sql/Test/compile
    build/sbt 'catalyst/testOnly *Varka*' 'sql/testOnly *Varka*'
    JAVA_OPTS="-XX:MaxVectorSize=16" build/sbt 'catalyst/testOnly *Varka*'
    JAVA_OPTS="-XX:MaxVectorSize=16" build/sbt 'sql/testOnly *Varka*'
    dev/lint-java && dev/scalastyle && build/sbt catalyst/doc
    dev/varka_gate.sh
    dev/varka_precommit.sh --working-tree

Task-specific gate: with `validityByWord` off, every emitted method is
byte-identical to master's, asserted rather than eyeballed. That is what
makes the option's default the only thing the reviewer has to judge.

## 6. The measurement

**On task 76's ladder, because it was left for this.** `PLAN_TASK_76.md` 10.9
committed the write-count ladder with its case ids (962-969) precisely so
this task could measure against the same rungs. Run it at 4, 8 and 16 lanes,
four runs per width, arms adjacent in one run, as 76 did - the same protocol,
so the two tables can be read against each other.

The number that decides the task is the **4-lane, one-write and two-write
rungs**, where task 76 measured the width-named helpers 3.8 to 6.8% and 0.5
to 1.5% *behind*. If this task's writer removes the regime, that deficit
should disappear rather than shrink, and the width-named helpers should win
at 4 lanes as they do at 8 and 16. Register that as the primary prediction.

Secondary, on the committed parity file: the masked rows of the shapes task
70 left with a per-group write, at both widths. And separately, on task 70's
short-batch rows, the driver change of 3.4.

**The band, not a flat 3%.** `dev/varka_bench_band.py` and the committed band
files exist since task 77; a rung that moves inside its band has not moved.
Compare with `dev/varka_bench_diff.py --band`.

## 7. Risks

1. **The store is wider than the buffer.** Section 2.1 is exactly this, and
   it is checked before anything is designed around it.
2. **A partial word is left unflushed on some path.** The loop's early
   returns - the all-null shortcut, the guard's status return - are paths out
   of the loop method, and each one has to flush or provably have nothing to
   flush. Enumerate them from `emitStatusReturn` and the shortcut rather than
   from memory; the length ladder in section 4 is what catches a missed one.
3. **The accumulator collides with the register pressure the task is trying
   to relieve.** One extra live `long` per written output, in a body that
   task 50 already made visible as register-allocation-sensitive. This is a
   reason to measure option A first: it changes traffic without changing the
   loop's shape, so a regression there is attributable.
4. **The four-lane win does not appear.** 10.4 states the register-file
   finding as the leading hypothesis and not as proof - "nothing here
   measures spill traffic directly". If the ladder's 4-lane deficit survives
   this change, the mechanism was something else, and that is a finding worth
   the task even though it is not the one intended.

## 8. Sequencing

1. Section 2's admission check, as its own commit: the capacity ladder test
   and the three single-writer confirmations. If it fails, stop and record.
2. `validityByWord` and option A, with the length-ladder tests.
3. The ladder run at three widths; A against today.
4. Option B behind the same flag if A leaves something; measure. Option C
   only if B's numbers make the method-size question worth asking.
5. The driver's liveness set (3.4), measured on the short-batch rows.
6. Default chosen from the committed numbers, docs, `SKILLS.md`, the
   milestone row, this plan's outcome section.

## 9. Explicitly out of scope

The read side and every `validityBitsAt*` helper; the source bitmaps' sizing;
the epilogue's own write; task 44's decision about splitting the epilogue;
task 46's helper choice, which task 76 declined and which this task's writer
is expected to make moot rather than revisit; and any change to what
`servedByPass` or `fillsValidityOnce` decide - this task changes how the
remaining writes are performed, never which roots perform them.
