# Testing and debugging

How a wrong answer here is caught, and the fixtures and oracles that decide what a test can catch.

One of Varka's lesson files; the index over all of them is
[`SKILLS.md`](../../../SKILLS.md) at the repository root, which is generated from
these files by `dev/varka_toc.py`.

## Buffer-Reuse Aliasing (UnsafeProjection)

- `UnsafeProjection` (and `GenerateUnsafeProjection`) reuses a single output
  `UnsafeRow` buffer across calls. Materializing results into an `Array` without
  copying yields an array whose elements all alias the last row's buffer: every
  output shows the last value.
- The copy belongs to whoever *materializes*, not to the operator. Spark operators
  stream reused rows deliberately: `ColumnarToRowEvaluatorFactory` is
  `input.rowIterator().asScala.map(toUnsafe)`, with no copy. `VarkaColumnarToRowExec`
  copied per row on both its kernel and fallback paths until finding 9 removed it; that
  cost an `UnsafeRow` allocation plus a memcpy per row on the hot path and bought a
  guarantee the standard path never gave. (Earlier revisions of this file prescribed the
  copy, from when the evaluator materialized with `process(...).toArray`; it streams now.)
- `QueryExecution.toRdd`'s scaladoc states the contract and names `collect()` as "one of
  known bad usage" - `RDD.collect` is `iter.toArray` per partition, so it aliases. Use
  `Dataset.collect` (which serializes each row as it iterates), or, in a test that wants
  `InternalRow`s, `toRdd.map(_.copy()).collect()`.
- Not a Varka rule: collecting a plain row-engine query that way returns 2 distinct row
  objects for 5 rows, with wrong values. `SparkContext.hadoopRDD`'s scaladoc documents
  the same hazard for reused Hadoop `Writable`s.
- Corollary for tests: a suite that materializes rows can pass for the wrong reason while
  an operator copies. `VarkaDifferentialSuite` did, and only failed once the operator
  stopped - the copy had been masking an unsound `toRdd.collect()` in the test itself.

## Alias Unwrap Is Needed at Two Layers

- A projection list is `Alias(expr, name)` at the top level, and after
  `BindReferences` the bound expression is `Alias(BoundReference(...), ...)`.
- Any code matching projection expressions against concrete types must unwrap `Alias`
  twice: once on the unbound list (`eligibleOps`) and once on the bound list
  (`buildOutputPlan`). Missing either makes eligibility silently fail (kernel never
  runs) or the plan match return `None`.
- Symptom: with both missing, fallback covers it; the kernel path never executes. The
  rule test and the `numVarkaBatches` metric reveal it once rows are correct.

## Masked Bugs

- A fallback path that returns *wrong* results can hide an entirely untouched kernel
  path. Here the "kernel results" seen early on were actually the fallback (with the
  aliasing bug). The real kernel only ran once the Alias unwraps landed, which is
  also when the stub trap surfaced. Two independent bugs masked each other.

## Debugging Method: Progressive Isolation

1. Verify the mechanism in isolation (e.g. a manual `MemorySegment.ofAddress(...).set`
   write that Arrow can read).
2. Verify the middle layer directly (call the kernel straight on Arrow buffers).
3. Instrument the deepest layer (prints inside the kernel). Prints not appearing =
   wrong class on the classpath.
Each step either pins the fault or narrows it.

## Columnar Transition Wiring (plan level)

- `ApplyColumnarRulesAndInsertTransitions` runs `preColumnarTransitions`, then
  `insertTransitions`, then `postColumnarTransitions`. A `postColumnarTransitions`
  rule sees the transitions already inserted.
- `ensureOutputsRowBased` gives a dual-mode plan (`supportsRowBased` and
  `supportsColumnar`, e.g. `InMemoryTableScanExec`) row output when its parent
  consumes rows, so above a cached scan there is often *no* `ColumnarToRowExec` to
  pattern-match on. Fuse on `child.supportsColumnar` (and switch the dual-mode
  child to columnar output), not on the presence of a transition.
- `ColumnarToRowExec` is row-only: it has no `doExecuteColumnar`, so calling
  `executeColumnar()` on it throws. A fusion node that consumes a columnar child
  must absorb the transition (`case ColumnarToRowExec(inner) => inner`) instead of
  wrapping it.
- The `ColumnarToRowTransition` tag is read by some machinery as "semantics-free
  row conversion" - `CachedBatchSerializer.convertToColumnarPlanIfPossible` strips
  a topmost transition and executes its *child* to get columnar cache input. A
  fused node wearing the tag (every Varka `*ColumnarToRowExec`) carries real work
  inside it, so every tag consumer that strips must instead convert the fused node
  to its columnar sibling (identical kernels, columnar out) - the Arrow serializer
  override does. Found in task 21 as a wrong-cached-view bug latent since task 6:
  every direct query stays right, and only a *cached* view materializes the
  dropped work. When adding a fused transition node, grep the tag's consumers.

## Metrics as the "did it really run" proof

- A fused plan plus correct results does not prove the kernels ran: the per-batch
  fallback also returns correct results. Prove execution with a metric
  (`numVarkaBatches`) bumped only on the kernel path.
- Read metrics *after* execution: run `checkAnswer`/`collect` first, then read.
  Reading before execution returns 0 even though every guard passed.
- The executed-plan root is often a `WholeStageCodegenExec` whose `metrics` map
  only has `pipelineTime`. Read the node's own metric via
  `plan.collectFirst { case v: VarkaColumnarToRowExec => v }`, not `plan.metrics`.

## Independent Reference Evaluators as Test Oracles

- For an algebraic surface (three-valued logic, null-skipping picks, blend
  semantics), implement the semantics *twice*: the generated code, and a tiny
  interpreter over the same IR inside the test suite (`Option[Int]` values,
  `Option[Boolean]` Kleene conditions). Run matrices against it row for row. Wrong-
  in-the-same-way bugs are unlikely across two representations that share nothing.
- A fold's *association* is not its *effect order*: a monadic `foldRight` over
  CASE branches evaluated the ELSE first and registered input ordinals right-to-left.
  Where side effects assign identities (ordinals, slots), compile in source order
  explicitly, then fold the already-compiled pieces.

## A fixture that fills undefined memory decides what its whole matrix can catch

- Arrow leaves the data under a null slot undefined, and the emitted loop loads every
  column unmasked, so what a test harness writes there is what stands between a
  lowering's garbage and its answer. For most of milestone 4 the suites wrote the
  *drawn value* into a null lane and `VarkaIrFuzzSuite` still did in September 2026 -
  values bounded by `columnBound` and `MONTH_ARITH_MAX_MONTHS` by construction, so a
  null lane could never reach a range guard's condemning comparison. Every guard since
  task 42 ANDs its mask with the row's validity word; nothing in either suite could
  have failed if one of them stopped. Poisoning null lanes with `Int.MinValue` and
  `Int.MaxValue` is what makes that AND load-bearing, and it costs nothing: the whole
  matrix passed unchanged at both widths the day it went in, which is the evidence
  that the ANDs are all there, not merely that the tests are green.
- Key the alternation on the *null ordinal*, never on the row index. The null patterns
  are themselves index predicates - `i % 2 == 1` is one of them - so an `i & 1` poison
  silently writes one extreme in every null lane of that pattern, and a quarter of the
  matrix only ever probes one side of every bound. Bounds here are asymmetric
  (`MAKE_DATE_MIN_YEAR` against `MAKE_DATE_MAX_YEAR`; task 69 widens `dayRange`
  upward only), so half a guard can go missing under a green suite.
- And poison only the slots the caller did not choose. A guard test that pins a
  boundary value at a lane it also marks null is testing that exact value; substituting
  an extreme kept it passing while quietly turning one case into a duplicate of the one
  beside it. `makeInputData`'s `poisonNulls = false` is for those, and the reason is in
  its javadoc so the next person does not undo it.

## Testing Under AQE

- Every Varka suite session disables AQE for plan determinism, which silently leaves
  the default-config path (AQE on) unpinned. It worked - but only an experiment
  proved it.
- Under AQE the fused node sits inside a query stage, and a query stage is a *leaf*:
  `SparkPlan.collect`/`collectFirst` never descend into it, so a naive assertion
  reports "not fused" while the node is right there in `treeString`. Traverse with
  `AdaptiveSparkPlanHelper` in AQE tests.

## A closed `ArrowBuf` still answers `capacity()` and `memoryAddress()`

Found reviewing task 59's per-task scratch buffers, and worth knowing before writing any
grow-and-reuse helper over Arrow memory.

`ArrowBuf.close()` is one line - `referenceManager.release()`. It does not touch the buffer's
own fields, and both `capacity()` and `memoryAddress()` are plain field reads (`getfield
capacity:J` and `getfield addr:J` in arrow-memory-core 19.0.0; check with `javap -c` rather
than assuming, the class is small). So a closed buffer reports the size it used to have and the
address it used to own, and a `capacity() < needed` test - the natural way to decide whether to
grow - cannot tell a live buffer from a freed one. There is no cheap liveness predicate to
substitute: the reference count lives in the ledger, not the buffer.

The consequence for a helper that replaces a buffer: **allocate the new one before closing the
old one**, never the reverse. `BufferAllocator.buffer` throws Arrow's `OutOfMemoryException`,
which is a plain `RuntimeException` - not `java.lang.OutOfMemoryError` - so `NonFatal` matches
it, `serveBatch` catches it as a per-batch failure and *the task keeps running*. Free first and
the throw leaves the caller's field pointing at a freed buffer, because the assignment that
would have replaced it never happens: the right-hand side is evaluated first. The next smaller
batch then reads the stale capacity, decides no grow is needed, and writes through a released
address; the task's cleanup closes it again and the ledger's reference count goes negative.
Allocating first costs both buffers for the width of one assignment and cannot strand a freed
one. The same ordering argument applies to any resource whose accessors survive its release.

Two things this cost that are worth copying rather than rediscovering. The regression test
cannot assert on the corruption - freed memory usually still holds its old contents, so reading
it back returns the right answer and proves nothing; assert on the allocator's own accounting
instead (`getAllocatedMemory` unchanged across the failed grow), which is exactly the invariant
and is a public API. And a release path that catches and logs, which is the right thing for a
task-completion listener, will swallow the double close that would otherwise have made the bug
loud - so hardening the cleanup and fixing the ordering have to be judged separately, or the
hardening hides the evidence for the fix.

Two more from the sweep that followed it.

**One `try` around a cleanup sequence satisfies the letter of "do not skip the allocator close"
and not its point.** The listener frees several things in turn - open batches, the derived
scratch, a subclass hook - and wrapping the lot in a single guard means a throwing batch close
still costs the two stages after it. Guard each stage separately, so every one of them runs;
the collection closes guard each element too, for the same reason one throwing element must not
strand its neighbours. On a *failure* path there is a second reason: a bare
`owned.foreach(_.close())` inside a `catch` that rethrows will replace the exception being
reported with a cleanup exception, so the error that actually matters never surfaces.

**Two idioms for one hazard is how the next person gets it wrong.** This file had two
grow-and-replace helpers. One nulled its field before closing - safe, because a throwing
allocation then leaves no dangling reference, though it destroys a usable buffer for nothing -
and the other closed first and was the bug. The second was written claiming to follow the
first's "discipline", and the claim was even in a comment. Neither the comment nor the reviewer
noticed the orders differed. Both are now acquire-then-release, which is strictly better than
either and, more to the point, is one rule rather than two.

## A checklist for the next node type or mode, from what three reviews found in this one

Task 63 (int32 arithmetic) shipped, was reviewed twice more after it shipped,
and each review found real bugs in the fixes the previous one produced. Twenty
or so findings sort into six categories, and each has a concrete habit that
would have caught its instance before a review had to. The unifying pattern:
every wrong-answer bug lived in an analysis whose own boundary was never
tested directly - only observed transitively, through a downstream matrix that
trusted the analysis to have fed it a true bound.

**1. Any bound, range or "how large can this get" computation uses checked
arithmetic and gets its own property test.** `cannotOverflow` compared against
`abs(Int.MinValue)` instead of `Int.MaxValue`, off by one; `intBound`'s
`datediff` arm assumed the date contract unconditionally, wrong the moment an
operand's own lane could wrap; nested bounds were combined with plain `+`/`*`,
so a value past `2^63` came back small and positive and "proved" anything
safe. All three were wrong answers, not declines, and none was caught by the
emitter's boundary-value matrix, because that matrix tests values, not the
bound-computation logic that decided whether a check was needed at all. Use
`Math.addExact`/`multiplyExact` by construction wherever bounds combine -
overflow computing a bound means "no bound" (`None`), never a wrapped number -
and write the property test directly: over random IR, the interval a node
reports must contain the value the reference evaluator computes, for every
lane pattern. That test does not exist for today's `intBound`; it is scoped
for the lattice that replaces it (`PLAN_MILESTONE_5.md` 2.15, task 84), but it
should exist for any hand-written bound function before that lands.

**2. Never state one admission rule in two places.** `compileOffset` and
`requireDayOffsetShape` each independently enumerated which node kinds a day
offset may be, in two languages, and they drifted: `date_add(d, weekday(d2) +
1)` was admitted by one and refused by the other, and the refusal fired only
at emit time, as a silent per-batch fallback under an EXPLAIN that still
claimed fusion. If a rule must be checked on both sides of the compiler/emitter
boundary, put it in one shared predicate both call (`isDayOffsetShape` is what
that looked like here), or write a test that enumerates the positions and
asserts the two accepted sets are the same set.

**3. Before extending a predicate with more than one reader, list the readers
and their exact question.** `guardedWord` was read by `planSlots` for "does
this node need a scratch local" and by `liveWords` for "must this node's word
stay alive," and task 63 assumed a third guarded kind would answer both the
same way. It answered no and yes: checked arithmetic parks its own values in
`intArithTmp` and never touches the shared scratch slot, so every checked node
reserved a local nothing read. Do not add a case to a shared predicate by
inspection; check what each existing call site actually does with the answer.

**4. A changed invariant is corrected in prose everywhere it was stated, in
the same commit.** The most dangerous single finding across all three reviews
was not in code: `PLAN_TASK_63.md` still told the next editor "extend
`guardedWord`, do not add a condition beside it," which had become false and
whose failure mode had gone from a loud `emitGuardCollect` refusal to a silent
wrong date. Scaladocs and a sibling plan (`PLAN_TASK_70.md`) said the same
superseded thing. Whenever a predicate, invariant or bound rule changes, grep
the repo - docs and comments included, not just call sites - for its old
description before considering the change done. This is the same discipline
`dev/varka_quote_check.py` already enforces for numbers, just not yet for
prose that states a rule.

**5. A new mode is atomic with widening every automated net that should
exercise it.** The IR fuzzer built `IntArith`/`IntNeg` with `Overflow.WRAP`
only, so every emitter path task 63 added - the FAIL-only accumulator
membership, the scratch-slot split, both arms of the mask disposal - sat
outside the project's differential oracle from the day it shipped to the day
a review noticed. A differential test that only `intercept`s an exception on
both engines is compatible with the fused path it exists to protect quietly
becoming residual; pin the plan shape (`assertFused`/`assertNotFused`)
alongside the exception, the way the sibling tests already did. And a change
that removes a dead local moves emitted bytecode exactly as a value change
does - regenerate the committed benchmark, don't reason that "it was unused so
it can't matter" (it moved one row 24.9% and left its sibling width
untouched).

**6. Call the authoritative predicate; do not reconstruct a look-alike.** The
`guardsBelow` fix for the `datediff` bound covered `last_day`, `trunc` and the
dynamic `trunc` and missed `add_months`, because the fix was reasoned from the
`Chrono` sealed interface rather than from `VarkaLoopEmitter.isChrono`, which
is `Chrono` *plus* `AddMonths` and is the actual predicate that decides
whether a producer gets task 52's guard. Whenever new logic must agree with an
existing rule elsewhere in the codebase, grep for that rule's real definition
and call or copy it exactly; a hand-derived approximation of a sealed
interface's membership is not the same question as "what does the emitter
actually treat as a calendar consumer."

## Check that the place a prediction blames actually exists

Task 68's prediction register asked its two interval rows to land within 3% of
their int twins and added the clause that made it look rigorous: "a larger gap
is a finding about the Arrow write path, not the lane". The gap came in at 5.1%
at one width, and the argument that followed was about noise bands - how far
this benchmark's cases move between runs, whether a within-run pair gap is
bounded by a between-run diff, whether three more runs would settle it.

None of that was needed. There is no Arrow write path to have a finding about.
`VarkaKernelEvaluator.project` reads `getDataBuffer().memoryAddress()` off each
output vector and hands the address to the kernel, which writes four-byte lanes
into it; `IntVector`, `DateDayVector` and `IntervalYearVector` are all
`BaseFixedWidthVector`s of `TYPE_WIDTH` 4, so the buffers are the same size and
the same bytes land in them, and one shared `setValueCount` closes the batch
with no branch on type. The accessors differ, but a columnar sink runs none of
them. The only per-type step in the whole route is which class the constructor
makes, once per output per batch.

The lesson is about the shape of the prediction, not about Arrow. A prediction
of the form "if X differs, the cause is Y" is two claims, and the second one is
checkable at *write* time, for free, by reading Y. Doing that here would have
cost one grep and would have replaced a 3% measurement question - which task 67
had already failed to answer and handed forward - with a code fact and a test
that pins it. Instead the clause was carried across two tasks as if it named a
real mechanism, and each task spent its measurement budget failing to resolve a
difference that could not exist.

Two habits follow:

- **When a plan names the place a difference would come from, open that place
  before the run, not after.** If it turns out to be empty, the prediction is
  not "unresolved pending a better instrument" - it is answered, and better than
  a measurement could answer it, because a benchmark can only ever fail to find
  a difference that is not there.
- **Prefer the invariant test to the timing whenever the claim is really about
  code.** "Admitting this type costs the kernel nothing" reads like a
  performance claim and is a structural one. As a test it is a few lines - the
  same arithmetic under two Spark types must produce byte-identical buffers from
  demonstrably different vector classes - and it fails the day the premise
  breaks, which is exactly when a benchmark's noise would hide it.
