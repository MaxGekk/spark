# Task 52: guard at the producer, not the extraction

**Status: planned, not started.** Task 51 removed the per-extraction range
guard task 26 shipped; this task is what closes the gap that removal opened.
Read `PLAN_TASK_51.md` and `PLAN_MILESTONE_4.md` sections 2.21-2.22 first -
they carry the argument for why the guard moved rather than simply
disappearing, and this file assumes that argument rather than repeating it.

If you find yourself making a design decision beyond what section 2 below
already settles, stop and say so in the pull request instead of choosing.

## 1. The one-sentence version

Move the runtime range check from every calendar extraction (where task 26
put it, and task 51 removed it from) to the arithmetic nodes that can
actually manufacture a day outside `VarkaChrono.NARROW_MIN_DAYS..
NARROW_MAX_DAYS` from a value the compiler cannot bound ahead of time - today,
`AddDays`/`SubDays` when the offset operand is not a `LiteralSlot`.

## 2. Scope, settled already

* **Guarded nodes: `AddDays` and `SubDays`, and only when `offset` is not an
  instance of `LiteralSlot`.** A literal offset's magnitude is visible to the
  compiler; task 38's own literal path is untouched by this task and needs no
  guard, exactly as it needed none before task 26 ever existed. A column
  offset (`IntegerType`, task 38) can be `Int.MinValue`/`Int.MaxValue`, which
  is why this is the one place a fresh check earns its cost.
* **`NextDay` is explicitly out.** Its `(days, offset)` shape looks the same,
  but task 33's compiler arm (`VarkaExpressionCompiler`, the `NextDay` case)
  accepts only a foldable weekday and always compiles it to a `LiteralSlot`,
  and the floorMod7 result it adds is bounded to `[0, 6]`. It cannot move a
  day far enough to matter and gets no guard under this task.
* **`AddMonths` is explicitly out.** Its month count is already bounded at
  compile time (`VarkaChrono.MONTH_ARITH_MIN_MONTHS`/`MAX_MONTHS`, task 40's
  decline path) - the same shape of protection this task is building for
  `AddDays`/`SubDays`, already built, for a different operand.
* **A bare `ColumnRef` is explicitly out.** The project's standing contract is
  that column data crossing the Spark boundary is `[0001, 9999]`; this task
  does not add a check at ingestion, and does not revisit that contract.
* **Every calendar extraction (`Year`, `Month`, `DayOfMonth`, `Quarter`,
  `DayOfYear`, `LastDay`) stays guard-free**, per task 51. It trusts its
  input, whether that input is a bare column or the (now guarded) output of a
  column-offset `AddDays`/`SubDays`.
* **Behind a flag, default off.** This changes the cost shape from task 26's
  guard (paid once per calendar *output*, and shared across sibling outputs
  by task 32's fragment sharing) to paid once per *guarded arithmetic node*,
  whether or not anything downstream ever reads a calendar field off it. That
  is a real, different trade and it needs its own number before it is the
  default - see section 5.

## 3. Mechanics

Reuse task 51's plumbing rather than rebuilding it - this is the payoff of
having left it in place:

* `Slots.guardAcc`, `emitStatusReturn` (already returns a constant zero when
  nothing sets a guard, and the guard mask when something does), the `DRIVER`
  mode's OR-across-methods reduction, `VarkaFusedKernel.STATUS_CHRONO_RANGE`,
  and `VarkaKernelEvaluator`'s existing fallback routing and metrics are all
  untouched infrastructure. Nothing here needs a new concept.
* The guard-allocation decision in `planSlots` (formerly `hasChrono`, deleted
  by task 51) needs a replacement predicate: whether an output's subtree
  contains an `AddDays`/`SubDays` node whose `offset` is not a `LiteralSlot`.
  Shape it the way `hasChrono` was shaped - a recursive walk over
  `childrenOf` - rather than reintroducing the old name for a different
  question.
* Emit the guard where `AddDays`/`SubDays` already compute their result: two
  `VectorMask` compares against `NARROW_MIN_DAYS`/`NARROW_MAX_DAYS` on the
  node's *output* (not its input - this node is where the value comes from,
  so there is no upstream fact to trust), ANDed with the node's own validity
  word and, in an epilogue, the bounds mask - the same three-way AND
  `emitEra`'s guard used to do, for the same reasons (a null row's data is
  undefined; a masked epilogue's padding lanes must not condemn a batch of
  real ones). OR the result into `s.guardAcc`.
* `VarkaEmitOptions` gains a switch (name and shape following the project's
  existing options, e.g. alongside `shareChronoPrefix`) that gates whether
  this guard is emitted at all. Off means `AddDays`/`SubDays` with a column
  offset behaves exactly as task 51 left it - computed, not checked.

## 4. Files

Expect the same files task 26 and task 51 touched, for the same reasons:

* `VarkaLoopEmitter.java` - the new guard predicate, the guard emission in
  `AddDays`/`SubDays`'s existing emit path, `VarkaEmitOptions` plumbing
  through to `planSlots`.
* `VarkaEmitOptions.java` - the new switch, its `with...`, and `canonical()`.
* `VarkaLoopEmitterSuite.scala` - a guard-decline unit test shaped like the
  one task 51 rewrote, but anchored on `AddDays`/`SubDays` with a column
  offset rather than a calendar extraction; the pinned fixtures (line map,
  shape hash) will move for any shape that now includes a guarded producer
  node, which is expected and should be re-pinned from the actual emitted
  output, not computed by hand.
* `sql/core/.../VarkaDifferentialSuite.scala` - a differential replacing the
  two task 51 removed, reshaped around `date_add`/`date_sub` with a column
  offset feeding a calendar extraction, checked through both the projection
  and filter paths the way the originals were.

## 5. Validation

* Both flag settings green, at both vector widths, in both modules.
* The differential from section 4: a column-offset `date_add` large enough to
  push a date past the narrowed range, with the flag on, matches the row
  engine (declined, not silently wrong) - closing exactly the gap
  `PLAN_TASK_51.md` section 4 recorded as open. With the flag off, the two
  engines are allowed to disagree, which is today's (task 51) behaviour and
  should be asserted as such rather than left unchecked.
* A committed benchmark number for the guard's cost on a column-offset
  `date_add` shape, at both widths, following the project's standing
  benchmark discipline (JMH or the catalyst parity harness per precedent,
  five iterations over two-second windows, compared by minimums). This
  number is what the owner uses to decide the flag's default - it is not
  assumed to be cheap just because task 26's version, amortised over several
  calendar outputs, was.
* No pinned fixture or committed number for a shape with no column-offset
  `AddDays`/`SubDays` node is expected to move.

## 6. Risks

1. **The guard now runs on the node's output, not an extraction's input.**
   `emitEra`'s old guard read the value a calendar node was about to
   decompose; this guard reads the value `AddDays`/`SubDays` just produced.
   Get the operand order and the validity/epilogue-mask ANDing wrong here and
   the failure mode is the same as task 26's own risk 3: a null row's
   undefined data trips the guard, or a masked epilogue's padding lanes
   condemn a batch of real rows past a non-lane-multiple length. Both are
   silent-and-slow, not silent-and-wrong, but both should be differential
   cases, not assumptions.
2. **The flag defaulting on without a number.** Section 2's "default off
   until measured" is not a formality; do not flip it without the benchmark
   in section 5 backing the choice.
3. **A future node that can also manufacture an out-of-range day.** This
   task's guarded-node set (`AddDays`/`SubDays` with a column offset) is
   exhaustive over what exists today, not a general mechanism - the way
   `isChrono`'s `instanceof Chrono` check is total over extractions because
   of the sealed interface, this task's predicate is a hand-picked pair of
   node types with no such enforcement. A future node that decomposes a
   column-driven runtime value into a day count (there is none today) would
   need to be added here by hand, and nothing will fail loudly if it is not.
   Worth a comment at the predicate's definition saying so, the way
   `isChrono`'s own javadoc explains its two-part check.
