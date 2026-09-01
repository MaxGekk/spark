# Task 32 (replanned): one decomposition, several fields

Supersedes the plan carried inline in `PLAN_MILESTONE_4.md` section 2.9 and the
"declined" outcome recorded there. The first pass answered the ceiling question
with a measurement that does not measure what it claims to; this plan repairs
the measurement first and then, if it clears, builds the sharing.

## 1. Why the first pass is being redone

Section 2.9's gate was the right gate: build the ceiling before the mechanism,
and decline the task if the ceiling is close to the 441.2 M rows/s four
independently emitted nodes reached in the parity file as it then stood. The
gate ran, reported 225.8, and the task was declined.

The kernel that produced 225.8 does not have the shape it claims. It is written
as

```
Fields f = computeFields(days);   // record of four IntVectors
```

and `computeFields` compiles to **376 bytes of bytecode** (`javap -c -p` on the
committed class; `-XX:+PrintInlining` says the same thing at runtime). C2's
`FreqInlineSize` is 325, so `computeFields` never inlines into the loop. Once it
does not inline, the `Fields` record and the four `IntVector`s it holds cannot be
scalar-replaced: escape analysis is a per-compilation-unit argument and there is
no unit that contains both the allocation and its consumers. So the kernel
really allocates five objects per lane group and really moves vectors through
the heap - and three of `computeFields`'s six calls to the 12-byte `magic()`
helper stop inlining too, once `computeFields` is itself over budget.

`VarkaLoopEmitter.emitChrono` - the path the kernel exists to model - emits
**zero** call boundaries in the lane path. Every intermediate is a local, every
op is a `jdk.incubator.vector` intrinsic in one method. The kernel and the
emitter therefore differ in the one dimension that dominates Vector API
throughput, and the 1.4-1.9x "slowdown" is the difference between those two
shapes, not between sharing and recomputing.

Two further reasons the number is not comparable, both from the same review:

* the shared kernel emits **no range guard**, while the four-node baseline emits
  one per field - so the baseline is charged four guards the ceiling does not
  pay, which flatters the *baseline*; and
* the shared kernel ORs validity into **one** buffer per lane group where a
  shippable version writes four physical Arrow validity buffers - which flatters
  the *ceiling*.

They push in opposite directions and neither is large, but a ceiling measurement
has to charge both sides the same things.

**What the arithmetic says the answer should be.** `year` alone runs at 1797.2
M rows/s and the four-field projection at 435.1 - a ratio of 4.13, i.e. nothing
is shared today beyond the column load and the loop control.

**One op count, used everywhere.** A calendar field is ~50 vector ops, of which
the shared prefix through `marchMonth` (the guard included) is **~45** and the
field's own tail is **~5**. So four fields cost **~200 ops** as four independent
nodes and **~65** shared - a saving of **~135 ops**, which is the figure
`SKILLS.md` and `PLAN_MILESTONE_4.md` section 2.9 both quote. (The review of
PR #66 read those two as contradicting each other, "~45 shared ops" against
"~135 ops the sharing saved"; they do not - one is per field, the other is the
three redundant copies a four-field projection drops. Both now say which.)

If throughput tracked op count the way task 26 found it does for a single-output
loop, the shared shape would land near 1797 x 50/65 ~ 1380 M rows/s, i.e. **3x
the four-node number** - not 0.5x. The measured 225.8 is a factor of six away
from that, which is about what a per-lane-group heap allocation costs.

Register pressure is also the wrong worry at these widths. Five int vectors and
two masks stay live across the tails; AVX-512 has 32 zmm registers and 8 mask
registers, 128-bit SSE has 16 xmm. Task 17's contrary result (raising
`GROUP_BUDGET` so two outputs kept cross-output CSE *lost*, 4494.0 against
3044.7) was a different trade: there the shared chain was eight ops, so
recomputing it was nearly free and the wider method was pure cost. Here the
shared work is ~45 ops and the tails are ~5. The ratio that decides the direction
is shared-work to per-field-tail, and it is 9:1 here against roughly 1:1 there.
Whether that argument survives contact with a narrow vector register file is a
different question, and section 7 is where it gets answered.

## 2. Decisions

1. **The task is not declined on the current evidence.** The measurement is
   repaired first, and the gate is then re-run honestly. If the repaired ceiling
   still does not clear the four-node number, the task declines with a real
   reason.
2. **The mechanism is emitter-side fragment sharing, not a multi-value IR node.**
   Section 2.9 listed a multi-value node as "the general answer". It is not
   needed: the values worth sharing are locals inside one node's emitted
   bytecode, and the emitter can share locals without the IR ever naming them.
   Both mechanisms emit *identical bytes*; they differ only in where the decision
   lives. That is an engineering-cost choice, not a measurable one, which is why
   this is the one place in this plan that is settled by argument rather than by
   building both - there is nothing to measure between them. A multi-value node
   returns only if some future primitive needs the shared value visible to the
   *planner* (to feed further IR nodes, or to CSE across loop-method groups);
   that stays in the debt register.
3. **Both lowerings ship, behind an emit option**, the way task 26 shipped
   `TOTAL`/`NARROWED` and task 14 shipped the three `FloorMod7` variants. The
   loser stays a live, differentially tested reference variant rather than dead
   code, and the parity benchmark keeps both cases so a future retune is measured
   rather than argued.
4. **Mechanism 3 stays declined**, with section 2.9's reason unchanged:
   decomposing the calendar into primitive IR nodes would put a four-field
   projection at ~60 nodes against `MAX_FUSED_NODES = 64`, and give the IR a
   general arithmetic vocabulary to serve one family.

## 3. The fragment mechanism

### 3.1 What a fragment is

A **fragment** is a run of emitted lane ops that (a) several nodes need, (b)
depends only on one shared child node, and (c) leaves its results in scratch
locals rather than on the operand stack. It is the sub-node counterpart of the
CSE the emitter already does between nodes.

```java
private record FragmentKey(FragmentKind kind, VarkaVectorIR child) {}
private enum FragmentKind { CHRONO_PREFIX }
```

One kind for now. The key carries the kind so that a second one is additive
rather than a rewrite.

`CHRONO_PREFIX` is exactly what `emitChronoPrefix` already emits (task 40 -
PR #67 - factored it out of `emitChrono`; this plan depends on that factoring
and so is sequenced after it): the guard, the biased day, era, day of era,
century, year of century, day of year, and the March-based month, into the eight
slots `s.chronoTmp` already allocates. Nothing in the tails writes back into
those slots, which is what makes the run shareable as-is.

The guard is inside the prefix (`emitEra`) and depends only on `days`, the node's
validity word - which `planWordRef` aliases to the child's - and the epilogue
bounds mask. All three are identical for every chrono node over the same child,
so sharing the prefix shares the guard **correctly**, and drops three of the four
guards a four-field projection pays today.

### 3.2 The four edits

* **`Slots.chronoTmp` is keyed by `FragmentKey`, not by node.** In `planSlots`,
  where the topo walk today does
  `if (isChrono(node)) s.chronoTmp.put(node, new int[]{...})`, it instead does
  `s.chronoTmp.computeIfAbsent(new FragmentKey(CHRONO_PREFIX, chronoChild(node)), ...)`.
  A small `chronoChild(VarkaVectorIR)` switch returns `n.days()` for each chrono
  node, beside the existing `isChrono`. Nodes that need extra private scratch
  (task 40's `AddMonths` wants three more) keep allocating that per node - the
  fragment owns the shared slots, the node owns its own.
* **`emitValue`'s chrono arm consults a per-lane-group fragment set.**
  `emitLaneGroup` already threads a `Set<VarkaVectorIR> computed` for node-level
  CSE; it gains a `Set<FragmentKey> emittedFragments` beside it, reset per lane
  group for exactly the same reason. `emitChrono` becomes: if the key is already
  in the set, skip straight to the tail; otherwise emit the child, `astore` it,
  emit the prefix, add the key, then the tail.
* **Grouping counts the fragment once.** `addOps` walks a subtree adding nodes to
  a `seen` set and counting only what is new. It gains the fragment as a
  synthetic member of that set: a chrono node contributes `CHRONO_PREFIX_WEIGHT`
  (~44) the first time its fragment key is new to the group and
  `chronoTailWeight(node)` (~6) always. Because `groupOutputs` already counts
  only what is new to a group, this makes chrono siblings over one child naturally
  want to sit together - no new grouping pass, just an honest cost function.
* **The budget rule admits a cheap output into an already-wide group.** Today
  `groupOutputs` splits when `ops + marginal > budget`. With the fragment
  counted, a four-field projection is `50, 6, 6, 6`: the first output already
  exceeds the budget on its own (which the current rule explicitly allows), and
  every rule-following split after that *duplicates a 44-op prefix to save 6 ops
  of method width* - strictly more work in strictly more methods. The rule
  becomes:

      join when  ops + marginal <= groupBudget                  (today's rule)
            or   marginal <= groupBudget && ops + marginal <= FUSED_CEILING

  The second clause admits only an output that is cheap *because it reuses work
  the group already has*, and `FUSED_CEILING` keeps the method away from the
  compile cliff `GROUP_BUDGET` exists to avoid and from the 8000-byte
  `HugeMethodLimit` that tasks 43/44 are about. `FUSED_CEILING` is **set by the
  measurement in section 5.2, not chosen here**; 96 is the starting candidate
  (one prefix plus eight tails).

### 3.3 The emit option

`VarkaEmitOptions` gains `boolean shareChronoPrefix` beside `cse`, with
`withShareChronoPrefix`, a `canonical()` rendering, and inclusion in the shape
hash on the existing non-default-only rule. `shareChronoPrefix = false` reproduces
today's bytes exactly, which is what makes the differential test in section 5.1
meaningful and what lets the benchmark price the change as an A/B in one run.

Which value `DEFAULTS` carries is the deliverable of section 5.2.

### 3.4 What this does for the rest of the calendar family

Tasks 33, 34 and 40 all start from the same prefix (`next_day`, `dayofyear`,
`add_months`, `date +/- INTERVAL n MONTH/YEAR`). Once the fragment is keyed on
(kind, child) rather than on a node type, `SELECT year(d), dayofyear(d)` and
`SELECT month(d), add_months(d, 1)` share it without another line of code. That
generality is the argument for doing this in the emitter rather than special-
casing the four task-26 fields.

It also cuts the epilogue. `epilogueMasked` is one method over *every* output by
task 24's deliberate decision, and the debt register measures it at 7530 bytes
for 16 calendar outputs and 8079 for 17 - across `HugeMethodLimit`, past which
HotSpot compiles nothing at all. Sixteen calendar outputs over one date column
share one prefix under this change, so the epilogue shrinks by roughly the 15
prefixes it stops repeating. That does not close tasks 43/44 - a projection over
16 *different* date columns still crosses - but it moves the reachable cases well
back from the edge, and section 5.3 measures the new byte counts so tasks 43/44
plan against real numbers.

## 4. Files

**Step A - repair the measurement** (branch `varka-task-32`, PR #66):

* `sql/varka/engine/.../vector/ChronoVectorOps.java` - hand-inline
  `computeFields` and `fourFieldsEpilogue`'s copy of it into their loops, so the
  lane path has no call boundary, matching what `emitChrono` emits; drop the
  `Fields` record; add the range guard (two compares, OR, AND with validity, OR
  into an accumulator, `anyTrue` after the loop) so the ceiling pays what a
  shippable version pays; write four destination validity buffers rather than
  one. Requote the class javadoc's baseline number from the committed results
  file rather than from memory.
* `sql/varka/engine/.../vector/ChronoVectorOpsTest.java` - replace the quarter
  oracle `(expected.getMonthValue() + 2) / 3`, which restates the implementation,
  with one derived from `LocalDate` independently of the month formula; drop the
  private `isBitSet` copy in favour of `VarkaVectorSupport.isBitSet`; restore
  `100` to `SIZES` to match `DateVectorOpsTest`.
* `sql/catalyst/src/test/scala/.../VarkaEmitterParityBenchmark.scala` -
  generalize the `chunked` helper to take a per-chunk callback instead of the
  hand-copied loop the new case added.
* `sql/catalyst/benchmarks/VarkaEmitterParityBenchmark-jdk25-results.txt` -
  regenerated in the same commit as the benchmark change, as every prior commit
  touching it did.
* `SKILLS.md` - the task-17 figures reintroduced stale (4587/3196) go back to the
  committed parity file's current 4494.0/3044.7 - which this task's own
  regeneration moves again, so both are requoted from the regenerated file, and
  `VarkaLoopEmitter`'s `GROUP_BUDGET` javadoc with them; the "ops saved" figure
  is reconciled with
  `PLAN_MILESTONE_4.md`'s (one number, quoted once, referenced from the other).
* `PLAN_MILESTONE_4.md` - section 2.9's outcome and section 9's debt entry
  rewritten around the repaired number; "sweep" reserved for `VarkaChronoSuite`'s
  exhaustive opt-in check, with `ChronoVectorOpsTest`'s ~100,000 sampled values
  described as sampling.
* `PLAN_TASK_34.md`, `PLAN_TASK_35.md` - **left alone.** The review of PR #66
  flagged their "task 32 may move the plumbing; it will not move the arithmetic"
  paragraph as a stale contingency this task had resolved. Under this plan it is
  not stale: it is exactly what step B does, and it is exactly the instruction
  those two recipes need - write the tail, do not restructure `emitChrono`, do
  not try to share anything by hand. The finding is recorded as not actioned,
  with this reason.

**Step B - build the sharing** (new branch off master, after PR #67 lands):

* `VarkaLoopEmitter.java` - `FragmentKey`/`FragmentKind`, `chronoChild`,
  `chronoTailWeight`, `CHRONO_PREFIX_WEIGHT`, `FUSED_CEILING`; `planSlots`
  keying `chronoTmp` by fragment; `emitLaneGroup`'s `emittedFragments` set;
  `emitChrono` skipping an already-emitted prefix; `addOps`/`groupOutputs` per
  section 3.2. `CHRONO_WEIGHT`'s javadoc is rewritten - it stops being "what a
  calendar node weighs" and becomes prefix plus tail.
* `VarkaEmitOptions.java` - the option, its `with`, `canonical()`, `isDefault`.
* `VarkaLoopEmitterSuite.scala` - the differential in section 5.1; the pinned
  line map and `everyNode` fixture, which move; a grouping test asserting the
  method partition for one, two, three and four fields over one column and over
  two different columns.
* `VarkaShapeCacheSuite.scala` - the `everyNode` hash, which moves.
* `VarkaDifferentialSuite.scala` - a query-level case per section 5.1.
* `VarkaEmitterParityBenchmark.scala` + results - section 5.2's cases.
* `docs/sql-varka.md`, `README.md` - requoted from the one regeneration run, if
  the default changes.

## 5. Verification

Both vector widths everywhere, per the standing gate:

```
build/sbt catalyst/Test/compile sql/Test/compile
build/sbt 'catalyst/testOnly *Varka*' 'sql/testOnly *Varka*'
build/sbt "project catalyst" 'set Test/javaOptions += "-XX:MaxVectorSize=16"' 'testOnly *Varka*'
build/sbt "project sql"      'set Test/javaOptions += "-XX:MaxVectorSize=16"' 'testOnly *Varka*'
./build/mvn -f sql/varka/engine/pom.xml install
dev/lint-java && dev/scalastyle
```

(`JAVA_OPTS` does not reach the forked test JVM; the `set Test/javaOptions` form
is the one that actually narrows the vectors.)

### 5.1 Correctness

The two lowerings must agree bit for bit. `shareChronoPrefix` is an optimization
and never a semantics change, which is exactly the property `cse` is already
pinned on:

* every shape the emitter suite drives, emitted both ways, outputs compared
  lane for lane, at both widths;
* the guard's behaviour under sharing: a batch with one out-of-range row must
  decline under both settings, and an in-range batch with nulls must decline
  under neither - the guard is now emitted once for several outputs, so the
  "silent total loss of fusion" failure mode `emitEra`'s javadoc describes has a
  new way to appear and needs its own case;
* `SELECT year(d), month(d), dayofmonth(d), quarter(d)` against the row engine
  over the Gregorian sweep data, both settings.

### 5.2 The measurement that decides the default

In `VarkaEmitterParityBenchmark`'s existing "year" section, same 4096-row chunks,
same repeat count, same in-range null-free data, on an idle machine, five
iterations over two-second windows, any ratio under 1.3x re-checked by minimums:

| case | shared | per-output |
|---|---|---|
| `year` | - | 1797.2 (committed) |
| `year, month` | new | new |
| `year, month, dayofmonth` | new | new |
| `year, month, dayofmonth, quarter` | new | 435.1 (committed) |
| four fields, mixed nulls | new | new |
| `year(d1), year(d2)` (two columns, nothing to share) | new | new |

The last row is the regression guard: two chrono nodes over *different* children
must not be pushed into one method by the new budget clause.

Step A's repaired hand-written kernel is measured in the same run as an
independent check that the emitted shared path reaches the hand-written ceiling.

### 5.3 The compile cliff, measured rather than assumed

`GROUP_BUDGET = 16` exists because a 64-op multi-output loop took a ~10 s tier-4
compile, during which the loop ran C1 with boxed vectors at ~1% speed. A shared
four-field loop is ~65 ops across four outputs - squarely the shape that finding
was made on - so this cannot be waved past on the grounds that a 59-op
*single*-output loop measured healthy.

Per the project's standing rule that JIT facts come from the JVM's own output:

* `-XX:+PrintCompilation` on the four-field kernel under both settings, reporting
  the wall time between the tier-4 task being queued and its completion for each
  loop method - one ~65-op method against four ~50-op methods, which may also
  compile in parallel on separate compiler threads;
* `-XX:+PrintInlining` confirming no call boundary survives in the lane path of
  either the emitted kernel or the repaired hand-written one;
* emitted bytecode size per loop method and for `epilogueMasked`, at 1, 2, 4 and
  16 calendar outputs over one column, both settings - the numbers tasks 43 and
  44 will plan against.

`FUSED_CEILING` is set from these numbers. If the compile time turns out to be
the binding constraint rather than throughput, the honest outcome is a ceiling
low enough to share two or three fields but not sixteen, which is still most of
the win - not a decline.

## 6. Predictions, registered before the measurements

Scored honestly in section 7 when the numbers land, per `sql/varka/AGENTS.md`.

1. The repaired hand-written kernel lands between 900 and 1400 M rows/s at
   AVX-512, i.e. **2.0x to 3.2x** the four-node baseline - reversing the recorded
   1.9x loss. Confidence: high, on the ~3x op-count ratio and the fact that the
   defect being removed is a per-lane-group heap allocation.
2. The emitted shared path lands within 10% of the repaired hand-written kernel.
   Confidence: medium-high - the emitter has no call boundary to begin with, so
   the two should be the same bytes modulo the driver.
3. Two fields shared beat two fields separate by 1.6x-1.9x; three by 2.2x-2.7x.
   The gain is sublinear in field count because the tails and the four stores are
   not shared.
4. Compile time for the ~65-op four-output method is under 1 s at tier 4 - the
   task-11 cliff was 64 *distinct nodes across many outputs* in a loop whose op
   mix was different, and a single-output 59-op loop already compiles promptly.
   Confidence: **low**. This is the prediction most likely to be wrong, and the
   one with a real chance of capping the mechanism at two or three fields.
5. At 128-bit the ratio is smaller than at AVX-512 but still above 1, because
   fewer lanes per op make the per-op cost relatively larger while the op-count
   saving is unchanged.
6. `epilogueMasked` at 16 calendar outputs over one column drops from 8079 bytes
   to under 3000, moving it back across `HugeMethodLimit` - a side effect, not a
   closure of tasks 43/44.

## 7. Outcome of step A: the gate clears at AVX-512, and is a wash at 128-bit

The kernel was rewritten with the whole lane path written out by hand (no method
call of any kind), with the narrow-range guard, and writing four destination
validity buffers through a `VarkaFusedKernel`-shaped array ABI. The benchmark's
hand-copied chunk loop was replaced by the same `eachChunk` walk every other case
in the section uses, so the two arms cannot differ in their addressing.

**The lane path is clean, confirmed from the JVM's own output** rather than
inferred. `javap -c -p` puts `vectorFourFields` at 936 bytes and
`fourFieldsEpilogue` at 642, with **zero** `invokestatic` to any `ChronoVectorOps`
method. `-XX:+PrintInlining`, restricted to this method by
`-XX:CompileCommand=option,...::vectorFourFields,PrintInlining`, reports exactly
one callee not inlined:

```
147516 4349    3  ChronoVectorOps::vectorFourFields (936 bytes)
     @ 919 ChronoVectorOps::fourFieldsEpilogue (642 bytes)
           failed to inline: callee is too large
147547 4349    3  ChronoVectorOps::vectorFourFields (936 bytes)
     COMPILE SKIPPED: out of virtual registers in linear scan
147569 4357 %  4  ChronoVectorOps::vectorFourFields @ 233 (936 bytes)
```

Bytecode 919 is past the loop - the epilogue runs once per batch, not once per
lane group - and everything inside the loop is a force-inlined vector intrinsic.
The old `computeFields` boundary is gone.

The third line is a finding in its own right: **C1 refuses this method outright**,
"out of virtual registers in linear scan", so it runs interpreted until C2's
tier-4 compile lands. It reproduces on every run. It does not affect a
steady-state benchmark number, and it is not a defect in this kernel - a 936-byte
straight-line vector body is what the emitter produces too - but it is the first
concrete evidence in this project that a wide shared body has a warmup cost the
four narrow bodies do not, and step B's compile-time gate (section 5.3) inherits
it as a thing to measure rather than a thing to assume.

### The numbers

AVX-512 (`IntVector.SPECIES_PREFERRED`, the development machine's native width),
`VarkaEmitterParityBenchmark`'s "year" section, 4096-row chunks, five iterations
over two-second windows, idle machine:

| | run 1 | run 2 | run 3 (the committed file) |
|---|---|---|---|
| four separate emitted nodes | 450.4 | 448.8 | 435.1 |
| shared decomposition, hand-written | **692.4** | **678.8** | **661.7** |
| ratio | **1.54x** | **1.51x** | **1.52x** |

This is a record of three runs; the committed results file is run 3, and it is
the one every other document quotes. `year` alone measured 1791.2, 1822.2 and
1797.2 in the same runs, so the four-node case is still about 4x one field:
nothing is shared today.

128-bit (`-XX:MaxVectorSize=16`), five runs, because the ratio came in under the
project's 1.3x re-check threshold and the first pass was faulted for leaving
exactly this number unrepeated:

| run | four nodes | shared | ratio |
|---|---|---|---|
| 1 | 155.9 | 165.7 | 1.06x |
| 2 | 157.6 | **236.1** | **1.50x** |
| 3 | 154.1 | 165.6 | 1.07x |
| 4 | 156.4 | 165.9 | 1.06x |
| 5 | 157.4 | 167.0 | 1.06x |

Compared by minimums, as the rule requires: 121, 85, 121, 121 and 120 ms against
a four-node baseline of 128, 127, 130, 128 and 127 ms - stable to 2% on the
baseline and bimodal on the shared kernel. Within a run the shared kernel's stdev
is 0 ms; between runs it moves 42%. That is a compilation the JVM either finds or
does not, not measurement noise, and averaging the two modes would describe a
state no run is ever in. `-XX:+PrintCompilation` on runs 3, 4 and 5 shows an
identical event sequence for all three slow-mode runs, so the C1 skip above is
not the discriminator; run 2 was not instrumented and the fast mode has not been
reproduced since.

**So: the gate clears at AVX-512 and does not clear at 128-bit.** Sharing is
worth 1.5x where there are 32 vector registers and 8 dedicated mask registers to
hold five live intermediates plus four outputs, and worth nothing reliable where
there are 16 vector registers and masks must live in them too. That is task 17's
register-pressure effect, found again - but as a width-dependent ceiling on the
win rather than as a reversal of its sign.

### Predictions, scored

1. **Wrong.** Predicted 900-1400 M rows/s at AVX-512 (2.0x-3.2x); measured 661.7
   to 692.4 over three runs (1.51x-1.54x). The direction was right and the confidence was stated
   as high, so this is a real miss on magnitude: the op-count model
   (`1797 x 50/65 ~ 1380`) assumes throughput is proportional to vector ops and
   nothing else, and it over-predicts by a factor of two. Time not accounted for
   by the decomposition - four stores, four validity-bitmap read-modify-writes,
   the chunk prologue and the loop control - is roughly half the four-node case's
   cost, and sharing does not touch any of it. **An op-count ratio is an upper
   bound on a sharing win, not an estimate of one**, and the next such prediction
   should be made as a bound.
2. Not yet measurable - step B builds the emitted path.
3. Not yet measurable - step B adds the two- and three-field cases.
4. Not yet measurable. Partial evidence above: C1 cannot allocate registers for
   the 936-byte body at all, which is not what prediction 4 was about (C2
   tier-4 time) but is not encouraging for it either.
5. **Right, and for the wrong reason.** Predicted the 128-bit ratio would be
   smaller than AVX-512's but still above 1: it is, 1.06x against 1.51x. The
   reason given - "fewer lanes per op make the per-op cost relatively larger
   while the op-count saving is unchanged" - does not survive the data, because
   that argument predicts a *proportionally similar* win, not its disappearance.
   The register file is the better explanation, and the bimodality is what a
   marginal allocation looks like.
6. Not yet measurable - step B.

### What does not fix the 128-bit mode, measured

Four hypotheses were tested against the bimodality, because a 1.5x win that the
narrow-vector shape cannot reach is what decides step B's scope. All four
failed, and they are recorded so nobody pays for them twice.

1. **Shorter live ranges.** `ChronoVectorOps.vectorFourFieldsShortLive` is the
   same arithmetic and the same op count on a schedule that keeps fewer values
   live: the year assembly hoisted so `era`, `century` and the year of century
   die before the tails, and each output stored the moment it exists rather than
   all four at the end - which is also what `emitLaneGroup` does, so this is the
   variant that mirrors the emitter. It is **slower at both widths**: 626.9 and
   642.6 against 686.2 and 691.3 at AVX-512, 156.5 and 157.7 against 165.6 and
   167.1 at 128-bit. Register pressure is real but is evidently not relieved by
   holding fewer values; C2's scheduler does better with the wide window. The
   variant is kept, differentially tested against the other, as the reference
   that stops this being re-proposed - and as a caution for step B, since the
   emitter's natural store-as-you-go shape is the losing one here.
2. **Forcing the two validity helpers to inline.**
   `-XX:CompileCommand=inline,...VarkaVectorSupport::orValidityBitsAt` and the
   same for `validityBitsAt`. These are the only calls left in the lane path, and
   `-XX:+PrintInlining` shows them genuinely failing, at bytecode 828, 839, 850
   and 861, with `NodeCountInliningCutoff` on one compilation and
   `callee is too large` on another - 212 bytes of a four-arm switch on
   `groupBytes(lanes)` that a constant lane count would fold away if it ever got
   in. Forcing them changes **nothing**: 691.3 at AVX-512 against 686.2 unforced,
   and at 128-bit one fast run and one slow one, the same split as without it.
3. **Forcing every Varka class to inline**, `-XX:CompileCommand=inline,*varka*::*`
   under `-XX:+UnlockDiagnosticVMOptions`. Also nothing: 119 ms then 83 ms, both
   modes, unchanged distribution. (Its AVX-512 companion run is discarded rather
   than quoted - it overlapped a build on the same machine and its anchor case,
   `year`, came in at 1643.6 M rows/s with a stdev of 7 ms against the 1791 to
   1831 at a stdev of 0 to 1 every clean run of this session produced. The
   no-effect-at-AVX-512 conclusion rests on experiment 2's clean 691.3 instead.)
4. **Disabling on-stack replacement**, `-XX:-UseOnStackReplacement`, on the
   theory that the mode was OSR-versus-standard compilation. Three runs, all
   slow: 121, 123, 119 ms.

Across all four configurations the shared kernel was measured 14 times at
128-bit and landed the fast mode 3 times, with no configuration making either
mode deterministic and none shifting the distribution enough to call from three
successes. Whatever picks the mode is inside C2's code generation for this body
and is not reachable from any of these levers.

Two things follow. First, **a JVM flag was never going to be the answer anyway**:
Spark cannot require `-XX:CompileCommand` on a user's JVM, so a flag that helped
would have been a diagnostic pointing at a code change, not a fix. The code
change these results point at, if anything, is making `orValidityBitsAt` small
enough to inline on its own merit - it is a width-generic switch called from
kernels that know their width at emit time - and that is a change to a helper
every Varka kernel calls, so it belongs in its own task with its own
measurement, not inside task 32. Second, this is the third time in this project
that a `-XX:CompileCommand=inline` flag has moved nothing in the catalyst parity
harness; the debt register's note that the same flag moves the engine's JMH
numbers 50-190% and the catalyst numbers by under 1% is the same observation,
and the harness is simply not in a state where inlining is what is left on the
table.

### What this changes for step B

**Step B splits in two, and only the first half is unconditional.** The
throughput case rests on a four-field projection, and the corpus does not contain
one - TPC-H uses `year` alone and TPC-DS pre-materialises `d_year`/`d_moy`/
`d_dom`. So the 1.5x is real but is not, on its own, worth relaxing a grouping
policy that exists to avoid a measured ten-second compile.

**B1 - fragment sharing inside a method. No policy change, do it.** Section 3.2's
first three edits only, leaving `groupOutputs` exactly as it is. Today each
calendar output already forms its own loop method, so no loop method holds two
chrono nodes and nothing there changes. What does change is the **epilogue**,
which by task 24's deliberate decision is one method over *every* output: the
debt register measures `epilogueMasked` at 7530 bytes for 16 calendar outputs and
8079 for 17, and 8000 is `HugeMethodLimit`, past which HotSpot compiles nothing
at all. Sixteen calendar outputs over one date column share one prefix under B1
instead of repeating fifteen, so the method that today falls off that cliff stops
doing so. That is a compilability win on a shape a user can actually write, it
needs no measurement to justify, and it is most of the mechanism tasks 43 and 44
would otherwise have to invent.

**B2 - the grouping change that buys the 1.5x. Gate it on the two-field case
first.** Extend the ceiling kernel to `year, month` and measure it. Two fields
share ~45 ops and pay ~5 each, so the op-count ratio is 1.9x - but the four-field
case delivered 1.5x against a 3.1x op-count ratio, i.e. about half, so two fields
should be expected around 1.25x-1.4x and could easily land lower. That
measurement costs an afternoon and decides whether `FUSED_CEILING` and the
budget-rule relaxation are worth their risk. If two fields clears ~1.3x at
AVX-512, build B2; if it lands near 1.1x, stop after B1 and record why.

Either way **the default cannot be flipped on the AVX-512 number alone**:
section 5.2's measurement must run at both widths on the emitted path, and
`shareChronoPrefix` has to be able to default differently from what AVX-512 alone
would choose. Whether the 128-bit bimodality follows the emitted body is the
first thing B2 should find out, since it has the same shape and the same register
demand - and per the section above, no JVM flag will make that question go away.

## 8. Risks

1. **Prediction 4.** The compile cliff is the one thing that could cap this, and
   it is measured in 5.3 before `FUSED_CEILING` is chosen rather than discovered
   after the mechanism ships.
2. **A shared prefix under a changed guard.** Sharing the guard is correct only
   because the guard reads nothing but the child and the masks. Any future chrono
   node whose guard depends on something else (an ANSI throw path, task 30) must
   either join the fragment key or opt out of it; the differential in 5.1 is what
   catches a violation, and `FragmentKind` is where a second guard shape would
   live.
3. **Pinned oracles move.** The line map, the `everyNode` fixture and the shape
   hash all change under the new default. Expected; re-pinned in the same commit
   with a note, the way task 26 did.
4. **Merge order.** Five calendar PRs (#61, #62, #63, #64, #67) are open against
   `emitChrono`. Step B rebases on all of them and must be sequenced last;
   step A touches none of the emitter and can land immediately.
5. **The ceiling kernel remains unshippable.** It has no `VarkaFusedKernel`
   wiring - the engine module cannot depend on catalyst - so it stays a
   measurement artifact even with the guard added. That is fine for its purpose
   and is stated in its class doc; it must not drift into looking like a
   production path.

## 9. Sequencing

**Step A**, in the existing `varka-task-32` branch, rewriting PR #66 from
"declined" to "measured honestly":

1. Repair the kernel (hand-inline, guard, four validity buffers) and its test
   oracle; re-run the differential against `java.time` at both widths.
2. Generalize the benchmark helper, re-run, regenerate the committed results
   file in the same commit.
3. Rewrite section 2.9's outcome, the debt entry, `SKILLS.md`'s numbers, and the
   stale contingency in `PLAN_TASK_34.md`/`PLAN_TASK_35.md`.

**Step B**, new branch off master once PR #67 has landed, gated on step A's
number clearing the four-node baseline:

4. The emit option and the fragment plumbing, `shareChronoPrefix = false` still
   the default: bytes unchanged for every existing shape, and the differential
   from 5.1 green. Nothing pinned moves in this commit.
5. The grouping change and `FUSED_CEILING`, chosen from 5.3's compile-time and
   bytecode-size numbers.
6. The parity benchmark cases and one regeneration run.
7. The default flipped, pinned oracles re-pinned, `docs/sql-varka.md` and
   `README.md` requoted from that run, section 2.9 and the debt register swept in
   the past tense.
