# Task 70: Validity as bitmap algebra in the driver

## 1. Where this came from

`PLAN_MILESTONE_4.md` row 70 and section 2.34, opened out of task 46's
admission check (`PLAN_TASK_46.md` 2.3 and 3.7), which priced one
`orValidityBitsAt` call at 1.87 to 3.24 ns per lane group whatever the vector
width, and observed that for most value roots the bits that call writes are a
bytewise function of the input bitmaps over the whole batch - so they can be
computed once per batch in the driver, the way task 45's `setValid` fills a
constant, instead of once per lane group in the loop. Task 45 took the call
off the dense path by knowing the answer in advance; task 46 made the calls
that survive inline; this task removes most of what survives on the masked
path. The numbers that motivate it are the masked rows of
`sql/catalyst/benchmarks/VarkaEmitterParityBenchmark-jdk25-results.txt` and
its `-128bit-` sibling as task 32 B2 regenerated them, quoted in section 2.4.

## 2. The admission check, done

Four things had to be true. Each was read off the emitter and the committed
files on 7 September 2026, at `bbc91b7ec39`; none needed a probe.

### 2.1 Every value node's word is an alias, an AND or an OR - except three

`VarkaLoopEmitter.planWordRef` is the whole algebra. A `ColumnRef` is its
input's word; a `LiteralSlot` is the all-true constant; `AddDays`, `SubDays`,
`DateDiff`, `NextDay`, `AddMonths` and `TruncDateDynamic` are the AND of their
two children's words; every calendar extraction, `ThursdayOf`, `DayOfWeek`,
`WeekDay` and `DayOfWeekIso` alias their child's; `Greatest` and `Least` OR
their children's; `IfElse` blends its two branches' words by the condition's
known-true mask, which is computed per lane group; a `Cond` root's slot is a
selection bitmap, not validity at all. So for every root whose tree holds no
`IfElse` and is not a `Cond`, the destination bitmap is a bytewise AND/OR
expression over the referenced inputs' bitmaps, and nothing about it varies
per lane group.

The one node that is null on valid inputs is the one the analysis already
names: non-ANSI `make_date` sets `Analysis.nullsFromValidInputs`, and the
dispatch takes the masked methods for every batch because of it. Task 63's
`try_*` forms will be the next; the predicate this task adds is written as an
exhaustive switch over the sealed IR so that a new node has to declare itself
(the `chronoChild` / `tailReadsMarchMonth` pattern) rather than default to
pure.

The derived leaves make two nodes *more* regular, not less. `TruncLevelLeaf`
turns a null, unrecognised or sub-day format into a null lane of the derived
input and zeroes its data; `WeekdayLeaf` does the same for a bad weekday name
(declining the batch in ANSI mode rather than nulling). So `trunc(d, fmt)` and
`next_day(d, s)` reach the kernel as plain ANDs over two input bitmaps, with
the nullness already in a bitmap.

### 2.2 The word has a third consumer, which the milestone section did not list

Section 2.34 counts two uses of a word in the masked body: the root's
validity write and the blend. There is a third. `emitGuardCollect` ANDs a
guard's condemning mask with the guarded node's word, so that a null lane -
whose data slot holds whatever the buffer holds - cannot condemn the batch.
That covers task 52's range guard on a `date_add`/`date_sub` with a column
offset under a calendar consumer, and task 60's month-count guard on
`add_months` with a column count. And the loop body already loads its
columns unmasked (`emitValue`'s `ColumnRef` arm takes the masked
`fromMemorySegment` only in the epilogue), so garbage in a null lane already
flows through the arithmetic today; the word AND is the only thing keeping it
out of the guard.

The consequence for the design: the task has two separable effects, and the
second has a stricter condition than 2.34 gave it.

* Removing a root's per-group *write* is safe whenever its word is a pure
  AND/OR expression (2.1). Per root.
* Removing the per-group *reads* and the word locals is a liveness question,
  per loop method: a word is emitted if and only if some node in the method
  still consumes it - a guard, a blend, a `Cond` root's OR, or a root whose
  validity the pass does not serve. "Every root qualifies" is neither
  necessary (a method of `year(d), greatest(d, d2)` needs no word once both
  roots are served by the pass) nor sufficient (a method of
  `year(date_add(d, off))` alone has one qualifying root and still needs the
  word for the guard).

What this check would have rejected: a design that dropped the read
whenever the roots qualified. On nullable data with a stray value in a null
slot it would decline batches that today fuse - answers right, through the
row engine, and the kernel's win gone on exactly the `year(date_add(d, off))`
shape task 52 built the guard for.

### 2.3 What the pass has to handle that `setValid` does not

`setValid` writes a constant. The pass writes an expression, and not every
operand is a bitmap. The three helpers the expression is built from -
`VarkaVectorSupport.copyValidity`, `andValidity` and `orValidity`, each
setting exactly `rows` bits, each allowing the destination to alias an
operand - landed with this plan, ahead of any emitter work, with
`VarkaVectorSupportBitmapAlgebraTest` holding them to the loop's own form
bit for bit (the `setValid` test's pattern), the operands sliced to exactly
the bitmap so an over-read fails the test rather than reaching a neighbouring
Arrow buffer.

An operand is in one of three runtime states, and the rule differs by
operator. All ones is the identity of an AND and annihilates an OR; all zeros
is the reverse:

| operand state | in an AND | in an OR |
|---|---|---|
| a bitmap (`0 < nullCount < rows`) | read it | read it |
| null count 0, no bitmap materialised | drop the operand | the whole expression is `setValid(dst, rows)` |
| null count `rows`, validity address `0L` | the whole expression is `zero(dst)` | drop the operand |

After dropping, an expression of two operands may be left with one, and then
it is a `copyValidity`; left with none, it is the `setValid` or the `zero`
that the operand's state already decided.

**The earlier draft of this section had one rule for all of it, and it was
wrong.** It said only that a null-free input "contributes an all-ones operand
... so the pass skips it", with no AND/OR distinction. Applied to
`greatest(d, d2)` where `d` is null-free and `d2` has nulls, skipping gives
`copyValidity(dst, d2Validity, rows)`, which marks null every row where `d2`
is null - rows on which `greatest` returns `d`'s value and the output is
valid. The same reading left the degenerate case undefined: `year(d)` over a
null-free `d` inside a masked kernel needs `setValid`, not a copy of a bitmap
that was never materialised. Both states are reachable today, because
`emitDispatch` takes `runMasked` as soon as *any* referenced input has nulls.

**What that costs the API, which commit 2 decides.** The emitter knows an
input's null count at emit time only for a literal; at runtime it arrives in
the `nullCounts` argument, and step (4) of `emitBody` stores `aconst_null`
into `srcValSeg[i]` for a null-free input and for an all-null one alike -
there is no segment to pass in either state. So the segment-only helpers as
landed force the emitter to generate the three-way choice above as bytecode,
per operand, per served output, which is the cost 6.1's prediction 6 prices
against the driver's `HugeMethodLimit`. The alternative is an overload taking
`(long address, int nullCount)` per operand, which puts the three states in
the engine - two `if`s in Java instead of a bytecode diamond, and nine
two-operand combinations a JUnit test can enumerate. Commit 2 picks between
them on the driver's emitted byte count for `year(d)` and `greatest(d, d2)`
and records which; the overload is additive, so the helpers as landed are not
wasted either way.

The final partial byte is masked to `length % 8` bits, the rule `setValid`'s
javadoc records and `assertSameOutput` enforces byte for byte.

### 2.4 The bound, from the current file

Masked against dense, M rows/s, the parity file as this branch commits it
(provenance `64ada2b5db0`, 2026-09-07, both widths from one regeneration):

| shape | AVX-512 | 128-bit |
|---|---|---|
| `year` | 2947.6 / 3385.2 | 1201.1 / 1334.8 |
| `year+month+day+quarter`, one loop method | 1084.6 / 1757.2 | 415.6 / 791.8 |
| `next_day(d, k)`, column kernel | 7245.4 / 7910.1 | 2851.6 / 3633.0 |
| `add_months(d, m)`, column count | 628.3 / 697.6 | 209.6 / 242.3 |
| `add_months(d, 13)` | 724.9 / 727.4 | 254.8 / 254.8 |
| `filter d < literal` (a `Cond` root; not served) | 24229.9 / 24202.6 | 4762.7 / 6384.7 |

Task 46 took most of the single-field gap: `year` has 12.9% and 10.0% left.
The prize is the multi-output method, 1.62x at AVX-512 and 1.91x at 128-bit
on four fields, which is the shape B2 emits by default now, and the two-input
node at 8.4% and 21.5%. `add_months` with a literal has nothing left - its
81-op tail hides one write - and the column-count form keeps its read for
the guard (2.2) until task 64 takes the guard off the in-range case.

**The baseline rows added with this plan** (the regeneration committed
beside it, at both widths), masked against dense:

| shape | AVX-512 | 128-bit |
|---|---|---|
| `greatest(d, d2)` (an OR root) | 10725.7 / 11344.9 | 2671.2 / 12909.1 |
| `year+month+day+quarter`, shared, chunk 4096 | 1378.8 / 2197.5 | 429.1 / 808.0 |
| the same, chunk 4095 | 1329.2 / 2211.0 | 430.0 / 809.3 |
| the same, chunk 64 | 898.5 / 1538.0 | 384.5 / 705.4 |
| the same, chunk 63 | 718.0 / 1367.4 | 353.3 / 660.5 |

Two things these say that the earlier table did not. The OR root is the
largest relative mover in the file at 128-bit - a 4.8x gap - because a
two-input masked body pays two reads, one write and its own OR word per
group, and at four lanes a group is four rows; a narrow-vector measurement
was not optional here, exactly as `SKILLS.md` says of anything that shares
live values. And the chunk-64 and chunk-63 rows are risk 2's before-numbers:
the masked four-field kernel runs at 0.58x and 0.53x of its dense twin on
short batches at AVX-512, and 0.55x and 0.53x at 128-bit, which is the gap a
per-batch pass has to close without costing more than the calls it replaces
on an 8-byte bitmap.

**The wide file's fastest dense rows are not trustworthy, and this section
says which claims that touches.** The regeneration's nine controls are flat -
every one between -0.2% and +0.8% - but in the AVX-512 file the
sub-microsecond dense rows fell hard against the previous file:
`date_add emitted loop, null-free` -33.5%, `sequential kernels (9 passes)`
-20.8%, `datediff emitted loop, null-free` -8.9%, while the 128-bit file's
same rows held or rose. That leaves orderings which cannot be true of
identical code on a wider datapath: wide `greatest(d, d2), null-free` reads
11344.9 against the narrow 12909.1, `arithmetic depth 4, null-free` 12495.5
against 15443.8, and inside the wide file `arithmetic depth 4, mixed nulls`
(15462.9) beats its own null-free row. These are the shortest cases in the
harness, about 0.08 us per call over 1,000,000 rows, so it is variance on a
timer-bound family rather than a bad build - every long row and every masked
row in the file behaves, which is why the tables above stand.

Two consequences, both registered rather than worked around. The OR root's
AVX-512 gap reads 5.5% in this file, and that divides a depressed dense row
by a healthy masked one, so it is the machine and not the shape: it is
described here and is deliberately *not* a prediction in 6.1. And prediction
4's bound - no masked row passes its dense counterpart - is already violated
by two rows of this file, `filter d < literal` and `arithmetic depth 4`, both
at AVX-512 and both in that same family.

`dev/varka_bench_regen.sh` prescribes a same-day re-run of the base commit in
exactly this case, and none was made. The re-run is the last step of this
branch, and it also picks up a source change made with these corrections: the
`datediff, first input all-null` baseline is gone, replaced by
`greatest(d, d2), emitted loop, first input all-null`. The old row never
reached a loop - `DateDiff` satisfies the masked driver's all-null shortcut,
so the batch returned straight after `zero(dstValidity)`, which is why it
read 644329.9 M rows/s bit-identical at both widths, could not have moved
whatever the pass does, and published a 135.5X Relative for a 125 KB memset.
`Greatest` is null-skipping, the shortcut declines it, and the OR of an
all-null operand is exactly 2.3's degenerate case. Section 9 records the
re-run's file and requotes every figure in this section from it.

### 2.5 The pinned oracles

`VarkaLoopEmitterSuite`'s `everyNode` fixture is an `IfElse` root over a tree
holding non-ANSI `MakeDate`, so its root does not qualify and its masked body
keeps every word: the pinned line map is predicted unmoved. `DEFAULTS`
renders empty whichever way the new switch defaults, so no production shape
hash moves. Both are asserted by the suites as they stand.

## 3. The design

### 3.1 The word expression, evaluated once per batch

Two additions to the analysis and one to the driver, all behind
`VarkaEmitOptions.validityByBitmap` (default decided in 8; off reproduces
today's bytes exactly, the `FloorMod7` precedent).

**The purity predicate.** `Analysis.pureWord(node)`: an exhaustive switch
over the sealed IR returning, for a value node, the bitmap expression its
word denotes - `Input(i)`, `AllTrue`, `And(a, b)`, `Or(a, b)` - or `null`
where the word is computed (`IfElse`, any node with `nullsFromValidInputs`
set, a `Cond`). It mirrors `planWordRef` case for case and the suite asserts
the two agree on every node the fixtures build, so the algebra cannot drift
from the emission.

**Where the pass goes, which is narrower than it looks.** `emitBody` in
`DRIVER` mode runs in five steps: (1-2) the prologue; (3) map each output's
data and validity segment and `zero` the bitmap, or `setValid` it on a dense
batch (task 45); (4) compute `dead[i]`, `hasNulls[i]` and `srcValSeg[i]` from
the runtime null counts; (5) the all-null shortcut, which returns 0 for a
batch whose every output is all-null. **The pass goes between (4) and (5)**,
and neither neighbour will do.

At (3) its inputs do not exist yet: 2.3's three operand states *are*
`dead[i]` and `hasNulls[i]`, and those are written in (4). After (5) a batch
that takes the shortcut returns with the destination bitmap never written,
and the reused Arrow validity buffer keeps the previous batch's bits, which
read as valid over undefined data. A two-output kernel makes that concrete:
`datediff(d0, d1)` beside `year(d2)` with `d0` all-null does *not* take the
shortcut, because output 2 reads no all-null column, so output 1's bitmap
would simply never be written.

Between (4) and (5) the pass *replaces* step (3)'s `zero`/`setValid` for the
outputs it serves - step (3) keeps emitting it for the others. That is the
only reading that holds step (3)'s stated invariant, "`zero(dstValidity)`
before any return below": it is kept by the pass, not by leaving the zero in
place and writing over it. An earlier draft of 2.3 assumed the opposite, that
an all-null AND operand could lean on step (3)'s zero having already run.
For a served output there is no step (3) zero to lean on, which is why 2.3's
table gives that state its own `zero(dst)` call.

**What it emits.** For each served output, one call into `VarkaVectorSupport`
per node of the expression - `copyValidity(dst, src, rows)`,
`andValidity(dst, a, b, rows)`, `orValidity(dst, a, b, rows)` - with 2.3's
operand states resolved either as bytecode here or inside the helpers, which
is the API question 2.3 leaves to commit 2. The helpers are the engine's,
reached by name like `setValid`; each sets exactly `rows` bits.

**Nesting, and where the destination alone stops being enough.** A
left-leaning expression is emitted inner-first into the destination: every
operand is then either an input bitmap or the destination itself, the
destination may alias an operand, and no scratch buffer is needed. That is
not every expression. `DateDiff` is the AND of its two children's words and
`Greatest`/`Least` the OR of theirs, and both IR nodes are binary, so
`datediff(greatest(d, d2), greatest(d3, d4))` denotes `And(Or(a, b),
Or(c, d))` - two live intermediates, which two-operand destination-aliasing
calls over one destination cannot evaluate in any order.
`VarkaIrFuzzSuite` builds exactly this: `DateDiff`, `Greatest` and `Least`
all take arbitrary `value(depth)` children, so the shape arrives whether the
surface has it or not.

Commit 2 handles it in two steps rather than by growing a scratch buffer.
First, flatten: AND and OR are associative and commutative over bitmaps, so
any tree of a *single* operator collapses to a left-leaning chain, which
covers every shape today's surface produces. Second, where a genuinely mixed
tree remains, decline to serve that root: it keeps its per-group write and
its word stays live, which is today's behaviour and is always correct. A test
asserts the declined-root count is zero over the suite's fixtures, so the
safety net cannot quietly become the common path, and the fuzzer is what
exercises the net itself.

**Word liveness, per method.** `planSlots` today allocates a word local per
referenced input and per own-word node. Under the switch it allocates them
only for words some emitted consumer reads: a `guardTmp` node's word, an
`IfElse`'s condition and branches, a `Cond` root's, a `Greatest`/`Least`
whose own word feeds one of those, and any root the pass does not serve. A
method with no live word skips the per-group `validityBitsAt` reads entirely,
and an input whose word is dead in a method needs none of that method's
null-state prologue either - no `srcValSeg`, no `dead`/`hasNulls` flags - so
the lane-group body and the prologue are then the dense method's bytes, the
one-body-not-two result, verified in 5 rather than assumed (6.1's prediction
6 sizes what the epilogue keeps if the prologue is not dropped with the
words).

**Liveness is checked by the emission, not by the list in 2.2.** `loadWord` is
the one call through which every consumer reads a word, so it counts each use
in `Slots` as it emits, and the emitter asserts at the end of every method
that a word the liveness pass declared dead was loaded zero times - an
`IllegalStateException` at emit time, which `VarkaIrFuzzSuite` drives over
random IR. The inventory in 2.2 is how the rule was designed; the counter is
what keeps a future consumer from being missed silently.

**The write.** `emitLaneGroup` skips the per-group `orValidityBitsAt` for a
root the pass served, the way `fillsValidityOnce` skips it on a dense batch;
the two decisions are taken in one place so the driver and the loop cannot
disagree, which is the failure `fillsValidityOnce`'s javadoc names. The
epilogue's per-output partial write goes with it: the pass covers every row
of the batch, tail included.

### 3.2 What is deliberately unchanged

* The dense path and task 45's `setValid`: `denseValidityOnce` stays a live
  option beside the new one rather than being folded into it.
* The guards, their word AND and the status route (tasks 42, 52, 60): a guard
  keeps its word, which is the whole of 2.2.
* `Cond` roots and the filter path (task 21): a selection bitmap is computed,
  not derived, and stays per group; task 46 is what made that call cheap.
* The fragment mechanism, grouping, `GROUP_BUDGET` and `FUSED_CEILING`: the
  validity calls were never counted as vector ops, so no *weight* moves and
  no shape regroups. The driver's *bytes* do move, which is a different
  question with a different limit behind it, and it is 6.1's prediction 6
  rather than an item on this list.
* Aliasing the output bitmap to an input's buffer (2.34's step two): an Arrow
  ownership and lifetime question at the evaluator, scoped out of this task
  and left in section 2.34.
* Task 64's per-batch range check, which decides *whether* a guard is emitted;
  this task decides what a body without a guard still has to read. 64 widens
  what this task drops, and is the natural next task, not a prerequisite.

### 3.3 Registered op counts

The metric this task moves is not `IntVector` invocations - none is added or
removed in any body - but the *validity work* per masked loop method: the
`validityBitsAt*` reads and the `orValidityBitsAt*` and
`orPartialValidityBitsAt*` writes today, the whole-batch helpers after.

A plain owner-wide count will not express it, and the difference decides
whether the table below can be asserted at all. `VarkaEmitterTestSupport`'s
three-argument `invocationCount` counts every invocation on an owner inside a
method, and `loadSegment` emits `VarkaVectorSupport.ofAddress` for each
segment the body touches in *every* body mode - only the `zero`/`setValid`
of step (3) is gated on `DRIVER`. So an owner-wide count of a masked loop
method can never reach zero however much validity work goes away, and every
"0" registered below would be unreachable with the tool named to read it.
The four-argument overload landed with this plan takes an exclusion list;
the metric is that count with `ofAddress` excluded, exact-matched rather than
by prefix for the reason `invokedNames` documents - the helpers carry a
lane-count suffix, and `orValidityBitsAt` is a prefix of
`orValidityBitsAt16`. `dev/varka_emit.sh` prints the same number as a
`validity` column beside `IntVector` and `VectorMask`, so a reviewer reads
the before and after without writing a test.

Registered from the emission sites in 2.2, to be asserted in 5:

| shape | masked loop method, `VarkaVectorSupport` calls today | after |
|---|---|---|
| `year(d)` | 1 read + 1 write | 0 |
| `year, month, dayofmonth, quarter` over `d` (one method) | 1 read + 4 writes | 0 |
| `next_day(d, k)`, column kernel | 2 reads + 1 write | 0 |
| `greatest(d, d2)` | 2 reads + 1 write | 0 |
| `year(date_add(d, off))`, guarded | 2 reads + 1 write | 2 reads (the guard) + 0 |
| `year(d)` beside `d < lit` (a `Cond` root, one method) | 1 read + 2 writes | 1 read + 1 write (the `Cond`) |
| `if(d < d2, d, d2)` | 2 reads + 1 write | unchanged |

`IntVector` counts per body are asserted unchanged for every shape above.

## 4. Files

| file | what |
|---|---|
| `sql/varka/engine/.../VarkaVectorSupport.java` | `copyValidity`, `andValidity`, `orValidity`, each setting exactly `rows` bits; tests in the engine module |
| `sql/catalyst/.../varka/VarkaEmitOptions.java` | `validityByBitmap`, `withValidityByBitmap`, in `canonical()` |
| `sql/catalyst/.../varka/VarkaLoopEmitter.java` | `Analysis.pureWord`, word liveness in `planSlots`, the pass in the driver, the skipped writes |
| `sql/catalyst/.../varka/VarkaLoopEmitterSuite.scala` | the tests in 5; the poisoned `makeInputData` landed with this plan |
| `sql/catalyst/.../varka/VarkaEmitterTestSupport.java` | `invocationCount`'s exclusion overload, 3.3's metric; landed with this plan |
| `sql/catalyst/.../varka/VarkaEmitDump.scala` | the `validity` column, so `dev/varka_emit.sh` shows the metric; landed with this plan |
| `sql/catalyst/.../varka/VarkaIrFuzzSuite.scala` | poisoned null lanes; landed with this plan |
| `sql/core/.../VarkaDifferentialSuite.scala` | both switch settings over the nullable fixtures, both widths |
| `sql/catalyst/.../VarkaEmitterParityBenchmark.scala` | the A/B rows in 6 |
| `sql/catalyst/benchmarks/VarkaEmitterParityBenchmark-jdk25-*` | one regeneration |
| `docs/sql-varka.md`, `SKILLS.md`, `PLAN_MILESTONE_4.md` | the validity paragraph, the lesson, row 70 and the 45/46/47 entries |

## 5. Tests, and what each is for

* **`pureWord` agrees with `planWordRef`** on every node of every fixture in
  the suite, by construction: catches an algebra that drifts from the
  emission.
* **Byte-identical validity, both settings, both widths**: `checkMatrix` over
  the calendar, arithmetic, `next_day`-column, `greatest`/`least` and
  `datediff` shapes at `remainderLengths` (1, 13, 17, 63, 1001) plus 64 and
  1000, every null pattern - the existing oracle; `assertSameOutput` holds on
  the tail byte.
* **The guard under nulls, with poisoned slots**: `year(date_add(d, off))`
  and `add_months(d, m)` over data whose null lanes hold `Int.MIN_VALUE` and
  `Int.MAX_VALUE`, asserting status 0 under both settings - the failure 2.2
  describes. Until this plan the fixtures wrote the drawn value under a null
  slot, in range by construction, so nothing could have provoked it.
  `makeInputData` now poisons, and so does `VarkaIrFuzzSuite`, which draws
  its columns inside `columnBound` and `MONTH_ARITH_MAX_MONTHS` and would
  otherwise have kept every null lane in range for ever - the fuzzer being
  named below as where an unlisted interaction shows. Two details the first
  cut got wrong and section 9 records: the alternation counts null slots, not
  row indices, because an `i & 1` poison collides with the `alternating` null
  pattern and would have left a quarter of the matrix on one side of every
  bound; and the handful of guard tests that deliberately place a boundary
  value *at* a lane they also null pass `poisonNulls = false`, since
  substituting an extreme there replaces the value the test names and turned
  one of them into a duplicate of the case beside it. The change landed with
  the plan rather than with the emitter work, so the whole existing matrix
  ran against garbage in its null lanes first - for tasks 42, 52 and 60 as
  much as for this one.
* **The dead-word invariant fires**: a test that emits a shape with a
  deliberately mis-marked word (through a test-only hook, the
  `misdescribeAdd` pattern) and asserts the emit-time exception, so the
  counter in 3.1 is known to be armed.
* **An all-null input through the pass**: an OR root (`greatest(d, d2)` with
  `d` all-null, output equal to `d2`'s bitmap, `0L` never dereferenced) and
  the AND root beside it in a two-output kernel that keeps the driver's
  all-null shortcut from firing (`datediff(d, d2)` with `year(d3)`), which is
  the case 3.1 says the pass must run before step (5) to cover.
* **A null-free input through the pass**: `greatest(d, d2)` with `d`
  null-free and `d2` nullable, whose output must be valid on every row -
  2.3's correction, and the one case the earlier rule got wrong. Beside it
  `year(d)` over a null-free `d` in a masked kernel, whose output is
  `setValid`, not a copy.
* **A mixed word tree declines rather than mis-evaluates**:
  `datediff(greatest(d, d2), greatest(d3, d4))` keeps its per-group write and
  its word; a single-operator tree of any depth is flattened and served. The
  declined-root count over the suite's fixtures is asserted at zero.
* **A mixed method**: `year(d)` beside `d < lit` in one loop method; the
  `Cond` keeps its per-group OR and the read stays; the year's write goes.
* **The liveness counts of 3.3**, asserted per method with
  `invocationCount(bytes, method, owner, List.of("ofAddress"))`, and
  `IntVector` counts unchanged.
* **The driver's size, pinned on the same ladder as the epilogue**: 20, 21,
  40, 44 and 48 outputs, both shared and unshared, asserted under
  `HugeMethodLimit` with the margin recorded - 6.1's prediction 6. Nothing
  measures the driver today, and it is the one method every batch runs.
* **One body, not two**: for the shapes whose methods have no live word, the
  masked loop method's code size equals the dense one's.
* **Pinned oracles**: the line map and the shape hash unmoved (2.5), asserted
  as the suites stand.
* **`VarkaIrFuzzSuite`** at both settings, which is where an interaction
  nobody listed would show.

## 6. The measurement

`VarkaEmitterParityBenchmark`, adjacent A/B pairs on shapes that already
exist so the point is what the pass does to kernels that ship: the "year"
section's `year, mixed nulls` and `year+month+day+quarter, shared, mixed
nulls`, the `next_day(d, k)` column kernel's mixed-null row, the
`add_months(d, m)` column-count mixed-null row (the control: its guard keeps
the read, so it should move by the write alone), and `add_months(d, 13),
mixed nulls` (the second control: predicted flat). Three rows the file did
not have were added with this plan, per the rule that a baseline is
committed before the change that moves it: `greatest(d, d2)` null-free and
mixed nulls in the datediff section (the OR root - the one shape where the
pass computes an OR), `greatest(d, d2)` with its first input all-null beside
them (the `0L`-address operand, on the OR root because an AND root's batch
takes the driver's all-null shortcut and never reaches a loop - 2.4), and the
four-field shared kernel's mixed-null arm on every rung of the alignment
ladder (chunks 4096, 4095, 64 and 63 - the short batches risk 2 is about,
which had only null-free rows). Each with the switch on and off, both widths,
one regeneration with `dev/varka_bench_regen.sh` on an idle machine. The
dense rows are the bound: no masked row may pass its dense counterpart.

### 6.1 Predictions, registered before the run

Every threshold below is a fraction of the row's own gap in the file the work
is measured against, not a fixed percentage of today's numbers. 2.4 says why:
this file is regenerated once more before the work starts, and a percentage
pinned to a superseded run would be scored against a file that no longer says
it.

1. `year+month+day+quarter, shared, mixed nulls` closes at least two thirds
   of its gap to the dense row at both widths: from 1084.6 towards 1757.2 and
   from 415.6 towards 791.8. Reason: four writes and one read per group go,
   and nothing else in that body differs from the dense one. Confidence
   medium-high.
2. `year, mixed nulls` closes the whole of its remaining gap to
   `year, null-free` - one read and one write per group is all that separates
   them - and is the smallest relative mover among the served shapes.
   `greatest(d, d2), mixed nulls` is the largest relative mover at 128-bit,
   closing at least half of its 4.8x gap (2671.2 towards 12909.1). Nothing is
   registered for `greatest` at AVX-512: 2.4 shows that gap is the wide run's
   depressed dense row, not the shape, and predicting against a number known
   to be wrong would score either way and mean nothing. Confidence high on
   the ordering, medium on the half.
3. `add_months(d, 13), mixed nulls` moves within run noise (its dense row is
   0.4% away). Confidence high. `add_months(d, m)` column count moves less
   than `next_day(d, k)` column, because its read stays for the guard.
   Confidence medium. The short-batch rows move with the long ones: the
   four-field masked kernel at chunk 64 and 63 closes at least half of its
   gap to the dense twin at both widths, and the pass is never the reason a
   short-batch row is slower than before. Confidence medium - this is risk 2
   measured. The all-null row moves with the other `greatest` rows, the
   shortcut having declined a null-skipping root; the row it replaces could
   not have moved at all, which is 2.4's reason for replacing it.
4. No masked row passes its dense counterpart. Confidence high; a miss is a
   measurement error to explain, not a result. Registered against the re-run
   of 2.4, not against the file as committed here, where two rows already
   violate it - `filter d < literal` (24229.9 masked against 24202.6 dense)
   and `arithmetic depth 4` (15462.9 against 12495.5), both at AVX-512 and
   both in the sub-microsecond family 2.4 flags. If the re-run leaves them
   inverted, the bound is wrong about that family, and this says so before
   the work rather than after it.
5. No pinned oracle moves and no dense committed number moves beyond noise.
   Confidence high.
6. **The epilogue's `HugeMethodLimit` crossing moves from 44 shared outputs
   to 47, and from 21 unshared to 22.** Measured before the work rather than
   discovered after, off `javap` of the classes the pinned crossing test
   emits, at `bbc91b7ec39`: in `epilogueMasked` each served output's write is
   one 12-byte sequence (`aload dst; iload i; i2l; lload word; iload lanes;
   invokestatic orPartialValidityBitsAt`) and each input's word read is one
   32-byte three-way block (dead, has nulls, null-free), and the ladder reads

   | outputs (dates) | shared today | shared after | unshared today | unshared after |
   |---|---|---|---|---|
   | 20 (5) | 3575 | 3175 | 7670 | 7270 |
   | 21 (6) | 4020 | 3576 | 8331 | 7887 |
   | 40 (10) | 7082 | 6282 | 18396 | 17596 |
   | 44 (11) | 8058 | 7178 | 20511 | 19631 |
   | 45 (12) | 8726 | 7802 | | |
   | 48 (12) | 9084 | 8124 | | |

   where "after" subtracts 12 bytes per output and 32 per input. Interpolating
   the shared column's 119 bytes per output inside a date, 46 outputs land
   near 7909 and 47 near 8016 - sixteen bytes over the limit - so the shared
   crossing is 47 or 48, and the pinned test re-pins to whichever it is;
   unshared, 21 now fits and 22 crosses near 8377. **In the epilogue** the
   crossing can only move outward, so nothing that compiles today stops
   compiling there. Confidence medium-high on the direction and the 21-to-22
   move, medium on 47 against 48. One more thing the same numbers say: after
   the reads and writes go, the 44-output masked epilogue is still 7178 bytes
   against the dense 6574, and that residue is the per-input null-state
   prologue - which is why 3.1 drops it with the dead words; the prediction
   for the one-body test in 5 is that with the prologue gone the two differ
   by under a hundred bytes.

   **The driver, which the ladder above does not cover and no test measures.**
   Every byte the pass adds lands in `emitBody`'s `DRIVER` mode. The driver is
   one method for all of a shape's outputs, and it is the one method that runs
   on every batch, so a `HugeMethodLimit` crossing costs more there than
   anywhere - task 44's failure mode, in the one direction the paragraph above
   calls impossible. Per served output the driver gains an `invokestatic` with
   its operand pushes, about 12 bytes, and loses step (3)'s `zero`/`setValid`,
   about 6. What decides whether that is the whole story is 2.3's API
   question: with the operand states resolved in bytecode it gains a
   three-way block per operand on top - the same 32-byte shape priced above -
   so about 70 bytes per two-input output rather than about 6, and on the
   48-output rung 3.3 KB against 0.3 KB. Registered: with the states in the
   engine the 48-output driver grows by under 500 bytes and crosses nothing;
   with them in bytecode it is predicted to cross `HugeMethodLimit` somewhere
   between 32 and 48 outputs. That is the measurement 2.3 says commit 2 picks
   the API on, and 5 adds the driver to the pinned ladder either way, since
   an unpinned driver would surface a crossing only as an unexplained
   wide-shape regression. Confidence medium on the byte figures, high that
   the driver has to be pinned.

The rule that decides the default: on, if prediction 4 holds and no served
row is slower than before at either width.

## 7. Risks

1. **A consumer of the word nobody listed.** 2.2 found three by reading the
   emitter; a fourth would show as a wrong answer under the differential or
   the fuzz suite, or as a spurious decline under the poisoned-slot test.
2. **The pass costs more than it saves on short batches.** At 64 rows the pass
   is 8 bytes against four lane groups' calls; the alignment ladder's chunk-64
   and chunk-63 rows show it either way.
3. **Reads kept for a guard hide the write's saving.** The column-count
   `add_months` control is there to show the write alone; if it does not
   move, the write was not the cost on that shape.
4. **Task 44's crossing moves again** - and it is not a risk to the shape or
   the numbers, only to the record. The crossing is a measured property that
   can only move outward here, it is pinned by a suite test that fails the
   moment it moves, and 6.1's prediction 6 says where it lands, so the
   re-pin in the same commit is a scored prediction rather than a surprise.
   `PLAN_TASK_32.md` 7.1's ladder is requoted in 9.
5. **A switch that defaults on changes the bytes of nearly every masked body
   in the shape cache at once.** The differential and fuzz suites at both
   settings are the oracle, the reference variant stays live, and the default
   flips in the last commit per 8 - the same discipline every lowering change
   here has followed, applied to the widest one so far.

## 8. Sequencing

0. **The base re-run, before any emitter work.** The parity file as this
   branch commits it is not a sound baseline for the AVX-512 column of the
   fastest dense rows (2.4), and the benchmark source has since changed - the
   `datediff` all-null row replaced by the `greatest` one - so the committed
   file no longer matches the harness that produced it. One regeneration of
   this same commit on an idle machine settles both, and 2.4, the milestone's
   2.34 and 6.1's predictions are requoted from it. Everything below is
   measured against that file, not this one.
1. `pureWord` with the agreement test, and the `loadWord` use counter with
   its invariant asserted on today's emitter (every word loaded at least
   once): no emitted byte changes. The engine helpers, the poisoned harness,
   `invocationCount`'s exclusion overload and the baseline rows are already
   in, with this plan.
2. The pass, the liveness rule and the skipped writes behind the switch, off
   by default; the tests of 5; both widths green. 2.3's API question is
   settled here, on the driver's emitted byte count, and 6.1's prediction 6
   scored against it before the throughput run.
3. The A/B rows, one regeneration, section 9 with the predictions scored; the
   default set by 6.1's rule; the docs and the milestone rows swept, including
   what task 47 is left with.

## 9. Outcome

Filled in when the measurement lands.
