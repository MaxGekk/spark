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

`setValid` writes a constant. The pass writes an expression, and two inputs
are special. An input whose null count is zero contributes an all-ones
operand, which the masked body already substitutes today (the `wNoNulls`
branch stores `-1L` instead of reading), so the pass skips it rather than
reading a bitmap that may not be materialised. An all-null input's validity
address is `0L` by the morsel contract and must not be dereferenced; it is an
all-zero operand, which for an AND root means the output is all-null (the
driver's `zero` has already written that) and for an OR root means the other
operand alone. The final partial byte is masked to `length % 8` bits, the
rule `setValid`'s javadoc records and `assertSameOutput` enforces byte for
byte.

### 2.4 The bound, from the current file

Masked against dense, M rows/s, the parity file at `bbc91b7ec39`:

| shape | AVX-512 | 128-bit |
|---|---|---|
| `year` | 3213.6 / 3460.4 | 1204.2 / 1335.0 |
| `year+month+day+quarter`, one loop method | 1124.0 / 1762.6 | 417.7 / 792.5 |
| `next_day(d, k)`, column kernel | 6935.0 / 8188.4 | 2858.6 / 3609.9 |
| `add_months(d, m)`, column count | 565.9 / 703.6 | 209.7 / 242.2 |
| `add_months(d, 13)` | 731.1 / 733.7 | 254.7 / 254.9 |
| `filter d < literal` (a `Cond` root; not served) | 21772.3 / 23841.3 | 4798.7 / 6757.5 |

Task 46 took most of the single-field gap: `year` has 8% and 11% left. The
prize is the multi-output method, 1.57x at AVX-512 and 1.90x at 128-bit on
four fields, which is the shape B2 emits by default now, and the two-input
node at 18% and 26%. `add_months` with a literal has nothing left - its
81-op tail hides one write - and the column-count form keeps its read for
the guard (2.2) until task 64 takes the guard off the in-range case.

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

**The pass, in the driver.** Where `emitBody` in `DRIVER` mode zeroes an
output's bitmap today (or `setValid`s it, task 45), a masked driver whose
root has a pure expression emits that expression instead: a call into
`VarkaVectorSupport` per node of the expression - `copyValidity(dst, src,
rows)`, `andValidity(dst, a, b, rows)`, `orValidity(dst, a, b, rows)` - with
the null-count and `0L`-address cases of 2.3 folded in by the emitter, which
knows both from the kernel's arguments. The helpers are the engine's, reached
by name like `setValid`; each sets exactly `rows` bits. A nested expression
is emitted inner-first into the destination, since every operand is either an
input bitmap or the destination itself, so no scratch buffer is needed.

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
  validity calls were never counted as vector ops, so no weight moves.
* Aliasing the output bitmap to an input's buffer (2.34's step two): an Arrow
  ownership and lifetime question at the evaluator, scoped out of this task
  and left in section 2.34.
* Task 64's per-batch range check, which decides *whether* a guard is emitted;
  this task decides what a body without a guard still has to read. 64 widens
  what this task drops, and is the natural next task, not a prerequisite.

### 3.3 Registered op counts

The metric this task moves is not `IntVector` invocations - none is added or
removed in any body - but `VarkaVectorSupport` invocations per masked loop
method, which `VarkaEmitterTestSupport.invocationCount` reads the same way.
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
| `sql/catalyst/.../varka/VarkaLoopEmitterSuite.scala` | the tests in 5 |
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
  describes, which no existing test provokes because the fixtures zero their
  null slots. And `checkMatrix`'s data generator fills every null slot with
  those sentinels by default from this task on, so the whole existing matrix
  becomes sensitive to garbage reaching a guard or a lowering's domain, for
  tasks 42, 52 and 60 as well as this one.
* **The dead-word invariant fires**: a test that emits a shape with a
  deliberately mis-marked word (through a test-only hook, the
  `misdescribeAdd` pattern) and asserts the emit-time exception, so the
  counter in 3.1 is known to be armed.
* **An all-null input through the pass**: an AND root (`datediff(d, d2)` with
  `d` all-null, output all-null without touching `0L`) and an OR root
  (`greatest(d, d2)`, output equal to `d2`'s bitmap).
* **A mixed method**: `year(d)` beside `d < lit` in one loop method; the
  `Cond` keeps its per-group OR and the read stays; the year's write goes.
* **The liveness counts of 3.3**, asserted per method with
  `invocationCount`, and `IntVector` counts unchanged.
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
mixed nulls` (the second control: predicted flat). Each with the switch on
and off, both widths, one regeneration with `dev/varka_bench_regen.sh` on an
idle machine. The dense rows are the bound: no masked row may pass its dense
counterpart.

### 6.1 Predictions, registered before the run

1. `year+month+day+quarter, shared, mixed nulls` closes at least two thirds
   of its gap to the dense row at both widths: from 1124.0 towards 1762.6 and
   from 417.7 towards 792.5. Reason: four writes and one read per group go,
   and nothing else in that body differs from the dense one. Confidence
   medium-high.
2. `year, mixed nulls` moves by under 8% at AVX-512 and under 11% at 128-bit -
   the whole of its remaining gap - and is the smallest mover among the
   served shapes. Confidence high.
3. `add_months(d, 13), mixed nulls` moves within run noise (its dense row is
   0.4% away). Confidence high. `add_months(d, m)` column count moves less
   than `next_day(d, k)` column, because its read stays for the guard.
   Confidence medium.
4. No masked row passes its dense counterpart. Confidence high; a miss is a
   measurement error to explain, not a result.
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
   unshared, 21 now fits and 22 crosses near 8377. The crossing can only move
   outward here, so nothing that compiles today stops compiling. Confidence
   medium-high on the direction and the 21-to-22 move, medium on 47 against
   48. One more thing the same numbers say: after the reads and writes go, the
   44-output masked epilogue is still 7178 bytes against the dense 6574, and
   that residue is the per-input null-state prologue - which is why 3.1 drops
   it with the dead words; the prediction for the one-body test in 5 is that
   with the prologue gone the two differ by under a hundred bytes.

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

1. The engine helpers with their tests, and `pureWord` with the agreement
   test: no emitted byte changes.
2. The pass, the liveness rule and the skipped writes behind the switch, off
   by default; the tests of 5; both widths green.
3. The A/B rows, one regeneration, section 9 with the predictions scored; the
   default set by 6.1's rule; the docs and the milestone rows swept, including
   what task 47 is left with.

## 9. Outcome

Filled in when the measurement lands.
