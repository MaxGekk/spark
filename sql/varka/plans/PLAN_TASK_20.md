# Task 20: the four gating shapes

Milestone 3 section 2.3 (`PLAN_MILESTONE_3.md`): the compiler-side shapes the
corpus survey named as the cheapest reach levers - `cast(string AS DATE)`
folding (85 sites), `BETWEEN` (41), `In`/`InSet` over the existing date lanes
(118 `IN (` sites), and `Coalesce` (41, with `IS [NOT] NULL` riding it, 21).

## 1. What exploration changed before any code was written

* **Two of the four shapes were already mostly served.** A `cast('...' AS
  DATE)` string literal is constant-folded to a date literal by the optimizer
  long before `VarkaColumnarRule` (a physical-plan rule) runs, and
  `SimplifyCasts` drops identity casts; bare-column `BETWEEN` reaches the
  compiler desugared to `And(GTE, LTE)` and has fused since task 11 (the
  differential suite already pinned it). Their remainder in this task: an
  identity-cast unwrap and a defensive `RuntimeReplaceable` case in the
  compiler (both matter only for hand-built trees - the fusion report, unit
  tests), plus differential coverage including the computed-input `BETWEEN`
  whose `_common_expr_0` hoist stacks two ProjectExecs that fuse
  independently.
* **The `IN` cap must be compiler-side.** The emitter's own limits
  (`MAX_FUSED_NODES` = 64 distinct ops, `MAX_CHAIN_DEPTH` = 16) throw at
  emission time, which the evaluator can only turn into a silent per-batch
  fallback - no task-16 reason, EXPLAIN still claiming fusion. A balanced
  `Or` fold admits at most 32 literals structurally (2n - 1 ops + the
  enclosing `IfElse`); a right-nested fold dies at 15 by depth.
* **`Coalesce` costs one IR record**, not a kernel: `IsNotNull(child)` as the
  first validity-reading - and first *total* - condition. `coalesce(a, b)`
  lowers to `IfElse(IsNotNull(a), a, b)`, whose existing masked validity
  math, `(kT & valid(a)) | (~kT & valid(b))` with `kT = valid(a)`, reduces to
  `valid(a) | valid(b)` - exactly coalesce. `IsNull` is `Not` over it (a
  slot swap, no emitted code); `nvl`/`ifnull` arrive as `Coalesce`; `nvl2`
  arrives as `If(IsNotNull(e1), e2, e3)`.

## 2. Decisions and their bases

* **Cap = 16 deduped literals** (settled with the project owner): 16 is the
  emitter's broadcast-hoist regime boundary, is depth-safe under any fold
  shape, and its 31 op nodes leave half the node budget to the rest of the
  projection. Above it, the entry declines with
  `IN list longer than the fused cap of 16`.
* **The compiler mirrors the emitter budgets** (the design reviewer's
  correction): with cap 16, two INs in one projection are 64 ops and a third
  entry would breach `MAX_FUSED_NODES` - silently, per-batch, at emission.
  `compilePartial` now checks each accepted entry against the running totals
  via `VarkaLoopEmitter.fitsBudgets` and demotes the overflowing entry to
  residual with `exceeds the emitter's fused budget`. This also closes a
  pre-existing hole: a 16-branch `CASE WHEN` or a 17-deep chain already
  reached the emitter and lost the whole kernel the same silent way.
* **`IsNotNull`'s child is restricted to a bare date column** (recorded
  limitation): a column's validity word is unconditionally live at every lane
  group, while a computed node's word materializes only during its value
  walk, which runs *after* condition emission in `IfElse` order. So every
  `coalesce` operand except the last must be a bare column;
  `coalesce(date_add(d, 1), e)` declines with
  `validity predicate over a computed operand` semantics (its own reason
  string for the coalesce shape).
* **`In` literals are deduped and sorted ascending** before slot assignment:
  `InSet` hands values over as an unordered `Set[Any]`, and Kleene OR is
  commutative, so canonical order changes no semantics while keeping the
  literal slots and the shape hash deterministic. The balanced fold shape is
  pinned by a compiler test (a later "cleanup" to `reduceRight` would
  silently reintroduce the depth wall at 15 literals).
* **A null `IN` element declines** rather than being modeled: SQL's
  `x IN (..., NULL)` turns every no-match into unknown, an algebra entry the
  EQ-OR chain does not have. `OptimizeIn` does not remove null literals, so
  the guard is the compiler's.

## 3. Predictions, registered before the benchmark run

The new `VarkaInExpressionBenchmark` (columnar-terminal `CASE WHEN d IN`,
plus stock-shaped filter anchors; the upstream `InExpressionBenchmark`
committed files are EPYC machines - the JDK-25 file reads 31.2 -> 8.6
M rows/s over 5 -> 500 dates - so the same-run Janino cases are the
baseline):

1. **Fused IN-5** lands in the `CASE WHEN` family's band: 5-8x Janino
   (branch-free EQ-OR chain vs a per-row 5-way comparison cascade).
2. **Fused IN-16** narrows but still wins: 2-5x. Janino's side crosses the
   optimizer's `InSet` threshold at 10 and becomes a hash-set/switch probe,
   while the varka side pays 31 vector ops and the >16-literal broadcast
   regime does not apply at exactly 16.
3. **Over-cap 50** is parity, 1.0x: both sessions run Janino; the decline
   costs a plan-time compile attempt, nothing per row.
4. **The filter anchors** are parity at every size: unfused on both sessions
   until task 21.

## 4. Measurement

`VarkaInExpressionBenchmark` generated twice back to back on the otherwise
idle committed-numbers machine; every case agrees between the runs by
minimums within 1 ms, and the committed file is the second run:

| case (2M Arrow-cached rows) | baseline best | varka best | relative |
|---|---|---|---|
| case-when IN, 5 literals, fused | 53 ms | 15 ms | 3.5x |
| case-when IN, 16 literals (the cap), fused | 49 ms | 12 ms | 4.0x |
| case-when IN, 50 literals, declined | 49 ms | 49 ms | 1.0x |
| filter IN anchor, 5 / 50 / 200 / 500 | 28-43 ms | 28-42 ms | 1.0x |

The parity rows are sub-1.3x claims by definition and hold in both runs.
One observation recorded, not claimed: the varka side reads slightly
*faster* at 16 literals than at 5 (12 vs 15 ms best, both runs) - both
sit inside the broadcast-hoist regime, and the gap is near the noise band;
nothing in this task rests on it.

## 5. Outcome

Status: **DONE.** The milestone row's gates: differential over the survey
shapes green (93 sql tests, 63 catalyst at both vector widths), `IN` at
5/16/17/50 including the cap boundary, coalesce over every null pattern
including all-null and no-null, the Kleene rules with a validity predicate
among `And`/`Or` operands pinned at the emitter level, decline reasons
asserted through the fusion report, and the columnar-terminal benchmark
committed against its same-run baseline with the upstream file as shape
reference.

### 5.1 Predictions scored: 3 of 4

1. **Fused IN-5 at 5-8x: wrong.** 3.5x. The direction held but the band did
   not - the varka side pays 15 ms where plain `CASE WHEN` pays 8-9, so the
   11-op EQ-OR chain costs more per row than the branch-misprediction model
   allowed for. Running score: 9.5 of 16.
2. **Fused IN-16 at 2-5x: right.** 4.0x.
3. **Over-cap 50 at parity: right.** 1.0x, both runs.
4. **Filter anchors at parity: right.** 1.0x at every size, both runs.

### 5.2 Deviations and findings

* No design deviation from sections 1-2; the identity-cast unwrap turned
  out doubly defensive (`SimplifyCasts` also removes identity casts in real
  queries), kept for the fusion report and hand-built trees.
* The budget mirroring changes one pre-existing behavior deliberately: a
  17-deep chain or an over-64-op projection now demotes the offending entry
  to residual with `exceeds the emitter's fused budget`, where before the
  whole kernel silently fell back per batch at emission.
* The all-node-types golden hash was re-pinned for the 15th record
  (`612c94d132690dc2`), per the suite's update rule.

## 6. Explicitly out of task 20

Filters and selection vectors (task 21 - `IN` here is condition-position
only, and the filter anchors in the benchmark stay unfused by design);
boolean outputs (milestone 4 item 5); string lanes (`cast(stringCol AS
DATE)` stays declined); `IsNotNull` over computed operands (the recorded
restriction above); `EqualNullSafe` (still its own algebra entry or
nothing); any change to the emitter's budget constants.
