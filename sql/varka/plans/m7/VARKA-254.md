# VARKA-254: The grouping weights against the emitted counts

## 1. Where this came from

Row 254 of `m7/PLAN.md`, from item 63 of `m8/SCOPE.md` (added from VARKA-209's build). The emitter
splits a projection's outputs into groups, one loop method each. Its first rule prices each node by
`VarkaEmitBudget.weightOf`, a hand-kept register of the vector operations a node's lowering emits,
and closes a group when the sum would pass `GROUP_BUDGET` (16). An output that reuses what the
group already computes - a calendar prefix, or whole nodes under the default `shareWholeNodes` -
may join up to `FUSED_CEILING` (400). The cost model's prediction (VARKA-199, VARKA-236) and the
measured byte and call-site limits can close a group earlier, never later.

Item 63 found the register over-counting a cheap calendar tail and asked for one of two things:
every weight equal to the count its node emits alone, pinned by a test; or the register retired and
the grouping read from measured costs. Its second clause: the shared-prefix ladder's group counts at
each ceiling are the ones the measured costs give. `m7/PLAN.md` open question 2 ties the choice to
VARKA-236's planner, which reads the same costs.

## 2. The admission check, done

**The census** (12 October 2026, a throwaway suite, not committed). Every IR record emitted alone at
the defaults, its `loopDense0` (`loopMasked0` where no dense body exists) read per Vector API class.
The weights' unit is the one both existing register tests read - `IntVector`, `LongVector`,
`DoubleVector` and `Vector` calls, not `VectorMask` - net of the operands' loads, the literals'
broadcasts and the output's store:

| node | weight | emitted | | node | weight | emitted |
|---|---:|---:|---|---|---:|---:|
| `DayOfWeek` | **1** | **16** | | `IntArith` WRAP / FAIL / NULL | 1 / 5 / 5 | 1 / 5 / 5 |
| `WeekDay` | **1** | **15** | | `IntNeg` WRAP / FAIL | 1 / 2 | 1 / 2 |
| `GuardedDay` | 1 | 2 | | `AddDays`, `SubDays`, `DateDiff` | 1 | 1 |
| `BoundedDivide` | 1 | 2 | | `Greatest`, `Least`, `IfElse` | 1 | 1 |
| `InRanges` | 6 | 3 | | `Compare`, `NarrowLane` | 1 | 1 |
| `DayOfWeekIso` | 18 | 16 | | `NextDay` | 15 | 15 |
| `ThursdayOf` | 19 | 17 | | `ConstDivide` int / long | 11 / 14 | 11 / 14 (the larger form) |
| calendar prefix | 31 | 29 | | `And`, `Or`, `Not`, `IsNotNull` | 1 | 0 (mask calls only) |

The calendar nodes are each 2 over, all from the prefix: 31 counted the column's load and the
output's store, which every other weight leaves out. The four field tails share one constant at the
widest (7) where they emit 4, 5, 5 and 7. `MakeDate` (60) is not yet read: under `failOnError =
false` it builds only a masked body; 8.2 reads the dense one under `true`. `WeekOfYear` checks out
at 2 + 17 + 29 + 16 = 64.

**What the corrections do to the grouping.** `VarkaEmitCostAudit` counts loop methods over the
corpus. With the census weights applied (masks excluded), on the four corpora that move:

| corpus | master | weights corrected, budget 16 | only `DayOfWeek`/`WeekDay` | corrected, budget 400 |
|---|---:|---:|---:|---:|
| wide int, planned | 8886 | 9152 (+3.0%) | 9142 | **2194 (-75%)** |
| wide compositions int, planned | 5006 | 5150 (+2.9%) | 5137 | **1238 (-75%)** |
| fuzz int | 1639 | 1656 (+1.0%) | 1655 | **1019 (-38%)** |
| wide long, planned | 6545 | 6545 | - | 1803 (-72%) |

So the under-counts dominate: correcting `DayOfWeek` and `WeekDay` alone accounts for nearly all of
the increase, and the calendar, guard and division corrections roughly cancel. And the budget is the
larger lever: at 16 it closes about three groups in four that the prediction would admit. Its
reason (`VARKA-71.md` 10.5, and the constant's own doc) was to keep methods below C1's refusal, which
`CALL_SITE_BUDGET` and the planned prediction now do on the built class. Builds rise under budget
400 (100 to 139 on the wide int corpus under the prediction), since more groups reach the limits.

**What it does to speed.** `VarkaGroupingBenchmark`, committed on unchanged code as its own pull
request, holds four shapes at budget 16 and 400. An uncommitted trial with the census weights
applied, at 256 bits on the laptop:

| shape | master | corrected, budget 16 | corrected, budget 400 |
|---|---|---|---|
| `dayofweek`/`weekday` over two dates + 4 `date_add` | 2 methods, 0.35 ns/row | **5, 0.51 (-32%)** | 2, 0.35 |
| 40 mixed outputs | 10, 1.98 | 16, 2.10 (-6%) | **8, 1.74 (+14%)** |
| 24 `date_add`, 8 checked sums (controls) | unchanged | unchanged | unchanged |

Correct weights alone make these shapes slower; correct weights with the budget lifted make them as
fast or faster. The controls build the same methods in every arm, but the checked sums at budget 400
read 6% slower at 256 bits and 17% at 128 on master itself, with one loop method in both arms, which
8.3 has to explain.

**Answer to open question 2.** Not retired: the prediction is fitted (VARKA-199 9.4: a register of
costs measured alone does not predict a wide group) and it judges a group, where the grouping needs a
price per node to walk with; row 200's exact partition needs one too. So the weights are retuned and
pinned, and the budget they are summed against is decided by measurement, below.

## 3. The design

### 3.1 The unit

A weight is the Vector API calls on a lane type (`IntVector`, `LongVector`, `DoubleVector`,
`Vector`) its node adds to a dense loop body alone, net of the operands' loads, the literals'
broadcasts and the output's store; `VectorMask` calls are not counted, as neither register counted
them, and a calendar node's weight is the prefix in its full form plus its tail, at the default
month axis. That is the unit `GROUP_BUDGET` and `FUSED_CEILING` were read in, so neither moves for a
unit change. A connective (`And`, `Or`, `Not`, `IsNotNull`) keeps weight 1 as a floor: it emits only
mask calls, and a weight of 0 would make it free to join any group.

### 3.2 The weights

`DayOfWeek` 16 and `WeekDay` 15 get their own constants; `GuardedDay` 2, `BoundedDivide` 2,
`InRanges` 3, `DayOfWeekIso` 16, `ThursdayOf` 17; `CHRONO_PREFIX_WEIGHT` 29; the field tails split
into `YEAR` 5, `MONTH` 4, `DAY_OF_MONTH` 5 and `QUARTER` 7; `MakeDate` from 8.2's reading. Every other
weight already holds.

### 3.3 The register test

`VarkaEmitterBudgetSuite`'s two register tests become one, over every record the sealed
`VarkaVectorIR` permits (read through `getPermittedSubclasses`, as `VarkaWithChildrenSuite` does), so
a new node without a register row fails it. Each row is a node emitted alone; the test asserts its
count in 3.1's unit equals `weightOf`, names the constant to recount when it does not, and keeps the
calendar pairs beside `month(d)` that pin the tails. Tagged `PinsDefaults`: the weights are the
default options' counts (the division's larger form is read under `withUseAVX(2)`, as today).

### 3.4 The budget, two arms

* **A.** The corrected weights against `GROUP_BUDGET` 16, as today.
* **B.** Under `predictGrouping`, the first rule's bound is the fused ceiling instead of the budget:
  the prediction and the measured limits bound a group, and the ceiling still bounds a method. A
  new emit option, `groupBudgetUnderPrediction`, false for B; with the prediction off, the budget
  binds as today in both arms.

Both are built; 6 picks the default, and the losing arm stays an option the matrix covers, since
the winner may depend on the machine.

### 3.5 The ladder clause

`VarkaEmitterBudgetSuite` gains a test over the shared-prefix ladder's two shapes
(`VarkaSharedPrefixBenchmark`: 64 cheap tails and 60 `make_date`) at ceilings 400, 200, 100 and 50:
the group counts the weights give equal the counts the measured costs give, read as the cost
model's predicted grouping without the weights' bound. The counts are pinned.

### 3.6 What is deliberately unchanged

`FUSED_CEILING`, `CALL_SITE_BUDGET`, the byte budget, the prediction and its margins, and every
lowering. No answer changes: the grouping decides only which loop method computes an output.

## 4. Files

| file | what |
|---|---|
| `VarkaEmitBudget.java` | the weights, 3.2 |
| `VarkaEmitOptions.java`, `VarkaLoopEmitter.java` | arm B's option and the first rule, 3.4 |
| `VarkaEmitterBudgetSuite.scala` | the register over every node, the ladder test, 3.3 and 3.5 |
| `sql/varka/matrix/configs.tsv` | arm B's losing value as a configuration |
| `sql/varka/emitted_bytes.json`, `sql/varka/emit_cost_audit.json` | regenerated |
| `sql/catalyst/benchmarks/VarkaGroupingBenchmark-*`, `VarkaSharedPrefixBenchmark-*` | regenerated |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-254.md` | the row and this plan |

## 5. Tests, and what each is for

* **The register (3.3)**: every weight equals its node's count, and a node without a row fails.
* **The ladder (3.5)**: item 63's second clause.
* **The bytes oracle**, regenerated: in a first trial (with mask calls counted, so heavier weights than 3.1's) 170
  blocks of 100 shapes moved, which is the
  regrouping and not a lowering change; the plan's 9 records the counts.
* **The suites that pin a group layout** (`PinsDefaults`) are updated where the corrected weights
  or the chosen arm move them, each change named in 9. In that trial only the calendar register
  failed at budget 16, of `VarkaEmitterBudgetSuite` and `VarkaGroupingBoundSuite`.
* **Every emitter and fuzz suite at both widths** in the gate, and the option matrix with arm B's
  configuration: no answer may change.

## 6. The measurement

`VarkaGroupingBenchmark` and `VarkaSharedPrefixBenchmark` at both widths, base against head through
`dev/varka_bench_pair.sh`, and the audit's loop-method counts. Arm B becomes the default if no
benchmark row is slower than master by more than 3% at either width and the corpus builds fewer loop
methods; otherwise A, with B kept as an option.

### 6.1 Predictions, registered before the run

1. **Arm A is slower on the weekday shape**: 5 loop methods against 2, at least 25% slower at 256
   bits; the mixed shape builds 16 and is 3 to 10% slower.
2. **Arm B is as fast on the weekday shape** (within 3%, 2 methods) **and faster on the mixed
   shape**: 8 methods, at least 10% faster at both widths.
3. **The corpus**: arm A builds 1 to 3% more loop methods, arm B 35 to 75% fewer, with more builds
   under B (the regroups the measured limits ask for).
4. **The shared-prefix ladder is unchanged** under both arms: its groups are bounded by the ceiling
   and the call-site budget, not by the group budget.
5. **The checked-sums control** reads the same under A and B as on master: B changes no method of
   it, and its budget-400 deficit is a property of the code emitted under that option, which 8.3
   reads.

## 7. Risks

1. **Wider methods spill.** Arm B's methods hold more live vectors; at 128 bits there are as many
   registers and twice the operations a row, so a spill shows there first. The 128-bit rows are the
   check, and a row past 3% sends the default to A.
2. **The prediction's margins were set on the old groupings.** VARKA-236 read its margins (21.1% on
   bytes, 17.7% on call sites) where each limit binds. Arm B puts more groups in that band, so more
   built classes may measure over a limit and rebuild; the audit's build counts are the check.
3. **Compile time and warm-up.** Fewer, larger methods move C2's work around; the warm-up suites in
   the gate's quiet step are the check.
4. **Scope.** If arm B wins it changes the default grouping everywhere, which is more than the row's
   size. If its measurement or its fallout outgrows the row, B becomes its own row and 254 lands as
   A with B off by default - recorded here, not decided now.

## 8. Sequencing

1. `VarkaGroupingBenchmark` on unchanged code, its own pull request (#746).
2. This plan, and the row marked Planned.
3. The census holes: `MakeDate`'s dense count under `failOnError = true`; the long lane under both
   division forms.
4. Why the checked sums read slower at budget 400 with one loop method in both arms: the two
   classes compared method by method.
5. The weights, the register and the ladder test; arm B's option.
6. Section 6 at both widths, the default chosen, the oracles and results files regenerated.
7. Section 9.
