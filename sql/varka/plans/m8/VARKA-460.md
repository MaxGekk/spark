# VARKA-460: `CondValue`, the condition-to-value bridge

## 0. Why this row exists

Row 331 made a `BOOLEAN` column a condition leaf and built the boolean conditions over it
(`VARKA-331.md`), and left out the node that turns a condition into a value: a value node over a
`Cond`, `1` where the condition is known true, `0` where it is known false and null where it is
unknown. Three rows wait on it and add only compiler arms once it exists: row 332's boolean casts
(`CAST(b AS INT)` and the other integral targets), row 335b's `bit_count(bool)` (Spark's is an
`INT` 1 or 0) and row 27's int-materialized boolean output. Row 475's conformance contract
(`VARKA-475.md` 3.1) needs it too: its `BOOLEAN` checks of the round trip, choice and the cast row
read a boolean as a value. The owner chose on 24 October 2026 to build this row before 3.1.

The design is `VARKA-331.md` 2.6, unchanged; this file records what was built against it.

## 1. Design

`CondValue(Cond cond, LaneType lane) implements VarkaVectorIR`. At each lane:

| condition | value | validity |
| :--- | :--- | :--- |
| known true (`kt`) | 1 | valid |
| known false (`kf`) | 0 | valid |
| unknown | arbitrary | null |

The semantics are Spark's `CAST(c AS INT)` (or `BIGINT` on the long lane), not `IF(c, 1, 0)`:
`IF` takes the ELSE arm under an unknown condition and answers 0, the cast answers null. The
emitted form is one blend of two broadcast constants under the known-true mask; the validity word
is `kt | kf`, the two words `IsNotNull(Cond)` already reads. In the dense body (no input null, a
condition that cannot make an unknown from valid inputs) every lane is valid. A kernel holding it
is masked-only exactly when its condition is not dense-safe, which the analysis reads from the
condition's own flags; there is no new rule.

What the node does not do:

- **No compiler arm.** Nothing in Spark's plans lowers to it in this row; 332, 335b and 27 add the
  arms. So no emitted byte of any existing shape can move, and the bytes oracle
  (`emitted_bytes.json`), `coverage.json` and `width_audit.json` must be byte-identical.
- **Int and long lanes only are tested**, as the row's done-when says. A narrow `CondValue` is
  well defined (0 and 1 fit every lane) but no row asks for one yet.
- **Unpriced.** As for row 331's nodes (`VARKA-331.md` 8, 4b), `VarkaEmitCostTable` gets no entry
  until row 352's refit; the cost model names its feature `CondValue/<lane>` and the price
  table's test lists it as awaiting that refit.

## 2. Sites

The sealed-switch sites a new node is a compile error at, from `VARKA-331.md` 5 (copied there from
`VARKA-336.md` 0.7): `VarkaVectorIR` (`permits`, `laneType`, `canonical`, the render, `childrenOf`,
`withChildren`), `Analysis` (the analysis and the refusal switch, `ownerOf`, `pureOf`), `Slots`
(the consumers, `bodyUses` and the liveness switches), `VarkaVectorWalk`, `VarkaOperandPosition`,
`VarkaEmitBudget`'s weight, `VarkaEmitCost.kindOf`, `VarkaChronoCompiler`'s rearm switch,
`VarkaRangeAnalysis` (the range `[0, 1]`), and in tests `VarkaReference`, `VarkaShrinker` and
`VarkaCanonicalIr`. A `default ->` arm does not fail to compile, so each one in those files is read
by hand and recorded in section 5.

## 3. Tests

The oracle is Spark's `Cast(c, IntegerType)` and `Cast(c, LongType)` for `VarkaReference`, then
`VarkaReference` for the kernels, as `VarkaEmitterBoolSuite` holds its conditions:

- the reference against Spark's `eval` for every state of a condition over boolean leaves;
- kernels with `CondValue` roots, int and long lanes, over every leaf state (true, false, null with
  the data bit clear, null with it set), at the suite's lengths, offsets and widths, in the masked
  and the dense body, each row's value and validity compared;
- a `CondValue` over a `CondConst(NULL)` tree must refuse the dense body;
- `CondValue` feeding a value operator (an `IntArith` add, a `BitCount`), the composition 332 and
  335b will build.

Prediction, at the calibration table's ratios for short emitter tests (VARKA-317, VARKA-281: well
under a second a kernel): the new tests add under 30 s to `VarkaEmitterBoolSuite`.

## 4. Done when

The row's text in `PLAN.md`: the node exists in the IR with its walk, analysis, slots, cost and
reference arms; a hand-built emitter suite holds it to Spark's `eval` over every state of the
condition at the lengths and lanes `VarkaEmitterBoolSuite` uses (dense and masked, int and long);
the sealed-switch sites a new node touches are listed (section 2 and 5); and the three dependants
can build on it without touching the walk.

## 5. Outcome

*24 October 2026. Done.* `CondValue(Cond cond, LaneType lane)` is in the IR, its lane held to its
condition's by the constructor. The emitted form is `zero.add(1, kT)`: a broadcast of 0 and a
masked add of 1 under the known-true mask, which is the blend of two constants the design asked
for in two lane calls (its budget weight is 2, checked by `VarkaEmitterBudgetSuite`'s register
row). The masked body stores `kT | kF` as its own word; the dense body has no unknown lane.

**Sites.** The sealed switches the compiler named, in the order the build found them:
`VarkaVectorIR` (`permits`, `laneType`, `inputOrdinal`, the two narrow-species walks, both
renderings, `childrenOf`, `withChildren`), `VarkaChronoCompiler.rearm`, `VarkaRangeAnalysis`
(the range `[0, 1]` at the int lane), `Analysis` (the refusal switch, the ring rule, `pureOf`,
`analyze` and the operand check), `Slots` (the walk and the propagation), `VarkaOperandPosition`
(both switches), `VarkaVectorWalk`, and in tests `VarkaReference` (both lanes), `VarkaGuardSpecs`,
`VarkaWordCensus`, `VarkaIrGrammar.boundsOf`, `VarkaRangeAnalysisSuite` and `VarkaCanonicalIr`
(the parse arm). Not switches, and so found by the defaults gate rather than the compiler: the
budget weight and the cost kind (written with the switches), and seven tests that list every node
type: the price table's list of nodes awaiting the refit (`VarkaEmitCostSuite`), the two
generator-reach lists (`VarkaIrFuzzSuite`), the rebuild list (`VarkaWithChildrenSuite`), the int
tree, the lane-derivation set and the emittable-lane table (`VarkaLaneTypeSuite`), the weight
register (`VarkaEmitterBudgetSuite`), and the raw-throw scan (`VarkaInternalErrorSuite`, which
caught a bare `IllegalArgumentException` in the constructor). The `default ->` arms over IR
nodes were read by hand: `Analysis.ownerOf` (an own word) and `Slots.planWordRef` (an own slot)
are right for the node and stay; `Analysis.pureOf` and `VarkaEmitCost.kindOf`, which this row
touches, lost theirs and name every node (sql/varka/AGENTS.md's rule for a touched switch), with
the same answers as before. `VarkaShrinker`, on section 2's list, needed nothing: it rebuilds
through `childrenOf` and `withChildren`, which have their arms.

**Not done here, as section 1 planned.** No compiler arm, no grammar production (the IR fuzzer's
reach lists name the node as out, with the boolean nodes, until row 352's refit), no cost-table
price (listed as awaiting the refit). The narrow lanes are refused by the constructor rather
than left untested (review below).

**Tests.** `VarkaEmitterBoolSuite` gained six tests and its harness reads a value root, its
output sized to the root's lane so a store past the batch faults: the reference against Spark's
`Cast(c, IntegerType)` and `Cast(c, LongType)` over every state of ten conditions, `OR` and a
`NULL` constant among them (and `IF(c, 1, 0)` answering 0 where the cast answers null); kernels
over every leaf state at four lanes, eight lanes and the default width, masked and dense, int and
long, and an int-lane `CondValue` widened into a long-driven kernel; `CondValue` under `IntArith`,
`BitCount`, `IfElse` and `Greatest`, and one condition read as a value and as a condition in one
kernel; a `CondConst(NULL)` tree keeping the kernel masked-only; `CondValue` over `AND` and `OR`
alone in its kernel over an all-null column. A planted fault (validity `kT` alone) failed four of
the first five; the sixth was written after them and fails when `And` and `Or` are put back to
not skipping (review below).

**Measured.** The first five tests took 2.0 s in the defaults gate against the 30 s predicted
(ratio 0.07, in the calibration table, which scores the prediction as made); the six take 2.8 s
in the gate after the review. The catalyst defaults gate passes (855 tests), and
`emitted_bytes.json`, `coverage.json` and `width_audit.json` are unchanged.

**Review (24 October 2026).** One read-only review before the push found a silent wrong answer:
the masked driver's all-null shortcut returns a batch as null when every root reads a column
that is null on every row, and trusts `skipping` for a value root. `And` and `Or` were analyzed
as not skipping (a condition root never takes the shortcut), so `CondValue(Or(b0, b1))` alone in
a kernel, with `b0` null throughout and `b1` true on a row, answered null where Spark's
`CAST(NULL OR TRUE AS INT)` is 1. The first sweeps missed it because every kernel there also held
a root that turns the shortcut off. The sixth test runs `CondValue` over `AND` and `OR` alone in
its kernel and failed as described. The first fix made `CondValue` skip on every condition.

**Review of the pull request (`/code-review high`, 24 October 2026).** Nine findings, no wrong
answer in the emitted code. Applied: the root cause rather than the first fix - `And` and `Or`
analyze as skipping, which no existing kernel reads (the flag's one reader excludes a condition
root, and the one value over a condition before this row, `IfElse`, skips itself), and
`CondValue` inherits the flag, so `CAST(b AS INT)` over a column keeps the shortcut; the
harness's outputs sized by lane (all were sized for a long, which hid an int root's overrun);
the narrow lanes refused in the constructor until a row tests them; `OR` and the `NULL` constant
held to Spark's cast; a widened int-lane `CondValue` in a long-driven kernel; the defaults
removed from the two touched switches; the comments that named `IfElse` as the only value over a
condition. Left: the fuzzers' reach (row 352, as for the other boolean nodes), and the masked
body rebuilding the known-true mask per consumer of one condition, as `IfElse` already does - a
cost to measure when the boolean casts land, not a defect.
