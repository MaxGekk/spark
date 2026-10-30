# VARKA-481: The checked narrow multiply through the range guard

## 0. What exists now (admission check)

From master at `ef32e007d3a` (30 October 2026). Varka paths are under
`sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen/` unless named;
Spark's facts were read in the fork's own sources under `sql/catalyst/src/main/scala/`. The row's
text is `PLAN.md` row 481, from `VARKA-329.md` 9.8 (the reviews' cut finding, "the five special
cases of the checked narrow multiply").

**Spark, the answers this row must keep.**

1. **The result type is the left operand's.** `BinaryArithmetic.internalDataType` is
   `left.dataType` for every non-decimal pair (`expressions/arithmetic.scala:218-222`, `dataType`
   at `:239`), and analysis gives both operands the same type, so `TINYINT * TINYINT` is `TINYINT`
   and `SMALLINT * SMALLINT` is `SMALLINT`. There is no widening to `INT`.
2. **LEGACY wraps at the type's width.** Generated code is `(byte)(eval1 * eval2)`
   (`arithmetic.scala:333-335`); the interpreted path is `numeric.times` with
   `implicitly[Numeric[Byte]]` (`types/PhysicalDataType.scala:154`, the short twin at `:355`).
3. **ANSI raises `BINARY_ARITHMETIC_OVERFLOW`, not `ARITHMETIC_OVERFLOW`.** `failOnError` is true
   under ANSI and TRY (`arithmetic.scala:225-228`); generated code calls `ByteExactNumeric.times` or
   `ShortExactNumeric.times` (`:321-332`), and the interpreted path reaches the same object through
   `PhysicalByteType.exactNumeric` (`PhysicalDataType.scala:155`, `:356`). `times` computes the
   int product and raises where it leaves `[MIN, MAX]` of the type (`types/numerics.scala:26-31`,
   `:45-49`; shorts `:57-68`, `:81-85`) through
   `QueryExecutionErrors.binaryArithmeticCauseOverflowError(x, "*", y, "try_multiply")`
   (`errors/QueryExecutionErrors.scala:672-686`): error class `BINARY_ARITHMETIC_OVERFLOW`,
   SQLSTATE `22003`, parameters `value1`, `symbol` (`*`), `value2` and `functionName`
   (`` `try_multiply` ``), message "`<value1> <symbol> <value2>` caused overflow. Use
   `<functionName>` to ignore overflow problem and return NULL."
   (`common/utils/src/main/resources/error/error-conditions.json:444-449`). The one overload takes
   `Short`, and the values render through `toSQLValue(v, ShortType)`
   (`errors/QueryErrorsBase.scala:55`, `expressions/literals.scala:670`), so a `TINYINT` overflow
   prints its operands with the `S` suffix: `100S * 2S caused overflow`. Varka never builds this
   text: the batch declines and the row engine raises it, so the parameters are Spark's by
   construction.
4. **TRY nulls the row.** `try_multiply` over numerics is `Multiply(l, r, EvalMode.TRY)`
   (`expressions/TryEval.scala:268-274`); `BinaryArithmetic.nullSafeCodeGen` wraps the same exact
   call in `try { } catch (Exception e) { isNull = true; }` (`arithmetic.scala:356-373`) and `eval`
   catches to null (`:376-396`). A null operand is a null result in every mode.

**Varka, what the row replaces.**

5. **The compiler builds a narrow `IntArith`.** `narrowArith` compiles both operands on the node's
   narrow lane and calls `arithOver` (`VarkaNodeCompiler.java:1045-1065`), which returns
   `IntArith(MUL, FAIL|NULL, x, y)` on the byte or short lane; its decline of a checked multiply
   is skipped for a narrow node by `&& !narrow` (`:1119`, special case 2).
6. **The drive lane walks the tree a second time.** `VarkaKernelPlan.emissionLane` returns `INT`
   where `holdsCheckedNarrowMultiply(root)` finds such a node (`varka/VarkaKernelPlan.java:179-181`,
   the walk at `:228-258`, special case 1). The row and 9.4 place the walk in `VarkaVectorIR`; it
   has since moved to the kernel plan.
7. **Analysis refuses it by a lane test.** `options.checkIntOverflow() && n.mode() == FAIL &&
   (n.op() != IntOp.MUL || n.laneType().isNarrow())` (`varka/Analysis.java:676-680`, special case
   3), under `Refusal.INT_OVERFLOW`, status `STATUS_INT_OVERFLOW`
   (`varka/VarkaFusedKernel.java:77`).
8. **The walk keeps two throws for it.** `emitIntArith` throws on a checked `MUL` only where
   `!narrow` (`varka/VarkaVectorWalk.java:661-663`, special case 4), and
   `emitNarrowOverflowMask` throws on a `MUL` that reached the narrow lane (`:740-743`, special case
   5). The int-lane branch of the same method (`:727-737`) is two compares against the node lane's
   range and an `or`, the sequence `emitRangeGuard` emits (`:1021-1035`) over a parked local.
9. **The general form exists for `CAST(bigint AS INT)`.** Under ANSI it is
   `NarrowLane(GuardedRange(x, Integer.MIN_VALUE, Integer.MAX_VALUE))` (`VarkaNodeCompiler.java:
   701-719`); under TRY it declines naming row 332 (`:707-710`), because `GuardedRange` has no
   `NULL` mode. `GuardedRange(child, lo, hi)` (`varka/VarkaVectorIR.java:536-543`) passes its child
   through and declines the batch where a live lane is outside; its javadoc calls the check
   unconditional (`:532-534`). It refuses `VALUE_RANGE` (`Analysis.java:688`), status
   `STATUS_VALUE_RANGE` (`VarkaFusedKernel.java:83-87`), shares its child's word (`Analysis.java:
   1086`, `:1145`; `varka/Slots.java:939`) and is built at 54 sites in main and test code.
10. **A narrow column's range is its type's; nothing narrow above a column is ranged.**
    `VarkaRangeAnalysis.range` gives a narrow `ColumnRef` its type's interval (row 480,
    `varka/VarkaRangeAnalysis.java:216-227`) and `UNKNOWN` to every other node off the int lane
    (`:229-236`), and `WidenLane` carries the child's range up (`:280-283`). So ANSI
    `CAST(y1 AS INT) * CAST(y2 AS INT)` fuses as a `WRAP` multiply today, while a `WidenLane` over
    a nested narrow node is `UNKNOWN`.
11. **The bytes oracle pins four rows of it at each width**: `y1 * y2`, `s1 * s2` (the coverage
    session is ANSI) and the two `try_multiply` rows (`sql/varka/emitted_bytes.json`, `y1 * y2` at
    lines 585 and 4281), and `option_arms`'s digests. The fuzz sequences draw no narrow arithmetic
    (`VARKA-329.md` 9.9).
12. **The proofs and sweeps exist.** `narrow_range.smt2`, ten checks, says its widened-arm compare
    covers the multiply "formed in the int lane" by `holdsCheckedNarrowMultiply` (rendered by
    `VarkaProofFiles.narrowRange`, test tree, lines 2469-2529). `VarkaEmitterNarrowArithSuite` runs
    every `TINYINT` pair (2^16 rows) per operator, mode and arm on every build, a 2^24-row
    `SMALLINT` slice per build, and all 2^32 `SMALLINT` pairs as opt-in tests; the first 2^32 sweep
    took 1106 s on the laptop (`results/VARKA-330-sweep.txt`).
13. **No benchmark times a checked narrow multiply.** `VarkaNarrowEmitBenchmark` (test tree,
    `org/apache/spark/sql/`) has seven shapes, none `ANSI` or `TRY` `*`; 330's own measurement of
    the arithmetic was not carried (`VARKA-329.md` 9.4, "Findings and limits").

### What differs from the row text and the brief

- **The error class is `BINARY_ARITHMETIC_OVERFLOW`** (item 3), not `ARITHMETIC_OVERFLOW`, which
  is the negation's (`MathUtils.negateExact`).
- **`holdsCheckedNarrowMultiply` is in `VarkaKernelPlan`**, not `VarkaVectorIR` (item 6).
- **The inner multiply is built `WRAP` directly, not through `arithOver`** (2.2): with item 10,
  `arithOver` would decline `(y1 + y2) * y3`, which fuses today.
- **The repeated compare does not go away with the multiply** (item 8): the widened arm's add and
  subtract still reach it. 2.7 names the bytes-neutral way to share it.
- **The status bit changes** from `STATUS_INT_OVERFLOW` to `STATUS_VALUE_RANGE` (2.5).
- **`checkIntOverflow=false` stops wrapping the multiply** unless the plan does something about it
  (2.7, point 1).

## 1. Goal and non-goal

An `ANSI` or `TRY` `TINYINT`/`SMALLINT` multiply compiles to
`NarrowLane(GuardedRange(IntArith(MUL, WRAP, WidenLane(x, INT), WidenLane(y, INT)), MIN, MAX,
mode))`, a tree the int-lane rule drives with no special case of its own; `GuardedRange` gains the
`NULL` mode that `TRY` needs; the five special cases are deleted; every answer is Spark's, on both
arms. Not this row: `try_cast` of a bigint to int (332, which this row's `NULL` mode unblocks), the
narrow add and subtract (they keep `add != sadd` natively and the int compare widened), the int32
checked multiply through the long lane (`VARKA-104.md` 3.3 and 8 step 5, the same form one lane up),
and the cost model's refit (352).

## 2. Design

### 2.1 The tree

| mode | today | after |
| :-- | :-- | :-- |
| LEGACY | `IntArith(MUL, WRAP, x, y)` on the narrow lane | unchanged: the native byte (short) `MUL` wraps |
| ANSI | `IntArith(MUL, FAIL, x, y)`, narrow lane, driven at int | `NarrowLane(GuardedRange(IntArith(MUL, WRAP, WidenLane(x), WidenLane(y)), MIN, MAX))` |
| TRY | `IntArith(MUL, NULL, x, y)`, narrow lane, driven at int | the same with `GuardedRange`'s mode `NULL` |

`MIN` and `MAX` are the node lane's, read from `Lane.of(lane).mostNegative()` and
`mostPositive()`, never written as constants. `narrowArith` builds the form when `op == MUL` and
the declared mode is not `WRAP`; `arithOver` is not called for it. The tree's lanes then hold
`INT`, so `emissionLane` answers `INT` at its existing test (`VarkaKernelPlan.java:179-180`) and the
second walk goes; `kernelLane` already joins such a root with narrow and int roots at the int lane.
At the root, `NarrowLane` narrows at the store (`Analysis.narrowsAtStore`), the same `I2B`/`I2S`
store the old node took; under it, `WidenLane` of a narrow column or literal in an int-driven kernel
is the widening load and emits nothing (`VarkaVectorWalk.java:278-285`).

### 2.2 The product in the wider lane, and why `WRAP` is exact

On a live lane both operands are values of the narrow type: a column by representation, a literal
by type, a nested narrow node because every narrow value's live lanes are reduced or checked before
a non-ring consumer reads them, and a `WidenLane` is one (`VARKA-329.md` 4.2; `Analysis.wrapAt`).
So the int product is at most `128 * 128 = 2^14` for bytes and `32768 * 32768 = 2^30` for shorts in
magnitude, inside the int range, and an int `MUL` in `WRAP` computes it exactly. A null lane may
hold anything - a `TRY` node below leaves its out-of-range result in its null lanes - and its
product may wrap; that is harmless, because the guard reads no null lane (2.3). The fact is the type
bound on live lanes, which the range analysis does not carry above a column (0, item 10,
deliberately: it is what keeps a checked narrow add's check), so the compiler states it by building
`WRAP` rather than asking `cannotOverflow`. `narrow_range.smt2` proves the exactness (3.2).

### 2.3 Raise versus decline, and the `NULL` mode

**What declines a batch.** Under `ANSI` a live lane outside `[MIN, MAX]` folds into the guard
accumulator through `emitGuardCollect`, ANDed with the guard's word (its child's: the operands' AND)
and the epilogue mask, and with the enclosing arms' condition where every use sits under one; the
kernel returns `STATUS_VALUE_RANGE`, the evaluator recomputes the batch on the row engine, and Spark
raises item 3's error on the first such row. Nothing in Varka raises: a decline is the only answer
a kernel gives to a value Spark throws on. A null lane never declines. The untaken-arm cliff of
`emitIntArith`'s javadoc applies unchanged: a guard in an arm the row does not take can decline a
batch whose answers are right, which costs the fusion and not an answer.

**`TRY`.** `GuardedRange` gets a mode: the record becomes `GuardedRange(child, lo, hi, Overflow
mode)` with the compact constructor refusing `WRAP` (a guard that does not guard), and the three-
argument constructor kept as `FAIL`, so the 54 existing sites do not change. Under `NULL`:

- **Analysis**: no refusal (`collectRefusals`, `Analysis.java:688`, refuses `VALUE_RANGE` under
  `FAIL` only); the node owns its word (`ownerOf`, `:1086`, `WordOwner.Own`), is not a pure word
  (`pureOf`, `:1145`, `null`), and sets `nullsFromValidInputs` (`analyze`, `:1286`), so the kernel
  has no dense body - `IntArith`'s `NULL` arm is the model at each site (`:1073`, `:1130`,
  `:1336-1342`).
- **Slots**: a word slot of its own (`wordRef`, `Slots.java:939`, as `IntArith` at `:955`), its
  word demanded (`:1487`, as `:1455`), and the guard's scratch planned as for `FAIL`.
- **Emission** (`VarkaVectorWalk.java:306-326`): the two compares and the `or`, then the `NULL` arm
  of `emitOverflowMask` (`:899-955`) - `word &= ~mask`, no epilogue AND, a throw in a dense body -
  instead of `emitGuardCollect`. The value passes through as under `FAIL`.
- **The canonical form** renders the mode only when it is `NULL` (`VarkaVectorIR.java:1504`, the
  line form at `:1595`), so no existing shape hash moves; `withChildren` (`:1757`) carries it.
- **The cost feature** is `GuardedRange/INT/NULL` under `NULL` (`VarkaEmitCost.kindOf`, `:120`),
  unpriced until 352; the `FAIL` feature keeps its name and its fitted row.
- **The tests' models**: `VarkaReference` evaluates the mode, `VarkaGuardSpecs` treats a `NULL`
  guard as no refusal (its `VALUE_RANGE` bound, test tree lines 356-361, is `FAIL`'s),
  `VarkaCanonicalIr`, `VarkaWordCensus` and `VarkaIrGrammar` learn the component.

`VarkaRangeAnalysis` reads `[lo, hi]` off a `GuardedRange` (`:278`), which under `NULL` holds on
live lanes only - the qualification 4.2 of `VARKA-329.md` already attaches to a narrow bound.
Nothing reads it off a null lane today; the javadoc says so.

### 2.4 The five special cases, removed

1. `holdsCheckedNarrowMultiply` and its call in `emissionLane`: deleted.
2. `arithOver`'s `&& !narrow` on the `MUL` decline: deleted, and its javadoc's narrow paragraph
   rewritten - no narrow `MUL` reaches `arithOver` in a checked mode.
3. `Analysis`'s clause: back to `n.op() != IntOp.MUL`, its comment shortened to the int lane.
4. and 5. The walk's throws: `IntArith`'s compact constructor refuses a checked `MUL` on a narrow
   lane (point 5 of 2.7), so `emitIntArith`'s throw is the int lane's alone and
   `emitNarrowOverflowMask`'s `MUL` throw is gone; the comments that explain the exception
   (`VarkaVectorWalk.java:597-631`, `:648-660`, `:716-724`, `:917-925`) are rewritten to the code
   as it is.

### 2.5 What moves, said before it moves

- **Status and refusal**: the multiply's decline is `VALUE_RANGE`, status bit `1 << 5`, where it
  was `INT_OVERFLOW`, `1 << 3`. `VarkaGuardSpecs` already reads a `GuardedRange` (its `fires`
  arm), and the differential suite's "declined outside with the verdict's status" reads the verdict
  the spec computes; both are run, and any test pinning the old bit is changed with a line saying
  why.
- **Bytes**: the four coverage rows at both widths and the `option_arms` digests move once
  (row 481's text). The `FAIL` body parks one vector where it parked three (`a`, `b`, `r`); the
  compares are the same.
- **Weights**: `weightOf` gives the old node 5 (`VarkaEmitBudget.java:611`) and the new tree 6
  (`WidenLane` 1 twice, the `WRAP` int `MUL` 1, `GuardedRange` 2, `NarrowLane` 1, `:622-637`) -
  conservative, since both widenings emit nothing. Not tuned here: 352's list gets the line.
- **The cost model**: the node was the unpriced `IntArith/MUL/FAIL/BYTE`; the tree is priced
  features (`GuardedRange/INT`, `VarkaEmitCostTable.java:137`; the int `MUL`) plus the unpriced
  `TRY` one. The predicted cost of these four shapes therefore changes, which can move a grouping;
  the bytes oracle shows it if so.

### 2.6 Both arms

The IR no longer depends on `narrowLanes` for this node, and both arms drive the tree at the int
lane, as they did; the prediction is that the two arms emit identical class bytes for each of the
four rows, which the suite asserts (3.1). Where a width cannot widen a narrow column (no baked lane
count, `validityByWidth=false`), the entry declines as before, through `narrowRefusal`.

### 2.7 Points the code does not settle

Each has a recommendation; each waits for the lead's review before the build.

1. **`checkIntOverflow=false`.** The option makes a checked node wrap (`VarkaEmitOptions.java:
   212-218`; `Analysis.loweredUnchecked`, `:817-819`), and `GuardedRange` is unconditional (0, item
   9). After the move the multiply raises and nulls under the option, its lines in
   `sql/varka/matrix/skips.tsv` go stale (six of the coverage harness over the four rows, four of
   the narrow arithmetic suite), and the option loses its off arm for `*`. (a) Accept: remove the
   lines, say in the option's javadoc that a narrow multiply's check is a range guard. (b) The
   compiler reads the option and builds the bare `NarrowLane(IntArith(MUL, WRAP, ...))` with it off,
   keeping the A/B. (c) A purpose flag on `GuardedRange`. *Recommended: (b)*: the option's promise
   is "the same node, cheaper", the compiler already has the options, and (c) adds a field every
   guard carries for one caller.
2. **The interior `NarrowLane`.** Away from a store, `NarrowLane` to a narrow target emits the shift
   pair (`VarkaVectorWalk.java:287-301`), where the old checked node left its in-range result as it
   was: two lanewise ops per interior checked multiply (`y1 * y2 < y3`). (a) Accept and measure.
   (b) Emit nothing where the child is a `GuardedRange` whose bounds lie in the target's range: on a
   live lane the value is already a narrow value, and a null lane's junk is what the old form left
   too. *Recommended: (b)*, with a coverage row for the interior shape under `TRY` beside a null.
3. **The repeated compare.** Item 8's int-lane branch and `emitRangeGuard` emit the same four
   calls over a parked vector; one helper `emitOutsideRange(cb, lane, slot, lo, hi)` used by both is
   bytes-neutral, which the oracle checks. The row names the repetition; its done-when does not.
   *Recommended: the helper, in this row*, proven by an unchanged oracle before the tree moves.
4. **The two arms' sweeps.** If 2.6's prediction holds, the `*` sweeps on the two arms run one class
   twice (question 1).
5. **Where the narrow `MUL` refusal lives.** (a) `IntArith`'s compact constructor refuses a checked
   `MUL` on a narrow lane. (b) Keep a throw in the walk. *Recommended: (a)*: the form then cannot be
   built, by the compiler or a test; the int lane's checked `MUL` stays constructible
   (`VarkaReproductionSuite`, `VarkaLaneTypeSuite` and `VarkaRangeAnalysisSuite` build one).

## 3. Tests and proofs

### 3.1 The suites

- `VarkaEmitterNarrowArithSuite`: unchanged answers - the named cases (`-128Y * -1Y`, `127Y * 127Y`
  and the short twins) at lengths 1 to 129 under four null patterns, every `TINYINT` pair, the
  `SMALLINT` slices - plus three checks: the four rows' kernels are driven at the int lane with no
  narrow `IntArith(MUL, FAIL|NULL)` in their trees, both arms emit identical class bytes for each,
  and a `TRY` product beside a null and beside a valid neighbour (the word rule: operands' AND with
  the guard's lanes cleared).
- New `IntArith` constructor cases in `VarkaLaneTypeSuite`, and `GuardedRange`'s `NULL` mode in the
  emitter's guard suites (`VarkaGuardEdgeSuite`): a guard under `NULL` in a masked body, its refusal
  of a dense body, its word owned, in an epilogue, and under an `IfElse` arm.
- `VarkaCoverageDifferentialSuite` over the narrow rows, both consumers, the partitions and ANSI's
  positions; the narrow, arithmetic, family and oracle suites; the option matrix on the branch.

### 3.2 `narrow_range.smt2`

Kept: the ten checks of the widened compare. Added, rendered from the code's constants:

1. **The product is exact at the int lane**: for `x`, `y` the sign extensions of two `w`-bit values
   into 32 bits, `bvmul x y` equals the 64-bit product of their 64-bit sign extensions, for `w` 8
   and 16 (`unsat` of the inequality); and refuted at `w = 17`, where `2^32` is reached, so the
   check is not vacuous.
2. **The `NULL` mode clears exactly the outside lanes**: over a 64-bit word and one lane's 32-bit
   result `r` at bit `i`, bit `i` of `word & ~mask` is bit `i` of `word` and not `outside(r)`, and
   every other bit is unchanged; `word | mask` and `word & mask` refuted.
3. **The guard composes**: `outside(bvmul x y)` holds at exactly the pairs whose exact product is
   not a `w`-bit value - the composition of check 1 with the existing mask claim, stated once.

The comments naming `holdsCheckedNarrowMultiply` (`VarkaProofFiles.java` test tree, lines 2474-2475
and 2491-2492) are rewritten. `dev/varka_prove.sh --solver both`; the solver times before and after
are written into section 10.

### 3.3 The exhaustive proof

- **`TINYINT`: every pair.** The 2^16 pairs in one batch per mode and arm, against Spark's own
  `eval` on every row: `ANSI` declines exactly the batches holding an outside row (each such batch
  is then held to the row engine's error), `TRY` matches row by row. Every build, as today.
- **`SMALLINT`: every pair, under row 330's budget.** The four 2^32 sweeps of `*` (`ANSI` and `TRY`,
  two arms) exist as opt-in tests; at the 1106 s floor of `results/VARKA-330-sweep.txt` they are
  about 74 minutes together, a floor (the multiply checks where the timed `+` did not). They run
  once on the branch before the pull request opens, on the laptop under one lock, timed into
  `results/VARKA-481-sweep.txt`; no new sweep is added, so the budget of `PLAN.md` 10.3 item 5 does
  not grow. The 2^24 slice of each runs on every build. Where the four are cut to two is question 1.
  Neither the nightly nor any CI workflow is known to run these sweeps today (section 7), which is
  why the branch run is named here rather than left to a schedule.

### 3.4 The generated files

`emitted_bytes.json` regenerated once, its diff limited to the four rows at each width and
`option_arms`, the rows read before committing; `coverage.json` and `docs/sql-varka.md` only if a
note changes (the `y1 * y2` note "widened to int for its check" stays true); `width_audit.json`
unchanged.

## 4. The benchmark

**The baseline first, on the unchanged code, its own pull request.** `VarkaNarrowEmitBenchmark`
gains the checked multiply, under both arms and at its existing ladder: (h) `y1 * y2` stored, in
`ANSI` and `TRY`; (i) `s1 * s2` stored, in both; (j) `y1 * y2 < y3` as a selection, `ANSI`, the
interior form of point 2 of 2.7; over data where no product overflows, so every arm answers. Run by
`benchmark.yml` on a GitHub-hosted EPYC 7763 (`expected-cpu` pinned, a CPU-check failure retried
hourly), at 256 and 128 bits, committed as `results/VARKA-481-baseline.txt` with its provenance.

**Then the pair**, `benchmark.yml` in pair mode on the change's pull request, base the merge base,
into `results/VARKA-481-pair/`, and an A/A run of the same class for the runner's spread.

Predictions, registered now:

1. (h) and (i) are within the A/A spread of the base: the guard parks one vector where three were
   parked, and the work is the same int multiply and two compares.
2. (j) is within the spread under point 2's (b), and slower by at most 15% in L1 under (a), the
   shift pair being two of about a dozen lanewise ops.
3. The two arms tie on every new shape at both widths, their class bytes being identical (2.6).
4. No other row of the class moves outside its spread.

## 5. Order, files and size

1. **The baseline** (4), its own pull request.
2. **The shared compare** (point 3 of 2.7), bytes-neutral, in the change's pull request as its
   first commit, the oracle unchanged at that commit.
3. **`GuardedRange`'s mode**, with its tests and the proof, still with no compiler change: nothing
   builds `NULL` yet, so the oracle is unchanged at that commit too.
4. **The tree** (2.1), the five deletions (2.4), the constructor refusal, the option's arm (point
   1), the regenerated oracle, the sweeps' run, the pair.

| file | what |
| :-- | :-- |
| `VarkaNodeCompiler.java` | `narrowArith`'s checked `MUL`; `arithOver`'s exception and javadoc |
| `varka/VarkaVectorIR.java` | `GuardedRange`'s mode, canonical forms, `IntArith`'s refusal |
| `varka/VarkaKernelPlan.java` | the walk deleted |
| `varka/Analysis.java`, `varka/Slots.java` | the refusal clause; the `NULL` guard's word |
| `varka/VarkaVectorWalk.java` | the guard's `NULL` arm, the throws, the shared compare |
| `varka/VarkaEmitCost.java`, `varka/VarkaEmitBudget.java` | the `NULL` feature; the javadoc |
| test models | `VarkaReference`, `VarkaGuardSpecs`, `VarkaCanonicalIr`, and the IR's censuses |
| tests | the narrow arithmetic suite, the guard suites, `VarkaLaneTypeSuite` |
| proofs | `VarkaProofFiles.java`, `sql/varka/proofs/narrow_range.smt2` |
| benchmark | `VarkaNarrowEmitBenchmark.java`, `results/VARKA-481-baseline.txt`, `-pair/` |
| generated | `emitted_bytes.json`; `sql/varka/matrix/skips.tsv` under point 1 |

**Size: medium, two pull requests.** The baseline about half a day with the runner's queue; the
change about two days, most of it the `NULL` mode's sites (2.3) and the oracle's review. Test time
added: three suite checks of milliseconds and three SMT checks of hundredths of a second; the
sweeps' run is a branch run, not a build step.

## 6. What it unlocks

- **Row 332**: `try_cast(bigint AS INT)` becomes `NarrowLane(GuardedRange(x, MIN, MAX, NULL))`, the
  decline at `VarkaNodeCompiler.java:707-710` replaced by one line; the narrow-to-narrow casts'
  `TRY` arm takes the same form.
- **`VARKA-104.md` 8 step 5**: the int32 checked multiply in both checked modes is this tree one
  lane up, `NarrowLane(GuardedRange(IntArith(MUL, WRAP, WidenLane(x), WidenLane(y)), INT_MIN,
  INT_MAX, mode))`, its exactness the same argument at `2^62`.
- **The census** moves nothing: the narrow `*` and `try_multiply` pairs are fused already.

## 7. Out of this row, and what was found beside it

Out: the narrow add, subtract and negation (they stay as they are); the int32 checked multiply
(104); `try_cast` (332); the weights and the refit (352); the untaken-arm cliff, which every guard
shares.

Found while reading, not fixed here: `dev/varka_nightly.sh` runs the opt-in sweeps as
`testOnly *VarkaChronoSuite *VarkaEmitter*Suite -- -z opt-in` (lines 169-172), while its own
comment (lines 161-162) says a JUnit suite matches no `-z` filter and runs nothing under it, and
`VarkaEmitterNarrowArithSuite` is a JUnit suite; no workflow under `.github/workflows` sets
`varka.sweep`. So the eighteen `SMALLINT` 2^32 sweeps of row 330 may run nowhere on a schedule.
Read, not run; it is for the lead to place.

## 8. Questions for the owner

1. **One arm per mode for the multiply's 2^32 sweeps.** If 3.1 confirms the two arms emit
   identical bytes for the checked multiply, the native and widened sweeps of `*` in `ANSI` and
   `TRY` run one class twice, about 37 of the 74 minutes. (a) Keep all four, as row 330's done-when
   reads. (b) Keep one arm per mode while the bytes-identity check holds, the check failing the
   build if the arms ever diverge. *Recommended: (b)*: the cut is `VARKA-329.md` 4.4's, which that
   plan leaves to the owner, and the identity check makes the dropped sweep's subject provably the
   kept one's.

## 9. Done when

Row 481's text holds, with section 0's corrections: an `ANSI` or `TRY` narrow multiply compiles to
2.1's tree on both arms; the five special cases are gone (2.4); `GuardedRange` has its `NULL` mode
with its sites (2.3); `narrow_range.smt2` proves the product's exactness and the `NULL` mode under
both solvers; every `TINYINT` pair and every `SMALLINT` pair of the kept sweeps agree with Spark,
timed into the results file; the bytes oracle moves for the four rows and the digests only; the
baseline and the pair are committed with their predictions scored; and the narrow suites, the
differential suite, the PR build and the option matrix pass.

## 10. Outcome
