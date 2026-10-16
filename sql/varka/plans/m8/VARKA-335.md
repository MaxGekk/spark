# VARKA-335: the bitwise and shift operators, `bit_count`, `bit_get` and the bitmap positions

*Row 335 of `m8/PLAN.md`, wave 0. Planned on 16 October 2026 at `8a50f61a368`, from `m9/SCOPE.md`
item 32 and `m8/PLAN.md` 2.9. Section 2 finds that only part of the row can land in wave 0, and
section 8 splits it accordingly.*

## 1. Where this came from

Item 32 scoped the bitwise family as "nothing in the way": `&`, `|`, `^`, `~` and the three shifts
are one lanewise operation each at the operand's width, and the emitter already emits `AND`,
`XOR` and the shifts inside other lowerings. None of the fifteen census entries the row owns
(`&`, `|`, `^`, `~`, `shiftleft`, `shiftright`, `shiftrightunsigned`, `<<`, `>>`, `>>>`,
`bit_count`, `bit_get`, `getbit`, `bitmap_bucket_number`, `bitmap_bit_position`) is fused today.
The integer reading added the semantics a kernel has to copy: `bit_count` is a 64-bit popcount of
the sign-extended value, the vector shifts mask their count as Java does, and the bitmap functions
reduce to blended `AND`s and a signed shift with `Long.MIN_VALUE` as the edge (2.9). The row's
done-when asks for every integral type each takes, by exhaustion at 32 bits and below.

## 2. The admission check, done

1. **What Spark computes** (`sql/catalyst/src/main/scala/.../expressions/`):
   `BitwiseAnd`, `BitwiseOr`, `BitwiseXor`, `BitwiseNot` (`bitwiseExpressions.scala` 46, 102,
   158, 210) at the operand's type; `BitwiseCount` (257 to 279), `Long.bitCount` of the widened
   value for `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT` and `BIGINT`, an `INT` result;
   `BitwiseGet` (316 to 345), `(byte) ((long) x >> pos & 1)` after a position check, a `TINYINT`
   result; `ShiftLeft`, `ShiftRight`, `ShiftRightUnsigned` (`mathExpressions.scala` 1507, 1543,
   1578, over `BitShiftOperation` 1443: value `INT` or `BIGINT`, count `INT`, the value's type as
   the result, the JVM's operators, which mask the count); `BitmapBucketNumber` and
   `BitmapBitPosition` (`bitmapExpressions.scala` 49, 87; `BitmapExpressionUtils.java` 30 to 43,
   `NUM_BITS = 32768`): bucket `v > 0 ? 1 + (v - 1) / 32768 : v / 32768`, position
   `v > 0 ? (v - 1) % 32768 : (-v) % 32768`, both `BIGINT` in and out.
2. **The IR has no bitwise node.** `IntOp` is `{ ADD, SUB, MUL }` (`VarkaVectorIR.java` 193) and
   `IntArith` carries overflow semantics the bitwise operators do not have. The emitter emits the
   operators only inside other lowerings: `emitShift` (`VarkaVectorWalk` 792 to 796, `LSHR` and
   `LSHL` by a constant, hard-wired to `IntVector`), `XOR` and `AND` in the overflow check (456 to
   461), `ASHR` in `VarkaDivisionLowering` 317, `LSHR` in `VarkaChronoLowering` 302 and 323. A
   shift by a vector count has never been emitted.
3. **Which parts need a later row.** Checked against the lanes as they are:
   * `TINYINT` and `SMALLINT` operands of `&`, `|`, `^`, `~` and `bit_count`: no such column is
     admitted until 329.
   * `bit_get`, `getbit`: a `TINYINT` result, written only once 329 admits the byte output.
   * `bit_count` over `BOOLEAN`: 331.
   * A `BIGINT` shifted by an `INT` column, and `bit_count(bigint)` under another node: mixed width,
     after 28. `bit_count(bigint)` as an output root needs nothing new - it is at most 64, and
     `NarrowLane` is admitted as an output root over values proven to fit (`VarkaVectorIR` 343),
     as the `TIME` extracts use it.
   * A `BIGINT` shifted by an `INT` literal: the count folds to a constant, single lane, wave 0.

   So wave 0 can fuse `&`, `|`, `^`, `~` over `INT` and `BIGINT`; the three shifts over `INT` by a
   literal or a column and over `BIGINT` by a literal; `bit_count` over `INT` and as a root over
   `BIGINT`; and both bitmap functions. That is 24 of the census's pairs and, by the census's
   types (the shift entries admit `INT` and `BIGINT` only, the bitmap ones `BIGINT` only), eight
   entries fused for every type they admit.
4. **`bit_count` on the int lane**: `pop32(x) + ((x >> 31) & 32)` is the 64-bit popcount of the
   sign-extended int (the high 32 bits are all ones exactly when `x < 0`); exhaustion over the
   2^32 ints checks it against `BitwiseCount`. The Vector API's `BIT_COUNT` lowers to `vpopcntd`
   only with AVX512_VPOPCNTDQ and to the JDK's nibble-table sequence on AVX2 (the integer reading,
   about 1.75 instructions per int row), so the row claims no speed-up for the count alone on
   AVX2 (2.9); 376's census records which host has which.
5. **The bitmap functions' lowering**: for `v > 0`, `v - 1` is non-negative, so the quotient is
   `(v - 1) >> 15` and the remainder `(v - 1) & 32767`; for `v <= 0`, the bucket is the truncating
   quotient `v / 32768`, which is `(v + ((v >> 63) >>> 49)) >> 15` (the sign trick
   `ConstDivide` applies), and the position `(-v) & 32767`, which equals `(-v) % 32768` for every
   `v <= 0` including `Long.MIN_VALUE`, where `-v` wraps to itself and both sides are 0. Two arms
   blended on `v > 0`. A full-range claim over a 64-bit value, so it goes to SMT
   (`dev/varka_prove.sh`), not to exhaustion, beside the named cases `Long.MIN_VALUE`, `-1`, `0`,
   `1`, `32768`, `32769`.

**What still needs a run.** Nothing gates the design: the operators are Java's own and the two
bitmap identities are checked by SMT as part of the work. One check is cheap and settles the
shift's emitted form before code: with a catalyst test build, `dev/varka_emit.sh "i << 3"
--columns i:int` today declines ("unsupported expression"), which confirms the arm is missing and
not merely undocumented; if it compiles, the row is coverage rows only for that shape.

## 3. The design

### 3.1 Two IR nodes and a popcount

* `BitOp(BitOpKind op, VarkaVectorIR left, VarkaVectorIR right)`, `op` one of `AND`, `OR`, `XOR`,
  `SHL`, `SHR`, `USHR`, both children on the node's lane (the shift count of a `BIGINT` is a long
  literal at wave 0), emitted as one `lanewise` - by a vector for a column count, by a scalar for
  a literal - through the lane's descriptors (`Lane.lanewiseVV`, and a lanewise-by-scalar shape
  for the literal count, generalizing `emitShift` beyond `IntVector`).
* `BitNot(VarkaVectorIR child)`, one `lanewise(NOT)`.
* `BitCount(VarkaVectorIR child)`: on the int lane `pop32(x) + ((x >> 31) & 32)`, on the long
  lane `pop64(x)` under a `NarrowLane` root.

A separate node rather than new `IntOp` members keeps `IntArith`'s overflow reasoning and the range
analysis's transfer function for it unchanged. `VarkaRangeAnalysis` answers `BitOp` and `BitNot`
as `UNKNOWN` except where the bound is free (`x & k` with a non-negative literal `k` is within
`[0, k]`, which lets a following checked add drop its check); `BitCount` is `[0, 64]`.

### 3.2 The bitmap functions

Compiled to the existing nodes plus `BitOp`, by section 2 item 5's two arms and a blend on
`v > 0`; no node of their own. Their SMT goals join the long-lane proof file's list as two claims,
stated over integers as `sql/varka/proofs/README.md` prescribes (the shift as floor division by
`2^15`, the mask as the non-negative remainder), with the file's time before and after recorded
(milestone risk 5).

### 3.3 What is deliberately unchanged

* `IntArith`, `IntNeg` and their overflow modes; `ConstDivide` is reused only as a pattern, not
  changed.
* `emitShift`'s existing callers keep their bytes; the generalization adds a path.
* Every part section 2 item 3 assigns to a later row.

### 3.4 Registered op counts

Expected, to be read with `dev/varka_emit.sh --table` once built and asserted by op-count tests:
`i & j`, `i | j`, `i ^ j`, `~i`, `i << 3`, `i << j` one `IntVector` invocation each, and the same
at the long lane; `bit_count(i)` four (the count, the shift, the `AND`, the add); `bit_count(l)`
one plus the narrowing store; `bitmap_bit_position(l)` about eight lane operations
(two arms of a subtract or negate and a mask, a compare, a blend), `bitmap_bucket_number(l)` about
ten. A count that differs is recorded with the tool's output, not reconciled by hand.

## 4. Files

* `.../codegen/varka/VarkaVectorIR.java`: `BitOp`, `BitNot`, `BitCount`
* `.../codegen/varka/Analysis.java`, `Slots.java`, `VarkaVectorWalk.java`: their analysis and
  emission; `emitShift` generalized
* `.../codegen/varka/Lane.java`: the lanewise-by-scalar shape for a literal count at either lane
* `.../codegen/varka/VarkaRangeAnalysis.java`: the transfer functions of 3.1
* `.../codegen/varka/VarkaEmitCostTable.java`: the new nodes' costs
* `.../codegen/VarkaNodeCompiler.java`: the compiler arms for the bitwise, shift, count and bitmap
  expressions
* test: `VarkaReference.java`, `VarkaIrGrammar.java`, `VarkaCanonicalIr.java`: the oracle, the
  fuzzer's productions, the canonical form
* test: a new `VarkaBitwiseSweepSuite.java` (`@VarkaSuite`): the exhaustion of section 5
* `sql/varka/proofs/` long-lane proof file, `VarkaProofFiles`: the two bitmap claims
* test: `VarkaCoverageSuite.scala`; `coverage.json`, `docs/sql-varka.md`, `emitted_bytes.json`:
  rows, regenerated; new bytes rows only
* `sql/varka/plans/m8/census_curated.txt`, `census.tsv`: the fused types

## 5. Tests, and what each is for

1. **Exhaustion over every `INT`** for `~i`, `bit_count(i)` and each shift by every count in
   `[-64, 64]` (the masking), and over every pair in a sampled 2^32 slice for `&`, `|`, `^` against
   Spark's own expressions; the full 2^32 of the unary ones runs nightly, a fixed slice in PR CI.
2. **The bitmap functions by SMT** over the whole long, and their named cases as coverage rows.
3. **Coverage rows** for every fused pair at both widths, with the poisoned null lanes; the row
   engine is the oracle.
4. **A long shift by an int column declines** with a reason naming row 28; catches a mixed-width
   tree admitted before its lane conversion exists.
5. **`bit_count(l)` under another node declines** for the same reason.
6. **The IR fuzzer draws the new nodes** under the option matrix.

No existing fixture moves; the bytes oracle gains rows, and `emitShift`'s callers keep theirs.

## 6. The measurement

The row claims no speed-up for `bit_count` on AVX2 (2.9). The operators are one instruction each
on every host the milestone measures (376 checks); a throughput number would restate the int and
long lanes' existing ones. So no benchmark section is added; the sweep's time is the cost measured.

### 6.1 Predictions, registered before the run

1. **The census moves by 24 pairs and 8 fully fused entries** in wave 0 (section 2 item 3).
2. **The int-lane `bit_count` identity holds over all 2^32 ints** against `BitwiseCount`.
3. **Each unary 2^32 sweep takes under five minutes** on the laptop, against Spark's own
   expression per value: estimated at about one minute (2^32 interpreted evaluations at a few
   nanoseconds each, spread over the cores, as VARKA-281's sweeps were) and stated at five, the
   calibration table's ratio for a check whose cost is the harness's reads rather than the
   kernel's work (VARKA-274a's tier witness). The row has three such sweeps (`~`, `bit_count`, the
   shifts over their counts); whichever of 330's and this row's lands first times the
   milestone's first 2^32 sweep and starts the budget total of 10.3 item 5.
4. **The two bitmap SMT claims add under 5 s** to the long-lane proof file under Z3 on the laptop,
   being linear over integers with one case split.

## 7. Risks

1. **The shift count's masking.** The Vector API masks the count as Java does; Spark uses Java's
   operators, so they agree, and test 1's counts outside `[0, 63]` hold it.
2. **The range analysis's `AND` rule.** A wrong bound would take a check off a later add; the
   rule is stated as an SMT claim in the range-analysis proof file beside the others.
3. **The partial delivery reads as done.** The row's done-when spans `TINYINT` and `SMALLINT`;
   the row stays open after wave 0, its remaining parts listed in section 8, and the census shows
   which pairs remain.

## 8. Sequencing

1. This plan.
2. **335a, wave 0**: `BitOp`, `BitNot`, `BitCount` over `INT` and `BIGINT` as section 2 item 3
   lists, the bitmap functions with their proofs, the sweeps and the coverage rows.
3. **335b, after 28, 329 and 331**: the narrow operands, `bit_get` and `getbit` with 329's byte
   output, a `BIGINT` shifted by an `INT` column and `bit_count(bigint)` under another node with
   28, `bit_count(bool)` with 331. Each lands as a pull request of its own when its lane exists.

Explicitly out of this row: any speed-up claim for the popcount on AVX2, the `TINYINT` output
itself (329), and the lane conversion (28).

## 9. Outcome

*To be written when the work lands.*
