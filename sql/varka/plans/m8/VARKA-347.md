# VARKA-347: The decimal load path

## 0. What exists now (admission check)

From the code at `71ef57af282` (27 October 2026; Varka paths under
`sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen/` unless named). The
row's text is the milestone plan's 2.8 ("The load path below 19 is a measurement") and items 1 and
89 bullet 6 of `m9/SCOPE.md`. What the code says about each premise is below, with the file and
line it rests on.

1. **Arrow holds every decimal in sixteen bytes, low word first.** `ArrowUtils.toArrowType`
   (`sql/api/.../util/ArrowUtils.scala:108`) maps every `DecimalType` to `Decimal(p, s, 128)`.
   Arrow Java writes the slot in native byte order: the low word in the first eight bytes and its
   sign extension in the next eight on a little-endian host (`ArrowCachedBatchSerializer.scala:444`,
   `compactDecimalUnscaledOffset`). The kernels load every column `LITTLE_ENDIAN` today, so a
   big-endian host is outside this row as it is outside every lane.
2. **Arrow Java does not check the bit width.** In arrow-vector 19.0.0 (the version `pom.xml`
   pins), `Types.java:950-954` maps every `Decimal` whose bit width is not 256 to `DecimalVector`,
   and `DecimalVector(Field, ...)` (`DecimalVector.java:81-82`) takes sixteen bytes a row whatever
   the field says. A `Decimal(p, s, 64)` field from another writer is read at the wrong stride.
   `ArrowUtils.fromArrowType` (`:144`) also ignores the width. So the row's `bitWidth == 128`
   check is needed, and it has to be Varka's own.
3. **The column path builds a `BigDecimal`-backed value and raises on an out-of-precision slot.**
   `ArrowColumnVector` (`sql/catalyst/.../vectorized/ArrowColumnVector.java:207-213`) picks
   `SmallDecimalAccessor` for a vector of precision 1 to 18. Its `getDecimal` (`:469-483`) checks
   the high word against the low word's sign, then calls `Decimal.apply(BigDecimal.valueOf(u,
   arrowScale), p, s)`, or `getObject` when the high word differs. `Decimal.set`
   (`sql/api/.../types/Decimal.scala:119-131`) raises `NUMERIC_VALUE_OUT_OF_RANGE` when the value
   has more digits than `p`. So a valid slot holding `10^p`, or any wrong high word (whose value is
   then at least `2^63`), raises on read in every mode. It also rescales from the Arrow field's
   scale to Spark's by `ROUND_HALF_UP`, so a field whose scale is not the Spark type's is read
   rounded. A null slot is never read (`isNullAt` first), so garbage under a null is harmless.
4. **The cache can hold such slots.** Spark's own writer never writes one: `DecimalWriter`
   (`sql/catalyst/.../arrow/ArrowWriter.scala:308-315`) writes a null when `changePrecision`
   fails. But a columnar input whose fields match the cache schema is cached zero-copy
   (`ColumnarBatchToArrowCachedBatchIterator.scala:92-100`), and `isCompatibleWithDeclaredField`
   (`ArrowUtils.scala:581-620`) compares types, not values. So a cached batch carries whatever an
   Arrow source wrote, under nulls too.
5. **The cache's row reader truncates instead** (`ArrowColumnReader.scala:124-148`): it writes the
   low word into the `UnsafeRow` with no check. This is `VARKA-365.md` 2 item 2, reported upstream
   under option (iii) and not changed here. Varka reads the columnar path, whose fallback is
   `ArrowColumnVector`, so the answer the kernel must match is item 3's.
6. **The cache format has no version and no schema.** `ArrowCachedBatch`
   (`ArrowCachedBatch.scala:49`) is the row count, one record batch message and the statistics;
   the schema is rebuilt on every read from the Spark types by `toArrowSchema(...,
   losslessInternalTypes = true)` (`ArrowCachedBatchToColumnarBatchIterator.scala:54`). The
   format already tags a field whose Spark type its Arrow type cannot carry, in the field's
   metadata (`ArrowUtils.scala:165-245`, the time precision and the nanosecond structs).
7. **The registry owes the type, with one lane per type.** `varka/VarkaTypes.java:281-283` gives
   `DECIMAL` the admission `Owed("347")`, storage 128 bits, no lanes, and no conformance suite.
   An admission is `OnLane(LaneType)` (`:65`) per type id, and `VarkaTypes.lane(DataType)`
   (`:224`), which the compiler and the evaluator ask, answers from the id alone. A decimal's lane
   depends on its precision: the long lane up to 18 (this row), the int lane up to 9 (this row's
   third arm), the lane pair above 18 (row 359).
8. **The evaluator admits a column by its vector class.** `VarkaLaneArrow.of`
   (`sql/core/.../execution/VarkaLaneArrow.java:104`) maps `DECIMAL` to null, and `reads`
   (`:132-139`) matches the class only. `isArrowBacked` (`VarkaEvaluatorBase.java:918-937`) checks
   the class and the row count; nothing reads the field. `allocate` (`:145-154`) uses the same
   table for outputs, and a decimal output is row 157's store.
9. **A kernel gets addresses; the stride is the emitter's.** `VarkaKernelRunner.fill`
   (`sql/core/.../execution/varka/VarkaKernelRunner.java:213-235`) passes each input's data
   address. The emitted body maps it as `length * stride` bytes, the stride being a lane's
   (`varka/VarkaBodyEmitter.java:1536-1546`, `Lane.java:136`, `byteStride = bits / 8`). There
   are two other branches: a boolean column (`boolInputs`), and an input whose stored lane is not
   the driving one, which `loadNarrowedSegment` (`:1518-1530`) maps at its own lane's stride. So a
   sixteen-byte branch must be tested before the narrowed one, or a decimal read on the int lane
   in a long-driven kernel is mapped at four bytes a row. `Analysis.registerInput`
   (`varka/Analysis.java:1205-1214`) records one lane per column. No input today is sixteen bytes a
   row.
10. **A column leaf is an ordinal and a lane.** `ColumnRef` (`varka/VarkaVectorIR.java:415`)
    renders in the shape key as `col:` plus the ordinal and the lane (`:1465`). A decimal read on
    the long lane as a `ColumnRef` would share a kernel class with a `BIGINT` column of the same
    tree: the shape key must tell them apart. `GuardedRange` (`:504`) declines a batch with a live
    lane outside a range, which is the precision check's shape; its stored-values form reads null
    rows too, so the check here must be the live-lane form.
11. **The census reading is thinner than the row's three operators.** `VARKA-376.md` 9 has
    `rearrange2`, `selectFrom` and `compress` on longs intrinsic and allocation-free at 512 and 256
    bits, and the Java fallback at 128 bits on x86 (68 to 220 bytes a row on the EPYC 7763,
    `results/VARKA-376-census-epyc-7763.json`). But the probes
    (`VarkaOperatorCensusProbe.java:521-538`, catalyst tests) are thinner than the design for two
    of them. `rearrange2` is the design's own form, `fromArray(a).rearrange(DEINTERLEAVE,
    fromArray(b))`, which JDK 25 implements as two one-vector rearranges and a blend
    (`LongVector.java:2333-2353` in `/home/max/proj/openjdk-build/jdk25`); it differs only in
    reading arrays, not segments. `selectFrom` is the one-vector form, and the two-vector
    `selectFrom(v1, v2)` the scope names is a different intrinsic (`selectFromTwoVectorOp`), native
    on x86 only with AVX-512, VL below 512 bits (`x86.ad:1833-1836`), and lowered to rearranges and
    a blend elsewhere (`vectorIntrinsics.cpp:2724-2739`); `compress` takes a data-dependent mask,
    not the constant even-lane one, and has no splice. All three read arrays, not segments.
12. **`compress` needs AVX2 with BMI2 and four long lanes on x86, and SVE on Arm, not "not
    NEON".** `x86.ad:1349-1355` refuses `CompressV` below AVX2, and its case in
    `x86.ad:1868-1875` falls through to `Op_VectorLongToMask` (`:1876-1882`), which refuses when
    `UseAVX < 3 && !supports_bmi2()`. At 128 bits the AVX2 EPYC reads `long.compress` as `F` (the
    two-lane mask refusal of m7 row 374). `aarch64_vector.ad:176-180` refuses it only when
    `UseSVE == 0`. The Neoverse N2 runner has SVE, and its census cells read `I` for all three at
    128 bits (`results/VARKA-376-census-neoverse-n2.json`). A host with NEON and no SVE is not in
    the census.
13. **The long lane's measurements rest on row 383.** A decimal kernel is a long-lane kernel, and
    row 383 found long-lane kernels released from tier 4 by profiled loop predication. Every arm
    here is timed under row 407's rule, each arm driven to level-4 code first, and its tier state
    recorded beside its number.
14. **The baseline is not yet merged.** Row 377's decimal class, `VarkaDecimalBaselineBenchmark`,
    is on branch `varka-377-decimal`, not on master and not yet a pull request. Its four sections
    (a comparison, an arithmetic chain, a cast and a choice) are rows 348 to 350's; none of them
    fuses by a load alone.

What depends on #1049 (the lane split, open): only the line numbers in `Lane.java` and
`VarkaVectorIR.java`, and `LaneType.bits()`, whose comment says a value "is this wide in a vector
and in its column". A decimal on the long lane is not; that sentence gains "except a decimal
column" here. The design needs no new `LaneType`.

### What differs from the row text and the milestone plan

- **The load needs the precision check, not only the high-word check** (item 3). A slot holding
  `10^p` has a correct high word and still raises on the column path, so the load declines its
  batch. Row 348's done-when names "the load's precision check"; this row builds it, and 348's
  clause is then met.
- **The scale is checked with the bit width** (items 2, 3): a field at another scale is read
  rounded by Spark and wrong by a kernel.
- **"No `compress` on NEON" is "no `compress` without SVE", and on x86 it needs BMI2 and four
  long lanes** (item 12), and the census does not
  cover the de-interleave as the row builds it (item 11), so step 2 adds those cells first.
- **The cache arm has no version tag to add** (item 6): the format would carry the layout as a
  tagged field, as it carries the nanosecond structs. And it is a reader change for every consumer
  of the cache (2.6), the kind of change `VARKA-365.md` 9.1 turned down. Question 1.
- **A load alone has no coverage row.** Every SQL reader of a decimal column - a comparison, a
  null test, a cast, a choice - is rows 348 to 350's, and a decimal output is 157's. Question 2.
- **The registry gains a precision-dependent lane** (item 7). Question 3.

## 1. Goal and non-goal

A `DECIMAL(p, s)` column with `p <= 18` and `s >= 0` is read by a kernel from Arrow's sixteen-byte
slots as unscaled values on the long lane, with every batch that Spark's column path would raise
on declined to that path. The load is built in the forms the row names, measured with load and
compute apart at each width, and the losing forms kept behind options. Not this row: anything
computed on the values (348 to 351), the store (157), precision above 18 (359), a negative scale.

## 2. Design

### 2.1 The leaf

A new value leaf, `DecimalColumn(int ordinal, LaneType lane, int precision)`, on the long lane (or
the int lane, 2.3). It renders as `dec:` plus the ordinal, the lane and the precision, so it shares
no kernel with a `BIGINT` column (item 0.10), and two precisions take two classes, since the bound
is part of the code. The scale is not in the leaf: the values are unscaled, and the nodes above
carry it.

The leaf loads the low words and arms one check over the live lanes: the batch declines if any
valid slot has `hi != lo >> 63`, or `lo` outside `[-(10^p - 1), 10^p - 1]`. One leaf owns both
checks because they are one fact - the slot holds a value of `DECIMAL(p, s)` - and a leaf without
its bound would compute where Spark raises. The check is unconditional, as `GuardedRange`'s is,
under a new decline status, "a decimal slot outside its precision": a new `VarkaFusedKernel`
status bit beside `STATUS_VALUE_RANGE` (`VarkaFusedKernel.java:68-87`; the runner's bits from 8
up are never returned by a kernel), named in `VarkaFallbackAccounting` (`:100-108`). It is not
`STATUS_VALUE_RANGE` itself, so the accounting tells a bad slot from a range guard's decline. In
the masked body the check is under the validity word (garbage under a null never declines); in
the dense body every lane is valid. The mismatch is accumulated in a mask across the
loop and tested once a body, not per group.

`Analysis` records the input as stored at sixteen bytes a row, in a `decimalInputs` bitmask beside
`boolInputs`, and `loadInputSegment` maps it as `length * 16` bytes.

### 2.2 The de-interleave and the check

For each output vector of `n` longs the leaf loads two vectors, `a` and `b`, over `n` slots. The
shuffles and masks are built once in the kernel's prologue from `SPECIES_PREFERRED`; no second
long species is used for a half-width load, which is the box `skills/vector-api-and-width.md`
names for two species of one lane type.

- **`rearrange`** (the default candidate): `lo = a.rearrange(EVEN, b)`, and for the check
  `hi = a.rearrange(ODD, b)`, then `hi != lo >> 63`. The censused operator (item 0.11).
- **`compress`**, an arm: `lo = a.compress(E).or(b.compress(E).unslice(n / 2))` with `E` the
  constant even-lane mask; for the check, the same with the odd mask. Its op count is not known
  until step 2's cell; expected to lose to `rearrange`, which JDK 25 builds as two
  one-vector permutes and a blend (item 0.11).
- **The two-vector `selectFrom`** is built only if step 2's cell reads `I` on the AVX2 runner; it
  has a native form only under AVX-512, and it is not in the census.
- **The shuffle-free check**, an arm over either form: a second load of the same rows at byte
  offset 8 gives `(hi0, lo1, hi1, ...)`, aligned with `a`'s `(lo0, hi0, lo1, ...)`, so
  `a8.compare(NE, a.lanewise(ASHR, 63))` under the even mask checks every high word with no
  cross-lane op. One output vector covers two input vectors, so it takes two offset loads (`a8`,
  `b8`) and two compares in place of one shuffle. Its compares run over the interleaved positions,
  two longs a row, so in the masked body each row's validity bit is first spread to its even lane,
  once for `a` and once for `b`: one more mask build per input vector. Its last lane reads eight
  bytes past the group, so the last full group of a batch and the epilogue take it masked.
  Unverified until measured; its loads and mask spreads may cost more than the shuffle it
  removes.

The epilogue loads `2 * remaining` longs under `indexInRange` masks. The options are a
`Choice` `decimalLoad` (`REARRANGE`, `COMPRESS`, and `SELECT_FROM` if built) and a `Flag`
`decimalCheckShuffleFree`, both `Reason.MACHINE`, since which form wins depends on the host.

### 2.3 The int lane for precision up to 9

Under a `Flag` `decimalIntLane` (`Reason.MACHINE`), a `DECIMAL(p, s)` with `p <= 9` is read on the
int lane: twice the lanes per register for every node above it. From Arrow it is a four-to-one
selection (four bytes of value in sixteen), built as two long de-interleaves and checks, then
`L2I` of both halves spliced into one int vector; the precision check implies the value fits an
int. It is expected to lose on the load and is judged on density, by the compute it carries
(4.3). It applies where the kernel plan drives at the int lane; a tree with a long value drives at
the long lane, and the int arm's columns then load in the low half as int columns do (`VARKA-28.md`
9). As with `narrowLanes`, the compute lane is the kernel plan's choice, not the registry's.

### 2.4 The registry and the Arrow admission

`VarkaTypes` gains an admission for a type whose lane depends on its precision,
`UpToPrecision(LaneType lane, int maxPrecision, String widerRow)`, which `lane(DataType)` and
`declineReason(DataType)` read: the long lane for `p <= 18`; for a wider decimal the reason "a
decimal(30,4) column: precision above 18 is row 359's"; for a negative scale the recorded decision
of the milestone plan's 2.8. Its lanes are `LONG` and `INT`, for the warm-up. Its conformance is
`Owed("Decimal", "348")`: the contract's ordering, choice and cast checks have no form until 348 to
350. `types.tsv` is regenerated.

`VarkaLaneArrow.of` maps `DECIMAL` to `DecimalVector` for reads. `allocate`
(`VarkaLaneArrow.java:145-154`) calls the matching entry's allocator, so the entry's allocator is
null and `allocate` returns null for it, as it does for a type with no entry; the compiler
declines a decimal output root before that, naming row 157. `isArrowBacked` checks a referenced
decimal column's field once a batch: `ArrowType.Decimal` with bit width 128 and the Spark type's
scale. The compiler hands the evaluator each decimal input's scale. It is a field read and allocates
nothing.

### 2.5 The first reader

If question 2 is answered as recommended, this row carries row 348's equal-scale forms over the
new leaf: `=`, `<>`, `<`, `<=`, `>`, `>=` and `<=>` of two decimal columns of one scale, or of a
column and a literal of its scale (the literal's unscaled long in the long table, read once in
the prologue), and `IS [NOT] NULL`. They are the existing `Compare`, `NullSafeEqual` and
`IsNotNull` at the long lane; no new node. Unequal scales, `IN`, the choice, negation and `abs`
stay 348's.

### 2.6 The cache layout, if built

Question 1 decides whether it is built in this row. What it would take:

- a SQL configuration, off by default, with its binding policy and the configuration audit suite
  run;
- under it, `toArrowField` with `losslessInternalTypes` writes a `DECIMAL(p, s)` with `p <= 18`
  as an `Int64` field tagged in its metadata with the precision and scale (`Int32` for `p <= 9`),
  so the schema rebuilt on read carries the layout and needs no version;
- a writer for the tagged field, `changePrecision` first and a null when it fails, as today;
- an `ArrowColumnVector` accessor for the tagged field that builds the same `BigDecimal`-backed
  value `SmallDecimalAccessor` does. This is a constraint, not a choice: a long-backed value would
  move the fallback to the exact rule and undo `VARKA-365.md` 9.1, on which row 350's design rests;
- `ArrowColumnReader`, `calculateMinMaxDecimal` (`ArrowCachedBatchSerializer.scala:905`) and the
  zero-copy check taught the tagged field (a `Decimal(p, s, 128)` input then takes the row copy);
- in the kernel, a plain long load with no check, since the writer bounded the values; the plan
  reads the layout from the cached relation when it plans, and a batch of the other layout fails
  `isArrowBacked` and takes the fallback.

The kernel side of this arm costs what a `BIGINT` column costs, which can be measured today
without building it (4.1).

### 2.7 Warm-up, cost, census

The warm-up's long block gains the shuffle, `compress`, `unslice` and offset-load calls of 2.2, and
its int block the `L2I` splice, held by `VarkaWarmupReachSuite`. `DecimalColumn` gets a cost feature
(and joins row 352's awaiting-refit list). The census marks the decimal pairs of 2.5 fused at
precision up to 18 only; a pair counts fused when it fuses at every precision (the milestone plan's
2.8), so the count moves with row 359.

## 3. Tests and oracles

1. **The slot table** (`VarkaDecimalLoadSuite`, catalyst), with batches written straight into
   sixteen-byte slots, at every arm of 2.2 and 2.3, in both bodies, in the first group, a middle
   group and the epilogue: garbage high words (random and all ones) under null slots, which must
   not decline; `10^p - 1` and `-(10^p - 1)`, which pass; `10^p` and `-10^p` with correct high
   words, and wrong high words (zero under a negative low word, minus one under a positive one, and
   one), each of which declines with the new status. Precision 1, 9, 10 and 18.
2. **End to end** (sql/core): the declined batch takes the row path, which raises
   `NUMERIC_VALUE_OUT_OF_RANGE` as a Varka-off columnar read does. The Varka-off reference must
   read through `ArrowColumnVector`: a cached table with Varka off reads through
   `ArrowColumnReader`, which truncates (`VARKA-365.md` 2 item 3), unless a column needs the
   fallback, so the fixture carries such a column or the reference reads the vector directly. A
   `Decimal(p, s, 64)` field, a scale mismatch and a `Decimal256Vector` are refused by
   `isArrowBacked` and answered right.
3. **The arms agree**: the differential over every option of 2.2 and 2.3 on the same batches, and
   the option matrix dispatched on the branch.
4. **Coverage rows** for the forms of 2.5 at precision 9 and 18, with columns `d9`, `d9b`, `d18`,
   `d18b` in the fixtures; the flipped tests (`VarkaTypesSuite`'s decimal decline now for `p > 18`
   only).
5. **The fuzzers**: a decimal sequence in `VarkaIrGrammar` with a seed of its own, so no existing
   hash moves, drawing in-precision values, the bounds, and garbage under nulls.
6. **The generated files** in `ADDING_A_TYPE.md`'s order; only new shapes and rows move. The
   128-bit x86 shapes are recorded refusals in `width_audit.json` (item 0.11).

## 4. Predictions and costs

The measurement is a catalyst class of its own, `VarkaDecimalLoadBenchmark`, on
`VarkaBenchmarkBase`, kernels emitted from the IR as `VarkaLaneConversionBenchmark`'s are, over a
ladder of row counts (in L2, in L3, past L3), results committed from `benchmark.yml` on the EPYC
7763 at 256 bits and at 128 (`MaxVectorSize=16`), and at 512 bits on a full-width runner when the
pool gives one, the datapath probe in the file. The baseline is row 377's decimal class, merged
first; the load class diffs against it only for the forms of 2.5.

1. **Load and compute apart.** Three kernels per arm over the same rows: load only (the leaf into
   a `BIGINT` store, the cheapest consumer); load plus a chain of `k` long adds (`k` = 1, 4, 16;
   IR only, as no SQL form exists before 349); and the same chain over a `BIGINT` column, which is
   compute with an eight-byte load. A pre-pass that de-interleaves into scratch as an int64
   column, timed alone, is the load with no compute at all. Load cost is the first minus the
   `BIGINT` load-only kernel; compute cost the chain's slope in `k`.
2. **The prediction per arm.** In L2, `rearrange` with its check costs 2 to 3 times the `BIGINT`
   load-only kernel per row, and the shuffle-free check saves part of that; past L3 every form is
   bound by reading sixteen bytes a row and costs about twice the `BIGINT` kernel, the forms tied.
   `compress` loses to `rearrange` at 256 and 512 bits. So the cache layout's kernel side, which is
   the `BIGINT` kernel, wins the load by up to 2 times past L3, and the margin falls as `k` grows.
3. **The int arm** loses the load at every rung and wins only in cache, at a chain long enough for
   its doubled lanes to pay for the narrowing: predicted from `k` = 4 to 16. Past L3 it loses,
   since it reads sixteen bytes for four.
4. **At 128 bits on x86** every form is the fallback loop and allocates (item 0.11), since x86
   lowers no two-lane mask (m7 row 374). The reading compares it with row 377's 128-bit file; if
   every arm loses to Spark's row path there, question 4 applies.
5. **No existing shape's bytes move**; the `option_arms` digests move with the new options.

The verdict per width names the default form, keeps the others behind their options, and records
the JDK limits of items 0.11 and 0.12 beside it.

Cost: large, as the row says. About six days after the baseline, one more if the cache arm is
built.

## 5. Order and files

1. **Row 377's decimal class**, its own pull request on the unchanged code.
2. **Census cells** in `VarkaOperatorCensusCatalog` for the de-interleave as built: the
   two-vector `selectFrom`, the constant-mask `compress` with its `unslice` splice, the offset-load
   check, all from segment loads; the laptop's column, then a `varka-width-audit.yml` dispatch.
3. **The leaf, the check, the registry and the Arrow admission** (2.1, 2.2's default, 2.4), with
   the slot table: bytes unchanged for every existing shape.
4. **The arms** (the rest of 2.2, and 2.3) and the differential.
5. **The first reader and the coverage rows** (2.5), if question 2 says so.
6. **The measurement** (4), committed per width, and the verdict with the JDK's limits.
7. **The cache layout** (2.6), only as question 1 decides.

| file | what |
|---|---|
| `varka/VarkaVectorIR.java` | `DecimalColumn` and its canonical form |
| `varka/Analysis.java`, `varka/VarkaKernelPlan.java` | the sixteen-byte input, the int arm's lane |
| `varka/VarkaVectorWalk.java`, `varka/VarkaBodyEmitter.java` | the de-interleave, check, segment |
| `varka/VarkaEmitOption.java`, `varka/VarkaEmitOptions.java` | the three options |
| `varka/VarkaTypes.java` | `UpToPrecision`, the entry, the reasons |
| `VarkaNodeCompiler.java`, `VarkaConditionCompiler.java` | the leaf; 2.5's forms |
| `VarkaLaneArrow.java`, `VarkaEvaluatorBase.java` (sql/core) | the vector, the field check |
| `VarkaFusedKernel.java`, `VarkaFallbackAccounting.java` | the decline status and its name |
| tests | the slot table, end to end, differential, coverage rows, fuzz sequence, census cells |
| benchmark | `VarkaDecimalLoadBenchmark` and its results files |
| generated | `coverage.json`, `docs/sql-varka.md`, `width_audit.json`, `emitted_bytes.json` |
| generated | `types.tsv`, `census.tsv` |

## 6. Out of this row

The decimal operators other than 2.5's (348), arithmetic and `UnscaledValue` (349), casts (350),
the functions (351), the store (157), precision above 18 and its pair (359), the exact
decimal-to-floating arm (366), a host with NEON and no SVE (not in the census), and a big-endian
host.

## 7. Declines that stay

A decimal output root (157), a decimal of precision above 18 (359), a negative scale (the milestone
plan's 2.8), an Arrow field of another bit width or scale (item 0.2), and every decimal operator 2.5
does not carry, each naming its row.

## 8. Questions for the owner

1. **The cache layout.** (a) Build it in this row, off by default, as the row text says. (b)
   Measure first: the `BIGINT` kernel is its kernel side (4.1), so build it only if that beats the
   best de-interleave by a margin the owner names (2 times past L3 is the prediction), as a row of
   its own. (c) Drop it. *Recommended: (b).* It changes the cache's readers for every consumer,
   with Varka off as well (2.6), which is what `VARKA-365.md` 9.1 weighed against, and the
   measurement can say first whether it pays.
2. **The first reader.** (a) This row carries row 348's equal-scale comparisons and null tests
   (2.5) for its coverage rows. (b) This row lands the leaf with IR-level tests only, and the
   coverage rows of its done-when move to 348. *Recommended: (a)*: section 4.1 of the milestone
   plan already lands 348 beside 347, and the comparison is row 377's first section, so the
   baseline diff starts here.
3. **The registry shape.** (a) A precision-reading admission, `UpToPrecision` (2.4). (b) Keep
   `OnLane(LONG)` and test the precision inside `lane(DataType)`. *Recommended: (a)*: the fact is
   the type's, (b) hides it in a function and `types.tsv` would not show it.
4. **Two long lanes.** If every form loses to Spark's row path at 128 bits on x86: (a) decline a
   decimal kernel at two long lanes, a recorded refusal; (b) fuse anyway, as the long compares
   there do. *Recommended: (a)*, decided by the measurement of 4.4.

## 9. Done when

Row 347's text holds, with the precision check in the load: the forms of 2.2 and 2.3 read a decimal
column (with coverage rows under question 2's answer, and the cache layout as question 1 decides);
the slot table passes with garbage high words under nulls, slots holding `10^p` and wrong high
words; `bitWidth == 128` and the scale are checked on the Arrow field; the measurement of 4 is
committed per width with its verdict and the JDK's limits; the losing arms stay behind their
options; the bytes oracle is unchanged for every existing shape; and the PR build and the option
matrix pass.

## 10. Outcome

(Written when the work lands.)
