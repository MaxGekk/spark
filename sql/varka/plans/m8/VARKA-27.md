# VARKA-27: Boolean outputs

## 0. What exists now (admission check)

From the code at `475322b11ca` (27 October 2026; Varka paths under
`sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen/` unless named). The
row's text is `m5/PLAN.md` 2.1 and the milestone plan's 2.5. What the code says about each premise
is below, with the file and line it rests on.

1. **A bare boolean column is already served beside a fused entry.** `classify` forwards any
   `BoundReference` entry before it compiles anything (`VarkaExpressionCompiler.java:584-586`), so
   `b AS c` is a zero-copy forward of the input's `BitVector`, as an int column's is. When nothing
   fuses, `EntryPass.run` returns no projection (`:565-566`), so a lone `SELECT b AS c` runs on
   Spark's projection. What declines is a boolean
   *expression* at the root: `compileRoot` (`:1116`) calls `compileNode`, no family of the chain
   claims a comparison or a connective (`VarkaNodeCompiler.java:194-205`: the predicate family is
   reached only through `compileCond`), the condition family leaves boolean `If`, `CASE WHEN` and
   `coalesce` to it (`VarkaConditionCompiler.java:106-110`), and `fallback` declines with
   "unsupported expression" (`VarkaNodeCompiler.java:663`). `SELECT d < d2 AS c`, `NOT b`, `b AND
   b2` and `IF(b, b2, NOT b2)` all decline there.
2. **A condition root is already a bitmap writer.** A filter's fused conjuncts are `Cond` roots of
   a `CompiledVarkaProjection` (`VarkaExpressionCompiler.java:817-830`). The body emits the
   condition and writes its known-true bits, `toLong` of the mask in the dense body and the `kt`
   word in the masked one, into the output's validity slot
   (`varka/VarkaBodyEmitter.java:1133-1155`), by `emitValidityWrite` (`:1265-1301`): per lane
   group through `orValidityBitsAt`, or accumulated
   in a register and stored a word at a time under `validityByWord` (`:1372-1375`). A `Cond` root
   maps no data segment (`:324`) and the kernel contract says its `dstData` is never dereferenced
   (`varka/VarkaFusedKernel.java:39-44`). The masked body already holds `kf` beside `kt`
   (`VARKA-331.md` 0), so a root's known word `kt | kf` costs one `lor`.
3. **The root needs a node of its own.** The shape key is the output IR, the counts and the options
   (`sql/core/.../VarkaEvaluatorBase.java:505-508`), not the output types. A bare `Cond` root in a
   projection would hash like the filter root over the same condition and share its class, which
   writes one bitmap where a boolean output needs two. A wrapper node keeps every filter shape, and
   so its bytes, as it is.
4. **A `BOOLEAN` output can only be a `BitVector`.** `ArrowUtils.toArrowType` maps `BooleanType`
   to `Bool` (`sql/api/.../util/ArrowUtils.scala:97`), and `ArrowColumnVector` answers
   `getBoolean` only from `BooleanAccessor` over a `BitVector`
   (`sql/catalyst/.../vectorized/ArrowColumnVector.java:193-194, 350-362`); every other accessor
   throws (`:293-295`). Varka hands
   each output out as an `ArrowColumnVector` (`VarkaEvaluatorBase.java:584`) and the cache may
   store it. So the "int column" of the measurement is not an output Spark can read as `BOOLEAN`;
   it can only be an intermediate packed afterwards (2.4).
5. **Arrow sizes both bitmaps to whole words, zeroed.** In arrow-vector 19.0.0 a `BitVector` has
   type width 0 (`BitVector.java:75`), and `allocateNew` gives the data and the validity buffer
   at least `((n + 63) >> 6) << 3` bytes each, one combined allocation rounded by the allocator's
   policy and then halved, data first (`BaseValueVector.java:130-148, 200-245`, the rounding at
   `:146`, the halving at `:207`), and zeroes both (`BaseFixedWidthVector.java:329-333`).
   A word store into the data bitmap therefore stays inside it; one past the last word would land
   in the validity.
6. **Every output site reads a width of 0 as nothing.** `VarkaLaneArrow.of(BOOLEAN)` is null
   (`sql/core/.../VarkaLaneArrow.java:104`), so `allocateVector` throws "no Varka output vector"
   (`VarkaKernelEvaluator.java:509`). `runKernel` records `dstWidth = getTypeWidth()`, 0 for a bit
   vector, and the sanitizer guards `len * 0` data bytes (`VarkaEvaluatorBase.java:587-593`). The
   cross-body check of `VARKA-303` allocates `max(len * w, 8)` bytes of data and compares slices of
   `w` bytes (`sql/core/.../varka/VarkaKernelRunner.java:373, 416-424`): a bit output would overrun
   its eight bytes and compare nothing. The reproduction records the 0 as the output width (`:511`),
   which its format documents as "a selection bitmap" (`varka/VarkaReproduction.java:195`) and
   `VarkaReplay` allocates as `rows * 0` data bytes
   (`sql/catalyst/src/test/.../VarkaReplay.java:217`). `VarkaLaneArrowSuite` holds a vector exactly
   where a lane reads the type (`:39`) and its type width at the storage bits (`:53-54`). These are
   the output mirror of the input sites row 331 fixed (`VARKA-331.md` 10).
7. **No condition root is served by the bitmap pass.** `planBitmapPass` skips every `Cond` root
   (`varka/Analysis.java:1050-1053`). `Analysis.pureOf` answers null for `CondValue` and every
   `Cond` but `BoolColumn`, whose pure word is its input's (`:1118, 1157-1164`). A `BoolOutput` is
   not a `Cond`, so the skip does not cover it; `m5/PLAN.md` 2.1 asks for the fact to be stated
   and tested.
8. **The all-null shortcut excludes condition roots** (`VarkaBodyEmitter.java:460-467`), and row
   460's review found the value over a condition that the shortcut answered wrongly
   (`VARKA-460.md` 5): `And` and `Or` analyze as skipping and `CondValue` inherits it.
9. **The count reads bytes.** `VarkaSelectionBitmap.countSet` is a byte loop
   (`varka/VarkaSelectionBitmap.java:85-97`), 512 reads for 4096 rows. Its one caller passes a
   slice of `(len + 7) / 8` bytes of a buffer laid out in whole words
   (`VarkaFilterEvaluator.java:211-212, 240-241`, sql/core), so a word loop needs the whole-word
   slice or a byte tail. No test calls it.
10. **The measurement, re-read.** `VarkaMilestone4MeasurementsBenchmark` compared `toLong` into a
    per-group short (`boolBitsOnly`) with an int column (`boolToVectorAnd`, `boolBlend`):
    8870 against 7511 ops/ms at 512 bits, 2761 against 3683 at 128
    (`sql/varka/engine/benchmarks/VarkaMilestone4MeasurementsBenchmark-jdk25-results.txt`). At
    four lanes the engine's own per-group bitmap write rewrites half a byte per group, the regime
    `validityByWord` exists for (`VarkaEmitOptions.java:159-171`), which `VARKA-47` measured 6 to
    9% ahead at four lanes and 11 to 20% behind at eight and sixteen (`m5/PLAN.md`, VARKA-92).
11. **The lanes.** Int, long and the narrow lanes have conditions today, and a narrow condition
    beside an int root runs widened at the int lane (`varka/VarkaKernelPlan.java:163-200`). The
    floating conditions are row 341's step 4, not built: `FLOAT` and `DOUBLE` are still
    `Owed("341")` (`varka/VarkaTypes.java:275-278`) after its lane split (#1049). The census
    (`VARKA-376.md` 9): `toLong` is intrinsic at every width but on longs at 128 bits on x86, where
    it is the Java fallback, which every long-lane filter root already pays there.
12. **The tables.** `coverage.json` holds 123 predicate rows of 260. The conformance contract
    cancels every boolean value form naming this row (`VarkaTypeConformance.java:279-283`), and its
    boolean round trip is `value("x", null)` (`:138-145`), a bare column that alone fuses nothing
    and runs on Spark's projection (item 1) once the rule goes. `VarkaTypes` names 27 as the row
    that reads a boolean as a value (`:261`).

### What differs from the row text and the milestone plan

- **"A boolean column as a projection result" is already forwarded beside a fused entry, and
  alone runs on Spark's projection** (item 1). This row builds the boolean expression as a root;
  a bare column stays as it is.
- **Both forms end in a `BitVector`** (item 4). The packed form writes the known-true bits into the
  data bitmap. The materialized form keeps the int column the measurement had, as `CondValue`'s
  0 and 1 stored to scratch at a byte a row and packed by a pass after the loop (2.4); it cannot be
  the output itself. Question 1.
- **The packed form has two writers already** (items 2, 10): per group and per word. Both are
  measured; the per-word default at four lanes stays with VARKA-92 (`m9/SCOPE.md` item 39), since
  flipping it moves every existing shape's bytes at four lanes.
- **The output sites need a bit width** (item 6), which the row text does not name.

## 1. Goal and non-goal

Every boolean-valued expression Varka fuses as a condition is also a projection output, written to
an Arrow `BitVector` with Spark's three-valued answer: data bit set where the condition is known
true, validity set where it is known (true or false), null where it is unknown. The selected-row
count is taken a word at a time. Not this row: boolean casts (332), `bit_count(bool)` (335b), the
floating conditions themselves (341), `greatest` and `least` over booleans (the census's rows 333
and 329), and the fuzzers' reach (352).

## 2. Design

### 2.1 The node

`BoolOutput(Cond cond) implements VarkaVectorIR`, a root only: its lane is its condition's, its one
child is the condition, its canonical form is `(bool-out <cond>)`, so no existing hash moves.
`Analysis` treats it as `CondValue` for `nullsFromValidInputs` (a `CondConst(NULL)` keeps the kernel
masked-only), `pureOf(BoolOutput)` is null, so the bitmap pass never serves it although
`planBitmapPass`'s `Cond` skip does not reach it (item 0.7), and a `BoolOutput` anywhere but a root
is refused. The sealed-switch sites are `VARKA-460.md` 5's list, which row 460 found by building the
last value node over a condition.

### 2.2 The write

In the root loop of `VarkaBodyEmitter` (item 0.2), beside the `Cond` arm:

| body | data bitmap | validity bitmap |
|:--|:--|:--|
| dense | `toLong` of the condition's mask | all ones: by the driver, or per group (\*) |
| masked | `kt` | `kt \| kf` |
| epilogue | the same, truncated by the existing lane mask | the same |

(\*) The driver fills it once under `denseValidityOnce`; with the option off, the per-group
all-ones write runs (`VarkaBodyEmitter.java:1318-1319`).

Both go through `emitValidityWrite`, the data into the output's data segment mapped as a bitmap of
whole words, so `validityByWord` switches both writers together and `Slots` plans a second
accumulator beside `validityAcc` only when a `BoolOutput` root exists: an unconditional slot would
renumber the later slots and move existing shapes' bytes. The driver zeroes the data bitmap with the
validity, since the warm-up and the cross-body check reuse their buffers. The bit has no stored
width, so none of the narrow, low-half or narrowed-segment stores
(`VarkaBodyEmitter.java:1171-1197`) apply: it is written at the driving lane's count. A total
condition (`<=>`, `IS NULL`) has `kt | kf` all ones, never null, as Spark has it.

The all-null shortcut (item 0.8) excludes a `BoolOutput` root outright, as it excludes a `Cond`
root, rather than through the skipping flag `CondValue` inherits: a total condition over an
all-null column has an answer (`b IS NULL` is true on every row, `b <=> b2` where both are null),
and row 460 tested the flag over `AND` and `OR` only. The tests are those three over an all-null
batch beside `b0 OR b1` with `b0` null throughout and `b1` true on a row.

### 2.3 The compiler and the lanes

`compileRoot` sends a `BooleanType` expression to `compileCond` and wraps the result, so every
predicate the compiler fuses is an output with no new rule, under the predicate family's switch;
a declined condition declines the entry with its own reason. The drive is the existing rule: a
`BoolOutput`'s emission lane is its condition's lanes, so an int condition beside int values
shares the kernel, a narrow one joins an int kernel widened, and an int condition beside long
values takes a kernel of its own (VARKA-480). A double condition follows row 341's 2.4 when it
lands (beside the long lane by `VectorMask.cast`), and its coverage rows come under this row's
test of 3.3 with no change here.

### 2.4 The materialized form

Behind a `MACHINE` option, `materializeBoolOutput`, off by default: the condition stored as
`CondValue`'s 0 or 1 narrowed to a byte a row into the kernel's scratch (the segment the
materialized prefixes use), and a pack pass after the loop and the epilogue that loads the scratch
as a `ByteVector` of the emission's width and writes `compare(NE, 0).toLong()` into the data bitmap:
one mask transfer per 16 rows at 128 bits where the packed form makes four. That holds in the dense
body only, where the condition is a `VectorMask`: in the masked body `kt` is a word, the packed form
writes it with no transfer, and the blend needs a mask made from the word per group
(`emitMaskOfWord`), so the option applies to the dense body and the masked body stays packed.
Validity is the packed form's. Its `toLong` is over a byte mask, so at 128 bits on x86 it also
avoids the Java fallback the long-lane `toLong` takes there (item 0.11). It is the int column of
item 0.10 made storable; whether it pays is the measurement's (4.3).

### 2.5 The evaluator and the runner

`VarkaLaneArrow.of(BOOLEAN)` names `BitVector` (its admission is unchanged: `isArrowBacked` matches
`BitVector` first, `VarkaEvaluatorBase.java:929`). BOOLEAN stays `AsCondition`, so
`VarkaLaneArrowSuite` gains an explicit exemption: BOOLEAN's vector is an output vector, not a
lane's, in its first test, and its type width of 0 against one storage bit in its second. Each
site of item 0.6 takes a bit output's data as `((len + 63) / 64) * 8` bytes: the sanitizer's guard,
the cross-body check's allocation and a bit-by-bit compare of data where validity is set. The
reproduction cannot reuse 0, which means a selection bitmap: a bit output records a new marker,
`KernelCall.BIT_OUTPUT = -1`, in `outputWidths` (the parser already reads it), and `VarkaReplay`
allocates whole words of data for it. The kernel contract's javadoc gains the
`BoolOutput` output: `dstData` is a bitmap of whole words. `VarkaTypes`'s boolean entry names 332
as the row that reads a boolean as a value (a cast) instead of 27.

### 2.6 The count

`countSet` counts `Long.bitCount` over the whole words and the last partial word masked, over the
whole-word slice the evaluator already allocates (item 0.9), which its javadoc then requires: 64
reads for 4096 rows. No Harley-Seal (the milestone plan's 2.5).

## 3. Tests and oracles

1. **`VarkaEmitterBoolSuite`**: `BoolOutput` over every leaf state (true, false, null with the data
   bit clear and set) for `AND`, `OR`, `NOT`, the five comparisons, `<=>`, `IF`, `coalesce` and
   `IS NULL`, int and long lanes, dense and masked, at 4, 8 and the default lanes and the suite's
   lengths and offsets, each row's data and validity bit held to Spark's `eval`, in both writers
   and the materialized form; the all-null cases of 2.2 (`b0 OR b1`, `b IS NULL`, `b <=> b2`,
   `CondConst(TRUE)`); a `CondConst(NULL)` root masked only.
2. **The bitmap-pass boundary** (`m5/PLAN.md` 2.1): `pureOf(BoolOutput)` is null, and every
   `BoolOutput` over a comparison, `And`, `Or`, `Not` or a `BoolColumn` of nullable operands is
   not served by the pass, a test beside the word-algebra tests; and the sentence in the algebra's javadoc.
3. **Every predicate row as an output.** `VarkaCoverageDifferentialSuite` runs each predicate row as
   `SELECT rid, (p) AS c` on every fixture, Varka against Spark row for row (true, false and null
   told apart), and asserts a kernel served it; a row that is a bare boolean column fuses nothing
   beside `rid` and asserts Spark's projection ran instead. `VarkaCoverageSuite` compiles each
   predicate row as a projection entry. Question 2.
4. **Coverage rows**: a family "Boolean outputs" of about eight rows for the documentation, one per
   lane and form (`d < d2`, `l < l2`, `y1 < y2`, `NOT b`, `b AND b2`, `(d < d2) OR b`,
   `l <=> l2`, `CASE WHEN b THEN b2 END`).
5. **Conformance** (`VARKA-484.md` 2): the owed rule for boolean values goes; the boolean round trip
   becomes `if(x IS NULL, true, x)` and `coalesce(x, false)`; and every condition form of every
   conformance suite also runs as a value output.
6. **The runner and the count**: the cross-body check fails on a flipped data bit planted on an
   all-valid row (a masked batch skips rows with a null input, `VarkaKernelRunner.java:419`); a
   replayed bit output writes whole words; an end
   to end boolean output under `-Dvarka.sanitizeMemory=true` at a length not a multiple of 64;
   `countSet` against the byte loop over lengths 0 to 200 and 4096, with stale bits past the end.
7. **The bytes oracle**: every existing shape's bytes unchanged; the new rows add entries.
8. **The option**: `materializeBoolOutput` in `VarkaEmitOption.TABLE`, so the IR fuzzer draws it and
   the option matrix runs it. The fuzzers' grammar leaves `BoolOutput` out with the other boolean
   nodes until row 352.

## 4. Predictions and costs

The measurement is a sql/core class of its own, `VarkaBooleanOutputBenchmark`, on
`VarkaBenchmarkBase` over Arrow-cached rows: `d < d2`, `l < l2`, `y1 < y2`, `NOT b`, `b AND b2` and
`(d < d2) AND (l < l2)` as outputs, each with no nulls (dense) and with nulls (masked), the filter
over the same condition as the control. Its baseline, both arms on the row path, is committed
first on the unchanged code; results from `benchmark.yml` on the EPYC 7763 at 256 and 128 bits.

1. **No existing shape's bytes move**; the `option_arms` digests move with the new option.
2. **Against Spark's row path**, every int and narrow output at least 1.5 times faster by best at
   256 bits, near the 2 to 3 times the filter benchmark's committed rows show; a boolean output is
   the filter's work and one more bitmap. Long outputs the same at 256 bits; at 128 bits on x86 the
   long dense body is the fallback `toLong` (item 0.11), predicted slower than the row path, a
   recorded result (question 4).
3. **The forms.** The per-word writer ahead of the per-group one by 5 to 10% at four lanes (int at
   128, long at 256) and behind at eight and more, as `VARKA-47` found for validity; the
   materialized form, dense body only (2.4), behind the packed one by more than 10% at 256 bits
   and within 10% either way at 128 bits.
4. **Test time**: the differential's 123 output tests at `VARKA-334`'s 0.38 s a test and the
   calibration table's ratio of 1.3, about 60 s; the emitter suite under 10 s more (`VARKA-460`'s
   0.07 of its estimate).

Cost: medium, as the row says. About four days after the baseline, one more for the materialized
form.

## 5. Order and files

1. **The baseline class**, its own pull request on the unchanged code.
2. **The count** (2.6) and its test.
3. **The node, the write and the runner sites** (2.1, 2.2, 2.5), IR-level tests only.
4. **The compiler arm, coverage rows, the differential and conformance** (2.3, 3.3 to 3.5), the
   generated files in `ADDING_AN_EXPRESSION.md`'s order.
5. **The materialized form** (2.4), if question 1 says so.
6. **The measurement** and its verdict; the PR build and the option matrix dispatched.

| file | what |
|---|---|
| `varka/VarkaVectorIR.java` | `BoolOutput` and its canonical form |
| `varka/Analysis.java`, `varka/Slots.java` | its flags, the data accumulator |
| `varka/VarkaBodyEmitter.java`, `varka/VarkaVectorWalk.java` | the write, the pack pass |
| `varka/VarkaEmitOption.java`, `varka/VarkaEmitOptions.java` | `materializeBoolOutput` |
| `varka/VarkaSelectionBitmap.java`, `varka/VarkaFusedKernel.java` | the count, the contract |
| `VarkaExpressionCompiler.java`, `varka/VarkaTypes.java` | the root arm, the values row |
| `VarkaLaneArrow.java`, `VarkaEvaluatorBase.java`, `VarkaKernelRunner.java` | the bit width |
| `varka/VarkaReproduction.java`, `VarkaReplay.java` (test) | the `BIT_OUTPUT` marker |
| `VarkaLaneArrowSuite.scala` (test) | BOOLEAN's output-vector exemption |
| tests | emitter suite, differential, coverage rows, conformance, runner, count |
| benchmark | `VarkaBooleanOutputBenchmark` and its results files |
| generated | `coverage.json`, `docs/sql-varka.md`, `emitted_bytes.json`, `width_audit.json` |

## 6. Out of this row

Boolean casts and `CAST(p AS INT)` as an output (332), `bit_count(bool)` (335b), the floating
conditions (341), the fuzzers' boolean reach (352), the per-word default at four lanes
(VARKA-92, `m9/SCOPE.md` item 39), and lifting an int condition onto a long kernel's lane instead
of a kernel of its own.

## 7. Declines that stay

`greatest` and `least` over booleans, and every boolean expression whose condition declines today
(a string comparison, a UDF, `isnan` before 341), each with its condition's reason.

## 8. Questions for the owner

Answered by the owner on 27 October 2026: (a) to all four - both forms built, measured and kept;
the derived coverage run with about eight documented rows; an int condition beside long values in
a kernel of its own; long outputs fused at 128 bits on x86 unless 4.2's reading shows a loss the
filters do not have.

1. **The forms.** (a) The packed form with both writers, and the materialized form of 2.4 behind
   its option, measured and kept whichever wins. (b) The packed form only, with item 0.4 recorded
   as the reason the int column is not an output. *Recommended: (a)*: the row keeps both forms, and
   the materialized one is the only form that cuts the mask transfers at 128 bits.
2. **The coverage claim.** (a) The derived run of 3.3 over every predicate row, with about eight
   documented rows. (b) A projection row in the table for each of the 123 predicate rows.
   *Recommended: (a)*: the run covers every predicate row present and future, 341's included,
   without doubling the table.
3. **An int condition beside long values.** (a) A kernel of its own, as today's lane rule gives.
   (b) Lift it to the long lane. *Recommended: (a)*: correct with no new rule; the lift needs every
   earlier entry re-lifted when the kernel's lane moves.
4. **Long outputs at 128 bits on x86.** (a) Fuse, as the long filters there do. (b) Decline at two
   long lanes. *Recommended: (a)*, unless 4.2's reading shows a loss the filters do not have.

## 9. Done when

Row 27's text holds over every predicate row of the coverage table, with `m5/PLAN.md` 2.1's list:
boolean sub-expressions stay in mask space and are written once at the root; the forms are
measured per width with their verdict and kept behind their options; the output is a `BitVector`
whose data is the known-true bits and whose validity is the known bits; a null input gives a null
output, never a false one, under every null pattern of the differential; no boolean root has a pure
word or is served by the bitmap pass, tested and stated in the javadoc. Also: `countSet` reads
words; the conformance round trip for `BOOLEAN` runs; the bytes oracle is unchanged for every
existing shape; and the PR build and the option matrix pass.

## 10. Outcome

(Written when the work lands.)
