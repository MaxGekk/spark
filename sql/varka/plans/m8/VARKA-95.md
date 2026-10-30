# VARKA-95: The checked int multiply over a column, and `i % k`

## 0. What exists now (admission check)

From master at `fa698652fd2` (30 October 2026; Varka paths under `sql/catalyst/src/main/java/org/
apache/spark/sql/catalyst/expressions/codegen/` unless named, Spark's under `sql/catalyst/src/main/
scala/org/apache/spark/sql/catalyst/` unless named, `arithmetic.scala` being `expressions/
arithmetic.scala`). The row's text is the milestone plan's row 95, from the catalogue's item 39
table (`m9/SCOPE.md`) and `m5/PLAN.md` 2.30. The JDK was read in
`/home/max/proj/openjdk-build/jdk25`. The scalar, vector and solver checks below are in
`results/VARKA-95-admission.txt`, with the program, its commands and its output.

**Spark's answers.** Each is what the kernel reproduces.

1. **`int * int` is an `INT`, wrapping under LEGACY.** `Multiply` (`arithmetic.scala:608-658`) keeps
   the left operand's type (`BinaryArithmetic.internalDataType`, `:218-222`); its integral arm is
   `numeric.times` when `failOnError` is off (`:655`), and its codegen `a * b` (`:345-351`), Java's
   wrapping multiply.
2. **ANSI raises `ARITHMETIC_OVERFLOW`.** `failOnError` is `ANSI | TRY` (`:224-228`); under it the
   `IntegerType` arm calls `MathUtils.multiplyExact(a, b, context)` (`:645-649`, codegen
   `:336-343`), which is `withOverflow(Math.multiplyExact(a, b), hint = "try_multiply", context)`
   (`sql/api/src/main/scala/org/apache/spark/sql/catalyst/util/MathUtils.scala:54-56`, the catch at
   `:141-147`). The error is `ExecutionErrors.arithmeticOverflowError`
   (`sql/api/src/main/scala/org/apache/spark/sql/errors/ExecutionErrors.scala:124-151`): class
   `ARITHMETIC_OVERFLOW`, template "<message>.<alternative> If necessary set <config> to \"false\"
   to bypass this error.", SQLSTATE `22003` (`common/utils/src/main/resources/error/
   error-conditions.json:129-134`), with `message` the JDK's "integer overflow" canonicalised to
   `overflow` (`:128-139`), `alternative` " Use 'try_multiply' to tolerate overflow and return NULL
   instead." (`:140-142`) and `config` the ANSI key. Varka never builds this text: the batch
   declines and the row engine raises it.
3. **TRY nulls the row.** `try_multiply` over numerics is `Multiply(l, r, EvalMode.TRY)`
   (`expressions/TryEval.scala:268-274`); `eval` catches to null (`arithmetic.scala:376-397`) and
   codegen wraps the exact call in a `try` that sets the result null (`:356-374`). A null operand
   is a null result in every mode (`:377-383`).
4. **`int % int` is Java's truncated remainder, typed by the dividend.** `Remainder`
   (`arithmetic.scala:1045-1110`) is a `DivModLike`; its integral arm is `integral.rem`
   (`:1101-1103`): `-7 % 2 = -1`, `7 % -2 = 1`, `Int.MinValue % -1 = 0`. `failOnError` is ANSI only
   (`:1058`), and `try_mod` is `Remainder(_, _, TRY)` (`TryEval.scala:179-191`).
5. **A zero divisor is null under LEGACY and TRY and raises `REMAINDER_BY_ZERO` under ANSI**,
   after the dividend's null (`DivModLike.eval`, `arithmetic.scala:701-727`): "Remainder by zero.
   Use `try_mod` to tolerate divisor being 0 and return NULL instead. If necessary set <config> to
   \"false\" to bypass this error.", SQLSTATE `22012` (`error-conditions.json:7054-7059`).
6. **A non-zero literal divisor makes the three modes one.** `divisorIsNonZero` (`:696-699`) drops
   the zero test from the generated code when the divisor is a non-null non-zero literal, and
   `Int.MinValue % -1` is 0 in Java with no exception, so for a literal `k != 0`, `i % k` is null
   exactly where `i` is, and never raises, in LEGACY, ANSI and TRY alike.

**Varka today.**

7. **A checked int multiply declines unless its operands' bounds rule overflow out, a literal
   factor included.** `case Multiply a when isInt(a)` (`VarkaNodeCompiler.java:496-497`) goes
   through `arithOver`, which declines a non-`WRAP` int `MUL` that `cannotOverflow` does not
   clear (`:1101-1129`, the decline at `:1121`). The coverage table states it: "`i * 2` over a
   bare int column declines" (`sql/varka/coverage.json:592`). So `m5/PLAN.md` 2.30's "multiplying
   by a *literal* is fine" holds only for a bounded operand; `i * 3` under ANSI, Spark's default,
   declines as `i * j` does. LEGACY `i * j` fuses today as a `WRAP` `IntArith`.
8. **The emitter refuses a checked int multiply too.** `emitIntArith` throws "a checked multiply
   has no int-lane overflow test" (`varka/VarkaVectorWalk.java:661-662`), `Analysis` gives a
   checked `MUL` no `INT_OVERFLOW` refusal unless it is narrow (`varka/Analysis.java:670-684`), and
   the IR fuzz grammar draws a `MUL` only as `WRAP` (`VarkaIrGrammar.java:701-704`, test tree).
   The reference evaluator already answers a checked `MUL` at the int lane, empty where the exact
   product leaves the range (`VarkaReference.java:276-298`, test tree).
9. **The pieces of both forms exist.** `WidenLane` and `NarrowLane` (`varka/VarkaVectorIR.java:563`,
   `:605`, row 28), a `GuardedRange` at the long lane (`:536`), a long `WRAP` multiply (row 39), a
   `NULL`-mode `IntArith` that narrows its own word (`Analysis.java:324-331`), and the int lane's
   `ConstDivide` (`VarkaVectorIR.java:764`) with its multiply-high lowering
   (`varka/VarkaDivisionLowering.java:309-342`), which converts each int half with `I2L`
   (`convertShape`, part 0 and 1), multiplies in long lanes, and narrows back with `L2I`.
10. **`ConstDivide` at the int lane is exact for every int divisor but 0 and -1.** Its pair comes
    from `signedMagic`, which every magnitude from 2 to `2^31` has (`VarkaDivisionLowering.java:
    230-275`), and `MulHiMagic` refuses a pair that misses Granlund and Montgomery's Theorem 5.1
    (`:185-194`), so a pair that reaches the emitter is exact by the theorem;
    `int_mulhi_divide.smt2` re-proves the lanes' arithmetic for the divisors in use. -1 is refused
    by `Analysis` (`:1363`) and 0 by the record. `Integer.MIN_VALUE` divides by the magnitude
    `2^31`.
11. **A tree holding a long node drives at the long lane.** `emissionLane`
    (`varka/VarkaKernelPlan.java:166-195`) returns `LONG` for any tree with a long node; its int
    values then sit in the low half of the int species, one row a long lane per iteration (row 28,
    `VARKA-28.md` 9), and an int root beside a long one takes a kernel of its own (`kernelLane`,
    `:203-210`). At 128 bits on x86 a long lane's mask is the Vector API's Java fallback
    (`VARKA-376.md` 9).
12. **The record already gives both halves of this row to other rows.** `VARKA-104.md` 3.3 and its
    sequencing step 5 plan "the int32 checked multiply through VARKA-28's widening, once 28 has
    landed", and `VARKA-481.md` 3.3 repeats it ("that row builds it, this one does not"), as
    `NarrowLane(GuardedRange(IntArith(MUL, WRAP, WidenLane(x, LONG), WidenLane(y, LONG)), INT_MIN,
    INT_MAX, mode))`. `VARKA-30.md` 2.4 builds `%` by a literal at the int lane as `a - q * c`
    over `ConstDivide`, and its section 7 calls row 95's `i % k` "the constant-divisor
    specialisation that this row's kernel takes for a literal". Section 2.1 proposes the split and
    section 8 asks it.

**The JDK.**

13. **No multiply-high on any lane type.** `VectorOperators` has no `MULHI` or `UMULH` (grep of
    `jdk.incubator.vector/VectorOperators.java`), so an int product's high word comes from long
    lanes.
14. **A long multiply of two sign-extended ints is one `vpmuldq`.** `MulVLNode::has_int_inputs`
    holds when both inputs are `VectorCastI2X` to long or an arithmetic right shift by 32 or more
    (`src/hotspot/share/opto/vectornode.cpp:2213-2232`), and `vmuldq_reg` matches it at
    `UseAVX > 0` with cost 100 (`src/hotspot/cpu/x86/x86.ad:6108-6118`), against 500 for
    `evpmullq` (`:6017-6031`) and for the three-multiply `vmulL_reg` sequence AVX2 otherwise takes
    (`:6076-6094`). The admission's probe (check 6) compiled the halves form of 2.2 and found,
    in the steady C2 method at 256 bits, `vpmuldq` and neither `vpmullq` nor `vpmuludq`, under the
    laptop's default level and under `-XX:UseAVX=2`; only the on-stack-replacement compile, which
    a kernel's loop method does not use once warm, took the general multiply.
15. **The int conversions are intrinsic everywhere the census reads.** `I2L` and `L2I` read `I` at
    every width on the three hosts (`results/VARKA-376-census-epyc-7763.json`, `-neoverse-n2.json`),
    which is what `emitMulHiDivide` already relies on.

### What differs from the row text and the milestone plan

- **The gap is not "a right operand that is a column".** A checked multiply of a bare int column
  declines with a literal factor too (item 7); the row's multiply half is every ANSI and TRY int
  `*` whose bounds do not clear it, and `m5/PLAN.md` 2.30 gets a dated note.
- **`i % k` has no modes and no zero test** (item 6); the row's "checked-multiply rule" does not
  apply to it, and what it needs is the exactness of `ConstDivide` (item 10) and one identity.
- **Both halves are claimed elsewhere** (item 12). The plan is written under the split of 2.1,
  marked as proposed; section 8 asks the lead and the owner.

## 1. Goal and non-goal

Under the proposed split of 2.1: an `INT` multiply in ANSI and TRY fuses whatever its operands, a
column or a literal, raising nothing in a kernel (an overflowing lane declines its batch under ANSI,
and is null under TRY, as Spark's row is); LEGACY keeps today's wrapping multiply. And `i % k`,
`mod(i, k)` and `try_mod(i, k)` for an `INT` dividend and a non-zero `INT` literal `k` fuse in every
mode. Not this row: a column divisor and every other type's `%` (30), `pmod` (30), `BIGINT` multiply
(104), the narrow multiply (481, 330), decimals (349), floating `%` (346).

## 2. Design

### 2.1 Ownership, as proposed

- **The int32 checked multiply moves to this row.** `VARKA-104.md` 8 step 5 and 3.3's first
  paragraph become pointers here; 104 keeps the `BIGINT` multiply and its options A to C. Row 95
  is the row whose text names the shape, and its two forms (2.2) need nothing of 104's lattice.
- **`i % k` by an `INT` literal moves to this row**, from `VARKA-30.md` 2.4's first bullet; 30
  keeps a column divisor, `pmod`, the narrow and `BIGINT` dividends, and its `IntDivide` node, and
  its 30a builds `%` by a column over the int lane, where the literal form is this row's tree.
  The literal form needs no node, no refusal and none of 342's floating division, so it can land
  before 30a, which waits on 104, 28, 329, 341 and 342 (`VARKA-30.md` 5).

Section 8 questions 1 and 2 hold the other options. If the lead keeps the record as it is, this
plan's 2.2 is handed to 104 and 2.4 to 30, and row 95 closes on "the record says why not".

### 2.2 The checked multiply: two forms, both built

The overflow test needs the 64-bit product (item 13). Two lowerings, built and measured against
each other, under an option `checkedIntMultiply` (`HALVES`, `LONG_TREE`), the losing arm kept:

**`HALVES`: an int-lane lowering of `IntArith(MUL, FAIL | NULL)`.** No new IR node. The emitter
gains the arm `emitIntArith` refuses today (item 8), in the shape of `emitMulHiDivide`:

    for part in 0, 1:
      p   = I2L(a, part) * I2L(b, part)          // one vpmuldq a part (item 14), exact: |p| <= 2^62
      hi |= L2I(p >>> 32, -part)                 // the product's high word, in this part's lanes
    lo  = a * b                                   // the wrapped product, the node's value
    ovf = hi != (lo >> 31)                        // set exactly where the product leaves the int

`ovf` is an int-lane mask, so it goes to the existing `emitOverflowMask`: `FAIL` ORs it into the
`INT_OVERFLOW` accumulator, `NULL` clears the node's own word (item 9). The shift before `L2I`
is logical, which AVX2 has for longs (`vpsrlq`), since only the low 32 bits of it are kept. About
fourteen lane operations against the wrapping multiply's one: four `I2L`, two long multiplies, two
shifts, two `L2I`, an `or`, the int multiply, a shift and a compare. The kernel stays int-driven,
so nothing else in it changes width. In a long-driven kernel only part 0 holds live lanes, and
part 1 is skipped. At a width with no long species to convert into (`divider.canConvert()`,
`VarkaDivisionLowering.java:89-91`) the shape declines as today.

**`LONG_TREE`: the composed tree** of item 12, `NarrowLane(GuardedRange(IntArith(MUL, WRAP,
WidenLane(x, LONG), WidenLane(y, LONG)), INT_MIN, INT_MAX, mode), INT)`, which needs no emitter
change but 481's `NULL` mode for TRY and its marked guard (`VARKA-481.md` 3.2) for the
`checkIntOverflow` rule. Its cost is item 11: the whole kernel drives at the long lane, half the
rows an iteration for every node in it, an int root beside it splits into its own kernel, and at
128 bits on x86 the guard's mask is the Java fallback.

**Considered, not built: the double-lane test**, `(double) a * (double) b` against
`[-2^31, 2^31 - 1]`. It is exact as a test (admission check 3: rounding is monotonic and both ends
are representable, so a product inside is exact and one outside rounds to outside), but its mask
is formed in double lanes of half the int count, and a mask crossing between the floating and
integral kinds is not built (`VARKA-341.md` 2.4); it would also convert with `I2D` where `HALVES`
converts with `I2L`, for no fewer operations. Recorded in section 7.

The compiler builds the form the option names: `arithOver` (`VarkaNodeCompiler.java:1101-1129`)
keeps its bound rule, so a multiply the bounds clear still emits `WRAP` with no check, and the
decline at `:1121` becomes the option's form. The compiler already takes the options
(`VarkaExpressionCompiler.java:121`).

### 2.3 The modes

| mode | today | `HALVES` | `LONG_TREE` |
|---|---|---|---|
| LEGACY | `IntArith(MUL, WRAP)` | unchanged | unchanged |
| ANSI | declined if unbounded | `IntArith(MUL, FAIL)`, `INT_OVERFLOW` | marked `FAIL` guard |
| TRY | declined if unbounded | `IntArith(MUL, NULL)`, own word | 481's `NULL` guard |

Under `checkIntOverflow=false` an ANSI multiply wraps under both forms, as every other int check
does and as the owner's choice B on row 481 keeps for the narrow multiply (`VARKA-481.md` 3.2):
for `HALVES` through the existing `INT_OVERFLOW` gate (`Analysis.java:676-680`, the `MUL`
exclusion lifted), for `LONG_TREE` through 481's marked guard. A TRY multiply does under the
option what `try_add` does today, under both forms; the gate reads `FAIL` only, so the build
reads the `NULL` path's handling of the option and states it in the outcome.

### 2.4 `i % k`

`Remainder` over an `INT` dividend and an `INT` literal `k` (folded by the optimizer from any
constant expression), in any mode, compiles to existing nodes:

- **`k = 0`** declines at compile time naming row 30: every row is null or raises.
- **`k = 1` or `-1`** is `IntArith(MUL, WRAP, x, 0)`: zero with the dividend's validity.
- **`|k| = 2^j`, `1 <= j <= 31`** (`Integer.MIN_VALUE` is `j = 31`), the shift form:
  `x - ((x + ((x >> 31) >>> (32 - j))) & -2^j)`, as `IntArith` and `BitOp` nodes. The bias makes
  the mask round toward zero, so the result has the dividend's sign, and `% -2^j` is `% 2^j`.
  Five lane operations. Exhaustively equal to Java's `%` for every int, every `j` and both signs
  (admission check 5b).
- **Any other `k`**: `IntArith(SUB, WRAP, x, IntArith(MUL, WRAP, ConstDivide(x, k), k))`. Java
  defines `a % k` as `a - (a / k) * k` (JLS 15.17.3), `ConstDivide` is `a / k` exactly (item 10),
  and with `|k| >= 2` the product `q * k` is at most `|a|` in magnitude, so neither wrapping
  operation wraps. Sampled against Java's `%` over 587 divisors (admission check 5a).

The magic-number remainder of Lemire, Kaser and Kurz ("Faster Remainder by Direct Computation",
2019), which skips the quotient, is not used: for a 32-bit dividend it needs a 64-bit multiplier
and the high half of a 64 by 32-bit product, two general long multiplies a part where the quotient
form has one `MulVL` by a 32-bit constant, a `vpmulld` and a subtraction. Recorded in section 7.

The shift form against the multiply-high form for a power of two is a performance fork, so both
trees are timed (section 4) and the owner picks the default; if the winner differs by host, the
choice becomes an option arm, otherwise the losing tree stays as the benchmark's and the
differential's reference.

The range analysis sees the composed tree, not a remainder, so `(i % 7) * 1000` under ANSI keeps
its multiply's check: correct, and a bound a remainder node of row 30's would give.

### 2.5 What declines and what raises

Nothing raises in a kernel. Per batch under ANSI: a valid lane whose product leaves the int range
(`INT_OVERFLOW`, or `VALUE_RANGE` under `LONG_TREE` before 481's marked guard). Never a decline:
LEGACY; TRY, which nulls; `i % k` for `k != 0`. At compile time: `% 0`, a non-literal divisor
(row 30), a non-`INT` operand (the rows of section 1), `HALVES` at a width with no long species.

### 2.6 Compiler, reference, cost, warm-up, census

`VarkaNodeCompiler` gets the `Remainder` arm beside the arithmetic ones and the option's form in
`arithOver`; the reference evaluator needs nothing new (item 8). `VarkaEmitBudget` gets a weight
for a checked int `MUL` under `HALVES`; the cost model's features `IntArith/MUL/FAIL/INT` and
`/NULL/INT` join row 352's awaiting-refit list. The warm-up's blocks reach the halves form
(`VarkaWarmupReachSuite`). The census's `*`, `try_multiply`, `%`, `mod` and `try_mod` rows gain
`int` (section 6).

## 3. Tests and proofs

1. **The differential at the edges.** Coverage rows for `i * j`, `i * 3`, `try_multiply(i, j)`,
   `datediff(d2, d) * i` (the shape 2.30 opened on), `i * j > 0` as a filter, `i % 7`, `i % -7`,
   `i % 16`, `i % Int.MinValue`, `i % 1`, `mod(i, 7)` and `try_mod(i, 7)`, each in the three
   modes, through `VarkaCoverageDifferentialSuite` and `VarkaMaybeThrows` for the raising rows
   (the error with its `== SQL ==` context, row 414's symptom), over fixture columns holding
   `Int.MinValue`, `Int.MaxValue`, `-1`, `0`, `1`, `46340`, `46341`, `65536` and nulls; each row
   names its verification for row 490's check.
2. **The edge pairs in the emitter suite** (`VarkaEmitterArithmeticSuite`, Scala, as the file is):
   the 187 edge values of the admission program, every one of the 34969 pairs in one batch, at
   512, 256 and 128 bits, each mode, both arms, against `Math.multiplyExact` and a mixed-null
   pattern; the same for `i % k` over the 187 dividends and the divisor list of check 5a. Then a
   long-driven kernel holding a checked multiply, `CAST(i * j AS BIGINT) + l`, where `HALVES`
   skips part 1.
3. **The power-of-two form over every int**, an opt-in 2^32 sweep a `j` as the existing
   `ConstDivide` sweeps are, run by hand in the gate's sweep step (no workflow sets `varka.sweep`,
   `VARKA-30.md` 3.2), the scalar form of it already in the admission (check 5b).
4. **SMT, `sql/varka/proofs/int_checked_multiply.smt2`**, over bit-vectors (`QF_BV`, as
   `leap_hash.smt2` is): the 64-bit product of two sign-extended ints never overflows; its low
   word is the int `bvmul`; `hi != lo >>a 31` holds exactly where the product leaves the int range;
   that last over any 64-bit value with no multiply at all; and the planted `>>>` refuted with a
   witness. The spike decides each under Z3 5.1.0 in 0 to 17 s (admission check 7). The statement
   that the range test is Z3's `bvsmulo` at 32 bits also holds, but took about 488 s, too slow
   for the linters' job, and is not needed there: `Math.multiplyExact` throws exactly when the
   exact product leaves the range, by its specification; it goes to the nightly's both-solver run
   or is dropped, and cvc5's times are taken before the file lands. The power-of-two form per `j`
   over bit-vectors in the same file. The quotient
   form's exactness is `int_mulhi_divide.smt2` and Theorem 5.1 (item 10); nothing new is proved
   there.
5. **Planted faults** (row 504), each planted on the branch, seen failing, reverted, and listed
   in the pull request with the test that caught it: the halves compare with `>>>` for `>>` (the
   admission's check 4 finds 8382 of the 34969 edge pairs wrong, so test 2 must fail); part 1
   skipped in an int-driven kernel (the upper lanes unchecked, test 2's pairs past the first half
   of a vector); the `NULL` arm disposing nothing (test 1's `try_multiply` rows); the sign tail of
   `emitMulHiDivide` dropped (`VarkaDivisionLowering.java:336-337`, test 1's `i % 7` on negative
   dividends); the bias's `>>>` as `>>` in the shift form (test 3); and the proof file's check
   changed to the logical shift, which must come back `sat`.
6. **The fuzzers.** No new node, so no new production: the IR grammar's `MUL` draws `FAIL` and
   `NULL` beside `WRAP` (`VarkaIrGrammar.java:701-704`), its comment going, in a sequence of its
   own seed as 481's nulling draw does (`VARKA-352.md`), with operand bounds that overflow on some
   draws; the Spark-level fuzzer's int columns draw `*` under ANSI and TRY and `%` by a literal;
   352's cross-mode property "TRY is null exactly where ANSI raises" for `try_multiply`.
7. **The matrix and the bytes.** The option matrix dispatched on the branch, `checkedIntMultiply`
   in its table and both arms fuzzed; `emitted_bytes.json` adds rows and moves no existing shape.

**Test time**, at the calibration table's ratios: the twelve coverage rows in three modes about
20 s in the differential (VARKA-336's 0.75 to 1.25 over the same kind of rows); the edge-pair
tests a few seconds (VARKA-460's 0.07 for short emitter tests); the proof file under 30 s under
Z3, its spike's sum; the opt-in sweeps outside the build. The step budget
(`sql/varka/ci/step-budget.tsv`) is read before the pull request says so.

## 4. The baseline, and predictions

**The baseline first, its own pull request on the unchanged code:** a sql/core class on the row
path, as 377's are, `VarkaIntArithmeticBaselineBenchmark`, over `i * j` and `i * 3` under ANSI,
`try_multiply(i, j)`, `datediff(d2, d) * i` under ANSI, `i * j` under LEGACY (fused today, the
control), `i % 7` and `i % 16`, as projections and as filters (`i * j > 0`, `i % 7 = 0`), over
data where no product overflows; run on the EPYC 7763 through `benchmark.yml` at 256 and 128 bits
and committed beside the class. Under the record's split (8.1 (b), 8.2 (b)) the `%` shapes go to
30's baseline class, which already lists `a % 7`.

**The kernel benchmark:** `VarkaArithmeticBenchmark` (catalyst) gains a section "the checked
multiply": LEGACY `WRAP`, ANSI `HALVES`, ANSI `LONG_TREE`, ANSI with `checkIntOverflow` off, TRY
under each arm, and a mixed-null case; and a section "remainder by a literal": `i % 7`, `i % 16`
by the shift tree and by the multiply-high tree, `i % -16`. Its javadoc's "there is no third arm
with the multiply checked, because there is no such kernel" is rewritten. Laptop runs label
themselves as such; the numbers the default rests on come from the EPYC 7763 at 256 and 128 bits.

Predictions, registered before the build:

1. `HALVES` under ANSI runs at 0.3 to 0.6 of the LEGACY wrapping rate at 256 bits in L1 and L2.
2. `LONG_TREE` runs at 0.5 to 0.9 of `HALVES` at 256 bits, and under 0.2 of it at 128 bits on
   x86, where its guard's long mask falls back to Java.
3. TRY costs within 10% of ANSI under each arm, the disposal being one AND against one OR.
4. Every fused multiply form beats the row path at 256 bits by at least 2 times.
5. `i % 7` beats the row path at 256 bits by at least 4 times; the shift tree beats the
   multiply-high tree for `i % 16` by 1.5 to 3 times.
6. No existing shape's bytes move: every checked int multiply declined before.

## 5. Order, files and size

1. **The baseline** (4), its own pull request.
2. **95a, `i % k`** (2.4): the `Remainder` arm, the coverage rows, the census lines, tests 1 to 3
   and 5 for it, the remainder section of the kernel benchmark. Needs nothing unmerged.
3. **95b, `HALVES`** (2.2, 2.3): the emitter arm, the `Analysis` exclusion lifted, the compiler's
   form, the budget weight, the proof file, the grammar, the remaining tests, the multiply section
   of the kernel benchmark. Needs nothing unmerged.
4. **95c, `LONG_TREE` and the verdict**: the option `checkedIntMultiply`, the tree, the pair on
   the runners, the default by the owner from the numbers. After row 481's `NULL` mode and marked
   guard merge.

| file | what |
|---|---|
| `VarkaNodeCompiler.java` | the `Remainder` arm, the option's form in `arithOver` |
| `varka/VarkaVectorWalk.java` | the halves lowering in `emitIntArith` |
| `varka/Analysis.java`, `Slots.java` | `INT_OVERFLOW` for a checked `MUL`; the halves' scratch |
| `varka/VarkaEmitOptions.java`, `VarkaEmitOption` table | `checkedIntMultiply` |
| `varka/VarkaEmitBudget.java`, the warm-up, the cost register | weight, reach, features |
| `VarkaIrGrammar.java` (test) | the checked `MUL` draw |
| `sql/varka/proofs/int_checked_multiply.smt2`, `VarkaProofFiles.java` | test 4 |
| tests | `VarkaEmitterArithmeticSuite`, coverage rows, the sweep |
| benchmarks | the baseline class, `VarkaArithmeticBenchmark`'s sections, their results |
| generated | `coverage.json`, `docs/sql-varka.md`, `census.tsv`, `emitted_bytes.json` |

**Size: medium, against the row's small**, in four pull requests: the baseline half a day, 95a a
day, 95b two to three days, 95c a day after 481.

## 6. What it unlocks in the census

`*` and `try_multiply` gain `int` in every mode (`m8/census.tsv`: `*` is fused for `int` under
LEGACY only, `try_multiply` for `byte` and `short`), and `%`, `mod` and `try_mod` gain `int` for a
literal divisor, which the census counts as a form of the pair, not the pair, until row 30's
column divisor lands. The `rows` column of `try_multiply` gains 95, and of `try_mod` too under 2.1.

## 7. Out of this row, and declines that stay

Out: a column divisor and `%` over other types (30), `pmod` (30), `BIGINT` multiply (104),
narrow multiply (481), decimal (349), floating `%` (346).

Kept for later, not built: the double-lane overflow test (2.2), which needs a mask crossing from
double to int lanes; the magic-number remainder (2.4), more multiplies than the quotient form at
vector width; a remainder node that ranges its result (2.4's last paragraph), which row 30's
`IntDivide` can carry.

Declines that stay: `i % 0`; a checked multiply at a width with no long species; under ANSI a
batch with an overflowing lane, by design.

## 8. Questions for the owner

1. **Who builds the int32 checked multiply** (item 0.12). (a) Row 95, as 2.1 proposes; `VARKA-
   104.md` 3.3 and 8 step 5 and `VARKA-481.md` 3.3 and 8 get pointers. (b) Row 104, as the record
   says, with 2.2's two forms handed to it; row 95's multiply half closes by pointer. *Recommended:
   (a)*: the row's text names the shape, both forms need nothing of 104's lattice or its long
   multiply options, and 95b is unblocked today where 104's step 5 waits on its own steps 2 to 4.
2. **Who builds `i % k`** (item 0.12). (a) Row 95, 2.4's trees, landing first; row 30's 2.4 keeps
   the column divisor, `pmod` and the other types, and 30a reuses this arm for an `INT` literal.
   (b) Row 30, as its 2.4 and 7 say; row 95's `%` half closes on "the record says why not", naming
   30, and its baseline's `%` shapes go to 30's class. *Recommended: (a)*: a literal divisor needs
   none of 30's node, refusal or floating division, and 30a waits on five rows.
3. **The default form of the checked multiply** (2.2), after 95c's measurement: `HALVES` or
   `LONG_TREE`, the other kept behind `checkedIntMultiply`. Not answerable before the numbers;
   prediction 2 leans to `HALVES`.

## 9. Done when

Row 95's text holds, with section 0's corrections: under the owner's answers to 8.1 and 8.2, an
`INT` multiply fuses under ANSI and TRY over a column and a literal alike, with overflow declining
the batch under ANSI and nulling the lane under TRY, and `i % k` fuses in the three modes for every
non-zero `INT` literal; tests 1 to 3 pass at every width under both arms,
`int_checked_multiply.smt2` proves 3.4's claims under both solvers, every planted fault of 3.5 is
caught, the measurement is committed at both widths on the EPYC 7763 with its predictions scored and
the default chosen, the bytes oracle is unchanged for every existing shape, and the PR build and the
option matrix pass. Or, for a half the owner gives to another row, the record names that row and
why.

## 10. Outcome

(Written when the work lands.)
