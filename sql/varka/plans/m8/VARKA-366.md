# VARKA-366: The exact decimal-to-floating arm

## 0. What exists now (admission check)

From master at `6e207c226fb` (30 October 2026; paths as `VARKA-350.md` 0 gives them), the JDK 25
sources in `/home/max/proj/openjdk-build/jdk25`, and the census files. The row's text is the
milestone plan's 2.2 (owner's decision A4), its section 9 entry on round-to-odd, and wave F3 ("366
under 350's three double-rule casts and 364's two-pass arm"). The numbers marked **[A]** are from
`results/VARKA-350-admission.txt`, shared with `VARKA-350.md`; nothing else was run.

**Spark's behaviour per (from, to, mode), within precision 18.** This row computes one value for
two pairs and for three more by composition:

| from | to | LEGACY | ANSI | TRY |
|---|---|---|---|---|
| `DECIMAL(p, s)` | `DOUBLE` | `BigDecimal.doubleValue` of `u * 10^-s` | the same | the same |
| `DECIMAL(p, s)` | `FLOAT` | `BigDecimal.floatValue`, rounded once | the same | the same |
| `DECIMAL(17 or 18, s)` | `INT`, `SMALLINT`, `TINYINT` | not this row (exact) | the double rule over this row's `DOUBLE` value (`VARKA-350.md` 2.3) | the same, null where ANSI raises |

1. **The two casts never fail.** `Decimal.toDouble` and `toFloat` are `toBigDecimal.doubleValue`
   and `floatValue` (`sql/api/.../types/Decimal.scala:245-247`), generated as `$c.toDouble()` and
   `$c.toFloat()` in every mode (`expressions/Cast.scala:2656-2657`, `:2693-2694`); the
   interpreted path goes through `DecimalIsFractional.toDouble` (`Decimal.scala:742`), the same
   call. No error class applies; TRY is LEGACY's answer. The backing does not matter: a
   long-backed value's `toBigDecimal` is `BigDecimal(longVal, scale)` (`Decimal.scala:193-199`)
   and a `BigDecimal`-backed one's is its own, the same exact value either way.
2. **JDK 25 rounds both correctly from the exact value.** `BigDecimal.doubleValue`
   (`src/java.base/share/classes/java/math/BigDecimal.java:3825-3862`) returns `(double)
   intCompact` at scale 0, `v / DOUBLE_10_POW[scale]` when `(long) v == intCompact` and `0 <
   scale <= 22` (one rounding of exact operands; its comment argues `Long.MAX_VALUE` comes out
   right too), and otherwise `fullDoubleValue`, an exact quotient with a round and a sticky bit
   (`:3864` on). `floatValue` (`:3742-3764`) is the same with `FLOAT_10_POW` up to `1e10`
   (`:4093-4096`) and `fullFloatValue` (`:3766-3805`), which rounds once from the exact value, not
   through a double. So the target is `RN64(u * 10^-s)` and `RN32(u * 10^-s)`.
3. **The domain below precision 19.** `abs(u) <= 10^18 - 1 < 2^60` by 347's load check, `0 <= s <=
   18`, so a nonzero value lies in `[10^-18, 10^18)`: no subnormal and no overflow in either
   format, no `Long.MAX_VALUE`, and `u = 0` gives `+0.0` (a `BigDecimal` zero has signum 0). An
   exact midpoint between two neighbours of the target exists only at scales 0 to 2 for binary64
   and 0 to 15 for binary32 **[A]** - an odd 54-bit (25-bit) significand times `5^s` must stay
   below `10^18` - so those are the tie windows.
4. **Where row 350 needs this row [A].** By `VARKA-350.md` 2.4, the quotient is correctly rounded on
   every lane for `DOUBLE` at `p <= 15` and the float quotient for `FLOAT` at `p <= 7`, and `FLOAT`
   between 8 and 15 is round-to-odd over the double quotient, so this arm is needed only at `p` 16,
   17 and 18 for both targets, and for the three narrow casts' double rule at 17 and 18. The guard
   `abs(u) <= 2^53` passes on 0.0090 of a uniform `DECIMAL(18, s)` lane, 0.0901 at 17 and 0.9007
   at 16.
5. **What the algorithm needs from the JDK.** A per-lane leading-zero count:
   `VectorOperators.LEADING_ZEROS_COUNT` on longs is matched with `UseAVX >= 2`
   (`src/hotspot/cpu/x86/x86.ad:1905-1909`), as `evplzcntq` with AVX512CD (`:2077-2080`) and as
   a macro sequence on AVX2 (`cpu/x86/c2_MacroAssembler_x86.cpp:6284`); the census has no cell for
   it. A 64 by 64 to 128-bit product: no lane multiply-high exists, so it is built from 32-bit
   limbs, one `vpmuludq` each when both inputs are visibly 32 bits (the milestone plan's 2.8,
   HotSpot's `vectornode.cpp` 2189 to 2237); a constant multiplier's limbs are broadcasts written
   `broadcast(c).and(0xFFFFFFFF)`. Unsigned compares for the carries are `ULT` and its kin,
   intrinsic at 512 and 256 bits and the fallback at two long lanes on x86 (`VARKA-359.md` 0 items
   9 and 10). Variable shifts are `BitOp(SHL | USHR)` with a vector count (`varka/VarkaVectorIR.
   java:401`, `:953`), for which the census has no cell (`VARKA-343.md` 0 item 5).
6. **No IR node or lowering of this row exists**, and no unsigned compare (359 adds them); 343's
   `VarkaConversionLowering` is the precedent for a per-host lowering behind one node.
7. **The sources are not in the repository.** The milestone plan's 2.2 cites "Eisel and Lemire's
   algorithm" and "their Theorem 1, Corollary 2 and Lemma 3" with "no fallback while `s <= 27`",
   from the float-conversions reading of 14 October 2026; neither that reading nor the papers are
   committed (`sql/varka/papers/README.md`'s contents name neither; a search of `sql/varka/` for
   "Eisel" finds only the milestone plan). The theorem numbers below are the row's, unchecked.
   Open point 3.
8. **The working-precision round-to-odd** of the milestone plan's section 9 is "a sketch, sampled
   here at 200,000 values with no miss, unproven where `r + ul` must be exact and uncosted".
9. **The baseline** has no decimal-to-floating section (`VARKA-350.md` 0 item 17).

### What differs from the row text and the milestone plan

- **The arm runs at precision 16 to 18 only** (item 4): below 16 row 350's quotients are exact on
  every lane, so "taken per batch when the guard mask of 350 is not all true" applies at three
  precisions.
- **"Per batch" rarely helps a uniform column** (item 4): a batch takes the guarded quotient only
  if every lane passes, which at `p = 16` and 0.9007 a lane is almost never true over a batch of
  thousands of rows. So the arm on every lane is built beside the per-batch form and the
  measurement picks the default (2.4, open point 1).
- **The arm is one node with a lowering of its own** (2.3), not a composition of long-lane nodes;
  open point 2.
- **`FLOAT` may rest on binary64's machinery** if the paper's exactness claim covers binary64 only
  (2.2, open point 4).
- **The two-pass form above 18 is 364's composition** over this row's lowering, which exposes the
  entry 364 needs (2.5).

## 1. Goal and non-goal

`CAST(d AS DOUBLE)` and `CAST(d AS FLOAT)` over a `DECIMAL(p, s)` with `16 <= p <= 18` give
`BigDecimal.doubleValue` and `floatValue` bit for bit on every lane, with no fallback and no
decline, by Eisel and Lemire's algorithm with the scale a per-kernel constant; the same value feeds
350's double rule at precision 17 and 18. Not this row: the quotients below 16 and the round-to-odd
below 16 (350), the casts' composition and their tests in the three modes (350), precision above 18
and the two-pass form (364), `s >= 28` (364's decline), floating to decimal (346).

## 2. Design

### 2.1 The algorithm at a constant scale, binary64

For `x = u * 10^q` with `q = -s` a compile-time constant, per lane, over `w = abs(u)`:

1. `w = 0` gives `+0.0` (a blend at the end).
2. `l = LEADING_ZEROS_COUNT(w)`; `w' = w << l`, bit 63 set.
3. `T = (T_hi, T_lo)`, the 128-bit normalised approximation of `5^q` the paper specifies, two longs
   in the kernel's long table, computed at compile time from `BigInteger` and checked against the
   paper's table rows by a unit test for `s = 0..27`.
4. `z = w' * T_hi` as 128 bits: row 360's `WideMul` by the constant `T_hi` (four limb products by
   its broadcast limbs, summed with their carries); this row builds no product of its own.
5. The second product `w' * T_lo`'s high word, a second `WideMul`, added into `z.lo` with its
   carry into `z.hi`; the scalar algorithm takes it only when the low nine bits of `z.hi` are all
   ones, and the lanes compute it always and blend it in under that mask.
6. `b = z.hi >>> 63`; `m = z.hi >>> (b + 9)`, a variable shift; the binary exponent is `E_s + 63 -
   l + b`, with `E_s` the compile-time part from `floor(log2(5^q)) + q`.
7. The tie: where `z.lo == 0` and `(m & 3) == 1` at a scale where an exact midpoint exists, `m`
   is not rounded up; by item 0.3 that is scales 0 to 2, and scale 0 is 350's conversion alone
   (`VARKA-350.md` 2.4), so the step is emitted at scales 1 and 2 only.
   The algorithm's own tie window is read from the transcription and must contain them.
8. `m += m & 1; m >>>= 1`; where `m == 2^53`, `m = 2^52` and the exponent gains one.
9. The bits `(m & (2^52 - 1)) | (E + 1023) << 52`, the sign of `u` ORed in, reinterpreted as a
   double; no subnormal or infinity branch, by item 0.3.

Steps 4 to 8 follow the algorithm as its paper states it; the masks (nine bits, and 38 for
binary32), the shifts and the exponent constant written here are this plan's reading of the
algorithm, to be checked against the transcription at step 1 of section 5 before any code. That is
eight limb multiplies and about forty other lane operations, the count the milestone plan gives, to
be registered by `dev/varka_emit.sh` when built. The row's exactness claim - "exact for every
unscaled long with no fallback while `s <= 27`" - is the paper's, cited by its Theorem 1, Corollary
2 and Lemma 3 once step 1 of section 5 has transcribed it (open point 3); within precision 18 the
scale is at most 18, inside that window.

### 2.2 binary32

The same products and normalisation; the constants differ: the mantissa is `z.hi >>> (b + 38)`, the
second product is taken where the low 38 bits of `z.hi` are all ones, the tie step is emitted at
scales 1 to 15 (item 0.3, scale 0 being the conversion), and the bits assemble at 23 and 127.
Whether the paper's no-fallback theorem covers binary32 is read at transcription. If it covers
binary64 only, `FLOAT` takes a second form that rests on binary64's: the binary64 arm with step 8
replaced by round-to-odd at 53 bits - `m`'s last bit ORed with the sticky bit, which is "`z.lo`
nonzero or a dropped bit set" - then one `D2F`, correct by Boldo and Melquiond's Theorem 3 (53 bits
against 24 + 2), the argument 350's round-to-odd already uses. Open point 4.

### 2.3 One node, one lowering

`ExactToFloating(VarkaVectorIR child, int scale, LaneType target)`: a long-lane child holding the
unscaled value with `abs(u) <= 10^18 - 1`, a floating-lane result, `target` `DOUBLE` or `FLOAT`,
rendering `etf:<scale>:<target>(<child>)` so that each scale is its own class (the table constants
are in the code). Its lowering lives in `varka/VarkaExactFloatLowering.java`, beside 343's
`VarkaConversionLowering`, and emits the steps of 2.1 or 2.2 with the table loaded once in the
prologue. The reference evaluator computes `BigDecimal.valueOf(u, scale).doubleValue()` (or
`floatValue()`), Spark's own call, so the oracle is not the algorithm. A scalar Java twin of the
lowering, `ExactToFloatingScalar` in the test tree, on `Math.unsignedMultiplyHigh` and
`Long.numberOfLeadingZeros`, runs the same steps for the exhaustion of 3.2 and is the scalar
reference arm.

A composition from long-lane nodes - `BitOp`, `IntArith`, 359's unsigned `Compare`s, a new
leading-zero node and a reinterpretation node - was considered, on 359's rule that a pair is words,
not a node. It is not taken here because the arm is one proof unit, its correctness is the paper's
theorem over the whole sequence and no intermediate is a SQL value, and a composition would put
about fifty nodes into the budget, the cost model and the fuzzer's grammar for one cast; but it is
the lead's to weigh (open point 2).

### 2.4 Which lanes take it: `decimalToFloatingArm`

A `Choice` option (`Reason.MACHINE`), read only at `p >= 16`, both arms built and the loser kept:

- **`EXACT`**: the node on every lane; one body, no guard. 350's `DOUBLE` cast at `p >= 16` is
  `ExactToFloating(u, s, DOUBLE)`.
- **`GUARDED`**, the row's form: a guard `abs(u) <= 2^53` is collected over the batch, as a decline
  status is; a batch whose guard holds on every valid lane is served by 350's quotient (or its
  round-to-odd for `FLOAT`), and a batch where it fails by the `EXACT` tree. How the batch is
  switched is open point 1.

The default is the measurement's (section 4). Both serve the double rule of 350's three narrow casts
at precision 17 and 18 unchanged, since that tree takes the `DOUBLE` value from 350's `toDouble`.

### 2.5 What 364 takes from it

364 runs the arm twice on the top 64 bits of a 128-bit magnitude, `w'` and `w' + 1`, with the shift
that took them as an exponent offset, and accepts a lane where both give one value. So the lowering
has an entry taking an already normalised `w'` (bit 63 set, unsigned) and a per-lane exponent
offset, which this row calls with `l` from step 2 and an offset of zero; `w' + 1` overflowing 64
bits is 364's case to handle. `s >= 28` is outside the row's exactness window and declines in 364.

### 2.6 The working-precision round-to-odd, an option arm for `DOUBLE`

Section 9's form - `u = uh + ul`, `q = RN(uh / 10^s)`, `r = fma(-q, 10^s, uh)`, the answer `RN(q +
odd((r + ul) / 10^s))` - is built as a third value of `decimalToFloatingArm`, `WORKING_ODD`, only
after the claim it lacks, that `r + ul` is exact, is proven: first by a reduced-width twin
exhausted in scalar Java (the same construction from binary32 to binary16 over every 32-bit `u`, as
343's twins are), then stated with the argument. If the twin finds a miss, the arm is not built and
section 10 records the witness. It uses 350's `Fma` node and two divides.

### 2.7 At 128 bits, cost, warm-up

At two long lanes on x86 the unsigned compares and blends are the fallback; the rule is 347's
answered question 4, as `VARKA-350.md` 2.6 applies it, and the measurement reports it. The cost
model gains one feature for the node, on row 352's awaiting-refit list, and the budget weighs it by
its operation count until the fit. The warm-up's long block gains `LEADING_ZEROS_COUNT`, the
variable shifts and the masked-constant multiply, held by `VarkaWarmupReachSuite`.

## 3. Tests and proofs

1. **The paper's theorem, cited.** The exactness claim of 2.1 cites the transcription's theorem
   numbers and states its preconditions (the scale window, the width of `w`, the table's rounding)
   beside the code that relies on each.
2. **Exhaustion against the proven quotient.** For every `u` with `abs(u) < 2^32` and every scale
   1 to 18, the scalar twin and the emitted kernel give the bits of `(double) u / 10^s` - correctly
   rounded there by the JDK's own argument (item 0.2: exact operands, one rounding) - and for
   `FLOAT` the bits of 350's round-to-odd, correct by Boldo and Melquiond's theorem: 2^33 values per
   scale and target, opt-in (`-Dvarka.sweep=true`) in the gate's sweep step with a 2^24 slice in PR
   CI, the first sweep timed before the rest (milestone plan 10.3 item 5). The full sweeps run by
   hand in that step: no CI workflow sets `varka.sweep` today, so no nightly run is claimed.
3. **Sampled regions against `BigDecimal`**, for `p` 16 to 18 at every scale: a seeded draw per
   binade of `[2^53, 10^18)`; `+-2^53` and its neighbours; every exact midpoint of the tie windows
   (an odd 54-bit or 25-bit significand times `5^s` times a power of two) and the values one unit
   either side; Trino's vectors (`READING_TRINO_DECIMAL.md` 10 item 5: `9007199791611905`,
   `960000084148223999` at scale 2, `912057769880088000` at scale 3, `576460752303480000` at scale
   2, `83886085` and `83886095` at scale 1 to `FLOAT`, `300315360073` at scale 14), each at every
   scale 0 to 18; the second-product cases - values where the low nine (or 38) bits of `z.hi` are
   all ones - found by a search in the scalar twin and tested by name.
4. **No vacuous pass, for row 504.** The sampled suite counts the lanes that took the second
   product and the tie step and fails if either count is zero; under a test flag the lowering drops
   the second product, and the suite must fail with the flag on.
5. **The table.** A unit test recomputes `T` for `s = 0..27` from `BigInteger` and compares it with
   the transcription's table rows.
6. **Coverage rows** in 350's "DECIMAL casts" family: `CAST(d16s9 AS DOUBLE)`, `CAST(d18 AS
   DOUBLE)`, `CAST(d18 AS FLOAT)`, `CAST(d18s14 AS DOUBLE)` and `d18 < x` over a double column
   (coercion's cast), and, after 350c, `CAST(d18s14 AS INT)` under ANSI and TRY, under each value of
   `decimalToFloatingArm`; each names `differential` or the sampled suite for row 490's check.
7. **The fuzzers**: the IR grammar draws `ExactToFloating` at scales 0 to 18 and both targets in
   the decimal sequence; the Spark-level fuzzer's decimal columns at precision 16 to 18 cast to both
   types.
8. **The bytes oracle**: `emitted_bytes.json` adds rows and moves no existing shape.

## 4. The baseline, and predictions

**The baseline is 350's addition** (`VARKA-350.md` 4), one pull request on the unchanged code:
`CAST(x AS DOUBLE)` and `CAST(x AS FLOAT)` at `(9, 2)`, `(18, 4)` and `(18, 14)`, and one more
column for this row at `(18, 4)` with integer parts below `9 * 10^11`, so that every unscaled value
lies below `2^53` and `GUARDED` takes the quotient on every batch, timed for both targets.
`benchmark.yml` on the EPYC 7763 at 256 and 128 bits.

Predictions, registered before the build:

1. **`EXACT` against the row path at 256 bits on the EPYC 7763**, at `(18, 4)` and `(18, 14)`: 4 to
   10 times faster; the row path builds a `BigDecimal` per value and divides in `fullDoubleValue`
   wherever the fast path fails.
2. **`EXACT` against 350's quotient at `(9, 2)`**, per row: 1.5 to 3 times slower at 256 bits on
   both hosts; this is the done-when's "cost measured against the guarded division".
3. **`GUARDED` against `EXACT`** on the uniform sections: slower by up to its quotient's cost,
   since nearly every batch fails the guard (item 0.4) and pays both; on the below-`2^53` section,
   faster by the difference of prediction 2. The default goes to `EXACT` unless a measured column
   type the census counts (`m9/SCOPE.md` 1.1's corpus) shows small values dominate.
4. **binary32 costs the same as binary64** within a tenth: the products and shifts are the same
   count.
5. **At 128 bits on x86** the arm is the fallback (two long lanes); it still beats the row path by
   2 to 4 times, an assumption with no measurement behind it, to be read beside 359's spike.
6. **No existing shape's bytes move**; the `option_arms` digests move with `decimalToFloatingArm`.

## 5. Order, files and size

1. **The papers** into `sql/varka/papers/` as its README prescribes: Lemire's "Number Parsing at a
   Gigabyte per Second" and, if the theorem numbers are there, Mushtak and Lemire's "Fast Number
   Parsing Without Fallback" (both in Software: Practice and Experience, venues to be confirmed from
   the deposits), from the open-access deposits by `pdftotext -bbox-layout`, each file's header
   naming this row; if a deposit's terms forbid a copy, the reading notes and the citation instead.
   Nothing is downloaded before the owner's yes (open point 3); the theorem numbers of 2.1 are then
   quoted from the transcription.
2. **The census cells**: `long.LEADING_ZEROS_COUNT`, `long.LSHL` and `long.LSHR` by a vector
   count, `long.MUL` of masked inputs, on the laptop and through `varka-width-audit.yml`.
3. **The scalar twin and its harness** (3.2, 3.3, 3.5), test side only, before any emitter code:
   it validates the constants, finds the second-product cases and times the exhaustion.
4. **366a, the node**: `ExactToFloating`, its lowering, the reference arm, both targets, the
   `EXACT` arm at `p >= 16` wired into 350's `toDouble` and `toFloat`, tests 3.2 to 3.8.
5. **366b, the per-batch form and the measurement**: `GUARDED` by open point 1's answer, the
   measurement against the baseline at both widths and 512 bits on the laptop, scored, the default
   set; then `WORKING_ODD` after its twin (2.6).

| file | what |
|---|---|
| `varka/VarkaVectorIR.java` | `ExactToFloating`, its canonical form |
| `varka/VarkaExactFloatLowering.java` (new), `varka/VarkaVectorWalk.java` | the lowering of 2.1, 2.2 |
| `varka/VarkaBodyEmitter.java`, `varka/Analysis.java` | the per-batch switch of open point 1 |
| `varka/VarkaEmitOption.java`, `varka/VarkaEmitOptions.java` | `decimalToFloatingArm` |
| `VarkaDecimalCastCompiler.java` | 350's `toDouble` and `toFloat` at `p >= 16` |
| `VarkaReference`, `VarkaCanonicalIr` | the reference arm, the parse arm |
| `varka/VarkaEmitBudget.java`, the cost register, the warm-up | weight, feature, reach |
| `sql/varka/papers/` | step 1's transcriptions and README rows |
| tests | the scalar twin, the exhaustion, the sampled suite, the table test, coverage rows |
| benchmark | 350's baseline sections and this row's below-`2^53` section, results files |
| generated | `coverage.json`, `docs/sql-varka.md`, `width_audit.json`, `emitted_bytes.json` |

**Size: medium as the row says, at its upper end, in two pull requests after the baseline.** The
papers half a day; the cells half a day; the twin and its harness a day and a half; 366a three days;
366b two. Test time: the exhaustion is 2^33 values per scale and target, of the order of a minute
per scale in the kernel and opt-in; the sampled suite tens of thousands of `BigDecimal` calls a
scale, seconds. It rests on 347 (the load), 360's `WideMul` (both products), 350b (`toDouble`,
`toFloat` and their below-16 forms), 343 (`ConvertLane(FLOAT)` and the reinterpretation), 359's
unsigned compares if 359 lands first (else this row adds them and 359 reuses), and 157 for the
outputs.

## 6. What it unlocks in the census

Nothing on its own: `cast` and `try_cast` over a decimal count with 364 at every precision
(`VARKA-350.md` 6). It closes the last decline of 350's decimal-to-floating pairs and of the three
narrow ANSI and TRY casts at precision 17 and 18 below 19, so `casts_fused.tsv`'s `DECIMAL(10, 2)`
forms need nothing more from it; the `fused_forms` column's precision-16-to-18 claim rests on this
row.

## 7. Out of this row, and declines that stay

Out: the quotients and round-to-odd below precision 16 and the casts' composition (350); the
two-pass form above 18 and its `s >= 28` decline (364); Trino's `FLOAT` midpoint-margin test, which
would keep more lanes on the quotient but needs its own bound proven (`READING_TRINO_DECIMAL.md` 7);
Schubfach and the shortest-digits path, which are 346's direction (`m9/SCOPE.md` item 97).

Declines that stay: none at precision 16 to 18 under `EXACT`; under `GUARDED` none either, a failed
guard switching arms rather than declining; a decimal in an int-driven kernel (347's leaf rule).

## 8. Questions for the owner

None of the owner's yet; step 1's download waits for the owner's yes (open point 3). The points
below are the lead's to settle or to put to the owner.

### Open points for the lead

1. **How `GUARDED` switches a batch.** (a) In the class: the emitted `run` calls the guarded body
   and, on a status bit of its own that never leaves the class, calls the exact body over the same
   batch, which overwrites every output. (b) In the runner: the bit leaves the kernel and
   `VarkaKernelRunner` reruns the batch through a sibling kernel compiled with `EXACT`, a second
   shape in the cache. (c) Per lane group: `if (!guard.allTrue())` inside the loop runs the exact
   tree for that group only, a branch the body emitter has never emitted (its bodies are
   straight-line; `anyTrue` appears only in the status fold, `varka/VarkaBodyEmitter.java:645-661`).
   Evidence: item 0.4 and prediction 3 - a uniform column fails the guard on almost every batch, so
   (a) and (b) pay both bodies there, and (c) pays per group but adds an emission construct. Or
   (d): build only `EXACT`, record `GUARDED` as measured-and-not-built from the spike numbers of
   step 3. The row's text says "per batch", which (a) and (b) are.
2. **One node or a composition** (2.3). (a) `ExactToFloating` with its own lowering. (b) Long-lane
   nodes plus a leading-zero node and a reinterpretation node, as 359 composes the pair. Evidence:
   2.3's reasons; 359's rule is stated for a value's words, and this arm's intermediates are not
   values.
3. **The papers and the float-conversions reading.** The row cites theorem numbers from a reading
   that is not committed (item 0.7). (a) Transcribe the papers at step 1, after the owner's yes. (b)
   Commit the float-conversions reading's notes, if they still exist, and cite from them. (c) Both.
   Until one is done, the plan's exactness claim is the row's word.
4. **`FLOAT`'s exactness argument** (2.2). (a) The binary32 algorithm, if the theorem covers it.
   (b) The binary64 arm with a round-to-odd output and one `D2F`, by Boldo and Melquiond, whatever
   the theorem covers. Evidence: 2.2; (b) is one more shared proof and no second set of constants.
   Both are tested by 3.2 and 3.3.

## 9. Done when

Row 366's text holds, with the corrections above: at precision 16 to 18 the 64-bit form fuses every
value for `DOUBLE` and `FLOAT`, bit-identical to `BigDecimal.doubleValue` and `floatValue` over the
exhaustion of 3.2, the sampled regions and tie windows of 3.3 and the fuzzer, with the paper's
Theorem 1, Corollary 2 and Lemma 3 (as the transcription numbers them) cited for the exactness
claim; the vacuity counts and the planted fault of 3.4 hold; 350's three narrow casts at precision
17 and 18 take their double from it; its cost is measured against 350's quotient and the row path
at both widths, the default of `decimalToFloatingArm` set by that measurement and the losing arm
kept; no existing shape's bytes move; and the PR build and the option matrix pass.

## 10. Outcome

(Written when the work lands.)
