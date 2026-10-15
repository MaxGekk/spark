# Reading for milestone 8: what Google Highway offers the math, integer and cast rows

Read on 15 October 2026 at the owner's request. Paths are relative to `/home/max/proj/highway`
at commit `e8f6dfa` (15 October 2026). Not repeated from `READING_PYTORCH.md`: tier names,
`fexp_u20`, PyTorch's tolerance test. **[V]** means the author read the code; **[I]** means
inferred. Nothing was built or run; no number below was measured here.

## 1. How Highway states and checks a math bound

* **The stated bound is a bit-pattern distance [V].** `hwy/tests/test_util.h` line 141,
  `ComputeUlpDelta`: equal values (so `-0` against `+0`, and infinities) give 0, two NaNs give
  0, else `max(bits) - min(bits)` as unsigned. Opposite signs give a huge count, so no tiny
  pair passes by straddling zero. It is our step count except on that one point: ours is the
  ordered distance, so a `-1e-320` against `+1e-320` is small for us and huge for Highway.
* **The test (`contrib/math/math_test-inl.h` `TestMath`) [V].** It strides over the bit
  patterns of a range (4000 samples per sign, `AdjustedReps`), puts one value in every lane
  with `Set`, reads lane 0, and compares with libm (`std::exp` etc.). It prints and asserts
  the maximum. `HWY_MATH_SWEEP_ALL` (off) sweeps every `float` pattern, `double` gets 1e8
  samples. The reference is libm, not a correctly rounded value, as ours is the row engine.
* **Signed zero is tested apart, because the comparator hides it [V].** `math_test.cc`
  `TestSignedZero` compares bit patterns: the comment says the ULP comparator "treats +0 ==
  -0". It is the same finding as 2.7.1's `-0.0` clause.
* **One number for every target and width [V].** The suite runs per supported target
  (`HWY_EXPORT_AND_TEST_P`) and per partial vector length (`ForPartialVectors`), but each
  function has one threshold, loosened for a target in a comment ("NEON is 3 instead of 2",
  `Log1p`; `ACosh32ULP()` is 8 on MinGW). Varka keys the bound by host, so ours is finer.
* **The header bound is not always the tested bound, nor the tested range the stated one [V].**
  `Log` states 4 (`math-inl.h` line 2346 or so) and is tested at 1 (`math_test.cc`); `Pow` states 5
  and is tested at 8 (`kMaxErrorUlp`) over bases 2^-3 to 2^4 and exponents 2^-4 to 2^5 only;
  `Exp` states a range to +706 for `double` and is tested only to +104; `sin`, `cos` and `tan`
  state and test `[-39000, 39000]` only. Spark's `sin(1e300)` needs full-range reduction, so no
  Highway number covers it.
* **Stated bounds, `float` and `double` alike unless noted [V], header lines 1758 to 3066.**
  1: `exp`. 2: `acos`, `asin`, `log10`, `log1p`, `log2`, `exp2`. 3: `acosh`, `asinh`,
  `atan`, `atanh`, `sin`, `cos`. 4: `log`, `expm1`, `sinh`, `cosh`, `tanh`, `hypot`, `tan`
  (`double` 3). 5: `pow`. 6: `cbrt`. `atan2` states none and is tested by a table with an
  absolute tolerance of 1e-5 (`math_tan_test.cc` lines 138 to 184): a second foil beside
  PyTorch's for 345.
* **A relative-error family is kept in its own file [V].** `fast_math-inl.h` states, for
  example, `FastExp` 0.0007% for `double`: about 3e10 steps [I, 7e-6 / 2.2e-16].

## 2. Where Highway agrees or differs on Java semantics

1. **`pow` is C99's [V].** `Pow` forces the exponent to 0 when `|a| == 1` (`math-inl.h`
   lines 2508 to 2517), so `pow(1, NaN)` and `pow(-1, inf)` are 1. Java gives NaN: the two
   cases in 358's exact table. Highway's `pow` has no weak branch to guard, since it takes
   `log2` in extra precision (`ExtPrecLog2ForPow`), nor any of fdlibm's exact exponents [I].
2. **Float to integer pins down overflow, not NaN [V].** `quick_reference.md` line 2098:
   `ConvertTo` "returns the closest representable value if the input exceeds the destination
   range"; `ConvertInRangeTo` is implementation-defined. `x86_128-inl.h` line 13684 builds
   `ConvertTo` for `f32` to `i32` as one compare `v >= 2147483648.0f`, a blend with the
   maximum, over `cvttps2dq`: negative overflow needs no fix because the hardware answer is
   already the minimum [I, from the comment at line 5170 that out-of-range gives
   `LimitsMin`]. Unsigned targets clamp negatives to 0 (`ZeroIfNegative`). The test
   (`convert_test.cc` lines 1121 to 1143) ORs an all-ones mask over NaN lanes on both
   sides, so a NaN result is explicitly unspecified. Java's `(int)NaN` is 0: a NaN compare
   and a zero blend is the part Varka adds, and it is not in Highway [I].
3. **Narrowing is saturating or wrapping, by name [V].** `DemoteTo` saturates integers
   (`quick_reference.md` line 2157); `TruncateTo` keeps the low bits (line 2145). The choice
   for TINYINT and SMALLINT casts is therefore two different calls, never one.
4. **`BIGINT` to `FLOAT` is round-to-odd on AVX2, and Highway has it [V].** `x86_128-inl.h`
   lines 12822 to 12857 (signed; the unsigned form follows): split into the high 52 bits and
   the low 12, convert both to `double` (exact), `sum = hi + lo`, `carry = (hi - sum) + lo` is
   the exact rounding error (a two-sum), a sticky bit is set in `sum`'s lowest bit when the
   error is non-zero, and a `double` to `float` demote rounds once. About 20 vector ops.
   AVX-512 uses `vcvtqq2ps`. Its test (`demote_test.cc` lines 324 to 372) is 1000 random 64-bit
   patterns plus a few fixed values against `static_cast<float>`; random patterns almost never
   land on a tie, so the tie and sticky cases are untested there [I]; 343's proof is stronger.
5. **Integer divide by zero is left open [V].** `IntDivUsingFloatDiv` leaves `b == 0`
   "implementation-defined" (`generic_ops-inl.h` line 5360): Spark's null or error is a
   mask on `b == 0` first.

## 3. What is worth taking

1. **Integer multiply without a native instruction, costed [V].** 64-bit low multiply on
   targets without `mullo_epi64` (`generic_ops-inl.h` lines 4930 to 4970): three 32x32
   `MulEven` (`vpmuludq`), two shifts, adds, no high-high product. Signed is the unsigned one.
   Unsigned 64x64 to 128 (`SSE2Mul128`, `x86_128-inl.h` 11839 to 11862, Hacker's Delight
   `muldwu`): four `MulEven`, about five adds, four shifts, two masks. Signed high word:
   `mulH - (a >> 63 & b) - (b >> 63 & a)` (lines 11866 to 11878), the "sign correction" of
   361 in two ANDs and two subtracts. That is a second citation for 361 beside Arrow's order.
2. **8-bit multiply is promote, 16-bit multiply, truncate [V]** (`generic_ops-inl.h` 4890 to
   4925): a half-width vector promotes whole; a full one promotes both halves, two
   `mullo_epi16`, `ConcatEven`. Evidence for 329's native-byte arm: it is not one instruction on
   AVX2, so the widened-onto-int arm pays comparably, the same conclusion as PyTorch's.
3. **Divide by an invariant divisor is Granlund-Montgomery [V]**, `contrib/intdiv/
   intdiv-inl.h`: `m = 2^N (2^l - d) / d + 1`, `t = mulhi(m, a)`, `q = (t + ((a - t) >> sh1))
   >> sh2`; signed rounds toward zero. 64-bit lanes use a scalar loop on NEON and PPC (the
   file says the vector path is "not expected to outperform scalar"). Row 363's divisor is
   a constant `5^k`, so the setup (a 128 by 64 divide) is compile time for us; the file is
   the cross-check for the constants and for the signed correction. Its edge tests are
   `MIN / -1`, divisor 1, `2^16`, `2^32 - 1` (`intdiv_test.cc` 290 to 349), a small list for
   363's SPARK-45786 port.
4. **Vector divide is float divide, with an argument [V]** (`generic_ops-inl.h` 5156 to
   5420): `i8`/`i16` through `float`, `i32` through `double`, valid because the float has at
   least `8 * size + 1` mantissa bits, so truncation is exact; `i64` needs two reciprocal
   steps. The comment's proof is the model for a TINYINT or SMALLINT `/` and `%` row, if one
   is planned [I, no such row seen].
5. **128-bit pair layout and compares [V].** `quick_reference.md` 1782 to 1812: `Lt128`,
   `Eq128` on adjacent 64-bit lanes (low at the even index), the mask true in both lanes of a
   pair. `Lt128Vec` (`x86_128-inl.h` 15575) is `Eq`, `Lt`, one lane shift, a blend and an
   interleave: about five ops, unsigned only. 359's signed high word needs a signed compare
   on the high lane and an unsigned one on the low; Highway supplies neither signed form.
6. **IFMA as an alternative limb [V].** `MulAdd52Lo/Hi` (`generic_ops-inl.h` 40 to 90;
   `x86_avx3-inl.h` 52) multiply 52-bit limbs with a fused add. A 128-bit operand is three
   limbs, so a full product is nine products times two halves: no saving over 362's sixteen
   `vpmuludq`, and AVX-512 IFMA only [I]. Worth one line under 362's A2 measurement.
7. **Mask facts for 331 [V].** Masks are opaque; on targets without mask registers a true lane
   is all ones, and `MaskFromVec` of any other lane value is implementation-defined
   (`quick_reference.md` 1443, 1619). So a boolean from a bit vector goes through `LoadMaskBits`, not
   a data lane [I]: 331's "arbitrary data bits under null slots" test fits that contract.

## 4. What is not worth taking

* **The `Fast*` kernels:** relative errors of 1e-5 are not a count of steps we could state.
* **Per-target dispatch** (`HWY_EXPORT`, a function-pointer table filled at first call): C2
  does this per vector species already.
* **The polynomial coefficients:** tuned to a libm reference; ours is fdlibm or an Intel stub.

Nothing in this note edits a row. Section 1 feeds 345 and 367; section 2 items 1, 2 and 4
feed 358 and 343; section 3 items 1, 3, 5 and 6 are citations for 361, 363, 359 and 362.
Not read: the NEON and SVE integer paths and `f16_math`.
