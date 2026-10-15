# Reading for milestone 8: what StarRocks, Velox and DuckDB offer for 128-bit decimals

Read on 15 October 2026 at the owner's request: do three C++ engines with 128-bit decimals have
anything for rows 347 and 359 to 364 of `m8/PLAN.md` (2.8)? Paths are relative to
`/home/max/proj/starrocks` at commit `a5dc21cf97d`, `/home/max/proj/velox` at `484ef8297` and
`/home/max/proj/duckdb` at `d397c964cc` (all 15 October 2026). **[V]** means the author read the
code; **[I]** means inferred, nothing was built or run.

## 1. What the engines are

All three keep a decimal of precision 19 to 38 as one native `__int128` per value and compute
row by row in scalar code.

* StarRocks: `Decimal128` is a column of `int128_t` (`column/runtime_type_traits.h`, `CppType =
  int128_t`) **[V]**. `exprs/decimal_binary_function.h` is one `for` loop per batch, calling
  `BinaryOperator::apply` on `lhs_data[i]`, `rhs_data[i]`; the loop body writes a per-row null
  and may `throw` **[V]**. A loop with a throw and a store to a null array is not something a
  compiler vectorises **[I]**; the arithmetic code names no SIMD (`-mavx2` appears only as a
  build flag) **[V]**. The 128-bit add, subtract and multiply are x86 inline asm (`adc`, `sbb`,
  `mul`) in `types/int128_arithmetics_x86_64.h` **[V]**.
* Velox: `functions/sparksql/DecimalArithmetic.cpp`, `DecimalMultiplyFunction::call` is a scalar
  per-row function; the type library `type/DecimalUtil.h` supplies the helpers **[V]**.
* DuckDB: `hugeint_t` is a struct `{ uint64_t lower; int64_t upper; }` with scalar operators
  (`common/types/hugeint.cpp`) **[V]**.

None has a split low-word and high-word layout, a vector kernel or a de-interleave **[V]**.
The three results below are therefore about arithmetic, not layout.

## 2. Worth taking

1. **A wide-path trigger that is one count per row** (Velox). Before a multiply whose result
   scale is cut, Velox adds the leading zeros of `|a|` and `|b|`: at 128 or fewer the product may
   pass 128 bits and the row takes the `int256_t` path; above, a plain `__int128` product is safe
   and is divided by `10^deltaScale` (`DecimalArithmetic.cpp` 421 to 455) **[V]**. The comment
   calls it "quick, but conservative". For rows 362 and 363 this is the shape of a batch guard:
   a lanewise count of significant bits (no vector `clz` on AVX2, a compare chain, or the
   `ZERO_EXTEND` and exponent trick, which 361 would have to measure **[I]**) gates the 256-bit
   column sum, so the common batch pays for 128 bits only. Row 362 does not have it today; its
   static shortcut `p1 + p2 + 1 <= 38` is the compile-time half of the same idea.
2. **The `p1 + p2 + 1 < 38` bound is one step weaker than 362's.** Velox skips the checks only
   when the result precision is below 38 (`rPrecision_ < 38`), and sends precision exactly 38
   through the checked path **[V]**. Row 362 uses `<= 38`. The plan's bound is the sharper one:
   the exact product of values below `10^p1` and `10^p2` is below `10^(p1+p2)`, so at `p1 + p2 =
   37` it is below `10^37`, which fits (the `+ 1` is Spark's result-type rule, not a bound on
   the product) **[I]**. Worth an SMT line in 362 stating which one the code uses.
3. **The output check is a table compare, two sides, per row** (Velox `valueInPrecisionRange`:
   `value < 10^p && value > -10^p`, with `kPowersOfTen` a 39-entry `int128_t` table) **[V]**.
   This is row 359's `10^p` check as the engines write it; there is no cheaper form in any of
   the three. In lanes it is two unsigned-high and low compares per side, with `10^p` a broadcast
   constant per query.
4. **DuckDB's rounding is already row 363's `(floor(n / 5^k) + 1) >> 1`.**
   `DecimalScaleDownOperator` (`function/cast/decimal_cast.cpp` 100) divides by `factor / 2`,
   adds or subtracts one by sign,
   then divides by 2, "so we don't overflow when rounding" **[V]**. With `factor = 10^k` this is
   `HALF_UP` away from zero, which is Spark's `ROUND_HALF_UP` **[I]**. It is independent
   confirmation that the pre-shift form needs no remainder; 363 already cites it.
5. **A fast narrow-times-narrow path that skips the check** (StarRocks). When both children of a
   multiply are casts that keep the scale of `DECIMAL32` or `DECIMAL64` columns and the result is
   `DECIMAL128`, `evaluate_decimal_fast_mul` strips the casts and runs `MulOp64x64_128` (or
   `32x64`, `32x32`) with `OverflowMode::IGNORE`: the product of two narrower values cannot
   overflow 128 bits (`exprs/arithmetic_expr.cpp` 90 to 140) **[V]**. This is row 361's case and
   the cast-elimination is worth a look in the emitter: widening both operands to the lane pair
   and then multiplying 128 by 128 would waste the shortcut; 360's widening casts should be
   removed under a multiply the way StarRocks removes them.
6. **Tests that compare the fast form with the plain one over random draws.**
   `test/types/int128_arithmetic_ops_test.cpp` checks `multi3` against `x * y` on random
   `int128_t` pairs (`compareMul`, line 104) **[V]**. Varka's proofs are stronger, but a random
   cross-check of the emitted kernel against `BigInteger` is the row-level test the proofs do not
   replace.

## 3. Not worth taking

* **The overflow style.** StarRocks templates the loop on `OverflowMode` (`IGNORE`,
  `OUTPUT_NULL`, `REPORT_ERROR`) and checks 128-bit machine overflow (`mul_overflow`,
  `add_overflow`), not `10^38`; I found no precision check in its arithmetic path, so a value
  between `10^38` and `2^127` is not flagged there **[V for the path read, not for the whole
  engine]**. Varka's `CheckOverflow` against `10^p` is the Spark rule and stays.
* **Multiply into 256 bits.** StarRocks's `int256_t` (`base/types/int256.cpp`) is a struct of
  `uint128_t low` and `int128_t high` with schoolbook products: 64 by 64, 128 by 128 (four `mul`),
  64 by 256 and a general case **[V]**. It serves `DECIMAL256`, which Spark lacks. DuckDB has no
  256-bit step and no Spark precision-loss rule: `BindDecimalMultiply` caps the width at 38,
  sets `check_overflow` and errors when the scale passes 38 **[V]**. Velox's 256-bit step is
  `boost::multiprecision::int256_t` with a library divide **[V]**.
* **Division by a power of ten.** Velox uses `in / divisor` and `in % divisor` on `int256_t`, then
  `abs(remainder) >= divisor >> 1` (`reduceScaleBy`) **[V]**. StarRocks `div_round` is `/`, `%`
  and a carry **[V]**. DuckDB's `Hugeint::DivModPositive` is a bit-serial loop of up to 127
  shift-and-compare steps per row **[V]**. None uses a magic multiply or reciprocal; the plan's
  statement that all engines use long division holds for these three. They give no cost to
  borrow, and no vector number, since each divides in scalar code.
* **Rescale up by multiply with a flag.** Velox `rescaleWithRoundUp` and StarRocks `scale_up` call
  `__builtin_mul_overflow` by `10^diff` and test the result against the target precision **[V]**.
  Row 360 needs the same arithmetic but in lanes; the plan's batch guard replaces the flag.

## 4. What this sharpens, and what it contradicts

* **Contradicts nothing in 2.8.** The interleaved sixteen-byte layout is the only one any engine
  has, and Arrow's, which is why 347 and 359 build the de-interleave; there is no prior measurement
  of a split layout to compare with.
* **A defect worth knowing, as evidence for proving carries.** In StarRocks `multiply_128x128`,
  `middle = p1 + p2` is a `uint128_t` sum of two products that can each reach
  `(2^64 - 1)^2`, so the sum can exceed `2^128` and wrap, losing a carry into `result_high`
  (`int256.cpp` 504 to 530) **[I]**, by arithmetic: for `a = b = 2^128 - 1`, `p1 + p2` is
  `2^129 - 2^66 + 2`. The function is reached for any pair with `high == 0` (`multiply_runtime`,
  line 646) **[V]**, so magnitudes in `[2^127, 2^128)` reach it. A decimal value stays below
  `2^127`, where the sum stays below `2^128`, so Spark decimals are probably not hit **[I]**.
  This was not run. It is another case for 361 and 362 proving the carry network and not
  sampling it: the dangerous region is one corner of the input space.
* **Row 362's guard** can borrow Velox's leading-zero test (section 2, item 1): add to the row
  as an option arm, measured against the unconditional column sum, if the owner wishes.
* **Rounding is not uniform across the engines.** StarRocks names a rule `ROUND_HALF_EVEN` and
  implements `abs_remainder >= divisor >> 1`, which is half up, with a `TODO` saying the two
  differ (`types/decimalv3.h`, `round_quotient`) **[V]**. For `divisor = 10^k` with `k >= 1` it
  is Spark's `HALF_UP` **[I]**, but the name and the code disagree, which is a reason to test
  the tie in both directions and to read the mode from the node, as the plan says.

Nothing here changes a row. Items 1, 2 and 5 of section 2 are additions to 362, 362 and 360 if
the owner wants them; the rest are citations. Not read: Velox's Spark `add`, `subtract` and
`divide` kernels beyond the multiply, StarRocks's cast kernels past the decimal-to-decimal path,
DuckDB's `Hugeint::TryMultiply` carry code, and every engine's SQL-level tests beyond a listing.
