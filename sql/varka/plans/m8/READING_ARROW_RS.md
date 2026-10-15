# Reading for milestone 8: what arrow-rs and Comet offer for decimals

Read on 15 October 2026 at the owner's request: does arrow-rs, and Comet's Spark layer on top of
it, have anything for rows 347, 359 to 364 and `casts.tsv`? Paths are relative to
`/home/max/proj/arrow-rs` at commit `dd24011` (15 October 2026) and
`/home/max/proj/datafusion-comet/native/spark-expr/src` at `8c229a703` (16 September 2026).
**[V]** means the author read the code; **[I]** means inferred. Nothing was built or run.
`READING_DECIMAL_ENGINES.md` (C++ engines) and `READING_RAPIDS.md` are not repeated.

## 1. How arrow-rs builds it

* **`i256` is `{ low: u128, high: i128 }`** (`arrow-buffer/src/bigint/mod.rs`) **[V]**.
  `wrapping_mul` is `mulx(a.low, b.low)` (an unsigned 128 by 128 to 256 from four 64 by 64
  products with `u128` carries, written as a portable x86 `mulx`) plus the two cross terms
  `a.high * b.low` and `a.low * b.high` wrapped into the high word **[V]**: two's complement,
  no `abs`, the sign a correction on the high 128 bits **[I]**, the shape of row 361's
  `unsignedMultiplyHigh` correction. `checked_mul` takes `abs`, multiplies, negates back with a
  borrow, and rejects when both highs are non-zero **[V]**.
* **Division is generic.** `div_rem` takes `abs` of both sides, runs native `u128 / u128` when
  both highs are zero, else Knuth D on four `u64` limbs (`div.rs`); a divisor of 64 bits or less
  goes through `div_rem_small`, four steps of the x86 `div` (inline asm) **[V]**. No special
  case for a power of ten, no reciprocal. Nothing to borrow for 363's magic divide.
* **Rounding is single, HALF_UP, on the exact value.** `multiply_fixed_point` forms the exact
  `i256` product, then `divide_and_round(10^k)`: `wrapping_div_rem`, `half = div / 2`, add one
  away from zero when `r >= half` (or `r <= -half`) **[V]**. The cast path (`make_downscaler`)
  does the same on the source type. Both are the pre-SPARK-45786 *fixed* behaviour, not the
  rapids double rounding **[I]**: there is no interim cut to 38 digits.
* **Overflow is two-level, and the second is not in the arithmetic.** `mul_checked` is machine
  overflow of `i128` (error text "Overflow happened on: a * b"); `with_precision_and_scale` only
  tags the type and checks precision and scale, **not the values** **[V]**. The `10^p` check is
  a separate pass: `is_validate_decimal_precision` compares against two 39-entry `i128` tables
  `MAX_/MIN_DECIMAL128_FOR_EACH_PRECISION` (`10^p - 1` and `-(10^p - 1)`) per value
  (`arrow-data/src/decimal.rs` 692 to 1048) **[V]**. Null mode is `null_if_overflow_precision`
  (`unary_opt`); error mode is `validate_decimal_precision` **[V]**.
* **Upscale takes `10^k` from the same table** (`MAX_FOR_EACH_PRECISION[k] + 1`, no `pow`); a
  scale gap past the table is an upscale overflow, and a downscale gap past it returns zeros
  (`make_upscaler`, `make_downscaler`) **[V]**.
* **Static shortcuts exist for casts.** A rescale is "infallible", skipping the check, when
  `p_in + delta <= p_out` (up) or `p_in - delta < p_out` (down; the `<` leaves room for the
  rounding carry into a new digit, as in the code comment's `(5, 3)` to `(3, 0)`) **[V]**.
  Row 360's batch guard can use both.
* **Decimal arithmetic itself is not Spark's.** Division truncates and adds four fixed scale
  digits (Hive/Postgres rule); `mul` errors when the result scale passes 38; none is
  `allowPrecisionLoss` **[V]** (`arrow-arith/src/numeric.rs` 1043 to 1160).

## 2. What Comet adds, and why

* **`WideDecimalBinaryExpr`** (`math_funcs/wide_decimal_binary_expr.rs`): add, subtract,
  multiply of `Decimal128` in one pass: widen to `i256`, scale up or **single** HALF_UP divide
  (`div_round_half_up`: `|2r| >= |d|`), compare against `+-(10^p_out - 1)` as `i256`, then
  narrow **[V]**. Its bench compares it with the old chain
  `Cast(to Decimal256), BinaryExpr, Cast(back)`, i.e. the arrow-rs-only route **[V]**; why the
  chain lost is not stated and no number was read.
* **When the wide path is used (planner.rs 1066 to 1096).** Multiply when `p1 + p2 >= 38`;
  add and subtract when `max(s) + max(p - s) >= 38`; otherwise plain arrow `i128` arithmetic
  then `CheckOverflow` **[V]**. That is exactly row 362's `p1 + p2 + 1 <= 38` and 360's
  "uncapped shapes", chosen by Comet independently of Velox's one-weaker bound **[I]**.
* **The result type comes from the node, never the session.** `DecimalPrecision.promote`
  wraps every decimal `+ - * / %` in `CheckOverflow(expr.dataType)`; its comment records issue
  #4124: recomputing from the live `SQLConf` re-labelled a stored `DEC(38, 17)` as `(38, 18)` and
  shifted values by 10x on Spark 4.1+ (SPARK-53968 keeps `allowPrecisionLoss` per expression)
  (`DecimalPrecision.scala` 25 to 35) **[V]**. This is row 363's "never the session
  configuration" rule, found in the field.
* **Null versus error.** Legacy and Try give null, Ansi raises; the closure cannot emit null, so
  Legacy returns the sentinel `i128::MAX` and a second pass `null_if_overflow_precision`
  nulls it **[V]**. `CheckOverflow` first scans `all(valid)` and reuses the buffers when
  nothing overflows, rescanning for the first offender only on the error path **[V]**.
  In Ansi the wide expression raises a generic "Arithmetic overflow" and the planner then skips
  the outer `CheckOverflow` (planner.rs 665), so Spark's error class may not be the one
  raised **[I]**; the Varka raise names a value and a precision, as `checkoverflow.rs` does.
* **Casts where it distrusts arrow-rs [V].** Decimal to `DOUBLE`/`FLOAT`: arrow computes
  `(unscaled as f64) / 10^scale`, two roundings, wrong by one ulp past `2^53` (`12345.6789`
  becomes `12345.678899999999`); Comet copies `BigDecimal.doubleValue`: one division when
  `|u| <= 2^53` and `scale <= 22` (`F64_EXACT_POW10`, `<= 2^24` and `<= 10` for float), else the
  exact string `"<u>e<-scale>"` through the correctly rounded parser (`numeric.rs` 983 to 1046).
  FLOAT is never narrowed from the double (`16777217.0000000001`). This is `casts.tsv`'s
  `dec -> double` guard and 366's exact arm, confirmed on a second implementation.
  Double to decimal: arrow does `(f * 10^s).round()`; Comet uses it only when
  `|x - round(x)| + |x| * 2^-48 < 0.5`, else Ryu shortest digits then integer HALF_UP
  (`0.5153125` is the counter-example) **[V]** - row 346's "lanewise guard from the rounding
  interval, exact path for the rest" with a concrete bound. Decimal to `TINYINT`/`SMALLINT`
  goes through `INT` first (`(value / 10^s) as i32 as i8`) **[V]**; decimal to `BOOLEAN` tests
  the raw unscaled value (`0.01` is true), bypassing the arrow kernel **[V]**.
* **Division**: `BigInt` once `p + l_exp > 38`; "truncate one digit past the target, add `+-5`,
  divide by 10" is HALF_UP in one step **[V]**. Out of Varka's scope.

## 3. Worth taking

1. **Check the whole wide value, not its low 128 bits.** arrow's `multiply_fixed_point`
   (unchecked) narrows with `as_i128()`; its test `..._allow_precision_loss_overflow` shows a
   product near `10^61` wrapping to `62946009661555981610246871926660136960`, a value that
   *passes* a `10^38` test **[V]**. Comet compares the `i256` first. For 363 the
   `10^p` check must run on the full 256-bit quotient before the low pair is kept.
2. **Fixtures for 363 and 364** (all from the tests read): rounding at scale 1 to 0 for
   `14, 15, 16, -14, -15, -16` times `+1`, and `25, -25` times `-1` (expect `1, 2, 2, -1, -2, -2,
   -3, 3`) **[V]**; `(38,18) * (38,18)` to scale 28 with `123456789.555555555555555555 *
   11.222222222222222222 = 1385459527.2345679012071330528765432099` (exact product rounds up at
   the 28th digit) and `0.000000000000000001 * 1.555555555555555555 = ...5556` **[V]**; Comet's
   `overflow_scale` and `overflow_precision` shapes: `64053151420411946063694043751862251568` as
   `(38, 0)` times `12345` as `(10, 0)`, and `(38, 37) * (10, 9)` **[V]**; and `1.050 -> 1.1`,
   `-1.050 -> -1.1`, and the all-nines `(5, 3)` to `(3, 0)` carry (a code comment) **[V]**.
3. **Boundary table for the rounding division.** arrow's `test_div_rem_powers_of_ten` takes every
   `10^k` for `k = 0..76`, divisors `p - 1, p, p + 1`, dividends `0, 1, MIN, MAX,
   i128::MIN/MAX, u64::MAX, 2^64, 2^64 + 1, u128::MAX, 2^128, 2^128 + 1` and `p - 1, p, p + 1`,
   in all four sign pairs, against `BigInt` **[V]**. For 363 take the shape with divisor
   `5^k +- 1` and `10^k +- 1` and `k` up to 38.
4. **Candidate list for the 256-bit multiply.** `test_i256` crosses 29 values (`0`, `+-1`, `+-2`,
   `u128::MAX` with highs `+-1`, `2^127`, `2^128`, `2^129` and their negations, `MIN`, `MAX`,
   `MIN >> 1`, `MAX >> 1`) against `BigInt` **[V]**. The set is the carry-boundary corners that
   the SMT proofs of 361 and 362 should also name as test inputs, since the StarRocks carry
   defect in `READING_DECIMAL_ENGINES.md` sat at one corner.
5. **Both table forms for 359's check**: one broadcast `10^p - 1` and its negation, or `10^p`
   with a strict compare; arrow's two tables hold `10^p - 1` and `-(10^p - 1)` **[V]**. It is a
   signed compare of the high word then an unsigned compare of the low word, per side.
6. **The static "infallible" tests of section 1** as 364's compile-time exemption from the
   check for decimal-to-decimal casts; the float limits (`2^53`, `2^24`, `22`, `10`, `2^-48`)
   of section 2 are numbers for 346 and 366 to test against.

## 4. Not worth taking

* **The arithmetic kernels as such.** Division with four extra digits, `mul` that errors past
  scale 38, and `i256` generic division are Hive-style or scalar; none gives Spark's rule or a
  vector form. Comet, which must match Spark, replaces them.
* **Sentinel then null pass.** Two passes for want of a null output in a closure; Varka's mask
  select needs neither.
* **`abs`-based `checked_mul`.** Sign-magnitude costs two `abs`, a conditional negate with a
  borrow and a high-word overflow test; the two's complement form of `wrapping_mul` is
  cheaper, and the product of two decimals cannot overflow 256 bits.

## 5. What this changes

Nothing in 2.8 or a row. Item 1 sharpens 363's done-when (check at full width, with the
wrapped vector); items 2 to 4 are fixtures for 361 to 364 and the SPARK-45786 port. Not read:
Comet's aggregates, its Scala cast serde, `round`/`ceil`/`floor`, arrow's `Decimal256` casts
and string formatting, and whether Comet's Ansi text matches Spark's on the wide path.
