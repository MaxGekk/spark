# Reading for milestone 8: what spark-rapids-jni offers

Read on 15 October 2026 at the owner's request: NVIDIA's spark-rapids-jni runs Spark's DECIMAL and
cast semantics on GPUs, so is there anything in it for rows 343, 347, 359 to 368? Paths are
relative to `/home/max/proj/spark-rapids-jni/src` at commit `2574edc` (dated 4 September 2026).
Read: `main/cpp/src/decimal_utils.cu`, `cast_string_to_float.cu`, `cast_float_to_string.cu`,
`cast_decimal_to_string.cu` (and the Ryu code under `ftos_converter.cuh` by its comments), the
Java `DecimalUtils.java`, and the tests `DecimalUtilsTest.java`, `CastStringsTest.java`,
`main/cpp/tests/multiply.cpp`, `cast_float_to_string.cpp`. **[V]** means the author re-read the
line (or ran the arithmetic); **[I]** means inferred. Nothing was built or run except two
`decimal` computations in Python for the vectors in section 2.

## 1. How the decimal kernels work

* **128-bit multiply [V].** Both operands are sign-extended to four 64-bit limbs (`chunked256`)
  and multiplied by schoolbook limb products truncated to 256 bits (`multiply`). There is no sign
  handling: a signed product of two 38-digit values is below 2^254, so the low 256 bits of the
  two's-complement product are exact **[I]**. Half of every operand is sign extension, so this
  does about twice the limb work that row 362's unsigned magnitude with a sign correction needs
  **[I]**; it is the simple form, not a model for the lane code.
* **Rescale and rounding [V].** `divide_and_round` is a bit-by-bit long division (256 iterations,
  with its own comment that it is likely very slow) on absolute values, then rounds away from zero when
  `2 * |remainder| >= |divisor|`. Two details worth keeping: doubling the remainder can lose its
  top bit for a divisor near 2^127, which the code detects (`(double_remainder >> 1) != r`) and
  treats as "round"; and the direction comes from the sign the quotient would have, because the
  quotient can be 0. Row 363's `5^k` pre-shift form needs neither, as its divisor stays one word.
* **Overflow [V].** One test at the end: `|x| >= 10^38` on the 256-bit value
  (`is_greater_than_decimal_38`). Add and subtract cannot wrap 256 bits; the Java wrapper rejects
  a scale gap above 77 (`The intermediate scale ... exceeds 256-bit representation`). Multiply
  has one extra pre-check, `precision10(product) - exponent > 38`, before it scales the product
  up, because that second multiply could wrap 256 bits **[I]**. The check is against 10^38 only:
  the library has no `10^p` for p below 38 (the signatures take a scale, not a precision), so it
  is the caller's. Row 360's `CheckOverflow` at the result's own precision is not in this code.
* **Null versus ANSI error [V].** The library decides nothing. Each decimal op returns a table of
  (overflow bool column, result column); the plugin chooses null or error. The integer multiply
  takes `ansi` and `try` flags and throws `exception_with_row_index` naming the first failing
  row (`multiply.cpp`: TRY nulls, ANSI throws index 1, 2, 3, 2 for the four widths). The
  float-to-decimal returns "the index of one failure row" (`CastFloatToDecimalResult`). Varka's
  `NUMERIC_VALUE_OUT_OF_RANGE` raise names a value, not a row; the library is no help there.
* **Add and subtract [V].** Both operands go to the finer scale first (an exact multiply by a
  power of ten), the sum is taken in 256 bits, and one `set_scale_and_round` goes to the result
  scale. This is row 360's "rescale-up of the coarser operand", with the final HALF_UP only when
  the result scale is coarser.

## 2. Spark edge cases it special-cased (comments quoted)

1. **The multiply rounds twice, on purpose, by default.** `decimal_utils.cu` (line 678): "Spark
   has a bug [SPARK-40129, SPARK-45786] in versions 3.2.4, 3.3.3, 3.4.1, 3.5.0 and 4.0.0 ... to
   match the legacy behavior we need to first round the result to a precision of 38 then ... to
   the final scale". `multiply128(a, b, scale)` passes `interimCast = true`. **[V]**
   The first rounding is HALF_UP to 38 digits, then HALF_UP to the target scale. I computed the
   exact product rounded once HALF_UP (Python `decimal`) for the five vectors in
   `DecimalUtilsTest.java`: the two `multiply128WithoutInterimCast` and `largePosMultiply` vectors
   agree with the library, but `simpleNegMultiplyTenByTenSparkCompat` differs in two of three
   rows (`...877.580354` against the single-round `...877.580353`; `...058.575920` against
   `...058.575919`). **[V]**
   **This sharpens row 363, it does not contradict it:** the plan's lemma (one HALF_UP of the
   exact product) holds for a Spark that has the fix; rapids matches the buggy releases. The
   differential must therefore run against the fork's own `Decimal.scala`, and the
   SPARK-45786 test port should say which behaviour the fork has. The fork's version is **[I]**,
   not checked here.
2. **Integral divide judges overflow on the 128-bit value** ("Spark doesn't report this as an
   overflow even though this has obviously overflowed", `intDivideNotOverflow`): the result is
   the low 64 bits, the flag comes from the wide value. Division is out of scope for Varka; the
   `intDivideNotOverflow` test is a record of a Spark quirk only.
3. **Divide by zero sets the overflow flag** and a zero result; remainder is computed using the
   Java definition of remainder, on absolute values, the sign taken from the dividend only.
4. **Division shifts beyond 38 digits** go through a base-10 long division in two steps
   (`n_shift_exp < -38`), since `10^39` does not fit 128 bits. It is the same wall as the plan's
   declined `k = 39` multiply. **[V]**
5. **Float to decimal** (`scaled_round`) is the one place the file spells out Spark's quirks: the
   input is "double-ized" first, so a FLOAT converts through its widened double (this agrees with
   `casts.tsv`'s "Decimal(double)"); "Spark chooses the rougher 3/10 ratio" for bits to decimal
   digits; "Spark rounds up the power-of-10 to floor for DOUBLES >= 2^63 (and yes, this is the
   exact cutoff)"; a float "never add[s] 1/2 bit". NaN and infinity give null with no failure
   row set, and `-bound >= v || v >= bound` gives null with one. **[V]**
   The method is arithmetic on the double's binary value plus flooring "digits past double
   precision", not a shortest-digits search. Whether it is bit-equal to `BigDecimal.valueOf` on
   every double is not shown by its tests (five columns); **[I]** it is an approximation, which
   is why rows 346 and 350 keep the shortest-digits path with a guard. Its vectors are good
   fixtures: `9.95` at scale 1 gives `10.0` (a tie at the shortest digits, though the binary
   value is below 9.95), `3527.61953125` at scale 7 gives `...195313`, `10.3` gives `103`.
6. **Strings (out of scope, owner's decision).** `cast_string_to_float.cu` keeps 19 digits in a
   `uint64_t`, truncates the rest, and computes `digits * 10^q` in double; the team shipped that
   with up to 1 ULP of error ("NVIDIA/cudf-spark#10773") and later added an integer-only
   correctly-rounded path only for `digits > 2^53` and `|q| <= 19`. The test then compares with
   `Double.parseDouble` bit for bit there, but allows `ulpDiff <= 1` elsewhere. **[V]** A
   case for the plan's bit-identical stance in 366, nothing more. `cast_decimal_to_string.cu`
   follows `BigDecimal.toString` (plain form while the adjusted exponent is at least -6, else
   `E+n`) and notes it "does not properly handle a max negative decimal value". Float to string
   is Ryu with Java's layout rules; it was not compared with the JDK's `Double.toString`.

## 3. What is worth taking

1. **Vectors.** The `Spark compat` multiply triples, `9.95`/`10.3`/`3527.61953125` for the float
   arm, `5e37 * 2` and `99..9 (38 nines) + 1` for the 10^38 edge (both overflow), the opposite
   sign overflow `577694938495380589068894346.7625198736 * -1258508260891400005608241690.15647`,
   `0.964808037102759 - 1` at scale 25, and the add at scale 10 that overflows for both signs.
2. **A flag that selects the legacy rule** is a precedent for 363's SPARK-45786 test: pin the
   expected value for the fork's Spark, and keep the other behaviour as a named vector.
3. **The remainder-doubling hazard** (item 2 of section 1) as a test: a divisor between 2^126 and
   10^38 with a remainder above 2^126, where doubling wraps a signed 128-bit value;
   relevant if 363 ever divides by a non-`5^k` constant.
4. **The NaN, infinity and out-of-bound rows of the float test** (`NaN`, `-Infinity`, `-1e7` in
   `DECIMAL(4, 1)`) as the cells of `float -> dec` in all three modes.

## 4. What is not worth taking

* **The algorithms.** Bit-by-bit division, a 77-step scan for `precision10` (marked as slow in its comment), a
  thread per row with a 256-bit scratch: none maps to a lane pair or to the Vector API.
* **The warp-wide string parser** and the Ryu tables: strings are out of scope.
* **The ANSI model.** A row index in an exception is a GPU batch convenience; Spark raises per
  value, which row 368's suites check.

Nothing here edits a row. Item 1 of section 2 is an addition to 363's done-when if the owner
wants it (state the fork's behaviour); the rest are fixtures and citations. Not read:
`cast_string.cu` (string to decimal and integer), `row_conversion`, the JSON and hash code.
