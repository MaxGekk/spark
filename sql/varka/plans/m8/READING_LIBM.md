# Reading for milestone 8: what Arm's routines and glibc say about exp, log and pow

Read on 15 October 2026 at the owner's request: how do these libraries reach their stated error,
what would a lane form cost, how is the stated ULP established, and are the bits the same with and
without FMA (item 98, the no-FMA costing, rows 345, 357, 358, 367)? Paths are relative to
`/home/max/proj/optimized-routines` at commit `97362b2` (13 October 2026) and `/home/max/proj/glibc`
at commit `2e27b7e9` (15 October 2026). There is no `pl/math/` in the Arm tree: the vector and SVE
code is `math/aarch64/{advsimd,sve}/`. **[V]** means the author read the code; **[I]** means
inferred. Nothing was built or run. `READING_JDK.md` is not repeated.

## 1. How the three functions reach their error

* **`exp` [V].** `math/exp.c`, same constants in glibc `e_exp.c`: `N = 128` table, degree-5
  polynomial. `z = x * N/ln2` is rounded by adding the shift `0x1.8p52`; `r = x - k*ln2hi/N -
  k*ln2lo/N`; `exp(x) = scale + scale * tmp`, `scale` from `T[idx+1] + (k << 45)`, `tmp = tail + r +
  r^2*C2 + ...` with the table's `tail` (two `uint64` per entry, 2 KB). `pow.c`'s header gives `exp`
  0.509 ULP, 0.511 without FMA. `|x|` outside `[2^-54, 512)` branches; from 512 `specialcase` scales
  by `2^1009` or `2^-1022`, and its subnormal arm rounds once before scaling.
* **`log` [V].** `math/log.c` (glibc `e_log.c` the same): `x = 2^k z`, `z` in `[OFF, 2 OFF)`, 128
  intervals indexed by mantissa bits after subtracting `OFF = 0x3fe6000000000000`, table `(invc,
  logc)`; `r = fma(z, invc, -1)`, `|r| < 1/256`; `log = k*Ln2 + logc + r + poly(r)` with `Ln2` split
  in two parts and a degree-6 polynomial. Inputs in `[1 - 2^-4, 1 + 0x1.09p-4)` skip the table: `r =
  x - 1`, degree-12 polynomial, `r - r^2/2` in hi and lo parts (0.532 or 0.507 ULP by table size).
  glibc `e_log.c`: 0.519 and 0.520 without FMA. Without FMA `r` is a different expression, `(z - chi
  - clo) * invc`, from a second table `tab2` (`HAVE_FAST_FMA`).
* **`pow` [V].** `math/pow.c`: `log_inline` returns `hi + lo` with relative error `1.3 * 2^-68` (128
  intervals, four doubles per entry, degree 8); `ehi = y*hi`, `elo = y*lo + fma(y, hi, -ehi)`; then
  the same `exp_inline(ehi, elo)`. Stated 0.54 ULP. Without FMA the product is a Dekker split (`&
  -1ULL << 27` masks).
* **Special cases are C99, not Java [V].** `pow.c` lines 255 to 300: `y == +-0` gives 1 for every
  `x` including NaN; `x == 1` gives 1 for every `y` including NaN (a signalling NaN excepted); `x ==
  -1`, `y == +-inf` gives 1; `|y| < 2^-65` gives `1 + y` or `1 - y`. These are the cases row 358
  lists as Java's NaN against C99's 1 [the Java side is from the plan, not read here].
* **glibc's `log10`, `expm1`, `log1p` are Sun's [V]** (1993 notice in `e_log10.c`, `s_expm1.c`,
  `s_log1p.c`); `exp`, `log`, `log2`, `pow`, `exp2` are the table code; the hyperbolics, `cbrt` and
  `erf` are something else (6.1).

## 2. The vector lane forms and what a table costs

* **AdvSIMD `exp` [V].** `advsimd/exp.c`: a 128-entry `uint64` `sbits` table only (1 KB), no `tail`,
  polynomial `r + C0 r^2 + C1 r^3 + C2 r^4`: about a dozen lane operations plus a table lookup per
  lane (`lookup_sbits`, body not read). Dropping the tail costs error: "1.9 + 0.5 ULP" against 0.51.
  Both vector `exp`s test `any(lane special)` once (`|x|` above 708.4): one lane sends the whole
  vector to a slower path.
* **SVE `exp` [V].** `svexpa` (FEXPA) holds the 64 values `2^(i/64)` in the instruction: no table,
  no gather. Header "0.51 + 0.5 ULP", limit 1.02. No Vector API operation corresponds [I].
* **SVE `log` and `pow` [V].** `log`: two gathers (`invc`, `logc`) from a 128-entry table, no hi/lo
  split, no near-1 branch; 2.64 ULP at `0x1.95e54bc91a5e2p+184` (AdvSIMD limit 1.67). `pow`: three
  gathers in the log, one from a 256-entry `sbits` table in the exp, tail dropped, measured 1.04 ULP
  at `(0x1.3d2d45bc848acp+63, -0x1.a48a38b40cd43p-12)`; AdvSIMD sends special lanes to the scalar
  routine one at a time.
* **Cost [V].** `mathbench` exists but no cycle count is stored in the tree, and a Vector API
  gather's cost is not read here. A table-free lane `exp` is SLEEF's shape [I].

## 3. How the stated ULP is established

* **Arm [V].** Polynomials are Remez in Sollya (`tools/exp.sollya`: degree 5, `N = 128`,
  coefficients rounded to double one at a time; `log.sollya` degree 12 on the near-1 interval). The
  header gives an analytic budget (`0.5 + 1.11/N + abs poly error * 2^53`), then `build/bin/ulp`
  measures against MPFR at 80 bits (`prec_mpfr_d`) on `TEST_INTERVAL` ranges (e.g. 400 000 points);
  README: exhaustive for float, "estimated from a sufficiently large random sample" for double.
  Worst arguments are pasted into comments. Quality bar: below 0.66 ULP for scalar double, 3.5 for
  fast variants. The figure is the error before rounding (`ulp.h`: distance to "the closest real
  number that gets rounded to got", 0 when correctly rounded), so `TEST_ULP (exp, 0.01)` means 0.51
  and the vector limits above are limit plus 0.5. Nothing is a proof; the comments are a bound plus
  random evidence, the "lower bound" 367 states.
* **glibc [V].** Expected values come from MPFR via `gen-auto-libm-tests` (`auto-libm-test-out-*`,
  four rounding modes). `libm-test-support.c` computes `ULPDIFF = |computed - expected| /
  ulp(expected)`, with `ulp` from `ilogb(expected)`; zero or subnormal expected uses the smallest
  subnormal. Passing means `ulps <= max_ulp`. The ulps file holds integers (`ceil`, line 343), in
  the format of the README's example `Function "yn":` then `double: 6`. A function absent from every
  file gets 0 if tested as exact else 9. A NaN against a number or an infinity against a finite
  value fails. The sign of a zero is checked except in the vector tests (`!flag_test_mathvec`).
* **The checkout lacks the arch files [V].** Only `dbl-64/libm-test-ulps`, `flt-32` and `i386`
  exist; `x86_64` and `aarch64` have none, so glibc's integer figure for `exp`, `log`, `pow` could
  not be read (the `dbl-64` file has no entry for them). Its entries are `double: 0` in all four
  rounding modes for `sinh cosh tanh asinh acosh atanh erf erfc lgamma`: the README says a 0 "can be
  useful to check for correctly rounded implementation", and these are (6.1). The manual
  (`math.texi` line 1547) says the aim is "within a few ulp", not correct rounding.
* **x86 libmvec states nothing [V].** `svml_d_{exp,log,pow}*` are Intel assembly with algorithm
  comments and no accuracy figure; `NEWS` lists "[28766] manual: Document libmvec accuracy". glibc's
  aarch64 libmvec has `advsimd/exp.c` and `sve/exp.c` (not diffed with Arm's).

## 4. Bits with and without FMA (item 98, the no-FMA costing)

* **The same C is two algorithms [V].** `HAVE_FAST_FMA` (Arm: `FP_FAST_FMA` or `__aarch64__`) or
  `__FP_FAST_FMA` (glibc) switch `log`'s `r` and `pow`'s product to different expressions, and the
  stated errors differ (0.509 and 0.511; 0.519 and 0.520; relative `1.3` and `1.5 * 2^-68`).
* **glibc x86_64 picks by CPU [V].** `multiarch/e_exp.c` ifunc: `e_exp-fma.c` (built with `-mfma
  -mavx2` from the same source) when FMA and AVX2 are usable, `-fma4` or the sse2 build otherwise;
  likewise `log`, `pow`, `expm1`, `log1p`. With `-mfma` the compiler also contracts every `a*b+c`
  [I]. So one glibc gives different bits on FMA and non-FMA hosts, in rare last-bit cases; no source
  states a reproducibility promise. Arm's vector code is fused by construction (`vfmaq`, `svmla`): a
  Java form on a host with no FMA would need `Math.fma` [I].
* **What this gives item 98 [I].** The non-FMA arms (`tab2` for `log`, the 27-bit split for `pow`,
  `exp` with no FMA branch at all) are pure `+ - *` with a fixed order, and Java does not contract,
  so emitting them gives one bit pattern on every host with no `Math.fma`, at the price of the
  second table, the splits and about 0.001 to 0.002 ULP more error. Emitting the FMA arms with
  `Math.fma` also gives one pattern if `Math.fma` is exact on every host (not read here). Neither
  equals `StrictMath`'s bits, so the contract is "within the bound", as item 98 says.

## 5. What the sources say about correct rounding

* The exp/log/pow code does not aim at it [V]: glibc's manual says so; Arm's README sets 0.66 ULP.
  The budgets leave 0.009 (`exp`), 0.019 (`log`), 0.04 (`pow`) ULP beyond 0.5. glibc does ship
  correctly rounded doubles for other functions (6.1).
* No source states how often a result is misrounded [V: none found]. With error before rounding at
  most `d` ULP, a midpoint must lie within `d` of the exact value, so the rate is at most about `d`
  [I]. Arm has correctly rounded **single** `expf`, `exp2f`, `exp10f` (`cr_expf.c`, headers read).

## 6. What is worth taking

1. **glibc carries correctly rounded double code for row 357 [V].** `e_sinh.c`, `e_cosh.c`,
   `s_tanh.c`, `s_asinh.c`, `e_acosh.c`, `e_atanh.c` (also `s_cbrt.c`, `s_erf.c`, `e_lgamma_r.c`)
   are copied from CORE-MATH ("Correctly rounded hyperbolic sine for binary64"; `NEWS`: "imported
   from the CORE-MATH project"), with a correctness proof cited in `e_sinh.c` (Melquiond and
   Zimmermann, March 2026). Shape: a double-double fast path with an error bound `e` and a rounding
   test (`lb = rh + (rl - e)`, `ub = rh + (rl + e)`, return when `lb == ub`), a slower accurate path
   (`as_exp_accurate`), and a table of hard cases (`as_sinh_database`). Comments record failures
   "(rndz, no fma)" and `-fma` builds exist, yet a correctly rounded result is unique, so if the
   proof holds the bits are the same on every host [I]. Spark's row engine is `Math.sinh` (fdlibm),
   so this is not Spark's answer: it is a true-value column for 367 (which says which side is wrong
   when two answers disagree) and the shape of item 98's "later correct rounding". `exp`, `log`,
   `pow` are not CORE-MATH here [V]. Cost was not read.
2. **A third foil for 345's definition [V].** glibc's `ULPDIFF` divides by `ulp(expected)`, which is
   the `k * Math.ulp(reference)` form 2.7.1 rejects (twice the doubles below a power of two), and it
   ignores the sign of zero in vector tests. With the PyTorch and JDK foils of the other readings it
   makes a third definition that the step count corrects.
3. **Switching regions for 367 [V].** `log`: `[1 - 2^-4, 1 + 0x1.09p-4)` and the subnormal rescale
   by `2^52`; `exp`: `|x| < 2^-54`, `|x| >= 512`, `ln(2^1022) = 0x1.6232bdd7abcd2p+9`, the subnormal
   result arm; `pow`: `|y| < 2^-65`, `|y| >= 2^63`, subnormal `x`.
4. **Item 98's shape [V/I].** The scalar `exp` is a 2 KB table plus degree 5 with 0.51 ULP; dropping
   the tail makes a lane form with a 1 KB table and 2.4 ULP. Sollya scripts regenerate the
   constants; the licence is `MIT OR Apache-2.0 WITH LLVM-exception` [V], compatible with Apache-2.0
   [I; the owner should confirm].

## 7. What is not worth taking

* FEXPA, SVE gathers, ifunc plumbing, non-nearest rounding, `errno` and `fenv` handling, the float
  routines, glibc's `log10`, `expm1`, `log1p` (Sun's) and the x86 libmvec assembly.

Nothing in this note edits a row. Not read: the CORE-MATH proof and slow paths, Arm's `exp2`,
`expm1`, `log1p`, glibc's aarch64 vector bodies, any cost, the arch ulps files (absent).
