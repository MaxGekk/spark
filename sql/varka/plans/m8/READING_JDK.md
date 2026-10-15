# Reading for milestone 8: what the JDK 25 sources say about math and conversions

Read on 15 October 2026 at the owner's request: do the JDK sources back what `m8/PLAN.md` 2.7.1
and rows 345, 346, 357, 358, 366 and 367 say about them? Paths are relative to
`/home/max/proj/openjdk-build/jdk25` at commit `6c48f4ed7` (tag `jdk-25-ga`, so not 25.0.4).
Spark's own call sites were checked in `mathExpressions.scala`. **[V]** means the author read the
code; **[I]** means inferred. Nothing was built or run.

## 1. Claims that hold

* **Pow's weak branch [V].** `src/java.base/share/classes/java/lang/FdLibm.java` lines 2197 to
  2216: for `|y| > 0x1.00000_ffff_ffffp31`, `|x| < 0x1.fffff_0000_0000p-1` or `|x| >
  0x1.00000_ffff_ffffp0` returns an infinity or zero by the sign of `y`; between them `log(x)` is
  `x - x^2/2 + x^3/3 - x^4/4` ("sufficient ... since |1-x| is tiny <= 2**-20"). The thresholds
  in row 358 are the code's. The weak domain is `[1 - 2^-21, 1 + 2^-20)` [I, from the hex], so
  the guard `||x| - 1| <= 2^-20` and `|y| > 2^31` is a superset of it.
* **Exact exponents [V].** Lines 2122 to 2150: `y == 0` first (so `pow(NaN, 0)` is 1), then NaN
  (`x + y`), then `y == 2` as `x * x`, `y == 0.5` as `Math.sqrt(x + 0.0)` when `x >=
  -Double.MAX_VALUE` (else falls through), `|y| == 1`. `pow(+-1, +-inf)` is `y - y`, NaN; `pow(1,
  NaN)` is NaN because the NaN test is first. All as the plan states.
* **Binding [V].** `src/jdk.incubator.vector/share/classes/jdk/incubator/vector/
  VectorMathLibrary.java`: x64 binds SVML (`__jsvml_<op><f>N_ha_<z0|l9|e9|ex>`), aarch64 and
  riscv64 SLEEF, else the Java lambdas. SLEEF is version 3.6.1 (`libsleef/README.md` line 7),
  tier `u10` for every operator but `hypot` (`u05`); SVML states no accuracy anywhere in the tree
  (the `.S` files are compiler output and carry only a licence header). `-Djdk.incubator.vector.
  DEBUG` is `jdk/internal/vm/vector/Utils.java` line 34.
* **Specification bounds [V].** `VectorOperators.java` lines 79 to 83: lanewise operators follow
  the accuracy of the equivalent `Math` method. `Math.java`: 2.5 ulp for `sinh`, `cosh`, `tanh`
  (lines 2669, 2697, 2732); also `tan` 1.25, `atan2` 2, `hypot` 1.5, the rest 1.
* **The fdlibm-everywhere list [V].** Only `sin cos tan tanh cbrt exp log log10 pow` are
  `@IntrinsicCandidate` with x86 stubs (`stubGenerator_x86_64.cpp` lines 3680 to 3710, Intel LIBM
  sources; `UseLibmIntrinsic` is a diagnostic flag, default true, `globals_x86.hpp` line 163).
  `atan2`'s C2 intrinsic returns false, "not yet correctly implemented" (`library_call.cpp` line
  1927). So `asin acos atan atan2 hypot sinh cosh expm1 log1p` are FdLibm on every host.
* **`log10` [V].** `FdLibm.Log10` is `y*log10_2lo + ivln10*log(x)` plus `y*log10_2hi`: Sun's
  formula, not a library's. Its comment states `log10(10**N) = N` for N = 0 to 22 only.

## 2. Where the source differs from the plan, or adds a fact

1. **The 636-ulp figure is not in the source [V].** `grep 636` over `FdLibm.java`,
   `test/jdk/java/lang/StrictMath` and `test/jdk/java/lang/Math` finds no such number, and the
   file has no comment about the branch beyond the series. 2.7.1 says "a known 636-ulp case
   (`FdLibm.java`, read here)" and item 11 repeats it. The branch is there; the number needs its
   own citation or "read here" should go. Row 367 should find the worst case by search anyway.
2. **Spark calls `StrictMath` for the largest group [V].** `Exp`, `Expm1`, `Log`, `Log2`, `Log10`,
   `Log1p`, `Pow` use `StrictMath` (`mathExpressions.scala` lines 573, 594, 783, 803, 832, 850,
   1435); `Sin`, `Sinh`, `Tanh`, `Cbrt`, `Hypot` and the other trigonometry use `Math`. On x86
   `Math.exp/log/pow` are Intel stubs but Spark does not call them, so for those three the
   Vector API's Java fallback (`Math.*`, `DoubleVector.java` lines 738 to 877) is a different
   function from Spark's, and a declined lane is not bit-exact even when the operator is unbound.
   The plan says this for `pow`; it holds for `exp`, `log`, `log10` too [I, from the two reads].
3. **`cbrt` is not host independent [V].** `Math.cbrt` is an x86 stub (Intel LIBM) and FdLibm
   on aarch64; `StrictMath.cbrt` is FdLibm. Spark calls `Math.cbrt`, so row 358 must treat it
   like `sin`, not like the fdlibm-everywhere group.
4. **The key needs the species width, not only the CPU [V].** `SVML.suffix` picks `z0` only for a
   512-bit species; a 256-bit or 128-bit species gets `l9` even on an AVX-512 CPU, and `POW` is
   unsupported for every species below 512 bits. `LOG`, `LOG10` and `POW` at 512 bits also need
   AVX512DQ. 64-bit double species are never bound.
5. **SLEEF has no `TANH` binding [V].** `SLEEF.isSupported` returns false for `TANH` ("skip due to
   performance considerations"), so row 357's `tanh` on NEON is a per-lane `Math.tanh` loop, with
   no vector kernel to measure. On aarch64 `Math.tanh` is plain FdLibm (the `dtanh` stub exists
   only on x86), so that fallback is bit-exact against Spark there [I, from the stub list].
6. **DEBUG proves the symbol, not the call [V].** `constructEntry` prints the address only for a
   supported operator, and unsupported ones print nothing. The native call exists only if C2's
   `inline_vector_call` (`vectorIntrinsics.cpp` lines 489 to 577) accepts it: constant entry,
   supported vector size. The interpreter and C1 run the Java lambda. The plan's "must report a
   symbol address" is necessary; the C2 intrinsic log is the other half [I].
7. **Masked math runs every lane [V].** `lanewiseTemplate` computes `unaryMathOp` over the whole
   vector and then `blend`s with the mask (`DoubleVector.java` lines 707 to 710). Masked-off
   lanes (nulls, a declined `pow` guard) still pay for, and pass through, the library call.
8. **Intel's `pow` has its own near-one region [V].** `stubGenerator_x86_64_pow.cpp` lines 40 to
   56: for `|x|` in `[1 - 1/32, 1 + 1/16)` a slower, more accurate path; the main path needs
   `|floor(log2|log2|x||) + floor(log2|y|)| < 8`. Both are switching regions for 367's probe,
   beside fdlibm's own (`log` near 1 at `|f| < 2^-20`, `exp` at `|x| < 2^-28`, 0.5 ln2, 1.5 ln2
   and the `k < -1021` subnormal path). The stub header also lists Java's `pow(+1, y) = NaN`.
9. **`sinh`, `cosh`, `tanh` are compositions in the source [V].** `FdLibm.Sinh/Cosh/Tanh` call
   `StrictMath.expm1` and `StrictMath.exp`, with cut-offs at `|x|` = 22 and 2^-28 (2^-55 for
   `tanh`); `tanh` returns exactly +-1 from 22 upward and `x * (1 + x)` below 2^-55. A vector
   `sinh` from SVML is not these formulas, so its bound is its own, as 2.7.1 says.
10. **JEP 537** is not in the tree; the SLEEF import is 3.6.1 [V].

## 3. What is worth taking

1. **The JDK's own vector test cannot fail a lane [V].** `test/jdk/jdk/incubator/vector/
   Double256VectorTests.java` lines 759 to 777: `isWithin1Ulp(actual, expected)` computes
   `nextDown(expected)` and `nextUp(expected)` but never compares `actual` with them. It checks
   only that a NaN pairs with a NaN, so any finite lane passes against any finite answer. It
   is the second foil, with PyTorch's tolerance, for the step-count definition in 345, and it is
   why the contracted bound cannot come from the JDK's tests.
2. **Row 366's oracle is correctly rounded and compact-fast [V].** `java/math/BigDecimal.java`
   lines 3742 to 3862 and the comment to 4040: `doubleValue` and `floatValue` take one IEEE
   division or multiplication when the unscaled long converts exactly and `|scale|` is at most
   22 (double) or 10 (float), else a round bit and a sticky bit on `floor(m/n)`, then
   `scalb` and a subnormal path; the float form never goes through a double. The kernel's `s <=
   27` arm is therefore beyond the JDK's fast path, and its `FLOAT` target must match
   `floatValue`, not `(float) doubleValue()`. `FloatingDecimal.java` line 1396 name that
   double-rounding trap for strings. The sticky-bit derivation is a template for the tie tests.
3. **Row 346's shortest-digits path is Schubfach [V].** `DoubleToDecimal.java` lines 243 to 330:
   `out = c & 1` makes the interval ends inclusive for even `c`; irregular spacing at `c ==
   C_MIN` uses `cbl = cb - 1` and `flog10threeQuartersPow2`; integral values take a shift fast
   path; subnormals with `t < 3` (`t < 8` for float) scale by 10 and `dk = -1` to keep two digits;
   the candidates are `s` and `s + 1` (or the `10 s'` pair), the nearer wins, ties to even. The
   128-bit table `g` has 617 entries (`MathUtils.java` line 193, K -324 to 292); the float form
   uses one `g1(k) + 1`, one 64-bit multiply and int arithmetic (`FloatToDecimal.java` lines 259
   to 263), which suits a lane. A bounded probe for the tie test should confirm the 2-digit floor.
4. **A line-by-line fdlibm reading for item 98 [V].** `Exp`, `Log`, `Log1p`, `Pow` are
   polynomials with high-word integer compares on `|x|`, so each branch is a lane compare and a
   blend. `Pow` is the exception, about 300 lines of split high and low parts. Java does
   not contract multiply-adds, which is why 36's emitted-fdlibm option must be costed without FMA.

## 4. What is not worth taking

* **The x86 stubs' assembly.** They state no accuracy and cannot be called from a lane.
* **`FDBigInteger` and the `parseDouble` correction loop.** Row 366 converts a long and a scale,
  not text, so `BigDecimal`'s algorithm is the closer model.
* **`BigDecimal`'s `scale < 0` arm.** It multiplies out to an integer; a kernel with a
  non-negative per-column scale never reaches it [I].

Nothing in this note edits a row. Items 1 to 6 of section 2 are corrections or additions the
owner may want in 2.7.1, 357, 358 and 367. Not read: `SharedRuntime::dexp` (C fdlibm, the aarch64
fallback), the SVML assembly, the C2 vector intrinsic's logging flag, and Spark's `Decimal`
conversion code.
