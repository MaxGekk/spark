# Reading for milestone 8: three papers on division and wide multiplication

Read on 16 October 2026 at the owner's request, from mechanical transcriptions (the
`pdftotext -bbox-layout` method of `sql/varka/papers/README.md`) kept beside the PDFs in
`/home/max/Downloads/Milestone8_Papers/markdown/`; formulas and tables were read against the PDFs
where they mattered. Two of the papers were named by an outside review of the milestone 8 plan, the
third by `READING_0X80.md`. **[R]** means read; **[I]** inferred.

| paper | source | licence of the copy |
| :--- | :--- | :--- |
| Monniaux, Pain: "Formally verified 32- and 64-bit integer division using double-precision floating-point arithmetic" (2022) | arXiv 2207.08420 | arXiv |
| Didier, El Mrabet, Glandus, Robert: "Truncated multiplication and batch software SIMD AVX512 implementation for faster Montgomery multiplications and modular exponentiation" (2024) | arXiv 2410.18129 | CC BY 4.0 |
| Edamatsu, Takahashi: "Fast Multiple-Precision Integer Division Using Intel AVX-512" (2023) | IEEE TETC, DOI 10.1109/TETC.2022.3196147 | an institution's IEEE Xplore copy; not to be redistributed |

## 1. Monniaux and Pain: the one to take, for row 30

**What it is [R].** Unsigned 32-bit and 64-bit division by a variable divisor with no integer
divide, for a processor with IEEE double precision and a fused multiply-add, every operation
rounded to the nearest (the tie rule does not matter). The 64-bit algorithm: a correctly rounded
single-precision reciprocal of `b`; one fixed-point step `alpha = fma(-b, r0, 1)`, `r = fma(alpha,
r0, r0)` to a relative error under `1049 * 2^-56` (about `2^-46`); a first quotient `q1` from `a`
and `r0` and its remainder `r1 = a - b * q1` in 64-bit integers; a second quotient `q3` from `r1`
and `r`, `r3 = r1 - b * q3`, and `q3 - 1` if `r3 < 0`; the answer `q1 + q2`. `b = 1` returns `a`
and `b >= 2^63` returns `a >= b`, both chosen by a conditional move, so the code is straight-line.
Signed division in C's sense (truncation toward zero) divides the absolute values and puts the
sign back. The proof, in Coq with Flocq and Gappa, covers every non-zero divisor, signed and
unsigned, quotient and remainder (883 lines for 32 bits, 2670 for 64), split at `b <= 2^42` and
`2^42 < b < 2^63`; it is in CompCert for the Kalray KV3 (files `kvx/FPDivision32.v` and
`kvx/FPDivision64.v`).

**Why it fits [I].** It needs round-to-nearest only, which is what the Vector API gives, where
Sneller's method needed directed rounding (`READING_SNELLER.md`). It covers the whole `BIGINT`
range, so a column divisor needs no per-batch bound and no decline for size. `Long.MIN_VALUE`
loses its sign to `2^63`, which is the `b >= 2^63` case as a divisor and the `b = 1` path as a
dividend over `1`. The work that depends only on `b` is shared by `div`, `%` and `pmod` over one
divisor.

**What it costs [I].** (1) The paper converts double to integer rounding to nearest; Java's
`(long) d` truncates. Emulating the rounding (the 2^52 add-and-subtract below `2^52`, the identity
above, by a blend) keeps the paper's operations and so its proof, with the emulation as a lemma
of its own; truncating instead would need the error argument redone with a two-sided correction.
(2) The double-to-long conversions and the 64-bit multiply are native under AVX-512DQ and emulated
by the JDK on AVX2, so the EPYC 7763 runners are where it may lose to the scalar loop. (3) The Coq
proof cannot run in this repository: the kernel follows the paper operation for operation, cites
the proof's files, and is tested at the corner cases the paper names. The kernel is written from
the paper, not from CompCert's code, whose licence was not checked. For `INT` it adds nothing: the
double route `trunc(a / (double) b)` is already exact for every 32-bit operand.

## 2. Didier and others: the layout confirmed, the truncation not transferable

Their "word slicing" stores one 52-bit limb of eight different values per register, one value
per lane **[R]**: Varka's layout for the 128-bit lane pair (rows 359 to 362). Their truncated
multiplication saves about 20% of a Montgomery multiplication by skipping the low partial
products, which is sound only because the low half of `T + q * N` is known to be zero **[R]**.
Row 363's magic-number division by `10^k` has no such cancellation, so a truncated product there
would need its own error bound and a fix-up **[I]**. The kernels use the 52-bit IFMA instructions
(`VPMADD52`); no Vector API operation reaching them is known here, and that was not checked **[I]**.

## 3. Edamatsu and Takahashi: little for this milestone

Reduced radix, limbs of 52 bits in 64-bit lanes so that carries accumulate in the spare bits and
are normalised later **[R]**, is what row 362 does already with 32-bit limbs. The numbers are one
large integer spread across the lanes rather than one value per lane, the correctness check is a
comparison with GMP at chosen values **[R]**, and decimal division is out of the milestone.
