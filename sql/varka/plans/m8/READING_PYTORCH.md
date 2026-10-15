# Reading for milestone 8: what PyTorch's CPU vector code offers

Read on 15 October 2026 at the owner's request, starting from pytorch/pytorch#151441 ("Implement
fast exp for AVX2 and AVX512 for the flash attention"): is there anything in PyTorch worth
borrowing for the milestone's math and cast rows? A search of `SKILLS.md`, the plans and the
lessons found no earlier mention of PyTorch. Paths are relative to `/home/max/proj/pytorch` at
commit `1b13c14` (15 October 2026). **[V]** means the author read the code or ran it;
**[I]** means inferred.

## 1. What the pull request is

`fexp_u20` (`aten/src/ATen/cpu/vec/vec256/vec256_float.h`, `vec512/vec512_float.h`) is a
float-only `exp` after Malossi et al.: it scales `x * log2(e)` into the IEEE exponent and
mantissa fields, and a degree-3 polynomial on the fractional part corrects the mantissa. Its one
caller is the flash-attention kernel (`aten/src/ATen/native/cpu/FlashAttentionKernel.cpp`), which
stores the result as fp16 or bf16 straight away. The "1 ULP" in the description holds only after
that store; the author says so in the review thread. The default `exp` stays SLEEF's `u10`.

**Not within any bound this project could state [V].** The kernel's logic compiled standalone
(AVX2 and FMA, nothing from PyTorch linked) gives:

| input | `fexp_u20` | libm `expf` |
| :--- | :--- | :--- |
| `NaN` | `-0` | `NaN` |
| `0` | 0.999947 | 1 |
| `1` | 2.71811 | 2.71828 |
| `88.7` | 3.32595e+38 | 3.32598e+38 |

The worst relative error over `[-80, 80]` stepped by 1e-4 is 1.2e-4, against 2^-24 (6e-8) for
one fp32 ulp. Only the two range masks repair edge cases, and `NaN` passes both. This is the
point of `m8/PLAN.md` 2.7.1: a bound is a count of representable values from the row engine's
answer, measured, never a claim made for a use the caller rounds away. Spark's `exp` takes
DOUBLE, a FLOAT input is cast first, so a float-only kernel would never be reached anyway.

## 2. What is worth taking

1. **The tier is part of the name.** `vec256_float.h` and `vec256_double.h` call SLEEF by tier:
   `u05` for `hypot`, `u10` for `exp`, `log`, `pow`, `tan`, `tanh` and the inverse functions,
   `u15` for `erfc`, `u35` for `sin` and `cos` on floats. Rows 345 and 367 key the bound by
   library; they should record the SLEEF tier beside each measured bound, so a measured step
   count above the stated tier is a finding. The tiers are a cross-check on 367's probe, not a
   bound for the contract: SVML states none.
2. **The exponent-field construction** is a candidate shape for `m9/SCOPE.md` item 98 (an emitted
   `exp`): range reduction by an integer add into the exponent field, one polynomial. At double
   width it needs a higher degree to reach a few ulp, and an explicit `NaN` path. It is a
   citation for item 98, not a row in this milestone.
3. **Where rounding hides error, a cheaper kernel is legitimate.** This is the pull request's
   whole argument. A cast to FLOAT or to a small integer is the same situation in Varka. It
   belongs in the discussion of row 358 as a design note only: the contract is stated per
   function, not per use, and nothing here changes that.

## 3. What is not worth taking

* **Casts.** `torch/headeronly/util/TypeCast.h` leaves float to integer as the platform's
  behaviour "for NumPy compatibility" (undefined for out-of-range values and `NaN`), and routes
  float to `uint8` through `int64` only to make compilers agree. Spark saturates, maps `NaN` to
  0 in LEGACY mode and raises in ANSI and TRY mode, which `casts.tsv` and the 62 exhaustion and
  29 SMT proofs already pin down. PyTorch is a cautionary case for rows 366 and 368, not a source.
* **Vector masks** (`aten/src/ATen/cpu/vec/vec_mask.h`) wrap platform mask registers behind one
  class; the Vector API's `VectorMask` already does that, and the emitter's mask handling is the
  record's own.
* **The rest of the tree** (autograd, tensors, GPU code) has no counterpart.

Nothing in this note changes a row. Item 1 of section 2 is an addition to 345 and 367 if the
owner wants it; the others are citations.
