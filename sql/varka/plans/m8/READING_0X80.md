# Reading for milestone 8: what Wojciech Mula's notes offer

Read on 16 October 2026 at the owner's request: 22 of the 174 notes on `0x80.pl/notesen.html`,
chosen by title for the milestone's types and casts (the index lists SIMD algorithms for base64,
string search, popcount and parsing, and C++ stories; the rest were not read). The site's HTTPS
certificate belongs to another host, so the pages were fetched over plain HTTP and their text
extracted. **[R]** means read; **[V]** means the author ran a check of it; **[I]** means inferred.
A search of `SKILLS.md`, the plans and the lessons found no earlier mention of the site.

## 1. What is worth taking

1. **Integer division by a column through floating point has a formal proof in the literature.**
   "Dividing unsigned 8-bit numbers" (21 December 2024) lists, under "See also", "Formally verified
   32- and 64-bit integer division using double-precision floating-point arithmetic" (David
   Monniaux and Alice Pain) **[R]**. It is neither in the reading set nor in the record; it is now in `Milestone8_Papers` as
   arXiv 2207.08420. Its abstract (the only part read) says the algorithm mixes floating-point and
   fixed-bitwidth integer computation, was proved correct in Coq, and targets a correctly rounded
   IEEE floating-point unit, which is what the Vector API's `DOUBLE` lane gives. Row 30's
   column divisor (`PLAN.md` 2.9) says the error bound of the double route is an interval lemma
   with sampling, not an SMT goal; a published proof may replace that, and it bears on Sneller's
   second iteration (`READING_SNELLER.md`). Its conditions, and whether they cover 4 lanes of
   signed 64-bit values with `Long.MIN_VALUE / -1`, are not known until it is read.
2. **`TINYINT` and `SMALLINT` division by a column in the float lane.** The 8-bit and 16-bit
   notes divide through `float`: every 8-bit and 16-bit integer converts exactly, the quotient is
   truncated (the default conversion rounds, which is off by one), and they report 6.7 to 15
   times the scalar `div` on four CPUs (cycles per byte); AVX-512 is no faster than AVX2 except on
   Ice Lake, and for 16 bits the approximate-reciprocal route needs a Newton step that loses the
   gain **[R]**. By the record's own bound (`skills/vector-api-and-width.md`: the bound is on the
   dividend), `trunc(a / b)` in a `float` lane is exact for any `|a| < 2^24` **[I]**, which every
   `TINYINT` and `SMALLINT` dividend meets. A named case for row 30.
3. **A split-and-add integer to float conversion rounds twice.** "SSE: conversion uint32 to
   float" (2008) converts the low 31 bits and adds 2^31 when the top bit is set **[R]**. Against
   one correct rounding of the whole value, 329,451 of 2,000,000 random inputs of 2^31 to 2^32
   differ (16.5%); for example 4275361147 is 4275361024 rounded once and 4275361280 by the trick
   **[V]**, a short Python check with `struct` rounding to `binary32` at each step. Row 343 asks
   for `BIGINT` to `FLOAT` rounded once, by round-to-odd; this is the negative case its tests
   should keep by name. To `DOUBLE` the same split is exact (both partial values fit 53 bits).

## 2. Smaller items

* **Floor and ceil (row 344).** "Calculate floor value without FPU/SSE" clears the bits after the
  binary point by an and-mask built from the exponent: 1 shift, 1 and, 1 subtract for the exponent,
  up to 4 compares for the cases, and 1 shift, 1 subtract, 1 negation, 1 and for the mask, with no
  `|x| < 2^52` condition **[R]**. Its demo shows positive inputs only, so how it floors a negative
  non-integer is not confirmed. "Floating point tricks" gives the 2^52 add-and-subtract
  construction the plan uses, with its limits (positive, below 2^52, no NaN or denormals) **[R]**.
* **`pow` by repeated square roots (row 358).** "Faster fractional exponents" computes `x^y` for
  a fractional `y` with one `sqrt` per significant bit of `y` (13 to 19 cycles of dependent
  latency each); it beat Go's `math.Pow` for sparse fractions (2^-2: 46.0 to 11.3 ns) and
  matched it near 2^-8 (45.1 against 46.0 ns) **[R]**. It cannot match `StrictMath.pow` bit for
  bit, so it could only sit under the ULP contract with a measured bound; the part read gives no
  accuracy figure. Low priority.
* **Three-valued logic (row 331).** The `vpternlog` note evaluates any 3-input boolean function in
  one instruction **[R]**. Three-valued `AND` and `OR` over known-true and known-false masks are
  2-input bitwise functions; whether C2 fuses such trees into `vpternlog` on AVX-512 was not
  checked **[I]**.
* **Denormals.** "Penalties of errors in SSE floating point calculations" says masked errors
  (denormal, invalid) still slow arithmetic a lot **[R]**; the fixtures of row 341's benchmarks
  should avoid denormals or include them on purpose.

## 3. What is not worth taking

The digit count by comparison with powers of ten (a precision check is a compare with one
constant), absolute difference of unsigned lanes, 64-bit absolute value (the Vector API has
`ABS`), the bit-mask building notes, the popcount note (it is about spelling suggestions), and
the integer-to-string notes (strings are out of scope). Not read: the other 152 notes.
