# VARKA-491: selects without the long-to-mask round trip on AVX2

## 0. Why this row exists

Row 396 (`VARKA-396.md`, `results/VARKA-396-report.md`) found that JDK 25's C2 miscompiles
`VectorMask.fromLong(s, m.toLong())` on x86 without AVX-512 mask registers: `VectorLongToMaskNode
::Ideal` replaces the round trip with a cast that keeps the mask encoding where canonical booleans
are needed, and a select by the result takes its ELSE branch on every true lane. Varka's masked
body meets it wherever it turns a condition word back into a mask: the known-true word
`kT = toLong(compare) & v0 & v1` folds to `toLong(compare)` when the columns have no nulls (their
validity words are the constant `-1`). The owner chose to keep the emitter off the pattern
(24 October 2026) rather than decline the hosts; JDK 27's rewritten rule (`m9/SCOPE.md` item 114)
is the longer road.

## 1. The sites (reviewed 24 October 2026)

Every `VectorMask.fromLong` the emitter writes, and whether its argument can fold to a mask's
`toLong` of the same lane count:

| Site | Argument | Can match |
| :--- | :--- | :--- |
| `VarkaVectorWalk` `IfElse`, masked body | `kT` of the condition | yes: `toLong(compare) & -1 & -1` |
| `VarkaVectorWalk` `CondValue`, masked body | `kT` | yes |
| `VarkaVectorWalk` arm masks (`emitArmContext`), masked body | `kT`, or `kT ^ -1` for the ELSE arm | the THEN arm yes; the XOR is not the pattern but is not proven safe either |
| `VarkaVectorWalk` `emitGuardCollect` | a node's validity word | not by construction, but a word can be a condition's `kT` |
| `VarkaVectorWalk` `emitPick` | `~word` of an operand | as the ELSE arm |
| `VarkaVectorWalk` `emitToDrivingLane` | `toLong` of an int mask, to a long mask | no: the rule requires equal lane counts (8 against 4) |
| dense body: `BoolColumn`, `CondConst`, `IsNotNull` | a bitmap read from memory, or a constant | no |
| `VarkaBodyEmitter` guard accumulators | the constant 0 | no |
| hand-written kernels (`ChronoVectorOps`, `DateVectorOps`, `IntRangeOps`, `SelectionVectorOps`) | validity bits read from memory, or constants | no |

The dense body keeps each condition as a `VectorMask` (`condMask`) and blends by it, so it never
makes the round trip.

**Two cheaper forms were tried and fail.** Passing `kT | ~laneBits` (the bits `fromLong` drops
anyway) does not stop the fold under `-XX:UseAVX=2` - C2 removes the OR - and an addition would be
removed by `AndIL_sum_and_mask` by design. Both mix the word with constants, which C2 folds. What
it cannot fold is a value it does not know: a static field that is not final.

## 2. Design

**Every masked-body word goes to `fromLong` XORed with a zero C2 cannot fold.** `VarkaVectorWalk`
emits `fromLong(species, word ^ VarkaOpaqueZero.zero)` at the first five sites of the table (the
picks' two conversions counted as one site); the rest keep `fromLong` of their constant or bitmap.
`VarkaOpaqueZero.zero` is a public, non-final static `long`, zero and never written. HotSpot folds
only final fields (and `@Stable` ones) into constants, so the `XorL` stays in the graph, the input
of `VectorLongToMask` is never a `VectorMaskToLong`, and the rule cannot apply. The cost is a load,
which C2 lifts out of the loop, and one XOR a site; each site keeps `fromLong`'s single call.

The XOR goes on every word, not only on those that can carry a `toLong`: the call count and the
bytes of a site then do not depend on its operands, which the emit cost model needs (below), and
the emitter needs no analysis of where a word came from. `fromLongMasks` (reason `MISCOMPILED`,
never drawn by the fuzzer or the option matrix) emits the bare `fromLong` for the A/B.

**Two forms were built and measured before this one, and lost** (section 6): the broadcast form
`broadcast(word).and(laneBits).compare(NE, 0)`, which never reaches `VectorLongToMask`; and, for
conditions, the comparison's own mask ANDed with `fromLong` of the operands' validity words, with
the broadcast form left for the words that carry a `toLong` anyway. Both cost through the emit cost
model as much as at their sites.

## 3. Tests

1. **The shrunk cases of row 396 as regression tests**, in `VarkaAvx2SelectSuite`: each case's
   kernel through `VarkaCompiledCheck`, which must reach the history the case failed in (a case
   that does not reach it in three tries is cancelled, not passed); the suite runs in its own JVM
   under `-XX:UseAVX=2` (`@VarkaSuite(jvmFlags)`, which `dev/varka_matrix.sh` honours too). Under
   `fromLongMasks` the long cases of 24 and 19 October fail, as row 396's report records.
2. **No emitted kernel hands `fromLong` a word a `toLong` reaches**: `VarkaMaskOfWordSuite` runs an
   ASM data-flow pass over every coverage row's kernel, at the defaults and under each option
   matrix configuration, marking each long a `toLong` reaches and clearing the mark at an XOR with
   `VarkaOpaqueZero.zero`; `emitToDrivingLane`'s lane change, whose lane counts differ, is the one
   exception. Under `fromLongMasks` it finds the round trip, so it is not vacuous.
3. The catalyst defaults gate, and a runner fuzz night (the nightly at seeds 202610190 to
   202610243) passing the compiled test, which row 396's done-when asks for.

`emitted_bytes.json`, the emit cost table and its audit, and the width audit are regenerated.

## 4. Predictions and costs

The emitter change is one helper at five sites. The regression suite and the byte check: half a
day. No change to what any kernel answers on AVX-512.

## 5. Done when

`m8/PLAN.md` row 491's done-when: the round trip's shape is not emitted on targets without mask
registers; each shrunk case of row 396 is a regression test passing under `-XX:UseAVX=2`; a runner
fuzz night passes the compiled test; the cost is measured on both instruction sets.

## 6. Outcome

**The broadcast form cost 15-46% on 16 laptop parity rows** (`VarkaEmitterParityBenchmark`, 5
rounds a side against the merge base `be2ec226e7d`, 24 October 2026), for two reasons the emitted
bytes showed. The `IfElse` blend kernels behind the "validity writes per body" rows grew by about
500 bytes. And the null-free calendar rows with `shareChronoPrefix` off, which hold no conversion at
all, went from two loop methods to four: about seven calls a site made the emit cost model's
call-site fit worse, its regenerated `SITES_MARGIN` rose from 0.206 to 0.286, and the planner
closed groups earlier. The base emitter with the regenerated table and base's margins emits base's
classes, byte for byte.

**The comparison-mask form brought the blend kernels to 2% over base but `SITES_MARGIN` to
0.439**: a site cost 0 or 2 calls by whether its operands are nullable, and 7 where it fell back to
the broadcast form, which the per-kind prices cannot see. A cost feature for the fallback brought
it to 0.347; the rest needed the emitter's analysis in the cost model.

**The opaque XOR restores the cost model**: `SITES_MARGIN` 0.206 and `BYTES_MARGIN` 0.156 against
base's 0.206 and 0.155, base's list of wide shapes that gain a loop method, and the calendar
kernels' classes the same size as base's, two loop methods each. The blend kernels are 114 bytes
over base.

The pair on the opaque form, and the runner pair and fuzz night: to be written.
