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
removed by `AndIL_sum_and_mask` by design. A form that hides the round trip from C2 is not
available; a form that does not go through `VectorLongToMask` is.

## 2. Design

**One helper for every word-to-mask conversion of the masked body.** `VarkaVectorWalk` emits
`maskOf(word)` where it emits `fromLong(species, word)` today, at the first five sites of the
table; the rest keep `fromLong`, since nothing can fold a constant or a bitmap read from memory
into a compare. `maskOf` is the broadcast form:

```java
lane.broadcast(word).and(LANE_BITS).compare(NE, 0)   // LANE_BITS: lane i holds 1 << i
```

which never reaches `VectorLongToMask`, so the rule cannot apply. At the int lane the word is
narrowed to an int first (the lane count is at most 16, so the bits fit); `LANE_BITS` is a static
constant per species of the emitted class, built in its static initialiser, as the range tables
are.

**Where it applies: decided by measurement, object if wrong.** Two choices:

1. *Everywhere.* The emitted bytes stay one per shape whatever the host, as the shape hashes and
   `emitted_bytes.json` assume. On AVX2 `fromLong` already lowers to a broadcast, an AND and a
   compare, so the form should cost nothing there; on AVX-512, where `fromLong` is one `kmov`, it
   is three instructions for each conversion.
2. *Only below AVX-512*, keyed on `VarkaEmitOptions.useAVX`. The bytes on an AVX-512 host stay as
   they are; but `useAVX` is not the host's level by default, on purpose, so the choice would need
   the host's level in the emitter's defaults, which the shape hashes then have to carry.

The row measures choice 1's cost on the laptop (AVX-512) and on a GitHub-hosted AVX2 runner,
with the masked-body rows of `VarkaEmitterParityBenchmark` (its results files under
`sql/catalyst/benchmarks/`, laptop and EPYC 7763 runner) and the boolean and
narrow choice rows, and takes choice 1 unless the AVX-512 cost is material (over 2% on a masked
row). The prediction, written before the run: under 1% on AVX2, a few percent on AVX-512's masked
select rows, nothing on dense rows.

## 3. Tests

1. **The shrunk cases of row 396 as regression tests**, in a new `VarkaAvx2SelectSuite`: each
   case's kernel through `VarkaCompiledCheck` under the history that failed, asserting the answers;
   it runs in its own JVM under `-XX:UseAVX=2` (on any x86 host, AVX2 being a subset), and once
   without the flag. Before the change it fails, which the row records.
2. **The emitted bytes hold no `fromLong` of a word in a masked body**: a check over the emitted
   classes of the coverage rows that the five sites call `maskOf`'s sequence and no `fromLong`
   with a non-constant argument, so a new site cannot reintroduce the shape.
3. The catalyst defaults gate, the option matrix over the touched suites, and a runner fuzz night
   (the nightly at seeds 202610190 to 202610243) passing the compiled test, which row 396's
   done-when asks for.

`emitted_bytes.json` moves for every shape with a masked select, and is regenerated; `coverage.json`
and `width_audit.json` do not, unless the width audit's census sees the new calls.

## 4. Predictions and costs

The emitter change is the five sites and one helper: half a day. The regression suite and the
byte check: half a day. The measurement on two machines: a day, most of it waiting for the
runner. No change to what any kernel answers on AVX-512.

## 5. Done when

`m8/PLAN.md` row 491's done-when: the round trip's shape is not emitted on targets without mask
registers; each shrunk case of row 396 is a regression test passing under `-XX:UseAVX=2`; a runner
fuzz night passes the compiled test; the cost is measured on both instruction sets.

## 6. Outcome

To be written as the work lands.
