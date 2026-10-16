# VARKA-374: At a 128-bit preferred species the warm-up does nothing, and 42 of 100 compositions release

## 1. Where this came from

`VARKA-373.md` 9 (16 October 2026). With `-XX:MaxVectorSize=16`, the gate's setting and the
preferred species of a 128-bit host such as Apple Silicon, the template warm-up of #827 feeds
exactly the species the kernels use and still does nothing: 43, 41, 41 and 43 of the first 100
compositions of the warm arm released at the deadline with it, 46 and 45 without it. At 512 bits
the same warm-up takes the rate to 0 of 100. The mechanism at 128 bits is not the one of
`VARKA-295.md` 9.6 or of 373's 4-lane case: the virtual call at `IntVector.add` is absent, and
the JFR allocation samples name `Long128Vector.maskFromArray`, `Long128Mask.prepare`,
`Int128Mask.bOp` and `LongVector.broadcastBits` inside kernel loops.

## 2. The admission check, done (16 October 2026)

To be done first, in the manner of `VARKA-373.md` 2: the 128-bit warm arm (400 compositions, the
JIT's own log, every release classified) with the mask sites named above, to learn whether the
cause is a compile-time trap count at a mask method, a boxing that escape analysis does not
remove in the mask path, or something the log has not shown. It would reject any fix that is
chosen before the sites are classified.

**The answer: none of the three is a profile, so no warm-up can reach it.** At
`-XX:MaxVectorSize=16` the long species is 2 lanes, and x86 C2 refuses every 2-lane mask before it
reads any profile. `match_rule_supported_vector` in `src/hotspot/cpu/x86/x86.ad` (a JDK 25 tree
at commit `6c48f4ed7`, lines 1734 to 1744; the JVM run was 25.0.4.1) returns false for
`VectorLoadMask`, `VectorMaskCast` and `VectorStoreMask` when `vlen == 2` ("Implementation
limitation"), and `arch_supports_vector` in
`share/opto/vectorIntrinsics.cpp` asks for exactly those whenever a Vector API intrinsic produces or
consumes a mask object. So a compare to a mask, a mask `and`/`or`/`not`, `blend`, a mask
`broadcast`, `anyTrue`, a mask cast and a masked load or store over `Long128Vector` all run the
Java fallback, a loop over a `boolean[2]` that allocates per operation (`maskFromArray`,
`Long128Mask.prepare` in the JFR samples of `VARKA-373.md` 9). The int species at 4 lanes is not
affected, and neither is any species of 4 lanes or more. This is the per-width census of
`VARKA-153.md` (the skills' "At two 64-bit lanes this JVM lowers no mask at all"); what is new is
that it is the cause of the 128-bit warm arm's deadline releases.

**What was run.** The warm arm of `VarkaSparkFuzzSuite` ("both sides of the warm-up handover",
seed 20261008, warm-up on, no `lanesOverride`, `-XX:MaxVectorSize=16`) in a JVM started from
`dev/varka_matrix.sh --module sql --list`'s classpath, pinned to cores 6-11, with
`-XX:+LogCompilation`, `-XX:+PrintTieredEvents`, `-XX:+PrintIntrinsics` and
`-Xlog:jit+compilation=debug,deoptimization=debug`: once over the first 100 compositions (518 s,
load average 15.1 at the start, 15.6 at the end) and once over all 400 (1,993 s, load 25.9 to 35.1;
other agents' JVMs were running on the other cores). Each kernel class was classified from the
compile log: does any C2 compile of one of its loop methods load the class
`Long128Vector$Long128Mask` (the 2-lane mask), and, as in `VARKA-373.md` 2, cause 1 (10 or more
traps or 3 not-entrant marks on one method) or cause 4 (compiled and still allocating).

| | first 100 | all 400 |
|---|---|---|
| deadline releases (verdicts COMPILED / RELEASED) | 41 (101 / 41) | 151 (383 / 151) |
| released classes | 25 | 79 |
| ... with a 2-lane mask in a loop compile | 23 | 66 |
| releases from those classes | 38 of 41 | 133 of 151 |
| classes not released | 75 | 273 |
| ... with a 2-lane mask in a loop compile | 0 | 0 |
| ... with a 4-lane `Int128Mask` in a loop compile | 62 | 248 |
| `** not supported` lines from `PrintIntrinsics` | 1,586 | 7,461 |
| ... of them at 2 lanes (long, and 3 double in the first 100) | all | all |
| releases from classes without a 2-lane mask, per 100 compositions | 3 | 3, 7, 5, 3 |

The split is complete in both runs: every kernel class whose loop holds a 2-lane long mask was
released, and no class that was not released holds one. The releases of the other classes are
the profiled-predication traps of the skills' deoptimization cycle (4 to 8 `profile_predicate`
traps each, some `unstable_if`), 3 to 7 per 100 compositions, under the row's bar of 10.

The refusals by kind, all 400: mask `broadcast` 1,868 (the `maskAll` and `fromLong` paths), mask
`and` (opcode 430) 780, `or` 545, `comp` to a mask 629 + 434 + 381 + smaller, `test`
(`anyTrue`/`allTrue`) 562 + 197, `blend` 501, mask cast 483, masked load 472, masked store 205.
There is no virtual call in any loop compile of either group, and `broadcastBits`, which the JFR
samples named, is inlined in both groups (637 copies in the classes that compiled), so it is not a
discriminator; nor is `Int128Mask.bOp`, since 248 classes with an `Int128Mask` compiled.

**A direct check** (`MaskProbe`, a standalone loop of compare, mask `and`, `blend` and
`trueCount` at the preferred species, 200,000 calls over 1,024 elements, one JVM on cores 6-11):

| JVM flags | int lanes | long lanes | int loop, ns a call | long loop, ns a call | refusals (long) | GCs (long) |
|---|---|---|---|---|---|---|
| `-XX:MaxVectorSize=16` | 4 | 2 | 2,175 | 38,852 | 33 | 34 |
| `-XX:MaxVectorSize=32` | 8 | 4 | 1,909 | 1,915 | 0 | 1 |
| `-XX:MaxVectorSize=64` (preferred here) | 16 | 8 | 1,659 | 2,308 | 0 | 1 |
| `-XX:UseAVX=1` (an AVX-only x86 host) | 4 | 2 | 1,698 | 39,061 | 33 | 39 |
| `-XX:UseAVX=2 -XX:MaxVectorSize=16` | 4 | 2 | not run | 37,711 | not read | not read |

Single runs on a loaded machine; the 18x gap is the claim, not the digits. It is the x86 back end
at 2 lanes whatever the instruction set: AVX-512 capped to 16 bytes, AVX2 capped, and an AVX-only
host whose native long species is 2 lanes.

**A correction to section 1 and to row 374: this is not what a 128-bit Apple Silicon host does.**
The aarch64 back end (`cpu/aarch64/aarch64_vector.ad`, `match_rule_supported_vector`) has no
`vlen == 2` refusal for the mask nodes, its `min_vector_size` admits a 2-element boolean vector
"to support vector api load/store mask", and `VARKA-153.md` 6 already measured the arm runner
(Neoverse N2, a 128-bit species) refusing nothing in the same shapes. So `-XX:MaxVectorSize=16` on
an x86 laptop imitates a 128-bit host's lane counts and not its back end, for every long kernel with
a mask (the skills' "A refusal at a forced width is the back end's, not the lane count's"). What
aarch64 without SVE does refuse is `VectorLongToMask` (`VectorMask.fromLong`, which needs SVE's
bit permutation), at every lane count; that is a different cost, read from the source and not
measured here. The x86 hosts that reach the refusal in production are those whose preferred long
species is 2 lanes: AVX-only and SSE-only processors.

## 3. The design

No change is built. The three candidates of the plan do not survive section 2:

* **Warm the mask paths.** The refusal is `match_rule_supported_vector`, a fixed table read before
  any profile; a warm-up can change a profile and not the table.
* **Avoid the mask method in the emitted loop behind an option.** It is not one method: compare,
  logic, blend, broadcast, test, cast and the masked memory accesses are all refused at 2 lanes, and
  every long-lane guard, `CASE`, comparison and validity merge is built from them. A long kernel
  without masks at 2 lanes is a second lowering of the long lane (a compare as a sign-bit
  subtraction, a blend as `and`/`or` over lane-wide all-ones vectors, the validity kept as a long
  vector), which is design work beyond this row.
* **Accept the rate and record it.** This is the outcome, with the qualification that the rate is
  an artifact of the gate's x86 setting and that its production reach is the 2-lane x86 hosts.

## 4. Files, tests, measurement, risks

Only this file and row 374 change. No test: there is no code for one to fail without, and the
census that pins the refusal per width already exists (`VarkaWidthAuditSuite`,
`sql/varka/width_audit.json`). The 512-bit arm was not rerun, since nothing changed.

## 9. Outcome (16 October 2026): the cause is named, and the record says why the row cannot fix it

The row's done-when has two arms, and it closes on the second. The cause at 128 bits is named from
two classified logs: x86 C2 lowers no 2-lane mask (section 2), so every kernel with a long mask in
a loop runs the Vector API's Java fallback for each mask operation, allocates, and is released at
the deadline whether the warm-up ran or not. It accounts for 133 of the 151 releases in the 400
compositions and 38 of the 41 in the first 100; every class carrying a 2-lane mask released and no
other did. The rest, 3 to 7 per 100, is the predication cycle and under the bar. The rate cannot
fall under 10 per 100 at `-XX:MaxVectorSize=16` on x86 without changing what the emitter builds at
2 long lanes, because the refusal is a fixed table in the JVM's x86 back end.

**Predictions and plan text, scored.** Section 2's three candidate causes (a trap count at a mask
method, an unremoved boxing in the mask path, something else): the second is the nearest, but the
boxing is not escape analysis failing; the intrinsic is never attempted. Row 374's "a 128-bit host
such as Apple Silicon" and `VARKA-373.md` 9's "the case of a 128-bit preferred species (Apple
Silicon ...)" are wrong for this cause, as section 2's correction says; this file corrects them and
373 is left as written.

**What the record leaves, not filed here.**

1. **The 2-lane long kernel, mask-free or declined.** The decision `VARKA-153.md` and
   `m5/PLAN.md` row 153 left "to the long-lane kernel design" was never taken: at a 2-lane long
   species on x86 a long kernel with a mask runs per lane and allocates. The two answers are a
   mask-free lowering of the long lane at 2 lanes, or a decline of such kernels when the host's
   preferred long species is 2 lanes on x86; the warm-up's deadline already hands such a kernel
   back, after ten seconds of allocating. Either is a scope item for the milestone that owns the
   long lane's kernels; a decline is a gap handed to the row engine and needs its own item.
2. **The gate's 128-bit arm measures x86 below its width, not a 128-bit host.** Any 128-bit release
   count from this laptop (`VARKA-295.md` 9.8, 373 9) is read with that said; the arm runner of
   `varka-width-audit.yml` is where a 128-bit aarch64 warm-up question would be asked.
3. **`VectorMask.fromLong` on NEON** is refused by aarch64 C2 at every lane count without SVE
   bit permutation (source only); a kernel that builds its validity mask with `fromLong` pays the
   fallback there. Not measured.

The scratch directory of the session (`374b`) holds the run script, `cls374.py` (the 2-lane mask
classifier), `MaskProbe.java`, and per run the classification, the release lines, the refusal
counts and the compressed `-Xlog` file; the compile logs (0.7 and 1.9 GB) were deleted after the
numbers above were taken.
