# VARKA-375: `VectorMask.fromLong` where the hardware refuses it

*Row 375 of `m8/PLAN.md`, wave 0, added by the review of 16 October 2026 (10.1 item 2). Planned
on 16 October 2026 at `8a50f61a368`. Half of the row's first question was answered the same day by
the canary run that the review asked for; section 2 records that, and the step that answers the
other half.*

## 1. Where this came from

JDK 25 intrinsifies `VectorLongToMask`, the node behind `VectorMask.fromLong`, on aarch64 only
when the core has SVE2 BitPerm (`aarch64_vector.ad` 217 and 218), and every Varka kernel builds its
validity masks with that call. On a NEON-only core (Apple's, Graviton2) or an SVE1 core
(Graviton3), `fromLong` runs the Vector API's Java fallback, which
`skills/vector-api-and-width.md` describes as a second implementation. Row 331's boolean leaf
(`m8/PLAN.md` 2.5) is the same call over a data word, and 1.1 item 5 keys the math bounds by the
Arm library, so the milestone measures on Arm. The row asks: does the Arm runner have BitPerm,
what does a validity-reading kernel allocate where `fromLong` falls back, and, if it falls back
somewhere the milestone measures, a construction every host intrinsifies.

## 2. The admission check, done

What the record and the source settled:

1. **The Arm runner has BitPerm** (canary run 38884285755, 16 October 2026,
   `dev/varka_canary/mathlane-aarch64-neoverse-n2.txt` and the skill entry "The JDK's SLEEF sends
   `exp` and `pow` to infinity early, SVML does not, and the Arm runner has BitPerm"):
   `svebitperm` in `/proc/cpuinfo` and in the JVM's feature line, and `fromBitsCoerced` inside
   `fromLong` reads `(intrinsic) late inline succeeded` wherever the species is a constant, with no
   `** not supported` line. Emitted kernels load their species by `getstatic` of a `static final`,
   always a constant. So **on every host the milestone measures today, `fromLong` is intrinsic**:
   the Neoverse N2 runner by BitPerm, and x86, where `x86.ad` (1876 to 1882) admits
   `VectorLongToMask` under AVX with BMI2, which the EPYC 7763 runners and the laptop have.
2. **The emitter's call sites**, the surface an alternative would replace: `VarkaBodyEmitter`
   195 (an empty guard mask per accumulator), `VarkaVectorWalk` 355, 672, 738, 772, 780 and 974,
   `IntRangeOps` 81, 82 and 101, `SelectionVectorOps` 136 and 172, `VarkaVectorTemplateWarmup`
   214 and 248; in the engine module `ChronoVectorOps` and `DateVectorOps` (the reference kernels).
   The descriptor is `VarkaDescriptors` 147.
3. **Where a host without BitPerm comes from.** Neither GitHub runner class lacks it, and the skill
   entry ends on that. HotSpot gives one on the same runner: `vm_version_aarch64.cpp` (JDK 25 GA
   tree, lines 631 to 634) clears `CPU_SVEBITPERM` and `CPU_SVE2` from the feature set whenever
   `UseSVE < 2`, and the match rule reads `VM_Version::supports_svebitperm()`. So
   **`-XX:UseSVE=1` on the Neoverse N2 runner refuses `VectorLongToMask` as an SVE1 core does**,
   and `-XX:UseSVE=0` gives a NEON-only JVM (which also loses every other SVE lowering, as a
   NEON-only core would). Both are simulations, of the JVM's feature set and not of a Graviton3's
   or an M-series core's timing; the repo has never set `UseSVE` (only `PrintFlagsFinal` reads of
   it, `benchmark.yml` 340, `varka-width-audit.yml` 84, `dev/varka_datapath.sh` 84). A real
   NEON-only host - an Apple Silicon machine, which `m7` row 374 notes behaves differently from
   x86 at two lanes - would confirm the timing; none is in the runner pool.
4. **`m7` row 374 is closed "Done, no fix"** (`m7/PLAN.md` 684, `VARKA-374.md` 9): at a 128-bit
   x86 species, two-lane long masks run the Java fallback because x86 C2 lowers no two-lane mask,
   and the rate of releases cannot fall below about 10 in 100 without a mask-free two-lane kernel.
   That is an x86 fallback of the same family as this row's, for a different reason (the lane
   count, not a CPU feature). Every 128-bit number this row records therefore carries the release
   rate beside it, as `m8/PLAN.md` 10.3 item 4 asks; there is no fix to wait for.

**What still needs a run, and what admits the build half of the row.** On the Arm runner, through
the canary workflow (`varka-canary.yml`, dispatched with `canary=MathLaneProbe`, whose third pass
runs the `fromlong` mode under `-XX:+PrintIntrinsics`), once with the defaults and once with
`-XX:UseSVE=1`, plus a validity-reading kernel's allocation rate in each (the assembly suite's
measure, `VarkaAssemblyProbe` with `getThreadAllocatedBytes`, at most one byte per row,
`VARKA-55.md`). The workflow needs an input for extra JVM options to pass `UseSVE`; that is this
row's first change. Dispatching it is the owner's or the implementing agent's step, not this
plan's.

* Under `UseSVE=1` the `fromLong` call should read `failed to inline (intrinsic)` with
  `lambda$fromLong$0 ... inline (hot)` (the fallback inlined), as the skill entry separates the
  kinds. If the fallback's kernel stays at or under one byte per row and its time is within the
  canary's band of the default's, the fallback costs nothing that matters, the row closes on the
  record, and nothing is built.
* **The build half is admitted** if the fallback allocates per iteration (five bytes per row or
  more, the box signature) or the validity-reading kernel is slower by more than the band. Then
  the alternative of 3.1 is built behind an option.

## 3. The design

### 3.1 The mechanism, if admitted

A mask from a long word, built from operators every host intrinsifies: broadcast the word to a
long vector, shift each lane right by its lane index (an index vector, a per-kernel constant),
`AND` with one, and compare with zero. For an int species of more lanes than a long species
holds, the word is split per long half, which is the two-part shape `VARKA-28.md` already uses for
widening. One helper in the emitter, `VarkaMaskFromWord`, emitted at each site of section 2 item 2
in place of the `fromLong` invocation, behind a `VarkaEmitOptions` switch `maskFromWord` with its
`VarkaEmitOption.TABLE` entry, so that `VarkaEmitOptionSuite`, the option matrix and the IR fuzzer
take it the day it lands. The default stays `fromLong` on every host the milestone measures,
since section 2 item 1 finds it intrinsic on all of them; the switch is chosen per host at kernel
compile time only if a later host needs it (a NEON-only executor class), which this row records and
does not build.

### 3.2 What is deliberately unchanged

* The default emission everywhere: `fromLong` stays, and `emitted_bytes.json` does not move under
  the x86 defaults.
* The hand-written ops (`IntRangeOps`, `SelectionVectorOps`) and the engine's reference kernels
  keep `fromLong`; if the emitter's switch wins on a host, porting them is a follow-up row, named
  in section 9, not this one.
* Row 331's boolean leaf is 331's; it uses whatever this row leaves as the default, and 331's
  done-when ("the mask built from the data word by a construction intrinsic on every host the
  milestone measures") is met by section 2 item 1 on today's hosts.
* `m7` row 374's two-lane x86 mask fallback: closed there, not reopened.

### 3.3 Registered op counts

Per mask, against `fromLong`'s one `VectorMask` invocation: one broadcast, one shift, one `AND`,
one compare - four `LongVector` invocations and no `VectorMask` one at a long species, twice that
for an int species split in halves. Read from `dev/varka_emit.sh "year(d)" --options
maskFromWord=true` and the defaults' table once the switch exists; asserted by an op-count test.

## 4. Files

* `.github/workflows/varka-canary.yml`: an input for extra JVM options, for the `UseSVE` runs
* `dev/varka_canary/MathLaneProbe.java`: the `fromlong` mode's allocation reading (or a sibling
  probe)
* `dev/varka_canary/mathlane-aarch64-neoverse-n2-usesve1.txt`: new: the `UseSVE=1` run's record
* `sql/varka/skills/vector-api-and-width.md`: the BitPerm entry extended with the `UseSVE` finding
* *if admitted:* `VarkaEmitOptions.java`, `VarkaEmitOption` table: the `maskFromWord` switch
* *if admitted:* `VarkaBodyEmitter.java`, `VarkaVectorWalk.java`, new `VarkaMaskFromWord.java`: the
  alternative at each site
* *if admitted:* `sql/varka/emitted_bytes.json`: variant rows only

## 5. Tests, and what each is for

1. **The canary record under `UseSVE=1`**: the inlining lines show the fallback, so the simulation
   is known to reach the refusal it claims; without it the allocation reading means nothing.
2. **If admitted, the switch's answers**: the coverage differential and the IR fuzzer under
   `maskFromWord=true` at both widths, every validity pattern of `VARKA-138.md`; catches a lane
   index off by one in the shift, which an all-valid batch would not show.
3. **If admitted, the bytes oracle under the defaults**: unchanged; catches the switch leaking
   into the default emission.
4. **If admitted, the allocation rate**: the assembly suite's one byte per row, for the switch's
   kernels at both widths.

## 6. The measurement

The canary's `fromlong` mode and a validity-reading kernel (`year(d)` over a nullable date column,
the masked path driven by `--nulls`) on the Arm runner, default and `UseSVE=1`, and if admitted the
switch's kernel against the default on x86 (EPYC 7763 runner and the laptop) and on Arm, both
widths, with the 128-bit release rate beside each 128-bit number (section 2 item 4).

### 6.1 Predictions, registered before the run

1. **`UseSVE=1` makes `fromLong` the fallback** on the N2 runner, by section 2 item 3's source
   reading; a run that shows the intrinsic means the feature strip happens later than the matcher
   reads it, and the simulation is not valid.
2. **The fallback allocates** in a validity-reading loop, at least the five bytes per row of the
   box signature. The fallback (`VectorMask.fromLong`, `VectorMask.java` 240 to 259 in the JDK
   25 GA tree) allocates a `boolean[laneCount]` and calls `fromValues` for every word except
   zero (an all-valid word is not special: the bits are masked to the lane count first, so it is
   `0xFFFF` at sixteen lanes and not -1), and whether C2 scalar-replaces that array inside an
   emitted loop is what the run measures. This is the prediction that would admit the build half.
3. **The four-operator construction ties `fromLong` on x86** within the band at both widths,
   since `VectorLongToMask` itself lowers to a short sequence there; the switch exists for hosts
   without the intrinsic, not for x86.

## 7. Risks

1. **`UseSVE=1` is not a Graviton3.** It strips the features the matcher reads; the timing is the
   N2's. Test 1 proves the refusal, and the record says it is a simulation.
2. **The construction's index vector is a second constant per kernel**, a new literal-like slot;
   if it moves the shape key it must be in the key, which the option's table entry ensures.
3. **The canary workflow's extra-options input** is used by other probes; it defaults to empty so
   their runs do not change.

## 8. Sequencing

1. This plan.
2. The canary input and the `UseSVE=1` run; its record and the skill entry committed, section 2's
   verdict recorded here.
3. If admitted: the switch behind the option, the tests of section 5, then the measurement of 6
   and 6.1 scored. The default does not flip unless a measured host needs it.

Explicitly out of this row: porting the hand-written ops and the engine's kernels, any per-host
switch selection at run time, `m7` row 374's two-lane x86 fallback, and 331's boolean leaf.

## 9. Outcome

*To be written when the work lands.*
