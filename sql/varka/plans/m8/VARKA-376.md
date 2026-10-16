# VARKA-376: which of this milestone's operators are intrinsic, and allocation-free, where

*Row 376 of `m8/PLAN.md`, wave 0, added by the review of 16 October 2026 (10.3 item 3). Planned on
16 October 2026 at `8a50f61a368`. The row is a census, so its admission check is short: whether
the census exists already in some form, and what it can rest on.*

## 1. Where this came from

Two designs of this milestone already rest on operators the HotSpot match rules refuse on a CI
host: row 30's long-to-double and double-to-long conversions, which `x86.ad` refuses without
AVX512DQ, so not on the EPYC 7763 runners (10.1 item 4), and 375's `VectorMask.fromLong`, refused
on aarch64 without SVE2 BitPerm (10.1 item 2). The record's operator table
(`skills/vector-api-and-width.md`, "Every operator the plans rely on is one instruction") was
taken on one host, and its two-species finding - a box per iteration, 6.4x to 12.8x slower - is a
case 329's two arms can create in one JVM. Row 376 asks, before the lane rows design around them:
for each family of operator the milestone's rows use, is it intrinsic at each width the milestone
measures, and does a loop of it allocate.

## 2. The admission check, done

1. **No operator-by-operator census exists.** The operator table was measured in September 2026
   with an uncommitted `Probe.java` on the laptop. `VarkaWidthAuditSuite` (catalyst, forking
   `VarkaWidthAuditProbe` once per width under `-XX:MaxVectorSize=<bytes> -Xbatch` and
   `PrintIntrinsics`, writing `sql/varka/width_audit.json`; run on both runner classes by
   `varka-width-audit.yml`, results as artifacts) reads intrinsic refusals per kernel *shape*, not
   per operator, and reads no allocation. `VarkaAssemblySuite` asserts allocation per row
   (`maxBytesPerRow = 1.0`, `VarkaAssemblyProbe`, `VARKA-55.md`) for a few loops. So the row
   builds the census; it does not re-run one.
2. **What a verdict is.** By the skill's entry "An intrinsic refusal line is not a verdict about
   this compile; the inlining decisions are", the verdict per operator is read from
   `-XX:+PrintInlining` filtered to the intrinsic's symbol (`late inline succeeded` against
   `failed to inline (intrinsic)`), never from `PrintIntrinsics`'s `**` lines alone; and by
   "Assert it as a rate, not as sites", the allocation half is
   `ThreadMXBean.getThreadAllocatedBytes` around a thousand steady-state calls, at most one byte
   per row, a box being at least five. Both instruments exist and are cheap.
3. **The widths, named by species and not by datapath.** The laptop's preferred species is 512
   bits on a 256-bit datapath (the skill's "This machine's AVX-512 is 256 bits wide"); the EPYC
   7763 runners are AVX2, a 256-bit species; the 128-bit species is the forced `MaxVectorSize=16`
   on x86 and the native one on the Neoverse N2 runner. The row's "512, 256 and 128" are species
   widths, and the table says so in its header.
4. **The two-species case must not run in a shared test JVM.** `VarkaSpeciesGuard` fails any suite
   that defines a second species of a lane type where a test is about to load a class, and
   `VarkaOwnJvm` runs such a suite in a child JVM (VARKA-246). The census's two-species probe
   therefore runs forked, in a JVM of its own, as the assembly and cliff suites fork theirs.
5. **`m7` row 374 is closed "Done, no fix"**: x86 C2 lowers no two-lane mask, so at a 128-bit x86
   species every masked long operator is the fallback. The census records that per operator as the
   expected refusal at that cell rather than a finding, and every 128-bit timing it takes carries
   the release rate beside it (`m8/PLAN.md` 10.3 item 4).

**What still needs a run.** The census itself: the probe of 3.1 on the laptop at its preferred
species and at `MaxVectorSize=32` and `16`, and on both runner classes through a dispatched
workflow. Nothing about the row's premise is in doubt, so the run is the work rather than an
admission gate; the first command, once the probe exists, is

    taskset -c 3-5 build/sbt -Dvarka.ownJvm=true 'catalyst/testOnly *VarkaOperatorCensusSuite'

and the row is done when its table is committed with a verdict in every cell.

## 3. The design

### 3.1 One forked probe, one row per operator family

`VarkaOperatorCensusProbe` (catalyst test tree, Java), a `main` that runs one tight loop per
operator at the JVM's preferred species, each loop the operator alone over an array, warmed until
C2's loop compile settles, then reports per operator: the inlining verdict of the intrinsic it
reaches, and the allocated bytes per row over a thousand calls. `VarkaOperatorCensusSuite`
(JUnit 5, `@VarkaSuite(ownJvm = true)`) forks it at each width the host supports and writes
`sql/varka/operator_census.json` under a regen flag, as the width audit writes its file. The
families, from the row:

* byte and short widening, narrowing - operators: `B2I`, `S2I`, `I2B`, `I2S` by `convertShape`, and
  the widening load; for rows: 329, 330
* byte and short lanewise - operators: `ADD`, `SUB`, `MUL`, `SADD`, `SSUB`, compares, shifts at byte
  and short; for rows: 329, 330
* floating compares - operators: `EQ`, `LT`, `LE`, `IS_NAN` on `DoubleVector`, `FloatVector`; for
  rows: 341
* conversions - operators: `L2D`, `D2L`, `F2D`, `D2F`, `I2D`, `L2F`; for rows: 30, 343, 344
* lane width - operators: `I2L` and `L2I` by `convertShape`, expanding parts 0 and 1, contracting 0
  and -1; for rows: 28
* fused multiply-add - operators: `FMA` on doubles; for rows: 30, 346, 366
* math - operators: each `VectorOperators` math operator the family binds; for rows: 345, 356 to 358
* unsigned long compares - operators: `ULT`, `UGT`, `ULE`, `UGE` on `LongVector`; for rows: 359, 360
* the de-interleave - operators: two-vector `rearrange`, `selectFrom`, `compress` on `LongVector`;
  for rows: 347, 359
* masks - operators: `VectorMask.fromLong`, `toLong` per species; for rows: 331, 375, 27
* two species - operators: 329's arms: a byte species and the int species in one JVM, both orders of
  first use; for rows: 329

### 3.2 What the census produces

The table, committed beside the record's operator table in `skills/vector-api-and-width.md` as a
new section, one row per operator, one column per host and species: `I` (intrinsic, no
allocation), `I+A` (intrinsic, allocating), `F` (fallback), with the allocation per row where it
is not zero. And for every row of `m8/PLAN.md` whose design uses an operator the table marks `F`
or `I+A` on a host, a line in that row's plan (or its row text, when it has no plan yet) naming the
form it takes on that host - the done-when's second half.

### 3.3 What is deliberately unchanged

* No emitter or kernel change, and no decision of another row's design: the census says where an
  operator is intrinsic; 30, 329, 343 and the rest choose their forms from it.
* The width audit and the assembly suite keep their scope; the census is per operator, they are
  per shape.
* `m7` row 374's two-lane x86 masks: recorded as a known `F`, not re-investigated.

### 3.4 Registered op counts

None: this row emits no kernel.

## 4. Files

* `sql/catalyst/src/test/java/.../codegen/varka/VarkaOperatorCensusProbe.java`: new: the
  per-operator loops
* `sql/catalyst/src/test/java/.../codegen/varka/VarkaOperatorCensusSuite.java`: new: forks the probe
  per width, writes the file
* `sql/varka/operator_census.json`: new: the census's output
* `.github/workflows/varka-width-audit.yml`: a step running the census on both runner classes
* `sql/varka/skills/vector-api-and-width.md`: the table, as a new section; `SKILLS.md` regenerated

## 5. Tests, and what each is for

1. **A planted fallback is read as `F`**: the probe's gather at a forced 128-bit species on x86,
   which VARKA-165 recorded as refused on part of the pool, or `fromLong` under `UseSVE=1` on the
   Arm runner (375); catches a census that reads every operator as intrinsic.
2. **A planted box is read as `I+A`**: the two-species loop in its boxing order, the case the
   skill measured; catches an allocation reading that cannot see a box.
3. **The suite's own JVM**: `VarkaSpeciesGuard` stays green in the shared JVM while the census
   runs; catches the census polluting the other suites.

## 6. The measurement

The census measures verdicts, not throughput; it times nothing it reports as a speed.

### 6.1 Predictions, registered before the run

1. **The conversions `L2D` and `D2L` read `F` at the 256-bit species on the EPYC 7763 runner**
   and `I` on the laptop, by `x86.ad`'s AVX512DQ rule (10.1 item 4).
2. **`fromLong` reads `I` on all three hosts at their defaults**, by 375's section 2.
3. **The two-species loop reads `I+A` in at least one order** of first use, as the skill's
   measurement found for `selectFrom`.
4. **The math operators read `I` wherever the library binds them** and `F` for `POW` at the AVX2
   runner's 256-bit species (the canary's finding).
5. **The suite takes under 10 minutes per host**, forking one JVM per width: about forty
   operators, each warmed for a few seconds, then doubled by the calibration table's observation
   that harness reads cost more than the kernel's arithmetic (VARKA-274a's tier witness, VARKA-243's
   suite); stated so the workflow step's timeout can be set.

## 7. Risks

1. **A loop of one operator is not a kernel.** C2 may lower an operator alone and refuse it in
   context; the census is a floor, and the rows' own assembly checks (`dev/varka_emit.sh --asm`)
   stay the verdict for a kernel.
2. **The runner pool varies** (`sql/varka/HARDWARE.md`): the x86 runner's column names its CPU
   from the run, and `expected-cpu` pins the EPYC 7763 where a cell's verdict depends on it.
3. **The Arm runner is one core class with BitPerm**; the `UseSVE=1` column is a simulation, as
   375 says.

## 8. Sequencing

1. This plan.
2. The probe and the suite, run on the laptop at its three widths, the planted cases of section 5
   green.
3. The workflow step, the two runner columns, the table committed with `SKILLS.md` regenerated.
4. The rows whose design meets an `F` or `I+A` cell get their line (section 3.2).

Explicitly out of this row: choosing any row's form, kernel-level assembly checks, and fixing any
refusal it finds.

## 9. Outcome

*To be written when the work lands.*
