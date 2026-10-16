# VARKA-373: The template warm-up feeds only the preferred width, so 4-lane kernels are not helped

## 1. Where this came from

`VARKA-295.md` 9.8 (15 October 2026): the warm-up of the shared Vector API templates (#827) removes
the deadline releases at the preferred species and at 16 lanes (0 releases in 3 runs against 19
to 59 without it), but at `lanesOverride=4` it makes no difference: 183, 141 and 144 releases with
it, 136, 139 and 148 without, 25 to 32 minutes a run in the 400-composition arm. A kernel at a
narrower species reaches the same shared templates, whose profiles the warm-up fed only with
preferred-width receivers. 9.8 lists what is not known: why 4 lanes is not fixed (no
deoptimization log was taken for a 4-lane run), whether feeding the narrower species would fix
it, and whether production ever emits at 4 lanes.

## 2. The admission check, done (16 October 2026)

The check of `VARKA-295.md` 9.5 and 9.6 was run on the 4-lane warm arm, and it names a cause that is
not the one 9.6 found at the preferred species. The change of 3.1 was therefore not written for
the branch (an experiment was, section 9), and the task stops here with the record.

**A correction first.** 9.8 says the machine's preferred species is 256 bits. On this host
(AMD Ryzen AI 9 HX PRO 370, AVX-512) `IntVector.SPECIES_PREFERRED` is `S_512_BIT`, 16 int lanes,
and `LongVector.SPECIES_PREFERRED` is 8 long lanes (read from a JVM started with the test options).
So 9.8's "16 lanes" cell was the preferred species itself, and 4 lanes (128-bit int vectors, 256-bit
long vectors, `Lane.speciesField`) is two steps below it; 8 lanes was not measured there.

**What was run.** The 400-composition warm arm of `VarkaSparkFuzzSuite` ("both sides of the
warm-up handover", seed 20261008, `lanesOverride=4`, warm-up on, the shipped 300 rounds at the
preferred species) in a JVM started by hand from `dev/varka_matrix.sh --module sql --list`'s
classpath and `jvm.opts`, pinned with `taskset`, with `-XX:+LogCompilation`,
`-XX:+PrintTieredEvents` and `-Xlog:jit+compilation=debug,deoptimization=debug`. Two runs: one on
cores 0-5 with the JFR evidence on (1,679 s), one on cores 6-11 without it (1,707 s). Both printed
the same counts: 379 COMPILED, 155 RELEASED (all at the deadline), 151 distinct kernel classes.
Each class was classified from the JIT's own record as in 9.5: cause 1 is a loop method with 10 or
more uncommon traps or 3 not-entrant marks, cause 2 a compile failure, cause 3 a loop method with
no C2 code, cause 4 C2 code installed and still allocating.

| classification of the 151 released classes | run 1 | run 2 |
|---|---|---|
| cause 1, deoptimization cycle | 1 (3 marks on one method) | 0 |
| cause 2, C2 bailout (`COMPILE SKIPPED`, failure element) | 0 | 0 |
| cause 3, no C2 loop code | 0 | 0 |
| cause 4, compiled and still allocating | 150 | 151 |
| most uncommon traps on one kernel method | 9 | 8 |

**It is not 9.6's compile-wide sums.** 9.6's gate is `<observe trap='class_check' ...
ccount='N'/>` (the compile-wide class-check count against 100) or the decompile count against 201
inside the kernel's own C2 compile. In the loop-method compiles of the released classes there is
none: 0 of 424 compiles in run 1 and 0 of 425 in run 2 (a few appear in the small epilogue
methods, 14 of 207 `epilogueDense` compiles in run 1, and in clean classes too).

**What is there instead: a virtual call at one shared call site.** 130 of the 151 released classes
have a `<virtual_call>` in a loop-method C2 compile (142 of 215 `loopDense` compiles and 148 of 209
`loopMasked`, run 1), against 27 of the 201 other classes (37 of 379 and 43 of 345). 1,047 of the
1,049 virtual calls in run 1 are the same one: `IntVector.add(Vector)` at bci 5, the call of the
abstract `lanewise`, and the compile log says why it stayed virtual:
`<observe trap='bimorphic_or_optimized_type_check' count='4' .../>` just before it, that is, the
site's trap count is 4, `PerBytecodeTrapLimit`'s default, so C2 may no longer put an uncommon trap on
the miss path of the receiver check and keeps a real call there; the vector that flows out of that
call is a non-exact `IntVector`, and the allocation of 9.6 follows. The remaining virtual calls
(`lanewise` at bci 17, 40 and 43 into `Int128Vector`, `intoMemorySegment`) are the same kind and
have no receiver profile.

The site's profile in a sampled released compile (class `w814aaf58f7391547`, `loopDense0`, 25.7 s
into run 1): 42,159 calls, 5,636 recorded as `Int128Vector`, 93 as `Int512Vector`, so most calls
are in neither of the two classes the profile keeps. The timeline in run 1: the warm-up's rounds at
the preferred species end by 17 s, C2 compiles `IntVector.add` on its own at 17,164 ms (compile
17471, from a profile of `Int512Vector` receivers), a receiver of another class reaches that
compiled `add` at 19,776 ms (the 128-bit kernels' calls) and it traps four times in 1 ms
(`bimorphic_or_optimized_type_check`, `maybe_recompile`), and every later compile that inlines `add` sees `count='4'`. That is 9.6's mechanism with a different gate: the
shared template's MethodData holds a trap recorded for a receiver class of another species, and the
per-bytecode limit, not the compile-wide sum, turns it into a permanent virtual call. The rate is
not constant: releases per 50 compositions in run 1 are 3, 12, 20, 22, 20, 27, 28 and 23, so the
first 100 are better than the rest.

**Would the change of 3.1 have fixed it? No: three experiments say it would not (section 9).**

**Whether production can emit at 4 lanes: no, on this evidence.** `Lane.emitLanes` bakes
`options.lanesOverride()` when it is not 0 and the JVM's preferred lane count otherwise, and
`VarkaEmitOptions` says production always leaves it 0, so a host emits at its preferred species and
the warm-up feeds that species. `lanesOverride=4` on a 512-bit host is a matrix-only
configuration. What production does reach is the case of a 128-bit preferred species (Apple
Silicon, `-XX:MaxVectorSize=16`, which the gate uses), and section 9 shows that case is not fixed by
the warm-up either.

## 3. The design

### 3.1 Warm the species a kernel will use

If step 1 shows the same class-check sums, `VarkaVectorTemplateWarmup` runs its rounds at each
species a host can bake (the preferred one and the narrower ones the emitter's `lanesOverride`
and the host's maximum vector size allow), on the same `varka-kernel-warmup` thread, once per
JVM, behind the existing property. Otherwise the design follows from the cause found.

### 3.2 What is deliberately unchanged

The preferred-species warm-up, its 300 rounds and its disable property; the query-path vector
code (`IntRangeOps`, `SelectionVectorOps`), which `VARKA-295.md` 9.7 leaves open.

### 3.3 Registered op counts

None; no emitter change.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen/VarkaVectorTemplateWarmup.java` | the species the rounds run at |
| `sql/varka/plans/m7/VARKA-295.md` | 9.9, the result |

Neither was changed (section 9): 295 carries a one-paragraph correction of 9.8 instead.

## 5. Tests, and what each is for

* The warm-up unit test at each species: the shared templates compile with no class-check trap
  recorded (the compile log), which a preferred-only warm-up would fail at the narrow species.
* The 400-composition warm arm at `lanesOverride=4`, 3 runs: deadline releases.
* The same arm at the preferred species and at 16 lanes: still 0, so the new rounds do not undo
  the old fix.

## 6. The measurement

Deadline releases and COMPILED counts in the 400-composition warm arm at `lanesOverride` 4, 16 and
the preferred species, warm-up on, three runs each, arms alternating over core sets as in 9.8;
the steady-state throughput driver of 9.8 at the preferred species, to show the extra rounds cost
nothing there.

### 6.1 Predictions, registered before the run

1. At 4 lanes the releases fall from 136 to 183 to under 10 per run, if step 1 names the trap sums;
   if it names something else, the prediction is void and the cause is the finding.
2. The extra warm-up costs under 300 ms more on the worker thread, off the query's path.
3. The preferred-species and 16-lane results do not change.

## 7. Risks

1. The cause at 4 lanes is not the trap sums, and warming more species does nothing; section 2's
   log shows it before any code is written.
2. Warming species the host cannot run throws; the existing `Throwable` guard in the warm-up
   catches it, and a test checks that an unsupported species is skipped.

## 8. Sequencing

The JIT-log classification first (a record, no code); the species loop behind the property; the
measurement; the default and the plan's 9.9 last.

## 9. Outcome (16 October 2026): the cause is not 9.6's, and the species change does not help

The task ends at its stopping rule: step 1 (section 2) did not name the trap and decompile sums,
and the one change tried, warming the narrower species, made things equal or worse. Nothing is
changed in the product code. The experiment was a patch to `VarkaVectorTemplateWarmup` (a `Group`
per lane count, the rounds interleaving the groups, the lane counts forced by a test-only system
property); it is not in the branch.

**The experiment.** The warm arm, `lanesOverride=4`, seed 20261008, the first 100 compositions
only (a run is 2 to 11 minutes instead of 28), one JVM per run on 6 pinned cores, up to four at
once. The numbers are deadline releases (every one of them at the deadline; none before it). A
"warm-up not run" row is a run whose experimental warm-up threw in its first line (a species
built with `Integer.class` instead of `int.class`, an index past a 2-lane vector) and was absorbed
by the guard, so it is a second measurement of the arm without the warm-up.

| 4 lanes, first 100 compositions | releases per run |
|---|---|
| warm-up off (`-Dvarka.warmup.vectorTemplates=false`), 2 runs | 31, 18 |
| warm-up not run (the experiment threw), 5 runs | 26, 31, 31, 27, 25 |
| shipped warm-up, preferred species only, 4 runs (two are the first 100 of section 2's) | 14, 14, 15, 16 |
| experiment: 128-bit species only, 1 run | 26 |
| experiment: preferred and 128-bit species interleaved, 1 run | 62 |

So the shipped warm-up helps the first 100 compositions a little (14 to 16 against 18 to 31, and
the off arm is itself wide) and the full 400 not at all: 155 releases in each of the two logged runs
against 9.8's 136, 139, 148 without it. Feeding the 128-bit species alone is the same as no
warm-up, and feeding it next to the preferred one is clearly worse (62, 668 s). A site whose
profile holds two classes is a bimorphic call, and a third class, which the log's profile suggests
is present (section 2), makes it a virtual call, so more species at the shared sites is the wrong
direction. These are single runs of the experimental arms, and the off arm shows 18 and 31, so only
the 62 and the 14 to 16 are clear of that spread. The full-size arms of section 6 were not run:
there is no change to measure, and the 3 x 3 runs of 28 minutes each would have measured the
shipped code.

**Other lane counts and the 128-bit host (shipped warm-up, first 100 compositions).**

| configuration | warm-up on | warm-up off |
|---|---|---|
| 16 lanes (the preferred species here) | 0 | 8 |
| 8 lanes | 7 | 10 |
| 4 lanes | 14, 14, 15, 16 | 31, 18 |
| `-XX:MaxVectorSize=16`, no override (the gate's setting: a 128-bit preferred species) | 43, 41, 41, 43 | 46, 45 |

The last row is the one that matters for production: where the preferred species is the narrow one,
the warm-up feeds exactly that species and still does nothing (about 42 of 100 compositions
released with it, about 45 without). One of the four runs (43, with the JIT log) was classified as
in section 2, and JFR allocation samples were taken in another (41): 27 distinct released classes, 3 with the deoptimization
signature (up to 12 traps and 5 not-entrant marks on a method), 24 compiled, and only 2 of them with
a virtual call in a loop method. So at 128 bits it is not the `IntVector.add` site: the JFR
allocation samples name `Long128Vector.maskFromArray`, `Long128Mask.prepare`, `Int128Mask.bOp` and
`LongVector.broadcastBits` inside kernel loops, and the class-check gate of 9.6 (`ccount`) appears
in 42 of the 103 `loopDense` compiles of the classes that did not release. It is a different
cause on the host that matters, and it is not diagnosed here.

**Not done, and why.** Section 6's measurement of the fix (nothing to measure), the steady-state
driver of 9.8 (its scratch directory was wiped by a reboot; skipped), the unit test of section 5
(there is no change for it to fail without), and the 128-bit class-by-class record.

**What the record leaves.**

1. 128-bit hosts: a release rate near 42% with the warm-up on, by the gate's own configuration.
   This is the production-relevant open item and wants its own task: classify the 128-bit
   releases from the log as section 2 does (the sites named above), then decide.
2. At 512 bits the warm-up is right (0 of 100 at 16 lanes, 0 of 2,000 in 9.8 3) and a narrower
   override is a matrix-only configuration; if the matrix's 4-lane arm needs to stop costing 28
   minutes, a smaller deadline for that arm (or its deletion from the warm arm) is a test choice,
   not a runtime change.
3. Whether a fix at the shared site is possible at all: the per-bytecode trap count of
   `IntVector.add` bci 5 is JVM-wide state of the JDK's own MethodData. The options are a flag
   (`PerBytecodeTrapLimit`, which 9.5's run t6 set together with the method limit at 100000, and
   which gave 151 releases at 8 cores, so not a fix there) or keeping every species of the JVM to one, as
   production does.

The scratch directory of the session (`373`) holds the two classified logs' extraction scripts
(`classify.py`, `census.py`, `cls2.py`), the run script and the experiment's diff; the logs
themselves, 2 GB each, were deleted after the numbers above were taken.
