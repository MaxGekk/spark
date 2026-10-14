# VARKA-327: The Spark-level fuzzer with the warm-up on

## 1. Where this came from

Row 327 of `m7/PLAN.md`, found on 13 October 2026 by VARKA-274's admission check (`VARKA-274.md`
3.5 and 3.7, item 3). `VarkaSparkFuzzSuite` holds random compositions of the coverage table's rows
to vanilla Spark, but every session it runs in turns the kernel warm-up off, so none of its
compositions takes the path production takes: a new shape's first batches on Spark's row path while
a background thread gets the kernel compiled, and the later ones on the compiled kernel. The row
is done when an arm of the fuzzer runs with the warm-up on, nightly, its answers compared with
vanilla Spark as the other arm's are.

## 2. The admission check, done

Checked on 14 October 2026 against `master` at `90b8871b705`, by reading the code and running the
arm itself (3.1) before this plan was committed, on the laptop at a load of 2 to 4 from other
agents' builds, on six cores. Times here are that machine's and rough.

**How the fuzzer turns the warm-up off.** `VarkaSharedSessions.newSession` sets
`spark.sql.codegen.varka.warmup.enabled=false` on `varkaSpark` and `disabledSpark`, with the
reason in a comment: a comparison of a shape's first query would otherwise compare the row engine
with itself. Nothing in `VarkaSparkDifferential` or the suite turns it back on.
`VarkaWarmupEndToEndSuite` is the one suite that does, per test, on fixed shapes, and pins which
path served each batch.

**The warm-up's switches.** Three, all needed: the session's
`spark.sql.codegen.varka.warmup.enabled`; the JVM's `VarkaKernelWarmup.canWarm()` (the allocation
sampler is supported and the compiler directive keeping C1 off warmed kernels is installed); and
`VarkaShapeKey.warmed`, which gives a warmed kernel a class of its own, so the off arm's shapes and
a warmed arm's never share a cache entry. A shape's `VarkaKernelWarmth` moves COLD, WARMING, then
COMPILED or RELEASED; both of the last two serve batches on the kernel. The verdict is the
allocation probe's; a shape without one is released `deadlineSeconds()` after its warm-up was
queued, 60 s in production, `-Dvarka.warmup.deadlineSeconds` in tests. `awaitIdle` waits for the
queue to drain.

**How the nightly runs the fuzzers.** `dev/varka_nightly.sh`'s `sparkfuzz` step runs the suite and
the reproducer replay at 50,000 compositions with the day's seed, under a 170-minute test timeout;
the warm-up is off throughout.

**What VARKA-274a learned that bears on this.** 3.5 left the Spark-level fuzzer out on purpose,
since a warmed comparison needs each query run twice with a wait between, as the end-to-end suite
does. 3.7 found that 3 of 40 small drawn kernels entered the deoptimization cycle on the warmed
path (VARKA-326) and that the warm-up's verdict comes before the epilogues are compiled in 32 of 39
kernels. The first says some compositions will never get a verdict and will wait out the deadline.

**Probe 1: 40 compositions at the production deadline** (the arm of 3.1, seed 20261008): 22 s for
the test, 50 warm-ups all COMPILED, 32 compositions handed from the row path to a kernel, no
disagreement.

**Probe 2: 400 at the production deadline**, stopped after about 104 compositions and 10 minutes:
7 warm-ups had been released, each after the full 60 s, and those waits were most of the time.
At that rate 2,000 compositions would spend about two and a half hours waiting.

**Probe 3: 400 at a 10 s deadline** (the arm as built): 8 min 35 s, about 1.3 s a composition.

| | seed 20261008, 400 | seed 7, 120 |
|---|---|---|
| warm-ups COMPILED / RELEASED | 477 / 35 (6.8% released) | 154 / 2 |
| runs on the row path while warming | 474 | 142 |
| runs on a kernel after the handover | 304 | 89 |
| compositions handed over | 304 (76%) | 89 (74%) |
| no Varka node in the plan | 34 | 6 |
| runs over other data, of which a new shape | 400, 21 | 120, 7 |
| waits for the warm-up that timed out | 0 | 0 |
| disagreements | 0 | 0 |

The seed-7 run was repeated without the JIT recording of 3.1: 69 s against 72 s with it, no
difference this machine's noise can show. Of seed 7's 31 compositions that never reached a kernel,
6 had no Varka node, 13 raised on both sides (compared by error class) and 12 had every cached
batch pruned by the scan's statistics before a Varka node saw one; none had a batch declined.

**What the 35 releases show (evidence for VARKA-295).** In all 35 the C2 compile queue was empty at the
release. In 24 the recording holds no deoptimization of the kernel; in 11 it holds
`profile_predicate` or `unstable_if` traps in a masked loop or `run`, VARKA-274a's cycle (VARKA-326). The
code list names methods with no code in use in 31 of them: the `runDense` or `runMasked` driver
overloads in 30, a masked loop and its epilogue in 8; in 4 every method had code. In 30 the warm-up
thread's allocation samples hold interpreted frames of a loop method the code list shows compiled
at tier 4 and in use; but the samples cover the last 20 s, the whole warm-up and its interpreted
start included, so they do not by themselves show a loop running interpreted after its compile
landed. What the releases do show is `VARKA-295.md` 9.3's signature - an empty queue, most of the
kernel compiled, the probe still allocating - in a 10-minute JVM on a lightly loaded machine, not
only in the module's 70-minute one, at about one small kernel in fifteen. The arm reports each
release's account in the test's output (3.1) and does not fail on it.

**What the check would have rejected.** A warmed arm whose runs all took the row path would
compare the row engine with itself; three in four compositions hand over, so the arm compares
kernels. A cost per composition near the production deadline's would have ruled out a nightly
arm of any volume; at 10 s it is 1.3 s.

## 3. The design

### 3.1 The warmed arm

A test in `VarkaSparkFuzzSuite`, "random compositions agree with the row engine on both sides of
the warm-up handover", over the main test's compositions (`draw(rows, seed, it)`, rectified),
`-Dvarka.sparkfuzz.warmupIterations` of them (default 8), `-Dvarka.sparkfuzz.warmupOnly=<it>`
replaying one. Each composition starts from an empty shape cache, so every one takes a handover.
`VarkaSparkDifferential.warmedDisagreement` does the work:

1. The fixture is cached in both sessions, and `varkaSpark` runs with the warm-up on.
2. The query runs on `varkaSpark` until no Varka node of it waited on a warm-up, at most four
   runs, waiting for the warm-ups between runs: a filter's warm-up comes first, and a projection
   over it claims its own only once the filter serves Arrow batches. Every run is compared with
   the row engine's answer (rows, or the error class) and the pivot check.
3. The warmed shape then runs once more over other data with the same nulls and new values
   (`VarkaSparkFuzz.laterData`), so that code C2 compiled from the warm-up's copy of one batch
   meets a batch it was not compiled from, as production's later batches do.

A disagreement's kind names the side of the switch: "on the row path while the kernel warms",
"after the handover to the kernel", or "over other data after the handover". Nothing reads the
verdict or its timing: a released kernel serves batches as a compiled one does, so a release is
reported, never failed, and this arm cannot become a second flaky warm-up test. Each release is
reported in the test's output with the outcome, `VarkaKernelWarmup.lastReleaseJitState()` and,
while the JIT recording runs (`VarkaJitEvidence`, on unless `-Dvarka.sparkfuzz.jitEvidence=false`),
its account of the kernel; the recording is restarted after each.

The arm's deadline is `-Dvarka.sparkfuzz.warmupDeadlineSeconds`, default 10 s. A small kernel
that compiles gets its verdict in under a second; one that does not waits the deadline out, and
2's 6.8% made that wait two thirds of probe 3's time at 10 s. The shorter deadline weakens a
release as row-295 evidence in one way only: a kernel whose compile is still queued at 10 s would
read as a release, which the account's compile queue shows, and in probe 3 it was empty every
time.

A disagreement is not shrunk: each smaller case is a new shape and a new warm-up. The reproducer
is written as the main test's are, with the fixture of the run that disagreed and the arm's replay
flags in its note.

### 3.2 What is deliberately unchanged

- **The forced declines and the ternary partition** stay in the main arm: each force is a new
  emission and so a new warm-up, and the partition three more queries, each potentially a new
  shape. The pivot check is kept, since it costs one row-engine query.
- **The shared sessions keep the warm-up off**, so every other suite compares the kernels'
  answers from a shape's first batch.
- **`VarkaKernelWarmup` and its verdict.** VARKA-295 owns why a kernel misses its verdict, VARKA-326
  the deoptimization cycle; this arm only reports what it meets.
- **`VarkaWarmupEndToEndSuite`**, which pins which path served each batch on fixed shapes.

### 3.3 Registered op counts

None: no emitted code changes.

## 4. Files

| file | what |
|---|---|
| `sql/core/src/test/scala/.../execution/VarkaSparkDifferential.scala` | `warmedDisagreement`, its counters, `withVarkaWarmup`, `withWarmupDeadline` |
| `sql/core/src/test/scala/.../execution/VarkaSparkFuzz.scala` | `laterData` |
| `sql/core/src/test/scala/.../execution/VarkaSparkFuzzSuite.scala` | the warmed arm and the planted-fault test; the planted search shared with the existing test |
| `dev/varka_nightly.sh` | the `sparkwarm` step |
| `sql/varka/skills/testing-and-debugging.md`, `SKILLS.md` | how to run the arm and read a release |

## 5. Tests, and what each is for

1. **The warmed arm** (3.1). Every run of every composition against the row engine, both sides
   of the handover and other data after it. At 50 compositions or more it also asserts that at
   least half were handed over to a kernel (2: three in four at two seeds), so an arm whose
   shapes stopped reaching the kernel fails instead of comparing the row engine with itself.
2. **A planted kernel fault is found after the handover, not before.** The existing planted
   rollback (`misdescribeRollback` 3, which changes the kernel's answer for a forced decline and
   nothing the row path does) on the first composition the warm-up-off search finds it on: the
   warmed arm must pass the row-path runs and report "after the handover to the kernel". Without
   it, an arm that compared each run with itself, or labelled the sides wrongly, would pass.
3. The existing planted-rollback test, unchanged in what it asserts, now shares its search with 2.

Both vector widths: the arm runs in whatever JVM the suite runs in; it holds nothing
width-specific. No pinned fixture moves.

## 6. The measurement

No benchmark. The measurement is the arm's own: its counts and time at the CI default and at the
nightly's volume, and the releases it reports.

### 6.1 Predictions, registered before the run

The calibration table (`skills/testing-and-debugging.md`) runs 1.1 to 5.0 for time predictions,
most near 1.2; these are stated from probe 3 with that margin.

1. The CI default (8 compositions) adds under 15 s to the suite on the laptop, and a release adds
   up to 10 s each.
2. The nightly's 2,000 compositions take under an hour (probe 3: 1.3 s a composition, so 43
   minutes, at 1.2 times that 52).
3. No disagreement at 2,000 compositions of the day's seed.
4. Releases at 1% to 8% of warm-ups, with an empty C2 queue in each.

## 7. Risks

1. **A composition's later data compiles to another shape** (a column whose type the new values
   widen), so its "other data" run compares the row path again. Counted (2: 21 of 400), not
   failed.
2. **A release in the module's CI JVM costs the deadline**, 10 s each. At 8 compositions and
   probe 3's rate, under one release a run.
3. **The JIT recording perturbs what it records.** Probe 3's pair showed no cost; the arm can run
   without it.
4. **The warm-up thread outlives the test** and copies batches under the memory sanitizer:
   `withVarkaWarmup` waits for it to drain before turning the warm-up off.

## 8. Sequencing

1. This plan.
2. The arm, the planted-fault test, the nightly step, the skill and the milestone row.

## 9. Outcome

Built as 3.1 describes, on 14 October 2026, with the review's corrections in 9.1. Timings are the
laptop's, at a load of 1 to 4 from other agents, on six cores, and rough.

**The nightly's volume.** The `sparkwarm` step's command at 2,000 compositions of seed 20261014
(the day's seed), with the review's corrections (9.1), took 13 min 24 s for the test and 14 min 10
s with sbt's start, about 0.4 s a composition. 2,477 warm-ups: 2,453 COMPILED, 22 released at the
deadline (0.9%) and 2 released at once, with nothing to wait for. 2,372 runs on the row path while
warming, 1,499 on a kernel; 1,499 compositions handed over (75%), and of the other 501, 200 had no
Varka node, 153 raised on both sides, 148 had every batch pruned, none had a batch declined. Of the
1,499 runs over other data, 30 met a new shape. No wait timed out. No disagreement. The same
command before the corrections took 14 min 4 s with the same counts to within a few, except 2,000
runs over other data (101 new shapes) and 25 releases, 2 of them the early kind 9.1's second item
stopped reporting.

**The CI default.** The whole of `VarkaSparkFuzzSuite` and `VarkaSparkReproducerSuite` in 114 s
for sbt and both suites; the warmed arm's 8 compositions 2.9 s (7 warm-ups, all COMPILED) and the
planted-fault test 0.9 s, so the PR's addition is about 4 s on this machine, plus 10 s for each
release, which at 1% to 7% of warm-ups is under one a run.

**The 22 deadline releases of the nightly run** have the shape of 2's 35: every one with an empty
C2 queue, 14 with no deoptimization of the kernel recorded and 8 with traps in a masked loop or
`run` (VARKA-326's cycle), 21 with some method without code in use (in 7 a masked loop and its
epilogue), 1 with code for every method, and a last probe of 21,040 to 522,640 bytes. So the
release rate depends on the seed (6.8% at seed 20261008, 1.3% at seed 7, 0.9% at seed 20261014),
and every release this arm has met is VARKA-295's signature or VARKA-326's cycle, none a compile
still queued at the deadline.

### 6.1's predictions, scored

1. **CI default under 15 s, a release up to 10 s each.** Held: about 4 s, no release in the runs.
2. **The nightly's 2,000 under an hour.** Held: 13 minutes, a quarter of the 52 predicted and a
   third of probe 3's rate; probe 3's seed released 6.8% of its warm-ups, this seed 0.9%, and the
   releases were most of probe 3's time. Ratio 0.26, the second pessimistic line in the table.
3. **No disagreement at 2,000.** Held.
4. **Releases at 1% to 8%, each with an empty C2 queue.** Missed narrowly on the rate, 0.9%, below
   the range; held on the queue, empty in all 22.

### 9.1 What the review changed

`/code-review high` on the branch found ten things; all were fixed but one.

1. **A failure over the other data was written with the first data's pivot**, which that data no
   longer holds; it is written without one.
2. **Every RELEASED outcome was reported as a missed verdict**, though the warm-up also releases a
   kernel with nothing to wait for and one that failed. Only a release whose queue and run time
   reach the deadline is reported; the others are counted.
3. **The side a run was put on read the warm-up batches alone**, so a run with a filter on its
   kernel while the projection above it warmed was labelled the row path, and one with every batch
   declined the kernel. A run's side is now one of four: the row path, the kernel, one node on its
   kernel while another warms, or no batch on a kernel.
4. **Two releases in one composition shared one recording**, and the second read a recording
   started after its warm-up. The recording is read for the first; the second says so.
5. **`laterData` kept the old values where a fresh draw was NULL**, so a fresh draw at null rate
   1.0 gave back the first data. A cell now takes the first of four fresh draws that is not NULL
   there, and a composition drawn from the wider pool draws its later data from it too.
6. **The pivot's own outputs were asked once per run**; once per composition now.
7. **The run over other data ran for compositions that never reached a kernel**, comparing the row
   engine with itself and counted as coverage; it runs only after a handover.
8. **`withVarkaWarmup` reset the session's setting to off** instead of restoring it; it restores.
   The copied property name is left as it is: `VarkaKernelWarmup.DEADLINE_PROPERTY` is
   package-private to the warm-up, and `VarkaWarmupEndToEndSuite` copies it the same way.
9. **Tasks were called "row 295" and "row 326"** in the skill, the plan and the nightly; by id now.
10. **A JVM that cannot warm failed the arm**; it cancels it (`assume`). The arm runs under every
    option-matrix configuration, which leaves out `VarkaWarmupEndToEndSuite` for timing a compile;
    this arm asserts on no timing, and its 8 compositions add seconds per configuration.
