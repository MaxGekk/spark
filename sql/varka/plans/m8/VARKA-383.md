# VARKA-383: Long-lane kernels released after a kernel history

## 1. Where this came from

Row 383 of `m8/PLAN.md` (added 16 October 2026, wave 0), which folds in `m9/SCOPE.md` item 77 on
the owner's decision of the same day. Two readings started it. `m7/VARKA-257.md` 9.2
(`sql/catalyst/benchmarks/VarkaKernelHistory-jdk25-probe.txt`) warmed N kernels of the IR fuzz
grammar through production's warm-up in one JVM and found that at the laptop's 512-bit width,
with the Vector API template prewarm on, the warm-up released long-lane kernels at its 30 s
deadline, compiled and still allocating, in 7 of 72 cases at history positions 4 to 15 and 43 of
144 at positions 16 to 63, where the prewarm had cut the int lane's releases past position 16 to
15 of 144. And `VarkaWarmupEndToEndSuite` failed on two pull requests on 16 October with
`RELEASED did not equal COMPILED` on one shape; the failed job's log reads
`runNanos=120151168042`, `firstProbeBytes=7915632`, `lastProbeBytes=197328`, a release at the
suite's 120 s deadline with its last probe still allocating (item 77's notes). 257's readings were
taken at a machine load of 14 to 30, so the row asks first for a reproduction on an idle
GitHub-hosted runner, then a fix of what it shows. Done when the runner's reproduction is
recorded, the cause it shows is fixed or recorded with its reason, the long lane's release rate
after a history is read again on the runner, and `VarkaWarmupEndToEndSuite` passes in a full CI
run of the sql module. It is in milestone 8 because `DECIMAL` up to precision 18, `DOUBLE` and
`TIMESTAMP_NTZ` are long-lane kernels.

## 2. The admission check, done

Done by reading, on 16 October 2026, at master `0fb726692e3`: the plans named below, the code of
the warm-up, the prewarm and the emitter, and the CI log of the failure (job 117019494135, the
first attempt of fork run 38912533464, `sql - other tests`, read with `gh api`). No run was made;
the timings belong to the runners (section 3.1).

### 2.1 What VARKA-257 measured, and what it did not

Releases at the 30 s deadline, of the kernels warmed, at the host's 512-bit width, cores 12-17 of
the laptop at loads of 14 to 30, two JVMs of three cores (257 9.2):

| position in the history | int, prewarm off | int, prewarm on | long, prewarm off | long, prewarm on |
|---|---|---|---|---|
| 0 (the control) | 0 of 12 | 0 of 12 | - | - |
| 1 to 3 | 4 of 12 | 0 of 12 | 0 of 24 | 0 of 24 |
| 4 to 15 | 15 of 72 | 0 of 72 | 11 of 72 | 7 of 72 |
| 16 to 63 | 40 of 144 | 15 of 144 | 46 of 144 | 43 of 144 |

Every long-lane release had C2 code in use for every loop method. The releases belong to the
history: position 0 never released, and the first released draws ran alone compiled 6 of 6. What
was not measured: any width but the laptop's own (512 bits, where the long lane is eight lanes),
any idle machine, and any JVM that ran kernels before the prewarm. The prewarm runs on the warm-up
worker's first start, before its first job, so in the probe it always runs first.

### 2.2 The CI failure is an int-lane kernel at 256 bits, not a long-lane one

The failure's own account, in the job's log at 15:37:52 (the assertion message carries
`VarkaJitEvidence`'s summary and `lastReleaseJitState`):

* **The shape is int-lane.** The query is `SELECT i, date_add(d, 3) AS a, add_months(d, 2) AS b,
  last_day(d) AS c FROM varka_dates_big`; `d` is a `DATE` and every output a `DATE`, and
  `VarkaNodeCompiler.laneOf` puts `DATE` and `INT` on the 32-bit lane. The evidence says
  `vector classes inlined [Int256Vector]`.
* **The runner's preferred species is 256 bits**: eight int lanes, a width VARKA-257 never read.
  The pool's 256-bit CPUs are the EPYC 7763 and 9V74 (`m7/VARKA-255.md` 2.6); the log does not
  print the model.
* **Compiled, not waiting.** Nine compiles, every loop and epilogue method at tier 4, all within
  two seconds of the warm-up's start; no compile failure, no deoptimization; the C2 queue empty at
  the deadline; the JVM's total compile time 3161025 ms, a JVM that had run for over an hour.
  `epilogueMasked0` and `runMasked` had no code in use.
* **Boxing, where VARKA-295 9.6 put it.** 787439648 bytes allocated by the warm-up thread over the
  last 20 s, 757807328 of them `int[]`, at `IntVector.lanewiseTemplate:798` and `:849` inlined
  into `loopDense1` and `loopMasked1`: the unmasked and masked `VectorSupport.binaryOp` calls,
  the intrinsic refused and its result boxed (the line numbers read against the JDK 25 source
  tree in `openjdk-build/jdk25`, not the runner's Zulu build). That is cause 4 of
  `m7/VARKA-295.md` 9.5 at the int lane.

So row 383's reading that the failure "is 257's cause" holds for the symptom - compiled and still
allocating at the deadline - and not for the lane or the width. A long-lane fix alone would leave
the done-when's last clause where it is.

### 2.3 When the prewarm ran in that JVM

The shared sessions pin the warm-up off (`TestSQLContext`, `VarkaSharedSessions`), so the worker,
and with it `VarkaVectorTemplateWarmup`, starts at the first warm-up the JVM runs. In this job the
first Varka suite started at 14:31 (`VarkaSQLQueryTestSuite`, then `VarkaLongLaneSuite` at 14:33,
fifteen Varka suites in all), every one with kernels serving batches unwarmed and C1 allowed on
them; the first suite with a warm-up arm, `VarkaSparkFuzzSuite`, started at 15:02, and the
end-to-end suite at 15:35. That these share one JVM is in the failure's own account: compile ids
near 967,500 and 3161025 ms of compile time, which a JVM started for one suite does not reach (and
`VarkaArrowSource`, the one other test source naming the warm-up, sets it off too). So the
prewarm ran, at the earliest, half an hour after the JVM's first kernels had profiled and compiled
the shared templates. Its own class doc says what follows: "it cannot undo
a trap a template has already recorded", and `m7/VARKA-370.md` 9.4 found no in-process undo that
does not deoptimize every kernel. The INFO line that says when the prewarm ran
(`Vector API template warm-up took`) is below the CI log's level, so the exact time is not
readable; making it readable is the first change (section 8).

In production the warm-up is on by default (`spark.sql.codegen.varka.warmup.enabled`), so the
first kernel's claim starts the worker and the prewarm comes first, except after query-path
vector code (`IntRangeOps`, `SelectionVectorOps`) or in a JVM whose sessions turned the warm-up on
late. The sql module's test JVM is that second case by construction.

### 2.4 What "still allocating" means for the verdict, and what a release costs

`VarkaKernelWarmup.Job.run` judges a probe of four whole-snapshot calls clean when it allocates
at most `FIXED_ALLOWANCE_BYTES` (4096) plus one byte a row (4096) plus 256 bytes a column a call
(`SEGMENT_BYTES_PER_COLUMN`), and at most a quarter of the first probe (`COMPILED_DROP`); two
clean probes in a row are the verdict. For this shape, one input and three outputs, the allowance
is 12288 bytes (13312 if `i` is a kernel input). The last probe, 197328 bytes, passes the drop
(the first was 7915632) and fails the allowance by sixteen times. At eight lanes a probe runs 512
iterations of a group's loop, so that is about 385 bytes an iteration, eight 48-byte `int[8]`
boxes: several refused operations per iteration, steady after the compile, never coming clean
twice in 23,000 paced probes. A later deadline cannot change it.

A release does not send the shape to the row path: `VarkaKernelWarmth.RELEASED` lets tasks run
the kernel, which is the boxing C2 code for the rest of the JVM's life. Item 77's "a released
shape serves its batches on the row path for good" is wrong against the code. What a release
costs is the boxed kernel, the shape's batches on the row path until the deadline (60 s in
production), and the one worker thread spinning through that deadline while up to 16 other
shapes wait behind it (`QUEUE_CAPACITY`).

### 2.5 The progress deadline of item 77 would not keep this kernel

Item 77 proposed releasing when the probes' allocation stops falling rather than at a fixed time.
Here the allocation fell from 7915632 to the plateau within the first seconds and stayed there:
a progress deadline would have released the shape after its stretch instead of after 120 s - the
same kernel, the same boxes, the same failed assertion - and freed the worker sooner. It is a fix
for the worker's throughput, not for the verdict, so this row does not build it (3.4).

### 2.6 What the prewarm feeds the long lane, against what the emitter calls

`VarkaVectorTemplateWarmup.longs()`, `doubles()` and `conversions()` against the long-lane call
forms in `VarkaBodyEmitter`, `VarkaDivisionLowering` and `Lane`:

| form the long lane emits | where | fed by the prewarm |
|---|---|---|
| `convertShape(L2D, ...)`, `convertShape(D2L, ...)` | `emitDoubleConvert`, every long `ConstDivide` where converts intrinsify (the laptop, the 9V45, the Xeons) | **no**: `I2L`, `L2I`, `I2D`, `D2I` only |
| `DoubleVector.div(double)` / `mul(double)` | the same route | **no**: `div(Vector)`, `mul(Vector)` only |
| `DoubleVector.add(double)`, `sub(double)` | `emitMagicDivide`, where converts do not (`UseAVX=2`: the 7763 and 9V74) | **no**: the vector forms only |
| `DoubleVector.lanewise(SUB, double, mask)` | the same | **no**: masked `ADD` with a scalar only |
| `DoubleVector.compare(GT, Vector)` | the same | **no**: `EQ`, `LT`, `GE` |
| `LongVector.mul(long)`, `and(long)`, `or(long)` | the same, and `NarrowLane` | partly: `lanewise(op, long)` for every binary operator, not the convenience forms |
| `lanewise(NEG, mask)`, `compare(LT, long)`, the loads, stores, blends, `broadcast` | everywhere | yes |

Each "no" is a template site whose profile the long kernels can be the first to give a second
operator class or a second species after the prewarm, which is 9.6's route to a class-check trap.
That makes the coverage gap a candidate, not a verdict: 257 9.5 says the classification needs
`-XX:+LogCompilation` on a replayed released history, and which route a long division takes
depends on the host, so the 256-bit runners exercise rows the laptop never did.

### 2.7 VARKA-371's three-core release is int-lane too

`m7/VARKA-371.md` 9.2's as-shipped release, composition 312's kernel `e33b1eb0`, ended with 21120
bytes in its last probe (`sql/varka/demo/trap_sums-laptop-jdk25-output.txt`). One `int[16]` per
iteration at 512 bits is 20480 bytes a probe (4 calls of 64 iterations of 80 bytes), and one
`long[8]` would be twice that; so it is an int-lane kernel with the prewarm first, the residue 257
read as 15 of 144, and it says that residue moves with the core count (2 of 3 JVMs on three
cores, 0 in 2,000 compositions on five).

### 2.8 What the check changed, and what it would have rejected

The row as written - reproduce 257 on a runner, fix the long-lane prewarm, and the end-to-end
suite passes - would have fixed the second without touching the first. The plan has two halves:

* **(a) the long lane after a history**, 257's reading, reproduced on idle runners at 256 and
  512 bits; the fix is the prewarm's coverage (3.3 A), admitted only by the runner's numbers.
* **(b) the end-to-end failure**, an int-lane kernel after an hour of unwarmed kernels; the fix
  is a prewarm that runs before the first kernel of the JVM rather than before the first warm-up
  (3.3 B), admitted by a reproduction that starts kernels before the worker.

A runner reading of 257's case at 0 long-lane releases would have closed half (a) as the laptop's
load; it would not have closed half (b), whose failure is on a runner.

## 3. The design

### 3.1 The reproduction on GitHub-hosted runners

**Where it runs.** No workflow runs `dev/varka_kernel_history.sh`: `varka-fuzz.yml` builds
catalyst's tests and forks probe JVMs but exports a classpath the launcher does not read, and
neither it nor a new workflow file can be dispatched on `MaxGekk/spark` before it is on that
repository's default branch, which is apache/spark's master. `benchmark.yml` is on that branch,
every recorded fork dispatch names it with `--ref` (`VARKA-259`'s), and it already has
`expected-cpu`, `extra-java-options` and `num-splits`. So the first change is a **history mode in
`benchmark.yml`**: two inputs, `history` (the launcher's arguments; blank keeps the workflow as it
is) and `history-bits` (`256`, `512` or blank). With `history` set, `matrix-gen` emits one job per
split, `tpcds-1g-gen`, `benchmark` and the pair job are skipped, and a new `kernel-history` job
per split: checks out, sets up JDK 25, reads `MaxVectorSize` from `-XX:+PrintFlagsFinal` and
**fails before the build when it is not `history-bits`** (a rerun of the failed jobs draws another
machine, as `expected-cpu` does), prints the CPU, runs
`dev/varka_kernel_history.sh $history --fork-base $(( (split - 1) * forks )) --out <dir>` and
uploads the directory; a `kernel-history-report` job merges the shards' logs and runs
`dev/varka_kernel_history.py --dir` over them, by width, with each shard's CPU listed.

**Changes to the launcher and the probe**, test code only:

* `--fork-base B`: fork `f` of a shard is `B + f` and draws seed `--seed + B + f`, so shards have
  distinct histories and file names, and with the default seed 20260903 forks 0 to 5 are exactly
  257's laptop histories, comparable fork by fork.
* `--prewarm` takes `off`, `on` (the shipped prewarm at the worker's start), `first` (the prewarm
  called by the probe before anything else), and later the fix arms' names (3.3).
* `--cold K` (`-Dvarka.history.cold`): before the warmed history, K grammar kernels, int and long
  in turn, emitted **unwarmed** (C1 allowed, as a test session's) and called forty times each on
  the probe's 1,024-row batch on the main thread, as a suite's four 10,000-row batches would; the
  worker, and so the `on` prewarm, starts only after them. The probe prints
  `templateAfter=<K>` so a fork that prewarmed first cannot read as late.
* `--test e2e` (`-Dvarka.history.test`): the kernel under test is the end-to-end suite's
  projection, `date_add(d, 3)`, `add_months(d, 2)`, `last_day(d)` over one date column with every
  seventeenth row null, compiled through `VarkaExpressionCompiler`, instead of the cheap tails.
  Its verdict is printed and counted; its answers are compared as the others are.
* Runners have four vCPUs: every dispatch passes `--jvms 1 --cpus 4`.

**The cases.**

| wave 1 dispatch | runner | cases | forks | what it answers |
|---|---|---|---|---|
| R1 | 256 bits | N = 16, 64; prewarm on, off | 6 (3 shards of 2) | half (a) at the CI's width, the magic-divide route |
| R2 | 512 bits | the same, at `--widths 64,32` | 6 (3 shards of 2) | half (a) at the laptop's width, and 256 on the same machine |
| R3 | 256 bits | N = 64, prewarm on, `-XX:PerMethodTrapLimit=100000 -XX:PerMethodRecompilationCutoff=100000` | 6 (1 shard) | whether 9.6's trap sums are the mechanism |
| R4 | 256 bits | `--cold 128 --after 0 --test e2e`, prewarm on (late) and first | 6 each (2 shards of 3) | half (b): an int kernel after unwarmed kernels |

128 bits is left out: there the long lane releases by its shape alone (`m7/VARKA-374.md`), which
is not this row's. The warm-up deadline stays 257's 30 s, so the numbers compare. Wave 2, only if
R1 or R2 releases a long kernel: one replay of a released draw (`--after <p+1> --forks 1 --seed
<s>`) with `-XX:+UnlockDiagnosticVMOptions -XX:+LogCompilation`, the log uploaded and read with
`dev/varka_trap_census.py` and VARKA-371's `ccount` reading (`sql/varka/demo/trap_sums.sh`), which
says which template sites hold the traps.

**The commands** (after the owner's go-ahead and the branch pushed to the fork; the PR body holds
them with their costs):

    gh workflow run benchmark.yml -R MaxGekk/spark --ref varka-383 -f jdk=25 \
      -f generate-tpcds=false -f num-splits=3 -f history-bits=256 \
      -f history="--after 16,64 --forks 2 --prewarm on,off --jvms 1 --cpus 4"
    gh workflow run benchmark.yml -R MaxGekk/spark --ref varka-383 -f jdk=25 \
      -f generate-tpcds=false -f num-splits=3 -f history-bits=512 \
      -f history="--after 16,64 --forks 2 --prewarm on,off --widths 64,32 --jvms 1 --cpus 4"
    gh workflow run benchmark.yml -R MaxGekk/spark --ref varka-383 -f jdk=25 \
      -f generate-tpcds=false -f num-splits=1 -f history-bits=256 \
      -f history="--after 64 --forks 6 --prewarm on --jvms 1 --cpus 4" \
      -f extra-java-options="-XX:PerMethodTrapLimit=100000 -XX:PerMethodRecompilationCutoff=100000"
    gh workflow run benchmark.yml -R MaxGekk/spark --ref varka-383 -f jdk=25 \
      -f generate-tpcds=false -f num-splits=2 -f history-bits=256 \
      -f history="--cold 128 --after 0 --test e2e --forks 3 --prewarm on,first --jvms 1 --cpus 4"

**What they cost.** Nine jobs, which fit the fork's forty slots beside one other run. Per fork at
the laptop, from 257: N = 16 about 80 s (its nightly arm, six forks in 3 min 52 s on two JVMs);
N = 64 about 7 minutes with the prewarm on and 9 off, most of it released kernels at 30 s each.
The calibration table has no laptop-to-runner line; at VARKA-257's ratio for a JIT-bound step
(3 min 52 s against 3 minutes) and a runner core assumed half again slower, a fork index of R1 is
about 25 minutes, so R1's jobs take 45 to 75 minutes each, R2's 90 to 150 (two widths, plus the
reruns until a 512-bit machine is drawn: 9 to 14 of 40 draws in 255 2.6), R3's 20 to 60 (short
if the limits clear the releases), R4's 25 to 50; each adds the catalyst test build, 222 to 299 s
in `sql/varka/ci/step-budget.tsv`. About two and a half hours of wall time and ten to twelve runner
hours. None of it runs in a pull request's CI.

### 3.2 What admits each half, registered before the runs

* **Half (a) is admitted** when, at either width, the long lane past position 15 with the prewarm
  on releases at least 8 of 144 with every loop method at tier 4, while position 0 stays at 0.
  **It closes as the laptop's load** when both widths read at most 2 of 144 there; the row then
  records the runner's numbers as the reason, and the long-lane fix is not built. Between the two,
  six more forks per width before deciding.
* **R3 decides which fix half (a) gets.** If the raised limits take the long lane's releases past
  15 under 3 of 144, the mechanism is 9.6's sums and the prewarm's coverage is the fix (3.3 A). If
  they do not, it is another route (373's per-bytecode trap, 374's refused intrinsic, or
  something new), and wave 2's classification comes before any build.
* **Half (b) is admitted** when R4's late arm releases the end-to-end kernel in at least 2 of 6
  forks and the `first` arm in 0 of 6. If the late arm releases none, a cold prefix of grammar
  kernels does not reproduce the sql JVM's state; the row then reads the next CI failure with the
  diagnostics of section 8 step 2 before choosing, and says so.

### 3.3 Candidate fixes, built only when admitted

Both arms go behind `varka.warmup.vectorTemplates`, the prewarm's existing system property, with
the shipped behaviour kept as the reference arm; the default flips in the last commit (section 8)
and only on the runner's re-reading.

**A. The prewarm mirrors the long kernels' call forms** (half a). `VarkaVectorTemplateWarmup`
gains the forms 2.6 lists: `convertShape` with `L2D` and `D2L`, `DoubleVector`'s scalar
`add`/`sub`/`mul`/`div`, its masked `SUB` with a scalar and `compare(GT, ...)`, and `LongVector`'s
scalar convenience forms, all at the preferred species only (`VARKA-373.md` 9: feeding a second
species made the releases worse, 62 against 14 to 16). The list is not kept by hand: a test reads
every Vector API call site, with the operator constant before it, out of the emitted classes of
the coverage corpus and of grammar draws at both lanes and both division routes, reads the
prewarm's own call sites out of its bytecode with the same walker, and fails on a form the
prewarm does not call (section 5). The value `full` selects the new set; `true` keeps the shipped
one. If wave 2 names sites the forms do not reach, the arm follows the log, not this table.

**B. The prewarm before the JVM's first kernel** (half b). Today it runs on the worker's first
start, which in a JVM whose sessions start with the warm-up off is after any number of unwarmed
kernels. The arm runs `VarkaVectorTemplateWarmup.ensure()` (already once per JVM, synchronized and
guarded) at the first kernel class the shape cache defines, whatever the session's warm-up
setting, so no kernel's calls precede it; the worker's call then finds it done. Its cost moves
from the worker to the first emitting thread, once per JVM: 219 to 345 ms at 300 rounds on a
loaded laptop (`VARKA-295.md` 9.7), paid by a query only where the warm-up is off, since with it
on the first kernel waits on the row path anyway. `-Dvarka.warmup.vectorTemplates.eager=true`
selects it. It does not reach query-path vector code that runs before any emission; if R4 or the
next CI log points there, the same call moves to the first Varka exec node's preparation.

**What is not a code fix, and how it relates.**

* **Raising `PerMethodTrapLimit` and `PerMethodRecompilationCutoff`** removed the releases in
  every run of `VARKA-295.md` 9.6 and `VARKA-371.md`, and it is R3's diagnostic. It is an
  operator's startup option (`spark.executor.extraJavaOptions`), not a product change: the flags
  are not manageable and a running JVM cannot raise them (`VARKA-370.md` 9.4). And it has a cost
  the record names: with the limits raised a deoptimization cycle stops ending and traps thousands
  of times a second while the warm-up reads it as settled (`skills/the-jit.md`, "With the trap
  limits raised..."). So the docs may mention it as a diagnostic, not recommend it.
* **Redefining the Vector API classes in-process** (`VARKA-370.md` 9.1) is the one undo of a
  pollution already made, at the price of deoptimizing every kernel in the JVM and a dynamic agent
  the JDK is retiring. Not for the product.
* **Re-emitting a released kernel** gives it fresh profiles of its own, but its new compile
  inlines the same shared templates with the same recorded traps; no gain.
* **Smaller loop methods** lower the sum by inlining fewer copies, but 9.6's sum of 254 from 4 or
  5 traps a copy would need under twenty copies per method; not a budget the emitter can keep.
* **Running the end-to-end suite in its own JVM** would pass the done-when's clause by hiding the
  case it reports, a long-lived JVM whose templates were profiled before the warm-up; kept as the
  owner's fallback only if half (b) is not admitted and B is not built.

### 3.4 What is deliberately unchanged

* **The verdict.** `blockClean`, the allowance and `CLEAN_PROBES` read 2.2's kernel correctly.
* **Item 77's two changes** (extra probes at the deadline for a shape whose last probe was clean,
  and a deadline on progress): neither keeps a kernel that never stops allocating (2.5). They go
  back to `m9/SCOPE.md` at this row's close, rewritten as a worker-throughput item with 2.4's
  queue arithmetic.
* **The 128-bit long lane** (`VARKA-374.md`) and **the 4-lane override** (`VARKA-373.md`).
* **The emitter.** No emitted byte changes; `emitted_bytes.json` stays byte-identical.

### 3.5 Registered op counts

None: no emitter change. The prewarm is not emitted code, and its call list is held by the
coverage test of section 5 rather than by op counts.

## 4. Files

| file | what |
|---|---|
| `.github/workflows/benchmark.yml` | the history mode: inputs `history`, `history-bits`; jobs `kernel-history`, `kernel-history-report` |
| `dev/varka_kernel_history.sh` | `--fork-base`, the `--prewarm` vocabulary, `--cold`, `--test` passed through |
| `dev/varka_kernel_history.py` | reads shards merged into one directory, the e2e kernel's verdict, `templateAfter`; long releases past 15 as a `--guard` ratchet (`--max-long-released`) |
| `sql/catalyst/src/test/java/.../varka/VarkaKernelHistoryProbe.java` | the cold prefix, the e2e subject, the `first` prewarm |
| `sql/catalyst/src/main/java/.../varka/VarkaVectorTemplateWarmup.java` | arm A's forms under `full`; a count of kernels defined before it ran |
| `sql/catalyst/src/main/java/.../varka/VarkaShapeCacheImpl.java` | arm B's call at the first kernel definition |
| `sql/catalyst/src/main/java/.../varka/VarkaKernelWarmup.java` | the worker's start time and the prewarm's, readable for the failure message |
| `sql/catalyst/src/test/java/.../varka/VarkaVectorTemplateWarmupCoverageSuite.java` | new: the emitted forms against the prewarm's |
| `sql/catalyst/src/test/java/.../varka/VarkaLongLaneHistorySuite.java` | new: a replayed history, every long kernel expected COMPILED |
| `sql/catalyst/src/test/scala/.../varka/VarkaVectorTemplateWarmupSuite.scala` | arm B's once-per-JVM and before-the-first-kernel checks |
| `sql/core/src/test/scala/.../execution/VarkaWarmupEndToEndSuite.scala` | the failure message says when the prewarm ran and after how many kernels |
| `dev/varka_nightly.sh` | the `history` step's long-lane ratchet, added lines only |
| `docs/sql-varka.md` | the warm-up section: what the prewarm covers, when it runs, the trap-limit options as a diagnostic |
| `sql/catalyst/benchmarks/VarkaKernelHistory-runner-jdk25-probe.txt` | new: the runners' readings, one section per dispatch with its CPU |
| `sql/varka/skills/the-jit.md` | the lesson on the prewarm's timing and coverage |
| `sql/varka/skills/testing-and-debugging.md` | the calibration line for the runner cost |
| `sql/varka/plans/m8/PLAN.md`, `sql/varka/plans/m9/SCOPE.md` | row 383; item 77's remainder at the close |

## 5. Tests, and what each is for

* **`VarkaVectorTemplateWarmupCoverageSuite`** (JUnit under `@VarkaSuite`, `PinsDefaults` where it
  reads the defaults' classes): every `(owner, method, descriptor, operator constant)` a kernel of
  the coverage corpus or of 200 grammar draws per lane calls, under `convertsFallBack` true and
  false and `mulHiDivide` on and off, is called by the `full` prewarm, both read from bytecode;
  and the shipped set's gaps equal a pinned list, so the reference arm cannot drift. It catches a
  new emitter form that no one added to the prewarm, which no JIT test would catch until a release.
* **`VarkaLongLaneHistorySuite`** (JUnit, `ownJvm`): replays 257's fork-0 history (seed 20260903)
  through the warm-up worker up to position 13, the long kernel released in both laptop runs with
  the prewarm on (257 2), with the deadline at 120 s and the prewarm's having run first asserted
  (`templateWarmupNanos() > 0` before the first job); expects COMPILED for every long-lane kernel,
  and on failure prints each outcome and the released kernels' tiers. It is a JIT outcome, so it
  goes in only once wave 1 has shown the replay releases on a runner under the shipped prewarm and
  the arm clears it; until then the nightly's ratchet below carries the reading.
* **`VarkaVectorTemplateWarmupSuite`**, arm B: with the eager flag the first kernel definition runs
  the prewarm before the class is defined and once per JVM (`completedRuns()`), and a later warm-up
  does not run it again; without the flag nothing changes.
* **`VarkaWarmupEndToEndSuite`**: its failure message gains the prewarm's duration, the JVM uptime
  at the worker's start and at the prewarm's, and the count of kernels defined before the prewarm;
  so the next failure says whether 2.3's explanation holds, without a log level change.
* **The nightly's `history` step** gains `--max-long-released`, set from the runner's re-reading
  after the fix, so a regression of the long lane fails the night rather than waiting for a CI
  flake.
* **The done-when's last clause**: `VarkaWarmupEndToEndSuite` passing in full CI runs of the sql
  module. One pass proves little against a flake seen twice in a day; the outcome counts every
  `sql - other tests` job of the fork between the default flip and the close, passes and failures.

Both widths: the coverage suite is width-independent by construction (it reads bytecode for the
host's species and under `-XX:MaxVectorSize=32`); the JIT readings are the runners'. No pinned
fixture moves.

## 6. The measurement

On runners only, through 3.1's dispatches: wave 1 before any fix, then R1 and R2 again with
`--prewarm on,full` (arm A against the shipped set, same forks, same seeds) and R4 again with
`--prewarm on,eager` (arm B). The control is position 0 of every case, and in R4 the `first` arm.
Readings go to `VarkaKernelHistory-runner-jdk25-probe.txt` with each shard's CPU and width.

### 6.1 Predictions, registered before the run

1. **Position 0 releases nothing** in any case, at either width (257: 0 of 12 in every arm).
2. **The long lane after a history reproduces on idle runners**: with the prewarm on, past
   position 15, between 20 and 60 of 144 at 512 bits and between 20 and 70 at 256 bits, where the
   magic-divide route uses more unfed forms (2.6); at positions 4 to 15 between 3 and 20 of 72.
   The reason: 257's releases were the history's (position 0 never, the draws alone compiled), and
   a mechanism keyed by template profiles does not need a loaded machine. Under 3 of 144 would
   say the opposite.
3. **The prewarm still does not move the long lane**: on against off within 15 of 144 of each
   other at each width (257: 43 against 46).
4. **The int lane past position 15, prewarm on, is under 25 of 144** at each width, and over 30
   with it off (257: 15 and 40).
5. **R3's raised limits take the long lane past 15 under 3 of 144**: the 9.6 mechanism, as every
   run with them so far.
6. **R4's late arm releases the end-to-end kernel in 2 to 6 of 6 forks and the `first` arm in 0**.
   This is the least certain: a cold prefix of 128 grammar kernels is shorter and narrower than an
   hour of the sql module's suites.
7. **Arm A on re-reading**: the long lane past 15 under 10 of 144 at each width, if 5 holds.
   **Arm B**: R4's eager arm at 0 of 6, as `first`.
8. **Cost**: R1's jobs 45 to 75 minutes, R2's 90 to 150, R3's 20 to 60, R4's 25 to 50, each plus
   the build (3.1); scored against the jobs' step times and added to the calibration table.

The default flips to `full` (arm A) if 7 holds at both widths with 1 and 4 not made worse, and to
eager (arm B) if R4's eager arm is at 0 and the first-query cost where the warm-up is off stays
within 9.7's range, measured on a runner with `VarkaColdStartBenchmark`.

## 7. Risks

1. **The runners read 0 for the long lane** (prediction 2 fails low): half (a) closes as the
   laptop's load with the runner's numbers as its record; half (b) is untouched.
2. **R3's limits do not clear the long lane**: the cause is not 9.6's sums and arm A is the wrong
   fix; wave 2's compile log names the sites first. 3.2 already gates the build on it.
3. **More forms make the prewarm's own profiles worse**: an operator class added at a site makes it
   megamorphic, which is the point, but a species added does harm (373: 62 against 14 to 16).
   Arm A adds no species; the coverage suite checks that every form is at the preferred one.
4. **The sql JVM's history is more than recorded class-check traps**: an hour of recompiles also
   grows the templates' decompile counts, which no prewarm prevents. Then arm B does not fix the
   end-to-end failure; section 5's message shows it at the next failure (prewarm first and the
   kernel still boxing), and the row records it and asks the owner about 3.3's last bullet.
5. **The JIT-outcome test flakes on a loaded runner**: it goes in only after the runners show it
   discriminates (section 5), with the deadline raised and its own JVM.
6. **A 512-bit runner is scarce**: R2 needs reruns of its failed-early jobs; if none lands in a
   day, R2 runs at 512 bits on the laptop after the owner says so, with the load stated.

## 8. Sequencing

1. This plan, and row 383 marked Planned.
2. The reproduction tooling and the diagnostics, no product behaviour: `benchmark.yml`'s history
   mode, the launcher's and the probe's options, the reader's merge; the end-to-end suite's failure
   message and the warm-up's timestamps it reads. Green alone; a smoke dispatch of one shard at
   N = 4 checks the mode before wave 1.
3. Wave 1 on runners (owner's go-ahead), the readings committed, 6.1 scored in 9.1, 3.2's
   admission decisions written down; wave 2 if a long kernel released.
4. Arm A behind `full` with the coverage suite, if half (a) is admitted; arm B behind the eager
   flag with its tests, if half (b) is.
5. The re-readings on runners, the cold-start cost of arm B.
6. The defaults flipped by 6.1's rule, the docs, the skills lesson, the calibration line, the
   nightly's ratchet, `VarkaLongLaneHistorySuite` if it discriminates; item 77's remainder back to
   `m9/SCOPE.md`; the end-to-end suite's CI passes counted for the done-when.

## 9. Outcome

Filled in as the runs land.

### 9.1 The reproduction tooling and the diagnostics (section 8 step 2), built

No product behaviour changed, and no run on a runner has been made yet.

* **`benchmark.yml`'s history mode.** Inputs `history` and `history-bits` as 3.1 has them. With
  `history` set, `matrix-gen` checks it (not white space alone, no `pair`, no `pr`, `history-bits`
  256, 512 or blank, and no `--fork-base`, `--out`, `--build`, `--guard` or `--max-slow`, which the
  jobs set), `tpcds-1g-gen`,
  `benchmark` and `pair` are skipped, and one `kernel-history` job runs per split: JDK 25 whatever
  `jdk` says, the CPU and `MaxVectorSize` printed and the job failed before the build when the
  width is not `history-bits` or the CPU not `expected-cpu`, catalyst's test classes built with the launcher's own command, then
  the launcher with the `history` words, each word of `extra-java-options` as a `--jvm`, and
  `--fork-base (split - 1) * forks` (`--forks` read from `history`, 10 when absent). Its summary
  goes to the job summary and its directory, without the forks' temporary directories, to the
  artifact `kernel-history-split-<n>`. `kernel-history-report` downloads every split, copies the
  fork logs into one directory with each split's provenance as `provenance-<n>.txt`, runs the
  reader over it under `--guard` with no limit on slow forks (so a wrong answer, a harness error
  or an unfinished fork fails it, and a slow one never does), and publishes the summary to the job summary and the artifact
  `kernel-history-report`; both uploads overwrite, for a rerun of the failed jobs. Every input reaches the shell through `env:`. With `history` blank the
  file's other jobs are as they were. A dispatch runs `num-splits` + 2 jobs: R1 and R2 five each,
  R3 three, R4 four, seventeen in all, of which the nine splits are the runner hours 3.1 costs.
* **The launcher** takes `--fork-base B` (forks `B` to `B + forks - 1`, seed `--seed + f`, the log
  named by the global fork), `--prewarm` `on`, `off` or `first`, `--cold K` and `--test tails|e2e`,
  checks them, and writes them to the provenance's `args:` line.
* **The probe** takes `-Dvarka.history.cold=K`: K grammar kernels, int and long in turn, from a
  seed of their own (the seed plus 2^40, so no fork's history draw repeats one), emitted unwarmed
  (C1 allowed), each called forty times on its 1,024-row batch on the main thread and then
  compared, one `VARKA_HISTORY_COLD` line each, all before the first warm-up starts the worker.
  `-Dvarka.history.prewarmFirst=true` (the launcher's `first`) runs the prewarm before the cold
  kernels. `-Dvarka.history.test=e2e` makes the kernel under test the end-to-end suite's three date
  outputs over the suite's dates, every seventeenth row null. `BEGIN` carries `prewarm`, `cold`
  and `test`; `DONE` carries `templateAfter`, the kernel classes defined before the prewarm ran,
  and `templateMillis` now comes from the prewarm's own record, so a `first` fork reports it too.
  `templateAfter` counts definitions, not runs: an `on` fork reads K + 1, the first warmed kernel
  being defined before the warm-up that starts the worker, which corrects 3.1's
  `templateAfter=<K>`. `VarkaKernelWarmup.templateWarmupNanos()` reads the same record now, so it
  is right whichever thread ran the prewarm.
* **The reader** lists each `provenance*.txt`'s CPU, groups forks by the vector bits each read
  (not the width asked for, which is `host` on every runner), labels a case with its kernel under
  test and cold prefix where they are not the defaults (from the launcher's provenance for a fork
  that died before saying), shows `templateAfter` per fork, always shows the
  e2e kernel's released count, and counts the cold kernels among those compared. The
  `--max-long-released` ratchet of section 4 is left for step 6, where section 5 sets its value
  from the runners' re-reading.
* **The diagnostics.** `VarkaVectorTemplateWarmup` records, when it runs, the JVM's uptime, the
  kernel classes defined before it (a JVM-wide count `VarkaShapeCacheImpl` keeps, one line beside
  the class definition; that file was listed for arm B only) and its duration;
  `VarkaKernelWarmup` records the uptime at the worker's first start and renders both as
  `templateWarmupAccount()`, which `VarkaWarmupEndToEndSuite` appends to its verdict assertion and
  to every other failure its catch rewrites, and the columnar-consumer test to its verdict
  assertion; it never throws, so a JVM where the prewarm's class
  cannot initialize keeps the failure it is explaining.
* **The self-review** (`/code-review high` on the branch) made ten findings; all ten were fixed
  as the items above describe, the last being one over-long line in the workflow.

Checked locally, counts only: the workflow parses, actionlint reports the 19 shellcheck notes
master's file has and none in the new jobs, shellcheck and ruff are clean on the launcher and the
reader, the matrix step's checks accept the four dispatches' `history` values and refuse `--out`
and `history-bits=128`, and the run step's argument handling was traced with a stand-in launcher
for R3 and R4 (fork bases 12 and 3, the trap-limit flags as two `--jvm`s). One run of
`--cold 4 --after 0 --test e2e --forks 1 --fork-base 3 --prewarm on,first` finished both forks
(seed 20260906, four cold kernels compared ok in each, `templateAfter` 5 and 0), one of
`--after 3 --forks 2 --fork-base 4` finished forks 4 and 5, and the report step's merge of the two
directories read all four forks with both provenances. The four commands of 3.1 run unchanged.
