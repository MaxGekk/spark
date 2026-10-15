# VARKA-295: Find why the warm-up end-to-end test times out on CI

## 1. Where this came from

Row 295 of `m7/PLAN.md`, found by VARKA-215 9: `VarkaWarmupEndToEndSuite`'s "a projection's first
query takes the row path and its next one the compiled kernel" failed on the fork's CI in two of
the first five runs of 6 October 2026 (#663's first run, #666's), each time with the warm-up
`RELEASED` after about sixty seconds and never `COMPILED`, and passed on a rerun. The branches under
test did not touch it. The failing job is `Varka suites: sql` (35 suites over 2 JVMs, one of them
the run of the defaults).

## 2. The admission check, done

What has to be true before a fix is worth writing is a cause, and the cause is not yet known. What
the check found on 8 October 2026:

* **Not reproduced locally.** The test alone takes 327 ms to its verdict (7,548 calls). Pinned to
  one core with three busy loops on it, a 4x oversubscription, it takes 4.1 s: a twelve-fold
  slowdown, nowhere near sixty seconds. Run through `dev/varka_matrix.sh` with the sanitizer on, as
  CI runs it, alone: 17 s for the suite, all green. All 35 sql suites over 2 JVMs, as CI runs them:
  green, no `CodeCache is full`, no warm-up stopped at its deadline.
* **Not head-of-line blocking.** The warm-up has one worker thread, and the deadline counts the
  time in the queue. But the test calls `VarkaShapeCache.invalidateAll()` first, and a queued job
  whose shape has left the cache ends at its next loop test, so a predecessor cannot hold the
  worker for long.
* **The CI logs say nothing more.** The failing job's log prints the test name and, in this run,
  none of the message that carries the warm-up's `Outcome` (queue time, run time, first and last
  probe), because the report then printed only the `FAILED` line.
* **The recorded mechanism that fits.** `sql/varka/skills/vector-api-and-width.md` records that a
  verdict is a JIT outcome and depends on the order profiles filled in: the unit warm-up suite
  failed one full run in three by the suites' order until it forked a JVM for its compile tests.
  That is the same symptom. The suites that cause it there run a second vector species, which no
  `sql/core` suite does; a shared Vector API template whose profile an earlier kernel polluted
  would do the same, and is not ruled out.

So the cause is one of: the compilers starved on the runner, a polluted template profile from an
earlier suite, or something not yet thought of. This task does not guess between them.

## 3. The design

### 3.1 Make the next failure say which

When the warm-up is released at its deadline it now records the JIT's state at that moment: the
compilers' total time, each code heap's use, and the head of the compile queue (the `Compiler.queue`
diagnostic command, read through its MBean). The log line carries it, and
`VarkaKernelWarmup.lastReleaseJitState()` hands it to the end-to-end test, whose assertion message
now ends with it. The matrix report prints the failure's message lines, so the next red run shows
the verdict, the queue and the cache in one place. The cost is on the release path only, which
runs once per shape that already waited sixty seconds.

### 3.2 What is deliberately unchanged

The 60-second deadline, the allowance and the verdict rule. Lengthening the deadline or retrying in
the test would make the failure rarer and say nothing about it, which is the thing this row is
asked not to do.

### 3.3 Registered op counts

None; no emitter change.

## 4. Files

| file | what |
|---|---|
| `VarkaKernelWarmup.java` | `jitState`, `lastReleaseJitState`, the release path records it |
| `VarkaWarmupEndToEndSuite.scala` | the verdict's assertion message carries it |
| `VarkaKernelWarmupSuite.scala` | a test that the state names the compilers, the cache and the queue |

## 5. Tests, and what each is for

`VarkaKernelWarmupSuite`'s new test reads `jitState` on the shared JVM and asserts it names the
compile time, a code heap and the queue, so the diagnostic cannot silently turn into
"unavailable" on a JDK that drops the command.

## 6. The measurement

Not a performance task. The evidence is CI: the diagnostic is on a branch whose `Varka suites: sql`
job is rerun until it fails, and the failure's message is the measurement.

### 6.1 Predictions, registered before the runs

1. The failure reproduces on the fork's runners within ten reruns of the job (it failed two in five).
2. The queue it prints is not empty and holds methods other than the warmed kernel's, or the
   verdict was a polluted profile with an empty queue. Which of the two is the answer.
3. The fix is then either the test's own JVM (as the unit suite's) or the deadline's honest wait,
   and which follows from 2.

## 7. Risks

1. **The diagnostic command is unavailable** on some JDK or flag: the state then says "unavailable"
   with the exception, and the test in 5 fails on a JDK where that is so.
2. **The failure does not recur** in the reruns: then the row stays open with this diagnostic in
   place, and the next red run on any branch carries the answer.

## 8. Sequencing

1. This plan and the diagnostic, one commit. Pushed with approval, and the job rerun until it fails.
2. The fix chosen from what the failure says, with section 9.

## 9. Outcome

Done on 9 October 2026, from the failure the diagnostic's predecessor needed: #678's first CI run
failed `Build modules: sql - other tests` (the whole `sql/test` in one JVM, 21,494 tests) with the
test of this row, and its message carried the warm-up's own outcome:

    state=RELEASED, calls=62724, rows=49487872, queuedNanos=36121608, runNanos=59968800886,
    firstProbeBytes=5858064, lastProbeBytes=20720; the first run took 522 ms

**The cause is the verdict's allowance, not the compile.** The warm-up was not starved (it waited
36 ms in the queue and ran 62,724 calls in its sixty seconds), and the kernel did compile: a probe
block allocated 5.86 MB when nothing was compiled and 20.7 KB at the end, 283 times lower, which
nothing short of C2 does (the interpreter and C1 box every operation). But a block is clean only
within `4096 + 1 byte a row + 256 a column per call` of allocation, 9,216 bytes for this kernel,
and in this JVM the compiled kernel allocated 20,720: a JVM in which 20,000 earlier tests have
shaped the Vector API templates' profiles leaves more calls out of line than the allowance counts.
On an idle JVM the same kernel ends at 480 to 800 bytes. So the warm-up ran to its deadline and
released a kernel that had been compiled for most of the minute, and its queries ran on the row path
meanwhile: the failure is the test's, but the sixty seconds are a user-visible cost.

**The fix.** A block is also clean when it falls `DRAMATIC_DROP` = 32 times below the first,
whatever it allocates (`VarkaKernelWarmup.clean`, with a unit test over the observed numbers and
the edges). 32 is far under the drops seen (283 in the polluted JVM, thousands in a clean one) and
far over what a partial compile gives; the existing quarter-and-allowance rule is unchanged for
every block that satisfies it.

**Corrections to this plan** (a record, not rewritten): 3.2 says the verdict rule is unchanged,
and it is now changed, by the above. Prediction 2 said the queue would be non-empty or the profile
polluted with an empty queue; it was the second, but the pollution is of the allowance, not of the
compile. Prediction 1 (it recurs within ten reruns) was wrong in the useful direction: the failure
came from a job that already existed, and ninety local runs of the 36 Varka suites over two
JVMs, pinned to four cores each (three at a time overnight), never failed. The failing JVM is the
big one; the Varka matrix's is too small to reach it.

**The diagnostic stays** (`lastReleaseJitState`, in the log and the test's message): a release at
the deadline from now on is a kernel that did not compile or one the new rule did not accept, and
the message says which. Its test caught that the code cache is one `CodeCache` pool below 240 MB,
not `CodeHeap` segments; the state names both.

### 9.1 Correction, 9 October 2026, after #683's CI

**Section 9's diagnosis was wrong and its fix is withdrawn.** Section 9 read one CI failure
(lastProbeBytes=20720 against a first block of 5858064, 283 times lower, over an allowance of
9216) as a kernel that had compiled and missed the absolute allowance in a JVM shaped by earlier
tests, and let a block 32 times below the first count as clean. #683's CI then failed
`VarkaKernelWarmupSuite` "a warm-up claimed by a batch with no nulls compiles both of the kernel's
drivers": the verdict was COMPILED with lastProbeBytes=38784 (152 times lower), and batches with
nulls then allocated 300,928 bytes against a limit of 20,480. The test's own comment says why
(VARKA-221 2): the light group's loop method compiles a compile or two after the heavy one, and
while it is still interpreted a long call boxes *tens of kilobytes*. That is the size of the
number I read as a compiled kernel's residue. A magnitude alone cannot tell the two apart, and the
32x rule reintroduced the very case the allowance was written to refuse.

**What the first failure most likely was, which section 9 had as risk 1 of the row's own text:**
a loop method whose C2 compile had not landed after a minute on a loaded runner, two JVMs on four
cores with the compile queue shared. Its 20 KB a block is an interpreted light loop, as in
VARKA-221. The warm-up releasing at its deadline was the policy working.

**The fix now:** the strict verdict is restored (the 32x rule and its unit test are removed); the
deadline reads `-Dvarka.warmup.deadlineSeconds` (default 60, production unchanged); the tests whose
subject is the verdict raise it, `VarkaWarmupEndToEndSuite` to 120 s with its waits at 150 s
and the forked probe of `VarkaKernelWarmupSuite` to 120 s, ended from outside after 240 s. A release at the 60-second
deadline is not asserted on by a test that cannot control the runner. The JIT-state diagnostic stays.

**What is still not known:** that the compile queue was the cause is inferred, not read: the
release path's diagnostic (the queue's head, the code cache) would show it and has not yet
fired on a runner. If it fires with an empty queue and a kernel that never compiles, this
correction is itself wrong, and the row reopens.

**Review of #688** (`/code-review high`) found: the test property was removed instead of restored
(restored now); the waits were stale in their messages (updated); the strict verdict had lost its
unit test (`blockClean` is a seam again, with the VARKA-295 counterexample as a case); the probe's
bound did not bound a hung child (a daemon thread ends it); the deadline property was not in the
class doc (it is); and, the finding that stands, the row is marked Done on a cause that is inferred:
production still releases at 60 s on a loaded host, and the row now reads "fix applied, cause
unconfirmed" until the release-path diagnostic has fired on a runner.

### 9.2 The diagnostic fired once, 12 October 2026, cut short

The release-path diagnostic fired on the fork's CI on 12 October (run 38223921205, `varka-279-impl`
at 4ef8f48443e, job "Varka suites: sql"). It fired before faacde3cc9e, which since that afternoon
runs `VarkaWarmupEndToEndSuite` alone after the module's other suites, as the gate's quiet step
does. The first projection test failed at the raised deadline:

    RELEASED did not equal COMPILED Outcome[shapeHash=wd36813e98d5e22ac, state=RELEASED,
    calls=107424, rows=95260672, queuedNanos=17846257, runNanos=119984889971,
    firstProbeBytes=5214992, lastProbeBytes=270576]; the first run took 683 ms; JIT: compile time
    333642 ms; CodeCache 45/128 MB; compile queue of 8 lines: Current compiles: | C1
    CompilerThread0  65099       3       jdk.internal.loader.NativeLib

**What it shows.** The warm-up task was not starved: it waited 18 ms in its own queue and ran
107,424 calls over the whole 120 s. The kernel was partly compiled, with its last probe block 19
times below the first, but at 270,576 bytes it was far over the 9 KB allowance and 7 to 14 times the
20 to 38 KB that one still-interpreted light loop left in 9.1's failures, so more of it was still
uncompiled. The code cache was not full, so the JIT had not stopped for lack of space. The
compiler threads had been busy, 334 s of compile time in a JVM that lived at most the job's 578 s
with two test JVMs sharing four vCPUs, but **at the deadline nothing was waiting for them.** The
queue's line count says so although its text was cut: `Compiler.queue` prints a header, one line
per compile in progress, and then for each of C1 and C2 a heading followed by its queued methods
or the single word `Empty`, with a blank line before each heading. That is 5 lines plus one per
compile in progress plus at least one per queue, and 8 lines with one compile in progress (the C1
compile shown) leaves exactly one line per queue: both `Empty`, and no C2 compile running. A JDK
25 JVM printed the same layout locally, 10 lines for three compiles in progress and two empty
queues.

**What that means for 9.1.** 9.1 inferred that the uncompiled loop methods were waiting in C2's
queue on a loaded runner. In this firing they were not in the queue at all, so for this failure
the inference is wrong. 9.1 named the case that would reopen the row as an empty queue and a
kernel that never compiles; the queue was empty and the kernel stayed mostly uncompiled for two
minutes, which is that case in all but the 19-fold drop. What keeps the methods out of the queue
is the open question: a compile that failed or bailed out and marked the method not compilable at
that tier, a deoptimization loop, or loop methods that never reach the tier policy's thresholds
under this warm-up. `-XX:+PrintCompilation` and JFR's `jdk.CompilationFailure` and
`jdk.Deoptimization` events in the failing JVM would tell them apart.

**What it does not show.** Which methods were compiled and which were not.
`dev/varka_matrix_report.py` cut each line of a failure's message at 400 characters, and the job
uploads no test reports, so the queue's text ended after its first entry; here only its line count
carried information. The cut is now `MESSAGE_WIDTH` = 4000 characters, wide enough for the JIT
state's twelve queue lines, so the next firing prints the queue whole.

**The rate since 9.1.** The sql job ran 46 times from 9 October to the night of 12 October and
failed this way once, against about two runs in five before 9.1. All 10 runs that include
faacde3cc9e passed. The rate fell, but the firing contradicts the cause 9.1 inferred, so the
row reads "fix applied, cause contradicted": the deadline and the quiet step made the failure
rarer without explaining it, and production still releases at 60 s when the kernel's loop
methods stay out of the compiler's reach. What remains is to find why they do, from the JIT's
own events in a failing JVM, before the test's wait or the verdict is changed.

### 9.3 Three firings in the module's JVM, 14 October 2026

The test failed three times on 14 October in "Build modules: sql - other tests", where it runs
after some 750 other suites in one JVM: #803 (`varka-255a` at 8bbacddebd4) on its first run and on
a rerun of that job, and #804 (`varka-sync-apache-1014` at 929a4ebf223, run 38503448829) on its
first attempt, which passed on the rerun. The failing and passing jobs ran the suites in the same
order. The three messages, each after the full 120 s:

    calls=108420, queuedNanos=32462928, firstProbeBytes=7942928, lastProbeBytes=24816;
    JIT: compile time 2983156 ms; CodeCache 72/128 MB; compile queue of 7 lines: Current
    compiles: | | C1 compile queue: | Empty | | C2 compile queue: | Empty |

    calls=108980, queuedNanos=93182403, firstProbeBytes=5882640, lastProbeBytes=20960;
    JIT: compile time 2915004 ms; CodeCache 70/128 MB; compile queue of 8 lines: Current
    compiles: | C2 CompilerThread0 ... sun.management.Util::newObjectName | ... | C2 compile
    queue: | ... javax.management.ObjectName::<init> |

    calls=108736, queuedNanos=22230628, firstProbeBytes=7942928, lastProbeBytes=24816;
    JIT: compile time 3086651 ms; CodeCache 72/128 MB; compile queue of 7 lines: (both Empty)

**They are 9.2's case, not 9.1's.** Nothing of the kernel's was queued: two queues are empty, and
the third's only entries are the `ObjectName` methods the diagnostic itself was calling. The
warm-up was not starved (18 to 93 ms in its own queue, some 108,000 calls) and the cache was not
full. The compilers had been busy all along, about 3,000 s of compile time in a JVM about 70
minutes old, but at the deadline they had nothing of this kernel to do.

**What the bytes say.** Run on the laptop at the runner's width (`-XX:MaxVectorSize=32`), the
first probe allocates 7,942,928 bytes, the same count as two of the failures, so the local and CI
probes can be compared byte for byte. Excluding one method at a time from compilation
(`-XX:CompileCommand=exclude`), with the test unchanged:

| left uncompiled | verdict | last probe, bytes |
|---|---|---|
| nothing | COMPILED | 1,040 to 2,240 |
| `run`, or every epilogue | COMPILED, from the four loops alone | 1,760 |
| one light loop (`loopDense0` or `loopMasked0`) | RELEASED | 49,312 |
| both light loops | RELEASED | 98,624 |
| the heavy masked loop | RELEASED | 2,556,144 |

An interpreted light loop allocates 24,656 bytes a call, and a probe makes two calls per driver.
CI's 24,816 and 20,960, and section 9's 20,720, are about one such call per probe: not a compiled
kernel with some residue left out of line (that is 1 to 2 KB), and not a light loop that never
compiled (that is 49,312). 9.2's 270,576 is about eleven such calls' worth. So in every
firing so far part of a light loop ran without compiled code while the compile queue was empty.
Two readings fit, and the next firing tells them apart: a light loop that never compiled and is
reached on one call in two (no `jdk.Compilation` event for it, no deoptimization), or compiled code
that a frame leaves once a probe through a trap and finishes interpreted (deoptimizations on `run`
or a light loop, thousands, all with one reason).

**Not reproduced locally.** The 26 Varka suites that precede it in that JVM, then the suite, in one
JVM on four cores: COMPILED in 0.24 s. All 754 suites that precede it in CI's order, then the
suite, in one JVM on four cores (`taskset -c 0-3`), 14,709 tests, at 512 bits and again at 256:
COMPILED both times, in 0.5 and 0.7 s, with compile ids near 936,000 and 945,000 against CI's
979,000, so the local JVMs had aged about as much. What CI has that the laptop does not is not
known.

**What changed.**

* The release-path state now lists the kernel class's compiled code, from `Compiler.codelist`
  (compile id, tier, state; on JDK 25 state 1 is not entrant), and the methods with no code in
  use, matched by name and descriptor (`VarkaKernelWarmup.jitState`). On a forced case it reads
  `no code in use: [loopDense0(7)]`. The list cannot mark an OSR compile, and it names classes
  rather than loaders; this test's shape is new to the JVM, so the second does not arise here.
* The test records the JIT's events while it runs (`VarkaJitEvidence`: every compile, compile
  failure, deoptimization and inlining decision, and the allocation samples, read back in two
  streaming passes from a bounded recording) and its failure message carries, for the kernel's
  class, each method's compiles with tier and outcome, the deoptimizations with their reasons, the
  inlining C2 refused, the Vector API classes it inlined (a second one would be the species
  pollution of `vector-api-and-width.md`), and where the warm-up thread's last 20 s of allocation
  came from, each frame marked interpreted, compiled or inlined. On the forced case that reads
  `loopDense0:1 (Interpreted)`. Any failure after the first run carries it, not only the
  verdict's. `-Dvarka.warmup.jitEvidence=always` logs it on a pass too.
* `dev/varka_matrix_report.py` keeps 12,000 characters of a message line, for the longer message.

**What is deliberately unchanged.** The test still fails the module's job. Cancelling it there, or
running it only in the quiet step, would stop the failures without explaining them, which 3.2 and
9.1 rule out; and the module's 70-minute JVM is the only place it has fired since faacde3cc9e,
and the closest thing CI has to a long-lived executor. The cost is that an unrelated pull request
can fail that job until a firing has been read; a rerun of the job has passed every time so far.
Production still releases a kernel at 60 s when this happens, and with one light loop interpreted
it then runs at most a few kilobytes a call above a compiled one, so the user-visible cost is the
minute on the row path, not the kernel's speed afterwards.

### 9.4 What 14 and 15 October add, and a plan to find the cause, 15 October 2026

*Evidence added since 9.3.* Three more observations bear on the cause, and none names it.

* **A local reproducer exists.** VARKA-327's fuzz arm with the warm-up on (#812) released 22
  warm-ups of 2,477 in a 10-minute JVM, a release rate of 6 to 7 per cent, and 29 and then 13
  in two runs of the same 400 compositions. All 22 had an empty C2 compile queue; 8 recorded
  `profile_predicate` or `unstable_if` traps (VARKA-326's deoptimization cycle: 3 of 40 drawn
  kernels cycled 97 to 110 times in 30 s on the warmed path); 21 had a kernel method with no
  code in use.
* **The symptom is reproduced without the cause.** The steady-state allocation test of
  VARKA-314 (#815) failed in a CI JVM with the same message as when C2 is forbidden to compile
  the kernel class (`-XX:CompileCommand=exclude,...VarkaFusedProjection_*::*`), to the
  character. That says what the symptom looks like, not why C2's output is missing.
* **The compile queue is not the cause.** The queue was empty at every release read so far,
  on CI and locally.

*The candidate causes.* The diagnostics so far cannot tell these four apart:

1. **A deoptimization cycle.** A trap makes the nmethod not entrant, the method recompiles and
   traps again, and the loop is interpreted for most of the window.
2. **A C2 bailout.** The compile fails ("out of nodes", the node limit, not retryable) and the
   method is then not compilable at tier 4.
3. **The tiered policy does not ask for the compile.** Its thresholds scale with the compile
   queue and the number of compiler threads, and a CI runner has 4 cores against this laptop's
   24.
4. **Compiled, but the Vector API boxes inside it,** so allocation survives in compiled code.

*The plan, cheapest first.* Each step either names a cause or removes one.

1. **Instrument the local reproducer** (hours): the warm-up fuzz arm with
   `-Xlog:jit+compilation=debug,deoptimization=debug`, `-XX:+PrintTieredEvents` and
   `-XX:+LogCompilation` on the kernel classes, and each release classified into the four
   causes. Bailouts print `COMPILE SKIPPED`; traps print their reason, action and bci.
2. **Join it with VARKA-326.** If cause 1 holds, 295 and 326 are the same bug and the question
   is why C2's recompilation does not converge on a loop-predication trap, with the JDK's own
   `CompilationPolicy` and `Deoptimization` sources (JDK 25, checked out locally) as the
   reference.
3. **Reproduce the CI environment locally:** the JVM pinned to 4 cores (`taskset`) with
   `-XX:CICompilerCount=3` beside a competing JVM, and the runner's JDK (Zulu 25.0.4-7) if it
   is available from a pinned source. A jump in the release rate points at cause 3.
4. **If the laptop cannot reproduce the CI rate, reproduce on CI:** a `workflow_dispatch` job
   that repeats the `sql - other tests` partition in eight parallel copies with the log flags on
   and the logs uploaded only on failure. The failure rate is about one run in ten, so a
   dispatch should give several failures for runner time, not laptop time.
5. **Make the diagnostics permanent and cheap.** The account of 9.3 is built at failure time and
   cannot see what happened earlier in the JVM's life. A rotating `-Xlog` file or a continuous
   JFR ring buffer limited to deoptimization and compilation-failure events survives to the
   failure.
6. **Ask the JVM directly** if the logs are inconclusive: `-XX:+WhiteBoxAPI` gives the
   compilation level, the compilability and the entry bci of each kernel loop method at the
   moment of release.

*What a result looks like.* A cause 1 or 2 result gives a concrete fix or workaround (a kernel
shape, or a flag the warm-up can set); a cause 3 result lets the warm-up stop waiting on a
deadline that the environment cannot meet; a cause 4 result is a Vector API escape-analysis
question for the kernels' emitted form. Row 295 stays open until a step names the cause.

### 9.5 Step 1 of 9.4, 15 October 2026: every release is cause 4, compiled and still allocating

**What was run.** The warm-up arm of `VarkaSparkFuzzSuite` (the `sparkwarm` step's test: `-z "both
sides of the warm-up handover"`, seed 20261008, 400 compositions, about 500 warm-ups, a 10-second
deadline in this arm) in a JVM started by hand from the classpath and `jvm.opts` that
`dev/varka_matrix.sh --module sql --list` exports, so it carries the options sbt's forked test JVM
has, at 512-bit lanes on the laptop, with a load average of 6.6 to 8.5 from other agents' work (not
idle). The JIT's own record was switched on with
`-XX:+UnlockDiagnosticVMOptions -XX:+LogCompilation -XX:+PrintTieredEvents
-Xlog:jit+compilation=debug,deoptimization=debug` (the log, one 1.8 GB XML per run, was read by
two streaming passes: every tiered-policy event, compile request, nmethod, uncommon trap,
not-entrant mark, bailout and C2 inlining decision of the `VarkaFused*` classes), and in two runs a
JFR recording with allocation samples (stack depth 12, frame types kept), deoptimizations and
compiles. The flags do not change what is measured: 41, 18, 18, 32, 26 and 36 deadline releases in
six runs of the same 400 compositions (3.6% to 8.2% of the warm-ups) against 29 and 13 in 9.4's two
runs without them. Seven runs and two replays in all; the logs and the extraction scripts are in the scratch
directory named at the end, not in the repository.

| run | JVM | flags beyond the log | warm-ups released at the deadline |
|---|---|---|---|
| t1 | 12 cores | none | 41 |
| t2 | 10 cores | + JFR (its 250 MB default cap kept the last 3 minutes of 6) | 18 |
| t3 | 10 cores | + JFR, uncapped | 18 |
| t4 | 12 cores | none (control) | 32 |
| t5 | 12 cores | `-XX:PerMethodTrapLimit=100000` | 26 |
| t6 | 8 cores | both `-XX:PerMethodTrapLimit` and `-XX:PerBytecodeTrapLimit` at 100000 | 151 |
| t7 | 4 cores (`taskset`) | `-XX:CICompilerCount=3`, the runner's scale | 36 |

**The result.** 171 releases in t1 to t5 and t7 (170 parse; one message was cut by interleaved
tiered-event output), each classified from the JIT's record of its own kernel class between the
warm-up's start and its release. The cause-4 class is assigned positively, not as what is left
over: every loop method of the kernel had a C2 nmethod, tier 4, installed and never marked not
entrant, and no class had a compile failure. The counts per cause:

| cause | releases | what was looked for, and what was found |
|---|---|---|
| 1, deoptimization cycle | 0 | a kernel method with 10 or more uncommon traps or 3 not-entrant marks. The most traps on any class are 8 (two loop methods, 4 each, `profile_predicate` with action `maybe_recompile`, in 11 of the 41 of t1); the most not-entrant marks 2. VARKA-326's cycle makes 97 to 110 in 30 s |
| 2, C2 bailout | 0 | a `<failure>` element inside any kernel compile task, or `make_not_compilable` at tier 4 on a kernel method: none in the six runs. JFR's 59 compilation failures in t3 (`retry without subsuming loads`, C2's own retry; two C1 `out of virtual registers`) include no kernel compile id. The only `make_not_compilable` on kernel methods is `excluded by CompileCommand` at tier 3, which is Varka's own C1 directive (`VarkaKernelCompileDirective`), not a bailout |
| 3, the policy does not ask | 0 | the last loop method of a released kernel had C2 code a median 0.5 s after the warm-up started, at most 3.3 s (t1); at the runner's scale (t7: 4 cores, 3 compiler threads) a median 0.8 s, p90 1.5 s, at most 5.7 s, and no more releases than on 12 cores (36 against 32 and 41). Epilogue methods with no C2 code were in 54 of the 170, 5 to 12 per run: each is a method whose tier-3 request the directive refuses and whose later events stop with the spin phase, and no probe runs one (1024 rows is a multiple of every lane count), so none explains a release |
| 4, compiled, in use, still allocating | 170 | see below |

`no code in use: [runDense(7), runMasked(7)]` in the release-path state is benign: those are 60 to
73-byte methods inlined into `run`'s tier-4 compile.

**Why cause 4 is the cause, three ways.**

1. *The bytes are one vector per loop iteration.* A probe is 4 calls of 1024 rows; a 512-bit int
   vector has 16 lanes, so each loop method runs 64 iterations a call, and an `int[16]` is 80
   bytes: 4 x 64 x 80 = 20,480 bytes. 45 of the 169 last-probe values are 20,480 to 22,400 bytes,
   the smallest 20,480 to the byte: **exactly one vector backing array a loop iteration survives
   in tier-4 code.** The rest are multiples: the median, 164,240 bytes, is eight arrays an
   iteration, the largest, 1,587,840, seventy-eight. A healthy kernel ends at 480 to 800 bytes, and
   the verdict's allowance is 4,096 + 4,096 + 1,024 a column, 9,216 bytes for one column, so one surviving array an iteration
   is enough to fail it for the whole deadline. In a replay of compositions 0 to 222 with a 20 ms
   `jdk.ThreadAllocationStatistics`, the warm-up thread's allocation counter grew a steady 3.9 MB/s
   over seconds 1 to 9, 20 KB per 5 ms pace interval, which is the same number from the other
   side.
2. *JFR puts it in compiled kernel frames.* In t3, 12 of the 18 releases have an allocation
   sample in their own kernel class after the first two seconds (sampling is sparse at 4 MB/s, and
   the other 6 have none); 99.8% of that weight is under a kernel frame of type `JIT compiled`,
   100% of it class `int[]`, and the top site is `IntVector.lanewiseTemplate:798` in 10 releases
   (the `VectorSupport.binaryOp` line, where a result vector is boxed) and
   `ScopedMemoryAccess.loadFromMemorySegmentScopedInternal:358` in 2:

   ```
   objectClass = int[]   weight = 3.0 MB   eventThread = "varka-kernel-warmup"
   IntVector.lanewiseTemplate(VectorOperators$Binary, Vector) line: 798      type: Inlined
   Int512Vector.lanewise(VectorOperators$Binary, Vector) line: 278           type: Inlined
   VarkaFusedProjection_w...loopMasked0(long[], long[], int[], long[], long[], int[], int)  type: JIT compiled
   ```
3. *The same class compiles clean in another run.* The 337 shapes seen in the six runs: 45 were
   released at least once, 10 in all six runs, 18 in four, and 35 of the 45 were COMPILED in
   another run. Their C2 compiles differ in one way the log shows: the number of `<virtual_call>`
   sites left in the loop and epilogue methods, a mean 1.19% of the call sites in the run that
   released and 0.55% in the run that did not, higher in 31 of the 35 shapes. For shape
   `w2d78f55c08bd76f0` (iteration 53) the `loopMasked0` compile of t1 (compile 34128, released) has
   119 `<virtual_call>` elements, 49 of them on `Int512Vector.lanewise` and 37 on `broadcast`; the
   one of t3 (compile 35233, COMPILED) has 34, with 18 and 8. Both are tier-4 compiles of the same 1,278
   bytecodes, with no failure.

The shapes that release are mostly the same ones run to run, so a release is a property of the
shape and of the JVM's profile history far more than of the machine or of timing, and
a replay of iteration 222 alone (`-Dvarka.sparkfuzz.warmupOnly=222`) is COMPILED; the first 223
compositions in a row released 1 of the 7 that t3 released in that range. The release-path state
the test already logs agrees with the JIT record: in t1's first release it lists the kernel's
five methods at `L4` and no loop method among those without code.

**What the virtual calls are, as a hypothesis for step 2 (not shown by step 1).** Each of those
`<virtual_call>` elements follows a `<predicted_call>` and an
`observe trap='class_check' count='0' mcount='0' ccount='136'`. In the JDK 25 source
(`src/hotspot/share/opto/doCall.cpp` 262 to 300) a monomorphic or bimorphic call that was inlined
gets an uncommon trap on the miss path, unless `too_many_traps_or_recompiles` says class-check
traps are already too many (`compile.cpp` 4128 to 4170: a trap recorded at this bci, or
`PerMethodTrapLimit` = 100 traps of the kind in the compile's inlined methods' profiles); then the
miss path is a real virtual call and the inlined path and the call rejoin, so a vector that
reaches the join cannot be scalar-replaced. The shared Vector API templates' profiles carry such
traps from every earlier kernel and species in the JVM (the deoptimization log has `class_check`
and `bimorphic_or_optimized_type_check` traps in `IntVector.bTest`, `AbstractMask.vectorSpecies`
and others). That is the "polluted template profile" of section 2, with a mechanism, but this step did not show that these virtual calls are where the 20,480 bytes come from, and t5
(below) says the compile-wide limit is not the only way to the virtual call: the per-bytecode
trap record in the shared methods' profiles (`has_trap_at`, `PerBytecodeTrapLimit` = 4) is the
other.

**Controls.** t5 (`PerMethodTrapLimit` 100000 alone) did not lower the rate (26 against 32 and
41), though the flag was in the JVM's arguments. t6 (both limits at 100000) made it five times
worse, 151 against 32, and made the other causes appear: 31 of its 133 parsed releases are cause 1
(`profile_predicate` traps by the hundred thousand, 84,926 `class_check` traps at one bci of the
harness's `VarkaFusedKernel.run`), 1 is cause 2 (`out of nodes during split` in `loopMasked0` of
shape `...8a2f200d`, which also releases at the defaults), 3 are cause 3. The trap limits are what
keeps the same C2 from the cycle of cause 1, so raising them is not a fix: it turns this symptom
into VARKA-326's. 9.4's step 2 (join with VARKA-326) is the same JIT with a different symptom.

**A correction candidate for 9.3, not asserted.** 9.3 read CI's 20,960 and 24,816 bytes as one
interpreted light-loop call per probe (24,656 bytes). At CI's 8 lanes one surviving `int[8]`
(48 bytes) per loop iteration over 4 calls of 1024 rows is 4 x 128 x 48 = 24,576 bytes, and the
two readings give the same number. Locally JFR's frame types say compiled code; for CI, the next
firing's frame-marked allocation samples (9.3) settle which.

**What the counts say about the next step.**

* Step 3 (the runner's scale) and step 4 (a CI dispatch) are not needed to find the cause: the
  runner's scale did not change the rate or the structure (t7), and the release follows the
  shape and the profile history. What the long CI JVM adds is more profile history, which fits
  9.3's 750-suite JVM.
* Step 2 changes shape: not a trap cycle, but what makes one allocation per iteration survive
  the tier-4 compile of this shape. Two replays of one shape that releases in all six runs
  (`67312f01`, iteration 220, or `e33b1eb0`, iteration 312), each after the compositions that
  precede it: one with `-XX:+PrintIntrinsics` (does `VectorSupport.binaryOp` at line 798 print
  `missing constant` or another rejection, so the Java fallback allocates, or is it intrinsified)
  and one on the fastdebug build with `-XX:+PrintEscapeAnalysis -XX:+PrintEliminateAllocations`
  (the allocation is there, so what keeps it from being scalar-replaced). The 20,480-byte
  arithmetic says there is exactly one allocation per iteration to find. A bisection of the prefix
  that makes iteration 222 release says which earlier kernels pollute.
* The verdict's allowance is crossed by one array per iteration, so how the warm-up should judge a
  kernel that is compiled at tier 4 but cannot be made allocation-free in this JVM is a
  question for after the cause: 9.1 withdrew the 32x rule for a reason that still holds.

**Every release of t1** (the first run; sorted by iteration; the shape is the last 8 hex digits of
the hash; all 41 are cause 4 by the test above; 'loops with C2 code' counts the kernel's loop
methods with a tier-4 nmethod in use at the deadline). t2 to t7 release mostly the same shapes;
their tables are in the scratch directory, with the per-run counts above.

| iter | shape (last 8 hex) | last probe (B) | loops with C2 code in use (first / last, s after start) | epilogues with none | traps on the kernel | virtual calls left (% of call sites) |
|---|---|---|---|---|---|---|
| 53 | 08bd76f0 | 430320 | 2 of 2 (0.3 / 0.5) | - | 0 | 2.8 |
| 79 | 783e7605 | 399600 | 2 of 2 (0.3 / 0.5) | - | 0 | 2.8 |
| 82 | 433382dc | 20960 | 2 of 2 (0.2 / 0.3) | - | 4 profile_predicate | 1.1 |
| 90 | a360d32e | 512480 | 6 of 6 (0.2 / 0.7) | epD0 | 0 | 2.5 |
| 98 | 4c712c1b | 389120 | 3 of 3 (0.1 / 0.2) | - | 0 | 2.3 |
| 104 | be396b93 | 22080 | 1 of 1 (0.2 / 0.2) | - | 0 | 0.8 |
| 109 | c7f4916e | 143680 | 4 of 4 (0.1 / 0.3) | epD1 | 0 | 1.5 |
| 110 | 5d560c10 | 31120 | 2 of 2 (0.2 / 0.4) | - | 4 profile_predicate | 0.8 |
| 119 | 2f926389 | 154240 | 4 of 4 (0.2 / 0.5) | - | 8 profile_predicate | 1.4 |
| 138 | cdbf41f3 | 31280 | 2 of 2 (0.2 / 0.3) | - | 0 | 1.2 |
| 156 | 72959b0a | 389120 | 3 of 3 (0.2 / 0.2) | - | 0 | 1.9 |
| 184 | b2184ac0 | 92880 | 4 of 4 (0.2 / 0.3) | - | 0 | 1.1 |
| 197 | 7746bae2 | 400080 | 6 of 6 (0.1 / 0.6) | epD0,epD2 | 0 | 2.4 |
| 206 | 8e294179 | 502080 | 2 of 2 (0.3 / 0.7) | - | 0 | 2.8 |
| 212 | ea10eeca | 72240 | 2 of 2 (0.2 / 0.2) | - | 0 | 1.6 |
| 220 | 67312f01 | 553680 | 4 of 4 (0.2 / 2.4) | epD1 | 4 profile_predicate | 2.5 |
| 222 | 78e9ebe3 | 492000 | 2 of 2 (0.3 / 0.6) | - | 0 | 2.9 |
| 224 | 831d5f52 | 21440 | 2 of 2 (0.2 / 0.3) | - | 8 profile_predicate | 0.7 |
| 226 | e9aa2b2e | 983600 | 4 of 4 (0.2 / 1.9) | epD0,epM0 | 0 | 3.4 |
| 232 | 32ce81a9 | 430480 | 6 of 6 (0.1 / 0.8) | epD0 | 0 | 2.2 |
| 234 | d6892e88 | 348160 | 3 of 3 (0.1 / 0.2) | - | 0 | 2.0 |
| 240 | 858f8853 | 430560 | 4 of 4 (0.2 / 0.8) | - | 4 profile_predicate | 2.0 |
| 260 | 64a5b133 | 164480 | 4 of 4 (0.2 / 0.5) | - | 4 profile_predicate | 1.7 |
| 263 | 4335e0df | 399680 | 4 of 4 (0.1 / 0.7) | epD0,epM0 | 0 | 2.8 |
| 268 | ac9d9354 | 20640 | 2 of 2 (0.2 / 0.2) | - | 0 | 2.1 |
| 274 | 1c25de6e | 563680 | 4 of 4 (0.2 / 0.8) | epD1 | 4 profile_predicate | 2.7 |
| 293 | b4b70692 | 40960 | 2 of 2 (0.1 / 0.1) | - | 0 | 1.3 |
| 302 | 1e23c716 | 430640 | 6 of 6 (0.1 / 0.5) | epD0,epD2 | 4 profile_predicate | 2.5 |
| 312 | e33b1eb0 | 1198720 | 2 of 2 (0.8 / 2.1) | - | 0 | 3.4 |
| 314 | 2789563e | 880640 | 1 of 1 (0.4 / 0.4) | - | 0 | 3.3 |
| 318 | f7b12c23 | n/a | 2 of 2 (0.1 / 0.2) | - | 0 | 2.5 |
| 319 | 785229a7 | 430480 | 6 of 6 (0.2 / 0.6) | - | 0 | 2.3 |
| 325 | 39ffed49 | 522640 | 4 of 4 (0.2 / 0.7) | epD0 | 0 | 2.6 |
| 326 | e63ac14f | 20480 | 2 of 2 (0.1 / 0.1) | - | 0 | 1.2 |
| 338 | 9787c504 | 82160 | 2 of 2 (0.2 / 0.4) | - | 4 profile_predicate | 1.7 |
| 339 | 6f882064 | 21040 | 2 of 2 (0.2 / 0.2) | - | 0 | 0.7 |
| 355 | 7603cfb3 | 348160 | 2 of 2 (0.1 / 0.2) | - | 0 | 2.5 |
| 364 | 8a2f200d | 983520 | 2 of 2 (0.6 / 3.3) | - | 0 | 3.2 |
| 370 | 5a9a1f8f | 410000 | 4 of 4 (0.1 / 0.5) | epD0,epM0 | 0 | 2.7 |
| 379 | 781e50f9 | 20800 | 4 of 4 (0.2 / 0.3) | - | 0 | 1.3 |
| 388 | a18ada49 | 400080 | 4 of 4 (0.3 / 0.7) | - | 4 profile_predicate | 2.0 |

The logs, the parsed per-class records and the scripts that produced this section are in
`/tmp/claude-1000/-home-max-proj-vecbricks-varka/53e2e690-d028-4b57-a163-fcdc477c97cf/scratchpad/295d/`
(`t1` and `t3` keep their `hotspot.xml` and `jit.log`, `t3` also its JFR recording; `tools/` has
the extractors); they are not in the repository.

### 9.6 Step 2 of 9.4, 15 October 2026: why the allocation survives, and who pollutes (follows 9.5)

**Cause.** A released loop compile has real virtual calls on class-check miss paths where a clean
one has uncommon traps (`doCall.cpp`). The vector joining from the call is a non-exact
`IntVector`, so the `binaryOp` intrinsic first refuses it, and the box that stays expands to a
`new int[16]` plus a `StoreVector` into it (`vector.cpp`); escape analysis removes the box but not
that mismatched-access array: 80 bytes, the 20,480. Two sums over the whole compile gate the miss
path, each adding a shared method's MethodData count once per inlined copy: class-check traps
against 100 (`PerMethodTrapLimit`) and decompiles against 201 (`PerMethodRecompilationCutoff`/2+1).
A clean `loopDense0` of `67312f01` totals 4 traps; a released one passes 100 at parse 427 of
3,309, as `VectorOperators.opKind` (inlined 170 times) holds 4 to 5 traps, set by standalone C2
compiles of `Int512Vector::lanewise`, `opKind` and `ImplCache::find` that speculated on a profile
and met another operator class. So (b), not (a): counts, not receiver-type profiles.

```
<observe trap='class_check' count='0' mcount='0' ccount='254'/>   (released; clean: none)
<observe trap='class_check' count='0' mcount='20' decompiles='1' mdecompiles='202'/>
JavaObject(23) NoEscape(NoEscape) is NSR. is used in LoadStore or mismatched access
```

| runs, seed 20261008, then iteration 220 | released |
|---|---|
| prefix `[]`, `[0]`, `[1]`, `[0,1]`, `[2,3]`, 6 runs each (ddmin: `{0,1}`) | 0,5,0,5,0 |
| prefix each of the 220 compositions alone, once | 55 (25%), no operator over 41% |
| `[0]`, 6 runs: no option / `TypeProfileLevel` 222 / 0 / `-UseBimorphicInlining` | 6, 4, 3, 4 |
| `[0]`: `PerMethodTrapLimit` / `PerMethodRecompilationCutoff` / both at 100000 | 5, 0, 0 |
| 400 compositions (500 warm-ups), no option, 3 runs: deadline releases | 18, 10, 39 |
| the same: cutoff x 3 / both x 2 / `PerMethodTrapLimit` x 1 | 3, 11, 3 / 0, 0 / 28 |

Composition 0 is `abs(ym)`; 54 more kernels do it too, a race between a template's C2 compile and
the first operator class it had not seen. Iteration 220 alone releases 0 of 12 on the product JDK,
2 of 4 on fastdebug. Both options removed every release in 1,000 warm-ups, the cutoff alone left
3 to 11. Not shown: which flipped sites are the vector-valued ones. No product code changed; the
suite gained `warmupSet` and `warmupShow`. Logs: `.../scratchpad/295e/` (`pq_*`, `fe1`, `full_*`).

### 9.7 Step 3: warm the shared templates first, and the releases go (follows 9.6)

**What was built.** `VarkaVectorTemplateWarmup`, run by `VarkaKernelWarmup`'s worker thread before
it takes its first job, once per JVM. It calls every Vector API method the emitter's kernels call
(`lanewise` unary, binary, shift, masked and scalar forms, `compare` and its mask forms, `blend`,
`broadcast`, `convertShape`, the reinterprets, `fromMemorySegment` and `intoMemorySegment` dense
and masked, the mask `and`/`or`/`not`/`anyTrue`/`toLong`/`fromLong`/`indexInRange`) on `IntVector`,
`LongVector` and `DoubleVector` at `SPECIES_PREFERRED`, with the operators the emitter names
(the arithmetic, bitwise and shift operators, `NEG`/`ABS`/`NOT`, the ten comparisons), interleaved
in each round so every template call site sees several operator classes while it is profiled.
`IntVector.compress` is in the list (the query-path `SelectionVectorOps` calls it). ROUNDS is 300. `-Dvarka.warmup.vectorTemplates=false` turns it off, for these measurements only.
The call in the worker is guarded (`VarkaKernelWarmup.templateWarmup`, any `Throwable`, logged, the worker goes on to its first job); the class's own `ensure` returns zero on a failure. `VarkaVectorTemplateWarmupSuite` pins: deterministic and used; the first `ensure` runs the body (a counter) and later ones do not; two threads run it once; an injected `Error` is absorbed and a thread reaches the code after the call; the property switches it off. The effect on the JIT is measured here, not asserted by a test, and a worker thread dying is not tested (the guard is).

**Which methods held the counts.** Runs of the two-composition reproducer with the
deoptimization log (`pc_*`, three each). Without the warm-up, `class_check` traps in the
`jdk.incubator.vector` templates, standalone compiles only: 24, 22 and 21; `VectorOperators.opKind`
4, 5 and 5 of them, `ImplCache.find` 4 (first run), `VectorMask.fromLong` 4 plus 4
`speculate_class_check`, `VectorMask.fromArray` and a `VectorMask` lambda's `load` the rest. With
it: 0, 0 and 0. What remains is `profile_predicate` and `osr_bci` traps in `ldLongOp`, `stLongOp` and
`bTest`, which are not class checks and are in both arms, and one more thing in all three runs:
`VectorMask.fromLong` still gets a tier-4 compile made not entrant by an uncommon trap, and 4
`speculate_class_check` traps at bci 24 (compile ids 9673, 9597, 9627). Bci 24 is
`AbstractSpecies.elementType()` on the `species` argument, so the speculation is on the species
class that call has seen, not on an operator; the warm-up feeds it the preferred int and long
species only, and a kernel's masks come from others (the conversions' half-width species). It did
not push any measured sum over 100 or 201, and releases went to zero, but it shows the profile
is not made polymorphic at every site, and the same mechanism could come back through a species
the warm-up does not use.

| arm (seed 20261008) | runs | deadline releases |
|---|---|---|
| reproducer, prefix `[0]` then iteration 220, without | 6 | 5 of 6 (9.6 had 5 of 6 too) |
| the same, with (1500 rounds) | 6 | 0 of 6 |
| the same, with 100, 300, 600 rounds | 6 each | 0 of 6 each |
| 400 compositions, about 500 warm-ups, without, 6 cores | 3 | 53, 78, 33 |
| the same, with 1500 rounds | 3 | 0, 0, 0 (COMPILED 497, 501, 500) |
| the same, with the shipped 300 rounds | 2 | 0, 0 (COMPILED 500, 499) |

Each run is one JVM on 6 cores (`taskset`), pairs with and without side by side, and every log
shows `Tests: succeeded 1`. The load average was 7 to 10 for the first six runs and 24 to 33 for
the 100/300/600 and the last two (other agents' work), so the without-arm's counts are higher than 9.6's
18, 10, 39 on 12 cores; the direction does not depend on it. Both options of 9.6 gave 0 of 1,000.

**What it costs.** The numbers come from `TMPCOST` lines of a temporary patch to
`VarkaSparkFuzzSuite` (the worker's `templateWarmupNanos` and each warm-up's queue and run time),
not in the branch, in the `c_*` and `r*` logs; the load average was 8 to 10 for the `c_*` runs and
24 to 33 for the `r*` ones. The branch now logs the warm-up's duration once at INFO
(`VarkaKernelWarmup.templateWarmupNanos`). On the worker thread: 312, 322, 404 and 658 ms at
1500 rounds; 238 to 398 ms at 600; 219 to 345 ms at 300 (6 runs); 123 to 572 ms at 100. It is
dominated by interpreting and class loading, not by the number of rounds. The first kernel's wait
(queue plus run, four runs each, 1500 rounds, `c_*`): 449 to 625 ms without the warm-up and 603 to
961 ms with it; the second kernel's run time fell from 607 to 639 ms to 301 to 379 ms, because the
templates are already compiled. It happens once per JVM on the warm-up thread while the shape's
batches take the row path, so the query that triggers it does not wait, but its shape's verdict is
later by the warm-up's duration, and that time counts against the 60 s deadline. A warm JVM re-runs
300 rounds in 28 to 42 ms (the unit suite), which is not the cost a fresh JVM pays.

**What it does not show, and what is not measured.**

* Steady-state kernel throughput with and without the warm-up. Only the warm-up job's run time
  and compile counts exist (COMPILED 497 to 501 of 500 against 421 to 467). The reasoning is that
  the emitter passes `getstatic VectorOperators.X` constants, so a call site folds by constant
  whatever the template's profile; that is reasoning, not a measurement. A benchmark pair
  (property on and off, a clean-compiling kernel, reported as rates) is owed, and was not run
  because the machine's load average was 23 while this was written.
* Non-preferred species. Production kernels bake the preferred species, which the warm-up feeds.
  A kernel at another species (`lanesOverride` 4 or 16, the option matrix) reaches the same
  shared templates, whose profiles now hold preferred-shape receivers: this could repeat the
  pollution, keyed by species instead of operator. No matrix run with the warm-up exists. The
  `speculate_class_check` residue above is the same kind.
* Query-path vector code. `IntRangeOps` and `SelectionVectorOps` run vector code on query threads
  before any warm-up thread exists, and a JVM that ran vector code first may already hold traps in
  the templates; the warm-up cannot remove them.
* That 300 rounds is the least that works: 100 rounds was clean in the 6 reproducer runs but was
  not run through the 400-composition arm. Whether the profile or the early tier-4 compile of
  `opKind`, `ImplCache.find` and the `lanewise` template is what matters (they reach tier 4
  inside the warm-up, `pc_on1` compile ids 9243 and 9377) was not separated.
* The alternating-cores question: every pair ran with the warm-up on cores 0 to 5 and the off arm
  on 6 to 11. The differences are large (53, 78, 33 against 0, 0, 0), but the pairs did not swap.

The property `varka.warmup.vectorTemplates` stays a system property, as `varka.warmup.deadlineSeconds`
is. Turning it into an internal SQLConf (string-keyed, a typed value bound once, as
`VARKA_WARMUP_ENABLED`) is a follow-up; the warm-up is JVM-wide and runs on a thread with no
session, so the likelier shape is a static conf read when the worker starts.

**After the review's changes** (the guard, `compress` in the list, `Arena.ofAuto`), on the final
build and at a load of 28 to 30: the reproducer 0 of 3 released, and one 400-composition run 0
deadline releases (COMPILED 497).

**Nothing else moved.** With the 300 rounds, under the defaults: `VarkaSparkFuzzSuite` (all arms),
`VarkaWarmupEndToEndSuite` and `VarkaProjectExecSuite`, 47 tests in sql, and the catalyst `*Varka*`
suites, all passed (the emitted bytes oracle is among them; no emitter code changed).

### 9.8 What 9.7 left owed: steady state, and the other species (15 October 2026)

Both measurements below ran on a quiet machine (load average 0.2 to 2.2, most of it the
measurement itself), one JVM at a time, pinned with `taskset` to cores 0-5 or 6-11, the arms
alternating between the two sets from one repeat to the next so that each pair swaps. The head was
0cd6d68edf2. Everything is in the scratch directory `295g` of the session, outside the branch.

**1. Steady-state throughput, warm-up on against off.** No existing benchmark starts the warm-up
worker: `VarkaArrowSessions.createSession` turns the kernel warm-up off unless asked, and the
throughput benchmarks do not ask. `VarkaColdStartBenchmark` does start it, but times first
queries. So a throwaway driver (`SteadyDriver.java`, not in the branch) was used. It builds the
benchmark ladder's session with the warm-up on, caches 2M dates in Arrow, and runs one projection
of four outputs `greatest(add_months(d, k), date_add(d, k), last_day(d))`, k from 1 to 4, to the
`noop` sink on one task thread. The first run claims the shape and queues the warm-up; the driver
waits for the verdict (`awaitIdle`; every run printed COMPILED, `fused=true`), sleeps 1.5 s, times
six runs ("early"), runs real batches for 3 s, then times ten runs ("steady"). The only difference
between the arms is `-Dvarka.warmup.vectorTemplates=false` (`templateWarmupMs` was 138 to 187 on
and 0 off in every run, so the worker did run the template warm-up in the on arm and not in the
off arm). Command, per run:

    steady.sh <tag> <cpus> [-Dvarka.warmup.vectorTemplates=false]

which is `taskset -c <cpus> java <the test JVM options> -Xmx8g -cp <driver>:<test classpath>
SteadyDriver`. Rates are millions of rows per second, higher is faster. The median of each JVM's
ten steady runs:

| run | cores | on | off |
|---|---|---|---|
| 1 | on 0-5, off 6-11 | 78.9 | 53.2 |
| 2 | on 6-11, off 0-5 | 35.6 | 78.4 |
| 3 | on 0-5, off 6-11 | 72.2 | 52.2 |
| 4 | on 6-11, off 0-5 | 51.8 | 77.9 |
| 5 | on 0-5, off 6-11 | 68.3 | 52.0 |
| 6 | on 6-11, off 0-5 | 51.7 | 69.1 |

Median of the six: 60.1 on, 61.1 off; the range is 35.6 to 78.9 on and 52.0 to 78.4 off. The
spread is not noise around one value. It is the core set: every JVM on cores 0-5 ran at 68 to 79
and every one on cores 6-11 at 36 to 53, in both arms. By set, the median of the three runs is
72.2 on and 77.9 off on cores 0-5 (68.3 to 78.9 against 69.1 to 78.4), and 51.7 on and 52.2 off
on cores 6-11 (35.6 to 51.8 against 52.0 to 53.2). The A/A runs (warm-up on in both, cores 0-5,
6-11, 0-5, 6-11) gave 49.6, 51.9, 75.9 and 51.0, so two identical arms differ by as much as the
two sets (75.9 and 49.6 on the same cores 0-5). I cannot tell why the cores differ (the
hardware is not known here) and one of the four cores-0-5 runs in the A/A set sat in the lower
mode. Within a JVM the ten steady runs are tight (a few percent) except in on1, on5 and off6,
which dropped by a third late in the series. The "early" six runs after the verdict are 17 to 47
and climb in every run of both arms, as the kernel's own batches finish compiling it.

What this shows: with 6 pairs of 10 timed runs there is no steady-state difference between the
arms that is larger than the JVM-to-JVM spread, which is 50 percent. The one gap that points at
the warm-up, the three cores-0-5 pairs (on 72.2 against off 77.9, 7 percent lower), is inside
that spread (the A/A pair on those cores is 75.9 against 49.6). It does not show that the
warm-up costs nothing: a difference of a few percent would not have been seen, and one kernel
shape was used.

**2. Non-preferred species, 400 compositions.** The option name is `lanesOverride`
(`VarkaEmitOptions`; the matrix lists it as `lanesOverride=4` and `lanesOverride=16`). It reaches
`VarkaSparkFuzzSuite` through `-Dvarka.matrix.config=lanesOverride=<n>`, since the suite's session
trait starts from `VarkaMatrix.base`. The arm is the 400-composition one of 9.7, with the matrix
property added: seed 20261008, `-Dvarka.sparkfuzz.warmupIterations=400`,
`-Dvarka.sparkfuzz.jitEvidence=false`, the test `both sides of the warm-up handover`, ScalaTest's
`Runner` on 6 cores, in the scratch copy of the earlier `run.sh`/`full.sh`. Three runs per cell,
the on/off order and the core set alternating (repeat 1: on 0-5, off 6-11; repeat 2 swapped;
repeat 3 as 1); every run printed `Tests: succeeded 1`. The machine's preferred species is 256
bits, eight int lanes, so 4 is a narrower and 16 a wider one than the warm-up feeds.

| lanes | warm-up | deadline releases | COMPILED | seconds |
|---|---|---|---|---|
| 4 | on | 183, 141, 144 | 313, 358, 355 | 1932, 1550, 1550 |
| 4 | off | 136, 139, 148 | 364, 360, 352 | 1501, 1495, 1627 |
| 16 | on | 0, 0, 0 | 497, 498, 498 | 126, 163, 128 |
| 16 | off | 43, 19, 59 | 456, 480, 440 | 581, 309, 744 |

(The RELEASED count in each log is the deadline releases plus 0 to 3 releases before the
deadline, which the 9.5 cause 4 does not cover.) For comparison, 9.7 had 53, 78 and 33 with the
warm-up off and 0 for all with it on at the preferred species.

So the template warm-up does what it did at the preferred species for 16 lanes (43, 19, 59 down to
0, 0, 0) and does nothing for 4 lanes: 183, 141, 144 on against 136, 139, 148 off, and each run
takes 25 to 32 minutes because every released kernel waits out the 60 s deadline. At 4 lanes the
warm-up makes things neither better nor worse that these three runs can resolve. The pollution
mechanism keyed by species that 9.7 feared is therefore real for the narrower species. The
repeats on swapped core sets agree within each cell, so the core set does not explain it.

**What remains unknown.**

* Why 16 is fixed and 4 is not. Not tested. The warm-up feeds the preferred species only; what
  16-lane and 4-lane kernels share with it (the half- and double-width species of the
  conversions, the masks) was not examined. The deoptimization log of a 4-lane run
  (`-Xlog:deoptimization*`, as in 9.7's `pc_*` runs) would say which templates trap; it was not
  taken.
* Whether feeding the narrower species in `VarkaVectorTemplateWarmup` (a second pass at the
  species one and two steps below the preferred) would clear the 4-lane releases. It is a larger
  warm-up (the cost in 9.7 is 140 to 190 ms for one pass here), so it is a design choice, not a
  measured result.
* Whether production ever emits at 4 lanes. `lanesOverride` is an option of the matrix; on a
  machine whose preferred species is 128 bits (the Apple Silicon laptop the benchmarks publish
  from) the preferred species is the 4-lane one and the warm-up feeds it, so the 4-lane releases
  here are a matrix-only result on this machine and would not appear there. That was not
  measured; the gate runs under `-XX:MaxVectorSize=16`, which makes `SPECIES_PREFERRED` the
  128-bit one and so exercises exactly that case, and was not run for this section.
* The throughput of kernels at 4 and 16 lanes with the warm-up on and off. Only the releases were
  counted.
* The query-path vector code point of 9.7 and the 100-round question are still open.

**3. The fix at volume (2,000 compositions per run, 5 cores each, load 0.5 to 6).** Warm arm,
seed 20261008, the seed whose earlier runs showed releases: 442 deadline releases without the
warm-up (`-Dvarka.warmup.vectorTemplates=false`), 4,854 s; 0 with it, 630 s. With the warm-up on,
seeds 20261009 to 20261013 gave 0 deadline releases each (553 to 707 s); the control at 20261009
also gave 0, so that seed does not separate the arms. Verdicts released before the deadline
(2 to 6 per run) occur with the warm-up on and off alike (6 in the control at 20261008), so the
warm-up does not cause them. One run, seed 20261008 with the warm-up on, failed at iteration 1405
with two different error classes (`CAST_OVERFLOW` against `INTERVAL_ARITHMETIC_OVERFLOW`): the
fixture's `y = 2147483647` row overflows both `ymy` and `ym`, and which error a side raises depends
on whether the view's unused column is computed (the cached no-batch side) or pruned (the row
engine). That is the fixture's own flaw, separate from the warm-up, and is not fixed here.
