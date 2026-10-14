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

* The release-path state now lists the kernel class's compiled methods, from `Compiler.codelist`
  (compile id, tier, state), and the methods with no code at all (`VarkaKernelWarmup.jitState`).
  On a forced case it reads `no code: [..., loopDense0, ...]`.
* The test records the JIT's events while it runs (`VarkaJitEvidence`: every compile, compile
  failure, deoptimization and inlining decision, and the allocation samples, read back in two
  streaming passes from a bounded recording) and its failure message carries, for the kernel's
  class, each method's compiles with tier and outcome, the deoptimizations with their reasons, the
  inlining C2 refused, the Vector API classes it inlined (a second one would be the species
  pollution of `vector-api-and-width.md`), and where the warm-up thread's last 20 s of allocation
  came from, each frame marked interpreted, compiled or inlined. On the forced case that reads
  `loopDense0:1 (Interpreted)`. `-Dvarka.warmup.jitEvidence=always` logs it on a pass too.
* `dev/varka_matrix_report.py` keeps 12,000 characters of a message line, for the longer message.

**What is deliberately unchanged.** The test still fails the module's job. Cancelling it there, or
running it only in the quiet step, would stop the failures without explaining them, which 3.2 and
9.1 rule out; and the module's 70-minute JVM is the only place it has fired since faacde3cc9e,
and the closest thing CI has to a long-lived executor. The cost is that an unrelated pull request
can fail that job until a firing has been read; a rerun of the job has passed every time so far.
Production still releases a kernel at 60 s when this happens, and with one light loop interpreted
it then runs at most a few kilobytes a call above a compiled one, so the user-visible cost is the
minute on the row path, not the kernel's speed afterwards.
