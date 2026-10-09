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
