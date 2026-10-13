# VARKA-274: The fuzzers check the code users run

## 1. Where this came from

Every answer check of a random kernel goes through `VarkaKernelChecks.runOne`
(`VarkaKernelChecks.java:556`). It defines the emitted class in a new loader, runs `run` once
over one batch of at most 1,000 rows (`VarkaIrFuzzSuite.scala:115`), compares, and releases the
loader. A class that runs once is never compiled, so the fuzzers' verdicts come from the
interpreter. The SQL suites that reach kernels through a session run with the warm-up off
(`VarkaSharedSessions.scala:111`), so a new kernel there also serves its few batches
interpreted. Users run something else: with the warm-up on, the default
(`SQLConf.VARKA_WARMUP_ENABLED`), a kernel serves batches only once C2 has compiled it
(`VarkaKernelWarmth`), and with the warm-up off, or after a warm-up's deadline, a kernel moves
through the interpreter, C1 and C2 while its batches run. The C2 vector code is checked against
an answer on three shapes only (`VarkaWarmupEndToEndSuite`).

Li, Jiang, Xu and Su ("Validating JIT Compilers via Compilation Space Exploration", SOSP 2023;
`m7/READING.md` 11) report, for a seven-day run against OpenJ9, that of the 154 seeds whose output
Artemis made differ, 138 (89.6%) "cannot be triggered simply by comparing the default and
fully-compiled JIT-trace" (p. 10, beside Table 4; checked against the PDF). Those needed the
mixed histories in between: a method compiled at one tier, recompiled at another, deoptimized,
entered by OSR. HotSpot's miscompilation in their example needed C1, C2, a deoptimization and an
OSR (p. 3). Artemis gives up on a seed after eight mutants (p. 10), which is where the row's
"about eight batch histories" comes from. The plan's section 2.2 ("Interpreted code")
and risk 8 take the row in; row 272, the interpreter, C1, C2 and intrinsics-off arms in forked
JVMs, follows it and reuses its tier proof (3.2).

## 2. The admission check, done

Probes on 13 October 2026, 15:45 to 16:15 CEST, on the laptop (AMD Ryzen AI 9 HX PRO 370, 24
threads, JDK 25.0.4.1, preferred int species 512 bits over a 256-bit datapath; load average 0.2
when the timed arms started), in a scratch worktree at `eca9171de4b` (master). The probe was a
Java class in the catalyst test package, run as a plain JVM on the catalyst test classpath with
`--add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED -Dspark.testing=1`. For
ground truth it used the JDK's WhiteBox API (`-XX:+UnlockDiagnosticVMOptions -XX:+WhiteBoxAPI`,
the class compiled from the OpenJDK source tree's `test/lib`), which reads a method's compile
level directly. WhiteBox ships in no JDK image, so it checked the facts and is not part of the
design. The probe drew int-lane shapes from `VarkaIrGrammar.drawShape` with the fuzzer's seed
(20260903), emitted them under `VarkaEmitOptions.DEFAULTS`, loaded each class once and ran its
batches through a harness that compares every row and validity bit with `VarkaReference`, as
`runOne` does. The probe and its logs were deleted afterwards.

**(a) Today's verdicts are interpreter-served.** `VarkaIrFuzzSuite`'s main test at its default
300 iterations, run with `-XX:+PrintCompilation` and a JFR recording (`settings=profile`): 7.7 s
wall, and not one compile line for a `VarkaFusedFuzz*` class at any tier. JFR's execution samples
caught 15 kernel frames, all of type `Interpreted`. What the interpreted frames call is not all
interpreted: the same run compiled 600 Vector API methods at tier 4 and 1,280 at tier 3 on their
own (they are shared by every kernel in the JVM), and `-XX:+PrintIntrinsics` showed 659 Vector API
intrinsics taken and 157 refused in those standalone compiles. So a fuzzer verdict today is the
interpreter running the kernel, calling library code that is partly intrinsified with every
vector boxed at the call boundary and partly the plain-Java fallback. Neither is the code C2
makes of a kernel, in which the vectors stay in registers. The composition fuzzer checks through
the same `runOne` and was not run separately.

**(b) Getting a fuzz kernel to C2 takes about a sixth of a second.** 40 drawn kernels (1 to 3
roots over 1 to 3 inputs; 4 to 16 kernel methods each), each warmed the way `VarkaKernelWarmup`
warms: calls of 32 rows plus the draw's length modulo 32, alternating the dense driver (no nulls)
and the masked one (every fifth row null), on a 1,024-row copy, then 16,000 calls and 200 ms
more. Under C1 excluded, as for production's warmed kernels (`VarkaKernelCompileDirective`):

| | min | median | p90 | max |
| --- | ---: | ---: | ---: | ---: |
| last C2 compile of the kernel after its first call (ms, from JFR) | 44 | 164 | 445 | 29,847 |
| the warm-up's allocation verdict (ms) | 29 | 122 | 343 | 29,054 |
| calls to that verdict | 5,874 | 14,916 | 16,368 | 327,888 |

Three of the 40 kernels (k = 8, 18 and 25) never settled: their loop methods entered the
`profile_predicate` deoptimization cycle (`the-jit.md`, "The deoptimization cycle is profiled
loop predication's"), 97 to 110 traps each in 30 s, and k = 8 still had a loop method at level
0 when the probe gave up. With C1 left on, the same 40 gave a median verdict of 122 ms and a
maximum of 497 ms, and k = 8 still cycled.

The warm-up's allocation verdict is not a C2 verdict. It reads what a 1,024-row call allocates,
and an epilogue that runs only the remainder rows allocates too little to show. At the moment
the verdict fired, 32 of the 39 kernels that reached it still had an epilogue method at level 0
and 3 had a loop method at level 0 (WhiteBox levels). The verdict is right for what it is for, a
kernel that has stopped boxing on its bulk rows; it cannot tell the checker that every method a
batch enters is C2 code.

The driver and dispatch methods (`run`, `runDense`, `runMasked`, 44 to 73 bytes) are often
never compiled on their own: JFR's `jdk.CompilerInlining` showed C2 inlining both drivers into
`run` ("inline (hot)") in some kernels, and in others `run` stayed interpreted while every loop
and epilogue was C2. All the per-row work is in the loop and epilogue methods
(`VarkaMethodNames.isGroupMethod`), which are never inlined into a driver ("hot method too big"),
so those are the methods whose tier a verdict has to name.

**(c) JFR proves which tier served a call, and costs nothing measurable.** A `RecordingStream`
in the test's own JVM, with no flags, on `jdk.Compilation` (threshold 0), `jdk.Deoptimization`
and `jdk.CompilerInlining`, reported every level-4 compile WhiteBox saw, and each trap that
`-Xlog:deoptimization=debug` logged (3 of 3 on the run compared). A test-only JFR event
committed around each compared call put every trap in its call: of the 27 traps taken after the
warm-ups, all 27 fell inside a compared call's window. The 40-kernel run took 101,527 ms with
the recording and 102,197 ms without it, and the median warm-up 352 ms against 357 ms. The
DiagnosticCommand MBean's `compilerCodelist`, the in-process `jcmd Compiler.codelist`, read in
about 3 ms the installed code of the whole JVM (3,233 lines): compile id, level and state per
method, `0` for in use and `1` for not entrant. It showed exactly the methods JFR had seen trap
as not entrant. In one run a `run` method read level 4 and later level 0 with no trap event; a
rerun under the deoptimization log did not compile that `run` at all, so the change was not
reproduced. Code can leave the code cache without a trap (an invalidation by a broken dependency
is not a JFR event), so the design reads a call's tier at both edges, from the code list, and
what happened inside, from the events.

**(d) An OSR mid-batch and a trap after a quiet warm-up can both be provoked, and both happen
on the users' paths.**

- **Traps.** After the warm-up above, which only ever ran remainders the slices give and never an
  all-null input, each kernel ran five batches. The batch with a remainder the warm-up never gave
  (1,000 rows) trapped in 9 of 40 kernels, an all-null first column in 28 of 40, the draw's own
  batch with nulls in 2 of 40; every trap was `unstable_if` with action `reinterpret`. The draw's
  own null-free batch trapped in none. Every answer matched the reference.
- **OSR.** On the warmed path, C1 excluded, one cold call of 2,000,000 rows OSR-compiled nothing
  in 12 kernels: with no tier-3 attempt the interpreter starts profiling late (`the-jit.md`, "A
  method C1 compiles at tier 2"). With C1 on, the path of a kernel run with the warm-up off or
  released at its deadline, every one of the 12 kernels OSR-compiled its loop methods at tier 3
  and then tier 4 inside that call. And on batches users have: 200 cold batches of 10,000 rows,
  C1 on, gave in each of the 12 kernels a tier-3 OSR compile of each loop method at batch 96 to
  101 and a tier-4 OSR compile at batch 163 to 186, each event inside one batch's window but
  three. The interpreter's back-edge counter crosses its threshold (60,000) a few calls before the
  invocation counter reaches its minimum (100 calls), so a 625-iteration batch is entered by OSR.
  All 2,400 batches matched the reference.

**What the check rejected.**

1. **The warm-up's verdict as the checker's.** It fires with epilogues interpreted (b).
2. **Per-method WhiteBox levels as the proof.** WhiteBox is not shipped, and a driver at level 0
   is usually inlined into a compiled `run` or harmless; the proof names the loop and epilogue
   methods only.
3. **"OSR mid-batch" on the warmed path.** It does not occur there (d). The OSR history runs the
   path where it does, C1 on.
4. **Out-of-contract values as a trap trigger.** The probe's first trap history put
   `Integer.MIN_VALUE` and `MAX_VALUE` on valid lanes; 11 of 40 kernels then answered differently
   from the reference, and the first of them (k = 2) did the same on a cold, interpreted class.
   Those values are outside the bound the grammar draws within (`VarkaIrGrammar.COLUMN_BOUND`),
   where neither side's answer is specified, so the guard history takes row 279's guard edges
   instead (3.3).
5. **Warming every draw of the nightly.** A kernel's seven compiled histories cost about 2.5 s
   from the numbers above: five warm-ups of about 350 ms on classes of their own, a recompile, and
   200 batches of 10,000 rows for `released`, which took 6.5 s for 12 kernels with every row
   compared. Against the nightly's 10,000 iterations a lane (`dev/varka_nightly.sh:54`) that is
   about 14 hours in one JVM; the compiled histories take a sample of draws instead (3.4).

## 3. The design

### 3.1 One class per kernel, run over a history

A **history** is the sequence of calls a kernel class meets before and during its compared
batches. `VarkaKernelChecks` gains a form of `runOne` that takes an already loaded kernel, so a
history loads its class once and compares as many batches on it as it needs; today's entry points
keep loading a class per batch and their messages do not change. A new Java class beside it,
`VarkaCompiledCheck`, runs a drawn case over each history in turn, each history on a class of its
own, since a history is exactly what ran on the class before:

| history | before the compared batch | compared, and the tier it must show |
| --- | --- | --- |
| `cold` | nothing | the drawn batch, interpreted: today's check, kept as it is |
| `warm-same` | warmed on the drawn batch's own values, both drivers | the drawn batch, both bodies: C2 |
| `warm-other` | warmed on another draw of the same columns, other nulls | the drawn batch, both bodies: C2 |
| `quiet-remainder` | warmed with remainder-free calls only | a batch whose length leaves a remainder: C2 or a trap |
| `quiet-null` | warmed with partial nulls only | a batch with an all-null input: C2 or a trap |
| `quiet-guard` | warmed inside every guard | row 279's edge values, on the decline side and the answer side: C2 or a trap |
| `recompiled` | `quiet-null`'s trap, then calls until the method is C2 code again | the trapping batch again: C2 after a deoptimization |
| `released` | C1 on, no warm-up: 200 batches of 10,000 rows | every batch: the interpreter, tier-3 OSR, tier-3, tier-4 OSR, C2, each compared |

Eight histories, seven of them new. Every compared batch is compared in full with the reference
whatever tier served it; the tier is a second assertion, about whether the history happened.

The warmed histories name their class with the warmed mark (`VarkaShapeCacheImpl.WARMED_MARK`)
under a fuzz prefix and install production's directive through
`VarkaKernelCompileDirective.readyForWarmup`, so C1 is kept off them as it is off a user's warmed
kernel. `released` uses a prefix the directive does not match, so C1 is on, as for a kernel run
with the warm-up off.

### 3.2 The tier witness

`VarkaTierWitness` (Java, catalyst test tree) holds one `RecordingStream` per JVM on
`jdk.Compilation`, `jdk.Deoptimization` and a test-only event, `VarkaCheckedCallEvent`, which
the check commits around each compared call with its history and batch. Before and after each
compared call it reads `compilerCodelist`. For a call it gives a verdict, a sealed type:

- **`C2`**: every loop and epilogue method of the body the batch selects
  (`VarkaMethodNames.loop`, `epilogue`, by the batch's null counts) is at level 4 and in use
  before and after the call, and no trap fell in the call's window;
- **`Trapped(method, reason)`**: a `jdk.Deoptimization` event fell in the window;
- **`Osr(level)`**: an OSR compile of one of those methods finished in the window;
- **`Mixed(levels)`**: some of those methods were below level 4 when the call began, and nothing
  happened inside.

Drivers and `run` are left out of the verdict: they hold no per-row work and are often inlined or
interpreted (2(b)). JFR delivers events about a second late, so the witness settles a history's
verdicts after its calls, on the stream's flush, and the check waits for it. Row 272 runs the same
histories in forked JVMs under `-Xint`, `-XX:TieredStopAtLevel=1` and the intrinsics off, and
asks the same witness which tier served: under `-Xint` it must see no compile at all.

### 3.3 Warming to C2, and what it is waited on

The warm-up is the check's own, not `VarkaKernelWarmup` (whose job and state machine serve the
production cache, and whose allocation verdict is not a C2 verdict, 2(b)). It reuses that class's
call shape: 32 rows plus the batch's remainder per call, alternating the drivers, on a 1,024-row
copy. It stops when the code list shows every loop and epilogue method of both drivers at level
4 and in use. A kernel that has not settled after 10 s (the measured p90 is 445 ms) is
**unsettled**: its batches are still compared and their verdicts recorded, the test reports the
count, and the shape is listed for the nightly's deoptimization-cycle guard rather than failed.
The `quiet-guard` history needs a draw with a guard; its batches come from `VarkaGuardSpecs`, the
guard-edge values row 279 builds, and a draw with no guard skips it.

### 3.4 Where it runs, and how much

- **`VarkaIrFuzzSuite`** gets a test, "random IR trees compiled by C2 match the reference
  evaluator over eight batch histories", at both lanes, over the first `-Dvarka.fuzz.compiled`
  draws (default 20) of the run's own seed, so a failure replays with the fuzzer's
  `-Dvarka.fuzz.only`, and `-Dvarka.fuzz.history=<name>` runs one history alone. Draws whose options set a lanes override run `cold` only and are counted:
  a second species in the JVM makes every later kernel's C2 code box at the merge of two receiver
  classes (`vector-api-and-width.md`), which no user's single-species JVM runs. The suite already
  runs in a JVM of its own (`VarkaOwnJvm`).
- **`VarkaCoverageCompositionFuzzSuite`**: its test of every coverage row's kernel alone runs
  `warm-same`, `quiet-null` and `released` per row, so each lowering the table lists is checked
  as C2 code.
- **The nightly** (`dev/varka_nightly.sh`) runs the compiled test over 2,000 draws a lane, split
  over eight JVMs by draw range, and prints the verdict counts per history and the unsettled
  shapes: at 2.5 s a kernel, 4,000 kernels over eight JVMs is about 21 minutes, if eight JVMs and
  their compiler threads share the 24 cores without slowing each other's compiles (prediction 9).

### 3.5 Deliberately unchanged

- **`VarkaSparkFuzzSuite` and the session suites** keep the warm-up off. Holding a random
  composition to Spark with the warm-up on needs each query run twice with a wait for the verdict
  between, as `VarkaWarmupEndToEndSuite` does; that is a candidate row (3.7), not this one.
- **`VarkaKernelWarmup`'s allocation verdict.** It decides when production stops taking the row
  path, and an epilogue interpreted on a batch's remainder costs little there; whether it should
  wait for the epilogues is a measurement of production's cost (3.7).
- **The JVM flags of the test JVMs.** The witness needs JFR and the DiagnosticCommand MBean, both
  in every JDK 25; forked JVMs with other flags are row 272's.
- **The wide compositions** (`drawWideShape`, 250 outputs and more) are not warmed: a wide kernel's
  C2 compiles take seconds each (`the-jit.md`, "A wide loop method's cliff"), and row 272's forked
  arms are the place to price them.

### 3.6 Registered op counts

None: no emitted code changes.

### 3.7 Candidate rows found by the check

1. **Small fuzz kernels enter the deoptimization cycle.** 3 of 40 drawn kernels, each of 1 to 3
   roots with only a masked driver, cycled on `profile_predicate` for 30 s under the warmed path
   (97 to 110 traps each), one of them with C1 on as well. The nightly's guard forks one
   `make_date` kernel only; the fuzz grammar's shapes reach the cycle more often than that guard
   can see. A row would census the cycle over the grammar's shapes and decide between a guard and
   a change to the emitted loop.
2. **The warm-up switches before the epilogues are compiled.** In 32 of 39 kernels. A row would
   measure what an interpreted epilogue costs per batch on production's paths and, if it shows,
   add the code list to the verdict.
3. **The Spark-level fuzzer with the warm-up on** (3.5).

## 4. Files

| file | what |
| --- | --- |
| `sql/catalyst/src/test/java/.../varka/VarkaKernelChecks.java` | the comparison over an already loaded kernel; today's entry points unchanged |
| `sql/catalyst/src/test/java/.../varka/VarkaCompiledCheck.java` | the histories, the check's warm-up, the unsettled count |
| `sql/catalyst/src/test/java/.../varka/VarkaTierWitness.java` | the recording, the code list, the per-call verdict |
| `sql/catalyst/src/test/java/.../varka/VarkaCheckedCallEvent.java` | the test-only JFR event around a compared call |
| `sql/catalyst/src/test/scala/.../varka/VarkaIrFuzzSuite.scala` | the compiled test at both lanes |
| `sql/catalyst/src/test/scala/.../varka/VarkaCoverageCompositionFuzzSuite.scala` | three histories per coverage row |
| `sql/catalyst/src/test/scala/.../varka/VarkaTierWitnessSuite.scala` | the witness's own tests (5) |
| `dev/varka_nightly.sh` | the compiled step, eight JVMs |
| `sql/varka/skills/the-jit.md`, `SKILLS.md` | what 2 found: the code list, OSR on 10,000-row batches with C1 on and none with it off, the allocation verdict and the epilogues |

## 5. Tests, and what each is for

1. **The witness sees each tier.** A kernel run once is `Mixed` with every method at level 0; the
   same kernel warmed is `C2`; a warmed kernel given an all-null input reports `Trapped` with
   `unstable_if`. Without this, a witness that always said `C2` would pass every history.
2. **The witness sees an OSR.** The `released` history's first 200 batches on one fixed shape
   report a tier-3 and a tier-4 `Osr` verdict on some batch.
3. **A trap is attributed to its call.** Two calls back to back, the second trapping: the trap is
   in the second's window and not the first's.
4. **The code list is read right.** The witness's parser over code-list text captured in 2's
   probe: a method listed at level 4 with state 0 counts as C2 code, the same method with state 1
   (not entrant) or missing does not. This is the read that would catch code leaving the cache
   without a trap event (2(c)); it is tested on captured text because no shipped API invalidates
   a method on demand.
5. **A planted wrong answer fails every history.** A test hook adds one to the reference's answer
   for one output on one row; each of the eight histories fails, its name in the message. Without
   it, a history that compared nothing would pass.
6. **Unsettled is a result, not a failure.** A kernel given a 1 ms settle deadline is reported
   unsettled, its batches compared.
7. **The replay works.** A failure in the compiled test names the seed, the iteration and the
   history, and `-Dvarka.fuzz.only` with `-Dvarka.fuzz.history` reruns just that.

No pinned fixture moves.

## 6. The measurement

The compiled test at its default (20 draws a lane) and the nightly's 2,000 draws a lane: the time
per kernel and per history, the verdict counts per history, the unsettled shapes, and any
disagreement. The PR-CI arm's time on the laptop and on a GitHub runner.

### 6.1 Predictions, registered before the run

1. No compiled history finds a wrong answer at 20 draws a lane; at 2,000 the chance of one is
   small but not nil, and a finding is fixed or becomes a row before the plan closes.
2. A kernel's seven compiled histories cost under 3 s on the laptop (about 2.5 s from 2), so the default arm adds under
   two minutes to `VarkaIrFuzzSuite` (two lanes, 20 draws each).
3. `warm-same` and `warm-other` show `C2` on at least 95% of settled draws.
4. `quiet-null` traps on at least half of the draws whose kernel has a masked driver (28 of 40
   measured), `quiet-remainder` on at least a sixth (9 of 40).
5. `released` shows a tier-4 OSR verdict on at least 90% of draws within its 200 batches (12 of
   12 measured).
6. Between 2% and 15% of draws are unsettled at 10 s (3 of 40 measured), and every unsettled one
   shows `profile_predicate` traps.
7. The long lane's kernels settle in the same time as the int lane's, within a factor of two at the
   median.
8. The JFR recording and code-list reads add under 5% to a history's time.
9. The nightly's 4,000 kernels over eight JVMs take under 30 minutes on the laptop.

## 7. Risks

1. **The JIT is not deterministic.** Compile timing depends on the machine's load, so a history's
   tier is a measured outcome, not a given. That is why the answer is compared whatever the tier,
   the tier verdict says whether the history happened, and an unsettled kernel is counted, not
   failed. A tier assertion that fails on a loaded runner fails as "history not reached", with the
   code list in the message, never as a wrong answer.
2. **One species per JVM.** A lanes-override draw in the same JVM boxes every later kernel's C2
   code; the compiled test runs those draws `cold` only (3.4), and `VarkaSpeciesGuard` already
   fails a suite that adds a species in the shared JVM.
3. **The cycle eats the budget.** An unsettled kernel holds its 10 s; three in forty would add
   about a second a kernel on average. The nightly's per-JVM watchdog is raised to match
   (`testing-and-debugging.md`, "A long opt-in run needs the suite timeout raised").
4. **JFR's late delivery.** A verdict read before the stream flushed would miss the last events;
   the witness settles on the flush callback after the history's last call, never on a sleep.
5. **The directive is JVM-wide.** Installed once by the compiled test, it reaches every warmed-mark
   class in that JVM; the fuzz prefix keeps it off other suites' kernels, and the suite's own JVM
   keeps it off other suites.

## 8. Sequencing

Two pull requests.

1. **274a: the witness and the histories.** `VarkaTierWitness`, the event, `VarkaCompiledCheck`
   with all eight histories, the loaded-kernel form in `VarkaKernelChecks`, the compiled test in
   `VarkaIrFuzzSuite` at the default 20 draws, and `VarkaTierWitnessSuite`. Proved by tests 1 to 7
   and predictions 2 to 5, 7 and 8.
2. **274b: wider and nightly.** The coverage rows' three histories, the nightly step over eight
   JVMs, and the skills section. Proved by predictions 1, 6 and 9 from the first nightly. Then
   section 9.

## 9. Outcome
