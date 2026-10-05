# VARKA-286: the gate in parallel

## 1. Where this came from

Asked on 5 October 2026, during VARKA-250's gate run, whether the gate used all the laptop's
cores: it does not. `dev/varka_gate.sh` runs its steps one after another, and its two test steps
run each module's Varka suites in one forked sbt test JVM, so the 24-core laptop sat at a load
average of about 2.7 for most of a 35-minute gate (VARKA-250's run: compile 160 s, wide 807 s,
narrow 809 s, sweep 144 s, doc 62 s, bench 30 s, lint 61 s, quotes 1 s, 2,074 s in all). The
owner's direction: "we have many cores in the laptop. try to utilize all of them when it is
possible."

## 2. The admission check, done

**The test time is there to split.** VARKA-250's wide log holds 757 s of test time in 74 suites;
the longest suite, `VarkaWidthAuditSuite`, takes 153 s and the next, `VarkaDifferentialSuite`,
125 s, so with the suites balanced across JVMs a width's wall time is bounded by about the
longest suite plus a JVM's start, not by the sum. The option matrix's runner (VARKA-248,
`dev/varka_matrix.sh`) already runs the suites as plain ScalaTest JVMs on the classpath sbt
exports, several at once, each in its own directory.

**The narrow step has never been narrow.** It sets `JAVA_OPTS="-XX:MaxVectorSize=16"` around
sbt. sbt forks the test JVMs (`catalyst/Test/fork` is `true`) and gives them
`Test/javaOptions`, which under that environment holds no `MaxVectorSize`; `JAVA_OPTS` reaches
neither their options nor their environment, and the `java` launcher does not read it anyway. So
every gate's narrow step has run the suites a second time at the host's width. Width inside the
tests was covered all along - the bytes oracle and the option matrix emit through
`lanesOverride` - but no gate has run the suites with the JIT held to 128-bit vectors. Passing the
flag to the narrow JVMs directly is part of this task, and the first truly narrow run may fail
where the old one could not.

## 3. The design

### 3.1 Three lanes after one build

The build stays one sbt invocation: compile both modules' tests, export the sql test classpath
(which holds catalyst's test classes too) and the test JVM options. Then three lanes run at once:

* **The suites.** `wide` and `narrow` each run every Varka suite of both modules - the same set
  as `testOnly *Varka*` - split across several JVMs per module, `narrow`'s with
  `-XX:MaxVectorSize=16` on each JVM's own command line. The suites are spread by longest
  first over the JVMs, weighted by their times in the previous gate's JUnit reports when there
  are any, evenly otherwise.
* **sbt.** `doc`, `lint` and `sweep`, one after another, since two sbt invocations in one
  worktree contend for its lock.
* **The rest.** `bench` (Maven) and `quotes`.

Two suites measure the JIT rather than answers and are moved out of the parallel wave into a
quiet phase at the end, one JVM per width: `VarkaAssemblySuite`, which reads compiled code, and
`VarkaWarmupEndToEndSuite`, whose first-query tests read a kernel's compilation state, which the
option matrix showed a loaded machine moves.

The runner gains what this needs and nothing else: `--defaults` (run no configuration, only the
defaults, and every suite - the gate runs the bytes oracle and the other suites the matrix
leaves out), `--split N` (each module's suites over N JVMs), `--jvm-arg` (an option on every test
JVM's command line), `--out DIR` (its working directory, so two runs can share one build), and
`--suites` / `--skip-suites` (the quiet phase and the parallel wave). The gate keeps its step
names, its `--only` / `--skip`, its per-step logs and its summary table, so a caller sees the
same gate, faster.

### 3.2 What is deliberately unchanged

What the gate runs: the same suites, the same sweeps, the same linters, now with a real narrow
width. The steps' deadline (VARKA-283). CI, whose jobs run sbt as before. The option matrix's own
behaviour when `--defaults` is not given.

### 3.3 Registered op counts

None: no emitter change.

## 4. Files

| file | what |
|---|---|
| `dev/varka_matrix.sh` | `--defaults`, `--split`, `--jvm-arg`, `--out`, `--suites`, `--skip-suites` |
| `dev/varka_matrix_report.py` | a module run in several JVMs, read as one |
| `dev/varka_gate.sh` | the build, then the three lanes, then the quiet phase |
| `m7/VARKA-286.md`, `m7/PLAN.md` | this plan, row 286 |

## 5. Tests, and what each is for

* The gate itself, run before and after on the same commit: the same suites pass in both, and
  the parallel gate's per-step logs carry each suite's results.
* The narrow step's width checked from inside a test JVM: the preferred species is 128 bits
  there.
* A deliberately failing suite shows the step FAILED and names the failure, as before.

## 6. The measurement

The whole gate's wall time on the laptop, before (VARKA-250's 2,074 s) and after, on the same
commit, with the load average sampled during the test lanes.

### 6.1 Predictions, registered before the run

1. The gate takes under 12 minutes, from 35; the test lanes, from 27 minutes, take under 6.
2. The load average during the test lanes is above 15.
3. The narrow step, now narrow, passes; if it does not, each failure becomes its own row.

## 7. Risks

1. **A suite that depends on running alone or after another** in one JVM - a shared Spark
   session's state, a shape cache warmed by an earlier suite. The split JVMs each start clean, as
   the matrix's did; a failure there shows in the comparison with the sequential gate.
2. **Memory**: about a dozen 4 GB test JVMs beside sbt's doc and lint, within the laptop's 83 GB.
3. **Power**: a ten-minute full load on the 60 W the USB-C supply negotiated drains the battery a
   little; the matrix runner's battery pause applies.

## 8. Sequencing

One pull request: the runner's options, the gate's lanes, the measurement and row 286.

## 9. Outcome

### 9.1 The parallel gate, 5 October 2026

Run twice on the same commit, the first time with no JUnit history in the gate's new directories
and so every suite weighted alike, the second weighted by the first's times:

| | sequential (VARKA-250's run) | parallel, unweighted | parallel, weighted |
|---|---:|---:|---:|
| the whole gate | 2,074 s | 781 s | **682 s** |
| wide | 807 s | 560 s | 498 s |
| narrow | 809 s | 563 s | 505 s |
| JVMs per width, slowest to fastest | 1 per module | 560 to 120 s | 504 to 347 s |
| load average, median and peak | about 2.7 | 17.5, 44.5 | 24.8, 33.8 |

1. **Held.** The gate takes 682 s, 11.4 minutes, from 35: three times as fast. **Missed** for the
   test lanes: 505 s, 8.4 minutes, not under 6. The machine is saturated while they run - a
   median load of 25 on 24 cores - and each width's six JVMs add up to about 2,560 s of wall time
   where the same suites hold 757 s of test time run alone: every JVM brings its own JIT and GC
   threads, and the SQL suites run local Spark with several threads each, so the lanes are bound
   by contention, not by cores left idle, and more JVMs would not shorten them. The weighting is
   worth keeping: unweighted, one JVM ran 560 s while another finished in 120.
2. **Held.** The load average's median is 24.8.
3. **Held.** The narrow step, now really at 128 bits - its log prints "the preferred vector is
   128 bits under the narrow step's flag", from `dev/varka_vector_bits.java` run with the step's
   flag - passes every suite, in both runs.

The sbt steps run slower beside the suites than alone - doc 236 s from 62, lint 172 s from 61 -
and still finish inside the suites' lane. The lesson of the narrow step is in
`sql/varka/skills/build-and-environment.md`.

