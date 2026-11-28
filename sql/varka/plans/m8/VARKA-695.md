# VARKA-695: The width audit's cost, cut to what the census needs

This file plans step 2 of row 695 (`PLAN.md` section 3). Step 1, which runs each distinct width
once, is #1839 (branch `varka-695-width-probes`) with its readings in
`results/VARKA-695-probe-skip.txt`; the CI split that gives the audit a job of its own is #1840.
The owner approved planning step 2 on 28 November 2026. Every runner number below is from
`results/VARKA-695-runner-probes.txt` (sections 1 to 4 there) unless another file is named.

## 1. Where this came from

Row 695's step 2: "if the 128-bit probe, which the first step leaves at about 350 s, is still above
300 s, run the epilogue phase of the audit at the preferred width only, or split the probes over CI
jobs, whichever the probe times in the Build's log show saves more, and say in the census which
width its epilogue column no longer covers". The row's done-when: "the slowest probe on a four-core
runner takes under 300 s, half the budget, with the pinned census unchanged or its change named, and
no probe of the width audit has passed 600 s for a week".

A probe is one child JVM of `VarkaWidthAuditSuite` running every audited shape at one vector width
(`VarkaWidthAuditProbe`). Since #1672 (row 550, `VARKA-550.md` 10.1) each shape runs in two
phases: 20,000 dense and masked calls at 256 rows, then a marker, then as many at 255 rows, which
no lane count divides, so the second phase is the only one that reaches the epilogue.

## 2. The admission check, done

**The condition holds: the 128-bit probe is above 300 s after step 1.** On the head of #1839 the
"Varka width audit" workflow (run 46154115029) took, on an AMD EPYC 9V74 with AVX-512, 330 s at the
preferred width, 479 s at 128 bits and 363 s at 256 bits (results section 1b). In check mode, as a
Build runs it, that is three probes and 1,172 s.

**The Build's log does not show the probe times.** The Build of the same head (run 46154115377,
Intel Xeon 6975P-C) ran the audit in 1,055 s by its JUnit report, but the scoped runner writes the
suite's stdout to `target/varka-scoped/runs/defaults/<unit>/run.log`, which the job neither prints
nor uploads, and the report's `system-out` is empty (section 1a). The same holds for master's run
46146333651 (1,828 s, five probes). The probe times exist only in the width-audit workflow's log,
and there the aarch64 job's lines were cut by the step's `tail -60` (section 1c). So the row's
"probe times in the Build's log" cannot be read today; commit 1 below makes them readable, and the
done-when's week of readings needs it.

**Every reading by runner class, after step 1, check mode** (sections 1 and 2; the EPYC 9V45 and
7763 rows are the five-probe runs with the probes step 1 skips left out):

| runner | preferred width | probes that run | slowest | sum |
|---|---|---|---|---|
| EPYC 9V74 (1b) | 512 | 330, 363 (256), 479 (128) | 479 | 1,172 |
| EPYC 9V45 (run 46024593085) | 512 | 246, 266 (256), 356 (128) | 356 | 868 |
| EPYC 7763, AVX2 (run 45988076736) | 256 | 450, 631 (128) | 631 | 1,081 |
| Neoverse N2, aarch64 (1c) | 128 | about 428 | about 428 | about 428 |

**The preferred probe is itself above 300 s on three of the four classes.** It must keep both
phases, because the preferred-width invariant tells an epilogue's masked subword refusal from a
loop body's by the lines first printed after the marker (`epilogueOnly`, `knownEpilogueRefusal`).
So no option that only shortens the forced probes can meet the done-when on an EPYC 9V74, an EPYC
7763 or the aarch64 runner.

**What the second phase costs.** No log times the two phases apart. The aarch64 runner ran all five
probes at 128 bits before and after #1672, so its run time per shape and probe gives the ratio: 1.46
to 1.50 times as long with the second phase, which is then about a third of a 128-bit probe (0.32 to
0.33, section 3). That is a derivation over different heads, not a measurement; commit 1 measures
it per probe.

**What the second phase found.** On the laptop, whose census `sql/varka/width_audit.json` is, #1672
added 1,732 verdict lines to the 128-bit column, in 295 of its 886 shapes, and none at 256 or 512
bits; 65 shapes had no refusal at 128 bits before and have one after (section 4). The coverage
table's "128-bit lanes" column is rendered from that column (`VarkaCoverageSuite`), so those 65
rows read "vector" without the second phase at 128 bits.

### What differs from the row text

- **#1774's runner was not a 256-bit one.** The admission check reads run 45988077035's five probes
  as one 256-bit run done four times. Its runner was an EPYC 9V45, whose preferred width is 512
  bits; the repeated probes were the preferred-again and the 64-byte one, and the 256-bit probe was
  a width of its own (section 2). Step 1's skip rule is right either way; only the count differs:
  an AVX-512 runner keeps three probes, an AVX2 runner two.
- **The 128-bit probe is not "about 350 s" on every runner**: 344 to 356 s on an EPYC 9V45, 479 s
  on an EPYC 9V74, 631 s on the one EPYC 7763 reading.
- **"The budget"** is now 1,200 s (`childTimeoutSeconds`, #1774); the done-when's 300 s is half of
  the 600 s the row was written against. The plan keeps 300 s.

## 3. The design

### 3.1 The options, with numbers

Each projection applies the derived ratio of section 2 (a second phase dropped divides a probe's
time by 1.46) and, for a split, divides a probe's time by the number of JVMs plus each JVM's setup
`o`: its start and the build of every audited shape before the first one runs, unmeasured, guessed
at 10 to 40 s **[A]**, measured by commit 1. "Slowest" is the slowest child JVM. A probe's time
already holds one setup, so n JVMs add n - 1 setups per probe: a sum written "907 + 3o" is three
probes split in two.

| option | 9V74 slowest / sum | 9V45 | 7763 | aarch64 | census |
|---|---|---|---|---|---|
| now, after step 1 | 479 / 1,172 | 356 / 868 | 631 / 1,081 | 428 / 428 | |
| A. second phase at the preferred width only (the row's first) | 330 / 907 | 246 / 672 | 450 / 882 | 428 / 428 | loses 1,732 lines at 128 bits; 65 coverage cells turn "vector" falsely |
| B. probes over CI jobs (the row's second) | 479 | 356 | 631 | 428 | unchanged |
| C. each probe's shapes over n JVMs, n = 2 | 240 + o | 178 + o | 316 + o | 214 + o | unchanged if the identity check passes |
| D. fewer calls per phase | unmeasured | | | | at risk |
| E. forced probes run the second phase alone | 330 / 907 | 246 / 672 | 450 / 882 | 428 / 428 | unchanged if the identity check passes |
| **C and E, n = 2** | **165 + o** / 907 + 3o | **123 + o** | **225 + o** | **214 + o** | unchanged if both identity checks pass |

- **A** shortens the 256- and 128-bit probes by their second phase and leaves the preferred one,
  so its slowest JVM stays above 300 s on three classes. It also deletes what the second phase
  found: the 1,732 lines at 128 bits are real fallbacks of long-lane epilogues at two long lanes,
  and the coverage table would claim a vector for 65 rows that fall back. Rejected.
- **B** cannot shorten any probe, so it cannot meet the done-when. It shortens the job: an extra job
  costs about 200 s of setup before its first suite JVM (section 1a), so on an AVX-512 runner three
  jobs would end near 479 + 200 s against 1,172 s now. #1840 already gives the audit a job of its
  own. Rejected for this step; it stays open if the job's wall time, not a probe's, becomes the
  constraint.
- **C** is B moved inside the job: the probe takes a shard `k/n` and runs every n-th shape from the
  k-th, and the suite merges the n censuses of a width. It is the only option that reaches the
  preferred probe. It costs n - 1 more setups per probe and saves nothing in total.
- **D** lowers `calls` (20,000). The calls are what make C2 compile each body under `-Xbatch`, and a
  method left at C1 prints no refusal, so the census would lose lines with no error on any host but
  the laptop. Not planned; commit 1's compile-time split says whether running, rather than
  compiling, is a large share at all, which is the condition for proposing it in a later row.
- **E** runs a forced probe's shapes at 255 rows only. A batch of 255 rows reaches the loop body
  (every lane count up to 64 makes at least three full groups) and the epilogue, so the probe's
  refusals are the two-phase probe's union, which is all the census keeps: `verdicts` reads
  `refusals`, where both phases' lines land, and `epilogueOnly` is read only for the preferred
  probe. It saves what A saves and loses nothing, if a ragged-only JVM prints the same lines, which
  the identity check of 5.2 decides. The epilogue is its own method (`epilogue*` keys in
  `emitted_bytes.json`), so a hot epilogue does not change how the loop body compiles **[A]**.

**Recommendation: C and E together, each behind a switch, defaults set from commit 1's reading.**
E cuts the cost (about 265 s a Build on an AVX-512 runner, about 200 s on an AVX2 one), and C and E
together bring every JVM under 300 s on all four classes when `o` is under about 75 s (C alone
leaves the EPYC 7763's 128-bit probe at 316 s plus `o`). Both are built and both
switches stay, so the readings can pick and the reference form stays runnable (the owner's practice
of building competing forms and keeping the loser as a reference). Nothing leaves the census: the
row's "say in the census which width its epilogue column no longer covers" becomes a sentence that
says no width lost it.

### 3.2 The mechanism

**The probe** (`VarkaWidthAuditProbe.scala`), three additions, each read from a system property the
suite sets on the child's command line:

- `-Dvarka.audit.phases=both|ragged`, absent meaning `both`. Under `ragged`, `runHot` skips
  `phase(rows)` and the `EPILOGUE_PHASE` marker and runs `phase(raggedRows)` alone.
- `-Dvarka.audit.shard=k/n`, absent meaning `0/1`, with `k` 0-based. Two functions in the probe
  object: `private[varka] def parseShard(text: String): (Int, Int)`, which accepts `k/n` with
  `1 <= n <= 8` and `0 <= k < n` and throws an `IllegalArgumentException` quoting the text
  otherwise, and `private[varka] def inShard(index: Int, k: Int, n: Int): Boolean = index % n == k`.
  `main` numbers the entries after the name filter, as now (`zipWithIndex` after the optional
  substring filter, which only a hand run sets), and keeps those with `inShard(i, k, n)`. The class
  name keeps that index (`CLASS_PREFIX + i`), so with no name filter a shape's class name does not
  depend on `n`. `main` prints
  `VARKA_AUDIT_SHARD=k/n` after the host lines so the parent can check it ran what it asked for.
- Times. After building the entries `main` prints `VARKA_AUDIT_SETUP_MS=<ms>`, the JVM's uptime
  then (`ManagementFactory.getRuntimeMXBean.getUptime`), which is the JVM's start plus the shape
  build. `runHot` takes four readings of the wall clock (`System.nanoTime`, as ms) `w0` to `w3` and
  of `ManagementFactory.getCompilationMXBean.getTotalCompilationTime` (ms, cumulative over the
  JVM) `c0` to `c3`: 0 on entry to `runHot`, 1 just before the first phase (after the emit, the
  class load and the input setup), 2 after the first phase (before the marker), 3 after the second
  phase. It prints, inside the shape's markers, one line
  `VARKA_AUDIT_TIMES <w1-w0> <w2-w1> <c2-c1> <w3-w2> <c3-c2> VARKA_AUDIT_TIMES_END`, the five
  numbers being emit, first phase, first phase's compiling, second phase, second phase's
  compiling. Under `ragged` readings 1 and 2 are the same instant, so the second and third numbers
  are 0. When `isCompilationTimeMonitoringSupported` is false the two compile numbers are -1. The end mark is there because a
  compiler thread's record can be glued onto any probe line (`VarkaProbeOutput`); a parser
  `private[varka] def timesIn(line: String): Option[Seq[Long]]` in the probe object reads the five numbers between the marks
  with `VarkaProbeOutput.between` and returns None for anything else.

**The suite** (`VarkaWidthAuditSuite.scala`, line numbers from #1839's head `9ce244f84b7`):

- Two settings beside `parallelProbes` (`:311`), read like it: `forcedPhases`, from
  `-Dvarka.widthAudit.forcedPhases`, `both` or `ragged`, anything else an
  `IllegalArgumentException` naming the key; and `shards`, from `-Dvarka.widthAudit.shards`, an
  integer from 1 to 8. Defaults `both` and 1 until commit 4. A forced probe is one whose
  `maxVectorSizeBytes` is defined, the probes asked for 16, 32 and 64 bytes; the preferred and the
  preferred-again probe are not forced and always run both phases. A skipped forced probe runs
  nothing, so its stand-in's form is the one it gets.
- `Census` (`:99`) gains a last field `times: ProbeTimes`, a new private case class
  `ProbeTimes(setupMs: Long, emitMs: Long, firstMs: Long, firstCompileMs: Long, secondMs: Long,
  secondCompileMs: Long, shapesTimed: Int)`: `setupMs` the sum of each JVM's one setup line, the
  next five the sums of the shapes' times lines, `shapesTimed` the number of those lines. `verdicts`
  and `render` do not read it, so the census file cannot move with it.
- `audit(probe)` (`:121`) becomes `audit(probe, shard: Int)`: it adds
  `-Dvarka.audit.shard=$shard/$shards` always, and `-Dvarka.audit.phases=ragged` when
  `probe.maxVectorSizeBytes.isDefined && forcedPhases == "ragged"`; the child's name gains
  `, shard ${shard + 1} of $shards` when `shards > 1`; the reader collects the setup and times
  lines into `times` and requires the `VARKA_AUDIT_SHARD` line to equal what was asked
  (`run.requireSuccess` first, then an `assert` naming the probe and shard).
- `private def mergeShards(probe: Probe, parts: Seq[Census]): Census`, new: every part's `preferredBits`,
  `useAVX`, `conversionsFallBack`, `featuresFailure` and `subwordMasksFallBack` must be equal, or it
  throws an `IllegalStateException` naming the probe, the field and the values; `refusals`,
  `declined` and `epilogueOnly` are unions whose key sets must be disjoint (a shape in two shards
  throws, naming it); `unattributed` is the concatenation in shard order with repeats dropped;
  `times` the field-by-field sums. The maps' order does not matter: `render` and `verdicts` sort by
  shape name. Every probe that runs is sharded, the preferred-again one included.
- `beforeAll` (`:332`) submits one task per (probe, shard), probe by probe in `probePlan`'s order
  and shard 0 first, to the pool, whose size `parallelProbes` now caps JVMs, not probes. Each task
  reports through `report` (which prefixes `VARKA-AUDIT-PROBE `) one line; seconds are whole,
  truncated as now, and each sum of ms is printed as whole seconds the same way. With `shards = 2`
  (numbers illustrative):

      VARKA-AUDIT-PROBE the audit probe asked for -XX:MaxVectorSize=16 (128 bits), shard 1 of 2 took 164 s on 4 processors; setup 21 s, emit 3 s, first phase 0 s (compiling 0 s), second phase 139 s (compiling 101 s), over 336 shapes

  With `shards = 1` the `, shard 1 of 1` part is left out and the line is otherwise the same, so
  `took N s on` stays the first number after the name, which the done-when's reading greps. Then,
  per probe, the shards' results are taken in shard order: the lowest-numbered failed shard is the
  probe's failure, wrapped in a `RuntimeException` naming the shard; else
  `Success(mergeShards(...))`. Stand-ins are given the merged result as now.
- `render`'s description (`:644`): the words "taken in one JVM per width with the shapes run in
  this order" stay as they are at `shards = 1`, and at n above 1 become "taken in n JVMs per width,
  the first running the 1st, (n+1)-th, (2n+1)-th... shape in this order, the second the 2nd,
  (n+2)-th..., and so on" with n written as a digit. After the description's last sentence one
  more, by `forcedPhases`:
  under `ragged`, "At the host's preferred width each shape runs a batch every lane count divides
  and then one none divides, so its epilogue's refusals are told from its loop body's; at a forced
  width it runs only the second batch, whose refusals are the loop body's and the epilogue's
  together, so no width's column leaves out the epilogue"; under `both`, "Each shape runs a batch
  every lane count divides and then one none divides, so every width's column includes the
  epilogue's refusals." This is the census change the done-when allows, named, and the row's "say
  in the census which width its epilogue column no longer covers": the answer written there is none.
  The check-mode test "sql/varka/width_audit.json is this host's census" passes when only the
  description differs (it compares the widths and logs an `info` line otherwise), so a commit that
  changes the text need not regenerate; commit 4 regenerates once.
- The doc comments that state the old counts are rewritten as the code is (`AGENTS.md`, "A comment
  describes the code as it is"): the class doc's "forks [[VarkaWidthAuditProbe]] once per width",
  `childTimeoutSeconds`'s "at most five probes one after another", `parallelProbes`'s "probePlan
  keeps two to five probes", and the probe's class doc on its two phases.

**CI** (commit 1):

- `.github/workflows/build_and_test.yml`, after the step "Run the Varka suites of ..." (`:1832` on
  master), a step `Print the width audit's probe times`, `if: ${{ !cancelled() && matrix.module ==
  'catalyst' }}`, running, from the checkout's root (the job's working directory, as for the suites step),
  `grep -h 'VARKA-AUDIT-PROBE' target/varka-scoped*/runs/*/*/run.log || echo "the width audit did
  not run in this job"`. It names no shard key, so it does not conflict with #1840's change of
  `matrix.shard` to `matrix.job`. Before writing it, confirm the path: `dev/varka_matrix.sh` writes
  `"$dir/run.log"` (`:446`) with `dir=$RUNS/<config>/<leaf>` (`:426`); the JUnit artifact's paths
  (`runs/defaults/catalyst-20/junit/`) show the layout. If `run.log` is elsewhere, use the path the
  script writes; if the scoped runner writes none, have `report` also append the line to
  `target/varka-width-audit-probes.log` under the checkout and grep that.
- `.github/workflows/varka-width-audit.yml`, the step "Take the census": `./build/sbt
  "catalyst/testOnly *VarkaWidthAuditSuite" 2>&1 | tee "$RUNNER_TEMP/width-audit.log" | tail -60`,
  and a step after it, `if: always()`, printing `grep -hE 'VARKA-AUDIT-PROBE|\*\*\* FAILED'
  "$RUNNER_TEMP/width-audit.log" || true`, so the aarch64 job's probe lines and its failing test's
  name are no longer cut (section 1c). The step's `continue-on-error` stays.

### 3.3 What is deliberately unchanged

- The preferred probe's two phases and the preferred-width invariant (row 550's).
- `calls`, `rows`, `raggedRows`, the JVM flags of a probe, `childTimeoutSeconds` (1,200 s) and the
  CI default `parallelProbes = 1`: a shard JVM beside another on four cores runs slower
  (`results/VARKA-581-probe-times.txt`, the concurrent design's four-core rows), which would move
  the slowest JVM the wrong way.
- Step 1's skip rule and the preferred-again probe's regen-only run (#1839).
- The CI job split (#1840) and the suites' weights (`sql/varka/ci/suite-times.tsv`), which the next
  `--update-weights` refresh picks up from the Builds.
- The aarch64 runner's failing test, which has failed on every aarch64 run of the width-audit
  workflow since #1672 (section 1c); commit 1 makes its name visible and the plan files nothing
  for it (question 3).

### 3.4 Registered op counts

None: no emitted class changes. `emitted_bytes.json` and the shape hashes must not move.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/src/test/scala/.../codegen/varka/VarkaWidthAuditProbe.scala` | the phases and shard properties, the setup and times lines, `timesIn`, the shard parser |
| `sql/catalyst/src/test/scala/.../codegen/varka/VarkaWidthAuditSuite.scala` | the two settings, `ProbeTimes`, `audit(probe, shard)`, `mergeShards`, `beforeAll`'s tasks and lines, the description, the merge test |
| `sql/catalyst/src/test/scala/.../codegen/varka/VarkaProbeOutputSuite.scala` | the shard-parser and times-line tests |
| `.github/workflows/build_and_test.yml` | the step printing the probe lines |
| `.github/workflows/varka-width-audit.yml` | `tee` and the step printing the probe lines and failures |
| `sql/varka/width_audit.json` | regenerated: the description only |
| `sql/varka/plans/m8/results/VARKA-695-step2.txt` | new: the laptop's identity checks and times, the PR's runner readings |
| `sql/varka/plans/m8/VARKA-695.md`, `PLAN.md` row 695 | section 9 and the row's note |

Size: about 250 changed lines of source and workflow, under the 1,000-line cap; the census and the
results files do not count.

## 5. Tests, and what each is for

### 5.1 Unit tests (no probe JVM)

1. `VarkaProbeOutputSuite`, "a shard names every n-th shape from its index, and the shards of a
   count cover the audit once": the shard parser accepts `0/1`, `1/2`, `2/3` and rejects `2/2`,
   `-1/2`, `0/0`, `a/b` and `1` with an `IllegalArgumentException`; for n from 1 to 4 over 886
   indices the n shards' sets are disjoint and their union is every index. Catches an off-by-one
   that drops or doubles a shape, which no census check sees on a host that is not the laptop.
2. `VarkaProbeOutputSuite`, "a probe's times line is read between its marks, and a garbled one is
   not read": a clean line, a line with a `PrintCompilation` record glued before and after (the
   suite's `record`), a line with four numbers, and one with no end mark; the last two read None.
3. `VarkaWidthAuditSuite`, "the shards' censuses of a width merge into one, and a shard of other
   answers fails the merge": parts built with `Census(...)` in its field order (`preferredBits`,
   `useAVX`, `conversionsFallBack`, `featuresFailure`, `refusals`, `declined`, `unattributed`,
   `epilogueOnly`, `subwordMasksFallBack`, `times`) and `Probe("test", None)`; two parts with disjoint shapes merge into the union of each map; parts
   differing in `preferredBits` throw naming the field; parts sharing a shape throw naming it.
   Catches a merge that keeps one shard's map, which a census with that shard's shapes alone would
   still pass on a runner (its check is cancelled off the laptop).

### 5.2 The identity checks, on the laptop (the census host)

Each is a regeneration, `VARKA_AUDIT_REGEN=true build/sbt 'catalyst/testOnly
*VarkaWidthAuditSuite'`, with the settings passed through `JDK_JAVA_OPTIONS` (it reaches the
forked test JVM, `dev/varka_ops/pre-pr.sh` 2), the file copied aside after each run and restored
with `git restore sql/varka/width_audit.json`. Compare only the widths, since the description moves
by design:

    python3 -I -c 'import json,sys; a,b=(json.load(open(f))["widths"] for f in sys.argv[1:3]); print("same" if a==b else "differ")' A.json B.json

1. **E**: `-Dvarka.widthAudit.forcedPhases=ragged` against the committed file: `same`.
2. **C**: `-Dvarka.widthAudit.shards=2` and then `=3`, both against the committed file: `same`.
3. **The AVX2 shape**: under `JDK_JAVA_OPTIONS="-XX:UseAVX=2 ..."` (the laptop's way of showing an
   AVX2 host, `sql/varka/AGENTS.md`), a regeneration at the old settings, kept as the reference;
   then (3a, commit 2) one at `forcedPhases=ragged`, and (3b, commit 3) one at `ragged` with
   `shards=2`, each compared with the reference: `same`. Never commit a `UseAVX=2` file.
4. **The defaults**: after commit 4, a plain regeneration; `git diff sql/varka/width_audit.json`
   shows the description line alone.

A `differ` in 1, 2, 3a or 3b stops that option: record the moved shapes and lines in the results file, keep
its default at the old form, and ask the owner (risk 1 and 2).

### 5.3 The suite itself

`build/sbt 'catalyst/testOnly *VarkaWidthAuditSuite *VarkaProbeOutputSuite'` in check mode on the
laptop at the defaults of each commit, and `*VarkaCoverageSuite`, whose 128-bit column reads the
census, once after commit 4: both green with no file moved but the census's description.

## 6. The measurement

The laptop's runs of 5.2 print every probe's line; copy them with the load average into
`results/VARKA-695-step2.txt`, first line a `# title`, with the head and the date. On the pull
request, the Build's new step and the width-audit workflow (x86_64 and aarch64) print the runner
lines; copy them with the run and job ids and each job's CPU line. The runner class is whatever the
pool gives; an EPYC 7763 reading, if one comes, is the one the default n rests on.

**The rule for the defaults (commit 4)**, from laptop readings so no push is needed before it:
`o` is the largest `setup` of any probe in a check-mode run of commit 1 on the laptop pinned to
four cores (`taskset -c 0-3 build/sbt 'catalyst/testOnly *VarkaWidthAuditSuite'`, the admission
check's way). `forcedPhases = ragged` if checks 1 and 3a read `same`, else `both`. `shards = 2` if
checks 2 and 3b read `same` and `o` is at most 30 s; `shards = 3` if they read `same` and `o` is
above 30 s; `shards = 1` if either reads `differ`. With `o` at 30 s, n = 2 projects 255 s on the
EPYC 7763 reading's preferred probe (450 s), the largest of the four classes. The runner readings
after the push score the predictions and the done-when; they do not set the defaults.

### 6.1 Predictions, registered before the run

Read against the calibration table (`dev/varka_calibration.py`), whose ratios run from 0.03 to 5
with most near 1, so each band below is wide on purpose.

1. Commit 1's first runner reading puts the second phase at 25 to 45% of each probe's time and
   compiling at more than half of each phase (the second phase's share is section 2's derivation;
   compiling is `VARKA-550.md` 10.1's **[A]**).
2. `o`, the setup, is 10 to 40 s on a four-core runner.
3. Under E a forced probe takes 55 to 75% of its two-phase time, and identity checks 1 and 3 read
   `same`.
4. At n = 2 each shard JVM takes 50 to 65% of its unsharded probe, and identity check 2 reads `same`.
5. At the defaults the slowest JVM of the pull request's Build and of its width-audit runs is under
   250 s on an AVX-512 runner and on the aarch64 one, and the audit's suite time in a check-mode
   Build on an AVX-512 runner is 900 to 1,100 s against 1,055 to 1,172 s now.

## 7. Risks

1. **A ragged-only JVM prints different lines** (C2 inlines differently in a method compiled with
   its remainder hot). Identity checks 1 and 3; on a `differ`, E stays off by default.
2. **A shape's lines depend on what ran before it in the JVM** (the shared templates' profiles,
   the mechanism of row 389). The census rests on the opposite ("two censuses of one width agree"),
   so a `differ` in check 2 is a finding about the census, not only about C: record it and ask the
   owner before choosing a default.
3. **`o` is larger than guessed**: every shard JVM builds all 886 shapes (`VarkaWidthAuditShapes.all`
   resolves every coverage row) before filtering. Commit 1 measures it, and the rule of 6 then
   takes n = 3. Above 60 s, building only a shard's own rows would need the shapes API changed,
   which this step does not do: record the reading in section 9 for a later row.
4. **A conflict with #1840** in `build_and_test.yml`: the new step uses `matrix.module` only.
   Whichever merges second rebases; the step's text does not change.
5. **Single readings on a mixed pool**: the EPYC 7763 took 1.7 to 1.9 times as long as the EPYC 9V45
   on the same probes (section 2). The week of the done-when is what covers the pool.

## 8. Sequencing

Start from `origin/master` once #1839 is merged; if it is not, branch from
`varka-695-width-probes` and say so in the pull request. Branch `varka-695-step2`
(`sql/varka/.agents/pull-requests.md`, "The branch"). Each commit compiles
(`build/sbt catalyst/Test/compile`) and passes 5.1's tests and the suite in check mode.

1. **Print each probe's times, and the probe lines in the CI logs.** The probe's setup and times
   lines and `timesIn`; `ProbeTimes` in `Census` and the times in the report line; unit test 5.1.2;
   both workflow changes of 3.2. No behaviour moves; the census does not change. Then the
   four-core laptop run of 6 that reads `o`, into the results file.
2. **A forced probe can run the second phase alone.** The `varka.audit.phases` property, the
   `forcedPhases` setting (default `both`), the description's sentence for each value; identity
   checks 1 and 3a.
3. **A probe can run its shapes in shards.** The `varka.audit.shard` property and its parser,
   `shards` (default 1), `audit(probe, shard)`, `mergeShards`, `beforeAll`'s tasks, the
   description's JVM sentence, the doc comments; unit tests 5.1.1 and 5.1.3; identity checks 2 and
   3b.
4. **The defaults, the census and the record.** The defaults by 6's rule; the regeneration
   (`dev/varka_regen_generated.sh --all`, which must move only the census's description); identity
   check 4; `results/VARKA-695-step2.txt`; section 9 here and row 695's note. Before the pull
   request, `dev/varka_ops/pre-pr.sh` (`dev/varka_ops/AGENTS.md`), then push and read the Build's
   new step and the width-audit runs into the results file.

## 9. Outcome

Not built yet.

## 10. Questions for the owner

*Answered on 28 November 2026, by the owner's word of that day:* question 1, yes, a probe is
one child JVM; question 2, yes, the forced censuses may lose the split; question 3, no row yet,
commit 1 prints the failing test's name and the owner decides then. The plan stands as written.

1. **Is "a probe" in the done-when one child JVM?** The plan reads it so: the 300 s is half a child
   JVM's budget (`childTimeoutSeconds`), and C meets it by making each child shorter. If a probe is
   a whole width's audit, C does not count, and no option here meets the target on an EPYC 9V74,
   an EPYC 7763 or the aarch64 runner, since the preferred probe alone is 330, 450 and about 428 s. The row's 600 s budget is now
   1,200 s (section 2); the plan keeps the row's 300 s.
2. **Should the forced censuses lose their split of epilogue lines from loop-body lines?** Under E
   they do. Nothing reads it today (`epilogueOnly` is read for the preferred probe only), and the
   file never kept it. The plan's answer: yes.
3. **The aarch64 width audit has failed one test on every run since #1672**, hidden because the
   workflow continues past the suite by design and the test's name is above `tail -60`. Commit 1
   prints the name. Should a row be filed once it is read? The plan files none.

## 11. Done when

Step 2 is done when, at the defaults, the slowest child JVM of the width audit on every four-core
runner read (the pull request's Build and width-audit runs, and master's Builds and width-audit
runs for the week after the merge) takes under 300 s; `sql/varka/width_audit.json`'s widths are
byte-identical, its description's change named in section 9; and no probe line in that week shows
more than 600 s. The week's lines are read with

    for repo in vecbricks/varka MaxGekk/spark; do for wf in varka-master.yml build_main.yml varka-width-audit.yml; do gh run list -R $repo --workflow $wf --created ">=<merge date>" --json databaseId --jq '.[].databaseId' | while read id; do gh run view $id -R $repo --log 2>/dev/null | grep -oE 'VARKA-AUDIT-PROBE .* took [0-9]+ s'; done; done; done

with `<merge date>` the merge's date as `YYYY-MM-DD` (a workflow a repository does not have prints
an error and is skipped; the fork's Build is `build_main.yml` and master's `varka-master.yml`, both
read from the runs of 28 November 2026), and the largest number is
written into section 9 with the count of lines read.
