# VARKA-255: CI and benchmark runs with no laptop in the path

## 1. Where this came from

Milestone 7 row 255 (`m7/PLAN.md` 2.4), item 76 redesigned. The fork's CI is kept to one Build
at a time by `dev/varka_ci_queue.sh` (VARKA-176), a process on the owner's laptop that cancels
the run a push starts and reruns it in turn; `VARKA-227.md` 3 proposed moving that dispatcher to
a workflow on `vecbricks/varka` with a personal access token on the fork, and the owner declined
any stored token on 2 October 2026. Section 2.4 then laid out the token-free split: the queue on
`MaxGekk/spark` on a five-minute cron with its own `GITHUB_TOKEN`, the check sync on the base
repository on a cron with its own; master's head run after each merge; a pull request's new tests
rerun several times, and a fix's new test required to fail on the base, as ClickHouse's CI does
(`m7/READING.md` 9).

The halfway review of 13 October (`m7/PLAN.md` 9.2, second and third findings; 9.5) added two
things the owner agreed to put in this row: **(a)** the benchmark pairs
(`dev/varka_bench_pair.sh`, VARKA-256) and results-file regenerations move to GitHub-hosted
runners through `.github/workflows/benchmark.yml`, the datapath probe (`dev/varka_datapath.sh`)
checked first, so that 322a's ten Java-against-Scala pairs, row 325's two results files (#789)
and 322a-2's pair (#791) stop waiting for an idle laptop; and **(b)** each gate and nightly step's
time is recorded from CI into a committed budget, and a pull request that adds more than 5% to a
step says so (`sql/varka/AGENTS.md`, "Pull requests").

The admission check below overturns two premises of 2.4 - that a five-minute cron can drive a
queue, and that one Build at a time is the right rule - so section 3 replaces 2.4's dispatcher
with a different mechanism and keeps 2.4's constraints: no stored credential, each repository
acting only on itself with its own `GITHUB_TOKEN`, and nothing on the laptop. That change is the
owner's to accept (section 8, owner action 1).

## 2. The admission check, done

Read-only, on 13 October 2026 between 20:30 and 21:55 CEST, from the laptop with the owner's
`gh` authentication for reads and `curl` without any for the credential-free probes. Times below
are CEST; the API reports UTC, two hours earlier. Nothing was dispatched, rerun, cancelled,
enabled or labelled. The probe scripts lived in the scratch area and are not committed.

### 2.1 GitHub's cron does not fire on time here, by hours

2.4 rests on a five-minute cron. GitHub's documentation says the shortest interval is five
minutes, that "the `schedule` event can be delayed during periods of high loads" and that "some
queued jobs may be dropped" (Events that trigger workflows, `schedule`). What it does in these
two repositories:

| workflow (repository) | cron | when it actually started | late by |
| --- | --- | --- | --- |
| `update_build_status.yml` (base) | every 15 min | 6 runs in 48 h of 12 and 13 October: 04:02, 11:10, 21:02, 03:20, 10:05, 17:57 | gaps of 377 to 592 min |
| `varka-fuzz.yml` (base) | 04:00 daily | 09:56 to 10:45, every day 6 to 13 October | about 6 to 7 h |
| `varka-weekly-matrix.yml` (base) | 03:00 Sunday | 08:01, 08:32, 08:39 on 27 September, 4 and 11 October | about 5 to 6 h |
| `stale.yml` (fork) | 02:00 daily | 05:50 to 07:31, 2 to 13 October | about 4 to 6 h |
| `build_java25.yml` (fork) | 06:00 every second day | 11:09 to 13:30, 21 September to 13 October | about 5 to 8 h |

(`gh api repos/<repo>/actions/workflows/<file>/runs?event=schedule`.) A fifteen-minute cron
acting three times a day is not a queue tick, and a five-minute one would not do better: the
delay is GitHub's, not the interval's. **No part of the design may wait on a cron**; crons stay
only as a backstop. This also explains why pull requests' `Build` checks have read stale since
the laptop queue stopped dispatching the sync (2.3): the cron is the only other thing that
updates them.

**Nothing needs enabling on the fork.** 2.4 expected a step "enable the scheduled workflows once
in the Actions tab". The fork already runs them: `actions/runs?event=schedule` on
`MaxGekk/spark` reports 2,500 scheduled runs, five of them on 13 October (90 of the latest 100
skipped, since they are apache/spark's branch schedulers). What does bind is the default-branch
rule: `schedule`, `workflow_dispatch` and `workflow_run` all trigger "only if the workflow file
exists on the default branch" (same page), and the fork's `master` is apache/spark's of 21 August
(`ee11a92a9f1`); `varka-canary.yml` is not on it (404), while `benchmark.yml` is, which is why
`benchmark.yml` can be dispatched on the fork at a PR branch's ref and a new workflow file could
not. Section 3 puts no file on the fork's `master`, which settles open question 7.3 of
`m7/PLAN.md` by not needing it.

### 2.2 The fork's runner slots on 13 October: full only while a full-matrix run held them

`build_main.yml` runs on the fork for every push; 48 Build runs were created on 13 October, and
the 36 that ran to success or failure were read job by job (`actions/runs/<id>/jobs`).

* **Most runs are small.** A Varka-scoped change runs 6 to 14 jobs (precondition, linters, docs
  checks, the Varka suites of catalyst and sql, the engine on two architectures, the assembly
  gate); two runs, `varka-322` from 14:25 and `varka-312b`'s rerun from 18:56, ran Spark's full
  module matrix, 35 jobs each, 136 and 73 minutes, their longest job (`Build modules: sql - slow
  tests`) 118 and 128 minutes.
* **The slots were saturated for 48 minutes of the day.** Minutes at each count of running jobs,
  from every job's start and end: 0 jobs for 257 minutes, 1 to 5 jobs for 432, 6 to 10 for 102,
  11 to 19 for 33, and 20 (the account's limit, GitHub Actions limits: "Free: 20 jobs running at
  once") for 48, all of them between 14:44 and 15:38, inside `varka-322`'s run.
* **Job waits came from that one run.** Of 365 jobs, the median waited 2 s from creation to
  start and the 90th percentile 4.5 min; the long waits, up to 49 minutes (`varka-bg-processes`'
  `Precompile Spark`), were scoped and small runs queued behind `varka-322`'s 35 jobs. This is
  the "queue sat for up to fifty minutes" of `m7/PLAN.md` 9.2.
* **One Build at a time would have cost the day six hours.** The 36 runs took 1,132 run-minutes
  between 06:54 and 21:27. Run strictly one after another, in the order they started, the last
  would have finished at 03:41 the next morning.
* **`ubuntu-slim` counts against the twenty.** During the saturated window the `Run Spark UI
  tests` jobs, which run on `ubuntu-slim`, waited 19 and 27 minutes. So a job that only waits
  still holds a slot.

So the rule 227 enforced, one Build at a time, is wrong for today's mix: the harm is a
full-matrix run taking every slot, and two of them interleaving. **Scoped runs must never wait;
a full-matrix run must not start while another full-matrix run is in flight; and a full-matrix
run should leave slots for the small runs beside it.**

### 2.3 The laptop queue has not run a rerun since 3 October

`dev/varka_ci_queue.sh run` dispatches `update_build_status.yml` after every rerun it completes
(`VARKA-227.md` 2). The base repository's dispatch history: 69 dispatches from 29 September to
3 October, the last at 22:04 on 3 October, none since. The queue file in the clone's git
directory is empty and the runner's lock was last taken at 23:43 on 3 October. The one rerun on
13 October, `varka-312b`'s, was started under the owner's account at 18:56, two and a half hours
after its run was created at 16:25 and cancelled, and no sync dispatch followed it, so it was not
the queue's `run`. So for ten days the fork's CI has run without a queue, the checks have been
synced only by the late cron (2.1), and 2.2's numbers are what no queue looks like.

### 2.4 Each repository can read the other without a credential, and write only itself

* **Reads.** The fork's runs are public: `curl https://api.github.com/repos/MaxGekk/spark/
  actions/runs/38413226994` with no token returned the run (`completed`, `success`, attempt 2),
  with `x-ratelimit-limit: 60`. Sixty an hour per IP is not enough on a hosted runner, whose
  egress address is shared, so every read in the design goes through the reading repository's own
  `GITHUB_TOKEN`, which GitHub limits to "1,000 requests per hour per repository" (Rate limits for
  the REST API). That a job's `GITHUB_TOKEN` reads another public repository is not an
  assumption: `update_build_status.yml` already reads the fork's runs with the base's token
  (`github.request('GET /repos/{owner}/{repo}/actions/runs/{run_id}', params)` with the fork's
  owner and name) and synced eight checks that way on 1 October (`VARKA-227.md` 5).
* **Writes.** The same unauthenticated client asking to rerun that run got `401`. The fork's
  default workflow permission is `write` (`actions/permissions/workflow`), but `build_main.yml`
  grants its job only `packages: write` (lines 29-30), and an explicit `permissions` block sets
  every scope it does not name to none, so a step inside the Build has no `actions: read` today.
  The base's default is `read`; `update_build_status.yml` and `notify_test_workflow.yml` ask for
  `checks: write` explicitly.
* **`notify_test_workflow.yml` runs on `pull_request_target`** (line 27), on `opened`,
  `reopened` and `synchronize`: every push to a pull request starts a job on the base with the
  base's write token, at the moment the fork's Build starts. That is the event the check sync can
  hang on instead of a cron. It must never check out the pull request's code, and does not.
* **Events from a `GITHUB_TOKEN` start no workflows**, "with the exception of
  `workflow_dispatch` and `repository_dispatch`" (Events that trigger workflows). 2.4's queue
  would rerun with the fork's token and then wait to hear the rerun end; whether a token-made
  rerun's completion fires `workflow_run` is not documented, and section 3 does not depend on it.

### 2.5 Master: nothing runs after a merge, and the weekly run does not run what it says

`build_main.yml` skips pushes to `master` on any repository but apache/spark (lines 34-36), and
`vecbricks/varka` is itself a fork of apache/spark (`repos/vecbricks/varka`: `fork: true`), so
641 of its 642 Build runs are skipped pushes to `master` (the other, a branch pushed there once,
was cancelled). 23 pull requests merged on 13 October and 8 to 29 a day over the week before;
none got a run of the merged head. The weekly matrix
(`varka-weekly-matrix.yml`, on the base) did run on 11 October, but ran 6 of its 20 jobs, skipped
the Varka suites, the engine, the assembly gate and every `Build modules` job, and failed the Java
linter (`InvalidJavadocPosition`), as it did on 27 September and 4 October. It passes no `jobs`
input, so its precondition decides from a diff (`build_and_test.yml` lines 131-134), and on
`master` the pull-request diff it computes is, by every sign, empty; the job log has expired, so
the first PR of section 8 confirms the cause before it relies on it. `build_java25.yml` shows the
form that runs what it names: an explicit `jobs` JSON, which the precondition takes verbatim
(lines 109, 332).

The base repository has runner slots of its own: `vecbricks` is an organization on the free plan
(`orgs/vecbricks`), and the twenty-job limit is per account, so work placed on the base does not
compete with pull requests' Builds on `MaxGekk/spark`. On 13 October the base ran the two fuzz
jobs and three sync passes.

### 2.6 Benchmarks on GitHub-hosted runners: what the pool gives and how long a class takes

`benchmark.yml` has run on the fork 133 times. The 40 latest successful runs, 25 September to 12
October, one job each (`actions/workflows/benchmark.yml/runs`, then each job's `Runner CPU`
annotation and its `Run benchmarks` step, which includes the `Test/package` build):

| runner CPU | draws | `dev/varka_datapath.sh` 512:256 (`sql/varka/HARDWARE.md`) |
| --- | ---: | --- |
| AMD EPYC 7763 (Zen 3) | 18 | 1.00, no AVX-512 |
| AMD EPYC 9V45 (Zen 5) | 9 | 1.99, full width |
| AMD EPYC 9V74 (Zen 4) | 8 | 0.91 to 1.00 |
| Intel Xeon Platinum 8370C | 3 | not in the table |
| Intel Xeon Platinum 8573C | 2 | 1.34 to 1.36 |

The 9V45 came up 9 times in 40, against "one draw in eighteen" in HARDWARE.md's survey of 11
September; the pool moves, which is why the probe runs on the measuring job itself
(`dev/varka_datapath.sh`, header: "a gate in one GitHub job says nothing about the machine
another job gets").

| class | step minutes per run, on runners |
| --- | --- |
| `CachedTableWidthBenchmark` | 8, 13 |
| `CodegenFallbackCostBenchmark` | 11, 14, 17 |
| `VarkaRangeFilterBenchmark` | 14 |
| `CaseWhenCodegenBenchmark` | 13 to 31 (16 runs) |
| `VarkaSizeLadderBenchmark` | 19 to 20 (5 runs) |
| `VarkaColdStartBenchmark` | 41, 42 |
| `VarkaWideKernelBenchmark` | 11, 88, 103, 189 |

Four facts follow for a pair on a runner. **Both sides in one job**: the pair's alternation is
the control only on one machine. **The six-hour job cap** ("Each job in a workflow can run for up
to 6 hours", Actions limits): a pair is two builds plus at least three rounds of two class runs
(`dev/varka_bench_pair.sh`, `--rounds`, early stop after three), so a twenty-minute class fits at
five rounds and `VarkaWideKernelBenchmark` does not fit at all. **The canary and the load check
refuse a runner**: `dev/varka_bench_regen.sh` stops when `dev/varka_canary/baseline-<host>.txt`
is missing (only `baseline-aqua.txt` exists) or the load is above 1.0, so a runner pair runs with
`--force`, which the pair script passes through. **The merge base comes from `origin`**:
`dev/varka_bench_pair.sh` fetches `origin master`, which on a fork checkout is `MaxGekk/spark`'s
master of August; on a base checkout it is `vecbricks/varka`'s, the right one.

### 2.7 Step times available today, for the first budget

* **CI, the Varka jobs of 13 October** (21 runs of each suite job; minutes, min / median / max):
  `Varka suites: catalyst` 12 / 23 / 43, of which the suites step 8 / 14 / 22 and the one-option
  step 3 / 6 / 23; `Varka suites: sql` 18 / 26 / 54, suites 12 / 16 / 19, option 5 / 7 / 34;
  `Varka assembly gate` 5 / 7 / 8; `Varka bench drivers` 2 / 2 / 3; the engine jobs 1 to 2; docs
  checks under 1.
* **CI, the nightly** (`varka-fuzz.yml`, 12 October, seconds): compile 222, IR fuzzer 404,
  composition fuzzer 43, variants to size 4 514; second job compile 299, every tree to size 5
  2,807, size 4 at 128 bits 124. (The 13 October run failed in its IR fuzzer step; that failure
  is not this row's.)
* **The laptop gate** writes `<step>.secs` under `target/varka-gate/` (`dev/varka_gate.sh:127`);
  sixteen worktrees still hold theirs. The `wide` step: 488 to 498 s on 5 and 6 October, 650 to
  848 s on 12 October (full gates in `varka-278` to `varka-311`); `doc` 207 to 304 s, `lint` 163
  to 280 s, `sweep` 185 to 387 s.
* **The laptop nightly** prints its step times only in its summary (`dev/varka_nightly.sh:151`)
  and keeps no file of them; no nightly log newer than 26 September survives. Its golden-file step
  is recorded at 21 to 33 minutes per arm in `m7/PLAN.md` 9.2.

The spread is the fact the budget has to live with: one day's catalyst suites step ranges 2.8
times from fastest to slowest across runners, and the laptop gate's `wide` step moved 15% between
two gates of the same day. **A 5% rule read against one run is noise.** The budget records
medians and spreads; a pull request's disclosure is read against the median, and an automatic
flag can only name a step beyond the spread.

### 2.8 What the check rejected

1. **A cron-driven queue** (2.4): the cron runs hours late (2.1).
2. **One Build at a time** (227): six hours on 13 October's load (2.2).
3. **A dispatcher that cancels and reruns.** It needs a waiter that hears the rerun end, which
   with a `GITHUB_TOKEN` rerun is either an undocumented `workflow_run` or a job holding a slot;
   and a cancelled attempt is what made the PR's check misleading in 227's first friction.
4. **A workflow file on the fork's `master`** for the same reason as 7.3: it must be carried
   across every sync of that branch with apache/spark, and nothing in section 3 needs it.
5. **GitHub's own `concurrency` queue.** Since `queue: max` a group can hold up to 100 pending
   runs in order (Workflow syntax, `concurrency`), but a group is decided before any job runs,
   from contexts such as the ref, so it cannot tell a full-matrix run from a scoped one; it would
   serialize every run, which is rejection 2.
6. **Pairs on the fork.** A pair holds one fork slot for hours, and its merge base would be
   August's (2.6).

## 3. The design

### 3.1 The queue becomes an admission step inside the Build

The Build itself decides whether it may fan out, so nothing outside it has to cancel, rerun or
wait for it. One step is appended to the `precondition` job of `build_and_test.yml`, after
`Check all modules` has written `required`, and runs only on a repository other than apache/spark
and vecbricks/varka, on a `push`:

* **A scoped or documents-only run passes at once.** The step reads `required`; if `build` is not
  `"true"` the run is not a full-matrix run (`build_and_test.yml` 230-245 set it false for both
  scopes) and the step ends in under a second.
* **A full-matrix run waits for older full-matrix runs.** The step lists the fork's Build runs
  that are `queued` or `in_progress` and were created before its own, and reads each one's jobs.
  An older run blocks while its `Check changes` job is still running, since that job either is
  still choosing (about a minute) or is waiting at this same step, which only a full-matrix run
  does; and while its `Precompile Spark` job exists and is not `skipped`, which marks a run that
  passed the step as full-matrix; a run with no job started yet blocks too. The step polls every
  120 seconds until no older run blocks. Nothing depends on reading another run's annotations or
  outputs. Order is first come, first served by run creation, the order
  GitHub's own queue keeps; 227's pull-request-number order assumed merges in number order, which
  23 merges in a day out of order do not keep.
* **`ci-hold` takes a pull request out.** The step finds its pull request on the base
  (`pulls?head=MaxGekk:<branch>`) and, when it carries the label, fails with "held by ci-hold".
  The run stops at its first job; removing the label and rerunning the run resumes it.
* **A re-push needs nothing.** `build_and_test.yml`'s concurrency group per ref with
  `cancel-in-progress: true` (lines 75-77) cancels the waiting run when the branch moves, as it
  does today.
* **The wait has a deadline**: 330 minutes, inside the 360-minute job cap, after which the step
  fails with the run it waited on named.
* **A full-matrix run leaves room.** The `Build modules` matrix's `max-parallel` (line 412) is
  20, so one full run can take every slot (2.2). On a fork it becomes 12, keeping eight slots for
  the small runs beside it; on apache/spark it stays 20. `strategy` may read the `github`
  context (Contexts, availability table), so this is one expression on the repository name.

What this costs and gains, against the laptop queue: a waiting full-matrix run holds one
`ubuntu-slim` slot while it waits (2.2), and a run waits only behind another full-matrix run,
which happened twice on 13 October; nothing runs on the laptop; the run is `in_progress` while it
waits, so the pull request's check says "in progress" rather than "cancelled", which removes 227's
first friction; and 211's docs-only verdict keeping is unnecessary, since a documents-only push
already runs only six short jobs (13 October's documents-only runs, 2.2; VARKA-160). The step's
token needs `actions: read`, added beside `packages: write` in `build_main.yml`'s job, the only
change to that file. The logic is a stdlib Python script, `dev/varka_ci_admit.py`, so it can be
run and tested off CI.

The step polls with the fork's token, which every job of the fork shares at 1,000 requests an
hour: one run listing and one job listing per older run in flight per poll, under 200 requests
an hour per waiting run with five runs in flight.

### 3.2 The check sync: on the base, hung on the pull request's push

`update_build_status.yml` keeps its pass as it is and gains a loop and a trigger:

* **Trigger**: `pull_request_target` on `opened`, `reopened` and `synchronize`, the event
  `notify_test_workflow.yml` already takes (2.4), beside the cron (kept as a backstop) and
  `workflow_dispatch`.
* **Loop**: the pass repeats every 120 seconds while any open pull request's `Build` check is not
  completed, and ends when all are, or at 345 minutes. A pass patches a check only when its
  status or conclusion differs from the fork run's, and skips pull requests whose check is already
  completed at the current head; today's pass patches every completed check on every pass.
* **One loop at a time**: `concurrency: {group: update-build-status, cancel-in-progress: false}`.
  A push during a loop leaves one run pending, which starts when the loop ends and finds nothing
  or the new head; a loop already running reads every open pull request on each pass, so a new
  push is synced by it within one interval.

Requests: one pull-request listing per pass plus about three per pull request with a run in
flight; with four in flight, about 400 an hour against the base's 1,000. The sync no longer
depends on anything dispatching it, so `dev/varka_ci_queue.sh`'s dispatch goes with the script
(3.7).

### 3.3 Master's head after each merge, on the base

A workflow on `vecbricks/varka`, `varka-master.yml`, on `push` to `master` and `workflow_dispatch`,
calls `build_and_test.yml` with an explicit `jobs` JSON: the Varka set (`varka-scoped`,
`varka-engine`, `varka-bench`, `varka-asm`, `varka-docs`), the linters and the Java 25 Maven build
(`java25`), which is the only place the shading and incubator-module failures of
`sql/varka/AGENTS.md`'s house rules appear. About ten jobs, about thirty minutes (2.7), on the
base's own slots (2.5). Its concurrency group keeps one run in flight and replaces a pending run
with the newest (GitHub's default for a group, Workflow syntax, `concurrency`), so a burst of merges
builds the last head of the burst; on 13 October's 23 merges that is a run every half hour while
merges come. A failure is visible on the commit and the workflow's page; the CI-failure
investigation in `CLAUDE.md` applies unchanged.

`varka-weekly-matrix.yml` passes an explicit `jobs` JSON too, Spark's full module set as
`build_java25.yml` names it, so that it runs the matrix its comment promises; and the
`InvalidJavadocPosition` finding that has failed it three Sundays is fixed in the same pull
request, since the master run's linter would fail on it as well.

### 3.4 New tests rerun several times, and a fix's new test failing on the base

Both in the Varka suite jobs of a pull request's Build, after the suites:

* **Reruns.** `dev/varka_new_tests.py` lists the tests the pull request adds, from the diff
  against its merge base: a new `test("...")` in a ScalaTest suite, a new `@DisplayName("...")` in
  a JUnit class under `@VarkaSuite`. The step reruns each through sbt's `-z` filter (JUnit by its
  display name) five times in the suite's JVM, and fails on any failure, naming the run. ClickHouse
  runs fifty (`m7/READING.md` 9); five is what fits the Varka job's time, and the count is an
  input. A new test that is already a fuzz or sweep under a seed is rerun with five other seeds,
  which is what a rerun is for there.
* **A fix's test fails on the base.** For a pull request labelled `fix`, the step builds the merge
  base's main sources with the pull request's test sources over them (`git checkout <base> --`
  every path outside `src/test`) and runs the new tests; it passes when each fails or does not
  compile, and fails, naming the test, when one passes on the base. A fix whose test passes before
  the fix has not shown the bug.

These two are the most likely to outgrow the row: the test listing has edge cases (a renamed test,
a test moved between files, a parameterized title), and the base build adds a compile. If either
needs more than section 8's last pull request, it becomes a row of its own, and section 9 says
so.

### 3.5 Benchmarks on GitHub-hosted runners (the review's point a)

`benchmark.yml` gains a pair mode and runs on the base repository by default.

* **Inputs**: `pr` (a pull request number; the job checks out `refs/pull/<pr>/head`, which the
  base keeps for every pull request, and blank for the dispatched ref as today), `pair` (true runs
  `dev/varka_bench_pair.sh` instead of the harness), `rounds` (default 3), and the existing
  `class`, `sections`, `expected-cpu` and `extra-java-options`. A list of classes in `class` gives
  one matrix job per class, `max-parallel` 6.
* **One job per pair**: the job runs `dev/varka_datapath.sh --require any --github` first,
  writing the CPU, the control and the 512:256 ratio to the job summary, then
  `dev/varka_bench_pair.sh <module> <class> --base <merge base> --rounds <n> --force`, both sides
  on that machine. The merge base is computed against the base's `master`, which a base checkout
  has as `origin` (2.6). The pair's verdict, its per-row table and both sides' files are the job's
  artifact and its summary.
* **Results files** keep today's path, `benchmark.yml` without `pair`, with `pr` added: the
  regenerated files come back as the artifact and the author commits them; `create-commit` cannot
  push from the base to a fork branch without a credential, so it stays for the fork's own
  dispatches. The file's provenance names the runner's CPU and the probe's ratio, as Spark's
  files name theirs (`varka-benchmarks-on-github-runners` in the project's record); a file that
  needs a full-width machine sets `expected-cpu` and the job stops at the probe on any other.
* **Why the base**: its slots are its own (2.5), so ten pairs of hours each do not take ten of the
  twenty slots the pull requests' Builds share; and its `origin` is the right merge base. The fork
  keeps `benchmark.yml` as it is, dispatchable at a branch's ref.
* **What does not fit**: a class whose single run exceeds about 50 minutes on a runner cannot pair
  at three rounds inside six hours. `VarkaWideKernelBenchmark` (88 to 189 minutes, 2.6) is the
  one known; its pair runs on a case subset per job through `VarkaBenchmarkBase`'s case filter
  (VARKA-322) or stays a laptop pair, and section 9 records which.

The three waiting jobs are the first users: 322a's ten pairs, 325's two results files and 322a-2's
pair, each dispatched with `pr` set.

### 3.6 The step-time budget (the review's point b)

* **The file**: `sql/varka/ci/step-budget.tsv`, one line per step: where it runs (`ci` or
  `laptop`), the job and step names, the runner CPU (for `ci`), the number of runs read, the
  median and the 90th percentile in seconds, and the date range. The first version is 2.7's times,
  as the review asked ("the first budget is the times of 13 October").
* **The tool**: `dev/varka_ci_times.py`. `collect` reads a range of runs' jobs and steps from the
  API - master's runs of 3.3 and `varka-fuzz.yml`'s by default, which are the only runs of a fixed
  tree - and writes the file; `compare <run>` prints a pull request run's steps against it, by
  the same runner CPU where the budget has one, marking a step above the budget's 90th percentile
  by more than 5%; `gate <dir>` does the same for a laptop gate's `.secs` files.
* **Where it shows**: the check sync (3.2) writes `compare`'s table into the `Build` check's
  summary when the run completes, so every pull request shows its step times against the budget
  without anyone running anything. The mark is a disclosure prompt, not a failure, because of
  2.7's spread.
* **The nightly writes its step times**: `dev/varka_nightly.sh` writes `<step>.secs` beside each
  log, as the gate does, so `gate <dir>` reads it too.
* **Kept current**: the budget is regenerated from master's runs by the closing PR of each
  milestone and whenever a PR's description says it moved a step; the file's history is the
  record of what the tests cost.

### 3.7 The laptop queue retires

`dev/varka_ci_queue.sh` is deleted in the pull request after the admission step has been proven
(section 8), with `sql/varka/skills/working-in-this-repo.md`'s two queue sections rewritten in
the past tense, and its `update_build_status.yml` dispatch comment removed.

### 3.8 Deliberately unchanged

* **The precondition's scope rules** (`dev/varka_scope.py`, VARKA-160): the admission step reads
  their verdict and does not change it.
* **The checkout-and-sync merge with apache/spark** before every build
  (`working-in-this-repo.md`, "A red Build here may be failing on code this repository does not
  contain").
* **`notify_test_workflow.yml`**: it still creates the check; 3.2 only keeps it current.
* **The fork's `master`** and its apache/spark schedulers.
* **`varka-surface-benchmark.yml`**, the published-number path with its own datapath gate.
* **Row 287's lane split of the Varka suite jobs**, measured on the runners in its own row.

### 3.9 Whether (a) and (b) belong here

Both do, for reasons of mechanism rather than convenience. (a) moves benchmark jobs to the base
repository's slots for the same reason 3.3 moves master's run there, and it is the same split of
work between the two accounts that 3.1 relies on to keep the fork's slots for pull requests. (b)
has one sound source of step times - master's per-merge runs of 3.3, the only CI runs of a fixed
tree after the fuzz nightly - and one place to show them on every pull request, the check sync of
3.2. Either built elsewhere would rebuild those parts. Each is its own pull request (section 8),
so either could still be cut to a row of its own without touching the others; 3.4 is the part
most likely to be.

### 3.10 Registered op counts

None: no emitted code changes.

## 4. Files

| file | what |
| --- | --- |
| `.github/workflows/build_and_test.yml` | the admission step in `precondition`; `max-parallel` 12 on forks for `Build modules` |
| `.github/workflows/build_main.yml` | `actions: read` beside `packages: write` |
| `dev/varka_ci_admit.py` | the admission decision: scoped or full, older full runs in flight, `ci-hold` |
| `.github/workflows/update_build_status.yml` | `pull_request_target`, the loop, one loop at a time, patch only on change, the step-time summary |
| `.github/workflows/varka-master.yml` | new: master's head after each merge, explicit `jobs` |
| `.github/workflows/varka-weekly-matrix.yml` | explicit `jobs` |
| the file the Java linter names | the `InvalidJavadocPosition` fix |
| `dev/varka_new_tests.py` | the new tests of a diff; the base-overlay check for `fix` |
| `.github/workflows/build_and_test.yml` (Varka suite jobs) | the rerun and fails-on-base steps |
| `.github/workflows/benchmark.yml` | `pr`, `pair`, `rounds`, the probe first, a class list as a matrix |
| `sql/varka/ci/step-budget.tsv` | new: the budget |
| `dev/varka_ci_times.py` | new: `collect`, `compare`, `gate` |
| `dev/varka_nightly.sh` | `<step>.secs` per step |
| `dev/varka_ci_queue.sh` | deleted after 3.1 is proven |
| `sql/varka/skills/working-in-this-repo.md`, `benchmarking.md`, `SKILLS.md` | the queue's sections in the past tense; the runner pair; the budget |
| `sql/varka/AGENTS.md` | "Pull requests": the budget's file and tool named where the rule is |

## 5. Tests, and what each is for

1. **`dev/varka_ci_admit.py` over recorded API responses** (unit tests in Python, run by the
   docs job): a scoped run passes; a full run with an older full run in flight waits and with only
   younger ones passes; a full run behind an older run still in its `Check changes` job waits; a
   held pull request fails; a finished first job with neither name fails loudly; a run listing
   with a completed older run passes. Without these, the only test of the step is a live day of
   CI.
2. **The step on the fork, live**: a scoped push passes the step in under a second; two
   full-matrix pushes (a workflow-file change each) a minute apart: the second's log says it
   waited on the first's run id and its fan-out starts within one poll of the first's end.
3. **`max-parallel` on the fork**: the full run's job list never shows more than 12 `Build
   modules` jobs running at once, read from the jobs API.
4. **The check sync**: a push to a test pull request; its `Build` check goes to `in_progress` and
   to the run's conclusion within one poll interval of the fork run's, read from the check-runs
   API against the run's `updated_at`. And a pass on unchanged checks issues no `PATCH` (the
   job's log counts them).
5. **The master run**: the merge of section 8's second PR runs `varka-master.yml` with the jobs
   it names and no `Build modules` job; the weekly run on the following Sunday runs the module
   jobs.
6. **`dev/varka_new_tests.py` over recorded diffs**: a new ScalaTest test, a new JUnit display
   name, a renamed test (counted as new), a moved test (not new), a test whose title is an
   interpolated string (reported as unlistable, not silently dropped).
7. **The fails-on-base step, planted**: a test PR adds a test that passes on master and labels it
   `fix`; the step fails naming it. The same PR with a real one-line bug fix and its test passes.
8. **The runner pair, A/A**: `benchmark.yml` with `pair` and `--base` equal to the head on one
   short class (`VarkaRangeFilterBenchmark`): the verdict passes and its widest row change is the
   runner's own spread, recorded beside the laptop's A/A.
9. **`dev/varka_ci_times.py` over recorded runs**: `collect` reproduces 2.7's medians from the
   same runs; `compare` marks a planted 20% slower step and not a 3% one inside the spread.

No pinned fixture moves.

## 6. The measurement

After each pull request lands: the fork's job waits and run times on the next working day with at
least one full-matrix run, read the way 2.2 read 13 October; the lag from a fork run's end to its
check's update; master runs per day and their times; the three waiting benchmark jobs' times and
CPUs; the budget's spread per step after two weeks of master runs.

### 6.1 Predictions, registered before the run

Cost predictions are stated at the calibration table's observed ratio
(`skills/testing-and-debugging.md`): the table's cost lines ran 1.1 to 5 times over, the harness
overhead being what plans missed, so the estimates below carry a factor of 1.5 over the
arithmetic unless they come from a measured run.

1. On a day with a full-matrix run, no scoped run's job waits more than 10 minutes, against 49 on
   13 October; the 90th percentile job wait stays under 5 minutes (4.5 measured with no queue).
2. A full-matrix run capped at 12 parallel `Build modules` jobs takes at most 20% longer than an
   uncapped one on a quiet fork, since its critical path is the 118 to 128 minute `sql - slow
   tests` job (2.2's two runs).
3. A pull request's `Build` check reaches the fork run's conclusion within 4 minutes of it (one
   120 s interval and the pass, at 1.5 times), against hours today.
4. The sync loop stays under 600 requests an hour on the base on a day like 13 October (400 by
   arithmetic, at 1.5 times).
5. `varka-master.yml` runs in 30 to 45 minutes and fewer than 15 times on a day of 23 merges.
6. The new-test reruns add under 3 minutes to a Varka suite job on a typical pull request, and the
   fails-on-base step under 8 minutes when it runs (a compile of the base, measured at 4 to 5
   minutes in `varka-fuzz.yml`'s compile step, plus the tests).
7. A pair of a twenty-minute class at three rounds finishes in under 4 hours on a runner; the
   three waiting jobs, apart from `VarkaWideKernelBenchmark`'s pair, complete with no laptop
   time.
8. The runner A/A pair's widest row change is under 10%, the pair's threshold, on at least two of
   the pool's CPU models; if not, the runner pair's threshold is raised for that class with the
   A/A as the reason.
9. With 2.7's spread, `compare` marks between one and three steps per ten pull requests that do
   not change test code, all of them on a runner CPU the budget has few runs of.

## 7. Risks

1. **The admission step holds a slot.** A full run waiting behind another holds an `ubuntu-slim`
   slot for up to two hours; two waiting hold two. Test 2 and prediction 1 measure it; if waits
   pile up, the step can cancel its own run instead of waiting, which is 227's cancellation
   brought back for full runs only.
2. **The job names as the full-run marker.** The step recognizes a full-matrix run by its
   `Check changes` and `Precompile Spark` jobs (3.1); a rename upstream would blind it. Test 1
   runs over recorded job listings, and the step fails loudly, not open, when a run whose first
   job has ended carries neither name.
3. **The sync loop's slot on the base.** One `ubuntu-slim` job for as long as a run is in flight,
   on the base's own slots, beside master runs (about ten jobs) and up to six benchmark jobs: 17 of
   20 at the busiest. A benchmark list larger than that waits in GitHub's queue, which costs time
   and nothing else.
4. **`pull_request_target` runs with a write token.** The loop must never check out or execute the
   pull request's code; it reads the API only, as `notify_test_workflow.yml` does. The review of
   the PR checks that the file contains no `actions/checkout`.
5. **Runner pairs on a CPU nobody chose.** A pair's verdict is read on its own machine, with the
   probe beside it; a results file regenerated on a runner names a CPU the committed neighbours
   in other sections may not share. `sections` (VARKA-256) keeps the rest of the file as
   committed, and the provenance says which machine wrote which.
6. **The master run's first runs fail on what nothing has run since September**, the Javadoc
   position first. That is the point of the run; the second PR of section 8 fixes what its own
   first run shows before it merges.
7. **GitHub changes a limit.** Every limit the design leans on is quoted in section 2 with its
   page; the step and the loop log their request counts, so a change shows as a failure with a
   number rather than as silence.

## 8. Sequencing

Five pull requests, each green on its own; the benchmark one first because three jobs are waiting
on it and it touches nothing the others do.

1. **255a, benchmarks on runners**: `benchmark.yml`'s `pr`, `pair` and `rounds`, the probe first,
   a class list as a matrix; test 8; then the three waiting jobs dispatched (322a's pairs, 325's
   files, 322a-2's pair) and prediction 7 scored as they finish.
2. **255b, master after each merge**: `varka-master.yml`, the weekly matrix's explicit `jobs`, the
   Javadoc fix and whatever else its first run shows; test 5; predictions 5. Confirms 2.5's reading
   of the weekly run's skip before relying on it.
3. **255c, the admission step and the check sync**: 3.1 and 3.2, `dev/varka_ci_admit.py` and its
   tests; tests 1 to 4; predictions 1 to 4 over the next working day with a full-matrix run.
4. **255d, the budget**: `step-budget.tsv` seeded from 2.7, `dev/varka_ci_times.py`, the nightly's
   `.secs`, the check summary; `dev/varka_ci_queue.sh` deleted and the skills rewritten; test 9;
   prediction 9 after ten pull requests.
5. **255e, new tests rerun and a fix's test failing on the base**: 3.4; tests 6 and 7; prediction
   6. If it grows past one pull request, a row of its own.

**Owner actions**, none of which an agent may take:

1. **Accept 3.1 and 3.2 in place of 2.4's cron dispatcher**, on 2.1 and 2.2's numbers.
2. **Create the labels `ci-hold` and `fix` on `vecbricks/varka`** before 255c and 255e merge;
   neither exists today (`repos/vecbricks/varka/labels`).
3. **Stop starting `dev/varka_ci_queue.sh run`, `hold` and `drop`** once 255c merges; a held
   run would be cancelled by the script and never rerun by anything.
4. **Nothing to enable in the fork's Actions tab** (2.1), and nothing to decide on 7.3: no file
   goes on the fork's `master`.

## 9. Outcome

