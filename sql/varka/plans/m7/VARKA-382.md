# VARKA-382: Error Prone and NullAway on every pull request

## 1. Where this came from

Row 382 of `m7/PLAN.md`. VARKA-266 added `dev/varka_errorprone.sh` - Error Prone and NullAway over
Varka's Java under `-Werror`, with a pending-class list that only shrinks - and ran it in one place,
the laptop gate's lint step (`dev/varka_gate.sh`, `lane_sbt`). Its plan said where CI stood:
"CI's linters job is Spark's and is left as it is; a CI step is its own row if the gate is not
enough" (`VARKA-266.md` 3.6). It was not enough. Two pull requests merged with findings no CI
step looked for - #809 (`VarkaReproduction`) and #827 (`VarkaKernelWarmup`,
`VarkaVectorTemplateWarmup`) - and the script failed on master until the fix in #875 (branch
`varka-errorprone-827`, not on master when this plan was written). Done when the check runs in
the pull request CI's own jobs whenever a file it reads may have changed, fails the job on a
finding, and its added CI time is stated.

## 2. The admission check, done

Checked on 16 October 2026 against `51f9e03e616` (master) and the fork's runs of `build_main.yml`.

* **Where Varka's CI runs.** `build_main.yml` calls `build_and_test.yml` with `java: 25`. Its
  precondition classifies the diff with `dev/varka_scope.py`: `spark` (any file outside Varka's
  own) runs Spark's module matrix behind `Precompile Spark`; `scoped` (only Varka's files, some
  code) runs the two `varka-scoped` jobs instead; `docs` and `none` run neither. The `lint` job
  ("Linters, licenses, and dependencies") runs on every run, its steps gated by `lint-spark`,
  `lint-code` and `lint-python`; `lint-code` is false only for documents.
* **Which scope the two breakages had.** #827 was `scoped`; #809 was `spark`, because it touched
  `SQLConf.scala`. A step only in the `varka-scoped` job would have caught #827 and missed #809, so
  the check has to run for both scopes, and the lint job is the one job that does.
* **What the script needs.** JDK 25 for both `java` and `javac` (it refuses another; the lint job
  installs the caller's `inputs.java` with `actions/setup-java`, a JDK). The five jars are pinned
  by version and sha256 and fetched from Maven Central into `build/varka-errorprone/` on first
  use, a few megabytes, with a copy in `~/.m2` used when one matches - network the lint job has.
  And a classpath: without `--classpath-file` it runs `build/sbt sql/compile` and exports
  `sql/Compile/fullClasspath`, so the lint job must compile catalyst, core and sql's main code.
* **What that compile costs in the lint job.** In a `spark` run the MiMa step before it
  (`dev/mima`, `mimaReportBinaryIssues`) has compiled sql's main code already, under Spark's
  profiles; the script's sbt run without them should find nothing to do, leaving the sbt start
  (the Scala linter step, mostly sbt's start, took 140 s in run 38914504326's lint job) and javac.
  In a `scoped` run MiMa is skipped and the compile is cold. The nearest measured number: the
  `varka-scoped` sql job of the same run compiled sql's main and test code with `-Phive` in 9
  minutes 45 s (14:21:02 to 14:30:46 in its log), its Coursier cache restored.
* **Which refs the script compares against, and where they come from on a fork.** The shrink
  check compares the pending list with the merge base's (default: the merge base with
  `origin/master`), and the unannotated set drops the names master has taken off its list since
  that base (read from `origin/master`). Pull request CI runs on the fork MaxGekk/spark, whose
  `origin/master` is Apache Spark's master (`ee11a92a9f1`, no pending list). The default base
  there would be the merge base with Apache Spark, whose tree has no list, so the shrink check
  would be skipped with a message; and the master read would find nothing, so the second check
  would vanish without one. `checkout-and-sync` already exports `VARKA_DIFF_BASE`, the merge base
  with vecbricks/varka's master, and nothing names that master itself.
* **Other callers.** `build_java17.yml` enables the lint job with `java: 17`; `varka-master.yml`
  and `varka-weekly-matrix.yml` enable it with `java: 25` and pass their own `jobs` JSON, where a
  flag the precondition adds is missing and so read as true by the `!= 'false'` convention.
* **The step-time budget** (`sql/varka/ci/step-budget.tsv`, #876) is not on master; nothing here
  edits it.

What the check would have rejected: a step only in the `varka-scoped` job (it misses #809's
scope), and a step that trusts `origin/master` on the fork (it silently drops one of the script's
two comparisons).

## 3. The design

### 3.1 One step in the lint job, gated by what the check reads

* **The step.** After the Varka proofs, in the lint job: fetch vecbricks/varka's master, then
  `dev/varka_errorprone.sh --master <that commit> --base "$VARKA_DIFF_BASE"`. The script's exit
  status is the step's, so a finding fails the job. It runs only when `inputs.java == '25'`, the
  JDK the script is written for, which keeps the Java 17 caller's lint job as it was.
* **The script gets `--master REF`.** Default `origin/master` as before, so the gate and laptop
  runs are unchanged; CI names the fetched commit. `--base` already exists.
* **The gate, a new precondition flag `lint-errorprone`.** True for every `spark` run and every
  run with no diff (master, a dispatch): those runs compile sql in MiMa first, so the step is
  cheap. For a `scoped` or `docs` run, true only when the diff touches what the check reads: main
  Java under `sql/catalyst/src/main/java/` or `sql/core/src/main/java/`, `dev/varka_errorprone.sh`
  or `dev/varka_nullaway_pending.txt`. So a Varka-scoped change to Scala, tests, tools or the
  engine does not pay the cold compile. The grep reads its whole input (no `-q`), since the shell
  runs under `pipefail`.

### 3.2 What is deliberately unchanged

The script's checks, its pinned jars and its sources; the gate's lint step; the `varka-scoped`
jobs. The alternative of a second step there for `scoped` runs, reusing the classpath
`dev/varka_matrix.sh` exports (`target/varka-scoped/classpath`, a test classpath and so a superset
of the main one) and costing no compile, is held back: two call sites with two conditions for a
saving in job-minutes and none in wall time (the lint job finishes long before the `varka-scoped`
jobs' 30 to 35 minutes). It is the change to make if the lint job's compile turns out longer than
predicted.

### 3.3 Registered op counts

None: no emitted code changes.

## 4. Files

| file | what |
|---|---|
| `.github/workflows/build_and_test.yml` | the `lint-errorprone` flag in the precondition; the step in the lint job |
| `dev/varka_errorprone.sh` | `--master REF` for the ref whose pending list the unannotated set follows |
| `sql/varka/plans/m7/PLAN.md` | row 382 |
| `sql/varka/plans/m7/VARKA-382.md` | this plan |

## 5. Tests, and what each is for

* **The script passes on master with #875's fixes** - a temporary local merge of
  `fork/varka-errorprone-827`, never committed - run as the step runs it, with `--master` and
  `--base` named: the step is green once #875 lands.
* **The script fails on this branch without them**, naming #809's and #827's findings: the step
  fails a job on a finding, which is the row's point.
* **The flag's shell over the cases** (spark with no diff; scoped with only a Varka tool; scoped
  with a Varka Java source; docs; the pending list; the script; an engine Java file): each gives
  the flag the design says.
* **The workflow parses and `actionlint` reports nothing new** against master's file.
* **This pull request's own CI**: it changes the workflow, so its scope is `spark` and the lint
  job runs the step after MiMa - the cheap path, scored in 9. The `scoped` path is first seen on
  the next Varka-scoped pull request that touches Varka's Java.

## 6. The measurement

The step's time in the lint job, from the job's step timings, in a `spark` run (this pull
request's) and the first `scoped` run that triggers it.

### 6.1 Predictions, registered before the run

The calibration table's CI-adjacent lines run 0.9 to 1.3 (VARKA-378, VARKA-280); these are stated
at 1.3 times the arithmetic.

1. **A `spark` run: about 2.5 minutes.** sbt's start and a no-op compile and export, about 90 s
   (the Scala linter step's 140 s holds a start and scalastyle), the jars' download a few seconds,
   javac about 20 s on the runner's four cores (11 s on the laptop's 24), times 1.3. Wrong if
   sbt's run without MiMa's profiles recompiles sql: then about 9 minutes.
2. **A `scoped` run that touches Varka's Java: about 10 minutes.** A cold compile of sql's main
   code only, about two thirds of the 9:45 that main and test took in the `varka-scoped` job,
   plus the start and javac, times 1.3. No wall time: the lint job's 7.5 minutes in run
   38914504326 become about 18, under the `varka-scoped` jobs' 30 to 35.
3. **Until #875 merges, the step fails on every branch based on master**, with #809's and #827's
   findings.

## 7. Risks

1. The `spark` path recompiles because the script's sbt run omits MiMa's profiles - prediction 1
   scores it; the fix would be passing the classpath MiMa's build exports.
2. The lint container lacks a tool the script uses (`curl`, `sha256sum`, `comm`, `awk`) - this
   pull request's CI shows it on the first run.
3. The fetch of vecbricks/varka's master fails; the step then fails rather than run with a wrong
   ref. `checkout-and-sync` fetches the same URL earlier in the job, so a failure here is a
   network failure for both.

## 8. Sequencing

One commit for the plan, one for the step and the option, the outcome after the local checks.

## 9. Outcome

Built as designed in 3.1, checked locally on 16 October 2026; this pull request's CI scores 6.1.

### 9.1 A third breakage, found by the check this row adds

Run on master with #875's fixes merged (`51f9e03e616` plus `fork/varka-errorprone-827`, a local
merge never committed), the script still failed: one NullAway error,
`VarkaEmitTrace.java:101: @NonNull field 'onEmission' not initialized`. #861 (VARKA-280) added
that field after #875 branched from `e0acfd4fa4a`, and #875's own commit passes (exit 0, 37 s with
the classpath exported, 7 s of javac). So between #875's branch and its merge a third pull request
merged with a finding no CI step looked for - the gap this row closes, reproduced while it was
being closed. #875 merged as `cf5aa283b6e` while this branch was in progress; the branch merges
master and fixes the finding the way the code means it: the field is null wherever no suite asks
for emission records, which its Javadoc already says and `noteEmission` already checks, so it is
`@javax.annotation.Nullable` rather than suppressed.

### 9.2 Checks

* **The script on this branch, as the CI step runs it** (`--master` and `--base` named): exit 0,
  98 files, 37 classes pending NullAway; 138 s with a cold `sql/compile` on the laptop, 7 s of
  javac. With the defaults the gate uses: exit 0.
* **The script on the branch before the merge and the fix fails**, naming #809's findings in
  `VarkaReproduction` (three NullAway errors and five warnings), #827's in
  `VarkaVectorTemplateWarmup` and `VarkaKernelWarmup`, and #861's: exit 1, so a finding fails the
  step.
* **The flag's shell over 3.1's cases** gives the design's value in each (5's list).
* **The workflow parses**, and `actionlint` reports the same 60 findings on it as on master's
  file, none new; `shellcheck` the same three on the script as before.
* **The suites over `VarkaEmitTrace`** (`VarkaKernelPlanSuite`, `VarkaCoverageSuite`,
  `VarkaEmitterBudgetSuite`, `VarkaIrFuzzSuite` through `dev/varka_matrix.sh --defaults`, the
  memory sanitizer on): 61 passed, none failed, 3 cancelled - the opt-in dumps and the opt-in
  steering A/B, which cancel without their properties - in 253 s over four JVMs.

6.1's predictions 1 and 2 are scored on the first CI runs that reach the step; prediction 3 is
void, since master passes with this branch's fix.
