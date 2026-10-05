# VARKA-287: the parallel suites in CI

## 1. Where this came from

Row 287, from VARKA-286: the laptop's gate now splits each module's Varka suites over several
JVMs, and CI's `varka-scoped` jobs still run them in one sbt test JVM. The runners are GitHub's
standard hosted ones, 4 vCPUs and 16 GB. Each module's job runs three things in sequence: the
suites under the defaults in sbt (`catalyst/testOnly *Varka*`; `sql/testOnly *Varka*
*ArrowCachedBatchSerializerSuite`), the emitted-bytes verdict, and the option matrix's one
configuration beside the defaults (VARKA-248). On #626's run that last step took about 8 minutes
for catalyst and 13 for SQL.

## 2. The admission check, done

**There are two savings, and the larger is not the split.** The matrix step runs the defaults
again only for the declining check, which compares each test's fused batches under a
configuration with the defaults'; sbt's run records none. If the first step runs the suites
through the runner instead, it records them, and the matrix step can take the defaults from it
and run the configuration alone. That removes one of the job's two full passes over the suites.
The split then adds what four cores allow: two JVMs per module, for the defaults and for the
configuration.

**Measuring it needs a Varka-only pull request.** `dev/varka_scope.py` treats `dev/varka_*` as
Varka's alone and `.github/workflows/build_and_test.yml` as Spark's, so a pull request that edits
the workflow runs Spark's module matrix and skips the `varka-scoped` jobs it changes. So the work
is two pull requests: the first moves the test step into a script that does exactly what the step
does today, and adds a cache that keeps the suites' JUnit times between runs; the second changes
only scripts, so its own CI runs the new step in the jobs it changes, and is the measurement.

## 3. The design

### 3.1 The script, then what it runs

**First pull request.** `dev/varka_scoped_suites.sh <module>` runs the module's suites as the step
does now, in sbt with `-Phive`. The step calls it. A cache step restores and saves
`target/varka-scoped`, the runner's working directory, keyed by module and run with the module's
latest as the fallback, so a later run finds the previous run's JUnit times to balance by.

**Second pull request.**

* `dev/varka_scoped_suites.sh` runs the module's suites through `dev/varka_matrix.sh --defaults
  --module <m> --split 2 -j 2 --sbt-arg -Phive --out target/varka-scoped`; the SQL job adds
  `ArrowCachedBatchSerializerSuite`, which is Varka's without the name, through a new
  `--extra-suite`.
* `dev/varka_matrix_ci.sh` takes the defaults' fused batches from that run (`--defaults-from`, a
  new runner option that copies a finished defaults run in and skips running it) and runs the
  configuration alone, split over two JVMs.
* The emitted-bytes verdict is unchanged: the bytes oracle is among the catalyst suites either
  way.

### 3.2 What is deliberately unchanged

What the jobs run: the same suites, the same configuration pick, the same merge-base check (which
runs its own defaults on the base, since the base's fused batches are not the branch's). Spark's
own module jobs, which `SERIAL_SBT_TESTS` keeps serial upstream.

### 3.3 Registered op counts

None: no emitter change.

## 4. Files

| file | what |
|---|---|
| `.github/workflows/build_and_test.yml` | the test step calls the script; the times cache (first pull request) |
| `dev/varka_scoped_suites.sh` | the module's suites, in sbt, then through the runner |
| `dev/varka_matrix.sh` | `--extra-suite`, `--defaults-from` (second pull request) |
| `dev/varka_matrix_ci.sh` | the defaults taken from the suites' run, the configuration split |
| `m7/VARKA-287.md`, `m7/PLAN.md` | this plan, row 287 |

## 5. Tests, and what each is for

* The first pull request's CI runs Spark's module matrix, as any workflow change does; the
  `varka-scoped` jobs it changes first run on the next Varka-only pull request, the second.
* The second pull request's own CI: both `varka-scoped` jobs, their suites through the runner and
  the matrix step with the defaults reused, against #626's times.

## 6. The measurement

The `varka-scoped` jobs' wall time on the runners, per module: the test step and the matrix
step, before (a recent Varka-only run) and after (the second pull request's run).

### 6.1 Predictions, registered before the run

1. Each module's job is at least a third shorter, mostly from the matrix step's reused defaults.
2. The SQL test step, split over two JVMs, takes under two thirds of its sbt time.

## 7. Risks

1. **Memory**: two 4 GB test JVMs on a 16 GB runner, beside nothing else in the step.
2. **An unbalanced first split**: the runner has no JUnit times until the cache holds a run; the
   first run weights every suite alike.

## 8. Sequencing

Two pull requests, as 2 explains: the script and the cache, then the runner behind them.

## 9. Outcome
