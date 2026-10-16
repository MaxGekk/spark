# VARKA-377: what Spark's row path costs on each new type, measured before the kernels

*Row 377 of `m8/PLAN.md`, wave 0, added by the review of 16 October 2026 (10.3 item 6). Planned on
16 October 2026 at `8a50f61a368`. These benchmarks run on GitHub-hosted runners, not on the
laptop: the owner's instruction for this row, and the runners are where the milestone's lane rows
will be measured by their pairs.*

## 1. Where this came from

No row of the milestone measures a new type against Spark. 2.8 expects the largest speed-ups of the
milestone from decimals, because every Arrow consumer builds a `BigDecimal` per value, and says
plainly that the expectation is unmeasured; a floating or boolean shape may instead tie Spark's
generated code, which reads primitives. The house rule for a change that improves something is a
baseline benchmark first, on the unchanged code, in its own pull request, so that the change
reports its speed-up as a diff. Row 377 is that baseline for each new type family, so that each
lane row (329, 331 and 27, 338, 341, 347 and 359) diffs against a committed file instead of
claiming a number.

## 2. The admission check, done

1. **No benchmark covers a new type.** There is no `DecimalType`, `DoubleType`, `FloatType`,
   `BooleanType`, `ByteType`, `ShortType` or `TimestampNTZType` projection in any Varka benchmark;
   the one decimal is an argument built for `make_time` and `make_dt_interval` in
   `VarkaLongLaneThroughputBenchmark.scala` (127 to 136), not a measured projection.
2. **Where the classes go.** A comparison with Spark's whole-stage-generated row path needs a
   session, so the classes are in sql/core's benchmark package, Java on `VarkaBenchmarkBase`
   (`sql/catalyst/src/test/java/org/apache/spark/sql/VarkaBenchmarkBase.java`), as
   `VarkaCodegenBenchmark.java` already is in `sql/core/src/test/java/.../execution/benchmark/`,
   with a session per arm the way `VarkaFilterBenchmark.scala` builds its baseline and Varka
   sessions (69 to 79). `sql/varka/AGENTS.md`: a new benchmark is Java on that base.
3. **Both arms are Spark's row path today.** With Varka on, a projection over a new type declines
   at its first node, so the Varka arm measures the decline: the planning-time refusal and the
   row path. It should tie the baseline; a gap is the decline's cost, which is worth knowing
   before a lane row moves it.
4. **The runner workflow** (`.github/workflows/benchmark.yml`) is dispatch-only and x86-only
   (`ubuntu-latest`), takes a `class` and no module (a sql/core class runs by its full name through
   `sql/Test/runMain`), a `jdk` (25 for Varka), `expected-cpu` (lines 64 to 68: the pool spans
   four CPU families, `sql/varka/HARDWARE.md` 31 to 37, the EPYC 7763 its commonest draw), and
   `extra-java-options` (74 to 76, passed to the driver at 262, its own description using
   `-XX:MaxVectorSize=32` as the example), which carries the 128-bit arm as
   `-XX:MaxVectorSize=16`. A run with `pr` set returns the results as an artifact for the author
   to commit by hand; `create-commit` pushes only to the dispatched branch (`skills/benchmarking.md`
   207 to 218).
5. **Naming.** A runner's results file sits beside the laptop's under the precedent
   `sql/core/benchmarks/VarkaColdStartBenchmark-jdk25-runner-9v74-results.txt`; the laptop files
   stay the laptop's (`m7/PLAN.md` row 325). This row's classes have no laptop file, so their files
   are `<Class>-jdk25-runner-epyc7763-results.txt` and the 128-bit companion
   `<Class>-jdk25-runner-epyc7763-128bit-results.txt`, each with its provenance file.
6. **A runner's own spread is wide**: its A/A moved one row by 110% by best
   (`skills/benchmarking.md` 220 to 226), so each class gets an A/A pair on the runner
   (`pair=true`, `base=HEAD`, five rounds; `rounds` applies to pair mode only), the baseline is
   read by median and by best, and a later lane row's pair is read against that A/A on the same
   CPU.
7. **The 128-bit arm on x86 releases kernels** at a known rate since `m7` row 374 closed without a
   fix; with both arms on the row path today it releases none, but the lane rows' later pairs will,
   so the class records the rate column from the start (`m8/PLAN.md` 10.3 item 4).

**What still needs a run.** The baselines themselves, one dispatch per class:

    gh workflow run benchmark.yml --repo MaxGekk/spark --ref <branch> \
      -f class=org.apache.spark.sql.execution.benchmark.VarkaDecimalBaselineBenchmark \
      -f jdk=25 -f expected-cpu="AMD EPYC 7763" -f pr=<n>

again with `-f extra-java-options=-XX:MaxVectorSize=16`, and once as an A/A pair
(`-f pair=true -f base=HEAD -f rounds=5`), whose spread is what a lane row's later pair on the same
CPU is read against. Dispatching is an external operation
and waits for the owner's go-ahead per the fork's CI rules (concurrent fork runs must fit the
account's job limit together). Nothing in the row's premise depends on the result: the run is the
deliverable, and the row is admitted as it stands.

## 3. The design

### 3.1 One class per family, four shapes each

| class | family | gates |
|---|---|---|
| `VarkaNarrowIntBaselineBenchmark` | `TINYINT`, `SMALLINT` | 329, 330 |
| `VarkaBooleanBaselineBenchmark` | `BOOLEAN` | 331, 27 |
| `VarkaTimestampNtzBaselineBenchmark` | `TIMESTAMP_NTZ` | 338 to 340 |
| `VarkaFloatingBaselineBenchmark` | `DOUBLE`, `FLOAT` | 341 to 344 |
| `VarkaDecimalBaselineBenchmark` | `DECIMAL(9, 2)`, `(18, 4)`, `(38, 10)` | 347, 348, 349, 359 |

Each class has the same four sections over a cached Arrow-backed table (the input the kernels will
read, built with `ArrowCachedBatchSerializer`), at a fixed row count and two null densities (0 and
0.1): a **comparison** (`x < y` as a filter), an **arithmetic chain** of three nodes (`(x + y) *
z - x`, or for `BOOLEAN` `(a AND b) OR NOT c`), a **cast** (to the type's widest neighbour in the
matrix: `TINYINT` to `INT`, `TIMESTAMP_NTZ` to `DATE`, `DOUBLE` to `BIGINT`, a decimal to the
next precision), and a **choice** (`CASE WHEN x < y THEN x ELSE y END`). Each section has two
arms, named so the labels survive the lanes landing: `spark row path (varka off)` and `varka on`.
The decimal class runs its sections at each of the three precisions, so the 18-to-19 boundary
the lane pair crosses is in the baseline.

### 3.2 Each class its own pull request, merged before its family's first lane row

The house rule for a baseline. The five are independent, so they can be dispatched together within
the account's job limit, and merged in any order; each must merge before the row it gates opens its
measurement, which then diffs against it with `dev/varka_bench_diff.py`.

### 3.3 What is deliberately unchanged

* No compiler, emitter or kernel change: the row measures the unchanged code.
* The laptop's files and the house's regeneration script: these classes are runner-only, and a
  laptop run of them, if ever made, is a separate file.
* No Arm runner benchmark: `benchmark.yml` is x86-only, and adding Arm is not this row's work.
* Spark's aggregates (q6's `sum`) stay out, as the milestone's scope says.

### 3.4 Registered op counts

None: this row emits no kernel.

## 4. Files

* `sql/core/src/test/java/org/apache/spark/sql/execution/benchmark/Varka*BaselineBenchmark.java`:
  five new classes, one per pull request
* `sql/core/benchmarks/Varka*BaselineBenchmark-jdk25-runner-epyc7763-results.txt`: the runner's
  results, committed by hand from the artifact
* `...-runner-epyc7763-128bit-results.txt`, `...-provenance.txt`: the 128-bit companion and the
  provenance

## 5. Tests, and what each is for

1. **Each class's dry run** in the pull request's CI (the benchmark harness's quick mode, one
   iteration) builds every section; catches a section that fails before the runner time is spent.
2. **The Varka arm's decline is checked, not assumed**: each section asserts once, before timing,
   that the Varka session's plan for it contains no Varka node; catches a section that a merged
   lane row has quietly started to fuse, which would make it no longer a baseline.

## 6. The measurement

Each class on an EPYC 7763 runner at its 256-bit species and at `-XX:MaxVectorSize=16`, both
arms, by the harness's methodology (`VARKA-14.md` 2.1), read by median and best, beside the
class's A/A pair.

### 6.1 Predictions, registered before the run

1. **The two arms tie in every section** within the class's own run-to-run spread, since both are
   the row path; the Varka arm's planning refusal is per query, not per row.
2. **The decimal sections are the slowest per row** of the five classes at every precision, and
   `DECIMAL(38, 10)` slower than `DECIMAL(18, 4)` by more than the class's spread, because every
   value is a `BigDecimal` above 18 on every path and every value read through `ArrowColumnVector`
   is one below (365's section 2).
3. **The floating and narrow-int comparison sections are within a factor of two of the existing
   `BIGINT` comparison** in the long-lane benchmarks' Spark arm, since Spark's generated code reads
   primitives for all of them.
4. **The width does not move the baseline**: the 256-bit and 128-bit files agree within the spread,
   since neither arm has a kernel.

## 7. Risks

1. **The runner pool varies.** `expected-cpu` pins the EPYC 7763 and fails a run on another CPU;
   a pair later run on a different CPU is not comparable with these files.
2. **A lane row merges before its baseline.** Section 3.2's order, and test 2, which fails the
   baseline once its section fuses.
3. **The cached input shapes the result.** A table read through `ArrowColumnVector` builds the
   `BigDecimal` that 365 may remove; if 365 lands under its option (i) or (ii), the decimal
   baseline is rerun on the same CPU and both files kept, labelled by the commit.

## 8. Sequencing

1. This plan.
2. Five pull requests, one class each, each with its dry run green; the runner dispatches after the
   owner's go-ahead, the results committed into the same pull request from the artifacts.
3. Each lane row's measurement section names its baseline file.

*Recorded 16 October 2026:* nothing is dispatched yet, since none of the five classes is written.
The order is each class written, one pull request each, then the dispatches within the fork's job
budget. The naming question (section 2 item 5) is settled when the classes' pull requests open,
by the names they carry.

Explicitly out of this row: any kernel, laptop baselines, Arm benchmarks, aggregates, and the lane
rows' own pairs.

## 9. Outcome

*To be written when the work lands.*
