# VARKA-322: The rest of the Scala benchmarks in Java

## 1. Where this came from

Row 322 of `m7/PLAN.md`, added by the owner on 12 October 2026 with rows 320 and 321 (`PLAN.md`
2.3, "The rest of the test code to Java"). Row 224 built the Java adapter over Spark's benchmark
harness, `VarkaBenchmarkBase`, and proved it on one class, `VarkaCompileBenchmark`, within 5% of
the Scala form on every row (`VARKA-224.md` 9). Since then a benchmark is ported only when a task
touches it (`sql/varka/AGENTS.md`), so the Scala benchmarks stay Scala for as long as nobody needs
to change them, and a new benchmark that resembles one of them is written by copying Scala into
Java. This row ports the rest: 26 classes, 8,283 lines, so the benchmarks stop holding the Scala
test toolchain in place and no new benchmark copies a Scala one.

Each class keeps its fully qualified name, its sections and its case names, so its committed
results file stays its results file and diffs line for line against the next regeneration, and
each proves itself with `dev/varka_bench_pair.sh`: the Scala class as the base, the Java one as
the head, every case within its committed band, or within 5% where no band is committed.

## 2. The admission check, done

**The inventory** (13 October 2026, master `0184df4e978`). Every `*Varka*Bench*.scala` file, 26 of
them and 8,283 lines, by the base it extends:

| class | module | lines | extends | step |
|---|---|---:|---|---|
| `VarkaCodegenBenchmark` | core | 91 | `BenchmarkBase` | 322a-1 |
| `VarkaUnreadLocalsBenchmark` | catalyst (`codegen.varka`) | 165 | `BenchmarkBase` | 322a-1 |
| `VarkaSharedPrefixBenchmark` | catalyst | 175 | `BenchmarkBase` | 322a-1 |
| `VarkaLongLaneBenchmark` | catalyst | 183 | `BenchmarkBase` | 322a-1 |
| `VarkaEmissionBenchmark` | catalyst | 195 | `BenchmarkBase` | 322a-1 |
| `VarkaVectorApiProbeBenchmark` | catalyst | 218 | `BenchmarkBase` | 322a-1 |
| `VarkaMethodSizeBenchmark` | catalyst | 251 | `BenchmarkBase` | 322a-1 |
| `VarkaArithmeticBenchmark` | catalyst | 340 | `BenchmarkBase` | 322a-1 |
| `VarkaTimeBenchmark` | catalyst | 434 | `BenchmarkBase` | 322a-1 |
| `VarkaWideKernelBenchmark` | catalyst | 458 | `BenchmarkBase` | 322a-1 |
| `VarkaEmitterParityBenchmark` | catalyst | 2,191 | `BenchmarkBase` | 322a-2 |
| `VarkaSizeLadderParallelBenchmark` | core | 115 | `SqlBasedBenchmark` | 322b |
| `VarkaRangeFilterBenchmark` | core | 123 | `SqlBasedBenchmark` | 322c |
| `VarkaFilterNarrowingBenchmark` | core | 151 | `SqlBasedBenchmark` | 322c |
| `VarkaSizeLadderTuningBenchmark` | core | 173 | `SqlBasedBenchmark` | 322c |
| `VarkaEvaluatorOverheadBenchmark` | core | 182 | `SqlBasedBenchmark` | 322c |
| `VarkaSchemaWidthBenchmark` | core | 184 | `SqlBasedBenchmark` | 322c |
| `VarkaWarmupDirectiveBenchmark` | core | 191 | `SqlBasedBenchmark` | 322c |
| `VarkaInExpressionBenchmark` | core | 200 | `SqlBasedBenchmark` | 322c |
| `VarkaSizeLadderBenchmark` | core | 207 | `SqlBasedBenchmark` | 322c |
| `VarkaFilterBenchmark` | core | 217 | `SqlBasedBenchmark` | 322c |
| `VarkaNarrowingBenchmark` | core | 225 | `SqlBasedBenchmark` | 322c |
| `VarkaLongLaneThroughputBenchmark` | core | 311 | `SqlBasedBenchmark` | 322c |
| `VarkaColdPathBenchmark` | core | 340 | `SqlBasedBenchmark` | 322c |
| `VarkaColdStartBenchmark` | core | 445 | `SqlBasedBenchmark` | 322c |
| `VarkaThroughputBenchmark` | core | 518 | `SqlBasedBenchmark` | 322c |

**The row's split is eleven and fifteen, not twelve and fourteen.** `VarkaCodegenBenchmark` lives in
`sql/core` but extends `BenchmarkBase` and emits a kernel directly, so it belongs with the kernel
benchmarks; all fifteen `SqlBasedBenchmark` classes are in `sql/core`. So 322a ports eleven
classes (4,701 lines), 322b proves the counterpart on one, and 322c ports fourteen.

**What the eleven need from the adapter.** Read class by class, they use `runBenchmark`, `new
Benchmark(name, values, minNumIters, warmupTime, minTime, output)`, `addCase` with and without a
fixed iteration count, and `run()`: all of it is in `VarkaBenchmarkBase` and `VarkaBenchmark`. Two
uses are not:

* **Selecting cases.** `VarkaArithmeticBenchmark`, `VarkaTimeBenchmark` and
  `VarkaEmitterParityBenchmark` keep only the cases whose name contains `-Dvarka.bench.only` by
  rewriting the Scala `Benchmark`'s case buffer, and skip `run()` when nothing is left or under
  `-Dvarka.bench.dryRun`, the mode `dev/varka_bench_ids.sh` runs to list the case ids. The wrapper
  hides that buffer, so it gains two methods: keep the cases whose name contains a string, and
  whether any case is left.
* **Writing to the results file outside a table.** `VarkaUnreadLocalsBenchmark` appends its
  allocation lines to the run's results file. The base class already holds that stream
  (`output()`), so a subclass writes it through the inherited Scala getter; no addition.

`addTimerCase`, `codegenBenchmark` and the per-iteration output are used by none of the eleven and
stay unwrapped, as row 224 left them for a benchmark that needs them.

**How a Java benchmark runs, and why base and head never share a tree.** A Java subclass declares
no `main`; JDK 25's launcher calls the inherited instance `BenchmarkBase.main` (JEP 512), and
`build/sbt "<module>/Test/runMain <class>"` forks through it, so Spark's own `main` names and writes
the results file (`VARKA-224.md` 2). `dev/varka_bench_regen.sh` and `dev/varka_bench_pair.sh`
resolve a bare class name from a `.scala` or a `.java` file under the module's test sources since
row 224. A Java class cannot sit beside the Scala object of the same name: the object compiles to
the class `Name` (its static forwarders) and `Name$`, so the two would define `Name` twice. Each
`.scala` file is therefore deleted in the commit that adds its port, and the pair compares across
commits: the pair runner builds the merge base, where the name is the Scala object, in a worktree
of its own, and runs the head, where it is the Java class, here, both under one name.

**The pair runner already reads best times and runs A/A.** The row asks for two changes before
322a's first pair: the verdict comparing best times as well as medians, and a Java-against-Java
pair measuring the runner's own spread. Both landed on 12 October 2026 (#769, "The pair runner
reads best runs too, and its own spread"): `dev/varka_bench_pair.py compare` prints each
row's change by the median and by the best run and
marks a row slower only when both are, and `dev/varka_bench_pair.sh --base HEAD` pairs the head
with itself. Nothing is to build for them; 6 uses them.

**The ids, recorded before any port.** `dev/varka_bench_ids.sh VarkaArithmeticBenchmark
VarkaTimeBenchmark` on the Scala forms, saved before the first port, is the oracle for the ported
dry run (5).

**What 322b and 322c need, sketched.** None of the fifteen calls `withSQLConf`, though the row
names it, and none overrides `getSparkSession`. Eight build their sessions through the Scala helper
`VarkaArrowSessions`; the other seven (`VarkaCodegenBenchmark` aside) stop the trait's default
session and build a baseline and a Varka session of their own with `SparkSession.builder()`, and
two (`VarkaSchemaWidthBenchmark`, `VarkaSizeLadderTuningBenchmark`) set a configuration with
`conf.set` and `conf.unset` around a case. The counterpart needs the default session made lazily
so a class that never uses it does not start it, a scoped set that restores the previous value,
`addTimerCase` (`VarkaColdPathBenchmark` and `VarkaColdStartBenchmark`), an overridable `suffix`
(`VarkaSizeLadderTuningBenchmark`) and the `noop()` write (used by eleven).
Four Scala helpers come along or stay callable: `VarkaArrowSessions` (134 lines),
`VarkaColdPath` (100) and `VarkaColdPathProbe` (81), `VarkaSizeLadder` (inside
`VarkaSizeLadderBenchmark.scala`, used by 322b's proof class) and `VarkaWarmupDirectiveChild` (a
child JVM's `main`, inside its benchmark's file).

## 3. The design

### 3.1 322a: the kernel benchmarks on the adapter

Each class becomes a Java class of the same name in the same package, extending
`VarkaBenchmarkBase`, in the module's `src/test/java`: nine in catalyst, `VarkaCodegenBenchmark` in
core (whose test classpath has catalyst's test classes). A port is statement for statement:

* **The same work in every timed body.** The data, the emitted kernels and the per-batch calls are
  the Scala's, and so is the harness work inside a timed loop, the arrays
  `VarkaWideKernelBenchmark`'s `scan` builds per batch included. Removing it would make a faster
  Java row that is not a port; it is a separate change for a later task.
* **The same names.** Section titles, benchmark names and case names, including their computed
  parts (a loop-method count, a group count, a byte count, `sizes.mkString(" + ")`), are formatted
  exactly as the Scala formats them, and the emitted kernels keep their class names
  (`VarkaArithBench<n>`, `VarkaTimeBench<n>`, `VarkaWideBench<n>`, ...), which dumps and
  provenance refer to.
* **The same checks.** `require` becomes a thrown `IllegalArgumentException` with the
  `requirement failed: ` prefix, as row 224's port did; Scala's `assert` in
  `VarkaLongLaneBenchmark` becomes a thrown `AssertionError`, since a Java `assert` is off by
  default. `scala.util.Random(seed)` is `java.util.Random(seed)`, drawn in the same order.
* **The sinks stay live.** A Scala `var` a closure accumulates into becomes a field, as in
  `VarkaCompileBenchmark`, so the JIT keeps the work for the same reason it did.
* **Modern Java where it is free**: records for the tuples the Scala builds (a shape, an arm, a
  rung), streams and lambdas in setup only. No `default` on a switch over a sealed type.

The adapter's addition, in `VarkaBenchmark`: `retainCases(String)`, which keeps the cases whose
name contains the string, and `hasCases()`. The dry-run and `only` logic was planned to stay in
each of the three benchmarks that use it, as in the Scala; the review of 322a-1 moved it into
`VarkaBenchmarkBase` instead (9, "The review's fixes"), so the parity benchmark's port calls it
rather than adding a third copy.

### 3.2 322b: a Java counterpart of `SqlBasedBenchmark`

`VarkaSqlBenchmarkBase` in core's test tree, extending `VarkaBenchmarkBase`: the session built
lazily with `SqlBasedBenchmark`'s defaults and stopped in `afterAll`,
`withConf(SparkSession, Map<String, String>, Runnable)` restoring every value it set (unset where
none was) so no case leaks into the next, `noop(Dataset)`, and the wrapper's `addTimerCase`.
Proved on `VarkaSizeLadderParallelBenchmark` with `VarkaSizeLadder`, as 224 proved the first
adapter.

### 3.3 322c: the other fourteen

On 3.2, smallest first, the shared helpers ported with their first user. Their designs are
written in a section 10 of this plan when 322b's pair is in.

### 3.4 What is deliberately unchanged

* **The committed results files.** No file is regenerated; a port reproduces its file.
* **The scripts.** `dev/varka_bench_regen.sh`, `dev/varka_bench_pair.sh`, `dev/varka_bench_ids.sh`
  and `dev/varka_bench_repeat.sh` take a class by its name and run it through `runMain`.
* **`spark-submit`.** The scripts are not the only entry point: `.github/workflows/benchmark.yml`
  runs `bin/spark-submit --class org.apache.spark.benchmark.Benchmarks`, and Spark's `Benchmarks`
  runner finds each class's `main` by reflection and called it as a static method, which a Java
  benchmark's inherited instance `main` is not (a `NullPointerException` from `invoke(null,
  ...)`). That one runner changes, in core's test tree: a `main` that is not static is invoked on
  an instance made with the class's no-arg constructor, and a static one as before (9, "The
  review's fixes"). `spark-submit` itself is left as it is: `--class` naming a Java benchmark
  directly still fails, since `JavaMainApplication` requires a static `main`, so a Java benchmark
  is run through `Benchmarks`, as the workflow does, or through `runMain`. Changing core's
  launcher for a test-tree convenience is out of proportion.
* **Spark's own Scala benchmarks**, and `sql/varka/bench`'s surface harness.

### 3.5 Registered op counts

None move; no emitter code changes.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/src/test/java/org/apache/spark/sql/VarkaBenchmark.java` | `retainCases`, `hasCases`, `Builder.timed()` |
| `sql/catalyst/src/test/java/org/apache/spark/sql/VarkaBenchmarkBase.java` | the dry run, case filter, case ids and their results-file guard; `require`, `defineKernel`, `roots`, `seq` |
| `core/src/test/scala/org/apache/spark/benchmark/Benchmarks.scala` | an instance `main` invoked on a new instance |
| `sql/catalyst/src/test/java/.../sql/Varka{Compile,Grouping}Benchmark.java` | their private `seq`, which the base's now clashes with, removed |
| `sql/catalyst/src/test/java/.../sql/Varka{SharedPrefix,LongLane,Emission,VectorApiProbe,MethodSize,Arithmetic,Time,WideKernel}Benchmark.java` | 322a-1's catalyst ports |
| `sql/catalyst/src/test/java/.../codegen/varka/VarkaUnreadLocalsBenchmark.java` | 322a-1 |
| `sql/core/src/test/java/.../execution/benchmark/VarkaCodegenBenchmark.java` | 322a-1 |
| `sql/catalyst/src/test/java/.../sql/VarkaEmitterParityBenchmark.java` | 322a-2 |
| `sql/core/src/test/java/.../execution/benchmark/VarkaSqlBenchmarkBase.java` | 322b |
| the fifteen `sql/core` ports and their helpers | 322b and 322c |
| each ported class's `.scala` | deleted with its port |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-322.md` | the row and this plan |

## 5. Tests, and what each is for

* **The compile**: `catalyst/Test/compile`, and `sql/Test/compile` for the core ports.
* **The dry run's ids**: `dev/varka_bench_ids.sh VarkaArithmeticBenchmark VarkaTimeBenchmark` on
  the Java forms prints the ids and the next free id 2 recorded from the Scala forms. It catches a
  kernel emitted under another id, or one dropped or added.
* **A run of every port, against its committed file's names**: each ported class run once in full
  without `SPARK_GENERATE_BENCHMARK_FILES`, its printed section titles, benchmark names and case
  names compared, in order, with its committed `-jdk25-results.txt`. It catches a renamed or
  reordered case and a computed label formatted differently, which is what would break the line
  for line diff; it says nothing about the timings, which are 6's.
* **The pairs**, in 6.
* **The gate**, at both widths.

## 6. The measurement

`dev/varka_bench_pair.sh <module> <class>` per ported class, on the idle laptop: the Scala class at
the merge base and the Java class on the branch, in alternating rounds, read by the median and by
the best run. A row passes within its committed band (`-jdk25-band.txt`, where one exists:
Arithmetic, MethodSize, SharedPrefix) or within 5% by the best run where none is committed. Before
the first: one A/A pair, `dev/varka_bench_pair.sh catalyst VarkaEmissionBenchmark --base HEAD`, the
runner's own spread on the Java side, against which a port's change is read.

The pair runner restores the committed files after each round, so no results file changes.

### 6.1 Predictions, registered before the run

1. **Every row of `VarkaCodegenBenchmark`, `VarkaEmissionBenchmark`, `VarkaArithmeticBenchmark`,
   `VarkaLongLaneBenchmark`, `VarkaSharedPrefixBenchmark` and `VarkaMethodSizeBenchmark` within 5%
   by the best run**, and within the A/A spread for most: their timed bodies are an emitter call
   or an emitted kernel's `run` with the same arguments, and the loop around it changes language.
   Codegen and Emission, millisecond bodies of emitter work, tightest.
2. **The hand-written rows may move either way**, past 5% for a few: `VarkaTimeBenchmark`'s
   hand-written magic multiply and `VarkaVectorApiProbeBenchmark`'s `passes` loops are the
   benchmark's own code, not an emitted kernel, so javac's bytecode in place of scalac's reaches
   C2 differently. A row of these outside 5% is read against the A/A spread before it is called
   a port's penalty.
3. **`VarkaWideKernelBenchmark` and `VarkaUnreadLocalsBenchmark` read the same or slightly
   faster**: their per-batch harness work (three small arrays per 4096-row batch, a tuple
   unpacked) is the same work in Java, and a Scala closure over it costs no less. A faster row
   there is read as the harness, not as a dropped sink, if the dry run and the names held.
4. **No row slower by 10% by both the median and the best**, the pair runner's gate.
5. **The ids are identical** (5), and every port's names match its committed file.
6. **The Java is longer**, by a fifth to two fifths, as the earlier ports grew: Scala's
   `for`-comprehensions over tuples become loops over records.

## 7. Risks

1. **A label formatted differently** (a `%s` of a Scala `Seq`, a float format) breaks the line
   for line diff without failing anything. 5's name comparison catches it.
2. **A dropped sink.** A Java accumulator the JIT can prove dead makes a row faster for the wrong
   reason. The sinks are fields as in `VarkaCompileBenchmark`; a row faster beyond the A/A spread
   is read as this first.
3. **The machine.** The pairs need the idle laptop; one taken beside another task's build reads
   that build. They are run on a quiet machine and the power profile recorded with them.
4. **A class that does not port mechanically** is left Scala and recorded in 9, not forced.

## 8. Sequencing

1. This plan, and the row marked Planned.
2. **322a-1**, one pull request: the adapter's two methods, then the ten classes of 4.1 (all of
   322a but the parity benchmark), each port with its `.scala` deleted in the same commit; the ids
   and the names checked (5). Ten files of 91 to 458 lines are reviewable one by one; the pairs
   follow on the quiet machine, before the merge.
3. **322a-2**, its own pull request: `VarkaEmitterParityBenchmark`, a fifth of the whole with its
   hand-written reference kernels, and its pair.
4. **322b**: the counterpart, proved on `VarkaSizeLadderParallelBenchmark`, and its pair.
5. **322c**: the fourteen, smallest first, in one or two pull requests by size.
6. Section 9 as each lands; the row Done when 322c's pairs are in.

## 9. Outcome

**322a implemented, pairs pending** (13 October 2026; 322a-2 the same day). The row stays
Planned.

**What was built.** 322a-1 as 8.2 says, in one commit after this plan: the ten classes of 4.1 are
Java under their names and packages, their `.scala` files deleted, 2,510 lines of Scala to 3,137
of Java (a quarter more), the parity benchmark left for 322a-2. The adapter gained more than 2
listed:
`VarkaBenchmark.retainCases` and `hasCases` as planned, and in `VarkaBenchmarkBase` three static
helpers every port needed, `require` and `requirementFailed` (Scala's message prefix, and a form
that keeps a timed loop free of string building) and `defineKernel` (define an emitted class and
instantiate it).

**The ids are identical.** `dev/varka_bench_ids.sh VarkaArithmeticBenchmark VarkaTimeBenchmark` on
the Java forms prints what 2 recorded from the Scala forms: 16 ids, 960 to 985, next 986; 31 ids,
0 to 30, next 31.

**Every port run once, against its committed file's names** (5), on the busy machine, with
`SPARK_GENERATE_BENCHMARK_FILES=1` and each file put back after: section titles, benchmark names
and case names, in order, identical to the committed file for `VarkaCodegenBenchmark` (2 cases),
`VarkaArithmeticBenchmark` (19), `VarkaLongLaneBenchmark` (64), `VarkaSharedPrefixBenchmark` (20),
`VarkaMethodSizeBenchmark` (64), `VarkaTimeBenchmark` (140) and `VarkaUnreadLocalsBenchmark` (32,
its allocation block's labels included); `VarkaVectorApiProbeBenchmark`, which commits no file, ran
its 12 cases to the end. Two differ, both for reasons in the committed files, not the ports:

* `VarkaEmissionBenchmark`: the run has a fourth section, "grouping by prediction", that the
  committed file lacks. The section was added on 30 September (`b8d6187d22a`) and the file was last
  regenerated on 29 September; its first three sections match.
* `VarkaWideKernelBenchmark`: the first five sections match (the loop-method counts in the labels
  included); the sixth stops at its own `require`: "sixty-four dates by field, 256 outputs fits one
  kernel: nothing to compare". The committed file, of 3 October, splits that shape 181 + 75. The
  Scala form fails the same way on today's emitter: its own `datesByField` and planning call, run as
  a throwaway Scala object on this branch's catalyst, print that the 256 outputs fit one kernel. So
  master's Scala class cannot finish a run since an emitter change after 3 October (VARKA-254's
  grouping change of 12 October is the likely one), and the class's pair cannot finish on either
  side until the shape is fixed. That is a change to the benchmark's subject, not to its language,
  and goes to a row of its own; the port keeps the Scala's shape and `require`. The seventh
  section (32 cases, loop-method counts in their labels) was not reached, so its labels are
  unchecked until then.

**What the run said about the harness.** `VarkaUnreadLocalsBenchmark`'s allocation block reads 0.0
bytes a batch past each kernel's own where the committed file reads 0.1 to 0.2, one run, which the
pair will confirm or not; and its "as emitted" rows
equalled its "stripped" ones (24,000 bytes a batch at 400 ladder entries, null-free), since the
elision became the default after the file was committed. The review fixed that (below).

**Also by the "only" filter**: `-Dvarka.bench.only="-i, ANSI"` on `VarkaArithmeticBenchmark` ran its
two unary-minus cases and skipped the two sections it emptied, which exercises `retainCases` and
`hasCases`.

**Correction to 3.1.** Scala's `assert` in `VarkaLongLaneBenchmark` became an
`IllegalStateException` with Scala's "assertion failed: " message, not an `AssertionError`: Spark's
checkstyle refuses `throw new ...Error(`.

**The review's fixes** (13 October 2026). A review of the pull request found fourteen things;
thirteen are fixed here, and the fourteenth, the wide-kernel benchmark's sixth section, is row
325's.

* **Spark's `Benchmarks` runner runs a Java benchmark.** It invoked every `main` it found as a
  static method, and a Java benchmark's is the instance method it inherits, so
  `.github/workflows/benchmark.yml` could not run any of the ported classes (3.4). It now makes
  an instance with the no-arg constructor when the `main` is not static. Proved by running
  `org.apache.spark.benchmark.Benchmarks org.apache.spark.sql.VarkaArithmeticBenchmark` under
  `-Dvarka.bench.dryRun=true` through catalyst's `Test/runMain`, the workflow's reflective path
  without the assembly `spark-submit` needs: before, `NullPointerException: Cannot invoke
  "Object.getClass()" because "obj" is null` at `Benchmarks.scala:128`; after, `Running
  org.apache.spark.sql.VarkaArithmeticBenchmark:` and the 16 ids, next 986.
* **The dry run and the case filter live in `VarkaBenchmarkBase`**: `dryRun()`, `only()`,
  `runCases`, `claimCaseId` and `reportCaseIds`, in place of the copies in the arithmetic and time
  benchmarks. `-Dvarka.bench.dryRun` accepts only `true` or `false`, in any case, as Scala's
  `toBoolean` did; `Boolean.parseBoolean` had read `1` as false and started the full run. And
  the base's `main` refuses either property while `SPARK_GENERATE_BENCHMARK_FILES=1`, before
  Spark's `main` opens, and so empties, the committed results file; the check had to precede
  that, so it is in `main`, not in `runCases`. The time benchmark's dry run returns after its
  ids, rather than filling and checking every rung of up to 8.4 million rows first. The ids are
  unchanged: 16, 960 to 985, next 986; 31, 0 to 30, next 31.
* **Helpers shared**: the timing settings most benchmarks use (five iterations, two-second
  warm-up, two-second minimum) are `VarkaBenchmark.Builder.timed()`, and `roots(n, entry)` and
  `seq(list)` are in the base; `VarkaCompileBenchmark` and `VarkaGroupingBenchmark` lose their own
  `seq`, which the base's now clashes with. `defineKernel` instantiates the class
  `defineGeneratedClass` returns instead of looking it up by name again. Smaller clarity fixes:
  the wide-kernel benchmark's destination count is `maxOutputs` (1,200, not `WIDEST`'s 400) and
  its first section's kernels are `(label, kernel)` records; the method-size benchmark's column
  is a record, not a two-element array read by position.

**Timed bodies that changed**, each because the work it timed was wrong, not to make it faster:

* **`VarkaUnreadLocalsBenchmark`, every arm.** The class prices the locals a loop method stores
  and never reads, by timing a kernel as emitted against the same kernel stripped of them. It
  emitted under `VarkaEmitOptions.DEFAULTS`, and elision has been the default since the file was
  committed, so the emitted class had nothing left to strip and every arm timed one class. Every
  arm now emits with `withElideUnreadLocals(false)`, the emitter as it was when the benchmark was
  written. The class bytes, emitted and then stripped of every kind: under the defaults, 1,739,686
  and 1,739,686 at 400 ladder entries (436,452 and 436,452 at 100; mixed family 276,435 at 100,
  548,375 at 200, unchanged by the strip); pinned, 1,784,262 and 1,744,886 at 400 (447,240 and
  437,752 at 100; mixed 286,390 and 277,624 at 100, 568,682 and 550,764 at 200). One full run
  of the class (busy machine, not written to a file) shows the arms apart again in the
  allocation block, as the committed file has them: at 400 ladder entries, null-free, 44,000
  bytes a batch as emitted against 24,000 stripped (committed: 40,084 and 24,000); with nulls
  52,040 against 28,000; the mixed family's eight lines equal the committed ones to the byte.
* **`VarkaVectorApiProbeBenchmark`, the six counting probes** of its second and third sections.
  Each returns a row count that the timed lambda threw away, so the JIT could drop the counting
  as dead code (the Scala's `() => Unit` did the same). The count is now added to a plain `long`
  field, published once to a volatile field after each benchmark runs, as in
  `VarkaCompileBenchmark`. The array-writing probes of the first section are unchanged: the array
  is their effect.
* **`VarkaCodegenBenchmark`, both cases.** The port made the sink a volatile field written every
  iteration; it is a plain field again, as the Scala's captured `var` was, published once after
  the run. The Varka case now calls `defineKernel`, which drops the `loadClass` lookup after the
  definition; the case keeps its committed name, "fused emit + define + load + instantiate".

**322a-2: the parity benchmark** (13 October 2026), its own pull request as 8.3 says, one commit
after 322a-1's merge (`25a1bae6893`). `VarkaEmitterParityBenchmark` is Java under its name and
package and its `.scala` is deleted: 2,191 lines of Scala to 2,526 of Java after the review, 15%
more. It uses 322a-1's helpers and adds none to the adapter: `timed()` on all seventeen benchmarks,
`runCases` on every one (no section called `run()` directly, so the dry run already reached every
section's setup), `claimCaseId` and `reportCaseIds` for its ids, `defineKernel` for every kernel,
the width ladder's included, `requirementFailed` for the status checks inside timed loops and
`roots` for the widest shape. One method per section over a record of the shared buffers, rather
than one 2,000-line `runBenchmarkSuite`; the per-section chunk walks are records' methods or local
lambdas, the tuples records (`Rung`, `TruncArms`, `Setting`, `LadderPoint`, `DivisionShape`).
Oddities kept as they were: "weekofyear (VARKA-37), null-free" is a case twice in its section, and
`nf2Data` is filled exactly as `nfData` is.

**The evidence, without a timed run** (the machine was busy). Both sides were run as a dry run,
`-Dvarka.bench.dryRun=true` through `catalyst/Test/runMain`, with core's `Benchmark` and
`BenchmarkBase` temporarily instrumented, uncommitted, to print every section title, every
benchmark's name with its rate denominator, minimum iterations, warm-up and minimum time, and every
case's name with its fixed iteration count, in order: the Scala form at `25a1bae6893` before its
deletion, the Java form on the branch. The instrumentation is three `println`s, one each at the top
of `Benchmark`'s constructor, of `Benchmark.addTimerCase` (which `addCase` goes through) and of
`BenchmarkBase.runBenchmark`, so any port can repeat it; it was reverted before the commit. The two
outputs are identical, 271 lines: 17 sections, 17 benchmarks (all 5 iterations, 2 s and 2 s), 235
cases, and the ids line, 125 ids, 0 to 984, next free 985; `dev/varka_bench_ids.sh
VarkaEmitterParityBenchmark` on the clean branch prints the same. Labels with computed parts match
too: the B2 ladder's loop-method counts read off the emitted classes and the width ladder's op
counts and loop-method bytecode sizes. The one check the benchmark makes at setup, that each of the
width ladder's six points emits its registered number of `IntVector` ops (`require(ops ==
expectedOps)`), ran and passed on both sides, since the dry run emits every kernel; the rest of its
checks are the per-batch status checks inside the timed loops, which only a timed run reaches. The
hand-written kernels (`DateVectorOps`, `ChronoVectorOps`, `ChronoScalarOps`) are engine code the
benchmark calls, unchanged.

Against the committed `-jdk25-results.txt` (last regenerated 20 September, `ee290a476f0`, its
labels renamed on 4 October): the same 17 section titles and 235 case names in the same order,
except twelve computed labels, identical between the two dry runs, that the emitter has moved
since the file was written. The B2 ladder's unshared and one-method arms now report fewer loop
methods (6 outputs unshared: 4 for 6; 8 outputs unshared: 6 for 8, one method: 2 for 1, ceiling
400: 2 for 1; 12 outputs unshared: 10 for 12, one method: 2 for 1, ceiling 400: 3 for 2), and the
width ladder's six loop methods are 11 bytes smaller each (258 to 1,960 bytes for 269 to 1,971).
So the next regeneration changes those twelve labels whichever language runs it, and the band
file, keyed by the old labels, does not cover those cases; the pair reads them within 5%.

**The review's fixes** (13 October 2026). A review of the pull request found ten things, all fixed
here. (1) The row count, the passes and the chunk size were `static final int`s, compile-time
constants that javac wrote into every timed loop as immediates, where the Scala read the object's
field and its local vals at run time; they are now plain `static int` fields, so no loop bound is a
constant the JIT is handed. `javap -c` before: `eachChunk`'s bounds read `ldc 1000000`, the
alignment walk's `bipush 20` and `ldc 1000000`, and the per-row `dayofweek` anchor's `ldc 1000000`,
57 `ldc 1000000` and 5 `sipush 4096` in the class in all; after: `getstatic numRows`, `getstatic
repeats` and `getstatic chunk` in each, and the literals left only in the static initialiser and
the alignment ladder's rung records (as the Scala's ladder tuples held them). (2) The trunc
section's format vectors are built inside its `try` and closed in a guarded `finally`, and their
builder closes a partial array on failure, as the next_day section's names already were; the Scala
form had the same leak, so this departs from it in setup only. (3) The guarded close catches what
Scala's `NonFatal` catches, not only runtime exceptions. (4) A declared type wherever the
right-hand side does not name it (`sql/varka/AGENTS.md`). (5) The null-count helpers' comments say
that a chunk walking the buffer can be one null off and why the kernel's choice of body is the
same either way. (6) `chain` builds on `chainOver`. (7) One name for each of the three values,
the per-section copies gone. (8) The scalar baseline keeps its own chunk walk, as the Scala's
`scalarChunked` was a loop of its own, and its comment says so; the runner styles stay as they
were, since unifying them would change how timed bodies reach their kernels. (9) The two
documents that described the trunc block's ids as a "tuple list". (10) This section's wrapping.
After the fixes the dry-run comparison was repeated: the 271 lines are again identical to the
Scala form's at `25a1bae6893`, the ids again 125 and next free 985, and the width ladder's check
again passes.

**The timed work, and every difference left.** Every timed body makes the Scala's calls with the
same arguments and builds the same per-batch arrays, the `take`-then-`map` destination arrays as
two arrays where the Scala built two. What differs is the language's own glue, and the pair is
read against this list:

1. **The chunk callbacks.** The chunk walks call a Java functional interface with primitive
   parameters (`ChunkBody`, `ScalarKernel`) where the Scala called an unspecialised `Function3` or
   `Function4`, which boxes its `Long` arguments unless escape analysis removes the boxes.
2. **The status checks.** Scala's `require(status == 0, s"...")` takes its message by name, so
   scalac allocated a thunk capturing `status` on every call, once per chunk; the Java tests the
   condition and allocates nothing unless it fails.
3. **The empty literal arrays.** One shared constant where `Array.empty[Int]`, mostly through a
   default argument, allocated one per call (per chunk in the chunked runners), as the arithmetic
   port did.
4. **The small loops.** The sequential chain and the widest shape's 64 passes are plain loops
   where the Scala's `for` over a `Range` ran a closure, and the chain's two locals replace a
   tuple per level; `Math.min` is called directly where `math.min` went through `scala.math`'s
   forwarder.
5. **How a timed body reaches its runner.** The Scala's runners were local functions, which
   scalac lifts into methods called directly with the captured values as arguments. In the Java
   they are methods of the year section's record (`chunked`, `chunkedTwo`, ...), static methods
   (the alignment walks, the predication call) or lambdas called through an interface (the
   next_day, trunc, B2 and epilogue runners, the DAG and grouping `run`), so some calls are
   interface calls where the Scala's were direct.
6. **How a body reads the buffers.** The Scala's closures captured each buffer; the Java lambdas
   capture one record of them and read a buffer through its accessor, a trusted final field.
7. **The loop bounds.** The row count, passes and chunk size are read from static fields, as the
   Scala read them from the object's field and its captured local vals; neither form gives the
   JIT a constant (fix 1 above).

If a chunked row reads faster than the A/A spread, items 1 to 3 are read first, as 6.1's
prediction 3 reads the wide-kernel harness; a row slower beyond it, item 5.

**Pending: the pairs** (6), on the quiet machine, none run yet:

```
dev/varka_bench_pair.sh catalyst VarkaEmissionBenchmark --base HEAD
dev/varka_bench_pair.sh core VarkaCodegenBenchmark
dev/varka_bench_pair.sh catalyst VarkaUnreadLocalsBenchmark
dev/varka_bench_pair.sh catalyst VarkaSharedPrefixBenchmark
dev/varka_bench_pair.sh catalyst VarkaLongLaneBenchmark
dev/varka_bench_pair.sh catalyst VarkaEmissionBenchmark
dev/varka_bench_pair.sh catalyst VarkaVectorApiProbeBenchmark
dev/varka_bench_pair.sh catalyst VarkaMethodSizeBenchmark
dev/varka_bench_pair.sh catalyst VarkaArithmeticBenchmark
dev/varka_bench_pair.sh catalyst VarkaTimeBenchmark
dev/varka_bench_pair.sh catalyst VarkaWideKernelBenchmark
dev/varka_bench_pair.sh catalyst VarkaEmitterParityBenchmark
```

The wide-kernel pair stops on both sides at the sixth section, as above, until that shape is
fixed. The parity pair is the longest: a full run of the class takes about twenty minutes a side,
so its five rounds take over three hours of the idle window. 6.1 is scored when the pairs are
in.

**322a-2's pair, run** (13 to 14 October 2026, 22:55 to 01:55 CEST, the owner away and no other
JVM on the machine; load 1.2 to 1.4 from the pair's own builds). `dev/varka_bench_pair.sh catalyst
VarkaEmitterParityBenchmark`, base `25a1bae6893` (the Scala form) against head `e17f656210a`, five
rounds a side, no early stop. Of 235 rows, 225 moved less than 10% by the median or the best;
2 are slower and 8 faster by 10% or more by both.

* **The two slower rows are hand-written per-row loops, not emitted kernels**: "per-row
  `LocalDate.of` make_date" at -15.1% by the median and -15.3% by the best, and "per-row
  `DateTimeUtils.truncDate` YEAR" at -10.4% and -11.1%. Both are the benchmark's references for the
  path Spark takes today, each a `while` loop around a call into `java.time` or `DateTimeUtils`.
  The Java loop makes the Scala's calls with the same arguments, and reads its destination (and,
  for truncDate, its input) through the shared buffers record's accessor where the Scala read a
  captured local (item 6 above). That alone does not explain them: the per-row `getWeekOfYear`
  and `LocalDate` year rows have the same shape and moved -4.1% and -1.8%. The cause is not
  established; the check that would settle it is the two loops' inlining and compiled code on both
  sides (`-XX:+PrintCompilation -XX:+UnlockDiagnosticVMOptions -XX:+PrintInlining`), on a quiet
  machine. No emitted kernel's row is among them, so no number the benchmark reports for Varka
  moves; what moves is two of its references to Spark's own path.
* **The eight faster rows are the small-chunk shapes**: the 63- and 64-row chunk arms with mixed
  nulls (dayofweek +11.4% and +16.6%, the depth-4 chain at 63 rows +14.7%, the 20-output epilogue
  ladder +14.0% to +30.8%, by the median). At 63 or 64 rows a chunk, the harness's per-chunk work
  is a large share of each call, and that is where items 1 to 3 differ: the Scala boxed its
  `Long` arguments through an unspecialised `Function3`, allocated a `require` message thunk and
  an empty literal array on every chunk, and the Java does none of these. The same arms at 4096
  rows, where the per-chunk work is a small share, moved under 2%. As prediction 3 said for the
  wide-kernel harness, these rows read the harness, not the kernel, and the dry run's identical
  names and ids (above) rule out a dropped body.

**6.1, for this class.** Prediction 4 **failed** for two rows: two rows are slower by 10% by both
the median and the best, the pair runner's gate. Prediction 2 held in the form it was written:
the rows that moved past 5% slower are the benchmark's own hand-written code, read here before
being called a port's penalty. No A/A run of this class exists yet to read the spread against; it
and the inlining check are owed, and either can run on a GitHub-hosted runner once the runner
pair lands.

**The inlining check** (14 October 2026). The per-row loops of both sides were run alone
(`-Dvarka.bench.only=per-row`, pinned to the fast cores) under `-XX:+PrintCompilation
-XX:+PrintInlining`, and the two slower loops under `-XX:+PrintAssembly`, the base built at
`25a1bae6893`. The machine was not quiet, so no timing is read. **Inlining does not differ.** Each
loop is compiled the same way on both sides (a C2 OSR compile, a deoptimization, a second OSR
compile, a normal C2 compile), and in every compile `LocalDate.of` with `LocalDate::create`,
`localDateToDays`, `truncDate` and the `MemorySegment` get and set chains are "inline (hot)" or
forced inline; the Java's `Buffers` accessors and its static forwarders inline too. Nothing is
"too big" on either side; the one refusal, `getWeekOfYear` "already compiled into a big method",
is on both. The compiled loops have the same shape (strip-mined, bounds and addresses hoisted,
the `LocalDate` scalar-replaced, 3208 against 3168 bytes for truncDate, 5064 against 5192 for
make_date). **What differs is register allocation**, the same in a second base run: the Java keeps
the segment bases in XMM registers and moves them back on every row (truncDate's store is `vmovq
%xmm11, %rbx; movq %rbx, %r11; movl %ecx, (%r11, %r8)` against the Scala's `movl %ecx, (%rbp,
%r8)`), and its loop safepoints hold 6 oops against 4 (truncDate) and 13 against 10 (make_date).
**Matching the Scala's source does not remove it.** Four variants of the two loops were compiled:
the buffers read into locals inside the closure; the bare segments captured, as the Scala did;
the pass count captured too, which gives the closure the Scala's exact signature; and the
module's methods called directly with the row count captured. Every one kept the XMM round trip
and 6 and 12 oops, so item 6 is not the cause and the source is unchanged. The cause stays open:
the register allocation is the only difference found, and nothing shows it costs 10% to 15%.

**The ten pairs, on GitHub-hosted runners** (14 October 2026). #803 (VARKA-255a) merged at 13:19
CEST, and at 13:21 the coordinator dispatched the block `VARKA-255.md` 9.1 lists under "After the
merge" as five runs on `vecbricks/varka`: the A/A of 6
([38555923693](https://github.com/vecbricks/varka/actions/runs/38555923693)), the nine other
classes of 322a-1 ([38555927657](https://github.com/vecbricks/varka/actions/runs/38555927657)),
row 325's two results files
([38555932989](https://github.com/vecbricks/varka/actions/runs/38555932989)) and its Emission pair
([38555937778](https://github.com/vecbricks/varka/actions/runs/38555937778)), and the parity pair
([38555942066](https://github.com/vecbricks/varka/actions/runs/38555942066)). All five completed;
the last job ended at 18:12 CEST. A pair ran at most five rounds, both sides on one machine, with
`--force` (no runner has a canary baseline), the base built in a worktree of its own. For 322a-1
the head is `refs/pull/780/head`, `5e2321c5951`, the ports after the review, and the base its
merge base with master, `ad1feaa347c` (#781), the Scala forms; for the parity class the head is
`f0975d9df75` and the base `8549423c12d` (#804); the A/A ran `5e2321c5951` against itself. The
pool handed out four CPU models. Each job's verdict, per-row table, CPU and probe are its summary,
every round's results file and provenance its artifact; the wall time below is the job's
`started_at` to `completed_at` in the API.

| class | rows | slower | faster | widest by median | widest by best | rounds | runner CPU | wall |
|---|---:|---:|---:|---:|---:|---:|---|---:|
| `VarkaEmissionBenchmark`, A/A (`base=HEAD`) | 28 | 0 | 0 | 9.9% | 16.2% | 5 | AMD EPYC 7763 | 0:51:07 |
| `VarkaCodegenBenchmark` | 2 | 0 | 0 | 5.2% | 4.9% | 5 | AMD EPYC 7763 | 0:47:36 |
| `VarkaUnreadLocalsBenchmark` | 32 | 0 | 0 | 79.8% | 10.9% | 5 | AMD EPYC 9V45 | 4:51:27 |
| `VarkaSharedPrefixBenchmark` | 20 | 0 | 0 | 9,505.3% | 17.0% | 5 | AMD EPYC 9V45 | 0:58:37 |
| `VarkaLongLaneBenchmark` | 64 | 0 | 0 | 67.8% | 9.2% | 5 | AMD EPYC 9V74 | 1:08:06 |
| `VarkaEmissionBenchmark` | 28 | 0 | 0 | 28.8% | 15.6% | 5 | AMD EPYC 7763 | 0:49:41 |
| `VarkaVectorApiProbeBenchmark` | 12 | 0 | 0 | 2.2% | 2.1% | 3 | AMD EPYC 7763 | 0:30:52 |
| `VarkaMethodSizeBenchmark` | 64 | 0 | 0 | 12,173.0% | 5.7% | 5 | AMD EPYC 9V45 | 1:22:59 |
| `VarkaArithmeticBenchmark` | 19 | 0 | 0 | 18.1% | 9.3% | 5 | AMD EPYC 9V45 | 0:39:10 |
| `VarkaTimeBenchmark` | 140 | 0 | 2 | 37.3% | 24.8% | 5 | Intel Xeon 6973P-C | 1:59:53 |
| `VarkaEmitterParityBenchmark` (#791) | 235 | 0 | 6 | 54.4% | 43.1% | 5 | AMD EPYC 7763 | 3:29:26 |
| `VarkaEmissionBenchmark` (#789, row 325) | 28 | 0 | 0 | 33.2% | 2.6% | 5 | AMD EPYC 7763 | 0:52:30 |

"Slower" and "faster" count the rows moved 10% or more by both the median and the best, the
pair's rule; the widest changes are either way. `VarkaVectorApiProbeBenchmark` stopped after three
rounds, every row within 3%. The nine-class run was a matrix of six jobs at a time, so
`VarkaMethodSizeBenchmark`, `VarkaArithmeticBenchmark` and `VarkaTimeBenchmark` started 31 to 50
minutes after the others. The Xeon job pinned itself (`taskset -c 0,1,2,3`: the regeneration
script's automatic pin found a cpufreq directory on that machine); the eleven others ran unpinned.

**The verdict for 322a: every ported class a runner can pair passes its pair**, prediction 4, on
every class. The wide-kernel pair was closed without one on 16 October 2026 (the last paragraph of
this section): its base cannot finish on any machine, and its single run on a runner took 2 h
55 min (below).

**The A/A spread, read first.** The Java side against itself: 28 rows, widest 9.9% by median and
16.2% by best, both on `[one emission] a column, copied`, a 14 to 32 ms emission whose best time
varied by that much over ten runs of identical code (bests of 14, 18, 19, 23 and 32 ms on one
side, 16, 19, 21, 22 and 23 on the other; the artifact's `base-*.txt` and `head-*.txt`). Every
other row is within 9.2% by median and 3.4% by best. The same row is the widest by median in the
two other Emission pairs on the 7763: -28.8% by median and +15.6% by best in 322a-1's, where it is
the widest by best too, and +33.2% by median and +2.1% by best in row 325's, whose widest best,
2.6%, is another row's. So it is read as the runner's spread, not the port's.

**The two rows the laptop pair found slower did not reproduce** ("322a-2's pair, run", above). On
the EPYC 7763, `per-row DateTimeUtils.truncDate YEAR` read -8.1% by median and -4.7% by best (base
bests 217 to 230 ms, head 227 to 248), the laptop's direction but under the gate; and `per-row
LocalDate.of make_date` read +13.3% by both, faster: the five base runs at 194 or 195 ms, the
five head runs at 172, with a standard deviation of 0 ms inside every run. The laptop, an AMD
Ryzen AI 9 HX PRO 370 (Zen 5) pinned to its fast cores, had read them -10.4%/-11.1% and
-15.1%/-15.3%. The inlining check, made on the laptop, found the two sides differing in register
allocation only. The two measurements differ in more than the CPU: the laptop ran its own JDK
build (25.0.4.1, Ubuntu), pinned, and the runner Zulu 25.0.4+7, unpinned. So what the record
supports is that the laptop's finding does not reproduce on an EPYC 7763 under that build, 15%
one way there and 13% the other here, and that the cause, still open, depends on the machine or
the JDK build or both; no number the benchmark reports for Varka moves on either machine.

**The parity pair's six faster rows** are the 20-output epilogue ladder at 63- and 64-row chunks
(the shared arms +33.4%/+35.7% and +54.4%/+43.1%, base bests 511 to 644 ms against head 357 to
470; the unshared +14.1%/+13.6% and +14.9%/+12.1%), the make_date reference above, and `scalar
year, magic multiply (may auto-vectorize), null-free` (+15.0%/+15.4%; 137 or 138 ms in every base
run, 119 or 120 in every head run). The ladder rows are the ones the laptop pair read faster for
the harness's per-chunk work (items 1 to 3 of the list above), wider here. Its two rows marked
"the median's alone" include `[dayofweek over 1000000 rows] lanewise DIV, null-free` at -20.7% by
median and -0.9% by best: a row of 4 or 5 ms, where the harness's integer milliseconds make one
step a fifth. `VarkaTimeBenchmark`'s two faster rows are emitted forms over 8,388,608 rows, past
L3, `the split itself ... both halves stored wide` (+37.3%/+11.5%) and `minute ... narrowed
store, half species` (+20.1%/+13.8%), rows of 9 to 19 ms where the same grain and the memory
system set the spread; its twelve rows marked "the median's alone" are rows of the same kinds.
The hand-written magic-multiply rows of prediction 2 moved between -13.9% and +24.8% by one
measure or the other, none by 10% by both: the +24.8% by best is a sub-millisecond row whose
nanoseconds a row print to one decimal, and the -13.9% by best (`hour: seconds of day, int32
lanes, hand-written magic multiply`, in L3) came with -8.6% by median.

**The odd medians, from the rows.** Four classes report a widest change by median of 68% to
12,173% while their widest by best stays within 17%. The per-round files say why; it is three
things, none of them the port.

* **A fork lottery on 2 ms rows.** `VarkaMethodSizeBenchmark`'s `[4 make_date outputs ...] single
  epilogue` rows (chunk 1024 and 1031, null-free) ran at 2 ms in some JVM forks and at 267 to 284
  ms in others, on both sides: the base drew three slow forks of five and the head two, so the
  medians sit in different modes (+12,081.1% and +12,173.0%) while the bests agree (+4.1% and
  -3.8%). `VarkaSharedPrefixBenchmark`'s `fused ceiling 400: 6 groups, prefix per group` is the
  same: 3 ms or 267 to 462 ms, the base three slow forks and the head two, +9,505.3% by median and
  +3.1% by best. The record traces this bimodality to JDK-8380195 (`skills/benchmarking.md`, "A
  benchmark number is reproducible within a run and not between runs"); three slow forks of five,
  the draw `VARKA-255.md` 9.1 reckoned five rounds would make rare, happened twice in one
  afternoon.
* **Sub-millisecond rows.** Seven of `VarkaLongLaneBenchmark`'s eight marked rows are the int64
  arms of `[262144 rows - both arms in L3]`, each under a millisecond (a best time of 0 ms, 0.2
  to 0.9 ns a row). Three of the base's five runs read those rows at between a quarter and two
  thirds of the other runs' rate and none of the head's did, so their medians read +46.2% to
  +67.8% and their bests -4.4% to -8.8%. The eighth is the table's int32 `column + literal, ANSI
  checked` arm, +12.0% by median and +4.9% by best; the other seven int32 arms moved under 6.1%
  by either measure, and the class's widest best, -9.2%, is a past-L3 row, `a comparison, as a
  selection, int64 lanes`, at +0.1% by median.
* **A class whose five iterations end before C2 does.** `VarkaUnreadLocalsBenchmark`'s three
  marked rows are the null-free 400-entry arms, a class of 1.7 MB. The base's bests read 192 to
  207 ms in every run; the head's read 193 to 203 in one or two runs and 322 to 4,800 in the
  others (four of five runs of `as emitted`, three of `reloads stripped` and `as emitted, again`),
  runs whose best iteration never reached the compiled speed, so the medians fell 38% to 80% and
  the bests held within 1%. The committed laptop file shows the class's nature, an average
  iteration of 3.5 to 4 s against a best of 0.3. Why the head drew those runs and the base did not
  is not established.

**`VarkaUnreadLocalsBenchmark`'s `as emitted` arms time a different class on each side**, by
design: 322a-1's review made every arm emit with `withElideUnreadLocals(false)` ("Timed bodies
that changed", above), where the base's Scala form emitted under the defaults and had nothing to
strip. So the head's `as emitted` and `as emitted, again` rows with nulls read -10.9% and -8.3% by
best (the first: bests of 232 to 238 ms against 207 to 219), and the first's allocation line
52,040 bytes a batch against the base's 28,000 to 39,760, which is the price of the unread locals
the benchmark exists to measure, and what the committed laptop file reads too (338 ms as emitted
against 309 stripped; 52,040 bytes against 28,000). The `stripped` arms, the closest to the class
the base timed, moved -3.5% to +2.9% by best.

**Row 325's pair and its two runner files.** The Emission pair of #789 (`16373fc283f` against
its merge base `453944cbfb1`, #806; the last row of the table) passed on the 7763 as it had on the
laptop, 28 rows, none 10% apart by both measures. The results-file run regenerated
`VarkaWideKernelBenchmark` on an EPYC 7763 (512:256 = 1.00; 2 h 55 min 41 s, 13:21 to 16:16 CEST)
and `VarkaEmissionBenchmark` on an EPYC 9V74 (512:256 = 0.91; 13 min 25 s), each with the
provenance file 255a writes. Both came back as artifacts and stay there: the committed files are
the laptop's (AMD Ryzen AI 9 HX PRO 370 pinned to its fast cores, the canary passing, JDK 25.0.4.1
Ubuntu; the runners ran Zulu 25.0.4+7 unpinned), every section measured on one machine in one run,
and a results file is regenerated on the machine that measured it, so that its rows stay
comparable with their own history and with the other files the plans quote beside them
(`VARKA-255.md` 7, risk 5); a runner file also brings no 128-bit companion. What the runner files
record, read with `dev/varka_bench_diff.py <committed> <runner>`: the Emission rows are all slower
on the 9V74 than on the laptop, by 36% to 44% on the emission rows and 50% to 54% on the
emission-and-definition rows, single-threaded compiler work on a lower clock; the wide-kernel
rows are all slower on the 7763, the kernel rows by 57% to 86% (most by 62% to 71%, the
22-cheap-tails arms by 68% to 86%) and the section's emission rows by 53% to 79%, and two rows
apart: `64 cheap tails, predicted (3 loop methods)`, null-free and with nulls, read 97% and 98%
slower, a fourteenth and a twentieth of their neighbours' rate (`weights`, `planned` and
`weights, again` at 4 loop methods, 62% to 64% slower) where the laptop has the four arms within
15% of each other - a slow mode of that kernel on that machine, which a regeneration there would
need a pair to read. And four labels differ by a byte count: `800 entries, split driver, the
loop's two builds (3451877 bytes)` is `(3451876 bytes)` on the runner, `800 entries, several
kernels, the loop's classes alone (3469249 bytes in 2 classes)` is `3469247`, and the 1200-entry
pair `5110577` and `5208235` are `5110576` and `5208233`: one byte less per class, so the diff
tool cannot match those four rows. The two runs differ in machine and in JDK build; which moved
the byte is not established, and no other label moved. A label that embeds a byte count is a diff
key that moves with whatever moves a byte; the lesson is in `skills/benchmarking.md`, and moving
the count out of the label is for the next change to that benchmark, not a row of its own.

**6.1, for the ten.** Prediction 4 held on every class a runner paired. Prediction 1 held for
`VarkaCodegenBenchmark` (4.9% by best) and failed on the letter for the other five:
`VarkaMethodSizeBenchmark` 5.7%, on one row; `VarkaEmissionBenchmark` 15.6%, on the row the A/A
read at 16.2%; `VarkaArithmeticBenchmark` 9.3%, on one row; `VarkaLongLaneBenchmark` 5.4% to
9.2%, on six of the sub-millisecond int64 arms, three int32 arms of the same table and the
past-L3 comparison above; `VarkaSharedPrefixBenchmark` 17.0%, on `fused ceiling 200: 20 groups,
prefix computed once`, where one base run read 17 ms and the other nine runs 21 or 22. A best
moves on one lucky run as a median on three slow ones, which is why the verdict reads both.
Prediction 2 held for the two classes it names: none of `VarkaTimeBenchmark`'s hand-written
magic-multiply rows moved by both measures, and `VarkaVectorApiProbeBenchmark` settled within 3%
after three rounds; the parity class's own hand-written references moved by both measures on both
machines, two slower on the laptop and two faster on the 7763, as above. Prediction 3 held for the
`stripped` arms and does not apply to the `as emitted` arms the review changed; the wide-kernel
half was not measured as a pair, for the reason in the last paragraph. Prediction 5 held, the ids identical (above). Prediction 6 held for
322a-1's ten classes, a quarter more Java than Scala (above), and failed for the parity class, 15%
more, under a fifth. The A/A spread the predictions were to be read against is 16.2% by best
for this class on a runner, where the plan had the laptop's one A/A, 4.1% by median
(`VARKA-256.md` 9), in mind.

**The tenth pair, closed without a pair (16 October 2026, owner's decision A of `VARKA-255.md`
9.3).** `VarkaWideKernelBenchmark`'s pair of 322a (the Java port against its Scala form at the
merge base) and row 325's second pair (the split fix against its merge base) cannot run on any
machine, the laptop included: both bases stop at the sixth section's `require` ("fits one kernel:
nothing to compare"), the bug 325 fixed, so a pair, which needs both sides to finish, has no
base. What exists in place of the pair is recorded here and in `VARKA-325`'s row: the dry-run ids
and the names of every section, benchmark and case identical between the Scala and Java forms;
the first five sections matching the committed laptop file (above); and the class regenerated
at the fixed head on an idle laptop and on a runner, every row within a few percent of the
3 October file on the laptop. That is a comparison against a committed file, not a base and a
head alternated on one machine, and the wide-kernel half of prediction 3 therefore stays
unmeasured as a pair. A section-subset path that would let a base run only the sections it can
finish is the alternative `VARKA-255.md` 9.3 describes; it was not built.
