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
name contains the string, and `hasCases()`. The dry-run and `only` logic stays in each of the
three benchmarks that use it, as in the Scala, about fifteen lines each.

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
* **Spark's own Scala benchmarks**, and `sql/varka/bench`'s surface harness.

### 3.5 Registered op counts

None move; no emitter code changes.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/src/test/java/org/apache/spark/sql/VarkaBenchmark.java` | `retainCases`, `hasCases` |
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

**322a implemented, pairs pending** (13 October 2026). The row stays Planned.

**What was built.** 322a-1 as 8.2 says, in one commit after this plan: the ten classes of 4.1 are
Java under their names and packages, their `.scala` files deleted, 2,510 lines of Scala to 3,137
of Java (a quarter more), the parity benchmark left for 322a-2. The adapter gained more than 2 listed:
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
  and goes to a row of its own; the port keeps the Scala's shape and `require`.

**What the run said about the harness.** `VarkaUnreadLocalsBenchmark`'s allocation block reads 0.0
bytes a batch past each kernel's own where the committed file reads 0.1 to 0.2, so the Java
harness allocates nothing per batch where the Scala's allocated a little; and its "as emitted" rows
now equal its "stripped" ones (24,000 bytes a batch at 400 ladder entries, null-free), since the
elision became the default after the file was committed.

**Correction to 3.1.** Scala's `assert` in `VarkaLongLaneBenchmark` became an
`IllegalStateException` with Scala's "assertion failed: " message, not an `AssertionError`: Spark's
checkstyle refuses `throw new ...Error(`.

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
```

The last stops on both sides at the sixth section, as above, until that shape is fixed. 6.1 is
scored when the pairs are in.
