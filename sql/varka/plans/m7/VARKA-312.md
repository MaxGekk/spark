# VARKA-312: A Varka test suite can be JUnit 5, its tests named as ScalaTest names them

## 1. Where this came from

Milestone 7 moves Varka's test code to Java (rows 310, 311, 320 and 321 are done). The suites
themselves cannot follow, because everything a Varka suite runs under is a ScalaTest mixin:

- `VarkaMatrixTests`: the option matrix's skip list (`sql/varka/matrix/skips.tsv`), the
  `PinsDefaults` cancellation, each test's fused-batch record, and the memory sanitizer's per-suite
  allocator check;
- `VarkaTestWatchdog`: the per-test cap that ends a hung test by name;
- `VarkaOwnJvm`: the marker that runs a suite in a child JVM.

On top of that, `dev/varka_matrix.sh`, which the gate's wide and narrow steps and CI's
`varka-scoped` jobs run through, finds suites by grepping Scala sources and starts
`org.scalatest.tools.Runner`. So `sql/varka/AGENTS.md` cannot yet say that a new suite whose
helpers are Java is written in Java (PLAN.md 2.3, "The tests' helpers to Java").

The owner added the row on 12 October 2026, and on the same day added the requirement on names:
a JUnit test reads as a ScalaTest one does. Each test carries its sentence (`@DisplayName("a
reduction that fails differently is not smaller")`, not `reductionThatFailsDifferently()`), and
sbt's run, the gate's logs, the matrix's reports and the skip list show and match that sentence.
On 13 October, after VARKA-320 merged, the owner asked for this plan.

## 2. The admission check, done

Four probes, in a scratch worktree at master `d7a5231d197`, all removed afterwards. The probe
suite had four tests: one passing, one failing, one aborting by assumption, and one without a
display name.

1. **What sbt prints today.** Spark's sbt build runs JUnit through `jupiter-interface` 0.17.0
   with `-v -a` (`project/SparkBuild.scala`). It prints `Test <class>#<method>() started` and, on
   failure, `Test <class>.<method> failed: ...`, always with the method name. With
   `--display-mode=tree` it prints the display names, but marks every test with `+` whatever its
   outcome. Neither reads as ScalaTest. Without `-v` it still prints the `started` lines.
2. **A listener prints ScalaTest's lines under sbt.** A JUnit Platform `TestExecutionListener`
   registered in `META-INF/services/org.junit.platform.launcher.TestExecutionListener` is picked
   up by `jupiter-interface`'s launcher with no change to the build. Printing on each test's end,
   it gave, inside sbt's output:

       VarkaJUnitNameProbe:
       - pins the defaults' structure; cancelled under a configuration !!! CANCELED !!!
         Assumption failed: not run under checkIntOverflow=false
       - a reduction that fails differently is not smaller
       - aMethodWithoutDisplayName()
       - a planted bug fails its test with the smaller case in the message *** FAILED ***
         output 0 row 7 differs (want 12, got 13)
       Tests: succeeded 2, failed 1, canceled 1, ignored 0, pending 0

   `jupiter-interface`'s own `started` and `failed` lines stay beside these.
3. **The same listener outside sbt.** The catalyst test classpath holds
   `junit-platform-launcher` 6.0.0 beside the engine and Jupiter 6.0.3, and no
   `junit-platform-reporting`. A plain `java -cp` main that builds a discovery request for one
   class and calls `LauncherFactory.create().execute(...)`, as the matrix's JVMs would, printed
   the same lines. The listener is auto-registered there too, and launcher 6.0.0 runs the 6.0.3
   engine.
4. **A cancel from an extension is a cancel.** An `InvocationInterceptor` that skips the method
   and throws opentest4j's `TestAbortedException` is reported ABORTED, printed `!!! CANCELED !!!`
   with its message. The interceptor read both keys the skip list matches on: the test's display
   name and the class's simple name.

What the probes settle: neither a custom launcher inside sbt nor a change to Spark's build is
needed for the names. One listener serves sbt and the matrix's JVMs alike. `PinsDefaults` and a
skip-list entry can cancel a JUnit test the way they cancel a ScalaTest one. What they would have
rejected: a design that relies on `jupiter-interface`'s output, which names methods in every mode
that marks outcomes, or on `junit-platform-reporting`'s XML, which is not on the classpath (and
writes the method name too).

Three facts from reading the consumers, which fix the formats:

- `dev/varka_matrix_report.py` reads, per JVM, the last `Tests: succeeded N, failed N, canceled
  N` line, the last `Suites: completed N, aborted N` line (a JVM without one counts as one
  abort), and each `*** FAILED ***` or `*** ABORTED ***` line with up to six indented lines after
  it.
- `dev/varka_suite_shards.py` weighs suites by the `time` attribute of each `testsuite` element
  in the JUnit XML under a run, keyed by the `name` attribute's last dotted part.
- `VarkaOwnJvm.parseReport` reads each `testcase` element's `name` and its `failure`, `error`
  or `skipped` child, and `dev/varka_matrix.sh` greps one suite's XML for `<skipped`.

## 3. The design

### 3.1 A Java core, ScalaTest and JUnit faces over it

The harness's logic moves to Java, in catalyst's test tree beside the checks 311 ported. The
ScalaTest traits become thin layers over it, as `VarkaKernelCheck` became over
`VarkaKernelChecks`. A JUnit extension is a second face over the same core:

- **`VarkaMatrixRuns` (Java)** takes the matrix's logic out of `VarkaMatrix`: the configuration
  and its parse, the configurations' list, the skip list's parse, `expectedFailure`, the
  fused-batch report, and `around`. `around` today calls ScalaTest's `cancel` and `fail`; the
  Java form takes the two as factories (`String -> RuntimeException`) and passes a cancel from the
  body through. The ScalaTest face passes `TestCanceledException` and `TestFailedException`, the
  JUnit face opentest4j's `TestAbortedException` and `AssertionFailedError`. `VarkaMatrix` stays
  as a Scala object of forwarders where Scala reads it (`VarkaMatrix.base`, `config`,
  `PinsDefaults`, `fusedBatches`), so no suite changes.
- **`VarkaWatchdog` (Java)** is `VarkaTestWatchdog.guard`, which needs nothing from ScalaTest. Its
  clock and effects stay parameters, so `VarkaTestWatchdogSuite` keeps testing it without waiting.
- **`VarkaChildJvm` (Java)** is `VarkaOwnJvm`'s child machinery: the command line copied from
  the parent, the budget, the killer thread, the copied output, and the report's parse. It
  starts a given main class with given arguments, and the two faces pass their own runners.
- **`VarkaSuiteReport` (Java)** writes and reads the JUnit XML both kinds of suite produce:
  `TEST-<fqcn>.xml`, a `testsuite` with `name`, `tests`, `failures`, `errors`, `skipped` and
  `time`, and one `testcase` per test named by its display name.

**The JUnit face.** A meta-annotation `@VarkaSuite` on a JUnit class registers one extension,
`VarkaSuiteExtension`. Like the traits, it has two parameters: `watchdogMinutes` (default the
property's, `0` for none, as a fuzzer that outlasts its cap mixes in `VarkaMatrixTests` alone)
and `ownJvm`. The extension composes as the traits do, matrix outermost and watchdog inside:

- `interceptTestMethod`: under a configuration, a test tagged `@Tag(PinsDefaults)` is cancelled
  without running, with the ScalaTest form's message. It throws `TestAbortedException` and does
  not use an `ExecutionCondition`, which would report the test ignored. Otherwise the body runs
  as `around(guard(proceed))`.
- `beforeAll` and `afterAll`: the memory sanitizer's allocator check, as `VarkaMatrixTests.run`
  does it.
- With `ownJvm`, in the parent JVM: the first test of the class runs the class in a child through
  `VarkaChildJvm` with the runner below, and every test takes its result from the child's XML
  instead of running. In the child (`-Dvarka.ownJvm=true`) the tests run directly.

Every key the skip list and the fused-batch report use is the suite's simple name and the test's
display name, so a JUnit test's skip-list line is written exactly as a ScalaTest one's.

**The listener.** `VarkaSuiteListener`, registered through `META-INF/services` in catalyst's
test resources, prints ScalaTest's lines for the tests of `@VarkaSuite` classes only:

- `<Suite>:` when a class starts;
- `- <name>` when a test passes;
- `- <name> *** FAILED ***`, then its message on indented lines, when one fails;
- `- <name> !!! CANCELED !!!`, then its reason, when one aborts;
- `- <name> !!! IGNORED !!!` when one is disabled.

Since the listener is global to the module's JUnit runs, it stays silent for every other test,
and Spark's own Java suites print what they print today. It also accumulates the counts and the
XML of each class, for the runner.

**The runner.** `VarkaSuiteRunner` (Java) is a main taking the arguments the scripts give
ScalaTest's Runner: `-u <dir>`, `-s <fqcn>`... and ScalaTest's output flags, which it passes
on. It sorts the named classes by reflection: those that are an `org.scalatest.Suite`, and
`@VarkaSuite` classes. In one JVM it runs the first set through
`org.scalatest.tools.Runner.run` with the same flags, then the second through the Launcher with
the listener, writing `TEST-<fqcn>.xml` to the same directory. Last it prints one combined
`Tests: succeeded ...` line and one `Suites: completed N, aborted M` line. ScalaTest's Runner
prints its own two lines first. The report reads the last of each, so the combined lines are
what it reads, and for a shard with no JUnit class they are ScalaTest's counts again. To have
ScalaTest's counts, the runner attaches a reporter (`-C`) that counts test events, so it does not
parse its own output.

**The scripts.** `dev/varka_matrix.sh` starts `VarkaSuiteRunner` where it started
`org.scalatest.tools.Runner`, with the same arguments. Its `module_suites` also scans the
module's `src/test/java` for `@VarkaSuite` classes named `Varka...Suite`. The gate's wide,
narrow and quiet steps and CI's `varka-scoped` jobs go through that script, so they need no
change of their own. `dev/varka_matrix_report.py` and `dev/varka_suite_shards.py` read the same
lines and the same XML as before and need none either.

**The rule.** The last commit adds to `sql/varka/AGENTS.md`'s Java rules: a new catalyst suite
whose helpers are Java is a JUnit 5 class under `@VarkaSuite`, each test with a `@DisplayName`
sentence. An existing suite moves only when a task touches it, as for other code.

### 3.2 What is deliberately unchanged

- **The sql/core suites.** Their base is Spark's `SharedSparkSession` and `QueryTest` under
  `VarkaSharedSessions`, ScalaTest classes that are Spark's own test surface, and nothing in
  this row gives a JUnit suite a session. The runner and the scripts handle a JUnit class in
  either module, so a later row that adds a session extension only adds that.
- **Spark's build.** `jupiter-interface`'s options stay as Spark sets them. Its `started` lines,
  which name the method, stay beside the listener's in sbt's output. The matrix, the gate's suite
  steps and CI's scoped jobs run through the runner, where those lines are not printed.
- **The gate's sweep step's name filter.** `sbt testOnly ... -- -z opt-in` filters ScalaTest
  suites. No JUnit suite has an opt-in test yet. One that does gets a JUnit tag the step selects,
  in the task that writes it.
- **Every existing ScalaTest suite**, which keeps its traits. Their behaviour is unchanged, which
  the first pull request proves.

### 3.3 Registered op counts

None: no emitted code changes.

## 4. Files

| file | what |
|---|---|
| `VarkaMatrixRuns.java`, `VarkaWatchdog.java`, `VarkaChildJvm.java`, `VarkaSuiteReport.java` | the core, from `VarkaMatrix`, `VarkaTestWatchdog` and `VarkaOwnJvm` |
| `VarkaMatrix.scala`, `VarkaMatrixTests.scala`, `VarkaTestWatchdog.scala`, `VarkaOwnJvm.scala` | thin ScalaTest layers over it |
| `VarkaSuite.java`, `VarkaSuiteExtension.java` | the annotation and the extension |
| `VarkaSuiteListener.java`, `META-INF/services/...TestExecutionListener` | the names, the counts, the XML |
| `VarkaSuiteRunner.java` | the combined main |
| `dev/varka_matrix.sh` | the runner, and discovery in `src/test/java` |
| `VarkaMethodNamesSuite.java` (from `.scala`) | the example port |
| `VarkaSuiteHarnessSuite.java` | the JUnit face's own tests |
| `sql/varka/AGENTS.md`, `sql/varka/skills/` | the rule, and how to write and run a JUnit suite |

## 5. Tests, and what each is for

1. **The core under the ScalaTest face:** `VarkaMatrixSuite` and `VarkaTestWatchdogSuite` pass
   unchanged, and every Varka suite in catalyst gives the same outcomes before and after the first
   pull request, at the defaults and under three configurations (two emit options and one batch
   size), read from the matrix's report. That is the proof that the move changed nothing.
2. **`VarkaSuiteHarnessSuite` (JUnit, itself under `@VarkaSuite`)**, through the core's seams with
   entries and a configuration passed in, as `VarkaMatrixSuite` does for the Scala face:
   - a `fails` entry cancels its test with the entry's reason;
   - a stale entry fails it;
   - a `PinsDefaults` test is cancelled under a configuration and runs at the defaults;
   - the fused-batch report gets the display name;
   - the watchdog's interrupt and halt reach a JUnit test's thread, with a fake clock;
   - an `ownJvm` class's test runs in the child and reports there, and a failure in the child
     fails its test in the parent with the child's message.
   The planted skip entry the row asks for lives here, not in `skips.tsv`, where an entry whose
   test passes is stale and fails.
3. **The example, `VarkaMethodNamesSuite` in Java.** Its five tests pass at the defaults and under
   every configuration where the Scala form passed. Its names, as sbt prints them and as the
   matrix's report prints them, are recorded beside the Scala form's in section 9, line for line.
4. **The runner on a mixed shard.** A matrix run of a shard holding the JUnit example beside
   ScalaTest suites gives one `Tests:` line that is the sum, one `Suites:` line, XML for each, and
   shard weights read from both.
5. **Silence for others.** One of Spark's own JUnit suites in catalyst prints the same lines with
   the listener on the classpath as without it.
6. **The gate** passes on each pull request.

## 6. The measurement

No benchmark. The runner's own cost is measured: a matrix run at the defaults over catalyst's
suites, before and after, in total time and per JVM.

### 6.1 Predictions, registered before the run

1. The first pull request moves no test outcome: identical counts per suite and per configuration
   in the matrix's report, and identical `fused.tsv`.
2. The example's sbt lines read as the ScalaTest form's: the same five sentences, `- ` before
   each, in the same order. `jupiter-interface`'s five `started` lines, with method names, are
   the only lines that differ.
3. The runner adds under two seconds per JVM, the Launcher's discovery of a handful of classes,
   to a catalyst matrix run at the defaults, whose time is measured before and after.
4. The core is smaller than the code it replaces, once the four Scala files are thin: about 650
   lines of Scala today become about 150 of Scala over about 600 of Java.

## 7. Risks

1. **The listener is global** to every JUnit run in catalyst's tests. Gated on `@VarkaSuite`, it
   prints nothing else; test 5 holds that.
2. **Launcher and engine versions differ** (6.0.0 and 6.0.3). Probe 3 ran them together. A Spark
   upgrade that moves one and not the other would show in test 4.
3. **The report reads the last summary line.** A runner that printed ScalaTest's line after its
   own would undercount, and test 4 catches it. So would a child's output copied into an
   `ownJvm` parent, which is why the parent's own lines come last, as they do for ScalaTest today.
4. **sbt runs JUnit and ScalaTest suites in one forked JVM.** The fused-batch report is written
   under a lock in the core, as `VarkaMatrix` writes it now.
5. **A name not unique in a class.** Two JUnit tests with one display name would share a skip-list
   key, which ScalaTest forbids and JUnit allows. The extension fails a class whose display names
   repeat.

## 8. Sequencing

Two pull requests.

1. **312a, the core.** The four Java classes and the Scala traits over them; no suite changes.
   Proved by test 1 and the gate.
2. **312b, JUnit.**
   - the annotation, the extension, the listener and the runner;
   - the script's discovery and runner;
   - `VarkaSuiteHarnessSuite`;
   - the example port;
   - then, in its last commit, the AGENTS.md rule and the skills note.
   Proved by tests 2 to 6 and the gate, and section 9 written.

## 9. Outcome

Two pull requests, as section 8 planned: 312a (#778, 13 October 2026) moved the harness's logic to
the Java core; 312b, this section's subject, added the JUnit face. Everything below 9.1 is 312b's,
measured on the laptop on 13 October 2026.

### 9.1 What 312a recorded

312a's pull request carried its proof: the matrix on master and on the branch, the defaults plus
`cse=false`, `division=DOUBLE_DIV` and `arrowBatchSize=3` over both modules' 83 Varka suites, gave
identical counts per configuration (1,051 passed and 31 cancelled at the defaults) and identical
per-test outcomes from the JUnit reports (9,329 lines each side). The fused-batch reports were
identical except for 8 counts in `VarkaGoldenCorpusSuite`, by one to three batches either way,
which that suite's counts do between two runs of the same code. One behaviour changed on purpose:
a test of an own-JVM suite whose child timed out or left no report now fails with that reason,
where a Scala `Map` default that `get` never reads had made it "reported no result".

### 9.2 What was built

- `VarkaSuite` (the annotation, 78 lines), `VarkaSuiteExtension` (256), `VarkaSuiteListener`
  (257), `VarkaSuiteRunner` (165), `VarkaSuiteReport.write` (+86), the `META-INF/services`
  registration, `VarkaSuiteHarnessSuite` (429, 11 tests), and `VarkaMethodNamesSuite` in Java
  (154 lines from 100 of Scala). `dev/varka_matrix.sh` starts the runner and finds
  `@VarkaSuite` classes in `src/test/java`.
- One change to the core: `VarkaMatrixRuns.Outcomes`'s factories return `Throwable`, not
  `RuntimeException`, because JUnit's failure, opentest4j's `AssertionFailedError`, is an `Error`.
  The ScalaTest face's lambdas compile unchanged.
- Where the plan was silent:
  - The extension's per-test logic has a seam, `runTest(config, entries, report, guard, ...)`,
    because `VarkaMatrixRuns.CONFIG` is read once at class load; the harness suite tests the
    composition through it, as `VarkaMatrixSuite` tests `around`'s.
  - The runner attaches a listener of its own with the report directory and turns the
    `META-INF/services` registration off for its Launcher; otherwise every line would print twice
    and the service-loaded instance would have no directory for the XML.
  - A class-level failure (a repeated name, Arrow memory left) is a failed class container, which
    the listener prints `Exception encountered when attempting to run a suite with class name: X
    *** ABORTED ***` and counts in `Suites: aborted`; its XML carries `errors="1"`.
  - sbt discovers nested static classes with `@Test` methods and Spark's catalyst job would run
    the harness's fixtures alone, so each fixture passes alone and its failures are planted by a
    flag only while a harness test runs it.
  - `VarkaSuiteHarnessSuite` joins `VarkaMatrixSuite` in the matrix's exclusions; the gate's
    defaults run includes it.

### 9.3 The tests

**Test 2, `VarkaSuiteHarnessSuite`:** 11 tests pass under sbt and in the matrix's runner: a `fails`
entry cancels with its reason (and not under another configuration), a stale entry fails while the
body's own cancel passes through, `PinsDefaults` cancels under `cse=false` without running the body
and runs at the defaults, the fused-batch report line is `VarkaSuiteHarnessSuite<TAB><display
name><TAB>-1`, the watchdog's interrupt ends a sleeping test and its halt stub fires on a spinning
one (the dump naming the suite), an own-JVM fixture's test runs in a child started through the
runner, a failure, a cancel and a missing name in a child's XML become an `AssertionFailedError`
with the child's message, a cancel and a failure in the parent, repeated display names are found
(a method without one counted by JUnit's default name), and the listener prints, counts
(`succeeded 1, failed 1, canceled 1, ignored 1`, one suite completed, one aborted) and writes XML
for planted outcomes, and prints nothing for a class not under the annotation.

**Test 3, the example.** sbt's `catalyst/testOnly *VarkaMethodNamesSuite`, the Scala form on
master:

    [info] VarkaMethodNamesSuite:
    [info] - the builders give the names the emitted classes have (23 milliseconds)
    [info] - the readers tell every kind of method apart, on both sides (3 milliseconds)
    [info] - a group's methods by column are the cost table's four kinds (2 milliseconds)
    [info] - a group method's group is read off its name, and every other method has none (2 ms)
    [info] - the scripts outside Java that mirror the names still name what the builders produce
             (2 milliseconds)
    [info] Run completed in 11 seconds, 20 milliseconds.
    [info] Total number of tests run: 5
    [info] Suites: completed 1, aborted 0
    [info] Tests: succeeded 5, failed 0, canceled 0, ignored 0, pending 0
    [info] All tests passed.

The Java form on the branch (the listener's lines reach sbt's output before `jupiter-interface`'s,
which sbt logs through its own channel):

    VarkaMethodNamesSuite:
    - the builders give the names the emitted classes have
    - a group's methods by column are the cost table's four kinds
    - a group method's group is read off its name, and every other method has none
    - the scripts outside Java that mirror the names still name what the builders produce
    - the readers tell every kind of method apart, on both sides
    [info] Test run started (JUnit Jupiter)
    [info] Test org.apache.spark...varka.VarkaMethodNamesSuite#builders() started
    [info] Test org.apache.spark...varka.VarkaMethodNamesSuite#groupMethodsByColumn() started
    [info] Test org.apache.spark...varka.VarkaMethodNamesSuite#groupOfName() started
    [info] Test org.apache.spark...varka.VarkaMethodNamesSuite#mirrors() started
    [info] Test org.apache.spark...varka.VarkaMethodNamesSuite#readers() started
    [info] Test run finished: 0 failed, 0 ignored, 5 total, 0.009s
    [info] Passed: Total 5, Failed 0, Errors 0, Passed 5

The matrix's run log and report, with a planted `fails` entry for the third test under `cse=false`
(the test passes, so the entry is stale and fails it; the committed list was restored after):

    Scala: - a group's methods by column are the cost table's four kinds *** FAILED ***
             stale entry in sql/varka/matrix/skips.tsv: 'a group's methods by column are the cost
             table's four kinds' of VarkaMethodNamesSuite passes under cse=false; remove the line
             (VarkaMatrix.scala:101)
    Java:  - a group's methods by column are the cost table's four kinds *** FAILED ***
             stale entry in sql/varka/matrix/skips.tsv: (the same message, without the source
             position)

The report's lines (`cse=false: - a group's ... *** FAILED ***` and the indented message) and the
five `fused.tsv` keys are the same strings in both forms, and the JUnit XML names the same five
test cases. Under every configuration (`--all`: the defaults and 50) both forms pass 5 of 5; the
two reports' rows are identical but for the seconds, 11 to 16 per JVM for the Scala form and 2 to
4 for the Java one, which starts no `SparkFunSuite`.

**Test 4, a mixed shard.** One matrix JVM at the defaults with `VarkaMatrixSuite` and
`VarkaTestWatchdogSuite` (ScalaTest) beside `VarkaMethodNamesSuite` and `VarkaSuiteHarnessSuite`
(JUnit): ScalaTest's Runner printed `Suites: completed 2` and `Tests: succeeded 10`; the own-JVM
fixture's child printed its own `Suites: completed 1` and `Tests: succeeded 1` copied into the log;
the runner's lines came last, `Suites: completed 4, aborted 0` and `Tests: succeeded 26, failed 0,
canceled 0, ignored 0, pending 0`, which the report read (26 passed, exit 0). Four
`TEST-<class>.xml` files, and `dev/varka_suite_shards.py 2` read weights from both kinds
(`VarkaMatrixSuite` 10.3 s, `VarkaSuiteHarnessSuite` 1.3 s, `VarkaTestWatchdogSuite` 0.33 s,
`VarkaMethodNamesSuite` 0.025 s) and put the first alone on one JVM.

**Test 5, silence for others.** `catalyst/testOnly XXH64Suite HiveHasherSuite` (Spark's own JUnit
suites) printed the same 18 lines on master and on the branch, timings aside, and no line the
listener would print.

**The full catalyst run.** `dev/varka_matrix.sh --defaults --module catalyst --split 3 -j 3`, 52
suites, on master and on the branch (the harness suite skipped on the branch, so the shards held
the same names): 616 passed and 40 cancelled on both, and the 811 test-case outcomes in the JUnit
XML identical, name for name.

### 9.4 The predictions

1. **312a moves no test outcome: right** (9.1), with the fused-batch counts of one suite that
   varies between runs of the same code.
2. **The example's sbt lines read as the ScalaTest form's: right on the sentences and the `- `,
   wrong on the order, and the plan undercounted the differences.** The five sentences are the
   same strings. JUnit runs a class's methods in its own order, here by method name, not in
   declaration order as ScalaTest does, so the second test printed last. The listener's lines go
   to standard output and are not sbt's log lines, so they carry no `[info]` prefix and no
   duration; and sbt's JUnit summary (`Test run finished`, `Passed: Total 5`) replaces ScalaTest's
   four summary lines, besides the five `started` lines the plan named. The skills note says the
   order is JUnit's; no `@Order` was added, since the order is no key anywhere.
3. **The runner adds under two seconds per JVM: right.** Seven interleaved rounds of one JVM per
   arm over the same classpath, the JVM's wall time less ScalaTest's own `Run completed in` time
   (the suite's body varies by seconds): ScalaTest's Runner alone a median 443 ms outside its run,
   `VarkaSuiteRunner` over the same suite 469 ms (+26 ms, within the rounds' spread of 376 to
   663), and the runner with one JUnit class added, the Launcher's start and five tests, 852 ms
   (+383 ms). The full catalyst run could not measure it: master's ran on an idle machine (load
   0.4) in 134, 177 and 471 s per JVM, the branch's beside Spark's golden-file suite from another
   worktree (load 5 to 9) in 221, 244 and 467 s; the longest JVM, which sets the run's time, moved
   by 4 s, and the shorter two by the load.
4. **The core smaller than the code it replaced: wrong.** The four Scala files were 653 lines and
   are 354; the Java core is 759 (with 312b's `write`, 845). The Scala layers kept their doc
   comments and every forwarder the suites call, and the Java carries the doc the Scala had.

### 9.5 The gate, and what is left

The whole gate (`dev/varka_gate.sh`, 13 October 2026, 14:40 to 15:01 CEST, beside Spark's
golden-file suite from another worktree, the load average up to 38) passed every step in 1,258 s:
`wide` and `narrow` 1,087 passed and 32 cancelled over both modules (the harness and the Java
example among them, under the runner), `quiet`, `sweep`, `doc`, `bench`, `lint` (`dev/lint-java`,
`dev/scalastyle`, `dev/varka_errorprone.sh`) and `quotes`.

Left open, outside this row:

- **The Scala face still applies the matrix to an own-JVM child's answer in the parent.**
  `VarkaMatrixTests` wraps `VarkaOwnJvm`'s answer, so a stale skip-list entry that the child fails
  is cancelled again in the parent, and the parent appends its own `fused.tsv` line (-1 or 0) over
  the child's real count for the same key. 9.6 fixed both for the JUnit face; the Scala face is a
  candidate row (no own-JVM suite is in sql/core, where batches are counted, so only the stale
  entry matters today).
- **An own-JVM child's report directory is left under the JVM's temporary directory**, inside the
  matrix's run directory, so `dev/varka_suite_shards.py` also reads it (a weight under
  `VarkaSuiteHarnessSuite$OwnJvmFixture`, the max of two times for a Scala suite's own name). It
  predates this row; deleting the directory after the parse would close it.
- **A sql/core suite cannot be JUnit yet**: no extension gives a JUnit class Spark's shared
  session (3.2), and the gate's sweep step selects ScalaTest's `opt-in` tests by name only.

### 9.6 The code review's fixes

A review of the pull request (13 October 2026) found nine faults by reading; each was confirmed
against the code, none was a false positive, and each was fixed in this row with a test in
`VarkaSuiteHarnessSuite` (now 17 tests) that failed on the code before the fix and passes after.
Where the test needs a new seam, the seam was added first with the old behaviour, so the "before"
run is the old code through it.

1. **A class-level abort in an own-JVM child passed in the parent.** The listener writes the abort
   as an `error` directly under `testsuite`, which `read()` never looked at, and `VarkaChildJvm`
   ignored the child's exit status once the report held any test. Now
   `VarkaSuiteReport.abortOf` reads that element and the parent fails every test with it, and a
   child that exits non-zero though its report holds no failure fails every test too (both also
   turn cancels into failures, so an all-cancelled class cannot hide an abort). Test: a stand-in
   child main writes one passing test, with and without an abort, and exits 1; before, both came
   back `Passed`.
2. **The parent re-applied the skip list to the child's verdict.** A stale entry the child failed
   came back to the parent as a failure of a listed test, which `around` cancelled. The parent of
   an own-JVM class now takes the child's answer bare: no `PinsDefaults`, no `around`, no
   watchdog; it also no longer writes its own `fused.tsv` line. Test: through `runTest`'s new
   `answeredByChild` flag, a child's stale failure stays an `AssertionFailedError`, a child's
   cancel keeps its message and no report line is written; before, a `TestAbortedException`.
3. **`@ParameterizedTest`, `@RepeatedTest` and `@TestFactory` bypassed the harness**, since
   Jupiter runs them through interceptor methods the extension does not override. Refused, not
   wrapped: their invocations are named `[1] ...`, poor skip-list keys, and a dynamic test has no
   method for the duplicate-name check to see. `checkShape`, called from `beforeAll`, fails the
   class ("a @VarkaSuite test must be a plain @Test"), which the listener reports as an aborted
   suite.
4. **The child's budget ignored `@VarkaSuite(watchdogMinutes)`.** `VarkaChildJvm.run` takes a
   budget; a JUnit class passes its own cap less a minute, none for 0, and the Scala face keeps
   the JVM-wide rule. Test: `budgetMinutes(0)` is empty, `(2)` is 1, `(30)` is 29; before, 9 for
   all. No behavioural test, which would need a child running for minutes.
5. **A `@Nested` class failed every test** ("registers VarkaSuiteExtension without @VarkaSuite"),
   and would have been keyed by the inner class's name. Refused by `checkShape` on the outer class.
6. **The runner moved every `-s` after the other flags**, so ScalaTest applied a `-z` to no suite
   (all three of `VarkaTestWatchdogSuite`'s tests ran), and it ignored `-z`/`-t` for JUnit
   classes. ScalaTest now gets argv in its order less the JUnit classes' `-s` and their filters,
   which a `PostDiscoveryFilter` applies to the display names. Test: `-s VarkaTestWatchdogSuite
   -z ... -s ListenerFixture -z ... -t ...` gives XML with 1 and 2 tests; before, 3 and 4.
7. **The child's cancel reason was replaced by "cancelled in its own JVM".** `read()` now keeps the
   `skipped` element's message, falling back to that text for ScalaTest's `<skipped/>`, which has
   none. Test: the existing child-report test asserts the reason.
8. **A `Varka...Suite` taking `@VarkaSuite` from a base class was never found by
   `dev/varka_matrix.sh`**, which greps each file for the annotation. `checkShape` requires the
   annotation on the class itself, so such a class aborts loudly under sbt instead of silently
   missing the matrix; `@Inherited` stays so that the extension still reaches it to say so.
9. **A class container JUnit reported ABORTED** (an assumption in `@BeforeAll`) counted as a
   completed suite with no tests. The listener now treats FAILED and ABORTED alike, as ScalaTest
   treats an exception in `beforeAll`: `*** ABORTED ***`, `Suites: aborted 1`, `errors="1"`. Test:
   a fixture whose `@BeforeAll` assumption fails while planted; before, `completed 1`.

Tests 3, 5 and 8 are one test of `checkShape` over three abstract fixtures; before, the first
assertion failed. After the fixes the four harness and example suites pass under sbt (32 tests),
and one matrix JVM at the defaults over `VarkaMatrixSuite`, `VarkaTestWatchdogSuite`,
`VarkaMethodNamesSuite` and `VarkaSuiteHarnessSuite` printed `Suites: completed 4, aborted 0` and
`Tests: succeeded 32` last, which the report read (32 passed, exit 0).
