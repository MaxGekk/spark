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
