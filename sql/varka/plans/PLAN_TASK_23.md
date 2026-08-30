# Task 23: Java-first migration, and the task-18 debt

Milestone 3's last task, from its section 3 row: migrate the Varka Scala
components that nothing forces to stay Scala - `VarkaShapeCache` first, as the
natural vehicle for the debt register's emit-options rework
(`PLAN_MILESTONE_3.md` section 10) - then an assessment of
`VarkaExpressionCompiler`, and a recorded boundary naming what stays Scala and
why. The gate is behaviour-preserving: every Varka suite green at both vector
widths with committed numbers unchanged, scalastyle and `dev/lint-java` clean.

This file is written before the work rather than during it, because the
exploration behind it changed the task's shape twice and those findings are
worth recording whether or not the implementation follows immediately. The
outcome section is added when the work lands.

## 1. The boundary, measured rather than assumed

The Java-first rule (`sql/varka/AGENTS.md`) names three surfaces that force
Scala: `SparkPlan` subclasses, the Catalyst rule and expression matching, and
ScalaTest suites. Measuring the tree against that rule gives a smaller
migration than the charter's wording suggests, and the honest number is worth
stating first: **474 Scala lines in two files.**

A second fact sets the standard the migration has to hold. Every existing Java
file in `.../codegen/varka/` is pure JDK - not one imports a Spark Scala class,
and `VarkaLoopEmitter`'s only Spark import is its own sibling IR file. That is
what lets the Java side be read and tested without Spark, and the migration
should keep the line rather than blur it.

| File | Lines | Verdict |
|---|---|---|
| `VarkaGeneratedClassLoader.scala` | 70 | **Migrate, first.** Imports only `ConcurrentHashMap`, extends `ClassLoader`, no Scala surface. A Java twin already exists - `sql/varka/engine/.../execution/VarkaClassLoader.java` - and this file's own doc calls itself a deliberate duplicate whose changes "belong in the other". Porting turns a behavioural duplicate into a literal one. |
| `VarkaShapeCache.scala` | 404 | **Migrate the core, keep a thin Scala facade** (section 3). Its Spark dependencies are configuration and logging, not logic. |
| `VarkaExpressionCompiler.scala` | 660 | **Stays Scala.** 35 `case` arms pattern-matching Catalyst case classes across 12 `match` blocks, 95 uses of `Seq`/`Option`/`mutable`, 10 `private[sql]` declarations. In Java every arm becomes an `instanceof` cascade over Scala case classes with `CollectionConverters` at each boundary, and `private[sql]` has no Java equivalent. The charter asks for an assessment here, not a migration; this is it. |
| `VarkaFusionReport.scala` | 103 | **Assessed and declined** - the one genuine judgement call. Plain string rendering, so it looks migratable, but every signature takes `Seq[NamedExpression]` / `Seq[Attribute]` / `Expression` and it pattern-matches `VarkaOutputSpec`. A veneer over forced-Scala types; Java buys nothing and adds a conversion layer. |
| `VarkaKernelEvaluator.scala` | 1062 | **Stays Scala**, and the file already argues why at `:42-49`: the task-21 review recorded a deliberate decision that `VarkaExecMetrics` stay a case class rather than a Java record, because every construction site is forced-Scala code leaning on named arguments and defaults over seven same-typed fields, where a record's positional constructor is a silent-swap hazard. That precedent is cited, not re-derived. |
| `VarkaColumnarRule`, `VarkaProjectExec`, `VarkaColumnarToRowExec`, `VarkaFilterExec` | 1118 | **Stay Scala.** `ColumnarRule` and `SparkPlan` subclasses - the rule's named exceptions. |
| Every `*Suite.scala` | 5619 | **Stays Scala.** ScalaTest. |

## 2. Decisions, and who made them

* **The shape cache splits rather than translates** - a pure-JDK Java core plus
  a thin Scala facade - decided by the project owner over the two
  alternatives put to him (one all-Java file calling Scala objects through
  `SQLConf$.MODULE$`, or dropping the migration and landing only the debt
  rework). The deciding evidence was that the file's Spark dependencies are
  all configuration and logging: `SparkEnv`, `Logging`, `SQLConf`,
  `StaticSQLConf`, `NonFateSharingCache`, `SparkStringUtils`. A facade absorbs
  every one of them, and it makes the "deterministic executor-side sizing"
  debt disappear by construction rather than by patch.
* **`VarkaGeneratedClassLoader` is in scope**, though the charter row does not
  name it - the owner's call, on the argument that it is the cleanest instance
  of the rule in the tree and that its Java twin already exists.
* **The register's `renderLineMap` item is swept too, with a new shallow
  rendering** rather than the substitution the register implies - the owner's
  call once exploration showed the obvious fix is wrong (section 4).

## 3. Design: the shape cache

**Java core** (pure JDK plus Guava, in the package's house style - records,
sealed interfaces where they fit, exhaustive switches, explanatory javadoc):
`VarkaShapeKey`, `VarkaLoaderShapeKey`, `VarkaShapeEntry`, `VarkaShapeLookup`,
the cache implementation with both Guava caches, the removal listener that
releases evicted loaders, the `LongAdder` counters, `shapeHash` /
`classNameFor` / `sourceFileFor`, and the execution side table. Logging through
`SparkLoggerFactory` - checkstyle bans `org.slf4j` directly, and this also
avoids the Scala `Logging` trait.

**Scala facade**: capacity resolution, and the `Seq`/`List` conversions that
keep every call site outside this file unchanged.

Two findings make the split cheap. `NonFateSharingCache` is usable from Java as
`new NonFateSharingCache<>(guavaCache)` - `private[spark]` is public in
bytecode and the primary constructor is public - so the loading discipline
survives untouched and the four-overload companion `apply` is bypassed. And
every callback in the file is already a Java functional interface
(`RemovalListener`, two `Callable`s), so no Scala-function to Java-function
adaptation is needed anywhere.

### The three non-mechanical points

Everything else is direct substitution. These are not:

1. **`VarkaShapeKey.outputs: Seq[VarkaVectorIR]` becomes
   `List<VarkaVectorIR>`.** The largest ripple, and it touches the correctness
   contract, since key equality is what makes a wrong hit a wrong answer. Use
   `List.copyOf` in a compact constructor so no caller can mutate a live key.
   Two construction sites gain `new`, records having no `apply`:
   `VarkaKernelEvaluator.scala:212` and the suite's `keyOf` helper at
   `VarkaShapeCacheSuite.scala:58-59`.
2. **`throw e.getCause` in the Guava unwrap** (`:220-226`) meets Java's checked
   exception rules. It must stay a rethrow, never a wrap: the file's own doc
   (`:145-148`) records that the cause has to reach the evaluator's
   `isCatchable` test as itself, which is what keeps a fatal error or an
   interrupt from being counted as a kernel failure.
3. **`var emitted` captured by the loading `Callable`** (`:212-219`) needs
   effectively-final capture.

`executionsFor` returns `Seq[String]` today and three Scala test sites use it
as a Scala collection, so the Java core returns `List<String>` and the facade
converts. Scala's auto-application rule for Java nullary methods means
`entry.className`, `entry.sourceFile`, `lookup.hit` and `hitCount` keep
compiling unchanged as long as the component names hold.

### The debt items the migration carries

* **Deterministic sizing.** Confirmed at `:350`: the lazy singleton runs
  `configuredMaxEntries()` once, on whichever thread first touches any entry
  point, so whether `SQLConf.get` returns a task's `ReadOnlySQLConf` or the
  driver fallback depends on who won the race - two identically configured
  executors can size differently. The split fixes it structurally: capacity
  becomes a constructor argument the facade resolves at one defined point. The
  boundary to document is sharper than the register states: a builder-set
  static reaches an executor only when the builder also created the
  `SparkContext`; on a session attached to an existing context,
  `SQLConf.mergeNonStaticSQLConfigs` drops static keys, so the value is
  silently lost on the driver and never propagates at all.
* **`recordExecution` onto `asMap().compute`** (`:302-329`): the `while` loop
  with its `getIfPresent` / `putIfAbsent` reinstate dance collapses into one
  atomic remapping. This is a pair of edits, not one - the per-set
  `synchronized` at `:311` also goes, since `compute` holds the bin lock, so
  `executionsFor` (`:249-250`), which relies on that same monitor today, must
  take its defensive copy inside a `computeIfPresent`.
* **The kernel constructor** is resolved reflectively per call in `newKernel()`
  (`:90`), once per task at `VarkaKernelEvaluator.scala:543`. Resolve once in
  `emit`, store it in the entry.
* **`className` and `sourceFile`** are pure functions of `shapeHash`
  (`:373-377`), so they become derived accessors and stop being stored. A
  record with a `byte[] classBytes` component gets identity equality for that
  component - harmless, since entries are only ever compared with `eq`/`ne`,
  but it deserves a comment, because a record looks like it has value
  equality.

## 4. Design: emit options

### What exists today

Five `private static volatile` hook fields in `VarkaLoopEmitter.java` -
`misdescribeAddForTesting` (196), `disableCseForTesting` (203),
`divFloorModForTesting` (210), `digitSumFloorModForTesting` (218),
`groupBudgetForTesting` (227) - each with a package-private setter that bumps
an `AtomicLong testHookGeneration` (237-268); two package-private queries,
`currentTestHookGeneration` (266) and `anyTestHookSet` (279); a public
re-export shim `VarkaEmitterTestSupport` in the catalyst test jar so `sql/core`
suites can reach the setters; and a reflection-based completeness test at
`VarkaShapeCacheSuite:314`.

The cache guards it with three reads: a gate that throws for every lookup while
any hook is set (`VarkaShapeCache.scala:204`), a generation snapshot before the
emit walk (`:275`), and a re-check after it (`:282`).

**Three races live in that protocol - the register names two.**

1. A hook set between the gate (`:204`) and the snapshot (`:275`) - an
   unbounded window, since the caller may block on another task's in-flight
   load of the same key - is already set when the snapshot is taken, so the
   re-check passes and the poisoned bytes are cached under the plain key.
2. The gate is JVM-wide: while any suite holds a hook, every unrelated
   concurrent query throws instead of simply emitting uncached.
   `VarkaShapeCacheSuite:216-220` asserts exactly that.
3. Every write bumps the counter, resets included, so one suite *clearing* its
   hook spuriously fails another thread's in-flight emit. This one is not in
   the register; the exploration for this task found it.

### The replacement

A `VarkaEmitOptions` record (Java, pure JDK, in the emitter's package) passed
explicitly into `VarkaLoopEmitter.emit` and folded into the cache key:

* `int groupBudget` - the one genuine tuning knob, default `GROUP_BUDGET = 16`.
* `boolean cse` - default true.
* `FloorMod7 floorMod7` - an enum `MAGIC | DIV | DIGIT_SUM`, replacing two
  independent booleans that can both be set today, an illegal state the record
  removes by construction.
* `boolean misdescribeAdd` - the one pure fault injector.

Every hook field, every setter, the generation counter, both queries, the gate,
the snapshot, the re-check, the four re-export methods and the reflection
enforcement test then delete. None of the three races can be expressed: options
travel as a value on the call, so there is nothing to write concurrently and
nothing to snapshot.

Two design points that come from the code rather than the idea:

* **The side table must not collide.** `executions` is keyed on `shapeHash`
  alone (`:190-194`) while the map is keyed on the full key, so options joining
  the key but not the hash would merge two variants' execution identities.
  Options reach `shapeHash` too.
* **Default options must hash to what they hash today.** The committed hashes
  (`586434f9b9739c40`, `612c94d132690dc2`) name the emitted classes, which are
  baked into the bytes and quoted in telemetry. So `shapeHash` renders the
  options only when they differ from the defaults: production hashes, class
  names and telemetry stay identical bit for bit, and only the variants a suite
  asks for get their own identity. That is what keeps "behaviour-preserving"
  literally true rather than approximately.

Two consequences to handle rather than discover. `fitsBudgets` (`:641-654`),
the compiler's eligibility mirror, does not consult `groupBudgetForTesting`
today, so a budget override already changes emission without changing
eligibility; once the budget is a record field, `fitsBudgets` takes the record
and the asymmetry closes. And the `emit` overload pair (`:421-424`, `:441-443`)
with nullable `sourceFile` and `planFragment` collapses into one entry point;
both strings stay outside the key, each already being a function of the hash.

### The line map

The register's last item is not in the shape cache at all: `renderLineMap`
lives in `VarkaLoopEmitter.java:1458-1472`, already Java. It renders IR nodes
through `Record.toString`, whose format no JDK promises - and so does the line
above it, `"outputs=" + outputs` at `:469`.

`canonical` is not a drop-in replacement: it recurses, so each line would carry
a whole subtree instead of one node, lengthening every entry and changing the
line map's decoding contract. The fix is a shallow rendering,
`canonicalShallow(node)`, added beside `canonical` in `VarkaVectorIR.java` - an
exhaustive switch over the same sealed hierarchy, rendering node kind plus
scalar fields with children referenced by index - pinned by its own committed
value the way `canonical` is. Both emitter call sites move onto it.

What that changes: the emitted `VarkaDebugInfo` content, so those bytes differ.
What it does not change: the shape hash, which is computed from `canonical`
over the IR - which is why the two committed hashes stay a valid oracle for
everything else. The task-16 debug-surface tests need checking for anything
that pins the old rendering.

### The one open question

Two `sql/core` suites use a hook as a fault injector and depend on the JVM-wide
gate throwing to manufacture an emission failure: `VarkaProjectExecSuite:401`
and `VarkaFilterExecSuite:183`, both calling
`VarkaEmitterTestSupport.setDisableCse(true)` and relying on the refusal, as
their own comments say. Once options ride the key that emission succeeds and
both tests lose their injection. Settle the replacement before deleting the
gate: inject at the evaluator seam, where those suites already build plans
through test doubles, rather than adding an internal config entry that only
tests would read. It is the only part of the rework that is not mechanical.

## 5. Documentation sweeps

### 5.1 The four-lane record: split it, do not delete it

`PLAN_MILESTONE_3.md:224-226` records four-lane coverage as "local only, via
`-XX:MaxVectorSize=16`", with a real aarch64 runner "dependent on runner
availability". The runner now exists - `build_and_test.yml:1088`, a matrix over
`ubuntu-latest` and `ubuntu-24.04-arm` - and ran green on PR #51. But the
record should not simply be struck, because the job runs `./build/mvn -f
sql/varka/engine/pom.xml install`: **the engine module alone**, five JUnit
classes, each twice (host width, then `-XX:MaxVectorSize=16` with
`-Dvarka.expected.int.lanes=4`). The catalyst and sql/core jobs that run
`VarkaLoopEmitterSuite`, `VarkaShapeCacheSuite` and `VarkaDifferentialSuite`
are `runs-on: ubuntu-latest`, x86_64 only, and build the engine with
`-Dmaven.test.skip=true`.

So the accurate statement is that the **hand-written kernels are covered on
real ARM hardware, and the emitted loops are not** - they still rely on
capped-width simulation on x86. Three sites say otherwise today:

* `PLAN_MILESTONE_3.md:224-226` - wrong for the kernels, right for the emitter.
* `ISSUES.md:594-599` - the "Suggested order" item 9 still lists the aarch64
  runner as outstanding, contradicting items 1 and 3 of its own list and the
  finding's own `Status: FIXED`.
* `ISSUES.md:77-79` - finding 1's caveat, "no `IntVector` path has actually
  executed at 4 lanes", is now false for the engine kernels.

One more found in passing: `sql/varka/engine/pom.xml:159-160` still asserts
"Every CI runner is x86_64 with 8 or 16 int lanes", now false for the job it
configures.

### 5.2 The debt register

`AGENTS.md` requires a swept entry be rewritten in the past tense with what the
sweep found, not deleted. The bullet spans two files and six items; this task
sweeps all six, and the rewrite should say what the sweep found that the
register did not know: the third race, the sharper builder-set-static
boundary, and that the line-map item needed a new rendering rather than a
substitution.

## 6. Verification

* `build/sbt catalyst/Test/compile sql/Test/compile`, then
  `catalyst/testOnly *Varka*` at the preferred width **and** under
  `-XX:MaxVectorSize=16`, and `sql/testOnly *Varka*`.
* `build/sbt catalyst/doc` - not optional. `AGENTS.md` records that Scala
  2.13's typechecker reports "illegal cyclic reference" on
  `java.lang.classfile` symbols and that the Maven scaladoc pass fails on it;
  this is the local reproduction, and it is the gate a Java file called from
  Scala can trip.
* `dev/lint-java`: checkstyle at severity `error`, nothing suppressing this
  package - 100 columns, mandatory ASF header, newline at end of file, no
  trailing whitespace, no tabs, no unused imports. `org.slf4j` is banned in
  favour of `SparkLogger`/`SparkLoggerFactory`; `com.google.common.cache` is
  not on the ban list, so the existing cache imports port over untouched.
* scalastyle on catalyst and sql, main and test; the 100-character and ASCII
  scans.
* **The behaviour-preservation proof is the committed hashes.**
  `586434f9b9739c40` (`VarkaShapeCacheSuite:293`) and `612c94d132690dc2`
  (`:311`) must come out byte-identical. They are SHA-256 over
  `VarkaVectorIR.canonical`, they name every emitted class, and those names are
  baked into the bytes and quoted in telemetry. If they hold, no committed
  number can have moved - which is why no benchmark is run and no performance
  claim is written anywhere.
* `VarkaGeneratedClassLoaderSuite`'s weak-reference Metaspace proof green
  against the Java loader.
* `./build/mvn -f sql/varka/engine/pom.xml install`, since the migrated
  loader's twin lives there and the two are meant to stay identical.

## 7. Sequencing

Four commits, each independently reviewable and green on its own:

1. `VarkaGeneratedClassLoader` to Java. Smallest, no design content, and it
   proves the Java-first mechanics - header, checkstyle, the scaladoc gate -
   before anything subtle depends on them.
2. The shape-cache split, carrying the sizing, `asMap().compute`,
   cached-constructor and name-from-hash items with it.
3. Emit options, deleting the hook stack, the gate, the generation counter, the
   re-export shim and the enforcement suite, and settling the two fault
   injection suites.
4. `canonicalShallow` and its pinning test, with both emitter call sites moved
   onto it - kept separate because it is the only commit that changes emitted
   bytes.

Then the records: this file's outcome section, the debt-register rewrite, the
four-lane sweep, and `PLAN_MILESTONE_3.md`'s task-23 row marked DONE, which
closes milestone 3.

## 8. Risks, ranked

1. **A wrong cache hit is a wrong answer.** The key's equality is the contract
   and the migration changes its representation. The oracle is the two
   committed hashes; `List.copyOf` in a compact constructor closes the aliasing
   hole a mutable component would open.
2. **The Guava unwrap must rethrow the cause as itself**, or a fatal error or
   an interrupt starts being counted as a kernel failure - a regression in the
   fallback contract that no existing test would catch, because the suites
   inject ordinary exceptions.
3. **Two suites lose their fault injector** when the gate goes. Settle the
   replacement before deleting it, not after.
4. **The line map's `Record.toString`** looks like a one-line fix and is not.
5. **Scaladoc, not scalac, is the gate** that fails on class-file types, so
   `catalyst/doc` runs before the PR rather than after CI says so.

## 9. Explicitly out of task 23

`VarkaExpressionCompiler`, `VarkaFusionReport`, `VarkaKernelEvaluator` and the
four exec nodes stay Scala, each with its reason recorded in section 1 - that
record is a deliverable of this task, not a deferral of it. No new config
entry. No new expression, type, lane width or IR node beyond the shallow
rendering section 4 names. No benchmark run and no performance claim: the task
moves no numbers, and section 6's committed hashes are how that is proven
rather than asserted.
