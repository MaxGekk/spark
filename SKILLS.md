# Varka Project Lessons Learned

Working notes from implementing the Varka MVP (Spark columnar execution). These are
reusable debugging lessons and project-specific gotchas. Not instructions; see
`AGENTS.md` for the workflow rules.

## Classpath Shadowing (the stub trap)

- Spark modules declare `test->test` project dependencies (e.g. `sql/core` on
  `sql/catalyst`). That puts the *compiled test classes directory* of the dependency
  on the dependent module's test classpath, ahead of jars in `~/.m2`.
- A test-only source that redefines a class at the same FQCN as a dependency silently
  shadows the real jar: the code "runs", produces no exception, and reinstalling the
  jar changes nothing.
- Varka kept a no-op stub `DateVectorOps` in `sql/catalyst/src/test/java` (Task 5) so
  catalyst tests could resolve the kernel owner FQCN without the engine jar. It
  shadowed the real kernel on the sql/core test classpath and made the kernel path
  appear to write nothing.
- Fix used: delete the stub, add the engine jar as a test-scope dep in the module's
  `pom.xml` (mirroring sql/core), delete the stale compiled class under `target/`.
- Detection: instrument the layer you *think* is running. If the debug prints never
  appear, you are executing a different class than the source you edited. Look for
  stale copies: `find target -name '*.class'`, search every jar/dir on the classpath
  for the FQCN.

## Buffer-Reuse Aliasing (UnsafeProjection)

- `UnsafeProjection` (and `GenerateUnsafeProjection`) reuses a single output
  `UnsafeRow` buffer across calls. Materializing results into an `Array` without
  copying yields an array whose elements all alias the last row's buffer: every
  output shows the last value.
- The copy belongs to whoever *materializes*, not to the operator. Spark operators
  stream reused rows deliberately: `ColumnarToRowEvaluatorFactory` is
  `input.rowIterator().asScala.map(toUnsafe)`, with no copy. `VarkaColumnarToRowExec`
  copied per row on both its kernel and fallback paths until finding 9 removed it; that
  cost an `UnsafeRow` allocation plus a memcpy per row on the hot path and bought a
  guarantee the standard path never gave. (Earlier revisions of this file prescribed the
  copy, from when the evaluator materialized with `process(...).toArray`; it streams now.)
- `QueryExecution.toRdd`'s scaladoc states the contract and names `collect()` as "one of
  known bad usage" - `RDD.collect` is `iter.toArray` per partition, so it aliases. Use
  `Dataset.collect` (which serializes each row as it iterates), or, in a test that wants
  `InternalRow`s, `toRdd.map(_.copy()).collect()`.
- Not a Varka rule: collecting a plain row-engine query that way returns 2 distinct row
  objects for 5 rows, with wrong values. `SparkContext.hadoopRDD`'s scaladoc documents
  the same hazard for reused Hadoop `Writable`s.
- Corollary for tests: a suite that materializes rows can pass for the wrong reason while
  an operator copies. `VarkaDifferentialSuite` did, and only failed once the operator
  stopped - the copy had been masking an unsound `toRdd.collect()` in the test itself.

## Alias Unwrap Is Needed at Two Layers

- A projection list is `Alias(expr, name)` at the top level, and after
  `BindReferences` the bound expression is `Alias(BoundReference(...), ...)`.
- Any code matching projection expressions against concrete types must unwrap `Alias`
  twice: once on the unbound list (`eligibleOps`) and once on the bound list
  (`buildOutputPlan`). Missing either makes eligibility silently fail (kernel never
  runs) or the plan match return `None`.
- Symptom: with both missing, fallback covers it; the kernel path never executes. The
  rule test and the `numVarkaBatches` metric reveal it once rows are correct.

## Masked Bugs

- A fallback path that returns *wrong* results can hide an entirely untouched kernel
  path. Here the "kernel results" seen early on were actually the fallback (with the
  aliasing bug). The real kernel only ran once the Alias unwraps landed, which is
  also when the stub trap surfaced. Two independent bugs masked each other.

## Debugging Method: Progressive Isolation

1. Verify the mechanism in isolation (e.g. a manual `MemorySegment.ofAddress(...).set`
   write that Arrow can read).
2. Verify the middle layer directly (call the kernel straight on Arrow buffers).
3. Instrument the deepest layer (prints inside the kernel). Prints not appearing =
   wrong class on the classpath.
Each step either pins the fault or narrows it.

## Environment Facts (verified in this repo)

- `TaskContext.get()` is non-null on the driver in local-mode tests, so kernel
  execution also happens in driver-side eval.
- Test JVMs pass `--enable-native-access=ALL-UNNAMED` (`project/SparkBuild.scala`),
  so `MemorySegment.ofAddress(...).reinterpret(n)` works in tests.
- `ColumnarBatch` and Arrow vectors are not serializable. To feed a batch into a node
  under test, rebuild the batch inside the task from a serializable spec. Nested case
  classes capture the non-serializable test suite via `$outer`; keep spec classes
  top-level.
- `SparkPlan.executeCollect()` goes through `getByteArrayRdd()` and casts rows to
  `UnsafeRow`; row-producing evaluators must emit `UnsafeRow`.

## Columnar Transition Wiring (plan level)

- `ApplyColumnarRulesAndInsertTransitions` runs `preColumnarTransitions`, then
  `insertTransitions`, then `postColumnarTransitions`. A `postColumnarTransitions`
  rule sees the transitions already inserted.
- `ensureOutputsRowBased` gives a dual-mode plan (`supportsRowBased` and
  `supportsColumnar`, e.g. `InMemoryTableScanExec`) row output when its parent
  consumes rows, so above a cached scan there is often *no* `ColumnarToRowExec` to
  pattern-match on. Fuse on `child.supportsColumnar` (and switch the dual-mode
  child to columnar output), not on the presence of a transition.
- `ColumnarToRowExec` is row-only: it has no `doExecuteColumnar`, so calling
  `executeColumnar()` on it throws. A fusion node that consumes a columnar child
  must absorb the transition (`case ColumnarToRowExec(inner) => inner`) instead of
  wrapping it.
- The `ColumnarToRowTransition` tag is read by some machinery as "semantics-free
  row conversion" - `CachedBatchSerializer.convertToColumnarPlanIfPossible` strips
  a topmost transition and executes its *child* to get columnar cache input. A
  fused node wearing the tag (every Varka `*ColumnarToRowExec`) carries real work
  inside it, so every tag consumer that strips must instead convert the fused node
  to its columnar sibling (identical kernels, columnar out) - the Arrow serializer
  override does. Found in task 21 as a wrong-cached-view bug latent since task 6:
  every direct query stays right, and only a *cached* view materializes the
  dropped work. When adding a fused transition node, grep the tag's consumers.

## Metrics as the "did it really run" proof

- A fused plan plus correct results does not prove the kernels ran: the per-batch
  fallback also returns correct results. Prove execution with a metric
  (`numVarkaBatches`) bumped only on the kernel path.
- Read metrics *after* execution: run `checkAnswer`/`collect` first, then read.
  Reading before execution returns 0 even though every guard passed.
- The executed-plan root is often a `WholeStageCodegenExec` whose `metrics` map
  only has `pipelineTime`. Read the node's own metric via
  `plan.collectFirst { case v: VarkaColumnarToRowExec => v }`, not `plan.metrics`.

## Extra Sessions on the Shared Context

- For side-by-side engine comparison, build extra sessions on the same context:
  `SparkSession.builder().sparkContext(spark.sparkContext)`. Clear the active and
  default sessions between creations (`SparkSession.clearActiveSession()` /
  `clearDefaultSession()`).
- Sessions have separate catalogs: a temp view cached in one is not visible in
  another; register and cache the data in every session.
- `InMemoryRelation` holds the cache serializer in a process-wide static
  initialized on first use; call `InMemoryRelation.clearSerializer()` in
  `beforeAll` and again in `afterAll` so the choice does not leak to later suites.
- AQE is on by default in the test framework's shared session; disable it
  explicitly on custom sessions, or plans change shape and QueryStage threads leak.

## Build Gotchas

- The engine module is a reactor module since the sbt wiring change: sbt builds it
  in-tree and puts its jar on the catalyst/sql test classpaths itself
  (`VarkaEngine`/`VarkaEngineDependency` in `project/SparkBuild.scala`), so no manual
  install step remains. Maven still builds it standalone
  (`./build/mvn -f sql/varka/engine/pom.xml test`; `mvn` is not on `PATH`, use
  `./build/mvn`), which is how the engine-only suites and JMH run.
  (Earlier revisions of this file described a `~/.m2` install cycle from before the
  reactor change.)
- scalastyle requires a trailing newline at EOF ("File must end with newline
  character") and rejects `throw new XxxError` via the `throwerror` rule. For a
  deliberate `NoClassDefFoundError` test hook, wrap the throw in
  `// scalastyle:off throwerror` / `// scalastyle:on throwerror`.
- **Java in a non-core module must not pass a Guava type to a `core` API - and only
  Maven can tell you.** `core/pom.xml` relocates `com.google.common` to
  `org.sparkproject.guava` when it shades, so in the shaded jar the signature reads
  `NonFateSharingCache(org.sparkproject.guava.cache.Cache)`. Scala never notices,
  because scalac resolves the symbol from the Scala pickle, which the shade plugin
  does not rewrite; javac reads the relocated descriptor and fails with
  `cannot infer type arguments`. So the same call compiles from Scala and not from
  Java, and SBT - which does not shade - hides it from both: the only gate that sees
  it is the "Java 25 build with Maven" CI job. Upstream hit this too (SPARK-44064
  added a Guava-free `NonFateSharingCache.apply` overload precisely "to avoid non-core
  modules Maven test failures caused by using shaded core module"), and the two other
  non-core users, `CodeGenerator` and `ProtobufUtils`, both take Guava-free overloads.
  Note what this means for the Scala side as well: a Scala call site that passes a
  Guava type compiles against a method that does not exist in the shaded artifact, so
  it is a latent runtime failure rather than a safe alternative. Keep Guava types
  inside the module that owns them; when a `core` utility cannot be reached without
  one, reimplement the few lines locally (task 23 did this for the shape cache's
  single-flight gate). Verifiable in seconds without a Maven run: `javac` the file
  against `~/.m2/.../spark-core_2.13-*.jar` and see it fail.
- **A Java class with an incubator-module type in a field needs `--add-modules` twice
  under Maven, and again only Maven can tell you.** Task 24 put `SelectionVectorOps` -
  a `jdk.incubator.vector` kernel - in catalyst's main sources. Adding
  `--add-modules jdk.incubator.vector` to `scala-maven-plugin`'s `javacArgs` compiles
  it, and the build then fails *after* a successful compile with
  `NoClassDefFoundError: jdk/incubator/vector/VectorSpecies`. The reason is zinc's
  API extraction: `sbt.internal.inc.ClassToAPI.structure` calls
  `Class.getDeclaredFields()` on the class file it just wrote, which loads the field
  types **reflectively, in the compiler's own JVM**. So the flag has to go in that
  plugin's `jvmArgs` as well - two blocks, both with `combine.children="append"` so
  the parent pom's own arguments survive. SBT never reaches this because the sbt
  launcher already runs with `--add-modules=jdk.incubator.vector`, which is exactly
  the shape of the Guava trap above: an SBT-green, Maven-red failure on the Varka Java
  surface. Reproducible in seconds without a Maven run - `java -cp
  sql/catalyst/target/scala-2.13/classes` a one-liner that calls
  `getDeclaredFields()` on the class, with and without the flag.

## Build Performance (measured, Aug 2026)

Benchmarked on a ThinkPad P16s Gen 4 (Ryzen AI 9 HX PRO 370, 12c/24t, 96 GB, NVMe).
Numbers are wall-clock for a cold `sql/compile` chain unless noted.

- The ceiling is Scala 2's single-threaded compiler frontend plus Spark's serial module
  graph (core -> catalyst -> sql/core). CPU sits at 350-570% out of a possible 2400%,
  so most of a build is one core running scalac.
- **Background CPU contention was the single largest effect.** The same cold build took
  103.9 s while three runaway browser tabs ate ~1.3 cores, and 89 s once they were gone
  (-14%). It also widened run-to-run spread from +/-1.7% to +/-15%.
- **One-shot `build/sbt` invocations cost ~9.3 s each** in JVM startup and build-definition
  loading. A no-op `sql/compile` is 11.0 s standalone but ~1.7 s as another command in a
  live session.
- **sbt beats Maven** for the same chain: 103.9 s vs 139.5 s, plus real incremental
  compilation. When Maven is required, `MAVEN_ARGS="-T 1C"` builds independent modules in
  parallel (CPU 551% -> 817%); `build/mvn` also ships a 4 GB heap and a 128 MB code cache,
  which `MAVEN_OPTS` can raise.

Tuning knobs that were tested and made **no measurable difference** -- do not re-litigate
these without new evidence:

- `-Ybackend-parallelism` (verified it reached scalac via `show sql/scalacOptions`)
- raising the sbt heap above the 8 GB set in `.sbtopts`
- Zinc's `recompileAllFraction` (0.2 / 0.5 / 1.0 all identical)
- scalac warning analysis: `-Wunused:imports` and the whole `-Wconf` list cost nothing
- JVM transparent huge pages, CPU governor / power profile, AC vs battery
- I/O, swap and filesystem tuning -- a cold build takes 62 major page faults and writes
  ~2 MB/s, so the kernel is not in the path at all
- genjavadoc is already gated to the unidoc config and does not run on normal compiles

Benchmarking method: compare interleaved A/B runs by their minimums. Single-run
comparisons on a contended machine carried a +/-15% noise band, large enough that several
apparent small wins turned out to be noise.

## C2 Compile Latency Is the Wide-Vector-Loop Cliff (root cause, proven)

- A 64-op emitted vector loop ran 1.0 G rows/s in one JVM and 9-13 M rows/s in
  another. First hypothesis - "history-dependent inlining", suspecting
  `InlineSmallCode` - was *refuted* by experiment: raising it changed nothing. The
  proven mechanism (`-XX:+PrintCompilation`): the method's tier-4 OSR compile takes
  ~10 seconds for a 1457-byte method whose 64 Vector API call sites each expand into
  large intrinsic graphs, and until it lands the loop runs the C1 version with boxed
  vectors. A 30-second window showed the rate jump 9 -> ~1000 M rows/s at t=12s.
  "JVM history" only shifted when the compile started relative to the measurement
  window - fresh JVMs got it in during warmup, busy ones did not.
- The structural fix stands regardless: keep every hot loop method small by
  construction (the emitter splits outputs across sibling loop methods of at most
  `GROUP_BUDGET = 16` ops, called from a driver). Small methods compile in
  moments; the 64-op kernel as four 16-op methods hits ~1 G rows/s in the same
  polluted JVM that showed the cliff.
- Corollary for benchmarks of generated code: a case that never speeds up may be
  waiting on a compile, not hitting a wall. Distinguish with a long window and
  periodic rate reporting before concluding anything; then read
  `-XX:+PrintCompilation` (a repeated OSR task line marked `blocked` was the tell).
- Related cost numbers: emitting + defining + loading + instantiating a fused kernel
  class is 130-450 us even for the widest shape - class *generation* is never the
  cold-start cost; C2 compile latency is.
- Same family, earlier finding (task 10): two vector loops emitted into one method
  also degrade each other (3x-4x on the second loop). One C2 compilation per hot
  loop, always - sibling methods, not longer methods.
- Same family, task 14's post-commit diagnosis: **a class defined per task re-pays
  the whole tier ladder per task.** The per-task loader defines a fresh kernel class
  each task; HotSpot treats it as new, so every task runs interpreter, then C1 with
  boxed vectors, then the C2 OSR compile - a *fixed per-task* cost that grows with
  the loop method's vector-op count (~13 ms for an 8-op chain, ~50 ms for the 20-op
  dayofweek fold) and dwarfs the ~80 us emission it sits next to. Two diagnostics
  that pin it: (1) scale the table 4x - a per-task-fixed cost leaves the absolute
  delta unchanged where a per-row cost quadruples it; (2) `-XX:+PrintCompilation`
  shows one tier-4 OSR of the same-named method *per task*, each followed by
  "made not entrant: OSR invalidation" as the task's class dies. The decomposition
  (PLAN_TASK_14.md 7.5): the C2 compile itself is ~1 ms per vector op (2/10/20-25 ms
  for 3/10/20-op loops), and the interpreted and C1 profiling phases before it
  scale the same way, because tier counters advance per backedge at boxed speed -
  which is also why a scratch-batch warm spin saves nothing. Corollary for any
  cross-task cache: caching `byte[]` does not help - a re-defined class is a new
  class and re-pays the ladder; only reusing the *loaded class* preserves the C2
  code. And benchmark tasks must be long enough to amortise the ladder, or the
  committed number prices JIT warm-up, not the kernel. Task 18 acted on the
  corollary - `VarkaShapeCache` shares the loaded class across tasks, keyed on
  the IR shape - and the committed depth curve flattened from 2.2x-eroding-to-1.3x
  into 6.5-7.2x flat, confirming the ladder was the whole erosion.
- Second corollary, caught by task 18's PR review after the results file was
  committed: **a cache keyed on structure silently defeats a harness that
  manufactures freshness through values.** `VarkaColdStartBenchmark` made each
  iteration "fresh" via distinct columns and literals - exactly what the shape key
  ignores by design - so after task 18 the guard query warmed the process-wide
  cache and every timed "cold" iteration measured a hit while the harness's own
  comments still promised a fresh emission. When a cache key changes, re-derive
  every benchmark's freshness argument from the new key rather than trusting the
  harness; the fix here invalidates the shape cache inside the timer loop.
- **The 64-op cliff does not generalize to every wide method - it is specific to
  what was compiling it.** Task 32 step B2 built the real thing the cliff worried
  about, a 200-vector-op single loop method (four calendar fields fused by a
  widened `GROUP_BUDGET`), and measured its compile time directly with
  `-XX:+PrintCompilation` filtered to the generated class's own name (every Varka
  kernel gets a distinct class, so this is exact). It reached tier 4 in **272 ms**,
  and the widest kernel in the same suite (twenty separate loop methods) in 2.4 s -
  neither approaches the ~10-second cliff, and neither shows the cliff's own tell
  (a repeated tier-4 task line marked `blocked`, zero occurrences anywhere in the
  log). The original 64-op finding was on a *different* kernel - `javac`-compiled
  Java source with a heavier call-site mix - and the risk register correctly
  treated it as a hazard to re-check rather than an assumed fact, which is exactly
  why this measurement was taken before recommending the design rather than after.
  The general lesson: a historic compile-time cliff belongs to the method that hit
  it until proven otherwise, not to "loop methods of that op count" as a category -
  measure the actual generated bytecode's compile time before assuming a past
  finding transfers to a new emitter, a new op mix, or a new class shape.

- **Op count is not the only compile gate: bytes are a second, harder one.**
  `GROUP_BUDGET` bounds a loop method's *vector ops* because compile time grows with
  them. `HugeMethodLimit` (8000 bytes, a product default) bounds a method's *bytecode*,
  and past it HotSpot does not compile the method slowly - it does not compile it at
  all, at any tier, so the method runs interpreted with boxed vectors forever. The two
  gates catch different shapes, and the emitter's epilogue is where the second one
  bites: task 24 made it one method over *every* output, so its size grows with the
  whole projection rather than with a group, and four calendar fields over five date
  columns crossed 8000 bytes. Measure it rather than estimating - the `Code`
  attribute's length, which is exactly what HotSpot measures, is two lines through
  `java.lang.classfile` (`VarkaEmitterTestSupport.codeSize`) - and assert the crossing
  in a test, so the next wide node moves a number instead of quietly falling off.
  Task 32 step B1's ladder is in `PLAN_TASK_32.md` section 7.1.

## A hand-written comparison kernel needs every fast path the real one has

`ChronoVectorOps.vectorFourFields` was built as task 32's throughput ceiling: same
arithmetic, same guard, same op count as the emitted kernel it stands in for. It has no
dense/masked split, though - it always builds a `VectorMask` and uses masked load/store
overloads, even when the caller reports every row non-null. On null-free data the emitted
kernel dispatches to a genuinely unmasked dense body (task 10's split), so the "ceiling" was
silently measuring the masked-body cost the whole time. Once step B2 made the true emitted
dense-shared kernel buildable, it beat the "ceiling" by 1.15-1.20x, reproducibly across three
runs. The lesson generalizes past this one kernel: a hand-written stand-in for an emitted
path is only a fair bound if it takes every fast path the emitted dispatcher can reach for
the data it is fed - a masked-only comparison kernel run on null-free data is not measuring
the same thing the emitted kernel is, and the gap does not announce itself; nothing crashes
or looks wrong, the "ceiling" just quietly is not one.

## Calling into an uncompilable method costs something even when it does nothing

A method past `HugeMethodLimit` is never compiled at any tier, so every call into it runs
interpreted - including a call that hits an early return and does no real work at all.
Measured on a 20-calendar-output epilogue (9436 bytes unshared, past the limit; 4048 bytes
shared, under it) at an *aligned* chunk size, where the generated early-return check
(`if (loopBound < length) ...; return 0`) fires on every single call and the epilogue never
executes a real row: the unshared (uncompilable) version still cost 1.36x the shared
(compilable) one, invisible at large chunks (4096 calls per pass, real vector work
dominates) and clearly visible at small ones (15625 calls per pass, the per-call cost
dominates). Do not describe a `HugeMethodLimit` crossing as "costs nothing on aligned
batches" - it costs nothing *extra in arithmetic* on aligned batches, but the call itself,
paid on every batch whether aligned or not, is measurably more expensive when the callee can
never leave the interpreter. The unaligned case remains far larger (7.3x at a chunk where
most of the batch is remainder) because there real interpreted arithmetic dominates too.

## A benchmark that reuses `repeats` for wall-clock scaling must also scale its declared row count

`Benchmark`'s Rate/Per-Row columns divide the measured time by the row count passed to its
constructor, not by what the timed closure actually does. A section whose closure loops
`repeats` times over `numRows` (the pattern this file's "year" and alignment-ladder sections
both use, to pay the per-call prologue at production rate rather than timing one giant call)
must construct `Benchmark` with `numRows * repeats`, not `numRows` - passing the smaller
number does not corrupt any *ratio* between cases in the same section (both are scaled by the
same missing factor), but every absolute Rate and Per-Row figure comes out wrong by exactly
that factor, silently, with no error and a plausible-looking table. Sanity-check a new
section's absolute numbers against a neighboring section of comparable op count before
trusting them, not just its ratios.

## Sharing below the node level in an emitter

- **The values worth sharing are not always nodes.** Varka's DAG-CSE memoizes on
  structural equality between IR nodes, which cannot help when the redundancy is
  *inside* one node's emitted run: `year(d)` and `month(d)` are two nodes that each
  emit the same forty-op civil-from-days decomposition, and era, century and the rest
  are locals, invisible to any walk over nodes. The fix does not need an IR change -
  a *fragment*, keyed on (kind, child) and tracked in a per-lane-group set beside the
  node-level `computed` set, shares the locals directly. A multi-value IR node emits
  identical bytes, so choosing between them is engineering cost, not throughput; the
  fragment wins because it generalizes to every node built on the same prefix without
  the IR naming any of them.
- **A shared run must be keyed on everything it reads, not just on its input.** The
  prefix here also emits a range guard, and the guard is ANDed with the node's
  validity word. Every plain extraction aliases its word to its child's, so those
  share safely - but `add_months(d, n)` ANDs the date's word with the month count's,
  so keying on the child alone would have given it a guard computed under a different
  mask. Put the extra input in the key and the mistake becomes unrepresentable rather
  than a rule in a comment.
- **A generated class's raw bytes never compare equal across two emissions**, because
  the harness names each class afresh and the name is in the constant pool. An
  assertion of the form "this option changed / did not change the bytecode" has to
  compare a *method's* code size (or its instructions), not `Arrays.equals` on the
  class. The false direction is the dangerous one: `!Arrays.equals` passes for any two
  emissions whatsoever, so a test written that way proves nothing at all.

## Vector API on HotSpot, Measured (JDK 25, x86-64)

- An *exact* magic multiply on int lanes exists only for dividends under roughly
  **46341**, and the bound falls straight out of the two conditions rather than
  needing a search: worst-case `e ~ d` forces `2^k > d * v`, hence `M ~ v`, hence
  `v * M < 2^31` gives `v < 2^15.5`. Past that, use a **round-down** magic
  (`M = floor(2^k/d)`), which never overestimates the quotient, and pay a fixed
  number of carry steps - one compare and two masked adjustments each, on a
  remainder the algorithm usually wants anyway. Task 26 needed two such divisions
  (146097 and 36524) and found no exact form at any useful range for either; what
  made the rest exact was *restructuring* - splitting an era into centuries first
  drops the `/365` dividend from 146096 to 36524, under the bound. Reach for a
  different decomposition before reaching for more carries.
- Those carries are not free, and the reason is the masked ops in them. Task 26
  predicted that its full-range variant would cost 5-12% over its narrowed one on
  op count alone (five ops on forty) and measured 14-24%: the five extra ops are
  masked adds and subtracts, which this project has separately measured at 2.3-2.9x
  an unmasked one. Count masked ops at their own weight when predicting.
- **`java.time` itself got 2.0x faster between JDK 17 and JDK 25**
  (`LocalDate.ofEpochDay(d).getYear()`: 236 against 479 M rows/s, same machine,
  `sql/varka/baselines/`). Any speedup quoted against a scalar `java.time` baseline
  has to say which JDK the baseline ran on, and a figure inherited from an older
  task may have a denominator two-fold different from today's. The same trap in the
  other direction: escape analysis scalarizes the `LocalDate` allocation in a tight
  loop, so a scalar calendar loop measured in a microbenchmark is far faster than
  the same code inside a query - task 26 predicted 15-30x over it and measured 3.7x.

- `VectorOperators` has no multiply-high on any lane type, and there is no sign that it
  will get one soon, so do not design around its arrival. Checked against JDK 25 (`javap`
  on `jdk.incubator.vector.VectorOperators`: `MUL` is the only multiply) and against
  openjdk/jdk master (code search for `MUL_HIGH` and `VECTOR_OP_MULHI`: no hits). C2 does
  have the operation internally - `MulHiLNode`/`UMulHiLNode` in `opto/mulnode.hpp`, used by
  `divnode.cpp` to lower scalar division by a constant and by the `Math.multiplyHigh`
  intrinsics, plus `MulHiLoLNode` for the fused 64x64-to-128 form - it is simply not exposed
  lanewise. The nearest request is JDK-8219881, "[vector] Optimized 32-to-64 bit vectorized
  multiply": an Enhancement, still Open, P4, filed February 2019, last touched October 2024,
  with `fixVersion` `repo-panama` rather than any release. The API does accept new integer
  ops when someone drives them - JDK-8338352 delivered `SADD`/`SSUB`/`SUADD`/`SUSUB`,
  `UMIN`/`UMAX` and the unsigned comparisons, all present in JDK 25 - so an RFE backed by a
  concrete workload is a real option, but it is a contribution to make, not a dependency to
  plan against.
- Because of that, full-range
  Granlund-Montgomery magic division is not expressible on int lanes - but a
  *range-narrowed* magic is: shrink the value first until the correctness condition
  (`v * e < 2^k`) and the no-overflow condition (`v * M < 2^31`) both fit in the low
  32 bits that `mul` does return. Mod-7 (task 14 follow-up, after a reviewer asked
  the right question): two 15-bit folds (`2^15 = 1 mod 7`) leave `v <= 32774` with
  the sign fixup, where `q = (v * 37450) >>> 18` is exactly `v / 7` with no final
  fixup - measured 1.6-1.8x the six-fold digit sum it replaced (which stays as a
  reference variant), ~9x lanewise `DIV` (no SIMD divide exists on x86; it
  effectively scalarizes), and ~57x a per-row `LocalDate` loop. The ~10-op-smaller
  method also cuts the per-task JIT warm-up above by ~28 ms per task.
- **The vector path is worth 6.5x over good scalar code, and the project had never checked.**
  `ChronoScalarOps` is `year(date)` as an ordinary Java loop over the same Arrow buffer, in the
  same 4096-row chunks, writing the same outputs: 284.2 M rows/s against the emitted kernel's
  1816.6 at AVX-512, both from the committed parity file. Until it was written, the only
  scalar anchor in that file was a per-row `LocalDate` loop, and the headline ratio was quoted
  against it. Keep the real
  baseline: "faster than `java.time`" and "faster than scalar arithmetic" are different claims
  and the second is the one that justifies the emitter.
- **`LocalDate.ofEpochDay(d).getYear()` in a tight loop does not allocate, and is a legitimate
  scalar baseline.** It measures 480.8 M rows/s, and C2 scalar-replaces the `LocalDate` -
  escape analysis sees it created and consumed in the same loop. It is worth stating because the
  opposite is the natural assumption and it was asserted out loud in this project before being
  checked. It also beats a hand-written 64-bit civil-from-days by 1.7x (480.8 against 284.2):
  `java.time` does the same Hinnant decomposition in 32-bit ints, and forcing everything through
  64-bit longs to buy exactness over the whole int32 range costs more than the exactness is
  worth in scalar code.
- **Spelling a constant division as `(x * M) >>> k` instead of `x / d` is worth 1.38x in scalar
  code** - 284.2 against 205.4 M rows/s for the same algorithm. C2 lowers `long / constant` to
  `MulHiL`, one instruction but a costlier one: it keeps the high half, and on x86-64 it
  clobbers RDX:RAX. An ordinary `imulq` plus a shift is cheaper wherever the product fits 64
  bits, which for a 32-bit dividend and a ~30-bit magic it does.
- **SuperWord will not auto-vectorize the calendar arithmetic, for two independent reasons,
  and neither is the `MemorySegment`.** The `(x * M) >>> k` form uses only `MulL` and
  `URShiftL`, both of which have vector counterparts (`MulVL`, `URShiftVL`), and the loop body
  was written branchless for exactly this reason. C2 still does not vectorize it:
  `-XX:-UseSuperWord` moves the number by 0.1% (284.1 against 284.4 in the A/B run). A
  four-case bisection -
  {`int[]`, `MemorySegment`} x {trivial body, full decomposition} - locates it: the trivial
  `int[]` loop gains 4.84x from SuperWord, the trivial `MemorySegment` loop only 1.09x, and the
  full body gains nothing on *either* (0.99x on `int[]`, 1.00x on the segment). The absolute
  figures in the committed parity file say the same thing: 26996.5 M rows/s for the trivial
  `int[]` loop against 6265.2 for the same body over a `MemorySegment`, and 287.1 for the full
  body on `int[]` against 284.2 over the segment - identical once the arithmetic is the real
  work. So the
  arithmetic is the binding constraint; segment addressing hurts as well but is not what stops
  it.
  `-XX:+TraceSuperWord` on a fastdebug JVM gives the mechanism exactly:
    - At the default `LoopUnrollLimit=60`, `SuperWord::transform_loop` is entered **zero
      times**. The body is too large to unroll, and with no pre/main/post structure there is no
      main loop for SuperWord to work on. It never gets asked.
    - At `-XX:LoopUnrollLimit=1000` it is entered four times and succeeds none. It builds packs
      - 8-wide `LoadI`, 8-wide `MulL` - and then
      `SuperWord::filter_packs_for_profitable` discards all of them
      (`WARNING: Removed pack: not profitable`), ending at `0 packs` and
      `SLP_extract did not vectorize`. The width mismatch is visible in the pack contents:
      eight 32-bit loads feeding 64-bit multiplies do not occupy one vector.
  The consequence for this project: the explicit Vector API is not a convenience over
  auto-vectorization here, it is the only route, and that is now measured rather than assumed.
  A lowering with no int-to-long mixing would clear the second gate, but the first is a tunable
  no shipped Spark can depend on.
- **The A/B that needs no tools**: run the case twice, once with `-XX:-UseSuperWord`. A loop
  SuperWord vectorized slows down; one it never touched does not move. That answers "did it
  vectorize" in a product JVM. Answering *why not* needs a fastdebug build - see the
  fastdebug section below.
- **Op counts bound a speed-up; they do not estimate one.** Three predictions in one milestone,
  all made from op ratios, all optimistic: sharing one decomposition across four calendar fields
  was predicted at 2.0x-3.2x and measured 1.5x; a scalar `year` was predicted within 2.5x of the
  vector kernel and came in at 6.5x; keeping fewer values live was predicted to help at narrow
  widths and lost at both. What the op-count model leaves out - stores, validity bookkeeping,
  loop control, dependency-chain latency - is routinely half the time or more. Predict a bound,
  say it is a bound, and measure.
- Masked lanewise ops and masked stores cost 2.3x-2.9x even when the mask is all-true:
  a runtime mask is opaque to C2 and a masked store never becomes a plain store. If
  masks carry no correctness (in-bounds accesses, invalid destination lanes declared
  undefined), run unmasked and keep validity in long words on the side.
- A vector held in a Java local across a loop pins one register for the whole body and
  blocks C2's rematerialization; ~32 such broadcasts collapsed throughput 7x. Emitted
  at each use, a loop-invariant broadcast gets hoisted when registers allow and
  rematerialized (one instruction) when they do not. Hoist only in measured-small
  regimes.
- Corollary for manual unrolling and software pipelining - the standard prescription
  for feeding a superscalar core, and a real gap, since C2 does not unroll Vector API
  pipelines: three of the measurements above price the experiment before it is run, and
  two of them cut against the prescription. (1) Unrolling by K multiplies the body's
  live temporaries by K, against a register file where ~32 pinned broadcasts already
  cost 7x - so unrolling and pre-broadcasting the loop's constants *compete* rather
  than compose, and K has to be varied together with the broadcast strategy, never
  alone. (2) It multiplies the loop method's op count by K against `GROUP_BUDGET = 16`,
  which exists because C2 compile latency is ~1 ms per vector op - affordable only
  since task 18 pays that once per shape rather than once per task. (3) It cannot
  rescue a chain built on lanewise `DIV`, which scalarizes: interleaving two scalarized
  chains is still scalar, so any such constant needs its range-narrowed magic first.
  None of this predicts that unrolling loses - it says the experiment has three known
  confounders. Registered as `PLAN_MILESTONE_4.md` task 25 (catalogue item 13);
  unmeasured as of this entry, and this bullet gets rewritten with the numbers when it
  is.
- Apply constant offsets *after* a mod, not before: `floorMod(days + 4, 7)` overflows
  int for days near `Int.MaxValue`, while `(floorMod(days, 7) + 4) mod 7` cannot.
  Negative inputs are where every strength-reduced mod goes wrong silently - a test
  range that never crosses zero proves nothing. The rule is about avoiding overflow,
  though, not an end in itself - it inverts when the oracle's own arithmetic already
  overflows on purpose. `next_day` (`PLAN_TASK_33.md`) computes `k - d` *before* the
  mod because Spark's `getNextDateForDayOfWeek` computes it in plain wrapping `int`
  arithmetic; reducing first disagreed with the row engine on 28 boundary cases in the
  planning pass's own check. Whose arithmetic the oracle is - exact (`LocalDate`,
  never wraps) or wrapping (plain Spark `int` math) - decides which way this rule
  points, and the reflex answer for one is the wrong answer for the other.
- A fixed-width species literal (`IntVector.SPECIES_256`) is not a safe way to get "the
  int species with half `LongVector.SPECIES_PREFERRED`'s lane count": under
  `-XX:MaxVectorSize=16` (this project's narrow-vector CI shape, 128-bit), no 256-bit
  registers exist, and the mismatch surfaces as a `VectorIntrinsics` bounds exception
  at a `fromMemorySegment` call site far from the real cause. Derive the matching
  species instead: `VectorSpecies.of(int.class,
  VectorShape.forBitSize(longSpecies.vectorBitSize() / 2))` tracks whatever
  "preferred" resolves to at the JVM's actual configured width, including the narrow
  shape. Any code pairing two lane types by a literal `SPECIES_*` constant needs the
  same check.
- Buffer alignment is not a null hypothesis once the buffer fits in cache: a 64-byte
  (AVX-512 register width) misaligned start costs 1.6-1.7x throughput at a 4096-row
  (one Spark batch, L1/L2-resident) working set, and 1.2x at 128-bit - reproducible
  across repeated runs at both widths (`VarkaMilestone4MeasurementsBenchmark`,
  `PLAN_MILESTONE_4.md` section 8). The same misalignment costs under 2% on a
  multi-megabyte streaming buffer, where DRAM bandwidth dominates and hides it -
  measure at the working-set size the real kernel runs at, not whichever size is
  convenient to allocate once. A 2-way unrolled kernel loses the same 50-60% as the
  non-unrolled one: unrolling does not hide a cache-line-split load.
- A materialization strategy's ranking can flip across the two vector widths this
  project already tests at. Packing a comparison mask straight to its output bitmap
  (skipping an intermediate int 0/1 column) wins by 1.16-1.18x at AVX-512 but *loses*
  by 1.40-1.51x at 128-bit (`VarkaMilestone4MeasurementsBenchmark`). A single
  same-JVM run at the development machine's native width is not enough evidence for
  a strategy that has to also hold at the narrow-vector CI shape.
- When a benchmark's K=1 case is fully unrolled straight-line source (the shape a real
  emitted kernel carries), the K>1 cases must be too - a small constant-bound runtime
  `for` loop over the op index is not a safe stand-in for hand-unrolled code, even
  though C2 usually fully unrolls tiny fixed-trip-count loops itself. Measuring
  `VarkaUnrollFactorBenchmark`'s K=2/K=4 cases through such a loop first showed K=4
  losing 30-60% on some shapes; rewriting them as straight-line interleaved code (same
  shape as K=1, just K independent lane groups instead of one) turned that into a
  reproducible +4-6% win on the shape where unrolling should help at all. The first
  number was an artifact of comparing a loop-shaped baseline against a straight-line
  one, not a real unrolling cost - a benchmark comparing "K=1" against "K>1" has to
  keep every other structural choice, including loop-vs-straight-line shape, identical
  between the arms.
- A hand-written kernel standing in for emitted code must not introduce a method
  boundary the emitted code does not have, and "it is a small private helper, it will
  inline" is not something to assume. Task 32 built a kernel to price sharing one
  civil-from-days decomposition across `year`/`month`/`dayofmonth`/`quarter`, wrote the
  decomposition as a `computeFields` helper returning a record of four `IntVector`s, and
  measured it 1.9x *slower* than the four independently emitted nodes - and task 32 was
  declined on that number. `computeFields` compiles to 376 bytecode bytes; C2's
  `FreqInlineSize` is 325, so it never inlined, so escape analysis never saw the record's
  allocation and its consumers in one compilation unit, so the record and its four vectors
  were really heap-allocated once per lane group. `VarkaLoopEmitter.emitChrono` emits zero
  call boundaries in its lane path. The kernel was measuring a Java abstraction the thing
  it modelled does not have. **Two cheap checks that would have caught it before the
  number was believed**: `javap -c -p` for any method holding lane arithmetic that exceeds
  325 bytes, and `-XX:+PrintInlining` (narrowed with
  `-XX:CompileCommand=option,Class::method,PrintInlining`) for a `failed to inline` inside
  the loop. Rebuilt hand-inlined, the same kernel runs 1.5x *faster* than the four nodes.
- An op-count ratio bounds a sharing win; it does not estimate one. The same task 32
  kernel shares ~45 of each field's ~50 vector ops, so four fields cost ~200 ops separate
  against ~65 shared - a 3x op-count ratio, which was registered as a prediction of
  2.0x-3.2x throughput. Measured: **1.51x-1.54x** at AVX-512 across three runs, the
  committed parity file's being 679.0 against 445.7 M rows/s. The half that went missing is
  everything the model ignored - four stores, four validity-bitmap read-modify-writes, the
  chunk prologue, loop control - none of which sharing touches. Predict a bound, not a number.
- Once a lane path has no calls left in it, `-XX:CompileCommand=inline` buys nothing, and
  it was never a fix anyway - Spark cannot require a `CompileCommand` on a user's JVM, so a
  flag that helped would only be a diagnostic pointing at a code change. Task 32 tested it
  properly before concluding that: `-XX:+PrintInlining` showed the two `VarkaVectorSupport`
  validity helpers genuinely failing to inline inside the shared loop
  (`NodeCountInliningCutoff` on one compilation, `callee is too large` on another - 212
  bytes of a four-arm switch on the lane width that a constant lane count would fold away),
  and forcing them in changed nothing at either vector width. Nor did forcing every Varka
  class (`inline,*varka*::*`). This is the third time an inlining flag has moved under 1% in
  the catalyst parity harness while the engine's JMH harness moves 50-190% on the same flag;
  a flag worth that much in only one harness is measuring the harness.
- A kernel can have two stable machine-code outcomes and no reachable reason. Task 32's
  shared kernel is bimodal at 128-bit: 121 ms or 85 ms, stdev 0 ms inside a run and 42%
  between runs, 3 fast outcomes in 14 runs. Neither forcing inlining, nor disabling
  on-stack replacement, nor rescheduling the body to keep fewer values live made either mode
  deterministic or shifted the distribution. **Report both modes; never average them** - the
  mean describes a state no run is ever in - and treat "which compilation the JVM landed on"
  as a first-class outcome rather than as noise to be smoothed away.
- Keeping fewer values live is not automatically faster, and the intuition is worth
  distrusting. Task 32 built a variant of the same kernel that hoisted the year assembly so
  three intermediates died early and stored each of four outputs the moment it existed
  instead of all four at the end. It lost at both widths (633.0 against 679.0 M rows/s in
  the committed parity file, 156.5 against 165.6 at 128-bit). C2's scheduler did better with
  the wider window than with the shorter live ranges - and the losing shape is the one the
  emitter naturally produces, which is worth knowing before assuming emitted code will match
  a hand-written ceiling.
- The same sharing win is width-dependent, and that is where task 17's register-pressure
  finding actually lives. At 128-bit the identical kernel is a wash: 1.06x in four runs of
  five, 1.50x in the fifth, stdev 0 ms inside each run and 42% between them - a
  compilation the JVM either finds or does not, so the two modes must be reported rather
  than averaged. Five live intermediates plus four outputs fit comfortably in 32 zmm plus
  8 dedicated mask registers and marginally in 16 xmm that must hold masks too; C1 refuses
  the 936-byte body outright at both widths ("out of virtual registers in linear scan").
  Task 17's `GROUP_BUDGET` result (raising it to keep two outputs' cross-output CSE in one
  method lost 4119.9 against 2928.2 M rows/s, current committed parity file) is the same
  effect. It sets a ceiling on how much sharing can win; it does not decide the sign, and
  a narrow-vector measurement is not optional for anything that shares live values.

## Generated Code Can Carry Its Own Debug Info (Class-File API)

- `CodeBuilder.lineNumber(n)` needs no options or flags: a `LineNumberTable` lands in
  the emitted method, and with a `SourceFile` attribute the JVM fills in file and line
  on every stack frame through the generated code - so a generated loop can name the
  *IR node* that threw, not just the method. Place the marker immediately before the
  node's own defining instruction, after its children are emitted: a marker at the
  start of a post-order case attributes the parent's op to whichever child was emitted
  last.
- Pick line numbers from a property of the IR (task 16 uses the children-before-parents
  topological index), not of the emission order, and record the decoding key inside the
  class - a custom attribute is the natural place, since it travels with the bytes into
  a heap dump or a `javap` capture.
- A custom attribute's payload is fixed-width: adding a field means updating the
  `attribute_length` the writer emits (4 -> 6 for two -> three constant-pool refs) *and*
  the reader's offsets. They are two sides of one format and belong in one commit.

## The Class-File API's Stack-Map Generator Is a Free Verifier

- `ClassFile.of().build(...)` computes stack map frames and rejects inconsistent
  operand stacks at *emit* time (`IllegalArgumentException` naming the bytecode
  offset, with a full instruction dump). A double-store bug in task 11 never reached
  the JVM - one layer earlier than the `ClassFile.verify`-before-load discipline,
  and two earlier than a runtime `VerifyError`.
- Member-resolution mistakes (wrong erased descriptor) still pass both build and
  verify and surface at first execution as `NoSuchMethodError`; keep the
  wrong-descriptor negative-control test so that failure mode stays diagnosable.

## Independent Reference Evaluators as Test Oracles

- For an algebraic surface (three-valued logic, null-skipping picks, blend
  semantics), implement the semantics *twice*: the generated code, and a tiny
  interpreter over the same IR inside the test suite (`Option[Int]` values,
  `Option[Boolean]` Kleene conditions). Run matrices against it row for row. Wrong-
  in-the-same-way bugs are unlikely across two representations that share nothing.
- A fold's *association* is not its *effect order*: a monadic `foldRight` over
  CASE branches evaluated the ELSE first and registered input ordinals right-to-left.
  Where side effects assign identities (ordinals, slots), compile in source order
  explicitly, then fold the already-compiled pieces.

## Testing Under AQE

- Every Varka suite session disables AQE for plan determinism, which silently leaves
  the default-config path (AQE on) unpinned. It worked - but only an experiment
  proved it.
- Under AQE the fused node sits inside a query stage, and a query stage is a *leaf*:
  `SparkPlan.collect`/`collectFirst` never descend into it, so a naive assertion
  reports "not fused" while the node is right there in `treeString`. Traverse with
  `AdaptiveSparkPlanHelper` in AQE tests.

## Write the Prediction Down, Then Measure

- Three perf predictions in this repo's plans were reversed by measurement: "the
  dense path won't beat masked-with-all-true" (it won 2.3x-2.9x), "the masked body
  needs masked ops" (unmasked + validity words doubled mixed-null throughput), and
  "no cliff at the op cap" (there was one, and it moved). JIT-adjacent performance
  intuition loses often enough that the plan should record the expectation, the A/B,
  and ship whichever wins - the written-down prediction is what makes the reversal
  visible and the numbers re-checkable.
- **Check what a benchmark never executes before believing what it says about a
  change there** (task 24). Every committed harness in this repo happened to be
  lane-aligned - this file's parity benchmark ran one call over 1,000,000 rows, the
  engine JMH's sizes are 32 / 10000 / 1000000, and Spark's default
  `COLUMN_BATCH_SIZE` is 4096, all multiples of 4, 8 and 16 - so `loopBound ==
  length` everywhere and the emitter's scalar remainder path had never executed a
  row under measurement. Any remainder-handling change was invisible to every
  committed number. When the code under test has an aligned fast path and a
  remainder path, the size ladder needs sizes like 4095 and 63 on it deliberately;
  a pair one row apart isolates the remainder (equal call counts), and a magnified
  pair (64/63) makes a per-row cost measurable that a 4096-row batch hides in
  noise. Two more measurement lessons from the same task: a cost quoted at one
  rung of a ladder is not a bound on the whole ladder (task 21's "~1-3 ns/row"
  copy cost, read as a ceiling, under-predicted the compress win threefold - the
  scalar copy grew with selectivity and the ceiling was one point on that curve);
  and an in-run control (cases the change cannot affect, measured in the same
  process) is what turns "the numbers moved" into "the noise floor is 15% and the
  effect is inside it".
- **A task that shrinks emitted bytecode must regenerate the committed benchmark
  file, or the next task to regenerate inherits its win** (task 48, measured).
  Task 51 removed the per-extraction range guard - two compares plus mask work on
  every calendar node's tail - and shipped without regenerating
  `VarkaEmitterParityBenchmark-jdk25-results.txt`. Task 48's regeneration
  therefore showed `year, null-free` moving 1823.4 to 2166.5 M rows/s, a fifth,
  for a change whose own A/B measures 1.01x. Three things separated the two, and
  all three are worth reproducing: an **in-run control** (`per-row LocalDate
  year`, which no Varka change touches, read 481.1-481.6 against a committed
  479.4, proving the machine had not drifted); an **in-run A/B** (both sides of
  the change as adjacent cases in one `Benchmark`, which is what actually
  isolates the task); and `git log` on the results file itself against `git log`
  on the emitter directory, which named the two commits that had landed in
  between and left exactly one candidate. Without the first two, a plausible and
  entirely false 21% could have been written down.
- Debugging corollary from the same stretch: before concluding files changed or
  vanished, verify the working directory. A shell whose cwd resets between commands
  plus relative paths fabricates convincing evidence of disaster; absolute paths in
  forensics, always.

## Reading a paper into the repo

- **An extractor that drops things beats one that invents them.** Varka keeps
  machine transcriptions of load-bearing third-party papers in
  `sql/varka/papers/`, and the conversion is deliberately mechanical: the text is
  rebuilt from `pdftotext -bbox-layout` word geometry, where a glyph smaller than
  its line's body and off its baseline is a superscript or a subscript. A plain
  `pdftotext` is useless for a maths paper - it flattens every script, so `2^16`
  becomes `216` and `N_C` becomes `NC` - while the geometric pass recovered 1408
  scripts across 35 pages with seven misses, all of them tokens the PDF had
  already merged.
- **`marker-pdf` was measured against that and rejected, on five pages.** It is
  genuinely better at what the geometric pass loses: eight displayed formulas
  came out as correct LaTeX with their tall delimiters intact, plus 95 table rows
  including assembly listings the geometric pass drops entirely. But on the same
  five pages it silently closed three half-open intervals (`[0, U[` to `[0, U]`,
  twice more elsewhere) and dropped a digit from an eleven-digit validity bound
  (`10441974239` to `1044197429`), and mangled two glyphs in prose. Every one of
  those reads as plausible. A model-based pipeline never leaves a gap where it
  could not read something - it produces something reasonable instead - so its
  output cannot be trusted for the constants and ranges that are the whole reason
  to have the paper. Note it also needs a `llama-server` binary since surya 0.22
  dropped the torch backend, and its `pillow<11` pin does not build on Python
  3.14.
- The general shape: **for anything a plan will quote as a number, prefer the
  extractor whose failure mode is a visible hole.** Then say in the file what the
  hole is, and point at the PDF for the parts that did not survive.

## Repo Workflow (vecbricks/varka)

- Remotes here: `origin` = `vecbricks/varka` (PR base, `master`), `fork` =
  `MaxGekk/spark` (PR head). Push the PR branch to `fork`, then open against
  `vecbricks/varka:master`.
- No JIRA IDs. Titles are `[VARKA] <short summary>`; PR descriptions are prose in
  the five standard template sections; sign off with a `Generated-by:` line naming
  the actual tool (recent PRs: `Generated-by: Claude Code (Claude Fable 5)`).
- Branch naming: `varka-<topic>` tracks `origin/master` and stays one commit ahead
  per PR.

## A bimodal kernel is usually the register allocator, and here is how to prove it

Task 32's shared four-field calendar kernel ran at either 165 or 236 M rows/s under
`-XX:MaxVectorSize=16` - stdev 0 ms *inside* a run, 42% *between* runs, 4 fast outcomes in 21.
Six hypotheses were tested and all failed: shorter live ranges, forcing the validity helpers to
inline, forcing every Varka class to inline, disabling on-stack replacement, raising
`LoopUnrollLimit`, and buffer alignment (which had an OpenJDK bug report behind it,
JDK-8380195, describing the same shape and blaming alignment - it is not that here).

**What it actually is.** Capture `PrintAssembly` for the method in both modes and compare the
*standard* (non-OSR) nmethod:

| | instructions | stack traffic | xmm spill moves | vpmulld | vpsrld | vpsubd |
|---|---|---|---|---|---|---|
| fast (240.6) | 1581 | 721 | **4** | 26 | 14 | 20 |
| slow (164.7) | 3000 | 1567 | **74** | 26 | 14 | 20 |

The vector op counts are **identical**, so it is not unrolling, not a different lowering and
not a missing intrinsic. The entire 1581-to-3000 difference is spill and reload traffic, 18x
more of it. C2's register allocator sometimes finds a clean allocation for this body and
sometimes does not, from the same IR. The OSR compilations, by contrast, are the same in both
runs to within one instruction (7303 against 7304).

It is width-specific for the obvious reason: 128-bit has 16 xmm registers and this body sits at
the edge of them - C1 refuses the same method outright with
`COMPILE SKIPPED: out of virtual registers in linear scan` - while AVX-512's 32 zmm registers
leave slack and show no bimodality at all.

**It is a property of the measurement environment, not of the kernel.** It does not reproduce
standalone (six runs, all 210.5 M rows/s, no spread) and does not reproduce under a fastdebug
JVM even running the whole benchmark (six runs, all ~161). It needs the product C2 *and* the
benchmark's accumulated JVM state. So production is not exposed to it, but a long benchmark's
later cases are, and a ratio is only trustworthy when both arms sit in the same run - which for
the shared-versus-four-node comparison they do, since they are adjacent cases.

**The general rule.** When a kernel is bimodal across JVMs with no spread inside a run, diff
the standard nmethod between the two modes and count spill moves before theorising. Equal op
counts with unequal instruction counts means allocation, not transformation, and no amount of
inlining, unrolling or alignment flags will touch it. The design answer is to reduce what must
be live at once - and note that *scheduling* to shorten live ranges did not help here
(`vectorFourFieldsShortLive` is bimodal too); what helps is not putting four outputs in one
method at a narrow width, which is why the four-node baseline is stable in all 21 runs.

**The effect is observable at runtime, with public API.** JFR's `jdk.Compilation` event
carries `method`, `compileLevel`, `isOsr` and - the useful one - `codeSize`, so a
`jdk.jfr.consumer.RecordingStream` can watch the compiled size of Varka's own generated kernel
methods as they are compiled, with no agent and no diagnostic flags. The fast and slow
allocations differ by roughly 2x in code size here, so the anomaly is visible in that number.
And because every kernel is emitted into a fresh class, "recompile it" is available too: emit
the same shape again under a new class name and the allocator gets a fresh roll.
That makes a detect-and-resample loop *possible*; it does not make it wise. Each resample costs
another class, another compile and another warm-up, against a shape whose kernel a short query
may only run a handful of times, and the detection needs a per-shape expected size that will
drift. Prefer the structural fix - do not put four outputs in one method at a width whose
register file cannot hold them - and use the JFR signal as a *diagnostic*, so that a
badly-allocated kernel is reportable rather than invisible, instead of as a control loop.

**Update, task 32 step B2: the "structural fix" above turns out to be a property of this
one hand-written kernel's bytecode, not of "four outputs in one method at 128-bit."** Once
the emitter's own fragment mechanism made the *real* shared-loop-method shape buildable
(`VarkaEmitOptions.withGroupBudget(200)`, four calendar outputs genuinely fused into one
generated method - the same four-output-one-method shape this section just said to avoid),
it was measured at 128-bit across three separate runs and showed **no bimodality at
all** - stable to a few milliseconds every time, at every field count from two to four. The
two hand-written kernels this task also built (`ChronoVectorOps.vectorFourFields`, the
"ceiling", and `vectorFourFieldsNoValidity`) remain bimodal on the same three runs, one of
them in a new flavor - flipping between fast and slow *within* a single run's iterations
rather than settling into one mode for the run's duration (`PLAN_TASK_32.md` section 7.4).
So: a register allocation this fragile is a property of one specific compiled method's
bytecode (here, `javac`'s output for a hand-written 936-byte body), not an inherent cost of
the technique it demonstrates. Do not generalize "four outputs in one method is unsafe at
128-bit" from a single hand-written kernel to the emitter's own generated bytecode for the
same shape - measure the actual generated path before declining a design on this basis.

**You do not need to run the fastdebug JVM to read product assembly.** HotSpot's fourth
fallback for locating the disassembler is `hsdis-<arch>.so` on `LD_LIBRARY_PATH`
(`disassembler.cpp`), so a `libhsdis.so` built once against a fastdebug tree can be copied to
`hsdis-amd64.so` and used with the *product* JVM:

```
LD_LIBRARY_PATH=<dir with hsdis-amd64.so> java -XX:+UnlockDiagnosticVMOptions \
    -XX:CompileCommand=print,<class>::<method> ...
```

That matters because some behaviour only appears in the product build - this bimodality among
it - so being able to disassemble there rather than only in fastdebug is what made the
comparison above possible at all.

## Building a fastdebug JDK for HotSpot diagnostics

Three questions in milestone 4 could not be answered from a product JVM - why SuperWord
declined a loop, what C2 actually emitted, and which of two compilations a bimodal kernel had
landed on. The flags that answer them (`TraceSuperWord`, `PrintOptoAssembly`, and a
`PrintAssembly` that can actually disassemble) are `develop` flags or need `hsdis`, and neither
ships in a product build. No prebuilt debug JDK exists on `jdk.java.net` for any version, so
building one is the only route. It takes about three and a half minutes on 24 cores.

```
git clone --depth 1 --branch jdk-25-ga https://github.com/openjdk/jdk.git
bash configure --with-debug-level=fastdebug --enable-headless-only \
    --with-hsdis=capstone --with-boot-jdk=$JAVA_HOME \
    --with-jobs=24 --disable-warnings-as-errors
make images build-hsdis
cp build/*/support/hsdis/libhsdis.so build/*/images/jdk/lib/server/
```

Four things that cost time and are not obvious:

* **`--enable-headless-only` does not remove the X11 or cups build dependencies.** It affects
  the runtime, not what configure demands. The full Ubuntu list is still needed: `autoconf`,
  `libx11-dev`, `libxext-dev`, `libxrender-dev`, `libxrandr-dev`, `libxtst-dev`, `libxt-dev`,
  `libcups2-dev`, `libfontconfig1-dev`, `libfreetype-dev`, `libasound2-dev`, and
  `libcapstone-dev` for hsdis.
* **Ubuntu 26.04 ships `uutils coreutils` as `date`, and it breaks the build.** OpenJDK's
  configure decides GNU-ness with `date --version | grep "GNU\|BusyBox"`
  (`make/autoconf/basic_tools.m4`). uutils names itself differently while behaving
  GNU-compatibly, so configure falls back to the BSD `date -u -j -f` form, produces an empty
  `SOURCE_DATE_ISO_8601`, and the build later fails packaging `jrt-fs.jar` with
  `option --date requires an argument`. Passing `--with-source-date=<ISO string>` does not help
  - it is rejected as unparseable by the same broken path. The fix is a shim earlier in `PATH`
  that answers `--version` with a string containing "GNU" and `exec`s `/usr/bin/date` for
  everything else.
* **`make images` does not build hsdis.** It needs the separate `build-hsdis` target, and the
  result is left in `support/hsdis/libhsdis.so`. HotSpot looks for it beside `libjvm.so`, so it
  has to be copied to `images/jdk/lib/server/` - putting it in `images/jdk/lib/` is not enough
  and yields `Loading hsdis library failed` with no further explanation.
* **The flag is `TraceSuperWord`, not `TraceAutoVectorization`**, in JDK 25 - check
  `src/hotspot/share/opto/c2_globals.hpp` rather than trusting a flag name from a newer
  release.

**Never take a number from this JVM.** fastdebug keeps the assertions, so absolute throughput
is not comparable with the product build and nothing measured on it belongs in a committed
results file. It is for reading what C2 did, not how fast it did it.
