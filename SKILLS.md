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
- Spark's own `ColumnarToRowEvaluator` avoids this by streaming rows for immediate
  consumption. When materializing (our evaluator does `process(...).toArray`), call
  `.copy()` per row: `input.rowIterator().asScala.map(r => proj(r).copy()).toArray`.
- Same fix required on the kernel path (`resultBatch.rowIterator().asScala.map(r =>
  toRow(r).copy()).toArray`).

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

## Build Gotchas

- The engine module is built with Maven, not sbt: `./build/mvn -o -f
  sql/varka/engine/pom.xml install -DskipTests`. `mvn` is not on `PATH`; use
  `./build/mvn`.
- sql/core test-scope depends on the engine jar from `~/.m2`; after editing engine
  sources, rebuild + reinstall or the test classpath keeps the old bytes.