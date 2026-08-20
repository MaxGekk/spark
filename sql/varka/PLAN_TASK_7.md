# Varka Task 7 - Differential + perf testing

**Status: PLAN** (implementation follows). See `IMPLEMENTATION_PLAN.md` for the
high-level MVP plan. Task 7 proves the Task 6 integration differentially
(Varka on/off answer equality over a wider query matrix), bounds the per-task
classloader Metaspace footprint (the engine's `VarkaClassLoaderTest` covers the
engine loader; the catalyst-side loader gets the same guarantee), and adds
performance benchmarks at two levels: a JMH kernel microbenchmark in the
standalone engine module, and Spark's `BenchmarkBase`/`Benchmark` harness for
end-to-end throughput and class-generation (Gen-time) time.

## 1. Goal

- **Differential**: a `QueryTest` suite (`VarkaDifferentialSuite`) that runs a
  query matrix over an Arrow-cached source with `spark.sql.codegen.varka.enabled`
  on vs off and asserts `checkAnswer` equality, and (where the plan is fused)
  that `numVarkaBatches > 0` proves the kernels actually ran.
- **Metaspace**: a catalyst-side `VarkaGeneratedClassLoaderSuite` mirroring the
  engine's unloadability proof (weak-reference collection after `release`, a
  1000-loader batch stress with `MetaspaceUsed` before/after), plus a lenient
  integration check that running many Varka tasks keeps Metaspace bounded.
- **Perf**: engine-level JMH benchmarks of `vectorAddDays`/`vectorSubDays`/
  `vectorDateDiff` vs scalar loops (Task 2's deferred follow-up), and
  Spark-side benchmarks of end-to-end query throughput and class-generation
  time (Varka assembly vs Janino compile).
- Validation: `checkAnswer` equality; throughput/Gen-time metrics.

## 2. Decisions (recorded here)

- **Two benchmark harnesses.** Spark has no JMH integration in this tree (no
  `jmh` module, no plugin); all in-tree benchmarks use `BenchmarkBase` +
  `Benchmark` (run via `test:runMain`, results to `benchmarks/`). The engine
  module is a standalone Maven module (Java 25, native-access flags already
  configured), so JMH is added *there* for the kernel microbenchmark, while the
  end-to-end cases use Spark's own harness. This matches Task 2's "JMH vs
  scalar loop (follow-up)" note.
- **JMH is isolated to the engine pom.** New deps (`jmh-core`,
  `jmh-generator-annprocess`) and the `maven-jmh-plugin` live only in
  `sql/varka/engine/pom.xml`; `mvn test` is unaffected (JMH runs via
  `jmh:benchmark` only).
- **Differential suite structure.** A new `VarkaDifferentialSuite` (not
  extending `VarkaEndToEndSuite`), with the shared 3-session setup extracted
  into a reusable `VarkaSharedSessions` trait used by both suites.
- **Metaspace integration check is lenient.** GC/collection timings are JVM
  dependent, so the deterministic guarantee lives in the unit loader suite;
  the integration check uses generous thresholds and only asserts bounded
  growth, to avoid CI flakiness.

## 3. Deliverables

### 3.1 Shared session trait (`VarkaSharedSessions`, sql/core test)

Extract from `VarkaEndToEndSuite`:

- `sparkConf`: `SPARK_CACHE_SERIALIZER` = `ArrowCachedBatchSerializer`,
  `CACHE_VECTORIZED_READER_ENABLED`.
- `beforeAll`/`afterAll`: `InMemoryRelation.clearSerializer()` around session
  creation; three sessions on the shared context (`spark` base, `varkaSpark`
  with `VARKA_ENABLED=true` + rule, `disabledSpark` with `false` + rule); AQE
  off on the custom sessions; `clearActiveSession`/`clearDefaultSession`
  between creations.
- Helpers: `cacheDates(session)`, `cacheDatePairs(session)`, `assertFused`,
  `assertNotFused`, `assertKernelsRan`.

`VarkaEndToEndSuite` is refactored onto the trait with no behavior change.

### 3.2 `VarkaDifferentialSuite` (`QueryTest` + `VarkaSharedSessions`)

Each test caches the source on `spark` (expected) and `varkaSpark` (actual),
runs the same query, and asserts `checkAnswer` equality; where the plan is
fused it also asserts `numVarkaBatches > 0`. Matrix:

1. `date_add`/`date_sub` with literal offsets {0, 3, -5, 100,
   `Int.MaxValue - 1`, `Int.MinValue`}.
2. `datediff` in both argument orders; nulls mixed / all-null / null-free.
3. Mixed-eligibility projections (`date_add(d, 3)`, `i`, `i + 1`) - not fused
   but equal.
4. Foldable offsets: `date_add(d, 1 + 2)`, `date_add(d, cast(3 as int))`.
5. Composed expressions: `date_add(date_add(d, 1), 2)`,
   `date_sub(date_add(d, 5), 5)`.
6. Filter + `ORDER BY` + `GROUP BY`/aggregation (`max(date_add(d, 1))`).
7. Multi-batch: `spark.sql.inMemoryColumnarStorage.batchSize` = 32 over ~1k
   rows; kernels run across all batches.
8. Multi-task: `repartition(n)` forces several tasks (one loader per task);
   results equal.
9. Non-Arrow source (vectorized parquet -> `OnHeapColumnVector`): varka on vs
   off equal via the fallback; plan not fused.

### 3.3 Metaspace

- `VarkaGeneratedClassLoaderSuite` (ScalaTest, catalyst test, `SparkFunSuite`):
  define/registry/`release` idempotency, unloadability after release
  (WeakReference + ReferenceQueue + GC-retry), 1000-loader batch stress
  asserting every loader is collected with `MetaspaceUsed` logged before/after.
- Integration check (in `VarkaDifferentialSuite`): run N tasks x M queries
  through the fused node, assert results correct and Metaspace delta bounded
  after GC (lenient).

### 3.4 Benchmarks

Engine JMH (`sql/varka/engine`):

- pom: add `jmh-core`, `jmh-generator-annprocess`, `maven-jmh-plugin`; reuse
  the native-access `argLine`; JMH >= 1.38 (Java 25).
- `src/jmh/java/org/apache/spark/sql/varka/vector/DateVectorOpsBenchmark.java`:
  the three kernels vs scalar-loop baselines over Arrow `DateDayVector`
  (sizes ~10k and 1M; null-free and mixed-null); report speedup vs scalar
  (MVP target >= 4x).
- Run: `./build/mvn -f sql/varka/engine/pom.xml jmh:benchmark`.

Spark end-to-end (sql/core test, `BenchmarkBase` + `Benchmark`):

- `VarkaThroughputBenchmark`: ~2M-row Arrow-cached dates; cases baseline
  (Janino) vs Varka-on for `date_add`/`date_sub`/`datediff` and a mixed
  projection (fallback); rows/sec; `SPARK_GENERATE_BENCHMARK_FILES=1` writes
  `benchmarks/VarkaThroughputBenchmark-jdk<NN>-results.txt`.
- `VarkaCodegenBenchmark` (Gen-time): per-op class generation - case "janino"
  (`GenerateUnsafeProjection.create`/`CodeGenerator.compile`) vs "varka"
  (`VarkaClassFileGen.assembleKernelClass` +
  `VarkaGeneratedClassLoader.defineGeneratedClass`); ns/op.
- Run: `build/sbt "sql/test:runMain org.apache.spark.sql.execution.VarkaThroughputBenchmark"`.

## 4. File layout

```
sql/core/src/test/scala/org/apache/spark/sql/execution/
  VarkaSharedSessions.scala           (new trait; session + assertion helpers)
  VarkaDifferentialSuite.scala        (new differential query matrix)
  VarkaThroughputBenchmark.scala      (new end-to-end rows/sec benchmark)
  VarkaCodegenBenchmark.scala         (new Gen-time benchmark)
  VarkaEndToEndSuite.scala            (refactored onto VarkaSharedSessions)
sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/codegen/
  VarkaGeneratedClassLoaderSuite.scala (new Metaspace/unloadability tests)
sql/varka/engine/
  pom.xml                              (+ jmh deps + maven-jmh-plugin)
  src/jmh/java/org/apache/spark/sql/varka/vector/
    DateVectorOpsBenchmark.java        (new kernel microbenchmark)
sql/varka/PLAN_TASK_7.md               (this file)
sql/varka/IMPLEMENTATION_PLAN.md       (task 7 row update)
```

## 5. Verification

- Engine: `./build/mvn -f sql/varka/engine/pom.xml test` (existing 23 tests
  still green; JMH runs only via `jmh:benchmark`).
- Catalyst: `JavaClassFileEngineSuite`, `ClassFileCodegenSupportSuite`,
  `VarkaGeneratedClassLoaderSuite`.
- sql/core: `VarkaColumnarToRowExecSuite`, `VarkaEndToEndSuite`,
  `VarkaDifferentialSuite`.
- `build/sbt scalastyle`; ASCII and <=100-char scan on changed files.
- Benchmarks smoke-run with minimal iterations to confirm they execute (not
  full perf runs).

## 6. Definition of done (Task 7)

- Differential suite green: every query in the matrix answers the same with
  Varka on and off; fused plans report `numVarkaBatches > 0`.
- Loader suite proves the catalyst-side loader is collected after release and
  survives a 1000-loader batch without Metaspace growth.
- Engine JMH benchmark compiles and runs; Spark benchmarks run via
  `test:runMain` and emit results.
- Full regression (engine + catalyst + sql/core) and style clean.
- `IMPLEMENTATION_PLAN.md` task 7 marked DONE.