---
layout: global
title: Varka - SIMD Date Arithmetic over Arrow
displayTitle: Varka - SIMD Date Arithmetic over Arrow
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

Varka is a research/experimental execution engine inside this Spark fork. It
compiles whole date-expression projections into a single SIMD vector loop -
bytecode emitted with the JDK 25 Class-File API, running the Vector API over
zero-copy Panama `MemorySegment` views of Arrow `DateDayVector` buffers -
bypassing Spark's per-row code generation on the happy path.

## Overview

The Spark SQL runtime normally executes expressions by generating Java source,
compiling it with Janino, and running one expression evaluation per row. Varka
eliminates that runtime compilation overhead (string generation and Janino
parsing) and unlocks SIMD by operating on whole columnar batches at once.
Since milestone 2 it does not dispatch to per-op kernels: an eligible
projection is compiled to a vector IR and *fused* - however many expressions
and however deep their nesting, the emitted class runs one loop with one load
per input column and one store per output.

The supported expression surface, over `DateType` columns (stored as `INT`
days since epoch) and foldable integer day offsets:

* `DATE_ADD` / `DATE_SUB` / `DATEDIFF`, nested to any depth up to the
  emitter's chain cap, including chains mixing them.
* `CASE WHEN` and `IF` over date comparisons (`<`, `<=`, `>`, `>=`, `=`) and
  their `AND` / `OR` / `NOT` combinations - executed branch-free by mask
  blend, with SQL's three-valued null semantics. `BETWEEN` arrives from the
  optimizer as its paired comparisons and fuses the same way.
* `IN` over date literals in condition position (task 20): an EQ chain
  joined by OR, capped at 16 deduplicated literals - a longer list declines
  with a recorded reason rather than risking the emitter's budgets.
* `COALESCE` / `NVL` / `IFNULL` / `NVL2` and the `IS [NOT] NULL` predicates
  (task 20), lowered onto a validity-reading condition. Every guarded
  operand must be a bare date column; a non-column operand declines.
* `GREATEST` / `LEAST` (null-skipping) and `DAYOFWEEK` / `WEEKDAY`.
* Common subtrees shared *across* outputs are computed once per lane group
  (DAG-CSE), which no per-row engine can keep in a vector register.

A projection does not have to be fully eligible: eligible entries fuse,
untouched input columns are forwarded zero-copy, and the remaining entries run
the standard row path per row, merged with the kernel outputs (task 12).

Explicitly out of scope are `CalendarInterval` (months/years), strings,
decimals and nested/complex types. Only integer day offsets are supported.

Varka is designed as a drop-in, zero-risk replacement: every Varka path falls
back to the standard row engine on any failure, so results are always correct.

## Architecture

### Columnar morsels

Spark's `DateType` columns reach Varka as Arrow `DateDayVector` (int32 days)
with a bit-packed validity buffer (1 bit per row, bit set = valid). A morsel
maps the data and validity buffers onto zero-copy Panama `MemorySegment`s, so
no heap objects are built per row:

    data segment     -> bytes of `4 * rowCount`
    validity segment -> bytes of `(rowCount + 7) / 8`, only when the column has
                        neither no nulls nor is fully null

The validity buffer is bit-packed. A byte-per-lane read would be a correctness
bug; the kernels instead load a `long` and build a `VectorMask` with
`VectorMask.fromLong`.

### The emitted fused loop

The live compute path since milestone 2. `VarkaExpressionCompiler` translates
an eligible projection into a small vector IR (`VarkaVectorIR` - column refs,
literal slots, arithmetic, comparisons, conditionals); `VarkaLoopEmitter`
assembles a class implementing `VarkaFusedKernel` from it with the Class-File
API - no Java source, no Janino, no external bytecode library. The emitted
class has a deliberate method anatomy:

* A per-batch dispatch picks one of two twin bodies: a *dense* body with
  unmasked loads and stores when every input is null-free (measured 2.3-2.9x
  the masked body in task 10), and a *masked* body that builds a
  `VectorMask` per lane group from the bit-packed validity words otherwise.
* The vector walk is split into sibling loop methods of at most
  `GROUP_BUDGET` (16) IR nodes each, one output group per method, plus a
  shared scalar-tail method. Separate methods, not one big loop: each gets
  its own C2 compilation, so no method's inlining budget can starve
  another's intrinsics - task 10 measured 3-4x on exactly that cliff.
* Interned subtrees (DAG-CSE) are computed once per lane group and reused
  across outputs; literals are hoisted to broadcast vectors in the prologue.
* Caps: chains up to `MAX_CHAIN_DEPTH` (16) deep, up to `MAX_FUSED_NODES`
  (64) distinct ops and `MAX_INPUTS` (64) input columns per kernel; anything
  beyond falls back.

Int32 arithmetic wraps on overflow, matching Spark's `DateAdd`/`DateSub`
non-ANSI semantics. The scalar tail mirrors the vector body row for row and
handles the remainder lanes.

The milestone-1 per-op kernels (`DateVectorOps.vectorAddDays` and friends)
remain in the engine as reference code and as the differential oracle for the
emitter's tests. The per-op dispatcher machinery they were once called through -
the `ClassFileCodegenSupport` trait, the `VarkaClassFileGen` assembler and the
kernel-shape interfaces - was retired in task 17, along with the
`CodeAndComment` cache-key field it fed (`PLAN_MILESTONE_2.md` section 8).

### The shape cache, class loaders and Metaspace

Task 14 measured the cost of the original per-task lifecycle: every task
defined a fresh class, so HotSpot re-ran the whole tier ladder - interpreter,
C1 with boxed vectors, C2 OSR - a fixed 13-50 ms per task that emission (~80
us) never was. Since task 18 the loaded class is shared instead:
`VarkaShapeCache` is a JVM-wide LRU keyed on the kernel's structural shape -
the IR (whose literal slots carry indices, never values), the input count and
the literal count, exactly the inputs the emitted bytes are a function of -
so tasks and sessions computing the same shape reuse one class, C2 code and
all. Each class lives in its own `VarkaClassLoader`, `release()`d when the
cache evicts it; once the last running task drops its reference the JVM
unloads the class, so Metaspace is bounded by the cache capacity rather than
by churn (Spark's codegen cache never releases a loader) or by task lifetime
(the pre-task-18 contract, still available at capacity 0). A registry +
`findClass` mirror Spark's `InMemoryClassLoader`.

### Null semantics and predication

The vector loop cannot branch per row, so SQL's null and conditional
semantics are implemented in mask algebra; `PLAN_MILESTONE_2.md` section 2.6
is the normative statement of the rules. In brief:

* Arithmetic is null-intolerant: an output row is valid only where every
  referenced input is valid, tracked as per-lane-group validity words.
* Comparisons and `AND`/`OR`/`NOT` follow three-valued logic as a
  *known-true / known-false* mask pair (`unknown` is neither), so
  `null AND false = false` comes out right without a branch.
* `IF`/`CASE WHEN` execute *all* arms and pick per lane with
  `VectorMask.blend` - branch-free, so data-dependent conditions cost the
  same as predictable ones (the throughput benchmark prices this).
* `GREATEST`/`LEAST` skip null operands (Spark semantics) rather than
  propagating them.
* `DAYOFWEEK`/`WEEKDAY` lower `Math.floorMod(d, 7)` branch-free: two 15-bit
  digit-sum folds (`2^15 = 1 mod 7`) narrow the value until Granlund-Montgomery
  magic division by 7 is exact in the low 32 bits, with no final fixup - the
  full-range multiply-high the classic trick needs does not exist in the
  Vector API, but pre-folding makes the low half sufficient. The task 11
  six-fold digit sum and the lanewise-DIV lowering are kept as reference
  variants (the parity benchmark's dayofweek section prices all three).

### Telemetry and debuggability

Every emitted class is self-describing (task 13, reconciled with sharing in
task 18): a `SourceFile` attribute named for the shape
(`VarkaFusedProjection_<hash>.java`, 16 hex chars of the shape's SHA-256), so
stack traces, profilers and heap dumps name the kernel with no mapping table,
and a `VarkaDebugInfo` custom attribute carrying the vector IR and the shape
identity. The class is shared across tasks, so the per-execution identity -
operator, stage, the projection list - is not in the bytes: the cache records
it per lookup in a bounded side table, and
`VarkaShapeCache.executionsFor(hash)` joins a shape name seen in a profile
back to the plan nodes that ran it. Task 16 extends that to the questions the
attributes alone did not answer:

* **Bytecode maps back to IR nodes.** The class carries a `LineNumberTable`
  whose line `n` is the `n`-th IR node in topological order, and
  `VarkaDebugInfo` records the decoding key (`<line>=<node>` per line). A
  stack frame reading `VarkaFusedProjection_<hash>.java:7` therefore names the node
  that threw, not merely the method - and profilers and crash logs inherit
  the same resolution for free.
* **Fallbacks name their kernel.** Every warning on the ghost-fallback path -
  emission failure and per-batch kernel failure, in both exec nodes - carries
  the kernel's `SourceFile` name, the IR it computes and the operator/stage it
  served, so a log line identifies the plan node without correlating
  timestamps.
* **The class reaches disk.** `spark.sql.codegen.varka.classDumpDirectory`
  writes each emitted class under its `SourceFile` name, so `javap -c -p`
  disassembles a generated loop with no debugger attached. Diagnostics only:
  a failed write is logged and never fails the query.
* **`EXPLAIN` says why an entry did not fuse.** Verbose `EXPLAIN` on either
  Varka node lists every projection entry as fused, forwarded (naming the
  child column) or residual with the compiler's decline reason - "unsupported
  expression", "day offset is not a foldable literal", "CASE WHEN without an
  ELSE branch", "non-date column of type ..." - in the query's own column
  names. The same account goes to the debug log once per task.

All of it is metadata or diagnostics: the emitted methods are byte-identical
with and without the attributes, which the JVM ignores by specification.
`VarkaDebugInfoReader` turns captured class bytes back into those strings.

### Execution integration

`VarkaColumnarRule` (a `ColumnarRule`) rewrites a Varka-eligible projection
(at least one fusable entry) over a columnar source when
`spark.sql.codegen.varka.enabled` is set.
It works in two stages, on either side of Spark's transition insertion, because
which node belongs in the plan depends on what the consumer above wants:

    // preColumnarTransitions: columnar in, columnar out
    ProjectExec(projectList, columnarChild)
      -> VarkaProjectExec(projectList, columnarChild)

    // postColumnarTransitions: a to-row transition that was inserted anyway is fused in
    ColumnarToRowExec(VarkaProjectExec(projectList, child))
      -> VarkaColumnarToRowExec(projectList, child)

A consumer that takes batches - a DSv2 write whose connector declares
`supportsColumnarWrite`, such as `noop` - therefore receives the kernels' own
Arrow batches with no transition at all, while a row consumer gets the single
fused node. The rule is registered on every `SparkSession` but is inert while
the config is off.

Both nodes run the same kernels through `VarkaKernelEvaluator`, and differ only
in what they do with its output batch and in how they fall back:
`VarkaColumnarToRowExec` converts to rows and, when the kernels cannot serve a
batch, projects the input's rows one by one; `VarkaProjectExec` passes the batch
on and has to materialise its fallback into a writable batch instead.

Per task and Arrow-supported batch:

1. Bind the projection and compile it with `VarkaExpressionCompiler` into
   fused entries (the IR), forwarded entries (bare input columns, passed
   through zero-copy) and residual entries (everything else).
2. Look the fused shape up, lazily, in the JVM-wide class cache
   (`VarkaShapeCache`, task 18): a miss emits and defines the class - named
   by its shape hash - in its own `VarkaGeneratedClassLoader`, and every
   task (or session) computing the same shape reuses the loaded class, C2
   code and all, skipping the fixed per-task JIT warm-up. The literals never
   enter the shape, they travel as runtime arguments - so one class serves
   every batch of every task of the shape.
3. Guard per batch that every referenced column is an `ArrowColumnVector`
   backed by a `DateDayVector`; otherwise the batch takes the per-row path.
4. Run the kernel: one vector loop writes every fused output into freshly
   allocated Arrow vectors. Forwarded columns are re-wrapped, not copied.
   Residual entries are evaluated per row and merged - at-row on the
   row-consumer node (the escape hatch task 12 measured both ways), into a
   writable batch on the columnar one.
5. Track `numVarkaBatches`, which only counts batches where the kernels
   succeeded, and the class-cache hit/miss per task. A class's loader is
   released when the bounded cache
   (`spark.sql.codegen.varka.cache.maxEntries`, default 100) evicts it, so
   Metaspace is bounded by cache capacity; running tasks keep their instance
   until they finish. The Janino fallback projection is compiled lazily,
   only if a batch actually needs it (task 15).

Neither node is `CodegenSupport`; whole-stage codegen splits at the boundary
with the columnar producer. They depend on the engine only by kernel
descriptors (strings), so a missing engine jar degrades to the fallback.

## Key design decisions

* **Java 25 baseline and a self-contained engine.** The Vector API and the
  Class-File API require a recent JDK. `sql/varka/engine` is a module of the
  Spark reactor, so a plain `./build/mvn install` builds it, but it keeps its
  own pom rather than inheriting `spark-parent` so its sources and tests can use
  the incubator-vector and native-access flags the Spark build does not set;
  catalyst uses `java.lang.classfile` on the Java 25 baseline.
* **Arrow-only fast path.** Arrow-backed batches (for example the Arrow cache
  serializer) map directly to segments. Vectorized Parquet produces
  `OnHeapColumnVector`/`OffHeapColumnVector`, not Arrow, so those batches fall
  back per batch.
* **Plan-level interception.** The rewrite happens in a `ColumnarRule` rather
  than by editing `ColumnarToRowExec` itself, and it straddles Spark's
  transition insertion: the projection becomes columnar-out before transitions,
  and a transition inserted above it is fused back in afterwards. That way the
  decision of whether rows are needed at all stays Spark's.
* **Ghost fallback and caching.** Any assembly or load failure lazily routes to
  Janino; the winning path is cached under the same key so a failed assembly is
  never retried and the job never crashes.
* **Extreme-offset oracle.** At `INT` overflow (`Int.MaxValue - 1`,
  `Int.MinValue`) the differential oracle is the plain int32 day wrap that
  `DateAdd.eval` and the kernels implement; Spark's end-to-end row engine adds a
  calendar-day rebase for out-of-range `DATE` results.
* **No unused configuration.** Every `spark.sql.codegen.varka.*` entry must be
  consumed. Today `enabled`, `classDumpDirectory` and `cache.maxEntries`
  exist, and each is read on the execution path that documents it.

## Module and file layout

| Location | Responsibility |
| :--- | :--- |
| `sql/varka/engine` | Standalone Java 25 module (`varka-engine`, Arrow 19.0.0): `VarkaMorsel`, `DateVectorOps`, `VarkaClassLoader` and their tests. |
| `sql/catalyst` | The vector IR, loop emitter and telemetry attribute under `codegen/varka/`; `VarkaExpressionCompiler`; `VarkaGeneratedClassLoader`; `DateVarkaSupport`'s day-offset folding; the Varka configs. |
| `sql/core` | `VarkaColumnarRule`, `VarkaColumnarToRowExec`, end-to-end test suites and benchmarks. |
| `sql/varka` | `VISION.md`, `Varka_MVP.md`, and `plans/` with the milestone plans (`PLAN_MILESTONE_1.md` is the MVP) and per-task plans. |

## Configuration

All Varka configurations are internal:

| Config | Default | Description |
| :--- | :--- | :--- |
| `spark.sql.codegen.varka.enabled` | `false` | When true, an eligible projection (at least one fusable entry) over Arrow `DateDayVector` columns runs the fused SIMD kernel instead of per-row codegen - as `VarkaProjectExec` where the consumer takes batches, and as `VarkaColumnarToRowExec` where it wants rows; ineligible entries run the row path per row and merge, and non-Arrow batches fall back entirely. |
| `spark.sql.codegen.varka.classDumpDirectory` | (none) | Diagnostics (task 16). When set, every emitted kernel class is written to this directory under its `SourceFile` name, for `javap`. A failed write is logged and never fails the query; every task of a shape holds identical bytes and overwrites one file. |
| `spark.sql.codegen.varka.cache.maxEntries` | `100` | Static (task 18). Capacity of the JVM-wide cache of loaded fused-kernel classes, keyed on the kernel's structural shape; the least recently used class is released on eviction, bounding Metaspace by this size. `0` restores the per-task emit-and-unload lifecycle. |

The rule is registered on every `SparkSession` but does nothing while the
config is off, so enabling the config is all that is needed:

```scala
val spark = SparkSession.builder()
  .appName("app")
  .config("spark.sql.codegen.varka.enabled", "true")
  // Arrow cache is the recommended production source of DateDayVector batches.
  .config("spark.sql.cache.serializer",
    "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer")
  .getOrCreate()
```

The Arrow fast path reads cached batches through the in-memory columnar
vectorized reader (`spark.sql.inMemoryColumnarStorage.enableVectorizedReader`,
`true` by default), so caching with the Arrow serializer produces
`ArrowColumnVector` `DateDayVector` batches. Without an Arrow-backed source
Varka silently uses the row engine for every batch and results stay correct.

The VISION draft also describes `spark.sql.codegen.varka.patch.threshold` and
`spark.sql.codegen.varka.fallback.ghost.enabled`. They are design intentions,
not configuration entries in this MVP: per the project rule ("no unused
config"), they will be added to `SQLConf` only when the code paths they gate
exist.

## Testing and benchmarks

* **Engine differential tests** cross-check every kernel against Arrow's own
  vector accessors, including null patterns, empty batches and offsets near
  `Integer.MAX_VALUE`.
* **Catalyst tests** check bytecode shape by disassembly, the loader
  define/release lifecycle, and ghost-fallback injection.
* **sql/core tests** (`VarkaColumnarToRowExecSuite`, `VarkaProjectExecSuite`,
  `VarkaColumnarWriteSuite`, `VarkaEndToEndSuite`,
  `VarkaDifferentialSuite`, `VarkaAutoRegistrationSuite`) prove plan fusion,
  `checkAnswer` equality over a query matrix - re-run warm so a cache hit is
  differentially checked too - `numVarkaBatches > 0` on fused plans,
  Metaspace bounds, and config-driven activation.
* **Metaspace proof** (`VarkaGeneratedClassLoaderSuite`,
  `VarkaShapeCacheSuite`) verifies with weak references that a released
  loader is collected, that a batch of 1000 loaders is fully collected, and -
  since task 18 - that a 10k-distinct-shape stress stays at cache capacity
  with every evicted loader collected.

Benchmark highlights from the committed runs - the throughput file from
task 19's run, which extended the row-consumer matrix with heavy-op twins;
cold start and class generation from task 18 (AMD Ryzen AI 9 HX PRO 370,
JDK 25, Linux, machine otherwise idle; every number below is the best of at
least five two-second-windowed iterations and lives in the committed results
files, which are the source of truth as the code moves):

* **End-to-end columnar throughput** over 2M Arrow-cached rows
  (`VarkaThroughputBenchmark`): 3.8-5.7x Janino for single ops and small
  trees (`date_add` 3.8x, `datediff` 5.6x, the nested
  `datediff(date_add(d, 1), d2)` 5.7x, the two-output shared subchain 5.7x),
  2.5x for a mixed projection where only one entry fuses. Before the class
  cache these read 1.7-2.3x: the per-task JIT warm-up was most of the gap
  between the buffer-level kernels and the end-to-end numbers.
* **`CASE WHEN` by mask blend**: 7.1x on data where the condition flips
  pseudo-randomly, 5.8x where it is perfectly predictable. The varka side
  costs the same on both within a millisecond (branch-free execution is
  data-oblivious, 8-9 ms best in the committed cases); the gap is Janino's
  branch misprediction on the unpredictable data.
* **Chain depth** (alternating `date_add`/`date_sub`, columnar consumer):
  7.0-7.5x, *flat* from depth 1 to depth 8. Task 14 committed this curve as
  2.2x eroding to 1.3x and diagnosed the erosion as the fixed per-task JIT
  warm-up that grew with the loop method's op count (`PLAN_TASK_14.md` 7.5);
  task 18's cross-task class cache removed exactly that term - every task
  now runs the C2-compiled loop from its first row - and the end-to-end
  curve became what the buffer-level numbers always predicted (depth 8
  within 10% of depth 1).
* **The row-consumer cost, stated plainly**: assemble-then-read costs a
  flat ~25 ns/row on an all-fused single-output projection, whatever the
  fused work - task 19's extended matrix pinned the floor (the ~16 ns/row
  previously quoted was contaminated by the pre-cache JIT warm-up). Fusion
  through rows wins exactly where Janino's own per-row cost exceeds that
  floor: `dayofweek` 1.2x and unpredictable `CASE WHEN` 1.1x, against the
  cheap chains at 0.8x (Janino ~20 ns/row) and residual-heavy at 0.6x.
  There is no break-even depth, and no plan-time cost gate separates the
  winners from the losers - an 8-op chain loses while the ~6-op `CASE WHEN`
  wins, because the differencer is Janino's cost, not Varka's - so the rule
  keeps fusing row consumers: task 19's recorded decision.
* **`dayofweek`**: 9.8x - 7 ms against Janino's 65 ms, the largest committed
  win, and the shape that pays even through a row consumer (1.2x there). This case shipped as the honest loss of the original task 14 run
  (0.9x, the magic-multiply lowering of 7.7 took it to 1.2x): its ~12-op
  loop method paid the heaviest per-task warm-up (~50 ms), so removing the
  ladder moved it furthest.
* **Cold start** (`VarkaColdStartBenchmark`, first execution of a fresh plan
  shape over 100K rows): 1.7x - 19 ms vs 31 ms best, 25 ms vs 38 ms average.
  A fresh shape misses the class cache, and the benchmark enforces that by
  invalidating the cache before each timed iteration - its column-and-literal
  freshness is invisible to the structural shape key. The varka side pays
  emission plus the class define here, essentially the per-task era's 18 ms;
  only repeated shapes get the cache's win.
* **Class generation in isolation** (`VarkaCodegenBenchmark`): emitting,
  defining, loading and instantiating a fused two-output kernel takes
  ~130 us against ~9 ms for one Janino projection compile - 68x. (The
  milestone-1 single-op dispatcher case that used to sit beside it, at
  ~420x, went with the dispatchers in task 17.)

## Deployment and requirements

* JDK 25 with the incubator Vector API module:
  `--add-modules jdk.incubator.vector`
  `--enable-native-access=ALL-UNNAMED`
* The engine jar in the repo is a test-scoped dependency; at runtime supply it
  with `--jars` (its absence only falls back to per-row execution).
* Arrow `DateDayVector` buffers come from Arrow-backed producers; the Arrow
  cache serializer is the recommended source.

## Limitations

The real current edges, stated with their numbers where they have one:

* **Int32 lanes only.** The IR carries one lane type; every supported
  expression is `INT`-shaped (`DateType` days or integer results). No
  `CalendarInterval`, strings, decimals, timestamps or nested types, and only
  foldable integer day offsets.
* **ANSI arithmetic over `datediff` outputs is excluded by design**: an
  integer `Add` over a `datediff` result is not a date expression, and ANSI
  overflow cannot throw row-accurately from a SIMD lane, so such entries stay
  residual.
* **The row-consumer read-back can cost more than fusion saves**: cheap
  chains commit at 0.8x and residual-heavy at 0.6x, while heavy shapes win
  through rows (`dayofweek` 1.2x, `CASE WHEN` 1.1x) - the ~25 ns/row
  assemble-then-read floor decides which (`VarkaThroughputBenchmark`).
  Task 19 measured both sides and recorded the acceptance: no decline rule,
  because no plan-time number separates the shapes and task 21's filters
  keep more output columnar.
* **Vectorized Parquet falls back**: `OnHeap`/`OffHeapColumnVector` batches
  are not Arrow. The Arrow cache serializer is the production source of
  eligible batches.
* **No whole-stage codegen integration.** The Varka nodes are not
  `CodegenSupport`; whole-stage codegen splits at the boundary.
* **Emitter caps**: chain depth 16, 64 distinct ops, 64 input columns per
  kernel, and 16 literals per fused `IN` list. Since task 20 the compiler
  mirrors the depth and op budgets and demotes an overflowing entry to
  residual with a recorded reason, instead of the whole kernel silently
  falling back per batch at emission. A capped `IN` still lands in one
  loop method (the emitter never splits inside an output): 33 vector ops
  in the benchmarked cap shape - 16 EQ + 15 OR + the blend + the branch
  arithmetic, about twice the per-method `GROUP_BUDGET` - so a fresh IN
  shape's first execution pays a one-time C2 compile of roughly 1 ms per
  vector op; the class cache amortizes it across every later task of that
  shape, and the exception is registered with the `GROUP_BUDGET` rule in
  `sql/varka/AGENTS.md`.

## Building, testing and running benchmarks

```bash
./build/mvn -f sql/varka/engine/pom.xml install
build/sbt "sql/testOnly org.apache.spark.sql.execution.VarkaDifferentialSuite"
# Engine JMH kernels (in-process, gated):
./build/mvn -f sql/varka/engine/pom.xml test -Dvarka.jmh=true
# Spark benchmarks (SPARK_GENERATE_BENCHMARK_FILES=1 to regenerate the committed files):
build/sbt "sql/test:runMain org.apache.spark.sql.execution.benchmark.VarkaThroughputBenchmark"
build/sbt "sql/test:runMain org.apache.spark.sql.execution.benchmark.VarkaColdStartBenchmark"
build/sbt "sql/test:runMain org.apache.spark.sql.execution.benchmark.VarkaCodegenBenchmark"
build/sbt "sql/test:runMain org.apache.spark.sql.execution.benchmark.VarkaInExpressionBenchmark"
build/sbt "catalyst/test:runMain org.apache.spark.sql.VarkaEmitterParityBenchmark"
```