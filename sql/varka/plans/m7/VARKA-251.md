# VARKA-251: `VarkaEvaluatorBase` split into Java components

## 1. Where this came from

Row 251 of `m7/PLAN.md`, item 74.5 of `m8/SCOPE.md`: `VarkaEvaluatorBase` is 1079 lines of Scala
holding the kernel runner, the batch ledger, scratch, the warm-up, fallback accounting and class
dumping, four of which VARKA-190's further kernels needed hooks into. The row asks for Java
components, not smaller Scala files (`m7/PLAN.md` 2.3), and names its proof: a before-and-after
benchmark of per-batch overhead. Row 267 then ports the evaluators themselves beside the
components; this row leaves them in Scala.

## 2. The admission check, done

**What the base holds, by responsibility** (read 6 October 2026, master `cf3146327bb` and later):

| responsibility | members | lines |
|---|---|---|
| runner | `FusedRunner`, `fusedRunner`, `fillSources`, `invokeFused`, `extractMorsel`, `ofAddress`, `Morsel` | about 230 |
| batch ledger | `kernelAllocator`, `openBatches`, `ensureCleanup`, `taskAllocator`, `track`, `trackOwned`, `forwardColumns`, `release`, `closeQuietly`, `closeAllQuietly` | about 150 |
| scratch | `derivedData`, `derivedValidity`, `kernelScratch`, `derivedScratch`, `growSlot`, `kernelScratchAddress`, the three releases | about 170 |
| warm-up | `kernelReady`, `startWarmup`, `anyNullableInput`, `inputWidths`, `warmed` | about 90 |
| fallback accounting | `serveBatch`'s causes: `recordKernelFailure`, `recordRowPathFailure`, `recordDeclinedBatch`, `recordRefusedBatch`, the allocation sample | about 150 |
| class dumping | `dumpClass` and the per-JVM memo | about 30 |
| what stays | the abstract `fusedPlan` and `identityEntries`, `kernelIdentity`, `shapeKey`, `emitOptions`, `canRun`, `isArrowBacked`, `serveBatch`, `runKernel` | |

The subclasses (`VarkaKernelEvaluator`, `VarkaFilterEvaluator`, `VarkaKernelPart`) use the base
through `runKernel`, `serveBatch`, `canRun`, `fusedRunner`, `fillSources`, `invokeFused`,
`taskAllocator`, `trackOwned`, `release`, `closeQuietly`, `closeAllQuietly` and `onTaskCleanup`.

**No benchmark measured per-batch overhead.** The throughput benchmarks run 4096-row cached
batches, where the kernel's rows dominate. So the benchmark comes first, against the unchanged
code, in its own commit (the rule that a baseline is committed before the change it judges):
`VarkaEvaluatorOverheadBenchmark` drives the projection and filter evaluators directly over
pre-built Arrow batches of 1, 16 and 1024 rows, four shapes (int lane, long lane, filter, derived
input), and checks every batch took the kernel. Its first run caught a shape that had stopped
fusing (`l + 1`: bigint arithmetic is not a coverage row; `least(l, 5000000000)` replaced it).

**No test reads these loggers by name**: `VarkaProjectExecSuite` captures at the root, the other
log-capturing suites name unrelated loggers. So the components can log under their own names.

## 3. The design

### 3.1 The components

Six Java classes in `sql/core/src/main/java/org/apache/spark/sql/execution/varka/`, one per
responsibility, each a plain class over the state it owns:

- **`VarkaBatchLedger`**: the task's Arrow child allocator, the open batches, the one
  task-completion listener, `track`, `trackOwned`, `forwardColumns`, `release`, and the quiet
  closes. The listener runs, each guarded on its own, the batch closes, then the cleanup actions
  registered with it (the scratch release, a subclass's hook), then the allocator close: the order
  and guards of today's listener.
- **`VarkaKernelScratch`**: the derived inputs' buffers and the prefix scratch, the
  allocate-store-release order of `growSlot`, and their release, over the ledger's allocator.
- **`VarkaKernelRunner`**: `FusedRunner`'s shape-cache lookup, kernel and argument arrays, and
  `fillSources` and `invokeFused` as `fill` and `invoke`. `VarkaBatchDeclined` and
  `VarkaKernelFailure` move to Java with it.
- **`VarkaWarmupGate`**: `kernelReady` and `startWarmup` over the runner.
- **`VarkaFallbackAccounting`**: the four causes, the allocation sample, and the two JFR events;
  the counters as a record of the `SQLMetric`s `VarkaExecMetrics` holds, null where absent.
- **`VarkaClassDump`**: the dump and its per-JVM memo.

`VarkaEvaluatorBase` stays a Scala class, about 300 lines: it builds the components, keeps the
members subclasses call with the same names, delegating, and keeps `serveBatch`, whose by-name
paths are Scala's (row 267 decides its Java form with the evaluators).

### 3.2 What is deliberately unchanged

The evaluators (row 267), the exec nodes, the emitter and the compiler. Every behaviour: the
cleanup order and guards, the fallback causes and their metrics and events, the warm-up claim
and release, the dump memo, the log messages. The Java rules of `sql/varka/AGENTS.md` hold, in
particular no allocation, stream, lambda or boxing on the per-batch path: `fill`, `invoke`, the
causes and `canRun` stay plain loops, and the identity a fallback event renders is a supplier
built once per evaluator.

### 3.3 Registered op counts

None: no emitter change; `emitted_bytes.json` cannot move.

## 4. Files

| file | what |
|---|---|
| `VarkaEvaluatorOverheadBenchmark.scala` and its results files | the per-batch benchmark, first commit |
| `sql/core/.../execution/varka/*.java` | the six components and the two exceptions |
| `VarkaEvaluatorBase.scala` | the composition |
| `VarkaKernelEvaluator.scala`, `VarkaFilterEvaluator.scala`, `VarkaKernelPart.scala` | their references to the moved members |
| `m7/VARKA-251.md`, `m7/PLAN.md` | this plan, row 251 |

## 5. Tests, and what each is for

- The `sql` module's Varka suites at both widths: every evaluator path, including the task-end
  cleanup suites (an early stop, a release per batch, the leak checks) and the fault-injection
  hooks, which drive each fallback cause.
- `catalyst`'s suites are unaffected but run in the gate.
- The benchmark before and after, below.

## 6. The measurement

`VarkaEvaluatorOverheadBenchmark`, regenerated with `dev/varka_bench_regen.sh core` at both widths
on the laptop, before (the first commit) and after (the split).

### 6.1 Predictions, registered before the run

1. No case slower than its baseline by more than 10%, or by more than the run's own spread where
   that is wider.
2. The 1-row cases, which are nearly all per-batch machinery, are no more than 50 ns a batch
   slower; they may be faster, since the Java `fill` reads each column's addresses directly where
   `fillSources` built a `Morsel` and two `MemorySegment`s per input per batch.

## 7. Risks

1. **The cleanup order changes.** The ledger's listener is written from today's, guard by guard,
   and the task-end suites check it.
2. **A per-batch allocation creeps in through the Java form**, a lambda or a boxed counter. The
   benchmark's 1-row cases would show it, and the code review checks the per-batch methods.
3. **Scala's `private[execution]` members become public Java**: the components are public classes
   in an internal package, as Spark's own Java under `execution` is.

## 8. Sequencing

1. The benchmark and its baseline results, and this plan: the first pull request.
2. The components, one commit each with the base delegating to it, the suites green at each; then
   the benchmark regenerated and section 9 written: the second pull request.

## 9. Outcome

### 9.1 The baseline, 6 October 2026

`VarkaEvaluatorOverheadBenchmark` on `f9ce6653094`, the unchanged evaluators, regenerated with
`dev/varka_bench_regen.sh core` on the laptop at both widths (the files' provenance). Nanoseconds
a batch, wide:

| rows a batch | int lane | long lane | filter | derived input |
|---|---|---|---|---|
| 1 | 620 | 579 | 138 | 812 |
| 16 | 589 | 592 | 518 | 1352 |
| 1024 | 822 | 1313 | 1233 | 30398 |
