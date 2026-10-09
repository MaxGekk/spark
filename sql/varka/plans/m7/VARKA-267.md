# VARKA-267: The evaluators to Java, beside 251's components

## 1. Where this came from

Row 267 of `m7/PLAN.md`, from the plan review (2.3) and `m8/SCOPE.md` item 74: after row 251 split
`VarkaEvaluatorBase` into six Java components, the evaluators themselves stayed Scala:
`VarkaEvaluatorBase` (559 lines, now a composition of those components), `VarkaKernelEvaluator`
(397), `VarkaFilterEvaluator` (389), `VarkaKernelPart` (60), `VarkaVectorProjection` (86),
`VarkaFusionReport` (117) and `VarkaExecMetrics` (94): 1,702 lines. The exec nodes and the columnar
rule stay thin Scala wrappers, since `SparkPlan` case classes are the interop surface the owner's
rule exempts. It also unblocks row 300 (the classifier and the data model, 217b), which can only
become Java records once their consumers, these evaluators, are Java.

## 2. The admission check, done

Read on 9 October 2026 on `master` (`363a1280790`):

* **The seams are narrow.** `VarkaKernelEvaluator` has 6 main-source consumers and 6 test files;
  `VarkaFilterEvaluator` 1 and 1; `VarkaEvaluatorBase` 3 and 2; `VarkaFusionReport` 5 and 3;
  `VarkaExecMetrics` 7 and 6 (11 construction sites in tests); `VarkaVectorProjection` 1 and 1;
  `VarkaKernelPart` 1 and none. Sixteen `new VarkaKernelEvaluator`/`new VarkaFilterEvaluator`
  sites in all.
* **What the Java inherits from the Scala is small.** The base's by-name `serveBatch[T]` (two
  closures and an `Either` per batch today) becomes two functional-interface parameters, which the
  Scala nodes pass as they pass closures now; Java code writes no lambda on the batch path, and
  allocates no `Either`. `Option`/`Seq` arguments (`classDumpDirectory`, `projectList`,
  `childOutput`) stay Scala types at the constructor and are read once in it, with `scala.jdk`
  converters, until row 300 gives their producers Java types.
* **A Java subclass of a Java base, and a Scala subclass of a Java base, both work during the
  migration**: the Scala `VarkaKernelEvaluator` can extend the Java base while the cut below is in
  flight, so each step is a PR that leaves master green.
* **A per-batch benchmark exists** (VARKA-251's `VarkaEvaluatorOverheadBenchmark`: the projection
  and filter evaluators driven over pre-built Arrow batches of 1, 16 and 1024 rows, four shapes,
  checking every batch took the kernel). It is the proof for the two steps that touch the batch
  path.
* **The throughput claim of 251 is the bar**: per batch the same as the unchanged evaluators over
  ten runs at each width, every case's range overlapping and no minimum more than 7% higher.

What the check would have rejected: a port of the base and the two evaluators in one PR (about
1,400 lines of Java in a PR that cannot be reviewed against its Scala), and carrying `Either` and
by-name closures into Java to keep the Scala shape.

## 3. The design

### 3.1 The cut: three pull requests, bottom up

1. **267a, the leaves**: `VarkaExecMetrics` as a record (the metrics a node registers, null where
   absent, with `nodeMetrics`, `projectionMetrics` and `fromNode` as static factories and a
   builder for the suites that set one or two), `VarkaSelection` as a record, `VarkaFusionReport`
   as a class of static renderers, and `VarkaVectorProjection`. None is on the per-batch kernel
   path; the vector projection is the per-row fallback and is measured on its own benchmark's
   shape.
2. **267b, the base and the filter evaluator**: `VarkaEvaluatorBase` (abstract), `VarkaKernelPart`
   and `VarkaFilterEvaluator`. `serveBatch` takes two `VarkaBatchPath<T>` interfaces. The Scala
   `VarkaKernelEvaluator` extends the Java base for this step.
3. **267c, the projection evaluator**: `VarkaKernelEvaluator`, `VarkaOwnedArrowColumnVector` and the
   weak warm-up-batch table, the allocation schedule and the logged-declines set that sit in its
   Scala companion.

Each step deletes the Scala it replaces, adapts its Scala consumers (the nodes, the rule, the
suites) and is proved on its own.

### 3.2 The Java rules applied

Records for the data carriers (the metrics, the selection); a `sealed` interface where the
fallback causes are a closed set, consumed by an exhaustive `switch`; pattern-matching `switch` over
the Arrow vector classes in `isArrowBacked` (no `default` over a sealed type; Arrow's classes are
not sealed, so the `default` there is the stated exception); no lambda, stream or boxing in the
batch path; explicit imports; 100 columns.

### 3.3 What is deliberately unchanged

The exec nodes, the columnar rule, `VarkaInputRows` and `VarkaRowToColumn` (Scala helpers of the
nodes), the six components of 251, and the compiler's data model, which row 300 ports; the
evaluators read it through its Scala accessors until then.

### 3.4 Registered op counts

None.

## 4. Files

Per step, in `sql/core/src/main/java/org/apache/spark/sql/execution/` (the evaluators beside
`varka/`, which holds the components), with the Scala files deleted and the nodes and suites
adapted. The list is written with each step's section 9.

## 5. Tests, and what each is for

The suites that exist are the proof, because the port is meant to change nothing:
`VarkaKernelEvaluatorSuite`, `VarkaFilterExecSuite`, `VarkaProjectExecSuite`,
`VarkaColumnarToRowExecSuite`, `VarkaDifferentialSuite`, `VarkaWarmupEndToEndSuite`, the
sanitizer's end-to-end suite and the rest of the 36 sql Varka suites, plus `emitted_bytes.json`
and `coverage.json` byte-identical (no emitter or compiler change), and the per-batch benchmark
for 267b and 267c.

## 6. The measurement

`VarkaEvaluatorOverheadBenchmark` before and after each of 267b and 267c, interleaved on the idle
laptop (`dev/varka_bench_pair.sh` once #683 has merged), both widths.

### 6.1 Predictions, registered before the runs

1. 267a changes no committed number: the leaves are off the kernel path, and the row-path
   benchmark's cases move within their noise.
2. 267b and 267c: every case of the overhead benchmark within 5% of the Scala either way, none
   more than 7% slower by minimums, and the per-batch allocation at or below the Scala's (no
   `Either`, no `Option`, no tuple per batch).
3. The Java is shorter than the Scala by no more than a fifth and longer by no more than a half;
   the Scala was dense with `Option` and `Seq` plumbing that records and `switch` replace.

## 7. Risks

1. **Per-batch allocation.** A boxed `Integer`, a `Supplier` created in Java or an iterator over a
   Scala `Seq` on the batch path undoes the hot path's discipline; the benchmark's allocation read
   and a review of every call in the batch path are the checks.
2. **Scala subclass of a Java base** (267b): protected access across the language boundary and
   the abstract methods' `Option` return types; the 267b compile is the check.
3. **The nodes' by-name closures** become `VarkaBatchPath` arguments; the four nodes' call sites
   and the suites' direct uses change together in one commit.
4. **A suite that reaches into a private member** through Scala's `private[execution]` (several
   do, for `kernelIdentity`, `emittedClassBytes`, `partialPlan`): those become package-private
   Java members, which Scala in the same package reads.

## 8. Sequencing

267a, then 267b, then 267c; this plan is the first commit of 267a and the rows 267b and 267c are
added to `m7/PLAN.md` as they start.

## 9. Outcome

### 9.1 Step 267a, the leaves

Done on 9 October 2026. `VarkaExecMetrics` (a `Serializable` record with `NONE`, a builder, and
the static `nodeMetrics`, `projectionMetrics`, `fromNode` and a null-safe `inc`), `VarkaSelection`
(a record), `VarkaFusionReport` (static renderers over the Scala data model) and
`VarkaVectorProjection` are Java; the four Scala files they replace are deleted. The nodes, the
evaluators' call sites and nine suites are adapted: 13 `foreach(_ += 1)` sites became `inc`, the
node metric maps convert with `asScala.toMap`, and the 11 construction sites in tests became
`NONE` or the builder.

**The proof held.** The 38 sql Varka suites pass (440 tests, 12 cancelled as before), scalastyle and
checkstyle are clean; no emitter or compiler file changed, so `emitted_bytes.json` and
`coverage.json` are untouched.

**One thing the port broke and the suites caught.** The first run failed 302 tests with `Task not
serializable: ... VarkaExecMetrics`: a Scala case class is `Serializable` and the exec nodes'
evaluator factories carry the bundle to the executors, a property a Java record does not have until
it says so. The record implements `Serializable` now, and says why.

**Predictions scored.** 1 (no committed number moves): held by construction, no benchmark file
changed, and not otherwise measured: the leaves are off the kernel path, and the vector
projection's loop is the same call sequence as the Scala's (an input-rows copy, the mutable
projection, a row id), which the per-batch benchmark of 267b and 267c does not exercise. The row
path's cost is therefore *not measured here* and rests on that identity; `VarkaVectorProjectionSuite`
pins its output.
