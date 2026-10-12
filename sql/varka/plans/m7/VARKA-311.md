# VARKA-311: The kernel, guard and variant checks in Java

## 1. Where this came from

Row 311 of `m7/PLAN.md`, added by the owner on 12 October 2026 with rows 310 and 312 (`PLAN.md`
2.3, "The tests' helpers to Java"). The "Java code" rules in `sql/varka/AGENTS.md` make test
helpers Java and the suites thin Scala wrappers over them. VARKA-310 moved the reference evaluator
to Java (`VarkaReference`), which was the reason the three largest Scala helpers stayed Scala. This
row moves those three:

* `VarkaKernelCheck` (431 lines): runs an emitted kernel over Arrow-shaped memory - poisoned null
  slots, the memory sanitizer's window, the dense and masked bodies - and holds every row and
  validity bit to the reference, and the reference to Spark's interpreted answer where a suite
  passes one (`VarkaSparkOracle`, VARKA-276).
* `VarkaGuardSpec` (226 lines): VARKA-279's verdict per batch - which guards must refuse it, which
  may, and the planted bound shifts.
* `VarkaVariantCheck` (206 lines): VARKA-278's equivalent variants of a fuzz case and the check that
  their kernels agree.

The row is done when the three are Java, each Scala object a forwarder with its signatures, every
caller unchanged, and the suites that use them give the same test names and outcomes as before.

## 2. The admission check, done

**The callers** (12 October 2026). `VarkaKernelCheck` is called from `VarkaIrFuzzSuite`,
`VarkaIrSeries`, `VarkaGuardEdgeSuite`, `VarkaSmallTreeSuite`, `VarkaMemorySanitizerSuite`,
`VarkaCoverageCompositionFuzzSuite` (its Spark comparison counter) and `VarkaVariantCheck`;
`VarkaGuardSpec` from `VarkaGuardEdgeSuite`; `VarkaVariantCheck` from `VarkaVariantSuite` and the IR
fuzzer's variants hook. All in catalyst's test tree.

**What they lean on that is still Scala.**

* `VarkaFuzzCase`, the case class every fuzz suite builds and passes; the Java checks read its
  fields and do not replace it.
* `VarkaSparkOracle`, which evaluates Catalyst expressions through `eval`: Spark's interop
  surface, which the rules keep Scala. It gains a Java-facing `answer` over a values array and null
  flags; its Scala form stays for its other callers.
* `VarkaFailureSignature` and `VarkaKnownFailures`, which classify and record fuzz failures from the
  exception a check throws. `sql/varka/fuzz/known_failures.tsv` holds no entries today, so no
  recorded signature depends on what the checks throw.
* ScalaTest's `assert`, which the checks fail through.

**A finding: failures are not classified as designed.** `VarkaFailureSignature` names a failure's
kind from its message's opening words - "output N row M differs" is an output mismatch, "selection
row" a selection mismatch - after taking off the case's label. But `assert(a === b, clue)` puts
ScalaTest's comparison first ("false did not equal true seed=...: selection row 12 differs"), so
the label is not at the start, no rule matches, and the kind falls to the exception's class:
VARKA-310's planted fault was signed "TestFailedException: false did not equal true seed=# ...".
The Java checks throw with the message the classifier expects - the label, then the mismatch - so
the kinds come out as VARKA-277 meant them.

## 3. The design

### 3.1 Java

Three final classes of static methods in catalyst's test tree, beside `VarkaReference`:

* **`VarkaKernelChecks`**: `Batch` and `LongBatch` as records over `int[][]`/`long[][]` data and
  `boolean[][]` null flags (in place of `Seq[Int => Boolean]` patterns); `runAndCompare`,
  `runAndCompareStatus`, `runEachBodyStatus` and their long-lane twins; `otherBody`; the Spark
  comparison counter. Failures are `IllegalStateException`s with the label first; checkstyle refuses
  a thrown `Error`, as VARKA-268 found.
* **`VarkaGuardSpecs`**: `Verdict` and `Shift` as records, `Guards` over `VarkaLoopEmitter`'s
  `guardsForTest`, and `verdict`, computed with `BigInteger` where the Scala form used `BigInt`.
* **`VarkaVariantChecks`**: `Outcome` as a sealed interface of records (`Compared`, `Declined`,
  `Skipped`),
  `Emit` as a functional interface, `variants`, `caseOf`, `check`, `checkPlain` and
  `retryWithoutBudget`, over `VarkaVariants` (already Java) and `VarkaKernelChecks`.

The names take an `s` because each Scala object keeps its own name while its forwarder lives.

### 3.2 The forwarders

Each Scala object keeps its members and their Scala signatures, converting a `Seq` of predicates to
null flags, a Scala function to the Java interface, and an outcome back, as VARKA-268's trait did.
No suite changes.

### 3.3 What is deliberately unchanged

* The suites, `VarkaFuzzCase`, `VarkaFailureSignature` and the shrinker.
* What is checked: the same rows, the same bodies, the same poisoned slots and sanitizer window,
  the same verdicts. Only the failure's exception type and its message's order change, and the
  message keeps every word.

## 4. Files

| file | what |
|---|---|
| `.../codegen/varka/VarkaKernelChecks.java`, `VarkaGuardSpecs.java`, `VarkaVariantChecks.java` | the checks, 3.1 |
| `.../codegen/varka/VarkaKernelCheck.scala`, `VarkaGuardSpec.scala`, `VarkaVariantCheck.scala` | the forwarders, 3.2 |
| `.../codegen/varka/VarkaSparkOracle.scala` | the Java-facing `answer` |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-311.md` | the row and this plan |

## 5. Tests, and what each is for

* **The same rows are checked.** `VarkaKernelChecks` counts the rows and bits it compares, beside
  the Spark comparison counter it already keeps; the IR fuzz suite, the IR series slice the gate
  runs, the guard edge suite and the composition fuzzer, at their fixed seeds, give the same counts
  before and after. Equal outcomes alone would not show that a check still looks at every row.
* **The planted faults still fail**, each where it does today: the IR fuzzer's planted liveness and
  generated-class bugs, VARKA-279's planted guard shifts, VARKA-278's variant tests under the
  planted liveness bug, the memory sanitizer suite's check that the catalyst harness runs its
  kernels inside the sanitizer's window, and a planted wrong answer in `VarkaReference`.
* **The classification works**: a test that plants an output mismatch and a selection mismatch and
  asserts their signatures' kinds are "output mismatch" and "selection mismatch".
* **Every suite that uses the checks**, unchanged, at both widths in the gate, with the same test
  names and outcomes.

## 6. The measurement

The IR fuzz suite's random-tree test and the guard edge suite, before and after, in their own JVMs.

### 6.1 Predictions, registered before the run

1. **The counts agree exactly** on every suite in 5's first item.
2. **Every planted fault fails**, and the two planted mismatches classify by kind.
3. **Every suite passes with the same test count**, at both widths.
4. **No suite is slower than 5%**: the checks do the same work, and the forwarders convert a batch
   once.
5. **The three Java classes come to about 1,100 lines** against the Scala forms' 863, as VARKA-310's
   did (492 against 335): exhaustive switches and statements where Scala had expressions.

## 7. Risks

1. **A check that silently checks less** - a loop bound, a body not run, a bit not read. 5's
   counts are what catch it; a planted fault catches only the fault planted.
2. **The memory sanitizer's window.** `sanitized` registers the batch's segments around the run and
   reads violations after; a Java form that registers too late or too narrow would pass every
   kernel. The sanitizer suite's harness test is the check.
3. **Message order and the shrinker.** A shrinker preserves a signature within one run, so the new
   kinds do not break it; but a failure recorded before this change would carry the old signature.
   None is recorded (2), and the plan's 9 says so for any reader with an old log.
4. **Size.** If the port outgrows one pull request, it lands as 311a (`VarkaGuardSpecs` and
   `VarkaKernelChecks`) and 311b (`VarkaVariantChecks`, which needs the first).

## 8. Sequencing

1. This plan, and the row marked Planned.
2. `VarkaGuardSpecs`; then `VarkaKernelChecks` with its row counter, the counts read before and
   after; then `VarkaVariantChecks` - each a commit with its forwarder.
3. The classification test and the planted faults.
4. Section 9.
