# VARKA-276: The reference evaluator held to Spark's interpreter

## 1. Where this came from

Row 276 of `m7/PLAN.md`: every coverage row's IR and every composed kernel run three ways - as the
kernel, through the reference evaluator, and through Catalyst's `eval` - with no session.
`m7/PLAN.md`'s reading of PQS (Rigger and Su, OSDI 2020, pp. 12-13) is the reason: PQS checks its
interpreter against the engine, and `VarkaReferenceEvaluator`, the oracle of both fuzzers and of
row 269, was held to nothing. A kernel that disagrees with both is a Varka bug; a reference that
disagrees with Spark is fixed, or becomes a named delta (row 244).

Today `VarkaKernelCheck` compares a kernel with the reference row by row, for the IR fuzzer's
drawn trees and for the composition fuzzer's compiled kernels. The reference's calendar answers
come from `java.time` and `DateTimeUtils`, its arithmetic from Java's operators; nothing compares
it with the Catalyst expression a kernel was compiled from.

## 2. The admission check, done

The check, built as a prototype on 10 October 2026: a `VarkaSparkOracle` that binds an output's
Catalyst expression to the kernel's input attributes and evaluates it on each row the kernel saw,
asked by `VarkaKernelCheck` before it compares the kernel with the reference. Run over the
composition fuzzer's default wide test (20 projections of 150 to 300 coverage rows, 48 kernels)
and over each of the coverage table's 92 rows compiled alone at both pinned widths:

* **The analysed expression needs the optimizer's replacements.** `extract` and its kin are
  `RuntimeReplaceable` and have no `eval`; the oracle swaps each for its replacement, as
  `ReplaceExpressions` does.
* **One disagreement, and it was the harness's.** On `t + INTERVAL '0' DAY TO SECOND` Spark
  answered 64653424019000 where the reference answered the input, 64653424019403: the composition
  fuzzer drew `TIME(6)` values to the nanosecond, which a `TIME(6)` column cannot hold, and Spark
  truncates them on its first arithmetic. The reference was right about the value it was handed;
  the value was outside the type. Drawn in whole microseconds, nothing disagrees.
* **The reference, the kernels and Spark then agree** on 536541 answers in the wide test and
  173777 in the per-row test.
* **A planted reference bug is blamed on the reference.** With `quarter` computed as
  `(month + 1) / 3`, the per-row test fails with "the reference evaluator and Spark disagree on
  output 0 (quarter(d))", where without the oracle the same bug reads as the kernel's.

What it would have rejected: a reference that disagreed with Spark on a row the kernel is checked
on, which would have made this row a list of fixes or deltas first.

## 3. The design

### 3.1 The third oracle

* **`VarkaSparkOracle`** (catalyst tests): output `o`'s expression, its `RuntimeReplaceable`
  nodes replaced, bound to the attributes the kernel's inputs read, evaluated on a
  `GenericInternalRow` of the row's lane values. Every type a kernel reads is its lane value in
  Catalyst's internal form - days, months, nanoseconds of a `TIME`, microseconds of a day-time
  interval. The answer is a value (a predicate's as 1 or 0), null, or the error Spark raised.
* **`VarkaKernelCheck`** takes it as an optional argument at both lanes. On each row and output,
  before the kernel is compared, the reference's answer must equal Spark's or both be null; Spark
  raising on a batch the kernel answered fails as the kernel's, since the kernel must have
  declined. A count of answers held to Spark lets a suite show the check ran.
* **The composition fuzzer** builds the oracle for every kernel of the wide test, each output's
  expression found through the projection's specs (kernel 0's `FusedOutput`s, later kernels'
  `KernelOutput`s), and a new test runs each coverage row alone - projections through `compile`,
  predicates through `compilePredicate`'s mask - four batches at each of the two pinned widths. A
  kernel that derives an input gets no oracle, since Spark would read the source column in place
  of the code; the coverage table has none, and the per-row test asserts it.
* **The `TIME` draw** is in whole units of the column's precision, in the same number of draws
  from the stream, so every recorded seed draws the same shapes.

### 3.2 What is deliberately unchanged

* **The IR fuzzer** (`VarkaIrFuzzSuite`): its trees are drawn as IR and have no Catalyst
  expression to ask. Row 269's exhaustive small trees are the same.
* **Named deltas**: none was needed. Row 244 defines them; the oracle takes a delta list when the
  first one appears.
* **The emitter, the compiler and the reference evaluator.** No production code changes.

### 3.3 Registered op counts

None move.

## 4. Files

| file | what |
|---|---|
| `VarkaSparkOracle.scala` | new: Spark's answer per output and row |
| `VarkaKernelCheck.scala` | the optional oracle at both lanes, the reference held to it, the count |
| `VarkaCoverageCompositionFuzzSuite.scala` | the oracle in the wide test, the per-row test, the `TIME` draw |
| `m7/PLAN.md`, the testing lessons | the records |

## 5. Tests, and what each is for

* The per-row test: every coverage row's kernel, the reference and Spark agree on four batches at
  each pinned width, and over 100000 answers were held to Spark.
* The wide test: as before, and every kernel's answers held to Spark too.
* The planted-bug tests of the composition fuzzer still find and shrink `misdescribeAdd`, so the
  oracle did not change which check fires on a kernel bug.
* A planted reference bug, in a copy, fails the per-row test as the reference's.

## 6. The measurement

The two tests' time, before and after, on the laptop.

### 6.1 Predictions, registered before the run

The prototype ran first, so section 2 already holds what a prediction would have. One remains:
a wider run, 200 wide compositions on another seed, finds no disagreement between the reference
and Spark.

## 7. Risks

1. **An oracle that agrees by construction.** Spark's `eval` and the reference both call
   `DateTimeUtils` for several calendar fields, so the two agree there whatever the kernel does;
   what the oracle adds there is the expression's own composition and null rules. The kernel is
   still held to the reference, which is the stronger check for those fields.
2. **Inputs outside a type's domain**, as the `TIME` draw was: a disagreement on such a row is the
   harness's, and the message prints the inputs so it can be told apart.

## 8. Sequencing

1. The plan, with the prototype's numbers.
2. The oracle, the check and the tests, and the records.

## 9. Outcome, 10 October 2026

### 9.1 What was built

As section 3 describes. The per-row test runs 184 kernels (92 rows at two widths, four batches
each) and holds 173777 answers to Spark in 17 s; the wide test now also holds 536541 answers to
Spark, and takes 24 s where it took 20 s.

### 9.2 The prediction scored

**Holds.** 200 wide compositions on seed 20261010 (set through
`set LocalProject("catalyst") / Test / javaOptions`, the form that reaches the suite's own JVM):
515 kernels, 396 at the int lane and 119 at the long lane, and 7253901 answers held to Spark,
with no disagreement.

### 9.3 What this leaves

* **The IR fuzzer's trees and row 269's** have no Catalyst expression; holding them to Spark
  would need the IR translated back, which is a decompiler this row did not want.
* **Named deltas** begin with row 244; the first one gives the oracle its delta list.
