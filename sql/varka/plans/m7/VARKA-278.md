# VARKA-278: Variants that must agree

## 1. Where this came from

Row 278 of `m7/PLAN.md`, from `m7/READING.md` 11's reading of Le, Afshari and Su, "Compiler
Validation via Equivalence Modulo Inputs" (PLDI 2014). EMI varies a program where its input does
not reach and requires the same output; 64 of its 147 bugs showed under one configuration only
(p. 8). Varka's tests compare each emitted kernel with `VarkaReferenceEvaluator`, but each shape
meets the emitter in one form: the fuzzer draws a tree, the small-tree series of VARKA-269
enumerates every tree to a size, and each is emitted as it was drawn. Yet most of what the emitter
decides depends on the company a tree keeps - which outputs share a subtree and so a slot, how the
outputs group into methods, which literal is folded and which column is read. A bug in one of
those decisions shows only when the same tree is emitted beside something else, and no test
emits it that way on purpose.

So: for every tree that passes, about eight variants whose answers are known from the tree's own,
each emitted and compared with the reference evaluator.

## 2. The admission check, done

Run on 11 and 12 October 2026 with a throwaway probe (`VarkaVariantProbeSuite`, not committed):
seven of the eight variants of 3.1 - all but the untaken arm, which needs the reference's
per-row condition and is built with the suite - over the int series, each emitted and compared
with the reference evaluator through `VarkaKernelCheck` on the series' own batches.

* **No failure under the defaults.** To size 3 (3,203 trees, every batch): 0 failures in 20,213
  variant checks. To size 4 (101,422 trees, one batch each by the series' rotation): 0 failures in
  644,004 checks - 100,946 trees have a value child for the two child variants, 36,424 a literal
  to move into a column.
* **The cost.** About 10 seconds per variant at size 3 in one JVM, each variant one emission and
  one comparison per tree, as the plain series' own check is. Size 4 took 53 CPU-minutes for the
  seven, 712 seconds of wall time over 6 JVMs on a loaded machine (a gate ran beside it), so size 4
  belongs to the night and size 3 to the gate, as 3.4 has it.
* **The untaken arm is rare.** 36 trees to size 4 hold an `IfElse`.
* **What the variants catch.** Of the four fault injectors in `VarkaEmitOption.TABLE`, two change
  answers (`misdescribeAdd`, `misdescribeWordLiveness`); the other two misstate the plan's byte
  estimate and the compiler's rollback, which a kernel's answer does not show. Both answer
  injectors are already caught by the plain series, so no injector is caught only by a variant.
  But under `misdescribeWordLiveness` the plain series fails 2,253 of the 3,203 trees and two
  variants fail more: beside a stranger fails all 3,203 and literal to column all 1,093 it applies
  to - 950 trees whose bug only a variant shows. The design passes on that: a planted bug narrower
  than this injector, one that shows only when an output keeps company, is exactly what the plain
  series would miss.

## 3. The design

### 3.1 The variants

Each is a function from a passing tree `T` (with its batch) to a projection whose expected answer
per output follows from `T`'s. "Passing" means `T` alone emitted and agreed with the reference.

| variant | from `T` | why the emitter sees something new |
|---|---|---|
| **beside a child** | `[T, C]`, `C` the first value child (VARKA-269 has this one) | a shared subtree, a slot |
| **permuted** | `[C, T]` | output order decides grouping and slot numbering |
| **duplicated** | `[T, T]`, one node object twice | the emitter's identity-based sharing |
| **copied** | `[T, T']`, `T'` structurally equal, fresh objects | structural sharing and the shape key |
| **beside a stranger** | `[T, D]`, `D` another passing tree of the lane, over other columns | grouping two unrelated outputs; `D`'s guards in `T`'s method |
| **literal to column** | each literal slot of `T` read from an added column that holds the literal on every row, never null | the column path of an arm the literal path served; range analysis without the constant |
| **columns renumbered** | `T`'s columns permuted, the batch's columns permuted alike | input ordinals in slots and the validity words |
| **untaken arm replaced** | in `IfElse(c, a, b)`, where the reference finds `c` false on every row of the batch, `a` replaced by another passing tree of its type (and `b` likewise where `c` is true everywhere) | code that must not run, with different code in it |

The relation is the reference evaluator's: every output of every variant is compared with it,
row for row and bit for bit, through `VarkaKernelCheck`. Three relations are weaker on purpose:

* **A variant may decline where `T` did not**, but only in the variants that change what the
  emitter knows: literal to column (a range the literal proved is now a column's) and beside a
  stranger (`D`'s guards share the batch). A decline there must name one of the guards the new
  part brings, by the exact status of VARKA-304; anywhere else a decline is a failure.
* **The untaken arm is batch-dependent.** It is defined only for the batch the reference showed
  the arm untaken on; on the other bodies that batch runs, the same holds, since the condition is
  per row.
* **Null lanes are free**, as everywhere: data under a null bit is not compared.

### 3.2 Where it runs

A new suite, `VarkaVariantSuite`, in the catalyst test tree beside `VarkaSmallTreeSuite`, its
helpers in Java (the "Java code" rules: test helpers Java, the suite a thin Scala wrapper). It walks
the series of VARKA-269 (`VarkaIrSeries`) and, for the fuzzer, a hook that `VarkaIrFuzzSuite` calls
on each passing draw, so random trees larger than the series' meet the variants too.

### 3.3 What is deliberately unchanged

* The reference evaluator and `VarkaKernelCheck`, which already compare any projection.
* The emit options: varying them is the option matrix's job, which already reruns every suite.
* Catalyst-level variants (a projection's entries reordered before compilation): VARKA-296 forces
  declines entry by entry, and row 298 grows Catalyst trees.

### 3.4 The tiers

| where | trees | variants |
|---|---|---|
| pull request (the gate) | the int and long series to size 3 | all eight |
| nightly (`varka-fuzz.yml`) | the series to size 4, and each fuzzer draw | all eight |

Priced by 2. Size 4 holds the first `IfElse`, so the untaken-arm variant runs only at night unless
the fuzzer's draws in the gate carry it, which they do.

### 3.5 Registered op counts

None; no emitted class changes.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/src/test/java/.../codegen/varka/VarkaVariants.java` | the eight variants, each a function from a tree and its batch |
| `sql/catalyst/src/test/scala/.../codegen/varka/VarkaVariantSuite.scala` | the suite over the series |
| `sql/catalyst/src/test/scala/.../codegen/varka/VarkaIrFuzzSuite.scala` | the hook on a passing draw |
| `.github/workflows/varka-fuzz.yml`, `dev/varka_nightly.sh` | the nightly tier |
| `sql/varka/skills/testing-and-debugging.md` | what the variants found, or that they found nothing |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-278.md` | the row and this plan |

## 5. Tests, and what each is for

* **Each variant against the reference**, over the series to size 3 in the gate: the reach the row
  is for.
* **Each variant is what it says**: a test per variant on one hand-written tree that the variant's
  output has the promised shape - the duplicated variant holds one node object twice, the copied
  one two equal objects, the literal variant no literal slot - so a variant cannot silently
  degrade into the plain tree and pass vacuously.
* **The fault injectors** of 2, kept as a test: each one the variants catch, they must keep
  catching.
* **Counted, not assumed**: the suite logs how many trees each variant applied to; an untaken-arm
  count of zero at size 4 fails.

## 6. The measurement

No benchmark: nothing in the emitter changes. The suite's time per tier is measured in 2 and
again in 9.

### 6.1 Predictions, registered before the run

1. **The variants cost about eight times the series** at a size, since each is one more emission
   and a comparison; the literal-to-column variant applies to fewer trees, the untaken arm to few.
2. **No failure under the defaults** to size 4. Varka has fuzzed this emitter for months; a find
   would be in the sharing or grouping code, which the plain series reaches least.
3. **At least one fault injector is caught only by a variant**: a sharing-related one, since a
   lone tree shares nothing with anything.

## 7. Risks

1. **Vacuous variants.** A variant that rebuilds the plain tree passes everything. Section 5's
   shape tests and the counts guard it.
2. **Declines hiding failures.** Allowing declines in two variants could let a kernel decline
   where it should answer. The decline must name a guard the new part brings, by its exact status
   bit.
3. **The gate's time.** If the gate tier costs more than about a minute a width, it shrinks to the
   cheapest variants and the rest move to the nightly, as the series' own tier did.

## 8. Sequencing

1. This plan, with 2 done, and the row marked Planned.
2. The variants and the suite at the gate tier, in one commit.
3. The nightly tier and the fuzzer hook.
4. Section 9.

## 9. Outcome

Done on 12 October 2026, in two commits after the plan: the variants and the suite at the gate
tier, then the nightly tier and the fuzzer hook.

**What was built.** `VarkaVariants` (Java) builds the eight variants of 3.1, each a list of roots
and a plan for its columns (an original column, or a literal slot's value on every row);
`VarkaVariantCheck` turns a passing case into each variant's case, emits it and compares every
output with the reference evaluator; `VarkaVariantSuite` runs the int and long series in each
variant, and `VarkaIrFuzzSuite` the variants of its passing draws. The stranger reads copies of
the columns at ordinals shifted past the original's, so it shares no column read with the tree
and still carries its own guards; in the series it is a root of size 1 or 2 by a fixed stride, in
the fuzzer the draw's next root. An untaken arm's replacement comes from the series' value trees
to size 2, or the draw's own value subtrees.

**The gate tier**, size 3 under the defaults, one batch per tree by rotation: 3,203 int trees and
1,382 long, every variant that applies checked (int: beside a child and permuted 3,154, literal
to column 1,093, the other four 3,203), nothing failed, nothing declined, nothing past the
class-file cap; 15 seconds for the int lane and 4 for the long in the suite's own JVM. Every
conditional tree of size 4 (36 int, 18 long) with its untaken arm replaced, on all four batches:
90 and 45 variants, in under a second, so the untaken arm runs in the gate after all.

**Size 4**, through `dev/varka_series.sh 4 --variants` over 24 JVMs on the laptop: 101,422 int
trees and 25,816 long, every variant that applies checked (int: beside a child 100,946, literal
to column 36,424, untaken arm 22; long: 25,730, 10,788 and 13), nothing failed and nothing
declined, in 226 seconds of wall time.

**The gate**: every step passed, the wide and narrow suites in 827 and 831 seconds, against 853
and 863 on VARKA-269's branch when it ran size 4 (lint failed once, on a local method's missing
return type in the shape test, and passed when rerun).

**The fuzzer**: its 300 draws a lane gave 1,845 int variants and 1,900 long, every kind
represented (untaken arm 40 and 85), all passing; the two random tests went from 0.8 and 0.7
seconds to 4.2 and 2.8.

**Predictions scored.**

1. **Wrong, cheaper.** In the gate the suite took 134 seconds at each width beside the other
   suites' JVMs (15 and 4 seconds for its two lanes alone), against 91 and 89 for the series
   itself: about one and a half times the series, not eight, because a variant meets one batch
   where the series meets four. In the fuzzer, which gives each variant the draw's own batch,
   a draw's variants cost about four draws.
2. **Held.** No failure under the defaults to size 4, at either lane (the admission check had run
   the int lane only).
3. **Wrong**, as 2 already found: both answer-changing injectors are caught by the plain series.
   What the variants add is reach under an injector, not an injector of their own: under
   `misdescribeWordLiveness` beside a stranger and literal to column fail trees the plain series
   passes, and the suite holds them to it.

**Corrections to the plan.**

* **3.1's untaken arm was wrong about the other body.** It said the arm stays untaken on the other
  bodies a batch runs, "since the condition is per row". The other body of a batch with nulls
  clears them, which can make a condition true that the batch never took: the fuzzer's first run
  found a draw where `isNotNull(c0)` over an all-null column turned true there, the replaced arm
  (a bare column) was taken, and its values left the range an enclosing `GuardedRange` was drawn
  for, which the kernel rightly declined. The variant meets the other body only on a batch with no
  nulls, where the values and so the conditions are the same.
* **3.3 said `VarkaKernelCheck` would not change.** The decline rule needs the status, which
  `runAndCompare` does not return; `runAndCompareStatus` and its long twin return it, beside the
  existing entry points, which are unchanged for their callers.
* **"Duplicated" and "copied" meet the same sharing.** The IR nodes are records and the emitter
  keys its maps on equality, with no identity map, so one object twice and two equal objects look
  alike to it. Both stay, for a future identity-keyed map.
* **The nightly takes every tenth fuzzer draw, not each.** At four draws' worth a draw, forty
  thousand draws a lane would pass the test's twenty-minute cap; `-Dvarka.fuzz.variantsEvery`
  sets the stride, 1 by default.
* **Files beyond 4's table:** `VarkaVariantCheck.scala`, the runner both suites share; the series'
  four batches moved from `VarkaSmallTreeSuite` to `VarkaIrSeries`, unchanged; and
  `dev/varka_series.sh --variants`, which the nightly script and workflow run. The nightly step is
  in the fuzz job, which took 12 of its 90 minutes.
* **The count check holds over a whole series only.** A slice of a sliced run can hold no
  conditional tree; the conditional test holds the untaken arm to its count instead.
