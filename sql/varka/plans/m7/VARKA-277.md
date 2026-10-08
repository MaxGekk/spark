# VARKA-277: One shrinker for every fuzz failure

## 1. Where this came from

Row 277 of `m7/PLAN.md`, from `m7/READING.md` 11 (Zeller and Hildebrandt, "Simplifying and
Isolating Failure-Inducing Input", on delta debugging): ddmin's result is 1-minimal by
construction, and a single culprit costs about 2 log2 n tests. The plan lists four rows that
produce failures a person would otherwise cut down by hand and so need it first: 262 (vanilla
Spark as the oracle, with "a failure shrunk to its smallest failing subtree and saved, the saved
reproducers replayed in PR CI"), 269 (every small tree), 278 (variants that must agree) and the
option matrix, whose skip entry names "the minimal delta from the defaults" (`m7/PLAN.md` 2).

Today a fuzz failure prints its seed, iteration, roots in canonical form, the options record,
the batch length, the null patterns and the literals, and is replayed with
`-Dvarka.fuzz.seed=... -Dvarka.fuzz.only=...`. Nothing reduces it. What the fuzzer drew is what the
reader gets: a tree of fifteen nodes, a 33-field options record and a batch of a thousand rows.

## 2. The admission check, done

The question is whether a shrinker has failures to work on, and what they look like. The repo
has three deliberate bugs, the options of reason `FAULT_INJECTOR` that the fuzzers leave out
(`misdescribeAdd`, `misdescribeWordLiveness`, `misdescribeDriverBytes`). Each was set through
`-Dvarka.matrix.config` and `VarkaIrFuzzSuite` run at its default seed, on 8 October 2026, on
`origin/master` at `da8b26f2ee1`:

| injector | what the suite did |
|---|---|
| `misdescribeWordLiveness` | three tests fail at iteration 0. The first failing shape is a 15-node tree under a 33-field options record: `(and (and (cmp:EQ lit:1 col:0) (cmp:EQ lit:0 col:0)) (cmp:EQ (addDays col:0 lit:0) (weekOfYear (thursdayOf col:0))))` |
| `misdescribeAdd` | the suite does not fail a test: the generated class throws `NoSuchMethodError` for `IntVector.add` out of the kernel's `loopMasked0`, which escapes every `assert`, and sbt reports "Uncaught exception when running VarkaIrFuzzSuite" |
| `misdescribeDriverBytes=1` | all five tests pass: the IR fuzzers do not exercise it |

Three things follow, and the second and third change the design.

1. **Failures are findable and large.** The first injector's first failure is already 15 nodes
   and one of 33 option fields, so a reduction has something to remove at every stage.
2. **A failure is not always an assertion.** A bug in the emitted class surfaces as a
   `LinkageError` or `VerifyError` from the generated code, not as a `TestFailedException`, and
   it takes the whole suite down. The shrinker's predicate must catch every `Throwable` and
   classify it, or it cannot shrink the failure the second injector produces.
3. **One option is not caught by the IR fuzzers at all.** That is a coverage fact for a different
   row (262's vanilla-Spark arm reaches the driver), and the shrinker leaves it alone; it is
   recorded in 7.

What the check would have rejected: a design that shrinks only `TestFailedException`s.

## 3. The design

### 3.1 A case is a value

`VarkaIrFuzzSuite.runOne` draws and runs in one method, and the draw uses a `Random` that the
null patterns share, so a case cannot be re-run with a part changed. It becomes two steps:

* `VarkaFuzzCase`, a record of everything a run reads: the lane (int or long), the roots,
  `numInputs`, the literals, the batch length, one null bitmap per input (a pattern is a
  function today; it is materialised), the data, `forceMasked`, the options, and the two input
  ordinals whose domains are constrained (`smallOrdinal`, `levelOrdinal`), which a move must not
  break.
* `run(case): Outcome`, which is `Pass`, `Skipped` (the class-file cap, `emitOrSkip`'s existing
  policy, which is a pass for the shrinker) or `Failed(signature, message)`.

`runOne` becomes `run(draw(iteration))`. This commit changes no behaviour: the same seeds draw
the same cases and fail with the same messages.

### 3.2 The moves, in the row's order

The shrinker keeps a failing case and tries smaller ones; a candidate is kept only if it fails
with the **same signature** (3.3), so that a reduction does not slide to a different bug.

1. **Roots.** ddmin over the root list: drop outputs. `numInputs` and the literal table do not
   shrink; indices stay valid and an unused column is harmless.
2. **Tree**, roots first, then level by level (*correction:* a node that is an operand whose
   domain the grammar constrains, the month count of `add_months` and the level of a dynamic
   truncation, is left alone with everything under it, since the data of those two columns is
   drawn to fit them and a replacement would build a case outside the kernel's contract), so the largest subtrees go first. Two moves per
   node: *hoist* a child of the same kind (value for value, condition for condition) in the
   node's place, and *replace* the node by the smallest leaf of its lane, a `ColumnRef` or
   `LiteralSlot` of an index already present. A candidate is built with the new
   `VarkaVectorIR.withChildren(node, children)`; the IR's constructors refuse an ill-typed tree
   (a calendar node over a long, a condition where a value goes), and that exception means "not a
   candidate". This is how the candidates stay well typed without a second type checker.
3. **Options.** ddmin over the set of options that differ from `VarkaEmitOptions.DEFAULTS`,
   resetting a subset to the default. The result is the matrix's "minimal delta".
4. **Batch.** The length by halving (the data and bitmaps cut with it), the null bitmaps toward
   all-valid and then toward fewer nulls, each column's data toward 0 and 1 where the column is
   not a constrained one, `forceMasked` toward false.

The four repeat until none changes anything (a tree move can unlock an option reset), bounded by
a candidate cap and a wall-clock cap so a pathological case cannot hold a CI job.

### 3.3 The signature

What groups failures and what the shrinker must preserve. It is the failure's **kind** and its
**normalised message**:

* kinds: a mismatch (output, validity, selection), a declined batch (status not zero), an
  emitter rejection or invariant (`IllegalArgumentException`, `IllegalStateException` from the
  emitter), a generated-class error (`LinkageError`, including `VerifyError`), a memory-sanitizer
  violation (`VarkaMemoryViolation`), and any other `Throwable` by its class;
* the message is the text with numbers, hex and the `seed=... iteration=...` context stripped to `#`,
  so row 7 and row 9 of the same mismatch are one signature and a `NoSuchMethodError` keeps its
  method name.

A candidate that fails with another signature is not smaller, it is a different failure; it is
reported once as a second finding.

Reproducibility: a failure that depends on the JIT (a trap after a warm-up) may not repeat. A
candidate counts as failing only if it fails with the signature, once for the deterministic
kinds, and the final shrunk case is run three times; one that does not repeat is reported as
unstable, with the last case that did.

### 3.4 Known failures

`sql/varka/fuzz/known_failures.tsv`, tab separated, in the form of `matrix/skips.tsv`: the
signature, the shrunk case on one line, and a reason that names a plan row or ticket. A
fuzzer's failure whose signature is on the list is reported as known and does not fail the run; a
new signature fails it with its shrunk case. **An entry that no run reproduces fails the stale
check** (`VarkaKnownFailuresSuite` replays each), as the skip list's entries do, so the list can
only shrink once a bug is fixed. It starts empty: at this master no fuzzer fails.

The shrunk case is printed in one line for a person. *Correction, made while building:* this
section first said a `VarkaFuzzCase.parse` would read that line back. The IR has no text parser
and writing one for forty node types is its own task, so an entry is replayed by its lane, seed
and iteration instead: the generator redraws the case and the run must fail with the entry's
signature. Saving shrunk reproducers as files, and replaying them in PR CI, is row 262's, and
needs that parser.

### 3.5 What is deliberately unchanged

* The grammar (`VarkaIrGrammar`), the draws, the oracle (`VarkaReferenceEvaluator`) and
  `VarkaKernelCheck`: the shrinker calls them.
* The composition fuzzer (`VarkaCoverageCompositionFuzzSuite`), which draws Catalyst expressions,
  not IR. Its failure has the same parts (a list of picked entries, the options, the batch), and
  the shrinker core is written over a case and a move set so that it can follow; it is a second
  commit if the first leaves time, and a follow-up row if not (decided at 8.7).
* Product code, with one exception: `VarkaVectorIR.withChildren`, the inverse of the existing
  `childrenOf`, an exhaustive switch over the sealed interface so that a new node type refuses to
  compile until it says how it is rebuilt.

### 3.6 Registered op counts

None move: no emitted code changes. The proof is `emitted_bytes.json` unchanged.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/src/main/java/.../codegen/varka/VarkaVectorIR.java` | `withChildren` |
| `sql/catalyst/src/test/scala/.../varka/VarkaFuzzCase.scala` | the case, its text form and `run` |
| `sql/catalyst/src/test/scala/.../varka/VarkaShrinker.scala` | ddmin and the four moves |
| `sql/catalyst/src/test/scala/.../varka/VarkaFailureSignature.scala` | the kinds and the normalisation |
| `sql/catalyst/src/test/scala/.../varka/VarkaIrFuzzSuite.scala` | draw and run split; failures shrunk |
| `sql/varka/fuzz/known_failures.tsv` | the known list, empty |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-277.md` | the row and this plan |

## 5. Tests, and what each is for

| test | what it catches that no other would |
|---|---|
| `VarkaShrinkerSuite`: ddmin on synthetic predicates | a 1-minimal result on lists with one, two and scattered culprits, and the candidate count near 2 log2 n |
| the same: tree moves over every node type | `withChildren` rebuilding each of the 40 node types, and an ill-typed candidate refused, not run |
| the same: signature stability | a reduction that slides to a different failure is rejected |
| `VarkaShrinkerInjectorSuite` | end to end on `misdescribeWordLiveness` and `misdescribeAdd`: the shrunk tree, delta and length are within 6.1's bounds, and the `NoSuchMethodError` is classified, not escaped |
| `VarkaKnownFailuresSuite` | a stale entry fails; a known signature does not fail a run; a new one does |
| `VarkaIrFuzzSuite` unchanged | the refactor drew and ran the same corpus |

## 6. The measurement

Not a timing: what the shrinker produces for the two injectors, recorded in section 9 with the
candidate count and wall time of each.

### 6.1 Predictions, registered before the run

1. **`misdescribeWordLiveness`'s first failure shrinks to at most 5 nodes and one root**, with an
   options delta that is the injector alone or the injector and at most two others, and a batch
   length of at most 17.
2. **`misdescribeAdd`'s shrinks to a tree containing an addition**, with the delta being the
   injector, and the `NoSuchMethodError` as its signature.
3. **Each shrink takes at most 600 candidate runs and 60 seconds**, and the shrunk case replays
   three times out of three.
4. **The refactor of `runOne` changes nothing**: the suite's counts and the seeds' first failures
   under each injector are what 2 recorded.

## 7. Risks

1. **A move that builds a tree the emitter accepts and the oracle misjudges**, reducing a real
   failure into a harness failure. The signature equality guards it: a different message is a
   different signature.
2. **Shrinking is slow because each candidate emits and loads a class.** The two caps bound it;
   the candidate count is in section 9.
3. **Normalisation merges two bugs into one signature**, or splits one into two. Both degrade the
   known list, not correctness; the signature is `kind` plus text, and a merge shows as a known
   entry that stays reproduced after its bug is fixed and another is found.
4. **`misdescribeDriverBytes` is not reached by the IR fuzzers** (2.3). It is not this task's.

## 8. Sequencing

1. This plan, with the row marked Planned.
2. `VarkaFuzzCase`, `run` and `runOne` as `run(draw(i))`, nothing else (6.1.4).
3. `withChildren` and its test over every node type.
4. The ddmin core and the tree moves, with synthetic tests.
5. Options and batch moves; the signature; the known list and its stale check.
6. The suites wired to shrink, the injector tests, section 9.
7. The composition fuzzer, or a follow-up row.

## 9. Outcome

Done on 8 October 2026.

**What was built.** `VarkaFuzzCase` and a draw/run split of `VarkaIrFuzzSuite` (no behaviour
change); `VarkaVectorIR.withChildren`; `VarkaShrinker` (ddmin over the roots, the trees, the
options and the batch); `VarkaFailureSignature`; `VarkaKnownFailures` and
`sql/varka/fuzz/known_failures.tsv`, empty, with a stale check. A failure in either lane of the IR
fuzzer is now a test failure of its own with the shrunk case in the message, and it includes a
failure out of the generated class, which used to end the suite as an uncaught error.

**Predictions scored.** On the first failing draw of each planted bug, `VarkaIrFuzzSuite` at its
default seed:

1. **Held.** `misdescribeWordLiveness`: 15 nodes to 2 (`(isNotNull col:0)`), one root, the options
   delta the injector alone, 7 rows to 1, in 15 runs and under 100 ms. The bound was 5 nodes,
   at most two other options and 17 rows.
2. **Held, with a note.** `misdescribeAdd`: 15 nodes to 5, `(cmp:EQ (addDays col:0 lit:0) col:0)`,
   the injector alone, one row, in 36 runs, signature `NoSuchMethodError` on `IntVector.add`. It
   stops at 5 and not at the addition's 3 because the root is a condition and a move keeps a
   condition a condition: the result is 1-minimal under the moves, not the smallest tree there is.
3. **Held.** Both took under 600 runs and under a second, and each replays three times of three.
4. **Held.** The default suite's five tests pass, and the first failure under
   `misdescribeWordLiveness` is character for character what it was before the split.

**What the admission check had right.** Failures are not always assertions: the second injector
escaped every `assert` and ended the suite before this task, and is now classified and shrunk.
The third (`misdescribeDriverBytes`) is still not reached by the IR fuzzers.

**What it did not do.** The composition fuzzer, which draws Catalyst expressions and not IR, is
unshrunk; the core is written over a case and a move set so that it can follow. That is row 294.
Saved reproducers need a text form of a case that parses (3.4's correction).

**Run under the matrix.** The new suites pass under `cse=false`, `methodByteBudget=0` and
`lanesOverride=4`.
