# VARKA-294: Shrink the composition fuzzer's failures

## 1. Where this came from

Row 294 of `m7/PLAN.md`, left by VARKA-277: the shrinker of 277 reduces an IR fuzz case (roots,
options, batch) and its core was written over a case and a move set "so that it can follow". The
composition fuzzer, `VarkaCoverageCompositionFuzzSuite`, draws Catalyst expressions and not IR: a
projection of one to three hundred picked coverage rows, a filter of one to sixty-four, and a wide
projection of 150 to 300 rows spread over eighty copies of the table's columns whose kernels are
run against the reference evaluator. A failure there names the seed, the iteration and every
picked row, which for 250 rows is a page of SQL to read before the one that matters.

## 2. The admission check, done

What the shrinker needs from 277, checked on 9 October 2026 against `master` (`beba41ed989`):

* **`VarkaShrinker.ddmin`** is generic (`ddmin[A](Vector[A])(fails)`), 1-minimal, and tested.
* **`VarkaFailureSignature.of`** takes a `Throwable` and a label and keeps the kind and the
  number-free first line of the message. It already classifies the harness's messages (output,
  validity and selection mismatches, a declined batch, an emitter rejection, a generated class's
  `LinkageError`); the composition properties' own failures (`entries neither fused nor
  declined`, `which fuses alone, declined for '...'`, `the compiler threw`) fall to the default
  kind, the exception's class and the line, which is enough to group them.
* **`VarkaShrinker.shrinkOptions`** is private and typed on `VarkaFuzzCase`, but its body is a
  `ddmin` over the options that differ from the defaults; it is lifted to a function of the options
  and a predicate, and the IR shrinker calls it, so there is one copy.
* **`VarkaKnownFailures`** parses `lane<TAB>seed<TAB>iteration<TAB>kind<TAB>text<TAB>reason` and
  accepts the lanes `int` and `long`. The composition kinds (`projection`, `predicate`, `wide`)
  are added to what it accepts, and the stale check replays them with the entry's own seed.
* **A real failure to shrink**: the IR fuzzer's planted-bug options (`misdescribeAdd` makes the
  emitted class fail to link wherever an add-days node is emitted) break every wide kernel that
  holds a `date_add`, which a draw of 150 to 300 rows almost always does. So the shrinker is
  tested on a real failure of the real property that starts at hundreds of rows, and not only on
  a synthetic predicate.

What the check would have rejected: a shrinker that needed a case type of its own copy of ddmin and
of the options reduction. Neither is needed.

## 3. The design

### 3.1 The case

`VarkaCompositionCase(kind, entries, options, checkSeed, label)`. An entry is the row's text and
its resolved Catalyst expression (already moved onto its copy of the columns for a wide
projection). `kind` is `projection`, `predicate` or `wide`. The three tests' bodies become a draw
(`seed`, `iteration` to a case) and a check (a case, or an exception), so the same check runs a
drawn case and a shrunk one. The wide check's batches come from a `Random` seeded with `checkSeed`
drawn at the end of the picks, so a shrunk case is checked against the same batches the original
was; this moves the wide test's batches against earlier seeds (its picks, copies and options are
drawn as before).

### 3.2 The moves

In the order 277 uses, repeated until a pass changes nothing:

1. **The entries**, by `ddmin`. A projection of 250 rows with one bad row ends at that row; a
   conjunction's order is kept.
2. **Each entry's expression**: a node is replaced by one of its children of the same data type, in
   level order from the root, kept when the case still fails with the signature. This is the "tree
   move" of 277 for a Catalyst tree, and it only builds expressions the analyzer already made
   (a child of the right type is already well typed there).
3. **The options**, by the lifted `shrinkOptions` over the lanes override and the exact grouping.

A candidate is kept only if it fails with the original's signature; a pass, a skip or another
failure is "not smaller", as in 277.

### 3.3 Reporting and the known list

A failure not on the known list is shrunk and rethrown as a test failure whose message has the
original and the shrunk case (kind, options, the entries' SQL one per line, the signature, runs
and time, and how to replay: `-Dvarka.fuzz.seed`, `-Dvarka.fuzz.only`). A failure with a listed
signature is reported as known and not shrunk. `-Dvarka.fuzz.shrink=false` leaves a failure as
drawn, as the IR fuzzer's flag does.

### 3.4 What is deliberately unchanged

The compiler, the emitter, the coverage table, the draws of picks, copies and options (so the
seeds that found past bugs draw the same compositions), the Spark differential of 262 (its
reproducers have their own shrinker and its own SQL-level moves, row 298) and the IR fuzzer's
behaviour.

### 3.5 Registered op counts

None; no emitter change.

## 4. Files

| file | what |
|---|---|
| `VarkaCompositionCase.scala`, `VarkaCompositionShrinker.scala` (test) | the case, the moves |
| `VarkaCoverageCompositionFuzzSuite.scala` | draw and check apart, shrink on failure, tests |
| `VarkaShrinker.scala` | `shrinkOptions` lifted to options and a predicate |
| `VarkaKnownFailures.scala`, `known_failures.tsv` header | the composition lanes |
| `sql/varka/skills/testing-and-debugging.md` | how a composition failure is read |

## 5. Tests, and what each is for

* **A planted bug shrinks from hundreds of rows to a few.** `misdescribeAdd` on a drawn wide case:
  the shrunk case holds at most three entries, one of them a `date_add`-bearing tree reduced to
  at most a handful of nodes, the option delta names `misdescribeAdd=true`, the signature is a
  generated-class `NoSuchMethodError`, the shrink stays inside its budget and is stable. This is
  the test no synthetic predicate can be: the failure is the emitter's, found through the whole
  compile, group and run path.
* **ddmin over entries finds a pair.** A synthetic property that fails when two named rows are
  both present shrinks a list of sixty to exactly those two.
* **Tree moves.** A property that fails when a tree contains a given leaf shrinks a nested
  expression to the path from the root to it, replaced by that leaf where the types allow.
* **A failure fails its test with the smaller case in the message**, as the IR fuzzer's does.
* **A known signature is reported, not failed,** and **every known composition failure still
  reproduces** (vacuous while the list is empty, as the IR fuzzer's is).
* The three existing tests keep their assertions; their draws are unchanged, so a green run before
  and after is the proof that the refactor did not move them.

## 6. The measurement

Not a performance task. The shrink's cost is the evidence: the planted case's run count and wall
time against the 600-run and 60-second budget of 277.

### 6.1 Predictions, registered before the run

1. The planted wide case shrinks to at most three entries within 300 runs and 30 seconds.
2. The three existing tests pass unchanged on seed 20260925 (the default) with no failure.
3. The option reduction ends at the planted option alone, or with one more.

## 7. Risks

1. **The wide check's batches change** against earlier seeds (3.1); no failure is known for them.
2. **A tree move can build an expression the analyzer would not**, if a child's data type matches
   but its nullability or collation differs; the check then throws something else, the signature
   differs, and the candidate is rejected.
3. **A kernel counter in class names** (`kernelCounter`) advances during a shrink; it only names
   classes.

## 8. Sequencing

1. This plan, the row marked Planned.
2. The lifted `shrinkOptions`, the case and the shrinker with their synthetic tests.
3. The suite's refactor, the planted-bug test and the known-list change.
4. The skill note and section 9.

## 9. Outcome

Filled in when the shrinker has run on the planted case.
