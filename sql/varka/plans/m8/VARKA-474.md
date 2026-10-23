# VARKA-474: one registration point for a type and its lane

## 0. Why this plan exists

Milestone 8 is seven new types, and four of them are still ahead: `TIMESTAMP_NTZ`, `DOUBLE`,
`FLOAT` and `DECIMAL` in two representations. Two have been added under this milestone, and their
diffs are the evidence for what a type costs today:

| Case | Production files | Test files | Docs, generated files | Commit |
| :--- | ---: | ---: | ---: | :--- |
| `BOOLEAN` conditions (row 331) | 17 | 20 | 10 | `5d9beefb8c0` |
| `TINYINT` and `SMALLINT` lanes (row 329, checkpoint) | 14 | 13 | still landing | `33996c315d9` |

The edits are not harm; each file exists for a reason. What is missing is a single place that says
"these are the sites, and here is what this one does": the list lives in plan prose and in the
previous diff, so the next type re-derives it, and a site that compiles without the new type - a
cost-model name, a census cell, a fuzz grammar production - is found later, by a reader or by CI.

This plan does three things, in this order: it records the sites the two cases touched and how each
one is guarded today (1); it makes the mechanical ones a compile-time registration point (2); and it
writes the guide a developer reads before starting the fifth type (3).

## 1. The sites a new type touches, and how each is guarded today

Three kinds of guard exist, and only the first is a compile error:

- **Exhaustive switch** over `VarkaVectorIR.LaneType` or over an IR node kind, with no `default`.
  A new member is a compile error at every consumer. This is the house rule and it already covers
  the emitters' per-kind dispatch.

- **Runtime check.** `Lane.of` scans the `Lane` enum and throws for an unmapped `LaneType`; the
  kernel cache and the evaluator then decline the batch. Adding a member compiles, and the failure
  is a decline at run time, which reads as "nothing fused" rather than "not wired".

- **No guard.** The cost model's feature names (`VarkaEmitCost.kindOf`), the census catalog
  (`VarkaOperatorCensusCatalog`), the coverage rows (`VarkaCoverageRows`), the IR fuzzer's grammar
  (`VarkaIrGrammar`) and the reference evaluator (`VarkaReference`) admit any node they know and
  ignore the rest. A new type is absent from them silently; the tests that would notice are the
  fuzzers and the matrix, and only if a draw reaches the new type.

The site list, with its guard on 22 October 2026 (production sites only; section 7 has each
guard after the refactor):

| Site | What it holds for the new type | Guard today |
| :--- | :--- | :--- |
| `VarkaVectorIR.java` | the `LaneType` member and the leaves that read the type | exhaustive switch |
| `Lane.java` | species constant, width, stride, vector class, conversions | `Lane.of` throws |
| `VarkaNodeCompiler.java` | `laneOf` (Spark `DataType` to lane), leaf construction, `inputLane` | none: an unknown type is an empty `Optional`, a decline |
| `VarkaExpressionCompiler.java`, `VarkaConditionCompiler.java` | the expression and condition dispatch | none: a fall-through decline, whose reason names the shape but not the missing type |
| `Analysis.java` | which nodes are admitted, input lanes, the guards a node arms | exhaustive switch |
| `VarkaBodyEmitter.java`, `VarkaLoopEmitter.java`, `VarkaVectorWalk.java` | the emitted code, per node kind | exhaustive switch |
| `VarkaReference.java` | the scalar semantics the fuzzers compare against | none |
| `VarkaEvaluatorBase.java` | `isArrowBacked`: the Arrow vector the type arrives in | `default -> false` |
| `VarkaKernelEvaluator.java` | `allocateVector`: the Arrow vector the type is written to | exhaustive switch |
| `VarkaKernelRunner.java`, `VarkaWarmupGate.java` | a bitmap input's width and the warm-up's copy | none (the `BitVector` case) |
| `VarkaVectorTemplateWarmup.java` | the lane's blocks: every Vector API method a kernel of the type calls, warmed `THIN_CALLS_PER_ROUND` times a round | `VarkaWarmupReachSuite` fails naming each method the kernels reach fewer than 2,000 times |
| `VarkaIrGrammar.java` | the IR fuzzer's draw | none |
| `VarkaCoverageRows.scala`, `coverage.json`, `docs/sql-varka.md` | the coverage row that makes the type count | the coverage suite fails on an undocumented expression, not on a type |
| `VarkaOperatorCensusCatalog.java` | the census cells the new operators need | none |
| `VarkaEmitCost.java` | the feature names the cost model prices | none |
| `VarkaWidthAuditShapes.scala` | the width audit's per-type shapes | none |

Two of the guards are weaker than the rule they belong to. `isArrowBacked`'s `default -> false`
is the dangerous one: a new lane that reaches it is declined on every batch, and the visible
symptom is a slow query, not an error. And `Lane.of` throwing at run time means the one place that
knows a lane's facts is also the last to be checked.

## 2. The refactor

Three changes, each small on its own, none changing emitted bytes:

**2.1 `Lane.of` becomes an exhaustive switch with no `default`.** A new `LaneType` member then
fails to compile until its `Lane` entry exists, at the one place every consumer already calls.
Half a day including the tests.

**2.2 The evaluator's Arrow knowledge moves into one table.** `isArrowBacked` and
`allocateVector` are today two per-type arms in two files, and the first has a `default`. A
`LaneArrow` record, one per `LaneType`, holding the Arrow vector class, the read and write
accessors and the accept predicate, populated by one exhaustive switch beside the evaluator (it
stays in `sql/core`, beside the two functions that read it), gives the type one more
registration point of the first kind. `isArrowBacked` becomes a lookup, and its `default`
disappears. A day.

**2.3 A checklist script.** `dev/varka_new_lane.py --check <LANE>` walks a curated site list - the
"no guard" rows of section 1, and the warm-up's typed blocks - and reports each site with no
case for the lane, `file:line` and what the site does. It cannot prove the case is right; it proves the site was looked at. Wired into
CI when the diff touches `LaneType` or `Lane`, it runs in seconds. A day.

Success criterion: a new type touches the `LaneType` member, one `Lane` entry, one `LaneArrow`
entry, the genuinely new IR leaves and emitter arms, and its tests; every other omission is a
compile error or a line the script prints.

## 3. The guide

`sql/varka/ADDING_A_TYPE.md`, in the shape of `ADDING_AN_EXPRESSION.md`: the touched-files table
counted from section 1's two cases, then numbered steps in the order the code runs, each ending in
a command whose output says whether the step is done - the lane entry and its compile check, the
leaves, `laneOf`, the dispatch, the guards, the emitters, the reference evaluator, the evaluator's
two functions, the fuzz grammar, the coverage row and its regeneration, the census cells, the cost
names, the benchmark class, and the matrix run over the new emitting suites. A short "what the
guards do not catch" section names the three kinds from section 1, so the reader knows which steps
are checked by the compiler and which by a script.

`AGENTS.md` gains one sentence beside the `ADDING_AN_EXPRESSION.md` pointer, so the entry point is
where a reader already looks.

## 4. Order and dependencies

- 2.1, 2.2 and 2.3 wait for rows 329 and 330 to merge: they edit `Lane.java`,
  `VarkaVectorIR.java` and both evaluator files, and a refactor running beside them would
  conflict on every push.
- The guide (3) can be written first, against today's code, and amended where the refactor moves
  a step; doing it first means the fifth type's developer is not blocked while the refactor runs.

## 5. Verification

- The bytes oracle: `emitted_bytes.json`, `coverage.json` and `width_audit.json` are identical
  before and after 2.1 and 2.2 - a refactor of lookup tables must not move a byte. The
  regeneration switches are for their suites, and the diff against the merge base is the check.
- The suites: `VarkaLaneTypeSuite`, the emitter suites of the types already present, and the
  evaluator's end-to-end suites.
- The script (2.3): run over `BYTE` and `SHORT` after they land, and over every existing lane, and
  the report for each says either "no site is missing" or the list.

## 6. What this plan does not do

It does not generate the emitter arms from a table. The narrow store's conversion and species
selection in `VarkaBodyEmitter` are logic, not boilerplate, and a generator would hide it. It also
does not touch the per-kind exhaustive switches: they are the model the other sites should follow.

It must survive the floating split: row 341 divides `Lane` into an integral and a floating half
(`m8/PLAN.md` 2.3), and a registration point that assumes integral descriptors would have to be
rebuilt for `DOUBLE` and `FLOAT`. The `Lane` entry carries the descriptor family, and the
evaluator's table carries the Arrow vector, so neither assumes an integral lane.

## 7. Outcome (23 October 2026)

Rows 329 and 330 had merged, so the order of section 4 held. The three changes landed in one pull
request; no emitted byte moved.

**2.1** `Lane.of` is a switch over `LaneType` with no `default`. `VarkaLaneTypeSuite` checks that
each entry names its own constant back.

**2.2** `VarkaLaneArrow`, beside the evaluator in `sql/core`, holds each lane's Arrow vectors in one
switch over `LaneType`: per vector, the class, the Spark types stored in it and how a destination
is built. `isArrowBacked` admits a lane's vector through it, and `allocateVector` builds a
destination from it. Two points the plan's wording got wrong are settled in the code. The admission
switch keeps a `default`: Arrow's vector classes are not a sealed set, so an unknown class must
still refuse the batch. What changed is that the classes that arm admits come from the per-lane
switch, so a lane cannot compile without naming them. And two admitted vectors belong to no lane -
a boolean column's `BitVector` and a derived input's `VarCharVector` - so they stay named cases in
`isArrowBacked`, beside the table. The read and write accessors the plan put in the record have no
reader and were left out. The Spark-type-to-lane knowledge now sits in `laneOf` (catalyst) and in
this table (sql/core); row 475's registry is where they become one.

**2.3** `dev/varka_new_lane.py --check [LANE...]` reads the lanes from the code - the `LaneType`
members, each lane's Vector API class from `Lane`, its Spark types from `laneOf` - and walks the
sites no compiler checks: per lane, `laneOf`'s admission, the template warm-up's load of the lane's
vector class, a fuzz-grammar leaf and a benchmark that reads the lane; per Spark type, a coverage
column, a width-audit column and a census entry fused today. The per-type sites are what catch a
new type on an existing lane: `TIMESTAMP_NTZ` added to the long lane in `laneOf` with nothing else
is reported missing at all three. Its one table is the census name of each Spark type, and a type
`laneOf` admits that the table lacks is a finding. A gap a later row owns is printed as owed:
today the fuzz grammar draws no `BYTE` or `SHORT` leaf, which row 352 owns. It runs in the
pre-commit hook when a lane's inputs or a walked site change, in the docs job beside the census
check, and in the varka-scoped catalyst job, since its inputs are Varka sources whose changes do
not run the docs job (a source file cannot join the docs job's trigger list: a change of only
such files would then count as documents and skip the suites).

Section 1's table, read again: `VarkaReference` and `VarkaIrGrammar` switch exhaustively over
`LaneType` for their per-lane parts, so the compiler checks them; the cost model and the census
catalog are keyed by node kind and need no arm for a lane. `allocateVector` was an if-chain ending
in a throw, not an exhaustive switch. After this row the sites stand as:

| Site | Guard after this row |
| :--- | :--- |
| `Lane.java` (`Lane.of`) | exhaustive switch |
| `VarkaLaneArrow.java` (`isArrowBacked`, `allocateVector`) | exhaustive switch over the lanes; `BitVector` and `VarCharVector` named |
| `VarkaNodeCompiler.laneOf` | the script (admission); a decline that names no type until row 475 |
| `VarkaReference.java`, `VarkaIrGrammar.java` | exhaustive switches for the per-lane parts; the script for a grammar leaf |
| `VarkaCoverageSuite.scala`, `VarkaWidthAuditShapes.scala`, `census.tsv` | the script, per Spark type |
| `VarkaVectorTemplateWarmup.java` | the script for a load of the lane's vector; `VarkaWarmupReachSuite` for every method |
| `VarkaEmitCost.java`, `VarkaOperatorCensusCatalog.java` | keyed by node kind: a new node's arm, not a new lane's |

Run over the four lanes and their eight Spark types: no site is missing but the grammar leaf of
`BYTE` and `SHORT`, owed to row 352.
