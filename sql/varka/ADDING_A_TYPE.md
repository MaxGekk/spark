# Adding a type to Varka

Milestone 8 adds seven types: two have landed in part, and four have not started (`TIMESTAMP_NTZ`,
`DOUBLE`, `FLOAT` and `DECIMAL`; `BOOLEAN` has its conditions and not yet the condition-to-value
bridge, row 460). This page is the path a type takes
through the engine, in the order the code runs, with the file named for each step and a command
that says whether the step is done.

Read [the architecture guide](../../docs/sql-varka.md) first, at least its glossary - *lane*,
*word*, *morsel* and *epilogue* mean something specific here - and skim
[adding an expression](ADDING_AN_EXPRESSION.md), because most of a type's expressions arrive by
that path once the type has a lane.

## The shape of the change

The two types added under this milestone touched these files; the counts are from their diffs.

| Case | Production | Tests | Docs, generated |
| :--- | ---: | ---: | ---: |
| `BOOLEAN` conditions (`5d9beefb8c0`) | 17 | 20 | 10 |
| `TINYINT` and `SMALLINT` lanes (`33996c315d9`, the one before the coverage work) | 14 | 13 | still landing |

The list is long because a type is not one feature: it is a lane in the IR, leaves that read it,
a rendering in the emitter, a way in and out of Arrow, and rows in the census and the coverage
table. What is missing today is a single place that says so; `plans/m8/VARKA-474.md` is the plan
to add it, and the steps below are the list until it lands.

## Where to read first

In this order, because each file names the next:

1. `VarkaVectorIR.java` - the `LaneType` member and the leaves that read a column of the type.
2. `Lane.java` - the runtime facts of a lane: its Vector API class and species constants, its
   width in bits and bytes, its conversions. `Lane.of` maps a `LaneType` to its entry.
3. `VarkaNodeCompiler.java` - `laneOf` (Spark `DataType` to lane), and where a leaf is built.
4. `Analysis.java` - which nodes are admitted, on which lane their inputs are read, and the
   guards a node arms.
5. `VarkaBodyEmitter.java`, `VarkaLoopEmitter.java`, `VarkaVectorWalk.java` - the emitted code,
   per node kind. The body emitter is the dense body, the loop emitter the groups, and the walk
   the node dispatch they share.
6. `VarkaEvaluatorBase.java` - `isArrowBacked`, where an Arrow column becomes a kernel input.
   `VarkaKernelEvaluator.java` - `allocateVector`, where an output goes back to Arrow.
7. `VarkaReference.java` - the scalar semantics the fuzzers compare against.
8. `VarkaIrGrammar.java` - what the IR fuzzer can draw.
9. `VarkaCoverageRows.scala` - the rows that make the type count as fused, and the two files
   they generate.

## The path

**1. Decide the lane and the representation, before writing anything.** A type either joins an
existing lane (`TIMESTAMP_NTZ` is a 64-bit count, `DATE` is already on the int lane) or needs a
new one (`BOOLEAN`'s bitmap, `DECIMAL`'s 128-bit pair). A type that fits an existing lane may
still need its own *stored* width: `TINYINT` is computed on the int lane and stored a byte a row.
Write the answer down in the row before the code: it decides every later step.
Check: read `VarkaVectorIR.LaneType` and `Lane` and say which entry the type uses, or which two
numbers (width in bits, bytes a row) its new entry carries.

**2. Add the lane's facts.** A new `LaneType` member, its `Lane` entry - the vector class name,
the species constants, the stride, the conversions - and the matching arms in every `switch` over
`LaneType`. The switches have no `default`, so the compiler lists the sites.
Check: `build/sbt catalyst/compile` - it fails at each missing case, and passes when they are all
there.

**3. Add the leaves.** A leaf names the column's type in the IR and carries the validity word.
A leaf that is also a condition (`BoolColumn`) carries its two words instead.
Check: `build/sbt 'catalyst/testOnly *VarkaLaneTypeSuite'`; extend it with the new member's width,
stride and lane.

**4. Teach the compiler.** `laneOf` in `VarkaNodeCompiler`, the leaf construction beside it, and
the family that claims the type's expressions (`VarkaExpressionCompiler`,
`VarkaConditionCompiler`, or a `Varka*Compiler` they chain). A type that must decline says why,
with the reason enumerating what it needs - the decline reasons are what a reader sees when a
query does not fuse.
Check: `build/sbt 'catalyst/testOnly *VarkaExpressionCompilerSuite'`.

**5. Teach the analysis.** Which nodes are admitted on the new lane, the lane the inputs are read
at, and any guard the node arms (an overflow check, a range guard). A guarded node parks its
operands; a node whose result is stored at another width says so.
Check: `build/sbt 'catalyst/testOnly *VarkaRangeAnalysisSuite'` and the family's emitter suite.

**6. Emit it.** The body emitter, the loop emitter and the walk, per node kind. Follow the
exhaustive-switch rule: a new node kind is a compile error at each of them, and a silent
fallthrough is the bug the rule exists to prevent.
Check: the family's emitter suite, and the bytes oracle: regenerate
`VARKA_BYTES_REGEN=true build/sbt 'catalyst/testOnly *VarkaEmittedBytesSuite'` and see that only
the new shapes moved in `sql/varka/emitted_bytes.json`.

**7. Add the reference semantics.** `VarkaReference` computes the node scalar-wise; the IR fuzzer
compares the kernel against it. A node the reference does not know is a node the fuzzer cannot
check.
Check: `build/sbt 'catalyst/testOnly *VarkaIrFuzzSuite'`.

**8. Let the fuzzers draw it.** `VarkaIrGrammar` for the IR fuzzer, and the Spark-level fuzzer's
fixture for the type's columns.
Check: `build/sbt 'catalyst/testOnly *VarkaIrFuzzSuite'` and
`build/sbt 'sql/testOnly *VarkaSparkFuzzSuite'`.

**9. Admit it in and out of Arrow.** `isArrowBacked` for the vector the type arrives in,
`allocateVector` for the vector it is written to, and `VarkaKernelRunner` for any input whose
stored width is not its type's (a bitmap is one bit a row, and its Arrow vector reports a type
width of zero). The sanitizer registers the rows a call can reach, not the allocation's padding.
Check: `build/sbt 'sql/testOnly *VarkaKernelEvaluatorSuite *VarkaWarmupEndToEndSuite
*VarkaMemorySanitizerEndToEndSuite'`.

**10. Give it coverage rows.** A load, a comparison, a choice and an output in
`VarkaCoverageRows.scala` - without them the type does not count as fused, whatever the emitter
does. Regenerate in this order: `VARKA_COVERAGE_REGEN=true build/sbt 'catalyst/testOnly
*VarkaCoverageSuite'`, then `VARKA_AUDIT_REGEN=true build/sbt 'catalyst/testOnly
*VarkaWidthAuditSuite'`, then `VARKA_BYTES_REGEN=true build/sbt 'catalyst/testOnly
*VarkaEmittedBytesSuite'`, then `VarkaCoverageSuite` again, and check that only the new rows moved
in `coverage.json`, `docs/sql-varka.md`, `width_audit.json` and `emitted_bytes.json`.
Check: `python3 dev/varka_census.py --check`.

**11. Census the operators the type needs.** New operators get a cell in
`VarkaOperatorCensusCatalog`, measured on the laptop by
`VARKA_CENSUS_REGEN=true build/sbt 'catalyst/testOnly *VarkaOperatorCensusSuite'`; the runner
columns come from the next `varka-width-audit.yml` dispatch, which needs the branch pushed.
Check: `python3 dev/varka_census.py --check` and the census suite.

**12. Price it.** `VarkaEmitCost.kindOf` names a node's cost feature; a node that falls to
`default` is priced as its class name alone, which lumps unlike emissions together. New nodes
join the awaiting-refit list (row 352) until the model is refitted.
Check: `build/sbt 'catalyst/testOnly *VarkaEmitCostSuite'`.

**13. Benchmark it.** A new feature family gets its own benchmark class beside
`VarkaLaneConversionBenchmark`, on `VarkaBenchmarkBase`, every arm driven to level-4 code before
it is timed (row 407's harness rule). Results are committed, not quoted.
Check: the benchmark's own results file, and the row's numbers against it.

**14. Run the matrix over the new emitting suites.** A new emitting suite runs under every
configuration, not only the one a PR's scoped CI run picks:
`dev/varka_matrix.sh --all --module catalyst --suites <the new suites> -j 4`, or `--module sql`
for the suites that live in `sql/core`, about 40 minutes, plus the defaults. A test whose premise
an option changes pins it, cancels under it, or gets a line in `sql/varka/matrix/skips.tsv` with
the reason.
Check: the matrix's table - every row `fail 0`, `unfused 0`, and no stale skip line.

**15. The linters, before the push.** `./dev/varka_errorprone.sh --master "$(git rev-parse
origin/master)"`, `./dev/lint-java`, `./dev/lint-scala`, `python3 dev/varka_census.py --check`,
the 100-column and non-ASCII scans, and the pre-commit hook.
Check: their exit codes, reported in the pull request.

## What the guards do not catch

Three kinds of site exist (see `plans/m8/VARKA-474.md` 1): exhaustive switches, which the compiler
checks; runtime checks such as `Lane.of`, which throw when a kernel of the new type first runs;
and sites with no guard at all - the cost model, the census, the coverage rows, the fuzz grammar,
the reference evaluator, the width audit's shapes. Between them sit the decline sites:
`VarkaNodeCompiler.laneOf` returns an empty `Optional` for a type it does not know, and the
expression and condition compilers fall through to a decline with a reason. A decline is the right
answer for an unsupported shape, but for a *new type* it is indistinguishable from "not implemented
yet" unless the reason names the type, so steps 4 and 5 are where a missing arm costs an hour of
confusion. Steps 7 to 12 are the unguarded sites, and their check commands are the only thing that
will notice a miss.

Two of today's guards are weaker than they look. `VarkaEvaluatorBase.isArrowBacked` ends its
`switch` over Arrow vector classes with `default -> false`, so a lane that reaches it is declined
on every batch and the symptom is a slow query; and `Lane.of` throws at run time rather than
failing the build. Plan `VARKA-474.md` replaces both with compile-time registration points.
