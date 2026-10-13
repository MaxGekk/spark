# VARKA-138: three stronger test forms for every coverage row

## 1. Where this came from

Row 138 started as one of milestone 5's open items (item 39) and was refined by the paper reading
of 11 October 2026 (`m7/READING.md` 11) and by `m7/PLAN.md` 2.2. It asks for three test forms over
the rows of `sql/varka/coverage.json`, the table of every expression shape Varka fuses:

1. **Poisoned null lanes in each row's masked body.** A null lane's data holds an extreme value,
   not a harmless one, and the kernel's masked body - the one it runs when an input has nulls -
   meets it, so a kernel that computes on a null lane's data and lets the result leak shows.
2. **Output validity pre-filled both ways.** The harnesses fill an output's validity bitmap with
   ones before the kernel writes it, which catches a kernel that fails to clear a bit and never one
   that fails to set it. EMI's lesson (Le, Afshari and Su, PLDI 2014) is to vary what the program
   must not depend on; the fill is such a thing.
3. **A ternary partition oracle for every filter row, and the ANSI form.** For a predicate `p`,
   the rows where `p` is true, where `NOT p` is true and where `p IS NULL` must split the table
   exactly, compared on rows, not counts (Rigger and Su, OOPSLA 2020, p. 20: counts missed 5 of 48
   bugs). Spark rewrites these partitions before Varka sees them (`BooleanSimplification`,
   `NullDownPropagation`, `InferFiltersFromConstraints`), so the oracle runs with those rules
   excluded too. Under ANSI, an error compares on its class, its SQLSTATE and its message
   parameters, as Spark's own `checkError` does, and Varka must not raise where Spark does not
   evaluate: after a residual conjunct, the right side of AND and OR, the untaken arm of CASE and
   IF, later COALESCE arguments.

The owner chose it as the next test-strength row on 13 October 2026.

## 2. The admission check, done

Probes on 13 October 2026, in this worktree at `eca9171de4b` (master after #783). The probe code
was a throwaway copy of `VarkaCoverageDifferentialSuite` with two extra tests and a system property
read in `VarkaKernelChecks`, all removed before the plan was committed.

**Form 1 is already in the record.** `VarkaKernelChecks.runOne` writes a poison value into every
null lane of every input (`VarkaEmitterFixtures.poison`, VARKA-70's harness rule), and a batch
whose inputs have no nulls can report one anyway (`forceMasked`) so the masked body runs.
`runAndCompare` then runs each compared batch through the body it did not take (`otherBody`), so
both bodies meet the reference evaluator. `VarkaCoverageCompositionFuzzSuite`'s test "every
coverage row's kernel agrees with the reference evaluator, and the reference with Spark's eval"
runs each of the 92 rows alone, at both widths the bytes oracle pins, on four drawn batches, and
holds over 100,000 answers to Catalyst's interpreted `eval` as well. What is left is chance: each
input's null pattern is drawn as none, every fifth row or random, so a one-input row meets no null
lane at all in about 1.2% of its four batches' draws per width ((1/3)^4).

**Form 2: zero-filled, nothing fails, and zero is what production does.** With the fill read from
a property (`VarkaKernelChecks.java:603`, `0xFF` today) and set to `0x00`,
`VarkaCoverageCompositionFuzzSuite`, `VarkaIrFuzzSuite` and `VarkaSmallTreeSuite` passed: 31 tests,
220 s. The property was shown to reach the forked test JVM by setting it to `bogus`, which failed
the per-row test with `NumberFormatException: For input string: "bogus"`. Production allocates
every output with Arrow's `allocateNew` (`VarkaKernelEvaluator.java:533`,
`VarkaFilterEvaluator.java:382` and 428), and a standalone check on Spark's classpath read 0 of
8,000 validity bits set after `allocateNew` on `IntVector`, three rounds, each after another
vector had set every bit of a chunk of the same size. So the harness has tested only the fill
production never uses; the zero fill is the one that matters, and it passes today.

**Form 3, the partitions: they agree, and `p IS NULL` never reaches Varka.** For each of the 27
predicate rows, on the suite's three fixtures (nulls mixed, none, all), under the default optimizer
and with the three rules excluded: `SELECT * FROM fixture WHERE p`, `... WHERE NOT (p)` and
`... WHERE (p) IS NULL` on Spark and on Varka, 486 queries.

| optimizer | `p` fused | `NOT p` fused | `p IS NULL` fused | rows differing |
| --- | ---: | ---: | ---: | ---: |
| default | 81 of 81 | 81 of 81 | 30 of 81 | 0 |
| three rules excluded | 81 of 81 | 81 of 81 | 0 of 81 | 0 |

Every partition answered as Spark's, and the three partitions' sizes summed to the table's in all
162 cases. Under the default optimizer `NOT (d < d2)` reaches Varka as
`isnotnull(d) AND isnotnull(d2) AND d >= d2`, as the reading said; with the rules excluded it
reaches Varka as `NOT (d < d2)` and is fused as written. `(p) IS NULL` is fused only where
`NullDownPropagation` has turned it into null checks on the columns; as written, Varka fuses it
for no row. So the third partition can be checked only as the complement of the two Varka
computes, and Varka evaluating the unknown truth value directly is a gap of its own (9, candidate
row).

**Form 3, ANSI: errors agree, and two of the four positions never reach Varka.** Each of the 51
of the 65 projection rows that read only `d`, `d2`, `i`, `l`, `ym`, `t` and `dt` was run over a
two-row cached fixture of those seven columns, one row at the
types' extremes (`9999-12-31`, `Int.MaxValue`, `Long.MaxValue`, the largest month and day-time
intervals, `23:59:59.999999`) and one ordinary, in four positions: alone, the THEN of a CASE whose
condition is false on both rows and that has no ELSE, the second argument of `coalesce(1, e)`, and
the right side of an AND whose left side is false. 204 queries, each outcome - the rows, or the
error's condition, SQLSTATE and parameters - compared between the engines: all 204 agreed. Nine
rows raise alone: 5 `ARITHMETIC_OVERFLOW`, 1 `CAST_OVERFLOW`, 1
`DATETIME_FIELD_OUT_OF_BOUNDS.WITHOUT_SUGGESTION`, 2 `INVALID_EXTRACT_FIELD`. Of the queries that
did not raise, Varka fused 41 of 42 alone and 49 of 49 behind the false AND, but 0 of 49 in the
CASE without ELSE and 0 of 21 in `coalesce(1, e)`, which the optimizer folds to the literal. So the
ANSI form must use shapes that reach Varka: a CASE with an ELSE, and a COALESCE whose first
argument is a column. An error where both raise is Spark's row path raising, since a kernel only
declines a batch that would raise (`docs/sql-varka.md`, "Null semantics and predication"); the
form's value is the other two outcomes, Varka raising where Spark does not and Varka not raising
where Spark does.

## 3. The design

### 3.1 Form 1: a null lane in every row's masked body, by construction

The per-row test of `VarkaCoverageCompositionFuzzSuite` keeps its four drawn batches and adds one
fixed batch per row and width: 257 rows, every input null on every third row and poisoned there,
so both bodies (the masked one by the nulls, the dense one by `otherBody`) meet a poisoned lane at
the loop and at the epilogue. 257 is a loop of whole lane groups at every width plus an epilogue.

### 3.2 Form 2: every compared batch at both fills

`VarkaKernelChecks.runOne` takes the fill as a parameter. Every caller that compares rows -
`runAndCompare`, the long-lane form, `runEachBodyStatus` - runs each batch at `0xFF` and at `0x00`,
and both runs must agree with the reference. The kernel runs twice per batch; the emission, the
class load and the reference evaluation, which cost more than a 1,000-row kernel call, run once.
The sentinel fill of the output values stays: it is the data side of the same question.

### 3.3 Form 3: the partition oracle in `VarkaCoverageDifferentialSuite`

The fixtures gain a row id column, `rid`, an int the predicates do not name. For each predicate
row, fixture and optimizer setting (default, and the three rules excluded through
`spark.sql.optimizer.excludedRules` on both sessions):

- `T` = `WHERE p` and `F` = `WHERE NOT (p)` on Varka, each with a Varka filter asserted in the
  plan (the probe found one in every case), each compared on rows with Spark's;
- `T` and `F` share no `rid`, and `U`, the ids in neither, equals Spark's `WHERE (p) IS NULL` on
  ids and values.

`U` is the complement and not a query of its own because Varka fuses no `IS NULL` over a
comparison (2); were it fused (9), the same test would query it.

### 3.4 Form 3: the ANSI form in `VarkaCoverageDifferentialSuite`

A fourth fixture, `varka_coverage_extremes`, holds the extremes row of 2 beside an ordinary one,
over all of the table's columns. For each projection row, four queries:

1. the row alone;
2. `CASE WHEN rid < 0 THEN e ELSE <a column of e's type> END`, an untaken arm with an ELSE;
3. `coalesce(<a column of e's type>, e)` over a column with no nulls in this fixture, a later
   argument;
4. `WHERE rid < 0 AND e IS NOT NULL`, the right side of a false AND.

and for each predicate row, `WHERE rid < 0 AND p` and `WHERE rid >= 0 OR p`. Each outcome is
compared: rows as rows; an error by its condition, SQLSTATE and message parameters, the three
`checkError` compares. A shape that raises on one engine only fails. Positions 2 to 4 assert a
Varka node in the plan, so the check cannot pass by never reaching Varka; a row whose shape Varka
declines in that position is listed in the test with its reason, as the coverage suite lists its
declines.

### 3.5 Deliberately unchanged

- **The fixtures' existing columns and rows**, which every coverage row's answer depends on; `rid`
  and the extremes fixture are added beside them.
- **`IS NULL` over a predicate stays unfused.** Fusing it is a compiler change with its own bytes
  oracle entries, a row of its own (9).
- **The fuzzers' null-pattern draws.** 3.1 adds a fixed batch; it does not change what is drawn,
  so seeds replay as before.

### 3.6 Registered op counts

None: no emitted code changes.

## 4. Files

| file | what |
| --- | --- |
| `VarkaKernelChecks.java` | the fill as a parameter, every compared batch at both fills |
| `VarkaCoverageCompositionFuzzSuite.scala` | the fixed poisoned batch per row and width |
| `VarkaCoverageDifferentialSuite.scala` | `rid`, the extremes fixture, the partition and ANSI tests |
| `docs/sql-varka.md` | "Testing and benchmarks": the three forms |

## 5. Tests, and what each is for

1. **A kernel that never sets a validity bit fails.** A planted fault: a test emits a coverage
   row's kernel with its validity store removed (through the emitter's existing test hooks, or a
   class-file rewrite of the store, whichever the code admits) and shows the `0x00` run fails it
   and the `0xFF` run does not. Without it, a zero fill that never fails could mean the fill never
   reaches the kernel.
2. **A kernel that never clears a bit still fails** under `0xFF`, the existing check, kept.
3. **Every row's masked body meets a poisoned lane**: the per-row test counts the fixed batches and
   asserts one per row and width.
4. **The partitions** (3.3) over all predicate rows, both optimizer settings.
5. **The ANSI form** (3.4) over all rows, every position.

## 6. The measurement

The per-row test's time and `VarkaCoverageDifferentialSuite`'s time, before and after, at the
defaults and in the matrix's `cse=false` configuration.

### 6.1 Predictions, registered before the run

1. No kernel fails at the zero fill, as the admission check found; test 1's planted fault does.
2. The per-row test takes at most 25% longer: the kernel runs twice a batch, and one batch of five
   is added, while emission and the reference dominate.
3. `VarkaCoverageDifferentialSuite` takes at most twice as long: it gains two queries per predicate
   row per fixture and setting, and four to six per row on the extremes fixture.
4. Positions 2 and 3 of the ANSI form reach Varka for at least 50 of the 65 projection rows, now
   that the CASE has an ELSE and the COALESCE starts with a column.
5. The partition oracle and the ANSI form find no disagreement, as the probes found none; a
   disagreement is fixed or becomes a row.

## 7. Risks

1. **The zero fill may expose a real bug** in a kernel shape the coverage rows do not reach; the
   fuzzers run through the same `runOne`, so it would show there first, as it did not in the
   probe's 31 tests.
2. **The ANSI shapes can change what fuses.** A CASE with an ELSE, or a COALESCE over a column, is
   a different expression from the row; where Varka declines it, the declined list says so, and a
   row with nothing fused in a position is not silently counted as checked.
3. **`rid` changes the fixtures' column set.** Rows name columns, so `rid` changes no row; it does
   widen `SELECT *`, which only the partition test uses.

## 8. Sequencing

Two pull requests.

1. **138a, the kernel side:** 3.1 and 3.2, tests 1 to 3, predictions 1 and 2.
2. **138b, the SQL side:** 3.3 and 3.4, tests 4 and 5, predictions 3 to 5. Then section 9.

## 9. Candidate rows found

- **Varka fuses `IS NULL` and `IS NOT NULL` over a comparison.** As written, `(p) IS NULL` is fused
  for none of the 27 predicate rows (2); the unknown truth value is exactly the validity a fused
  comparison already computes, so the node is one mask. Spark's `NullDownPropagation` hides the gap
  only where it can push the test onto columns.
- **Varka fuses a CASE without an ELSE.** `CASE WHEN c THEN e END` was fused for none of 49 rows
  (2), while the same CASE with an ELSE is the coverage table's own shape; the missing ELSE is a
  null literal of `e`'s type.

## 10. Outcome
