# VARKA-484: a conformance contract per type

## 0. Why this row exists

`VARKA-475.md` 3.1, the first of the type architecture's five pieces, as a row of its own. The
reviews of the `TINYINT` and `SMALLINT` lanes (`VARKA-329.md` 9.8) found most defects in the
tests rather than the emitter: no fixture row had a narrow pair null on the left only, the "no
nulls" fixture held narrow nulls, and two equality rows matched no row of any fixture, so a
kernel answering a constant false would have passed them. Each type writes its fixtures by hand,
and nothing says what they must hold. Four types are still ahead (`TIMESTAMP_NTZ`, `DOUBLE`,
`FLOAT`, `DECIMAL`); this row writes the contract they will be held to and proves it on the
three types that have landed since the milestone began: `TINYINT`, `SMALLINT` and `BOOLEAN`.

The `BOOLEAN` checks read a boolean as a value through `CondValue` (row 460, #1005), so this row
is stacked on it.

## 1. Design

**One abstract base, one suite per type.** `VarkaTypeConformance` (Java, catalyst tests) holds the
seven checks as `@Test` methods, each reading the type's descriptor; `VarkaTinyIntConformanceSuite`,
`VarkaSmallIntConformanceSuite` and `VarkaBooleanConformanceSuite` each supply one and carry
`@VarkaSuite` (the suite extension refuses parameterized tests and needs the annotation on the
concrete class; inherited tests run). The narrow two run in their own JVM, as every suite with a
narrow species does. The descriptor holds only what the checks read: the Spark type, its census
name, its lane, its values, a literal, and how a column of it is stored.

**Through the real compiler.** A check builds Catalyst expressions over attributes of the type,
compiles them with `VarkaExpressionCompiler`, emits the class and runs it through
`VarkaKernelChecks.runLoadedNarrow`, which runs both bodies at both output fills and compares every
row with `VarkaReference`; every row is also held to `VarkaSparkOracle`, Spark's own `eval` of the
same expression. That is the "row engine" of `VARKA-475.md` 3.1 in this row: vanilla Spark's
answer on the same rows, without the planner. The coverage differential suite already runs SQL
end to end over every coverage row; a planner-level pass of the contract is a later step.

**Generated fixtures.** From the type's values - minimum, maximum, 0, 1, -1 and the neighbours of
the extremes for the integral types; true and false for `BOOLEAN` - a check draws every pair of
values under each null pattern of a pair (left only, right only, both, neither), an equal pair
for every value, and a batch with no null for the dense body, at lengths that cover a partial
lane group and the epilogue. A boolean column is stored as Arrow stores it, a bit a row, with the
data bit under a null set on half the null rows (Spark's writer clears only the validity bit).

**The rule a hand-written fixture broke.** Every check asserts, for every root, that its fixture
can tell a right kernel from a wrong one: a condition root is true on some row and false on
another; a value root has a valid row, and a null row where a null pattern exists. A fixture that
fails it fails the check, which is `VARKA-475.md` 3.1's "a fixture that cannot make a row of the
type's coverage true is a finding of the contract, not of a reviewer".

**Required or owed, from the registers.** Each check names the census operators its forms use,
and the cast check reads the type's rows of `casts.tsv`. Per form: where the register marks the
type fused today, the kernel must answer and agree with Spark and the reference; where it does
not, the compiler must decline, and the check is cancelled naming the rows that own the gap (the
register's `rows` column), which the option matrix counts in its cancelled column. A form that
fuses where the register says it does not, or declines where it says it does, fails: that is
register drift, found here before 3.2 checks the registers on their own.

**Decided in the plan, object if wrong:**

1. *Pass is zero failures:* every check holds, or is cancelled naming the row that owes it. For
   `BOOLEAN` today that means: load, null handling, ordering and choice run (row 331's conditions
   and `CondValue`); the round trip is owed to row 27 (a boolean output) and every cast row to row
   332; overflow does not apply, and its check asserts the census lists no arithmetic for `bool`,
   so it can fail rather than pass by being empty.
2. *The census's `fused_today` means "fuses in a kernel"*, as its `=` row already reads for `int`
   (a comparison's result is a condition, not an output). Row 331 left its boolean claims out:
   `=`, `<`, `<=`, `<=>`, `and`, `or`, `not`, `isnull`, `isnotnull`, `if`, `coalesce` and
   `case` fuse over `bool` and the census marks none. This row adds the claims the coverage table
   backs, and the contract's drift rule keeps them honest from now on.
3. *Defaults only in the suite.* The widened arm is `narrowLanes=false` in the option matrix, as
   the other representation arms are; the contract does not loop over arms, since the matrix runs
   each suite under all 52 configurations.

## 2. The seven checks

| Check | Forms (integral) | Forms (`BOOLEAN`) |
| :--- | :--- | :--- |
| load | `x < y`, `x = <literal>`, `x IS NOT NULL` | `b`, `NOT b`, `CondValue` of `b` |
| round trip | a value stored at the type: `if(x IS NULL, <lit>, x)`, the pick of `CASE WHEN x > y` | owed to row 27 |
| null handling | `x + y` (LEGACY), `x = y`, `x <=> y`, `x IS NULL`, under every null pattern | `b AND b2`, `b OR b2`, `b <=> b2`, `b IS NULL` |
| ordering | `<`, `<=`, `>`, `>=`, `=`, `<=>`, `greatest`, `least` over every pair | the five comparisons (false < true) |
| choice | `CASE WHEN`, `if`, `coalesce` with the type's values | `IF(b, ...)`, `CASE WHEN b`, `coalesce(b, b2)` |
| cast row | each `casts.tsv` row from the type, in the three modes | each row from `bool` |
| overflow | `+`, `-`, `*`, negation, `abs` at the extremes: LEGACY wraps, ANSI declines exactly the batches Spark raises on, TRY nulls exactly those rows | not applicable, asserted |

## 3. The census check

`dev/varka_census.py --check` fails for a landed type with no conformance suite: each of `bool`,
`byte`, `short` and the six types `laneOf` admits (`int`, `bigint`, `date`, `time`, `ym`, `dt`)
must have `Varka<Name>ConformanceSuite.java` under the catalyst tests, or an entry in an `OWED`
map naming the row that will write it. The six existing types are owed to row 485, added with
this row: their suites, once the contract has held on three types. A `--self-test` proves that
removing a suite fails the check.

## 4. Predictions and costs

At the calibration table's ratios for short emitter tests (VARKA-281, VARKA-317, VARKA-460:
0.03 to 0.08 of estimates that priced a compile per batch), and VARKA-280's 7 ms an emission with
the emitter cold: about 60 emitted classes a suite, each run over a few hundred short batches, so
each suite under 30 s alone, and the three together under 90 s in the defaults gate.

No emitted byte moves: the contract adds tests, a test-side storage layout and an oracle arm.
`emitted_bytes.json`, `coverage.json` and `width_audit.json` stay byte-identical; `census.tsv`
moves by the boolean claims of decision 2.

## 5. Done when

`VARKA-475.md` 3.1's done-when: the base exists with the seven checks and the generated fixtures;
`TINYINT`, `SMALLINT` and `BOOLEAN` pass it (zero failures, the owed forms cancelled naming their
rows); the census check fails for a type with no conformance suite. Plus the house's: the option
matrix over the three suites under every configuration, with the skip lines it needs; the
catalyst defaults gate; the linters.

## 6. Outcome

To be written as the work lands.
