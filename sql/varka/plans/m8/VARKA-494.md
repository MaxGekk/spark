# VARKA-494: One registry of the types Varka reads

## 0. What exists now (admission check)

From the code at `608686315e7` (25 October 2026; `C` is
`sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen/`, `T` is
`sql/catalyst/src/test/`). The design is `VARKA-475.md` 3.4, widened by its 9.2.1; this is that
design checked against the code and made into steps.

1. **Which lane a Spark type takes is decided by seven functions in five files.**
   `VarkaNodeCompiler.laneOf` (`C:675`) is the source; `onLongLane` (`:694`), `isNarrow`
   (`:638`), `VarkaConditionCompiler.onNarrowLane` (`:756`) and
   `VarkaExpressionCompiler.readsColumnOf` (`:239`, the test harness's question) are filters
   over it; `VarkaTimeCompiler.isTimestamp` (`:225`) is the one decision kept beside it, the two
   timestamps declined by name; and `VarkaLaneArrow.of` (`sql/core/.../VarkaLaneArrow.java:85`)
   repeats the type-to-lane map as eight predicates on the Arrow side. Nothing checks that
   `laneOf` and `VarkaLaneArrow` agree except a column reaching a kernel.
2. **A type with no arm declines as something else.** A column `laneOf` does not know reaches
   `VarkaNodeCompiler.fallback` (`:655`) and declines as "non-date column of type double" - true
   of a `DOUBLE`, but it says neither that the type is planned nor which row builds it. Only the
   two timestamps are named, by `isTimestamp`'s "outside milestone 5", a reason that is out of
   date for `TIMESTAMP_NTZ` and does not say why the zoned type is out. `ADDING_A_TYPE.md`
   ("What the guards do not catch") names this as the hour lost per missing arm.
3. **The type facts the tools need are kept in six more lists, by hand** (`VARKA-475.md` 9.1,
   finding 3, rechecked here):
   - `dev/varka_census.py:65-70` - `EXISTING`, `NEW`, `ORDER`, and `CONFORMANCE_SUITE` (`:925`);
   - `dev/varka_new_lane.py:70` - `CENSUS_NAME`, Spark class to census name;
   - `VarkaCoverageSuite.censusType` (`T`, `.../varka/VarkaCoverageSuite.scala:1432`);
   - `VarkaTypeConformance.CAST_TARGETS` (`T`, `.../varka/VarkaTypeConformance.java:596`), each
     census name as SQL;
   - `VarkaCastMatrixSuite.TYPES` (`T`, `.../varka/VarkaCastMatrixSuite.java:77`), each census
     name's forms (field ranges, precisions);
   - `VarkaSparkOracle` (`T`, `.../varka/VarkaSparkOracle.scala:57-61`), a stored value to a Spark
     value by type.
   `VarkaKernelChecks` derives a test column's storage from its lane (`:871`, `:895`) and
   `VarkaWarmupReach` the species a type reaches (`widenedSpecies`, `:158`); both are per-lane
   code where the fact is per type. Rows 484 and 486 found these lists disagreeing with each other and with the
   compiler while every test passed (`VARKA-475.md` 9.1, finding 1).
4. **Most type tests are not admission and stay.** The compilers test Spark types in 48 places
   (`VarkaNodeCompiler` 21, `VarkaIntervalCompiler` 11, `VarkaTimeCompiler` 9, `VarkaChronoCompiler`
   5, `VarkaConditionCompiler` 2): "is this operand exactly an `INT`", "is this a day-time
   interval ending in `SECOND`". Those are an arm's semantics, not which lane a type takes, and
   the registry does not replace them (`VARKA-475.md` 3.4: "only what has a reader today").
5. **The method is row 488's** (`VARKA-475.md` 9.1, finding 5): build the new code beside the
   old, prove the two equal, move the consumers one at a time with the oracles after each, and
   delete the old code and the equality test last.

## 1. Goal and non-goal

Every fact about a type that more than one place needs is written once, in a registry, and read
from there by the compiler, the Arrow table, the test harness and the Python tools. A type that
is not built yet declines with its name and the row that builds it.

Not this row: ordering and equality semantics (the floats bring them, `VARKA-475.md` 9.2.2, with
row 341), value shapes for decimal (3.5), node properties (row 489), generating the census from
the compiler (row 487), generating the warm-up (475's open question 3).

## 2. Design

### 2.1 The id

`VarkaTypeId`, a closed enum in `varka/` beside `Lane` (475's open question 1: there, so the
evaluator in `sql/core` and the tests read it without depending on the compiler): `BOOLEAN`,
`BYTE`, `SHORT`, `INT`, `LONG`, `FLOAT`, `DOUBLE`, `DECIMAL`, `DATE`, `TIME`, `TIMESTAMP_NTZ`,
`YM_INTERVAL`, `DT_INTERVAL`, and `TIMESTAMP`, the zoned type, so that its decision is an entry
and not a special case in one compiler. `VarkaTypes.of(DataType)` is the one function from a
Spark type to an id, empty for every other type.

### 2.2 The entry

`VarkaTypes.info(id)` returns a record; each field has a reader today, listed:

| field | what | read by |
|---|---|---|
| `census` | the census name (`int`, `ntz`, `dec`, ...) | census and new-lane tools, coverage suite |
| `sql` | the SQL spelling of a cast target | conformance cast check |
| `admission` | what a kernel does with a column (2.3) | the compilers, `readsColumnOf` |
| `storage` | bytes a row, or bit-packed | `VarkaLaneArrow`, `VarkaKernelChecks`, oracle |
| `forms` | the Spark types of the id (precisions, field ranges) | cast matrix suite |
| `species` | the vector lanes a kernel of the type reaches | warm-up reach model |
| `conformance` | its conformance suite's name, or the row that owes it | census tool |

The record is data, built once in a static table; nothing on the batch path reads it.

### 2.3 The admission answer

A sealed interface with four cases, so a switch over it is exhaustive:

- `OnLane(LaneType)` - read as a lane (`INT`, `LONG`, `BYTE`, `SHORT`);
- `AsCondition` - read as two bitmaps, conditions only (`BOOLEAN`, row 331; its values row 27);
- `Owed(String row)` - planned, not built: `FLOAT` and `DOUBLE` (341), `DECIMAL` (347),
  `TIMESTAMP_NTZ` (338);
- `Declined(String decision)` - out by decision: `TIMESTAMP`, "its arithmetic is in the session
  zone".

`laneOf(t)` becomes `of(t)` then `OnLane`'s lane. A column whose answer is `Owed` declines as "a
double column: the DOUBLE lane is row 341's"; `Declined` with its decision. A type with no id
keeps the generic decline.

### 2.4 The committed file

A test writes the registry to `sql/varka/types.tsv` (one line per id: the fields of 2.2 that a
tool reads), failing when the file differs, as `VarkaCastMatrixSuite` writes
`m8/casts_fused.tsv` (row 486). `dev/varka_census.py` and `dev/varka_new_lane.py` read it in
place of `EXISTING`, `NEW`, `ORDER`, `CONFORMANCE_SUITE` and `CENSUS_NAME`. The census keeps its
`GROUPS`: they are its own judgement, not type facts.

### 2.5 What changes for a new type

Adding `TIMESTAMP_NTZ` (row 338) becomes: its entry's admission from `Owed("338")` to
`OnLane(LONG)`, its storage and Arrow vector, and the regenerated `types.tsv`. The compiler, the
census, the coverage suite, the cast suite and the conformance contract see it from there.

## 3. Steps

1. **Beside the old code.** `VarkaTypeId`, `VarkaTypes`, the entries for all fourteen ids, the
   `types.tsv` writer, and an equality test: over every form of `VarkaCastMatrixSuite.TYPES`
   plus the Spark types no id holds, the registry's lane equals `laneOf`, its storage equals
   `VarkaLaneArrow`'s, and its census name equals each of the six lists of 0.3. No consumer
   changes; this step finds where the lists already disagree.
2. **The production consumers**, one commit each, the byte oracles run after each: `laneOf` and
   its four filters read the registry; `isTimestamp`'s decline becomes the `Declined` entry;
   `VarkaLaneArrow`'s predicates match by id; `fallback` names an `Owed` type and its row.
3. **The test harness**: `censusType`, `CAST_TARGETS`, `TYPES`, the oracle's arms (by storage),
   `VarkaKernelChecks`' layout (by storage and the kernel plan's lane, never its own) and the
   reach model's species read the registry.
4. **The tools** read `types.tsv`; the pre-commit hook and the docs job run the check that the
   file is current.
5. **The old code goes**: `laneOf`'s body, the filters, the six lists, and step 1's equality test
   last.
6. `ADDING_A_TYPE.md` steps 1 to 4 and "What the guards do not catch" say the registry is the
   one registration point; `VARKA-475.md` 3.4 and 9.3 point here.

## 4. Tests and oracles

1. Step 1's equality test, while old and new coexist.
2. The three byte oracles - `emitted_bytes.json`, `coverage.json`, `width_audit.json` - unchanged
   after every commit of step 2; `casts_fused.tsv` unchanged.
3. The decline texts: a test per admission case - an `Owed` column names its type and row, the
   zoned timestamp its decision, a `MAP` column the generic decline. The two tests that pin
   "a timestamp column is outside milestone 5" (`VarkaExpressionCompilerSuite:2569`,
   `VarkaLongLaneSuite:250`) take the new words.
4. `types.tsv`'s check fails on a stale file and on an id with no conformance suite and no owing
   row, as `dev/varka_census.py --check` does today.
5. The gate, and the matrix over the changed suites.

## 5. Predictions and costs

1. No emitted byte moves, no fused form moves: the registry answers what `laneOf` answered.
2. Step 1's equality test finds at least one list that disagrees; each is fixed in the list's
   favour or the registry's, and said in section 10.
3. The decline text changes for every column of an owed or declined type: EXPLAIN and the
   fallback metrics' reasons read differently, and nothing else does.

Cost: three to four days - 475's two to three, plus 9.2.1's harness and tools.

## 6. Files

| file | what |
|---|---|
| `C/varka/VarkaTypeId.java` | new: the id |
| `C/varka/VarkaTypes.java` | new: the entries and the admission answer |
| `C/VarkaNodeCompiler.java` | `laneOf` and its filters; `fallback` |
| `C/VarkaConditionCompiler.java` | `onNarrowLane` |
| `C/VarkaExpressionCompiler.java`, `C/VarkaTimeCompiler.java` | `readsColumnOf`; `isTimestamp` |
| `sql/core/.../VarkaLaneArrow.java` | predicates by id |
| tests | the equality test, the decline tests, the six lists of 0.3, the layout and reach model |
| `dev/varka_census.py`, `dev/varka_new_lane.py` | read `types.tsv` |
| `sql/varka/types.tsv` | new, generated |
| docs | `ADDING_A_TYPE.md`; `VARKA-475.md` 3.4 and 9.3; `m8/PLAN.md` rows 475, 494 and 338 |

## 7. Risks

1. **An admission test the grep missed**: a Spark-type test in a compiler that is in fact
   "which lane", not an arm's semantics. Step 1's equality test cannot see it; step 2's review
   reads each of the 48 sites of 0.4 once and lists any it moves.
2. **A god object** (`VARKA-475.md` 7). The record keeps only fields with a reader; a field
   whose reader goes is removed with it.
3. **The Python tools depend on a file a test writes.** The check fails a stale file, as
   `casts_fused.tsv`'s does, so a commit that changes the registry without the file fails the
   hook. *Corrected 26 October 2026 (section 10):* it fails `VarkaTypesSuite` in the catalyst
   job, not the hook, which cannot run the suite.

## 8. Questions for the owner

None. 475's open question 1, where the registry lives, is answered in 2.1 by its own default.

## 9. Done when

`VARKA-475.md` 3.4's done-when and 9.2.1's: `laneOf` and its siblings are one function; the
census's type names and the reach model's species come from the registry; a decline for a type
the compiler does not support names that type, and the row for an owed one; `CAST_TARGETS`, the
cast suite's `TYPES`, the census's type lists and the oracle's per-type arms read the registry,
and the harness takes layouts and lanes from the registry and the kernel plan; and
`emitted_bytes.json`, `coverage.json`, `width_audit.json` and `casts_fused.tsv` are unchanged.

## 10. Outcome

Built on 26 October 2026, in the steps of section 3, each a commit with the three byte oracles,
`casts_fused.tsv` and `types.tsv` checked after it; none moved at any step.

1. **The registry.** `VarkaTypeId` (fourteen ids, in the census's order) and `VarkaTypes`, an
   entry per id with seven fields: census name, SQL spelling, admission, storage bits, forms,
   the lanes its kernels reach, and conformance. Admission and conformance are sealed sets
   (`OnLane`, `AsCondition`, `Owed`, `Declined`; `Suite`, `Owed`, `NotLanded`). `VarkaTypes.of`
   is the one map from a Spark type to an id; `idOf` is its allocation-free form, for the
   evaluator, which asks once per output vector of a batch. `VarkaTypesSuite` writes
   `sql/varka/types.tsv`, maps every form back to its own id, and checks each entry's storage
   against the Arrow type Spark itself writes for every form (`ArrowUtils.toArrowType`).
2. **Step 1's equality test found no disagreement**, against prediction 5.2: the registry's lane
   equalled `laneOf`'s for every form, and a one-off comparison of `types.tsv` with the Python
   tools' lists (order, conformance, owed rows, census names, the types on each lane) and a read
   of the three Java and Scala lists found them all equal. Rows 484 and 486 had reconciled them a
   day earlier. The review found an eighth hand-kept list the admission check missed:
   `VarkaCoverageDifferentialSuite.onLongLane`, the long lane's types again; it reads the
   registry too.
3. **The production readers.** `laneOf`, `onLongLane`, `onNarrowLane` and `readsColumnOf` are
   gone; the compilers and the sql suites' Arrow source ask `VarkaTypes.lane`. `isNarrow`
   (`VarkaNodeCompiler`) stays as a one-line predicate over an expression that reads it.
   `VarkaLaneArrow` is one switch over the ids, and `VarkaLaneArrowSuite` (new, sql/core) fails
   when the table and the registry disagree - a type on a lane with no vector, or a vector at
   another width - the gap the review found the plan's step 1 test could not see.
4. **The declines (prediction 5.3, held).** A column of a type the registry holds and no lane
   reads declines in `VarkaNodeCompiler.fallback` with the registry's reason: "a double column:
   the DOUBLE lane is row 341's", "a timestamp_ntz column: the TIMESTAMP_NTZ lane is row 338's",
   the zoned timestamp's decision, and "a boolean column read as a value, which row 27 builds".
   The time compiler's own timestamp arm and its "outside milestone 5" went. Two tests took the
   new words (`VarkaExpressionCompilerSuite`, `VarkaFilterExecSuite`'s EXPLAIN lines); the
   review checked that no family between the old arm and the fallback claims a column, so
   nothing else moved.
5. **The harness and the tools.** The coverage suite's census names, the conformance contract's
   cast targets, the cast matrix's types and forms, the oracle's conversion (by storage width),
   and the warm-up reach model's lanes come from the registry; the kernel checks take a lane's
   byte width from production's `Lane`. `dev/varka_census.py` and `dev/varka_new_lane.py` read
   `types.tsv`; the new-lane walk no longer parses `laneOf`'s source.

**What differs from the plan.**

- *`EXISTING` and `NEW` stay in `dev/varka_census.py`.* They are the milestone's split of the
  types (the owner's decision of 14 October), not a fact of a type; the census checks that they
  cover exactly the registry's census types. `ORDER` and the conformance tables are read.
- *No pre-commit check that `types.tsv` is current* (7.3 said the hook would fail it). The hook
  cannot run the JVM suite that writes it, and a check keyed on `VarkaTypes.java` changing would
  fail a comment-only edit with no way to pass; `VarkaTypesSuite` fails a stale file in the
  catalyst job, as `VarkaCastMatrixSuite` does for `casts_fused.tsv`. The hook re-runs the census
  and the lane walk when the registry or its table changes.
- *The kernel checks are per lane, not per type*: they run IR with no Spark types, so their
  widths come from `Lane`, the emitter's own figure, rather than from the registry's storage.

**Verification.** The full laptop gate passes (wide 1,465 s, narrow 1,469 s, the
sweeps, docs, benchmarks, quote check), and the option matrix runs the changed catalyst suites
(`VarkaTypesSuite`, `VarkaCastMatrixSuite`, `VarkaWarmupReachSuite`, the three conformance suites,
`VarkaExpressionCompilerSuite`) and sql suites (`VarkaFilterExecSuite`, `VarkaLongLaneSuite`)
under all 51 configurations and the defaults: every row exit 0, `fail 0`, `unfused 0`. Error
Prone, `dev/lint-java` and `dev/lint-scala` pass. An independent review compared every replaced
list with master and ran both versions of the two Python tools, whose output is byte-identical;
its findings (an unused import, the missing Arrow-table check, stale comments, the mixed-lane
strides) are fixed.
