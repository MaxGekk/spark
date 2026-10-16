# VARKA-334: `IS NULL` and `IS NOT NULL` over a predicate

*Row 334 of `m8/PLAN.md`, wave 0. Planned on 16 October 2026 at `8a50f61a368`, from `m9/SCOPE.md`
item 93 and the parked branch it names. The design is the branch's; the row's work is to
understand the branch's seven test failures, which section 2 sizes and leaves as the first step.*

## 1. Where this came from

VARKA-138's admission check (`m7/VARKA-138.md` 2 and 9) found that `(p) IS NULL` was fused for
none of the coverage table's 27 predicate rows. It reaches Varka only where `NullDownPropagation`
pushes the test onto the columns, 30 of 81 cases; a predicate it cannot push through, or a query
whose optimizer settings keep the form, hands the filter or the CASE back to Spark's row path.
Item 93 (13 October 2026) proposed the design and a local branch built it; its first catalyst run
failed seven tests, undiagnosed, and the work was parked. Row 336 (null-safe equality) is the
second condition over validity words and follows this one.

## 2. The admission check, done

1. **The design.** A condition already carries a known-true and a known-false mask
   (`VarkaVectorIR.Cond`, permitting `Compare`, `And`, `Or`, `Not`, `IsNotNull` and `InRanges`,
   215 to 225). `IsNotNull` takes a condition child beside a column: `kT = kT(c) | kF(c)` and
   `kF = ~kT` in a masked body, constant true in a dense one, which has no unknown lane; `IS NULL`
   stays the `Not` of it, a slot swap (680). Today `IsNotNull` (682 to 695) is restricted to a
   `ColumnRef`, its walk at `VarkaVectorWalk` 977 to 980 reading the column's word.
2. **The branch.** `varka-326`, local to the main checkout and not pushed, one commit
   `8077a4b9ddf` (13 October 2026) on merge base `25a1bae6893`, which master has passed by 124
   commits. Nine files, 97 lines added and 16 removed: `VarkaConditionCompiler.java` (a
   `BooleanType` operand of a validity predicate compiles as a condition), `Analysis.java` (the
   child may be a `Cond`), `Slots.java` (the condition child's edge followed in a masked body only,
   and no word demanded for it), `VarkaVectorIR.java` (the doc and the restriction),
   `VarkaVectorWalk.java` (a masked arm: load `kT` and `kF`, `OR`, store, then `XOR -1`),
   `VarkaReference.java`, `VarkaCoverageSuite.scala`, `coverage.json` (four rows, all fusing) and
   `docs/sql-varka.md`. The branch names the work "VARKA-326" in two comments, and `m7` has a row
   326 of its own; this row renames them VARKA-334.
3. **The seven failures, grouped by suite.** Three are in `VarkaEmittedBytesSuite`: the oracle, its
   many-threads test, and "validityOrFirst moves nothing at the defaults and moves the bytes over
   its subject" (624). Four are in `VarkaCoverageCompositionFuzzSuite`: the per-row kernel check,
   the wide composition test and two shrinker tests. Inferred, not checked: the first three are
   the bytes oracle meeting four new coverage rows with no `emitted_bytes.json` entries, which
   fails the oracle and every test that reads the same fixture, and would be cured by
   regenerating it; the composition four are the real question, because that suite composes
   coverage rows into wider trees, and a condition child under `IsNotNull` is the first node whose
   child's masks must be computed before a parent condition reads them in a masked body - an
   ordering `Slots` now follows by the new edge rule, which a composed tree with the same predicate
   shared by two parents may break.
4. **No null literal is involved**, and the row needs no new IR node: `IsNotNull` widens its child.
   Its validity semantics (`skips = true`, Analysis) are unchanged.
5. **The census does not move.** A predicate is `BOOLEAN`-typed, so `isnull` over one is an
   `isnull` pair of type `bool`, and the census counts a type only where a coverage row reads a
   column of it (2.1); a boolean column is 331's. The row's progress shows in the coverage table
   and item 93's 27 predicate rows, not in the census share.

**What still needs a run, and what admits the row.** The branch replayed on master and the seven
tests run, on cores 3 to 5:

    dev/varka_task_new.sh 334 "IS NULL and IS NOT NULL over a predicate"
    git cherry-pick 8077a4b9ddf          # in the task's worktree
    taskset -c 3-5 build/sbt 'catalyst/testOnly *VarkaEmittedBytesSuite \
      *VarkaCoverageCompositionFuzzSuite'

then, if the three bytes-oracle tests fail as before, the suite again under
`VARKA_BYTES_REGEN=true` (its regeneration switch, `VarkaEmittedBytesSuite` 63) and once more
without it.
**Admitted** as designed if the bytes-oracle tests pass once the fixture holds the new rows and
the composition failures reduce to a named cause with a seed (`VarkaCoverageCompositionFuzzSuite`
prints its seed and iteration). If a composition failure is a wrong answer in the kernel, not in
the harness, the design is rejected as it stands until the cause is fixed; the row records which.

## 3. The design

### 3.1 The branch's design, completed

1. **The compiler**: `compileValidity` compiles a `BooleanType` child as a condition and wraps it
   in `IsNotNull`; the filter and the `CASE` condition paths reach it as they reach a column's.
2. **The analysis and the slots**: as the branch has them, plus whatever section 2's run finds
   the composition needs - most likely that a condition child shared by two parents is lowered
   once per lane group and its masks read by both.
3. **The walk**: the branch's masked arm; the dense arm emits the constant-true mask without
   lowering the child (no unknown lane exists there).
4. **The partition oracle**: VARKA-138's TLP `WHERE` oracle queries `p IS NULL` directly instead of
   its composed form, item 93's last clause.
5. **The cost table**: an `IsNotNull` over a `Cond` costs the child's lowering plus one `OR` and one
   `XOR` in a masked body and nothing in a dense one, entered in `VarkaEmitCostTable` so the
   budget prices it.

### 3.2 What is deliberately unchanged

* `IsNotNull` over a column: same bytes (the oracle holds it).
* A value child other than a column: still refused - a computed value's word materializes only
  during its value walk, after condition emission.
* `<=>` and `equal_null` (336), a boolean column as a condition (331), boolean outputs (27).

### 3.3 Registered op counts

`(d < d2) IS NOT NULL` in a masked body: the comparison's own count plus two `VectorMask`
operations (`or`, then `not`); in a dense body: zero beyond a constant mask, the comparison not
lowered. Read with `dev/varka_emit.sh "(d < d2) IS NULL" --nulls 64` once built, asserted by an
op-count test.

## 4. Files

* `.../codegen/VarkaConditionCompiler.java`: the boolean operand of a validity predicate
* `.../codegen/varka/VarkaVectorIR.java`: `IsNotNull`'s child restriction and doc
* `.../codegen/varka/Analysis.java`, `Slots.java`, `VarkaVectorWalk.java`: the condition child's
  analysis, edges and lowering
* `.../codegen/varka/VarkaEmitCostTable.java`: the node's cost over a condition
* test: `VarkaReference.java`, `VarkaIrGrammar.java`: the oracle's arm and the fuzzer's production
* test: `VarkaCoverageSuite.scala`; sql/core `VarkaSparkFuzz.scala` (the TLP oracle): the rows; `p
  IS NULL` queried directly
* `sql/varka/coverage.json`, `docs/sql-varka.md`, `sql/varka/emitted_bytes.json`: regenerated; new
  rows only

## 5. Tests, and what each is for

1. **The coverage rows**: `p IS NULL` and `p IS NOT NULL` as filters and as `CASE` conditions over
   every predicate row of the coverage table (27), with the poisoned null lanes and both validity
   fills; the row engine is the oracle.
2. **The seven tests of section 2** green, with the cause of each named in section 9.
3. **The partition test**: `p`, `NOT p` and `p IS NULL` partition the rows exactly, with the TLP
   oracle querying `p IS NULL` itself.
4. **A shared predicate**: one condition under `IS NULL` and under a `CASE` in the same kernel;
   catches the lowering-order case section 2 infers.
5. **Column children unchanged**: the oracle's existing `IsNotNull` rows do not move.

## 6. The measurement

None of throughput: the shapes fused here went to the row path before, and a kernel is not
measured against itself. The cost table's entry is checked by the emit cost suite.

### 6.1 Predictions, registered before the run

1. **The three bytes-oracle failures are the missing fixture rows** and pass once it is
   regenerated (section 2 item 3's inference).
2. **The four composition failures share one cause**, a condition child lowered in the wrong order
   or twice in a composed tree, not a wrong mask formula.
3. **All 27 predicate rows fuse** under `IS NULL` and `IS NOT NULL`, as filters and as conditions,
   once the composition cause is fixed.
4. **The added test time is under 20 s**: about sixty coverage rows, estimated at 10 s and doubled
   at VARKA-243's ratio, the calibration table's commonest miss.

## 7. Risks

1. **The composition failures are a wrong answer.** Then section 2's admission rejects the design
   as it stands; the fuzzer's seed reproduces it and the row stops until the cause is fixed.
2. **A condition lowered twice.** Correct but slower; the op-count test and the cost table see it.
3. **The branch is 124 commits behind.** The cherry-pick may conflict in `Slots` or the walk, both
   edited since by milestone 7; the conflict is resolved against master's structure, not the
   branch's.

## 8. Sequencing

1. This plan; section 2's replay and the seven tests, their causes recorded here.
2. The branch's change on master with the fixes, the coverage rows and the regenerated fixtures.
3. The partition oracle's direct query, the cost table entry.
4. The predictions scored.

Explicitly out of this row: null-safe equality (336), boolean columns (331), boolean outputs (27),
and a computed value child of `IsNotNull`.

## 9. Outcome

*To be written when the work lands.*
