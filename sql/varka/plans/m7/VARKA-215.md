# VARKA-215: Port VarkaConditionCompiler to Java

## 1. Where this came from

Row 215 of `m7/PLAN.md`, from `m8/SCOPE.md` item 81: the second of the three family ports after
VARKA-214 (`m7/VARKA-214.md`), which followed the recipe VARKA-175 set for the interval family
(`m6/VARKA-175.md` 2.3, 2.4 and 9). The owner's rule since 4 October 2026 (`sql/varka/AGENTS.md`,
"Java code") applies to every Scala-to-Java port.

`VarkaConditionCompiler.scala` is 443 lines: the conditional arms (`IF`, `CASE WHEN`,
`coalesce`), the three-valued condition compiler (`compileCond`: comparisons, `IN` over dates,
the connectives, the validity predicates), the balanced `andFold` and `orFold` the facade folds a
filter's conjuncts and a split predicate's pieces with, `foldPick` for `greatest` and `least`, and
`rangeSet`, which recognises a disjunction of ranges over one column.

## 2. The admission check, done

A port has one admission check, that it changes nothing, and the proofs that decide it are those
of 214, fixed before the code:

* **The emitted bytes**: `VarkaEmittedBytesSuite` against the committed `emitted_bytes.json`, not
  regenerated.
* **The coverage table**: `VarkaCoverageSuite` and `coverage.json` byte-identical. The suite scans
  each compiler file's patterns for the Catalyst classes it admits; the class names this file
  matches, `If`, `CaseWhen`, `Coalesce`, the five comparisons, `In`, `InSet`, `And`, `Or`, `Not`,
  `IsNull`, `IsNotNull`, `RuntimeReplaceable`, must all still be found in the Java form.
* **The chain**: `VarkaFamilyChainSuite`, no node claimed by two families.
* **The compiler's own suites**: `VarkaExpressionCompilerSuite`, `VarkaRangeSetCompilerSuite` (the
  one test that calls `rangeSet` directly) and the fuzzers that compose conditions.
* **The decline texts**: every string constant of the Scala file found in the Java file, by a
  comparison of the two.
* **Emission times**: unlike 214's, this benchmark does reach the family. `VarkaEmissionBenchmark`
  emits `a predicate: d < d2 AND d IS NOT NULL`, and every other shape passes through the
  chain's `"condition"` entry, so the pair measures the family's matching and not only the chain.
  Two runs of master and two of the port, the same morning on an idle machine, both widths.

Baseline, taken on `origin/master` at `fe857dc6d4a` on 8 October 2026 before the port, on the idle
laptop with `dev/varka_bench_regen.sh catalyst VarkaEmissionBenchmark` (both widths, twice, the
results kept outside the repository and the committed files restored). The second run is 8
minutes after the first, and the two differ by up to 5% on a row: at 256 bits one row reads
4% slower the second time; at 128 bits nine rows read 3 to 5% *faster*, mostly the kernel-wide
rungs, as if the first run were still warming the machine. So the noise to clear is 5% on a
row, and the direction of a difference matters less than whether both port runs show it against
both master runs.

One caveat on the baseline. The Java file was written into the worktree while the baselines were
queued, so the second baseline and the first one's 128-bit half compiled it beside the Scala
object. Nothing referenced it (the facade still called the Scala object), so what ran was
master's code; the extra class is not a code path.

## 3. The design

### 3.1 The seam

214's, unchanged: one package-private `final class VarkaConditionCompiler` in the facade's
package; `static VarkaFamilyArm arm(...)`, a `switch` over the node that returns the lowering as
a deferred call or `null`; the facade's `familyChain` entry becomes
`javaFamily(VarkaConditionCompiler.arm(_, inputs, literals, sink))`; `scala.Option` and
`LinkedHashMap<?, ?>` stay at the boundary; the Scala file is deleted.

The class has more entry points than 214's, because the facade and a test call it directly:

| entry | caller | Java form |
|---|---|---|
| `compileCond` | the facade's predicate pass | `Option<Cond> compileCond(Expression, LinkedHashMap<?, ?>, LinkedHashMap<?, ?>, DeclineSink)` |
| `andFold`, `orFold` | the facade | take a `scala.collection.immutable.Seq<Cond>`, which is what its call sites pass |
| `foldPick` | `greatest` and `least` in the facade | takes the `scala.Function2` the facade passes |
| `rangeSet` | the arm, and one test | a Java record result: see 3.2 |

### 3.2 What is deliberately unchanged, and what moves

* **Evaluation order and short-circuiting.** `IF` compiles its condition, then both branches,
  and only then asks whether the lanes agree. `CASE WHEN` compiles *every* branch and the ELSE
  even after one declines, in query order, and only then checks the lanes, in the order
  `cond0, value0, cond1, value1, ..., else`, which fixes which two lanes a mismatch note names.
  `compare` compiles the left operand and stops if it declines. Each of these fills the input and
  literal tables or notes a decline, so each is written as early returns in source order, with
  no argument computed ahead of its turn.
* **The decline texts**, constant for constant, and the fall-through to "unsupported predicate".
* **The sort and the cap of an `IN` list**: distinct, sorted, at most `MaxInLiterals` (16),
  folded balanced, so the literal slots and the shape hash keep their order.
* **`rangeSet`'s result.** It is called by one Scala test as `rangeSet(e).map(_._2)`, a Scala
  tuple. In Java it returns an `Optional<RangeSet>`, a record of the column and the bounds, and
  the test's one helper reads the record. Its internals become small records (`Range`, `Side`)
  in place of tuples of options; the arithmetic, the strict-bound step, the sort and the merge
  are the same.
* **`sameLane`** was used only inside this file and becomes private.

The pattern matching over Catalyst's case classes is `instanceof` and type patterns, the stated
exception to the sealed-switch rule of `sql/varka/AGENTS.md`, as in 214.

### 3.3 Registered op counts

None move. The proof is `emitted_bytes.json` unchanged.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/src/main/java/.../codegen/VarkaConditionCompiler.java` | the port |
| `sql/catalyst/src/main/scala/.../codegen/VarkaConditionCompiler.scala` | deleted |
| `sql/catalyst/src/main/scala/.../codegen/VarkaExpressionCompiler.scala` | the `"condition"` chain entry |
| `sql/catalyst/src/test/scala/.../codegen/VarkaRangeSetCompilerSuite.scala` | reads `rangeSet`'s record |
| `sql/catalyst/src/test/scala/.../varka/VarkaCoverageSuite.scala` | the file moves to the Java list |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-215.md` | the row and this plan |

## 5. Tests, and what each is for

No test is added; the oracles of 214 are the proof, plus `VarkaRangeSetCompilerSuite`, which is
the only one that exercises `rangeSet`'s arithmetic directly (bounds, strict steps, merging,
clamping at the int extremes) and so the only one that would catch a slip in its port.

## 6. The measurement

`VarkaEmissionBenchmark`, regenerated with `dev/varka_bench_regen.sh catalyst
VarkaEmissionBenchmark` on an idle machine: two runs of master, then two of the port, both widths,
compared with `dev/varka_bench_diff.py`. The files are not committed: the committed file is
master's from 29 September and reads about a third slower than master does today, so regenerating
it would move every row of a benchmark this task did not change (214 9).

### 6.1 Predictions, registered before the run

1. **The bytes do not move.** The rule that decides the task.
2. **No row of the emission benchmark is reproducibly slower than master**, at either width: a
   difference beyond the noise has to appear in both port runs against both master runs. The
   noise is the larger of 214's two readings, 6% on a row.
3. **The Java file is 480 to 560 lines against the Scala 443.** 214's came out 668 against 409
   (1.6 times), 175's 409 against 250; this file has more of its length in comments and in the
   `rangeSet` arithmetic, which become records but not more lines.

## 7. Risks

1. **Evaluation order.** A condition compiled early changes the input slot order or a decline
   text. The emitted bytes show the first and the decline comparison the second.
2. **`CASE WHEN` stops at the first decline** in a careless port, where the Scala compiles every
   branch. Covered by a decline text that depends on which branch noted first; the compiler suite
   has cases with a declining branch in each position.
3. **A class the coverage scan no longer finds**, dropping a Catalyst class from the published
   coverage without a failing test. The byte-identity of `coverage.json` catches it.
4. **`rangeSet`'s merge**, the one place with real arithmetic: sorting by lower bound must stay
   stable, and a strict bound must step before the clamp. `VarkaRangeSetCompilerSuite`.

## 8. Sequencing

1. This plan, and the row marked Planned.
2. The port with the call sites and the two test edits, in one commit.
3. The measurement and section 9.

## 9. Outcome

Filled in when the measurement lands.
