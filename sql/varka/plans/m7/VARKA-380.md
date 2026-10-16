# VARKA-380: A fused conjunct no longer runs ahead of a residual one that can raise

## 1. Where this came from

Row 380 of `m7/PLAN.md`, from `VARKA-298.md` 9.2: when Varka splits a filter, the conjuncts it
fuses run in the Varka node below and the rest in a row `FilterExec` above, so a row a fused
conjunct rejects never reaches a residual conjunct that Spark evaluates before it. Where that
residual conjunct raises on the row - under ANSI most arithmetic and `make_date`, and year-month
interval arithmetic in every mode - Spark fails the query and Varka returns rows. VARKA-298's
census met it four times in 17,490 nested compositions and kept five shrunk reproducers,
`sql/varka/fuzz/spark/varka-380-<seed>-<iteration>.sql`, as `status: known 380`. It is the mirror
of row 273, which gave the per-row fallback the whole condition in Spark's order and assumed the
raising conjunct was the fused one. The row is in milestone 7 because it is a wrong answer (1.1
point 2); it is done when the five reproducers are regressions that agree and
`-Dvarka.sparkfuzz.nested=false` is gone from the `sparkfuzz` step of `dev/varka_nightly.sh`.

## 2. The admission check, done

The five reproducers, marked `status: regression` on this branch before any code change, fail
`VarkaSparkReproducerSuite` (16 October 2026, at `d0c686d4275`, the head of `varka-298`). The
suite stops at the first file, so a throwaway suite (not committed) replayed all five through the
same `disagreement` and also ran each query on the row engine alone with whole-stage codegen on
(the default) and off:

| reproducer | ANSI | Varka against the row engine | vanilla, codegen | vanilla, interpreted |
|---|---|---|---|---|
| 20261102-4775 | on | only the row engine throws | `INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION` | the same |
| 20261104-2199 | on | only the row engine throws | `DATETIME_FIELD_OUT_OF_BOUNDS.WITHOUT_SUGGESTION` | the same |
| 20261105-291 | on | only the row engine throws | `ARITHMETIC_OVERFLOW` | the same |
| 20261105-3145 | on | only the row engine throws | `ARITHMETIC_OVERFLOW` | the same |
| 20261106-4069 | off | only the row engine throws | `INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION` | the same |

So every one is Varka answering where Spark raises, and Spark raises the same condition in both
of its evaluation orders: on these five the oracle does not depend on which order Spark uses
(3.1). What the check would have rejected: a reproducer on which vanilla Spark answers in one
order and raises in the other would have made "what Spark does" a choice, not a fact, and the fix
a matter of picking an order.

## 3. The design

### 3.1 What Spark does, which is the oracle

Spark documents no order for a filter's conjuncts, and its code has two:

* **Interpreted, and Varka's own fallback**: `Predicate.create` over the `AND` tree, which
  evaluates the conjuncts as written and stops at the first false one.
* **Whole-stage codegen** (`FilterExec.generatePredicateCode`, `basicPhysicalOperators.scala`):
  the conjuncts that are not `IsNotNull` as written, each preceded by the `IsNotNull` tests of the
  columns it references (and of the null-intolerant expressions it contains), and the `IsNotNull`
  tests nothing referenced at the end. The source's own note above that algorithm says it may revisit the
  order and consider reordering predicates: the order is an implementation choice.

The optimizer adds to this: `InferFiltersFromConstraints` prepends inferred conjuncts, which is
how reproducer 291's `(l2 + 12) = 5000000000` comes to be first, and `NullDownPropagation`
rewrites null tests (reproducer 4069). Spark 4 runs ANSI by default, so the leading residual
conjunct is often arithmetic that can raise. The goal is the owner's: Varka raises wherever
vanilla Spark raises and returns the same rows wherever it does not, with the default session
(whole-stage codegen on) as the vanilla Spark of the differential.

### 3.2 The mechanism: demote, in the compiler

`VarkaExpressionCompiler.predicate` already loops: it classifies the conjuncts
(`predicateOnce`), and when the size admission refuses the fused set it demotes a conjunct and
classifies again. The order rule joins that loop, ahead of the size admission: after each pass,
`VarkaConjunctOrder.behindRaising` finds the fused conjuncts that Spark evaluates after a residual
conjunct that can raise, and they are demoted, with the reason "Spark evaluates it after `<that
conjunct>`, which can raise an error; fused, it would drop the rows that raise it". A demoted
conjunct that can raise is itself a raising residual on the next pass, and a size demotion can
make another, so the rule is asked again every round; the demoted set only grows, so the loop
ends. The residual `FilterExec` then evaluates the demoted conjuncts with the raising one, in
Spark's order.

The check holds a fused conjunct to both of Spark's orders: it stays fused only if it comes before
the first raising residual conjunct in the written order *and* in the codegen order. The written
order alone would keep `isnotnull(l)` fused ahead of a raising `(l2 + 12) = ...`, which codegen
evaluates first; the codegen order alone would be wrong for a session that runs interpreted.

Whether a conjunct can raise is `VarkaConjunctOrder.canRaise`, conservative: a tree is error-free
only when every node is one of attributes, literals, `IS [NOT] NULL`, `NOT`/`AND`/`OR`,
comparisons, `IN`, `IF`/`CASE`/`COALESCE`, `startswith`/`endswith`/`contains`, `+`/`-`/`*`/negation
over floating point in any mode or over integers outside ANSI, and a widening cast (number to
wider number, date to timestamp, anything to a string). Anything else counts as raising, which can
cost fusion and never an answer. The compiler had no such check to reuse: its ANSI handling is
the range analysis of the arithmetic it fuses (`intArith`, `IntRangeOps`), and a residual
conjunct is by definition one it did not compile.

It lives in the compiler, not in `VarkaColumnarRule.rewriteFilter`, because three callers classify
the same condition - the rule, the evaluator in the task, and `VarkaFusionReport` for EXPLAIN -
and must agree; EXPLAIN then shows the demotion reason per conjunct with no extra code.

### 3.3 The alternatives, and why they are not built

1. **Run the residual conjuncts first, on the batch.** A row filter cannot sit below a columnar
   node without converting the batch to rows and back; it gives up the kernel's row reduction on
   every mixed filter, not only the affected ones.
2. **Keep fusing, and use the fused mask only after the residual check** (for a row the mask
   rejects, still evaluate the residual conjuncts Spark would have evaluated first, for their
   errors). For the rows the fused prefix rejects this does nothing that demotion does not; for
   the rows it keeps, it saves evaluating the demoted conjuncts per row, at the cost of a second
   selection output (the prefix's mask beside the full one) and a per-row guard pass in both
   evaluators, which are row 318's files. It wins only on a shape with a fusible conjunct after a
   cheap raising residual and a selective fusible one after that; no filter benchmark has a
   residual conjunct at all (6), so there is nothing to measure it on. Left as an option for the
   owner (9).
3. **Fuse only the prefix before the first residual conjunct of any kind.** Correct without a
   raise check, but `s = 'x' AND d < ...` - row 273's shape and the common one - would lose its
   fused conjunct for a string comparison that cannot raise.

### 3.4 What is deliberately unchanged

* The kernel, the emitter and the evaluators: the fix changes which conjuncts fuse, not how a
  fused set runs, so no emitted byte moves and no `VarkaEmitOptions` switch is added (a switch
  that brings a wrong answer back is not an option arm).
* Row 273's fallback evaluates the whole condition as written, while codegen interleaves the
  `IsNotNull` tests. Where a *fused* conjunct raises on a row a later-written `IsNotNull` would
  reject, the two can still differ; that is the fallback's order, a narrower gap than this row's,
  recorded in 9.
* `VarkaFilterExec.scala`, `VarkaFilterEvaluator.java`, `VarkaColumnarRule.scala` (row 318) and
  `DeclineSink`, `VarkaRangeAnalysis`, `VarkaChronoCompiler` (row 379) are not touched.

### 3.5 Registered op counts

None move: no fused set that passes the rule changes, and the emitted-bytes fixture holds no split
filter with a raising residual conjunct.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/.../codegen/VarkaConjunctOrder.java` | new: `canRaise`, Spark's codegen order, `behindRaising` |
| `sql/catalyst/.../codegen/VarkaExpressionCompiler.java` | the order rule in `predicate`'s loop |
| `sql/catalyst/src/test/java/.../codegen/VarkaConjunctOrderSuite.java` | new JUnit suite |
| `sql/core/src/test/.../VarkaDifferentialSuite.scala` | the differential test of 5 |
| `sql/varka/fuzz/spark/varka-380-*.sql`, `README.txt` | the five reproducers made regressions |
| `dev/varka_nightly.sh` | the `sparkfuzz` step draws the nested trees again |

## 5. Tests, and what each is for

* `VarkaSparkReproducerSuite`: the five reproducers, now `status: regression`, agree.
* `VarkaConjunctOrderSuite` (catalyst, JUnit): `canRaise` over the list and its edges (ANSI against
  LEGACY integer arithmetic, floating point, interval arithmetic in LEGACY, a narrowing cast);
  the codegen order of a condition with `IsNotNull` tests referenced and unreferenced; and through
  `explainPredicate`, that a raising residual first demotes the fused conjuncts after it with the
  reason, that a fused conjunct written before it stays fused, that an `isnotnull` written first
  but placed after it by codegen is demoted, and that a residual which cannot raise demotes nothing.
* `VarkaDifferentialSuite`, "a residual conjunct that can raise ...": the conjunct-order case on a
  cached table under ANSI and LEGACY against the row engine - raising first, raising between two
  fused conjuncts, a fused conjunct that rejects the raising row before it, the codegen `IsNotNull`
  case, and year-month interval arithmetic under LEGACY - each with whether the plan still fuses.
* The nested-tree census of 6.

## 6. The measurement

**Correctness volume.** `VarkaSparkFuzzSuite` with the nested trees at the three seeds that met
the bug in VARKA-298 9.1 (20261102, 20261104, 20261105), 5,000 compositions each,
`-Dvarka.sparkfuzz.maxFailures` lifted, one JVM per seed on cores 12 to 23.

**Cost.** Every predicate of `VarkaFilterBenchmark` and `VarkaFilterNarrowingBenchmark` fuses
whole: none has a residual conjunct, so the rule demotes nothing and the plans and kernels are
those of the merge base. The pair on GitHub runners is given in 9 for the owner to dispatch.

### 6.1 Predictions, registered before the run

1. The five reproducers agree, each by Varka raising Spark's condition.
2. The census finds no disagreement in 15,000 compositions (VARKA-298 found three of 9.2's kind
   at these seeds).
3. The filter pair reads every row within the runner's band: the change is planning only, one
   pass over the conjuncts per classification round.
4. Existing Varka suites that assert fusion of a filter whose raising residual conjunct comes
   first now fail by design; I expect none or one, since the suites' mixed filters put a string
   comparison beside the fused conjuncts.

## 7. Risks

1. `canRaise` lists an expression that can raise: the census and the differential test would
   show it as a disagreement; each listed node is a comparison, a null test or a choice, plus
   arithmetic and casts guarded by mode and type.
2. Spark's codegen order changes upstream: the port in `VarkaConjunctOrder.codegenOrder` would
   drift silently. The rule also holds the written order, so a drift can only lose the cases where
   the two differ, which are the `IsNotNull` ones; the differential test's `IsNotNull` case shows it.
3. Fusion lost in real queries under ANSI: a leading BIGINT arithmetic conjunct now demotes every
   fused conjunct after it. That is the correct cost; alternative 2 of 3.3 is how to win it back.

## 8. Sequencing

1. This plan with the admission check.
2. The rule and its tests, the five reproducers made regressions, the nightly switch removed; the
   census and 9.
