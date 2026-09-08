# Task 78: a forwarded-only projection over a Varka filter

## 1. Where this came from

`PLAN_MILESTONE_4.md` row 78 and section 2.40, adopted 7 September 2026 out of
task 62's run and carried in the milestone's debt register as the one entry
there where the answer to "should Varka have run here" is no. The number that
motivated it is `SELECT d FROM t WHERE d < d2` at **0.66x** of stock Spark with
a columnar consumer and **0.57x** counted, from the 1B-row run recorded in
`PLAN_TASK_62.md` 10 - not the 0.64x that section 2.40 and the milestone row
still quote, which is the earlier laptop figure of `PLAN_TASK_62.md` 9.1 and is
superseded. A second loss in the same family survives that run: `d IS NOT NULL`
counted at 96.8% selected, **0.43x**, which `PLAN_TASK_62.md` 10 names as task
19's read-back floor and puts in the public table as the loss it is.

Both matter now rather than later because row 62 closes the milestone by
rewriting the README from that run for outside readers, and these are the two
rows in it where the fork is slower than the engine it forked.

## 2. The admission check, done

The design in section 2.40 is stated as two layers. There are three, and the
committed run already separates them further than that section does. What
follows is checked against the code on master (`d468c131534`) and against the
1B-row run's own plan classification, not inferred from the throughput ratios.

### 2.1 Which shapes are affected, and why exactly those

The rule's eligibility test is
`isVarkaEligible = VarkaExpressionCompiler.compilePartial(...).isDefined`
(`VarkaColumnarRule.scala:123`), and `compilePartial` returns `Some` only when
**at least one entry fused** and the fused trees reference a column
(`VarkaExpressionCompiler.scala:289-293`). A projection of bare forwarded
columns fuses nothing, so the rule declines it in both stages and a Janino
`Project` is left above the Varka node.

But that alone does not predict which rows lose, and the committed surface
shows it: every filter row in the benchmark is
`SELECT d FROM varka_dates WHERE <pred>`
(`DateSurfaceBenchmark.filterColumnarQuery`), so a narrowing projection is
written on *all* of them, and only three are classified `PARTIAL`. The three
are exactly the two-column predicates - `d < d2`, `d = d2`,
`d < d2 AND month(d) = 6`, the entries `Surface.java` marks `residualFilter`
- and every one-column predicate is `FUSED`.

The separator is Spark's own column pruning, upstream of this rule. For a
one-column predicate the pruned relation output is `[d]`, the filter's output
is `[d]`, and `Project [d]` over it is redundant and removed before any
columnar rule runs. For a two-column predicate the pruned output is `[d, d2]`,
the filter forwards both (`VarkaFilterExecBase.output` is
`outputWithNullability(child.output, ...)` - a filter narrows nothing), and
`Project [d]` survives because it genuinely has two columns to narrow to one.

**So the shape is not "a forwarded-only projection over a Varka filter". It is
"a predicate over more columns than its consumer wants".** The title this task
inherited describes the symptom; the admission check's first result is that the
condition is a property of the predicate's arity against the parent's required
set, which is a plan-time signal the rule can read directly - and that is what
makes the cheapest candidate in section 2.40 implementable at all.

### 2.2 The three layers, and what each costs

**Layer 1, the operator the rule declines to absorb.** The surviving
`Project [d]` is a separate operator: a boundary out of the Varka node's
iterator and a per-row `UnsafeProjection` over the already-materialised row.

**Layer 2, the read-back floor.** `VarkaFilterColumnarToRowExec` is
deliberately not `CodegenSupport` (`VarkaFilterExec.scala:288-289`), so it
serves rows through `Iterator[InternalRow]` at task 19's ~25 ns per row, while
stock plans `ColumnarToRowExec` - which is `CodegenSupport` - into one
generated loop with the predicate at 14.8 ns per input row for this query.

**Layer 3, the width of the row that crosses it.** This is the one section
2.40 does not name, and it is inside layer 2 rather than beside it. The
conversion is `UnsafeProjection.create(childOutput, childOutput)`
(`VarkaFilterExec.scala:356`), applied per selected row in `selectedRows`. It
serialises **every** child column - so for `SELECT d ... WHERE d < d2` the node
copies `d2` into an `UnsafeRow` for every selected row, and the `Project` above
immediately discards it. Stock's whole-stage loop reads the columnar vectors
directly through generated accessors and builds one row at the end, of the
columns the consumer actually asked for.

Layer 3 is why "prune the output" is a real fix and not a cosmetic one: it
removes work, it does not merely move an operator.

### 2.3 What the committed numbers do and do not already settle

Section 2.40 says the task's first step is "one run of the throughput row with
the projection removed by hand", to size which layer costs what. That step is
still needed, and the check found the reason the existing data cannot stand in
for it.

`PLAN_TASK_62.md` 10 says "The same shape without the narrowing, `d = d2`, runs
6.95x". **That sentence is wrong and this plan corrects it:** `d = d2` is
`Surface.residualFilter("d = d2")` (`Surface.java:108`), a two-column predicate
carrying exactly the same narrowing `Project`, and the 1B-row run classifies it
`PARTIAL` for that reason. It is not a control for the narrowing. What differs
between it and `d < d2` is selectivity - an equality between two independent
date columns selects almost nothing, `<` selects about 70% - and the cost of
layers 2 and 3 is proportional to **selected** rows, which is why the two rows
sit at 6.95x and 0.66x with the same plan shape.

The correction is not pedantic: it is the difference between "the projection
costs 6.3x" and "we have not measured what the projection costs". The A/B in
section 6 therefore holds selectivity fixed and varies only the projection.

What the run *does* settle is layer 2 alone: `d IS NOT NULL` counted at 96.8%
selected is 0.43x with no residual projection in its plan, on a predicate whose
kernel is nearly free. That is the floor, measured, with layers 1 and 3 absent.
So the floor alone can lose 2.3x, and any design that only removes the
projection has that as its ceiling on this family of shapes.

### 2.4 What the check would have rejected

That the losing shapes were a corner rather than a family (they are the
two-column predicates, which is `SELECT a FROM t WHERE b < c`); that the rule
could not tell them apart at plan time (column pruning has already made the
arity difference visible in `child.output` against the parent's required set);
that removing the projection would close the gap (layer 2's own 0.43x says it
cannot, on a high-selectivity shape); and that the existing surface already
contained the A/B (it does not - the apparent control shares the defect).

## 3. The design

### 3.1 The measurement first, because it chooses between the candidates

Nothing is built before section 6's A/B runs. Section 2.40 offers three
candidates and says the numbers should order them; sections 2.2 and 2.3 above
say what the numbers have to separate, which the current surface does not.

*Amended on 8 September 2026, by the owner's direction: candidate B is built
first and the A/B runs after it, when the machine that can run it is free. The
inversion is recorded rather than the paragraph rewritten, per
`sql/varka/AGENTS.md` - a plan is a record. What it costs is that the
measurement now validates a choice instead of making one; what makes that
acceptable is that B removes work rather than moving it (2.2's layer 3), so it
cannot be slower than what it replaces, and candidate A stays available if the
numbers say B does not reach 1.00x. What is given up is the chance to learn
that A was enough.*

### 3.2 Candidate A: decline the shape

Teach the rule that a Varka filter under a row consumer, whose output is wider
than the parent's required set, is a losing plan, and leave stock's plan alone.

This recovers to exactly 1.00x with no new machinery, and it is available now
in a way it was not for task 19: task 19 had no plan-time signal separating its
winners from its losers, and section 2.1 found one. Declining is a repair, not
a win, and the row must say so.

The risk to check before adopting it: the same signal must not decline the
shapes that currently *win*. Every one-column predicate has an equal output and
required set and is untouched by construction, which is the check.

### 3.3 Candidate B: absorb the narrowing

Either let `VarkaFilterExecBase` prune its output to the parent's required
columns, or let the rule take a forwarded-only projection above a Varka node.
Both remove layer 1's operator; only the first also removes layer 3's copy of
`d2`, so the first is the one worth building, and the second is what it looks
like if pruning proves too invasive.

Mechanically the node's `output` stops being `child.output` and the row
conversion becomes `UnsafeProjection.create(required, childOutput)`. The
`columnarSibling` contract and the cache serializer's conversion to
`VarkaFilterExec` both have to carry the required set, or a stripped transition
silently widens the output again - the same class of bug task 21 found when a
stripped transition silently dropped the filter.

**As built.** `VarkaFilterColumnarToRowExec` gained
`narrowing: Option[Seq[NamedExpression]]`, defaulting to `None`. `output` is the
projected schema when it is set; the `UnsafeProjection` the node already built
is created over `narrowing.getOrElse(childOutput)`, so the narrowed shape costs
nothing extra and the unnarrowed one is unchanged expression for expression;
and `columnarSibling` wraps the columnar filter in a `VarkaProjectExec` carrying
the same list, which is what keeps the cache serializer's swap honest.

The rule arm sits *after* the eligibility arm, so a projection with anything to
fuse still becomes a Varka projection node and only a forwarded-only one reaches
here - `isForwardedNarrowing` accepts a column or a rename of one and refuses
anything computed. It also refuses to fire on a node that already has a
narrowing, so the pass is idempotent. Nothing is absorbed across a residual
`FilterExec`, which falls out of the match shape rather than needing a test: the
residual predicate reads columns the narrowing would have removed.

### 3.4 Candidate C: fuse the boundary

Make the node `CodegenSupport`. This is scope item 13's first lever and the
only candidate that can put the shape above stock rather than beside it, and it
is the only one that touches layer 2, which section 2.3 shows is the binding
constraint at high selectivity. Item 13's admission check belongs beside this
one; if C is taken, the two are one piece of work and this plan defers to that
item rather than duplicating it.

## 4. Files

| file | what |
|---|---|
| `VarkaColumnarRule.scala` (+ suite) | candidate A's plan-time signal, or candidate B's forwarded-only acceptance |
| `VarkaFilterExec.scala` (+ suite) | candidate B: the required-column set on `VarkaFilterExecBase.output`, the narrowed `UnsafeProjection`, and the `columnarSibling` round trip |
| `VarkaDifferentialSuite.scala`, `VarkaSharedSessions.scala` | `SELECT d FROM t WHERE d < d2` through a row and a columnar consumer, at three selectivities |
| `DateSurfaceBenchmark.java`, `Surface.java` | the A/B entries of section 6; `residualFilter` flipped back to `filter` for whichever shapes the chosen candidate fuses |
| `PLAN_TASK_62.md` | 10's `d = d2` sentence corrected per 2.3, in place, saying what it corrects |
| `PLAN_MILESTONE_4.md`, this file | row 78, section 2.40's two-layer framing amended to three and its stale 0.64x requoted, section 9 |

## 5. Tests, and what each is for

* **The rule.** A two-column predicate under a narrowing projection plans what
  the chosen candidate says it plans, and a one-column predicate's plan is
  byte-identical to today's - the assertion that the fix did not reach the
  shapes that already win.
* **The differential.** `SELECT d FROM t WHERE d < d2` against the row engine
  through both a row and a columnar consumer, with the fallback metrics; under
  candidate B, a stripped transition (the cache serializer's path) returning
  the same columns, which is the bug class section 3.3 names.
* **The plan classification.** `DateSurfaceBenchmark.classifyPlan` reporting
  `FUSED` for any shape the candidate fuses, so `--expect-fused` holds it
  rather than recording it as expected-partial.
* **What none of these catch**, stated because it is the reason section 6
  exists: whether the change is faster. Only the A/B says that.

## 6. The measurement

One A/B that varies the projection and holds selectivity fixed, which is the
run section 2.3 shows the surface does not contain:

| query | what it isolates |
|---|---|
| `SELECT d FROM t WHERE d < d2` | layers 1 + 2 + 3, the shape as it ships |
| `SELECT d, d2 FROM t WHERE d < d2` | layers 2 + 3 with no residual projection - same predicate, same selectivity, the `Project` now redundant and removed |
| `SELECT count(*) FROM t WHERE d < d2` | the predicate with nothing crossing the floor |

Stock and fork, at 10%, 70% and 100% selected, since the read-back share moves
with selectivity. The first two rows' difference is layer 1; the second row's
distance from stock is layers 2 and 3 together; `d IS NOT NULL` counted at 0.43x
is already the floor's own row. Run through `dev/varka_bench_surface.sh` on the
same 512-bit runner task 62 pins, not the laptop, because the ratios being
compared are against stock on the same host.

### 6.1 Predictions, registered before the run

1. `SELECT d, d2 ... WHERE d < d2` is above 1.00x of stock at 10% selected and
   below it at 100%, because layer 2's cost is per selected row and the
   kernel's advantage is per input row.
2. Layer 1 is worth less than the gap to 1.00x at 70% selected, so candidate B
   alone does not turn `d < d2` into a win at that selectivity - if it does,
   the floor is cheaper than task 19 measured and that is a finding about task
   19, not about this shape.
3. Layer 3 is a measurable part of layer 2's cost - the two-column row is
   more than 10% slower to cross the floor than the one-column row at equal
   selectivity - which is what makes pruning worth building rather than just
   moving the operator.

## 7. Risks

1. **Declining a shape that wins.** Candidate A's signal keyed on output width
   against required width; every currently-fused filter row asserted unmoved.
2. **A stripped transition widening the output again** under candidate B: the
   `columnarSibling` and cache-serializer paths, which task 21's bug taught to
   check, with a differential through the cache.
3. **The A/B measured on the laptop** and read as if it were the pinned runner:
   task 62's own 9.1-to-10 correction is what that mistake looks like, and this
   plan's section 1 requotes it for the same reason.
4. **Candidate C escaping its scope.** If the numbers point at `CodegenSupport`,
   this task stops and scope item 13 opens; it does not grow into it.

## 8. Sequencing

1. This plan, the milestone row marked Planned, and section 2.40 amended to
   three layers with its stale figure requoted.
2. `PLAN_TASK_62.md` 10's `d = d2` sentence corrected, since a later reader
   would otherwise take it as the control it is not.
3. The section 6 A/B, on the pinned runner, committed with provenance.
4. The candidate the numbers choose, with section 5's tests.
5. The surface entries flipped for whatever now fuses; section 9; row 78.

## 9. Outcome

<!-- Filled in when the measurement lands: the numbers with the committed file
     they trace to (dev/varka_quote_check.py holds you to this), 6.1's
     predictions scored one by one, what moved that the plan did not list, and
     what the task leaves for later - which goes to the milestone's debt
     register or a scope document, never to a code comment. -->
