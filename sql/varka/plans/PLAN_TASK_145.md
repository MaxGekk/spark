# Task 145: a narrowed filter loses the columnar path

*Scoped 18 September 2026 (milestone 5 section 2.81, row 145) from task 144's
crossed experiment, corrected twice the same night; planned 22 September 2026.*

## 1. The question

Three shapes over twenty million rows under a `noop` sink, from
`PLAN_TASK_144.md` 9.3:

| query | node | columnar sink | row path forced |
| :--- | :--- | ---: | ---: |
| `SELECT i FROM t WHERE i > 50000` | no narrowing | 862.3 | 144.4 |
| `SELECT i2 FROM t WHERE i > 50000` | narrowing | 120.3 | 142.8 |
| `SELECT i, i2 FROM t WHERE i > 50000` | no narrowing | 901.3 | 109.5 |

Two readings were taken and both were wrong. The first was "forwarding a
column costs ten times"; the second was "a filter that narrows its output
costs eight times". The control that refutes both is the right-hand column:
with the row read-back forced by `toRdd`, the narrowed and un-narrowed shapes
are within 1% of each other, and task 78 had already measured that shape at
three selectivities and both widths and found the narrowed form slightly
*faster*.

So the narrowing costs nothing. What the un-narrowed shapes have is something
the narrowed one lacks: they are seven times faster under a columnar sink than
under a forced row path, and the narrowed one is not faster at all. They stay
columnar end to end; it does not. The 120.3 is task 19's read-back floor,
reached through a plan difference rather than through a kernel difference.

The question is what that plan difference is, given that
`VarkaFilterColumnarToRowExec.columnarSibling` already builds
`VarkaProjectExec(narrowing, VarkaFilterExec(condition, child))` - the
columnar-out node computing exactly what the narrowed transition computes.
The machinery for the columnar answer exists. Something is not reaching it.

## 2. The change

This is an investigation with two admissible endings, which is why the row's
acceptance line is "route it there or record why not". Step 1 is not a change
at all.

### 2.1 Step 1: read the plans, before changing anything

Print the physical plan for all three queries under a columnar sink and under
a forced row path, six trees, and record them in section 5. This is minutes of
work and it either confirms the hypothesis below or replaces it. The project's
own lesson is that a reading taken from a rate rather than from the mechanism
has been wrong twice on this very row.

### 2.2 The hypothesis the code suggests

`VarkaColumnarRule` has two stages. Before transitions, a `ProjectExec` becomes
a `VarkaProjectExec` only when `isVarkaEligible(projectList, child.output)`,
which asks the compiler whether at least one entry *fuses* - and forwarding a
bare column is explicitly not fusing it. `SELECT i2 FROM t WHERE i > 50000`
projects one bare column, so it is not eligible, the pre stage leaves an
ordinary `ProjectExec`, and the transition pass inserts a row boundary under
it. After transitions, the arm that exists for this shape absorbs the
projection into the filter's *to-row* node:

    case ProjectExec(projectList, filter: VarkaFilterColumnarToRowExec)
        if filter.narrowing.isEmpty && isForwardedNarrowing(projectList, filter.output) =>
      filter.copy(narrowing = Some(projectList))

That absorption is right on its own terms - it saves a row-level projection
above the transition - but it settles the plan at the row boundary. The
un-narrowed shapes never acquire a projection to absorb, so the pre stage
leaves `VarkaFilterExec`, which is columnar out, and the sink consumes it
columnar.

If that is what the trees show, the mechanism is: **a projection that only
narrows is not eligible, so the only node that can perform it is a row node,
even when the consumer wanted columnar.** `columnarSibling` is never consulted
because nothing strips this transition on a plain query path; the one in-tree
consumer that does is `ArrowCachedBatchSerializer.convertToColumnarPlanIfPossible`
on the cache-population path.

### 2.3 If the hypothesis holds, the arms

| arm | what changes | reach |
| :--- | :--- | :--- |
| A | the pre stage builds `VarkaProjectExec(narrowing, VarkaFilterExec(...))` when the child is columnar and the projection only narrows | the plain query path and the cache path both |
| B | the post stage consults `columnarSibling` when the consumer supports columnar | narrow, and duplicates what transition insertion already decides |
| C | leave it, and record that a narrowing consumer pays the read-back | costs nothing, explains the 120.3 |

Arm A is the one to build if the trees confirm the hypothesis: it makes the
eligibility question "can Varka serve this plan columnar" rather than "does
any entry fuse", which is the question the sink is actually asking, and it
reuses the sibling's own construction rather than inventing a second one. Arm
B repeats the transition pass's decision in a second place, which is the shape
of bug the `VarkaFusedTransition` trait exists to prevent. Arm C is the honest
ending if A turns out to regress a shape that fuses today, and section 5 says
so either way.

**The risk arm A carries, and the test that catches it.** A projection that
narrows *and* fuses must keep its existing plan; the new case has to run after
the eligible arm, exactly as the absorption case already does, and a suite
assertion pins that ordering rather than leaving it to the reader.

## 3. Predictions, registered before the run

1. **The plan trees differ at the projection, not at the filter.** Under a
   columnar sink, the un-narrowed shapes end at `VarkaFilterExec` and the
   narrowed one at `VarkaFilterColumnarToRowExec` with `narrowing` set. If
   instead both end at the same node, the hypothesis is wrong and section 5
   records what the trees showed.
2. **The narrowed shape under arm A lands within 10% of the un-narrowed one**
   under a columnar sink - near 862.3 rather than near 120.3 - because the
   kernel is the same one and the difference is a read-back that no longer
   happens.
3. **No shape that fuses today changes plan.** The eligible arm runs first, so
   a projection with anything to fuse is untouched, and `emitted_bytes.json`
   does not move because no IR changes.
4. **The row-path column does not move.** Arm A adds a columnar route; it does
   not touch what a row consumer gets, so 142.8 stays 142.8 within its band.

## 4. Verification

- `VarkaFilterExecSuite` and `VarkaProjectExecSuite`, with a plan-shape
  assertion per arm of the rule and the ordering assertion of 2.3.
- `VarkaDifferentialSuite`, for the answers: a narrowing filter must select
  the same rows and the same columns whichever node runs it.
- `VarkaEmittedBytesSuite` and `VarkaCoverageSuite` unchanged, for prediction 3.
- The three queries re-measured under both sinks with the same harness task
  144 used, on an idle machine, and the table of section 1 reprinted beside the
  new numbers in section 5.
- `VarkaTimeArrowCacheSuite` and the serializer suite, because the cache
  builder is a columnar consumer of exactly this shape and arm A changes what
  it is handed.
- `dev/scalastyle`, and the 100-column and non-ASCII scans.

## 5. Outcome

<!-- filled when the work is done: the six plan trees first, then the arm taken -->

## 6. Explicitly out of this task

- **The read-back floor itself** (task 19, and item 13 of
  `SCOPE_MILESTONE_6.md`): this task removes one plan's need to pay it, not
  the cost of paying it.
- **Whole-stage codegen support** for the Varka transition nodes, which the
  `VarkaColumnarToRowExec` comment records as a follow-up and which no
  measurement here needs.
- **Narrowing inside the kernel.** A filter that narrows is forwarding
  columns, not computing them; making the kernel drop columns is a different
  design and nothing here asks for it.
