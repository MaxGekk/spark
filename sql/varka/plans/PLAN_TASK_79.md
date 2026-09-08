# Task 79: the guard and the untaken arm

## 1. Where this came from

`PLAN_MILESTONE_4.md` row 79 and section 2.41, opened 7 September 2026 out of
task 60's review and widened by task 63 (`PLAN_TASK_63.md` 9.8) when checked
arithmetic became a third node kind disposing of its mask through the same
`emitGuardCollect`. The observation: a vector body computes both branches of an
`IfElse`, and a guard does not produce a value that a blend can discard - it
condemns the batch. So a guarded node inside a `CASE` arm declines batches on
rows the condition sends to the other arm. Answers stay correct, since the row
engine recomputes the declined batch; the fusion is what is lost.

## 2. The admission check, done

Three things were checked against master (`5448fe05f62`) with
`dev/varka_emit.sh`. One confirms 2.41's design hazard, one refutes 2.41's
motivating example, and together they change which fix is worth building.

### 2.1 The sharing hazard is real, and two lines of SQL reach it

2.41 warns that a guarded node can be shared between a use inside an arm and a
use outside it, so a guard qualified by the arm's mask would be emitted once
under that mask and reused by the unconditional consumer - which would then stop
declining batches it must decline. Silently wrong, not slow.

It is not hypothetical. For

    CASE WHEN d < DATE'2020-01-01' THEN add_months(d, m) ELSE DATE'1999-01-01' END,
    add_months(d, m)

the emitter's line map has seven nodes and `(addMonths 1 4)` appears once:

    1=col:0   2=lit:0   3=(cmp:LT 1 2)   4=col:1
    5=(addMonths 1 4)   6=lit:1   7=(if 3 5 6)

`sharedSlot` is keyed on `analysis.useCount > 1` with no notion of arm, and
`emitValue` reloads on `computed.contains(node)` alone; its own javadoc says
sharing crosses outputs "since the loop body is one straight line". So the
disjunction rule 2.41 states - a node used anywhere unconditionally keeps an
unqualified guard, and only a node used solely under arms is narrowed - is
mandatory for the honest fix, not a refinement of it.

### 2.2 The motivating example does not fuse, and cannot

2.41 motivates the task with

    CASE WHEN m BETWEEN -1000 AND 1000 THEN add_months(d, m) ELSE NULL END

and says the cliff "defeats exactly the range test the user wrote to keep the
shape fused". The emitter's answer for that projection is
`nothing fused: every entry declined`. The condition is the reason: a
comparison's operand goes through `compileNode`, whose value leaf is `DateType`
(and, since task 67, a year-month interval) - a bare `IntegerType` column in
predicate position has no arm and declines. `PLAN_TASK_63.md` 9.8 already noted
this for the arithmetic instance; it is true of the whole family.

**So the user cannot write the range test at all, and the sentence that
motivates this task is wrong.** The cliff is real - see 2.3 - but it is not
"your own guard is ignored". It is "a condition about something else does not
protect the arm", which is a weaker claim and changes the design trade in 3.

### 2.3 What is actually reachable

Conditions that fuse are date- or fused-field-driven, and both instances of the
cliff are reachable through them:

    CASE WHEN d < DATE'2020-01-01' THEN add_months(d, m) ELSE DATE'1999-01-01' END
      -> (if (cmp:LT col:0 lit:0) (addMonths col:0 col:1) lit:1), fused, and the
         column-count AddMonths is in `selfGuarding`, so its guard fires on every
         lane whatever the condition says.

    CASE WHEN d < DATE'2020-01-01' THEN i + 1 ELSE year(d) END
      -> (if (cmp:LT col:0 lit:0) (int:ADD:FAIL col:1 lit:1) (year col:0)), fused,
         with the ANSI check's mask going through the same collect.

Both fuse today and both decline a batch for a row the condition discards. That
is the task's whole surface: a guarded node under an arm, with a condition that
cannot mention the thing being guarded.

### 2.4 What the check would have rejected

That the sharing hazard was theoretical (it is two lines of SQL); that the
user's own bound is what the cliff defeats (the user cannot express it); and
that the cheap fix is obviously better (2.2 removes its main argument - see
3.2).

## 3. The design

### 3.1 What the two candidates are worth, after 2.2

2.41 offers two. *AND the arm's condition mask into the guard* keeps the shape
fused and needs 2.1's disjunction rule. *Exclude a node under an arm from the
guarded set* is cheaper and makes the shape decline at compile time with a
reason instead.

2.2 cuts the second one's argument down. It was attractive while the story was
"the user wrote a bound and we ignored it", because then a compile-time decline
at least tells them so. But the user cannot write that bound, so the choice is
between a shape that fuses and occasionally declines a batch, and a shape that
never fuses at all. Declining every batch of a shape that would otherwise fuse
whenever no extreme row is present is a regression for the common case, not a
repair - the opposite of task 78, where declining recovered a real loss.

**So this task builds the honest fix**, and the cheap one is recorded here as
the fallback if 3.3's measurement says the arm mask costs more than the
declines it prevents.

### 3.2 Threading the arm context

`emitValue`'s `IfElse` arm already emits `emitCond` before the branch values, so
the condition's known-true word is in `s.kt` and its dense mask in `s.condMask`
by the time a node inside an arm is emitted (2.41 read this out and it holds).
What is missing is context, not the mask: `emitGuardCollect` does not know which
arm it is under or with which polarity.

The change is an arm mask threaded through the value walk - the then-branch
takes `kT`, the else-branch its complement, nested arms compose by AND - and
`emitGuardCollect` ANDs it in beside the node's word and the epilogue mask.

### 3.3 The rule that keeps sharing sound

Per node, over the walk that already collects the guarded set: the guard's
qualifying mask is the **disjunction over the node's use contexts**. A node used
anywhere outside an arm has an unqualified guard; only a node used solely under
arms is narrowed, and then by the OR of those arms' masks. `SKILLS.md` records
this class one level down - task 32's prefix fragment had to be keyed on the
guard's extra input rather than the child alone - and this task cites it rather
than rediscovering it.

`Greatest` and `Least` are unaffected and the rule should say so where it is
written: they are validity-driven, with no untaken arm.

## 4. Files

| file | what |
|---|---|
| `VarkaLoopEmitter.java` | the arm mask threaded through `emitValue`; `emitGuardCollect` taking it; the per-node use-context disjunction in the guarded-set walk |
| `VarkaLoopEmitterSuite.scala` | 2.3's two shapes as status matrices; the shared-node case of 2.1 asserted to keep declining; the `codeSize` deltas |
| `VarkaDifferentialSuite.scala` | both shapes end to end, with the declined metric at zero where the extreme rows are all in the untaken arm |
| `VarkaEmitterParityBenchmark.scala` + results | 3.4's A/B, since the emitted bytes move |
| `PLAN_MILESTONE_4.md`, this file | row 79, section 2.41's motivating example corrected per 2.2, section 9 |

## 5. Tests, and what each is for

* **The status matrix**, over 2.3's two shapes: a batch whose only out-of-range
  row is sent to the untaken arm returns 0 instead of declining - the whole
  point - while a batch whose out-of-range row is in the *taken* arm still
  declines. The second half is what fails if the arm mask is ANDed with the
  wrong polarity.
* **The sharing case**, from 2.1: the guarded node used both inside an arm and
  bare in the same projection keeps declining. This is the silent-wrong-answer
  test, and it should be written before the fix so it fails first.
* **Nested arms**, one `CASE` inside another's arm, composing by AND.
* **The differential**, both shapes, with `numFallbackBatchesDeclined` at zero
  where today it is positive, and answers equal either way - answers cannot
  move, since only the decline route changes.
* **Every unguarded shape byte-identical**, which is the assertion that the
  thread reached only the guards.

## 6. The measurement

`VarkaEmitterParityBenchmark`, the guarded shapes it already carries, plus 2.3's
`CASE` forms: the arm mask adds one AND per guarded node per lane group, so the
prediction is that it is inside the noise on a memory-bound loop and the win is
the declines it prevents, which the differential counts rather than times.

### 6.1 Predictions, registered before the run

1. The arm mask costs under 3% at both widths on the `CASE` shapes, and nothing
   measurable on the shapes without an `IfElse`.
2. No unguarded shape's bytes move; the pinned line map and shape hash are
   unchanged for every shape with no guarded node under an arm.
3. On a fixture where every out-of-range row falls in the untaken arm, the
   declined count goes from positive to zero, and the answers do not move.

## 7. Risks

1. **Polarity.** The else-branch takes the complement of `kT`, and a
   three-valued condition makes "not known-true" and "known-false" different
   sets. The rule must use the arm's own mask, not the negation of the other's.
2. **The sharing rule** (2.1, 3.3), which is the one that is silently wrong if
   it is got wrong; its test is written first.
3. **Nested arms composing by AND** and the epilogue mask still being ANDed in.
4. **A guarded node under a `Coalesce`**, which compiles to `IfElse` - the rule
   must treat it as an arm like any other, and the test list says so.

## 8. Sequencing

1. This plan and the milestone row; 2.41's motivating example corrected in place
   with a note saying what replaced it.
2. The failing tests: 5's sharing case and the status matrix.
3. The use-context disjunction in the analysis, then the arm mask in the walk.
4. The A/B, section 9, row 79.

## 9. Outcome

<!-- Filled in when the measurement lands: the numbers with the committed file
     they trace to (dev/varka_quote_check.py holds you to this), 6.1's
     predictions scored one by one, what moved that the plan did not list, and
     what the task leaves for later - which goes to the milestone's debt
     register or a scope document, never to a code comment. -->
