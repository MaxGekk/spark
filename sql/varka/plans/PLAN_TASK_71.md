# Task 71: what the loop-method budget is actually bounding

## 1. Where this came from

`PLAN_MILESTONE_4.md` row 71 and section 2.35, out of task 32 step B2's
regeneration. `GROUP_BUDGET = 16` rests on one measurement, task 17's: two
outputs over a shared depth-8 chain ran about 1.4x faster as two loop methods
than as one, read as register pressure. The parity file has carried both arms
ever since so that a retune would be measured rather than argued, and they have
measured the other way since `aef0b82260e`.

## 2. What is already established, before this plan proposes anything

### 2.1 The reversal is real, and it is not the file's noise

The committed file, both widths:

| width | budget 16, split | budget 24, merged | relative |
|---|---|---|---|
| AVX-512 | 5002.7 | 6593.1 | 1.3X |
| 128-bit | 2319.5 | 3078.9 | 1.3X |

And it is not the file's noise. Over task 77's ten runs per width of an
unchanged file (10 September 2026, idle and pinned), merged is ahead in **ten
runs of ten at both widths**, with the merged-over-split ratio ranging 1.29 to
1.32 at AVX-512 and 1.32 to 1.33 at 128-bit. Both arms sit in the band's quiet
tiers - 3.6% and 4.4% spread at AVX-512, 0.8% and 1.0% at 128-bit - so a 31% gap
is an order of magnitude outside anything the band would excuse. Those ten-run
medians are scratch data and are deliberately not quoted here; the committed
rates above are the file's, and the ratio range and the win count are facts about
the experiment rather than numbers wanting a home.

The shipped default loses about 31% on the one shape it was chosen from.

### 2.2 Nothing measured so far favours a narrow method

B2's ceiling ladder (`PLAN_TASK_32.md` 7.6) put `add_months` outputs over one
date into one method against several: one method won at six, eight and twelve
outputs at both widths, and every split cost - 9% and 17% at twelve outputs
against a ceiling of 400, 31% and 36% against a ceiling of 200.

### 2.3 The counterweight is compile time, not register pressure

Also from that ladder, under `-XX:+PrintCompilation`: past about 1900 bytes C1
refuses a loop method ("out of virtual registers in LIR"), so it runs
*interpreted* until C2 lands - about 340 ms at 376 ops, once per shape per JVM
behind task 18's class cache. That is what stopped the ceiling at 400 rather
than 700, and it is the cost a budget ladder has to price too.

### 2.4 There are two bounds, and the asymmetry is the real question

`groupOutputs` decides with one condition
(`VarkaLoopEmitter.java:891-892`):

    boolean fits = group.ops + marginal <= options.groupBudget()
        || (withNext.saved > 0 && group.ops + marginal <= options.fusedCeiling());

`saved` counts **civil-from-days prefix reuse only** - `walk` adds to it exactly
when `sharePrefix && isChrono(node)` and the group already computes that date's
prefix. So a group whose outputs share a *calendar prefix* may reach 400 ops in
one method, while a group whose outputs share an *arithmetic chain* is held to
16. Task 17's pair is the second kind. It has twenty distinct ops and no prefix,
so clause 2 never applies to it and the budget splits it - and merging it wins
by 31%.

**So the row's question is narrower than the situation.** "Is 16 still the
budget" treats the number as the variable. The measurement says the variable may
be what `saved` counts. Section 2.35 already anticipated this - "whether clause 2
should widen with it is the same measurement's second column" - and this plan
takes it as a first-class arm rather than a footnote.

## 3. The admission check, to do first

### 3.1 What is the merged arm's win actually made of?

The javadoc's own reading is that task 17's loss was a refused `orValidityBitsAt`
call in the wider method, which task 46's reordering let inline, and that
register pressure was never binding at 24 ops. **That is a hypothesis with a
number attached to it and no direct evidence.** If it is right, the budget was
never bounding what it claims to bound, and picking a new number without knowing
that is picking a number.

Establish it before the ladder: `dev/varka_emit.sh --table` for the op counts of
both arms, `codeSize` per loop method at each rung, and `-XX:+PrintInlining` on
the merged arm to see whether the OR is inlined now where it was refused before.
The recipe is `SKILLS.md`'s "A refused call is refused by the caller's budget,
and the caller's budget is spent in program order", which measured exactly this
on this helper.

### 3.2 Which rows can carry a verdict, and which cannot

Section 2.35 names task 17's pair, two chains over a shared subchain, the
`CASE WHEN` arms, the DAG-CSE outputs and the mod-7 pair. Task 77's band says
they are not equally readable:

| row | AVX-512 | 128-bit |
|---|---|---|
| task 17's pair, both arms | tier 1 (3.6%, 4.4%) | tier 0 (0.8%, 1.0%) |
| `CASE WHEN, depth-4 arms` | tier 2 (11.8%, 21.8%) | tier 1-2 |
| `arithmetic depth 4` | tier 2 (10.3%) | tier 2 (23.5%, 24.6%) |
| `fused, CSE` (DAG-CSE) | **unreadable** (28.7%) | tier 2 (11.8%) |

Only task 17's pair can settle a small effect. The DAG-CSE row cannot carry a
wide-width verdict at all, and the plan says so rather than quoting it. Where a
rung's effect lands inside a row's tier, the honest report is "this row does not
distinguish these rungs", not a ranking.

### 3.3 What a default change costs in pinned oracles

The default is not a number in isolation: moving it re-partitions loop methods,
and a large part of the emitter suite addresses methods **by name**. Enumerate
before choosing, because this cost may exceed the gain:

* Method-count assertions taken at the default -
  `VarkaLoopEmitterSuite.scala:1810-1856` (six `loops(...) === N`, one of them
  reasoning explicitly "1 + 38 > 16"), `:2792`, `:2806-2811`, `:3014-3015`,
  and `:3030` which pins the names `loopMasked0`/`loopMasked1`.
* Roughly two dozen pinned op-count oracles read off `loopDense0`/`loopMasked0`
  by name; a regroup that renumbers or merges methods invalidates the addressing
  even where the total does not move.
* `VarkaAssemblySuite.scala:634-694` pins literal method names inside expected
  C2 frame strings.

Whether these are re-pinned or the default stays and only the *rule* changes is
an outcome of the ladder, not an assumption of it.

### 3.4 There is no existing guard that a budget change moves only what it should

B2's byte-identity test
(`VarkaLoopEmitterSuite.scala:2820-2865`, "with no prefix to reuse, sharing
changes no loop method") compares `shareChronoPrefix` **off against on at one
budget**. It is not a budget guard, and section 2.35's sentence that it "stays as
the assertion that a budget change changes exactly the shapes it names" reads it
as more than it is. This task owes the guard that sentence describes: a corpus
whose grouping must not move between two budgets, asserted on method names and
`codeSize`.

### 3.5 What the check would have rejected

That `GROUP_BUDGET` is a hard constant needing new plumbing (it seeds
`DEFAULTS` and the emitter reads `options.groupBudget()`; `withGroupBudget`
exists and the benchmark already uses it, so a ladder is extra `emit` calls and
fresh case ids). That B2's guard covers budgets. That the DAG-CSE row can rank
rungs. That the question is only the number. And that the reversal might be
noise - 2.1 settles that.

## 4. The design

### 4.1 The ladder, and its second column

Two dimensions, measured together because they answer one question:

* **The budget**, at 16, 24, 32, 48 and 64, on task 17's pair and on the shapes
  3.2 admits, at both widths.
* **Clause 2's reach**, as an emit option so it is priced rather than argued:
  today `saved` counts prefix reuse only; the arm counts whole-node reuse too,
  which is what task 17's pair has. If the wider clause makes the budget
  irrelevant for these shapes, that is a better answer than a bigger number,
  because it leaves the bound where compile time actually needs it.

Compile time beside throughput at every rung, per the standing rule and 2.3:
first tier-4 landing per loop method, whether C1 refused, and the kernel's total
settle time.

### 4.2 The guard 3.4 found missing

A budget-identity test over a corpus with nothing to regroup - a single output,
two outputs sharing nothing, an output already wider than any rung - asserting
identical method names and `codeSize` across two budgets. It fails the day a
budget change reaches a shape it has no business reaching.

### 4.3 What this task does not do

The fragment mechanism, `FUSED_CEILING`, a single output's internal width (task
43 measured it and left the decision to the emitter), `MAX_FUSED_NODES`, and
task 44's epilogue - which 7.6 shows is what makes a twelve-output kernel slow
to settle at any grouping.

## 5. Files

| file | what |
|---|---|
| `VarkaEmitOptions.java` | the clause-2 reach as an option, if 4.1's second arm is built |
| `VarkaLoopEmitter.java` | `groupOutputs`' condition; `GROUP_BUDGET`'s javadoc, which currently sends the retune to "task 43's question" and means this row |
| `VarkaEmitterParityBenchmark.scala` | the ladder's rungs, on fresh case ids (600 and 601 are taken; enumerate with `dev/varka_bench_ids.sh`) |
| `VarkaLoopEmitterSuite.scala` | 4.2's guard, and whatever 3.3's enumeration says must be re-pinned |
| `VarkaAssemblySuite.scala` | only if the default moves and the frame names with it |
| benchmarks + bands | regenerated, gated, and the band re-measured if grouping moves enough rows |
| `PLAN_MILESTONE_4.md`, this file | row 71, section 2.35's correction about the guard, section 9 |

## 6. Tests

* **4.2's budget-identity guard**, which is the one this task owes outright.
* **The grouping assertions of 3.3**, re-pinned with their reasoning updated
  where the default moves - each one states an arithmetic ("1 + 38 > 16") that
  is part of the record, not incidental.
* **The differential is untouched**: grouping changes which method holds an op,
  never the answer. If any differential moves, the change is wrong.

## 7. The measurement, and predictions registered before it

1. **The merged arm's win is the inlining, not the budget.** `-XX:+PrintInlining`
   shows `orValidityBitsAt` inlined in the merged arm at 24 ops where task 17's
   evidence had it refused. If instead it is refused in both and merged still
   wins, the register-pressure reading was wrong for a different reason and the
   ladder is measuring something nobody has named.
2. **The budget's win keeps growing to at least 32 and then flattens**, because
   nothing in 2.2 favours narrow and 2.3's cliff is a byte count that twenty
   ops do not approach.
3. **The wider clause 2 makes the budget nearly irrelevant on these shapes**: at
   any rung, whole-node reuse groups task 17's pair, so its rows differ by less
   than their band. That would make the rule the answer and leave the number to
   compile time.
4. **A default change re-pins more than ten assertions.** 3.3's enumeration is
   the estimate; the prediction is that the true count exceeds it, because
   pinned oracles address methods by name and the enumeration found them by
   grep.

## 8. Risks

1. **A number that helps one shape and hurts another.** The corpus is small and
   task 17's pair is the only quiet one, so a rung that wins there and moves the
   others inside their band is not a measured win. State it as such.
2. **The pinned-oracle churn is the real cost** and it is paid in a file where
   the numbers are also the record. 3.3 is enumerated before anything is chosen.
3. **Compile time is a startup cost, so a throughput ladder will not see it.**
   2.3's method is the one that does, and it is part of the measurement rather
   than a follow-up.
4. **Widening clause 2 touches every shape with a shared subtree**, which is far
   more than task 17's pair. 4.2's guard is what bounds it, and it is written
   before the option.

## 9. Sequencing

1. This plan and row 71.
2. 3.1's mechanism probe, which decides whether the ladder is measuring the
   budget or the inliner.
3. 3.3's enumeration of what a default change would re-pin.
4. 4.2's guard, before either arm is built.
5. The ladder, both dimensions, both widths, with compile time beside it.
6. The default and the rule chosen from the numbers; the re-pinning; the
   regeneration, gated and banded.
7. Section 2.35's correction, row 71, `SKILLS.md`, section 10.

## 10. Outcome

### 10.1 Step 3.1's mechanism probe: the win is the CSE, and prediction 1 is wrong

Emitted both arms directly and counted what is in them, then ran each alone in
its own JVM under `-XX:+PrintCompilation` and `-XX:+PrintInlining`.

**What each arm contains**, per loop method, from the emitted bytes:

| budget | loop methods | bytes each | `IntVector` calls | `VarkaVectorSupport` calls |
|---|---|---|---|---|
| 16 | 2 | 432 | 30 each | 5 each |
| 24, 32, 48, 64 | 1 | 504 | 43 | 5 |

Merging takes the kernel from 60 vector ops and 10 support calls per lane group
to 43 and 5 - a 31% reduction in work. The committed throughput gain is 1.3X.
The two numbers match closely enough that nothing else needs explaining: the
merged arm wins because the shared depth-8 chain is computed once instead of
twice, which is the cross-output CSE the budget was splitting.

**Prediction 1 is refuted, and by the absence of the thing it named.** It said
the win would be `orValidityBitsAt` inlining in the merged arm. That call is
**not emitted at all** for this shape - zero occurrences across a full
`-XX:+PrintInlining` log of both kernels. Task 70's bitmap pass serves these
roots (two chains over one column, so the word is a bare leaf), which removed
the per-group OR entirely. The javadoc's hypothesis is about a call that no
longer exists here. It may still be the right account of *why the rows reversed
at `aef0b82260e`*, which predates task 70 and is not something this probe can
reach; what it is not is the reason the merged arm wins today.

**Compilation behaviour is identical per method**, which took a second run to
establish honestly. Run in the benchmark's own order the split arm showed ten
compilation events against the merged arm's three, which looks like a
recompilation difference and is not: the split arm runs first and pays the
warmup. Run alone in a fresh JVM each, every loop method in both arms compiles
four times at tier 3 and three at tier 4. The split simply has two methods, so
it compiles twice as much. (Three tier-4 compiles of one method is itself more
than a healthy method needs, but it is the same in both arms, so it is not what
separates them - that is task 90's.)

### 10.2 What this changes in the ladder

**Rungs above 24 are byte-identical on this shape.** The table above is the
whole of it: 24, 32, 48 and 64 emit the same single 504-byte method. So the
five-rung ladder of 4.1 has exactly two distinct outcomes here, and any ranking
among the upper rungs measured on this shape would be measuring the file's
noise. The ladder needs shapes that straddle the higher budgets to say anything
about them, and 3.2 already showed the other candidate shapes are too noisy to
rank small effects. **What the ladder can actually establish is where the
split/merge boundary should sit, not which of several wide budgets is best.**

### 10.3 Step 3.3's enumeration: the re-pinning cost is one assertion, and
prediction 4 is wrong

The enumeration was done by changing the default and running the suites, not by
grepping for what might move - a grep finds what addresses a method by name, and
what matters is what that addressing actually *sees*.

| default | catalyst Varka suites | sql/core Varka suites |
|---|---|---|
| 16 (shipped) | 267 pass | 180 pass |
| 24 | 267 pass, 0 fail | - |
| 64 | 266 pass, **1 fail** | 180 pass, 0 fail |

**At 24, nothing moves at all.** The roughly two dozen pinned op-count oracles
that read `loopDense0`/`loopMasked0` by name are unaffected, because the shapes
they use are single-output or already grouped; the addressing is only fragile in
principle.

**At 64, exactly one assertion fails**, and it is the one whose reasoning is
written out in the source: `VarkaLoopEmitterSuite.scala:1838-1841`, "year reuses
nothing against [x + 1] and 1 + 38 > 16, so it opens a group of its own, which
month then joins". At 64 that sum fits and the two become one method. The
assertion did exactly what an assertion with its arithmetic spelled out is for.

**Prediction 4 said a default change would re-pin more than ten assertions, and
that the true count would exceed the grep's estimate.** It is one, and only past
24. The prediction reasoned from how the tests *address* methods rather than from
what those tests actually construct, which is the same error in miniature that
2.39 made - inferring from a shape instead of measuring it.

**Two gaps in this enumeration, stated rather than buried.** `VarkaAssemblySuite`
cancels all 23 of its tests in this environment for want of a disassembler, so
its `loopDense0` frame-name pins were never exercised at any budget; they are
checked on a host that has one, or the task ships not knowing. And the
benchmarks were not run here - the parity file's case names carry loop-method
counts, so a default change moves the file's text as well as its numbers.

**The differential does not move**, at any budget tried, which is the claim
section 6 makes: grouping decides which method holds an op, never the answer.
