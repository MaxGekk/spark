# Task 87: every emitted method counted in the units the JVM enforces

*Planned 23 September 2026 together with task 168, on the owner's decision that
the two are one mechanism: task 87 was "the epilogue is the one method no budget
bounds" (`PLAN_MILESTONE_5.md` 2.18) and task 168 "one budget, counted in bytes,
over every emitted method" (`PLAN_MILESTONE_6.md` 2.2). The epilogue is just the
method nobody counted, so planning them apart risks 87 building a mechanism 168
replaces. Both rows point here.*

## 1. Where this came from

On 8 September 2026 a 35-million-iteration fuzz run built a 67244-byte
`epilogueMasked` for a nested `make_date` tree and the Class-File API refused
it, because a JVM method is capped at 65535 bytes (`PLAN_MILESTONE_5.md` 2.18).
The analysis then was that three caps each bound something other than bytes -
`MAX_CHAIN_DEPTH` one output's depth, `MAX_FUSED_NODES` the distinct ops,
`GROUP_BUDGET` one loop method's *weight* - and that the epilogue is emitted
once for every output with no cap applying to it at all.

Milestone 6 made this its first task because its closing post rests on the claim
that Spark cannot count what the JVM enforces - its own documentation says "we
cannot know how many bytecode will be generated, so use the code length as
metric" - while Varka, emitting bytecode through the Class-File API, can. Varka
cannot make that claim while it has the same defect.

## 2. The admission check, done

Four findings, each from the tools or the JVM rather than from reading the
emitter, and together they change what this task is.

### 2.1 The pinned reproducer no longer reproduces

`-Dvarka.fuzz.seed=2026092800 -Dvarka.fuzz.only=73411`, replayed on master
`c4e00a86484`:

    build/sbt "project catalyst" \
      'set Test/javaOptions += "-Dvarka.fuzz.seed=2026092800"' \
      'set Test/javaOptions += "-Dvarka.fuzz.only=73411"' \
      "testOnly *VarkaIrFuzzSuite"

passes, four tests green. `VarkaIrFuzzSuite` fails loudly on a rejected
emission, so the iteration now draws a different tree: the grammar has gained
nodes since 8 September (`GuardedRange`, `NarrowLane`, `BoundedDivide`, the long
lane), and a new node reshuffles the whole sample
(`sql/varka/skills/emitter-and-ir.md`, "Adding an IR node moves the bytes
oracle's fuzz digests"). **A fuzz coordinate is not a durable reproducer.** This
task pins its shapes as built expressions instead.

### 2.2 The ladder: bytes grow where weight does not look

`make_date(year(d), month(d), k)` for `k` in `1..n`, through `dev/varka_emit.sh`
at the host's preferred width (512 bits):

| n | `epilogueDense` | `epilogueMasked` | `loopMasked0` | epilogue `IntVector` | loop groups |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 968 | 1087 | 1065 | 93 | 1 |
| 4 | 2498 | 2716 | 2663 | 258 | 1 |
| 8 | 4536 | 4886 | 3285 | 478 | 2 |
| 12 | 6866 | 7500 | 3409 | 698 | 3 |
| 13 | 7533 | **8206** | 3440 | 753 | 3 |
| 14 | **8202** | 8914 | 3471 | 808 | 3 |
| 16 | 9524 | 10314 | 3533 | 918 | 4 |
| 32 | 20214 | 21600 | 4029 | 1798 | 7 |
| 48 | 30890 | 32886 | 4739 | 2678 | 10 |
| 60 | 38898 | 41338 | 5357 | 3338 | 12 |

Three readings.

* **The epilogue grows linearly, about 690 bytes an output**, because every
  output lands in it. The loop methods do not, because `groupOutputs` splits
  them - five `make_date` outputs to a group under `FUSED_CEILING`, since they
  share a civil-from-days prefix.
* **The loop methods grow too, and weight cannot see it.** `loopMasked0` goes
  from 3533 to 5357 bytes between 16 and 60 outputs while its `IntVector`
  count stays at 313. Whatever grows is not an op the budget counts.
* **Sixty outputs do not reach 65535.** `MAX_FUSED_NODES` (64) stops this family
  at 62 distinct ops, first. The class-file cap is not the first limit this
  shape meets - 2.3 is.

### 2.3 The first cliff is 8000 bytes, and Varka is over it at 13 outputs

On this JDK (`25.0.4.1`, product build) `DontCompileHugeMethods` is `true` and
`HugeMethodLimit` is a develop flag, fixed at 8000 and not settable. A method
larger than that is **never compiled, by either tier**. From the JVM's own
`-XX:+PrintCompilation`, running the kernel 50000 rounds of 1024 rows:

* **4 outputs.** `epilogueDense` (2498 bytes) compiled at tier 3 and tier 4.
* **16 outputs.** All four `loopDense<g>` compiled at tiers 3 and 4.
  `epilogueDense` (9524 bytes) **does not appear at all**.
* **16 outputs, `-XX:-DontCompileHugeMethods`.** `epilogueDense` (9524 bytes)
  compiled at tier 3 and tier 4.

So the absence is the huge-method rule and nothing else. By 2.2's sizes the
masked epilogue crosses 8000 bytes at 13 outputs and the dense one at 14; the
JVM's output confirms the rule on the dense method at 16, and the masked
crossing is inferred from its size under that rule - the probe of section 5 pins
both methods at every rung rather than leaving the inference standing. **From
13 or 14 outputs, then, the epilogue runs interpreted for the life of the
JVM** - a JIT cliff at an eighth of the size of the failure the task was opened
on, reached by an ordinary projection, and silent: nothing counts it, logs it or
declines on it.

This is the same cliff 1.1 of the milestone plan found in Spark's default
configuration, which sets its own limit to 65535 while HotSpot stops at 8000.
Varka had it too.

### 2.4 Why the epilogue was left unbounded, and when it costs

The decision is written down beside the code (`VarkaBodyEmitter`, the `EPILOGUE`
arm): "One method for every output, not one per group: the epilogue runs a
single pass per batch, so `GROUP_BUDGET` - which exists to keep a *hot* method's
C2 compile cheap - has nothing to bound here." The reasoning is about compile
*cost*; the rule that bites is about bytecode *size*. A method that runs once
per batch still has to be compiled to be fast, and above 8000 bytes it never is.

When it costs is decided by the first lines of `emitEpilogue`: if the batch
divides evenly by the lane count it returns before any vector work - "the common
case, since the default `COLUMN_BATCH_SIZE` is 4096". Otherwise it runs **one
full lane group's body under a mask**: at 16 outputs, 918 `IntVector`
operations, interpreted, where the Vector API does not intrinsify and every
operation boxes.

So the cliff is expected to be invisible on round batches and severe on ragged
ones - and ragged batches are ordinary: the last batch of every partition, and
every batch `VarkaFilterExec` produces, since it compacts the selected rows into
batches of whatever length the predicate leaves. A projection over a Varka
filter meets a tail on nearly every batch. Section 6 is designed around exactly
this.

### 2.5 One figure in `VarkaEmitBudget`'s javadoc does not hold for this family

`GROUP_BUDGET`'s javadoc says "Past roughly 1900 bytes C1 refuses a loop method
... ('out of virtual registers in LIR')". Here C1 compiled `loopDense0` to
`loopDense2` at 3294 to 3964 bytes, at tier 3, without refusing. The refusal it
describes is a register-pressure limit, which correlates with bytes on the
family it was measured on and not on this one. It is recorded here rather than
acted on: it is one more place a byte figure stood in for something else.

*Correction, 23 September 2026, from 2.6.5 below: this section is wrong. The
tier-3 lines it read were C1's attempts; the same log, read without truncating
the line, follows each of the 3294- to 3964-byte loop methods with `COMPILE
SKIPPED: out of virtual registers in LIR generator`, while the 1536-byte one
compiles. The javadoc's figure holds for this family as well.*

**What the check would have rejected.** The premise that this task fixes a rare
67KB failure reached only by fuzzing volume. It fixes a common, silent
interpretation cliff at 13 outputs, and the rare failure is its far end.

### 2.6 The open questions, answered before any code

Reviewing this plan raised nine questions; the ones that could change the
design were answered from the tools and the JVM before a line of emitter code.
Three of the answers change 3.1, and one of them corrects 2.5.

**2.6.1 Nothing Varka ships crosses 8000 bytes today.** Every shape the bytes
oracle pins, emitted at both widths with each method's code length read through
`VarkaEmitterTestSupport.codeSize`:

| corpus | shapes | largest loop method | largest epilogue | any method over 8000 |
| :--- | ---: | ---: | ---: | ---: |
| the coverage table | 92 | 1288 | 1321 | 0 |
| the int fuzz sequence | 10000 | 5021 | 5731 | 0 |
| the long fuzz sequence | 10000 | 1486 | 1549 | 0 |

So there is no shipped shape running interpreted, and no single output the
regroup could not split. It also means **the fuzzer cannot find this cliff**:
its grammar draws kernels of one to three roots and never approaches the size
where it happens, which is why the 67KB tree took 35 million iterations. The
cliff is reached by many heavy outputs in one projection, which is a query
shape rather than a fuzz shape - task 172's realistic query is where it lives,
and task 179's standing fuzz job needs a wide mode to see it at all.

**2.6.2 Every group method sets up every output, so splitting a group cannot
shrink it.** `javap` of the 16- and 60-output classes: `loopMasked0` holds the
same five outputs and the same 313 `IntVector` calls in both, and at 60 it has
exactly 88 more `laload` and 88 more `invokestatic` (two
`VarkaVectorSupport.ofAddress` calls for each of 44 more outputs), 44 more
`iaload`/`istore` pairs (their literals), and 144 `aload_w` and 77 `astore_w`
that were not there before - past 255 local slots every load and store takes a
`wide` prefix. The source is `VarkaBodyEmitter`'s prologue,
`for (int o = 0; o < numOutputs; o++)`, run in every body mode: the driver, each
loop group and the epilogue materialize the destination segments of every
output in the kernel. That is the growth 2.2 saw with the op count fixed, and it
means a group's size has a term that scales with the whole kernel. **3.1's
regroup step would not converge** on a large kernel without first making each
group set up only what it writes.

**2.6.3 The early return has to stay in the epilogue.** At four outputs over
1024-row batches - sixteen lanes, so no batch ever has a tail - the epilogue was
compiled at tier 4 (2.3). It warmed up entirely on calls that returned before
any vector work. Moving the return into the driver, as 3.1 proposed, would make
it warm only on ragged batches, and a cached scan has one of those per
partition; the method would stay interpreted exactly where it is rarely needed
and then, when needed, be slow.

**2.6.4 When a kernel sees a ragged batch.** Both
`spark.sql.inMemoryColumnarStorage.batchSize` and the Arrow cache's
`spark.sql.execution.arrow.maxRecordsPerBatch` default to 10000, which divides
by every lane count from two to sixteen, so a cached scan is ragged once per
partition. After a Varka filter it is the reverse: `VarkaFilterExec` compacts
the selected rows into a fresh batch, so a Varka projection stacked on it sees a
tail on nearly every batch. `emitEpilogue`'s comment gives the default as 4096;
the number is wrong and the conclusion survives it, and the code task corrects
the comment.

**2.6.5 There are three thresholds, not one.** Read from `-XX:+PrintCompilation`
in full rather than field by field:

| method size | what the JIT does |
| :--- | :--- |
| up to about 1900 bytes | C1 compiles it, C2 later |
| about 1900 to 8000 | C1 refuses ("out of virtual registers in LIR") and it is interpreted until C2 lands |
| over 8000 | never compiled, at any tier |

The middle band is harmless for a loop method, whose backedges bring C2 in
quickly, and not for an epilogue, which has no loop and reaches C2 only by
invocation count - `Tier4InvocationThreshold` is 5000 on this JDK.

**2.6.6 How big the cliff is.** A scratch probe of the dump tool - one run in
one fork, run long enough that the timed half comes after tiering, and *not a
result*: the benchmark of section 6 commits the numbers - put a ragged batch at
about three times the cost of an even one at 16 outputs, permanently, and within
a percent of it at 12 once C2 had landed; at 12 the same probe, stopped earlier,
read about twice, which is the warmup of 2.6.5. At four outputs the tail cost a
few percent. So the cliff is real and permanent above 8000, and prediction 1's
"more than an order of magnitude" is likely too strong; it stays as registered
and the benchmark scores it.

**2.6.7 to 2.6.9.** A method's code length is readable today:
`VarkaEmitterTestSupport.codeSize` parses the class through the Class-File API,
so 3.1's measurement has a tested reader to follow. Bytecode size is the same at
every width to within a byte (2.6.1's two widths), so the cliff is
width-independent even though its cost is not. And OpenJ9, Graal and a user
running `-XX:-DontCompileHugeMethods` behave differently; that is a sentence for
`docs/sql-varka.md`, not a design input.

**What the dump tool gained.** `VarkaEmitDump` takes `--rows N` (1024 by
default) and reports the time of the second half of its rounds, which is what
2.6.6 and the ragged-batch half of 2.6.3 needed.

## 3. The design

### 3.1 The budget counts the method it emitted

`VarkaEmitBudget` keeps weight for what weight is good at and gains a measured
check for what it is not. The unit that decides whether a method is compiled is
the length of its code attribute, and the Class-File API knows it the moment the
method is built - so the budget reads it rather than estimating it.

1. **The epilogue is partitioned by the same groups as the loop.** The
   `EPILOGUE` arm stops emitting one method for every output and emits
   `epilogueDense<g>` and `epilogueMasked<g>` per group, with the `DRIVER` arm
   calling each after the loops, exactly as it calls `loopDense<g>`. The early
   return of 2.4 moves into the driver, so an even batch still costs one compare
   and no calls. This alone bounds the epilogue by what already bounds the
   loops, which is 2.2's third column: every loop method on the ladder is under
   5400 bytes.
2. **Every emitted method is measured against the limits.** After the class is
   built, `VarkaEmitBudget` reads each method's code length and the class's
   constant pool size and checks them against named limits: 8000 bytes per
   method (`DontCompileHugeMethods`), 65535 bytes as the class-file cap, 65535
   constant-pool entries, 255 parameter slots. A method over 8000 bytes is
   regrouped - the group split and the class emitted again - and a shape that
   still exceeds a limit after its groups are single outputs is **declined with
   a reason** naming the limit and the method, never thrown.
3. **Weight keeps grouping; bytes keep safety.** `GROUP_BUDGET` exists for C2's
   node and inlining budgets, which are about op count, and its argument in the
   javadoc stands. What it cannot do is bound size, and after this task it is
   not asked to. That also settles what task 148's under-count *is*: a
   grouping-balance defect, not a size risk, and 148 is done in its own row
   against that reading.

All of it is behind a `VarkaEmitOptions` switch, `methodByteBudget` (0 is
today's form, kept as the live reference under every test), and the default
flips in the last commit on 6.1's rule.

*Corrections, 23 September 2026, from 2.6. Step 1's early return does **not**
move into the driver: it stays inside each `epilogue<g>`, which is how the
epilogue warms up (2.6.3). A step comes **before** step 1: each group's methods
materialize only the destination segments and literals of the outputs that
group writes, so a group's size stops carrying a term that scales with the
whole kernel (2.6.2). Without it, step 2's regroup cannot converge; with it, the
split is what makes a method smaller. That step moves the bytes of every
multi-group kernel, so it is measured on its own before the epilogue changes.
And the warmup of 2.6.5 is a separate question from the 8000-byte limit: below
it an epilogue still waits for C2 by invocation count. Whether to fold the
masked tail into the loop method - which has backedges - or accept the warmup is
decided by 6's measurement rather than here.*

### 3.2 What is deliberately unchanged

* `MAX_CHAIN_DEPTH` and `MAX_FUSED_NODES` stay IR-level declines in `Analysis`;
  they bound the tree, not the class.
* `FUSED_CEILING` and the prefix-sharing rule in `groupOutputs` stay as they
  are; the byte check can only split a group they formed, never merge one.
* The typed decline reasons across the whole emitter are task 169's; this task
  adds the ones its limits need and asserts that none of them throws.
* Whether the method limit should be lower than 8000 is task 170's A/B. This
  task takes 8000 because it is the number the JVM's output above shows to
  matter.
* The vanilla arm and the published ladder are task 171's; 6 builds the file it
  extends.

### 3.3 Registered op counts

Partitioning moves operations between methods and must not add any. Per shape,
the sum of `IntVector` invocations over the epilogue methods equals today's
single epilogue: **258 at 4 outputs, 698 at 12, 918 at 16, 3338 at 60** (2.2),
and the loop methods' counts are unchanged. Under the switch, every method of
every rung reads at most 8000 bytes in `dev/varka_emit.sh`. Asserted by 5.

## 4. Files

| file | what |
|---|---|
| `VarkaEmitBudget.java` | the limits, the per-method measurement, the regroup and the decline reasons |
| `VarkaBodyEmitter.java` | `EPILOGUE` per group, `DRIVER` calling each epilogue and holding the even-batch return |
| `VarkaLoopEmitter.java` | emitting `epilogueDense<g>` / `epilogueMasked<g>`, and re-emitting on a regroup |
| `VarkaEmitOptions.java` | `methodByteBudget`, in `canonical()` so the shape key sees it |
| `VarkaEmitterSuite` | the ladder, the op-count and size assertions, the decline |
| `VarkaEmittedBytesSuite`, `emitted_bytes.json` | unmoved under defaults; one arm for the switch (task 167's rule) |
| a new `VarkaMethodSizeBenchmark` and its results files | 6; task 171 adds the vanilla arm to it |
| `PLAN_MILESTONE_6.md` | rows 87 and 168 point here |

## 5. Tests, and what each is for

* **The ladder under the switch**, 4 to 60 outputs at 512 and 128 bits: every
  emitted method at most 8000 bytes, the `IntVector` sums of 3.3 unchanged, and
  answers equal to `VarkaReferenceEvaluator` at lengths 1024, 1031 and 7 - an
  even batch, a ragged one, and one shorter than a lane group.
* **The JVM compiles every method.** A forked probe in the style of
  `VarkaAssemblySuite` runs the 16-output shape under `-XX:+PrintCompilation`
  and asserts every `loop` and `epilogue` method reaches tier 4 under the
  switch - the property this task exists for, asserted from the JVM rather than
  inferred from a size. Under the legacy form it asserts the opposite, which is
  2.3 pinned.
* **A shape at each limit declines with its reason and does not throw**,
  including one that reaches the class-file cap. That shape is found and pinned
  as an expression, not a fuzz coordinate (2.1).
* **The fuzz suite toggles every `with*` on `VarkaEmitOptions`**, so the switch
  is fuzzed the day it lands at no extra cost.
* `emitted_bytes.json` unmoved under defaults until the flip; at the flip, the
  moved rows are exactly the shapes whose epilogue exceeded 8000 bytes,
  explained shape by shape.

## 6. The measurement

`VarkaMethodSizeBenchmark`, its own file per the project's rule for a new
family. The ladder of 2.2 at 4, 8, 12, 13, 14, 16, 32 and 60 outputs, both forms
by explicit label (`single epilogue`, `epilogue per group`), at batch lengths
**1024 and 1031**, at 512 and 128 bits, regenerated with
`dev/varka_bench_regen.sh` and banded before anything is read. The control row
is the 4-output shape, whose epilogue compiles under both forms. **Committed as
its own pull request before the change**, so the baseline exists before the
improvement (the project's rule for a benchmark that does not exist yet).

### 6.1 Predictions, registered before the run

1. **On ragged batches the single epilogue falls off a cliff between 12 and 14
   outputs**, masked first: per-row cost at 1031 rows steps up at 13 and again
   at 14 by more than an order of magnitude against the 12-output rung, because
   a full lane group's body runs interpreted per batch.
2. **On even batches there is no cliff.** At 1024 rows the single epilogue
   returns before any vector work, so the ladder stays smooth through 14 and
   beyond and the two forms read within band of each other. If this fails, the
   early return is not doing what 2.4 says, which is a finding of its own.
3. **The per-group epilogue removes the cliff and costs nothing measurable.** At
   every rung and both lengths it is within band of the single epilogue where
   the single one compiles (4 to 12 outputs), and far faster where it does not.
4. **The loops do not move.** Loop methods are unchanged by the switch, so on
   even batches every rung's rate is within band under both forms.

5. **Below 8000 bytes, the ragged cost is warmup only.** At 12 outputs the
   ragged penalty is visible in the first thousands of batches and gone once
   C2 compiles the epilogue; above 8000 it never goes. *Added 23 September 2026
   after 2.6.6, and so not registered before a run in the strict sense: a probe
   had already read it. It is kept apart from 1 to 4 for that reason.*

*Note on prediction 1, 23 September 2026: 2.6.6's probe suggests about three
times per batch rather than more than an order of magnitude. The prediction
stands as registered and the benchmark scores it.*

**The rule for the default:** flip when the per-group form is nowhere worse than
the single form beyond its band, at any rung, length or width.

## 7. Risks

1. **Regrouping re-emits the class**, so a shape over budget costs two
   emissions. `VarkaEmissionBenchmark` prices it; the class cache means it is
   paid once per shape per JVM, but a pathological shape could regroup several
   times, which the decline bounds.
2. **Splitting the epilogue forfeits the tail's cross-output sharing** - the
   same trade the loop made, on a path that runs at most once per batch.
   Prediction 3 is where it would show.
3. **The interpreted cost may be smaller than predicted** if the tail's work is
   dominated by something other than the vector operations. Then the cliff is
   real and cheap, and the milestone's post must say so rather than lean on it.
4. **The constant pool is not reached by this family.** Its check needs a
   synthetic shape to be tested at all, and the task says so rather than
   claiming coverage it lacks.
5. **Measuring code length needs the method built.** If the Class-File API does
   not expose it without a full class build, the measurement moves after
   `ClassFile.build`, which is 3.1's step 2 as written.

## 8. Sequencing

Each commit green on its own.

1. This plan, and rows 87 and 168 marked Planned.
2. `VarkaMethodSizeBenchmark`, legacy form only, committed with its band - its
   own pull request.
3. The measurement in `VarkaEmitBudget` and `dev/varka_emit.sh` reporting the
   constant pool beside bytes. No behaviour change; the oracle unmoved.
3a. Each group's methods materialize only their own outputs' segments and
   literals (2.6.2), behind the switch, with its own byte ladder: the first
   change that makes a group method smaller when it is split.
4. The per-group epilogue behind `methodByteBudget`, with 5's tests and the JIT
   probe.
5. The regroup and the declines, with the shape that reaches the class-file cap.
6. The benchmark regenerated with both forms, 6.1 scored, the default flipped
   and the oracle regenerated with its movement explained.

## 9. Outcome

*Written when the measurement lands.*
