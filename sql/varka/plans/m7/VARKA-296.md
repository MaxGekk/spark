# VARKA-296: Answers must not depend on which outputs fuse

## 1. Where this came from

Row 296 of `m7/PLAN.md`, left by VARKA-262 (3: "each fused output forced to decline in turn,
the answer unchanged") and `m7/READING.md` 1. Comet's `CometFallbackInvarianceSuite` forces one
expression back to Spark at a time through `spark.comet.expression.<Name>.enabled`, requires the
same outcome on both legs - the rows, or the same error class - and counts a comparison only when
the executed plan proves the flip moved execution (otherwise `FAIL-BIND`, never a pass).

Varka makes the same promise from the other side. A projection is served by three mechanisms at
once: the fused kernel, input vectors forwarded zero-copy, and a residual that Spark's row engine
computes beside the kernel in one per-row pass (`VarkaOutputSpec`: `FusedOutput`, `KernelOutput`,
`ForwardedOutput`, `ResidualOutput`). The compiler decides per entry which one, and "granular
degradation" (`VISION.md` 2.2) says that declining one entry never changes another's answer. Nothing
tests that directly. The random differentials compare Varka with Spark over whatever split the
compiler happened to choose, and the compiler has no way to choose another.

## 2. The admission check, done

Two questions decide whether the row is worth building: can a decline be forced through the
compiler's own decline path, and would the checks it enables find anything the existing ones do not.
Checked on 9 October 2026 on `master` (`e15b56c3050`) with a throwaway hook in
`VarkaExpressionCompiler.classifyOnce` (never committed): a position forced to decline *after* the
entry compiled, through the branch an over-budget entry takes, and five planted faults in that
branch's rollback, one at a time. Two throwaway probes ran it: a compiler-level one (random
projections of the coverage table's rows, 3,000 at each of three widths of up to 12, 40 and 120
entries, each with up to three fused entries forced in turn) and an end-to-end one (the compositions
of `VarkaSparkFuzzSuite`, seed 20261008, 1,500 of them, every output forced in turn).

**The hook works, and the baseline is clean.** Of 12,201 forced declines at the compiler level, every
one changed the plan, and in none did the projection with the entry forced differ from the
projection with the entry deleted (`fused`, `more`, `specs` without the entry, and `declines` shifted
past it): 0 mismatches. End to end, of 5,760 forced outputs 3,019 (52%) raised the executed plan's
`numResidualEntries` by exactly one and were compared with the row engine's answer: 0 disagreements.
The rest were outputs already residual, forwarded, or in a plan with no Varka project node, and
are not counted. The 3,019 covered date (1,226), int (1,123), bigint (181), time(6) (163),
year-to-month (220), month (52), day-to-second (26) and hour-to-second (28) outputs.

**Natural declines do not reach the branch a forced one does.** Across the 9,000 compiler-level
compositions the compiler declined 21,726 entries by itself, and every one was the one-lane rule
(a long-lane output beside int-lane ones); none was a compiled entry over the budgets, none an
entry that failed to compile, none an entry demoted by the size admission. So the code that rolls the
shared tables back for those cases (`classifyOnce`'s last branch, which truncates `inputs`,
`literals`, the long table and the bounds) is reached by this harness only when a decline is
forced. The end-to-end harness has the same shape: in the 1,500 unforced compositions the projection
classifier declined entries 11,433 times (counted per classification, and it classifies a plan
several times), every one by the lane rule, and none through the compiled-entry branch.

**Planted faults.** Five faults in that rollback, each a plausible mistake in a table added later,
and which check sees each (counts are out of the forced declines of the three widths, 12,201, or of
the 1,500 end-to-end compositions):

| planted fault | what it does to answers | existing composition fuzzer | existing Spark differential | structural check | answer check |
|---|---|---|---|---|---|
| a declined entry's input stays in the table | nothing: a wasted input, later indices shifted | caught in its wide test, by accident, as an input-count overflow (200 compositions) | 0 of 1,500 | 3,740 of 12,201 | 0 |
| a declined entry's bound stays | nothing: extra batch declines | 0 of 3,000 | 0 of 1,500 | 21 of 12,201 | 0 |
| a declined entry's int literals stay | none: `CompiledVarkaProjection` refuses a kernel with both literal tables | 0 of 3,000 | 0 of 1,500 | raised at the first forced decline | raised at the first forced decline |
| a declined entry's long literals stay | the same | 0 of 3,000 | 0 of 1,500 | the same | the same |
| inputs truncated one too far | wrong values or errors | caught in its wide test, by accident, as a column-ordinal error (200) | 0 of 1,500 | 2,815 of 12,201 | 283 of 2,614 applied |

What the table says. The answer check alone is blind to the two faults that waste work without
changing an answer; the structural check sees them, by comparing the compiled tables with those of
the projection the entry was never in. The one fault that changes answers is invisible to the
existing Spark differential in 1,500 compositions and is found 283 times by forcing, first at
iteration 82. And the existing composition fuzzer sees two of the five, in a test that was written
for something else and only through an exception.

**Cost.** The compiler-level probe took 2 to 7 seconds for 3,000 compositions; the end-to-end one
178 seconds for 1,500 with up to four forced runs each, 0.12 s per composition against 0.08 s for the
unforced differential (`VARKA-297.md` 9: 5,000 in 7 minutes).

What the check would have rejected: forcing through the `demoted` map the size admission already
has, because an entry demoted there is compiled not at all, so the rollback under test never runs;
an answer check alone, for the reason above; and relying on natural declines, which never reach the
branch.

## 3. The design

### 3.1 The hook

`VarkaEmitOptions.forceResidualAt`, an int: 0 is off, `k + 1` forces the projection's entry at
position `k` to decline. It is a record component with its builder setter, `with*` method, javadoc,
default and one entry in `VarkaEmitOption.TABLE` (a `Count`, reason `KNOB`, rendered as a count
tag so a forced variant rides the shape key, no fuzz draws and no inventory arms, since it changes
no emitted byte). It reaches the compiler by the one derivation every caller uses,
`VarkaColumnarToRowExec.emitOptions`, which the rule, the nodes and the evaluators all ask - the
requirement `compilePartial`'s own doc states, that the plan and the task classify alike - and tests
set it through `setEmitOptionsForTesting`, restored in a `finally` like the other hooks there. It is
an option and not a `SQLConf` entry for the reason that hook gives: the production configuration
surface stays free of emitter knobs.

A second option, `misdescribeRollback`, is the fault injector the structural check's own test needs,
as `misdescribeAdd` and `misdescribeDriverBytes` are for theirs (a `Count`, reason `FAULT_INJECTOR`,
0 in production, never drawn by the fuzzers): 1 keeps a declined entry's inputs, 2 keeps its bounds,
3 truncates the inputs one too far. It lives in the same branch as the hook.

In `classifyOnce`, a compiled entry at the forced position that would have been accepted takes the
branch of an over-budget entry: the four tables rolled back to their marks, a decline reason
("forced residual: forceResidualAt"), and not recorded as an entry another kernel could take (the
`alone` set). Deliberately the existing path and not a new one: a forced decline with a path of its
own would test the path of its own.

### 3.2 The structural check

A decline must be the entry never having been seen. In `catalyst`, a suite draws projections as
`VarkaCoverageCompositionFuzzSuite` does (the coverage table's rows, the same seed and
`-Dvarka.fuzz.compositions`, a default of a few hundred), compiles each, picks up to three entries
that fused, and for each compiles twice more: with that position forced, and with the entry removed
from the list. It asserts that the fused sub-projection (outputs, types, input ordinals, literals,
bounds, derived inputs, long literals), the further kernels, the specs without the forced position and
the declines shifted past it are equal. No Spark session is involved.

A failure goes through the existing composition shrinker (ddmin over the entries) with a signature
that names what differed ("forced decline differs from deletion on [inputOrdinals]"), and
`VarkaKnownFailures` gets a `forced` lane.

### 3.3 The answer check

In `VarkaSparkDifferential`, after a composition's unforced comparison, the case's output `k` is
forced and the Varka query run again with the option set. The comparison counts only if the executed
plan's Varka nodes report exactly one more `numResidualEntries` than the unforced run (the plan
proves the flip moved execution; Comet's `FAIL-BIND` rule), and then the outcome - rows sorted, or
the error class - must equal the row engine's. An unchanged plan is tallied, never passed, and the
suite requires a floor on the applied share so that a hook that silently stopped working fails it.
PR CI forces one output per composition, chosen by the seed; the nightly forces every output
(`-Dvarka.sparkfuzz.forceAll=true`). A disagreement is shrunk by the existing shrinker with kind
`forced output k: ...`, and the reproducer file gains a `-- force: k` line, as it has `-- pivot:`,
so the replay forces it too.

### 3.4 What is deliberately unchanged

Production behaviour (the default is 0); the coverage table, the emitter and the kernels; the
pivot and the ternary partition, which run on the unforced query; the row fallback's own code
(`VarkaRowToColumn`, VARKA-299). Two neighbours are not this row's. A *runtime* decline of one batch
is forced by the hooks that exist (`setFailEmissionForTesting` and the size-ladder's), and
`compilePredicate` classifies a filter's conjuncts into a fused mask kernel and residual ones with the
same hand-rolled rollback ("mirroring `compilePartial`'s per-entry eligibility including the table
rollback"): the invariance holds there too, and the hook this row builds is what a follow-up row
would use, so it is a follow-up and not scope creep.

### 3.5 Registered op counts

None move: the option defaults to 0 and no emitted byte depends on it. `emitted_bytes.json` and
`coverage.json` are byte-identical.

## 4. Files

| file | what |
|---|---|
| `.../codegen/varka/VarkaEmitOptions.java`, `VarkaEmitOption.java` | the two options and their table entries |
| `.../codegen/VarkaExpressionCompiler.scala` | the forced branch in `classifyOnce` |
| `.../codegen/varka/VarkaForcedDeclineSuite.scala` | the structural check and its planted-fault test |
| `.../codegen/varka/VarkaKnownFailures.scala` | the `forced` lane |
| `sql/core/.../execution/VarkaSparkDifferential.scala`, `VarkaSparkFuzz.scala` | the answer check, the force in a reproducer |
| `VarkaSparkFuzzSuite.scala`, `VarkaSparkReproducerSuite.scala` | the arm, the replay |
| `dev/varka_nightly.sh` | `forceAll` for the nightly step |
| `sql/varka/skills/testing-and-debugging.md` | a lesson: a decline is the entry deleted |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-296.md` | the row and this plan |

## 5. Tests, and what each is for

| test | what it catches that no other would |
|---|---|
| the structural check at a fixed seed | a rollback that leaves a table different from the entry's never having been seen: a stale input, bound, literal or derived input |
| a planted-fault test: with `misdescribeRollback` set to 1, 2 and 3, the check reports each and the shrinker reduces it to the declined entry and the entry that follows | the check being blind; three of the five faults of 2 are the cases (the literal faults are caught by `CompiledVarkaProjection`'s own `require`) |
| the answer check at a fixed seed | a seam bug between the kernel's columns and the residual's: output numbering across fused, kernel, forwarded and residual, the residual writer's handling of a type that never declines today, ownership of the merged batch |
| the hook moves the plan in at least a third of forced runs | the hook, or the position mapping, silently doing nothing (Comet's `FAIL-BIND`) |
| a reproducer with `-- force:` parses back to what was rendered and replays forced | the text form drifting from the parser |
| `VarkaEmitOptionSuite`, the shape-key and bytes suites | the new option leaving the defaults' bytes or key unchanged |

Both widths for the answer check (`-XX:MaxVectorSize` 16 and 64 in the gate); the structural check
has no vector code.

## 6. The measurement

Not a timing: the yield and the cost, recorded in 9. The yield is the structural check's mismatches
over a nightly's compositions and the answer check's disagreements over 5,000; the cost is the
seconds of the PR-CI arm and of the nightly step before and after.

### 6.1 Predictions, registered before the build

1. **The structural check finds nothing on `master`** over 100,000 compositions: the rollback is exact
   today (12,201 forced declines found no difference). A mismatch would be a finding, recorded as a
   row.
2. **The answer check finds nothing on `master`** over 5,000 compositions (3,019 applied forced runs
   found nothing in 1,500), and about half of the forced outputs change the plan.
3. **The planted over-truncation is found within 100 compositions** by the answer check (iteration 82
   in 2) and within three by the structural one.
4. **The PR-CI arm, forcing one output per composition, adds under 50% to the 200-composition run's
   ten seconds**, and the nightly's step, forcing every output of 50,000 compositions, takes under
   half as long again as it does now.

## 7. Risks

1. **The forced path is not the natural one.** It is the over-budget branch by construction, and 2
   shows natural declines never take it; a bug specific to the one-lane branch (its own copy of the
   rollback) is not exercised by forcing. The structural check's planted-fault test can plant in that
   branch too; reading the two branches together, there are four copies of one rollback, and a
   follow-up could make them one helper so that one test covers all.
2. **Positions drift.** The optimizer can drop or reorder project-list entries, so the case's output
   `k` is not always position `k`; the `numResidualEntries` gate counts only a run whose plan changed
   by exactly one.
3. **Most forced runs change nothing** (48% in 2): outputs already residual or forwarded. The
   floor on the applied share is the guard, and PR CI's one forced output per composition should pick
   among the outputs the unforced plan fused, which the unforced run's metric does not say; the draw
   forces a seeded position and accepts the loss.
4. **A residual entry changes the order errors are raised in** under ANSI, since the row engine
   computes the residual entries in a pass of their own. Both engines raising passes (the error
   class is compared), and the comparison is of one error class, not which row raised it.
5. **Time.** The nightly's Spark step grows by the share 6.1 predicts; it runs under
   `dev/varka_deadline.sh` like the others.

## 8. Sequencing

1. This plan, with row 296 marked Planned.
2. The option, the compiler's forced branch and the structural check with its planted-fault test.
3. The answer check, the shrinker's kind, the reproducer line and the replay.
4. The nightly step, the lesson, and section 9.

## 9. Outcome

To be written when the measurement lands.
