# VARKA-290: The size loop restructured

## 1. Where this came from

Row 290 of `m7/PLAN.md`, scope item 74.2, added by the owner on 6 October 2026: VARKA-236 kept
the size loop as the plan's last resort, so the loop stays, and it is the hardest method in the
emitter to read. The private `VarkaLoopEmitter.emit` is 282 lines at `fd52b033767` (310 to 591).
After the argument checks and a comment of 64 lines, one `while (true)` carries ten locals across
its passes, and each of its three resets clears a different subset of them. The fallback order -
roll back the call-site splits, drop the exact grouping, drop the prediction, decline - is the
order of three `if` blocks at the bottom of the loop, and the comment states it in two further
places.

## 2. The admission check, done

What has to be true for a refactor of this loop to be checkable: an oracle that sees every
decision the loop makes, not only the bytes it ends with. `emitted_bytes.json` sees the bytes
of the shapes it pins, and two orders of the same decisions can end in the same bytes. So the
check is a dump of every decision over a fixed corpus, taken on the base, and diffed after.

The dump is an opt-in test in `VarkaIrFuzzSuite`, `VARKA_SIZE_TRACE_DUMP=<file>`, like the
coverage suite's fusion dump (VARKA-300). One line per emission: the outcome (the class's
SHA-256, or the decline's reason, outputs and planned cut), all nine `VarkaEmitTrace` counters,
and the plan's corrections by name. The corpus:

* the first 1500 drawn shapes of each lane, under their drawn options, which draw the small
  byte budgets one time in five;
* 24 wide compositions of at least 250 outputs, under the six wide variants and seven planned
  ones: the split driver off, the driver misread by 4000 bytes, a 2000-byte budget, the call
  sites split, the exact grouping off, the prediction off, and the split driver off with the
  driver misread.

Taken at `fd52b033767`, twice, byte-identical: 3,312 emissions, 3,100 classes and 212 declines.

| what the loop did | emissions |
|---|---:|
| built the class more than once | 168 |
| declined before the build, on the plan | 34 |
| split the driver into stages before the build | 130 |
| halved a group on bytes | 33 |
| split a group on call sites | 27 |
| rolled the call-site splits back | 25 |
| resized the stages after a build | 48 |
| dropped the exact grouping | 86 |
| dropped the prediction | 38 |
| corrected the plan | 30 |
| declined after the build with a planned cut | 14 |
| declined with outputs no regroup shrinks | 92 |

Every counter is reached. Two branches are not: the class-file cap on a method's code
(VARKA-219), whose refusal is read as the measurement, and the constant pool's cap. Neither has
a fuzzed shape heavy enough. `VarkaEmitterBudgetSuite` tests both, the refusal with the night
fuzz run's nested `make_date` and the pool with a 70,000-entry count, and it is run on its own
after the restructure as well as in the gate.

What the check would have rejected: a restructure proven by `emitted_bytes.json` alone, which
pins none of the wide compositions and none of the planned variants above.

## 3. The design

### 3.1 The loop as a state object and named steps

The state the loop keeps across passes, and what each reset does to it at `fd52b033767`:

| reset | `grouping` | `siteBudget` | `forcedStarts` | `stageGroups` | `afresh` | `readGroups`, `readWidest` |
|---|---|---|---|---|---|---|
| roll back the call-site splits | the current grouping without the site budget | 0 | cleared | 0 | true | kept |
| drop the exact grouping | the caller's options without it | the caller's | cleared | 0 | true | kept |
| drop the prediction | the caller's options without it or the exact grouping | the caller's | cleared | 0 | true | kept |

The asymmetries are the behaviour: a rollback starts from the current grouping, a fallback from
the caller's options, and a fallback puts the call-site budget back, so a later pass may split on
sites and roll back again. The plan's reading survives every reset.

* **`SizeLoop`**, a private static final class, holds one emission's state: the inputs that do
  not change (the class's description, the analysis, the options, the trace, the budget) and the
  ten locals above. It is a class rather than a record because `forcedStarts`, `exactRuns` and
  the prediction tallies change in place between builds. `emit` checks its arguments, analyzes,
  and runs it.
* **The pass as steps**: group the outputs; plan the driver (`plan`, which may decline before
  any build); build and measure (`build`, returning the bytes, the measurement and the limit it
  is judged against, which a refusal changes to the cap); split the groups over a limit
  (`split`); resize the stages (`resizeStages`); and fall back (`fallBack`).
* **The fallback order stated once**: `fallBack` is the four steps in their order, each a few
  lines, and returns false where only the decline is left. The three resets become one shared
  `startAfresh` and the two changes of grouping. The comment in `emit` points at `fallBack` for
  the order instead of restating it.

### 3.2 What is deliberately unchanged

Every helper the loop calls (`groupOutputs`, `driverAlone`, `stageSize`, `halveGroups`,
`noteCorrections`, `plannedCutOf`, the measurements in `VarkaEmitBudget`), the options, the
trace, the decline messages, and the order of everything: the same builds with the same
groupings. The comment's paragraphs keep what they say. Rewriting comments that narrate history
is row 252's.

### 3.3 Registered op counts

None move: no emitted byte changes.

## 4. Files

| file | what |
|---|---|
| `VarkaLoopEmitter.java` | `emit`'s loop as `SizeLoop` |
| `VarkaEmitTrace.java` | its pointer to the loop |
| `VarkaIrFuzzSuite.scala` | the dump, and the wide compositions' draws as a helper it shares |

## 5. Tests, and what each is for

* **The dump of 2**, on the branch, byte-identical to the base's 3,312 lines: every outcome,
  counter and correction over the corpus.
* **`emitted_bytes.json` unedited**, `VarkaEmittedBytesSuite` passing.
* **`VarkaEmitterBudgetSuite`** on its own: the cap refusal and the pool decline the dump does
  not reach.
* **`VarkaKernelPlanSuite`**, **`VarkaEmitterSplitDriverSuite`** and the fuzz suite's wide test,
  which assert on the trace and the stages.
* **The gate**, both widths.

## 6. The measurement

None. The same builds run in the same order with one object more per emission, never per batch,
so `VarkaCompileBenchmark` is not rerun.

### 6.1 Predictions, registered before the run

1. The dump is byte-identical to the base's.
2. `emit` and `SizeLoop` together are longer than the 282 lines they replace, by under a fifth:
   the steps' signatures and doc comments cost more than the shared resets save.

## 7. Risks

1. **A reset merged.** One `reset()` for all three would make a fallback keep the rolled-back
   site budget, or a rollback restart from the caller's options. The dump's 25 rollbacks and 124
   fallbacks would show it.
2. **The build counter read out of order.** Whether a build is the plan's, so a reaction is a
   correction, reads `trace.builds == 1` after the increment. The 30 named corrections would show
   it.
3. **The exact grouping priced again each pass.** `ExactRuns` caches on the outputs' identity,
   so the loop must pass the one copied list. Not a wrong answer, a slower compile; checked by
   reading.

## 8. Sequencing

1. This plan; row 290 marked Planned.
2. The restructure and the dump, in one commit.

## 9. Outcome

Done on 11 October 2026, as 3.1 describes: `emit` checks its arguments, analyzes, and runs a
`SizeLoop`, whose pass is six steps (`group`, `plan`, `buildAndMeasure`, `split`,
`resizeStages`, `fallBack`), and whose resets are one `startAfresh` beside the changes of grouping
in `fallBack`.

* **Every decision is unchanged.** The dump of 2 on the branch is byte-identical to the base's:
  3,312 lines, every outcome, counter and correction.
* **`emitted_bytes.json` is unchanged**, and `VarkaEmittedBytesSuite` passes in the gate.
* **The branches the dump does not reach** pass on their own: `VarkaEmitterBudgetSuite`, with the
  cap refusal and the pool decline, and `VarkaKernelPlanSuite` and
  `VarkaEmitterSplitDriverSuite`, 66 tests.
* **The gate passes**: every Varka suite at both widths, and lint.
* **The fallback order is stated once**, in `fallBack`'s doc and body; the note in `emit` points
  at it.

**Predictions scored (6.1).**

1. **Held.**
2. **Missed.** The 282 lines are 389, 1.38 times, not under 1.2. The code is 215 lines against
   161; the rest is the steps' doc comments, which say what the one comment said about each
   branch, beside the branch. Most of the added code is the state's fields and constructor, 45
   lines where the locals were declared in 22.

**The dump is kept**, as an opt-in test in `VarkaIrFuzzSuite` beside the coverage suite's fusion
dump, for the next change to the size loop to diff.

**Review of #715 (`/code-review high`), 11 October 2026.** No behaviour change in the
restructure. One older bug, fixed here because `SizeLoop` now holds the state it needed:

* **Whether a build is the plan's read the caller's trace.** `planned && trace.builds == 1`
  holds only for the first emission of a trace, and the fuzzers add up one trace over a run, so
  every correction after a run's first went unnamed. No answer or byte depends on it: production
  and the cost audit pass a fresh trace per emission. `SizeLoop` counts its own builds now. A new
  `VarkaKernelPlanSuite` test emits twice into one trace and expects two corrections; on the base
  it finds one.

And cleanups, with the dump byte-identical to the base's after them:

* `split` returns its stuck outputs in a small `Split` record, where `run` passed it a list to
  fill; a class built without a byte budget returns on `budget == 0`, not on a null measurement;
  and a dropped grouping switch restores the caller's call-site budget in one `regroupFrom`.
* The notes that pointed at the loop "above" or in `emit` point at `SizeLoop`'s steps.
* The dump's first line names the seeds and the options drawn from, and it cancels under a
  matrix config, where the variants named planned may not be.
