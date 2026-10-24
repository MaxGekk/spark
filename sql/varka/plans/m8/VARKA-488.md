# VARKA-488: a kernel plan, the lane decided once per kernel

## 0. Why this row exists

`VARKA-475.md` 3.3 as a row of its own. A kernel's lane - the species its loop drives at, the
species each input loads at and whether it widens, the lane each node is emitted at - is worked
out today by each consumer from the roots and the options:

- the compiler's admission (`VarkaExpressionCompiler`, about line 620) calls
  `VarkaLoopEmitter.narrowRefusal` on the entry alone, then `VarkaVectorIR.kernelLane` on the
  kernel with the entry, then `narrowRefusal` again on that kernel, composing a different reason
  each time;
- the budget mirror (`VarkaLoopEmitter.fitsBudgets`) calls `narrowRefusal` and `kernelLane` again;
- the emitter calls its private `laneOf(outputs, options)` five times (`emit`, and the three
  driver builders), and `Analysis` derives `mixed()`, `laneOf(node)`, `inputLane`, `lanesOf`,
  `widenLanes` and `planNarrowGroups` from the lane it is handed;
- `CompiledVarkaProjection` calls `kernelLane` in its constructor's literal check and in `lane()`;
- in tests, `VarkaWidthAuditShapes.lane`, `VarkaEmittedBytesSuite`, `VarkaReplay` and
  `VarkaEmitterBoolSuite` read the lane from the first root alone (`emissionLane(roots.head)`).

They agree because each was fixed to agree: the reviews of the narrow lanes (`VARKA-329.md` 9.8)
found the disagreements - an output width with two unchecked sources, the checked narrow multiply
decided per root, a refusal reason on the wrong entry - and the fixes made `kernelLane` the one
function. `TIMESTAMP_NTZ` (row 338) is next: a long decomposed into the int32 calendar prefix,
with casts to and from `DATE`, the first type whose lanes cross between consumers by design.
3.3 makes the lane a value computed once, before that type lands.

## 1. Design

**`VarkaKernelPlan`, a record in `varka/` beside `Lane` and `Analysis`** (`VARKA-475.md` 8,
question 1's default), built by one static function from the roots and the options:

```java
static VarkaKernelPlan of(List<VarkaVectorIR> roots, VarkaEmitOptions options)
```

It holds what the consumers now derive:

- `refusal`: why no kernel of these roots can be emitted, with the root it belongs to - the
  lane mismatch (a long root beside a narrow or int one) and the three narrow refusals, word for
  word as today, since five suites pin them; empty when the kernel can be emitted;
- `drive`: the lane the loop runs at, `kernelLane`'s answer;
- `mixed`: whether the kernel converts between lanes;
- `laneOf(node)`: the lane each node is emitted at, `Analysis.laneOf`'s rule (the drive lane
  in a single-lane kernel, the node's own in a mixed one, a narrowing at its store excepted);
- `inputLane(ordinal)` and `widenLanes`/`lanesOf`: each input's load, its species' lane count
  and its widening part count;
- `narrowGroups`: the int groups one iteration computes.

A plan with a refusal has no lane fields; asking one throws, so no consumer can emit past a
refusal.

**Each consumer reads it.**

- The compiler builds the plan of the entry alone and of the kernel with the entry, and records
  the refusal it finds; it composes no reason of its own (the "joining drives ..." prefix comes
  from the plan, which knows the refusal came from joining).
- `fitsBudgets` takes the plan and prices it.
- The emitter builds the plan once in `emit` and hands it to `Analysis`, whose constructor takes
  it in place of the lane; the five `laneOf(outputs, options)` calls go.
- `CompiledVarkaProjection.lane(options)` reads the plan's drive lane, and its constructor's
  literal check keeps reading the plan under `DEFAULTS`, as its comment explains.
- The test sites that read a lane from the first root read the plan of all the roots.

`VarkaVectorIR.kernelLane` and `VarkaLoopEmitter.narrowRefusal` leave the public surface;
`emissionLane` stays as the plan's per-root step, package-private.

**What stays in `Analysis`.** Slots, words, guards and `wrapAt`. 3.3's prose lists the reduction
sites among the plan's fields, and its done-when does not; `wrapAt` places reductions, which is
about emission, and it reads `checkIntOverflow` node by node. Decided below.

**Byte identity covers acceptance; an equivalence test covers refusal.** `emitted_bytes.json`,
`coverage.json` and `width_audit.json` pin what the compiler fuses and the bytes it emits; a plan
that stopped refusing what the old code refused would emit no byte anywhere. So the refactor
carries a test, deleted in its last commit with the old functions, that builds the plan beside the
old derivations over a corpus - every coverage row's compiled roots, `VarkaIrFuzzSuite`'s
generator, the narrow suites' shapes - and asserts that `drive` equals `kernelLane`, that
`refusal` equals `narrowRefusal` and the lane-mismatch reason (text and root), and that
`narrowGroups`, `mixed` and `laneOf(node)` for every node equal `Analysis`'s. It runs under every
configuration of the option matrix, since the lane turns on `narrowLanes`, `validityByWidth` and
`lanesOverride`.

**One pull request, each commit byte-identical.** (a) The plan as a pure recomputation and the
equivalence test, no consumer changed. (b) The consumers one at a time, the three oracle suites
run after each: `CompiledVarkaProjection`, `fitsBudgets`, the compiler, then the emitter and
`Analysis`. (c) The old functions and the equivalence test deleted, the test sites moved. If the
diff outgrows a review, the seam is after the three non-emitter consumers: a plan read by the
compiler, the budget and the projection but not yet by `Analysis` is a coherent state.

**Decided in the plan, object if wrong:**

1. *`wrapAt` stays in `Analysis`*, as above; the plan holds the lanes, `Analysis` where the
   reductions go.
2. *The warm-up reach model is 3.4's.* `VarkaWarmupGate` lists species by hand; the registry
   supplies them. This row does not touch it.
3. *Reasons do not change.* Every refusal text stays byte for byte; the plan owns them.

## 2. Predictions and costs

`VARKA-475.md` 3.3 estimates three to five days. No emitted byte moves: the three generated
files are byte-identical at every commit. The plan is built once per candidate entry in the
compiler (as `kernelLane` and `narrowRefusal` are today, three walks of the roots) and once per
emission, so compile time does not move measurably. The equivalence test, over about 260 coverage
rows and a few thousand fuzzer draws, compile only, takes under 30 s at the calibration table's
short-test ratios; it lives only until the last commit.

## 3. Done when

`VARKA-475.md` 3.3's done-when: the compiler, `fitsBudgets`, the emitter and
`CompiledVarkaProjection` read one plan; no consumer derives a lane from a single root, the test
sites included; a refusal reason is produced only by the plan; and `emitted_bytes.json`,
`coverage.json` and `width_audit.json` are byte-identical. Plus the house's: the equivalence test
green under every configuration before it is deleted, the option matrix over the touched suites,
the catalyst defaults gate, the linters.

## 4. Outcome

*24 October 2026. Done.* `VarkaKernelPlan` is in `varka/`, built by `of(roots, options)`, and the
compiler's admission, `fitsBudgets`, the emitter with `Analysis`, and `CompiledVarkaProjection`
read it. `kernelLane` and the per-root `emissionLane` moved from `VarkaVectorIR` into the plan
(the per-root step package-private, for the unit tests that ask about one root's lane);
`VarkaLoopEmitter.narrowRefusal` and the emitter's private `laneOf` are gone. `Analysis` takes
the plan in its constructor and answers `mixed`, `laneOf(node)`, `narrowsAtStore`, `inputLane`,
`lanesOf` and `narrowGroups` from it; its `operands`, `roots`, `mixed` and `rootsAnalyzed` state
went with the derivations. The refusal texts are unchanged, now each produced by the plan with
its kind (`NARROW` or `LANE_MISMATCH`), and the compiler adds only the "joining drives ..."
prefix it always added.

**Three commits, each byte-identical,** as section 1 planned: the plan beside the old code with
the equivalence test; every consumer reading it; the old functions, the equivalence test and the
first-root test sites removed. The equivalence test held the plan to `kernelLane`,
`narrowRefusal` and `Analysis` - the drive lane, the refusal text, `mixed`, `narrowGroups`,
`lanes`, `lanesOf`, every node's lane and every input's - over 2,400 random kernels of six
grammars, 2,000 joined pairs and every coverage row, and the compiler's admission of one entry
after another, under all 52 configurations of the option matrix. It took 87 s at the defaults
against under 30 s predicted (the coverage rows are compiled from SQL, which the estimate did
not price), and lived only until the last commit.

**Not moved, as decided.** `wrapAt` stays in `Analysis`. Two lane-mismatch checks stay with the
compiler arms, since they guard building one node over two lanes inside one root, not the
kernel's lane: `VarkaConditionCompiler`'s operand check, and the predicate path's check before it
folds conjuncts with `AND`, which now takes its text from the plan's `laneMismatchReason`.

**Behaviour at the edges.** The emitter refuses a kernel the plan refuses, so a direct caller
that hands it a narrow value it cannot widen fails at `planOf` with the plan's reason rather than
later in emission; the compiler never hands it one. `CompiledVarkaProjection.lane` names the
plan's reason when it throws.

**Review (24 October 2026).** One read-only review found no behaviour change. Applied: the
lane-mismatch search walked every prefix again for each refused entry, quadratic in the outputs;
it is one pass now. An import left unused, an orphaned javadoc, the mismatch text kept in two
places, `Refusal`'s unread root, two stale comments and the emitter skill's notes were fixed.

**Measured.** The catalyst defaults gate passes (873 tests) and the sql defaults gate (1310);
`emitted_bytes.json`, `coverage.json` and `width_audit.json` are byte-identical. The option
matrix over the twelve suites the change touches found one test that asserted the drive lane
under `validityByWidth=false`, where the plan now refuses the int-lane kernel for the missing
lane count; the test accepts that refusal as the int lane's answer, which is what it is.
