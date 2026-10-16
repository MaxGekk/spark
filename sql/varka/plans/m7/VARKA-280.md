# VARKA-280: What the emitter reached

## 1. Where this came from

Row 280 of `m7/PLAN.md`, from the reading of QPG (Ba and Rigger, "Testing Database Engines via
Query Plan Guidance", ICSE 2023; `m7/READING.md` 11): steering generation toward query plans not
seen before found 1.4 times SQLancer's unique bugs, where the coverage-guided SQLRight, with more
code coverage, found 17 times fewer (pp. 8-9). `m7/PLAN.md` 2.2 names Varka's plan as the
emitter's record of what it did - the canonical IR over literal slots, the `VarkaEmitTrace`
reactions, the groups and their shared or materialized prefixes, one kernel or several, the
decline reasons - and asks that every fuzzer and coverage suite report the distinct ones it
reached and the mechanisms it never did, which row 265 reads, then that steering - draws that
reached something new kept and mutated - be measured by a fixed-seed A/B and kept only if it
reaches more.

Before this task the only such report was `VarkaEmitTrace`'s six size counters, which the IR
fuzzer printed and the wide test asserted. Nothing named what the emitter decides about a whole
shape beyond them, so a run that never grouped two outputs past the budget, never loaded a
materialized prefix or never folded a guard under an arm passed as quietly as one that did.

## 2. The admission check, done

The check is how many mechanisms the suites' fixed-seed runs never reach: if the answer were none,
the report would carry no news and the A/B could only be about how soon each is reached. It needed
the list and the hook to exist, so they were built first as a probe (section 3), with
`VarkaEmittedBytesSuite` green on the probe - the hook writes no byte - and run on 16 October 2026
on the laptop, pinned to cores 12-23: `VARKA_REACH_DUMP=<dir> build/sbt 'catalyst/testOnly
*VarkaIrFuzzSuite *VarkaCoverageSuite'` at the suites' defaults (seed 20260903 for the int lane,
20260919 for the long lane, 300 iterations a lane, 10 wide compositions).

Of the 39 mechanisms in the list:

| report | emissions | declined | distinct plans | distinct mechanism sets | reached |
| :--- | ---: | ---: | ---: | ---: | ---: |
| int-lane draws (with their variants and retries) | 2220 | 88 | 1846 | 514 | 28 |
| long-lane draws | 2183 | 21 | 1815 | 309 | 19 |
| wide compositions | 10 | 6 | 10 | 9 | 30 |
| coverage table | 94 | 0 | 83 | 22 | 19 |

* **Four are reached by no test of the IR fuzzer**: `PLAN_CORRECTED`, `METHOD_CAP_REFUSAL`,
  `DECLINE_PLANNED` and `DECLINE_CONSTANT_POOL`. The first and third need the plan under the size
  loop's planned variants, which only the opt-in size-trace dump draws; the second is the shape
  past the class-file cap, about one in three hundred thousand draws (`VARKA-219.md`); the last
  needs a constant pool past 65535 entries, which no grammar builds.
* **The two lanes' draws alone miss nine**: beside those four, `STAGED_DRIVER`, `STAGE_SPLIT`,
  `CALL_SITE_ROLLBACK`, `EXACT_GROUPING_DROPPED` and `PREDICTION_DROPPED`, which only the wide
  compositions reach. The long lane misses twenty, eight of them calendar mechanisms its grammar
  has no node for.
* **The coverage table misses twenty**, every size reaction and every decline among them: its
  widest shape, all 66 projection rows as one projection, is one kernel that fits, and none of its
  66 projection compilations needs more than one kernel.

So the report has news on every suite, and what the suites never reach is mostly the size loop's
rare corners and what a lane cannot build. A probe of the A/B ran too, at the default seeds and
300 emissions an arm, with the first steered arm only (keep a case that reaches a new set of
mechanisms): int lane 25 mechanisms plain against 27 steered, long lane 18 against 18; 185 and 150
distinct sets plain against 134 and 109 steered; and a mechanism both arms reached was reached
later by the steered arm in most cases, since half its steps are mutations. It showed the keep
rule keeps too much - 185 of 300 plain draws reach a new set - which is why a second steered arm
was added (3.3) before the predictions below were registered. The probe is one seed; the
measurement (section 6) is sixteen seed-lanes at two budgets.

## 3. The design

### 3.1 The list and the hook

`VarkaEmitMechanism` (new, main) is a closed enum of 39 mechanisms in seven families: the grouping
(one group, several, a group of several outputs past the group budget held by what they share, a
group the prediction widened, a staged driver), sharing (a node loaded again from its CSE local, a
calendar prefix reused in its body, stored to the scratch, loaded from it, run without the month
step), validity (the bitmap pass, its decline on a mixed AND/OR tree, a kernel with no dense
body, the all-null shortcut, a dead input word), the guards (one per `Analysis.Refusal` reason,
mapped by an exhaustive switch, a guard under an arm, an exact status), the constant division by
form, the size loop's reactions and the plan's (`VarkaEmitTrace`'s counters, the plan's
correction, a method refused at the class-file cap), and the declines by kind. It leaves out what
an option selects on its own - the mod-7 form, the calendar's division, the driver's form - since
the fuzzers draw every option at random and reach those by construction.

Each mechanism is noted where the emitter does it, into `Analysis.reached`, an `EnumSet` the
analysis carries for the reason it carries the options: the body mechanisms where their bytes are
emitted (the CSE reload in `VarkaVectorWalk.emitValue`, the guard in `emitGuardCollect`, the
prefix in `VarkaChronoLowering.emitChronoPrefixOnce`, the division in `emitConstDivide`, the
driver's in a note after `emitDriver`, the dead word after `emitGroupBody`), not where `Slots`
plans a slot, since the planner plans slots for bodies and nodes no build emits. The size loop
notes the grouping, the declines and the refusals, and its `run` hands the set to the trace when
the emission ends, built or declined: `VarkaEmitTrace.noteEmission` adds the mechanism of every
counter that moved during the emission - `counters()` and `COUNTED` are the one place a counter is
paired with its mechanism - counts each mechanism over the trace's emissions, and hands a record
of the emission (outputs, mechanisms, group count, declined) to `onEmission` where a suite set
one. In the engine `onEmission` is null and no record is built. All of it runs once per emission,
at compile time; nothing is per batch, and no `CodeBuilder` is touched.

### 3.2 The reports

`VarkaEmitReach` (new, test Java) is the suites' consumer of those records: per mechanism the
emissions that reached it and the first that did, the mechanisms never reached, the distinct plans
- the canonical outputs, the group count and the mechanism set, so two draws differing only in
literal values share a plan - and the distinct mechanism sets, the plan without its trees.
`VarkaIrFuzzSuite` attaches one to its trace for the int-lane test, the long-lane test and the
wide test, and prints each; the wide test's list of size mechanisms it must reach now reads the
trace's per-mechanism counts through the enum instead of naming the counters a second time.
`VarkaCoverageSuite` gains a test that emits every row's kernels under the defaults, as
`guardsOf` enumerates them, then all projection rows as one projection and all predicate rows as
one conjunction, and counts how many projection compilations needed more than one kernel ("one
kernel or several" is the compiler's decision, read from `compilePartial`). With
`VARKA_REACH_DUMP` naming a directory each suite writes its reports there as a markdown table,
and the run of section 2 is committed as `sql/varka/coverage/reach/VarkaIrFuzzSuite.md` and
`VarkaCoverageSuite.md`, the files row 265 reads. Nothing is asserted on a count: what is reached
moves with the grammar, the emitter and the matrix's base.

### 3.3 Steering, and its A/B

`VarkaSteering` (new, test Java) generates emission only. The plain arm emits the lane's draws 0 to
n-1. A steered arm, at each step, takes the next fresh draw with even odds or where nothing is kept
yet, and otherwise mutates a kept case with the mutations the suites already have: one option of
`VarkaEmitOption.TABLE` redrawn as the IR fuzzer draws it, or one of the case's `VarkaVariants`
that the form without a byte budget holds. Two keep rules, both run (the house's rule for
competing designs):

* **new set**: keep a case whose emission reached a set of mechanisms no earlier one did - QPG's
  rule with the trees left out, since nearly every random tree is a new plan;
* **rare mechanism**: keep a case that reached a mechanism fewer than four earlier emissions did,
  closer to a coverage-guided fuzzer's favoured inputs, since the probe showed the first rule keeps
  most draws.

A new test of `VarkaIrFuzzSuite` runs the three arms per lane at `-Dvarka.fuzz.steer` emissions an
arm (default 300, the main tests' count) over `-Dvarka.fuzz.steerSeeds` (default the suite's two
seeds) and prints one `A/B` line per arm - mechanisms reached, distinct sets and plans, the median
over the mechanisms all three reached of the emission that first reached each, and the list never
reached - and the first emission per mechanism per arm. A steered sequence is a function of the
seed and the emission-only history, so a steered case is reproduced by rerunning the sequence to
its step, in seconds, before any kernel runs.

If the measurement keeps steering, a steered test joins the fuzzer: the steered sequence of 300
cases a lane, each checked against the reference evaluator as the main tests check their draws,
its failures shrunk and named with the case. If not, steering stays out of the fuzzer, the A/B
test stays as the re-runnable measurement, and the result goes to the skills file.

### 3.4 What is deliberately unchanged

* The emitted bytes: `emitted_bytes.json` byte-identical, held by `VarkaEmittedBytesSuite`.
* The main tests' draws and their order: `VarkaIrGrammar`'s int sequence is the bytes oracle's
  committed corpus, and steering is a separate sequence in a separate test.
* `VarkaSparkFuzz` and `VarkaSparkFuzzSuite`, the Spark-level fuzzer, which row 298 owns now: its
  report is a follow-up of a few lines once 298 merges - attach a `VarkaEmitReach` to a trace the
  evaluator's emissions go through, which the engine path does not have today (its `emit` builds
  its own trace), so it needs a trace passed down the shape cache, or a `VarkaEmitTrace` sink set
  per test. `VarkaCoverageCompositionFuzzSuite` and `VarkaSmallTreeSuite` are fuzzers outside the
  files this task was given; each is a few lines with `VarkaEmitReach` and is left to its next
  change, listed in section 9.
* The compiler, the shape cache and the evaluator.

### 3.5 Registered op counts

None: no emitted byte changes.

## 4. Files

| file | what |
| --- | --- |
| `sql/catalyst/.../varka/VarkaEmitMechanism.java` | new: the closed list of mechanisms, by family |
| `sql/catalyst/.../varka/Analysis.java` | the emission's reached set and `reach` |
| `sql/catalyst/.../varka/VarkaEmitTrace.java` | `noteEmission`, the counter-to-mechanism pairing, per-mechanism counts, `onEmission` |
| `sql/catalyst/.../varka/VarkaLoopEmitter.java` | the size loop's notes and its `run` handing the set over; `noteWidened` for groups past the budget |
| `sql/catalyst/.../varka/VarkaBodyEmitter.java`, `VarkaVectorWalk.java`, `VarkaChronoLowering.java`, `VarkaDivisionLowering.java` | the body mechanisms, noted where emitted |
| `sql/catalyst/src/test/java/.../varka/VarkaEmitReach.java` | new: the report and its markdown |
| `sql/catalyst/src/test/java/.../varka/VarkaSteering.java` | new: the plain and steered sequences |
| `VarkaIrFuzzSuite.scala` | the three reports, the wide test's list through the enum, the A/B test, the dump |
| `VarkaCoverageSuite.scala` | the table's reach test and its dump |
| `sql/varka/coverage/reach/*.md` | new: the committed reports |
| `m7/PLAN.md` | row 280 only |

## 5. Tests, and what each is for

* `VarkaEmittedBytesSuite`: the hook changes no byte, at both widths of its corpus.
* `VarkaIrFuzzSuite`, all tests: the reports attach and detach around the tests that own them, the
  wide test still reaches its six size mechanisms through the enum, and the A/B test runs.
* `VarkaCoverageSuite`: the reach test emits every row's kernels and notes one emission per kernel
  (asserted: a kernel whose emission went unnoted is a hook that missed an exit).
* The suites that read `VarkaEmitTrace`'s counters - `VarkaKernelPlanSuite`,
  `VarkaEmitterBudgetSuite`, `VarkaEmitterSplitDriverSuite`, `VarkaExactGroupingSuite` - unchanged
  in what they assert.

No pinned fixture moves.

## 6. The measurement

The A/B test at sixteen seed-lanes - seeds 2026101601 to 2026101608, each at both lanes - and two
budgets, 300 emissions an arm (the main tests' count) and 3000, emission only, pinned to cores
12-23: `build/sbt -Dvarka.fuzz.steerSeeds=2026101601,...,2026101608 -Dvarka.fuzz.steer=<n>
'catalyst/testOnly *VarkaIrFuzzSuite -- -z steering'`. The tally reads the `A/B` lines.

### 6.1 Predictions, registered before the run

1. **At 300 emissions neither steered arm reaches more**: each arm's mean mechanism count within
   one of the plain arm's at each lane. The grammar draws one to three roots under options drawn
   per draw, so most mechanisms fall in the first twenty draws, and the ones left - the size
   loop's reactions and the declines - need a width that a mutation adds only by doubling a draw's
   roots, a few times in 150 mutations.
2. **At 3000 the rare-mechanism arm reaches one more than plain at the int lane on average**, the
   size reactions its repeated variants grow into; the long lane equal.
3. **Both steered arms reach fewer distinct mechanism sets than plain** at both budgets, and reach
   the mechanisms all arms reach later (a higher median first emission), since half their steps
   mutate cases already kept.
4. **The rule that decides**: steering is kept only if one steered arm, at 300 emissions, reaches
   more mechanisms than plain in at least 10 of the 16 seed-lanes and fewer in at most 2, with a
   mean higher by at least one. By prediction 1 it is not kept.
5. **Cost**: the A/B test at its defaults, one seed pair, 1800 emissions, under 10 s - two
   milliseconds an emission, doubled for the harness by the calibration table's lesson; the
   coverage suite's reach test under 2 s.

## 7. Risks

1. **A mechanism noted where it is not emitted, or missed where it is.** The notes sit at the
   emission sites, so a body never built notes nothing; a missed exit of the size loop would leave
   an emission unnoted, which the coverage suite's emission count catches.
2. **The hook changes bytes.** It writes no instruction; `VarkaEmittedBytesSuite` holds it.
3. **The list rots**: a mechanism added to the emitter without an entry is simply not reported. The
   guard reasons are tied by an exhaustive switch, and the size counters by `counters()` beside
   them; the rest rely on review, as the module's javadoc says what belongs.
4. **The dump overwrites the committed reports from a partial run** (a `-z` filter). It writes only
   where `VARKA_REACH_DUMP` is set, and the command in each report's first line regenerates it.

## 8. Sequencing

One commit for the plan; one for the list, the hook, the reports and the A/B test, with the
committed reports; one for the measurement's outcome in section 9 and the row, and, if steering is
kept, the steered test.

## 9. Outcome
