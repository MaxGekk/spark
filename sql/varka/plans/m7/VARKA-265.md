# VARKA-265: How well the tests test: branch coverage and a bounded mutation run

## 1. Where this came from

Row 265 of `m7/PLAN.md`, from the plan review (2.2, "How well the tests test") and `m7/READING.md`
6 and 11. Nothing in the record measured how well the suites test the emitter: no branch coverage
of the emitter's own classes and no mutation testing. Druid fails a pull request on changed lines
under 50% covered and none of the thirteen engines READING.md 6 surveyed mutates at all; Petrovic
and Ivankovic ("State of Mutation Testing at Google", ICSE-SEIP 2018) grew a mutant's usefulness
from 20 to 80 per cent by turning every "not useful" into a rule (pp. 2-6), and READING.md 11 adds
the Varka-specific trap: a mutant that only the emitted-bytes oracle kills changed bytes and no
answer, so it is not a behavioural kill. The row asks for branch coverage of the emitter and the
compiler under the suites and fuzzers first, then a bounded PIT run on `Slots` and `Analysis` in an
idle window, PIT reaching the ScalaTest suites test by test with `NON_VOID_METHOD_CALLS`,
`REMOVE_CONDITIONALS` and the bitwise mutators beside its defaults, three verdicts per mutant, and
each survivor answered by a test or an equivalence note.

It builds on two finished rows. VARKA-285 runs every Varka suite under the JaCoCo agent and reports
the classes the emitter emits; its agent already instruments the emitter's package, so the
emitter's own coverage is a second report over the same execution file, not a second run. VARKA-280
(in review as #861, this branch's base) lists the 39 mechanisms the emitter can apply to a shape
and commits what the IR fuzzer and the coverage suite reached (`sql/varka/coverage/reach/`); the
coverage report reads those files and says, per mechanism, whether the line recording it ran under
all the suites.

The owner set the phasing on 16 October 2026: the plan and branch coverage first; the PIT setup
built and verified on a tiny scope while the laptop is loaded by other agents (load average about
30 on 24 cores), with the full run left as a command for the owner's idle window; and the owner's
rule on tools - name the publisher and coordinates of PIT and any plugin, official Maven Central
artifacts only, and stop to report if the ScalaTest integration needs a third-party plugin.

## 2. The admission check, done

Three things had to be true before building the reports: that PIT reaches the ScalaTest suites
test by test with nothing but official artifacts, that a mutant run means what it says despite the
emitter's caches, and that the bytes oracle's verdict separates from a behavioural one. All three
were checked on 16 October 2026 on the laptop, pinned to cores 12-23 beside three other agents.

**Provenance.** PIT is `org.pitest:pitest`, `org.pitest:pitest-entry` and
`org.pitest:pitest-command-line`, version 1.30.0 (released 27 August 2026; 1.30.1 of 14 October was
two days old and passed over), published by the pitest project (pitest.org, Henry Coles) on Maven
Central, Apache-2.0, fetched by `build/mvn dependency:get` into the local Maven repository as
`dev/varka_gen_coverage.sh` fetches JaCoCo. Their runtime dependencies are Maven Central artifacts
too: ASM 9.10.1 (OW2), `jopt-simple` 4.9 and the HTML report (shaded into the command-line jar),
`stringtemplate` 3.2.1 (pulled by the HTML report, unused since the run writes XML and CSV), and
Commons Text and Lang, which the launcher takes from Spark's own test classpath (1.15.0 and 3.21.0)
rather than a second copy. No build file gains a dependency. **No third-party plugin**: PIT ships
JUnit 4 support only, and the Varka suites are ScalaTest, so the ScalaTest integration is written
here against PIT's SPI (`dev/varka_pit/VarkaPitScalaTest.java`, about 220 lines), as is the filter
that restricts the second run (`dev/varka_pit/VarkaPitOnly.java`). The JUnit 5 `@VarkaSuite`
classes would need `org.pitest:pitest-junit5-plugin` 1.2.3 (official, the same publisher, May 2025),
whose compatibility with the JUnit 6.0.3 Spark uses is unverified; those suites test the method
names, the platform, the range rules and reproduction files, not `Slots` or `Analysis`, so they are
left out of this run (3.2) rather than the plugin added.

**The mutators.** The free 1.30.0 jar lists 22 mutators. `NON_VOID_METHOD_CALLS` and
`REMOVE_CONDITIONALS` are there. No mutator named for bitwise operators is: the bitwise and shift
swaps (`&` to `|`, `|` to `&`, `^` to `&`, `<<` and `>>` and `>>>` exchanged) are part of `MATH`,
which is one of the defaults, so "the bitwise mutators beside its defaults" is met by the defaults
themselves. A dedicated bitwise operator set exists only outside the free artifact, and is not
used.

**The probe.** `dev/varka_pit.sh --classes Analysis --methods planBitmapPass,pureOf --suites
VarkaEmitterValiditySuite --threads 2`: two methods of `Analysis` and their constructor, 108
mutants, one behavioural suite of 24 tests, then the bytes oracle.

* Test by test works: PIT's coverage phase ran the 24 tests one unit each in 15 s and attributed
  each kill to its test by name (`VarkaEmitterValiditySuite.the served and declined root counts,
  per shape` killed most of `planBitmapPass`'s mutants). The suite is an own-JVM suite, run inline
  with `-Dvarka.ownJvm=true`, since a child JVM would load the unmutated classes.
* Caches: every unit empties the shape cache before its test (`VarkaShapeCache.invalidateAll`). A
  kernel cached by an earlier test was emitted by the code before the mutant was swapped in, and
  would hide the mutant or blame it for another's bytes.
* The verdicts separate: 60 killed by the behavioural suite, 45 killed only by the bytes oracle, 3
  survived. With one behavioural suite of thirty the bytes-only count is high by construction; it
  shows the oracle kills what the behavioural tests miss, and so that reading its kills as
  behavioural would have scored this probe 97% where the behavioural score is 56%.
* Cost: the behavioural run took 76 s, the bytes oracle's 743 s at two threads with load average
  about 25: its pinned-shapes test alone took 48 s in the coverage phase, and every mutant it
  covers reruns it. Hence `VarkaPitOnly`: the second run gets only the mutants the first did not
  kill (48 of the 108 here), which `dev/varka_pit_verdicts.py --keep` writes.
* `git status` after the run: `emitted_bytes.json` and every other committed file unchanged.

What the check would have rejected: a ScalaTest integration that needed a third-party plugin
(stop and report), a test unit that could not run one test alone or reset the emitter's cache
between tests, or a bytes oracle that killed nothing the behavioural suites
miss, which would make the second verdict empty and the second run pure cost. None of the three.

## 3. The design

### 3.1 Branch coverage of the emitter and the compiler

`dev/varka_gen_coverage.sh` gains the compiler's classes in the agent's includes - `codegen.Varka*`,
`codegen.CompiledVarka*`, `codegen.PartialVarka*` beside the `codegen.varka.*` it already had - and
a second analyser, `dev/varka_gen_coverage/VarkaEmitterCoverage.java`, run on the same execution
file against catalyst's built classes. The emitted-class report is unaffected: VARKA-285's
analyser keeps only classes carrying a Varka line map, which no source class has. The new report,
`sql/varka/coverage/emitter.md`, gives:

* per area (emitter, compiler) and per source file: methods, methods entered, instructions and
  branches covered, branches missed;
* per mechanism of `VarkaEmitMechanism`, whether the code that records it ran under all the suites,
  beside whether VARKA-280's reach reports saw it. A mechanism is read where it really happens: a
  `reach(...)` call, where a call choosing between two mechanisms ran both only if its condition
  took both branches; a size counter of `VarkaEmitTrace`, paired with its mechanism by the order of
  `counters()` and `COUNTED`, at the lines that increment it; and a guard's reason through
  `VarkaEmitMechanism.guard`, where the report says the guards' line ran and not which reason;
* every line of `Slots` and `Analysis` with a branch no suite took, with its source: the mutation
  run's survivors on these lines are expected, and a survivor elsewhere had a test reach it;
* the methods never entered, per file.

A class whose name has execution data under another class id ran under other bytes than the built
ones, and is listed as stale rather than counted as missed.

### 3.2 The mutation run

`dev/varka_pit.sh` builds catalyst's test classes (or reuses them), compiles the two plugin classes
against the suites' own ScalaTest and PIT, and runs PIT twice over the same mutants of the named
classes (default `Slots,Analysis`, and their nested classes):

1. **Behavioural**: every eligible catalyst ScalaTest suite except `VarkaEmittedBytesSuite`.
2. **Bytes**: `VarkaEmittedBytesSuite` alone, over the mutants the first run did not kill.

`dev/varka_pit_verdicts.py` joins them by PIT's mutant identity (class, method, descriptor,
mutator, instruction indexes, all from the bytecode) into the three verdicts and writes the report:
counts per class and per mutator, and every mutant no behavioural test kills, with its answer from
`sql/varka/mutation/notes.tsv` - the test that now kills it, or why it changes no behaviour - or
"open". A timeout or a crashed minion is a kill, as PIT counts it, and the report counts them apart.

The suites' JVM options are sbt's, plus the memory sanitizer and both bodies checked as in the
matrix, the IR fuzzer at 30 iterations a lane (`-Dvarka.fuzz.iterations`, a tenth of its default, as
every mutant it covers reruns it) and own-JVM suites inline. Spark's sbt exports catalyst as a jar,
so the classpath gives way to the classes directory PIT mutates. Left out of both runs, each with
its reason in the script: the suites whose verdict is the JIT's or the machine's (assembly, tier
witness, inlining cliff, the deoptimization cycle, compilation watch, both warm-up suites), the
tooling checks (watchdog, probe output, the matrix's machinery), and the cost audit, which compares
a committed file as the bytes oracle does. `--methods` restricts the mutants for a probe; PIT
filters methods only by exclusion, so every other method name the classes have is excluded.

### 3.3 What is deliberately unchanged

* The emitter and the compiler: no byte moves, `emitted_bytes.json` untouched.
* `VarkaSparkFuzz*.scala` (row 298's) and the sql/core suites: PIT runs catalyst only, since a
  sql/core suite's `beforeAll` builds a Spark session, which a test-by-test run would build once a
  test; the coverage run does include sql/core, as VARKA-285's does.
* A diff-coverage gate on pull requests (Druid's, READING.md 6) and mutation runs on changed lines
  stay with `m9/SCOPE.md`'s "Mutation runs on each pull request's changed lines".

### 3.4 Registered op counts

None: no emitter change.

## 4. Files

| file | what |
|---|---|
| `dev/varka_gen_coverage.sh` | the compiler's classes in the includes; the second report |
| `dev/varka_gen_coverage/VarkaEmitterCoverage.java` | new: the emitter and compiler report |
| `sql/varka/coverage/emitter.md` | new, generated |
| `sql/varka/coverage/generated.md` | regenerated from the same run |
| `dev/varka_pit.sh` | new: the two PIT runs and the verdicts |
| `dev/varka_pit/VarkaPitScalaTest.java` | new: PIT's test plugin for ScalaTest, test by test |
| `dev/varka_pit/VarkaPitOnly.java` | new: the second run's mutant filter |
| `dev/varka_pit_verdicts.py` | new: the three verdicts, the keep list, the notes |
| `sql/varka/mutation/notes.tsv` | new: the answers to the survivors |
| `sql/varka/skills/testing-and-debugging.md` | the lesson, and the calibration lines |
| `SKILLS.md` | regenerated index |

## 5. Tests, and what each is for

The tools are checked by running them, as VARKA-285's was: the probe of section 2 end to end (two
runs and the verdicts), the coverage report over a two-suite smoke run before the full one (it
caught a guard mechanism reported from the helper's line instead of its own), and the full
coverage run. The verdict script refuses a second run whose mutants are not exactly the first run's
unkilled ones, and a note that matches no open mutant. New tests come from the survivors, each
named in `notes.tsv`.

## 6. The measurement

The coverage run is `dev/varka_gen_coverage.sh -j 4 --split 3` on this branch, beside other agents'
load. The full mutation run is the owner's, in an idle window, with the command and the expected
duration of 9; its dry run here gives the mutant count and the coverage phase's time.

### 6.1 Predictions, registered before the run

1. **Emitter branch coverage 82%, within five points**, over every Varka suite; the compiler's
   80% within five. The two-suite smoke run gave 60.3% and 70.0%, and the fuzzers reach far more.
2. **`Analysis` 90% or more of its branches, `Slots` 85% or more**: both run for every emission,
   and what they miss is a validation's refusal and a rare layout.
3. **The constant pool's decline is the one mechanism whose recording code runs under no suite**;
   the plan's decline and the method-cap refusal, which the reach reports never saw, do run under
   some other suite (the smoke run already shows both).
4. **Under 40 emitter methods never entered.**
5. **Slots and Analysis give 2,000 to 3,500 mutants** under the three mutator groups (the probe's
   two methods and constructor gave 108 over about 110 lines).
6. **Verdicts of the full run: 80% or more behavioural, 8 to 15% bytes only, 10% or less survived**
   (uncovered included), and `Slots`' bytes-only share above `Analysis`', since a slot planned for a
   node that never reads it shifts every later local and changes no answer (`Slots`' class
   comment).
7. **Cost of the full run**: stated in 9 from the dry run's coverage phase, at the calibration
   table's ratio for harness-bound work (about 1.3 to 2 over the arithmetic).

## 7. Risks

1. **A cache other than the shape cache** keeps a kernel or a plan across tests, and hides
   mutants. The probe's kills across tests that share shapes argue against; a survivor whose line
   the coverage shows run by a test that should see it is the sign, and the notes read for it.
2. **A test flaky under PIT** fails in the coverage phase and stops the run. PIT's
   `skipFailingTests` is left off on purpose, so a flake stops the run rather than silently
   dropping a test; the exclusions take out the suites whose verdict is the machine's.
3. **Timeouts read as kills.** A mutant that loops forever is killed, and one that only slows a
   test past the timeout is reported killed too; the report counts timeouts apart.
4. **The bytes oracle kills an equivalent mutant**, which is why its verdict is read as a survivor;
   the notes say which bytes-only mutants are equivalent.

## 8. Sequencing

1. The plan, with the admission check (this commit).
2. The coverage tool and report: the includes, the analyser, the regenerated reports.
3. The PIT setup: the plugins, the runner, the verdicts, the dry run's numbers and the idle-window
   command in 9.
4. The full run (owner, idle window), then the notes and the tests that answer its survivors.

## 9. Outcome

### 9.1 Branch coverage

`dev/varka_gen_coverage.sh -j 4 --split 3` on 16 October 2026, cores 12-23 beside three other
agents: 102 suites in 1268 s, 1352 tests passed, 34 cancelled, one failed - `VarkaTierWitnessSuite`,
which asserts the JIT tier of a call and saw C2's code trap on the memory sanitizer's check under
the load; a machine verdict, not the emitter's, and the coverage it reached is counted. From
`sql/varka/coverage/emitter.md`:

| area | methods | entered | branches |
| :--- | ---: | ---: | ---: |
| emitter (`codegen.varka`) | 917 | 895 | 90.0% (392 missed) |
| compiler (`codegen.Varka*`) | 324 | 320 | 83.7% (179 missed) |

`Analysis` takes 96.2% of its branches (14 missed, on 12 lines, five of them validation
refusals a well-formed shape never meets) and `Slots` 92.9% (40 missed, on 24 lines). The
constant pool's decline is the one mechanism whose recording code no suite runs. The plan's
decline and the method-cap refusal, which VARKA-280's reach reports never saw, do run under other
suites.

**Correction to section 4**: `generated.md` is not committed regenerated. The same run moved it
from 14,745 emitted classes to 61,599 and shows families VARKA-284's table names no reason for -
`VarkaFusedFuzzTier`, and `VarkaFusedProjection_w<hash>` classes that `VarkaGenCoverage.family`
does not fold, since their suffix starts with `w` - which is row 284's and 285's to read, not this
row's.

### 9.2 The mutation run, set up

The dry run over the full scope (`dev/varka_pit.sh --dry-run`): **1,744 mutants**, 1,006 in `Slots`
and 738 in `Analysis`; the coverage phase ran 588 tests of 40 suites one unit each in 330 s, with
the IR fuzzer's C2 test off. The first dry run had it on, and it alone took 482 s of 869; the same
run stopped on `VarkaWidthAuditSuite`, which fails under PIT's agent and is now left out.

A sample over all 40 suites (`--classes Analysis --methods planBitmapPass,pureOf`):
`sql/varka/mutation/sample.md`, 104 mutants, 103 killed by a behavioural test, none by the bytes
oracle alone, one survivor, answered in `notes.tsv` as equivalent (`planWordAlgebra` keeps every
`Cond` node out of `pureWord`, so `planBitmapPass`'s `instanceof Cond` test is redundant). The
probe's other survivor, javac's `requireNonNull` on `pureOf`'s pattern switch, is killed here: a
narrow run's survivor is not an answer. A killed mutant ran one test at the median; the survivor
ran 405. The run took 320 s of coverage and 493 s of mutants, serially, since PIT made one unit
of the class - hence `--unit-size` (default 20).

**The idle-window command** (owner), from `/home/max/proj/varka-265`:

    dev/varka_pit.sh --threads 12 --report sql/varka/mutation/verdicts.md \
      > target/varka-pit.log 2>&1

Twelve minions of 4 GB each. **Expected duration about two hours, and stop to read it past
four**: the build and coverage phase about 8 minutes; the killed mutants about 1,400 at about 2 s,
5 minutes over twelve threads; the mutants the first run leaves, about 300 by prediction 6, each
rerunning about 400 covering tests at the sample survivor's cost, between 150 s idle and 250 s
loaded, 60 to 105 minutes over twelve threads; the bytes oracle's run on those, at about 14 s a
mutant, under 10 minutes. The estimate is the arithmetic times the calibration table's ratio for
harness-bound work, up to 2, which is the four hours; what decides it is the number left by the
first run, which the report states.

### 9.3 The predictions, scored

1. Emitter branch coverage 82% within five: **missed**, 90.0%; the compiler's 80% within five:
   **held**, 83.7%. The fuzzers and the matrix base reach more of the emitter than the smoke run's
   two suites suggested.
2. `Analysis` 90% or more, `Slots` 85% or more: **held**, 96.2% and 92.9%.
3. The constant pool's decline the only mechanism no suite reaches: **held**.
4. Under 40 emitter methods never entered: **held**, 22 - among them two `Slots.plan` overloads
   with no caller in the main or test tree, dead code whose mutants are uncovered by construction.
5. 2,000 to 3,500 mutants: **missed**, 1,744; the probe's two methods are denser in calls than the
   classes on average.
6. and 7. The full run's verdicts and duration: open, the owner's idle-window run.

### 9.4 Left for later

* The full run's survivors, each a test or a note, after the owner's run.
* The two dead `Slots.plan` overloads: delete them, or leave their mutants as uncovered by
  construction; an owner's call, since it touches the emitter.
* `generated.md`'s new families (9.1), for rows 284 and 285.

