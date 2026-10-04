# VARKA-248: `VarkaEmitOptions` from one table of options

*Row 248 of `m7/PLAN.md`, wave 0, from `m8/SCOPE.md` item 74.1 and `m7/READING.md` 3 and 11;
opened 4 October 2026, the second of milestone 7's refactors, after VARKA-249.*

## 1. Where this came from

`VarkaEmitOptions` is a record of 40 options - switches between two lowerings, size budgets, the
target's lane count and AVX level, fault injectors. A new option touches about nine places: the
record component, the positional `DEFAULTS` (a row of bare booleans), the compact constructor, the
builder's field and setter and copy, a `with*` method, `canonical()`, the bytes suite's option
inventory, and - through reflection over the `with*` methods - the IR fuzzer's draws and
`VarkaEmitDump`'s parser. Nothing records why an option exists, so VARKA-247's question, whether
each still earns its place, has to be asked from scratch.

## 2. The admission check, done

**What reads the options generically.** `canonical()` renders them into the shape hash: the first
26 by position, the 14 added since as tags, each tag shown when its option is true, false, or off
its default - `predictGrouping` and `planSize`, now on by default, still render when true, so a
non-default variant's string carries them. The bytes suite's inventory lists arms by hand and has
fallen behind (`rangeSets`, `splitConditions`, `groupLocalSlots`, `materializeChronoPrefix`,
`callSiteBudget` and `heavyGroupOutputs` are missing). The fuzzer reflects over the `with*`
methods in name order with a hand list of int domains; `VarkaEmitDump` reflects to parse
`name=value`. `VarkaShapeCacheSuite` reflects over the record's components on purpose, so that a
forgotten field fails it, and stays.

**Why each option exists**, read off its own javadoc, falls into five kinds: a reference form kept
for its A/B (most switches, the trunc and mod-7 lowerings); a winner that depends on the machine
(the division lowering, the AVX level, the lane count); a size knob that tests and benchmarks vary
(the four budgets); a check kept switchable so its cost can be priced, off being wrong
(`checkIntOverflow`, `guardDayProducers`); and a fault injector (the three `misdescribe`
options). None is retired today; an option VARKA-247 retires is removed, not marked.

**Scope.** The row also asks for the suites to run under the options' configurations with a
reasoned skip list, and for a shape marked as declining to fail when it starts to fuse. That is a
test harness of its own on top of the table, so it is this task's second step, its own pull request.

## 3. The design

### 3.1 Step 1: the table

`VarkaEmitOption`, a sealed interface of three records - `Flag` (a boolean), `Count` (an int) and
`Choice` (an enum) - each holding the option's name, its `Reason`, its default, its accessor on the
record and its setter on the builder, how it renders into `canonical()`, and for ints the values the
fuzzer draws and the inventory audits. `VarkaEmitOption.TABLE` lists all 40 in the record's
declaration order. From it:

* `DEFAULTS` stays a positional row, and a test checks each of its values against the table
  (see 9.1: building it from the table costs every executor about 17 ms at startup).
* `canonical()` joins the positional options and appends the tags, in table order.
* The bytes suite's inventory is every option's audit values: both values of a flag, every
  constant of a choice, a count's listed values.
* The IR fuzzer draws each option from its kind, in setter-name order, consuming the random stream
  as the reflection did.
* `VarkaEmitDump` parses `name=value` through the table.

The record, its validation, its builder and its `with*` methods stay: every caller keeps its API.
**Considered and set aside:** generating the record from the table (an annotation processor or a
source generator), more machinery than 40 entries need.

### 3.2 Step 2: the configuration matrix

The Varka suites rerun once per *configuration*: the defaults with one option changed, applied
to every kernel the suites emit - `cse=false`, `groupBudget=8`, `division=DOUBLE_DIV`,
`lanesOverride=4`. A reference form or a machine's alternative must give the defaults' answers
under every test, where today it meets only the few tests that set it. The idea is DuckDB's
`test/configs`, and the declining marker is Druid's `cannotVectorize` (`m7/READING.md` 3).

**3.2.1 The configurations.** Every non-default arm of every table entry but the fault
injectors: a flag's other value, a choice's other constants, a count's audit values, and
`lanesOverride` at 4 and 16. The table gives 43 arms today, plus the two lane counts: 45
configurations (28 reference arms, 6 machine, 9 knob, 2 priced checks). They are derived from
`VarkaEmitOption.TABLE` and named as `canonical` `name=value` pairs, so a new option joins the
matrix with its entry.

**3.2.2 Reaching every kernel.** One base value, `VarkaMatrix.base` in the catalyst test jar,
read from `-Dvarka.matrix.config=<name=value,...>` and `DEFAULTS` without it:

* the SQL suites: `VarkaColumnarToRowExec`'s existing test hook starts at the base instead of
  `DEFAULTS`, and its nine resets restore the base;
* the emitter suites: `VarkaEmitterTestBase`'s four defaulted parameters, and the test code's
  other `VarkaEmitOptions.DEFAULTS` uses that build a variant (`DEFAULTS.withX(...)`), start from
  the base - a mechanical change over about 300 sites;
* production code is unchanged: `DEFAULTS` stays a constant and no system property reaches it.

The alternative, `DEFAULTS` itself reading the property, is one line and reaches the defaulted
parameters in main code too, but it puts an emitter knob on every production JVM, which the
existing hook's comment keeps off the configuration surface on purpose. A matrix run counts, per
suite, the emissions whose options are not the base; the first run's counts name the paths the
base does not reach, and each is closed or listed.

**3.2.3 What stays out, and why.** Whole suites whose subject is the defaults or the machine, not
an answer: the bytes oracle and the cost audit (they compare with committed output of the
defaults), `VarkaEmitOptionSuite` and the shape-hash pins, `VarkaAssemblySuite` (it reads the
JIT's output) and the benchmarks. The list is in the runner with a reason per suite.

**3.2.4 The skip list.** `sql/varka/matrix/skips.tsv`, one line per test a configuration is
expected to break: the configuration, the suite, the test, the kind and the reason. Kind `fails`:
the test is expected to fail, as every ANSI overflow test does under `checkIntOverflow=false`.
`VarkaTestWatchdog`'s test wrapper, which 63 of the 68 Varka suites already run through (the
other five mix it in), runs a listed test, cancels it with the reason if it fails, and fails it
as a stale entry if it passes. Each entry names the smallest configuration that breaks the test,
one option, since every configuration changes one.

**3.2.5 The declining marker.** A configuration that makes the emitter decline everything would
pass every SQL test vacuously: the row path answers instead. So the wrapper reads the shape
cache's build and decline counts before and after each test, and the run records them per test;
the defaults run is configuration zero. A test that fused under the defaults and fuses nothing
under a configuration fails, unless the skip list marks it kind `declines` with a reason - a
single output over a small `methodByteBudget` declines by design (VARKA-87) - and a `declines`
entry whose test fuses fails as stale. VARKA-275, the guard on hidden kernel-failure fallbacks,
is the per-test half of the same concern and lands independently.

**3.2.6 Running it.** `dev/varka_matrix.sh [--config <name=value>]... [--all] [-j N]` builds once,
then runs each configuration as its own ScalaTest runner JVM on the test classpath sbt exports,
N at a time, so parallel runs share no sbt lock or target directory. Each writes a JUnit report
and the per-test counts; the script prints one line per configuration and exits non-zero on any
failure. On the laptop, `-j 8` keeps within memory (83 GB) and within what the 100 W charger
supplies (3 October 2026: twenty JVMs drained the battery and flipped the power profile); a
PR touching an option's code path runs its configurations this way in about one suite run's time.

The full matrix runs nightly on GitHub Actions, `varka-option-matrix.yml` on vecbricks/varka
beside the fuzz nightly: one build job, then shards of configurations, each shard running the
script with `-j 2` on a four-core runner; at about 14 minutes per configuration, 45
configurations are roughly 11 runner-hours. PR CI is unchanged. The first nightly's times set
the shard size.

**3.2.7 Done when.** The nightly runs all 45 configurations green, every red test either fixed
or listed with its reason; the first run's reach counts are closed or listed; the script runs a
single configuration on the laptop; `sql/varka/AGENTS.md` says a new option gets its matrix arms
from its table entry and a broken test a skip line with its reason.

### 3.3 What is deliberately unchanged

Every option, its default, its validation and its `with*` method; `canonical()`'s output for every
value; the emitted bytes; the fuzzer's draws for every seed; `pinnedArms`, the bytes oracle's
user-selectable arms.

### 3.4 Registered op counts

None: no emitted byte moves.

## 4. Files

`VarkaEmitOption.java` (new), `VarkaEmitOptions.java` (`DEFAULTS`, `canonical()`),
`VarkaEmittedBytesSuite.scala` (the inventory), `VarkaIrFuzzSuite.scala` (the draws),
`VarkaEmitDump.scala` (the parser), `VarkaEmitOptionSuite.scala` (new).

## 5. Tests, and what each is for

* `VarkaEmitOptionSuite`: the table names every record component once, in declaration order;
  `DEFAULTS` holds every option's default; each entry reads and sets the component it is named
  for and no other; initialising and rendering the defaults never loads the table; a flipped
  option and a changed count render as the committed renderings do; each reason kind is what the
  table says for the options the javadoc names.
* During development, not committed: the old and new `canonical()` over random option values, and
  the old and new fuzzer draws over many seeds, compared for equality before the old code goes.
* The bytes oracle and the cost audit, unchanged; the Varka suites through the gate.

## 6. The measurement

None.

### 6.1 Predictions, registered before the run

1. `emitted_bytes.json` and `emit_cost_audit.json` byte-identical.
2. The old and new `canonical()` agree on 100,000 random option values, and the old and new draws
   on 10,000 seeds.
3. Adding an option after this lands touches the record component, the builder, a `with*` method,
   the javadoc and one table entry: five places, where it was about nine.

## 7. Risks

1. **A rendering that drifts** would move non-default variants' shape hashes; the side-by-side
   comparison before the old code is deleted is what rules it out.
2. **A static initialisation cycle** between the table and `DEFAULTS`: the table holds method
   references only, so building `DEFAULTS` from it does not reenter the record's initialiser.

## 8. Sequencing

Step 1, one pull request. Step 2 in two: the base value, the wrapper's skip list and counts, the
runner script and the first laptop run with its skip entries; then the nightly workflow, once the
first PR has measured a configuration's time on the laptop.

## 9. Outcome

### 9.1 Step 1, the table, 4 October 2026

1. **Held.** `emitted_bytes.json` and `emit_cost_audit.json` are byte-identical.
2. **Held.** The table-built `DEFAULTS` equals the positional one; the table's `canonical()` agrees
   with the hand-written one on 100,000 random option values of 100,000; and the table's fuzzer
   draws agree with the reflection's on 10,000 seeds of 10,000, the random stream left at the same
   position after each, so every fixed seed draws what it drew. The comparisons ran with the old
   code beside the new and were deleted with it.
3. **Held, counting the builder as one place.** Adding an option is now the record component, the
   builder (its field, setter, copy in `toBuilder` and argument in `build`), its `with*` method, its
   javadoc, its value in `DEFAULTS` and one table entry; `canonical()`, the inventory, the fuzzer
   and the dump's parser follow from the entry. Before, each of those four was an edit too.

`dev/varka_gate.sh` passed every step. Two things the table needed that the plan did not foresee:
Scala cannot express a Java enum's bound, so `Choice` gained `withIndex` and `withNamed`, which
are what the fuzzer and the dump call; and the bytes suite's hand-written inventory had fallen six
options behind the record, which reading it off the table repairs. `VarkaEmitDump` was run end to
end with options of every kind and still refuses a boolean that is neither `true` nor `false`.

The review of the pull request changed four things. `DEFAULTS` went back to its positional row:
building it from the table linked the table's 80-odd method references when the record
initialised, which a fresh JVM timed at 17 to 26 ms (median about 19 ms over fourteen runs, with
and without lambdas linked beforehand) against 4 ms for the record itself, and every executor
would have paid it for options only tests vary. `VarkaEmitOptionSuite` checks the row against the
table and shows, in a class loader of its own, that the record and its rendered defaults never
load the table. A second test moves each entry off its default and checks that exactly the
component it is named for moved, which the defaults test could not see, since it reads each option
through its own entry; a deliberately miswired setter fails it. `lanesOverride` is a knob, not a
machine-dependent winner: production always uses the preferred width, and the override exists so
one JVM's suites can emit for every width.
