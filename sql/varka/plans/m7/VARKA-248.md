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

* `DEFAULTS` is built by name from each option's default, not from a positional row.
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

The suites under each option's non-default configuration, a committed skip list with a reason per
entry naming the minimal option delta that fails, and a declining-shape marker that fails when the
shape starts to fuse (`m7/READING.md` 3, DuckDB's `test/configs` and Druid's `cannotVectorize`).
Planned in this file before its code, once step 1 has merged.

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
  `DEFAULTS` holds every option's default; a flipped option and a changed count render as the
  committed renderings do; each reason kind is what the table says for the options the javadoc
  names.
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

Step 1, one pull request; step 2 planned here, then its own pull request.

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
   javadoc and one table entry; `DEFAULTS`, `canonical()`, the inventory, the fuzzer and the dump's
   parser follow from the entry. Before, each of those five was an edit too.

`dev/varka_gate.sh` passed every step. Two things the table needed that the plan did not foresee:
Scala cannot express a Java enum's bound, so `Choice` gained `withIndex` and `withNamed`, which
are what the fuzzer and the dump call; and the bytes suite's hand-written inventory had fallen six
options behind the record, which reading it off the table repairs. `VarkaEmitDump` was run end to
end with options of every kind and still refuses a boolean that is neither `true` nor `false`.
