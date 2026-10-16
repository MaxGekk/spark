# VARKA-318: A kill switch per expression family, named in its messages

## 1. Where this came from

Row 318 of `m7/PLAN.md`, added by the owner on 12 October 2026 from `m9/SCOPE.md` items 24 and 29.
Item 24 notes that Comet lets each expression be switched off on its own
(`spark.comet.expression.<Class>.enabled=false`), and that "a per-expression kill switch is the
escape hatch a user needs when one kernel is wrong in production and the rest are not". The item
also says why this is cheap here: the compiler already declines node by node, and a declined node
with a reason is exactly what a switch produces. Item 29 (`m7/READING.md` 3) supplies the form
the message should take: StarRocks's JIT fails a query with "set jit_level = 0 to disable jit and
retry". Today the only switch is `spark.sql.codegen.varka.enabled`, which turns off all of Varka.
The row is done when each family's switch is documented, a test turns each one off and sees the
family decline with its reason, and the messages name the switch. It builds on VARKA-314b
(#863), which turns Varka's invariant failures into `INTERNAL_ERROR` with a `Varka context:`
record and adds the docs section "When Varka falls back, and when it fails".

## 2. The admission check, done

The question is whether a grouping already exists that a switch can act on in one place, or
whether one would have to be invented. There are two groupings.

* **`coverage.json`'s `family` field** has nine documentation groups, for example "Day
  arithmetic", "Calendar fields", "Months and year-month intervals" and "Choice and nulls". These
  label whole rows, not nodes, and a class can sit in several groups: `Add` appears under three,
  `Cast` under four, `Year` under four. A switch keyed on them would have to decide which group
  owns each node, which means inventing a third grouping.
* **The compiler's chain, `VarkaNodeCompiler.FAMILIES`**, holds the date leaves, then calendar,
  interval, time, condition and int arithmetic. `compileNode` asks each group in order whether it
  claims a node and runs the first claimant. `VarkaFamilyChainSuite` already proves that no node
  of the coverage table is claimed twice. So one test in `compileNode` gates every node of a
  family, and each node belongs to exactly one family.

The chain is the grouping used here. It matches the row's examples: the calendar
(`VarkaChronoCompiler`), the conditionals (`VarkaConditionCompiler`) and the checked arithmetic
(the int arithmetic arm, whose ANSI forms are the checked ones). The date leaves get no switch:
they read a date column or literal, or unwrap an identity cast, and fuse nothing of their own.

The condition family needs two more gates. `VarkaConditionCompiler.compileCond` compiles a
filter's conjuncts without going through the chain (`predicateOnce`), and the class's own javadoc
calls itself "the predicate family", covering the predicates as well as `IF`, `CASE WHEN` and
`coalesce`. With the switch off, then, a filter's conjuncts also decline. The time family has
one entry outside the chain as well: `compileRoot` reaches a `TIME` `StaticInvoke` directly when
it is a root. Both entries get the same gate. A check would have rejected the chain if one node
class were claimed by two families, and the chain suite already rules that out.

The interval and time families also claim column and literal leaves of their lane types. The time
family claims every 64-bit column, `bigint` included. If those leaves were gated, switching time
off would stop `l > 5` over a `bigint` column from fusing. So reads (a `BoundReference` or a
`Literal`) are never gated: a switch covers a family's operations, not the columns they share.

## 3. The design

### 3.1 The switches, and where they travel

There are five boolean session configurations,
`spark.sql.codegen.varka.family.{calendar,interval,time,condition,intArithmetic}.enabled`, all
true by default and all declared in `SQLConf`. Version 5.0.0 follows the house convention: every
`spark.sql.codegen.varka.*` entry is 5.0.0, which is `dev/next_version_candidates.py`'s master
version, since Varka does not ship to `branch-4.x`. The binding policy is `NOT_APPLICABLE`, as on
their neighbours. On the code side they are one Java enum, `VarkaFamily`, with a label, a switch
key, the off-reason and the retry sentence. A test holds every enum key equal to a registered
`SQLConf` entry.

The switch is **not an emit option**, for three reasons. First, it changes which entries the
compiler admits and never what the emitter makes of an IR, so it has no place in the shape hash
or `canonical()`: a kernel of the remaining families is byte-identical. Second, a
`VarkaEmitOption.TABLE` entry would make "calendar off" an arm of the option matrix, and that arm
would decline half the suites' subjects by design. Third, the table's sealed Flag, Count and
Choice cases have no set-valued member. The template's guidance to put new behaviour behind a
`VarkaEmitOptions` switch is about emitter lowerings and does not apply here.

The binding still has to follow `useAVX`'s path. The planning compile and the executor's compile
(`VarkaKernelEvaluator`, `VarkaFilterEvaluator`) must classify alike, or the plan and the task
would disagree about which entries fused. So `VarkaSessionEmit`, the record the columnar rule
resolves from the `SQLConf` once and the plan carries to the evaluators, gains `familiesOff`. Each
compiler entry point gets an overload that takes the set beside the `VarkaEmitOptions`, and the
set reaches `DeclineSink`, which already carries the one compile option the arms read,
`rangeSets`. The existing overloads pass the empty set, so no suite changes.

**The decline.** In `compileNode`, a family that claims a node while its switch is off declines
the node with `offReason()`, for example "Varka's calendar family is switched off
(spark.sql.codegen.varka.family.calendar.enabled=false)". The reason is the entry's first note,
so the verbose `EXPLAIN` and the fusion report show it as they show any decline.

**The planning error.** A throw out of a family's lowering is caught once, at the innermost
`compileNode` that ran the arm, and carried to the planning catch, which adds the family to the
record. The `INTERNAL_ERROR`'s sentence then ends with `retry()`: "To plan the query with Varka's
calendar family on Spark's row engine, set spark.sql.codegen.varka.family.calendar.enabled to
false and retry." This is StarRocks's sentence. The exception type the site raised, the cause and
the record are as VARKA-314b left them.

**The context record.** `VarkaErrorContext` gains a `families` field. Its line,
`families: calendar (spark.sql.codegen.varka.family.calendar.enabled), ...`, lists the families
whose nodes the failing kernel holds (at planning, the family that threw). A kernel or emission
failure's warning therefore names the switches that would keep that kernel's work off Varka. The
compiler records the families of each compiled projection's accepted entries in
`CompiledVarkaProjection.families`, following the sink's existing mark-and-truncate rollback.

**The runbook.** In `docs/sql-varka.md`, the configuration list gains the five switches, each
with the expressions it covers. "When Varka falls back, and when it fails" gains a step: for a
family that is wrong or keeps failing, set its switch, named on the decline, the error and the
`families:` line.

### 3.2 What is deliberately unchanged

* The date leaves and every read stay unswitchable, as explained in 2.
* A switch covers the nodes its family claims. Code of one family called directly by another's
  lowering runs under the caller's switch: the calendar's interval operand
  (`VarkaIntervalCompiler.yearsToMonths`, `intervalOperand`) and `greatest`/`least` folding
  through `VarkaConditionCompiler.foldPick` under int arithmetic. The owner may overrule this.
* The emitter, the IR, every kernel's bytes and `emitted_bytes.json` do not change.
* `coverage.json`'s nine groups stay as they are. Whether the coverage table should gain a column
  naming each row's switches is a question for the owner (section 9).

### 3.3 Registered op counts

None. No kernel changes.

## 4. Files

| file | what |
|---|---|
| `SQLConf.scala` | the five switch entries and their accessors |
| `codegen/varka/VarkaFamily.java` (new) | the families, their keys and their message sentences |
| `VarkaNodeCompiler.java` | the gate and the failure catch in `compileNode`; `Family` carries its `VarkaFamily` |
| `DeclineSink.java` | the families that are off, the families used, with rollback |
| `VarkaExpressionCompiler.java` | the overloads; the time-root and predicate gates; the family on the planning catch |
| `CompiledVarkaProjection.java` | `families` |
| `varka/VarkaErrors.java`, `varka/VarkaErrorContext.java` | the retry sentence; the `families:` line |
| `VarkaSessionEmit.java` and the exec nodes, evaluators and fusion report | `familiesOff` bound once and passed beside the options; the families on the evaluator's record |
| `docs/sql-varka.md` | the switches, the runbook step |
| `VarkaFamilySwitchSuite.java` (new, catalyst) | section 5 |
| `VarkaFamilyChainSuite.scala`, `VarkaErrorContextSuite.scala` | the sink's constructor; the end-to-end switch test |

## 5. Tests, and what each is for

* **`VarkaFamilySwitchSuite`** (JUnit, `@VarkaSuite`, catalyst), compile-only:
  * for every family and every coverage row: with that family off, a row whose fused nodes include
    the family declines with `offReason()`, and every other row compiles exactly as it does with
    every switch on; every family has at least one row that declines. This would catch a family
    gated in one entry point but not another (the time root, the filter path), and a gate that
    leaks into a neighbour.
  * the enum's keys are `SQLConf`'s, so a renamed key cannot leave the messages naming a setting
    that does not exist.
  * a failure planted inside a family's lowering raises `INTERNAL_ERROR` whose message ends with
    that family's `retry()`, keeps the site's Java type, and renders `families:`.
  * a reads-only check: with the time family off, `l > 5` over a `bigint` column still fuses.
* **sql/core, `VarkaErrorContextSuite`**: `SET` each switch to false on a query over the Arrow
  cache and check that the answer equals Spark's, the verbose `EXPLAIN` shows the off-reason, and
  the query still fuses another family's entry. Then check that a planted kernel failure's warning
  carries the `families:` line naming the kernel's switch.
* No pinned fixture moves: kernels are unchanged, and the defaults turn nothing off.

## 6. The measurement

There is no benchmark, since no kernel changes. The cost is test time.

### 6.1 Predictions, registered before the run

1. The catalyst suite takes under 5 s. That is about 100 coverage rows times six configurations,
   compile-only, at a few milliseconds each, priced at the calibration table's ratio of about 2
   for new suites (VARKA-243, VARKA-244).
2. The sql/core additions take under 10 s of test time: five short queries and one planted
   failure over one cached view, against VARKA-315's 4.5 s for eight queries.
3. No suite other than the two edited ones changes its outcome.

## 7. Risks

1. A family entry point the gate misses. The per-row test over the coverage table, with both
   projection and predicate rows, would show it.
2. The planning and executor compiles disagreeing because one path drops `familiesOff`. The
   sql/core test runs through the real evaluator, where a disagreement shows as a different
   classification.
3. The failure carrier changing an exception type a caller catches. The planted-failure test
   asserts the type, and `VarkaInternalErrorSuite` runs unchanged.

## 8. Sequencing

1. This plan and the row.
2. The switches, the compiler gates and the messages, with both suites and the docs.
3. Section 9 and the row marked done, after the review.
