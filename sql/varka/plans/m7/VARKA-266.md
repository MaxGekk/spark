# VARKA-266: Error Prone and NullAway on Varka's Java

## 1. Where this came from

Row 266 of `m7/PLAN.md`, from the plan review (2.3) and `m7/READING.md` 11: Error Prone and
NullAway on Varka's Java packages only; first confirm Error Prone on JDK 25's javac with the
incubator module; its own javac pass; NullAway staged by class with a list that only shrinks,
reasons on suppressions, and checks where Scala calls into Java. The reading's numbers: NullAway
on every build at 1.15 times the compile (pp. 1, 8), staged by package with unannotated classes
trusted (p. 5); Varka's `null`s sit mostly in five classes, and a class filled across passes, as
`Slots` is, cannot use `@Initializer`.

## 2. The admission check, done

Run on 11 October 2026 at `fc056778159`, on the laptop.

**Error Prone runs on JDK 25's javac with the incubator module.** Error Prone 2.50.0 as a javac
plugin (`-Xplugin:ErrorProne`, `-XDcompilePolicy=simple`, ten `--add-exports` and two
`--add-opens` of `jdk.compiler` to the plugin), over the Varka sources against sbt's exported
classpath, with `--add-modules jdk.incubator.vector`. Its with-dependencies bundle lacks the
Checker Framework dataflow jar its analyses load (`io.github.eisop:dataflow-errorprone:
3.41.0-eisop1`); without it the run stops on `NoClassDefFoundError`. NullAway 0.14.2 needs
`jspecify` 1.0.1 and `dataflow-nullaway` 4.2.3 beside it, and refuses to start without
`AnnotatedPackages`.

**The sources, Varka's Java and nothing else:** in catalyst, `codegen/*Varka*.java` and
`DeclineSink.java` (19 files) and all of `codegen/varka` (52); in core,
`execution/Varka*.java` and `execution/varka` (18): 89 files. The engine module
(`sql/varka/engine`) is built by Maven against its own classpath and is left for a later step.

**Error Prone's default checks:** no error; 64 warnings over catalyst's files in 5.7 seconds and
5 over core's, 69 in all:

| check | count | reading |
|---|---:|---|
| `ImmutableEnumChecker` | 19 | `Lane`'s `ClassDesc` and `MethodTypeDesc` fields, which are immutable values the checker does not know |
| `InvalidLink` | 15 | Javadoc links to members that moved or are private |
| `ReferenceEquality` | 9 | each an intended identity: an IR node, a singleton `DataTypes` value, a memo's last argument |
| `UnusedVariable` | 8 | |
| `ArrayRecordComponent` | 3 | records holding arrays, whose `equals` is identity |
| `StringSplitter`, `NonApiType`, `IntLongMath` | 2 each | |
| nine others | 1 each | among them `EnumOrdinal` in `VarkaDerivedInput.key` and `NarrowCalculation` |

**NullAway** with `AnnotatedPackages` over the two packages: 336 warnings in 3.9 seconds, in 28
classes: `VarkaVectorWalk` 86, `Slots` 74, `Analysis` 37, `VarkaChronoLowering` 23,
`VarkaLoopEmitter` 22, `VarkaExecMetrics` 11, `VarkaBodyEmitter` 10, and 21 classes with 1 to 7.

**Where Scala calls into Java:** nine static methods (`VarkaExpressionCompiler.compilePartial`
and `compilePredicate`, `VarkaFusionReport.lines` and `predicateLines`, four of
`VarkaExecMetrics`, `VarkaSelectionBitmap.isSet`) and the three evaluators' constructors, from the
exec nodes. The facade's results are `Optional` since row 305; `VarkaExecMetrics`' fields are
null where a node registered no metric.

What the check would have rejected: a pass that is advisory, which rots; and fixing NullAway's
336 findings in this row.

## 3. The design

### 3.1 The pass

`dev/varka_errorprone.sh`: javac with Error Prone and NullAway over the 89 sources, one run per
module (catalyst's sources against catalyst's classpath, core's against core's), writing class
files to a scratch directory that nothing reads. The four jars are pinned by version and sha256 in
the script and fetched into `build/` on first use, as `build/mvn` fetches Maven; nothing is
checked in. `-Werror`: a new warning fails the pass. It requires JDK 25 and says so, rather than
failing with a stack trace from the `--add-exports` set, which is coupled to the JDK. The
classpath comes from the gate's existing export where there is one, and from sbt otherwise.

### 3.2 Error Prone, clean

The mechanical findings are fixed (`InvalidLink`, `UnusedVariable`, `UnnecessaryParentheses`,
`StringSplitter`, `InvalidParam`, `AlmostJavadoc`, `BadImport`, `StatementSwitchToExpressionSwitch`,
the `NarrowCalculation` and `IntLongMath` casts). `EnumOrdinal` in `VarkaDerivedInput.key`
becomes an explicit code per kind, for the reason VARKA-304 gave each refusal an explicit bit:
reordering an enum must move no key. Where a check's finding is the intended code, it is
suppressed at the site with a reason (`ReferenceEquality`'s identities) or disabled for the run
with a reason in the script (`ImmutableEnumChecker`, whose `ClassDesc`/`MethodTypeDesc` fields are
immutable JDK values the checker cannot see).

### 3.3 NullAway, staged by class

`dev/varka_nullaway_pending.txt` lists the classes NullAway treats as unannotated
(`UnannotatedClasses`): at first the classes that warn today, less the small ones this row fixes
to show the list shrinks (`VarkaExecMetrics` and the JFR events, which annotate their nullable
fields). Every other Varka class is checked. The list only shrinks, mechanically: the script
fails when the file has a line the merge base's does not.

### 3.4 Reasons on suppressions

Every `@SuppressWarnings` naming an Error Prone check or `"NullAway"` in the 89 files carries a
comment on its line or the line above saying why; the script greps for it and fails otherwise.

### 3.5 Where Scala calls into Java

The nine static methods and the evaluator constructors state their nullness: a parameter or
return that may be null is `@javax.annotation.Nullable` (jsr305, already on Spark's classpath, so
no dependency enters the build), and NullAway checks the Java side of each against it.

### 3.6 Where it runs

The gate's lint step, after `dev/lint-java` and `dev/scalastyle`. CI's linters job is Spark's and
is left as it is; a CI step is its own row if the gate is not enough.

### 3.7 What is deliberately unchanged

Every emitted byte and every answer; the 336 NullAway findings in the pending classes, a row per
heavy class later; the engine module.

## 4. Files

| file | what |
|---|---|
| `dev/varka_errorprone.sh` | new: the pass |
| `dev/varka_nullaway_pending.txt` | new: the classes not yet checked |
| `dev/varka_gate.sh` | the pass in the lint step |
| the Varka Java sources with findings | the fixes, the suppressions, the boundary annotations |
| `sql/varka/skills/testing-and-debugging.md` | how to run it and read it |

## 5. Tests, and what each is for

* **The pass is green** with `-Werror` over the 89 files.
* **It fails** on a planted finding of each kind: a new Error Prone warning, a NullAway warning in
  a checked class, a line added to the pending list, a suppression without a reason.
* **The gate**, both widths: the fixes move no byte (`emitted_bytes.json`) and no answer.

## 6. The measurement

The gate's lint step's time before and after.

### 6.1 Predictions, registered before the run

1. The pass is green with `-Werror` after the fixes.
2. The pending list starts at no more than 25 classes, and the script fails a commit that adds one.
3. The lint step's time rises by under 30 seconds.

## 7. Risks

1. **The JDK coupling.** The `--add-exports` set follows javac's internals; a JDK update can break
   the pass. The script pins the expected major version and fails with a message.
2. **A fix that changes emitted bytes.** The fixes are to source-level idioms; the bytes oracle
   would show one that was not.

## 8. Sequencing

1. This plan; row 266 marked Planned.
2. The pass, the pending list (every warning class) and the checks, green by listing.
3. The fixes: Error Prone clean, the small classes out of the pending list, the boundary
   annotated; and the gate's lint step.
4. The outcome.
