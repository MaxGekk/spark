# VARKA-268: The emitter suites' helpers in Java

## 1. Where this came from

Row 268 of `m7/PLAN.md`, optional, from the plan review (2.3): `VarkaEmitterTestBase`'s helpers to
Java, the suites thin wrappers over them. The "Java code" rules in `sql/varka/AGENTS.md` say test
helpers are Java and the suites thin Scala wrappers over them, and port code a change touches.
`VarkaEmitterTestBase` is the largest Scala helper left in the emitter's tests: a 787-line trait
that 18 suites mix in, holding

* the emitted class built and loaded (`emit`, `emitMulti`, `load`, `methodNames`);
* the input and output columns over Arrow-shaped memory (`Col`, `makeInputData`, the poisoned
  null slots, `makeOutput`, their long-lane twins, `assertSameOutput`, `runKernel2`);
* the data the suites drive: lengths, null patterns, the calendar boundary days, the ISO-week
  corners, the `make_date` triples, the extreme and small spreads;
* three sweeps of every day of the narrow range against `DateTimeUtils` and `LocalDate`
  (`sweepTrunc`, `sweepCalendar`, `sweepLastDay`);
* the two matrices, `checkMatrix` and `checkLongMatrix`, which run a shape over every length and
  null pattern against `VarkaReferenceEvaluator`.

The row is done when the helpers' code is Java and the trait is a thin layer over it, with every
suite passing as before.

## 2. The admission check, done

**What the suites lean on** (12 October 2026, counted over the 18 suites). The Scala shapes are
everywhere: `emitMulti(...)._2` and output tuples' `._2` 252 times, `._1` 59; `checkMatrix` 110
calls, `combos` 66, `nullPatterns` 59, `makeOutput` 41, `makeInputData` 36, `laneOps` 41, `extreme`
46; default arguments on `emit`, `emitMulti`, `makeInputData`, `makeOutput` and `checkMatrix`; and
Scala functions (`Int => Boolean`, `Int => Int`, `(Int, Int) => Int`) as the null patterns and data.
Porting the suites to Java types would touch thousands of lines of 18 files no task is touching,
which the porting rule does not ask for.

**What stays Scala.** `checkMatrix` and `checkLongMatrix` drive `VarkaReferenceEvaluator`, a Scala
object over `Seq[Option[Int]]` rows, and are Scala for the reason `VarkaKernelCheck` is.

**The sweeps' time today**, under `-Dvarka.sweep=true` on the laptop, each suite in its own JVM:
the trunc sweep 9.1 seconds, the calendar sweep 30.5, the last-day sweep 1.2
(`VarkaEmitterChronoSuite`), and the division suite's test that runs all three under both double
division forms 117 seconds.

## 3. The design

### 3.1 Java

`VarkaEmitterFixtures` (`sql/catalyst/src/test/java/.../codegen/varka/`), a final class of static
methods and records, beside `VarkaEmitterTestSupport`, which reads class files back and stays as
it is:

* `Emitted(String className, byte[] bytes)`, `emit(roots, numInputs, numLiterals, options)` naming
  the class from one counter, `load(Emitted)` returning `Loaded(VarkaFusedKernel kernel,
  VarkaGeneratedClassLoader loader)`, `methodNames(Emitted)`;
* `Col(MemorySegment data, MemorySegment validity, int nullCount)` with `validityAddress(length)`;
  `makeInputData(arena, length, IntPredicate isNull, IntUnaryOperator value, boolean poisonNulls)`,
  `poison(nullOrdinal)`, `makeOutput`, `makeLongInput`, `makeLongOutput`, `outputValidityBytes`,
  `assertSameOutput`, `runKernel2`;
* the sweeps, failing with an exception that names the first day that differs;
* the data: `LENGTHS`, `OFFSETS`, `CALENDAR_BOUNDARY_DAYS`, `ISO_WEEK_DAYS`, `MAKE_DATE_ALL` as a
  record `Triple(int year, int month, int day)`, `extreme`, `small`, `calendarDays`,
  `defaultData`, `laneOps`, `monthStepOps`, `singleEpilogueSize`.

Records, `var` where the right side names the type, and no allocation inside the sweeps' row loops,
as the rules ask.

### 3.2 The trait

`VarkaEmitterTestBase` keeps every member's name and Scala signature, so no suite changes: each
member forwards to `VarkaEmitterFixtures`, adapting the Scala shapes - a tuple from a record, a
Scala function to an `IntPredicate`, a default argument - and `checkMatrix` and `checkLongMatrix`
stay whole, building their columns through the Java helpers. A suite is ported to Java types when
a task touches it, as the rule says.

### 3.3 What is deliberately unchanged

* The 18 suites, line for line.
* `VarkaKernelCheck` and the reference evaluator.
* The data's values and the order a generator yields them: a suite's cases are the same cases.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/src/test/java/.../codegen/varka/VarkaEmitterFixtures.java` | the helpers, 3.1 |
| `sql/catalyst/src/test/scala/.../codegen/varka/VarkaEmitterTestBase.scala` | the thin trait, 3.2 |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-268.md` | the row and this plan |

## 5. Tests, and what each is for

* **Every emitter suite, unchanged**, at both widths in the gate, with the same test names and
  counts: the port changes no case.
* **The data equal**, checked once before the Scala bodies go: a throwaway test that compares
  every table and every generator over a range of indices, Scala against Java, element for
  element; recorded in 9, not committed.
* **The sweeps under `-Dvarka.sweep=true`**, before and after, for 6.1's prediction.

## 6. The measurement

The sweeps' times, as 2 measured them, after the port.

### 6.1 Predictions, registered before the run

1. **Every suite passes with the same test count**, at both widths.
2. **The trait shrinks to under 350 lines**, `checkMatrix` and `checkLongMatrix` most of it.
3. **The sweeps take the same time within 10%.** The row loops are the same loops; the Scala ones
   already avoided allocation per row, except `sweepCalendar`'s `Seq` per day, which the Java form
   drops, so the calendar sweep may come out faster.

## 7. Risks

1. **A table or generator that changes in translation** - an index, an operator's precedence,
   integer `%` on a negative - changes a suite's cases silently while it still passes. 5's
   equality check is what catches it.
2. **Default arguments and overloads.** A Scala default argument has no Java form; the trait keeps
   the defaults, and the Java methods take every argument.
3. **Assertion messages.** A Java helper fails with an `AssertionError` rather than ScalaTest's
   `TestFailedException`; ScalaTest reports both as a failure with the message, so a failure still
   names the case.

## 8. Sequencing

1. This plan, and the row marked Planned.
2. `VarkaEmitterFixtures`, the trait over it, and the equality check run, in one commit.
3. Section 9.

## 9. Outcome

Done on 12 October 2026, in one commit after the plan.

**What was built.** `VarkaEmitterFixtures` (Java, 600 lines) holds the helpers: the emitted class
built and loaded (`Emitted`, `Loaded`), the IR chains, the columns and outputs (`Col`, `Output`),
the data tables and generators, the cost readings, and the three sweeps, which share one chunk
loop and fail on the first day that differs. `VarkaEmitterTestBase` is down from 787 lines to 465:
every member keeps its name and Scala signature and forwards to the Java class, adapting a tuple,
a Scala function or a default argument; `checkMatrix`, `checkLongMatrix` and `combos` stay whole,
as do `tripleData` (over the suites' Scala triples) and the three option values, which read
`VarkaMatrix.base`. No suite changed.

**The data equal.** Before the trait's bodies were replaced, a throwaway suite compared every
table and generator, Scala against Java, over indices 0 to 4999 and every column, and the
columns' bytes for every null pattern, length and poisoning: 152,870 comparisons, all equal.

**The suites.** The 17 suites that mix the trait in, on this branch and on master the same
hour: the same 249 test names with the same outcomes, 232 passed and 17 cancelled (the opt-in
ones).

**Predictions scored.**

1. **Held.** Every suite passes with the same test count (the gate's two widths recorded below).
2. **Wrong.** The trait is 465 lines, not under 350: the forwarders kept their documentation, and
   the long-lane matrix and the type adaptations add more than estimated.
3. **Held.** The sweeps take the same time: trunc 8.1 seconds against 9.1, calendar 29.5 against
   30.5, last day 1.2 against 1.2, and the division suite's sweep of all three under both division
   forms 114 against 117, each suite in its own JVM on the laptop.

**Corrections to the plan.** 3.1 put the trunc levels through `DateTimeUtils`; they are
`private[sql]` Scala values, read from Java through `DateTimeUtils$.MODULE$`, as
`VarkaChronoCompiler` does. `makeDateNull` keeps its `MakeDate` type, which a suite reads. The
suites that mix the trait in are 17, not 18: the eighteenth file named in 1 only mentions it.
The Java helpers fail with an `IllegalStateException`, not 7.3's `AssertionError`: Spark's
checkstyle refuses a thrown `Error`, as the other Java test helpers already follow, and ScalaTest
reports either as a failure with its message. The first gate's lint step caught it.
