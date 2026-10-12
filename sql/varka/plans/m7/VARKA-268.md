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
* the sweeps, failing with an `AssertionError` that names the first days that differ;
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
