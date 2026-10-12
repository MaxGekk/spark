# VARKA-310: The reference evaluator in Java

## 1. Where this came from

Row 310 of `m7/PLAN.md`, added by the owner on 12 October 2026 with rows 311 and 312 (`PLAN.md`
2.3, "The tests' helpers to Java"). Catalyst's Varka tests are 80 Scala files to 15 Java, and the
"Java code" rules in `sql/varka/AGENTS.md` port a helper when a task touches it. The largest
helpers left - `VarkaKernelCheck`, `VarkaVariantCheck`, `VarkaGuardSpec` - stay Scala for one
reason: they drive `VarkaReferenceEvaluator`, a Scala object. This row ports that object; 311 ports
the three helpers over it.

`VarkaReferenceEvaluator` is the oracle the emitted kernels are checked against row by row: an
independent implementation of the IR's semantics - three-valued conditions, the blend,
null-skipping `greatest`/`least`, the calendar through `java.time` and `DateTimeUtils` rather than
through `VarkaChrono` - so that the emitted bytes are held to the definition and not to the model
they were derived from. `VarkaSparkOracle` in turn holds it to Spark's own interpreted `eval` on
every fuzzed row (VARKA-276).

The row is done when the evaluator's code is Java, the Scala object is a forwarder over it with
every caller unchanged, and the two answered alike on the fuzz grammar's trees before the Scala
body went.

## 2. The admission check, done

**The object** (12 October 2026). 335 lines, four entry points:

* `evalValue(node, row: Seq[Option[Int]], lits: Array[Int]): Option[Int]` - every int-lane node;
* `evalCond(cond, row, lits): Option[Boolean]` - Kleene logic, `None` for unknown;
* `evalLong` and `evalCondLong` - the long-lane subset VARKA-85 ships, which throws on any node
  outside it rather than answering.

**The callers.** Six files call it, all in catalyst's test tree, none in `sql/core`:
`VarkaKernelCheck` (8 call sites), `VarkaEmitterTestBase` (4 forwarders, which its 17 suites reach
as `evalValue` and kin), `VarkaGuardSpec` (4), `VarkaVariantCheck` (2), `VarkaRangeAnalysisSuite`
(2) and `VarkaEmitterLongLaneSuite` (1, through the base's `evalLong`). Three more name it in
documentation only (`VarkaSmallTreeSuite`, `VarkaIrFuzzSuite`, `VarkaSparkOracle`). So the Java
class lands in catalyst's test tree beside `VarkaEmitterFixtures`.

**What the Scala form leaves unchecked.** `evalValue`'s match has no default: a node it does not
name is a `MatchError` at run time, found only when a fuzzer draws it. `evalLong` ends in
`case other => throw`, so a node added to the IR is silently "unsupported at the long lane" until
someone notices. A Java `switch` over the sealed `VarkaVectorIR` with no `default`, as the rules
ask, makes a new node a compile error in all four entry points.

**What a row is.** Every caller builds a row as `Seq[Option[Int]]` (or `Long`) per row and calls
the evaluator once per output; the evaluator allocates an `Option` at every node it visits.

## 3. The design

### 3.1 Java

`VarkaReference` (`sql/catalyst/src/test/java/.../codegen/varka/`), a final class of static
methods. The name differs from the Scala object's because the two share a package for as long as
the forwarder lives; 311 moves the callers to it and the object goes.

* `OptionalInt value(VarkaVectorIR node, int[] row, boolean[] isNull, int[] lits)` and
  `Truth cond(Cond cond, int[] row, boolean[] isNull, int[] lits)`;
* `OptionalLong longValue(...)` and `Truth longCond(...)` over `long[]`;
* `enum Truth { TRUE, FALSE, UNKNOWN }` with `isTrue()`, Kleene's three values named rather than an
  `Optional<Boolean>`.

Each entry point is one exhaustive `switch` with record patterns and no `default`. A node with no
meaning on a lane - a calendar node at the long lane, `NarrowLane` as an int-lane value, a
condition in a value position - is a `case` that throws with the reason the Scala form gives, so
the set of refusals is spelled out and a new node has to be placed in it.

The semantics are the Scala form's, line for line, including what it deliberately does not do:
the guards pass their child through (a declined batch is the status assertion's business), a
checked operation that overflows has no value (`Math.addExact` and kin define overflow, and their
throw is caught), `make_date` is null where `LocalDate.of` rejects the triple, and each calendar
node is the `java.time` or `DateTimeUtils` definition its Scala case cites. The comments that say
why travel with the cases.

### 3.2 The forwarder

`VarkaReferenceEvaluator` keeps its four methods and their Scala signatures, each converting the
row once - `Seq[Option[Int]]` to the values and the null flags - and the answer back to an
`Option`. No caller changes.

### 3.3 What is deliberately unchanged

* The six callers and every suite through them.
* `VarkaSparkOracle`, which holds the reference to Spark: it now holds the Java form.
* The semantics: no case is corrected in passing. A disagreement found by 5's check is a bug in
  the port, unless it is a bug in the Scala form, which is then its own row.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/src/test/java/.../codegen/varka/VarkaReference.java` | the evaluator, 3.1 |
| `sql/catalyst/src/test/scala/.../codegen/varka/VarkaReferenceEvaluator.scala` | the forwarder, 3.2 |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-310.md` | the row and this plan |

## 5. Tests, and what each is for

* **The two forms agree**, checked before the Scala body goes: a throwaway suite draws shapes from
  `VarkaIrGrammar` - `drawShape` and `drawWideShape` at the int lane, `drawLongShape` and
  `drawWideLongShape` at the long lane - with lane values drawn as the fuzzer draws them plus the
  lane's edges (`MIN_VALUE`, `MAX_VALUE`, 0, -1) at every null pattern, and compares the two
  evaluators on every output of every row: the same value, the same null, or the same exception
  class. Recorded in 9, not committed, as 268's data check was.
* **Every suite that reaches the evaluator**, unchanged, at both widths in the gate: the IR and
  variant fuzzers, the Spark differential (whose `VarkaSparkOracle` check now holds the Java form
  to Spark), the guard edge suite and the emitter suites.
* **A planted fault** in the Java form (one calendar case off by one day) must fail the fuzz suite
  and the agreement check, so the forwarder is shown to be on the path.

## 6. The measurement

The IR fuzz suite's time at its default iteration count, before and after, in its own JVM.

### 6.1 Predictions, registered before the run

1. **The two forms agree on every row**: at least 10,000 shapes per lane, no disagreement.
2. **Every suite passes with the same test count**, at both widths.
3. **The fuzz suite is no slower than 5%**: the forwarder converts a row once per output, which
   costs about what the Scala form's per-node `Option`s cost; the Java form allocates an
   `OptionalInt` per node visited, the same count.
4. **The evaluator is about 300 lines of Java** and the forwarder under 60 lines of Scala.

## 7. Risks

1. **A semantic slip in translation** - Scala's `%` against `Math.floorMod`, an `Int` widened to
   `Long` where the Scala form wrapped, a `for` comprehension's short-circuit order reversed so an
   exception fires where the Scala form returned `None` first. 5's agreement check, with the edges,
   is what catches it; an exception's class is compared, not only a value.
2. **Evaluation order and exceptions.** The Scala `for` evaluates the left operand first and stops at
   its `None`; a right operand that would throw (a `truncDate` on a bad level) is never reached.
   The Java form keeps the order and the short circuit, case by case.
3. **Forwarder cost.** If prediction 3 fails, the row conversion is the suspect; 311 removes the
   forwarder from the hot callers anyway.

## 8. Sequencing

1. This plan, and the row marked Planned.
2. `VarkaReference`, the agreement check run against the Scala body, the planted fault, then the
   forwarder - in one commit.
3. Section 9.
