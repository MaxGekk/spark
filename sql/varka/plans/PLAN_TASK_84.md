# Task 84: one value-range lattice, instead of two overlapping analyses

*Milestone 5, section 2.15. Opened 8 September 2026 from task 63's review; planned
15 September 2026 as the first task of the milestone's spine after the sync (117).*

## 1. Where this came from

Two traversals in `VarkaExpressionCompiler` compute overlapping facts about what a
node can hold. `dayRange` (line 1404) answers "which epoch days can this subtree
produce", for admitting a calendar node's child; `intBound` (line 940) answers "how
large can this int be in absolute value", for taking the check off a checked
operation. They duplicate the literal-slot lookup, they disagree about what a
runtime guard proves, and `intBound`'s `datediff` arm has to call `dayRange` with
`guarded = false` to bridge them. Task 63's review found three wrong answers in
the compiler and two of them sat in that seam: a bound that assumed the date
contract for operands a literal shift had pushed out of int range, and nested
bounds combined with wrapping `Long` arithmetic, so a product past 2^63 came back
small and positive and "proved" a checked operation safe. Both were fixed by hand
- `exactly` and `withinInt` are those fixes - and nothing prevents the next arm
from forgetting either discipline, because there is no place where the
discipline lives.

Milestone 5 makes this urgent rather than tidy. Task 85 parameterises the emitter
on a lane, and the lane descriptor has to carry an interval type; every 64-bit
decision after it is a range query - whether a `TIME` division is exact, whether
an interval extract needs its bound, whether `make_time`'s multiply keeps its
check, whether `date - date` can widen unguarded. Writing a third bound function
for `long` beside these two would give the new lane the bugs the review found.
The owner confirmed 84 before 85 on 8 September for exactly that reason.

## 2. The admission check, done

**What the two functions actually compute, read rather than remembered.**

`intBound` returns `Option[Long]`, a magnitude: a literal is its absolute value;
the calendar fields are fixed constants - `year` 40 000, `month` 12, `dayofmonth`
31, `quarter` 4, `dayofyear` 366, `weekofyear` 53, `dayofweek` 7, `weekday` 6,
`dayofweek_iso` 7; `datediff` is the widest difference of its operands' day
intervals, both asked *unguarded* and refused unless both fit int32; `IntArith`
combines its operands' magnitudes with `multiplyExact` for `MUL` and `addExact`
otherwise, returning `None` on `Long` overflow; `IntNeg` passes its child through;
everything else - a column, a date-valued node used as an int - is `None`. Its
one consumer that matters is `cannotOverflow`, which builds the candidate
`IntArith` and asks whether its magnitude stays at or under `Int.MaxValue`.

`dayRange` returns a `DayRange` - `Bounded(lo, hi)` or `Unknown` - under a
`guarded` flag with no default: a `ColumnRef` is the contract range; a literal is
itself; a literal day shift moves the interval by exactly its value; `next_day`
by 1 to 7; `add_months(n)` by 28n to 31n in whichever order, and a column month
count by 31 times the emitter's `MONTH_ARITH_MIN/MAX_MONTHS`; `last_day` by 0 to
30; `trunc` by -365 to 0; `make_date` is the whole years of the narrow range;
`GuardedDay` is the narrow range regardless of its child; `ThursdayOf` is -3 to
3; `greatest`, `least` and `IfElse` take the hull; anything else is `Unknown`. A
*column* day offset is the crux: with `guarded` on it answers the narrow range on
the strength of the runtime guard a calendar consumer arms, with it off it answers
`Unknown`. `guardsBelow` re-arms the flag under a node that is itself a calendar
consumer (`isChrono`'s set plus `AddMonths`), so `datediff(last_day(date_add(d,
i)), d2)` is bounded although `datediff` arms nothing. Its consumers are
`admitCalendar` (asymmetric: `NARROW_MIN_DAYS` below, `NARROW_DECOMPOSE_MAX_DAYS`
above), `rearm`, and `intBound`'s `datediff` arm.

**The callers a replacement must serve**, by line in the compiler today:
`intBound` at 666, 678, 724 (the interval arms), 946 (itself), 982 (the `datediff`
arm), 1006 (`IntNeg`), 1054 (`cannotOverflow`) and 1327; `dayRange` at 971 and 972
(`intBound`'s `datediff`), 1416, 1421, 1441 (its own helpers), 1601 and 1621
(`admitCalendar`); `admitCalendar` at 786, 793 and 1517; `rearm` throughout
1563 to 1638. The runtime-bound registry - `bound(ordinal, lo, hi)`,
`VarkaInputBound`, `inputBounds` at 356 and 427 - is a separate thing: it records
what the emitted guards *enforce*, and the analysis only *reads* that promise. It
stays as it is.

**The oracles already exist, which is what makes this refactor safe to attempt.**
`VarkaExpressionCompilerSuite`: 97 tests, 56 assertions on decline reasons, 7 on
"checked int multiply whose operands do not rule out overflow", 34 on guards and
ranges. `VarkaCoverageSuite`: every row of the coverage table compiles, and
`coverage.json` is byte-compared, so any row whose classification moved fails
it. `VarkaDifferentialSuite`: the fusion classification of every committed query.
And `dev/varka_emit.sh` over the 52 `Surface` and 12 `Chains` entries prints a
verdict and a shape hash per entry, which is a mechanical before/after over the
published inventory. Nothing in this task is measured by a timing; it is measured
by these staying identical.

**The property test 2.15 asks for does not exist.** Nothing today asserts, over
random IR, that the interval a node reports contains what the reference evaluator
computes. `VarkaIrFuzzSuite` has the generator (`randomOptions` and the value
grammar, private to it) and `VarkaReferenceEvaluator.evalValue` has the oracle;
neither has been pointed at a range. Step 1 below writes that test first, against
a thin adapter over today's two functions, so the plan learns whether the current
code passes it before anything is replaced - and section 6.1 predicts the answer.

## 3. The design

### 3.1 The mechanism

Two Java files in `codegen/varka/`, beside `VarkaChrono.java` and
`IntRangeOps.java`, and the compiler's two traversals deleted.

**`VarkaValueRange.java` - the lattice, pure data.** A sealed interface `Range`
with two members: the record `Bounded(long lo, long hi)` and the singleton
`Unknown`. Operations, every one total and every one saturating to `Unknown` on
`Long` overflow rather than wrapping: `shift(lo, hi)`, `hull(other)`, `add`,
`sub`, `mul` (interval arithmetic over the four corner products), `neg`, `abs`,
and the queries `magnitude()` (the larger of `|lo|` and `|hi|`, as an
`OptionalLong`), `fitsInt()`, and `within(lo, hi)`. Saturation is a property of
the type, not a discipline of its callers: `exactly` and `withinInt` cease to
exist because there is nothing left for them to guard. The laws are unit-tested
directly - `hull` is commutative, associative and idempotent; `shift` composes;
`mul` of two ranges contains every product of members; and every operation on
`Unknown` is `Unknown`.

**`VarkaRangeAnalysis.java` - one traversal, two questions as queries.** A single
`range(node, literals, policy)` returning a `Range` for any `VarkaVectorIR`, with
an exhaustive `switch` over the sealed interface - so a node type without a
transfer function refuses to compile, which is the protection the old code did
not have (three bugs, none reachable by a missing arm). The literal table crosses
the Java boundary as a `LongUnaryOperator` from slot index to value, built once
per compile from the `LinkedHashMap` the compiler already keeps.

`policy` replaces `guarded: Boolean` plus `guardsBelow` with an explicit
`GuardPolicy`: `ARMED` - a calendar consumer above has armed the producer guards,
so a column-offset producer answers the narrow range - and `NONE` - nothing is
armed, so it answers `Unknown`. A calendar-consumer node re-arms the policy for
its subtree exactly where `guardsBelow = true` does today. Making the policy a
parameter of the *query* rather than a fact baked into one traversal's arms is
2.15's second property: the calendar admission and the overflow check need the
same distinction and only one of them had it.

The two old questions become two queries:

* `dayRange(node)` is `range(node, literals, ARMED)` from `admitCalendar` and
  `rearm`, and `range(node, literals, NONE)` where `intBound`'s `datediff` arm
  asked unguarded - the transfer functions reproduce today's shifts and constants
  exactly.
* `intBound(node)` is `range(node, literals, NONE).magnitude()`, with the
  calendar fields' magnitudes as transfer functions of the field nodes - the
  same nine constants - and `datediff` computed as `sub(end, start)` of the two
  operands' `NONE` ranges, refused unless both fit int32, which is today's rule
  stated as interval arithmetic instead of four `abs` calls.

`cannotOverflow`, `admitCalendar`, `rearm` and the interval arms keep their
signatures and call the analysis; `DayRange`, `Bounded`, `Unknown`, `exactly` and
`withinInt` leave the compiler. `rearm` stays in Scala - it rewrites IR and
belongs with the compiler - and reads `GuardedDay`'s transfer function (the narrow
range, regardless of child) from the analysis like everything else.

**The option not taken, and why.** A Scala-side refactor that merely merges the
two functions would be smaller. It was not taken because the milestone's next task
makes the lane a parameter and the one after that adds `long` values, and both
need a domain whose operations are total over `long` and whose transfer functions
are enumerated by the compiler - which is a Java sealed type and an exhaustive
switch, not a Scala partial function with a `case _ => Unknown` at the bottom.

### 3.2 What is deliberately unchanged

**Every bound is reproduced, not tightened.** The nine field constants, the
28n-to-31n month rule, `trunc`'s -365, `ThursdayOf`'s 3, the unguarded `datediff`
rule, the asymmetric admission limits: all identical, on purpose, because the
admission check is "every shape admitted or declined identically" and a tighter
bound would admit a shape today declined - `year` is really bounded well under
40 000 - and move the oracle. Tightening is a separate task with its own before
and after, and it is entered in the debt register by this one.

The runtime-bound registry, the `GuardedDay` node, `rearm`'s rewrite rule (a node
is re-armed only when its *own* shift is runtime-valued), the decline reasons'
wording, the shape key, and every emitted byte. The IR does not change, so no
shape hash changes, so no committed benchmark number can move.

### 3.3 Registered op counts

Not applicable: no IR or emitter change. The check that stands in for it is
`dev/varka_emit.sh` over the 64 inventory entries printing identical verdicts and
identical shape hashes before and after.

## 4. Files

* `sql/catalyst/src/main/java/.../codegen/varka/VarkaValueRange.java` - new.
* `sql/catalyst/src/main/java/.../codegen/varka/VarkaRangeAnalysis.java` - new.
* `sql/catalyst/src/main/scala/.../codegen/VarkaExpressionCompiler.scala` -
  `intBound`, `dayRange`, `DayRange`, `Bounded`, `Unknown`, `exactly` and
  `withinInt` removed; `cannotOverflow`, `admitCalendar`, `rearm`, the `datediff`
  arm and the interval arms call the analysis.
* `sql/catalyst/src/test/scala/.../codegen/varka/VarkaRangeAnalysisSuite.scala` -
  new: the lattice laws, the transfer functions against hand-computed intervals
  for every node type, and the property test.
* `sql/catalyst/src/test/scala/.../codegen/varka/VarkaIrFuzzSuite.scala` - its
  value grammar extracted to a shared test object so the property test generates
  the same trees the fuzzer does, on the `VarkaSqlResolve` precedent of one
  generator rather than two that drift.
* `sql/varka/plans/PLAN_MILESTONE_5.md` - row 84, and the debt-register entry for
  tightening.
* `docs/sql-varka.md` - the "Key design decisions" paragraph on range analysis, one
  sentence.

## 5. Tests, and what each is for

* **Lattice laws** (`VarkaRangeAnalysisSuite`): saturation on every operation at
  both `Long` extremes; `hull` commutative, associative, idempotent; `mul`
  containing every corner product; `Unknown` absorbing. These are the discipline
  task 63 added by hand, now asserted of the type.
* **Transfer functions, one test per node type**, each against an interval
  computed by hand from today's rule - so the "reproduced, not tightened" promise
  is a test, not a review note. Includes the policy: a column day offset is
  `Unknown` under `NONE` and the narrow range under `ARMED`, and a `last_day`
  above it re-arms.
* **The property test**, the one 2.15 says the task is not done without: over
  random IR from the shared grammar, random literal tables and random lane rows
  including nulls, at every node of every tree, `range(node, NONE)` contains
  `evalValue(node, row, lits)` wherever that is defined; and under `ARMED`, the
  same for every row in which each column-offset producer's own result lies in
  the narrow range - which is precisely the promise the runtime guard makes and
  therefore the specification of the policy. Ten thousand trees by default, the
  seed printed, replay by `-Dvarka.range.seed`.
* **The oracles, unchanged**: `VarkaExpressionCompilerSuite`, `VarkaCoverageSuite`
  (including the byte comparison of `coverage.json`), `VarkaDifferentialSuite`'s
  classification, and the `dev/varka_emit.sh` before/after over the inventory,
  its output committed under `target/` for the PR and quoted in section 9.

## 6. The measurement

None. This task produces no number and moves none; section 3.3 says how that is
checked.

### 6.1 Predictions, registered before the run

1. **Today's code passes the property test** on the shapes the fuzzer generates.
   2.15 says the test is one "the current code cannot pass"; the prediction here is
   that task 63's `exactly` and `withinInt` closed the holes it would have found
   and the test's value is keeping them closed. If it fails against today's code,
   the failure is a live bug in the compiler, gets its own fix and its own commit
   before any line of the refactor, and this prediction is scored wrong in the
   informative direction.
2. **Zero oracle differences**: 97 compiler-suite tests, 4 coverage checks with an
   unchanged `coverage.json`, an unmoved differential classification, and 64
   identical verdicts and shape hashes from `dev/varka_emit.sh`.
3. **The compiler shrinks by about 150 lines and the Java gains about 250**, the
   difference being the laws stated once and the transfer functions listed
   exhaustively rather than defaulted.

## 7. Risks

* **Tightening by accident.** A transfer function written from the definition
  rather than from today's rule gives a tighter interval, which admits more and
  moves the oracle. The per-node tests in section 5 are written from today's
  constants before the analysis is, so the analysis is made to pass them.
* **The policy's semantics drifting from the guard's.** `ARMED` means "a
  calendar consumer above has armed the producer guards"; the property test's row
  filter is the executable form of that sentence, and a change to the guard's
  promise (task 91 chooses a different bound) changes the filter first.
* **The Java/Scala seam.** The analysis takes a `LongUnaryOperator` for literals
  and returns a Java sealed type the Scala side pattern-matches; Scala 2.13 sees
  Java sealed interfaces as non-exhaustive, so the Scala callers match with an
  explicit `case _` that throws, never one that answers.
* **`rearm`.** It reads intervals mid-rewrite; if the analysis is called on a
  partly rewritten subtree, the policy must be the one the finished tree will
  have. The existing `rearm` tests plus the emit before/after are what catch a
  mismatch, and `rearm` is touched last.

## 8. Sequencing

1. The shared fuzz grammar and the property test, run against a thin adapter over
   today's `intBound` and `dayRange`. Score prediction 1. If it fails, stop and
   fix the bug first.
2. `VarkaValueRange` and its law tests.
3. `VarkaRangeAnalysis` with every transfer function, made to pass the per-node
   tests written from today's rules; the exhaustive switch is what enumerates
   "every".
4. The compiler switched over caller by caller - `cannotOverflow`, the `datediff`
   arm, the interval arms, `admitCalendar`, `rearm` last - with the compiler suite
   run after each; then the old functions deleted.
5. The oracles: coverage suite, differential suite, `dev/varka_emit.sh` before/after
   over the inventory, the full gate.
6. Section 9, the milestone row, the debt-register entry for tightening, and the
   `docs/sql-varka.md` sentence.

## 9. Outcome

*To be written from the oracles and the property test.*
