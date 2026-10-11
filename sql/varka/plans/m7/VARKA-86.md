# VARKA-86: One operand admission, stated once

## 1. Where this came from

Row 86 of `m7/PLAN.md`, scoped in milestone 5 (`m5/PLAN.md` 2.17, written 8 September 2026),
moved to `m8/SCOPE.md` item 39 and taken by milestone 7 after the Java ports, "so that the
admission is written once in Java and not ported twice". 2.17's observation: four near-copies
decide whether an expression may be an operand (`intOperand`, `compileIntOperand`,
`compileOffset`, `compare`'s `operand`), and the emitter restates their conclusions in its own
shape checks, a second statement in another language. The two drifted once: `date_add(d,
weekday(d2) + 1)` was fused in EXPLAIN and refused at emit time, a ghost fallback.

## 2. The admission check, done

Surveyed on 11 October 2026 at `fc056778159`.

**What 2.17 asked for that has landed since.** Two of its items:

* The day offset's two statements became one predicate. Task 63's review (`576792b8bed`, 8
  September 2026) moved the emitter's list into `VarkaVectorIR.isDayOffsetShape` and gated
  `compileOffset`'s arithmetic arm on it, so the compiler builds only what the emitter takes.
* The widening 2.17 said this row would carry - an `IntegerType` column as a comparison
  operand - landed as task 122 (`cd7900fde2a`, 17 September 2026): `i > 0` compares in the
  kernel.

So the exception 2.17 allowed (one shape moving from residual to fused) has been taken, and this
row moves no decline reason.

**What remains: six operand positions.**

| position | compiler | emitter |
|---|---|---|
| int arithmetic operand | `VarkaNodeCompiler.intOperand` | none: `IntArith` takes any int node |
| int argument (`make_date`'s fields, and others) | `VarkaNodeCompiler.compileIntOperand` | none |
| comparison operand | `VarkaConditionCompiler.operand` | none: `Compare` takes any int node |
| day offset (`date_add`, `date_sub`) | `compileOffset`, gated on `isDayOffsetShape` | `requireDayOffsetShape`, on the same predicate |
| month count (`add_months`, year-month intervals) | `compileMonths` | `requireMonthCountShape`, on the same predicate |
| `next_day`'s weekday | builds a literal slot or the derived column | `requireOffsetShape`, a local pair |

The first three have no emitter check, so their copies are compiler-internal: one leaf rule (an
int literal as a slot, an `IntegerType` column as an int column) written three times, the
copies differing in the order of the two tests and in their decline messages, which the compiler
suite asserts by text.

The last three are the compiler-emitter pairs, and one of them still agrees only by
construction: `compileMonths` never asks the predicate `requireMonthCountShape` applies. Its arms
build a slot, a column, `VarkaIntervalCompiler.yearsToMonths`' checked multiply (an `IntArith`,
or a slot where `arithOver` folds it) and the negated interval's `IntNeg`, all of which the
predicate admits today; an arm that built anything else would fuse in EXPLAIN and be refused at
emit time. That is the drift 2.17 is about, in the one position left open to it.

And `isDayOffsetShape` is a list of `instanceof`s, so a new IR node kind joins or misses each
position silently.

What the check would have rejected: a test asserting that the compiler's set equals the
emitter's, which, with one predicate on both sides, would be true by construction.

## 3. The design

### 3.1 The positions, an exhaustive switch

`VarkaOperandPosition`, a public enum in the `varka` package beside `VarkaVectorIR`, with the
three positions the emitter checks - `DAY_OFFSET`, `MONTH_COUNT`, `WEEKDAY` - and
`admits(VarkaVectorIR)` written as a pattern switch over the sealed IR with no `default`. A new
node kind does not compile until each position says yes or no to it, which is what makes
"widening the compiler widens the emitter or fails to compile" literal. `WEEKDAY` stays the
strictest (a literal slot or a column), for the reason `requireOffsetShape`'s comment gives: its
range comes from a compile-time fold or the derived weekday leaf, with no runtime guard behind
it; the reason moves with it. `isDayOffsetShape` goes.

### 3.2 Both sides through it

* **The emitter**: `Analysis`'s three `require*Shape` become one `requireOperand(position, node,
  name)`, which throws with the operand's name as today.
* **The compiler**: `compileOffset`'s arithmetic arm, every arm of `compileMonths`, and
  `next_day`'s weekday finish through one `admitted(position, compiled, expr, sink)`, which
  declines with a reason where the position does not take what was built.

### 3.3 One leaf rule

`VarkaNodeCompiler.intLeaf(e, inputs, literals)`, the int literal and the `IntegerType` column,
shared by `intOperand`, `compileIntOperand` and the comparison's `operand`, each keeping its own
type rule and decline message verbatim. `intOperand` and `compileIntOperand` differ in nothing
else, so they become one method with the position's text as a parameter for the message.

### 3.4 What is deliberately unchanged

Every decline reason's text; every fused shape; the emitter's acceptance; int arithmetic in
predicate position, which 2.17 keeps out ("a widened leaf is not a widened tree"). No widening
in this row.

### 3.5 Registered op counts

None: nothing emitted changes.

## 4. Files

| file | what |
|---|---|
| `VarkaOperandPosition.java` | new: the three positions and their exhaustive admission |
| `VarkaVectorIR.java` | `isDayOffsetShape` removed |
| `Analysis.java` | one `requireOperand` for the three checks |
| `VarkaNodeCompiler.java` | `intLeaf`; one int operand method |
| `VarkaConditionCompiler.java`, `VarkaChronoCompiler.java`, `VarkaIntervalCompiler.java` | the leaf rule; the positional arms through `admitted` |
| `VarkaFamilyChainSuite.scala` or a new suite | the tests of 5 |

## 5. Tests, and what each is for

* **The fusion dump** (`VarkaCoverageSuite`, `VARKA_FUSION_DUMP`), byte-identical to master's:
  every entry point over every coverage row and seventeen option sets, with each decline's
  reason, so a merged helper that changed a message or a gate that changed a decision shows.
* **`emitted_bytes.json` unchanged**: the emitter side.
* **The fail-fast kept**: per position, the emitter refuses a kind the position does not admit
  (`AddDays` over a `Year`, `AddMonths` over a `DateDiff`, `NextDay` over an `IntArith`), with
  the operand's name in the message.
* **The compiler gates**: over the dump corpus's compiled IR, every positional operand satisfies
  its position's `admits`.
* **Exhaustiveness**: enforced by compilation.
* **The gate**, both widths.

## 6. The measurement

None: nothing emitted changes.

### 6.1 Predictions, registered before the run

1. The fusion dump is byte-identical to master's.
2. `emitted_bytes.json` is unchanged.
3. No test's asserted decline reason changes.

## 7. Risks

1. **A message that drifts in the merge.** `intOperand` and `compileIntOperand` decline with
   different texts; the merged method keeps both by taking the text, and the dump and the
   compiler suite read them.
2. **The month-count gate declining a shape that fused.** Every arm builds an admitted kind
   today, so the gate should never fire on the corpus; the dump would show it if it did.

## 8. Sequencing

1. This plan; row 86 marked Planned.
2. The positions, the gate, the leaf rule and the tests.
3. The outcome.
