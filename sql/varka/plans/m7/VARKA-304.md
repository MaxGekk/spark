# VARKA-304: A status bit per refusal reason

## 1. Where this came from

Row 304 of `m7/PLAN.md`, filed by VARKA-83 (`VARKA-83.md` 3.2). VARKA-83 declared every runtime
refusal once, as `Analysis.Refusal`, with six reasons; but every reason still returns the one
status bit `STATUS_CHRONO_RANGE`, so a declined batch says that some guard fired and not which.
The enum makes the answer one constant per reason. This row gives each reason its own bit and has
the evaluator count declines by reason.

## 2. The admission check, done

Surveyed on 11 October 2026 at `bc8c96fc871`.

**The status word.** Three bits are in use: bit 0 (`VarkaFusedKernel.STATUS_CHRONO_RANGE`, 1),
which every refusing kernel returns, and the evaluator's own `VarkaKernelRunner.STATUS_INPUT_BOUND`
(2) and `STATUS_DERIVED_INPUT` (4), which no kernel emits. `ChronoVectorOps` in the engine module
mirrors bit 0 by value. The forced-decline test hook throws a literal 1. Every reader of the word
is `VarkaFallbackAccounting.declinedBatch`, which logs the integer and bumps one counter.

**Where a body folds its guards.** Every guard site passes the refusing node to
`VarkaVectorWalk.emitGuardCollect` (three call sites, through `emitRangeGuard` for three more),
and that node is the key of `Analysis.refusals`, so each collect knows its reason. A body ORs
every guard into one mask, `Slots.guardAcc`, and `emitStatusReturn` turns a non-empty mask into
the constant 1.

**The census.** The question that decides the design is how many bodies fold guards of more than
one reason into that one mask: those cannot say which reason fired without a mask per reason. A
temporary count in the slot planner, over every body `VarkaEmittedBytesSuite`'s pinning test
emits (the coverage rows and the 20,000 fuzz shapes, under every pinned arm and both widths),
found 972,798 guarded loop and epilogue bodies, of which 251,460 (25.8%) hold two or three
reasons. The commonest pairs are `INT_OVERFLOW` with `VALUE_RANGE` (173,188 bodies),
`DAY_RANGE` with `VALUE_RANGE` (14,614) and `DAY_RANGE` with `MAKE_DATE` (11,598).

So the row's proof as written - `emitted_bytes.json`'s diff limited to the status constants - is
reachable only if a multi-reason body keeps its one mask. A mask per reason would put another
live mask in the loop of a quarter of the guarded bodies, which on AVX2 is another vector
register, to make a rare event exact.

**The decision** (the owner's, on 11 October 2026): the default keeps one mask and reports the
union of the body's reasons, exact for a single-reason body and a candidate set otherwise; an
emit option gives each reason its own mask, for exact attribution when diagnosing.

What the check would have rejected: a mask per reason by default.

## 3. The design

### 3.1 The bits

Each `Refusal` carries its status bit as an explicit field, not its ordinal, so reordering the
enum moves no bytes. The constants live in `VarkaFusedKernel`:

| reason | bit | value |
|---|---:|---:|
| `DAY_PRODUCER` | 0 | 1 |
| `MONTH_COUNT` | 1 | 2 |
| `MAKE_DATE` | 2 | 4 |
| `INT_OVERFLOW` | 3 | 8 |
| `DAY_RANGE` | 4 | 16 |
| `VALUE_RANGE` | 5 | 32 |

`DAY_PRODUCER` keeps bit 0, which is what `STATUS_CHRONO_RANGE` describes ("a day outside the
range its calendar lowering is defined over") and what `ChronoVectorOps`' hand-written kernels
mean by it; so a body whose only reason is the producer guard is byte-identical. The constant is
renamed `STATUS_DAY_PRODUCER`; the engine's mirror keeps its name and points at it.

The evaluator's own bits move up, out of the kernels' range: `STATUS_INPUT_BOUND` to bit 8 (256),
`STATUS_DERIVED_INPUT` to bit 9 (512), and the forced-decline hook's literal 1 becomes a named
`STATUS_FORCED`, bit 10 (1024). They are never emitted, so moving them moves no bytes.

### 3.2 The default: one mask, the body's reasons

`Slots` already walks a body's nodes for their refusal families when it allocates `guardAcc`. It
also ORs their status bits into `Slots.guardStatus`, and `emitStatusReturn` loads that instead of
1. One mask, one reduction, one constant: the bytes change only where the constant does. A
constant above 5 is a `bipush` rather than an `iconst`, one byte longer per status return.

In the bodies whose slot loop visits nodes it does not emit (the whole kernel's, under
`groupLocalSlots=false` or a byte budget of 0, `Analysis.Refusal.Family`), the union is over the
same set the family rule uses, so it can name a reason the body does not emit. That is a
candidate the reader can rule out, never a reason missing.

### 3.3 The option: a mask per reason

`exactRefusalStatus`, off by default, under a new option reason `DIAGNOSTIC`: it changes what a
declined batch reports and never an answer or whether a batch declines, so it is neither a
reference form nor a priced check. On, `Slots` gives a body one mask per distinct reason in
enum order, the first in `guardAcc`'s slot and the rest after it; `emitGuardCollect` ORs into its
node's reason's mask, `emitStatusReturn` reduces each mask and ORs its bit into the result. In a
single-reason body it emits what the default does. It renders as a flag tag, so no existing hash
moves.

It reaches a kernel through the tests and `dev/varka_emit.sh --options`. A session conf for it
would thread through the exec nodes' evaluator factories, which row 305's pull request changes;
it becomes its own row.

### 3.4 The evaluator

`VarkaFallbackAccounting.declinedBatch` counts each set bit against its reason
(`declinedByReason`), names the reasons in its debug line, and the `VarkaFallbackEvent` JFR event
gains the status word as a field, so a recording filters declines by reason. The plan's SQL
metric `fallbackBatchesDeclined` keeps counting batches; six more metrics on every Varka node
would clutter every plan for a rare event, and the JFR event is the diagnostic channel VARKA-22
built for this.

### 3.5 What is deliberately unchanged

Every guard's emission and condition, every refusal's family and parking (the slot planner's
rule, VARKA-83), and whether a batch declines. The compiler: nothing.

### 3.6 Registered op counts

None move. The emitter suite's metric counts vector invocations in the loop; the default changes
a constant after it, and the option ORs each guard into another mask, the same op. The option's
extra reductions are in the status return, once per method.

## 4. Files

| file | what |
|---|---|
| `VarkaFusedKernel.java` | the six reason bits |
| `Analysis.java` | each `Refusal`'s bit |
| `Slots.java` | `guardStatus`; one mask per reason under the option |
| `VarkaBodyEmitter.java` | the status return; each mask's initialization |
| `VarkaVectorWalk.java` | the collect picks its reason's mask |
| `VarkaEmitOptions.java`, `VarkaEmitOption.java` | `exactRefusalStatus`, `Reason.DIAGNOSTIC` |
| `VarkaKernelRunner.java` | the evaluator's bits moved, `STATUS_FORCED` |
| `VarkaFallbackAccounting.java`, `VarkaFallbackEvent.java` | counts by reason, the status field |
| `ChronoVectorOps.java` | the mirror's doc |
| the emitter suites that assert a status | the reason's bit instead of `STATUS_CHRONO_RANGE` |
| `emitted_bytes.json` | regenerated |
| `skills/emitter-and-ir.md` | the status word |

## 5. Tests, and what each is for

* **Each reason alone**: for every `Refusal`, a shape whose only guard is that reason, fed a value
  that fires it, returns exactly its bit under both arms; the existing guard tests in the chrono,
  arithmetic and long-lane suites assert their reason's bit rather than bit 0.
* **Two reasons in one body**: a shape with `MAKE_DATE` and `INT_OVERFLOW` guards in one body,
  fed values that fire each alone and both: the default returns both bits every time, the option
  returns the bit that fired.
* **The bits**: every reason's bit is a distinct power of two, disjoint from the evaluator's.
* **The accounting**: a declined status counts each of its bits against its reason.
* **`emitted_bytes.json`**: regenerated, with every moved method hash in a body holding a reason
  other than `DAY_PRODUCER`, and the option audit listing `exactRefusalStatus` as moving only
  multi-reason shapes.
* **The option table** (`VarkaEmitOptionSuite`): the new reason and flag.
* **The gate**, both widths; the matrix runs every suite with the option on.

## 6. The measurement

None: the default changes a constant per guarded method, and the option is a diagnostic that no
production path selects. The check is the bytes.

### 6.1 Predictions, registered before the run

1. Under the defaults, every method whose hash moves in `emitted_bytes.json` holds a reason other
   than `DAY_PRODUCER`, and a class-bytes diff of a guarded coverage row shows only the status
   constant.
2. The option moves only the shapes with a multi-reason body; in every other shape it is
   byte-identical to the default.
3. No registered op count moves.

## 7. Risks

1. **A bit misattributed by the union.** A multi-reason body names a reason that did not fire.
   That is the design, stated in the docs and the event's description; the option is the remedy.
2. **The evaluator's bits shared with a kernel's.** The bits test shows them disjoint.
3. **A test asserting the union under the matrix's option arm.** The two-reason test sets the
   option both ways itself; any other test that meets a multi-reason body asserts its reason's
   bit with a mask, which holds under both arms.

## 8. Sequencing

1. This plan; row 304 marked Planned, and the session conf filed as its own row.
2. The bits, the default's union, the option, the accounting, the tests and the regenerated file.
3. The outcome.
