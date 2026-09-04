# Task 59: next_day with a weekday column, through a derived int32 leaf

## 1. Where this came from

`PLAN_MILESTONE_4.md` row 59 and section 2.26, from the coverage survey of 4
September 2026. Task 33 covers `next_day(d, 'MON')`: the weekday folds at
compile time into the literal `k = dayOfWeek - 1` and travels as a
`LiteralSlot`, so one emitted class serves every weekday. A weekday **column**
declines today with "next_day with a non-foldable weekday", because the
kernel has no string lanes and the row engine's parse -
`DateTimeUtils.getDayOfWeekFromString`, case-insensitive, three spellings per
day, a null or an error for anything else - is per row. Section 2.26 names the
one mechanism that lets a string-argument date function run in the kernel
without string lanes: an int32 column the evaluator derives per batch, before
the kernel runs, by the row engine's own parser. Task 61 (`trunc` with a
format column) reuses it, which is why it is worth building even if a lone
`next_day` gains little, and section 2.26 says the number is committed
whichever way it falls.

## 2. The admission check, done

**What the kernel would read.** A cached string column reaches the evaluator
as an `ArrowColumnVector` over a plain `VarCharVector`: every one of the
serializer's four schema constructions passes `largeVarTypes = false`
(`ArrowCachedBatchSerializer.scala`, checked in this worktree), and
`ArrowUtils.toArrowType` maps `StringType` to `Utf8` under that flag.
`ArrowColumnVector.getUTF8String` over it is a zero-copy
`UTF8String.fromAddress` into the Arrow buffer, valid while the batch is. The
evaluator therefore reads names through the accessor it already trusts, and
the two sibling vector types (`LargeVarCharVector`, `ViewVarCharVector`) are
refused the way a non-Arrow
batch is, not handled.

**The parser is collation-blind.** `NextDay.inputTypes` accepts any string
collation, and `getDayOfWeekFromString` ignores it: `string.toString`, then
`toUpperCase(Locale.ROOT)`, then a match against twenty-one spellings, with no
trimming. So the derived column is the same under every collation, and the
leaf reproduces the row engine by calling the row engine's function, or by
an ASCII fast path that agrees with it.

**Where the fast path must give up.** `"\u017Funday".toUpperCase(Locale.ROOT)`
is `SUNDAY` (U+017F, long s) and `"fr\u0131day"` is `FRIDAY` (U+0131, dotless
i), checked with `jshell` on this worktree's JDK. A byte-level parser is
exact only over ASCII input, so any byte at or above `0x80` delegates the row
to `getDayOfWeekFromString` itself. Over ASCII, `toUpperCase(Locale.ROOT)` is
the
plain ASCII fold, so the fast path's case folding and the row engine's agree
on every byte; section 5 pins that over the domain that matters.

**The value the kernel sees.** `k = dayOfWeek - 1` lies in `-1..5`
(`THURSDAY = 0` .. `WEDNESDAY = 6`), and task 33's lowering `d + 1 +
floorMod(k - d, 7)` is exact for *every* int `k`, since its oracle is Spark's
own wrapping arithmetic. No bound to check, no guard to emit; task 52's
`dayRange` arm for `NextDay` shifts the date by `[1, 7]` without reading the
offset, so the range analysis is untouched.

**The rule that decides the ANSI route.** `NextDay.nullSafeEval` never parses
the name when the date is null: `next_day(NULL, 'xyz')` is NULL under ANSI,
not an error. A pre-pass that raised on the parse would therefore *diverge*
from the row engine on exactly the rows where a bad name sits beside a null
date. Section 2.26 said ANSI needs no decline because the pre-pass calls the
same function; that is corrected here. Under ANSI the leaf never throws: an
unrecognised name declines the batch to the row engine, which then computes
every row by its own rules - NULL where the date is null, the
`ILLEGAL_DAY_OF_WEEK` error where it is not - so the user-visible error is the
row engine's own, and Varka keeps its invariant that no kernel-side code
raises a user-facing exception. Under the non-ANSI mode nothing throws and
nothing declines: an unrecognised name is a null lane, and the node's word,
the AND of the date's and the offset's, nulls the row as the row engine does.

**What the check would have rejected:** a cache that produced large or view
string vectors; a parser that consulted collation; an ASCII case fold that
differed from `Locale.ROOT`'s on some ASCII byte; or a row engine that parsed
the weekday before testing the date for null. None is the case. The cost of
the pre-pass is section 6's question, not an admission question: section 2.26
ships the mechanism either way.

## 3. The design

### 3.1 The derived input, from compiler to kernel

**In the compiled plan.** `CompiledVarkaProjection` gains a defaulted field
`derivedInputs: Seq[VarkaDerivedInput] = Nil`, the `inputBounds` precedent:
a property of the plan, not of the emitted bytes, so it is not part of
`VarkaShapeKey` - the kernel reads N int inputs and does not know which of
them a pre-pass produced.
`VarkaDerivedInput(inputIndex: Int, sourceOrdinal: Int, kind: VarkaDerivedKind)`
says which kernel input is derived, from which child column, and how.
`VarkaDerivedKind` is a Java enum in the engine package beside `IntRangeOps`:
`WEEKDAY` and `WEEKDAY_ANSI` now (the `NextDay.failOnError` flag is fixed at
construction, so it is part of the kind, not of the batch), task 61's
`TRUNC_LEVEL` later.

**In the compiler.** The `inputs` map stays `LinkedHashMap[Int, Int]`, child
ordinal to kernel input index, so no arm's signature changes and the four
open compiler branches (tasks 37, 42, 57, 58) take only additive conflicts. A
derived input is interned under a *synthetic* key that no child ordinal can
collide with, `VarkaDerivedInput.key(sourceOrdinal, kind)`, a negative int,
so `compilePartial`'s and `compilePredicate`'s mark-and-truncate discipline
rolls a derived input back with the plain columns when its entry declines,
and two `next_day` expressions over the same column share one leaf. Both
construction sites translate the keys at the end: `inputOrdinals` carries
the source ordinal for a derived key, and `derivedInputs` the note.

The `NextDay` arms become three, the weekday resolved before the date as
today (a declining weekday must register no ordinals):

    case NextDay(start, dow, _) if dow.foldable => (unchanged: the literal slot)
    case NextDay(start, br: BoundReference, failOnError) if br.dataType is a
        StringType (any collation) =>
      val kind = if (failOnError) WEEKDAY_ANSI else WEEKDAY
      for (d <- compileNode(start, ...)) yield
        new IRNextDay(d, derivedRef(br, kind, inputs))
    case n: NextDay =>
      sink.note(
        "next_day with a weekday that is neither a literal nor a column", n)

`derivedRef` is `columnRef`'s twin over the synthetic key. `upper(s)` and
other expressions over the column decline: the leaf reads a stored column,
and an expression before it is the row engine's.

**In the evaluator.** `isArrowBacked` checks a derived input's *source*: an
`ArrowColumnVector` over a `VarCharVector` holding exactly the batch's rows;
anything else refuses the batch as a non-Arrow input does. `fillSources`
fills the plain inputs as today, then for each derived input calls
`WeekdayLeaf.fill(names, len, kind, dstData, dstValidity)` into a per-task
scratch pair (data `4 * len` bytes, validity `(len + 63) / 64 * 8`) allocated
from `taskAllocator()`, grown on demand under the filter's `maskBuf`
discipline (null the field, close, reallocate; released by the base
evaluator's task-completion cleanup before the overridable `onTaskCleanup`
hook and the allocator close), and points the kernel's `srcData`,
`srcValidity` and `srcNullCount` at it. `fill` returns the null count, or
`-1` when an unrecognised name was met under `WEEKDAY_ANSI`; on `-1` the
evaluator throws `VarkaBatchDeclined(STATUS_DERIVED_INPUT)`, a new bit 2
beside `STATUS_INPUT_BOUND` (bit 1), so the log line names the cause and the
batch is metered as declined, not as a row-path failure. The filter evaluator
calls the same `fillSources`, so `WHERE next_day(d, s) = d2` gets the leaf
without further work. The scratch is read only inside `kernel.run`, so
reusing it across batches is safe under the nodes' one-batch-at-a-time
iteration, exactly as `maskBuf` is.

**The leaf.** `WeekdayLeaf.java`, in the engine package, Java per the
project's direction, with two parsers kept as live variants under the same
tests so the measurement picks the default (the `FloorMod7` precedent,
applied to evaluator-side code): `ROW_ENGINE` calls
`DateTimeUtils.getDayOfWeekFromString` per row, catching
`SparkIllegalArgumentException` for the null; `ASCII` reads the
`UTF8String`'s bytes in place, folds ASCII letters, matches the two-, three-
and full-length spellings, returns the miss without constructing an
exception, and delegates any row with a byte at or above `0x80` to
`ROW_ENGINE`. Both write the int and the validity bit through
`MemorySegment` at the given addresses; neither allocates on the ASCII path.
`VarkaKernelEvaluator` passes `WeekdayLeaf.DEFAULT_PARSER`, chosen in the
last commit from section 6's numbers.

**In the emitter.** Task 38's widening, applied to `NextDay`: `analyze` calls
`requireOffsetShape(n.offset())` instead of `requireLiteralOffset`
(`AddMonths` keeps the literal rule; task 60 widens that one); `planWordRef`
answers `andRef(word(days), word(offset))` instead of the date's word alone,
so a nullable offset gives the node its own slot; and the `emitValue` arm,
after its final add, stores that word the way `AddMonths` does by hand -
`emitAndWord` when the body is masked and the node owns a word. The dup/swap
stack discipline is unchanged: a `ColumnRef` load leaves exactly one value on
the operand stack, as the broadcast did. The lowering is byte for byte the
same; only the operand's origin differs.

### 3.2 What is deliberately unchanged

* The literal form: `next_day(d, 'MON')` still folds at compile time and its
  kernel bytes do not move (the register and both pinned fixtures assert it;
  `everyNode`'s `NextDay` has a literal offset and no node type is added).
* `emitFloorMod7` and the `NextDay` lowering; `dayRange`'s `[1, 7]` arm.
* `requireLiteralOffset` for `AddMonths`: task 60.
* `compileOffset` and `compileNode`'s date-only leaf: an int column still
  enters only where a task admitted it; the derived leaf is a `ColumnRef`
  built by the `NextDay` arm alone, so no other node can reach a string
  column through it.
* The row engine's parser and `NextDay`'s codegen: the leaf calls the one and
  the decline route defers to the other.
* `BatchSpec` and `buildBatch` in the exec suite stay fixed-width: the
  evaluator-level tests go through SQL fixtures, and the leaf's unit tests
  build their own `VarCharVector`.
* Task 61's `TruncDateDynamic`: it gets a kind and reuses the plumbing; its
  node is its own task.

### 3.3 Registered op counts

Dense-loop `IntVector` calls in `loopDense0`, printed with `dev/varka_emit.sh`
(`--table`, columns `d:date,off:int`) in this worktree before any change; the
register test in section 5 asserts the after column.

| kernel | before | after |
|---|---|---|
| `next_day(d, 'MON')` | 18 | 18 (unchanged, asserted) |
| `next_day(d, k)`, `k` a column | - | 18 (predicted) |
| `date_add(d, 3)` / `date_add(d, off)`, the task 38 control pair | 4 / 4 | unchanged |

The prediction rests on the control pair: task 38's column offset costs the
same number of dense-loop calls as the literal, since a literal is one
`broadcast` and a column one `fromMemorySegment` per lane group, one call
either way. The literal form's 18 is `NEXT_DAY_WEIGHT`'s 15 plus the date
load, the offset broadcast and the store.

## 4. Files

| file | what |
|---|---|
| `VarkaDerivedKind.java`, `WeekdayLeaf.java` (engine package, new) | the kind enum; `fill` with the two parsers, `DEFAULT_PARSER` |
| `VarkaExpressionCompiler.scala` | `VarkaDerivedInput`, the `derivedInputs` field and both construction sites, `derivedRef`, the three `NextDay` arms |
| `VarkaLoopEmitter.java` | `analyze`, `planWordRef`, `emitValue` for `NextDay`; the `requireLiteralOffset` javadoc |
| `VarkaVectorIR.java` | the `NextDay` record doc: a literal slot or a column |
| `VarkaKernelEvaluator.scala` | `isArrowBacked` over derived sources; `fillSources`' pre-pass; the scratch pair and its cleanup; `STATUS_DERIVED_INPUT` |
| `WeekdayLeafSuite.scala` (new) | the parser domain, the fill contract, the ANSI sentinel |
| `VarkaLoopEmitterSuite.scala` | the rejection test flipped for `NextDay`; the two-column matrix; the register row |
| `VarkaIrFuzzSuite.scala` | arm 8 may pick a column offset |
| `VarkaExpressionCompilerSuite.scala` | the derived plan, sharing, rollback, the predicate site, the expression decline |
| `VarkaSharedSessions.scala` | `cacheDatesWeekday`; `withAnsi` (task 42's helper if merged, else this task stacks on 42) |
| `VarkaDifferentialSuite.scala` | both modes, the null-date rule, the filter, the reuse query |
| `VarkaKernelEvaluatorSuite.scala` | the refused-batch route for a non-`VarCharVector` source; allocator accounting |
| `VarkaEmitterParityBenchmark.scala` + files | the `next_day` section: literal, column, the leaf under both parsers, the per-row anchor |
| `VarkaThroughputBenchmark.scala` + files | `next_day(d, s)`, the reuse row, the literal control |
| `docs/sql-varka.md`, `sql/varka/SKILLS.md` | the surface line; the null-date lesson |
| `PLAN_MILESTONE_4.md`, this file | row 59, the 2.26 correction, section 9 |

## 5. Tests, and what each is for

The oracle for the leaf is `DateTimeUtils.getDayOfWeekFromString(s) - 1`,
the definition, with "throws" as the seventh outcome; the oracle for the node
stays Spark's `getNextDateForDayOfWeek`, quoted in the reference evaluator,
which is already null-correct over a nullable offset.

* **`WeekdayLeafSuite`, the domain.** Every one of the twenty-one spellings in
  every upper/lower case pattern of its letters (a few thousand strings);
  every one- and two-byte ASCII string; every one-byte ASCII mutation of every
  spelling; the empty string and `' MON'` (no trimming); and the non-ASCII
  rows `\u017Funday`, `fr\u0131day` and a Cyrillic look-alike of `monday`.
  Both parsers must equal the oracle on every string. The failure it catches
  that nothing else would: an ASCII fold or a length test that accepts a
  near-miss the row engine rejects, or rejects a fold the row engine accepts.
* **`WeekdayLeafSuite`, the fill contract.** A hand-built `VarCharVector`
  mixing spellings, nulls and a bad name: the data lanes, the validity bits
  and the returned null count under `WEEKDAY`; `-1` under `WEEKDAY_ANSI` with
  the bad name, the count without it; a zero-length batch.
* **Emitter suite.** The rejection test keeps `AddMonths(col, col)` rejected
  and now asserts `NextDay(col, col)` emits. `checkMatrix` over a two-column
  `NextDay`, `k` cycling `-1..5` plus an out-of-range `k`, every null pattern
  of both columns, every length, both widths: the failure it catches is a
  word that ignores the offset's nulls, or a stack shape broken by the load.
  The register asserts 3.3's after column and that the literal form's count
  and both pinned fixtures do not move.
* **Fuzzer.** Arm 8 draws a column offset as well as a literal, so the
  reference evaluator's null rule is checked over random null patterns.
* **Compiler suite.** `next_day(d, s)` compiles to
  `NextDay(ColumnRef(0), ColumnRef(1))` with `inputOrdinals == Seq(d, s)` and
  one `WEEKDAY` note at input 1, `WEEKDAY_ANSI` when `failOnError`; two
  `next_day` over `s` share input 1; an entry that declines after interning
  the leaf leaves no note behind (the mark-and-truncate discipline, which is
  the failure a synthetic key could hide); `WHERE next_day(d, s) = d2`
  through `compilePredicate` carries the note too; `next_day(d, upper(s))`
  declines with the new reason. The task-33 test that asserted a column
  weekday declines is rewritten to assert it fuses.
* **Differential**, over `cacheDatesWeekday` (`d`, `d2`, `s`: the spellings
  in three case styles, `'xyz'`, `''`, `' MON'`, a null name, a null date
  beside a bad name; one partition, task 42's discipline): non-ANSI answers
  match the row engine, fused, no fallback of any kind; ANSI raises the
  `ILLEGAL_DAY_OF_WEEK` error identical to the row engine's (task 42's
  identity idiom) with `numFallbackBatchesDeclined > 0` and
  `numFallbackBatchesRowPath == 0`; ANSI over a fixture whose only bad name
  sits on a null-date row returns NULL with no error - the test that decided
  the route in section 2; ANSI over all-valid names fuses with no decline; the
  filter `WHERE next_day(d, s) = d2`; the reuse query `next_day(d, s),
  next_day(d2, s)` with `derivedInputs.size == 1` on the exec's plan; one
  `UTF8_LCASE` column giving the same answers.
* **Evaluator suite.** A batch whose weekday source is not a `VarCharVector`
  is refused (metered `numFallbackBatchesRefused`), and `withTask`'s allocator
  accounting closes at zero after a run that grew the scratch across two batch
  sizes - the failure it catches is a leaked or double-closed scratch buffer.

## 6. The measurement

Three committed places, each regenerated with `dev/varka_bench_regen.sh` on
an idle machine, the catalyst one at both widths.

**`VarkaEmitterParityBenchmark`**, a new `next_day` section:

| case | what |
|---|---|
| `next_day(d, 'MON')` | the literal kernel, the control that must not move |
| `next_day(d, k)`, null-free and mixed | the two-input kernel through `chunkedTwo`, `k` a column cycling `-1..5` |
| `weekday leaf, row-engine parser` / `weekday leaf, ascii parser` | `WeekdayLeaf.fill` alone over a `VarCharVector` of valid spellings; and once more over 10% unrecognised names under `WEEKDAY` |
| `next_day(d, s) per row` | the anchor: `DateTimeExpressionUtils.getNextDateExact` per row, the row engine's own path |

Section 2.26's rule: the fused form is the column kernel plus the better
parser; if their sum does not beat the anchor by 1.3x, the mechanism ships
and the entry records that its value is in reuse.

**`VarkaThroughputBenchmark`**: `next_day(d, s)` over the new fixture, Varka
against Janino, with `next_day(d, 'MON')` as the control row and
`next_day(d, s), next_day(d2, s)` as the reuse row. Task 62's public surface
gets a `next_day(d, s)` entry once both tasks have merged; not this task.

### 6.1 Predictions, registered before the run

1. The register: 18 for the column form; the literal row unmoved at 18.
2. The column kernel within 5% of the literal kernel at both widths: a load
   in place of a broadcast, the task 38 delta, which is one call either way.
3. The ASCII parser at least 5x the row-engine parser's rate on valid names
   (no `String`, no `toUpperCase`, no boxing), and at least 50x on the 10%
   unrecognised run under `WEEKDAY`, where the row-engine parser constructs a
   `SparkIllegalArgumentException` per bad row. The ASCII parser becomes the
   default unless it loses either comparison.
4. The parity ratio: kernel plus ASCII leaf against the per-row anchor lands
   between 1.5x and 3x - the anchor pays the same parse *and* a `String`
   per row, so the leaf's saving is the parse allocation and the kernel's
   saving is the arithmetic. Under 1.3x is the outcome 2.26 priced in and
   would be recorded, not argued with.
5. Throughput: `next_day(d, s)` Varka over Janino between 1.0x and 1.5x, the
   reuse row above the single row by at least 1.3x since the leaf is paid
   once for two nodes.

## 7. Risks

1. **The null-date rule.** Section 2's finding; the differential's null-date
   ANSI test is the check, and the SKILLS.md lesson says why a derived leaf
   must never raise.
2. **A scratch buffer leak or double close** on a grow that throws: the
   `maskBuf` discipline, and the evaluator suite's allocator assertion across
   two batch sizes.
3. **The catch-all in `serveBatch`** turning a real pre-pass bug into a
   silent row-path fallback: every differential asserts
   `numFallbackBatchesRowPath == 0`.
4. **The ASCII fold** accepting or rejecting a string the row engine treats
   the other way: the domain test, including the non-ASCII delegation rows.
5. **The synthetic key** colliding with a child ordinal or surviving a
   rollback: negative keys cannot collide, and the compiler suite's rollback
   test covers the truncate.
6. **Conflicts** with tasks 37, 42, 57 and 58 in the compiler's arms and the
   emitter's switches: additive, taken by whichever merges second; the
   `withAnsi` helper is task 42's, so this task stacks on 42 if 42 is still
   open when its differential is written.
7. **The 1.3x rule failing** for a lone `next_day`: priced by 2.26; the
   outcome is recorded in section 9 and the milestone row either way.

## 8. Sequencing

1. `VarkaDerivedKind`, `WeekdayLeaf` with both parsers, `WeekdayLeafSuite`,
   and the parity benchmark's leaf and anchor cases. Green alone; no emitter
   or compiler change; nothing pinned moves.
2. The emitter: `requireOffsetShape` on `NextDay`, the `andRef` word, the
   `emitAndWord` store; the flipped rejection test, the two-column matrix, the
   register row, the fuzzer arm; the parity benchmark's column-kernel case.
   Pinned fixtures asserted unmoved.
3. The compiler and the evaluator: `VarkaDerivedInput`, the arms, the
   pre-pass, `STATUS_DERIVED_INPUT`; the compiler, evaluator and differential
   tests; the fixture; the docs.
4. The measurement: both benchmarks regenerated on an idle machine, the
   parser default set from the numbers, section 9, the SKILLS.md lesson, the
   milestone row.

## 9. Outcome

<!-- Filled in when the measurement lands: the numbers with the committed file
     they trace to (dev/varka_quote_check.py holds you to this), 6.1's
     predictions scored one by one, what moved that the plan did not list, and
     what the task leaves for later - which goes to the milestone's debt
     register or a scope document, never to a code comment. -->
