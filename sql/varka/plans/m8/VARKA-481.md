# VARKA-481: The checked narrow multiply through the range guard

## 1. Where this came from

The `/code-review max` of #989 (23 October 2026, `VARKA-329.md` 9.8, a finding cut by the
review's cap): an ANSI or TRY `TINYINT`/`SMALLINT` multiply stays a narrow `IntArith`, held up by
special cases in five places, and its int-lane range compare repeats `emitRangeGuard`. The general
form is the one `CAST(bigint AS INT)` already has under ANSI,
`NarrowLane(GuardedRange(IntArith(MUL, WRAP, WidenLane(x), WidenLane(y)), MIN, MAX))`, which the
existing int-lane rule drives at the int lane. TRY needs a `NULL` mode on `GuardedRange`, which
`try_cast` of a bigint lacks too (row 332). This row comes after row 480, whose narrow-column range
and arithmetic changes touch the same code.

## 2. The admission check, done

From the code (25 October 2026; paths under
`sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen/`):

1. **The five special cases, and nothing else depends on them.**
   - (a) `varka/VarkaKernelPlan.java:169-170` and its walk at `:224-243`: `emissionLane` forces the
     int lane for a narrow non-`WRAP` `MUL`, and `kernelLane` (`:186-193`) joins a native narrow
     root beside it at the int lane (989-9).
   - (b) `VarkaNodeCompiler.java:956`: `&& !narrow` exempts it from the checked-multiply decline.
   - (c) `varka/Analysis.java:668-669`: the clause that gives a narrow multiply `INT_OVERFLOW`.
   - (d) `varka/VarkaVectorWalk.java:661`: `&& !narrow` on the "no int-lane overflow test" throw.
   - (e) `varka/VarkaVectorWalk.java:740-742`: the "emitted at the int lane" throw.
   No test pins either throw or the int-lane decision; the narrow suites assert answered or
   declined, not the status bit (`VarkaEmitterNarrowArithSuite.java:514-529`). The other `!narrow`
   in `arithOver` (never rewrite a narrow `FAIL` to `WRAP`) stays: it protects narrow addition and
   subtraction.
2. **The general form drives at the int lane on both arms by the existing rule.** `lanesIn` of
   `NarrowLane(GuardedRange(IntArith(..., WidenLane(x, INT), WidenLane(y, INT))))` holds `INT`, so
   `emissionLane` returns the int lane (`VarkaKernelPlan.java:169`), and the native arm never ran a
   multiply on a narrow lane anyway (its saturating difference, `VarkaVectorWalk.java:745-754`, is
   for addition and subtraction).
3. **The two compares are the same.** The multiply's (`VarkaVectorWalk.java:727-737`) and
   `emitRangeGuard` (`:1025-1039`) both compare against the narrow type's minimum and maximum and
   OR them; they differ in what consumes the mask - `emitOverflowMask` with `INT_OVERFLOW`, whose
   `NULL` arm clears the node's own word, against `emitGuardCollect` with `VALUE_RANGE`, which has
   no `NULL` disposal - and in the scratch they park in (`intArithTmp`, three slots, against
   `guardTmp`, one).

## 3. The design

### 3.1 A `NULL` guard, and the multiply through it

1. **`GuardedRange` gains a mode** (`FAIL`, `NULL`), kept by a three-argument constructor for the
   seven existing call sites, rendered by `canonical` and carried by `withChildren` (a `FAIL` and a
   `NULL` kernel must not share a cache key), and split in the cost key. Under `NULL` the guard
   clears its own word where the value is out of range: it owns a word (as a nulling `IntArith`
   does), sets `nullsFromValidInputs`, and has no pure word. `emitRangeGuard` hands its mask either
   to `emitGuardCollect` (`FAIL`) or to `emitOverflowMask`'s `NULL` arm. The reference evaluator
   gains the `NULL` arm; the fuzz grammar draws both modes; `narrow_range.smt2` gains the claim that
   a lane stays valid exactly when it was valid and in range. On its own this step moves no bytes.
2. **`narrowArith` builds the general form** for a `MUL` whose mode is not `WRAP`:
   `NarrowLane(GuardedRange(IntArith(MUL, WRAP, WidenLane(x, INT), WidenLane(y, INT)), MIN, MAX,
   mode), lane)`, directly rather than through `arithOver`. The product of two narrow values is
   exact at the int lane (2^14 for bytes, 2^30 for shorts), so the `WRAP` multiply is the exact
   product and the guard alone decides the overflow.
3. **The special cases go**: (a) to (e) and their comments
   (`VarkaVectorWalk.java:613-619, 652-660, 715-724, 922-926`, `Analysis.java:661-666`,
   `VarkaNodeCompiler.java:928-935`, `VarkaProofFiles.java:2398-2421`).
4. **No redundant narrowing.** An interior narrowed multiply, read by another node as in
   `(y1 * y2) + y3`, would take `emitWrapToNarrow`'s shift pair after a guard that already bounds
   it: two lanewise operations for nothing. The pair is skipped when the child is a `GuardedRange`
   within the target type's bounds.

### 3.2 The question for the owner: `checkIntOverflow=false`

Today the narrow multiply's check is an `INT_OVERFLOW` refusal, in the CHECKED family, which
`checkIntOverflow=false` turns off (`Analysis.java:321-322, 668`); the reference configuration then
computes the wrapped product. Through `GuardedRange` the check becomes `VALUE_RANGE`, in the
REARMED family, which no option turns off (`:333-336`). Two choices:

- **A. The guard stays on under `checkIntOverflow=false`** (the general form as it stands). The
  option no longer covers narrow multiplies; the six `skips.tsv` lines that say "the overflow check
  is off by design" for them (`skips.tsv:37, 42, 45, 50, 53, 58`) go false and are removed.
- **B. The option still turns it off**: a narrow multiply's guard is built only when
  `checkIntOverflow` is on, so the option keeps its meaning for every integer type; the skip lines
  stay.

B keeps one option meaning one thing across the integer types; A is simpler and is what
`CAST(bigint AS INT)` does today. *The owner chose B (25 October 2026):* the option is a priced
check (`Reason.PRICED_CHECK`), which exists so the A/B can price every ANSI and TRY int check
against the arithmetic it protects, and a cast was never in its scope; `guardDayProducers` is the
precedent for a priced option over a range guard. The one condition sits in `narrowArith`, where
the node's shape is decided.

### 3.3 What is deliberately unchanged

Narrow addition and subtraction (their saturating difference on the native arm, their `FAIL`
protection in `arithOver`); `try_cast` of a bigint, which can take the new `NULL` mode in row 332;
the int and long lanes' own checked arithmetic.

### 3.4 Registered op counts

Per checked narrow multiply at the int lane: today two compares, an OR and the overflow mask over
three parked values; after, the same two compares and OR over one parked value, the guard's
collect or nulling, and no shift pair at an interior read. Asserted from `emitted_bytes.json`'s
diff and the emitter suite's counts once built.

## 4. Files

| file | what |
|---|---|
| `varka/VarkaVectorIR.java` | `GuardedRange`'s mode, `canonical`, `withChildren` |
| `varka/Analysis.java`, `varka/Slots.java` | the `NULL` guard's word; special case (c) |
| `varka/VarkaVectorWalk.java` | `emitRangeGuard`'s two disposals; (d), (e); the skipped shift pair |
| `varka/VarkaKernelPlan.java` | special case (a) |
| `VarkaNodeCompiler.java` | the general form in `narrowArith`; (b) |
| `varka/VarkaEmitCost*.java` | the cost key by mode |
| `VarkaReference.java`, `VarkaIrGrammar.java`, `VarkaGuardSpecs.java` | the `NULL` mode's oracle, draws and specs |
| `VarkaProofFiles.java`, `sql/varka/proofs/narrow_range.smt2` | the `NULL` claim |
| `sql/varka/matrix/skips.tsv` | only under choice A |
| `sql/varka/emitted_bytes.json`, the cost table | regenerated |

## 5. Tests, and what each is for

1. The narrow suites (`VarkaEmitterNarrowArithSuite` at both arms, the TINYINT and SMALLINT
   conformance suites, the 2^24 SMALLINT slices) - the answers do not move.
2. A `NULL`-mode guard test in the emitter suite over every edge value, both widths, the oracle
   nulling where the guard fires - the new mode's own test.
3. `VarkaCoverageDifferentialSuite` - it derives the expected status from the guards, so the
   moved bit (`INT_OVERFLOW` to `VALUE_RANGE`) is checked, not assumed.
4. The fuzz suite drawing both modes, and the option matrix (under choice B, the
   `checkIntOverflow=false` configuration still wraps).
5. An interior narrowed multiply (`(y1 * y2) + y3`) emits no shift pair - the op count.

## 6. The measurement

The change is a simplification; it makes no speed claim, so no benchmark gate. If section 3.4's
counts show fewer operations per checked narrow multiply, the parity benchmark's narrow rows are
regenerated as the task that moved the bytes (the rule), not as a claim.

### 6.1 Predictions, registered before the run

1. Every checked narrow multiply's kernel re-hashes; no answer moves.
2. Fallback metrics report `VALUE_RANGE` where they reported `INT_OVERFLOW` for these multiplies.
3. Under choice B, `checkIntOverflow=false` still wraps a narrow multiply, and the skip lines hold.

## 7. Risks

1. The range analysis's bound on a `GuardedRange` (`VarkaRangeAnalysis.java:268`) holds on live
   lanes only once a `NULL` mode exists (`VARKA-329.md` 4.2's qualification); a consumer that read
   a nulled lane's value would see the unbounded product. Test 2 runs the nulled lanes through
   consumers, and the analysis's javadoc states the qualification.
2. A literal under `WidenLane` in an int-driven kernel is a shape no test has reached; test 1's
   literal rows (`y1 * 3Y`) cover it.
3. The status-bit change can surprise a reader of fallback metrics; the row and the PR state it.

## 8. Sequencing

1. After row 480 merges: the `NULL` mode on `GuardedRange` with its oracle, grammar, specs and
   proof, moving no bytes.
2. The general form in `narrowArith`, the special cases removed, the shift pair skipped, behind
   choice B's condition.
3. The generated files regenerated once; the gate and the matrix's scoped configuration.

## 9. Outcome

(Written when the work lands.)
