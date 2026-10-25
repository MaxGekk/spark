# VARKA-338: The `TIMESTAMP_NTZ` lane

## 0. What exists now (admission check)

From the code at `608686315e7` (25 October 2026; paths under
`sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen/` unless named):

1. **The type is declined by name, in one place.** `VarkaNodeCompiler.laneOf` (`:675`) has no arm
   for `TimestampNTZType`, its javadoc saying both timestamp types are "deliberately absent", and
   `VarkaTimeCompiler.isTimestamp` (`:225`) declines a column of either with
   `TIMESTAMP_OUT_OF_MILESTONE` ("a timestamp column is outside milestone 5"). Every other site
   keys on `laneOf` or `onLongLane`: once `laneOf` answers `LONG`, the long-lane column leaf
   (`VarkaTimeCompiler.java:161`), the long-literal slot (`:168`, an NTZ literal's value is
   already a `Long`), the comparisons, `IN`, the choice nodes and the null tests take an NTZ value
   as they take a `TIME` one. No arm keys on "any long-lane type" for an operator Spark types
   differently for NTZ: `ntz - ntz` is `SubtractTimestamps`, `ntz + dt` is `TimestampAddInterval`,
   and neither `CAST(ntz AS BIGINT)` nor negation is legal Spark.
2. **Arrow.** Spark writes NTZ as `ArrowType.Timestamp(MICROSECOND, null)`
   (`ArrowUtils.scala:113`), a `TimeStampMicroVector`; the zoned type is a
   `TimeStampMicroTZVector`, a different class, so an entry in `VarkaLaneArrow.of(LONG)`
   (`sql/core/.../VarkaLaneArrow.java:107`) admits NTZ alone. `fromField` builds the destination
   from Spark's own field, as for the long lane's other three.
3. **`DATE` to `TIMESTAMP_NTZ` raises in every mode but TRY.** `Cast` evaluates it as
   `daysToMicros(d, UTC)`, which is `Math.multiplyExact(days, MICROS_PER_DAY)`
   (`SparkDateTimeUtils.scala:374`) whatever `evalMode` says; `try_cast` wraps the whole cast in
   a `catch` that returns null (`Cast.scala:1587-1596`; `canUseLegacyCastForTryCast` is false for
   a date source). The product fits a long exactly for days in `[-106,751,991, 106,751,991]`.
4. **The pieces the cast needs exist.** `WidenLane(x, LONG)` is the int-to-long sign extension
   (`varka/VarkaVectorIR.java:531`); `GuardedRange` passes its child through and sends a batch
   with a live lane outside its bounds to the row engine, which raises or nulls on the same row
   (`:479-511`). Under TRY that is a correct answer computed slower, not a wrong one: the row
   engine evaluates the same `try_cast`. A checked long multiply has no lowering (the
   checked-multiply decline, `VarkaNodeCompiler.java:956`), so the cast cannot be a `MUL` in
   `FAIL` mode; it is a `WRAP` multiply under the guard, exact by the guard's bounds, the way
   `CAST(bigint AS INT)` builds its guard directly (`VarkaNodeCompiler.narrowing`, `:601`).
   That precedent declines TRY outright, leaving its null to row 332; this row does not wait,
   since the guard's fallback already nulls on the right row, and 332 or 481 can lift
   `narrowing`'s TRY decline the same way.
5. **`TIMESTAMP_NTZ` to `DATE` is a floor division the long lane cannot do exactly today.**
   Spark computes `microsToDays(t, UTC)`, the epoch day of the UTC local date, which is
   `floorDiv(t, 86,400,000,000)`. The long lane's `ConstDivide` truncates and is exact only
   under `EXACT_DIVIDEND_BOUND = 2^52` (`varka/VarkaVectorIR.java:704-722`,
   `sql/varka/proofs/long_divide.smt2`); `2^52` microseconds is 142.7 years, so a bound guard
   would send every batch holding a date before 1827 or after 2112 - a `9999-12-31` sentinel
   among them - to the row engine. Section 2.6 of the milestone plan has the exact form, placed
   under row 340: `86,400,000,000 = 2^13 * 10,546,875`, so the floor division is an arithmetic
   shift by 13 and then a floor division by 10,546,875 of a value of magnitude at most `2^50`,
   inside `ConstDivide`'s bound. The cast needs it first, so it moves to this row (section 2.3)
   and row 340 reuses it.
6. **The tests that pin the decline.** `VarkaExpressionCompilerSuite` "a timestamp column
   declines with the milestone's reason, in both forms" (`:2569`) and `VarkaLongLaneSuite` "a
   timestamp comparison stays on the row engine" (`sql/core`, `:250`) run over `ts` and `ntz`;
   their NTZ halves flip to fused, their zoned halves stay. `VarkaVectorProjectionSuite` groups
   NTZ with the long types for its fixture already. `VarkaCastMatrixSuite` and
   `VarkaTypeConformance` name `ntz` and wait for it; `m8/casts_fused.tsv` moves when the three
   pairs of section 2.3 fuse.
7. **The date fields stay declined, as row 340 needs.** `year(ntz)` arrives as
   `Year(CAST(ntz AS DATE))` (`GetDateField` is `ImplicitCastInputTypes`), so the cast of 2.3
   sits under every date field. `VarkaChronoCompiler.calendarInput` admits a child only when
   `dayRange` bounds it inside the int32 prefix, and "an unknown producer declines"; the cast's
   `NarrowLane` has no day range (`VarkaRangeAnalysis`, the `NarrowLane` arm), so the fields
   decline until 340 gives the cast its range or a `GuardedDay`.
8. **The conformance overflow check has no NTZ form.** `VarkaTypeConformance.overflow` builds
   `x + y`, `x - y`, `x * y`, `-x`, `abs(x)` and the `try_` functions, none legal Spark over two
   NTZ values; only `BOOLEAN` is exempt today, by asserting the census lists no arithmetic for
   it.

### What differs from the row text and the milestone plan

- **The registry is not built first.** `VARKA-475.md` 5 orders its kernel plan (3.3) and type
  registry (3.4) before this row, because "the four consumers would otherwise have to agree on
  by hand" on a long decomposed into the int32 calendar prefix. The kernel plan is row 488, done
  (24 October 2026), and it is what made the consumers agree; the decomposition is row 340's,
  not this row's. With the milestone's focus on the new types (owner, 22 October 2026), this row
  adds one arm to `laneOf` and the registry row absorbs it when it replaces `laneOf`. The arm is
  the same one line whichever lands first.
- **Row 332 is not needed.** Section 4's order puts this row "after 28 and 332", for 332's cast
  harness. That harness is now row 486's `VarkaCastMatrixSuite` and row 484's conformance
  contract, both done; 332's own casts are the integral and interval ones, and none is on this
  row's path.
- **The floor division moves here from 340** (item 5).

## 1. Goal and non-goal

A `TIMESTAMP_NTZ` column is read, compared, chosen, tested for null and written by a kernel, and
cast to and from `DATE`, with the same answers as Spark's row engine in LEGACY, ANSI and TRY.
Not this row: NTZ arithmetic (339), the date and time fields and the casts with `TIME` (340), the
zoned `TIMESTAMP` (out of the milestone by decision).

## 2. Design

### 2.1 The lane

NTZ joins the long lane: microseconds since the epoch, signed, eight bytes a row, ordered as a
signed long. No new `LaneType`, no new `Lane` entry, no new species, so nothing in the emitter
or the warm-up changes for the value itself.

1. `laneOf` gains `TimestampNTZType` in its long arm, and its javadoc says why the zoned type
   stays out (its arithmetic is in the session zone, not lane arithmetic).
2. `isTimestamp` becomes the zoned type alone, and its decline reason drops "milestone 5" for
   the decision it records: "a zoned timestamp column, whose arithmetic is in the session zone".
3. `VarkaLaneArrow.of(LONG)` gains `TimeStampMicroVector` for `TimestampNTZType::equals` through
   `fromField`; its comment loses "the timestamp vectors are deliberately not here".
4. `readsColumnOf` then admits NTZ, so `VarkaArrowSource` feeds NTZ columns to the kernels in
   the `sql` suites; that is the end-to-end path the coverage rows run through.

### 2.2 What fuses by that alone

Comparisons (`=`, `<>`, `<`, `<=`, `>`, `>=`, `<=>`), `IN` over literals, `IS [NOT] NULL`, `IF`,
`CASE`, `COALESCE`, `NVL`, `greatest`, `least`, and the column and choice as outputs, each over
NTZ operands, by the long lane's existing nodes - a prediction from item 0.1, which step 1's
coverage rows check form by form; a form that still declines gets its reason in section 7 or a
fix in this row. A comparison of a `DATE` and an NTZ arrives as
`CAST(d AS TIMESTAMP_NTZ) <op> ntz` and fuses once section 2.3's cast does.

### 2.3 The casts with `DATE`

- **`CAST(d AS TIMESTAMP_NTZ)`**, every mode:
  `IntArith(MUL, WRAP, GuardedRange(WidenLane(d), -106751991, 106751991), slot(86400000000L))`.
  The guard bounds the day so the multiply is exact; a batch with a live day outside goes to
  the row engine, which raises under LEGACY and ANSI and nulls under TRY, on that row. When row
  481 gives `GuardedRange` its `NULL` mode, the TRY form takes it and nulls in the kernel; until
  then TRY is correct and the overflowing batch is slower.
- **`CAST(ntz AS DATE)`**, every mode (it cannot fail): `NarrowLane(q)` with
  `y = x >> 13` (`BitOp(SHR)`, a floor division by `2^13`),
  `y' = y + ((y >> 63) & (1 - 10546875))` (a negative `y` moved down by `d - 1`, so the
  truncating divide floors), and `q = ConstDivide(y', 10546875, 2^51)`. The nested floors are one
  floor because both divisors are positive integers; `|y'| < 2^50 + 2^24`, inside the bound;
  every quotient is in `[-106,751,992, 106,751,991]`, so the narrowing is exact. All of it is
  existing nodes; no new IR, lowering or emit option.
- **`CAST(ntz AS TIMESTAMP_NTZ)`** is the identity and compiles to its child, as the date and
  `TIME` identities do.

The division is a compiler helper (`floorDivLong(x, divisor)`, taking only divisors whose odd
part keeps the shifted magnitude under `2^52`), so 340's date fields call it rather than build
it again. 2.6's Granlund-Montgomery form for the second step needs a 64-bit high multiply the
Vector API does not have; it stays 340's to measure as an arm if the census finds one.

### 2.4 Proofs

- `long_divide.smt2` gains the claim, over all 64-bit `x`, that the composition of 2.3 equals
  `floorDiv(x, 86400000000)`, through `dev/varka_prove.sh` - the integer identity only, since the
  file already proves `ConstDivide` exact under its bound.
- `DATE` to NTZ is proven by exhaustion over all 2^32 day values against `Math.multiplyExact`,
  as `casts.tsv` row 85 says: in range the product, out of range the guard's decline.

## 3. Tests and oracles

The oracle is Spark's own evaluation, through the reference evaluator and the differential
suites, as for every landed type.

1. **`VarkaTimestampNtzConformanceSuite`** over row 484's contract, values from
   `integralValues(Long.MIN_VALUE, Long.MAX_VALUE)` plus the day edges `+-106,751,991 * 86.4e9`
   and the first micros past them: load, round trip, nulls, ordering, choice and its row of the
   cast matrix. The overflow check's exemption widens from `BOOLEAN` to the types with no
   `+`, `-` and `*` (0.8), with the same assertion that the census lists none for them; NTZ's
   own arithmetic is 339's, and its overflow is tested there. The literal rendering is the one
   detail the fixture must settle (NTZ has no `%d` literal form).
2. **Coverage rows**, ADDING_A_TYPE step 11: a load, a comparison against a literal and against
   a column, `IN`, a choice, a null test, an output, both casts and a `DATE` and NTZ comparison;
   each runs through the differential suite at every position.
3. **The edges of 2.3**: `try_cast` and `CAST` of `DATE` at days `+-106,751,991` (fused) and
   `+-106,751,992` (a raise under LEGACY and ANSI, a null under TRY, from the fallback), with
   the overflowing day in one batch of several so the other batches stay fused; `CAST(ntz AS
   DATE)` at `Long.MIN_VALUE`, `Long.MAX_VALUE`, `-1`, `0`, `+-86,400,000,000` and their
   neighbours, against `java.time` (`LocalDateTime.ofEpochSecond(...).toLocalDate().toEpochDay()`)
   as well as Spark.
   And `year(ntz)` declines with its reason (0.7), pinned so that 340 sees the change it makes.
4. **The flipped tests** of 0.6: the NTZ halves assert fused and the same answers; the zoned
   halves keep their decline with its new reason.
5. **The fuzzers**: the IR grammar's long leaf already covers the lane; the Spark fuzzer's
   fixture gains an NTZ column over the conformance values.
6. **The generated files** regenerated in ADDING_A_TYPE's order: `coverage.json`, the docs table,
   `width_audit.json`, `emitted_bytes.json`, `m8/casts_fused.tsv`; only new rows and shapes may
   move.
7. **The matrix** over the new conformance suite and the changed suites (step 15).

## 4. Predictions and costs

1. No existing shape's bytes move; the new shapes are the casts and the NTZ rows.
2. `dev/varka_new_lane.py --check` passes for the long lane and for `TimestampNTZType` once the
   coverage rows exist.
3. The census counts NTZ as fused for the operators of 2.2 and the two casts, and owes the rest
   to 339 and 340.

Cost: small, as the row says, plus the moved division: about two days. No benchmark gate: the
change adds no lowering a measurement would decide between, and a benchmark of the new types is
row 377's.

## 5. Order and files

1. The lane (2.1) with the flipped tests, the conformance suite and the coverage rows of 2.2.
2. The two casts (2.3) with their proof, edges and exhaustion.
3. The generated files, the gate, the matrix over the new and changed suites.

| file | what |
|---|---|
| `VarkaNodeCompiler.java` | `laneOf`'s arm; the casts; `floorDivLong` |
| `VarkaTimeCompiler.java` | `isTimestamp` for the zoned type alone; the reason |
| `sql/core/.../VarkaLaneArrow.java` | `TimeStampMicroVector` |
| `sql/varka/proofs/long_divide.smt2`, `VarkaProofFiles.java` | the floor-division claim |
| tests | the conformance suite, coverage rows, the edges, the two flipped tests, the fuzz fixture |
| generated | `coverage.json`, `docs/sql-varka.md`, `width_audit.json` |
| generated | `emitted_bytes.json`, `m8/casts_fused.tsv` |
| `sql/varka/plans/m8/PLAN.md` | rows 338 and 340, 2.6's note that the division moved |

## 6. Out of this row

NTZ arithmetic (339); the date and time fields, `TIME` to NTZ and NTZ to `TIME` (340); `hash`
over NTZ (337); the zoned `TIMESTAMP`; `TIMESTAMP_NTZ` as a partition or sort key in the
planner (nothing Varka-specific).

## 7. Declines that stay

`CAST(ts AS TIMESTAMP_NTZ)` and its reverse (the session zone); a zoned column anywhere; every
NTZ function of 339 and 340 until those rows, each with the reason naming the row.

## 8. Questions for the owner

None. The one departure from an earlier plan, building this row before 475's registry, is
section 0's, decided by the milestone's focus on the new types.

## 9. Done when

Row 338's text holds: each form of 2.2 and both casts fuse with coverage rows; `DATE` to NTZ is
guarded at `+-106,751,991` days, raising under LEGACY and ANSI and null under TRY, with
`try_cast` tested at `+-106,751,991` and `+-106,751,992`. And: the conformance suite passes, its
overflow check exempt as `BOOLEAN`'s is (0.8); the floor division is proven over the whole
long range and pinned at `Long.MIN_VALUE` and `Long.MAX_VALUE`; `dev/varka_census.py --check` and
`dev/varka_new_lane.py --check` pass; no existing shape's bytes move.

## 10. Outcome

(Written when the work lands.)
