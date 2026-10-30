# VARKA-332: The integral and boolean casts in the three modes

## 0. What exists now (admission check)

From master at `ef32e007d3a` (30 October 2026; Varka paths under
`sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen/` unless named, Spark
paths under `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/`), and from row 481's plan
on the local branch `varka-481` (`0e309d43adb`, not merged). The row's text is the milestone plan's
2.2 and 2.4. Spark's facts were read in the fork's sources; nothing here was run.

1. **Spark's modes.** `ansiEnabled` is true under ANSI and under TRY
   (`expressions/Cast.scala:720-722`); `canUseLegacyCastForTryCast` exempts only string sources
   (`:1602-1612`), so every pair here runs the ANSI code inside a `catch` that answers null, in
   the interpreted path (`:1586-1597`) and in codegen (`:1706-1716`). TRY is therefore "null
   exactly where ANSI throws, ANSI's value elsewhere" for every pair of this row.
2. **Integral to integral** (`Cast.scala`, `castToLong` `:1142-1166`, `castToInt` `:1177-1208`,
   `castToShort` `:1211-1272`, `castToByte` `:1275-1338`):
   - LEGACY: `numeric.toLong(b)`, `numeric.toInt(b)`, `numeric.toInt(b).toShort`,
     `numeric.toInt(b).toByte` (`:1161`, `:1203`, `:1267`, `:1333`), in codegen `(byte) c` and its
     kin (`:2508`, `:2546`, `:2583`): Java's narrowing, the low bits kept.
   - ANSI: `LongExactNumeric.toInt` for `BIGINT` to `INT` (`types/numerics.scala:112-118`) and
     `CastUtils.shortToByteExact`, `intToByteExact`, `longToByteExact`, `intToShortExact`,
     `longToShortExact` for the narrow targets (`expressions/CastUtils.java:51-73`; dispatched at
     `Cast.scala:1243-1246`, `:1307-1312`, codegen `:2411-2440`): `v == (target) v` or throw.
   - The widenings are exact in every mode.
3. **The error.** `QueryExecutionErrors.castingCauseOverflowError(t, from, to)`
   (`sql/catalyst/src/main/scala/org/apache/spark/sql/errors/QueryExecutionErrors.scala:79-89`):
   a `SparkArithmeticException` of condition `CAST_OVERFLOW`, SQLSTATE `22003`, with four
   parameters - `value` (`toSQLValue(t, from)`, so `128` for an int, `128L` for a bigint),
   `sourceType`, `targetType` and `ansiConfig` - and the message "The value <value> of the type
   <sourceType> cannot be cast to <targetType> due to an overflow. Use `try_cast` to tolerate
   overflow and return NULL instead." (`common/utils/src/main/resources/error/
   error-conditions.json:1009-1014`). A kernel never raises: a batch holding a value Spark raises
   on declines, and the row engine raises this error on the first such row, so the error matches
   Spark's by construction, as `CAST(bigint AS INT)` under ANSI does today.
4. **Boolean to and from integral.** To an integral type, `if (b) 1 else 0` in every mode
   (`Cast.scala:1148`, `:1183`, `:1221`, `:1285`; codegen `:2497`, `:2535`, `:2571`, `:2608`);
   from one, `x != 0` (`:862-869`, codegen `:2323-2324`). Neither can fail.
5. **Interval casts with integral types** (`catalyst/util/IntervalUtils.scala:814-995`). Spark's
   legality admits every field range both ways (`Cast.scala:302-308`, `:134-135`), multi-field
   ranges included. The helpers throw `CAST_OVERFLOW` whatever the mode, so LEGACY raises too,
   ANSI raises, TRY nulls:
   - `intToYearMonthInterval` (`:814-828`): `multiplyExact(v, 12)` for a YEAR end, `v` for a
     MONTH end (`YEAR TO MONTH` ends in MONTH). Overflow past `[-178956970, 178956970]`.
   - `longToYearMonthInterval` (`:830-839`): `v.toInt` checked first, then the int rule.
   - `intToDayTimeInterval` (`:886-902`): `multiplyExact` for a DAY end (overflow past
     `[-106751991, 106751991]`), a plain product for HOUR, MINUTE and SECOND, which cannot
     overflow from an int.
   - `longToDayTimeInterval` (`:904-919`): `multiplyExact` for every end: the bounds are
     `Long.MAX_VALUE / unit`, 106751991 days, 2562047788 hours, 153722867280 minutes and
     9223372036854 seconds, symmetric (each unit has a factor 5, so none divides `2^63`).
   - A `TINYINT` or `SMALLINT` source goes through the int rule (`Cast.scala:1110-1115`,
     `:1130-1135`) and never overflows.
   - `yearMonthIntervalToInt` (`:855-860`): `v / 12` (YEAR end) or `v`; `yearMonthIntervalToShort`
     and `ToByte` (`:862-884`) check the quotient's range and raise with the interval as `value`.
     To `BIGINT` it is the int widened (`Cast.scala:1164-1165`), which cannot fail.
   - `dayTimeIntervalToLong` (`:921-928`): `v / unit`, Java's truncating division; `ToInt`,
     `ToShort`, `ToByte` (`:956-990`) check the quotient's range and raise.
6. **`TIME` to integral is a three-way split the row text does not mention.** `timeToLong` is
   `floorDiv(nanos, 10^9)` (`Cast.scala:1038-1040`); to `BIGINT` and `INT` it cannot fail
   (`:1155`, `:1197`); to `SMALLINT` and `TINYINT` an out-of-range second is **null under
   LEGACY** through `errorOrNull` (`:1168-1174`, `:1234-1242`, `:1298-1306`; codegen
   `:2354-2379`), raises under ANSI and nulls under TRY. `DATE` to a number or `BOOLEAN` is null in
   every row under LEGACY and illegal otherwise (`Cast.scala:859-861`, `:1150`, `:1185`, `:1223`).
7. **What fuses today.** The compiler's integral casts are four arms in `arithmeticArm`
   (`VarkaNodeCompiler.java:509-536`): `CAST(bigint AS INT)` (`narrowing`, `:701-718`) is
   `NarrowLane` under LEGACY, `NarrowLane(GuardedRange(x, int range))` under ANSI, and declines
   under TRY naming this row (`:707-710`); `CAST(int AS BIGINT)` is `WidenLane`; a narrow value to
   `INT` is `WidenLane(x, INT)`, no instruction (`:534-536`). The interval arms are in
   `VarkaIntervalCompiler.java:115-176`: both MONTH relabels, `CAST(i AS INTERVAL YEAR)` through
   a checked multiply that `arithOver` declines, the year-month unit relabels, `CAST(ymy AS INT)`
   as `ConstDivide(x, 12)`, and `CAST(ymm AS INT)` only for `MONTH_INTERVAL` exactly
   (`:172-174`). `VarkaTimeCompiler.java:174-195` holds the `TIME` and day-time relabels.
8. **The IR pieces** (`varka/VarkaVectorIR.java`): `Overflow { WRAP, FAIL, NULL }` (`:415`);
   `GuardedRange(child, lo, hi)` (`:536-545`), which only declines a batch; `WidenLane` and
   `NarrowLane` with a target lane (`:563-633`), which already admit a `BYTE` child under a `LONG`
   target; `ConstDivide` (`:764-860`), exact at the long lane only under a stated bound of at most
   `2^52`; `CondValue(cond, lane)` (`:1224-1237`), which refuses a narrow lane "until a row that
   needs it (a cast to TINYINT or SMALLINT) tests its lowering". `floorDivLong`
   (`VarkaNodeCompiler.java:787-810`) is exact over every long for an even divisor whose shifted
   magnitude stays under `2^52`: of the four day-time units only the DAY's (`2^13 * 10546875`)
   passes; HOUR (`2^10`), MINUTE (`2^8`) and SECOND (`2^6`) are refused (checked in Python here).
9. **A narrow value under a long kernel has no lowering.** `VarkaKernelPlan.java:297-300` refuses
   it ("a narrow value under a bigint kernel has no lowering"), and `Analysis.java:1236-1241` holds
   the same rule per node; so `byte`/`short` to `BIGINT` and back decline. `VARKA-343.md` 2.2 says
   the first of rows 332 and 343 to land builds that load, for `CAST(y AS DOUBLE)` too.
10. **The census has no direct narrow-long conversion cell.** `operator_census.json` holds `B2I`,
    `S2I`, `I2B`, `I2S`, `B2S`, `I2L`, `L2I` and the part-form byte loads, all intrinsic where
    they run on the laptop; no `B2L`, `S2L`, `L2B` or `L2S`.
11. **The null disposal of a range guard is planned twice and built nowhere.** Row 481's plan
    (local branch, owner's choice B of 25 October 2026) gives `GuardedRange` a `FAIL`/`NULL` mode;
    `VARKA-343.md` 4 builds the same disposal in 343b and says "332's plan, when written, takes
    this disposal rather than building its own". 343 is wave F2, after this row; 481 has no
    start. Row 338 took the other road for TRY: a `FAIL` guard whose declined batch the row
    engine nulls, "a correct answer computed slower" (`VARKA-338.md` 0, item 4).
12. **The cast register and the probe.** `VarkaCastMatrixSuite` compiles each form as a
    projection, and a `BOOLEAN` target as a predicate since a boolean output is row 27's
    (`VarkaCastMatrixSuite.java:155-170`). `dev/varka_census.py:766-784` gives `332` to every pair
    no other rule claims, which is why `TIME` and `DATE` pairs carry it. The conformance contract's
    cast check reads each form's answer from `casts_fused.tsv` and holds an ANSI cast only to
    declining (`VarkaTypeConformance.java:252-272`, `:344-351`), the gap the done-when's clause of
    27 October closes.
13. **No cast proof file exists**: `sql/varka/proofs/` holds `narrow_range.smt2`,
    `narrow_wrap.smt2` and `long_divide.smt2`, and the `casts.smt2` row 490 names is not built.
14. **The sweep budget is not in `PLAN.md` yet.** Row 330's first 2^32 sweep took 1106 s
    (`results/VARKA-330-sweep.txt`), but row 330 is "Started" with the budget among what is left,
    and `PLAN.md` 10.3 item 5 does not hold the number; the F1 wave row makes 332's sweeps wait on
    it.

**Coverage, from `m8/casts.tsv` and `m8/casts_fused.tsv`.** The row's text names four groups, 36
pairs and 124 forms (identities apart, which the optimizer removes and the register leaves out):

| group | pairs | forms | fused in every mode today |
|---|---:|---:|---|
| integral to integral | 12 | 12 | 3 (to `int` and `bigint`); `bigint` to `int` in LEGACY, ANSI |
| `BOOLEAN` with integral | 8 | 8 | 0 |
| integral to interval | 8 | 52 | 2 (`int` to the MONTH ends); `int` to DAY in context only |
| interval to integral | 8 | 52 | 2 (`INTERVAL YEAR` and `MONTH` to `int`) |

The register gives 332 four more groups by its fallthrough (item 12), none in the row's text:
`TIME` to integral (4 pairs, 12 forms, none fused), `TIME` narrowed (6 forms, 3 fused),
day-time to day-time (90 forms, 36 fused), `DATE` to a number or `BOOLEAN` (5 forms, LEGACY only,
none fused). Whether they are this row's is open (section 8 leaves it to the lead's review first).

### What differs from the row text and the milestone plan

- **The F1 wave row's "332 ... is the casts' harness 338 builds on" is stale**: the harness is
  rows 484 and 486, and 338 is done without this row (`VARKA-338.md` 0).
- **`TIME` to `SMALLINT` and `TINYINT` nulls under LEGACY** (item 6), where the row's mode split
  says wrap or raise; if those pairs are in, they need the null disposal in LEGACY too.
- **Interval casts raise under LEGACY** (item 5), as the row says, and so do the interval-to-narrow
  targets the register calls "narrower targets check".
- **Multi-field intervals cast to and from integral types** (item 5): `VarkaIntervalCompiler`'s
  comment "Only a single-field interval casts to an integral type" (`:158-161`) is wrong, and
  `CAST(INTERVAL YEAR TO MONTH AS INT)`, the MONTH-end identity, declines because the arm matches
  `MONTH_INTERVAL` alone. Both are corrected here.
- **The day-time units below a day have no exact full-range division** (item 8); 2.4 names the
  guard this plan takes first.
- **The narrow load at the long width is this row's** (item 9), as 343's plan foresaw.

## 1. Goal and non-goal

Every form of the four groups of section 0 fuses in LEGACY, ANSI and TRY with the row engine's
answers: LEGACY's wrap and the interval casts' raise, ANSI's `CAST_OVERFLOW` by a declined batch,
TRY's null on the lane, a `BOOLEAN` target as a condition. Not this row: a `BOOLEAN` value output
(27), the floating (343) and decimal (350, 364) casts, string casts, the fallthrough groups unless
the lead's review puts them in.

## 2. Design

### 2.1 One table of targets, one shape per cast

Every cast of the row is one of five compositions over existing nodes; the compiler builds them
from a table keyed by (source, target), so no pair has a hand-written arm:

| kind | tree | pairs |
|---|---|---|
| widen | `WidenLane(x, target)` | integral to a wider integral |
| narrow | `NarrowLane(g(x, target range), target)` | integral to a narrower integral |
| test | `Not(Compare(EQ, x, 0))` as a condition | integral to `BOOLEAN` |
| select | `CondValue(c, target lane)` | `BOOLEAN` to integral |
| scale | `IntArith(MUL, WRAP, g(x, bound), unit)` or a division, then a lane change | intervals |

`g(x, lo, hi)` is the identity under LEGACY for an integral narrowing (the wrap), and a range guard
otherwise: declining in ANSI and, for the interval casts, in LEGACY; nulling in TRY (2.3). The
ranges are the target types' `MIN_VALUE` and `MAX_VALUE` and section 0 item 5's bounds, held in
one place (`VarkaTypes`' entry for the target, beside its lane), so rows 343 and 350 read the same
numbers.

The new arms live in `VarkaIntegralCastCompiler` (a family of its own or the arithmetic family's,
which the lead's review settles first, since moving row 28's arms changes what the arithmetic
switch turns off); `narrowing`'s TRY decline goes.

### 2.2 The kernels, per cast

| cast | Vector API operations, per lane group |
|---|---|
| `byte`/`short` to `int` | the widening load (`B2I`/`S2I`), today's |
| `byte` to `short` | WIDENED: nothing (both on the int lane); NATIVE: `B2S` |
| `byte`/`short` to `bigint` | the narrow load at the long width (2.5), then `I2L`, part 0 |
| `int` to `byte`/`short`, LEGACY | WIDENED: the shift pair, or the store alone; NATIVE: `I2B` |
| `bigint` to `int`/`short`/`byte`, LEGACY | `L2I`, part 0, then as the row above |
| `short` to `byte`, LEGACY | WIDENED: the shift pair at the int lane; NATIVE: `S2B` |
| ANSI and TRY narrowing | `compare(LT, lo)`, `compare(GT, hi)`, `or`; collected or cleared |
| integral to `BOOLEAN` | `compare(EQ, 0)`, known-true and known-false swapped |
| `BOOLEAN` to integral | `zero.add(1, kT)`, validity `kT \| kF` (`VARKA-460.md`) |
| `int` to YEAR, DAY | item 0.5's guard, `mul` by 12 (int lane) or `I2L` and `mul` by the unit |
| `int` to HOUR, MINUTE, SECOND | `I2L`, `mul` by the unit: exact for every int, no guard |
| `bigint` to an interval | item 0.5's guard (year-month: the int range, then YEAR's), `mul` |
| narrow source to an interval | the widening, `mul`: no guard |
| year-month to integral | `ConstDivide(x, 12)` (YEAR end); a narrow target's guard; `I2L` |
| day-time to integral | 2.4's truncating division, then the guard for `int`, `short`, `byte` |

The laptop's census (`operator_census.json`) reads the cells the table uses intrinsic at 512 and
256 bits: the conversions of item 0.10, `long.MUL`, `long.LT` and `long.GT`, the short and byte
operators. At 128 bits the long compares are the Java fallback (two long lanes, `m7` row 374), so
a guard over a `bigint` or day-time value there is the fallback, recorded in `width_audit.json`.
The census has no `S2B`, `B2L`, `S2L`, `L2B` or `L2S` cell, so the narrow-long forms compose
through the int lane and the NATIVE `short` to `byte` needs a cell before it is emitted; a direct
`B2L` is the alternative the lead's review weighs (census cells first, as `VARKA-343.md` 5 step 2
does).

### 2.3 What declines a batch and what raises

A kernel never raises. A live lane that Spark raises on declines its batch through the guard's
collect (`VALUE_RANGE`), and the row engine raises `CAST_OVERFLOW` with Spark's parameters on the
first such row (item 0.3). A null lane never declines. Under TRY the lane outside the range becomes
null and the batch runs on, by `GuardedRange`'s `NULL` disposal. Which row builds that disposal is
the open point of item 0.11; this plan's design takes it from row 481's plan as written there (a
mode on the node, in `canonical`, `nullsFromValidInputs`, the masked body) and builds it here if it
has not landed when 332's second pull request starts. Until it exists, TRY could fuse as 338's
does, by the declining guard (a correct answer, slower on a batch holding one out-of-range value);
that interim form is not this row's done state.

The `BOOLEAN`, widening and narrow-source interval casts cannot fail, so their three modes are one
kernel; the shape key separates the others by the guard's mode.

### 2.4 The day-time division

`dayTimeIntervalToLong` truncates over every long. For a DAY end the exact full-range form is
`floorDivLong` over a biased dividend: `trunc(x / d) = floorDiv(x + ((x >> 63) & (d - 1)), d)`,
whose bias cannot overflow (a negative `x` gains at most `d - 1`), three lane operations more
than 338's floor division and proven in `long_divide.smt2` beside it. For HOUR, MINUTE and SECOND
`floorDivLong` refuses the divisor (item 0.8), so the first form is `ConstDivide` under a
`GuardedRange` of `|x| < 2^52` microseconds (about 142 years), whose declined batch the row engine
computes, as 338 guarded before its exact form; a full-range division for those units is the
alternative the lead's review weighs, shared with row 103's truncating quotient (`rows` =
`103,332`).

### 2.5 The narrow load at the long width

A long-driven kernel reading a `TINYINT` or `SMALLINT` column loads it widened to the int lane in
the int species of the long species' width, the low half, as an int column is (`VARKA-28.md` 9),
then `I2L`. The byte loads at four long lanes and below take 329's part form (`VARKA-329.md` 9.6):
an eight-lane 64-bit byte species converted in parts, the loop bound on whole iterations.
`VarkaKernelPlan`'s and `Analysis`' refusals (item 0.9) narrow to the forms still without a
lowering; the NATIVE arm computes such a kernel as WIDENED does, since a long value is present
(`VARKA-329.md` 2.3, third case). The narrowing store of a long value to a byte or short column
is `L2I` then the narrow store of the int lane. Both are written once for 343's
`CAST(y AS DOUBLE)`.

### 2.6 Compiler, reference, cost, warm-up

- `CondValue` admits `BYTE` and `SHORT` (its javadoc's condition): under WIDENED it emits at the
  int lane, under NATIVE at the narrow lane.
- `VarkaReference` needs the narrow `CondValue` and, if this row builds it, the guard's `NULL` arm.
- The range analysis bounds a `CondValue` to `[0, 1]`, so `CAST(b AS TINYINT)` under a narrow
  consumer emits no shift pair.
- Cost: no new node; the guard's mode is a cost key as row 481 states. The warm-up blocks gain the
  narrow load at the long width, held by `VarkaWarmupReachSuite`.
- **What 343 and 350 build on**: the target table of 2.1, the `NarrowLane(g(x))` shape for a narrow
  target (343's `NarrowLane(ConvertLane(x, INT))` under the same guard), the guard's `NULL`
  disposal, the narrow load at the long width, and `CondValue` at every integral lane (343 adds the
  floating lanes, 350 a scaled select for `BOOLEAN` to decimal).
- **What 368 needs**: Spark's cast suites build `Cast(Literal(v), to, mode)` and check it with no
  constant folding under 368's harness, so each arm accepts a `LiteralSlot` child exactly as a
  column; a TRY out-of-range literal must be nulled in the kernel, or 368 records a declined batch
  where the matrix says fused; and an ANSI out-of-range value declines with `VALUE_RANGE`, which 368
  reads apart from a compile decline. *Inference, not checked:* whether a projection whose only
  leaf is a literal reaches a kernel at all is 368's harness question (it may lift the literal to a
  one-row column); 332 makes either route meaningful.

## 3. Tests and proofs

1. **Exhaustion, narrow and boolean sources**: every `TINYINT` (2^8) and `SMALLINT` (2^16) value,
   one batch, through every pair and form from them in the three modes and both narrow arms, held
   to Spark's own `Cast.eval` on every row and to the reference; the `BOOLEAN` source's four leaf
   states (null with either data bit) at every offset and both widths, as 331's oracle.
2. **Exhaustion, int and year-month sources** (2^32 each), one sweep per distinct tree, the
   modes sharing a tree sharing a sweep: `int` to `byte` and `short` (wrap, decline, null) on both
   arms, 12; `int` to `BOOLEAN`, 1; `int` to YEAR and to DAY (decline, null), 4; `int` to HOUR,
   MINUTE, SECOND, 3; year-month YEAR and MONTH ends to `byte` and `short` (decline, null) on both
   arms, 16; to `bigint`, 2. **38 sweeps**, 42028 s at row 330's floor of 1106 s each (11 h 40
   min); one arm per tree is 24 sweeps, 26544 s. They wait on 330's budget in `PLAN.md` (item
   0.14), run opt-in in the nightly and idle windows with `-Dspark.test.timeout`, the first timed
   before the rest, and PR CI runs a 2^24 slice of each.
3. **SMT for the 64-bit sources** (`bigint`, day-time), in a new `sql/varka/proofs/casts.smt2`
   (row 490's file; whichever lands first creates it) under Z3 and cvc5, over integers as the
   proofs' README prescribes: the wrap of `L2I` and the shift pair equals reduction mod `2^w`; the
   guard's two compares hold exactly where `v != (target) v`; each interval bound is the largest
   `|v|` whose product fits (the `multiplyExact` condition); the biased floor division of 2.4
   equals truncation over every long; the `2^52` guard's `ConstDivide` equals Java's `/`
   (`long_divide.smt2`'s claim, cited). The 16-bit narrowing rules join `narrow_range.smt2`.
4. **Named cases**, each in three modes and with a null beside it: `127`, `128`, `-128`, `-129`
   to `TINYINT` from each wider source; `32767`, `32768`, `-32769` to `SMALLINT`; `2^31` and
   `-2^31 - 1` as `bigint` to `int`; `Long.MIN_VALUE` and `Long.MAX_VALUE` to every narrower target;
   178956970 and 178956971 to `INTERVAL YEAR`; 106751991 and 106751992 to `INTERVAL DAY` from
   `int` and `bigint`; each `bigint` bound of item 0.5 and the next value; `INTERVAL '-1' DAY` less
   one microsecond to `bigint` (truncation toward zero gives 0); `2^52` microseconds either side.
5. **Coverage rows**, a "casts, integral and boolean" family over the fixture's `y1`, `s1`, `i`,
   `l`, `b`, `ym` and `dt` columns: each form of section 0's four groups once, the `BOOLEAN` targets
   as filters, each naming its verification (row 490's rule): the exhaustive suite, a `casts.smt2`
   section, or `differential`. `VarkaCastMatrixSuite` regenerates `casts_fused.tsv`, and
   `dev/varka_census.py` the `fused_today` column.
6. **The conformance contract.** Each integral and interval type's cast row answers; the ANSI
   narrowing forms gain an in-range value set (`Form.over`), so an ANSI cast's answers are compared
   with Spark's and not only its declines (the done-when's clause of 27 October 2026).
7. **The fuzzers.** The IR grammar draws the five compositions of 2.1 with each guard mode, in a
   sequence of its own seed; the Spark-level fuzzer gains the in-scope pairs of `casts.tsv` as
   cast productions, each mode, leaving the rest of the register to row 352.
8. **Spark's suites (368).** A dry run of `CastWithAnsiOffSuite`, `CastWithAnsiOnSuite` and
   `TryCastSuite` through 368's harness, if it exists by then, for this row's pairs only; 368 owns
   the record and the nightly diff.
9. **The bytes oracle**: `emitted_bytes.json` adds rows and moves no existing shape, except
   `CAST(bigint AS INT)` under TRY, which becomes a kernel.

## 4. The baseline, and predictions

**The baseline first, its own pull request on the unchanged code:**
`VarkaIntegralCastBaselineBenchmark` in sql/core, on `VarkaFloatingBaselineBenchmark`'s pattern
(two arms, a fuse check, `benchmark.yml` on the EPYC 7763 at 256 and 128 bits): `CAST(i AS
TINYINT)` in the three modes, `CAST(l AS SMALLINT)` and `CAST(y AS BIGINT)`, `CAST(b AS INT)` and
`WHERE CAST(i AS BOOLEAN)`, `CAST(i AS INTERVAL YEAR)` and `CAST(dt AS BIGINT)`, without and with
one row in ten null. The fuse check expects no Varka node in those sections; a control section,
`CAST(i AS BIGINT)`, already fuses and is labelled so, its check inverted.

Predictions, registered before the build:

1. Against the baseline at 256 bits, the narrowings and the selects 3 to 10 times faster than the
   row path; the TRY forms within 1.3 times of their LEGACY twins.
2. `CAST(dt AS BIGINT)` (a guarded double division) within 1.5 times of `CAST(i AS INTERVAL
   YEAR)`.
3. At 128 bits the int-lane forms keep at least 2 times; the long-driven forms with a byte load
   tie the row path or better.
4. No existing shape's bytes move but `try_cast` of a bigint to int.

## 5. Order, files and size

1. **The baseline** (4), its own pull request.
2. **332a, the integral and boolean casts**: the target table, the narrow load and store at the long
   width, the narrowings in LEGACY and ANSI, the tests, the selects (`CondValue` at narrow lanes),
   the sweeps of narrow and boolean sources, the SMT sections for `bigint`, coverage rows.
3. **332b, the intervals and TRY**: the guard's `NULL` disposal if not landed, every TRY form, the
   interval casts and the day-time division of 2.4, the multi-field fix, the int and year-month
   sweeps after 330's budget, the conformance in-range sets, the measurement and its verdict.

| file | what |
|---|---|
| `VarkaIntegralCastCompiler.java` (new), `VarkaNodeCompiler.java` | the table, the arms |
| `VarkaIntervalCompiler.java` | the interval casts, the multi-field arms and comment |
| `varka/VarkaVectorIR.java` | `CondValue` at narrow lanes; `GuardedRange`'s mode if built here |
| `varka/VarkaKernelPlan.java`, `Analysis.java`, `VarkaVectorWalk.java` | the long-width load |
| `varka/VarkaTypes.java` | the target ranges |
| `varka/VarkaRangeAnalysis.java`, the warm-up | `CondValue`'s bound; the reach |
| `sql/varka/proofs/casts.smt2`, `narrow_range.smt2`, `long_divide.smt2` | 3.3 |
| `VarkaProofFiles.java` | 3.3's rendering |
| tests | sweeps, named cases, conformance sets, fuzz productions |
| benchmark | `VarkaIntegralCastBaselineBenchmark`, its queries suite and results files |
| generated | `coverage.json`, `docs/sql-varka.md`, `emitted_bytes.json`, `width_audit.json` |
| generated | `casts_fused.tsv`, `casts.tsv`, `census.tsv` |

**Size: medium, in three pull requests.** The baseline half a day; 332a about four days (the long
width load is most of it); 332b about four, one more if the disposal is built here. The sweeps
add machine time (3.2), not gate time; the gate gains the 2^24 slices and the narrow sweeps, of
the order of the narrow suites' seconds.

## 6. What it unlocks in the census

When the row lands, every form of the four groups fuses in the three modes. Of the 36 pairs, 29
fuse in no form today, 4 in part (`bigint` to `int`, `int` to year-month, year-month to `int`,
`int` to day-time in context) and 3 whole. The census's `cast` and `try_cast` entries have an empty
`fused_today` and list every type (`census.tsv`). *Inference, not checked in `dev/varka_census.py`:*
those entries credit a type only once every cast from it fuses, in which case the integral types
and `bool` also wait on 343's and 350's pairs. The fallthrough groups count only if they are put in.

## 7. Out of this row

Out: a `BOOLEAN` value output (27), the floating, decimal and string casts, `TIME` and `DATE`
pairs and the day-time narrowings unless the lead's review puts them in, `try_eval`, a full-range
division for the sub-day units if 2.4's guard is kept.

Declines that stay, each naming its row: an ANSI or interval batch holding a value Spark raises
on (the row engine raises it), a day-time value past `2^52` microseconds under a sub-day unit
(2.4), a narrow value in a long kernel at a lane count with no byte species (329's refusal).

## 8. Questions for the owner

1. **The sweep budget.** Section 3.2's 38 sweeps of 2^32 are about 11 h 40 min at row 330's floor;
   one arm per tree, the cut 330's record leaves to the owner, is 24 sweeps and 7 h 22 min, with
   SMT proving the formulas for both arms. (a) Keep both arms. (b) Cut to one arm, the default.
   *Recommended: (b)*: the arms differ only in the narrowing's instruction, which `narrow_wrap.smt2`
   and `narrow_range.smt2` prove for both, and the narrow-source sweeps stay on both arms.

## 9. Done when

Row 332's text holds, with the corrections above: every form of section 0's four groups fuses in
the three modes, the `BOOLEAN` targets as conditions; the narrow, boolean, int and year-month
sources are exhausted against Spark's `Cast` under question 1's answer and the 64-bit sources
proven in `casts.smt2` under both solvers; the named cases pass; each integral and interval type's
conformance suite compares an ANSI narrowing's in-range answers with Spark's; `casts_fused.tsv`,
`casts.tsv` and the census agree with the compiler; no existing shape's bytes move but the one
named; the measurement against the baseline is committed with its predictions scored; and the PR
build and the option matrix pass.

## 10. Outcome

(Written when the work lands.)
