# Varka Milestone 8 Plan: every numeric and temporal type

*Opened 14 October 2026, while milestone 7 runs. The owner set the theme the same day: new data
types, and more casts and expressions on the existing ones. The decisions in section 1 are his
and fixed; this plan works inside them.*

This is a task plan, not a scope catalogue. Under the house rule (`sql/varka/AGENTS.md`), the
catalogue moved forward in the same pull request: the catalogue that sat in this folder is now
`m9/SCOPE.md`, with its item numbers unchanged, its reading note (`READING_LAYOUTS.md`) and the
plans of the two rows it holds (`VARKA-64.md`, `VARKA-73.md`) beside it, and every inbound
reference rewritten by `dev/varka_catalogue_move.sh 8 9`, which a later merge reruns. The items
this milestone takes are named below with the number they have there: 1, 2, 31, 32, 33, 34, 35,
36, 54, 55, 89 (bullet 1, and bullet 6 without its runtime bound), 93 and 94. So are the rows it
takes from the catalogue's moved-row tables (items 15 and 39), which keep their task numbers and
their designs where they are: 27, 28, 30, 39, 89, 95, 103, 104 and 157. Four of those already
have plan files in this folder (`VARKA-28.md`, `VARKA-39.md`, `VARKA-89.md`, `VARKA-104.md`).

*Owner's decision, 14 October 2026, on the math family: item 36's first option, a ULP contract
behind a SQL configuration (2.7.1, row 345). The functions it covers are fused within a stated
bound, the one deliberate departure from bit-identical answers, recorded as such in
`SCOPE_STANDARD_MODE.md`.*

*Owner's decision, 14 October 2026, on decimals: precision 18 binds the inputs, every
intermediate and the result type, as Spark's type rules compute them, and no per-batch value bound
admits a wider declared type (2.8). TPC-H q6 at its specification's types is not fused by this
milestone.*

*Owner's decision, 14 October 2026: no milestone 7 row moves.* The rows still open in
`m7/PLAN.md` stay there and close there; nothing below depends on one except where a row says so.

## 1. The question, and what "done" means

Varka fuses six Spark types on two lanes. `VarkaNodeCompiler.laneOf` admits `INT`, `DATE` and
year-month intervals on a 32-bit lane and `BIGINT`, `TIME` and day-time intervals on a 64-bit one;
`VarkaEvaluatorBase.isArrowBacked` reads their six Arrow vectors and nothing else;
`VarkaKernelEvaluator.allocateVector` writes the same six. Everything else a query computes over a
number - a `DOUBLE` price, a `DECIMAL` amount, a `BOOLEAN` flag, a `SMALLINT` code, a
`TIMESTAMP_NTZ` event time - goes back to Spark's row engine at the first node that touches it.
And on the six types it has, Varka fuses 62 of the 182 expressions the census below finds in
scope, 15 of them for every type they take.

The owner's theme is the answer: **new data types, and more casts and expressions on the existing
ones.**

* **The type set.** The existing six, checked against the code above, plus seven new ones:
  `BOOLEAN`, `TINYINT` (`BYTE`), `SMALLINT` (`SHORT`), `FLOAT`, `DOUBLE`, `TIMESTAMP_NTZ` and
  `DECIMAL` with precision at most 18. A decimal of higher precision declines.
* **Out of scope, owner's decision, 14 October 2026:** `STRING` itself and every cast to or from
  it; `TIMESTAMP` with the local time zone; `DECIMAL` above precision 18.
* **The expressions:** "Explore all expressions, operators and functions which support existing
  and new types only. We should support most of them." Section 2.1 is that exploration, committed
  as a file a script regenerates, and 1.1 item 2 turns "most" into a number.
* **The milestone ends with a post**, named in 1.1 item 7.

### 1.1 Done when

1. **The seven new types are read and written by kernels.** Each has a coverage row for a load, a
   comparison, a choice and an output, its Arrow vector admitted by `isArrowBacked` and allocated
   by `allocateVector`, the IR fuzzer and the Spark-level fuzzer drawing it (352), and the bytes
   oracle unchanged for every existing shape at each lane's landing. The two representation
   questions with machine-dependent answers are built both ways and measured, the losing arm kept
   (329 for `BYTE` and `SHORT`, 347 for the decimal load path).
2. **Most of the census is fused.** Of the 182 in-scope entries of `m8/census.tsv`, at least 122
   (two thirds) are fused for every type they admit, and of its 882 entry-and-type pairs at least
   662 (three quarters); every pair left unfused carries its reason in the census, each reason one
   of: the math configuration switched off (2.7.1), a hard case declined (346), or a named row of
   `m9/SCOPE.md`. Today the counts are 15 entries and 96 pairs. A type counts as fused only where a
   coverage row reads a column of it, which `dev/varka_census.py` checks against `coverage.json`
   and row 328 makes exact.
3. **The cast matrix is fused and proven.** Every one of the 106 legal pairs of `m8/casts.tsv`
   is fused under LEGACY, ANSI and TRY where the mode admits it, or declined with its reason; each
   proven as section 5 assigns, by exhaustion where the source has at most 32 bits (62 pairs), by
   the SMT proofs of `dev/varka_prove.sh` where the source is a 64-bit integer and the rule an
   integer one (32), and by sampled regions and the fuzzer where the source is a double or the pair
   rounds a 64-bit value to a floating type (12).
4. **Spark's semantics hold at the edges and are tested there**: NaN and both zeros in every
   floating comparison, choice and `IN` (item 89 bullet 1's table against
   `SQLOrderingUtil.compareDoubles`); a zero divisor null under LEGACY and an error under ANSI for
   every numeric type; integral overflow wrapping at its own width under LEGACY and raising under
   ANSI, `TINYINT` and `SMALLINT` included; decimal rescaling `ROUND_HALF_UP`, and decimal overflow
   null under LEGACY and an error under ANSI; the ANSI form comparing error classes, as
   `VARKA-138.md` built it, on every new type.
5. **The math family is fused under the ULP contract** (345, 356 to 358): each of its 27 entries
   states its bound per host library (SVML at AVX-512, SVML at AVX2, SLEEF on NEON) in the coverage
   table and the user support table, its test oracle compares within that bound, it is gated by
   `spark.sql.codegen.varka.math.ulpContract.enabled`, and the departure is entry 3.4 of
   `SCOPE_STANDARD_MODE.md`. Every other expression stays bit-exact.
6. **The existing types are filled in**: `BIGINT` arithmetic (104), the integral division shapes
   (30, 95, item 33), the day-time interval expressions (103), `date - date` (39), the year-month
   divisions (89), the bitwise family (item 32), null-safe equality (item 34), the choice shapes of
   items 55 and 94, and `IS NULL` over a predicate (item 93).
7. **The post is published**: *One engine, every number: Varka across Spark's numeric and temporal
   types* (355). The claim it has to earn is the census's: what share of Spark's scalar expressions
   over these thirteen types a kernel now answers, bit for bit with the row engine (or within the
   stated contract for the math family), and how that is known - the exhaustive sweeps, the SMT
   proofs, the fuzzers - with every declined case named. A share without the "how it is known"
   half is not the post.
8. Beside the spine, the support table users read (353, item 54) states the census per type, and
   the milestone closes on 354, which reads this list row by row.

## 2. Design

Each row's design is in the item or plan it comes from, where it has one; this section says what
the milestone adds and how the rows meet. Every count below comes from `dev/varka_census.py` at
commit `90b8871b705` (14 October 2026), whose output is the two committed files.

### 2.1 The census

`dev/varka_census.py` reads `FunctionRegistry.scala` and writes `m8/census.tsv`, one line per
registered name and per implicit operator. The mechanical half - every name, the class or builder
it registers, the registry section - comes from the source; the judgement half - which types each
admits within the set, its result, what Varka fuses today and what is left - is
`m8/census_curated.txt`, read against each class's `inputTypes` and `dataType`. The script fails
rather than guess: a registered name with neither a curated line nor an exclusion, a curated name
the registry no longer has, a class the sources do not define, or a type claimed fused for a class
that no row of `coverage.json` both names and reads a column of, and any registry entry line it
did not read. The last-but-one is a check and not a proof - a coverage row naming several classes
credits each with every column it reads - and row 328 adds the single-expression coverage rows
that make it exact. A type fused only over a derived operand (`year(d) * 2` for an int multiply)
is marked, and its curated note must quote the coverage row. `--check` fails if either committed
file is stale, and row 328 runs it in the linters' job.

**The registry has 599 names, not 511.** `SCOPE_FUNCTIONS.md` counted 511 on 19 September 2026.
The same parse over the registry at `3fe328d55c0` (20 September) finds 595, with the sections
above its table differing too (math 69 against 61, datetime 85 against 64), so the old figure does
not reproduce by this method and is not used here; the four names since are upstream additions.
599 names register 534 classes. Beside them, 27 operators reach a plan without a registry entry
of their own (`try_cast`, `InSet`, `SubtractDates`, `SubtractTimestamps`, `TimestampAdd`, the
interval multiplications, divisions and extracts, `CheckOverflow`, `MakeDecimal`, `UnscaledValue`,
`TryEval`, the optimizer's relabels). `PromotePrecision` no longer exists in this tree; decimal
arithmetic computes its result type in `BinaryArithmetic` itself.

**What is out, and why.** 444 of the 599 names are out:

| reason | names |
| :--- | ---: |
| `STRING` input or output, or a parse of one (owner's decision) | 151 |
| aggregate, window or generator (Varka fuses scalar projections and filters) | 105 |
| nested types: array, map, struct, the vector functions | 65 |
| sketch state, `BINARY`, `VARIANT`, geospatial | 83 |
| `TIMESTAMP` with the local time zone (owner's decision) | 20 |
| a constant per query, folded before Varka sees it (`current_date`, `pi`) | 7 |
| nondeterministic, or a per-task fact (`rand`, `uuid`, `spark_partition_id`) | 9 |
| a `NULL`-typed or `CALENDAR INTERVAL` result | 4 |

**What is in.** 182 entries - 155 registered names and the 27 implicit operators - over 156
classes. By family, with the entry-and-type pairs they admit and the pairs fused today:

| family | entries | pairs | fused pairs | fully fused entries |
| :--- | ---: | ---: | ---: | ---: |
| conditional | 11 | 120 | 8 | 0 |
| predicate | 16 | 149 | 25 | 0 |
| math and arithmetic | 59 | 226 | 12 | 0 |
| bitwise | 13 | 41 | 0 | 0 |
| datetime | 41 | 102 | 40 | 11 |
| hash | 2 | 26 | 0 | 0 |
| conversion (`cast` and its aliases) | 11 | 104 | 0 | 0 |
| misc (bitmap positions) | 2 | 2 | 0 | 0 |
| implicit operators | 27 | 112 | 11 | 4 |
| **total** | **182** | **882** | **96** | **15** |

By type, pairs fused today over pairs admitted: `INT` 15 of 90, `BIGINT` 12 of 86, `DATE` 41 of 63,
`TIME` 13 of 49, year-month interval 10 of 55, day-time interval 5 of 57; and none yet of the new
types' `BOOLEAN` 41, `TINYINT` 75, `SMALLINT` 75, `FLOAT` 65, `DOUBLE` 101, `DECIMAL` 71,
`TIMESTAMP_NTZ` 54.

By the hardest work left once its lanes exist: 15 done, 73 plumbing (a compiler arm and IR on the
lanes), 60 semantics (overflow modes, NaN, rounding, decimal scale), 27 the math family under
its ULP contract (2.7.1), and 7 hard - `round`, `bround`, `truncate`, `%`, `mod`, `pmod` and
`try_mod`, each hard only on a floating operand (Spark rounds a double through `BigDecimal`, and
a floating remainder is `fmod`), and semantics on the integral and decimal ones.

Three properties of the census to read it by:

* **"Fused today" under-counts on purpose.** A type counts only where a coverage row reads a
  column of it: `docs/sql-varka.md` says year-month intervals fuse in the comparisons and `IN`, but
  no coverage row shows it, so the census does not count it. Row 328 adds the coverage rows that
  close such gaps, which moves the count without moving the compiler.
* **The cast entries count by source type**, as the census's other entries count by type: an
  alias admits exactly the sources `Cast` allows it (the script computes them from the matrix's
  legality, not by hand), and a source counts fused when every legal pair from it in
  `m8/casts.tsv` is fused in every mode it admits. So the conversion family's 104 pairs move with
  section 2.2's matrix.
* **The done-when share is over pairs and over entries.** Pairs weigh an expression by how many
  types it serves, so `=` over thirteen types is thirteen; entries weigh `factorial` the same as
  `+`. Either alone would flatter one kind of work.

### 2.2 The cast matrix

`m8/casts.tsv`, from the same script, lists every ordered pair of the thirteen types that
`Cast.canCast` (LEGACY, `Cast.scala` lines 249 to 340) or `Cast.canAnsiCast` (ANSI, lines 92 to
205) allows; `try_cast` follows `canTryCast`, which for these atomic types is `canAnsiCast`
(lines 211 to 239). Of 169 ordered pairs, 106 are legal under LEGACY and 98 under ANSI and TRY;
the eight LEGACY-only pairs are `DATE` to each numeric type and to `BOOLEAN`, all of which return
null for every row. Row 328 holds the transcription to Spark by a suite that asks `Cast` itself
about every pair, so the matrix cannot drift from the rules it copies.

**By what a pair does to a value:**

| kind | pairs | what it is, and the mode split |
| :--- | ---: | :--- |
| identity | 9 | same type |
| relabel, unit narrowing or truncation | 3 | year-month MONTH end and day-time SECOND end keep the value, `TIME` widening keeps it (all three fused today); a coarser end field or a narrower `TIME` precision divides |
| widening | 12 | integral to a wider integral, to a floating type that holds it exactly, `FLOAT` to `DOUBLE`; exact in every mode |
| narrowing | 6 | integral to a narrower integral: LEGACY wraps, ANSI raises, TRY nulls |
| rounding | 6 | `INT` or `BIGINT` to `FLOAT`, `BIGINT` to `DOUBLE`, `DOUBLE` to `FLOAT`, decimal to floating; round to nearest even, no error in any mode |
| truncation | 12 | floating to integral (LEGACY saturates to `INT` or `BIGINT`, NaN to zero, and for `TINYINT` and `SMALLINT` saturates to `INT` and then wraps; ANSI and TRY range-check and raise on NaN), decimal to integral (toward zero; LEGACY wraps, ANSI raises) |
| scale up | 13 | integral to decimal (overflow past `p - s` digits: null or error), `DATE` to `TIMESTAMP_NTZ` and integral to an interval (`multiplyExact`: raises in every mode) |
| rescale | 4 | decimal to decimal (`ROUND_HALF_UP` on a smaller scale, overflow null or error), `TIME` to decimal, decimal to an interval |
| floor division, floor modulo, unit division | 16 | `TIMESTAMP_NTZ` to `DATE` and to `TIME`, `TIME` and the intervals to integral and decimal targets |
| test, select | 14 | numeric to `BOOLEAN` (`x != 0`, NaN true), `BOOLEAN` to numeric (one or zero) |
| always null | 8 | `DATE` to numeric and `BOOLEAN`, LEGACY only |
| session date | 1 | `TIME` to `TIMESTAMP_NTZ` takes its date from `CURRENT_DATE`, which `ComputeCurrentTime` makes a literal per query |
| hard | 2 | `FLOAT` and `DOUBLE` to decimal: `Decimal(double)` goes through the double's shortest decimal string |

Decimal to floating is in "rounding" with a bound: `Decimal.toDouble` is
`BigDecimal.doubleValue`, correctly rounded, and an unscaled long `u` divided by `10^s` in double
arithmetic is also correctly rounded while `|u| < 2^53` and `s <= 22`, because both operands are
then exact and IEEE division rounds once. Above that bound the conversion of `u` rounds first and
the two can differ, so 350 guards the bound and declines a batch outside it.

**How each pair is proven** is by its source, since the source's width is what makes exhaustion
possible: every value of a `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `DATE` or year-month interval,
and every bit pattern of a `FLOAT` (2^32 of them), is tried against Spark's own `Cast` - 62 pairs.
A 64-bit integer source (`BIGINT`, day-time interval, `TIME`, `TIMESTAMP_NTZ`, the unscaled
decimal) cannot be exhausted, but where the rule is an integer one - a wrap, a range check, a
multiply by a power of ten, a division by a unit, 32 pairs - it is what milestone 7's SMT proofs
state (`VARKA-240.md`, `VARKA-241.md`), so each is proven there over the whole domain, beside
sampled regions and the fuzzer. The other 12 - a `DOUBLE` source, and a 64-bit source rounding to
`FLOAT` or `DOUBLE` - get sampled regions (both zeros, NaN, the infinities, the subnormals, each
integral boundary and its neighbouring values) and the fuzzer: `VARKA-241.md` found
floating-point theory does not decide in useful time. Seven pairs are fused today, each in
part (the note column says which part).

### 2.3 What the lane model has, and what each new type asks of it

Checked in the code on 14 October 2026:

* **`VarkaVectorIR.LaneType` is `{ INT, LONG }`**, carried by the leaves and derived everywhere
  else by an exhaustive switch over the sealed IR. The main sources name `LaneType.INT` or
  `LaneType.LONG` 55 times and the emitter's `Lane.INT` or `Lane.LONG` 26 times, across 16 files
  (`git grep -E "LaneType\.(INT|LONG)\b|\bLane\.(INT|LONG)\b" -- 'sql/*/src/main/*'`). A new
  lane type is a compile error at every exhaustive switch, which is what the Java rules want.
* **`Lane` derives each lane's descriptors from an integral scalar** (`CD_int`, `CD_long`): the
  broadcast, the lanewise and compare shapes, the shifts and the bitwise operators. A `FLOAT` or
  `DOUBLE` lane needs that enum split into its integral and floating halves before any floating
  arm exists - the operators differ (no shifts, no `AND`; `DIV` is a true divide), and so does
  what a comparison means (section 2.7). That is the structural reason the cheapest-first order of
  the owner's brief is right: `BYTE` and `SHORT` exercise a third and fourth width on integral
  descriptors, before the floating split.
* **A kernel reads a column as a `MemorySegment` over the Arrow data buffer with a byte stride**
  (`Lane.byteStride`). `TinyIntVector`, `SmallIntVector`, `Float4Vector`, `Float8Vector` and
  `TimeStampMicroVector` are the same shape at strides one, two, four, eight and eight;
  `BitVector` is not (one bit a row, section 2.5) and `DecimalVector` is sixteen bytes a row
  whatever the precision (`ArrowUtils.toArrowType` maps every decimal to `Decimal(p, s, 128)`,
  section 2.8).
* **Mixed widths wait on `VARKA-28.md`.** Today an emission carries one lane and the IR refuses a
  tree that mixes them, except for `NarrowLane`: a long value delivered as an int column, allowed
  only as an output root and only over values proven to fit (the `TIME` field extracts), which
  row 28 generalizes rather than replaces. Most of the cast matrix,
  the `TIMESTAMP_NTZ` calendar fields (a long dividing into the int32 calendar prefix) and every
  implicit promotion Catalyst inserts (`i + l` arrives as `CAST(i AS BIGINT) + l`) are mixed-width
  trees, so row 28 is wave 0 whatever the type order.

### 2.4 `TINYINT` and `SMALLINT` (329, 330, 332)

Spark computes `TINYINT + TINYINT` as a `TINYINT`: LEGACY wraps at eight bits and ANSI raises
when the int sum leaves `[-128, 127]` (`ByteExactNumeric.plus`); the same at sixteen bits for
`SMALLINT`. Comparisons, choice and `IN` are width-independent. There are two ways to give them
lanes, and which is faster depends on the machine - the case where the owner keeps both arms:

* **Widen onto the int lane.** Load eight or sixteen bits, sign-extend to int32 (`ByteVector` to
  `IntVector` by `convertShape` or a widening load), compute on the int lane the engine already
  has, and narrow at the store. Arithmetic then needs a re-narrow per node: LEGACY's wrap is a
  shift left and an arithmetic shift right by 24 (or 16), ANSI's check a range compare. No new
  descriptors; a quarter (or half) of the lanes per vector of a native lane.
* **Native `ByteVector` and `ShortVector` lanes.** Four (or two) times the elements per vector,
  wrapping arithmetic for free, ANSI overflow by the sign test the int lane uses; but two new
  `Lane` entries, and every mixed-width node with an int or long converts.

329 builds both behind an emit option, as `VARKA-248.md` 3.2's matrix expects of an option, and
measures them on comparisons, a three-node arithmetic chain and a mixed `TINYINT` and `INT`
projection at both vector widths. The default is the faster on the CI runners' widths; the other
arm stays, fuzzed and in the option matrix. 330 then gives both arms the arithmetic in the three
modes, proven by exhaustion over every pair of `TINYINT` operands (2^16) and every pair of
`SMALLINT` ones (2^32) for each operator.

### 2.5 `BOOLEAN` (331, 27)

A `BOOLEAN` column is not a lane: Arrow's `BitVector` holds one bit a row, with no byte stride.
What the engine already has is the right shape for it. A condition in a kernel carries a
known-true and a known-false mask (`VARKA-138.md`, item 93), and the validity bitmap is read and
written a word at a time. So a `BOOLEAN` column enters as a condition leaf - `VectorMask.fromLong`
over its data word, with known-true the valid set bits and known-false the valid clear ones - and
`AND`, `OR` and `NOT` over such leaves are Spark's three-valued logic with no new rule. It leaves
as row 27's boolean output: the mask packed into a `BitVector`'s data buffer, which is the
validity write the driver already does over a second buffer. Row 27's own measurement
(`m5/PLAN.md` 2.1) found packing the mask straight into the bitmap 1.18x faster at AVX-512 and
slower at 128 bits than materializing an int column, so it keeps both forms, as its text says.

With 27, every predicate the engine fuses can also be a projection output (`SELECT d < d2 AS
flag`), which is most of the predicate family's pairs at a stroke.

### 2.6 `TIMESTAMP_NTZ` (338, 339, 340)

Item 31's design holds: `TimestampNTZType` is microseconds since the epoch on the long lane,
evaluated in UTC (`zoneIdForType`), so its arithmetic is plain long arithmetic and its exact
helpers raise on overflow whatever the ANSI setting - the checked lowering is the only correct one,
and a batch with an overflowing lane declines to the row engine, which raises the same error.

* **338, the lane.** `TimeStampMicroVector` in `isArrowBacked` and `allocateVector`, one arm in
  `laneOf`; comparisons, choice, `IN`, the null tests; the casts with `DATE` - `DATE` to
  `TIMESTAMP_NTZ` is `multiplyExact(days, micros per day)`, which overflows the long past
  106,751,991 days either side of the epoch, so it is guarded; the other direction is a floor
  division.
* **339, the arithmetic.** `ntz - ntz` (a day-time interval), `ntz +/- dt`, `timestampadd` for
  the units up to a week, `time_bucket` over a day-time width: plain checked arithmetic. `ntz +/-
  ym` and `timestampadd` by months, quarters or years are calendar arithmetic on the date part, the
  `add_months` machinery over the day and the time of day carried beside it.
* **340, the decomposition.** `year`, `month` and the other date fields go through the date:
  `floorDiv(micros, 86400000000)` into the int32 calendar prefix, the first kernel where a long
  feeds the calendar machinery. That division needs an exact full-range form. A candidate the row
  proves: 86,400,000,000 is 2^11 times 42,187,500, so the floor division is an arithmetic shift by
  11 followed by a floor division by 42,187,500 of a value whose magnitude is at most 2^52 -
  inside `ConstDivide.EXACT_DIVIDEND_BOUND` except at its one edge, which a guard takes. `hour`,
  `minute` and `second` are a floor modulo by the day and divisions by constants; the casts with
  `TIME`, and `make_timestamp_ntz(date, time)`.

Out with the zoned type: `date_trunc`, `months_between` and `timestampdiff`, whose operands Spark
coerces to `TIMESTAMP`.

### 2.7 `DOUBLE` and `FLOAT` (341 to 346)

Items 35 and 36 scope the lane and the math family; three facts of Spark's semantics decide the
first rows:

* **Comparisons are not IEEE's.** `SQLOrderingUtil.compareDoubles` makes NaN equal to NaN and
  greater than every other value, and `-0.0` equal to `0.0`. Item 89 bullet 1 has the formulas
  (from Polars's total-order kernels and Spark's `genEqual`) and the constant-specialised forms; the
  same ordering decides `greatest`, `least` and `IN`. 341 ships the first floating comparison with
  item 89 bullet 1's table test of every operator.
* **A zero divisor is null or an error, not an infinity.** `Divide`, `Remainder` and `Pmod` are
  `DivModLike`: null under LEGACY and raised under ANSI when the divisor is zero (`-0.0` included,
  since `x == 0`). And `Divide` takes only `DOUBLE` and `DECIMAL`, so `i / j` over two ints arrives
  as a division of two casts to double - which makes 342 the row that fuses the commonest division
  users write.
* **Which operations are bit-exact.** `+`, `-`, `*`, `/` and `sqrt` are correctly rounded by IEEE
  754 in a lane exactly as in the scalar code, and `abs`, negation, `signum`, `degrees` and
  `radians` (`Math.toDegrees` is one multiply) are exact by construction; 342 ships them all. The
  transcendental functions are not: item 36's probe found every bound operator differing from the
  library Spark calls on up to thirteen percent of inputs on every host, so they are fused under
  the contract 2.7.1 describes.

#### 2.7.1 The math family's ULP contract (345, 356 to 358)

The owner chose item 36's first option on 14 October 2026. What it means, piece by piece:

* **A bound per function and per host library, stated where users read.** Each fused math function
  states in the coverage table and in the user support table (353) how far its lanes may be from
  `java.lang.Math`: "within 1 ulp" for most, "within 2 ulp" for `log10` everywhere and for `tanh`
  on x86, as item 36's measurements found (`SCOPE_FUNCTIONS.md` 3). The bound is per host library,
  since the JDK binds a different one on each - SVML at AVX-512, SVML at AVX2, SLEEF on NEON - and
  an operator the JDK leaves unbound on a host (`POW` at AVX2, `TANH` on NEON) runs the scalar call
  and is exact there. Item 36's probe measured fourteen operators against the library Spark calls,
  which is `StrictMath` for `exp`, `expm1`, `log`, `log10`, `log1p` and `pow`; 345 extends
  `dev/varka_canary/MathLaneProbe.java` to every function the family fuses (`asin`, `acos`,
  `sinh`, `cosh` and the composites `sec`, `csc`, `cot`, `log2`, `asinh`, `acosh`, `atanh` were
  not probed) and to both references, and states the larger distance, so a bound against
  `java.lang.Math` is also a bound against the row engine's answer. A function without a measured
  bound on a host is not fused there.
* **The oracle compares within the bound, and only for these functions.** The coverage oracle, the
  reference evaluator and the fuzzers compare a math function's lanes with the row engine's within
  its stated bound for the host they run on, and a composition containing one compares its outputs
  that way; every other expression, including `sqrt`, `degrees`, `radians` and the arithmetic, stays
  bit for bit.
* **A SQL configuration gates it**, because it is a departure:
  `spark.sql.codegen.varka.math.ulpContract.enabled`, on the two-surface model of
  `SCOPE_STANDARD_MODE.md` 4, with the emit option in the shape key. Off, the math family declines
  with a reason naming the configuration. The configuration audit suite runs after it is added.
* **One query can mix the two answers.** The ghost fallback serves a declined batch on the row
  engine, so with the configuration on, rows of one output column can come from a kernel in one
  batch and from `Math` or `StrictMath` in the next, and may differ in the last bit by batch. That
  is the mix a cluster already produces: HotSpot's `Math.sin` itself differs between x86 and
  aarch64 executors, so a column of `sin` over a mixed cluster is already not one bit pattern. The
  coverage table says so beside the bound.
* **The register records it.** `SCOPE_STANDARD_MODE.md` 3.4 is the first deliberate departure from
  "row answers follow Spark", with the reason, the bound and the configuration.

**The default is open** (section 7). On, users get the speed item 36 measured - 5x to 14x at eight
lanes, 2.5x to 9x at four, 1.1x to 3.9x at two - with answers a unit in the last place from vanilla
Spark's. Off, Varka keeps its promise that enabling it changes nothing a query returns, and the
speed is opt-in. The plan recommends **off**: the bit-identical promise is what the differential
suites, the ghost fallback's soundness and the posts all rest on, a user who wants the speed loses
one setting, and a default can be flipped once users have read the bound.

The rounding functions (344) are built, not called, since `VectorOperators` has no rounding
operator: `floor`, `ceil` and `rint` by the 2^52 construction, `floor` and `ceil` of a double
returning a `BIGINT` through the saturating conversion Spark uses. `round`, `bround` and
`truncate` over a double go through `BigDecimal` in Spark, and a floating `%` is `fmod`; those are
the hard entries, and 346 records for each an exact construction or the reason it declines.

### 2.8 `DECIMAL` up to precision 18 (347 to 351, 157)

Items 1 and 2 are the design input. What this plan adds is the owner's precision rule and the
load-path choice.

**Representation.** A decimal of precision at most 18 is an unscaled long - the long lane - and
its scale rides on the Spark type, as a year-month interval's unit does. Spark's own column vectors
store precision up to 9 as an int and up to 18 as a long; Arrow stores every decimal as 128 bits,
so the Arrow-backed column a kernel reads is pairs of longs.

**The load path is a measurement, built both ways (347).** Item 1's de-interleave - two loads and
an even-lane shuffle per output vector, or a compress where it intrinsifies - with the high word
checked against the sign of the low one, a mismatch declining the batch; against a cache layout in
which `ArrowCachedBatchSerializer` writes a decimal of precision up to 18 as its unscaled long (or
int up to 9), which the kernel loads like a `BIGINT` and the row path reads through a column vector
that rebuilds the `Decimal`. The cache arm changes a file format, so it carries a version tag the
serializer checks; the de-interleave serves every other Arrow source (Python, a columnar reader)
either way. Item 1 asked for exactly this measurement first, since the whole decimal case rests on
it. Row 157 is the output's mirror: the store that writes the value word and its sign extension
into each sixteen-byte slot. A precision-up-to-9 decimal on the int lane is a third arm 347
measures.

**The precision rule binds every node: owner's decision, 14 October 2026.** Precision 18 binds
the inputs, every intermediate and the result type, as Spark's type rules compute them, and no
per-batch value bound admits a wider declared type - so item 89 bullet 6's runtime bound (the long
product is exact when both unscaled operands fit 32 bits in the batch) is not taken. What that
declines, plainly:

* **Products.** `DECIMAL(p1, s1) * DECIMAL(p2, s2)` is `DECIMAL(p1 + p2 + 1, s1 + s2)`, so a product
  fuses only when `p1 + p2 <= 17`. `DECIMAL(10, 2) * DECIMAL(10, 2)` is `DECIMAL(21, 4)` and
  declines. **TPC-H q6 at its specification's types is not fused by milestone 8**: its
  `l_extendedprice * l_discount` over `DECIMAL(15, 2)` is `DECIMAL(31, 4)`. (This tree's own
  benchmark schema declares `DECIMAL(10, 0)`, which still exceeds the rule's product bound.)
* **Sums and differences.** The result is `max(p1 - s1, p2 - s2) + max(s1, s2) + 1` digits, so two
  operands at precision 18, or any pair whose integer and fraction digits together reach 18,
  decline; `DECIMAL(17, s) + DECIMAL(17, s)` is the widest that fuses at equal scales.
* **Quotients.** `s = max(6, s1 + p2 + 1)` and `p = p1 - s1 + s2 + s`, so only small operands stay
  under 18 (`DECIMAL(5, 2) / DECIMAL(5, 2)` is `DECIMAL(13, 8)`), and those are 346's hard entry.
* **Integral operands meet decimals at their decimal width.** Type coercion turns a `TINYINT`,
  `SMALLINT`, `INT` or `BIGINT` met by a decimal into `DECIMAL(3, 0)`, `(5, 0)`, `(10, 0)` or
  `(20, 0)`. So **every arithmetic or comparison between a `BIGINT` and a decimal declines**, its
  coerced side being `DECIMAL(20, 0)`; an `INT` times a `DECIMAL(p, s)` fuses only for `p <= 7`.
* **The census and the casts.** The census counts a type, not a precision, so no entry or pair
  leaves it: an entry's decimal pair counts fused when it fuses for every declared precision whose
  result the rule admits, and declines with this reason for the rest. Of the census's in-scope
  entries, the ones that meet the rule as a limit are the decimal arithmetic (`+`, `-`, `*`, `/`,
  `%`, `mod`, `pmod`, `div` and the `try_` forms), the comparisons and `IN` between a decimal and an
  integral or decimal of other width (through coercion), `CheckOverflow`, and the interval
  products and quotients by a decimal factor. In the cast matrix every pair's target is a type in
  the set, so no legal pair leaves; what declines is a cast to a decimal type above 18, already out
  of the set, and the implicit `BIGINT`-to-`DECIMAL(20, 0)` coercion above. The functions with
  decimal results all stay inside: `time_to_seconds` is `DECIMAL(14, 6)`, `extract(SECOND FROM dt)`
  `DECIMAL(8, 6)`, `make_time`'s seconds `DECIMAL(16, 6)`, `make_dt_interval`'s `DECIMAL(18, 6)`.
* **The done-when shares do not change**: 1.1 item 2 counts entries and pairs by type, and its
  targets stand; the declines above are reasons the census records.

**Semantics, ANSI and LEGACY.** Under the rule, decimal `+`, `-` and `*` cannot overflow: the
result precision Spark picks (`max(p1 - s1, p2 - s2) + max(s1, s2) + 1` for a sum, `p1 + p2 + 1`
for a product) holds every exact result of operands within their precisions, and
`spark.sql.decimalOperations.allowPrecisionLoss` changes a type only above precision 38. So the two
modes agree, provided the inputs respect their precisions - which the load checks, as one range
compare per lane group, a violation declining the batch. 349 proves the claim per operator over
every precision pair up to 18 by the SMT tooling, and the rescale to a common scale (a multiply by
`10^k`, constant per node) with it. What can overflow is a cast to a smaller precision (null under
LEGACY and TRY, `NUMERIC_VALUE_OUT_OF_RANGE` under ANSI), and what rounds is a cast to a smaller
scale: `ROUND_HALF_UP`, which on an unscaled long is an add of half the divisor away from zero and
a truncating division by `10^k`, a constant division the long lane's proven forms serve (350).
Decimal division needs a wider intermediate by item 2's reading; its result often stays under
precision 18 (`DECIMAL(5, 2) / DECIMAL(5, 2)` is `DECIMAL(13, 8)`), but `u1 * 10^k` does not fit
a long in general, so it is a hard entry until 346 finds an exact construction or the owner rules
it out.

### 2.9 The existing types

The rows that fill in the six types run beside the new lanes, not after them, and most are the
cheapest work in the milestone: 104 (`BIGINT` arithmetic, its plan written and its overflow test
already lane-generic), 30, 95 and item 33 (`div`, `%` and `pmod` over the integral lanes, and a
column multiplier), 103 (day-time interval arithmetic, extracts and `make_dt_interval`), 39 (`date
- date`), 89 (the year-month divisions), 335 (item 32's bitwise family, the emitter already
emitting `AND`, `XOR` and the shifts), 336 (item 34's null-safe equality, a design question about
the null model before it is code), 333 (item 55's int branches, item 94's CASE without ELSE, and
the rewritten conditionals `nullif`, `nvl2`, `zeroifnull`, `nullifzero`, `between`), and 334 (item
93's `IS NULL` over a predicate, whose local branch exists with seven failures to understand).

### 2.10 Tests and proofs as the lanes land

Every lane row extends the oracles in the same pull request: the coverage table (the row engine as
the oracle for every shape), the reference evaluator's semantics for the type, the IR fuzzer's
grammar and the Spark-level fuzzer's columns. 352 closes what the lane rows cannot do alone - the
compositions across types (a `SMALLINT` compared with a `DECIMAL`, a `DOUBLE` cast of a `BIGINT`
sum) - and runs them under the option matrix. A lane's first pull request also keeps
`emitted_bytes.json` unchanged for every existing shape: adding a lane must not move a byte of an
int or long kernel.

The exhaustive sweeps cost machine time, stated against the calibration table in
`skills/testing-and-debugging.md`: the int32 lowerings' exhaustion takes about thirteen and a half
minutes of each nightly (`m7/PLAN.md` 1), so each 2^32 sweep this plan adds - a `SMALLINT` operator
pair, a `FLOAT` source cast - is predicted at that order per sweep and, by the table's observed
ratios, up to twice that once the harness's reads are counted. They run in the nightly and in idle
windows; PR CI runs a fixed sampled slice of each.

### 2.11 The post

*One engine, every number* is for JVM and database engineers, as milestone 7's own post was, and it
is built from the census rather than from a benchmark: the share it states is 1.1 item 2's, per
type and per family, and its substance is how a kernel can be shown to agree with Spark across
thirteen types - exhaustion where the width allows, SMT for 64-bit integer rules, the fuzzers for
the rest, and the cases declined with their reasons. Whether a benchmark headline joins it is the
owner's (section 7).

## 3. Task breakdown

Task numbers continue the single sequence from 328, the next free number at commit `90b8871b705`
on 14 October 2026 (the highest in any plan, table or branch was 327); rows taken from the
catalogue keep theirs. Sizes are the house's small, medium and large.

| task | what it is | where it came from | size |
| ---: | :--- | :--- | :--- |
| 328 | The census and the cast matrix kept true: `dev/varka_census.py --check` in the linters' job; a suite holding `m8/casts.tsv`'s legality to `Cast.canCast`, `canAnsiCast` and `canTryCast` over every pair; the coverage rows that prove what the documentation already says fuses (year-month comparisons and `IN`, null tests on every existing type), and a single-expression coverage row behind every fused claim, so the census's check reads one class per row; `dev/varka_catalogue_move.sh --check 8 9` beside them. **Done when** the three checks run in CI and the census's fused count moves only with a coverage row | this plan, 2.1 and 2.2 | small |
| 28 | Lane-width conversion: one kernel over two widths, the int-to-long cast and the narrowing back. **Done when** `VARKA-28.md`'s own list holds, with the mixed-width trees of this milestone's casts admitted | catalogue item 39 table; `VARKA-28.md` | medium |
| 104 | `BIGINT` arithmetic in the three modes, closing VARKA-30's int64 half. **Done when** `VARKA-104.md`'s list holds | catalogue item 39 table; `VARKA-104.md` | medium |
| 30 | The integral division family: `div`, `%` and `pmod` over `INT` and `BIGINT` (item 33's sign rule, corrected by item 89), a zero divisor null or raised. **Done when** each fuses in the three modes with the row engine's error classes, proven by exhaustion at 32 bits and SMT at 64 | catalogue item 39 table; `m5/PLAN.md` 2.4; item 33 | medium |
| 95 | An int multiply by a column, and `i % k`. **Done when** both fuse with the checked-multiply rule, or the record says why not | catalogue item 39 table; `m5/PLAN.md` 2.30 | small |
| 103 | Day-time interval arithmetic, multiplications and divisions, the extracts and `make_dt_interval`. **Done when** `m5/PLAN.md` 2.38's list fuses, each division exact under its stated bound or guarded | catalogue item 39 table; `m5/PLAN.md` 2.38 | medium |
| 39 | `date - date`, the first mixed-width kernel. **Done when** `VARKA-39.md`'s list holds | catalogue item 39 table; `VARKA-39.md` | small |
| 89 | The year-month divisions beyond `extract(YEAR FROM ym)`. **Done when** `VARKA-89.md`'s list holds | catalogue item 39 table; `VARKA-89.md` | small |
| 333 | The choice and null shapes on every existing type: item 55's int branches, item 94's CASE without ELSE, `coalesce`, `greatest`, `least` and `IN` on each existing type, and the rewritten conditionals `nullif`, `nvl2`, `zeroifnull`, `nullifzero`, `between`. **Done when** each fuses on all six existing types with a coverage row, `date_add(d, i)`'s offset rule unchanged | items 55, 94 | small |
| 334 | `IS NULL` and `IS NOT NULL` over a predicate. **Done when** item 93's list holds, the seven failures of its branch understood | item 93 | small |
| 335 | The bitwise and shift operators, `bit_count`, `bit_get` (its `TINYINT` result admitted with 329) and the bitmap position functions. **Done when** each fuses on every integral type it takes, by exhaustion at 32 bits and below | item 32 | small |
| 336 | Null-safe equality, `<=>` and `equal_null`: the null-tolerant binary node decided and built. **Done when** item 34's question has a recorded answer and both fuse on every type | item 34 | medium |
| 329 | `TINYINT` and `SMALLINT` columns, built both ways: widened onto the int lane and native byte and short lanes, behind an emit option, measured on comparisons, a three-node chain and a mixed projection at both widths. **Done when** both arms load, compare, choose and store with coverage rows, the measurement is committed with its verdict per width, and the losing arm stays fuzzed in the option matrix | this plan, 2.4 | large |
| 330 | `TINYINT` and `SMALLINT` arithmetic in the three modes: wrap at the type's width, ANSI's range check, TRY's null. **Done when** `+`, `-`, `*`, negation, `abs` and the `try_` forms fuse on both arms, proven by exhaustion over every operand pair | this plan, 2.4 | medium |
| 331 | `BOOLEAN` columns as conditions: `BitVector` read as known-true and known-false masks, three-valued `AND`, `OR` and `NOT` over columns, comparisons and choice over booleans. **Done when** a boolean column filters, chooses and compares with coverage rows and the null-pattern tests of `VARKA-138.md` | this plan, 2.5 | medium |
| 27 | Boolean outputs: a predicate or a boolean column as a projection result, in both forms row 27's measurement keeps. **Done when** `m5/PLAN.md` 2.1's list holds over every predicate row of the coverage table | catalogue item 15 table; `m5/PLAN.md` 2.1 | medium |
| 332 | The integral and boolean casts in the three modes (wrap, raise, null), and the interval casts to and from integral types. **Done when** every such pair of `m8/casts.tsv` fuses, proven by exhaustion where the source has at most 32 bits and by SMT where it has 64 | this plan, 2.2 | medium |
| 337 | `hash` and `xxhash64` over the numeric and temporal columns. **Done when** both fuse over every type in the set with Spark's per-type byte layouts, proven against `Murmur3HashFunction` and `XxHash64Function` by exhaustion at 32 bits | this plan, 2.1 | medium |
| 338 | The `TIMESTAMP_NTZ` lane: load and store, comparisons, choice, `IN`, the null tests, the casts with `DATE`. **Done when** each fuses with coverage rows, `DATE` to `TIMESTAMP_NTZ` guarded against `multiplyExact`'s overflow | item 31 | small |
| 339 | `TIMESTAMP_NTZ` arithmetic: `ntz - ntz`, `ntz +/- dt`, `ntz +/- ym`, `timestampadd`, `time_bucket`, checked. **Done when** each fuses, the overflowing lane declining to the row engine's error, the calendar forms proven against `java.time` over the sweep the date family uses | item 31 | medium |
| 340 | `TIMESTAMP_NTZ` decomposition: the date fields through an exact full-range floor division into the int32 prefix, `hour`, `minute`, `second`, `extract` over it, the casts with `TIME`, `make_timestamp_ntz(date, time)`. **Done when** each fuses, the division proven by SMT over the whole long range and added to `ChronoDivide`'s table | item 31; `m5/PLAN.md` 2.6 for the long-lane division | medium |
| 341 | The floating lanes: `Lane` split into integral and floating halves, `DOUBLE` and `FLOAT` lane types, `Float8Vector` and `Float4Vector` read and written, comparisons, choice and `IN` by Spark's NaN and `-0.0` rules, `isnan`, `nanvl`. **Done when** item 89 bullet 1's table test passes for every operator and its constant-specialised forms, the bytes oracle unchanged for every integral shape | items 35, 89 | large |
| 342 | Floating arithmetic and the exact functions: `+`, `-`, `*`, `/` (a zero divisor null or raised; an int division through its casts), negation, `abs`, `signum`, `sqrt`, `degrees`, `radians`, `try_divide`, the interval products and quotients by a floating factor. **Done when** each fuses, bit-identical to the row engine over sampled regions and the fuzzer | item 35 | medium |
| 343 | The floating casts: integral to floating (rounding as the JVM does), `FLOAT` and `DOUBLE` to each other, floating to integral (LEGACY's saturation, ANSI's and TRY's range check and NaN), to and from `BOOLEAN`, `time_from_seconds` over a double. **Done when** every such pair of `m8/casts.tsv` fuses in the three modes, a `FLOAT` source proven over all 2^32 bit patterns | this plan, 2.2 | medium |
| 344 | The rounding functions: `floor`, `ceil`, `rint` over a double by the 2^52 construction, `floor` and `ceil` to `BIGINT`, `round`, `bround` and `truncate` over integral types, `factorial`, `width_bucket`. **Done when** each fuses, the double forms checked over every boundary region and the integral ones by exhaustion at 32 bits | items 35, 36 | medium |
| 345 | The math family's ULP contract and its configuration (2.7.1): `MathLaneProbe` extended to every function and both references, a bound per function and per host library, the coverage and support tables' bound column, the oracle and fuzzers comparing within the bound, `spark.sql.codegen.varka.math.ulpContract.enabled` with its emit option in the shape key, and `SCOPE_STANDARD_MODE.md` 3.4. **Done when** a probe run on each of the three host libraries is committed, the configuration declines the family when off, and a test shows a within-bound comparison catches a lane two units further out | item 36; owner's decision, 14 October 2026 | medium |
| 356 | The trigonometric functions under the contract: `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `sec`, `csc`, `cot`. **Done when** each fuses within its stated bound with a coverage row per host library | items 35, 36 | medium |
| 357 | The hyperbolic functions under the contract: `sinh`, `cosh`, `tanh`, and Spark's formulas for `asinh`, `acosh`, `atanh`. **Done when** each fuses within its stated bound, the formulas reproduced term by term | items 35, 36 | medium |
| 358 | The exponential, logarithmic, power and root functions under the contract: `exp`, `expm1`, `log` (one and two arguments), `ln`, `log10`, `log1p`, `log2`, `pow`, `power`, `cbrt`, `hypot`. **Done when** each fuses within its stated bound, `log10` at its two units | items 35, 36 | medium |
| 346 | The hard entries: floating `round`, `bround`, `truncate`, `%`, `pmod` and `try_mod`, floating to decimal, decimal division. **Done when** each has an exact construction fused with its proof or a recorded reason to decline, in the census | this plan, 2.7 and 2.8 | small |
| 347 | The decimal load path, built three ways (for declared precisions up to 18; no wider type admitted by a value bound): the de-interleave (shuffle, and compress where it intrinsifies) with the high word checked, an unscaled-long cache layout for precision up to 18, and the int lane for precision up to 9. **Done when** all three read a decimal column with coverage rows, the measurement is committed per width with its verdict, and the arms that lose stay behind the option | items 1, 89 bullet 6 without its runtime bound | large |
| 157 | The widening store: a decimal output as its value word and sign extension in each sixteen-byte slot. **Done when** `m5/PLAN.md` 2.93's list holds | catalogue item 39 table; `m5/PLAN.md` 2.93 | small |
| 348 | Decimal comparisons, choice, `IN`, null tests, negation and `abs`, at equal and at unequal scales (a constant rescale proven to fit the long), declining every pair whose common type exceeds precision 18, a `BIGINT` operand's `DECIMAL(20, 0)` included. **Done when** each fuses, the load's precision check declining an out-of-precision batch | items 1, 2 | medium |
| 349 | Decimal `+`, `-` and `*` under the precision rule, with `CheckOverflow`, `MakeDecimal` and `UnscaledValue`. **Done when** each fuses for every operand pair whose inputs, coerced operands and result stay within precision 18 and declines the rest with the rule as its reason (no value bound admits a wider type), the no-overflow claim and the rescales proven by SMT over every precision pair, LEGACY and ANSI giving the same bytes | item 2 | medium |
| 350 | The decimal casts, to and from decimal types up to precision 18: to and from integral types, decimal to decimal (`ROUND_HALF_UP`, overflow null or raised), to and from `BOOLEAN`, to floating under the 2^53 guard. **Done when** every decimal pair of `m8/casts.tsv` fuses or is a recorded hard entry, the integer rules proven by SMT | this plan, 2.2 and 2.8 | medium |
| 351 | Decimals in the existing types' functions, all of whose decimal types are within precision 18: `time_to_seconds`, `second` with its fraction, `extract(SECOND FROM dt)`, `make_time`, `make_dt_interval` and `make_timestamp_ntz` over decimal seconds, `time_from_seconds` over a decimal, `floor`, `ceil`, `round`, `bround` and `truncate` over a decimal. **Done when** each fuses with a coverage row | `m5/PLAN.md` 2.93; items 1, 2 | medium |
| 352 | The fuzzers and the reference evaluator across types: the IR fuzzer's grammar and the Spark-level fuzzer's columns over all thirteen types, with the cross-type compositions and the option matrix's configurations. **Done when** both fuzzers draw every type at every nightly and a cross-type disagreement found is fixed or a named row | this plan, 2.10 | medium |
| 353 | The support table for users: `docs/sql-varka.md` states, per type and per family, what fuses, from the census and `coverage.json`, and its "three types" paragraph says what the code admits, with each math function's ULP bound per host library (2.7.1). **Done when** the table is generated and `VarkaCoverageSuite` fails when it drifts | item 54 | small |
| 354 | The closing task: 1.1 read row by row, the census and the cast matrix regenerated on the milestone's build, the README's account updated. **Done when** each item of 1.1 is met or its gap is named | this plan | small |
| 355 | The milestone's post, *One engine, every number*, for JVM and database engineers. **Done when** published with the census share per type and the proofs behind it, as 2.11 says | this plan, 2.11 | medium |

## 4. Ordering

| wave | tasks | why they wait |
| ---: | :--- | :--- |
| 0 | 328, 28, 104, 333, 334, 335 | 328 makes every later count honest; 28 gates most casts and the timestamp fields; 104, 333, 334 and 335 need nothing new |
| 1 | 329, 330, 331, 27, 332, 30, 95, 103, 39, 89, 336 | the first new types prove the plumbing on integral descriptors; 330 after 329's arms; 27 beside 331; 332 after 28 and 329; 30 and 95 after 104's checked forms; 39 after 28 |
| 2 | 338, 339, 340, 337 | the long lane's new type, after 28 (340's narrowing) and 332 (its casts' harness) |
| 3 | 341, 342, 343, 344, 345, 356, 357, 358, 346 | the floating split, comparisons first; arithmetic before the math functions; 345's contract and configuration before 356 to 358, which fuse the family under it; 344 after 341's lane; 346 once 344 and 350 have met their hard cases |
| 4 | 347, 157, 348, 349, 350, 351 | the decimal path last, its load measured first, as item 1 asks |
| 5 | 352, 353, 354, 355 | 352 runs across waves 1 to 4 and closes here; 354 last by definition; 355 written from what 354 records |

At the halfway point the milestone is reviewed against 1.1, not against this table, as milestone
7's was (`m7/PLAN.md` 9).

## 5. Verification

Every structural row cites the proof its row names:

* **Exhaustion** for every operation whose operands total at most 32 bits: the `TINYINT` and
  `SMALLINT` arithmetic (330, every operand pair), the casts from a source of at most 32 bits,
  every `FLOAT` bit pattern included (332, 343, 338), the integral division family at 32 bits (30),
  the bitwise family (335), the hash functions' 32-bit inputs (337).
* **SMT** (`dev/varka_prove.sh`, `VARKA-240.md`) for every 64-bit integer rule: the casts from
  `BIGINT`, `TIME`, `TIMESTAMP_NTZ` and the intervals (332, 340), the timestamp floor division
  (340),
  the decimal rescales, the no-overflow claim and the `ROUND_HALF_UP` division (348, 349, 350), the
  64-bit division family (30, 104).
* **Within the stated bound** for the 27 math functions under the ULP contract (2.7.1), on each
  host library, and nowhere else.
* **The coverage oracle** (the row engine) for every shape a row admits, with the poisoned null
  lanes and both validity fills of `VARKA-138.md`, and its ANSI form comparing error classes.
* **The fuzzers**, IR and Spark-level, drawing each type from the pull request that adds it (352).
* **The bytes oracle**: `emitted_bytes.json` unchanged for every existing shape when a lane lands
  (329, 331, 338, 341, 347), and moved only by the shapes the row adds.
* **The measurements** of 329 and 347 follow the house methodology (`VARKA-14.md` 2.1) on an idle
  machine, at both vector widths, with the CI runners' readings beside the laptop's.
* `dev/varka_census.py --check`, `dev/varka_quote_check.py` at zero orphans and
  `dev/varka_toc.py --check` clean; the linters run locally before every push.

## 6. Risks

1. **The census is judgement as well as mechanism.** The registry half cannot be wrong without
   the script failing; the types and needs are read by hand. Mitigation: each type claimed fused is
   checked against a coverage row, and each lane row re-reads the census lines it touches and
   corrects them in its own pull request.
2. **The floating split touches every emitter path.** `Lane` derives its descriptors from an
   integral scalar, and 81 references in 16 files name the two lanes (2.3). Mitigation: the bytes
   oracle and the integral exhaustion unchanged as 341's gate, and the split landed before any
   floating arm.
3. **The decimal cache arm changes a file format.** Mitigation: a version tag the serializer checks,
   the row path's reader proven by the coverage oracle, and the de-interleave kept for every other
   source whatever the measurement says.
4. **The precision rule declines the headline decimal query.** TPC-H q6 at its specification's
   types is not fused by this milestone (2.8, owner's decision). Accepted: the post's claim is the
   census, not that query, and the post says which decimal shapes decline and why.
5. **The ULP contract is a departure users may not expect.** Mitigation: the configuration, the
   bound stated per function and host in the coverage and support tables, the register entry, and
   a default the owner sets knowingly (section 7).
6. **The exhaustive sweeps cost the nightly time.** Each 2^32 sweep is predicted at the int32
   sweeps' order and up to twice it (2.10). Mitigation: PR CI runs slices, the nightly the rest,
   and an idle window the first full pass, its times recorded against the prediction.
7. **The new types will find disagreements.** NaN, `-0.0`, wraps at narrow widths and half-up
   rounding are where engines disagree. Each finding is fixed, or a named delta in the standard-mode
   register, or a row; the halfway review weighs the growth against 1.1.

## 7. Open questions

1. **The default of `spark.sql.codegen.varka.math.ulpContract.enabled`** (2.7.1). On buys item 36's
   speed for every user, at answers a unit in the last place from vanilla Spark's; off keeps
   enabling Varka answer-neutral and makes the speed opt-in. The plan recommends off.
2. **`TIME` to `TIMESTAMP_NTZ` takes the session's current date.** After `ComputeCurrentTime` it
   is a literal per query, so 340 can fuse it as the time plus a constant; confirm that a cast whose
   answer depends on when the query runs is wanted in a kernel.
3. **Decimal division**: an exact construction found by 346, or out of this milestone by decision?
4. **The post's headline**: the census share alone (2.11), or a benchmark number beside it?

## 8. Explicitly out of milestone 8

* `STRING` and every cast to or from it, `TIMESTAMP` with the local time zone, and `DECIMAL` above
  precision 18: owner's decision, 14 October 2026. So are the functions that take or return them -
  151 registered names for strings, 20 for the zoned timestamp (2.1).
* Aggregate, window and generator functions: Varka fuses scalar projections and filters; the
  aggregation items of `m9/SCOPE.md` (4, 5, 6) are a later milestone's.
* Nested types, `BINARY`, `VARIANT`, sketches and geospatial types, which have no representation.
* The nanosecond timestamp types, sixteen bytes a value (item 31's own exclusion).
* Everything else in `m9/SCOPE.md` not named above, among them the plugin (item 53), the
  configuration surface (item 64, and `VARKA-64.md` with it) and the guard walk (`VARKA-73.md`).
* Every milestone 7 row, open or closed: owner's decision, 14 October 2026.
