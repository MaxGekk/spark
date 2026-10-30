# VARKA-103: Day-time interval arithmetic, products, quotients, extracts and `make_dt_interval`

## 0. What exists now (admission check)

From master at `5cdb032b9ef` (30 October 2026). Varka paths are under
`sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen/` unless named;
Spark's under `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/` unless named; Guava
33.6.0 (the version `pom.xml:207` pins) was read in its sources jar, the JDK in
`/home/max/proj/openjdk-build/jdk25`. The row's text is the milestone plan's row 103 and its 2.6
(last paragraph), and `m5/PLAN.md` 2.38. Numbers marked **[A]** are from
`results/VARKA-103-admission.txt`, a Java program and its output: the planned constructions in
scalar Java against the calls Spark's code makes, sampled, not proved.

**Spark's answers.** Every day-time interval form below raises in every evaluation mode: none has
a LEGACY wrap or an ANSI switch, and the `try_` spellings over an interval are `TryEval`. So
LEGACY, ANSI and TRY collapse to one kernel per form, and TRY is item 12.

1. **`dt + dt`, `dt - dt`, `-dt`** are `IntervalMathUtils.addExact`, `subtractExact` and
   `negateExact` on the long (`expressions/arithmetic.scala:466-467`, `:564-565`, `:99`, the
   codegen `:88-92`, `:314-319`). They raise `INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION`
   (`try_add`, `try_subtract`) and `.WITHOUT_SUGGESTION` for the negation, built with
   `context = null` (`util/IntervalMathUtils.scala:26-46`), so neither engine prints a SQL
   context for them. The class reads "Integer overflow while operating with intervals.", with
   "Use <functionName> to tolerate overflow and return NULL instead." or "Try devising
   appropriate values for the interval parameters." (`error-conditions.json:3699`).
2. **`abs(dt)`** is `LongExactNumeric.abs` interpreted and `c < 0 ? MathUtils.negateExact(c) : c`
   generated (`arithmetic.scala:180`, `:193-195`; `types/numerics.scala:110`), whatever
   `failOnError` says: at `Long.MIN_VALUE` it raises `ARITHMETIC_OVERFLOW` ("long overflow",
   `error-conditions.json:129`), not the interval class of item 1.
3. **`dt * k`, `k * dt`** arrive as `MultiplyDTInterval(dt, k)` from either side
   (`analysis/BinaryArithmeticWithDatetimeResolver.scala:163-164`). By the factor's type
   (`expressions/intervalExpressions.scala:697-705`, codegen `:709-722`):
   - integral: `Math.multiplyExact(micros, k.longValue())`, a bare `java.lang.ArithmeticException`
     with no error class;
   - `FLOAT`, `DOUBLE`: `DoubleMath.roundToLong((double) micros * k, HALF_UP)` - the long
     converted to a double first (rounding to nearest above `2^53`), one IEEE multiply, then
     Guava's `roundIntermediate`: `z = rint(x)`, and if `abs(x - z) == 0.5` then
     `x + copySign(0.5, x)`, a NaN or an infinity raising, then the range test
     `MIN_LONG_AS_DOUBLE - z < 1.0 & z < 2^63` and `(long) z` (`DoubleMath.java:54-98`,
     `:157-169`), its `ArithmeticException` carrying no error class;
   - decimal: `Decimal.set(micros) * k` under `MathContext(39, DOWN)`, then `setScale(0,
     HALF_UP).longValueExact()` (`sql/api/.../types/Decimal.scala:523-524`, `:597`).
4. **`dt / k`** is `DivideDTInterval` (`BinaryArithmeticWithDatetimeResolver.scala:171`,
   `intervalExpressions.scala:854-924`). Before dividing, in every mode: a zero `k` raises
   `INTERVAL_DIVIDED_BY_ZERO`, "Division by zero. Use `try_divide` to tolerate divisor being 0
   and return NULL instead." (`:747-756`, `-0.0` included, since `num == 0`;
   `error-conditions.json:3717`), and for an integral `k` the pair `micros == Long.MIN_VALUE`,
   `k == -1` raises - with **different classes in the two engines**: the interpreter's
   `checkDivideOverflow` raises `INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION` (`try_divide`,
   `:733-745`), the generated code `overflowInIntegralDivideError`, `ARITHMETIC_OVERFLOW` with the
   query context (`:889-893`; `errors/QueryExecutionErrors.scala:344-346`). The quotient itself:
   integral `LongMath.divide(micros, k, HALF_UP)` (`:869`; `LongMath.java:382-433`, the
   truncated quotient moved one step away from zero when `2|r| >= |k|`); floating
   `roundToLong(micros / (double) k, HALF_UP)` (`:874`); decimal `Decimal./`, to 39 fractional
   digits `DOWN`, then `HALF_UP` to scale 0 (`:870-872`; `Decimal.scala:526-533`).
5. **The extracts** (`intervalExpressions.scala:100-123`, parsed at `:139-158`, the unit admitted
   only inside the type's start and end fields) call `IntervalUtils` (`util/IntervalUtils.scala:
   47-68`): `getDays = (int) (x / D)`, `getHours = (byte) ((x % D) / H)`, `getMinutes = (byte)
   ((x % H) / M)`, `getSeconds = Decimal(x % M, 8, 6)`, with `D = 86,400,000,000`,
   `H = 3,600,000,000`, `M = 60,000,000`, Java's truncating `/` and `%`. None can fail.
6. **`make_dt_interval(days, hours, mins, secs)`** takes three ints and a `DECIMAL(18, 6)`
   (`intervalExpressions.scala:536-537`); the short forms fill `Literal(0)` and
   `Literal(Decimal(0, 18, 6))` (`:519-527`). `makeDayTimeInterval` (`IntervalUtils.scala:786-801`)
   starts from `secs.toUnscaledLong` and does `addExact(micros, multiplyExact(days, D))`, then the
   same with `hours * H`, then `mins * M`, each partial sum checked, any failure raising
   `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION` with the query context
   (`QueryExecutionErrors.scala:737-744`). Only the days product can overflow:
   `multiplyExact(days, D)` raises at `+-106,751,992` and not at `+-106,751,991` **[A, 6]**, while
   `Int.MIN_VALUE * H` is -7730941132800000000 and `Int.MIN_VALUE * M` far less **[A, 6]**. A
   partial sum can overflow where the final one fits: days 106,751,991, hours -1, secs
   14454775808 micros has a true sum that fits a long, and Spark raises **[A, 6]**.
7. **`dt div dt2`** is `IntegralDivide`, whose `inputType` takes the day-time interval
   (`arithmetic.scala:970-971`): Java's truncating long `/`, a `BIGINT`, a zero divisor null
   under LEGACY and `DIVIDE_BY_ZERO` under ANSI, and no `MIN / -1` check for an interval left
   operand (`:965-968`), so `Long.MIN_VALUE div -1` is `Long.MIN_VALUE` **[A, 5]**. Row 30 sends
   the interval `div` here (`VARKA-30.md` 1, 7).
8. **What Varka has.** A day-time interval column and literal on the long lane
   (`VarkaTimeCompiler.java:159-165`), its comparisons, null tests and choices (coverage family
   "Long-lane predicates" and "Long-lane choice"), and its SECOND-ended relabel cast, every
   coarser end declining "which truncates" (`:185-195`). No arm claims `Add`, `Subtract`,
   `UnaryMinus` or `Abs` over it, nor `MultiplyDTInterval`, `DivideDTInterval`, `MakeDTInterval` or
   the extracts, except `ExtractANSIIntervalDays` inside `d + CAST(i AS INTERVAL DAY)`
   (`VarkaChronoCompiler.java:521-532`). The nodes it needs exist: `IntArith` `ADD`/`SUB` in
   `FAIL` at the long lane (row 339's plan uses them), `IntNeg` `FAIL` at the long lane
   (`varka/VarkaVectorIR.java:701`), `GuardedRange` (`:536`), `WidenLane` and `NarrowLane`,
   `BitOp`, `IfElse`, `Compare`, and `ConstDivide` at the long lane under a stated bound of at
   most `2^52` (`:764-849`).
9. **The year-month forms this row mirrors** (`VarkaIntervalCompiler.java:183-209`): `+`, `-`,
   negation, `abs` (a compare, `IntNeg` `FAIL`, a blend, `:282-298`), `make_ym_interval`, the
   multiply by an int (`:345-366`, which declines over an unbounded column because `arithOver`
   refuses every checked `MUL` off the narrow lanes, `VarkaNodeCompiler.java:1119-1122`), and
   row 89's `ym / k` for an integral literal (`:385-430`: `k = 0` and `k = -1` decline, `k = 1` is
   the operand, the rest `ConstDivide`, the wrapping remainder and a step away from zero when
   `|r| >= ceil(|k| / 2)`). Row 414 holds `ym / -1`.
10. **The shift split is row 338's, built.** `floorDivLong` (`VarkaNodeCompiler.java:787-810`):
    `D = 2^13 * 10,546,875` **[A, 1]**, an arithmetic shift by 13, a down step of `1 - odd` for a
    negative value, and a `ConstDivide` by 10,546,875, proven over every long in
    `sql/varka/proofs/long_divide.smt2`. It floors; Spark's `getDays` truncates. Row 332's plan
    (2.4) designs the truncation over it: `trunc(x / D) = floorDivLong(x + ((x >> 63) & (D - 1)))`,
    three lane operations, the bias unable to overflow. The check: 50000022 longs, the edges among
    them, 0 differ from Java's `x / D`, the largest shifted dividend 2^50 in magnitude, a quarter
    of `ConstDivide`'s bound, and the quotient always an int (at most 106,751,991) **[A, 1]**. No
    Granlund-Montgomery multiply is involved: 340 found it needs a 64-bit high multiply the Vector
    API lacks (`VARKA-340.md` 0).
11. **One full-range division serves every extract.** With `q = trunc(x / D)` and the wrapping
    `r = x - q * D` (`|r| < D`, so under `2^37`), `x % H == r % H` and `x % M == r % M` because
    `H` and `M` divide `D` and `r` keeps `x`'s sign; so `getHours` is `r / H`, `getMinutes`
    `(r % H) / M`, `getSeconds` `r % M`, each a `ConstDivide` bounded by `D` with no guard
    **[A, 2]**: 0 of 50000022 differ on each. More: `trunc(x / u) = q * (D / u) + r / u` for every
    divisor `u` of `D`, over every long - checked for `H`, `M`, `10^6`, 1000, 60 and 24, 0
    differ **[A, 2]** - and `D` has 504 divisors, 75 of them from 2 to 1000 **[A, 2]**. This
    answers, for the units of a day, what 332's section 7 leaves to row 30 ("a full-range
    day-time division below a day").
12. **TRY is `TryEval`, which no arm claims.** `try_add`, `try_subtract`, `try_multiply` and
    `try_divide` over an interval become `TryEval(Add(...))` and kin (`expressions/TryEval.scala:
    90-97`, `:135-141`, `:229-235`, `:268-274`); 342 and 340 decline it by name, and row 481's
    `NULL` guard (`VARKA-481.md` 3.1) serves `GuardedRange`, not a caught exception. So this row
    needs nothing of 481 and builds no TRY form.
13. **The other factors and divisors are other rows'.** A floating factor is row 342's 2.4, both
    product and quotient, with `fp_half_up.smt2` and cells (`VARKA-342.md` 2.4, 3.7), after 343's
    conversions; its form agreed with Guava on every cell of **[A, 7]** and on 50000000 drawn
    pairs. A decimal factor: row 349's plan (section 7) marks the products unclaimed with 361 and
    363 the suggested home, and the quotients go to 363, which claims them and declines them until
    a variable 128-bit division is planned (`VARKA-363.md` 2.5, 8 question 1). A column divisor or
    factor needs row 30's `IntDivide` or 104's checked long multiply. A decimal literal in SQL is
    the common spelling: `dt * 1.5` is a decimal product, not a double one (section 8, point 7).
14. **`java.time` is not an oracle for negative values.** `Duration` keeps floor seconds and a
    non-negative nano part, so at `x = -D + 1` `toDays()` is -1 where `x / D` is 0; over 2000000
    drawn longs `toDays` differs 66144 times and the hour, minute and second parts as often
    **[A, 3]**. The done-when's "`java.time` the oracle at the edges" holds for the products'
    and the sums' edges only through `Duration.plus` and `multipliedBy`, which raise at the long's
    edges; the extracts' oracle is Spark's own row engine and `BigInteger`.
15. **The outputs are other rows'.** `extract(DAY)` is an int from a long-driven kernel,
    `NarrowLane` as 338's `CAST(ntz AS DATE)`; hours and minutes are `ByteType`, and a narrow
    value under a long kernel is refused (`varka/VarkaKernelPlan.java:297-300`) until the
    narrowing store of a long value, `L2I` then the narrow store, which 332's 2.5 writes once for
    itself and 343; seconds is a `DECIMAL(8, 6)`, row 157's 64-bit store.
16. **The baseline exists for none of it.** `VarkaLongLaneThroughputBenchmark` times a day-time
    comparison filter and `least` only; no class times the arithmetic, the extracts or
    `make_dt_interval`.

### What differs from the row text and the milestone plan

- **The truncation is the biased floor, not the magnitude.** The row's "over the magnitude with
  the sign restored" needs a long `abs` (the Java fallback below AVX-512, `VARKA-348.md` 0) and a
  logical shift: with the arithmetic shift the row implies, `Long.MIN_VALUE`'s magnitude wraps and
  the quotient comes out 106751991 against -106751991, 4 of the drawn longs differing **[A, 1]**.
  The bias form of 332's 2.4 is exact and uses 338's `floorDivLong` unchanged.
- **Hours and minutes need 332's long-to-byte store, not 329's.** 329's byte store takes an
  int-lane value; this kernel is driven at the long lane (item 15).
- **"Each division exact under its stated bound or guarded"** holds with no guard for the extracts
  and for `dt / k` with `k` dividing `D` (item 11); other integral literals take 30's constant
  forms, `ConstDivide` under a `2^52` guard first.
- **The `HALF_UP` double-factor tests are 342's lowering** (item 13); this row's cells go to 342's
  3.7 (point 2 of section 8).
- **`m5/PLAN.md` 2.38's correction is already there.** The dated note of 14 October 2026
  (`m5/PLAN.md:2379-2386`) is the done-when's "corrected by a dated note"; nothing is added.
- **`java.time` is the oracle at the arithmetic edges only** (item 14).
- **The wave edges are wider than G1's "after 28 and 329":** 332b (the byte store, the bias
  helper if it lands first), 347 and 157 (the seconds), 30 (column divisors, `div`) and 104 (a
  column factor). G1 runs after F3 already, so only 332, 30 and 104 add an edge (section 8).

## 1. Goal and non-goal

Every day-time interval form of `m5/PLAN.md` 2.38 fuses with Spark's answers and declines a batch
exactly where Spark raises: `+`, `-`, negation and `abs`; `dt * k` and `dt / k` for an integral
literal `k`, and for an integral column once 104 and 30 give the checked multiply and the
division; `extract` of `DAY`, `HOUR`, `MINUTE` and `SECOND`; `make_dt_interval` with its partial
sums in Spark's order; and `dt div` a literal or a column. Not this row: a floating factor (342), a
decimal column factor (361, 363), `TryEval` (no row), the interval casts beyond the identity
relabels (section 8), the year-month forms beyond section 8, point 6.

## 2. Design

All arms go into a new day-time half of `VarkaIntervalCompiler`, gated on `DayTimeIntervalType`
as the year-month arms are on theirs, under the same family switch
(`spark.sql.codegen.varka.family.interval.enabled`). No new IR node.

### 2.1 The checked arithmetic

| form | lowering | declines a batch where |
|---|---|---|
| `dt + dt2`, `dt - dt2` | `IntArith(ADD or SUB, FAIL)` | a sum or difference overflows |
| `-dt` | `IntNeg(FAIL)` | `Long.MIN_VALUE` |
| `abs(dt)` | `IfElse(dt < 0, IntNeg(FAIL, dt), dt)`, the year-month `absolute` | `Long.MIN_VALUE` |
| `+dt` | the child | never |

The mode is `FAIL` unconditionally, as for the year-month arms (`VarkaIntervalCompiler.java:
448-470`). Two units meet through a cast to the wider type; a cast to an end field at least as fine
as the source's is the identity on the value (a whole number of the source unit is a whole number
of the finer one), so it compiles to the child beside the SECOND relabel, and the truncating casts
keep declining (section 8, point 5). The error each engine raises is the row engine's own: the
declined batch re-evaluates the expression, so `abs` raises `ARITHMETIC_OVERFLOW` and `-dt` the
interval class (items 1, 2). Under `checkIntOverflow = false` these `FAIL` nodes lose their check
as the year-month ones do (`varka/Analysis.java:676-686`); whatever row 481 settles for that option
applies to both.

### 2.2 The products by an integral literal

`multiplyExact(x, k)` raises exactly outside `[lo, hi]`, with `lo = ceil(MIN / k)` and
`hi = floor(MAX / k)` for `k > 0` and the two swapped for `k < 0`, computed in the compiler with
`BigInteger` and clamped to the long: a `GuardedRange(x, lo, hi)` then `IntArith(MUL, WRAP)`, 39's
composition (`VarkaChronoCompiler.java:340-358`). Over 3200160 cells at sixteen `k`, both signs,
`Long.MIN_VALUE`, `Long.MAX_VALUE` and the int edges among them, the guard and `multiplyExact`
never disagree; the symmetric bound `+-(MAX / |k|)` would wrongly decline 4 admitted cells, those
with `x * k = MIN` **[A, 4]**. `k = 0` is the literal 0 (Spark multiplies and never raises), `k = 1`
the operand, `k = -1` the checked negation of 2.1. A `TINYINT`, `SMALLINT` or `INT` literal is its
long value. `k * dt` is the same node.

### 2.3 The extracts

    b = BitOp(AND, BitOp(SHR, x, 63), D - 1)             the bias, 0 or D - 1
    q = floorDivLong(IntArith(ADD, WRAP, x, b), D)       trunc(x / D), every long
    r = IntArith(SUB, WRAP, x, IntArith(MUL, WRAP, q, D))
    DAY    = NarrowLane(q, INT)
    HOUR   = narrow-to-byte(ConstDivide(r, H, bound D))
    MINUTE = narrow-to-byte(ConstDivide(IntArith(SUB, WRAP, r, MUL(ConstDivide(r, H), H)), M, D))
    SECOND = r - ConstDivide(r, M, D) * M                as DECIMAL(8, 6) through 157's store

`q`, `r` and the hour quotient are shared nodes, so a query extracting several fields computes
the chain once. The narrowing to an int is exact since `|q| <= 106,751,991`; to a byte, since
`|hours| <= 23` and `|minutes| <= 59`. The bias helper is `truncDivLong(x, d)` beside
`floorDivLong`, for any positive even `d` that `floorDivLong` admits, with its claim in
`long_divide.smt2`: built by whichever of 332b and this row lands first, reused by the other
(section 8, point 1). The new arm claims `ExtractANSIIntervalDays` where it is a node of the walk;
inside `DateAdd` the extract is the offset, which the date arm reads itself without recursing
(`VarkaChronoCompiler.java:477-485`), so no node has two claimants (`VarkaFamilyChainSuite`
holds that). That arm still declines a day offset other than `CAST(i AS INTERVAL DAY)` ("day
interval is not an int column cast to days"), so `d + dt` over a stored `INTERVAL DAY` column
keeps declining; admitting this row's quotient there under the date arm's day guard is a
follow-up for the calendar family, not built here.

### 2.4 The quotients by an integral literal

`LongMath.divide(x, k, HALF_UP)`: `k = 0` and `k = -1` decline at compile time, the first because
every valid row raises, the second as row 89 declines `ym / -1`, naming row 414 (item 4 is new
evidence for it, section 8); `k = 1` is the operand. Otherwise, with `q` the truncated quotient and
the wrapping `r = x - q * k`:

    bump = IfElse(|r| >= ceil(|k| / 2) and r != 0, x and k of one sign ? 1 : -1, 0)
    result = q + bump

where `|r|` is the compare-negate-blend of 2.1 (`|r| < |k|`, so it cannot overflow) and
`ceil(|k| / 2)` a compile-time constant, `(|k| >> 1) + (|k| & 1)`, and `2^62` for
`k = Long.MIN_VALUE`. The constant must not be `(|k| + 1) / 2`: at `k = Long.MAX_VALUE` it
overflows, and the admission program's first run failed 462239 of 7600418 cells on it; with the
overflow-free form 0 of 7600418 differ from `LongMath.divide` over nineteen divisors **[A, 5]**.
`q` and `r` come from:

- **`k` dividing `D`** (`|k|` among its 504 divisors): `q = q_D * (D / k) + r_D / k`, `r = r_D % k`
  over 2.3's `q_D`, `r_D`, exact over every long with no guard; 4800264 cells, 0 differ **[A, 5]**.
- **any other `k`:** row 30's constant forms for a `BIGINT` dividend (`VARKA-30.md` 2.4): its
  `GUARDED` default, `ConstDivide` under a `GuardedRange` of `|x| < 2^52`, about 142 years, and
  its `MULHI` arm when chosen. Before 30 lands the `GUARDED` form is buildable from existing nodes,
  and this row builds it there rather than wait (section 8, point 4).
- **`k = Long.MIN_VALUE`:** 30's `IfElse(x = MIN, 1, 0)` for `q`, and `r = x` elsewhere.

### 2.5 Column factors and divisors, and `div`

- **`dt * i` over an integral column** is a checked long multiply of a value nothing bounds: row
  104's option C (or B with AVX512DQ, `VARKA-104.md` 3.3); until it lands the arm declines naming
  104. A `TINYINT` or `SMALLINT` column needs 332's narrow load at the long width.
- **`dt / i` over an integral column** is row 30's `IntDivide(QUOT)` and `REM` at the long lane
  (`VARKA-30.md` 2.1, 2.2), `ZeroDivisor.FAIL` and `checkMinOverMinusOne` set (Spark raises on both
  in every mode), then the `HALF_UP` step as Guava writes it, wrapping arithmetic included:
  `cmp = |r| - (|i| - |r|)`, a step of `1 | ((x ^ i) >> 63)` where `r != 0` and `cmp >= 0`. Copying
  Guava's operations keeps `i = Long.MIN_VALUE`, where `|i|` wraps, right by the same wrap.
- **`dt div dt2` and `dt div` a day-time literal**: `IntDivide(QUOT)` with the zero disposal by
  mode (`NULL` under LEGACY, `FAIL` under ANSI) and `checkMinOverMinusOne` unset, since Spark wraps
  there (item 7); a literal divisor dividing `D` takes 2.4's day split, any other 30's constant
  forms, `INTERVAL '0' SECOND` declining at compile time. The result is a `BIGINT`, the same lane.

### 2.6 `make_dt_interval`

    s0 = the seconds as an unscaled long at scale 6
    s1 = IntArith(ADD, FAIL, s0, IntArith(MUL, WRAP,
             GuardedRange(WidenLane(days), -106751991, 106751991), D))
    s2 = IntArith(ADD, FAIL, s1, IntArith(MUL, WRAP, WidenLane(hours), H))
    s3 = IntArith(ADD, FAIL, s2, IntArith(MUL, WRAP, WidenLane(mins), M))

The order is Spark's and is not reassociated: each checked add declines exactly the lanes whose
partial sum Spark finds overflowing, the cell of item 6 among them; 5000000 drawn argument sets,
2708702 of them raising in Spark, give 0 differences **[A, 6]**. The hour and minute products need
no guard by the int's range (item 6); the range analysis cannot see that at the long lane, so the
`WRAP` mode is the compiler's by construction, as the `TIME` conversions' is. `s0` is the default
`Literal(Decimal(0, 18, 6))`, a constant-folded decimal literal (`1.5` is cast to `(18, 6)` and
folded), or a `DECIMAL(18, 6)` column through 347's leaf; another seconds operand is a cast to
decimal and 350's (section 8, point 3, for which row reads the literal). Overflowing lanes decline
and the row engine raises the error with its context.

### 2.7 Reference, cost, warm-up, census

No new node, so the reference evaluator, the cost model's features and the budget's weights are
unchanged; the warm-up reach suite is rerun, since the arms emit long-lane `MUL`, `SHR` and
`ConstDivide` combinations the blocks may not hold (`VarkaWarmupReachSuite`). The census's
`abs`, `negative`, `+`, `-`, `div`, `dt_times_num`, `dt_div_num`, `make_dt_interval`, `extract`,
`date_part`, `datepart` and the four `extract_dt_*` entries are updated as each lands (section 6).

## 3. Tests and proofs

1. **SMT over integers**, linear by constants, in `sql/varka/proofs/long_divide.smt2` beside 338's
   claim: the bias form equals `trunc(x / D)` over every long and its biased dividend does not
   overflow; `x - trunc(x / D) * D` equals `x rem D` and lies in `(-D, D)`; `r rem H = x rem H`
   and `r rem M = x rem M`; and `trunc(x / u) = q * (D / u) + trunc(r / u)` for `u` in `H`, `M`,
   `10^6` and every divisor a coverage row uses. In a new `sql/varka/proofs/dt_arith.smt2`: the
   multiply guard's bounds against `multiplyExact`'s range, per literal used by a test; the days
   guard and the hour and minute products' ranges of 2.6; and the `HALF_UP` rule of 2.4 per
   divisor, as `ym_divide.smt2` does at the int lane (a symbolic divisor is nonlinear and was not
   solved there either). Priced at VARKA-245's observed ratio: under 8 s per solver.
2. **Sampled, where the claim is not linear**: the column divisor's Guava sequence (2.5) against
   `LongMath.divide` over drawn pairs with `i` at 0, `+-1`, `+-2`, `Long.MIN_VALUE`,
   `Long.MAX_VALUE`; the `make_dt_interval` draw of **[A, 6]**.
3. **Named cells** through the coverage differential and an emitter suite, both widths: `dt + dt2`
   at `MAX` and `MAX - 1`; `-dt` and `abs(dt)` at `MIN` and `MIN + 1`, with the raised class of
   each (items 1, 2); `dt * k` at `lo - 1`, `lo`, `hi`, `hi + 1` for `k = 2`, `-2`, `MIN`; the
   extracts at `MIN`, `MAX`, `+-D`, `+-(D - 1)`, `+-1` and 0; `dt / k` at the ties `+-5 / 2` and
   `5 / -2` and at `2^62 / MIN` **[A, 5]**, at `+-2^52` for a guarded divisor; `make_dt_interval`
   at days `+-106,751,991` and `+-106,751,992` and the partial-sum cell of item 6; `dt div -1` at
   `MIN`, which wraps.
4. **The sweep, run by hand in the gate**: a `VarkaDayTimeSweepSuite`, tagged so the PR build skips
   it as 481's and 330's sweeps are, drawing 2^28 longs per form against Spark's own expressions on
   the row engine. No CI workflow runs it; the gate records its time.
5. **Coverage rows**, a family "Day-time interval arithmetic": `dt + dt2`, `dt - dt2`, `-dt`,
   `abs(dt)`, `dt * 3`, `3 * dt`, `dt / 7` (guarded), `dt / 24` (day split), `dt div dt2` (after
   30), the four `extract`s and `date_part`, `make_dt_interval(i, i2, i3)` and with a
   `DECIMAL(18, 6)` column, each naming its verification for row 490's check (the SMT sections, or
   `differential`).
6. **The fuzzers**: the Spark-level grammar gains the forms over `dt` and `dt2` (row 352's
   per-type draw, `VARKA-352.md` 2.2).

## 4. The baseline, and predictions

**The baseline first, its own pull request on the unchanged code**:
`VarkaDayTimeIntervalBaselineBenchmark`, a class of its own, over Arrow-cached `dt`, `dt2`, three
int columns and a `DECIMAL(18, 6)` column: the projections `dt + dt2 - dt`, `abs(dt)`, `dt * 3`,
`dt / 7`, `dt / 24`, `extract(DAY FROM dt)`, `extract(HOUR FROM dt)`, the four extracts together,
`make_dt_interval(i, i2, i3)` and with the decimal seconds, and the filter
`extract(HOUR FROM dt) >= 12`. Run on the EPYC 7763 runners at 256 and 128 bits through the
benchmark workflow, committed beside the class.

Predictions, registered before the build:

1. **No existing shape's bytes move**; the `option_arms` digests move with the new shapes.
2. **The arithmetic chain and `abs`** at least five times faster than the row path at 256 bits:
   three loads, two checked ops and a store against the row path's `IntervalMathUtils` calls.
3. **The extracts** at least three times faster at 256 bits: about a dozen lane operations and two
   `ConstDivide`s against two `%` and one `/` per row; the four together within twice one alone,
   by the shared chain.
4. **`dt / 7`** within 20% of `dt / 24`: one `ConstDivide` and a guard against the day split's
   chain.
5. **At 128 bits on x86**, the filter falls under row 503's two-lane rule as it stands and the
   projections still beat the row path.
6. **Test time**: the proof additions under 8 s per solver (VARKA-245's ratio); the coverage
   differential's new rows under 10 s (VARKA-317's observed ratio); the sweep, by hand, under
   five minutes on the laptop (VARKA-335's ratio, about 0.12, against a priced ten minutes).

## 5. Order, files and size

1. **The baseline** (4), its own pull request.
2. **103a, needing nothing unbuilt**: 2.1, 2.2, `extract(DAY)`, 2.4 with the day split and the
   `GUARDED` form, the proof additions, the cells and coverage rows of those forms.
3. **103b, after 332b and 157 and 347**: `HOUR`, `MINUTE`, `SECOND`, `make_dt_interval`, and the
   measurement against the baseline at both widths.
4. **103c, after 30 and 104**: 2.5's column forms and `div`.

| file | what |
|---|---|
| `VarkaIntervalCompiler.java` | the day-time arms of 2.1 to 2.6 |
| `VarkaTimeCompiler.java` | the identity relabel to a finer end field |
| `VarkaNodeCompiler.java` | `truncDivLong`, the day split helper |
| `sql/varka/proofs/long_divide.smt2`, `dt_arith.smt2`, `VarkaProofFiles.java` | 3.1's claims |
| tests | the emitter and sweep suites, the coverage rows, the fuzz grammar |
| benchmark | the baseline class and its results files |
| generated | `coverage.json`, `docs/sql-varka.md`, `emitted_bytes.json` |
| generated | `width_audit.json`, `census.tsv` |

**Size: medium, in four pull requests.** The baseline about half a day; 103a about two days;
103b about two after its prerequisites; 103c about a day after 30 and 104.

## 6. What it unlocks in the census

The day-time pairs of `abs`, `negative`, `+` and `-` count when 103a lands; `extract_dt_days`
with it, and `extract_dt_hours` and `extract_dt_minutes` with 103b; `extract_dt_seconds` and
`make_dt_interval` with 103b over 157's store and 347's leaf; `extract`, `date_part` and `datepart`
over `dt` when the four fields fuse; `dt_times_num` and `dt_div_num` for the integral types when
103c lands, their `float`, `double` and `dec` cells staying with 342, 361 and 363; `div` over `dt`
with 103c. `try_add` and `try_subtract` over `dt` stay with `TryEval`, which no row builds; their
`rows` column naming 103 is corrected in 103a's pull request.

## 7. Out of this row, and declines that stay

Out: the floating factor (342); a decimal column factor (361, 363); `TryEval` and the `try_`
forms over an interval; the legacy `CalendarInterval` forms; `avg(dt)`, whose
`DivideDTInterval(sum, count)` lives inside an aggregate (`expressions/aggregate/Average.scala:
124`), which Varka does not fuse.

Declines that stay: a literal zero divisor; `dt / -1` until row 414; a column factor before 104 and
a column divisor before 30; a batch with a lane Spark raises on (an overflowing sum, product,
negation or partial sum, a zero column divisor, `MIN / -1`, a value past `2^52` under the guarded
quotient); the truncating day-time casts (section 8, point 5); at two long lanes on x86 a condition
root under row 503's rule.

## 8. Questions for the owner

None of the owner's yet; the lead's open points, from the admission check:

1. **Who builds `truncDivLong`.** 332's 2.4 builds it in 332b, and its section 7 says "row 103,
   whose day-time work holds the truncating quotient". (a) Whichever of 332b and 103a lands first
   builds it with its `long_divide.smt2` claim, the other reuses. (b) 332b only, 103a waiting.
   *Recommended: (a)*: 103a otherwise waits on 481 through 332b for three lane operations.
2. **The double-factor cells.** The row text asks this row to test `HALF_UP` at ties, `2^53` and
   the long edges; 342's 2.4 builds the lowering and its 3.7 lacks those cells. (a) They join
   342's 3.7 from **[A, 7]**: `(2^53 + 1) * 0.5` is 4503599627370496, not `2^52 + 1`, by the
   conversion; `MAX * 1.0` and `(MAX - 511) * 1.0` raise while `(MAX - 512) * 1.0` is
   9223372036854774784 and `MAX * -1.0` is `MIN`; `7 / Infinity` is 0. (b) This row adds them as
   its own test after 342. *Recommended: (a)*, one owner per lowering.
3. **The seconds.** Row 351's text claims `extract(SECOND FROM dt)` and `make_dt_interval` over
   decimal seconds, and G1 says "its decimal seconds being 351's shapes", while this row's
   done-when lists both. Neither needs anything 351 builds: the extract is a remainder stored by
   157, the seconds operand a literal or a column read by 347. (a) This row builds both (103b), and
   351's text drops them. (b) This row builds `make_dt_interval` with the literal seconds only and
   the `r % M` node; 351 the rest. *Recommended: (a)*; with (b) the done-when needs the same split.
4. **The guarded quotient before row 30.** (a) 103a builds `ConstDivide` under the `2^52` guard
   for a literal `k` not dividing `D` from existing nodes, and 30's `MULHI` arm replaces it when
   chosen. (b) Such a `k` declines until 30. *Recommended: (a)*: the divisors that matter most
   divide `D` anyway (item 11).
5. **The casts parked on this row.** 332's section 7 sends day-time to day-time at a coarser end (54
   forms) here, and 350's section 7 the decimal to and from day-time casts ("row 103 or 332's
   interval work"). The first is `trunc(x / u) * u` for `u` in `D`, `H`, `M` and `10^6`, exact over
   every long by item 11; `durationToMicros` (`sql/api/.../util/SparkIntervalUtils.scala:56`) is
   to be read for its edge at the minimum duration first. (a) Both join this row as a 103d. (b) The
   coarser casts here, the decimal ones to 350 or 364. (c) Neither, recorded unclaimed.
   *Recommended: (b)*: the coarser casts are this row's division, the decimal ones are decimal
   casts.
6. **`ym * k` for a literal.** The census names 103 on `ym_times_num`; `ym * 2` over a stored
   column declines today for want of a checked int multiply (item 9), and 2.2's guard serves the
   int lane unchanged. (a) 103a applies it to `ym * k` for an integral literal. (b) Out.
   *Recommended: (a)*, one arm and the same proof.
7. **A decimal literal factor.** `dt * 1.5` and `dt / 1.5` are decimal forms (item 3). For a
   literal `c = u / 10^s` they are `HALF_UP(x * u / 10^s)` and `HALF_UP(x * 10^s / u)`: 2.2's guard
   on `x * u` or `x * 10^s`, then 2.4's division by a constant, all 64-bit; 2400176 cells over
   eight literals agree with Spark's `Decimal` path where the guard admits **[A, 8]**. (a) This row
   takes the literal forms; 361 and 363 keep the column ones. (b) All stay with 361 and 363.
   *Recommended: (a)*, the spelling users write.
8. **Row 414's evidence.** 414's symptom is a missing `== SQL ==` block. The closer fact is item
   1's: `IntervalMathUtils.negateExact` raises with `context = null`, so if the fallback evaluates
   a negation (the lowering `ym / -1` was given) rather than the original `DivideYMInterval`,
   whose own raise carries `origin.context`, the block vanishes as 414 describes. A second fact:
   `DivideDTInterval` and `DivideYMInterval` raise different classes for `MIN / -1` in the two
   engines (item 4; `intervalExpressions.scala:799-801`, `:818`), which would show as a condition
   mismatch if the fallback and the oracle took different paths. Both are candidates for 414, to
   be handed to that row and not fixed here.
9. **The G1 edges.** The waves table's G1 row says "103 after 28 and 329"; this plan needs 332b
   (or point 1's (a)), 157, 347, and for 103c 30 and 104. The table is outside this row's line, so
   its edit is the lead's.

## 9. Done when

Row 103's text holds, with the corrections above: `m5/PLAN.md` 2.38's list fuses - `+`, `-`,
negation, `abs`, the products and quotients by an integral literal and, after 104 and 30, an
integral column, the four extracts and `make_dt_interval`, each with coverage rows; every division
is exact over every long (the extracts, the divisors of `D`) or under its stated `2^52` guard; the
SMT claims of 3.1 hold under both solvers; the cells of 3.3 pass at both widths, with the error
class of each declined lane the row engine's; the sweep has run in the gate with its time recorded;
the measurement against the baseline is committed at both widths with its predictions scored; and
the PR build and the option matrix pass.

## 10. Outcome

