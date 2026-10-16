# VARKA-245: The proofs restated as refinements of the specification

## 1. Where this came from

Row 245 of `m7/PLAN.md`, item 59 step 3 of `m9/SCOPE.md`: "Item 58's proofs re-stated as refinements
of the spec, soundness and guard completeness, in Spark mode and in standard mode; the standard-mode
register cites the standard-mode theorems as its bounds." The proofs of rows 240 to 242 show a
lowering equal to a Java formula - the multiply-high equal to `/`, a calendar site's magic equal to
its quotient - and the specification of rows 243 and 244 (`proofs/spec/STANDARD.md`, `DELTAS.md`,
`EXTENSIONS.md`) says what the operations are. Nothing yet connects the two: no file states that a
kernel's answer is the specification's answer, or that its declines are exactly where the
specification raises or the lowering's bound is passed. `m7/PLAN.md` 2.1 fixes the form, from
`m7/READING.md` 11's reading of Alive: "five queries per mode over (data, valid) pairs, data free
under a null bit: a satisfiable precondition, a decline wherever Spark raises, null-ness equal,
values equal on valid lanes, and a decline only where Spark raises or the input leaves the bound."
The owner kept the row on 13 October 2026, to be decided again when 244 lands (`m7/PLAN.md` 9.4
item 1); 244 is in review as #854, and this branch is based on it.

## 2. The admission check, done

Done on 16 October 2026, before any writing. What had to be true: that the solvers decide the
hardest refinement, the calendar's decomposition against the standard's day count, within the
per-check limit (`VARKA_PROVE_LIMIT_MS`, 10 s) and with room in the linters' minute; and that the
standard's calendar rules can be tied to a closed formula without a sweep.

### 2.1 The decomposition in one check: too slow

The default prefix (`julianMap`, `neriSchneiderMonth`: `VarkaChrono.narrowedJulian` and `fields`),
every magic replaced by its exact quotient, as `chrono_divide.smt2` proves each one is over its
site's dividends, and the claim that for every day `n` in `[NARROW_MIN_DAYS,
NARROW_DECOMPOSE_MAX_DAYS]` the year, month and day it gives are a valid date whose
`range_calendar.smt2` `cal.epoch` is `n`: **unsat, in 91 s under Z3 and 28 s under cvc5** on cores
12 to 23 beside other agents' jobs. True, and nine times the per-check limit.

### 2.2 Split into lemmas: seconds

With the standard's day count written as one floor-division formula (`spec.days`, Hinnant's
`days_from_civil` over SMT-LIB's flooring `div`, one branch for every year where `cal.epoch` has two):

| check | Z3 | cvc5 |
| :--- | ---: | ---: |
| the era's tail: every day of era `[0, 146096]` gives a valid date with `spec.days` = day of era less 719468 (under `ALL`) | 1.9 s | 0.6 s |
| the same under `QF_NIA` | 15.5 s | 1.4 s |
| the 400-year period: `spec.days(y + 400 k, m, d) = spec.days(y, m, d) + 146097 k`, and the month length, for every `y`, `k` (under `ALL`) | 0.1 s | 1.0 s |
| the same under `QF_NIA` | 0.8 s | past 120 s |
| the rules: 1970-01-01 is day 0; the day after a day of the month, the first of the next month after the month's last day, 1 January after 31 December, each one more - for every year | 0.01 to 0.1 s | 0.01 to 0.1 s |

The rules make `spec.days` the Gregorian calendar's count without an exhaustive sweep: every valid
date's successor is one more, and day 0 is 1970-01-01, so by induction in both directions it counts
days, for every integer year (item 59 expected "rules to formula by exhaustion"; the solver does it
for all years in milliseconds). The file declares `ALL`: cvc5 does not decide the period under
`QF_NIA`, and Z3 takes eight times longer on the tail.

**What it would have rejected:** a form that misses the 10 s per-check limit under either solver,
or a period lemma neither solver decides; then the calendar refinements would have been stated over
the suite's sweep instead of proven. Neither happened.

### 2.3 What the kernels are, read from the emitter

`dev/varka_emit.sh` on each coverage row restated (16 October, this branch), and the guards
`VarkaGuardSpecs` reads (`VarkaLoopEmitter.guardsForTest`):

* `i + 1`, `i - 1`, `-i`, `year(d) + i` are `int:ADD:FAIL` and the like under ANSI - an
  `INT_OVERFLOW` guard on the exact result - and `WRAP` without ANSI;
* `date_add(d, i)` and `d + CAST(i AS INTERVAL DAY)` are both `addDays col col`, Java's wrapping
  add, the cast's bound checked per batch on the offset column (`VarkaInputBound`,
  `VarkaChrono.INTERVAL_DAY_LIMIT_DAYS`); no guard on the result unless a calendar node reads it;
* `year(d)` over a bare column is `(year col:0)` with **no guard**: the range analysis takes the
  column to lie in the project's column contract, 0001-01-01 to 9999-12-31
  (`VarkaRangeAnalysis.CONTRACT`), which "is not checked at ingestion" (`docs/sql-varka.md`);
* `d + ym`, `add_months(d, i)` are `addMonths`, the count held to `[MONTH_ARITH_MIN_MONTHS,
  MONTH_ARITH_MAX_MONTHS]` by a `MONTH_COUNT` guard; `d - ym` negates the count with a `FAIL` node;
* `extract(YEAR FROM ym)` is `divc:12`, the multiply-high `int_mulhi_divide.smt2` proves exact for
  every int;
* `ym - ymm` is `int:SUB:FAIL` and `abs(ym)` an `IfElse` over `neg:FAIL`;
* `t + dt` (`VarkaTimeCompiler.timeAddInterval`): the interval held to a day by a `GuardedRange`,
  times 1000 wrapping, the sum held to `[0, NANOS_PER_DAY - 1]` by a second; `t - t2`, `hour`,
  `minute` and `second` are `ConstDivide`s of the nanoseconds (`long_divide.smt2`,
  `time_bounds.smt2`);
* `weekday`, `dayofweek` and the ISO day of the week are `floorMod(n, 7)` (`floor_mod7.smt2`) and a
  modular offset, so the `+ 3` never overflows.

## 3. The design

### 3.1 A refinement, as five queries

One rendered file, `sql/varka/proofs/refinement.smt2`, by `VarkaProofFiles.refinement()`. Per lane, a
kernel's outcome is `(decline, valid, data)` and the specification's `(raises, valid, value)`, both
functions of the operands' `(data, valid)` pairs; the data under a clear bit is a free variable. For
each operation and each mode, after a precondition `pre` (each operand a value of its lane and its
type's contract - a time in `[0, NANOS_PER_DAY)`) and the lowering's registered `bound` (a guard's
domain, an input bound, the column contract), five checks, in `m7/PLAN.md` 2.1's words:

1. **a satisfiable precondition** - `sat`: `pre`, `bound`, every operand valid and the specification
   defined, so the next four are not vacuous;
2. **a decline wherever the specification raises** - `unsat`: `pre`, `raises`, not `decline`;
3. **null-ness equal** - `unsat`: `pre`, not `raises`, not `decline`, `valid_k != valid_s`;
4. **values equal on valid lanes** - `unsat`: `pre`, not `raises`, not `decline`, `valid_s`,
   `data != value`;
5. **a decline only where the specification raises or the input leaves the bound** - `unsat`:
   `pre`, `bound`, `decline`, not `raises`.

2 to 4 are soundness, 5 guard completeness; a null lane is inside every bound, so 5 also says a null
never declines. The kernel's arithmetic is written with `java.smt2`'s operators, and where a lowering
proof holds a form equal to an operator - a magic to its quotient, the multiply-high to `jint.div`, a
long division to `jlong.div`, the leap hash to the Gregorian rule - the refinement uses the operator
and cites the proof. That substitution is only sound over the range the lowering proof covers, so
the file **checks it**: for each site the refinement uses, a check that every dividend it feeds the
site lies within the site's `maxDividend` and its stride and residue (read from
`VarkaChronoLowering.ChronoDivide`, not typed), and that every year fed to the leap flag lies within
`[-YEAR_BIAS, LEAP_HASH_MAX_BIASED_YEAR - YEAR_BIAS]`. The composition is then lemmas the solver
holds, not a remark in a header.

### 3.2 The two modes

* **Spark mode**: the specification is `STANDARD.md`'s definition with `DELTAS.md`'s patches
  applied, or `EXTENSIONS.md`'s definition. Where a delta holds in one ANSI setting only
  (`overflow_wraps`, `overflow_yields_null`), the two settings are two arms, each against the
  kernel the compiler emits under it (`WRAP` or `FAIL`; `make_date`'s null or decline).
* **Standard mode**: the unpatched definition, with the register's ranges as the bound
  (`SCOPE_STANDARD_MODE.md` 2: assume the standard's range, register it as an input bound, decline
  outside), over the kernel as the emitter builds it. Where a delta is a *result* - one no guard can
  make agree - the refinement is false, and the query that fails is written `expect sat` with the
  delta's own example as its witness (`DATE '2024-01-31' + INTERVAL '1' MONTH` for
  `clamp_to_month_end`), followed by the same query with the delta's inputs excluded, `unsat`, which
  shows the delta is the only departure. The register (3.1 to 3.3, 3.7) then cites the `unsat`
  theorems as the bounds it would register and the `sat` witnesses as what standard mode cannot buy.

Where no delta touches an operation the two modes are one statement, and the file says so once
rather than rendering the same five checks twice.

### 3.3 The calendar layer

Under the refinements, the lemmas of 2.2 as checks: the four rules (with the leap year as C1
defines it), `spec.days` pinned to `LocalDate.toEpochDay` at rendered dates around year 0, the
1582 change, 1970, 9999/10000 and the narrow range's ends; the era's tail, the 400-year period,
their composition over the guarded range, and `spec.days` injective over valid dates, so the
kernel's date is *the* date of `n`. A calendar refinement then asserts those lemmas' instances
and is linear over a few variables. The inverse direction `add_months` recomposes with
(`daysFromCivil`) gets the same: equal to `spec.days` over every valid date of the years the month
arithmetic reaches.

### 3.4 The operations restated

| `STANDARD.md` / `EXTENSIONS.md` | coverage rows | lowering proofs composed | modes |
| :--- | :--- | :--- | :--- |
| 4.1 `+ - *` and negation | `i + 1`, `i - 1`, `-i`, `year(d) + i`, `year(d) * 2` | none (`java.smt2`) | Spark non-ANSI, Spark ANSI, standard |
| 4.10 YEAR, MONTH, DAY | `year(d)`, `month(d)`, `day(d)` | `chrono_divide`, the lemmas | Spark, standard |
| 4.11 HOUR, MINUTE | `hour(t)`, `minute(t2)` | `long_divide`, `time_bounds` | one statement |
| 4.12 YEAR of an interval | `extract(YEAR FROM ym)` | `int_mulhi_divide` | Spark, standard |
| 4.13 date plus months | `d + ym`, `d - ym`, `add_months(d, i)` | `chrono_divide`, `leap_hash`, the lemmas | Spark, standard |
| 4.14, 4.19 date plus days | `date_add(d, i)`, `d + CAST(i AS INTERVAL DAY)` | none | Spark, standard |
| 4.15 time plus interval | `t + dt` | `time_bounds` | Spark, standard |
| 4.16 time minus time | `t - t2` | `long_divide`, `time_bounds` | one statement |
| 4.17, 4.18 interval arithmetic | `ym - ymm`, `abs(ym)`, an interval times a literal | none | one statement |
| ext. `datediff` | `datediff(d2, d)` | none | Spark |
| ext. day of the week | `weekday`, `dayofweek`, `extract(DAYOFWEEK_ISO ...)` | `floor_mod7` | Spark |
| ext. `make_date` | `make_date(2021, i, 1)` | `leap_hash`, the inverse | Spark non-ANSI, Spark ANSI |
| ext. time conversions | `second(t)`, `time_from_micros(...)` | `long_divide`, `time_bounds` | Spark |

### 3.5 What is deliberately unchanged

* **The analyses' proofs** (`value_range`, `range_analysis`, `range_calendar`) state the
  preconditions the bounds come from, not lowerings; they are cited, not restated.
* **The proofs of 240 to 242** stay as they are: the refinement composes them.
* **`STANDARD.md` 4.2 to 4.9** (comparison, `IS NULL`, the connectives, `IN`, `CASE`, `COALESCE`)
  have no lowering proof to restate: no arithmetic, a lane-wise compare or blend. And the extensions
  whose lowerings carry their own algebra - the ISO week and week-year, `dayofyear`, `quarter`,
  `last_day`, `next_day`, `trunc`, `make_ym_interval`, `greatest`/`least`, `time_diff`,
  `time_trunc` - are not in this row; section 9 says where they go.
* **The century-then-year prefix**, the live reference variant behind `julianMap=false`: the
  default is restated; the variant is held by the suite's sweep, and section 9 says whether its
  lemma was cheap enough to add.
* **No kernel, emitter, guard or coverage row changes.** A refinement that fails because the code is
  wrong is a finding for the owner, not something this row fixes.
* **`STANDARD.md`, `DELTAS.md`, `EXTENSIONS.md`** are #854's; a correction one of them needs is
  reported, and at most a one-line pointer to the theorems is added.

### 3.6 Registered op counts

None: nothing is emitted differently.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/src/test/java/.../varka/VarkaProofFiles.java` | `refinement()` and its `render()` entry |
| `sql/varka/proofs/refinement.smt2` | rendered |
| `sql/varka/proofs/README.md` | its row |
| `sql/varka/plans/SCOPE_STANDARD_MODE.md` | 3.1, 3.2, 3.3 and 3.7 cite the theorems |
| `sql/varka/plans/m7/PLAN.md` | row 245 only |

## 5. Tests, and what each is for

* **`refinement.smt2` under `dev/varka_prove.sh --solver both`**, whole and each check alone (the
  README's step 4): every verdict its expectation. A kernel whose answer, null-ness or declines
  leave the specification fails a check; so does a site fed past its proven range.
* **`VarkaProofFilesSuite`**: the committed file is the rendering, so a constant that moves in
  `VarkaChrono`, `DateTimeConstants` or `ChronoDivide` without the proof fails here.
* **Planted faults, by hand, not committed**: the clamp dropped from the Spark-mode `add_months`
  kernel, the `INT_OVERFLOW` guard dropped from the ANSI arm, a null lane allowed to decline, the
  time guard's bound moved by one, and a rule's leap year wrong; each must fail a check.

## 6. The measurement

None in the benchmark sense. The solve times, under both solvers on the laptop, and the linters'
step before and after.

### 6.1 Predictions, registered before the run

The calibration table in `skills/testing-and-debugging.md` has no line for a solver's time; the
runner's ratio to the laptop is VARKA-241's 2.1 (`VARKA-241.md` 9.3).

1. **The file holds under both solvers** in every Spark-mode refinement, over the column contract
   for the calendar rows; standard mode is false exactly where `DELTAS.md` says "a result" -
   `overflow_wraps` (the `WRAP` kernel), `clamp_to_month_end`, `time_add_raises` - and in one place
   it does not: a date plus days, whose kernel has no guard on its result, so year 10000 is an
   answer where the standard raises (`proleptic_range`, which `DELTAS.md` calls a range).
2. **Under 250 checks; under 8 s under Z3 and under 8 s under cvc5** on the laptop with cores 12
   to 23 loaded, so the linters' step grows by about 17 s on the runner. The estimate is the
   heaviest spiked check (the era's tail, 1.9 s under Z3), the rest linear over a few variables.
   If Z3 is the slow one by more than double, the file joins `CVC5_IN_LINT`.
3. **The column contract is load-bearing and the proof says so**: without it, the unguarded
   `year(d)` is not a refinement of the Spark-mode definition (`proleptic_range` makes any int32 day
   a date), and the check that shows it is `sat` at the first day past
   `NARROW_DECOMPOSE_MAX_DAYS`.
4. **`VarkaProofFilesSuite` grows by under a second**; the gate and the nightly by the solver times
   of 2 alone (the nightly runs both solvers).
5. Every planted fault of section 5 fails at least one check.

## 7. Risks

1. **The refinement restates the emitter by hand.** A kernel written in `java.smt2` is a second
   implementation, and could agree with the specification while the emitted code does not. The
   lowering proofs and the emitter suites (the scalar twins, the sweeps) hold the code to the same
   forms; this row says which form each refinement assumes, with the IR `dev/varka_emit.sh`
   printed (2.3).
2. **A lemma instance asserted in the wrong form** would make a refinement vacuous. Each refinement
   keeps its query-1 `sat`, and each lemma is its own `unsat` check over symbolic variables.
3. **cvc5's fragility** (the README's step 4): a check that holds alone comes back `unknown` in a
   run. The file is run whole and each check alone.
4. **The linters' minute.** If the step passes it, the file is the one to move to the nightly.

## 8. Sequencing

1. This plan, and row 245 marked Planned.
2. `refinement()` and the rendered file, the README row; run under both solvers.
3. The register's citations, section 9 and the row's outcome.
