# VARKA-372: The fuzzer fixture raises on a hostile row, so a tenth of its compositions test nothing

## 1. Where this came from

`VARKA-319.md` 2 found on the way that about 8% of the composition fuzzer's cases read a fixture
that overflows its interval casts, and recorded it as a debt. The expected-errors census at
volume (35,000 compositions, 7 seeds, 15 October 2026, scratch `C/`) measured it: 393 to 479 of
each 5,000 compositions (8 to 10%) end in an error on both sides, none of them in the pools drawn
"safe" under LEGACY and 17 to 18% of the "full" pools, mostly `CAST_OVERFLOW`. Those cases compare
an error class with an error class and nothing else, so about a tenth of the fuzzer's volume
checks no value. The same fixture caused the one failure of the warm-up run at volume
(`VARKA-295.md` 9.8, seed 20261008, iteration 1405): `CAST_OVERFLOW` against
`INTERVAL_ARITHMETIC_OVERFLOW`, each side's error legitimate for a different column of the same
hostile row, because the side that reads the cached view computes every column and the row engine
prunes the unused one. A nightly run of the warmed arm can fail on this at random.

## 2. The admission check, done

To be done before the change: for each column the fixture builds from a drawn int (`ymm`, `ymy`,
`ym`), the exact set of drawn values that raise. `CAST(m AS INTERVAL MONTH)` is the identity on
months and cannot overflow. `CAST(y AS INTERVAL YEAR)` is `12 * y` and raises for `|y| >
178956970`; `make_ym_interval(y, mm)` is `12 * y + mm` and raises when that leaves the int range.
The check enumerates the fuzzer's value pools (`VarkaSparkFuzz`) against these two bounds and
counts the rows that raise, so the 8 to 10% is explained row by row. It would reject a fix that
clamps `y` for every column, because `y` also reaches no other column and the boundary values
`178956970` and `-178956970` must still be drawn.

## 3. The design

### 3.1 Total columns, not skipped compositions

Two forms are built and measured. (a) The fixture's `ymy` and `ym` columns are NULL where the
cast or the construction would overflow, so the view never raises and every composition compares
values; the boundary years that do not overflow stay in the pool. (b) The composition is run on a
fixture materialised once up front, and a composition whose fixture raises is counted and skipped,
not compared. (a) recovers the volume; (b) is the smaller change. The default is the one that
leaves fewer compositions erroring on both sides without removing a hostile value from the pool.

### 3.2 What is deliberately unchanged

The expected-errors list (`VARKA-319.md`): `CAST_OVERFLOW` stays listed because the compositions
themselves cast drawn ints. Its reason text changes if the fixture no longer raises it. The
dependence of the answer on whether a side computes unused columns is not removed from the
engines, only from the fixture.

### 3.3 Registered op counts

None; no emitter change.

## 4. Files

| file | what |
|---|---|
| `sql/core/src/test/scala/org/apache/spark/sql/execution/VarkaSparkFuzz.scala` | the fixture text (`fixtureRows`) |
| `sql/core/src/test/scala/org/apache/spark/sql/execution/VarkaSparkFuzzSuite.scala` | the census and a test for the fixture |
| `sql/varka/fuzz/expected_errors.tsv` | the `CAST_OVERFLOW` reason, if it changes |

## 5. Tests, and what each is for

* A fixture test: over every value of the pools, the view builds and counts without raising
  (catches a hostile value reintroduced to the pool).
* The warmed arm at seed 20261008 over iteration 1405 passes (the failure that started this).
* The census run again at 5,000 compositions on seven seeds: the share of compositions erroring
  on both sides falls from 8 to 10% to the share the compositions' own casts produce, and the
  list's other entries are still raised.

## 6. The measurement

The share of compositions that error on both sides, per pool, before and after, on the same seeds
(20261101 to 20261107, 5,000 compositions), by the census line each run prints. No benchmark.

### 6.1 Predictions, registered before the run

1. The "full ANSI on" and "full ANSI off" pools fall from 17 to 18% erroring on both sides to
   under 6%, since the compositions' own `CAST(i AS INTERVAL ...)` still raise.
2. The "safe ANSI on" pool is unchanged (2 to 27 of about 1,260).
3. No new disagreement appears in 35,000 compositions.

## 7. Risks

1. Making the columns NULL hides an error the engines should agree on. The compositions' own
   casts and `ym - ymm` still raise, which the census shows by the entries still being raised.
2. A fix that clamps the pool loses the boundary draw. Section 5's census shows the boundary
   values are still drawn.

## 8. Sequencing

The admission check and the fixture test first; the fixture change behind a property, with the old
form kept to reproduce the flake; the census; the default and the list's reason text last.

## 9. Outcome

Not started.
