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

The cause is the draw's hostile years, not the view's casts in general. `CAST(m AS INTERVAL
MONTH)` is the identity on months and cannot overflow. `CAST(y AS INTERVAL YEAR)` is `12 * y` and
raises for `|y| > 178956970`. The hostile pool for `y` in `VarkaSparkFuzz.drawData` was
`{178956971, Int.MaxValue}`, both of which overflow, so any hostile `y` cell made the cached view
raise; the safe pool (`-2` to `4`) and the wide pool (`-100` to `100`) never do, and the hostile
`mm` pool (`12`, `-1`) does not matter on its own. The expected share follows: a full case has
one hostile row on average, with one to three of its twelve columns hostile, so about one case in
six has a hostile `y` cell, 17%; the census measured 17 to 18% of the full pools and none of the
safe ANSI-off pools. The check would have rejected a fix that only skips the failing compositions:
that keeps the rate and removes the volume.

## 3. The design

### 3.1 Total columns, and hostile years that fit

Built: the fixture's `ymy` and `ym` are `CASE WHEN <the 64-bit product is in the int range> THEN
<the cast or construction> END`, so the view never raises, and the hostile `y` pool is `{178956969,
-178956970}`, the largest years that fit with every `mm` the pool draws. The hostile cells are
therefore valid and extreme, and the interval operations of a composition (`d + ym`, `ym - ymm`)
are what overflow, which is the comparison the pool was drawn for. The pool keeps its size, so
the main stream's draws are the ones it drew before. The alternative of skipping compositions
whose fixture raises was not built: it keeps the rate.

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

* `the fixture builds for every drawn case, so no composition compares fixture errors`: 600
  drawn cases in their own ANSI mode, the fixture collected without raising. It fails on the old
  fixture (composition 17, `CAST_OVERFLOW` for 2147483647) and passes on the new one.
* The suite's other 18 tests, `VarkaSparkReproducerSuite`, `VarkaGoldenCorpusSuite` and the
  differential suites pass; no pinned fixture moved.
* The census at 5,000 compositions on seven seeds (section 9).

## 6. The measurement

The share of compositions that error on both sides, per pool, on the seven seeds of the census
(20261101 to 20261107, 5,000 compositions each), before and after, from the line each run prints.

### 6.1 Predictions, registered before the run

1. The "full ANSI on" and "full ANSI off" pools fall from 17 to 18% erroring on both sides to
   under 6%, since the compositions' own `CAST(i AS INTERVAL ...)` still raise.
2. The "safe ANSI on" pool is unchanged (2 to 27 of about 1,260).
3. No new disagreement appears in 35,000 compositions.

## 7. Risks

1. Making the columns NULL hides an error the engines should agree on. The compositions' own
   casts and `ym - ymm` still raise, which the census shows by every entry still being raised.
2. The over-the-limit years (`178956971`) are no longer drawn into the view. The compositions'
   own `CAST(i AS INTERVAL YEAR)` over a hostile `i` (`Int.MaxValue`) still reach that edge, and
   the unit suites (`VarkaDifferentialSuite`) pin it.

## 8. Sequencing

The fixture test first, run against the old fixture to see it fail; then the fixture and the
pool; the census; the entry's reason text last.

## 9. Outcome

On the seven seeds, 5,000 compositions each (15 October 2026, scratch `372/`; before: `C/`):

| | before | after |
|---|---|---|
| compositions erroring on both sides, of 5,000 | 393 to 479 | 14 to 68 |
| full ANSI off pool, of about 1,250 | 177 to 230 | 2 to 16 |
| full ANSI on pool, of about 1,250 | 207 to 228 | 10 to 39 |
| safe ANSI on pool, of about 1,260 | 2 to 27 | 2 to 27 |
| disagreements, unexpected conditions | 0, 0 | 0, 0 |

Predictions: 1 held (at most 3.2% in the full pools); 2 held; 3 held (35,000 compositions). The
iteration-1405 case of seed 20261008 is a different case now, because the draw changed, and the
warm arm passes at that seed. Every entry of the expected-errors list is still raised in at
least one seed; `CAST_OVERFLOW` now in four of seven, so its reason text says where it comes
from. What is left: the engines still differ in which unused column of a view raises first, which
the fixture no longer shows them; that is not fixed, and a composition over a view with a
raising column would still compare an arbitrary one of two errors.
