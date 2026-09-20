# Task 105: the `TIME` surface benchmark

*Opened 20 September 2026 from `PLAN_MILESTONE_5.md` 2.40. The number the
milestone's message quotes comes from this file's descendants, so the plan is
written before the first run and the predictions before the first number.*

## 1. Where this sits

The date surface (`PLAN_TASK_62.md`) is the shape: a `Surface`-shaped inventory,
one entry per expression in the spelling a reader would write, the projection
and the filter form of each, run by one driver over one cached table through
`dev/varka_bench_surface.sh` against four arms - stock 4.2.0 on JDK 17 and 25,
the fork with the engine off, the fork with the engine on - with the canary, the
datapath probe, the residency guard, the fixed-share rule and the `EXPLAIN`
check applying to every file written. Task 105 adds the `TIME` family to that
machinery without changing what the machinery checks.

What it produces is the input to two things after it: task 101's band for the
`TIME` surface, which the message needs before any per-entry figure is quoted,
and task 118, which takes the surface to a runner the datapath probe proves
full-width and writes the README's `TIME` table from it.

## 2. What is built

**The table, `varka_times`.** Two `TIME(6)` columns, two day-time intervals and
two `bigint`s - one 64-bit lane each, which is the milestone's point - generated
from `range` the way `varka_dates` is, with a fixed null pattern per column:
every 31st `t` null like `d`, every 47th `dt`, every 53rd `l2`. `t` and `t2` are
spread over the whole day by coprime strides through `make_time`; `dt` is under
a minute and runs forward before noon and backward after it, so `t + dt` stays
inside the day on every row - a crossing row is Spark's own error, Varka's guard
declines the batch, and a surface row is meant to time the kernel and not the
decline (`PLAN_TASK_102.md` 7.1). `dt2` is sub-second; `l` and `l2` are counts
under ten thousand million so the comparisons against `5000000000` select about
half.

The type sits behind `spark.sql.timeType.enabled`, internal and off outside
tests in every distribution the surface runs against. The driver sets it on the
session when it builds this table, so no arm can forget it: a forgotten flag
would fail the stock arm alone, at parse time, after the canary and the build.

**The inventory, `Times`.** Twenty-three entries over the coverage table's own
shapes, since the coverage suite already proves those fuse and the first full
run should be a measurement rather than a search for the entry that does not:
the three field extracts as projections; `time_trunc` at two levels; `t - t2`,
`time_diff` at two units; `t + dt`; the choice family over `TIME`, intervals
and `bigint`; and eight predicates on their own. The extracts have no filter
form on purpose: `hour(t) = 12` puts the narrowed int under a comparison, which
the kernel cannot hold until task 28, so the compiler declines it and its row
would time the row engine (`PLAN_TASK_102.md` 8.6).

**The driver.** `DateSurfaceBenchmark.run` takes the table its entries read as a
`TableShape` (`ALL`, `DATES`, and now `TIMES`), which names the view, the
columns and the column the filter shape's columnar consumer selects; the
query builders and the checksum take the shape. `TimeSurfaceBenchmark` is the
six-line entry point, as `DateChainBenchmark` is. `--benchmark time` selects it
in the shell driver, the merge tool knows the `TimeSurface` stem, and the
workflow offers the choice.

**The tests.** `TimesTest` runs every entry in both shapes on the stock release
the module compiles against, over a thousand rows, which is also the check that
the type and every function the list uses exist in the release the stock arm
downloads; asserts the table's null counts, the spread of `t` over all 24
hours, that `t + dt` exists on every row and that `dt`'s sign follows `t`'s half
of the day; that the entries read the times table and are refused the date one;
and that no label is shared with the date surface, so a merge cannot confuse
their files.

## 3. What is not built here

The results files. Two things stand in front of them. The extracts fuse only
with #268 (task 102 group C) merged, and every entry is held to
`--expect-fused`; a run before that fails on `hour(t)`, correctly. And the row
count is a measurement, not a choice: the date surface settled on 1e9 rows in
32g because the fixed-share rule (under 5% of wall time per Varka row) pushed
it up and the residency guard bounded it above. This table's rows are 8 bytes a
column where the date table's are 4, so the same memory holds half the rows,
and the extracts' kernels are divide-bound rather than bandwidth-bound, so they
may clear the rule at fewer rows. Section 5 says how the number is found.

## 4. Predictions, registered before the first run

1. **The allocation rows read a larger ratio against stock than the date
   surface's median.** `hour`, `minute`, `second` and both `time_trunc` rows
   against stock 4.2.0 on JDK 25 read above the date surface's committed median
   of 19.5x (README, the surface's 45 projection rows), because the stock arm
   builds a `LocalTime` per row where the date arm did integer arithmetic.
2. **Most of that difference is the baseline, not the lane.** The same rows
   against the fork with the engine off - which runs the same `LocalTime` path
   - read within 20% of their stock ratio, as the date surface's engine-off
   control tracked stock (49 of 50 pairs within 20%, `PLAN_TASK_62.md`); and the
   arithmetic rows (`t - t2`, `time_diff`, `t + dt`, the predicates) read in the
   date surface's ordinary class, near its 10x, because stock is integer
   arithmetic there too.
3. **The row count that clears the fixed-share rule is between 2.5e8 and 5e8**
   in a 32g driver, and the cache is resident there: about 48 bytes a row
   uncompressed against the date table's 23 GiB at 1e9 rows.
4. **The filter rows split as the date surface's did**: the columnar-consumer
   filters win by less than their projections, and the counted filters least,
   since a count is one aggregate over a mask and the row engine's cost per row
   is smallest there.
5. **No entry declines.** Every entry fuses on the fork with `--expect-fused`
   and zero fallback batches, at the first run after #268 merges.

## 5. Sequencing

1. This PR: the table, the inventory, the driver's shape parameter, the entry
   point, the selectors, the tests, this plan. No numbers.
2. After #268 merges: a smoke run of the fork arm alone at a small row count
   with `--expect-fused`, which is prediction 5's test and costs minutes.
3. The row-count ladder on the fork arm, 2.5e8 and 5e8 rows in 32g, reading the
   fixed share and the residency line; the smallest count that clears both is
   the committed one (prediction 3).
4. The four arms at that count on the laptop, committed as
   `TimeSurface-<label>-results.txt` with provenance, and the diff table read
   against predictions 1, 2 and 4.
5. The band (task 101): twelve runs of the fork arm under a scratch label, by
   shards if one arm takes over twenty minutes, written as
   `TimeSurface-jdk25-band.txt`; no per-entry figure is quoted before it exists.
6. Task 118 takes the file to the full-width runner and writes the README.
