# Task 144: what the long lane costs end to end

*Milestone 5, section 2.80. Opened and measured the night of 17-18 September
2026, from task 29's registered prediction 6.1.2, which nothing had scored.*

## 1. Where this came from

Task 29 made the long lane reachable from SQL and registered a prediction it
could not score: a `bigint` comparison filter over an Arrow-cached table should
run at **0.45x to 0.60x** of the same filter over an `int` column. Task 142 had
priced the lane at the *kernel* - 1.5x to 2.0x while both arms are in cache, a
flat 2.11x to 2.20x once they are not - and the prediction carried that number up
a layer by assuming the rest of the query scales with it.

Nothing measured it, and the milestone is about to build four more tasks on this
lane (88, 102, 103, 104), each to be judged by a ratio against the row engine.
A baseline lands as its own change before the work it judges, so this is it.

## 2. The admission check, done

**The measurement is possible today and was not before.** Task 29 admits
comparisons, `IS [NOT] NULL`, the connectives, `greatest`/`least` and `CASE WHEN`
over `bigint`, `TIME(p)` and day-time intervals; no arithmetic. That is enough
for a filter and a projection at both widths, which is what the prediction is
about.

**The int-lane twin of a long projection is a date column, not an int column.**
A bare `int` column is admitted as a comparison or arithmetic operand (task 122)
and never as a *value*, so `greatest(i, i2)` declines where `greatest(l, l2)`
fuses. The date columns are the int lane's value leaves and the same 32 bits, so
the pairing is by lane width rather than by Spark type. Found by the benchmark's
own fusion assertion, which is why every case asserts before it times.

**Every case asserts it fused.** A benchmark whose subject silently fell back
would report the row engine on both arms and call the ratio 1.0. The check uses
the four node classes `VarkaSharedSessions.isVarkaNode` names - a filter feeding
a row consumer is a `VarkaFilterColumnarToRowExec`, and a check that looked only
for `VarkaFilterExec` would call a fused plan unfused, which is exactly what the
first version of this benchmark did.

## 3. The design

`VarkaLongLaneThroughputBenchmark` in `sql/core`, on
`VarkaThroughputBenchmark`'s pattern: two sessions on one context, one with Varka
and one without, an Arrow-cached table, `noop()` sinks, and the committed-run
methodology of five iterations over two-second windows.

Every case is a pair - one shape at two widths over identical values - so the
ratio of the two Varka rows is the lane's end-to-end price and the ratio of each
Varka row to its own baseline is what the engine is worth on that shape. `TIME`
and day-time interval cases run beside the `bigint` ones because all three share
the lane and none shares a Catalyst expression, so a lowering that was
accidentally type-specific shows as one row out of line.

**Two scales, because one cannot answer the question.** At two million rows the
per-batch fixed costs are a large share of the work and do not double with the
lane; at twenty million the kernel's share grows. Task 142's finding - that the
lane's cost depends on where the working set sits - is the reason to expect the
ratio to move, and the second scale is what shows whether it does.

**And a crossed experiment**, because the first two scales disagreed: the same
filter at two widths, with the compared column and the output column crossed,
plus a `count(*)` form that outputs no column at all.

## 4. Files

| file | what |
|---|---|
| `VarkaLongLaneThroughputBenchmark.scala` | the benchmark |
| `VarkaLongLaneThroughputBenchmark-jdk25-results.txt` | its committed results |
| `sql/varka/plans/PLAN_MILESTONE_5.md` | section 2.80, rows 144 and 145 |
| `sql/varka/plans/PLAN_TASK_29.md` | 9.1's prediction 2, now scored |
| `sql/varka/skills/benchmarking.md` | the lesson of 9.2 |

## 5. Tests, and what each is for

None of its own: a measurement whose guard is the fusion assertion on every case.

## 6. The measurement

Committed in `sql/core/benchmarks/`. The numbers below are rates in millions of
rows per second, Varka arms only, from that file.

## 7. Risks

1. **One machine, one row count each.** The ratios below are this laptop's; the
   two scales are what keep the reading honest rather than a single number.
2. **The `noop` sink** means no consumer reads the output, so a projection's cost
   is the kernel and the write, not a downstream read.

## 8. Sequencing

After task 29, before tasks 102, 103 and 104, whose numbers this is the baseline
for.

## 9. Outcome

**Task 29's prediction is wrong, and the way it is wrong is the result.** The
long lane's end-to-end cost is not a single band, and at no scale is it the
kernel's 2.1x:

| shape | int32 | int64 | ratio |
|---|---:|---:|---:|
| filter, column against column, 2e6 | 62.9 | 60.4 | 0.96x |
| filter, column against literal, 2e6 | 180.4 | 132.0 | 0.73x |
| filter, two conjuncts and a null check, 2e6 | 69.6 | 58.8 | 0.84x |
| projection, greatest, 2e6 | 227.1 | 207.7 | 0.91x |
| projection, CASE WHEN, 2e6 | 240.1 | 211.2 | 0.88x |
| filter, column against column, 2e7 | 96.4 | 93.9 | 0.97x |
| filter, column against literal, 2e7 | 1034.9 | 324.0 | **0.31x** |

At two million rows the lane costs between 0.73x and 0.96x - far less than the
kernel's 2.1x, because the Arrow cache read, the batch machinery and the filter's
plumbing are most of the work and none of them doubles with the lane. The
predicted 0.45x to 0.60x band is met by no case at that scale.

At twenty million rows the two filters part company: column against column stays
at 0.97x while column against literal falls to 0.31x, *below* the predicted band
and below the kernel ratio. So the answer to "what does the long lane cost end to
end" is: it depends on the shape by a factor of three, and the plan that
predicted one band for it had the wrong model.

**What the engine is worth on these shapes**, against the row engine: filters
1.4x to 2.1x, projections 6.6x to 8.1x, and at twenty million rows the int32
column-against-literal filter reaches 11.1x. The `TIME` filter is 1.8x and the
day-time interval filter 1.4x, the lowest of the family.

### 9.1 Where the cost is, which is not where the prediction looked

The crossed experiment at twenty million rows, Varka arms:

| compared | output | rate |
|---|---|---:|
| int32 | int32 | 1101.9 |
| int64 | int64 | 327.8 |
| int64 | int32 | 103.0 |
| int32 | int64 | 99.0 |
| int64 | none (`count(*)`) | 128.0 |
| int32 | none (`count(*)`) | 132.6 |

Two things follow, and the plans say why. The fast pair are the two queries whose
executed plan is `VarkaFilterColumnarToRow (predicate)` with no forwarded column
- one column, filtered and output. Between them the only difference is the width,
and the long arm is **0.30x** the int one: more than the kernel's 2.1x can
explain, and consistent with the compaction the filter does on its surviving
column, where the vectorised `compress` path serves four-byte vectors only and an
eight-byte column takes a per-row copy (`VarkaKernelEvaluator`, the finding
`PLAN_TASK_29.md` 2 recorded as task 128's). That is the leading explanation and
this measurement does not prove it; what would is the same pair with
`compactInt64` in place, which is task 128.

The four slow cases all carry a second column. Their plans read
`VarkaFilterColumnarToRow (predicate), List(<the other column>)` - a forwarded
column to compact - and they collapse to about 100 M rows/s whichever width was
compared, a **tenfold** drop from the one-column int32 case. `count(*)`, which
outputs no column at all but adds an aggregate above the filter, sits at 130.
That cost is not the long lane's: it is paid by an int32 forwarded column too,
and it is much larger than the lane's. It is recorded as row 145.

### 9.2 What this changes for the milestone

* **A kernel ratio is not an end-to-end ratio**, in either direction: 2.1x at the
  kernel became 0.73x-0.96x at two million rows and 0.31x on one shape at twenty
  million. Tasks 102, 103 and 104 should quote this file rather than task 142's
  when they speak about queries.
* **Task 128 has its motivating number**: 0.30x on the shape where the compaction
  is the only difference.
* **Row 145** is new and larger than the lane question that found it.
