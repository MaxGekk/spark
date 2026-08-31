# Task 26: calendar extraction - `year`, `month`, `dayofmonth`, `quarter`

Milestone 4's vocabulary task, from its section 2.4 and task row 26 (scope
catalogue item 6): the one extraction family the headline corpus asks for.
TPC-H q7, q8 and q9 use `year(date)` and nothing else; TPC-DS pre-materialises
`d_year`, `d_moy`, `d_dom` and `d_qoy`, so the family appears there zero times.
Intuition overweights this item and the corpus says it is one function wide -
`month` and `dayofmonth` ride along because one civil-from-days decomposition
yields all three, and `quarter` because it is `(month + 2) / 3`.

Today `Year` declines at `VarkaExpressionCompiler`'s trailing `case other` and
the whole projection falls back to Janino.

This file is written before the work, in the shape `PLAN_TASK_23.md` used: the
exploration behind it settled the algorithm and produced numbers worth
recording whether or not the implementation follows immediately. The outcome
section is added when the work lands, and the predictions in section 8 are
scored there rather than edited away.

## 1. The admission check, done before any emitter work

The milestone requires this first, and it is the part of the task that could
have ended in a decline. `VectorOperators` has no multiply-high on any lane
type, so full-range Granlund-Montgomery division is inexpressible on int lanes;
only a range-narrowed magic works, where the value is shrunk until both
`v * e < 2^k` and `v * M < 2^31` hold in the low 32 bits `mul` returns. The
technique is task 14's follow-up (`PLAN_TASK_14.md` 7.7 and the `SKILLS.md`
entry it added) - `PLAN_MILESTONE_4.md` attributes it to task 17 in two places,
which is wrong, and this task corrects it there.

**The first finding is a bound, not a constant.** Worst-case `e ~ d` forces
`2^k > d * v` for correctness, hence `M ~ v`, hence `v * M < 2^31` gives
`v < 46341`. An exact magic multiply on int lanes exists only for dividends
under roughly 46000. The civil-from-days dividends are millions and 146096,
both far past it, so **146097 and 36524 admit no exact magic at any useful
range** - the milestone's named risk, realised.

**The second finding is that this does not decline the field.** With
`M = floor(2^k / d)` the quotient is never overestimated, the shortfall is
bounded inside a known input range, and each correction step is one `r >= d`
compare and two masked adjustments - on a remainder the algorithm wants anyway.
Probed at the widest admissible `k` for each division (dividend bound in
brackets):

| division | k | M | verdict |
|---|---|---|---|
| `w / 146097` [< 2^24] | 24 | 114 | round-down, 1 correction |
| `days / 146097` [full int32] | 15 | 14699 on `days >> 16` | round-down, 2 corrections |
| `doe / 36524` [146096] | 28 | 7349 | round-down, 1 correction |
| `doc / 365` [36523] | 24 | 45966 | exact |
| `(5*doy + 2) / 153` [1827] | 27 | 877241 | exact |
| `(153*mp + 2) / 5` [1685] | 22 | 838861 | exact |
| `(month + 2) / 3` [14] | 28 | 89478486 | exact |

The century-then-year split also retires 1461, the fourth constant the
milestone listed: dividing by 365 inside a century has a dividend of 36523,
which is under the bound, so that division is exact.

**Verified, not asserted.** The int32-faithful model of the narrowed lowering
was checked against an arbitrary-precision reference over all 16777216 days of
its range - zero mismatches, and no unintended int32 overflow - and that
reference was checked against the JDK calendar over all 3652059 days of
`0001-01-01 .. 9999-12-31`. For the total lowering the correction bound was
established by exhaustive search over all 65536 high halves at both low-half
endpoints, which is a proof because the error is monotone in the low half:
`0 <= q - q0 <= 2`. Both become committed tests (section 6); these planning
runs are what justify writing the algorithm down, not a substitute for them.

## 2. Decisions, and who made them

Settled with the owner during planning:

1. **Scope is `year`, `month`, `dayofmonth` and `quarter`.** The decomposition
   that yields the year computes the day of the year on the way, so the other
   three are a handful of ops and a test arm each, not extra algorithm work.
   `dayofyear` and date-level `date_trunc` stay out.
2. **Both range strategies ship as measured variants**, the way
   `VarkaEmitOptions.FloorMod7` keeps three mod-7 lowerings. `TOTAL` is correct
   for every int32 day and needs no runtime check; `NARROWED` is cheaper per row
   but valid only over a bounded day range, so it carries a guard and a
   batch-level fallback. The benchmark decides which becomes the default and the
   owner picks after seeing the numbers. Both stay differentially tested, so the
   loser is a live reference variant rather than dead code.

## 3. Design: the two lowerings

Everything from the day of era (`doe`, in `[0, 146096]`) onward is shared, and
is the exhaustively verified part. The variants differ only in how `era` and
`doe` are reached.

### The shared tail

```
cen  = (doe * 7349) >>> 28                  // century in era, round-down
r2   = doe - cen * 36524
c2   = r2 >= 36524 ; cen += c2 ; doc = r2 - (c2 ? 36524 : 0)
cen == 4 -> cen = 3, doc += 36524           // the era's last day spills
yoc  = (doc * 45966) >>> 24                 // exact; may overshoot by a year
doy  = doc - (365 * yoc + (yoc >>> 2))
doy < 0 -> doy += 365 + (yoc & 3 == 0), yoc -= 1
mp   = ((5 * doy + 2) * 877241) >>> 27      // exact
year    = 400 * era + 100 * cen + yoc + (mp >= 10)
month   = mp < 10 ? mp + 3 : mp - 9
day     = doy - (((153 * mp + 2) * 838861) >>> 22) + 1
quarter = ((month + 2) * 89478486) >>> 28
```

Every comparison is a `VectorMask` and every conditional a masked add or
subtract, so there is no branch in the lane path. The two overshoot fixes -
the era's spilling last day and the year the exact `/365` rounds past - are the
price of doing this with quotients instead of a table, and each is three ops.

### `NARROWED`: one division, one correction, and a guard

`BIAS = 719468 + 32 * 146097 = 5394572` is the shift to March-based years plus
32 eras, so the range reaches back past year zero without ever dividing a
negative number.

```
w    = days + 5394572                       // 0 <= w < 2^24 inside the range
era  = (w * 114) >>> 24
r    = w - era * 146097
c    = r >= 146097 ; era += c ; doe = r - (c ? 146097 : 0)
                                            // the -32 folds into `year`
```

Valid for days in `[-5394572, 11382643]`, which is years -12800 to 33134 -
strictly containing every date SQL can express, and reachable past by
`date_add`. Hence section 4.

### `TOTAL`: correct for every int32 day, no runtime check

The obstacle is that `days + 719468` overflows in the top 719468 days, so this
variant never forms it: the epoch shift is folded into the remainder, where it
costs one compare.

```
h    = (days >> 16) + 32768                 // 0 <= h < 2^16
q    = ((h * 14699) >>> 15) - 14700         // q <= floor(days / 146097) <= q + 2
r    = days - q * 146097                    // q * 146097 may wrap; r is exact
r >= 146097 -> q++, r -= 146097             // twice
t    = r + 135080                           // 719468 = 4 * 146097 + 135080
era  = q + 4 + (t >= 146097)
doe  = t - (t >= 146097 ? 146097 : 0)
```

The wrap in `q * 146097` is deliberate and safe: the true `days - q * 146097`
lies in `[0, 3 * 146097)`, so the low 32 bits carry it exactly. It reads like a
bug and therefore gets a comment saying why it is not, plus its own
`Int.MaxValue` boundary test.

Predicted emitted op counts for `year`: `NARROWED` about 40 plus about 4 for
the guard, `TOTAL` about 45. The other three fields add 3 to 9 ops each on top
of the shared tail.

## 4. The guard and the kernel status ABI

`NARROWED` needs to detect a day outside its range and decline the batch rather
than compute it wrongly, which is the ghost-fallback contract
(`sql/varka/AGENTS.md`): a Varka failure degrades to the row engine and never
fails a query.

Per calendar node, per lane group: two compares against the range constants,
OR'd together, ANDed with the row's validity mask - a null row's data bytes are
undefined and must not trip the guard - and OR'd into an accumulator mask, with
one `anyTrue` after the loop.

The status leaves the kernel through the one interface the generated class
implements:

```java
int run(long[] srcData, long[] srcValidity, int[] srcNullCount,
    long[] dstData, long[] dstValidity, int[] scalarArgs, int length);
```

Non-zero means "this batch's outputs are not valid; re-run it on the row
engine". Bit 0 is the calendar range. It is a bitmask rather than a boolean so
task 30's ANSI throw path can add its own bit instead of inventing a second
channel. Inside the generated class the body methods return `int` and the
driver ORs them; a kernel with no guard returns a constant zero, so `TOTAL` pays
nothing beyond the return.

`VarkaKernelEvaluator` reads the status in `invokeFused`: non-zero releases the
outputs the kernel just wrote and routes the batch down the existing non-kernel
path - the one an Arrow-unbacked batch already takes - counted by its own
metric so a silent fallback is visible rather than merely slow.
`VarkaFilterExec`'s mask path takes the same branch.

## 5. `GROUP_BUDGET` must learn that a node has weight

`groupOutputs` and `addOps` count IR nodes, so four calendar outputs read as
four ops against `GROUP_BUDGET = 16` and land in one loop method of roughly 180
vector ops - squarely in the C2 cliff the budget exists to avoid, where a 64-op
loop took a ten-second tier-4 compile and ran boxed at about 1% speed
meanwhile (`VarkaLoopEmitter` lines 167-192). `addOps` therefore gains a
per-node weight: 1 for every node today, and the emitted op count for the
calendar nodes, so each calendar output forms its own sibling method. The
budget's own documentation already blesses that shape - an output wider than
the budget forms its own group untouched, and single-output loops measured
healthy at 59 ops.

The consequence is stated up front rather than discovered: `SELECT year(d),
month(d)` computes the decomposition twice, once per method. That is the trade
task 17 measured and chose - recomputing ops in registers beat a wider method's
register pressure, 4.1 against 3.0 G rows/s - and the corpus asks for one field
at a time. It opens a debt register entry in `PLAN_MILESTONE_4.md` naming what
closing it would take, which is multi-value IR nodes.

## 6. Verification

The standing gates, at both vector widths.

```
build/sbt catalyst/Test/compile sql/Test/compile
build/sbt 'catalyst/testOnly *Varka*' 'sql/testOnly *Varka*'
JAVA_OPTS="-XX:MaxVectorSize=16" build/sbt 'catalyst/testOnly *Varka*'
JAVA_OPTS="-XX:MaxVectorSize=16" build/sbt 'sql/testOnly *Varka*'
build/sbt catalyst/doc
./build/mvn -f sql/varka/engine/pom.xml install
dev/lint-java && dev/scalastyle
```

Beyond "the suites are green":

* **The exhaustive sweeps**, committed as opt-in tests behind a system property
  the way the reference-variant tests are, with their results quoted in this
  file: `NARROWED` against the JDK calendar over all 16777216 days of its
  range, and `TOTAL` against a long-arithmetic reference over all 2^32 days -
  a vector kernel at 16 lanes makes that seconds rather than hours - with that
  reference itself checked against the JDK calendar over the SQL range.
* **The curated boundary set**, which the default suite runs every time: era
  and century edges, 400-year cycle boundaries, February 28 and 29, every
  month-length boundary, pre-1970, year 1, `Int.MinValue`, `Int.MaxValue`, and
  the narrowed range's edges plus one day past each.
* **The variants agree** on every shape the emitter suite drives, at both
  widths, inside the narrowed range.
* **The guard**: a differential where `date_add` pushes dates past the narrowed
  range under `NARROWED`, asserting the row engine's answers and that the
  fallback metric fired - and that the same metric stays zero on in-range data
  with nulls, which is what proves the validity mask is in the guard.
* **The pinned oracles move.** Both `everyNode` fixtures - the shape hash in
  `VarkaShapeCacheSuite` and the line map in `VarkaLoopEmitterSuite` - gain four
  node types. That is expected here, unlike task 24 where they had to hold, and
  the new literals are re-pinned with the reason recorded in the outcome
  section. `VarkaEmitOptions.canonical()` is empty for `DEFAULTS`, so adding a
  field does not move a production hash.
* **`year` compiling on the TPC-H shape**, per the milestone's validation
  column: a q7-shaped query in the differential suite, and `EXTRACT(YEAR FROM
  d)`, which desugars to the same `Year` node.

Numbers are regenerated in one run on an otherwise idle machine, five
iterations over two-second windows, any ratio under 1.3x re-run and compared by
minimums. The **catalyst parity harness is the instrument, not the engine JMH
one**: milestone 4's debt register records that the engine's `forks = 0` phase
measures a degraded JIT state and cannot be trusted for an A/B today.

## 7. The measurement that decides the default

A `year` section in `VarkaEmitterParityBenchmark`, shaped like the four-way
`dayofweek` section that already exists there: `TOTAL`, `NARROWED`, a scalar
JDK-calendar loop and the Janino row path, over 4096-row batches, at both
vector widths, plus the four-field projection so section 5's grouping change is
measured rather than assumed. The committed file is what the owner reads before
choosing the default, and the choice is recorded here with the numbers behind
it.

## 8. Predictions, registered before the measurements

1. `year` lands at 40 to 45 emitted vector ops, about 3.5x `dayofweek`'s magic
   lowering.
2. `TOTAL` costs 5 to 12% against `NARROWED` on in-range data - five ops on
   forty. That is inside the 1.3x rule, so it needs the interleaved A/B rerun
   compared by minimums before it is written down.
3. On `SELECT year(d)` the emitted kernel beats the Janino row path by 3 to 5x
   (`dayofweek` is 8.8x at a third the ops) and the scalar calendar loop by 15
   to 30x.
4. Splitting the calendar nodes into their own loop methods leaves single-field
   projections unchanged and keeps the four-field projection off the compile
   cliff; no committed number for an existing shape moves.
5. The two pinned literals move. Nothing else does.

## 9. Sequencing

Five commits, each green on its own:

1. **The admission check and the models**: the derivation, both scalar models,
   the exhaustive sweeps, and sections 1 to 3 of this file. No emitter change,
   so the case is on record before the code that rests on it.
2. **The IR, the compiler arms and the `TOTAL` emitter path**, with the
   `addOps` weight; the pinned literals re-pinned here. `TOTAL` goes first
   because it needs no plumbing, so the calendar arithmetic is green before the
   ABI moves.
3. **The kernel status ABI and the guard**: `int run`, `NARROWED` behind
   `VarkaEmitOptions`, the evaluator fallback, the metric, the out-of-range
   differential.
4. **The measurement**: the parity benchmark section and one regeneration run.
5. **The default and the docs**: the owner's choice recorded here,
   `docs/sql-varka.md` and `README.md` requoted from that one run, the
   milestone's task-26 row marked done, its debt register entry opened, and its
   task-17 attribution corrected.

## 10. Risks, ranked

1. **A correction step that is not enough.** Both variants rest on a bounded
   shortfall. It is proven by exhaustive sweep, not by the inequality alone,
   which is why the sweeps are commit 1 rather than commit 4.
2. **The deliberate wrap in `TOTAL`.** `q * 146097` overflows int32 near the
   extremes by design. A future reader who "fixes" it breaks the top of the
   range silently - hence the comment and the `Int.MaxValue` boundary test.
3. **Null rows tripping the guard.** Data under a null row is undefined, which
   is why the guard mask is ANDed with validity. A miss here is a silent
   full-batch fallback - slow, not wrong - so the differential asserts the
   metric is zero on in-range data with nulls.
4. **The ABI change reaching further than expected.** `run` is implemented by
   generated classes and called from three places, but the emitter suites and
   the parity benchmark construct kernels directly and move with it.
5. **The compile cliff.** Four calendar outputs in one method is the failure
   mode section 5 exists to prevent, so the four-field shape is in the
   benchmark.
6. **Numbers moving under the task's own feet.** Commits 2 to 4 all touch
   emitted bytes; regenerate once, in commit 4.

## 11. Explicitly out of task 26

* **`dayofyear`, date-level `date_trunc`, `last_day`, `next_day`.** The algebra
  yields them and the corpus does not ask; they enter with their own argument
  the way `IN` and `Coalesce` entered milestone 3.
* **A `DIV` reference variant** of the decomposition, the way `FloorMod7` keeps
  one. Two independently derived variants already check each other, and the
  exhaustive sweep against the JDK calendar is a stronger oracle than a third
  lowering would be.
* **Multi-value IR nodes** so several calendar fields share one decomposition -
  the debt register entry from section 5, not this task's change.
* **`year(timestamp)`.** The analyzer inserts `Cast(TimestampType, DateType)`,
  which the compiler declines today; it enters with task 29's int64 lanes.
* **A hand-written `DateVectorOps` kernel.** `dayofweek` set the precedent that
  a lowering introducing no new lane type lives only in the emitter.
