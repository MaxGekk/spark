# Varka Milestone 5 Plan: the other lanes

Milestone 4 opened as *breadth* - the engine learning the types, expressions
and loop schedules a query contains - and grew, task by task, into the date
family and the emitter under it. On 4 September 2026 the owner re-scoped it to
exactly that: milestone 4 is `DateType` plus the emitter and evaluator
infrastructure, and every task whose subject is another lane or output type
moved here, unchanged. This file is those tasks. It is a task plan, not a scope
catalogue: each task below was designed in milestone 4's plan, several with
measurements already committed, and they keep the numbers they were given
there. The coverage milestone that used to be called milestone 5 - decimals,
strings as keys, grouped aggregation, published benchmark numbers - is now
milestone 6, and its scope document is `SCOPE_MILESTONE_6.md`.

## 1. What moved, and why this order

Six tasks, in the dependency order milestone 4's plan already gave them:

* **27, boolean outputs** - comparisons and connectives as projection results.
  Independent of the rest; the one pure continuation of milestone 3's mask
  machinery. Its `toVector`-against-`blend` measurement is committed.
* **28, lane-width conversion** - the width machinery 29, 30 and 39 lean on.
  Its loop-shape measurement (narrowest-drive against part loops) is
  committed and decided: narrowest-drive.
* **29, int64 lanes** - `TimestampNTZ`, `bigint`, `LongType` and `TimestampType`
  comparisons and differences: the second `LaneType`.
* **30, ANSI-correct integer arithmetic** - `try_*` first, the throw path
  second, `Multiply` overflow through 28's widening; the blend-then-`DIV`
  mechanism for the masked epilogue's zero-safety invariant is pre-measured.
* **39, `date - date`** - the first mixed-width kernel, an int32 input pair and
  an int64 output; depends on 28 and 29. Its recipe (`PLAN_TASK_39.md`) was
  written against machinery that does not exist yet and says so.
* **49, exact civil-from-days in long lanes** - depends on 29; its admission
  check is committed (`verify_long_lane_magic.py`) and its gate registered.
* **65, Joffe's `fast32` civil-from-days in int lanes** - added 5 September
  2026 (section 2.7): the int32-lane alternative to 49, admitted or declined
  by a sweep before any emitter change. Independent of 29.

What stays true from milestone 4's plan and is not repeated here: the three
invariants (one lane width per kernel, every value lane-shaped, no lane reads
its neighbour), the standing gates in `PLAN_MILESTONE_4.md` section 5, and the
debt register there, which these tasks keep sweeping. Cross-references inside
the moved text were repointed: "2.5's committed results file" now reads 2.1,
the catalogue's "see 2.x" pointers follow the new section numbers, and the
old milestone 5 is named milestone 6 where the text meant the coverage
milestone.

Section 2 carries the six design sections as milestone 4 wrote them, section 3
the task table, sections 4 to 8 what milestone 4's files, verification, risks,
open questions and exclusions said about these tasks, and section 9 the scope
catalogue items about other lanes, item numbers preserved because other plans
cite them.

## 2. Design

### 2.1 Boolean outputs (task 27, item 5)

*Moved from `PLAN_MILESTONE_4.md` section 2.5 on 4 September 2026, text
unchanged except for the cross-references noted in section 1.*

The cheapest item and the only pure continuation of milestone 3: comparisons
and `And`/`Or`/`Not` as projection *results*, built on task 21's mask-as-value
machinery. `VectorMask.toVector` against a `blend` of one and zero was
pre-registered as a measurement, not a debate, and it is now measured
(`VarkaMilestone4MeasurementsBenchmark`, committed forked-JVM run in
`sql/varka/engine/benchmarks/VarkaMilestone4MeasurementsBenchmark-jdk25-results.txt`,
which superseded four in-process runs; their reading is in git history and
the file lists what moved): `toVector` is ahead by 1.12x at AVX-512 and
`blend` by 1.04x at 128-bit - a small, width-dependent gap where the
in-process runs had reported a tie, and not the clear winner the
pre-registration expected either way. The real,
width-dependent finding is a different question the pre-registration did not
ask: whether to materialize an int column at all. Skipping it - packing
`VectorMask.toLong()` straight into the output bitmap - wins by 1.18x at
AVX-512 but *loses* by 1.34x-1.39x at 128-bit. A compound predicate, `(a > b)
AND (c < d)` kept in mask space the whole way through versus materialized as
an int column at every node, shows the same direction with a smaller margin
than the in-process runs claimed: mask-space is ahead at both widths, by 1.02x
at AVX-512 and 1.07x at 128-bit - never worse; the 1.24x-1.37x the earlier
runs reported at 128-bit was the harness. Two consequences for the task: walk
boolean sub-expressions in mask space and materialize only once at the output
boundary (never worse, at either width), and the single-comparison bits-only
shortcut needs a width check rather than a single
committed choice, since its sign flips between the two vector widths this
project already tests at. The two real questions the pre-registration also
named are format and nulls: Spark's bit-packed boolean vector against
Arrow's validity-style bitmap at the output boundary, and the three-valued
rules holding there exactly as they hold in the interior - a null input
produces a null output, never a false one. The differential runs every null
pattern for exactly that reason.

### 2.2 Lane-width conversion (task 28, item 1)

*Moved from `PLAN_MILESTONE_4.md` section 2.6 on 4 September 2026, text
unchanged except for the cross-references noted in section 1.*

The width machinery items 2 and 4 lean on. The hard part is not the
conversion, it is the lane count: at one shape an int32 species holds twice
the lanes of an int64 species, so a mixed-width kernel either drives the loop
at the narrowest lane count and leaves wide lanes half empty, or emits a part
loop per conversion and carries two trip counts. That is the one decision in
this item that is expensive to reverse, so the scope's open question 2 was
pre-registered as a measurement before the task opens: both shapes on a
`cast(int AS long) + long` chain. Measured
(`VarkaMilestone4MeasurementsBenchmark`, same committed results file as 2.1):
narrowest-drive and part-loop are statistically tied at both vector widths,
on every run - four total - narrowest-drive slightly ahead most of the time
(within 1.01x-1.07x, inside this file's own noise band). Part-loop's extra
bookkeeping - two trip counts, two stores per int chunk - buys nothing
measured, so task 28 opens already knowing the winner: narrowest-drive, for
the simpler build (one trip count) at the same throughput. The recorded
fallback if a wider mixed-type shape measures differently once task 28 is
under way: items 2 and the multiply half of 4 can be built width-locked and
retrofitted.

### 2.3 int64 lanes: `TimestampNTZ` and `bigint` (task 29, item 2)

*Moved from `PLAN_MILESTONE_4.md` section 2.7 on 4 September 2026, text
unchanged except for the cross-references noted in section 1.*

The first new lane type, and the natural one: the only type whose semantics
are already written down (milestone 2 section 2.6 quality, for dates) and
whose expressions Varka already compiles at another width. `TimestampNTZType`
is pure int64 microseconds; comparisons, differences and literal arithmetic
come with it, plus comparisons and diffs on `TimestampType` and `LongType`
columns generally. Zoned day and month arithmetic stays out until its
semantics are written down with the same care - the tzdata-as-interval-arrays
technique is recorded in the catalogue for that day.

`LongVector` halves the lanes, so every parity gate reruns at both widths and
the same expression has roughly half the headroom it had at int32 - a number
to commit, not a surprise to discover. Micros-to-second and second-to-day are
divisions by invariant constants (1000000, 86400); there is no multiply-high
on long lanes, so the range-narrowed magic multiply is the first thing to try
(the parity file prices `DIV` at roughly an eighth of the magic rate, 652
against 5657 M rows/s on the `dayofweek` case). This task also lands the field
differential mode task 22 explicitly left to it, because this is where the
correctness surface widens.

### 2.4 ANSI-correct integer arithmetic (task 30, item 4)

*Narrowed on 4 September 2026:* the int32 add, subtract, multiply and negate, in both
evaluation modes and the `try_*` forms, are milestone 4's task 63 (`PLAN_MILESTONE_4.md`
2.30); this section's design carries over to it, and what remains here is division,
remainder and the int64 forms.

*Moved from `PLAN_MILESTONE_4.md` section 2.8 on 4 September 2026, text
unchanged except for the cross-references noted in section 1.*

Most arithmetic in most queries, and the `datediff(d2, d1) + 1` shape that
keeps appearing in date work. The order inside the task is the risk order:

* **`try_add`, `try_subtract`, `try_multiply` first.** They want nulls, not
  throws: the wrap-versus-saturate difference mask *is* the output validity,
  no branch needed. If the ANSI path prices badly, `try_*` alone still ships.
* **The ANSI throw path second**: compute the wrapping op and the saturating
  op over the same inputs, `compare(NE, ..).anyTrue()` decides whether to
  leave the vector loop, and a scalar re-walk of the offending lane group
  raises the error against the right row - the ghost-fallback discipline the
  project already runs on. On the no-overflow path that is one vector op and
  one well-predicted branch, and the prediction to register is that this
  prices acceptably.
* **`Multiply` overflow rides task 28's widening** - there is no saturating
  multiply, so detection widens to long lanes and compares against the
  narrowed result. It lands only if 28's machinery makes it cheap.

`date_add` stays exempt: it wraps by spec. The validation is a kind of
assertion the suites have never made: an error-*identity* differential - the
same `SparkException` as the row engine, attributed to the same row.

One obligation task 24 left at this task's door, sharpened by its review: the
masked epilogue's invariant that **no operation in the walk may trap on `0`**
(inactive lanes read `0` from a masked load) currently lives only in the
emitter's class doc, and division is the first node that will violate it. This
task must not just remember the paragraph - it should make the invariant
structural when the first trapping node lands: an explicit zero-safety member on
the sealed `VarkaVectorIR` (no default), so a node that can trap does not
compile until the epilogue emitter blends a safe divisor or takes the masked
lanewise form. A prose invariant fails only on unaligned batch lengths, which
task 24 measured as the lengths no committed harness ever runs.

Which of those two mechanisms the enforcement should reach for is
pre-measured (`VarkaMilestone4MeasurementsBenchmark`, same committed results
file as 2.1): blend-then-`DIV` beats masked `DIV` at both vector widths, on
every run - four total - 1.08x-1.10x at AVX-512, 1.18x-1.19x at 128-bit by
minimum. The smallest margin of the five measurements in that file, but the
only one where all eight data points (two widths times four runs) agree in
both direction and rough magnitude, which is the interleaved comparison the
under-1.3x rule asks for. Blend a safe divisor into inactive lanes; the
structural check exists to make sure some such mechanism runs before an
unmasked `DIV`, not to leave the choice open each time.

### 2.5 `date - date`, the first mixed-width kernel (task 39)

*Moved from `PLAN_MILESTONE_4.md` section 2.13 on 4 September 2026, text
unchanged except for the cross-references noted in section 1.*

The natural first consumer of tasks 28 and 29, and a better one than the
synthetic `cast(int AS long) + long` chain their measurement used: int32 inputs,
an int64 output, exactly one width conversion, one output, and an error path.
The smallest real expression with that shape.

It is not `datediff`, which Varka already compiles and which returns an
`IntegerType` day count. Since Spark 3.2 the `-` operator between two dates
returns `DayTimeIntervalType(DAY)` - physically **long microseconds** - as
`Math.multiplyExact(Math.subtractExact(l, r), MICROS_PER_DAY)`. Two facts about
that line shape the task: it throws unconditionally, not only under ANSI, since
`SubtractDates` carries no `failOnError`; and the legacy
`CalendarIntervalType` variant behind `spark.sql.legacy.interval.enabled` is a
different result type that must decline.

**The finding that made this worth writing down now is that it does not need
task 30.** A lane cannot throw, but it does not have to: task 26 built the
channel where a kernel notices what it cannot compute, returns a status, and
the row engine recomputes the batch - and the row engine then raises the
identical exception at the identical row, because it *is* the row engine. Both
overflow tests are cheap and branchless (`((l ^ r) & (l ^ diff)) < 0` for the
subtraction, and a comparison against `Long.MAX_VALUE / MICROS_PER_DAY =
106751991` for the multiply), and overflow needs a date range of 292,000 years,
so the fallback costs nothing anyone will measure. Task 30 exists for
expressions where declining is too expensive; this is not one, and the recipe
says so rather than reaching for machinery because it is there.

The recipe is the first written **against machinery that does not exist yet**,
so it names tasks 28's and 29's plumbing provisionally and tells the executing
agent to stop and report if the real thing differs rather than adapt on the
fly. The gap between what it assumed and what 28 and 29 actually build is the
most useful thing its outcome section can record - and it is a cheap trial of
whether a recipe can usefully be written ahead of its dependencies at all.

### 2.6 Exact civil-from-days in long lanes (task 49)

*Moved from `PLAN_MILESTONE_4.md` section 2.19 on 4 September 2026, text
unchanged except for the cross-references noted in section 1.*

Task 26's whole design rests on one absence: `VectorOperators` has no
multiply-high on any lane type, so a full-range Granlund-Montgomery magic
division is not expressible on int lanes, and what ships instead is a
*range-narrowed* round-down magic with correction carries, a narrow-range guard,
a batch-decline path and a `VarkaChrono` constant table to support it. That
absence was re-checked during task 32 and is not temporary: no `MUL_HIGH` in
JDK 25 or in openjdk/jdk master, and JDK-8219881, the nearest request, has been
Open at P4 since February 2019 on `repo-panama` (`SKILLS.md` has the detail).

**But multiply-high was never the only route to an exact magic. A 64-bit low
product is enough, and `LongVector`'s `MUL` provides one today.** Widen the
dividend to int64 lanes and the product of a 32-bit value and a ~30-bit magic
lands well inside a signed 64-bit lane, so the quotient is exact with a single
multiply and a shift - no round-down, no carries, no range restriction.

Checked, over the range the lowering actually needs rather than a round number.
Days are int32 and the March-based bias makes the dividend
`w = days + 2^31 + 719468`, so `w` spans `[0, 2^32 + 719468)`:

| division | dividend range | k | M | largest product |
|---|---|---|---|---|
| `/146097` | `[0, 2^32 + 719468)` | 47 | 963315389 | 2^61 |
| `/36524` | `[0, 2^24)` | 38 | 7525953 | 2^46 |
| `/365` | `[0, 2^24)` | 31 | 5883517 | 2^46 |

Three bits of headroom on the widest one, and none to spare beyond it: the same
search over `[0, 2^33)` finds no exact pair at all. So the margin is real but
thin, and the admission check is not a formality.

That table is reproducible rather than asserted:
`sql/varka/plans/verify_long_lane_magic.py` searches for each pair, checks it at
every multiple-of-`d` boundary in range - which is where an inexact magic must
first disagree, the error being monotone between them - and fails loudly if the
`[0, 2^33)` search unexpectedly succeeds, since that would mean this section
understates the headroom. It is committed for the same reason
`verify_chrono_tails.py` and `verify_days_from_civil.py` are.

**What it deletes.** The narrow-range guard and its two compares; both
round-down magics and their correction carries; `STATUS_CHRONO_RANGE` as a
reason a chrono batch declines, with the evaluator fallback and metric that
serve it; the `NARROWED` variant and the range constants in `VarkaChrono`; and
the standing caveat that `year(date_add(d, n))` can decline for a large enough
`n`. The status ABI itself stays - task 30's ANSI path wants its own bit - but
the calendar family stops being a reason a batch is recomputed on the row
engine.

**Update: the guard half of this is already gone (task 51).** Before this task
was picked up, the owner had the emitter's per-extraction guard removed for a
different reason - it re-verified a fact CSE and task 32's fragment sharing had
usually already established, on every calendar node, when the one case that
actually needs a fresh check is a value a *producer* node manufactured from
unbounded runtime arithmetic (`date_add`/`date_sub` with a column offset, not a
literal). `PLAN_TASK_51.md` and `PLAN_TASK_52.md` have the detail; task 52 is
where the check returns, at the producer, not the extraction. So by the time
task 49 is picked up, `emitEra` no longer carries the two compares or the
`s.guardAcc` wiring, `hasChrono` is gone, and `STATUS_CHRONO_RANGE` already goes
unset - what remains for *this* task to delete is the round-down magics and
their carries, the `NARROWED` variant, and `VarkaChrono`'s range constants,
plus reconciling with whatever task 52 has done to the producer nodes by then
(an exact lowering needs no range check for the calendar extraction itself, but
task 52's producer-side check is about the query's arithmetic, not the
extraction, and stays relevant regardless of which lowering reads its output).

**What it costs.** Half the lanes: eight per vector at AVX-512 instead of
sixteen, four instead of eight at 128-bit. Plus an `I2L` on the way in and an
`L2I` per output on the way out. Counting ops out of what `emitChronoPrefix`
would become, this is roughly 25-28 ops over eight lanes against today's ~45
over sixteen - about 3.2 against 2.8 ops per row before conversions - so the
honest expectation is a **small throughput loss bought with a large
simplification**, not a win. That is a legitimate trade and it is the owner's
call, but it has to be made on a number.

**Sequencing.** Depends on task 29, which brings int64 lanes and the second
`LaneType`; there is no cheap way to prototype this before it lands, and no
reason to try. It is also an *alternative* to task 32's step B rather than a
complement: the fragment mechanism is lane-type agnostic and would compose
mechanically, but the two wins overlap, since a long-lane prefix is a different
prefix to share. Whichever lands second inherits the smaller half, and the
milestone should not pretend otherwise.

**The gate, and it is the strict one.** Task 26 verified its narrowed lowering
against `LocalDate` over all 16,777,216 days of its range and its total variant
against a long-arithmetic reference over **all 2^32 days**, as an opt-in
committed test, on the grounds that a vector kernel at sixteen lanes makes that
seconds rather than hours. This lowering claims exactness over a wider range
than either, on a three-bit margin, so it inherits that standard and not a
smaller one: the sweep is commit 1, before any emitter change, and the
boundary set gains `2^31 - 1`, `-2^31`, and both ends of the biased dividend.

**Predictions, registered here.** The lowering lands at 25-30 emitted ops; it
runs 0.75x to 1.0x the shipped narrowed lowering on `year` at AVX-512 and
relatively better at 128-bit, where halving an already-small lane count costs
less than the corrections it removes; and no committed number for a non-calendar
shape moves. If it clears 1.0x anywhere, that is a surprise worth writing down
rather than a result to assume.

**Declined if** the sweep finds any day where the exact form disagrees, or the
measured cost at AVX-512 is worse than 0.75x - at which point the simplification
is not worth a quarter of the calendar family's throughput, and the entry goes
to the debt register with the number attached.

### 2.7 Joffe's `fast32` civil-from-days in int lanes (task 65)

*Added 5 September 2026, from a reading of the Habr translation of Ben Joffe's
"fast-date-64" post and of the `benjoffe_fast32_v2.hpp` (2026) and
`benjoffe_fast32_v1_wide.hpp` files in `benjoffe/fast-date-benchmarks`, on the
owner's request. The same repository was read in September 2026 for task 54
(`SKILLS.md`, "The Julian map"); what follows is what that review did not
cover, because the `fast32_v2` file postdates it.*

**What the prefix already took from this source.** The Julian map (task 54,
+25% on `year` at both widths) and the month numerator whose low half is the
day (task 53). What the earlier review set aside was the rest of `fast64`: it
reads the *fractional* part of the year division - the low word of a 64x64
product - as the year-part, and folds the leap day into `(yrs % 4) * 512`, so
the month/day split never computes a day of year at all. That is four
multiplies for the whole date against Neri-Schneider's seven, and the review
filed it under task 49's long lanes because every multiply reads a high half.

**What is new: `fast32_v2`.** Joffe's own 32-bit rewrite of the same chain
("based on the 64-bit algorithm, but using smaller constants throughout,
avoiding umulh"), backwards-counting, with the year-part read off the low word
of a 32x32 product and the month/day split as `m_num = (yrs & 3) * 64 + shift +
ypt`, `month = m_num >> 8`, `day = ((m_num & 255) * DAY_MUL) >>> 32`. His
option A is exact from -284,449-07-13 to +284,449-01-30, wider than the
narrowed prefix's range by an order of magnitude; the scalar measurement puts
it at 1.18-1.38x Neri-Schneider's time against `fast64`'s 1.00x, on three
machines. The `fast32_v1_wide` file is the bucket technique the task 54 review
already recorded as the guard-free fallback (full int32 range, 100% overflow
safe, at more ops).

**Why it is not a port.** Each of his multiplies is still a 32x32->64 product
read from the high half (`>> 47`, `>> 32`): scalar-friendly, but the Vector
API has no multiply-high on any lane, which is the absence task 49 works
around by halving the lanes. So the transfer to int lanes is what tasks 53 and
54 did by hand - re-derive each stage as a low-32-bit magic with its own exact
range - and it is not obvious that every stage survives it: the year-part is
*defined* as a high-half fraction, and the `(yrs & 3)` absorption of the leap
day depends on the year-part's scale. The two ideas that transfer without that
question are the backwards count (no `+ 3` alignment terms and one subtraction
off the critical path) and the split's shape, in which the month and the day
come out of one add and one shift.

**Why it may be worth it.** The prefix is latency-bound on its dependent chain
(task 54's lesson: count stages, not ops), and this chain is one stage shorter
than the prefix's - no day of year before the month/day split - with one fewer
correction. Registered expectation: 5-15% on the prefix at both widths if the
low-product derivation holds over at least the narrowed range, and a wider
covered range as the second prize, which would shrink what task 52's producer
guard has to protect. Against that: the numerator of task 53 already gives
month and day from one multiply, so part of the gain may already be banked.

**The admission check, before any emitter change.** As for task 49, a sweep
first, committed as a script beside `verify_long_lane_magic.py`:

1. Transcribe the two files' algorithm text and constants into
   `sql/varka/papers` under the BSL-1.0 notice they carry, with the reading
   notes; they are code, not a paper, so the notes are the load-bearing part.
2. Derive, for each stage, a low-32-bit magic (round-down plus at most one
   carry, as `emitChronoPrefix` does today) and its exact range, and sweep the
   whole chain against `LocalDate` over the union of the derived ranges. The
   gate is the narrowed range at minimum
   (`VarkaChrono.NARROW_MIN_DAYS..NARROW_MAX_DAYS`); a wider exact range is
   recorded, a narrower one declines the task.
3. Count the dependent stages of the surviving chain against the prefix's.
   If it is not shorter, decline: the op count alone did not predict task 54.

**If admitted:** a `VarkaEmitOptions` variant and an A/B in
`VarkaEmitterParityBenchmark` beside the task 53 and 54 pairs, at both widths;
the register and the `HugeMethodLimit` ladder re-pinned, since every prefix
change moves them; the default chosen from the committed numbers.

**Relation to task 49.** An alternative, not a complement, in the same sense
2.6 gives for task 32's step B: both shorten the prefix, and whichever lands
second inherits the smaller half. This one needs no int64 lanes and can run
before task 29; if it admits and measures well, task 49's own expectation
(0.75x-1.0x) gets harder to justify on throughput and stands on the
simplification alone.

**Declined if** step 2's exact range is narrower than today's, or step 3 finds
no shorter chain, or the A/B is under 1.0x at either width; the numbers go to
the debt register either way.

## 3. Task breakdown

The rows as milestone 4's table carried them, task numbers unchanged. 28 opens
the milestone; 29 and 30 follow it; 39 and 49 wait on 29; 27 can run at any
point; 65 waits on nothing and is an admission check before it is a task.

| # | Task | Deliverables | Validation |
|---|---|---|---|
| 27 | Boolean outputs | Mask-to-column materialisation (`toVector` against `blend`, measured); the bit-packed format decision at the Spark/Arrow boundary; three-valued rules holding at the output boundary | Differential over every null pattern - a null input never becomes false; `SELECT d > DATE '2000-01-01' AS flag` and filter-leftover boolean columns compile; committed number on one boolean-output shape |
| 28 | Lane-width conversion | The mixed-width loop-shape measurement (open question 2: narrowest-drive against part loops) on `cast(int AS long) + long`, committed before integration; `convert`/`convertShape` emission following the winner; numeric `Cast` and Catalyst's implicit promotions over the supported types | Differential on mixed int32/int64 trees at both widths; the loop-shape decision recorded with its numbers; no regression on single-width shapes |
| 29 | int64 lanes: `TimestampNTZ`, `bigint` | The second `LaneType`; `TimestampNTZ` comparisons, differences, literal arithmetic; `TimestampType` and `LongType` comparisons and diffs; range-narrowed magic constants for 1000000 and 86400 or a recorded decline; the field differential mode from task 22 | Every parity gate re-run at the long species and both vector widths; the halved-headroom number committed rather than discovered; zoned operations demonstrably declined, not wrong |
| 30 | ANSI integer arithmetic - **narrowed on 4 September 2026**: the int32 add, subtract, multiply and negate over fused fields, int columns and literals, with the ANSI overflow decline and the `try_*` validity form, moved into milestone 4 as task 63 (`PLAN_MILESTONE_4.md` 2.30); what stays here is the rest | `/` (a double), `div` (task 29's long lane), `%` and `pmod` with the divide-by-zero rule, the int64 forms, and `Multiply` overflow through 28's widening where task 63's saturating check is not enough | The error-identity differential: same `SparkException`, same row, as the row engine under ANSI; `try_*` differential over overflow-dense and overflow-free data; committed number on the no-overflow path against Janino |
| 39 | `date - date`. **Planned** (`PLAN_TASK_39.md`), blocked on tasks 28 and 29 | The node, the int32-to-int64 conversion, the eight-byte output, and both overflow tests routed through task 26's decline channel rather than task 30's throw path; the legacy `CalendarInterval` variant declining. The int-to-long step is the two-part `convertShape` from the preferred int species, never a load through a half-width int species: two species of one lane type in one JVM turn the shared `IntVector` templates bimorphic and C2 keeps a heap box per loop iteration (`SKILLS.md`, "Every operator the plans rely on"), and the lane-width "tie" in `VarkaMilestone4MeasurementsBenchmark-jdk25-results.txt` was measured in exactly such a JVM | The overflow boundary exact in both directions (106751991 succeeds, 106751992 declines); Varka's exception identical to the row engine's, compared by running both; `datediff` unaffected; green at both widths, where an int64 lane holds a different number of rows |
| 49 | Exact civil-from-days in long lanes. **Planned in section 2.19** (PR #69; there is no `PLAN_TASK_49.md`), blocked on task 29 | The admission check first, over all 2^32 days against a long-arithmetic reference: exact magic division with a 64-bit low product and no correction carries, run for **both** decompositions - the three-division era/century/year form (146097, 36524, 365) and task 54's two-division Julian map (146097 on `4 * d + 3`, then 1461), which Ben Joffe's `fast64` shows reaching four multiplies for the whole date where Neri-Schneider needs seven; then the lowering, and the guard, the decline path, the `NARROWED` variant and `VarkaChrono`'s range constants removed with it. Verified before starting (`SKILLS.md`, "Every operator the plans rely on"): `LongVector.mul` by a constant compiles to one `vpmullq` on this CPU (AVX-512DQ with VL), not the three-multiply emulation plain AVX2 gets, and unsigned long compares are one `vpcmpuq` into a k-mask. Plan B if the 0.75x gate fails: Joffe's bucket technique for a guard-free int-lane total - `bucket = (d + 2^31) >>> 20`, reduce by `bucket * 1022679`, add `bucket * 2800` to the year - about 14 ops against task 26's `TOTAL` at 16 and without the deliberate wrap; his `article_2_l1` variant replaces two of those multiplies with an eight-entry offset table, one lane permute on a 256-bit int species | The exhaustive sweep as a committed opt-in test, at both widths; the parity `year` case measured against the shipped narrowed lowering in one run; declined on the record if the sweep disagrees anywhere or AVX-512 costs more than 0.75x |
| 65 | Joffe's `fast32` civil-from-days in int lanes. **Scoped in section 2.7** (5 September 2026); independent of 29 | The admission check first: the two source files transcribed into `sql/varka/papers` with reading notes; a committed script deriving a low-32-bit magic and its exact range per stage and sweeping the chain against `LocalDate`; the dependent-stage count against the prefix's. If admitted, an emit-option variant, the A/B beside the task 53 and 54 pairs at both widths, the register and the `HugeMethodLimit` ladder re-pinned, and the default chosen from the numbers | Exact over at least the narrowed range, or declined; a shorter dependent chain than the prefix's, or declined; the A/B at or above 1.0x at both widths, or the numbers go to the debt register |

## 4. Files

From milestone 4's section 4, the parts that belong to these tasks:
`VarkaVectorIR` (the second `LaneType`, conversion nodes, the boolean output),
`VarkaLoopEmitter` (conversions, the overflow detectors, the zero-safety
member the first trapping node makes structural), `VarkaExpressionCompiler`
(casts, arithmetic, boolean roots), `VarkaShapeCacheImpl` only if the key
vocabulary grows; in `sql/core`, the evaluators (int64 buffers, boolean output
vectors) and `VarkaColumnarRule` (new eligible roots); in the engine module,
hand-written reference kernels only where a parity anchor is needed for a new
lane type, per the reference-code commenting rule.

## 5. Verification

Milestone 4's standing gates, inherited whole, plus the two this milestone
adds:

* Differential against the row engine over every new shape, null patterns
  included, at the preferred width and `-XX:MaxVectorSize=16` - now at every
  lane width this milestone adds, not just every vector width.
* **The error-identity differential** (task 30): the same `SparkException`
  attributed to the same row, which the suites have never had to assert
  before.
* The byte-exact oracle still holds everywhere this milestone goes; it stops
  being universal only when item 3's doubles enter, which is why item 3's
  oracle decision (section 7) is taken early even though the item is
  deferred.

## 6. Risks

* **The mixed-width decision is expensive to reverse.** Task 28's loop-shape
  choice is baked into the emitter; that is why it was measured first
  (narrowest-drive won) and why width-locked retrofit is the recorded
  fallback.
* **Half the lanes.** Every int64 shape has roughly half the headroom of its
  int32 sibling; task 29 commits that number rather than discovering it.
* **A recipe written ahead of its machinery.** Task 39's recipe names 28's and
  29's plumbing provisionally; its outcome section is where the gap between
  assumption and reality gets recorded, and the executing agent stops rather
  than adapts when the real thing differs.

## 7. Open questions

From milestone 4's section 7, the two owned by these tasks:

1. **The ULP oracle** (item 3): a reading task - what accuracy Spark promises
   for `exp`, `log`, `pow` and the trig family, and what bound a vector
   differential asserts. Cheap, the item's gating decision, and what lets item
   3 be argued back in without a design pause. Recorded in this file when
   settled.
2. **Mixed-width loop shape**: measured before task 28 opens (2.2);
   narrowest-drive, unless a wider mixed-type shape measures differently once
   28 is under way.

## 8. Explicitly out of milestone 5

* **Item 3, float and double lanes** - the taxi benchmark's item; re-enters
  whenever that target is argued for, with its catalogue entry intact below.
* **Items 7 to 10** - aggregation, string functions, string keys, cross-lane
  movement: the follow-on ladder, carried in the catalogue below with full
  design input; each enters only with its own argument, and the aggregate
  wiring milestone 6 depends on is item 7.
* **`DecimalType`** - per milestone 4's item 12; its design pass is
  `SCOPE_MILESTONE_6.md` items 1 and 2.
* **Zoned timestamp arithmetic** - stays out until its semantics are written
  down; item 2 below keeps the tzdata-as-interval-arrays design for that day.

## 9. Scope catalogue

Milestone 4's pre-plan catalogue items about other lanes and about the
follow-on ladder, item numbers preserved because `SCOPE_MILESTONE_6.md`,
`PLAN_TASK_21.md` and `SKILLS.md` cite them. Items 6, 11, 12 and 13 stay in
`PLAN_MILESTONE_4.md` section 10.

### Item 1. Lane-width conversion, and mixed-type expression trees

Adopted as task 28 (see 2.2). The design input carried over whole: the hard
part is the lane count, not the conversion; `convertShape(I2L, longSpecies,
part)` yields one long vector per part with `partLimit` parts; the
narrowest-drive-versus-part-loop choice is measured before either is built in;
Spark's narrowing `Cast` throws under ANSI and wraps without it, tying this to
item 4.

### Item 2. int64 lanes: `TimestampNTZ`, `bigint`, and the second lane width

Adopted as task 29 (see 2.3). Kept for the zoned day when it comes: pack the
IANA tzdata transitions into flat `long[]` interval arrays and resolve a
vector of timestamps against them with a SIMD binary search, rather than
per-row `ZoneRules` lookups.

What a production instance of that design looks like, from
`NVIDIA/spark-rapids-jni` (`datetime_utils.cuh`, `timezones.cu`, read
September 2026), so the zoned task starts from its corners rather than
rediscovering them:

* **Two sorted arrays per zone, not one.** Converting *from* UTC searches the
  UTC instants; converting *to* UTC searches the local instants, because the
  same transition sits at different positions on the two axes. Each entry
  carries both instants and the offset after it.
* **The table is finite and the rules take over past its end.** Beyond the
  last stored transition the zone's two DST rules (month, day-of-week rule,
  time, offsets before and after) are evaluated arithmetically for the row's
  year - in lanes, that is the calendar family's own arithmetic, not a lookup.
  Java's `ZoneRules` has the same shape: `getTransitions()` then
  `getTransitionRules()`.
* **Gaps and overlaps decide the rounding.** A UTC instant one microsecond
  before a gap must floor-divide to seconds, or truncation snaps it onto the
  transition and picks the post-gap offset (their issue #14861); a local
  wall-clock inside a gap resolves to the post-gap offset to match
  `LocalDateTime.atZone`. Both are one-line decisions that a differential
  against Spark finds only if the fixtures straddle a transition by less than a
  second.
* **Scope by zone kind.** UTC and fixed-offset session zones are a constant
  add and belong to task 29's first kernels; region zones are the design above
  and decline until it is built. The taxi benchmark's `year(pickup_datetime)`
  (`SCOPE_MILESTONE_6.md` 1.5) is the first query that needs the region case.

### Item 3. Float and double lanes, and the numeric function family

**Deferred by the headline decision** (section 1) - the survey found zero
`DOUBLE`/`FLOAT` columns in TPC-DS and TPC-H; this is the taxi benchmark's
item and re-enters with that target. Design input kept in full:

* *The transcendentals are real vector calls.* JDK 25 ships `libjsvml.so`
  inside `jdk.incubator.vector`, and `VectorMathLibrary` looks its symbols up
  through a `SymbolLookup` at first use - so `lanewise(EXP, ..)` on x64
  reaches Intel's SVML port rather than a per-lane `Math.exp` loop. What
  aarch64 does instead must be checked before any doc claims the same.
* *So the oracle has to change.* SVML is not bit-identical to `Math` and
  `StrictMath`, so a double differential must be ULP-bounded, and Spark's own
  accuracy guarantee has to be read before a bound is picked. That reading is
  this milestone's open question 1 (section 7).
* *Comparison is not IEEE.* Spark's `SQLOrderingUtil.compareDoubles` makes
  NaN equal NaN and sort above everything, and `-0.0` equal `0.0`;
  `VectorOperators.EQ`/`LT` are IEEE. Every emitted double comparison needs an
  explicit NaN fix-up on the mask, and `NormalizeFloatingNumbers` does not
  save us - it rewrites only window partition keys and equi-join keys.
* *`round` and `DecimalType` are not this item* - `round(x, n)` is
  scale-dependent and decimals are not a lane type (item 12).

**Vector API it needs**: `DoubleVector` and `FloatVector`; `lanewise(Unary)`
with `SQRT`, `EXP`, `LOG`, `LOG10`, `CBRT`, `SIN` through `TANH`, `EXPM1`,
`LOG1P`; `lanewise(Binary)` with `POW`, `ATAN2`, `HYPOT`; the `FMA` ternary;
`Vector.test` with `IS_NAN`, `IS_INFINITE`, `IS_FINITE`.

### Item 4. ANSI-correct integer arithmetic, priced rather than assumed

Adopted as task 30 (see 2.4). The pricing argument carried over whole:
wrap-versus-saturate difference lanes are exactly the overflowed lanes, one
vector op and one well-predicted branch on the common path, `try_*` as the
branchless easy case worth shipping alone.

### Item 5. Boolean outputs

Adopted as task 27 (see 2.1).

### Item 7. Aggregation: the first horizontal reduction

**Deferred - first in the follow-on ladder**, and milestone 6's aggregate
wiring depends on it. Design input kept in full:

**Spark surface.** `HashAggregateExec`'s partial aggregation without grouping
keys: `sum`, `min`, `max`, `count`, `avg`, `bit_and`, `bit_or`, `bit_xor`,
`bool_and`, `bool_or`. Then the shape milestone 3's survey named and declined:
`CASE WHEN <date cmp> THEN x ELSE 0 END` inside `sum(..)` (TPC-DS q21 and
q40) - aggregate-*input* fusion, a different wiring from the projection path.

**Vector API it needs**: `reduceLanes(Associative)` and its masked overload,
`reduceLanesToLong`, and the `Associative` set - `ADD`, `MUL`, `MIN`, `MAX`,
`AND`, `OR`, `XOR`, `FIRST_NONZERO`.

**Design input.** The reduction belongs at the *end* of the batch: accumulate
into vector accumulators inside the loop and reduce once, with
multi-accumulator unrolling (acc0-acc3, breaking the dependency chain) - item
13's principle, applied at the one place a loop-carried dependency makes it
mandatory rather than measurable, and task 25's numbers will already exist.
The masked `reduceLanes` overload handles nulls without a branch. `sum` over
`LongType` inherits item 4's overflow question; `avg` is `sum` plus a
`trueCount`. Grouped aggregation is *not* this item - grouping is hashing and
partitioning (item 9's machinery, probably its own milestone). It changes what
an operator *is* rather than what an expression computes, so it wants the
plan-shape lessons from filters behind it - which it now has.

### Item 8. String functions, and the byte lanes they need

**Deferred - last in the ladder by frequency, named for completeness.** Kept
in full:

**Spark surface.** `length`, `upper`/`lower` on the ASCII fast path, `LIKE
'prefix%'`, `startswith`/`endswith`/`contains`, `substr`/`substring`,
`concat`, and `cast(string AS DATE)` done properly rather than folded. Four of
the six corpus functions still missing after milestone 6 are here
(`SCOPE_MILESTONE_6.md` section 1.7) - most of what stands between the roadmap
and the whole corpus function surface, and a long thin tail: 37 uses against
item 9's 275 key references.

**Vector API it needs**: `ByteVector` and `ShortVector`; `compare` with
`anyTrue`/`allTrue`; `rearrange` for byte permutation inside a value.

**Design input.** Variable width is the whole problem: Arrow strings are
offsets plus bytes, every operation is data-dependent in length, and the
fixed-lane-count loop stops being the right shape - which is why SWAR date
parsing stays in its own design pass.

**`cast(string AS DATE)`, designed** (September 2026, from Daniel Lemire's
`sse_date.c`, the 2023 "Parsing time stamps faster with SIMD instructions"
post, and his 2018 `eightchartoi.c`; `SKILLS.md`, "Validate a fixed-format
string with a saturating subtraction"). Spark's `stringToDate` grammar is wide:
trimming, an optional sign, a 4-to-7-digit year, 1-or-2-digit month and day,
and an optional `T` or space tail. The corpus writes `yyyy-MM-dd`. The kernel
accepts exactly that 10-byte form and sends every other row to the row engine,
the way task 26's guard does - it is a shape mask, not a parser.

* **Validation, branch-free.** XOR the bytes with `0x30`, so digits become
  0..9 and the dashes become `0x1D`. One saturating unsigned subtraction
  (`SUSUB`, in JDK 25's `VectorOperators`) against a per-position limit vector,
  `9 9 9 9 1D 1 9 1D 3 9`, leaves a nonzero residue for any non-digit, a
  wrong separator, a leading month digit above 1 or a leading day digit above
  3. Pair the digits into two-digit values and subtract again against `12` and
  `31` to catch 13..19 and 32..39. Subtract the other way against a minimum
  vector - `1` under the month and day pairs, `1D` under each separator, so a
  separator must equal `0x1D` exactly rather than merely fall below it - to
  reject month and day zero and a digit where a dash belongs. OR the residues;
  the row is in shape iff the OR is zero.
  The row's length must be 10 as well, which the offsets say before any byte is
  read. Day-in-month and the leap rule are not checked here: they fall out of
  `emitDaysFromCivil`'s month-length compare, which is already emitted.
* **Digits to fields, without `maddubs`.** The Vector API has no byte
  multiply-add, and does not need one. With the eight digits `yyyyMMdd` packed
  into one long lane, `eightchartoi.c`'s SWAR ladder - multiply by
  `1 + (10 << 8)`, shift 8, mask `0x00FF..`; multiply by `1 + (100 << 16)`,
  shift 16, mask `0x0000FFFF..` - stops after two steps with the four two-digit
  fields `yy yy MM dd` in 16-bit slots, which is what `emitDaysFromCivil`
  (task 40) takes after `year = 100 * hi + lo`. Four rows per 256-bit vector,
  in `long` lanes.
* **The load is the open question, and it is item 3's of milestone 6.** A
  10-byte record does not align to a long lane. The candidates are the
  index-spill path the gather probe used (per-row scalar loads into a `long[]`,
  then `fromArray`) or a `ByteVector.rearrange` compacting three rows out of a
  32-byte load when the column is known fixed-width. Neither is measured.

Not taken from the same source: its `is_leap_year_fast` and `leap_days_fast`
assume 1970..2106 and special-case only 2100, a narrowing Varka has no use for
under a total decomposition; and its `HHmmSS` combine, two 64-bit magic
multiplies over the `pmaddubsw` output, is tuned to a time part Spark dates do
not carry and Spark timestamps do not arrive with as strings at the kernel.

**The fallback's boundary, pinned from a second port of the same grammar**
(`NVIDIA/spark-rapids-jni`, `cast_string_to_datetime.cu`, ported from Spark
3.5's `SparkDateTimeUtils` and tested against Spark; read September 2026).
Its kernels are scalar C++ run once per thread - digit loops, early returns -
so nothing of their shape transfers to lanes, but the facts they pin do:

* Trimming treats `c <= 32 || c == 127` as whitespace, which is
  `UTF8String.trimAll`'s definition, not Java's `isWhitespace` alone.
* The year takes 4 to 7 digits for a date (4 to 6 for a timestamp); a date is
  valid only for years within +-10,000,000, so `1000000-01-01` parses and
  `10000001-01-01` does not.
* After the day, one space or `T` ends the parse and anything may follow:
  `2025-01-01T`, `+2025-01-01Txxx` and `-2025-01-01 xxx` are all valid.
* Its `castStringToDate` fixture list is the fallback test for the fixed-form
  kernel, every row of it a shape the mask must decline and the row engine must
  then accept: `"  2025"`, `"2025-01 "`, `"2025-1  "`, `"2025-1-1"`,
  `"2025-1-01"`, `"2025-01-1"`, `"2025-01-01"`, `"2025-01-01T"`,
  `"+2025-01-01Txxx"`, `"-2025-01-01 xxx"`, and the two large years above.
* Its ANSI protocol is the status contract Varka already has: parse to a
  nullable column, and under ANSI fail the batch if the null count grew. It
  also documents one deliberate deviation - its pattern parser accepts
  one-digit month and day for `yyyy/MM/dd` where Spark's strict formatter
  rejects them - which is the kind of shortcut a differential against Spark
  refuses by construction.

### Item 9. String keys: equality, hashing, dictionaries

**Deferred - its near-term half is already milestone 6's item 3** (fixed-width
equality against a literal, hashing short values for grouping); what stays
here is the machinery that subset does not need. Kept in full:

**Spark surface.** Strings as keys: equality against a literal, `IN` against
a small set, grouping and join keys (275 group-by references, 60% of all).
`hash`, `xxhash64`, `murmur3`. The plain bit expressions that share the
operators: `bit_count`, `shiftleft`, `shiftright`, `shiftrightunsigned`, `&`,
`|`, `^`, `~`.

**Vector API it needs**: `ROL`/`ROR` (murmur3's mix is rotate-multiply-xor);
`BIT_COUNT`, `LEADING_ZEROS_COUNT`, `TRAILING_ZEROS_COUNT`, `REVERSE`,
`REVERSE_BYTES`; `BITWISE_BLEND`; `COMPRESS_BITS`/`EXPAND_BITS`; `LSHL` and
`ASHR`; `VectorShuffle` in full with `rearrange` and two-vector `selectFrom`.

**Design input.** *There is no off-heap gather.* Gather and scatter exist
only on the `int[]` array overloads, never on `MemorySegment`. A dictionary
decode over an off-heap Arrow dictionary either copies the dictionary on-heap
or uses `rearrange`/`selectFrom` with a dictionary small enough to sit in one
vector - and a low-cardinality `CHAR(n)` column is exactly what a Parquet
reader dictionary-encodes, so this is the common case. Scatter is missing
too, which is why grouped aggregation expects to vectorise the hash and key
compare while keeping the probe and accumulator update scalar.

**What a gather costs, now measured** (`VarkaVectorApiProbeBenchmark`, added
because this item rested on an assumption). Reading `year` out of a day-indexed
table - the shape Impala ships for 1950-2049 - against the civil-from-days
arithmetic, over 20M dates at AVX-512: the `IntVector` gather runs at 3573.9 M
rows/s over the whole 143 KB table and 3728.2 over a seven-year span, against
2379.8 and 2368.3 for the arithmetic. **A gather is not the slow primitive this
item assumed it was**, which makes the copy-the-dictionary-on-heap option more
attractive than the paragraph above implies, and it is worth re-reading before
item 9 is planned rather than inheriting the assumption.

Two findings come with it. A plain **scalar** `int[]` loop over the same table
is faster still - 4630.0 M rows/s on the seven-year span, 1.95x the vector
arithmetic - because the Vector API takes a gather's index map as an `int[]`,
so the index vector is stored and read back, and that spill is the API's rather
than the machine's.

**Fused, the ranking inverts.** The same three measured as `year(d) = 1998`,
counted, where the vector paths never leave a register: the gather reaches
3999.8 M rows/s against the arithmetic's 1453.0 - **2.8x** - while the scalar
loop that led the unfused table falls to 848.1, and the hybrid an emitter would
have to produce (spill the lane group, scalar-lookup, reload) is a wash with the
arithmetic at 1446.9. So emitting scalar code for a calendar node buys nothing
once the result is compared and counted.

**Correction, measured after the above was written.** That paragraph went on to
say the only lowering which would pay is one the API forbids. It does not. The
missing `fromMemorySegment` index-map overload blocks gathering *from* an
off-heap table - this item's dictionary case, where the claim still stands - and
says nothing about gathering an **on-heap constant table** indexed by off-heap
data. A calendar table is the second kind, because Varka owns it: the column
loads with `fromMemorySegment`, the index vector spills with `intoArray`, and
the gather reads a table on the heap. Measured in that shape, with the column in
a `MemorySegment` the way a real kernel has it, an era-indexed table reaches
2070.8 M rows/s against the arithmetic's 1329.3 - a **1.6x**.

The table's size is worth taking from ClickHouse, whose `DATE_LUT_SIZE` is
146097: one Gregorian era. Indexed by day of era rather than by an arbitrary
year window, such a table needs **no fallback for any `int32` date**, and the
index is what `emitEra` already produces, so the table replaces everything after
it. That is a candidate lowering for the whole calendar family and belongs in
its own task. What it changes for this item is narrower: the
copy-the-dictionary-on-heap option is attractive on a measured basis rather than
on an assumption about what gathers cost.

### Item 10. Cross-lane movement: windows, prefix sums, row indices

**Deferred - after item 7 in the ladder**; it shares the not-lane-shaped
problem and adds a state contract on top. Kept in full:

**Spark surface.** `WindowExec` where the frame lives inside one batch: `lag`
and `lead` by a small constant offset, running aggregates over `ROWS BETWEEN
UNBOUNDED PRECEDING AND CURRENT ROW`, `row_number` within a batch,
`monotonically_increasing_id`.

**Vector API it needs**: `slice(int, Vector)` and `unslice`; `addIndex`;
`VectorSpecies.iotaShuffle`; `rearrange`.

**Design input.** `lag(x, 1)` across a lane group is exactly `slice(lanes -
1, previousVector)`; a running sum is the log-step prefix scan (shift by 1,
2, 4 and add). What makes it an operator change is the carry: a frame crosses
batch boundaries, so the kernel needs carry-in and carry-out state and a
visible partition boundary - a contract like milestone 3's selection vector,
not a new IR node.
