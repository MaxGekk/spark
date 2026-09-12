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
* **66, second-level chrono fragments** - added 5 September 2026 (section
  2.8): the calendar tails' shared parts (year and leap flag, January month,
  month start) factored the way task 32's prefix was, behind the same
  fragment mechanism. Pays only inside one lane group, so it follows task
  32's step B2 grouping decision, which it does not change.
* **74, the validity-word algebra's missing axioms** - added 7 September
  2026 (section 2.9) from the owner's question, over task 70's PR, of what
  the word algebra is mathematically and which of its laws the emitter does
  not yet use. Two it does not: the coalesce axiom and absorption. Not a
  lane, but an emitter follow-up the owner placed in this milestone so that
  milestone 4 can close.
* **75, zero-copy validity for leaf words** - added the same day (section
  2.10): an output whose word *is* an input's bitmap shares the buffer
  instead of copying it. Small and bounded by a committed number - and the
  number moved under it when task 70 was regenerated a third time, to 0.4%,
  below this task's own decline line, so read 2.10 before starting it.
* **81, Spark's own date tests as a differential corpus** - added the same day
  (section 2.11), from the owner's question about estimating test coverage:
  every oracle this project checks itself against, it also wrote, so the
  expressions Spark's own suites exercise are run over Arrow-cached fixtures in
  both engines with the plan classified fused, partial or declined. The
  golden-file inputs are the first source, once each statement's literals are
  rewritten into columns - without that they constant-fold and reach no
  kernel. Follows task 74 and nothing else.

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

*Added 7 September 2026, from section 2.9's reading of task 70's word
algebra.* A boolean output is where that algebra's domain ends, and the task
has to say so in code. Task 70's bitmap pass serves a root whose validity is
a pure function of the inputs' validity bitmaps, which holds for every date
node because each is either strict (null in, null out) or null-skipping.
SQL's connectives are neither: `a AND b` is *false*, not null, when one side
is false and the other null, so the validity of a boolean output depends on
the operands' values as well as their bitmaps - Kleene's three-valued
lattice, not the bitmap lattice. `Analysis.pureOf` falls to `null` for any
node without an arm, so a boolean root is fail-safe today by omission; task
27 makes it fail-safe by statement: a test that every boolean root - a
comparison, `And`, `Or`, `Not` over nullable operands - has no pure word and
is never served, and a sentence in the emitter's algebra javadoc naming the
boundary. Mask space, where 2.1 already keeps the connectives, is the right
place for the known-true and known-false pair the connectives need.

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

### 2.8 Second-level chrono fragments (task 66)

*Added 5 September 2026, from the owner's question over the IR data-flow
drawing (`docs/img/varka/varka-ir-levels.svg`): where the prefixes are, and
whether there are only two.*

**What exists.** One shared fragment kind: `FragmentKind.CHRONO_PREFIX`, task
32's step B1 - the civil-from-days decomposition run once per distinct date
per lane group into eight locals that every `Chrono` tail and `AddMonths` read.
The mod-7 lowering is not a fragment; `emitFloorMod7` is emitted inside each
node that needs it. And the calendar tails are in the same state: their
helpers are factored in the Java source (task 35 and task 61 did it for
`trunc`) but every node re-emits them against the prefix's locals.

**The register of repeated tails**, read off `VarkaLoopEmitter` on the task 61
branch as call sites of each helper, each site a node that recomputes the same
value when it sits beside another consumer over the same date:

| shared value | helper | emitted by |
|---|---|---|
| plain year, then the leap flag, then the January day of year | `emitChronoYear` (6 sites), `emitLeapFlag` (4), `emitJanuaryDayOfYear` (2) | `Year`; `DayOfYear`; `TruncDate` YEAR and QUARTER; `TruncDateDynamic`; `LastDay` and `AddMonths` (the leap flag only) |
| January-based month | `emitChronoMonth` (6 sites) | `Month`; `Quarter`; `TruncDate` QUARTER; `TruncDateDynamic`; `AddMonths`; the recompose `trunc` form |
| month start, zero-based day of month | `emitMonthStart` (6 sites), `emitZeroBasedDayOfMonth` (2) | `DayOfMonth`; `TruncDate` MONTH; `TruncDateDynamic`; `LastDay`; `AddMonths` |
| `floorMod(d, 7)` over the same date | `emitFloorMod7` (4 sites) | `DayOfWeek`; `WeekDay`; `TruncDateDynamic`'s week result. Not `NextDay`, whose mod is over `k - d`; the week-rule tasks (37, 57, 58) add consumers through their Thursday shift, which node-level CSE already shares |

So `year(d), dayofyear(d), trunc(d, 'YEAR')` runs the year-and-leap chain three
times, `month(d), quarter(d)` the month step twice, and `dayofweek(d),
weekday(d)` the twelve-op mod twice.

**The design is the existing one, one level down.** Three or four more
`FragmentKind`s (`YEAR_PARTS`, `JANUARY_MONTH`, `MONTH_START`, `FLOOR_MOD_7`),
keyed by the same decomposed child, body mode and lane group as the prefix
(`fragmentKey`), allocated once per fragment in `planSlots` and emitted once
per lane group by the first consumer (`emittedFragments`), the later consumers
reading the locals. The prefix's slot discipline carries over unchanged: a
fragment writes only its own locals and never a sibling's, which is the lesson
`PLAN_TASK_36.md` recorded after doing it the other way first. The tails'
helpers already take their inputs and outputs as slot numbers, so the change is
in the planning and the once-per-group check, not in the arithmetic. The
`elideChronoMonth` question repeats one level down: a fragment is emitted only
if some consumer in the group reads it, decided over the group as
`fragmentsReadingMonth` decides the month step today.

**What it is worth, honestly.** A tail is 4-12 ops against the prefix's ~30,
so this pays only when three or more fields of one date sit in one lane group -
which is exactly the shape task 32's step B2 makes common by relaxing
`GROUP_BUDGET` for calendar outputs, and no other. Registered expectation:
10-25% on the four-field shared row, under 5% on any two-field one, nothing on a
single-field query. The register test's counts move for every shared shape and
the `HugeMethodLimit` ladder may move again (every prefix change has), so both
are re-pinned as fixtures, not as findings.

**Sequencing.** After B2's grouping decision is in (`PLAN_TASK_32.md` 7.2 says
its gate cleared; the default is a policy the owner sets from both widths'
numbers). Independent of tasks 29 and 65; if 65 replaces the prefix, the
year-parts fragment changes shape but not its existence.

**The gate.** The four-field parity row shared under the new fragments against
the same row under B1 alone, both widths, three runs, minimum best-time, on the
shape `year(d), dayofyear(d), trunc(d, 'YEAR'), month(d)` and on the committed
`year+month+day+quarter` row. Under 1.05x at AVX-512 on both: decline, and the
register above goes to the debt register as the record of what re-emission
costs.

**Beyond dates.** The next first-level prefixes are milestone 5's timestamp
work, where sharing will matter more than here because every field pays the
split first: micros to days plus micros-of-day (under `date(ts)`, the calendar
tails and the time-of-day tails), seconds-of-day under `hour`/`minute`/`second`,
and the per-timestamp zone offset once item 2's tzdata design lands. They are
recorded in section 9's item 2 as the shape to design the fragment keys for,
not as tasks.

### 2.9 The validity-word algebra's missing axioms (task 74)

*Added 7 September 2026. The owner, reading task 70's PR (#145), asked what
the word algebra is from a mathematical point of view and whether known laws
of that structure could be applied to more complex expressions. This section
is the answer, with the census that turned it into a task.*

**What the algebra is.** Task 70's `WordExpr` is the `{AND, OR, 1}`-reduct
of a finite Boolean algebra: the validity bitmaps of a batch are the direct
power of the two-element Boolean algebra over the batch's rows, and the
emitter uses only the two lattice operations and the top element. For each
operator alone that is a bounded semilattice - associative, commutative,
idempotent, with `1` as AND's unit and OR's annihilator - and the free
bounded semilattice on the input ordinals is exactly the set of leaves plus
the operator that `BitmapPass` stores as a served root's normal form. The
pass's soundness (`PLAN_TASK_70.md` 3.1) is the fact that a direct power's
operations are componentwise, so evaluating a word per lane group and
concatenating equals evaluating it over the whole batch. Two laws of the
lattice the folding does not use: **absorption** (`x AND (x OR y) = x` and
its dual) and the fact that `coalesce`, which the compiler builds as
`IfElse(IsNotNull(x), x, y)`, denotes `x OR y` - the compiler's own comment
on `compileCoalesce` proves this (`(kT AND v(x)) OR (NOT kT AND v(y))` with
`kT = v(x)` reduces to `v(x) OR v(y)`), and `pureOf` still returns nothing
for it because `IfElse` has no arm.

**The census, and a caveat found on 12 September 2026.** Its `surface` corpus is
not the whole surface: `resolve` had no analyzer pass, so every date/interval
shape task 67 added parsed to an `Add` the compiler declined, and the corpus was
truncated by hand to "the `Surface` projections that resolve without the
analyzer's type coercion". The figures below are therefore over about two thirds
of the surface while reading as though they were over all of it. The resolver is
fixed and the interval columns are declared, so widening the corpus is now a
matter of adding the entries and requoting this paragraph - work for whichever of
tasks 74 and 75 next touches the census, since it moves every number here.

`VarkaWordCensus` (catalyst test scope,
`dev/varka_word_census.sh`) classifies every value root of three corpora by
its word today and under the two extensions, and checks its verdict against
the emitter's by emitting each single-root shape with the pass on and off:
a served root changes `loopMasked0`'s bytes. Run on 7 September 2026 over
the `Surface` projections, sixteen composites on the operator boundary, and
20000 shapes from the fuzzer's value grammar (seed 7): 6686 single-root
shapes cross-checked, 0 disagreements. What it found:

| corpus | leaf | chain | no expression | mixed |
|---|---|---|---|---|
| `Surface` projections (30) | 22 | 5 | 3 (`if`, `CASE`, `coalesce`) | 0 |
| fuzzer grammar (39742 roots) | 25584 | 6405 | 6183 | 1570 |

* **The coalesce axiom** turns `coalesce(d, d2)` - the one `Surface` entry
  with no expression that is not a comparison blend - into an OR chain, and
  with it `coalesce(d, d2, d3)` (a chain of three), `year(coalesce(d, d2))`
  and `greatest(coalesce(d, d2), d3)`. In the fuzzer's grammar it gives a
  word to 1674 of the 6183 roots that have none; the rest are comparison
  blends and `make_date`, which are not pure and stay so.
* **Absorption** turns `datediff(greatest(d, d2), d)` and
  `greatest(date_add(d, i), d)` from mixed trees into a single leaf, and
  `datediff(coalesce(d, d2), d)` likewise once the coalesce axiom is in. In
  the grammar it resolves 1014 of the 1570 mixed roots. No `Surface` entry is
  mixed, so on the inventory alone this law is worth nothing; it is two
  rewrite rules, and it is what makes the coalesce axiom compose.
* **What stays mixed** after both: 556 of 39742 grammar roots, 1.4%, and
  composites such as `datediff(greatest(d, d2), greatest(d3, d4))` and
  `date_add(greatest(d, d2), i)`. Their Strahler numbers are 2 or 3 in all
  but two of the 40000; section 9's item 14 is the evaluator that would
  serve them, and this census is its admission threshold.

**Two things the reading found already done or not worth doing.** The unit
and annihilator laws have a run-time half - a null-free column is the top
element and drops out of an AND or decides an OR - and the engine's five
pass entry points already resolve it per batch from the null counts, before
touching a bitmap. And cross-output sharing, which the normal form makes
exact (two roots want the same bitmap iff their leaf sets and operator
agree), is not worth a mechanism: in the grammar's 13284 multi-root shapes
only 403 served roots repeat a *non-leaf* form of a sibling and 62 pairs
stand in the subset relation under one operator; a repeated leaf is already
a copy. Recorded so that nobody designs it twice.

**The design.** Two `pureOf` arms and two folding rules, no new node, no
new option component:

* `IfElse(IsNotNull(x), x, y)` denotes `or(w(x), w(y))`, matched
  structurally (the condition's child is the then-branch, by reference).
  `WordOwner` stays `Own` for the node, since the blend still computes its
  slot when a consumer wants it; Theorem 1's one-directional check therefore
  constrains nothing new. Liveness already demands both operand words for
  the blend's known-true mask, so serving the root removes its write and
  nothing else.
* `andExpr(a, Or(p, q))` with `p == a` or `q == a` folds to `a`, and the
  dual in `orExpr`. `andRef`, the slot-level folding, is deliberately left
  without absorption: the emitter may stay more conservative than the
  algebra (the plan's Theorem 1), and the agreement assertion already
  allows an `Own` owner under any expression.

Behind no switch of its own: both are extensions of `validityByBitmap`'s
rule and ride its option. The census tool loses its mirror: task 74's first
commit exposes the emitter's own classification through a test hook and
re-runs the census through it, which is also the test that the mirror was
right.

**A third arm, held as a question.** A filter whose predicate is a null
test - `WHERE d IS NOT NULL`, `IS NULL`, and their conjunctions - has a
selection mask that is a pure function of the input bitmaps too, with
complement: the `Cond` sub-algebra over `IsNotNull` leaves is the full
Boolean algebra, De Morgan gives it a negation normal form, and the driver
could write the selection with the same pass plus an and-not entry point.
It is not in this task's deliverables because the filter's masked loop for
that predicate is already a load and a store per group, and compaction, not
the mask, is where the filter's time goes. Measured before it is built:
one row in `VarkaFilterBenchmark`.

**Validation.** `coalesce(d, d2)` served: `loopMasked0` and
`epilogueMasked` byte-equal to the dense twins, the way task 70 pinned the
four-field shape; the parity benchmark gains a `coalesce(d, d2)` A/B pair
(pass on against the per-group reference arm) beside task 70's, both
widths; the differential over the nullable fixtures for `coalesce` with two
and three operands, `datediff(greatest(d, d2), d)` and
`greatest(date_add(d, i), d)`, values byte-identical to the row engine over
every null pattern; the fuzzer with both extensions randomised, two million
shapes clean; the census re-run through the emitter's analysis with the
composites' verdicts unchanged. Registered expectation: `coalesce(d, d2)`
masked with mixed nulls lands on its dense row at both widths, as `year`
and the four fields did; no other committed row moves.

### 2.10 Zero-copy validity for leaf words (task 75)

*Added 7 September 2026, from the same reading.*

**The observation.** After task 70, 22 of the 30 `Surface` projections have
a leaf word: the output's validity *is* one input's bitmap, and the driver's
pass is a copy of it - `copyColumnValidity`, `(rows + 7) / 8` bytes per
output per batch. Arrow lets a vector share a buffer instead: the fork's
`ArrowCachedBatchSerializer` already hands vectors buffers it does not own,
and `BaseFixedWidthVector.loadFieldBuffers` retains a foreign validity buffer
through its `ReferenceManager` when the null count is strictly between zero
and the row count (checked against `arrow-vector` 19.0.0's bytecode: the
retain at the end of `BitVectorHelper.loadValidityBuffer`; the all-valid and
all-null cases allocate a constant buffer instead). So an output with a leaf
word can be assembled from a fresh data buffer and the input's own validity
buffer, retained, and the copy disappears. The null count travels with it,
which removes the second thing this task is about: Arrow's `getNullCount`
is a scan of the bitmap on every call (`BitVectorHelper.getNullCount`, no
cache in 19.0.0), and the evaluator asks for it once per input per batch in
`extractMorsel` and once per compacted column in the filter - on a vector
Varka itself just wrote, whose count the pass could have kept.

**What it is worth, bounded before it is built.** *Requoted on 7 September
2026, and the requote moves the bound. This paragraph was written against
task 70's second regeneration; the review of #145 forced a third, and the
gap it rested on fell from 3.4% to 0.4%. The superseded figures are in this
file's history and in `PLAN_TASK_70.md` 9.2, which scores its predictions
against the run they came from.*

The committed parity file puts masked `year(d)` at 3446.7 M rows/s against
its dense twin at 3459.3 at AVX-512, adjacent rows in one run: a **0.4% gap**
on identical loop bytes, where the earlier run read 3.4%. At 128-bit the
masked row is 1332.5 against the dense 1331.4, which is to say ahead of it.
The four-field shape's masked row also sits above its dense row, 1703.9
against 1687.4. So the ceiling this task could recover is 0.4% on the
cheapest single-field kernel at the wide width and nothing anywhere else, and
the null-count scans are of that same order (a `popcount` per 64 rows).

**Which is below this task's own decline threshold**, stated in the next
paragraph as 2% at AVX-512. On today's committed numbers the admission check
would decline before it ran. Two things follow rather than one. The
*zero-copy validity* half is, on this evidence, already answered: the copy it
would remove is not visible in the file, and the honest outcome is a recorded
decline unless someone wants the probe for its own sake. The *cached null
count* half is untouched by that arithmetic, because the masked-against-dense
gap does not measure it at all - `getNullCount` is a bitmap scan the evaluator
pays per input per batch and again per compacted column in the filter, on
vectors Varka itself just wrote, and no row in the parity file prices it. If
this task survives, that is what it is about, and it wants its own
measurement rather than this one.

**The admission check.** In the parity harness, the masked `year(d)` row
with the destination validity pre-filled and the copy skipped, against the
row as committed, both widths, three runs, minimum best-time. Under 2% at
AVX-512: decline, and the bound above goes to the debt register as the
record. At or above: the task is the buffer sharing in `computeFused` (the
leaf case of the pass resolved to a retained input buffer, keyed off the
same `served` table the driver reads), a cached null count on
`VarkaOwnedArrowColumnVector` for every output the pass wrote, and the
filter's compaction reading it; validated by the differential over every
null pattern (a shared buffer must never be written by the kernel - the
loop's skipped write for a served root is what makes this safe, and a test
asserts the output's validity address equals the input's) and by Arrow's
allocator accounting closing to zero at task end with the retained buffers
released.

**Sequencing.** After #145 (task 70), which it reads; independent of task
74, though the two share the driver's `served` table and merge trivially in
either order.

### 2.11 Spark's own date tests as a differential corpus (task 81)

*Added 7 September 2026, from the owner's question about how test coverage
could be estimated, and his agreement with the shape the answer proposed:
"run them under both engines and assert both the answers and that Varka
actually ran".*

**Why this and not line coverage.** Every oracle this project checks itself
against, it also wrote: `VarkaReferenceEvaluator` for the fuzzer, the hand-built
matrices in the emitter suite, the fixtures in the differential suite. They are
good instruments and they share one weakness - an assumption held by the author
of the lowering is held by the author of the oracle. Spark's own date tests are
the corpus this project did not write, encoding what the engine is supposed to
do rather than what we thought it did. That is worth more than a coverage
percentage, and it is the only instrument here that can find a misreading of
Spark's semantics rather than a slip in their implementation.

**What can be reached, and what cannot.** Two kinds of date test exist upstream
and only one can meet Varka at all.

* `DateExpressionsSuite` (catalyst, 105 tests) drives Catalyst expressions
  through `checkEvaluation`. There is no plan, no physical operator and no
  columnar batch in that path, so Varka is structurally absent. Nothing to do
  here, and saying so is the point: a task that set out to "run Spark's date
  tests under Varka" would otherwise spend its first day discovering it.
* `DateFunctionsSuite` (sql/core, 68 tests) and the date parts of
  `ColumnExpressionSuite` go through SQL and the DataFrame API, so they reach a
  physical plan. They still miss Varka as written, because Varka needs an
  Arrow-backed columnar source and these build 77 DataFrames from local
  sequences and cache none of them - the same gap `SCOPE_MILESTONE_6.md`'s
  benchmark survey found in Spark's benchmarks.

**The design: harvest the corpus, not the suite.** The obvious approach -
subclass the suite with the Arrow cache serializer set - founders on the
fixtures: the data is built inline at 77 sites and never cached, so the
subclass would run every test through the row engine and pass, measuring
nothing. Making it work by rewriting every `LocalRelation` into a cached Arrow
relation is a session-extension change wide enough to alter what the rest of
the suite tests.

So the task harvests instead: extract the date expressions and SQL the upstream
tests exercise, as a committed list, and run each over the differential suite's
own Arrow-cached fixtures in both engines. `VarkaSharedSessions` already builds
the pair of sessions and sets `SPARK_CACHE_SERIALIZER`; the corpus is the new
part. A harvested entry that Varka cannot fuse is kept and marked, because the
count of what is in and what is out is itself the coverage number this task
exists to produce - and unlike a percentage it says *which* expressions.

**The golden files are the better source, and they need a rewrite step.**
`sql-tests/inputs/date.sql` and its siblings are the same semantics corpus
written as SQL text rather than Scala, which makes them far cheaper to harvest
than a suite body: the statement is already a string and the expected answer is
already committed beside it in the `.sql.out`. Seven date-family inputs carry
254 `select` statements between them.

They cannot be run under Varka as they stand, and it is worth writing down why
so that nobody tries: **the files carry no data.** In `date.sql`, 94 of the 101
statements are literal expressions - `make_date(2019, 1, 1)`, `date '2019-01-01'`
- which constant folding evaluates during optimisation, so no scan, no columnar
batch and no physical operator ever exists for the rule to rewrite. The seven
that do read a relation read `date_view`, one row of two *string* columns built
from literals. Across the seven files, 40 of 254 statements have a `FROM` at all,
and those read views of the same kind. Running them under Varka today would
exercise exactly zero kernels, and every test would pass.

So the harvest's real work is a **rewrite**: turn each statement's literal
operands into columns of an Arrow-cached fixture, so `make_date(2019, 1, 1)`
becomes `make_date(y, m, d)` over a fixture whose rows include that triple. That
rewrite is not incidental - it is precisely what moves an expression out of
constant folding and into a kernel, which is the whole reason the corpus is
worth having.

One consequence to state plainly: **the golden `.sql.out` files stop being the
oracle** once the operands become columns, because the answers change with the
data. The oracle remains the row engine over the same fixture, as everywhere
else in the differential suite. What the golden corpus contributes is the list
of expressions and edge cases Spark's own maintainers thought worth pinning -
the part this project cannot write for itself - not the expected values.

**Asserting that Varka ran is half the task.** A declined entry falls back to
the row engine and answers correctly, so a corpus run that only compares
answers passes whether or not Varka executed a single kernel. Task 62 met this
exactly and built the classification for it: `Fusion.PARTIAL` when a row-engine
`Filter` or a non-empty `Project` sits above the Varka node, and a partial shape
fails a run that asked for fusion. This task inherits that rather than
reinventing it, and the per-entry verdict - fused, partial, declined - is what
gets committed beside the answers.

**What it will not catch, stated so nobody reads more into a green run.** These
tests use a handful of rows each, so they exercise the epilogue and never a full
lane group, which is where several of this project's bugs have lived - the
fuzzer and the emitter suite's length matrices stay the instrument for that. And
the corpus is only as good as the harvest: an expression the harvest misses is
invisible, so the extraction is committed and reviewable rather than done once
by hand.

**Sequencing.** After task 74, because the fuzzer covers shape space far more
densely than 68 hand-written tests will and its one node-type gap should close
first. Independent of everything else in this milestone.

### 2.12 The mask-to-long disposal in a checked kernel (task 82)

*Added 8 September 2026, from task 63's measurement.*

**The observation.** Task 63's ANSI overflow check is five lanewise
operations, and in a dense body it costs what five operations cost: 1.4% on
`i + 1` at AVX-512 and 26.6% at 128-bit (`VarkaArithmeticBenchmark`, requoted
from `80d06a51560`). In a masked body at 128 bits the same node costs 65.8% -
6522.9 M rows/s against 19073.8 with the check off - on arithmetic that did not
change. At AVX-512 the masked cost is 3.9% - though 2.21 finds the cross-run
diff behind that reading is inside the file's noise band, and asks this task to
re-take it pinned before scoping itself on it. The first run read 19.0%, and
that turned out to be a dead local slot rather than the disposal, so this task
is a 128-bit finding and the wide width is not evidence for it. The
difference is not the sign test. It is what happens to the mask afterwards:
`emitGuardCollect` converts it to a `long` through `VectorMask.toLong`, ANDs
it with the node's validity word, and ORs the result into the batch
accumulator, once per lane group. `try_add`, which disposes of the same mask
by narrowing the word rather than accumulating it, is slower again - 3780.6
against the wrapping add's 18946.9 at 128-bit.

**Why it is worth its own task.** `emitGuardCollect` is not task 63's code.
Task 52's range guard on a column-offset day producer uses the same helper and
the same accumulator, and task 42's `make_date` year check and task 60's
month-count check reach it too. So every runtime refusal Varka emits pays this
in its masked body, and none of them has ever been priced against a version
that does not - task 52's A/B measured the guard whole, mask and disposal
together, at 10-15%, which is consistent with this but does not separate them.

**The shape of a fix, to be decided by measurement rather than here.** Three
candidates, cheapest first. Keep the accumulator as a `VectorMask` and OR the
masks lanewise, converting once per batch at `emitStatusReturn` instead of
once per lane group - which is a smaller change than it sounds, because the
accumulator is already a local. Or keep a `long` but skip the AND where the
node's word is `WORD_DEAD` or all-ones, which task 70's algebra can already
say. Or hoist the whole collect out of the group when the analysis proves the
mask empty for the batch, which is task 64's statistics-directed selection
arriving at the same place from the other side, and the reason these two want
to be read together.

**The admission check.** `VarkaArithmeticBenchmark`'s masked rows, which
exist and are committed, are the before. A candidate has to move the checked
mixed-null `i + 1` row at 128-bit by more than 10% without moving the dense
rows or any unguarded shape's bytes - and at AVX-512 there is only 3.5% to win,
so that width decides nothing here, and `VarkaEmitterParityBenchmark`'s
task 52 guard pair has to move with it or the change is not what it claims.

### 2.13 What a new node type costs, and which of it is avoidable (tasks 83 to 86)

*Added 8 September 2026, from what task 63 cost to build and what its review
found afterwards.*

**The observation, measured rather than remembered.** Task 63 added two node
types to a mature emitter, which makes it a natural experiment in where the
cost of a new type actually falls. The emitter holds **seventeen switches over
the IR: ten exhaustive, seven carrying a `default`**. Sorting task 63's own
mistakes by where the decision lived predicts what each cost almost exactly:

| where the decision lives | a missing arm | what it cost this task |
|---|---|---|
| an exhaustive switch - `childrenOf`, `analyze`, `emitValue`, `liveWords` twice, `assertWordAlgebraAgrees` | a compile error | minutes, at the keyboard |
| a switch with a `default` - `ownerOf`, `pureOf`, `planWordRef`, `chronoChild`, `emitChrono`, `tailReadsMarchMonth`, `collectColumnOffsetProducers` | a silent pessimisation: right answer, worse code | never noticed; found by reading |
| **no switch at all** - the bound analysis, and `requireDayOffsetShape` against `compileOffset` | **a wrong answer, or a ghost fallback** | a max-effort review, after the PR was open |

All three of task 63's wrong answers and its one ghost fallback came from the
last row. That is the ranking rule these four tasks are ordered by: not how
much code a refactor removes, but how far the decision it touches sits from an
exhaustive match. The rule matters twice over because the expression-porting
work is meant to be delegated to cheaper agents - the "recipe for a cheap
agent" shape tasks 33, 39 and 40 are written in, and the reason task 70 chose
the form that "keeps the emitter smaller, which the delegation goal wants"
(`PLAN_TASK_70.md` 3). For that reader the goal is not that a weak model
writes less, it is that a weak model **cannot fail quietly**; `SKILLS.md`'s
"a recipe for a cheap agent ages at the rate of the emitter" is the same
lesson from the other side.

**What is deliberately not here.** The chrono fragment machinery. Thirty-odd
lane ops behind a shared prefix is genuinely unlike every other node, and
folding it into a general scheme would cost more than it returns - the same
judgement `VarkaVectorIR`'s own javadoc already records.

### 2.14 One refusal, instead of four (task 83)

**The observation.** Four node kinds now refuse lanes at run time: task 42's
`make_date` year check, task 52's range guard on a column-offset day producer,
task 60's month-count guard, and task 63's overflow check. Each arrived with
its own analysis set - `guardedProducers`, `selfGuarding`, `checkedArith` -
its own slot rule, and its own arm in `planSlots`, and all four dispose of
their mask through the same `emitGuardCollect` into the same accumulator and
report the same `STATUS_CHRONO_RANGE`.

The accretion is visible in one predicate. `guardedWord` was written for task
52, gained a disjunct for task 60, gained a third for task 63, and its javadoc
argued that keeping it single is what stops the next kind being added to one
reader and forgotten in the other. Task 63's review then found that the two
readers were asking different questions - `planSlots` wants "does this node
need a scratch local", `liveWords` wants "must this node's word stay alive" -
and that checked arithmetic answers yes to the second and no to the first, so
every checked node had been reserving a local nothing ever loaded. The
predicate had to split, which is the argument for a real abstraction rather
than a fourth disjunct.

The shared status bit is the same story at the telemetry end: an ANSI overflow
decline reports "chrono range", and `VarkaFusedKernel`'s own javadoc invites a
new lowering to take its own bit. Task 63 left this registered rather than
fixed (`PLAN_TASK_63.md` 9.7, from its 7.4).

**The shape.** Refusal becomes a property a node declares - the mask it
computes, the word that qualifies it, and the reason it refuses - with one
analysis set, one slot rule, one collect, and a status bit per reason. A fifth
refusing node is then one arm, and the "which of the four fired" question that
telemetry cannot answer today becomes free.

**The admission check.** No emitted byte moves for any shape that exists
today: the pinned line map, the shape hash and every `codeSize` assertion hold
unchanged, and `dev/varka_emit.sh --table` shows the same op counts for
`year(date_add(d, off))`, `add_months(d, m)`, `make_date` and `i + 1` under
ANSI. That is the whole check - this task buys legibility and a status bit,
not speed, and if it moves a byte it has changed something it should not have.

### 2.15 One value-range lattice, instead of two overlapping analyses (task 84)

**The observation.** Two analyses compute overlapping facts about what a node
can hold. `dayRange` answers "which epoch days can this subtree produce", for
admitting a calendar node's child (task 52). `intBound` answers "how large can
this int be in absolute value", for removing an overflow check (task 63). They
disagree about what a runtime guard proves, they duplicate the literal-slot
lookup, and task 63 had to bridge them - `intBound`'s `datediff` arm now calls
`dayRange` with a flag that turns the guard assumption off.

That bridge is where the bugs were. Of the three wrong answers task 63's
review found, two were in exactly this seam: `datediff`'s bound assumed the
date contract for operands that a literal shift had already pushed out of int
range, and nested bounds were combined with wrapping `Long` arithmetic, so a
bound that passed 2^63 came back small and positive and proved anything at all
safe. The third was an off-by-one in the comparison the bound feeds. Three
bugs, one region, and none of them reachable by a missing switch arm - which
is why they survived to a review.

**The shape.** One interval domain over lane values, with the two questions as
queries on it rather than as separate traversals. Two properties do the work.
The lattice's operations saturate by construction, so the `Math.addExact`
discipline task 63 had to add by hand is not something a later arm can forget.
And "what does a runtime guard prove" becomes an explicit parameter of a query
rather than a fact baked into one traversal's arms, because that is the
distinction both the calendar admission and the overflow check need and only
one of them had.

It is also the natural first piece of the compiler to write in Java: a sealed
interval type and its lattice operations are pure data, with no Catalyst
surface to speak of.

**The admission check.** Every shape the compiler admits or declines today is
admitted or declined identically - the compiler suite's decline reasons and
fused shapes are the oracle, unchanged - and the differential's fusion
classification does not move on any committed query. Then one new property
test the current code cannot pass: over randomly generated IR, the interval a
node reports contains the value the reference evaluator computes, for every
lane pattern. If that test is not written, the task has not been done, because
it is the only thing standing where three bugs already stood.

### 2.16 Lane type as a parameter (task 85)

**The observation.** `INT_VECTOR` appears **204 times** in
`VarkaLoopEmitter.java`, beside sixteen four-byte stride assumptions and
eighteen species references. Every one is a place int64, boolean or decimal
lanes have to reach, and milestone 5's own headline tasks - 28's lane-width
conversion and 29's int64 lanes - are blocked behind exactly this.

The IR does not carry a lane type at all: `ColumnRef(int ordinal)` names a
column and nothing else, and the emitter supplies int32 by assumption. That
worked while every node was int32 and will not survive the second lane.

**The forcing function, which is cheap and already wanted.** Year-month
intervals are int32 months - the *same* physical lane as DATE and INT (the
owner's standing interest, `PLAN_MILESTONE_4.md` 2.x). Three logical types
over one physical lane is precisely the case that decides the design question:
it shows that the lane belongs to the node's physical representation and not
to its Spark type, which is also what `SCOPE_MILESTONE_6.md`'s "several
representations per logical type" will need. So that type can land before this
refactor and be its test rather than wait behind it.

**The option space.** (a) Parameterise the emitter on a lane descriptor -
vector class, species field, byte stride, load and store descriptors - and
switch on it at the sites that genuinely differ. (b) Generate a per-lane
emitter from a template. (c) Duplicate the emitter per lane. The count above
is the argument: of 204 sites most are mechanical and roughly twenty carry
real per-lane behaviour, which favours (a); (b) and (c) both re-create the
"two copies drift" failure this milestone is trying to remove. Measure before
committing, since (a) risks a megamorphic descriptor call in the hot path and
that is a benchmark question, not an argument.

**The admission check.** The int32 lane's emitted bytes are unchanged - the
pinned oracles again - and a second lane type reaches the same green
differential and fuzz matrices as the first, at both vector widths. The fuzz
suite's reachability test, which today asserts the generator can build every
node type in the sealed hierarchy, is widened to node type times lane type;
that test is what will fail when a later type arrives without an arm, and it
is the cheapest thing in this whole section.

### 2.17 One operand admission, stated once (task 86)

**The observation.** Four near-copies decide whether an expression may be an
operand: `intOperand`, `compileIntOperand`, `compileOffset` and `compare`'s
own `operand`. Task 63's review found them independently and called them
duplicated. Worse, the emitter restates their conclusions in four
`require*Shape` helpers, and those are a second, independent statement of the
same rule.

They drifted, exactly as a second statement does. Task 63 widened
`compileOffset` to admit arithmetic but `requireDayOffsetShape` admits only
three node kinds, so `date_add(d, weekday(d2) + 1)` - which lowers to task
57's `DayOfWeekIso` - was accepted by the compiler, marked fused in EXPLAIN,
and then refused at emit time, where the evaluator turns the refusal into a
silent per-batch fallback. That is the ghost fallback `sql/varka/AGENTS.md`
forbids, and it shipped in the PR until a review found it.

**The shape.** One admission function taking what the position accepts, and an
emitter check *derived from the same table* rather than written beside it -
so that widening the compiler either widens the emitter or fails to compile.
The check's fail-fast value is worth keeping; what is not worth keeping is
stating the rule twice in two languages.

**A widening this task carries, added 8 September 2026 from task 79's admission
check.** `compare`'s `operand` admits an int *literal* - which is what makes
`month(d) = 6` fuse - and sends everything else to `compileNode`, whose value
leaf is `DateType` and the year-month interval, so a bare `IntegerType` column
in predicate position declines. One case for an `IntegerType` `BoundReference`
fuses `CASE WHEN m > 0 THEN d ELSE d2 END` and
`CASE WHEN m >= -1000 AND m <= 1000 THEN add_months(d, m) ELSE ... END`,
verified by patching the compiler and reverting; `PLAN_TASK_79.md` 2.2 has the
IR.

It belongs here rather than in a row of its own precisely because it is a
widening of one of the four copies. Doing it standalone is the move that
produced the ghost fallback this task exists to prevent, and the reasoning that
it is safe standalone - `Compare` takes arbitrary IR operands, both lanes are
int32, so the emitter probably needs nothing - is the same shape of reasoning
that was wrong last time. Under this task the table decides and the enumeration
test proves it. It is also the natural first exercise of the unified table: a
position gains a kind, and nothing else in the file has to be touched for the
emitter to agree.

`BETWEEN` comes along for free and needs no arm of its own, which is worth
saying because the first reading of it said the opposite. `Between`'s
replacement is `With(input) { ref => And(...) }`, and the compiler has no arm
for `With`/`CommonExpressionRef` - so `dev/varka_emit.sh` declines it. That tool
resolves names and functions and nothing else; it never optimizes, and
`RewriteWithExpression` inlines a binding whose child is `CollapseProject.isCheap`,
which an `Attribute` or `BoundReference` is. `d BETWEEN <lit> AND <lit>` fusing
whole at 7.96x in task 62's run is the standing proof. A `BETWEEN` over an input
`isCheap` refuses is a different question - the rewrite hoists it into a
`Project` rather than inlining - and nobody has looked at it.

Int arithmetic in predicate position stays out and where task 63's comment put
it, since a widened *leaf* is not a widened *tree*.

**The admission check.** A test that enumerates the operand positions and,
for each, asserts that the set the compiler admits and the set the emitter
accepts are the same set - the assertion whose absence let the drift above
ship. Then every decline reason in the compiler suite unchanged *except* the
one the widening above removes, which is the single shape this task is allowed
to move from residual to fused, and which its own test names.

### 2.18 The epilogue is the one method no budget bounds (task 87)

*Added 8 September 2026, from a 35-million-iteration fuzz run. **Absorbs
milestone 4's row 44, "the epilogue's size", on 11 September 2026**: that row
asked for a size ladder that can see the problem - 4095 and 63 rather than only
4096 - and the epilogue measured against `HugeMethodLimit`, with the mechanism
chosen on the numbers. This section has the same defect at a harder threshold,
with a one-iteration reproducer, and the mechanism it needs - partitioning the
epilogue the way `GROUP_BUDGET` partitions the loop - is the one row 44 would
have had to choose. Two rows would have designed one partitioning twice. Row
44's size ladder and its `HugeMethodLimit` measurement become requirements
here: the 65535-byte cap says the epilogue must be split, and
`HugeMethodLimit` (8000 bytes, past which C2 declines to compile at all) says
how small the pieces have to be for the split to be worth anything.*

**The observation.** `VarkaLoopEmitter.emit` built a 67244-byte
`epilogueMasked` for a nested `make_date` tree and the Class-File API refused
it: the JVM caps a method at 65535 bytes. It reproduces as one iteration,
`-Dvarka.fuzz.seed=2026092800 -Dvarka.fuzz.only=73411`, and the tree that
produced it contains no arithmetic node at all - this is a date-op finding that
predates task 63 and was reached by volume rather than by anything new.

**Why the existing caps did not catch it.** There are three, and each bounds
something other than the bytes of the method that failed. `MAX_CHAIN_DEPTH` (16)
bounds the depth of one output. `MAX_FUSED_NODES` (64) bounds the distinct ops
in the whole kernel after CSE. `GROUP_BUDGET` (16) bounds the weight of one
*loop* method, and the emitter partitions the loop into `loopDense<g>` and
`loopMasked<g>` accordingly - which is exactly what `MAX_FUSED_NODES`' javadoc
leans on when it says the ops "are spread over loop methods of at most
GROUP_BUDGET ops each, so this caps the kernel, not any one compiled method".

The epilogue is not partitioned. `emitBody` is called once for it with
`group = -1`, so every group's ops land in a single `epilogueMasked`, and the
one bound that was supposed to keep a method small is the one that does not
apply to it. The claim quoted above is therefore true of the loop methods and
false of the epilogue, which is the sentence to correct along with the code.

Weight is also not bytes, and `make_date` is where the two diverge most:
`MAKE_DATE_WEIGHT` is 60 against a `GROUP_BUDGET` of 16, so a single
`make_date` already exceeds a group on its own and rides the `FUSED_CEILING`
escape. Sixty-four of the heaviest op the emitter has, all in one method, is
about 67KB - which is the number observed. A budget counted in weight cannot
bound bytes unless the weight-to-bytes ratio is bounded too, and across the op
set it spans more than an order of magnitude.

**What a user sees today, which is why this is not urgent.** Nothing wrong.
`VarkaKernelEvaluator.fusedRunner` catches the emission failure by name - its
comment already says "an IR shape past the emitter's caps" - logs a warning,
counts `numEmissionFailures`, emits an `EMISSION_FAILURE` fallback event, and
every batch takes the per-row path. Answers are the row engine's. The costs are
that the kernel is built and thrown away once per task, and that a shape inside
the documented caps degrades silently rather than being declined at compile
time with a reason, which is the outcome the ghost-fallback contract in
`sql/varka/AGENTS.md` asks for everywhere else.

**The task.** Either partition the epilogue the way the loop is partitioned, or
give the emitter a byte budget it can check before it hands the class to the
Class-File API - and in both cases turn the failure into a decline with a
reason rather than an exception the evaluator has to catch. The choice is worth
measuring rather than arguing: partitioning adds a call per group to a body that
runs once per batch, and the epilogue is the tail, so the per-batch cost lands
on short batches hardest.

**The admission check.** The fuzz iteration above, as a pinned emitter test,
declining with a reason instead of throwing; every shape that fits today
emitting the same bytes, against the pinned line map and the `codeSize`
assertions; and `MAX_FUSED_NODES`' javadoc corrected to say which methods its
guarantee covers.

### 2.19 An exact division through double lanes (task 88)

*Added 9 September 2026, from task 68's admission check.*

**The observation.** Task 26's whole design, and task 65's replacement for it,
rest on one absence: `VectorOperators` has no multiply-high on any lane type, so
an exact Granlund-Montgomery magic is not expressible on int lanes. 2.7 takes
that as given and routes around it by widening the dividend to int64, where a
64-bit low product is enough.

There is a second route, and nothing in this repository has considered it: widen
to *double* lanes instead. `(double) v` is exact for every int32, IEEE
multiplication is correctly rounded, and `D2I` narrows by truncating toward zero
- which is Java's `/` exactly. So `trunc((double) v * (1.0 / d))` is a candidate
lowering for `v / d` with no magic constant, no round-down correction and no
range restriction.

**Why it is exact, which decides whether this is worth a task at all.**
If `d` divides `v` the quotient is an integer under 2^31, exactly representable,
and the multiply is correctly rounded to it. If it does not, the true quotient
`v / d` has denominator `d` in lowest terms, so it lies at least `1 / d` from
every integer; the floating error is at most about `2^31 / d * 2^-52`, which is
`2^-21 / d`. Truncation therefore lands on the same integer for every divisor
below roughly 2^21. The margin does not thin out at the top of the range - it
scales with the quotient - which is the difference from an int-lane magic, whose
exact range is a fixed fraction of the type's.

Checked as well as argued: over structured and random int32 dividends, for
divisors 12, 3, 7 and 100, both `(double) v / (double) d` and the faster
`(double) v * (1.0 / d)` matched Java's `/` on every case, and an `I2D` /
multiply / `D2I` round trip runs on this machine's preferred species - sixteen
int lanes to two eight-lane double halves.

**What it would delete, if it wins.** The same list 2.7 offers - the round-down
magics and their correction carries, the `NARROWED` variant, `VarkaChrono`'s
range constants - but without task 65's precondition, since it needs no int64
lane and therefore none of milestone 5's lane-width work. It also removes the
range bound from `extract(YEAR FROM ym)`, which task 68 deferred for exactly
that bound (`PLAN_TASK_68.md` 2.2: the int-lane magic for `/12` is exact over
0..49,151, about one forty-thousandth of a year-month interval's range). It does
not rescue `extract(MONTH FROM ym)`, whose output is a `ByteType` Varka cannot
emit; that is task 89's other blocker and no division removes it.

**Why it is an A/B and not a decision.** It costs what 2.7 costs and possibly
more: half the lanes, two conversions in and two out per int vector, and a
double multiply rather than an integer one. 2.7's own estimate for the int64
route is "a small throughput loss bought with a large simplification"; this one
has the same shape of cost and must be measured against both the shipped magic
*and* task 65's widening, on the same shapes, before either is chosen. Three
arms, one benchmark.

**The admission check.** The exactness argument above, verified exhaustively
rather than sampled - for `/12` over the whole int32 range, and for the calendar
divisors 146097, 36524, 1461 and 365 over the dividend ranges
`emitChronoPrefix` actually produces - by a committed script beside
`verify_long_lane_magic.py`, which is the precedent. Then the op counts for one
extraction under each of the three lowerings, from `dev/varka_emit.sh --table`,
before any measurement is taken.

**What would reject it.** A divisor at or above 2^21 (none of Varka's are); a
lowering that needs the *remainder* at full width, where the double route gives
the quotient and the remainder costs a multiply back; and the conversion cost
exceeding the magic it replaces on the shapes that matter, which is what the A/B
is for. `Float16` and single-precision floats are not candidates - 24 mantissa
bits cannot hold an int32 dividend - so this is a double-lane question only.

### 2.20 The year-month interval divisions (task 89)

*Added 9 September 2026, split out of task 68 by its admission check.*

**The observation.** Of the eight expressions `PLAN_MILESTONE_4.md` 2.33 gave
task 68, two need division by a constant: `extract(YEAR | MONTH FROM ym)` and
`ym / num`. `PLAN_TASK_68.md` 2.2 to 2.4 found three things about them the
section had assumed away. The exact int-lane magic for `/12` is the one the
emitter already has, `MONTH_ARITH_M`, and it is exact over `0..49,151` - about
one forty-thousandth of a year-month interval's int32 range. That magic
computes a floor, and `extract` is Java's `/`, which truncates; they differ on
every negative with a remainder. And `ym / num` rounds `HALF_UP`, ties away
from zero, which needs the remainder as well as the quotient, over an exact
range that depends on the divisor.

A fourth blocker is not about division at all: `extract(MONTH FROM ym)` returns
a **`ByteType`** (`ExtractIntervalPart[Int](ByteType, getMonths, ...)`), and
Varka has no byte lane and no `ByteType` arm in `allocateVector`. A perfect
division still leaves that expression un-emittable.

**Why it waits.** Two routes in this milestone remove the range bound outright:
task 65's int64 widening and task 88's double lanes. Building a range-guarded
int-lane version first means building the thing either exists to delete, and
then owning both. So this task is sequenced after whichever of 65 and 88 the
three-arm A/B chooses, and takes its division from that.

**The task, once a route is chosen.** `extract(YEAR)` as the chosen division
with a truncation correction on the negative side; `extract(MONTH)` as
`months - 12 * q` over it, *once a byte output exists* - which is its own
question and may leave `extract(MONTH)` residual for longer than its twin;
`ym / num` for a literal `num` as the chosen division plus the `HALF_UP` step,
with a power-of-two `num` taking the shift; `ym / col` declining, since the
divisor is then not a constant.

**The admission check.** The truncation and `HALF_UP` corrections verified over
the full int32 month range against `IntervalUtils.getYears`, `getMonths` and
`IntMath.divide`, by a committed script; the byte-output question answered - an
`IntVector` narrowed at the store, or a decline with a reason - before
`extract(MONTH)` is attempted; and a throughput pair per shape against the row
engine, since these are new lowerings and not, as task 68's group A is, old
kernels under a new type.
### 2.21 The benchmark files are not reproducible run to run (task 90)

*Added 9 September 2026, from the investigation task 79's section 9 asked for.
The band this section asks for was measured on 10 September and is committed
beside each results file; the numbers below come from three runs and understate
it, and the row records what ten runs per width give instead.*

**The observation.** Two regenerations of `VarkaEmitterParityBenchmark` with no
code change between them disagree, on rows nothing touched. Pinned to one core
complex the median case moves 1.6%, but 73 of 211 cases move more than 3%, 22
move more than 10%, and the worst reaches 26%. Unpinned the worst reaches 75%.
Task 79's 9.1 refused to commit a regeneration on that evidence; this is what
the evidence turned out to be.

**What it is not**, each eliminated by measurement rather than argument:

* *Within-run noise.* Across all 207 cases of a committed run, the ratio of the
  average iteration to the best iteration has a median of 1.007 and never
  exceeds 1.5. Every case is tight inside its own run - the harness reports the
  best of tens of thousands of iterations, and the average is the same number.
  So the compiled code is in place before measurement starts, which rules out
  compiler-queue saturation, code-cache exhaustion and deopt storms.
* *The clock.* Sampling `scaling_cur_freq` through six runs of one case gives
  5.08 to 5.14 GHz, a 1.2% spread, while the throughput of those same runs
  moves 31%. At a fixed clock, the work per cycle is what changed.
* *Address layout.* Disabling ASLR with `setarch -R` does not narrow the
  spread; interleaved, the randomised runs were tighter than the fixed ones.
* *Contention.* The machine is idle; nothing but the JVM is on the pinned cores.

**What it is, and task 32 got here first.** A per-fork JIT and code-layout
lottery: the same bytecode produces slightly different machine code and
placement in each JVM. `-Xbatch`, which removes the compilation timing races,
narrows one case's spread from 49% to 14% and does not remove it.

`PLAN_TASK_32.md` 11 investigated this and went further on the JIT side than
this section does. It found the upstream report - JDK-8380195, "Vector API
produces bimodal performance - nondeterministic C2 intrinsification across JVM
forks", roughly 2x across identically configured forks, closed **Not an Issue**
in April 2026 - and it tested and refuted the obvious levers: buffer alignment
raised to 64 bytes, which pinned the *slow* mode rather than the fast one;
`-XX:-UseOnStackReplacement`; `-XX:LoopUnrollLimit` at 250; and forced
inlining. Its conclusion was that whatever picks the mode is inside C2's code
generation and is not reachable from those levers, and it measured one kernel
21 times at 128-bit, landing the fast mode 4 times. Nothing here contradicts
it. What this section adds is the size of the effect across a whole file, the
proof that it is not within-run, and the machine half below.

**The machine half is new, and it is the fixable part.** The Ryzen AI 9 HX 370
is heterogeneous: four Zen5 cores at 5.16 GHz and eight Zen5c at 3.29 GHz, on
two separate 16 MB L3 slices. An unpinned thread is rescheduled between them
during a 40-minute run, so each case is measured wherever it happened to be.
The clock alone is worth 1.57x - and the datapath is not the difference, since
the measured ratio of 1.5632 matches the clock ratio of 1.5680 to 0.3%, so both
core types retire this code at the same rate per cycle. Migration also costs L3
residency where the working set fits one slice: `fused, depth 1` reads 154 GB/s
resident and 37.7 GB/s after, the 4x collapse that started this. Pinning to the
fast complex takes the worst case from 75% to 26%. That is done, in
`dev/varka_bench_regen.sh`, and recorded in each provenance file.

**What follows, and it is mostly reassuring.** An A/B whose two arms sit in the
same run is sound: they share a JVM, a layout and a clock. Every A/B in this
project is built that way, which is why task 79's arm-context pair read 0.5%
and 1.4% across two runs whose absolute rates disagreed by 75%, and why task
67's interval pair was trustworthy. The project's *decisions* are not in
question wholesale. What is in question is a number compared against a previous
run, below the band - and the regeneration diff's "moved by at least 3%" report
is below the band for every memory-bound row.

**One decision does rest on such a diff**, and it is named here so it is
re-tested rather than inherited: `PLAN_TASK_63.md` 9.7 attributes 26.1%
(14706.5 to 18542.2 M rows/s at AVX-512) to removing a dead local slot, from a
comparison of two *unpinned* regenerations, and 2.12 above narrows task 82 to
"a 128-bit task" on the strength of it. 26.1% is at the very top of the band
measured here, from runs where the worst case is 75%. The mechanism may be real
- the emitted bytes did change, and a dead local does change register pressure
- but the magnitude is not evidence until it is re-measured pinned.

**The task.** Establish the band per file with `dev/varka_bench_repeat.sh`,
commit it beside the results, and make the regeneration diff report against the
band rather than a flat 3%. Then decide, with numbers, whether the absolute
rates are worth buying back: N forks per case with a median, which is what JMH
does and would cost N times a regeneration. The alternative is to stop treating
absolute rates as comparable across runs and let the A/Bs carry every claim,
which is close to what the plans already do in practice.

**The admission check.** The band measured for the parity, arithmetic and
throughput files, three runs each, committed; and one shape whose A/B is known
tight - task 79's arm context - shown to stay tight across those same runs
while its absolute rate wanders, which is the evidence that the two kinds of
number deserve different treatment.

### 2.22 A guard bound the shift above it chooses (task 91)

*Added 10 September 2026, from task 69's outcome.*

**The observation.** Task 69 gave the upward direction its own limit and three
of four conservatively-declining shapes fused again. The fourth did not:
`weekofyear(date_add(d, off))` and its `yearofweek` twin, which
`PLAN_MILESTONE_4.md` 9 named as the ordinary query shape the debt was really
about. `ThursdayOf` shifts `+-3`, and the `-3` side reaches
`NARROW_MIN_DAYS - 3`, where the narrowing is not conservative but genuinely
undefined: `w = days + NARROW_BIAS` goes negative and `(w * NARROW_ERA_M) >>>
NARROW_ERA_K` reads it as about 4.29e9. No headroom exists below the way it
did above, because `NARROW_MIN_DAYS` is exactly `w = 0`.

**The lever is the guard, not the constant.** Task 52's runtime guard on a
column-offset `date_add`/`date_sub` compares its result against
`[NARROW_MIN_DAYS, NARROW_MAX_DAYS]` and falls the batch back when it leaves.
Those two bounds are already *parameters*: `emitRangeGuard` takes `lo` and
`hi`, because task 60 reuses the same block for the month count against
`MONTH_ARITH_MIN/MAX_MONTHS`. Only `emitAndValidatedOp`'s call site hardcodes
the day pair. Nothing requires them to be *those* constants: the compiler
knows, from `dayRange`, exactly how far the subtree above the producer shifts
the day, and
could ask the guard to enforce `[NARROW_MIN_DAYS + 3, NARROW_MAX_DAYS]` for a
`ThursdayOf` consumer, or `[NARROW_MIN_DAYS + 365, ...]` for a `trunc` one. The
run-time cost is identical - the same compare against a different immediate -
and the compile-time decline becomes a batch fallback only for the batches that
actually contain a day in the last three (or 365) of a thirteen-thousand-year
window, which is to say never, in practice.

**Why it is worth a row.** It closes the debt register entry task 69 swept only
half of, it turns two more ordinary shapes from residual into fused, and it
generalises: every downward-shifting consumer over a guarded producer becomes
admissible by the same rule, which is the whole `trunc` family. It also
subsumes the asymmetry task 69 shipped, since the upward side is the same idea
with the shift added to the ceiling instead of the floor.

**Why it is not free.** The guard's bound stops being a constant of
`VarkaChrono` and becomes a per-node property the emitter reads, which touches
`VarkaEmitOptions`' canonical form and the shape key: two subtrees identical
except for the shift above them must not share a kernel. That is the same
question task 84's value-range lattice answers for `dayRange` and `intBound`,
so this task belongs after 84 and should take its interval representation
rather than inventing a second one.

**The admission check.** Half of it is already answered: `NARROW_MIN_DAYS`
appears three times in `VarkaLoopEmitter`, and only one is a call site - the
guard block itself is parameterised. What remains is a `VarkaEmitDump`
op-count diff between a guard at the floor and one three days above it,
showing the kernel differs by an immediate and nothing else, and the shape key
shown to separate two otherwise-identical subtrees whose shifts differ, both
before any downward consumer is admitted.
### 2.23 The validity write, keyed on the bit layout (task 92)

*Added 10 September 2026, from task 47's measurement.*

**What task 47 established.** The per-group validity write is a
read-modify-write, and at four lanes a validity group is half a byte, so two
consecutive groups rewrite the same byte and serialise on it - the regime
task 76 found its helper choice inverting inside. Task 47 built the writer
that removes it: the bits accumulate in a register and the whole 64-bit word
is stored, with no read. Over ten pinned runs it is a **6 to 9% win at 4
lanes** at one and two writes, and the inversion disappears with it. It is an
**11 to 20% loss at 8 and 16 lanes**, where a group owns whole bytes and
there was no chain: an eight-byte store plus an accumulator, a mask, a shift
and a branch is more work than a one-byte read-modify-write whose helper
already inlines.

**The rule that follows, and why it is not task 76's rejected one.** Task 76
declined a rule keyed on the write count because it would be two thresholds
fitted to one machine. This is one condition and it is not fitted: **a
validity group smaller than a byte**, which is `lanes < 8` - 2 and 4 lanes,
and nothing else, forever, because a group is `lanes` bits. It is the
mechanism written down rather than a number tuned. `widthSpecialised` already
reads `analysis.lanes`, so the plumbing exists.

**Why it is a task and not a line of task 47.** Not because the rule is
academic - this section said that at first and it was wrong, corrected here
rather than tidied away. `SPECIES_PREFERRED` for an int lane is 128 bits on
every NEON-only aarch64, Apple Silicon among them, and on x86 without AVX2:
four lanes, which is exactly the regime task 47 measured its 6 to 9% win in,
and the reason this project commits 128-bit companion results files at all.
The rule is a real target's default, not a `MaxVectorSize` flag's.

What it is a task for is confirmation. Task 47's four-lane numbers come from
`-XX:MaxVectorSize=16` on an x86 whose preferred width is 16 lanes, which
simulates the lane count and not the store behaviour of a machine that has
only 128-bit registers. A default worth 6 to 9% on a whole class of hardware
should be measured on that hardware once. That makes this task's admission
check a hardware question, and it shares one with `PLAN_MILESTONE_4.md` row
62's pinned runner - though not the same machine: 62 wants a wider one than
this laptop and this wants a narrower one.

**Two more items ride with it, because they share a ladder run.** Option B of
`PLAN_TASK_47.md` 3.1 - store once per word rather than once per group - is
where the two widths that lose might be recovered, since what they pay for is
the wider store and not the removed read; it is a branch per group against
three stores in four saved at 16 lanes. And `PLAN_TASK_47.md` 3.4's driver
item, the masked driver's dead null-state prologue (`PLAN_TASK_70.md` 9.5),
which pays per batch rather than per group and is the whole of the remaining
gap on a 64-row batch at AVX-512 - 1080.1 against the dense 1549.6.

**One thing to settle first, and it is cheap.** Task 47's ladder has a step
at three writes that neither task's model predicts: both per-group arms fall
away sharply there and the word writer does not, so its advantage reads 22 to
38% at every width against 5 to 9% at its neighbours. The emitted code is
ruled out - all four rungs are one loop method growing ~130 bytes per write,
asserted in `VarkaLoopEmitterSuite`. The leading hypothesis is task 46's own
mechanism, the caller's node count crossing C2's inlining cutoff so one more
OR call stops being inlined. `-XX:+PrintInlining` on the k=2 and k=3 rungs
answers it in one run, and until it does, no rule may be fitted across k=3 in
either task's table.

### 2.24 Instruction-level parallelism (task 25, item 13)

The debt register's rule applies: a prediction goes in writing before the
first measurement, and the honest null hypothesis is that C2 plus the
out-of-order engine already collect most of the available overlap on a 16-op
body, so K pays only on the long chains. The three confounders move together,
never one at a time: K, the broadcast strategy (pinned locals collapsed
throughput 7x at ~32 broadcasts, so unrolling and pre-broadcasting *compete*),
and `GROUP_BUDGET`, which unrolling multiplies against a ~1 ms-per-vector-op
C2 compile (**confirmed by task 43**: 1.1 ms per op at AVX-512 and 2.0 at
128-bit, measured across a 20-to-248-op ladder - see `PLAN_TASK_43.md` 8.2, and
the reconciliation note in 2.16). The candidates are the shapes that are compute-bound and already
carry a committed number to beat: `dayofweek` (a 20-op fold), `CASE WHEN` on
an unpredictable condition, and the depth-8 chain. Row-consumer shapes are
bounded by the ~25 ns/row read-back floor and the filter path by compaction;
no kernel-side ILP moves either, so neither is a candidate. One negative
result worth carrying in: a 2-way unrolled add kernel over a misaligned
buffer still lost 50-60% to the aligned case (section 8's buffer-alignment
entry) - unrolling does not incidentally hide the alignment penalty, so this
task's outcome and that entry's are independent questions, not one deferring
to the other.

Open question 4 is answered, ahead of the task and with the broadcast
confounder held fixed at "emitted per use" so it does not contaminate the
result (`VarkaUnrollFactorBenchmark`, committed results file in
`sql/varka/engine/benchmarks/`, four runs total including two taken after
merging task 24's PR and enabling the machine's performance mode, neither of
which changed a conclusion): on an 8-op chain, K = 1, 2 and 4 are flat at
both vector widths, on every run - within 4% either way, no consistent
winner. The honest null hypothesis holds exactly on a body this short. On a
20-op chain (the `dayofweek`-length candidate), K = 2 wins reproducibly at
both widths and on every run - +2.6% to +9.2% at AVX-512, +1.2% to +6.2% at
128-bit - and K = 4 adds no further, consistent benefit over K = 2 on either
width (the sign varies run to run, always within a few percent). So "K pays
only on the long chains" is confirmed rather than merely predicted, and the
planner version below should cap K at 2 rather than search further: 4 was
measured to buy nothing on the one shape where unrolling helped at all, while
still paying `GROUP_BUDGET`'s doubled cost over K = 2. This measurement is
also where a real methodology trap surfaced and was caught: comparing K = 1
(straight-line unrolled source, the shape a real emission carries) against an
earlier K = 2/4 written as a small constant-bound runtime loop over the op
index produced a spurious 30-60% *loss* at K = 4 - an artifact of the loop
shape, not of unrolling. Rewriting K > 1 as straight-line interleaved code,
matching K = 1's shape exactly, is what produced the numbers above (`SKILLS.md`
carries the general lesson).

Re-measured in forked JVMs after the harness debt closed (the results file's
header says how): the same picture. Depth 8 flat at both widths; depth 20
gains +4.4% from K = 2 at AVX-512 and +4.3% at 128-bit by min, and K = 4 over
K = 2 is +3.4% at one width and -1.9% at the other. The conclusion did not
depend on the harness, which is what a plain add/sub chain should show.

If a factor above 1 pays, the deliverable is the planner version: the emitter
already knows the DAG's live-temporary count per lane group, so K is chosen
per shape, and a shape whose live set fills the register file declines to
unroll. That version exists only because the loop is generated - it is the
whole reason this item belongs to Varka rather than to hand-written kernels.

Task 24 goes first because an unrolled body's remainder is `K * lanes - 1`
rows, so the tail question and the unroll question share a harness (open
questions 4 and 5) - and the batch-size knee sweep (question 6) rides the same
harness for the wide-shape case. Whatever the outcome, the `SKILLS.md`
unrolling bullet is rewritten with the numbers, as it promises itself.

### 2.25 Output order for prefix affinity (task 72)

Added 7 September 2026, from B2's one pinned limitation (`PLAN_TASK_32.md`
10.2 and 7.6). `groupOutputs` is greedy in output order, so in
`year(d), year(d2), month(d)` the month is offered to the group holding
`year(d2)`, whose prefix it cannot reuse, and forms a third loop method that
recomputes the decomposition of `d` the first method already ran; adjacent,
the same three outputs take two methods. B2 pinned that as a limitation
rather than fixing it, because the driver's output order is the projection's
and other things key on it.

**The admission check, which is most of the task.** Nothing in the emitter
requires a group's output indices to be contiguous: `groupOutputs` already
returns lists of indices, each loop method takes its list, and the driver
calls the methods in group order while the destinations stay indexed by
output. So the change may be a two-pass grouping - gather each calendar
output into the group whose prefix it reuses, wherever that group is, then
fill the rest greedily as today - with no change to the evaluator's per-output
vectors or to `VarkaDebugInfo`'s line map, both of which key on the output
index and not on which method computes it. The check is to establish that:
every consumer of a group walked, the line map and the pinned oracles asserted
unmoved, and the differential suite run with a deliberately permuted grouping
before the rule is written. If a consumer does depend on contiguity, the task
says which and stops, and the debt entry stays.

**What it is worth.** The shapes B2 measured, with one output between the
siblings: `year(d), year(d2), month(d)` against `year(d), month(d), year(d2)`
in the parity harness, at both widths, with the prediction registered that
the permuted grouping matches the adjacent one within noise. Date columns in
a projection are usually adjacent, so this is a small task with a bounded
win; it is a row because the limitation is pinned in a test that should be
flipped by a change, not silently.

### 2.26 A stopping rule for the guard walk (task 73)

Added 7 September 2026, out of task 70's fuzz run (see the debt register).

**The observation.** `Analysis.collectGuardedProducers` calls
`collectColumnOffsetProducers(chronoChild(node), ...)` for every `isChrono`
node, and that walk descends the entire subtree, adding every
`AddDays`/`SubDays` with a column offset it meets. It has no stopping rule.
So in `month(dayOfWeek(date_add(date_add(d, off), off)))` the producer is
guarded against `[NARROW_MIN_DAYS, NARROW_MAX_DAYS]` on its own value, when
the value `month` actually decomposes is the `dayOfWeek` result and is always
1 to 7. `weekday`, `dayofweek_iso`, `weekofyear` and `datediff` behave the
same way: each bounds its output, and none of them stops the walk.

The guard is not wrong, it is unnecessary. A batch whose producer leaves the
range is declined and recomputed on the row engine, so the answers are right;
what is lost is the fusion.

**Why the task starts with an admission check, like task 69.** Through the
compiler this shape never reaches the emitter. `dayRange` has no rule for a
mod-7 node, so it returns `Unknown`, and `checkedForCalendar` declines the
entry at compile time with "day producer the calendar range analysis does not
bound". The entry is residual either way, and only a caller that builds IR
directly - `VarkaIrFuzzSuite`, and any future planner-side rewrite - reaches
the over-guard. So the first question is whether any SQL shape observes the
difference at all. If none does, the honest outcome is to record that and
close the task, exactly as task 69's section 2 is allowed to.

If the check finds the shape does matter, the two halves have to move
together, and the compiler half is the one that changes what a user sees:
`dayRange` would gain a rule that a mod-7 node re-bases its child to a known
small interval regardless of what the child's interval was, which is the same
observation stated on the other side of the compiler. Then
`year(dayofweek(date_add(d, off)))` fuses instead of going residual.

**What closing the emitter half takes.** A stopping rule on the walk: descend
only through nodes that pass a day through to the decomposition - `AddDays`,
`SubDays`, `Greatest`, `Least`, `IfElse`, `NextDay`, `ThursdayOf`, `LastDay`,
`AddMonths`, `TruncDate`, `MakeDate` - and stop at any node whose output is a
bounded quantity of its own: the mod-7 family, `DateDiff`, `WeekOfYear` and
every calendar field extraction. The set is the same one `dayRange` would
need, which is the argument for taking both halves in one task rather than
letting the two analyses drift apart again - drift between them is what this
finding is.

**How it was found, which is part of what it is.** Not by reading the
emitter: by running `VarkaIrFuzzSuite` at 1.84 million iterations across
twenty jobs on 7 September 2026, where every one of the twenty stopped on a
shape of this form. At the shipped budget of 300 iterations it is
unreachable. The suite's own half of the mismatch - a `Gen.bound` of 7 on a
mod-7 node hid the producers beneath it from the `chronoBound` check - was
fixed with task 70, because the fuzzer is unusable past about ten thousand
iterations without it.

### 2.27 String-column compaction that keeps the Arrow layout (task 80)

Added 7 September 2026, out of task 59's review (see the debt register).

**The observation.** A derived int32 leaf (task 59's weekday, task 61's trunc
level) reads its source through a `VarCharVector`. A fused Varka filter ahead
of the projection hands it a compacted batch whose string columns went through
the generic on-heap compaction, so the leaf's source is no longer Arrow-backed
and the projection refuses the batch: a stacked `next_day(d, s)` over a Varka
filter is counted in `numFallbackBatchesNonArrow` and computed on the row
path, with correct answers.

**Why it is a task now.** Every future derived leaf over a string column
inherits it, and milestone 6's item 3 puts string columns under filters and
group keys, so the shape stops being a corner exactly when that milestone
starts. Fixing it late means fixing it under a benchmark rather than under a
differential.

**The design.** A string-column compaction that keeps the Arrow layout,
writing offsets and data buffers rather than materialising rows - task 21's
`filterCompact` for fixed-width columns is the pattern, and the shape of the
work is one pass to sum the selected lengths, one to write offsets, one to
copy bytes. Measured on the task 59 differential's own fixture, with the
metric as the gate: the stacked shape must stop counting
`numFallbackBatchesNonArrow` at all.

### 2.28 Statistics-directed guard selection (task 64)

Added on 4 September 2026 from a question the owner asked about task 52's
runtime guard: the input batch, or the node before, may already know the
range of a column, and then the per-lane check is work the batch has proved
unnecessary. Task 52 (#115) puts a per-lane range check on a `date_add` whose
offset is a column and whose result a calendar node reads, at a measured 5-15%
of that kernel null-free and 13-14% with mixed nulls (`PLAN_TASK_52.md` 11).
The check exists because the compiler cannot bound a column at compile time;
a batch can.

**Three sources of the bound, in order of plumbing.** First, compute it: a
vector minimum and maximum over the offset column before the kernel runs,
which task 56 already does for the interval bound through
`IntRangeOps.allWithin` and which the throughput benchmark could not measure.
A date column holds the contract range (`CONTRACT_MIN_DAYS..CONTRACT_MAX_DAYS`),
so if every offset of the batch lies in `[NARROW_MIN_DAYS - CONTRACT_MIN_DAYS,
NARROW_MAX_DAYS - CONTRACT_MAX_DAYS]` no lane of `date_add(d, off)` can leave
the calendar range and the batch runs the **unguarded** kernel - the class
task 52's option already emits, since the shape cache keys on options. Second,
read it: the cached-batch serializers, the Arrow one included, compute count,
null count, lower and upper bound per column for every cached batch, and use
them today only to prune batches under a filter at the scan; the fork owns the
serializer and the scan-to-batch iterator, so the bounds can ride with the
`ColumnarBatch` to the exec node, where the check costs nothing - the null
count already travels that way for the null-free fast path. Third, the file:
Parquet row-group and page statistics, which the Arrow-native datasource
(`SCOPE_MILESTONE_6.md`, item 8's neighbourhood) is the place to attach.

**The design, in two steps.** Step one, the pre-pass: the evaluator, for each
compiled projection whose plan carries a guarded producer, runs
`IntRangeOps.allWithin` over the offset input with the bound above and picks
the unguarded kernel when it holds, the guarded one when it does not - both
from the shape cache, both already tested by task 52's suite, so the change is
in `VarkaKernelEvaluator` alone and the emitter does not move. The in-kernel
guard stays as the answer for the batch whose offsets say "maybe", which in
the corpus is never. Step two, the statistics: `ArrowCachedBatchSerializer`'s
per-batch bounds attached to the batch it deserializes, read by the evaluator
before it computes anything, so the pre-pass is skipped when the bound is
already known; the same channel answers task 56's interval bound for free and
opens batch pruning inside the fused pipeline later. Both steps behind their
own switch, with the pass and the lookup priced against the guard on the
parity benchmark's `year(date_add(d, off))` pair and on the throughput
benchmark's `date_add(d, i)` control.

**What it does not change.** Task 52's compile-time analysis is what says
which producers need a check at all; this task decides per batch whether a
given one does. A batch with a far offset still declines, through the same
route, and the differential's far-offset fixtures hold that. Depends on #115
and on task 56's kernel, both on master before it starts.

**Why this moved out of milestone 4** (11 September 2026). The milestone's
remaining subject is task 62's closing measurement and the README written from
it, and this task cannot reach either. `DateSurfaceBenchmark`'s surface has 39
entries and exactly two carry a column offset: `date_add(d, i)`, which has no
calendar consumer and so is never given task 52's guard - the parity file's own
`task 52 control` pair is the proof, being the same number with the option on
and off - and `add_months(d, i)`, whose guard is task 60's count guard, which
3.2 of `PLAN_TASK_64.md` excludes by name. There is no `year(date_add(d, i))`
in the surface at all.

So this task changes no number the public table will show. Its own section 6
proposes *adding* a surface entry for the shape, and that entry should be
justified as coverage if it is wanted - "the surface should cover a calendar
node over a column offset" is a good reason; "so that this task has somewhere
to appear" is not, and conflating them would put a shape in the public table
because it flatters us.

Two smaller reasons point the same way. Landing it would move the parity and
throughput files, so after task 62 (B) runs the committed companions go stale,
and before it the closing measurement waits on a task that adds nothing to its
output. And the prize is shrinking on the width that matters: the requote of
`PLAN_TASK_64.md` 1 found the masked 16-lane row had already lost a third of
the guard's share to task 70's bitmap pass, and a genuine 512-bit datapath
makes the surrounding compute faster again while the guard stays two vector
compares.

It sits here beside row 82, which its own text says "shares its ground with
task 64".

## 3. Task breakdown

The rows as milestone 4's table carried them, task numbers unchanged. 28 opens
the milestone; 29 and 30 follow it; 39 and 49 wait on 29; 27 can run at any
point; 65 waits on nothing and is an admission check before it is a task; 66
follows task 32's B2 grouping decision; 74 and 75 follow #145 (task 70) and
nothing else, 75 being an admission check before it is a task; 81 follows 74.
83 to 86 are the engine refactors task 63 argued for (section 2.13), ordered by how
far the decision each touches sits from an exhaustive match rather than by size: 84
before 85, because the lattice is what makes a new lane safe and the lane parameter
only makes one possible - **the owner confirmed that order on 8 September 2026**,
against the alternative of taking 85 first to unblock tasks 28 and 29 sooner and
following it with 84, which would have ported today's two bound functions into a
third and let the next lane inherit the bugs task 63's review found. 83 and 86 are
independent of both and of each other.

| # | Task | Deliverables | Validation |
|---|---|---|---|
| 25 | ILP: the unroll factor as a plan decision (section 2.24). **Not started** and **moved from milestone 4** (11 September 2026), where nothing waited on it: its harness stopped measuring a degraded JIT state with PR #105, so its first job is re-establishing what it measures rather than measuring | The registered prediction, then the three-confounder matrix (K x broadcast strategy x `GROUP_BUDGET`) on `dayofweek`, unpredictable `CASE WHEN`, and the depth-8 chain; if K > 1 pays, per-shape K chosen from the live-temporary count the emitter already computes; the `SKILLS.md` bullet rewritten with the numbers; the batch-size knee sweep (question 6) on a wide fused shape | A committed number per candidate shape against its existing baseline; prediction scored honestly; no committed number regresses on shapes where K stays 1 |
| 27 | Boolean outputs | Mask-to-column materialisation (`toVector` against `blend`, measured); the bit-packed format decision at the Spark/Arrow boundary; three-valued rules holding at the output boundary | Differential over every null pattern - a null input never becomes false; `SELECT d > DATE '2000-01-01' AS flag` and filter-leftover boolean columns compile; committed number on one boolean-output shape |
| 28 | Lane-width conversion | The mixed-width loop-shape measurement (open question 2: narrowest-drive against part loops) on `cast(int AS long) + long`, committed before integration; `convert`/`convertShape` emission following the winner; numeric `Cast` and Catalyst's implicit promotions over the supported types | Differential on mixed int32/int64 trees at both widths; the loop-shape decision recorded with its numbers; no regression on single-width shapes |
| 29 | int64 lanes: `TimestampNTZ`, `bigint` | The second `LaneType`; `TimestampNTZ` comparisons, differences, literal arithmetic; `TimestampType` and `LongType` comparisons and diffs; range-narrowed magic constants for 1000000 and 86400 or a recorded decline; the field differential mode from task 22 | Every parity gate re-run at the long species and both vector widths; the halved-headroom number committed rather than discovered; zoned operations demonstrably declined, not wrong |
| 30 | ANSI integer arithmetic - **narrowed on 4 September 2026**: the int32 add, subtract, multiply and negate over fused fields, int columns and literals, with the ANSI overflow decline and the `try_*` validity form, moved into milestone 4 as task 63, **which shipped** (`PLAN_TASK_63.md` 9); what stays here is the rest | `/` (a double), `div` (task 29's long lane), `%` and `pmod` with the divide-by-zero rule, the int64 forms, and `Multiply` overflow through 28's widening where task 63's saturating check is not enough | The error-identity differential: same `SparkException`, same row, as the row engine under ANSI; `try_*` differential over overflow-dense and overflow-free data; committed number on the no-overflow path against Janino |
| 39 | `date - date`. **Planned** (`PLAN_TASK_39.md`), blocked on tasks 28 and 29 | The node, the int32-to-int64 conversion, the eight-byte output, and both overflow tests routed through task 26's decline channel rather than task 30's throw path; the legacy `CalendarInterval` variant declining. The int-to-long step is the two-part `convertShape` from the preferred int species, never a load through a half-width int species: two species of one lane type in one JVM turn the shared `IntVector` templates bimorphic and C2 keeps a heap box per loop iteration (`SKILLS.md`, "Every operator the plans rely on"), and the lane-width "tie" in `VarkaMilestone4MeasurementsBenchmark-jdk25-results.txt` was measured in exactly such a JVM | The overflow boundary exact in both directions (106751991 succeeds, 106751992 declines); Varka's exception identical to the row engine's, compared by running both; `datediff` unaffected; green at both widths, where an int64 lane holds a different number of rows |
| 49 | Exact civil-from-days in long lanes. **Planned in section 2.19** (PR #69; there is no `PLAN_TASK_49.md`), blocked on task 29 | The admission check first, over all 2^32 days against a long-arithmetic reference: exact magic division with a 64-bit low product and no correction carries, run for **both** decompositions - the three-division era/century/year form (146097, 36524, 365) and task 54's two-division Julian map (146097 on `4 * d + 3`, then 1461), which Ben Joffe's `fast64` shows reaching four multiplies for the whole date where Neri-Schneider needs seven; then the lowering, and the guard, the decline path, the `NARROWED` variant and `VarkaChrono`'s range constants removed with it. Verified before starting (`SKILLS.md`, "Every operator the plans rely on"): `LongVector.mul` by a constant compiles to one `vpmullq` on this CPU (AVX-512DQ with VL), not the three-multiply emulation plain AVX2 gets, and unsigned long compares are one `vpcmpuq` into a k-mask. Plan B if the 0.75x gate fails: Joffe's bucket technique for a guard-free int-lane total - `bucket = (d + 2^31) >>> 20`, reduce by `bucket * 1022679`, add `bucket * 2800` to the year - about 14 ops against task 26's `TOTAL` at 16 and without the deliberate wrap; his `article_2_l1` variant replaces two of those multiplies with an eight-entry offset table, one lane permute on a 256-bit int species | The exhaustive sweep as a committed opt-in test, at both widths; the parity `year` case measured against the shipped narrowed lowering in one run; declined on the record if the sweep disagrees anywhere or AVX-512 costs more than 0.75x |
| 64 | Statistics-directed guard selection (section 2.28). **Planned** (`PLAN_TASK_64.md`, requoted 11 September 2026) and **moved from milestone 4** the same day: the surface task 62 measures carries no shape that pays task 52's producer guard, so this task cannot change a number the public table shows | Step one: the evaluator runs `IntRangeOps.allWithin` over a guarded producer's offset column against `[NARROW_MIN_DAYS - CONTRACT_MIN_DAYS, NARROW_MAX_DAYS - CONTRACT_MAX_DAYS]` and picks the unguarded or the guarded kernel from the shape cache per batch; step two: the Arrow cache's per-batch column bounds attached to the `ColumnarBatch` and read before the pass, answering task 56's bound too; each behind a switch | The guarded kernel never runs on the differential's in-range fixtures and the far-offset fixtures still decline; the pass and the lookup priced against the guard on the parity `year(date_add(d, off))` pair, both widths, with a registered prediction that the null-free and mixed-null cost of task 52's guard is recovered; byte identity of the emitter |
| 65 | Joffe's `fast32` civil-from-days in int lanes. **Scoped in section 2.7** (5 September 2026); independent of 29 | The admission check first: the two source files transcribed into `sql/varka/papers` with reading notes; a committed script deriving a low-32-bit magic and its exact range per stage and sweeping the chain against `LocalDate`; the dependent-stage count against the prefix's. If admitted, an emit-option variant, the A/B beside the task 53 and 54 pairs at both widths, the register and the `HugeMethodLimit` ladder re-pinned, and the default chosen from the numbers | Exact over at least the narrowed range, or declined; a shorter dependent chain than the prefix's, or declined; the A/B at or above 1.0x at both widths, or the numbers go to the debt register |
| 66 | Second-level chrono fragments. **Scoped in section 2.8** (5 September 2026); after task 32's B2 grouping decision | `FragmentKind`s for the year parts, the January month, the month start and `floorMod(d, 7)`, keyed and planned as the prefix is; emitted once per lane group, elided when no consumer in the group reads them; the register and the `HugeMethodLimit` ladder re-pinned; the A/B beside task 32's shared rows at both widths | The matrix and the whole-range sweep under a widened group budget over every pair and triple of calendar outputs; the byte identity of every single-field kernel; the gate in 2.8 (at or above 1.05x at AVX-512 on both shapes), or the register goes to the debt register |
| 72 | Output order for prefix affinity (section 2.25): `year(d), year(d2), month(d)` takes three loop methods where the adjacent order takes two | The admission check first - no consumer of a group depends on contiguous output indices - then a two-pass grouping that gathers a calendar output into the group whose prefix it reuses wherever that group is; the evaluator and the line map untouched | The pinned limitation in `VarkaLoopEmitterSuite` flipped to two methods; the pinned oracles unmoved; the permuted and adjacent orders within noise in the parity harness at both widths; the differential suite green with the two orders |
| 73 | A stopping rule for the guard walk (section 2.26). **Planned** (`PLAN_TASK_73.md`), and its admission check is **done and overturns 2.37's premise**: the section expected no SQL shape to reach the over-guard, but `dayRange` bounds `make_date` from task 42's published years without looking at its children, so `year(make_date(2020, 1, dayofweek(date_add(d, off))))` fuses and the emitted kernel carries a guard the shape cannot need - 1274 to 1317 bytes in the masked loop. The task is therefore a fix rather than the decline 2.37 expected: a column-offset day producer is guarded on its own value even when a mod-7 node between it and the calendar node has already re-based the day (task 70's fuzz run; see the debt register) | The admission check first - whether any SQL shape observes the difference, given that `dayRange` returns `Unknown` for a mod-7 child and declines the entry at compile time before the emitter is reached, which can legitimately close the task with the finding recorded. If it does: a stopping rule on `collectColumnOffsetProducers` that descends only through nodes passing a day to the decomposition and stops at any node whose output is bounded in itself, and the matching rule in `dayRange`, taken together so the two analyses cannot drift apart again | The reproducer from the fuzz run served rather than declined at both widths (seed 20260907005 iteration 61379's shape, and the nine siblings substituting `weekday`, `dayofweek_iso` and `datediff`); the compiler suite's decline for `year(dayofweek(date_add(d, off)))` flipped to `fuses` if the compiler half moves, or the reason requoted if it does not; every guarded shape task 52 and task 60 pin still declining, since the rule may only remove guards a bounded node stands under; `VarkaIrFuzzSuite` at a million iterations per width with the `chronoBound` check relaxed to match, which is the oracle that found it |
| 74 | The validity-word algebra's missing axioms. **Scoped in section 2.9** (7 September 2026); after #145 | The coalesce axiom (`IfElse(IsNotNull(x), x, y)` denotes `x OR y`) and absorption in `pureOf`'s folding, behind task 70's switch; the census tool re-run through the emitter's own analysis rather than a mirror; the `coalesce(d, d2)` parity A/B pair | `coalesce(d, d2)` masked byte-equal to its dense twin and on its dense row at both widths; the differential over the nullable fixtures for two- and three-operand `coalesce`, `datediff(greatest(d, d2), d)` and `greatest(date_add(d, i), d)`; two million fuzz shapes with both extensions randomised; no other committed row moves |
| 75 | Zero-copy validity for leaf words. **Scoped in section 2.10**, and **the bound moved under it** (7 September 2026): task 70's third regeneration puts the masked-against-dense gap on `year(d)` at 0.4% at AVX-512 and below zero at 128-bit, under this task's own 2% decline line, so the zero-copy half is answered before the probe runs and what may survive is the cached null count, which that gap does not measure | The probe: masked `year(d)` with the copy skipped against the committed row, both widths. If admitted, the leaf case of the pass resolved to the input's validity buffer retained through Arrow's reference manager, a cached null count on Varka-owned output vectors, and the filter's compaction reading it | Under 2% at AVX-512 on the probe: declined on the record. Otherwise the differential over every null pattern with the output's validity address asserted equal to the input's, allocator accounting closing to zero with the retained buffers released, and the `year(d)` masked row on its dense row |
| 80 | String-column compaction that keeps the Arrow layout (section 2.27): a derived int32 leaf over a string column is refused per batch when a fused Varka filter sits under it, because the filter's compaction leaves the column on-heap (task 59's review; see the debt register) | The compaction writing offsets and data buffers rather than materialising rows, on task 21's `filterCompact` pattern; sized before milestone 6's item 3 puts string columns under filters and group keys | The stacked `next_day(d, s)` over a Varka filter counting no `numFallbackBatchesNonArrow` at all on task 59's own fixture, answers unchanged, and the fixed-width compaction's numbers not moving |
| 81 | Spark's own date tests as a differential corpus (section 2.11). **Scoped** (7 September 2026) | The harvest, from the golden-file inputs first - `sql-tests/inputs/date.sql` and its six date-family siblings, 254 `select` statements already written as SQL text - and from `DateFunctionsSuite` and `ColumnExpressionSuite` after them; `DateExpressionsSuite` excluded on the record, because `checkEvaluation` never reaches a physical plan. Then the rewrite that makes the corpus reachable at all: each statement's literal operands turned into columns of an Arrow-cached fixture, since 94 of `date.sql`'s 101 statements are constant-folded before any operator exists and the rest read one row of strings. Per entry: the answers compared against the row engine on the same fixture, and the plan classified fused, partial or declined on task 62's `Fusion` rule, so a declined entry cannot pass as a silent fallback | Every harvested entry agreeing with the row engine; the fused/partial/declined split committed as the coverage number, naming which expressions are out rather than a percentage; a harvest and rewrite that are re-runnable rather than a hand-copied list, so an upstream statement added later is picked up; and the limits stated in the plan - the golden `.sql.out` files stop being the oracle once operands become columns, and a handful of rows per entry reaches the epilogue and never a full lane group |
| 82 | The mask-to-long disposal in a checked kernel (section 2.12). **Scoped** (8 September 2026); reads task 63's committed numbers and shares its ground with task 64 | `emitGuardCollect`'s per-lane-group `VectorMask.toLong`, the AND with the node's word and the OR into the accumulator, which every runtime refusal shares - task 52's range guard, task 42's year check, task 60's month-count check and task 63's overflow check; the candidates are a mask-typed accumulator converted once per batch, skipping the AND where the algebra says the word is dead or all-ones, and hoisting the collect where the batch's statistics prove the mask empty | The checked mixed-null `i + 1` row at 128-bit (63.7% of the unchecked row today) moves by more than 10% while the dense rows and every unguarded shape's bytes do not, and task 52's guard pair in the parity file moves with it |
| 83 | One refusal, instead of four (section 2.14). **Scoped** (8 September 2026), from task 63's review; independent of 84 to 86 | The four runtime refusals - task 42's `make_date` year check, task 52's range guard, task 60's month count, task 63's overflow check - behind one node property carrying its mask, its qualifying word and its reason, with one analysis set, one slot rule, one collect and a status bit per reason, replacing `guardedProducers`/`selfGuarding`/`checkedArith`, the `guardedWord`/`guardScratch` pair and the shared `STATUS_CHRONO_RANGE` | No emitted byte moves for any shape that exists today: the pinned line map, the shape hash, every `codeSize` assertion and `dev/varka_emit.sh --table`'s op counts for `year(date_add(d, off))`, `add_months(d, m)`, `make_date` and ANSI `i + 1` all unchanged - this task buys a status bit and legibility, not speed |
| 84 | One value-range lattice (section 2.15). **Scoped** (8 September 2026), from the three bugs task 63's review found in the seam between `dayRange` and `intBound`; before 85 | One saturating interval domain over lane values, with the calendar admission (task 52) and the overflow check (task 63) as queries on it rather than two traversals, and "what a runtime guard proves" as an explicit parameter of a query rather than a fact baked into one traversal's arms; written in Java, being pure data | Every shape the compiler admits or declines today unchanged, decline reasons included, and the differential's fusion classification unmoved; plus the property test the current code cannot pass - over random IR, the interval a node reports contains the value the reference evaluator computes, for every lane pattern |
| 85 | Lane type as a parameter (section 2.16). **Scoped** (8 September 2026); after 84, and blocking milestone 5's own tasks 28 and 29 | The emitter parameterised on a lane descriptor - vector class, species, byte stride, load and store descriptors - against the 204 `INT_VECTOR` references, 16 four-byte stride assumptions and 18 species references it carries today; the lane on the node's physical representation rather than inferred from the Spark type, with year-month intervals (int32 months, the same lane as DATE and INT) as the forcing function that can land first; measured against a generated-per-lane emitter, since a descriptor risks a megamorphic call in the hot path | The int32 lane's emitted bytes unchanged against the pinned oracles; a second lane type reaching the same green differential and fuzz matrices at both vector widths; and the fuzz reachability test widened from every node type to node type times lane type |
| 86 | One operand admission, stated once (section 2.17). **Scoped** (8 September 2026), from the ghost fallback task 63's review found; independent of 83 to 85 | `intOperand`, `compileIntOperand`, `compileOffset` and `compare`'s `operand` as one function taking what the position accepts, and the emitter's four `require*Shape` checks derived from that same table rather than restated beside it, so widening the compiler either widens the emitter or fails to compile; carrying one widening as the table's first exercise - a bare `IntegerType` column in comparison operand position, which declines today and which task 79's admission check verified fuses with one case added, and which is folded in here rather than taken alone because widening one copy in isolation is what produced the ghost fallback | A test enumerating the operand positions and asserting that the set the compiler admits and the set the emitter accepts are the same set - the assertion whose absence let `date_add(d, weekday(d2) + 1)` ship as fused in EXPLAIN and a silent per-batch fallback at run time; every compiler decline reason unchanged, since no shape may move |
| 87 | The epilogue is the one method no budget bounds (section 2.18). **Scoped** (8 September 2026), from a 35-million-iteration fuzz run; independent of 83 to 86. **Absorbs milestone 4's row 44** (11 September 2026), which asked for the same partitioning at a softer threshold - its size ladder (4095 and 63, not only 4096) and its `HugeMethodLimit` measurement are requirements here | The epilogue emitted as one method holding every group, so a tree inside `MAX_FUSED_NODES` can pass 65535 bytes and the Class-File API refuses the class - `epilogueMasked` at 67244 bytes for nested `make_date`, replaying at `-Dvarka.fuzz.seed=2026092800 -Dvarka.fuzz.only=73411`; either partition it as the loop is partitioned or give the emitter a byte budget, and in both cases decline with a reason rather than throw | The pinned fuzz iteration declining with a reason instead of throwing; every shape that fits today emitting identical bytes against the pinned line map and the `codeSize` assertions; and `MAX_FUSED_NODES`' javadoc no longer claiming a per-method guarantee it only has for the loop |
| 88 | An exact division through double lanes (section 2.19). **Scoped** (9 September 2026), from task 68's admission check; independent of 65 but competes with it | `trunc((double) v * (1.0 / d))` as a lowering for integer division by a constant, exact for every int32 dividend and any divisor below about 2^21 - no magic, no correction carries, no range restriction, and no int64 lane, so none of this milestone's lane-width work is a precondition. Verified numerically and round-tripped through `I2D`/`D2I` on the preferred species; the admission check makes that exhaustive over the calendar divisors and commits the script | The exhaustive check committed beside `verify_long_lane_magic.py`; op counts per lowering from `dev/varka_emit.sh --table` before any timing; then a three-arm A/B - today's range-narrowed magic, task 65's int64 widening, and this - on one benchmark over the same shapes, with the choice made from the numbers rather than from the simplification each offers |
| 89 | The year-month interval divisions (section 2.20). **Scoped** (9 September 2026), split out of task 68; after whichever of 65 and 88 the A/B chooses | `extract(YEAR FROM ym)` and `ym / num` on the chosen exact division, with the truncation correction `extract` needs and the `HALF_UP` step `ym / num` needs; `extract(MONTH FROM ym)` behind the further question of a `ByteType` output the evaluator does not have; `ym / col` declining, the divisor not being a constant | The two rounding corrections verified over the full int32 month range against Spark's own `getYears`, `getMonths` and `IntMath.divide` by a committed script; the byte-output question settled before `extract(MONTH)` is built; a throughput pair per shape against the row engine, these being new lowerings |
| 90 | The benchmark files are not reproducible run to run (section 2.21). **Partly done** (10 September 2026): the band is measured and committed for the parity and throughput benchmarks at both widths, and the regeneration diff classifies against it. That half landed under task 77, which had re-scoped itself onto this row's work without noticing this row existed - recorded here rather than quietly absorbed. Scoped 9 September 2026 from the investigation task 79's section 9 asked for; the pinning half was already done | Two regenerations with no change between them disagree on 73 of 211 cases by more than 3% and 22 by more than 10%, pinned; unpinned the worst is 75%. Measured out: within-run noise (avg/best median 1.007), the clock (constant to 1.2% while throughput moves 31%), ASLR, contention. What remains is the per-fork C2 lottery `PLAN_TASK_32.md` 11 already traced to JDK-8380195. `dev/varka_bench_repeat.sh` measures the band; `dev/varka_bench_regen.sh` now pins to the fast core complex and records it. **Measured, 10 September 2026**, over ten runs per width on an idle pinned machine: the parity file's median spread is 5.34% at AVX-512 and 1.72% at 128-bit, p90 22.30% and 11.88%, worst 227.15% and 39.06%; the throughput file 5.31% and 3.67%. Two findings the section did not predict. The narrow width is the *quieter* of the two by a factor of three at the median, so collapses have been found at 128-bit because that file is quiet enough for one to stand out, not because it is unstable - and the two widths need separate bands for that reason. And three runs understate the band: this row's own 1.6% median comes from three runs, where ten give 5.34% at the same width | The band committed per file for the parity and throughput benchmarks: **done**. The regeneration diff reported against the band rather than a flat 3%: **done**, cutting held-out false alarms on an unchanged file from 32.9% of rows to 5.6% at AVX-512 and 11.4% to 2.1% at 128-bit. Still open: the arithmetic benchmark's band; task 63's 9.7 dead-local figure re-taken pinned before task 82 scopes itself on it; the decision on N-fork medians taken from the cost, with the fallback stated - that absolute rates stop being compared across runs and the within-run A/Bs carry the claims; and the cause itself, which task 77's census leaves open with one method taking 100 runtime deoptimisations across 194 compiles and the `task_queued` records unread |
| 91 | A guard bound the shift above it chooses (section 2.22). **Scoped** (10 September 2026), from task 69's outcome: it closed the upward half of `PLAN_MILESTONE_4.md` 9's conservative-decline entry and left the half that motivated it, `weekofyear`/`yearofweek` over a column offset, still residual because `ThursdayOf` shifts downward and there is no headroom below `NARROW_MIN_DAYS`; after 84, whose interval representation it should take | Task 52's runtime guard comparing against a bound the compiler chooses from the shift `dayRange` already computes for the subtree above the producer - `[NARROW_MIN_DAYS + 3, NARROW_MAX_DAYS]` under a `ThursdayOf` consumer, `+ 365` under a `trunc` one - so every downward-shifting consumer over a guarded producer becomes admissible at the same run-time cost, one compare against a different immediate | The guard's compare shown to be the only place `NARROW_MIN_DAYS` enters these kernels, by grep and a `VarkaEmitDump` op-count diff; the shape key shown to separate two subtrees identical but for the shift above them; `weekofyear(date_add(d, off))` fused with a differential over a batch that straddles the moved bound |
| 92 | The validity write, keyed on the bit layout (section 2.23). **Scoped** (10 September 2026), from task 47's measurement: its word writer wins 6 to 9% at 4 lanes and loses 11 to 20% at 8 and 16, so it shipped as an option defaulting off. **Worth more than it was first written as** (corrected 11 September 2026): four int lanes is `SPECIES_PREFERRED` on every NEON-only aarch64 and on x86 without AVX2, so this is a real target's default rather than a `MaxVectorSize` flag's; what it needs is one confirming run on such a machine, since task 47's four-lane numbers simulate the lane count on a 16-lane x86 | `validityByWord` defaulting on where a validity group is smaller than a byte (`lanes < 8`) - one condition read off the bit layout rather than two thresholds fitted to a machine, which is what task 76 declined; plus option B of `PLAN_TASK_47.md` 3.1, storing once per word rather than once per group, and 3.4's masked-driver liveness item, both of which share this task's ladder run | The k=3 step in task 47's ladder explained from `-XX:+PrintInlining` before any rule is fitted across it - the emitted code is already ruled out - and the width rule's win reproduced on a machine that runs at that width rather than under a `MaxVectorSize` flag |

## 4. Files

From milestone 4's section 4, the parts that belong to these tasks:
`VarkaVectorIR` (the second `LaneType`, conversion nodes, the boolean output),
`VarkaLoopEmitter` (conversions, the overflow detectors, the zero-safety
member the first trapping node makes structural), `VarkaExpressionCompiler`
(casts, arithmetic, boolean roots), `VarkaShapeCacheImpl` only if the key
vocabulary grows; in `sql/core`, the evaluators (int64 buffers, boolean output
vectors) and `VarkaColumnarRule` (new eligible roots); in the engine module,
hand-written reference kernels only where a parity anchor is needed for a new
lane type, per the reference-code commenting rule. Tasks 74 and 75 touch
`VarkaLoopEmitter`'s `Analysis` (two `pureOf` arms, two folding rules, a
test hook for the census), `VarkaWordCensus` and `dev/varka_word_census.sh`
in catalyst test scope, and, if 75 is admitted, `VarkaKernelEvaluator`'s
output assembly and `VarkaOwnedArrowColumnVector`.

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

### Item 14. A lockstep validity evaluator for arbitrary pure words

*Added 7 September 2026 with section 2.9; the item that section's census
declined to make a task.*

**Design input.** Task 70's pass serves a root whose word is a chain of one
operator, because the engine folds a chain into the destination bitmap in
place and a tree that mixes AND and OR needs a second live intermediate. That
restriction is an artefact of evaluating one operator at a time over the
whole column. Walk every input bitmap in lockstep instead, evaluating the
whole term word by word in registers, and the registers needed are the
tree's Strahler number - Ershov's theorem gives it as the exact minimum, and
the census (2.9) puts it at 2 or 3 for all but two of 40000 grammar shapes.
Every pure term becomes servable in one pass with no scratch buffer; the
chain evaluator is the case where the tree is left-leaning; and the same
walk evaluates every root of a projection over one pass of the inputs, with
shared subterms held in registers, which is the only form in which
cross-output sharing (2.9) would pay. The emitted form is either a small
interpreter over a postfix encoding of the term - a handful of opcodes, one
dispatch per operator per 64 rows, negligible beside a kernel - or the loop
emitted into the driver as bytecode, which the driver's `HugeMethodLimit`
ladder (task 70 pinned it) would have to absorb.

**Why it is an item and not a task.** After task 74, the words it would
serve are 1.4% of the fuzzer grammar's roots and none of the `Surface`
inventory; the shapes are `datediff(greatest(d, d2), greatest(d3, d4))`
and `date_add(greatest(d, d2), i)` and their kin, which no corpus query in
`SCOPE_MILESTONE_6.md`'s survey has. It enters when one does, with the
census re-run over that corpus as its admission check, and it should then
also take the `Cond` null-test predicates 2.9 holds as a question, since
complement is one more opcode to the interpreter and a fourth entry point
to the chain.
