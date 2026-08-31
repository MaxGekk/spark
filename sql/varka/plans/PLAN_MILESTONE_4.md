# Varka Milestone 4 Plan: breadth

Milestone 3 closed with task 23, so this file is no longer the scope document it
opened as: it is the task plan that document promised, written against the
measurements it said should order it. The scope catalogue it grew from is kept
whole in section 9, with every item's number unchanged, because other plans cite
those numbers (`PLAN_TASK_21.md` cites items 5 and 11, `SKILLS.md` cites item
13, and `SCOPE_MILESTONE_5.md` cites items 1 through 12 throughout). Where the
catalogue and this plan disagree, this plan wins - the catalogue records what
was thought before the survey and before milestone 3's numbers, and several of
those thoughts have already been corrected in writing (`SCOPE_MILESTONE_5.md`
section 2).

Milestone 1 built kernels. Milestone 2 built the emitter and proved it on int32
date chains. Milestone 3 made that fast path reach real queries: filters, the
four gating shapes, cross-task class reuse. Milestone 4 is **breadth**: the
engine stops being a single-type demo and learns the types, expressions and
loop schedules a query actually contains. Task numbering continues the
project's single sequence and resumes at 24, after milestone 3's 18-23; the
committed spine is tasks 24-30.

## 1. Why this order

The scope document refused to order itself until three inputs existed. All
three now do:

* **The survey ran** (`SCOPE_MILESTONE_5.md` section 1), and its corrections
  are folded in rather than re-litigated: there is not one `DOUBLE` or `FLOAT`
  column in TPC-DS or TPC-H, so item 3 is the taxi benchmark's item; the old
  item 8 was two items and is now 8 (string functions, 37 uses) and 9 (string
  keys, 275 references, with the cheap subset pulled into milestone 5);
  decimals - the most-aggregated type in both corpora - stay out per item 12,
  and their design pass is milestone 5 items 1 and 2. Item 6's calibration
  survived: `year(` appears 3 times, the rest of the extraction family zero.
* **Milestone 3 landed the enablers.** Task 18's shape cache is what makes item
  13 affordable at all (a longer C2 compile is now paid once per shape, not per
  task); task 21 made masks first-class values and priced the compaction that
  item 11 is expected to replace (~1-3 ns/row typed copy - the ceiling on what
  `compress(mask)` can recover on this machine, `PLAN_TASK_21.md` section 5);
  task 23's emit-options record is the option surface this milestone's emitter
  work rides on.
* **The headline decision is made.** The owner picked TPC-H and TPC-DS as the
  corpus this milestone builds toward, not the taxi benchmark. Consequences:
  item 3 (float and double lanes) leaves the committed spine and waits with the
  follow-ons - only its oracle decision lands early, because it is a reading
  task and it unblocks the item whenever it is argued back in (section 7). The
  taxi whole-query claim stays milestone 5's target 5, and becomes reachable
  the day items 2, 3 and 6 have all landed.

The scope document's three invariants still decide what can share a task, and
the spine keeps them apart: one lane width per kernel (items 1, 2), every value
lane-shaped (item 7, deferred), no lane reads its neighbour (items 8-11's
cross-lane work, of which only item 11's compaction is taken, in the operator
where task 21 already bounded it). A task that takes two invariants at once is
a task whose failure cannot be attributed to one of them.

## 2. Design

### 2.1 The scalar tail, mask interrogation, compaction (task 24, item 11)

The tail is the argument. The emitter's remainder handling is a *second full
walk of the IR*, emitting scalar bytecode for every node - roughly half of the
per-node emitter surface, and every new node type in tasks 26-30 would
otherwise be written twice. `indexInRange` produces the mask for a partial lane
group directly, so the replacement shape is: the main loop stays **unmasked**
(masked ops cost 2.3x-2.9x even all-true, `SKILLS.md`), and one masked epilogue
iteration replaces the entire scalar walk. The temptation to resist, named
here so the task does not discover it: masking the main loop would be simpler
still and would cost more than the tail ever did.

The task opens with the measurement the scope filed as open question 3: the
tail's actual share of emission time and of loop runtime. If the runtime share
is negligible, the case rests entirely on emitter code size - still a good
case, but it must be made on the honest number.

Second deliverable: `compress(mask)` compaction in `VarkaFilterExec`, replacing
task 21's scalar typed copy. The ceiling is committed (~1-3 ns/row), so the
prediction registers against it before the run. On x64 with AVX-512 `compress`
intrinsifies to `VPCOMPRESSD`; the development machine (Zen 5) will flatter
it, so the number is taken at `-XX:MaxVectorSize=16` as well, and the fallback
verdict is written either way. Third: `anyTrue`/`allTrue` per-lane-group
all-null and all-valid fast paths, where the prologue today has them only per
batch.

This task changes emitted bytes, so the two pinned shape hashes and the
pinned line-map literal move, and are regenerated under their own update
rule - the one task in the spine where that is expected rather than alarming.

### 2.2 Instruction-level parallelism (task 25, item 13)

The debt register's rule applies: a prediction goes in writing before the
first measurement, and the honest null hypothesis is that C2 plus the
out-of-order engine already collect most of the available overlap on a 16-op
body, so K pays only on the long chains. The three confounders move together,
never one at a time: K, the broadcast strategy (pinned locals collapsed
throughput 7x at ~32 broadcasts, so unrolling and pre-broadcasting *compete*),
and `GROUP_BUDGET`, which unrolling multiplies against a ~1 ms-per-vector-op
C2 compile. The candidates are the shapes that are compute-bound and already
carry a committed number to beat: `dayofweek` (a 20-op fold), `CASE WHEN` on
an unpredictable condition, and the depth-8 chain. Row-consumer shapes are
bounded by the ~25 ns/row read-back floor and the filter path by compaction;
no kernel-side ILP moves either, so neither is a candidate. One negative
result worth carrying in: a 2-way unrolled add kernel over a misaligned
buffer still lost 50-60% to the aligned case (section 8's buffer-alignment
entry) - unrolling does not incidentally hide the alignment penalty, so this
task's outcome and that entry's are independent questions, not one deferring
to the other.

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

### 2.3 Calendar extraction, `year` first (task 26, item 6)

The one vocabulary item that fits milestone 2's machinery as it stands: int32
lanes, existing operators, task 17's range-narrowed magic multiply. The task
*opens* with its admission check, before any emitter work: each of 146097,
36524, 1461 and 153 needs its own range-narrowing argument - the value shrunk
until both `v * e < 2^k` and `v * M < 2^31` hold inside the low 32 bits `mul`
returns - or it has no vector lowering, because lanewise `DIV` scalarizes at
~9x. A constant that will not narrow changes the algorithm choice
(Neri-Schneider preferred, Cassels and Hinnant the alternates), and if none
survives for a given field, that field is declined with a task-16 reason
rather than shipped slow.

`year` first - it is the one extraction TPC-H uses (q7, q8, q9) and the whole
of what the headline corpus asks of this family - and `month` and
`dayofmonth` committed with it, by the owner's decision during planning: the
candidate algorithms are civil-from-days decompositions that produce all
three fields in one pass (Cassels' `(5 * d + 2) / 153` form exists precisely
to yield month and day), so once the admission check clears the constants,
the two extra functions are the same lowering rather than extra algorithm
work. `quarter` rides `month` as `(month + 2) / 3`, a division whose constant
narrows trivially; `dayofyear` and date-level `date_trunc` follow as the
algebra yields them. The corpus calibration stands and is not overridden:
`month(` and `quarter(` appear zero times in the benchmarks, so everything
past `year` is vocabulary completeness, taken because it is nearly free - not
because the corpus asks for it.

### 2.4 Boolean outputs (task 27, item 5)

The cheapest item and the only pure continuation of milestone 3: comparisons
and `And`/`Or`/`Not` as projection *results*, built on task 21's mask-as-value
machinery. `VectorMask.toVector` against a `blend` of one and zero was
pre-registered as a measurement, not a debate, and it is now measured
(`VarkaMilestone4MeasurementsBenchmark`, committed run in
`sql/varka/engine/benchmarks/VarkaMilestone4MeasurementsBenchmark-jdk25-results.txt`):
the two are statistically tied at both vector widths, on two separate runs -
neither wins the way the pre-registration expected. The real, width-dependent
finding is a different question the pre-registration did not ask: whether to
materialize an int column at all. Skipping it - packing `VectorMask.toLong()`
straight into the output bitmap - wins by 1.16-1.18x at AVX-512 but *loses* by
1.40-1.51x at 128-bit, reproduced on both runs. A compound predicate,
`(a > b) AND (c < d)` kept in mask space the whole way through versus
materialized as an int column at every node, shows the same width split:
tied at AVX-512, but mask-space wins by 1.24-1.37x at 128-bit - never worse,
sometimes decisively better. Two consequences for the task: walk boolean
sub-expressions in mask space and materialize only once at the output
boundary (never worse, and the compound case argues for it directly), and the
single-comparison bits-only shortcut needs a width check rather than a single
committed choice, since its sign flips between the two vector widths this
project already tests at. The two real questions the pre-registration also
named are format and nulls: Spark's bit-packed boolean vector against
Arrow's validity-style bitmap at the output boundary, and the three-valued
rules holding there exactly as they hold in the interior - a null input
produces a null output, never a false one. The differential runs every null
pattern for exactly that reason.

### 2.5 Lane-width conversion (task 28, item 1)

The width machinery items 2 and 4 lean on. The hard part is not the
conversion, it is the lane count: at one shape an int32 species holds twice
the lanes of an int64 species, so a mixed-width kernel either drives the loop
at the narrowest lane count and leaves wide lanes half empty, or emits a part
loop per conversion and carries two trip counts. That is the one decision in
this item that is expensive to reverse, so the scope's open question 2 was
pre-registered as a measurement before the task opens: both shapes on a
`cast(int AS long) + long` chain. Measured
(`VarkaMilestone4MeasurementsBenchmark`, same committed results file as 2.4):
narrowest-drive and part-loop are statistically tied at both vector widths,
on two separate runs, narrowest-drive very slightly ahead each time (within
1.01x-1.05x, inside this file's own noise band). Part-loop's extra
bookkeeping - two trip counts, two stores per int chunk - buys nothing
measured, so task 28 opens already knowing the winner: narrowest-drive, for
the simpler build (one trip count) at the same throughput. The recorded
fallback if a wider mixed-type shape measures differently once task 28 is
under way: items 2 and the multiply half of 4 can be built width-locked and
retrofitted.

### 2.6 int64 lanes: `TimestampNTZ` and `bigint` (task 29, item 2)

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

### 2.7 ANSI-correct integer arithmetic (task 30, item 4)

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

Any division this task (or a later one) adds inherits a mechanism question
task 24 already flagged and did not answer: the emitter's masked epilogue
leaves inactive lanes reading 0, and integer division traps on a zero
divisor - the first trapping op the walk admits. Two fixes exist: blend a
safe divisor (1) into inactive lanes before an unmasked `DIV`, or the masked
lanewise `DIV` form, which never evaluates inactive lanes. Pre-measured
(`VarkaMilestone4MeasurementsBenchmark`, same committed results file as 2.4):
blend-then-`DIV` beats masked `DIV` at both vector widths, on two separate
runs - 1.08-1.12x at AVX-512, 1.18-1.19x at 128-bit by minimum. The smallest
margin of the five measurements in that file, but the only one where all four
data points (two widths times two runs) agree in both direction and rough
magnitude, which is the interleaved comparison the under-1.3x rule asks for.
Blend a safe divisor; do not reach for the masked lanewise form.

## 3. Task breakdown

Tasks 24-30 are the committed spine, in dependency order: 24 halves the
per-node emitter surface every later task would otherwise pay twice; 25 shares
24's harness and changes how every later kernel is emitted; 26 and 27 spend
milestone 2's machinery before 28 complicates it; 28 enables 29 and 30's
widening. Items 7, 10, 9 and 8 are the follow-on ladder in that order - each
needs its own argument to enter, per the milestone 3 rule. Numbering continues
the single sequence; milestone 5 resumes it at 31 unless this plan grows the
way milestone 3's did.

| # | Task | Deliverables | Validation |
|---|---|---|---|
| 24 | The scalar tail, interrogation, compaction | The tail-cost measurement (open question 3) recorded first; the unmasked-body-plus-masked-epilogue loop via `indexInRange`, deleting the emitter's second scalar IR walk; `compress(mask)` compaction in `VarkaFilterExec` against the committed ~1-3 ns/row ceiling, with the non-AVX-512 verdict; per-lane-group `anyTrue`/`allTrue` fast paths | Differential green at both vector widths, all null patterns, all-selected and none-selected; pinned hashes regenerated under their update rule; filter ladder re-run and committed; emitter per-node surface reduction stated as a number |
| 25 | ILP: the unroll factor as a plan decision | The registered prediction, then the three-confounder matrix (K x broadcast strategy x `GROUP_BUDGET`) on `dayofweek`, unpredictable `CASE WHEN`, and the depth-8 chain; if K > 1 pays, per-shape K chosen from the live-temporary count the emitter already computes; the `SKILLS.md` bullet rewritten with the numbers; the batch-size knee sweep (question 6) on a wide fused shape | A committed number per candidate shape against its existing baseline; prediction scored honestly; no committed number regresses on shapes where K stays 1 |
| 26 | Calendar extraction, `year` first | The four-constant range-narrowing admission check, recorded before emitter work; `year`, `month` and `dayofmonth` committed - one civil-from-days decomposition yields all three - with `quarter` riding `month` and `dayofyear`/date-level `date_trunc` as the algebra yields them; fields whose constants will not narrow declined with a task-16 reason | Differential across the Gregorian range including pre-1970, leap years, month-length boundaries and the 400-year cycle edges, at both widths; parity numbers committed; `year` demonstrably compiling on the TPC-H q7/q8/q9 shape |
| 27 | Boolean outputs | Mask-to-column materialisation (`toVector` against `blend`, measured); the bit-packed format decision at the Spark/Arrow boundary; three-valued rules holding at the output boundary | Differential over every null pattern - a null input never becomes false; `SELECT d > DATE '2000-01-01' AS flag` and filter-leftover boolean columns compile; committed number on one boolean-output shape |
| 28 | Lane-width conversion | The mixed-width loop-shape measurement (open question 2: narrowest-drive against part loops) on `cast(int AS long) + long`, committed before integration; `convert`/`convertShape` emission following the winner; numeric `Cast` and Catalyst's implicit promotions over the supported types | Differential on mixed int32/int64 trees at both widths; the loop-shape decision recorded with its numbers; no regression on single-width shapes |
| 29 | int64 lanes: `TimestampNTZ`, `bigint` | The second `LaneType`; `TimestampNTZ` comparisons, differences, literal arithmetic; `TimestampType` and `LongType` comparisons and diffs; range-narrowed magic constants for 1000000 and 86400 or a recorded decline; the field differential mode from task 22 | Every parity gate re-run at the long species and both vector widths; the halved-headroom number committed rather than discovered; zoned operations demonstrably declined, not wrong |
| 30 | ANSI integer arithmetic | `try_add`/`try_subtract`/`try_multiply` via the difference-mask-as-validity path; the ANSI throw path via saturating detection and scalar re-walk, priced with a registered prediction; `Multiply` overflow through 28's widening if it is cheap, declined with a reason if not | The error-identity differential: same `SparkException`, same row, as the row engine under ANSI; `try_*` differential over overflow-dense and overflow-free data; committed number on the no-overflow path against Janino |

## 4. Files

* **Changed (catalyst):** `VarkaVectorIR` (the second `LaneType`, conversion
  and extraction nodes, boolean output), `VarkaLoopEmitter` (masked epilogue,
  unrolling, conversions, the overflow detectors), `VarkaEmitOptions` (the
  unroll factor joins the record if task 25 says it exists),
  `VarkaExpressionCompiler` (extraction family, casts, arithmetic, boolean
  roots), `VarkaShapeCacheImpl` only if the key vocabulary grows.
* **Changed (sql/core):** `VarkaFilterExec` (compaction), the evaluators
  (int64 buffers, boolean output vectors), `VarkaColumnarRule` (new eligible
  roots).
* **Engine module:** new hand-written reference kernels only where a parity
  anchor is needed for a new lane type (the task-26/29 algorithms), per the
  reference-code commenting rule.
* **Docs:** `docs/sql-varka.md` and `README.md` requoted from one run
  whenever a task moves committed numbers - tasks 24 and 25 will; the later
  tasks add numbers rather than move them.

## 5. Verification

The standing gates, inherited, with the two hardenings the scope promised:

* Differential against the row engine over every new shape, null patterns
  included, at the preferred width **and** `-XX:MaxVectorSize=16` - now at
  every lane width the milestone adds, not just every vector width.
* Parity: an emitted loop stays at or above the hand-written kernel where one
  exists; committed results regenerated in a single run on an idle machine, on
  the five-iteration two-second-window methodology.
* The ghost fallback still never fails a query; a shape the engine cannot
  express correctly is declined with a task-16 reason, never computed wrongly.
* **New:** the error-identity differential (task 30) - the same
  `SparkException` attributed to the same row - which the suites have never
  had to assert before. The byte-exact oracle otherwise still holds
  everywhere this milestone goes; it stops being universal only when item 3's
  doubles enter, which is exactly why the oracle decision is taken early even
  though the item is deferred.
* The pinned shape hashes remain the behaviour oracle for refactors, and a
  task that legitimately moves them (24 above all) regenerates them under
  their update rule and says so, rather than treating the oracle as noise.

## 6. Risks

* **Masking the main loop.** The cheap implementation of task 24 masks every
  iteration and pays 2.3x-2.9x everywhere to save a tail that costs almost
  nothing per batch. The epilogue-only design is the whole point; the
  tail-cost measurement exists so the trade is visible.
* **A constant that will not narrow.** Task 26's algorithms live or die by
  four magic multiplies. The admission check runs before emitter work so a
  dead constant changes the algorithm, not the shipped semantics.
* **The mixed-width decision is expensive to reverse.** Task 28's loop-shape
  choice is baked into the emitter; that is why it is measured first and why
  width-locked retrofit is the recorded fallback.
* **Numbers move under the milestone's own feet.** Tasks 24 and 25 change
  emitted bytes and committed relatives; docs are requoted from one run, never
  patched case by case.
* **Scope creep through the catalogue.** The spine is 24-30. Items 7, 10, 9
  and 8 are real and stay in section 9 with full design input; each enters
  only with its own argument, the way `In` and `Coalesce` entered milestone 3.

## 7. Open questions, and where each is settled

The scope's section 8, each question now owned by a task or settled here:

1. **The ULP oracle** (item 3): a reading task - what accuracy Spark promises
   for `exp`, `log`, `pow` and the trig family, and what bound a vector
   differential asserts. It lands during the early spine (by task 26's close)
   even though item 3 is deferred, because it is cheap, it is the item's
   gating decision, and settling it in writing is what lets item 3 be argued
   back in without a design pause. Recorded in this file when settled.
2. **Mixed-width loop shape**: task 28 opens with it.
3. **What the scalar tail actually costs**: task 24 opens with it.
4. **Does an unroll factor above 1 pay**: task 25, prediction first.
5. **Does an unrolled loop still want a scalar tail**: tasks 24 and 25 share
   the harness, as the scope required.
6. **Is 4096 rows still the knee for wide fused shapes**: rides task 25's
   harness; either a knee worth respecting or the question retired in writing.
7. **Where the survey's corpus ends**: settled by the headline decision in
   section 1 - TPC-H and TPC-DS rank this milestone, taxi ranks item 3, and
   the type ranking the corpus could not give came from semantics-readiness
   (int64 first, because its semantics are already written).

## 8. Explicitly out of milestone 4

* **Item 3, float and double lanes** - the headline decision's consequence.
  Only its oracle decision lands (section 7). The item re-enters whenever the
  taxi target is argued for, with its catalogue entry intact.
* **Items 7 (aggregation), 10 (windows), 9 (string keys and dictionaries),
  8 (string functions)** - the follow-on ladder, in that order. Item 7 is
  first in line because milestone 5's aggregate wiring depends on it; none
  enters without its own argument.
* **The Varka Java configuration surface** - task 23 built and then scoped it
  out; the owner left it unscheduled. The design, the two converter lessons
  and the three increments are recorded in `PLAN_TASK_23.md` under "Deferred
  to a dedicated task"; it takes a number when it starts.
* **`DecimalType`** - per item 12; its design pass is `SCOPE_MILESTONE_5.md`
  items 1 and 2.
* **Grouped aggregation, hash joins, sorting** - grouping is hashing and
  partitioning, a milestone of its own after item 7.
* **The Arrow-native Parquet reader and writer** - the project owner's work.
  Coordinate, do not duplicate.
* **Buffer alignment enforcement** - the missing measurement is no longer
  missing (`VarkaMilestone4MeasurementsBenchmark`, section 2.4's committed
  results file, `addAligned`/`addMisaligned`): a buffer start offset by 4
  bytes (still 4-byte int-aligned, but every AVX-512 load then spans two
  64-byte cache lines) costs 1.56-1.68x throughput at the default width and
  1.22-1.25x at 128-bit, reproduced on two separate runs, over the L1/L2-
  resident 4096-row working set every real Varka kernel actually runs at.
  Section 2.2's ILP item does not absorb this for free either: a 2-way
  unrolled version of the same misaligned kernel (not committed - a scratch
  check, not this file's methodology) still lost 50-60%, so unrolling and
  alignment are independent levers, not substitutes. The measurement item 13
  was waiting on is done; what stays out of milestone 4's committed spine is
  the enforcement itself - an allocator-level change - which is now a design
  question with real numbers behind it rather than a deferred unknown, to be
  argued in with its own task the way item 13 or the string items would be.
* **Whole-stage code generation** - in the charter (`VISION.md` section 13),
  not in this milestone.

## 9. Scope catalogue

The pre-plan catalogue, item numbers preserved. Items the plan above adopts
are condensed to a pointer; items it defers keep their full design input,
which is what makes them worth carrying forward.

### Item 1. Lane-width conversion, and mixed-type expression trees

Adopted as task 28 (see 2.5). The design input carried over whole: the hard
part is the lane count, not the conversion; `convertShape(I2L, longSpecies,
part)` yields one long vector per part with `partLimit` parts; the
narrowest-drive-versus-part-loop choice is measured before either is built in;
Spark's narrowing `Cast` throws under ANSI and wraps without it, tying this to
item 4.

### Item 2. int64 lanes: `TimestampNTZ`, `bigint`, and the second lane width

Adopted as task 29 (see 2.6). Kept for the zoned day when it comes: pack the
IANA tzdata transitions into flat `long[]` interval arrays and resolve a
vector of timestamps against them with a SIMD binary search, rather than
per-row `ZoneRules` lookups.

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
  this milestone's open question 1 and lands early regardless (section 7).
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

Adopted as task 30 (see 2.7). The pricing argument carried over whole:
wrap-versus-saturate difference lanes are exactly the overflowed lanes, one
vector op and one well-predicted branch on the common path, `try_*` as the
branchless easy case worth shipping alone.

### Item 5. Boolean outputs

Adopted as task 27 (see 2.4).

### Item 6. Calendar field extraction, `year` first

Adopted as task 26 (see 2.3). The corpus calibration kept on the record:
TPC-DS pre-materialises calendar parts (`d_year`, `d_moy`, `d_dom`, `d_qoy`,
`d_dow`), so extraction appears zero times there; TPC-H q7, q8 and q9 use
`year(date)` and nothing else. Intuition overweights this item; the corpus
says it is one function wide.

### Item 7. Aggregation: the first horizontal reduction

**Deferred - first in the follow-on ladder**, and milestone 5's aggregate
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
the six corpus functions still missing after milestone 5 are here
(`SCOPE_MILESTONE_5.md` section 1.7) - most of what stands between the roadmap
and the whole corpus function surface, and a long thin tail: 37 uses against
item 9's 275 key references.

**Vector API it needs**: `ByteVector` and `ShortVector`; `compare` with
`anyTrue`/`allTrue`; `rearrange` for byte permutation inside a value.

**Design input.** Variable width is the whole problem: Arrow strings are
offsets plus bytes, every operation is data-dependent in length, and the
fixed-lane-count loop stops being the right shape - which is why SWAR date
parsing stays in its own design pass. The SWAR sketch, kept: load the digit
bytes as one word, subtract `0x30303030`, collapse with a multiply-add, and
validate the separators with one vector compare whose failing lanes send the
batch to the existing parser - the ghost-fallback discipline again.

### Item 9. String keys: equality, hashing, dictionaries

**Deferred - its near-term half is already milestone 5's item 3** (fixed-width
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

### Item 11. Compaction, mask interrogation, and the scalar tail

Adopted as task 24 (see 2.1), with task 21's committed compaction numbers as
the ceiling the scope asked for.

### Item 12. Considered and set aside

Recorded so they are not re-proposed:

* **`Float16`**: no Spark type maps to it, not even through Arrow.
* **Unsigned comparison and min/max**: Spark has no unsigned integer type;
  available as internals (hash bucketing, `shiftrightunsigned`), not an
  expression family.
* **`DecimalType`**: not a lane type - precision <= 18 fits an int64
  unscaled value, but the general case is 128-bit with no lane at any
  species. It needs its own design pass, not an item here; that pass is
  `SCOPE_MILESTONE_5.md` items 1 and 2, made urgent by the survey.
* **`CPUFeatures`**: package-private, so a fallback decision comes from a
  measurement or the species width, never a feature query.
* **Hash joins**: scalar probing over off-heap tables with SIMD reserved for
  radix partitioning and post-probe projection; they want item 7 first and a
  milestone of their own after it.
* **`reinterpretAs*` / `viewAs*`**: useful inside item 3 for NaN
  canonicalisation, not an item.

### Item 13. Instruction-level parallelism: the unroll factor

Adopted as task 25 (see 2.2). The full three-constraint pricing - the 7x
pinned-broadcast collapse, the ~1 ms-per-vector-op compile cliff against
`GROUP_BUDGET`, and `DIV` scalarization that unrolling cannot rescue - lives
in `SKILLS.md`'s "Vector API on HotSpot, Measured", whose unrolling bullet
task 25 rewrites with the numbers. The morsel-locality half was satisfied by
construction (a 4096-row int32 batch is 16 KB, L1-resident); the wide-shape
knee is open question 6 and rides task 25's harness.
