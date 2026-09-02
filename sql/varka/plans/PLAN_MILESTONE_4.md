# Varka Milestone 4 Plan: breadth

Milestone 3 closed with task 23, so this file is no longer the scope document it
opened as: it is the task plan that document promised, written against the
measurements it said should order it. The scope catalogue it grew from is kept
whole in section 10, with every item's number unchanged, because other plans cite
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
committed spine is tasks 24-31 (24-30 as planned, plus 31, added during task 24
at the owner's request; see 2.2).

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

This task changes emitted bytes, and the expectation written here before it
ran was that the two pinned shape hashes and the pinned line-map literal
would move with them. **They did not, and task 24 records why**
(`PLAN_TASK_24.md` section 5): the hashes are taken over the IR, the input
counts and the emit options, and the line map over the IR's topological
schedule - none of which a change to the emitted method structure touches.
So the pinned oracles were this task's behaviour-preservation proof rather
than its collateral. The spine's first task that legitimately moves them is
26, which adds IR nodes.

### 2.2 Asserting the instructions, not the ratio (task 31)

Added during task 24, at the owner's request, and scheduled before task 25
because it is task 25's instrument. Every vectorization claim this project
makes today is inferred from a throughput ratio - the parity gate's "emitted
loop within 0.9x of the hand-written kernel" stands in for "C2 intrinsified the
Vector API calls". Task 24 showed how weak that inference is: the same kernels
measured 50-190% apart under `-XX:CompileCommand=inline,jdk/incubator/vector/*.*`
in the engine's JMH harness and within 1% under it in the catalyst harness, so a
ratio can move for reasons that have nothing to do with the instructions
emitted. A test that reads the instructions cannot.

The mechanism is a forked JVM with `-XX:+UnlockDiagnosticVMOptions
-XX:+PrintAssembly -XX:CompileCommand=compileonly,<class>::<method>`, whose
output is scanned for the instruction *family* the shape should produce. Four
things decide whether this is a good test or a flaky one:

* **It asserts a family, never a mnemonic.** The lane width is a property of the
  host - `zmm` on AVX-512, `ymm` on AVX2, NEON on aarch64, and `xmm` under the
  narrow-vector CI run - so the assertion is "a packed integer add on a vector
  register of the width this host reports", derived from `IntVector.SPECIES_PREFERRED`
  rather than hard-coded. The interesting negative is a *scalar* body where a
  vector one was expected, and that is what the test names when it fails.
* **It skips cleanly without `hsdis`.** `PrintAssembly` degrades to a warning and
  bytecode-level output when the disassembler is absent, which is the likely
  state of a CI runner; the suite must detect that and skip rather than fail, and
  say which it did. It is a gate on the developer machine and the runners that
  have `hsdis`, not a gate that goes red for missing tooling.
* **It names methods the emitter generates.** Emitted classes are named for their
  shape hash, so `compileonly` takes a wildcard over the generated package and
  the loop-method naming scheme (`loopDense0`, `epilogueMasked`), which task 24
  made stable.
* **The kernels come first.** `DateVectorOps` is the reference and its shapes are
  fixed; the emitted loops follow, one per gating shape, so a regression is
  attributable to the emitter rather than to the whole stack.

The deliverable that makes this pay beyond a one-off: the assertion sits beside
the existing parity gate, so a future task learns from a *named missing
instruction* instead of from a number that drifted.

This task also owns the second half of the same question, deferred here by the
owner: **whether forcing C2 to inline Varka's own packages changes anything**.
Task 24 measured the JDK half - `-XX:CompileCommand=inline,jdk/incubator/vector/*.*`
moved the engine's JMH numbers by 50-190% and the catalyst benchmark by under 1%,
which turned out to be a fact about the JMH harness rather than about Varka (see
section 9's debt register). The same flag aimed at
`org/apache/spark/sql/varka/**` and at the emitted classes' package is untested,
and belongs with the assembly work because both answer "what did C2 actually do"
with evidence rather than with a ratio. Whatever it finds, a JVM flag cannot be
the shipped answer - it would have to be set on every executor - so the outcome
is either a documented recommendation in `docs/sql-varka.md` or a recorded
decline.

### 2.3 Instruction-level parallelism (task 25, item 13)

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

### 2.4 Calendar extraction, `year` first (task 26, item 6)

The one vocabulary item that fits milestone 2's machinery as it stands: int32
lanes, existing operators, task 14's range-narrowed magic multiply (this file
cited task 17 for it until task 26 traced it; the technique shipped as task
14's follow-up, `PLAN_TASK_14.md` 7.7). The task
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

### 2.5 Boolean outputs (task 27, item 5)

The cheapest item and the only pure continuation of milestone 3: comparisons
and `And`/`Or`/`Not` as projection *results*, built on task 21's mask-as-value
machinery. `VectorMask.toVector` against a `blend` of one and zero was
pre-registered as a measurement, not a debate, and it is now measured
(`VarkaMilestone4MeasurementsBenchmark`, committed run in
`sql/varka/engine/benchmarks/VarkaMilestone4MeasurementsBenchmark-jdk25-results.txt`,
four runs total, including two after merging task 24's PR with the machine's
performance mode on): the two are statistically tied at both vector widths,
on every run - neither wins the way the pre-registration expected. The real,
width-dependent finding is a different question the pre-registration did not
ask: whether to materialize an int column at all. Skipping it - packing
`VectorMask.toLong()` straight into the output bitmap - wins by 1.10-1.18x at
AVX-512 but *loses* by 1.40-1.60x at 128-bit, reproduced on every run. A
compound predicate, `(a > b) AND (c < d)` kept in mask space the whole way
through versus materialized as an int column at every node, shows a related
split: the winner flips at AVX-512 (by up to 1.07x either way), but
mask-space wins reproducibly at 128-bit on every run, by 1.24x-1.37x - never
worse, sometimes decisively better. Two consequences for the task: walk
boolean sub-expressions in mask space and materialize only once at the output
boundary (never worse, and the compound case argues for it directly), and the
single-comparison bits-only shortcut needs a width check rather than a single
committed choice, since its sign flips between the two vector widths this
project already tests at. The two real questions the pre-registration also
named are format and nulls: Spark's bit-packed boolean vector against
Arrow's validity-style bitmap at the output boundary, and the three-valued
rules holding there exactly as they hold in the interior - a null input
produces a null output, never a false one. The differential runs every null
pattern for exactly that reason.

### 2.6 Lane-width conversion (task 28, item 1)

The width machinery items 2 and 4 lean on. The hard part is not the
conversion, it is the lane count: at one shape an int32 species holds twice
the lanes of an int64 species, so a mixed-width kernel either drives the loop
at the narrowest lane count and leaves wide lanes half empty, or emits a part
loop per conversion and carries two trip counts. That is the one decision in
this item that is expensive to reverse, so the scope's open question 2 was
pre-registered as a measurement before the task opens: both shapes on a
`cast(int AS long) + long` chain. Measured
(`VarkaMilestone4MeasurementsBenchmark`, same committed results file as 2.5):
narrowest-drive and part-loop are statistically tied at both vector widths,
on every run - four total - narrowest-drive slightly ahead most of the time
(within 1.01x-1.07x, inside this file's own noise band). Part-loop's extra
bookkeeping - two trip counts, two stores per int chunk - buys nothing
measured, so task 28 opens already knowing the winner: narrowest-drive, for
the simpler build (one trip count) at the same throughput. The recorded
fallback if a wider mixed-type shape measures differently once task 28 is
under way: items 2 and the multiply half of 4 can be built width-locked and
retrofitted.

### 2.7 int64 lanes: `TimestampNTZ` and `bigint` (task 29, item 2)

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

### 2.8 ANSI-correct integer arithmetic (task 30, item 4)

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
file as 2.5): blend-then-`DIV` beats masked `DIV` at both vector widths, on
every run - four total - 1.08x-1.10x at AVX-512, 1.18x-1.19x at 128-bit by
minimum. The smallest margin of the five measurements in that file, but the
only one where all eight data points (two widths times four runs) agree in
both direction and rough magnitude, which is the interleaved comparison the
under-1.3x rule asks for. Blend a safe divisor into inactive lanes; the
structural check exists to make sure some such mechanism runs before an
unmasked `DIV`, not to leave the choice open each time.

### 2.9 One decomposition, several fields (task 32, from the debt register)

Added after task 26 measured what its own design cost, which is the only
reason it is here: `SELECT year(d)` runs at 1797 M rows/s and
`SELECT year(d), month(d), dayofmonth(d), quarter(d)` at 435 - 4.1x for four
fields, which is near enough to 4x that nothing is being shared but the column
load and the loop control.

The cause is structural rather than accidental. All four fields fall out of one
civil-from-days decomposition, and ~45 of a field's ~51 vector ops are that
shared work; only the last handful differ. But `Year(col0)`, `Month(col0)`,
`DayOfMonth(col0)` and `Quarter(col0)` are four distinct IR nodes, each
emitting the whole decomposition before its own tail. The emitter's DAG-CSE
cannot help: it memoizes on structural equality between *nodes*, and the values
worth sharing here - era, century, year of century, day of year, the
March-based month - are not nodes at all. They are locals inside one node's
emitted bytecode, invisible to the walk that would share them.

**The task opens with the ceiling, not the mechanism.** A hand-written kernel
computing all four fields from one decomposition, against the 441 M rows/s the
four separate nodes reach, at both widths. That gate exists because task 17
already measured the opposite of the obvious answer: raising `GROUP_BUDGET` so
two outputs could keep their cross-output CSE in one method *lost*, 4119.9
against 2928.2 M rows/s in the current committed parity file, because the wider
method's register pressure cost more
than recomputing the shared ops. Here the shared work is ~45 ops rather than
eight, but five values would have to stay live across four output tails, so the
same effect is in play and the direction is not predictable from op counts. If
the ceiling is close to 441, the task is declined with a task-16 reason before
any IR changes - which is a real possible outcome, not a formality.

If it clears, three mechanisms, in the order they should be considered:

1. **A multi-value node and its selectors.** `ChronoFields(days)` computes the
   decomposition into slots and leaves nothing on the operand stack;
   `Year(fields)`, `Month(fields)` and the rest read one slot each. This is the
   general answer: any future primitive with several results - `divmod`, a
   string operation returning an offset and a length, date-level `date_trunc`
   beside `year` - takes the same shape. It is also the expensive one, because
   the IR's whole contract is that a node evaluates to exactly one value:
   `emitValue`, slot planning, the CSE memo, `canonicalShallow` and the line
   map, the shape hash, and a rule keeping a multi-value node out of value
   positions the way `Cond` is kept out of them today.
2. **Emitter-side fusion, with no IR change.** When one group holds two or more
   calendar nodes over the same child, emit the decomposition once and branch to
   the tails. Local and cheap, but it argues with task 26's own node weight,
   which deliberately puts each calendar output in its own loop method; the
   grouping would need the opposite rule for exactly this case, and the fused
   method lands near 60 ops across four outputs - the multi-output shape task 11
   measured the C2 compile cliff on.
3. **Decomposing into primitive IR nodes** so ordinary DAG-CSE shares them.
   **Declined in advance, with the reason on the record**: one `year` is ~51
   ops, so a four-field projection would be ~60 distinct nodes against
   `MAX_FUSED_NODES = 64` and a `GROUP_BUDGET` of 16, and the IR would acquire a
   general arithmetic vocabulary to serve one family.

Whatever the outcome, the deliverable includes sweeping the debt register entry
in the past tense with what the measurement found, per `sql/varka/AGENTS.md`.

**Outcome: the gate clears at AVX-512, and the task proceeds. Replanned in
`PLAN_TASK_32.md`, which supersedes this section.**

It was first answered the other way. A ceiling kernel was built
(`sql/varka/engine/.../vector/ChronoVectorOps.java`, differentially tested against
`java.time.LocalDate` in `ChronoVectorOpsTest`), measured 225.8 M rows/s against 430.7 for
the four emitted nodes, and task 32 was declined on that number. **The number was wrong, and
in a way worth recording**: the kernel factored its decomposition into a `computeFields`
helper returning a record of four `IntVector`s, that helper compiled to 376 bytecode bytes,
and C2's `FreqInlineSize` is 325 - so it never inlined into the loop, the record and its four
vectors could not be scalar-replaced, and the kernel really allocated five objects per lane
group. `emitChrono`, the path it exists to model, emits no call boundary at all. The kernel
was measuring the cost of a Java abstraction the emitted code does not have. `SKILLS.md`
carries the general lesson.

Rebuilt with the whole lane path written out by hand (`javap` and `-XX:+PrintInlining` both
confirm no call survives in the loop), with the narrow-range guard it had omitted, and writing
four destination validity buffers instead of one - so that both arms are charged the same
things. Measured in `VarkaEmitterParityBenchmark`'s "year" section, same 4096-row chunks and
the same `eachChunk` walk as the case it sits beside:

| | AVX-512 (M rows/s) | 128-bit (M rows/s) |
|---|---|---|
| four separate emitted nodes | 450.4, 448.8, 435.1, 445.7 | 154.1 to 157.6 over five runs |
| shared decomposition, hand-written | **692.4, 678.8, 661.7, 679.0** | 165.6 to 167.0, once 236.1 |
| ratio | **1.54x, 1.51x, 1.52x, 1.52x** | 1.06x, once 1.50x |

So sharing is worth about **1.5x at AVX-512**, reproducibly, and is a **wash at 128-bit** -
four of five runs at 1.06x, one at 1.50x, with zero stdev inside each run and 42% between
them. That bimodality is a compilation the JVM either finds or does not; C1 declines the
936-byte body outright ("out of virtual registers in linear scan") at both widths. Task 17's
register-pressure effect is real and visible here, but as a width-dependent ceiling on the
size of the win rather than as a reversal of its sign: 32 vector registers and 8 mask
registers hold five live intermediates and four outputs comfortably, 16 vector registers
holding masks as well do not.

Mechanism 3 (decomposing into primitive IR nodes) stays declined in advance for the reason
above. Mechanism 1 (a multi-value IR node) is also declined, on a reason this section did not
have: mechanisms 1 and 2 emit *identical bytes*, so the choice between them is engineering
cost, not throughput, and mechanism 2 - emitter-side sharing keyed on (fragment, child node) -
needs no IR change and generalizes to tasks 33, 34 and 40's nodes for free. `PLAN_TASK_32.md`
section 3 has the design; the default is not flipped on the AVX-512 number alone, since the
narrow-vector shape has to be measured on the emitted path first.

**Step B1 is built and on by default; step B2 is still gated.** The fragment mechanism ships
(`VarkaEmitOptions.shareChronoPrefix`, `FragmentKey`, `emitChronoPrefixOnce`) with the
grouping policy untouched, so every calendar output still gets its own loop method and no loop
body has anything to share. What it changes is the epilogue, which task 24 made one method
over *every* output: four fields over one date now decompose once there rather than four
times, and the 8000-byte `HugeMethodLimit` crossing moves from 17 calendar outputs to 40.
That is a compilability win on a shape a user can write, it needed no benchmark to justify -
which is why the default flipped here rather than waiting on B2 - and it moved no pinned
oracle and no committed number, the latter established by a test asserting every loop method
is byte for byte what it was rather than by a re-measurement. `PLAN_TASK_32.md` section 7.1
has the ladder and the two places the plan turned out to be wrong (the fragment key needs the
validity word, and a 16-field projection over one date does not exist). B2 - the grouping
relaxation that buys the 1.5x - cleared its gate at both widths (`PLAN_TASK_32.md` 7.2, 7.4,
7.5) and is planned in that file's section 10: a grouping clause that admits an output into
a wider method only when it reuses a fragment the method already computed - so task 17's
plain-chain case stays split - bounded by a `fusedCeiling` that a ladder past four fields sets.

### 2.10 `next_day`, as a handover experiment (task 33)

The smallest piece of vocabulary the survey after task 26 turned up, taken for
a reason that is not about vocabulary at all: it is the first task written to
be executed by a cheap agent rather than by whoever planned it, and it is
chosen because it is the one candidate where nothing has to be decided.

`next_day(d, <literal weekday>)` is `d + 1 + floorMod(k - d, 7)` for a
compile-time `k`, and every piece of that already exists - the mod-7 magic
multiply from task 14's follow-up, the unary null-intolerant node shape, the
literal-in-a-slot convention from `date_add`. About seventeen vector ops,
twelve of them already measured as `dayofweek`. There is no measurement to
take, no range to guard, no lowering to choose between.

The one trap is a trap in the opposite direction from the one `SKILLS.md`
records. `k - d` does overflow near `Integer.MIN_VALUE` - but Spark's own
`getNextDateForDayOfWeek` computes it in plain `int` arithmetic and wraps, so
byte-exactness requires reproducing the overflow rather than avoiding it. The
planning pass wrote the careful version first and checked it: reducing before
subtracting disagrees with the row engine on the bottom handful of int days for
every weekday, 28 cases in the boundary set. `dayofweek` is the reverse case,
because its oracle is `LocalDate`, which never wraps. Whose arithmetic the
oracle is decides which way the rule points, and that distinction is now in the
recipe because it is exactly what a cheap agent would get wrong.

`PLAN_TASK_33.md` is written as a step-by-step recipe - exact files,
exact switches, the oracle to write the test against, the two-step form of the
narrow-vector run that `JAVA_OPTS` silently gets wrong - and its outcome
section asks the executing agent to record which steps turned out to be
misleading. That record is the point of the experiment: whether a task of this
shape can be handed over, and what a recipe has to contain before it can be.

The corpus does not ask for `next_day` any more than it asked for `month`.
This task is not claiming otherwise; it is buying a measurement of the handover
itself, and picking the cheapest possible payload to buy it with.

### 2.11 The rest of the date-field family (tasks 34-37)

Four more expressions the survey after task 26 turned up, taken for the same
reason task 33 was: each is a **tail on a decomposition that already exists**,
so the whole of each task is one IR node, one case in `emitChrono`'s tail
switch, and its tests. They are separate tasks rather than one because they are
meant to go to separate agents, and because they are genuinely independent -
the only ordering is that 34 builds the leap flag 35, 36 and 37 all want.

| task | expression | lowering | size |
|---|---|---|---|
| 34 | `dayofyear` | `doy >= 306 ? doy - 305 : doy + 60 + L` | ~10 ops |
| 35 | `trunc(d, 'YEAR'\|'MONTH'\|'QUARTER')` | `d - dayofyear + 1`, `d - dom + 1`, and a four-way quarter-start select | ~5-15 ops |
| 36 | `last_day` | `d + length - dom`, length from the same linear form the day tail uses, February special-cased | ~12 ops |
| 37 | `weekofyear` | ISO-8601: provisional week from day-of-year and weekday, then the two year-boundary corrections | ~60 ops |

**Every formula above was verified during planning against `java.time` over
all 3,652,059 days of `0001-01-01..9999-12-31` - zero mismatches**, by
`plans/verify_chrono_tails.py`, which is committed beside the recipes so the
claim is re-runnable by whoever is asked to trust it. That check
is why they are worth handing over: the four recipes carry arithmetic that has
already been run, so the executing agent is transcribing rather than deriving.
It also earned its keep immediately - the first draft of `dayofyear` used 59
where the answer is 60, and failed on 84% of days.

Two things the four have in common, both written into the recipes. Their
oracles are all `LocalDate`, which is exact, so the ordinary no-overflow rule
applies - the opposite of task 33, where Spark's own arithmetic wraps and the
lowering has to wrap with it. And the leap flag they need is computed from the
*reported* year with two magic multiplies over a year biased by 13200, rather
than from `yoc` and `century` with bit tricks that go wrong at the century and
era boundaries.

The corpus asks for none of them. As with 33, that is said plainly rather than
argued around: what these buy is a second, wider trial of the handover - four
tasks, four agents, one of them (37) deliberately harder than the rest, and
four outcome sections recording where the recipes misled whoever ran them.

### 2.12 A day offset that is a column (task 38)

Not a new expression: `date_add(d, n)` and `d + n` already reach the compiler
as `DateAdd`, and the emitter's arm for it is already vector-vector lane math.
What declines them when `n` is a column rather than a literal is four guards,
three of which exist to enforce milestone 1's scope - "foldable integer day
offsets" - rather than to protect against anything the engine cannot do.

The finding that makes this worth a task is that foldability is the visible
guard and not the real one: **Varka cannot read a non-date column at all.** The
compiler's only leaf is a `DateType` `BoundReference`, and `isArrowBacked`
requires every referenced column to be an Arrow `DateDayVector`. So this task
is really the input boundary opening by one type, and the day offset is what
makes that concrete and testable.

Two things in it can produce wrong answers rather than declines, which is why
it is written as a recipe rather than left as a one-line note. `planWordRef`
aliases `AddDays`'s validity to the date child alone - correct while the offset
is always a literal, wrong the moment it can be a nullable column, and the fix
is `andRef` over both children, which is provably a no-op for a literal because
`andRef(a, WORD_ALL_TRUE)` returns `a`. And `DateAdd.inputTypes` accepts
`ShortType` and `ByteType` **without a cast**, so a short column would be read
by an int32 lane load as garbage; those must decline by naming `IntegerType`
exactly rather than by accepting any integral type.

Because no node type is added and the literal path is untouched, this is the
one task in the milestone whose acceptance includes **neither pinned value
moving and no committed number moving** - which also makes it the easiest to
review.

The corpus does not ask for this either. What argues for it is that the door it
opens is on the way to everywhere else: an `IntegerType` column is the first
non-date input the engine has ever read, and items 2, 3 and 4 all need that
boundary open before they can start.

### 2.13 `date - date`, the first mixed-width kernel (task 39)

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

### 2.14 days-from-civil, and month arithmetic (task 40)

The headline of this task is not an expression. It is **days-from-civil** - the
inverse of task 26's decomposition - which `make_date`, `months_between`,
`date_trunc('QUARTER')` and interval month arithmetic all want, and none of
which has it. `date + INTERVAL n MONTH / YEAR` is what makes it concrete and
testable, and it comes with `add_months(d, n)` and `d - INTERVAL n MONTH` for
free: all three are the same node, and the subtraction arrives as a
`RuntimeReplaceable` the compiler already unwraps.

Notably this needs **none** of tasks 28, 29 or 30: a year-month interval is
physically a month count, so the whole thing is int32 and is available as soon
as 26 lands.

The investigation behind it turned up two things worth recording here rather
than only in the recipe.

**The inverse is cheaper than the forward direction.** Its divisions are by
400, 4, 100 and 5, all on small operands, so every one admits an *exact* magic
multiply with no correction step, where task 26's forward direction needed two
round-down magics with carries. Checked: the round trip is the identity over
all 3,652,059 days from year 1 to year 9999.

**The natural formulation of the month arithmetic does not work.** Folding the
year into a total month count and dividing by 12 puts the dividend near
400,000, far past the ~46341 bound an exact magic needs and past the ~160,000
that round-down plus one correction reaches. Keeping the dividend small - the
month index plus the offset, divided by 12, with the quotient added to the year
- makes it exact at `M = 43691, k = 19`, and bounds the literal the compiler
will accept at about two thousand years. The planning pass wrote the wrong one
first; `plans/verify_days_from_civil.py` is committed beside the recipe so the
right one can be re-run rather than trusted.

There is no vectorization-specific algorithm here to find, and the recipe says
so: Hinnant's `days_from_civil` and Neri-Schneider's optimized form are plain
branch-free integer arithmetic, which is exactly what makes them vectorize.
What is not avoidable is the decompose-adjust-recompose round trip, because the
clamp - 31 January plus one month is 28 or 29 February - needs the day of
month. About 90 ops, roughly twice `year`.

### 2.15 The two ends of the date-integer boundary (tasks 41, 42)

Both come out of the same sweep as tasks 34-40, and both are about the boundary
between a date and the integers it is made of rather than about calendar
arithmetic.

**Task 41, `unix_date` and `date_from_unix_date`**, is the smallest task in the
milestone and the only one that adds no IR node, no emitter code and no lane
arithmetic. Spark's implementation of each is `input.asInstanceOf[Int]` in
full: a date *is* a day count and these two only relabel the type. So the
lowering is two compiler arms that unwrap to the child, and the entry's output
type comes from the Catalyst expression as it already does. Neither pinned
value moves and no emitted bytes change.

The argument for it is not the functions, which nobody calls. It is that one
unsupported expression demotes a whole projection entry to the row path, so a
free relabel sitting in the middle of an otherwise fusible chain currently
blocks everything around it. The task's real test is a projection with one
ordinary entry and one relabelled one, which must fuse both.

**Task 42, `make_date`**, is the other direction and much the larger: three
integer columns in, a date out. It is the first expression to read three
integer columns - so it waits on task 38 - and the first whose result can be
**null for a non-null input**, which is what makes it worth a recipe.

Its three-way distinction is the thing an implementer will get wrong. A null
input is ordinary validity. An **invalid** date - month 13, 30 February - is a
*semantic* result: null in non-ANSI, an exception in ANSI. A year beyond what
the lowering's magic multiplies cover is an *engine limitation*, which declines
the batch in both modes and lets the row engine answer. Confusing the last two
gives wrong answers in one direction and spurious errors in the other. In ANSI
mode the invalid case also declines, because a lane cannot throw - the same
trick task 39 uses, and the reason this task needs no error machinery of its
own.

### 2.16 What `GROUP_BUDGET` does not bound (tasks 43, 44)

Both come out of the review of task 26, and both are the same discovery from
two sides: `GROUP_BUDGET` bounds one of the three method shapes the emitter
produces, and task 26's wide nodes made the other two visible. Neither is a
calendar problem - both would have arrived with any node worth more than a few
ops - so they are their own tasks rather than corrections to 26.

Both are **design tasks, not recipes**: each opens with a measurement whose
answer decides between three mechanisms, so neither is delegable the way tasks
33-42 are.

**Task 43: a loop method inside one output is unbounded.** `groupOutputs`
partitions *between* outputs and never inside one, so `GROUP_BUDGET` binds only
when the ops are spread across several. `CHRONO_WEIGHT` therefore separates
calendar nodes that are separate output roots and does nothing for calendar
nodes under one root. Measured on the emitter as it stands:
`CASE WHEN d < DATE '...' THEN year(d) ELSE month(d) END` is one root and emits
**one** loop method of 926 bytecode bytes; `least(greatest(year, month),
greatest(dayofmonth, quarter))` is one root and emits **one** method of 1672
bytes, roughly 190 vector ops. The budget's own javadoc records single-output
loops as healthy "at every width tried", and the width tried was 59 ops.

The task opens by finding out where that stops being true: single-output loops
at 60, 100, 150, 190 and 250 vector ops, measuring both steady-state throughput
and the time to reach it, which is the axis task 11 measured when it set the
budget. If there is a cliff, three mechanisms, and the choice is the task:
split inside an output (which the budget's javadoc rejects on register-residency
grounds, but rejected it without data past 59 ops); decline the shape at compile
time through `fitsBudgets`, which is honest and loses fusion for a
`CASE WHEN year ELSE month`; or accept it and record where the cliff sits so
the next wide node is weighed against a number.

**Task 44: the epilogue is one method over every output.** Task 24 decided that
deliberately - the epilogue runs one pass per batch, so the compile-time
argument behind `GROUP_BUDGET` does not apply to it - and that reasoning is
still right about compile *time* and silent about bytecode *size*. HotSpot
refuses to compile any method past `HugeMethodLimit`, 8000 bytes by default, so
past that the epilogue is not compiled by C1 or C2 at all and runs interpreted
with boxed vectors: the ~1% state the `GROUP_BUDGET` javadoc describes, on
every batch whose length is not a lane multiple.

Measured, by emitting the classes and reading the method: `epilogueMasked` is
7530 bytes at 16 calendar outputs and **8079 at 17** - so the limit is crossed
at seventeen, well inside `MAX_FUSED_NODES = 64`, and five date columns of four
fields is twenty. The same 32-output projection built from `date_add` instead
is 1811 bytes, so this is new with wide nodes rather than a standing property.

The task's own trap is that the benchmark cannot see this: every case in the
year section drives 4096-row chunks, so the epilogue's early return always
fires and the wide epilogue is never timed. That is exactly the lesson
`SKILLS.md` records from task 24 - a size ladder needs 4095 and 63 on it
deliberately - and getting the measurement to show the problem is half the
task. Then the mechanism: group the epilogue as the loops are grouped, bound it
by emitted bytes rather than op count, or decline the shape.

### 2.17 The validity write, which costs more than the arithmetic (tasks 45-47)

Added after task 32, and for the same reason 2.9 was added after task 26: a
measurement said something the design did not expect, and the number was worth
tasks.

Task 32's repaired ceiling kernel gives three AVX-512 points on one line -
`year` alone at 0.556 ns/row over ~50 vector ops, the shared four-field kernel
at 1.512 over ~65, and four independent nodes at 2.298 over ~200. Fitting them
puts the marginal cost of a vector op at about **0.0058 ns/row**, which makes
the shared kernel's entire civil-from-days decomposition worth about **0.38
ns/row of its 1.512**. The other **1.13 ns/row - three quarters of it - is
fixed per-lane-group cost**, and going from one output to four adds about 0.29
ns/row *per extra output*: at sixteen lanes that is ~4.6 ns per extra
store-and-validity pair per lane group, roughly eighteen cycles. A vector store
is a fraction of that.

The suspect is `VarkaVectorSupport.orValidityBitsAt`, and task 32 established
from `-XX:+PrintInlining` rather than from timings that it **does not inline**
inside a wide loop: 212 bytes, refused with `NodeCountInliningCutoff` on one
compilation and `callee is too large` on another, and neither
`-XX:CompileCommand=inline` nor `-XX:LiveNodeCountInliningCutoff` at 400000
lifts it. So each of the four calls per lane group is a real call doing bounds
checks, a four-arm switch on `groupBytes(lanes)` and a read-modify-write.

**This was a three-point fit, not a measurement, and task 32's step B2 gate
settled it** (`PLAN_TASK_32.md` section 7.2, finding 2):
`ChronoVectorOps.vectorFourFieldsNoValidity` is the same arithmetic and the
same guard with every validity buffer and every `orValidityBitsAt` call
removed, and across three runs it costs 0.65-0.67 ns/row against the
validity-carrying kernel's 1.50-1.52 - **55.6% to 56.7% of the ceiling
kernel's time is the validity write**, not the three quarters the fit
estimated (that number priced everything the decomposition does not touch, of
which validity is the majority but not all). The fit's direction was right
and tasks 45-47 are confirmed worth more than task 32's own mechanism on this
number alone.

Three tasks, in this order, because each may shrink the next:

* **Task 45, the null-free fast path.** Arrow permits an output vector with
  `null_count == 0` to carry no validity buffer at all, and the driver already
  knows `srcNullCount == 0` when it dispatches to the dense body. Today the
  dense path still loads `-1L` per lane group and calls `orValidityBitsAt`
  anyway (`emitLaneGroup`, the `loadConstant(-1L)` beside
  `invokestatic(SUPPORT, orValidityBits(s), ...)`). One fill in the driver -
  which already walks the buffer once to zero it - replaces every one of those
  calls on the shape most real queries take. The masked path is untouched.
* **Task 46, validity helpers that can inline.** The emitter already chooses
  the helper by *name* at emit time (`validityBits`, `orValidityBits`, which
  select the partial variants for the epilogue), and it knows the species, so
  the width can join the name: `orValidityBitsAt16` and its siblings, each about
  thirty bytes with the switch already resolved, under `MaxInlineSize` and
  therefore inlinable whether or not the call site is judged hot. The current
  helper cannot fold its switch because it cannot inline, and cannot inline
  because it has not folded; naming the width breaks that cycle. This one is
  generic - every Varka kernel calls these helpers, not just the calendar ones -
  which is both its value and the reason it needs the whole suite green at both
  widths rather than a calendar-shaped argument.
* **Task 47, one validity write per word instead of per lane group.** At sixteen
  lanes a lane group covers sixteen rows and touches two bytes; four lane groups
  fill one 64-bit word. Accumulating the bits in a register and storing once per
  64 rows turns four read-modify-writes into one store, and removes the
  read entirely. It is the largest change of the three - the loop grows a
  second, coarser stride, and the epilogue has to flush a partial accumulator -
  so it goes last, and only if 45 and 46 leave something on the table.

Task 45 is expected to make the null-free case nearly free and do nothing for
the masked one; 46 to pay across the board and most at narrow widths, where
lane groups are smallest and the per-group cost is amortised over four rows
instead of sixteen; 47 to pay only on the masked path once 45 has taken the
dense one away. All three are measured on `VarkaEmitterParityBenchmark`'s
existing cases rather than new ones, because the point is what they do to
kernels that already exist.

### 2.18 A `year` that does not compute the month (task 48)

`emitChrono`'s year tail needs one bit out of the March-based month: whether
the March year has turned January, which is `mp >= 10`. But `mp` is
`(5 * doy + 2) / 153`, so `mp >= 10` is exactly `doy >= 306` - integer
arithmetic, no approximation, and `doy` is already in a local when the tail
runs. So `year` alone never needs the month step at all: one compare replaces a
multiply, an add, a magic multiply and a shift.

It is worth its own task rather than a line in another because of which shape
it helps. `year` alone is what TPC-H q7, q8 and q9 run, and it is the only
calendar extraction the headline corpus asks for; it is also the case task 26
measured at 1797 M rows/s and the one every later calendar task is compared
against. Four or five ops off a ~50-op body is a few percent, which is inside
the noise of a single run and therefore has to be measured on the
interleaved-A/B, compare-by-minimums methodology rather than asserted.

It does **not** help a shared prefix: if task 32's step B lands, the prefix
computes `mp` for the month, day-of-month and quarter tails regardless, and
`year` reading `doy >= 306` instead saves one op rather than five. So this task
is about the single-output path, it is independent of task 32 either way, and
whichever of the two lands second inherits the smaller half of the win.

**Update, after step B1 landed first**: read literally, that leaves this task the one-op
half. `PLAN_TASK_48.md` does not accept it: the prefix's month step is dead work exactly
when no consumer of that prefix reads `marchMonth`, and the emitter knows its consumers
per body at plan time, so the step becomes conditional on them - the full five-op win for
a `year`-only kernel, correctly nothing for `year(d), month(d)` in one method, and for free
to `dayofyear` and `trunc(d, 'YEAR')`, which test `doy >= 306` themselves. The identity is
proved in that plan's section 2 and asserted over all 366 values rather than stated.

### 2.19 Exact civil-from-days in long lanes (task 49)

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

### 2.20 Making a bad register allocation visible (task 50)

Task 32 spent six failed hypotheses on a kernel that ran at either 165 or 236 M
rows/s under `-XX:MaxVectorSize=16` - stdev 0 inside a run, 42% between runs -
before the cause turned out to be C2's register allocator. The two compilations
contain *identical* vector op counts; the whole difference is spill traffic,
four stack moves against seventy-four. The allocator sometimes finds a clean
assignment for a body that sits at the edge of the 16-register xmm file and
sometimes does not, from the same IR. `SKILLS.md` has the evidence.

The structural answer is task 32's own: do not put four outputs in one loop
method at a width whose register file cannot hold them, which is what the
`shareChronoPrefix` decision becomes once its default is made width-dependent.
This task is the other half - **not preventing it, but noticing it** - because
today a badly-allocated kernel is completely invisible. It costs 30 to 40% and
nothing anywhere reports that it happened.

**It is observable with public API.** JFR's `jdk.Compilation` event carries
`method`, `compileLevel`, `isOsr` and `codeSize`, and
`jdk.jfr.consumer.RecordingStream` (public since JDK 14) can consume those
events in-process with no agent and no diagnostic flags. The fast and slow
allocations of the same kernel differ by about 2x in compiled size - 1581
instructions against 3000 - so the anomaly is plainly present in that one field.

**The expectation is self-calibrating, which is what makes this worth building.**
The obvious design is a committed table of expected sizes per shape, and it is
the wrong one: it has to come from somewhere and it drifts every time the
emitter changes. Varka already keys every kernel by a shape hash, and the same
shape emits byte-identical bytecode, so the comparison is between *compilations
of the same shape hash* rather than against any constant. The first compilation
of a shape establishes the size; a later one that differs materially is the
report. No table, no drift, and it gets more accurate the longer a JVM lives.

Scope, deliberately narrow:

* A `RecordingStream` subscribed to `jdk.Compilation`, filtered to Varka's
  generated kernel classes, with OSR compilations excluded - they are not what
  the steady-state path runs and task 32 found them identical across both modes
  anyway.
* Per shape hash, the first non-OSR `codeSize` seen, and a metric plus a debug
  log when a later one for the same hash differs by more than a threshold the
  task picks from measured data rather than guessing.
* Off unless enabled, through Varka's own configuration surface. Subscribing to
  `jdk.Compilation` is not free and this is a diagnostic, not a feature, so it
  should cost exactly nothing when nobody has asked for it.
* **A diagnostic and never a control loop.** Section 9's debt entry records the
  detect-and-re-emit idea and why it is not being built.

Risks worth stating: JFR may be unavailable or disabled in a deployment, in
which case this reports nothing and must degrade silently; the stream costs a
thread; and a shape whose kernel is only ever compiled once in a JVM produces no
comparison at all, which is the common case for a short query and means this
mostly serves long-lived sessions.

### 2.21 Remove the per-extraction range guard (task 51)

Every calendar extraction (`Year`, `Month`, `DayOfMonth`, `Quarter`,
`DayOfYear`, `LastDay`, `AddMonths`) has carried a per-lane range check since
task 26: two compares against `VarkaChrono.NARROW_MIN_DAYS`/`NARROW_MAX_DAYS`,
ANDed with validity and the epilogue's bounds mask, ORed into a body-wide
accumulator that declines the whole batch to the row engine if any lane's day
fell outside the range the narrowed civil-from-days lowering is proven exact
over. The guard was correct and load-bearing when it shipped.

The owner's objection, raised while reviewing task 36's own copy of the same
guard: the check re-verifies a fact at every calendar extraction that reads a
given value, when CSE and task 32's fragment sharing already prove the *same*
value's range once, for every field read off it in the same query. A query
extracting `year`, `month` and `last_day` from one column pays the guard once
today, not three times, because the fragment sharing already collapses the
extractions onto one shared prefix - but a query with a hundred *different*
calendar expressions, none of them CSE-equal, still pays it a hundred times for
what is, in the cases that matter, the same underlying guarantee: the day came
from a column, which the project's own contract already promises is
`[0001, 9999]`, or from arithmetic the compiler already bounded (`add_months`'s
literal-month-count check, task 40).

That argument does not cover every case. `date_add`/`date_sub` with a *column*
offset (task 38) can push a day arbitrarily far from any bound using a runtime
value the compiler cannot see - the value did not cross the Spark boundary as
data, Varka's own `AddDays`/`SubDays` node manufactured it. A guard at the
extraction is not redundant for that value; it is the only place a check has
ever existed for it, since the arithmetic node that created the value carries
no check of its own. So the guard's job splits into two real questions -
"is this a fact already established elsewhere" (the case for removing the
extraction-side check) and "who established it, and where" (the case for a
narrower check somewhere else) - and the owner's ruling was to act on the first
now and treat the second as its own task: **remove the guard, then add it back
only at the nodes that can actually manufacture an out-of-range day**, tracked
as task 52.

**What changed.** `hasChrono` (whose only caller decided whether a body
allocated a guard accumulator) is deleted. `emitEra` no longer emits the two
compares, the validity/epilogue-mask ANDing, or the OR into `s.guardAcc`; it is
now only the day-of-era arithmetic, unconditionally. `s.guardAcc` is therefore
always null today, and `emitStatusReturn` - already written to return a
constant zero whenever nothing set a guard - needed no change at all to do the
right thing. The `int run` ABI, `STATUS_CHRONO_RANGE`, the evaluator's fallback
routing and its metric all stay: nothing sets the bit today, but task 52 is the
next task and would need every piece of this back immediately, so leaving it
is not speculative scaffolding, it is scoped, already-planned reuse.

**What this costs, honestly.** `VarkaChrono.narrowed` is unchanged and is
still undefined - not merely inaccurate - outside `NARROW_MIN_DAYS`..
`NARROW_MAX_DAYS`. Before this task, a day outside that range was declined to
the row engine; after it, the same day is computed anyway and can produce a
wrong year, month, day or quarter with no signal above debug logging. This is
a real, temporary violation of the ghost-fallback contract in
`sql/varka/AGENTS.md` ("a Varka failure degrades to the row engine and never
returns a wrong answer"), accepted deliberately by the owner rather than found
and fixed, and it stays open until task 52 lands a check at the nodes that can
actually produce such a day. Two differential tests that asserted the old
decline behaviour end to end were removed rather than rewritten to assert the
new, weaker one (`VarkaDifferentialSuite.scala`, see the test file for the
pointer back here); two unit tests that checked the same thing at the emitter
level were rewritten to assert that an out-of-range day is now computed, not
declined (`VarkaLoopEmitterSuite.scala`).

**No emitted byte moves for an in-range shape.** The guard's removal deletes
code, it does not change the arithmetic any calendar node runs on an in-range
day, so neither pinned fixture (`VarkaLoopEmitterSuite`'s line map,
`VarkaShapeCacheSuite`'s shape hash) moves, and no committed parity number is
expected to change - a body that used to also compute a guard mask now simply
does not, which can only help, and is not itself a claim this task measures.

### 2.22 Guard at the producer, not the extraction (task 52)

The other half of 2.21's ruling, not yet built. Where task 26's guard checked
every calendar *extraction's* input, this task checks the *producer* nodes
that can put a day outside `VarkaChrono`'s narrowed range using a value the
compiler cannot bound at compile time - today, that is exactly `AddDays`/
`SubDays` when the offset is a column rather than a literal (task 38's
day-offset support). **Second version of the plan:** a literal offset is not
bounded either - `foldDaysOffset` accepts any `Int`, so
`year(date_add(d, 20000000))` fuses today - and the task now opens with a
compile-time day-shift interval analysis that declines such an entry for free,
with the runtime guard reserved for the genuinely unbounded column-offset case.
`PLAN_TASK_52.md` section 1 has the rule. `NextDay`'s own offset is not in this set even though it
takes the same `(days, offset)` shape: task 33's compiler arm accepts only a
foldable weekday and always compiles it to a `LiteralSlot`, and floorMod7's
result is bounded to `[0, 6]`, so `NextDay` cannot move a day far enough to
matter and needs no guard of its own. `add_months`'s literal month count is
already bounded at compile time too (task 40's `MONTH_ARITH_MIN_MONTHS`/
`MONTH_ARITH_MAX_MONTHS` decline), so it needs no runtime check under this
scheme either; a bare `ColumnRef` needs none, under the project's standing
contract that column data is `[0001, 9999]` at the Spark boundary. A downstream
calendar extraction trusts whatever its input already established instead of
re-checking it - which is exactly task 51's removal, now paired with a check
that actually covers the gap it opened.

**Shape.** Reuses task 51's still-live plumbing: `s.guardAcc`,
`emitStatusReturn`'s zero-vs-`STATUS_CHRONO_RANGE` return, and the evaluator's
existing fallback route and metric. The new work is entirely in deciding
*which* nodes set the accumulator - the column-offset arithmetic nodes, not
the calendar extractions - and only when their offset operand is not a
literal the compiler already bounded.

**Behind a flag, off until measured.** Every column-offset `date_add`/
`date_sub`/`next_day` pays this whether or not a calendar extraction ever
reads its result, which is a different cost shape than task 26's guard (paid
per calendar output, shared by fragment sharing) - it needs its own number
before it is the default, the way task 32's `shareChronoPrefix` and task 49's
long-lane lowering each earned their default from a measurement rather than an
argument. A `VarkaEmitOptions` switch, default off, is where task 52 starts;
the owner picks the default from the number the way every other guard-shaped
decision in this milestone has been decided.

**Validation.** A differential shaped like the two task 51 removed - a
column-offset `date_add` pushing a date past the range, checked end to end
through both the projection and filter paths - but anchored on the producer
node rather than the extraction, since that is now where the check lives. Both
flag settings green; a committed number for the guard's cost isolates what it
adds on top of the arithmetic it protects.

## 3. Task breakdown

Tasks 24-44 are the committed spine, in dependency order: 24 halves the
per-node emitter surface every later task would otherwise pay twice; 31 gives
25 an instrument that reads instructions rather than ratios, which is what 25's
central question needs (see 2.2); 25 shares
24's harness and changes how every later kernel is emitted; 26 and 27 spend
milestone 2's machinery before 28 complicates it; 28 enables 29 and 30's
widening. 32 and 33 are the two tasks here that no scope document predicted. 32 exists
because 26 measured what its own design cost and the number was worth a task
(see 2.9), which is the milestone's own rule about debts working as intended;
33 exists to measure something else entirely - whether a task can be handed to
a cheap agent as a recipe (see 2.10) - and picks the smallest payload it can
to do it. 34-37 widen that trial to four more payloads of increasing size
(see 2.11), and each of them makes task 32's debt a little more expensive,
which is worth watching rather than ignoring.
45-48 are the third unplanned addition, and they arrive the way 32 did: task
32's own measurement, once it was repaired, showed that three quarters of the
four-field kernel's time is validity bookkeeping rather than date arithmetic
(see 2.17). 45, 46 and 47 are that bookkeeping, and unlike everything else in
this milestone they are not about the calendar at all - every Varka kernel
writes validity, so whatever they win, every kernel wins. They run after 32
because 32's kernel is the instrument that measures them, and 45 opens by
turning its own premise into a number before any of the three is built. 48 is
unrelated to all of it and is here only because task 32's arithmetic review
noticed it (see 2.18). 49 comes from the same review asking why the calendar
lowering is range-narrowed at all, and finding that the answer - no
multiply-high on int lanes - stops applying once the lanes are int64 (see
2.19); it depends on task 29 and it competes with task 32's step B rather than
adding to it.
Items 7, 10, 9 and 8 are the follow-on ladder in that order - each
needs its own argument to enter, per the milestone 3 rule. Numbering continues
the single sequence; this plan has already grown twice the way milestone 3's did
(task 31, section 2.2, then tasks 32-44, sections 2.9 to 2.16, and now tasks
45-48, sections 2.17 and 2.18, task 49, section 2.19, task 50, section 2.20,
and now tasks 51 and 52, sections 2.21 and 2.22), so milestone 5
resumes at 53.

Task 51 is a fourth unplanned addition, and unlike 32, 45-48 and 49 it did not
come from a measurement - it came from the owner questioning task 26's guard
design directly, mid-review of task 36 (see 2.21). 52 is 51's other half,
tracked separately because the owner asked for the guard's removal and its
replacement to ship as two decisions rather than one: 51 is done, 52 is a plan
only, and nothing currently blocks it from being picked up next.

| # | Task | Deliverables | Validation |
|---|---|---|---|
| 24 | The scalar tail, interrogation, compaction. **DONE** (`PLAN_TASK_24.md`) | The tail-cost measurement (open question 3) recorded first; the unmasked-body-plus-masked-epilogue loop via `indexInRange`, deleting the emitter's second scalar IR walk; `compress(mask)` compaction in `VarkaFilterExec` against the committed ~1-3 ns/row ceiling, with the non-AVX-512 verdict; per-lane-group `anyTrue`/`allTrue` fast paths | Differential green at both vector widths, all null patterns, all-selected and none-selected; the pinned hashes and line map unchanged, which is the proof the refactor preserved behaviour (they were expected to move; see `PLAN_TASK_24.md` section 5); filter ladder re-run and committed; emitter per-node surface reduction stated as a number |
| 31 | Assert the instructions, not the ratio | A forked-JVM `PrintAssembly` harness; host-derived instruction-family assertions over the `DateVectorOps` kernels and one emitted loop per gating shape; a clean skip where `hsdis` is absent | The suite fails on a scalar body where a vector one is expected, and says which method and which family; green at both vector widths; skipped-not-failed on a runner without a disassembler |
| 25 | ILP: the unroll factor as a plan decision | The registered prediction, then the three-confounder matrix (K x broadcast strategy x `GROUP_BUDGET`) on `dayofweek`, unpredictable `CASE WHEN`, and the depth-8 chain; if K > 1 pays, per-shape K chosen from the live-temporary count the emitter already computes; the `SKILLS.md` bullet rewritten with the numbers; the batch-size knee sweep (question 6) on a wide fused shape | A committed number per candidate shape against its existing baseline; prediction scored honestly; no committed number regresses on shapes where K stays 1 |
| 26 | Calendar extraction, `year` first. **DONE** (`PLAN_TASK_26.md`) | The four-constant range-narrowing admission check, recorded before emitter work; `year`, `month` and `dayofmonth` committed - one civil-from-days decomposition yields all three - with `quarter` riding `month` and `dayofyear`/date-level `date_trunc` as the algebra yields them; fields whose constants will not narrow declined with a task-16 reason | Differential across the Gregorian range including pre-1970, leap years, month-length boundaries and the 400-year cycle edges, at both widths; parity numbers committed; `year` demonstrably compiling on the TPC-H q7/q8/q9 shape |
| 27 | Boolean outputs | Mask-to-column materialisation (`toVector` against `blend`, measured); the bit-packed format decision at the Spark/Arrow boundary; three-valued rules holding at the output boundary | Differential over every null pattern - a null input never becomes false; `SELECT d > DATE '2000-01-01' AS flag` and filter-leftover boolean columns compile; committed number on one boolean-output shape |
| 28 | Lane-width conversion | The mixed-width loop-shape measurement (open question 2: narrowest-drive against part loops) on `cast(int AS long) + long`, committed before integration; `convert`/`convertShape` emission following the winner; numeric `Cast` and Catalyst's implicit promotions over the supported types | Differential on mixed int32/int64 trees at both widths; the loop-shape decision recorded with its numbers; no regression on single-width shapes |
| 29 | int64 lanes: `TimestampNTZ`, `bigint` | The second `LaneType`; `TimestampNTZ` comparisons, differences, literal arithmetic; `TimestampType` and `LongType` comparisons and diffs; range-narrowed magic constants for 1000000 and 86400 or a recorded decline; the field differential mode from task 22 | Every parity gate re-run at the long species and both vector widths; the halved-headroom number committed rather than discovered; zoned operations demonstrably declined, not wrong |
| 30 | ANSI integer arithmetic | `try_add`/`try_subtract`/`try_multiply` via the difference-mask-as-validity path; the ANSI throw path via saturating detection and scalar re-walk, priced with a registered prediction; `Multiply` overflow through 28's widening if it is cheap, declined with a reason if not | The error-identity differential: same `SparkException`, same row, as the row engine under ANSI; `try_*` differential over overflow-dense and overflow-free data; committed number on the no-overflow path against Janino |
| 33 | `next_day`, as a handover experiment. **DONE** (`PLAN_TASK_33.md`, PR #61) | The node, the compiler arm declining every non-literal weekday, and the emitter arm over the existing mod-7 lowering; `PLAN_TASK_33.md` written as an executable recipe and scored in its own outcome section on which steps misled the agent that ran it | Every Varka suite green at both widths; the two pinned fixtures re-pinned under their update rule; no committed benchmark number moves, since the task adds a node type and changes no existing shape |
| 34 | `dayofyear` | The node, the January-based conversion off `emitChrono`'s March-based day of year, and the shared leap-flag helper tasks 35-37 reuse | Every Varka suite green at both widths; the pinned fixtures re-pinned; a day outside the covered range still declines (**this decline was removed by task 51**; see 2.19's update note and `PLAN_TASK_51.md`) |
| 35 | `trunc(date, YEAR/MONTH/QUARTER)` | One node carrying the level as a shape-bearing field, three lowerings, and the decline path for every level and format this task does not cover | As 34, plus a `DateType` output proved to feed further date arithmetic in the same chain |
| 36 | `last_day` | The node and the month-length tail, with February's leap case as its own branch | As 34, with every month length exercised in both a leap and a common year (the decline this inherited from 34 is likewise removed by task 51) |
| 37 | `weekofyear` | The node, the ISO-8601 rule including both year-boundary corrections, and the weeks-in-year helper called for two years | As 34, plus a dense day-by-day sweep across forty year boundaries rather than a boundary list |
| 38 | A day offset that is a column | The four guards moved, the `andRef` validity fix, `IntegerType` leaves and Arrow `IntVector` inputs accepted, short and byte offsets declining | A null offset producing a null row, at both widths; short and byte columns declining; **no pinned value and no committed number moves**, since no node type is added and the literal path is untouched |
| 39 | `date - date` | The node, the int32-to-int64 conversion, the eight-byte output, and both overflow tests routed through task 26's decline channel rather than task 30's throw path; the legacy `CalendarInterval` variant declining | The overflow boundary exact in both directions (106751991 succeeds, 106751992 declines); Varka's exception identical to the row engine's, compared by running both; `datediff` unaffected; green at both widths, where an int64 lane holds a different number of rows |
| 40 | days-from-civil, and month arithmetic. **DONE** (`PLAN_TASK_40.md`, PR #67) | `emitDaysFromCivil` as a helper three later expressions can call; the node behind `date +- INTERVAL n MONTH/YEAR` and `add_months`; the small-dividend month arithmetic and the literal bound it implies | The round trip tested on its own, not only through the expression; the clamp cases in both directions; a non-foldable or over-large month count declining; green at both widths |
| 41 | `unix_date` / `date_from_unix_date` | Two compiler arms that unwrap to the child, no IR node and no emitted code; the bare-`ColumnRef` output shape tested | A projection mixing a relabelled entry with an ordinary one fuses both; no pinned value moves, no committed number moves, no emitted bytes change for any existing shape |
| 42 | `make_date` | The three-child node, the validity predicate as a computed word in non-ANSI and a decline in ANSI, and the engine's year limit declining in both modes | The three-way distinction tested apart - null input, invalid date, unsupported year; both ANSI settings; the ANSI exception identical to the row engine's, compared by running both |
| 43 | What bounds a loop method inside one output | The cliff located first - single-output loops at 60 to 250 ops, throughput and time-to-peak - then split, decline or accept, chosen on that number | A committed number per width; whichever mechanism wins, `CASE WHEN year ELSE month` either fuses within a stated bound or declines with a recorded reason |
| 44 | The epilogue's size | A size ladder that can see the problem (4095 and 63, not only 4096), the epilogue measured against `HugeMethodLimit`, and the mechanism chosen on it | The wide-projection epilogue compiles, or declines; the committed ladder shows the epilogue's cost at a non-aligned length, which no committed case does today |
| 32 | One decomposition, several fields. **REPLANNED; step A and step B1 done, step B2's gate cleared** (see 2.9 and `PLAN_TASK_32.md`) | The first ceiling kernel measured a non-inlining `computeFields` helper rather than the sharing, and was rebuilt hand-inlined, guarded and writing four validity buffers: 692.4/678.8 against 450.4/448.8 M rows/s at AVX-512 (1.5x), and a wash at 128-bit (1.06x, one run of five at 1.50x). Step B builds emitter-side fragment sharing behind a `VarkaEmitOptions` switch, with the default decided at both widths rather than closed. Step B1 built the fragment and made it the default: the epilogue's `HugeMethodLimit` crossing moves from 17 calendar outputs to 40 (**task 51 moves this again, to 19/44 - see the debt register**), and B2's grouping relaxation stays gated on the two-field measurement. **The gate cleared** (`PLAN_TASK_32.md` 7.2): 1.29x at two fields, 1.57x at three, 1.80x at four, growing rather than shrinking against prediction 3's expectation - and the emitted dense-body shared kernel turns out to beat `ChronoVectorOps`'s own "ceiling" by 1.15-1.20x, since that kernel has no dense path and was measuring the masked body throughout - and the same 1.29x/1.57x/1.80x pattern reproduces at 128-bit (1.31x/1.47x/1.67x), so the width-dependent bimodality that made the hand-written ceiling a wash at 128-bit (`PLAN_TASK_32.md` 7.4) turns out to belong to that specific kernel and not to the sharing mechanism. The compile-cliff risk `GROUP_BUDGET` itself exists to avoid was also measured directly rather than assumed (`PLAN_TASK_32.md` 7.5): `-XX:+PrintCompilation` on every kernel in the parity suite shows the widest single loop method (200 ops, four fields) reaching tier 4 in 272 ms and the widest kernel overall (twenty separate methods) in 2.4 s, nowhere near the historic 10-second cliff and with no `blocked` compile task anywhere in the log | `ChronoVectorOpsTest` differentials the kernel against `java.time` over its exact sweep range and a boundary set, at both widths (the engine module's own narrow-vector Maven profile); the emitted lowering swept against `LocalDate` over all 16,777,216 covered days under both settings; no pinned oracle moved, and every loop method asserted byte for byte unchanged, so no committed number for any existing shape can have |
| 45 | The null-free validity fast path. **Planned** (`PLAN_TASK_45.md`) | The bound first: a validity-free variant of `ChronoVectorOps` sizing the prize (2.17), then the dense driver filling the output validity once per batch instead of the dense loop ORing it per lane group | The whole Varka suite at both widths with the dense/masked pair still agreeing bit for bit; the committed parity cases regenerated in one run, with the null-free and mixed-null rows of each moving in opposite directions or not at all |
| 46 | Validity helpers that inline | Width-specialised `validityBitsAt`/`orValidityBitsAt` siblings under `MaxInlineSize`, selected by the emitter's existing name choice, with the switch resolved at emit time | `-XX:+PrintInlining` showing no `failed to inline` for them in a wide loop - the diagnostic, not the timing, is the deliverable - plus the full suite at both widths and one parity regeneration |
| 47 | One validity write per word | Bits accumulated across lane groups and stored once per 64 rows, with the epilogue flushing a partial accumulator | The masked path's committed cases, the 4095/63 non-aligned lengths task 44 adds, and the dense/masked agreement; gated on what 45 and 46 leave |
| 48 | A `year` that does not compute the month. **Planned** (`PLAN_TASK_48.md`) | `doy >= 306` replacing the March-month step in the year tail only, with the equivalence recorded as an integer identity rather than an approximation | The existing exhaustive `VarkaChronoSuite` sweep unchanged and still green; the parity `year` case measured by interleaved A/B compared by minimums, since the expected effect is inside a single run's noise |
| 49 | Exact civil-from-days in long lanes | The admission check first, over all 2^32 days against a long-arithmetic reference: exact magic division by 146097, 36524 and 365 with a 64-bit low product and no correction carries; then the lowering, and the guard, the decline path, the `NARROWED` variant and `VarkaChrono`'s range constants removed with it | The exhaustive sweep as a committed opt-in test, at both widths; the parity `year` case measured against the shipped narrowed lowering in one run; declined on the record if the sweep disagrees anywhere or AVX-512 costs more than 0.75x |
| 50 | Make a bad register allocation visible | A `jdk.Compilation` JFR stream filtered to Varka's generated kernels, non-OSR only, comparing `codeSize` between compilations of the same shape hash rather than against any committed table; a metric and a debug log on divergence; off unless enabled | The stream observed to see Varka kernel compilations and report their sizes at both widths; zero cost when disabled, asserted rather than assumed; explicitly no re-emission on detection (see section 9) |
| 51 | Remove the per-extraction range guard. **DONE** (`PLAN_TASK_51.md`) | `hasChrono` and `s.guardAcc`'s allocation deleted; `emitEra`'s two compares and the mask ANDing/ORing into the accumulator removed, leaving only the day-of-era arithmetic; `emitStatusReturn`, the `int run` ABI and `STATUS_CHRONO_RANGE` left in place, unset, for task 52 to reuse; the two guard-specific differential tests removed and the two guard-decline unit tests rewritten to assert the new, weaker behaviour | Every Varka suite green at both widths in both modules; the two pinned fixtures unchanged (no emitted byte for an in-range shape moves); `dev/lint-java`, `dev/scalastyle`, `build/sbt catalyst/doc` clean |
| 52 | Guard at the producer, not the extraction. **Planned** (`PLAN_TASK_52.md`, second version) | A compile-time day-shift interval analysis in the compiler that declines a calendar entry whose literal `date_add`/`date_sub` chain can leave the narrowed range's slack around the `[0001, 9999]` contract (free, not flagged, closes the gap reachable on master today), and a flag-gated runtime guard on `AddDays`/`SubDays` with a column offset (PR #62) that a calendar node consumes - the old guard's bytecode at the producer's output, once per distinct producer, reusing task 51's still-live `s.guardAcc`/`STATUS_CHRONO_RANGE` plumbing | The bound's edges at `+-1` in the compiler suite; the producer guard declining in a loop lane, an epilogue lane and not under a null offset; `date_add(d, off)` alone byte-identical under both flag values; the two differentials task 51 removed restored around the producer and the compile-time decline; a committed number for the guard's cost on the one shape that pays it |

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
* **Scope creep through the catalogue.** The spine is 24-31. Items 7, 10, 9
  and 8 are real and stay in section 10 with full design input; each enters
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
  missing (`VarkaMilestone4MeasurementsBenchmark`, section 2.5's committed
  results file, `addAligned`/`addMisaligned`): a buffer start offset by 4
  bytes (still 4-byte int-aligned, but every AVX-512 load then spans two
  64-byte cache lines) costs 1.56-1.79x throughput at the default width and
  1.22-1.25x at 128-bit, reproduced on all four runs (including two taken
  after merging task 24's PR with the machine's performance mode on, which
  changed nothing), over the L1/L2-resident 4096-row working set every real
  Varka kernel actually runs at.
  Section 2.3's ILP item does not absorb this for free either: a 2-way
  unrolled version of the same misaligned kernel (not committed - a scratch
  check, not this file's methodology) still lost 50-60%, so unrolling and
  alignment are independent levers, not substitutes. The measurement item 13
  was waiting on is done; what stays out of milestone 4's committed spine is
  the enforcement itself - an allocator-level change - which is now a design
  question with real numbers behind it rather than a deferred unknown, to be
  argued in with its own task the way item 13 or the string items would be.
* **Whole-stage code generation** - in the charter (`VISION.md` section 13),
  not in this milestone.

## 9. Debt register

One bullet per debt: what it is, why it is a debt, and what closing it would
take. Opened during task 24, per `sql/varka/AGENTS.md` - a swept entry is
rewritten in the past tense with what the sweep found, never deleted.

* **`GROUP_BUDGET` bounds one of the emitter's three method shapes.** **Adopted as tasks
  43 and 44 (see 2.16)**, both found by the review of task 26 rather than planned.
  `groupOutputs` partitions between outputs and never inside one, so a single output root
  holding several wide nodes emits one unbounded loop method - measured at 1672 bytes and
  roughly 190 vector ops for `least(greatest(year, month), greatest(dayofmonth, quarter))`,
  against the 59-op width the budget's own evidence covers. And the epilogue is one method
  over every output by task 24's deliberate decision, which is right about compile time and
  silent about bytecode size: `epilogueMasked` measured 7530 bytes at 16 calendar outputs
  and 8079 at 17, crossing the 8000-byte `HugeMethodLimit` past which HotSpot compiles
  nothing at all. Neither is a calendar defect; task 26 only made them reachable.
  **Task 32 step B1 moved the second number and did not close either task**: sharing the
  civil-from-days prefix between calendar outputs over one date takes the crossing from 17
  outputs to 40 - four date columns to ten - with the full ladder in `PLAN_TASK_32.md`
  section 7.1. Task 44 therefore plans against ten columns rather than four, and task 43's
  case is untouched, because a single output holding several wide nodes shares nothing.
  **The crossing's cost is now priced, not just its bytecode size**
  (`PLAN_TASK_32.md` section 7.3, twenty calendar outputs over five dates): 1.36x at both
  an aligned chunk (64 rows) and a batch one row short of aligned (4095), and 7.3x where
  most of a batch is remainder (chunk 63). The 1.36x at chunk 64 is the more important of
  the two, because it fires even though the epilogue does no real work there -
  `emitEpilogue`'s own generated body returns immediately when the batch divides evenly,
  but the *method itself* is still called on every batch, and calling into a method
  HotSpot will never compile at any tier costs something even when that call does nothing.
  **This corrects the framing above**: "runs interpreted... on every batch whose length is
  not a lane multiple" describes only where the interpreter does real *arithmetic*; the
  interpreter-call tax on the early return is paid on every batch, aligned included, small
  at these widths but not zero.
  **Task 51 moved both numbers again, for a reason unrelated to sharing.** Removing the
  per-extraction range guard (2.21) shrinks every calendar node's emitted bytecode, shared
  or not - the guard lived in `emitEra`, which both paths call - so both crossings moved
  out again: unshared from 17 outputs to 19, shared from 40 to 44. The full re-measured
  ladder is in `PLAN_TASK_32.md` section 7.1's update, kept alongside the original rather
  than overwriting it. Task 44's own baseline (ten date columns, from step B1) is now a
  baseline for a number that has since moved twice for two independent reasons - sharing,
  then guard removal - which is worth knowing before task 44 is actually picked up: measure
  fresh against the emitter as it stands then, not against either ladder here.

* **A calendar field is computed once per output, not once per date.** **Half closed by
  task 32 step B1, the rest gated on a measurement (see 2.9 and `PLAN_TASK_32.md`)**, after
  a first pass that swept this entry the other way and had to be redone. Step B1 built the
  emitter-side fragment and made it the default, so a projection's *epilogue* now computes
  the decomposition once per date rather than once per field; the loop methods still compute
  it once per field, because relaxing the grouping policy that keeps them apart is step B2
  and B2 is gated on measuring the two-field case first. A calendar field is ~50 vector ops, ~45 of which are the
  shared civil-from-days prefix and ~5 the field's own tail; four fields therefore cost
  ~200 ops as four independent nodes against ~65 shared, a saving of ~135 ops. The
  hand-written ceiling kernel that prices that saving reaches 679.0 M rows/s against the
  four emitted nodes' 445.7 in the committed parity file - **1.5x** over four runs - and 165.6 to
  167.0 against 154.1 to 157.6 at 128-bit, a wash (`ChronoVectorOps`,
  `ChronoVectorOpsTest`, sql/varka/engine). The first pass measured 225.8 for the same
  kernel and declined the task; that kernel had a 376-byte helper past C2's 325-byte
  inlining budget in its lane path, so it priced a heap allocation per lane group rather
  than the sharing. Task 17's finding (raising `GROUP_BUDGET` so two outputs could share
  cross-output CSE in one method *lost*, 4119.9 against 2928.2 M rows/s in the current
  committed results file) still holds and is visible here as the reason the win is 1.5x
  rather than the ~3x the op count alone would predict, and as the reason it disappears at
  128-bit - but it does not reverse the sign. Closing the debt needs neither a multi-value
  IR node nor any IR change: the values worth sharing are locals inside one node's emitted
  bytecode, and the emitter can share them keyed on (fragment, child node). That is step B
  of `PLAN_TASK_32.md`. A multi-value node stays parked here for a future primitive whose
  shared value must be visible to the *planner* rather than only to the emitter -
  `divmod` and a string operation returning an offset and a length were the general
  examples; neither has been measured.

* **A badly-allocated kernel could be detected and re-emitted, and deliberately is not.**
  Every Varka kernel is emitted into a fresh class, so re-emitting the same shape under a new
  class name gives C2's register allocator a fresh roll - and task 50 makes the bad roll
  detectable. A detect-and-resample loop is therefore *buildable* with no new machinery. It is
  not scheduled, in this milestone or the next, and the reasoning is recorded here so it is not
  re-proposed as an obvious win. Each resample costs another class, another compilation and
  another warm-up, against a kernel a short query may run only a handful of times, so the
  expected value is negative wherever it matters most; class churn is already a watched concern
  (`VarkaClassLoaderTest` stresses a thousand loaders against metaspace); nothing bounds the
  retry, so a cap turns it into a slot machine and no cap turns it into a loop; and it treats a
  symptom whose structural cause - four outputs in one loop method at a width with sixteen
  vector registers - task 32 can remove outright at zero runtime cost. Revisit only for a
  kernel long-lived enough that one extra compilation amortises, and only with task 50's
  numbers in hand to say how often the bad roll actually happens.

* **`DateVectorOpsBenchmark` measures a degraded JIT state.** The engine's JMH
  runs with `forks = 0`, in the surefire JVM, *after* the JUnit suites have
  exercised the same kernels - so every committed figure in
  `DateVectorOpsBenchmark-jdk25-results.txt` is measured against profiles those
  suites polluted. Task 24 found it the hard way: three rounds of A/B in that
  harness said a kernel change cost 4-50%, and the clean catalyst harness then
  put the same change inside its own noise. The tell was
  `-XX:CompileCommand=inline,jdk/incubator/vector/*.*`, which moved the JMH
  numbers by 50-190% and the catalyst numbers by under 1% - a flag worth that
  much in only one harness is measuring the harness. A second symptom, visible
  in the committed file's own error columns: `scalarSubDays.MIXED_NULL`, which
  no recent task has touched, swings 3x between runs. Closing it means giving
  the JMH phase its own JVM (`forks = 1`) or separating it from the test phase,
  and then regenerating the whole results file, because every number in it
  moves. It matters now because task 25 is about to ask this harness whether an
  unroll factor pays, and on today's evidence it cannot answer.

## 10. Scope catalogue

The pre-plan catalogue, item numbers preserved. Items the plan above adopts
are condensed to a pointer; items it defers keep their full design input,
which is what makes them worth carrying forward.

### Item 1. Lane-width conversion, and mixed-type expression trees

Adopted as task 28 (see 2.6). The design input carried over whole: the hard
part is the lane count, not the conversion; `convertShape(I2L, longSpecies,
part)` yields one long vector per part with `partLimit` parts; the
narrowest-drive-versus-part-loop choice is measured before either is built in;
Spark's narrowing `Cast` throws under ANSI and wraps without it, tying this to
item 4.

### Item 2. int64 lanes: `TimestampNTZ`, `bigint`, and the second lane width

Adopted as task 29 (see 2.7). Kept for the zoned day when it comes: pack the
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

Adopted as task 30 (see 2.8). The pricing argument carried over whole:
wrap-versus-saturate difference lanes are exactly the overflowed lanes, one
vector op and one well-predicted branch on the common path, `try_*` as the
branchless easy case worth shipping alone.

### Item 5. Boolean outputs

Adopted as task 27 (see 2.5).

### Item 6. Calendar field extraction, `year` first

Adopted as task 26 (see 2.4). The corpus calibration kept on the record:
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

Adopted as task 25 (see 2.3). The full three-constraint pricing - the 7x
pinned-broadcast collapse, the ~1 ms-per-vector-op compile cliff against
`GROUP_BUDGET`, and `DIV` scalarization that unrolling cannot rescue - lives
in `SKILLS.md`'s "Vector API on HotSpot, Measured", whose unrolling bullet
task 25 rewrites with the numbers. The morsel-locality half was satisfied by
construction (a 4096-row int32 batch is 16 KB, L1-resident); the wide-shape
knee is open question 6 and rides task 25's harness.
