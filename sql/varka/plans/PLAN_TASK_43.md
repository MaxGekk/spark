# Task 43: what bounds a loop method inside one output

## 1. Where this came from

`PLAN_MILESTONE_4.md` 2.16 / task row 43. `groupOutputs` partitions *between*
outputs and never inside one, so `GROUP_BUDGET` binds only when a projection's
ops are spread across several roots. `CHRONO_WEIGHT` separates calendar nodes
that are separate output roots and does nothing for calendar nodes under one
root. The budget's own javadoc calls single-output loops healthy "at every width
tried", and the width tried was **59 ops**.

This task finds out where that stops being true. Per 2.16 it is a design task
rather than a recipe: the measurement decides between three mechanisms, and the
choice is the task. This file covers the measurement, which is all that can be
built while the emitter is held by other work (section 5).

### 1.1 Two corrections to 2.16, both measured

**The example no longer demonstrates the problem.** 2.16 says
`least(greatest(year, month), greatest(dayofmonth, quarter))` is one root
emitting one method of 1672 bytes, "roughly 190 vector ops". Measured on
today's emitter it is **61** `IntVector` ops and 7533 class bytes. The
difference is task 32 step B1: fragment sharing collapses the four calendar
prefixes into one, so the shape that used to be four decompositions is now one
decomposition and four tails. The number was true when written and is not now.
`year` alone measures 39 ops, which agrees with `PLAN_TASK_48.md`'s own figure -
so the emitter has not drifted; the shape has.

**`HugeMethodLimit` is not the constraint here.** Task 44's concern is that the
epilogue crosses HotSpot's 8000-byte refusal threshold at seventeen calendar
outputs. A single-output *loop* method does not come close: measured across the
ladder below, `loopDense0` runs from 287 bytes at 20 ops to **1989 bytes at 248
ops**, about 7.4 bytes per op, so it would take roughly 1050 ops to reach 8000.
Whatever bounds a loop method inside one output, it is not a hard refusal to
compile - which means the answer has to come from timing and from register
residency, not from a size threshold.

## 2. The instrument

The ladder has to vary op count and nothing else. Three things would confound it
if the shape were built the obvious ways:

* **A chain of `AddDays`/`SubDays`** varies dependency depth along with op count,
  which is task 25's axis, not this one.
* **Repeated calendar nodes** get their prefixes shared (step B1 above), so op
  count stops tracking node count - the correction in 1.1 is exactly this trap.
* **Repeated identical subtrees** are CSE'd by `emitValue`, so they cost once.

The shape that avoids all three is a `greatest`/`least` tree over independent
`dayofweek(d + k)` subtrees, each with a distinct literal slot: no two subtrees
are equal, so nothing is CSE'd; `DayOfWeek` shares no fragment; and the subtrees
are independent of each other, so the body is wide rather than deep. Measured,
it is exactly linear at **19 ops per step**:

| subtrees | `loopDense0` ops | `loopDense0` bytes | `epilogueMasked` bytes |
|---|---|---|---|
| 1 | 20 | 287 | 385 |
| 2 | 39 | 431 | 572 |
| 3 | 58 | 572 | 756 |
| 4 | 77 | 713 | 940 |
| 5 | 96 | 854 | 1124 |
| 6 | 115 | 995 | 1308 |
| 7 | 134 | 1137 | 1493 |
| 9 | 172 | 1421 | 1863 |
| 11 | 210 | 1705 | 2233 |
| 13 | 248 | 1989 | 2603 |

That covers 2.16's requested ladder - 60, 100, 150, 190, 250 ops - at 3, 5, 8,
10 and 13 subtrees, and the op counts are asserted off the class file rather
than assumed, so a lowering change that moves them fails the benchmark's own
check rather than silently re-labelling the x-axis.

## 3. What is measured

1. **Steady-state throughput per row** at each ladder point, at both vector
   widths. Rows per second is the wrong unit here because every point does a
   different amount of work; the number that matters is **nanoseconds per row
   per op**, which is flat if the loop scales and rises where it does not.
2. **Time to reach steady state.** This is the axis task 11 measured when it set
   `GROUP_BUDGET`, and the one the budget exists to protect: a body that
   eventually runs fast but takes seconds of C2 time is a bad trade for a short
   query. Read from `-XX:+PrintCompilation` as the wall time from JVM start to
   the tier-4 compilation of `loopDense0`, the same way `PLAN_TASK_32.md` 7.5
   read it.
3. **Whether C1 refuses.** `SKILLS.md` records C1 declining a wide body at
   128-bit with `COMPILE SKIPPED: out of virtual registers in linear scan`. If
   that appears anywhere on the ladder it is a stronger signal than any timing,
   because it is the register file saying no rather than a benchmark saying
   slower.

## 4. Predictions, registered before the run

1. Nanoseconds per row per op is flat to within noise across the whole ladder at
   AVX-512. The 32 zmm registers hold this body comfortably at every width
   tried, and nothing in the lowering changes shape with size.
2. At 128-bit it is **not** flat, and degrades somewhere between 100 and 200
   ops, because 16 xmm registers is where `SKILLS.md`'s spill investigation put
   the edge for a body of about forty live values.
3. Tier-4 compile time grows roughly linearly and stays under one second at 248
   ops. `PLAN_TASK_32.md` 7.5 measured a 200-op method reaching tier 4 in 272 ms
   and the whole twenty-method kernel in 2.4 s, so task 26's "~10 second cliff"
   is expected to be a number about a machine and a JDK that no longer applies.
4. C1 refuses somewhere on the 128-bit ladder and nowhere on the AVX-512 one.

Prediction 3 is the one that matters for the milestone: if it holds, the
compile-cliff argument behind `GROUP_BUDGET = 16` rests on a stale measurement,
and that is worth knowing before the next task weighs a node against it.

## 5. What this task does not do

**It does not choose the mechanism.** 2.16 offers three - split inside an
output, decline the shape through `fitsBudgets`, or accept it and record where
the cliff sits. All three edit `VarkaLoopEmitter` or the compiler, which PR #64
and PR #78 hold. The measurement is what those PRs do not block, and it is what
the decision has to rest on anyway, so it goes first and the decision follows
once the emitter is free. 2.16 asks for exactly this order.

**It does not touch the epilogue.** That is task 44, and the epilogue column in
section 2's table is recorded only because it comes free from the same emission.

## 6. Files

| file | what |
|---|---|
| `VarkaEmitterParityBenchmark.scala` | the ladder section: the shape builder, the op-count assertions, the per-op timing at both widths |
| `VarkaEmitterParityBenchmark-jdk25-results.txt` | regenerated in one run |
| `sql/varka/plans/PLAN_TASK_43.md` | this file, with section 8 filled in |
| `PLAN_MILESTONE_4.md` | 2.16's two corrections from 1.1, and row 43 |
| `SKILLS.md` | whatever the ladder says about where a single-output loop stops scaling |

Nothing under `src/main`. This task measures the emitter; it does not change it.

## 7. Risks

1. **Measuring the wrong axis.** Section 2's three confounders are the whole
   design of the instrument, and 1.1 is evidence the trap is real rather than
   theoretical - the milestone's own example fell into it.
2. **A ladder whose x-axis drifts.** The op counts are asserted off the class
   file, so a lowering change breaks the assertion instead of quietly moving the
   axis.
3. **Reading compile time from a benchmark JVM.** `PrintCompilation` output has
   to come from a run whose only load is the shape under test, or the timings
   measure queueing behind other compilations. Each ladder point gets its own
   JVM for that reading.
4. **Concluding from one host.** Everything here is one Zen 5 machine on one
   JDK. The conclusion that can be drawn is about this machine; the conclusion
   that cannot is "no cliff exists". Section 8 states which is which.

## 8. Outcome

Filled in when the measurement lands: the per-op timing at both widths, the
tier-4 compile times, whether C1 refused anywhere, and which of section 4's
predictions held.
