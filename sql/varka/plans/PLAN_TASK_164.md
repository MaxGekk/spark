# Task 164: a `TIME` chains benchmark, for the full-width number

*Scoped 21 September 2026 (milestone 5 section 2.100, row 164) from
`PLAN_TASK_118.md` section 3 item 5; planned 22 September 2026.*

## 1. The question

The closing task wants a number measured on a machine with a full-width
512-bit datapath, and the `TIME` surface cannot supply one. The reason is the
one `Chains` already wrote down for dates: a surface entry times a single
expression, and its lightest entries are bound by memory bandwidth rather than
by arithmetic, so a wider datapath cannot move them however genuine it is.
`DateSurfaceBenchmark` met this first and `DateChainBenchmark` is the answer
that was built for it.

The long lane makes the same problem worse in one direction and better in
another, and neither is guesswork. Worse: a `TIME` row is eight bytes rather
than four, so a given entry moves twice the bytes per row and reaches the
memory floor at half the arithmetic. Better: the `TIME` lowerings are
divisions, and a division is the most expensive thing Varka emits - the
conversion form is seven lane operations and the magic form fourteen, against
one for an add - so a `TIME` expression carries far more arithmetic per link
than a date one does.

Which of those wins is not known, and it decides the whole shape of the entry
list. That is what this task settles before it quotes anything.

There is a second reason the chains are the right vehicle rather than a wider
surface. The surface's ratios are Varka against stock Spark on one expression,
and the committed files put the fifteen projection entries between 13.4x and 38.2x
(`TimeSurface-varka-jdk25-results.txt` against
`TimeSurface-spark-4.2.0-jdk25-results.txt`). Those are the floor of what the
engine is worth: stock Spark pays its per-row costs once for a single
operation and at every link of a chain, while the kernel fuses the chain into
one loop. A chain file is where that shows.

## 2. The change

`TimeChainBenchmark` beside `DateChainBenchmark`, over the `varka_times`
table, with `TimeChains` as its entry list - the exact mirror of
`Chains`/`DateChainBenchmark`, which is four lines of benchmark class and one
list. The harness, the guards, the job-size rule, the provenance block and the
committed-file discipline are `DateSurfaceBenchmark.run`'s and are reused
unchanged; `dev/varka_bench_surface.sh` gains a `timechains` arm beside its
`surface`, `chains` and `time` ones.

Three things are genuinely new, and they are the task.

### 2.1 The op floor has to be re-derived, not copied

`Chains.MIN_OPS` is 280, and its comment derives that from a model of the date
lane: roughly 0.02 ns per emitter op over a memory floor near 0.8 ns, against
a 3.6 ns/row threshold, which puts break-even near 140 and the floor at twice
it on purpose. Every input to that model changes at the long lane. The memory
floor doubles with the row width. The per-op cost is not the date lane's,
because a long-lane op at 512 bits processes eight lanes where an int op
processes sixteen. And the threshold is the same fixed-share rule but over a
table twice the size, so a given row count buys a different amount of
executor time.

**So `TimeChains.MIN_OPS` is derived from a measurement, not inherited.** The
derivation is one cheap run: the existing `TimeSurface` files already give the
per-row cost of entries whose op counts `dev/varka_emit.sh --table` reports,
and two points on that line fix the slope and the floor. The comment records
the arithmetic, as `Chains`'s does, and the number is whatever it says.

### 2.2 The entries mix the long lane's three types

`Chains` mixes DATE, INT and the year-month interval in every entry, because
Varka covers three int32 types and a benchmark of one understates that. The
`varka_times` table offers the long lane's three - `t`, `t2` as `TIME`, `dt`,
`dt2` as day-time intervals, `l`, `l2` as `bigint` - and the same rule
applies: every entry carries more than one of them where the type system
allows it, and at least one produces an interval or a `TIME` rather than only
consuming one.

What limits composition here is that the `TIME` extracts narrow to an int at
the store and the emitter narrows only at an output root, so `hour(t) + 1`
does not fuse (`PLAN_TASK_102.md` 8.3, the restriction task 28 lifts). A chain
therefore composes in the long lane and narrows once at the end, or does not
narrow at all: `time_trunc` over `time_add`-shaped arithmetic, `time_diff`
over two composed times, `greatest`/`least` and the conditionals over
composed values. The list is built from what fuses, and every entry is checked
with `--expect-fused` before it is timed, which is the harness's existing
guard.

### 2.3 The job size is a runner constraint, not a laptop one

The date surface needs 5e8 rows and no runner in the pool can hold that table;
the date chains clear the fixed-share rule at 1e8 because they are
compute-bound. A `TIME` row is eight bytes to a date's four, and the row's own
acceptance line names 2e8 rows in 12g on a runner. Whether that holds is the
first thing a dispatch finds out, and it is cheaper to find out with one
`build-only` warm-up and one ungated row-ladder dispatch than with a gated one
that waits for the Zen 5 - the lesson `PLAN_TASK_62.md` 11.8 records about
pricing a run on the machine that is scarce.

## 3. Predictions, registered before the run

1. **`TimeChains.MIN_OPS` lands above `Chains`'s 280.** A long-lane op covers
   half the lanes of an int one at the same width and the row is twice as
   wide, so more ops are needed to leave the memory floor. If it lands below,
   the division's cost per op is doing more work than the model expects and
   the comment says so.
2. **The chain ratio against stock exceeds the surface's.** The surface's
   `TIME` projection rows read 13.4x to 38.2x against stock 4.2.0 in the
   committed files, and the spread is the row engine's cost for the expression
   rather than the kernel's saving (task 155's caution). The chains, which
   amortise stock's per-row costs over more links, read above the top of that
   range on the entries that compose deepest.
3. **Every entry fuses with no fallback batch**, which the harness checks
   rather than the plan asserting it.
4. **The entries clear `--max-fixed-share` at 2e8 rows in 12g on a runner.**
   This is the one the dispatch can refute cheaply, and the row's acceptance
   line names it.
5. **The band is tier 0 or 1 for every projection entry.** The date chains are
   compute-bound and quiet; a `TIME` chain that lands in tier 2 is a signal
   that it is still memory-bound, which would send its op count back to 2.1.

## 4. Verification

- The band measured before any figure is quoted, with
  `dev/varka_bench_band.py --write`, on an idle pinned machine - the standing
  rule, and the one task 90 exists to enforce.
- The laptop's four arms first, as the `TIME` surface has them: stock 4.2.0 on
  JDK 17 and 25, the fork with the engine off, the fork with it on.
- Then the runner, through `.github/workflows/varka-surface-benchmark.yml`,
  with `build-only` to warm the SHA cache, an ungated row ladder to price the
  job, and only then the gated dispatch.
- A unit test that every entry's op count clears `MIN_OPS`, mirroring the one
  `Chains` has, so an entry that stops earning its place fails a suite rather
  than a runner dispatch.
- `dev/varka_quote_check.py`, since every figure quoted anywhere must trace to
  one of the committed files this task writes.

## 5. Outcome

<!-- filled when the work is done -->

## 6. Explicitly out of this task

- **Lifting the narrowing restriction** so `hour(t) + 1` fuses. That is task
  28 (`SCOPE_MILESTONE_6.md` item 39), and this list is built from what fuses
  today rather than waiting for it.
- **A `TIME` equivalent of the surface's filter entries.** The chains answer
  the datapath question, which is a projection question; the filter shapes are
  the surface's and stay there.
- **The full-width number itself.** This task delivers the benchmark, its band
  and the laptop arms; quoting a 512-bit figure is the closing task's (118),
  which is also where the runner-pool hit rate is already priced.
- **Re-deriving `Chains.MIN_OPS`.** The date model stands for the date lane;
  this task derives a second number for a second lane and leaves the first
  alone.
