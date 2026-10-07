# VARKA-292: Narrowed outputs mapped at their own width

*Scoped 7 October 2026 (milestone 7 section 2.2, row 292, found by row 263); opened 7 October 2026.*

## 1. The question

`VARKA-263.md` 4: under the memory sanitizer, six TIME tests fail with `a mapping of 80000 bytes ...
the nearest is output data 0 ... for 40000 bytes` and `a mapping of 40 bytes ... for 20 bytes`.
The emitter sizes every data segment as `dataBytes = length * lane.byteStride` (`VarkaBodyEmitter
.emitSizes`), and the output of a `NarrowLane` root - `hour`, `minute` and `second` of a TIME, whose
long-lane kernel stores an int32 at `i * 4` - is mapped at that size, twice its own. The stores are
int32 wide and the answers are right; the bounds check on the upper half of that output's segment
enforces nothing, so a bad store there would raise nothing. Nothing else the sanitizer ran left its
buffer.

## 2. The change

One method in `VarkaBodyEmitter`: `emitOutputSegments` maps an output whose root is a
`NarrowLane` through `loadNarrowedSegment`, at `(long) length * 4` computed from the length, in
place of `loadSegment` at `dataBytes`. Every other output, and every source, keeps `dataBytes`: a
kernel has one lane, and only a narrowed root stores at another width.

Alternatives considered. *A second size slot, `narrowedBytes`, computed once beside `dataBytes`:*
rejected, since a local slot shifts every later slot's number in every kernel with a narrowed root,
which would make the bytes oracle's diff harder to read for nothing. *`dataBytes >> 1`:* shorter,
but it assumes the lane's stride is eight, which `NarrowLane`'s own contract does not state; the
length is the one thing that does not change. The size is computed in the prologue, outside the
loops, so it adds nothing to a loop body.

## 3. Predictions, registered before the run

Registered in row 292 of `m7/PLAN.md` before the change was written, as its proof:

1. `emitted_bytes.json` moves only for shapes with a narrowed root, and the diff is read.
2. The six TIME tests pass under the sanitizer.
3. The kernels' time is unchanged on a benchmark pair.

Made precise before the benchmark was run (the oracle and the tests had been): on
`VarkaTimeBenchmark`, the arms with a narrowed store (the conversion form narrowed, and narrowed
on the half species) are within 3% of master's, taken as the means of three alternating runs of
each, and the arms with no narrowed store, which the change cannot reach, are within 3% too, which
is the control.

## 4. Verification

*Written as the work happened, 7 October 2026.*

**The bytes oracle.** `VarkaEmittedBytesSuite` failed on the change, in every block of the long-lane
fuzz set, as it should. Regenerated (`VARKA_BYTES_REGEN=true`) and compared with the old file
structurally: 36 values moved, and only these - the coverage rows `hour(t)`, `minute(t2)` and
`second(t)` at both widths (24), the long-lane fuzz set's block digests at both widths (2), and the
option arms' per-width digests (10). No value of the int-lane coverage rows or of the int-lane fuzz
set moved, since a narrowed root exists only at the long lane. The change is five bytes in each
prologue that maps a narrowed output: `iload`, `i2l`, `ldc2_w` and `lmul` in place of one `lload`.

**The price tables, which the bytes moved.** `VarkaEmitCostSuite` regenerated its table
(`VARKA_COST_REGEN=true`): one row moved, `NarrowLane`, by exactly 5 in each of its four columns
(the table's diff has the values), and the hand-set register beside it by 5 in each of its four,
which is the five bytes.
`VarkaEmitCostAuditSuite` regenerated its audit against the compiled table: seven methods
moved between its counts and its percentiles moved by at most 0.1 points. Rerunning
`VarkaEmittedBytesSuite` against the new table matched without a second regeneration, so no
grouping decision flipped.

**The six TIME tests under the sanitizer.** `VarkaTimeArithmeticSuite`,
`VarkaCoverageDifferentialSuite` and `VarkaMemorySanitizerEndToEndSuite` with
`-Dvarka.sanitizeMemory=true`: 111 tests, none failed; before the change six of them failed.

**Every Varka suite with the sanitizer on** (`dev/varka_matrix.sh --defaults --split 3 -j 6
--jvm-arg -Dvarka.sanitizeMemory=true`, as in `VARKA-263.md` 4): with the change but before the two
cost oracles were regenerated, 964 ok and two failed, both of them those oracles, with no violation
and no aborted suite. After regenerating them: 966 ok, none failed, 27 canceled (the suites' own, as in
`VARKA-263.md` 4), none aborted, in 329 seconds, and no sanitizer violation: every Varka suite
passes with the sanitizer on, so row 263's last step is unblocked.

**The timing pair.** `VarkaTimeBenchmark` (the TIME extracts, whose arms with a narrowed store are
this change's kernels) run three times each on master and on the change, alternating, by
`dev/varka_bench_regen.sh --no-narrow`, which pins to the fastest cores and starts only on a quiet
machine (load 0.62 to 0.78 at each start). The tree of the baseline worktree is identical to
master's. The 32 arms with a narrowed store: the change's rate over master's is 0.985 to 1.011,
0.999 on average, and none is outside 3%. The 108 arms with none, the controls the change cannot
reach: 24 moved by more than 3%, 23 of them by no more than their own run-to-run spread, and one
by more (1.085, a hand-written int32 arm at 262,144 rows, whose twin at 16,384 rows moved the
other way, 0.911): noise.

**Two false starts, kept.** The first pair ran with the machine at a load of 1.8, so the script
refused and exited at once, and the six "results" were the committed file copied six times: no
guard noticed, and the log did. The pair now waits for a quiet machine before each run and stops
on a run that produced no new file. The second attempt ran and crashed at once: `VarkaTimeBenchmark`
had not run since `ConstDivide` began requiring a dividend bound at the long lane (the benchmark
built its divisions without one), so it threw before measuring anything. Its four long-lane
divisions now state the bounds `VarkaTimeCompiler` states, a day of nanoseconds, then an hour, then
a minute, in the same edit in both worktrees.

## 5. Outcome

1. *`emitted_bytes.json` moves only for shapes with a narrowed root, and the diff is read.* Held,
   as above: 36 values, all of them long-lane.
2. *The six TIME tests pass under the sanitizer.* Held: 111 tests, none failed.
3. *The kernels' time is unchanged on a benchmark pair.* Held for the kernels the change reaches:
   32 arms, 0.985 to 1.011. The controls were predicted within 3% as well, and 24 of 108 were not;
   23 of those are inside their own spread and the other is an int-lane arm the change cannot
   reach, so the prediction as worded failed on the benchmark's noise and not on the change.

What it found that it did not look for: `VarkaTimeBenchmark` had not run since `ConstDivide`'s
bound. It is repaired here, since the timing pair needs it; its three committed result files
predate the bound, and are stale (below). Nothing else became a row.

*What pins the fix.* The bytes oracle, in every run, since it fails on any change to the mapping's
bytecode; and the six TIME tests, which pin the mapping's size once the sanitizer is on in the
suites, which is row 263's last step and can now follow.

## 6. Explicitly out of this task

* Running the suites with the sanitizer on by default, which is row 263's last step and follows
  this task.
* Sources mapped at the lane's stride when a column is narrower than the lane: none exists today
  (the sanitizer found none), and one would be a different finding.
* Regenerating `VarkaTimeBenchmark`'s three committed result files (`-jdk25-results.txt`, the
  128-bit and 256-bit companions) and its provenance: they predate `ConstDivide`'s bound and are
  stale against the lowering that now ships. The two-width regeneration is about half an hour of
  a quiet machine; it is a debt of the benchmark, not of this task.
