# VARKA-378: A nightly guard over the deoptimization cycle of the NULL-mode kernels

## 1. Where this came from

Row 378 of `m7/PLAN.md`, candidate 1 of `VARKA-326.md` 9.4. VARKA-326's census found that every
kernel the fuzz grammar draws that enters the C2 `profile_predicate` deoptimization cycle is a
NULL-mode kernel - a null-propagating `int` add or subtract (`int:ADD:NULL`, `int:SUB:NULL`) or a
`make_date` that does not fail on error, so the emitter builds no dense side and every batch runs
the masked loop: 104 of 104 cycling kernels over three arms, about a quarter of the NULL-mode
kernels (47 of 204 at the preferred width, 18 of 204 at 128 bits; `VARKA-326.md` 9.1). And no
guard in the nightly covers them (`VARKA-326.md` 9.3): the nightly's `deopt` step forks one dense
`make_date` kernel per fresh JVM and reads `loopDense` only, and the compiled fuzz test prints an
unsettled kernel without failing. The row asks for `dev/varka_deopt_census.sh` over a fixed draw
range, one JVM per lane width (512 and 128 bits), run nightly, recording the kernels past the 10 s
cut and their `profile_predicate` traps; since the class cycles today, the guard ratchets: it
fails when the count rises above a committed one, and the committed count falls with any fix.
Done when the step runs in `dev/varka_nightly.sh` with its count committed, a planted cycling
kernel above the count fails it, and the step's time is stated against the nightly budget.

## 2. The admission check, done

What had to be true for the guard to be worth building: that the census script runs one JVM on a
few cores in minutes, and that a fixed range at the compiled test's own 10 s deadline still holds
cycling kernels to count. Checked on 16 October 2026 on the laptop (AMD Ryzen AI 9 HX PRO 370,
JDK 25.0.4.1, preferred species 512 bits) at `7f706e67035`, whose compiled code is #851's
(`2188387ac5e`; this branch adds documents only), the catalyst test build copied from #851's
worktree:

* **A trial, draws 0 to 24 a lane at the preferred width, one JVM on cores 3 to 5, settle
  deadline 10 s**: 50 kernels in 70 s with the build warm (1.4 s a kernel), at a load average of
  20.6 at start and 18.5 at end - other agents' JVMs on the other 21 cores. 11 NULL-mode kernels,
  one cycling by VARKA-189's rule (long-lane draw 19, `int:ADD:NULL`, 51 traps on its masked loop)
  and the same one past the cut, not settled at the deadline.
* **The deadline truncates the settle time at about 10 s** (10,021 ms for that kernel), so a cut
  read as `settle_ms > 10000` holds only by the margin the warm-up loop overshoots by. The guard
  counts a kernel not settled at the deadline, or settled after the cut, with more than four
  `profile_predicate` traps.
* **The committed census holds the range.** Arm A's and B's first JVMs ran draws 0 to 99 a lane
  in one JVM each, exactly the guard's range and order, at a 30 s deadline on six cores:
  `VARKA-326-census.tsv` gives, at the preferred width, 13 cycling (int 1, long 12) and 2 past the
  cut (both long); at 128 bits, 5 cycling (all int) and 3 past the cut (all int).
* **The sanitizer is off**: `target/varka-matrix/jvm.opts`, which the census script reads, sets
  no `varka.sanitizeMemory`, as a JIT-measuring run needs (`sql/varka/AGENTS.md`).

It would have stopped the row at a documented reason had a JVM of 200 kernels taken more than
the deopt step's own 243 s several times over, or had the range held no cycling kernel to count.

## 3. The design

### 3.1 A guard mode over the census script

* **`dev/varka_deopt_census.sh --guard`** runs the range the count file names, one JVM per width
  one after the other (preferred width, then `-XX:MaxVectorSize=16`), each pinned with `taskset`
  to the count file's cores, at the fixed seed 20260903 (VARKA-274's and the census's, never the
  nightly's day seed: a count is comparable only over the same draws), with the matrix base
  options and the compiled test's 10 s deadline. It then hands the run directories to the reader.
* **`dev/varka_deopt_census.py --guard FILE`** reads each width's run directory, prints per width
  the count, the kernels it counts (lane, draw, NULL-mode nodes, `profile_predicate` traps, the
  most on one method, settle ms), the cycling count by VARKA-189's rule beside it, and a
  `verdict:` line, and exits 1 when a width's count is above its ratchet, when a width has no run
  directory or no kernel, or when a JVM failed. Below the ratchet it says so and asks for the
  ratchet to be lowered, without failing.
* **The count file, `dev/varka_deopt_census_guard.tsv`**: one row per width with the range
  (`from`, `draws`), the deadline, the cores, and the ratchet, so the configuration and the count
  are one committed thing and a change to either is one diff.
* **The count**: kernels past the 10 s cut with more than four `profile_predicate` traps, the
  row's own measure. Its spread over three runs decides the ratchet; if the cut count turns out
  unstable where the trap-based cycling count is not (`VARKA-326.md` 7.2: the load moves seconds
  more than traps), the ratchet moves to the cycling count with the cut kernels still printed, and
  section 9 says so.
* **The nightly step `deoptnull`**, after `deopt`, with `--skip-deoptnull`.

### 3.2 What is deliberately unchanged

The emitter and its masked loop (scope item 2 of `VARKA-326.md` 9.4), the census's arms and their
defaults (the guard is a mode beside them), `VarkaIrFuzzSuite` and its switches (#851's), the
nightly's `deopt` step (VARKA-189's dense guard, kept: it covers a shape this one does not run).

### 3.3 Registered op counts

None: no emitted code changes.

## 4. Files

| file | what |
| --- | --- |
| `dev/varka_deopt_census.sh` | `--guard [FILE]`: the count file's widths, one JVM after the other, then the reader's verdict |
| `dev/varka_deopt_census.py` | `--guard FILE`: the count per width against its ratchet, the counted kernels, the verdict; `--self-test` |
| `dev/varka_deopt_census_guard.tsv` | the guard's range, deadline, cores and ratchet per width |
| `dev/varka_nightly.sh` | the `deoptnull` step and `--skip-deoptnull` |
| `sql/varka/plans/m7/VARKA-378.md` | this plan |
| `sql/varka/plans/m7/PLAN.md` | row 378 |

## 5. Tests, and what each is for

* **A planted cycling kernel, in the reader** (`dev/varka_deopt_census.py --self-test`): a run
  directory of one recorded kernel past the cut, trimmed from a guard run's log to its
  `VARKA_CENSUS` line and its class's compile and trap lines, is read beside a run directory of a
  recorded kernel that settled; with the ratchet at 0 the guard fails with a count of 1, at 1 it
  passes, and with the settled kernel alone at a ratchet of 1 it passes and asks for the ratchet to
  be lowered; an empty run directory fails. It catches a reader whose patterns no longer match the
  log (the census's own first failure mode, `VARKA-326.md` 7.3) and a comparison the wrong way.
* **Planted cycling, live**: the guard with `-XX:PerMethodTrapLimit=100000
  -XX:PerBytecodeTrapLimit=100000` (the census's arm L), under which a kernel that enters the
  cycle never leaves it, must exit 1. Run once and recorded in 9, not in the nightly.

No Scala or Java test moves; no pinned fixture changes.

## 6. The measurement

The guard's configuration run three times, one width at a time on cores 3 to 5, the load
recorded with each run, to give the count's spread; then the planted runs. Not a benchmark: the
counts are kernels and traps, and the step's time is a wall-clock reading.

### 6.1 Predictions, registered before the run

1. **The count at draws 0 to 99 a lane**: at the preferred width 2 past the cut (13 cycling), at
   128 bits 3 (5 cycling), the committed census's first JVMs (2.). The 10 s deadline cuts the
   cycle where the census's 30 s did not, so the cycling count may read lower; the cut count should
   not move by more than two either way.
2. **The step's time**: 200 kernels a width at the trial's 1.4 s, plus up to 10 s for each kernel
   past the cut, is about 5 minutes a width; at the calibration table's ratio for harness-bound
   checks (about 1.2) about 6 minutes a width, 12 minutes for the step.
3. **The spread over three runs**: the cut count within one of its median per width; the cycling
   count within two (`VARKA-326.md` 9.2: 46 to 49 over 800 kernels, the same 18 at 128 bits).

## 7. Risks

1. **The load moves the cut.** Measured here at a load of about 20 on three pinned cores, while
   the nightly runs on an otherwise idle machine; an idle run may count fewer. Read by the three
   repeats; the first idle nightly confirms or lowers the ratchet, which only ever falls.
2. **The cycle depends on history** (`VARKA-326.md` 9.1), so a change anywhere in the range's
   earlier draws - a grammar change, an option's default - moves the count without touching the
   cycle. The count file names its range so such a change shows in the same diff that rebases it.
3. **A reader that matches nothing would pass**: guarded by failing on a width with no kernel and
   by the self-test over recorded lines.

## 8. Sequencing

One commit: the guard mode, the count file, the nightly step, this plan and row 378.

## 9. Outcome

Measured 16 October 2026, 11:05 to 11:55 CEST, on the laptop at `7f706e67035` with this
branch's guard (the compiled code is #851's), each run the guard as the nightly runs it
(`dev/varka_deopt_census.sh --guard --skip-build`, the build copied from #851's worktree), one
JVM at a time on cores 3 to 5. Those are one full-speed core (3, 5.2 GHz) and two compact ones
(4 and 5, 3.3 GHz, another L3), whose SMT siblings 15 to 17 belonged to other agents. The
machine was not idle: other agents' JVMs held the other cores, at a load average of 21, 46, 35
and 15 at the four runs' starts and up to 46 during them.

### 9.1 The count and its spread

| run | load at start | pref: past the cut | pref: cycling | pref: s | 16: past the cut | 16: cycling | 16: s | step s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 21.0 | 8 | 12 | 343 | 2 | 5 | 525 | 868 |
| 2 | 46.3 | 8 | 11 | 455 | 3 | 5 | 470 | 926 |
| 3 | 35.2 | 2 | 3 | 260 | 3 | 4 | 381 | 642 |

* **At 128 bits the count is stable**: 2, 3, 3 past the cut, always int-lane draws 8 and 92
  (`makeDate:NULL`) and, twice, 25 (`int:ADD:NULL,int:SUB:NULL`); cycling by VARKA-189's rule 5,
  5, 4. **At the preferred width it is not**: 8, 8, 2 past the cut, all long-lane `int:ADD:NULL` or
  `int:SUB:NULL` kernels from one set (26, 32, 42, 63, 76, 88, 92 recur; 63 is in all three), and
  the trap-based cycling count moves with it, 12, 11, 3. So 3.1's fallback, the cycling count,
  is no steadier here and was not taken: the ratchet is on the row's own measure, the kernels past
  the cut.
* **Why it moves: the load slows the cycle, not the set of kernels in it.** At the census's load of
  0.07 (arm A, six cores, the same range in one JVM) the same kernels reached the trap cap fast:
  long 32, 42 and 76 had 100 traps by 10 s and the others settled in 0.5 to 5 s with 7 to 24
  traps, so only 2 were past the cut; here they had 20 to 55 traps at 10 s and were still in the
  cycle. Run 3's preferred-width JVM, the fastest (260 s), caught 2, as the census did.
* **The ratchet, committed in `dev/varka_deopt_census_guard.tsv`: 8 at the preferred width, 3 at
  128 bits**, the most of the three runs. On a quiet machine the census's own counts at this range
  are 2 and 3, so the first idle nightly should ask for the preferred width's to be lowered (the
  guard says "below the ratchet - lower it"); the count file says to lower it when two nightly
  runs agree. A ratchet of 8 at the preferred width lets up to six new kernels into the cycle
  unseen on a quiet night until it is lowered; that is the price of setting it on a loaded
  machine, and lowering it is the first follow-up.

### 9.2 The planted kernel

* **The reader's self-test** (`dev/varka_deopt_census.py --self-test`, which the guard now runs
  before its JVMs): a recorded cycling kernel (long-lane draw 19 of the admission trial, its census
  line, six of its traps and its made-not-entrant lines) fails the guard at a ratchet of 0 and
  passes at 1; four traps are not counted; a settled kernel alone below the ratchet passes and asks
  for it to be lowered; a JVM whose exit file is not 0 fails, and so does a width with no kernel.
  All six cases pass; the comparison mutated to `>=` fails two of them. (The review of 9.6 added a seventh, a JVM that left no exit file.)
* **A planted kernel over a real night's logs**: run 3's two width directories, at the committed
  ratchets (128 bits at its ratchet, 3), with one more run directory under the 128-bit width
  holding one recorded kernel past the cut - run 1's long-lane draw 25 at the preferred width,
  trimmed to its census line and its class's 96 compile and trap lines. The guard counts 4 at 128
  bits and exits 1: `verdict: FAILED - width 16: 4 past the cut, above the ratchet 3`.
* **The live plant 5 proposed did not plant.** The guard with `-XX:PerMethodTrapLimit=100000
  -XX:PerBytecodeTrapLimit=100000` (the census's arm L) passed, counting 0 and 1: with the limits
  raised the masked loops trapped 320,187 times at `profile_predicate` at the preferred width and
  555,548 at 128 bits, up to 3,898 on one method, but every kernel settled, in about a second -
  the method is not made not entrant on each trap, so the warm-up sees it at tier 4 and the
  VARKA-189 rule sees no recompiles. A finding beside the row: a kernel that traps thousands of
  times while running C2 code is invisible both to the warm-up's verdict and to this count. Under
  the JVM's defaults the cap of 100 traps ends that form, so it is not today's cycle; it is listed
  here for whoever reads the masked loop (`VARKA-326.md` 9.4 item 2).

### 9.3 The step's time, against the nightly

The step is 642 to 926 s (11 to 15 minutes) on the loaded laptop, plus the guard's build: a
`dev/varka_matrix.sh --module catalyst --list`, which after the fuzz step's compile is sbt's
start and an export. **No budget exists for the nightly as a whole** (`m7/PLAN.md` 9.2: "no plan
carries a budget for the gate or the nightly as a whole"), so against what there is: the step's
own deadline of 10,800 s (`VARKA_STEP_DEADLINE`) has twelve times the room; the steps beside it
take 243 s (`deopt`, `VARKA-189.md` 10.1), about 13.5 minutes (`sweep`, `VARKA-240.md`), about
14 minutes (`sparkwarm`, row 327) and 21 to 33 minutes an arm (`golden`). So it is the size of the
sweep or the warmed differential. On a quiet night it should be shorter, since a kernel past the
cut costs the full 10 s and a quiet machine catches fewer: the admission trial's 1.4 s a kernel
gives about 5 minutes a width. `--skip-deoptnull` leaves it out.

### 9.4 Predictions scored

1. **The count**, predicted 2 at the preferred width and 3 at 128 bits, within two either way:
   **held at 128 bits** (2, 3, 3), **missed at the preferred width** in two runs of three (8, 8,
   2). The miss is the load (9.1): the prediction read a quiet machine's census for a loaded
   machine's run.
2. **The step's time**, predicted about 12 minutes (5 minutes a width at the trial's rate, at a
   ratio of 1.2): 10.7 to 15.4 minutes, a ratio of 0.9 to 1.3 on a machine at load 21 to 46. Held
   within the ratio. This line is owed to the calibration table in
   `skills/testing-and-debugging.md`, a file outside this row's ownership; it is listed for the
   pull request that merges this one.
3. **The spread**, predicted within one of the median past the cut and within two cycling:
   **held at 128 bits** (2 to 3; 4 to 5), **missed at the preferred width** (2 to 8; 3 to 12).

### 9.5 What it leaves

* Lower the preferred width's ratchet from the first two quiet nightly runs (9.1).
* The trap-without-recompile form of 9.2 for the masked-loop scope item.
* The calibration line of 9.4 item 2.

### 9.6 The review

`/code-review high` on the branch against master found ten points; fixed:

* **A failing JVM ended the guard.** Under `set -e` the subshell around `java` exited before it
  wrote its exit file, so the second width never ran and no verdict was printed. It now runs with
  `set +e`; checked with `--jvm-arg -Xmx1k` over a five-draw count file: both widths ran, both exit
  files read 1, and the verdict failed naming them. The self-test gained the missing-exit-file case.
* **The JVM inherited the loop's standard input**, the count file's rows; it now reads
  `/dev/null`.
* **The verdict's status** is taken from the reader directly rather than from a pipeline with `tee`.
* **One parser for the count file**: the shell reads its rows through
  `dev/varka_deopt_census.py --guard-rows`, which rejects a reordered header, a wrong column count
  and a cell that is not a number.
* **`--guard` refuses the census's own flags** (`--draws`, `--width`, `--settle-ms` and the rest)
  instead of ignoring them.
* **The nightly's quick-trial usage lines** skip the new step too.
* **The census report's 10 s cut** is now the guard's own `past_cut`.

Not changed: the ratchet of 8 at the preferred width (9.1 says why, and its lowering is the first
follow-up; counting traps per second of C2 code instead, as `the-jit.md` now suggests, would be a
row of its own); running the two widths side by side on six cores, which would halve the step but
not match the runs that set the ratchets, all made one JVM at a time on cores 3 to 5; and skipping
the guard's build, which keeps the nightly from reading a classpath older than the day's merges.
