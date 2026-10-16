# VARKA-326: The deoptimization cycle over the fuzz grammar's shapes, censused

## 1. Where this came from

Row 326 of `m7/PLAN.md`, found on 13 October 2026 by VARKA-274's admission check (`VARKA-274.md`
2(b) and 3.7, candidate 1): of 40 int-lane kernels drawn from `VarkaIrGrammar.drawShape` and
warmed the way `VarkaKernelWarmup` warms, with C1 kept off, three (k = 8, 18 and 25) never settled
in 30 s. Their loop methods entered the `profile_predicate` deoptimization cycle (`the-jit.md`,
"The deoptimization cycle is profiled loop predication's"), 97 to 110 traps each, and each had only
a masked driver. VARKA-274 9.4 saw the same cycle once more in the compiled test (1 of 14 int-lane
cases, 35 to 70 traps). The nightly's guard against the cycle forks one `make_date` kernel only, so
nothing in the tests says how often a kernel users run never settles. The row is done when the
cycle is counted over the grammar's shapes from the JVM's own trap log, the shapes that enter it
are named, and this record says whether a guard in the nightly covers them. A change to the
emitted loop, if the census calls for one, is a scope item of its own and is not made here.

## 2. The admission check, done

What had to be true for a census to be worth running: that the harness VARKA-274 built can draw
the grammar's kernels at volume, warm them on the path production warms them on, and name each
kernel in the JVM's own log, so that a verdict is read from the trap record and not from a
snapshot. Checked on 16 October 2026 on the laptop (AMD Ryzen AI 9 HX PRO 370, JDK 25.0.4.1,
preferred species 512 bits), at `1536cb3dafd`:

* **The harness exists.** VARKA-274's probe was deleted, but its design landed as
  `VarkaCompiledCheck`: `VarkaIrFuzzSuite`'s compiled test runs each draw of `drawInt`/`drawLong`
  (the same `VarkaIrGrammar` draws) over the eight histories, and its `warm-same` history is the
  probe's warm-up: production's class prefix and compiler directive (C1 off), calls of 32 rows plus
  the batch's remainder, both drivers alternated, until every loop and epilogue method of both
  bodies is C2 code in use, or a deadline. `-Dvarka.fuzz.history=warm-same` runs it alone and
  `-Dvarka.fuzz.settleMillis=30000` gives it VARKA-274's 30 s.
* **What it lacked, three switches** (section 4): a range start, so that four JVMs split 400
  draws a lane; the matrix base options in place of each draw's random ones, since VARKA-274's
  probe emitted under the defaults and the suite draws every option; and one line per kernel
  naming the class the JVM's log calls it by (`VARKA_CENSUS`).
* **The log names every kernel.** A 6-draw trial: 12 classes, each one's tier-4 compiles,
  `made not entrant` lines and `[deoptimization]` trap lines read from
  `-Xlog:jit+compilation=debug,deoptimization=debug` and joined to its draw by class name.
* **The snapshot is not the verdict.** On the first full arm, kernels with 100
  `profile_predicate` traps on one method still reported `settled` (one code-list read between two
  traps sees every method at level 4), so the census reads the cycle from the trap record (3.1).

It would have stopped the row at a documented reason if the compiled check could not be pointed
at the grammar's draws one history at a time, or if the log could not be tied to a draw.

## 3. The design

### 3.1 A census, no emitter change

`dev/varka_deopt_census.sh` runs the compiled test's `warm-same` history over a range of draws in
several JVMs, each pinned to its own cores with `taskset` and logging compiles and traps, and
`dev/varka_deopt_census.py` reads the logs. Per kernel it records the tier-4 `made not entrant`
count and the `profile_predicate` trap count of each method, the span from its first to its last
`profile_predicate` trap, the warm-up's settle time, and the shape: lanes, roots, input columns,
batch length, forced masking, and the NULL-mode nodes (below).

**The criterion.** A kernel is *cycling* by VARKA-189's rule, the one the nightly guard's parser
(`dev/varka_deopt_cycle.py`) applies: some loop method made not entrant at tier 4 three or more
times and trapped at `profile_predicate` more than four times (`PerBytecodeTrapLimit` is 4, so a
method that settles traps at most four times at its loop head and is recompiled once). Applied
here to the loop methods of both sides, where the guard's parser reads `loopDense` only. Beside
it the census gives VARKA-274's own cut, settle time over the compiled test's 10 s deadline with
more than four traps, and the count not settled at the 30 s deadline.

**A NULL-mode kernel** holds a node that nulls a lane from valid inputs: `IntArith` in NULL mode
(`int:ADD:NULL`, `int:SUB:NULL`, try_add and try_subtract) or a `MakeDate` that does not fail on
error (`makeDate:NULL`). `Analysis` then sets `nullsFromValidInputs`, and the emitter builds no
dense side: every batch runs `runMasked` (`VarkaLoopEmitter.emitDispatch`).

**Arms**, 400 draws a lane of seed 20260903 (VARKA-274's) unless said, four JVMs of six cores:

| arm | what | kernels |
| --- | --- | ---: |
| A | matrix base options, preferred width (512-bit) | 800 |
| B | as A at `-XX:MaxVectorSize=16` (128-bit), the gate's width | 800 |
| C | each draw's own random options, as the nightly fuzz job runs them (lanes-override draws run `cold` only and are left out) | 634 |
| R | A's last quarter (draws 300 to 399) in one JVM, again | 200 |
| N | R under `-XX:-UseProfiledLoopPredicate` | 200 |
| L | R with `PerMethodTrapLimit` and `PerBytecodeTrapLimit` raised to 100,000 | 200 |
| S | draws 343, 364, 375 and 381, each alone in a fresh JVM | 8 |

### 3.2 What is deliberately unchanged

The emitter and its loops (a change is the scope item section 9 lists), the nightly and its guard
(this row records what they cover; adding a guard is a candidate row), `VarkaCompiledCheck`'s
criteria and the compiled test's defaults: the three switches are off unless set.

### 3.3 Registered op counts

None: no emitted code changes.

## 4. Files

| file | what |
| --- | --- |
| `sql/catalyst/src/test/scala/.../varka/VarkaIrFuzzSuite.scala` | `-Dvarka.fuzz.compiledFrom` (range start; `compiled` stays the exclusive end), `-Dvarka.fuzz.compiledOptions=defaults` (the draws under `VarkaMatrix.base`, which is `VarkaEmitOptions.DEFAULTS` when no `-Dvarka.matrix.config` is set), `-Dvarka.fuzz.census=true` (a `VARKA_CENSUS` line per history run: lane, draw, history, class, settled, settle ms, the draw's label) |
| `dev/varka_deopt_census.sh` | runs an arm: the range split over JVMs pinned with `taskset`, provenance with the load average at start and end |
| `dev/varka_deopt_census.py` | reads the run directories: the verdict per kernel, the summary per lane, a TSV |
| `sql/varka/plans/m7/VARKA-326-census.tsv` | one row per kernel of every arm (2,842 rows, 129 KB: larger than `m8/census.tsv`, kept whole because the shares and the repeats need their denominators) |

## 5. Tests, and what each is for

No test is added: the census is a measurement. The switches leave the compiled test unchanged when
unset; the classifier's reading was checked against a hand reading of the first trial's log and
against the scratch reader the arms first ran with (the same 47 to 49 kernels, 9.2).

## 6. The measurement

The arms of 3.1, read from the JVM's own log. Not a benchmark: no rate is quoted, and the counts
are kernels and traps.

### 6.1 Predictions, registered before the run

None of this plan's own. The plan was written after the census ran (sections 1 to 9 together, as
the row's brief asked), so no prediction here was registered before a run, and none is invented
after one. The one registered beforehand is VARKA-274's prediction 6: "between 2% and 15% of draws
are unsettled at 10 s, and every unsettled one shows `profile_predicate` traps". It is scored in
9.1.

## 7. Risks

(Written after the runs, like the rest of 1 to 8: the risks the census was read against, not ones
foreseen.)

1. **The JIT is not deterministic**, so a verdict could be a coin. Read by repeats: the same
   ranges run again (9.2).
2. **Four JVMs share the machine**, so settle times stretch. The cycle is read from traps, which
   the load moves less than it moves seconds; the load average is recorded with each run.
3. **A reader that matched nothing would report zero cycling.** Guarded by the trap totals printed
   per arm, and by the first reading, which matched no trap line until the tag decorator's padding
   was allowed for.

## 8. Sequencing

One commit: the switches, the two scripts, the TSV, this plan and row 326.

## 9. Outcome

Measured 16 October 2026, 09:10 to 10:35 CEST, on the laptop at `1536cb3dafd` with this branch's
changes. Load average at each arm's start (the machine was otherwise idle; the load is the census's
own JVMs): A 0.07, B 3.15, C 3.93, R/N/L 5.28, S 3.95.

### 9.1 The census

| arm | lane | kernels | NULL-mode | cycling | of them NULL-mode | a method at 100 traps | 274's 10 s cut | not settled at 30 s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | int | 400 | 68 | 6 | 6 | 2 | 2 | 0 |
| A | long | 400 | 136 | 41 | 41 | 14 | 16 | 0 |
| B | int | 400 | 68 | 18 | 18 | 11 | 11 | 0 |
| B | long | 400 | 136 | 0 | 0 | 0 | 0 | 1 |
| C | int | 307 | 46 | 5 | 5 | 1 | 1 | 0 |
| C | long | 327 | 117 | 34 | 34 | 12 | 13 | 0 |

* **Every cycling kernel is NULL-mode, and every trap that makes the cycle is on its masked
  side.** 104 cycling kernels over the three arms, none outside the NULL-mode class (0 of 596
  others in A, 596 in B, 471 in C). Outside it, `profile_predicate` traps are rare and none
  meets the rule (0 kernels trap in A, 2 in B with 4 traps each, 4 in C with 4 or 5). Inside it the trap is nearly universal:
  200 of A's 204 NULL-mode kernels trap at least once, all in `loopMasked` methods. The cycle is
  the masked loop of a kernel that has no dense side.
* **About a quarter of the NULL-mode kernels cycle**: 47 of 204 in A (23%), 18 of 204 in B, 39
  of 163 in C. Which ones depends on the lane width and the lane type: at 512 bits the long lane
  (8 lanes) cycles in 41 of 136 and the int lane (16 lanes) in 6 of 68; at 128 bits the int lane
  (4 lanes) in 18 of 68 (`makeDate:NULL` 12 of 46) and the long lane (2 lanes) in none, every one
  of its 136 trapping one to four times and settling.
* **Within the class, small kernels cycle more.** Within the NULL-mode kernels of A the cycling
  share falls steeply with more roots (27 of 70 at one root, 13 of 57 at two, 7 of 77 at three)
  and more input columns (30 of 87, 14 of 68, 3 of 48), and is flat over batch length (22 of 98
  at 16 rows or fewer, 25 of 106 above) and forced masking (36 of 155, 11 of 49); by NULL-mode
  node, `int:ADD:NULL` 20 of 61,
  `int:SUB:NULL` 22 of 68, both 1 of 25, `makeDate:NULL` 4 of 46.
* **The cycle ends, at the JVM's trap cap.** No method of any arm passed 100 `profile_predicate`
  traps, and 16 kernels in A sat at exactly 100: `PerMethodTrapLimit` is 100 on JDK 25. With both
  limits raised (arm L) the trap never stops: 87,508 traps over the range's 49 NULL-mode kernels,
  up to 21,168 on one method. So on JDK 25's defaults a cycling kernel burns up to 100 traps and
  then runs, and what the cycle costs is the time that takes: from first to last trap, median 6 s,
  p90 15 s, the longest 24 s among A's 47 (B: median 10 s, longest 19 s; C: median 4 s, longest
  18 s); between compiles the method runs interpreted, since C1 is off on the warmed path. 18 of A's kernels were
  past the compiled test's 10 s deadline; none was still cycling at 30 s. VARKA-274's three
  kernels "never settled in 30 s" with 97 to 110 traps: the same cycle reaching the same cap on
  that day's timing.
* **`-XX:-UseProfiledLoopPredicate` removes it** (arm N): no `profile_predicate` trap in any of the
  range's 200 kernels, where R had 15 cycling and 521 traps. As VARKA-209 found, the flag's cost to
  everything else is unmeasured.
* **The cycle depends on what ran before in the JVM.** The four long-lane kernels 343, 364, 375
  and 381 reached 100 traps in every in-sequence run; each alone in a fresh JVM (arm S) trapped
  not at all, and int-lane 375 trapped 7 times without cycling. So `-Dvarka.fuzz.only=<k>` does not
  replay a verdict; the same range in the same order does (9.2).
* **VARKA-274's prediction 6**, between 2% and 15% of draws unsettled at 10 s, every one with
  `profile_predicate` traps: at its 10 s cut, 18 of A's 800 (2%; the long lane 16 of 400, the int
  lane 2 of 400), 11 of B's 800 (1%) and 14 of C's 634 (2%), every one with more than four traps.
  **Held** at the low edge for A and C, **missed** low for B; the trap half held throughout. What
  the census adds to it: only a masked driver, as VARKA-274's three had, is the whole class (104
  of 104), and the class has a name, the NULL-mode kernels.
* **VARKA-274's own three.** Int-lane draws 8, 18 and 25 are all NULL-mode (`makeDate:NULL` for 8
  and 18). At the preferred width under the matrix base, 18 cycles (13 traps, settled in 0.5 s),
  8 does not trap and 25 traps four times; at 128 bits all three cycle, 8 with 104 traps over 15 s.
* **The kernels not settled at a deadline are not the cycle**: B's long-lane 317, a 1-row batch
  with 4 traps, and two long-lane kernels of the scratch runs, one a 1-row batch and one whose
  drawn batch never entered its masked side, with no `profile_predicate` trap at all.

### 9.2 Repeats

The census first ran from scratch scripts, then again through `dev/varka_deopt_census.sh`; the
tables above are the second run. A: 49 and 47 cycling, 46 in both. B: 18 and 18, the same 18. C:
40 and 39, 37 in both. Draws 300 to 399 in four runs (A's last JVM twice, R, and a scratch rerun):
14 cycling in all four, 15 in any. Trap counts below the cap move between runs (13 to 100 on one
kernel); the set of cycling kernels hardly does.

### 9.3 What the nightly guards

* **The deoptimization-cycle step** (`dev/varka_nightly.sh` `deopt`, `dev/varka_deopt_cycle.sh
  --forms group --forks 10 --fail-on-cycle`) forks ten fresh JVMs per width, each running
  `VarkaDeoptCycleProbe`'s one twelve-output `make_date` kernel on the `batches` path over a
  null-free column. Its `make_date` fails on error, so the kernel has both sides (its
  `VARKA_DEOPT_METHODS` line lists `runDense` and `runMasked`), the null-free batches run only the
  dense one, and the parser counts `loopDense` methods only. It never runs a NULL-mode kernel or a
  masked loop, and its design, one kernel in a fresh JVM, would not see this cycle even on the
  right shape: arm S shows the cycling kernels settle alone.
* **The inlining-cliff step** forks the same way over the cheap-tail and `make_date` shapes, and
  reads inlining, not traps.
* **The compiled test** runs in the nightly fuzz job (`.github/workflows/varka-fuzz.yml`, 20 draws
  a lane per JVM) and in the platform step (`dev/varka_platform.sh`, 20 draws a lane per arm), with
  each draw's random options and a 10 s deadline. A kernel still cycling at 10 s is printed as an
  `unsettled:` line with its trap reasons and fails nothing; `-Dvarka.fuzz.requireTiers=true`,
  which the platform step sets, fails a lane only when no case of it settled
  (`VarkaCompiledCheck.warmUpsC2`). A cycle shorter than 10 s leaves no trace at all.

So **no guard in the nightly covers the shapes that enter the cycle**: the NULL-mode kernels'
masked loops in a JVM that has run other kernels.

### 9.4 Candidate rows (listed, not filed)

1. **A guard over the NULL-mode class.** `dev/varka_deopt_census.sh` over a fixed range, one JVM
   per lane width, failing when a kernel passes the 10 s cut; the class is a quarter of the
   grammar's draws, so 100 draws a lane holds about 50 of them. It would guard a fix; today it
   would fail.
2. **The emitted masked loop, a scope item.** Which predicate C2 hoists and loses in a
   NULL-mode `loopMasked` (the trap's `pc=` against the compare that branches to the stub, as
   `the-jit.md` reads one), and whether the loop can stop offering it, as VARKA-198 did by
   holding fewer segments. Not for this row.
3. **What `-XX:-UseProfiledLoopPredicate` costs** the kernels that never cycle, before anyone
   considers setting it.
