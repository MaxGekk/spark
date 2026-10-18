# VARKA-389: The warm-up releases every wide projection kernel, on every machine

## 1. Where this came from

Row 389 of `m7/PLAN.md`, opened on 18 October 2026 by the close (`VARKA-261.md` 5.5, prediction
4) on the owner's word: milestone 7 does not close with this regression in it, and the tag
`varka-m7` placed that morning on `793536853ab` moves to the commit that resolves this row.

The close regenerated `VarkaColdStartBenchmark` on the milestone 7 build, as every close does, and
found the warm-up's verdict RELEASED, the 60-second deadline reached with no verdict, on 51 of the
file's 71 verdicts. The committed file of 28 September (`5778ec020f1`, engine `0914c607ff8`), same
laptop, same eight-core pin, same JDK build string, had 71 COMPILED. Two "Run benchmarks"
dispatches on GitHub-hosted runners, an AMD EPYC 7763 and a Xeon 8370C, read exactly the same 51
of 71. The split is by shape on all three machines: every projection shape of the benchmark
(`greatest(add_months(d, k), date_add(d, k), last_day(d))` at 16 to 100 entries, in the
first-query, back-to-back and steady-state sections) releases; the two range-filter shapes compile
10 of 10. The files: `sql/core/benchmarks/VarkaColdStartBenchmark-jdk25-results.txt` (laptop, 18
October), `-runner-7763-m7-` and `-runner-xeon8370c-m7-` (the pair), against the same names at
`5778ec020f1` and `-runner-xeon8370c-` of 27 September.

The regression was invisible until the close because the laptop nightly has not run since 26
September, and because PR #900's runner pairs of 16 October reported the same verdicts as a
small-host effect, which became `m9/SCOPE.md` item 103. Item 103 now carries the cross-machine
numbers and points here.

## 2. How much

Varka with the warm-up, 28 September against 18 October, the laptop:

| Row | 28 Sep | 18 Oct | Change |
| :--- | ---: | ---: | ---: |
| Once compiled, 16 entries, 100K rows, best | 27 ms | 35 ms | 1.3x slower |
| Once compiled, 54 entries | 26 ms | 53 ms | 2.0x |
| Once compiled, 100 entries | 42 ms | 81 ms | 1.9x |
| Back to back, median of queries 2 to 15, 54 entries | 128 ms | 187 ms | +46% |
| Back to back, 100 entries | 240 ms | 303 ms | +26% |
| Steady state at 2M rows, straight after the verdict, 16 entries | 48 ms | 125 ms | 2.6x |
| Steady state at 2M rows, 100 entries | 250 ms | 621 ms | 2.5x |
| Steady state, after two seconds of its own batches, 16 entries, average | 60 ms | 1,639 ms | 27x |

The Xeon 8370C runner, 27 September against 18 October: once compiled 37 to 64 ms at 16 entries
and 72 to 193 ms at 100; steady state at 2M rows 87 to 336 ms best at 16 entries and 520 to 2,062
at 100; the "own batches" average at 100 entries 1.7 s to 36 s.

Per query at 100K rows Varka still beats stock (187 ms against 428 at 54 entries), so a user sees
a smaller win, not a loss. At 2M rows the kernel runs 2.5x to 4x slower than its 28 September
self, the gap between code with its vector operations scalarised and code that boxes them, and
the averages with multi-second spikes read like a kernel that keeps deoptimising. The night's
surfaces, single-expression projections over a billion rows, read 11x to 54x against stock with
medians in line with 5 October (`VARKA-261.md` 5.5, prediction 1), so this is the wide,
multi-entry shapes, not steady-state kernels in general.

## 3. What the verdict measures

`VarkaKernelWarmup.run` does not ask the JIT whether a method is compiled. It calls the kernel in
blocks, then runs probe calls under `VarkaAllocationSampler` and compares the bytes they allocated
with an allowance (a fixed part, a part per probe row, a part per column per call); a kernel whose
probe stays within the allowance for `CLEAN_PROBES` probes in a row is COMPILED, and one whose
probe is still allocating at the deadline is RELEASED, with a reason string the benchmark does not
print. So RELEASED means "this kernel's calls still allocate after 60 seconds": either a loop or
epilogue method is not yet in C2 code, or it is, and the code still boxes vectors, or the call
path allocates something the allowance does not cover. The benchmark's call counts fit a kernel
running interpreted or boxed for the whole minute: about 56,000 calls in 60 seconds at 16 entries
on the laptop (VARKA-295's released outcomes read the same, about 62,000) against 13,500 calls in
1.9 seconds for the 28 September COMPILED verdict.

## 4. First findings, 18 October

Read with `dev/varka_emit.sh` on the 16-entry shape (`VARKA-389` scratch log `e1-emit16.log`) and
a smoke run of the benchmark (`VARKA_COLDSTART_SMOKE=true`, rung 16 only) under
`-XX:+PrintCompilation` with `-Dvarka.warmup.deadlineSeconds=10` (`e2-smoke.log`):

1. **Not HugeMethodLimit.** The kernel is one class of 16 group methods, four groups of four
   outputs, each group a dense and a masked loop with an epilogue each, 3,098 to 3,820 bytes of
   bytecode; every method is under 8,000.
2. **Every group method is past C1.** Each carries 379 to 430 Vector API call sites against the
   call-site budget of 93 (`VarkaEmitBudget.CALL_SITE_BUDGET`), and the emitter leaves them
   there by design: a group of at most `HEAVY_GROUP_OUTPUTS` (6) outputs may keep a method over
   the budget, on VARKA-209's census that "narrow groups settled within seconds and did not
   cycle". These four-output groups do not settle.
3. **The directive works as written.** The warmed class's methods are excluded from C1 ("made
   not compilable on level 3 ... excluded by CompileCommand") and C2 is asked for all sixteen
   within three seconds of the warm-up's start (JVM time 42.1 to 44.8 s for a warm-up released
   at about 52 s). So the profile is created in time and the compiles are requested in time;
   what is open is whether they finish and what they produce.
4. **C2 is near its limits on these methods.** An OSR compile of `loopMasked2` ends with
   `COMPILE SKIPPED: out of nodes during split`. A method of 400 Vector API call sites inlines
   hundreds of template bodies; past C2's inlining cutoffs the remaining calls stay calls, and a
   Vector API call that is not inlined is not intrinsified: it boxes. That is one mechanism that
   gives both symptoms at once, a probe that never comes clean and compiled code 2.5x to 4x
   slower.
5. **The shape carries VARKA-379's guard.** Every output's IR reads the date column through
   `(guardedRange col:0 -719162 2932896)`, the per-batch range check merged on 17 October
   (#874), three times per output. Whether that guard is what pushed these groups past the cliff
   is the first bisection point.

## 5. The investigation

In order, each step cheap enough to run inside the close's day:

1. The smoke rung at `78930efbbe3`, the commit before VARKA-379, same flags: COMPILED or RELEASED.
2. A flight recording of the master smoke rung (`jdk.Compilation` events for the sixteen methods,
   their durations and whether any fails, and the `org.apache.spark.sql.varka.KernelWarmup`
   event) with `-XX:+PrintInlining` filtered to the kernel class: which call sites C2 declines to
   inline, and why.
3. The emit tool at `78930efbbe3` and at `0914c607ff8` (28 September): the same shape's groups
   and call sites then, so the grouping's history is read rather than guessed.
4. The fix direction, priced: `heavyGroupOutputs` lowered so that groups of this weight split,
   measured on the smoke rung (verdicts, once-compiled and steady-state rows) and then on the
   full benchmark at both widths and a runner pair; or, if step 2 shows the compiles finishing
   with their templates inlined, the allocation the probe sees and where it comes from.
5. A check that would have caught this: the warm-up verdict of the 16-entry shape in a suite, on
   a runner, with a bound on the time to COMPILED, so a change that pushes a group past the
   cliff fails its pull request rather than the next close.

## 6. Predictions, registered before the run

1. The smoke rung at `78930efbbe3` reads COMPILED on the 16-entry shape (VARKA-379 is the
   change), with probability 0.5; if not, the bisection lands on one of VARKA-254 (`b5d1c36f22a`,
   grouping weights), VARKA-296 (`b72d71d727f`) or VARKA-86 (`b2e5ef370dc`), 0.35 together.
2. The flight recording shows every one of the sixteen tier-4 compiles finishing within the
   10-second window, so the release is about what the code does, not about the compile queue:
   0.6.
3. `-XX:+PrintInlining` shows Vector API call sites in the group methods declined for size
   (`NodeCountInliningCutoff`, `size > DesiredMethodLimit` or "already compiled into a big
   method"), where the range-filter kernel's methods show none: 0.6.
4. With the heavy groups split (`heavyGroupOutputs` at 1, or the budget applied to them), the
   smoke rung reads COMPILED within 5 seconds and the once-compiled row returns within 15% of
   28 September's 27 ms: 0.6.
5. The 28 September engine's emit of the shape has more groups and fewer call sites per method
   than today's, by at least a factor of two: 0.6.
6. The fix costs the steady-state throughput of narrow heavy shapes nothing measurable (the
   hold rule, `VARKA-261.md` 8): the full cold-start file after the fix has every row within 10%
   of 28 September's or better, and the surfaces' `date_add(d, 3)` row unchanged: 0.7.

## 7. Done when

The cause is named with the commit that introduced it; the fix is merged with
`VarkaColdStartBenchmark` regenerated on the laptop at both widths and a runner pair, every
projection shape COMPILED and the once-compiled rows within 15% of 28 September's; the README's
and `docs/sql-varka.md`'s cold-start row requoted from those files; a check in the suites that
fails when the 16-entry shape's warm-up does not reach COMPILED on a runner; `m9/SCOPE.md` item
103 rewritten to what remains of it, if anything; and the close (`VARKA-261.md`, #919) updated,
the tag moved to the fix's merge commit.

## 8. Outcome

To be written as the steps of section 5 run.
