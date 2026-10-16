# VARKA-371: the JIT's per-compile trap sums, and why one vector array survives in a warmed kernel

*Row 371 of `m7/PLAN.md`. Outlined on 15 October 2026 from `VARKA-295.md` 9.5 and 9.6, in the
shape of `VARKA-258.md`: the audience, the claim, the evidence the record already holds, what a
reader can reproduce, the outline, and done-when. It serves point 5 of 1.1 (promotion continues)
and is ordered with the posts of 2.5. **The draft waits for the result of the fix on branch
`varka-295f`**: the post's last section is the remedy, and it is not written until that result is
known. This file holds the outline only.*

## 1. The question

The reader is a Spark or JVM developer who has used the Vector API or reads C2's logs. They know
that C2 inlines hot code and that a deoptimization throws compiled code away. They do not know
that the guard on one inlined call can depend on counters kept by other methods, or
that a kernel compiled at the top tier can still allocate. The post answers one question: **why
does a warmed Vector API kernel allocate one array per loop iteration on in a few per cent of warm-ups?**

## 2. The claim, and the five-minute version

A warmed Vector API kernel compiled at the top tier still allocates one `int[16]` per loop
iteration in a few per cent of warm-ups (3.6% to 8.2% in the six runs of `VARKA-295.md` 9.5),
because C2's class-check gate adds up the trap counts of every inlined copy of a shared template. In `doCall.cpp` a class-check miss path is an uncommon trap while the
compile-wide sums are under their limits and a real virtual call once they are not; the sums are
class-check traps against `PerMethodTrapLimit` (100) and decompiles against
`PerMethodRecompilationCutoff` / 2 + 1 (201), each adding a shared method's recorded count once
per inlined copy. `VectorOperators.opKind` is inlined 170 times in one loop and holds 4 to 5
traps, left by earlier standalone compiles of the Vector API's templates that guessed one
operator class and met another. The real call makes the `binaryOp` intrinsic refuse the vector,
the box that stays expands to a `new int[16]`, and escape analysis does not remove it. Two JVM
options remove it.

The five-minute version, in four paragraphs: a kernel that should not allocate does, sometimes;
how it was found to be neither a deoptimization cycle, nor a bailout, nor a compile nobody asked
for; the two sums and the one method that pushes a compile past them; what to set, and what a
program can and cannot do about it from inside.

## 3. The evidence the post cites

9.5 is in `VARKA-295.md` on master; 9.6 is in the pull request on the fork's `varka-295e` branch
(#825) until that merges;
the post's numbers are quoted from the committed files under `dev/varka_quote_check.py`.

* **Step 1 (9.5):** the warm-up arm run seven times with the JIT's own log, 171 releases at the
  deadline classified. None is a deoptimization cycle, a C2 bailout or a policy that never asked
  for the compile; all are the same cause: every loop method compiled at tier 4 and in use, yet
  one `int[16]` surviving per iteration, 20,480 bytes a probe (4 calls x 64 iterations x 80 bytes).
* **Step 2 (9.6):** the source reading (`doCall.cpp`, `vector.cpp`), the log lines
  (`observe trap='class_check' ... ccount='254'` in a released compile, none in a clean one), a
  clean `loopDense0` totalling 4 traps against a released one passing 100 at parse 427 of 3,309,
  and the table of runs: prefix `{0,1}` found by delta debugging, 55 of 220 compositions (25%)
  releasing alone, 18, 10 and 39 releases in 400 compositions without an option, and none in
  1,000 warm-ups with `-XX:PerMethodTrapLimit=100000 -XX:PerMethodRecompilationCutoff=100000`
  (the cutoff alone left 3 to 11).
* **The shape of the cause:** 54 more kernels do what composition 0 does; it is a race between a
  template's standalone C2 compile and the first operator class it had not seen.
* **What a program can do about it (row 370):** none of the three limits is a manageable flag, a
  compiler directive cannot reach them, WhiteBox can but needs two JVM options and a jar. Filled
  in from `VARKA-370.md` 9 when that row is done.

## 4. The outline

1. **A kernel that allocates when it should not.** One probe, 20,480 bytes, in a few warm-ups in a hundred; the
   same shape clean on the next run.
2. **Ruling things out.** Reading every release from the JIT's own log: not a deoptimization
   cycle, not a bailout, not a policy that never asked. What each looks like in the log, in a
   line each.
3. **The two sums.** What `doCall.cpp` does on a class-check miss, the two limits, and why a
   method inlined 170 times counts 170 times. The one figure of the post: the sum over parse
   positions in a clean and a released compile.
4. **Who leaves the traps.** Standalone compiles of the shared templates and the first operator
   class they had not seen.
5. **What removes it.** The two options, with 0 releases in 1,000 warm-ups, and why they are an
   operator's choice: they cannot be set at run time (row 370), and a different pair (the
   per-method and per-bytecode trap limits) caused a deoptimization cycle in step 1's run, so which
   options matters.
6. **What the library could do.** The result of `varka-295f`'s remedy, in a short section, and
   what a Vector API user can do without Varka. **Not written until that result is known.**
7. **A link** to the reproduction.

## 5. What a reader can reproduce

* The replay of one composition with the JIT's log on and the two options set or not, a command
  line in the post and the script under `sql/varka/demo/` (the warm-up arm's `warmupSet` and
  `warmupShow` switches of 9.6 are the starting point).
* The log lines that name the sums, found with one `grep` over `-XX:+LogCompilation` output.
* The limits' defaults, from `java -XX:+PrintFlagsFinal -version`, with the JDK and CPU stated.

Reproducibility depends on a rate of a few per cent, so the post gives the number of warm-ups to
run (1,000) and the expected count, not a single run.

## 6. Done when

The outline's sections 1 to 5 are drafted and kept short (the owner cuts length from Varka's public
writing); section 6 is written from `varka-295f`'s result; every number traces under
`dev/varka_quote_check.py`; the replay runs from a clean checkout; and the post is published,
named with the JDK it was measured on.

## 7. Sequencing

1. This outline, and the row marked outlined.
2. `varka-295f`'s result, then row 370's verdicts.
3. The draft, then publication, then section 9.

## 8. Explicitly out of this task

* The remedy itself (row 295 and its follow-up) and the check of row 370; the post reports them.
* A proposal to the OpenJDK project about the gate; the post can say a sum per compile is a
  design choice and leave the proposal as its own decision.

## 9. Outcome

### 9.1 The draft, 16 October 2026

`POST_TRAP_SUMS.md` is the draft, about 1000 words on 4's outline, written once the fix's result
was known (`VARKA-295.md` 9.7 to 9.9). Section 6 of the outline is its "What a library can do",
including what the fix does not cover (`VARKA-373.md`, `VARKA-374.md`). Still owed before
publication: the replay script under `sql/varka/demo/` and its run of 1,000 warm-ups (5), row
370's final verdict on what a running JVM can do (the draft states what its admission check
found), and the owner's edit.

**The replay, 16 October 2026.** `sql/varka/demo/trap_sums.sh` is the script 5 asked for, and
`trap_sums-laptop-jdk25-output.txt` beside it is its run; the post's "Reproducing it" names both.
Each replay is a fresh JVM running the warm arm of `VarkaSparkFuzzSuite` at seed 20261008 over
`warmupSet=0,220` (9.6's two-composition reproducer: `abs(ym)`, then shape `67312f01`), with
`jitEvidence=false`, from the classpath `dev/varka_matrix.sh --module sql --list` exports. Three
arms alternate replay by replay: the template warm-up off (`-Dvarka.warmup.vectorTemplates=false`,
the JVM the post describes), the same with `-XX:PerMethodTrapLimit=100000
-XX:PerMethodRecompilationCutoff=100000`, and stock Varka. A release is a line of the arm's
"released at the deadline" report in a run whose test passed. `--keep` adds `-XX:+LogCompilation`
to the first arm until a replay releases and prints, per C2 compile of the kernel's class, the
first `<observe trap='class_check' ... ccount=.../>` line; `--compositions K` runs the
K-composition arm instead.

What it showed, on the laptop pinned to cores 6 to 8 at a load average of 5 to 45 from other work,
against the classes of master 81aad3b42fa:

| run | template warm-up off | off, with both options | stock |
|---|---|---|---|
| 100 replays of `0,220` each, 9,577 s | 77 of 100 | 0 of 100 | 0 of 100 |
| 400 compositions, one JVM each, 1,125 s | 29 (of 534 verdicts) | 0 | 0 |
| `--keep` | released at the first replay; four compiles of `67312f01`'s loop and epilogue methods, each first at `ccount='100'`, the sum at the limit in 602 to 892 of their trap checks | | a stock replay's log: no `ccount` line at all |

**Against the post's numbers.** The 400-composition run's 29 sits inside 9.6's 10 to 39 in 500,
and the options' 0 agrees with its 0 in 1,000, though one run of each is not 1,000 warm-ups. The
outline's plan (5) of 1,000 replays rested on a rate of a few per cent, which is the 400-composition
arm's rate per warm-up; the two-composition replay releases in most JVMs (77 of 100, 9.6 had 5 of
6), so 100 replays per arm separate the arms by 77 to 0 and 1,000 were not run: at about 32 s a
replay on 3 cores, 3,000 would take over a day. The post's paragraph now gives the replay's rate per JVM and the 400-composition run's per warm-up.
Every release was iteration 220's kernel. With the log on, the first arm released in 6 of 6
replays (the five tries while the script was written, and `--keep`'s), so the log is for seeing a
release and the counts are taken without it; why the log raises the rate was not examined. After the review the script takes a relative `--out`, refuses a
classpath that is not sql's (`dev/varka_deopt_census.sh` leaves catalyst's in the same place),
writes each replay's exit status to `status.tsv` and exits 1 when every replay of an arm failed,
and prints its header for `--help`; the counting is unchanged, and one replay per arm after the
change released 1, 0 and 0. The post's 128-bit sentence now follows `VARKA-374.md` (x86 has no
2-lane mask), where it had said "most Arm cores" and "still open".

**Row 370's verdict** (merged as #864, `VARKA-370.md` 9) replaced the paragraph of "What removes
it" that began "They are an operator's choice", in the coordinator's wording adjusted to the
post: the in-process agent reset that took the releases from 58 and 68 to 1 and 0, and why it is
not a fix. "Within seconds" became "within half a minute", the interval at which `results/` saw
the counts back. Still owed before publication: the owner's edit.

### 9.2 Review after merge, 16 October 2026

A code review of #872 reported eight findings. Each was checked against the files and the
committed output before anything changed; all but the last were confirmed and fixed.

1. **Kernels where the record counts compositions: confirmed, fixed.** The post said "about 440
   in 2,000 kernels", "55 of 220 kernels" and "the first 100 kernels", and its measurement
   section said the fuzzer "warms one kernel per random composition". The sources count
   compositions (`VARKA-295.md` 9.8 3, the "prefix each of the 220 compositions alone" row of
   9.6, the "first 100 compositions" of `VARKA-373.md` 9), and a composition can warm more than
   one kernel: the 400 compositions warm 534 kernels. All four now say compositions, as do
   Figure 2's note and Figure 4's subtitle. "Kernels released at the deadline" stays where it
   is the numerator; the 1,000 and 500 warm-ups of "What removes it" already matched 9.6.
2. **The replay's compositions could move silently: confirmed, fixed.** `warmupSet=0,220` names
   draws over `coverage.json`'s rows. The script now runs one JVM first with
   `-Dvarka.sparkfuzz.warmupShow=true` and stops unless compositions 0 and 220 print as pinned in
   the script (`abs(ym)`, and the four outputs and filter of 220; two JVMs printed them
   identically), and in the replays of `0,220` it names any released shape other than
   `w7fd4deef67312f01` and keeps that replay's report. The check is its own JVM, so the counted
   replays run as before. The post says the script stops unless its compositions are commit
   57cf25f8c98's: `coverage.json` did not change between the 100-replay run's classes
   (81aad3b42fa) and it, and the refreshed runs below were built from it. The post does not send
   readers to that commit, whose script is #872's.
3. **`--keep` read a failed replay as a clean one: confirmed, fixed.** Each try now has its own
   directory; a failed try keeps its report (not its JIT log) and is counted, and when every try
   failed the script says so instead of "no release". A stub `java` drove both paths, and the
   wrong-shape stop.
4. **`--keep` printed `ccount='100'` where the post quotes 254: confirmed, fixed by printing the
   largest.** Within one compile the observed sum only rises (100, 104, 112 and on, in steps of
   one inlined copy), so the script now prints the line with the largest `ccount` per compile. The
   old log was gone, so `--keep` ran again: `ccount='1100'` in the loop and epilogue dense
   compiles, 1372 in the masked ones, against 9.6's 254 in its run; the sum at a compile's end
   varies run to run, the form is what reproduces. The post quotes the 1100 line.
5. **"A clean replay has none" was a hand-made contrast: confirmed, fixed.** After the release,
   `--keep` replays the stock arm once with the same log and runs the same check on it, which
   printed `none` (0 `ccount` lines in the whole log, 133 class-check observations). The post
   says "finds none as Varka ships"; the output file's hand-written lines are replaced by what
   the script prints.
6. **Figure 3's middle block gave all arms the defaults arm's warm-ups: confirmed, fixed.** A
   `--compositions` replay's line now ends with its verdicts, `fig43.py` reads each arm's own,
   asserts its releases against the summary table and labels each bar "n of warm-ups"; the lower
   block takes its count from the spike's runs. That needed the 400-composition run again.
7. **The catalyst-classes prefix: confirmed, fixed.** sql's exported classpath already holds
   catalyst's jar and test classes; the script now uses `$(cat "$BUILD/classpath")` as
   `dev/varka_matrix.sh` does, and every run below used it.
8. **Share `replay()` with `varka_matrix.sh`: declined.** The matrix launches `VarkaSuiteRunner`
   over whole suites with its own arguments; `replay()` launches ScalaTest's `Runner` for one test
   with a per-replay working and temporary directory. What they share is the environment, the JVM
   options and `-cp`, three lines; a sourced helper would couple two scripts for less than that.

**The refreshed runs** (`trap_sums-laptop-jdk25-output.txt`, cores 6 to 8, load average about 5
falling to 2): the 400-composition run released 46 of 534 warm-ups with the template warm-up
off, 0 of 534 with both options, and **1 of 534 as Varka ships**: composition 312's kernel,
`e33b1eb0`, the second reproducer 9.6 names, at the deadline with 21,120 bytes in its last probe.
Two more stock JVMs over the same 400 compositions on the same cores, the command `replay()` runs,
gave 1 (the same kernel, 21,280 bytes) and 0. So as shipped, this kernel released in 2 of 3
JVMs on 3 cores here, where 9.8 saw 0 in 2,000 compositions on each of five seeds with 5 cores. The
core count is the difference visible between the runs, not a shown cause. The post's "none"
sentences quote 9.8 and are unchanged; Figure 3 shows the release and its caption names it. The
defaults arm's 46 is above 9.6's 10 to 39 and the earlier run's 29. Whether the stock release
becomes a row is the owner's decision; it is outside this task. Logs: the committed run's release
in `target/varka-demo/trap_sums/stock-1/run.log` of the `varka-371-fixes` worktree, the two
extra JVMs in `.../scratchpad/stock400-2/` and `stock400-3/` (`replay()`'s java line with
`-Dvarka.sparkfuzz.warmupIterations=400` and no other option, cores 6 to 8, one after the other).
Figure 1 is unaffected by the new quote: it reads 9.6's log and labels the limit and the parse
position where the sum passes it, not the sum at the compile's end. The 100-replay run was not
repeated: its counting did not change, and it takes 2 hours 40 minutes. `--keep` released at the
first try, so the log-on defaults arm now stands at 7 of 7.

**Self-review of this change** (`/code-review high`, ten findings, deduplicated but not verified
by the tool; each checked here):

* *A wrong-shape release stopped the run* and blamed `coverage.json`, though composition 0's own
  kernel can miss the deadline under load, and an emitter change renames a shape while the
  compositions stay. Fixed: the replay line names the other shapes and keeps the report, and the
  run goes on. The `grep -q` pipeline that could lose the match to SIGPIPE went with it.
* *The post named a commit whose script lacks the check*: fixed as in item 2 above.
* *"To none" against Figure 3's release*: kept. The sentences quote 9.8's 0 in 2,000 compositions
  on five seeds; the caption carries this run's release, and the owner decides whether the claim
  needs a new measurement.
* *`fig43.py` lost the check that the spike's runs and the arms warm as many kernels*: restored,
  over all three arms and both spike runs (534 each). It also reads the composition count from
  the run's summary line instead of a fixed 400, and refuses a run of more than one replay.
* *The verdicts dropped the count of releases before the deadline*: fixed by appending it when it
  is not 0, so the committed output, where it was 0 in every arm, is still what the script
  prints. The output file's note on composition 312 is marked as added by hand.
* *The keep directories of an earlier run survived*: `--keep` clears them first.
* *The check costs a JVM per invocation* (about half a minute): declined. Checking offline would
  re-implement the suite's draw, and half a minute is small beside a 400-composition run.

**The claim scoped to its data** (the coordinator, after the self-review). This corrects the two
places above that kept "none" unchanged: the post may not say "none" beside a figure that shows a
release. "In short" item 3 now says the warm-up "all but removes" the releases: none in 12,000
compositions on five cores (9.8's seed 20261008 and five more, 2,000 each) and 2 of 1,602
warm-ups on three (the three 400-composition JVMs as shipped, 1, 1 and 0 of 534 each). "What a
library can do" says the same and calls the core count the one visible difference, not a cause.
The two options still "remove" it: 0 in every run. Figure 3's caption now says the upper block
replays composition 220 after composition 0 in fresh JVMs and the middle one runs 400 compositions,
a longer history, so "0 as shipped" and "1 as shipped" are not read as one measurement; the
reproduction paragraph quotes the 400-composition run's 46, 0 and 1, tells the reader to run from
a current checkout of master (the composition check exists only there), and the plan files of
"How these were measured" are links. The post fell from 1,197 to 1,195 words; the cuts are a
sentence on what C2 leaves on a miss path (the next paragraph says it), the 4-lane halving of the
agent reset, the two-shape detail of the 4-lane trap, the log options' list and the run time.

The as-shipped release (composition 312, `e33b1eb0`, compiled and still allocating at the deadline)
is for the reproduction of m8 row 383 (planned in #888, not yet merged).
