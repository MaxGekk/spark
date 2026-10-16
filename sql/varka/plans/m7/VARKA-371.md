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
