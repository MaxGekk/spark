# Task 77: the benchmark's profile, not the benchmark's code

## 1. Where this came from

`PLAN_MILESTONE_4.md` row 77 and section 2.39, out of task 70's review. The
parity file's `fused, 64 ops` case - four disjoint depth-16 chains, masked -
read 270.4 to 273.2 M rows/s at 128-bit through every regeneration up to
`00eeed82279`, then 8.8 at `aef0b82260e`, the commit that moved the per-group
validity OR ahead of the compute. At 8.8 the fused kernel was slower than the
64 sequential single-op passes it exists to beat. The same commit made the same
row 1.8x *faster* at AVX-512, which is presumably why it read as a win.

**Section 2.39's investigation is done and this plan does not redo it.** Two
causes are excluded by measurement and are not to be re-tested: not a
method-size cliff (masked loop 898 bytes with the pass off, 804 with it on,
both far under `HugeMethodLimit`), and not a compile failure (under
`-XX:+PrintCompilation` at four lanes both arms take every masked method to
tier 4 with no bailout). In a fresh JVM the two arms are within 5% of each
other. The emitted code was never slower.

What the JVM's own output says instead: in the benchmark's JVM each of the four
`loopMasked` methods is compiled to tier 4 six times and made not entrant
thirteen times across the timed window, where a healthy kernel compiles once and
settles in about twenty milliseconds. `-XX:+LogCompilation` attributes that to
nine `profile_predicate` traps per method with action `maybe_recompile`, plus
one `unstable_if` - C2 hoisting a loop predicate on the strength of a profile,
the predicate failing, and the method being rebuilt instead of run. The profile
is not this kernel's: about ninety earlier kernels in the same file have already
written the shared Vector API templates by the time this case runs.

So `aef0b82260e` did not create the collapse. It moved this shape across a
threshold where an existing mis-speculation began to bite, and task 70's bitmap
pass moved it back (966.6 and 961.6 in the two regenerations since, above the
pre-regression baseline) without touching the cause.

### 1.1 This is the head of a cluster, and the cluster is the reason it matters

Three debt-register entries share one signature, and all three name isolating
the JVM as a closing option:

* **This row** (adopted as task 77).
* **The parity harness's depressed-row cluster.** Every regeneration since task
  52 has shown one cluster of unrelated rows 10-30% below the committed file -
  once a single row at -99% - a different cluster each run, while the rows the
  task is measuring reproduce within 2%. Every task since 52 has had to run the
  benchmark twice to tell its own movement from the run's cluster.
* **The `dayofweek, chunk 64/63` rows' bimodality.** Two of five regenerations
  on branches read those three rows 23-38% under both master runs, with no code
  those rows execute changed.

The file carries 494 `profile_predicate` traps in total; this kernel's 36 are
one large cluster and a second bad kernel is another. So the phenomenon is the
file's, not the shape's, and none of it is visible in the committed numbers as
anything but a slow row.

### 1.2 The precedents, one of which is this same case

**The engine module's JMH harness had this class of problem and closed it by
isolation.** `forks = 1` per runner, plus a `@State` keeping the second int
species out of every other fork. The entry records what that bought: the number
task 24 could previously only reach by forcing C2 to inline the Vector API -
`vectorDateDiff` null-free at 10000 rows, 1276 against the in-process 435 - is
what a plain forked JVM measures (1211). The flag had been measuring the
harness.

**And this exact case collapsed once before, in task 11.** `PLAN_TASK_11.md` 6.3
records a ~100x history-dependent collapse of the 64-op kernel: 1.0 G rows/s
fresh, 9 M after seven other kernels had run in the same JVM. That cause was C2
*compile latency* on a monolithic method - the tier-4 OSR compile took about ten
seconds - and the cure was structural, `GROUP_BUDGET = 16` splitting the kernel
into sibling loop methods. That fix is why this case was assumed safe, and it is
worth stating plainly that task 77's mechanism is a different one:
deoptimisation from a borrowed profile, not a compile that has not landed yet.
The benchmark's own comment at the case still cites task 11's condition.

## 2. The admission check, to do first

Section 2.39 named the mechanism for one kernel. Four things this task rests on
are **not** established, and section 2 is establishing them. Do not write the
`--fork-per-section` flag first.

### 2.1 The census: which rows are affected, and by how much

494 traps are attributed in aggregate and to two kernels by inspection. Nothing
says which rows carry the rest, so "isolate the JVM" is a fix whose benefit
cannot be sized and whose regression risk cannot be bounded.

Produce, from one `-XX:+LogCompilation` run of the parity benchmark at 128-bit,
a table of every compiled kernel method with its tier-4 compile count, its
not-entrant count and its trap counts by reason. Rank by trap count. A committed
script does the attribution, because the next person will need it too.

**The trap section 2.39 records for whoever does this:** the log is 883 MB for a
single run, and its `<klass>` and `<method>` ids are scoped per compilation unit
rather than globally. Resolving traps by those ids attributes them to whatever
unrelated method shares an id - a mistake the first pass of that investigation
made. Attribute through `<nmethod compile_id=...>`, whose ids are global, and
reproduce the counts once before quoting them.

### 2.2 Does isolation actually fix it, through the real harness

The fresh-JVM probe drove the shape through `VarkaEmitDump`'s `--rounds` and
`--nulls`, not through `VarkaEmitterParityBenchmark`. That is enough to say the
emitted code is not slower and not enough to say the harness change works.

Run the one section that holds this case in its own JVM through the real
benchmark class, at both widths, and compare against the same section inside the
full run. If the row does not recover, forking is not the fix and the task turns
to the second candidate (2.5) with that recorded.

### 2.3 Does the same condition reach the closing measurement

This is the part that decides whether task 77 blocks task 62.

`DateSurfaceBenchmark` builds one `SparkSession` and loops over every
`Surface.ENTRIES` entry - about fifty shapes since task 68 - in one JVM. That is
structurally the same condition the parity file has: many kernels writing the
shared Vector API profiles before the later ones compile. Whether it bites there
is unverified, and the closing measurement's audience is outside this
repository.

Check it the same way: one `-XX:+LogCompilation` run of the surface driver at
whatever row count keeps the run tractable, the same census, and a comparison of
the first-run and last-run entries' rates. If the traps are there, the public
table needs either isolation or a caveat, and that is a finding task 62 has to
carry whatever this task does about it.

`VarkaThroughputBenchmark` gets the same question and is a third case again: it
runs about 46 sections in one JVM but each section is an end-to-end Spark query,
where the kernel is a smaller share of the time.

### 2.4 What forking costs, per harness - and it is not one answer

| harness | sections | per-fork setup | verdict to establish |
|---|---|---|---|
| `VarkaEmitterParityBenchmark` (catalyst) | 15 | no `SparkSession`; per-case data built in the block | 15 JVM starts, expected cheap |
| `VarkaThroughputBenchmark` (core) | ~46 | ~10 cached tables of 2M rows, built once for the whole run | forking per section rebuilds every fixture per fork |
| `DateSurfaceBenchmark` (bench module) | 1 loop, ~50 entries | one table at 500M-1B rows | per-entry forking is not on the table |

Measure the setup cost per harness before choosing. The plan's expectation is
that the parity file forks per section, the throughput file forks per *group* of
sections or not at all, and the surface driver does not fork - but that is the
thing to establish, not to assume.

### 2.5 The second candidate, and why it is not first

Section 2.39's other route is to reduce what the emitted loop offers C2 to
speculate about. That is an emitter change on the default path for every kernel,
and it wants 2.1's census first: without knowing which shapes trap, it is a
guess at what to remove. It stays in scope as the answer if 2.2 says isolation
does not work, and out of scope otherwise.

### 2.6 What the check would have rejected

That the deliverable is a `validityOrFirst` rule - section 2.39 proposed one
before the reason was known, and the reason retired it. That the collapse is
about the validity OR at all: the OR moved this shape across a threshold, and
the threshold is the profile. That a fresh-JVM probe of the emitted code settles
whether a harness change works. And that one forking rule can serve three
harnesses whose per-fork setup differs by four orders of magnitude.

## 3. The design, if the check passes

### 3.1 A section filter that leaves Spark's harness alone

`BenchmarkBase.runBenchmark` is `final` and `BenchmarkBase.main` opens the
results file with a truncating `FileOutputStream`, so a class run once per
section would leave only the last section in the file. Neither is worth
changing in a Spark-derived file that upstream also edits.

So the filter lives in the Varka benchmark classes, which already receive
`mainArgs` through `runBenchmarkSuite`:

* `--list-sections` prints one line per section, in order, and exits. The list
  is the same list `runBenchmark` is called with, derived from one place so the
  two cannot disagree.
* `--section <n>` runs only that section.

Each fork writes its own results file; `dev/varka_bench_regen.sh` concatenates
them in section order, so the committed file's format does not change and a
reader cannot tell from its contents that it was produced in pieces. The
provenance file records the methodology, as the JMH entry's regeneration did.

### 3.2 The runner flag

`dev/varka_bench_regen.sh` gains `--fork-per-section`, which enumerates sections
with `--list-sections`, runs one JVM each under the same pinning and the same
load gate, concatenates, and writes `sections: forked` into the provenance. The
existing single-JVM path stays the default until the numbers say otherwise.

### 3.3 The parity case the milestone row requires

After task 70 the per-group OR lives only on **unserved** roots, and nothing in
the file measures a large instance of it. Verified against the emitter rather
than assumed:

A root is served only when the bitmap pass is on, the root is not a `Cond`, its
word is *pure* (`Analysis.pureOf` returns a `WordExpr`), and that word flattens
to a **single** operator - one AND or one OR. So the unserved shapes are:

| shape | why |
|---|---|
| a `Cond` root | never served; a selection bitmap is computed, not derived |
| an `IfElse` root | `pureOf` returns null: the word is computed per lane group |
| `make_date` | same, via `nullsFromValidInputs` |
| **`IntArith` in `Overflow.NULL`** (`try_add`, `try_subtract`) | same; **section 2.39 misses this one** |
| a **mixed** AND/OR tree | pure, but `BitmapPass.of` declines a tree that is not one operator; the only category counted in `declinedBitmapRoots` |

Two corrections to section 2.39 that this task carries:

1. **The closure over subtrees.** `andExpr`/`orExpr` return null when either
   operand is null, so *any* root whose subtree contains an `IfElse`, a
   `make_date` or a NULL-mode `IntArith` is unserved - `year(if(...))`,
   `datediff(make_date(...), d)`. Section 2.39 lists only bare roots.
2. **Two of them never take a dense body at all.** A non-ANSI `make_date` and a
   NULL-mode `IntArith` set `nullsFromValidInputs`, and the emitter then emits
   no dense body, so every batch of those shapes takes the masked methods and
   the per-group OR. They are not "the null case" of those shapes; they are
   those shapes.

**The A/B, and the lever it uses.** For an unserved root, `validityByBitmap` is
not a lever: the OR is emitted either way. So the pair is two roots whose
compute is as close as the algebra allows and whose *words* differ in exactly
the thing the pass keys on:

| arm | root | word | served |
|---|---|---|---|
| control | `datediff(d, d2)` | one AND over two input bitmaps | yes |
| subject | `datediff(greatest(d, d2), greatest(d3, d4))` | AND over two ORs - mixed | no |

Both masked, mixed nulls. The subject pays two extra `Greatest` blends, which is
the confound and has to be priced: a third arm, `datediff(greatest(d, d2),
greatest(d, d2))` - still mixed, so still unserved - does not remove it either.
The honest instrument is the subject run with `validityByBitmap` off on the
*control*, so both arms carry the per-group OR and the difference is the two
blends alone; the OR's own cost is then the control's two arms.

Case ids: the file requires them unique and refuses a duplicate, and two blocks
compute theirs, so they are enumerated with `dev/varka_bench_ids.sh` rather than
read off the neighbours. The maximum in use today is 961.

### 3.4 What this task does not touch

The emitter, unless 2.2 fails and 2.5 is taken. `GROUP_BUDGET` (task 71). The
validity helper choice (task 76). Task 47's per-lane-group remainder. The
committed *values* in any results file beyond the regeneration the methodology
change forces.

## 4. Files

| file | what |
|---|---|
| `dev/varka_trap_census.py` (new) | the `LogCompilation` attribution of 2.1, through global `nmethod` ids |
| `VarkaEmitterParityBenchmark.scala` | the section list, `--list-sections`/`--section`, and 3.3's cases |
| `VarkaThroughputBenchmark.scala` | the same filter, if 2.4 admits forking there |
| `dev/varka_bench_regen.sh` | `--fork-per-section`, the concatenation, the provenance line |
| `VarkaLoopEmitterSuite.scala` | the served/declined count test extended to a NULL-mode `IntArith` and to a subtree closure, neither covered today |
| `PLAN_MILESTONE_4.md` | 2.39's two corrections, row 77, and the two sibling debt entries swept in the past tense with what the census found |
| `SKILLS.md` | the lesson, whichever way the check goes |

## 5. Tests, and what each is for

* **The served/declined counts, extended.** Today's per-shape test covers
  `IfElse`, `make_date`, `Compare` and the mixed `datediff(greatest, greatest)`.
  It does not cover a NULL-mode `IntArith`, and it does not cover the subtree
  closure. Both are asserted, because 3.3's corrections to 2.39 are claims about
  the emitter and belong in the emitter's suite rather than in a plan.
* **The section list agrees with the sections.** A benchmark whose
  `--list-sections` misses a section would silently drop it from a forked
  regeneration - the file would look complete and be short. Derived from one
  list, and the dry-run path asserts the count.
* **The census script, on a committed fixture log slice.** Small enough to
  commit, large enough to exercise the id scoping the first pass got wrong.

No differential and no correctness test: this task changes a harness, and if it
ends up changing the emitter (2.5) that half arrives with the emitter's own
gates.

## 6. The measurement

The measurement *is* the task, so section 6 is what to record rather than an
A/B beside a feature.

1. **The census table** (2.1): every kernel method by trap count, at 128-bit.
2. **One section, forked against in-run** (2.2), both widths.
3. **The whole file, forked per section against the committed file** - the diff
   is the finding, and it is expected to be large in both directions.
4. **The surface driver's exposure** (2.3), as a yes or no with its evidence.
5. **3.3's arms**, once, so the per-group OR on an unserved root has a number.

### 6.1 Predictions, registered before the run

1. Isolation removes the loop for this kernel: tier-4 compiles per masked method
   fall from six to one and not-entrant from thirteen to one, and the 128-bit
   row lands within 10% of its fresh-JVM rate.
2. The 494 traps concentrate rather than spread: more than half of them sit in
   fewer than five kernels.
3. The rows that move most between the shared-JVM and forked runs overlap the
   depressed-row cluster the debt register has been recording since task 52. If
   they do not, the cluster is a second phenomenon and stays open.
4. The surface driver shows the same traps. Its loop is fifty kernels in one
   JVM, and nothing about it differs from the parity file except that each
   kernel does more work per call.
5. The per-group OR on an unserved root costs under 10% of the control at both
   widths - it is one call per lane group against a loop body - and if it is
   more, that is a finding about `orValidityBits` rather than about the pass.

## 7. Risks

1. **A methodology change invalidates every committed number in the file**, so
   it forces a whole regeneration and the provenance has to say so. The JMH
   entry did exactly this and is the precedent for how to record it.
2. **Forking per section weakens cross-section comparison.** This project's A/B
   doctrine is that two arms sharing a run share a JVM, a layout and a clock.
   Forking preserves that *within* a section and destroys it *between* sections.
   Every A/B in the file must therefore have its arms in one section, which is
   already true today and becomes a rule the file has to state.
3. **The census misattributes.** Per-compilation-unit ids, an 883 MB log; the
   counts get reproduced once before they are quoted.
4. **The fix does not generalise.** If forking is right for the parity file,
   wrong for the throughput file and impossible for the surface driver, the task
   ships one harness fixed and two documented - which is a legitimate outcome
   and should be written as one rather than stretched into a rule.
5. **2.5 is an emitter change on every kernel's default path.** It only opens if
   2.2 fails, and it opens with its own admission check.

## 8. Sequencing

1. The census script and 2.1's table.
2. 2.2's one-section probe, both widths.
3. 2.3's surface-driver check, since it is what task 62 needs to know.
4. 2.4's setup-cost measurement per harness.
5. If admitted: the section filter, the runner flag, the full forked
   regeneration with its provenance.
6. 3.3's cases and the extended emitter test.
7. 2.39's corrections, the two sibling debt entries swept, row 77, `SKILLS.md`,
   section 9.

## 9. Outcome

<!-- Filled in when the measurement lands: the census table, 6.1's predictions
     scored one by one, what moved that the plan did not list, and what the task
     leaves for later - which goes to the milestone's debt register or a scope
     document, never to a code comment. -->
