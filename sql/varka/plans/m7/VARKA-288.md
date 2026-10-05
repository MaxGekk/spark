# VARKA-288: the cliff's sequel, a large CASE WHEN that splits only where it pays

*Row 288 of `m7/PLAN.md`, opened 5 October 2026 at the owner's request, after asking whether the
changes of apache/spark#59225 (SPARK-33301) would interest a broader audience and whether the
post could continue "The 8000-byte cliff in Spark SQL". Planned in the shape of the earlier posts
(`m6/VARKA-210.md`, `m6/VARKA-233.md`): the question, what the record holds, what still has to
be measured with its predictions, the outline, and done-when.*

## 1. The question

"The 8000-byte cliff in Spark SQL" (VARKA-210, published 26 September 2026) left one thread open.
Its section 5 showed a large `CASE WHEN` inside a whole-stage codegen stage stepping off the cliff,
9433 bytes at 100 branches and about 6 times slower, and its section 6 listed SPARK-33301 as "in
review". Its workaround, `spark.sql.codegen.hugeMethodLimit=8000`, turns the step into a slope by
taking the stage out of whole-stage codegen.

apache/spark#59225 closes that thread, and how it closes it is the post: splitting is not free below
the line, so the stage decides per method from its own bytecode. That answers the question "Under
8000 bytes by construction" (VARKA-181, milestone 6's closing post) raised from the other side:
Varka knows each method's size as it emits it, and Spark, which cannot predict bytecode from Java
source, compiles a trial and reads the sizes off the class.

The post is for the cliff post's readers, people who run Spark and JVM developers. It mentions
Varka only through the link to "Under 8000 bytes by construction", as the earlier Spark posts
did.

## 2. What the record already holds

All of these are committed on #59225's branch (`SPARK-33301-redesign` on MaxGekk/spark) and land on
apache/spark master with it:

- `sql/core/benchmarks/CaseWhenCodegenBenchmark{,-jdk21,-jdk25}-results.txt`: rungs of 2 to 300
  branches, four CASE WHENs in one projection, a string CASE WHEN with a column in its `ELSE`,
  and a keyed aggregate, each under split as needed, not split, split always and whole-stage
  codegen off, with the largest method of each stage. The headline rows, best times on JDK 17 /
  21 / 25:

  | shape | split as needed | not split | split always | whole-stage off |
  |---|---|---|---|---|
  | 64 branches (6049 bytes) | 92 / 95 / 63 ms | 92 / 94 / 64 ms | 97 / 100 / 64 ms | 211 / 219 / 152 ms |
  | 96 branches (9057 bytes unsplit) | 120 / 122 / 82 ms | 3992 / 3717 / 2394 ms | 115 / 115 / 75 ms | 296 / 305 / 200 ms |
  | 300 branches | 249 / 227 / 162 ms | 11854 / 12083 / 7043 ms | 232 / 216 / 143 ms | 976 / 951 / 709 ms |

- The PR description's account of the gate, its cost (5-13% on one 300-branch CASE WHEN, 5-17% on
  four, 8-15% in the keyed aggregate, against splitting always) and the per-run literal case.
- The differential fuzz (96,000 queries, not part of the PR), which found one split that did not
  compile and is pinned by a test.
- The 64-branch finding that motivated the gate: splitting where the stage did not need it cost
  5-15% on the runners.

## 3. What has to be measured before the draft

### 3.1 The cliff post's workaround against the fix

The cliff post recommended `hugeMethodLimit=8000`. For a CASE WHEN inside a stage that setting
makes the stage fall back out of whole-stage codegen, so it should time close to the "whole-stage
off" column; the post has to say how the fix compares with the advice its predecessor gave.

**What.** One arm added to a run of `CaseWhenCodegenBenchmark` on the runners,
`hugeMethodLimit=8000` with the split off (the released behaviour), at 64, 96 and 300 branches
and in the keyed aggregate, on the build of #59225's merge commit.

**Predictions.**

1. At 96 and 300 branches the workaround is within 15% of the whole-stage-off arm, and split as
   needed is at least 3 times faster than it at 300 branches.
2. At 64 branches the workaround changes nothing, since the method is under 8000 bytes.

### 3.2 Why splitting always costs below the line

The post says C2 stops inlining partway through the chain of calls. That is the PR's explanation,
and the post should show it from the JIT's own output, as the earlier posts showed the cliff with
`-XX:+PrintCompilation`.

**What.** The 64-branch rung under split always and split as needed, run with
`-XX:+UnlockDiagnosticVMOptions -XX:+PrintInlining` on JDK 25 on the laptop; the inlining
decisions for the stage's `processNext` and the split methods, filed beside the results.

**Prediction.**

3. Under split always, `processNext` inlines the first split calls and the rest are refused with
   a size or depth reason (`too big`, `inlining too deep` or `NodeCountInliningCutoff`); under split
   as needed there is no call to refuse.

### 3.3 The release

Which release first ships the fix: #59225's conf is versioned 4.4.0, so if it merges to master and
`branch-4.x`, 4.4.0. Checked with `dev/pr_merge_status.py 59225` once merged.

## 4. The outline

About 1200 to 1500 words, two figures. The earlier posts ran 3300 to 5300 words; this one builds on
the cliff post instead of explaining the cliff again.

1. **Where the cliff post stopped.** Two sentences and the link: a `CASE WHEN` inside a stage
   steps off at about 96 branches, and SPARK-33301 was in review.
2. **Splitting is not free.** Splitting every large CASE WHEN fixes the cliff and costs 5-15% where
   there was no cliff; the inlining evidence of 3.2.
3. **Measure, don't predict.** The stage generates its code once with the splittable expressions
   marked, compiles it as a trial, splits only the expressions in methods past 8000 bytes, and keeps
   the split only if it lowers the bytes past the line. A stage under the line keeps exactly the
   code it had. The contrast with "Under 8000 bytes by construction" in one paragraph.
4. **The numbers.** Figure: time against branch count, 2 to 300, for not split, split always, split
   as needed and whole-stage off, from the committed results files; the table of section 2; the
   workaround of 3.1.
5. **What it still costs.** The second code generation per execution, and a literal that changes
   every run, such as `current_timestamp()`.
6. **What to do.** On the release that ships it, nothing; before it, the cliff post's workaround,
   with 3.1's number for what it gives up.

Figures in `sql/varka/plans/figures/` as `fig<n>.py`, drawn from the committed results files the
way `fig19.py` is.

## 5. Done when

1. #59225 is merged, and the post names the release that ships it (3.3).
2. 3.1 and 3.2 are measured, their files committed and their predictions scored in section 9.
3. The post is published on vecbricks.github.io, every number in it tracing to a committed file.
4. The three published posts' links to apache/spark#59069, which #59225 replaced, point to
   #59225; this can land before the rest.

## 6. Sequencing

1. The link fix in the three posts (5.4), now.
2. The measurements of 3.1 and 3.2, once #59225's review has settled, so that they run on the
   code that merges.
3. The draft, shown to the owner; the figures; publishing after the merge.

## 7. Explicitly out of this task

- The follow-ups #59225 lists (the other `splitExpressionsWithCurrentInputs` callers, a decision
  key that ignores literal values, the sizes kept in the compile cache): each is its own JIRA.
- The PR's internals that the PR description already documents: the input collector, the
  occurrence-keyed matching, the decision memo.

## 8. Risks

1. **The review changes the design again.** The draft waits for the review to settle (6.2), and
   section 3 of the post describes the mechanism, not the code.
2. **The merge slips past 4.4.0.** The post names the release from 3.3's check, not from the
   conf's version.

## 9. Outcome
