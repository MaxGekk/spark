# VARKA-252: Comments that describe the code as it is

*Scoped 3 October 2026 (milestone 7 section 2, row 252, item 74.7), refined 9 October 2026 by
`m7/READING_POLARS.md` 9; opened 17 October 2026.*

## 1. The question

Varka's comments were written as the code changed, and many of them say what the code used to do:
"since VARKA-254 the prediction bounds groups", "16 until VARKA-254", "the guard stack that used
to do the refusing is gone". A new reader has to subtract the history to find the code, and the
history is already kept where it belongs: in the task plans and in git. The Polars reading (9)
sampled 25 comments matching `used to|the old|no longer|once carried` across Varka's main
sources and found 21 that narrate history. Neither `CLAUDE.md` nor `sql/varka/CLAUDE.md` states
the rule, so nothing stops new ones.

The paper reading (`m7/READING.md` 11) found two comments that are wrong rather than dated:
`VarkaEmitterDivisionSuite` says Java's `/` throws at `Integer.MIN_VALUE / -1`, where it wraps
(JLS 15.17.2), and the javadoc of `emitConstDivide` says every form of the constant division is
exact over the whole lane, where the long-lane forms are exact only under
`ConstDivide.EXACT_DIVIDEND_BOUND`. The same section's other comment findings - bounds quoted as
if exact that are only sufficient (`YEAR_M`'s 44858, which first fails at 44894), and
`NARROW_DECOMPOSE_MAX_DAYS` past what the closed form proves - were corrected by VARKA-242:
every bound in `VarkaChrono` now says "Sufficient: ... first fails at N (`chrono_divide.smt2`)",
and `VarkaChronoLowering`'s `ChronoDivide` javadoc states the exact-or-sufficient convention.
This task checks that and does not repeat it.

## 2. The change

### 2.1 Scope

Varka's own code: every tracked `.java`, `.scala`, `.py` and `.sh` file whose path names Varka -
the emitter, IR and compiler under `sql/catalyst/.../codegen/` and `.../codegen/varka/`, the exec
nodes, rule and suites under `sql/core/.../execution/` and `sql/core/.../sql/`, the engine, bench,
demo and spike modules under `sql/varka/`, and the `dev/varka_*` tools: 465 files. Spark's own
files are not touched, nor the plans, the lesson files or the posts, which are records and narrate
on purpose.

Comments only. No identifier, string literal, test name or line of code changes, and no code
moves.

### 2.2 How a narrating comment is found

A grep over comment lines (`//`, `*`, `/*`, `#`) for the phrases that mark a contrast with a past
version: `used to`, `the old`, `previously`, `no longer`, `originally`, `once carried`,
`had been`, `since VARKA-`, `before VARKA-`, `until VARKA-`, `now that`, `the earlier`,
`at first` (227 lines); then `now` on its own (92 more lines) and dates in comments (27). Each
hit is read in its comment, and the comment is put in one of three buckets.

* **Narration**: the comment contrasts the code with an earlier version of itself. It is
  rewritten in the present tense. Three forms cover nearly all of them:
  (a) "since VARKA-n, X" becomes "X (VARKA-n)": the task id stays as the pointer to the record,
  which the house rule allows (`sql/varka/CLAUDE.md`, "Tasks are called by their id"), and the
  chronology goes;
  (b) a constant's former value ("16 until VARKA-254") is deleted, since the current value is the
  code and the recount is in the task's plan, which the pointer names;
  (c) a regression test's history ("Before VARKA-15 this constructor compiled the fallback eagerly
  and threw") becomes the invariant the test pins, with the failure it guards against stated as
  a hypothetical ("a constructor that compiled the fallback eagerly would throw here").
* **Present-tense semantics that use the words**: "so the loader no longer retains the class
  after release", "an entry its seed no longer reproduces is stale", "the methods in use now",
  "`rem` is now the day of era" (a step in a derivation). Kept: these describe what the code does
  at run time, which is the minority the Polars survey found ("no longer", 4 of 25).
* **A reason that names a measured fact or a proof**: kept, with only the chronology dropped.
  "VARKA-17 measured the merge as a loss; the parity file has read it as a win since VARKA-46"
  is a measured disagreement, and the comment is why the code is as it is.

A pointer to unfinished work ("until VARKA-28 gives the emitter a width conversion") is neither:
it says what the code does not do yet and which row owns it, and it stays.

### 2.3 The two corrections

* `VarkaEmitterDivisionSuite`, the test "a constant division by zero or by -1 is refused": the
  comment says that Java's `/` wraps at `Integer.MIN_VALUE / -1` (to `Integer.MIN_VALUE`, JLS
  15.17.2) rather than throwing, and that the emitter refuses the divisor because the lane has no
  correct answer for that input, not because the JVM would raise.
* `VarkaDivisionLowering`: the class javadoc and `emitConstDivide`'s javadoc split the claim by
  lane - the int-lane forms are exact over the whole int lane, the long-lane forms under the
  dividend bound the node states and its caller proves (`ConstDivide.EXACT_DIVIDEND_BOUND`) - and
  point at `ConstDivide`'s javadoc, which already says this correctly, rather than restating it.

### 2.4 The rule kept

* A sentence in `sql/varka/CLAUDE.md` (the real file; `AGENTS.md` is its symlink) in the section
  on open work, its sibling rule: a comment describes the code as it is; what the code used to
  do belongs in the plan and in git.
* A check in `dev/varka_precommit.sh`: a line the commit adds, in a Varka source file (Scala,
  Java, Python or shell under a Varka directory or named `Varka*`), that matches `used to` or
  `the old`, case-insensitive, is a finding. Only those two phrases: every hit of them in the
  Polars sample was history, where "no longer" was a third behaviour. `used to` after `is`,
  `are`, `be`, `been` or `being` is the instrumental use ("is used to convert") and is not
  matched. Markdown is out: plans narrate history by design, and this plan names the phrases.
  The `dev/` scripts are swept by hand but are outside the check, as they are outside the check
  for open-work markers: `is_varka` does not match `dev/`, which is also why the script's own
  pattern never trips it.
* A self-test case for each direction, in the fixture repository the self-test already builds: a
  committed `used to` under an unrelated edit is not reported, an added one is.

## 3. Predictions, registered before the run

1. Between 150 and 200 comments change, across 70 to 100 files, most of them narration of form
   (a) and the regression-test form (c); fewer than 10 hits of the phrase grep turn out to be
   present-tense semantics.
2. `sql/varka/emitted_bytes.json` is byte-identical and `VarkaEmittedBytesSuite` passes without
   regenerating it; `VarkaCoverageSuite` passes with `coverage.json` and `docs/sql-varka.md`
   unchanged. Comments reach no emitted byte and no generated file.
3. Every `+` and `-` line of the diff outside the plan, `sql/varka/CLAUDE.md` and
   `dev/varka_precommit.sh` is inside a comment.

No cost is predicted: the pre-commit check is one `grep` per staged Varka file.

## 4. Verification

* `build/sbt catalyst/Test/compile sql/Test/compile`; `dev/lint-java`; `dev/scalastyle`; ruff
  over the touched Python; `dev/varka_precommit.sh --working-tree` and `--selftest`.
* `build/sbt 'catalyst/testOnly *VarkaEmittedBytesSuite *VarkaCoverageSuite'`, and
  `git status` showing no generated file changed.
* A scan of `git diff -U0` for any changed line not inside a comment.

## 5. Outcome

## 6. Explicitly out of this task

* Comments in Spark's own files, and the narration in plans, lesson files and posts, which are
  records.
* A check for "no longer", "previously" or "since VARKA-": each has present-tense uses the check
  could not tell apart, and a check that fires on correct text teaches `--no-verify`.
