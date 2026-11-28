# Working in `sql/varka`

Instructions for this directory and the Varka code it governs: the engine module
here, the emitter and IR in `sql/catalyst/.../codegen/varka/`, the exec nodes and
rule in `sql/core/.../execution/Varka*`, and `docs/sql-varka.md`.

To find where something lives (a directory, a class, a generated file, a tool, a decline
reason) or which class does which step of a query: read `MAP.md` in this directory.

## Starting a row

1. Find the row: `grep -n '^| <n> | ' sql/varka/plans/m*/PLAN.md`. `plans/README.md` names the
   milestone in flight; a row of the next milestone's plan is not built before the owner's word.
2. Read the row whole: its state in bold, the rows it waits on, and its **Done when**. If a row
   it needs is not Done, stop and report. An expression's row: step 0 of
   `ADDING_AN_EXPRESSION.md`.
3. If the row has a plan file (`ls sql/varka/plans/m*/VARKA-<n>.md`), go to the next section.
4. Run `dev/varka_task_new.sh <n> "<title>"` from the main checkout: the worktree, the branch and
   the plan file from the template, with the hooks installed (`plans/README.md`, "A task's plan
   file").
5. Before writing the plan: read `.agents/plans.md`. Fill in its admission check (section 2 of the
   template); the first commit is the plan.
6. Build. Before each commit: read `.agents/checks.md`. Before naming a branch, writing a title,
   pushing or merging: read `.agents/pull-requests.md`.
7. When the work lands, the row opens with its state in bold (`**Done**`, `**Done in part**`,
   `**Started**`; the list is in the header of `dev/varka_plan_lint.py`) and says what was built
   and measured, and the plan's section 9 says the same; `dev/varka_plan_lint.py --offline`
   checks the row.

## A row that already has a plan

`dev/varka_task_new.sh` exits 1 with "this task has a plan", or "branch ... already exists".

1. Read the plan file whole: its sequencing (section 8) names the parts (`360a`, `360b`, ...)
   and the order they merge in, and its outcome (section 9) says which have landed.
2. Check what master already holds: `git log origin/master --oneline --grep '\[VARKA-<n>\]'`.
3. Check that no branch is in flight for the part: `git branch --list 'varka-<n>*'` and
   `dev/varka_worktree.sh list`. A branch someone else holds is theirs; ask before taking it.
4. Make the worktree and the branch by hand from `origin/master`, named as
   `.agents/pull-requests.md` says ("The branch"), then run `dev/varka_precommit.sh
   --install-hook` in the new worktree.
5. Continue the plan file that exists; do not start a second one. From here, steps 5 to 7 of
   "Starting a row" apply.

## A red option matrix

1. The words. A *configuration* is the emit defaults with one option changed, written
   `name=value` (`cse=false`), from `VarkaEmitOption.TABLE`; the defaults are the configuration
   `defaults`, and a platform configuration is named `isa=...`. `dev/varka_matrix.sh --list`
   prints them. A *unit* is one runner JVM, written `configuration|module|suite,suite,...`. A
   *bin* is one CI job, named "Option matrix: <bin>" (`b021`), that runs several units, two at a
   time. The definitions are in the header of `dev/varka_matrix_bins.py`.
2. Find the failing unit. The "Option matrix: report" job's summary lists each configuration's
   result. The red bin's artifact `varka-option-matrix-<bin>`
   (`gh run download <run id> -R <owner>/<repo> -n varka-option-matrix-<bin>`) holds
   `<bin>-failed/units.txt`, one line `exit <status>: <configuration>|<module>|<suites>` for each
   unit that failed.
3. Read the message: in the same artifact, the files `run.log` and `junit/*.xml` under
   `target/varka-units/<bin>/<bin>-u<k>/runs/<configuration>/<module>-<bin>-<k>/`. A platform
   unit: read the section "A platform bin runs the defaults under a fixed instruction set" of
   `sql/varka/skills/testing-and-debugging.md` first.
4. Rerun the one test locally, from the worktree's root:
   `dev/varka_matrix.sh --module <module> --config <configuration> --suites <Suite> --test
   "<text of the test's name>"`. It builds first and runs the defaults beside the configuration;
   its output is `target/varka-matrix/runs-test/<configuration>/<module>[-<k>]/run.log`, its JUnit
   XML in `junit/` there. CI's jobs also pass `--jvm-arg -Dvarka.series.size=4`. Without
   `--test` it runs the whole unit, under `target/varka-matrix/runs/`.
5. Before changing the test or the code: read `.agents/option-matrix.md`, which says when a test
   starts from `VarkaMatrix.base`, takes `PinsDefaults` or `C2Code`, or gets a reasoned line in
   `sql/varka/matrix/skips.tsv`. A skip is only for an answer a configuration breaks by design;
   a wrong answer is a bug to fix.
6. Done is the unit green locally under that configuration and the matrix rerun green on the
   branch.

## Read before acting

Each line names when to read a file; the file holds the rules for that case.

- Before writing or editing a plan file, a milestone's `PLAN.md` or `SCOPE.md`, or a lesson
  under `skills/`: read `.agents/plans.md`.
- Before making a performance claim, regenerating a benchmark, or quoting a number in a
  document: read `.agents/measurements.md`.
- Before committing, and before pushing a change to what the emitter writes: read
  `.agents/checks.md` (the pre-commit hook and the AVX2 recheck).
- Before writing or changing a test the option matrix runs, adding a line to
  `sql/varka/matrix/skips.tsv`, or adding an emit option: read `.agents/option-matrix.md`.
- Before changing what the compiler or the emitter writes, a bounded lowering, a buffer a kernel
  reads, or the fallback; or to ask what a projection compiles to: read `.agents/kernels.md`.
- Before writing or touching Java, writing a suite or a benchmark, or passing a Guava,
  incubator-module or Class-File API type across a module or into Scala: read `.agents/java.md`.
- Before citing or adding a paper under `papers/`: read `.agents/papers.md`.
- Before merging, or when master's run is red: read `.agents/master-green.md`.
- Before you look for, run or add a `dev/varka*` tool: read `.agents/tools.md`, the index of
  them all. The scripts that gate, push and watch a pull request are in `dev/varka_ops/`; read
  its `INDEX.md` and `AGENTS.md` first. Worktrees are listed and the merged ones removed with
  `dev/varka_worktree.sh list|gc`.

## What the engine covers, without reading the compiler

`coverage.json` in this directory lists every expression the compiler fuses, with
the Catalyst expression class each one resolves to, and `docs/sql-varka.md` carries
the same rows as a table. Both are generated by `VarkaCoverageSuite` and neither can
drift: the suite compiles every row through the real compiler, fails if an expression
class the compiler matches on is documented nowhere, and fails if either file differs
from what it renders. So "is this expression covered" is a lookup in `coverage.json`
rather than a read of the compiler's families (`VarkaNodeCompiler` and the
`Varka*Compiler` classes it chains, each with its own `switch`), and the answer is as
current as the last build.

The generated files every type row rewrites (`coverage.json`, `emitted_bytes.json`,
`width_audit.json`, `types.tsv`, the census and cast tables, the docs blocks) are never merged
by hand. `sql/varka/.gitattributes` gives them a merge driver, `dev/varka_generated_merge.py`,
which merges them per row and writes what it cannot compute (`option_arms`, the docs counts)
from one side, listed as owed; after the merge, `dev/varka_regen_generated.sh` runs the owed
regenerations in order, and CI's checks fail a stale file with its command (`VARKA-546.md`).

Adding an expression is the most common change here and has its own recipe:
`ADDING_AN_EXPRESSION.md`, ten numbered steps each ending in a command whose result
says whether that step is done. Step 7 is the coverage table, and it cannot be
skipped quietly - the suite fails until the new expression is documented.

Adding a type is the milestone's own change and has its own path:
`ADDING_A_TYPE.md`, fifteen numbered steps from the lane's facts to the matrix run, each ending in
a command whose result says whether the step is done. Its closing section says which sites the
compiler checks and which it does not; `dev/varka_new_lane.py --check <LANE>` walks the second kind
and prints each one a lane is missing.

Adding an emit option (a `VarkaEmitOptions` component) has its checklist in `ADDING_AN_OPTION.md`:
the code edits in order, the hand-kept lists, the regeneration, and a search that shows nothing
was missed.

## Open work is recorded, never left as a marker

**The Varka code carries no `TODO` or `FIXME` comments, and should not start.**
A marker in code has no owner, no reason and no paper trail; six months later
nobody can tell whether it is a plan, a doubt or a leftover. Every piece of known
but unfinished work lives in a plan file instead, where it carries the reason it
was deferred and what has to be true before it starts:

* **Work that belongs to a task in flight** goes in that task's plan file
  (`plans/m<n>/VARKA-<id>.md`), under "What is deliberately unchanged" or its outcome.
* **Work with no owner yet - the "we should really..." kind** goes in the **debt register** of
  the milestone in flight, which `plans/README.md` names (`plans/m2/PLAN.md` section 8 is the
  worked example, written during the task-11 audit). One bullet per debt: what it
  is, why it is a debt, and what closing it would take. A debt with a measurement
  attached is worth ten with an adjective.
* **Work for later milestones** goes in the furthest-out scope document -
  currently `plans/m10/SCOPE.md` - which keeps a scope catalogue for
  exactly this. A milestone's catalogue moves forward when its own plan
  becomes a task plan.

When a debt is swept, the register entry is not deleted - it is rewritten in the
past tense with the task that swept it and what the sweep found. The register is
a record, not a queue.

If you are about to type `// TODO`, put it in one of those three places and
reference it from the code only if the code would be confusing without the
pointer.

**A comment describes the code as it is.** What the code used to do, which task changed it and
when are history, and history is kept in the plans and in git; a comment that says "used to", "the
old" or "since VARKA-<n>" makes the next reader subtract it to find the code. A task id stays as a
pointer to the record ("the prediction bounds groups (VARKA-254)"), a regression test states the
failure it guards against rather than the day it happened, and a reason that names a measured fact
keeps the fact. `dev/varka_precommit.sh` flags `used to`, `the old` and `since VARKA-<n>` on the
lines a commit adds to Varka's code.

## Tasks are called by their id

**Tasks are called by their id, `VARKA-<n>`**, in plans, skills, code comments, test titles,
benchmark labels and pull request titles (their form: `.agents/pull-requests.md`); the number is the
one the task tables have always used, and "task 128" is how the record wrote it before 4 October
2026, when the plans moved into milestone folders and every reference was rewritten.

## Processes: started, waited on and stopped by PID, never by pattern

Several agents and long jobs share this machine at once: test JVMs of other worktrees, night
loads, benchmarks. So:

* **Never kill by name pattern** - no `pkill`, `killall`, `kill $(pgrep ...)` or
  `pgrep ... | xargs kill`. A pattern matches whole command lines, and every JVM's flags hold
  common words: on 13 October 2026 `pkill -f` with a pattern holding `error` stopped three test
  JVMs of other worktrees an hour into their runs. Kill only a PID recorded when the process
  started (`pid=$!` right after `&`), and a `setsid` job by its group (`kill -- -"$pid"`).
* **Never wait in a `pgrep` loop.** It matches its own shell and never ends, or matches another
  job and ends too early. Start a job you want to hear the end of as the agent tool's background
  command, which reports its exit; or use `dev/varka_bg.sh start NAME CMD...`, then
  `wait NAME` and `stop NAME`, which keep the PID, its start time and the exit status under
  `target/bg/`. A script that runs long steps writes each step's exit status to a file, as
  `dev/varka_matrix.sh` does, and a waiter reads that file.
* When you hand a long job to another agent, say this in the brief.
* **Never `git stash`.** The stash list is one per repository, shared by every worktree, so a stash
  can be popped by another agent's work, or left behind under a name nobody owns. To set changes
  aside, commit them on your branch, or save `git diff > <file>` in your scratch area.
* **Write only inside your own worktree and scratch area.** Another agent's worktree, the main
  checkout and other agents' `target/` directories are theirs.
* **Name the files you will edit** in your first progress message. Two tasks that both edit one
  file run one after the other, not side by side: on 13 October 2026 two agents edited
  `VarkaKernelChecks` at once, and the second to merge had to rebuild the first's changes inside
  its own. Whoever launches agents in parallel lists, in each brief, the files that agent owns.

## Pull requests: reviewed by their author before they open

* **Before naming a branch, writing a pull request's title, pushing or merging: read
  `.agents/pull-requests.md`** (the remotes, branch names, title forms, the gate, who merges).
* **Review your own diff before opening the pull request**, with the review the owner runs
  (`/code-review high` on the diff), fix every finding you verify, and say in the description
  what the review found and what you changed. On 13 October 2026 six pull requests were reviewed
  after their CI, and every one had seven to ten findings - a suite that could overwrite Spark's
  golden files, a circular fault check, a benchmark that split shapes unlike the compiler, a tier
  witness that dropped traps - each costing a second CI round and a second agent run. The owner's
  review after CI is then the second look, not the first.
* **A pull request stays under 1000 changed lines** of hand-written files - source, plans and
  notes, and hand-curated data such as `census_curated.txt` - added plus removed against its merge
  base. Generated and results files, the papers under `sql/varka/papers/` and the `READING_*.md`
  notes do not count. `dev/varka_msg_check.py pr` prints the count and fails at the cap, and so
  does the Build's `lint` job (VARKA-554).
* **A benchmark port merges with its pair** (`dev/varka_bench_pair.sh` against the merge base), or
  its description says why not and names the row that owes the pair.
* **A change that adds test time says so**: the gate's and the nightly's affected steps, their
  time before and after, in the description, read against the committed budget
  `sql/varka/ci/step-budget.tsv` (`dev/varka_ci_times.py gate <dir>` for a laptop gate or
  nightly; a Build's check carries the same table for its CI steps).

## Java first

* **The project is migrating from Scala to modern Java (25+): prefer Java for new
  Varka code wherever possible.** Records, sealed interfaces and pattern-matching
  switches are already the house style in the IR and emitter; match it. Reach for
  Scala only at surfaces that force it - `SparkPlan` subclasses, Catalyst rules,
  ScalaTest suites. Matching over Catalyst expressions is not one of them: a
  compiler family matches in a Java `switch` over type patterns with `when`
  guards, as well as in a Scala `match`, and what a port costs is at its boundary
  with Scala code (`VARKA-175.md` 5). Existing Scala is not rewritten as a
  side effect of another task; the migration is its own work (milestone 3,
  VARKA-23).
