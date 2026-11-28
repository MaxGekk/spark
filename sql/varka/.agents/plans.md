# Plans and lessons

## Plans are records, not scratch space

`sql/varka/plans/` holds one folder per milestone, `m<n>/`, with the milestone's own documents and
one file per task named by the task's id, `m<n>/VARKA-<id>.md`; a task's plan lives in the folder of
the milestone whose table holds its row (a row moved to the scope catalogue goes with it). They are
written *as the work happens*: a task file states what was planned, then what was actually built,
what was measured and where the plan turned out wrong. Predictions written before a measurement stay
in the file after it, scored honestly - the project keeps count. Do not retro-edit a task file to
look prescient; add the correction and say what it corrects. A cost prediction - a test's or a
suite's added time, a check's overhead - reads the calibration table first
(`dev/varka_calibration.py` prints it; `sql/varka/skills/testing-and-debugging.md` says how it is
kept) and is stated at the ratio the table has observed unless the plan says why not; once scored,
it joins the table as a file of its own under `sql/varka/skills/calibration/`, which
`dev/varka_calibration.py --add-row` writes from the row.

A milestone's file is named for the stage it is in. While it is a scope
catalogue - numbered items with design input, no task numbers assigned yet - it
is `m<n>/SCOPE.md`; when it becomes a task plan with a numbered task
table it is `m<n>/PLAN.md`. Milestone 3's file says in its own opening
paragraph that it "is no longer the scope document it opened as", which is
exactly the transition the two names make visible from the outside.

A milestone's plan may only be renamed or renumbered if every inbound reference
is updated with it. Other plans cite sections and items by number.

A lesson that will outlive the task that learned it - especially a negative
result, which is what stops the next person re-litigating a settled question -
goes in `sql/varka/skills/`, one file per topic, as a new `##` section in the
file it belongs to. `SKILLS.md` at the repository root is the index over them
and is generated: run `dev/varka_toc.py SKILLS.md --from sql/varka/skills`
after adding one, which the pre-commit hook checks. The split is for retrieval -
the record is two hundred kilobytes and nothing needs all of it to answer one
question - so put the lesson where a reader looking for that topic would find
it, rather than opening a file per lesson.
