# VARKA-384: The calibration table as one file per entry

## 1. Where this came from

Milestone 7's debt register (`m7/PLAN.md` section 10, the first entry under "In CI and
tooling"), opened on 16 October 2026. The calibration table in
`skills/testing-and-debugging.md` gains a line from most tasks, since `sql/varka/AGENTS.md`'s
cost-prediction rule asks each scored prediction to join it. Every one of those lines was appended
after the same last row, so each merge conflicted with every open branch that had added one, and
each resolution cost a push and a CI restart: six or so per merge on 16 October 2026, the day six
of the table's fifteen lines landed (VARKA-244, 317, 378, 280, 245 and 257, by `git blame` of the
table). Row 384 asks for one file per entry, the table generated from them, the pre-commit check
failing when the two differ, and done when two branches that each add an entry merge without a
conflict, the generated table equals today's lines, and the cost-prediction rule names the files.

## 2. The admission check, done

Two checks, run on 16 October 2026 before the design was fixed.

1. **A committed generated table still conflicts.** In a scratch clone, two branches off master
   each added an entry file and inserted their row after today's last one in
   `testing-and-debugging.md`, as a regenerated table block would. `git merge-tree --write-tree a
   b` exited 1: a conflict, because both branches insert at the same point, which is today's
   failure moved into the generated block. The repository squash-merges through GitHub, which does
   not run `.gitattributes` merge drivers, so `merge=union` or a custom driver would make the
   merge clean on a laptop and leave it conflicting on the pull request. This check rejected the
   design the row describes, a table committed between markers and checked against the files:
   it meets "generated" and "checked" and fails the first condition of done. So the files are
   the record and the table is printed from them on demand (3.1); nothing committed holds the
   rows twice.
2. **Today's order is not derivable from dates.** `git blame` of the table gives each line the day
   it joined: the rows are in that order except VARKA-138b (14 October), which sits second, beside
   VARKA-138a (13 October) and before the two VARKA-274a lines (13 October), and the four lines of
   14 October are in commit-time order, not task order. So a key of date then task reorders
   today's table, and the byte-compare of done would fail. The fifteen entries carry their
   position instead (3.1).

## 3. The design

### 3.1 One file per entry, and a tool that prints the table

* **Where.** `sql/varka/skills/calibration/`, one Markdown file per entry. `dev/varka_toc.py`
  indexes only the `.md` files directly under `skills/`, so the directory adds nothing to
  `SKILLS.md`.
* **The file.** A Markdown list, one field per item in a fixed order - `task`, `added`, `order`
  (only on the fifteen migrated entries), `predicted`, `measured`, `ratio`, `note` - with a long
  value wrapped at 100 columns onto lines indented by two spaces, so it renders as a list on
  GitHub and reads in a terminal. The five table cells are the five fields of the same names.
* **The name.** The task cell with every run of characters other than letters, digits and dots
  made one hyphen: `VARKA-274a (6.1 #8)` is `VARKA-274a-6.1-8.md`. The cell starts with the task
  id, so two tasks never pick the same name; several entries of one task differ by the prediction
  they score, and a second entry for one prediction by what its cell adds (`VARKA-274a (6.1 #8,
  rerun)` is `VARKA-274a-6.1-8-rerun.md`).
* **The order.** Entries are listed in the order they were added: by `added`, the day the entry
  joined the table, then by file name. The fifteen entries kept by hand carry `order`, their
  position in the old table, and come first; no new entry may set it, and the tool rejects an
  `order` outside 1 to 15 or a repeated one. Two branches that each add an entry change no common
  line, and the printed order after both merge does not depend on which merged first.
* **The tool.** `dev/varka_calibration.py` prints the table as Markdown, the header and one row
  per entry, byte for byte the old table's form. `--check` reports a malformed file: a field
  missing, unknown or repeated, a file not named after its task, a `|` or a stray space in a value
  (either would change the printed row), a bad date, an `order` it may not carry, and a file not
  in the canonical layout. `--add-row "<row>"` writes the file for a pasted table row dated today
  (or `--added`), which is the migration path for open branches; `--normalize` puts hand-edited
  files back into the layout. Exit status is the number of findings, as `dev/varka_toc.py`'s is.
* **The pre-commit check.** `dev/varka_precommit.sh` runs `--check` when an entry file or the tool
  is staged. The quote check (`dev/varka_quote_check.py`) gains the directory, so the numbers the
  entries quote stay checked after they leave `testing-and-debugging.md`.
* **The skill and the rule.** The section in `testing-and-debugging.md` keeps its heading, which
  `SKILLS.md` and about twenty plans link to, its introduction and its "what the table says so
  far" paragraph; the rows are replaced by a paragraph naming the command, the directory, the
  naming rule and how to add an entry. `sql/varka/AGENTS.md`'s cost-prediction rule names the
  command and the directory.

### 3.2 What is deliberately unchanged

The fifteen entries' text, cell for cell. The plans that cite a line by its task ("VARKA-243's
line") stay valid, since each line is a file named after that task; they are records and are not
rewritten. The open plans that list `testing-and-debugging.md` for "the calibration line" (for
one, `m8/VARKA-383.md`) follow the migration note in 3.4 when they land.

### 3.3 Registered op counts

None: the task changes tooling and documents, no emitted code.

### 3.4 The migration note for open branches

A branch that added a line to the old table conflicts with this change in
`testing-and-debugging.md`. To resolve it after merging master:

1. Resolve the section to master's text, dropping the branch's row. In a merge of master into the
   branch, master is `--theirs`: when the branch changed nothing else in the file,
   `git checkout --theirs sql/varka/skills/testing-and-debugging.md` does it; otherwise edit the
   conflict by hand and keep master's side of the table's section. Copy the row first.
2. Turn the row into a file: `dev/varka_calibration.py --add-row '<the row, with its leading and
   trailing |>' --added <the day the prediction was scored>`. Quote the row in single quotes, since
   rows hold backticks and `$`.
3. `dev/varka_calibration.py --check`, then `dev/varka_calibration.py | tail -3` to see the row
   at the end of the table, and commit the new file under `sql/varka/skills/calibration/`.

## 4. Files

| file | what |
|---|---|
| `dev/varka_calibration.py` | new: prints the table, `--check`, `--add-row`, `--normalize` |
| `sql/varka/skills/calibration/*.md` | new: the fifteen entries |
| `sql/varka/skills/testing-and-debugging.md` | the rows replaced by the reading and adding paragraph |
| `sql/varka/AGENTS.md` | the cost-prediction rule names the command and directory |
| `dev/varka_precommit.sh` | runs `--check` on an entry or tool change |
| `dev/varka_quote_check.py` | the entry files join the documents it checks |
| `sql/varka/plans/m7/PLAN.md` | row 384's status |

## 5. Tests, and what each is for

* **The byte-compare.** Today's 17 table lines (header, separator, 15 rows) saved before the change
  and compared with `cmp` against the tool's output after it. Catches a lost, reordered or altered
  row.
* **The merge.** Two scratch branches off this branch each run `--add-row` for a different task;
  `git merge-tree --write-tree` exits 0, and the merged tree's printed table ends with both rows.
* **`--check`'s failure paths**, on a scratch copy of the directory: an unknown field, missing
  fields, a `|`, a double space, a bad date, a repeated `order`, a misnamed file, and `--add-row`
  refusing an existing file.
* **The checks the repository runs**: `dev/varka_precommit.sh`, `ruff check` and `ruff format
  --check` on the Python, `dev/varka_quote_check.py`, `dev/varka_toc.py --check`.

## 6. The measurement

None: no performance claim. The cost is a printed table instead of a committed one, read with a
command a planner runs once per plan.

### 6.1 Predictions, registered before the run

1. The printed table equals today's 17 lines byte for byte.
2. Two branches that each add an entry merge with `git merge-tree` exiting 0.
3. The quote check finds no new orphan with the entries in its set.

## 7. Risks

1. **A reader of `testing-and-debugging.md` no longer sees the rows inline.** The section names the
   command in its first sentence after the introduction, and the rule in `AGENTS.md` does too.
2. **A hand-written entry that renders a different row.** `--check` rejects anything outside the
   canonical layout, so a file the hook passed prints exactly its fields.
3. **Entries added on open branches by hand before they read this.** The migration note (3.4).

## 8. Sequencing

1. The plan and row 384 marked Planned.
2. The tool, the fifteen files, the skill section, the rule, the hook and the quote-check glob in
   one commit, since the files without the section would duplicate the rows and the section
   without the files would lose them.
3. Section 9 and row 384 Done.

## 9. Outcome
