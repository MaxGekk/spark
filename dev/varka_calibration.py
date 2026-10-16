#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""The calibration table of scored cost predictions, printed from one file per entry.

  dev/varka_calibration.py                    # print the table, as Markdown
  dev/varka_calibration.py --check            # exit non-zero if an entry file is malformed
  dev/varka_calibration.py --add-row "| VARKA-n (6.1 #k) | ... |" [--added YYYY-MM-DD]
                                              # write one entry file from a table row

A plan that predicts a cost reads this table first and states its estimate at the ratio
observed (sql/varka/AGENTS.md, "Plans are records, not scratch space"); a scored prediction
adds an entry. The entries live under sql/varka/skills/calibration/, one file per entry, and
the table is printed rather than committed: when it was a table in a skill file every task
appended a row after the same last line, so every merge conflicted with every open branch
that had added one. Two new files never conflict; a committed rendering of them would, since
both branches would append its row at the same place.

An entry file is a Markdown list, one field per item in this order, a long value wrapped onto
lines indented by two spaces:

  * task: VARKA-274a (6.1 #8)
  * added: 2026-10-13
  * predicted: the tier witness adds under 5%
  * measured: +19% (int) and +25% (long) per case; +6% on the whole test
  * ratio: 3.8 to 5.0
  * note: one code-list read per call, about 3.5 ms, is most of it

`task` is the table's first cell: the task id, then the prediction it scores in parentheses.
The file is named after it - `VARKA-274a-6.1-8.md`, every run of characters other than
letters, digits and dots becoming one hyphen - so two tasks never pick the same name, and a
second entry for one prediction (a rerun, say) differs by what its cell adds:
`VARKA-274a (6.1 #8, rerun)` is `VARKA-274a-6.1-8-rerun.md`. `added` is the day the entry
joined the table. The table lists the entries in the order they were added: by `added`, then
by file name. The fifteen entries kept by hand before 17 October 2026 carry an `order` field
after `added` instead, their position in that table, because one of them sat out of date
order (VARKA-138b beside VARKA-138a); they come first, and no new entry sets it.

--check fails on a missing, unknown or repeated field, a file named other than its task, a
value with a `|` or a stray space (either would change the printed row), an `order` outside
the fifteen, anything in the directory that is not an entry file, and a file not in the
canonical layout, which --add-row writes and --normalize restores in a file edited by hand.
"""

import argparse
import datetime
import os
import re
import sys
import textwrap

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIRECTORY = os.path.join("sql", "varka", "skills", "calibration")
HEADER = "| task | predicted | measured | ratio | note |\n| --- | --- | --- | ---: | --- |\n"
FIELDS = ["task", "added", "order", "predicted", "measured", "ratio", "note"]
CELLS = ["task", "predicted", "measured", "ratio", "note"]
OPTIONAL = {"order"}
# The number of entries the hand-kept table held when it became files; only they may carry
# `order`.
LEGACY_ROWS = 15
WIDTH = 100


def show(path: str) -> str:
    """A path as the messages print it: from the repository's root when it is inside it."""
    relative = os.path.relpath(path, ROOT)
    return path if relative.startswith("..") else relative


def file_name(task: str) -> str:
    """The entry's file name, from its task cell."""
    return re.sub(r"[^A-Za-z0-9.]+", "-", task).strip("-") + ".md"


def render(entry: dict) -> str:
    """The canonical text of an entry file."""
    lines = []
    for field in FIELDS:
        if field not in entry:
            continue
        lines += textwrap.wrap(
            entry[field],
            width=WIDTH,
            initial_indent=f"* {field}: ",
            subsequent_indent="  ",
            break_long_words=False,
            break_on_hyphens=False,
        )
    return "\n".join(lines) + "\n"


def parse(text: str) -> tuple:
    """The fields of an entry file, and the problems that make it unreadable."""
    entry, problems, field = {}, [], None
    for number, line in enumerate(text.split("\n"), 1):
        if not line:
            continue
        match = re.match(r"\* ([a-z]+): (.*)$", line)
        if match:
            field, value = match.groups()
            if field not in FIELDS:
                problems.append(f"line {number}: unknown field `{field}`")
            elif field in entry:
                problems.append(f"line {number}: `{field}` repeated")
            entry[field] = value
        elif line.startswith("  ") and field is not None:
            entry[field] += " " + line.strip()
        else:
            problems.append(f"line {number}: neither `* field: value` nor a continuation")
    return entry, problems


def validate(name: str, text: str) -> list:
    """Everything wrong with one entry file, as messages; empty when it is well formed."""
    entry, problems = parse(text)
    for field in FIELDS:
        if field not in entry and field not in OPTIONAL:
            problems.append(f"no `{field}` field")
    for field, value in entry.items():
        if "|" in value:
            problems.append(f"`{field}` holds a `|`, which would split its table cell")
        if value != " ".join(value.split()) or not value:
            problems.append(f"`{field}` is empty or holds a stray space")
    if "task" in entry and name != file_name(entry["task"]):
        problems.append(f"named after its task it is {file_name(entry['task'])}")
    if "added" in entry:
        try:
            datetime.date.fromisoformat(entry["added"])
        except ValueError:
            problems.append(f"`added` is not a YYYY-MM-DD date: {entry['added']}")
    if "order" in entry and entry["order"] not in {str(n) for n in range(1, LEGACY_ROWS + 1)}:
        problems.append(f"`order` is only for the {LEGACY_ROWS} entries kept by hand before")
    if not problems and text != render(entry):
        problems.append("not in the canonical layout; run dev/varka_calibration.py --normalize")
    return problems


def load(directory: str) -> tuple:
    """The entries in table order, and every problem found, as `path: message` lines."""
    entries, findings, orders = [], [], {}
    for name in sorted(os.listdir(directory)):
        path = os.path.join(directory, name)
        if not name.endswith(".md") or not os.path.isfile(path):
            # A misnamed entry would otherwise drop out of the table without a word.
            findings.append(f"{show(path)}: not an entry file; entries are `<task>.md` files")
            continue
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
        problems = validate(name, text)
        findings += [f"{show(path)}: {p}" for p in problems]
        entry, _ = parse(text)
        entry["file"] = name
        if "order" in entry:
            if entry["order"] in orders:
                findings.append(
                    f"{show(path)}: `order` {entry['order']} also in {orders[entry['order']]}"
                )
            orders[entry["order"]] = name
        entries.append(entry)

    def key(entry: dict) -> tuple:
        if "order" in entry and entry["order"].isdigit():
            return (0, int(entry["order"]), "", "")
        return (1, 0, entry.get("added", ""), entry["file"])

    return sorted(entries, key=key), findings


def table(entries: list) -> str:
    return HEADER + "".join(
        "| " + " | ".join(e.get(c, "") for c in CELLS) + " |\n" for e in entries
    )


def add_row(directory: str, row: str, added: str) -> int:
    """Write the entry file for one table row; the migration path for a branch that added a
    line to the old table."""
    cells = [c.strip() for c in row.strip().strip("|").split("|")]
    if len(cells) != len(CELLS):
        print(f"a row has {len(CELLS)} cells ({', '.join(CELLS)}); this one has {len(cells)}")
        return 1
    entry = dict(zip(CELLS, cells))
    entry["added"] = added
    name = file_name(entry["task"])
    path = os.path.join(directory, name)
    if os.path.exists(path):
        print(f"{show(path)} already exists")
        return 1
    problems = validate(name, render(entry))
    if problems:
        print("\n".join(f"{show(path)}: {p}" for p in problems))
        return 1
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(render(entry))
    print(f"wrote {show(path)}")
    return 0


def normalize(directory: str) -> int:
    """Rewrite every readable entry file in the canonical layout."""
    for name in sorted(os.listdir(directory)):
        path = os.path.join(directory, name)
        if not name.endswith(".md") or not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
        entry, problems = parse(text)
        if not problems and render(entry) != text:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(render(entry))
            print(f"normalized {show(path)}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="report malformed entry files")
    mode.add_argument("--add-row", metavar="ROW", help="write the entry file for a table row")
    mode.add_argument("--normalize", action="store_true", help="rewrite files canonically")
    parser.add_argument("--added", default=datetime.date.today().isoformat(), help="with --add-row")
    parser.add_argument("--dir", default=os.path.join(ROOT, DIRECTORY), help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.add_row:
        return add_row(args.dir, args.add_row, args.added)
    if args.normalize:
        return normalize(args.dir)
    entries, findings = load(args.dir)
    if args.check:
        for finding in findings:
            print(finding)
        return len(findings)
    sys.stdout.write(table(entries))
    for finding in findings:
        print(finding, file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
