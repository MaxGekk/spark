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
"""Replace some sections of a Spark benchmark results file and keep the rest as committed.

    dev/varka_bench_sections.py list FILE
    dev/varka_bench_sections.py splice OLD NEW --sections "title one,title two" [--out FILE]
    dev/varka_bench_sections.py restore --sections "title one" [FILE...]
    dev/varka_bench_sections.py --selftest

A results file is a run of sections, each opened by a rule of `=` characters, a title line (the
name the benchmark gave `runBenchmark`) and a second rule. A runner regeneration rewrites every
section of a class's file on whichever CPU the pool assigns, so a pull request that adds one
section replaces all the others' numbers with that machine's (VARKA-256, `m8/SCOPE.md` item 72;
it happened on 30 September 2026 and was spliced back by hand). This is the splice.

`splice` writes OLD with the named sections replaced by NEW's, in OLD's order; a named section
OLD lacks is appended; every other section is OLD's bytes, unchanged. `restore` does it for the
files a run just regenerated: for each results file that differs from HEAD (or the files named) it
takes HEAD's version as OLD and the working-tree file as NEW, and writes the result back, which is
what the benchmark workflow runs after the benchmark when a `sections` input is set. A file with
no committed version is left as it is. A named section the new file does not have is an error
that lists the titles it has, since a misspelt title would otherwise silently keep the old
numbers.

Titles are matched exactly, ignoring surrounding space. The text before the first rule (the
provenance header of a file that has one) belongs to no section and is always OLD's.
"""

import argparse
import re
import subprocess
import sys

RULE = re.compile(r"^={20,}\s*$")


def split_sections(text):
    """[(title, text)] in file order. The first entry's title is "" for the preamble, which is
    present only if there is text before the first rule."""
    lines = text.splitlines(keepends=True)
    starts = []
    i = 0
    while i + 2 < len(lines):
        if RULE.match(lines[i]) and not RULE.match(lines[i + 1]) and RULE.match(lines[i + 2]):
            starts.append((i, lines[i + 1].strip()))
            i += 3
        else:
            i += 1
    out = []
    if not starts:
        return [("", text)] if text else []
    if starts[0][0] > 0:
        out.append(("", "".join(lines[: starts[0][0]])))
    for k, (at, title) in enumerate(starts):
        end = starts[k + 1][0] if k + 1 < len(starts) else len(lines)
        out.append((title, "".join(lines[at:end])))
    return out


def parse_titles(spec):
    """The titles a `--sections` value names: separated by `|` (or newlines), since titles hold
    commas (126 of the 480 committed ones) and none holds a `|`. Blank entries and repeats go."""
    titles = []
    for part in spec.replace("\n", "|").split("|"):
        part = part.strip()
        if part and part not in titles:
            titles.append(part)
    return titles


def splice(old, new, titles):
    """OLD with the sections `titles` taken from NEW; see the module doc.

    A title may name several sections of a file (a loop that calls `runBenchmark` with one name):
    NEW's sections of that title, in order, take the place of OLD's first one, and OLD's others
    go. A title OLD lacks is appended.
    """
    new_by = {}
    for title, body in split_sections(new):
        if title:
            new_by.setdefault(title, []).append(body)
    missing = [t for t in titles if t not in new_by]
    if missing:
        raise SystemExit(f"no such section in the new file: {missing}; it has {list(new_by)}")
    out = []
    placed = set()
    for title, body in split_sections(old):
        if title in titles:
            if title not in placed:
                out.extend(new_by[title])
                placed.add(title)
        else:
            out.append(body)
    for title in titles:
        if title not in placed:
            out.extend(new_by[title])
            placed.add(title)
    return "".join(out)


def committed(path):
    r = subprocess.run(["git", "show", f"HEAD:{path}"], capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


def changed_results():
    r = subprocess.run(
        ["git", "diff", "--name-only", "--", "*benchmarks/*-results.txt"],
        check=True,
        capture_output=True,
        text=True,
    )
    return [p for p in r.stdout.split() if p]


def restore(titles, files):
    """Splices each regenerated file against HEAD's. A file that holds none of the titles (the
    128-bit companion, another class's file under a wildcard) goes back to HEAD whole; a file that
    holds some and not all is an error, as is a run in which no file holds any."""
    held = 0
    for path in files or changed_results():
        old = committed(path)
        if old is None:
            print(f"{path}: no committed version, left as it is")
            continue
        with open(path, encoding="utf-8") as f:
            new = f.read()
        have = {t for t, _ in split_sections(new) if t}
        present = [t for t in titles if t in have]
        if not present:
            print(f"{path}: holds none of the sections; restored to HEAD")
            result = old
        elif len(present) < len(titles):
            lacking = [t for t in titles if t not in have]
            raise SystemExit(f"{path}: lacks the sections {lacking}; it has {sorted(have)}")
        else:
            result = splice(old, new, titles)
            held += 1
            print(f"{path}: sections {titles} taken from the run, the rest as committed")
        with open(path, "w", encoding="utf-8") as f:
            f.write(result)
    if held == 0:
        raise SystemExit(f"no regenerated file holds the sections {titles}")


def selftest():
    def sec(title, body):
        rule = "=" * 96 + "\n"
        return f"{rule}{title}\n{rule}\n{body}\n"

    a1, b1, c1 = sec("a", "old a\n"), sec("b", "old b\n"), sec("c", "old c\n")
    a2, b2, d2 = sec("a", "new a\n"), sec("b", "new b\n"), sec("d", "new d\n")
    old = "preamble\n" + a1 + b1 + c1
    new = a2 + b2 + d2
    assert [t for t, _ in split_sections(old)] == ["", "a", "b", "c"]
    # Only b replaced: a and c are old's bytes, in old's order, the preamble kept.
    assert splice(old, new, ["b"]) == "preamble\n" + a1 + b2 + c1
    # Two named, one of them new to old: appended last.
    assert splice(old, new, ["a", "d"]) == "preamble\n" + a2 + b1 + c1 + d2
    # Nothing named: old, byte for byte.
    assert splice(old, new, []) == old
    # A name the new file lacks is an error that names what it has.
    try:
        splice(old, new, ["c"])
        raise AssertionError("expected a refusal")
    except SystemExit as e:
        assert "['a', 'b', 'd']" in str(e), e
    # A file without rules is one preamble and nothing can be named in it.
    assert split_sections("plain\n") == [("", "plain\n")]
    # The rules must be a rule, a title, a rule: a table's dashes do not open a section.
    assert [t for t, _ in split_sections(sec("x", "-----\nrow\n"))] == ["x"]
    # Titles with commas are named by `|`; repeats and blanks go.
    assert parse_titles("x, y|z|| z |") == ["x, y", "z"]
    # A title that names several sections: the new ones, in order, take the first old one's place.
    old2 = a1 + sec("loop", "old 1\n") + b1 + sec("loop", "old 2\n")
    new2 = sec("loop", "new 1\n") + sec("loop", "new 2\n") + sec("loop", "new 3\n")
    assert splice(old2, new2, ["loop"]) == (
        a1 + sec("loop", "new 1\n") + sec("loop", "new 2\n") + sec("loop", "new 3\n") + b1
    )
    # Naming a section twice appends it once.
    assert splice(old, new, ["d", "d"]) == old + d2
    print("varka_bench_sections: selftest passed")


def main():
    if "--selftest" in sys.argv:
        selftest()
        return
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("list")
    p.add_argument("file")
    p = sub.add_parser("splice")
    p.add_argument("old")
    p.add_argument("new")
    p.add_argument("--sections", required=True)
    p.add_argument("--out")
    p = sub.add_parser("restore")
    p.add_argument("--sections", required=True)
    p.add_argument("files", nargs="*")
    args = ap.parse_args()
    titles = parse_titles(getattr(args, "sections", ""))
    if args.cmd == "list":
        with open(args.file, encoding="utf-8") as f:
            for title, _ in split_sections(f.read()):
                if title:
                    print(title)
    elif args.cmd == "splice":
        with open(args.old, encoding="utf-8") as f:
            old = f.read()
        with open(args.new, encoding="utf-8") as f:
            new = f.read()
        result = splice(old, new, titles)
        if args.out:
            with open(args.out, "w", encoding="utf-8") as f:
                f.write(result)
        else:
            sys.stdout.write(result)
    else:
        restore(titles, args.files)


if __name__ == "__main__":
    main()
