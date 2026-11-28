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

"""Is every Varka tool under dev/ in the tool index, does the index name only what exists, and
does every Varka Python tool answer --help?

  dev/varka_tools_lint.py              # check the index against the tree
  dev/varka_tools_lint.py --self-test  # the check's cases, over fixture names and texts

The index is `sql/varka/.agents/tools.md`: one bullet for each entry under `dev/` whose name
starts with `varka`, a file or a directory, saying what it is for, whether it starts a JVM or a
build, and its usual command line. An agent finds a tool there and not by listing `dev/`, so a
tool the index lacks is a tool nobody is sent to, and a bullet for a tool that is gone sends a
reader to a command that fails.

The entries are the first path components under `dev/` of the files git tracks and of the
untracked files it does not ignore, so a new tool counts before it is added and a build's
leftovers do not. A bullet is a line that opens with "- `dev/<name>`" (a directory may end with a
slash); a name in running text is not a bullet. The findings:

  * an entry with no bullet;
  * a bullet whose entry does not exist;
  * an entry with two bullets;
  * a bullet, read to the next bullet, heading or blank line, without exactly one of the tags the
    index defines: `[sbt]`, `[java]`, `[no JVM]`, `[data]`.

And, for every entry that is a Python file (`dev/varka*.py`), run with no input and a
deadline of HELP_SECONDS each:

  * `--help` that does not exit 0 or prints nothing;
  * `<subcommand> --help` that does not exit 0 or prints nothing, for each subcommand the
    `--help` text names: a brace group of an argparse usage line (`{plan,times}`, not an
    option's choices) and a word after the tool's own path in an example line
    (`dev/varka_msg_check.py pr ...`).

An agent learns a tool's arguments from its `--help`, so a tool that crashes on it, or reads it
as an argument and starts its work, fails the one question every reader asks first. A tool
answers `-h` or `--help` anywhere on its command line before it does anything else.

The exit status is the number of findings, 100 at most.
"""

import argparse
import concurrent.futures
import os
import re
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = "sql/varka/.agents/tools.md"
PREFIX = "varka"
TAGS = ("[sbt]", "[java]", "[no JVM]", "[data]")
BULLET_RE = re.compile(r"^- `dev/(" + PREFIX + r"[^`/]*)/?`")
# A tool's --help is a print: seconds enough for the slowest import, short of a tool's real work.
HELP_SECONDS = 30
BRACE_RE = re.compile(r"(\S*)\s\{([A-Za-z][\w,-]*)\}")


def tracked_entries(root):
    """The names directly under dev/ that start with the prefix: tracked, or not ignored."""
    pattern = "dev/" + PREFIX + "*"
    out = subprocess.run(
        [
            "git",
            "-C",
            root,
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
            pattern,
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    names = {path.split("/")[1] for path in out.split("\0") if path}
    # A file deleted in the working tree and not yet staged is still in git's index.
    return sorted(n for n in names if os.path.lexists(os.path.join(root, "dev", n)))


def bullets(text):
    """Each bullet of the index as (name, line number, the bullet's text)."""
    found = []
    lines = text.split("\n")
    for i, line in enumerate(lines):
        m = BULLET_RE.match(line)
        if not m:
            continue
        body = [line]
        for rest in lines[i + 1 :]:
            if not rest.strip() or rest.startswith(("- ", "#")):
                break
            body.append(rest)
        found.append((m.group(1), i + 1, "\n".join(body)))
    return found


def check(entries, text, index=INDEX):
    """The findings for the entries under dev/ against the index's text."""
    findings = []
    seen = {}
    for name, lineno, body in bullets(text):
        if name in seen:
            findings.append(
                f"{index}:{lineno}: dev/{name} has a second bullet (the first is at line "
                f"{seen[name]}): keep one"
            )
            continue
        seen[name] = lineno
        if name not in entries:
            findings.append(
                f"{index}:{lineno}: the index names dev/{name}, which does not exist: remove "
                "the bullet, or rename it with the tool"
            )
        tags = [t for t in TAGS if t in body]
        if len(tags) != 1:
            findings.append(
                f"{index}:{lineno}: the bullet of dev/{name} carries {len(tags)} of the tags "
                f"{', '.join(TAGS)}: give it exactly one"
            )
    for name in entries:
        if name not in seen:
            findings.append(
                f'dev/{name} is not in {index}: add a bullet "- `dev/{name}`: ..." to the '
                "section it belongs to, with what it is for, a tag and its usual command line"
            )
    return findings


def subcommands(name, text):
    """The subcommands a tool's --help text names, in the order it names them."""
    if text.lstrip().startswith("usage:"):
        # argparse's usage: the first brace group that is not an option's choices (`--mode
        # {a,b}`) is the subcommand; a later one is an argument of the subcommand.
        for before, group in BRACE_RE.findall(text.lstrip().split("\n\n")[0]):
            option = before.lstrip("[")
            if not option.startswith("-") or option.endswith("]"):
                return group.split(",")
        return []
    example = re.compile(r"dev/" + re.escape(name) + r" ([a-z][a-z0-9_-]*)(?=\s|$)", re.M)
    return list(dict.fromkeys(example.findall(text)))


def answers(path, args):
    """None when `path args --help` exits 0 and prints, else why not."""
    shown = " ".join([f"dev/{os.path.basename(path)}", *args, "--help"])
    try:
        run = subprocess.run(
            [sys.executable, path, *args, "--help"],
            cwd=os.path.dirname(os.path.dirname(path)),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=HELP_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return f"{shown}: still running after {HELP_SECONDS} s: answer --help before any work"
    if run.returncode != 0:
        last = (run.stderr.strip() or run.stdout.strip()).splitlines()[-1:] or ["no output"]
        return f"{shown}: exit {run.returncode} ({last[0][:120]}): print the usage and exit 0"
    if not (run.stdout.strip() or run.stderr.strip()):
        return f"{shown}: exit 0 and no output: print the usage"
    return None


def help_findings(root, entries):
    """The findings of --help over the Python entries, each then over its subcommands."""

    def one(name):
        path = os.path.join(root, "dev", name)
        first = answers(path, [])
        if first:
            return [first]
        text = subprocess.run(
            [sys.executable, path, "--help"],
            cwd=root,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=HELP_SECONDS,
        )
        words = subcommands(name[:-3] + ".py", text.stdout + "\n" + text.stderr)
        return [f for f in (answers(path, [w]) for w in words) if f]

    tools = [n for n in entries if n.endswith(".py")]
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as pool:
        return [f for found in pool.map(one, tools) for f in found]


def self_test():
    failures = []

    def case(name, entries, text, *expected):
        got = check(entries, text)
        ok = len(got) == len(expected) and all(e in g for e, g in zip(expected, got))
        if not ok:
            failures.append(f"{name}: expected {list(expected)}, got {got}")

    a = "- `dev/varka_a.sh`: runs a. `[sbt]` `dev/varka_a.sh`\n"
    d = "- `dev/varka_d/`: the files a reads. `[data]`\n"
    both = ["varka_a.sh", "varka_d"]
    case("every entry indexed", both, "# Index\n\n" + a + d)
    case("an empty tree and an empty index", [], "# Index\n")
    case("a tool with no bullet", both, a, "dev/varka_d is not in")
    case("a bullet for a tool that is gone", ["varka_a.sh"], a + d, "names dev/varka_d, which")
    case("a tool with two bullets", both, a + d + a, "dev/varka_a.sh has a second bullet")
    case(
        "a name in running text is not a bullet",
        both,
        a + "Read `dev/varka_d/` first.\n",
        "dev/varka_d is not in",
    )
    case(
        "a bullet without a tag",
        both,
        a + "- `dev/varka_d/`: the files a reads.\n",
        "dev/varka_d carries 0 of the tags",
    )
    case(
        "a bullet with two tags",
        both,
        d + "- `dev/varka_a.sh`: runs a. `[sbt]` or `[java]`\n",
        "dev/varka_a.sh carries 2 of the tags",
    )
    case(
        "a tag on a continuation line counts",
        both,
        d + "- `dev/varka_a.sh`: runs a,\n  at length. `[no JVM]`\n  `dev/varka_a.sh`\n",
    )
    case(
        "a tag after a blank line belongs to no bullet",
        both,
        d + "- `dev/varka_a.sh`: runs a.\n\n`[sbt]` is a tag.\n",
        "dev/varka_a.sh carries 0 of the tags",
    )
    case(
        "the next bullet ends a bullet",
        both,
        "- `dev/varka_a.sh`: runs a.\n" + d,
        "dev/varka_a.sh carries 0 of the tags",
    )
    case(
        "another prefix is not this lint's",
        ["varka_a.sh"],
        a + "- `dev/lint-java`: Spark's. `[sbt]`\n",
    )
    case(
        "a path below an entry is not a bullet of it",
        both,
        a + "- `dev/varka_d/Probe.java`: one file. `[data]`\n",
        "dev/varka_d is not in",
    )

    def words(name, text, *expected):
        got = subcommands("varka_t.py", text)
        if got != list(expected):
            failures.append(f"{name}: expected subcommands {list(expected)}, got {got}")

    words(
        "an argparse subcommand group",
        "usage: varka_t.py [-h] {plan,times} ...\n\nWhat it does.\n",
        "plan",
        "times",
    )
    words(
        "a second brace group is the subcommand's argument",
        "usage: varka_t.py [-h] [--base BASE]\n    {list,rerun} {catalyst,sql}\n\nIt.\n",
        "list",
        "rerun",
    )
    words(
        "an option's choices are not subcommands",
        "usage: varka_t.py [-h] [--mode {fast,slow}] FILE\n\nWhat it does.\n",
    )
    words(
        "the example lines of a hand-written usage",
        "What it does.\n\n  dev/varka_t.py commit FILE\n  dev/varka_t.py pr --title T\n"
        "  dev/varka_t.py --selftest\n  dev/varka_t.py in.txt\n  dev/varka_t.py commit X\n",
        "commit",
        "pr",
    )
    words("a brace group outside the usage line", "What it does: {a,b}.\n")

    # The check itself, over planted tools: one that answers, one that crashes after a
    # subcommand (the slip of dev/varka_msg_check.py's `pr --help`), one that reads --help as
    # input and fails, and one whose help prints nothing.
    good = (
        '"""Usage.\n\n  dev/varka_good.py run FILE\n"""\nimport sys\n'
        'if "--help" in sys.argv:\n    print(__doc__)\n    sys.exit(0)\n'
    )
    crash = (
        '"""Usage.\n\n  dev/varka_crash.py pr --body-file F\n"""\nimport sys\n'
        'if sys.argv[1:2] == ["--help"]:\n    print(__doc__)\n    sys.exit(0)\n'
        'opts = dict(zip(sys.argv[2::2], sys.argv[3::2]))\nopen(opts["--body-file"])\n'
    )
    reads = "import sys\nopen(sys.argv[1])\n"
    silent = "import sys\nsys.exit(0)\n"
    with tempfile.TemporaryDirectory() as tmp:
        os.mkdir(os.path.join(tmp, "dev"))
        tools = {"varka_good.py": good, "varka_crash.py": crash, "varka_reads.py": reads}
        tools["varka_silent.py"] = silent
        for name, text in tools.items():
            with open(os.path.join(tmp, "dev", name), "w", encoding="utf-8") as f:
                f.write(text)
        got = sorted(help_findings(tmp, sorted(tools) + ["varka_data"]))
        expected = [
            "dev/varka_crash.py pr --help: exit 1 (KeyError: '--body-file')",
            "dev/varka_reads.py --help: exit 1 (FileNotFoundError",
            "dev/varka_silent.py --help: exit 0 and no output",
        ]
        ok = len(got) == len(expected) and all(g.startswith(e) for g, e in zip(got, expected))
        if not ok:
            failures.append(f"help_findings: expected {expected}, got {got}")

    # The tree's own lookup: this file is tracked, starts with the prefix and exists.
    here = os.path.basename(__file__)
    if here not in tracked_entries(ROOT):
        failures.append(f"tracked_entries: dev/{here} itself is not among the entries")

    for f in failures:
        print(f"FAIL {f}", file=sys.stderr)
    print(f"varka_tools_lint self-test: {len(failures)} failure(s)")
    return 1 if failures else 0


def main(argv):
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--self-test", action="store_true", help="run the check's cases")
    args = ap.parse_args(argv)
    if args.self_test:
        return self_test()
    path = os.path.join(ROOT, INDEX)
    if not os.path.isfile(path):
        print(f"{INDEX} does not exist: the tool index is gone", file=sys.stderr)
        return 1
    with open(path, encoding="utf-8") as f:
        text = f.read()
    entries = tracked_entries(ROOT)
    findings = check(entries, text) + help_findings(ROOT, entries)
    for finding in findings:
        print(finding, file=sys.stderr)
    if not findings:
        tools = sum(1 for n in entries if n.endswith(".py"))
        print(
            f"varka_tools_lint: {len(entries)} entries under dev/, each with one bullet; "
            f"{tools} Python tools answer --help, and after each subcommand"
        )
    return min(len(findings), 100)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
