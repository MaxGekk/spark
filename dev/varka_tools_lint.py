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

And, for every Python tool git tracks (`dev/varka*.py`; an untracked file is never run), run
once with `--help`, no input, a deadline of HELP_SECONDS and an environment that cannot reach
GitHub (no GH_TOKEN or GITHUB_TOKEN, GH_HOST an invalid host, git's ssh disabled):

  * `--help` that does not exit 0, prints nothing or is still running at the deadline;
  * the subcommands that run's text names, and only these: the first brace group of an
    argparse usage line (`{plan,times}`, not an option's choices) and each word after the
    tool's own path at the start of an example line (`dev/varka_msg_check.py pr ...`). A tool
    the help text names no subcommand of is checked by its `--help` alone;
  * a tool with such a subcommand whose source does not read `-h`/`--help` anywhere on the
    command line (`"--help" in argv`, `"--help" in sys.argv[1:]`) and does not use argparse:
    a tool that checks only its first argument would start the subcommand's work on
    `<subcommand> --help`, so it is reported and not run;
  * else `<subcommand> --help` that does not exit 0, prints nothing or is still running.

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
# A tool reads -h/--help anywhere on its command line: the test this row's tools make
# (`"--help" in argv`), or argparse, whose parser and each subparser answer it in any place.
HELP_ANYWHERE_RE = re.compile(r"""["']--help["']\s+in\s|\bArgumentParser\(""")
# The variables a child must not see, and those it gets instead, so that a tool which takes
# `--help` for work cannot dispatch a run or push: gh has no token and no real host, git no ssh.
HIDDEN_ENV = ("GH_TOKEN", "GITHUB_TOKEN", "GH_ENTERPRISE_TOKEN", "GITHUB_ENTERPRISE_TOKEN")
OFFLINE_ENV = {"GH_HOST": "gh-disabled.invalid", "GIT_SSH_COMMAND": "false"}


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


def tracked_tools(root):
    """The Python tools directly under dev/ that git tracks: the only ones the lint runs."""
    out = subprocess.run(
        ["git", "-C", root, "ls-files", "-z", "--cached", "dev/" + PREFIX + "*.py"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    names = {path.split("/")[1] for path in out.split("\0") if path.count("/") == 1}
    return sorted(n for n in names if os.path.isfile(os.path.join(root, "dev", n)))


def offline_env():
    """This process's environment without GitHub's tokens, gh pointed at no host."""
    env = {k: v for k, v in os.environ.items() if k not in HIDDEN_ENV}
    env.update(OFFLINE_ENV)
    return env


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
    example = re.compile(r"^\s*dev/" + re.escape(name) + r" ([a-z][a-z0-9_-]*)(?=\s|$)", re.M)
    return list(dict.fromkeys(example.findall(text)))


def answers(path, args, seconds=HELP_SECONDS):
    """`path args --help`, run once offline: (None, its output) when it exits 0 and prints,
    else (why not, its output so far). A run past the deadline is a finding, not an error."""
    shown = " ".join([f"dev/{os.path.basename(path)}", *args, "--help"])
    try:
        run = subprocess.run(
            [sys.executable, path, *args, "--help"],
            cwd=os.path.dirname(os.path.dirname(path)),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=seconds,
            env=offline_env(),
        )
    except subprocess.TimeoutExpired:
        return f"{shown}: still running after {seconds} s: answer --help before any work", ""
    text = run.stdout + "\n" + run.stderr
    if run.returncode != 0:
        last = (run.stderr.strip() or run.stdout.strip()).splitlines()[-1:] or ["no output"]
        why = f"{shown}: exit {run.returncode} ({last[0][:120]}): print the usage and exit 0"
        return why, text
    if not text.strip():
        return f"{shown}: exit 0 and no output: print the usage", text
    return None, text


def help_anywhere(path):
    """Does the tool's source read -h/--help anywhere on its command line? Read, not run."""
    with open(path, encoding="utf-8", errors="replace") as f:
        return bool(HELP_ANYWHERE_RE.search(f.read()))


def help_findings(root, tools, seconds=HELP_SECONDS):
    """The findings of --help over the given Python tools, each then over its subcommands."""

    def one(name):
        path = os.path.join(root, "dev", name)
        first, text = answers(path, [], seconds)
        if first:
            return [first]
        words = subcommands(name, text)
        if words and not help_anywhere(path):
            # Running `<subcommand> --help` would start the subcommand's work in a tool that
            # checks only its first argument, so the source answers for it instead.
            return [
                f"dev/{name}: reads -h/--help only in some places, so `dev/{name} {words[0]} "
                f'--help` would run {words[0]}; not run: test `"-h" in argv or "--help" in '
                "argv` before any work"
            ]
        return [f for f in (answers(path, [w], seconds)[0] for w in words) if f]

    tools = [n for n in tools if n.endswith(".py")]
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

    # The check itself, over planted tools in a scratch repository: one that answers and logs
    # each run, one that checks only its first argument (a subcommand's `--help` would start
    # its work), one that crashes after a subcommand before its help test (the slip of
    # dev/varka_msg_check.py's `pr --help`), one that reads --help as input, one whose help
    # prints nothing, one past the deadline, one that fails when it can reach GitHub, and one
    # git does not track, which the lint must never run.
    good = (
        '"""Usage.\n\n  dev/varka_good.py run FILE\n"""\nimport os, sys\n'
        'with open(os.path.join(os.path.dirname(__file__), "good.log"), "a") as f:\n'
        '    f.write(" ".join(sys.argv[1:]) + "\\n")\n'
        'if "--help" in sys.argv:\n    print(__doc__)\n    sys.exit(0)\n'
    )
    work = (
        "import os, sys\n"
        "def done(what):\n"
        '    open(os.path.join(os.path.dirname(__file__), what + ".done"), "w").close()\n'
    )
    first = (
        '"""Usage.\n\n  dev/varka_first.py pr --body-file F\n"""\n'
        + work
        + 'if sys.argv[1:2] == ["--help"]:\n    print(__doc__)\n    sys.exit(0)\n'
        'done("first")\n'
    )
    crash = (
        '"""Usage.\n\n  dev/varka_crash.py pr --body-file F\n"""\nimport sys\n'
        'if sys.argv[1:2] == ["pr"]:\n    open(sys.argv[3])\n'
        'if "--help" in sys.argv:\n    print(__doc__)\n    sys.exit(0)\n'
    )
    reads = "import sys\nopen(sys.argv[1])\n"
    silent = "import sys\nsys.exit(0)\n"
    slow = "import time\ntime.sleep(60)\n"
    online = (
        "import os, sys\n"
        f"hidden = [k for k in {HIDDEN_ENV!r} if k in os.environ]\n"
        f"if hidden or os.environ.get('GH_HOST') != {OFFLINE_ENV['GH_HOST']!r}:\n"
        "    sys.exit('can reach GitHub')\n"
        "print('offline')\n"
    )
    untracked = work + 'done("untracked")\n'
    planted = os.environ.get("GH_TOKEN")
    os.environ["GH_TOKEN"] = "planted-by-the-self-test"
    try:
        with tempfile.TemporaryDirectory() as tmp:
            dev = os.path.join(tmp, "dev")
            os.mkdir(dev)
            tools = {
                "varka_good.py": good,
                "varka_first.py": first,
                "varka_crash.py": crash,
                "varka_reads.py": reads,
                "varka_silent.py": silent,
                "varka_slow.py": slow,
                "varka_online.py": online,
            }
            for name, text in {**tools, "varka_untracked.py": untracked}.items():
                with open(os.path.join(dev, name), "w", encoding="utf-8") as f:
                    f.write(text)
            git = ["git", "-C", tmp]
            subprocess.run(git + ["init", "-q"], check=True)
            subprocess.run(git + ["add", *(f"dev/{n}" for n in tools)], check=True)
            listed = tracked_tools(tmp)
            if listed != sorted(tools):
                failures.append(f"tracked_tools: expected {sorted(tools)}, got {listed}")
            got = sorted(help_findings(tmp, listed, seconds=3))
            expected = [
                "dev/varka_crash.py pr --help: exit 1 (IndexError",
                "dev/varka_first.py: reads -h/--help only in some places",
                "dev/varka_reads.py --help: exit 1 (FileNotFoundError",
                "dev/varka_silent.py --help: exit 0 and no output",
                "dev/varka_slow.py --help: still running after 3 s",
            ]
            ok = len(got) == len(expected) and all(g.startswith(e) for g, e in zip(got, expected))
            if not ok:
                failures.append(f"help_findings: expected {expected}, got {got}")
            ran = sorted(n for n in os.listdir(dev) if n.endswith(".done"))
            if ran:
                failures.append(f"help_findings: a planted tool did its work: {ran}")
            with open(os.path.join(dev, "good.log"), encoding="utf-8") as f:
                runs = f.read().splitlines()
            if runs != ["--help", "run --help"]:
                failures.append(f"help_findings: one run each expected, varka_good.py ran {runs}")
    finally:
        if planted is None:
            os.environ.pop("GH_TOKEN")
        else:
            os.environ["GH_TOKEN"] = planted

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
    findings = check(entries, text) + help_findings(ROOT, tracked_tools(ROOT))
    for finding in findings:
        print(finding, file=sys.stderr)
    if not findings:
        tools = len(tracked_tools(ROOT))
        print(
            f"varka_tools_lint: {len(entries)} entries under dev/, each with one bullet; "
            f"{tools} tracked Python tools answer --help, and after each subcommand it names"
        )
    return min(len(findings), 100)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
