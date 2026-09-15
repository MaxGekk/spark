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
"""The table of contents for a long Markdown document, generated from its own headings.

  dev/varka_toc.py SKILLS.md            # rewrite the contents block in place
  dev/varka_toc.py --check SKILLS.md    # exit non-zero if the block is stale

SKILLS.md is the project's record of lessons that outlive the task that learned them,
and at sixty-odd sections a reader cannot find the one they need by scrolling. GitHub
renders no contents of its own and the file is not a Jekyll page, so the list is
generated here and checked by the pre-commit hook: a hand-written one would be wrong
by the third lesson added.

The block is delimited by the two markers below. A document without them is left
alone and reported, rather than having a block inserted at a guessed position.
"""

import argparse
import os
import re
import sys

BEGIN = "<!-- BEGIN generated contents -->"
END = "<!-- END generated contents -->"


def anchor(heading: str) -> str:
    """GitHub's slug: lowercased, punctuation dropped, spaces to hyphens."""
    slug = heading.strip().lower()
    slug = re.sub(r"[^\w\s-]", "", slug)
    return re.sub(r"\s+", "-", slug)


def headings(text: str, level: str) -> list:
    """The headings of one level, in document order, skipping fenced code blocks."""
    found = []
    in_code = False
    for line in text.split("\n"):
        if line.startswith("```"):
            in_code = not in_code
        elif not in_code and line.startswith(level):
            found.append(line[len(level) :].strip())
    return found


def contents(text: str) -> str:
    """One line per `##` heading of this document, as a Markdown list."""
    return "\n".join(f"* [{t}](#{anchor(t)})" for t in headings(text, "## ")) + "\n"


def index(directory: str) -> str:
    """One group per file in `directory`, titled by its `# ` heading, listing its lessons.

    Groups come out in path order, which is arbitrary but stable: the alternative is an
    order stored somewhere, which is one more thing that can disagree with the files.
    """
    groups = []
    for name in sorted(os.listdir(directory)):
        if not name.endswith(".md"):
            continue
        path = os.path.join(directory, name)
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
        titles = headings(text, "# ")
        if not titles:
            print(f"{path}: no `# ` title; skipped")
            continue
        lessons = headings(text, "## ")
        groups.append(f"#### [{titles[0]}]({path})\n")
        groups.append("".join(f"* [{t}]({path}#{anchor(t)})\n" for t in lessons))
        groups.append("\n")
    return "".join(groups)


def rewrite(text: str, body: str) -> str:
    start, end = text.index(BEGIN), text.index(END)
    return text[: start + len(BEGIN)] + "\n" + body + text[end:]


def main() -> int:
    parser = argparse.ArgumentParser(add_help=True, description=__doc__)
    parser.add_argument("--check", action="store_true", help="report staleness, write nothing")
    parser.add_argument("--from", dest="source", help="index this directory instead")
    parser.add_argument("files", nargs="+")
    args = parser.parse_args()

    findings = 0
    for path in args.files:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
        if BEGIN not in text or END not in text:
            print(f"{path}: no {BEGIN} block; nothing to generate")
            findings += 1
            continue
        body = index(args.source) if args.source else contents(text)
        current = text[text.index(BEGIN) + len(BEGIN) : text.index(END)].lstrip("\n")
        if current == body:
            continue
        if args.check:
            fix = f"dev/varka_toc.py {path}"
            if args.source:
                fix += f" --from {args.source}"
            print(f"{path}: the contents block is stale; run {fix}")
            findings += 1
        else:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(rewrite(text, body))
            print(f"{path}: contents regenerated ({body.count(chr(10))} entries)")
    return findings


if __name__ == "__main__":
    sys.exit(main())
