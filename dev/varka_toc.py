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
import re
import sys

BEGIN = "<!-- BEGIN generated contents -->"
END = "<!-- END generated contents -->"


def anchor(heading: str) -> str:
    """GitHub's slug: lowercased, punctuation dropped, spaces to hyphens."""
    slug = heading.strip().lower()
    slug = re.sub(r"[^\w\s-]", "", slug)
    return re.sub(r"\s+", "-", slug)


def contents(text: str) -> str:
    """One line per `##` heading, in document order, as a Markdown list."""
    lines = []
    in_code = False
    for line in text.split("\n"):
        if line.startswith("```"):
            in_code = not in_code
        elif not in_code and line.startswith("## "):
            title = line[3:].strip()
            lines.append(f"* [{title}](#{anchor(title)})")
    return "\n".join(lines) + "\n"


def rewrite(text: str, body: str) -> str:
    start, end = text.index(BEGIN), text.index(END)
    return text[: start + len(BEGIN)] + "\n" + body + text[end:]


def main() -> int:
    parser = argparse.ArgumentParser(add_help=True, description=__doc__)
    parser.add_argument("--check", action="store_true", help="report staleness, write nothing")
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
        body = contents(text)
        current = text[text.index(BEGIN) + len(BEGIN) : text.index(END)].lstrip("\n")
        if current == body:
            continue
        if args.check:
            print(f"{path}: the contents block is stale; run dev/varka_toc.py {path}")
            findings += 1
        else:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(rewrite(text, body))
            print(f"{path}: contents regenerated ({body.count(chr(10))} entries)")
    return findings


if __name__ == "__main__":
    sys.exit(main())
