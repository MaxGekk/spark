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
"""Pick the one option configuration a pull request's CI runs (VARKA-248.md 3.2.8).

    dev/varka_matrix_pick.py CONFIGURATIONS BRANCH [BASE]
    dev/varka_matrix_pick.py --self-test         # run this file's doctests

CONFIGURATIONS is the file `dev/varka_matrix.sh --list` writes, one `name=value` per line.
Prints the picked configuration on the first line and why on the second.

When BASE is given and the diff from it names an option in code - `withName`, `.name(` or
`::name` on an added or removed line of a Java or Scala file - the pick is one of that option's
configurations, so a change to an option's code path is checked under that option. Otherwise,
and to choose among several, a stable hash of the branch name picks: a rerun or a later push of
the same branch draws the same configuration, and different branches spread over the list. The
branch rather than the pull request number, because the fork's push builds do not know their
pull request.
"""

import hashlib
import re
import subprocess
import sys


def stable_index(text, count):
    return int(hashlib.sha256(text.encode()).hexdigest(), 16) % count


def named_options(diff, names):
    r"""The option names a diff's changed code lines mention, in the order of `names`.

    >>> diff = "+  base.withSplitDriver(false)\n- o.division()\n"
    >>> named_options(diff, ["division", "splitDriver"])
    ['division', 'splitDriver']
    >>> named_options("+  // a division by zero\n", ["division"])
    []
    >>> named_options("+++ b/VarkaEmitOptions.java\n", ["cse"])
    []
    """
    changed = [
        line[1:]
        for line in diff.splitlines()
        if line[:1] in "+-" and not line.startswith(("+++", "---"))
    ]
    text = "\n".join(changed)
    found = []
    for name in names:
        cap = name[0].upper() + name[1:]
        pattern = rf"\bwith{cap}\b|\.{name}\(|::{name}\b"
        if re.search(pattern, text):
            found.append(name)
    return found


def pick(configurations, branch, diff=None):
    r"""The configuration and the reason for it.

    >>> configs = ["cse=false", "division=DOUBLE_DIV", "division=DOUBLE_RECIP",
    ...            "splitDriver=false"]
    >>> pick(configs, "some-branch", "+  x = Builder::splitDriver\n")
    ('splitDriver=false', 'the diff names splitDriver')
    >>> pick(configs, "some-branch")[0] == pick(configs, "some-branch")[0]
    True
    >>> pick(configs, "some-branch", "+  o.withDivision(d)\n")[0].startswith("division=")
    True
    """
    names = list(dict.fromkeys(c.split("=", 1)[0] for c in configurations))
    if diff:
        options = named_options(diff, names)
        if options:
            candidates = [c for c in configurations if c.split("=", 1)[0] in options]
            choice = candidates[stable_index(branch, len(candidates))]
            return choice, f"the diff names {', '.join(options)}"
    choice = configurations[stable_index(branch, len(configurations))]
    return choice, f"no option named in the diff; the hash of branch {branch}"


def main(argv):
    if argv == ["--self-test"]:
        import doctest

        failed, _ = doctest.testmod()
        return 1 if failed else 0
    if len(argv) not in (2, 3):
        print(__doc__, file=sys.stderr)
        return 2
    with open(argv[0]) as f:
        configurations = [line.strip() for line in f if line.strip()]
    diff = None
    if len(argv) == 3 and argv[2]:
        diff = subprocess.run(
            ["git", "diff", "-U0", argv[2], "HEAD", "--", "*.java", "*.scala"],
            capture_output=True,
            text=True,
            check=False,
        ).stdout
    choice, reason = pick(configurations, argv[1], diff)
    print(choice)
    print(reason)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
