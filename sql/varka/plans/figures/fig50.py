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


"""Figure 50: what random draws reach - the emitter's 39 named mechanisms (how it groups outputs
into methods, shares work, writes validity, guards, divides, shrinks a class that is too big, and
declines), by family, split into those the IR fuzzer's random draws reach at either width, those
only its few hand-sized wide compositions reach, and those no fuzzer test reaches. Read from
`sql/varka/coverage/reach/VarkaIrFuzzSuite.md`, the reach report the suite writes."""

import os
import re

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
REPORT = os.path.join(HERE, "..", "..", "coverage", "reach", "VarkaIrFuzzSuite.md")
GREY = "#5c5f66"
DRAWS, WIDE, NONE = "#51cf66", "#ffd43b", "#ff8787"
FAMILIES = ["grouping", "sharing", "validity", "guards", "division", "size", "decline"]


def mechanisms(path):
    """{family: [(name, wide, int draws, long draws, never)]} of the per-mechanism table."""
    with open(path) as f:
        lines = f.read().splitlines()
    start = next(i for i, ln in enumerate(lines) if ln.startswith("| family | mechanism |"))
    out = {}
    for line in lines[start + 2 :]:
        if not line.startswith("|"):
            break
        cells = [c.strip() for c in line.strip("|").split("|")]
        fam, mech, wide, ints, longs, never = cells
        name = re.match(r"`(\w+)`", mech).group(1)
        n = [0 if v == "-" else int(v) for v in (wide, ints, longs)]
        out.setdefault(fam, []).append((name, n[0], n[1], n[2], never == "never"))
    return out


m = mechanisms(REPORT)
assert list(m) == FAMILIES, list(m)
total = sum(len(v) for v in m.values())

r = Rough(1000, 560, seed=5001)
r.text(40, 44, "What random draws reach", size=32)
r.text(
    40,
    84,
    "the bytecode writer's %d named mechanisms by family, and which fuzzer tests reach each"
    % total,
    size=20,
    color=GREY,
)

X0, LABEL, UNIT = 260, 240, 60
y = 130
for fam in FAMILIES:
    rows = m[fam]
    draws = sum(1 for _, w, i, lg, nv in rows if i or lg)
    wide = sum(1 for _, w, i, lg, nv in rows if w and not (i or lg))
    none = sum(1 for _, w, i, lg, nv in rows if nv)
    assert draws + wide + none == len(rows), fam
    x = X0
    for n, fill in ((draws, DRAWS), (wide, WIDE), (none, NONE)):
        for _ in range(n):
            r.rect(x + 4, y, UNIT - 8, 30, fill=fill, solid=True)
            x += UNIT
    r.text(LABEL, y + 15, fam, size=20, anchor="end")
    y += 50

y += 16
for i, (fill, name) in enumerate(
    (
        (DRAWS, "random draws reach it"),
        (WIDE, "only the wide compositions"),
        (NONE, "no fuzzer test"),
    )
):
    x = X0 + i * 240
    r.rect(x, y, 22, 18, fill=fill, solid=True)
    r.text(x + 32, y + 10, name, size=18)

finish(r, "fig50-what-random-draws-reach")
