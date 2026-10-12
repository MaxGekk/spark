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
"""Figure 32: what C2 makes of the 64-branch CASE WHEN stage, kept in one piece and split always.
Kept whole, the stage's processNext calls one method that holds the whole CASE WHEN, too big to
inline, which C2 compiles on its own. Split always, processNext inlines the small doConsume and
the first two of its five group methods, and calls the other three, refused on size.

The methods, their sizes and the decisions are read from the committed file when the script runs
(sql/core/benchmarks/CaseWhenInlining-jdk25-288-results.txt), so the figure cannot drift from it
(VARKA-288.md 9.1)."""

import os
import re

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.join(HERE, "..", "..", "..", "core", "benchmarks")
RESULTS = os.path.join(BENCH, "CaseWhenInlining-jdk25-288-results.txt")


def read_decisions(path):
    """{section heading: [(method, bytes, decision)]} for the C2 tasks of processNext."""
    sections, current = {}, None
    head = re.compile(r"^(split as needed|split always), (?:first )?C2 task of processNext")
    call = re.compile(
        r"^\s+(project_\w+) \((\d+)(?: bytes)?\)\s+(inline_success|inline_fail): (.*)$"
    )
    with open(path) as f:
        for line in f:
            line = line.rstrip()
            if m := head.match(line):
                current = m.group(1) if m.group(1) not in sections else None
                if current:
                    sections[current] = []
            elif line and not line.startswith(" "):
                current = None
            elif (m := call.match(line)) and current:
                sections[current].append(
                    (m.group(1), int(m.group(2)), m.group(3) == "inline_success", m.group(4))
                )
    return sections


decisions = read_decisions(RESULTS)
whole = decisions["split as needed"]
always = decisions["split always"]
groups = [d for d in always if "group" in d[0]]

GREEN, RED, INK, GREY = "#2f9e44", "#e03131", "#212529", "#868e96"


def box(r, x, y, w, h, label, size, color=INK):
    """A box with its label centred: `Rough.rect`'s own label reuses its width argument for the
    stroke width, which puts the label at the box's left edge."""
    r.rect(x, y, w, h, color=color)
    r.text(x + w / 2, y + h / 2, label, size=size, anchor="middle")


r = Rough(960, 640, seed=2881)
r.text(40, 40, "What C2 makes of the 64-branch stage", size=30)
r.text(
    40,
    72,
    "the stage's row loop, processNext, and the calls C2 inlines into it or refuses;"
    " -XX:+LogCompilation, JDK 25",
    size=20,
    color="#5c5f66",
)

# Kept whole: one call a row, refused, to one method compiled on its own.
r.text(40, 130, "kept in one piece (split as needed, under 8000 bytes)", size=22)
box(r, 40, 150, 250, 70, "processNext\nthe row loop", 20)
name, size, inlined, reason = whole[0]
box(r, 470, 150, 300, 70, "%s\n%d bytes, compiled alone" % (name, size), 19)
r.arrow(290, 185, 468, 185, color=RED, width=2.4)
r.text(380, 166, "one call a row", size=18, anchor="middle", color=RED)
r.text(380, 210, reason, size=17, anchor="middle", color=GREY)

# Split always: the inlined part drawn inside processNext, the refused groups outside it.
r.text(40, 290, "split always", size=22)
r.rect(40, 310, 430, 270)
r.text(56, 336, "processNext, as C2 compiled it", size=19)
consume = next(d for d in always if "doConsume" in d[0])
box(
    r,
    60,
    352,
    390,
    50,
    "%s, %d bytes: inlined" % (consume[0].replace("project_", ""), consume[1]),
    18,
    color=GREEN,
)
y = 414
for g in groups:
    if g[2]:
        box(
            r,
            60,
            y,
            390,
            50,
            "%s, %d bytes: inlined\nwith its branch methods" % (g[0].replace("project_", ""), g[1]),
            17,
            color=GREEN,
        )
        y += 62
refused = [g for g in groups if not g[2]]
for i, g in enumerate(refused):
    gy = 330 + i * 86
    box(
        r,
        620,
        gy,
        300,
        62,
        "%s, %d bytes\ncompiled on its own" % (g[0].replace("project_", ""), g[1]),
        17,
    )
    r.arrow(470, 450, 618, gy + 31, color=RED, width=2.0)
r.text(
    545,
    600,
    "%d calls a row, refused: %s" % (len(refused), refused[0][3]),
    size=18,
    anchor="middle",
    color=RED,
)

finish(r, "fig32-what-c2-inlines")
