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

"""Figure 40: bigger stages cost more to compile - Janino's best time for one compile from a
source new to the cache, against the length of that source, for projections of 1 to 99 entries
and aggregates of 10 to 60 sums by a key; every JVM that runs the stage pays it. Read from the
"Janino's compile time against a stage's size" table of the runner's committed results
(sql/core/benchmarks/CodegenRecompileBenchmark-jdk25-results.txt), so the figure cannot drift
from it."""

import os
import re

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(
    HERE, "..", "..", "..", "core", "benchmarks", "CodegenRecompileBenchmark-jdk25-results.txt"
)
GREY = "#5c5f66"
BLUE, ORANGE = "#1971c2", "#e8590c"


def shapes(path):
    """{'projection'|'aggregate': [(entries, chars, best ms)]} from the size table."""
    row = re.compile(
        r"^(projection of (\d+) entries|aggregate of (\d+) sums)\D.*?\s(\d+)\s+(\d+) bytes"
        r"\s+([\d.]+)\s+([\d.]+)\s*$"
    )
    out = {"projection": [], "aggregate": []}
    with open(path) as f:
        for line in f:
            if m := row.match(line.rstrip()):
                kind = "projection" if m.group(2) else "aggregate"
                n = int(m.group(2) or m.group(3))
                out[kind].append((n, int(m.group(4)), float(m.group(6))))
    return out


data = shapes(RESULTS)
assert data["projection"] and data["aggregate"], data

r = Rough(1000, 660, seed=4001)
r.text(40, 44, "Bigger stages cost more to compile", size=32)
r.text(
    40,
    84,
    "Janino's best time for one compile of a new source, against the source's length; "
    "GitHub runner (AMD EPYC 7763), JDK 25",
    size=17,
    color=GREY,
)

X0, X1, Y0, Y1 = 110, 900, 520, 150
XMAX, YMAX = 90000.0, 20.0


def px(chars):
    return X0 + (X1 - X0) * chars / XMAX


def py(ms):
    return Y0 - (Y0 - Y1) * ms / YMAX


r.arrow(X0, Y0, X1 + 30, Y0)
r.arrow(X0, Y0, X0, Y1 - 30)
for ms in (0, 5, 10, 15, 20):
    r.line(X0 - 8, py(ms), X0, py(ms))
    r.text(X0 - 14, py(ms), "%d ms" % ms, size=16, anchor="end", color=GREY)
for chars in (0, 20000, 40000, 60000, 80000):
    r.line(px(chars), Y0, px(chars), Y0 + 8)
    r.text(px(chars), Y0 + 26, "{:,}".format(chars), size=16, anchor="middle", color=GREY)
r.text((X0 + X1) / 2, Y0 + 62, "characters of generated Java source", size=19, anchor="middle")

# The projections run below the aggregates, so their labels go below their points and the
# aggregates' above, and only at the ends and the middle, so no label sits on the other line.
for kind, color, unit, dy, name_at in (
    ("projection", BLUE, "entries", 26, (16, 0)),
    ("aggregate", ORANGE, "sums", -24, (16, 0)),
):
    rows = data[kind]
    pts = [(px(c), py(ms)) for _, c, ms in rows]
    r.curve(pts, color=color, width=2.6)
    shown = {0, len(rows) // 2, len(rows) - 1}
    for i, ((n, c, ms), (x, y)) in enumerate(zip(rows, pts)):
        r.ellipse(x, y, 4, 4, color=color, width=2.2)
        if i in shown:
            r.text(
                x,
                y + dy,
                "%d %s, %s ms" % (n, unit.rstrip("s") if n == 1 else unit, "%.2f" % ms),
                size=15,
                anchor="middle",
                color=color,
            )
    lx, ly = pts[-1]
    r.text(lx + name_at[0], ly + name_at[1], kind + "s", size=19, color=color)

r.note(
    40,
    630,
    "a new literal pays this once on the driver and again in every executor that runs the stage",
    size=19,
)
finish(r, "fig40-bigger-stages-cost-more")
