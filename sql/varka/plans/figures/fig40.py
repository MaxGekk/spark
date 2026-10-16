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
"Janino's compile time against a stage's size" table of the stock Spark 4.2.0 run
(sql/varka/demo/compiles/times-jdk25-output.txt), so the figure cannot drift from it. The
source lengths span two orders of magnitude, so the length axis is logarithmic, its ends and
ticks taken from the data."""

import math
import os
import re

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, "..", "..", "demo", "compiles", "times-jdk25-output.txt")
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


def cpu(path):
    with open(path) as f:
        for line in f:
            if line.startswith("cpu:"):
                return re.sub(r"\s+\d+-Core Processor$", "", line.split(":", 1)[1].strip())
    raise ValueError("no cpu: line in " + path)


def steps(lo, hi):
    """The 1-2-5 values from the largest at or below lo to the smallest at or above hi."""
    vals = [m * 10**e for e in range(0, 8) for m in (1, 2, 5)]
    first = max(v for v in vals if v <= lo)
    last = min(v for v in vals if v >= hi)
    return [v for v in vals if first <= v <= last]


data = shapes(RESULTS)
assert data["projection"] and data["aggregate"], data
rows = data["projection"] + data["aggregate"]

r = Rough(1000, 660, seed=4001)
r.text(40, 44, "Bigger stages cost more to compile", size=32)
r.text(
    40,
    84,
    "Janino's best time for one compile of a new source, against the source's length; "
    "stock Spark 4.2.0, GitHub runner (%s), JDK 25" % cpu(RESULTS),
    size=17,
    color=GREY,
)

X0, X1, Y0, Y1 = 110, 900, 520, 150
XTICKS = steps(min(c for _, c, _ in rows), max(c for _, c, _ in rows))
XMIN, XMAX = XTICKS[0], XTICKS[-1]
YSTEP = 5
YMAX = YSTEP * (math.floor(max(ms for _, _, ms in rows) / YSTEP) + 1)


def px(chars):
    return X0 + (X1 - X0) * math.log(chars / XMIN) / math.log(XMAX / XMIN)


def py(ms):
    return Y0 - (Y0 - Y1) * ms / YMAX


r.arrow(X0, Y0, X1 + 30, Y0)
r.arrow(X0, Y0, X0, Y1 - 30)
for ms in range(0, YMAX + 1, YSTEP):
    r.line(X0 - 8, py(ms), X0, py(ms))
    r.text(X0 - 14, py(ms), "%d ms" % ms, size=16, anchor="end", color=GREY)
for chars in XTICKS:
    r.line(px(chars), Y0, px(chars), Y0 + 8)
    r.text(px(chars), Y0 + 26, "{:,}".format(chars), size=16, anchor="middle", color=GREY)
r.text(
    (X0 + X1) / 2,
    Y0 + 62,
    "characters of generated Java source (log scale)",
    size=19,
    anchor="middle",
)

# On the log axis the two shapes do not share a length. The projections, nearly flat, label
# above their points; the aggregates, steep, to the left of theirs, clear of the curve. Only the
# ends and the middle are labelled.
for kind, color, one, many, dx, dy, anchor in (
    ("projection", BLUE, "entry", "entries", 0, -24, "middle"),
    ("aggregate", ORANGE, "sum", "sums", -14, -4, "end"),
):
    rows = data[kind]
    pts = [(px(c), py(ms)) for _, c, ms in rows]
    r.curve(pts, color=color, width=2.6)
    shown = {0, len(rows) // 2, len(rows) - 1}
    for i, ((n, c, ms), (x, y)) in enumerate(zip(rows, pts)):
        r.ellipse(x, y, 4, 4, color=color, width=2.2)
        if i in shown:
            r.text(
                x + dx,
                y + dy,
                "%d %s, %s ms" % (n, one if n == 1 else many, "%.2f" % ms),
                size=15,
                anchor=anchor,
                color=color,
            )
    lx, ly = pts[-1]
    r.text(lx + 16, ly, kind + "s", size=19, color=color)

r.note(
    40,
    630,
    "a new literal pays this once on the driver and again in every executor that runs the stage",
    size=19,
)
finish(r, "fig40-bigger-stages-cost-more")
