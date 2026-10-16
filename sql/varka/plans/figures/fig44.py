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

"""Figure 44: where the template warm-up helps - deadline releases in the first 100
compositions of Varka's warm-up test, with the warm-up of the shared templates on and off, at
the laptop's preferred 512-bit vectors, at 8- and 4-lane overrides, and with the JVM capped at
128-bit vectors. Each bar is one JVM. The counts are read from the "Other lane counts and the
128-bit host" table of VARKA-373.md, section 9, so the figure cannot drift from it."""

import os

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
RECORD = os.path.join(HERE, "..", "m7", "VARKA-373.md")
GREY = "#5c5f66"
OFF, ON = "#ff8787", "#69db7c"


def table(path):
    """{first cell: (on counts, off counts)} of the configuration table."""
    with open(path) as f:
        lines = f.read().splitlines()
    start = lines.index("| configuration | warm-up on | warm-up off |")
    out = {}
    for line in lines[start + 2 :]:
        if not line.startswith("|"):
            break
        cells = [c.strip() for c in line.strip("|").split("|")]
        on, off = ([int(v) for v in c.split(",")] for c in cells[1:3])
        out[cells[0]] = (on, off)
    return out


t = table(RECORD)
ROWS = [
    ("512-bit, 16 lanes\n(the preferred width)", "16 lanes (the preferred species here)"),
    ("8 lanes (override)", "8 lanes"),
    ("4 lanes (override)", "4 lanes"),
    ("x86 capped at\n128 bits", next(k for k in t if k.startswith("`-XX:MaxVectorSize=16`"))),
]
assert len(t) == len(ROWS), t

r = Rough(1000, 700, seed=4401)
r.text(40, 44, "Where the template warm-up helps", size=32)
r.text(
    40,
    84,
    "kernels released at the deadline in the first 100 compositions of Varka's warm-up test; "
    "JDK 25, the 512-bit laptop; one JVM a bar",
    size=17,
    color=GREY,
)

X0, X1, LABEL, TOP = 300, 900, 280, 50


def px(v):
    return X0 + (X1 - X0) * v / TOP


y = 130
for label, key in ROWS:
    on, off = t[key]
    y0 = y
    for fill, values in ((OFF, off), (ON, on)):
        for v in values:
            if v:
                r.rect(X0, y, px(v) - X0, 20, fill=fill, solid=True)
            r.text(px(v) + 10, y + 11, "%d" % v, size=17)
            y += 25
    r.text(LABEL, (y0 + y - 5) / 2, label, size=18, anchor="end")
    r.line(X0 - 10, y + 5, X1, y + 5, color="#dee2e6")
    y += 16
r.arrow(X0, y, X1 + 20, y)
for v in range(0, TOP + 1, 10):
    r.line(px(v), y - 6, px(v), y + 6)
    r.text(px(v), y + 22, "%d" % v, size=15, anchor="middle", color=GREY)

y += 56
r.rect(X0, y, 22, 18, fill=OFF, solid=True)
r.text(X0 + 30, y + 10, "warm-up off", size=17)
r.rect(X0 + 170, y, 22, 18, fill=ON, solid=True)
r.text(X0 + 200, y + 10, "warm-up on", size=17)

finish(r, "fig44-where-the-template-warm-up-helps")
