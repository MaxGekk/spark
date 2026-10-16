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

"""Figure 37: what the class cache keeps - n queries that differ only in an integer, run twice,
and the share of the second loop's queries that compiled again, under the default cache of 100
entries and under 1000. Each query compiles three classes in local mode, so the default cache
holds about thirty such queries, and past that the second loop compiles every query again.
Read from sql/varka/demo/compiles/cache-100-jdk25-output.txt and cache-1000-jdk25-output.txt."""

import os

from rough import Rough, finish

GREY = "#5c5f66"
HERE = os.path.dirname(os.path.abspath(__file__))


def rows(entries):
    """[(n, queries of the second loop that compiled)] from one output file."""
    path = os.path.join(HERE, "..", "..", "demo", "compiles", "cache-%d-jdk25-output.txt" % entries)
    out = []
    for line in open(path).read().splitlines():
        parts = line.split()
        if len(parts) == 6 and parts[0].isdigit() and parts[4] == "of":
            out.append((int(parts[0]), int(parts[3])))
    return out


SERIES = [(100, "#e03131", "maxEntries = 100, the default"), (1000, "#2f9e44", "maxEntries = 1000")]
data = {entries: rows(entries) for entries, _, _ in SERIES}

r = Rough(960, 590, seed=3701)
r.text(40, 44, "What the class cache keeps", size=32)
r.text(
    40,
    80,
    "n queries that differ only in an integer, then the same n again: how many of the",
    size=20,
    color=GREY,
)
r.text(
    40,
    106,
    "second loop's queries compiled again (stock Spark 4.2.0, local mode)",
    size=20,
    color=GREY,
)

X0, X1, Y0, Y1 = 110, 880, 500, 150
NMAX = max(n for n, _ in data[100])


def px(n):
    return X0 + (X1 - X0) * n / float(NMAX)


def py(share):
    return Y0 - (Y0 - Y1) * share


r.line(X0, Y0, X1 + 20, Y0)
r.line(X0, Y0, X0, Y1 - 20)
for share in (0.0, 0.5, 1.0):
    r.line(X0 - 8, py(share), X0, py(share))
    r.text(X0 - 14, py(share) + 6, "%d%%" % (share * 100), size=19, anchor="end", color=GREY)
for n in (10, 30, 50, 100, 150):
    r.line(px(n), Y0, px(n), Y0 + 8)
    r.text(px(n), Y0 + 28, str(n), size=18, anchor="middle", color=GREY)
r.text((X0 + X1) / 2, Y0 + 64, "distinct queries in the loop", size=21, anchor="middle")

for i, (entries, color, label) in enumerate(SERIES):
    pts = [(px(n), py(again / float(n))) for n, again in data[entries]]
    r.curve(pts, color=color, width=3.0, monotone=True)
    for x, y in pts:
        r.ellipse(x, y, 4, 4, color=color, width=2.0)
    r.line(560, 300 + i * 34, 600, 300 + i * 34, color=color, width=3.0)
    r.text(610, 306 + i * 34, label, size=19)

r.note(px(33) + 14, py(0.72), "three classes a query: 100 entries hold about 30", size=19)

finish(r, "fig37-what-the-class-cache-keeps")
