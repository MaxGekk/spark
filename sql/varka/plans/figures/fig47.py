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


"""Figure 47: both bodies run - of the kernel classes Varka's suites ran, how many ran both of
their loops (the one for null-free batches and the one that carries a null mask), how many have
only one by construction, and how many ran only one of two. Three committed versions of
`sql/varka/coverage/generated.md`, the JaCoCo report over every emitted class, kept with their
commits in `data/both-bodies.tsv`; the last row is held to the current file, so the figure fails
when the report is regenerated and the data file is not."""

import os
import re

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data", "both-bodies.tsv")
REPORT = os.path.join(HERE, "..", "..", "coverage", "generated.md")
GREY = "#5c5f66"
BOTH, ONE_BY_DESIGN, ONE_OF_TWO, NEITHER = "#51cf66", "#4dabf7", "#ff8787", "#ced4da"


def rows(path):
    out = []
    with open(path) as f:
        lines = [ln for ln in f.read().splitlines() if ln and not ln.startswith("#")]
    head = lines[0].split("\t")
    for ln in lines[1:]:
        out.append(dict(zip(head, ln.split("\t"))))
    return out


def current(path):
    """(ran, both, only driver, only dense, only masked, neither) of the report's first table."""
    with open(path) as f:
        text = f.read()
    m = re.search(r"\| emitted \| ran \|.*?\n\|[-: |]+\n\| (\d+(?: \| \d+)+) \|", text)
    assert m, "generated.md's class table changed; update the figure"
    v = [int(x) for x in m.group(1).split(" | ")]
    return v[1:]


data = rows(DATA)
last = data[-1]
assert current(REPORT) == [
    int(last[k]) for k in ("ran", "both", "one_by_design", "only_dense", "only_masked", "neither")
], "generated.md moved; add its new version to data/both-bodies.tsv"

r = Rough(1000, 520, seed=4701)
r.text(40, 44, "Did the tests run both loops?", size=32)
r.text(
    40,
    84,
    "the kernel classes Varka's suites ran, by which of their two loops ran; one bar a report",
    size=20,
    color=GREY,
)

X0, X1, LABEL = 300, 940, 280
TOP = max(int(d["ran"]) for d in data)


def px(v):
    return X0 + (X1 - X0) * v / TOP


y = 140
for d in data:
    parts = [
        (int(d["both"]), BOTH),
        (int(d["one_by_design"]), ONE_BY_DESIGN),
        (int(d["only_dense"]) + int(d["only_masked"]), ONE_OF_TWO),
        (int(d["neither"]), NEITHER),
    ]
    x = X0
    for v, fill in parts:
        if v:
            r.rect(x, y, px(v) - X0, 44, fill=fill, solid=True)
        x += px(v) - X0
    r.text(px(int(d["both"])) - 8, y + 22, d["both"], size=18, anchor="end")
    r.text(LABEL, y + 22, d["label"].replace("\\n", "\n"), size=19, anchor="end")
    y += 90

y += 10
for i, (fill, name) in enumerate(
    (
        (BOTH, "both loops ran"),
        (ONE_BY_DESIGN, "has one loop by design"),
        (ONE_OF_TWO, "one of its two loops ran"),
        (NEITHER, "returned before either"),
    )
):
    x = X0 + (i % 2) * 320
    yy = y + (i // 2) * 34
    r.rect(x, yy, 22, 18, fill=fill, solid=True)
    r.text(x + 32, yy + 10, name, size=18)

finish(r, "fig47-did-the-tests-run-both-loops")
