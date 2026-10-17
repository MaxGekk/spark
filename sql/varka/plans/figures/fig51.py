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


"""Figure 51: defects by the method that found them - one row per verification method of
milestone 7 (and one for reading), a chip per find: a defect, a gap in the tests, or nothing.
Read from `data/defects-by-method.tsv`, which names the record behind each chip."""

import os

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data", "defects-by-method.tsv")
GREY = "#5c5f66"
DEFECT, GAP, NONE = "#ff8787", "#ffd43b", "#dee2e6"
ORDER = [
    "proofs",
    "enumeration",
    "variants",
    "expression differential",
    "random queries",
    "Spark's own tests",
    "coverage",
    "mutation testing",
    "JIT tiers",
    "memory checking",
    "reading",
]


def finds(path):
    """{method: [(kind, label)]} in the file's order."""
    out = {}
    with open(path) as f:
        for line in f:
            if line.startswith("#") or line.startswith("method\t") or not line.strip():
                continue
            method, kind, label, _record = line.rstrip("\n").split("\t")
            out.setdefault(method, []).append((kind, label))
    return out


m = finds(DATA)
assert sorted(m) == sorted(ORDER), sorted(m)
defects = sum(1 for rows in m.values() for kind, _ in rows if kind == "defect")

r = Rough(1000, 700, seed=5101)
r.text(40, 44, "Defects by the method that found them", size=32)
r.text(
    40,
    84,
    "the %d defects and the test gaps milestone 7 found, one chip each, by what found it" % defects,
    size=20,
    color=GREY,
)

X0, LABEL, H, CHAR, RIGHT = 290, 270, 30, 7.6, 980
y = 124
for method in ORDER:
    x = X0
    top = y
    for kind, label in m[method]:
        fill = {"defect": DEFECT, "gap": GAP, "none": NONE}[kind]
        w = int(len(label) * CHAR) + 18
        if x + w > RIGHT:  # a method with more finds than fit on one line wraps
            x = X0
            y += H + 8
        r.rect(x, y, w, H, fill=fill, solid=True)
        r.text(x + 9, y + H / 2, label, size=17)
        x += w + 8
    r.text(LABEL, top + H / 2, method, size=20, anchor="end")
    y += 44

y += 10
for i, (fill, name) in enumerate(
    ((DEFECT, "a defect"), (GAP, "a gap in the tests"), (NONE, "nothing found"))
):
    x = X0 + i * 220
    r.rect(x, y, 22, 18, fill=fill, solid=True)
    r.text(x + 32, y + 10, name, size=18)

finish(r, "fig51-defects-by-method")
