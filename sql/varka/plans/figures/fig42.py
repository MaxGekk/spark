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

"""Figure 42: who leaves the traps - not the kernel that allocates, but earlier standalone C2
compiles of the Vector API's shared templates. A template compiled after seeing one operator
class speculates on it; the first other class traps, and the trap stays in the template's
profile for the life of the JVM. A later kernel that inlines the template many times carries
that count into its compile's sum (Figure 41). The numbers are read from VARKA-295.md: the
template names, opKind's count and the copies from 9.6, the 55 of 220 lone compositions from 9.6's
table, and the template trap counts with and without the warm-up from 9.7."""

import os
import re

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
RECORD = os.path.join(HERE, "..", "m7", "VARKA-295.md")
GREY = "#5c5f66"


def record(path):
    with open(path) as f:
        text = f.read()
    s96 = re.sub(r"\s+", " ", text[text.index("### 9.6 ") : text.index("### 9.7 ")])
    s97 = re.sub(r"\s+", " ", text[text.index("### 9.7 ") : text.index("### 9.8 ")])
    m = re.search(
        r"`VectorOperators.opKind` \(inlined (\d+) times\) holds (\d+) to (\d+) traps, set by "
        r"standalone C2 compiles of `([\w:]+)`, `(\w+)` and `([\w:]+)`",
        s96,
    )
    assert m, "9.6's cause paragraph changed; update the figure"
    copies, lo, hi, t1, t2, t3 = m.groups()
    alone = re.search(r"prefix each of the (\d+) compositions alone, once \| (\d+) ", s96)
    assert alone, "9.6's table changed; update the figure"
    off = re.search(r"standalone compiles only: (\d+), (\d+) and (\d+);", s97)
    on = re.search(r"With it: (\d+), (\d+) and (\d+)\.", s97)
    assert off and on, "9.7's template counts changed; update the figure"
    return dict(
        copies=int(copies),
        traps="%s to %s" % (lo, hi),
        templates=(t1, "VectorOperators::" + t2, t3),
        of=int(alone.group(1)),
        alone=int(alone.group(2)),
        off=", ".join(off.groups()),
        on=", ".join(on.groups()),
    )


n = record(RECORD)


def box(x, y, w, h, fill, label, size):
    r.rect(x, y, w, h, fill=fill)
    r.text(x + w / 2, y + h / 2, label, size=size, anchor="middle")


r = Rough(1000, 690, seed=4201)
r.text(40, 44, "Who leaves the traps", size=32)
r.text(
    40,
    84,
    "not the kernel that allocates: earlier compiles of the Vector API's shared code",
    size=20,
    color=GREY,
)

W, H = 270, 92
XS = (40, 365, 690)
ROWS = [
    (
        "first",
        [
            ("grey", "an earlier kernel,\ne.g. abs over a\nyear-month interval"),
            ("blue", "C2 compiles a shared\ntemplate on its own,\ne.g. %s" % n["templates"][1]),
            ("yellow", "it has seen one\noperator class, so it\nspeculates on that class"),
        ],
    ),
    (
        "then",
        [
            ("grey", "a later kernel brings\nanother operator class"),
            ("orange", "the class check fails:\nan uncommon trap"),
            ("violet", "the trap stays in the\ntemplate's profile, for\nthe life of the JVM"),
        ],
    ),
    (
        "later",
        [
            ("grey", "the allocating kernel\nis compiled: opKind\ninlined %d times" % n["copies"]),
            ("violet", "every copy brings\nopKind's %s traps" % n["traps"]),
            ("red", "the compile's sum\npasses the limit\n(Figure 1)"),
        ],
    ),
]
for row, (when, boxes) in enumerate(ROWS):
    y = 130 + row * 150
    for i, (fill, label) in enumerate(boxes):
        box(XS[i], y, W, H, fill, label, 18)
        if i:
            r.arrow(XS[i] - 50, y + H / 2, XS[i] - 6, y + H / 2)
    if row:
        # From the end of the row above back to the start of this one.
        r.curve(
            [(XS[2] + W / 2, y - 58), (500, y - 30), (XS[0] + W / 2, y - 6)],
            arrow=True,
        )

r.note(
    40,
    600,
    "run alone before the allocating kernel, %d of %d earlier compositions were enough"
    % (n["alone"], n["of"]),
    size=19,
)
r.note(
    40,
    648,
    "class-check traps left in the templates, three runs: %s; "
    "with Varka's template warm-up first: %s" % (n["off"], n["on"]),
    size=19,
)

finish(r, "fig42-who-leaves-the-traps")
