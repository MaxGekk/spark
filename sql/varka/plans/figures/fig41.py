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

"""Figure 41: one sum for the whole compile - how C2 decides, for a whole compile at once,
whether a class-check miss is an uncommon trap or a real call. Each of the 170 inlined copies
of VectorOperators.opKind adds the traps opKind's profile holds to one compile-wide sum; once
the sum passes PerMethodTrapLimit, every later class-check miss in the method is compiled as a
real virtual call, and the vector flowing out of it ends up in an int[16] on every iteration.
The two strips are drawn to scale over the compile's parse positions. Every number is read from
the step-2 record of VARKA-295.md (section 9.6), so the figure cannot drift from it."""

import os
import re

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
RECORD = os.path.join(HERE, "..", "m7", "VARKA-295.md")
GREY = "#5c5f66"


def step2(path):
    """The numbers of 9.6's cause paragraph, matched across its line breaks."""
    with open(path) as f:
        text = f.read()
    sec = text[text.index("### 9.6 ") : text.index("### 9.7 ")]
    sec = re.sub(r"\s+", " ", sec)
    m = re.search(
        r"A clean `loopDense0` of `67312f01` totals (\d+) traps; a released one passes (\d+) "
        r"at parse (\d+) of ([\d,]+), as `VectorOperators.opKind` \(inlined (\d+) times\) "
        r"holds (\d+) to (\d+) traps",
        sec,
    )
    assert m, "9.6's cause paragraph changed; update the figure"
    clean, limit, parse, total, copies, lo, hi = m.groups()
    assert re.search(r"class-check traps against %s \(`PerMethodTrapLimit`\)" % limit, sec)
    cutoff = re.search(r"decompiles against (\d+) \(`PerMethodRecompilationCutoff`/2\+1\)", sec)
    assert cutoff, "9.6's decompile limit changed; update the figure"
    nbytes = re.search(r"that mismatched-access array: (\d+) bytes", sec)
    assert nbytes, "9.6's array size changed; update the figure"
    return dict(
        clean=int(clean),
        limit=int(limit),
        parse=int(parse),
        total=int(total.replace(",", "")),
        copies=int(copies),
        lo=int(lo),
        hi=int(hi),
        cutoff=int(cutoff.group(1)),
        bytes=int(nbytes.group(1)),
    )


n = step2(RECORD)


def box(x, y, w, h, fill, label, size):
    r.rect(x, y, w, h, fill=fill)
    r.text(x + w / 2, y + h / 2, label, size=size, anchor="middle")


r = Rough(1000, 700, seed=4101)
r.text(40, 44, "One sum for the whole compile", size=32)
r.text(
    40,
    84,
    "C2 compiling the loop of one Varka kernel; JDK 25, 512-bit vectors",
    size=20,
    color=GREY,
)

# The sum: every inlined copy of opKind adds the traps its profile holds.
traps = "+%d to %d" % (n["lo"], n["hi"])
r.text(40, 140, "every inlined copy adds opKind's recorded traps", size=22)
for i, label in enumerate(["copy 1", "copy 2", "copy 3"]):
    box(40 + i * 150, 166, 130, 66, "orange", "opKind %s\n%s" % (label, traps), 18)
    r.arrow(172 + i * 150, 199, 188 + i * 150, 199)
r.text(510, 199, "...", size=28, anchor="middle")
box(550, 166, 130, 66, "orange", "opKind copy %d\n%s" % (n["copies"], traps), 18)
r.arrow(686, 199, 726, 199)
box(732, 160, 228, 78, "violet", "one sum per compile\nlimit %d" % n["limit"], 19)
r.text(
    732,
    262,
    "(PerMethodTrapLimit; decompiles have\na second sum, against %d)" % n["cutoff"],
    size=16,
    color=GREY,
)

# The two strips, to scale over the compile's parse positions.
X0, X1 = 200, 940


def px(p):
    return X0 + (X1 - X0) * p / n["total"]


r.text(40, 312, "what a class-check miss becomes, by parse position", size=22)
r.text(X0 - 14, 362, "clean", size=20, anchor="end")
box(
    X0,
    338,
    X1 - X0,
    48,
    "green",
    "an uncommon trap everywhere: the sum stays at %d" % n["clean"],
    18,
)
r.text(X0 - 14, 438, "released", size=20, anchor="end")
box(X0, 414, px(n["parse"]) - X0, 48, "green", "trap", 18)
box(px(n["parse"]), 414, X1 - px(n["parse"]), 48, "red", "a real virtual call", 18)
for p, label in (
    (0, "0"),
    (n["parse"], "{:,}".format(n["parse"])),
    (n["total"], "{:,}".format(n["total"])),
):
    r.line(px(p), 466, px(p), 478)
    r.text(px(p), 494, label, size=17, anchor="middle", color=GREY)
r.note(px(n["parse"]) + 14, 520, "the sum passes %d here" % n["limit"], size=18)

# What the real call does to the vector.
chain = [
    ("orange", "a real call returns\nan IntVector of\nunknown shape"),
    ("yellow", "the binaryOp\nintrinsic\nrefuses it"),
    ("red", "the box stays:\nnew int[16]"),
    ("red", "%d bytes on\nevery loop\niteration" % n["bytes"]),
]
for i, (fill, label) in enumerate(chain):
    x = 40 + i * 236
    box(x, 576, 196, 96, fill, label, 18)
    if i:
        r.arrow(x - 34, 624, x - 6, 624)

finish(r, "fig41-one-sum-for-the-whole-compile")
