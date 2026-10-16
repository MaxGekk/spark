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


"""Figure 49: seven JVM configurations, one set of answers - the fuzzers' kernels run in a forked
JVM per configuration (the interpreter alone, C1 alone, C2, the Vector API's intrinsics off,
everything compiled before it runs, C2's stress flags, a debug build of the JDK), each proving
from the JVM's own events which compiler's code served the compared calls, and every answer
compared with the default C2 run's. Read from the arm table of VARKA-272.md section 9.1; the
wall times in that table are a loaded laptop's and are not drawn."""

import os
import re

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
RECORD = os.path.join(HERE, "..", "m7", "VARKA-272.md")
GREY = "#5c5f66"

NAMES = {
    "int": "the interpreter only",
    "c1": "C1 only",
    "c2": "C2, as shipped",
    "novec": "Vector API intrinsics off",
    "xcomp": "everything compiled first",
    "stress": "C2 with its stress flags",
    "fastdebug": "a debug build of the JDK",
}


def table(path):
    """[(arm, answers, differing or None for the reference)] of section 9.1's arm table."""
    with open(path) as f:
        lines = f.read().splitlines()
    start = lines.index("| arm | wall | answers | shared with `c2`, differing | tier proof read |")
    out = []
    for line in lines[start + 2 :]:
        if not line.startswith("|"):
            break
        cells = [c.strip() for c in line.strip("|").split("|")]
        arm = cells[0].strip("`")
        answers = int(cells[2].replace(",", ""))
        m = re.match(r"([\d,]+), (\d+)$", cells[3])
        out.append((arm, answers, int(m.group(2)) if m else None))
    return out


rows = table(RECORD)
assert [a for a, _, _ in rows] == list(NAMES), rows
assert sum(1 for _, _, d in rows if d is None) == 1

r = Rough(1000, 560, seed=4901)
r.text(40, 44, "Seven JVMs, one set of answers", size=32)
r.text(
    40,
    84,
    "answers each configuration compared with the default JVM's, and how many differed",
    size=20,
    color=GREY,
)

X0, X1, LABEL = 330, 820, 310
TOP = max(n for _, n, _ in rows)


def px(v):
    return X0 + (X1 - X0) * v / TOP


y = 130
for arm, n, differ in rows:
    fill = "#ced4da" if differ is None else "#51cf66"
    r.rect(X0, y, px(n) - X0, 26, fill=fill, solid=True)
    r.text(LABEL, y + 13, NAMES[arm], size=19, anchor="end")
    note = "the reference" if differ is None else "%d differ" % differ
    r.text(px(n) + 10, y + 13, "%d, %s" % (n, note), size=17)
    y += 50
r.line(X0, 120, X0, y)

finish(r, "fig49-seven-jvms-one-set-of-answers")
