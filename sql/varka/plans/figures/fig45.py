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


"""Figure 45: who checks whom - the oracles a Varka answer is compared with, and what each is
held to. The answer that counts is vanilla Spark's; Catalyst's own `eval` of an expression is
Spark's interpreter; the reference evaluator, a plain Java interpreter of Varka's expression
tree, is held to it; the kernel is compared with the reference evaluator on every small tree and
with Spark's session on random queries and Spark's own SQL tests; the arithmetic inside it is
proven against the SQL-92 definitions with Spark's departures named. The coverage row count is
read from `coverage.json` and the departures from `spec/DELTAS.md`, so the labels cannot drift."""

import json
import os
import re

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
VARKA = os.path.join(HERE, "..", "..")
GREY = "#5c5f66"

with open(os.path.join(VARKA, "coverage.json")) as f:
    ROWS = len(json.load(f)["expressions"])
with open(os.path.join(VARKA, "proofs", "spec", "DELTAS.md")) as f:
    DELTAS = len(re.findall(r"^### 1\.\d+ `", f.read(), flags=re.M))
assert ROWS > 0 and DELTAS > 0

r = Rough(1000, 720, seed=4501)
r.text(40, 44, "Who checks whom", size=32)
r.text(
    40,
    84,
    "each arrow is a comparison the tests run; the answer that counts is Spark's",
    size=20,
    color=GREY,
)


def box(x, y, w, h, fill, label, size=19):
    r.rect(x, y, w, h, fill=fill)
    r.text(x + w / 2, y + h / 2, label, size=size, anchor="middle")


# The answer that counts.
box(330, 120, 340, 70, "green", "Spark, with Varka off", size=22)
# Two ways of reaching it.
box(40, 270, 320, 80, "white", "Catalyst's eval of the\nsame expression (Spark's own)")
box(640, 270, 320, 80, "white", "the same query,\nwith Varka on")
box(40, 440, 320, 80, "yellow", "the reference evaluator:\na plain Java interpreter")
box(640, 440, 320, 80, "blue", "the kernel:\nbytecode Varka wrote")
box(330, 610, 340, 80, "violet", "SQL-92 definitions, with\nSpark's %d departures named" % DELTAS)

r.arrow(200, 270, 400, 192, dash="6 6")
r.text(150, 222, "is Spark", size=19, color=GREY)
r.arrow(800, 270, 600, 192)
r.text(835, 214, "random queries,\nSpark's own SQL tests", size=19, anchor="middle")
r.arrow(200, 440, 200, 352)
r.text(212, 396, "every one of the %d expressions\nVarka compiles, and random ones" % ROWS, size=19)
r.arrow(800, 440, 800, 352)
r.text(812, 396, "runs inside it", size=19)
r.arrow(640, 480, 362, 480)
r.text(500, 455, "every small tree,\nrandom trees", size=19, anchor="middle")
r.arrow(760, 522, 640, 612)
r.text(770, 585, "its arithmetic,\nproven by two solvers", size=19)

finish(r, "fig45-who-checks-whom")
