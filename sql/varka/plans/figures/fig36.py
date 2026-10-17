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

"""Figure 36: where one query is compiled - the plan becomes one Java source per stage on the
driver, the driver compiles it to read its methods' sizes, the source travels to the executors in
the task, and each executor compiles it again, with the cached table's batch filter beside it;
every new class then runs in the interpreter until the JIT compiles it. The counts in the boxes
are read from sql/varka/demo/compiles/executors-jdk25-output.txt, the classes each JVM compiled
for a query with a new integer, so the figure cannot drift from the run."""

import os
import re

from rough import Rough, finish

GREY = "#5c5f66"
OUTPUT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "..",
    "demo",
    "compiles",
    "executors-jdk25-output.txt",
)


def counts():
    """{jvm: classes} for the query with a new integer value."""
    lines = open(OUTPUT).read().splitlines()
    header = next(line for line in lines if line.startswith("query"))
    jvms = re.findall(r"driver|executor \d+", header)
    row = next(line for line in lines if line.startswith("int, new value"))
    return dict(zip(jvms, (int(c) for c in row.split()[3:])))


compiled = counts()


def box(x, y, w, h, fill, label, size=19):
    r.rect(x, y, w, h, fill=fill)
    r.text(x + w / 2, y + h / 2, label, size=size, anchor="middle")


def classes(n):
    return "%d class%s" % (n, "" if n == 1 else "es")


r = Rough(1000, 640, seed=3601)
r.text(40, 44, "Where one query is compiled", size=32)
r.text(
    40,
    84,
    "SELECT count(*) FROM t WHERE i = 6, after i = 5, on a driver and two executors",
    size=20,
    color=GREY,
)

# The driver: plan, source, Janino.
r.text(40, 140, "driver", size=24, color="#1971c2")
box(40, 160, 200, 80, "grey", "the plan")
r.arrow(246, 200, 286, 200)
box(292, 160, 250, 80, "blue", 'one Java source a stage,\n"... i == 6 ..."')
r.arrow(548, 200, 588, 200)
box(
    594,
    160,
    360,
    80,
    "yellow",
    "Janino, to read the methods' sizes:\n%s, the stage" % classes(compiled["driver"]),
)
r.note(292, 270, "the cache is keyed by that text: a new 6 is a new key", size=19)

# The task carries the source; each executor compiles it again.
r.arrow(417, 290, 417, 340)
r.text(430, 324, "the source travels in the task", size=18, color=GREY)
executors = sorted(k for k in compiled if k.startswith("executor"))
for i, ex in enumerate(executors):
    y = 360 + i * 110
    r.text(40, y + 46, ex, size=22, color="#1971c2")
    box(
        170,
        y,
        420,
        80,
        "yellow",
        "Janino again: %s,\nthe stage and the cached table's batch filter" % classes(compiled[ex]),
    )
    r.arrow(596, y + 40, 636, y + 40)
    box(642, y, 312, 80, "orange", "interpreter, then C1,\nthen C2")

r.note(170, 606, "every new class starts again in the interpreter", size=19)

finish(r, "fig36-where-one-query-is-compiled")
