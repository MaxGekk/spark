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

"""Figure 39: what a click costs - the mean milliseconds of a short query over a cached table
with the same value each time or a new one, each bar split into the part spent in Janino and
the rest; and below, the same comparison for an integer written into the source against one
passed to the class as a reference, the change Spark already makes for strings. Read from the
two "mean of 40 queries" blocks of the runner's committed results
(sql/core/benchmarks/CodegenRecompileBenchmark-jdk25-results.txt), so the figure cannot drift
from them."""

import os
import re

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(
    HERE, "..", "..", "..", "core", "benchmarks", "CodegenRecompileBenchmark-jdk25-results.txt"
)
GREY = "#5c5f66"
JANINO, REST = "#e8590c", "#74c0fc"


def means(path):
    """[[(label, classes, ms, janino ms)]] per "mean of 40 queries" block, in file order."""
    head = re.compile(r"^mean of \d+ queries")
    row = re.compile(r"^(.*?)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s*$")
    blocks, current = [], None
    with open(path) as f:
        for line in f:
            line = line.rstrip()
            if head.match(line):
                current = []
                blocks.append(current)
            elif current is not None and (m := row.match(line)):
                current.append(
                    (m.group(1).strip(), float(m.group(2)), float(m.group(3)), float(m.group(4)))
                )
            else:
                current = None
    return blocks


blocks = means(RESULTS)
assert len(blocks) == 2, len(blocks)

r = Rough(1000, 640, seed=3901)
r.text(40, 44, "What a click costs", size=32)
r.text(
    40,
    84,
    "SELECT count(*) over 100,000 cached rows, mean of 40 queries, ms; local mode on a GitHub "
    "runner (AMD EPYC 7763), JDK 25",
    size=17,
    color=GREY,
)

X0, X1, LABEL, MAX = 360, 900, 340, 50.0


def px(ms):
    return X0 + (X1 - X0) * ms / MAX


def bar(y, label, classes, ms, janino):
    r.text(LABEL, y + 16, label, size=18, anchor="end")
    if janino > 0:
        r.rect(X0, y, px(janino) - X0, 30, fill=JANINO, solid=True)
    r.rect(px(janino), y, px(ms) - px(janino), 30, fill=REST, solid=True)
    r.text(px(ms) + 10, y + 16, "%s ms, %d classes" % (("%.2f" % ms), classes), size=17)


y = 130
r.text(40, y, "the value in the query", size=22)
y += 22
for label, classes, ms, janino in blocks[0]:
    bar(y, label, classes, ms, janino)
    y += 40
y += 26
r.text(40, y, "an integer in the source, or passed to the class", size=22)
y += 22
for label, classes, ms, janino in blocks[1]:
    bar(y, label, classes, ms, janino)
    y += 40

r.arrow(X0, y + 4, X1 + 20, y + 4)
for ms in (0, 10, 20, 30, 40, 50):
    r.line(px(ms), y - 2, px(ms), y + 10)
    r.text(px(ms), y + 28, "%d" % ms, size=16, anchor="middle", color=GREY)

r.rect(X0, y + 52, 22, 18, fill=JANINO, solid=True)
r.text(X0 + 30, y + 61, "Janino compiling", size=17)
r.rect(X0 + 200, y + 52, 22, 18, fill=REST, solid=True)
r.text(X0 + 230, y + 61, "everything else", size=17)
finish(r, "fig39-what-a-click-costs")
