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
the rest; and below, as a separate measurement on another build and CPU, the same comparison for
an integer written into the source against one passed to the class as a reference, the change
Spark already makes for strings. The upper block reads the "mean of 40 queries" table of the
stock Spark 4.2.0 run (sql/varka/demo/compiles/times-jdk25-output.txt); the lower block reads the
reference block of this repository's benchmark results
(sql/core/benchmarks/CodegenRecompileBenchmark-jdk25-results.txt), since the stand-in expression
it needs is test code and exists only there. Each block names its own build and CPU and has its
own axis, so the two are not read as one measurement."""

import os
import re

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
STOCK = os.path.join(HERE, "..", "..", "demo", "compiles", "times-jdk25-output.txt")
FORK = os.path.join(
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


def cpu(path):
    """The CPU a results file names: run.sh's "cpu:" header or the JMH-style machine line."""
    with open(path) as f:
        lines = f.read().splitlines()
    for line in lines:
        if line.startswith("cpu:"):
            return line.split(":", 1)[1].strip()
    return next(line.strip() for line in lines if re.match(r"^(AMD|Intel|Apple)", line))


def short(name):
    """'AMD EPYC 9V45 96-Core Processor' -> 'AMD EPYC 9V45'."""
    return re.sub(r"\s+\d+-Core Processor$", "", name)


stock = means(STOCK)
fork = means(FORK)
assert len(stock) == 1, len(stock)
assert len(fork) == 2, len(fork)
# The fork's first block is the 4.2.0 run's arms on the other build; the figure shows its second.
reference = fork[1]

r = Rough(1000, 715, seed=3901)
r.text(40, 44, "What a click costs", size=32)
r.text(
    40,
    84,
    "SELECT count(*) over 100,000 cached rows, mean of 40 queries, ms; local mode, JDK 25, "
    "GitHub runners",
    size=17,
    color=GREY,
)

X0, X1, LABEL = 360, 900, 340
MAX = 10.0 * (int(max(ms for b in (stock[0], reference) for _, _, ms, _ in b) / 10) + 1)


def px(ms):
    return X0 + (X1 - X0) * ms / MAX


def bar(y, label, classes, ms, janino):
    r.text(LABEL, y + 16, label, size=18, anchor="end")
    if janino > 0:
        r.rect(X0, y, px(janino) - X0, 30, fill=JANINO, solid=True)
    r.rect(px(janino), y, px(ms) - px(janino), 30, fill=REST, solid=True)
    r.text(px(ms) + 10, y + 16, "%s ms, %d classes" % (("%.2f" % ms), classes), size=17)


def block(y, title, where, rows):
    """A titled block of bars with its own axis; returns the y below it."""
    r.text(40, y, title, size=22)
    r.text(40, y + 26, where, size=16, color=GREY)
    y += 46
    for label, classes, ms, janino in rows:
        bar(y, label, classes, ms, janino)
        y += 40
    r.arrow(X0, y + 4, X1 + 20, y + 4)
    for ms in range(0, int(MAX) + 1, 10):
        r.line(px(ms), y - 2, px(ms), y + 10)
        r.text(px(ms), y + 28, "%d" % ms, size=16, anchor="middle", color=GREY)
    return y + 44


y = block(
    130,
    "the value in the query",
    "stock Spark 4.2.0, %s" % short(cpu(STOCK)),
    stock[0],
)
y = block(
    y + 30,
    "an integer in the source, or passed to the class",
    "a separate run: this repository's Spark (master-based, Varka off), %s" % short(cpu(FORK)),
    reference,
)

r.rect(X0, y + 8, 22, 18, fill=JANINO, solid=True)
r.text(X0 + 30, y + 17, "Janino compiling", size=17)
r.rect(X0 + 200, y + 8, 22, 18, fill=REST, solid=True)
r.text(X0 + 230, y + 17, "everything else", size=17)
finish(r, "fig39-what-a-click-costs")
