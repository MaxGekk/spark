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

"""Figure 33: the class is not the cost - on one millisecond scale, what making a class costs
each generator for the same sixteen date expressions, both cold (Janino over Spark's generated
source; Varka's compiler, emitter, define and load) against what the first, second and
compiled queries of a new shape cost each engine: Spark, Varka with no warm-up, and Varka with
its warm-up, over a hundred thousand cached rows, the cold-start benchmark's first rung.

Every value is read from the committed files when the script runs, so the figure cannot drift
from them: the class bars from the matched section of
sql/core/benchmarks/VarkaCodegenBenchmark-jdk25-results.txt (a GitHub runner, VARKA-259 9.2), the
query bars from VarkaColdStartBenchmark-jdk25-results.txt (the laptop)."""

import os
import re

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.join(HERE, "..", "..", "..", "core", "benchmarks")
CODEGEN = os.path.join(BENCH, "VarkaCodegenBenchmark-jdk25-results.txt")
COLD = os.path.join(BENCH, "VarkaColdStartBenchmark-jdk25-results.txt")
RUNG = 16
GREY = "#5c5f66"
RED, ORANGE, GREEN = "#e03131", "#f08c00", "#2f9e44"


def per_row_ms(path, table, prefix):
    """The 'Per Row(ns)' column, in milliseconds, of the row starting with prefix in the table
    whose heading starts with table."""
    row = re.compile(r"^(.*?)\s+\d+\s+\d+\s+\d+\s+[\d.]+\s+([\d.]+)\s+[\d.]+X\s*$")
    inside = False
    with open(path) as f:
        for line in f:
            line = line.rstrip()
            if line.endswith("Relative"):
                inside = line.startswith(table)
            elif inside and (m := row.match(line)) and m.group(1).strip().startswith(prefix):
                return float(m.group(2)) / 1e6
    raise ValueError(table + ": " + prefix)


def rung_best_ms(path, rung):
    """{case: best ms} for one rung of the cold-start benchmark."""
    head = re.compile(r"^(\d+) entries over \d+ Arrow-cached rows:")
    row = re.compile(r"^(.*?)\s+(\d+)\s+\d+\s+\d+\s+[\d.]+\s+[\d.]+\s+[\d.]+X\s*$")
    rows, current = {}, None
    with open(path) as f:
        for line in f:
            line = line.rstrip()
            if m := head.match(line):
                current = int(m.group(1))
            elif line.startswith("===="):
                current = None
            elif current == rung and (m := row.match(line)):
                rows[m.group(1).strip()] = int(m.group(2))
    return rows


TABLE = "%d date expressions:" % RUNG
janino = per_row_ms(CODEGEN, TABLE, "spark: generate + Janino compile, cold")
emit = per_row_ms(CODEGEN, TABLE, "varka: compile + emit + define + load + instantiate, cold")
cold = rung_best_ms(COLD, RUNG)

r = Rough(1000, 700, seed=3301)
r.text(40, 44, "The class is the cheap part", size=32)
r.text(
    40,
    84,
    "milliseconds, best of five, one scale; %d date expressions, JDK 25; classes on a GitHub "
    "runner, queries on a laptop over 100,000 cached rows" % RUNG,
    size=17,
    color=GREY,
)

X0, X1, LABEL = 330, 930, 310
MAX = 250.0


def px(ms):
    return X0 + (X1 - X0) * ms / MAX


def bar(y, label, ms, color, text=None):
    r.text(LABEL, y + 22, label, size=20, anchor="end")
    w = max(px(ms) - X0, 3)
    r.rect(X0, y, w, 32, fill=color)
    r.text(X0 + w + 10, y + 22, text or "%d ms" % ms, size=20)


y = 130
r.text(40, y + 10, "making the class", size=23)
bar(y + 26, "Spark: source, then Janino", janino, GREY, "%.1f ms" % janino)
bar(y + 70, "Varka: compile, emit, load", emit, GREEN, "%.1f ms" % emit)

y = 270
r.text(40, y + 10, "running the query", size=23)
# (label, the benchmark's case prefix, colour, runs)
groups = [
    ("Spark", "vanilla Spark", RED, ["first run", "second run"]),
    ("Varka, no warm-up", "Varka", ORANGE, ["first run", "second run"]),
    (
        "Varka with warm-up",
        "Varka with warm-up",
        GREEN,
        ["first run", "second run", "once compiled"],
    ),
]
y += 26
for engine, case, color, runs in groups:
    for run in runs:
        bar(y, "%s, %s" % (engine, run), cold[case + ", " + run], color)
        y += 40
    y += 10

r.line(X0, 150, X0, y - 4, width=1.4)
r.arrow(X0, y - 4, X1 + 30, y - 4)
for ms in (0, 50, 100, 150, 200, 250):
    r.line(px(ms), y - 10, px(ms), y + 2)
    r.text(px(ms), y + 22, "%d" % ms, size=17, anchor="middle", color=GREY)

r.note(
    40,
    y + 66,
    "a cheaper class leaves the first query slower: the kernel serves batches before C2 "
    "has compiled it",
    size=20,
)
finish(r, "fig33-the-class-is-the-cheap-part")
