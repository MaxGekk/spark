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
"""Figure 31: a CASE WHEN of n branches inside a whole-stage codegen stage, five ways - split as
needed (SPARK-33301), split always, not split, whole-stage codegen off, and the cliff post's
workaround (no split, hugeMethodLimit 8000) - best milliseconds for two million rows against n,
both axes log. Not split steps off where the stage's method passes 8000 bytes; split as needed
follows it below the line and split always above; the workaround is whole-stage off past it.

Every value is read from the committed results file when the script runs
(sql/core/benchmarks/CaseWhenCodegenBenchmark-jdk25-288-results.txt), so the figure cannot drift
from it (VARKA-288.md 9.1)."""

import math
import os
import re

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.join(HERE, "..", "..", "..", "core", "benchmarks")
RESULTS = os.path.join(BENCH, "CaseWhenCodegenBenchmark-jdk25-288-results.txt")
PROVENANCE = os.path.join(BENCH, "CaseWhenCodegenBenchmark-jdk25-288-provenance.txt")

NEEDED = "split as needed"
ALWAYS = "split always"
NOT = "not split"
OFF = "whole stage codegen off"
WORK = "not split, hugeMethodLimit 8000"
LABELS = {
    NEEDED: "split as needed",
    ALWAYS: "split always",
    NOT: "not split",
    OFF: "whole-stage off",
    WORK: "hugeMethodLimit=8000",
}
COLORS = {NEEDED: "#2f9e44", ALWAYS: "#1971c2", NOT: "#e03131", OFF: "#868e96", WORK: "#f08c00"}


def read_ladder(path):
    """{branches: {case: best ms}} for the one-CASE-WHEN rungs, and the 8000-byte crossing."""
    rows, sizes, rung = {}, {}, None
    head = re.compile(r"^one CASE WHEN of (\d+) branches:")
    size = re.compile(r"^largest method of the stages: .* split always, (\d+) bytes not split")
    row = re.compile(r"^(.*?)\s+(\d+)\s+\d+\s+\d+\s+[\d.]+\s+[\d.]+\s+[\d.]+X\s*$")
    pending = None
    with open(path) as f:
        for line in f:
            line = line.rstrip()
            if m := size.match(line):
                pending = int(m.group(1))
            elif m := head.match(line):
                rung = int(m.group(1))
                rows[rung] = {}
                if pending is not None:
                    sizes[rung] = pending
            elif line.startswith(("four ", "string ", "a CASE WHEN")):
                rung = None
            elif (m := row.match(line)) and rung is not None:
                rows[rung][m.group(1).strip()] = float(m.group(2))
    return rows, sizes


def read_provenance(path):
    fields = {}
    with open(path) as f:
        for line in f:
            if ":" in line and not line.startswith(" "):
                key, value = line.split(":", 1)
                fields[key.strip()] = value.strip()
    return fields


rows, sizes = read_ladder(RESULTS)
prov = read_provenance(PROVENANCE)
cpu = prov["cpu"].split(" (")[0].replace(" 64-Core Processor", "")
jdk = prov["jdk"].replace("OpenJDK 64-Bit Server VM ", "JDK ")
rungs = sorted(rows)

r = Rough(960, 700, seed=288)
r.text(40, 40, "A CASE WHEN of n branches inside a stage, five ways", size=30)
r.text(
    40,
    72,
    "best milliseconds for two million rows, both axes log; %s, %s" % (cpu, jdk),
    size=20,
    color="#5c5f66",
)

X0, X1, Y0, Y1 = 120, 720, 580, 120
LO, HI = 30.0, 20000.0
NLO, NHI = 1.6, 380.0


def px(n):
    return X0 + (X1 - X0) * (math.log10(n) - math.log10(NLO)) / (math.log10(NHI) - math.log10(NLO))


def py(ms):
    return Y0 - (Y0 - Y1) * (math.log10(ms) - math.log10(LO)) / (math.log10(HI) - math.log10(LO))


r.line(X0, Y0, X1 + 20, Y0)
r.line(X0, Y0, X0, Y1 - 20)
for ms, label in [(50, "50 ms"), (100, "100 ms"), (1000, "1 s"), (10000, "10 s")]:
    r.line(X0 - 8, py(ms), X0, py(ms))
    r.text(X0 - 14, py(ms), label, size=19, anchor="end", color="#5c5f66")
for n in rungs:
    r.line(px(n), Y0, px(n), Y0 + 8)
    r.text(px(n), Y0 + 24, str(n), size=19, anchor="middle", color="#5c5f66")
r.text((X0 + X1) / 2, Y0 + 56, "branches", size=21, anchor="middle")

# The 8000-byte line: between the last rung whose unsplit method is under it and the first past.
under = max(n for n in rungs if sizes.get(n, 0) <= 8000)
past = min(n for n in rungs if sizes.get(n, 0) > 8000)
xl = (px(under) + px(past)) / 2
r.line(xl, Y0, xl, Y1 - 10, color="#adb5bd", dash="6,6")
r.text(xl + 8, Y1 - 4, "unsplit method past 8000 bytes", size=18, color="#868e96")

# Drawn back to front, so the green split-as-needed line sits on top where it meets the others.
order = (OFF, WORK, NOT, ALWAYS, NEEDED)
ends = {}
for case in order:
    pts = [(px(n), py(rows[n][case])) for n in rungs]
    r.curve(pts, color=COLORS[case], width=3.0, monotone=True)
    for x, y in pts:
        r.ellipse(x, y, 4, 4, color=COLORS[case], width=2.0)
    ends[case] = py(rows[rungs[-1]][case])
# Labels at the last rung, pushed apart where lines end close together.
placed = []
for case in sorted(order, key=lambda c: ends[c]):
    y = ends[case]
    for p in placed:
        if abs(y - p) < 24:
            y = p + 24
    placed.append(y)
    r.text(px(rungs[-1]) + 12, y + 6, LABELS[case], size=20, color=COLORS[case])

slow = rows[past][NOT] / rows[past][NEEDED]
r.note(
    170,
    150,
    "at %d branches, not split is never compiled:\n%.0fx split as needed" % (past, slow),
    size=19,
)
r.text(
    40,
    670,
    "the workaround makes a stage past 8000 bytes run without whole-stage codegen,"
    " so past the line it times as whole-stage off",
    size=17,
    color="#868e96",
)

finish(r, "fig31-split-as-needed")
