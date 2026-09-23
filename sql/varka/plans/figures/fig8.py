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

"""Figure 8: the guards a run passes before its numbers are quoted, drawn as gates on the way
from a benchmark run to a committed results file."""

from rough import Rough, finish

r = Rough(900, 1270, seed=61)
gates = [
    (
        "the canary",
        "three fixed loops read within a few percent of this\nhost's committed"
        " baseline, or the machine is not in\nits measured state (development machines only)",
        "orange",
    ),
    (
        "the datapath probe",
        "how wide is the vector unit really: 256:128 reads\nabout 2 as a"
        " control; 512:256 reads 1.14 on this laptop\nand 2.01 on a Zen 5 runner",
        "violet",
    ),
    (
        "residency",
        "the whole table stayed in memory. A table that\nspills recomputes"
        " every iteration and passes\nevery other rule with a better number",
        "blue",
    ),
    (
        "EXPLAIN, and no fallback",
        "every entry expected to fuse plans with a Varka\nnode,"
        " and not one batch of it fell through the\ntrapdoor - a blend would look like a"
        " kernel rate",
        "green",
    ),
    (
        "the fixed-share rule",
        "the job's constant - scheduling, collecting - is under\n5%"
        " of a Varka row's wall time, or the row is\nmeasuring the harness",
        "yellow",
    ),
    (
        "the band",
        "twelve repeated runs give each case a tier;\na move inside its tier is"
        " noise, and no\nrow is quoted without one",
        "grey",
    ),
]
r.ellipse(450, 50, 60, 28, fill="blue", label="a run", size=20)
y = 100
for i, (name, what, fill) in enumerate(gates):
    r.arrow(450, y - 15, 450, y + 5)
    r.rect(270, y + 10, 360, 56, fill=fill, label=name, size=21)
    r.text(450, y + 100, what, size=17, anchor="middle", color="#5c5f66")
    # the refusal, off to the left
    r.arrow(265, y + 38, 190, y + 38, color="#e03131", head=9)
    r.text(
        180,
        y + 38,
        "refused" if i < 4 else ("fails" if i == 4 else "unquoted"),
        size=17,
        anchor="end",
        color="#e03131",
    )
    y += 160
r.arrow(450, y - 15, 450, y + 5)
r.rect(250, y + 10, 400, 100, fill="green")
r.text(450, y + 35, "the committed results file", size=21, anchor="middle")
r.text(
    450,
    y + 63,
    "cpu, jdk, kernel, rows, cache, MaxVectorSize,",
    size=15,
    anchor="middle",
    color="#5c5f66",
)
r.text(
    450,
    y + 85,
    "the probe's reading and the canary's, then the numbers",
    size=15,
    anchor="middle",
    color="#5c5f66",
)
r.note(
    60,
    y + 140,
    "and the quote check: every decimal in the docs traces to one of these"
    " files, or the build fails",
    size=17,
)

finish(r, "fig8-the-gates")
