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

"""Figure 35: nine numbers on a query's way to machine code - the six steps a Spark SQL stage
takes from the plan to running rows (the planner's whole-stage check, the generated source,
Janino, the class cache, the JIT, the generated hash map), with each of the nine limits of the
codegen-numbers post placed under the step where it acts, and what happens past it. Red marks
the three a query can fall off without an error (a slower path, silently); yellow the rest.
A diagram; the defaults are Spark 4.2.0's and HotSpot's, read from source for the post."""

from rough import Rough, finish

GREY = "#5c5f66"

STEPS = [
    ("plan", "whole-stage\ncheck"),
    ("source", "Java text,\nsplit in methods"),
    ("Janino", "source to\nbytecode"),
    ("cache", "one class per\ndistinct source"),
    ("JIT", "bytecode to\nmachine code"),
    ("run", "rows through\nthe method"),
]
# (step index, number, what it bounds, what happens past it, cliff?)
LIMITS = [
    (0, "100", "fields per operator", "it leaves the stage", True),
    (1, "1024", "characters a method", "a new method", False),
    (1, "255", "parameter slots", "no split at all", False),
    (2, "65,535", "bytes a method, and\npool entries", "no class", False),
    (2, "65535", "Spark's own size\ncheck's default", "never trips", False),
    (3, "100", "classes per JVM", "evict, compile again", False),
    (4, "8000", "bytes a method", "interpreted, 6x slower", True),
    (4, "8000", "bytes inlined per\ncompile", "callees stay calls", True),
    (5, "16 bits", "of the fast hash\nmap's capacity", "the regular map", False),
]

r = Rough(1000, 650, seed=3501)
r.text(40, 44, "Nine numbers on the way to machine code", size=32)
r.text(
    40,
    84,
    "where each limit acts on a Spark SQL stage, and what happens past it; Spark 4.2.0 defaults",
    size=19,
    color=GREY,
)

X0, COL, W = 30, 160, 146
TOP, H = 120, 84
for i, (name, what) in enumerate(STEPS):
    x = X0 + i * COL
    r.rect(x, TOP, W, H, fill="blue")
    r.text(x + W / 2, TOP + 22, name, size=22, anchor="middle")
    r.text(x + W / 2, TOP + 58, what, size=16, anchor="middle")
    if i < len(STEPS) - 1:
        r.arrow(x + W + 2, TOP + H / 2, x + COL - 2, TOP + H / 2)

TAG_H, GAP = 150, 18
slot = {}
for step, number, bounds, past, cliff in LIMITS:
    k = slot.get(step, 0)
    slot[step] = k + 1
    x = X0 + step * COL
    y = TOP + H + 34 + k * (TAG_H + GAP)
    r.line(x + W / 2, y - 30 if k == 0 else y - GAP, x + W / 2, y, color=GREY, width=1.2)
    r.rect(x, y, W, TAG_H, fill="red" if cliff else "yellow")
    r.text(x + W / 2, y + 26, number, size=28, anchor="middle")
    r.text(x + W / 2, y + 72, bounds, size=16, anchor="middle")
    r.text(x + W / 2, y + 122, "past it:\n" + past, size=15, anchor="middle", color=GREY)

r.note(
    40,
    610,
    "red: the query takes a slower path with no error, and mostly without a log line",
    size=19,
)
finish(r, "fig35-nine-numbers-on-the-way")
