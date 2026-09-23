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

"""Figure 7: the four arms every table compares - two stock releases on two JDKs, the fork with
the engine off, the fork with it on - over one cached table, with one benchmark jar."""

from rough import Rough, finish

r = Rough(900, 700, seed=53)
r.rect(150, 40, 600, 70, fill="blue")
r.text(450, 62, "varka_times: the same 500 million rows for every arm", size=21, anchor="middle")
r.text(
    450,
    92,
    "one core; cached 22.8 GiB as Arrow, 16.6 GiB as Spark's own format",
    size=17,
    anchor="middle",
)
arms = [
    ("stock Spark\n4.2.0, JDK 17", "yellow"),
    ("stock Spark\n4.2.0, JDK 25", "yellow"),
    ("this fork,\nengine off", "grey"),
    ("this fork,\nVarka on", "green"),
]
for i, (lbl, fill) in enumerate(arms):
    x = 40 + i * 210
    r.arrow(450 if i in (1, 2) else (300 if i == 0 else 600), 115, x + 95, 175)
    r.rect(x, 180, 190, 90, fill=fill, label=lbl, size=20)
    r.arrow(x + 95, 275, x + 95, 320)
    r.rect(x, 325, 190, 60, fill="white", label="results file", size=18)
r.text(
    450,
    420,
    "one benchmark jar, built against no part of the fork, runs all four",
    size=18,
    anchor="middle",
    color="#868e96",
)

# What each neighbouring pair isolates.
r.line(135, 470, 345, 470)
r.text(240, 500, "the JVM's share:\nJDK 17 to 25", size=18, anchor="middle")
r.line(345, 560, 555, 560)
r.text(450, 590, "the fork's share: it tracks\nSpark master, not 4.2.0", size=18, anchor="middle")
r.line(555, 470, 765, 470)
r.note(590, 500, "the engine's share:\nthe number that counts", size=19)
r.text(
    450,
    670,
    "a ratio is quoted against stock, and against the fork with the engine off when they differ",
    size=16,
    anchor="middle",
    color="#868e96",
)

finish(r, "fig7-four-arms")
