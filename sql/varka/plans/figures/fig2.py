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

"""Figure 2: what hour(t) costs stock Spark - a LocalTime built per row to read one field -
against the one division Varka's lane does."""

from rough import Rough, finish

r = Rough(900, 720, seed=11)

# The value both paths start from.
r.rect(30, 320, 240, 64, fill="blue", label="t = 52 349 000 000 000", size=20)
r.text(150, 410, "nanoseconds since\nmidnight, one long", size=18, anchor="middle")

# Stock path.
r.text(310, 60, "stock Spark", size=24)
r.curve([(270, 335), (290, 200), (305, 170)], arrow=True)
r.rect(310, 90, 320, 160, fill="yellow")
r.text(470, 118, "LocalTime.ofNanoOfDay(t)", size=21, anchor="middle")
for i, ln in enumerate(["hour = 14", "minute = 32", "second = 29", "nano = 0"]):
    r.text(340 + (i % 2) * 150, 160 + (i // 2) * 34, ln, size=20)
r.text(470, 232, "a new object", size=16, anchor="middle", color="#868e96")
r.arrow(635, 170, 665, 170)
r.rect(670, 140, 130, 60, fill="grey", label=".getHour()", size=19)
r.arrow(805, 170, 830, 170)
r.rect(830, 140, 60, 60, fill="green", label="14", size=24)
r.note(310, 280, "built, read once, thrown away:\n16.3 ns a row", size=21)

# Varka path.
r.text(310, 470, "Varka", size=24)
r.curve([(270, 370), (290, 500), (305, 530)], arrow=True)
r.rect(310, 500, 320, 64, fill="green", label="t / 3 600 000 000 000", size=21)
r.arrow(635, 532, 830, 532)
r.rect(830, 502, 60, 60, fill="green", label="14", size=24)
r.note(310, 600, "one 64-bit division, 8 lanes at a time:\n1.0 ns a row, and no object", size=21)
r.text(
    450,
    690,
    "same answer; one path allocates, the other divides",
    size=18,
    anchor="middle",
    color="#868e96",
)

finish(r, "fig2-localtime-per-row")
