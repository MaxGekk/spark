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

"""Figure 34: two queries, one class - the same projection run twice with a different integer.
Spark writes the integer into the generated source, so the two queries are two strings, two
cache keys, two Janino compiles and two classes for the JIT to compile. Varka's IR replaces the
integer with a slot, so both queries are one shape and one hash, and the second query reuses the
first one's loaded, already compiled class with a different value in slot 0. A diagram, no
measurements."""

from rough import Rough, finish

GREY = "#5c5f66"


def box(x, y, w, h, fill, label, size):
    r.rect(x, y, w, h, fill=fill)
    r.text(x + w / 2, y + h / 2, label, size=size, anchor="middle")


r = Rough(1000, 680, seed=3401)
r.text(40, 44, "Two queries, one class", size=32)
r.text(
    40,
    84,
    "the same projection, rerun by a dashboard with a new number",
    size=20,
    color=GREY,
)

QUERIES = [("date_add(d, 7), year(d)", 7), ("date_add(d, 30), year(d)", 30)]

# Spark: the literal goes into the text, so each query is its own class.
r.text(40, 140, "Spark", size=26, color="#e03131")
for i, (sql, k) in enumerate(QUERIES):
    y = 170 + i * 110
    box(40, y, 230, 76, "grey", sql, 19)
    r.arrow(276, y + 38, 316, y + 38)
    box(322, y, 250, 76, "blue", 'Java source with\n"... value = d + %d; ..."' % k, 18)
    r.arrow(578, y + 38, 618, y + 38)
    box(624, y, 150, 76, "yellow", "Janino:\nclass %d" % (i + 1), 19)
    r.arrow(780, y + 38, 820, y + 38)
    box(826, y, 140, 76, "orange", "JIT compiles\nclass %d" % (i + 1), 19)
r.note(322, 380, "a different string is a different cache key", size=19)

# Varka: the literal becomes a slot, so both queries are one shape.
r.text(40, 430, "Varka", size=26, color="#2f9e44")
for i, (sql, k) in enumerate(QUERIES):
    y = 456 + i * 80
    box(40, y, 230, 60, "grey", sql, 19)
    r.arrow(276, y + 30, 340, 520)
    r.text(282, y + (-8 if i == 0 else 70), "slot 0 = %d" % k, size=17, color=GREY)
box(346, 470, 250, 100, "violet", "one shape:\ndate_add(col 0, slot 0),\nyear(col 0)", 19)
r.arrow(602, 520, 642, 520)
box(648, 482, 150, 76, "green", "one class,\nloaded once", 19)
r.arrow(804, 520, 844, 520)
box(850, 482, 116, 76, "green", "compiled\nonce", 19)
r.note(346, 640, "the second query reuses the class and its JIT compile", size=19)

finish(r, "fig34-two-queries-one-class")
