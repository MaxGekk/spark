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

"""Figure 38: two paths for a value - how Spark's generated code carries a literal, and why
one path recompiles. An integer, long, double, date or timestamp is written into the Java
source as text, so a new value is a new source, a new cache key and a new class; a string,
decimal or binary value goes into the class's references array, so the source is the same and
the cache hits. A named parameter is substituted with a literal before planning and takes the
literal's path. The class counts on each path are read from the committed local-mode output
of literals.scala (sql/varka/demo/compiles/literals-jdk25-output.txt), so the figure cannot
drift from it."""

import os
import re

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
OUTPUT = os.path.join(HERE, "..", "..", "demo", "compiles", "literals-jdk25-output.txt")
GREY = "#5c5f66"


def counts(path):
    """{query label: classes compiled} from the literals table."""
    row = re.compile(r"^(\S.*?)\s{2,}(\d+)(\s|$)")
    out = {}
    with open(path) as f:
        for line in f:
            if (m := row.match(line.rstrip())) and not line.startswith("query"):
                out[m.group(1).strip()] = int(m.group(2))
    return out


c = counts(OUTPUT)
written = {c[k] for k in ("int, new value", "bigint, new value", "date, new value")}
passed = {c[k] for k in ("string, new value", "decimal, new value")}
param = c["int parameter, new"]
assert len(written) == 1 and len(passed) == 1, (written, passed)
written, passed = written.pop(), passed.pop()


def box(x, y, w, h, fill, label, size):
    r.rect(x, y, w, h, fill=fill)
    r.text(x + w / 2, y + h / 2, label, size=size, anchor="middle")


r = Rough(1000, 570, seed=3801)
r.text(40, 44, "Two paths for a value", size=32)
r.text(
    40,
    84,
    "how a WHERE clause's value reaches the generated class, after the same query with i = 5",
    size=19,
    color=GREY,
)

# The literal's path: written into the text.
r.text(40, 140, "WHERE i = 6", size=24)
r.text(40, 168, "int, bigint, double,\ndate, timestamp", size=17, color=GREY)
r.arrow(210, 150, 250, 150)
box(256, 112, 250, 78, "red", 'written into the source:\n"... value == 6; ..."', 18)
r.arrow(512, 150, 552, 150)
box(558, 112, 170, 78, "red", "a new text:\na new cache key", 18)
r.arrow(734, 150, 774, 150)
box(780, 112, 180, 78, "red", "%d classes\ncompiled again" % written, 19)

# The parameter joins it before planning.
r.text(40, 262, "WHERE i = :p", size=24)
r.text(40, 290, "a named parameter", size=17, color=GREY)
r.curve([(230, 268), (300, 240), (360, 196)], arrow=True)
r.text(
    300,
    280,
    "substituted with a literal before planning: %d classes" % param,
    size=17,
    color=GREY,
)

# The reference's path: passed to the class.
r.text(40, 400, "WHERE s = '6'", size=24)
r.text(40, 428, "string, decimal,\nbinary", size=17, color=GREY)
r.arrow(210, 410, 250, 410)
box(256, 372, 250, 78, "green", "passed in references[ ];\nthe source only reads it", 18)
r.arrow(512, 410, 552, 410)
box(558, 372, 170, 78, "green", "the same text:\na cache hit", 18)
r.arrow(734, 410, 774, 410)
box(780, 372, 180, 78, "green", "%d classes" % passed, 19)

r.note(
    40,
    530,
    "Spark caches classes by their source text, so only the first path compiles again",
    size=20,
)
finish(r, "fig38-two-paths-for-a-value")
