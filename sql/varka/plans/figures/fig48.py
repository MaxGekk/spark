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


"""Figure 48: what Spark's own SQL tests ask of Varka - Spark's golden-file suite run with Varka
on, every relation cached as Arrow so Varka can see it. Above, the share of test files in which
any query ran a Varka kernel and the share of query executions it served; below, the files where
it served the most. Read from `sql/varka/spark_suites/served.json`, the served report the suite
writes, so the figure cannot drift from it."""

import json
import os

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
SERVED = os.path.join(HERE, "..", "..", "spark_suites", "served.json")
GREY = "#5c5f66"
SERVED_FILL, REST = "#4dabf7", "#e9ecef"
TOP_FILES = 8

with open(SERVED) as f:
    d = json.load(f)
tests = d["tests"]
total = d["total"]
assert total["tests"] == len(tests), (total["tests"], len(tests))
touched = sum(1 for v in tests.values() if v["served"])
# A file's analyzer-only twin ("<file>_analyzer_test") repeats its queries; show each file once.
top = sorted(
    ((k, v) for k, v in tests.items() if not k.endswith("_analyzer_test")),
    key=lambda kv: (-kv[1]["served"], kv[0]),
)[:TOP_FILES]

r = Rough(1000, 680, seed=4801)
r.text(40, 44, "What Spark's own SQL tests ask of Varka", size=32)
r.text(
    40,
    84,
    "Spark's golden-file suite, Varka on: the same results as Varka off, "
    "and this much run by Varka",
    size=20,
    color=GREY,
)

X0, X1, LABEL = 380, 830, 360


def bar(y, part, whole, label, value):
    r.rect(X0, y, X1 - X0, 30, fill=REST, solid=True)
    r.rect(X0, y, max((X1 - X0) * part / whole, 3), 30, fill=SERVED_FILL, solid=True)
    r.text(LABEL, y + 15, label, size=19, anchor="end")
    r.text(X1 + 10, y + 15, value, size=17)


bar(
    130,
    touched,
    total["tests"],
    "test files where Varka ran",
    "%d of %d" % (touched, total["tests"]),
)
bar(
    180,
    total["served"],
    total["executions"],
    "query executions Varka served",
    "%d of %d" % (total["served"], total["executions"]),
)

r.text(40, 270, "the files where Varka served the most", size=22)
y = 310
widest = max(v["executions"] for _, v in top)
for name, v in top:
    w = (X1 - X0) * v["executions"] / widest
    r.rect(X0, y, w, 22, fill=REST, solid=True)
    r.rect(X0, y, max((X1 - X0) * v["served"] / widest, 3), 22, fill=SERVED_FILL, solid=True)
    r.text(LABEL, y + 11, name, size=16, anchor="end")
    r.text(X0 + w + 10, y + 11, "%d of %d" % (v["served"], v["executions"]), size=16)
    y += 36

y += 14
r.rect(X0, y, 22, 18, fill=SERVED_FILL, solid=True)
r.text(X0 + 32, y + 10, "Varka computed it", size=17)
r.rect(X0 + 220, y, 22, 18, fill=REST, solid=True)
r.text(X0 + 252, y + 10, "Spark computed it", size=17)

finish(r, "fig48-what-sparks-own-tests-ask")
