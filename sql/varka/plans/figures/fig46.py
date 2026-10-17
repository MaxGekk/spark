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


"""Figure 46: the proof checks, file by file - the number of solver queries (`(check-sat)`) in
each committed file under `sql/varka/proofs/`, counted from the files themselves, with what each
file proves in plain words. A query is a proof when the solver answers `unsat` (the claim's
negation has no model) and a guard against a vacuous proof when it must answer `sat`; the
figure counts both. A proof file added or removed fails the assertion below until it has a
label."""

import glob
import os

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
PROOFS = os.path.join(HERE, "..", "..", "proofs")
GREY = "#5c5f66"

LABELS = {
    "long_divide.smt2": "64-bit division by a constant",
    "chrono_divide.smt2": "every calendar division",
    "refinement.smt2": "each expression against the\nstandard and Spark's departures",
    "range_analysis.smt2": "the range analysis's arithmetic",
    "value_range.smt2": "the value-range rules",
    "range_calendar.smt2": "the range analysis's calendar",
    "time_bounds.smt2": "the TIME type's bounds",
    "int_mulhi_divide.smt2": "32-bit division by a constant",
    "floor_mod7.smt2": "the day of the week (mod 7)",
    "java_check.smt2": "Java's operators, held to the JVM",
    "leap_hash.smt2": "the leap-year hash",
}

counts = {}
for path in glob.glob(os.path.join(PROOFS, "*.smt2")):
    with open(path) as f:
        n = f.read().count("(check-sat)")
    if n:
        counts[os.path.basename(path)] = n
assert set(counts) == set(LABELS), sorted(set(counts) ^ set(LABELS))
rows = sorted(counts.items(), key=lambda kv: -kv[1])
total = sum(counts.values())

r = Rough(1000, 680, seed=4601)
r.text(40, 44, "What the solvers prove", size=32)
r.text(
    40,
    84,
    "solver queries per proof file, %d in all; the nightly runs every one under Z3 and cvc5"
    % total,
    size=20,
    color=GREY,
)

X0, X1, TOP = 400, 900, max(counts.values())


def px(v):
    return X0 + (X1 - X0) * v / TOP


y = 130
for name, n in rows:
    h = 44 if "\n" in LABELS[name] else 36
    r.rect(X0, y + h / 2 - 11, max(px(n) - X0, 3), 22, fill="blue", solid=True)
    r.text(px(n) + 10, y + h / 2, "%d" % n, size=18)
    r.text(X0 - 14, y + h / 2, LABELS[name], size=18, anchor="end")
    y += h + 10
r.line(X0, 120, X0, y)

finish(r, "fig46-what-the-solvers-prove")
