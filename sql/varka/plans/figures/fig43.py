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

"""Figure 43: what removes the releases - the warm-up arm of Varka's composition fuzzer with
Varka's template warm-up off, with both trap limits raised, and as Varka ships, and in a
separate set of runs with the limits raised from inside the running JVM or the counts reset once
at 60 s. The upper block reads the 100-replay table of the replay script's committed run
(sql/varka/demo/trap_sums-laptop-jdk25-output.txt), the middle block its 400-composition
run, each arm with its own count of warm-ups from that arm's verdict line, and the lower block
the verdict line of each preferred-species run in the trap-reset spike's results
(sql/varka/spikes/trap-reset/results/). Each block has its own axis, since the counts are of
different things."""

import os
import re

from rough import Rough, finish

HERE = os.path.dirname(os.path.abspath(__file__))
REPLAY = os.path.join(HERE, "..", "..", "demo", "trap_sums-laptop-jdk25-output.txt")
SPIKE = os.path.join(HERE, "..", "..", "spikes", "trap-reset", "results")
GREY = "#5c5f66"
OFF, LIMITS, STOCK, RESET = "#ff8787", "#74c0fc", "#69db7c", "#b197fc"


def replay_tables(path):
    """[{arm: (jvms with a release, of, releases)}] per summary table, in file order."""
    head = re.compile(r"^arm\s+replays with a release")
    row = re.compile(r"^(defaults|trap limits|stock)\s+(\d+) of (\d+)\s+(\d+)\s*$")
    tables, current = [], None
    with open(path) as f:
        for line in f:
            line = line.rstrip()
            if head.match(line):
                current = {}
                tables.append(current)
            elif current is not None and (m := row.match(line)):
                current[m.group(1)] = (int(m.group(2)), int(m.group(3)), int(m.group(4)))
            else:
                current = None
    return tables


def warmups(line):
    """(deadline releases, warm-ups) from a verdict line naming COMPILED and RELEASED counts."""
    compiled = int(re.search(r"COMPILED (\d+)", line).group(1))
    m = re.search(r"RELEASED (\d+)", line)
    released = int(m.group(1)) if m else 0
    return released, compiled + released


def arm_warmups(path):
    """(compositions, {arm: (deadline releases, warm-ups)}) of the one-replay compositions run,
    from its per-replay lines, which end with the replay's verdicts, and its summary line."""
    line = re.compile(r"^(defaults|trap limits|stock)\s+replay\s+(\d+): .*\((COMPILED \d+.*)\)$")
    head = re.compile(r"^seed \d+, compositions 0 to (\d+), (\d+) replays per arm")
    out, count = {}, None
    with open(path) as f:
        for text in f:
            if m := line.match(text.rstrip()):
                assert m.group(2) == "1" and "before the deadline" not in m.group(3), text
                out[m.group(1)] = warmups(m.group(3))
            elif m := head.match(text):
                assert m.group(2) == "1", text
                count = int(m.group(1)) + 1
    assert count, "no compositions run in the replay output"
    return count, out


def spike_runs(name, runs):
    """[(deadline releases, warm-ups)] of the named runs in one results file."""
    out, run = {}, None
    with open(os.path.join(SPIKE, name)) as f:
        for line in f:
            if m := re.match(r"^== run (\w+):", line):
                run = m.group(1)
            elif line.startswith("+ warm-up verdicts:") and run in runs:
                assert "of the releases 0 before the deadline" in line, line
                out[run] = warmups(line)
    assert sorted(out) == sorted(runs), (name, out)
    return [out[k] for k in runs]


tables = replay_tables(REPLAY)
assert len(tables) == 2, tables
jvms, comps = tables
assert all(of == 100 for _, of, _ in jvms.values()), jvms
COMPOSITIONS, arms = arm_warmups(REPLAY)
assert sorted(arms) == sorted(comps), (arms, comps)
assert all(arms[a][0] == comps[a][2] for a in arms), (arms, comps)
spike = [
    ("off, no action", OFF, spike_runs("control.txt", ["ctl1", "ctl2"])),
    ("off, limits raised (WhiteBox)", LIMITS, spike_runs("flags.txt", ["flags1", "flags2"])),
    ("off, one reset at 60 s", RESET, spike_runs("redefine-once.txt", ["redef1", "redef2"])),
]
# The spike's runs are over the same compositions, so they warm as many kernels as each arm.
counts = {n for _, _, runs in spike for _, n in runs} | {n for _, n in arms.values()}
assert len(counts) == 1, (spike, arms)
(WARMUPS,) = counts
spike = [(label, fill, [v for v, _ in runs]) for label, fill, runs in spike]
ARMS = [
    ("template warm-up off", OFF, "defaults"),
    ("off, both limits raised", LIMITS, "trap limits"),
    ("Varka as shipped", STOCK, "stock"),
]

r = Rough(1000, 735, seed=4301)
r.text(40, 44, "What removes the releases", size=32)
r.text(
    40,
    84,
    "Varka's warm-up test, JDK 25, 512-bit vectors, on a laptop; each block has its own axis",
    size=18,
    color=GREY,
)

X0, X1, LABEL = 330, 900, 312


def block(y, title, rows, top, step):
    """Titled bars with their own axis from 0 to `top`; returns the y below the axis."""

    def px(v):
        return X0 + (X1 - X0) * v / top

    r.text(40, y, title, size=20)
    y += 30
    for label, fill, values in rows:
        r.text(LABEL, y + 12, label, size=17, anchor="end")
        for v in values:
            # A value is a count, or a count with the text to print beside its bar.
            v, text = v if isinstance(v, tuple) else (v, "%d" % v)
            if v:
                r.rect(X0, y, px(v) - X0, 20, fill=fill, solid=True)
            r.text(px(v) + 10, y + 11, text, size=17)
            y += 25
        y += 5
    r.arrow(X0, y, X1 + 20, y)
    for v in range(0, top + 1, step):
        r.line(px(v), y - 6, px(v), y + 6)
        r.text(px(v), y + 22, "%d" % v, size=15, anchor="middle", color=GREY)
    return y + 56


y = block(
    136,
    "100 fresh JVMs per arm, each running one earlier kernel, then the allocating one: "
    "JVMs that released",
    [(label, fill, [jvms[arm][0]]) for label, fill, arm in ARMS],
    100,
    20,
)
y = block(
    y,
    "one JVM per arm through %d compositions: kernels released at the deadline, of its warm-ups"
    % COMPOSITIONS,
    [(label, fill, [(arms[arm][0], "%d of %d" % arms[arm])]) for label, fill, arm in ARMS],
    80,
    20,
)
block(
    y,
    "a second set of runs over the same compositions (%d warm-ups), the warm-up off, two JVMs "
    "per arm" % WARMUPS,
    spike,
    80,
    20,
)

finish(r, "fig43-what-removes-the-releases")
