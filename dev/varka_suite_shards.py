#!/usr/bin/env python3
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
"""Spread suites over N JVMs, longest first (VARKA-286.md 3.1), or over N CI jobs.

    dev/varka_suite_shards.py N WEIGHTS_DIR SUITE...     # one line per JVM, suites space separated
    dev/varka_suite_shards.py --job I/N WEIGHTS_FILE SUITE...   # CI job I's suites, one a line
    dev/varka_suite_shards.py --update-weights WEIGHTS_FILE JUNIT_DIR...
    dev/varka_suite_shards.py --self-test

Each suite's weight is its time in the JUnit reports under WEIGHTS_DIR (any depth, the files
ScalaTest's `-u` writes), and the median of the known weights for a suite that has none, so a new
suite is placed as an ordinary one. The heaviest unplaced suite goes to the lightest JVM, which
keeps the longest JVM near the longest suite when one suite dominates. Empty JVMs are not
printed.

`--job` splits a module's suites over the CI jobs that run them, the same way, with the weights
read from a committed file (`sql/varka/ci/suite-times.tsv`, a simple suite name and its seconds a
line) rather than from a run's reports. Each job computes its own share, so every job must see the
same weights: a committed file is the same in every job of a run, where a restored cache need not
be. A suite the file lacks takes the median, so a stale file costs balance and never coverage.
`--update-weights` writes the file from the JUnit reports of a run of every suite.
"""

import heapq
import statistics
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def weights(root):
    """Seconds per suite (by simple name) from the JUnit reports under `root`."""
    times = {}
    for path in Path(root).rglob("*.xml") if root and Path(root).exists() else []:
        try:
            suite = ET.parse(path).getroot()
        except ET.ParseError:
            continue
        name = suite.get("name", "").split(".")[-1]
        if name and suite.get("time"):
            times[name] = max(times.get(name, 0.0), float(suite.get("time")))
    return times


def place(count, suites, times):
    """Suites over `count` bins, heaviest first onto the lightest; a bin may be empty."""
    known = list(times.values())
    default = statistics.median(known) if known else 1.0
    weighted = sorted(suites, key=lambda s: (-times.get(s.split(".")[-1], default), s))
    heap = [(0.0, k) for k in range(count)]
    placed = [[] for _ in range(count)]
    for suite in weighted:
        load, k = heapq.heappop(heap)
        placed[k].append(suite)
        heapq.heappush(heap, (load + times.get(suite.split(".")[-1], default), k))
    return placed


def shards(count, suites, times):
    """Suites (fully qualified) over `count` JVMs, heaviest first onto the lightest.

    >>> shards(2, ["p.A", "p.B", "p.C"], {"A": 10.0, "B": 6.0, "C": 5.0})
    [['p.A'], ['p.B', 'p.C']]
    >>> shards(3, ["p.A"], {})
    [['p.A']]
    >>> shards(2, ["p.A", "p.New"], {"A": 4.0})
    [['p.A'], ['p.New']]
    """
    return [p for p in place(count, suites, times) if p]


def job_shard(index, count, suites, times):
    """Job `index` of `count`: the suites the same placement gives it, empty when it has none.

    >>> job_shard(1, 3, ["p.A", "p.B", "p.C", "p.D"], {"A": 9.0, "B": 5.0, "C": 4.0, "D": 1.0})
    ['p.B']
    >>> job_shard(2, 3, ["p.A", "p.B", "p.C", "p.D"], {"A": 9.0, "B": 5.0, "C": 4.0, "D": 1.0})
    ['p.C', 'p.D']
    >>> job_shard(1, 2, ["p.A"], {})
    []
    """
    return place(count, suites, times)[index]


def read_weights(path):
    """Seconds per simple suite name from a weights file; # lines and blank lines are skipped."""
    times = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                name, secs = line.split("\t")
                times[name] = float(secs)
    return times


def write_weights(path, times):
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            "# Seconds per Varka suite, the weights CI splits a module's suites over its jobs by\n"
            "# (dev/varka_suite_shards.py --job). Written by --update-weights from a run's JUnit\n"
            "# reports; a suite missing here takes the median.\n"
        )
        for name in sorted(times):
            f.write(f"{name}\t{times[name]:.1f}\n")


def main(argv):
    if argv == ["--self-test"]:
        import doctest

        failed, _ = doctest.testmod()
        return 1 if failed else 0
    if len(argv) >= 3 and argv[0] == "--job":
        index, count = (int(x) for x in argv[1].split("/"))
        if not 0 <= index < count:
            print(f"--job {argv[1]}: the index counts from zero, below the count", file=sys.stderr)
            return 2
        for suite in job_shard(index, count, argv[3:], read_weights(argv[2])):
            print(suite)
        return 0
    if len(argv) >= 3 and argv[0] == "--update-weights":
        times = read_weights(argv[1]) if Path(argv[1]).exists() else {}
        for d in argv[2:]:
            times.update(weights(d))
        # A report names fixtures and censuses too (a nested `Suite$Fixture`, a `...Census`);
        # only the suites a job is split by belong, or they drag the median a new suite takes.
        times = {k: v for k, v in times.items() if k.endswith("Suite") and "$" not in k}
        write_weights(argv[1], times)
        return 0
    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    for shard in shards(int(argv[0]), argv[2:], weights(argv[1])):
        print(" ".join(shard))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
