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
"""Spread suites over N JVMs, longest first (VARKA-286.md 3.1).

    dev/varka_suite_shards.py N WEIGHTS_DIR SUITE...     # one line per JVM, suites space separated
    dev/varka_suite_shards.py --self-test

Each suite's weight is its time in the JUnit reports under WEIGHTS_DIR (any depth, the files
ScalaTest's `-u` writes), and the median of the known weights for a suite that has none, so a new
suite is placed as an ordinary one. The heaviest unplaced suite goes to the lightest JVM, which
keeps the longest JVM near the longest suite when one suite dominates. Empty JVMs are not
printed.
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


def shards(count, suites, times):
    """Suites (fully qualified) over `count` JVMs, heaviest first onto the lightest.

    >>> shards(2, ["p.A", "p.B", "p.C"], {"A": 10.0, "B": 6.0, "C": 5.0})
    [['p.A'], ['p.B', 'p.C']]
    >>> shards(3, ["p.A"], {})
    [['p.A']]
    >>> shards(2, ["p.A", "p.New"], {"A": 4.0})
    [['p.A'], ['p.New']]
    """
    known = list(times.values())
    default = statistics.median(known) if known else 1.0
    weighted = sorted(suites, key=lambda s: (-times.get(s.split(".")[-1], default), s))
    heap = [(0.0, k) for k in range(count)]
    placed = [[] for _ in range(count)]
    for suite in weighted:
        load, k = heapq.heappop(heap)
        placed[k].append(suite)
        heapq.heappush(heap, (load + times.get(suite.split(".")[-1], default), k))
    return [p for p in placed if p]


def main(argv):
    if argv == ["--self-test"]:
        import doctest

        failed, _ = doctest.testmod()
        return 1 if failed else 0
    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    for shard in shards(int(argv[0]), argv[2:], weights(argv[1])):
        print(" ".join(shard))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
