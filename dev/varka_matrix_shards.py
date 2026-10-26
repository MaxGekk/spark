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
"""
The option matrix's configurations split over its CI jobs by measured time (VARKA-493).

A shard of the weekly matrix is one GitHub-hosted job, and the matrix takes as long as its
slowest shard. Kept by position (I, I + N, ...), three shards ran five configurations and nine
ran four, and the slowest configuration landed wherever its position put it. This splits them
longest first instead: each configuration, in decreasing order of weight, goes to the shard with
the least weight so far (ties to the lower shard), which given the same weights and
configurations is the same split on every job.

A configuration's weight is its seconds divided by the defaults run of the same shard, so a
runner's speed, which differs between jobs, cancels out. `sql/varka/matrix/seconds.tsv` holds
them, written by `times` from the reports of a run's shards. A configuration it does not list
weighs the median of those it does.

  dev/varka_matrix_shards.py split I/N WEIGHTS < configurations   # the configurations of shard I
  dev/varka_matrix_shards.py times REPORT... > WEIGHTS            # weights from shard reports
  dev/varka_matrix_shards.py --selftest
"""

import statistics
import sys


def read_weights(path):
    weights = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            if not line.strip() or line.startswith("#"):
                continue
            name, weight = line.rstrip("\n").split("\t")
            weights[name] = float(weight)
    return weights


def split(configurations, count, weights):
    """The configurations of each of `count` shards, each list in the input's order."""
    fill = statistics.median(weights.values()) if weights else 1.0
    order = sorted(
        range(len(configurations)),
        key=lambda k: (-weights.get(configurations[k], fill), k),
    )
    loads = [0.0] * count
    owner = {}
    for k in order:
        shard = min(range(count), key=lambda s: (loads[s], s))
        owner[k] = shard
        loads[shard] += weights.get(configurations[k], fill)
    return [[c for k, c in enumerate(configurations) if owner[k] == s] for s in range(count)]


def report_rows(path):
    """(configuration, seconds) for each row of a `varka_matrix.sh` report's summary table that
    ran to completion: a configuration that failed or was cut short has no whole time to weigh."""
    rows = []
    with open(path, encoding="utf-8") as f:
        lines = f.read().splitlines()
    try:
        start = next(i for i, line in enumerate(lines) if line.startswith("configuration "))
    except StopIteration:
        return rows
    for line in lines[start + 1 :]:
        parts = line.split()
        if len(parts) < 3 or not parts[1].isdigit() or not parts[2].isdigit():
            break
        if parts[1] == "0" and int(parts[2]) > 0:
            rows.append((parts[0], int(parts[2])))
    return rows


def times(reports):
    """Each configuration's seconds over its shard's defaults run, from every report."""
    weights = {}
    for path in reports:
        rows = dict(report_rows(path))
        defaults = rows.pop("defaults", 0)
        if defaults <= 0:
            print(
                f"varka_matrix_shards: {path} has no defaults run; its rows are skipped",
                file=sys.stderr,
            )
            continue
        for name, seconds in rows.items():
            weights[name] = round(seconds / defaults, 3)
    return weights


def selftest():
    fails = 0

    def check(name, got, want):
        nonlocal fails
        if got != want:
            print(f"varka_matrix_shards selftest: {name}: expected {want}, got {got}")
            fails += 1

    configs = ["a", "b", "c", "d", "e"]
    weights = {"a": 1.0, "b": 1.0, "c": 3.0, "d": 1.0, "e": 1.0}
    shards = split(configs, 2, weights)
    # The heaviest goes first, alone on shard 0; the four light ones fill shard 1 to 3.0 first,
    # then the last one joins whichever shard is lighter.
    check("the heavy configuration is alone", shards[0][0], "c")
    check("every configuration in one shard", sorted(sum(shards, [])), configs)
    check("the loads", sorted(sum(weights[c] for c in s) for s in shards), [3.0, 4.0])
    check("the same split again", split(configs, 2, weights), shards)
    # The median of {3.0} is 3.0, so `x` ties `c` and goes first by position.
    check(
        "an unlisted configuration takes the median",
        split(["x", "c"], 2, {"c": 3.0}),
        [["x"], ["c"]],
    )
    check("more shards than configurations", split(["a"], 3, weights), [["a"], [], []])
    report = [
        "== 2 configuration(s) and the defaults",
        "configuration                    exit  secs    ok fail canc abort unfused",
        "defaults                            0   800  2143    0   62     0       0",
        "fast=true                           0   400  2048    0  157     0       0",
        "slow=true                           0  1600  2048    0  157     0       0",
        "",
        "after the table",
    ]
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
        f.write("\n".join(report) + "\n")
    check(
        "weights over the shard's defaults", times([f.name]), {"fast=true": 0.5, "slow=true": 2.0}
    )
    if fails == 0:
        print("varka_matrix_shards selftest: ok")
    return fails


def main(argv):
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0 if argv else 2
    if argv[0] == "--selftest":
        return selftest()
    if argv[0] == "split":
        index, count = (int(x) for x in argv[1].split("/"))
        if count < 1 or not 0 <= index < count:
            print(
                f"varka_matrix_shards: shard {argv[1]} is not I/N with 0 <= I < N", file=sys.stderr
            )
            return 2
        weights = read_weights(argv[2])
        configurations = [line.strip() for line in sys.stdin if line.strip()]
        for c in split(configurations, count, weights)[index]:
            print(c)
        return 0
    if argv[0] == "times":
        print("# configuration\tseconds over the shard's defaults run (dev/varka_matrix_shards.py)")
        for name, weight in sorted(times(argv[1:]).items()):
            print(f"{name}\t{weight}")
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
