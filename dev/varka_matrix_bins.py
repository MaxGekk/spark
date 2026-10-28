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
The fanned-out option matrix's bins (VARKA-507): every (configuration, module, suite) the matrix
runs, packed into CI jobs of about the same length.

A unit is one runner JVM: a suite that runs alone (`@own` in `dev/varka_matrix.sh
--list-suites`), or a group of a module's other suites under one configuration, cut so that no
group outweighs a lane. A bin is one CI job with two lanes (`--lanes`), two JVMs at a time on
the runner's four cores, and takes units longest first onto its lightest lane until every lane
reaches the target. The defaults are a configuration like any other, named `defaults`.

A unit's weight is its suites' seconds, from `sql/varka/matrix/suite-seconds.tsv` where a
suite is listed there (written by `times` from a fanned-out run's JUnit reports) and from the
pull-request jobs' `sql/varka/ci/suite-times.tsv` otherwise, times the configuration's weight
over the defaults (`sql/varka/matrix/seconds.tsv`), plus a JVM's start. A suite neither file
lists weighs the median, so a new suite is placed as an ordinary one.

  dev/varka_matrix_bins.py plan --configurations FILE --suites MODULE=FILE... \\
      [--target SECONDS] > bins.json          # the job matrix, {"include": [...]}
  dev/varka_matrix_bins.py times JUNIT_DIR... > suite-seconds.tsv
  dev/varka_matrix_bins.py --selftest

Each bin's `units` is one line a JVM, `configuration|module|suite,suite,...`, the suites as
simple names, which the bin's job hands to `dev/varka_matrix.sh --suites`.
"""

import argparse
import json
import re
import statistics
import sys
import unittest
from pathlib import Path

SUITE_TIMES = "sql/varka/ci/suite-times.tsv"
SUITE_SECONDS = "sql/varka/matrix/suite-seconds.tsv"
CONFIG_WEIGHTS = "sql/varka/matrix/seconds.tsv"
DEFAULTS = "defaults"
# A runner JVM's start and its first Spark session, before any suite's own time.
JVM_START = 20.0
LANES = 2


def read_tsv(path):
    """`name<TAB>number` lines, `#` comments and blank lines aside; empty for a missing file."""
    out = {}
    p = Path(path)
    if not p.exists():
        return out
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        name, value = line.split("\t")[:2]
        out[name] = float(value)
    return out


def read_suites(path):
    """A module's suites as `--list-suites` prints them: (simple name, runs alone)."""
    out = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        alone = line.startswith("@own ")
        name = line[len("@own ") :] if alone else line
        out.append((name.rsplit(".", 1)[-1], alone))
    return out


def suite_weight(suite_seconds, suite_times):
    """The seconds of a suite under the defaults, by the measured file, the PR file, the median."""
    known = list(suite_seconds.values()) or list(suite_times.values()) or [30.0]
    fill = statistics.median(known)

    def weight(suite):
        if suite in suite_seconds:
            return suite_seconds[suite]
        return suite_times.get(suite, fill)

    return weight


def units(configurations, suites, weight, config_weights, lane):
    """Every (configuration, module, suites, seconds) JVM the matrix runs, the defaults first."""
    fill = statistics.median(config_weights.values()) if config_weights else 1.0
    out = []
    for config in [DEFAULTS] + list(configurations):
        factor = 1.0 if config == DEFAULTS else config_weights.get(config, fill)
        for module, listed in suites:
            shared = []
            for name, alone in listed:
                seconds = weight(name) * factor
                if alone:
                    out.append((config, module, [name], JVM_START + seconds))
                else:
                    shared.append((name, seconds))
            # The shared suites in groups no heavier than a lane, longest first into the lightest.
            total = sum(s for _, s in shared)
            groups = max(1, int(-(-total // max(lane - JVM_START, 1.0))))
            loads = [[0.0, []] for _ in range(groups)]
            for name, seconds in sorted(shared, key=lambda u: (-u[1], u[0])):
                g = min(loads, key=lambda x: x[0])
                g[0] += seconds
                g[1].append(name)
            for load, names in loads:
                if names:
                    out.append((config, module, sorted(names), JVM_START + load))
    return out


def pack(all_units, lane, lanes=LANES):
    """Bins of `lanes` lanes each, units longest first onto the lightest lane, a bin closed when
    a unit would take its lightest lane past the target; a unit longer than a lane takes a bin."""
    bins = []
    for unit in sorted(all_units, key=lambda u: (-u[3], u[0], u[1], u[2])):
        placed = False
        for b in bins:
            loads = b["lanes"]
            k = min(range(lanes), key=lambda i: loads[i])
            if loads[k] + unit[3] <= lane:
                loads[k] += unit[3]
                b["units"].append(unit)
                placed = True
                break
        if not placed:
            bins.append({"lanes": [unit[3]] + [0.0] * (lanes - 1), "units": [unit]})
    return bins


def plan(configurations, suites, suite_seconds, suite_times, config_weights, lane, lanes=LANES):
    weight = suite_weight(suite_seconds, suite_times)
    bins = pack(units(configurations, suites, weight, config_weights, lane), lane, lanes)
    include = []
    for k, b in enumerate(bins):
        include.append(
            {
                "bin": f"b{k:03d}",
                "seconds": round(max(b["lanes"])),
                "units": "\n".join(f"{c}|{m}|{','.join(s)}" for c, m, s, _ in b["units"]),
            }
        )
    return {"include": include}


def times(junit_dirs):
    """Each suite's seconds under the defaults, the slowest of its runs, from JUnit reports
    under `<dir>/defaults/<module>-...`."""
    seen = {}
    for d in junit_dirs:
        for xml in Path(d).glob("**/defaults/*/junit/TEST-*.xml"):
            head = xml.read_text(encoding="utf-8", errors="replace")[:4000]
            m = re.search(r'<testsuite[^>]*\bname="([^"]+)"[^>]*\btime="([0-9.]+)"', head)
            if not m:
                m2 = re.search(r'<testsuite[^>]*\btime="([0-9.]+)"[^>]*\bname="([^"]+)"', head)
                if not m2:
                    continue
                name, seconds = m2.group(2), float(m2.group(1))
            else:
                name, seconds = m.group(1), float(m.group(2))
            simple = name.rsplit(".", 1)[-1]
            seen[simple] = max(seen.get(simple, 0.0), seconds)
    return seen


class SelfTest(unittest.TestCase):
    def test_every_unit_lands_once_and_no_lane_passes_the_target(self):
        suites = [
            ("sql", [("A", False), ("B", False), ("C", True)]),
            ("catalyst", [("D", False), ("E", True)]),
        ]
        weights = {"A": 100.0, "B": 50.0, "C": 300.0, "D": 10.0, "E": 30.0}
        configs = ["x=1", "y=2", "z=3"]
        out = plan(configs, suites, weights, {}, {"x=1": 2.0}, 700.0)["include"]
        lines = [line for b in out for line in b["units"].split("\n")]
        # Every configuration meets every suite exactly once, the defaults included.
        met = sorted(
            (c, s) for c, _, ss in (line.split("|") for line in lines) for s in ss.split(",")
        )
        expected = sorted((c, s) for c in ["defaults"] + configs for s in "ABCDE")
        self.assertEqual(met, expected)
        self.assertTrue(all(b["seconds"] <= 700 for b in out), out)

    def test_a_unit_longer_than_a_lane_takes_a_lane_of_its_own(self):
        bins = pack(
            [
                ("defaults", "sql", ["Big"], 920.0),
                ("defaults", "sql", ["S"], 25.0),
                ("x=1", "sql", ["S"], 25.0),
            ],
            300.0,
        )
        self.assertEqual(len(bins), 1)
        # The long unit fills one lane alone; the short ones share the other.
        self.assertEqual(sorted(bins[0]["lanes"]), [50.0, 920.0])

    def test_three_lanes_take_a_third_unit_before_a_new_bin(self):
        units = [("defaults", "sql", [s], 100.0) for s in "ABC"]
        self.assertEqual(len(pack(units, 100.0)), 2)
        self.assertEqual(len(pack(units, 100.0, lanes=3)), 1)

    def test_shared_suites_split_into_groups_no_heavier_than_a_lane(self):
        suites = [("sql", [(f"S{i}", False) for i in range(10)])]
        weights = {f"S{i}": 100.0 for i in range(10)}
        got = units([], suites, suite_weight(weights, {}), {}, 320.0)
        self.assertTrue(all(u[3] <= 320.0 + 100.0 for u in got), got)
        self.assertEqual(sorted(s for u in got for s in u[2]), sorted(weights))

    def test_a_suite_no_file_lists_weighs_the_median(self):
        w = suite_weight({}, {"A": 10.0, "B": 30.0, "C": 50.0})
        self.assertEqual(w("New"), 30.0)


def main(argv):
    if argv[:1] == ["--selftest"]:
        result = unittest.main(argv=[sys.argv[0]], exit=False, verbosity=0).result
        ok = result.wasSuccessful()
        print("varka_matrix_bins selftest: " + ("ok" if ok else "FAILED"))
        return 0 if ok else 1
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--configurations", required=True)
    p.add_argument("--suites", action="append", required=True, metavar="MODULE=FILE")
    p.add_argument(
        "--target",
        type=float,
        default=480.0,
        help="a lane's seconds on a fast runner (default 480)",
    )
    p.add_argument(
        "--lanes",
        type=int,
        default=LANES,
        help=f"JVMs a bin runs at a time (default {LANES})",
    )
    t = sub.add_parser("times")
    t.add_argument("dirs", nargs="+")
    args = parser.parse_args(argv)
    if args.cmd == "times":
        for name, seconds in sorted(times(args.dirs).items()):
            print(f"{name}\t{seconds:.1f}")
        return 0
    configurations = [
        c.strip()
        for c in Path(args.configurations).read_text(encoding="utf-8").splitlines()
        if c.strip()
    ]
    suites = []
    for entry in args.suites:
        module, path = entry.split("=", 1)
        suites.append((module, read_suites(path)))
    out = plan(
        configurations,
        suites,
        read_tsv(SUITE_SECONDS),
        read_tsv(SUITE_TIMES),
        read_tsv(CONFIG_WEIGHTS),
        args.target,
        args.lanes,
    )
    json.dump(out, sys.stdout)
    print()
    total = sum(b["seconds"] for b in out["include"])
    print(
        f"{len(out['include'])} bins, the longest {max(b['seconds'] for b in out['include'])} s,"
        f" {total} s in all",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
