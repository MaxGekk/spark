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
group outweighs a lane. A bin is one CI job with two lanes, two JVMs at a time on the runner's
four cores, and takes units longest first onto its lighter lane until both lanes reach the
target. The defaults are a configuration like any other, named `defaults`.

A unit's weight is its suites' seconds, from `sql/varka/matrix/suite-seconds.tsv` where a
suite is listed there (written by `times` from a fanned-out run's JUnit reports, each suite's
median over the configurations) and from the pull-request jobs' `sql/varka/ci/suite-times.tsv`
otherwise, times the configuration's weight over the defaults (`sql/varka/matrix/seconds.tsv`),
plus a JVM's start. A suite neither file lists weighs the median, so a new suite is placed as
an ordinary one.

  dev/varka_matrix_bins.py plan --configurations FILE --suites MODULE=FILE... \\
      [--target SECONDS] > bins.json          # the job matrix, {"include": [...]}
  dev/varka_matrix_bins.py times [--reach FILE] JUNIT_DIR... > suite-seconds.tsv
  dev/varka_matrix_bins.py --selftest

Each bin's `units` is one line a JVM, `configuration|module|suite,suite,...`, the suites as
simple names, which the bin's job hands to `dev/varka_matrix.sh --suites`.
"""

import argparse
import json
import re
import statistics
import sys
import tempfile
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


def read_reach(path):
    """`VarkaTestReach`'s record: the configurations it covers, from its `#<TAB>covers<TAB>`
    lines, and for each suite the configurations that move each of its tests, with the suite's
    set-up line (`suite<TAB>*`) and the JVM's (`*<TAB>*`) joined to every test they cover. None
    for a record its recording marked broken."""
    covered, tests, setup, everywhere = set(), {}, {}, set()
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if line.startswith("#"):
            if len(parts) >= 2 and parts[1] == "broken":
                return None
            if len(parts) == 3 and parts[1] == "covers":
                covered |= {c for c in parts[2].split(",") if c}
            continue
        if len(parts) < 3:
            continue
        moved = set() if parts[-1] == "-" else set(parts[-1].split(","))
        suite, test = parts[0], "\t".join(parts[1:-1])
        if suite == "*":
            everywhere |= moved
        elif test == "*":
            setup.setdefault(suite, set()).update(moved)
        else:
            tests.setdefault(suite, []).append(moved)
    joined = {
        suite: [m | setup.get(suite, set()) | everywhere for m in moved]
        for suite, moved in tests.items()
    }
    return covered, joined


def reach_share(reach, config, suite):
    """The share of `suite`'s tests that `config` runs: all of them where there is no record, the
    record does not cover the configuration, or it has no test of the suite. A test marked `*`
    runs under every configuration."""
    if reach is None or config == DEFAULTS:
        return 1.0
    covered, tests = reach
    if config not in covered or suite not in tests:
        return 1.0
    moved = tests[suite]
    return sum(config in m or "*" in m for m in moved) / len(moved)


def units(configurations, suites, weight, config_weights, lane, reach=None):
    """Every (configuration, module, suites, seconds) JVM the matrix runs, the defaults first. A
    suite whose tests a configuration's record leaves all to the defaults is not run under it, and
    one it runs some tests of weighs that share of its time, a tenth at least, for its set-up."""
    fill = statistics.median(config_weights.values()) if config_weights else 1.0
    out = []
    for config in [DEFAULTS] + list(configurations):
        factor = 1.0 if config == DEFAULTS else config_weights.get(config, fill)
        for module, listed in suites:
            shared = []
            for name, alone in listed:
                share = reach_share(reach, config, name)
                if share == 0.0:
                    continue
                seconds = weight(name) * factor * max(share, 0.1)
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


def pack(all_units, lane):
    """Bins of two lanes each, units longest first onto the lighter lane, a bin closed when a
    unit would take its lighter lane past the target; a unit longer than a lane takes a bin."""
    bins = []
    for unit in sorted(all_units, key=lambda u: (-u[3], u[0], u[1], u[2])):
        placed = False
        for b in bins:
            lanes = b["lanes"]
            k = min(range(LANES), key=lambda i: lanes[i])
            if lanes[k] + unit[3] <= lane:
                lanes[k] += unit[3]
                b["units"].append(unit)
                placed = True
                break
        if not placed:
            bins.append({"lanes": [unit[3]] + [0.0] * (LANES - 1), "units": [unit]})
    return bins


def plan(configurations, suites, suite_seconds, suite_times, config_weights, lane, reach=None):
    weight = suite_weight(suite_seconds, suite_times)
    bins = pack(units(configurations, suites, weight, config_weights, lane, reach), lane)
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


def times(junit_dirs, reach=None):
    """Each suite's seconds run whole, the median over the configurations that ran every test of it,
    the defaults among them, from JUnit reports under
    `<dir>/<configuration>/<module>-.../junit`. With the run's reach record, a configuration
    that ran only the tests the record let through is left out: its time is a share of the
    suite's, and the planner applies that share to the weight itself (`units`), so counting it
    here too would shrink the weight twice (VARKA-508). The configurations' runs land on the
    pool's mix of runners, so the median is a run's typical cost of the suite rather than the
    cost on the fastest or the slowest machine."""
    per_config = {}
    for d in junit_dirs:
        for xml in Path(d).glob("**/*/*/junit/TEST-*.xml"):
            head = xml.read_text(encoding="utf-8", errors="replace")[:4000]
            tag = re.search(r"<testsuite\b[^>]*>", head)
            if not tag:
                continue
            name = re.search(r'\bname="([^"]+)"', tag.group(0))
            seconds = re.search(r'\btime="([0-9.]+)"', tag.group(0))
            if not name or not seconds:
                continue
            config = xml.parent.parent.parent.name
            key = (name.group(1).rsplit(".", 1)[-1], config)
            per_config[key] = per_config.get(key, 0.0) + float(seconds.group(1))
    runs = {}
    for (suite, config), seconds in per_config.items():
        if reach_share(reach, config, suite) == 1.0:
            runs.setdefault(suite, []).append(seconds)
    return {suite: statistics.median(seconds) for suite, seconds in runs.items()}


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

    def test_shared_suites_split_into_groups_no_heavier_than_a_lane(self):
        suites = [("sql", [(f"S{i}", False) for i in range(10)])]
        weights = {f"S{i}": 100.0 for i in range(10)}
        got = units([], suites, suite_weight(weights, {}), {}, 320.0)
        self.assertTrue(all(u[3] <= 320.0 + 100.0 for u in got), got)
        self.assertEqual(sorted(s for u in got for s in u[2]), sorted(weights))

    def test_times_are_the_median_over_configurations(self):
        def report(root, config, leaf, suite, seconds):
            d = Path(root, "runs", config, leaf, "junit")
            d.mkdir(parents=True, exist_ok=True)
            # ScalaTest writes the attributes on the line after the tag, hostname before name.
            Path(d, f"TEST-org.x.{suite}.xml").write_text(
                f'<?xml version="1.0"?>\n<testsuite\n hostname="h" name="org.x.{suite}"'
                f' time="{seconds}" tests="1">\n</testsuite>\n',
                encoding="utf-8",
            )

        with tempfile.TemporaryDirectory() as root:
            report(root, "defaults", "sql-b000-0", "A", 10)
            report(root, "x=1", "sql-b001-0", "A", 30)
            # One configuration's two leaves of a suite add up before the median.
            report(root, "y=2", "sql-b002-0", "A", 15)
            report(root, "y=2", "sql-b003-0", "A", 25)
            report(root, "x=1", "sql-b001-0", "B", 4)
            self.assertEqual(times([root]), {"A": 30.0, "B": 4.0})

    def test_times_count_only_whole_runs_of_a_recorded_suite(self):
        with tempfile.TemporaryDirectory() as root:
            for config, suite, secs in [
                ("defaults", "A", 100),
                ("x=1", "A", 20),
                ("w=4", "A", 120),
                ("defaults", "B", 10),
                ("x=1", "B", 30),
            ]:
                d = Path(root, "runs", config, "sql-0", "junit")
                d.mkdir(parents=True, exist_ok=True)
                Path(d, f"TEST-p.{suite}.xml").write_text(
                    f'<testsuite name="p.{suite}" time="{secs}" tests="2"/>'
                )
            # x=1 is covered and runs one of A's two tests; w=4 is not covered, so runs A whole;
            # B has no record, so every configuration ran it whole.
            f = Path(root, "reach.tsv")
            f.write_text("#\tcovers\tx=1\nA\tone\tx=1\nA\ttwo\t-\n")
            got = times([Path(root, "runs")], read_reach(f))
            self.assertEqual(got, {"A": 110.0, "B": 20.0})
            self.assertEqual(times([Path(root, "runs")]), {"A": 100.0, "B": 20.0})

    def test_a_configuration_drops_the_suites_its_record_leaves_to_the_defaults(self):
        with tempfile.TemporaryDirectory() as root:
            f = Path(root, "reach.tsv")
            f.write_text(
                "#\tcovers\tx=1,y=2,w=4\nA\tone\tx=1\nA\ttwo\t-\nB\tone\t-\nB\t*\tw=4\n",
                encoding="utf-8",
            )
            reach = read_reach(f)
        suites = [("sql", [("A", True), ("B", True), ("C", True)])]
        got = units(["x=1", "y=2", "z=3"], suites, lambda s: 100.0, {}, 480.0, reach)
        ran = {(u[0], u[2][0]): u[3] for u in got}
        # x=1 moves one of A's two tests: half of A, no B; y=2 moves nothing it covers; z=3 is
        # not covered, so it runs everything; C has no record, so every configuration runs it.
        self.assertEqual(ran[("x=1", "A")], JVM_START + 50.0)
        self.assertNotIn(("x=1", "B"), ran)
        self.assertNotIn(("y=2", "A"), ran)
        self.assertEqual({s for c, s in ran if c == "z=3"}, {"A", "B", "C"})
        self.assertEqual({c for c, s in ran if s == "C"}, {"defaults", "x=1", "y=2", "z=3"})
        # B's set-up moved under w=4, so all of B runs there.
        got = units(["w=4"], suites, lambda s: 100.0, {}, 480.0, reach)
        self.assertIn(("w=4", "B"), {(u[0], u[2][0]) for u in got})

    def test_a_broken_record_is_none(self):
        with tempfile.TemporaryDirectory() as root:
            f = Path(root, "reach.tsv")
            f.write_text("A\tone\t-\n#\tbroken\tboom\n", encoding="utf-8")
            self.assertIsNone(read_reach(f))

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
    p.add_argument("--reach", help="the Build's record of the tests each configuration moves")
    t = sub.add_parser("times")
    t.add_argument("dirs", nargs="+")
    t.add_argument("--reach", help="the run's record: only whole runs of a suite count")
    args = parser.parse_args(argv)
    if args.cmd == "times":
        reach = read_reach(args.reach) if args.reach and Path(args.reach).is_file() else None
        for name, seconds in sorted(times(args.dirs, reach).items()):
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
        read_reach(args.reach) if args.reach else None,
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
