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

A suite whose unit would outlast a lane under a configuration is split by its tests into parts,
each a unit of its own, run through the runner's test filter (`dev/varka_matrix.sh --tests`).
The parts are balanced by the tests' seconds in `sql/varka/matrix/test-seconds.tsv` (written by
`tests` from the same JUnit reports, each test's median over the configurations that ran it),
with a test the reach record says the configuration does not move weighing nothing, since the
runner skips it there (it still goes to one part, whose JVM then skips it), and a test the
reach record lists but the file does not weighing the median of the suite's tests. A part takes
its share of the whole unit's seconds. The file and the reach record are the planner's only
lists of a suite's tests, so a suite the file does not list is never split, and `check` fails a
run in which a configuration ran a suite without a test another configuration ran it with: a
test added since the file was written, which no part names, is caught there when another
configuration ran the suite whole.

  dev/varka_matrix_bins.py plan --configurations FILE --suites MODULE=FILE... \\
      [--target SECONDS] > bins.json          # the job matrix, {"include": [...]}
  dev/varka_matrix_bins.py times [--reach FILE] JUNIT_DIR... > suite-seconds.tsv
  dev/varka_matrix_bins.py tests JUNIT_DIR... > test-seconds.tsv
  dev/varka_matrix_bins.py check JUNIT_DIR...
  dev/varka_matrix_bins.py --selftest

Each bin's `units` is one line a JVM, `configuration|module|suite,suite,...`, the suites as
simple names, which the bin's job hands to `dev/varka_matrix.sh --suites`. A part of a split
suite adds its tests as a fourth field, `configuration|module|suite|test<TAB>test...`, last
because a test's name may hold a `|`, which the job hands to `--tests`.
"""

import argparse
import json
import re
import statistics
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

SUITE_TIMES = "sql/varka/ci/suite-times.tsv"
SUITE_SECONDS = "sql/varka/matrix/suite-seconds.tsv"
TEST_SECONDS = "sql/varka/matrix/test-seconds.tsv"
CONFIG_WEIGHTS = "sql/varka/matrix/seconds.tsv"
DEFAULTS = "defaults"
# A runner JVM's start and its first Spark session, before any suite's own time.
JVM_START = 20.0
# The settings a configuration may name that change only a Spark session's configuration, not
# an emit option: `VarkaMatrixRuns.parseOver` leaves the options as they are for them. No
# catalyst suite builds a session, so under a configuration of these alone the catalyst suites
# would run as the defaults run them, and the matrix leaves them out (VARKA-524).
SESSION_ONLY = ("arrowBatchSize",)


def reaches(config, module):
    """Whether `config` can change anything `module`'s suites run: not catalyst's, for a
    configuration that names only session settings."""
    if module != "catalyst" or config == DEFAULTS:
        return True
    names = {part.split("=", 1)[0] for part in re.split(r"[,_]", config) if "=" in part}
    return not names or not names <= set(SESSION_ONLY)


LANES = 2
# `tests` lists the tests of the suites whose run under some configuration took this long, a
# quarter of the default lane: a suite no configuration ran for that long is far from a lane.
SPLIT_CANDIDATE = 120.0


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


def read_test_seconds(path):
    """`suite<TAB>test<TAB>seconds` lines, as `tests` writes them: {suite: {test: seconds}}."""
    out = {}
    p = Path(path)
    if not p.exists():
        return out
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        suite, test, seconds = line.rsplit("\t", 2)
        out.setdefault(suite, {})[test] = float(seconds)
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
    lines, and for each suite each test's name with the configurations that move it, the suite's
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
            tests.setdefault(suite, {})[test] = moved
    joined = {
        suite: {test: m | setup.get(suite, set()) | everywhere for test, m in moved.items()}
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
    moved = tests[suite].values()
    return sum(config in m or "*" in m for m in moved) / len(moved)


def runs_test(reach, config, suite, test):
    """Whether `config`'s run of `suite` runs `test` rather than skipping it, by the record."""
    if reach is None or config == DEFAULTS:
        return True
    covered, tests = reach
    moved = tests.get(suite, {}).get(test)
    return config not in covered or moved is None or config in moved or "*" in moved


def split_tests(config, suite, seconds, lane, test_seconds, reach):
    """The parts of a suite whose unit outlasts a lane: lists of its tests, longest first onto the
    lightest part, as many parts as the unit's seconds fill lanes, each with its share of the
    seconds. A test longer than a lane takes a part alone, which then outlasts the lane. Every
    test the file or the record lists lands in exactly one part; one part when the suite has no
    test the file lists."""
    recorded = test_seconds.get(suite, {})
    if not recorded:
        return [(None, seconds)]
    fill = statistics.median(recorded.values())
    listed = set(recorded) | set(reach[1].get(suite, {}) if reach else ())
    weights = {
        t: recorded.get(t, fill) if runs_test(reach, config, suite, t) else 0.0 for t in listed
    }
    count = min(len(listed), int(-(-seconds // max(lane - JVM_START, 1.0))))
    parts = [[0.0, []] for _ in range(count)]
    for test in sorted(listed, key=lambda t: (-weights[t], t)):
        p = min(parts, key=lambda x: x[0])
        p[0] += weights[test]
        p[1].append(test)
    total = sum(weights.values()) or 1.0
    return [(sorted(names), seconds * load / total) for load, names in parts if names]


def units(configurations, suites, weight, config_weights, lane, reach=None, test_seconds=None):
    """Every (configuration, module, suites, seconds, tests) JVM the matrix runs, the defaults
    first, `tests` empty but for a part of a split suite. A suite whose tests a configuration's
    record leaves all to the defaults is not run under it, and one it runs some tests of weighs
    that share of its time, a tenth at least, for its set-up. A suite heavier than a lane is split
    by its tests (split_tests), each part in a JVM of its own, whether or not the suite runs
    alone: a suite that runs alone keeps its JVM to itself, and one that shares can run alone."""
    fill = statistics.median(config_weights.values()) if config_weights else 1.0
    out = []
    for config in [DEFAULTS] + list(configurations):
        factor = 1.0 if config == DEFAULTS else config_weights.get(config, fill)
        for module, listed in suites:
            if not reaches(config, module):
                continue
            shared = []
            for name, alone in listed:
                share = reach_share(reach, config, name)
                if share == 0.0:
                    continue
                seconds = weight(name) * factor * max(share, 0.1)
                if JVM_START + seconds > lane and test_seconds:
                    parts = split_tests(config, name, seconds, lane, test_seconds, reach)
                else:
                    parts = [(None, seconds)]
                if len(parts) > 1:
                    for tests, part in parts:
                        out.append((config, module, [name], JVM_START + part, tuple(tests)))
                elif alone:
                    out.append((config, module, [name], JVM_START + seconds, ()))
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
                    out.append((config, module, sorted(names), JVM_START + load, ()))
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


def unit_line(unit):
    """A unit as the bin's job reads it, `configuration|module|suites[|tests]`."""
    config, module, names, _, tests = unit
    line = f"{config}|{module}|{','.join(names)}"
    return line + "|" + "\t".join(tests) if tests else line


def splits(all_units, lane):
    """One line for each split suite under a configuration, its parts' seconds, and one for each
    suite whose whole unit still outlasts the lane, its tests not in the test file or only one."""
    parts, unsplit = {}, []
    for config, _, names, seconds, tests in all_units:
        if tests:
            parts.setdefault((config, names[0]), []).append(round(seconds))
        elif len(names) == 1 and seconds > lane:
            unsplit.append(f"not split {names[0]} under {config}: {round(seconds)} s")
    return [
        f"split {suite} under {config}: {len(s)} parts, {', '.join(map(str, sorted(s)))} s"
        for (config, suite), s in sorted(parts.items())
    ] + sorted(unsplit)


def plan(
    configurations,
    suites,
    suite_seconds,
    suite_times,
    config_weights,
    lane,
    reach=None,
    test_seconds=None,
    log=None,
):
    weight = suite_weight(suite_seconds, suite_times)
    all_units = units(configurations, suites, weight, config_weights, lane, reach, test_seconds)
    for line in splits(all_units, lane) if log else ():
        print(line, file=log)
    bins = pack(all_units, lane)
    include = []
    for k, b in enumerate(bins):
        include.append(
            {
                "bin": f"b{k:03d}",
                "seconds": round(max(b["lanes"])),
                "units": "\n".join(unit_line(u) for u in b["units"]),
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


def junit_tests(junit_dirs):
    """Each (configuration, suite)'s tests, {test: [seconds, ran]}, from the same JUnit reports,
    the leaves of one configuration joined. `ran` is whether the leaf's `fused.tsv` names the
    test: a test the reach record skipped passes unrun, with a report but no fused line."""
    out, fused_by_leaf = {}, {}
    for d in junit_dirs:
        for xml in Path(d).glob("**/*/*/junit/TEST-*.xml"):
            try:
                root = ET.parse(xml).getroot()
            except ET.ParseError:
                continue
            suite = root.get("name", "").rsplit(".", 1)[-1]
            leaf = xml.parent.parent
            if leaf not in fused_by_leaf:
                fused_by_leaf[leaf] = set()
                if (leaf / "fused.tsv").exists():
                    for line in (leaf / "fused.tsv").read_text(encoding="utf-8").splitlines():
                        s, t, _ = line.rsplit("\t", 2)
                        fused_by_leaf[leaf].add((s.rsplit(".", 1)[-1], t))
            fused = fused_by_leaf[leaf]
            tests = out.setdefault((leaf.parent.name, suite), {})
            for case in root.iter("testcase"):
                name = case.get("name", "")
                t = tests.setdefault(name, [0.0, False])
                t[0] += float(case.get("time") or 0.0)
                t[1] = t[1] or (suite, name) in fused
    return out


def per_test_seconds(junit_dirs, candidate=SPLIT_CANDIDATE):
    """Each test's seconds, {suite: {test: seconds}}, for the suites a configuration ran for
    `candidate` seconds or more: the median over the configurations that ran the test, or over all
    that reported it when none has a fused line for it (a test outside the matrix's hook)."""
    runs = junit_tests(junit_dirs)
    longest = {}
    for (_, suite), ts in runs.items():
        longest[suite] = max(longest.get(suite, 0.0), sum(t[0] for t in ts.values()))
    samples = {}
    for (_, suite), ts in runs.items():
        if longest[suite] >= candidate:
            for name, (seconds, ran) in ts.items():
                samples.setdefault(suite, {}).setdefault(name, []).append((seconds, ran))
    out = {}
    for suite, by_test in samples.items():
        for name, ss in by_test.items():
            kept = [x for x, ran in ss if ran] or [x for x, _ in ss]
            out.setdefault(suite, {})[name] = statistics.median(kept)
    return out


def missing_tests(junit_dirs):
    """The tests some configuration ran a suite without, where another ran them: a test a split
    left out of every part, or one the run lost otherwise. A configuration that ran nothing of a
    suite is not counted, since the reach record drops whole suites. Lines to print, none when
    every configuration that ran a suite ran all of its tests."""
    by_suite = {}
    for (config, suite), ts in junit_tests(junit_dirs).items():
        by_suite.setdefault(suite, {})[config] = set(ts)
    out = []
    for suite, by_config in sorted(by_suite.items()):
        every = set().union(*by_config.values())
        for config, ran in sorted(by_config.items()):
            for name in sorted(every - ran):
                out.append(f"{config}\t{suite}\t{name}")
    return out


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
                ("defaults", "sql", ["Big"], 920.0, ()),
                ("defaults", "sql", ["S"], 25.0, ()),
                ("x=1", "sql", ["S"], 25.0, ()),
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

    def test_a_session_only_configuration_leaves_catalyst_out(self):
        suites = [("sql", [("S", False)]), ("catalyst", [("C", False)])]
        got = units(["arrowBatchSize=1", "cse=false"], suites, lambda s: 10.0, {}, 480.0)
        modules = {(c, m) for c, m, *_ in got}
        self.assertNotIn(("arrowBatchSize=1", "catalyst"), modules)
        self.assertIn(("arrowBatchSize=1", "sql"), modules)
        self.assertIn(("cse=false", "catalyst"), modules)
        self.assertIn(("defaults", "catalyst"), modules)
        self.assertFalse(reaches("arrowBatchSize=17", "catalyst"))
        self.assertTrue(reaches("arrowBatchSize=1,cse=false", "catalyst"))

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

    def test_a_suite_past_the_lane_splits_and_every_test_runs_once_per_configuration(self):
        # A runs alone, B shares a JVM with C; both outlast a 300 s lane under every
        # configuration, C does not, and D, which the file does not list, is never split.
        suites = [("sql", [("A", True), ("B", False), ("C", False), ("D", True)])]
        weights = {"A": 600.0, "B": 500.0, "C": 50.0, "D": 900.0}
        per_test = {
            "A": {"a|pipe": 250.0, "a2": 200.0, "a3": 100.0, "a4": 30.0, "a5": 20.0},
            "B": {f"b{i}": 50.0 for i in range(10)},
            "C": {"c1": 50.0},
        }
        configs = ["x=1", "y=2"]
        got = units(configs, suites, suite_weight(weights, {}), {}, 300.0, None, per_test)
        for config in ["defaults"] + configs:
            mine = [u for u in got if u[0] == config]
            for suite in "AB":
                parts = [u for u in mine if suite in u[2]]
                self.assertGreater(len(parts), 1, parts)
                # A part runs its suite alone, and its tests are each in exactly one part.
                self.assertTrue(all(u[2] == [suite] for u in parts), parts)
                ran = sorted(t for u in parts for t in u[4])
                self.assertEqual(ran, sorted(per_test[suite]))
                self.assertAlmostEqual(
                    sum(u[3] - JVM_START for u in parts), weights[suite], places=6
                )
                self.assertTrue(all(u[3] <= 300.0 for u in parts), parts)
            self.assertEqual([u[4] for u in mine if "C" in u[2]], [()])
            self.assertEqual([u[4] for u in mine if "D" in u[2]], [()])
        # The bin's job reads a part's line with IFS='|' into four fields, the last the rest.
        line = unit_line(next(u for u in got if "a|pipe" in u[4]))
        config, module, suite, rest = line.split("|", 3)
        self.assertEqual((module, suite), ("sql", "A"))
        self.assertIn("a|pipe", rest.split("\t"))

    def test_a_split_weighs_unknown_tests_at_the_median_and_skipped_ones_at_nothing(self):
        with tempfile.TemporaryDirectory() as root:
            f = Path(root, "reach.tsv")
            # t1 and t2 move under x=1; t3 moves under nothing; new is in no time file.
            f.write_text(
                "#\tcovers\tx=1\nS\tt1\tx=1\nS\tt2\tx=1\nS\tt3\t-\nS\tnew\tx=1\n",
                encoding="utf-8",
            )
            reach = read_reach(f)
        per_test = {"S": {"t1": 300.0, "t2": 100.0, "t3": 200.0}}
        parts = split_tests("x=1", "S", 600.0, 320.0, per_test, reach)
        # Two parts: t1 alone, and t2 with new at the median (200 s); t3 is skipped under x=1,
        # so it weighs nothing, yet still runs in one part so the JVM's own skip covers it.
        self.assertEqual(sorted(t for names, _ in parts for t in names), ["new", "t1", "t2", "t3"])
        loads = sorted(round(s) for _, s in parts)
        self.assertEqual(loads, [300, 300])
        # Under the defaults every test runs, t3 at its 200 s.
        parts = split_tests("defaults", "S", 800.0, 320.0, per_test, reach)
        self.assertEqual(len(parts), 3)
        self.assertEqual(sorted(t for names, _ in parts for t in names), ["new", "t1", "t2", "t3"])

    def test_a_test_longer_than_the_lane_takes_a_part_alone(self):
        parts = split_tests("defaults", "S", 500.0, 300.0, {"S": {"big": 450, "s": 50}}, None)
        self.assertEqual(sorted(names for names, _ in parts), [["big"], ["s"]])
        self.assertEqual(split_tests("defaults", "S", 500.0, 300.0, {}, None), [(None, 500.0)])

    def test_per_test_seconds_and_the_check_read_the_bins_reports(self):
        def report(root, config, leaf, suite, cases, fused=()):
            d = Path(root, "runs", config, leaf, "junit")
            d.mkdir(parents=True, exist_ok=True)
            body = "".join(f'<testcase name="{n}" time="{t}"/>' for n, t in cases)
            Path(d, f"TEST-org.x.{suite}.xml").write_text(
                f'<testsuite name="org.x.{suite}" time="1">{body}</testsuite>', encoding="utf-8"
            )
            with open(Path(d.parent, "fused.tsv"), "a", encoding="utf-8") as f:
                f.write("".join(f"{suite}\t{n}\t1\n" for n in fused))

        with tempfile.TemporaryDirectory() as root:
            report(root, "defaults", "sql-b0-0", "S", [("t1", 100), ("t2", 30)], ["t1", "t2"])
            # Under x=1 the reach record skipped t2: its near-zero time is not a sample.
            report(root, "x=1", "sql-b1-0", "S", [("t1", 140), ("t2", 0.01)], ["t1"])
            # Two parts of one split under y=2 join; the small suite is not listed.
            report(root, "y=2", "sql-b2-0", "S", [("t1", 120)], ["t1"])
            report(root, "y=2", "sql-b2-1", "S", [("t2", 50)], ["t2"])
            report(root, "y=2", "sql-b2-1", "Small", [("s", 5)], ["s"])
            self.assertEqual(per_test_seconds([root]), {"S": {"t1": 120.0, "t2": 40.0}})
            self.assertEqual(missing_tests([root]), [])
            # A configuration whose parts lost t2 is named.
            report(root, "z=3", "sql-b3-0", "S", [("t1", 120)], ["t1"])
            self.assertEqual(missing_tests([root]), ["z=3\tS\tt2"])


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
    for cmd in ("tests", "check"):
        sub.add_parser(cmd).add_argument("dirs", nargs="+")
    args = parser.parse_args(argv)
    if args.cmd == "times":
        reach = read_reach(args.reach) if args.reach and Path(args.reach).is_file() else None
        for name, seconds in sorted(times(args.dirs, reach).items()):
            print(f"{name}\t{seconds:.1f}")
        return 0
    if args.cmd == "tests":
        for suite, by_test in sorted(per_test_seconds(args.dirs).items()):
            for name, seconds in sorted(by_test.items()):
                print(f"{suite}\t{name}\t{seconds:.1f}")
        return 0
    if args.cmd == "check":
        missing = missing_tests(args.dirs)
        for line in missing:
            print(f"missing\t{line}")
        print(f"{len(missing)} tests a configuration ran a suite without", file=sys.stderr)
        return 1 if missing else 0
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
        read_test_seconds(TEST_SECONDS),
        sys.stderr,
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
