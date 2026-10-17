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
"""The IR fuzzer's compiled test at volume, totalled over its JVMs (VARKA-274).

  dev/varka_compiled_fuzz.py RUN_DIR...
  dev/varka_compiled_fuzz.py --self-test

Reads the run directories dev/varka_compiled_fuzz.sh writes - each holds run.log, whose
VARKA_COMPILED lines (VarkaIrFuzzSuite under -Dvarka.fuzz.census=true) name one history run of
one drawn case per line, and exit, the JVM's status - and prints, per lane: the cases run over
the compiled histories and the time a case over them; per history the runs, the warm-ups settled
and confirmed, and what the runs' counted calls came to (all C2, trapped, an OSR at level 4 or 3,
mixed); then the unsettled cases with their trap reasons, the share of cases with an unsettled
warm-up and how many of those trapped at profile_predicate, the histories not reached as the
JVMs printed them, and every failed test.

The exit status is 1 when a JVM's exit file is missing or not 0 (a wrong answer fails its test)
or a run directory has no VARKA_COMPILED line; a history not reached is printed, not failed.
"""

import argparse
import collections
import glob
import io
import os
import statistics
import sys
import tempfile

WARMED = ("warm-same", "warm-other", "quiet-remainder", "quiet-null", "quiet-guard", "recompiled")
HISTORIES = ("cold",) + WARMED + ("released",)

Run = collections.namedtuple(
    "Run",
    "lane draw history kind settled confirmed settle_ms millis traps label",
)


# A VARKA_COMPILED line that does not parse, such as one another thread's output broke into.
GARBLED = "garbled"


def parse_line(line):
    """One VARKA_COMPILED line as a Run, GARBLED if it does not parse, None for any other line."""
    if not line.startswith("VARKA_COMPILED\t"):
        return None
    f = line.rstrip("\n").split("\t", 10)
    if len(f) != 11:
        return GARBLED
    try:
        return Run(
            lane=f[1].lower(),
            draw=int(f[2]),
            history=f[3],
            kind=f[4],
            settled=f[5] == "true",
            confirmed=f[6] == "true",
            settle_ms=int(f[7]),
            millis=int(f[8]),
            traps=f[9],
            label=f[10],
        )
    except ValueError:
        return GARBLED


def percentiles(values):
    if not values:
        return "-"
    s = sorted(values)
    p90 = s[min(len(s) - 1, max(0, -(-len(s) * 9 // 10) - 1))]
    return f"median {int(statistics.median(s))}, p90 {p90}, max {s[-1]}"


def read_dir(d):
    """The runs, the not-reached lines, the failure lines, the garbled count and the exit status of
    one JVM."""
    runs, not_reached, failures, garbled = [], [], [], 0
    log = os.path.join(d, "run.log")
    if os.path.exists(log):
        with open(log, errors="replace") as f:
            for line in f:
                r = parse_line(line)
                if r is GARBLED:
                    garbled += 1
                elif r is not None:
                    runs.append(r)
                elif "history not reached" in line:
                    not_reached.append(line.strip())
                elif "*** FAILED ***" in line or "*** ABORTED ***" in line:
                    failures.append(line.strip())
    try:
        with open(os.path.join(d, "exit")) as f:
            status = f.read().strip()
    except OSError:
        status = "missing"
    return runs, not_reached, failures, garbled, status


def report(dirs, out):
    """Writes the totals to `out`; returns the exit status."""
    bad = 0
    runs, not_reached, failures = [], [], []
    for d in dirs:
        r, n, f, garbled, status = read_dir(d)
        runs += r
        if garbled:
            print(f"{d}: {garbled} garbled VARKA_COMPILED line(s) skipped", file=out)
        not_reached += [f"{os.path.basename(d)}: {x}" for x in n]
        failures += [f"{os.path.basename(d)}: {x}" for x in f]
        if status != "0":
            print(f"{d}: JVM exit {status}", file=out)
            bad = 1
        if not r:
            print(f"{d}: no VARKA_COMPILED line", file=out)
            bad = 1
    for lane in ("int", "long"):
        lane_runs = [r for r in runs if r.lane == lane]
        if not lane_runs:
            continue
        by_case = collections.defaultdict(list)
        for r in lane_runs:
            by_case[r.draw].append(r)
        compiled = {k: v for k, v in by_case.items() if any(r.history != "cold" for r in v)}
        case_ms = [sum(r.millis for r in v if r.history != "cold") for v in compiled.values()]
        cold_only = len(by_case) - len(compiled)
        print(
            f"{lane} lane: {len(compiled)} cases over the compiled histories, ms a case "
            f"{percentiles(case_ms)}; {cold_only} run cold only (a lanes override)",
            file=out,
        )
        for h in HISTORIES:
            hr = [r for r in lane_runs if r.history == h]
            if not hr:
                continue
            line = f"  {h}: {len(hr)} run"
            if h in WARMED:
                line += (
                    f", settled {sum(r.settled for r in hr)}/{len(hr)}"
                    f" (confirmed {sum(r.confirmed for r in hr)})"
                )
            kinds = collections.Counter(r.kind for r in hr)
            line += (
                f", ms {percentiles([r.millis for r in hr])}; cases {dict(sorted(kinds.items()))}"
            )
            print(line, file=out)
        unsettled = collections.defaultdict(list)
        for r in lane_runs:
            if r.history in WARMED and not r.settled:
                unsettled[r.draw].append(r)
        cycling = sum(
            1 for v in unsettled.values() if any("profile_predicate" in r.traps for r in v)
        )
        share = 100.0 * len(unsettled) / len(compiled) if compiled else 0.0
        print(
            f"  unsettled: {len(unsettled)} of {len(compiled)} cases ({share:.1f}%), "
            f"{cycling} of them trapping at profile_predicate",
            file=out,
        )
        for k in sorted(unsettled):
            v = unsettled[k]
            hs = ",".join(r.history for r in v)
            print(f"    draw {k} in {hs}: traps {v[0].traps}; {v[0].label[:160]}", file=out)
    print(f"{len(not_reached)} history(ies) not reached", file=out)
    for x in not_reached:
        print(f"  {x}", file=out)
    print(f"{len(failures)} failed test(s)", file=out)
    for x in failures:
        print(f"  {x}", file=out)
    if failures:
        bad = 1
    return bad


def self_test():
    """The reader over two planted JVMs: counts, the unsettled share and a failed JVM."""
    lines = [
        "VARKA_COMPILED\tINT\t0\tcold\tall mixed\ttrue\tfalse\t0\t5\t-\tcase 0",
        "VARKA_COMPILED\tINT\t0\twarm-same\tall C2\ttrue\ttrue\t400\t900\t-\tcase 0",
        "VARKA_COMPILED\tINT\t0\treleased\tosr4\ttrue\tfalse\t0\t1200\t-\tcase 0",
        "VARKA_COMPILED\tINT\t1\twarm-same\tsome mixed\tfalse\tfalse\t10000\t10100\t"
        "{profile_predicate=40}\tcase 1",
        "VARKA_COMPILED\tINT\t2\tcold\tall mixed\ttrue\tfalse\t0\t4\t-\tcase 2",
        "info line - history not reached: released, no case of the INT lane",
        "VARKA_COMPILED\tINT\tx\tcold\tall mixed\ttrue\tfalse\t0\t4\t-\tcase x",
    ]
    with tempfile.TemporaryDirectory() as tmp:
        for i, (body, status) in enumerate([(lines[:4], "0"), (lines[4:], "1")]):
            d = os.path.join(tmp, f"run-{i}")
            os.makedirs(d)
            with open(os.path.join(d, "run.log"), "w") as f:
                f.write("\n".join(body) + "\n")
            with open(os.path.join(d, "exit"), "w") as f:
                f.write(status + "\n")
        buf = io.StringIO()
        status = report(sorted(glob.glob(os.path.join(tmp, "run-*"))), buf)
    text = buf.getvalue()
    expect = [
        "int lane: 2 cases over the compiled histories, ms a case median 6100",
        "1 run cold only",
        "warm-same: 2 run, settled 1/2 (confirmed 1)",
        "unsettled: 1 of 2 cases (50.0%), 1 of them trapping at profile_predicate",
        "1 history(ies) not reached",
        "JVM exit 1",
        "1 garbled VARKA_COMPILED line(s) skipped",
    ]
    missing = [e for e in expect if e not in text]
    if status != 1 or missing:
        print(text, file=sys.stderr)
        print(f"self-test failed: status {status}, missing {missing}", file=sys.stderr)
        return 1
    print("self-test passed: the reader counted the planted runs and failed the failed JVM")
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("dirs", nargs="*")
    p.add_argument("--self-test", action="store_true")
    a = p.parse_args()
    if a.self_test:
        return self_test()
    if not a.dirs:
        p.error("no run directory")
    return report(a.dirs, sys.stdout)


if __name__ == "__main__":
    sys.exit(main())
