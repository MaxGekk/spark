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
"""The deoptimization cycle over the fuzz grammar's drawn kernels (VARKA-326).

  dev/varka_deopt_census.py [--tsv FILE] [--label NAME] RUN_DIR...

Reads the run directories dev/varka_deopt_census.sh writes - each holds run.log, whose
VARKA_CENSUS lines name one drawn kernel per line, and xlog.txt, the JVM's own
-Xlog:jit+compilation=debug,deoptimization=debug output - and prints, per lane, how many
kernels entered the cycle and which shapes they were. With --tsv it also writes one row per
kernel.

A kernel is CYCLING by VARKA-189's rule, the one dev/varka_deopt_cycle.py gives the nightly's
guard, here applied to the loop methods of both sides: some loop method was made not entrant at
tier 4 three or more times and trapped at profile_predicate more than four times. Beside it the
rows carry what the compiled test's own criterion (VARKA-274) sees: the warm-up's settle time,
so a reader can cut at the compiled test's 10 s deadline instead. A kernel's cycle seconds are
the span from its first to its last profile_predicate trap. A kernel is NULL-MODE when its tree
holds a node that nulls a lane from valid inputs (an overflow in NULL mode, a MakeDate that does
not fail on error): the emitter then builds no dense side (Analysis.nullsFromValidInputs).
"""

import argparse
import collections
import glob
import os
import re
import statistics
import sys

COMPILE = re.compile(
    r"^\[([\d.]+)s\]\[jit,compilation\s*\]\s+\d+\s+[%sbn! ]*?\s*(\d)\s+"
    r"\S*VarkaFusedProjection_(wfuzz\d+)::(\w+)(?: @ \d+)? \(\d+ bytes\)(.*)$"
)
DEOPT = re.compile(
    r"^\[([\d.]+)s\]\[deoptimization\s*\]\s+cid=\d+\s+level=\d\s+"
    r"\S*VarkaFusedProjection_(wfuzz\d+)\.(\w+)\([^)]*\)\S*\s+trap_bci=\d+\s+(\S+)"
)
# The cap the JVM puts on one method's traps, -XX:PerMethodTrapLimit (100 on JDK 25).
TRAP_CAP = 100


def read_run(run_dir):
    """Yields one dict per kernel of one run directory."""
    census = []
    with open(os.path.join(run_dir, "run.log"), errors="replace") as f:
        for line in f:
            if line.startswith("VARKA_CENSUS\t"):
                census.append(line.rstrip("\n").split("\t", 7)[1:])
    not_entrant = collections.defaultdict(collections.Counter)
    traps = collections.defaultdict(collections.Counter)
    pp_times = collections.defaultdict(list)
    with open(os.path.join(run_dir, "xlog.txt"), errors="replace") as f:
        for line in f:
            m = COMPILE.match(line)
            if m:
                _, tier, cls, method, note = m.groups()
                if tier == "4" and "made not entrant" in note:
                    not_entrant[cls][method] += 1
                continue
            m = DEOPT.match(line)
            if m:
                when, cls, method, reason = m.groups()
                traps[cls][(method, reason)] += 1
                if reason == "profile_predicate":
                    pp_times[cls].append(float(when))
    for lane, k, _history, class_name, settled, settle_ms, label in census:
        cls = class_name.rsplit("_", 1)[-1]
        yield {
            "run": os.path.basename(run_dir.rstrip("/")),
            "lane": lane,
            "k": int(k),
            "settled": settled == "true",
            "settle_ms": int(settle_ms),
            "label": label,
            "not_entrant": not_entrant.get(cls, {}),
            "traps": traps.get(cls, {}),
            "pp_times": pp_times.get(cls, []),
        }


def roots_of(label):
    m = re.search(r"roots=\[(.*?)\] options=", label)
    return m.group(1) if m else ""


def count_roots(tree):
    """The roots of a canonical list: split at the commas outside every parenthesis."""
    depth, roots = 0, 1 if tree else 0
    for ch in tree:
        depth += (ch == "(") - (ch == ")")
        roots += ch == "," and depth == 0
    return roots


def judge(kernel):
    """The kernel's shape and verdict columns."""
    label = kernel["label"]
    tree = roots_of(label)
    nodes = set(re.findall(r"\(([A-Za-z][\w:]*)", tree))
    null_nodes = sorted(n for n in nodes if n.endswith(":NULL"))
    pp = collections.Counter()
    other = collections.Counter()
    for (method, reason), n in kernel["traps"].items():
        (pp if reason == "profile_predicate" else other)[method] += n
    loops = [m for m in pp if m.startswith("loop")]
    cycling = [m for m in loops if kernel["not_entrant"].get(m, 0) >= 3 and pp[m] > 4]
    times = kernel["pp_times"]
    return {
        "roots": count_roots(tree),
        "columns": len(set(re.findall(r"col:(\d+)", tree))),
        "length": int(re.search(r" length=(\d+)", label).group(1)),
        "force_masked": int("forceMasked=true" in label),
        "null_nodes": ",".join(null_nodes),
        "pp_traps": sum(pp.values()),
        "max_method_pp": max(pp.values(), default=0),
        "sides": "".join(sorted({"M" if m.startswith("loopMasked") else "D" for m in loops})),
        "cycle_s": round(max(times) - min(times)) if times else 0,
        "cycling": int(bool(cycling)),
        "settled": int(kernel["settled"]),
        "settle_ms": kernel["settle_ms"],
    }


COLUMNS = [
    "run", "lane", "k", "null_nodes", "roots", "columns", "length", "force_masked",
    "settled", "settle_ms", "pp_traps", "max_method_pp", "sides", "cycle_s", "cycling",
]  # fmt: skip


def bucket(n):
    for bound, name in ((0, "0"), (4, "1-4"), (19, "5-19"), (99, "20-99")):
        if n <= bound:
            return name
    return "100+"


def report(rows, label):
    print(f"== {label}: {len(rows)} kernels")
    for lane in ("INT", "LONG", "ALL"):
        rs = [r for r in rows if lane == "ALL" or r["lane"] == lane]
        nm = [r for r in rs if r["null_nodes"]]
        rest = [r for r in rs if not r["null_nodes"]]
        cyc = [r for r in rs if r["cycling"]]
        h = collections.Counter(bucket(r["pp_traps"]) for r in nm)
        print(
            f"{lane:5s} kernels {len(rs)}; NULL-mode {len(nm)}; cycling {len(cyc)} "
            f"(NULL-mode {sum(r['cycling'] for r in nm)}, others "
            f"{sum(r['cycling'] for r in rest)}); others with any profile_predicate trap "
            f"{sum(1 for r in rest if r['pp_traps'])}; "
            f"a method at {TRAP_CAP} traps {sum(r['max_method_pp'] >= TRAP_CAP for r in rs)}; "
            f"most traps on one method {max((r['max_method_pp'] for r in rs), default=0)}"
        )
        print(
            "      NULL-mode kernels by profile_predicate traps: "
            + ", ".join(f"{b} {h.get(b, 0)}" for b in ("0", "1-4", "5-19", "20-99", "100+"))
        )
        late = [r for r in rs if r["settle_ms"] > 10_000 and r["pp_traps"] > 4]
        print(
            f"      settle over 10 s with more than four traps (the compiled test's cut): "
            f"{len(late)}; not settled by the deadline: {sum(not r['settled'] for r in rs)} "
            f"({sum(1 for r in rs if not r['settled'] and not r['pp_traps'])} of them "
            f"without a profile_predicate trap)"
        )
        secs = sorted(r["cycle_s"] for r in cyc)
        if secs:
            print(
                f"      cycle seconds of the cycling kernels: median "
                f"{statistics.median(secs):.0f}, p90 {secs[int(0.9 * (len(secs) - 1))]}, "
                f"max {secs[-1]}; 5 s or more {sum(s >= 5 for s in secs)}, 10 s or more "
                f"{sum(s >= 10 for s in secs)}, 20 s or more {sum(s >= 20 for s in secs)}"
            )
        for name in ("roots", "columns", "length_le_16", "force_masked"):
            t = collections.defaultdict(lambda: [0, 0])
            for r in nm:
                key = (
                    ("<=16" if r["length"] <= 16 else ">16") if name == "length_le_16" else r[name]
                )
                t[key][0] += 1
                t[key][1] += r["cycling"]
            cells = "; ".join(f"{k}: {v[1]}/{v[0]}" for k, v in sorted(t.items(), key=str))
            print(f"      NULL-mode, cycling/kernels by {name}: {cells}")
        t = collections.defaultdict(lambda: [0, 0])
        for r in nm:
            t[r["null_nodes"]][0] += 1
            t[r["null_nodes"]][1] += r["cycling"]
        cells = "; ".join(f"{k}: {v[1]}/{v[0]}" for k, v in sorted(t.items()))
        print(f"      NULL-mode, cycling/kernels by NULL-mode nodes: {cells}")


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--tsv", help="write one row per kernel here")
    p.add_argument("--label", default="census")
    p.add_argument("run_dirs", nargs="+", metavar="RUN_DIR")
    args = p.parse_args()
    dirs = [d for pattern in args.run_dirs for d in sorted(glob.glob(pattern))]
    rows = []
    for d in dirs:
        for kernel in read_run(d):
            rows.append({**judge(kernel), "run": kernel["run"], "lane": kernel["lane"],
                         "k": kernel["k"]})  # fmt: skip
    rows.sort(key=lambda r: (r["lane"], r["k"]))
    if args.tsv:
        with open(args.tsv, "w") as f:
            f.write("\t".join(COLUMNS) + "\n")
            for r in rows:
                f.write("\t".join(str(r[c]) for c in COLUMNS) + "\n")
    if not rows:
        print("no VARKA_CENSUS line in the run directories", file=sys.stderr)
        return 1
    report(rows, args.label)
    return 0


if __name__ == "__main__":
    sys.exit(main())
