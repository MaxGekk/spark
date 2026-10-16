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
  dev/varka_deopt_census.py --guard COUNT_FILE GUARD_DIR    # the nightly guard (VARKA-378)
  dev/varka_deopt_census.py --guard-rows COUNT_FILE        # its rows, for the shell
  dev/varka_deopt_census.py --self-test

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

With --guard it reads GUARD_DIR/<width>/run-* for each row of the count file
(dev/varka_deopt_census_guard.tsv) and counts the kernels PAST THE CUT: not settled by the
warm-up's deadline, or settled after it, with more than four profile_predicate traps. The exit
status is 1 when a width counts more than its ratchet, has no kernel, or a JVM's exit file is
not 0; a count below the ratchet passes and asks for the ratchet to be lowered. --self-test runs
the guard over a planted cycling kernel and a settled one, recorded from a guard run's logs.
"""

import argparse
import collections
import contextlib
import glob
import io
import os
import re
import statistics
import sys
import tempfile

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
        late = [r for r in rs if past_cut(r, 10_000)]
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


def collect(dirs):
    """The judged rows of every kernel in the run directories, sorted by lane and draw."""
    rows = []
    for d in dirs:
        for kernel in read_run(d):
            rows.append({**judge(kernel), "run": kernel["run"], "lane": kernel["lane"],
                         "k": kernel["k"]})  # fmt: skip
    rows.sort(key=lambda r: (r["lane"], r["k"]))
    return rows


def past_cut(row, deadline_ms):
    """Past the warm-up's deadline with the cycle's traps. The warm-up stops a few ms after its
    deadline, so a kernel not settled reads about the deadline itself and is counted by its
    flag, not by its milliseconds."""
    return row["pp_traps"] > 4 and (not row["settled"] or row["settle_ms"] > deadline_ms)


GUARD_COLUMNS = ["width", "seed", "from", "draws", "deadline_ms", "cpus", "cpu_base", "ratchet"]


def read_guard_file(path):
    """The count file's rows, as dicts of GUARD_COLUMNS; comments and blank lines skipped."""
    specs = []
    with open(path) as f:
        for line in f:
            if not line.strip() or line.startswith("#"):
                continue
            cells = line.rstrip("\n").split("\t")
            if cells == GUARD_COLUMNS:
                continue
            if cells[0] == "width" or len(cells) != len(GUARD_COLUMNS):
                raise ValueError(f"{path}: expected the columns {GUARD_COLUMNS}: {line!r}")
            spec = dict(zip(GUARD_COLUMNS, cells))
            for c in GUARD_COLUMNS[1:]:
                int(spec[c])  # a ValueError names the cell that is not a number
            specs.append(spec)
    if not specs:
        raise ValueError(f"{path}: no width")
    return specs


def guard(specs, guard_dir):
    """Prints each width's count against its ratchet and the verdict; returns the exit status."""
    failures, lower = [], []
    for spec in specs:
        width, ratchet = spec["width"], int(spec["ratchet"])
        deadline = int(spec["deadline_ms"])
        dirs = sorted(glob.glob(os.path.join(guard_dir, width, "run-*")))
        crashed = []
        for d in dirs:
            try:
                with open(os.path.join(d, "exit")) as f:
                    code = f.read().strip()
            except OSError:
                code = "missing"
            if code != "0":
                crashed.append(f"{os.path.basename(d)} exit {code}")
        rows = collect(dirs)
        counted = [r for r in rows if past_cut(r, deadline)]
        cycling = sum(r["cycling"] for r in rows)
        nm = sum(1 for r in rows if r["null_nodes"])
        print(
            f"width {width}: {len(rows)} kernels, NULL-mode {nm}; past the {deadline} ms cut with "
            f"more than four profile_predicate traps {len(counted)} (ratchet {ratchet}); "
            f"cycling by VARKA-189's rule {cycling}"
        )
        for r in counted:
            print(
                f"   {r['lane']:4s} draw {r['k']:3d} {r['null_nodes'] or '-'}: "
                f"{r['pp_traps']} profile_predicate traps, {r['max_method_pp']} on one method, "
                f"{'settled at ' + str(r['settle_ms']) + ' ms' if r['settled'] else 'not settled'}"
            )
        if crashed:
            failures.append(f"width {width}: {', '.join(crashed)}")
        if not rows:
            failures.append(f"width {width}: no kernel read under {guard_dir}/{width}")
        elif len(counted) > ratchet:
            failures.append(
                f"width {width}: {len(counted)} past the cut, above the ratchet {ratchet}"
            )
        elif len(counted) < ratchet:
            lower.append(f"width {width} to {len(counted)}")
    if failures:
        print("verdict: FAILED - " + "; ".join(failures))
        return 1
    if lower:
        print(
            "verdict: ok, below the ratchet - lower it in the count file ("
            + ", ".join(lower)
            + ") once a second run agrees"
        )
    else:
        print("verdict: ok, at the ratchet")
    return 0


# A guard run's lines, trimmed to two kernels (16 October 2026, cores 3-5, the preferred width):
# long-lane draw 19, a NULL-mode kernel that cycled on its masked loop until the deadline, and
# long-lane draw 18, which settled. Their labels lose the options, which nothing here reads.
_CLS = "org.apache.spark.sql.varka.execution.VarkaFusedProjection_"
_SELF_TEST_CENSUS = [
    ("LONG", 19, _CLS + "wfuzz45", "false", 10021,
     "roots=[(cmp:GT (int:ADD:NULL col:0:long col:0:long) (greatest col:0:long col:1:long))]"
     " options=OPTS length=16 patterns=null-free,every-5th forceMasked=false"),
    ("LONG", 18, _CLS + "wfuzz44", "true", 619,
     "roots=[(divc:1000 (neg:FAIL col:1:long))] options=OPTS length=15 forceMasked=false"),
]  # fmt: skip
_SELF_TEST_NOT_ENTRANT = (
    "[{t}s][jit,compilation] {cid}       4       " + _CLS + "wfuzz45::loopMasked0 (468 bytes)"
    "   made not entrant: uncommon trap"
)
_SELF_TEST_TRAP = (
    "[{t}s][deoptimization ] cid={cid}     level=4 "
    + _CLS
    + "wfuzz45.loopMasked0([J[J[I[J[J[I[JI)I"
    " trap_bci=463 profile_predicate maybe_recompile pc=0x000076629c883e84"
)


def _write_run(run_dir, kernels, traps, exit_code="0"):
    os.makedirs(run_dir)
    with open(os.path.join(run_dir, "run.log"), "w") as f:
        for lane, k, cls, settled, ms, label in kernels:
            f.write(f"VARKA_CENSUS\t{lane}\t{k}\twarm-same\t{cls}\t{settled}\t{ms}\t{label}\n")
    with open(os.path.join(run_dir, "xlog.txt"), "w") as f:
        for i in range(traps):
            cid = 7368 + 2 * i
            t = f"{55.7 + 0.1 * i:.3f}"
            f.write(_SELF_TEST_TRAP.format(t=t, cid=cid) + "\n")
            f.write(_SELF_TEST_NOT_ENTRANT.format(t=t, cid=cid) + "\n")
    if exit_code is not None:
        with open(os.path.join(run_dir, "exit"), "w") as f:
            f.write(exit_code + "\n")


def self_test():
    """The guard over a planted cycling kernel: fails above the ratchet, passes at it."""
    cycling, settled = _SELF_TEST_CENSUS
    spec = dict(zip(GUARD_COLUMNS, ["pref", "20260903", "0", "1", "10000", "3", "3", "0"]))
    cases = [
        # (name, kernels, traps, exit file, ratchet, expected status, expected verdict start)
        ("a planted cycling kernel above the ratchet", [settled, cycling], 6, "0", 0, 1,
         "verdict: FAILED"),
        ("a planted cycling kernel at the ratchet", [settled, cycling], 6, "0", 1, 0,
         "verdict: ok, at"),
        ("four traps are not the cycle", [settled, cycling], 4, "0", 0, 0, "verdict: ok, at"),
        ("a settled kernel below the ratchet", [settled], 0, "0", 1, 0, "verdict: ok, below"),
        ("a JVM that failed", [settled], 0, "1", 1, 1, "verdict: FAILED"),
        ("a JVM that left no exit file", [settled], 0, None, 1, 1, "verdict: FAILED"),
        ("no kernel", [], 0, "0", 1, 1, "verdict: FAILED"),
    ]  # fmt: skip
    bad = 0
    for name, kernels, traps, exit_code, ratchet, want, verdict in cases:
        with tempfile.TemporaryDirectory() as tmp:
            _write_run(os.path.join(tmp, "pref", "run-0"), kernels, traps, exit_code)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                got = guard([{**spec, "ratchet": str(ratchet)}], tmp)
            lines = out.getvalue().splitlines()
            if got != want or not lines or not lines[-1].startswith(verdict):
                bad += 1
                print(f"self-test FAILED: {name}: status {got}, wanted {want}")
                print("\n".join("   " + line for line in lines))
    print("self-test: ok" if not bad else f"self-test: {bad} of {len(cases)} failed")
    return 1 if bad else 0


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--tsv", help="write one row per kernel here")
    p.add_argument("--label", default="census")
    p.add_argument("--guard", metavar="COUNT_FILE", help="the guard's verdict over GUARD_DIR")
    p.add_argument("--guard-rows", metavar="COUNT_FILE", help="print the count file's rows")
    p.add_argument("--self-test", action="store_true", help="the guard over recorded kernels")
    p.add_argument("run_dirs", nargs="*", metavar="RUN_DIR")
    args = p.parse_args()
    if args.self_test:
        return self_test()
    if args.guard_rows:
        # The shell reads the count file through this, so the two never parse it differently.
        for spec in read_guard_file(args.guard_rows):
            print("\t".join(spec[c] for c in GUARD_COLUMNS))
        return 0
    if args.guard:
        if len(args.run_dirs) != 1:
            p.error("--guard takes one GUARD_DIR")
        return guard(read_guard_file(args.guard), args.run_dirs[0])
    if not args.run_dirs:
        p.error("no RUN_DIR")
    dirs = [d for pattern in args.run_dirs for d in sorted(glob.glob(pattern))]
    rows = collect(dirs)
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
