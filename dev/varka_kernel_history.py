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
# Reads the fork logs of dev/varka_kernel_history.sh (VARKA-257) and says, per case - the vector
# width, the template prewarm on or off, and N, the kernels warmed before the kernel under test -
# how often that kernel landed in a slow mode, and how often the history's own kernels were
# released by the warm-up still allocating, by their position in the history. A case also names
# the kernel under test (the cheap tails, or e2e for VarkaWarmupEndToEndSuite's projection) and the
# unwarmed kernels run before anything was warmed (cold), and each fork says after how many kernel
# classes the template prewarm ran (VARKA-383). The directory may hold the logs of several shards
# merged, one provenance-<shard>.txt each, whose CPUs are listed.
#   dev/varka_kernel_history.py --dir target/varka-kernel-history/<date-time>
#   dev/varka_kernel_history.py --dir DIR --guard --max-slow 0   # the nightly's verdict
# A fork's kernel under test is slow by its rate, as the inlining-cliff reader cuts it (the median
# of its last three seconds over --slow-at nanoseconds a row: the loop past C1's limit), or by its
# allocation (over --alloc-at bytes a call in its last second: a C2 compile that kept a vector box
# per loop iteration, VARKA-295), and both are reported apart. A fork whose log has no DONE line,
# or whose JVM exited with a status other than the probe's own (0, 1 for a wrong answer, 2 for a
# harness error), did not finish; a fork whose timed calls declined is "declined", since its rate
# measured no loop. A wrong answer or a harness error, from any kernel of any fork, is listed, and
# under --guard fails, as do an unfinished or declined fork and more slow forks than --max-slow.
import argparse
import glob
import os
import re
import statistics
import sys
from collections import defaultdict

PREFIX = "VARKA_HISTORY_"
NAME = re.compile(r"width-(\w+)-prewarm-(\w+)-after-(\d+)-fork-(\d+)\.log$")


def fields(text):
    return dict(part.split("=", 1) for part in text.split() if "=" in part)


def read_fork(path, slow_at, alloc_at):
    m = NAME.search(os.path.basename(path))
    fork = {
        "path": path,
        "width": m.group(1),
        "prewarm": m.group(2),
        "after": int(m.group(3)),
        "fork": int(m.group(4)),
        "bits": None,
        "test_kind": "tails",
        "cold": 0,
        "cold_kernels": [],
        "kernels": [],
        "test": {},
        "rates": [],
        "alloc": None,
        "code": "",
        "wrong": [],
        "errors": [],
        "done": None,
        "skipped": 0,
    }
    with open(path, errors="replace") as log:
        for raw in log:
            line = raw.strip()
            if not line.startswith(PREFIX):
                continue
            key, _, rest = line[len(PREFIX) :].partition("=")
            if key == "BEGIN":
                begin = fields(rest)
                fork["bits"] = begin.get("bits")
                fork["test_kind"] = begin.get("test", "tails")
                fork["cold"] = int(begin.get("cold", "0"))
            elif key == "COLD":
                fork["cold_kernels"].append(fields(rest))
            elif key == "KERNEL":
                f = fields(rest)
                f["position"] = int(rest.split()[0])
                fork["kernels"].append(f)
            elif key == "SKIPPED":
                fork["skipped"] += 1
            elif key == "TEST":
                fork["test"] = fields(rest)
            elif key == "RATE":
                fork["rates"].append(float(rest.split()[1]))
            elif key == "ALLOC":
                fork["alloc"] = float(rest.split()[0])
            elif key == "CODE":
                fork["code"] = rest
            elif key == "WRONG":
                fork["wrong"].append(rest)
            elif key == "ERROR":
                fork["errors"].append(rest)
            elif key == "DONE":
                fork["done"] = fields(rest)
    exit_path = path[: -len(".log")] + ".exit"
    exit_status = None
    if os.path.exists(exit_path):
        with open(exit_path) as f:
            exit_status = f.read().strip()
    last = fork["rates"][-3:]
    fork["ns_per_row"] = statistics.median(last) if last else None
    if (
        fork["done"] is None
        or fork["ns_per_row"] is None
        or fork["alloc"] is None
        or exit_status not in (None, "0", "1", "2")
    ):
        fork["verdict"] = "unfinished"
    elif fork["done"].get("runStatus", "0") != "0":
        fork["verdict"] = "declined"
    elif fork["ns_per_row"] > slow_at:
        fork["verdict"] = "slow-rate"
    elif fork["alloc"] > alloc_at:
        fork["verdict"] = "slow-alloc"
    else:
        fork["verdict"] = "fast"
    return fork


# Positions in the history, bucketed, for the history kernels' own release counts.
BUCKETS = [(0, 0), (1, 3), (4, 15), (16, 63), (64, 1 << 30)]


def bucket(position):
    for lo, hi in BUCKETS:
        if lo <= position <= hi:
            return "%d" % lo if lo == hi else "%d-%s" % (lo, hi if hi < 1 << 30 else "")
    return "?"


def label(fork):
    """A fork's width as shown: its preferred int vector bits, once it printed them."""
    return "%s bits" % fork["bits"] if fork["bits"] else fork["width"]


def variant(fork):
    """The kernel under test and the cold prefix, where they are not the defaults."""
    parts = []
    if fork["test_kind"] != "tails":
        parts.append(fork["test_kind"])
    if fork["cold"]:
        parts.append("cold %d" % fork["cold"])
    return ", ".join(parts)


def template_after(fork):
    """Kernel classes defined before the template prewarm ran, '-' if it did not or is unread."""
    after = (fork["done"] or {}).get("templateAfter", "-1")
    return "-" if after == "-1" else after


def main():
    parser = argparse.ArgumentParser(description="The slow mode's frequency after N kernels.")
    parser.add_argument("--dir", required=True)
    parser.add_argument("--slow-at", type=float, default=40.0, help="ns a row (default 40)")
    parser.add_argument(
        "--alloc-at", type=float, default=2048.0, help="bytes a call (default 2048)"
    )
    parser.add_argument("--guard", action="store_true")
    parser.add_argument("--max-slow", type=int, default=0)
    args = parser.parse_args()
    paths = sorted(glob.glob(os.path.join(args.dir, "width-*-fork-*.log")))
    if not paths:
        sys.exit("no fork log in %s" % args.dir)
    all_forks = [read_fork(p, args.slow_at, args.alloc_at) for p in paths]
    launched = {}
    for prov in sorted(glob.glob(os.path.join(args.dir, "provenance*.txt"))):
        with open(prov, errors="replace") as f:
            lines = f.read().splitlines()
        cpu = next((ln.split(":", 1)[1].strip() for ln in lines if ln.startswith("cpu:")), "?")
        print("%s: %s" % (os.path.basename(prov), cpu))
        for ln in lines:
            if ln.startswith("args:"):
                launched.setdefault("test", fields(ln).get("test", "tails"))
                launched.setdefault("cold", int(fields(ln).get("cold", "0")))
    # A fork whose JVM died before its BEGIN line belongs to the case the launcher started it in.
    for f in all_forks:
        if f["bits"] is None:
            f["test_kind"] = launched.get("test", "tails")
            f["cold"] = launched.get("cold", 0)

    print(
        "%-38s %-11s %8s %9s %8s  %s"
        % (
            "case (bits, prewarm, N)",
            "verdict",
            "ns/row",
            "bytes/call",
            "tmpl after",
            "warm-up | loop tiers",
        )
    )
    for f in sorted(
        all_forks,
        key=lambda f: (label(f), f["test_kind"], f["cold"], f["prewarm"], f["after"], f["fork"]),
    ):
        print(
            "%-38s %-11s %8s %9s %8s  %s %sms | %s"
            % (
                "%s, %s, N=%d #%d%s"
                % (
                    label(f),
                    f["prewarm"],
                    f["after"],
                    f["fork"],
                    " (%s)" % variant(f) if variant(f) else "",
                ),
                f["verdict"],
                "%.1f" % f["ns_per_row"] if f["ns_per_row"] is not None else "-",
                "%.0f" % f["alloc"] if f["alloc"] is not None else "-",
                template_after(f),
                f["test"].get("state", "-"),
                f["test"].get("millis", "-"),
                f["code"] or "-",
            )
        )

    print()
    print(
        "the kernel under test: slow by rate / slow by allocation / fast / unfinished or declined"
    )
    cases = defaultdict(list)
    for f in all_forks:
        # By the bits the fork read, not the width it was asked for: a run with no width
        # requirement may land its splits on machines of two widths.
        cases[(label(f), f["prewarm"], f["after"], variant(f))].append(f)
    slow = 0
    for case in sorted(cases):
        group = cases[case]
        n = defaultdict(int)
        for f in group:
            n[f["verdict"]] += 1
        slow += n["slow-rate"] + n["slow-alloc"]
        fast = [f["ns_per_row"] for f in group if f["verdict"] == "fast"]
        released = sum(1 for f in group if f["test"].get("state") == "RELEASED")
        # The e2e kernel's warm-up verdict is the reading its case exists for, so it is always
        # shown; the cheap tails' only when one released.
        shown = released or group[0]["test_kind"] != "tails"
        print(
            "  %-9s prewarm %-5s, N=%-3d%s %3d / %3d / %3d / %3d   slow %d of %d%s%s"
            % (
                case[0],
                case[1],
                case[2],
                " (%s)" % case[3] if case[3] else "",
                n["slow-rate"],
                n["slow-alloc"],
                n["fast"],
                n["unfinished"] + n["declined"],
                n["slow-rate"] + n["slow-alloc"],
                len(group),
                "; fast %.1f to %.1f ns/row" % (min(fast), max(fast)) if fast else "",
                "; warm-up released %d of %d" % (released, len(group)) if shown else "",
            )
        )

    print()
    print("the history's kernels: released by the warm-up / warmed, by lane and position")
    # By lane too: the history alternates the lanes, int at even positions, so position 0, the
    # control, is an int kernel, and a long kernel that releases alone shows at every position.
    by_position = defaultdict(lambda: [0, 0, 0])
    for f in all_forks:
        for k in f["kernels"]:
            key = (label(f), f["prewarm"], variant(f), k.get("lane", "?"), bucket(k["position"]))
            by_position[key][1] += 1
            if k.get("state") == "RELEASED":
                by_position[key][0] += 1
                # Every loop method with C2 code in use: compiled and still allocating.
                if "tiers" in k and ":none" not in k["tiers"]:
                    by_position[key][2] += 1
    for key in sorted(by_position, key=lambda k: k[:4] + (int(k[4].split("-")[0]),)):
        released, total, compiled = by_position[key]
        print(
            "  %-9s prewarm %-5s,%s %-4s positions %-6s %4d / %4d (%.1f%%), %d with every loop C2"
            % (
                key[0],
                key[1],
                " (%s)" % key[2] if key[2] else "",
                key[3],
                key[4],
                released,
                total,
                100.0 * released / total,
                compiled,
            )
        )

    wrong = [(f, w) for f in all_forks for w in f["wrong"]]
    errors = [(f, e) for f in all_forks for e in f["errors"]]
    unfinished = sum(1 for f in all_forks if f["verdict"] in ("unfinished", "declined"))
    skipped = sum(f["skipped"] for f in all_forks)
    compared = sum(
        len(f["cold_kernels"]) + len(f["kernels"]) + (1 if f["done"] else 0) for f in all_forks
    )
    print()
    for f, w in wrong:
        print("WRONG ANSWER in %s: %s" % (os.path.basename(f["path"]), w))
    for f, e in errors:
        print("HARNESS ERROR in %s: %s" % (os.path.basename(f["path"]), e))
    print(
        "verdict: %d of %d forks slow%s; %d wrong answers%s in %d kernels compared%s"
        % (
            slow,
            len(all_forks),
            ", %d unfinished or declined" % unfinished if unfinished else "",
            len(wrong),
            ", %d harness errors" % len(errors) if errors else "",
            compared,
            "; %d draws skipped (declined or repeated)" % skipped if skipped else "",
        )
    )
    if args.guard and (wrong or errors or unfinished or slow > args.max_slow):
        sys.exit(1)


if __name__ == "__main__":
    main()
