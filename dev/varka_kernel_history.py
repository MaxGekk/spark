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
# released by the warm-up still allocating, by their position in the history.
#   dev/varka_kernel_history.py --dir target/varka-kernel-history/<date-time>
#   dev/varka_kernel_history.py --dir DIR --guard --max-slow 0   # the nightly's verdict
# A fork's kernel under test is slow by its rate, as the inlining-cliff reader cuts it (the median
# of its last three seconds over --slow-at nanoseconds a row: the loop past C1's limit), or by its
# allocation (over --alloc-at bytes a call in its last second: a C2 compile that kept a vector box
# per loop iteration, VARKA-295), and both are reported apart. A fork whose log has no DONE line
# did not finish. A wrong answer, from any kernel of any fork, is listed, and under --guard fails,
# as do an unfinished fork and more slow forks than --max-slow.
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
        "kernels": [],
        "test": {},
        "rates": [],
        "alloc": None,
        "code": "",
        "wrong": [],
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
                fork["bits"] = fields(rest).get("bits")
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
            elif key == "DONE":
                fork["done"] = fields(rest)
    last = fork["rates"][-3:]
    fork["ns_per_row"] = statistics.median(last) if last else None
    if fork["done"] is None or fork["ns_per_row"] is None or fork["alloc"] is None:
        fork["verdict"] = "unfinished"
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

    print(
        "%-28s %-11s %8s %9s  %s"
        % ("case (bits, prewarm, N)", "verdict", "ns/row", "bytes/call", "warm-up | loop tiers")
    )
    for f in sorted(all_forks, key=lambda f: (f["width"], f["prewarm"], f["after"], f["fork"])):
        print(
            "%-28s %-11s %8s %9s  %s %sms | %s"
            % (
                "%s, %s, N=%d #%d" % (f["bits"] or f["width"], f["prewarm"], f["after"], f["fork"]),
                f["verdict"],
                "%.1f" % f["ns_per_row"] if f["ns_per_row"] is not None else "-",
                "%.0f" % f["alloc"] if f["alloc"] is not None else "-",
                f["test"].get("state", "-"),
                f["test"].get("millis", "-"),
                f["code"] or "-",
            )
        )

    print()
    print("the kernel under test: slow by rate / slow by allocation / fast / unfinished per case")
    cases = defaultdict(list)
    for f in all_forks:
        cases[(f["bits"] or f["width"], f["prewarm"], f["after"])].append(f)
    slow = 0
    for case in sorted(cases, key=lambda c: (c[0], c[1], c[2])):
        group = cases[case]
        n = defaultdict(int)
        for f in group:
            n[f["verdict"]] += 1
        slow += n["slow-rate"] + n["slow-alloc"]
        fast = [f["ns_per_row"] for f in group if f["verdict"] == "fast"]
        released = sum(1 for f in group if f["test"].get("state") == "RELEASED")
        print(
            "  %4s bits, prewarm %-3s, N=%-3d %3d / %3d / %3d / %3d   slow %d of %d%s%s"
            % (
                case[0],
                case[1],
                case[2],
                n["slow-rate"],
                n["slow-alloc"],
                n["fast"],
                n["unfinished"],
                n["slow-rate"] + n["slow-alloc"],
                len(group),
                "; fast %.1f to %.1f ns/row" % (min(fast), max(fast)) if fast else "",
                "; warm-up released %d" % released if released else "",
            )
        )

    print()
    print("the history's kernels: released by the warm-up / warmed, by position in the history")
    by_position = defaultdict(lambda: [0, 0])
    for f in all_forks:
        for k in f["kernels"]:
            key = (f["bits"] or f["width"], f["prewarm"], bucket(k["position"]))
            by_position[key][1] += 1
            if k.get("state") == "RELEASED":
                by_position[key][0] += 1
    for key in sorted(by_position, key=lambda k: (k[0], k[1], int(k[2].split("-")[0]))):
        released, total = by_position[key]
        print(
            "  %4s bits, prewarm %-3s, positions %-6s %4d / %4d (%.1f%%)"
            % (key[0], key[1], key[2], released, total, 100.0 * released / total)
        )

    wrong = [(f, w) for f in all_forks for w in f["wrong"]]
    unfinished = sum(1 for f in all_forks if f["verdict"] == "unfinished")
    compared = sum(len(f["kernels"]) + (1 if f["done"] else 0) for f in all_forks)
    print()
    for f, w in wrong:
        print("WRONG ANSWER in %s: %s" % (os.path.basename(f["path"]), w))
    print(
        "verdict: %d of %d forks slow%s; %d wrong answers in %d kernels compared"
        % (
            slow,
            len(all_forks),
            ", %d unfinished" % unfinished if unfinished else "",
            len(wrong),
            compared,
        )
    )
    if args.guard and (wrong or unfinished or slow > args.max_slow):
        sys.exit(1)


if __name__ == "__main__":
    main()
