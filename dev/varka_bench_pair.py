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
"""Compare a benchmark class's results on a base and a head, run in alternating batches.

    dev/varka_bench_pair.py compare --base B1 B2 ... --head H1 H2 ... [--threshold 10]
    dev/varka_bench_pair.py settled --base B1 B2 ... --head H1 H2 ... [--within 3]
    dev/varka_bench_pair.py --selftest

`dev/varka_bench_pair.sh` runs a class on a merge base and on the head, alternately, N rounds, and
hands the files to this. DuckDB benchmarks a pull request against its merge base in alternating
batches, compares medians, fails at 10% slower and stops early within 3%
(`scripts/regression/test_runner.py`; `sql/varka/plans/m7/READING.md` 9). One regeneration on one
machine cannot tell a pull request's change from the machine's day (a same-file diff of two
regenerations read 8 to 32% of unchanged rows as moved, VARKA-77); alternating the two sides
cancels what drifts between batches, and the median of each side's runs discards the batch that
a neighbour on the runner disturbed.

`compare` matches rows by (table, case, occurrence) as `dev/varka_bench_diff.py` does, takes each
side's median rate over its files, and prints the change in percent (+ is faster, - is slower, the
sign of the diff tool). A row is *slower* when its median change is at most -threshold and *faster*
at least +threshold; the exit status is 1 if any row is slower, which is the gate. `settled` says
whether the rounds so far already show every row within `--within` percent either way, in which
case more rounds would not change the verdict; the driver stops early on it, after at least three.
"""

import argparse
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import varka_bench_diff as bench


def rates(paths):
    """[ ({key: rate}, {key: ns a row}) per file ], the rows by (table, case, occurrence)."""
    out = []
    for p in paths:
        with open(p, encoding="utf-8") as f:
            text = f.read()
        out.append((bench.parse(text)[0], bench.per_row(text)))
    return out


def medians(runs, which):
    """{key: median of column `which` (0 the rate, 1 the ns a row) over the runs with the row}."""
    keys = dict.fromkeys(k for r in runs for k in r[which])
    return {k: statistics.median([r[which][k] for r in runs if k in r[which]]) for k in keys}


def changes(base_runs, head_runs):
    """{key: percent change of the head against the base, + faster}, from the medians.

    A cold case prints its rate as 0.0 and only its time a row carries the digits, so the change
    is read the way the diff tool reads it, from whichever printed figure has more of them.
    """
    br, bn = medians(base_runs, 0), medians(base_runs, 1)
    hr, hn = medians(head_runs, 0), medians(head_runs, 1)
    out = {}
    for k in br:
        if k in hr:
            c = bench.rate_change(br[k], hr[k], bn.get(k), hn.get(k))
            if c == c:  # not NaN
                out[k] = c
    return out


def verdicts(ch, threshold):
    slower = [k for k, c in ch.items() if c <= -threshold]
    faster = [k for k, c in ch.items() if c >= threshold]
    return slower, faster


def settled(ch, within):
    return bool(ch) and all(abs(c) <= within for c in ch.values())


def label(key):
    table, case, occ = key
    return f"[{table}] {case}" + (f" #{occ}" if occ > 1 else "")


def selftest():
    def run(a, b=None):
        # A file's worth of rows: rate and ns a row, ns the reciprocal of the rate (1000 / rate).
        return (
            {("t", "a", 1): a, ("t", "b", 1): 50.0},
            {("t", "a", 1): 1000.0 / a, ("t", "b", 1): 20.0},
        )

    base = [run(100.0), run(110.0), run(90.0)]
    ch = changes(base, [run(101.0)] * 3)
    assert abs(ch[("t", "a", 1)] - 1.0) < 1e-6 and settled(ch, 3.0), ch
    slow, fast = verdicts(changes(base, [run(70.0)] * 3), 10.0)
    assert slow == [("t", "a", 1)] and not fast
    # A single disturbed batch does not move the median: one head run at 20 against two at 100.
    assert verdicts(changes([run(100.0)] * 3, [run(20.0), run(100.0), run(100.0)]), 10.0) == (
        [],
        [],
    )
    # A row on one side only is not compared.
    only_x = ({("t", "x", 1): 1.0}, {("t", "x", 1): 1.0})
    only_y = ({("t", "y", 1): 1.0}, {("t", "y", 1): 1.0})
    assert changes([only_x], [only_y]) == {}
    print("varka_bench_pair: selftest passed")


def main():
    if "--selftest" in sys.argv:
        selftest()
        return
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("cmd", choices=["compare", "settled"])
    ap.add_argument("--base", nargs="+", required=True)
    ap.add_argument("--head", nargs="+", required=True)
    ap.add_argument("--threshold", type=float, default=10.0)
    ap.add_argument("--within", type=float, default=3.0)
    args = ap.parse_args()
    ch = changes(rates(args.base), rates(args.head))
    if args.cmd == "settled":
        print("settled" if settled(ch, args.within) else "open")
        return
    if not ch:
        print("no row is common to both sides: nothing was compared", file=sys.stderr)
        sys.exit(2)
    only = len(medians(rates(args.base), 0).keys() ^ medians(rates(args.head), 0).keys())
    if only:
        print(f"warning: {only} rows are on one side only and were not compared", file=sys.stderr)
    slower, faster = verdicts(ch, args.threshold)
    for key, c in sorted(ch.items(), key=lambda kv: kv[1]):
        mark = "  <-- slower" if key in slower else ("  <-- faster" if key in faster else "")
        print(f"{c:+7.1f}%  {label(key)}{mark}")
    print(
        f"\n{len(ch)} rows, {len(slower)} slower and {len(faster)} faster by {args.threshold:g}% "
        f"or more, medians of {len(args.base)} base and {len(args.head)} head runs"
    )
    sys.exit(1 if slower else 0)


if __name__ == "__main__":
    main()
