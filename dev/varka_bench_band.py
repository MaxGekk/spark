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
# How far apart do repeated runs of one benchmark land, per case?
#
#   dev/varka_bench_band.py run1.txt run2.txt ...            # the band, per case
#   dev/varka_bench_band.py --split-half run1.txt ...        # does the band reproduce?
#
# A regeneration's diff prints "moved 14.2%" against nothing, so every task since
# task 52 has had to run the whole file twice to tell its own change from the
# run's noise. This measures what an unchanged file does, which is the number the
# diff needs in order to say whether a move means anything.
#
# `--split-half` is the question that decides what the band can be. It splits the
# runs in two, measures each half independently, and reports how well one half's
# per-case spread predicts the other's. If it predicts well, a per-case band is
# meaningful and a quiet case may be held to a tighter threshold than a noisy
# one. If it does not - which is what `PLAN_MILESTONE_4.md`'s debt register
# suggests, recording "a different cluster each run" - then only a single
# file-level threshold is defensible, and claiming per-case precision would be
# inventing it.
#
# Keys are (table, case, occurrence). The occurrence index is not decoration:
# four sections of the parity file share one table header, and two rows in it
# share a case name as well, so a (table, case) pair is not unique and a keying
# that assumes it is silently merges rows.

import re
import statistics
import sys

ROW = re.compile(r"^(.*?)\s+(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)X\s*$")
HEADER = re.compile(r"^(.*?):\s+Best Time\(ms\)")


def parse(path):
    """{(table, case, occurrence): rate} for one run's output."""
    rates, seen, table = {}, {}, ""
    with open(path, errors="replace") as f:
        for line in f:
            line = re.sub(r"^\[info\] ?", "", line.rstrip())
            h = HEADER.match(line)
            if h:
                table = h.group(1).strip()
                continue
            m = ROW.match(line)
            if m:
                base = (table, m.group(1).strip())
                seen[base] = seen.get(base, 0) + 1
                rates[base + (seen[base],)] = float(m.group(5))
    return rates


def spreads(runs):
    """{key: (spread_pct, lo, hi)} over the keys every run has."""
    common = set(runs[0])
    for r in runs[1:]:
        common &= set(r)
    out = {}
    for k in common:
        vs = [r[k] for r in runs]
        out[k] = ((max(vs) - min(vs)) / min(vs) * 100, min(vs), max(vs))
    return out


def summarise(label, sp):
    ps = sorted(v[0] for v in sp.values())
    print(f"\n== {label}: {len(ps)} cases")
    med, p90, worst = statistics.median(ps), ps[int(0.9 * len(ps))], ps[-1]
    print(f"   median {med:6.2f}%   p90 {p90:6.2f}%   max {worst:6.2f}%")
    for t in (3, 10, 20):
        print(f"   over {t:2d}%: {sum(1 for p in ps if p > t):4d}")


def main():
    args = sys.argv[1:]
    split = "--split-half" in args
    paths = [a for a in args if not a.startswith("--")]
    runs = [parse(p) for p in paths]
    for p, r in zip(paths, runs):
        print(f"{p}: {len(r)} rows")
    if len(runs) < 2:
        sys.exit("need at least two runs")

    if not split:
        sp = spreads(runs)
        summarise(f"band over {len(runs)} runs", sp)
        print(f"\n{'worst cases':70s} {'lo':>9s} {'hi':>9s} {'spread':>8s}")
        for k, (p, lo, hi) in sorted(sp.items(), key=lambda kv: -kv[1][0])[:15]:
            print(f"{(k[1] + ' | ' + k[0])[:70]:70s} {lo:9.1f} {hi:9.1f} {p:7.1f}%")
        return

    half = len(runs) // 2
    a, b = spreads(runs[:half]), spreads(runs[half:])
    summarise(f"half A (runs 1-{half})", a)
    summarise(f"half B (runs {half + 1}-{len(runs)})", b)
    common = sorted(set(a) & set(b))
    xs = [a[k][0] for k in common]
    ys = [b[k][0] for k in common]
    # Does a case that was noisy in one half come out noisy in the other? Reported
    # two ways: the correlation of the spreads, and the overlap of the two halves'
    # worst quartiles, which is what a per-case band would actually rely on.
    try:
        r = statistics.correlation(xs, ys)
    except statistics.StatisticsError:
        r = float("nan")
    q = max(1, len(common) // 4)
    wa = {k for k in sorted(common, key=lambda k: -a[k][0])[:q]}
    wb = {k for k in sorted(common, key=lambda k: -b[k][0])[:q]}
    print(f"\n== does the band reproduce, over {len(common)} shared cases")
    print(f"   correlation of per-case spread, half A vs half B : {r:.3f}")
    print(
        f"   worst-quartile overlap                           : {len(wa & wb)}/{q}"
        f"  (chance is about {q // 4})"
    )


if __name__ == "__main__":
    main()
