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
"""Compare Spark benchmark results files row by row, the way the Varka plans do.

Two modes:

  dev/varka_bench_diff.py OLD NEW              # before/after: same case, two files
  dev/varka_bench_diff.py --git REV FILE       # before = FILE at git revision REV
  dev/varka_bench_diff.py --within FILE --ab "Julian map" "century-then-year"
                                               # A/B: pairs of rows in one file whose
                                               # names differ only by those two labels

Rows are matched by (table, case name), where the table is the name on each
results table's header line ("date_add over 1000000 rows:  Best Time(ms) ...") -
present both in the generated files and in a plain run's stdout, which the
128-bit companion files are made from. The rate column (M rows/s,
computed by Spark's Benchmark from the best time) is what is compared, since
that is what every plan quotes. Rows moving by at least --threshold percent
are marked; rows matching --control (the scalar anchors) are listed first,
because if they moved the machine moved and nothing else in the file can be
read. Exit status 0 always; this is a reading aid, not a gate.
"""
import argparse
import re
import subprocess
import sys

ROW = re.compile(r"^(.*?)\s+(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)X\s*$")
HEADER = re.compile(r"^(.*?):\s+Best Time\(ms\)")


def parse(text):
    """{(table, case): rate} plus the ordered list of keys, in file order."""
    rates, order, table = {}, [], ""
    for line in text.splitlines():
        line = re.sub(r"^\[info\] ?", "", line)
        h = HEADER.match(line)
        if h:
            table = h.group(1).strip()
            continue
        m = ROW.match(line)
        if m:
            key = (table, m.group(1).strip())
            rates[key] = float(m.group(5))
            order.append(key)
    return rates, order


def read(path, rev=None):
    if rev is None:
        with open(path, encoding="utf-8") as f:
            return f.read()
    return subprocess.run(["git", "show", f"{rev}:{path}"], check=True,
                          capture_output=True, text=True).stdout


def pct(before, after):
    return (after / before - 1.0) * 100.0 if before else float("nan")


def print_rows(rows, threshold):
    # A case name repeats across tables ("hand-written kernel, null-free" is in date_add's
    # table and in datediff's), so name the table wherever the case alone is ambiguous.
    counts = {}
    for _, case, _, _ in rows:
        counts[case] = counts.get(case, 0) + 1
    labels = [(f"[{table}] {case}" if counts[case] > 1 else case, b, a)
              for table, case, b, a in rows]
    width = max((len(label) for label, _, _ in labels), default=10)
    print(f"{'case':{width}}  {'before':>9}  {'after':>9}  {'change':>8}")
    for label, b, a in labels:
        change = pct(b, a)
        mark = " <--" if abs(change) >= threshold else ""
        print(f"{label:{width}}  {b:9.1f}  {a:9.1f}  {change:+7.1f}%{mark}")


def before_after(old_text, new_text, args):
    old, _ = parse(old_text)
    new, order = parse(new_text)
    control = re.compile(args.control)
    controls, moved, same, missing = [], [], [], []
    for key in order:
        section, case = key
        if key not in old:
            missing.append(key)
            continue
        row = (section, case, old[key], new[key])
        if control.search(case):
            controls.append(row)
        elif abs(pct(old[key], new[key])) >= args.threshold:
            moved.append(row)
        else:
            same.append(row)
    if controls:
        print(f"-- controls ({args.control}); if these moved, the machine moved --")
        print_rows(controls, args.threshold)
        print()
    print(f"-- moved by at least {args.threshold:g}% --")
    print_rows(moved, args.threshold) if moved else print("(none)")
    if args.all and same:
        print()
        print("-- within the threshold --")
        print_rows(same, args.threshold)
    gone = [k for k in old if k not in new]
    if missing or gone:
        print()
        for k in missing:
            print(f"new only: [{k[0]}] {k[1]}")
        for k in gone:
            print(f"old only: [{k[0]}] {k[1]}")


def within(text, label_a, label_b, threshold):
    rates, order = parse(text)
    rows = []
    for section, case in order:
        if label_a in case:
            twin = (section, case.replace(label_a, label_b))
            if twin in rates:
                rows.append((section, case, rates[twin], rates[(section, case)]))
    print(f"-- {label_a} (after) against {label_b} (before), same run --")
    print_rows(rows, threshold) if rows else print("(no pairs found)")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("old", nargs="?", help="the older results file")
    p.add_argument("new", nargs="?", help="the newer results file")
    p.add_argument("--git", metavar="REV", help="read the old side of FILE from this revision")
    p.add_argument("--within", metavar="FILE", help="A/B pairs inside one file")
    p.add_argument("--ab", nargs=2, metavar=("A", "B"),
                   help="the two labels that distinguish an A/B pair's rows")
    p.add_argument("--threshold", type=float, default=3.0,
                   help="percent change that counts as moved (default 3)")
    p.add_argument("--control", default=r"per-row|scalar|LocalDate|row engine",
                   help="regex naming the control rows (default: the scalar anchors)")
    p.add_argument("--all", action="store_true", help="also list rows within the threshold")
    args = p.parse_args()

    if args.within:
        if not args.ab:
            p.error("--within needs --ab A B")
        within(read(args.within), args.ab[0], args.ab[1], args.threshold)
        return
    if args.git:
        if not args.old or args.new:
            p.error("--git REV takes exactly one FILE")
        before_after(read(args.old, args.git), read(args.old), args)
        return
    if not (args.old and args.new):
        p.error("give OLD NEW, or --git REV FILE, or --within FILE --ab A B")
    before_after(read(args.old), read(args.new), args)


if __name__ == "__main__":
    main()
