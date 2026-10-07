#!/usr/bin/env bash
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

"""Reads results/step5-*.txt (bench.sh's output) and prints the tables of VARKA-291 section 9.6.

Every arm is compared with the records on the same JDK: jdk25 arms with the jdk25 records, the V
layouts with the plain records on the early-access JDK, and the value records with those same
plain records. A ratio is the arm's time over the records' time, so under 1 is faster. Cold times
are medians over the fresh-JVM samples; warm times are the minimum over the measured iterations,
with the spread (the slowest over the fastest) beside it, since a claim under 1.3x is read against
the spread (sql/varka/AGENTS.md, "Measurements, not adjectives").
"""

import glob
import os
import re
import statistics
from collections import defaultdict

here = os.path.dirname(os.path.abspath(__file__))
results = os.path.join(here, "results")
ORDER = [
    "cheap_tails-22",
    "wide_int-1",
    "size_ladder-100",
    "size_ladder-200",
    "deep_chain-1024",
    "grown_ladder-2000",
]


def lines(mode, variant):
    """All of a mode's and variant's files: step5-MODE-VARIANT.txt and any -SUFFIX.txt beside it."""
    out = []
    for path in sorted(glob.glob(os.path.join(results, f"step5-{mode}-{variant}*.txt"))):
        out += [l.rstrip("\n") for l in open(path)]
    return out


def baseline_variant(variant):
    return "jdk25" if variant == "jdk25" else "plain"


def arm_label(variant, arm):
    return "value records" if variant == "value" else arm


def bytes_table():
    out = ["bytes a node (records measured with Instrumentation.getObjectSize, the rest computed)"]
    out.append(f"{'arm':<14}" + "".join(f"{g:>20}" for g in ORDER))
    rows = defaultdict(dict)
    for variant in ("jdk25", "plain", "value"):
        for l in lines("bytes", variant):
            m = re.match(r"BYTES (\S+) (\S+) nodes (\d+) (\S+) (\d+) bytes_per_node ([\d.]+)", l)
            if m:
                key = (
                    "value records"
                    if variant == "value"
                    else m[1] + ("" if variant == "jdk25" else " (ea)" if m[1] == "records" else "")
                )
                if m[1] == "records" and variant == "jdk25":
                    key = "records"
                rows[key][m[2]] = m[6]
    for key, d in rows.items():
        out.append(f"{key:<14}" + "".join(f"{d.get(g, '-'):>20}" for g in ORDER))
    return out


def cold_table():
    out = ["cold, fresh JVM each, median of the samples, microseconds; ratio over the records"]
    out.append(
        f"{'variant':<7} {'arm':<14} {'graph':<18} {'n':>3} {'build':>9} {'analyze':>9} "
        f"{'build+analyze':>13} {'ratio':>6} {'intern':>9} {'ratio':>6}"
    )
    data = defaultdict(lambda: defaultdict(list))
    for variant in ("jdk25", "plain", "value"):
        for l in lines("cold", variant):
            m = re.match(
                r"COLD (\S+) (\S+) nodes (\d+) build_ns (\d+) analyze_ns (\d+) "
                r"intern_ns (\d+)",
                l,
            )
            if m:
                data[(variant, m[1], m[2])]["b"].append(int(m[4]))
                data[(variant, m[1], m[2])]["a"].append(int(m[5]))
                data[(variant, m[1], m[2])]["i"].append(int(m[6]))
    for variant in ("jdk25", "plain", "value"):
        for graph in ORDER:
            base = data.get((baseline_variant(variant), "records", graph))
            for (v, arm, g), d in sorted(data.items()):
                if v != variant or g != graph:
                    continue
                b, a, i = (statistics.median(d[k]) / 1000 for k in "bai")
                ratio = ratio_i = "-"
                if base:
                    bb, ba, bi = (statistics.median(base[k]) / 1000 for k in "bai")
                    ratio = f"{(b + a) / (bb + ba):.2f}"
                    ratio_i = f"{i / bi:.2f}"
                out.append(
                    f"{variant:<7} {arm_label(variant, arm):<14} {graph:<18} "
                    f"{len(d['b']):>3} {b:>9.0f} {a:>9.0f} {b + a:>13.0f} {ratio:>6} "
                    f"{i:>9.0f} {ratio_i:>6}"
                )
    return out


def warm_table():
    out = [
        "warm, minimum over the measured iterations, microseconds an operation; spread = "
        "slowest/fastest iteration; ratio over the records"
    ]
    out.append(
        f"{'variant':<7} {'arm':<14} {'graph':<18} {'op':<8} {'min us':>10} {'spread':>7} "
        f"{'ratio':>6}"
    )
    data = defaultdict(list)
    for variant in ("jdk25", "plain", "value"):
        for l in lines("warm", variant):
            m = re.match(
                r"WARM (\S+) (\S+) nodes (\d+) op (\S+) iteration (\d+) ns_per_op "
                r"([\d.]+)",
                l,
            )
            if m:
                data[(variant, m[1], m[2], m[4])].append(float(m[6]))
    for variant in ("jdk25", "plain", "value"):
        for graph in ORDER:
            for op in ("build", "intern", "analyze"):
                base = data.get((baseline_variant(variant), "records", graph, op))
                for (v, arm, g, o), xs in sorted(data.items()):
                    if v != variant or g != graph or o != op:
                        continue
                    lo = min(xs) / 1000
                    ratio = f"{min(xs) / min(base):.2f}" if base else "-"
                    out.append(
                        f"{variant:<7} {arm_label(variant, arm):<14} {graph:<18} {op:<8} "
                        f"{lo:>10.2f} {max(xs) / min(xs):>7.2f} {ratio:>6}"
                    )
    return out


def identity_table():
    """The JDK 25 arms against records whose analysis memoizes by node identity, not structure."""
    out = [
        "against records with an identity memo (the fairer records analysis), JDK 25: ratio of "
        "the arm's time over theirs"
    ]
    out.append(f"{'arm':<10} {'graph':<18} {'cold build+analyze':>19} {'warm analyze':>13}")
    cold = defaultdict(lambda: defaultdict(list))
    for l in lines("cold", "jdk25"):
        m = re.match(r"COLD (\S+) (\S+) nodes (\d+) build_ns (\d+) analyze_ns (\d+)", l)
        if m:
            cold[(m[1], m[2])]["t"].append(int(m[4]) + int(m[5]))
    warm = defaultdict(list)
    for l in lines("warm", "jdk25"):
        m = re.match(
            r"WARM (\S+) (\S+) nodes (\d+) op analyze iteration (\d+) ns_per_op "
            r"([\d.]+)",
            l,
        )
        if m:
            warm[(m[1], m[2])].append(float(m[5]))
    for graph in ORDER:
        base_c = cold.get(("records-id", graph))
        base_w = warm.get(("records-id", graph))
        if not base_c or not base_w:
            continue
        for arm in ("records", "records-id", "A", "B", "C"):
            c = cold.get((arm, graph))
            w = warm.get((arm, graph))
            if not c or not w:
                continue
            rc = statistics.median(c["t"]) / statistics.median(base_c["t"])
            rw = min(w) / min(base_w)
            out.append(
                f"{arm:<10} {graph:<18} {rc:>19.2f} {rw:>13.2f}"
                f"   (cold {statistics.median(c['t']) / 1000:.0f} us, "
                f"warm analyze {min(w) / 1000:.2f} us)"
            )
    return out


def prewarm_table():
    """Cold again with the one-time costs paid on another graph first, and the cold rebuild."""
    out = [
        "cold with a pre-initialized JVM (one build, analyze, intern and rebuild of a 10-node "
        "graph first), median, microseconds; ratio of build+analyze over the records' (jdk25: "
        "the identity-memo ones)"
    ]
    out.append(
        f"{'variant':<7} {'arm':<14} {'graph':<18} {'build':>8} {'analyze':>8} "
        f"{'b+a':>8} {'ratio':>6} {'rebuild':>8}"
    )
    data = defaultdict(lambda: defaultdict(list))
    for variant in ("jdk25", "plain", "value"):
        for l in lines("coldprewarm", variant):
            m = re.match(
                r"COLD (\S+) (\S+) nodes (\d+) build_ns (\d+) analyze_ns (\d+) intern_ns (\d+) "
                r"rebuild_ns (\d+) prewarm 1",
                l,
            )
            if m:
                d = data[(variant, m[1], m[2])]
                d["b"].append(int(m[4]))
                d["a"].append(int(m[5]))
                d["r"].append(int(m[7]))
    for variant in ("jdk25", "plain", "value"):
        for graph in ORDER:
            base_arm = "records-id" if variant == "jdk25" else "records"
            base = data.get((baseline_variant(variant), base_arm, graph))
            for (v, arm, g), d in sorted(data.items()):
                if v != variant or g != graph:
                    continue
                b, a, r = (statistics.median(d[k]) / 1000 for k in "bar")
                ratio = "-"
                if base:
                    base_total = sum(statistics.median(base[k]) for k in "ba") / 1000
                    ratio = f"{(b + a) / base_total:.2f}"
                out.append(
                    f"{variant:<7} {arm_label(variant, arm):<14} {graph:<18} {b:>8.0f} "
                    f"{a:>8.0f} {b + a:>8.0f} {ratio:>6} {r:>8.0f}"
                )
    return out


def rebuild_table():
    out = [
        "warm rebuild (remap every row's children, rehash, re-intern), minimum, microseconds; "
        "spread; ratio over the records' reconstruction (a lower bound: records are not "
        "deduplicated)"
    ]
    out.append(
        f"{'variant':<7} {'arm':<14} {'graph':<18} {'min us':>10} {'spread':>7} {'ratio':>6}"
    )
    data = defaultdict(list)
    for variant in ("jdk25", "plain", "value"):
        for l in lines("warmrebuild", variant):
            m = re.match(
                r"WARM (\S+) (\S+) nodes (\d+) op rebuild iteration (\d+) ns_per_op "
                r"([\d.]+)",
                l,
            )
            if m:
                data[(variant, m[1], m[2])].append(float(m[5]))
    for variant in ("jdk25", "plain", "value"):
        for graph in ORDER:
            base = data.get((baseline_variant(variant), "records", graph))
            for (v, arm, g), xs in sorted(data.items()):
                if v != variant or g != graph:
                    continue
                ratio = f"{min(xs) / min(base):.2f}" if base else "-"
                out.append(
                    f"{variant:<7} {arm_label(variant, arm):<14} {graph:<18} "
                    f"{min(xs) / 1000:>10.2f} {max(xs) / min(xs):>7.2f} {ratio:>6}"
                )
    return out


def alloc_table():
    out = [
        "heap allocated by one build, once warm (3,000 builds): bytes, GCs and GC milliseconds "
        "over the run, ns a build"
    ]
    out.append(
        f"{'variant':<7} {'arm':<14} {'graph':<18} {'bytes/build':>12} {'gcs':>5} "
        f"{'gc ms':>6} {'ns/build':>10}"
    )
    for variant in ("jdk25", "plain", "value"):
        rows = {}
        for l in lines("alloc", variant):
            m = re.match(
                r"ALLOC (\S+) (\S+) nodes (\d+) builds (\d+) heap_bytes_per_build (\d+) "
                r"gc_count (\d+) gc_ms (\d+) ns_per_build (\d+)",
                l,
            )
            if m:
                rows[(m[2], m[1])] = (m[5], m[6], m[7], m[8])
        for graph in ORDER:
            for (g, arm), v in sorted(rows.items()):
                if g == graph:
                    out.append(
                        f"{variant:<7} {arm_label(variant, arm):<14} {graph:<18} "
                        f"{v[0]:>12} {v[1]:>5} {v[2]:>6} {v[3]:>10}"
                    )
    return out


def failures():
    out = []
    for path in sorted(glob.glob(os.path.join(results, "step5-*.txt"))):
        for l in open(path):
            if l.startswith("FAILURE"):
                out.append(os.path.basename(path) + ": " + l.rstrip())
    return ["failures"] + (out or ["none"])


if __name__ == "__main__":
    sections = [
        bytes_table(),
        cold_table(),
        warm_table(),
        identity_table(),
        prewarm_table(),
        rebuild_table(),
        alloc_table(),
        failures(),
    ]
    print("\n\n".join("\n".join(s) for s in sections))
