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
# How far apart do two runs of the same benchmark land, with nothing changed?
#
#   dev/varka_bench_repeat.sh catalyst VarkaEmitterParityBenchmark 3
#
# Runs the benchmark N times, pinned exactly as dev/varka_bench_regen.sh pins it,
# writes nothing to the committed results files, and reports the per-case spread:
# the median, how many cases exceed 3, 10 and 20 percent, and the worst few.
#
# It exists because a diff between two regenerations is not evidence of a change
# until you know what an unchanged file does. On this machine it does more than
# anyone expected: pinned, the parity file's median case moves 1.6% between two
# runs, 73 of 211 cases move more than 3%, and 22 move more than 10%, with the
# worst near 26%. Unpinned the worst reaches 75%. That is not thermal drift and
# not the clock - the frequency is constant to 1.2% across runs while throughput
# moves 31% - it is a per-fork JIT and code-layout lottery, which `-Xbatch`
# narrows from 49% to 14% and does not remove.
#
# What follows from it, and the reason this script is committed rather than run
# once: an A/B whose two arms sit in the same run is sound, because they share a
# JVM, a layout and a clock, and that is how every A/B in this project is built.
# A number compared against a *previous run* is not sound below the band this
# script measures. Before reading a regeneration's diff as a regression, run
# this and compare the diff against the band.
set -uo pipefail
root="$(git rev-parse --show-toplevel)"; cd "$root"
[ "$#" -ge 2 ] || { sed -n '17,45p' "$0"; exit 2; }
module="$1"; klass="$2"; runs="${3:-3}"

case "$klass" in
  *.*) fqcn="$klass"; klass="${klass##*.}" ;;
  *)
    case "$module" in catalyst) src="sql/catalyst/src/test" ;; *) src="sql/core/src/test" ;; esac
    file="$(find "$src" -name "$klass.scala" | head -1)"
    [ -n "$file" ] || { echo "no $klass.scala under $src" >&2; exit 2; }
    fqcn="$(sed -n 's/^package \(.*\)$/\1/p' "$file" | head -1).$klass" ;;
esac

# The same fast-CCX pin dev/varka_bench_regen.sh uses, for the same reasons.
maxf=0
for d in /sys/devices/system/cpu/cpu[0-9]*/cpufreq; do
  f=$(cat "$d/cpuinfo_max_freq" 2>/dev/null || echo 0)
  [ "$f" -gt "$maxf" ] && maxf=$f
done
fast=""
for d in /sys/devices/system/cpu/cpu[0-9]*/cpufreq; do
  c=$(basename "$(dirname "$d")"); c=${c#cpu}
  f=$(cat "$d/cpuinfo_max_freq" 2>/dev/null || echo 0)
  [ "$f" = "$maxf" ] && fast="${fast:+$fast,}$c"
done
if [ -n "$fast" ] && command -v taskset > /dev/null; then
  runner=(taskset -c "$fast"); echo "pinned to cpus $fast"
else
  runner=(); echo "not pinned"
fi

out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT
for i in $(seq 1 "$runs"); do
  echo "== run $i of $runs"
  "${runner[@]}" build/sbt -batch "$module/Test/runMain $fqcn" > "$out/run$i" 2>&1 \
    || { echo "run $i failed:"; tail -5 "$out/run$i"; exit 1; }
done

python3 - "$out" "$runs" <<'PY'
import re, sys, io, os, statistics
out, runs = sys.argv[1], int(sys.argv[2])
row = re.compile(r'^(?:\[info\] )?(\S.*?)\s{2,}\d+\s+\d+\s+\d+\s+([\d.]+)\s+[\d.]+\s+[\d.]+X\s*$')
def parse(p):
    d, seen = {}, {}
    for ln in io.open(p, errors="replace"):
        m = row.match(ln.rstrip())
        if m:
            n = m.group(1).strip()
            seen[n] = seen.get(n, 0) + 1
            d[f"{n}#{seen[n]}"] = float(m.group(2))
    return d
rs = [parse(os.path.join(out, f"run{i}")) for i in range(1, runs + 1)]
common = set(rs[0])
for r in rs[1:]:
    common &= set(r)
spread = []
for k in common:
    vs = [r[k] for r in rs]
    spread.append(((max(vs) - min(vs)) / min(vs) * 100, k, min(vs), max(vs)))
spread.sort(reverse=True)
ps = [s[0] for s in spread]
print(f"\n{len(common)} cases over {runs} runs")
print(f"  median spread : {statistics.median(ps):.2f}%")
for t in (3, 10, 20):
    print(f"  over {t:2d}%       : {sum(1 for p in ps if p > t):4d}")
print(f"\n{'worst cases':60s} {'min':>9s} {'max':>9s} {'spread':>7s}")
for p, k, lo, hi in spread[:10]:
    print(f"{k[:60]:60s} {lo:9.1f} {hi:9.1f} {p:6.1f}%")
PY
