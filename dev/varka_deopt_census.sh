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
# How often does a kernel the fuzz grammar draws enter the C2 deoptimization cycle, and which
# shapes (VARKA-326)? Runs VarkaIrFuzzSuite's compiled test over a range of draws, the
# warm-up history only, in several JVMs at once, each pinned to its own cores and each logging
# the JVM's own compile and trap record (-Xlog:jit+compilation=debug,deoptimization=debug), and
# reads the logs with dev/varka_deopt_census.py. The draws are the suite's, so a kernel is
# named by its lane and index; a kernel's cycle depends on what ran before it in its JVM, so
# a verdict replays by running the same range in the same order (--from, --draws, --jvms), not
# by naming the kernel (--only runs it alone).
#
#   dev/varka_deopt_census.sh --label A                       # 400 draws a lane, matrix base
#   dev/varka_deopt_census.sh --label B --width 16            # the 128-bit width
#   dev/varka_deopt_census.sh --label C --options drawn       # each draw's own random options
#   dev/varka_deopt_census.sh --label R --from 300 --draws 100 --jvms 1       # one range alone
#   dev/varka_deopt_census.sh --label N --jvm-arg -XX:-UseProfiledLoopPredicate
#   dev/varka_deopt_census.sh --label S --only 375 --jvms 1   # draw 375 of each lane, alone
#
# --draws is the total per lane, split evenly over --jvms (default 4); --cpus is the cores per
# JVM (default 6) and --cpu-base the first core of the first JVM, so that two runs side by
# side do not share cores; --settle-ms is the warm-up's deadline (30000: the compiled test's own is
# 10000, and the census reads the cut from the settle time); --seed is the fuzzer's seed
# (20260903, the one VARKA-274 drew). --out names the directory (default
# target/varka-deopt-census/<label>). Needs the catalyst test build and its exported classpath;
# runs dev/varka_matrix.sh --module catalyst --list for them when they are not there. Logs run
# to hundreds of megabytes: delete the run directories once the table is written.
usage() { sed -n '17,/^[^#]/p' "$0" | sed '$d'; exit "${1:-2}"; }
set -euo pipefail
label=census; from=0; draws=400; jvms=4; cpus=6; cpu_base=0; options=defaults; width=pref
settle=30000; seed=20260903; only=; out=; jvm_args=()
while [ $# -gt 0 ]; do
  case "$1" in
    --label) label="$2"; shift 2 ;;
    --from) from="$2"; shift 2 ;;
    --draws) draws="$2"; shift 2 ;;
    --jvms) jvms="$2"; shift 2 ;;
    --cpus) cpus="$2"; shift 2 ;;
    --cpu-base) cpu_base="$2"; shift 2 ;;
    --options) options="$2"; shift 2 ;;
    --width) width="$2"; shift 2 ;;
    --settle-ms) settle="$2"; shift 2 ;;
    --seed) seed="$2"; shift 2 ;;
    --only) only="$2"; shift 2 ;;
    --jvm-arg) jvm_args+=("$2"); shift 2 ;;
    --out) out="$2"; shift 2 ;;
    -h|--help) usage 0 ;;
    *) usage ;;
  esac
done
cd "$(dirname "$0")/.."
root=$(pwd)
out=${out:-target/varka-deopt-census/$label}
case "$out" in /*) ;; *) out="$root/$out" ;; esac
case "$out" in /*) ;; *) out="$root/$out" ;; esac
if [ ! -s target/varka-matrix/classpath ]; then
  dev/varka_matrix.sh --module catalyst --list > /dev/null
fi
opts=$(cat target/varka-matrix/jvm.opts)
cp="$root/sql/catalyst/target/scala-2.13/classes:$(cat target/varka-matrix/classpath)"
mkdir -p "$out"
per=$(( (draws + jvms - 1) / jvms ))
width_arg=()
[ "$width" != pref ] && width_arg=("-XX:MaxVectorSize=$width")
only_arg=()
[ -n "$only" ] && only_arg=("-Dvarka.fuzz.only=$only")
{
  dirty=$(git diff --quiet HEAD -- sql dev || echo ' (dirty)')
  echo "commit:  $(git rev-parse --short=11 HEAD)$dirty"
  echo "jdk:     $(java -version 2>&1 | head -1)"
  echo "cpu:     $(grep -m1 'model name' /proc/cpuinfo | sed 's/.*: //')"
  echo "load:    $(cut -d' ' -f1-3 /proc/loadavg) at start"
  echo "args:    label=$label from=$from draws=$draws jvms=$jvms options=$options width=$width" \
    "settle=$settle seed=$seed only=${only:-all} jvm-args=${jvm_args[*]:-none}"
} > "$out/provenance.txt"
xlog="jit+compilation=debug,deoptimization=debug"
pids=()
for ((i = 0; i < jvms; i++)); do
  dir="$out/run-$i"
  mkdir -p "$dir/tmp"
  lo=$(( from + i * per ))
  hi=$(( lo + per ))
  first=$(( cpu_base + i * cpus ))
  # The fork's own directory, so a crash leaves its files there. `taskset` pins the JVM and
  # the compiler threads it starts to the cores it was given.
  (cd "$dir" && SPARK_TESTING=1 SPARK_SCALA_VERSION=2.13 taskset -c "$first-$((first + cpus - 1))" \
    java $opts "-Djava.io.tmpdir=$dir/tmp" -Dvarka.ownJvm=true "-Dvarka.fuzz.seed=$seed" \
    "-Dvarka.fuzz.compiled=$hi" "-Dvarka.fuzz.compiledFrom=$lo" \
    "-Dvarka.fuzz.compiledOptions=$options" -Dvarka.fuzz.census=true \
    -Dvarka.fuzz.history=warm-same "-Dvarka.fuzz.settleMillis=$settle" \
    -Dspark.test.timeout=170 -Dvarka.test.watchdog.minutes=170 \
    "-Xlog:$xlog:file=$dir/xlog.txt:uptime,tags:filecount=0,filesize=0" \
    "${width_arg[@]}" "${jvm_args[@]}" "${only_arg[@]}" -cp "$cp" \
    org.scalatest.tools.Runner -oW \
    -s org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaIrFuzzSuite \
    -z "compiled by C2" > "$dir/run.log" 2>&1; echo $? > "$dir/exit") &
  pids+=($!)
done
echo "== $label: $jvms JVMs of $per draws a lane each, logs under $out"
status=0
for pid in "${pids[@]}"; do wait "$pid" || status=1; done
# Traps and settle times move with the machine's load, so the record keeps it at both ends.
echo "load:    $(cut -d' ' -f1-3 /proc/loadavg) at end" >> "$out/provenance.txt"
for ((i = 0; i < jvms; i++)); do
  [ "$(cat "$out/run-$i/exit" 2>/dev/null)" = 0 ] || { echo "run-$i failed"; status=1; }
done
dev/varka_deopt_census.py --label "$label" --tsv "$out/census.tsv" "$out"/run-* \
  | tee "$out/summary.txt"
exit "$status"
