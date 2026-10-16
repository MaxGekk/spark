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
# The IR fuzzer's compiled test at volume (VARKA-274): each drawn kernel run over the eight batch
# histories of VarkaCompiledCheck - interpreted, warmed to C2, trapped after a quiet warm-up,
# recompiled, and released with C1 on through the OSRs - every compared batch held to the
# reference evaluator and the tier that served it read by the JFR tier witness. The draws are
# split by range over several JVMs at once, each pinned to its own cores, and
# dev/varka_compiled_fuzz.py totals them: the verdicts per lane and history, the time a case,
# the unsettled kernels with their trap reasons, the histories not reached and any wrong answer.
# Then every coverage row's kernel alone over warm-same, quiet-null and released, in one JVM on
# all the cores (VarkaCoverageCompositionFuzzSuite's compiled test, which a PR's run samples).
#
#   dev/varka_compiled_fuzz.sh                          # the nightly's: 2000 draws a lane, 8 JVMs
#   dev/varka_compiled_fuzz.sh --seed 42 --draws 80 --jvms 2 --cpu-base 18   # a trial
#   dev/varka_compiled_fuzz.sh --skip-build             # reuse the last build and classpath
#   dev/varka_compiled_fuzz.sh --no-coverage            # without the coverage rows' arm
#
# --draws is the total per lane from --from (default 0), split evenly over --jvms (default 8);
# --cpus is the cores per JVM (default 3) and --cpu-base the first core of the first JVM;
# --seed is the fuzzer's seed (default today's date, as the nightly's fuzz step); --settle-ms the
# warm-up's deadline (default 10000, the compiled test's own); --out the directory (default
# target/varka-compiled-fuzz/<seed>). A failure replays with the seed and draw its message names,
# -Dvarka.fuzz.only=<draw> and -Dvarka.fuzz.history=<history>, as VarkaIrFuzzSuite says; a
# kernel's tier can depend on what ran before it in its JVM, so a tier fact replays by running the
# same range (--from, --draws, --jvms) instead.
#
# The catalyst test classes and their exported classpath are rebuilt first (dev/varka_matrix.sh
# --module catalyst --list), since the nightly runs after a day's merges. Never rebuild them while
# these JVMs run: sbt rewriting a class under a running JVM reads as a finding.
#
# Exit status: 1 when a JVM failed (a wrong answer fails its test), a fuzz JVM left no run line, or
# the coverage rows' JVM failed; else 0.
# A history not reached is counted, not failed: the tier facts are held on a quiet machine by
# VarkaTierWitnessSuite.
usage() { sed -n '17,/^[^#]/p' "$0" | sed '$d'; exit "${1:-2}"; }
set -euo pipefail
from=0; draws=2000; jvms=8; cpus=3; cpu_base=0; seed="$(date +%Y%m%d)"; settle=10000; out=
skip_build=; coverage=1
while [ $# -gt 0 ]; do
  case "$1" in
    --from) from="$2"; shift 2 ;;
    --draws) draws="$2"; shift 2 ;;
    --jvms) jvms="$2"; shift 2 ;;
    --cpus) cpus="$2"; shift 2 ;;
    --cpu-base) cpu_base="$2"; shift 2 ;;
    --seed) seed="$2"; shift 2 ;;
    --settle-ms) settle="$2"; shift 2 ;;
    --out) out="$2"; shift 2 ;;
    --skip-build) skip_build=1; shift ;;
    --no-coverage) coverage=0; shift ;;
    -h|--help) usage 0 ;;
    *) usage ;;
  esac
done
cd "$(dirname "$0")/.."
root=$(pwd)
out=${out:-target/varka-compiled-fuzz/$seed}
case "$out" in /*) ;; *) out="$root/$out" ;; esac
if [ -z "$skip_build" ] || [ ! -s target/varka-matrix/classpath ]; then
  dev/varka_matrix.sh --module catalyst --list > /dev/null
fi
opts=$(cat target/varka-matrix/jvm.opts)
cp="$root/sql/catalyst/target/scala-2.13/classes:$(cat target/varka-matrix/classpath)"
mkdir -p "$out"
# The reader proves itself on planted lines before it reads tonight's.
dev/varka_compiled_fuzz.py --self-test
# One JVM over draws [lo, hi) of both lanes, in its own directory so a crash leaves its files
# there. `taskset` pins the JVM and the compiler threads it starts to the cores it was given.
# The watchdogs, 120 minutes a fuzz JVM and 40 for the coverage arm, keep the build, both and the
# reader inside the nightly's three-hour step deadline, so a long night still ends in totals: a
# fuzz JVM's share of the nightly measured about 90 minutes in a trial (VARKA-274.md 9.7).
# Arguments: directory, first core, last core, watchdog minutes, suite, test filter, then system
# properties.
run_jvm() {
  local dir="$1" first="$2" last="$3" minutes="$4" suite="$5" filter="$6"
  shift 6
  mkdir -p "$dir/tmp"
  # `set +e` inside the subshell: a JVM that fails must still leave its exit file.
  (set +e; cd "$dir" && SPARK_TESTING=1 SPARK_SCALA_VERSION=2.13 taskset -c "$first-$last" \
    java $opts "-Djava.io.tmpdir=$dir/tmp" -Dvarka.ownJvm=true "-Dvarka.fuzz.seed=$seed" \
    "-Dvarka.fuzz.settleMillis=$settle" "$@" \
    "-Dspark.test.timeout=$minutes" "-Dvarka.test.watchdog.minutes=$minutes" -cp "$cp" \
    org.scalatest.tools.Runner -oW \
    -s "org.apache.spark.sql.catalyst.expressions.codegen.varka.$suite" \
    -z "$filter" < /dev/null > "$dir/run.log" 2>&1; echo $? > "$dir/exit")
}
per=$(( (draws + jvms - 1) / jvms ))
{
  dirty=$(git diff --quiet HEAD -- sql dev || echo ' (dirty)')
  echo "commit:  $(git rev-parse --short=11 HEAD)$dirty"
  echo "jdk:     $(java -version 2>&1 | head -1)"
  echo "cpu:     $(grep -m1 'model name' /proc/cpuinfo | sed 's/.*: //')"
  echo "load:    $(cut -d' ' -f1-3 /proc/loadavg) at start"
  echo "args:    seed=$seed from=$from draws=$draws jvms=$jvms cpus=$cpus cpu-base=$cpu_base" \
    "settle=$settle"
} > "$out/provenance.txt"
rm -rf "$out"/run-* "$out/coverage"
pids=()
for ((i = 0; i < jvms; i++)); do
  lo=$(( from + i * per ))
  hi=$(( lo + per ))
  [ "$hi" -gt $(( from + draws )) ] && hi=$(( from + draws ))
  [ "$lo" -ge "$hi" ] && break
  first=$(( cpu_base + i * cpus ))
  run_jvm "$out/run-$i" "$first" "$(( first + cpus - 1 ))" 120 \
    VarkaIrFuzzSuite "compiled by C2" \
    "-Dvarka.fuzz.compiled=$hi" "-Dvarka.fuzz.compiledFrom=$lo" -Dvarka.fuzz.census=true &
  pids+=($!)
done
echo "== seed $seed: ${#pids[@]} JVMs of $per draws a lane each, logs under $out"
for pid in "${pids[@]}"; do wait "$pid" || true; done
if [ "$coverage" -eq 1 ]; then
  start=$SECONDS
  run_jvm "$out/coverage" "$cpu_base" "$(( cpu_base + jvms * cpus - 1 ))" 40 \
    VarkaCoverageCompositionFuzzSuite "as C2 code" -Dvarka.fuzz.coverageCompiled=all
  echo "coverage rows: $((SECONDS - start)) s" >> "$out/provenance.txt"
fi
echo "load:    $(cut -d' ' -f1-3 /proc/loadavg) at end" >> "$out/provenance.txt"
status=0
dev/varka_compiled_fuzz.py "$out"/run-* > "$out/summary.txt" || status=$?
if [ "$coverage" -eq 1 ]; then
  {
    echo "coverage rows (exit $(cat "$out/coverage/exit" 2>/dev/null || echo missing)):"
    grep -E '^ +\+ |\*\*\* (FAILED|ABORTED) \*\*\*' "$out/coverage/run.log" || true
  } >> "$out/summary.txt"
  [ "$(cat "$out/coverage/exit" 2>/dev/null)" = 0 ] || status=1
fi
cat "$out/summary.txt"
exit "$status"
