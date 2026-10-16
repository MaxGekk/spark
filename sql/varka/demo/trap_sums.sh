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
# Why a warmed Vector API kernel can still allocate, on your own machine (the post is
# sql/varka/plans/m7/POST_TRAP_SUMS.md).
#
# The replay is the warm-up arm of VarkaSparkFuzzSuite over one prefix and one composition:
# composition 0 of seed 20261008, `abs` over a year-month interval, whose kernel leaves
# class-check traps in the Vector API's shared templates, then composition 220, whose kernel
# C2 then compiles with real virtual calls and which keeps allocating until the warm-up's
# ten-second deadline releases it. Each replay is a fresh JVM, because what decides a release
# is the JVM's profile history. The script counts the replays that release, in three arms:
#
#   defaults      Varka's own template warm-up off (-Dvarka.warmup.vectorTemplates=false),
#                 which is the JVM the post describes
#   trap limits   the same, with -XX:PerMethodTrapLimit=100000
#                 -XX:PerMethodRecompilationCutoff=100000
#   stock         stock Varka: its template warm-up on, no options (the post's "What a
#                 library can do")
#
# The arms alternate replay by replay, so a change in the machine's load reaches all three.
#
#   sql/varka/demo/trap_sums.sh                    # 20 replays per arm
#   sql/varka/demo/trap_sums.sh --runs 100 --cpus 0-3
#   sql/varka/demo/trap_sums.sh --keep             # replay until one releases, keep its JIT log
#   sql/varka/demo/trap_sums.sh --compositions 400 --runs 1   # the post's 400-composition arm
#
# --keep replays the defaults arm with -XX:+LogCompilation until a replay releases, keeps that
# replay's log (about 100 MB; at most 30 tries, --keep-tries N), and prints, for each compile of
# a Varka kernel where C2 found too many class-check traps, the line with the largest sum, the
# kind of line the post quotes:
#
#   <observe trap='class_check' count='0' mcount='0' ccount='1100'/>
#
# ccount is the compile-wide sum of class-check traps, against PerMethodTrapLimit (100 by
# default). It then replays the stock arm once with the same log and runs the same check on it,
# which finds no such line in a kernel's compiles. The limits' defaults on your JVM:
# java -XX:+PrintFlagsFinal -version | grep -E 'PerMethod(Trap|Recomp)'.
#
# --compositions K runs compositions 0 to K-1 in one JVM per replay instead of the prefix and
# composition 220, as the counts the post quotes for 500 and 1,000 warm-ups were taken; each
# replay's line then ends with its warm-up verdicts (COMPILED n, RELEASED n).
#
# The compositions are drawn from sql/varka/coverage.json, so a change to it can change which
# expressions composition 0 and 220 are. The script first runs one JVM that prints both and
# stops unless they are the ones above, and in the replays of 0 and 220 stops on a release of
# any kernel but 220's (shape w7fd4deef67312f01). The post's counts were taken over the
# compositions of commit 57cf25f8c98 of vecbricks/varka.
#
# The first run builds sql's test classes and exports the test classpath with
# dev/varka_matrix.sh --module sql --list (tens of minutes on a cold checkout); --skip-build
# reuses it. Results go to target/varka-demo/trap_sums/ unless --out names another directory;
# status.tsv there has one line per replay (arm, replay, the JVM's exit status, releases,
# seconds, warm-up verdicts), and the script exits 1 when every replay of an arm failed. It
# needs Linux (taskset for --cpus) and bash 4.4 or later.
#
# Written on JDK 25.0.4 at a 512-bit preferred vector species. The release depends on the JIT's
# profile history, so on another JDK or vector width the rates differ; on x86 where the preferred
# species is 128 bits wide, Varka's template warm-up does not clear the releases (VARKA-374).

set -uo pipefail

usage() { awk 'NR >= 18 && !/^#/ { exit } NR >= 18 { sub(/^# ?/, ""); print }' "$0"; }
caller=$(pwd)
cd "$(dirname "$0")/../../.." || exit 1
ROOT=$(pwd)
runs=20
cpus=""
out=$ROOT/target/varka-demo/trap_sums
build=1
keep=0
compositions=0
keep_tries=30
while [ $# -gt 0 ]; do
  case "$1" in
    --runs) runs=$2; shift 2 ;;
    --cpus) cpus=$2; shift 2 ;;
    --out) case "$2" in /*) out=$2 ;; *) out=$caller/$2 ;; esac; shift 2 ;;
    --skip-build) build=0; shift ;;
    --keep) keep=1; shift ;;
    --keep-tries) keep_tries=$2; shift 2 ;;
    --compositions) compositions=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done

BUILD=$ROOT/target/varka-matrix
if [ "$build" = 1 ]; then
  echo "== building sql's test classes and exporting the test classpath (see $BUILD/build.log)"
  dev/varka_matrix.sh --module sql --list > /dev/null || exit 1
fi
[ -s "$BUILD/classpath" ] && [ -s "$BUILD/jvm.opts" ] || {
  echo "no classpath or JVM options in $BUILD; run without --skip-build" >&2; exit 1; }
# Another script can leave catalyst's classpath there (dev/varka_deopt_census.sh does).
grep -q 'sql/core/target' "$BUILD/classpath" || {
  echo "the classpath in $BUILD is not sql's; run without --skip-build" >&2; exit 1; }
CP=$(cat "$BUILD/classpath")
mapfile -t OPTS < "$BUILD/jvm.opts"
pin=()
[ -n "$cpus" ] && pin=(taskset -c "$cpus")
TRAP_LIMITS=(-XX:PerMethodTrapLimit=100000 -XX:PerMethodRecompilationCutoff=100000)
mkdir -p "$out"

SHAPE=w7fd4deef67312f01
# replay <dir> <JVM option>...: one replay in a fresh JVM, its ScalaTest report in <dir>/run.log.
# Later options win, so an option given here overrides the workload's.
replay() {
  local dir=$1; shift
  local workload=("-Dvarka.sparkfuzz.warmupSet=0,220")
  [ "$compositions" -gt 0 ] && workload=(-Dvarka.sparkfuzz.warmupIterations="$compositions")
  rm -rf "$dir"; mkdir -p "$dir/tmp"
  (cd "$dir" && SPARK_TESTING=1 SPARK_SCALA_VERSION=2.13 "${pin[@]}" java "${OPTS[@]}" \
    -Djava.io.tmpdir="$dir/tmp" -Dvarka.sparkfuzz.seed=20261008 "${workload[@]}" \
    -Dvarka.sparkfuzz.jitEvidence=false -Dspark.test.timeout=170 \
    -Dvarka.test.watchdog.minutes=170 "$@" -cp "$CP" org.scalatest.tools.Runner -oW \
    -s org.apache.spark.sql.execution.VarkaSparkFuzzSuite \
    -z "both sides of the warm-up handover" > "$dir/run.log" 2>&1)
  echo $? > "$dir/exit"
  rm -rf "$dir/tmp" "$dir/spark-warehouse"
}

# released <dir>: the replay's releases at the deadline, or "failed" when its JVM or test did not
# pass.
released() {
  [ "$(cat "$1/exit")" = 0 ] || { echo failed; return; }
  grep -q 'Tests: succeeded 1' "$1/run.log" || { echo failed; return; }
  grep -c 'a kernel was released at the deadline' "$1/run.log"
}

# verdicts <dir>: the replay's warm-up verdicts, e.g. "COMPILED 505, RELEASED 29".
verdicts() {
  sed -n 's/^.*warm-up verdicts: \(.*\), of the releases.*$/\1/p' "$1/run.log" | tail -1
}

# foreign <dir>: stops the script when a replay of 0 and 220 released another kernel than 220's,
# which means the compositions are no longer the ones the post replays.
foreign() {
  [ "$compositions" -gt 0 ] && return
  grep 'a kernel was released at the deadline' "$1/run.log" | grep -qv "shapeHash=$SHAPE" || return
  echo "a replay released a kernel other than composition 220's ($SHAPE); the compositions" \
    "at seed 20261008 have changed (see $1/run.log)" >&2
  exit 1
}

# One line per compile of a kernel method (a class named for its shape, VarkaFused*_w<hash>)
# in which C2 found the compile-wide class-check sum too high: the compile id, the method, how
# many of its trap checks the sum decided, and the <observe> line with the largest sum. HotSpot
# writes that form, with mcount and ccount, only when the sum is at the limit.
observed() {
  awk 'function flush() {
         if (n > 0) printf "compile %s %s: at the limit in %d checks, the largest %s\n",
           id, name, n, top
         n = 0; most = -1 }
       /<task / { flush(); name = ""
         if ($0 ~ /VarkaFused[A-Za-z]*_w/) {
           match($0, /method=\047[^(\047]*/); name = substr($0, RSTART + 8, RLENGTH - 9)
           match($0, /compile_id=\047[0-9]*/); id = substr($0, RSTART + 12, RLENGTH - 12) } }
       /<\/task>/ { flush(); name = "" }
       name != "" && /<observe trap=.class_check. .*ccount=/ {
         n++; match($0, /ccount=\047[0-9]*/); c = substr($0, RSTART + 8, RLENGTH - 8) + 0
         if (c > most) { most = c; top = $0; sub(/^[ \t]*/, "", top) } }
       END { flush() }' "$1"
}

# The compositions the replay of 0 and 220 stands for, as -Dvarka.sparkfuzz.warmupShow prints
# them at seed 20261008. A change to coverage.json can redraw them; the replay then means nothing.
declare -A EXPECT=([0]="outputs Vector(abs(ym)), filter Vector() OR")
EXPECT[220]="outputs Vector(d - ym, year(d) + i, ym - ymm, t - t2),"
EXPECT[220]+=" filter Vector(dt < INTERVAL '0' SECOND) OR"
echo "== checking that compositions 0 and 220 at seed 20261008 are the replayed ones"
replay "$out/check" "-Dvarka.sparkfuzz.warmupSet=0,220" -Dvarka.sparkfuzz.warmupShow=true
[ "$(released "$out/check")" = failed ] && {
  echo "the check's replay failed; its log is $out/check/run.log" >&2; exit 1; }
for it in 0 220; do
  got=$(sed -n "s/^.*iteration $it: outputs \(.*[^ ]\) *$/outputs \1/p" "$out/check/run.log" \
    | head -1)
  [ "$got" = "${EXPECT[$it]}" ] || {
    printf 'composition %s at seed 20261008 is not the replayed one:\n' "$it" >&2
    printf '  expected: %s\n  found:    %s\n' "${EXPECT[$it]}" "$got" >&2
    echo "check out commit 57cf25f8c98 of vecbricks/varka to replay the post's compositions" >&2
    exit 1; }
done
rm -rf "$out/check"

LOG=(-XX:+UnlockDiagnosticVMOptions -XX:+LogCompilation)
if [ "$keep" = 1 ]; then
  fails=0
  for try in $(seq 1 "$keep_tries"); do
    dir=$out/keep-$try
    replay "$dir" -Dvarka.warmup.vectorTemplates=false "${LOG[@]}" -XX:LogFile="$dir/hotspot.xml"
    n=$(released "$dir")
    echo "replay $try: released $n"
    if [ "$n" = failed ]; then
      # A failed replay keeps its report, without its JIT log.
      fails=$((fails + 1)); rm -f "$dir/hotspot.xml"; continue
    fi
    foreign "$dir"
    if [ "$n" -gt 0 ]; then
      grep 'a kernel was released at the deadline' "$dir/run.log" | cut -c1-200
      echo "JIT log kept: $dir/hotspot.xml"
      echo "class-check sums at the limit in Varka kernels' compiles:"
      observed "$dir/hotspot.xml"
      clean=$out/keep-stock
      replay "$clean" "${LOG[@]}" -XX:LogFile="$clean/hotspot.xml"
      n=$(released "$clean")
      [ "$n" = failed ] && {
        echo "the stock replay failed; its log is $clean/run.log" >&2; exit 1; }
      echo "the same check on one stock replay with the log ($n released, $(verdicts "$clean")," \
        "JIT log kept: $clean/hotspot.xml):"
      lines=$(observed "$clean/hotspot.xml")
      echo "${lines:-none}"
      exit 0
    fi
    rm -rf "$dir"
  done
  if [ "$fails" = "$keep_tries" ]; then
    echo "every one of $keep_tries replays failed; their reports are in $out/keep-*" >&2
  else
    echo "no release in $keep_tries replays, $fails of them failed (reports in $out/keep-*)" >&2
  fi
  exit 1
fi

arms=("defaults" "trap limits" "stock")
declare -A total=() releasing=() failed=()
for arm in "${arms[@]}"; do total[$arm]=0; releasing[$arm]=0; failed[$arm]=0; done
start=$(date +%s)
printf 'arm\treplay\texit\treleased\tseconds\tverdicts\n' > "$out/status.tsv"
for r in $(seq 1 "$runs"); do
  for arm in "${arms[@]}"; do
    case "$arm" in
      defaults) args=(-Dvarka.warmup.vectorTemplates=false) ;;
      "trap limits") args=(-Dvarka.warmup.vectorTemplates=false "${TRAP_LIMITS[@]}") ;;
      stock) args=() ;;
    esac
    dir=$out/${arm// /_}-$r
    t0=$(date +%s)
    replay "$dir" "${args[@]}"
    n=$(released "$dir")
    v=$(verdicts "$dir")
    printf '%s\t%d\t%s\t%s\t%d\t%s\n' "$arm" "$r" "$(cat "$dir/exit")" "$n" \
      $(( $(date +%s) - t0 )) "$v" >> "$out/status.tsv"
    [ "$n" = failed ] || foreign "$dir"
    if [ "$n" = failed ]; then
      failed[$arm]=$((failed[$arm] + 1))
    else
      total[$arm]=$((total[$arm] + n))
      [ "$n" -gt 0 ] && releasing[$arm]=$((releasing[$arm] + 1))
      # A replay's report is kept only when it released, so the directory stays small.
      [ "$n" -gt 0 ] || rm -rf "$dir"
    fi
    printf '%-12s replay %3d: %s released at the deadline, %d s%s\n' \
      "$arm" "$r" "$n" $(( $(date +%s) - t0 )) "$([ "$compositions" -gt 0 ] && echo " ($v)")"
  done
done

echo
echo "seed 20261008, $([ "$compositions" -gt 0 ] && echo "compositions 0 to $((compositions - 1))" \
  || echo "composition 0 then 220"), $runs replays per arm, $(( $(date +%s) - start )) s"
printf '%-12s  %-28s  %s\n' arm "replays with a release" "releases at the deadline"
for arm in "${arms[@]}"; do
  ok=$((runs - failed[$arm]))
  printf '%-12s  %-28s  %s\n' "$arm" "${releasing[$arm]} of $ok" "${total[$arm]}"
  [ "${failed[$arm]}" -gt 0 ] && echo "  ${failed[$arm]} replays failed; their logs are in $out"
done
status=0
for arm in "${arms[@]}"; do [ "${failed[$arm]}" = "$runs" ] && status=1; done
exit $status
