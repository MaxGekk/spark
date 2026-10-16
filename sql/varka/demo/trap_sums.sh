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
# replay's log (about 100 MB), and prints the line of each compile of a Varka kernel
# where C2 found too many class-check traps, the line the post quotes:
#
#   <observe trap='class_check' count='0' mcount='0' ccount='254'/>
#
# ccount is the compile-wide sum of class-check traps, against PerMethodTrapLimit (100 by
# default). A replay that compiles clean has no such line in its kernel's compiles. The limits'
# defaults on your JVM: java -XX:+PrintFlagsFinal -version | grep -E 'PerMethod(Trap|Recomp)'.
#
# --compositions K runs compositions 0 to K-1 in one JVM per replay instead of the prefix and
# composition 220, as the counts the post quotes for 500 and 1,000 warm-ups were taken.
#
# The first run builds sql's test classes and exports the test classpath with
# dev/varka_matrix.sh --module sql --list (tens of minutes on a cold checkout); --skip-build
# reuses it. Results go to target/varka-demo/trap_sums/ unless --out names another directory.
#
# Written on JDK 25.0.4 at a 512-bit preferred vector species. The release depends on the JIT's
# profile history, so on another JDK or vector width the rates differ; where the preferred
# species is 128 bits wide, Varka's template warm-up does not clear the releases (VARKA-374).

set -uo pipefail

cd "$(dirname "$0")/../../.."
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
    --out) out=$2; shift 2 ;;
    --skip-build) build=0; shift ;;
    --keep) keep=1; shift ;;
    --keep-tries) keep_tries=$2; shift 2 ;;
    --compositions) compositions=$2; shift 2 ;;
    *) sed -n '18,60p' "$0"; exit 2 ;;
  esac
done

BUILD=$ROOT/target/varka-matrix
if [ "$build" = 1 ]; then
  echo "== building sql's test classes and exporting the test classpath (see $BUILD/build.log)"
  dev/varka_matrix.sh --module sql --list > /dev/null || exit 1
fi
[ -s "$BUILD/classpath" ] && [ -s "$BUILD/jvm.opts" ] || {
  echo "no classpath or JVM options in $BUILD; run without --skip-build" >&2; exit 1; }
CP=$ROOT/sql/catalyst/target/scala-2.13/classes:$(cat "$BUILD/classpath")
mapfile -t OPTS < "$BUILD/jvm.opts"
pin=()
[ -n "$cpus" ] && pin=(taskset -c "$cpus")
TRAP_LIMITS=(-XX:PerMethodTrapLimit=100000 -XX:PerMethodRecompilationCutoff=100000)
mkdir -p "$out"

# replay <dir> <JVM option>...: one replay in a fresh JVM, its ScalaTest report in <dir>/run.log.
replay() {
  local dir=$1; shift
  local workload=(-Dvarka.sparkfuzz.warmupSet=0,220)
  [ "$compositions" -gt 0 ] && workload=(-Dvarka.sparkfuzz.warmupIterations="$compositions")
  rm -rf "$dir"; mkdir -p "$dir/tmp"
  (cd "$dir" && SPARK_TESTING=1 SPARK_SCALA_VERSION=2.13 "${pin[@]}" java "${OPTS[@]}" \
    -Djava.io.tmpdir="$dir/tmp" -Dvarka.sparkfuzz.seed=20261008 "${workload[@]}" \
    -Dvarka.sparkfuzz.jitEvidence=false -Dspark.test.timeout=170 \
    -Dvarka.test.watchdog.minutes=170 "$@" -cp "$CP" org.scalatest.tools.Runner -oW \
    -s org.apache.spark.sql.execution.VarkaSparkFuzzSuite \
    -z "both sides of the warm-up handover" > "$dir/run.log" 2>&1)
  rm -rf "$dir/tmp" "$dir/spark-warehouse"
}

# released <dir>: the replay's releases at the deadline, or "failed" when its test did not pass.
released() {
  grep -q 'Tests: succeeded 1' "$1/run.log" || { echo failed; return; }
  grep -c 'a kernel was released at the deadline' "$1/run.log"
}

# One line per compile of a kernel method (a class named for its shape, VarkaFused*_w<hash>)
# in which C2 found the compile-wide class-check sum too high: the compile id, the method, how
# many calls it decided, and the first such <observe> line. HotSpot writes that form,
# with mcount and ccount, only when the sum decides a call site.
observed() {
  awk 'function flush() {
         if (n > 0) printf "compile %s %s: at the limit for %d calls, the first %s\n",
           id, name, n, first
         n = 0 }
       /<task / { flush(); name = ""
         if ($0 ~ /VarkaFused[A-Za-z]*_w/) {
           match($0, /method=\047[^(\047]*/); name = substr($0, RSTART + 8, RLENGTH - 9)
           match($0, /compile_id=\047[0-9]*/); id = substr($0, RSTART + 12, RLENGTH - 12) } }
       /<\/task>/ { flush(); name = "" }
       name != "" && /<observe trap=.class_check. .*ccount=/ {
         if (n++ == 0) { first = $0; sub(/^[ \t]*/, "", first) } }
       END { flush() }' "$1"
}

if [ "$keep" = 1 ]; then
  for try in $(seq 1 "$keep_tries"); do
    dir=$out/keep
    replay "$dir" -Dvarka.warmup.vectorTemplates=false -XX:+UnlockDiagnosticVMOptions \
      -XX:+LogCompilation -XX:LogFile="$dir/hotspot.xml"
    n=$(released "$dir")
    echo "replay $try: released $n"
    if [ "$n" != failed ] && [ "$n" -gt 0 ]; then
      grep 'a kernel was released at the deadline' "$dir/run.log" | cut -c1-200
      echo "JIT log kept: $dir/hotspot.xml"
      echo "class-check sums at the limit in Varka kernels' compiles:"
      observed "$dir/hotspot.xml"
      exit 0
    fi
  done
  echo "no release in $keep_tries replays" >&2
  exit 1
fi

arms=("defaults" "trap limits" "stock")
declare -A total=() releasing=() failed=()
for arm in "${arms[@]}"; do total[$arm]=0; releasing[$arm]=0; failed[$arm]=0; done
start=$(date +%s)
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
    if [ "$n" = failed ]; then
      failed[$arm]=$((failed[$arm] + 1))
    else
      total[$arm]=$((total[$arm] + n))
      [ "$n" -gt 0 ] && releasing[$arm]=$((releasing[$arm] + 1))
      # A replay's report is kept only when it released, so the directory stays small.
      [ "$n" -gt 0 ] || rm -rf "$dir"
    fi
    printf '%-12s replay %3d: %s released at the deadline, %d s\n' \
      "$arm" "$r" "$n" $(( $(date +%s) - t0 ))
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
exit 0
