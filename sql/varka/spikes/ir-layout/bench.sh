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

# The IR-storage spike's measurement (VARKA-291, plan section 6): one arm, one graph and one measure
# in each JVM, from IrLayoutBench, pinned to the fastest cores as dev/varka_bench_regen.sh does.
#
#   bench.sh VARIANT MODE OUTFILE [--samples N] [--arms "A B"] [--graphs "g1 g2"] [--seconds S]
#            [--ops "build rebuild"] [--prewarm GRAPH]
#
#   VARIANT  jdk25  the sealed records, A, B and C, on the JDK that runs the build
#            plain  the same records and V16 and V63 on the early-access JDK (EA_JAVA_HOME)
#            value  the IR as value records on the early-access JDK
#   MODE     cold   a fresh JVM for every sample, --samples of them (default 20)
#            warm   one JVM for each measure: 3 warm-up and 5 measured iterations of --seconds (2)
#            bytes  bytes a node, with a java agent for the records
#            alloc  heap bytes allocated and GC time over 3,000 builds, once warm
#
#   --prewarm GRAPH   cold mode: build once on another graph first, paying the one-time costs
#   --ops             warm mode: the measures to run (default: build intern analyze rebuild)
#
# The machine must be quiet: it refuses to start above a load of 0.8 unless FORCE=1.

set -euo pipefail

variant=${1:?variant: jdk25|plain|value}
mode=${2:?mode: cold|warm|bytes}
outfile=${3:?output file}
shift 3

samples=20
seconds=2
ops="build intern analyze rebuild"
prewarm=""
arms=""
graphs="cheap_tails-22 wide_int-1 size_ladder-100 size_ladder-200 deep_chain-1024 grown_ladder-2000"
while [ $# -gt 0 ]; do
  case "$1" in
    --samples) samples=$2; shift 2 ;;
    --seconds) seconds=$2; shift 2 ;;
    --arms) arms=$2; shift 2 ;;
    --graphs) graphs=$2; shift 2 ;;
    --ops) ops=$2; shift 2 ;;
    --prewarm) prewarm=$2; shift 2 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../../.." && pwd)
pkg=sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen/varka
test_pkg=sql/catalyst/src/test/java/org/apache/spark/sql/catalyst/expressions/codegen/varka
main=org.apache.spark.sql.catalyst.expressions.codegen.varka

case "$variant" in
  jdk25) jdk=${JAVA_HOME:-}; default_arms="records A B C"; value=0 ;;
  plain) jdk=${EA_JAVA_HOME:?set EA_JAVA_HOME}; default_arms="records V16 V63"; value=0 ;;
  value) jdk=${EA_JAVA_HOME:?set EA_JAVA_HOME}; default_arms="records"; value=1 ;;
  *) echo "variant: jdk25|plain|value" >&2; exit 2 ;;
esac
arms=${arms:-$default_arms}
bin=${jdk:+$jdk/bin/}

load=$(cut -d' ' -f1 /proc/loadavg)
if [ "${FORCE:-0}" != 1 ] && awk -v l="$load" 'BEGIN { exit !(l > 0.8) }'; then
  echo "load average $load is above 0.8: the machine is not quiet (FORCE=1 to override)" >&2
  exit 3
fi

out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
mkdir -p "$out/src" "$out/classes"
if [ "$value" = 1 ]; then
  sed -E 's/^(  |public )record /\1value record /' "$root/$pkg/VarkaVectorIR.java" \
    > "$out/src/VarkaVectorIR.java"
else
  cp "$root/$pkg/VarkaVectorIR.java" "$out/src/VarkaVectorIR.java"
fi
shopt -s nullglob
extra=()
flags=()
if [ "$variant" != jdk25 ]; then
  extra=("$here"/src-ea/*.java)
  flags=(--enable-preview --source 27 --add-exports java.base/jdk.internal.value=ALL-UNNAMED
    --add-exports java.base/jdk.internal.vm.annotation=ALL-UNNAMED -Xlint:-preview)
fi
"${bin}javac" "${flags[@]}" -d "$out/classes" "$out/src/VarkaVectorIR.java" \
  "$root/$test_pkg/VarkaIrDescription.java" "$here"/src/*.java "${extra[@]}" 2>&1 \
  | { grep -v '^Note:' || true; }
printf 'Premain-Class: %s.SizeAgent\n' "$main" > "$out/manifest.mf"
"${bin}jar" --create --file "$out/size-agent.jar" --manifest "$out/manifest.mf" -C "$out/classes" .

# The fastest cores, as dev/varka_bench_regen.sh picks them.
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
runner=()
[ -n "$fast" ] && command -v taskset > /dev/null && runner=(taskset -c "$fast")

jvm=("${bin}java")
[ "$variant" != jdk25 ] && jvm+=(--enable-preview
  --add-exports java.base/jdk.internal.value=ALL-UNNAMED
  --add-exports java.base/jdk.internal.vm.annotation=ALL-UNNAMED)
jvm+=(-Xss16m -Xmx4g -cp "$out/classes")
[ "$mode" = bytes ] && jvm+=("-javaagent:$out/size-agent.jar")

{
  echo "# VARKA-291 step 5, variant $variant, mode $mode, $(date +%Y-%m-%d)"
  echo "# jdk: $("${bin}java" -version 2>&1 | head -1)"
  echo "# pin: ${runner[*]:-none}; load average at start $load; samples $samples; seconds $seconds;\
prewarm ${prewarm:-none}"
  echo "# power: governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null \
|| echo n/a) profile=$(powerprofilesctl get 2>/dev/null || echo n/a)"
} > "$outfile"

run() {
  # A measure that does not finish (records hash and equality walk the subtree) is a result, and so
  # is a JVM that crashes: both are written with the cause instead of being dropped.
  local text line
  text=$(timeout 300 "${runner[@]}" "${jvm[@]}" "$main.IrLayoutBench" \
    --graphs "$here/graphs" "$@" 2>&1 || true)
  line=$(echo "$text" | grep -E '^(COLD|WARM|BYTES|ALLOC)' || true)
  if [ -z "$line" ]; then
    cause=$(echo "$text" | grep -m1 -E 'SIGSEGV|Exception|Error|Killed' | cut -c1-120 || true)
    line="FAILURE $* :: $cause"
  fi
  echo "$line" >> "$outfile"
}

for graph in $graphs; do
  for arm in $arms; do
    case "$mode" in
      cold) for _ in $(seq "$samples"); do
              run --arm "$arm" --graph "$graph" --mode cold ${prewarm:+--prewarm "$prewarm"}
            done ;;
      alloc) run --arm "$arm" --graph "$graph" --mode alloc ;;
      warm) for op in $ops; do
              run --arm "$arm" --graph "$graph" --mode warm --op "$op" --seconds "$seconds"
            done ;;
      bytes) run --arm "$arm" --graph "$graph" --mode bytes ;;
    esac
  done
done
echo "# done, load average at end $(cut -d' ' -f1 /proc/loadavg)" >> "$outfile"
