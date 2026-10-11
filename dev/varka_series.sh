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
# Every small IR tree to a size, across JVMs (VARKA-269): VarkaSmallTreeSuite with
# -Dvarka.series.size, one ScalaTest runner JVM per slice, each taking every n-th tree. The pull
# request's size 4 runs in the gate; this is for the larger ones: size 5, 3.76 million trees at
# both lanes, eleven and a half minutes over 12 JVMs on the laptop (VARKA-269.md 9), and size 6,
# about thirty times as many, in an idle window. JVMs and not threads, because the emitter is not shown safe to call from several
# threads (row 307).
#
#   dev/varka_series.sh 5                    # size 5 over one JVM per core
#   dev/varka_series.sh 5 --jvms 12          # over 12
#   dev/varka_series.sh 5 --all-batches      # every tree meets all four batches
#   dev/varka_series.sh 6 --skip-build       # reuse the last build's classpath
#
# Logs and the classpath go to target/varka-series/. Exit status 0 when every slice passed.

set -uo pipefail

cd "$(dirname "$0")/.."
ROOT=$(pwd)
OUT=$ROOT/target/varka-series

usage() { sed -n '/^# Every small/,/^# Logs/p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

[ $# -ge 1 ] || usage
size=$1; shift
[[ "$size" =~ ^[0-9]+$ ]] || usage
jvms=$(nproc)
build=1
batches=false
while [ $# -gt 0 ]; do
  case "$1" in
    --jvms) jvms=$2; shift 2 ;;
    --all-batches) batches=true; shift ;;
    --skip-build) build=0; shift ;;
    -h|--help) usage ;;
    *) echo "unknown option: $1" >&2; usage ;;
  esac
done

mkdir -p "$OUT"
if [ "$build" = 1 ]; then
  echo "== build: catalyst's test classes and classpath"
  # The classpath is the one line of the log that is a path list once sbt's colours are
  # stripped, as varka-fuzz.yml reads it.
  build/sbt -batch "catalyst/Test/compile" "export catalyst/Test/fullClasspath" \
    > "$OUT/build.log" 2>&1 || { echo "build failed: $OUT/build.log" >&2; exit 1; }
  sed 's/\x1b\[[0-9;]*[A-Za-z]//g' "$OUT/build.log" | grep -E '^/[^ ]+$' | tail -1 \
    > "$OUT/classpath"
fi
grep -q scalatest "$OUT/classpath" 2>/dev/null || { echo "no classpath in $OUT" >&2; exit 1; }
CP=$(cat "$OUT/classpath")

echo "== size $size over $jvms JVMs, all batches: $batches"
pids=()
for k in $(seq 0 $((jvms - 1))); do
  # A slice of size 6 runs for hours: past SparkFunSuite's twenty-minute cap on a test body and
  # the suite watchdog's ten (both in minutes), so both are raised to a day. The runner JVM is
  # already a JVM of its own, so the suite runs in it rather than forking a child.
  java --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED -Xmx4g \
    -Dspark.testing=1 -Dspark.test.home="$ROOT" -Dvarka.ownJvm=true \
    -Dspark.test.timeout=1440 -Dvarka.test.watchdog.minutes=1440 \
    -Dvarka.series.size="$size" -Dvarka.series.slice="$k/$jvms" \
    -Dvarka.series.allBatches="$batches" \
    -cp "$CP" org.scalatest.tools.Runner -oW \
    -s org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSmallTreeSuite \
    -z "matches the reference evaluator" \
    > "$OUT/slice-$k.log" 2>&1 &
  pids+=($!)
done

# Waited on by process id, which ends on every path.
failed=0
for k in "${!pids[@]}"; do
  if wait "${pids[$k]}"; then
    echo "slice $k: passed"
  else
    echo "slice $k: FAILED ($OUT/slice-$k.log)"; failed=1
  fi
done
grep -h -E "Tests: succeeded|FAILED \*\*\*" "$OUT"/slice-*.log | sort | uniq -c
exit $failed
