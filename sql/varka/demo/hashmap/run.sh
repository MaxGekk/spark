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

# Runs capacity.scala on a stock Spark distribution and writes capacity-jdk<N>-output.txt beside
# it. The script prints times, which depend on the machine, so its committed output comes from a
# GitHub-hosted runner through .github/workflows/varka-demo.yml, which names the run and the CPU
# in the output's header:
#
#   sql/varka/demo/hashmap/run.sh <spark-home> [<java-home>]
#
# --smoke runs the script's small grid instead, to check it end to end on any machine; its output
# is capacity-smoke-jdk<N>-output.txt, a check and not a measurement, and is not committed:
#
#   sql/varka/demo/hashmap/run.sh --smoke <spark-home> [<java-home>]
#
# The layout is that of ../compiles/run.sh --times.

set -euo pipefail
name=capacity
if [ "${1:-}" = "--smoke" ]; then
  export CAPACITY_SMOKE=1
  name=capacity-smoke
  shift
fi
SPARK="${1:?usage: run.sh [--smoke] <spark-home> [<java-home>]}"
if [ -n "${2:-}" ]; then
  export JAVA_HOME="$2"
fi
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
cd "$(dirname "$0")"
version="$("$JAVA" -version 2>&1 | head -1)"
jdk="$("$JAVA" -XshowSettings:properties -version 2>&1 \
  | sed -n 's/^ *java.specification.version = //p')"
spark="$(sed -n 's/^Spark \([^ ]*\) .*/\1/p' "$SPARK/RELEASE")"
commit="$(git rev-parse --short=11 HEAD)"

# The machine's CPU: lscpu's model name on Linux, x86 or Arm, and the brand string on macOS.
cpu() {
  local model
  model="$(lscpu 2>/dev/null | sed -n 's/^Model name: *//p' | head -1)"
  [ -n "$model" ] || model="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
  echo "${model:-unknown}"
}

# The fast map's page must hold 2^20 records of 40 bytes for the capacityBit=20 arm; with 4g the
# driver's page is Spark's largest default, 64 MB. The script checks it.
options=(--master 'local[1]' --driver-memory 4g)
log="$(mktemp)"
"$SPARK/bin/spark-shell" --conf spark.ui.enabled=false \
  --conf spark.ui.showConsoleProgress=false "${options[@]}" -i capacity.scala \
  > "$log" 2>&1 < /dev/null || true
out="$name-jdk$jdk-output.txt"
{
  echo "Output of capacity.scala (${options[*]}) on stock Spark, by run.sh."
  if [ -n "${GITHUB_RUN_ID:-}" ]; then
    echo "run:     ${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"
  fi
  echo "commit:  $commit"
  echo "cpu:     $(cpu)"
  if [ -n "${GITHUB_RUN_ID:-}" ]; then
    echo "pinned:  ${EXPECTED_CPU:-no (the runner is not pinned)}"
  fi
  echo "java:    $version"
  echo "spark:   $spark"
  echo
  # From the first ### line to the last, without Spark's own log lines and the shutdown after.
  sed -n '/^###/,/^### Spark/p' "$log" | { grep -v -E '^[0-9]{2}/[0-9]{2}/[0-9]{2} ' || true; }
} > "$out"
if ! grep -q '^### Spark' "$out"; then
  mv "$log" "$name-jdk$jdk.log"
  echo "capacity.scala did not finish; its log is $PWD/$name-jdk$jdk.log, which ends:" >&2
  tail -n 40 "$name-jdk$jdk.log" >&2
  exit 1
fi
rm -f "$log" "$name-jdk$jdk.log"
cat "$out"
