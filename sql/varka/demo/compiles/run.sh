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

# Runs the scripts of this directory on a stock Spark distribution and writes the outputs
# committed beside them. What they print are counts of compiled classes, which do not depend on
# the machine, so they run on whatever machine has the distribution:
#
#   sql/varka/demo/compiles/run.sh <spark-home> [<java-home>]

set -euo pipefail
SPARK="${1:?usage: run.sh <spark-home> [<java-home>]}"
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

# run <script> <output name> <spark-shell options...>
run() {
  local script="$1" name="$2"
  shift 2
  local log
  log="$(mktemp)"
  "$SPARK/bin/spark-shell" --conf spark.ui.enabled=false \
    --conf spark.ui.showConsoleProgress=false "$@" -i "$script" > "$log" 2>&1 < /dev/null || true
  local out="$name-jdk$jdk-output.txt"
  {
    echo "Output of $script ($*) on stock Spark, by run.sh."
    echo "commit:  $commit"
    echo "java:    $version"
    echo "spark:   $spark"
    echo
    # From the first ### line to the last, without Spark's own log lines and the shutdown after.
    sed -n '/^###/,/^### Spark/p' "$log" | { grep -v -E '^[0-9]{2}/[0-9]{2}/[0-9]{2} ' || true; }
  } > "$out"
  if ! grep -q '^### Spark' "$out"; then
    echo "$script did not finish; its log is $log" >&2
    return 1
  fi
  rm -f "$log"
  cat "$out"
}

run literals.scala literals --master 'local[1]'
# local-cluster's workers keep their executors' logs under the distribution's work/ directory;
# this run's are removed after it, and any that were there before are left alone.
before="$(ls "$SPARK/work" 2>/dev/null || true)"
run executors.scala executors --master 'local-cluster[2,1,1024]'
for app in $(ls "$SPARK/work" 2>/dev/null || true); do
  grep -qxF "$app" <<< "$before" || rm -rf "${SPARK:?}/work/$app"
done
rmdir "$SPARK/work" 2>/dev/null || true
run cache.scala cache-100 --master 'local[1]'
run cache.scala cache-1000 --master 'local[1]' --conf spark.sql.codegen.cache.maxEntries=1000
