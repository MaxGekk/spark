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

# The Arrow question of VARKA-291 (plan section 9.7): what an Arrow batch costs as the wire image
# or the store of an IR graph held as layout D's columns. Needs the Arrow jars (arrow-vector,
# arrow-memory, arrow-format, flatbuffers and their dependencies, e.g. the jars/ of a Spark
# distribution) in ARROW_JARS. Arrow 19's patched netty allocator needs netty 4.1, not the 4.2 a
# Spark 4.2 distribution carries: ARROW_NETTY is a classpath of netty-buffer and netty-common 4.1
# jars, put first.
#
#   ARROW_JARS=/path/to/spark/jars ARROW_NETTY=/m2/netty-buffer-4.1.126.Final.jar:/m2/netty-common-\
# 4.1.126.Final.jar sql/varka/spikes/ir-layout/arrow-spike.sh OUTFILE

set -euo pipefail

: "${ARROW_JARS:?set ARROW_JARS to a directory with the Arrow jars}"
outfile=${1:?output file}
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../../.." && pwd)
java=${JAVA_HOME:+$JAVA_HOME/bin/}java
javac=${JAVA_HOME:+$JAVA_HOME/bin/}javac
pkg=sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen/varka
test_pkg=sql/catalyst/src/test/java/org/apache/spark/sql/catalyst/expressions/codegen/varka
main=org.apache.spark.sql.catalyst.expressions.codegen.varka

cp="${ARROW_NETTY:+$ARROW_NETTY:}$ARROW_JARS/*"
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
"$javac" -cp "$cp" -d "$out" "$root/$pkg/VarkaVectorIR.java" \
  "$root/$test_pkg/VarkaIrDescription.java" "$here"/src/*.java "$here"/src-arrow/*.java 2>&1 \
  | { grep -v '^Note:' || true; }

fast=""
maxf=0
for d in /sys/devices/system/cpu/cpu[0-9]*/cpufreq; do
  f=$(cat "$d/cpuinfo_max_freq" 2>/dev/null || echo 0)
  [ "$f" -gt "$maxf" ] && maxf=$f
done
for d in /sys/devices/system/cpu/cpu[0-9]*/cpufreq; do
  c=$(basename "$(dirname "$d")"); c=${c#cpu}
  [ "$(cat "$d/cpuinfo_max_freq" 2>/dev/null || echo 0)" = "$maxf" ] && fast="${fast:+$fast,}$c"
done
runner=()
[ -n "$fast" ] && command -v taskset > /dev/null && runner=(taskset -c "$fast")

{
  echo "# VARKA-291 Arrow spike, $(date +%Y-%m-%d), pin ${runner[*]:-none}"
  echo "# jdk: $("$java" -version 2>&1 | head -1)"
  echo "# arrow: $(ls "$ARROW_JARS" | grep -E '^arrow-vector' | head -1)"
} > "$outfile"
for graph in cheap_tails-22 wide_int-1 size_ladder-100 size_ladder-200 deep_chain-1024 \
    grown_ladder-2000; do
  "${runner[@]}" "$java" --add-opens java.base/java.nio=ALL-UNNAMED \
    -Dio.netty.tryReflectionSetAccessible=true -Xss16m -Xmx4g -cp "$out:$cp" \
    "$main.ArrowSpike" "$here/graphs" "$graph" 2>&1 | grep -E '^ARROW' >> "$outfile" \
    || echo "FAILURE $graph" >> "$outfile"
done
