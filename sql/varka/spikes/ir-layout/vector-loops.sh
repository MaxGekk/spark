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

# Which layout the JVM can vectorize a pass over (VARKA-291 plan, section 9.7): VectorLoops over
# heap columns, off-heap columns and off-heap rows, with C2's auto-vectorizer on and off, and with
# the Vector API. JDK from JAVA_HOME (default: the one on the path); needs the incubator module.
#
#   sql/varka/spikes/ir-layout/vector-loops.sh OUTFILE

set -euo pipefail

outfile=${1:?output file}
here=$(cd "$(dirname "$0")" && pwd)
java=${JAVA_HOME:+$JAVA_HOME/bin/}java
javac=${JAVA_HOME:+$JAVA_HOME/bin/}javac

out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
"$javac" --add-modules jdk.incubator.vector -d "$out" "$here"/src-vector/*.java 2>&1 \
  | { grep -v -E '^(Note:|warning:|1 warning)' || true; }

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

width=$("$java" --add-modules jdk.incubator.vector -XX:+PrintFlagsFinal -version 2>/dev/null \
  | awk '/ MaxVectorSize /{print "MaxVectorSize=" $4}')
{
  echo "# VARKA-291 vector loops, $(date +%Y-%m-%d), pin ${runner[*]:-none}"
  echo "# jdk: $("$java" -version 2>&1 | head -1)"
  echo "# $width"
} > "$outfile"
for mode in heap-columns segment-columns segment-rows vector-heap vector-segment; do
  for sw in on off; do
    flag=(); [ "$sw" = off ] && flag=(-XX:-UseSuperWord)
    "${runner[@]}" "$java" --add-modules jdk.incubator.vector "${flag[@]}" \
      -Dvloops.superword=$sw \
      -cp "$out" org.apache.spark.sql.catalyst.expressions.codegen.varka.VectorLoops "$mode" \
      2>&1 | grep -E '^VLOOP' >> "$outfile" || echo "FAILURE $mode $sw" >> "$outfile"
  done
done
