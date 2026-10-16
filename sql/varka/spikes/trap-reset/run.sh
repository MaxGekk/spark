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
# run.sh <out-dir> <tag> <mode:none|flags|clear|redefine> [extra JVM options...]
# One run of the warm arm of VarkaSparkFuzzSuite ("both sides of the warm-up handover", seed
# 20261008) with the template warm-up off, through Replay370, pinned to cores 0-2. Needs
# build.sh's jars in <out-dir> and `dev/varka_matrix.sh --module sql --list` run in the checkout
# (for target/varka-matrix/classpath and jvm.opts). ITER sets the composition count (400).
set -u
W=$(cd "$(dirname "$0")/../../../.." && pwd)
out=$(realpath -m "$1"); tag=$2; mode=$3; shift 3
d=$out/$tag
mkdir -p "$d/tmp"
OPTS=$(cat "$W/target/varka-matrix/jvm.opts")
CP=$out/probe.jar:$W/sql/catalyst/target/scala-2.13/classes
CP=$CP:$(cat "$W/target/varka-matrix/classpath")
cd "$d"
uptime > "$d/load.before"
start=$(date +%s)
SPARK_TESTING=1 SPARK_SCALA_VERSION=2.13 taskset -c 0-2 java $OPTS -Djava.io.tmpdir="$d/tmp" \
  -XX:+UnlockDiagnosticVMOptions -XX:+WhiteBoxAPI -Xbootclasspath/a:"$out/wb.jar" \
  -Dr370.mode="$mode" -Dr370.jar="$out/probe.jar" "$@" \
  -Dvarka.warmup.vectorTemplates=false \
  -Dvarka.sparkfuzz.warmupIterations="${ITER:-400}" -Dvarka.sparkfuzz.seed=20261008 \
  -Dvarka.sparkfuzz.jitEvidence=false -Dspark.test.timeout=170 -Dvarka.test.watchdog.minutes=170 \
  -cp "$CP" Replay370 -oW \
  -s org.apache.spark.sql.execution.VarkaSparkFuzzSuite -z "both sides of the warm-up handover" \
  > "$d/run.log" 2>&1 &
echo $! > "$d/pid"
wait $!
echo $? > "$d/exit"
echo $(( $(date +%s) - start )) > "$d/seconds"
uptime > "$d/load.after"
