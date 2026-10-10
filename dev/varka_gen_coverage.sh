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
# The coverage of the classes Varka emits (VARKA-285): every Varka suite of catalyst and
# sql/core, as the gate's wide step runs them, under the JaCoCo agent, then each emitted class
# analysed on its own and the report written to sql/varka/coverage/generated.md.
#
#   dev/varka_gen_coverage.sh                 # run the suites under the agent, then the report
#   dev/varka_gen_coverage.sh --report-only   # the report again, from the last run's data
#   dev/varka_gen_coverage.sh -j 6 --split 3  # the matrix's parallelism (these are the defaults)
#
# The agent instruments an emitted class once inclnolocationclasses is on - it is defined by an
# ordinary class loader with no code source - and classdumpdir keeps its bytes, which exist
# nowhere else. Every test JVM appends to one execution file; JaCoCo locks it to append. JaCoCo's
# own report cannot read the result, since the suites emit one class name with different bytes,
# so dev/varka_gen_coverage/VarkaGenCoverage.java analyses each class with a builder of its own.
# It runs as a single-file program against org.jacoco.core and ASM from the local Maven
# repository, fetched there if missing, and catalyst's built classes: no build gains a dependency.
#
# Exit status: the suites' (a failed suite still reports the coverage it reached), else the
# analyser's.
set -uo pipefail

cd "$(dirname "$0")/.."
ROOT=$(pwd)
OUT=$ROOT/target/varka-gen-coverage
REPORT=$ROOT/sql/varka/coverage/generated.md
jobs=6
split=3
run=1
while [ "$#" -gt 0 ]; do
  case "$1" in
    -j) jobs="$2"; shift 2 ;;
    --split) split="$2"; shift 2 ;;
    --report-only) run=0; shift ;;
    -h|--help) sed -n '18,/^set /p' "$0" | sed '$d'; exit 0 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done

JACOCO=0.8.15
ASM=9.10.1
M2=${M2_REPO:-$HOME/.m2/repository}
AGENT=$M2/org/jacoco/org.jacoco.agent/$JACOCO/org.jacoco.agent-$JACOCO-runtime.jar
CORE=$M2/org/jacoco/org.jacoco.core/$JACOCO/org.jacoco.core-$JACOCO.jar
LIBS=("$AGENT" "$CORE")
for a in asm asm-commons asm-tree; do LIBS+=("$M2/org/ow2/asm/$a/$ASM/$a-$ASM.jar"); done
fetch() {
  local coordinate=$1
  build/mvn -q dependency:get -Dartifact="$coordinate" -Dtransitive=false > /dev/null
}
[ -f "$AGENT" ] || fetch "org.jacoco:org.jacoco.agent:$JACOCO:jar:runtime"
[ -f "$CORE" ] || fetch "org.jacoco:org.jacoco.core:$JACOCO"
for a in asm asm-commons asm-tree; do
  [ -f "$M2/org/ow2/asm/$a/$ASM/$a-$ASM.jar" ] || fetch "org.ow2.asm:$a:$ASM"
done
for lib in "${LIBS[@]}"; do
  [ -f "$lib" ] || { echo "varka_gen_coverage: $lib is missing" >&2; exit 1; }
done

status=0
if [ "$run" = 1 ]; then
  rm -rf "$OUT/jacoco.exec" "$OUT/classes"
  mkdir -p "$OUT"
  # The emitted kernels' packages and the names the suites emit under.
  includes='org.apache.spark.sql.varka.*:org.apache.spark.sql.catalyst.expressions.codegen.varka.*'
  includes+=':Varka*:t:X'
  agent="-javaagent:$AGENT=destfile=$OUT/jacoco.exec,append=true"
  agent+=",inclnolocationclasses=true,classdumpdir=$OUT/classes,includes=$includes"
  start=$(date +%s)
  dev/varka_matrix.sh --defaults --out "$OUT/matrix" --split "$split" -j "$jobs" \
    --skip-suites VarkaAssemblySuite,VarkaWarmupEndToEndSuite --jvm-arg "$agent"
  status=$?
  echo "varka_gen_coverage: the suites took $(( $(date +%s) - start )) s, exit status $status"
fi

CATALYST=$ROOT/sql/catalyst/target/scala-2.13/classes
[ -d "$CATALYST" ] || { echo "varka_gen_coverage: no catalyst classes at $CATALYST" >&2; exit 1; }
[ -f "$OUT/jacoco.exec" ] || { echo "varka_gen_coverage: no $OUT/jacoco.exec" >&2; exit 1; }
mkdir -p "$(dirname "$REPORT")"
cp="$CORE:$CATALYST"
for a in asm asm-commons asm-tree; do cp+=":$M2/org/ow2/asm/$a/$ASM/$a-$ASM.jar"; done
java -cp "$cp" dev/varka_gen_coverage/VarkaGenCoverage.java "$OUT/jacoco.exec" "$OUT/classes" \
  "$REPORT" || exit 1
echo "varka_gen_coverage: report written to ${REPORT#"$ROOT"/}"
exit "$status"
