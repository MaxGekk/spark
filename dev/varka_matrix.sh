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
# The option configuration matrix (VARKA-248 step 2, sql/varka/plans/m7/VARKA-248.md 3.2):
# the Varka suites rerun once per configuration - the emit defaults with one option changed -
# and every kernel they emit carries it.
#
#   dev/varka_matrix.sh --config cse=false             # one configuration
#   dev/varka_matrix.sh --config cse=false --config groupBudget=8 -j 2
#   dev/varka_matrix.sh --all -j 8                     # every configuration, eight at a time
#   dev/varka_matrix.sh --list                         # print the configurations and stop
#   dev/varka_matrix.sh --all --skip-build             # reuse the last build and classpath
#
# sbt builds once and exports the test classpath and JVM options; each configuration then runs
# as its own ScalaTest runner JVM in its own directory under target/varka-matrix/, so parallel
# runs share no sbt lock, warehouse or temporary directory. The defaults always run too, as
# configuration zero: a test that fused under the defaults and fuses nothing under a
# configuration fails, unless sql/varka/matrix/skips.tsv marks it "declines" (see VarkaMatrix).
#
# Exit status: 0 when every configuration passed, 1 otherwise; one line per configuration.

set -uo pipefail

cd "$(dirname "$0")/.."
ROOT=$(pwd)
OUT=$ROOT/target/varka-matrix
jobs=1
build=1
list=0
configs=()

usage() { sed -n '18,35p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-2}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --config) configs+=("$2"); shift 2 ;;
    --all) configs+=("ALL"); shift ;;
    -j) jobs="$2"; shift 2 ;;
    --skip-build) build=0; shift ;;
    --list) list=1; shift ;;
    -h|--help) usage 0 ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

# Suites whose subject is the defaults or the machine rather than an answer, so a configuration
# breaks them by construction. Each name with its reason.
EXCLUDED=(
  "VarkaEmittedBytesSuite:compares the emitted bytes with the defaults' committed oracle"
  "VarkaEmitCostAuditSuite:compares the cost audit with the defaults' committed oracle"
  "VarkaEmitOptionSuite:checks the defaults themselves"
  "VarkaMatrixSuite:checks the matrix's own machinery, not an answer"
  "VarkaAssemblySuite:reads the JIT's output, not an answer"
  "VarkaWarmupEndToEndSuite:times a kernel's compilation, which a loaded machine moves"
)

mkdir -p "$OUT"
if [ "$build" = 1 ]; then
  echo "== build: catalyst and sql test classes, the test classpath and JVM options"
  build/sbt -batch 'catalyst/Test/compile' 'sql/Test/compile' \
    'export sql/Test/fullClasspath' 'show sql/Test/javaOptions' > "$OUT/build.log" 2>&1 || {
      echo "build failed; see $OUT/build.log" >&2; exit 1; }
  # The exported classpath is the one line that is a colon-separated list of paths.
  grep -E '^/[^ ]+:/' "$OUT/build.log" | tail -1 > "$OUT/classpath"
  sed -n 's/^\[info\] \* //p' "$OUT/build.log" | grep -v '^-Djava.io.tmpdir=' > "$OUT/jvm.opts"
fi
[ -s "$OUT/classpath" ] && [ -s "$OUT/jvm.opts" ] || {
  echo "no classpath or JVM options in $OUT; run without --skip-build" >&2; exit 1; }
CP=$(cat "$OUT/classpath")
mapfile -t OPTS < "$OUT/jvm.opts"

# The configurations, from VarkaMatrix itself so that the table stays their one source.
java "${OPTS[@]}" -cp "$CP" org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaMatrixMain \
  > "$OUT/configurations" 2> "$OUT/configurations.err" || {
    echo "could not list the configurations; see $OUT/configurations.err" >&2; exit 1; }
if [ "$list" = 1 ]; then cat "$OUT/configurations"; exit 0; fi
[ ${#configs[@]} -gt 0 ] || usage
if [ "${configs[0]}" = "ALL" ]; then mapfile -t configs < "$OUT/configurations"; fi

# Every concrete Varka suite of a module, by its source file, minus the exclusions, as runner
# arguments. Each module runs in a JVM of its own, as sbt runs it: a shape a catalyst suite
# compiled must not already be warm when a SQL suite asks for its first query.
module_suites() {
  local file pkg cls entry skip
  while IFS= read -r file; do
    pkg=$(sed -n 's/^package \(.*\)$/\1/p' "$file" | head -1)
    for cls in $(grep -oE '^class Varka[A-Za-z0-9]*Suite\b' "$file" | awk '{print $2}'); do
      skip=0
      for entry in "${EXCLUDED[@]}"; do [ "${entry%%:*}" = "$cls" ] && skip=1; done
      [ "$skip" = 0 ] && echo "-s $pkg.$cls"
    done
  done < <(grep -rlE '^class Varka[A-Za-z0-9]*Suite\b' "$1" | sort)
}
read -ra catalyst_suites <<< "$(module_suites sql/catalyst/src/test/scala | tr '\n' ' ')"
read -ra sql_suites <<< "$(module_suites sql/core/src/test/scala | tr '\n' ' ')"
echo "== ${#configs[@]} configuration(s) and the defaults," \
  "$(( (${#catalyst_suites[@]} + ${#sql_suites[@]}) / 2 )) suites, $jobs JVMs at a time"

# One module's suites under one configuration, in runs/<configuration>/<module>/.
run_one() {
  local config=$1 module=$2 name=${1:-defaults}
  local dir=$OUT/runs/${name//[^A-Za-z0-9=_.-]/_}/$module
  local -a suites
  if [ "$module" = catalyst ]; then suites=("${catalyst_suites[@]}"); else suites=("${sql_suites[@]}"); fi
  rm -rf "$dir"; mkdir -p "$dir/tmp"
  local start; start=$(date +%s)
  (cd "$dir" && SPARK_TESTING=1 SPARK_SCALA_VERSION=2.13 java "${OPTS[@]}" \
    -Djava.io.tmpdir="$dir/tmp" -Dvarka.matrix.config="$config" \
    -Dvarka.matrix.report="$dir/fused.tsv" \
    -cp "$CP" org.scalatest.tools.Runner -oW -u "$dir/junit" "${suites[@]}" > "$dir/run.log" 2>&1)
  echo "$?" > "$dir/exit"
  echo "$(( $(date +%s) - start ))" > "$dir/seconds"
  echo "$name" > "$dir/../name"
}
# Each configuration's two modules in the background, at most $jobs JVMs at once; the defaults
# first. The SQL suites take the longer, so each configuration starts them first.
rm -rf "$OUT/runs"
all=("" "${configs[@]}")
running=0
for config in "${all[@]}"; do
  for module in sql catalyst; do
    run_one "$config" "$module" &
    running=$((running + 1))
    if [ "$running" -ge "$jobs" ]; then wait -n; running=$((running - 1)); fi
  done
done
wait

python3 dev/varka_matrix_report.py "$OUT/runs" sql/varka/matrix/skips.tsv
