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
# Spark's golden-file suite with its queries fed to Varka (VARKA-271,
# sql/varka/plans/m7/VARKA-271.md 3.2): VarkaSQLQueryTestSuite in a JVM of its own.
#
#   dev/varka_golden_suite.sh                          # the Varka arm over every golden file
#   dev/varka_golden_suite.sh --attribute              # and the cache-only arm over its failures
#   dev/varka_golden_suite.sh --arm cacheOnly          # the Arrow source with Varka off
#   dev/varka_golden_suite.sh --file date.sql --file interval.sql   # a few files only
#   dev/varka_golden_suite.sh --fault misdescribeAdd --file date.sql
#                                                      # the planted fault: date_add's tests fail
#   dev/varka_golden_suite.sh --skip-build --out DIR   # reuse the last build's classpath
#   dev/varka_golden_suite.sh --no-lift                # without lifting literals (VARKA-271.md 3.3)
#
# --file is a substring of the test name, ScalaTest's -z, and may repeat. --deadline SECS stops
# each arm past that many seconds (default 7200). --attribute reruns the cache-only arm over the
# tests the Varka arm failed: a test failing in both is the harness's, one failing only with
# Varka on is Varka's. VARKA_GOLDEN_SUITE_REGEN=true in the environment rewrites
# sql/varka/spark_suites/served.json from the run; the suite refuses unless it is the Varka arm
# over every file with no fault planted. misdescribeAdd shows the suite reaching date_add through
# the lifted literals; misdescribeWordLiveness, under which every emission raises, fails every test
# Varka serves.
#
# The suite needs its own JVM: it sets the Arrow cache serializer, a process-wide field fixed at
# first use. sbt builds once and exports the test classpath and JVM options, as
# dev/varka_matrix.sh does; each arm runs as a ScalaTest runner JVM in its own directory.
#
# Exit status: 0 when the Varka arm (or the one --arm named) passed, 1 otherwise.
set -uo pipefail
cd "$(dirname "$0")/.."
ROOT=$(pwd)
OUT=$ROOT/target/varka-golden-suite
arm=varka
build=1
attribute=0
fault=""
lift=true
deadline=7200
files=()
usage() { sed -n '18,/^[^#]/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit "${1:-2}"; }
while [ $# -gt 0 ]; do
  case "$1" in
    --arm) arm="$2"; shift 2 ;;
    --file) files+=("$2"); shift 2 ;;
    --fault) fault="$2"; shift 2 ;;
    --attribute) attribute=1; shift ;;
    --no-lift) lift=false; shift ;;
    --deadline) deadline="$2"; shift 2 ;;
    --skip-build) build=0; shift ;;
    --out) OUT=$(realpath -m "$2"); shift 2 ;;
    -h|--help) usage 0 ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done
case "$arm" in varka|cacheOnly) ;; *) echo "--arm is varka or cacheOnly, got '$arm'" >&2; exit 2 ;;
esac
mkdir -p "$OUT"

if [ "$build" = 1 ]; then
  echo "== build: sql's test classes, its test classpath and JVM options"
  build/sbt -batch sql/Test/compile "export sql/Test/fullClasspath" "show sql/Test/javaOptions" \
    > "$OUT/build.log" 2>&1 || { echo "build failed; see $OUT/build.log" >&2; exit 1; }
  sed 's/\x1b\[[0-9;]*[A-Za-z]//g; s/\r//g' "$OUT/build.log" > "$OUT/build.plain.log"
  grep -E '^/[^ ]+:/' "$OUT/build.plain.log" | tail -1 > "$OUT/classpath"
  sed -n 's/^\[info\] \* //p' "$OUT/build.plain.log" | grep -v '^-Djava.io.tmpdir=' \
    > "$OUT/jvm.opts"
fi
[ -s "$OUT/classpath" ] && [ -s "$OUT/jvm.opts" ] || {
  echo "no classpath or JVM options in $OUT; run without --skip-build" >&2; exit 1; }
CP=$(cat "$OUT/classpath")
mapfile -t OPTS < "$OUT/jvm.opts"

# Runs one arm over the given -z filters (none: every test) in $OUT/<name>; prints its summary.
run_arm() {
  local name=$1 which=$2; shift 2
  local dir=$OUT/$name
  local -a filters=() extra=()
  local f
  for f in "$@"; do filters+=(-z "$f"); done
  [ -n "$fault" ] && extra+=("-Dvarka.goldenSuite.fault=$fault")
  rm -rf "$dir"; mkdir -p "$dir/tmp" "$dir/junit"
  local start; start=$(date +%s)
  (cd "$dir" && SPARK_TESTING=1 SPARK_SCALA_VERSION=2.13 "$ROOT/dev/varka_deadline.sh" \
    "$deadline" java "${OPTS[@]}" -Xmx6g -Djava.io.tmpdir="$dir/tmp" \
    -Dspark.test.home="$ROOT" -Dvarka.goldenSuite.arm="$which" -Dvarka.goldenSuite.lift="$lift" \
    -Dvarka.goldenSuite.servedOut="$dir/served.json" "${extra[@]}" \
    -cp "$CP" org.scalatest.tools.Runner -oW -u "$dir/junit" \
    -s org.apache.spark.sql.VarkaSQLQueryTestSuite "${filters[@]}" > "$dir/run.log" 2>&1)
  local status=$?
  echo "$status" > "$dir/exit"
  echo "$(( $(date +%s) - start ))" > "$dir/seconds"
  echo "== $name ($which): exit $status in $(cat "$dir/seconds") s;" \
    "$(grep -E '^Tests: ' "$dir/run.log" | tail -1)"
  return "$status"
}

# The names of the tests an arm failed, one a line, from its JUnit XML.
failed_tests() {
  python3 - "$OUT/$1/junit" <<'EOF'
import glob, sys, xml.etree.ElementTree as ET
for path in glob.glob(sys.argv[1] + "/*.xml"):
    for case in ET.parse(path).getroot().iter("testcase"):
        if case.find("failure") is not None or case.find("error") is not None:
            print(case.get("name"))
EOF
}

run_arm "$arm" "$arm" "${files[@]}"
status=$?
if [ "$attribute" = 1 ] && [ "$arm" = varka ] && [ "$status" != 0 ]; then
  mapfile -t failed < <(failed_tests varka)
  if [ "${#failed[@]}" -gt 0 ]; then
    run_arm cacheOnly cacheOnly "${failed[@]}"
    mapfile -t harness < <(failed_tests cacheOnly)
    for t in "${failed[@]}"; do
      kind="varka"
      for h in "${harness[@]}"; do [ "$h" = "$t" ] && kind="harness"; done
      echo "failed ($kind): $t"
    done
  fi
fi
exit "$status"
