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
# The platform under the kernels (VARKA-272, sql/varka/plans/m7/VARKA-272.md 3): the IR fuzzer's
# kernels run under the interpreter, C1, C2, the Vector API's intrinsics off, -Xcomp and C2's
# seeded stress flags, each in a JVM of its own, the answers compared across the arms and every
# arm made to prove its tier.
#
#   dev/varka_platform.sh                         # every product arm: 2000 draws a lane, 20 compiled
#   dev/varka_platform.sh --arms int,c2 --draws 5 # two arms, five compiled draws a lane
#   dev/varka_platform.sh --iterations 200 --draws 2 -j 2 --cpus 3   # small, on a shared machine
#   dev/varka_platform.sh --skip-build --build-dir target/varka-scoped  # on a matrix build
#   dev/varka_platform.sh --seed 20261014 --out target/varka-nightly/2026-10-14/platform
#   dev/varka_platform.sh --list                  # the arms and their flags
#
# Each arm is one JVM running VarkaIrFuzzSuite's two plain tests (--iterations random trees a
# lane: the volume, and the answers) and its compiled test (--draws a lane over the eight batch
# histories of VARKA-274: the tier proof), with one seed, so every arm draws the same corpus. The
# JVM claims its tier (-Dvarka.fuzz.tier) and its vector setting (-Dvarka.fuzz.vectorSupport); the
# suite holds the JVM's own flags to the claim, and fails the arm when a history its tier must
# show did not happen (-Dvarka.fuzz.requireTiers=true): the interpreter arm every call
# interpreted and nothing compiled, C1 some released call served by C1's code and no level-4
# code, the C2 arms a settled warm-up all C2, -Xcomp nothing interpreted. Every compared batch's
# digest goes to the arm's answers.tsv (-Dvarka.fuzz.answers), and dev/varka_platform_report.py
# compares the arms on the keys they share and prints the table.
#
# The arms:
#   int        -Xint                       the interpreter alone; the compiled test's cold history
#   c1         -XX:TieredStopAtLevel=1     C1 alone: cold and released, the warm-ups skip themselves
#   c2         the default JVM             the tiers as a user runs them
#   novec      -XX:-EnableVectorSupport    the Vector API's plain-Java fallback: cold and released,
#              since C2 takes 20 to 30 s a loop method on the int lane's fallback and no warm-up
#              settles (VARKA-272.md 9); released shows C1's OSR code, and C2's where it arrives
#   xcomp      -Xcomp                      everything compiled before its first call
#   stress     StressLCM, StressGCM, StressIGVN, StressCCP, StressIncrementalInlining and
#              StressMacroExpansion, product diagnostic flags, with StressSeed set to the seed
#   fastdebug  the stress flags on a fastdebug JDK, plus its develop stress and verify flags; runs
#              when VARKA_FASTDEBUG_JDK names the build or ../openjdk-build/jdk25's fastdebug
#              image exists beside the repository, else it is skipped with a line
# StressUnstableIfTraps is left out of the stress set: it threw away a warm-up's code on random
# traps often enough that two int-lane cases in five never settled, at ten seconds each
# (VARKA-272.md 9); --jvm-arg -XX:+StressUnstableIfTraps adds it to a longer run.
#
# Each arm runs in <out>/<arm>/, its working directory: run.log, junit/, answers.tsv, jvm.log
# (-Xlog:compilation=info, where EnableVectorSupport prints at startup), arm.jfr (a recording of
# the compiles and deoptimizations the tier witness enables), command.txt, version.txt, exit and
# seconds; after a crash, hs_err_<pid>.log, replay_<pid>.log (when a compiler thread crashed)
# and JFR's emergency hs_err_pid<pid>.jfr land there too, and the report names them. <out>
# defaults to target/varka-platform/<date-time>/. The build - catalyst's test classes, the
# classpath and the JVM options exported as dev/varka_matrix.sh exports them - goes to
# --build-dir (target/varka-platform/build) and is reused with --skip-build.
#
# -j arms run at a time, each under dev/varka_deadline.sh for --arm-deadline seconds (3600), with
# the suite's own caps set past it. --cpus gives each JVM -XX:ActiveProcessorCount, for a run
# beside other work. --variants-every takes the variants of every n-th plain draw (0, none: the
# arms are about the tiers, and the variants triple the plain tests' time).
#
# Exit status: 0 when every arm passed and the arms agree; 1 otherwise; the report says which.

set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
ROOT=$(pwd)
OUT=""
BUILD=$ROOT/target/varka-platform/build
PRODUCT_ARMS=(int c1 c2 novec xcomp stress)
arms=()
iterations=2000
draws=20
seed=$(date +%Y%m%d)
jobs=2
cpus=0
build=1
list=0
arm_deadline=3600
variants_every=0
fastdebug_jdk=${VARKA_FASTDEBUG_JDK:-}
jvm_args=()

# Usage text is found rather than numbered: a hard-coded range silently truncates as the
# comment above it grows. Ends at the first line that is not a comment.
usage() { sed -n '18,/^[^#]/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit "${1:-2}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --arms) IFS=, read -r -a arms <<< "$2"; shift 2 ;;
    --iterations) iterations=$2; shift 2 ;;
    --draws) draws=$2; shift 2 ;;
    --seed) seed=$2; shift 2 ;;
    -j) jobs=$2; shift 2 ;;
    --cpus) cpus=$2; shift 2 ;;
    --out) OUT=$(realpath -m "$2"); shift 2 ;;
    --build-dir) BUILD=$(realpath -m "$2"); shift 2 ;;
    --skip-build) build=0; shift ;;
    --jvm-arg) jvm_args+=("$2"); shift 2 ;;
    --arm-deadline) arm_deadline=$2; shift 2 ;;
    --variants-every) variants_every=$2; shift 2 ;;
    --fastdebug-jdk) fastdebug_jdk=$2; shift 2 ;;
    --list) list=1; shift ;;
    -h|--help) usage 0 ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done
[[ "$seed" =~ ^[0-9]+$ ]] || { echo "the seed is a number, got '$seed'" >&2; exit 2; }

# The fastdebug build: named, or the sibling checkout the lesson files cite (openjdk-build/jdk25).
if [ -z "$fastdebug_jdk" ]; then
  sibling=$(dirname "$ROOT")/openjdk-build/jdk25/build/linux-x86_64-server-fastdebug/images/jdk
  [ -x "$sibling/bin/java" ] && fastdebug_jdk=$sibling
fi
[ ${#arms[@]} -gt 0 ] || arms=("${PRODUCT_ARMS[@]}" fastdebug)

# C2's seeded stress flags, product diagnostics in JDK 25; the seed is the run's, never 0, where
# C2 draws a fresh seed per compile and a crash would not replay.
STRESS=(-XX:+UnlockDiagnosticVMOptions -XX:+StressLCM -XX:+StressGCM -XX:+StressIGVN
  -XX:+StressCCP -XX:+StressIncrementalInlining -XX:+StressMacroExpansion "-XX:StressSeed=$seed")
# The fastdebug build's develop flags: C2's loop stress and recompilation, C1's register
# allocation, invariant motion and range-check stress, and C2's loop and graph verifiers. Its
# per-compilation memory limit, a gigabyte by default and a crash when hit, is raised to two:
# StressIncrementalInlining takes a kernel's epilogue compile from about 340 MB to about 1,160 MB
# (VARKA-272.md 9), so the default limit crashed every run on its first warmed kernel; past two
# gigabytes a crash is a finding again, and its hs_err and replay files stay in the arm's directory.
FASTDEBUG_EXTRA=(-XX:+StressLoopPeeling -XX:+StressDuplicateBackedge -XX:+StressRecompilation
  -XX:+StressLinearScan -XX:+StressLoopInvariantCodeMotion -XX:+StressRangeCheckElimination
  -XX:+VerifyLoopOptimizations -XX:+VerifyGraphEdges "-XX:CompileCommand=MemLimit,*.*,2G~crash")

# One arm's JVM, flags, claimed tier, vector setting, the compiled histories it runs (empty for
# all eight) and its plain draws a lane, into ARM_JAVA, ARM_FLAGS, ARM_TIER, ARM_VECTOR,
# ARM_HISTORY and ARM_ITERATIONS. -Xcomp compiles every method of every drawn kernel before its
# first call, about a second a draw pair, so it takes a tenth of the plain draws.
arm_spec() {
  ARM_JAVA=java; ARM_FLAGS=(); ARM_TIER=c2; ARM_VECTOR=true; ARM_HISTORY=""
  ARM_ITERATIONS=$iterations
  case "$1" in
    int) ARM_FLAGS=(-Xint); ARM_TIER=interpreter; ARM_HISTORY=cold ;;
    c1) ARM_FLAGS=(-XX:TieredStopAtLevel=1); ARM_TIER=c1 ;;
    c2) ;;
    novec) ARM_FLAGS=(-XX:-EnableVectorSupport); ARM_VECTOR=false; ARM_HISTORY=cold,released ;;
    xcomp)
      ARM_FLAGS=(-Xcomp); ARM_TIER=xcomp
      ARM_ITERATIONS=$(( iterations / 10 > 0 ? iterations / 10 : 1 )) ;;
    stress) ARM_FLAGS=("${STRESS[@]}") ;;
    fastdebug)
      ARM_JAVA=$fastdebug_jdk/bin/java
      ARM_FLAGS=("${STRESS[@]}" "${FASTDEBUG_EXTRA[@]}") ;;
    *) echo "unknown arm: $1; the arms are ${PRODUCT_ARMS[*]} fastdebug" >&2; exit 2 ;;
  esac
}

if [ "$list" = 1 ]; then
  for arm in "${arms[@]}"; do
    arm_spec "$arm"
    printf '%-10s tier=%-11s vectorSupport=%-5s draws=%-5s %s %s\n' "$arm" "$ARM_TIER" \
      "$ARM_VECTOR" "$ARM_ITERATIONS" "$ARM_JAVA" "${ARM_FLAGS[*]}"
  done
  exit 0
fi

[ -n "$OUT" ] || OUT=$ROOT/target/varka-platform/$(date +%Y-%m-%dT%H%M%S)
mkdir -p "$OUT" "$BUILD"
if [ "$build" = 1 ]; then
  echo "== build: catalyst's test classes, its test classpath and JVM options"
  build/sbt -batch "catalyst/Test/compile" "export catalyst/Test/fullClasspath" \
    "show catalyst/Test/javaOptions" > "$BUILD/build.log" 2>&1 || {
      echo "build failed; see $BUILD/build.log" >&2; exit 1; }
  # Under CI sbt colours its output, so the escape sequences and carriage returns are stripped
  # before the classpath and the JVM options are read off the log, as varka_matrix.sh does.
  sed 's/\x1b\[[0-9;]*[A-Za-z]//g; s/\r//g' "$BUILD/build.log" > "$BUILD/build.plain.log"
  grep -E '^/[^ ]+:/' "$BUILD/build.plain.log" | tail -1 > "$BUILD/classpath"
  sed -n 's/^\[info\] \* //p' "$BUILD/build.plain.log" | grep -v '^-Djava.io.tmpdir=' \
    > "$BUILD/jvm.opts"
fi
[ -s "$BUILD/classpath" ] && [ -s "$BUILD/jvm.opts" ] || {
  echo "no classpath or JVM options in $BUILD; run without --skip-build" >&2; exit 1; }
grep -q scalatest "$BUILD/classpath" || { echo "no test runner in $BUILD/classpath" >&2; exit 1; }
CP=$(cat "$BUILD/classpath")
mapfile -t OPTS < "$BUILD/jvm.opts"
RUNNER=org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSuiteRunner
SUITE=org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaIrFuzzSuite
# The suite's own caps, past the arm's deadline, so that the deadline is what ends a hung arm.
cap_minutes=$(( arm_deadline / 60 + 2 ))
cpu_args=()
[ "$cpus" -gt 0 ] && cpu_args=("-XX:ActiveProcessorCount=$cpus")

echo "== ${#arms[@]} arm(s): ${arms[*]}; seed $seed, $iterations draws a lane and $draws" \
  "compiled, $jobs at a time, under $OUT"
{
  echo "seed=$seed"; echo "iterations=$iterations"; echo "draws=$draws"
  echo "arms=${arms[*]}"; echo "jvm_args=${jvm_args[*]}"
} > "$OUT/run.txt"

# One arm, in its own directory, under the deadline; its exit status and seconds recorded.
run_arm() {
  local arm=$1
  arm_spec "$arm"
  local dir=$OUT/$arm
  rm -rf "$dir"; mkdir -p "$dir/tmp" "$dir/junit"
  if [ "$arm" = fastdebug ] && [ ! -x "$ARM_JAVA" ]; then
    echo "no fastdebug JDK (VARKA_FASTDEBUG_JDK unset, no sibling openjdk-build/jdk25 image)" \
      > "$dir/skipped"
    echo "-- $arm: skipped, $(cat "$dir/skipped")"
    return 0
  fi
  local history=()
  [ -n "$ARM_HISTORY" ] && history=("-Dvarka.fuzz.history=$ARM_HISTORY")
  # Every arm unlocks the experimental flags, so that EnableVectorSupport is readable from
  # inside the JVM; the flags of the arm come after it. The error, replay and recording files
  # and the working directory are the arm's own, so a crash leaves everything there.
  local command=("$ARM_JAVA" "${OPTS[@]}" "-Djava.io.tmpdir=$dir/tmp"
    -XX:+UnlockExperimentalVMOptions "${cpu_args[@]}"
    "-XX:ErrorFile=$dir/hs_err_%p.log" "-XX:ReplayDataFile=$dir/replay_%p.log"
    "-Xlog:compilation=info:file=$dir/jvm.log"
    "-XX:StartFlightRecording:settings=none,filename=$dir/arm.jfr,dumponexit=true"
    "${ARM_FLAGS[@]}" "${jvm_args[@]}"
    -Dvarka.ownJvm=true "-Dvarka.fuzz.seed=$seed" "-Dvarka.fuzz.iterations=$ARM_ITERATIONS"
    "-Dvarka.fuzz.variantsEvery=$variants_every" "-Dvarka.fuzz.compiled=$draws"
    -Dvarka.fuzz.requireTiers=true "-Dvarka.fuzz.tier=$ARM_TIER"
    "-Dvarka.fuzz.vectorSupport=$ARM_VECTOR" "-Dvarka.fuzz.answers=$dir/answers.tsv"
    "${history[@]}" "-Dspark.test.timeout=$cap_minutes"
    "-Dvarka.test.watchdog.minutes=$cap_minutes"
    -cp "$CP" "$RUNNER" -oW -u "$dir/junit" -s "$SUITE"
    -z "compiled by C2" -z "IR trees match the reference evaluator")
  printf '%q ' "${command[@]}" > "$dir/command.txt"
  "$ARM_JAVA" -version > "$dir/version.txt" 2>&1
  local start; start=$(date +%s)
  (cd "$dir" && SPARK_TESTING=1 SPARK_SCALA_VERSION=2.13 \
    "$ROOT/dev/varka_deadline.sh" "$arm_deadline" "${command[@]}" > "$dir/run.log" 2>&1)
  local status=$?
  echo "$status" > "$dir/exit"
  echo "$(( $(date +%s) - start ))" > "$dir/seconds"
  echo "-- $arm: exit $status in $(cat "$dir/seconds") s"
}

running=0
for arm in "${arms[@]}"; do
  run_arm "$arm" &
  running=$((running + 1))
  if [ "$running" -ge "$jobs" ]; then wait -n; running=$((running - 1)); fi
done
wait

python3 dev/varka_platform_report.py "$OUT"
