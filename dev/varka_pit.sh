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
# A bounded mutation run (VARKA-265): PIT mutates the emitter classes named, runs the catalyst
# Varka ScalaTest suites against each mutant test by test, and gives each mutant one of three
# verdicts - killed by a behavioural test, killed only by the emitted-bytes oracle, survived.
#
#   dev/varka_pit.sh                                  # Slots and Analysis, every eligible suite
#   dev/varka_pit.sh --classes Analysis --suites VarkaEmitterValiditySuite --methods liveWords
#   dev/varka_pit.sh --threads 10 --report sql/varka/mutation/Slots-Analysis.md
#   dev/varka_pit.sh --skip-build --verdict-only      # the verdicts again, from the last run
#   dev/varka_pit.sh --shard 2/8 --threads 3 --heap 3g --out target/varka-pit-2   # one CI shard
#
# Two PIT runs over the same mutants, since PIT stops a mutant at its first kill. The first runs
# every eligible suite's behavioural tests, so a kill there is behavioural; the second runs the
# structural ones - VarkaEmittedBytesSuite and every test tagged PinsDefaults, whose subject is the
# defaults' emitted structure (a grouping, an op count) - on the mutants the first left, so a
# mutant only they kill changed what the emitter emits and no answer: read as a survivor, since
# they kill equivalent mutants too (m7/PLAN.md 2.2). The report calls them the bytes oracle. PIT identifies
# a mutant by class, method, descriptor, mutator and index, which both runs share, and
# dev/varka_pit_verdicts.py joins them and writes the report.
#
# PIT is the official org.pitest artifacts from Maven Central (pitest, pitest-entry and
# pitest-command-line, version below, publisher the pitest project, pitest.org), fetched into the
# local Maven repository if missing. Its test plugin for ScalaTest is dev/varka_pit/, written here
# against PIT's SPI: no third-party plugin. The suites' JVM options are sbt's, with the memory
# sanitizer and both bodies checked as in the matrix, and own-JVM suites run inline
# (-Dvarka.ownJvm=true), since a child JVM would load the unmutated classes. Suites whose verdict
# is the JIT's or the machine's, and the JUnit 5 suites, which PIT's plugin does not reach, are
# left out (the list below, each with its reason).
#
# The full run is sharded on GitHub's runners by .github/workflows/varka-mutation.yml (the owner,
# 16 October 2026): each shard runs this script with --shard K/N, both runs over its slice, and a
# last job joins the shards' mutations.xml into one report with dev/varka_pit_verdicts.py --shards.
# A shard writes no report of its own, since a note on a method whose mutant fell to another shard
# would read as stale. The slice is dev/varka_pit/VarkaPitOnly's: each class's mutants sorted by
# PIT's identity and dealt round-robin, so the shards are even in count and a method's mutants,
# whose survivors cost the run, spread across them.
#
# Options:
#   --classes A,B     emitter classes to mutate, simple names in codegen.varka (Slots,Analysis)
#   --methods m,n     mutate only these methods: every other method name of the classes, read
#                     with javap, goes to PIT's --excludedMethods (PIT has no inclusion filter)
#   --suites S,T      only these suites (simple names); default every eligible one
#   --threads N       PIT minion threads (default 8)
#   --heap SIZE       each minion's -Xmx, in place of sbt's 4g (a runner fits three at 3g)
#   --shard K/N       mutate only shard K of N (K from zero); writes no report, see above
#   --timeout-factor F, --timeout-const MS   PIT's mutant timeout, F times the test's unmutated
#                     time plus MS (default 2 and 60000); a TIMED_OUT mutant counts as killed, so
#                     a slower or noisier machine than the laptop wants a larger factor
#   --unit-size N     mutants per PIT work unit (default 20): PIT otherwise makes one unit of a
#                     class, which one thread runs alone however many are given
#   --out DIR         working directory (default target/varka-pit)
#   --report FILE     the verdicts' markdown (default DIR/verdicts.md)
#   --mutators LIST   PIT mutators (default DEFAULTS,NON_VOID_METHOD_CALLS,REMOVE_CONDITIONALS)
#   --fuzz-iterations N   the IR fuzzer's budget a lane under PIT (default 30, a tenth of its own)
#   --skip-build      reuse DIR's classpath and JVM options
#   --dry-run         PIT's dry run: the mutants and the coverage phase, no mutant run
#   --verdict-only    only join the last two runs and write the report
#
# Exit status: 0 when both runs and the report finished, 1 otherwise.
set -uo pipefail

cd "$(dirname "$0")/.."
ROOT=$(pwd)
OUT=$ROOT/target/varka-pit
classes=Slots,Analysis
methods=""
suites=""
threads=8
heap=""
shard=""
timeout_factor=2
timeout_const=60000
unit_size=20
report=""
mutators=DEFAULTS,NON_VOID_METHOD_CALLS,REMOVE_CONDITIONALS
fuzz_iterations=30
build=1
run=1
dry=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --classes) classes="$2"; shift 2 ;;
    --methods) methods="$2"; shift 2 ;;
    --suites) suites="$2"; shift 2 ;;
    --threads) threads="$2"; shift 2 ;;
    --heap) heap="$2"; shift 2 ;;
    --shard) shard="$2"; shift 2 ;;
    --timeout-factor) timeout_factor="$2"; shift 2 ;;
    --timeout-const) timeout_const="$2"; shift 2 ;;
    --unit-size) unit_size="$2"; shift 2 ;;
    --out) OUT=$(realpath -m "$2"); shift 2 ;;
    --report) report=$(realpath -m "$2"); shift 2 ;;
    --mutators) mutators="$2"; shift 2 ;;
    --fuzz-iterations) fuzz_iterations="$2"; shift 2 ;;
    --skip-build) build=0; shift ;;
    --dry-run) dry=1; shift ;;
    --verdict-only) run=0; build=0; shift ;;
    -h|--help) sed -n '18,/^set /p' "$0" | sed '$d'; exit 0 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done
[ -n "$report" ] || report=$OUT/verdicts.md
if [ -n "$shard" ]; then
  [[ "$shard" =~ ^([0-9]+)/([0-9]+)$ ]] && [ "${BASH_REMATCH[1]}" -lt "${BASH_REMATCH[2]}" ] || {
    echo "--shard is K/N with 0 <= K < N, not $shard" >&2; exit 2; }
  [ "$run" = 1 ] || { echo "--shard writes no report; join shards with" \
    "dev/varka_pit_verdicts.py --shards" >&2; exit 2; }
fi
PKG=org.apache.spark.sql.catalyst.expressions.codegen.varka
BYTES_ORACLE=$PKG.VarkaEmittedBytesSuite

# Suites left out of both runs, each with its reason: the matrix's own exclusions for the JIT and
# the machine, and the suites that check tooling rather than the emitter's answers.
EXCLUDED=(
  "VarkaAssemblySuite:reads the JIT's output, not an answer"
  "VarkaTierWitnessSuite:asserts which JIT tier ran a call, which a loaded machine moves"
  "VarkaInliningCliffSuite:measures where C2 stops inlining, not an answer"
  "VarkaDeoptCycleSuite:counts the JIT's deoptimizations, not an answer"
  "VarkaCompilationWatchSuite:watches C2's compilations, not an answer"
  "VarkaWidthAuditSuite:reads C2's printed lowerings, which fail under PIT's agent"
  "VarkaKernelWarmupSuite:waits on the JIT's tiers, which a mutant run's machine moves"
  "VarkaVectorTemplateWarmupSuite:waits on the JIT's tiers, which a mutant run's machine moves"
  "VarkaTestWatchdogSuite:checks the suites' watchdog, not the emitter"
  "VarkaProbeOutputSuite:checks a probe's output parsing, not the emitter"
  "VarkaMatrixSuite:checks the matrix's own machinery, not an answer"
  "VarkaEmitCostAuditSuite:compares the cost audit with a committed file, as the bytes do"
  "VarkaEmittedBytesSuite:the bytes oracle, run second with the PinsDefaults tests"
)

PIT=1.30.0
M2=${M2_REPO:-$HOME/.m2/repository}
PIT_JARS=()
for a in pitest pitest-entry pitest-command-line; do
  PIT_JARS+=("$M2/org/pitest/$a/$PIT/$a-$PIT.jar")
done
fetch() {
  build/mvn -q dependency:get -Dartifact="$1" > /dev/null
}
[ -f "${PIT_JARS[2]}" ] || fetch "org.pitest:pitest-command-line:$PIT"
for jar in "${PIT_JARS[@]}"; do
  [ -f "$jar" ] || { echo "varka_pit: $jar is missing" >&2; exit 1; }
done

mkdir -p "$OUT"
if [ "$build" = 1 ]; then
  echo "== build: catalyst's test classes, its test classpath and JVM options"
  build/sbt -batch catalyst/Test/compile "export catalyst/Test/fullClasspath" \
    "show catalyst/Test/javaOptions" > "$OUT/build.log" 2>&1 || {
      echo "build failed; see $OUT/build.log" >&2; exit 1; }
  # Read as dev/varka_matrix.sh reads them: the one colon-separated line, and the option lines.
  sed 's/\x1b\[[0-9;]*[A-Za-z]//g; s/\r//g' "$OUT/build.log" > "$OUT/build.plain.log"
  grep -E '^/[^ ]+:/' "$OUT/build.plain.log" | tail -1 > "$OUT/classpath"
  sed -n 's/^\[info\] \* //p' "$OUT/build.plain.log" | grep -v '^-Djava.io.tmpdir=' \
    > "$OUT/jvm.opts"
fi
if [ "$run" = 1 ]; then
  [ -s "$OUT/classpath" ] && [ -s "$OUT/jvm.opts" ] || {
    echo "no classpath or JVM options in $OUT; run without --skip-build" >&2; exit 1; }
  CP=$(cat "$OUT/classpath")
  grep -q scalatest "$OUT/classpath" || { echo "no test classpath in $OUT/classpath" >&2; exit 1; }

  # The plugin, compiled against the suites' own ScalaTest and registered for ServiceLoader.
  rm -rf "$OUT/plugin" && mkdir -p "$OUT/plugin/META-INF/services"
  javac -Xlint:all,-path -Werror -d "$OUT/plugin" \
    -cp "$CP:$(IFS=:; echo "${PIT_JARS[*]}")" \
    dev/varka_pit/VarkaPitScalaTest.java dev/varka_pit/VarkaPitOnly.java || exit 1
  services=$OUT/plugin/META-INF/services
  echo VarkaPitScalaTest > "$services/org.pitest.testapi.TestPluginFactory"
  echo VarkaPitOnly > "$services/org.pitest.mutationtest.build.MutationInterceptorFactory"

  # The eligible suites: every concrete ScalaTest Varka...Suite of catalyst minus the exclusions.
  mapfile -t found < <(grep -rlE '^class Varka[A-Za-z0-9]*Suite\b' sql/catalyst/src/test/scala \
    | sort | while IFS= read -r file; do
      pkg=$(sed -n 's/^package \(.*\)$/\1/p' "$file" | head -1)
      for cls in $(grep -oE '^class Varka[A-Za-z0-9]*Suite\b' "$file" | awk '{print $2}'); do
        skip=0
        for entry in "${EXCLUDED[@]}"; do [ "${entry%%:*}" = "$cls" ] && skip=1; done
        [ -n "$suites" ] && ! [[ ",$suites," == *",$cls,"* ]] && skip=1
        [ "$skip" = 0 ] && echo "$pkg.$cls"
      done
    done)
  [ ${#found[@]} -gt 0 ] || { echo "no eligible suite" >&2; exit 1; }
  printf '%s\n' "${found[@]}" > "$OUT/suites"
  # Which shard this is, for dev/varka_pit_verdicts.py --shards to check none is missing.
  if [ -n "$shard" ]; then echo "$shard" > "$OUT/shard"; else rm -f "$OUT/shard"; fi

  # The test classpath, one entry a line; the plugin first, so ServiceLoader finds it in minions.
  # Spark's sbt exports catalyst as a jar, and PIT mutates classes in a directory it is given, so
  # the jar gives way to the directory it was packaged from.
  CLASSES=$ROOT/sql/catalyst/target/scala-2.13/classes
  { echo "$OUT/plugin"; tr ':' '\n' <<< "$CP" \
      | sed -E "s|^$ROOT/sql/catalyst/target/scala-2.13/spark-catalyst_[^/]*\.jar\$|$CLASSES|"; } \
    > "$OUT/classpath.lines"
  # pitest-entry needs Commons Text and Lang in the launching JVM; Spark's own pinned versions,
  # from the test classpath, rather than a second copy.
  LAUNCH_EXTRA=$(grep -E '/commons-(text|lang3)-[0-9.]+\.jar$' "$OUT/classpath.lines" | paste -sd:)
  [ -n "$LAUNCH_EXTRA" ] || { echo "no commons-text on the test classpath" >&2; exit 1; }
  grep -qx "$CLASSES" "$OUT/classpath.lines" || {
    echo "catalyst's classes are not on the test classpath" >&2; exit 1; }
  targets=""
  for c in ${classes//,/ }; do targets+="${targets:+,}$PKG.$c,$PKG.$c\$*"; done
  # --methods: PIT filters methods by exclusion only, so exclude every other name the classes have.
  excluded_methods=""
  if [ -n "$methods" ]; then
    CLASSES_DIR=$CLASSES/${PKG//.//}
    lambda_re='^lambda[$]([^$]+)[$][0-9]+$'
    # javap names a constructor by its class, so PIT's <init> and <clinit> are added by hand.
    for name in $( (for c in ${classes//,/ }; do
        for f in "$CLASSES_DIR/$c.class" "$CLASSES_DIR/$c\$"*.class; do
          [ -f "$f" ] && javap -p "$f"
        done
      done | sed -nE 's/.* ([A-Za-z0-9_$<>]+)\(.*/\1/p'; echo '<init>'; echo '<clinit>') \
      | sort -u); do
      # A lambda's body is its method's code: lambda$<method>$<n> goes with <method>.
      owner=$name
      [[ "$name" =~ $lambda_re ]] && owner=${BASH_REMATCH[1]}
      [[ ",$methods," == *",$owner,"* ]] || excluded_methods+="${excluded_methods:+,}$name"
    done
  fi
  if [ -n "$heap" ]; then
    # sbt's options name -Xmx twice; drop both rather than rely on the last one winning.
    mapfile -t OPTS < <(grep -v '^-Xmx' "$OUT/jvm.opts")
    OPTS+=("-Xmx$heap")
  else
    mapfile -t OPTS < "$OUT/jvm.opts"
  fi
  # The IR fuzzer's C2 test runs each draw through eight JIT histories (482 s of the 869 s
  # coverage phase on 16 October 2026): no draws under PIT, since what a mutant changes is the
  # emission, which the fuzzer's interpreted test reaches over the same draws.
  OPTS+=(-Dvarka.sanitizeMemory=true -Dvarka.checkBothBodies=true -Dvarka.ownJvm=true
    "-Dvarka.fuzz.iterations=$fuzz_iterations" -Dvarka.fuzz.compiled=0)
  argline="${OPTS[*]}"

  # The run's scope, for the report's opening line.
  echo "Mutated: \`${classes//,/\`, \`}\`${methods:+, methods \`${methods//,/\`, \`}\`}," \
    "mutators $mutators; behavioural run: the untagged tests of ${#found[@]} catalyst suites;" \
    "the bytes oracle: VarkaEmittedBytesSuite and their PinsDefaults tests." > "$OUT/scope"

  pit() {
    local name=$1 tests=$2 structure=$3 only=${4:-} start status
    rm -rf "${OUT:?}/$name" && mkdir -p "$OUT/$name"
    local extra=()
    [ -n "$excluded_methods" ] && extra+=(--excludedMethods "$excluded_methods")
    [ "$dry" = 1 ] && extra+=(--dryRun true)
    start=$(date +%s)
    local launch=()
    [ -n "$only" ] && launch+=("-Dvarka.pit.only=$only")
    # Both runs take the shard: the second's keep file is already inside it, and the filter
    # stops a wrong keep file widening the slice.
    [ -n "$shard" ] && launch+=("-Dvarka.pit.shard=$shard")
    java "${launch[@]}" -cp "$OUT/plugin:$(IFS=:; echo "${PIT_JARS[*]}"):$LAUNCH_EXTRA" \
      org.pitest.mutationtest.commandline.MutationCoverageReport \
      --classPathFile "$OUT/classpath.lines" \
      --targetClasses "$targets" --targetTests "$tests" \
      --sourceDirs sql/catalyst/src/main/java \
      --mutableCodePaths "$CLASSES" \
      --mutators "$mutators" --threads "$threads" --mutationUnitSize "$unit_size" \
      --outputFormats XML,CSV --timestampedReports false --reportDir "$OUT/$name" \
      --timeoutConst "$timeout_const" --timeoutFactor "$timeout_factor" \
      --argLine "$argline -Dvarka.pit.structure=$structure" --verbosity VERBOSE_NO_SPINNER "${extra[@]}" \
      > "$OUT/$name.log" 2>&1
    status=$?
    echo "varka_pit: run $name took $(( $(date +%s) - start )) s, exit status $status"
    echo "$status" > "$OUT/$name.status"
    return "$status"
  }
  behavioural=$(IFS=,; echo "${found[*]}")
  pit behavioural "$behavioural" exclude || exit 1
  if [ "$dry" = 1 ]; then
    echo "varka_pit: dry run; the mutants are in $OUT/behavioural/mutations.xml"
    exit 0
  fi
  # The second run needs only the mutants the first did not kill (VarkaPitOnly filters to them).
  python3 dev/varka_pit_verdicts.py --keep "$OUT/behavioural/mutations.xml" "$OUT/keep.tsv" \
    || exit 1
  if [ -s "$OUT/keep.tsv" ]; then
    pit bytes "$BYTES_ORACLE,$behavioural" only "$OUT/keep.tsv" || exit 1
  else
    rm -rf "${OUT:?}/bytes" && mkdir -p "$OUT/bytes"
    echo '<mutations/>' > "$OUT/bytes/mutations.xml"
  fi
  if [ -n "$shard" ]; then
    echo "varka_pit: shard $shard done; its runs are in ${OUT#"$ROOT"/}"
    exit 0
  fi
fi

python3 dev/varka_pit_verdicts.py "$OUT/behavioural/mutations.xml" "$OUT/bytes/mutations.xml" \
  "$report" --notes sql/varka/mutation/notes.tsv --scope "$(cat "$OUT/scope" 2>/dev/null)" \
  || exit 1
echo "varka_pit: verdicts written to ${report#"$ROOT"/}"
