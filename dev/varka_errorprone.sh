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
# Error Prone and NullAway over Varka's Java (VARKA-266): its own javac pass, apart from the build,
# over the Varka sources of sql/catalyst and sql/core, against the classpath sbt exports, writing
# class files nothing reads. A warning fails it (-Werror). Three further checks:
#   - NullAway treats the classes in dev/varka_nullaway_pending.txt as unannotated, and checks
#     every other class; the list may only shrink, so a line the merge base's list does not have
#     fails the pass;
#   - every @SuppressWarnings in those sources says why, in a comment on its line or the line
#     above;
#   - the JDK is the one the javac flags below are written for.
#
#   dev/varka_errorprone.sh                         # export the classpath with sbt, then check
#   dev/varka_errorprone.sh --classpath-file FILE   # a classpath already exported (the gate's)
#   dev/varka_errorprone.sh --base REF              # the ref the pending list is compared with
#
# The jars are pinned by version and sha256 and fetched into build/varka-errorprone/ on first use.

set -uo pipefail

cd "$(dirname "$0")/.."
ROOT=$(pwd)
OUT=$ROOT/target/varka-errorprone
LIB=$ROOT/build/varka-errorprone
PENDING=$ROOT/dev/varka_nullaway_pending.txt
classpath_file=""
base=""

usage() { sed -n '/^# Error Prone and NullAway/,/^# The jars/p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
while [ $# -gt 0 ]; do
  case "$1" in
    --classpath-file) classpath_file=$2; shift 2 ;;
    --base) base=$2; shift 2 ;;
    -h|--help) usage ;;
    *) echo "unknown option: $1" >&2; usage ;;
  esac
done
mkdir -p "$OUT" "$LIB"
failed=0
fail() { echo "varka_errorprone: $*" >&2; failed=1; }

# The --add-exports below are javac's internals as of JDK 25; another major version is refused by
# name rather than met as an IllegalAccessError from inside the plugin.
major=$(java -XshowSettings:properties -version 2>&1 | sed -n 's/^ *java.specification.version = //p')
if [ "$major" != 25 ]; then
  echo "varka_errorprone: needs JDK 25, found ${major:-unknown}" >&2
  exit 1
fi

# The jars: Error Prone's bundle, the Checker Framework dataflow it loads but does not bundle, and
# NullAway with its two dependencies (Guava comes in Error Prone's bundle).
jars=(
  "com/google/errorprone/error_prone_core/2.50.0/error_prone_core-2.50.0-with-dependencies.jar 8ec037a6d57c0d880ed78c6a67445e5018a17a89b42cb6847ddef9081c504378"
  "io/github/eisop/dataflow-errorprone/3.41.0-eisop1/dataflow-errorprone-3.41.0-eisop1.jar 10434fba4e53f55fa9c76904cde414b918932548c9dfc4e2d634ac05ff7a7d10"
  "com/uber/nullaway/nullaway/0.14.2/nullaway-0.14.2.jar eb02eb2da5ee30bcac4621a259a84153b20690078dc47ef920589b8371f456ed"
  "org/jspecify/jspecify/1.0.1/jspecify-1.0.1.jar 070d75f261fe4c5b8202508366715f7f2d4660f88c8ef7e6d3575e48c9683b66"
  "org/checkerframework/dataflow-nullaway/4.2.3/dataflow-nullaway-4.2.3.jar 0524cc55673ac10f08d74ca659f7a12c9ce74fba1120a61950c5011b843fb25e"
)
processorpath=""
for entry in "${jars[@]}"; do
  path=${entry% *}; sha=${entry#* }; jar=$LIB/$(basename "$path")
  if [ ! -f "$jar" ] || [ "$(sha256sum "$jar" | cut -d' ' -f1)" != "$sha" ]; then
    curl --retry 3 --silent --show-error -fL -o "$jar.part" "https://repo1.maven.org/maven2/$path" \
      && mv "$jar.part" "$jar" || { echo "varka_errorprone: could not fetch $path" >&2; exit 1; }
    if [ "$(sha256sum "$jar" | cut -d' ' -f1)" != "$sha" ]; then
      rm -f "$jar"; echo "varka_errorprone: $path does not match its pinned sha256" >&2; exit 1
    fi
  fi
  processorpath+="$jar:"
done

# The sources: Varka's Java in the two modules, and nothing else of Spark's.
codegen=sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen
execution=sql/core/src/main/java/org/apache/spark/sql/execution
ls "$codegen"/*Varka*.java "$codegen"/DeclineSink.java "$codegen"/varka/*.java \
  "$execution"/Varka*.java "$execution"/varka/*.java > "$OUT/sources" || exit 1

# Every suppression says why: a comment on the annotation's line or the line above it.
while IFS= read -r src; do
  awk -v f="$src" '/@SuppressWarnings/ && $0 !~ /\/\/|\/\*/ && prev !~ /\/\/|\*\// {
      print f ":" FNR ": a @SuppressWarnings without a reason" } { prev = $0 }' "$src"
done < "$OUT/sources" > "$OUT/suppressions"
if [ -s "$OUT/suppressions" ]; then
  cat "$OUT/suppressions" >&2; fail "every @SuppressWarnings says why"
fi

# The pending list only shrinks: no line the base's list does not have.
if [ -z "$base" ]; then
  base=$(git merge-base HEAD origin/master 2>/dev/null || echo HEAD)
fi
if git cat-file -e "$base:dev/varka_nullaway_pending.txt" 2>/dev/null; then
  added=$(comm -13 <(git show "$base:dev/varka_nullaway_pending.txt" | grep -v '^#' | sort -u) \
    <(grep -v '^#' "$PENDING" | sort -u))
  if [ -n "$added" ]; then
    echo "$added" | sed 's/^/  /' >&2
    fail "dev/varka_nullaway_pending.txt only shrinks; these are new since $base"
  fi
fi
unannotated=$(grep -v -e '^#' -e '^$' "$PENDING" | paste -sd, -)

# The classpath: the one given, or sql's, which holds catalyst's classes too.
if [ -z "$classpath_file" ]; then
  classpath_file=$OUT/classpath
  build/sbt -batch "sql/compile" "export sql/Compile/fullClasspath" 2>&1 \
    | sed 's/\x1b\[[0-9;]*[A-Za-z]//g' | grep -E '^/[^ ]+:/' | tail -1 > "$classpath_file"
fi
grep -q scala-library "$classpath_file" || { echo "varka_errorprone: no classpath" >&2; exit 1; }

opens=()
for p in api file main model parser processing tree util code comp; do
  opens+=("-J--add-exports=jdk.compiler/com.sun.tools.javac.$p=ALL-UNNAMED")
done
opens+=("-J--add-opens=jdk.compiler/com.sun.tools.javac.code=ALL-UNNAMED"
  "-J--add-opens=jdk.compiler/com.sun.tools.javac.comp=ALL-UNNAMED")
# Each check turned off says why:
#   ImmutableEnumChecker - Lane's fields are ClassDesc and MethodTypeDesc, immutable JDK values
#     the checker does not know to be immutable.
checks=(
  "-Xep:ImmutableEnumChecker:OFF"
  "-Xep:NullAway:ERROR"
  "-XepOpt:NullAway:AnnotatedPackages=org.apache.spark.sql.catalyst.expressions.codegen,org.apache.spark.sql.execution"
  "-XepOpt:NullAway:UnannotatedClasses=$unannotated"
)
rm -rf "$OUT/classes"; mkdir -p "$OUT/classes"
start=$SECONDS
javac "${opens[@]}" -XDcompilePolicy=simple --should-stop=ifError=FLOW \
  -processorpath "$processorpath" "-Xplugin:ErrorProne ${checks[*]}" \
  --add-modules jdk.incubator.vector -proc:none -Xlint:-options,-incubating -Werror \
  -Xmaxwarns 100000 -Xmaxerrs 100000 \
  -d "$OUT/classes" -cp "$(cat "$classpath_file")" @"$OUT/sources" > "$OUT/javac.log" 2>&1 \
  || fail "Error Prone or NullAway found something; $OUT/javac.log"
grep -E '(warning|error): \[' "$OUT/javac.log" | head -40 >&2
echo "varka_errorprone: $(wc -l < "$OUT/sources") files, $((SECONDS - start)) s," \
  "$(grep -c -v -e '^#' -e '^$' "$PENDING") classes pending NullAway"
exit $failed
