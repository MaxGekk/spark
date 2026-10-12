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
#   - NullAway treats a class as unannotated only while dev/varka_nullaway_pending.txt lists it
#     both here and on origin/master, and checks every other class. The list only shrinks: it may
#     not grow longer than the base's (the merge base with origin/master), and a name the base
#     does not list must replace one it does, as a renamed or moved class does;
#   - every @SuppressWarnings in those sources says why, in a // comment after it on its line or
#     on the line above, and none suppresses NullAway over a whole class;
#   - java and javac are the JDK the javac flags below are written for.
#
#   dev/varka_errorprone.sh                         # export the classpath with sbt, then check
#   dev/varka_errorprone.sh --classpath-file FILE   # a classpath already exported (the gate's)
#   dev/varka_errorprone.sh --base REF              # the ref the pending list is compared with
#
# The jars are pinned by version and sha256 and fetched into build/varka-errorprone/ on first use.

set -uo pipefail
# comm needs the order sort gave it; one collation for both.
export LC_ALL=C

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
# name rather than met as an IllegalAccessError from inside the plugin. Both tools are checked,
# since the java and the javac first on PATH can come from different JDKs.
major=$(java -XshowSettings:properties -version 2>&1 | sed -n 's/^ *java.specification.version = //p')
javac_major=$(javac -version 2>&1 | sed -n 's/^javac \([0-9]*\).*/\1/p')
if [ "$major" != 25 ] || [ "$javac_major" != 25 ]; then
  echo "varka_errorprone: needs JDK 25, found java ${major:-unknown}," \
    "javac ${javac_major:-unknown}" >&2
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
  # A copy in the local Maven repository, which has the same layout, saves the download.
  m2=$HOME/.m2/repository/$path
  if [ ! -f "$jar" ] && [ -f "$m2" ] && [ "$(sha256sum "$m2" | cut -d' ' -f1)" = "$sha" ]; then
    cp "$m2" "$jar"
  fi
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

# Every suppression says why: a // comment after the annotation on its line, or a line comment
# directly above it that has words in it (a rule of dashes is a section header, not a reason).
# The end of a Javadoc block above is not a reason, and a Javadoc line that only mentions the
# annotation is not a suppression. A NullAway suppression on a class would exempt the class
# without its name on the pending list, so it is refused.
while IFS= read -r src; do
  awk -v f="$src" '
    /^[[:space:]]*(\/\*|\*)/ { prev = $0; next }
    nullaway && !/^[[:space:]]*(@|\/\/|$)/ {
      if ($0 ~ /(^|[[:space:]])(class|record|enum|interface)[[:space:]]/)
        print f ":" FNR ": NullAway suppressed over a whole class; list it as pending instead"
      nullaway = 0
    }
    /@(java\.lang\.)?SuppressWarnings\(/ {
      if ($0 !~ /SuppressWarnings\([^)]*\)[^"]*\/\/.*[A-Za-z]/ \
          && prev !~ /^[[:space:]]*\/\/.*[A-Za-z][A-Za-z][A-Za-z]/)
        print f ":" FNR ": a @SuppressWarnings without a reason"
      if ($0 ~ /"NullAway"/) nullaway = 1
    }
    { prev = $0 }' "$src"
done < "$OUT/sources" > "$OUT/suppressions"
if [ -s "$OUT/suppressions" ]; then
  cat "$OUT/suppressions" >&2; fail "every @SuppressWarnings says why, and none covers a class"
fi

# The pending list only shrinks. Against the base: no longer than its list, and every name it does
# not list paired with one it lists that is gone, which lets a pending class be renamed or moved
# but not split into two pending classes.
names() { grep -v -e '^#' -e '^$' | sort -u; }
if [ -z "$base" ]; then
  base=$(git merge-base HEAD origin/master 2>/dev/null) || {
    echo "varka_errorprone: no merge base with origin/master; pass --base REF" >&2; exit 1; }
fi
if git cat-file -e "$base:dev/varka_nullaway_pending.txt" 2>/dev/null; then
  base_names=$(git show "$base:dev/varka_nullaway_pending.txt" | names)
  added=$(comm -13 <(echo "$base_names") <(names < "$PENDING"))
  removed=$(comm -23 <(echo "$base_names") <(names < "$PENDING"))
  if [ -n "$added" ] && [ "$(echo "$added" | wc -l)" -gt "$(echo "$removed" | grep -c .)" ]; then
    echo "$added" | sed 's/^/  /' >&2
    fail "dev/varka_nullaway_pending.txt only shrinks; these are new since $base, with no" \
      "removed name for each"
  fi
else
  echo "varka_errorprone: $base has no pending list; the shrink check is skipped" >&2
fi
# Unannotated: listed here, less what origin/master has taken off its list since the base. Such a
# class is checked here too, so the branch fails now rather than master after the merge.
pending=$(names < "$PENDING")
if [ -n "${base_names:-}" ] \
    && master_names=$(git show origin/master:dev/varka_nullaway_pending.txt 2>/dev/null | names) \
    && [ -n "$master_names" ]; then
  pending=$(comm -23 <(echo "$pending") \
    <(comm -23 <(echo "$base_names") <(echo "$master_names")))
fi
unannotated=$(echo "$pending" | paste -sd, -)

# The classpath: the one given, or sql's, which holds catalyst's classes too.
if [ -z "$classpath_file" ]; then
  classpath_file=$OUT/classpath
  build/sbt -batch "sql/compile" "export sql/Compile/fullClasspath" 2>&1 \
    | sed 's/\x1b\[[0-9;]*[A-Za-z]//g; s/\r//g' | grep -E '^/[^ ]+:/' | tail -1 > "$classpath_file"
fi
grep -q scala-library "$classpath_file" || { echo "varka_errorprone: no classpath" >&2; exit 1; }

opens=()
for p in api file main model parser processing tree util code comp; do
  opens+=("-J--add-exports=jdk.compiler/com.sun.tools.javac.$p=ALL-UNNAMED")
done
opens+=("-J--add-opens=jdk.compiler/com.sun.tools.javac.code=ALL-UNNAMED"
  "-J--add-opens=jdk.compiler/com.sun.tools.javac.comp=ALL-UNNAMED")
# No check is turned off: an intended finding is suppressed at its site, with a reason.
checks=(
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
  -d "$OUT/classes" -cp "$(cat "$classpath_file")" @"$OUT/sources" > "$OUT/javac.log" 2>&1
javac_status=$?
if [ "$javac_status" -ne 0 ]; then
  # Error Prone's and NullAway's findings carry their check's name in brackets; anything else
  # javac printed (a missing symbol, a stale classpath) is shown as it is, not blamed on them.
  if grep -q -E '(warning|error): \[' "$OUT/javac.log"; then
    grep -E '(warning|error): \[' "$OUT/javac.log" | head -40 >&2
  else
    tail -20 "$OUT/javac.log" >&2
  fi
  fail "javac failed; $OUT/javac.log"
fi
echo "varka_errorprone: $(wc -l < "$OUT/sources") files, $((SECONDS - start)) s," \
  "$(echo "$pending" | grep -c .) classes pending NullAway"
exit $failed
