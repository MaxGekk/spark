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

# The IR-storage spike's harness (VARKA-291) on the early-access Valhalla JDK (JEP 401, a preview
# feature, so it never reaches the build): the same sources as run.sh, plus src-ea/, compiled with
# preview enabled. The variant says what the IR's node classes are:
#
#   plain   the sealed records as they are, the control that holds the JDK constant
#   value   the same file with `value` added to every record (arm 5), generated here and never
#           committed
#
#   EA_JAVA_HOME=/path/to/jdk-27-ea sql/varka/spikes/ir-layout/run-ea.sh plain|value [--graphs DIR]
#       [--layouts A,B,C,V16,V63|none]
#
# JVM_OPTS adds JVM options. The value variant needs JVM_OPTS=-XX:-DoEscapeAnalysis on the full
# corpus: with it on, C2 of build 27-jep401ea3+1-1 crashes (VARKA-291 plan, 9.5).

set -euo pipefail

if [ -z "${EA_JAVA_HOME:-}" ] || [ ! -x "$EA_JAVA_HOME/bin/javac" ]; then
  echo "set EA_JAVA_HOME to an early-access Valhalla JDK (JEP 401)" >&2
  exit 2
fi
variant=${1:-}
case "$variant" in
  plain | value) shift ;;
  *) echo "usage: run-ea.sh plain|value [--graphs DIR]" >&2; exit 2 ;;
esac

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../../.." && pwd)
pkg=sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen/varka
test_pkg=sql/catalyst/src/test/java/org/apache/spark/sql/catalyst/expressions/codegen/varka

out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
mkdir -p "$out/src"

if [ "$variant" = value ]; then
  sed -E 's/^(  |public )record /\1value record /' "$root/$pkg/VarkaVectorIR.java" \
    > "$out/src/VarkaVectorIR.java"
else
  cp "$root/$pkg/VarkaVectorIR.java" "$out/src/VarkaVectorIR.java"
fi

shopt -s nullglob
ea_sources=("$here"/src-ea/*.java)

# --source, not --release: --release rejects --add-exports, which the internal-API arms need.
"$EA_JAVA_HOME/bin/javac" --enable-preview --source 27 \
  --add-exports java.base/jdk.internal.value=ALL-UNNAMED \
  --add-exports java.base/jdk.internal.vm.annotation=ALL-UNNAMED \
  -Xlint:-preview -d "$out/classes" "$out/src/VarkaVectorIR.java" \
  "$root/$test_pkg/VarkaIrDescription.java" "$here"/src/*.java "${ea_sources[@]}" 2>&1 \
  | { grep -v '^Note:' || true; }

# Deep chains recurse as deep as they are nested; 16 MB of stack holds a thousand levels with room.
# shellcheck disable=SC2086
"$EA_JAVA_HOME/bin/java" --enable-preview ${JVM_OPTS:-} \
  --add-exports java.base/jdk.internal.value=ALL-UNNAMED \
  --add-exports java.base/jdk.internal.vm.annotation=ALL-UNNAMED \
  -Dir.variant="$variant" -Xss16m -Xmx4g -cp "$out/classes" \
  org.apache.spark.sql.catalyst.expressions.codegen.varka.IrLayoutHarness \
  --graphs "$here/graphs" --layouts A,B,C,D,V16,V63 "$@"
