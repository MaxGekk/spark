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
# The IR-storage spike's harness (VARKA-291), outside the build: compiles the IR's own source, the
# description class from the test tree and the harness with plain javac, then runs it.
#
#   sql/varka/spikes/ir-layout/run.sh                      # the committed graphs
#   sql/varka/spikes/ir-layout/run.sh --graphs DIR         # graphs the exporter wrote
#   JAVA_HOME=/path/to/jdk sql/varka/spikes/ir-layout/run.sh
#
# The exporter writes every family of graphs (the committed ones are the small ones):
#   build/sbt "catalyst/Test/runMain org.apache.spark.sql.catalyst.expressions.codegen.varka.\
#   VarkaIrLayoutExport OUTDIR"

set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../../.." && pwd)
java=${JAVA_HOME:+$JAVA_HOME/bin/}java
javac=${JAVA_HOME:+$JAVA_HOME/bin/}javac
pkg=sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen/varka
test_pkg=sql/catalyst/src/test/java/org/apache/spark/sql/catalyst/expressions/codegen/varka

out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

"$javac" -d "$out" "$root/$pkg/VarkaVectorIR.java" "$root/$test_pkg/VarkaIrDescription.java" \
  "$here"/src/*.java

# Deep chains recurse as deep as they are nested; 16 MB of stack holds a thousand levels with room.
"$java" -Xss16m -Xmx4g -cp "$out" \
  org.apache.spark.sql.catalyst.expressions.codegen.varka.IrLayoutHarness \
  --graphs "$here/graphs" "$@"
