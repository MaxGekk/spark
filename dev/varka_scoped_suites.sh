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
# One module's Varka suites under the defaults, as CI's varka-scoped job runs them
# (VARKA-287.md 3.1):
#
#   dev/varka_scoped_suites.sh catalyst
#   dev/varka_scoped_suites.sh sql          # the Varka suites and ArrowCachedBatchSerializerSuite
#
# The step lives here rather than in build_and_test.yml so that changing how the suites run is a
# change to a Varka script, whose pull request runs the varka-scoped jobs it changes; a change to
# the workflow runs Spark's module matrix instead (dev/varka_scope.py).
#
# The suites run through dev/varka_matrix.sh over two JVMs, the runner's four cores' worth, in
# target/varka-scoped, which the job caches so the next run balances its JVMs by this run's JUnit
# times. The run records each test's fused batches too, which the option matrix's step
# (dev/varka_matrix_ci.sh) takes instead of running the defaults again.

set -euo pipefail

cd "$(dirname "$0")/.."
module=${1:?usage: dev/varka_scoped_suites.sh catalyst|sql}
extra=()
case "$module" in
  catalyst) ;;
  sql) extra=(--extra-suite
         sql:org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializerSuite) ;;
  *) echo "unknown module: $module" >&2; exit 2 ;;
esac
dev/varka_matrix.sh --defaults --module "$module" --split 2 -j 2 --sbt-arg -Phive \
  --out target/varka-scoped "${extra[@]}"
