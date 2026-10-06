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
# The option matrix in a pull request's CI (VARKA-248.md 3.2.8): one module's Varka suites under
# one emit option configuration, beside the defaults.
#
#   dev/varka_matrix_ci.sh catalyst|sql
#
# dev/varka_matrix_pick.py picks the configuration from the diff against VARKA_DIFF_BASE and the
# branch name. When it fails, the same configuration and module run on VARKA_DIFF_BASE: if every
# failure line of the branch's report is also in the base's, the failures were there before this
# change, and the step passes with a warning naming them; otherwise it fails. Writes its verdict
# to GITHUB_STEP_SUMMARY when that is set.

set -uo pipefail

module=${1:?usage: dev/varka_matrix_ci.sh catalyst|sql}
cd "$(dirname "$0")/.."
OUT=target/varka-matrix
SUMMARY=${GITHUB_STEP_SUMMARY:-/dev/null}
branch=${GITHUB_HEAD_REF:-${GITHUB_REF_NAME:-$(git rev-parse --abbrev-ref HEAD)}}
base=${VARKA_DIFF_BASE:-}

python3 dev/varka_matrix_pick.py --self-test || exit 1
# The job's suites step (dev/varka_scoped_suites.sh) has run the defaults through the runner in
# target/varka-scoped: its build is reused, and its fused batches are what the declining check
# compares with, so the configuration runs alone. Without that run, the defaults run here too.
scoped=target/varka-scoped
reuse=()
# As the suites step splits them (dev/varka_scoped_suites.sh).
split=2
[ "$module" = sql ] && split=1
if [ -d "$scoped/runs/defaults" ] && [ -s "$scoped/classpath" ]; then
  reuse=(--skip-build --build-dir "$scoped" --defaults-from "$scoped" --split "$split")
  dev/varka_matrix.sh --module "$module" "${reuse[@]:0:3}" --list > /dev/null || exit 1
else
  dev/varka_matrix.sh --module "$module" --sbt-arg -Phive --list > /dev/null || exit 1
  reuse=(--skip-build)
fi
mapfile -t picked < <(python3 dev/varka_matrix_pick.py "$OUT/configurations" "$branch" "$base")
config=${picked[0]}
echo "== $module under $config: ${picked[1]}" | tee -a "$SUMMARY"

dev/varka_matrix.sh --module "$module" "${reuse[@]}" --config "$config" -j 2 \
  > "$OUT/report.txt" 2>&1
status=$?
cat "$OUT/report.txt"
if [ "$status" = 0 ]; then
  echo "passed" >> "$SUMMARY"
  exit 0
fi

# The lines that name a failure under the picked configuration or the defaults.
failures() {
  grep -E "^  (defaults|${config//./\\.}): " "$1" | sed 's/ ([0-9]* milliseconds)//' | sort -u
}
failures "$OUT/report.txt" > "$OUT/failures-branch.txt"

if [ -z "$base" ]; then
  echo "failed, and there is no merge base to compare with" | tee -a "$SUMMARY"
  exit 1
fi
echo "== the same on the merge base $base"
worktree=$(mktemp -d)/base
git worktree add --detach "$worktree" "$base" > /dev/null 2>&1 || {
  echo "failed, and the merge base $base could not be checked out" | tee -a "$SUMMARY"; exit 1; }
if [ ! -x "$worktree/dev/varka_matrix.sh" ]; then
  echo "failed; the merge base predates the option matrix, so nothing to compare" |
    tee -a "$SUMMARY"
  exit 1
fi
(cd "$worktree" &&
  dev/varka_matrix.sh --module "$module" --sbt-arg -Phive --config "$config" -j 2) \
  > "$OUT/report-base.txt" 2>&1
cat "$OUT/report-base.txt"
failures "$OUT/report-base.txt" > "$OUT/failures-base.txt"

new=$(comm -23 "$OUT/failures-branch.txt" "$OUT/failures-base.txt")
if [ -z "$new" ]; then
  {
    echo "::warning::$config fails on the merge base too; the failures predate this change:"
    cat "$OUT/failures-branch.txt"
  } | tee -a "$SUMMARY"
  exit 0
fi
{
  echo "failed under $config, and these failures are not on the merge base:"
  echo "$new"
} | tee -a "$SUMMARY"
exit 1
