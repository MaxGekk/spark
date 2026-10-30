#!/usr/bin/env python3
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
"""
Finds the newest Build run of the repository that saved its compile state, so a Varka job can
start from it and let sbt compile only what differs (VARKA-520).

A Varka-scoped Build's first sql job archives every module's `target/scala-2.13` - the class
directories and Zinc's analysis of what each source produced - as `varka-compile-state` once its
suites have passed. Zinc decides what to recompile by the sources' hashes against that analysis,
so the state need not come from this branch or from master: the newest one any branch saved is
the closest to most pull requests, and whatever differs is compiled again. A run with no usable
state compiles clean, as every run did before.

This prints `run=<id>` of that Build run, or `run=` when no recent run has an unexpired state.

  dev/varka_compile_state.py REPOSITORY      # in a step, with GH_TOKEN set
  dev/varka_compile_state.py --selftest
"""

import json
import subprocess
import sys
import unittest

ARTIFACT = "varka-compile-state"
WORKFLOW = "build_main.yml"
# How many of the newest completed Build runs to look through: a state is kept three days, and
# the fork completes a few dozen Builds on a busy day.
RUNS = 40


def gh(path):
    out = subprocess.run(["gh", "api", path], check=True, capture_output=True, text=True)
    return json.loads(out.stdout)


def has_state(artifacts):
    """Whether a run's artifacts hold an unexpired compile state."""
    return any(a["name"] == ARTIFACT and not a.get("expired") for a in artifacts)


def newest(runs, artifacts_of):
    """The id of the newest run whose artifacts hold a state, or None. `runs` are the API's
    workflow runs; `artifacts_of` gives a run's artifacts by its id."""
    for run in sorted(runs, key=lambda r: r["id"], reverse=True):
        if run.get("status") == "completed" and has_state(artifacts_of(run["id"])):
            return run["id"]
    return None


def main(argv):
    if argv == ["--selftest"]:
        result = unittest.main(argv=["selftest"], exit=False).result
        print("varka_compile_state selftest: " + ("ok" if result.wasSuccessful() else "FAILED"))
        return 0 if result.wasSuccessful() else 1
    if len(argv) != 1:
        print(__doc__, file=sys.stderr)
        return 2
    repository = argv[0]
    runs = gh(
        f"repos/{repository}/actions/workflows/{WORKFLOW}/runs?status=completed&per_page={RUNS}"
    )
    run = newest(
        runs["workflow_runs"],
        lambda i: gh(f"repos/{repository}/actions/runs/{i}/artifacts")["artifacts"],
    )
    print(f"run={run if run is not None else ''}")
    return 0


class SelfTest(unittest.TestCase):
    def test_the_newest_completed_run_with_a_state_wins(self):
        runs = [
            {"id": 3, "status": "in_progress"},
            {"id": 2, "status": "completed"},
            {"id": 1, "status": "completed"},
        ]
        arts = {
            1: [{"name": ARTIFACT}],
            2: [{"name": ARTIFACT}, {"name": "other"}],
            3: [{"name": ARTIFACT}],
        }
        self.assertEqual(newest(runs, arts.get), 2)

    def test_an_expired_state_or_none_gives_none(self):
        runs = [{"id": 2, "status": "completed"}, {"id": 1, "status": "completed"}]
        arts = {1: [{"name": ARTIFACT, "expired": True}], 2: [{"name": "other"}]}
        self.assertIsNone(newest(runs, arts.get))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
