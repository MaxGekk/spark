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
Finds the pull request Build's archive of the test classes for the option matrix's build job
(VARKA-507), so the matrix need not compile them again.

A push of Varka files alone runs the Build's varka-scoped jobs, and a job of their own beside
them ("Varka option matrix classes", VARKA-519) archives sql's test classes, whose classpath holds
catalyst's too, as `varka-option-matrix-build` (dev/varka_matrix_archive.sh) right after building
them. This prints `run=<id>` of the Build
run of the commit that has the archive, waiting while a run of that commit may still upload
one, or `run=` when none will: no Build of the commit, a Build whose changes ran Spark's
module matrix instead, or a wait past --wait minutes. The matrix compiles for itself then.

The same Build's varka-scoped sql jobs record, as they run the suites at the defaults, which
configurations move each test (VarkaTestReach), one `varka-matrix-reach-<job>` artifact a job.
`reach RUN` prints `reach=1` when every such job of the run has succeeded with its artifact, so
the matrix may skip what the record allows, and `reach=` otherwise: a configuration then runs
every test.

  dev/varka_matrix_reuse.py REPOSITORY SHA [--wait MINUTES]    # in a step, with GH_TOKEN set
  dev/varka_matrix_reuse.py reach REPOSITORY RUN
  dev/varka_matrix_reuse.py --selftest
"""

import argparse
import json
import subprocess
import sys
import time
import unittest

ARTIFACT = "varka-option-matrix-build"
WORKFLOW = ".github/workflows/build_main.yml"
JOB = "Varka option matrix classes"
UPLOAD = "Upload the test classes for the option matrix"
# The scoped jobs that record: the sql ones (build_and_test.yml says why not the catalyst ones).
SCOPED = "Varka suites: sql "
REACH = "varka-matrix-reach-"
PRECONDITION = "Check changes"
# How long a Build gets to start the job that builds the classes: a run not yet created, a
# precondition still deciding or a job still queued behind a full fork. Only a job that has
# started compiling earns the whole wait, since a queued one may hold the matrix past the time it
# takes to compile.
START_MINUTES = 5.0


def gh(path):
    out = subprocess.run(["gh", "api", path], check=True, capture_output=True, text=True)
    return json.loads(out.stdout)


def verdict(run, artifacts, jobs):
    """What one Build run says: `ready` when it holds the archive, `started` while its sql job
    runs towards the upload, `pending` while that job is not running yet, `none` when the run
    will not upload one."""
    if any(a["name"] == ARTIFACT and not a.get("expired") for a in artifacts):
        return "ready"
    if run["status"] == "completed":
        return "none"
    names = {j["name"].split(" / ")[-1]: j for j in jobs}
    job = names.get(JOB)
    if job is None:
        # The scoped jobs are created once the precondition decides them.
        check = names.get(PRECONDITION)
        return "none" if check and check["status"] == "completed" else "pending"
    upload = next((st for st in job.get("steps", []) if st["name"] == UPLOAD), None)
    if job["status"] == "completed" or (upload and upload["status"] == "completed"):
        return "none"
    return "started" if job["status"] == "in_progress" else "pending"


def find(repository, sha, wait_minutes, sleep=time.sleep, now=time.monotonic):
    start = now()
    deadline = start + wait_minutes * 60

    def waited():
        return f", after {now() - start:.0f} s"

    while True:
        runs = [
            r
            for r in gh(f"repos/{repository}/actions/runs?head_sha={sha}&per_page=50")[
                "workflow_runs"
            ]
            if r["path"] == WORKFLOW
        ]
        states = {}
        for r in sorted(runs, key=lambda r: r["id"], reverse=True):
            artifacts = gh(f"repos/{repository}/actions/runs/{r['id']}/artifacts")["artifacts"]
            jobs = gh(f"repos/{repository}/actions/runs/{r['id']}/jobs?per_page=100")["jobs"]
            states[r["id"]] = verdict(r, artifacts, jobs)
            if states[r["id"]] == "ready":
                return r["id"], f"Build run {r['id']} has the test classes{waited()}"
        if runs and all(s == "none" for s in states.values()):
            return None, f"no Build of this commit archives the test classes{waited()}"
        if "started" not in states.values() and now() >= start + START_MINUTES * 60:
            what = "has started its job" if runs else "has started"
            return None, f"no Build of this commit {what}{waited()}"
        if now() >= deadline:
            return None, f"no archive within {wait_minutes:g} minutes"
        sleep(30)


def reach_complete(jobs, artifacts):
    """Whether every varka-scoped sql job succeeded and each left a reach artifact."""
    scoped = [j for j in jobs if j["name"].split(" / ")[-1].startswith(SCOPED)]
    names = {a["name"] for a in artifacts if not a.get("expired")}
    return (
        bool(scoped)
        and all(j.get("conclusion") == "success" for j in scoped)
        and (len([n for n in names if n.startswith(REACH)]) == len(scoped))
    )


class SelfTest(unittest.TestCase):
    RUN = {"status": "in_progress"}

    def test_an_archive_is_ready_whatever_the_jobs_say(self):
        self.assertEqual(verdict(self.RUN, [{"name": ARTIFACT}], []), "ready")

    def test_an_expired_archive_is_not(self):
        done = {"status": "completed"}
        self.assertEqual(verdict(done, [{"name": ARTIFACT, "expired": True}], []), "none")

    def test_waits_for_the_precondition_then_for_the_job(self):
        check = {"name": "Run / Check changes", "status": "in_progress"}
        self.assertEqual(verdict(self.RUN, [], [check]), "pending")
        check = dict(check, status="completed")
        self.assertEqual(verdict(self.RUN, [], [check]), "none")
        job = {"name": "Run / " + JOB, "status": "queued"}
        self.assertEqual(verdict(self.RUN, [], [check, job]), "pending")
        job = dict(job, status="in_progress")
        self.assertEqual(verdict(self.RUN, [], [check, job]), "started")
        self.assertEqual(verdict(self.RUN, [], [check, dict(job, status="completed")]), "none")
        # Past its upload step without an archive, the job will not make one.
        uploaded = dict(job, steps=[{"name": UPLOAD, "status": "completed"}])
        self.assertEqual(verdict(self.RUN, [], [check, uploaded]), "none")

    def test_reach_is_complete_only_with_every_scoped_job_and_its_artifact(self):
        jobs = [
            {"name": "Run / Varka suites: sql 1 of 3", "conclusion": "success"},
            {"name": "Run / Varka suites: sql 2 of 3", "conclusion": "success"},
            {"name": "Run / Varka docs checks", "conclusion": "failure"},
        ]
        both = [{"name": REACH + "sql-1of3"}, {"name": REACH + "sql-2of3"}]
        self.assertTrue(reach_complete(jobs, both))
        self.assertFalse(reach_complete(jobs, both[:1]))
        failed = [dict(jobs[0], conclusion="failure")] + jobs[1:]
        self.assertFalse(reach_complete(failed, both))
        self.assertFalse(reach_complete([jobs[2]], []), "a Build with no scoped job")
        catalyst = {"name": "Run / Varka suites: catalyst 1 of 3", "conclusion": "success"}
        self.assertTrue(reach_complete(jobs + [catalyst], both), "catalyst jobs record nothing")

    def fake_find(self, runs, jobs, wait):
        """`find` over a repository whose Build runs and jobs never change; the seconds slept."""
        clock = [0.0]
        global gh
        saved = gh

        def api(path):
            if "runs?" in path:
                return {"workflow_runs": runs}
            return {"artifacts": [], "jobs": jobs}

        gh = api
        try:
            got = find(
                "o/r",
                "s",
                wait,
                sleep=lambda s: clock.__setitem__(0, clock[0] + s),
                now=lambda: clock[0],
            )
        finally:
            gh = saved
        return got[0], clock[0]

    def test_a_commit_with_no_build_gets_the_short_wait(self):
        self.assertEqual(self.fake_find([], [], 25), (None, START_MINUTES * 60))

    def test_a_queued_sql_job_gets_the_short_wait(self):
        run = {"id": 1, "path": WORKFLOW, "status": "in_progress"}
        job = {"name": "Run / " + JOB, "status": "queued", "steps": []}
        self.assertEqual(self.fake_find([run], [job], 25), (None, START_MINUTES * 60))

    def test_a_started_sql_job_is_waited_for_until_the_deadline(self):
        run = {"id": 1, "path": WORKFLOW, "status": "in_progress"}
        job = {"name": "Run / " + JOB, "status": "in_progress", "steps": []}
        self.assertEqual(self.fake_find([run], [job], 25), (None, 1500.0))


def main(argv):
    if argv[:1] == ["reach"]:
        repository, run = argv[1], argv[2]
        try:
            jobs = gh(f"repos/{repository}/actions/runs/{run}/jobs?per_page=100")["jobs"]
            artifacts = gh(f"repos/{repository}/actions/runs/{run}/artifacts?per_page=100")[
                "artifacts"
            ]
            done = reach_complete(jobs, artifacts)
        except (subprocess.CalledProcessError, KeyError, ValueError) as e:
            print(f"no reach: {e}", file=sys.stderr)
            done = False
        print(f"reach={'1' if done else ''}")
        return 0
    if argv[:1] == ["--selftest"]:
        result = unittest.main(argv=[sys.argv[0]], exit=False, verbosity=0).result
        ok = result.wasSuccessful()
        print("varka_matrix_reuse selftest: " + ("ok" if ok else "FAILED"))
        return 0 if ok else 1
    parser = argparse.ArgumentParser()
    parser.add_argument("repository")
    parser.add_argument("sha")
    parser.add_argument("--wait", type=float, default=25.0)
    args = parser.parse_args(argv)
    run, why = find(args.repository, args.sha, args.wait)
    print(why, file=sys.stderr)
    print(f"run={run or ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
