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
"""Tests of dev/varka_scheduled_failure.py: python3 dev/varka_scheduled_failure_test.py"""

import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import varka_scheduled_failure as vsf

TIMEOUT_MESSAGE = (
    "The job running on runner GitHub Actions 12 has exceeded the maximum execution time of "
    "90 minutes."
)
LOST_MESSAGE = "The runner has received a shutdown signal. The operation was canceled."
CONCURRENCY_MESSAGE = "Canceling since a higher priority waiting request for 'g' exists"
RUN = "https://r/9"
TITLE = "Scheduled run failed: Varka fuzz (first red d)"


class FakeGitHub:
    """A subprocess.run stand-in for gh: scripted failures plus a small issue store.

    fail maps a command prefix such as "issue create" to the number of times it fails first;
    lost_reply makes the first "issue create" / "issue comment" fail after it took effect.
    """

    def __init__(
        self, labels=(), fail=None, lost_reply=(), annotations=None, issues=(), race_label=False
    ):
        self.race_label = race_label
        self.labels, self.fail, self.lost = set(labels), dict(fail or {}), set(lost_reply)
        self.annotations, self.issues, self.comments = annotations or {}, list(issues), []
        self.calls = []

    def __call__(self, command, input=None, **_):
        self.calls.append((command, input))
        key = " ".join(c for c in command[1:3] if not c.startswith("-"))
        text = " ".join(command)
        out = ""
        if self.fail.get(key, 0) > 0:
            self.fail[key] -= 1
            return subprocess.CompletedProcess(command, 1, "", "502")
        if key == "label list":
            out = json.dumps([{"name": n} for n in sorted(self.labels)])
        elif key == "label create":
            self.labels.add(vsf.LABEL)
            if self.race_label:  # another report created it first: this create is refused
                return subprocess.CompletedProcess(command, 1, "", "already exists")
        elif key == "issue list":
            wanted = "--label" in command
            out = json.dumps(
                [i for i in self.issues if not wanted or vsf.LABEL in i.get("labels", [vsf.LABEL])]
            )
        elif key == "issue create":
            self.issues.append({"number": 100 + len(self.issues), "title": command[4],
                                "body": input,
                                "labels": [vsf.LABEL] if "--label" in command else []})  # fmt: skip
        elif key == "issue comment":
            self.comments.append((command[3], input))
        elif key == "issue view":
            comments = [{"body": b} for n, b in self.comments if n == command[3]]
            out = json.dumps({"comments": comments})
        elif "check-runs/" in text:
            out = "".join(json.dumps({"message": m}) + "\n" for m in self.annotations[
                int(text.split("check-runs/")[1].split("/")[0])])  # fmt: skip
        if key in ("issue create", "issue comment") and key in self.lost:
            self.lost.discard(key)
            return subprocess.CompletedProcess(command, 1, "", "reply lost")
        return subprocess.CompletedProcess(command, 0, out, "")


def cancelled(job_id, name):
    return {"id": job_id, "name": name, "conclusion": "cancelled", "html_url": "u-" + name}


FAILURE = {"id": 2, "name": "Run / Build `x`\nmodules", "conclusion": "failure", "html_url": "u1"}
NOTES = {
    10: [TIMEOUT_MESSAGE.upper()],
    11: [LOST_MESSAGE],
    12: [],
    13: ["x", TIMEOUT_MESSAGE],
    14: [CONCURRENCY_MESSAGE],
    15: ["The run was Canceled by someone"],
    16: [CONCURRENCY_MESSAGE, TIMEOUT_MESSAGE],
}


def setUpModule():
    # The code under test prints what it decides; keep that out of the test report.
    stack = contextlib.ExitStack()
    stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
    unittest.addModuleCleanup(stack.close)


def quiet(_):
    return None


def reader(github):
    return lambda job: vsf.annotations_of("o/r", job, runner=github, sleep=quiet)


def verbs(github):
    return [" ".join(c[1:3]) for c, _ in github.calls]


def apply(decision, github, workflow="Varka fuzz"):
    vsf.act(decision, "o/r", True, workflow, RUN, runner=github, sleep=quiet)


def create(github, extra=""):
    body = f"Run: {RUN}\n" + vsf.marker("Varka fuzz") + extra
    apply(("create", TITLE, body, f"Run: {RUN}\n"), github)


class Decision(unittest.TestCase):
    def test_failed_and_timed_out_count_and_others_do_not(self):
        jobs = [
            {"name": "ok", "conclusion": "success"},
            FAILURE,
            {"name": "slow", "conclusion": "timed_out"},
            {"name": "n", "conclusion": "neutral"},
            {"name": "s", "conclusion": "skipped"},
        ]
        self.assertEqual([j["name"] for j in vsf.failed_jobs(jobs)], [FAILURE["name"], "slow"])

    def test_create_and_comment_texts(self):
        self.assertEqual(vsf.decide("W", "r", "s", [], [], "d"), ("none",))
        kind, title, body, text = vsf.decide("Varka fuzz", "https://r/1", "abc", [FAILURE], [], "d")
        self.assertEqual((kind, title), ("create", TITLE))
        for want in ("https://r/1", "abc", vsf.marker("Varka fuzz"), "Failed jobs (1)"):
            self.assertIn(want, body)
        self.assertIn("`Run / Build 'x' modules`: failure, u1", body)
        self.assertIn("https://r/1", text)
        self.assertNotIn("This issue is", text)

    def test_belongs_to_by_marker_or_title_not_by_prefix(self):
        own = {"number": 7, "title": "x", "body": "t\n" + vsf.marker("Varka fuzz")}
        other = {"number": 3, "title": "Scheduled run failed: Varka fuzz matrix (first red d)"}
        renamed = {"number": 9, "title": TITLE, "body": ""}
        self.assertFalse(vsf.belongs_to(other, "Varka fuzz"))
        self.assertTrue(vsf.belongs_to(renamed, "Varka fuzz") and vsf.belongs_to(own, "Varka fuzz"))
        dec = vsf.decide("Varka fuzz", "r2", "d", [FAILURE], [other, renamed, own], "d2")
        self.assertEqual(dec[:2], ("comment", 7))
        self.assertEqual(vsf.decide("Varka fuzz", "r", "d", [FAILURE], [other], "d")[0], "create")

    def test_hostile_names_stay_inside_the_marker(self):
        for hostile in ("a --> <script>\n# x", "a --!> b", "a --->b", "<!-- x"):
            mark = vsf.marker(hostile)
            inner = mark[len("<!-- ") : -len(" -->")]
            self.assertTrue("\n" not in mark and "--" not in inner, mark)
        self.assertEqual(vsf.code("a --> b"), "`a --> b`")
        self.assertEqual(vsf.one_line("a\n b"), "a b")

    def test_long_list_is_cut(self):
        many = [{"name": f"j{i}", "conclusion": "failure"} for i in range(vsf.MAX_JOBS_LISTED + 5)]
        self.assertIn("and 5 more", vsf.decide("W", "r", "s", many, [], "d")[2])

    def test_read_jobs_shapes(self):
        nd = '{"name":"a"}\n{"name":"b"}\n'
        self.assertEqual(len(vsf.read_jobs(nd)), 2)
        self.assertEqual(len(vsf.read_jobs('{"jobs":[{"name":"a"}]}')), 1)
        self.assertEqual(vsf.read_jobs(""), [])

    def test_needs_failure_without_a_listed_job_still_reports(self):
        own = {"number": 7, "title": "x", "body": vsf.marker("Varka fuzz")}
        made = vsf.decide("Varka fuzz", "https://r/3", "s3", [], [], "d3", "failure")
        self.assertEqual(made[0], "create")
        self.assertIn("showed no failed job", made[2])
        self.assertNotIn("Failed jobs", made[2])
        said = vsf.decide("Varka fuzz", "https://r/3", "s3", [], [own], "d3", "failure")
        self.assertEqual(said[:2], ("comment", 7))
        self.assertEqual(vsf.decide("W", "r", "s", [], [], "d", "cancelled"), ("none",))


class CancelledJobs(unittest.TestCase):
    def reportable(self, jobs, github=None, **kwargs):
        github = github or FakeGitHub(annotations=NOTES)
        return vsf.reportable_jobs(jobs, reader(github), **kwargs)

    def test_timeout_text_is_reported(self):
        got = self.reportable([cancelled(10, "slow")])
        self.assertEqual((got[0]["name"], got[0]["why"]), ("slow", "timeout"))
        self.assertEqual(self.reportable([cancelled(13, "second")])[0]["why"], "timeout")

    def test_deliberate_cancels_are_not_reported(self):
        self.assertEqual(self.reportable([cancelled(14, "replaced"), cancelled(15, "person")]), [])

    def test_a_timeout_beats_a_deliberate_text(self):
        self.assertEqual(self.reportable([cancelled(16, "both")])[0]["why"], "timeout")

    def test_unrecognised_or_missing_text_fails_safe(self):
        got = self.reportable([cancelled(11, "lost"), cancelled(12, "bare")])
        self.assertEqual([j["why"] for j in got], ["cancelled for an unrecognised reason"] * 2)

    def test_annotation_call_path(self):
        github = FakeGitHub(annotations=NOTES)
        self.reportable([cancelled(10, "slow")], github)
        self.assertIn("check-runs/10/annotations", github.calls[0][0][3])
        self.assertNotIn("--repo", github.calls[0][0])

    def test_unreadable_annotations_are_reported_with_a_note(self):
        github = FakeGitHub(annotations=NOTES, fail={"api": 9})
        got = self.reportable([cancelled(10, "slow")], github)
        self.assertEqual(got[0]["why"], "could not read why it was cancelled")
        self.assertEqual(len(github.calls), vsf.GH_ATTEMPTS)
        self.assertIn("could not read why", vsf.decide("W", "r", "s", got, [], "d")[2])
        self.assertTrue(self.reportable([cancelled(0, "noid")])[0]["why"].startswith("could not"))

    def test_garbled_reply_is_reported_with_a_note(self):
        def garbled(_):
            raise ValueError("not json")

        got = vsf.reportable_jobs([cancelled(10, "slow")], garbled)
        self.assertEqual(got[0]["why"], "could not read why it was cancelled")

    def test_sibling_beside_a_failure_and_a_timeout(self):
        jobs = [cancelled(14, "sibling"), FAILURE, cancelled(10, "slow")]
        got = self.reportable(jobs)
        self.assertEqual([j["name"] for j in got], [FAILURE["name"], "slow"])
        text = vsf.decide("W", "r", "s", got, [], "d")[2]
        self.assertIn("Failed jobs (2)", text)
        self.assertNotIn("sibling", text)

    def test_time_budget_marks_the_rest_unread(self):
        ticks = iter([0, 0, vsf.ANNOTATION_BUDGET_SECONDS + 1, 999, 999])
        github = FakeGitHub(annotations=NOTES)
        jobs = [cancelled(14, "first"), cancelled(10, "late"), cancelled(14, "later")]
        got = self.reportable(jobs, github, clock=lambda: next(ticks))
        self.assertEqual([j["name"] for j in got], ["late", "later"])
        self.assertEqual({j["why"] for j in got}, {"not read, out of time"})
        self.assertEqual(len(github.calls), 1, "only the first job was read")

    def test_read_cap_collapses_the_rest_into_one_line(self):
        github = FakeGitHub(annotations={i: [LOST_MESSAGE] for i in range(1, 31)})
        got = self.reportable([cancelled(i, f"j{i}") for i in range(1, 31)], github)
        self.assertEqual(len(got), vsf.MAX_ANNOTATION_READS + 1)
        self.assertEqual(got[-1]["name"], "10 more cancelled jobs")
        self.assertEqual(len(github.calls), vsf.MAX_ANNOTATION_READS)


class Gh(unittest.TestCase):
    def test_three_tries_with_pauses(self):
        pauses, flaky = [], FakeGitHub(fail={"x": 2})
        self.assertEqual(vsf.gh(["x"], "o/r", runner=flaky, sleep=pauses.append), "")
        self.assertEqual((len(flaky.calls), len(pauses)), (3, 2))
        dead = FakeGitHub(fail={"x": 9})
        with self.assertRaisesRegex(vsf.GhError, "502"):
            vsf.gh(["x"], "o/r", runner=dead, sleep=pauses.append)
        self.assertEqual(len(dead.calls), vsf.GH_ATTEMPTS)

    def test_api_has_no_repo_flag(self):
        github = FakeGitHub()
        vsf.gh(["api", "x"], "o/r", runner=github)
        self.assertNotIn("--repo", github.calls[0][0])

    def test_summary_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "summary.md")
            vsf.write_summary("one", target)
            vsf.write_summary("two", target)
            with open(target, encoding="utf-8") as handle:
                self.assertEqual(handle.read(), "one\ntwo\n")
        vsf.write_summary("ignored", None)


class Writes(unittest.TestCase):
    def test_label_present_is_not_touched(self):
        github = FakeGitHub(labels=[vsf.LABEL])
        create(github)
        self.assertEqual(verbs(github), ["label list", "issue create"])
        self.assertIn("--label", github.calls[1][0])

    def test_label_missing_is_created_once_without_force(self):
        github = FakeGitHub()
        create(github)
        self.assertEqual(verbs(github), ["label list", "label create", "issue create"])
        self.assertNotIn("--force", github.calls[1][0])

    def test_label_already_exists_counts_as_success(self):
        github = FakeGitHub(race_label=True)
        create(github)
        self.assertIn("--label", github.calls[-1][0])

    def test_failing_label_step_opens_an_unlabelled_issue_with_a_note(self):
        github = FakeGitHub(fail={"label list": 9, "label create": 9})
        create(github)
        self.assertNotIn("--label", github.calls[-1][0])
        self.assertIn("add it by hand", github.issues[0]["body"])

    def test_two_red_runs_with_a_failing_label_make_one_issue_and_one_comment(self):
        github = FakeGitHub(fail={"label list": 99, "label create": 99})
        first = vsf.decide("Varka fuzz", "https://r/1", "s", [FAILURE], [], "d")
        apply(first, github)
        found = vsf.open_issues_of("o/r", runner=github, sleep=quiet)
        second = vsf.decide("Varka fuzz", "https://r/2", "s", [FAILURE], found, "d")
        self.assertEqual(second[:2], ("comment", 100))
        apply(second, github)
        self.assertEqual((len(github.issues), len(github.comments)), (1, 1))

    def test_lost_reply_with_a_failed_label_finds_the_unlabelled_issue(self):
        github = FakeGitHub(fail={"label list": 9, "label create": 9}, lost_reply=["issue create"])
        create(github)
        self.assertEqual(len(github.issues), 1)
        self.assertEqual(verbs(github).count("issue create"), 1)

    def test_lost_reply_creates_no_second_issue(self):
        github = FakeGitHub(labels=[vsf.LABEL], lost_reply=["issue create"])
        create(github)
        self.assertEqual(len(github.issues), 1)
        self.assertEqual(verbs(github).count("issue create"), 1)

    def test_a_failure_without_effect_is_retried(self):
        github = FakeGitHub(labels=[vsf.LABEL], fail={"issue create": 1})
        create(github)
        self.assertEqual(len(github.issues), 1)
        self.assertEqual(verbs(github).count("issue create"), 2)

    def test_an_issue_that_appeared_meanwhile_gets_a_comment(self):
        earlier = {"number": 4, "title": "t", "body": vsf.marker("Varka fuzz")}
        github = FakeGitHub(labels=[vsf.LABEL], fail={"issue create": 1}, issues=[earlier])
        create(github)
        self.assertEqual(len(github.issues), 1)
        self.assertEqual(github.comments, [("4", f"Run: {RUN}\n")])

    def test_comment_retries_without_duplicates(self):
        github = FakeGitHub(fail={"issue comment": 1})
        apply(("comment", 7, f"Run: {RUN}\n"), github)
        self.assertEqual((len(github.comments), verbs(github).count("issue comment")), (1, 2))
        github = FakeGitHub(lost_reply=["issue comment"])
        apply(("comment", 7, f"Run: {RUN}\n"), github)
        self.assertEqual((len(github.comments), verbs(github).count("issue comment")), (1, 1))

    def test_comment_fails_after_three_tries(self):
        github = FakeGitHub(fail={"issue comment": 9})
        with self.assertRaises(vsf.GhError):
            apply(("comment", 7, "C"), github)
        self.assertEqual(verbs(github).count("issue comment"), vsf.GH_ATTEMPTS)

    def test_dry_run_changes_nothing(self):
        github = FakeGitHub()
        vsf.act(("create", "T", "B", "C"), "o/r", False, runner=github)
        self.assertEqual(github.calls, [])


if __name__ == "__main__":
    unittest.main()
