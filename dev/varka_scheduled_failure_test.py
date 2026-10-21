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
# Real annotation messages, read with `gh api repos/<repo>/check-runs/<job id>/annotations`.
# A newer run of the same concurrency group replaced the run: MaxGekk/spark, run 39552258555,
# job 119064574793 (and 118865240720, 118866149851 in run 39491136589).
CONCURRENCY_MESSAGE = (
    "Canceling since a higher priority waiting request for build-test-Build-refs/heads/varka-409 "
    "exists"
)
# The same job's other failure-level annotation, and a compile error noise that a cancelled
# compile leaves behind (run 39491136589, job 118866149851, which also has the concurrency text).
OPERATION_CANCELED = "The operation was canceled."
CANCELLED_COMPILE_NOISE = (
    "non-static class StatefulProcessor.implicits$ cannot be referenced from a static context"
)
# A person cancelled the run: vecbricks/varka, run 37290363433, jobs 111699085537, 111699085607,
# 111699085621.
USER_MESSAGE = "The run was canceled by @MaxGekk."
# No timed-out or lost-runner job was found in vecbricks/varka or MaxGekk/spark (60 failed and
# cancelled runs searched on 21 October 2026), so the two texts above are GitHub's documented
# wording, not an observed one.
RUN = "https://r/9"
DATE = "2026-10-20"
TITLE = f"Scheduled run failed: Varka fuzz (first red {DATE})"


class FakeGitHub:
    """A subprocess.run stand-in for gh: scripted failures plus a small issue store.

    fail maps a command prefix such as "issue create" to the number of times it fails first;
    lost_reply makes the first "issue create" / "issue comment" fail after it took effect.
    """

    def __init__(
        self,
        labels=(),
        fail=None,
        lost_reply=(),
        annotations=None,
        issues=(),
        race_label=False,
        jobs=(),
    ):
        self.jobs = list(jobs)
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
        elif "actions/runs/" in text:
            out = "".join(json.dumps(j) + "\n" for j in self.jobs)
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
    14: [CONCURRENCY_MESSAGE, OPERATION_CANCELED],
    17: [CONCURRENCY_MESSAGE, OPERATION_CANCELED, CANCELLED_COMPILE_NOISE],
    18: [OPERATION_CANCELED],
    19: ["Canceled by someone else entirely"],
    15: [USER_MESSAGE, OPERATION_CANCELED],
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
        kind, title, body, text = vsf.decide(
            "Varka fuzz", "https://r/1", "abc", [FAILURE], [], DATE
        )
        self.assertEqual((kind, title), ("create", TITLE))
        for want in ("https://r/1", "abc", vsf.marker("Varka fuzz"), "Failed jobs (1)"):
            self.assertIn(want, body)
        self.assertIn("`Run / Build 'x' modules`: failure, u1", body)
        self.assertIn("https://r/1", text)
        self.assertNotIn("This issue is", text)

    def test_belongs_to_by_marker_or_title_not_by_prefix(self):
        own = {"number": 7, "title": "x", "body": "t\n" + vsf.marker("Varka fuzz")}
        other = {
            "number": 3,
            "title": "Scheduled run failed: Varka fuzz matrix (first red 2026-10-20)",
        }
        renamed = {"number": 9, "title": TITLE, "body": ""}
        self.assertFalse(vsf.belongs_to(other, "Varka fuzz"))
        longer = {
            "number": 5,
            "title": f"Scheduled run failed: Varka fuzz (long) (first red {DATE})",
        }
        self.assertFalse(vsf.belongs_to(longer, "Varka fuzz"))
        self.assertTrue(vsf.belongs_to(longer, "Varka fuzz (long)"))
        self.assertFalse(vsf.belongs_to({"title": TITLE + " tail"}, "Varka fuzz"))
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

    def test_read_cap_reports_the_rest_when_a_read_job_was_not_deliberate(self):
        notes = {i: [CONCURRENCY_MESSAGE] for i in range(1, 31)}
        notes[3] = [LOST_MESSAGE]
        github = FakeGitHub(annotations=notes)
        got = self.reportable([cancelled(i, f"j{i}") for i in range(1, 31)], github)
        self.assertEqual([j["name"] for j in got], ["j3", "10 more cancelled jobs"])
        self.assertEqual(len(github.calls), vsf.MAX_ANNOTATION_READS)

    def test_read_cap_with_every_read_job_deliberate_reports_nothing(self):
        notes = {i: [USER_MESSAGE, OPERATION_CANCELED] for i in range(1, 101)}
        github = FakeGitHub(annotations=notes)
        jobs = [cancelled(i, f"j{i}") for i in range(1, 101)]
        self.assertEqual(self.reportable(jobs, github), [])
        self.assertEqual(len(github.calls), vsf.MAX_ANNOTATION_READS)
        self.assertEqual(vsf.decide("W", "r", "s", [], [], "d", "cancelled"), ("none",))

    def test_real_texts_only_the_observed_phrases_are_deliberate(self):
        got = self.reportable([cancelled(14, "a"), cancelled(15, "b"), cancelled(17, "c")])
        self.assertEqual(got, [], "concurrency, a person, and a cancelled compile's noise")
        for job_id in (18, 19):
            self.assertEqual(len(self.reportable([cancelled(job_id, "x")])), 1, job_id)


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


class Hang(unittest.TestCase):
    def test_a_hung_call_is_a_failed_attempt_with_the_timeout_passed(self):
        seen, pauses = [], []

        def hung(command, **kwargs):
            seen.append(kwargs.get("timeout"))
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])

        with self.assertRaisesRegex(vsf.GhError, "timed out"):
            vsf.gh(["x"], "o/r", runner=hung, sleep=pauses.append)
        self.assertEqual(seen, [vsf.GH_CALL_TIMEOUT_SECONDS] * vsf.GH_ATTEMPTS)
        self.assertEqual(len(pauses), vsf.GH_ATTEMPTS - 1)

    def test_a_hang_then_success_is_retried(self):
        calls = []

        def once(command, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                raise subprocess.TimeoutExpired(command, 1)
            return subprocess.CompletedProcess(command, 0, "ok", "")

        self.assertEqual(vsf.gh(["x"], "o/r", runner=once, sleep=quiet), "ok")


class Needs(unittest.TestCase):
    def test_worst_result_of_the_needed_jobs(self):
        def needs(*results):
            return json.dumps(
                {f"j{i}": {"result": r, "outputs": {}} for i, r in enumerate(results)}
            )

        self.assertEqual(vsf.needs_result_of(needs("success", "failure", "cancelled")), "failure")
        self.assertEqual(vsf.needs_result_of(needs("success", "cancelled")), "cancelled")
        self.assertEqual(vsf.needs_result_of(needs("success", "skipped")), "")
        self.assertEqual(vsf.needs_result_of(""), "")


class Cli(unittest.TestCase):
    ARGS = ["--workflow", "Varka fuzz", "--run-url", RUN, "--sha", "abc", "--date", DATE]

    def run_main(self, argv, github=None, summary=None):
        github = github or FakeGitHub()
        out, env = io.StringIO(), {"GITHUB_STEP_SUMMARY": summary} if summary else {}
        old = dict(os.environ)
        os.environ.update(env)
        try:
            with contextlib.redirect_stdout(out):
                vsf.main(self.ARGS + argv, runner=github, sleep=quiet)
        finally:
            os.environ.clear()
            os.environ.update(old)
        return github, out.getvalue()

    def jobs_file(self, tmp, jobs):
        path = os.path.join(tmp, "jobs.json")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("\n".join(json.dumps(j) for j in jobs))
        return path

    def test_dry_run_from_a_jobs_file_changes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self.jobs_file(tmp, [FAILURE])
            github, out = self.run_main(["--jobs-file", path])
        self.assertEqual(github.calls, [])
        self.assertIn("open an issue titled", out)
        self.assertIn("dry run", out)

    def test_apply_from_the_run_id_opens_one_issue(self):
        github = FakeGitHub(labels=[vsf.LABEL], jobs=[FAILURE])
        github, _ = self.run_main(["--run-id", "5", "--apply"], github)
        self.assertEqual(len(github.issues), 1)
        jobs_call = github.calls[0][0]
        self.assertIn("repos/vecbricks/varka/actions/runs/5/jobs?per_page=100", jobs_call)
        self.assertIn(".jobs[] | {id, name, conclusion, html_url}", jobs_call)

    def test_needs_json_failure_reports_without_a_listed_job(self):
        needs = json.dumps({"fuzz": {"result": "failure"}})
        github = FakeGitHub(labels=[vsf.LABEL], jobs=[])
        github, _ = self.run_main(["--run-id", "5", "--apply", "--needs-json", needs], github)
        self.assertEqual(len(github.issues), 1)
        self.assertIn("showed no failed job", github.issues[0]["body"])
        github = FakeGitHub(labels=[vsf.LABEL], jobs=[])
        github, _ = self.run_main(["--run-id", "5", "--apply"], github)
        self.assertEqual(github.issues, [])

    def test_a_missing_option_is_a_usage_error(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
            vsf.main(["--workflow", "W"])
        self.assertEqual(caught.exception.code, 2)
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
            vsf.main(self.ARGS)
        self.assertEqual(caught.exception.code, 2, "neither --run-id nor --jobs-file")

    def test_gh_failure_writes_the_summary_and_exits_non_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = os.path.join(tmp, "summary.md")
            github = FakeGitHub(fail={"api": 9})
            with self.assertRaises(SystemExit) as caught:
                self.run_main(["--run-id", "5", "--apply"], github, summary)
            self.assertIn("Could not report", str(caught.exception.code))
            with open(summary, encoding="utf-8") as handle:
                text = handle.read()
        self.assertIn(RUN, text)
        self.assertIn("needs result unknown", text)

    def test_malformed_needs_json_is_a_summary_entry_too(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = os.path.join(tmp, "summary.md")
            with self.assertRaises(SystemExit):
                self.run_main(
                    [
                        "--run-id",
                        "5",
                        "--needs-json",
                        "{",
                    ],
                    None,
                    summary,
                )
            with open(summary, encoding="utf-8") as handle:
                self.assertIn("Could not report", handle.read())


if __name__ == "__main__":
    unittest.main()
