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

# Real annotation messages, read with `gh api repos/<repo>/check-runs/<job id>/annotations`.
# They are why a cancelled job is reported with a note and not classified: the texts differ by
# cause, GitHub does not document them, and reading them costs one call per job.
#  * A newer run of the same concurrency group replaced the run: MaxGekk/spark, run
#    39552258555, job 119064574793.
CONCURRENCY_MESSAGE = (
    "Canceling since a higher priority waiting request for build-test-Build-refs/heads/varka-409 "
    "exists"
)
#  * A person cancelled the run: vecbricks/varka, run 37290363433, jobs 111699085537,
#    111699085607, 111699085621.
USER_MESSAGE = "The run was canceled by @MaxGekk."
# No timed-out job was found in 60 failed and cancelled runs of vecbricks/varka and
# MaxGekk/spark searched on 21 October 2026.
RUN = "https://r/9"
ATTEMPT = RUN + "/attempts/1"
DATE = "2026-10-20"
TITLE = f"Scheduled run failed: Varka fuzz (first red {DATE})"


class FakeGitHub:
    """A subprocess.run stand-in for gh: scripted failures plus a small issue store.

    fail maps a command prefix such as "issue create" to the number of times it fails first;
    lost_reply makes the first "issue create" / "issue comment" fail after it took effect;
    each call advances the fake clock by tick seconds.
    """

    def __init__(self, labels=(), fail=None, lost_reply=(), issues=(), race_label=False,
                 jobs=(), clock=None, tick=0):  # fmt: skip
        self.jobs, self.race_label, self.clock, self.tick = list(jobs), race_label, clock, tick
        self.labels, self.fail, self.lost = set(labels), dict(fail or {}), set(lost_reply)
        self.issues, self.comments, self.calls = list(issues), [], []

    def __call__(self, command, input=None, **kwargs):
        assert kwargs["timeout"] == vsf.GH_CALL_TIMEOUT_SECONDS
        self.calls.append((command, input))
        if self.clock:
            self.clock.now += self.tick
        key = " ".join(c for c in command[1:3] if not c.startswith("-"))
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
            out = json.dumps(self.listed(command))
        elif key == "issue create":
            self.issues.append({"number": 100 + len(self.issues), "title": command[4],
                                "body": input,
                                "labels": [vsf.LABEL] if "--label" in command else []})  # fmt: skip
        elif key == "issue comment":
            self.comments.append((command[3], input))
        elif key == "issue view":
            out = json.dumps({"comments": [{"body": b} for n, b in self.comments
                                           if n == command[3]]})  # fmt: skip
        elif "actions/runs/" in " ".join(command):
            out = "".join(json.dumps(j) + "\n" for j in self.jobs)
        if key in ("issue create", "issue comment") and key in self.lost:
            self.lost.discard(key)
            return subprocess.CompletedProcess(command, 1, "", "reply lost")
        return subprocess.CompletedProcess(command, 0, out, "")

    def listed(self, command):
        if "--label" in command:
            return [i for i in self.issues if vsf.LABEL in i["labels"]]
        if "--search" in command:  # finds by body text, whatever the labels
            return [i for i in self.issues if vsf.MARKER_WORD in (i["body"] or "")]
        return self.issues[-20:]


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def issue(number, body="", labels=(vsf.LABEL,), title="t"):
    return {"number": number, "title": title, "body": body, "labels": list(labels)}


def job(conclusion, name="j", **more):
    return {"id": 1, "name": name, "conclusion": conclusion, "html_url": f"u-{name}", **more}


FAILURE = {"id": 2, "name": "Run / Build `x`\nmodules", "conclusion": "failure", "html_url": "u1"}


def setUpModule():
    # The code under test prints what it decides; keep that out of the test report.
    stack = contextlib.ExitStack()
    stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
    unittest.addModuleCleanup(stack.close)


def quiet(_):
    return None


def make_gh(github, clock=None):
    return vsf.Gh("o/r", runner=github, sleep=quiet, **({"clock": clock} if clock else {}))


def verbs(github):
    return [" ".join(c[1:3]) for c, _ in github.calls]


def create(github, extra=""):
    body = f"Run: {ATTEMPT}\n" + vsf.marker("Varka fuzz") + extra
    vsf.act(
        ("create", TITLE, body, f"Run: {ATTEMPT}\n"), make_gh(github), True, "Varka fuzz", ATTEMPT
    )


class Decision(unittest.TestCase):
    def test_failed_timed_out_and_cancelled_count_and_others_do_not(self):
        jobs = [job(c, c) for c in ("success", "failure", "timed_out", "cancelled", "neutral",
                                    "skipped", None)]  # fmt: skip
        body = vsf.decide("W", "r", "s", jobs, [], "d")[2]
        self.assertIn("did not pass (3)", body)
        for name in ("failure", "timed_out", "cancelled"):
            self.assertIn(f"`{name}`:", body)
        for name in ("success", "neutral", "skipped", "None"):
            self.assertNotIn(f"`{name}`:", body)

    def test_create_and_comment_texts(self):
        self.assertEqual(vsf.decide("W", "r", "s", [], [], "d"), ("none",))
        kind, title, body, text = vsf.decide(
            "Varka fuzz", ATTEMPT, "abc", [FAILURE, job("cancelled", "c")], [], DATE
        )
        self.assertEqual((kind, title), ("create", TITLE))
        for want in (ATTEMPT, "abc", vsf.marker("Varka fuzz"), "did not pass (2)"):
            self.assertIn(want, body)
        self.assertIn("`Run / Build 'x' modules`: failure, u1", body)
        self.assertIn(f"`c`: {vsf.CANCELLED_NOTE}, u-c", body)
        self.assertIn(ATTEMPT, text)
        self.assertNotIn("This issue is", text)

    def test_belongs_to_by_marker_or_title_not_by_prefix(self):
        own = {"number": 7, "title": "x", "body": "t\n" + vsf.marker("Varka fuzz")}
        other = {
            "number": 3,
            "title": f"Scheduled run failed: Varka fuzz matrix (first red {DATE})",
        }
        renamed = {"number": 9, "title": TITLE, "body": ""}
        longer = {
            "number": 5,
            "title": f"Scheduled run failed: Varka fuzz (long) (first red {DATE})",
        }
        self.assertFalse(vsf.belongs_to(other, "Varka fuzz"))
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

    def test_long_list_is_cut(self):
        many = [job("failure", f"j{i}") for i in range(vsf.MAX_JOBS_LISTED + 5)]
        body = vsf.decide("W", "r", "s", many, [], "d")[2]
        self.assertIn("and 5 more", body)
        self.assertNotIn("`j40`", body)

    def test_read_jobs_shapes(self):
        self.assertEqual(len(vsf.read_jobs('{"name":"a"}\n{"name":"b"}\n')), 2)
        self.assertEqual(len(vsf.read_jobs('{"jobs":[{"name":"a"}]}')), 1)
        self.assertEqual(vsf.read_jobs(""), [])

    def test_a_failed_need_without_a_listed_job_still_reports(self):
        own = {"number": 7, "title": "x", "body": vsf.marker("Varka fuzz")}
        for result in ("failure", "cancelled"):
            made = vsf.decide("Varka fuzz", ATTEMPT, "s3", [], [], "d3", result)
            self.assertEqual(made[0], "create")
            self.assertIn("showed no failed or cancelled job", made[2])
            self.assertIn(ATTEMPT, made[2])
            self.assertNotIn("Jobs that did not pass", made[2])
            said = vsf.decide("Varka fuzz", ATTEMPT, "s3", [], [own], "d3", result)
            self.assertEqual(said[:2], ("comment", 7))
        self.assertEqual(vsf.decide("W", "r", "s", [], [], "d", "other"), ("none",))

    def test_worst_result_of_the_needed_jobs(self):
        def needs(*results):
            return json.dumps({f"j{i}": {"result": r} for i, r in enumerate(results)})

        self.assertEqual(vsf.needs_result_of(needs("success", "failure", "cancelled")), "failure")
        self.assertEqual(vsf.needs_result_of(needs("success", "cancelled")), "cancelled")
        self.assertEqual(vsf.needs_result_of(needs("success", "skipped")), "other")
        self.assertEqual(vsf.needs_result_of("{}"), "other")
        for bad in ("", "{", "[1]", '{"a": 3}', '{"a": {}}', "null"):
            self.assertEqual(vsf.needs_result_of(bad), "failure", bad)

    def test_a_run_attempt_is_not_a_longer_attempt(self):
        self.assertTrue(vsf.mentions(f"Run: {RUN}/attempts/1\n", RUN + "/attempts/1"))
        self.assertFalse(vsf.mentions(f"Run: {RUN}/attempts/10", RUN + "/attempts/1"))
        self.assertFalse(vsf.mentions(f"Run: {RUN}/attempts/2", RUN + "/attempts/1"))


class Gh(unittest.TestCase):
    def test_three_tries_with_pauses(self):
        pauses, flaky = [], FakeGitHub(fail={"x": 2})
        gh = vsf.Gh("o/r", runner=flaky, sleep=pauses.append)
        self.assertEqual(gh(["x"]), "")
        self.assertEqual((len(flaky.calls), len(pauses)), (3, 2))
        dead = FakeGitHub(fail={"x": 9})
        with self.assertRaisesRegex(vsf.GhError, "502"):
            vsf.Gh("o/r", runner=dead, sleep=pauses.append)(["x"])
        self.assertEqual(len(dead.calls), vsf.GH_ATTEMPTS)

    def test_api_has_no_repo_flag(self):
        github = FakeGitHub()
        make_gh(github)(["api", "x"])
        self.assertNotIn("--repo", github.calls[0][0])

    def test_a_hung_call_is_a_failed_attempt_and_is_retried(self):
        calls = []

        def hung_once(command, **kwargs):
            calls.append(kwargs["timeout"])
            if len(calls) == 1:
                raise subprocess.TimeoutExpired(command, kwargs["timeout"])
            return subprocess.CompletedProcess(command, 0, "ok", "")

        self.assertEqual(make_gh(hung_once)(["x"]), "ok")
        self.assertEqual(calls, [vsf.GH_CALL_TIMEOUT_SECONDS] * 2)

    def test_past_the_deadline_no_call_starts(self):
        clock = Clock()
        github = FakeGitHub(clock=clock, tick=130)
        gh = make_gh(github, clock)
        for _ in range(3):  # calls at 0, 130 and 260 s are inside the 360 s
            gh(["x"])
        with self.assertRaises(vsf.OutOfTime):
            gh(["x"])
        self.assertEqual(len(github.calls), 3)

    def test_a_degraded_github_ends_the_report_inside_the_budget(self):
        clock = Clock()
        github = FakeGitHub(labels=[vsf.LABEL], clock=clock, tick=50,
                            fail={"issue create": 99, "issue list": 99})  # fmt: skip
        with self.assertRaises(vsf.OutOfTime):
            make_gh(github, clock).create(TITLE, "B", "T", "Varka fuzz", ATTEMPT)
        self.assertLessEqual(clock.now, vsf.TOTAL_SECONDS + vsf.GH_CALL_TIMEOUT_SECONDS)
        self.assertLess(verbs(github).count("issue create"), vsf.GH_ATTEMPTS + 1)

    def test_summary_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "summary.md")
            vsf.write_summary("one", target)
            vsf.write_summary("two", target)
            with open(target, encoding="utf-8") as handle:
                self.assertEqual(handle.read(), "one\ntwo\n")
        vsf.write_summary("ignored", None)


class Issues(unittest.TestCase):
    def test_a_label_less_issue_is_found_by_the_search(self):
        github = FakeGitHub(issues=[issue(4, vsf.marker("Varka fuzz"), labels=())])
        found = make_gh(github).open_issues()
        self.assertEqual([i["number"] for i in found], [4])
        self.assertEqual(len(github.calls), 3, "label, search and newest: three bounded lists")
        self.assertTrue(all("--limit" in c for c, _ in github.calls))

    def test_the_newest_list_sees_an_issue_the_search_has_not_indexed_yet(self):
        github = FakeGitHub(issues=[issue(4, "no marker word", labels=())])
        self.assertEqual(len(make_gh(github).open_issues()), 1)


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
        self.assertIn("this issue has none", github.issues[0]["body"])

    def test_two_red_runs_with_a_failing_label_make_one_issue_and_one_comment(self):
        github = FakeGitHub(fail={"label list": 99, "label create": 99})
        gh = make_gh(github)
        first = vsf.decide("Varka fuzz", "https://r/1", "s", [FAILURE], [], "d")
        vsf.act(first, gh, True, "Varka fuzz", "https://r/1")
        second = vsf.decide("Varka fuzz", "https://r/2", "s", [FAILURE], gh.open_issues(), "d")
        self.assertEqual(second[:2], ("comment", 100))
        vsf.act(second, gh, True, "Varka fuzz", "https://r/2")
        self.assertEqual((len(github.issues), len(github.comments)), (1, 1))

    def test_lost_reply_creates_no_second_issue_even_without_the_label(self):
        for kwargs in ({"labels": [vsf.LABEL]}, {"fail": {"label list": 9, "label create": 9}}):
            github = FakeGitHub(lost_reply=["issue create"], **kwargs)
            create(github)
            self.assertEqual(len(github.issues), 1)
            self.assertEqual(verbs(github).count("issue create"), 1)

    def test_a_failure_without_effect_is_retried(self):
        github = FakeGitHub(labels=[vsf.LABEL], fail={"issue create": 1})
        create(github)
        self.assertEqual(len(github.issues), 1)
        self.assertEqual(verbs(github).count("issue create"), 2)

    def test_a_rerun_of_the_same_run_is_not_a_landed_write(self):
        earlier = issue(4, f"Run: {RUN}/attempts/1\n" + vsf.marker("Varka fuzz"))
        github = FakeGitHub(labels=[vsf.LABEL], fail={"issue create": 1}, issues=[earlier])
        vsf.act(("create", TITLE, "B", "Run: x\n"), make_gh(github), True, "Varka fuzz",
                RUN + "/attempts/2")  # fmt: skip
        self.assertEqual(github.comments, [("4", "Run: x\n")], "reported on the open issue")
        github = FakeGitHub(fail={"issue comment": 1})
        github.comments.append(("7", f"Run: {RUN}/attempts/1\n"))
        vsf.act(("comment", 7, "C"), make_gh(github), True, "Varka fuzz", RUN + "/attempts/2")
        self.assertEqual(len(github.comments), 2, "attempt 2 posts its own comment")

    def test_a_hung_label_endpoint_does_not_eat_the_budget(self):
        github = FakeGitHub(fail={"label list": 99, "label create": 99})
        create(github)
        self.assertEqual(verbs(github).count("label list"), 1)
        self.assertEqual(verbs(github).count("label create"), 1)
        self.assertEqual(len(github.issues), 1)

    def test_a_failed_nested_comment_opens_no_second_issue(self):
        github = FakeGitHub(labels=[vsf.LABEL], fail={"issue create": 1, "issue comment": 9},
                            issues=[issue(4, vsf.marker("Varka fuzz"))])  # fmt: skip
        with self.assertRaises(vsf.GhError):
            create(github)
        self.assertEqual(len(github.issues), 1)
        self.assertEqual(verbs(github).count("issue create"), 1)

    def test_the_final_attempt_is_checked_for_having_landed(self):
        github = FakeGitHub(labels=[vsf.LABEL])
        real_call, state = github.__call__, {"n": 0}

        def lose_third(command, input=None, **kwargs):
            if command[1:3] == ["issue", "comment"]:
                state["n"] += 1
                if state["n"] == vsf.GH_ATTEMPTS:  # the last reply is lost, the comment landed
                    real_call(command, input, **kwargs)
                return subprocess.CompletedProcess(command, 1, "", "502")
            return real_call(command, input, **kwargs)

        vsf.act(("comment", 7, f"Run: {ATTEMPT}\n"), make_gh(lose_third), True, "Varka fuzz",
                ATTEMPT)  # fmt: skip
        self.assertEqual(len(github.comments), 1)

    def test_an_issue_that_appeared_meanwhile_gets_a_comment(self):
        github = FakeGitHub(labels=[vsf.LABEL], fail={"issue create": 1},
                            issues=[issue(4, vsf.marker("Varka fuzz"))])  # fmt: skip
        create(github)
        self.assertEqual(len(github.issues), 1)
        self.assertEqual(github.comments, [("4", f"Run: {ATTEMPT}\n")])

    def test_comment_retries_without_duplicates(self):
        text = f"Run: {ATTEMPT}\n"
        for kwargs, tries in (
            ({"fail": {"issue comment": 1}}, 2),
            ({"lost_reply": ["issue comment"]}, 1),
        ):
            github = FakeGitHub(**kwargs)
            vsf.act(("comment", 7, text), make_gh(github), True, "Varka fuzz", ATTEMPT)
            self.assertEqual((len(github.comments), verbs(github).count("issue comment")),
                             (1, tries))  # fmt: skip

    def test_comment_fails_after_three_tries(self):
        github = FakeGitHub(fail={"issue comment": 9})
        with self.assertRaises(vsf.GhError):
            vsf.act(("comment", 7, "C"), make_gh(github), True, "Varka fuzz", ATTEMPT)
        self.assertEqual(verbs(github).count("issue comment"), vsf.GH_ATTEMPTS)

    def test_dry_run_changes_nothing(self):
        github = FakeGitHub()
        vsf.act(("create", "T", "B", "C"), make_gh(github), False)
        self.assertEqual(github.calls, [])


class Cli(unittest.TestCase):
    ARGS = ["--workflow", "Varka fuzz", "--run-url", RUN, "--run-attempt", "1", "--sha", "abc",
            "--date", DATE]  # fmt: skip

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

    def test_dry_run_from_a_jobs_file_changes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "jobs.json")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(json.dumps(FAILURE))
            github, out = self.run_main(["--jobs-file", path])
        self.assertEqual(github.calls, [])
        self.assertIn("open an issue titled", out)
        self.assertIn("dry run", out)

    def test_apply_from_the_run_id_opens_one_issue_and_a_cancelled_run_is_reported(self):
        github = FakeGitHub(labels=[vsf.LABEL], jobs=[FAILURE])
        github, _ = self.run_main(["--run-id", "5", "--apply"], github)
        self.assertEqual(len(github.issues), 1)
        self.assertIn(ATTEMPT, github.issues[0]["body"])
        jobs_call = github.calls[0][0]
        self.assertIn("repos/vecbricks/varka/actions/runs/5/jobs?per_page=100", jobs_call)
        self.assertEqual(sum("annotations" in " ".join(c) for c, _ in github.calls), 0)
        github = FakeGitHub(labels=[vsf.LABEL], jobs=[job("success"), job("cancelled", "c")])
        github, _ = self.run_main(["--run-id", "5", "--apply", "--needs-json",
                                   '{"a": {"result": "cancelled"}}'], github)  # fmt: skip
        self.assertIn(vsf.CANCELLED_NOTE, github.issues[0]["body"])

    def test_a_failed_need_reports_without_a_listed_job_and_green_needs_do_not(self):
        needs = json.dumps({"fuzz": {"result": "failure"}})
        github = FakeGitHub(labels=[vsf.LABEL], jobs=[])
        github, _ = self.run_main(["--run-id", "5", "--apply", "--needs-json", needs], github)
        self.assertEqual(len(github.issues), 1)
        self.assertIn("showed no failed or cancelled job", github.issues[0]["body"])
        green = json.dumps({"fuzz": {"result": "success"}})
        github = FakeGitHub(labels=[vsf.LABEL], jobs=[])
        github, _ = self.run_main(["--run-id", "5", "--apply", "--needs-json", green], github)
        self.assertEqual(github.issues, [])

    def test_unparseable_or_empty_needs_still_report(self):
        for needs in ("{", ""):
            github = FakeGitHub(labels=[vsf.LABEL], jobs=[])
            github, _ = self.run_main(["--run-id", "5", "--apply", "--needs-json", needs], github)
            self.assertEqual(len(github.issues), 1)

    def test_an_unreadable_jobs_listing_still_reports_a_failed_need(self):
        github = FakeGitHub(labels=[vsf.LABEL], fail={"api": 9})
        needs = json.dumps({"fuzz": {"result": "failure"}})
        github, _ = self.run_main(["--run-id", "5", "--apply", "--needs-json", needs], github)
        self.assertEqual(len(github.issues), 1)

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
            github = FakeGitHub(labels=[vsf.LABEL], fail={"issue create": 9})
            with self.assertRaises(SystemExit) as caught:
                self.run_main(["--run-id", "5", "--apply", "--needs-json", ""], github, summary)
            self.assertIn("Could not report", str(caught.exception.code))
            with open(summary, encoding="utf-8") as handle:
                text = handle.read()
        self.assertIn(RUN, text)
        self.assertIn("needs failure", text)


if __name__ == "__main__":
    unittest.main()
