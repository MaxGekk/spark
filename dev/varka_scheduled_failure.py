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
"""Report a failed scheduled workflow run as a GitHub issue.

    dev/varka_scheduled_failure.py --workflow W --run-url URL --run-attempt 1 --sha SHA \\
        --run-id 123 --needs-json "$NEEDS" --date 2026-10-20 --apply    # tests: *_test.py

The last job of each scheduled Varka workflow calls this (through
.github/actions/report-scheduled-failure) when a job it needs failed or was cancelled.

  * An open issue that belongs to the same workflow gets a comment with the run, the jobs and
    the head commit; a second red night of the same workflow never opens a second issue.
  * Otherwise one issue is opened, titled "Scheduled run failed: <workflow> (first red <date>)".
  * Nothing is closed; a person closes the issue, and the next red run opens a new one.

An issue belongs to a workflow by the marker line in its body, or, if the body was edited, by
its title. Label-less issues (see below) are found too.

Which jobs are reported: every job whose conclusion is failure, timed_out or cancelled
(skipped, success, neutral and none are not). A cancelled job is listed with a note that a
person, a newer run of the same concurrency group or a timeout cancelled it, and is not
classified: the conclusion cannot tell them apart, and the check-run annotation that could is
one call per job with a wording GitHub does not document. Observed: "Canceling since a higher
priority waiting request for build-test-... exists" for a newer run (MaxGekk/spark run
39552258555, job 119064574793) and "The run was canceled by @MaxGekk." for a person
(vecbricks/varka run 37290363433, job 111699085537); no timed-out job in 60 failed and
cancelled runs. A text list would drift and a dropped red night costs more than a line.

If the needs result (--needs-json, toJSON(needs): the worst of failure, cancelled and the
rest; unparseable counts as failure) is failure or cancelled but the jobs listing shows no
reportable job (it lags), the issue is still opened or commented, saying so.

The jobs come from the Actions API (--run-id) or, in tests, a file (--jobs-file) of JSON
objects, one per line or in a {"jobs": [...]} document. Every gh call has a 60 second timeout
and is tried three times. A write (a new issue or a comment) is retried only after checking
that the failed attempt did not land: the run URL with its /attempts/<n> (so a re-run of the
same run is a new report) is already in the issue or comment. No gh call starts after 6
minutes, inside the report job's 10. After the final failure of a gh call the report goes to
$GITHUB_STEP_SUMMARY and the script exits non-zero. The label is created only when missing and
never blocks the issue ("already exists" is success); if it fails the issue says it has none.
Without --apply nothing is changed and the open issues are not read (see --issues-file).
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time

LABEL = "scheduled-run-failed"
LABEL_COLOR = "B60205"
LABEL_DESCRIPTION = "A scheduled workflow run failed; a person closes the issue"
REPORTABLE = ("failure", "timed_out", "cancelled")
CANCELLED_NOTE = (
    "cancelled (by a person, by a newer run of the same concurrency group, or by a timeout)"
)
MARKER_WORD = "varka-scheduled-failure"
MAX_JOBS_LISTED = 40
GH_ATTEMPTS = 3
GH_BACKOFF_SECONDS = 2
GH_CALL_TIMEOUT_SECONDS = 60
TOTAL_SECONDS = 360


class GhError(Exception):
    """A gh call that never succeeded."""


class OutOfTime(GhError):
    """The total deadline passed; no further gh call is started."""


def one_line(text):
    """The text on one line, so a name cannot break out of a title or a marker."""
    return re.sub(r"\s+", " ", text).strip()


def marker(workflow):
    # An HTML comment ends at "-->" or "--!>", so no two hyphens may stay adjacent in it.
    return f"<!-- {MARKER_WORD}: {re.sub('-(?=-)', '- ', one_line(workflow))} -->"


def code(text):
    return "`" + one_line(text).replace("`", "'") + "`"


def read_jobs(text):
    """The jobs from gh's output: one object per line, or a {"jobs": [...]} document."""
    try:
        whole = json.loads(text)
    except json.JSONDecodeError:
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    return whole.get("jobs", [whole]) if isinstance(whole, dict) else list(whole)


def needs_result_of(needs_json):
    """'failure', 'cancelled' or 'other' from toJSON(needs); unparseable counts as failure."""
    try:
        results = [n["result"] for n in json.loads(needs_json or "{}").values()]
    except (ValueError, TypeError, KeyError, AttributeError):
        return "failure"
    return "failure" if "failure" in results else "cancelled" if "cancelled" in results else "other"


def job_lines(jobs):
    lines = []
    for job in jobs[:MAX_JOBS_LISTED]:
        url = job.get("html_url") or ""
        status = CANCELLED_NOTE if job.get("conclusion") == "cancelled" else job.get("conclusion")
        lines.append(f"- {code(job.get('name', '?'))}: {status}" + (f", {url}" if url else ""))
    if len(jobs) > MAX_JOBS_LISTED:
        lines.append(f"- and {len(jobs) - MAX_JOBS_LISTED} more; the run lists them all")
    return lines


NO_JOB_NOTE = "The run's jobs listing showed no failed or cancelled job (it can lag); see the run."


def belongs_to(issue, workflow):
    if marker(workflow) in (issue.get("body") or ""):
        return True
    name = re.escape(one_line(workflow))
    shape = rf"Scheduled run failed: {name} \(first red \d{{4}}-\d{{2}}-\d{{2}}\)"
    return re.fullmatch(shape, issue.get("title") or "") is not None


def mentions(text, run_ref):
    """True if the text names this run attempt, not a longer number that starts the same."""
    return re.search(re.escape(run_ref) + r"(?!\d)", text or "") is not None


def decide(workflow, run_ref, sha, jobs, open_issues, date, needs_result="other"):
    """("none",), ("comment", number, text) or ("create", title, body, text) for the run."""
    reportable = [j for j in jobs if j.get("conclusion") in REPORTABLE]
    if not reportable and needs_result == "other":
        return ("none",)
    lines = [f"Run: {run_ref}", f"Head commit: {sha}"]  # the facts of a new issue and a comment
    if reportable:
        lines += [f"Jobs that did not pass ({len(reportable)}):", *job_lines(reportable)]
    else:
        lines.append(NO_JOB_NOTE)
    text = "\n".join(lines) + "\n"
    mine = sorted((i for i in open_issues if belongs_to(i, workflow)), key=lambda i: i["number"])
    if mine:
        return ("comment", mine[0]["number"], text)
    body = (
        f"The scheduled workflow {code(workflow)} failed.\n\n{text}\n"
        "This issue is the one place its red runs are reported: a later red run of the same "
        "workflow comments here while the issue is open, and a person closes it once the cause "
        f"is fixed or filed.\n\n{marker(workflow)}\n"
    )
    return ("create", f"Scheduled run failed: {one_line(workflow)} (first red {date})", body, text)


class Gh:
    """gh calls for one repository, each tried GH_ATTEMPTS times, all inside one deadline."""

    def __init__(self, repo, runner=subprocess.run, sleep=time.sleep, clock=time.monotonic):
        self.repo, self.runner, self.sleep, self.clock = repo, runner, sleep, clock
        self.deadline = clock() + TOTAL_SECONDS

    def __call__(self, args, stdin=None, retry=True):
        """The output of a gh call; GhError if it never succeeds, OutOfTime past the deadline.

        retry=False is for a write the caller retries itself, after checking it did not land.
        """
        # gh api takes the repository in its path and has no --repo flag.
        command = ["gh", *args] if args[0] == "api" else ["gh", *args, "--repo", self.repo]
        attempts = GH_ATTEMPTS if retry else 1
        for attempt in range(1, attempts + 1):
            if self.clock() > self.deadline:
                raise OutOfTime(f"gave up after {TOTAL_SECONDS} s: gh {' '.join(args)}")
            try:
                result = self.runner(command, capture_output=True, text=True, check=False,
                                     input=stdin, timeout=GH_CALL_TIMEOUT_SECONDS)  # fmt: skip
            except OSError as error:  # gh is not installed
                result = subprocess.CompletedProcess(command, 127, "", str(error))
            except subprocess.TimeoutExpired:  # a hung connection is a failed attempt
                result = subprocess.CompletedProcess(command, 124, "", "timed out")
            if result.returncode == 0:
                return result.stdout
            if attempt < attempts:
                self.sleep(GH_BACKOFF_SECONDS * attempt)
        raise GhError(f"gh {' '.join(args)} failed {attempts} time(s):\n{result.stderr[-600:]}")

    def json(self, args):
        return json.loads(self(args) or "[]")

    def jobs_of_run(self, run_id):
        path = f"repos/{self.repo}/actions/runs/{run_id}/jobs?per_page=100"
        jq = ".jobs[] | {id, name, conclusion, html_url}"
        return read_jobs(self(["api", "--paginate", path, "--jq", jq]))

    def open_issues(self):
        """Open issues by label, by marker search (finds a label-less one) and the newest 20
        (the search lags, so a moment-old issue shows only here); three bounded lists."""
        base = ["issue", "list", "--state", "open", "--json", "number,title,body"]
        found = {}
        for extra in (["--label", LABEL, "--limit", "100"],
                      ["--search", f'"{MARKER_WORD}" in:body', "--limit", "100"],
                      ["--limit", "20"]):  # fmt: skip
            for issue in self.json(base + extra):
                found[issue["number"]] = issue
        return list(found.values())

    def ensure_label(self):
        """True if the label exists afterwards; created only when missing, never overwritten."""
        try:
            found = self.json(["label", "list", "--search", LABEL, "--json", "name"])
            if any(entry.get("name") == LABEL for entry in found):
                return True
        except (GhError, ValueError, AttributeError):
            pass  # unknown: try to create it; an existing label makes that fail harmlessly
        try:
            self(["label", "create", LABEL, "--color", LABEL_COLOR,
                  "--description", LABEL_DESCRIPTION])  # fmt: skip
            return True
        except GhError as error:
            print(f"the label could not be created: {error}")
            return "already exists" in str(error)

    def write(self, command, text, landed):
        """Run a write, retrying; a retry first asks landed() whether the failed attempt took."""
        for attempt in range(1, GH_ATTEMPTS + 1):
            try:
                self(command, text, retry=False)
                return
            except GhError as error:
                if attempt == GH_ATTEMPTS or isinstance(error, OutOfTime):
                    raise
            self.sleep(GH_BACKOFF_SECONDS * attempt)
            try:
                if landed():
                    return
            except OutOfTime:
                raise
            except (GhError, ValueError):
                pass  # unreadable: treat as not landed and try again

    def comment(self, number, text, run_ref):
        def landed():
            seen = json.loads(self(["issue", "view", str(number), "--json", "comments"]) or "{}")
            found = any(mentions(c.get("body"), run_ref) for c in seen.get("comments", []))
            if found:
                print("the failed attempt had posted the comment; not posting another")
            return found

        self.write(["issue", "comment", str(number), "--body-file", "-"], text, landed)

    def create(self, title, body, text, workflow, run_ref):
        labelled = self.ensure_label()
        if not labelled:
            body += f"\nThe label {LABEL} could not be created, so this issue has none.\n"

        def landed():
            mine = [i for i in self.open_issues() if belongs_to(i, workflow)]
            if any(mentions(i.get("body"), run_ref) for i in mine):
                print("the failed attempt had created the issue; not creating another")
                return True
            if mine:  # an issue appeared meanwhile: report there, not in a second one
                self.comment(min(i["number"] for i in mine), text, run_ref)
                return True
            return False

        command = ["issue", "create", "--title", title, "--body-file", "-"]
        self.write(command + (["--label", LABEL] if labelled else []), body, landed)


def act(decision, gh, apply, workflow="", run_ref=""):
    """Print the decision and, with apply, carry it out."""
    if decision[0] == "none":
        print("no failed or cancelled job; nothing to report")
    elif decision[0] == "comment":
        print(f"comment on issue #{decision[1]}:\n{decision[2]}")
        if apply:
            gh.comment(decision[1], decision[2], run_ref)
    else:
        print(f"open an issue titled {decision[1]!r}:\n{decision[2]}")
        if apply:
            gh.create(decision[1], decision[2], decision[3], workflow, run_ref)


def write_summary(text, path=None):
    """Put the text on the job's summary page."""
    path = path or os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(text + "\n")


def read_file(path):
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def execute(args, **kwargs):
    """Decide and, with --apply, act; GhError when GitHub cannot be reached."""
    gh = Gh(args.repo, **kwargs)
    jobs = read_jobs(read_file(args.jobs_file)) if args.jobs_file else gh.jobs_of_run(args.run_id)
    if args.issues_file:
        issues = json.loads(read_file(args.issues_file) or "[]")
    elif args.apply:
        issues = gh.open_issues()
    else:
        issues = []
        print("dry run: the open issues were not read")
    run_ref = f"{args.run_url}/attempts/{args.run_attempt}"
    decision = decide(args.workflow, run_ref, args.sha, jobs, issues, args.date,
                      args.needs_result)  # fmt: skip
    act(decision, gh, args.apply, args.workflow, run_ref)


def main(argv=None, **kwargs):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    for name in ("workflow", "run-url", "run-attempt", "sha", "date"):
        parser.add_argument(f"--{name}", required=True)  # date: UTC, YYYY-MM-DD
    parser.add_argument("--run-id", help="the run whose jobs gh lists")
    parser.add_argument("--jobs-file", help="the run's jobs as gh prints them, not --run-id")
    parser.add_argument("--issues-file", help="open issues as JSON; gh lists them when absent")
    parser.add_argument("--needs-json", default="", help="toJSON(needs) of the calling job")
    parser.add_argument("--repo", default="vecbricks/varka")
    parser.add_argument("--apply", action="store_true", help="change GitHub; dry run otherwise")
    args = parser.parse_args(argv)
    if not (args.jobs_file or args.run_id):
        parser.error("--run-id or --jobs-file is required")
    args.needs_result = needs_result_of(args.needs_json)
    try:
        execute(args, **kwargs)
    except (GhError, ValueError, OSError, AttributeError, TypeError, KeyError) as error:
        message = (
            f"Could not report the failed scheduled run of {code(args.workflow)}: {error}\n"
            f"The run to read: {args.run_url} (head {args.sha}, needs {args.needs_result})."
        )
        write_summary(message)
        sys.exit(message)


if __name__ == "__main__":
    main()
