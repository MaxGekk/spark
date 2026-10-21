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

    dev/varka_scheduled_failure.py --workflow "Varka fuzz" --run-url URL --sha SHA \\
        --run-id 123 --needs-json "$NEEDS" --date 2026-10-20 --repo vecbricks/varka --apply
    python3 dev/varka_scheduled_failure_test.py      # the tests

A red scheduled run has no author to see it, so nobody read the weekly matrix while it failed
five weeks running. The last job of each scheduled Varka workflow calls this (through
.github/actions/report-scheduled-failure) when an earlier job of the run failed. The rule:

  * An open issue labelled "scheduled-run-failed" that belongs to the same workflow gets a
    comment with the run, the failed jobs and the head commit; a second red night of the same
    workflow never opens a second issue.
  * Otherwise one issue is opened, titled "Scheduled run failed: <workflow> (first red <date>)".
  * Nothing is closed. A person closes the issue once the cause is fixed or filed, and the
    next red run then opens a new one.

An issue belongs to a workflow by the marker line in its body, or, if someone edited the body
away, by its title. The open issues are read without the label, so an issue opened without it
(see below) is still found.

Which jobs count. A run is reported when a job FAILED (failure or timed_out). A job that exceeds
its timeout-minutes ends as cancelled, but so does a job a person cancelled, a newer run
replaced or a failing sibling cancelled (fail-fast), and the conclusion alone cannot tell them
apart. So for each cancelled job the check-run annotations are read (the job id of the jobs
listing is its check-run id), and the rule fails SAFE: a cancelled job is NOT reported only when
an annotation positively identifies a concurrency or user cancel (DELIBERATE_CANCEL_TEXTS), and
not when any annotation says "maximum execution time". Any other text, no annotation and an
unreadable reply all report the job, the last with the note "could not read why it was
cancelled": a reworded GitHub text costs one extra line, never a dropped red night. Observed on
real runs: "Canceling since a higher priority waiting request for <group> exists" (concurrency)
and "The run was canceled by @<user>." (a person); "The operation was canceled." comes with
both and proves nothing. Not observed: a timeout's "has exceeded the maximum execution time"
(GitHub's documented text) and a lost runner. A run that fails at startup runs no job and so
reports nothing. Reading is bounded: at most 20 cancelled jobs are read, in at most 3
minutes, each gh call for at most 60 seconds. If every job read was a deliberate cancel the
rest are taken as the same (a person cancelling a run of 100 jobs reports nothing); otherwise
the rest are reported unread, the jobs past the cap as one summary line.

When the caller's needs say failure (--needs-json, the action's toJSON(needs)) but no job is
reportable (the listing lags, or covers only the latest attempt), the issue is still opened or
commented, saying that the listing showed none: a red run is never ended by a green
reporter. Needs that say only cancelled and show only deliberate cancels report nothing.

The jobs come from the Actions API (--run-id), or from a file for tests (--jobs-file): one
JSON object per line ({"id", "name", "conclusion", "html_url"}) or one {"jobs": [...]}
document. A gh call is tried three times; if a read still fails, the report is written to
$GITHUB_STEP_SUMMARY and the script exits non-zero. A write (a new issue or comment) is retried
too, but only after checking that the failed attempt did not land (the issue or comment with
this run's URL is already there), so a lost reply never posts it twice. The label is created
only when missing and an "already exists" reply counts as success; if the label step still
fails, the issue is opened without it and says so.
Without --apply nothing is changed on GitHub and the open issues are not read (give
--issues-file to see the comment case).
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
FAILED = ("failure", "timed_out")
TIMEOUT_TEXT = "maximum execution time"
# Annotation texts (lower case) that say a cancelled job was cancelled on purpose. Only these
# make a cancelled job unreportable; any other text, or none, is reported, so a rewording on
# GitHub's side fails safe. Both were observed on real runs (fixtures in the test file):
#  * "Canceling since a higher priority waiting request for <group> exists": a newer run of
#    the same concurrency group replaced the run (MaxGekk/spark run 39552258555).
#  * "The run was canceled by @<user>.": a person cancelled the run (vecbricks/varka run
#    37290363433).
# Seen on both and so no evidence of intent: "The operation was canceled." Not found in any
# run searched: a job past its timeout-minutes ("... has exceeded the maximum execution time
# of N minutes", GitHub's documented text, so it is also checked for) and a lost runner.
DELIBERATE_CANCEL_TEXTS = (
    "canceling since a higher priority waiting request for ",
    "the run was canceled by @",
)
MAX_ANNOTATION_READS = 20
ANNOTATION_BUDGET_SECONDS = 180
MAX_JOBS_LISTED = 40
GH_ATTEMPTS = 3
GH_BACKOFF_SECONDS = 2
GH_CALL_TIMEOUT_SECONDS = 60


class GhError(Exception):
    pass


def marker(workflow):
    # An HTML comment ends at "-->" or "--!>", so no two hyphens may stay adjacent in it.
    return f"<!-- varka-scheduled-failure: {re.sub('-(?=-)', '- ', one_line(workflow))} -->"


def one_line(text):
    """The text on one line, so a name cannot break out of a title or a marker comment."""
    return re.sub(r"\s+", " ", text).strip()


def code(text):
    """A markdown code span that holds the text whatever it contains."""
    return "`" + one_line(text).replace("`", "'") + "`"


def read_jobs(text):
    """The jobs from gh's output: one object per line, or a {"jobs": [...]} document."""
    stripped = text.strip()
    if not stripped:
        return []
    try:
        whole = json.loads(stripped)
        if isinstance(whole, dict) and "jobs" in whole:
            return whole["jobs"]
        if isinstance(whole, dict):
            return [whole]
        return list(whole)
    except json.JSONDecodeError:
        return [json.loads(line) for line in stripped.splitlines() if line.strip()]


def failed_jobs(jobs):
    return [j for j in jobs if j.get("conclusion") in FAILED]


def is_deliberate_cancel(messages):
    """True only if an annotation names a deliberate cancel and none names a timeout."""
    texts = [(m or "").lower() for m in messages]
    if any(TIMEOUT_TEXT in t for t in texts):
        return False
    return any(d in t for t in texts for d in DELIBERATE_CANCEL_TEXTS)


def reportable_jobs(jobs, annotations_of, clock=time.monotonic):
    """The jobs to report: failed ones, and cancelled ones not known to be deliberate.

    annotations_of(job) gives the annotation messages or raises GhError or ValueError. A
    cancelled job whose reason cannot be read, or was not read within the cap and the time
    budget, is reported with a note, never dropped.
    """
    out, started, read, beyond, all_deliberate = [], clock(), 0, 0, True
    for job in jobs:
        if job.get("conclusion") in FAILED:
            out.append(job)
        elif job.get("conclusion") == "cancelled":
            if read >= MAX_ANNOTATION_READS:
                beyond += 1
                continue
            read += 1
            if clock() - started > ANNOTATION_BUDGET_SECONDS:
                out.append({**job, "why": "not read, out of time"})
                all_deliberate = False
                continue
            try:
                messages = annotations_of(job)
            except (GhError, ValueError):
                out.append({**job, "why": "could not read why it was cancelled"})
                all_deliberate = False
                continue
            if is_deliberate_cancel(messages):
                print(f"not reported, cancelled on purpose: {code(job.get('name', '?'))}")
            else:
                all_deliberate = False
                why = (
                    "timeout"
                    if any(TIMEOUT_TEXT in (m or "").lower() for m in messages)
                    else ("cancelled for an unrecognised reason")
                )
                out.append({**job, "why": why})
    if beyond and all_deliberate:
        # Every job read was a deliberate cancel (a person cancelled a big run, or a newer run
        # replaced it): the rest of the same run is taken to be the same.
        print(f"not reported, {beyond} more cancelled jobs, taken as deliberate like the rest")
    elif beyond:
        out.append({"name": f"{beyond} more cancelled jobs", "conclusion": "cancelled",
                    "why": "not read"})  # fmt: skip
    return out


def issue_title(workflow, date):
    return f"Scheduled run failed: {one_line(workflow)} (first red {date})"


def job_lines(failed):
    lines = []
    for job in failed[:MAX_JOBS_LISTED]:
        url = job.get("html_url") or job.get("url") or ""
        name = code(job.get("name", "?"))
        why = f" ({job['why']})" if job.get("why") else ""
        lines.append(f"- {name}: {job.get('conclusion')}{why}" + (f", {url}" if url else ""))
    if len(failed) > MAX_JOBS_LISTED:
        lines.append(f"- and {len(failed) - MAX_JOBS_LISTED} more; the run lists them all")
    return lines


def report(run_url, sha, failed, note=""):
    """The run's facts, the body of a new issue and of a comment alike."""
    lines = [f"Run: {run_url}", f"Head commit: {sha}"]
    if failed:
        lines += [f"Failed jobs ({len(failed)}):", *job_lines(failed)]
    if note:
        lines.append(note)
    return "\n".join(lines)


def issue_body(workflow, run_url, sha, failed, note=""):
    return (
        f"The scheduled workflow {code(workflow)} failed.\n\n"
        + report(run_url, sha, failed, note)
        + "\n\nThis issue is the one place its red runs are reported: a later red run of the "
        "same workflow comments here while the issue is open, and a person closes it once the "
        "cause is fixed or filed.\n\n" + marker(workflow) + "\n"
    )


def belongs_to(issue, workflow):
    if marker(workflow) in (issue.get("body") or ""):
        return True
    name = re.escape(one_line(workflow))
    shape = rf"Scheduled run failed: {name} \(first red \d{{4}}-\d{{2}}-\d{{2}}\)"
    return re.fullmatch(shape, issue.get("title") or "") is not None


def needs_result_of(needs_json):
    """'failure', 'cancelled' or '' from toJSON(needs): the worst result of the needed jobs."""
    results = [(n or {}).get("result") for n in json.loads(needs_json or "{}").values()]
    return "failure" if "failure" in results else "cancelled" if "cancelled" in results else ""


NO_JOB_NOTE = (
    "The run's jobs listing showed no failed job although a job it depends on failed "
    "(the listing can lag or cover only the latest attempt); the run page has the failure."
)


def decide(workflow, run_url, sha, failed, open_issues, date, needs_result=""):
    """("none",), ("comment", number, text) or ("create", title, body, text) for the run.

    failed are the reportable jobs (reportable_jobs).
    """
    note = ""
    if not failed:
        if needs_result != "failure":
            return ("none",)
        note = NO_JOB_NOTE
    mine = sorted((i for i in open_issues if belongs_to(i, workflow)), key=lambda i: i["number"])
    if mine:
        return ("comment", mine[0]["number"], report(run_url, sha, failed, note) + "\n")
    return (
        "create",
        issue_title(workflow, date),
        issue_body(workflow, run_url, sha, failed, note),
        report(run_url, sha, failed, note) + "\n",
    )


def gh(args, repo, stdin=None, runner=subprocess.run, sleep=time.sleep, retry=True):
    """The output of a gh call, tried GH_ATTEMPTS times; GhError if it never succeeds.

    retry=False is for a write the caller retries itself, after checking the attempt did not
    land (a retry after a reply lost on the way back would post it twice).
    """
    # gh api takes the repository in its path and has no --repo flag.
    command = ["gh", *args] if args[0] == "api" else ["gh", *args, "--repo", repo]
    attempts = GH_ATTEMPTS if retry else 1
    for attempt in range(1, attempts + 1):
        try:
            result = runner(
                command, capture_output=True, text=True, check=False, input=stdin,
                timeout=GH_CALL_TIMEOUT_SECONDS,
            )  # fmt: skip
        except OSError as error:  # gh is not installed
            result = subprocess.CompletedProcess(command, 127, "", str(error))
        except subprocess.TimeoutExpired:  # a hung connection counts as a failed attempt
            result = subprocess.CompletedProcess(
                command, 124, "", f"timed out after {GH_CALL_TIMEOUT_SECONDS} s"
            )
        if result.returncode == 0:
            return result.stdout
        if attempt < attempts:
            sleep(GH_BACKOFF_SECONDS * attempt)
    raise GhError(f"gh {' '.join(args)} failed {attempts} time(s):\n{result.stderr[-600:]}")


def jobs_of_run(repo, run_id, **kwargs):
    out = gh(
        ["api", "--paginate", f"repos/{repo}/actions/runs/{run_id}/jobs?per_page=100",
         "--jq", ".jobs[] | {id, name, conclusion, html_url}"],
        repo, **kwargs,
    )  # fmt: skip
    return read_jobs(out)


def annotations_of(repo, job, **kwargs):
    """The messages of the job's check-run annotations; GhError if they cannot be read."""
    if not job.get("id"):
        raise GhError(f"job {job.get('name')} has no id")
    out = gh(
        ["api", "--paginate", f"repos/{repo}/check-runs/{job['id']}/annotations",
         "--jq", ".[] | {message}"],
        repo, **kwargs,
    )  # fmt: skip
    return [a.get("message") for a in read_jobs(out)]


def open_issues_of(repo, **kwargs):
    """Every open issue (by marker or title, not by label, so a label-less one is found)."""
    out = gh(
        ["issue", "list", "--state", "open", "--limit", "1000", "--json", "number,title,body"],
        repo, **kwargs,
    )  # fmt: skip
    return json.loads(out or "[]")


def ensure_label(repo, **kwargs):
    """True if the label exists afterwards; it is created only when missing, never overwritten."""
    try:
        if label_exists(repo, **kwargs):
            return True
    except (GhError, ValueError, AttributeError):
        pass  # unknown: try to create it; an existing label makes that fail harmlessly
    try:
        gh(["label", "create", LABEL, "--color", LABEL_COLOR,
            "--description", LABEL_DESCRIPTION], repo, **kwargs)  # fmt: skip
        return True
    except GhError as error:
        if "already exists" in str(error):
            return True
        print(f"the label could not be created: {error}")
    try:  # a concurrent report may have created it between the check and the create
        return label_exists(repo, **kwargs)
    except (GhError, ValueError, AttributeError):
        return False


def label_exists(repo, **kwargs):
    found = json.loads(gh(["label", "list", "--search", LABEL, "--json", "name"], repo,
                          **kwargs) or "[]")  # fmt: skip
    return any(entry.get("name") == LABEL for entry in found)


NO_LABEL_NOTE = (
    "The label " + LABEL + " could not be created or applied, so this issue has none; add "
    "it by hand. Later red runs find the issue by its marker."
)


def create_issue(title, body, text, repo, workflow, run_url, sleep=time.sleep, **kwargs):
    """Open the issue, retrying; a retry first checks the failed attempt did not create it."""
    labelled = ensure_label(repo, sleep=sleep, **kwargs)
    if not labelled:
        body += "\n" + NO_LABEL_NOTE + "\n"
    command = ["issue", "create", "--title", title, "--body-file", "-"]
    if labelled:
        command += ["--label", LABEL]
    for attempt in range(1, GH_ATTEMPTS + 1):
        try:
            gh(command, repo, body, retry=False, sleep=sleep, **kwargs)
            return
        except GhError:
            if attempt == GH_ATTEMPTS:
                raise
            sleep(GH_BACKOFF_SECONDS * attempt)
            try:
                found = open_issues_of(repo, sleep=sleep, **kwargs)
            except (GhError, ValueError):
                continue  # unreadable: treat as not landed and try again
            mine = [i for i in found if belongs_to(i, workflow)]
            if any(run_url in (i.get("body") or "") for i in mine):
                print("the failed attempt had created the issue; not creating another")
                return
            if mine:  # an issue appeared meanwhile: report there instead of opening a second
                number = min(i["number"] for i in mine)
                comment_issue(number, text, repo, run_url, sleep=sleep, **kwargs)
                return


def comment_issue(number, text, repo, run_url, sleep=time.sleep, **kwargs):
    """Comment, retrying; a retry first checks the failed attempt did not post the comment."""
    for attempt in range(1, GH_ATTEMPTS + 1):
        try:
            gh(["issue", "comment", str(number), "--body-file", "-"], repo, text, retry=False,
               sleep=sleep, **kwargs)  # fmt: skip
            return
        except GhError:
            if attempt == GH_ATTEMPTS:
                raise
            sleep(GH_BACKOFF_SECONDS * attempt)
            try:
                seen = json.loads(
                    gh(
                        ["issue", "view", str(number), "--json", "comments"],
                        repo,
                        sleep=sleep,
                        **kwargs,
                    )
                    or "{}"  # fmt: skip
                )
            except (GhError, ValueError):
                continue  # unreadable: treat as not landed and try again
            if any(run_url in (c.get("body") or "") for c in seen.get("comments", [])):
                print("the failed attempt had posted the comment; not posting another")
                return


def act(decision, repo, apply, workflow="", run_url="", **kwargs):
    """Print the decision and, with apply, carry it out."""
    if decision[0] == "none":
        print("no reportable failed job in the run; nothing to report")
    elif decision[0] == "comment":
        print(f"comment on issue #{decision[1]}:\n{decision[2]}")
        if apply:
            comment_issue(decision[1], decision[2], repo, run_url, **kwargs)
    else:
        print(f"open an issue titled {decision[1]!r}:\n{decision[2]}")
        if apply:
            create_issue(decision[1], decision[2], decision[3], repo, workflow, run_url, **kwargs)


def write_summary(text, path=None):
    """Put the text on the job's summary page, where a red job's reader looks first."""
    path = path or os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(text + "\n")


def execute(args, **kwargs):
    """Decide and, with --apply, act; GhError when GitHub cannot be reached."""
    if args.jobs_file:
        with open(args.jobs_file, encoding="utf-8") as handle:
            jobs = read_jobs(handle.read())
    else:
        jobs = jobs_of_run(args.repo, args.run_id, **kwargs)
    if args.issues_file:
        with open(args.issues_file, encoding="utf-8") as handle:
            issues = json.loads(handle.read() or "[]")
    elif args.apply:
        issues = open_issues_of(args.repo, **kwargs)
    else:
        issues = []
        print("dry run: the open issues were not read, so an open issue is not seen")
    failed = reportable_jobs(jobs, lambda job: annotations_of(args.repo, job, **kwargs))
    decision = decide(
        args.workflow, args.run_url, args.sha, failed, issues, args.date, args.needs_result
    )
    act(decision, args.repo, args.apply, args.workflow, args.run_url, **kwargs)


def main(argv=None, **kwargs):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--workflow")
    parser.add_argument("--run-url")
    parser.add_argument("--sha")
    parser.add_argument("--date", help="UTC date of this run, YYYY-MM-DD")
    parser.add_argument("--run-id", help="the run whose jobs gh lists")
    parser.add_argument("--jobs-file", help="the run's jobs as gh prints them, instead of --run-id")
    parser.add_argument("--issues-file", help="open labelled issues as JSON; gh when absent")
    parser.add_argument(
        "--needs-json",
        default="",
        help="toJSON(needs) of the calling job; a failed need reports even if no job is listed",
    )
    parser.add_argument("--repo", default="vecbricks/varka")
    parser.add_argument("--apply", action="store_true", help="change GitHub; dry run otherwise")
    args = parser.parse_args(argv)
    for need in ("workflow", "run_url", "sha", "date"):
        if not getattr(args, need):
            parser.error(f"--{need.replace('_', '-')} is required")
    if not (args.jobs_file or args.run_id):
        parser.error("--run-id or --jobs-file is required")
    args.needs_result = ""
    try:
        args.needs_result = needs_result_of(args.needs_json)
        execute(args, **kwargs)
    except (GhError, ValueError, OSError, AttributeError, TypeError, KeyError) as error:
        message = (
            f"Could not report the failed scheduled run of {code(args.workflow)}: {error}\n"
            f"The run to read: {args.run_url} (head commit {args.sha}, needs result "
            f"{args.needs_result or 'unknown'})."
        )
        write_summary(message)
        sys.exit(message)


if __name__ == "__main__":
    main()
