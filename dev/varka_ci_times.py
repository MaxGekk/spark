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
"""What each CI and laptop step costs, against the committed budget (VARKA-255.md 3.6).

    dev/varka_ci_times.py collect [--repo R] [--workflow F]... [--created RANGE] [--runs ID...]
                                  [--jobs REGEX] [--conclusion C]... [--gate-dir DIR]...
                                  [--laptop-job gate|nightly] [--budget FILE] [--out FILE]
    dev/varka_ci_times.py compare RUN_ID [--repo R] [--budget FILE] [--markdown]
    dev/varka_ci_times.py gate DIR [--laptop-job gate|nightly] [--budget FILE]
    dev/varka_ci_times.py --self-test

The budget, `sql/varka/ci/step-budget.tsv`, has one line per step: where it runs (`ci` or
`laptop`), the job and the step, the runner CPU (`-` when the job does not record it, which the
Build's jobs do not), the number of runs read, the median and the 90th percentile in seconds,
and the first and last date read. A CI job's own wall time is the step `(job)`.

`collect` reads the jobs and steps of a set of runs from the API - by default the runs of
`varka-master.yml` and `varka-fuzz.yml` on vecbricks/varka, the only CI runs of a fixed tree -
and, with `--gate-dir`, the `<step>.secs` files a laptop gate (`dev/varka_gate.sh`) or nightly
(`dev/varka_nightly.sh`) leaves, one directory per run. It rewrites the steps it read in the
budget and keeps every other line. `compare` prints one run's steps against the budget and
`gate` one laptop directory's; either marks a step that took more than 5%, and at least 30 s,
over the budget's 90th percentile. The mark is a prompt to say so in the pull request, never a
failure: one day's suites step ranged 2.8 times across runners (VARKA-255.md 2.7), so only a
step beyond the spread is named. The check sync (`update_build_status.yml`) puts
`compare --markdown` in each pull request's Build check when its run completes.

Reads use GH_TOKEN (or GITHUB_TOKEN) when set.
"""

import argparse
import json
import math
import os
import re
import statistics
import sys
import unittest
import urllib.request
from datetime import datetime

API = "https://api.github.com"
HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_BUDGET = os.path.join(HERE, "..", "sql", "varka", "ci", "step-budget.tsv")
HEADER = ["where", "job", "step", "cpu", "runs", "median_s", "p90_s", "first", "last"]
JOB_STEP = "(job)"
# A step this much over the budget's 90th percentile is marked, if it is also at least
# MIN_EXCESS seconds over: the API's whole seconds make a 4-second step read 25% slower at 5.
MARGIN = 1.05
MIN_EXCESS = 30
# The table a check summary carries starts here, so a later patch replaces rather than appends.
MARKER = "<!-- varka-step-times -->"


# Reading the API.


def next_link(link):
    """The `rel="next"` URL of a Link header, or None.

    >>> next_link('<https://x/a?page=2>; rel="next", <https://x/a?page=5>; rel="last"')
    'https://x/a?page=2'
    """
    for part in (link or "").split(","):
        seg = part.strip().split(";")
        if len(seg) >= 2 and seg[1].strip() == 'rel="next"':
            return seg[0].strip().lstrip("<").rstrip(">")
    return None


def urlopen(url):
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/vnd.github+json")
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode()), resp.headers.get("Link", "")


def get_all(path, key, opener=urlopen):
    url = API + path
    items = []
    while url:
        body, link = opener(url)
        items.extend(body.get(key, []))
        url = next_link(link)
    return items


# The arithmetic.


def seconds(start, end):
    if not start or not end:
        return None
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return int((datetime.strptime(end, fmt) - datetime.strptime(start, fmt)).total_seconds())


def p90(values):
    """The 90th percentile by nearest rank.

    >>> p90([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    9
    >>> p90([5])
    5
    """
    ordered = sorted(values)
    return ordered[max(0, math.ceil(0.9 * len(ordered)) - 1)]


def job_name(name):
    """A job's name without the calling job's prefix, which differs between callers.

    >>> job_name("Run / Varka suites: catalyst")
    'Varka suites: catalyst'
    >>> job_name("Nightly fuzz")
    'Nightly fuzz'
    """
    return name.split(" / ", 1)[1] if name.startswith("Run / ") else name


def is_housekeeping(name):
    """The runner's own steps, which no change to the repository moves.

    >>> [is_housekeeping(n) for n in ("Set up job", "Post Checkout", "Checkout")]
    [True, True, False]
    """
    return name in ("Set up job", "Complete job") or name.startswith("Post ")


def job_samples(jobs, pattern=None, conclusions=("success",)):
    """(job, step) -> [(seconds, date)] over completed jobs and their completed steps."""
    found = {}
    for job in jobs:
        name = job_name(job.get("name", ""))
        if pattern and not re.search(pattern, name):
            continue
        if job.get("status") != "completed" or job.get("conclusion") not in conclusions:
            continue
        date = (job.get("started_at") or "")[:10]
        total = seconds(job.get("started_at"), job.get("completed_at"))
        if total is not None:
            found.setdefault((name, JOB_STEP), []).append((total, date))
        for step in job.get("steps", []):
            if step.get("conclusion") != "success" or is_housekeeping(step.get("name", "")):
                continue
            secs = seconds(step.get("started_at"), step.get("completed_at"))
            if secs is not None:
                found.setdefault((name, step["name"]), []).append((secs, date))
    return found


def gate_samples(directory, laptop_job):
    """(job, step) -> [(seconds, date)] from one gate or nightly directory's .secs files."""
    found = {}
    for entry in sorted(os.listdir(directory)):
        if not entry.endswith(".secs"):
            continue
        path = os.path.join(directory, entry)
        status = path[: -len(".secs")] + ".status"
        if os.path.exists(status):
            with open(status) as f:
                # A failed step's time is not what the step costs.
                if f.read().strip() != "ok":
                    continue
        with open(path) as f:
            text = f.read().strip()
        if not text.isdigit():
            continue
        date = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d")
        found.setdefault((laptop_job, entry[: -len(".secs")]), []).append((int(text), date))
    return found


def rows_from(samples, where, cpu="-"):
    rows = []
    for (job, step), values in sorted(samples.items()):
        secs = [s for s, _ in values]
        dates = sorted(d for _, d in values if d)
        rows.append(
            {
                "where": where,
                "job": job,
                "step": step,
                "cpu": cpu,
                "runs": str(len(secs)),
                "median_s": str(round(statistics.median(secs))),
                "p90_s": str(p90(secs)),
                "first": dates[0] if dates else "-",
                "last": dates[-1] if dates else "-",
            }
        )
    return rows


def merge(samples, more):
    for key, values in more.items():
        samples.setdefault(key, []).extend(values)
    return samples


# The budget file.


def read_budget(path):
    if not os.path.exists(path):
        return []
    rows = []
    with open(path) as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            fields = line.rstrip("\n").split("\t")
            if fields == HEADER:
                continue
            rows.append(dict(zip(HEADER, fields)))
    return rows


def write_budget(rows, out):
    order = {"ci": 0, "laptop": 1}
    rows = sorted(
        rows, key=lambda r: (order.get(r["where"], 2), r["job"], r["step"] != JOB_STEP, r["step"])
    )
    out.write(
        "# The step-time budget (VARKA-255.md 3.6), written by dev/varka_ci_times.py collect.\n"
    )
    out.write("\t".join(HEADER) + "\n")
    for r in rows:
        out.write("\t".join(r[h] for h in HEADER) + "\n")


def replace_rows(old, new):
    keys = {(r["where"], r["job"], r["step"]) for r in new}
    return [r for r in old if (r["where"], r["job"], r["step"]) not in keys] + new


# Comparing a run with the budget.


def compare_rows(budget, where, measured):
    """[(job, step, secs, budget row or None, marked)] for one run's (job, step) -> secs."""
    index = {(r["job"], r["step"]): r for r in budget if r["where"] == where}
    out = []
    for (job, step), secs in sorted(measured.items()):
        row = index.get((job, step))
        limit = int(row["p90_s"]) if row is not None else 0
        marked = row is not None and secs > limit * MARGIN and secs - limit >= MIN_EXCESS
        out.append((job, step, secs, row, marked))
    return out


def render(compared, markdown):
    lines = []
    known = [c for c in compared if c[3] is not None]
    marked = [c for c in known if c[4]]
    if markdown:
        lines.append(MARKER)
        lines.append("### Step times against the budget")
        lines.append("")
        lines.append(
            "%d of %d steps are in `sql/varka/ci/step-budget.tsv`; %d took more than 5%% and 30 s "
            "over its 90th percentile (marked), which a pull request that adds test time says in "
            "its description. A mark is not a failure." % (len(known), len(compared), len(marked))
        )
        lines.append("")
        lines.append("| job | step | s | budget median | budget p90 | runs | |")
        lines.append("| --- | --- | ---: | ---: | ---: | ---: | --- |")
        for job, step, secs, row, mark in known:
            lines.append(
                "| %s | %s | %d | %s | %s | %s | %s |"
                % (
                    job,
                    step,
                    secs,
                    row["median_s"],
                    row["p90_s"],
                    row["runs"],
                    "**above**" if mark else "",
                )
            )
    else:
        lines.append("%-40s %-50s %6s %7s %6s %5s" % ("job", "step", "s", "median", "p90", "runs"))
        for job, step, secs, row, mark in compared:
            if row is None:
                lines.append(
                    "%-40s %-50s %6d %7s %6s %5s  (not in the budget)"
                    % (job[:40], step[:50], secs, "-", "-", "-")
                )
            else:
                lines.append(
                    "%-40s %-50s %6d %7s %6s %5s%s"
                    % (
                        job[:40],
                        step[:50],
                        secs,
                        row["median_s"],
                        row["p90_s"],
                        row["runs"],
                        "  ABOVE" if mark else "",
                    )
                )
        lines.append("%d marked of %d in the budget" % (len(marked), len(known)))
    return "\n".join(lines) + "\n"


def measured_of(samples):
    return {key: values[0][0] for key, values in samples.items()}


# Commands.


def cmd_collect(args, opener=urlopen):
    samples = {}
    for run_id in run_ids(args, opener):
        jobs = get_all(
            "/repos/%s/actions/runs/%d/jobs?per_page=100" % (args.repo, run_id), "jobs", opener
        )
        merge(samples, job_samples(jobs, args.jobs, tuple(args.conclusion or ["success"])))
    laptop = {}
    for directory in args.gate_dir or []:
        merge(laptop, gate_samples(directory, args.laptop_job))
    new = rows_from(samples, "ci") + rows_from(laptop, "laptop")
    rows = replace_rows(read_budget(args.budget), new)
    if args.out == "-":
        write_budget(rows, sys.stdout)
    else:
        with open(args.out or args.budget, "w") as f:
            write_budget(rows, f)
    print(
        "collect: %d CI and %d laptop steps written"
        % (len(rows_from(samples, "ci")), len(rows_from(laptop, "laptop"))),
        file=sys.stderr,
    )
    return 0


def run_ids(args, opener):
    if args.runs:
        return [int(r) for r in args.runs]
    if args.gate_dir and not (args.workflow or args.created):
        # Laptop directories alone: no runs to read.
        return []
    ids = []
    for workflow in args.workflow or ["varka-master.yml", "varka-fuzz.yml"]:
        query = "status=completed&per_page=100"
        if args.created:
            query += "&created=" + args.created
        ids.extend(
            r["id"]
            for r in get_all(
                "/repos/%s/actions/workflows/%s/runs?%s" % (args.repo, workflow, query),
                "workflow_runs",
                opener,
            )
        )
    return ids


def cmd_compare(args, opener=urlopen):
    jobs = get_all(
        "/repos/%s/actions/runs/%s/jobs?per_page=100" % (args.repo, args.run_id), "jobs", opener
    )
    # Every completed job of the run, failed ones too: a failed job's passing steps still say
    # what they cost.
    measured = measured_of(job_samples(jobs, None, ("success", "failure")))
    sys.stdout.write(render(compare_rows(read_budget(args.budget), "ci", measured), args.markdown))
    return 0


def cmd_gate(args):
    measured = measured_of(gate_samples(args.dir, args.laptop_job))
    sys.stdout.write(render(compare_rows(read_budget(args.budget), "laptop", measured), False))
    return 0


def main(argv):
    if argv[:1] == ["--self-test"]:
        import doctest

        failed, _ = doctest.testmod()
        result = unittest.main(argv=[sys.argv[0]], exit=False, verbosity=2).result
        return 1 if failed or not result.wasSuccessful() else 0
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("collect")
    c.add_argument("--repo", default="vecbricks/varka")
    c.add_argument("--workflow", action="append")
    c.add_argument("--created", help="GitHub's created filter, e.g. 2026-10-13 or A..B")
    c.add_argument("--runs", nargs="+")
    c.add_argument("--jobs", help="a regex the job's name must match")
    c.add_argument("--conclusion", action="append", help="job conclusions read (default success)")
    c.add_argument("--gate-dir", action="append")
    c.add_argument("--laptop-job", default="gate", choices=["gate", "nightly"])
    c.add_argument("--budget", default=DEFAULT_BUDGET)
    c.add_argument("--out", help="default: the budget file itself; - for stdout")
    m = sub.add_parser("compare")
    m.add_argument("run_id")
    m.add_argument("--repo", default="MaxGekk/spark")
    m.add_argument("--budget", default=DEFAULT_BUDGET)
    m.add_argument("--markdown", action="store_true")
    g = sub.add_parser("gate")
    g.add_argument("dir")
    g.add_argument("--laptop-job", default=None, choices=["gate", "nightly"])
    g.add_argument("--budget", default=DEFAULT_BUDGET)
    args = p.parse_args(argv)
    if args.cmd == "collect":
        return cmd_collect(args)
    if args.cmd == "compare":
        return cmd_compare(args)
    if args.laptop_job is None:
        args.laptop_job = "nightly" if "varka-nightly" in os.path.abspath(args.dir) else "gate"
    return cmd_gate(args)


# The tests (VARKA-255.md 5, test 9), over recorded shapes of the API's responses.


def step(name, start, end, conclusion="success"):
    return {"name": name, "started_at": start, "completed_at": end, "conclusion": conclusion}


def at(minute, second=0):
    return "2026-10-13T10:%02d:%02dZ" % (minute, second)


def suite_job(suites_s, option_s, conclusion="success"):
    # Times inside one hour: setup one minute, the suites, then the option step.
    return {
        "name": "Run / Varka suites: catalyst",
        "status": "completed",
        "conclusion": conclusion,
        "started_at": at(0),
        "completed_at": at(1 + (suites_s + option_s) // 60, (suites_s + option_s) % 60),
        "steps": [
            step("Set up job", at(0), at(1)),
            step("Run the Varka suites of catalyst", at(1), at(1 + suites_s // 60, suites_s % 60)),
            step(
                "The Varka suites of catalyst under one emit option configuration",
                at(1 + suites_s // 60, suites_s % 60),
                at(1 + (suites_s + option_s) // 60, (suites_s + option_s) % 60),
            ),
        ],
    }


class TimesTest(unittest.TestCase):
    SUITES = [480, 840, 1320, 600, 900]  # 8, 14, 22, 10, 15 minutes
    OPTION = [180, 360, 1380, 300, 420]

    def collected(self):
        samples = {}
        for s, o in zip(self.SUITES, self.OPTION):
            merge(samples, job_samples([suite_job(s, o)]))
        return {(r["job"], r["step"]): r for r in rows_from(samples, "ci")}

    def test_collect_takes_the_median_and_the_90th_percentile(self):
        rows = self.collected()
        suites = rows[("Varka suites: catalyst", "Run the Varka suites of catalyst")]
        self.assertEqual(
            (suites["runs"], suites["median_s"], suites["p90_s"]), ("5", "840", "1320")
        )
        option = rows[
            (
                "Varka suites: catalyst",
                "The Varka suites of catalyst under one emit option configuration",
            )
        ]
        self.assertEqual(option["median_s"], "360")
        whole = rows[("Varka suites: catalyst", JOB_STEP)]
        # Each job is a minute of setup, the suites and the option step: 720 to 2,760 s.
        self.assertEqual(whole["median_s"], "1260")
        self.assertEqual(suites["first"], "2026-10-13")

    def test_failed_jobs_and_steps_are_not_collected(self):
        samples = job_samples([suite_job(480, 180, conclusion="failure")])
        self.assertEqual(samples, {})
        job = suite_job(480, 180)
        job["steps"][2]["conclusion"] = "failure"
        names = {s for _, s in job_samples([job])}
        self.assertNotIn("The Varka suites of catalyst under one emit option configuration", names)

    def test_compare_marks_a_20_percent_slower_step_and_not_a_3_percent_one(self):
        budget = list(self.collected().values())
        key = ("Varka suites: catalyst", "Run the Varka suites of catalyst")
        p90_s = 1320
        slow = compare_rows(budget, "ci", {key: int(p90_s * 1.20)})
        near = compare_rows(budget, "ci", {key: int(p90_s * 1.03)})
        self.assertTrue(slow[0][4])
        self.assertFalse(near[0][4])
        # 20% over the median is inside this step's spread, and is not marked either.
        self.assertFalse(compare_rows(budget, "ci", {key: int(840 * 1.20)})[0][4])

    def test_a_short_step_is_not_marked_for_a_few_seconds(self):
        budget = [
            {
                "where": "ci",
                "job": "J",
                "step": "s",
                "cpu": "-",
                "runs": "9",
                "median_s": "4",
                "p90_s": "4",
                "first": "-",
                "last": "-",
            }
        ]
        self.assertFalse(compare_rows(budget, "ci", {("J", "s"): 9})[0][4])
        self.assertTrue(compare_rows(budget, "ci", {("J", "s"): 40})[0][4])

    def test_a_step_missing_from_the_budget_is_listed_unmarked(self):
        out = compare_rows([], "ci", {("Varka docs checks", "new step"): 9})
        self.assertEqual(out, [("Varka docs checks", "new step", 9, None, False)])
        self.assertIn("not in the budget", render(out, False))

    def test_the_markdown_table_starts_at_the_marker(self):
        budget = list(self.collected().values())
        key = ("Varka suites: catalyst", "Run the Varka suites of catalyst")
        text = render(compare_rows(budget, "ci", {key: 2000}), True)
        self.assertTrue(text.startswith(MARKER))
        self.assertIn("**above**", text)

    def test_collect_rewrites_its_steps_and_keeps_the_rest(self):
        old = [
            {
                "where": "ci",
                "job": "J",
                "step": "s",
                "cpu": "-",
                "runs": "1",
                "median_s": "5",
                "p90_s": "5",
                "first": "-",
                "last": "-",
            },
            {
                "where": "laptop",
                "job": "gate",
                "step": "wide",
                "cpu": "-",
                "runs": "2",
                "median_s": "490",
                "p90_s": "498",
                "first": "-",
                "last": "-",
            },
        ]
        new = [dict(old[0], median_s="7", p90_s="7")]
        merged = replace_rows(old, new)
        self.assertEqual(
            sorted((r["job"], r["median_s"]) for r in merged), [("J", "7"), ("gate", "490")]
        )

    def test_gate_reads_secs_files(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            for name, secs in (("wide", "489"), ("lint", "199"), ("wide", "")):
                with open(os.path.join(d, name + ".secs"), "w") as f:
                    f.write(secs + "\n")
            with open(os.path.join(d, "lint.status"), "w") as f:
                f.write("ok\n")
            with open(os.path.join(d, "doc.secs"), "w") as f:
                f.write("250\n")
            with open(os.path.join(d, "doc.status"), "w") as f:
                f.write("FAILED\n")
            samples = gate_samples(d, "gate")
        self.assertEqual(
            sorted((k, v[0][0]) for k, v in samples.items()), [(("gate", "lint"), 199)]
        )

    def test_the_committed_budget_parses(self):
        if not os.path.exists(DEFAULT_BUDGET):
            self.skipTest("no budget beside this script")
        rows = read_budget(DEFAULT_BUDGET)
        self.assertTrue(rows)
        for r in rows:
            self.assertIn(r["where"], ("ci", "laptop"))
            self.assertLessEqual(int(r["median_s"]), int(r["p90_s"]))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
