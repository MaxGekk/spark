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
"""The arms of dev/varka_platform.sh read back (VARKA-272): one line per arm, the answers
compared across the arms, and the verdict.

    dev/varka_platform_report.py target/varka-platform/<run>
    dev/varka_platform_report.py --self-test

Each arm's directory holds what the script left: exit, seconds, run.log with the suite's
`platform:` line (the JVM's own flags) and its "history(ies) not reached" count, answers.tsv
(digest, repeat, key per compared batch), and after a crash an hs_err file; a `skipped` file
names an arm that did not run. An arm fails on a non-zero exit, a crash file, or a run.log with
no `platform:` line, which means the suite never reached its proof. The answers are compared with
the reference arm's - c2 where it ran, else the first arm - on the keys both wrote: a key whose
digests differ is a disagreement, printed with the key, which names the seed, the iteration and
the history to replay under both arms' flags. Exit status 1 when an arm failed or two arms
disagree.
"""

import re
import sys
import tempfile
from pathlib import Path

ARMS = ("int", "c1", "c2", "novec", "xcomp", "stress", "fastdebug")

NOT_REACHED = re.compile(r"(\d+) history\(ies\) not reached")

# How many disagreeing keys the report prints in full; the rest are counted.
SHOWN = 10


def read_answers(path):
    """`answers.tsv` as {(key, repeat): digest}; a line that is not three fields is skipped."""
    answers = {}
    if not path.exists():
        return answers
    for line in path.read_text(errors="replace").splitlines():
        parts = line.split("\t", 2)
        if len(parts) == 3:
            answers[(parts[2], parts[1])] = parts[0]
    return answers


def read_arm(directory):
    """One arm's directory as a dict: name, skipped, exit, seconds, facts, not_reached, crash,
    answers; `facts` is the suite's platform line without its prefix, or None."""
    arm = {
        "name": directory.name,
        "skipped": None,
        "exit": None,
        "seconds": None,
        "facts": None,
        "not_reached": None,
        "crash": [],
        "answers": {},
    }
    skipped = directory / "skipped"
    if skipped.exists():
        arm["skipped"] = skipped.read_text().strip()
        return arm
    for field in ("exit", "seconds"):
        path = directory / field
        if path.exists() and path.read_text().strip().lstrip("-").isdigit():
            arm[field] = int(path.read_text().strip())
    log = directory / "run.log"
    if log.exists():
        for line in log.read_text(errors="replace").splitlines():
            text = line.strip().lstrip("+ ").strip()
            if text.startswith("platform: "):
                arm["facts"] = text[len("platform: ") :]
            match = NOT_REACHED.search(text)
            if match:
                arm["not_reached"] = int(match.group(1))
    arm["crash"] = sorted(p.name for p in directory.glob("hs_err*")) + sorted(
        p.name for p in directory.glob("replay_*")
    )
    arm["answers"] = read_answers(directory / "answers.tsv")
    return arm


def failures(arm):
    """Why the arm failed, each as a phrase; empty for an arm that passed or was skipped."""
    if arm["skipped"] is not None:
        return []
    why = []
    if arm["exit"] is None:
        why.append("no exit status: the arm never finished")
    elif arm["exit"] != 0:
        why.append(f"exit {arm['exit']}")
    if arm["crash"]:
        why.append("crashed: " + ", ".join(arm["crash"]))
    if arm["facts"] is None:
        why.append("no platform line: the suite never reached its proof")
    return why


def compare(reference, other):
    """The other arm's answers against the reference's: (shared, differing keys, only in the
    reference, only in the other)."""
    shared = reference.keys() & other.keys()
    differing = sorted(k for k in shared if reference[k] != other[k])
    return (len(shared), differing, len(reference.keys() - shared), len(other.keys() - shared))


def facts_shown(facts):
    """The facts line cut to what tells the arms apart: the home's last directories and the
    flags, without the vm version."""
    if facts is None:
        return "-"
    parts = []
    for item in facts.split():
        if item.startswith("java.home="):
            parts.append("home=" + "/".join(item.split("/")[-3:]))
        elif not item.startswith("vm="):
            parts.append(item)
    return " ".join(parts)


def report(run):
    """Prints the report of the run under `run`; True when every arm passed and all agree."""
    directories = [d for d in sorted(run.iterdir()) if d.is_dir() and d.name != "build"]
    directories.sort(key=lambda d: ARMS.index(d.name) if d.name in ARMS else len(ARMS))
    arms = [read_arm(d) for d in directories]
    if not arms:
        print(f"no arm under {run}")
        return False
    ok = True
    print(f"{'arm':<10} {'status':<8} {'secs':>6} {'unreached':>9} {'answers':>8}  flags")
    for arm in arms:
        why = failures(arm)
        if arm["skipped"] is not None:
            status = "skipped"
        elif why:
            status = "FAILED"
            ok = False
        else:
            status = "ok"
        secs = "-" if arm["seconds"] is None else str(arm["seconds"])
        unreached = "-" if arm["not_reached"] is None else str(arm["not_reached"])
        print(
            f"{arm['name']:<10} {status:<8} {secs:>6} {unreached:>9} {len(arm['answers']):>8}"
            f"  {facts_shown(arm['facts'])}"
        )
        if arm["skipped"] is not None:
            print(f"           {arm['skipped']}")
        for phrase in why:
            print(f"           {phrase}")
    ran = [a for a in arms if a["skipped"] is None and a["answers"]]
    if len(ran) >= 2:
        reference = next((a for a in ran if a["name"] == "c2"), ran[0])
        print(f"answers against {reference['name']} ({len(reference['answers'])} keys):")
        for arm in ran:
            if arm is reference:
                continue
            shared, differing, only_ref, only_arm = compare(reference["answers"], arm["answers"])
            line = (
                f"  {arm['name']:<10} {shared} shared, {len(differing)} differ; "
                f"{only_ref} only in {reference['name']}, {only_arm} only in {arm['name']}"
            )
            print(line)
            if differing:
                ok = False
                for key, repeat in differing[:SHOWN]:
                    print(f"    DISAGREE (repeat {repeat}): {key}")
                if len(differing) > SHOWN:
                    print(f"    and {len(differing) - SHOWN} more")
            if shared == 0:
                print(f"    no key in common with {reference['name']}: nothing compared")
                ok = False
    elif len(ran) == 1:
        print(f"one arm with answers ({ran[0]['name']}): nothing to compare")
    print("verdict: " + ("every arm passed and the arms agree" if ok else "FAILED"))
    return ok


def write_arm(
    run,
    name,
    exit_status=0,
    facts="UseCompiler=true",
    not_reached=0,
    answers=(),
    crash=None,
    skipped=None,
):
    """A fixture arm directory for the self-test."""
    d = run / name
    d.mkdir()
    if skipped is not None:
        (d / "skipped").write_text(skipped)
        return
    (d / "exit").write_text(f"{exit_status}\n")
    (d / "seconds").write_text("12\n")
    lines = ["- the compiled test"]
    if facts is not None:
        lines.append(f"  + platform: java.home=/x/jdk vm=25 {facts} ")
    lines.append(f"  + {not_reached} history(ies) not reached ")
    (d / "run.log").write_text("\n".join(lines) + "\n")
    (d / "answers.tsv").write_text(
        "".join(f"{digest}\t{repeat}\t{key}\n" for key, repeat, digest in answers)
    )
    if crash:
        (d / crash).write_text("# A fatal error\n")


def self_test():
    """Four outcomes over fixture directories: agreement, a disagreement, a key one arm lacks,
    and a crashed arm; and an arm with no platform line fails."""
    base = [
        ("seed=1 iteration=0 history=cold", "1", "aa"),
        ("seed=1 iteration=1", "1", "bb"),
        ("seed=1 iteration=1", "2", "cc"),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        run = Path(tmp) / "agree"
        run.mkdir()
        write_arm(run, "c2", answers=base)
        write_arm(run, "int", facts="UseCompiler=false", answers=base)
        write_arm(run, "fastdebug", skipped="no fastdebug JDK")
        assert report(run), "agreeing arms must pass"

        run = Path(tmp) / "disagree"
        run.mkdir()
        write_arm(run, "c2", answers=base)
        changed = base[:1] + [("seed=1 iteration=1", "1", "ee")] + base[2:]
        write_arm(run, "novec", facts="EnableVectorSupport=false", answers=changed)
        assert not report(run), "a differing digest must fail"

        run = Path(tmp) / "missing"
        run.mkdir()
        write_arm(run, "c2", answers=base)
        write_arm(run, "c1", facts="TieredStopAtLevel=1", answers=base[:2])
        assert report(run), "a key one arm lacks is not a disagreement"
        shared, differing, only_ref, only_arm = compare(
            read_answers(run / "c2" / "answers.tsv"), read_answers(run / "c1" / "answers.tsv")
        )
        assert (shared, differing, only_ref, only_arm) == (2, [], 1, 0)

        run = Path(tmp) / "crashed"
        run.mkdir()
        write_arm(run, "c2", answers=base)
        write_arm(run, "stress", exit_status=134, answers=base[:1], crash="hs_err_42.log")
        assert not report(run), "a crashed arm must fail"
        arm = read_arm(run / "stress")
        assert arm["crash"] == ["hs_err_42.log"] and failures(arm)[0] == "exit 134", arm

        run = Path(tmp) / "unproven"
        run.mkdir()
        write_arm(run, "c2", facts=None, answers=base)
        assert not report(run), "an arm whose suite printed no platform line must fail"
    print("self-test: ok")
    return 0


def main(argv):
    if argv == ["--self-test"]:
        return self_test()
    if len(argv) != 1 or argv[0].startswith("-"):
        print(__doc__.strip(), file=sys.stderr)
        return 2
    return 0 if report(Path(argv[0])) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
