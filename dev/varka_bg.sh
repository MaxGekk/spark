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
# Long jobs in the background, waited on and stopped by the PID recorded when they started, never
# by a name pattern.
#
#   dev/varka_bg.sh start NAME CMD [ARGS...]   # start CMD in a session of its own
#   dev/varka_bg.sh status NAME                # running, or exited with its status
#   dev/varka_bg.sh wait NAME [SECONDS]        # until it exits; its status, or 124 past SECONDS
#   dev/varka_bg.sh stop NAME                  # TERM to its whole group, KILL a minute later
#   dev/varka_bg.sh list                       # every job under the state directory
#
# A job's state lives in $VARKA_BG_DIR (default target/bg) of the checkout: NAME.pid holds the
# PID and its start time from /proc, NAME.log the job's output, NAME.exit its exit status once it
# ends. The start time guards against a reused PID: a PID whose start time differs is not the job.
# The job runs under setsid, so its PID is its process group and `stop` reaches everything it
# started and nothing else.
#
# Why it exists: pkill -f and pgrep -f match whole command lines, and a pattern matches more than
# meant - the waiting shell itself, or every JVM whose flags hold the word. On 13 October 2026 a
# pattern holding "error" stopped three test JVMs of other worktrees (sql/varka/AGENTS.md,
# "Processes").
set -uo pipefail
cd "$(dirname "$0")/.."
DIR=${VARKA_BG_DIR:-$(pwd)/target/bg}
usage() { sed -n '18,/^[^#]/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit "${1:-2}"; }

name_ok() {
  [[ "${1:-}" =~ ^[A-Za-z0-9._-]+$ ]] || {
    echo "a job name is letters, digits, . _ -" >&2; exit 2; }
}

# The start time of a PID, field 22 of /proc/PID/stat, or empty when no such process exists.
started() {
  local stat
  stat=$(cat "/proc/$1/stat" 2>/dev/null) || return 0
  # The command name, field 2, may hold spaces; the fields after its closing parenthesis do not.
  stat=${stat##*) }
  echo "$stat" | awk '{print $20}'
}

# Whether the recorded job is still the process it was: same PID, same start time.
alive() {
  local pid when
  read -r pid when < "$DIR/$1.pid" || return 1
  [ -n "$pid" ] && [ "$(started "$pid")" = "$when" ]
}

cmd=${1:-}; shift || true
case "$cmd" in
  start)
    name_ok "${1:-}"; name=$1; shift
    [ "$#" -gt 0 ] || { echo "no command to run" >&2; exit 2; }
    mkdir -p "$DIR"
    if [ -f "$DIR/$name.pid" ] && alive "$name"; then
      echo "$name is already running (pid $(cut -d' ' -f1 "$DIR/$name.pid"))" >&2; exit 1
    fi
    rm -f "$DIR/$name.exit"
    # The wrapper records the exit status itself, so `wait` reads a file rather than guessing.
    setsid bash -c '"$@"; echo $? > "$0"' "$DIR/$name.exit" "$@" > "$DIR/$name.log" 2>&1 \
      < /dev/null &
    pid=$!
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      when=$(started "$pid"); [ -n "$when" ] && break; sleep 0.1
    done
    echo "$pid ${when:-}" > "$DIR/$name.pid"
    echo "$name started: pid $pid, log $DIR/$name.log"
    ;;
  status)
    name_ok "${1:-}"; name=$1
    [ -f "$DIR/$name.pid" ] || { echo "$name: no such job" >&2; exit 2; }
    if [ -f "$DIR/$name.exit" ]; then echo "$name: exited $(cat "$DIR/$name.exit")"
    elif alive "$name"; then echo "$name: running (pid $(cut -d' ' -f1 "$DIR/$name.pid"))"
    else echo "$name: gone without an exit status (killed?)"; exit 1
    fi
    ;;
  wait)
    name_ok "${1:-}"; name=$1; limit=${2:-0}
    [ -f "$DIR/$name.pid" ] || { echo "$name: no such job" >&2; exit 2; }
    start=$SECONDS
    while [ ! -f "$DIR/$name.exit" ] && alive "$name"; do
      if [ "$limit" -gt 0 ] && [ $((SECONDS - start)) -ge "$limit" ]; then exit 124; fi
      sleep 5
    done
    [ -f "$DIR/$name.exit" ] || { echo "$name: gone without an exit status (killed?)" >&2; exit 1; }
    exit "$(cat "$DIR/$name.exit")"
    ;;
  stop)
    name_ok "${1:-}"; name=$1
    [ -f "$DIR/$name.pid" ] || { echo "$name: no such job" >&2; exit 2; }
    alive "$name" || { echo "$name is not running"; exit 0; }
    pid=$(cut -d' ' -f1 "$DIR/$name.pid")
    kill -TERM -- "-$pid" 2>/dev/null
    for _ in $(seq 60); do alive "$name" || break; sleep 1; done
    alive "$name" && kill -KILL -- "-$pid" 2>/dev/null
    echo "$name stopped"
    ;;
  list)
    [ -d "$DIR" ] || exit 0
    for f in "$DIR"/*.pid; do
      [ -e "$f" ] || continue
      "$0" status "$(basename "$f" .pid)"
    done
    ;;
  -h|--help) usage 0 ;;
  *) usage ;;
esac
