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
# One capture of the repository's traffic, into sql/varka/traffic/<timestamp>.json.
#
#   dev/varka_traffic_snapshot.sh            # capture, and print the summary
#   dev/varka_traffic_snapshot.sh --print    # print the last capture's summary only
#
# GitHub keeps fourteen days of traffic and no more, so the before-and-after of a post
# exists only if someone took the before. Run this the hour before publishing anything and
# again a week later; sql/varka/traffic/README.md says which of the numbers is the one to
# read. Needs `gh` authenticated with access to the repository's traffic (push rights).
set -euo pipefail
trap 'echo "$(date -u +%FT%TZ) EXIT $?"' EXIT

repo=vecbricks/varka
here="$(cd "$(dirname "$0")/.." && pwd)"
out_dir="$here/sql/varka/traffic"

summarise() {
  python3 - "$1" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
v, c = d["views"], d["clones"]
print("taken     %s" % d["taken"])
print("views     %d in %d days, %d unique" % (v["count"], len(v["views"]), v["uniques"]))
print("clones    %d, %d unique" % (c["count"], c["uniques"]))
print("stars     %d" % d["repo"]["stars"])
print("referrers " + ", ".join("%s %d/%d" % (r["referrer"], r["count"], r["uniques"])
                               for r in d["referrers"][:8]))
PY
}

if [ "${1:-}" = --print ]; then
  latest="$(ls -1 "$out_dir"/*.json 2>/dev/null | sort | tail -1)"
  [ -n "$latest" ] || { echo "no snapshot in $out_dir" >&2; exit 1; }
  echo "$latest"; summarise "$latest"; exit 0
fi

mkdir -p "$out_dir"
stamp="$(date -u +%Y-%m-%dT%H%M%SZ)"
file="$out_dir/$stamp.json"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"; echo "$(date -u +%FT%TZ) EXIT $?"' EXIT

for ep in views clones popular/referrers popular/paths; do
  gh api "repos/$repo/traffic/$ep" > "$tmp/$(echo "$ep" | tr '/' '-').json"
done
gh api "repos/$repo" --jq '{stars: .stargazers_count, forks: .forks_count,
  watchers: .subscribers_count}' > "$tmp/repo.json"

python3 - "$tmp" "$file" "$stamp" "${2:-a routine capture}" <<'PY'
import json, sys
tmp, out, stamp, why = sys.argv[1:5]
doc = {"taken": stamp, "why": why}
for name, key in (("views", "views"), ("clones", "clones"),
                  ("popular-referrers", "referrers"), ("popular-paths", "paths")):
    doc[key] = json.load(open("%s/%s.json" % (tmp, name)))
doc["repo"] = json.load(open(tmp + "/repo.json"))
open(out, "w").write(json.dumps(doc, indent=2) + "\n")
PY

echo "$file"
summarise "$file"
