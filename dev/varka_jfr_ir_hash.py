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
#
# The share of JFR execution samples spent hashing or comparing Varka IR nodes (VARKA-222):
#
#   jfr print --events jdk.ExecutionSample --stack-depth 128 emit.jfr | dev/varka_jfr_ir_hash.py
#
# A sample counts as inside an IR hashCode (or equals) when any frame of its stack is a
# VarkaVectorIR record's; the caller is the first frame past the outermost such frame that is
# neither the IR nor java.util/java.lang.
import collections
import re
import sys

events = sys.stdin.read().split("jdk.ExecutionSample")[1:]
total = len(events)
in_hash = in_equals = 0
callers = collections.Counter()
for event in events:
    frames = re.findall(r"^\s+([\w.$<>]+)\(", event, re.M)
    hashing = [i for i, f in enumerate(frames) if re.search(r"VarkaVectorIR\$\w+\.hashCode", f)]
    comparing = [i for i, f in enumerate(frames) if re.search(r"VarkaVectorIR\$\w+\.equals", f)]
    in_hash += bool(hashing)
    in_equals += bool(comparing)
    if hashing or comparing:
        for f in frames[max(hashing + comparing) + 1 :]:
            if "VarkaVectorIR" not in f and "java.util" not in f and "java.lang" not in f:
                callers[f.rsplit(".", 2)[-2] + "." + f.rsplit(".", 1)[-1]] += 1
                break


def pct(n):
    return 100.0 * n / max(total, 1)


print(
    f"{total} samples; inside an IR hashCode {in_hash} ({pct(in_hash):.1f}%), "
    f"inside an IR equals {in_equals} ({pct(in_equals):.1f}%)"
)
for caller, count in callers.most_common(4):
    print(f"  {count:4d}  {caller}")
