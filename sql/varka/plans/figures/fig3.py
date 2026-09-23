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

"""Figure 3: a batch is columns, not rows - Arrow buffers mapped as Panama memory segments."""

from rough import Rough, finish

r = Rough(900, 760, seed=5)
cols = [
    ("t", "blue", "TIME(6)"),
    ("t2", "blue", "TIME(6)"),
    ("dt", "violet", "interval"),
    ("dt2", "violet", "interval"),
    ("l", "orange", "bigint"),
    ("l2", "orange", "bigint"),
]
r.text(40, 45, "one Arrow batch of the varka_times table", size=28)
for i, (name, fill, kind) in enumerate(cols):
    x = 40 + i * 138
    r.text(x + 48, 95, name, size=26, anchor="middle")
    for j in range(8):
        r.rect(x, 120 + j * 34, 96, 30, fill=fill)
    r.rect(x + 104, 120, 16, 266, fill="grey")
    r.text(x + 48, 415, kind + "\n8 bytes", size=17, anchor="middle")
    r.text(x + 112, 415, "1 bit\na row", size=14, anchor="middle", color="#868e96")
r.line(40, 470, 860, 470)
r.line(40, 455, 40, 470)
r.line(860, 455, 860, 470)
r.text(450, 500, "MemorySegment.ofAddress(buf.memoryAddress())", size=20, anchor="middle")
r.text(450, 528, ".reinterpret(rows * 8)", size=20, anchor="middle")
r.note(
    40, 580, "the loop reads the bytes where Arrow put them:\nno copy, no object, no JNI", size=21
)
r.rect(560, 590, 200, 48, fill="yellow", label="UnsafeRow", size=20)
r.text(
    660,
    665,
    "what stock codegen loops over:\none object, six fields, one at a time",
    size=17,
    anchor="middle",
    color="#868e96",
)

finish(r, "fig3-batch-is-columns")
