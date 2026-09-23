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

"""Figure 4: one emitted class per projection, named after its plan node, and the ghost
fallback that hands a refused batch to the row engine."""

from rough import Rough, finish

r = Rough(900, 720, seed=23)

# The pipeline across the top.
r.rect(30, 70, 170, 90, fill="grey", label="Project\nhour(t), t + dt", size=19)
r.text(115, 185, "the plan node", size=16, anchor="middle", color="#868e96")
r.arrow(205, 115, 240, 115)
r.rect(245, 70, 170, 90, fill="violet", label="Varka\ncompiler", size=20)
r.text(330, 185, "expressions to\na small vector IR", size=16, anchor="middle", color="#868e96")
r.arrow(420, 115, 455, 115)
r.rect(460, 50, 250, 130, fill="green")
r.text(585, 75, "emitted class", size=22, anchor="middle")
r.text(585, 105, "Class-File API, JDK 25", size=17, anchor="middle")
r.text(585, 132, "SourceFile: Project#12", size=17, anchor="middle")
r.text(585, 159, "VarkaDebugInfo: the IR", size=17, anchor="middle")
r.text(
    585,
    205,
    "one class per projection shape,\nloaded with the task, unloaded with it",
    size=16,
    anchor="middle",
    color="#868e96",
)
r.arrow(715, 115, 750, 115)
r.rect(755, 70, 120, 90, fill="orange", label="C2", size=26)
r.text(815, 185, "one loop,\nmonomorphic", size=16, anchor="middle", color="#868e96")

# Run time: batches through the kernel, and the trapdoor.
r.text(30, 290, "at run time, batch by batch:", size=24)
for i in range(3):
    r.rect(30 + i * 95, 325, 82, 44, fill="blue", label="batch %d" % (i + 1), size=17)
r.arrow(320, 347, 355, 347)
r.rect(360, 305, 300, 90, fill="green", label="the kernel:\n8 lanes a step", size=20)
r.arrow(665, 347, 700, 347)
for i in range(2):
    r.rect(705 + i * 90, 325, 78, 44, fill="green", label="out %d" % (i + 1), size=17)
r.line(420, 396, 600, 396, width=3.5, color="#e03131")
r.text(510, 425, "the trapdoor", size=18, anchor="middle", color="#e03131")
r.arrow(510, 440, 510, 490, color="#e03131")
r.rect(360, 495, 300, 60, fill="yellow", label="stock Spark's row engine", size=20)
r.arrow(665, 525, 700, 525, color="#e03131")
r.rect(705, 503, 78, 44, fill="green", label="out", size=17)
r.note(
    30,
    610,
    "a batch the kernel refuses - a sum past midnight, a shape it cannot\n"
    "compile - is recomputed by the row engine. The query never fails,\n"
    "and the other batches never notice.",
    size=20,
)

finish(r, "fig4-one-class-per-projection")
