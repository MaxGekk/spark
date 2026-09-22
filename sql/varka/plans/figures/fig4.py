"""Figure 4: one emitted class per projection, named after its plan node, and the ghost
fallback that hands a refused batch to the row engine."""

from rough import Rough, finish

r = Rough(1320, 600, seed=23)

# The pipeline across the top.
r.rect(60, 80, 200, 90, fill="grey", label="Project\nhour(t), t + dt", size=18)
r.text(160, 195, "the Catalyst plan node", size=15, anchor="middle", color="#868e96")
r.arrow(265, 125, 335, 125)
r.rect(340, 80, 200, 90, fill="violet", label="Varka compiler", size=20)
r.text(
    440,
    195,
    "Catalyst expressions to\na small vector IR",
    size=15,
    anchor="middle",
    color="#868e96",
)
r.arrow(545, 125, 615, 125)
r.rect(620, 60, 260, 130, fill="green")
r.text(750, 85, "emitted class", size=21, anchor="middle")
r.text(750, 115, "Class-File API, JDK 25", size=16, anchor="middle")
r.text(750, 143, "SourceFile: Project#12", size=16, anchor="middle")
r.text(750, 170, "VarkaDebugInfo: the IR", size=16, anchor="middle")
r.text(
    750,
    215,
    "one class per projection shape,\nloaded with the task, unloaded with it",
    size=15,
    anchor="middle",
    color="#868e96",
)
r.arrow(885, 125, 955, 125)
r.rect(960, 80, 200, 90, fill="orange", label="C2", size=24)
r.text(
    1060,
    195,
    "the JIT sees one loop,\nmonomorphic calls",
    size=15,
    anchor="middle",
    color="#868e96",
)

# The batches flowing through the kernel, and the trapdoor.
r.text(60, 300, "at run time, batch by batch:", size=22)
for i in range(4):
    r.rect(60 + i * 90, 330, 76, 40, fill="blue", label="batch %d" % (i + 1), size=15)
r.arrow(425, 350, 495, 350)
r.rect(500, 300, 300, 100, fill="green", label="the kernel: 8 lanes a step", size=19)
r.arrow(805, 350, 875, 350)
for i in range(3):
    r.rect(880 + i * 90, 330, 76, 40, fill="green", label="out %d" % (i + 1), size=15)
# trapdoor under the kernel
r.line(560, 400, 740, 400, width=3, color="#e03131")
r.text(650, 425, "trapdoor", size=15, anchor="middle", color="#e03131")
r.curve([(650, 435), (650, 480)], arrow=True, color="#e03131")
r.rect(520, 485, 260, 60, fill="yellow", label="stock Spark's row engine", size=18)
r.arrow(785, 515, 900, 515, color="#e03131")
r.rect(905, 495, 60, 40, fill="green", label="out", size=15)
r.note(
    60,
    470,
    "a batch the kernel refuses - a sum past midnight,\na shape it cannot"
    " compile - is recomputed by\nthe row engine. The query never fails and\nthe other"
    " batches never notice.",
    size=17,
)

finish(r, "fig4-one-class-per-projection")
