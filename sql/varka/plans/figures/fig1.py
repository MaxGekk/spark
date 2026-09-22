"""Figure 1: stock Spark's row-at-a-time loop against Varka's one vector loop over a column."""

from rough import Rough, finish

r = Rough(900, 1140, seed=7)

# Top: stock Spark.
r.text(40, 45, "Stock Spark: one row at a time", size=30)
for i in range(6):
    r.rect(40, 90 + i * 40, 150, 34, fill="yellow", label="row %d" % (i + 1), size=20)
r.text(115, 345, "...", size=28, anchor="middle")
r.rect(270, 105, 330, 220, fill="grey")
r.text(435, 140, "generated Java", size=24, anchor="middle")
code = ["for (row : batch) {", "  LocalTime.of(...)", "     .getHour()", "  allocate; store", "}"]
for i, ln in enumerate(code):
    r.text(300, 178 + i * 32, ln, size=20)
for i in range(3):
    r.arrow(195, 107 + i * 40, 265, 150 + i * 30)
for i in range(6):
    r.rect(680, 90 + i * 40, 110, 34, fill="yellow", label="h", size=20)
    if i < 3:
        r.arrow(605, 150 + i * 30, 675, 107 + i * 40)
r.note(40, 400, "one object per row, and one lane of the core's eight used", size=21)
r.text(40, 465, "the 512-bit register:", size=21)
for i in range(8):
    r.rect(40 + i * 82, 490, 74, 46, fill="orange" if i == 0 else None)
r.text(40, 565, "1 of 8 lanes busy", size=20, color="#6741d9")

r.line(40, 610, 860, 610, dash="10 10", width=1.2)

# Bottom: Varka.
Y = 660
r.text(40, Y, "Varka: one loop over the column", size=30)
for i in range(8):
    r.rect(40, Y + 40 + i * 38, 130, 32, fill="blue", label="t%d" % (i + 1), size=18)
r.text(105, Y + 365, "Arrow column t,\n8 bytes a row", size=18, anchor="middle")
r.rect(250, Y + 60, 380, 270, fill="green")
r.text(440, Y + 95, "emitted vector loop", size=24, anchor="middle")
r.text(440, Y + 130, "load 8 lanes", size=20, anchor="middle")
r.text(440, Y + 160, "divide by 3600e9", size=20, anchor="middle")
for i in range(8):
    r.rect(272 + i * 42, Y + 185, 36, 40, fill="orange")
r.text(440, Y + 260, "8 rows per instruction", size=20, anchor="middle")
r.text(440, Y + 292, "store 8 lanes", size=20, anchor="middle")
r.arrow(175, Y + 190, 245, Y + 190)
for i in range(8):
    r.rect(700, Y + 40 + i * 38, 110, 32, fill="green", label="h%d" % (i + 1), size=18)
r.arrow(635, Y + 190, 695, Y + 190)
r.text(755, Y + 365, "hour(t),\nan int column", size=18, anchor="middle")
r.note(250, Y + 400, "no object per row, one store per output, all 8 lanes busy", size=21)

finish(r, "fig1-row-loop-vs-vector-loop")
