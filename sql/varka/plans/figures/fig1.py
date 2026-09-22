"""Figure 1: stock Spark's row-at-a-time loop against Varka's one vector loop over a column."""

from rough import Rough, finish

r = Rough(1320, 600, seed=7)

# Left: stock Spark.
r.text(60, 50, "Stock Spark: one row at a time", size=30)
for i in range(6):
    r.rect(60, 110 + i * 34, 150, 28, fill="yellow", label="row %d" % (i + 1), size=17)
r.text(135, 325, "...", size=26, anchor="middle")
r.rect(300, 130, 260, 200, fill="grey")
r.text(430, 165, "generated Java", size=22, anchor="middle")
for i, ln in enumerate(
    ["for (row : batch) {", "  LocalTime.of(...)", "   .getHour()", "  ; allocate ; store", "}"]
):
    r.text(430, 200 + i * 29, ln, size=18, anchor="middle")
for i in range(3):
    r.arrow(215, 124 + i * 34, 295, 160 + i * 20)
for i in range(6):
    r.rect(600, 110 + i * 34, 90, 28, fill="yellow", label="h", size=17)
    if i < 3:
        r.arrow(565, 160 + i * 20, 595, 124 + i * 34)
r.note(70, 380, "one object per row,\none lane of the core's 8 used", size=19)
r.text(70, 460, "the 512-bit register:", size=19)
for i in range(8):
    r.rect(70 + i * 44, 480, 40, 34, fill="orange" if i == 0 else None)
r.text(70, 545, "1 of 8 lanes busy", size=17, color="#6741d9")

r.line(705, 40, 705, 570, dash="8 8", width=1.2)

# Right: Varka.
X = 760
r.text(X, 50, "Varka: one loop over the column", size=30)
for i in range(8):
    r.rect(X, 100 + i * 30, 110, 26, fill="blue", label="t%d" % (i + 1), size=15)
r.text(X + 55, 355, "Arrow column t\n(8 bytes a row)", size=17, anchor="middle")
r.rect(X + 190, 120, 220, 210, fill="green")
r.text(X + 300, 150, "emitted vector loop", size=22, anchor="middle")
r.text(X + 300, 180, "load 8 lanes", size=18, anchor="middle")
r.text(X + 300, 208, "divide by 3600e9", size=18, anchor="middle")
for i in range(8):
    r.rect(X + 205 + i * 24, 235, 20, 24, fill="orange")
r.text(X + 300, 285, "8 rows per instruction", size=18, anchor="middle")
r.text(X + 300, 312, "store 8 lanes", size=18, anchor="middle")
r.arrow(X + 115, 220, X + 185, 220)
for i in range(8):
    r.rect(X + 440, 100 + i * 30, 60, 26, fill="green", label="h%d" % (i + 1), size=15)
r.arrow(X + 410, 220, X + 435, 220)
r.text(X + 470, 355, "hour(t)\nint column", size=17, anchor="middle")
r.note(X + 10, 420, "no object per row,\none store per output,\nall 8 lanes busy", size=19)

finish(r, "fig1-row-loop-vs-vector-loop")
