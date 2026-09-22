"""Figure 6: whole-projection fusion - two outputs over t + dt share the sum, and the chain
becomes one loop with one load per column and one store per output."""

from rough import Rough, finish

r = Rough(900, 1000, seed=41)
r.text(40, 40, "SELECT time_trunc('MINUTE', t + dt), hour(t + dt)", size=25)

# The expression graph, roots at the top.
r.text(250, 95, "store: time_trunc", size=19, anchor="middle")
r.text(650, 95, "store: hour", size=19, anchor="middle")
r.arrow(250, 150, 250, 112)
r.arrow(650, 150, 650, 112)
r.rect(150, 155, 200, 60, fill="green", label="* 60e9", size=23)
r.rect(550, 155, 200, 60, fill="grey", label="narrow to int", size=21)
r.arrow(250, 270, 250, 220)
r.arrow(650, 270, 650, 220)
r.rect(150, 275, 200, 60, fill="green", label="/ 60e9", size=23)
r.rect(550, 275, 200, 60, fill="green", label="/ 3600e9", size=23)
r.arrow(400, 395, 270, 340)
r.arrow(500, 395, 630, 340)
r.rect(350, 400, 200, 60, fill="orange", label="t + dt", size=24)
r.text(575, 480, "guard: still inside the day?", size=16, color="#868e96")
r.note(40, 420, "computed once,\nkept in a register,\nused by both outputs", size=19)
r.arrow(300, 545, 400, 465)
r.arrow(600, 545, 500, 465)
r.rect(200, 550, 180, 56, fill="blue", label="t", size=24)
r.rect(520, 550, 180, 56, fill="violet", label="dt", size=24)
r.text(450, 640, "one load per input column", size=18, anchor="middle", color="#868e96")

# The loop it becomes.
r.rect(40, 680, 820, 300, width=2.2)
r.text(450, 710, "the one emitted loop", size=24, anchor="middle")
lines = [
    "for (i = 0; i < rows; i += 8) {",
    "  a = load(t, i);  b = load(dt, i)",
    "  s = a + b                       // shared",
    "  store(out0, i, s / 60e9 * 60e9)",
    "  store(out1, i, (s / 3600e9).narrow())",
    "}",
]
for i, ln in enumerate(lines):
    r.text(70, 750 + i * 34, ln, size=20)
r.note(560, 800, "one body,\neight rows a turn,\nno call per row", size=19)

finish(r, "fig6-fusion-shared-subtree")
