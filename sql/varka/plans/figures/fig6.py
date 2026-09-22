"""Figure 6: whole-projection fusion - two outputs over t + dt share the sum, and the chain
becomes one loop with one load per column and one store per output."""

from rough import Rough, finish

r = Rough(1320, 600, seed=41)
r.text(60, 45, "SELECT time_trunc('MINUTE', t + dt), hour(t + dt) FROM times", size=26)

# Columns at the bottom.
r.rect(200, 470, 140, 50, fill="blue", label="t", size=22)
r.rect(420, 470, 140, 50, fill="violet", label="dt", size=22)
r.text(380, 555, "one load per input column", size=17, anchor="middle", color="#868e96")

# The shared sum.
r.arrow(270, 465, 350, 400)
r.arrow(490, 465, 410, 400)
r.rect(300, 340, 160, 56, fill="orange", label="t + dt", size=22)
r.text(470, 405, "guard: still inside the day?", size=14, color="#868e96")
r.note(70, 360, "computed once,\nkept in a register,\nused by both outputs", size=17)

# Left branch: time_trunc.
r.arrow(340, 336, 250, 270)
r.rect(150, 210, 190, 56, fill="green", label="/ 60e9", size=21)
r.arrow(245, 206, 245, 160)
r.rect(150, 100, 190, 56, fill="green", label="* 60e9", size=21)
r.arrow(245, 96, 245, 70)
r.text(245, 60, "store: time_trunc", size=17, anchor="middle")

# Right branch: hour.
r.arrow(420, 336, 510, 270)
r.rect(420, 210, 190, 56, fill="green", label="/ 3600e9", size=21)
r.arrow(515, 206, 515, 160)
r.rect(420, 100, 190, 56, fill="grey", label="narrow to int", size=19)
r.arrow(515, 96, 515, 70)
r.text(515, 60, "store: hour", size=17, anchor="middle")

# The loop box on the right.
r.rect(760, 100, 480, 420, fill=None, width=2.2)
r.text(1000, 130, "the one emitted loop", size=24, anchor="middle")
for i, ln in enumerate(
    [
        "for (i = 0; i < rows; i += 8) {",
        "  a = load(t, i);  b = load(dt, i)",
        "  s = a + b            // shared",
        "  m = s / 60e9 * 60e9",
        "  h = (s / 3600e9).narrow()",
        "  store(out0, i, m)",
        "  store(out1, i, h)",
        "}",
    ]
):
    r.text(790, 175 + i * 34, ln, size=18)
r.note(790, 470, "no call per row, no call per output:\none body, eight rows a turn", size=16)

finish(r, "fig6-fusion-shared-subtree")
