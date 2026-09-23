"""Figure 10: what the twelve TIME chains cost per row on a full-width machine, against the
three arms they are compared with, and the same twelve on a machine without AVX-512."""

from rough import Rough, finish

r = Rough(900, 640, seed=83)
r.text(40, 40, "one chained TIME expression, nanoseconds a row", size=26)
r.text(
    40, 72, "the median of twelve, at 100 million cached rows, one core", size=17, color="#868e96"
)

# The full-width machine: four bars, log-ish by hand since stock is 34x Varka.
r.text(40, 120, "AMD EPYC 9V45, AVX-512, datapath 1.97", size=21)
bars = [
    ("stock 4.2, JDK 17", 154.1, "yellow"),
    ("stock 4.2, JDK 25", 142.9, "yellow"),
    ("the fork, engine off", 118.3, "grey"),
    ("Varka", 4.2, "green"),
]
scale = 620.0 / 160.0
for i, (label, ns, fill) in enumerate(bars):
    y = 150 + i * 46
    r.text(40, y + 17, label, size=17)
    w = max(6.0, ns * scale)
    r.rect(230, y, w, 34, fill=fill)
    r.text(230 + w + 12, y + 17, "%.1f" % ns, size=18)
r.note(
    230,
    360,
    "stock spends about 34 times as long on the same row,\nand most of that is"
    " machinery rather than arithmetic",
    size=18,
)

# The machine without AVX-512.
r.text(40, 450, "AMD EPYC 7763, avx2 only: the same twelve", size=21)
bars2 = [("stock 4.2, JDK 25", 251.3, "yellow"), ("Varka", 62.0, "green")]
scale2 = 620.0 / 260.0
for i, (label, ns, fill) in enumerate(bars2):
    y = 480 + i * 46
    r.text(40, y + 17, label, size=17)
    w = max(6.0, ns * scale2)
    r.rect(230, y, w, 34, fill=fill)
    r.text(230 + w + 12, y + 17, "%.1f" % ns, size=18)
r.text(
    40,
    600,
    "the scale differs between the two panels; the machines are 1.8x apart and Varka is 14.6x",
    size=16,
    color="#868e96",
)

finish(r, "fig10-the-chains")
