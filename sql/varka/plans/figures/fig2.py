"""Figure 2: what hour(t) costs stock Spark - a LocalTime built per row to read one field -
against the one division Varka's lane does."""

from rough import Rough, finish

r = Rough(1320, 520, seed=11)

# The value both sides start from.
r.rect(60, 210, 250, 60, fill="blue", label="t = 52 349 000 000 000", size=19)
r.text(185, 292, "nanoseconds since midnight, one long", size=16, anchor="middle")

# Stock path: up and over.
r.curve([(310, 225), (380, 140), (450, 120)], arrow=True)
r.rect(455, 60, 330, 130, fill="yellow")
r.text(620, 85, "LocalTime.ofNanoOfDay(t)", size=21, anchor="middle")
for i, ln in enumerate(["hour = 14", "minute = 32", "second = 29", "nano = 0"]):
    r.text(500 + (i % 2) * 150, 120 + (i // 2) * 30, ln, size=17)
r.text(620, 175, "a new object", size=15, anchor="middle", color="#868e96")
r.arrow(790, 125, 880, 125)
r.rect(885, 95, 140, 60, fill="grey", label=".getHour()", size=19)
r.arrow(1030, 125, 1110, 125)
r.rect(1115, 95, 90, 60, fill="green", label="14", size=22)
r.note(455, 215, "built, read once, thrown away -\n16.3 ns a row on the laptop", size=18)

# Varka path: straight across.
r.curve([(310, 255), (380, 330), (450, 350)], arrow=True)
r.rect(455, 320, 330, 60, fill="green", label="t / 3 600 000 000 000", size=21)
r.arrow(790, 350, 1110, 350)
r.rect(1115, 320, 90, 60, fill="green", label="14", size=22)
r.note(455, 420, "one 64-bit division, 8 lanes at a time -\n1.0 ns a row, and no object", size=18)
r.text(
    660,
    470,
    "same answer, one path allocates and one does arithmetic",
    size=17,
    anchor="middle",
    color="#868e96",
)

finish(r, "fig2-localtime-per-row")
