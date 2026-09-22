"""Figure 5: dividing eight nanosecond counts at once - the conversion form, three operations
where the machine converts long lanes to double lanes in one instruction (AVX-512), and the
fourteen-operation magic form that reads the bits as a double where it does not (AVX2)."""

from rough import Rough, finish

r = Rough(900, 760, seed=31)
r.text(40, 45, "hour(t) is t / 3 600 000 000 000, eight lanes at a time", size=26)
for i in range(8):
    r.rect(40 + i * 100, 80, 90, 44, fill="blue", label="t%d" % (i + 1), size=19)
r.text(40, 150, "8 longs in one 512-bit register", size=18, color="#868e96")

# Path A: the conversion form.
r.text(40, 210, "AVX-512: the conversion form, 3 vector ops", size=23)
for i, (lbl, fill) in enumerate([("to double", "grey"), ("divide", "green"), ("to long", "grey")]):
    x = 40 + i * 230
    r.rect(x, 240, 190, 64, fill=fill, label=lbl, size=21)
    if i:
        r.arrow(x - 38, 272, x - 5, 272)
r.text(740, 272, "= 8 hours", size=21)
r.note(40, 340, "vcvtqq2pd and back: one instruction each, and only AVX-512 has them", size=19)

# Path B: the magic form.
r.text(40, 430, "AVX2: no long-to-double instruction, so the magic form, 14 vector ops", size=23)
steps = [
    ("|t|", "grey"),
    ("OR 0x433...", "violet"),
    ("read as\ndouble, - 2^52", "violet"),
    ("divide", "green"),
    ("round,\nfix the floor", "violet"),
    ("mask,\nsign", "grey"),
]
for i, (lbl, fill) in enumerate(steps):
    x = 40 + i * 140
    r.rect(x, 460, 122, 74, fill=fill, label=lbl, size=17)
    if i:
        r.arrow(x - 17, 497, x - 4, 497, head=8)
r.text(40, 560, "= 8 hours, the same eight", size=19)
r.note(
    40,
    610,
    "for 0 <= u < 2^52, the bits of u OR 0x4330000000000000, read as a\n"
    "double, are exactly 2^52 + u: a conversion with no convert instruction.\n"
    "The same identity, backwards, turns the quotient into a long.",
    size=19,
)
r.text(
    40,
    730,
    "Both forms are exact for every dividend under 2^52; a day is 8.64e13 ns, under 2^47.",
    size=17,
    color="#868e96",
)

finish(r, "fig5-two-division-forms")
