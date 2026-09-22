"""Figure 5: dividing eight nanosecond counts at once - the conversion form where the machine
converts long lanes to double lanes in one instruction (AVX-512), and the magic form that
reads the bits as a double where it does not (AVX2)."""

from rough import Rough, finish

r = Rough(1400, 640, seed=31)
r.text(60, 45, "hour(t) is t / 3 600 000 000 000, eight lanes at a time", size=28)

# The input vector.
for i in range(8):
    r.rect(60, 100 + i * 32, 130, 28, fill="blue", label="t%d" % (i + 1), size=15)
r.text(125, 375, "8 longs", size=16, anchor="middle")

# Path A: conversion form.
r.text(290, 90, "AVX-512: the conversion form, 7 vector ops", size=21)
steps_a = [("to double", "grey"), ("divide", "green"), ("to long", "grey")]
for i, (lbl, fill) in enumerate(steps_a):
    x = 290 + i * 190
    r.rect(x, 120, 150, 70, fill=fill, label=lbl, size=19)
    if i:
        r.arrow(x - 40, 155, x - 5, 155)
r.arrow(195, 155, 285, 155)
r.text(290 + 3 * 190 - 30, 155, "= 8 hours", size=19)
r.note(290, 225, "vcvtqq2pd and back: one instruction each,\nbut only AVX-512 has them", size=17)

# Path B: magic form.
r.text(290, 320, "AVX2: no long-to-double instruction, so the magic form, 14 vector ops", size=21)
steps_b = [
    ("|t|", "grey"),
    ("OR 0x433...", "violet"),
    ("read as double\n- 2^52", "violet"),
    ("divide", "green"),
    ("round, fix floor", "violet"),
    ("mask, sign", "grey"),
]
for i, (lbl, fill) in enumerate(steps_b):
    x = 290 + i * 165
    r.rect(x, 350, 140, 70, fill=fill, label=lbl, size=15)
    if i:
        r.arrow(x - 25, 385, x - 5, 385)
r.arrow(195, 385, 285, 385)
r.text(290 + 6 * 165 - 20, 385, "= 8 hours", size=19)
r.note(
    290,
    460,
    "for 0 <= u < 2^52, the bits of u OR 0x4330000000000000 read as a\n"
    "double are exactly 2^52 + u - a conversion with no convert instruction.\n"
    "The same identity, backwards, turns the quotient into a long.",
    size=17,
)
r.text(
    60,
    590,
    "Both forms are exact for every dividend under 2^52; a day is 8.64e13 nanoseconds, under 2^47.",
    size=17,
    color="#868e96",
)

finish(r, "fig5-two-division-forms")
