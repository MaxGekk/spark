"""Figure 3: a batch is columns, not rows - Arrow buffers mapped as Panama memory segments."""

from rough import Rough, finish

r = Rough(1320, 560, seed=5)
cols = [
    ("t", "blue", "TIME(6)\n8 bytes"),
    ("t2", "blue", "TIME(6)\n8 bytes"),
    ("dt", "violet", "interval\n8 bytes"),
    ("dt2", "violet", "interval\n8 bytes"),
    ("l", "orange", "bigint\n8 bytes"),
    ("l2", "orange", "bigint\n8 bytes"),
]
r.text(60, 45, "one Arrow batch of the varka_times table", size=28)
for i, (name, fill, kind) in enumerate(cols):
    x = 60 + i * 150
    r.text(x + 50, 90, name, size=24, anchor="middle")
    # the data buffer: a tall strip of cells
    for j in range(8):
        r.rect(x, 110 + j * 30, 100, 26, fill=fill, size=12)
    r.text(x + 50, 365, kind, size=15, anchor="middle")
    # the validity bitmap: one bit a row, drawn as a thin strip
    r.rect(x + 108, 110, 14, 236, fill="grey")
    r.text(x + 115, 365, "1 bit\na row", size=12, anchor="middle", color="#868e96")
# Panama bracket under the whole thing
r.line(60, 430, 970, 430)
r.line(60, 415, 60, 430)
r.line(970, 415, 970, 430)
r.text(
    515,
    460,
    "MemorySegment.ofAddress(buf.memoryAddress()).reinterpret(rows * 8)",
    size=18,
    anchor="middle",
)
r.note(
    60, 500, "the loop reads the bytes where Arrow put them:\nno copy, no object, no JNI", size=18
)
# the row on the right, for contrast
r.text(1130, 90, "a row", size=24, anchor="middle")
r.rect(1050, 110, 160, 40, fill="yellow", label="UnsafeRow", size=17)
r.text(1130, 195, "one object,\nsix fields,\none at a time", size=16, anchor="middle")
r.arrow(1130, 235, 1130, 265)
r.text(1130, 295, "what stock codegen\nloops over", size=16, anchor="middle", color="#868e96")

finish(r, "fig3-batch-is-columns")
