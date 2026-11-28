# Measurements, not adjectives

Any performance claim in code comments, plans or docs traces to a committed
benchmark result file. The methodology is fixed (`sql/varka/plans/m2/VARKA-14.md` 2.1): at least
five iterations over two-second windows, generated on an otherwise idle machine,
and any claim resting on a ratio under ~1.3x is re-run and compared by minimums
before it is written down. When a change moves committed numbers, regenerate the
affected results files in one run rather than patching quoted figures case by
case, and requote the docs from that run.

The tooling for that lives in `dev/`. `dev/varka_bench_regen.sh <module> <Class>`
regenerates one benchmark at both vector widths, refuses a busy machine, writes
the narrow run to a committed `-128bit-results.txt` companion and a provenance
sidecar (commit, JDK, kernel, CPU, load), and ends by diffing the wide file
against the committed one. `dev/varka_bench_diff.py` is that diff on its own:
before/after between two files or against a git revision, or A/B pairs inside
one file (`--within FILE --ab "label A" "label B"`), controls listed first.
Read the controls before anything else: a run whose scalar anchors are flat but
whose earliest sections all dropped was started on a machine still hot from a
test suite, and its numbers are the machine's, not the code's.

A diff's "moved 14.2%" means nothing on its own, because an unchanged file moves
too, and by a different amount per case: over ten runs the parity file's median
case moves 5%, its worst 227%. So a family's moves are read against its *band*,
a committed per-case tier (`<Class>-jdk25-band.txt` beside the results, from
`dev/varka_bench_band.py`) that the regen script's diff picks up by itself.
The rule for when a family gets one: **the first time someone has to read a
move in it**, not before. Then `dev/varka_bench_repeat.sh <module> <Class> 10
--band` runs the unchanged benchmark ten times on the quiet machine and writes
the band, `--narrow` the same for the 128-bit companion, and both are committed
with the results they band. Until a family has one the flat threshold applies,
and a move read against it is read with that said.

Two more guards. `dev/varka_bench_canary.sh` runs three fixed loops - a
compute-bound control, a cache-resident vector add, a memory-bound one - and
compares them with the committed baseline for this host; the regen script runs
it first and stops when the machine is not in its baseline state, because the
same code has measured its memory-bound kernels 20-27% apart on different days
with every compute-bound row flat. And `dev/varka_quote_check.py` checks that
every number with decimals quoted in the plans, the lesson files, the docs and the
README appears in a committed results file, now or in its history; the ones
that predate the tool are in `dev/varka_quote_allowlist.txt` with a reason
each, and that list only shrinks. It is a step of the gate.

A regeneration ends with the requote: `dev/varka_bench_diff.py --git HEAD <file>
--requote` lists, under each moved row, every document line that quotes its old
number, and the regeneration is done when that list is empty or every line left
says on purpose that it quotes the number a change moved away from. The checks
that need volume run from `dev/varka_nightly.sh` - the canary, the fuzzer at ten
thousand iterations with the day's seed, the exhaustive sweeps, the guard against
the C2 deoptimization cycle, optionally the gate - into a dated log under
`target/varka-nightly/`.
