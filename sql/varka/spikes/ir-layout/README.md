# IR layout spike (VARKA-291)

A throwaway harness that compares ways to store Varka IR nodes. The plan is
`plans/m7/VARKA-291.md`; read its sections 2 and 9.2 for the layouts and the predictions.

Steps 2 and 3 hold four of the arms, on JDK 25 with no preview features; step 4 adds three on the
early-access Valhalla JDK (`run-ea.sh`, below):

| arm | what it is |
|---|---|
| records | today's sealed `VarkaVectorIR` records, built by their constructors |
| layout A | FFM struct rows of 32 bytes: `kind, c0, c1, c2` ints, then `p0, p1` longs |
| layout B | FFM struct rows of 16 bytes: `head` (kind and packed scalars), then `c0, c1, c2` |
| layout C | layout A's six fields as `int[]` and `long[]` columns, the plain-array baseline |
| layout D | layout A's fields as one off-heap segment for each field (off-heap columns) |
| layout V16 | layout B's row and packing in a flat array of 16-byte value records (internal API) |
| layout V63 | a `long` a node (kind, three 19-bit child ids) in a flat array, scalars in `int[]` |
| value records | the real IR with `value` added to every record, generated at run time (arm 5) |

A, B and C hash-cons rows in an open-addressing `int[]` table of row ids, and spill what does not
fit a row (a list, or wide scalars) to an interned pool of ints. B refuses a scalar that does not
fit. C keeps layout A's packing, so it is the same 32 bytes a node, and shares the off-heap pool of
lists with the FFM arms.

## What it checks

Every graph goes through the same checks before any timing is read:

1. Records built by constructors equal `VarkaIrDescription.rebuild` of the description.
2. The distinct-node counts of the four arms match.
3. A small interval analysis (`Intervals`, `RowTransfer`, a stand-in for `VarkaRangeAnalysis`)
   gives the same facts on each arm: distinct nodes, a checksum, and the roots' bounds.
4. Each layout (A, B and C) round-trips through `toGraph` and `rebuild` back to equal records.

`EdgeCases` adds six synthetic graphs at the packing limits: the largest column and literal
indices, one index past B's limit (B must refuse), a long lane, the extremes of the wide scalars,
lists, and a node with three children.

The "one cold pass" line is a smoke timing, not a measurement: it varies from run to run, and the
arms run in one JVM in a fixed order, so a later arm benefits from the earlier ones' warm-up.
The measurement is step 5.

## Running

    sql/varka/spikes/ir-layout/run.sh                    # the committed graphs, 320 of them
    sql/varka/spikes/ir-layout/run.sh --graphs DIR       # any directory of *.graphs files

`run.sh` compiles with plain `javac` into a temporary directory, so no build is needed. It reads
`JAVA_HOME` when set. The full corpus (4,456 graphs, 244,587 nodes) comes from
`VarkaIrLayoutExport` in `sql/catalyst`'s test tree.

## Graphs

`graphs/` holds samples small enough to commit: the size ladder, the make-date ladder, the cheap
tails, the deep chain, the 10,002-node ladder, 150 int and 150 long fuzz graphs, and four wide
graphs of each lane. Between them they use all 37 node kinds.

## The early-access JDK

`run-ea.sh` needs `EA_JAVA_HOME`, a JDK built from the JEP 401 early-access branch (tested on
`27-jep401ea3+1-1`). Its first argument is the variant:

    EA_JAVA_HOME=/path/to/jdk run-ea.sh plain   # the IR's records as they are, the JDK constant
    EA_JAVA_HOME=/path/to/jdk run-ea.sh value   # the same file with `value` on every record

`src-ea/` holds what only that JDK compiles (`RowsV16`, `RowsV63` and `EaLayouts`, which registers
them), so `run.sh` on JDK 25 never sees it. The harness checks that the variant it was told is what
the classes are (`Class.isValue`), and each V layout's constructor requires `isFlatArray`, so a
row the VM did not flatten fails instead of being measured as if it were.

Value records crash C2 of this build on the full corpus (`results/step4-ea-value-crash.txt`), so
the value variant runs there with `JVM_OPTS=-XX:-DoEscapeAnalysis`. See plan section 9.5.

## The measurement

`bench.sh VARIANT MODE OUTFILE` runs `IrLayoutBench`, one arm, one graph and one measure in each
JVM, pinned to the fastest cores and refusing to start on a busy machine (load above 0.8). The
variants are `jdk25`, `plain` and `value` as above; the modes are `cold` (fresh JVMs), `warm`
(two-second iterations) and `bytes`. The measures are `build`, `intern` (hash and equality of every
node) and `analyze`. `summarize.py` turns `results/step5-*.txt` into the tables of plan section 9.6,
each arm against the records on the same JDK, and against records that memoize by identity.

    sql/varka/spikes/ir-layout/bench.sh jdk25 cold results/step5-cold-jdk25.txt
    sql/varka/spikes/ir-layout/summarize.py

`vector-loops.sh OUTFILE` measures which layouts C2 and the Vector API can vectorize a per-row pass
over (`src-vector/`, needs `jdk.incubator.vector`), and `arrow-spike.sh OUTFILE` the cost of an
Arrow batch as the wire image or the store of layout D's columns (`src-arrow/`, needs Arrow jars:
see the script's header). `bench.sh` also has `alloc` (heap bytes and GC over 3,000 builds),
`--prewarm GRAPH --prewarm-rounds N` (a cold first compile after N rounds of warm-up) and the
`rebuild` measure.

