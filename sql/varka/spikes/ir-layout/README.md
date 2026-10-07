# IR layout spike (VARKA-291)

A throwaway harness that compares ways to store Varka IR nodes. The plan is
`plans/m7/VARKA-291.md`; read its sections 2 and 9.2 for the layouts and the predictions.

Steps 2 and 3 hold four of the arms, on JDK 25 with no preview features:

| arm | what it is |
|---|---|
| records | today's sealed `VarkaVectorIR` records, built by their constructors |
| layout A | FFM struct rows of 32 bytes: `kind, c0, c1, c2` ints, then `p0, p1` longs |
| layout B | FFM struct rows of 16 bytes: `head` (kind and packed scalars), then `c0, c1, c2` |
| layout C | layout A's six fields as `int[]` and `long[]` columns, the plain-array baseline |

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

The "one cold pass" line is a smoke timing, not a measurement. The measurement is step 5.

## Running

    sql/varka/spikes/ir-layout/run.sh                    # the committed graphs, 320 of them
    sql/varka/spikes/ir-layout/run.sh --graphs DIR       # any directory of *.graphs files

`run.sh` compiles with plain `javac` into a temporary directory, so no build is needed. It reads
`JAVA_HOME` when set. The full corpus (4,456 graphs, 244,587 nodes) comes from
`VarkaIrLayoutExport` in `sql/catalyst`'s test tree.

## Graphs

`graphs/` holds samples small enough to commit: the size ladder, the make-date ladder, the cheap
tails, the deep chain, 150 int and 150 long fuzz graphs, and four wide graphs of each lane. Between
them they use all 37 node kinds.
