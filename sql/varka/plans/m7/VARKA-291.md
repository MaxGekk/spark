# VARKA-291: the IR's storage, records against flat layouts

## 1. Where this came from

Row 291 of `m7/PLAN.md`, opened 6 October 2026 at the owner's request. Item 85 of `m8/SCOPE.md`
ends on a storage question: hold the IR's nodes as rows of flat arrays, in memory the client
supplies (jegg issue #92), instead of as the records of `VarkaVectorIR`. The owner then asked
whether the migration to jegg could come sooner, with "all Varka IRs as rows in flat arrays", and
whether Valhalla's value classes were worth an experiment. Item 85 records the probe that answered
the second question in part and the experiment that answers the rest (its "storage spike"), and
says the question is a measurement. This task is that measurement, scheduled in the milestone in
flight, with its plan written before any number is taken.

The decision it serves: whether any layout beats records by enough to justify losing the sealed
`switch` over the IR (item 85, "Where the nodes live"), and so whether the ports of rows 214 to 217
should build the IR through factory methods that a later representation could change under them.

## 2. The admission check, done

Checked on 6 October 2026, before any harness was written:

* **The IR builds outside sbt.** `VarkaVectorIR.java` imports only `java.util`, and plain `javac`
  compiles it alone to 45 classes. So the harness needs no Spark build, and it can run on any JDK.
* **The same file builds as value records.** With `value` added to its 37 record declarations it
  compiles on the early-access JDK `27-jep401ea3+1-1` (checksum verified). That is an arm that
  costs one `sed`: the real IR as value records, with nothing redesigned.
* **Nothing depends on a node's identity.** The one `IdentityHashMap` in the Varka code is over
  emitter `Label`s in `VarkaEmitterTestSupport`; no IR node is keyed or compared by identity, so
  value records change no behaviour the IR relies on.
* **A flat row of the width a node needs is not available through public APIs of a value class.**
  The probe table in item 85 (`isFlatArray` on the early-access build): a nullable value record
  flattens at 7 bytes and not at 8, and a 16-byte row flattens only as a null-restricted
  non-atomic array of a `@LooselyConsistentValue` class, through `jdk.internal`. What this would
  have rejected: arm 4 as first written, value-record rows of 16 bytes, which are not flat; it
  becomes the 63-bit packed row, which is, and the internal-API row as a labelled reference.
* **FFM struct rows work on JDK 25 without preview**, with a 16-byte row of opcode, payload and two
  child ids, bounds-checked, off heap or over an `int[]` (item 85).

Still to check, and the first step of the work: that a reflection over the 37 record kinds can
export any graph the compiler builds as a description (kind, payload components, child indices),
which every arm then builds from.

## 3. The design

### 3.1 The graphs, and one description for every arm

The compiler's graphs are Scala-built, so the harness cannot build them itself. An exporter in the
test tree, `VarkaIrLayoutExport`, walks the real IR by reflection over its record components and
writes each graph as a description: one line per node, its kind, its non-child components as the
payload, and its children as earlier lines' indices, shared subtrees written once. Reflection runs
once, outside any timing, and the description file is what every arm reads, so the arms build
exactly the same graphs.

The graphs: the shapes of `VarkaEmitCostCorpus` and the TPC queries through the compiler, and
projections grown to ten thousand nodes by composing the corpus's shapes over distinct inputs, the
size item 85 names as the benchmark's. The small shapes are committed; a ten-thousand-node graph
is regenerated from a seed.

### 3.2 The arms

All in `sql/varka/spikes/ir-layout/`, plain `javac` and `java`, outside the build:

1. **Records.** The real `VarkaVectorIR` built from the description through each record's canonical
   constructor, as the baseline.
2. **`int[]` columns**, jegg issue #92's layout: opcode, payload and child ids as columns indexed
   by node id, with the payload words and a side array for variable arity.
3. **FFM struct rows**, a `StructLayout` row with `static final` `VarHandle`s, in an `Arena` and
   over a heap array through `MemorySegment.ofArray`.
4. **Value-record rows on the early-access JDK**: the 63-bit packed row (opcode and up to three
   child ids in one word, about half a million nodes at most); and, labelled a reference, the
   16-byte row through the internal API, flat only there.
5. **The real IR as value records**, on the early-access JDK, the same file with `value` added.

### 3.3 What is measured, and how

For each arm and graph: build time from the description; bytes a node (records through
`Instrumentation.getObjectSize` over the whole graph, the flat layouts from their own sizes, and
whether an array is flat from `ValueClass.isFlatArray`, never from heap deltas, which were noisy);
structural hash and equality of every node, with hash-consing in the flat arms; one bottom-up
analysis, a `dayRange`-like interval pass over the graph; and the cold first run, a fresh JVM per
sample, at least twenty, with the first call's time, since the memory argument rests on the cold
compile and item 85 says the graphs are small. Warm times are five or more two-second iterations,
pinned as `dev/varka_bench_regen.sh` pins them, on an otherwise idle machine; a claim under 1.3x
is re-run and compared by minimums and read against a band (`sql/varka/AGENTS.md`).

### 3.4 Correctness first

A benchmark of wrong work measures nothing. Before a number is taken, every flat arm must agree
with the records: the number of distinct subtrees, the interval fact of every node, and a round
trip of each graph back to records whose `VarkaVectorIR.canonical` equals the original's.

### 3.5 What is deliberately unchanged

The IR, the compiler, the emitter and every test. No Varka main code is touched; the spike lives
beside the project, so the arms that need the preview JDK break nothing the build builds.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/src/test/.../VarkaIrLayoutExport.scala` | the exporter: the corpus and the grown projections as descriptions |
| `sql/varka/spikes/ir-layout/` | the harness for the five arms, a run script per JDK, a README |
| `sql/varka/spikes/ir-layout/results/` | each run's output with its provenance, and the bands |
| `m7/VARKA-291.md`, `m7/PLAN.md` | this plan, row 291 |

## 5. Tests, and what each is for

* The agreement check of 3.4 on every graph and every arm: the one that stops a layout winning by
  computing less.
* The exporter's round trip over the 37 kinds: every kind a graph can contain is described and
  rebuilt, so a kind the reflection misses fails the export, not a later arm.
* `isFlatArray` read for every array arm 4 claims is flat, so a layout the VM did not flatten is
  reported as it is.

## 6. The measurement

Arms against the measures of 3.3, on the corpus's shapes and the grown projections, at the two
sizes that matter: under a thousand nodes, where Varka's graphs are, and ten thousand.

### 6.1 Predictions, registered before the run

1. A flat layout is at least twice as small a node as a record, whose header and child references a
   row does not pay.
2. On a graph of a thousand nodes or fewer, every layout's time is within 1.3x of the records',
   either way, because the graphs are small; the gap, if there is one, opens past that and in
   hashing and equality, where ids replace a walk.
3. FFM rows cost more than `int[]` columns only where a `VarHandle` is not folded.
4. The real IR as value records (arm 5) is smaller a node than records by less than half, since
   the child references stay, and within 1.3x in time.

### 6.2 The gate

A layout is worth losing the sealed `switch` over the IR for only if it beats records by 1.3x or
more on the cold first compile of a representative projection, or by a factor that grows with
depth on hash and equality. A layout that is not flat, or that needs a `jdk.internal` class or the
preview, is a finding about a later JDK and not a candidate. If a layout passes, the next decision
is whose schema the rows are: jegg's, generic over opcode, payload and child ids, with Varka's IR a
client of it, or Varka's.

## 7. Risks

1. **A `VarHandle` that is not folded** makes FFM look slower than it is. The arm's accessors are
   `static final` and a variant with a non-constant handle is run beside it, so the cost is seen
   either way.
2. **The corpus is small.** Item 85 expects Varka's graphs to be small, which is the point; the
   grown projections are synthetic, built from the corpus's own shapes, and are labelled so.
3. **Different hash-consing in each arm.** One algorithm, open addressing over ids, in every flat
   arm, and the records' own `hashCode` and `equals` as they ship, so each arm is measured as it
   would be used.
4. **The early-access JDK is unfinished.** Its results are a reference for a later JDK, never a
   claim about Varka's; any arm that needs it is read that way.
5. **Cold-start samples are noisy.** At least twenty fresh JVMs, minimums and medians both, and the
   band of the records arm read first.

## 8. Sequencing

1. The exporter and its round trip over the 37 kinds, the first check of section 2.
2. Arms 1 and 2, the agreement check, and the harness's run script, on JDK 25.
3. Arm 3, on JDK 25.
4. Arms 4 and 5 on the early-access JDK, with the run script for it.
5. The measurement of section 6 on the quiet machine, the bands first.
6. Section 9, the gate's verdict, and item 85's "Where the nodes live" updated from it.

## 9. Outcome
