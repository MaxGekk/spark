# Task 85: lane type as a parameter

*Milestone 5, section 2.16. Opened 8 September 2026 from task 63's review; planned
15 September 2026 as the second task on the milestone's spine, after 84 and
before 29.*

## 1. Where this came from

`VarkaLoopEmitter.java` is 5 924 lines and names `jdk.incubator.vector.IntVector`
205 times. The IR has a slot for a lane type - `enum LaneType { INT }` with a
default `laneType()` returning it - and one value in the slot; it is read exactly
once in the emitter, at line 1855, to throw on anything else. `ColumnRef(int
ordinal)` names a column and nothing more, and the emitter supplies int32 by
assumption at every load, store, broadcast, arithmetic op, comparison, blend and
address computation. That held while every node was int32; milestone 5's subject
is a second lane, and 28, 29, 88 and every `TIME` row sit behind this task.

Section 2.16 also asked for a forcing function before the refactor: three logical
types over one physical lane, to show the lane belongs to the node's physical
representation and not its Spark type. Task 67 delivered it - year-month intervals
are int32 months, the same lane as `DATE` and `INT` - so this task starts with its
example in hand and does not have to argue the point.

## 2. The admission check, done

**Where the 205 references actually are** - counted per method with a
brace-tracking scan, because the answer decides the design:

| region | refs | per-lane? |
| :--- | ---: | :--- |
| calendar kernels: `emitAddMonths` 20, `emitDaysFromCivil` 19, `emitChronoPrefix` 18, `emitMakeDate` 14, `emitFloorMod7` 13, `emitChronoLastDay` 10, `emitJulianYearOfEra` 6, `emitTruncQuarter` 6, `emitChronoYear`/`emitJanuaryDayOfYear`/`emitChronoTrunc` 5 each, `emitLeapFlag`/`emitEra`/`emitChronoMonth` 4 each, `emitFold`/`emitCarry` 3 each, and the small ones | ~140 | **no** - they consume an epoch-day lane, which is int32 by definition; at another lane they must not run, and a conversion node (task 28) is how a wider value reaches them |
| class-level descriptors (`INT_VECTOR`, the `FROM`/`INTO_MEMORY_SEGMENT` and `LANEWISE_*` and `BROADCAST` method types) | 12 | **yes** - these *are* the lane |
| `emitValue`'s `ColumnRef` load and `LiteralSlot` broadcast, `emitIntArith`, `emitIntNeg`, `emitPick`, `IfElse`'s blend | ~24 | **yes** - the generic node arms |
| `emitBody`, `emitLaneGroup`, `emitRangeGuard`, the species prologue (`speciesField`, `PREFERRED_LANES`, `emitLanes`), and three `LSHL 2` byte-offset sites (lines 2944, 3520, 4857) | ~10 | **yes** - species, lane count and stride |

So 2.16's estimate holds and is now a list: about forty sites carry per-lane
behaviour, and the rest must instead *assert* the int lane, which they cannot do
today because nothing tells them which lane they are in.

**The Vector API surface the emitter uses**, from every `invokevirtual` on the
int vector: `add` 58, `sub` 39, `compare` 31, `mul` 23, `broadcast` 8, `blend` 8,
`lanewise` 6, `min` 4, `and` 4, `intoMemorySegment` 2, `fromMemorySegment` 2, `or`,
`max`, `div` once each. Every one exists on `LongVector` with the same name and the
same shape, differing only in the receiver class and the scalar argument type -
which is what makes a descriptor, rather than a second emitter, the right unit.

**Two facts outside the emitter that a lane parameter meets.** The kernel
interface is `int run(long[] srcData, long[] srcValidity, int[] srcNullCount,
long[] dstData, long[] dstValidity, int[] scalarArgs, int length)` - literals
cross into the kernel as `int[]`, and a long lane's literal does not fit. And
`VarkaShapeKey` is a record over the IR and the emit options, so a lane carried
as a component of the IR nodes reaches the cache key by record equality, and one
carried anywhere else does not.

**The measurement 2.16 asked for is settled by construction, and the plan says
so rather than running it.** The option space weighed (a) a descriptor against (b)
a generated per-lane emitter on the risk of "a megamorphic descriptor call in the
hot path". The descriptor is consulted when the class is *emitted*, in Java, and
the bytes it emits name `IntVector` or `LongVector` constants directly; the
emitted loop contains no descriptor and no dispatch on lane. There is no hot-path
cost to measure. What the choice can affect is emission time, about 80 us per
shape today and amortised by the shape cache, and the int32 bytes - and those are
the admission check: the int32 lane's emitted method bodies must be
**byte-identical** before and after, which is a stronger statement than any
timing and is checked mechanically.

## 3. The design

### 3.1 The mechanism

**The lane is a component of the leaf nodes, and derived everywhere else.**
`ColumnRef(int ordinal, LaneType lane)` and `LiteralSlot(int index, LaneType
lane)`; every other node's `laneType()` is a function of its children - `IntArith`,
`IntNeg`, `Greatest`, `Least` and `IfElse` take their operands' lane and refuse
mixed operands, `Compare` and the other `Cond` nodes answer the lane they compare
in, and every `Chrono` node and `AddDays`/`SubDays`/`DateDiff`/`MakeDate`/`GuardedDay`
answers `INT` and refuses a child that is not - the calendar consumes days. The
refusal is in the record constructors, so a malformed tree cannot be built, which
is the cheapest place to stop it. `LaneType` gains `LONG` in this task, for the
proof lane below; `BOOLEAN` and the rest wait for their tasks. Because the lane is
a record component, `VarkaShapeKey` distinguishes two shapes that differ only in
lane without a change of its own, and a test asserts that.

**The descriptor.** A Java enum `Lane` in the emitter with one member per
`LaneType`, carrying everything the forty sites read: the vector `ClassDesc`,
`speciesField(lanes)` (`SPECIES_` followed by lanes times the lane's bit width),
the bit width and the byte-offset shift (2 for int, 3 for long), the preferred
and permitted lane counts (int: 2, 4, 8, 16; long: 1, 2, 4, 8), the
`MethodTypeDesc`s for load, store, broadcast, lanewise and pick built once per
member, the opcode that loads a scalar argument (`iload` or `lload`), and a field
for the width-dependent lowerings later tasks fill - 88's division form per
`UseAVX`, left at its int value here. The forty sites read the descriptor; the
~140 calendar sites call `Lane.INT.require(node)` on entry and are otherwise
untouched.

**Literals for a long lane, without moving an int32 byte.** The kernel interface
keeps `int[] scalarArgs` exactly as it is for `INT` shapes, so their `run`
descriptor and body do not change. A shape whose lane is `LONG` is emitted against
a second interface method, `run(..., int[] scalarArgs, long[] longArgs, int
length)`, and the evaluator calls whichever the shape declares. Two slot tables,
one per lane, keyed as today. The alternative - widening `scalarArgs` to `long[]`
for everyone - changes every int32 `run` descriptor and every `iload` to
`lload`/`l2i`, and fails the admission check on purpose-built grounds.

**The proof lane, and where 85 stops.** 2.16's admission check needs "a second
lane type reaching the same green differential and fuzz matrices"; a descriptor
with one member proves nothing. So this task ships `LONG` for the lane-generic
subset only: a `bigint` column loaded and stored, literals broadcast, `IntArith`
(`+`, `-`, `*` wrapping and checked), `IntNeg`, `Compare`, `IfElse`, `Greatest`,
`Least` - the arithmetic-and-comparison core of task 104, at both vector widths,
in the emitter's own parity matrix built from IR. What it does *not* ship is
29's: the `TIME`, day-time interval and timestamp semantics, the division
lowerings, the compiler arms that admit a `LongType` column from SQL, and the
evaluator's Arrow buffers beyond the one `BigIntVector` the proof needs. The
SQL-level differential at the long lane therefore arrives with 104, and this
plan says so rather than claiming it.

**The reachability test.** `VarkaIrFuzzSuite` asserts today that its generator can
build every node type in the sealed hierarchy. It becomes node type times lane
type, with a compatibility table the generator obeys (calendar nodes at `INT`
only) - so a later lane arriving without an arm fails here first, in seconds. It
needs the reference evaluator to answer at `long` for the proof subset, which is
part of task 119; **that part of 119 lands with this task**, and the milestone
plan's graph is corrected to say so.

### 3.2 What is deliberately unchanged

Every int32 emitted method body, byte for byte - the oracle. Every calendar
kernel's code. The `run` signature for int shapes. The validity-word machinery,
including its `lanes < 8` behaviour (task 92 lands beside 29, when four-lane
vectors exist in production shapes). The emit options and the shape key's own
fields. The compiler's admission rules: no SQL shape fuses or declines differently
after this task, which the compiler suite, the coverage suite and the differential
enforce.

### 3.3 Registered op counts

No int32 shape changes an op count; the byte-identity check subsumes the table.
For the proof lane, `dev/varka_emit.sh --table` over `l + 1`, `l * 2`, `l < l2`
and `if(l < l2, l, l2)` over `bigint` columns registers the long lane's first op
counts, expected to equal their int32 twins' exactly.

## 4. Files

* `codegen/varka/VarkaVectorIR.java` - `LaneType.LONG`; the lane component on
  `ColumnRef` and `LiteralSlot`; `laneType()` derived per node, refusing mixed or
  wrong-lane children in the constructors.
* `codegen/varka/VarkaLoopEmitter.java` - the `Lane` enum; the ~40 generic sites
  read it; the ~140 calendar sites assert `INT`; the second `run` for `LONG`.
* `codegen/varka/VarkaFusedKernel.java` - the `longArgs` overload.
* `codegen/varka/VarkaShapeKey.java` - no change; a test that lane reaches it.
* `sql/core/.../VarkaKernelEvaluator.scala` - two scalar tables; the `run`
  chosen by the shape's lane; `BigIntVector` in and out for the proof lane.
* `VarkaExpressionCompiler.scala` - constructs leaves with `LaneType.INT`; admits
  nothing new.
* Tests: `VarkaEmittedBytesSuite` (new, the oracle - see section 5);
  `VarkaLoopEmitterSuite` with the proof lane's parity cases at both widths;
  `VarkaIrFuzzSuite`'s reachability at node x lane and its grammar over the
  proof subset at `LONG`; `VarkaReferenceEvaluator` at `long` for that subset
  (119's first part); `VarkaShapeCacheSuite`'s lane-in-key case.
* `docs/sql-varka.md` - the emitter anatomy section names the lane descriptor;
  the glossary gains *lane type*.
* `PLAN_MILESTONE_5.md` - row 85; 119's first part moved beside 85 in 3.1.

## 5. Tests, and what each is for

* **`VarkaEmittedBytesSuite` - the oracle, written first.** For every `Surface`
  and `Chains` entry (64) and for ten thousand fuzz shapes at a fixed seed, the
  emitted class's method bodies (the `Code` attribute bytes per method, not the
  whole class - constant-pool order may legitimately move) hashed and pinned
  against today's emitter, at both vector widths. Every step of section 8 runs
  it; a byte moving on an int32 shape is a failed step, not a note.
* **The proof lane's parity matrix** in `VarkaLoopEmitterSuite`: the subset over
  `bigint` at 128 and 512 bits, every null pattern the int cases use, against the
  reference evaluator at `long`.
* **Reachability at node x lane** in the fuzz suite, with the compatibility table
  as the specification of which nodes exist at which lane.
* **Lane in the key**: two shapes differing only in `ColumnRef`'s lane produce two
  `VarkaShapeKey`s and two cache entries.
* **Malformed trees refused**: a calendar node over a `LONG` child, and an
  `IntArith` over mixed lanes, fail in the constructor with the lane named.
* **The standing oracles**: compiler suite, coverage suite with its byte-compared
  `coverage.json`, differential classification, `dev/varka_emit.sh` verdicts and
  shape hashes over the inventory - all unchanged.

## 6. The measurement

No committed number moves: the int32 bytes are identical, so the int32 kernels
are the same kernels. The one number this task produces is the proof lane's
first: the four op counts in 3.3, and one throughput case for `l + l2` at both
widths in `VarkaEmitterParityBenchmark`, committed so 104 has a baseline before it
adds anything (the baseline-as-its-own-PR rule).

### 6.1 Predictions, registered before the run

1. **Zero int32 bytes move**, across 64 inventory entries and ten thousand fuzz
   shapes at both widths, at every step of section 8.
2. **About forty sites change**, not 205; the calendar kernels gain one assertion
   line each and nothing else.
3. **The proof lane's op counts equal their int32 twins'**, since the ops are the
   same ops on a different vector class; its throughput per row is close to half
   the int32 case's at the same width (half the lanes per vector), and at 256
   bits - four lanes - it is *worse* than half by roughly the 6 to 9% task 47
   measured for byte-granular validity writes, which is 92's number arriving on
   schedule rather than a defect of this task.
4. **Emission time is unchanged within noise**, read from the emission events
   `VarkaCompilationWatch` already records, because the descriptor is a static
   table consulted once per site.

## 7. Risks

* **Byte identity broken by something other than semantics.** Rebuilding
  descriptors per lane can reorder the constant pool or rename a synthetic; the
  oracle therefore hashes method bodies, and a change it reports is read from
  `dev/varka_emit.sh --asm`-style bytecode diffs before it is accepted or fixed.
* **The `run` overload.** Two entry points on one interface is one more thing the
  evaluator must get right per shape; the lane-in-key test and a wrong-overload
  test (a `LONG` shape called through the int `run` fails with the lane named)
  cover it.
* **`broadcast` argument types.** `IntVector.broadcast(VectorSpecies, long)`
  exists beside the `int` form; the descriptor pins the exact `MethodTypeDesc` per
  lane so the int lane keeps emitting the call it emits today.
* **Scala 2.13 and a Java enum with per-member data.** The compiler and evaluator
  read the descriptor only through a small API; the emitter owns it.
* **Scope creep into 29.** The proof lane is a fixed list; anything a `TIME` or
  interval semantic needs is 29's and is declined here with a pointer.

## 8. Sequencing

1. `VarkaEmittedBytesSuite` against today's emitter, both widths. Green means the
   oracle exists.
2. `LaneType` as a component of the leaves, `INT` everywhere, derivations and
   constructor refusals; the compiler constructs leaves with it. Oracle green.
3. The `Lane` enum with the `INT` member only; the ~40 sites switched one method
   at a time, oracle after each; the calendar sites assert `INT`. Oracle green.
4. `LONG`: the member, the `longArgs` overload, the evaluator's second table and
   `BigIntVector`, the proof subset's emitter arms; 119's evaluator arms for the
   subset; the parity matrix and the node x lane reachability. Oracle still green
   on int32.
5. The lane-in-key test; the four op counts and the one throughput baseline
   committed; the standing oracles; the full gate.
6. Section 9, the milestone row, 119's first part recorded as landed, the docs.

## 9. Outcome

*To be written from the oracle and the proof lane's numbers.*
