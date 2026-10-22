# VARKA-475: the compiler's type architecture

## 0. Why this plan exists

Milestone 8 is "every numeric and temporal type", and the owner's direction is broader still: Varka
will support Spark's data types, read against the SQL standard's categories. Four of the seven
milestone types have not started, and the two that have show what one costs and where the
knowledge lives.

The project already keeps the two registers this plan needs: `m8/census.tsv`, which names each
in-scope expression and the thirteen types it admits, and `m8/casts.tsv`, whose 106 pairs each
carry the LEGACY, ANSI and TRY answer, the kind of lowering, the semantics and the proof
obligation. Both are documentation and coverage checks. Neither is read by the compiler: the cast
lowerings are `case Cast c when ...` arms spread across the family compilers, and a type is
admitted by the if-chain in `VarkaNodeCompiler.laneOf`.

The target space, by the standard's categories:

| SQL standard category | Spark types | Varka today | Milestone |
| :--- | :--- | :--- | :--- |
| Exact numeric | BYTE, SHORT, INT, LONG, DECIMAL(p,s) | two lanes; narrow and decimal in flight | m8 |
| Approximate numeric | FLOAT, DOUBLE | none | m8 |
| Boolean | BOOLEAN | conditions only; `CondValue` is row 460 | m8 |
| Datetime | DATE, TIME, TIMESTAMP, TIMESTAMP_NTZ | DATE, TIME | NTZ in m8; LTZ out by decision |
| Interval | YEAR-MONTH, DAY-TIME | both | done |
| Character, binary | STRING, CHAR, VARCHAR, BINARY | none | out of m8; m9 item 3 |
| Row and collections | STRUCT, ARRAY, MAP | none | no representation yet |
| User-defined, JSON, spatial | UDT, VARIANT, geospatial | none | later |

Four pieces follow, in the order the milestones force them. Each can take its own task row when
it starts; this plan is the design they share.

## 1. What the design gets right, and what must not move

The IR is sealed and typed, and the node-kind switches are exhaustive, so a new node is a compile
error at every consumer. A lane is a physical width, `Lane` is a per-lane facts table, and the
evaluator's `isArrowBacked` and `allocateVector` are the only Arrow boundary. The shape cache
amortises compilation, and three contracts hold the engine honest: the bytes oracle
(`emitted_bytes.json`), the fuzzers against the reference evaluator, and the option matrix.

Every piece below is additive to this design. None of them may change a byte of an existing
shape, and the checks for that are section 4's.

## 2. The four pieces

### 2.1 A type registry: `VarkaTypes`

A closed `VarkaTypeId` enum - BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, DATE, TIME,
TIMESTAMP_NTZ, YM_INTERVAL, DT_INTERVAL, and declared-but-unsupported STRING, BINARY and NESTED -
with `of(DataType)` in one function, replacing `laneOf` and its siblings. Each id carries a
descriptor record: the standard category, the null and ordering semantics, the literal codec, the
cast rules, the census name, the cost feature and the decline reasons.

What changes: the family compilers ask the descriptor instead of hard-coding; `Analysis` asks it
for the guards a node arms; the decline reason names the type it does not support, so "declined"
stops being indistinguishable from "not implemented". The registry is compile-time data, read
once per shape; nothing on the batch path calls it.

**Done when** `laneOf` and its siblings are one function; the census's type names come from the
registry rather than a second list; a decline for a type the compiler does not support names that
type; and every existing suite and the three generated files are unchanged.

Cost: three to four days. It lands before `TIMESTAMP_NTZ` (row 338), which is the first type whose
semantics cross a lane boundary: a long decomposed into the int32 calendar prefix, with casts to
and from `DATE`.

### 2.2 Value shapes: a value is not always one lane

`LaneType` is a width, but a value is not always one lane-width. `DECIMAL(19..38)` is a pair of
long lanes (m8 2.8), `BOOLEAN` is a bit, `STRING` will be an offset-length buffer, and a struct is
a composite. Today the IR conflates the shape with the lane; decimal 128 forces the separation.

The proposal: a `ValueShape` - `ONE(lane)`, `PAIR(lane)`, `BITS`, `VARLEN(offsets, bytes)`,
`COMPOSITE(children)` - declared by every IR node for its result, and every shape change is an IR
node of its own. `WidenLane` and `NarrowLane` already are two such conversions; `PackBits` and
`UnpackBits` (the boolean condition algebra already works on two words), and the
`Interleave`/`Deinterleave` a decimal pair needs, would join them. The emitter then dispatches on
shape and never guesses from the logical type.

**Done when** decimal 128 is expressed as `PAIR` with its conversions as IR nodes; a shape change
never happens inside an emitter arm; and the emitted bytes of every existing shape are unchanged.

Cost: five to ten days, and it belongs to the row that forces it, `DECIMAL` (rows 347, 359), not
to a refactor milestone.

### 2.3 Executable registers: `casts.tsv` and `census.tsv`

The cast matrix is the larger half of the breadth goal (106 pairs) and the register already holds
each pair's answer. Make it executable: a `dev/varka_casts.py --check` that compares the
hand-coded arms with `casts.tsv` and fails on a pair that is claimed and not emitted, or emitted
and not claimed; later, a generated `VarkaCastTable` the family compilers consult, so a new type
adds rows rather than arms. The same treatment for the census's `types` column: admission becomes
a lookup whose decline names the type, and the two registers become the compiler's input rather
than its documentation.

**Done when** the check runs in CI and in the pre-commit hook; the 106 pairs agree with the
compiler; and adding a pair is a row plus a lowering, with no new arm in a family compiler.

Cost: about three days for casts, one for the census.

### 2.4 A conformance contract per type

A `VarkaTypeConformance` trait, parameterized by `VarkaTypeId`, with one check each for: the load,
the round trip, null handling, SQL ordering (NaN and both zeros for the floating types), choice,
the type's row of the cast matrix, and overflow in LEGACY, ANSI and TRY. The oracle is the
reference evaluator for the kernel checks and the row engine for the end-to-end ones.

A type cannot land without it, exactly as an expression cannot land without its coverage row
today. A new type's definition of done becomes "the conformance suite passes", and the census
reports by the standard category, which makes the remaining work finite and standard-defined.

**Done when** the trait exists with those seven checks; the first two types pass it; and the
census check fails for a type with no conformance suite.

Cost: three to five days for the trait and the first two types.

## 3. Sequencing and dependencies

| When | Piece | Forced by |
| :--- | :--- | :--- |
| after 329/330 merge | 2.1 registry, and row 474's `LaneArrow` table | `TIMESTAMP_NTZ` (338) next |
| with decimal | 2.2 shapes, 2.3 casts | `DECIMAL` (347, 359) |
| with floats | the `Lane` split into integral and floating halves, the ordering fields of 2.1 | `DOUBLE`, `FLOAT` (341) |
| continuous | 2.4 conformance | every type |

Pieces 2.1 and 2.3 touch `VarkaNodeCompiler` and the family compilers; 2.2 touches
`VarkaVectorIR` and the emitters; 2.4 adds tests only. None depends on another but each is cheaper
after the registry exists.

## 4. Verification

- The bytes oracle: `emitted_bytes.json`, `coverage.json` and `width_audit.json` are byte-identical
  across 2.1 and 2.3 for every existing shape, and move only where a shape is new in 2.2.
- The existing suites: the emitter suites of every type, the differential suites and both fuzzers
  pass unchanged after each piece.
- The matrix: every piece that adds an emitting test runs under all configurations
  (`dev/varka_matrix.sh --all`), and the counts stay green with nothing unfused.
- The registers: `dev/varka_casts.py --check` and `dev/varka_census.py --check` are the two
  commands that say whether a pair or a type is wired.

## 5. Non-goals and risks

- **No e-graph representation chooser.** `m9/SCOPE.md` item 11 argues for one; the `ValueShape`
  conversions are its seed, and the data to decide it does not exist yet.
- **No type-aware IR beyond semantic parameters.** A node carries the shape plus the parameters it
  needs (ordering, overflow mode, rounding). The logical type stays in the lowering and the
  reference evaluator, or the emitter stops being shape-directed.
- **No virtual calls on the batch path.** The registry is read at compile time, once per shape.
- **The registry must not become a god object.** It is records and small functions; the behaviour
  stays where it is emitted.
- **The risk is abstraction ahead of the evidence.** Each piece is scheduled against the row that
  forces it, and the first (2.1) is the smallest that pays for itself: it replaces an if-chain
  whose misses are silent declines.

## 6. Open questions

1. **Where the registry lives.** `VarkaTypes` in the catalyst codegen package beside
   `VarkaNodeCompiler`, or in `varka/` beside `Lane`. The first keeps the lowering together; the
   second lets the evaluator and the tests read it without depending on the compiler.
2. **Generated or checked.** 2.3 can generate the cast table from `casts.tsv`, or check the
   hand-coded arms against it. Generation is cheaper to keep in sync; checking keeps the
   lowerings reviewable as code. The plan assumes checking first, generation only if the arms
   outgrow review.
3. **`VARLEN` and `COMPOSITE` in the shape enum now, or when strings and nested types start.**
   Declaring them now costs two enum members and no code; declaring them later costs a refactor of
   every `switch (shape)`.
