# VARKA-489: node properties as a descriptor

`VARKA-475.md` 9.2, item 3, as a row of its own. A new IR node reached about twenty places
(`VARKA-475.md` 2), most of them exhaustive switches over every node type asking one small
property of it. This row gives each node one record of those properties, `VarkaNodeProps`, and
moves the switches that only asked a property to read it.

## 0. Inventory

Every switch over `VarkaVectorIR` or `Cond` node types with three or more node arms, in the main
and test trees of `sql/catalyst`, at `4e09c1ecfce` (the line is the `switch`). 43 switches, plus
`VarkaVariants.deepCopy`, which defaults. Classified as the brief asks:

- **(a)** a property: one answer per node type, reading only the node's own fields;
- **(b)** real per-node behaviour - lowering, emission, evaluation, canonical form, structure;
- **(c)** mixed: a per-node part and a part that depends on options or on the tree.

| Switch | Class | Now |
| :--- | :--- | :--- |
| `VarkaEmitBudget.java:597` `weightOf` | a | reads `props().weight()` |
| `VarkaEmitCost.java:112` `kindOf` | a | reads `props().costKind()` |
| `VarkaOperandPosition.java:76` `admits` | a | reads `props().fills()` |
| `VarkaOperandPosition.java:109` `restrictedOperands` | a | reads `props().restricted()` |
| `VarkaVectorIR.java:201` `inputOrdinal` | a | reads `props().validity()` |
| `Analysis.java:795` `isNarrowRing` | c | node's `Ring`; the check option stays here |
| `Analysis.java:1063` `ownerOf` | c | node's `Validity`, folded over its operands |
| `Analysis.java:1116` `pureOf` | c | node's `Validity`, folded over its operands |
| `Slots.java:919` `planWordRef` | c | node's `Validity`, folded over its operands |
| `Slots.java:1474` `propagate` | c | derived: the words of the value operands |
| `Analysis.java:653` `collectRefusals` | c | stays: options and the node's place in the tree |
| `Slots.java:1369` `visit` (liveness demands) | c | stays: per-node demands, some by option |
| `VarkaVectorIR.java:256` `narrowValuesIn` | c | stays: a walk with per-node descent |
| `VarkaVectorIR.java:305` `needsNarrowSpecies` | c | stays: the same walk's twin |
| `VarkaRangeAnalysis.java:236` `over` | c | stays: interval arithmetic per node |
| `VarkaWordCensus.java:143` `pure` (test) | c | stays: the census's own model, see 2 |
| `VarkaVectorIR.java:140` `laneType` | b | structure: derived from children |
| `VarkaVectorIR.java:1498` `canonical` | b | canonical form |
| `VarkaVectorIR.java:1587` `canonicalShallow` | b | canonical form |
| `VarkaVectorIR.java:1681` `childrenOf` | b | structure |
| `VarkaVectorIR.java:1751` `withChildren` | b | structure |
| `Analysis.java:1254` `analyze` | b | lowering analysis |
| `VarkaVectorWalk.java:174` `storeWord` (the value walk) | b | emission |
| `VarkaVectorWalk.java:1513` `emitBoolCompare` (condition walk) | b | emission |
| `VarkaChronoLowering.java:392`, `:1831`, `:1867`, `:1898` | b | the calendar family's lowering |
| `VarkaChronoCompiler.java:117`, `:733` | b | compiling and re-arming day producers |
| `VarkaConditionCompiler.java:304`, `:478`, `:646` | b | lane rewriting of conditions |
| `VarkaReference.java:75`, `:182`, `:339`, `:405`, `:528` (test) | b | the reference evaluator |
| `VarkaIrGrammar.java:246` `boundsOf`, `:604` (test) | b | the fuzzer's magnitude bounds |
| `VarkaGuardSpecs.java:387` `fires` (test) | b | a guard's firing rule |
| `VarkaWordCensus.java:430`, `VarkaKernelWarmupProbe.java:137` (test) | b | test drawing |

Counts: 5 (a), 11 (c) of which 5 now read the descriptor, 27 (b).

The five properties the row names map onto the descriptor so: *ring under narrow reduction* is
`ring`; *pure* and *skips a null input* are both `validity` - a word is a pure function of the
input bitmaps unless the node computes it from values (`OWN`) or is a condition (`NO_WORD`), and
`SKIPS_NULL` is the OR of `Greatest` and `Least`; *budget weight* is `weight`; *cost kind* is
`costKind`. The operand positions (`fills`, `restricted`) are the two further (a) switches of
`VarkaOperandPosition`, moved so that a new node does not touch that file.

## 1. The descriptor

`VarkaNodeProps` is a record beside the IR with six components: `Ring ring`, `Validity validity`,
`int weight`, `String costKind`, `Set<VarkaOperandPosition> fills` and
`List<VarkaOperandPosition.Restricted> restricted`. `Ring` is sealed over `Never`, `Pick` and
`Arith(Overflow mode)`; `Validity` over `Input(int ordinal)`, `AllTrue`, `Strict`, `SkipsNull`,
`Own` and `NoWord`. Each answer reads the node's own fields and lane and nothing else; what
depends on the options is the pass's to decide from the answer (`Analysis.isNarrowRing` asks
`loweredUnchecked` of an `Arith`'s mode), and what depends on the operands is folded by the pass
over `childrenOf` (`Strict` is an AND of the operands' words, a unary node's an alias of its
child's).

**How a node supplies it.** `VarkaVectorIR` declares `VarkaNodeProps props()` with no default,
and every one of the 47 records implements it next to its constructor checks. "Required at
construction" is the compiler's: a record without `props()` does not compile. `VarkaNodePropsSuite`
keeps it so - it fails if the interface method gains a default or if `Cond` or `Chrono` defines
one, which would let a family member inherit an answer, and it walks the sealed hierarchy's
permitted records checking each declares its own.

Not a record component: the records' structural `equals` and `hashCode` are the emitter's memo
keys and the shape cache's identity, and the descriptor is derived from the fields that are
there. The alternative was one exhaustive `switch (node)` in `VarkaNodeProps`, which the compiler
enforces as well; per record was chosen because a new node is then written in one place, its
record, with the descriptor beside the constructor that checks its operands. The weight
constants stay in `VarkaEmitBudget` with the op counts that derive them, and the records name
them.

Three hand-kept copies of one rule become one: `ownerOf`, `pureOf` and `planWordRef` each listed
how a node's word follows from its operands' (`planWordAlgebra`'s doc said "case for case"), and
now all three fold the node's `validity`. `Slots.propagate`, which only an own-word node reaches,
demands its value operands' words, which every arm of the old switch that could be reached did.

## 2. What stays a switch, and how to add a node now

The (b) switches are the node's behaviour and stay exhaustive: its lane and structure
(`laneType`, `childrenOf`, `withChildren`, both canonical forms), its lowering (`Analysis.analyze`,
the walks of `VarkaVectorWalk`, the calendar family's), the compilers' rewrites and the test
harness's evaluators and grammars. The (c) switches that stay mix a node's part with an option or
the tree: `collectRefusals` decides a refusal from the options and from whether a calendar node
reads the node; `Slots.visit` demands words by mode and by guard shape; `narrowValuesIn` and
`needsNarrowSpecies` are walks whose per-node descent is the point; `VarkaRangeAnalysis.over` is
interval arithmetic. `VarkaWordCensus.pure` is the census's own model of a word, with two
extensions the emitter does not have, and differs from `pureOf` on purpose (`IntNeg` is not
pure there); bringing it onto the descriptor is a census change, not this row's.

**Adding a node now** (the boolean output of `varka-27-boolout` and `varka-27-arm`, the decimal
column of `varka-347-leaf` and `varka-347-reader`, a refusal on `varka-503-two-lane-decline`):

1. Write the record in `VarkaVectorIR` and add it to the `permits` list (or `Cond`'s).
2. Implement `props()` in the record: its `Ring` (`NEVER` unless reduction modulo a narrow width
   commutes with it), its `Validity` (`Input` for a leaf reading a column's word, `STRICT` for
   null in any operand is null out, `SKIPS_NULL` for an OR, `OWN` for a word computed from values,
   `NO_WORD` for a condition), its weight in lane operations (a constant in `VarkaEmitBudget` if it
   needs a derivation), its cost feature (the record name plus whatever chooses its lowering), and
   its operand positions (`FILLS_NOTHING` and `RESTRICTS_NOTHING` unless it is or has a day offset,
   month count, weekday or level).
3. Add its arms to the switches the compiler then names: in `VarkaVectorIR` the five structural
   ones and the two narrow walks; `Analysis.collectRefusals` (a node that refuses is an arm there)
   and `Analysis.analyze`; `Slots.visit`; the emission walk in `VarkaVectorWalk`; the compilers'
   rewrites that list every node (`VarkaChronoCompiler.rearm`, `VarkaRangeAnalysis.over`); and in
   the tests `VarkaIrGrammar.boundsOf`, `VarkaReference` and `VarkaGuardSpecs`.
4. Price the new `costKind` feature in `VarkaEmitCostTable` the way `VarkaEmitCostSuite` says (a
   group holding an unpriced feature has no prediction, and is grouped by the budget alone), and
   give the node a row in `VarkaEmitterBudgetSuite`'s register, which fails until it has one.

A branch written before this row rebases by deleting its arms from `weightOf`, `kindOf`,
`isNarrowRing`, `ownerOf`, `pureOf`, `planWordRef`, `propagate`, `admits`, `restrictedOperands`
and `inputOrdinal` and moving each arm's answer into its record's `props()`.

## 3. Tests

- `VarkaNodePropsSuite` (new, kept): the guarantee of 1, and over 500 drawn shapes that a `Strict`
  or `SkipsNull` node's operands are values, an `Input` node is a leaf whose ordinal
  `inputOrdinal` reports, and `NO_WORD` belongs to conditions only.
- An equivalence suite, run and then deleted (its frozen copies of the old switches are per-node
  lists a new node would have to touch, the cost this row removes): the ten replaced functions,
  copied from `4e09c1ecfce`, against the descriptor over every distinct node of 4000 drawn shapes
  of all six grammars plus hand-built variants of every mode, lane, sign and level - the word
  rules compared symbolically, operand by operand, and the ring rule under both values of
  `checkIntOverflow`. Result in 4.
- The byte oracles: `emitted_bytes.json`, `coverage.json` and `width_audit.json` byte-identical.

## 4. Outcome

*27 October 2026.* Built as designed in 1: `VarkaNodeProps` and 47 `props()` implementations, and
ten functions reading them - `weightOf`, `kindOf`, `admits`, `restrictedOperands`,
`inputOrdinal`, `isNarrowRing`, `ownerOf`, `pureOf`, `planWordRef` and `propagate` - where there
were ten exhaustive switches, nine of them over every node type. A new node now writes those
answers once, in its record, instead of an arm in each of ten places, and no longer touches
`VarkaEmitBudget`, `VarkaEmitCost` or `VarkaOperandPosition` at all. What it still touches is
step 3 of 2's recipe: the IR file's structural switches, its lowering's arms and the test
harness's evaluators, each an arm of behaviour rather than a restated property.

- **Equivalence:** the frozen copies agreed with the descriptor on all 229417 distinct nodes of
  the corpus, every one of the 47 record types among them, the ring rule under both values of
  `checkIntOverflow`. The suite was then deleted, as 3 says.
- **Byte oracles:** `VarkaEmittedBytesSuite`, `VarkaCoverageSuite` and `VarkaWidthAuditSuite`
  pass against the committed `emitted_bytes.json`, `coverage.json` and `width_audit.json`, which
  did not change. Because those pin acceptance only (`VARKA-475.md` 9.1, finding 5), the coverage
  suite's fusion dump (`VARKA_FUSION_DUMP`), every fusion and decline decision over the table, was
  written before and after the change and is identical, 12171 lines.
- **One behaviour the oracles did not pin, found by the scoped run:** the first `Validity.Input`
  refused a negative ordinal, and since the analysis reads the descriptor before the emitter's
  admission, a kernel over column -1 was refused with that internal error instead of the
  admission's "column ordinal -1 outside" (`VarkaEmitterContractSuite`). The check was removed:
  a descriptor states facts and leaves refusing to the admission.
- **Suites and checks:** the catalyst Varka suites as the PR build runs them
  (`dev/varka_scoped_suites.sh catalyst`), the sql suites over the oracles' rows
  (`VarkaCoverageDifferentialSuite`, `VarkaDifferentialSuite`, `VarkaGoldenCorpusSuite`,
  `VarkaFarDaySuite`, `VarkaEndToEndSuite`, `VarkaSparkFuzzSuite`: 1037 tests), Error Prone,
  `dev/lint-java`, `dev/lint-scala`, the census check, `dev/varka_new_lane.py --check`, the plan
  lint and the quote check pass.
