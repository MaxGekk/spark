# VARKA-337: The canonicalisation node, `hash` and `xxhash64`

## 0. What exists now (admission check)

From master at `5cdb032b9ef` (30 October 2026; Varka paths under
`sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen/` unless named, Spark
paths under `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/` unless named). The row's
text is the milestone plan's 2.7 (its first bullet's last sentence) and its census rows `hash`,
`xxhash64`, `normalize_nan_and_zero` and `known_fp_normalized`; the canonicalisation node moved
here from row 341 (`VARKA-341.md` 0, item 9, and "What differs"). The JDK's facts were read in
`/home/max/proj/openjdk-build/jdk25`. The checks marked *(admission)* ran on the laptop, as the
program in `m8/results/VARKA-337-admission.txt` and its output, at the host's defaults (AVX-512)
and again under `-XX:UseAVX=2`, with Spark 4.2.0's classes as the oracle: the numeric arms of
`hash.scala`, `Murmur3_x86_32.java` and `XXH64.java` are unchanged between `v4.2.0` and this
master (the diff touches only the calendar interval and nanosecond arms).

1. **Spark's hash is a seed folded through the children, a null child skipped.**
   `HashExpression.eval` starts from `seed` and sets `hash = computeHash(child.eval, type, hash)`
   for each child in order (`expressions/hash.scala:400-409`); the generated code is the same fold
   under `nullSafeExec(child.nullable, isNull)` (`:413-442`), so a null child leaves the running
   value as it is, and the expression is never null (`nullable = false`, `:366`). `hash` is
   `Murmur3Hash` with an `Int` seed of 42 (`:849-856`), its value `Murmur3HashFunction.hash(...)
   .toInt` (`:860-864`, which truncates the seed with `seed.toInt`, `:871-882`); `xxhash64` is
   `XxHash64` with a `Long` seed of 42 (`:925-938`). *(admission)*: 100000 rows over an `INT`,
   `BIGINT`, `DOUBLE` and `FLOAT` column, one value in four null, agree with the hand fold of
   `hashInt` and `hashLong` with the null skip, 409 of them all null and hashing to the seed, and
   both expressions report `nullable = false`.
2. **The per-type input is a 32- or a 64-bit word, except two cases.** `computeHashWithTailRec`
   (`hash.scala:634-659`) and the interpreted `hash` (`:707-745`):

   | type | Spark's call (`hash.scala`) | the word |
   |---|---|---|
   | `BOOLEAN` | `hashInt(b ? 1 : 0)` (`:473`) | 0 or 1 |
   | `TINYINT`, `SMALLINT`, `INT`, `DATE`, `ym` | `hashInt` (`:641`, `:651`) | sign-extended |
   | `BIGINT`, `TIME`, `TIMESTAMP_NTZ`, `dt` | `hashLong` (`:642-643`, `:650`) | the value |
   | `FLOAT` | `hashInt(f == -0.0f ? 0 : floatToIntBits(f))` (`:476`) | canonical bits |
   | `DOUBLE` | `hashLong(d == -0.0d ? 0L : doubleToLongBits(d))` (`:486`) | canonical bits |
   | `DECIMAL(p, s)`, `p <= 18` | `hashLong(toUnscaledLong)` (`:496-501`) | the unscaled value |
   | `DECIMAL(p, s)`, `p > 18` | `hashUnsafeBytes` of `toByteArray()` (`:502-510`) | 1 to 16 bytes |

   (`ym` and `dt` are the year-month and day-time intervals.)

   `floatToIntBits` and `doubleToLongBits` map every NaN to `0x7fc00000` and
   `0x7ff8000000000000`, and `x == -0.0` holds for both zeros, whose canonical bits are 0: the
   hash's own step is the canonicalisation. *(admission)*: 200000 samples per type, half at seed
   42 and half at drawn seeds, for all fourteen of the table's types (two decimal precisions),
   through `Murmur3HashFunction.hash` and `XxHash64Function.hash`, with no mismatch.
3. **The 64-bit and 32-bit primitives.** `Murmur3_x86_32.hashInt` is `fmix(mixH1(seed,
   mixK1(v)), 4)` and `hashLong` the same over the low then the high int, `fmix(..., 8)`
   (`common/unsafe/.../hash/Murmur3_x86_32.java:48-53`, `:111-122`): per step three or four
   32-bit multiplies by constants, two rotates, shifts and XORs. `XXH64.hashInt` and `hashLong`
   (`expressions/XXH64.java:60-76`) are one 64-bit lane of xxHash64's tail plus its `fmix`
   (`:120-127`): four multiplies by 64-bit constants for an int input, five for a long, two
   rotates. The byte forms differ between the two: Spark's `Murmur3_x86_32.hashUnsafeBytes` is the
   legacy one, whose tail mixes each remaining byte, sign-extended, as a whole int
   (`Murmur3_x86_32.java:66-78`), and `XXH64.hashUnsafeBytes` reads 8-byte words, then one 4-byte
   word, then single unsigned bytes (`XXH64.java:92-114`), with no 32-byte stripe below 32 bytes
   (`:129-192`).
4. **Spark inserts the normalisation only into keys a join, a window or an aggregate evaluates.**
   `NormalizeFloatingNumbers` wraps the equi-join keys, the null-aware anti-join's key and a
   window's partition expressions (`optimizer/NormalizeFloatingNumbers.scala:69-101`), as
   `KnownFloatingPointNormalized(NormalizeNaNAndZero(e))` for a floating scalar (`:134-135`), and
   runs once at the end of the optimizer (`optimizer/Optimizer.scala:303`); the planner applies
   the same `normalize` to aggregate grouping keys (`sql/core/.../SparkStrategies.scala:599-606`,
   `:744-750`, `:776-781`, `:812-819`) and streaming state keys. None of these is a `Project` or a
   `Filter`, so no Varka node sees either class from a query (`VARKA-341.md` 0, item 9).
   `NormalizeNaNAndZero` is NaN to `Float.NaN` or `Double.NaN`, `-0.0` to `0.0`, every other value
   itself, null-intolerant (`NormalizeFloatingNumbers.scala:180-246`).
   `KnownFloatingPointNormalized` is a `TaggingExpression` (`expressions/constraintExpressions.
   scala:66-69`): its value, type, nullability and code are its child's (`:24-31`).
5. **The canonicalisation node holds in lanes.** *(admission)* `x.add(0.0).blend(NaN,
   x.test(IS_NAN))` at the `FloatVector` and `DoubleVector` species: over every one of the 2^32
   `FLOAT` patterns its raw bits equal those of Spark's `FLOAT_NORMALIZER` and of the hash's input
   of item 2, and applied twice it is unchanged; over 67108864 `DOUBLE` patterns (17 specials,
   among them the four NaN range endpoints `0x7ff0000000000001`, `0x7fffffffffffffff`,
   `0xfff0000000000001` and `0xffffffffffffffff`, then random bits, NaN payloads of both signs,
   subnormals and normals) the same three agree. No mismatch at either instruction set. The NaN
   test reads the input, not the sum, so the add and the test are independent.
6. **The hash steps hold in lanes.** *(admission)* Hand-written Vector API forms of `hashInt`
   (the int species, `h * 5` as `(h << 2) + h`) and of `XXH64.hashInt` (the long species over the
   zero-extended int) match Spark's over every 32-bit input at seed 42, and composed with item 5
   they match `Murmur3HashFunction.hash` and `XxHash64Function.hash` over every `FLOAT` pattern;
   the long forms - `hashLong` in row 28's representation (the low and high ints by `L2I` into the
   low half of the int species of the long species' width) and `XXH64.hashLong` - match over
   16777216 drawn longs, half at seed 42 and half at drawn seeds. No mismatch at either
   instruction set. Each 2^32 sweep took 1.0 to 2.0 s against the primitives and 4.5 to 5.3 s
   against the boxed `hash` on 24 threads (laptop wall times of the check, not benchmarks).
7. **The decimal above 18 can be built from the two words.** *(admission)* `toByteArray`'s length
   is `(bitLength(u ^ (u >> 127)) >> 3) + 1`, two leading-zero counts and a blend, and the array
   read little-endian is the byte-reversed pair shifted right by `8 * (16 - length)` bits; from
   these, the legacy Murmur3 tail and the XXH64 tail match Spark's for precision 19 to 38, every
   length from 1 to 16 drawn (39781 values of length 1, 1675 of length 16), no mismatch. The
   longest array is 9 bytes at precision 19 to 21 and 16 at 36 to 38, so every in-range value's
   bytes fit the pair, and a column of one precision still hashes arrays of every length up to its
   own: the step is a variable-length loop, unrolled as four masked int steps and three masked
   byte steps for Murmur3, two words, one int and three bytes for XXH64. Written as lane
   operations it is about 95 per value against about 17 for a 32-bit Murmur3 step (2.4).
8. **The JDK.** The 64-bit multiply is `evpmullq` with AVX512DQ (`cpu/x86/x86.ad:6018-6031`),
   seven instructions on AVX2 (`vpshufd`, `vpmulld`, `vpshufd`, `vpaddd`, `vpsllq`, `vpmuludq`,
   `vpaddq`; `:6071-6094`), a single `vpmuludq` when C2 sees both inputs as zero-extended 32-bit
   values (`:6096-6106`, `share/opto/vectornode.cpp:2195-2237`), and on NEON without SVE two scalar
   `mul`s through lane moves (`cpu/aarch64/aarch64_vector.ad:1040-1056`), SVE's `mul` with SVE.
   The rotate is `vprold` and `vprolq` only with EVEX (`x86.ad:1639-1647`), and elsewhere C2
   degrades it to two shifts and an or where variable shifts exist
   (`share/opto/vectornode.cpp:445-475`, `:1688`); aarch64 has no rotate rule. The int multiply is
   `vpmulld` (SSE4.1 and up) and NEON's `mul` (`aarch64_vector.ad:1019`). Byte reversal needs
   AVX2 (`x86.ad:1899-1903`); a long leading-zero count is native only with AVX512CD
   (`:2077-2080`) and emulated by C2 on AVX2 (`cpu/x86/c2_MacroAssembler_x86.cpp:6284`).
9. **The census has none of these cells.** `results/VARKA-376-census-epyc-7763.json` and
   `-neoverse-n2.json` hold no `int.MUL`, `long.MUL`, `ROL` or leading-zero cell; the double
   `ADD` is row 342's cell and the reinterpretations row 343's (`VARKA-343.md` 4 item 2).
10. **What the code has.** `IntArith(MUL, WRAP)`, `BitOp` (`AND`, `OR`, `XOR`, `SHL`, `SHR`,
    `USHR`), `WidenLane`, `NarrowLane`, `IfElse`, `CondValue` (row 460) and `LiteralSlot` exist
    (`varka/VarkaVectorIR.java:395-415`, `:563-626`, `:953`, `:1209-1240`); no rotate, no floating
    `+` (row 342, planned) and no reinterpretation node. The floating lanes are read and compared
    (#1049, #1054, #1065); a floating node in an integral emission is refused until row 343a's
    width rule (`VARKA-343.md` 0 item 3, 2.2), and `floatingDecline` admits no hash or
    normalisation class (`VarkaNodeCompiler.java:338-359`). Row 28 is done: an int value in a
    long-driven kernel lives in the low half of the int species of the same width (`VARKA-28.md`
    9), which is the representation item 6 checked. The decimal leaf is row 347's, not merged;
    above 18 it is row 359's word leaf, planned (`VARKA-359.md` 2.2).
11. **No benchmark hashes.** No Varka benchmark class has a `hash` or `xxhash64` case; the
    decimal baseline uses `xxhash64` only to generate its data
    (`sql/core/.../VarkaDecimalBaselineBenchmark.java:123`). `benchmark.yml` runs on x86 only.
12. **Census.** `hash` and `xxhash64` admit 13 types each, 26 pairs, none fused
    (`m8/census.tsv:280-281`); `normalize_nan_and_zero` and `known_fp_normalized` admit `float`
    and `double`, the first naming rows 341 and 337 and the second row 341 (`:627`, `:626`).
    Row 368's hash half waits on this row: the 74 hash checks with a column in
    `HashExpressionsSuite` all decline (`VARKA-368.md` 0 item 5).

### What differs from the row text and the milestone plan

- **The hash's floating step is the canonicalisation** (item 2): the node is not something put in
  front of Spark's hash but Spark's own `-0.0` and `floatToIntBits` rule, written once. So the
  node is built where Spark builds `NormalizeNaNAndZero` and as that one step of `hash` and
  `xxhash64`, and nowhere else (2.1). Open point 4.
- **Two new nodes, a dedicated canonicalisation and a hash step** (2.1, 2.2), not compositions of
  existing ones: the NaN test needs IEEE's unordered compare, which the IR does not expose (its
  floating `Compare` is Spark's order), and a hash step written out is 17 to 30 nodes a column.
- **Row 342 is not needed; row 343a is.** The node emits its own add, so it waits on no floating
  arithmetic; but a floating value inside an integral kernel, which every floating hash is, needs
  343a's width rule (item 10).
- **The decimal above 18 is buildable** (item 7) but needs row 359's word leaf, which is not
  built. Open point 1 (decline at first, or build).
- **The two normalisation entries cannot count in the census** from a coverage row (item 4).
  Open point 5.
- **The NEON measurement has no runner** (item 11). Open point 2.
- **A math function's value hashes bit-exact by default.** By the owner's decision of 30 October
  2026 the math functions are bit-exact to vanilla Spark by default and the ULP contract is opt-in
  and off; so `hash` over a math function's value fuses at the default, and declines only where the
  opt-in contract binds that value (the milestone plan's risk 13 and question 2).

## 1. Goal and non-goal

`hash` and `xxhash64` fuse over any number of children of the numeric and temporal types -
`BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`, `DATE`,
`TIMESTAMP_NTZ`, `TIME` and both interval types - with Spark's per-type words, its seed fold and its
null skip, bit for bit; and the canonicalisation node exists as the one shared form of `-0.0` to
`0.0` and NaN to the canonical NaN, built from `NormalizeNaNAndZero` with
`KnownFloatingPointNormalized` read as a relabel, for the later rows that evaluate keys. Not this
row: `hive-hash`, the collation-aware variants, `crc32`, `md5`, `sha*` and `xxh3_*` (outside the
type set or the census), string, binary and nested children, the zoned `TIMESTAMP` (declined by the
registry), a Varka shuffle, join or aggregate, and the decimal above 18 if open point 1 is
answered (a).

## 2. Design

### 2.1 The canonicalisation node

`CanonicalFloat(child)`, a `FLOAT` or `DOUBLE` node over a child of the same lane:

    canon(x) = blend(x + 0.0, NaN_lane, isNaN(x))

with `NaN_lane` the lane's canonical NaN, `0x7fc00000` or `0x7ff8000000000000`, broadcast once in
the prologue, and `isNaN` the lane's `IS_NAN` test of the input. Three lane operations,
independent add and test, then the blend. Why each holds (item 5 checks them): under
round-to-nearest an exact zero sum of operands of opposite signs is `+0.0`, so `-0.0 + 0.0` is
`+0.0`; a non-zero finite or infinite `x` plus zero is `x`; a NaN stays a NaN of some payload, which
the blend replaces; the JVM keeps subnormals (no flush-to-zero), so they pass. The alternative,
`blend(x, +0.0, x == 0.0)` before the NaN blend, is four operations and is not taken.

- **Validity** is the child's (`NormalizeNaNAndZero` is null-intolerant); a null lane's value is
  never read.
- **Where it is built**: (a) for `NormalizeNaNAndZero(e)`, over `e`'s node; (b) as the first step of
  a floating child of `hash` or `xxhash64` (2.2), because that is Spark's hash step (item 2).
  Never before a comparison, an arithmetic node, a choice or an output: those keep their bits, and
  Spark's order already treats the zeros and the NaNs as equal (`VARKA-341.md` 2.5).
- **Idempotent, structurally.** `CanonicalFloat.of(x)` returns `x` when `x` is already a
  `CanonicalFloat` (item 5: `canon(canon(x)) = canon(x)` bit for bit), so `hash(KnownFP...(
  NormalizeNaNAndZero(x)))`, which is what a shuffle over a normalised join key hashes, emits one
  node, not two.
- **`KnownFloatingPointNormalized` is a relabel**: the compiler compiles its child. It never
  removes a canonicalisation by trusting the tag; only the structural rule above does, so a tag over
  a value that is not the node is harmless.
- **Rule 1 of `VARKA-475.md` 4**: a canonicalisation is a new kind of computation, not a new type.
  The composed form - 342's floating `IntArith(ADD)` and an `IfElse` over `x = NaN` - would need
  341's literal-specialised compare to be three operations (the general Spark-order equality is
  five, `VARKA-341.md` 2.5 and 5.1) and would make the node unrecognisable to the idempotence rule
  and to later rows.
- **For later rows**: a grouping key, a join key or a window partition key evaluated by a Varka
  operator in a later milestone takes this node from `NormalizeNaNAndZero` as Spark puts it there;
  the node's javadoc says it is the only canonicalisation, so no second form is written.

### 2.2 The hash step

`HashStep(HashFn fn, VarkaVectorIR value, VarkaVectorIR seed)`, `HashFn` being `MURMUR3` (seed and
result on the `INT` lane) or `XXH64` (on the `LONG` lane):

- **The value's lane picks the primitive**: a 32-bit value (`INT`, or `FLOAT` read as its bits)
  takes `hashInt`, a 64-bit one (`LONG`, or `DOUBLE` read as its bits) `hashLong`. The constructor
  refuses a narrow lane (the compiler widens it first, 2.3) and a floating value that is not a
  `CanonicalFloat`, so Spark's floating rule cannot be skipped by a later caller. A floating value
  is read through `viewAsIntegralLanes`, a reinterpretation C2 emits as nothing.
- **The null skip**: the result is `blend(seed, step(value, seed), valid(value))`, emitted only
  where the value can be null; the node's validity is the seed's, and the root seed is a literal,
  so a hash root is never null, as Spark's (item 1).
- **The fold**: `hash(c1, ..., cn)` is `HashStep(fn, cn, ... HashStep(fn, c1, LiteralSlot(seed)))`,
  in Spark's child order. The seed is the expression's own (`Murmur3Hash.seed`, `XxHash64.seed`),
  so a seed other than 42 works the same.
- **Murmur3 over a 64-bit value** is the low and high ints, each `NarrowLane`'s `L2I` of `v` and
  of `v >>> 32`, then two `mixK1`/`mixH1` steps and `fmix(8)`, in row 28's representation (the
  int species of the long width, low half used; item 6). **XXH64 over a 32-bit value** is the
  value widened and masked to `0xFFFFFFFF` in the long lane.
- **The lowering** is the primitives' own sequence: `IntVector.mul`, `lanewise(ROL)`, `XOR`,
  `LSHR`, `add`; `h * 5` as `(h << 2) + h`, one shift and one add against a `vpmulld`; the
  constants broadcast once in the prologue. The value-side half of each step (`mixK1(v)`, XXH64's
  `rotl(v * P2, 31) * P1`) does not read the seed, so a kernel of n children has n independent
  value chains and one serial seed chain, which the emitter writes value side first.
- **Rule 1**: a hash mix is a new kind of computation. The composition - `IntArith(MUL)`, `BitOp`
  with a new `ROL` kind, `IfElse` for the skip - is 17 nodes a 32-bit Murmur3 step and about 30 a
  64-bit one, which the budget, the shape key and the cost model would each price node by node; it
  is recorded as considered (section 7).

### 2.3 The compiler

A `VarkaHashCompiler` in a family `HASH` (`varka/VarkaFamily.java`, switch
`spark.sql.codegen.varka.family.hash.enabled`, declared in `SQLConf` with its binding policy),
claiming `Murmur3Hash`, `XxHash64`, `NormalizeNaNAndZero` and `KnownFloatingPointNormalized` by
exact class - so `CollationAwareMurmur3Hash`, `CollationAwareXxHash64` and `HiveHash` decline as
today - and `floatingDecline` admitting the four. Per child, by its Spark type:

| child type | the value given to `HashStep` |
|---|---|
| `BOOLEAN` | `CondValue(cond, INT)` (row 460): 1, 0, its validity the condition's |
| `TINYINT`, `SMALLINT` | `WidenLane(x, INT)`, sign extension (on either narrow arm) |
| `INT`, `DATE`, year-month interval | the node |
| `BIGINT`, `TIME`, `TIMESTAMP_NTZ`, day-time interval | the node |
| `FLOAT`, `DOUBLE` | `CanonicalFloat.of(x)` |
| `DECIMAL(p, s)`, `p <= 18` | row 347's leaf, the unscaled long, under its slot check |
| `DECIMAL(p, s)`, `p > 18` | open point 1: a decline naming row 337b, or 2.5 |
| `NullType`, a null literal | dropped: Spark's step is empty (`hash.scala:639`) or skips the null |

A child is any expression the chain compiles to a value of its type, `hash(a + 1, year(d))`
included; a child the chain declines declines the hash with its reason. The output binds an `INT`
or a `BIGINT` root.

### 2.4 Cost, per lane group, and what C2 makes of it

Lane operations of 2.2, before C2's lowering (registered by `dev/varka_emit.sh` once built):

| step | ops | multiplies | rotates |
|---|---|---|---|
| Murmur3, 32-bit value | 17 | 4 (`vpmulld`) | 2 |
| Murmur3, 64-bit value | 27, two of them `L2I` | 6 | 4 |
| XXH64, 32-bit value | 15 | 4 (64-bit) | 1 |
| XXH64, 64-bit value | 16 | 5 (64-bit) | 2 |
| canonicalisation | 3 | 0 | 0 |
| null skip | 1 | 0 | 0 |
| decimal above 18, Murmur3 (2.5) | about 95 | 7 | 7 |

On AVX-512 each is one instruction. On AVX2 each 64-bit multiply is seven instructions and each
rotate three (item 8), so an XXH64 64-bit step is about 50 instructions per four rows, against
about 15 scalar ones per row in the row engine's call: near parity in instructions, and the row
engine's per-row cost around the call is what a fused kernel saves. That is why the row asks for
XXH64 to be measured separately on AVX2. The one candidate for the AVX2 multiply is the explicit
three-`vpmuludq` form, `lo(a) * lo(C) + ((hi(a) * lo(C) + lo(a) * hi(C)) << 32)`, whose cross
terms C2 lowers to `vpmuludq` only if it sees the zero-extension pattern of item 8; the spike
(section 4) times it beside C2's form and it is built as an option only if it wins by the owner's
margin. On NEON without SVE every 64-bit multiply is two scalar multiplies (item 8), so XXH64 there
is scalar in effect; Murmur3 is native.

### 2.5 The decimal above 18, if built

Over row 359's two word leaves (`VARKA-359.md` 2.2), the construction of item 7 in long lanes: the
sign, the two leading-zero counts and the length; the byte reversal of each word and their swap;
the variable 128-bit right shift (two shifts and an or below 64 bits, one shift above, a blend at
0, since the Vector API takes a shift count modulo 64); the four chunks by `L2I`; then four Murmur3
int steps each under the mask `j < length / 4`, the tail chunk chosen by three blends, three byte
steps under `i < length % 4`, each byte sign-extended by a shift pair, and `fmix(length)` with the
length a lane value. XXH64: the seed plus `P5 + length`, two word steps, one int step, three byte
steps, `fmix`. The proof is SMT over bit-vectors (3.4), since 128 bits cannot be exhausted.

### 2.6 Reference, fuzzers, warm-up

`VarkaReference` evaluates `CanonicalFloat` by `NormalizeFloatingNumbers`' normalisers over bits and
`HashStep` by calling `Murmur3_x86_32` and `XXH64` directly, so the reference is Spark's code, not a
second copy of the mix. The IR fuzzer draws `HashStep` chains and `CanonicalFloat` in a sequence of
its own seed, so no existing corpus hash moves (row 352's rule, `VARKA-352.md` 434). The warm-up's
int and long blocks gain the rotates and the multiplies the steps call, its float and double blocks
the `IS_NAN` test, the add and the blend, held by `VarkaWarmupReachSuite`. The cost model gets a
feature per step kind, on row 352's awaiting-refit list, and the budget a weight row.

## 3. Tests and proofs

1. **Exhaustion at 32 bits** (the row's done-when), through the emitted kernels, against
   `Murmur3HashFunction` and `XxHash64Function` at seed 42: every `INT` (which covers `DATE`, the
   year-month interval and, by widening, every `TINYINT` and `SMALLINT`), every `FLOAT` pattern,
   and `BOOLEAN`'s two values and null; and every `FLOAT` pattern through `CanonicalFloat` against
   `FLOAT_NORMALIZER`, applied once and twice. In the nightly, as row 330's sweeps are, each a
   2^32 run with the reference computed in parallel.
2. **Sampled 64-bit regions**, bit for bit: the `DOUBLE` patterns of item 5 (the four NaN range
   endpoints, payload NaNs of both signs, both zeros, both infinities, the subnormal edges, the
   extremes) and drawn longs at the powers of two and their negatives, at seed 42 and at drawn
   seeds; every type of item 2 through the coverage differential with null patterns, the poisoned
   null lanes and both validity fills; the fold over two to eight children of mixed lanes, nulls in
   every child position, all children null.
3. **Byte lengths** (if 2.5 is built): values of byte length 1, 2, 8, 9, 15 and 16 (the row's
   regions) and each length from 1 to 16 at precisions 19, 20, 30 and 38, positive and negative,
   with the boundary values `10^p - 1`, `-(10^p - 1)`, `-128`, `-129`, `127`, `128`.
4. **SMT** (`sql/varka/proofs/hash.smt2`, over bit-vectors as `leap_hash.smt2` is, since the
   steps are Java's int and long operators exactly): the emitted step equals the primitive's
   expression for every seed and value - `h * 5 = (h << 2) + h`, the rotate as the emitter writes
   it, row 28's halves recombined - one check per step kind of 2.4; and, for 2.5, the length
   formula against the minimal two's complement width over every 128-bit value in `+-(10^38 - 1)`,
   and the shifted chunks against the byte array's definition. The four multiply steps are
   identical terms on both sides, so the solver's cost is in the decimal claims; priced from the
   spiked form, as `VARKA-245` taught.
5. **Coverage rows**, a family "hash": `hash` and `xxhash64` over each column type of the fixtures,
   over two and over four children of mixed lanes, over a narrow column on both arms, over a
   boolean, over a literal beside a column, over `x` and `f` with NaN and both zeros meeting, and
   over a decimal column at `(10, 2)` and `(18, 4)`; each naming its verification for row 490's
   check (`hash.smt2`'s sections, the sweeps, or `differential`).
6. **The normalisation through the harness**: `NormalizeNaNAndZero` and
   `KnownFloatingPointNormalized(NormalizeNaNAndZero(x))` built directly in row 509's
   `VarkaExpressionHarness`, since no query reaches them (item 4), and `hash` over the second, which
   emits one `CanonicalFloat`.
7. **Row 368c**: `HashExpressionsSuite` through the Varka evaluator once this lands
   (`VARKA-368.md` 5 step 3); this row's coverage rows are what it needs.
8. **The generated files** in `ADDING_AN_EXPRESSION.md`'s order; the PR build and the option
   matrix dispatched on the branch.

## 4. The baseline, the spike, and predictions

**The baseline first, its own pull request on the unchanged code**: a class
`VarkaHashBaselineBenchmark` (sql/core, on `VarkaSqlBenchmarkBase`, its own file by the per-feature
rule) timing the row path over Arrow-cached rows: `hash` and `xxhash64` over one `INT`, one
`BIGINT`, one `DOUBLE`, one `DECIMAL(18, 4)` and one `DECIMAL(38, 10)` column, over four mixed
columns, and with one row in ten null; run by hand on the EPYC 7763 through `benchmark.yml` at 256
and 128 bits, committed from the artifact as runner files.

**The spike** (catalyst test side, no emitter, in the baseline's pull request or its own): the
hand-written lanes of item 6 against a scalar loop over the same arrays, per step kind, at 512, 256
and 128 bits on the laptop and at 256 and 128 on the EPYC 7763 by hand; and the explicit
three-`vpmuludq` multiply beside C2's on AVX2 (2.4). Laptop numbers are labelled laptop. No CI
workflow runs either: the sweeps of the gate are run by hand.

Predictions, registered before the spike and the build:

1. **Murmur3 over 32-bit values** beats the row path by 5 to 20 times at 256 bits; it is native at
   every width on every host.
2. **XXH64 over 64-bit values on AVX2** beats the scalar loop by at most 1.5 times, and loses to it
   at 128 bits; it still beats the row path at 256 bits, by 2 to 5 times. On AVX-512 it beats the
   scalar loop by 2 to 4 times.
3. **The explicit multiply** ties C2's form within 1.3 times on the EPYC 7763 (Zen 3's `vpmulld`
   is cheap), so it is not built.
4. **The canonicalisation** adds at most 10% to a floating hash at 256 bits.
5. **No existing shape's bytes move**; the `option_arms` digests move with the new shapes.
6. **Test time**: four nightly 2^32 kernel sweeps at `VARKA-335.md`'s 7 s for a unary sweep, ratio
   taken as 1 since the admission's hand-written sweeps took 1.0 to 5.3 s, so about 30 s; the
   coverage rows within row 317's ratio of the differential's existing time.

## 5. Order, files and size

1. **The baseline class**, its own pull request.
2. **The spike**, and the census cells it needs (`int.MUL`, `int.ROL`, `long.MUL`, `long.ROL`, and
   for 2.5 `long.REVERSE_BYTES` and `long.LEADING_ZEROS_COUNT`) on the three hosts' censuses.
3. **337a**: `CanonicalFloat` and `HashStep`, the compiler and its family, the null skip and the
   fold, every type of 2.3 but the decimal above 18; the reference, the warm-up, the proof file's
   step claims, the sweeps, the coverage rows, the generated files; then the measurement against
   the baseline at both widths and 512 on the laptop, its predictions scored.
4. **337b** (open point 1): the decimal above 18 over 359's word leaves, its proof claims and
   byte-length regions.

Dependencies: row 343a's width rule (a floating value in an integral kernel, `VARKA-343.md` 2.2)
before the floating children; row 347's leaf before the decimal at 18 and below; row 359a's word
leaves before 337b; rows 28 and 460, done. Not row 342: the node emits its own add.

| file | what |
|---|---|
| `varka/VarkaVectorIR.java` | `CanonicalFloat`, `HashStep`, `HashFn`, canonical forms |
| `varka/VarkaVectorWalk.java`, `varka/VarkaBodyEmitter.java` | the lowerings, the constants |
| `varka/Analysis.java`, `varka/Slots.java` | the step's validity rule (the seed's) |
| `VarkaHashCompiler.java` (new), `VarkaNodeCompiler.java` | 2.3; `floatingDecline` |
| `varka/VarkaFamily.java`, `SQLConf` | the `HASH` family and its switch |
| `varka/VarkaEmitBudget.java`, the cost register, the warm-up | weights, features, reach |
| `sql/varka/proofs/hash.smt2`, `VarkaProofFiles.java` | 3.4 |
| tests | sweeps, regions, coverage rows, harness tests, fuzz sequence, reference arms |
| benchmark | `VarkaHashBaselineBenchmark`, the spike, their results files |
| generated | `coverage.json`, `docs/sql-varka.md`, `emitted_bytes.json`, `width_audit.json` |
| generated | `census.tsv` |

**Size: medium, as the row says, plus 337b.** The baseline about half a day, the spike about a
day, 337a about four days, 337b about three after 359a.

## 6. Ownership

| what | owner |
|---|---|
| `CanonicalFloat`; `NormalizeNaNAndZero`'s arm; `KnownFloatingPointNormalized` as a relabel | 337 |
| `HashStep`, `hash`, `xxhash64` over every type of 2.3 | 337 (the decimal above 18: 337b) |
| the floating lanes, their loads, Spark's order, `isnan` | 341, done in part |
| floating `+`, `-`, `*`, `/`, the exact functions | 342; not used by the node |
| a floating value in an integral kernel; the conversions; the reinterpretation cells | 343a |
| the decimal leaf at 18 and below; the word leaves above | 347; 359 |
| the hash suites through the evaluator | 368c, after this row |
| the hash forms in the Spark fuzzer | 352, once these coverage rows exist |
| a Varka grouping, join, window or shuffle over normalised keys | later, reusing the node |

The census rows `normalize_nan_and_zero` and `known_fp_normalized` name 337 alone (open point 5).

## 7. Out of this row, and declines that stay

Out: section 1's non-goal list. Considered and not taken: the composition of 2.2 and 2.1 (node
counts, and the idempotence rule needs a recognisable node), and the canonicalisation by a zero
compare (four operations, 2.1).

Declines that stay, each naming its row: a floating child before 343a; a decimal child before 347,
and above 18 before 337b; a child of a type outside the set (string, binary, nested, `TIMESTAMP`);
the collation-aware variants and `HiveHash`; a math function's value under the opt-in ULP contract
(the milestone plan's question 2).

## 8. Open points for the lead

1. **The decimal above 18.** (a) Decline it in 337a naming 337b, and build 337b after 359a; the
   row's done-when ("both fuse over every type in the set") is met at 337b, and the census's `dec`
   pairs count only then, by 2.8's rule that a decimal pair counts at every precision. (b) Build it
   in 337 after 359a, holding the row until then. Evidence: the construction is checked against
   Spark at every length (item 7), so feasibility is not in doubt; it is about 95 lane operations a
   value, against a row path that pays a `BigDecimal` and an array a row (359's baseline puts a
   `(38, 10)` comparison at 365 ns a row, `VARKA-359.md` 0 item 12); and 359 is planned, not
   started, so (b) holds the other 24 pairs behind it. *Recommended: (a).*
2. **The NEON measurement.** The row asks for XXH64 measured on NEON; `benchmark.yml` has no Arm
   runner (item 11), as row 359 found. (a) The N2's census cells only (`varka-width-audit.yml` runs
   on `ubuntu-24.04-arm`), with item 8's reading that the 64-bit multiply is scalar there. (b) An
   Arm job in `benchmark.yml`, a workflow change of its own row. *Recommended: (a)*, as 359 did.
3. **The canonicalisation's family.** (a) `HASH`, since its only reader in milestone 8 is the hash.
   (b) Row 342's `FLOATING_ARITHMETIC`, since it is a floating operation. *Recommended: (a)*;
   the family covers the nodes it claims, and a later key operator can move it.
4. **Reading "only where Spark inserts it".** This plan builds the node from `NormalizeNaNAndZero`
   and as the hash's own floating step (2.1), on the reading that the step is Spark's hash rule and
   not an insertion. Confirm, or name a narrower reading.
5. **The two normalisation entries in the census.** No query puts them in a `Project` or `Filter`
   (item 4), and the census credits a class only through a coverage row's operands. (a) Leave them
   in, uncounted, their rows column `337` and the note saying why no coverage row can show them.
   (b) Move them out of the denominator with that reason, by the owner's decision, as decimal
   division left it. (c) Teach the coverage table a harness-built row (3.6). *Recommended: (a)*
   until a later milestone's key operator gives them a query.

## 9. Done when

Row 337's text holds, with the corrections above: `hash` and `xxhash64` fuse over every type of
2.3 with Spark's per-type words, fold and null skip; the 32-bit sweeps of 3.1 pass against
`Murmur3HashFunction` and `XxHash64Function`, and the sampled regions of 3.2 (payload NaNs, the
four NaN range endpoints) bit for bit; `CanonicalFloat` matches `NormalizeNaNAndZero` over every
`FLOAT` and the sampled `DOUBLE` regions; `hash.smt2` proves the step claims; XXH64 is measured
separately on AVX2, AVX-512 and, as open point 2 is answered, NEON; the baseline is committed before
the build and the measurement after it, predictions scored; the PR build and the option matrix
pass. Under open point 1 (a), the decimal above 18 and its byte lengths 1, 2, 8, 9, 15 and 16 are
337b's, and the row closes with it.

## 10. Outcome

(Written when the work lands.)
