# Spark's built-in functions, as Varka's port sees them

*Opened 19 September 2026, on the owner's statement that ideally every Spark SQL
function is ported to Varka. A catalogue, not a plan: it says what each family
would need, which needs are shared, and one measured fact about the math family
that decides how it can be planned. Nothing here is built until a task claims
it.*

## 1. The census

`FunctionRegistry` registers **511** built-in scalar, aggregate and generator
functions. Varka fuses **46 Catalyst classes** today (`sql/varka/coverage.json`,
57 rows), nearly all of them dates and int32 arithmetic. By the registry's own
section headers:

| family | count | what a Varka port needs | where it stands |
|---|---:|---|---|
| datetime | 64 | int32 and int64 lanes; the calendar algorithms | Varka's home ground; `TIME` is milestone 5's subject |
| math | 61 | **a double lane**; the Vector API's math operators | section 2 and 3 |
| aggregate | 76 | the aggregation operator, milestone 6's target | `SCOPE_MILESTONE_6.md` items 4 and 5 |
| string | 67 | a string representation | `SCOPE_MILESTONE_6.md` item 80's family |
| predicate, conditional | 13 + 10 | compares, blend, `IS_NAN` | largely fused; `LIKE` and the regexes are strings |
| bitwise | 6 | shifts and counts, all in the Vector API | `SCOPE_MILESTONE_6.md` item 32 |
| hash | 9 | xxhash64 and murmur over lanes | feasible for numeric inputs; nothing scoped |
| array, map, lambda, collection, struct | 54 | variable-length nested values per row | far; no representation |
| window | 9 | operator-level, not expression-level | after aggregation |
| JSON, XML, CSV, Avro, Protobuf, variant, URL | 42 | parsing a string into structure | outside the lane model |
| datasketch, vector, ST, misc | 78 | sketch state, geometry, seeded random, session facts | outside the lane model |

Read down the third column and "all of them" is five representation problems
and a tail. The five: dates and times, which are in hand; **doubles, which are
not started** - today a double lane exists only as a conversion target inside one
node's lowering; strings; nested types; and aggregate state. The tail is the
parsing, sketching and session families, which are not vector work whatever the
representation.

The counts are the registry's sections and not a claim about difficulty: one
`aggregate` row is `sum` and another is `percentile_approx`.

## 2. The math family, function by function

Sixty-one registered names. What each needs, given a double lane:

**One Vector API operator each - 23.** `sin cos tan asin acos atan sinh cosh
tanh exp expm1 log ln log10 log1p cbrt sqrt pow atan2 hypot abs negative
positive`, and `signum` as a compare and a blend. The operators exist in JDK 25's
`VectorOperators` (`SIN COS TAN ASIN ACOS ATAN EXP LOG LOG10 SQRT CBRT SINH COSH
TANH EXPM1 LOG1P`, and `ATAN2 POW HYPOT` as binaries), and C2 lowers them to a
vector math library rather than to Java: Intel's SVML on x86 (`libjsvml.so`,
symbols of the form `__jsvml_sin8_ha_z0`), a SLEEF derivative on aarch64. The
JDK's own `legal/jdk.incubator.vector/sleef.md` is the notice.

**Composites of those - about a dozen.** `log2` is `log(x) / log(2)`; `cot`,
`sec`, `csc` are reciprocals of `tan`, `cos`, `sin`; `degrees` and `radians` are a
multiply; `e` and `pi` are constants; `asinh`, `acosh`, `atanh` are Spark's own
log-and-sqrt formulas in `ExpressionImplUtils`, which a kernel reproduces
term by term rather than through an operator the Vector API does not have;
`factorial` is a table of 21 entries; `width_bucket` is a divide and a compare.

**The rounding gap.** `VectorOperators` has **no rounding operator** - no
`FLOOR`, `CEIL`, `RINT`, `ROUND`. So `floor`, `ceil`, `ceiling`, `rint`, `round`
and `bround` are built, not called: the 2^52 add-and-subtract that
`VarkaLoopEmitter.emitMagicDivide` already uses for its floor, or a `D2L`
conversion and back. Spark's `floor` and `ceil` return `LONG` for a double
argument, so for them the conversion *is* the result. `round` and `bround` on a
double go through `BigDecimal` in Spark, which a kernel has to match digit for
digit; `truncate` likewise.

**Division by a column.** `mod`, `pmod` and `div` are the integral family. Where
the divisor is a constant they are task 88's magic multiply; where it is a
column there is no magic, and the double route serves them as
`a - trunc(a / b) * b`, exact below 2^53 under the bound task 88 states. That is
a new lowering, not a variant of an existing one.

**Not lane work.** `bin`, `hex`, `unhex`, `conv` produce strings. `rand`,
`randn`, `random`, `uniform` are seeded per partition and Spark's answers depend
on the row order the generator sees. `try_add`, `try_subtract`,
`try_multiply`, `try_divide`, `try_mod` are the overflow family and belong with
tasks 63 and 104, not here.

## 3. The measured fact: which of these can be bit-exact

Varka's contract is that a fused result equals the row engine's. For the math
family the row engine is a scalar library call, and Spark uses **two** scalar
libraries:

* `java.lang.Math` for `sin cos tan asin acos atan sinh cosh tanh cbrt sqrt
  atan2 hypot rint signum`;
* `StrictMath` - fdlibm, the portable reference - for `exp expm1 log ln log10
  log1p pow`, and `log2` through `StrictMath.log`.

Whether a lane agrees with the scalar call is a property of the host's two
libraries, so it was measured rather than reasoned about.
`dev/varka_canary/MathLaneProbe.java` runs each operator over 262144 inputs
after C2 and compares every lane against both libraries. On 19 September 2026,
JDK 25, this repository's Zen 5, with the JVM's own log confirming an
`__jsvml_*_ha_z0` symbol resolved for every operator and `PrintIntrinsics`
refusing none:

| operator | lanes differing from `java.lang.Math` | from `StrictMath` |
|---|---:|---:|
| `SIN`, `COS`, `TAN` | 0 | 8707, 8726, 9549 (1 ulp) |
| `EXP`, `LOG` | 0 | 24936, 12921 (1 ulp) |
| `LOG10` | 0 | 24198 (2 ulp) |
| `TANH`, `CBRT` | 0 | 4772 (2 ulp), 21962 (1 ulp) |
| `EXPM1`, `LOG1P`, `ATAN` | 0 | 0 |

**The lanes are `java.lang.Math` bit for bit**, on every operator and every
input tried - HotSpot's scalar `Math.sin` intrinsic and `libjsvml` are both
Intel's, and they agree. Two consequences follow, and they cut the family in
half:

1. **Every function Spark computes with `java.lang.Math` ports bit-exactly, for
   free**: `sin cos tan asin acos atan sinh cosh tanh cbrt sqrt atan2 hypot`,
   and the composites over them. The contract holds without a proof.
2. **`exp`, `log`, `ln`, `log2`, `log10` and `pow` cannot be bit-exact through
   the Vector API on this host.** Spark computes them with fdlibm; the lanes
   compute them with SVML; and the two differ by one or two units in the last
   place on between five and ten percent of ordinary inputs. `expm1` and
   `log1p` happened to agree on every input tried, which is worth knowing and
   not worth relying on. `pow` is a binary operator the probe does not yet
   drive, and there is no reason to expect it to be the exception.

For those six the options are three, and none is free. A **ULP contract** in
place of bit identity - the tier SLEEF names `_u10`, the place
`SCOPE_STANDARD_MODE.md` keeps for a deliberate deviation - which also has to
say what the ghost fallback means when one query is answered half by each
library. A **Varka-emitted fdlibm**: `exp` and `log` are a table and a short
polynomial, they vectorise, and a lane that reproduces fdlibm's arithmetic
reproduces its bits; bit-exact, slower than SVML by a factor to be measured,
and a kernel to maintain. Or a **decline** until Spark itself moves off
`StrictMath`, which it has shown no sign of doing. `SCOPE_MILESTONE_6.md` item
36 holds the decision.

**All of this is one host.** On aarch64 the lanes are SLEEF, and whether
`Math.sin` is an intrinsic there or a call into fdlibm decides which half of the
table that platform lands in. The probe is committed so that the answer is a
run and not a guess; task 121's runners and the aarch64 CI job are where it
runs next.

## 4. What is shared across the families

The families are separate representations, but three needs recur and are worth
building once:

* **A double lane** unlocks the whole math family, `nanvl`, `isnan`, and every
  numeric function whose argument is a double. It is the single largest
  unblocker in the table and is not scoped.
* **Rounding without a rounding operator** is needed by the math family, by
  `round`-style datetime functions, and by any decimal work later. One helper,
  measured once.
* **A per-function exactness statement** - bit-exact, or a ULP bound, or
  declined - is what section 3 forces on the math family and what the standard
  mode register already asks of intervals. It generalises: every ported
  function should say which it is, and the coverage table should carry the
  answer.

## 5. Sources read for this catalogue

`FunctionRegistry.scala` (the census), `mathExpressions.scala` and
`ExpressionImplUtils.java` (which library each function calls, and the
composites), JDK 25's `VectorOperators` (the operator set, enumerated from the
running JVM rather than from documentation), and the SLEEF and OpenVML
libraries, whose reading is recorded in
`sql/varka/skills/vector-api-and-width.md` under the SLEEF heading. SLEEF has no
integer division and never converts a 64-bit integer; what it contributed is
listed there.
