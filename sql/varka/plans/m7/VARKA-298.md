# VARKA-298: Nested Catalyst trees in the composition fuzzer

## 1. Where this came from

Row 298 of `m7/PLAN.md` (2.2), split off VARKA-262 when that task built the Spark-level
differential (`VARKA-262.md` 1 and 3.4): "nested Catalyst trees with neighbouring types, so the
analyzer inserts casts (`VarkaCatalystGrammar`, beside `VarkaIrGrammar`), and the shrinker reaching
inside a row's own expression". `VarkaSparkFuzzSuite` composes the 92 rows of
`sql/varka/coverage.json` as they are written: every output and conjunct is one fixed expression
over columns, so the trees the compiler receives are the table's own, at the table's depth, with
the operand types the table chose. The two things that follow are what this row is for. A row
whose operands meet a neighbouring type - an INT beside a BIGINT, an `INTERVAL MONTH` beside an
`INTERVAL YEAR` - reaches the compiler with a `Cast` the analyzer wrote, and only a handful of rows
do (`ym - ymm`, `CAST(ymy AS INTERVAL MONTH)`, the TIME and interval literals); nothing composes
them. And the shrinker of `VarkaSparkFuzz.shrink` reduces the parts of a composition (outputs,
conjuncts, rows) and never the inside of an expression, because there was nothing inside to
reduce: `VARKA-262.md` 3.4 left that for this row.

## 2. The admission check, done

Two questions decide whether a grammar of nested trees is worth drawing: does the analyzer insert
casts where the grammar puts neighbouring types side by side, and do the resulting trees still
fuse, or would the arm compare the row engine with itself. A throwaway suite (not committed) ran
87 hand-written expressions, one per production the grammar would have, each alone as the only
output or the only conjunct of a query over the fuzzer's view, under ANSI off and on, on
16 October 2026 at `7cc402b8f34`. It read the analyzed plan's casts, whether the Varka session's
plan had a Varka node, and the two engines' answers.

| neighbour pair | example | cast inserted | fuses as an output | fuses in a `WHERE` |
|---|---|---|---|---|
| INT beside BIGINT | `i + l`, `greatest(i, l)`, `coalesce(i, l)`, `year(d) = l` | int to bigint | **never** (13 of 13; an INT literal folds into a BIGINT one and does) | yes (`i < l2`, `year(d) = l`) |
| a TINYINT or SMALLINT literal where an INT goes | `i + 3Y`, `year(d) * 2S`, `date_add(d, 3Y)` | tinyint, smallint to int | yes | yes |
| the three year-month units | `ymm + ymy`, `greatest(ym, ymm)`, `coalesce(ymy, ymm)`, `if(c, ymm, ymy)` | to YEAR TO MONTH | yes, every one | yes (`ymm < ymy`, `ym >= ymm`) |
| day-time fields | `greatest(dt, INTERVAL '1' DAY)`, `coalesce(dt, INTERVAL '90' MINUTE)`, `greatest(t - t2, dt)` | to DAY TO SECOND | yes in `greatest`, `least`, `coalesce`; not in `if` or `CASE` | yes |
| a DATE beside a date string | `d < '2021-06-01'`, `coalesce(d, '2021-01-01')` | string to date under ANSI; **date to string under LEGACY** in `if` and `coalesce` | under ANSI only | yes, both modes |
| TIME literals of lower precision | `greatest(t, TIME'09:30:00')`, `t < TIME'09:30'` | none (the literal is typed TIME(6)) | yes in `greatest`, `least`, `coalesce` | yes |

Every pair inserts the cast the text does not write, and every answer agreed. Three things change
the design:

1. **A widened INT never fuses in a projection.** Varka's long lane has no arm for `Cast(int to
   bigint)`, so every BIGINT choice over an INT operand (`greatest(i, l)`, `coalesce(i, l)`,
   `if(c, i, l)`) falls back whole; BIGINT arithmetic does not fuse at all (`l + l2`, `l - 1`),
   so `i + l` would not either way. The grammar
   keeps the pair (in a `WHERE` it does fuse, and as one output among several it is a residual
   entry beside fused ones, which is the composition the fallback must get right), but at a weight
   that leaves most trees on productions that fuse. The gap itself is not this row's to close;
   it is recorded in 9.
2. **A date string is a neighbour only in a comparison.** `if(c, d, '2021-06-01')` is a DATE under
   ANSI and a STRING under LEGACY, so a parent that takes a DATE would type differently in the two
   modes, and `greatest(d, '2021-06-01')` does not analyze in either. The grammar uses the date
   string only as a comparison's right operand.
3. **A TIME plus an interval raises `DATETIME_OVERFLOW` in every mode** once the sum leaves the
   day (both engines, both modes, in the check), a condition the expected-errors list does not
   hold. A drawn interval over twelve drawn times leaves the day on nearly every batch, so a
   production `<time> + <day-time>` would turn every tree holding it into an error-against-error
   comparison. Only `t + INTERVAL '0' SECOND`, which the table already has, is drawn.

What the check would have rejected: a grammar whose trees mostly fall back - if the INT-to-BIGINT
pair, the only one that never fuses as an output, had been the whole of the neighbouring types, the
arm would have compared the row engine with itself.

## 3. The design

### 3.1 The grammar

`VarkaCatalystGrammar` (Java, catalyst test sources, beside `VarkaIrGrammar` and `VarkaShrinker`):
a typed grammar of SQL text over the view's twelve columns. Seven types (DATE, INT, BIGINT, TIME, a
day-time interval of any fields, a year-month interval of any unit, BOOLEAN); a production is a
template with `$0`, `$1`, `$2` for its children and the types they take; a node is a `record` of
its type, template and children, and renders its text. The productions are the coverage families
over typed operands - calendar fields of a DATE, day and month arithmetic, int arithmetic and
choice, the long lane's choices and the TIME accessors, year-month arithmetic, comparisons, null
tests, `IN` and the connectives - plus the neighbour pairs of 2: BIGINT productions that take an
INT operand, TINYINT and SMALLINT literals among the INT leaves, the three year-month columns and
interval literals of four day-time field ranges among the leaves, TIME differences (HOUR TO SECOND)
among DAY TO SECOND operands, and a DATE compared with a date string. A position whose argument
must be a literal of a form (a `trunc` format, a `time_trunc` unit, `next_day`'s day) is part of the
template, so no draw or shrink can put an expression there. Trees go at most three levels below an
operation at the root; a leaf is a column two times in three, else a literal.

### 3.2 The draw

Half the compositions of `VarkaSparkFuzz.draw` (`nestedIteration`: three in a row with, three
without, so the nested ones meet the safe and full pools, the wider pool, pivots and the partition
check in every combination) add one or two nested outputs and, on a filter one time in two or
without one a time in three, a nested conjunct, each at a random place among the coverage rows.
The trees come from a `java.util.Random` of their own, so the coverage rows, the data, the ANSI
mode and the pivot of every composition are the ones it drew before (`drawRows` is the old draw,
and a test holds `draw` to it). The `Case` carries a map from each nested output's or conjunct's
text to its tree; everything else about a case, the reproducer file included, stays text.

### 3.3 The shrinker

`VarkaShrinker.shrinkTree` is the IR shrinker's tree pass made generic over a `TreeMoves`
(children, rebuild, replacements): positions root first and then level by level, each node
replaced by the first smaller candidate that still fails, until none does. The grammar's
replacements keep a node's type, so the parent still types: each column of the type and its first
literal, then each child of the same type; for a boolean, `d IS NOT NULL` and its boolean children.
An output takes any type, so at an output's root every child is a candidate too (`freeRoot`). In
`VarkaSparkFuzz.shrink` the pass runs inside each nested output after the outputs' ddmin and inside
each nested conjunct after the conjuncts', every candidate checked by the same `still` (the same
kind of disagreement, within the same 400 runs).

### 3.4 What is deliberately unchanged

* `VarkaShrinker.shrinkTrees`, the IR shrinker, which keeps its own pass: its admissibility
  (protected positions, the constructors' refusals) is the IR's, and it pins the shrink results of
  `VarkaShrinkerSuite`.
* `VarkaSparkDifferential`, the check, and `VarkaSparkReproducerSuite`, the replay: a nested case
  is text like any other.
* The fixture and its pools (VARKA-372 keeps the view total; nothing here reads a new column).
* The expected-errors list, unless the census meets a condition it lacks (then an entry with its
  reason, in 9).
* No product code; no emitted code changes.

### 3.5 Registered op counts

None; no emitter change.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/src/test/java/.../codegen/varka/VarkaCatalystGrammar.java` | the grammar |
| `sql/catalyst/src/test/java/.../codegen/varka/VarkaShrinker.java` | `TreeMoves` and `shrinkTree` |
| `sql/core/src/test/scala/org/apache/spark/sql/execution/VarkaSparkFuzz.scala` | the nested draw, the case's trees, the shrink inside them |
| `sql/core/src/test/scala/org/apache/spark/sql/execution/VarkaSparkFuzzSuite.scala` | the tests of 5, and the census's nested line |
| `sql/varka/fuzz/expected_errors.tsv` | only if the census meets a new legitimate condition |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-298.md` | the row and this plan |

## 5. Tests, and what each is for

| test | what it catches that no other would |
|---|---|
| the grammar's trees analyze under ANSI on and off, and the analyzer inserts casts | a production that does not type in one mode, which would compare two analysis errors and test nothing; and a grammar that stopped meeting neighbouring types (at least a quarter of 300 outputs and 150 predicates carry a cast the text does not write) |
| a share of the grammar's trees fuse on their own | a grammar drifting onto productions Varka declines, so the arm compares the row engine with itself (at least a fifth of 100 outputs and of 100 predicates fuse alone) |
| half the compositions add nested trees and keep the coverage rows they drew | the nested stream moving the main one, which would move every seed's record |
| the shrinker reduces inside a nested output to the subtree that causes it | the shrink stopping at the output, as before this row (a BIGINT three levels over its cause becomes the INT `datediff(d2, d)`) |
| the shrinker keeps a nested conjunct boolean and each node inside it of its type | a reduction that breaks the typing, which the oracle would read as a new kind and reject at the cost of runs |
| the main test, at 200 compositions (half nested) | a disagreement over a nested tree |

The suite's other tests keep passing; the planted-rollback tests scan the planted seed's first 300
compositions, half of which now carry nested trees, so the composition they find may move (9 says).

## 6. The measurement

The census of `VARKA-372.md` 9 repeated on the nested draw: 5,000 compositions on each of seven
seeds (20261101 to 20261107), each seed its own JVM pinned to cores 12 to 23, reading the lines the
main test prints: the disagreements, the compositions that errored on both sides by pool and among
the nested ones, the conditions raised, and the time.

### 6.1 Predictions, registered before the run

1. **No disagreement in 35,000 compositions** (17,500 with nested trees). The coverage rows agreed
   over 35,000 in VARKA-372, and the nested trees mostly recombine the same compiled arms; the new
   surface is the casts. A disagreement would be a finding, with its shrunk case and a proposed row.
2. **The nested compositions error on both sides more than the others, 5 to 12% against 1 to 3%**:
   nested int arithmetic, `make_date` and `make_ym_interval` over drawn ints and `add_months` over
   nested month counts overflow on hostile rows, and under ANSI on safe ones too.
3. **No condition outside the expected-errors list**: the grammar raises the listed overflows and
   field errors, and the one new condition the check met (`DATETIME_OVERFLOW`) is not drawn.
4. **The run takes 10 to 30% longer per composition than the coverage-only draw** (VARKA-262 9:
   20,000 in 21 minutes, about 16 a second): half the compositions carry one or two more outputs
   and sometimes a conjunct. At the calibration table's ratio for checks that add work per query
   (about 1.5), 15 to 45%.

## 7. Risks

1. **A tree that analyzes differently in the two modes** (the date string of 2) compares
   different queries under one seed. The analysis test runs every drawn tree under both modes;
   a type that differs between them is not caught by it, which is why the date string is only a
   comparison's operand.
2. **Error-against-error comparisons crowding out values** (VARKA-372's lesson). The census
   reports the nested compositions' both-errored count apart; above a third, the grammar's value
   domains need narrowing.
3. **The grammar drifting from what fuses** as the compiler grows: the fuse-alone floor catches a
   collapse, not a slow drift; the info lines say the share each run.
4. **The shrinker sliding to another bug of the same kind** inside a tree; the kind is as coarse
   as it was for the parts (`VARKA-262.md` 3.4), and a shrunk case is re-read before it is believed.

## 8. Sequencing

1. This plan, with the row marked, after the admission check.
2. The grammar, `shrinkTree`, the nested draw and the shrink, with the tests of 5.
3. The census on seven seeds, the expected-errors list if it must change, then section 9.

## 9. Outcome
