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

Done on 16 October 2026, at `7cc402b8f34` (master) plus this branch.

**What was built.** `VarkaCatalystGrammar` (3.1); `VarkaShrinker.TreeMoves` and `shrinkTree`
(3.3); in `VarkaSparkFuzz`, the nested draw on half the compositions (`drawRows` is the old draw),
the case's map from text to tree, and the shrink inside nested outputs and conjuncts; in the
suite, the five tests of 5 plus one for the race of 9.3, and an info line for the nested
compositions. One entry joins `sql/varka/fuzz/expected_errors.tsv`. Three switches were added
along the way: `-Dvarka.sparkfuzz.nested=false` (no nested trees, which replays a seed's draw as it
was before this row), `-Dvarka.sparkfuzz.maxFailures` (default 3; a census needs it raised, or a
seed's third disagreement ends its run), and the rerun of 9.3. No product code changed.

*Corrections to 1-8, found while building.* The grammar lost two productions the plan listed:
`extract(YEAR | MONTH FROM <ym>)` takes only a field its interval has, so the operand is widened to
YEAR TO MONTH first by `+ INTERVAL '0-0' YEAR TO MONTH`, a cast the analyzer writes; and
`<ym> * 2` is gone, because an overflowing interval multiply raises a bare `ArithmeticException`
in every mode on both engines (Spark's `MultiplyYMInterval`), which reaches the fuzzer as the job's
`org.apache.spark.SparkException` with no condition. Listing that class would excuse every task
failure; the coverage row `make_ym_interval(year(d), month(d)) * 2` keeps the multiply drawn over a
domain that cannot overflow. The planted-rollback tests now draw `drawRows`: at the planted seed's
composition 5, a nested tree meets the planted rollback's branch with no force at all ("values
differ" unforced), and those tests are about the force.

*Corrections from the review of the diff* (`/code-review high` on the branch, ten findings, all
taken). The nested compositions are five in a row with and five without, not three (3.2): with a
period of six, every nested pivot case fell in the safe pool and every nested partition check in a
pivot case. The warmed arm and the pivot test draw `drawRows`, since both would meet 9.2 and 9.3
with no rerun. A tree that renders as one of its composition's coverage rows (`year(d)` is both) is
not added, so `trees` names nested trees only. The shrinker keeps a racing kind only if it shows
twice, `shrinkTree` does not ask again about a candidate that passed, the reruns of 9.3 leave the
census counters as they were, and the grammar's depth is stated as it is: a boolean due at
`MAX_DEPTH` takes leaves one level deeper. The census below is the final code's; the two earlier
runs (the discovery run and one at the period of six) are in 9.1's last paragraph.

### 9.1 The census

Seven seeds (20261101 to 20261107), 5,000 compositions each, one JVM per seed pinned to cores 12
to 23, `-Dvarka.sparkfuzz.maxFailures` lifted so that no seed stops at its third disagreement, on a
machine shared with other agents' builds (load 20 to 58), on the final code:

| | VARKA-372 9 (coverage rows only) | this run |
|---|---|---|
| compositions | 35,000 | 35,000, of which 17,490 with nested trees |
| erroring on both sides, of 5,000 | 14 to 68 | 48 to 110 |
| of them, among the about 2,500 nested per seed | - | 43 to 77 (1.7 to 3.1%) |
| of them, among the about 2,500 others per seed | - | 5 to 37 (0.2 to 1.5%) |
| safe ANSI-off pool erroring on both sides | 0 | 0 |
| disagreements | 0 | 4, all in nested compositions and all of 9.2's kind |
| two listed errors racing (9.3), absorbed by the rerun | - | 0 |
| unexpected conditions | 0 | 0 |

From the tests of 5 at the default seed: 144 of 300 nested outputs and 90 of 150 nested
predicates carry a cast the analyzer inserted; 41 of 100 outputs and 80 of 100 predicates fuse
alone.

Two earlier runs at the same seeds led here. The first, a discovery run with the cap of three,
stopped six seeds after 2,000 to 2,800 compositions on the multiply and the condition of 9.4, and
met the race of 9.3 twice. The second, with those fixed and the cap lifted but before the review's
corrections (the period of six), found four disagreements, three of 9.2's kind and one race.

### 9.2 Finding: a fused conjunct hides an earlier residual conjunct's error (proposed row)

**A Varka bug, not fixed here.** When Varka splits a filter, the conjuncts it fuses run in the
`VarkaFilterColumnarToRow` below the row `Filter` that holds the rest. Spark's `Filter` evaluates
its conjuncts in their optimized order and stops at the first false one; Varka evaluates the fused
ones first. So a row that a fused conjunct rejects never reaches a residual conjunct that Spark
evaluates *before* it and that raises on that row: Spark raises, Varka answers. Under ANSI most
conditions do this; year-month interval arithmetic raises in every mode, so it happens under
LEGACY too. It is the mirror of row 273, which gave the fallback the whole condition in Spark's
order and assumed the raising conjunct is the fused one. Five shrunk reproducers across the census
runs (three of them found in two runs):

* seed 20261105, iteration 291 (ANSI): `SELECT weekofyear(d) AS c0 FROM fz WHERE
  (((12 + l) = 5000000000)) AND (l = l2)` over one row with `l` = -9223372036854775808 and `l2` =
  9223372036854775807. The optimizer's `InferFiltersFromConstraints` adds `(l2 + 12) = 5000000000`
  and puts it first: Spark's condition is `((l2 + 12) = 5000000000) AND isnotnull(l) AND
  isnotnull(l2) AND ((l + 12) = 5000000000) AND (l = l2)`. Spark raises `ARITHMETIC_OVERFLOW` on
  `l2 + 12`. Varka fuses `isnotnull(l) AND isnotnull(l2) AND (l = l2)`, which rejects the row, and
  the residual `Filter` never sees it: no rows.
* seed 20261104, iteration 2199 (ANSI): `SELECT l AS c0 FROM fz WHERE ((date_from_unix_date(i) >=
  make_date(i, i, i))) AND (d IN (DATE '2021-01-01', DATE '2021-06-01'))` over three rows, one with
  `i` = 3840. Spark raises `DATETIME_FIELD_OUT_OF_BOUNDS.WITHOUT_SUGGESTION` (month 3840) from the
  first conjunct; Varka fuses the `IN`, which rejects that row first.
* seed 20261102, iteration 4775 (ANSI): `SELECT dt AS c0 FROM fz WHERE NOT (((ym + ym) IS NOT
  NULL)) AND (d = d2)` over one row whose `ym` is 2147483636 months and whose `d` differs from
  `d2`. Spark raises `INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION`; Varka fuses `d = d2`.
* seed 20261105, iteration 3145 (ANSI): `... WHERE NOT (((- (i * 2)) < (-42L))) AND NOT (l = l2)`
  over two rows, one with `i` = 2147483647 and `l` = `l2`: `ARITHMETIC_OVERFLOW` on `i * 2` in
  Spark, the row rejected by the fused `NOT (l = l2)` in Varka.
* seed 20261106, iteration 4069 (LEGACY): `... WHERE ((((ymy - INTERVAL '3' MONTH) + ymy) >= ym))
  IS NULL AND (dt > dt2) IS NULL` over four rows, one with `y` = 178956969:
  `INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION` in Spark with ANSI off, the row rejected by the
  fused null test of `dt > dt2` in Varka.

Proposed row: "A fused conjunct runs before a residual conjunct that Spark's optimized `Filter`
evaluates first, so where that residual conjunct raises (under ANSI, or year-month interval
arithmetic in any mode) Varka answers and Spark raises (VARKA-298 9.2, five reproducers; the mirror
of row 273). Decide between fusing only the conjuncts before the first residual one that can
raise, and evaluating the residual conjuncts first on the batch; save the reproducers under
`sql/varka/fuzz/spark/` as `known` until then." The fix is open: the first choice gives up fusion
wherever a raising residual conjunct comes first, the second gives up the kernel's row reduction
for the residual ones.

**What it means for the nightly.** At four in 17,490 nested compositions, the nightly's 50,000 at
the day's seed (about 25,000 nested) will meet five or six a night on average and fail its
`sparkfuzz` step until the row is fixed: a `known` reproducer in `sql/varka/fuzz/spark/` makes the
replay suite expect the disagreement, but the main test fails on any disagreement it draws, known
or not, so keeping the files does not keep the nightly green. Either the owner accepts a red
`sparkfuzz` step until the row lands, or the nightly runs with `-Dvarka.sparkfuzz.nested=false`
until then; `dev/varka_nightly.sh` is not this task's to change.

### 9.3 Finding: two legitimate errors race across partitions (harness, proposed row)

The discovery run's seed 20261106, iteration 4259, and the second run's seed 20261106, iteration
3861: two outputs that each raise a listed condition on different rows (`d - ym` overflowing,
`make_date(3, 3, unix_date(d))` out of range). The view has two partitions (the sessions run on
`local[2]`), and whichever task fails first decides what the query raises, on either engine and
from one run to the next: replayed eight times in one JVM, the reproducer of 4259 agreed five times
and disagreed three, with either engine raising either condition. It is not Varka's: both engines
flip. Nested trees make it more likely, since more of a composition's outputs can raise. The suite
now reruns a disagreement whose two conditions are both listed for that mode (`racingErrors`)
three times and believes it only if every rerun disagrees, counting the others in an info line.
Four of five 300-composition runs at the default seed met one and passed on its rerun (one before
the rerun existed failed on composition 203 of that seed, `make_date(2021, i, 1)` beside `(d2 +
ymm) + ymy`); the PR arm's 200 compositions stop short of it, and the final census met none. A
deterministic Varka bug that raises the wrong listed condition still fails, since it repeats.

Proposed row: "The Spark-level differential's view has two partitions, so two listed errors on rows
in different partitions race and the query's error is either one, on either engine (VARKA-298 9.3).
Make the comparison deterministic - one partition for error cases, or the set of conditions each
row raises compared instead of the first - and drop the rerun." The view keeps its two partitions
here: one partition would take the multi-partition, multi-task runs out of every composition.

### 9.4 The expected-errors list

One entry: `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION`, both engines, ANSI and LEGACY,
raised by `make_ym_interval(i, i)` and by `d - ymm` (negating an `INTERVAL MONTH` of
`Int.MinValue` months), both reachable only with drawn ints in those positions, so only the nested
trees meet it; both engines raise it in both modes (checked by replay). The final census met it in
six of seven seeds. `DATETIME_OVERFLOW` (2) is not listed, since the grammar does not draw it.

### 9.5 Predictions scored

1. **Refuted.** Four disagreements in 35,000 compositions, all one real Varka bug (9.2); the earlier
   runs also met a harness race (9.3) and two classes of grammar errors (the conditionless multiply
   and the unlisted condition of 9.4).
2. **Refuted on the magnitude, held on the direction.** The nested compositions error on both
   sides at 1.7 to 3.1%, against 0.2 to 1.5% for the others, not 5 to 12% against 1 to 3%.
3. **Refuted.** One condition outside the list (9.4) and one with no condition at all (the
   multiply), both legitimate and raised identically by both engines.
4. **Refuted: no cost measurable on this machine.** The 300-composition run at the default seed,
   five times with the nested trees and five without, alternating, took 70 to 306 s and 85 to
   432 s, the spread being the load of other agents' builds; by minimums, 70 s with and 85 s
   without. An earlier pair under load 48 read 148 s against 85 s at 200 compositions, which is the
   machine, not the code. The calibration table in `skills/testing-and-debugging.md` would take a
   line (predicted 10 to 45%, measured none above the noise); that file is not this task's.

### 9.6 Left for later

* The two proposed rows of 9.2 and 9.3.
* **A widened INT on the long lane** (2): Varka declines every BIGINT choice over an INT operand
  (`greatest(i, l)`, `coalesce(i, l)`, `if(c, i, l)`), and fuses none of BIGINT arithmetic, so a
  projection that mixes the two lanes falls back whole. A `Cast(int to bigint)` arm on the long
  lane is the scope item; with the arithmetic it belongs with milestone 8's new numeric types.
* **A TIME plus an interval** raises `DATETIME_OVERFLOW` in every mode on both engines and is not
  drawn (2); drawing it needs a value domain that keeps most sums inside the day.
* The shrink inside a conjunct is skipped under a pivot (a smaller conjunct may take another value
  on the pivot row), and the outputs' ddmin can drop the forced output and leave the index pointing
  at another (VARKA-296's, unchanged).

