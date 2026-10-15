# VARKA-319: An expected-errors list in the differential fuzzers

*Row 319 of `m7/PLAN.md` (2.4, "Errors and troubleshooting"), added by the owner on
12 October 2026. Planned on 15 October 2026 at master `e82396c55e1`. Section 2 corrects the row
in one place: only one fuzzer reads Spark's error conditions.*

## 1. Where this came from

The row: SQLancer keeps, per generator, a list of the errors a randomly drawn statement may
raise for a legitimate reason (`ExpectedErrors`), with the cause of each in a comment; an error
on the list is skipped and any other error is a finding (`m7/READING.md` 1). The Spark-level
differential (VARKA-262) compares the two engines' error conditions (`getCondition`) but keeps no
such list: two engines raising the same condition pass, whichever condition it is, so
`CAST_OVERFLOW` from an ANSI cast and an `INTERNAL_ERROR` from a planning bug are read alike.
Done when the list is committed with a reason per entry and a planted unexpected condition fails
the fuzzer.

## 2. The admission check, done

**Where an error is consumed.** Every place the Spark-level fuzzers
(`VarkaSparkFuzz.scala`, `VarkaSparkDifferential.scala`, `VarkaSparkReproducerSuite`) read one:

| site | what it does with an error today | here |
|---|---|---|
| `outcomeOf` | collapses a throw to the first `SparkThrowable` condition in the cause chain, else the exception's class name | unchanged; its string is what the list is keyed by |
| `difference` | both sides raise the same string: pass; different strings, or one side only: a disagreement | unchanged; the new check runs before it |
| `disagreement` (main arm, forced runs) | passes `difference` as above | reads the list first, both sides |
| `warmedDisagreement` | the same, for every run of the handover | reads the list first |
| `partitionBroken` | returns None when any of its four queries raised | reads the list for each raise, so an unlisted one is reported; a listed one still says nothing about the rows |
| `pivotMissing` | returns None when a side raised | unchanged: the main check has already read the same two outcomes |
| `rectify` | drops a conjunct the row engine raises on, to build the draw | unchanged, and recorded in 7: it consumes errors to shape the data, not to judge an engine |
| `VarkaSparkReproducerSuite` | replays through `disagreement` | reads the list through it |

The catalyst-level fuzzers (`VarkaIrFuzzSuite`, `VarkaCoverageCompositionFuzzSuite`) do not
compare Spark's conditions: they run Varka's reference evaluator, whose errors are Java
exceptions of Varka's own, and treat every `Throwable` as a failure already. The row's "differential
fuzzers" is, in this repository, the Spark-level one.

**Which conditions occur.** A throwaway suite (not committed) ran compositions through both
sessions and recorded, for every raise, the condition, the side, the ANSI setting and the family of
the coverage rows that raise it alone (each output and conjunct re-run alone; a condition every
part raises is the fixture's). Two seeds, 1,500 compositions each, on 15 October 2026:

| condition | side | mode | raised by | seen |
|---|---|---|---|---|
| `CAST_OVERFLOW` | both | ANSI and LEGACY | the fixture: its interval columns are casts of drawn ints, and a hostile int overflows the cast when the cached relation is built, so every query over that fixture raises | 71+54 and 76+47 per side, about 8% of the compositions |
| `CAST_OVERFLOW` | both | LEGACY | Day arithmetic (`d + CAST(i AS INTERVAL DAY)`: the int-to-day-interval cast is checked in every mode) | 1 |
| `ARITHMETIC_OVERFLOW` | both | ANSI | Integer arithmetic (`i + 1`, `year(d) + i`) | 2 |
| `ARITHMETIC_OVERFLOW` | both | ANSI and LEGACY | Months and year-month intervals (`add_months(d, i)` adds with `Math.addExact` in both modes) | 2 and 2 |
| `DATETIME_FIELD_OUT_OF_BOUNDS.WITHOUT_SUGGESTION` | both | ANSI | Calendar fields (`make_date(2021, i, 1)` with `i` outside 1 to 12; LEGACY answers null) | 6 and 5 |
| `EXPRESSION_DECODING_FAILED` | both | ANSI | Day arithmetic (`date_add(d, i)` overflowing: Spark's own `ArithmeticException` arrives wrapped in this condition) | 1 |

Every raise was on both sides at once, with the same condition; no run met a condition on one
side only, a Java exception that is not a `SparkThrowable`, or `INTERNAL_ERROR`. The list is
therefore five entries (the two `CAST_OVERFLOW` rows merge), all `both`. Of the 3,000 compositions, none was a combination of parts that
raised only together.

What the check would have rejected: a list keyed by condition alone. `ARITHMETIC_OVERFLOW` is
expected under ANSI for integer arithmetic and under both modes for `add_months`, but `i + 1`
overflowing under LEGACY would be a wrapped value in Spark, so the entries are keyed by mode.

*Not rejected, found on the way.* About 8% of the compositions read a fixture that overflows its
interval casts, so they compare an error with an error and nothing else. That is the fixture's
draw, not this row; it is recorded as a debt in 9 rather than fixed here.

*Correction, made while testing (15 October 2026).* The census above was too short to be
complete, and the list it produced was caught out by the first long run: 5,000 compositions at seed
11 failed on `INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION` (`ym - ymm`: year-month interval
arithmetic is checked in every mode, both engines raise it), and seed 13 on
`EXPRESSION_DECODING_FAILED` under LEGACY (`date_add(d, i)` overflows in every mode, not only
under ANSI). Neither is a Varka bug (both engines agree); the list became six entries, the last
entry's modes widened, and "five entries" in 3 to 5 reads six. The census needs about 5,000
compositions per seed to see a condition that one in a few hundred compositions raises.

## 3. The design

### 3.1 The list and its reader

`sql/varka/fuzz/expected_errors.tsv`, tab separated and `#` for comments, as
`known_failures.tsv` is: condition (or an exception class name, for a non-Spark throw), side
(`row`, `varka` or `both`), the modes (`LEGACY`, `ANSI`, comma separated), the coverage families
that raise it (semicolon separated, or `fixture`), and the reason. The fuzzer's two modes are the
ANSI setting; a `TRY` mode does not exist because a `try_` row is not a setting and is expected to
raise nothing, so a raise from one is unlisted by construction.

`VarkaExpectedErrors` (Java, a record `Entry`, next to `VarkaKnownFailures`' pattern: parsed from
`spark.test.home`, read once) answers one question, `unexpected(outcome, side, mode)`.
`VarkaSparkFuzz.unexpectedError(off, on, ansi)` applies it to the row engine's outcome and
Varka's and returns the kind `unexpected error condition X from the row engine under ANSI` (or
Varka); `disagreement`, `warmedDisagreement` and the forced runs call it before `difference`, and
`partitionBroken` takes the same check as a parameter for its queries. The kind names the
condition, and the shrinker keeps a failure of the same kind, so the reproducer is the smallest
case that raises it.

A listed condition on one side only is still a disagreement: `difference` runs after the list.

### 3.2 What is deliberately unchanged

* The comparison of two outcomes (`difference`), the draw, the shrinker, the reproducer format.
* Row 317 (errors compared as answers on the ANSI rows of the coverage differential) is the
  neighbour and builds on this list; it is not built here.
* The catalyst-level fuzzers (2).
* `rectify`'s dropping of a conjunct (7).

### 3.3 Registered op counts

None: no emitted code changes.

## 4. Files

| file | what |
|---|---|
| `sql/varka/fuzz/expected_errors.tsv` | the list, five entries with a reason each |
| `sql/core/src/test/java/org/apache/spark/sql/execution/VarkaExpectedErrors.java` | the parser and the lookup |
| `sql/core/src/test/scala/org/apache/spark/sql/execution/VarkaSparkFuzz.scala` | `unexpectedError`, and `partitionBroken` reading the check |
| `sql/core/src/test/scala/org/apache/spark/sql/execution/VarkaSparkDifferential.scala` | the calls, and the census of what was raised |
| `sql/core/src/test/scala/org/apache/spark/sql/execution/VarkaSparkFuzzSuite.scala` | the tests of 5 |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-319.md` | row 319's cell; this file |

## 5. Tests, and what each is for

1. **A planted unexpected condition fails the case, end to end.** `i / 0` under ANSI over one
   non-null row raises `DIVIDE_BY_ZERO` on both engines, which is on no entry; `disagreement`
   must return a kind naming `DIVIDE_BY_ZERO`, and the same case with the entry added (a list
   parsed from text) must pass. Catches a check that is skipped on the path both engines agree on.
2. **A listed condition passes**: `make_date(2021, i, 1)` with `i = 13` under ANSI raises
   `DATETIME_FIELD_OUT_OF_BOUNDS.WITHOUT_SUGGESTION` on both engines and `disagreement` returns
   None. The mode and side keys are covered by the synthetic outcomes of 3.
3. **Synthetic outcomes** (no session): listed/unlisted, wrong side (`row` entry raised by
   Varka), wrong mode, a non-Spark class name, and that both-sides-raise-a-listed-condition is
   no finding while one-side-only remains `difference`'s.
4. **The shrinker keeps an unexpected-condition kind** and reduces the planted case to `i / 0`.
5. **The partition check reports an unlisted raise** (a `run` function returning a synthetic
   `Left`) and stays silent on a listed one.
6. **The list file is well formed**: every entry has a reason, a known side, known modes, families
   that are rows of `coverage.json` (or `fixture`), a condition that exists in Spark's
   `error-conditions.json` (or an exception class that loads), and no duplicates.
7. **Unused entries**: the main arm records what it saw; after the default run an entry never
   raised is reported with `info`. It is a warning, not a failure, because which conditions a
   200-composition run meets moves with the coverage table (a new row reshuffles the draw), and
   a failure there would blame an unrelated pull request.

Both emit configurations: the suites run under the defaults and under one other (`VarkaMatrix`)
in 9.

## 6. The measurement

None: a test-strength change with no performance claim.

### 6.1 Predictions, registered before the run

1. The default arm (200 compositions, seed 20261008) raises at least the fixture's
   `CAST_OVERFLOW` and `DATETIME_FIELD_OUT_OF_BOUNDS...` and no unlisted condition.
2. The 200-composition run does not meet all five entries (three of the five were seen once or
   twice in 1,500), so the unused-entries line is non-empty.
3. A 5,000-composition run at another seed raises no unlisted condition (it would be a finding).

## 7. Risks

1. **The list goes stale or grows to hide bugs.** An entry names the family and the mode, and
   the reason names the Spark behaviour; a reviewer reads each addition. Test 7 reports the
   unused ones.
2. **A rare condition, not in the 3,000-composition census, fails a nightly** (a real finding, or
   a missing entry). It fails with the condition and a reproducer, which is the point.
3. **`rectify` consumes errors unread**: a conjunct that raises on the pivot row is dropped
   without the list, so an unexpected condition on that path is never seen. Left; it judges no
   engine and its raise is the row engine's alone.
4. **Entries are per condition, mode and side, not per expression**: a listed condition raised by
   an unrelated family passes. Narrowing it needs the culprit of a multi-part case, which the
   fuzzer does not compute; the families column is documentation checked against the table.

## 8. Sequencing

1. This plan and row 319's cell.
2. The list, the Java reader, the check wired into the four sites, with tests 3, 5 and 6.
3. The end-to-end tests 1, 2 and 4, the census line, section 9.

## 9. Outcome

Built as 3 describes, with the list at six entries (2, correction). Review and merge are the
coordinator's.

**The census** (the committed fuzzer prints it with `info`, one line per condition, side and
mode). The default arm (seed 20261008, 200 compositions, 183 fused): `CAST_OVERFLOW` 10 ANSI and
9 LEGACY per side (the fixture), `DATETIME_FIELD_OUT_OF_BOUNDS.WITHOUT_SUGGESTION` 1 ANSI per
side. 5,000 compositions at seed 11: `CAST_OVERFLOW` 204 ANSI and 206 LEGACY, `DATETIME...` 33,
`ARITHMETIC_OVERFLOW` 3 ANSI, `INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION` 3 ANSI and 2 LEGACY,
every one on both sides. At seed 13: the same plus `EXPRESSION_DECODING_FAILED` 2 ANSI and 7
LEGACY and `ARITHMETIC_OVERFLOW` 1 LEGACY. No run raised a condition on one side only, a
non-Spark exception or `INTERNAL_ERROR`; the only unlisted conditions ever met were the two the
correction names, each a missing entry.

**The tests of 5** all pass: the planted `i / 0` (`DIVIDE_BY_ZERO` on both engines under ANSI)
fails with "unexpected error condition DIVIDE_BY_ZERO from the row engine under ANSI", the seed,
the iteration and a shrunk reproducer whose select is `i / 0 AS c0`; `make_date(2021, 13, 1)`
passes; the synthetic outcomes, the partition check and the file's well-formedness pass. Run under
the defaults and under `cse=false` through `dev/varka_matrix.sh` (20 tests each, 0 failures).

**Predictions** (6.1). 1: held (the default arm raised `CAST_OVERFLOW` and `DATETIME...` only).
2: held (three of the five entries then listed were not raised in 200 compositions; with six
entries two to three are unused in a default run, reported as a line, not a failure). 3: **missed**,
twice: seed 11 and seed 13 each raised a condition not on the list (the correction in 2).

**The partition check** (row 352's gap) now reads the list: a raise on any of its four queries that
the list does not allow is reported; a listed raise still says nothing about the rows.

**What moved that the plan did not list.** `failureText` was extracted from the main test's loop
so that the planted test goes through the failure path the fuzzer uses. `VarkaExpectedErrors`
is Java, `VarkaSparkFuzz.unexpectedRaise` its one Scala caller.

**Left for later.**
* About 8% of compositions read a fixture whose interval casts overflow, so they compare an error
  with an error and nothing else (`CAST_OVERFLOW`, 71 to 206 per 1,500 to 5,000 compositions). The
  fixture's draw should keep the interval-cast columns' ints inside the cast's range in a
  composition that is meant to compare values; it is the draw's debt, not this row's.
* `rectify` consumes errors unread (7, risk 3).
* The unused-entries line is a warning; a hard check needs a census long enough to be stable under
  a changed coverage table.
* The outcome is keyed by the outermost Spark condition, so a wrapper such as
  `EXPRESSION_DECODING_FAILED` hides a different inner exception on one engine. Comparing the
  innermost exception's class too is row 317's business (errors compared as answers).
* The review (a branch-diff read) found that `noteRaises` does not count the forced and partition
  queries, so the unused-entries warning can name an entry that those raised; cosmetic.
