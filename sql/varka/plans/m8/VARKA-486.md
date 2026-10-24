# VARKA-486: the cast register held to the compiler

## 0. Why this row exists

`VARKA-475.md` 3.2, its cast half, as a row of its own. `casts.tsv` says, for each of the 106
legal pairs of the thirteen types, whether Varka fuses it today, in its `fused_today` column. The
column is copied from `FUSED_CASTS` in `dev/varka_census.py`, which is written by hand, and
nothing asks the compiler: `cast_errors` holds the claims to the coverage rows that cast each
pair (VARKA-328), which proves a claimed pair fuses in some row, but not that an unclaimed pair
declines, nor which modes and forms of a claimed pair fuse. The column is also free text ("MONTH
unit only", "LEGACY and ANSI; TRY's null waits on row 332"), and since row 484 the conformance
contract fails on text it cannot read, so row 485's `INT` and interval suites cannot read their
cast rows until the register says exactly what fuses.

A spike (24 October 2026) asked the compiler about every pair of forms - each interval's start
and end fields, three time precisions - in the three modes, and found the hand claims wrong in
three places:

1. `ym -> int`: claimed "MONTH-ended source only"; `CAST(INTERVAL YEAR AS INT)` fuses too (and
   `m8/PLAN.md` row 332 says it declines). A coverage row, `CAST(ymy AS INT)`, already casts it.
2. `ym -> ym`: claimed "MONTH end field only"; every one of the six casts between the three
   year-month forms fuses, `YEAR TO MONTH` to `YEAR` among them.
3. `int -> dt`: claimed as fused; no bare cast to a day-time form fuses. The pair fuses only as
   `date_add`'s offset (`d + CAST(i AS INTERVAL DAY)`), inside another operator.

The answer is per form, not per pair: `dt -> dt` fuses for 36 of its 90 form pairs (every target
whose end field is `SECOND`), `ym -> int` for two of three sources. No hand-written list will
keep up with that.

## 1. Design

**The compiler writes the register's form-level half.** `VarkaCastMatrixSuite`, which already
holds `casts.tsv`'s legality to Spark's `Cast`, gains a test that compiles every legal cast over
a column - every source form to every target form, in LEGACY, ANSI and TRY (`try_cast`) - and
records each answer in a committed file, `sql/varka/plans/m8/casts_fused.tsv`: one line per form
pair, with `fused`, `declined` or `illegal` per mode. Under `VARKA_CASTS_REGEN=true` the test
rewrites the file; otherwise it fails on any difference, naming the pairs that moved, as
`VarkaCoverageSuite` does for `coverage.json`. The form: a projection `CAST(x AS T)`; a
predicate when `T` is `BOOLEAN`, since a boolean output waits on row 27 (the spike found the
projection and the predicate agree on all 650 form pairs). "Fused" means the compiler returns a
plan, so `byte -> int`, which compiles to a widening load and no cast node, counts.

The forms are the suite's `TYPES`, widened to every form a type has: the three year-month and
ten day-time field ranges, `TIME(0)`, `TIME(3)` and `TIME(6)`, two decimals. Legality is then
checked over the same forms.

**`casts.tsv` derives `fused_today` from that file.** `dev/varka_census.py` reads
`casts_fused.tsv` and writes each pair's `fused_today` as the modes in which some form fuses
(`LEGACY,ANSI,TRY`, or a subset, or empty), and a new last column, `fused_forms`: `all` when
every form legal in each of those modes fuses there, otherwise `n of m` per mode, the detail
being in `casts_fused.tsv`.
`FUSED_CASTS` goes. What stays by hand is `CONTEXT_CASTS`, the pairs that fuse only inside
another operator (`int -> dt` today), each with the coverage row that shows it; the check
requires the probe to say such a pair declines bare, so a pair that starts to fuse bare must
leave the list.

**Why generating the claim is not circular.** The two directions of drift both fail:
- the compiler changes (a pair starts or stops fusing): the suite fails in CI until the file is
  regenerated, and the regeneration shows in the diff;
- someone edits `casts.tsv` by hand: `dev/varka_census.py --check` fails in the pre-commit hook
  and the docs job, the file being stale against its inputs. A hand edit of `casts_fused.tsv`
  itself, with the census regenerated after it, passes the hook and fails the suite in CI, which
  compares the file line for line.

And the human gate stays: `cast_errors` still requires a coverage row for every fused pair and
fails on a row casting an unfused one, so a new fusion forces a coverage row (which runs the cast
end to end against Spark) and a lost one fails the row that still casts it. `coverage.json`
records a row's casts by type pair and mode, not by form, so this check is per pair; a row whose
form declines fails `VarkaCoverageSuite` itself, which requires every row to fuse.

**The conformance contract reads the form.** `VarkaTypeConformance`'s cast check looks up its
exact form pair and mode in `casts_fused.tsv`, instead of reading `fused_today`'s text. A form
the file lacks fails the check, naming it. This is what row 485's suites need.

**Decided in the plan, object if wrong:**

1. *The file records the defaults.* The spike ran under all 52 configurations of the option
   matrix and one moved: `validityByWidth=false` declines `TINYINT` and `SMALLINT` to `INT`,
   since it leaves a narrow column no widening load (the refusal the conformance contract
   already cancels on). The probe compiles under `VarkaEmitOptions.DEFAULTS` and is tagged as
   pinning the defaults, so the matrix cancels it under a configuration rather than run the
   same answers 52 times.
2. *No `VarkaCastTable`.* Generating the compiler's cast arms from the register waits until the
   arms outgrow review (`VARKA-475.md` 6, question 2), as 3.2 says.
3. *The census half is row 487.* Probing whether an expression fuses over a type needs a SQL form
   with the right operand positions, which the census does not hold (183 entries, 881 pairs).
   Row 487 measures how far substituting a column into the coverage rows reaches before
   committing to a design; this row does not start it.

## 2. Changes

| File | Change |
| :--- | :--- |
| `VarkaCastMatrixSuite.java` | every form of each type; the fusion test writing or checking `casts_fused.tsv` |
| `casts_fused.tsv` | new, generated |
| `dev/varka_census.py` | `fused_today` and `fused_forms` from `casts_fused.tsv`; `FUSED_CASTS` replaced by `CONTEXT_CASTS`; its doctest |
| `casts.tsv` | regenerated: the three corrected pairs, the new column |
| `VarkaTypeConformance.java` | the cast check reads `casts_fused.tsv` by form |
| `m8/PLAN.md` | rows 486 and 487; row 332's year-month parenthetical corrected |

## 3. Predictions and costs

The spike probed 650 form pairs in 13 s of test time, compile only. The fusion test adds about
that to `VarkaCastMatrixSuite`; at the calibration table's ratios for short tests (0.07 to 0.37)
the estimate is under 20 s. The hook needs no change: `casts_fused.tsv` is under
`sql/varka/plans/`, which it already treats as a census input. No emitted byte moves; `coverage.json`, `emitted_bytes.json` and
`width_audit.json` stay byte-identical. `casts.tsv` moves by the three corrections and the new
column; `census.tsv` does not move.

## 4. Done when

`VARKA-475.md` 3.2's done-when for the casts: the check runs in CI (the suite) and in the
pre-commit hook (`--check` over the committed file), and the 106 pairs agree with the compiler.
Plus the house's: the option matrix over `VarkaCastMatrixSuite` and the three conformance suites,
the catalyst defaults gate, the linters.

## 5. Outcome

*24 October 2026. Done.* `VarkaCastMatrixSuite`'s `TYPES` hold every form of each type, and its
new test compiles every legal cast between two forms in the three modes: 365 form pairs over 97
type pairs (the 106 less the nine identities of a one-form type), written to `casts_fused.tsv`
under `VARKA_CASTS_REGEN=true` and checked line by line otherwise. `dev/varka_census.py` derives
`fused_today` and the new `fused_forms` column from it; `FUSED_CASTS` is gone, and
`CONTEXT_CASTS` holds one pair, `int -> dt` as `date_add`'s offset. The conformance contract
looks up its exact form pair, which row 485 needs.

**What the register now says.** Ten pairs fuse in some form: `byte`, `short` to `int`, `int` to
`bigint` and `ym` to `ym` in every form and mode; `bigint` to `int` under LEGACY and ANSI (TRY's
null waits on row 332); `int` to `ym` and `ym` to `int` for two forms of three (a target or
source ending in `MONTH`, and `INTERVAL YEAR` to `INT`); `time` to `time` for the three widening
precisions of six; `dt` to `dt` for 36 forms of 90 (every target ending in `SECOND`); and
`int -> dt` only in context. The three wrong claims of section 0 are corrected, and so is row
332's text.

**Planted faults.** With `byte -> int` marked declined under LEGACY and `bool -> int` marked fused
in `casts_fused.tsv`, the suite fails naming both lines, the `TINYINT` contract fails with "fuses,
and the register owes it to rows 332", and the `BOOLEAN` contract with "declines, and the register
marks bool fused".

**Measured.** The fusion test takes 1.7 s in the defaults gate (13 s in the spike's cold JVM)
against under 20 s predicted, a ratio of 0.08 (in the calibration table). The option matrix over
`VarkaCastMatrixSuite` and the three conformance suites is clean in all 52 runs (the fusion test
cancelled under each configuration, as it pins the defaults), and the catalyst defaults gate
passes (873 tests). `coverage.json`, `emitted_bytes.json`, `width_audit.json` and `census.tsv`
are unchanged.

**Review (24 October 2026).** One read-only review before the push found no way for the register
to drift unnoticed. Applied: a `CONTEXT_CASTS` pair must have a coverage row casting it (the hand
list had lost that check in the move); the suite compares the file line for line, so a repeated
or reordered line fails rather than passing a set comparison; `fused_forms` counts only the forms
legal in each mode, and names the modes when their counts differ; section 1 says which check
catches a hand edit of `casts_fused.tsv`. Left as a stated limit: the coverage-row check is per
pair, since `coverage.json` records casts without their forms.
