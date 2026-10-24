# VARKA-490: formal verification caught up with the new types

## 0. Why this row exists

An audit on 24 October 2026 (Max asked whether formal verification keeps up with new code,
`TINYINT` and `SMALLINT` above all) found that it does not, and that nothing would say so.

`sql/varka/proofs/refinement.smt2` proves kernels equal to the specification (`spec/`) per
coverage row and mode. Its rows were chosen by hand in milestone 7 and none has been added
since. Of the 256 coverage rows:

| Type | Rows | Proven end to end (refinement) | What else holds them |
| :--- | ---: | :--- | :--- |
| `INT` | 68 | `+ - * neg`, the WRAP and FAIL kernels, int32 | the differential suites |
| dates, `TIME`, intervals | 144 | 20 rows, with the calendar's lemmas | the differential suites |
| `TINYINT` | 23 | none | `narrow_*.smt2` prove the lowering's pieces; every pair (2^16) swept in LEGACY and TRY, named cases in ANSI |
| `SMALLINT` | 23 | none | the same pieces; 2^24 of the 2^32 pairs swept per operator and mode |
| `BOOLEAN` | 19 | none | the exhaustive three-valued tables (`VARKA-331.md` 3.2), `CondValue` per leaf state (row 460) |
| `BIGINT` | 41 | none (`bitmap.smt2` proves the two bitmap functions' lowering) | the differential suites; no `+ - *` row yet |
| casts | 10 fused pairs | none | coverage rows; `casts.tsv`'s `proof` column names SMT or exhaustion for them, and neither exists |

Four gaps follow:

1. **`SMALLINT` arithmetic is neither proven nor exhausted.** The four `narrow_*.smt2` files
   prove the formulas the lowering is built from - the widened arm's wrap, the native arm's
   checked add through the saturating one, the range compare, the deferred reduction - and
   nothing proves that the kernel for `s1 + s2`, in a mode and on an arm, is Spark's answer.
2. **`TINYINT` under ANSI** is checked at named cases only.
3. **The fused casts that compute** - `bigint -> int`'s range check, `INTERVAL YEAR` to `INT`,
   `INTERVAL YEAR TO MONTH` to `INTERVAL YEAR` - have no proof.
4. **Nothing ties a fused row to its verification**, which is how 1 to 3 happened: a row lands
   with the differential suites alone and nothing fails. And the proofs' README says the nightly
   runs every file under both solvers; no workflow does (`dev/varka_nightly.sh` does, run by
   hand), and CI runs each file under one.

## 1. Design

**A verification ledger on the coverage row.** Each row of `VarkaCoverageSuite` names how it is
verified, written into `coverage.json` beside its family and operands. The vocabulary is closed:

- `refinement: <section>` - a section of `refinement.smt2`, named as the file echoes it;
- `proof: <file>` - a lowering proof under `sql/varka/proofs/`;
- `exhaustive: <suite>` - a suite that sweeps the row's whole input domain against Spark;
- `differential` - the differential suites alone, counted as the gap it is;
- `owed: <row>` - a row of `m8/PLAN.md` that will verify it.

`VarkaCoverageSuite` fails a row with no entry, a section `refinement.smt2` does not echo, a file
absent from `sql/varka/proofs/`, a suite absent from the tests, or an owed row absent from the
plan, as the census check does for the conformance suites (row 484). The suite also renders the
audit table above into `docs/sql-varka.md`'s generated block, per type and per kind, so the
counts stay current and a new type's rows show where they stand.

The unit is the coverage row, not the IR node: every other fact about a fused shape lives on the
row, the milestone's done-when counts rows, and `refinement.smt2` is organised by what rows
compute. A node-to-proof list would be one more hand-kept copy (`VARKA-475.md` 9.1, finding 3).

**The narrow refinement.** A section of `refinement.smt2` for the narrow arithmetic rows - `+`,
`-`, `*`, negation, `abs`, `try_add`, `try_subtract`, `try_multiply` over `TINYINT` and
`SMALLINT` - on both arms (the widened one, the int lane with the narrow wrap and the range
compare; the native one, the narrow lane with the saturating check), in LEGACY, ANSI and TRY,
each against the specification at the type's width: `STANDARD.md` 4.1 for `SMALLINT`, an
`EXTENSIONS.md` entry for `TINYINT` (which the standard does not have), and the deltas
`overflow_wraps` and `overflow_yields_null` at the narrow width. The kernel side composes the
lemmas `narrow_*.smt2` already prove rather than restating the lowering, as the calendar's
refinement composes `chrono_divide.smt2`. Bit-vectors of 8 and 16 bits are small; if the integer
encoding of the README's rule is slow for a multiply, the section says which it uses and why.

**`TINYINT` under ANSI, every pair.** `VarkaEmitterNarrowArithSuite` gains the ANSI sweep of
every pair (2^16 rows, as LEGACY and TRY have), declining exactly the batches Spark raises on.

**The fused casts that compute.** One proof file, `casts.smt2`, rendered by `VarkaProofFiles`:
`bigint -> int` under LEGACY (the low 32 bits) and ANSI (the range check that declines), and the
year-month conversions the probe found fused (`INTERVAL YEAR` to `INT`, a division by twelve of a
multiple of twelve; `YEAR TO MONTH` to `YEAR`, the truncation), each against Spark's `Cast`. The
widening casts (`byte`, `short` to `int`, `int` to `bigint`) and the relabels need no proof and
the ledger says so.

**The nightly runs both solvers.** `varka-fuzz.yml` gains a step running
`dev/varka_prove.sh --solver both`, as its README says it does, and the README names the
workflow.

**Decided in the plan, object if wrong:**

1. *Exhaustion counts as verification* where the domain is finite and swept whole: the house
   already accepts it for every source of 32 bits or fewer (`casts.tsv`'s `proof` column), the
   three-valued tables (`VARKA-331.md` 3.2) and the bit count (row 335). An SMT restatement of a
   per-lane truth table adds a label, not assurance; the `BOOLEAN` rows are `exhaustive`.
2. *`differential` is a legal entry.* The ledger makes the gap countable rather than failing
   every comparison and choice row on the first day; whether those get proofs is the owner's call
   on the counts the table shows.
3. *Row 396's nightly failure is not this row's.* The fuzz workflow has failed every night since
   20 October on the IR fuzzer's compiled-history test in three of four JVMs, which is row 396;
   its row records that it is still failing.

## 2. Predictions and costs

The ledger is about 256 row entries, classified by family with per-row overrides; about half a
day. The narrow refinement is the large part: eight operators, two widths, two arms, three modes,
about 96 refinements of five queries each, a day to two; the solver time is unknown until the
first width runs, and the section's time is recorded under both solvers. The ANSI sweep adds one
2^16-row batch per operator, under a second. `casts.smt2` is small. No emitted byte moves.

## 3. Done when

Every coverage row names its verification and `VarkaCoverageSuite` fails one that does not or
names what does not exist; the audit table is generated; the narrow arithmetic rows are
`refinement` under both solvers; `TINYINT`'s ANSI pairs are swept whole; the three computing
casts are proven; the nightly runs every proof under both solvers. Plus the house's: the option
matrix over the touched suites, the catalyst defaults gate, the linters, `dev/varka_prove.sh
--lint` and `--solver both`.

## 4. Outcome

To be written as the work lands.
