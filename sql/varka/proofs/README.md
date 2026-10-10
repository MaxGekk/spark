# `sql/varka/proofs`

Machine-checked proofs of Varka's arithmetic lowerings, in SMT-LIB. `dev/varka_prove.sh` runs them
under Z3 in the linters' job and under Z3 and cvc5 in the nightly, and fails on any verdict other
than the one each check expects. They began with VARKA-240 (`sql/varka/plans/m7/VARKA-240.md`);
VARKA-241 added the long lane's division forms, and VARKA-242 the calendar's divisions,
`floorMod7` and the leap hash.

| file | what it holds | written by |
| :--- | :--- | :--- |
| `java.smt2` | the prelude: Java's `int` and `long` operators over SMT-LIB's integers, each defined as the JLS defines it, and doubles by their rounding | hand |
| `java_check.smt2` | the prelude against the JVM: each operator over boundary operands, the expected values computed by Java | `VarkaProofFiles` |
| `int_mulhi_divide.smt2` | the int lane's multiply-high division (VARKA-149) exact for every divisor in use, with `signedMagic`'s constants | `VarkaProofFiles` |
| `long_divide.smt2` | the long lane's two division forms exact under `ConstDivide.EXACT_DIVIDEND_BOUND` for every divisor in use, and the conversion form's exact bound past it (VARKA-241) | `VarkaProofFiles` |
| `chrono_divide.smt2` | every calendar division site (`ChronoDivide`): the magic form with its carry exact over the site's dividends and first failing where the table says, and both double forms exact over the range (VARKA-242) | `VarkaProofFiles` |
| `floor_mod7.smt2` | `emitFloorMod7`'s three forms, `Math.floorMod(v, 7)` for every int, by lemmas over each fold's digits (VARKA-242) | `VarkaProofFiles` |
| `leap_hash.smt2` | the leap flag's perfect hash and its unsigned compare exact over every biased year to `LEAP_HASH_MAX_BIASED_YEAR`, over bit-vectors (VARKA-242) | `VarkaProofFiles` |

## Running them

    dev/varka_prove.sh --install z3      # once: Z3 5.1.0 into target/varka-solvers/
    dev/varka_prove.sh                   # every proof under Z3
    dev/varka_prove.sh --install both && dev/varka_prove.sh --solver both   # and under cvc5 1.4.1
    dev/varka_prove.sh --self-test       # the checker of verdicts, checked

By hand, the prelude goes after the file's `set-logic` line:
`sed '/^(set-logic/r sql/varka/proofs/java.smt2' sql/varka/proofs/int_mulhi_divide.smt2 | z3 -in`.
A solver's line numbers count the inserted prelude.

## What a proof looks like

A file opens with a header naming the Java it encodes and its `set-logic` line: `QF_NIA`, or `ALL`
where cvc5 decides the checks only under it or the file reasons over bit-vectors. Every check is
an `(echo "<what it shows>: expect sat|unsat")` followed by its `(check-sat)` between `push` and
`pop`. An `unsat` check is a proof: the negation of the claim has no model. A `sat` check shows the
statement is not vacuous: the domain admits its boundary inputs, and a constant changed by one is
refuted. The script holds every verdict to the expectation before it, and fails on `unknown`, on a
check past its time limit, and on any other output, since Z3 goes on answering after an error.

Constants that live in the code are rendered from it: `VarkaProofFiles` (catalyst tests) writes the
file, and `VarkaProofFilesSuite` fails when the committed file differs;
`VARKA_PROOFS_REGEN=true build/sbt 'catalyst/testOnly *VarkaProofFilesSuite'` rewrites it.

## Why integers, not bit-vectors

The multiply-high division stated over bit-vectors times out under Z3, cvc5 and Bitwuzla, while the
same statement over integers takes each solver a fraction of a second (`VARKA-240.md` 2.1).
Bit-blasting turns it into the equivalence of two multiplier circuits; in integers every product has
a constant factor and the statement is linear. So the prelude states Java's wrapping with `mod`
instead of leaning on bit-vector operators, which would also have the wrong meaning in places: a
bit-vector shift does not mask its count. A proof that multiplies two unknowns is nonlinear, and a
solver may answer `unknown`, which fails the run: such a proof needs its own design.

Doubles are integers too (`VARKA-241.md` 2). In SMT-LIB's floating-point theory a solver bit-blasts
the division, and the long lane's division by 1000 took Bitwuzla eight minutes alone on the machine;
stated over integers it takes milliseconds. A double is `M * 2^E`, and since `2^E` for an unknown `E`
is not linear, the prelude returns no double: `jdouble.rne` relates an exact rational to the double
it rounds to in a binade the proof names. A proof that rounds a value it cannot place has one check
per binade the value can fall in, and a check that those binades cover every value its domain gives,
without which a check could pass by leaving a value out.

The leap hash is the exception (`leap_hash.smt2`): a mask of scattered bits over a wrapped product.
Over integers it ran past ten minutes under Z3; over bit-vectors, whose operators are Java's int
operators exactly for a multiply, a mask and an unsigned compare, both solvers take under a second.

## Adding a proof

1. One file per lowering, its header naming the Java it encodes. Constants that live in the code
   are rendered: a method in `VarkaProofFiles`, and the file in its `render()`.
2. The claim as `unsat` checks, and `sat` checks that its domain admits its boundary inputs and
   that a constant changed by one is refuted.
3. `dev/varka_prove.sh --solver both` holds, in seconds: the linters' job allows a minute for all of
   them.
4. cvc5 is the fragile one (`VARKA-242.md` 2.2 and 9). Where it answers `unknown`, try in order:
   `(set-logic ALL)` in place of `QF_NIA`; one lemma per step over named digits or quotients, so
   each check is linear arithmetic over a few small variables; a residue written as 7 times a
   declared integer rather than through `mod`; and fewer wrapping operations per check. A check
   that holds alone can still come back `unknown` after other checks in the same run, so a new file
   is run whole, and each of its checks alone, under both solvers.

## The solvers

Z3 5.1.0 (MIT) from PyPI's `z3-solver`, whose wheel ships the binary, and cvc5 1.4.1 (BSD-3-Clause)
from its GitHub release archive, its SHA-256 checked. `dev/varka_prove.sh` pins both and refuses
other versions. They are development tools: nothing here is compiled, packaged or shipped.
