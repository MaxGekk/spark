# VARKA-270: how Varka knows its answers are right, and where that stops

*Row 270 of `m7/PLAN.md` (section 2.5; done-when point 5 of 1.1). Planned and drafted on
17 October 2026 in the shape of the earlier posts' plans (`VARKA-371.md`, `VARKA-258.md`): the
reader, the claim and the surprise it opens on, the evidence, the outline with a figure per
section, what waits on open pull requests, the word budget, and done-when. The draft is
`POST_CORRECTNESS.md`; its figures are `figures/fig45.py` to `figures/fig50.py`.*

## 1. The question

The reader is a JVM or database engineer who has never read a Varka plan. They know what a
vectorized engine and a JIT are; they may have built a differential test or a fuzzer. The post
answers one question: **how does a fork that compiles SQL expressions into Vector API bytecode at
run time know it returns the same answers as Spark, and what can it not know?**

"Right" has one meaning, the house rule: the same answer as vanilla Spark, errors included. The
SQL standard enters only as a register of where Spark departs from it; the post must not read as
"Varka is more correct than Spark".

## 2. The claim, and the surprise it opens on

**The surprise.** On 10 October 2026 every Varka suite passed, and a coverage run over the classes
the emitter wrote showed that of the 2,803 kernel classes the suites ran, 733 had both their loop
bodies run: the rest had one of their two bodies (the null-free one or the masked one) compiled,
loaded and pinned by hash, and never compared with anything. Source: the three committed versions
of `sql/varka/coverage/generated.md` (commits 6fa0c863a5f, 69302411f8f, 05169a63d19): 733, then
2,029, then 2,427 of about 2,800 entering both, with 213 more that have one body by construction.
It opens straight onto the post's subject: tests pass over what they do not run, so the checks
have to be checked.

Other candidates considered and not used to open: Granlund and Montgomery's Theorem 5.1 failing
for 327,741,950 divisors under the textbook's shifts (`VARKA-240.md` 2.3) is striking but moves
no divisor in use, so it needs a qualifier at once, and it goes in the proofs section; the column
contract that a stored date past year 42400 breaks (`VARKA-245.md` 9.2) is a solver's `sat`, not
run end to end, and goes in "Where the checking stops".

**The claim.** The arithmetic is proven, the answers are compared with Spark three ways, the tests
themselves are measured, and the checking stops at named places: the bytecode's plumbing, the JVM,
the column contract, what the random generators cannot reach, and tests whose verdict depends on
what the JIT did.

## 3. Words the post defines

Each house term is defined in one clause where it first appears, and avoided where a plain word
does. The post uses them only in these senses:

| term | the post's definition |
|---|---|
| kernel | a class Varka writes as bytecode at run time to compute one query's expressions over a batch of columns with the Vector API |
| lane | not used; "32-bit" and "64-bit kernels" instead of the int and long lanes |
| dense and masked body | a kernel's two loops: one for batches with no nulls, one that carries a null mask |
| decline | a kernel refusing a batch, which Spark's own row engine then computes |
| residual filter | the conditions of a `WHERE` that Varka does not compile and Spark evaluates |
| oracle | what an answer is compared with |
| differential | running the same query two ways and comparing the answers |
| fuzzer | a test that generates random expressions and data |
| tier witness | the JVM's own events, read during a test, that say which compiler's code ran a compared call |
| emitted bytes | a committed hash of every class the emitter writes for a fixed set of shapes |
| coverage row | one expression the compiler fuses, listed in the generated coverage table |
| reference evaluator | a plain Java interpreter of Varka's expression tree, written separately from the emitter |

## 4. The evidence, by section

Every number in the post traces to a committed file, listed in the post's provenance section.
No laptop timing is quoted: the solvers' and suites' times in the record are laptop or mixed
measurements, so the post says only what a committed file states (the proofs run in the linters'
CI job).

## 5. The outline

Body: no task ids, row or pull request numbers, or milestone words; they go in the provenance
section. Each section answers one question, stated in its heading. One figure per section, each
drawn from a committed file by a script under `figures/`.

| section | the question | figure, and its data |
|---|---|---|
| opening and "In short" | - | none; the opening's number is Figure 3's |
| What does "right" mean? | what is the answer compared with? | fig45, who checks whom: a diagram of the oracles (Spark's session, Catalyst's `eval`, the reference evaluator, the specification with Spark's departures) and what each holds to what; its counts read from `coverage.json` and `spec/DELTAS.md` |
| Is the arithmetic exact? | how do you know a division by a magic number is exact? | fig46, the proof checks per file: the `(check-sat)` count of each committed `sql/varka/proofs/*.smt2` |
| Does the bytecode compute the expression? | is the emitter's output the tree's meaning? | fig47, both bodies run: the three versions of `coverage/generated.md`, kept in `figures/data/both-bodies.tsv` with their commits, the last row held to the current file |
| Does it match Spark? | does a query return Spark's rows? | fig48, what Spark's own SQL tests ask of Varka: `sql/varka/spark_suites/served.json`, executions and served executions of the files Varka serves most |
| Do the tests run the code users run? | is the compiled vector code checked, or only the interpreter? | fig49, seven JVM configurations: the arm table of `VARKA-272.md` 9.1, answers per arm and the differing count |
| Where does the checking stop? | what is not proven, not reached, or not decidable? | fig50, what random draws reach: `coverage/reach/VarkaIrFuzzSuite.md`, mechanisms reached per report and by none |
| How these were measured | provenance | - |

## 6. What waits on open pull requests

Three pieces are in open pull requests; the draft carries a slot for each, marked
`[PENDING #<n>: ...]` so that `grep -n 'PENDING #' POST_CORRECTNESS.md` finds them, and quotes no
number from their branches:

* **#890 (row 265)**: branch coverage of the emitter and compiler, and the bounded mutation run.
  Slot in "Where does the checking stop?".
* **#871 (row 298)**: nested Catalyst trees of neighbouring types in the Spark-level fuzzer, and
  what they found. Slot in "Where does the checking stop?".
* **#870 (row 274b)**: kernels warmed to C2 and checked on the coverage rows, and the nightly
  step over many draws. Slot in "Do the tests run the code users run?".

Each slot is filled from that row's plan once it merges, the markers removed and the pull request
numbers moved to the provenance section.

## 7. Word budget and predictions

1. The body (title to the provenance heading) is 1,500 to 1,900 words, the low end of the earlier
   posts' 1,500 to 2,500, before the three slots are filled; filling them adds at most 200.
2. Six figures, one per section, every one generated from a committed file.
3. `dev/varka_quote_check.py` finds no orphan in the post or this plan.

## 8. Done when

The draft reads as a post rather than a plan row (short sentences, one idea a paragraph, every
house term defined); every number traces; the three slots are filled after their pull requests
merge; the owner's edit is in; and the post is published from `dev/varka_post_page.py`'s page.
Row 261 rewrites the README's account of how Varka is checked from the same record, and the two
should not contradict each other.

## 9. Explicitly out of this task

* New measurements: the post reports the record and runs nothing.
* The README's account of checking (row 261).
* A separate post on the proofs alone, for which the record has enough; the owner's call.

## 10. Outcome

### 10.1 The draft, 17 October 2026

`POST_CORRECTNESS.md` is the draft, on 5's outline, written to the coordinator's readability
brief of the same day: every house term defined where it first appears, no task, row or pull
request number in the body, one question a section. Rendered with `dev/varka_post_page.py`.

Scored against 7:

1. **Held, at the top of the range.** The body from the title to the provenance heading is about
   1,980 words with the figure lines and the three slots counted, about 1,880 without them; a
   first pass was 2,050 and was cut.
2. **Held.** Six figures, `fig45.py` to `fig50.py`, each reading a committed file: `coverage.json`
   and `spec/DELTAS.md`, the `.smt2` files, `figures/data/both-bodies.tsv` (held to the current
   `coverage/generated.md`), `spark_suites/served.json`, `VARKA-272.md` 9.1, and
   `coverage/reach/VarkaIrFuzzSuite.md`. Figure 45 is a diagram; its two counts are read.
3. **Held.** `dev/varka_quote_check.py` finds no orphan in the post or this plan.

What the draft changed from the outline: the bugs the checks found are told in the sections
where they were found rather than in a section of their own, and the filter-order bug sits in
"Where does the checking stop?", since it was found by reading after every test had passed over
it. Owed before publication: the three slots, from rows 265, 298 and 274b once they merge, and the
owner's edit.
