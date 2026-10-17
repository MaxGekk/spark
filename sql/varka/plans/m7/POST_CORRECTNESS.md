# How Varka knows its answers are Spark's, and where it stops

Varka is a fork of Apache Spark that compiles SQL expressions into kernels: classes it writes as
bytecode at run time, each computing a query's expressions over a batch of columns with the Java
Vector API. Every kernel has two loops. One runs batches with no nulls; the other carries a null
mask. In October we measured which of that code Varka's tests actually ran. Of about 2,800
kernel classes the suites ran, about 730 had run both loops. In most of the rest, one loop had
been compiled and loaded, and never compared with anything.

This post is for people who build engines and test them: how Varka checks that its answers
are right, and where the checking stops.

> **In short**
>
> 1. "Right" means the same answer as vanilla Spark, errors included. What Varka cannot match,
>    it hands back to Spark.
> 2. The arithmetic is proven: about 2,000 solver queries, with constants read from the code.
> 3. Answers are compared with Spark on random queries, on Spark's own SQL tests, and on
>    every small expression tree, nearly four million of them.
> 4. The tests are measured too: which loops ran, which JIT compiler's code ran, and which
>    memory a kernel touched.
> 5. The checking stops where proofs and random tests stop. The bytecode around the
>    arithmetic is tested but not proven, and the JVM itself is trusted.
> 6. Random tests reach only what they draw, and some verdicts depend on what the JIT did.

## What does "right" mean?

Right means what Spark says. Varka's kernels must return the rows vanilla Spark returns, with the
same nulls. When Spark raises an error, Varka must raise the same one, compared by error class,
SQLSTATE and message parameters.

When a kernel cannot promise that answer for a batch, it declines: Spark's own row engine
computes the batch instead. A kernel that throws is caught the same way. That safety net can hide
a broken kernel behind a right answer, so under test a fallback no test declared is a failure.

The SQL standard is not the target. We wrote down the SQL-92 definition of each operation the
standard fixes, and named the 11 places where Spark departs from it, such as clamping
`add_months` to the end of the month. Each departure has a test showing Spark doing it. The
proofs below use these definitions with Spark's departures applied.

![Who checks whom](figures/svg/fig45-who-checks-whom.svg)

*Figure 1. Every check ends at Spark's answer. Kernels are compared with the reference
evaluator, the reference evaluator with Spark's own expression engine, and whole queries with
Spark directly.*

## Is the arithmetic exact?

Kernels replace slow operations with fast ones that are exact only over a range. A division by
7 becomes a multiply by a "magic" constant and a shift. Tests sample such ranges; a solver covers
all of them.

Each such rewrite has a proof file in SMT-LIB, checked by two solvers, Z3 and cvc5, which must
agree. Java's operators are defined once, in a shared prelude, and that prelude is itself checked
against the JVM on boundary values. The constants are rendered from the Java code, and a test
fails when the committed file differs from what the code would write. To show a proof is not
vacuous, each file also re-checks one claim with a constant off by one, and that check must fail.

The proofs found things. The usual recipe for magic numbers, from Hacker's Delight, does not
always meet the theorem it is justified by: for 327,741,950 of the 32-bit divisors, its shift is
too small for Granlund and Montgomery's condition. The recipe's numbers are still exact, for a
different reason, and no divisor Varka uses today was affected. To guard divisors added later,
Varka now asserts the theorem and raises the shift where it fails. The proofs also label every
bound as sufficient or exact. The year-of-century division is used on dividends up to 36,524 and
is first wrong at 44,894, and a test pins that input.

![What the solvers prove](figures/svg/fig46-what-the-solvers-prove.svg)

*Figure 2. Most of the proof effort sits in division: the 64-bit forms and the calendar. The
range analysis, which decides when a check can be skipped, is proven too.*

## Does the bytecode compute the expression?

Varka compiles an expression into a small tree of vector operations, then writes bytecode for
the tree. The reference evaluator, plain Java code that computes the same tree one value at a
time and is written separately from the bytecode writer, says what each tree means. The question
is whether the bytecode agrees.

Varka also enumerates: every well-typed tree of up to five nodes, for kernels over 32-bit and over
64-bit values, nearly four million trees in all, goes through the bytecode writer and is compared
with the reference evaluator over data with nulls. Nothing failed. Each passing tree of up to four
nodes is also rewritten in ways that must not change its answer, such as permuting its outputs or
adding an output nobody reads, and checked again.

The opening number came from JaCoCo, the JVM coverage tool, which instruments a generated class
like any other. Most
kernels were compared on one loop only, because a test usually runs a shape once, on one pattern
of nulls. Now every compared batch also runs through the other loop, and the two must agree.

Kernels read and write raw memory, so the tests run with a memory checker on. Every memory
segment a kernel maps is checked against the buffers it was handed, and a canary sits past each
buffer. It found one thing: some outputs were mapped at twice their real size. No answer was
wrong, but a bad write could have gone unnoticed.

![Did the tests run both loops?](figures/svg/fig47-did-the-tests-run-both-loops.svg)

*Figure 3. When first measured, about a quarter of the kernels that ran had both loops compared.
Today nearly nine in ten do, and most of the rest have only one loop by design.*

## Does a query return Spark's rows?

The reference evaluator is Varka's own, so it is held to Spark's. For every expression Varka
compiles, it must agree with Catalyst, Spark's own expression engine, evaluating the same
expression. Then whole queries are compared with Spark. A fuzzer, a test that generates random
expressions and data, builds projections and filters over data with nulls. It runs each through
Spark with Varka on and off, with Spark's ANSI mode on and off, and compares the rows. A failure
is shrunk to its smallest failing expression and saved, and saved failures replay on every pull
request. In a census of 35,000 random queries, the flat half never disagreed; the nested half is
below.

Two other checks borrow Spark's own tests. Spark's golden SQL files, its SQL test files with
recorded expected output, run with Varka on and give the same results as with it off. A harvest of
Spark's date tests runs each of 731 statements as a projection and as a filter on both engines. None
disagrees, and the 122 that raise an error raise the same one.

The fuzzers have found real bugs. One was a filter fallback that failed on a column of
`NULL` type where Spark answered. Not every bug came from a test: a filter that raised an error on
rows Spark never evaluates was found by reading, and it is in the last section.

![What Spark's own SQL tests ask of Varka](figures/svg/fig48-what-sparks-own-tests-ask.svg)

*Figure 4. Spark's SQL tests pass with Varka on, but Varka computes under 4% of the queries they
run. They mostly test Spark.*

## Do the tests run the code users run?

A JVM runs new code in its interpreter first, and compiles it with C1 and then C2 once it is
hot. The fuzzers run each kernel on a fresh class, once per batch, with Varka's warm-up off. So
their verdicts come from the interpreter, which runs the Vector API's plain Java fallback, not
the vector instructions C2 emits. Users run the C2 code.

Two checks close that gap. The first runs the fuzzers' kernels in the seven JVM configurations
of Figure 5, each proving from the JVM's own events which compiler's code served the compared
calls.

The second warms each kernel to C2 before comparing it, over eight histories such as a trap after a
warm-up that never saw nulls, or a compile in the middle of a batch. A tier witness reads the
compilation events of JFR (Java Flight Recorder) and the code cache's method list during the test,
so each compared call is known to have run C2's code. In the first run 85% of warm-ups reached C2
within their deadline, and no history found a wrong answer.

Every pull request now warms the kernels of four coverage rows per lane type through three of
those histories, and every night a thousand random kernels per lane type go through all eight,
then every coverage row. A first two-JVM trial found no wrong answer. In 3 of its 66 cases the
kernel never settled in C2, each caught in the same cycle of deoptimizing and recompiling.

![Seven JVMs, one set of answers](figures/svg/fig49-seven-jvms-one-set-of-answers.svg)

*Figure 5. The same kernels gave the same answers in all seven JVM configurations. The largest
runs compared about 3,400 answers each.*

## Where does the checking stop?

**The plumbing is tested, not proven.** The proofs cover arithmetic. The bytecode around it is
not proven: null masks, validity bits, how outputs are split into methods, the tail of a batch.
That is where Varka's real bugs have been, and a proof there would rest on a model of the Vector
API and of memory, which is where the risk lives. Enumeration and the fuzzers are the method.

**The JVM is trusted.** No proof reaches HotSpot. JDK 25 has a known C2 bug that can drop a scalar
store followed by a masked vector store (JDK-8388492). Varka's stores should not take that shape,
and comparing the interpreter with C2 should show a wrong value. That is a test on the JDKs and CPUs
we run, not a proof.

**One assumption about stored data is unchecked.** A date column is assumed to hold years 1 to
9999, so `year(d)` runs with no guard. Spark can store any 32-bit day. The solver finds exactly
that gap: past a day in the year 42400, the date decoding leaves the range it is proven for. The
code's scalar twin already gets the day of the month wrong there, and further out it returns a
month of 31,848. We read from the code that a cached column can reach that kernel. We have not
run it end to end.

[PENDING #874: date columns checked per batch to lie within years 1 to 9999. Once it merges, this
paragraph becomes a closed gap: say what the check does and what it costs, or move the paragraph
out of this section.]

**Random tests reach only what they draw.** The bytecode writer names 39 mechanisms it uses to shape
code. Random trees reach 29 of them. The paths that shrink a class that is too big are reached
mostly by a few large expressions drawn for the purpose, and three mechanisms no fuzzer reaches at
all. The filter bug shows the limit plainly. Spark stops evaluating a filter's conditions at the
first false one. Varka compiles some of the conditions and leaves the rest to Spark. When a kernel
declined a batch, Varka refiltered it with the compiled conditions alone, so they ran on rows an
earlier condition would have excluded. Under ANSI that raised an error Spark never raised. Every
test passed over it; reading a paper on testing showed where to look, and it is fixed. Its mirror
image turned up later, by a different route.

Flat random expressions miss what Spark's analyzer does to nested ones. A second grammar draws
expressions nested across neighbouring types, so that the analyzer inserts casts, and the
shrinker now reduces inside them. Half of the 35,000 queries in the census were nested, and 4 of
those 17,490 disagreed, all from one bug, the mirror of the one above: Varka compiled a condition
that Spark evaluates after a condition it left to Spark, so where that one raised, Spark failed
the query and Varka returned rows.

[PENDING #908, #909: the mirror bug fixed; the 150,000-query census with nested trees and a
deterministic error verdict found two more, rows 386 and 387, and one disagreement inside Spark
itself, between its compiled and interpreted filters (SPARK-60315). Say how each ended.]

Two measurements say how much of that the tests actually check. Under every Varka suite the
bytecode writer takes 90% of its branches and the compiler 84%, and one of the writer's named
mechanisms, the constant pool's decline, is reached by none. A mutation run then planted 1,760
small faults, a flipped condition or a dropped call, in the two classes that plan every kernel
before a byte is written. Tests that check an answer caught 85% of them. Of the other 256, 37 were
caught by tests written after reading them, and the rest change no answer: a check that cannot
fire, state nothing reads, or only the shape of the emitted code. No fault exposed a wrong answer;
the five gaps were tests that did not exist.

**Some verdicts depend on the JIT.** Varka hands a kernel real batches only after C2 has compiled it
and it runs without allocating. Whether that happens before the deadline depends on what the JIT
did, in that JVM, on that machine. On three cores, 2 of 1,602 warm-ups still missed it. Such tests
report what happened, against a ceiling the nightly holds, rather than assert an outcome.

![What random draws reach](figures/svg/fig50-what-random-draws-reach.svg)

*Figure 6. Random trees reach most of how the bytecode writer shapes code, but few of the paths that
shrink a class that is too big. Three mechanisms no fuzzer reaches.*

## How these were measured

Every number above comes from a committed file in [Varka](https://github.com/vecbricks/varka),
and each figure is drawn from one by a script beside it under `sql/varka/plans/figures/`. The
plans named here are under `sql/varka/plans/m7/`.

* The opening and Figure 3: `sql/varka/coverage/generated.md`, the JaCoCo report over every
  emitted class, at commits 6fa0c863a5f (first measured), 69302411f8f and 05169a63d19 (today),
  kept in `figures/data/both-bodies.tsv`; `VARKA-285.md`, `VARKA-284.md`, `VARKA-303.md`.
* What "right" means and Figure 1: `sql/varka/proofs/spec/STANDARD.md`, `spec/DELTAS.md`,
  `sql/varka/coverage.json`; `VARKA-243.md`, `VARKA-244.md`, `VARKA-275.md`, `VARKA-317.md`.
* The proofs and Figure 2: the files under `sql/varka/proofs/` and its README; `VARKA-240.md` 2.3
  (Theorem 5.1), `VARKA-242.md` (bounds), `VARKA-245.md` 9.2 (the column contract),
  `VARKA-281.md` (the range analysis).
* Enumeration, variants and the memory checker: `VARKA-269.md` 9, `VARKA-278.md`, `VARKA-263.md`,
  `VARKA-292.md`.
* Queries against Spark and Figure 4: `VARKA-276.md`, `VARKA-262.md`, `VARKA-372.md` (35,000
  queries), `VARKA-299.md` (the `NULL`-type column), `VARKA-81.md` and
  `sql/varka/golden_corpus.json` (731 statements), `VARKA-271.md` and
  `sql/varka/spark_suites/served.json` (Figure 4).
* The JIT tiers and Figure 5: `VARKA-272.md` 9.1, `VARKA-274.md` 9.
* Where it stops and Figure 6: `m9/SCOPE.md` item 58 (what a proof can reach), item 56
  (JDK-8388492), `m7/PLAN.md` section 2.2 and its row 273 (the filter order), `VARKA-280.md` and
  `sql/varka/coverage/reach/VarkaIrFuzzSuite.md`, `VARKA-371.md` 9.2 (2 of 1,602 on three cores),
  `VARKA-378.md` (the nightly ceiling).
