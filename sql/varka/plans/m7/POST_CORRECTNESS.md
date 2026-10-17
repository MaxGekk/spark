# How a SQL engine is tested: ten methods, what each found, and what each missed

Varka is a fork of Apache Spark that compiles SQL expressions into kernels: classes it writes as
bytecode at run time, each computing a query's expressions over a batch of columns with the Java
Vector API. Every kernel has two loops. One runs batches with no nulls; the other carries a null
mask. In October we measured which of that code the test suites had actually compared with
anything. Of about 2,800 kernel classes the suites ran, about 730 had run both loops under a
comparison. The rest had been compiled, loaded and run, and never checked.

This post is for testers and quality engineers. It lays out the ten methods Varka uses to check
that its answers are right, each in the same shape: what it checks and against what, what it has
found, what it costs to run, and where it stops. A table up front links to each.

> **In short**
>
> 1. The oracle is vanilla Spark: the same rows, the same nulls, the same error. What a kernel
>    cannot promise, it hands back to Spark, and under test a hand-back no test declared is a
>    failure.
> 2. Ten methods, from solver proofs to mutation testing. Three of them have found nothing, and
>    the post says what that is worth.
> 3. Of the nine defects the milestone found, random queries found four, the solver two, the
>    memory checker one, the JIT's own log one. The one every test passed over was found by
>    reading.
> 4. Two kinds of verdict needed work of their own: a race inside the fuzzer's harness, and
>    verdicts that depend on what the JIT did in that JVM.

## The oracle

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

## The methods at a glance

| Method | Compared with | Inputs | Found |
|---|---|---|---|
| [1. Proofs of the arithmetic](#m1) | the operation's definition | every value in range | a latent shift; the date bound |
| [2. Enumeration of small kernels](#m2) | the reference evaluator | every tree to 5 nodes | nothing |
| [3. Variants](#m3) | the kernel itself | rewrites that keep the answer | nothing |
| [4. Expressions against Spark](#m4) | Spark's expression engine | every covered expression | nothing |
| [5. Random queries against Spark](#m5) | Spark with Varka off | random projections and filters | four defects |
| [6. Spark's own tests](#m6) | Spark's recorded output | golden files; 731 date statements | nothing |
| [7. Coverage of generated code](#m7) | a measure, no oracle | the suites | two gaps |
| [8. Mutation testing](#m8) | the suites | 1,760 planted faults | five missing tests |
| [9. The JIT tiers](#m9) | the same answer in every tier | 7 JVM configurations, 8 warm histories | one defect, one gap |
| [10. Memory checking](#m10) | the buffers a kernel was given | every kernel run | one defect |

<a id="m1"></a>
## 1. Proofs of the arithmetic

Kernels replace slow operations with fast ones that are exact only over a range. A division by 7
becomes a multiply by a "magic" constant and a shift. Each such rewrite has a proof file in
SMT-LIB, checked by two solvers, Z3 and cvc5, which must agree. Java's operators are defined once,
in a shared prelude, and that prelude is itself checked against the JVM on boundary values. The
constants are rendered from the Java code, and a test fails when the committed file differs from
what the code would write. To show a proof is not vacuous, each file also re-checks one claim with
a constant off by one, and that check must fail.

The proofs found two things. The usual recipe for magic numbers, from Hacker's Delight, does not
always meet the theorem it is justified by: for 327,741,950 of the 32-bit divisors, its shift is
too small for Granlund and Montgomery's condition. The numbers are still exact, for a different
reason, and no divisor Varka uses was affected; Varka now asserts the theorem and raises the shift
where it fails. The second find was an assumption about data. A date column was assumed to hold
years 1 to 9999, so `year(d)` ran with no guard. Spark can store any 32-bit day, and past a day in
the year 42400 the date decoding leaves the range it is proven for. Run over a cached table, the
kernel returned a day of the month of 2 where Spark returns 1. A proposed change makes the
assumption a check inside the kernel's loop and declines the batch past it. The proofs run in
about 14 minutes of every nightly; the slowest file takes Z3 28 seconds.

Where it stops: the proofs cover arithmetic. The bytecode around it, null masks, validity bits,
how outputs are split into methods, the tail of a batch, is not proven, and that is where Varka's
real bugs have been. A proof there would rest on a model of the Vector API and of memory, which is
where the risk lives.

![What the solvers prove](figures/svg/fig46-what-the-solvers-prove.svg)

*Figure 2. Most of the proof effort sits in division: the 64-bit forms and the calendar. The
range analysis, which decides when a check can be skipped, is proven too.*

<a id="m2"></a>
## 2. Enumeration of small kernels

Varka compiles an expression into a small tree of vector operations, then writes bytecode for
the tree. The reference evaluator, plain Java code that computes the same tree one value at a
time and is written separately from the bytecode writer, says what each tree means. Every
well-typed tree of up to five nodes, for kernels over 32-bit and over 64-bit values, nearly four
million trees in all, goes through the bytecode writer and is compared with the reference
evaluator over data with nulls.

It has found nothing. Every pull request runs the trees to four nodes, about 100,000, in about a
minute at both vector widths; the night runs the five-node trees, about 11 minutes for the 64-bit
ones. Where it stops: five nodes, one pattern of data, and only what the reference evaluator
defines. A tree the reference evaluator gets wrong passes with the bytecode that agrees with it,
which is why method 4 exists.

<a id="m3"></a>
## 3. Variants

Each passing tree of up to four nodes is also rewritten in ways that must not change its answer,
such as permuting its outputs or adding an output nobody reads, and checked again. This is
metamorphic testing: the oracle is the kernel's own answer before the rewrite.

It has found nothing. It costs about two minutes at each width in the pull request gate. Where it
stops: it checks only the relations we thought to write, and a bug that survives the rewrite
survives the test.

<a id="m4"></a>
## 4. Expressions against Spark

The reference evaluator is Varka's own, so it is held to Spark's. For every expression Varka
compiles, it must agree with Catalyst, Spark's own expression engine, evaluating the same
expression over the same values. A reference that disagrees with Spark is fixed, or becomes one
of the 11 named departures.

It found one disagreement, and that was the harness's: a value drawn outside its type. It runs in
under half a minute. Where it stops: one expression at a time, no plan, no partitions, no filter
order.

<a id="m5"></a>
## 5. Random queries against Spark

A fuzzer, a test that generates random expressions and data, builds projections and filters over
data with nulls. It runs each through Spark with Varka on and off, with Spark's ANSI mode on and
off, and compares the rows and the errors. A failure is shrunk to its smallest failing expression
and saved, and saved failures replay on every pull request. A second grammar draws expressions
nested across neighbouring types, so that Spark's analyzer inserts casts, and the shrinker reduces
inside them.

This is the method that finds bugs. A filter fallback that failed on a column of `NULL` type where
Spark answered. The mirror of the filter-order bug of the last section: Varka compiled a condition
that Spark evaluates after a condition it left to Spark, so where that one raised, Spark failed the
query and Varka returned rows; 4 of 17,490 nested queries in a census of 35,000. Then, once the
verdict was made deterministic (below), two more in 150,000 nested queries. In one, Varka's
columnar filter finished its batch before the projection above saw a row, so where Spark raised
the projection's error on the first row, Varka raised the filter's on the second; a proposed change
delivers the rows before the failing one first. In the other, Varka raises where Spark's default
session answers, and Spark disagrees with itself: its compiled filter stops at a condition that is
NULL, its interpreted filter goes on to the one that raises (SPARK-60315). A change proposed
upstream would make the compiled filter raise too, so that case waits for Spark's decision.

The saved reproducers replay in seconds on every pull request; the night draws 50,000 queries; a
census of 150,000 takes six JVMs on pinned cores. Where it stops: random draws reach only what
they draw. The bytecode writer names 39 mechanisms it uses to shape code. Random trees reach 29.
The paths that shrink a class that is too big are reached mostly by a few large expressions drawn
for the purpose, and three mechanisms no fuzzer reaches at all.

![What random draws reach](figures/svg/fig50-what-random-draws-reach.svg)

*Figure 3. Random trees reach most of how the bytecode writer shapes code, but few of the paths
that shrink a class that is too big. Three mechanisms no fuzzer reaches.*

<a id="m6"></a>
## 6. Spark's own tests

Two checks borrow Spark's tests. Spark's golden SQL files, its SQL test files with recorded
expected output, run with Varka on and give the same results as with it off. A harvest of Spark's
date tests runs each of 731 statements as a projection and as a filter on both engines. None
disagrees, and the 122 that raise an error raise the same one.

The harvest runs in 74 seconds; the golden files take about an hour a night in a JVM of their
own. Where it stops: they test Spark. Varka computes under 4% of the queries the golden files run,
and the rest never reach a kernel.

![What Spark's own SQL tests ask of Varka](figures/svg/fig48-what-sparks-own-tests-ask.svg)

*Figure 4. Spark's SQL tests pass with Varka on, but Varka computes under 4% of the queries they
run.*

<a id="m7"></a>
## 7. Coverage of generated code

The opening number came from JaCoCo, the JVM coverage tool, which instruments a generated class
like any other. Most kernels were compared on one loop only, because a test usually runs a shape
once, on one pattern of nulls. Now every compared batch also runs through the other loop, and the
two must agree; today nearly nine in ten kernels have both loops compared, and most of the rest
have one loop by design. Under every Varka suite the bytecode writer takes 90% of its branches
and the compiler 84%, and one of the writer's named mechanisms, the constant pool's decline, is
reached by no test.

The measurement costs nothing worth naming: the suites take the same time with the agent on.
Where it stops: coverage is a measure, not an oracle. A line that ran under a test that compared
nothing counts as covered, which is exactly what the opening number showed.

![Did the tests run both loops?](figures/svg/fig47-did-the-tests-run-both-loops.svg)

*Figure 5. When first measured, about a quarter of the kernels that ran had both loops compared.
Today nearly nine in ten do, and most of the rest have only one loop by design.*

<a id="m8"></a>
## 8. Mutation testing

A mutation run planted 1,760 small faults, a flipped condition or a dropped call, in the two
classes that plan every kernel before a byte is written. Tests that check an answer caught 85% of
them. Of the other 256, 37 were caught by tests written after reading them, and the rest change no
answer: a check that cannot fire, state nothing reads, or only the shape of the emitted code. No
fault exposed a wrong answer; the five gaps were tests that did not exist.

The run is made by hand on the laptop, not nightly. Where it stops: two classes, and faults of the
kinds the tool knows how to plant.

<a id="m9"></a>
## 9. The JIT tiers

A JVM runs new code in its interpreter first, and compiles it with C1 and then C2 once it is hot.
The fuzzers run each kernel on a fresh class, once per batch, with Varka's warm-up off. So their
verdicts come from the interpreter, which runs the Vector API's plain Java fallback, not the
vector instructions C2 emits. Users run the C2 code.

Two checks close that gap. The first runs the fuzzers' kernels in the seven JVM configurations of
Figure 6, each proving from the JVM's own events which compiler's code served the compared calls.
The second warms each kernel to C2 before comparing it, over eight histories such as a trap after
a warm-up that never saw nulls, or a compile in the middle of a batch. A tier witness reads the
compilation events of JFR (Java Flight Recorder) and the code cache's method list during the test,
so each compared call is known to have run C2's code.

No tier has given a different answer. Reading the JIT's own log did find a defect: a warmed kernel
that still allocated one vector array per loop iteration in a few per cent of warm-ups, because
C2 adds up the trap counts of every inlined copy of a shared template; that story is in
[its own post](https://vecbricks.github.io/why-a-warmed-kernel-can-still-allocate/). And in 3 of
66 cases of the first two-JVM trial the kernel never settled in C2, each caught in the same cycle
of deoptimizing and recompiling. Every pull request warms the kernels of four coverage rows per
lane type through three histories; every night a thousand random kernels per lane type go through
all eight, about 2.5 seconds a kernel, then every coverage row. Where it stops: one JDK, and the
CPUs we run.

![Seven JVMs, one set of answers](figures/svg/fig49-seven-jvms-one-set-of-answers.svg)

*Figure 6. The same kernels gave the same answers in all seven JVM configurations. The largest
runs compared about 3,400 answers each.*

<a id="m10"></a>
## 10. Memory checking

Kernels read and write raw memory, so the suites run with a memory checker on. Every memory
segment a kernel maps is checked against the buffers it was handed, and a canary sits past each
buffer. It found one thing: some outputs were mapped at twice their real size. No answer was
wrong, but a bad write could have gone unnoticed.

It is on in every suite in CI. Where it stops: it checks addresses, not values, and only on the
paths the suites take.

## Verdicts that could not be trusted

Two kinds of verdict needed work before the methods above could be believed.

**The fuzzer's own race.** Its queries ran over two partitions, so when rows in different
partitions raised different errors, either could arrive first, on either engine, and the fuzzer
absorbed the disagreement by rerunning three times. A comparison whose answer depends on which
task fails first can neither pass nor fail honestly. Now a query that raised runs again over one
partition, the rerun is gone, and a census of 150,000 flat queries found no disagreement where
the race had been absorbing them.

**Verdicts that depend on the JIT.** Varka hands a kernel real batches only after C2 has compiled
it and it runs without allocating. Whether that happens before the deadline depends on what the
JIT did, in that JVM, on that machine. On three cores, 2 of 1,602 warm-ups still missed it. Such
tests report what happened, against a ceiling the nightly holds and lowers, rather than assert an
outcome that is not theirs to assert.

## Defects by the method that found them

Nine defects in the milestone, and the method that found each is not the one a plan would have
predicted. The proofs found two. Random queries found four, three of them only after the fuzzer's
own race was removed. The memory checker found one, the JIT's log one. And the filter-order bug,
a fused condition run on rows an earlier condition would have excluded, so that under ANSI Varka
raised an error Spark never raised, was found by none of the ten: every test passed over it, and
reading a paper on testing showed where to look. Three methods have found nothing, and that is
worth less than it sounds: enumeration and variants check the bytecode against a reference that
method 4 checks against Spark, and Spark's own tests mostly test Spark.

![Defects by the method that found them](figures/svg/fig51-defects-by-method.svg)

*Figure 7. One chip per find. Red is a wrong answer, a wrong error or a wrong memory access;
yellow a gap in the tests; grey a method that has found nothing.*

## Where the checking stops

The plumbing is tested, not proven. The JVM is trusted: no proof reaches HotSpot, and JDK 25 has
a known C2 bug that can drop a scalar store followed by a masked vector store (JDK-8388492);
Varka's stores should not take that shape, and comparing the interpreter with C2 should show a
wrong value, which is a test on the JDKs and CPUs we run, not a proof. The proofs rest on
assumptions about the data, and one of them was wrong. Random tests reach only what they draw.
Mutation testing has looked at two classes. And some verdicts are the JIT's, reported and bounded
rather than asserted.

## How these were measured

Every number above comes from a committed file in [Varka](https://github.com/vecbricks/varka),
and each figure is drawn from one by a script beside it under `sql/varka/plans/figures/`. The
plans named here are under `sql/varka/plans/m7/`.

* The opening and Figure 5: `sql/varka/coverage/generated.md`, the JaCoCo report over every
  emitted class, at commits 6fa0c863a5f (first measured), 69302411f8f and 05169a63d19 (today),
  kept in `figures/data/both-bodies.tsv`; `VARKA-285.md`, `VARKA-284.md`, `VARKA-303.md`.
* The oracle and Figure 1: `sql/varka/proofs/spec/STANDARD.md`, `spec/DELTAS.md`,
  `sql/varka/coverage.json`; `VARKA-243.md`, `VARKA-244.md`, `VARKA-275.md`, `VARKA-317.md`.
* Method 1 and Figure 2: the files under `sql/varka/proofs/` and its README; `VARKA-240.md` 2.3
  (Theorem 5.1) and its nightly cost, `VARKA-242.md` (bounds), `VARKA-245.md` 9.2 (the column
  contract) and pull request #874 (the date bound).
* Methods 2 to 4: `VARKA-269.md` 9 (enumeration and its costs), `VARKA-278.md` (variants),
  `VARKA-276.md` (the reference evaluator against Spark).
* Method 5 and Figure 3: `VARKA-262.md`, `VARKA-372.md` (35,000 queries), `VARKA-298.md` 9.2
  (the mirror), `VARKA-299.md` (the `NULL`-type column), `VARKA-380.md`, `VARKA-381.md` 9 (the
  two censuses of 150,000), `m8/PLAN.md` rows 386 and 387, pull request #911, SPARK-60315 and
  apache/spark#59573, `VARKA-280.md` and `sql/varka/coverage/reach/VarkaIrFuzzSuite.md`.
* Method 6 and Figure 4: `VARKA-81.md` and `sql/varka/golden_corpus.json` (731 statements),
  `VARKA-271.md` and `sql/varka/spark_suites/served.json`.
* Method 8: `VARKA-284.md`.
* Method 9 and Figure 6: `VARKA-272.md` 9.1, `VARKA-274.md` 9, `VARKA-370.md` and `VARKA-371.md`
  9.2 (the trap sums; 2 of 1,602 on three cores), `VARKA-378.md` (the nightly ceiling).
* Method 10: `VARKA-263.md`, `VARKA-292.md`.
* Figure 7: `figures/data/defects-by-method.tsv`, which names the record behind each chip; the
  filter order is `m7/PLAN.md` section 2.2 and its row 273.
* Where it stops: `m9/SCOPE.md` item 58 (what a proof can reach), item 56 (JDK-8388492).
