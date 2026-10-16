# Bytecode without source

*Draft for row 259, 16 October 2026, the companion of row 258's post. Every number is from
`sql/core/benchmarks/VarkaCodegenBenchmark-jdk25-results.txt` and
`sql/core/benchmarks/VarkaColdStartBenchmark-jdk25-results.txt` (JDK 25.0.4 on an AMD Ryzen AI 9
HX PRO 370 laptop), or from the published posts and skill files it links. The benchmark item 79
asks for, emission against Janino over the same expression list, is not yet run; see
`VARKA-259.md` 9.1.*

How Spark compiles a query, and why it compiles it again [link to row 258's post when it is
published] followed one query from its plan to running code: one Java source string per
stage, compiled by Janino on the driver and again on every executor, cached by the text of that
source, and then compiled a third time by the JIT. This post is for the people who write that kind
of code generator. It asks what changes when the generator writes bytecode instead of Java, from
what we learned building Varka, a fork of Spark that compiles expressions into Java Vector API
kernels and writes their classes directly.

The short answer: generating the class gets cheap and its size becomes a fact instead of a guess.
The JIT's wait does not change at all, and the saving that matters comes from what the class is
keyed by, which has nothing to do with bytecode.

## What a string costs

Spark's generator produces text. It cannot know how much bytecode the text will become, so it
guesses: `spark.sql.codegen.methodSplitThreshold` cuts code into methods every 1024 characters, and
its own documentation says why - "We cannot know how many bytecode will be generated, so use the
code length as metric." On a projection of forty `x + k` that comes to about seven characters a
byte. Only after Janino has parsed and compiled the text does Spark read the methods' sizes off the
class, and by then the class exists; [Under 8000 bytes by
construction](https://vecbricks.github.io/under-8000-bytes-by-construction/) told that part.

Text has a second property that row 258's post turns on. Spark's cache of generated classes is
keyed by the source, and some literals are written into the source. A query that differs only in
an integer or a date is a new string, so it is a new class.

## Writing the class directly

Varka's emitter never produces Java. It walks a small IR of vector operations and writes bytecode
with `java.lang.classfile`, the Class-File API that became final in JDK 24. Two things follow.

**The size is read, not estimated.** The emitter builds the class, reads every method's length in
the unit HotSpot enforces, and only then decides: a group of outputs with a method over 8000 bytes
is split and the class built again; an output too large for any method is declined with a reason,
and Spark computes that one. None of this needs the class to be loaded. One exception taught us
something: the class-file format's own cap, 65,535 bytes of code in a method, is enforced by the
Class-File API while it assembles the class, so past it there is no class to measure. The API
throws "Code length N is outside the allowed range", and the emitter reads that refusal as the
measurement of that one method.

**The assembler checks the stack.** `ClassFile.of().build(...)` computes the stack map frames
itself, and an inconsistent operand stack fails at emit time with the bytecode offset and an
instruction dump, before the JVM's verifier could see it. A double-store bug in an early kernel
never got as far as a class loader. What it does not catch is a wrong method descriptor, which
builds, verifies and fails on first execution with `NoSuchMethodError`; we keep a test for exactly
that.

## What the class is keyed by

A third difference is not about bytecode at all: it comes from emitting from an IR in which each
constant is a slot, and a source generator could do the same. Spark already does it for strings,
decimals and binaries, which it passes to the class as references rather than writing them into the
text.

Varka's emitter takes a tree with every constant replaced by a slot. `date_add(d, 7), year(d)`
and `date_add(d, 30), year(d)` are one shape with one hash, and run one class with a different
value in slot 0; so does the same pair over any other date column, since the tree says "column
0". Varka's cache of loaded classes is keyed by that shape, so a dashboard that
reruns one query with a new date pays for its class once, and its JIT compile once. Spark has met
the same question in a narrower place: since 4.4.0 a stage remembers in its generated code whether
a large `CASE WHEN` was split, so a literal that changes every run makes it decide again, and a key
that leaves literal values out is filed as
[SPARK-60048](https://issues.apache.org/jira/browse/SPARK-60048).

## What emission costs

Varka's codegen benchmark times both generators cold, one class per iteration:

| generator | one class |
|:--|--:|
| Spark: `GenerateUnsafeProjection.generate` (source, then Janino) | about 6.5 ms |
| Varka: emit, define, load and instantiate a fused kernel | about 99 us |

That is 65.6 times. The Janino arm defeats Spark's cache the way row 258's post describes, with a
new literal each iteration, and it has done so since Varka's first milestone, because without it
every iteration after the first would hit the cache.

The comparison is not like for like, and we say so before anyone else does. The Janino arm
compiles a one-output `date_add` projection with Spark's row writer; the Varka arm emits a
two-output kernel with a shared subexpression. [A benchmark that emits and compiles the same
expression list both ways is owed before publication; the table above is replaced or joined by
its numbers.]

## What it does not change

Generating a class is not running it fast. A new class starts in the interpreter, moves to C1's
code and then to C2's, and a Vector API kernel is slow until C2 has it, because the vectors stay
boxed until then. Varka's cold-start benchmark shows where the time goes, for sixteen date
expressions over a hundred thousand cached rows:

| | first run | second run | once compiled |
|:--|--:|--:|--:|
| Spark | 110 ms | 53 ms | |
| Varka, no warm-up | 241 ms | 213 ms | |
| Varka with its warm-up | 140 ms | 62 ms | 27 ms |

On Varka, planning includes classifying the projection and emitting its kernel, and planning a
new shape takes 18 ms against Spark's 17. The class costs a tenth of a millisecond to make, and
Varka without help is still the slowest of the three for its first runs: the kernel is serving
batches before C2 has compiled it. What fixes that is
not the bytecode but a background thread that warms the new kernel while the shape's batches take
Spark's own path, and hands over when the JIT has compiled it. In this run the hand-over came after
1.6 to 1.9 seconds, and from there the query takes 27 ms against Spark's 53.

So for a generator that writes bytecode, the saving that matters is the second and third queries,
not the first: a class that is never rebuilt for a new constant never waits for the JIT again.

## What you give up

There is no source to read. A Spark developer runs `explain("codegen")` and reads Java; a
Varka developer needs tools to see what was built:

* `dev/varka_emit.sh "date_add(d,7)" "year(d)"` prints the shape, its hash, the literals in slot
  order and every method with its bytes, for any projection.
* The emitted methods carry a line number table whose numbers are IR node indices, decoded by a
  custom attribute inside the class, so a stack trace through a kernel names the IR node that
  threw.
* `javap` works on the bytes like on any class.

The larger cost is that every new expression is written as instructions rather than as a line
of Java, and what keeps those honest is the stack map generator and the tests, not a compiler.

## Reproducing it

```
dev/varka_bench_regen.sh core VarkaCodegenBenchmark
dev/varka_emit.sh "date_add(d,7)" "year(d)"
```

and, on any stock Spark, `df.explain("codegen")` for the generated Java and the stage's
`maxMethodCodeSize`. The code, the benchmarks and every results file quoted here are in
[vecbricks/varka](https://github.com/vecbricks/varka).
