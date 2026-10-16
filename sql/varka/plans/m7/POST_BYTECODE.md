# The class got 65 times cheaper. The first query got slower.

Varka is a fork of Apache Spark that compiles SQL expressions into Java Vector API kernels. Spark
turns each query stage into Java source and compiles it with Janino. Varka skips the source: it
writes the kernel's bytecode directly. When we timed the two, making one class took Spark about
6.5 milliseconds and Varka about a tenth of one.

Then we timed the first query of a new shape, and Varka without help was the slowest bar on the
chart.

![The class is the cheap part](figures/svg/fig33-the-class-is-the-cheap-part.svg)

*Figure 1. Making a class and running a query, on one scale. Sixteen date expressions over
100,000 cached rows, best of five, JDK 25 on a laptop. The two class bars are not the same
work; see "The 65x, honestly" below.*

This post is for people who write code generators. It covers what changes when the generator
writes bytecode instead of source, and what doesn't. In short: the size of the generated code
becomes a fact instead of a guess; the class becomes almost free to make, which turns out not to
matter; and the saving that does matter comes from how classes are cached, which has nothing to
do with bytecode.

## Two ways to make a class

![Two generators, one measurement](figures/svg/fig25-two-generators-one-measurement.svg)

*Figure 2. Spark writes Java source, compiles it with Janino and only then reads the method
sizes off the class. Varka emits bytecode, reads the same sizes off the class it just built,
and rebuilds it with smaller groups when one is too big.*

**Spark guesses the size.** Spark's generator writes text, and text doesn't know how much
bytecode it will become. So Spark cuts the code into methods every 1024 characters
(`spark.sql.codegen.methodSplitThreshold`). The setting's own documentation admits the guess:
"We cannot know how many bytecode will be generated, so use the code length as metric." On a
projection of forty `x + k` it comes to about seven characters per byte of bytecode. Spark reads
the real sizes only after Janino has compiled the class. By default it then does nothing with
them: the check that could act, `spark.sql.codegen.hugeMethodLimit`, defaults to 65535, a size
no method that compiled can reach. [Under 8000 bytes by
construction](https://vecbricks.github.io/under-8000-bytes-by-construction/) tells that part
of the story.

**Varka measures it.** Varka's emitter walks a small tree of vector operations and writes
bytecode with `java.lang.classfile`, the Class-File API that became final in JDK 24. It builds
the class, reads every method's length in the unit HotSpot enforces, and then decides. A group
of outputs whose method is over 8000 bytes is split and the class built again. An output too big
for any method is declined with a reason, and Spark computes that one instead. Nothing has been
loaded yet. The one surprise was the class-file format's hard cap of 65,535 bytes of code per
method. The Class-File API enforces it while it assembles the class, so past the cap there is no
class to measure. Instead the API throws "Code length N is outside the allowed range", and the
emitter takes that error as the measurement.

**The assembler checks the stack, too.** `ClassFile.of().build(...)` computes the stack map
frames itself. A method that leaves the operand stack inconsistent fails at emit time, with the
bytecode offset and an instruction dump, before the JVM's verifier ever sees it. One early
kernel stored a value twice, and the bug never reached a class loader. What the assembler
doesn't catch is a wrong method descriptor. That builds, verifies, and fails the first time it
runs with `NoSuchMethodError`, so we keep a test for exactly that.

## Why the cheap class didn't help

Go back to Figure 1. Every new class starts in the bytecode interpreter, moves to C1's code and
then to C2's. A Vector API kernel is slow until C2 compiles it, because until then each vector
is a real object on the heap. Varka without help hands batches to a kernel that C2 hasn't
compiled yet. Its first run takes 241 ms and its second 213 ms, against Spark's 110 and 53.

The fix has nothing to do with bytecode. A background thread warms the new kernel while the
shape's batches take Spark's own path. When the JIT has compiled the kernel, the thread hands
over to it. In this run the hand-over came after 1.6 to 1.9 seconds, and from then on the query
took 27 ms against Spark's 53. Planning a new shape, including emitting its kernel, took 18 ms
against Spark's 17.

So a tenth of a millisecond for the class doesn't change the first query, and 6.5 ms wouldn't
have changed it either. Making the class was never the expensive part. Waiting for the JIT is.

## Never make the class again

If the JIT is the expensive part, the best class is one you already have. That depends on what
the class cache is keyed by.

![Two queries, one class](figures/svg/fig34-two-queries-one-class.svg)

*Figure 3. Spark writes an integer literal into the generated source, so a query that differs
only in that number is a new string, a new class and a new JIT compile. Varka's emitter turns
the literal into a slot, so both queries are one shape and run on one compiled class.*

Spark caches generated classes by their source text, and some literals are written into that
text. A dashboard that reruns `date_add(d, 7)` as `date_add(d, 30)` gets a new string, so a new
Janino compile, so a new class that the JIT starts again from the interpreter. Spark already
avoids this for strings, decimals and binary values, which it passes to the class as references
instead of writing them into the text. [Link to row 258's post, which measures what the extra
compiles cost on Spark, when it is published.]

Varka's emitter takes a tree with every constant replaced by a slot. `date_add(d, 7), year(d)`
and `date_add(d, 30), year(d)` are one shape with one hash, and run on one class with a different
value in slot 0. So does the same pair over any other date column, because the tree says
"column 0", not `d`. The cache of loaded classes is keyed by that shape, so the dashboard pays
for its class, and its JIT compile, once.

This part isn't about bytecode at all. A source generator could put constants in slots just as
well. Spark has run into the same question in a narrower place: since 4.4.0 a stage records in
its generated code whether it split a large `CASE WHEN`, so a literal that changes on every run
makes it decide again. A cache key that leaves literal values out is filed as
[SPARK-60048](https://issues.apache.org/jira/browse/SPARK-60048).

## The 65x, honestly

| generator | one class, cold |
|:--|--:|
| Spark: `GenerateUnsafeProjection.generate` (source, then Janino) | about 6.5 ms |
| Varka: emit, define, load and instantiate a fused kernel | about 99 us |

That is 65.6 times. To time Janino cold, the benchmark gives it a new literal on every iteration,
which defeats Spark's cache exactly as Figure 3 shows. The two arms are not the same work,
though, and we'd rather say so before anyone else does. The Janino arm compiles a one-output
`date_add` projection with Spark's row writer; the Varka arm emits a two-output kernel with a
shared subexpression. [A benchmark that emits and compiles the same expression list both ways is
owed before publication; this table is replaced or joined by its numbers.]

## What you give up

There is no source to read. A Spark developer runs `explain("codegen")` and reads Java. A Varka
developer needs tools to see what was built:

* `dev/varka_emit.sh "date_add(d,7)" "year(d)"` prints the shape, its hash, the literals in slot
  order and every method with its size, for any projection.
* The emitted methods carry a line number table whose numbers are tree node indices, so a stack
  trace through a kernel names the node that threw.
* `javap` reads the bytes like any other class.

The bigger cost is that every new expression is written as instructions instead of a line of
Java. What keeps those honest is the stack map check and the tests, not a compiler.

## Reproducing it

```
dev/varka_bench_regen.sh core VarkaCodegenBenchmark
dev/varka_bench_regen.sh core VarkaColdStartBenchmark
dev/varka_emit.sh "date_add(d,7)" "year(d)"
```

On any stock Spark, `df.explain("codegen")` shows the generated Java and the stage's
`maxMethodCodeSize`. Every number here comes from
`sql/core/benchmarks/VarkaCodegenBenchmark-jdk25-results.txt` and
`VarkaColdStartBenchmark-jdk25-results.txt` (JDK 25.0.4, AMD Ryzen AI 9 HX PRO 370), and the
figures are drawn from those files by the scripts beside them. The code and the results are in
[vecbricks/varka](https://github.com/vecbricks/varka).
