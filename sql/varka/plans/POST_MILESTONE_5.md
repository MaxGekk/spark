# Eight rows per instruction: Spark's `TIME` type on Varka's 64-bit lane

*The long read that closes milestone 5, drafted 22 September 2026 and finished
with the milestone's last numbers (`PLAN_TASK_118.md` section 4 C). Every
figure is a script under `figures/`; every number is from a committed results
file under `sql/varka/bench/benchmarks/`. The LinkedIn post is its trailer and
sits at the end of this file.*

---

Spark 4.1 gave SQL a `TIME` type: a time of day with no date attached, stored as
nanoseconds since midnight in a 64-bit integer, with the usual functions over
it - `hour`, `minute`, `second`, `time_trunc`, arithmetic with intervals,
differences between two times. It arrived behind a flag and is on its way to
being on by default.

On the development laptop - an AMD Ryzen AI 9 HX PRO 370, a Zen 5 core with a
full 512-bit vector datapath, running OpenJDK 25 on one core - stock Spark 4.2
computes `hour(t)` over a cached column at 61.3 million rows a second. Varka, a
research fork of Spark that compiles a projection into one vector loop,
computes the same `hour(t)` on the same core at 1041.4 million rows a second:
about seventeen times faster. Every number in this post comes from a committed
results file that names its machine, JDK, row count and vector width, so each
one can be checked without rerunning anything.
This post is about where those seventeen come from. Not from a faster
algorithm - the algorithm is a division - but from what the loop around it
looks like, what it reads, and what it does not do per row.

The ideas come first and the numbers last, because the numbers only mean
something once you can see the loop. If you already know how Spark's codegen
works, section 3 is where Varka starts.

## 1. What Spark's codegen produces

Whole-stage code generation was the right idea in 2015 and it still is: instead
of interpreting an expression tree per row, Spark writes the Java for a
pipeline of operators, compiles it with Janino, and runs a tight loop that the
JIT can inline through. It removed a virtual call per operator per row and won
a lot.

What it produces is still a row loop. The generated code takes one row, applies
every expression to it, writes one output row, and goes round again. Every
column value is read out of an `UnsafeRow`, every intermediate is a local, and
anything that needs an object - a `LocalTime`, a `Decimal`, a `UTF8String` -
gets a new one per row.

![Stock Spark's row loop against Varka's one loop over the column](figures/out/fig1-row-loop-vs-vector-loop.svg)

*Figure 1. Left: the generated Java takes rows one at a time and uses one lane
of a core that has eight 64-bit lanes to offer. Right: Varka's emitted loop
takes eight values of the column per instruction.*

The core underneath has moved on. A server core today does 512 bits of integer
work per instruction: eight 64-bit values, sixteen 32-bit ones. A row loop uses
one lane of that and leaves the other seven idle, and no amount of JIT
cleverness recovers them, because the loop's shape - one row, many columns,
then the next row - is the wrong way round for the hardware. The vector unit
wants one column, many rows.

## 2. What `hour(t)` costs

Take the smallest example. A `TIME` is a long: nanoseconds since midnight. The
hour is that long divided by 3 600 000 000 000. Here is how stock Spark gets
there, from `DateTimeUtils`:

```scala
def getHoursOfTime(nanos: Long): Int = LocalTime.ofNanoOfDay(nanos).getHour
```

![What hour(t) costs stock Spark against what it costs the lane](figures/out/fig2-localtime-per-row.svg)

*Figure 2. Stock Spark builds a `LocalTime` - four fields, one allocation - to
read one of the fields back. The lane divides.*

That is a perfectly reasonable line of Scala, and it is what a row engine
almost has to write: `java.time` is the correct library and `getHour` is the
correct method. The cost is that `LocalTime.ofNanoOfDay` does four divisions
to fill four fields, allocates an object to hold them, and the caller reads one
field and drops the object. Escape analysis sometimes removes the allocation
and sometimes does not, and the four divisions stay either way. Measured over
five hundred million cached rows, `hour(t)` costs stock Spark 16.3 nanoseconds a
row on this machine, and `minute(t)` and `second(t)` the same to the decimal,
because they are the same object built for a different field.

Varka's lane does one 64-bit division and stores the quotient. It costs
1.0 nanoseconds a row, and most of that is the memory traffic - an eight-byte
read and a four-byte write per row - rather than the division.

## 3. A batch is columns, not rows

The first thing Varka changes is what the loop reads. Spark already has a
columnar format in the JVM: cached tables live in `ColumnarBatch`es, and a
Varka session caches them as Arrow. An Arrow column is two buffers - the values
back to back, and a validity bitmap with one bit per row - and nothing else.

![One Arrow batch of six columns, and the row it is not](figures/out/fig3-batch-is-columns.svg)

*Figure 3. The `varka_times` table the benchmarks use: two `TIME` columns, two
day-time intervals, two `bigint`s, eight bytes a row each, plus a bit per row
for nulls. The loop reads the buffers where Arrow allocated them.*

Varka maps each buffer to a Panama `MemorySegment` over the address Arrow
already owns - no copy, no `ByteBuffer`, no object per value - and the emitted
loop loads eight lanes from it at a time with the Vector API. The validity
bitmap is loaded as bits and becomes a `VectorMask`, so a null is a lane that
is masked out rather than a branch.

This is the point where a native engine - Velox, DataFusion, Comet, Gluten -
crosses into C++ over JNI, and gets a very good vector engine on the other
side. Varka stays in the JVM on purpose: one jar for every CPU, one memory and
crash domain, ordinary profilers and stack traces, and eventually a user's own
vectorised function fused into the same loop as the built-ins instead of rows
handed back across a boundary. Whether the JVM can do the vector part well
enough is the experiment, and it is what the numbers at the end are for.

## 4. One class per projection, and a trapdoor

The second change is what the loop *is*. Varka does not interpret a vector
expression tree, and it does not call a library of kernels either - a call per
operator per batch is cheap, but the calls are megamorphic and the JIT cannot
see across them. Instead the compiler turns the Catalyst expressions of a
projection into a small vector IR, and an emitter writes a Java class for that
IR with JDK 25's Class-File API: one class per projection shape, holding one
loop.

![From the plan node to an emitted class, and the trapdoor under the kernel](figures/out/fig4-one-class-per-projection.svg)

*Figure 4. Above: a projection becomes an IR, the IR becomes bytecode, the JIT
sees one loop with monomorphic call sites. Below: batch by batch at run time,
with the trapdoor to the row engine under the kernel.*

Two consequences are worth knowing about. The class is named after its plan
node - its `SourceFile` attribute reads like `Project#12` - and carries a custom
attribute with the IR it computes, so a profiler, a flame graph or a heap dump
names the query's operator with no mapping table. And the class is loaded by
the task that needs it and unloaded with it, so a long-running executor does
not accumulate a codegen cache that only grows.

The trapdoor is the contract that makes the rest possible. Anything the
compiler cannot lower declines *per expression* at plan time, and the stock
row path computes that column while the fusable ones still fuse. Anything the
emitted kernel cannot compute for a *batch* - a `TIME` plus an interval that
would cross midnight, which is an error in Spark, or any failure at all in the
kernel - is refused, and the row engine computes that batch instead. A query on
Varka can be slower than it should be; it cannot be wrong or fail because of
Varka. That rule is what lets the engine grow one expression family at a time
in front of users rather than behind them.

## 5. The 64-bit lane, and why a division is the expensive thing

`TIME` is the third type on Varka's 64-bit lane, after `bigint` and day-time
intervals, and it is the one that made the lane interesting, because nearly
everything you do to a time of day is a division by a constant: the hour is a
division by 3.6e12, the minute by 6e10 and 60, `time_trunc('MINUTE', t)` a
division and a multiplication, `time_diff` a subtraction and a division. On the
32-bit lane a division by a constant is a multiply-high and a shift - a few
cheap instructions. On the 64-bit lane there is no multiply-high in the Vector
API, and a 64-bit product overflows anyway, so the lane goes through doubles.

![The two lowerings of a 64-bit division by a constant](figures/out/fig5-two-division-forms.svg)

*Figure 5. With AVX-512 the machine converts eight longs to eight doubles in
one instruction, divides, and converts back: seven vector operations. With AVX2
there is no such conversion, so the lane reads the bits of the long as a double
through an identity and back again: fourteen.*

A double holds every integer below 2^53 exactly, and a correctly rounded
quotient of two such integers is exact as an integer whenever the dividend is
below 2^52, which every nanosecond-of-day is by a wide margin. So the
conversion form is exact, and on a machine with AVX-512 it is a convert, a
divide and a convert.

Most laptops and a good share of cloud machines have AVX2 and not AVX-512, and
AVX2 has no instruction that turns a vector of longs into a vector of doubles.
HotSpot then compiles the conversion as eight scalar conversions, which is
worse than not vectorising. For those machines Varka emits a second form
built on a bit trick: for `0 <= u < 2^52`, the bit pattern of `u` OR'd with
`0x4330000000000000` *read as a double* is exactly `2^52 + u`, so a subtraction
recovers `u` as a double with no conversion instruction at all, and the same
identity run backwards turns the quotient into a long. The floor that Java's
truncating division needs is built by hand - the Vector API has no lanewise
floor either - and the sign is handled by dividing the magnitude and negating
the lanes that need it afterwards. Fourteen operations against seven, all of
them vector operations, all of them instructions AVX2 has.

Both forms are exact for every dividend under 2^52, and the emitter carries
that bound as an obligation on every 64-bit division it compiles, checks it
against what it knows of the dividend, and a test drives both forms through the
JVM at the bound to confirm that each fails exactly the way its arithmetic says
it should above it. The lowering is chosen per machine, at emission, from what
the JVM reports it can do, and the shape of the compiled kernel is part of the
cache key, so a jar moved between machines recompiles rather than misbehaves.

## 6. One loop for the whole projection

The third change is the one that compounds. A projection usually has several
outputs, and they usually share work: `hour(t + dt)` and
`time_trunc('MINUTE', t + dt)` both need `t + dt`, and both need it guarded
against leaving the day. A row engine computes the sum twice per row; a
kernel-library engine computes it twice per batch and materialises it twice.
Varka compiles the projection as one graph, finds the shared subtrees, and
emits one loop in which the shared value is computed once per eight rows and
stays in a register.

![Two outputs sharing a subtree, and the loop they become](figures/out/fig6-fusion-shared-subtree.svg)

*Figure 6. Left: the expression graph of two outputs over the same guarded
sum. Right: the loop the emitter writes for it - one load per input column,
the shared sum once, one store per output.*

This is where the row engine's overhead stops being amortised and starts being
multiplied: every link of a chain costs stock Spark another virtual call, another
boxed intermediate or another object, while the fused loop pays one load per
input column and one store per output whatever the depth. It is also why a
single-expression benchmark understates the engine. The `TIME` surface below
measures one call per row and reads 13x to 38x; the chains, which compose the
calls, are where the fused loop shows what it is worth, and they are the number
the closing section is built around.

## 7. How it is measured

*To write with figures 7 and 8: the four arms, the fixed-share and residency
guards drawn as gates, the datapath probe, the band.*

## 8. The numbers

*To write from the committed files: the surface's median and the honest split
between kernel rate and row-engine cost (`PLAN_TASK_155.md`), the `TIME`
chains on the laptop and on the full-width runner (task 164), the AVX2 arm
(task 121).*

## 9. What it does not do yet, and how to reproduce it

*To write: the coverage table's declines read from `coverage.json`, tasks 28,
103 and 104 by name, and the README's reproduction recipe with the `time` and
`timechains` selectors.*

---

## The LinkedIn post

*Drafted last, from the sections above: one idea, one picture, one number, one
ask.*
